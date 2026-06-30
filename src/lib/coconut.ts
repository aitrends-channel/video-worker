// Coconut.co transcoding integration.
//
// SKETCH STATUS: untested against the live Coconut API. Verify the
// job-spec shape and endpoint paths against https://docs.coconut.co
// before relying on this in production. The HTTP shape below tracks
// the v2 API as of late 2024; if Coconut has revised the schema,
// adjust submitJob/pollJob accordingly.
//
// Pattern this enables in runAssembly:
//   1. Worker uploads mixed.mp4 to R2 (already happens today).
//   2. submitJob() POSTs a job spec to Coconut referencing the R2
//      public URL as input and an R2 path as output (via R2's
//      S3-compatible API credentials).
//   3. Coconut transcodes on their hardware while the worker is FREE
//      to handle the next assembly.
//   4. pollJob() checks status every few seconds until terminal.
//   5. On success, the output is already in R2 — no copy step needed.
//
// Env vars required:
//   COCONUT_API_KEY            — get from https://app.coconut.co
//   COCONUT_API_BASE           — defaults to https://api.coconut.co/v2
//   R2_S3_ENDPOINT             — your R2 S3-compatible endpoint
//                                 (https://<account>.r2.cloudflarestorage.com)
//   R2_ACCESS_KEY_ID
//   R2_SECRET_ACCESS_KEY
//   R2_BUCKET_NAME             — bucket where Coconut writes the output
//   R2_PUBLIC_URL              — public URL prefix for that bucket

export interface CoconutFinalizeOptions {
  // Source video — Coconut fetches it via HTTPS. The R2 public URL
  // of mixed.mp4 works directly here.
  inputUrl: string;

  // Output spec.
  outputBucketKey: string;       // e.g. "user/projectId/finals/output.mp4"
  outputWidth: number;            // final pixel width
  outputHeight: number;           // final pixel height

  // Optional overlays.
  captionsAssUrl?: string | null;  // public URL of an ASS subtitle file
  logoUrl?: string | null;

  // Optional callback URL — Coconut POSTs job events here so you can
  // skip polling. Omit to use pollJob() instead.
  notificationUrl?: string;
}

// Coconut's actual API returns status with a `job.` prefix:
// `job.starting`, `job.processing`, `job.completed`, `job.failed`,
// `job.canceled`. The dashboard sometimes shows them uppercase
// (JOB.FAILED) but the wire format is lowercase + prefixed.
export type CoconutJobStatus =
  | "job.starting"
  | "job.processing"
  | "job.completed"
  | "job.failed"
  | "job.canceled"
  | "job.queued"
  | string; // tolerate other strings rather than blow up

export interface CoconutJob {
  id: string;
  status: CoconutJobStatus;
  errors?: { code: string; message: string }[];
  output_url?: string;            // populated on completed
}

// Build the Coconut job spec. Shape verified against a known-working
// example from Coconut's dashboard. Key constraints:
//   - `notification` is REQUIRED, must be `{ url }` (no type/events).
//     We default to COCONUT_WEBHOOK_URL env var; if unset we still
//     include the key with a placeholder URL — Coconut will try to
//     POST events there and silently fail, but the job itself runs.
//   - `outputs` is keyed by CONTAINER ("mp4", "webm", etc.) and its
//     value is an ARRAY of outputs in that container.
//   - Each output uses high-level `format: { width, height, quality }`
//     instead of low-level codec/preset/crf. Quality 1-5; 4 is "high"
//     (~veryfast crf 23 equivalent).
//   - R2 storage is passed under `storage` using Coconut's s3other
//     service — they support any S3-compatible target.
function buildJobSpec(opts: CoconutFinalizeOptions) {
  // URL-format storage config. The "object" form with
  // service:"s3other" + endpoint field appears in our submitted
  // spec, but Coconut's job-detail dashboard confirms the endpoint
  // is NOT being applied to the upload — every PUT hits AWS S3
  // default and AWS rejects the R2 credentials with the
  // `s3_access_key_id_not_found` error. The URL form encodes
  // endpoint, region, and path-style addressing into a single
  // connection string that Coconut can't drop.
  //
  // Pattern: s3://ACCESS:SECRET@bucket?endpoint=URL&region=auto&force_path_style=true
  // Both access_key and secret are URL-encoded so chars like '/'
  // or '+' in R2 secrets don't break URL parsing.
  if (!process.env.R2_ACCOUNT_ID || !process.env.R2_ACCESS_KEY_ID || !process.env.R2_SECRET_ACCESS_KEY || !process.env.R2_BUCKET_NAME) {
    throw new Error("R2 storage not fully configured — R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME are all required for Coconut.");
  }
  const r2Endpoint = `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
  const storageUrl = `s3://${encodeURIComponent(process.env.R2_ACCESS_KEY_ID)}:${encodeURIComponent(process.env.R2_SECRET_ACCESS_KEY)}@${process.env.R2_BUCKET_NAME}?endpoint=${encodeURIComponent(r2Endpoint)}&region=auto&force_path_style=true`;
  const storage = { url: storageUrl };

  // Coconut's format block takes a `resolution` PRESET string,
  // not raw width/height. Map our dimensions to the closest preset
  // based on the short side — that's the standard interpretation
  // (e.g. 1920x1080 and 1080x1920 are both "1080p"). Coconut
  // preserves the input's aspect ratio when given a resolution
  // preset, so vertical/square outputs come through naturally as
  // long as the worker's intermediate is already the right aspect.
  const shortSide = Math.min(opts.outputWidth, opts.outputHeight);
  const resolution =
    shortSide >= 2160 ? "2160p" :
    shortSide >= 1440 ? "1440p" :
    shortSide >= 1080 ? "1080p" :
    shortSide >= 720 ? "720p" :
    shortSide >= 480 ? "480p" : "360p";

  // Output-level params Coconut accepts are narrow (path, key,
  // format, and a few format-internal options). Subtitle and
  // watermark transforms appear to live at the JOB level instead
  // — adding them as output-level keys triggered the same
  // "Output param key not valid" error pattern that "transformation"
  // hit. Move them up to the job root.
  // Coconut's canonical examples all show paths starting with "/".
  // The leading slash + URL-encoded "@" together work around a
  // Coconut path-parser bug: email-style folder names like
  // "nyefene1@gmail.com/..." make their parser read the "@" as a
  // userinfo delimiter, then it can't extract the filename
  // extension from what's left — fails the spec validation with
  // "output_filename_not_valid". Encoding only the "@" (not the
  // whole path) keeps the URL readable and Coconut URL-decodes the
  // path back to its literal form when constructing the actual S3
  // PUT key — so the R2 storage key stays "nyefene1@gmail.com/..."
  // and our download URL reconstruction (which uses the un-encoded
  // outputBucketKey) still finds the file.
  const sanitizedPath = ("/" + opts.outputBucketKey.replace(/^\/+/, "")).replace(/@/g, "%40");

  const output: Record<string, unknown> = {
    path: sanitizedPath,
    key: "mp4:final",
    // String-form format. Coconut v2 documents both an object form
    // ({ resolution, quality }) and a colon-separated string form
    // (`mp4:1080p:quality=4`). Object form fails our captioned jobs
    // with output_filename_not_valid — the path-validator can't
    // extract the filename extension even from a clean
    // "/coconut-out/<id>/final_burned.mp4" path. Adding a `container`
    // key to the object form returns output_format_not_valid. The
    // string form encodes container, resolution, and quality
    // explicitly in one token — different code path in Coconut's
    // validator, with a chance of dodging the subtitle-induced
    // strict-mode bug.
    format: `mp4:${resolution}:quality=4`,
  };

  // Notification URL is required by Coconut's schema. We previously
  // tried a "placeholder" fallback URL but Coconut still tries to
  // POST to it and surfaces "notification error" on every job. Fail
  // fast instead — the user gets a clear error pointing at the env
  // var to set, rather than silent dashboard noise.
  //
  // Get a test webhook URL from app.coconut.co → Notifications,
  // copy it into Render's COCONUT_WEBHOOK_URL env var.
  const notificationUrl = opts.notificationUrl ?? process.env.COCONUT_WEBHOOK_URL;
  if (!notificationUrl) {
    throw new Error("COCONUT_WEBHOOK_URL not set — grab a test webhook URL from app.coconut.co → Notifications and set it as an env var.");
  }

  const spec: Record<string, unknown> = {
    input: { url: opts.inputUrl },
    storage,
    notification: { url: notificationUrl },
    outputs: {
      mp4: [output],
    },
  };
  // Captions + logo as job-level transforms — applies to all
  // outputs in the job. If Coconut still complains the key names
  // might be slightly different (e.g. "watermark" vs "image_watermark");
  // the debug log printed before the request shows the exact JSON
  // for the next 400 to point at.
  if (opts.captionsAssUrl) {
    spec.subtitles = { source: opts.captionsAssUrl };
  }
  if (opts.logoUrl) {
    spec.watermark = {
      url: opts.logoUrl,
      position: "top_right",
    };
  }
  return spec;
}

const API_BASE = (process.env.COCONUT_API_BASE ?? "https://api.coconut.co/v2").replace(/\/$/, "");

export async function submitJob(opts: CoconutFinalizeOptions): Promise<CoconutJob> {
  if (!process.env.COCONUT_API_KEY) {
    throw new Error("COCONUT_API_KEY not set — Coconut finalize unavailable");
  }
  const spec = buildJobSpec(opts);
  // Log the exact spec we're submitting (redacted credentials) so
  // when Coconut returns a 400 the worker logs show the payload
  // that triggered it. Pair the printed JSON with the error
  // message to calibrate field names without guessing.
  const redactedSpec = JSON.parse(JSON.stringify(spec)) as Record<string, unknown>;
  const redactedStorage = redactedSpec.storage as Record<string, unknown> | undefined;
  if (redactedStorage?.credentials) {
    redactedStorage.credentials = "[redacted]";
  }
  // URL-format storage embeds secrets in the URL string — scrub
  // those too so they don't leak to logs.
  if (typeof redactedStorage?.url === "string") {
    redactedStorage.url = redactedStorage.url.replace(/s3:\/\/[^@]+@/, "s3://[redacted]@");
  }
  console.log(`[coconut] submitting job spec: ${JSON.stringify(redactedSpec)}`);
  const res = await fetch(`${API_BASE}/jobs`, {
    method: "POST",
    headers: {
      "Authorization": `Basic ${Buffer.from(`${process.env.COCONUT_API_KEY}:`).toString("base64")}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(spec),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Coconut submit failed: HTTP ${res.status} ${body.slice(0, 500)}`);
  }
  return await res.json() as CoconutJob;
}

export async function getJob(jobId: string): Promise<CoconutJob> {
  const res = await fetch(`${API_BASE}/jobs/${jobId}`, {
    headers: {
      "Authorization": `Basic ${Buffer.from(`${process.env.COCONUT_API_KEY}:`).toString("base64")}`,
    },
  });
  if (!res.ok) throw new Error(`Coconut getJob failed: HTTP ${res.status}`);
  return await res.json() as CoconutJob;
}

// Poll a job to completion. Backoff: starts at 3s, doubles up to 30s.
// Aborts cleanly if the shared signal triggers (Stop button). Timeout
// at the caller's discretion — typically wrap with an outer timer.
//
// Status strings come back with a `job.` prefix (`job.completed`,
// `job.failed`, etc.) — earlier match against bare "failed" silently
// failed and the loop ran forever, spamming the same status line. Now
// we strip the prefix before comparing AND match against the prefixed
// forms defensively. onUpdate is throttled to fire only when the
// status actually changes.
export async function pollJob(
  jobId: string,
  signal?: AbortSignal,
  onUpdate?: (status: CoconutJob["status"]) => void,
): Promise<CoconutJob> {
  let delay = 3000;
  const MAX_DELAY = 30_000;
  let lastStatus: string | null = null;
  while (true) {
    if (signal?.aborted) throw new Error("ASSEMBLY_STOPPED_BY_USER");
    const job = await getJob(jobId);
    // Strip the `job.` prefix so terminal-state checks work against
    // both prefixed and bare forms (some Coconut endpoints/versions
    // differ).
    const normalized = (job.status ?? "").replace(/^job\./, "");
    if (onUpdate && job.status !== lastStatus) {
      onUpdate(job.status);
      lastStatus = job.status;
    }
    if (normalized === "completed") return job;
    if (normalized === "failed" || normalized === "canceled") {
      // Log the FULL response on failure so we can see whatever
      // fields Coconut actually populated (their `errors` array
      // is sometimes empty even on real failures — the reason may
      // live under `outputs[i].error`, `events`, or only on the
      // dashboard). The dump gives us everything to debug from.
      console.error(`[coconut] job ${jobId} failed; full response:`, JSON.stringify(job, null, 2));
      const errMsg = job.errors?.[0]?.message ?? `job ended in ${job.status} — check Coconut dashboard for details`;
      const errCode = job.errors?.[0]?.code ? ` [${job.errors[0].code}]` : "";
      throw new Error(`Coconut job ${jobId} ${job.status}${errCode}: ${errMsg}`);
    }
    await new Promise((r) => setTimeout(r, delay));
    delay = Math.min(MAX_DELAY, delay * 2);
  }
}
