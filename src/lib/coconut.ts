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
  // Confirmed by Coconut error: credential field names are
  // `access_key_id` and `secret_access_key` (no s3_ prefix). The
  // earlier `s3_access_key_id_not_found` error was actually AWS's
  // error (the request was reaching AWS S3, not R2) — not a hint
  // about Coconut's schema.
  const r2Endpoint = process.env.R2_ACCOUNT_ID
    ? `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`
    : undefined;
  const storage = {
    service: "s3other",
    bucket: process.env.R2_BUCKET_NAME,
    // R2 expects "auto" — not a real AWS region name. Reads (GET via
    // public URL) ignore region entirely, but PutObject SigV4 signing
    // includes the region in the canonical request, so a wrong value
    // like "us-east-1" produces a signature mismatch that R2 returns
    // as a generic upload failure (no helpful detail surfaces back
    // through Coconut).
    region: "auto",
    endpoint: r2Endpoint,
    credentials: {
      access_key_id: process.env.R2_ACCESS_KEY_ID,
      secret_access_key: process.env.R2_SECRET_ACCESS_KEY,
    },
    force_path_style: true,
  };

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
  // Without the leading slash their path parser fell over on our
  // email-style folder names ("user@gmail.com/...") — extracting
  // an empty extension and erroring on "format mp4 doesn't match".
  // Adding the leading slash should anchor the parser correctly
  // without us having to URL-encode the '@' (which would change
  // the R2 storage key and break our own download URL).
  const sanitizedPath = "/" + opts.outputBucketKey.replace(/^\/+/, "");

  const output: Record<string, unknown> = {
    path: sanitizedPath,
    key: "mp4:final",
    format: {
      resolution,
      // 1=low, 5=highest. 4 maps roughly to veryfast/crf 23.
      quality: 4,
    },
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
