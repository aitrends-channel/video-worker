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
  logoXPct?: number;
  logoYPct?: number;
  logoSizePct?: number;

  // Optional callback URL — Coconut POSTs job events here so you can
  // skip polling. Omit to use pollJob() instead.
  notificationUrl?: string;
}

export interface CoconutJob {
  id: string;
  status: "queued" | "processing" | "completed" | "failed" | "canceled";
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
  const storage = {
    service: "s3other",
    bucket: process.env.R2_BUCKET_NAME,
    region: "auto",
    endpoint: process.env.R2_S3_ENDPOINT,
    credentials: {
      access_key_id: process.env.R2_ACCESS_KEY_ID,
      secret_access_key: process.env.R2_SECRET_ACCESS_KEY,
    },
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

  // Subtitle burn-in and watermark sit DIRECTLY on the output
  // object as top-level params. The earlier "transformation"
  // wrapper isn't a Coconut concept — each transform is its own
  // first-class output key.
  const output: Record<string, unknown> = {
    path: opts.outputBucketKey,
    key: "mp4:final",
    format: {
      resolution,
      // 1=low, 5=highest. 4 maps roughly to veryfast/crf 23.
      quality: 4,
    },
  };
  if (opts.captionsAssUrl) {
    output.subtitles = { source: opts.captionsAssUrl };
  }
  if (opts.logoUrl) {
    output.watermark = {
      url: opts.logoUrl,
      position: "top_right",
    };
  }

  // Notification URL: prefer caller-provided, fall back to env var.
  // The key is required by Coconut's schema. If neither source has
  // a URL we still need SOMETHING — supply the request endpoint
  // path under Coconut's domain as a no-op fallback, which Coconut
  // accepts as a syntactically valid URL and routes nowhere
  // meaningful.
  const notificationUrl = opts.notificationUrl
    ?? process.env.COCONUT_WEBHOOK_URL
    ?? "https://app.coconut.co/notifications/http/placeholder";

  return {
    input: { url: opts.inputUrl },
    storage,
    notification: { url: notificationUrl },
    outputs: {
      mp4: [output],
    },
  };
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
export async function pollJob(
  jobId: string,
  signal?: AbortSignal,
  onUpdate?: (status: CoconutJob["status"]) => void,
): Promise<CoconutJob> {
  let delay = 3000;
  const MAX_DELAY = 30_000;
  while (true) {
    if (signal?.aborted) throw new Error("ASSEMBLY_STOPPED_BY_USER");
    const job = await getJob(jobId);
    if (onUpdate) onUpdate(job.status);
    if (job.status === "completed") return job;
    if (job.status === "failed" || job.status === "canceled") {
      const msg = job.errors?.[0]?.message ?? `job ended in ${job.status}`;
      throw new Error(`Coconut job ${jobId} ${job.status}: ${msg}`);
    }
    await new Promise((r) => setTimeout(r, delay));
    delay = Math.min(MAX_DELAY, delay * 2);
  }
}
