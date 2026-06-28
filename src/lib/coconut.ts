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

// Build the Coconut job spec. The shape mirrors their JSON config
// format. R2 storage credentials are passed as a "service" so Coconut
// can write directly to your R2 bucket via the S3-compatible API.
function buildJobSpec(opts: CoconutFinalizeOptions) {
  const r2Service = {
    service: "s3other",
    bucket: process.env.R2_BUCKET_NAME,
    region: "auto",
    endpoint: process.env.R2_S3_ENDPOINT,
    credentials: {
      access_key_id: process.env.R2_ACCESS_KEY_ID,
      secret_access_key: process.env.R2_SECRET_ACCESS_KEY,
    },
  };

  // Filter chain: scale → subtitles → logo overlay.
  // Each filter is expressed as a Coconut transformation block.
  // Subtitles + image overlay are first-class operations.
  const filters: Record<string, unknown>[] = [];
  if (opts.captionsAssUrl) {
    filters.push({
      subtitles: {
        source: opts.captionsAssUrl,
        burn_in: true,
      },
    });
  }
  if (opts.logoUrl) {
    filters.push({
      watermark: {
        url: opts.logoUrl,
        // Coconut expresses position as keywords + offsets. Map our
        // pct anchors to the closest gravity. We default to top-right
        // since that's the project's default position.
        position: "top_right",
        width: `${Math.round((opts.logoSizePct ?? 0.1) * 100)}%`,
        opacity: 1,
      },
    });
  }

  return {
    input: { url: opts.inputUrl },
    storage: r2Service,
    notification: opts.notificationUrl
      ? { type: "http", url: opts.notificationUrl, events: true }
      : undefined,
    outputs: {
      // Single mp4 output at the user's chosen resolution.
      "video:mp4": {
        path: opts.outputBucketKey,
        video: {
          codec: "h.264",
          profile: "high",
          preset: "veryfast",
          crf: 23,
          width: opts.outputWidth,
          height: opts.outputHeight,
          fps: 24,
        },
        audio: {
          codec: "aac",
          bitrate: 128,
          sample_rate: 44100,
          channels: 2,
        },
        filters: filters.length > 0 ? filters : undefined,
      },
    },
  };
}

const API_BASE = (process.env.COCONUT_API_BASE ?? "https://api.coconut.co/v2").replace(/\/$/, "");

export async function submitJob(opts: CoconutFinalizeOptions): Promise<CoconutJob> {
  if (!process.env.COCONUT_API_KEY) {
    throw new Error("COCONUT_API_KEY not set — Coconut finalize unavailable");
  }
  const spec = buildJobSpec(opts);
  const res = await fetch(`${API_BASE}/jobs`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${process.env.COCONUT_API_KEY}`,
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
    headers: { "Authorization": `Bearer ${process.env.COCONUT_API_KEY}` },
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
