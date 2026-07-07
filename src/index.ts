import "dotenv/config";
import express from "express";
import cors from "cors";
import fs from "fs";
import { startVideoWorker, resumeBeatPoll, type QueuedBeat } from "./workers/video-worker.js";
import { setupHealthRoutes } from "./routes/health.js";
import { setupAssembleRoute } from "./routes/assemble.js";
import { setupTranscriptRoute } from "./routes/transcript.js";
import { supabase } from "./lib/supabase.js";

const app = express();
const PORT = parseInt(process.env.PORT || "3001");

const allowedOrigins = (process.env.ALLOWED_ORIGIN ?? "http://localhost:3000")
  .split(",")
  .map((o) => o.trim());

const corsOptions = {
  origin: (origin: string | undefined, cb: (err: Error | null, allow?: boolean) => void) => {
    if (!origin || allowedOrigins.includes(origin)) return cb(null, true);
    cb(new Error(`CORS: origin ${origin} not allowed`));
  },
  allowedHeaders: ["Content-Type", "Authorization", "Range"],
  exposedHeaders: ["Content-Range", "Accept-Ranges", "Content-Length"],
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
};

// Middleware — explicit OPTIONS handler must come before routes
app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.json({ limit: "10mb" }));

// Routes
setupHealthRoutes(app);
setupAssembleRoute(app);
setupTranscriptRoute(app);

app.get("/api/worker/status", (_req, res) => {
  res.json({ worker: "video-generation", running: true, timestamp: new Date().toISOString() });
});

// Write YouTube cookies to disk if provided — required for yt-dlp from datacenter IPs
export const YT_COOKIES_PATH = "/tmp/yt-cookies.txt";
const ytCookies = process.env.YOUTUBE_COOKIES;
if (ytCookies) {
  fs.writeFileSync(YT_COOKIES_PATH, ytCookies, "utf-8");
  console.log("[server] YouTube cookies written to", YT_COOKIES_PATH);
} else {
  console.warn("[server] YOUTUBE_COOKIES not set — yt-dlp will fail from datacenter IPs");
}

// Clear stale "processing" assemblies BEFORE accepting connections to avoid
// the race where the cleanup fires after a new assembly job has already started.
//
// Two-pass cleanup: honor in-flight stops FIRST, then re-queue the rest.
// If the worker was OOM-killed mid-encode after the user clicked Stop, the
// graceful catch in assemble.ts never ran — assembly_status is still
// "processing" and assembly_stop_requested is still true. Without this
// pass, the next worker would re-queue → re-claim → honor stop → clear
// the flag → another worker re-claims with no stop flag and runs to
// completion (or another OOM), and the UI flaps between states forever.
const { error: stopHonorError } = await supabase.from("projects")
  .update({
    assembly_status: "stopped",
    assembly_progress: "Stopped — click Resume to continue",
    assembly_stop_requested: false,
    assembly_finished_at: new Date().toISOString(),
  })
  .eq("assembly_status", "processing")
  .eq("assembly_stop_requested", true);
if (stopHonorError) console.error("[server] Failed to honor stale stop requests:", stopHonorError.message);
else console.log("[server] Honored stale stop requests on processing assemblies");

const { error: cleanupError } = await supabase.from("projects")
  .update({
    assembly_status: "queued",
    assembly_progress: "Queued…",
    assembly_error: null,
    // Defensively clear the finalize-preview flag too. If a worker
    // died mid-finalize-preview transition, leaving the flag true
    // would cause the next worker to immediately re-abort on first
    // poll — re-queueing without clearing creates a livelock.
    assembly_finalize_preview_requested: false,
  })
  .eq("assembly_status", "processing");
if (cleanupError) console.error("[server] Failed to re-queue stale assemblies:", cleanupError.message);
else console.log("[server] Re-queued stale processing assemblies");

// Recover any video beats that were mid-flight when the last worker
// instance died. Two paths:
//   • beat still has a valid video_job_id → RESUME polling the
//     existing KIE job instead of re-submitting. Prevents the "same
//     clip billed twice" pattern we hit whenever tsx-watch reloads
//     the worker or a Vercel deploy rolls the process. The KIE
//     render keeps running server-side while we were down.
//   • beat has no video_job_id (worker died before submit landed)
//     → reset to "queued" so the pollLoop picks it up cleanly.
{
  const { data: staleBeats, error: staleErr } = await supabase.from("project_beats")
    .select(`
      beat_number, project_id, video_prompt, image_url, video_status, video_job_id,
      video_model_id, video_duration, video_aspect_ratio, video_resolution,
      projects!inner(user_id, video_model_id, video_duration, video_aspect_ratio, video_resolution)
    `)
    .in("video_status", ["rendering", "submitting"]);

  if (staleErr) console.error("[server] Failed to query stale video beats:", staleErr.message);

  const rows = staleBeats ?? [];
  const toResume = rows.filter((r) => !!r.video_job_id);
  const toRequeue = rows.filter((r) => !r.video_job_id);

  // Reset each no-job-id beat individually so the update is scoped
  // to that (project_id, beat_number) pair — a bulk .in() couldn't
  // combine both keys cleanly.
  for (const r of toRequeue) {
    const { error } = await supabase.from("project_beats")
      .update({ video_status: "queued", video_job_id: null, video_error: null })
      .eq("project_id", r.project_id as string)
      .eq("beat_number", r.beat_number as number);
    if (error) console.error(`[server] Failed to re-queue beat ${r.beat_number}:`, error.message);
  }
  console.log(`[server] Video-beat recovery: resume=${toResume.length}, requeue=${toRequeue.length}`);

  // Kick off resume-polls in parallel. Each promise handles its own
  // errors (marking the beat failed) via the same catch handler as
  // the normal pollLoop path. Fire-and-forget on purpose — startup
  // continues, and the resume-polls run alongside the pollLoop.
  for (const r of toResume) {
    const proj = Array.isArray(r.projects) ? r.projects[0] : r.projects as Record<string, unknown>;
    const beatModelId = (r.video_model_id as string | null) ?? (proj?.video_model_id as string | null);
    const beatDuration = (r.video_duration as string | number | null) ?? (proj?.video_duration as string | number | null);
    const beatAspectRatio = (r.video_aspect_ratio as string | null) ?? (proj?.video_aspect_ratio as string | null);
    const beatResolution = (r.video_resolution as string | null) ?? (proj?.video_resolution as string | null);
    const userId = proj?.user_id as string | undefined;
    if (!beatModelId || !userId) {
      console.warn(`[server] Cannot resume beat ${r.beat_number} — missing model/user; leaving as-is`);
      continue;
    }
    const beat: QueuedBeat = {
      beat_number: r.beat_number as number,
      project_id: r.project_id as string,
      video_prompt: r.video_prompt as string,
      image_url: r.image_url as string | undefined,
      video_model_id: beatModelId,
      video_duration: beatDuration ?? undefined,
      video_aspect_ratio: beatAspectRatio ?? "16:9",
      video_resolution: beatResolution ?? undefined,
      user_id: userId,
    };
    void resumeBeatPoll(beat, r.video_job_id as string, r.video_status === "rendering")
      .catch(async (err: Error) => {
        console.error(`[server] Resume-poll failed for beat ${r.beat_number}:`, err.message);
        await supabase.from("project_beats")
          .update({ video_status: "failed", video_error: err.message })
          .eq("project_id", r.project_id as string)
          .eq("beat_number", r.beat_number as number);
      });
  }
}

// Start server
const server = app.listen(PORT, () => {
  console.log(`[server] Video worker service listening on port ${PORT}`);
  console.log(`[server] Environment: ${process.env.NODE_ENV || "development"}`);
  startVideoWorker();
});

async function gracefulShutdown(signal: string) {
  console.log(`[server] ${signal} received, shutting down gracefully...`);
  // Re-queue any in-progress assemblies so they resume after the worker comes back up
  await supabase.from("projects")
    .update({ assembly_status: "queued", assembly_progress: "Queued…", assembly_error: null })
    .eq("assembly_status", "processing");
  server.close(() => { console.log("[server] HTTP server closed"); process.exit(0); });
}

process.on("SIGTERM", () => { gracefulShutdown("SIGTERM").catch(console.error); });
process.on("SIGINT",  () => { gracefulShutdown("SIGINT").catch(console.error); });

process.on("uncaughtException", (err) => {
  console.error("[worker] uncaughtException:", err);
});

process.on("unhandledRejection", (err) => {
  console.error("[worker] unhandledRejection:", err);
});

export default app;
