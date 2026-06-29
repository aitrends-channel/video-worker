import "dotenv/config";
import express from "express";
import cors from "cors";
import fs from "fs";
import { startVideoWorker } from "./workers/video-worker.js";
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

// Re-queue any video beats stuck in "rendering" OR "submitting" —
// they were mid-flight when the last instance died. Both states
// imply a worker had claimed the beat but never finished. Resetting
// to "queued" lets the new instance pick them up cleanly. Wiping
// video_job_id prevents any stale poll from a previous job from
// accidentally completing this row.
const { error: renderingError } = await supabase.from("project_beats")
  .update({ video_status: "queued", video_job_id: null, video_error: null })
  .in("video_status", ["rendering", "submitting"]);
if (renderingError) console.error("[server] Failed to re-queue stale rendering beats:", renderingError.message);
else console.log("[server] Re-queued stale rendering/submitting beats");

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
