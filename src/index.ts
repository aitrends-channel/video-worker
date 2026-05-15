import "dotenv/config";
import express from "express";
import cors from "cors";
import { startVideoWorker } from "./workers/video-worker.js";
import { setupHealthRoutes } from "./routes/health.js";
import { setupAssembleRoute } from "./routes/assemble.js";
import { setupTranscriptRoute } from "./routes/transcript.js";
import { supabase } from "./lib/supabase.js";

const app = express();
const PORT = parseInt(process.env.PORT || "3001");

const corsOptions = {
  origin: "*",
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

// Video worker info endpoint
app.get("/api/worker/status", (req, res) => {
  res.json({
    worker: "video-generation",
    running: true,
    concurrency: 3,
    timestamp: new Date().toISOString(),
  });
});

// Clear stale "processing" assemblies BEFORE accepting connections to avoid
// the race where the cleanup fires after a new assembly job has already started
const { error: cleanupError } = await supabase.from("projects")
  .update({ assembly_status: "queued", assembly_progress: "Queued…", assembly_error: null })
  .eq("assembly_status", "processing");
if (cleanupError) console.error("[server] Failed to re-queue stale assemblies:", cleanupError.message);
else console.log("[server] Re-queued stale processing assemblies");

// Re-queue any video beats stuck in "rendering" — they were mid-flight when the last instance died
const { error: renderingError } = await supabase.from("project_beats")
  .update({ video_status: "queued", video_job_id: null, video_error: null })
  .eq("video_status", "rendering");
if (renderingError) console.error("[server] Failed to re-queue stale rendering beats:", renderingError.message);
else console.log("[server] Re-queued stale rendering beats");

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
