import "dotenv/config";
import express from "express";
import cors from "cors";
import { startVideoWorker } from "./workers/video-worker.js";
import { setupHealthRoutes } from "./routes/health.js";
import { setupAssembleRoute } from "./routes/assemble.js";
import { supabase } from "./lib/supabase.js";

const app = express();
const PORT = parseInt(process.env.PORT || "3001");

const corsOptions = {
  origin: "*",
  allowedHeaders: ["Content-Type", "Authorization"],
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
};

// Middleware — explicit OPTIONS handler must come before routes
app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.json({ limit: "10mb" }));

// Routes
setupHealthRoutes(app);
setupAssembleRoute(app);

// Video worker info endpoint
app.get("/api/worker/status", (req, res) => {
  res.json({
    worker: "video-generation",
    running: true,
    concurrency: 3,
    timestamp: new Date().toISOString(),
  });
});

// Start server
const server = app.listen(PORT, () => {
  console.log(`[server] Video worker service listening on port ${PORT}`);
  console.log(`[server] Environment: ${process.env.NODE_ENV || "development"}`);
  startVideoWorker();
});

async function gracefulShutdown(signal: string) {
  console.log(`[server] ${signal} received, shutting down gracefully...`);
  // Mark any in-progress assemblies as failed so the client shows a clear retry message
  await supabase.from("projects")
    .update({ assembly_status: "failed", assembly_error: "Worker restarted mid-assembly — please try again", assembly_progress: null })
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
