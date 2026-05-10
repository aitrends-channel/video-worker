import "dotenv/config";
import express from "express";
import cors from "cors";
import { videoWorker } from "./workers/video-worker.js";
import { setupHealthRoutes } from "./routes/health.js";

const app = express();
const PORT = parseInt(process.env.PORT || "3001");

// Middleware
app.use(cors());
app.use(express.json());

// Routes
setupHealthRoutes(app);

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
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("[server] SIGTERM received, shutting down gracefully...");
  server.close(async () => {
    console.log("[server] HTTP server closed");
    await videoWorker.close();
    process.exit(0);
  });
});

process.on("SIGINT", async () => {
  console.log("[server] SIGINT received, shutting down gracefully...");
  server.close(async () => {
    console.log("[server] HTTP server closed");
    await videoWorker.close();
    process.exit(0);
  });
});

export default app;
