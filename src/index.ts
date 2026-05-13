import "dotenv/config";
import express from "express";
import cors from "cors";
import { startVideoWorker } from "./workers/video-worker.js";
import { setupHealthRoutes } from "./routes/health.js";
import { setupAssembleRoute } from "./routes/assemble.js";

const app = express();
const PORT = parseInt(process.env.PORT || "3001");

// Middleware
app.use(cors({
  origin: "*",
  allowedHeaders: ["Content-Type", "Authorization"],
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
}));
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

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("[server] SIGTERM received, shutting down gracefully...");
  server.close(() => { console.log("[server] HTTP server closed"); process.exit(0); });
});

process.on("SIGINT", () => {
  console.log("[server] SIGINT received, shutting down gracefully...");
  server.close(() => { console.log("[server] HTTP server closed"); process.exit(0); });
});

export default app;
