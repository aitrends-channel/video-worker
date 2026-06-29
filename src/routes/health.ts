import { Express } from "express";

export function setupHealthRoutes(app: Express) {
  app.get("/health", (_req, res) => {
    res.json({
      status: "ok",
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
    });
  });

  app.get("/health/ready", (_req, res) => {
    res.json({
      ready: true,
      timestamp: new Date().toISOString(),
    });
  });
}
