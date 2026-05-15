import { type Express } from "express";
import { execFile } from "child_process";
import { promisify } from "util";
import { fileURLToPath } from "url";
import fs from "fs";
import path from "path";
import { YT_COOKIES_PATH } from "../index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Binary is downloaded to dist/yt-dlp at build time; this file compiles to dist/routes/transcript.js
const YT_DLP = path.resolve(__dirname, "..", "yt-dlp");

const execFileAsync = promisify(execFile);

function parseVtt(content: string): string {
  const lines = content.split("\n");
  const textLines: string[] = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (
      !trimmed ||
      trimmed === "WEBVTT" ||
      trimmed.startsWith("NOTE") ||
      trimmed.startsWith("STYLE") ||
      trimmed.startsWith("Kind:") ||
      trimmed.startsWith("Language:") ||
      /^\d{2}:\d{2}.*-->/.test(trimmed) ||
      /^\d+$/.test(trimmed)
    ) continue;
    textLines.push(trimmed.replace(/<[^>]+>/g, ""));
  }

  // Deduplicate consecutive identical lines (rolling captions repeat)
  const deduped = textLines.filter((line, i) => i === 0 || line !== textLines[i - 1]);
  return deduped.join(" ").replace(/\s+/g, " ").trim();
}

function parseSrt(content: string): string {
  return content
    .split("\n")
    .filter((line) => {
      const t = line.trim();
      return t && !/^\d+$/.test(t) && !/^\d{2}:\d{2}:\d{2},\d{3}/.test(t);
    })
    .join(" ")
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

export function setupTranscriptRoute(app: Express) {
  app.get("/api/transcript/:videoId", async (req, res) => {
    const { videoId } = req.params;

    if (!/^[a-zA-Z0-9_-]{11}$/.test(videoId)) {
      return res.status(400).json({ error: "Invalid video ID" });
    }

    const tmpDir = path.join("/tmp", `transcript-${videoId}-${Date.now()}`);
    fs.mkdirSync(tmpDir, { recursive: true });

    try {
      const hasCookies = fs.existsSync(YT_COOKIES_PATH);
      await execFileAsync(YT_DLP, [
        ...(hasCookies ? ["--cookies", YT_COOKIES_PATH] : []),
        "--write-auto-sub",
        "--write-sub",
        "--sub-lang", "en",
        "--skip-download",
        "--no-playlist",
        "--quiet",
        "--no-warnings",
        "--output", path.join(tmpDir, "%(id)s"),
        `https://www.youtube.com/watch?v=${videoId}`,
      ], { timeout: 45000 });

      const files = fs.readdirSync(tmpDir);
      const subFile = files.find((f) => f.endsWith(".vtt") || f.endsWith(".srt"));

      if (!subFile) {
        return res.status(404).json({ error: "No captions found for this video" });
      }

      const content = fs.readFileSync(path.join(tmpDir, subFile), "utf-8");
      const text = subFile.endsWith(".srt") ? parseSrt(content) : parseVtt(content);

      if (!text) {
        return res.status(404).json({ error: "Caption file was empty" });
      }

      console.log(`[transcript] yt-dlp succeeded for ${videoId} (${text.length} chars)`);
      return res.json({ text });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`[transcript] yt-dlp failed for ${videoId}: ${msg}`);
      return res.status(500).json({ error: msg });
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
}
