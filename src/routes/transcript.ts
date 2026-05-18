import { createRequire } from "module";
import { type Express } from "express";
import { execFile } from "child_process";
import { promisify } from "util";
import { fileURLToPath } from "url";
import fs from "fs";
import path from "path";
import { YT_COOKIES_PATH } from "../index.js";
import { supabase } from "../lib/supabase.js";

const _require = createRequire(import.meta.url);
const ffmpegPath = _require("ffmpeg-static") as string | null;

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

async function transcribeWithGroq(audioPath: string, groqKey: string): Promise<string> {
  const audioBuffer = fs.readFileSync(audioPath);
  const ext = path.extname(audioPath).slice(1).toLowerCase();
  const mimeTypes: Record<string, string> = {
    m4a: "audio/mp4",
    webm: "audio/webm",
    ogg: "audio/ogg",
    mp3: "audio/mpeg",
    wav: "audio/wav",
    opus: "audio/ogg",
    flac: "audio/flac",
  };
  const mimeType = mimeTypes[ext] ?? "audio/mpeg";

  const formData = new FormData();
  formData.append("file", new Blob([audioBuffer], { type: mimeType }), path.basename(audioPath));
  formData.append("model", "whisper-large-v3-turbo");
  formData.append("response_format", "text");
  formData.append("language", "en");

  const res = await fetch("https://api.groq.com/openai/v1/audio/transcriptions", {
    method: "POST",
    headers: { Authorization: `Bearer ${groqKey}` },
    body: formData,
  });

  if (!res.ok) {
    const errText = await res.text().catch(() => String(res.status));
    throw new Error(`Groq API ${res.status}: ${errText}`);
  }

  return (await res.text()).trim();
}

export function setupTranscriptRoute(app: Express) {
  app.get("/api/transcript/:videoId", async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, "");
    if (!token) return res.status(401).json({ error: "Unauthorized" });

    const { data: { user }, error: authError } = await supabase.auth.getUser(token);
    if (authError || !user) return res.status(401).json({ error: "Unauthorized" });

    const { videoId } = req.params;

    if (!/^[a-zA-Z0-9_-]{11}$/.test(videoId)) {
      return res.status(400).json({ error: "Invalid video ID" });
    }

    const tmpDir = path.join("/tmp", `transcript-${videoId}-${Date.now()}`);
    fs.mkdirSync(tmpDir, { recursive: true });

    try {
      const hasCookies = fs.existsSync(YT_COOKIES_PATH);
      const cookieArgs = hasCookies ? ["--cookies", YT_COOKIES_PATH] : [];
      const ffmpegArgs = ffmpegPath ? ["--ffmpeg-location", ffmpegPath] : [];

      // --- Step 1: Try captions (fast, no download needed) ---
      let captionText: string | null = null;
      try {
        await execFileAsync(YT_DLP, [
          ...cookieArgs,
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

        if (subFile) {
          const content = fs.readFileSync(path.join(tmpDir, subFile), "utf-8");
          const text = subFile.endsWith(".srt") ? parseSrt(content) : parseVtt(content);
          if (text) captionText = text;
        }
      } catch (captionErr) {
        console.warn(`[transcript] captions failed for ${videoId}: ${captionErr instanceof Error ? captionErr.message : String(captionErr)}`);
      }

      if (captionText) {
        console.log(`[transcript] captions succeeded for ${videoId} (${captionText.length} chars)`);
        return res.json({ text: captionText, source: "captions" });
      }

      // --- Step 2: Audio download + Groq Whisper ---
      const groqKey = process.env.GROQ_API_KEY;
      if (!groqKey) {
        return res.status(404).json({ error: "No captions available and GROQ_API_KEY is not configured" });
      }

      console.log(`[transcript] falling back to audio transcription for ${videoId}`);

      await execFileAsync(YT_DLP, [
        ...cookieArgs,
        ...ffmpegArgs,
        "-f", "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio",
        "--no-playlist",
        "--quiet",
        "--no-warnings",
        "--output", path.join(tmpDir, "audio.%(ext)s"),
        `https://www.youtube.com/watch?v=${videoId}`,
      ], { timeout: 120000 });

      const audioFiles = fs.readdirSync(tmpDir).filter((f) => f.startsWith("audio."));
      if (!audioFiles.length) {
        return res.status(500).json({ error: "Audio download produced no output file" });
      }

      const audioPath = path.join(tmpDir, audioFiles[0]);
      const sizeMB = (fs.statSync(audioPath).size / 1024 / 1024).toFixed(1);
      console.log(`[transcript] audio downloaded for ${videoId}: ${audioFiles[0]} (${sizeMB} MB)`);

      const text = await transcribeWithGroq(audioPath, groqKey);
      if (!text) {
        return res.status(500).json({ error: "Whisper returned empty transcript" });
      }

      console.log(`[transcript] Whisper succeeded for ${videoId} (${text.length} chars)`);
      return res.json({ text, source: "whisper" });

    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`[transcript] failed for ${videoId}: ${msg}`);
      return res.status(500).json({ error: msg });
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
}
