import { createRequire } from "module";
import { type Express, type Request, type Response } from "express";
import { supabase } from "../lib/supabase.js";
import { uploadBuffer } from "../lib/storage.js";
import Anthropic from "@anthropic-ai/sdk";
import fs from "fs";
import path from "path";
import os from "os";

const require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-require-imports
const ffmpeg = require("fluent-ffmpeg") as typeof import("fluent-ffmpeg");
const ffmpegPath = require("ffmpeg-static") as string | null;
const ffprobeStatic = require("ffprobe-static") as { path: string };

if (ffmpegPath) ffmpeg.setFfmpegPath(ffmpegPath);
if (ffprobeStatic?.path) ffmpeg.setFfprobePath(ffprobeStatic.path);

// ── Helpers ───────────────────────────────────────────────────────────────────

async function getSettings(userId: string): Promise<{ elevenlabs_api_key: string; anthropic_api_key: string }> {
  const { data } = await supabase
    .from("app_settings")
    .select("elevenlabs_api_key, anthropic_api_key")
    .eq("user_id", userId)
    .single();
  return {
    elevenlabs_api_key: data?.elevenlabs_api_key?.trim() || process.env.ELEVENLABS_API_KEY || "",
    anthropic_api_key: data?.anthropic_api_key?.trim() || process.env.ANTHROPIC_API_KEY || "",
  };
}

async function downloadFile(url: string, dest: string): Promise<void> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status} ${res.statusText}`);
  const buf = await res.arrayBuffer();
  fs.writeFileSync(dest, Buffer.from(buf));
}

function getMediaDuration(filePath: string): Promise<number> {
  return new Promise((resolve, reject) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ffmpeg.ffprobe(filePath, (err: Error, meta: any) => {
      if (err) reject(new Error(`ffprobe failed: ${err.message}`));
      else resolve((meta?.format?.duration as number) ?? 0);
    });
  });
}

function normalizeClip(src: string, isImage: boolean, duration: number, output: string, w: number, h: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const vf = `scale=${w}:${h}:force_original_aspect_ratio=decrease,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:black,fps=24`;
    const cmd = ffmpeg();
    if (isImage) {
      cmd.input(src).inputOptions(["-loop", "1"]);
    } else {
      cmd.input(src).inputOptions(["-stream_loop", "-1"]);
    }
    cmd
      .outputOptions(["-t", String(duration), "-vf", vf, "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-an", "-pix_fmt", "yuv420p"])
      .output(output)
      .on("end", () => resolve())
      .on("error", (err: Error) => reject(new Error(`normalize clip failed: ${err.message}`)))
      .run();
  });
}

function blackClip(duration: number, output: string, w: number, h: number): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg()
      .input(`color=black:size=${w}x${h}:rate=24`)
      .inputOptions(["-f", "lavfi"])
      .outputOptions(["-t", String(duration), "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-an", "-pix_fmt", "yuv420p"])
      .output(output)
      .on("end", () => resolve())
      .on("error", (err: Error) => reject(new Error(`black clip failed: ${err.message}`)))
      .run();
  });
}

function concatClips(listFile: string, output: string): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg()
      .input(listFile)
      .inputOptions(["-f", "concat", "-safe", "0"])
      .outputOptions(["-c", "copy"])
      .output(output)
      .on("end", () => resolve())
      .on("error", (err: Error) => reject(new Error(`concat failed: ${err.message}`)))
      .run();
  });
}

function mixAudio(video: string, audio: string, output: string): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg()
      .input(video)
      .input(audio)
      .outputOptions(["-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-shortest"])
      .output(output)
      .on("end", () => resolve())
      .on("error", (err: Error) => reject(new Error(`audio mix failed: ${err.message}`)))
      .run();
  });
}

function burnSubtitles(video: string, assPath: string, output: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const escaped = assPath.replace(/\\/g, "/").replace(/:/g, "\\:");
    ffmpeg()
      .input(video)
      .outputOptions(["-vf", `ass='${escaped}'`, "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "copy"])
      .output(output)
      .on("end", () => resolve())
      .on("error", (err: Error) => reject(new Error(`subtitle burn failed: ${err.message}`)))
      .run();
  });
}

// ── Transcription ─────────────────────────────────────────────────────────────

interface TranscriptionWord {
  text?: string;
  word?: string;
  start?: number;
  start_time?: number;
  end?: number;
  end_time?: number;
  type?: string;
}

async function transcribeAudio(audioPath: string, apiKey: string): Promise<TranscriptionWord[]> {
  const audioBytes = fs.readFileSync(audioPath);
  const MAX_ATTEMPTS = 4;
  let lastError = "";

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const formData = new FormData();
    formData.append("file", new Blob([audioBytes], { type: "audio/mpeg" }), "voiceover.mp3");
    formData.append("model_id", "scribe_v1");
    formData.append("timestamps_granularity", "word");
    formData.append("tag_audio_events", "false");

    const res = await fetch("https://api.elevenlabs.io/v1/speech-to-text", {
      method: "POST",
      headers: { "xi-api-key": apiKey },
      body: formData,
    });

    if (res.ok) {
      const data = await res.json() as { words?: TranscriptionWord[] };
      return (data.words ?? []).filter((w) => (w.type ?? "word") === "word");
    }

    const body = await res.text();
    lastError = `ElevenLabs STT error ${res.status}: ${body}`;
    if (res.status !== 429 || attempt === MAX_ATTEMPTS) break;

    const delay = 2000 * Math.pow(2, attempt - 1);
    console.warn(`[assemble] transcription 429 — retrying in ${delay / 1000}s (attempt ${attempt}/${MAX_ATTEMPTS})`);
    await new Promise((r) => setTimeout(r, delay));
  }

  throw new Error(lastError);
}

// ── Beat alignment ────────────────────────────────────────────────────────────

function normalizeWord(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function alignBeats(beatTexts: string[], transcriptionWords: TranscriptionWord[], totalAudioDuration: number): number[] {
  const getStart = (w: TranscriptionWord) => w.start ?? w.start_time ?? 0;

  if (!transcriptionWords.length) {
    const wordCounts = beatTexts.map((t) => Math.max(1, t.trim().split(/\s+/).filter(Boolean).length));
    const total = wordCounts.reduce((s, n) => s + n, 0);
    return wordCounts.map((n) => Math.max(0.5, (n / total) * totalAudioDuration));
  }

  const transcriptNorm = transcriptionWords.map((w) => normalizeWord(w.text ?? w.word ?? ""));
  let searchFrom = 0;
  const beatStartIndices: number[] = [];

  for (const beatText of beatTexts) {
    const beatWords = beatText.trim().split(/\s+/).filter(Boolean).map(normalizeWord).filter(Boolean);

    if (!beatWords.length || searchFrom >= transcriptionWords.length) {
      beatStartIndices.push(Math.min(searchFrom, transcriptionWords.length - 1));
      continue;
    }

    const window = beatWords.slice(0, 3);
    let bestIdx = searchFrom;

    for (let i = searchFrom; i < transcriptionWords.length; i++) {
      let matches = 0;
      for (let j = 0; j < window.length && i + j < transcriptionWords.length; j++) {
        if (transcriptNorm[i + j] === window[j]) matches++;
      }
      if (matches >= Math.min(2, window.length)) {
        bestIdx = i;
        break;
      }
    }

    beatStartIndices.push(bestIdx);
    searchFrom = bestIdx + 1;
  }

  const durations: number[] = [];
  for (let i = 0; i < beatTexts.length; i++) {
    const startIdx = beatStartIndices[i];
    const nextIdx = i < beatTexts.length - 1 ? beatStartIndices[i + 1] : transcriptionWords.length;
    const start = getStart(transcriptionWords[Math.min(startIdx, transcriptionWords.length - 1)]);
    const end = nextIdx < transcriptionWords.length
      ? getStart(transcriptionWords[nextIdx])
      : totalAudioDuration;
    durations.push(Math.max(0.5, end - start));
  }

  return durations;
}

// ── Caption / ASS ─────────────────────────────────────────────────────────────

interface SrtSegment { index: number; start: number; end: number; text: string; }

interface AssStyle {
  fontSize: number; alignment: number; marginV: number;
  primaryColour: string; outlineColour: string; backColour: string;
  bold: number; borderStyle: number; outline: number; shadow: number;
}

function buildSrtSegmentsFromBeats(beats: Beat[], durations: number[], wordsPerSegment = 7): SrtSegment[] {
  const segments: SrtSegment[] = [];
  let cursor = 0;
  for (let i = 0; i < beats.length; i++) {
    const duration = durations[i];
    const words = (beats[i].script_segment ?? "").trim().split(/\s+/).filter(Boolean);
    if (words.length) {
      for (let j = 0; j < words.length; j += wordsPerSegment) {
        const chunk = words.slice(j, j + wordsPerSegment);
        const start = cursor + (j / words.length) * duration;
        const end = cursor + (Math.min(j + wordsPerSegment, words.length) / words.length) * duration;
        segments.push({ index: segments.length + 1, start, end, text: chunk.join(" ") });
      }
    }
    cursor += duration;
  }
  return segments;
}

function buildSrtSegments(words: TranscriptionWord[], wordsPerSegment = 7): SrtSegment[] {
  const getStart = (w: TranscriptionWord) => w.start ?? w.start_time ?? 0;
  const getEnd = (w: TranscriptionWord) => w.end ?? w.end_time ?? getStart(w) + 2;

  const segments: SrtSegment[] = [];
  for (let i = 0; i < words.length; i += wordsPerSegment) {
    const chunk = words.slice(i, Math.min(i + wordsPerSegment, words.length));
    const start = getStart(chunk[0]);
    const lastWord = chunk[chunk.length - 1];
    const nextWord = words[i + wordsPerSegment];
    const end = getEnd(lastWord) || (nextWord ? getStart(nextWord) : start + 3);
    const text = chunk.map((w) => (w.text ?? w.word ?? "").trim()).filter(Boolean).join(" ");
    if (text) segments.push({ index: segments.length + 1, start, end, text });
  }
  return segments;
}

function toAssTime(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  const cs = Math.round((seconds % 1) * 100);
  return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}.${String(cs).padStart(2, "0")}`;
}

function buildAssStyle(style: string, size: string, position: string, videoH: number): AssStyle {
  const fontPct = size === "small" ? 0.030 : size === "large" ? 0.052 : 0.040;
  const fontSize = Math.round(videoH * fontPct);
  const alignment = position === "top" ? 8 : 2;
  const marginV = Math.round(videoH * 0.03);
  switch (style) {
    case "bold":
      return { fontSize, alignment, marginV, primaryColour: "&H0000FFFF", outlineColour: "&H00000000", backColour: "&H00000000", bold: 1, borderStyle: 1, outline: 2, shadow: 0 };
    case "boxed":
      return { fontSize, alignment, marginV, primaryColour: "&H00FFFFFF", outlineColour: "&H00000000", backColour: "&H80000000", bold: 0, borderStyle: 3, outline: 0, shadow: 0 };
    case "minimal":
      return { fontSize, alignment, marginV, primaryColour: "&H00FFFFFF", outlineColour: "&H00000000", backColour: "&H00000000", bold: 0, borderStyle: 1, outline: 1, shadow: 0 };
    default:
      return { fontSize, alignment, marginV, primaryColour: "&H00FFFFFF", outlineColour: "&H00000000", backColour: "&H00000000", bold: 0, borderStyle: 1, outline: 2, shadow: 1 };
  }
}

function writeAss(segments: SrtSegment[], s: AssStyle, videoW: number, videoH: number, filePath: string): void {
  const styleRow =
    `Style: Default,Arial,${s.fontSize},${s.primaryColour},&H000000FF,` +
    `${s.outlineColour},${s.backColour},${s.bold},0,0,0,100,100,0,0,` +
    `${s.borderStyle},${s.outline},${s.shadow},${s.alignment},10,10,${s.marginV},1`;

  const dialogues = segments
    .map((seg) => `Dialogue: 0,${toAssTime(seg.start)},${toAssTime(seg.end)},Default,,0,0,0,,${seg.text}`)
    .join("\n");

  const content =
    `[Script Info]\nScriptType: v4.00+\nPlayResX: ${videoW}\nPlayResY: ${videoH}\n\n` +
    `[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n${styleRow}\n\n` +
    `[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n${dialogues}`;

  fs.writeFileSync(filePath, content, "utf-8");
}

async function translateSegments(segments: SrtSegment[], targetLanguage: string, anthropicKey: string): Promise<SrtSegment[]> {
  if (!segments.length) return segments;
  const anthropic = new Anthropic({ apiKey: anthropicKey });
  const numbered = segments.map((s) => `${s.index}. ${s.text}`).join("\n");
  const msg = await anthropic.messages.create({
    model: "claude-sonnet-4-6",
    max_tokens: 4096,
    messages: [{
      role: "user",
      content: `Translate these numbered caption lines to ${targetLanguage}. Return only the translated lines in exactly the same "N. text" format, one per line:\n\n${numbered}`,
    }],
  });
  const raw = msg.content[0].type === "text" ? msg.content[0].text.trim() : "";
  const lines = raw.split("\n").filter(Boolean);
  return segments.map((seg) => {
    const line = lines.find((l) => l.startsWith(`${seg.index}.`));
    if (!line) return seg;
    return { ...seg, text: line.replace(/^\d+\.\s*/, "").trim() };
  });
}

// ── Types ─────────────────────────────────────────────────────────────────────

type Beat = {
  beat_number: number;
  script_segment: string | null;
  video_url: string | null;
  image_url: string | null;
};

// ── Route ─────────────────────────────────────────────────────────────────────

export function setupAssembleRoute(app: Express): void {
  app.post("/api/assemble", async (req: Request, res: Response): Promise<void> => {
    const { token, projectId, aspectRatio = "16:9", voiceoverType = "cleaned",
      captionsEnabled = false, captionsLanguage = "source",
      captionsStyle = "classic", captionsSize = "medium", captionsPosition = "bottom",
    } = req.body as {
      token: string; projectId: string; aspectRatio?: string; voiceoverType?: "cleaned" | "original";
      captionsEnabled?: boolean; captionsLanguage?: string;
      captionsStyle?: string; captionsSize?: string; captionsPosition?: string;
    };

    if (!token) { res.status(401).json({ error: "Unauthorized" }); return; }
    if (!projectId) { res.status(400).json({ error: "projectId required" }); return; }

    const { data: { user }, error: authError } = await supabase.auth.getUser(token);
    if (authError || !user) { res.status(401).json({ error: "Unauthorized" }); return; }

    const [w, h] = aspectRatio === "9:16" ? [1080, 1920] : aspectRatio === "1:1" ? [1080, 1080] : [1920, 1080];

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.flushHeaders();

    const send = (event: object) => {
      res.write(`data: ${JSON.stringify(event)}\n\n`);
      // Flush so the client receives each event immediately, not buffered
      if (typeof (res as unknown as { flush?: () => void }).flush === "function") {
        (res as unknown as { flush: () => void }).flush();
      }
    };

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "assemble-"));

    try {
      send({ type: "status", message: "Loading project data…" });

      const [projectRes, beatsRes] = await Promise.all([
        supabase.from("projects").select("tts_url, tts_cleaned_url").eq("id", projectId).eq("user_id", user.id).single(),
        supabase.from("project_beats").select("beat_number, script_segment, video_url, image_url").eq("project_id", projectId).order("beat_number"),
      ]);

      if (projectRes.error) throw new Error("Project not found");

      const proj = projectRes.data as { tts_url: string | null; tts_cleaned_url: string | null };
      const beats = (beatsRes.data ?? []) as Beat[];

      const voiceoverUrl = voiceoverType === "original"
        ? (proj.tts_url ?? proj.tts_cleaned_url)
        : (proj.tts_cleaned_url ?? proj.tts_url);

      if (!voiceoverUrl) throw new Error("No voiceover found — generate a voiceover on the Generate page first.");
      if (!beats.length) throw new Error("No beats found in this project.");

      send({ type: "status", message: "Downloading voiceover…" });
      const voiceoverPath = path.join(tmpDir, "voiceover.mp3");
      await downloadFile(voiceoverUrl, voiceoverPath);

      const totalDuration = await getMediaDuration(voiceoverPath);
      if (totalDuration <= 0) throw new Error("Could not determine voiceover duration");

      send({ type: "status", message: "Transcribing voiceover…" });
      let transcriptionWords: TranscriptionWord[] = [];
      try {
        const { elevenlabs_api_key } = await getSettings(user.id);
        if (!elevenlabs_api_key) throw new Error("ElevenLabs API key not configured.");
        transcriptionWords = await transcribeAudio(voiceoverPath, elevenlabs_api_key);
        console.log(`[assemble] transcription: ${transcriptionWords.length} words`);
      } catch (transcribeErr) {
        console.warn("[assemble] transcription failed, falling back to proportional:", transcribeErr);
      }

      const beatTexts = beats.map((b) => b.script_segment ?? "");
      const durations = alignBeats(beatTexts, transcriptionWords, totalDuration);

      send({ type: "status", message: "Processing video clips…" });
      const clipPaths: string[] = [];

      for (let i = 0; i < beats.length; i++) {
        const beat = beats[i];
        const duration = durations[i];
        const clipPath = path.join(tmpDir, `clip_${String(i).padStart(3, "0")}.mp4`);

        send({ type: "progress", current: i + 1, total: beats.length, message: `Processing beat ${beat.beat_number}…` });

        try {
          if (beat.video_url) {
            const ext = beat.video_url.includes(".webm") ? "webm" : "mp4";
            const srcPath = path.join(tmpDir, `src_${i}.${ext}`);
            await downloadFile(beat.video_url, srcPath);
            await normalizeClip(srcPath, false, duration, clipPath, w, h);
          } else if (beat.image_url) {
            const ext = beat.image_url.toLowerCase().includes(".png") ? "png" : "jpg";
            const srcPath = path.join(tmpDir, `src_${i}.${ext}`);
            await downloadFile(beat.image_url, srcPath);
            await normalizeClip(srcPath, true, duration, clipPath, w, h);
          } else {
            await blackClip(duration, clipPath, w, h);
          }
        } catch (clipErr) {
          console.error(`[assemble] beat ${beat.beat_number} clip error:`, clipErr);
          await blackClip(duration, clipPath, w, h);
        }

        clipPaths.push(clipPath);
      }

      send({ type: "status", message: "Joining clips…" });
      const listPath = path.join(tmpDir, "concat.txt");
      fs.writeFileSync(listPath, clipPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n"));
      const joinedPath = path.join(tmpDir, "joined.mp4");
      await concatClips(listPath.replace(/\\/g, "/"), joinedPath);

      send({ type: "status", message: "Mixing voiceover…" });
      const outputPath = path.join(tmpDir, "output.mp4");
      await mixAudio(joinedPath, voiceoverPath, outputPath);

      let finalOutputPath = outputPath;
      if (captionsEnabled) {
        try {
          send({ type: "status", message: "Generating captions…" });
          let segments = transcriptionWords.length > 0
            ? buildSrtSegments(transcriptionWords)
            : buildSrtSegmentsFromBeats(beats, durations);

          if (captionsLanguage !== "source") {
            send({ type: "status", message: `Translating captions to ${captionsLanguage}…` });
            const { anthropic_api_key } = await getSettings(user.id);
            if (!anthropic_api_key) throw new Error("Anthropic API key not configured.");
            segments = await translateSegments(segments, captionsLanguage, anthropic_api_key);
          }

          const assPath = path.join(tmpDir, "captions.ass");
          const assStyle = buildAssStyle(captionsStyle, captionsSize, captionsPosition, h);
          writeAss(segments, assStyle, w, h, assPath);

          send({ type: "status", message: "Burning captions into video…" });
          const captionedPath = path.join(tmpDir, "captioned.mp4");
          await burnSubtitles(outputPath, assPath, captionedPath);
          finalOutputPath = captionedPath;
        } catch (captionErr) {
          const msg = captionErr instanceof Error ? captionErr.message : String(captionErr);
          console.warn("[assemble] caption burn failed:", msg);
          send({ type: "caption_warn", message: `Captions failed (${msg}) — assembling without captions.` });
        }
      }

      send({ type: "status", message: "Uploading assembled video…" });
      const buf = fs.readFileSync(finalOutputPath);
      const publicUrl = await uploadBuffer(
        `${projectId}/assembled_${Date.now()}.mp4`,
        buf.buffer as ArrayBuffer,
        "video/mp4"
      );

      await supabase.from("projects")
        .update({ assembled_url: publicUrl })
        .eq("id", projectId)
        .eq("user_id", user.id);

      send({ type: "done", url: publicUrl });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Assembly failed";
      console.error("[assemble]", message);
      send({ type: "error", message });
    } finally {
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
      res.end();
    }
  });
}
