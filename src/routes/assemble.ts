import { createRequire } from "module";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { type Express, type Request, type Response } from "express";
import { supabase } from "../lib/supabase.js";
import { uploadFile } from "../lib/storage.js";
import Anthropic from "@anthropic-ai/sdk";
import fs from "fs";
import path from "path";
import os from "os";

const _require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-require-imports
const ffmpeg = _require("fluent-ffmpeg") as typeof import("fluent-ffmpeg");
const ffmpegPath = _require("ffmpeg-static") as string | null;
const ffprobeStatic = _require("ffprobe-static") as { path: string };

if (ffmpegPath) ffmpeg.setFfmpegPath(ffmpegPath);
if (ffprobeStatic?.path) ffmpeg.setFfprobePath(ffprobeStatic.path);

console.log(`[assemble] ffmpeg: ${ffmpegPath}`);
console.log(`[assemble] ffprobe: ${ffprobeStatic?.path}`);

// ── Progress helper ───────────────────────────────────────────────────────────

async function setProgress(projectId: string, progress: string) {
  await supabase.from("projects")
    .update({ assembly_progress: progress })
    .eq("id", projectId);
}

// ── Settings ──────────────────────────────────────────────────────────────────

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

// ── ffmpeg helpers ────────────────────────────────────────────────────────────

async function downloadFile(url: string, dest: string): Promise<void> {
  const res = await fetch(url, { signal: AbortSignal.timeout(60_000) });
  if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
  if (!res.body) throw new Error(`No response body for ${url}`);
  const nodeStream = Readable.fromWeb(res.body as import("stream/web").ReadableStream);
  await pipeline(nodeStream, fs.createWriteStream(dest));
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

const FFMPEG_TIMEOUT_MS = 10 * 60_000;

function ffmpegWithTimeout(
  build: (cmd: ReturnType<typeof ffmpeg>) => ReturnType<typeof ffmpeg>,
  label: string
): Promise<void> {
  return new Promise((resolve, reject) => {
    const cmd = build(ffmpeg());
    let settled = false;
    const settle = (fn: () => void) => {
      if (!settled) { settled = true; clearTimeout(timer); fn(); }
    };
    const timer = setTimeout(() => {
      settle(() => {
        try { cmd.kill("SIGKILL"); } catch { /* ignore */ }
        reject(new Error(`ffmpeg timed out: ${label}`));
      });
    }, FFMPEG_TIMEOUT_MS);
    cmd
      .on("end", () => settle(resolve))
      .on("error", (err: Error) => settle(() => reject(new Error(`${label} failed: ${err.message}`))))
      .run();
  });
}

function normalizeClip(src: string, isImage: boolean, duration: number, output: string, w: number, h: number): Promise<void> {
  const vf = `scale=${w}:${h}:force_original_aspect_ratio=decrease,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:black,fps=24`;
  return ffmpegWithTimeout((cmd) => {
    if (isImage) cmd.input(src).inputOptions(["-loop", "1"]);
    else cmd.input(src).inputOptions(["-stream_loop", "-1"]);
    return cmd
      .outputOptions(["-t", String(duration), "-vf", vf, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-an", "-pix_fmt", "yuv420p", "-threads", "1"])
      .output(output);
  }, `normalizeClip`);
}


function concatClips(listFile: string, output: string): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg()
      .input(listFile).inputOptions(["-f", "concat", "-safe", "0"])
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
      .input(video).input(audio)
      .outputOptions(["-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-shortest"])
      .output(output)
      .on("end", () => resolve())
      .on("error", (err: Error) => reject(new Error(`audio mix failed: ${err.message}`)))
      .run();
  });
}

function burnSubtitles(video: string, assPath: string, output: string): Promise<void> {
  const escaped = assPath.replace(/\\/g, "/").replace(/:/g, "\\:");
  return ffmpegWithTimeout((cmd) =>
    cmd
      .input(video)
      .outputOptions(["-vf", `ass='${escaped}'`, "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "copy", "-threads", "1"])
      .output(output),
  "burnSubtitles");
}

// ── Transcription ─────────────────────────────────────────────────────────────

interface TranscriptionWord {
  text?: string; word?: string;
  start?: number; start_time?: number;
  end?: number; end_time?: number;
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
      method: "POST", headers: { "xi-api-key": apiKey }, body: formData,
    });
    if (res.ok) {
      const data = await res.json() as { words?: TranscriptionWord[] };
      return (data.words ?? []).filter((w) => (w.type ?? "word") === "word");
    }
    lastError = `ElevenLabs STT ${res.status}: ${await res.text()}`;
    if (res.status !== 429 || attempt === MAX_ATTEMPTS) break;
    await new Promise((r) => setTimeout(r, 2000 * Math.pow(2, attempt - 1)));
  }
  throw new Error(lastError);
}

// ── Beat alignment ────────────────────────────────────────────────────────────

function normalizeWord(s: string): string { return s.toLowerCase().replace(/[^a-z0-9]/g, ""); }

function alignBeats(beatTexts: string[], words: TranscriptionWord[], totalDuration: number): number[] {
  const getStart = (w: TranscriptionWord) => w.start ?? w.start_time ?? 0;
  if (!words.length) {
    const counts = beatTexts.map((t) => Math.max(1, t.trim().split(/\s+/).filter(Boolean).length));
    const total = counts.reduce((s, n) => s + n, 0);
    return counts.map((n) => Math.max(0.5, (n / total) * totalDuration));
  }
  const norm = words.map((w) => normalizeWord(w.text ?? w.word ?? ""));
  let from = 0;
  const startIdxs: number[] = [];
  for (const text of beatTexts) {
    const beatWords = text.trim().split(/\s+/).filter(Boolean).map(normalizeWord).filter(Boolean);
    if (!beatWords.length || from >= words.length) { startIdxs.push(Math.min(from, words.length - 1)); continue; }
    const win = beatWords.slice(0, 3);
    let best = from;
    for (let i = from; i < words.length; i++) {
      let m = 0;
      for (let j = 0; j < win.length && i + j < words.length; j++) if (norm[i + j] === win[j]) m++;
      if (m >= Math.min(2, win.length)) { best = i; break; }
    }
    startIdxs.push(best);
    from = best + 1;
  }
  const durations: number[] = [];
  for (let i = 0; i < beatTexts.length; i++) {
    const si = startIdxs[i];
    const ni = i < beatTexts.length - 1 ? startIdxs[i + 1] : words.length;
    const start = getStart(words[Math.min(si, words.length - 1)]);
    const end = ni < words.length ? getStart(words[ni]) : totalDuration;
    durations.push(Math.max(0.5, end - start));
  }
  return durations;
}

// ── Caption / ASS ─────────────────────────────────────────────────────────────

interface SrtSegment { index: number; start: number; end: number; text: string; }
interface AssStyle { fontSize: number; alignment: number; marginV: number; primaryColour: string; outlineColour: string; backColour: string; bold: number; borderStyle: number; outline: number; shadow: number; }

type Beat = { beat_number: number; script_segment: string | null; video_url: string | null; image_url: string | null; };

function buildSrtSegmentsFromBeats(beats: Beat[], durations: number[], wps = 7): SrtSegment[] {
  const segs: SrtSegment[] = []; let cursor = 0;
  for (let i = 0; i < beats.length; i++) {
    const dur = durations[i];
    const words = (beats[i].script_segment ?? "").trim().split(/\s+/).filter(Boolean);
    for (let j = 0; j < words.length; j += wps) {
      const chunk = words.slice(j, j + wps);
      segs.push({ index: segs.length + 1, start: cursor + (j / words.length) * dur, end: cursor + (Math.min(j + wps, words.length) / words.length) * dur, text: chunk.join(" ") });
    }
    cursor += dur;
  }
  return segs;
}

function buildSrtSegments(words: TranscriptionWord[], wps = 7): SrtSegment[] {
  const getS = (w: TranscriptionWord) => w.start ?? w.start_time ?? 0;
  const getE = (w: TranscriptionWord) => w.end ?? w.end_time ?? getS(w) + 2;
  const segs: SrtSegment[] = [];
  for (let i = 0; i < words.length; i += wps) {
    const chunk = words.slice(i, Math.min(i + wps, words.length));
    const start = getS(chunk[0]);
    const end = getE(chunk[chunk.length - 1]) || (words[i + wps] ? getS(words[i + wps]) : start + 3);
    const text = chunk.map((w) => (w.text ?? w.word ?? "").trim()).filter(Boolean).join(" ");
    if (text) segs.push({ index: segs.length + 1, start, end, text });
  }
  return segs;
}

function toAss(s: number): string {
  return `${Math.floor(s / 3600)}:${String(Math.floor((s % 3600) / 60)).padStart(2, "0")}:${String(Math.floor(s % 60)).padStart(2, "0")}.${String(Math.round((s % 1) * 100)).padStart(2, "0")}`;
}

function buildAssStyle(style: string, size: string, position: string, h: number): AssStyle {
  const fs2 = Math.round(h * (size === "small" ? 0.030 : size === "large" ? 0.052 : 0.040));
  const al = position === "top" ? 8 : 2;
  const mv = Math.round(h * 0.03);
  switch (style) {
    case "bold":    return { fontSize: fs2, alignment: al, marginV: mv, primaryColour: "&H0000FFFF", outlineColour: "&H00000000", backColour: "&H00000000", bold: 1, borderStyle: 1, outline: 2, shadow: 0 };
    case "boxed":   return { fontSize: fs2, alignment: al, marginV: mv, primaryColour: "&H00FFFFFF", outlineColour: "&H00000000", backColour: "&H80000000", bold: 0, borderStyle: 3, outline: 0, shadow: 0 };
    case "minimal": return { fontSize: fs2, alignment: al, marginV: mv, primaryColour: "&H00FFFFFF", outlineColour: "&H00000000", backColour: "&H00000000", bold: 0, borderStyle: 1, outline: 1, shadow: 0 };
    default:        return { fontSize: fs2, alignment: al, marginV: mv, primaryColour: "&H00FFFFFF", outlineColour: "&H00000000", backColour: "&H00000000", bold: 0, borderStyle: 1, outline: 2, shadow: 1 };
  }
}

function writeAss(segs: SrtSegment[], s: AssStyle, w: number, h: number, file: string): void {
  const row = `Style: Default,Arial,${s.fontSize},${s.primaryColour},&H000000FF,${s.outlineColour},${s.backColour},${s.bold},0,0,0,100,100,0,0,${s.borderStyle},${s.outline},${s.shadow},${s.alignment},10,10,${s.marginV},1`;
  const dlg = segs.map((sg) => `Dialogue: 0,${toAss(sg.start)},${toAss(sg.end)},Default,,0,0,0,,${sg.text}`).join("\n");
  fs.writeFileSync(file,
    `[Script Info]\nScriptType: v4.00+\nPlayResX: ${w}\nPlayResY: ${h}\n\n` +
    `[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n${row}\n\n` +
    `[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n${dlg}`, "utf-8");
}

async function translateSegments(segs: SrtSegment[], lang: string, key: string): Promise<SrtSegment[]> {
  if (!segs.length) return segs;
  const anthropic = new Anthropic({ apiKey: key });
  const numbered = segs.map((s) => `${s.index}. ${s.text}`).join("\n");
  const msg = await anthropic.messages.create({
    model: "claude-sonnet-4-6", max_tokens: 4096,
    messages: [{ role: "user", content: `Translate these numbered caption lines to ${lang}. Return only the translated lines in exactly the same "N. text" format, one per line:\n\n${numbered}` }],
  });
  const raw = msg.content[0].type === "text" ? msg.content[0].text.trim() : "";
  const lines = raw.split("\n").filter(Boolean);
  return segs.map((s) => {
    const line = lines.find((l) => l.startsWith(`${s.index}.`));
    return line ? { ...s, text: line.replace(/^\d+\.\s*/, "").trim() } : s;
  });
}

// ── Background assembly job ───────────────────────────────────────────────────

interface AssembleOptions {
  userId: string; projectId: string; aspectRatio: string; voiceoverType: "cleaned" | "original";
  captionsEnabled: boolean; captionsLanguage: string; captionsStyle: string; captionsSize: string; captionsPosition: string;
}

async function runAssembly(opts: AssembleOptions): Promise<void> {
  const { userId, projectId, aspectRatio, voiceoverType, captionsEnabled, captionsLanguage, captionsStyle, captionsSize, captionsPosition } = opts;
  const [w, h] = aspectRatio === "9:16" ? [480, 854] : aspectRatio === "1:1" ? [480, 480] : [854, 480];

  const progress = (msg: string) => {
    console.log(`[assemble] ${projectId}: ${msg}`);
    return setProgress(projectId, msg);
  };

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "assemble-"));

  // Ping our own health endpoint every 4 min so Render free tier doesn't
  // spin the service down during a long background assembly
  const selfUrl = process.env.SELF_URL;
  const keepAlive = selfUrl
    ? setInterval(() => { fetch(`${selfUrl}/health`).catch(() => {}); }, 4 * 60_000)
    : null;

  try {
    await progress("Loading project data…");

    const [projectRes, beatsRes] = await Promise.all([
      supabase.from("projects").select("tts_url, tts_cleaned_url").eq("id", projectId).single(),
      supabase.from("project_beats").select("beat_number, script_segment, video_url, image_url").eq("project_id", projectId).order("beat_number"),
    ]);
    if (projectRes.error) throw new Error("Project not found");

    const proj = projectRes.data as { tts_url: string | null; tts_cleaned_url: string | null };
    const allBeats = (beatsRes.data ?? []) as Beat[];
    const voiceoverUrl = voiceoverType === "original" ? (proj.tts_url ?? proj.tts_cleaned_url) : (proj.tts_cleaned_url ?? proj.tts_url);
    if (!voiceoverUrl) throw new Error("No voiceover found — generate a voiceover on the Generate page first.");
    if (!allBeats.length) throw new Error("No beats found in this project.");

    // Use only beats that have a generated video clip — skip gaps entirely
    const beats = allBeats.filter((beat) => beat.video_url).slice(0, 1);
    if (!beats.length) throw new Error("No video clips have been generated yet — generate video clips on the Generate page first.");
    console.log(`[assemble] ${projectId}: assembling ${beats.length}/${allBeats.length} beats (video clips only)`);

    await progress("Downloading voiceover…");
    const voiceoverPath = path.join(tmpDir, "voiceover.mp3");
    await downloadFile(voiceoverUrl, voiceoverPath);
    const totalDuration = await getMediaDuration(voiceoverPath);
    if (totalDuration <= 0) throw new Error("Could not determine voiceover duration");

    await progress("Transcribing voiceover…");
    let transcriptionWords: TranscriptionWord[] = [];
    try {
      const { elevenlabs_api_key } = await getSettings(userId);
      if (!elevenlabs_api_key) throw new Error("ElevenLabs API key not configured.");
      transcriptionWords = await transcribeAudio(voiceoverPath, elevenlabs_api_key);
    } catch (e) {
      console.warn("[assemble] transcription failed, using proportional fallback:", e);
    }

    const durations = alignBeats(beats.map((b) => b.script_segment ?? ""), transcriptionWords, totalDuration);

    await progress("Processing video clips…");
    const clipPaths: string[] = new Array(beats.length).fill("");

    // Process clips sequentially to stay within Render's 512 MB RAM limit
    for (let start = 0; start < beats.length; start += 1) {
      const slice = beats.slice(start, start + 1);
      await progress(`Processing clip ${start + 1} of ${beats.length}…`);
      await Promise.all(slice.map(async (beat, localIdx) => {
        const i = start + localIdx;
        const clipPath = path.join(tmpDir, `clip_${String(i).padStart(3, "0")}.mp4`);
        try {
          if (beat.video_url) {
            const ext = beat.video_url.includes(".webm") ? "webm" : "mp4";
            const src = path.join(tmpDir, `src_${i}.${ext}`);
            console.log(`[assemble] beat ${beat.beat_number}: downloading video…`);
            await downloadFile(beat.video_url, src);
            console.log(`[assemble] beat ${beat.beat_number}: encoding clip…`);
            await normalizeClip(src, false, durations[i], clipPath, w, h);
            try { fs.unlinkSync(src); } catch { /* ignore */ }
          } else if (beat.image_url) {
            const ext = beat.image_url.toLowerCase().includes(".png") ? "png" : "jpg";
            const src = path.join(tmpDir, `src_${i}.${ext}`);
            console.log(`[assemble] beat ${beat.beat_number}: downloading image…`);
            await downloadFile(beat.image_url, src);
            console.log(`[assemble] beat ${beat.beat_number}: encoding clip…`);
            await normalizeClip(src, true, durations[i], clipPath, w, h);
            try { fs.unlinkSync(src); } catch { /* ignore */ }
          }
          console.log(`[assemble] beat ${beat.beat_number}: done`);
          clipPaths[i] = clipPath;
        } catch (e) {
          console.error(`[assemble] beat ${beat.beat_number} skipped:`, e);
          // leave clipPaths[i] as "" — filtered out of concat below
        }
      }));
    }

    await progress("Joining clips…");
    const validClipPaths = clipPaths.filter((p) => p !== "");
    if (!validClipPaths.length) throw new Error("All clips failed to encode — nothing to assemble.");
    const listPath = path.join(tmpDir, "concat.txt");
    fs.writeFileSync(listPath, validClipPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n"));
    const joinedPath = path.join(tmpDir, "joined.mp4");
    await concatClips(listPath, joinedPath);
    // Free disk space — individual clips are no longer needed
    for (const p of validClipPaths) { try { fs.unlinkSync(p); } catch { /* ignore */ } }

    await progress("Mixing voiceover…");
    const outputPath = path.join(tmpDir, "output.mp4");
    await mixAudio(joinedPath, voiceoverPath, outputPath);
    try { fs.unlinkSync(joinedPath); } catch { /* ignore */ }

    let finalPath = outputPath;
    if (captionsEnabled) {
      try {
        await progress("Generating captions…");
        let segs = transcriptionWords.length > 0 ? buildSrtSegments(transcriptionWords) : buildSrtSegmentsFromBeats(beats, durations);
        if (captionsLanguage !== "source") {
          await progress(`Translating captions to ${captionsLanguage}…`);
          const { anthropic_api_key } = await getSettings(userId);
          if (!anthropic_api_key) throw new Error("Anthropic API key not configured.");
          segs = await translateSegments(segs, captionsLanguage, anthropic_api_key);
        }
        const assPath = path.join(tmpDir, "captions.ass");
        writeAss(segs, buildAssStyle(captionsStyle, captionsSize, captionsPosition, h), w, h, assPath);
        await progress("Burning captions…");
        const captionedPath = path.join(tmpDir, "captioned.mp4");
        await burnSubtitles(outputPath, assPath, captionedPath);
        try { fs.unlinkSync(outputPath); } catch { /* ignore */ }
        finalPath = captionedPath;
      } catch (e) {
        console.warn("[assemble] caption burn failed:", e);
      }
    }

    await progress("Uploading…");
    const publicUrl = await uploadFile(`${projectId}/assembled_${Date.now()}.mp4`, finalPath, "video/mp4");

    await supabase.from("projects")
      .update({ assembled_url: publicUrl, assembly_status: "done", assembly_progress: null, assembly_error: null })
      .eq("id", projectId);

    console.log(`[assemble] ${projectId}: done → ${publicUrl}`);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Assembly failed";
    console.error(`[assemble] ${projectId} failed:`, message);
    await supabase.from("projects")
      .update({ assembly_status: "failed", assembly_error: message, assembly_progress: null })
      .eq("id", projectId);
  } finally {
    if (keepAlive) clearInterval(keepAlive);
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
  }
}

// ── Route ─────────────────────────────────────────────────────────────────────

const assemblingProjects = new Set<string>();

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

    if (assemblingProjects.has(projectId)) {
      res.json({ started: false, reason: "Assembly already in progress for this project" });
      return;
    }

    // Mark as processing immediately so the client sees state change on next poll
    await supabase.from("projects")
      .update({ assembly_status: "processing", assembly_progress: "Starting…", assembly_error: null })
      .eq("id", projectId).eq("user_id", user.id);

    assemblingProjects.add(projectId);

    // Fire and forget — no SSE, no long-lived connection
    runAssembly({ userId: user.id, projectId, aspectRatio, voiceoverType: voiceoverType as "cleaned" | "original",
      captionsEnabled: Boolean(captionsEnabled), captionsLanguage, captionsStyle, captionsSize, captionsPosition,
    }).catch(console.error).finally(() => assemblingProjects.delete(projectId));

    res.json({ started: true });
  });
}
