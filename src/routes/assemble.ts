import { createRequire } from "module";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { type Express, type Request, type Response } from "express";
import { supabase } from "../lib/supabase.js";
import { uploadFile, userFolderForId } from "../lib/storage.js";
import { redis } from "../lib/queue.js";
import Anthropic from "@anthropic-ai/sdk";
import { getAnthropicClient } from "../lib/anthropic.js";
import fs from "fs";
import path from "path";
import os from "os";
import { createHash } from "crypto";

const _require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-require-imports
const ffmpeg = _require("fluent-ffmpeg") as typeof import("fluent-ffmpeg");
const ffmpegPath = _require("ffmpeg-static") as string | null;
const ffprobeStatic = _require("ffprobe-static") as { path: string };

if (ffmpegPath) ffmpeg.setFfmpegPath(ffmpegPath);
if (ffprobeStatic?.path) ffmpeg.setFfprobePath(ffprobeStatic.path);

console.log(`[assemble] ffmpeg: ${ffmpegPath}`);
console.log(`[assemble] ffprobe: ${ffprobeStatic?.path}`);

// ── Preview storage ───────────────────────────────────────────────────────────

const PREVIEW_DIR = path.join(os.tmpdir(), "aitrends-previews");
if (!fs.existsSync(PREVIEW_DIR)) fs.mkdirSync(PREVIEW_DIR, { recursive: true });

// ── Progress helper ───────────────────────────────────────────────────────────

async function setProgress(projectId: string, progress: string) {
  await supabase.from("projects")
    .update({ assembly_progress: progress })
    .eq("id", projectId);
}

// ── Settings ──────────────────────────────────────────────────────────────────

async function getSettings(userId: string): Promise<{ elevenlabs_api_key: string }> {
  const { data } = await supabase
    .from("account_settings")
    .select("elevenlabs_api_key")
    .eq("user_id", userId)
    .single();
  return {
    elevenlabs_api_key: data?.elevenlabs_api_key?.trim() || process.env.ELEVENLABS_API_KEY || "",
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

// 30 min cap. Burning subtitles re-encodes the full assembled video, which
// on Render's shared CPU can run 15-20+ min for longer scripts. Other
// ffmpeg steps (normalizeClip, concat, mixAudio) finish in seconds-minutes
// so a higher ceiling here doesn't add real latency on the fast path.
const FFMPEG_TIMEOUT_MS = 30 * 60_000;

// Unique marker so the catch path in runAssembly can distinguish a
// user-requested stop from a real error and persist the checkpoint
// instead of clearing it.
const STOPPED_MARKER = "ASSEMBLY_STOPPED_BY_USER";

function ffmpegWithTimeout(
  build: (cmd: ReturnType<typeof ffmpeg>) => ReturnType<typeof ffmpeg>,
  label: string,
  signal?: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new Error(STOPPED_MARKER));
      return;
    }
    const cmd = build(ffmpeg());
    let settled = false;
    const settle = (fn: () => void) => {
      if (!settled) { settled = true; clearTimeout(timer); signal?.removeEventListener("abort", onAbort); fn(); }
    };
    const onAbort = () => {
      settle(() => {
        try { cmd.kill("SIGKILL"); } catch { /* ignore */ }
        reject(new Error(STOPPED_MARKER));
      });
    };
    const timer = setTimeout(() => {
      settle(() => {
        try { cmd.kill("SIGKILL"); } catch { /* ignore */ }
        reject(new Error(`ffmpeg timed out: ${label}`));
      });
    }, FFMPEG_TIMEOUT_MS);
    signal?.addEventListener("abort", onAbort, { once: true });
    cmd
      .on("end", () => settle(resolve))
      .on("error", (err: Error) => settle(() => reject(new Error(`${label} failed: ${err.message}`))))
      .run();
  });
}

function normalizeClip(src: string, isImage: boolean, duration: number, output: string, w: number, h: number, signal?: AbortSignal): Promise<void> {
  const vf = `scale=${w}:${h}:force_original_aspect_ratio=decrease,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:black,fps=24`;
  return ffmpegWithTimeout((cmd) => {
    if (isImage) cmd.input(src).inputOptions(["-loop", "1"]);
    else cmd.input(src).inputOptions(["-stream_loop", "-1"]);
    return cmd
      .outputOptions(["-t", String(duration), "-vf", vf, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-an", "-pix_fmt", "yuv420p"])
      .output(output);
  }, `normalizeClip`, signal);
}


function concatClips(listFile: string, output: string, signal?: AbortSignal): Promise<void> {
  return ffmpegWithTimeout((cmd) =>
    cmd
      .input(listFile).inputOptions(["-f", "concat", "-safe", "0"])
      .outputOptions(["-c", "copy"])
      .output(output),
    "concat",
    signal,
  );
}

function mixAudio(video: string, audio: string, output: string, videoDuration: number, signal?: AbortSignal): Promise<void> {
  return ffmpegWithTimeout((cmd) =>
    cmd
      .input(video).inputOptions(["-fflags", "+genpts"])
      .input(audio)
      // +faststart skipped: on constrained disk it can corrupt the moov atom; range-request serving handles moov-at-end fine
      .outputOptions(["-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-t", String(videoDuration)])
      .output(output),
    "audio mix",
    signal,
  );
}

function burnSubtitles(video: string, assPath: string, output: string, signal?: AbortSignal): Promise<void> {
  // Escape backslashes and colons for ffmpeg filtergraph syntax (no shell quoting needed)
  const escaped = assPath.replace(/\\/g, "/").replace(/:/g, "\\:");
  return ffmpegWithTimeout((cmd) =>
    cmd
      .input(video)
      // "veryfast" cuts burn time ~2x vs "fast" on Render's shared CPU,
      // at the cost of ~15-20% larger output. Captions are a re-encode
      // pass so we trade size for staying under the timeout ceiling.
      .outputOptions(["-vf", `ass=${escaped}`, "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "copy", "-movflags", "+faststart"])
      .output(output),
  "burnSubtitles", signal);
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
  const getEnd = (w: TranscriptionWord) => w.end ?? w.end_time ?? getStart(w);
  if (!words.length) {
    const counts = beatTexts.map((t) => Math.max(1, t.trim().split(/\s+/).filter(Boolean).length));
    const total = counts.reduce((s, n) => s + n, 0);
    return counts.map((n) => Math.max(0.5, (n / total) * totalDuration));
  }
  // Per-beat start-of-narration anchors.
  //
  // Old behavior (the source of the "visuals ahead of narration" drift):
  // greedy forward scan that picked the FIRST position where 2 of the
  // first 3 beat-words matched the transcription. Common starter words
  // ("And", "The", "But", "Then") match many places, and the first hit
  // is almost always earlier than the actual start — which clips the
  // previous beat short and makes the next beat's visual show before
  // its narration. Drift compounds across the video.
  //
  // New strategy:
  //  1. Predict each beat's expected start index in the transcription
  //     by proportional word-count scaling. If the script has 500
  //     words and the transcription has 520 (TTS expanded a few
  //     numbers / contractions), the scale factor is 520/500 = 1.04;
  //     beat i's expected start is cumulative-script-words[i] * 1.04.
  //  2. Search only within a small ± window around the expected
  //     position (capped also by the previous beat's match so anchors
  //     stay monotonically increasing).
  //  3. Pick the BEST-scoring position in that window, where score is
  //     the number of first-WINDOW_LEN beat-words that match the
  //     transcription at that offset. Ties broken by closeness to the
  //     expected position.
  //  4. Falls through to the expected position if nothing matches —
  //     bounded by the proportional anchor so a no-match beat can't
  //     warp the timeline.
  const norm = words.map((w) => normalizeWord(w.text ?? w.word ?? ""));
  const beatWordCounts = beatTexts.map((t) => Math.max(1, t.trim().split(/\s+/).filter(Boolean).length));
  const totalScriptWords = beatWordCounts.reduce((s, n) => s + n, 0);
  const scale = words.length / totalScriptWords;
  const expectedStarts: number[] = [];
  {
    let cum = 0;
    for (const c of beatWordCounts) {
      expectedStarts.push(Math.min(words.length - 1, Math.max(0, Math.floor(cum * scale))));
      cum += c;
    }
  }
  // Window size scales with the expected per-beat length so a 50-word
  // beat searches ~10 words around its anchor while a 10-word beat
  // only searches ~5. Capped on both sides to avoid degenerate cases.
  // Wider window (7 vs 5) gives the matcher more disambiguation power
  // when common starter words (And, The, But, Then) would otherwise
  // produce false matches in multiple places.
  const WINDOW_LEN = 7;

  // Pre-compute the inter-word gap *before* each transcription word.
  // A long preceding gap is a strong "beat could start here" signal —
  // narrators pause naturally at clause/sentence boundaries, which are
  // also where the script-segmenter usually drew beat boundaries. We
  // add this as a small score bonus during matching so an ambiguous
  // word match (common starter word, low count) doesn't override a
  // clearly pause-anchored position.
  //
  // Tuning: pause windows are calibrated against typical narrator
  // cadence. <150ms = inter-word, 150–250ms = natural breath,
  // 250–400ms = clause break, >400ms = strong sentence/topic break.
  const precedingGapBonus = new Array<number>(words.length).fill(0);
  for (let i = 1; i < words.length; i++) {
    const gap = getStart(words[i]) - getEnd(words[i - 1]);
    if (gap >= 0.40) precedingGapBonus[i] = 1.0;       // strong pause
    else if (gap >= 0.25) precedingGapBonus[i] = 0.5;  // typical clause break
    else if (gap >= 0.15) precedingGapBonus[i] = 0.2;  // light pause / breath
  }

  const startIdxs: number[] = [];
  for (let bi = 0; bi < beatTexts.length; bi++) {
    const beatWords = beatTexts[bi].trim().split(/\s+/).filter(Boolean).map(normalizeWord).filter(Boolean);
    const expected = expectedStarts[bi];
    if (!beatWords.length) {
      startIdxs.push(Math.min(expected, words.length - 1));
      continue;
    }
    const win = beatWords.slice(0, Math.min(WINDOW_LEN, beatWords.length));
    const slack = Math.max(3, Math.min(20, Math.floor(beatWordCounts[bi] * scale * 0.4)));
    const lowerBound = bi > 0 ? startIdxs[bi - 1] + 1 : 0;
    const searchStart = Math.max(lowerBound, expected - slack);
    const searchEnd = Math.min(words.length, expected + slack + 1);
    let bestPos = expected;
    let bestScore = -1;
    let bestRawMatches = -1;
    let bestDistance = Infinity;
    for (let j = searchStart; j < searchEnd; j++) {
      let m = 0;
      for (let k = 0; k < win.length && j + k < words.length; k++) if (norm[j + k] === win[k]) m++;
      // Allow positions with 0 word matches to still be considered —
      // a strong pause anchor can hint at the right boundary even
      // when TTS rendered words don't perfectly match the script
      // (e.g. "$100" → "one hundred dollars").
      const score = m + (bi > 0 ? precedingGapBonus[j] : 0);
      if (m === 0 && score === 0) continue;
      const dist = Math.abs(j - expected);
      if (
        score > bestScore ||
        (score === bestScore && m > bestRawMatches) ||
        (score === bestScore && m === bestRawMatches && dist < bestDistance)
      ) {
        bestScore = score;
        bestRawMatches = m;
        bestDistance = dist;
        bestPos = j;
      }
    }
    // Require either 2+ raw word matches OR a position carrying a
    // strong-pause anchor (>=0.5) to trust the match — otherwise a
    // single common-word hit on the wrong position is worse than the
    // proportional prediction. Clamp to monotonicity bound.
    const trustMatch = bestRawMatches >= 2 || (bestRawMatches >= 1 && bestScore - bestRawMatches >= 0.5);
    if (!trustMatch) bestPos = Math.max(lowerBound, expected);
    startIdxs.push(Math.min(bestPos, words.length - 1));
  }
  // The true end of speech is the last transcribed word's end time, not the
  // full audio file length. If the voiceover has trailing silence, using
  // totalDuration here makes the final beat absurdly long and normalizeClip
  // loops its source video to fill the gap (the "last 3 minutes repeats one
  // clip" bug). Cap at lastWordEnd + small natural-decay buffer.
  const lastWordEnd = getEnd(words[words.length - 1]);
  // 2s pad gives natural speech-tail decay AND covers transcription-missed
  // trailing words (ElevenLabs occasionally drops the very last 1-2 words
  // when they end on a soft consonant). Still drops genuine multi-minute
  // silence-only tails which is the original bug we're guarding against.
  const SPEECH_TAIL_PAD_SEC = 2;
  const speechEnd = Math.min(totalDuration, lastWordEnd + SPEECH_TAIL_PAD_SEC);

  // Per-beat durations come straight from the word-timestamp gaps:
  //   beat i runs from word[startIdxs[i]].start to word[startIdxs[i+1]].start
  //   (last beat ends at speechEnd, capped by the lastWordEnd + 2s guard
  //   above).
  // No more proportional cap — the cap (originally 1.5× word-share) was
  // clipping beats that genuinely take longer than their word-share due
  // to natural pauses or slow delivery, which made every subsequent
  // beat's visual show early and drift cascaded through the video. With
  // the speechEnd guard bounding the last beat AND the post-concat
  // freeze-pad in runAssembly filling any trailing-silence gap to the
  // voiceover's totalDuration, the cap no longer protects against
  // anything load-bearing. Removing it keeps every beat exactly on its
  // narration boundaries.
  const durations: number[] = [];
  for (let i = 0; i < beatTexts.length; i++) {
    const si = startIdxs[i];
    const ni = i < beatTexts.length - 1 ? startIdxs[i + 1] : words.length;
    const start = getStart(words[Math.min(si, words.length - 1)]);
    const end = ni < words.length ? getStart(words[ni]) : speechEnd;
    durations.push(Math.max(0.5, end - start));
  }
  return durations;
}

// ── Caption / ASS ─────────────────────────────────────────────────────────────

interface SrtSegment { index: number; start: number; end: number; text: string; }
interface AssStyle { fontSize: number; alignment: number; marginV: number; primaryColour: string; outlineColour: string; backColour: string; bold: number; borderStyle: number; outline: number; shadow: number; }

type Beat = { beat_number: number; script_segment: string | null; video_url: string | null; image_url: string | null; duration_ms?: number | null; };

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

async function translateSegments(segs: SrtSegment[], lang: string, anthropic: Anthropic): Promise<SrtSegment[]> {
  if (!segs.length) return segs;
  const numbered = segs.map((s) => `${s.index}. ${s.text}`).join("\n");
  const msg = await anthropic.messages.create({
    model: process.env.CLAUDE_MODEL ?? "claude-sonnet-4-6", max_tokens: 4096,
    messages: [{ role: "user", content: `Translate these numbered caption lines to ${lang}. Return only the translated lines in exactly the same "N. text" format, one per line:\n\n${numbered}` }],
  });
  const raw = msg.content[0].type === "text" ? msg.content[0].text.trim() : "";
  const lines = raw.split("\n").filter(Boolean);
  return segs.map((s) => {
    const line = lines.find((l) => l.startsWith(`${s.index}.`));
    return line ? { ...s, text: line.replace(/^\d+\.\s*/, "").trim() } : s;
  });
}

// ── Checkpoint state ──────────────────────────────────────────────────────────
//
// Persisted on projects.assembly_checkpoint after each completed stage
// so a user-requested Stop can be resumed without redoing the work.
// Two hashes are tracked so we can invalidate just the suffix of stages
// that depend on the changed options:
//
//   core_hash   = aspectRatio + voiceoverType. If this changes between
//                 stop and resume, *everything* downstream of the
//                 voiceover/scale decision must be re-done (the saved
//                 clip / joined / mixed mp4s are wrong sizes / wrong
//                 audio basis).
//   captions_hash = captionsEnabled + the four caption-style options
//                 (language, style, size, position). If only this
//                 changes, we can keep the saved mixed.mp4 and just
//                 re-burn captions.
//
// Stage outputs are uploaded to R2 under a `_assembly/` prefix and
// deleted by the cleanup pass on successful completion. clip_urls is
// sparse — entry i is set only when beat i's normalized clip has been
// uploaded; null/undefined means "not done yet, normalize on this run".
interface AssemblyCheckpoint {
  core_hash: string;
  captions_hash: string;
  transcription_words?: TranscriptionWord[];
  clip_urls?: (string | null)[];
  joined_url?: string;
  padded_url?: string;
  mixed_url?: string;
  captioned_url?: string;
}

function hashString(s: string): string {
  // FNV-1a 32-bit — plenty for change detection, no crypto dep needed.
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16);
}

function coreHash(opts: AssembleOptions): string {
  return hashString(`${opts.aspectRatio}|${opts.voiceoverType}`);
}

function captionsHash(opts: AssembleOptions): string {
  return hashString(`${opts.captionsEnabled}|${opts.captionsLanguage}|${opts.captionsStyle}|${opts.captionsSize}|${opts.captionsPosition}`);
}

async function loadCheckpoint(projectId: string): Promise<AssemblyCheckpoint | null> {
  const { data } = await supabase.from("projects").select("assembly_checkpoint").eq("id", projectId).single();
  return (data?.assembly_checkpoint as AssemblyCheckpoint | null) ?? null;
}

async function saveCheckpoint(projectId: string, ckpt: AssemblyCheckpoint): Promise<void> {
  await supabase.from("projects").update({ assembly_checkpoint: ckpt }).eq("id", projectId);
}

async function clearCheckpoint(projectId: string): Promise<void> {
  await supabase.from("projects").update({ assembly_checkpoint: null }).eq("id", projectId);
}

async function isStopRequested(projectId: string): Promise<boolean> {
  const { data } = await supabase.from("projects").select("assembly_stop_requested").eq("id", projectId).single();
  return !!(data?.assembly_stop_requested as boolean | undefined);
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

  // Stop signal: a background poll watches projects.assembly_stop_requested
  // every 3s. When set, we abort any running ffmpeg via the shared
  // AbortController; the catch path below sees STOPPED_MARKER and
  // transitions the project to assembly_status="stopped" with the
  // checkpoint intact for Resume. Explicit checkStop() calls between
  // stages mean we don't have to wait up to 3s for the poll to notice
  // when an awaited stage finishes.
  const aborter = new AbortController();
  const signal = aborter.signal;
  const stopPoll = setInterval(async () => {
    if (signal.aborted) return;
    try {
      if (await isStopRequested(projectId)) {
        console.log(`[assemble] ${projectId}: stop requested — aborting`);
        aborter.abort(new Error(STOPPED_MARKER));
      }
    } catch {
      // poll error — swallow, will retry next tick
    }
  }, 3000);
  const checkStop = async (): Promise<void> => {
    if (signal.aborted) throw new Error(STOPPED_MARKER);
    // Also check directly so a Stop click between stages doesn't wait
    // up to a poll tick.
    if (await isStopRequested(projectId)) {
      aborter.abort(new Error(STOPPED_MARKER));
      throw new Error(STOPPED_MARKER);
    }
  };

  // Checkpoint: skip stages whose output is already in R2 from a prior
  // run of this same project. Validated against the current options
  // hashes so a config change since Stop invalidates the suffix of
  // stages that depend on the changed pieces.
  const currentCoreHash = coreHash(opts);
  const currentCaptionsHash = captionsHash(opts);
  let checkpoint: AssemblyCheckpoint;
  {
    const loaded = await loadCheckpoint(projectId);
    if (loaded && loaded.core_hash === currentCoreHash) {
      if (loaded.captions_hash === currentCaptionsHash) {
        checkpoint = loaded;
      } else {
        console.log(`[assemble] ${projectId}: captions opts changed — discarding captioned_url`);
        checkpoint = { ...loaded, captions_hash: currentCaptionsHash, captioned_url: undefined };
      }
    } else {
      if (loaded) console.log(`[assemble] ${projectId}: core opts changed — discarding checkpoint`);
      checkpoint = { core_hash: currentCoreHash, captions_hash: currentCaptionsHash };
    }
  }
  const persistCheckpoint = async (): Promise<void> => { await saveCheckpoint(projectId, checkpoint); };
  const userFolder = await userFolderForId(userId);
  const ckptPathFor = (name: string): string => `${userFolder}/${projectId}/_assembly/${name}`;

  // Ping our own health endpoint every 4 min so Render free tier doesn't
  // spin the service down during a long background assembly
  const isDev = process.env.NODE_ENV === "development";
  const selfUrl = isDev
    ? (process.env.SELF_URL_LOCAL ?? null)
    : (process.env.SELF_URL_PRODUCTION ?? null);
  const keepAlive = selfUrl
    ? setInterval(() => { fetch(`${selfUrl}/health`).catch(() => {}); }, 4 * 60_000)
    : null;

  try {
    await progress("Loading project data…");

    const [projectRes, beatsRes] = await Promise.all([
      supabase.from("projects").select("tts_url, tts_cleaned_url, beat_timings_voiceover_hash").eq("id", projectId).single(),
      supabase.from("project_beats").select("beat_number, script_segment, video_url, image_url, duration_ms").eq("project_id", projectId).order("beat_number"),
    ]);
    if (projectRes.error) {
      // PostgREST returns specific codes/messages — surfacing them
      // makes "Project not found" actionable instead of mysterious.
      // The most common culprit is a missing column from an
      // unapplied migration (e.g., 044 added beat_timings_voiceover_hash).
      const msg = projectRes.error.message || "unknown";
      const code = projectRes.error.code ? ` [${projectRes.error.code}]` : "";
      throw new Error(`Project lookup failed${code}: ${msg}`);
    }

    const proj = projectRes.data as { tts_url: string | null; tts_cleaned_url: string | null; beat_timings_voiceover_hash: string | null };
    const allBeats = (beatsRes.data ?? []) as Beat[];
    const voiceoverUrl = voiceoverType === "original" ? (proj.tts_url ?? proj.tts_cleaned_url) : (proj.tts_cleaned_url ?? proj.tts_url);
    if (!voiceoverUrl) throw new Error("No voiceover found — generate a voiceover on the Generate page first.");
    if (!allBeats.length) throw new Error("No beats found in this project.");

    // Include all beats that have either a video clip or an image — skip empty beats
    const beats = allBeats.filter((beat) => beat.video_url || beat.image_url);
    if (!beats.length) throw new Error("No images or video clips found — generate images on the Generate page first.");
    const videoCount = beats.filter((b) => b.video_url).length;
    console.log(`[assemble] ${projectId}: assembling ${beats.length}/${allBeats.length} beats (${videoCount} video, ${beats.length - videoCount} image)`);

    await checkStop();
    await progress("Downloading voiceover…");
    const voiceoverPath = path.join(tmpDir, "voiceover.mp3");
    await downloadFile(voiceoverUrl, voiceoverPath);
    const totalDuration = await getMediaDuration(voiceoverPath);
    if (totalDuration <= 0) throw new Error("Could not determine voiceover duration");
    console.log(`[assemble] ${projectId}: voiceover duration = ${totalDuration.toFixed(2)}s`);

    // ── Stage A: transcription + beat alignment ────────────────────────
    //
    // Three things may need to happen here, each with its own cache:
    //   1. STT (ElevenLabs) — required for caption text and for
    //      alignBeats. Cached in the assembly checkpoint so a Resume
    //      doesn't pay for it twice within the same run.
    //   2. alignBeats — measures per-beat narration durations.
    //      Persisted to project_beats.duration_ms so reassemblies of
    //      the same project (different aspect ratio, captions tweak,
    //      voiceover trim) skip STT *and* the matcher entirely.
    //   3. Caption transcription — when captions are enabled the
    //      assembler still needs word-level timestamps even if
    //      durations are cached. We re-STT in that case (could be
    //      cached separately on the project row later).
    //
    // Cache invalidation: voiceover URL is hashed; if it differs from
    // projects.beat_timings_voiceover_hash, stored durations are stale
    // and a fresh STT+match pass runs. The URL changes on every TTS
    // regeneration (filename includes a Date.now() timestamp) so the
    // signal is reliable.
    const voiceoverHash = createHash("sha256").update(voiceoverUrl).digest("hex");
    const allBeatsHaveDuration = beats.every((b) => typeof b.duration_ms === "number" && (b.duration_ms ?? 0) > 0);
    const hashMatches = proj.beat_timings_voiceover_hash === voiceoverHash;
    const canUseStoredDurations = allBeatsHaveDuration && hashMatches;
    const needSttForCaptions = captionsEnabled;
    const needSttForAlignment = !canUseStoredDurations;

    let transcriptionWords: TranscriptionWord[] = checkpoint.transcription_words ?? [];
    if (transcriptionWords.length) {
      console.log(`[assemble] ${projectId}: transcription loaded from checkpoint (${transcriptionWords.length} words)`);
    } else if (needSttForAlignment || needSttForCaptions) {
      await checkStop();
      await progress("Transcribing voiceover…");
      try {
        const { elevenlabs_api_key } = await getSettings(userId);
        if (!elevenlabs_api_key) throw new Error("ElevenLabs API key not configured.");
        transcriptionWords = await transcribeAudio(voiceoverPath, elevenlabs_api_key);
      } catch (e) {
        console.warn("[assemble] transcription failed, using proportional fallback:", e);
      }
      if (transcriptionWords.length) {
        checkpoint.transcription_words = transcriptionWords;
        await persistCheckpoint();
      }
    } else {
      console.log(`[assemble] ${projectId}: skipping STT — beats already aligned and captions disabled`);
    }
    if (transcriptionWords.length) {
      const lastWord = transcriptionWords[transcriptionWords.length - 1];
      const lastEnd = lastWord.end ?? lastWord.end_time ?? lastWord.start ?? lastWord.start_time ?? 0;
      console.log(`[assemble] ${projectId}: transcribed ${transcriptionWords.length} words, lastWordEnd = ${lastEnd.toFixed(2)}s (audio is ${totalDuration.toFixed(2)}s; trailing silence ≈ ${(totalDuration - lastEnd).toFixed(2)}s)`);
    }

    // Per-beat durations: prefer stored (skips alignBeats entirely),
    // fall back to running the matcher and persisting the result for
    // next time.
    let durations: number[];
    if (canUseStoredDurations) {
      durations = beats.map((b) => Math.max(0.5, (b.duration_ms ?? 0) / 1000));
      console.log(`[assemble] ${projectId}: using stored beat durations (sum=${durations.reduce((s, d) => s + d, 0).toFixed(2)}s, voiceover=${totalDuration.toFixed(2)}s)`);
    } else {
      durations = alignBeats(beats.map((b) => b.script_segment ?? ""), transcriptionWords, totalDuration);
      // Persist newly-measured timings so the next assembly skips this
      // pass. Best-effort: a DB hiccup here doesn't fail the current
      // assembly, just means the next one re-measures.
      try {
        const updates = beats.map((b, i) => ({
          beat_number: b.beat_number,
          duration_ms: Math.round(durations[i] * 1000),
        }));
        await Promise.all(updates.map((u) =>
          supabase
            .from("project_beats")
            .update({ duration_ms: u.duration_ms })
            .eq("project_id", projectId)
            .eq("beat_number", u.beat_number)
        ));
        await supabase
          .from("projects")
          .update({ beat_timings_voiceover_hash: voiceoverHash })
          .eq("id", projectId);
        console.log(`[assemble] ${projectId}: persisted ${updates.length} beat timings (voiceoverHash=${voiceoverHash.slice(0, 12)}…)`);
      } catch (e) {
        console.warn(`[assemble] ${projectId}: failed to persist beat timings — next assembly will re-measure:`, e);
      }
    }

    // ── Stage B: per-clip normalization → concat → joined.mp4 ───────────
    //
    // Each clip is uploaded to R2 after encoding (checkpoint.clip_urls[i])
    // so a Stop mid-loop preserves completed clips. On Resume, we
    // re-download cached clips instead of re-encoding them.
    if (!checkpoint.joined_url) {
      checkpoint.clip_urls = checkpoint.clip_urls ?? new Array(beats.length).fill(null);
      const clipPaths: string[] = new Array(beats.length).fill("");
      await progress("Processing video clips…");
      for (let i = 0; i < beats.length; i++) {
        await checkStop();
        const clipPath = path.join(tmpDir, `clip_${String(i).padStart(3, "0")}.mp4`);
        const cached = checkpoint.clip_urls[i];
        if (cached) {
          await progress(`Restoring clip ${i + 1} of ${beats.length}…`);
          try {
            await downloadFile(cached, clipPath);
            clipPaths[i] = clipPath;
            continue;
          } catch (e) {
            console.warn(`[assemble] beat ${beats[i].beat_number}: cached clip download failed, re-encoding:`, e);
            // fall through to fresh encode
          }
        }
        const beat = beats[i];
        await progress(`Processing clip ${i + 1} of ${beats.length}…`);
        try {
          if (beat.video_url) {
            const ext = beat.video_url.includes(".webm") ? "webm" : "mp4";
            const src = path.join(tmpDir, `src_${i}.${ext}`);
            console.log(`[assemble] beat ${beat.beat_number}: downloading video…`);
            await downloadFile(beat.video_url, src);
            console.log(`[assemble] beat ${beat.beat_number}: encoding clip…`);
            await normalizeClip(src, false, durations[i], clipPath, w, h, signal);
            try { fs.unlinkSync(src); } catch { /* ignore */ }
          } else if (beat.image_url) {
            const ext = beat.image_url.toLowerCase().includes(".png") ? "png" : "jpg";
            const src = path.join(tmpDir, `src_${i}.${ext}`);
            console.log(`[assemble] beat ${beat.beat_number}: downloading image…`);
            await downloadFile(beat.image_url, src);
            console.log(`[assemble] beat ${beat.beat_number}: encoding clip…`);
            await normalizeClip(src, true, durations[i], clipPath, w, h, signal);
            try { fs.unlinkSync(src); } catch { /* ignore */ }
          }
          // Upload the normalized clip so a future Stop can resume past it.
          try {
            const clipUrl = await uploadFile(ckptPathFor(`clip_${String(i).padStart(3, "0")}.mp4`), clipPath, "video/mp4");
            checkpoint.clip_urls[i] = clipUrl;
            await persistCheckpoint();
          } catch (uploadErr) {
            console.warn(`[assemble] beat ${beat.beat_number}: clip checkpoint upload failed:`, uploadErr);
            // Not fatal — we just lose the resume guarantee for this clip.
          }
          console.log(`[assemble] beat ${beat.beat_number}: done`);
          clipPaths[i] = clipPath;
        } catch (e) {
          if (e instanceof Error && e.message === STOPPED_MARKER) throw e;
          console.error(`[assemble] beat ${beats[i].beat_number} skipped:`, e);
          // leave clipPaths[i] as "" — filtered out of concat below
        }
      }

      await checkStop();
      await progress("Joining clips…");
      const validClipPaths = clipPaths.filter((p) => p !== "");
      if (!validClipPaths.length) throw new Error("All clips failed to encode — nothing to assemble.");
      const listPath = path.join(tmpDir, "concat.txt");
      fs.writeFileSync(listPath, validClipPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n"));
      const joinedLocal = path.join(tmpDir, "joined.mp4");
      await concatClips(listPath, joinedLocal, signal);
      for (const p of validClipPaths) { try { fs.unlinkSync(p); } catch { /* ignore */ } }
      try {
        const joinedUrl = await uploadFile(ckptPathFor("joined.mp4"), joinedLocal, "video/mp4");
        checkpoint.joined_url = joinedUrl;
        await persistCheckpoint();
      } catch (e) {
        console.warn(`[assemble] joined.mp4 checkpoint upload failed:`, e);
      }
    }

    // ── Stage C: freeze-pad → padded.mp4 (only when needed) ─────────────
    //
    // Per-beat durations from alignBeats are word-timestamp-anchored, so
    // every beat's visual lands on its narration. With the original
    // (untrimmed) voiceover, the sum of those beats stops at lastWordEnd
    // + 2s while the audio file keeps going through trailing silence —
    // joined.mp4 ends up shorter than totalDuration. We can't scale the
    // durations uniformly (drifts every beat) and we can't extend the
    // last beat's clip (re-introduces the "last clip loops for 3 min"
    // bug). Holding the last frame for the gap preserves both.
    //
    // Skipped when joined ≈ totalDuration (cleaned voiceover, fast path)
    // or when mixed.mp4 is already cached (skip-ahead on Resume).
    const PAD_EPSILON_SEC = 0.2;
    if (!checkpoint.mixed_url) {
      const joinedDisk = path.join(tmpDir, "joined.mp4");
      if (!fs.existsSync(joinedDisk)) {
        await checkStop();
        await progress("Restoring joined video…");
        await downloadFile(checkpoint.joined_url!, joinedDisk);
      }
      const joinedDuration = await getMediaDuration(joinedDisk).catch(() => 0);
      console.log(`[assemble] ${projectId}: joined duration = ${joinedDuration.toFixed(2)}s`);
      if (joinedDuration <= 0) throw new Error("Joined video has 0 duration — clip encoding produced invalid output");
      const tailDuration = Math.max(0, totalDuration - joinedDuration);
      if (tailDuration > PAD_EPSILON_SEC && !checkpoint.padded_url) {
        await checkStop();
        await progress(`Padding video to voiceover length (+${tailDuration.toFixed(1)}s freeze)…`);
        const paddedPath = path.join(tmpDir, "padded.mp4");
        console.log(`[assemble] ${projectId}: freezing last frame for ${tailDuration.toFixed(2)}s (joined=${joinedDuration.toFixed(2)}s → target=${totalDuration.toFixed(2)}s)`);
        await ffmpegWithTimeout((cmd) =>
          cmd
            .input(joinedDisk)
            .outputOptions([
              "-vf", `tpad=stop_mode=clone:stop_duration=${tailDuration}`,
              "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
              "-an", "-pix_fmt", "yuv420p",
            ])
            .output(paddedPath),
          "padToVoiceover",
          signal,
        );
        try { fs.unlinkSync(joinedDisk); } catch { /* ignore */ }
        try {
          const paddedUrl = await uploadFile(ckptPathFor("padded.mp4"), paddedPath, "video/mp4");
          checkpoint.padded_url = paddedUrl;
          await persistCheckpoint();
        } catch (e) {
          console.warn(`[assemble] padded.mp4 checkpoint upload failed:`, e);
        }
      }
    }

    // ── Stage D: mix audio → output.mp4 ─────────────────────────────────
    const outputPath = path.join(tmpDir, "output.mp4");
    if (checkpoint.mixed_url) {
      await checkStop();
      await progress("Restoring mixed video…");
      await downloadFile(checkpoint.mixed_url, outputPath);
    } else {
      // Decide which source to mix from: padded.mp4 if we made one,
      // else joined.mp4.
      let mixSrc: string;
      if (checkpoint.padded_url) {
        mixSrc = path.join(tmpDir, "padded.mp4");
        if (!fs.existsSync(mixSrc)) {
          await checkStop();
          await progress("Restoring padded video…");
          await downloadFile(checkpoint.padded_url, mixSrc);
        }
      } else {
        mixSrc = path.join(tmpDir, "joined.mp4");
        if (!fs.existsSync(mixSrc)) {
          await checkStop();
          await progress("Restoring joined video…");
          await downloadFile(checkpoint.joined_url!, mixSrc);
        }
      }
      await checkStop();
      await progress("Mixing voiceover…");
      // Cap at totalDuration (voiceover length). The pad step above
      // ensures the video is at least totalDuration when there's
      // significant trailing silence; the cap also trims any tiny
      // encoding-rounding overshoot.
      await mixAudio(mixSrc, voiceoverPath, outputPath, totalDuration, signal);
      try { fs.unlinkSync(mixSrc); } catch { /* ignore */ }
      try {
        const mixedUrl = await uploadFile(ckptPathFor("mixed.mp4"), outputPath, "video/mp4");
        checkpoint.mixed_url = mixedUrl;
        await persistCheckpoint();
      } catch (e) {
        console.warn(`[assemble] mixed.mp4 checkpoint upload failed:`, e);
      }
    }

    // ── Stage E: burn captions → captioned.mp4 (optional) ────────────────
    let finalPath = outputPath;
    if (captionsEnabled) {
      if (checkpoint.captioned_url) {
        await checkStop();
        await progress("Restoring captioned video…");
        const cached = path.join(tmpDir, "captioned.mp4");
        await downloadFile(checkpoint.captioned_url, cached);
        try { fs.unlinkSync(outputPath); } catch { /* ignore */ }
        finalPath = cached;
      } else {
        await checkStop();
        await progress("Generating captions…");
        let segs = transcriptionWords.length > 0 ? buildSrtSegments(transcriptionWords) : buildSrtSegmentsFromBeats(beats, durations);
        console.log(`[assemble] ${projectId}: ${segs.length} caption segments`);
        if (!segs.length) throw new Error("No caption segments could be generated — check that beats have script text");
        if (captionsLanguage !== "source") {
          await progress(`Translating captions to ${captionsLanguage}…`);
          const anthropic = await getAnthropicClient(userId);
          segs = await translateSegments(segs, captionsLanguage, anthropic);
        }
        const assPath = path.join(tmpDir, "captions.ass");
        writeAss(segs, buildAssStyle(captionsStyle, captionsSize, captionsPosition, h), w, h, assPath);
        console.log(`[assemble] ${projectId}: ASS file written → ${assPath}`);
        await checkStop();
        await progress("Burning captions…");
        const captionedPath = path.join(tmpDir, "captioned.mp4");
        await burnSubtitles(outputPath, assPath, captionedPath, signal);
        try { fs.unlinkSync(outputPath); } catch { /* ignore */ }
        finalPath = captionedPath;
        console.log(`[assemble] ${projectId}: captions burned`);
        try {
          const captionedUrl = await uploadFile(ckptPathFor("captioned.mp4"), captionedPath, "video/mp4");
          checkpoint.captioned_url = captionedUrl;
          await persistCheckpoint();
        } catch (e) {
          console.warn(`[assemble] captioned.mp4 checkpoint upload failed:`, e);
        }
      }
    }

    // ── Stage F: validate + remux + upload final ────────────────────────
    const finalStat = fs.statSync(finalPath);
    console.log(`[assemble] ${projectId}: final file size = ${finalStat.size} bytes`);
    if (finalStat.size < 1024) throw new Error(`Assembly produced an invalid output (${finalStat.size} bytes) — ffmpeg may have failed silently`);

    const probedDuration = await getMediaDuration(finalPath).catch(() => 0);
    console.log(`[assemble] ${projectId}: probed duration = ${probedDuration.toFixed(2)}s`);
    if (probedDuration <= 0) throw new Error("Output video has 0 duration — moov atom may be corrupt; please reassemble");

    const persistentPath = path.join(PREVIEW_DIR, `${projectId}.mp4`);
    await checkStop();
    // Final remux: ffprobe reads duration from packets but browsers
    // rely on the moov atom header. -c copy + +faststart computes a
    // browser-playable file.
    await ffmpegWithTimeout((cmd) =>
      cmd
        .input(finalPath)
        .outputOptions(["-c", "copy", "-movflags", "+faststart"])
        .output(persistentPath),
      "remux",
      signal,
    );
    const remuxedDuration = await getMediaDuration(persistentPath).catch(() => 0);
    console.log(`[assemble] ${projectId}: remuxed duration = ${remuxedDuration.toFixed(2)}s`);
    if (remuxedDuration <= 0) throw new Error("Remuxed video has 0 duration — please reassemble");

    await progress("Uploading to cloud…");
    previewFiles.set(projectId, persistentPath);
    let publicUrl: string;
    try {
      publicUrl = await uploadFile(`${userFolder}/${projectId}/assembled_${Date.now()}.mp4`, persistentPath, "video/mp4");
    } catch (uploadErr) {
      // The assembled video is fine — only the upload step failed. Preserve
      // the file and flip the project to `preview` so the user can retry
      // *just* the upload (POST /api/upload/:projectId) instead of redoing
      // the entire ffmpeg pipeline.
      const message = uploadErr instanceof Error ? uploadErr.message : "Upload failed";
      console.error(`[assemble] ${projectId} upload failed (preview preserved for retry):`, message);
      await supabase.from("projects")
        .update({ assembly_status: "preview", assembly_error: `Upload failed: ${message}`, assembly_progress: null })
        .eq("id", projectId);
      return;
    }
    previewFiles.delete(projectId);
    try { fs.unlinkSync(persistentPath); } catch { /* ignore */ }

    // Successful completion — clear the checkpoint. (R2 _assembly/ stage
    // objects are left behind; they're small and overwritten by the
    // next run, or cleaned up by the project-delete folder sweep.)
    await supabase.from("projects")
      .update({
        assembly_status: "done",
        assembled_url: publicUrl,
        assembly_progress: null,
        assembly_error: null,
        assembly_checkpoint: null,
        assembly_stop_requested: false,
        current_state: 15,
      })
      .eq("id", projectId);

    console.log(`[assemble] ${projectId}: done → ${publicUrl}`);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Assembly failed";
    if (message === STOPPED_MARKER) {
      // User-requested stop — keep the checkpoint so Resume picks up
      // from the last completed stage. Clear stop_requested so the next
      // Resume → claim cycle doesn't trip the abort the moment the new
      // run starts.
      console.log(`[assemble] ${projectId}: stopped — checkpoint preserved`);
      await supabase.from("projects")
        .update({
          assembly_status: "stopped",
          assembly_progress: "Stopped — click Resume to continue",
          assembly_error: null,
          assembly_stop_requested: false,
        })
        .eq("id", projectId);
    } else {
      console.error(`[assemble] ${projectId} failed:`, message);
      previewFiles.delete(projectId);
      try { fs.unlinkSync(path.join(PREVIEW_DIR, `${projectId}.mp4`)); } catch { /* ignore */ }
      await supabase.from("projects")
        .update({ assembly_status: "failed", assembly_error: message, assembly_progress: null })
        .eq("id", projectId);
      // Drop checkpoint on real failures so the next attempt starts clean.
      await clearCheckpoint(projectId).catch(() => {});
    }
  } finally {
    clearInterval(stopPoll);
    if (keepAlive) clearInterval(keepAlive);
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
  }
}

// ── Assembly queue poll loop ──────────────────────────────────────────────────

async function assemblyPollLoop() {
  console.log("[assembly-queue] poll loop started");
  while (true) {
    try {
      if (assemblingProjects.size === 0) {
        const { data: rows } = await supabase
          .from("projects")
          .select("id, user_id")
          .eq("assembly_status", "queued")
          .limit(1);

        for (const row of rows ?? []) {
          const projectId = row.id as string;
          const userId = row.user_id as string;

          // Atomic claim
          const { data: claimed } = await supabase
            .from("projects")
            .update({ assembly_status: "processing", assembly_progress: "Starting…" })
            .eq("id", projectId)
            .eq("assembly_status", "queued")
            .select("id")
            .single();

          if (!claimed) continue;

          const opts = (await redis.get(`assembly:${projectId}`) as Record<string, unknown> | null) ?? {};

          assemblingProjects.add(projectId);
          runAssembly({
            projectId,
            userId,
            aspectRatio: (opts.aspectRatio as string | undefined) ?? "16:9",
            voiceoverType: ((opts.voiceoverType as string | undefined) ?? "cleaned") as "cleaned" | "original",
            captionsEnabled: (opts.captionsEnabled as boolean | undefined) ?? false,
            captionsLanguage: (opts.captionsLanguage as string | undefined) ?? "source",
            captionsStyle: (opts.captionsStyle as string | undefined) ?? "default",
            captionsSize: (opts.captionsSize as string | undefined) ?? "medium",
            captionsPosition: (opts.captionsPosition as string | undefined) ?? "bottom",
          }).finally(() => {
            assemblingProjects.delete(projectId);
            redis.del(`assembly:${projectId}`).catch(() => {});
          });
        }
      }
    } catch (err) {
      console.error("[assembly-queue] poll error:", err);
    }
    await new Promise((r) => setTimeout(r, 5000));
  }
}

// ── Route ─────────────────────────────────────────────────────────────────────

const assemblingProjects = new Set<string>();
const previewFiles = new Map<string, string>(); // projectId → persistent preview path

export function setupAssembleRoute(app: Express): void {
  assemblyPollLoop().catch(console.error);

  // On startup: restore preview files that survived a worker restart
  (async () => {
    try {
      const { data: previews } = await supabase.from("projects").select("id").eq("assembly_status", "preview");
      for (const p of previews ?? []) {
        const filePath = path.join(PREVIEW_DIR, `${p.id}.mp4`);
        if (fs.existsSync(filePath)) {
          previewFiles.set(p.id, filePath);
          console.log(`[preview] restored: ${p.id}`);
        } else {
          await supabase.from("projects")
            .update({ assembly_status: "failed", assembly_error: "Preview expired — please reassemble", assembly_progress: null, assembled_url: null })
            .eq("id", p.id);
          console.log(`[preview] expired (file missing): ${p.id}`);
        }
      }
    } catch (e) {
      console.warn("[preview] restore failed:", e);
    }
  })();

  app.get("/api/preview/:projectId", (req: Request, res: Response): void => {
    const filePath = previewFiles.get(req.params.projectId);
    if (!filePath || !fs.existsSync(filePath)) {
      res.status(404).json({ error: "Preview not available" });
      return;
    }
    const stat = fs.statSync(filePath);
    const fileSize = stat.size;
    const range = req.headers.range;

    res.set("Content-Type", "video/mp4");
    res.set("Accept-Ranges", "bytes");

    if (range) {
      const [startStr, endStr] = range.replace(/bytes=/, "").split("-");
      const start = parseInt(startStr, 10);
      const end = endStr ? parseInt(endStr, 10) : fileSize - 1;
      const chunkSize = end - start + 1;
      res.set("Content-Range", `bytes ${start}-${end}/${fileSize}`);
      res.set("Content-Length", String(chunkSize));
      res.status(206);
      fs.createReadStream(filePath, { start, end }).pipe(res);
    } else {
      res.set("Content-Length", String(fileSize));
      res.status(200);
      fs.createReadStream(filePath).pipe(res);
    }
  });

  app.post("/api/upload/:projectId", async (req: Request, res: Response): Promise<void> => {
    const { projectId } = req.params;
    const { token } = req.body as { token: string };

    if (!token) { res.status(401).json({ error: "Unauthorized" }); return; }

    const filePath = previewFiles.get(projectId);
    if (!filePath || !fs.existsSync(filePath)) {
      res.status(404).json({ error: "No preview available to upload" });
      return;
    }

    const { data: { user }, error: authError } = await supabase.auth.getUser(token);
    if (authError || !user) { res.status(401).json({ error: "Unauthorized" }); return; }

    await supabase.from("projects")
      .update({ assembly_status: "uploading", assembly_progress: "Uploading…", assembly_error: null })
      .eq("id", projectId);

    res.json({ started: true });

    (async () => {
      try {
        const userFolder = await userFolderForId(user.id);
        const publicUrl = await uploadFile(`${userFolder}/${projectId}/assembled_${Date.now()}.mp4`, filePath, "video/mp4");
        previewFiles.delete(projectId);
        try { fs.unlinkSync(filePath); } catch { /* ignore */ }
        await supabase.from("projects")
          .update({ assembled_url: publicUrl, assembly_status: "done", assembly_progress: null, assembly_error: null })
          .eq("id", projectId);
        console.log(`[upload] ${projectId}: done → ${publicUrl}`);
      } catch (err) {
        const message = err instanceof Error ? err.message : "Upload failed";
        console.error(`[upload] ${projectId} failed:`, message);
        await supabase.from("projects")
          .update({ assembly_status: "preview", assembly_error: message, assembly_progress: null })
          .eq("id", projectId);
      }
    })().catch(console.error);
  });

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

    // Verify project belongs to this user before any state mutation
    const { data: project, error: projectError } = await supabase
      .from("projects")
      .select("id")
      .eq("id", projectId)
      .eq("user_id", user.id)
      .single();
    if (projectError || !project) { res.status(403).json({ error: "Project not found" }); return; }

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
