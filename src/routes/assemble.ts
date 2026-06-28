import { createRequire } from "module";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { type Express, type Request, type Response } from "express";
import { supabase } from "../lib/supabase.js";
import { uploadFile, userFolderForId } from "../lib/storage.js";
import { redis } from "../lib/queue.js";
import { logProjectCost } from "../lib/costs.js";
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

async function downloadFile(url: string, dest: string, signal?: AbortSignal): Promise<void> {
  const maxAttempts = 3;
  let lastError: Error | null = null;

  const attemptFetch = async (): Promise<{ ok: boolean; status: number; body: import("stream/web").ReadableStream | null }> => {
    const timeoutSignal = AbortSignal.timeout(60_000);
    const controller = new AbortController();
    const onAbort = () => controller.abort();
    signal?.addEventListener("abort", onAbort, { once: true });
    timeoutSignal.addEventListener("abort", onAbort, { once: true });
    try {
      return await fetch(url, { signal: controller.signal }) as unknown as { ok: boolean; status: number; body: import("stream/web").ReadableStream | null };
    } finally {
      signal?.removeEventListener("abort", onAbort);
      timeoutSignal.removeEventListener("abort", onAbort);
    }
  };

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const res = await attemptFetch();
      if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
      if (!res.body) throw new Error(`No response body for ${url}`);
      const nodeStream = Readable.fromWeb(res.body);
      await pipeline(nodeStream, fs.createWriteStream(dest));
      return;
    } catch (err) {
      if (signal?.aborted) throw new Error(STOPPED_MARKER);
      const error = err instanceof Error ? err : new Error(String(err));
      lastError = error;
      const shouldRetry = attempt < maxAttempts && /fetch failed|Download timed out|Failed to download .*:( 5\d\d| 429)/i.test(error.message);
      if (!shouldRetry) throw error;
      const backoffMs = 500 * attempt;
      console.warn(`[assemble] download attempt ${attempt} failed for ${url}: ${error.message}; retrying in ${backoffMs}ms`);
      await sleep(backoffMs, signal);
    }
  }

  throw lastError ?? new Error(`Failed to download ${url}`);
}

function escapeConcatListEntry(value: string): string {
  return `file '${value.replace(/\\/g, "/").replace(/'/g, "'\\''")}'`;
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

// ── Memory + stage metrics ────────────────────────────────────────────────────
//
// Track RSS (resident set size — what Render's OOM killer actually
// watches) and elapsed wall-clock per stage so we can tell where memory
// peaked AFTER an assembly completes, instead of guessing from logs.
// Persisted to projects.assembly_metrics at the success path.
interface StageMetric { stage: string; rss_mb: number; heap_mb: number; t_ms: number; }
interface AssemblyMetrics { peak_rss_mb: number; stages: StageMetric[]; }

function createMetricsTracker(): {
  record: (stage: string) => StageMetric;
  snapshot: () => AssemblyMetrics;
} {
  const stages: StageMetric[] = [];
  let peakRssMb = 0;
  let stageStartedAt = Date.now();
  return {
    record: (stage: string): StageMetric => {
      const mem = process.memoryUsage();
      const rss_mb = Math.round(mem.rss / 1024 / 1024);
      const heap_mb = Math.round(mem.heapUsed / 1024 / 1024);
      const now = Date.now();
      const entry: StageMetric = { stage, rss_mb, heap_mb, t_ms: now - stageStartedAt };
      stageStartedAt = now;
      stages.push(entry);
      if (rss_mb > peakRssMb) peakRssMb = rss_mb;
      console.log(`[assemble:metrics] stage=${stage} rss=${rss_mb}MB heap=${heap_mb}MB elapsed=${entry.t_ms}ms`);
      return entry;
    },
    snapshot: (): AssemblyMetrics => ({ peak_rss_mb: peakRssMb, stages: [...stages] }),
  };
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new Error(STOPPED_MARKER));
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(new Error(STOPPED_MARKER));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

// 30 min cap. Burning subtitles re-encodes the full assembled video, which
// on Render's shared CPU can run 15-20+ min for longer scripts. Other
// ffmpeg steps (normalizeClip, concat, mixAudio) finish in seconds-minutes
// so a higher ceiling here doesn't add real latency on the fast path.
const FFMPEG_TIMEOUT_MS = 30 * 60_000;
const ASSEMBLY_SAFE_MODE = process.env.ASSEMBLY_SAFE_MODE === "1";
const DEFAULT_FFMPEG_THREADS = 1;
const FFMPEG_THREADS = ASSEMBLY_SAFE_MODE
  ? 1
  : Math.max(1, Math.min(2, Number.parseInt(process.env.FFMPEG_THREADS ?? String(DEFAULT_FFMPEG_THREADS), 10) || 1));

// Unique marker so the catch path in runAssembly can distinguish a
// user-requested stop from a real error and persist the checkpoint
// instead of clearing it.
const STOPPED_MARKER = "ASSEMBLY_STOPPED_BY_USER";
// Distinct marker for the "skip the final burn, ship the preview"
// path. Surfaces in the catch handler so the terminal-state writer
// knows to promote assembly_preview_url → assembled_url with
// status=done, rather than the usual stopped/failed transitions.
const FINALIZE_PREVIEW_MARKER = "ASSEMBLY_FINALIZE_WITH_PREVIEW";

// When a project has many beats, prefer a single final-stage burn
// of captions/logo instead of baking them into every per-beat clip.
// This reduces the number of concurrent ffmpeg re-encodes at the
// cost of one sequential full-video re-encode.
const FINAL_BURN_THRESHOLD = 80;

function ffmpegWithTimeout(
  build: (cmd: ReturnType<typeof ffmpeg>) => ReturnType<typeof ffmpeg>,
  label: string,
  signal?: AbortSignal,
  threadCount: number = FFMPEG_THREADS,
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new Error(STOPPED_MARKER));
      return;
    }
    const cmd = build(ffmpeg().addOption("-threads", String(threadCount)).addOption("-filter_threads", String(threadCount)));
    // Capture the tail of ffmpeg stderr so error messages aren't
    // just "Conversion failed!" — the real diagnostic (OOM, codec
    // error, filter syntax) is upstream of that. We keep the last
    // ~8 KB so the log line is bounded but useful. fluent-ffmpeg
    // emits stderr line-by-line via its "stderr" event.
    let stderrTail = "";
    const STDERR_TAIL_MAX = 8192;
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
      .on("stderr", (line: string) => {
        stderrTail += line + "\n";
        if (stderrTail.length > STDERR_TAIL_MAX) {
          stderrTail = stderrTail.slice(stderrTail.length - STDERR_TAIL_MAX);
        }
      })
      .on("end", () => settle(resolve))
      .on("error", (err: Error) => settle(() => {
        // Surface stderr tail alongside the generic error message so
        // we can tell OOM apart from filter syntax errors apart from
        // upstream codec issues in the worker logs.
        const tail = stderrTail.trim().split("\n").slice(-12).join(" | ");
        reject(new Error(`${label} failed: ${err.message}${tail ? ` :: ${tail}` : ""}`));
      }))
      .run();
  });
}

export type LogoOverlay = {
  logoPath: string;
  sizePct: number; // 0–1, logo width as fraction of video width
  xPct: number;    // 0–1, top-left x as fraction of video width
  yPct: number;    // 0–1, top-left y as fraction of video height
};

function normalizeClip(
  src: string,
  isImage: boolean,
  duration: number,
  output: string,
  w: number,
  h: number,
  logoOverlay: LogoOverlay | null,
  subtitles: { assPath: string } | null,
  signal?: AbortSignal,
): Promise<void> {
  const baseFilter = `scale=${w}:${h}:force_original_aspect_ratio=decrease,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:black,fps=24`;
  const assFilter = subtitles ? `ass=${escapeAssPath(subtitles.assPath)}` : null;
  // Preset/CRF depends on whether this is now the FINAL encoder pass.
  //   - subtitles=null: the clip is intermediate — concatClips uses
  //     `-c copy` and the mix step also stream-copies the video, so
  //     the current ultrafast/crf 28 stands as the output the user
  //     sees. Keep it fast.
  //   - subtitles=set: captions are baked in here instead of in a
  //     downstream full-video burn pass. That burn pass used
  //     veryfast/crf 23 — match it so captioned final-quality is
  //     identical to the old pipeline. The +CPU cost per beat is
  //     dwarfed by the eliminated whole-video re-encode.
  const preset = subtitles ? "veryfast" : "ultrafast";
  const crf = subtitles ? "23" : "28";

  return ffmpegWithTimeout((cmd) => {
    if (isImage) cmd.input(src).inputOptions(["-loop", "1"]);
    else cmd.input(src).inputOptions(["-stream_loop", "-1"]);

    if (logoOverlay) {
      // Composite the logo during the same encode pass that's
      // already happening for each clip — eliminates the separate
      // full-video re-encode we used to do downstream. The encoder
      // is the dominant cost; adding the overlay filter is ~free.
      // When subtitles are also baked in, append the ass filter
      // *after* the logo overlay so captions render on top of the
      // composited frame, not under the logo.
      const logoW = Math.max(8, Math.min(w, Math.round((w * logoOverlay.sizePct) / 2) * 2));
      const x = Math.round(w * logoOverlay.xPct);
      const y = Math.round(h * logoOverlay.yPct);
      const overlayLabel = assFilter ? "[premix]" : "[v]";
      const graph: string[] = [
        `[0:v]${baseFilter}[base]`,
        `[1:v]scale=w=${logoW}:h=-2[logo]`,
        `[base][logo]overlay=x=${x}:y=${y}:format=auto${overlayLabel}`,
      ];
      if (assFilter) graph.push(`[premix]${assFilter}[v]`);
      cmd.input(logoOverlay.logoPath)
        .complexFilter(graph)
        .outputOptions([
          "-map", "[v]",
          "-t", String(duration),
          "-c:v", "libx264", "-preset", preset, "-crf", crf,
          "-an", "-pix_fmt", "yuv420p",
        ]);
    } else {
      const vf = assFilter ? `${baseFilter},${assFilter}` : baseFilter;
      cmd.outputOptions([
        "-t", String(duration),
        "-vf", vf,
        "-c:v", "libx264", "-preset", preset, "-crf", crf,
        "-an", "-pix_fmt", "yuv420p",
      ]);
    }

    return cmd.output(output);
  }, `normalizeClip`, signal);
}

// Encode a single beat to a complete, self-contained mp4: visual +
// per-beat voiceover + captions + logo, all at the user's chosen
// FINAL resolution. Output is concat-copy ready (every beat shares
// codec, dims, fps, sample rate, channels) so the downstream concat
// step is bitstream-level free.
//
// Stage F (final-burn) is gone in this pipeline — captions, logo,
// and resolution upscale all happen here per beat. The pad/mix
// stages are gone too because per-beat audio gives exact total
// duration and the audio rides INSIDE the beat mp4, concatenating
// for free with the video.
//
// Filter graph (all paths converge on a single `[v]` and a single
// audio map):
//   [0:v] scale+pad+fps              [base]
//   [base] (optional subtitles)      [withSubs]
//   [logo] (optional scale)          [logo]
//   [withSubs][logo] overlay         [v]
//   audio: -map 1:a (the voiceover stream)
//
// Visual handling:
//   - Image: -loop 1 + -t <dur>      → still frame for the whole beat
//   - Video: -stream_loop -1 + -t   → loops short source if needed,
//                                     -t cuts at audio length
// Pick libx264 params based on output resolution. At 4K (2160p+) a
// veryfast/crf 23 encode holds enough reference frames + lookahead
// for libx264 to push past 500 MB resident — which on a 2 GB Render
// Standard instance triggers sporadic exit-234 failures and
// eventual OOM-kills. ultrafast at 4K drops B-frames, CABAC,
// lookahead, and extra refs: ~50% the memory, ~2× faster encode,
// files ~30% larger at the same crf. Acceptable trade for AI-gen
// footage that doesn't need maximum compression.
type EncodeParams = { preset: string; crf: string };
function pickEncodeParams(h: number, degraded: boolean): EncodeParams {
  // 1440p and above: lighter encoder regardless of degraded flag.
  // Below that, default veryfast/crf 23.
  // degraded=true is the retry path — drop further even at 1080p.
  if (degraded || h >= 1440) {
    return { preset: "ultrafast", crf: "28" };
  }
  return { preset: "veryfast", crf: "23" };
}

async function encodeBeatFull(opts: {
  visualPath: string;
  isImage: boolean;
  voiceoverPath: string;
  durationSec: number;
  output: string;
  w: number;
  h: number;
  logoOverlay: LogoOverlay | null;
  subtitles: { assPath: string } | null;
  degraded?: boolean;
  signal?: AbortSignal;
}): Promise<void> {
  const { visualPath, isImage, voiceoverPath, durationSec, output, w, h, logoOverlay, subtitles, degraded, signal } = opts;

  const { preset, crf } = pickEncodeParams(h, degraded ?? false);

  const baseFilter = `scale=${w}:${h}:force_original_aspect_ratio=decrease,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:black,fps=24`;
  const assFilter = subtitles ? `ass=${escapeAssPath(subtitles.assPath)}` : null;

  return ffmpegWithTimeout((cmd) => {
    // Visual input. Image loops naturally; video loops if shorter
    // than the voiceover, gets trimmed by -t if longer.
    if (isImage) cmd.input(visualPath).inputOptions(["-loop", "1"]);
    else cmd.input(visualPath).inputOptions(["-stream_loop", "-1"]);

    // Voiceover input — input index 1. Encoded into the output mp4
    // so subsequent concat -c copy preserves it across beat
    // boundaries without re-encoding audio.
    cmd.input(voiceoverPath);

    if (logoOverlay) {
      // Three inputs: visual (0), audio (1), logo image (2).
      const logoW = Math.max(8, Math.min(w, Math.round((w * logoOverlay.sizePct) / 2) * 2));
      const x = Math.round(w * logoOverlay.xPct);
      const y = Math.round(h * logoOverlay.yPct);
      cmd.input(logoOverlay.logoPath);
      const graph: string[] = [];
      // Subtitles apply BEFORE overlay so the logo composites on top
      // of the captioned frame (matches the previous behavior of
      // normalizeClip).
      if (assFilter) {
        graph.push(`[0:v]${baseFilter},${assFilter}[base]`);
      } else {
        graph.push(`[0:v]${baseFilter}[base]`);
      }
      graph.push(`[2:v]scale=w=${logoW}:h=-2[logo]`);
      graph.push(`[base][logo]overlay=x=${x}:y=${y}:format=auto[v]`);
      cmd.complexFilter(graph);
      cmd.outputOptions([
        "-map", "[v]", "-map", "1:a",
        "-c:v", "libx264", "-preset", preset, "-crf", crf, "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
        "-t", String(durationSec),
        "-shortest",
      ]);
    } else {
      // No logo — simpler -vf chain on the visual input.
      const vf = assFilter ? `${baseFilter},${assFilter}` : baseFilter;
      cmd.outputOptions([
        "-map", "0:v", "-map", "1:a",
        "-vf", vf,
        "-c:v", "libx264", "-preset", preset, "-crf", crf, "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
        "-t", String(durationSec),
        "-shortest",
      ]);
    }

    return cmd.output(output);
  }, "encodeBeatFull", signal);
}

// Build proportional caption segments for one beat from its script
// segment + voiceover duration. No STT — words are laid out evenly
// across the voiceover length. Less precise than word-level
// timestamps but free, and accurate within ~one word boundary on
// natural-cadence narration.
function buildBeatCaptions(scriptSegment: string, durationSec: number, wps = 5): SrtSegment[] {
  const words = scriptSegment.trim().split(/\s+/).filter(Boolean);
  if (!words.length) return [];
  const segs: SrtSegment[] = [];
  for (let i = 0; i < words.length; i += wps) {
    const chunk = words.slice(i, Math.min(i + wps, words.length));
    const start = (i / words.length) * durationSec;
    const end = (Math.min(i + wps, words.length) / words.length) * durationSec;
    segs.push({ index: segs.length + 1, start, end, text: chunk.join(" ") });
  }
  return segs;
}

function concatClips(listFile: string, output: string, signal?: AbortSignal): Promise<void> {
  return ffmpegWithTimeout((cmd) =>
    cmd
      .input(listFile).inputOptions(["-f", "concat", "-safe", "0"])
      .outputOptions(["-c", "copy"])
      .output(output),
    "concat",
    signal,
    1,
  );
}

// Trim leading + trailing silence from an mp3 without disturbing
// internal pauses (the breath beats between sentences inside a single
// clip stay put). This is what removes the ~150–300ms of dead air that
// ElevenLabs Turbo leaves at the front and back of every render — those
// pads add up when you concat N beats head-to-tail.
//
// Filter graph explanation:
//   1. silenceremove start_periods=1 start_threshold=-50dB
//      → drop one block of silence from the head (everything below
//        -50dBFS until the first non-silent sample).
//   2. aformat dblp + areverse
//      → flip the buffer end-to-end so the original tail is now at the
//        head. silenceremove only trims from the head, so this is how
//        we reach the trailing silence.
//   3. silenceremove again with the same params trims the (reversed)
//      head, which is the original tail.
//   4. aformat dblp + areverse flips it back to forward time.
//
// We re-encode to libmp3lame 128k mono/stereo (matches Turbo's own
// output) so every trimmed file shares codec + sample rate. That keeps
// the downstream concatClips() `-c copy` pass valid (no re-encode at
// the join step → still no audible artifacts at boundaries).
function trimSilence(input: string, output: string, signal?: AbortSignal): Promise<void> {
  return ffmpegWithTimeout((cmd) =>
    cmd
      .input(input)
      .audioFilters([
        "silenceremove=start_periods=1:start_silence=0:start_threshold=-50dB:detection=peak",
        "aformat=dblp",
        "areverse",
        "silenceremove=start_periods=1:start_silence=0:start_threshold=-50dB:detection=peak",
        "aformat=dblp",
        "areverse",
      ])
      .outputOptions(["-c:a", "libmp3lame", "-b:a", "128k", "-ar", "44100"])
      .output(output),
    "trimSilence",
    signal,
  );
}

// Build a tiny still-frame mp4 from joined.mp4's last frame, encoded
// for `durationSec` at the same resolution/fps/pixfmt as the
// per-beat clips. Then concat-copy joined + tail → padded. This
// replaces the old tpad full-video re-encode that ran on every
// legacy-mode assembly whose voiceover had trailing silence (most of
// them). A 30-minute 1080p re-encode at ultrafast is still several
// minutes of CPU and a sustained memory spike; the still-frame
// encode is sub-second and the concat copy is I/O-bound.
async function buildFreezeTail(
  joinedPath: string,
  tmpDir: string,
  durationSec: number,
  w: number,
  h: number,
  matchPreset: { preset: string; crf: string },
  signal?: AbortSignal,
): Promise<string> {
  const lastFrame = path.join(tmpDir, "lastframe.jpg");
  await ffmpegWithTimeout((cmd) =>
    cmd
      .input(joinedPath)
      .inputOptions(["-sseof", "-3"])
      .outputOptions(["-update", "1", "-q:v", "2", "-frames:v", "1"])
      .output(lastFrame),
    "extractLastFrame",
    signal,
  );
  const tailPath = path.join(tmpDir, "tail.mp4");
  // Match the surrounding per-beat clip params (preset + crf) exactly
  // so concat -c copy is bitstream-safe. ultrafast and veryfast use
  // different entropy coders (CAVLC vs CABAC) in libx264 — mixing
  // them in a single concatenated stream can confuse strict decoders.
  // The caller passes the same {preset, crf} pair normalizeClip used.
  await ffmpegWithTimeout((cmd) =>
    cmd
      .input(lastFrame).inputOptions(["-loop", "1"])
      .outputOptions([
        "-t", String(durationSec),
        "-vf", `scale=${w}:${h}:force_original_aspect_ratio=decrease,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:black,fps=24`,
        "-c:v", "libx264", "-preset", matchPreset.preset, "-crf", matchPreset.crf,
        "-an", "-pix_fmt", "yuv420p",
      ])
      .output(tailPath),
    "encodeFreezeTail",
    signal,
  );
  try { fs.unlinkSync(lastFrame); } catch { /* ignore */ }
  return tailPath;
}

// Add background music to a joined video that ALREADY contains
// voiceover audio (baked into each beat during Stage B). Single
// ffmpeg pass with -c:v copy — the video stream is untouched, only
// the audio tracks are mixed. Compared to the old mixAudio function,
// this is structurally simpler: two inputs instead of three, no
// voiceover concat needed (already done by the video concat).
function addBgmToVideo(
  video: string,
  bgmPath: string,
  bgmVolume: number,
  bgmLoops: number,
  output: string,
  videoDuration: number,
  signal?: AbortSignal,
): Promise<void> {
  return ffmpegWithTimeout((cmd) => {
    const lightInput = ["-thread_queue_size", "512", "-probesize", "1000000", "-analyzeduration", "0"];
    cmd.input(video).inputOptions([...lightInput, "-fflags", "+genpts"]);
    // bgmLoops is FINITE (ceil(voiceover/bgm)+1) so amix drains
    // cleanly when both inputs hit EOF. -stream_loop -1 here caused
    // muxer deadlocks in the old pipeline; the finite count is the
    // proven fix.
    cmd.input(bgmPath).inputOptions([...lightInput, "-stream_loop", String(bgmLoops)]);
    cmd.complexFilter([
      `[1:a]volume=${bgmVolume}[bgmDucked]`,
      `[0:a][bgmDucked]amix=inputs=2:duration=first:dropout_transition=0[mix]`,
    ]);
    cmd.outputOptions([
      "-map", "0:v", "-map", "[mix]",
      "-c:v", "copy",
      "-c:a", "aac", "-b:a", "128k",
      "-t", String(videoDuration),
      "-shortest",
      "-max_muxing_queue_size", "1024",
    ]);
    return cmd.output(output);
  }, "addBgmToVideo", signal);
}

// ── Transcription ─────────────────────────────────────────────────────────────

interface TranscriptionWord {
  text?: string; word?: string;
  start?: number; start_time?: number;
  end?: number; end_time?: number;
  type?: string;
}

async function transcribeAudio(audioPath: string, apiKey: string, signal?: AbortSignal): Promise<TranscriptionWord[]> {
  const audioBytes = fs.readFileSync(audioPath);
  const MAX_ATTEMPTS = 4;
  let lastError = "";
  try {
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      if (signal?.aborted) throw new Error(STOPPED_MARKER);
      const formData = new FormData();
      formData.append("file", new Blob([audioBytes], { type: "audio/mpeg" }), "voiceover.mp3");
      formData.append("model_id", "scribe_v1");
      formData.append("timestamps_granularity", "word");
      formData.append("tag_audio_events", "false");
      const res = await fetch("https://api.elevenlabs.io/v1/speech-to-text", {
        method: "POST", headers: { "xi-api-key": apiKey }, body: formData, signal,
      });
      if (res.ok) {
        const data = await res.json() as { words?: TranscriptionWord[] };
        return (data.words ?? []).filter((w) => (w.type ?? "word") === "word");
      }
      lastError = `ElevenLabs STT ${res.status}: ${await res.text()}`;
      if (res.status !== 429 || attempt === MAX_ATTEMPTS) break;
      await sleep(2000 * Math.pow(2, attempt - 1), signal);
    }
  } catch (err) {
    if (signal?.aborted) throw new Error(STOPPED_MARKER);
    throw err instanceof Error ? err : new Error(String(err));
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

type Beat = {
  beat_number: number;
  script_segment: string | null;
  video_url: string | null;
  image_url: string | null;
  duration_ms?: number | null;
  voiceover_url?: string | null;
  voiceover_duration_ms?: number | null;
};

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

// Escape an ASS subtitle file path for ffmpeg's filtergraph syntax.
// Backslashes become forward slashes (Windows compatibility) and
// colons get escaped so ffmpeg doesn't read the rest of the path as
// filter options. Used by both the per-clip normalizeClip caption
// bake-in and (historically) the standalone burn pass.
function escapeAssPath(p: string): string {
  return p.replace(/\\/g, "/").replace(/:/g, "\\:");
}

// Slice the absolute-timeline segment list down to a single beat's
// window and re-base timings to be beat-relative. The per-beat
// normalizeClip pass burns captions directly into each clip; each
// beat needs only the segments that fall (or partially fall) within
// its [beatStart, beatStart + duration) window, with start/end
// shifted by -beatStart and clipped to [0, duration]. Segments that
// span beat boundaries are sliced into both beats so the caption
// text appears continuously across the concat boundary.
function sliceSegmentsForBeat(segs: SrtSegment[], beatStart: number, duration: number): SrtSegment[] {
  const beatEnd = beatStart + duration;
  const out: SrtSegment[] = [];
  for (const seg of segs) {
    if (seg.end <= beatStart || seg.start >= beatEnd) continue;
    const start = Math.max(seg.start, beatStart) - beatStart;
    const end = Math.min(seg.end, beatEnd) - beatStart;
    if (end > start) {
      out.push({ index: out.length + 1, start, end, text: seg.text });
    }
  }
  return out;
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
  // True when the per-beat clips in clip_urls had captions baked into
  // their normalizeClip pass (the new pipeline). Compared against
  // the current run's captionsEnabled at load — a mismatch (caption
  // state flipped, or style changed) invalidates clip_urls and every
  // visuals stage downstream, because the cached clips' caption
  // layer (or its absence) won't match what this run wants. Old
  // checkpoints written before this field existed read as undefined
  // → falsy, which is the right default for projects that pre-date
  // the bake-in path.
  clips_baked_captions?: boolean;
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
  // trimSilenceEnabled changes the audio track contents, so it has to
  // be in the core hash — toggling it invalidates everything from the
  // mix step down. Aspect ratio and voiceoverType already covered the
  // core but didn't anticipate the silence-trim flag.
  // Background music is also part of the audio mix, so a change
  // (different file or different volume) must blow away the mixed
  // checkpoint.
  const bgmKey = opts.backgroundMusicUrl
    ? `${opts.backgroundMusicUrl}@${opts.backgroundMusicVolume ?? 0.15}`
    : "nobgm";
  // resolution changes the dimensions every clip is encoded at, so it
  // must invalidate the entire checkpoint chain from normalizeClip
  // forward.
  // Logo properties go into the core hash too — changing the file,
  // position, or size invalidates the post-mix overlay output.
  const logoKey = opts.logoUrl
    ? `${opts.logoUrl}@${opts.logoSize ?? 0.1}x${opts.logoX ?? 0.85},${opts.logoY ?? 0.05}`
    : "nologo";
  return hashString(`${opts.aspectRatio}|${opts.voiceoverType}|${opts.trimSilenceEnabled ? "trim" : "raw"}|${bgmKey}|${opts.resolution ?? "1080p"}|${logoKey}`);
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

// Combined signal poll: returns whichever interrupt the user has
// requested (stop OR finalize-with-preview), or null if neither is
// set. Combining the two flags into one query keeps the polling
// cost flat — the existing stopPoll fires every 3s and used to do
// exactly one Supabase select; now it still does exactly one.
type InterruptSignal = "stop" | "finalize-preview" | null;
async function readInterruptSignal(projectId: string): Promise<InterruptSignal> {
  const { data } = await supabase
    .from("projects")
    .select("assembly_stop_requested, assembly_finalize_preview_requested")
    .eq("id", projectId)
    .single();
  if (!data) return null;
  // finalize-preview wins if both are somehow set — it's the
  // "happy path" terminal outcome (user gets a done video) where
  // stop just halts the run.
  if ((data as { assembly_finalize_preview_requested?: boolean }).assembly_finalize_preview_requested) return "finalize-preview";
  if ((data as { assembly_stop_requested?: boolean }).assembly_stop_requested) return "stop";
  return null;
}

// ── Background assembly job ───────────────────────────────────────────────────

type ResolutionPreset = "720p" | "1080p" | "1440p" | "2160p";

interface AssembleOptions {
  userId: string; projectId: string; aspectRatio: string; voiceoverType: "cleaned" | "original";
  captionsEnabled: boolean; captionsLanguage: string; captionsStyle: string; captionsSize: string; captionsPosition: string;
  // When true, run the per-beat trimSilence pass before concat. Off by
  // default so a normal Assemble produces audio identical to the source
  // beats. Toggled on by the assemble page's dedicated "Trim silences"
  // button — the user opts in explicitly.
  trimSilenceEnabled: boolean;
  // Optional background music. When present, the file at this URL is
  // downloaded and mixed under the voiceover at the given volume (0–1,
  // default 0.15). The mix duration matches the voiceover so the track
  // never extends past the narration. Empty/undefined disables bgm.
  backgroundMusicUrl?: string | null;
  backgroundMusicVolume?: number;
  // Render resolution preset. Drives the [w, h] passed to normalizeClip
  // for every per-beat clip. Defaults to "1080p" (1920×1080 for 16:9).
  // Picked from the assemble page; backwards compat with the old
  // hardcoded 480p when undefined keeps existing in-flight runs sane.
  resolution?: ResolutionPreset;
  // Optional channel logo overlay. URL is the user-uploaded logo
  // image; position is the top-left corner as a fraction of video
  // dimensions (0–1); size is the logo width as a fraction of video
  // width. All three are needed to render the overlay — null/undefined
  // disables it.
  logoUrl?: string | null;
  logoX?: number;
  logoY?: number;
  logoSize?: number;
}

// Aspect-ratio-aware dimensions for a resolution preset. The number
// stays the "short side" for vertical (9:16) so the user gets a
// portrait video at the expected total pixels (e.g. 1080p portrait =
// 1080×1920 like TikTok/Reels), and the "long side" for 16:9 so 1080p
// horizontal = 1920×1080 like YouTube. Square keeps it on both axes.
function dimsFor(aspect: string, preset: ResolutionPreset | undefined): [number, number] {
  const effectivePreset = ASSEMBLY_SAFE_MODE ? "720p" : (preset ?? "1080p");
  const map = { "720p": { long: 1280, short: 720 }, "1080p": { long: 1920, short: 1080 }, "1440p": { long: 2560, short: 1440 }, "2160p": { long: 3840, short: 2160 } } as const;
  const { long, short } = map[effectivePreset];
  if (aspect === "9:16") return [short, long];
  if (aspect === "1:1") return [short, short];
  return [long, short]; // 16:9 default
}

// Intermediate resolution for the per-beat encode pool when we know a
// final-burn upscale pass is coming anyway. Cap at 720p so 1080p+
// projects pay 720p-sized memory per clip — the final-burn pass
// already re-encodes the whole video at the target resolution, so the
// per-clip resolution only affects how much intermediate disk + RAM
// each Stage B clip costs. ASSEMBLY_SAFE_MODE was already pulling the
// final output down to 720p, so the cap is harmless there. Lower
// resolutions (≤720p) pass through unchanged. Captions/logo bake into
// the final-burn pass at the user's chosen resolution, so the intermediate
// downscale doesn't blur text.
function intermediateDimsFor(
  aspect: string,
  preset: ResolutionPreset | undefined,
  useFinalBurn: boolean,
): [number, number] {
  if (!useFinalBurn) return dimsFor(aspect, preset);
  const cap: ResolutionPreset = "720p";
  const rank: Record<ResolutionPreset, number> = { "720p": 0, "1080p": 1, "1440p": 2, "2160p": 3 };
  const effective = preset && rank[preset] > rank[cap] ? cap : (preset ?? cap);
  return dimsFor(aspect, effective);
}

function getAssemblyConcurrency(beatCount: number): number {
  if (ASSEMBLY_SAFE_MODE) return 1;
  const requested = Math.max(1, getAssemblyBeatLimit());
  if (beatCount > 80) return 1;
  return Math.min(2, requested);
}

async function runAssembly(opts: AssembleOptions): Promise<void> {
  const { userId, projectId, aspectRatio, voiceoverType, captionsEnabled, captionsLanguage, captionsStyle, captionsSize, captionsPosition, trimSilenceEnabled, backgroundMusicUrl, backgroundMusicVolume, resolution, logoUrl, logoX, logoY, logoSize } = opts;
  const [finalW, finalH] = dimsFor(aspectRatio, resolution);
  console.log(`[assemble] ${projectId}: resolution=${resolution ?? "1080p"} dims=${finalW}x${finalH}`);

  // Memory + per-stage wall-clock tracker. Stamped at every stage
  // boundary below. Final snapshot is written to projects.assembly_metrics
  // on success so we can see where memory peaked after the fact instead
  // of guessing from server logs.
  const metrics = createMetricsTracker();
  metrics.record("start");

  const progress = (msg: string) => {
    console.log(`[assemble] ${projectId}: ${msg}`);
    return setProgress(projectId, msg);
  };

  // Throttled per-beat progress writer for hot loops (audio prep,
  // Stage B encode). Still console.logs every message for debugging,
  // but rate-limits the Supabase update so a 137-beat loop doesn't
  // pay 137 serialized DB write costs (~100-200ms each under load —
  // multiple SECONDS of latency the workers were waiting on between
  // beats). The next phase's unconditional progress() call always
  // overtakes the final throttled write within a second, so the UI
  // never sees a stuck count.
  let lastProgressDbAt = 0;
  const PROGRESS_MIN_INTERVAL_MS = 500;
  const progressThrottled = async (msg: string) => {
    console.log(`[assemble] ${projectId}: ${msg}`);
    const now = Date.now();
    if (now - lastProgressDbAt < PROGRESS_MIN_INTERVAL_MS) return;
    lastProgressDbAt = now;
    await setProgress(projectId, msg);
  };

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "assemble-"));

  // Interrupt signal: a background poll watches two flags every 3s —
  // assembly_stop_requested (Stop button) and
  // assembly_finalize_preview_requested (Use this version button).
  // Either flips abortReason and aborts via the shared
  // AbortController. The catch path below reads abortReason to
  // decide between the "stopped" and "finalize with preview"
  // terminal transitions. Explicit checkStop() calls between stages
  // mean we don't have to wait up to 3s for the poll to notice when
  // an awaited stage finishes.
  const aborter = new AbortController();
  const signal = aborter.signal;
  let abortReason: "stop" | "finalize-preview" | null = null;
  const triggerInterrupt = (which: "stop" | "finalize-preview") => {
    abortReason = which;
    const marker = which === "stop" ? STOPPED_MARKER : FINALIZE_PREVIEW_MARKER;
    aborter.abort(new Error(marker));
  };
  const stopPoll = setInterval(async () => {
    if (signal.aborted) return;
    try {
      const sig = await readInterruptSignal(projectId);
      if (sig) {
        console.log(`[assemble] ${projectId}: interrupt=${sig} — aborting`);
        triggerInterrupt(sig);
      }
    } catch {
      // poll error — swallow, will retry next tick
    }
  }, 3000);
  const checkStop = async (): Promise<void> => {
    if (signal.aborted) {
      // Pick the matching marker for the already-set reason so the
      // catch handler routes to the right terminal transition.
      throw new Error(abortReason === "finalize-preview" ? FINALIZE_PREVIEW_MARKER : STOPPED_MARKER);
    }
    // Also check directly so an interrupt click between stages doesn't
    // wait up to a poll tick.
    const sig = await readInterruptSignal(projectId);
    if (sig) {
      triggerInterrupt(sig);
      throw new Error(sig === "finalize-preview" ? FINALIZE_PREVIEW_MARKER : STOPPED_MARKER);
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
      const prevBaked = !!loaded.clips_baked_captions;
      const captionsMatch = loaded.captions_hash === currentCaptionsHash && prevBaked === captionsEnabled;
      if (captionsMatch) {
        checkpoint = loaded;
      } else if (captionsEnabled || prevBaked) {
        // Captions are now baked into per-beat normalizeClip passes
        // (instead of a downstream burn). So any change to caption
        // state — toggling on/off or restyling — invalidates every
        // cached clip and everything downstream. Keep just the audio
        // side: transcription_words is style-independent and
        // expensive to redo. The new run will re-encode clips with
        // the right (or no) caption layer.
        console.log(`[assemble] ${projectId}: caption state changed (baked=${prevBaked}→${captionsEnabled}, hash=${loaded.captions_hash}→${currentCaptionsHash}) — discarding visuals checkpoint`);
        checkpoint = {
          core_hash: loaded.core_hash,
          captions_hash: currentCaptionsHash,
          transcription_words: loaded.transcription_words,
        };
      } else {
        // captionsEnabled=false on both sides — nothing was baked in
        // either way. Only the standalone captioned_url would have
        // been affected, but that no longer exists on this path.
        // Old checkpoints with a captioned_url field are tolerated
        // (the field just goes unused).
        console.log(`[assemble] ${projectId}: captions opts changed (off→off) — checkpoint usable as-is`);
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
      supabase.from("project_beats").select("beat_number, script_segment, video_url, image_url, duration_ms, voiceover_url, voiceover_duration_ms").eq("project_id", projectId).order("beat_number"),
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
    if (!allBeats.length) throw new Error("No beats found in this project.");

    // ── Per-beat mode required ──────────────────────────────────────────
    //
    // This build assembles every beat as a complete, self-contained
    // mp4 (visual + voiceover + captions + logo, at the user's chosen
    // final resolution). The downstream pipeline is just concat +
    // optional bgm — no final-burn, no upscale, no separate audio mix.
    // Legacy single-voiceover projects can't fit this shape without an
    // STT+alignment pass that was removed for simplicity. Reject them
    // with a clear error rather than silently falling back.
    const anyVoiceover = allBeats.some((b) => !!b.voiceover_url);
    if (!anyVoiceover) {
      throw new Error("Per-beat voiceovers required on this build — generate beat voiceovers before assembling.");
    }
    const beats = allBeats.filter((b) => (b.video_url || b.image_url) && b.voiceover_url);
    if (!beats.length) {
      throw new Error("No beats have both a visual and a voiceover yet — generate at least one beat's voiceover and visual first.");
    }
    const videoCount = beats.filter((b) => b.video_url).length;
    const droppedNoVoiceover = allBeats.filter((b) => (b.video_url || b.image_url) && !b.voiceover_url).length;
    console.log(`[assemble] ${projectId}: assembling ${beats.length}/${allBeats.length} beats (${videoCount} video, ${beats.length - videoCount} image${droppedNoVoiceover > 0 ? `, ${droppedNoVoiceover} dropped: no voiceover` : ""})`);

    // Encode at the user's chosen FINAL resolution from the start.
    // No intermediate downscale + upscale-at-end pass — the per-beat
    // architecture eliminates the slow final-burn step where that
    // optimization used to live.
    const w = finalW;
    const h = finalH;

    metrics.record("load-project");

    // Compute per-beat durations. Trim-on path requires downloading
    // and re-measuring audio per beat; trim-off uses the persisted
    // voiceover_duration_ms. totalDuration is needed before Stage B
    // when bgm is enabled (for finite loop count) so we resolve all
    // durations up front.
    const durations: number[] = new Array(beats.length).fill(0);
    const preTrimmedVoiceoverPaths: string[] = new Array(beats.length).fill("");
    if (trimSilenceEnabled) {
      await checkStop();
      await progress(`Preparing ${beats.length} beat voiceovers (trim)…`);
      const audioLimit = ASSEMBLY_SAFE_MODE ? 1 : 2;
      let nextAudioIdx = 0;
      let firstAudioError: Error | null = null;
      const audioWorker = async (): Promise<void> => {
        while (true) {
          if (firstAudioError) return;
          const i = nextAudioIdx++;
          if (i >= beats.length) return;
          try {
            await checkStop();
            const beat = beats[i];
            const rawPath = path.join(tmpDir, `voice_raw_${String(i).padStart(3, "0")}.mp3`);
            await downloadFile(beat.voiceover_url!, rawPath, signal);
            const trimmedPath = path.join(tmpDir, `voice_${String(i).padStart(3, "0")}.mp3`);
            await trimSilence(rawPath, trimmedPath, signal);
            try { fs.unlinkSync(rawPath); } catch { /* ignore */ }
            const dur = await getMediaDuration(trimmedPath);
            preTrimmedVoiceoverPaths[i] = trimmedPath;
            durations[i] = Math.max(0.1, dur);
          } catch (e) {
            if (!firstAudioError) firstAudioError = e instanceof Error ? e : new Error(String(e));
            return;
          }
        }
      };
      await Promise.all(Array.from({ length: audioLimit }, () => audioWorker()));
      if (firstAudioError) throw firstAudioError;
    } else {
      for (let i = 0; i < beats.length; i++) {
        const dur = (beats[i].voiceover_duration_ms ?? 0) / 1000;
        if (dur <= 0) {
          throw new Error(`Beat ${beats[i].beat_number} is missing voiceover_duration_ms — re-run voiceover generation.`);
        }
        durations[i] = Math.max(0.1, dur);
      }
    }
    const totalDuration = durations.reduce((s, d) => s + d, 0);
    console.log(`[assemble] ${projectId}: total duration = ${totalDuration.toFixed(2)}s (sum of ${durations.length} beats)`);

    metrics.record("voiceover-prep");

    // ── Stage B: per-beat full encode ───────────────────────────────────
    //
    // Each beat produces a complete mp4 — visual scaled/padded to
    // finalW×finalH, per-beat voiceover muxed in, optional captions
    // burned in, optional logo overlaid. All beats share codec, dims,
    // fps, sample rate, and channels so the concat step downstream
    // is a pure -c copy with no re-encode (and no audio mix).
    if (!checkpoint.joined_url) {
      checkpoint.clip_urls = checkpoint.clip_urls ?? new Array(beats.length).fill(null);
      checkpoint.clips_baked_captions = captionsEnabled;
      await persistCheckpoint();
      const clipPaths: string[] = new Array(beats.length).fill("");
      await progress("Processing beats…");

      const pendingClipUploads = new Set<Promise<void>>();
      let persistDirty = false;
      let persistInFlight: Promise<void> = Promise.resolve();
      let persistDebounceTimer: ReturnType<typeof setTimeout> | null = null;
      let persistHeartbeatTimer: ReturnType<typeof setInterval> | null = null;
      const PERSIST_DEBOUNCE_MS = 2_000;
      const PERSIST_HEARTBEAT_MS = 30_000;
      const flushPersist = async (): Promise<void> => {
        if (persistDebounceTimer) { clearTimeout(persistDebounceTimer); persistDebounceTimer = null; }
        if (!persistDirty) { await persistInFlight; return; }
        persistDirty = false;
        persistInFlight = persistInFlight.then(persistCheckpoint, persistCheckpoint);
        await persistInFlight;
      };
      const markPersistDirty = (): void => {
        persistDirty = true;
        if (persistDebounceTimer) return;
        persistDebounceTimer = setTimeout(() => {
          persistDebounceTimer = null;
          void flushPersist();
        }, PERSIST_DEBOUNCE_MS);
      };
      persistHeartbeatTimer = setInterval(() => { void flushPersist(); }, PERSIST_HEARTBEAT_MS);

      // Logo is one image shared by every beat — download once.
      let logoOverlay: LogoOverlay | null = null;
      if (logoUrl) {
        await checkStop();
        await progress("Downloading channel logo…");
        const logoPath = path.join(tmpDir, "logo");
        try {
          await downloadFile(logoUrl, logoPath, signal);
          logoOverlay = {
            logoPath,
            sizePct: typeof logoSize === "number" ? logoSize : 0.1,
            xPct: typeof logoX === "number" ? logoX : 0.85,
            yPct: typeof logoY === "number" ? logoY : 0.05,
          };
        } catch (e) {
          console.warn(`[assemble] logo download failed, encoding beats without overlay:`, e);
        }
      }

      const beatLimit = getAssemblyConcurrency(beats.length);
      console.log(`[assemble] ${projectId}: beat concurrency=${beatLimit} (requested=${getAssemblyBeatLimit()}, beats=${beats.length})`);
      let nextIdx = 0;
      let completed = 0;
      let firstError: Error | null = null;

      const processOne = async (i: number): Promise<void> => {
        const clipPath = path.join(tmpDir, `clip_${String(i).padStart(3, "0")}.mp4`);
        const cached = checkpoint.clip_urls![i];
        if (cached) {
          try {
            await downloadFile(cached, clipPath, signal);
            clipPaths[i] = clipPath;
            return;
          } catch (e) {
            console.warn(`[assemble] beat ${beats[i].beat_number}: cached clip download failed, re-encoding:`, e);
          }
        }
        const beat = beats[i];

        // Resolve the voiceover local path. Trim-on populated
        // preTrimmedVoiceoverPaths[i] up front; trim-off downloads
        // just-in-time so we don't hold all voiceovers in tmp at once.
        let voiceoverLocal = preTrimmedVoiceoverPaths[i];
        let cleanupVoice = false;
        if (!voiceoverLocal) {
          const dlPath = path.join(tmpDir, `voice_${String(i).padStart(3, "0")}.mp3`);
          await downloadFile(beat.voiceover_url!, dlPath, signal);
          voiceoverLocal = dlPath;
          cleanupVoice = true;
        }

        // Captions: proportional word timing within the beat's
        // voiceover window. No STT — accuracy is "good enough" for
        // typical narration cadence and we avoid the API round-trips
        // and cost.
        let beatSubtitles: { assPath: string } | null = null;
        if (captionsEnabled && beat.script_segment) {
          const beatSegs = buildBeatCaptions(beat.script_segment, durations[i]);
          if (beatSegs.length > 0) {
            const assPath = path.join(tmpDir, `captions_${String(i).padStart(3, "0")}.ass`);
            writeAss(beatSegs, buildAssStyle(captionsStyle, captionsSize, captionsPosition, h), w, h, assPath);
            beatSubtitles = { assPath };
          }
        }

        try {
          let visualPath = "";
          let isImage = false;
          if (beat.video_url) {
            const ext = beat.video_url.includes(".webm") ? "webm" : "mp4";
            visualPath = path.join(tmpDir, `src_${i}.${ext}`);
            await downloadFile(beat.video_url, visualPath, signal);
            isImage = false;
          } else if (beat.image_url) {
            const ext = beat.image_url.toLowerCase().includes(".png") ? "png" : "jpg";
            visualPath = path.join(tmpDir, `src_${i}.${ext}`);
            await downloadFile(beat.image_url, visualPath, signal);
            isImage = true;
          } else {
            return;
          }

          // First attempt with the resolution-tier-default encoder
          // params. If it fails (typically OOM/exit 234 on 4K under
          // memory pressure), retry once with degraded=true to force
          // ultrafast/crf 28 — meaningfully lower memory and faster,
          // at the cost of slightly bigger files. Silent drops corrupt
          // the audio/visual timeline, so we'd rather pay the size
          // cost than lose a beat.
          try {
            await encodeBeatFull({
              visualPath,
              isImage,
              voiceoverPath: voiceoverLocal,
              durationSec: durations[i],
              output: clipPath,
              w, h,
              logoOverlay,
              subtitles: beatSubtitles,
              signal,
            });
          } catch (encodeErr) {
            if (encodeErr instanceof Error && (encodeErr.message === STOPPED_MARKER || encodeErr.message === FINALIZE_PREVIEW_MARKER)) {
              throw encodeErr;
            }
            console.warn(`[assemble] beat ${beat.beat_number}: first encode failed (${encodeErr instanceof Error ? encodeErr.message : encodeErr}); retrying with degraded params`);
            await encodeBeatFull({
              visualPath,
              isImage,
              voiceoverPath: voiceoverLocal,
              durationSec: durations[i],
              output: clipPath,
              w, h,
              logoOverlay,
              subtitles: beatSubtitles,
              degraded: true,
              signal,
            });
            console.log(`[assemble] beat ${beat.beat_number}: degraded encode succeeded`);
          }

          try { fs.unlinkSync(visualPath); } catch { /* ignore */ }
          if (cleanupVoice) { try { fs.unlinkSync(voiceoverLocal); } catch { /* ignore */ } }
          if (beatSubtitles) { try { fs.unlinkSync(beatSubtitles.assPath); } catch { /* ignore */ } }

          const uploadPromise = (async () => {
            try {
              const clipUrl = await uploadFile(ckptPathFor(`clip_${String(i).padStart(3, "0")}.mp4`), clipPath, "video/mp4");
              checkpoint.clip_urls![i] = clipUrl;
              markPersistDirty();
            } catch (uploadErr) {
              console.warn(`[assemble] beat ${beat.beat_number}: clip checkpoint upload failed:`, uploadErr);
            }
          })();
          pendingClipUploads.add(uploadPromise);
          void uploadPromise.finally(() => { pendingClipUploads.delete(uploadPromise); });
          clipPaths[i] = clipPath;
        } catch (e) {
          if (e instanceof Error && (e.message === STOPPED_MARKER || e.message === FINALIZE_PREVIEW_MARKER)) throw e;
          console.error(`[assemble] beat ${beats[i].beat_number} skipped:`, e);
        }
      };

      const beatWorker = async (): Promise<void> => {
        while (true) {
          if (firstError) return;
          const i = nextIdx++;
          if (i >= beats.length) return;
          try {
            await checkStop();
            await processOne(i);
          } catch (e) {
            if (!firstError) firstError = e instanceof Error ? e : new Error(String(e));
            return;
          }
          completed++;
          if (beats.length > 0) {
            await progressThrottled(`Processed ${completed} of ${beats.length} beats…`);
          }
        }
      };

      await Promise.all(Array.from({ length: beatLimit }, () => beatWorker()));

      if (pendingClipUploads.size > 0) {
        await progress(`Finalizing ${pendingClipUploads.size} beat uploads…`);
        await Promise.allSettled([...pendingClipUploads]);
      }
      if (persistHeartbeatTimer) { clearInterval(persistHeartbeatTimer); persistHeartbeatTimer = null; }
      await flushPersist();
      if (firstError) throw firstError;
      metrics.record("stage-b-encode");

      if (logoOverlay) { try { fs.unlinkSync(logoOverlay.logoPath); } catch { /* ignore */ } }

      // ── Stage C: concat all beats ─────────────────────────────────────
      await checkStop();
      await progress("Joining beats…");
      const validClipPaths = clipPaths.filter((p) => p !== "");
      if (!validClipPaths.length) throw new Error("All beats failed to encode — nothing to assemble.");
      if (validClipPaths.length < clipPaths.length) {
        const dropped = clipPaths
          .map((p, idx) => p === "" ? beats[idx].beat_number : null)
          .filter((n): n is number => n !== null);
        console.warn(`[assemble] ${projectId}: ${dropped.length} beat(s) failed — dropping beats ${dropped.join(",")}`);
      }
      const listPath = path.join(tmpDir, "concat.txt");
      fs.writeFileSync(listPath, validClipPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n"));
      const joinedLocal = path.join(tmpDir, "joined.mp4");
      await concatClips(listPath, joinedLocal, signal);
      for (const p of validClipPaths) { try { fs.unlinkSync(p); } catch { /* ignore */ } }
      metrics.record("stage-c-concat");
      try {
        const joinedUrl = await uploadFile(ckptPathFor("joined.mp4"), joinedLocal, "video/mp4");
        checkpoint.joined_url = joinedUrl;
        await persistCheckpoint();
      } catch (e) {
        console.warn(`[assemble] joined.mp4 checkpoint upload failed:`, e);
      }
    }

    // ── Stage D: optional BGM mix ───────────────────────────────────────
    //
    // joined.mp4 already has voiceover baked into its audio track
    // (each beat carries its own voiceover, concatenated transparently
    // by the -c copy concat step). When bgm is enabled, mix it under
    // the existing audio in a single ffmpeg pass with -c:v copy. When
    // not, joined.mp4 IS the final output and we just point finalPath
    // at it.
    const outputPath = path.join(tmpDir, "output.mp4");
    let finalPath: string;
    const joinedDisk = path.join(tmpDir, "joined.mp4");
    if (!fs.existsSync(joinedDisk)) {
      await checkStop();
      await progress("Restoring joined video…");
      await downloadFile(checkpoint.joined_url!, joinedDisk, signal);
    }

    if (backgroundMusicUrl) {
      await checkStop();
      await progress("Downloading background music…");
      const bgmLocalPath = path.join(tmpDir, "bgm.mp3");
      let bgmConfig: { volume: number; loops: number } | null = null;
      try {
        await downloadFile(backgroundMusicUrl, bgmLocalPath, signal);
        const bgmDuration = await getMediaDuration(bgmLocalPath);
        if (!isFinite(bgmDuration) || bgmDuration <= 0.5) {
          throw new Error(`bgm duration unreasonable: ${bgmDuration}`);
        }
        let bgmVolume = backgroundMusicVolume ?? 0.15;
        if (bgmVolume < 0) bgmVolume = 0;
        if (bgmVolume > 1) bgmVolume = 1;
        const rawLoops = Math.ceil(totalDuration / bgmDuration) + 1;
        const loops = Math.min(10_000, Math.max(1, rawLoops));
        bgmConfig = { volume: bgmVolume, loops };
        console.log(`[assemble] ${projectId}: bgm duration=${bgmDuration.toFixed(2)}s, voiceover=${totalDuration.toFixed(2)}s → ${loops} loops`);
      } catch (e) {
        if (signal.aborted) throw e;
        console.warn(`[assemble] bgm setup failed, continuing without music:`, e instanceof Error ? e.message : e);
        bgmConfig = null;
      }

      if (bgmConfig) {
        await progress("Mixing background music…");
        try {
          await addBgmToVideo(joinedDisk, bgmLocalPath, bgmConfig.volume, bgmConfig.loops, outputPath, totalDuration, signal);
          finalPath = outputPath;
        } catch (e) {
          if (signal.aborted) throw e;
          console.warn(`[assemble] bgm mix failed, shipping voiceover-only:`, e instanceof Error ? e.message : e);
          finalPath = joinedDisk;
        }
        try { fs.unlinkSync(bgmLocalPath); } catch { /* ignore */ }
      } else {
        finalPath = joinedDisk;
      }
    } else {
      finalPath = joinedDisk;
    }
    metrics.record("bgm-mix");
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

    metrics.record("upload-done");
    const metricsSnapshot = metrics.snapshot();
    console.log(`[assemble] ${projectId}: metrics peak_rss=${metricsSnapshot.peak_rss_mb}MB stages=${metricsSnapshot.stages.length}`);

    // Successful completion — clear the checkpoint. (R2 _assembly/ stage
    // objects are left behind; they're small and overwritten by the
    // next run, or cleaned up by the project-delete folder sweep.)
    // assembly_preview_url is cleared so a stale mixed.mp4 link from
    // mid-run doesn't show alongside the finished video.
    await supabase.from("projects")
      .update({
        assembly_status: "done",
        assembled_url: publicUrl,
        assembly_progress: null,
        assembly_error: null,
        assembly_checkpoint: null,
        assembly_stop_requested: false,
        assembly_finalize_preview_requested: false,
        assembly_preview_url: null,
        assembly_finished_at: new Date().toISOString(),
        assembly_metrics: metricsSnapshot,
        current_state: 15,
      })
      .eq("id", projectId);

    console.log(`[assemble] ${projectId}: done → ${publicUrl}`);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Assembly failed";
    // Capture metrics at the failure point too — peak RSS on a
    // crashed run is the single most useful data point for chasing
    // the next OOM. Stamp it on both stop and failure transitions.
    const terminalLabel = message === STOPPED_MARKER ? "stopped" : message === FINALIZE_PREVIEW_MARKER ? "finalize-preview" : "failed";
    metrics.record(terminalLabel);
    const metricsSnapshot = metrics.snapshot();
    console.log(`[assemble] ${projectId}: metrics (terminal) peak_rss=${metricsSnapshot.peak_rss_mb}MB stages=${metricsSnapshot.stages.length}`);
    if (message === FINALIZE_PREVIEW_MARKER) {
      // User clicked "Use this version" — the worker just STOPS here.
      // The actual promotion of assembly_preview_url → assembled_url
      // happens client-side via PATCH {commit_preview: true} when the
      // user clicks Continue. Two-step confirm gives them a moment to
      // verify the preview is what they want before locking it in.
      //
      // What's preserved (vs. a normal Stop):
      //   - assembly_finalize_preview_requested stays TRUE so the UI
      //     can show Continue (instead of Resume) on the stopped
      //     panel. The flag IS the signal to the front-end.
      //   - assembly_preview_url stays set — Continue needs it.
      //   - assembly_checkpoint preserved in case the user changes
      //     their mind and Cancels: cancel_assembly does its own
      //     cleanup of the _assembly/ R2 folder including mixed.mp4.
      console.log(`[assemble] ${projectId}: finalize-preview requested — stopping for user confirmation`);
      await supabase.from("projects")
        .update({
          assembly_status: "stopped",
          assembly_progress: "Stopped — preview ready, click Continue to use it",
          assembly_error: null,
          assembly_stop_requested: false,
          assembly_finished_at: new Date().toISOString(),
          assembly_metrics: metricsSnapshot,
        })
        .eq("id", projectId);
    } else if (message === STOPPED_MARKER) {
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
          assembly_finalize_preview_requested: false,
          assembly_preview_url: null,
          assembly_finished_at: new Date().toISOString(),
          assembly_metrics: metricsSnapshot,
        })
        .eq("id", projectId);
    } else {
      console.error(`[assemble] ${projectId} failed:`, message);
      previewFiles.delete(projectId);
      try { fs.unlinkSync(path.join(PREVIEW_DIR, `${projectId}.mp4`)); } catch { /* ignore */ }
      await supabase.from("projects")
        .update({
          assembly_status: "failed",
          assembly_error: message,
          assembly_progress: null,
          assembly_finalize_preview_requested: false,
          assembly_preview_url: null,
          assembly_finished_at: new Date().toISOString(),
          assembly_metrics: metricsSnapshot,
        })
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

// Admin-tunable concurrency for the assembly worker. Read from
// product_config.batched_processes every ~30s so the dashboard can
// raise/lower these without restarting the worker.
//   assemblyProjectLimit  → how many whole assemblies run in parallel
//   assemblyBeatLimit     → parallel beats per assembly (Stage B) —
//                           consumed inside runAssembly via getAssemblyBeatLimit()
let assemblyProjectLimit = 1;
let assemblyBeatLimit = 1;

export function getAssemblyBeatLimit(): number {
  return assemblyBeatLimit;
}

async function refreshAssemblyConcurrency(): Promise<void> {
  try {
    const { data } = await supabase
      .from("product_config")
      .select("batched_processes")
      .eq("service", "_global")
      .single();
    const cfg = (data as { batched_processes?: { assembly_projects?: unknown; assembly_beats?: unknown } } | null)?.batched_processes;
    const projRaw = cfg?.assembly_projects;
    const beatRaw = cfg?.assembly_beats;
    const proj = typeof projRaw === "number" ? projRaw : Number(projRaw);
    const beat = typeof beatRaw === "number" ? beatRaw : Number(beatRaw);
    if (Number.isInteger(proj) && proj >= 1 && proj <= 5 && proj !== assemblyProjectLimit) {
      console.log(`[assembly-queue] project limit changed: ${assemblyProjectLimit} → ${proj}`);
      assemblyProjectLimit = proj;
    }
    if (Number.isInteger(beat) && beat >= 1 && beat <= 10 && beat !== assemblyBeatLimit) {
      console.log(`[assembly-queue] beat limit changed: ${assemblyBeatLimit} → ${beat}`);
      assemblyBeatLimit = beat;
    }
  } catch (err) {
    console.warn("[assembly-queue] Failed to refresh concurrency from product_config:", err instanceof Error ? err.message : err);
  }
}

async function assemblyPollLoop() {
  await refreshAssemblyConcurrency();
  console.log(`[assembly-queue] poll loop started (projects=${assemblyProjectLimit}, beats=${assemblyBeatLimit})`);
  let ticks = 0;
  while (true) {
    try {
      // Re-read admin knobs every ~30s (6 ticks × 5s) so changes propagate
      // without a worker restart.
      if (ticks++ % 6 === 0) await refreshAssemblyConcurrency();

      const slots = assemblyProjectLimit - assemblingProjects.size;
      if (slots > 0) {
        const { data: rows } = await supabase
          .from("projects")
          .select("id, user_id")
          .eq("assembly_status", "queued")
          .limit(slots);

        for (const row of rows ?? []) {
          const projectId = row.id as string;
          const userId = row.user_id as string;

          // Skip if it's already in flight (race between fetch + claim).
          if (assemblingProjects.has(projectId)) continue;

          // Atomic claim. assembly_started_at is stamped here so the
          // duration we compute later reflects wall-clock from worker
          // pickup — not from the user's click (which sat in the
          // queue waiting for a free slot). assembly_finished_at is
          // cleared so a Resume after a Stop doesn't carry a stale
          // finish stamp from the prior attempt.
          const { data: claimed } = await supabase
            .from("projects")
            .update({
              assembly_status: "processing",
              assembly_progress: "Starting…",
              assembly_started_at: new Date().toISOString(),
              assembly_finished_at: null,
            })
            .eq("id", projectId)
            .eq("assembly_status", "queued")
            .select("id")
            .single();

          if (!claimed) continue;

          const opts = (await redis.get(`assembly:${projectId}`) as Record<string, unknown> | null) ?? {};
          console.log(`[assembly-queue] ${projectId}: opts from redis = bgm=${JSON.stringify(opts.backgroundMusicUrl)} vol=${JSON.stringify(opts.backgroundMusicVolume)} logo=${JSON.stringify(opts.logoUrl)} logoXY=${JSON.stringify(opts.logoX)},${JSON.stringify(opts.logoY)} logoSize=${JSON.stringify(opts.logoSize)} keys=[${Object.keys(opts).join(",")}]`);

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
            trimSilenceEnabled: (opts.trimSilenceEnabled as boolean | undefined) ?? false,
            backgroundMusicUrl: (opts.backgroundMusicUrl as string | undefined) ?? null,
            backgroundMusicVolume: (opts.backgroundMusicVolume as number | undefined) ?? 0.15,
            resolution: (opts.resolution as ResolutionPreset | undefined) ?? "1080p",
            logoUrl: (opts.logoUrl as string | undefined) ?? null,
            logoX: (opts.logoX as number | undefined) ?? 0.85,
            logoY: (opts.logoY as number | undefined) ?? 0.05,
            logoSize: (opts.logoSize as number | undefined) ?? 0.1,
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
      trimSilenceEnabled = false,
      backgroundMusicUrl = null,
      backgroundMusicVolume = 0.15,
      resolution = "1080p",
      logoUrl = null,
      logoX = 0.85,
      logoY = 0.05,
      logoSize = 0.1,
    } = req.body as {
      token: string; projectId: string; aspectRatio?: string; voiceoverType?: "cleaned" | "original";
      captionsEnabled?: boolean; captionsLanguage?: string;
      captionsStyle?: string; captionsSize?: string; captionsPosition?: string;
      trimSilenceEnabled?: boolean;
      backgroundMusicUrl?: string | null;
      backgroundMusicVolume?: number;
      resolution?: ResolutionPreset;
      logoUrl?: string | null;
      logoX?: number;
      logoY?: number;
      logoSize?: number;
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

    // Mark as processing immediately so the client sees state change on next poll.
    // assembly_started_at / _finished_at maintained the same way as the
    // queue claim site for consistent analytics across both entry paths.
    await supabase.from("projects")
      .update({
        assembly_status: "processing",
        assembly_progress: "Starting…",
        assembly_error: null,
        assembly_started_at: new Date().toISOString(),
        assembly_finished_at: null,
      })
      .eq("id", projectId).eq("user_id", user.id);

    assemblingProjects.add(projectId);

    // Fire and forget — no SSE, no long-lived connection
    runAssembly({ userId: user.id, projectId, aspectRatio, voiceoverType: voiceoverType as "cleaned" | "original",
      captionsEnabled: Boolean(captionsEnabled), captionsLanguage, captionsStyle, captionsSize, captionsPosition,
      trimSilenceEnabled: Boolean(trimSilenceEnabled),
      backgroundMusicUrl,
      backgroundMusicVolume: typeof backgroundMusicVolume === "number" ? backgroundMusicVolume : 0.15,
      resolution,
      logoUrl,
      logoX: typeof logoX === "number" ? logoX : 0.85,
      logoY: typeof logoY === "number" ? logoY : 0.05,
      logoSize: typeof logoSize === "number" ? logoSize : 0.1,
    }).catch(console.error).finally(() => assemblingProjects.delete(projectId));

    res.json({ started: true });
  });
}
