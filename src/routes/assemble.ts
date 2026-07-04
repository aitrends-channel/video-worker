import { createRequire } from "module";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { type Express, type Request, type Response } from "express";
import { supabase } from "../lib/supabase.js";
import { uploadFile, userFolderForId, listKeysWithPrefix } from "../lib/storage.js";
import { redis } from "../lib/queue.js";
import { logProjectCost } from "../lib/costs.js";
import Anthropic from "@anthropic-ai/sdk";
import { getAnthropicClient } from "../lib/anthropic.js";
import fs from "fs";
import { openAsBlob } from "node:fs";
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

// Content-addressed lookup against the R2 public URL. Used by the
// per-beat encode cache to skip re-encoding when the same input set
// has already produced an output. Returns true when the object
// exists, false otherwise — never throws, so a cache miss falls
// through to a fresh encode instead of failing the whole assembly.
async function r2ObjectExists(publicUrl: string): Promise<boolean> {
  try {
    const res = await fetch(publicUrl, { method: "HEAD", signal: AbortSignal.timeout(10_000) });
    return res.ok;
  } catch {
    return false;
  }
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
    // -loglevel error: ffmpeg buffers all stderr/stdout until the
    // subprocess exits. Default 'info' emits ~1 progress line per
    // second per encode — small per beat, but cumulative across a
    // 138+ beat run. 'error' silences progress while still surfacing
    // genuine failures via the .on('error') callback below.
    const cmd = build(
      ffmpeg()
        .addOption("-hide_banner")
        .addOption("-loglevel", "error")
        .addOption("-threads", String(threadCount))
        .addOption("-filter_threads", String(threadCount)),
    );
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
  // Captions OR logo baked in here means Stage D will -c:v copy this
  // output straight into the final mp4 — match the old burn-pass
  // quality (veryfast/crf 23). With neither, the clip is intermediate
  // and ultrafast/crf 28 is fine.
  const isFinalQualityPass = !!subtitles || !!logoOverlay;
  const preset = isFinalQualityPass ? "veryfast" : "ultrafast";
  const crf = isFinalQualityPass ? "23" : "28";
  // libx264 stillimage tune skips motion-search and inter-frame
  // adaptive logic the encoder would otherwise spend cycles on for a
  // static (image) source. Pure speedup, no quality loss for the
  // single-frame-looped path.
  const tuneOpts: string[] = isImage ? ["-tune", "stillimage"] : [];

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
      // [0:v:0] explicitly picks the FIRST video stream — AI-generated
      // sources sometimes carry an attached_pic / cover-art stream that
      // [0:v] would also match, causing the mp4 muxer to fail late in
      // the encode after several minutes of CPU.
      const graph: string[] = [
        `[0:v:0]${baseFilter}[base]`,
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
          ...tuneOpts,
          // x264 memory tuning: default rc-lookahead is 40 frames held
          // resident in the encoder's reference pool — at 1080p that's
          // ~50-100 MB per ffmpeg process. Cutting to 5 still gives the
          // rate controller enough lookahead for the short per-beat
          // segments we encode here. scenecut=0 disables I-frame
          // insertion-on-scene-change (we re-encode anyway, so the
          // adaptive logic just adds buffer pressure). -bufsize caps
          // the rate-control buffer at 1 MB.
          "-x264-params", "rc-lookahead=5:scenecut=0",
          "-bufsize", "1M",
          "-an", "-pix_fmt", "yuv420p",
        ]);
    } else {
      const vf = assFilter ? `${baseFilter},${assFilter}` : baseFilter;
      cmd.outputOptions([
        "-t", String(duration),
        "-vf", vf,
        "-c:v", "libx264", "-preset", preset, "-crf", crf,
        ...tuneOpts,
        "-x264-params", "rc-lookahead=5:scenecut=0",
        "-bufsize", "1M",
        "-an", "-pix_fmt", "yuv420p",
      ]);
    }

    return cmd.output(output);
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

function mixAudio(
  video: string,
  audio: string,
  output: string,
  videoDuration: number,
  videoWidth: number,
  videoHeight: number,
  signal?: AbortSignal,
  bgm?: { path: string; volume: number; loops: number } | null,
  logo?: LogoOverlay | null,
): Promise<void> {
  return ffmpegWithTimeout((cmd) => {
    // probesize + analyzeduration cap how much of each input ffmpeg
    // pre-reads to detect codec/timing. Mixed.mp4 and the trimmed
    // voiceover are all known-format outputs from upstream ffmpeg
    // passes — the default 5MB / 5s probe is overhead. Cap aggressively
    // so the input pipeline doesn't sit on big read-ahead buffers.
    const lightInput = ["-thread_queue_size", "512", "-probesize", "1000000", "-analyzeduration", "0"];
    cmd.input(video).inputOptions([...lightInput, "-fflags", "+genpts"]);
    cmd.input(audio).inputOptions(lightInput);

    // Audio graph — unchanged from before. amix when BGM present,
    // straight passthrough otherwise.
    const audioFilters: string[] = [];
    let audioMap: string;
    if (bgm) {
      audioFilters.push(`[2:a]volume=${bgm.volume}[bgmDucked]`);
      audioFilters.push(`[1:a][bgmDucked]amix=inputs=2:duration=first:dropout_transition=0[mix]`);
      audioMap = "[mix]";
    } else {
      audioMap = "1:a";
    }
    if (bgm) {
      cmd.input(bgm.path).inputOptions([...lightInput, "-stream_loop", String(bgm.loops)]);
    }

    // Logo overlay (when present) shifts the video stream from
    // -c:v copy to a libx264 re-encode. We accept the wall-clock
    // cost because it's the only consistent place to apply the
    // overlay regardless of useFinalBurn / useCoconut /
    // assembly_beats_at_final_res — keeping logo handling in one
    // stage matters more than the per-run CPU saving from the
    // copy path. The logo input index depends on whether BGM is
    // present (BGM is 2, logo follows it; without BGM, logo is 2).
    let videoMap: string;
    if (logo) {
      const logoInputIdx = bgm ? 3 : 2;
      cmd.input(logo.logoPath);
      const logoW = Math.max(8, Math.min(videoWidth, Math.round((videoWidth * logo.sizePct) / 2) * 2));
      const x = Math.round(videoWidth * logo.xPct);
      const y = Math.round(videoHeight * logo.yPct);
      const videoFilters = [
        `[${logoInputIdx}:v]scale=w=${logoW}:h=-2[logo]`,
        `[0:v:0][logo]overlay=x=${x}:y=${y}:format=auto[v]`,
      ];
      cmd.complexFilter([...audioFilters, ...videoFilters]);
      videoMap = "[v]";
    } else {
      if (audioFilters.length > 0) cmd.complexFilter(audioFilters);
      videoMap = "0:v";
    }

    const videoEncodeFlags = logo
      ? ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p"]
      : ["-c:v", "copy"];

    cmd.outputOptions([
      "-map", videoMap, "-map", audioMap,
      ...videoEncodeFlags,
      "-c:a", "aac", "-b:a", "128k",
      "-t", String(videoDuration),
      ...(bgm ? ["-shortest"] : []),
      "-max_muxing_queue_size", "1024",
    ]);
    // +faststart skipped: on constrained disk it can corrupt the moov atom; range-request serving handles moov-at-end fine
    return cmd.output(output);
  }, "audio mix", signal);
}

// ── Transcription ─────────────────────────────────────────────────────────────

interface TranscriptionWord {
  text?: string; word?: string;
  start?: number; start_time?: number;
  end?: number; end_time?: number;
  type?: string;
}

async function transcribeAudio(audioPath: string, apiKey: string, signal?: AbortSignal): Promise<TranscriptionWord[]> {
  // openAsBlob returns a Blob whose .stream() reads from disk on demand —
  // the bytes never sit in JS heap. Previously fs.readFileSync loaded the
  // entire voiceover (~30 MB for long projects) into memory and a Blob
  // copy retained it across all 4 retry attempts.
  const audioBlob = await openAsBlob(audioPath, { type: "audio/mpeg" });
  const MAX_ATTEMPTS = 4;
  let lastError = "";
  try {
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      if (signal?.aborted) throw new Error(STOPPED_MARKER);
      const formData = new FormData();
      formData.append("file", audioBlob, "voiceover.mp3");
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
  // True when Stage B's per-beat normalizeClip pass composited the
  // channel logo. Mirrors clips_baked_captions but for the logo overlay
  // layer. When true, Stage D drops the logo work and stays on -c:v
  // copy. Old checkpoints (pre-bake-logo migration) have this undefined
  // → falsy → Stage D applies the logo overlay as before so resumed
  // legacy projects still get a logo.
  clips_baked_logo?: boolean;
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

function getAssemblyConcurrency(
  resolution: ResolutionPreset | undefined,
  allImages: boolean,
  beats: number,
  captionsEnabled: boolean,
): number {
  if (ASSEMBLY_SAFE_MODE) return 1;
  const slider = Math.max(1, getAssemblyBeatLimit());
  const res = (resolution ?? "1080p") as AssemblyResolution;

  // Admin's per-scenario rule wins if one matches. Rule is still
  // bounded by the global slider — `assembly_beats` is the absolute
  // ceiling. No resolution safety cap when a rule matches: the admin
  // explicitly chose this value for this scenario.
  const matched = matchAssemblyBeatRule({ resolution: res, beats, allImages, captionsEnabled });
  if (matched) {
    return Math.min(slider, matched.value);
  }

  // No rule matched — fall back to slider capped by resolution-aware
  // safety floor. Image-only beats use ~half the RSS of a video
  // transcode at the same resolution, so we allow more parallelism.
  // 4K is floored at 2 so even 2160p runs keep some parallelism.
  const videoCap = res === "2160p" ? 2 : res === "1440p" ? 1 : res === "1080p" ? 2 : slider;
  const imageCap = res === "2160p" ? 2 : res === "1440p" ? 2 : res === "1080p" ? 4 : slider;
  const memCap = allImages ? imageCap : videoCap;
  return Math.min(slider, memCap);
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

    // ── Mode selection ─────────────────────────────────────────────────
    //
    // Per-beat mode: ANY beat carries its own voiceover_url. Each
    // beat's clip duration = its audio file's duration. No STT for
    // alignment, no alignBeats matcher — there's nothing to align
    // because the boundaries ARE the audio files. We assemble the
    // subset of beats that have BOTH a visual and a voiceover. Beats
    // without a voiceover are dropped, so partial generation runs
    // (e.g. dev word cap, stopped mid-batch) produce a shorter video
    // covering only the available audio instead of failing.
    //
    // Legacy mode: the project has a single voiceover (tts_url or
    // tts_cleaned_url) and zero per-beat audio. The assembler runs
    // STT + the predictive matcher to figure out per-beat durations,
    // as before. Kept intact for backward compatibility with projects
    // assembled before migration 045 / the per-beat voiceover feature.
    const anyVoiceover = allBeats.some((b) => !!b.voiceover_url);
    const perBeatMode = anyVoiceover;

    // Filter beats per the chosen mode. In per-beat mode we require
    // BOTH a visual and a voiceover; in legacy mode just a visual.
    const beats = perBeatMode
      ? allBeats.filter((b) => (b.video_url || b.image_url) && b.voiceover_url)
      : allBeats.filter((b) => b.video_url || b.image_url);
    if (!beats.length) {
      throw new Error(perBeatMode
        ? "No beats have both a visual and a voiceover yet — generate at least one beat's voiceover and visual first."
        : "No images or video clips found — generate images on the Generate page first.");
    }
    const videoCount = beats.filter((b) => b.video_url).length;
    const droppedNoVoiceover = perBeatMode
      ? allBeats.filter((b) => (b.video_url || b.image_url) && !b.voiceover_url).length
      : 0;
    console.log(`[assemble] ${projectId}: mode=${perBeatMode ? "per-beat" : "legacy single-voiceover"}, assembling ${beats.length}/${allBeats.length} beats (${videoCount} video, ${beats.length - videoCount} image${droppedNoVoiceover > 0 ? `, ${droppedNoVoiceover} dropped: no voiceover` : ""})`);

    // Decide intermediate vs final resolution and whether the captions/logo
    // burn pass runs at the end. Done HERE (right after beats are loaded)
    // so the per-beat encode pool can downscale intermediate clips when
    // we know a final-burn upscale will happen anyway — saves ~2.25× per
    // 1080p clip on the >80 beat path. For the per-beat-bake path
    // (default), intermediate dims === final dims so quality is unchanged.
    // Per-beat encodes run at the user's final resolution directly.
    // The intermediate-720p-plus-final-burn-upscale pattern existed
    // to offload the upscale step to Coconut; Coconut was removed
    // (it never accepted our captioned spec — see git log around
    // 2026-06-30). With Coconut gone there's no reason to encode
    // small and upscale later. Captions bake per-beat (Stage B),
    // logo composites at the audio mix (Stage D), Stage F is just
    // remux + upload.
    // Decided here (not at Stage F) because the Stage B logo gate
    // below needs to know whether the final-burn pass will be
    // outsourced. When Coconut handles Stage F, we bake the logo
    // per-beat in Stage B instead of deferring it to Coconut —
    // simpler Coconut spec (no watermark) + one less thing that
    // can break upstream. When the local fallback runs Stage F or
    // useFinalBurn is false, the existing per-beat-bake path
    // handles it. Logo in Stage F is now only used when Coconut
    // is unavailable AND captions are enabled (single-pass final
    // burn covers both).
    const [w, h] = [finalW, finalH];

    metrics.record("load-project");

    const voiceoverPath = path.join(tmpDir, "voiceover.mp3");
    let totalDuration = 0;
    let durations: number[] = [];
    let transcriptionWords: TranscriptionWord[] = [];
    // Background audio assembly promise. Used by the canUseRemoteAudioConcat
    // path when captions are off — durations are known immediately from
    // voiceover_duration_ms, so we kick off the audio concat ffmpeg in
    // the background and let Stage B's video encodes begin in parallel.
    // For captions-on or non-fast paths this stays resolved synchronously
    // (no behavior change). Awaited before Stage D's mix step.
    let audioReadyPromise: Promise<void> = Promise.resolve();
    let audioRunsInBackground = false;

    if (perBeatMode) {
      // ── PER-BEAT PATH ────────────────────────────────────────────────
      //
      // Three previous sequential loops (download → trim → measure)
      // collapsed into one worker pool that processes each beat
      // end-to-end. Same admin-tunable concurrency knob the visuals
      // pool uses (assembly_beats from product_config), so the audio
      // and visuals stages scale together when ops dials it up.
      //
      // Output arrays are pre-allocated and indexed by beat position
      // so workers can't race — `audioPaths[i]` and
      // `measuredDurations[i]` are written exactly once each. The
      // concat list below still reads in beat order, unchanged.
      //
      // Optional per-beat silence trim. Off by default — only runs
      // when the user clicked "Trim silences" on the assemble page,
      // which sets trimSilenceEnabled=true. Each render normally ships
      // with ~150-300ms of dead air at start/end; trimming removes
      // those pads so the concat doesn't accumulate inter-beat pauses.
      // Internal pauses inside each clip are preserved either way.
      //
      // Even when trim is off we still need a per-beat duration to
      // anchor the visual timeline. With trim on, the persisted
      // voiceover_duration_ms is wrong (it describes untrimmed audio),
      // so re-measure via ffprobe on the trimmed file. With trim off,
      // prefer the cached value when present to save the ffprobe call.
      await checkStop();
      await progress(`Preparing ${beats.length} beat voiceovers…`);
      const audioPaths: string[] = new Array(beats.length).fill("");
      const measuredDurations: number[] = new Array(beats.length).fill(0);

      const canUseRemoteAudioConcat = !trimSilenceEnabled && beats.every((beat) => {
        const dur = beat.voiceover_duration_ms ?? 0;
        return typeof dur === "number" && dur > 0 && typeof beat.voiceover_url === "string" && beat.voiceover_url.length > 0;
      });

      let didRemoteAudioConcat = false;
      if (canUseRemoteAudioConcat) {
        await checkStop();
        await progress("Preparing beat audio timeline…");
        durations = beats.map((beat) => Math.max(0.1, (beat.voiceover_duration_ms ?? 0) / 1000));
        totalDuration = durations.reduce((sum, d) => sum + d, 0);
        console.log(`[assemble] ${projectId}: using remote beat URLs for audio concat (sum=${totalDuration.toFixed(2)}s, trim=off)`);

        const audioListPath = path.join(tmpDir, "audio_concat.txt");
        fs.writeFileSync(audioListPath, beats.map((beat) => escapeConcatListEntry(beat.voiceover_url!)).join("\n"));

        if (!captionsEnabled) {
          // Captions OFF: durations are already set from voiceover_duration_ms
          // and Stage B doesn't need voiceoverPath. Launch the audio concat
          // ffmpeg in the background so Stage B's video encodes can begin in
          // parallel with the (often 30-90s) concat job. Awaited just before
          // Stage D's mix.
          await checkStop();
          await progress("Joining per-beat audio in background…");
          audioRunsInBackground = true;
          didRemoteAudioConcat = true;
          audioReadyPromise = (async () => {
            try {
              await concatClips(audioListPath, voiceoverPath, signal);
            } catch (e) {
              if (signal.aborted) throw e;
              console.warn(`[assemble] ${projectId}: background remote audio concat failed, falling back to local download/concat:`, e instanceof Error ? e.message : e);
              // Inline minimal local fallback — durations are already
              // correct (from voiceover_duration_ms); we just need to
              // produce voiceoverPath from a serially-downloaded copy.
              const localPaths: string[] = [];
              for (let i = 0; i < beats.length; i++) {
                if (signal.aborted) throw new Error(STOPPED_MARKER);
                const localPath = path.join(tmpDir, `audio_raw_${String(i).padStart(3, "0")}.mp3`);
                await downloadFile(beats[i].voiceover_url!, localPath, signal);
                localPaths.push(localPath);
              }
              const localListPath = path.join(tmpDir, "audio_local_concat.txt");
              fs.writeFileSync(localListPath, localPaths.map((p) => escapeConcatListEntry(p)).join("\n"));
              await concatClips(localListPath, voiceoverPath, signal);
              for (const p of localPaths) { try { fs.unlinkSync(p); } catch { /* ignore */ } }
            }
          })();
        } else {
          // Captions ON: keep serial behavior. STT runs on voiceoverPath
          // right after this, and Stage B's per-beat caption slice depends
          // on its result — parallelizing here would force a Stage B await
          // anyway, eliminating the win and complicating the code.
          await checkStop();
          await progress("Joining per-beat audio…");
          try {
            await concatClips(audioListPath, voiceoverPath, signal);
            didRemoteAudioConcat = true;
          } catch (e) {
            console.warn(`[assemble] ${projectId}: remote audio concat failed, falling back to local download/concat:`, e instanceof Error ? e.message : e);
          }
        }
      }
      if (!didRemoteAudioConcat) {
        // Audio prep is light per-process — small mp3 download +
        // optional silenceremove ffmpeg + ffprobe duration. Each
        // ffmpeg holds maybe 50-100 MB resident. Decouple from the
        // visual encode pool (which had to cap at 1 for >80 beats
        // because of 4K frame buffers), and run up to 6 audio
        // workers in parallel. For a 200-beat trim-on project this
        // drops audio prep from ~7-10 min sequential down to ~1-2
        // min while staying well under the 2 GB Standard ceiling.
        const audioLimit = ASSEMBLY_SAFE_MODE ? 1 : Math.min(beats.length, 6);
        let nextAudioIdx = 0;
        let audioCompleted = 0;
        let audioFirstError: Error | null = null;

        const processOneAudio = async (i: number): Promise<void> => {
          await checkStop();
          const beat = beats[i];
          const rawPath = path.join(tmpDir, `audio_raw_${String(i).padStart(3, "0")}.mp3`);
          await downloadFile(beat.voiceover_url!, rawPath, signal);
          if (trimSilenceEnabled) {
            const trimmed = path.join(tmpDir, `audio_${String(i).padStart(3, "0")}.mp3`);
            await trimSilence(rawPath, trimmed, signal);
            const dur = await getMediaDuration(trimmed);
            audioPaths[i] = trimmed;
            measuredDurations[i] = Math.max(0.1, dur);
            try { fs.unlinkSync(rawPath); } catch { /* ignore */ }
          } else {
            let dur = beat.voiceover_duration_ms ? beat.voiceover_duration_ms / 1000 : 0;
            if (!dur || dur <= 0) dur = await getMediaDuration(rawPath);
            audioPaths[i] = rawPath;
            measuredDurations[i] = Math.max(0.1, dur);
          }
        };

        const audioWorker = async (): Promise<void> => {
          while (true) {
            if (audioFirstError) return;
            const i = nextAudioIdx++;
            if (i >= beats.length) return;
            try {
              await processOneAudio(i);
            } catch (e) {
              // Unlike the visuals pool, an audio failure can't be
              // silently skipped — a missing beat's audio leaves the
              // master voiceover with a gap and throws off every
              // downstream duration. Bubble the first error and let
              // the other workers wind down on the next iteration.
              if (!audioFirstError) audioFirstError = e instanceof Error ? e : new Error(String(e));
              return;
            }
            audioCompleted++;
            if (beats.length > 0) {
              await progressThrottled(`Prepared ${audioCompleted} of ${beats.length} beat voiceovers…`);
            }
          }
        };

        await Promise.all(Array.from({ length: audioLimit }, () => audioWorker()));
        if (audioFirstError) throw audioFirstError;

        durations = measuredDurations;
        totalDuration = durations.reduce((sum, d) => sum + d, 0);
        console.log(`[assemble] ${projectId}: per-beat durations sum=${totalDuration.toFixed(2)}s (${durations.length} beats, trim=${trimSilenceEnabled ? "on" : "off"})`);

        // Concatenate per-beat audio into one track for the mix step.
        // All trimmed files share codec/sample rate (mp3 128k 44.1kHz
        // from trimSilence), so `-c copy` works cleanly — no re-encode at
        // the join step means no audible artifacts at boundaries.
        await checkStop();
        await progress("Joining per-beat audio…");
        const audioListPath = path.join(tmpDir, "audio_concat.txt");
        fs.writeFileSync(audioListPath, audioPaths.map((p) => escapeConcatListEntry(p)).join("\n"));
        await concatClips(audioListPath, voiceoverPath, signal);
        for (const p of audioPaths) { try { fs.unlinkSync(p); } catch { /* ignore */ } }
      }

      // Captions need word-level timestamps even in per-beat mode.
      // Run STT on the concatenated audio so caption timings line up
      // with the final mixed track.
      if (captionsEnabled) {
        transcriptionWords = checkpoint.transcription_words ?? [];
        if (!transcriptionWords.length) {
          await checkStop();
          await progress("Transcribing for captions…");
          try {
            const { elevenlabs_api_key } = await getSettings(userId);
            if (!elevenlabs_api_key) throw new Error("ElevenLabs API key not configured.");
            transcriptionWords = await transcribeAudio(voiceoverPath, elevenlabs_api_key, signal);
          } catch (e) {
            console.warn("[assemble] per-beat caption transcription failed:", e);
          }
          if (transcriptionWords.length) {
            checkpoint.transcription_words = transcriptionWords;
            await persistCheckpoint();
            // Log the cost — sum of characters across all returned
            // words. ElevenLabs Scribe is the only real upstream
            // charge in the assembler; ffmpeg work runs on our own
            // Render box. Same approach in the legacy path below.
            const chars = transcriptionWords.reduce(
              (sum, w) => sum + (w.text ?? w.word ?? "").length,
              0,
            );
            void logProjectCost({
              projectId,
              userId,
              step: "assemble",
              provider: "elevenlabs",
              model: "scribe_v1",
              units: chars,
              unitKind: "elevenlabs_chars",
            });
          }
        }
      }
      metrics.record("per-beat-audio");
    } else {
      // ── LEGACY SINGLE-VOICEOVER PATH ─────────────────────────────────
      const legacyVoiceoverUrl = voiceoverType === "original" ? (proj.tts_url ?? proj.tts_cleaned_url) : (proj.tts_cleaned_url ?? proj.tts_url);
      if (!legacyVoiceoverUrl) throw new Error("No voiceover found — either generate a per-beat voiceover on the Voiceover step OR a single voiceover on the legacy Generate flow.");

      await checkStop();
      await progress("Downloading voiceover…");
      await downloadFile(legacyVoiceoverUrl, voiceoverPath, signal);
      totalDuration = await getMediaDuration(voiceoverPath);
      if (totalDuration <= 0) throw new Error("Could not determine voiceover duration");
      console.log(`[assemble] ${projectId}: voiceover duration = ${totalDuration.toFixed(2)}s`);

      // Stage A (legacy): STT + alignBeats with hash-based caching.
      const voiceoverHash = createHash("sha256").update(legacyVoiceoverUrl).digest("hex");
      const allBeatsHaveDuration = beats.every((b) => typeof b.duration_ms === "number" && (b.duration_ms ?? 0) > 0);
      const hashMatches = proj.beat_timings_voiceover_hash === voiceoverHash;
      const canUseStoredDurations = allBeatsHaveDuration && hashMatches;
      const needSttForCaptions = captionsEnabled;
      const needSttForAlignment = !canUseStoredDurations;

      transcriptionWords = checkpoint.transcription_words ?? [];
      if (transcriptionWords.length) {
        console.log(`[assemble] ${projectId}: transcription loaded from checkpoint (${transcriptionWords.length} words)`);
      } else if (needSttForAlignment || needSttForCaptions) {
        await checkStop();
        await progress("Transcribing voiceover…");
        try {
          const { elevenlabs_api_key } = await getSettings(userId);
          if (!elevenlabs_api_key) throw new Error("ElevenLabs API key not configured.");
          transcriptionWords = await transcribeAudio(voiceoverPath, elevenlabs_api_key, signal);
        } catch (e) {
          console.warn("[assemble] transcription failed, using proportional fallback:", e);
        }
        if (transcriptionWords.length) {
          checkpoint.transcription_words = transcriptionWords;
          await persistCheckpoint();
          // Same cost log as the per-beat path above.
          const chars = transcriptionWords.reduce(
            (sum, w) => sum + (w.text ?? w.word ?? "").length,
            0,
          );
          void logProjectCost({
            projectId,
            userId,
            step: "assemble",
            provider: "elevenlabs",
            model: "scribe_v1",
            units: chars,
            unitKind: "elevenlabs_chars",
          });
        }
      } else {
        console.log(`[assemble] ${projectId}: skipping STT — beats already aligned and captions disabled`);
      }
      if (transcriptionWords.length) {
        const lastWord = transcriptionWords[transcriptionWords.length - 1];
        const lastEnd = lastWord.end ?? lastWord.end_time ?? lastWord.start ?? lastWord.start_time ?? 0;
        console.log(`[assemble] ${projectId}: transcribed ${transcriptionWords.length} words, lastWordEnd = ${lastEnd.toFixed(2)}s (audio is ${totalDuration.toFixed(2)}s; trailing silence ≈ ${(totalDuration - lastEnd).toFixed(2)}s)`);
      }

      if (canUseStoredDurations) {
        durations = beats.map((b) => Math.max(0.5, (b.duration_ms ?? 0) / 1000));
        console.log(`[assemble] ${projectId}: using stored beat durations (sum=${durations.reduce((s, d) => s + d, 0).toFixed(2)}s, voiceover=${totalDuration.toFixed(2)}s)`);
      } else {
        durations = alignBeats(beats.map((b) => b.script_segment ?? ""), transcriptionWords, totalDuration);
        try {
          const updates = beats.map((b, i) => ({
            beat_number: b.beat_number,
            duration_ms: Math.round(durations[i] * 1000),
          }));
          // Batched UPDATE via RPC — one server round-trip instead of
          // one per beat. Migration 082 in youtube-engine.
          const { error: batchErr } = await supabase.rpc("batch_update_beat_durations", {
            p_project_id: projectId,
            p_updates: updates,
          });
          if (batchErr) throw batchErr;
          await supabase
            .from("projects")
            .update({ beat_timings_voiceover_hash: voiceoverHash })
            .eq("id", projectId);
          console.log(`[assemble] ${projectId}: persisted ${updates.length} beat timings (voiceoverHash=${voiceoverHash.slice(0, 12)}…)`);
        } catch (e) {
          console.warn(`[assemble] ${projectId}: failed to persist beat timings — next assembly will re-measure:`, e);
        }
      }
      metrics.record("legacy-audio-stt");
    }

    // Captions: build the segment list ONCE here, then bake a per-beat
    // slice into each clip's normalizeClip pass below. This replaces
    // the old downstream burnSubtitles full-video re-encode — the
    // encoder is already running per beat for scale/pad/logo, so
    // adding the ass filter is essentially free. Translation also
    // runs once at this point so every per-beat slice carries
    // pre-translated text.
    //
    // The base segment list comes from STT word timings when
    // available (most accurate) or proportional segmentation of each
    // beat's script_segment as a fallback (when STT silently failed).
    // The fallback path used to live inside the burn block; moving it
    // up here means a missing STT no longer surfaces as a late
    // "captions disabled" surprise.
    let baseCaptionSegs: SrtSegment[] = [];
    if (captionsEnabled) {
      const raw = transcriptionWords.length > 0
        ? buildSrtSegments(transcriptionWords)
        : buildSrtSegmentsFromBeats(beats, durations);
      if (!raw.length) {
        throw new Error("No caption segments could be generated — check that beats have script text");
      }
      if (captionsLanguage !== "source") {
        await checkStop();
        await progress(`Translating captions to ${captionsLanguage}…`);
        const anthropic = await getAnthropicClient(userId);
        baseCaptionSegs = await translateSegments(raw, captionsLanguage, anthropic);
      } else {
        baseCaptionSegs = raw;
      }
      console.log(`[assemble] ${projectId}: prepared ${baseCaptionSegs.length} caption segments to bake into per-beat clips`);
    }
    // transcriptionWords is no longer referenced past this point — the
    // captions block above consumed it, alignBeats already ran (legacy
    // path), and Stage B's per-beat slice operates on baseCaptionSegs.
    // Release now instead of waiting until Stage D so V8 can reclaim the
    // word array (can be ~1 MB for a 30-minute project) during the long
    // Stage B encode loop. The checkpoint copy stays in the DB-persisted
    // object for Resume.
    transcriptionWords = [];

    // Cumulative start time of each beat in the master timeline,
    // computed once from durations[] so the per-beat slicer doesn't
    // re-sum on every loop iteration. cumulativeStarts[i] is the
    // offset where beat i's clip begins after concat.
    const cumulativeStarts: number[] = new Array(beats.length).fill(0);
    for (let i = 1; i < beats.length; i++) {
      cumulativeStarts[i] = cumulativeStarts[i - 1] + durations[i - 1];
    }

    // ── Stage B: per-clip normalization → concat → joined.mp4 ───────────
    //
    // Each clip is uploaded to R2 after encoding (checkpoint.clip_urls[i])
    // so a Stop mid-loop preserves completed clips. On Resume, we
    // re-download cached clips instead of re-encoding them.
    if (!checkpoint.joined_url) {
      checkpoint.clip_urls = checkpoint.clip_urls ?? new Array(beats.length).fill(null);
      // Stamp the bake-in mode on the checkpoint so the loader can
      // detect a state flip on a future Resume (e.g. user disabled
      // captions after a Stop). Persist BEFORE the worker pool
      // launches — clip uploads are fire-and-forget below, so the
      // first one's checkpoint write isn't guaranteed to land before
      // a Stop arrives. This explicit upfront persist closes that
      // window and only costs one DB write.
      // Captions are always baked per-beat now (Stage B). Persist
      // the flag so Resume runs that find an existing clip_url know
      // whether to honor it or re-encode.
      checkpoint.clips_baked_captions = captionsEnabled;
      await persistCheckpoint();
      const clipPaths: string[] = new Array(beats.length).fill("");
      await progress("Processing video clips…");

      // Fire-and-forget machinery for the per-clip R2 upload + the
      // BATCHED checkpoint persist that follows it. Workers used to
      // block on the R2 round-trip (~200-800ms each) before returning
      // to the pool — for a 100-beat project that was 30-80s of
      // upload latency the encoder was idle on. Hand the upload to a
      // background promise instead, and drain any still-in-flight
      // ones at the post-pool sync point below.
      //
      // Batched persist: each completed upload marks the checkpoint
      // dirty but DOES NOT immediately write to Supabase. A debounced
      // flusher coalesces all dirty marks within a 2s window into a
      // single DB write, and also force-flushes every 30s so a Stop
      // arriving mid-batch still has a fresh-ish checkpoint. Cuts the
      // 100-clip project's persist count from ~100 round-trips down
      // to ~3-5. Stage B's end calls flushPersist() to drain.
      const pendingClipUploads = new Set<Promise<void>>();
      // R2 uploads can fall behind the encoder pool on long projects —
      // each in-flight S3 PutObject holds a file stream + SDK chunk
      // buffers (~5 MB each). Letting 50+ accumulate stacks hundreds of
      // MB on top of the ffmpeg pool's residence, contributing to OOM.
      // Cap in-flight uploads; the worker awaits one to drain before
      // queueing the next.
      const MAX_PENDING_CLIP_UPLOADS = 4;
      const awaitUploadSlot = async (): Promise<void> => {
        while (pendingClipUploads.size >= MAX_PENDING_CLIP_UPLOADS) {
          await Promise.race(pendingClipUploads);
        }
      };
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
        // Chain on persistInFlight so two concurrent flushes serialize
        // — without this, a debounce flush and a heartbeat flush could
        // overlap and the later HTTP request could land before the
        // earlier one, clobbering its payload with stale state.
        persistInFlight = persistInFlight.then(persistCheckpoint, persistCheckpoint);
        await persistInFlight;
      };
      const markPersistDirty = (): void => {
        persistDirty = true;
        if (persistDebounceTimer) return;
        persistDebounceTimer = setTimeout(() => {
          persistDebounceTimer = null;
          // Fire and forget — the flush itself never throws (catches
          // are inside persistCheckpoint via supabase's chained call).
          void flushPersist();
        }, PERSIST_DEBOUNCE_MS);
      };
      persistHeartbeatTimer = setInterval(() => { void flushPersist(); }, PERSIST_HEARTBEAT_MS);

      // Pre-fetch the user's beat-cache index in one paginated bucket
      // scan. Previously each beat fired its own HEAD against the public
      // R2 URL — 293 round-trips on a fresh long project, almost all of
      // them misses on cold-cache runs. One ListObjectsV2 (with cursor
      // pagination) returns every cached beat key for this user; the
      // per-beat check then becomes an in-memory Set lookup.
      const beatCachePrefix = `${userFolder}/_beat_cache/`;
      let cachedBeatKeys: Set<string>;
      try {
        cachedBeatKeys = await listKeysWithPrefix(beatCachePrefix);
        console.log(`[assemble] ${projectId}: prefetched ${cachedBeatKeys.size} cached beat keys under ${beatCachePrefix}`);
      } catch (e) {
        console.warn(`[assemble] ${projectId}: beat-cache prefetch failed, falling back to per-beat HEAD checks:`, e instanceof Error ? e.message : e);
        cachedBeatKeys = new Set();
      }

      // Logo overlay: download here and composite per-beat in normalizeClip
      // so Stage D can stay on -c:v copy (no full-video re-encode just
      // to add a watermark). Download failure is non-fatal — Stage B
      // continues without the overlay and Stage D's existing logo-
      // download path runs as a fallback.
      let stageBLogoOverlay: LogoOverlay | null = null;
      if (logoUrl) {
        try {
          await progress("Downloading channel logo…");
          const logoPath = path.join(tmpDir, "logo");
          await downloadFile(logoUrl, logoPath, signal);
          stageBLogoOverlay = {
            logoPath,
            sizePct: typeof logoSize === "number" ? logoSize : 0.1,
            xPct:    typeof logoX === "number"    ? logoX    : 0.85,
            yPct:    typeof logoY === "number"    ? logoY    : 0.05,
          };
          console.log(`[assemble] ${projectId}: Stage B logo overlay configured (size=${stageBLogoOverlay.sizePct}, pos=${stageBLogoOverlay.xPct},${stageBLogoOverlay.yPct})`);
        } catch (e) {
          if (signal.aborted) throw e;
          console.warn(`[assemble] ${projectId}: logo download failed in Stage B, deferring to Stage D fallback:`, e instanceof Error ? e.message : e);
        }
      }
      // Persist clips_baked_logo alongside captions before the worker
      // pool launches, mirroring the crash-safety reasoning around the
      // earlier persistCheckpoint() — if a Stop arrives during the
      // first encodes, the checkpoint already reflects what was baked.
      checkpoint.clips_baked_logo = !!stageBLogoOverlay;
      await persistCheckpoint();

      // Worker-pool over the beat list. Each worker pulls the next
      // un-claimed index, processes it independently, then loops back
      // for more. clipPaths[]/checkpoint.clip_urls[] are indexed by
      // beat position so there are no inter-worker write conflicts.
      // STOPPED_MARKER propagates via firstError so all in-flight
      // workers bail at the next iteration.
      const allImages = beats.every((b) => !b.video_url && !!b.image_url);
      const beatLimit = getAssemblyConcurrency(resolution, allImages, beats.length, captionsEnabled);
      const matchedRule = matchAssemblyBeatRule({
        resolution: (resolution ?? "1080p") as AssemblyResolution,
        beats: beats.length,
        allImages,
        captionsEnabled,
      });
      console.log(`[assemble] ${projectId}: beat concurrency=${beatLimit} (slider=${getAssemblyBeatLimit()}, beats=${beats.length}, resolution=${resolution ?? "1080p"}, allImages=${allImages}, captions=${captionsEnabled}, matchedRule=${matchedRule ? `"${matchedRule.name}"=${matchedRule.value}` : "none"})`);
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
            // fall through to fresh encode
          }
        }
        const beat = beats[i];
        // Per-beat content-addressed cache. Build a stable hash of
        // every input that affects the encoded output: source URL,
        // duration, intermediate dimensions, encoder preset/crf,
        // logo overlay params, and the exact caption segments for
        // this beat (so a script edit invalidates only the changed
        // beats — not the whole batch). The cache key lives under
        // the user folder (no cross-tenant sharing) at a stable
        // path that survives checkpoint clears. On hit, downloads
        // instead of re-encoding; on miss, the upload below writes
        // to this same key so the next run picks it up.
        const sourceKey = beat.video_url ?? beat.image_url ?? "no-src";
        // Logo is baked per-beat in Stage B again — beats with vs
        // without an overlay produce different output and must hash to
        // different cache slots. Existing "nologo" cached beats stay
        // valid for projects without a logo.
        const logoKey = stageBLogoOverlay
          ? `${stageBLogoOverlay.sizePct}@${stageBLogoOverlay.xPct},${stageBLogoOverlay.yPct}`
          : "nologo";
        const subsKey = (captionsEnabled && baseCaptionSegs.length > 0)
          ? hashString(JSON.stringify(sliceSegmentsForBeat(baseCaptionSegs, cumulativeStarts[i], durations[i])) + `|${captionsStyle}|${captionsSize}|${captionsPosition}|${h}`)
          : "nosubs";
        const encPreset = (captionsEnabled && baseCaptionSegs.length > 0) ? "veryfast-23" : "ultrafast-28";
        const beatCacheHash = hashString(`${sourceKey}|${durations[i].toFixed(3)}|${w}x${h}|${encPreset}|${logoKey}|${subsKey}`);
        const beatCacheKey = `${userFolder}/_beat_cache/${beatCacheHash}.mp4`;
        const beatCacheUrl = `${(process.env.R2_PUBLIC_URL ?? "").replace(/\/$/, "")}/${beatCacheKey}`;
        // Cache lookup: prefer the prefetched Set (one bucket-list at
        // Stage B start) over a per-beat HEAD. The Set might be empty
        // if the prefetch failed; in that case fall back to a HEAD so
        // we still get cache hits, just slower.
        const cacheHit = cachedBeatKeys.size > 0
          ? cachedBeatKeys.has(beatCacheKey)
          : await r2ObjectExists(beatCacheUrl);
        if (cacheHit) {
          try {
            await downloadFile(beatCacheUrl, clipPath, signal);
            clipPaths[i] = clipPath;
            checkpoint.clip_urls![i] = beatCacheUrl;
            persistDirty = true;
            console.log(`[assemble] beat ${beat.beat_number}: cache hit (${beatCacheHash})`);
            return;
          } catch (e) {
            console.warn(`[assemble] beat ${beat.beat_number}: cache download failed, re-encoding:`, e);
            // fall through
          }
        }
        // Captions bake-in: slice the master segment list to this
        // beat's [beatStart, beatStart + duration) window, shift to
        // beat-relative timings, and write a per-beat ASS file.
        // normalizeClip will inject `ass=<path>` into its filter
        // chain so captions land on the clip as part of the same
        // encoder pass that's already doing scale/pad/logo. Empty
        // slices (no captions in this beat's window) just write an
        // events-less ASS file — ffmpeg renders no overlay text but
        // the filter is still invoked harmlessly.
        let beatSubtitles: { assPath: string } | null = null;
        if (captionsEnabled && baseCaptionSegs.length > 0) {
          const beatSegs = sliceSegmentsForBeat(baseCaptionSegs, cumulativeStarts[i], durations[i]);
          const assPath = path.join(tmpDir, `captions_${String(i).padStart(3, "0")}.ass`);
          writeAss(beatSegs, buildAssStyle(captionsStyle, captionsSize, captionsPosition, h), w, h, assPath);
          beatSubtitles = { assPath };
        }
        try {
          if (beat.video_url) {
            const ext = beat.video_url.includes(".webm") ? "webm" : "mp4";
            const src = path.join(tmpDir, `src_${i}.${ext}`);
            console.log(`[assemble] beat ${beat.beat_number}: downloading video…`);
            await downloadFile(beat.video_url, src, signal);
            // No per-beat probe: the [0:v:0] selector in normalizeClip
            // already handles multi-stream sources (incl. attached_pic
            // cover art). Probing 293 beats just to log a warning cost
            // ~200ms each in ffprobe spawn latency.
            console.log(`[assemble] beat ${beat.beat_number}: encoding clip…`);
            await normalizeClip(src, false, durations[i], clipPath, w, h, stageBLogoOverlay, beatSubtitles, signal);
            try { fs.unlinkSync(src); } catch { /* ignore */ }
          } else if (beat.image_url) {
            const ext = beat.image_url.toLowerCase().includes(".png") ? "png" : "jpg";
            const src = path.join(tmpDir, `src_${i}.${ext}`);
            console.log(`[assemble] beat ${beat.beat_number}: downloading image…`);
            await downloadFile(beat.image_url, src, signal);
            console.log(`[assemble] beat ${beat.beat_number}: encoding clip…`);
            await normalizeClip(src, true, durations[i], clipPath, w, h, stageBLogoOverlay, beatSubtitles, signal);
            try { fs.unlinkSync(src); } catch { /* ignore */ }
          }
          if (beatSubtitles) { try { fs.unlinkSync(beatSubtitles.assPath); } catch { /* ignore */ } }
          // Fire-and-forget the clip upload (and the serialized
          // checkpoint persist that follows) so this worker is free
          // to grab the next beat immediately instead of stalling on
          // R2 latency. See pendingClipUploads block above for the
          // design notes. The drain at the end of Stage B awaits any
          // still-pending uploads before concat unlinks the local
          // files. Bounded by MAX_PENDING_CLIP_UPLOADS so the upload
          // queue can't grow unbounded on long projects where R2
          // latency outpaces encode throughput.
          await awaitUploadSlot();
          const uploadPromise = (async () => {
            // Upload to the content-addressed cache key (computed at
            // the top of processOne). Survives checkpoint clears and
            // is reused by future assemblies whose beats hash to the
            // same value — only the inputs that affect the encode
            // are in the hash, so re-renders that don't change a
            // given beat skip its work entirely.
            //
            // Retry up to 2 extra times on transient network errors
            // (Cloudflare TLS read timeouts are the common one). A
            // failed upload only costs us the resume-cache entry —
            // the local mp4 is still concat-included — so the worst
            // case is one beat re-encoded on a future Resume. But
            // when 700+ beats run, 1-2 transient blips per run are
            // normal, and recovering them keeps the cache complete.
            const MAX_UPLOAD_ATTEMPTS = 3;
            let lastErr: unknown = null;
            for (let attempt = 1; attempt <= MAX_UPLOAD_ATTEMPTS; attempt++) {
              try {
                const clipUrl = await uploadFile(beatCacheKey, clipPath, "video/mp4");
                checkpoint.clip_urls![i] = clipUrl;
                // Mark the checkpoint dirty instead of persisting now.
                // A debounced flush within PERSIST_DEBOUNCE_MS will
                // coalesce this with other clip-completion writes into
                // one DB round-trip. See the persist machinery above
                // Stage B for design notes.
                markPersistDirty();
                if (attempt > 1) {
                  console.log(`[assemble] beat ${beat.beat_number}: upload succeeded on attempt ${attempt}`);
                }
                return;
              } catch (uploadErr) {
                lastErr = uploadErr;
                if (attempt < MAX_UPLOAD_ATTEMPTS) {
                  const backoffMs = 500 * attempt;
                  console.warn(`[assemble] beat ${beat.beat_number}: upload attempt ${attempt} failed (${uploadErr instanceof Error ? uploadErr.message : uploadErr}); retrying in ${backoffMs}ms`);
                  await new Promise((r) => setTimeout(r, backoffMs));
                }
              }
            }
            console.warn(`[assemble] beat ${beat.beat_number}: clip checkpoint upload failed after ${MAX_UPLOAD_ATTEMPTS} attempts:`, lastErr);
            // Not fatal — we just lose the resume guarantee for
            // this clip. Worker has already returned by this point.
          })();
          pendingClipUploads.add(uploadPromise);
          void uploadPromise.finally(() => { pendingClipUploads.delete(uploadPromise); });
          console.log(`[assemble] beat ${beat.beat_number}: encode done (upload in flight)`);
          clipPaths[i] = clipPath;
        } catch (e) {
          if (e instanceof Error && e.message === STOPPED_MARKER) throw e;
          console.error(`[assemble] beat ${beats[i].beat_number} skipped:`, e);
          // leave clipPaths[i] as "" — filtered out of concat below
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
            if (e instanceof Error && (e.message === STOPPED_MARKER || /stop/i.test(e.message))) {
              if (!firstError) firstError = e;
              return;
            }
            // processOne already swallows non-stop errors per-beat;
            // anything that bubbles here is a stop signal.
            if (!firstError) firstError = e instanceof Error ? e : new Error(String(e));
            return;
          }
          completed++;
          // Force a V8 GC every 20 completed beats. Stage B's beat
          // loop allocates many short-lived objects (ASS strings, hash
          // inputs, S3 PutObject parameter blocks) that fragment the
          // old generation over a 138+ beat run. The pre-mix gc() at
          // Stage D runs too late to help Stage B's own peak. Cheap
          // and only active when --expose-gc is on (already set in
          // package.json's start script).
          if (global.gc && completed % 20 === 0) {
            try {
              const beforeMb = Math.round(process.memoryUsage().rss / 1024 / 1024);
              global.gc();
              const afterMb = Math.round(process.memoryUsage().rss / 1024 / 1024);
              console.log(`[assemble] ${projectId}: gc at beat ${completed} — rss ${beforeMb}MB → ${afterMb}MB`);
            } catch { /* ignore */ }
          }
          if (beats.length > 0) {
            await progressThrottled(`Processed ${completed} of ${beats.length} clips…`);
          }
        }
      };

      await Promise.all(Array.from({ length: beatLimit }, () => beatWorker()));

      // Drain pending fire-and-forget clip uploads BEFORE either
      // throwing (Stop case) or moving on to concat. Two reasons:
      //   1. Checkpoint must reflect work that actually landed in
      //      R2, otherwise a Resume re-encodes clips we paid for.
      //   2. concat unlinks each clipPath after reading the concat
      //      list — an upload still reading the file at that point
      //      would fail and leave the checkpoint stale.
      // allSettled is defensive: uploadPromise catches its own
      // errors (so it never rejects today), but allSettled keeps
      // the drain working if that ever changes.
      if (pendingClipUploads.size > 0) {
        await progress(`Finalizing ${pendingClipUploads.size} clip checkpoints…`);
        await Promise.allSettled([...pendingClipUploads]);
      }
      // Drain the batched persist so the final flush lands BEFORE we
      // tear down the heartbeat timer and BEFORE concat unlinks the
      // local clip files. After this point checkpoint.clip_urls
      // matches what's actually in R2.
      if (persistHeartbeatTimer) { clearInterval(persistHeartbeatTimer); persistHeartbeatTimer = null; }
      await flushPersist();
      if (firstError) throw firstError;
      metrics.record("stage-b-encode");
      // Logo file stays on disk through Stage D — that's where the
      // overlay actually gets composited now. Stage D cleans it up
      // after the mix completes.

      await checkStop();
      await progress("Joining clips…");
      const validClipPaths = clipPaths.filter((p) => p !== "");
      if (!validClipPaths.length) throw new Error("All clips failed to encode — nothing to assemble.");
      if (validClipPaths.length < clipPaths.length) {
        // Silent partial-failure regression guard: surface dropped beats
        // so the user can tell their finished video is shorter than
        // intended instead of getting a quietly truncated render.
        const dropped = clipPaths
          .map((p, idx) => p === "" ? beats[idx].beat_number : null)
          .filter((n): n is number => n !== null);
        console.warn(`[assemble] ${projectId}: ${dropped.length} clip(s) failed to encode — dropping beats ${dropped.join(",")}`);
      }
      const listPath = path.join(tmpDir, "concat.txt");
      fs.writeFileSync(listPath, validClipPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n"));
      const joinedLocal = path.join(tmpDir, "joined.mp4");
      await concatClips(listPath, joinedLocal, signal);
      for (const p of validClipPaths) { try { fs.unlinkSync(p); } catch { /* ignore */ } }
      metrics.record("stage-b-concat");
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
        await downloadFile(checkpoint.joined_url!, joinedDisk, signal);
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
        // Old path here re-encoded the ENTIRE joined video just to
        // glue trailing frozen frames onto the tail. On a 30 min 1080p
        // assembly that was several minutes of libx264 + a sustained
        // memory spike for zero new visual content. The replacement:
        // grab the last frame, encode a tiny still-frame .mp4 of
        // duration = tailDuration with the same codec params we
        // already use for per-beat clips, then concat -c copy joined
        // + tail. The encode is sub-second (one frame at 24fps for a
        // few seconds), the concat is I/O-only. If concat-copy fails
        // due to bitstream mismatch we fall back to the old tpad
        // re-encode — never seen in practice, but cheap insurance.
        try {
          // Pick encoder params that match the per-beat clips inside
          // joined.mp4. When captions were baked into per-beat clips,
          // normalizeClip used veryfast/crf 23; otherwise ultrafast/crf 28.
          // Mismatch here would cross CABAC/CAVLC boundaries inside one
          // concatenated stream, which is technically valid H.264 but
          // upsets stricter players.
          // Captions are always baked per-beat now (Stage B), so the
          // per-beat encoder is veryfast/crf 23 whenever captions are
          // enabled, ultrafast/crf 28 otherwise. Match here so the
          // freeze-tail concat stays bitstream-safe.
          const subsBaked = captionsEnabled;
          const tailEnc = subsBaked
            ? { preset: "veryfast", crf: "23" }
            : { preset: "ultrafast", crf: "28" };
          const tailPath = await buildFreezeTail(joinedDisk, tmpDir, tailDuration, w, h, tailEnc, signal);
          const concatListPath = path.join(tmpDir, "pad_concat.txt");
          fs.writeFileSync(concatListPath, [joinedDisk, tailPath].map((p) => escapeConcatListEntry(p)).join("\n"));
          await concatClips(concatListPath, paddedPath, signal);
          try { fs.unlinkSync(tailPath); } catch { /* ignore */ }
        } catch (e) {
          if (signal.aborted) throw e;
          console.warn(`[assemble] freeze-tail concat-copy failed (${e instanceof Error ? e.message : e}), falling back to tpad re-encode`);
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
        }
        try { fs.unlinkSync(joinedDisk); } catch { /* ignore */ }
        try {
          const paddedUrl = await uploadFile(ckptPathFor("padded.mp4"), paddedPath, "video/mp4");
          checkpoint.padded_url = paddedUrl;
          await persistCheckpoint();
        } catch (e) {
          console.warn(`[assemble] padded.mp4 checkpoint upload failed:`, e);
        }
        metrics.record("freeze-pad");
      }
    }

    // ── Stage D: mix audio → output.mp4 ─────────────────────────────────
    const outputPath = path.join(tmpDir, "output.mp4");
    if (checkpoint.mixed_url) {
      await checkStop();
      await progress("Restoring mixed video…");
      await downloadFile(checkpoint.mixed_url, outputPath, signal);
    } else {
      // Decide which source to mix from: padded.mp4 if we made one,
      // else joined.mp4.
      let mixSrc: string;
      if (checkpoint.padded_url) {
        mixSrc = path.join(tmpDir, "padded.mp4");
        if (!fs.existsSync(mixSrc)) {
          await checkStop();
          await progress("Restoring padded video…");
          await downloadFile(checkpoint.padded_url, mixSrc, signal);
        }
      } else {
        mixSrc = path.join(tmpDir, "joined.mp4");
        if (!fs.existsSync(mixSrc)) {
          await checkStop();
          await progress("Restoring joined video…");
          await downloadFile(checkpoint.joined_url!, mixSrc, signal);
        }
      }
      await checkStop();
      let bgmVolume = backgroundMusicVolume ?? 0.15;
      // Clamp volume to a sensible range so an accidental >1 doesn't
      // produce a wall-of-music clip and a negative doesn't crash.
      if (bgmVolume < 0) bgmVolume = 0;
      if (bgmVolume > 1) bgmVolume = 1;

      // Download BGM to local disk ONCE, then ffprobe it to compute
      // a FINITE stream_loop count for the mix step. The previous
      // -stream_loop -1 was the source of the "mix step never
      // proceeds" hang: amix with duration=first should drain when
      // the voiceover input ends, but an infinite-loop second input
      // can hold the demuxer pipeline open and deadlock the muxer.
      // A finite loop count gives both audio inputs a natural EOF.
      //
      // loops = ceil(voiceover / bgm) + 1 (the +1 is a safety frame
      // so any sub-second rounding doesn't end the bgm one millisecond
      // before amix wants the last sample). Clamped to [1, 10_000]
      // so a degenerate sub-second bgm doesn't request millions of
      // loops and a probe failure doesn't pass a NaN downstream.
      //
      // Failure at any step (download, probe, bad duration) disables
      // BGM and falls through to voiceover-only — the user still
      // gets a finished video instead of a permanently stuck mix.
      let bgmConfig: { path: string; volume: number; loops: number } | null = null;
      if (backgroundMusicUrl) {
        await progress("Downloading background music…");
        const bgmLocalPath = path.join(tmpDir, "bgm.mp3");
        try {
          await downloadFile(backgroundMusicUrl, bgmLocalPath, signal);
          const bgmDuration = await getMediaDuration(bgmLocalPath);
          if (!isFinite(bgmDuration) || bgmDuration <= 0.5) {
            throw new Error(`bgm duration unreasonable: ${bgmDuration}`);
          }
          const rawLoops = Math.ceil(totalDuration / bgmDuration) + 1;
          const loops = Math.min(10_000, Math.max(1, rawLoops));
          console.log(`[assemble] ${projectId}: bgm duration=${bgmDuration.toFixed(2)}s, voiceover=${totalDuration.toFixed(2)}s → ${loops} loops`);
          bgmConfig = { path: bgmLocalPath, volume: bgmVolume, loops };
        } catch (e) {
          if (signal.aborted) throw e;
          console.warn(`[assemble] bgm setup failed, continuing without music:`, e instanceof Error ? e.message : e);
          bgmConfig = null;
        }
      }

      // Logo download — fallback path. When Stage B already baked
      // the overlay per-beat (clips_baked_logo=true on this checkpoint),
      // skip the download AND skip handing an overlay to mixAudio so it
      // stays on -c:v copy. Old checkpoints (no flag) or failed Stage B
      // logo downloads still get the overlay applied here.
      let stageDLogoOverlay: LogoOverlay | null = null;
      if (logoUrl && !checkpoint.clips_baked_logo) {
        await checkStop();
        await progress("Downloading channel logo…");
        const logoPath = path.join(tmpDir, "logo");
        try {
          await downloadFile(logoUrl, logoPath, signal);
          stageDLogoOverlay = {
            logoPath,
            sizePct: typeof logoSize === "number" ? logoSize : 0.1,
            xPct:    typeof logoX === "number"    ? logoX    : 0.85,
            yPct:    typeof logoY === "number"    ? logoY    : 0.05,
          };
        } catch (e) {
          if (signal.aborted) throw e;
          console.warn(`[assemble] logo download failed, continuing without overlay:`, e instanceof Error ? e.message : e);
        }
      }

      // Drop large in-memory arrays before the mix step to give Node
      // a chance to GC before ffmpeg spawns. The transcription word
      // list (used by alignBeats and buildSrtSegments — both done by
      // now) and per-beat duration array (consumed by Stage B which
      // has completed) are safe to release. baseCaptionSegs already
      // sliced into the per-beat ASS files; the master list is no
      // longer referenced by any downstream stage.
      transcriptionWords = [];
      durations = [];
      baseCaptionSegs = [];
      if (global.gc) {
        try { global.gc(); } catch { /* ignore — only available with --expose-gc */ }
      }

      // Drain the background audio concat (kicked off at the top of
      // per-beat audio prep when captions were off) before the mix. By
      // the time Stage B finishes encoding 293 beats, the concat is
      // almost always done — this await is usually a no-op.
      if (audioRunsInBackground) {
        await progress("Finalizing voiceover…");
        await audioReadyPromise;
      }

      await progress(bgmConfig ? "Mixing voiceover + music…" : "Mixing voiceover…");
      // Cap at totalDuration (voiceover length). The pad step above
      // ensures the video is at least totalDuration when there's
      // significant trailing silence; the cap also trims any tiny
      // encoding-rounding overshoot.
      try {
        await mixAudio(mixSrc, voiceoverPath, outputPath, totalDuration, w, h, signal, bgmConfig, stageDLogoOverlay);
      } catch (e) {
        if (signal.aborted) throw e;
        // If the mix failed with bgm enabled, retry once without it
        // — same fault-tolerance as the old "download failed → no
        // music" branch, just shifted to the mix step. The user
        // still gets a finished video with voiceover only. Logo
        // stays in the retry — if logo overlay was the cause, the
        // outer try/catch fails normally and surfaces the error.
        if (bgmConfig) {
          console.warn(`[assemble] mix with bgm failed, retrying without music:`, e instanceof Error ? e.message : e);
          await mixAudio(mixSrc, voiceoverPath, outputPath, totalDuration, w, h, signal, null, stageDLogoOverlay);
        } else {
          throw e;
        }
      }
      try { fs.unlinkSync(mixSrc); } catch { /* ignore */ }
      if (bgmConfig) { try { fs.unlinkSync(bgmConfig.path); } catch { /* ignore */ } }
      try {
        const mixedUrl = await uploadFile(ckptPathFor("mixed.mp4"), outputPath, "video/mp4");
        checkpoint.mixed_url = mixedUrl;
        await persistCheckpoint();
        // mixed.mp4 IS the final video now (no Stage F re-encode).
        // The in-progress preview row is no longer useful: by the
        // time mixed.mp4 is uploaded, the assembly is essentially
        // done — the upload-as-assembled step right below replaces
        // assembled_url within seconds, so a preview row would
        // flash on and off.
      } catch (e) {
        console.warn(`[assemble] mixed.mp4 checkpoint upload failed:`, e);
      }
      metrics.record("mix");
    }

    // Stage D.5 (full-video logo re-encode) used to live here. It was
    // replaced by baking the logo into the per-clip Stage B encode —
    // saves a full-video re-encode pass on every assembly.
    //
    // Stage E (standalone burnSubtitles full-video re-encode) used to
    // run here too. Captions are now baked into each beat's
    // normalizeClip pass (Stage B) so the mixAudio output already
    // carries the burned-in caption layer. Eliminating the second
    // whole-video re-encode saves several minutes on long captioned
    // projects. The fallback path through buildSrtSegmentsFromBeats
    // and the translation step were moved upstream too — see the
    // baseCaptionSegs block above Stage B.

    // ── Stage F: nothing. Mixed.mp4 IS the final video ─────────────────
    //
    // Stage F used to run a full-video re-encode pass that combined
    // upscale, captions, and logo. Each of those moved upstream:
    //   - Captions  → baked per-beat in Stage B (normalizeClip's
    //                 subtitles=ass filter, with a per-beat segment
    //                 slice)
    //   - Logo      → composited at Stage D's audio mix (mixAudio's
    //                 overlay branch when stageDLogoOverlay is set)
    //   - Upscale   → no longer needed; Stage B encodes at the user's
    //                 chosen final resolution directly
    // So mixed.mp4 emerging from Stage D is the finished video. We
    // just remux for faststart and upload.
    const finalPath = outputPath;
    metrics.record("final-burn");
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

// Mirrors lib/concurrency-config.ts (engine repo). Kept in sync by hand
// — both sides validate the shape on read so a malformed rule from the
// DB silently drops instead of crashing.
type AssemblyResolution = "720p" | "1080p" | "1440p" | "2160p";
const ASSEMBLY_RESOLUTIONS: readonly AssemblyResolution[] = ["720p", "1080p", "1440p", "2160p"];
interface AssemblyBeatRule {
  name: string;
  when: {
    resolution?: AssemblyResolution;
    maxBeats?: number;
    minBeats?: number;
    allImages?: boolean;
    captionsEnabled?: boolean;
  };
  value: number;
}
let assemblyBeatRules: AssemblyBeatRule[] = [];

export function getAssemblyBeatLimit(): number {
  return assemblyBeatLimit;
}

export function getAssemblyBeatRules(): AssemblyBeatRule[] {
  return assemblyBeatRules;
}

// Pure helper: pick the first rule whose conditions all match the
// in-flight project's parameters. Returns null when no rule matches —
// callers should fall back to the global slider in that case.
function matchAssemblyBeatRule(ctx: {
  resolution: AssemblyResolution;
  beats: number;
  allImages: boolean;
  captionsEnabled: boolean;
}): AssemblyBeatRule | null {
  for (const rule of assemblyBeatRules) {
    const w = rule.when;
    if (w.resolution !== undefined && w.resolution !== ctx.resolution) continue;
    if (w.maxBeats !== undefined && ctx.beats > w.maxBeats) continue;
    if (w.minBeats !== undefined && ctx.beats < w.minBeats) continue;
    if (w.allImages !== undefined && w.allImages !== ctx.allImages) continue;
    if (w.captionsEnabled !== undefined && w.captionsEnabled !== ctx.captionsEnabled) continue;
    return rule;
  }
  return null;
}

async function refreshAssemblyConcurrency(): Promise<void> {
  try {
    const { data } = await supabase
      .from("product_config")
      .select("batched_processes")
      .eq("service", "_global")
      .single();
    const cfg = (data as { batched_processes?: { assembly_projects?: unknown; assembly_beats?: unknown; assembly_beats_rules?: unknown } } | null)?.batched_processes;
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
    // Validate-on-read so a malformed rule (e.g. typo'd resolution
    // string) silently drops instead of poisoning getAssemblyConcurrency.
    const rulesRaw = cfg?.assembly_beats_rules;
    if (Array.isArray(rulesRaw)) {
      const cleaned: AssemblyBeatRule[] = [];
      for (const r of rulesRaw) {
        if (!r || typeof r !== "object") continue;
        const obj = r as Record<string, unknown>;
        const v = typeof obj.value === "number" ? obj.value : Number(obj.value);
        if (!Number.isInteger(v) || v < 1 || v > 10) continue;
        const name = typeof obj.name === "string" ? obj.name : "Unnamed";
        const whenObj = (obj.when && typeof obj.when === "object") ? obj.when as Record<string, unknown> : {};
        const when: AssemblyBeatRule["when"] = {};
        if (typeof whenObj.resolution === "string" && (ASSEMBLY_RESOLUTIONS as readonly string[]).includes(whenObj.resolution)) {
          when.resolution = whenObj.resolution as AssemblyResolution;
        }
        const mb = typeof whenObj.maxBeats === "number" ? whenObj.maxBeats : Number(whenObj.maxBeats);
        if (Number.isInteger(mb) && mb > 0) when.maxBeats = mb;
        const nb = typeof whenObj.minBeats === "number" ? whenObj.minBeats : Number(whenObj.minBeats);
        if (Number.isInteger(nb) && nb > 0) when.minBeats = nb;
        if (typeof whenObj.allImages === "boolean") when.allImages = whenObj.allImages;
        if (typeof whenObj.captionsEnabled === "boolean") when.captionsEnabled = whenObj.captionsEnabled;
        cleaned.push({ name, when, value: v });
      }
      const prevSig = JSON.stringify(assemblyBeatRules);
      const nextSig = JSON.stringify(cleaned);
      if (prevSig !== nextSig) {
        console.log(`[assembly-queue] rules changed: ${assemblyBeatRules.length} → ${cleaned.length} rule(s)`);
        assemblyBeatRules = cleaned;
      }
    } else if (assemblyBeatRules.length > 0) {
      console.log(`[assembly-queue] rules cleared`);
      assemblyBeatRules = [];
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

          // Read options with three-tier fallback:
          //   1. Redis (set by /api/generate/assemble) — the most
          //      recent user submission via the UI
          //   2. project row's persisted columns — survives Redis
          //      eviction, manual SQL re-queues, and worker restarts
          //   3. Hard-coded defaults — used only when neither
          //      Redis nor the row has a value
          // Previously the worker read only from Redis and fell to
          // defaults when Redis was empty, which meant a SQL
          // re-queue silently dropped the user's bgm/logo/captions
          // configuration.
          const opts = (await redis.get(`assembly:${projectId}`) as Record<string, unknown> | null) ?? {};
          const { data: rowOpts } = await supabase
            .from("projects")
            .select("background_music_url, background_music_volume, logo_url, logo_x, logo_y, logo_size, captions_enabled, captions_language, captions_style, captions_size, captions_position, trim_silence_enabled, aspect_ratio, resolution")
            .eq("id", projectId)
            .maybeSingle();
          const projectRow = (rowOpts as Record<string, unknown> | null) ?? {};
          // Pick: prefer Redis, fall back to projectRow column,
          // fall back to hard default. Explicit null/undefined check
          // (not `??`) is intentional — a stored false / 0 / "" is
          // a valid user choice that must beat the hard default.
          const pick = <T>(redisKey: string, rowKey: string, fallback: T): T => {
            const r = opts[redisKey];
            if (r !== undefined && r !== null) return r as T;
            const rv = projectRow[rowKey];
            if (rv !== undefined && rv !== null) return rv as T;
            return fallback;
          };
          const finalBgmUrl = pick<string | null>("backgroundMusicUrl", "background_music_url", null);
          const finalLogoUrl = pick<string | null>("logoUrl", "logo_url", null);
          const finalCaptions = pick<boolean>("captionsEnabled", "captions_enabled", false);
          console.log(`[assembly-queue] ${projectId}: opts resolved bgm=${JSON.stringify(finalBgmUrl)} logo=${JSON.stringify(finalLogoUrl)} captions=${finalCaptions} (redisKeys=[${Object.keys(opts).join(",")}], rowKeys=[${Object.keys(projectRow).join(",")}])`);

          assemblingProjects.add(projectId);
          runAssembly({
            projectId,
            userId,
            aspectRatio: pick<string>("aspectRatio", "aspect_ratio", "16:9"),
            voiceoverType: (pick<string>("voiceoverType", "voiceover_type", "cleaned")) as "cleaned" | "original",
            captionsEnabled: finalCaptions,
            captionsLanguage: pick<string>("captionsLanguage", "captions_language", "source"),
            captionsStyle: pick<string>("captionsStyle", "captions_style", "default"),
            captionsSize: pick<string>("captionsSize", "captions_size", "medium"),
            captionsPosition: pick<string>("captionsPosition", "captions_position", "bottom"),
            trimSilenceEnabled: pick<boolean>("trimSilenceEnabled", "trim_silence_enabled", false),
            backgroundMusicUrl: finalBgmUrl,
            backgroundMusicVolume: pick<number>("backgroundMusicVolume", "background_music_volume", 0.15),
            resolution: pick<ResolutionPreset>("resolution", "resolution", "1080p"),
            logoUrl: finalLogoUrl,
            logoX: pick<number>("logoX", "logo_x", 0.85),
            logoY: pick<number>("logoY", "logo_y", 0.05),
            logoSize: pick<number>("logoSize", "logo_size", 0.1),
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

    // Atomic claim — same guard the poll loop uses at the other entry
    // path. The previous unconditional UPDATE would overwrite a row
    // that was already mid-assembly on a different worker (Render
    // replica, retry from engine, double-click), producing 2-3
    // parallel runs of the same project that each fight for CPU/RAM
    // and submit duplicate Coconut jobs. The .in() filter accepts the
    // restart-eligible statuses (queued, stopped, failed, preview)
    // and rejects in-flight ones (processing, uploading), so a
    // legitimate Resume still works while a duplicate trigger
    // bounces with started=false.
    //
    // Note: the assemblingProjects.has() check above catches the
    // SAME-worker race. This DB claim catches the CROSS-worker one.
    const RESTARTABLE_STATUSES = ["queued", "stopped", "failed", "preview"];
    const { data: claimed } = await supabase.from("projects")
      .update({
        assembly_status: "processing",
        assembly_progress: "Starting…",
        assembly_error: null,
        assembly_started_at: new Date().toISOString(),
        assembly_finished_at: null,
      })
      .eq("id", projectId)
      .eq("user_id", user.id)
      .or(`assembly_status.in.(${RESTARTABLE_STATUSES.join(",")}),assembly_status.is.null`)
      .select("id")
      .maybeSingle();
    if (!claimed) {
      res.json({ started: false, reason: "Assembly already in progress for this project" });
      return;
    }

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
