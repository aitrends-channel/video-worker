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

// Quick stream-layout check on a downloaded beat source. Catches the
// AI-video-with-cover-art-thumbnail class of input that has multiple
// video streams or an attached_pic, which the mp4 muxer would later
// choke on if `-map 0:v` matched all of them. Returns null when the
// probe itself fails so the caller doesn't hard-fail on a transient
// ffprobe error — better to attempt the encode and let normalizeClip
// (which uses an explicit [0:v:0] selector) handle it.
interface SourceProbe { videoStreams: number; hasAttachedPic: boolean; }
function probeSource(filePath: string): Promise<SourceProbe | null> {
  return new Promise((resolve) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ffmpeg.ffprobe(filePath, (err: Error, meta: any) => {
      if (err) { resolve(null); return; }
      const streams = Array.isArray(meta?.streams) ? meta.streams : [];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const videos = streams.filter((s: any) => s?.codec_type === "video");
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const hasAttachedPic = videos.some((s: any) => s?.disposition?.attached_pic === 1);
      resolve({ videoStreams: videos.length, hasAttachedPic });
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
  signal?: AbortSignal,
  bgm?: { path: string; volume: number; loops: number } | null,
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
    if (bgm) {
      // Three inputs: video, voiceover (1), bgm (2). bgm.loops is a
      // FINITE count computed from ffprobe of the bgm + voiceover
      // duration, NOT -1. The previous version used -stream_loop -1
      // and would hang the entire mix step: amix with duration=first
      // is supposed to drain when the voiceover (input 1) ends, but
      // the infinite-loop input 2 keeps producing packets and the
      // muxer can deadlock waiting for both demuxer threads to reach
      // EOF. -t at the output should propagate back through the
      // filter graph but doesn't reliably in all ffmpeg builds with
      // complex_filter. A finite stream_loop value gives input 2 a
      // natural EOF, amix drains cleanly, the mux finalizes.
      //
      // -shortest is layered on top as belt-and-braces: stop the
      // whole output as soon as any stream ends. Combined with
      // duration=first, the voiceover length wins.
      //
      // volume=`bgm.volume` keeps music under the dialog (default
      // 0.15 ≈ -16 dB, classic "podcast bed" level).
      cmd.input(bgm.path).inputOptions([...lightInput, "-stream_loop", String(bgm.loops)]);
      cmd.complexFilter([
        `[2:a]volume=${bgm.volume}[bgmDucked]`,
        `[1:a][bgmDucked]amix=inputs=2:duration=first:dropout_transition=0[mix]`,
      ]);
      cmd.outputOptions([
        "-map", "0:v", "-map", "[mix]", "-c:v", "copy", "-c:a", "aac", "-b:a", "128k",
        "-t", String(videoDuration),
        "-shortest",
        "-max_muxing_queue_size", "1024",
      ]);
    } else {
      cmd.outputOptions([
        "-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-b:a", "128k",
        "-t", String(videoDuration),
        "-max_muxing_queue_size", "1024",
      ]);
    }
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
  // Admin override: when assembly_beats_at_final_res is on, the
  // per-beat encode runs at the user's chosen output resolution
  // regardless of whether Stage F will run. Lets the admin trade
  // Stage B encode time for a simpler/no Stage F upscale.
  if (getAssemblyBeatsAtFinalRes()) return dimsFor(aspect, preset);
  if (!useFinalBurn) return dimsFor(aspect, preset);
  const cap: ResolutionPreset = "720p";
  const rank: Record<ResolutionPreset, number> = { "720p": 0, "1080p": 1, "1440p": 2, "2160p": 3 };
  const effective = preset && rank[preset] > rank[cap] ? cap : (preset ?? cap);
  return dimsFor(aspect, effective);
}

function getAssemblyConcurrency(): number {
  if (ASSEMBLY_SAFE_MODE) return 1;
  // Admin's slider value is honored exactly (validated 1..10 at the
  // refresh layer). No hidden ceiling here — if the operator
  // explicitly dialed 8, they want 8. The previous beat-count-aware
  // caps (2 for >80 beats, 4 otherwise) over-protected for
  // Render-Standard memory limits and ignored local/larger-tier
  // deploys where higher fan-out is safe.
  return Math.max(1, getAssemblyBeatLimit());
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
    const preferFinalBurn = beats.length > FINAL_BURN_THRESHOLD || ASSEMBLY_SAFE_MODE;
    const useFinalBurn = preferFinalBurn && (captionsEnabled || !!logoUrl);
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
    const useCoconut = !!process.env.COCONUT_API_KEY && useFinalBurn && (captionsEnabled || !!logoUrl);
    const [w, h] = intermediateDimsFor(aspectRatio, resolution, useFinalBurn);
    if (w !== finalW || h !== finalH) {
      console.log(`[assemble] ${projectId}: intermediate dims=${w}x${h} (final ${finalW}x${finalH}); final-burn pass will upscale`);
    }

    metrics.record("load-project");

    const voiceoverPath = path.join(tmpDir, "voiceover.mp3");
    let totalDuration = 0;
    let durations: number[] = [];
    let transcriptionWords: TranscriptionWord[] = [];

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

        await checkStop();
        await progress("Joining per-beat audio…");
        const audioListPath = path.join(tmpDir, "audio_concat.txt");
        fs.writeFileSync(audioListPath, beats.map((beat) => escapeConcatListEntry(beat.voiceover_url!)).join("\n"));
        try {
          await concatClips(audioListPath, voiceoverPath, signal);
          didRemoteAudioConcat = true;
        } catch (e) {
          console.warn(`[assemble] ${projectId}: remote audio concat failed, falling back to local download/concat:`, e instanceof Error ? e.message : e);
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

    // Cumulative start time of each beat in the master timeline,
    // computed once from durations[] so the per-beat slicer doesn't
    // re-sum on every loop iteration. cumulativeStarts[i] is the
    // offset where beat i's clip begins after concat.
    const cumulativeStarts: number[] = new Array(beats.length).fill(0);
    for (let i = 1; i < beats.length; i++) {
      cumulativeStarts[i] = cumulativeStarts[i - 1] + durations[i - 1];
    }

    // useFinalBurn was decided up front (right after beats were loaded)
    // so the per-beat encode pool could pick intermediate dims. The
    // logo download path below still depends on the same flag.
    let finalLogoPath: string | null = null;

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
      checkpoint.clips_baked_captions = captionsEnabled && !useFinalBurn;
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

      // Download the channel logo. If we're doing a final-stage burn
      // we still need the logo locally later, but we don't bake it
      // into per-beat clips: that would recreate the heavy parallel
      // encode pattern. Store either a per-clip overlay or a final
      // logo path depending on `useFinalBurn`.
      let stageBLogoOverlay: LogoOverlay | null = null;
      if (logoUrl) {
        await checkStop();
        await progress("Downloading channel logo…");
        const logoPath = path.join(tmpDir, "logo");
        try {
          await downloadFile(logoUrl, logoPath, signal);
          // Bake into Stage F (one full-video pass) only when the
          // LOCAL final-burn will run anyway — i.e. captions are
          // enabled AND Coconut isn't taking over. In every other
          // case (Coconut, or no captions), bake per-beat in Stage B.
          // The per-beat overlay rides along on an encode that's
          // happening anyway, so it's effectively free CPU. Coconut's
          // spec becomes simpler too — fewer params, fewer failure
          // modes.
          if (useFinalBurn && !useCoconut) {
            finalLogoPath = logoPath;
          } else {
            stageBLogoOverlay = {
              logoPath,
              sizePct: typeof logoSize === "number" ? logoSize : 0.1,
              xPct:    typeof logoX === "number"    ? logoX    : 0.85,
              yPct:    typeof logoY === "number"    ? logoY    : 0.05,
            };
          }
        } catch (e) {
          console.warn(`[assemble] logo download failed, encoding clips without overlay:`, e);
        }
      }

      // Worker-pool over the beat list. Each worker pulls the next
      // un-claimed index, processes it independently, then loops back
      // for more. clipPaths[]/checkpoint.clip_urls[] are indexed by
      // beat position so there are no inter-worker write conflicts.
      // STOPPED_MARKER propagates via firstError so all in-flight
      // workers bail at the next iteration.
      // Keep this conservative so long-form projects don't overload the
      // worker with too many simultaneous ffmpeg encodes and temp-file writes.
      const beatLimit = getAssemblyConcurrency();
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
        const logoKey = stageBLogoOverlay && logoUrl
          ? `${logoUrl}@${stageBLogoOverlay.sizePct}x${stageBLogoOverlay.xPct},${stageBLogoOverlay.yPct}`
          : "nologo";
        const subsKey = (!useFinalBurn && captionsEnabled && baseCaptionSegs.length > 0)
          ? hashString(JSON.stringify(sliceSegmentsForBeat(baseCaptionSegs, cumulativeStarts[i], durations[i])) + `|${captionsStyle}|${captionsSize}|${captionsPosition}|${h}`)
          : "nosubs";
        const encPreset = (!useFinalBurn && captionsEnabled && baseCaptionSegs.length > 0) ? "veryfast-23" : "ultrafast-28";
        const beatCacheHash = hashString(`${sourceKey}|${durations[i].toFixed(3)}|${w}x${h}|${encPreset}|${logoKey}|${subsKey}`);
        const beatCacheKey = `${userFolder}/_beat_cache/${beatCacheHash}.mp4`;
        const beatCacheUrl = `${(process.env.R2_PUBLIC_URL ?? "").replace(/\/$/, "")}/${beatCacheKey}`;
        if (await r2ObjectExists(beatCacheUrl)) {
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
        if (!useFinalBurn && captionsEnabled && baseCaptionSegs.length > 0) {
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
            // Probe layout before encoding. Multi-video-stream sources
            // (including the AI-video-with-cover-art-thumbnail case)
            // are handled by the [0:v:0] pin in normalizeClip; we log
            // the anomaly so a future codec regression surfaces in
            // the assembly log instead of as a silent 10-min waste.
            const probe = await probeSource(src);
            if (probe && (probe.videoStreams > 1 || probe.hasAttachedPic)) {
              console.warn(`[assemble] beat ${beat.beat_number}: source has ${probe.videoStreams} video stream(s)${probe.hasAttachedPic ? " incl. attached_pic" : ""} — encoding first stream only`);
            }
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
          // files.
          const uploadPromise = (async () => {
            try {
              // Upload to the content-addressed cache key (computed at
              // the top of processOne). Survives checkpoint clears and
              // is reused by future assemblies whose beats hash to the
              // same value — only the inputs that affect the encode
              // are in the hash, so re-renders that don't change a
              // given beat skip its work entirely.
              const clipUrl = await uploadFile(beatCacheKey, clipPath, "video/mp4");
              checkpoint.clip_urls![i] = clipUrl;
              // Mark the checkpoint dirty instead of persisting now.
              // A debounced flush within PERSIST_DEBOUNCE_MS will
              // coalesce this with other clip-completion writes into
              // one DB round-trip. See the persist machinery above
              // Stage B for design notes.
              markPersistDirty();
            } catch (uploadErr) {
              console.warn(`[assemble] beat ${beat.beat_number}: clip checkpoint upload failed:`, uploadErr);
              // Not fatal — we just lose the resume guarantee for
              // this clip. Worker has already returned by this point.
            }
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

      // Logo was baked into each clip; the source file isn't needed
      // for any later stage.
      if (stageBLogoOverlay) {
        try { fs.unlinkSync(stageBLogoOverlay.logoPath); } catch { /* ignore */ }
      }

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
          const subsBaked = captionsEnabled && !useFinalBurn;
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

      // Drop large in-memory arrays before the mix step to give Node
      // a chance to GC before ffmpeg spawns. The transcription word
      // list (used by alignBeats and buildSrtSegments — both done by
      // now) and per-beat duration array (consumed by Stage B which
      // has completed) are safe to release. baseCaptionSegs is held
      // back when useFinalBurn=true because the final-burn pass
      // still needs it; in the per-beat-bake path it can also drop.
      transcriptionWords = [];
      durations = [];
      if (!useFinalBurn) baseCaptionSegs = [];
      if (global.gc) {
        try { global.gc(); } catch { /* ignore — only available with --expose-gc */ }
      }

      await progress(bgmConfig ? "Mixing voiceover + music…" : "Mixing voiceover…");
      // Cap at totalDuration (voiceover length). The pad step above
      // ensures the video is at least totalDuration when there's
      // significant trailing silence; the cap also trims any tiny
      // encoding-rounding overshoot.
      try {
        await mixAudio(mixSrc, voiceoverPath, outputPath, totalDuration, signal, bgmConfig);
      } catch (e) {
        if (signal.aborted) throw e;
        // If the mix failed with bgm enabled, retry once without it
        // — same fault-tolerance as the old "download failed → no
        // music" branch, just shifted to the mix step. The user
        // still gets a finished video with voiceover only.
        if (bgmConfig) {
          console.warn(`[assemble] mix with bgm failed, retrying without music:`, e instanceof Error ? e.message : e);
          await mixAudio(mixSrc, voiceoverPath, outputPath, totalDuration, signal, null);
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
        // Surface mixed.mp4 as an in-progress preview WHEN the
        // final-burn pass is still ahead of us — the user gets a
        // playable video while the multi-minute captions/logo/
        // upscale re-encode runs in the background. On the
        // non-final-burn path, mix is the last encode and the
        // upload completes seconds later, so a preview URL would
        // flash on and off.
        if (useFinalBurn) {
          await supabase.from("projects")
            .update({ assembly_preview_url: mixedUrl })
            .eq("id", projectId);
        }
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

    // ── Stage F: validate + remux + upload final ────────────────────────
    //
    // SINGLE-PASS final burn: any combination of upscale, subtitles,
    // and logo overlay now runs in ONE ffmpeg invocation instead of
    // two. Earlier versions did pass 1 = scale + subtitles, pass 2 =
    // logo overlay — two full-video re-encodes at final resolution
    // that doubled the finalize wall-clock on long projects. Fused
    // into a single complex_filter, the encoder runs once and every
    // frame goes through the whole chain: scale → subtitles → logo.
    //
    // Scale algorithm: bicubic instead of lanczos. Lanczos is best
    // for true-source upscaling where preserving detail matters; on
    // AI-generated footage (already softer than camera-shot video)
    // the perceptual difference is negligible and bicubic encodes
    // 2-3x faster.
    //
    // Captions are written at finalH so font sizing matches the
    // user's chosen output resolution, not the intermediate.
    let finalPath = outputPath;
    // useCoconut was decided up at the mode-selection block so the
    // Stage B logo gate could read it. Re-evaluating here would
    // produce a different result (finalLogoPath was nulled out when
    // useCoconut was true), so we just reuse the earlier value.
    // When Coconut fails partway, we still want the assembly to
    // complete via the local burn path rather than aborting the
    // whole run. Track the failure here so the else-if below picks
    // it up (we can't just throw — the user's already paid for the
    // earlier stages and the local fallback works fine, just slower).
    let coconutFailed = false;
    if (useCoconut) {
      // ── Stage F: offload the final burn to Coconut ──────────────────
      //
      // Skip the local ffmpeg re-encode entirely. Coconut runs the
      // scale/subtitles/logo on their hardware while THIS worker
      // returns to processing the next user. Cuts ~5-25 min of CPU
      // off the worker's wall-clock for this assembly.
      //
      // Pre-requisites:
      //   - mixed.mp4 already in R2 (checkpoint.mixed_url is set
      //     above; we re-use that URL as Coconut's input).
      //   - ASS captions file uploaded to R2 too so Coconut can fetch
      //     it. Logo is already at logoUrl (the user uploaded it).
      //
      // Output: Coconut writes the finalized mp4 directly to R2 at
      // the same _assembly/ prefix. We then download it locally for
      // the existing remux+upload tail to consume — keeps the moov
      // faststart + upload-failure-retry logic intact.
      try {
        await checkStop();
        await progress("Submitting final-burn to Coconut…");
        const { submitJob, pollJob } = await import("../lib/coconut.js");
        const hasCaptions = captionsEnabled && baseCaptionSegs.length > 0;
        let captionsAssUrl: string | null = null;
        if (hasCaptions) {
          // Upload the ASS file to R2 so Coconut can fetch it via HTTPS.
          // Path mirrors the other _assembly/ artifacts.
          const assLocal = path.join(tmpDir, "final_captions.ass");
          writeAss(baseCaptionSegs, buildAssStyle(captionsStyle, captionsSize, captionsPosition, finalH), finalW, finalH, assLocal);
          captionsAssUrl = await uploadFile(ckptPathFor("final_captions.ass"), assLocal, "text/x-ssa");
        }
        // Coconut's output path can't go through the userFolder
        // (which is the user's email) because their path parser
        // mangles paths containing "@" and "." chars (we hit
        // output_filename_not_valid even with the @ URL-encoded —
        // gmail.com's dot is what their parser is reading as a
        // filename extension). Use a flat coconut-out/<projectId>/
        // prefix in the same R2 bucket, scoped per-project. The
        // worker downloads the result from the same path right
        // after Coconut completes, so this prefix never appears
        // in the final assembled_url stored on the project row —
        // that comes from the worker's own re-upload below.
        const outputKey = `coconut-out/${projectId}/final_burned.mp4`;
        const job = await submitJob({
          inputUrl: checkpoint.mixed_url!,
          outputBucketKey: outputKey,
          outputWidth: finalW,
          outputHeight: finalH,
          captionsAssUrl,
          // Logo is already baked into the per-beat Stage B encodes
          // when useCoconut=true (see the Stage B logo gate). Sending
          // null here keeps the Coconut spec simpler — Coconut just
          // scales + burns captions, no watermark step that could
          // fail upstream.
          logoUrl: null,
        });
        console.log(`[assemble] ${projectId}: coconut job ${job.id} submitted`);
        await progress("Finalizing on Coconut…");
        await pollJob(job.id, signal, (status) => {
          // Throttle progress writes — pollJob may emit multiple
          // updates as status flips queued → processing → completed.
          void progress(`Coconut: ${status}…`);
        });
        console.log(`[assemble] ${projectId}: coconut job ${job.id} completed`);
        // Coconut wrote directly to R2. Download to a local path so
        // the remux + final-upload tail below works unchanged.
        const burnedPath = path.join(tmpDir, "final_burned.mp4");
        const burnedUrl = `${process.env.R2_PUBLIC_URL?.replace(/\/$/, "")}/${outputKey}`;
        await downloadFile(burnedUrl, burnedPath, signal);
        finalPath = burnedPath;
      } catch (e) {
        // User-requested stop / finalize-preview propagate unchanged.
        const msg = e instanceof Error ? e.message : String(e);
        if (msg === STOPPED_MARKER || msg === FINALIZE_PREVIEW_MARKER || signal.aborted) throw e;
        // Coconut failed (config issue, upload failure, etc). Don't
        // burn the run — fall through to the local ffmpeg burn block
        // below. The user gets a working video; we get a clear log
        // line pointing at Coconut so we can fix it without blocking
        // assemblies.
        console.warn(`[assemble] ${projectId}: Coconut failed (${msg}) — falling back to local final burn`);
        await progress("Coconut unavailable — finishing locally…");
        coconutFailed = true;
      }
    }
    if (!useCoconut || coconutFailed) {
      if (useFinalBurn && (captionsEnabled || finalLogoPath)) {
      // Coconut not configured — fall back to local ffmpeg burn.
      // This is the same code path as before; unchanged logic, just
      // gated behind the "COCONUT_API_KEY missing" check above.
      await checkStop();
      const needsUpscale = w !== finalW || h !== finalH;
      await progress("Burning captions…");
      const assPath = path.join(tmpDir, "final_captions.ass");
      const hasCaptions = captionsEnabled && baseCaptionSegs.length > 0;
      if (hasCaptions) {
        writeAss(baseCaptionSegs, buildAssStyle(captionsStyle, captionsSize, captionsPosition, finalH), finalW, finalH, assPath);
      }
      const burnedPath = path.join(tmpDir, "final_burned.mp4");

      // Build the video filter chain step list. Order matters:
      // scale FIRST so the subtitles+logo render at output dims,
      // subtitles BEFORE logo so the logo composites on top of the
      // captioned frame (matches the per-beat-bake ordering).
      const videoSteps: string[] = [];
      if (needsUpscale) videoSteps.push(`scale=${finalW}:${finalH}:flags=bicubic`);
      if (hasCaptions) videoSteps.push(`subtitles=${escapeAssPath(assPath)}`);

      if (finalLogoPath) {
        // Logo present → use complex_filter so we can chain video
        // ops with the overlay input. [0:v:0] runs scale/subtitles
        // on the FIRST video stream only (some sources carry an
        // attached_pic that [0:v] would also pick up and break the
        // mp4 muxer), [1:v] sizes the logo, the final overlay
        // composites them. When there are no preceding video steps
        // the chain is a null filter (just labels through) so the
        // graph structure stays consistent.
        const posX = Math.round(finalW * (logoX ?? 0.85));
        const posY = Math.round(finalH * (logoY ?? 0.05));
        const logoPx = Math.max(8, Math.min(finalW, Math.round((finalW * (logoSize ?? 0.1)) / 2) * 2));
        const videoChain = videoSteps.length > 0
          ? `[0:v:0]${videoSteps.join(",")}[base]`
          : `[0:v:0]null[base]`;
        await ffmpegWithTimeout((cmd) =>
          cmd
            .input(outputPath)
            .input(finalLogoPath)
            .complexFilter([
              videoChain,
              `[1:v]scale=w=${logoPx}:h=-2[logo]`,
              `[base][logo]overlay=${posX}:${posY}:format=auto[v]`,
            ])
            .outputOptions(["-map", "[v]", "-map", "0:a?", "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "copy"])
            .output(burnedPath),
          "finalBurnFused",
          signal,
        );
        finalPath = burnedPath;
      } else if (videoSteps.length > 0) {
        // No logo — single-input -vf chain is simpler and lighter
        // than complex_filter. Covers scale-only, subtitles-only,
        // and scale+subtitles cases.
        await ffmpegWithTimeout((cmd) =>
          cmd
            .input(outputPath)
            .outputOptions(["-vf", videoSteps.join(","), "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "copy"])
            .output(burnedPath),
          "finalBurnSimple",
          signal,
        );
        finalPath = burnedPath;
      }
      // Else: no video changes needed (no captions, no logo, no
      // upscale). Shouldn't happen given the outer condition but
      // fall through with finalPath = outputPath for safety.
    } else if (w !== finalW || h !== finalH) {
      // useFinalBurn was off (no captions, no logo) but we still
      // produced intermediate clips at a smaller resolution and the
      // user wants something larger. Single upscale pass before
      // remux. Same encode params as the burn passes above so output
      // quality is consistent across modes.
      await checkStop();
      await progress(`Upscaling to ${finalW}x${finalH}…`);
      const scaled = path.join(tmpDir, "scaled.mp4");
      await ffmpegWithTimeout((cmd) =>
        cmd
          .input(outputPath)
          .outputOptions(["-vf", `scale=${finalW}:${finalH}:flags=bicubic`, "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "copy"])
          .output(scaled),
        "intermediateUpscale",
        signal,
      );
      finalPath = scaled;
    }
    }
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
// Admin-tunable Assemble flag. Sourced from
// product_config.batched_processes alongside the numeric knobs.
// When true, Stage B encodes at the user's final resolution and
// Coconut only burns captions; when false (default), Stage B uses
// 720p intermediate and Coconut handles the upscale.
let assemblyBeatsAtFinalRes = false;

export function getAssemblyBeatLimit(): number {
  return assemblyBeatLimit;
}

export function getAssemblyBeatsAtFinalRes(): boolean {
  return assemblyBeatsAtFinalRes;
}

async function refreshAssemblyConcurrency(): Promise<void> {
  try {
    const { data } = await supabase
      .from("product_config")
      .select("batched_processes")
      .eq("service", "_global")
      .single();
    const cfg = (data as { batched_processes?: { assembly_projects?: unknown; assembly_beats?: unknown; assembly_beats_at_final_res?: unknown } } | null)?.batched_processes;
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
    const finalResRaw = cfg?.assembly_beats_at_final_res;
    if (typeof finalResRaw === "boolean" && finalResRaw !== assemblyBeatsAtFinalRes) {
      console.log(`[assembly-queue] beats-at-final-res changed: ${assemblyBeatsAtFinalRes} → ${finalResRaw}`);
      assemblyBeatsAtFinalRes = finalResRaw;
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
