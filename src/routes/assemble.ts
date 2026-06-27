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
  const timeoutSignal = AbortSignal.timeout(60_000);
  const controller = new AbortController();
  const onAbort = () => controller.abort();
  signal?.addEventListener("abort", onAbort, { once: true });
  timeoutSignal.addEventListener("abort", onAbort, { once: true });
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
    if (!res.body) throw new Error(`No response body for ${url}`);
    const nodeStream = Readable.fromWeb(res.body as import("stream/web").ReadableStream);
    await pipeline(nodeStream, fs.createWriteStream(dest));
  } catch (err) {
    if (signal?.aborted) throw new Error(STOPPED_MARKER);
    if (timeoutSignal.aborted) throw new Error(`Download timed out: ${url}`);
    throw err instanceof Error ? err : new Error(String(err));
  } finally {
    signal?.removeEventListener("abort", onAbort);
    timeoutSignal.removeEventListener("abort", onAbort);
  }
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
const DEFAULT_FFMPEG_THREADS = Math.max(1, Math.min(4, Math.floor(os.cpus().length / 2)));
const FFMPEG_THREADS = ASSEMBLY_SAFE_MODE
  ? 1
  : Math.max(1, Number.parseInt(process.env.FFMPEG_THREADS ?? String(DEFAULT_FFMPEG_THREADS), 10) || 1);

// Unique marker so the catch path in runAssembly can distinguish a
// user-requested stop from a real error and persist the checkpoint
// instead of clearing it.
const STOPPED_MARKER = "ASSEMBLY_STOPPED_BY_USER";

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

function mixAudio(
  video: string,
  audio: string,
  output: string,
  videoDuration: number,
  signal?: AbortSignal,
  bgm?: { path: string; volume: number } | null,
): Promise<void> {
  return ffmpegWithTimeout((cmd) => {
    cmd.input(video).inputOptions(["-fflags", "+genpts"]);
    cmd.input(audio);
    if (bgm) {
      // Three inputs: video, voiceover (1), bgm (2). -stream_loop -1
      // on the bgm input loops the file indefinitely so a short
      // music track fills the entire voiceover duration instead of
      // dropping out mid-narration. amix duration=first caps the
      // combined output to the voiceover length so the looped bgm
      // never trails past the end. volume=`bgm.volume` keeps music
      // well under the dialog (default 0.15 ≈ -16 dB, classic
      // "podcast bed" level).
      cmd.input(bgm.path).inputOptions(["-stream_loop", "-1"]);
      cmd.complexFilter([
        `[2:a]volume=${bgm.volume}[bgmDucked]`,
        `[1:a][bgmDucked]amix=inputs=2:duration=first:dropout_transition=0[mix]`,
      ]);
      cmd.outputOptions(["-map", "0:v", "-map", "[mix]", "-c:v", "copy", "-c:a", "aac", "-t", String(videoDuration)]);
    } else {
      cmd.outputOptions(["-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-t", String(videoDuration)]);
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

async function isStopRequested(projectId: string): Promise<boolean> {
  const { data } = await supabase.from("projects").select("assembly_stop_requested").eq("id", projectId).single();
  return !!(data?.assembly_stop_requested as boolean | undefined);
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

async function runAssembly(opts: AssembleOptions): Promise<void> {
  const { userId, projectId, aspectRatio, voiceoverType, captionsEnabled, captionsLanguage, captionsStyle, captionsSize, captionsPosition, trimSilenceEnabled, backgroundMusicUrl, backgroundMusicVolume, resolution, logoUrl, logoX, logoY, logoSize } = opts;
  const [w, h] = dimsFor(aspectRatio, resolution);
  console.log(`[assemble] ${projectId}: resolution=${resolution ?? "1080p"} dims=${w}x${h}`);

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
        // Audio prep is I/O bound (R2 downloads + ffprobe duration). Even
        // with trimSilence on, each trim is a sub-second ffmpeg op on a
        // small mp3 — no real contention with the per-clip encode pool
        // because that pool only starts AFTER this loop completes.
        // Keep this pool conservative to avoid too many concurrent I/O
        // downloads while still making reasonable progress.
        const audioLimit = ASSEMBLY_SAFE_MODE ? 1 : Math.min(Math.max(1, getAssemblyBeatLimit()), beats.length, 3);
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
      checkpoint.clips_baked_captions = captionsEnabled;
      await persistCheckpoint();
      const clipPaths: string[] = new Array(beats.length).fill("");
      await progress("Processing video clips…");

      // Fire-and-forget machinery for the per-clip R2 upload + the
      // checkpoint persist that follows it. Workers used to block on
      // the R2 round-trip (~200-800ms each) before returning to the
      // pool — for a 100-beat project that was 30-80s of upload
      // latency the encoder was idle on. Hand the upload to a
      // background promise instead, and drain any still-in-flight
      // ones at the post-pool sync point below.
      //
      // persistChain serializes the checkpoint writes so two
      // concurrent uploads can't race: without it, persist A could
      // serialize its JSON payload (containing url-A), then persist
      // B writes (containing both A + B), then persist A's HTTP
      // request lands AFTER B's, clobbering B's url. The chain
      // forces them through one at a time even when many uploads
      // finish back-to-back.
      const pendingClipUploads = new Set<Promise<void>>();
      let persistChain: Promise<void> = Promise.resolve();
      const persistSerial = (): Promise<void> => {
        persistChain = persistChain.then(persistCheckpoint, persistCheckpoint);
        return persistChain;
      };

      // Download the channel logo once so every clip in the worker
      // pool can read it. Baking the overlay into the per-clip
      // encode here replaces the old Stage D.5 full-video re-encode.
      // The coreHash already includes logoKey, so any cached clips
      // we restore from checkpoint were encoded with the same logo
      // settings — no risk of mixing logoed/non-logoed clips.
      let stageBLogoOverlay: LogoOverlay | null = null;
      if (logoUrl) {
        await checkStop();
        await progress("Downloading channel logo…");
        const logoPath = path.join(tmpDir, "logo");
        try {
          await downloadFile(logoUrl, logoPath, signal);
          stageBLogoOverlay = {
            logoPath,
            sizePct: typeof logoSize === "number" ? logoSize : 0.1,
            xPct:    typeof logoX === "number"    ? logoX    : 0.85,
            yPct:    typeof logoY === "number"    ? logoY    : 0.05,
          };
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
      const beatLimit = ASSEMBLY_SAFE_MODE
        ? 1
        : Math.min(Math.max(1, getAssemblyBeatLimit()), beats.length, 4);
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
              const clipUrl = await uploadFile(ckptPathFor(`clip_${String(i).padStart(3, "0")}.mp4`), clipPath, "video/mp4");
              checkpoint.clip_urls![i] = clipUrl;
              await persistSerial();
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
      if (firstError) throw firstError;

      // Logo was baked into each clip; the source file isn't needed
      // for any later stage.
      if (stageBLogoOverlay) {
        try { fs.unlinkSync(stageBLogoOverlay.logoPath); } catch { /* ignore */ }
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
      // Download the user's background music to a local temp file if
      // one was provided. Failure here logs and disables bgm rather
      // than failing the entire mix — the assembly still succeeds with
      // voiceover-only audio so the user doesn't lose progress over a
      // music-file outage.
      let bgmPath: string | null = null;
      let bgmVolume = backgroundMusicVolume ?? 0.15;
      // Clamp volume to a sensible range so an accidental >1 doesn't
      // produce a wall-of-music clip and a negative doesn't crash.
      if (bgmVolume < 0) bgmVolume = 0;
      if (bgmVolume > 1) bgmVolume = 1;
      if (backgroundMusicUrl) {
        try {
          await progress("Downloading background music…");
          bgmPath = path.join(tmpDir, "bgm.mp3");
          await downloadFile(backgroundMusicUrl, bgmPath, signal);
        } catch (e) {
          console.warn(`[assemble] bgm download failed, continuing without music:`, e);
          bgmPath = null;
        }
      }
      await progress(bgmPath ? "Mixing voiceover + music…" : "Mixing voiceover…");
      // Cap at totalDuration (voiceover length). The pad step above
      // ensures the video is at least totalDuration when there's
      // significant trailing silence; the cap also trims any tiny
      // encoding-rounding overshoot.
      await mixAudio(
        mixSrc,
        voiceoverPath,
        outputPath,
        totalDuration,
        signal,
        bgmPath ? { path: bgmPath, volume: bgmVolume } : null,
      );
      try { fs.unlinkSync(mixSrc); } catch { /* ignore */ }
      if (bgmPath) { try { fs.unlinkSync(bgmPath); } catch { /* ignore */ } }
      try {
        const mixedUrl = await uploadFile(ckptPathFor("mixed.mp4"), outputPath, "video/mp4");
        checkpoint.mixed_url = mixedUrl;
        await persistCheckpoint();
      } catch (e) {
        console.warn(`[assemble] mixed.mp4 checkpoint upload failed:`, e);
      }
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
    const finalPath = outputPath;
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
        assembly_finished_at: new Date().toISOString(),
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
          assembly_finished_at: new Date().toISOString(),
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
          assembly_finished_at: new Date().toISOString(),
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
