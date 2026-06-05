import { submitVideoJob, pollVideoJob } from "../lib/kie.js";
import { uploadFromUrl, userFolderForId } from "../lib/storage.js";
import { supabase } from "../lib/supabase.js";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

interface QueuedBeat {
  beat_number: number;
  project_id: string;
  video_prompt: string;
  image_url?: string;
  video_model_id: string;
  video_duration?: string | number;
  video_aspect_ratio: string;
  user_id: string;
}

const CONCURRENCY = 3;
const POLL_INTERVAL_MS = 5000;
const CREDITS_RETRY_MS = 5 * 60 * 1000; // retry after 5 minutes
let activeJobs = 0;
let creditsExhaustedAt: number | null = null;

async function failAllQueued(errorMessage: string) {
  const { error } = await supabase
    .from("project_beats")
    .update({ video_status: "failed", video_error: errorMessage })
    .eq("video_status", "queued");
  if (error) console.error("[worker] Failed to bulk-fail queued beats:", error.message);
  else console.log("[worker] Bulk-failed all remaining queued beats due to credits exhaustion");
}

async function processBeat(beat: QueuedBeat) {
  const { beat_number: beatNumber, project_id: projectId, video_prompt: videoPrompt,
    image_url: imageUrl, video_model_id: modelId, video_duration: duration,
    video_aspect_ratio: aspectRatio, user_id: userId } = beat;

  console.log(`[worker] Processing beat ${beatNumber} for project ${projectId}`);

  const { data: settings, error: settingsError } = await supabase
    .from("account_settings")
    .select("kie_api_key")
    .eq("user_id", userId)
    .single();

  if (settingsError) console.warn(`[worker] Could not fetch settings for ${userId}:`, settingsError.message);

  const kieApiKey = settings?.kie_api_key ?? process.env.KIE_API_KEY;
  if (!kieApiKey) throw new Error(`No KIE API key found for user ${userId}`);

  const jobId = await submitVideoJob(videoPrompt, modelId, kieApiKey, imageUrl, duration, aspectRatio);
  console.log(`[worker] Submitted video job: ${jobId}`);

  let videoUrl: string | undefined;
  // Kling 3.0, Veo, and longer clips can legitimately run 12-18 min on KIE.
  // 120 attempts × 10s = 20 min ceiling catches almost all stragglers.
  const MAX_POLL_ATTEMPTS = 120;
  // KIE intermittently returns state=failed with no specific reason
  // mid-generation, then completes successfully on the next poll. Track
  // unexplained failures separately and only give up after a few in a
  // row — a "failed" with a real error reason still trips the hard fail
  // immediately, so legit failures don't waste the full 20-min budget.
  const SOFT_FAIL_LIMIT = 3;
  let softFailures = 0;
  const GENERIC_FAIL_REASONS = new Set(["Video generation failed", "Veo generation failed", ""]);

  for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
    await sleep(10000);
    const status = await pollVideoJob(jobId, modelId, kieApiKey);
    console.log(`[worker] Poll attempt ${attempt + 1} beat=${beatNumber} status=${status.status} hasUrl=${!!status.videoUrl}${status.error ? ` err="${status.error}"` : ""}`);
    if (status.status === "done") {
      if (status.videoUrl) { videoUrl = status.videoUrl; break; }
      // Done but no URL — log full response and keep polling briefly in case URL appears
      console.warn(`[worker] Beat ${beatNumber} done but no videoUrl yet, attempt ${attempt + 1}`);
      if (attempt >= 3) throw new Error("Job completed on KIE but no video URL was returned");
    }
    if (status.status === "failed") {
      const reason = (status.error ?? "").trim();
      const isUnexplained = GENERIC_FAIL_REASONS.has(reason);
      if (isUnexplained && softFailures + 1 < SOFT_FAIL_LIMIT) {
        softFailures++;
        console.warn(`[worker] Beat ${beatNumber} soft-fail ${softFailures}/${SOFT_FAIL_LIMIT - 1} (no reason from KIE) — retrying`);
        continue;
      }
      throw new Error(`kie.ai video job failed: ${reason || "no reason returned by KIE"}`);
    } else {
      // Any non-failed response resets the soft-fail counter — KIE is
      // making progress again.
      softFailures = 0;
    }
  }

  if (!videoUrl) throw new Error("Video generation timed out after 20 minutes");

  const userFolder = await userFolderForId(userId);
  const storagePath = `${userFolder}/${projectId}/videos/beat-${beatNumber}.mp4`;
  const publicUrl = await uploadFromUrl(storagePath, videoUrl, "video/mp4");
  console.log(`[worker] Uploaded: ${publicUrl}`);

  await supabase.from("project_beats")
    .update({ video_url: publicUrl, video_status: "done" })
    .eq("project_id", projectId)
    .eq("beat_number", beatNumber);

  const { data: doneBeats } = await supabase
    .from("project_beats")
    .select("beat_number")
    .eq("project_id", projectId)
    .eq("video_status", "done");

  await supabase.from("projects")
    .update({ videos_progress: doneBeats?.length ?? 0 })
    .eq("id", projectId);

  console.log(`[worker] Beat ${beatNumber} complete`);
}

async function tryClaimBeat(beat: QueuedBeat): Promise<boolean> {
  // Atomic claim: only succeeds if still 'queued'
  const { data, error } = await supabase
    .from("project_beats")
    .update({ video_status: "rendering" })
    .eq("project_id", beat.project_id)
    .eq("beat_number", beat.beat_number)
    .eq("video_status", "queued")
    .select("beat_number")
    .single();

  return !error && !!data;
}

async function pollLoop() {
  console.log(`[worker] Supabase-polling worker started (concurrency: ${CONCURRENCY})`);
  while (true) {
    try {
      const slots = CONCURRENCY - activeJobs;
      if (creditsExhaustedAt !== null) {
        if (Date.now() - creditsExhaustedAt < CREDITS_RETRY_MS) {
          await sleep(POLL_INTERVAL_MS);
          continue;
        }
        console.log("[worker] Credits retry window elapsed — resuming");
        creditsExhaustedAt = null;
      }
      if (slots > 0) {
        const { data: rows, error: queryError } = await supabase
          .from("project_beats")
          .select(`
            beat_number, project_id, video_prompt, image_url,
            projects!inner(user_id, video_model_id, video_duration, video_aspect_ratio)
          `)
          .eq("video_status", "queued")
          .limit(slots);
        if (queryError) console.error("[worker] Queue query failed:", queryError.message);

        for (const row of rows ?? []) {
          const proj = Array.isArray(row.projects) ? row.projects[0] : row.projects as Record<string, unknown>;
          const beat: QueuedBeat = {
            beat_number: row.beat_number as number,
            project_id: row.project_id as string,
            video_prompt: row.video_prompt as string,
            image_url: row.image_url as string | undefined,
            video_model_id: proj?.video_model_id as string,
            video_duration: proj?.video_duration as string | number | undefined,
            video_aspect_ratio: (proj?.video_aspect_ratio as string) ?? "16:9",
            user_id: proj?.user_id as string,
          };

          if (!beat.video_model_id || !beat.user_id) {
            console.warn(`[worker] Beat ${beat.beat_number} missing model/user, skipping`);
            continue;
          }

          const claimed = await tryClaimBeat(beat);
          if (!claimed) continue;

          activeJobs++;
          processBeat(beat)
            .catch(async (err: Error) => {
              console.error(`[worker] Beat ${beat.beat_number} failed:`, err.message);
              await supabase.from("project_beats")
                .update({ video_status: "failed", video_error: err.message })
                .eq("project_id", beat.project_id)
                .eq("beat_number", beat.beat_number);
              if (err.message.toLowerCase().includes("insufficient") || err.message.toLowerCase().includes("balance")) {
                creditsExhaustedAt = Date.now();
                console.error("[worker] Credits exhausted — failing all remaining queued beats, will retry in 5 min");
                await failAllQueued(err.message);
              }
            })
            .finally(() => { activeJobs--; });
        }
      }
    } catch (err) {
      console.error("[worker] Poll error:", err);
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

export function startVideoWorker() {
  pollLoop().catch(console.error);
}
