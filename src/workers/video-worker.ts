import { submitVideoJob, pollVideoJob } from "../lib/kie.js";
import { uploadFromUrl } from "../lib/storage.js";
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
let activeJobs = 0;
let creditsExhausted = false;

async function processBeat(beat: QueuedBeat) {
  const { beat_number: beatNumber, project_id: projectId, video_prompt: videoPrompt,
    image_url: imageUrl, video_model_id: modelId, video_duration: duration,
    video_aspect_ratio: aspectRatio, user_id: userId } = beat;

  console.log(`[worker] Processing beat ${beatNumber} for project ${projectId}`);

  const { data: settings, error: settingsError } = await supabase
    .from("app_settings")
    .select("kie_api_key")
    .eq("user_id", userId)
    .single();

  if (settingsError) console.warn(`[worker] Could not fetch settings for ${userId}:`, settingsError.message);

  const kieApiKey = settings?.kie_api_key ?? process.env.KIE_API_KEY;
  if (!kieApiKey) throw new Error(`No KIE API key found for user ${userId}`);

  const jobId = await submitVideoJob(videoPrompt, modelId, kieApiKey, imageUrl, duration, aspectRatio);
  console.log(`[worker] Submitted video job: ${jobId}`);

  let videoUrl: string | undefined;
  for (let attempt = 0; attempt < 60; attempt++) {
    await sleep(10000);
    const status = await pollVideoJob(jobId, modelId, kieApiKey);
    if (status.status === "done" && status.videoUrl) { videoUrl = status.videoUrl; break; }
    if (status.status === "failed") throw new Error(`kie.ai video job failed: ${status.error}`);
  }

  if (!videoUrl) throw new Error("Video generation timed out after 10 minutes");

  const storagePath = `${projectId}/videos/beat-${beatNumber}.mp4`;
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
      if (creditsExhausted) {
        await sleep(60000); // check again in 1 min
        creditsExhausted = false;
        continue;
      }
      if (slots > 0) {
        const { data: rows } = await supabase
          .from("project_beats")
          .select(`
            beat_number, project_id, video_prompt, image_url,
            projects!inner(user_id, video_model_id, video_duration, video_aspect_ratio)
          `)
          .eq("video_status", "queued")
          .limit(slots);

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
              if (err.message.toLowerCase().includes("insufficient") || err.message.toLowerCase().includes("balance")) {
                creditsExhausted = true;
                console.error("[worker] Credits exhausted — pausing until credits are topped up");
              }
              await supabase.from("project_beats")
                .update({ video_status: "failed" })
                .eq("project_id", beat.project_id)
                .eq("beat_number", beat.beat_number);
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
