import { Worker } from "bullmq";
import { queueConnection } from "../lib/queue.js";
import { submitVideoJob, pollVideoJob } from "../lib/kie.js";
import { uploadFromUrl } from "../lib/storage.js";
import { supabase } from "../lib/supabase.js";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export const videoWorker = new Worker(
  "video-generation",
  async (job) => {
    const { projectId, beatNumber, videoPrompt, imageUrl, modelId, duration, aspectRatio, userId } = job.data as {
      projectId: string;
      beatNumber: number;
      videoPrompt: string;
      imageUrl?: string;
      modelId: string;
      duration?: string | number;
      aspectRatio?: string;
      userId: string;
    };

    console.log(`[worker] Processing beat ${beatNumber} for project ${projectId}`);

    // Look up the user's KIE API key from app_settings
    const { data: settings, error: settingsError } = await supabase
      .from("app_settings")
      .select("kie_api_key")
      .eq("user_id", userId)
      .single();

    if (settingsError) console.warn(`[worker] Could not fetch settings for user ${userId}:`, settingsError.message);

    const kieApiKey = settings?.kie_api_key ?? process.env.KIE_API_KEY;
    if (!kieApiKey) throw new Error(`No KIE API key found for user ${userId}`);

    const { error: renderingError } = await supabase
      .from("project_beats")
      .update({ video_status: "rendering" })
      .eq("project_id", projectId)
      .eq("beat_number", beatNumber);
    if (renderingError) console.warn(`[worker] Failed to set rendering status for beat ${beatNumber}:`, renderingError.message);

    // Submit to kie.ai video generation
    const jobId = await submitVideoJob(videoPrompt, modelId, kieApiKey, imageUrl, duration, aspectRatio);
    console.log(`[worker] Submitted video job: ${jobId}`);

    // Poll until complete
    let videoUrl: string | undefined;
    let attempts = 0;
    const maxAttempts = 60; // 10 minutes at 10s intervals

    while (!videoUrl && attempts < maxAttempts) {
      await sleep(10000);
      attempts++;

      const status = await pollVideoJob(jobId, modelId, kieApiKey);

      if (status.status === "done" && status.videoUrl) {
        videoUrl = status.videoUrl;
        console.log(`[worker] Video ready: ${videoUrl}`);
      } else if (status.status === "failed") {
        throw new Error(`kie.ai video job failed: ${status.error}`);
      }

      await job.updateProgress(Math.min(Math.round((attempts / maxAttempts) * 100), 99));
    }

    if (!videoUrl) {
      throw new Error("Video generation timed out after 10 minutes");
    }

    // Upload to Supabase Storage
    const storagePath = `${projectId}/videos/beat-${beatNumber}.mp4`;
    const publicUrl = await uploadFromUrl(storagePath, videoUrl, "video/mp4");
    console.log(`[worker] Uploaded to storage: ${publicUrl}`);

    // Update DB
    const { error: doneError } = await supabase
      .from("project_beats")
      .update({ video_url: publicUrl, video_status: "done" })
      .eq("project_id", projectId)
      .eq("beat_number", beatNumber);
    if (doneError) console.warn(`[worker] Failed to mark beat ${beatNumber} done:`, doneError.message);

    // Update project progress
    const { data: doneBeat, error: progressQueryError } = await supabase
      .from("project_beats")
      .select("beat_number")
      .eq("project_id", projectId)
      .eq("video_status", "done");
    if (progressQueryError) console.warn(`[worker] Failed to count done beats:`, progressQueryError.message);

    const { error: progressError } = await supabase
      .from("projects")
      .update({ videos_progress: doneBeat?.length ?? 0 })
      .eq("id", projectId);
    if (progressError) console.warn(`[worker] Failed to update project progress:`, progressError.message);

    console.log(`[worker] Beat ${beatNumber} complete`);
    return { url: publicUrl, beatNumber };
  },
  {
    connection: queueConnection,
    concurrency: 3,
  }
);

videoWorker.on("failed", async (job, err) => {
  if (!job) return;
  const { projectId, beatNumber } = job.data;
  console.error(`[worker] Job ${job.id} failed:`, err.message);
  await supabase
    .from("project_beats")
    .update({ video_status: "failed" })
    .eq("project_id", projectId)
    .eq("beat_number", beatNumber);
});

videoWorker.on("ready", () => {
  console.log("[worker] Video worker ready — concurrency: 3");
});

videoWorker.on("error", (err) => {
  console.error("[worker] Worker error:", err);
});

