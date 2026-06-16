import { submitVideoJob, pollVideoJob } from "../lib/kie.js";
import { uploadFromUrl, userFolderForId, deleteObject, r2KeyFromUrl } from "../lib/storage.js";
import { supabase } from "../lib/supabase.js";
import { logProjectCost } from "../lib/costs.js";

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

// Default concurrency cap. The actual value is sourced from
// product_config._global.batched_processes.video_worker on each poll
// tick so the admin can tune it from the dashboard without a restart.
// We keep a constant default to fall back to if the DB read fails.
const DEFAULT_CONCURRENCY = 3;
let concurrency = DEFAULT_CONCURRENCY;
const POLL_INTERVAL_MS = 5000;
const CREDITS_RETRY_MS = 5 * 60 * 1000; // retry after 5 minutes
let activeJobs = 0;
let creditsExhaustedAt: number | null = null;

async function refreshConcurrency(): Promise<void> {
  try {
    const { data } = await supabase
      .from("product_config")
      .select("batched_processes")
      .eq("service", "_global")
      .single();
    const raw = (data as { batched_processes?: { video_worker?: unknown } } | null)
      ?.batched_processes?.video_worker;
    const n = typeof raw === "number" ? raw : Number(raw);
    if (Number.isInteger(n) && n >= 1 && n <= 50) {
      if (n !== concurrency) {
        console.log(`[worker] Concurrency changed: ${concurrency} → ${n}`);
        concurrency = n;
      }
    }
  } catch (err) {
    console.warn("[worker] Failed to refresh concurrency from product_config:", err instanceof Error ? err.message : err);
  }
}

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

  // Wall-clock from submit to terminal status — powers the engine's
  // "Fastest" tab in the video model picker. Stamped here so the
  // measured elapsed reflects what the user perceives: time between
  // hitting Queue and the clip becoming available. Includes KIE
  // queueing + actual generation, which is what matters for ranking.
  const submitT0 = Date.now();
  const jobId = await submitVideoJob(videoPrompt, modelId, kieApiKey, imageUrl, duration, aspectRatio);
  console.log(`[worker] Submitted video job: ${jobId}`);

  // Note: we deliberately do NOT flip the beat to "rendering" yet.
  // KIE accepting the submission only means it has the request, not
  // that it's actively producing video — the first few polls often
  // return state=pending while the task sits in KIE's internal
  // queue. The beat stays "submitting" through that window and only
  // promotes to "rendering" when a poll actually returns status=
  // "processing". That way the UI badge is honest: "rendering"
  // means KIE is genuinely working on this clip right now.
  let promotedToRendering = false;

  let videoUrl: string | undefined;
  // Kling 3.0, Veo, and longer clips can legitimately run 12-18 min on KIE.
  // 120 attempts × 10s = 20 min ceiling catches almost all stragglers.
  const MAX_POLL_ATTEMPTS = 120;
  // KIE intermittently returns state=failed with no extractable reason
  // mid-generation, then completes successfully on the next poll. Track
  // unexplained failures (empty error string from pollVideoJob — the
  // signal that no failMsg / failReason / error / errorMessage / failCode
  // / errorCode field was set) and only give up after a few in a row.
  // A failed response with a real reason still trips the hard fail
  // immediately so legit failures don't waste the full 20-min budget.
  const SOFT_FAIL_LIMIT = 3;
  let softFailures = 0;

  for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
    await sleep(10000);
    const status = await pollVideoJob(jobId, modelId, kieApiKey);
    console.log(`[worker] Poll attempt ${attempt + 1} beat=${beatNumber} status=${status.status} hasUrl=${!!status.videoUrl}${status.error ? ` err="${status.error}"` : ""}`);
    // Log credits as soon as KIE bills us (done OR failed). The
    // ledger should reflect actual spend even on failed jobs since
    // KIE charged for the attempt.
    if ((status.status === "done" || status.status === "failed") && status.creditsConsumed) {
      // durationSec drives the engine's "N cr/s" picker chip. We coerce
      // the project's video_duration (string-or-number depending on
      // how it was stored) to a number. Frame-counted models (Sora's
      // n_frames knob) shouldn't write a seconds value — pass null so
      // the per-sec query filter excludes them rather than computing
      // a misleading credits-per-frame value.
      const FRAME_COUNTED_MODELS = new Set(["sora-2-image-to-video"]);
      const rawDuration = typeof duration === "string" ? Number(duration) : duration;
      const durationSec = (!FRAME_COUNTED_MODELS.has(modelId) && typeof rawDuration === "number" && rawDuration > 0)
        ? rawDuration
        : null;
      void logProjectCost({
        projectId,
        userId,
        step: "video_gen",
        provider: "kie",
        model: modelId,
        units: status.creditsConsumed,
        unitKind: "kie_credits",
        durationSec,
        elapsedMs: Date.now() - submitT0,
      });
    }

    // First time KIE reports the job as actively processing, flip
    // the beat from "submitting" to "rendering". Guarded by the
    // promotedToRendering flag so we hit the DB at most once per
    // beat instead of on every poll. The status guard on the
    // UPDATE keeps us safe against the cancel-pending sweep: if
    // the row is no longer "submitting" (e.g. cancelled), the
    // UPDATE matches zero rows and we just keep polling — final
    // commit guard prevents zombie completion.
    if (!promotedToRendering && status.status === "processing") {
      const { data: promoted } = await supabase.from("project_beats")
        .update({ video_status: "rendering" })
        .eq("project_id", projectId)
        .eq("beat_number", beatNumber)
        .eq("video_status", "submitting")
        .select("beat_number");
      if (promoted && promoted.length > 0) {
        console.log(`[worker] Beat ${beatNumber} KIE started processing — flipped submitting → rendering`);
      }
      promotedToRendering = true;
    }
    if (status.status === "done") {
      if (status.videoUrl) { videoUrl = status.videoUrl; break; }
      // Done but no URL — log full response and keep polling briefly in case URL appears
      console.warn(`[worker] Beat ${beatNumber} done but no videoUrl yet, attempt ${attempt + 1}`);
      if (attempt >= 3) throw new Error("Job completed on KIE but no video URL was returned");
    }
    if (status.status === "failed") {
      const reason = (status.error ?? "").trim();
      if (!reason && softFailures + 1 < SOFT_FAIL_LIMIT) {
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
  // Timestamp in the key guarantees every render produces a unique
  // URL — without it, browsers + CDNs cache the OLD render against the
  // same URL and the user sees stale content when a re-queued beat
  // finishes. Same pattern as the image route's beat-N_TS.png keys.
  //
  // Capture the previous video_url BEFORE upload + UPDATE so we can
  // best-effort delete the orphaned R2 object after the new one
  // successfully lands. The engine route no longer wipes video_url
  // at queue time — keeping the old URL means the UI keeps showing
  // the existing clip while regen runs, and if regen fails the user
  // doesn't lose what they had.
  const { data: previousRow } = await supabase
    .from("project_beats")
    .select("video_url")
    .eq("project_id", projectId)
    .eq("beat_number", beatNumber)
    .single();
  const previousVideoUrl = (previousRow?.video_url as string | null) ?? null;

  const storagePath = `${userFolder}/${projectId}/videos/beat-${beatNumber}_${Date.now()}.mp4`;
  const publicUrl = await uploadFromUrl(storagePath, videoUrl, "video/mp4");
  console.log(`[worker] Uploaded: ${publicUrl}`);

  // Idempotency guard: only commit the new URL if the beat is STILL
  // in "rendering" state. If the engine's cancel-pending route, the
  // user, or another worker flipped this beat to "failed" / "paused"
  // / "done" while our KIE poll was running, do NOT overwrite — the
  // new render becomes an R2 orphan and the user keeps the state
  // they intentionally landed on. Previously the unconditional UPDATE
  // would resurrect a cancelled beat with a video the user thought
  // they'd discarded, leading to "mystery video appeared after
  // cancel" confusion.
  // Allow either "submitting" or "rendering" as the valid prior
  // state — KIE occasionally skips the processing phase entirely
  // and goes straight from pending → done (cached result, very
  // fast model), so we never promoted the beat to rendering.
  // Anything else (failed / paused / done / null) means the row
  // was cancelled or already finalized; skip and clean up the
  // orphan upload.
  const { data: updated } = await supabase.from("project_beats")
    .update({ video_url: publicUrl, video_status: "done" })
    .eq("project_id", projectId)
    .eq("beat_number", beatNumber)
    .in("video_status", ["submitting", "rendering"])
    .select("beat_number");

  if (!updated || updated.length === 0) {
    console.log(`[worker] Beat ${beatNumber} no longer submitting/rendering — skipping commit; new render at ${publicUrl} is orphaned in R2`);
    // Clean up the just-uploaded orphan so we don't leak storage when
    // the beat was cancelled out from under us. Best-effort.
    const orphanKey = r2KeyFromUrl(publicUrl);
    if (orphanKey) {
      try { await deleteObject(orphanKey); }
      catch (e) { console.warn(`[worker] Failed to delete orphan ${orphanKey}:`, e instanceof Error ? e.message : e); }
    }
    return;
  }

  // Now that the new URL is live on the row, drop the previous R2
  // object. Best-effort — a failure here just leaves an orphan, no
  // user-visible consequence. Skip when there was no prior URL (first
  // generation for this beat) or when the URL points at something
  // outside our R2 bucket (e.g. legacy paths).
  if (previousVideoUrl && previousVideoUrl !== publicUrl) {
    const oldKey = r2KeyFromUrl(previousVideoUrl);
    if (oldKey) {
      try {
        await deleteObject(oldKey);
        console.log(`[worker] Deleted previous video object: ${oldKey}`);
      } catch (e) {
        console.warn(`[worker] Failed to delete previous video object ${oldKey}:`, e instanceof Error ? e.message : e);
      }
    }
  }

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
  // Atomic claim: only succeeds if still 'queued'. Note we
  // transition to "submitting" — an intermediate state that
  // signals "this worker is about to talk to KIE." Status is
  // bumped to "rendering" only after KIE actually accepts the
  // job (see processBeat). This way the UI never shows
  // "rendering" for beats where the KIE call hasn't happened
  // yet, and a worker crash before submission leaves the row
  // in a recoverable "submitting" state instead of a stuck
  // "rendering" state with no job behind it.
  const { data, error } = await supabase
    .from("project_beats")
    .update({ video_status: "submitting" })
    .eq("project_id", beat.project_id)
    .eq("beat_number", beat.beat_number)
    .eq("video_status", "queued")
    .select("beat_number")
    .single();

  return !error && !!data;
}

async function pollLoop() {
  await refreshConcurrency();
  console.log(`[worker] Supabase-polling worker started (concurrency: ${concurrency})`);
  let ticks = 0;
  while (true) {
    try {
      // Re-read the admin-tunable concurrency every ~30s (6 ticks at
      // 5s/tick) so the worker picks up changes without a restart.
      if (ticks++ % 6 === 0) await refreshConcurrency();
      const slots = concurrency - activeJobs;
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
