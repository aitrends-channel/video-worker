const KIE_BASE_URL = "https://api.kie.ai";

async function kieRequest<T>(endpoint: string, options: RequestInit, apiKey: string): Promise<T> {
  const url = `${KIE_BASE_URL}${endpoint}`;
  let res: Response;
  try {
    res = await fetch(url, {
      ...options,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
        ...(options.headers ?? {}),
      },
    });
  } catch (err) {
    // Node's undici throws a bare "fetch failed" with no URL context
    // when the underlying network call can't complete (DNS, reset,
    // KIE outage). Wrap it so the worker log + DB video_error tells
    // us which endpoint died, which makes triaging the next outage
    // an order of magnitude faster.
    const cause = err instanceof Error ? err.message : String(err);
    throw new Error(`kie.ai network error on ${endpoint}: ${cause}`);
  }
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`kie.ai error ${res.status}: ${body}`);
  }
  return res.json() as Promise<T>;
}

interface KieTaskResponse {
  code: number;
  msg: string;
  data: { taskId: string };
}

interface KieRecordResponse {
  code: number;
  data: {
    state?: string;
    status?: string;
    resultJson?: string;
    output?: string | string[];
    // KIE puts the failure reason in different fields depending on the
    // model family / endpoint. The image-side polling already checks
    // all of these; the worker had only been reading two.
    failReason?: string;
    failMsg?: string;
    failCode?: string | number;
    error?: string;
    errorMessage?: string | null;
    errorCode?: string | number | null;
    videoInfo?: { videoUrl?: string };
    successFlag?: number;
    videoUrl?: string;
    video_url?: string;
    // KIE bills per task; recordInfo returns the credit count
    // for the completed task. Used by the project_costs ledger.
    creditsConsumed?: number;
    [key: string]: unknown;
  };
}

// Pulls the most specific human-readable reason KIE included in a
// failed-job response. Returns null when KIE gave nothing — that's the
// signal the worker uses to treat the failure as transient and retry.
function extractFailureReason(d: KieRecordResponse["data"] | undefined): string | null {
  if (!d) return null;
  const candidates = [d.failMsg, d.failReason, d.error, d.errorMessage];
  for (const c of candidates) {
    if (typeof c === "string" && c.trim()) return c.trim();
  }
  const code = d.failCode ?? d.errorCode;
  if (code !== null && code !== undefined && String(code).trim()) {
    return `fail code ${String(code).trim()}`;
  }
  return null;
}

const MODEL_DURATION_KEYS: Record<string, string> = {
  "sora-2-image-to-video": "n_frames",
};

export async function submitVideoJob(
  prompt: string,
  modelId: string,
  apiKey: string,
  imageUrl?: string,
  duration?: string | number,
  aspectRatio = "16:9"
): Promise<string> {

  // Veo
  if (modelId === "veo3" || modelId === "veo3_fast") {
    // KIE's /api/v1/veo/generate serves both Veo 3 variants. Without
    // a `model` field it returns "Invalid Model" because the endpoint
    // can't tell which variant we want. Both ids are passed through
    // as-is — KIE accepts "veo3" and "veo3_fast" verbatim.
    const body: Record<string, unknown> = { prompt, model: modelId };
    if (!imageUrl) body.aspect_ratio = aspectRatio;
    if (imageUrl) body.imageUrls = [imageUrl];
    const res = await kieRequest<KieTaskResponse>("/api/v1/veo/generate", {
      method: "POST",
      body: JSON.stringify(body),
    }, apiKey);
    if (res.code !== 200) {
      console.error(`[kie] submit rejected modelId=${modelId} msg="${res.msg}" body=${JSON.stringify(body).slice(0, 600)}`);
      throw new Error(res.msg ?? "Failed to submit Veo job");
    }
    return res.data.taskId;
  }

  // Runway
  if (modelId === "runway") {
    // KIE's Runway endpoint rejects submissions without a quality
    // field — "Video quality cannot be empty". 720p is a safe default
    // for Gen-3 Turbo; surface a user-selectable picker later if we
    // want to expose 1080p / standard / high tiers.
    const body: Record<string, unknown> = { prompt, quality: "720p" };
    if (!imageUrl) body.aspectRatio = aspectRatio;
    if (duration) body.duration = duration;
    if (imageUrl) body.imageUrl = imageUrl;
    const res = await kieRequest<KieTaskResponse>("/api/v1/runway/generate", {
      method: "POST",
      body: JSON.stringify(body),
    }, apiKey);
    if (res.code !== 200) {
      console.error(`[kie] submit rejected modelId=${modelId} msg="${res.msg}" body=${JSON.stringify(body).slice(0, 600)}`);
      throw new Error(res.msg ?? "Failed to submit Runway job");
    }
    return res.data.taskId;
  }

  // Generic createTask models
  const input: Record<string, unknown> = { prompt };
  if (!imageUrl) input.aspect_ratio = aspectRatio;
  if (duration !== undefined) {
    const key = MODEL_DURATION_KEYS[modelId] ?? "duration";
    input[key] = duration;
  }
  if (imageUrl) {
    if (modelId === "grok-imagine/image-to-video") input.image_urls = [imageUrl];
    else if (modelId === "wan/2-7-image-to-video") input.first_frame_url = imageUrl;
    // wan/2-6-flash mirrors wan/2-7's input shape — first_frame_url,
    // not image_urls. Previous image_urls was a guess from when the
    // model first landed and KIE returns "Video model rejected" on
    // the wrong field name.
    else if (modelId === "wan/2-6-flash-image-to-video") input.first_frame_url = imageUrl;
    else if (modelId === "sora-2-image-to-video") input.image_urls = [imageUrl];
    else if (modelId === "bytedance/seedance-2-fast") input.first_frame_url = imageUrl;
    else if (modelId === "bytedance/seedance-1.5-pro") input.input_urls = [imageUrl];
    // Kling on KIE takes `image_urls` (array of one URL) AND a
    // required boolean `sound`. The "This field is required" we
    // saw with image_urls alone was actually KIE complaining
    // about missing `sound`, not the image field. Confirmed via
    // KIE playground example body for kling-2.6/image-to-video.
    // We pass sound=false (no audio) — adding a user toggle later
    // is straightforward if we want Kling's built-in audio gen.
    else if (modelId === "kling-2.6/image-to-video") {
      input.image_urls = [imageUrl];
      input.sound = false;
    } else if (modelId === "kling-3.0/video") {
      input.image_urls = [imageUrl];
      input.sound = false;
    }
    else input.image_url = imageUrl;
  }

  const res = await kieRequest<KieTaskResponse>("/api/v1/jobs/createTask", {
    method: "POST",
    body: JSON.stringify({ model: modelId, input }),
  }, apiKey);
  if (res.code !== 200) {
    // KIE often returns "This field is required" without naming the
    // field, which makes it impossible to fix without seeing what we
    // actually sent. Log the full submit body (truncated to 600 chars
    // so a giant prompt doesn't dominate the log) so the missing
    // field is obvious from the worker stdout.
    console.error(`[kie] submit rejected modelId=${modelId} msg="${res.msg}" body=${JSON.stringify({ model: modelId, input }).slice(0, 600)}`);
    throw new Error(res.msg ?? "Failed to submit video job");
  }
  return res.data.taskId;
}

export async function pollVideoJob(
  taskId: string,
  modelId: string,
  apiKey: string
): Promise<{ status: "pending" | "processing" | "done" | "failed"; videoUrl?: string; error?: string; creditsConsumed?: number }> {

  // Veo
  if (modelId === "veo3" || modelId === "veo3_fast") {
    const data = await kieRequest<KieRecordResponse>(`/api/v1/veo/record-info?taskId=${taskId}`, {}, apiKey);
    const flag = data.data?.successFlag;
    const creditsConsumed = typeof data.data?.creditsConsumed === "number" ? data.data.creditsConsumed : undefined;
    if (flag === 1) return { status: "done", videoUrl: data.data?.videoUrl ?? (typeof data.data?.resultJson === "string" ? data.data.resultJson : undefined), creditsConsumed };
    if (flag === 2 || flag === 3) {
      const reason = extractFailureReason(data.data);
      console.log(`[kie] Veo failed taskId=${taskId} flag=${flag} reason=${reason ?? "(none)"} keys=${Object.keys(data.data ?? {}).join(",")}`);
      return { status: "failed", error: reason ?? "", creditsConsumed };
    }
    // Veo's recordInfo only exposes the terminal successFlag (1/2/3).
    // There's no API signal that says "actively generating now" vs
    // "queued in KIE's internal queue" — so we default to pending,
    // which keeps the beat in "submitting" until KIE returns a real
    // terminal state. Better to under-claim than to mislabel.
    return { status: "pending" };
  }

  // Runway
  if (modelId === "runway") {
    const data = await kieRequest<KieRecordResponse>(`/api/v1/runway/record-detail?taskId=${taskId}`, {}, apiKey);
    const d = data.data;
    const raw = (d?.state ?? "").toLowerCase();
    const creditsConsumed = typeof d?.creditsConsumed === "number" ? d.creditsConsumed : undefined;
    if (raw === "success") return { status: "done", videoUrl: d?.videoInfo?.videoUrl, creditsConsumed };
    if (raw === "fail") {
      const reason = extractFailureReason(d);
      console.log(`[kie] Runway failed taskId=${taskId} state=${raw} reason=${reason ?? "(none)"} keys=${Object.keys(d ?? {}).join(",")}`);
      return { status: "failed", error: reason ?? "", creditsConsumed };
    }
    return { status: raw === "generating" ? "processing" : "pending" };
  }

  // Generic
  const data = await kieRequest<KieRecordResponse>(`/api/v1/jobs/recordInfo?taskId=${taskId}`, {}, apiKey);
  const d = data.data;
  const raw = (d?.state ?? d?.status ?? "").toLowerCase();

  console.log(`[kie] poll taskId=${taskId} raw_state="${raw}" keys=${Object.keys(d ?? {}).join(",")}`);

  const DONE = ["succeed", "success", "completed", "done", "finish", "finished", "complete"];
  const FAIL = ["failed", "error", "fail"];
  // Only states where KIE is ACTIVELY producing the video map to
  // "processing". States like "queued" / "waiting" / "pending" mean
  // the job is sitting in KIE's internal queue but hasn't started
  // generation yet — those map to "pending" so the worker can keep
  // the beat in "submitting" instead of prematurely promoting it
  // to "rendering". (Previously this list lumped them all together,
  // which meant the badge flipped to "rendering" the moment KIE
  // acknowledged the job, even if no work was actually happening.)
  const PROCESSING = ["generating", "running", "processing", "active", "in_progress"];
  const PENDING = ["pending", "queued", "waiting", "created", "submitted"];

  let jobStatus: "pending" | "processing" | "done" | "failed" = "pending";
  if (DONE.includes(raw)) jobStatus = "done";
  else if (FAIL.includes(raw)) jobStatus = "failed";
  else if (PROCESSING.includes(raw)) jobStatus = "processing";
  else if (PENDING.includes(raw)) jobStatus = "pending";

  const creditsConsumed = typeof d?.creditsConsumed === "number" ? d.creditsConsumed : undefined;

  let videoUrl: string | undefined;
  if (jobStatus === "done") {
    // Check top-level videoUrl first
    if (typeof d?.videoUrl === "string" && d.videoUrl.startsWith("http")) videoUrl = d.videoUrl;
    // Check resultJson
    if (!videoUrl && typeof d?.resultJson === "string") {
      try {
        const parsed = JSON.parse(d.resultJson) as { resultUrls?: string[]; url?: string; videoUrl?: string; video_url?: string };
        videoUrl = parsed.resultUrls?.[0] ?? parsed.videoUrl ?? parsed.video_url ?? parsed.url;
      } catch {
        if (d.resultJson.startsWith("http")) videoUrl = d.resultJson;
      }
    }
    if (!videoUrl && Array.isArray(d?.output)) videoUrl = (d.output as string[]).find((u) => u.startsWith("http"));
    if (!videoUrl && typeof d?.output === "string" && d.output.startsWith("http")) videoUrl = d.output;

    if (!videoUrl) console.warn(`[kie] Done but no videoUrl found. data=${JSON.stringify(d)}`);
  }

  if (jobStatus === "failed") {
    const reason = extractFailureReason(d);
    if (!reason) {
      // Log the entire data block so we can see what KIE actually sent
      // when none of the known fields had a reason. Helps add new
      // candidates to extractFailureReason if KIE introduces a field.
      console.log(`[kie] Generic failed taskId=${taskId} state=${raw} keys=${Object.keys(d ?? {}).join(",")} data=${JSON.stringify(d).slice(0, 600)}`);
    }
    return { status: jobStatus, videoUrl, error: reason ?? "", creditsConsumed };
  }

  return { status: jobStatus, videoUrl, creditsConsumed };
}
