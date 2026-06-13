const KIE_BASE_URL = "https://api.kie.ai";

async function kieRequest<T>(endpoint: string, options: RequestInit, apiKey: string): Promise<T> {
  const res = await fetch(`${KIE_BASE_URL}${endpoint}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(options.headers ?? {}),
    },
  });
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
    const body: Record<string, unknown> = { prompt };
    if (!imageUrl) body.aspect_ratio = aspectRatio;
    if (imageUrl) body.imageUrls = [imageUrl];
    const res = await kieRequest<KieTaskResponse>("/api/v1/veo/generate", {
      method: "POST",
      body: JSON.stringify(body),
    }, apiKey);
    if (res.code !== 200) throw new Error(res.msg ?? "Failed to submit Veo job");
    return res.data.taskId;
  }

  // Runway
  if (modelId === "runway") {
    const body: Record<string, unknown> = { prompt };
    if (!imageUrl) body.aspectRatio = aspectRatio;
    if (duration) body.duration = duration;
    if (imageUrl) body.imageUrl = imageUrl;
    const res = await kieRequest<KieTaskResponse>("/api/v1/runway/generate", {
      method: "POST",
      body: JSON.stringify(body),
    }, apiKey);
    if (res.code !== 200) throw new Error(res.msg ?? "Failed to submit Runway job");
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
    else if (modelId === "wan/2-6-flash-image-to-video") input.image_urls = [imageUrl];
    else if (modelId === "sora-2-image-to-video") input.image_urls = [imageUrl];
    else if (modelId === "bytedance/seedance-2-fast") input.first_frame_url = imageUrl;
    else if (modelId === "bytedance/seedance-1.5-pro") input.input_urls = [imageUrl];
    else input.image_url = imageUrl;
  }

  const res = await kieRequest<KieTaskResponse>("/api/v1/jobs/createTask", {
    method: "POST",
    body: JSON.stringify({ model: modelId, input }),
  }, apiKey);
  if (res.code !== 200) throw new Error(res.msg ?? "Failed to submit video job");
  return res.data.taskId;
}

export async function pollVideoJob(
  taskId: string,
  modelId: string,
  apiKey: string
): Promise<{ status: "pending" | "processing" | "done" | "failed"; videoUrl?: string; error?: string }> {

  // Veo
  if (modelId === "veo3" || modelId === "veo3_fast") {
    const data = await kieRequest<KieRecordResponse>(`/api/v1/veo/record-info?taskId=${taskId}`, {}, apiKey);
    const flag = data.data?.successFlag;
    if (flag === 1 || flag === 2 || flag === 3) {
      // Cost recon — find which field carries credits consumed.
      console.log(`[kie-cost-recon] video-veo model=${modelId} flag=${flag} response=`, JSON.stringify(data));
    }
    if (flag === 1) return { status: "done", videoUrl: data.data?.videoUrl ?? (typeof data.data?.resultJson === "string" ? data.data.resultJson : undefined) };
    if (flag === 2 || flag === 3) {
      const reason = extractFailureReason(data.data);
      console.log(`[kie] Veo failed taskId=${taskId} flag=${flag} reason=${reason ?? "(none)"} keys=${Object.keys(data.data ?? {}).join(",")}`);
      return { status: "failed", error: reason ?? "" };
    }
    return { status: "processing" };
  }

  // Runway
  if (modelId === "runway") {
    const data = await kieRequest<KieRecordResponse>(`/api/v1/runway/record-detail?taskId=${taskId}`, {}, apiKey);
    const d = data.data;
    const raw = (d?.state ?? "").toLowerCase();
    if (raw === "success" || raw === "fail") {
      console.log(`[kie-cost-recon] video-runway model=${modelId} state=${raw} response=`, JSON.stringify(data));
    }
    if (raw === "success") return { status: "done", videoUrl: d?.videoInfo?.videoUrl };
    if (raw === "fail") {
      const reason = extractFailureReason(d);
      console.log(`[kie] Runway failed taskId=${taskId} state=${raw} reason=${reason ?? "(none)"} keys=${Object.keys(d ?? {}).join(",")}`);
      return { status: "failed", error: reason ?? "" };
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
  const PROCESSING = ["generating", "running", "processing", "active", "in_progress", "queued", "waiting"];

  let jobStatus: "pending" | "processing" | "done" | "failed" = "pending";
  if (DONE.includes(raw)) jobStatus = "done";
  else if (FAIL.includes(raw)) jobStatus = "failed";
  else if (PROCESSING.includes(raw)) jobStatus = "processing";

  if (jobStatus === "done" || jobStatus === "failed") {
    // Reconnaissance log for cost tracking — dumps the full KIE
    // recordInfo payload at terminal state so we can identify
    // which field carries credits consumed. Fires once per task.
    console.log(`[kie-cost-recon] video-generic model=${modelId} verdict=${jobStatus} response=`, JSON.stringify(data));
  }

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
    return { status: jobStatus, videoUrl, error: reason ?? "" };
  }

  return { status: jobStatus, videoUrl };
}
