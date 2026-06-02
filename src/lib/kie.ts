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
    failReason?: string;
    error?: string;
    videoInfo?: { videoUrl?: string };
    successFlag?: number;
    videoUrl?: string;
    video_url?: string;
    [key: string]: unknown;
  };
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
    if (flag === 1) return { status: "done", videoUrl: data.data?.videoUrl ?? (typeof data.data?.resultJson === "string" ? data.data.resultJson : undefined) };
    if (flag === 2 || flag === 3) return { status: "failed", error: "Veo generation failed" };
    return { status: "processing" };
  }

  // Runway
  if (modelId === "runway") {
    const data = await kieRequest<KieRecordResponse>(`/api/v1/runway/record-detail?taskId=${taskId}`, {}, apiKey);
    const d = data.data;
    const raw = (d?.state ?? "").toLowerCase();
    if (raw === "success") return { status: "done", videoUrl: d?.videoInfo?.videoUrl };
    if (raw === "fail") return { status: "failed", error: d?.failReason ?? "Runway job failed" };
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

  return {
    status: jobStatus,
    videoUrl,
    error: jobStatus === "failed" ? (d?.failReason ?? d?.error ?? "Video generation failed") : undefined,
  };
}
