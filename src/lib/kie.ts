const KIE_BASE_URL = "https://api.kie.ai";

export async function submitVideoJob(
  prompt: string,
  modelId: string,
  apiKey: string,
  imageUrl?: string,
  duration?: string | number,
  aspectRatio?: string
): Promise<string> {
  const res = await fetch(`${KIE_BASE_URL}/video/generate`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      prompt,
      model_id: modelId,
      image_url: imageUrl,
      duration: duration ? String(duration) : undefined,
      aspect_ratio: aspectRatio,
    }),
  });

  if (!res.ok) {
    const error = await res.json().catch(() => ({})) as { error?: string };
    throw new Error(`KIE API error: ${error.error ?? res.statusText}`);
  }

  const data = await res.json() as { request_id: string };
  return data.request_id;
}

export async function pollVideoJob(
  jobId: string,
  modelId: string,
  apiKey: string
): Promise<{ status: string; videoUrl?: string; error?: string }> {
  const res = await fetch(`${KIE_BASE_URL}/video/status?request_id=${jobId}&model_id=${modelId}`, {
    headers: {
      "Authorization": `Bearer ${apiKey}`,
    },
  });

  if (!res.ok) {
    throw new Error(`KIE status check failed: ${res.statusText}`);
  }

  const data = await res.json() as {
    status: string;
    video_url?: string;
    error?: string;
  };

  return {
    status: data.status,
    videoUrl: data.video_url,
    error: data.error,
  };
}
