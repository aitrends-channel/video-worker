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
    const body = await res.text().catch(() => "");
    // KIE sits behind Cloudflare. A 5xx (502/503/504) comes back as a
    // full HTML error page — kilobytes of markup. Throwing that verbatim
    // floods the worker logs and, on the submit path, lands the whole
    // page in the beat's video_error column. Collapse an HTML body to a
    // short marker and hard-cap anything else so only the useful (usually
    // small JSON) error text survives. The poll loop already treats this
    // throw as a transient network failure and retries.
    const looksHtml = /^\s*<(?:!doctype|html|head|body)/i.test(body);
    const snippet = looksHtml
      ? "(HTML error page — likely a Cloudflare/upstream outage)"
      : body.replace(/\s+/g, " ").trim().slice(0, 300);
    throw new Error(`kie.ai error ${res.status} on ${endpoint}${snippet ? `: ${snippet}` : ""}`);
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
    // Veo's /veo/record-info nests the result under a `response` object:
    //   data.response.resultUrls[0]  (base render)
    //   data.response.fullResultUrls / full_result_urls (upscaled variants)
    // None of the flat fields above are ever populated for Veo, so
    // without reading this the poll never finds the URL and times out.
    response?: {
      resultUrls?: string[] | null;
      fullResultUrls?: string[] | null;
      full_result_urls?: string[] | null;
      originUrls?: string[] | null;
    };
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

// Some models expose their output-size knob under a name that isn't
// "resolution". This map is the WORKER'S copy of the resolutionKey
// values declared in youtube-engine/lib/kie/videoModels.ts — every
// entry there with a non-default resolutionKey MUST be mirrored here,
// otherwise the worker sends the picker's chosen value under the wrong
// input field name and KIE silently defaults to the model's cheapest
// tier (the "picked 4K, got 720p" bug class).
//
// Current coverage:
//   - kling-3.0/video   → input.mode      (std / pro / 4K)
//   - runway            → body.quality    — handled inline in Runway's
//                                            branch above; NOT read from
//                                            this map because Runway
//                                            uses a different endpoint
//                                            entirely (/api/v1/runway/
//                                            generate) with camelCase
//                                            top-level fields, not the
//                                            createTask input object.
//   - everything else   → input.resolution (default fallback below)
//
// When adding a new video model in videoModels.ts, either verify its
// resolutionKey is "resolution" (default here — do nothing) or add it
// to this map at the same time.
const MODEL_RESOLUTION_KEYS: Record<string, string> = {
  "kling-3.0/video": "mode",
};

export async function submitVideoJob(
  prompt: string,
  modelId: string,
  apiKey: string,
  imageUrl?: string,
  duration?: string | number,
  aspectRatio = "16:9",
  resolution?: string,
  callBackUrl?: string,
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
    if (duration !== undefined) body.duration = duration;
    if (resolution) body.resolution = resolution;
    if (callBackUrl) body.callBackUrl = callBackUrl;
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
    // field — "Video quality cannot be empty". The picker now sends
    // the user's chosen tier through `resolution`; default to 720p
    // when the caller didn't pick one so this branch stays valid.
    //
    // A null resolution at this point should be rare — the picker
    // defaults selectedVideoResolution to the first supported value
    // on every model change, and the resolution pill is set-only
    // (not toggle) as of the Bug 1 fix. If we see this warn line in
    // production it means either a legacy beat submitted before those
    // fixes or a race in the model-change effect — worth surfacing
    // rather than hiding as a silent 720p downgrade.
    if (!resolution) {
      console.warn(`[kie] runway submit with no resolution, defaulting to 720p (would have KIE-rejected without a value)`);
    }
    const body: Record<string, unknown> = { prompt, quality: resolution ?? "720p" };
    if (!imageUrl) body.aspectRatio = aspectRatio;
    if (duration) body.duration = duration;
    if (imageUrl) body.imageUrl = imageUrl;
    if (callBackUrl) body.callBackUrl = callBackUrl;
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
  // Generic resolution injection: covers grok-imagine, wan/2-7, wan/2-6-flash,
  // seedance-2, and seedance-2-fast which don't need any other tweaks. The
  // model-specific branches below (seedance-1.5-pro, kling-3.0) also set
  // resolution/mode inline — they will override this with the same value.
  if (resolution) {
    const key = MODEL_RESOLUTION_KEYS[modelId] ?? "resolution";
    input[key] = resolution;
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
    // Both Seedance 2 variants (non-fast and fast) take first_frame_url
    // per KIE docs — the fast variant is the only visual difference to
    // the user, but the payload shape is identical. Non-fast used to
    // fall through to the default input.image_url branch below, which
    // KIE rejected with a "Video model rejected" or a
    // "field is required" reply, causing the Seedance 2 4K path to
    // fail without a clear signal to the user.
    else if (modelId === "bytedance/seedance-2" || modelId === "bytedance/seedance-2-fast") {
      input.first_frame_url = imageUrl;
    }
    else if (modelId === "bytedance/seedance-1.5-pro") {
      // Seedance 1.5 Pro on KIE needs input_urls + a stack of
      // required scalars (resolution, fixed_lens, generate_audio,
      // nsfw_checker, aspect_ratio) that all surface as
      // "This field is required" if missing. Confirmed via the
      // KIE playground example body.
      //   fixed_lens     = false  (allow camera motion)
      //   generate_audio = false  (we add audio downstream via TTS)
      //   nsfw_checker   = false  (don't auto-block legitimate content)
      // input.resolution was previously set here with a "720p" default;
      // now it's covered by the generic resolution injection above,
      // which uses the picker's selection when available. If the
      // picker didn't pass a value (rare — only happens on legacy
      // beats predating migration 090), KIE errors with
      // "field is required" — that's the correct failure mode
      // instead of silently downgrading to 720p.
      // aspect_ratio is required EVEN with input_urls present —
      // the generic if-no-image branch above misses this case for
      // Seedance, so we set it here explicitly.
      input.input_urls = [imageUrl];
      input.aspect_ratio = aspectRatio;
      input.fixed_lens = false;
      input.generate_audio = false;
      input.nsfw_checker = false;
    }
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
      // Kling 3.0 needs everything 2.6 wants plus aspect_ratio and
      // multi_shots. multi_shots controls Kling's storyboard-style
      // multi-segment feature; we pass false to use the single-prompt
      // path. KIE rejects with "multi_shots cannot be empty" if the
      // field is missing entirely, so it's effectively required even
      // when off. aspect_ratio is required even with image_urls
      // present, unlike most other KIE models where it only matters
      // for text-to-video.
      //
      // input.mode was previously set here with a "std" default; now
      // it's covered by the generic resolution injection above via
      // MODEL_RESOLUTION_KEYS["kling-3.0/video"] = "mode". If the
      // picker didn't pass a value, KIE errors with
      // "field is required" — that's the correct failure mode
      // instead of silently downgrading to std tier.
      input.image_urls = [imageUrl];
      input.sound = false;
      input.aspect_ratio = aspectRatio;
      input.multi_shots = false;
    }
    else input.image_url = imageUrl;
  }

  const res = await kieRequest<KieTaskResponse>("/api/v1/jobs/createTask", {
    method: "POST",
    body: JSON.stringify({ model: modelId, input, ...(callBackUrl ? { callBackUrl } : {}) }),
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

// Pull a playable http(s) URL out of a KIE record-info payload, tolerating
// the several shapes different model families use: a direct videoUrl, a
// resultJson blob (JSON with resultUrls/url/videoUrl/video_url, or a bare
// http string), or an output array/string. Returns undefined when no
// usable URL is present yet — callers MUST treat that as "not ready"
// (keep polling), never as a completed job.
function extractVideoUrl(d: KieRecordResponse["data"] | undefined): string | undefined {
  if (!d) return undefined;
  if (typeof d.videoUrl === "string" && d.videoUrl.startsWith("http")) return d.videoUrl;
  if (typeof d.video_url === "string" && d.video_url.startsWith("http")) return d.video_url;
  if (typeof d.videoInfo?.videoUrl === "string" && d.videoInfo.videoUrl.startsWith("http")) return d.videoInfo.videoUrl;
  // Veo shape: data.response.{resultUrls|fullResultUrls|full_result_urls}[0].
  const resp = d.response;
  if (resp) {
    const fromResponse =
      resp.resultUrls?.[0] ?? resp.fullResultUrls?.[0] ?? resp.full_result_urls?.[0];
    if (typeof fromResponse === "string" && fromResponse.startsWith("http")) return fromResponse;
  }
  if (typeof d.resultJson === "string") {
    try {
      const parsed = JSON.parse(d.resultJson) as { resultUrls?: string[]; url?: string; videoUrl?: string; video_url?: string };
      const u = parsed.resultUrls?.[0] ?? parsed.videoUrl ?? parsed.video_url ?? parsed.url;
      if (typeof u === "string" && u.startsWith("http")) return u;
    } catch {
      if (d.resultJson.startsWith("http")) return d.resultJson;
    }
  }
  if (Array.isArray(d.output)) { const u = (d.output as string[]).find((x) => typeof x === "string" && x.startsWith("http")); if (u) return u; }
  if (typeof d.output === "string" && d.output.startsWith("http")) return d.output;
  return undefined;
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
    if (flag === 1) {
      const url = extractVideoUrl(data.data);
      // Veo reports successFlag=1 for the base render, then runs a
      // SEPARATE upscale pass (e.g. 720p/1080p → 4K) before the final
      // URL is populated. Report "done" ONLY once a real URL exists;
      // otherwise stay "processing" and keep polling through the upscale.
      // Returning done here without a URL (and treating raw resultJson as
      // a URL) is what surfaced "completed on KIE but no url returned".
      if (url) return { status: "done", videoUrl: url, creditsConsumed };
      return { status: "processing", creditsConsumed };
    }
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
    videoUrl = extractVideoUrl(d);
    // A "done" state with no URL yet (a provider that flips state before
    // the asset is written, or a follow-up upscale still running) is
    // treated as still processing so the worker keeps polling rather than
    // hard-failing with "completed but no url".
    if (!videoUrl) {
      console.warn(`[kie] Done state but no videoUrl yet — treating as processing. data=${JSON.stringify(d).slice(0, 400)}`);
      jobStatus = "processing";
    }
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
