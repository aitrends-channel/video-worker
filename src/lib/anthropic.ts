import Anthropic from "@anthropic-ai/sdk";
import { supabase } from "./supabase.js";

const KIE_CLAUDE_BASE_URL = "https://api.kie.ai/claude";

/**
 * Mirror of engine/lib/claude/routing.ts so the worker honors the same
 * admin Config → Anthropic routing toggle:
 *
 *   client_kie    – per-user KIE key (default, current worker behavior)
 *   heclus_kie    – Heclus's KIE key (product_config service='heclus_kie_api_key')
 *   heclus_direct – direct Anthropic call w/ product_config service='anthropic_api_key'
 *
 * Keeping the two implementations parallel rather than sharing a package
 * because the engine + worker are deployed independently and dragging a
 * shared module across both adds release-coupling we don't want yet.
 */
export type AnthropicRouting = "client_kie" | "heclus_kie" | "heclus_direct";

export async function getAnthropicRouting(): Promise<AnthropicRouting> {
  const { data } = await supabase
    .from("product_config")
    .select("anthropic_routing")
    .eq("service", "_global")
    .single();
  const v = (data?.anthropic_routing ?? null) as string | null;
  if (v === "heclus_kie" || v === "heclus_direct") return v;
  return "client_kie";
}

export async function getActiveProductKey(service: string): Promise<string | null> {
  const { data } = await supabase
    .from("product_config")
    .select("keys, current_index, active")
    .eq("service", service)
    .single();
  if (!data || data.active === false) return null;
  const keys = (data.keys ?? []) as string[];
  if (!keys.length) return null;
  const idx = data.current_index ?? 0;
  return keys[Math.min(idx, keys.length - 1)] ?? null;
}

// ── KIE fetch wrapper (envelope unwrap + UA neutralizer) ─────────────────────
// Direct port of engine/lib/claude/client.ts fetchViaKie. See that file for
// the full explanation of (1) UA block, (2) envelope on failure, (3)
// envelope on success.

function rebuild(upstream: Response, body: string): Response {
  const headers = new Headers();
  upstream.headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower === "content-encoding" || lower === "content-length" || lower === "transfer-encoding") return;
    headers.set(name, value);
  });
  headers.set("content-type", upstream.headers.get("content-type") ?? "application/json");
  return new Response(body, { status: upstream.status, statusText: upstream.statusText, headers });
}

const fetchViaKie: typeof fetch = async (input, init) => {
  const headers = new Headers(init?.headers);
  headers.set("User-Agent", "heclus-worker/1.0");
  const upstream = await fetch(input, { ...init, headers });
  const text = await upstream.text();

  if (!upstream.ok) return rebuild(upstream, text);

  let parsed: unknown;
  try { parsed = JSON.parse(text); } catch { return rebuild(upstream, text); }

  const isEnvelope =
    parsed !== null &&
    typeof parsed === "object" &&
    "code" in parsed &&
    typeof (parsed as { code: unknown }).code === "number" &&
    ("data" in parsed || "msg" in parsed);

  if (!isEnvelope) return rebuild(upstream, text);

  const env = parsed as { code: number; msg?: string; data?: unknown };

  if (env.code >= 400) {
    const message = env.msg || `KIE error ${env.code}`;
    const body = JSON.stringify({
      type: "error",
      message,
      error: { type: "api_error", message },
    });
    return new Response(body, {
      status: env.code,
      statusText: "KIE error",
      headers: { "content-type": "application/json" },
    });
  }

  return new Response(JSON.stringify(env.data ?? {}), {
    status: 200,
    statusText: "OK",
    headers: { "content-type": "application/json" },
  });
};

// ── Settings helper (per-user) ───────────────────────────────────────────────

async function getUserKieKey(userId: string): Promise<string | null> {
  const { data } = await supabase
    .from("account_settings")
    .select("kie_api_key")
    .eq("user_id", userId)
    .single();
  return (data?.kie_api_key?.trim() as string | undefined) || null;
}

// ── Public client factory ────────────────────────────────────────────────────

export async function getAnthropicClient(userId: string): Promise<Anthropic> {
  const routing = await getAnthropicRouting();

  if (routing === "heclus_direct") {
    const key = await getActiveProductKey("anthropic_api_key");
    if (!key) {
      throw new Error("Heclus Anthropic key not configured — set one in Config → API Keys (service: Anthropic API Key (direct)).");
    }
    return new Anthropic({ apiKey: key, maxRetries: 0, timeout: 180_000 });
  }

  let kieKey: string | null = null;
  if (routing === "heclus_kie") {
    kieKey = await getActiveProductKey("heclus_kie_api_key");
    if (!kieKey) {
      throw new Error("Heclus KIE key not configured — set one in Config → API Keys (service: Heclus KIE API Key).");
    }
  } else {
    kieKey = await getUserKieKey(userId);
    if (!kieKey) throw new Error("KIE API key not configured for this user.");
  }

  return new Anthropic({
    apiKey: null,
    authToken: kieKey,
    baseURL: KIE_CLAUDE_BASE_URL,
    fetch: fetchViaKie,
    maxRetries: 0,
    timeout: 180_000,
  });
}
