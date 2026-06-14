import { supabase } from "./supabase.js";

/**
 * Worker-side mirror of youtube-engine/lib/costs.ts. The two files
 * must stay in sync on schema — both write to the same project_costs
 * table — but each lives in its own service so the worker doesn't
 * have to depend on the app codebase.
 *
 * Fail-soft: a logging failure must never crash a beat generation
 * the user already paid KIE for.
 */
export type CostStep =
  | "channel_analysis"
  | "topic"
  | "script"
  | "visuals"
  | "prompts_image"
  | "prompts_video"
  | "tts"
  | "image_gen"
  | "video_gen"
  | "assemble"
  | "thumbnail_concept"
  | "thumbnail_image";

export type CostUnitKind =
  | "claude_tokens_in"
  | "claude_tokens_out"
  | "claude_tokens_cache_read"
  | "claude_tokens_cache_creation"
  | "kie_credits"
  | "elevenlabs_chars"
  | "supadata_transcripts";

export interface CostEntry {
  projectId: string;
  userId: string;
  step: CostStep;
  provider: "anthropic" | "kie" | "elevenlabs" | "supadata";
  model?: string | null;
  units: number;
  unitKind: CostUnitKind;
}

export async function logProjectCost(entry: CostEntry): Promise<void> {
  if (!entry.units || entry.units <= 0) return;
  if (!entry.projectId || !entry.userId) return;
  try {
    const { error } = await supabase
      .from("project_costs")
      .insert({
        project_id: entry.projectId,
        user_id: entry.userId,
        step: entry.step,
        provider: entry.provider,
        model: entry.model ?? null,
        units: entry.units,
        unit_kind: entry.unitKind,
      });
    if (error) {
      console.warn(`[costs] insert failed step=${entry.step} provider=${entry.provider} unit_kind=${entry.unitKind}:`, error.message);
    }
  } catch (e) {
    console.warn(`[costs] insert threw step=${entry.step}:`, e instanceof Error ? e.message : e);
  }
}
