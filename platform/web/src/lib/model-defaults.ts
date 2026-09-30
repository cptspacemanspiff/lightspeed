import { useQuery } from "@tanstack/react-query";
import { MODEL_TIER_DEFAULT, modelTierRef, resolveModelTier, type ModelTierTable } from "@lightspeed/platform-shared";
import { api, type ModelConfig, type ModelDefaults, type ModelListResponse, type ModelRef } from "@/api";

export const modelDefaultsKey = (universeId: string) => ["model-defaults", universeId] as const;

export function useModelDefaults(universeId: string, enabled = true) {
  return useQuery({
    queryKey: modelDefaultsKey(universeId),
    queryFn: () => api<ModelDefaults>("GET", `/api/v1/universes/${universeId}/models/defaults`),
    enabled,
  });
}

export function useModelDiscovery(universeId: string, enabled = true) {
  return useQuery({
    queryKey: ["models", universeId],
    queryFn: () => api<ModelListResponse>("GET", `/api/v1/universes/${universeId}/models`),
    staleTime: 60_000,
    enabled,
  });
}

export function modelFromConfig(config: unknown): ModelConfig | null {
  if (!config || typeof config !== "object") return null;
  const model = (config as { model?: unknown }).model;
  if (!model || typeof model !== "object") return null;
  const route = model as Partial<ModelConfig>;
  return typeof route.providerId === "string" && typeof route.apiKind === "string" && typeof route.model === "string"
    ? route as ModelConfig : null;
}

/// The model field as written: a route, a tier reference, or nothing.
export function modelRefFromConfig(config: unknown): ModelRef | null {
  const route = modelFromConfig(config);
  if (route) return route;
  const tier = modelTierRef((config as { model?: unknown } | null | undefined)?.model);
  return tier ? { tier } : null;
}

/// The configuration-layer tier table a document carries, if any.
export function modelTableFromConfig(config: unknown): ModelTierTable | undefined {
  if (!config || typeof config !== "object") return undefined;
  const models = (config as { models?: unknown }).models;
  return models && typeof models === "object" && !Array.isArray(models) ? models as ModelTierTable : undefined;
}

export type CreationModelSource = "Session model" | "Profile model" | "Universe default" | `Tier ${string}`;

/// The model a new session will start with, the way the runtime resolves
/// it: an explicit route, the profile's, or the `default` tier; a tier
/// reference at either layer resolves through the profile's own table and
/// the universe. `error` names a tier that does not resolve.
export function resolveCreationModel(
  explicit: ModelRef | null | undefined,
  profile: ModelRef | null | undefined,
  defaults: ModelDefaults | undefined,
  models?: ModelTierTable,
): { model: ModelConfig | null; source: CreationModelSource; chain?: string[]; error?: string } {
  const resolveTier = (tier: string): { model: ModelConfig | null; source: CreationModelSource; chain?: string[]; error?: string } => {
    if (!defaults) return { model: null, source: `Tier ${tier}` };
    try {
      const resolved = resolveModelTier(defaults, models, tier);
      return { model: resolved.route.model, source: `Tier ${tier}`, chain: resolved.chain };
    } catch (error) {
      return { model: null, source: `Tier ${tier}`, error: error instanceof Error ? error.message : "tier does not resolve" };
    }
  };
  if (explicit && "tier" in explicit) return resolveTier(explicit.tier);
  if (explicit) return { model: explicit, source: "Session model" };
  if (profile && "tier" in profile) return resolveTier(profile.tier);
  if (profile) return { model: profile, source: "Profile model" };
  if (models && MODEL_TIER_DEFAULT in models) return { ...resolveTier(MODEL_TIER_DEFAULT), source: "Universe default" };
  return { model: defaults?.agentRun ?? null, source: "Universe default" };
}

export function modelLabel(model: ModelConfig) {
  const provider = model.providerId === "openai" ? "OpenAI" : model.providerId === "anthropic" ? "Anthropic" : model.providerId;
  return `${provider} · ${model.model}`;
}
