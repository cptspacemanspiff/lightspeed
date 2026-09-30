import { z } from "zod";
import { AGENT_MODEL_API_KINDS } from "./model-defaults.js";

/// Model tiers: names such as `default`, `fast`, or `grader` that resolve to
/// a concrete provider route through layered tables. The universe table is
/// the general layer; a profile or session `models` table overrides it by
/// name. This mirrors the runtime's resolver for previews and the demo.

export const MODEL_TIER_DEFAULT = "default";
export const MODEL_TIER_NAME_PATTERN = /^[A-Za-z0-9._-]{1,64}$/;
export const MODEL_TIER_TABLE_MAX_ENTRIES = 64;

export interface ModelRoute {
  providerId: string;
  apiKind: string;
  model: string;
}

export interface ModelTierRoute {
  model: ModelRoute;
  reasoningEffort?: string | null;
  maxOutputTokens?: number | null;
}

export interface ModelTierAlias {
  tier: string;
}

export type ModelTierBinding = ModelTierRoute | ModelTierAlias;
export type ModelTierTable = Record<string, ModelTierBinding>;

export interface ModelTierUniverse {
  agentRun?: ModelRoute | null;
  tiers?: ModelTierTable | null;
}

export type ModelTierLayer = "universe" | "config";

export interface ModelTierResolution {
  tier: string;
  layer: ModelTierLayer;
  chain: string[];
  route: ModelTierRoute;
}

const routeName = z.string().min(1).refine(
  (value) => value.trim() === value && new TextEncoder().encode(value).length <= 512,
  "Use 1–512 bytes without surrounding whitespace.",
);
const tierName = z.string().regex(MODEL_TIER_NAME_PATTERN, "Use 1–64 letters, digits, '-', '_', or '.'.");

export const modelRouteSchema = z.object({
  providerId: routeName,
  apiKind: z.enum(AGENT_MODEL_API_KINDS),
  model: routeName,
}).strict();

export const modelTierRouteSchema = z.object({
  model: modelRouteSchema,
  reasoningEffort: z.string().min(1).max(64).optional(),
  maxOutputTokens: z.number().int().min(1).optional(),
}).strict();

export const modelTierAliasSchema = z.object({ tier: tierName }).strict();

export const modelTierBindingSchema = z.union([modelTierAliasSchema, modelTierRouteSchema]);

export const modelTierTableSchema = z.record(tierName, modelTierBindingSchema).refine(
  (table) => Object.keys(table).length <= MODEL_TIER_TABLE_MAX_ENTRIES,
  `A tier table holds at most ${MODEL_TIER_TABLE_MAX_ENTRIES} tiers.`,
);

/// `models/tiers/put`: the whole universe table under the defaults revision.
export const modelTiersPutSchema = z.object({
  tiers: modelTierTableSchema.refine(
    (table) => !(MODEL_TIER_DEFAULT in table),
    { message: "The universe `default` tier is the agent-run default; set it under Defaults.", path: [MODEL_TIER_DEFAULT] },
  ),
  expectedRevision: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
}).strict();

/// `models/tiers/resolve`: an optional profile and an optional extra table.
export const modelTiersResolveSchema = z.object({
  profile: z.union([
    z.object({ kind: z.literal("named"), profileId: z.string().min(1) }).strict(),
    z.object({ kind: z.literal("inline"), profile: z.record(z.string(), z.unknown()) }).strict(),
  ]).optional(),
  models: modelTierTableSchema.optional(),
}).strict();

export function isModelTierAlias(binding: ModelTierBinding): binding is ModelTierAlias {
  return "tier" in binding;
}

/// A model field is either a route or a tier reference.
export function modelTierRef(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const tier = (value as { tier?: unknown }).tier;
  return typeof tier === "string" && !("providerId" in value) ? tier : null;
}

/// The universe layer's binding for a name; `default` is the agent-run
/// default, and `undefined` means the name is unknown there.
function universeBinding(universe: ModelTierUniverse, name: string): ModelTierBinding | null | undefined {
  if (name === MODEL_TIER_DEFAULT) return universe.agentRun ? { model: universe.agentRun } : null;
  return universe.tiers?.[name];
}

/// Resolves a tier name the way the runtime does: the most specific layer
/// that defines the name wins, an alias resolves in its own layer or a more
/// general one, and an alias to its own name reaches through to the next
/// more general layer. Throws with the runtime's wording for unknown names,
/// cycles, and an unset default.
export function resolveModelTier(
  universe: ModelTierUniverse,
  config: ModelTierTable | undefined,
  name: string,
): ModelTierResolution {
  if (!MODEL_TIER_NAME_PATTERN.test(name)) throw new Error(`model tier name ${JSON.stringify(name)} may contain only ASCII letters, digits, '-', '_', or '.'`);
  const chain = [name];
  const visited = new Set<string>();
  let current = name;
  let floor: ModelTierLayer = "config";
  let layer: ModelTierLayer | undefined;
  for (;;) {
    let found: ModelTierLayer | undefined;
    let binding: ModelTierBinding | null | undefined;
    if (floor === "config" && config && Object.hasOwn(config, current)) {
      found = "config";
      binding = config[current];
    } else {
      binding = universeBinding(universe, current);
      if (binding !== undefined) found = "universe";
    }
    if (binding === null) {
      throw Object.assign(new Error("no universe model default is configured for agentRun; choose a model or set it with models/defaults/put"), { kind: "model_default_unset" });
    }
    if (!found || binding === undefined) {
      throw new Error(chain.length === 1
        ? `unknown model tier ${JSON.stringify(name)}`
        : `model tier ${JSON.stringify(name)} does not resolve: ${chain.join(" -> ")} names unknown tier ${JSON.stringify(current)}`);
    }
    layer ??= found;
    const key = `${found}:${current}`;
    if (visited.has(key)) throw new Error(`model tier ${JSON.stringify(name)} does not resolve: alias cycle ${chain.join(" -> ")}`);
    visited.add(key);
    if (!isModelTierAlias(binding)) return { tier: name, layer, chain, route: binding };
    if (binding.tier === current) {
      if (found === "universe") throw new Error(`model tier ${JSON.stringify(name)} does not resolve: universe tier ${JSON.stringify(current)} aliases itself`);
      floor = "universe";
    } else {
      floor = found;
    }
    chain.push(binding.tier);
    current = binding.tier;
  }
}

/// Every name a configuration can use: its own table first, then the
/// universe with `default` in front.
export function modelTierNames(universe: ModelTierUniverse, config?: ModelTierTable): Array<{ tier: string; layer: ModelTierLayer }> {
  const seen = new Set<string>();
  const names: Array<{ tier: string; layer: ModelTierLayer }> = [];
  for (const tier of Object.keys(config ?? {})) if (!seen.has(tier)) { seen.add(tier); names.push({ tier, layer: "config" }); }
  for (const tier of [MODEL_TIER_DEFAULT, ...Object.keys(universe.tiers ?? {})]) if (!seen.has(tier)) { seen.add(tier); names.push({ tier, layer: "universe" }); }
  return names;
}

/// Merges a specific table over a general one by name, as an explicit
/// session configuration merges over its profile.
export function mergeModelTierTables(general?: ModelTierTable, specific?: ModelTierTable): ModelTierTable | undefined {
  if (!general && !specific) return undefined;
  return { ...(general ?? {}), ...(specific ?? {}) };
}
