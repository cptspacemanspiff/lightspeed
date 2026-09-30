import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  AGENT_MODEL_API_KINDS,
  MODEL_TIER_DEFAULT,
  isModelTierAlias,
  modelTiersPutSchema,
  resolveModelTier,
  type ModelTierBinding,
  type ModelTierTable,
} from "@lightspeed/platform-shared";
import { api, ApiError, type ModelDefaults, type ModelTiersPutParams } from "@/api";
import { modelDefaultsKey, modelLabel, useModelDefaults, useModelDiscovery } from "@/lib/model-defaults";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ModelChoicePicker, type ModelChoice, type ModelPickerTier } from "@/components/models/model-choice-picker";

const apiLabels: Record<string, string> = {
  "openai:responses": "OpenAI Responses",
  "openai:completions": "OpenAI Chat Completions",
  "anthropic:messages": "Anthropic Messages",
};

/// The universe tier table: named roles such as `fast` or `grader` that
/// profiles reference instead of a concrete route. `default` is the
/// agent-run default from the Defaults section above.
export function ModelTiersSection({ universeId, writable, onEdit }: {
  universeId: string;
  writable: boolean;
  onEdit: (defaults: ModelDefaults, tier: string | null) => void;
}) {
  const defaults = useModelDefaults(universeId);
  const queryClient = useQueryClient();
  const remove = useMutation({
    mutationFn: ({ revision, tiers, tier }: { revision: number; tiers: ModelTierTable; tier: string }) => {
      const { [tier]: _removed, ...rest } = tiers;
      return api<ModelDefaults>("PUT", `/api/v1/universes/${universeId}/models/tiers`, {
        tiers: rest, expectedRevision: revision,
      } satisfies ModelTiersPutParams);
    },
    onSuccess: (value) => queryClient.setQueryData(modelDefaultsKey(universeId), value),
    onError: () => { void queryClient.invalidateQueries({ queryKey: modelDefaultsKey(universeId) }); },
  });
  const table = defaults.data?.tiers ?? {};
  const names = Object.keys(table);
  return (
    <section aria-labelledby="model-tiers-heading" className="mt-8">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 id="model-tiers-heading" className="text-sm font-semibold">Tiers</h2>
          <p className="text-xs text-muted-foreground">
            Named roles that profiles and sessions reference with <span className="font-mono">{"{ \"tier\": \"fast\" }"}</span>.
            A tier binds a route and its settings, or aliases another tier. Sessions keep the route they resolved.
          </p>
        </div>
        {writable && defaults.data && !defaults.error && (
          <Button variant="outline" size="sm" onClick={() => { remove.reset(); onEdit(defaults.data!, null); }}>Add tier</Button>
        )}
      </div>
      <div className="divide-y rounded-xl border">
        {defaults.isLoading && <p className="px-5 py-4 text-sm text-muted-foreground">Loading…</p>}
        {defaults.data && names.length === 0 && (
          <p className="px-5 py-4 text-sm text-muted-foreground">No tiers beyond <span className="font-mono">default</span>. Add one to give profiles a portable model role.</p>
        )}
        {defaults.data && names.map((name) => {
          const binding = table[name]!;
          const resolution = resolveTier(defaults.data!, name);
          return (
            <div key={name} role="group" aria-label={`Tier ${name}`} className="flex flex-wrap items-center justify-between gap-4 px-5 py-4">
              <div className="min-w-0 space-y-1">
                <h3 className="font-mono text-sm font-medium">{name}</h3>
                <p className="break-words text-sm">{bindingLabel(binding)}</p>
                <p className={`text-xs ${resolution.error ? "text-amber-700 dark:text-amber-400" : "text-muted-foreground"}`}>
                  {resolution.error ?? resolution.summary}
                </p>
              </div>
              {writable && !defaults.error && <div className="flex gap-2">
                <Button variant="outline" size="sm" disabled={remove.isPending} onClick={() => { remove.reset(); onEdit(defaults.data!, name); }}>Change</Button>
                <Button variant="ghost" size="sm" disabled={remove.isPending} onClick={() => remove.mutate({ revision: defaults.data!.revision, tiers: table, tier: name })}>{remove.isPending ? "Removing…" : "Remove"}</Button>
              </div>}
            </div>
          );
        })}
        {defaults.error && <div className="flex items-center gap-3 px-5 py-4 text-sm text-destructive" role="alert">
          <span>Could not load tiers: {defaults.error.message}</span>
          <Button variant="outline" size="sm" onClick={() => void defaults.refetch()}>Retry</Button>
        </div>}
        {remove.error && <p role="alert" className="px-5 pb-4 text-sm text-destructive">{remove.error instanceof ApiError && remove.error.status === 409 ? "Tiers changed elsewhere. Review the current table before removing a tier." : remove.error.message}</p>}
      </div>
    </section>
  );
}

function resolveTier(defaults: ModelDefaults, name: string): { summary?: string; error?: string } {
  try {
    const resolved = resolveModelTier(defaults, undefined, name);
    const settings = [
      resolved.route.reasoningEffort ? `reasoning ${resolved.route.reasoningEffort}` : null,
      resolved.route.maxOutputTokens ? `max output ${resolved.route.maxOutputTokens}` : null,
    ].filter(Boolean).join(", ");
    const chain = resolved.chain.length > 1 ? `${resolved.chain.join(" → ")} · ` : "";
    return { summary: `${chain}${apiLabels[resolved.route.model.apiKind] ?? resolved.route.model.apiKind}${settings ? ` · ${settings}` : ""}` };
  } catch (error) {
    return { error: error instanceof Error ? error.message : "Tier does not resolve." };
  }
}

function bindingLabel(binding: ModelTierBinding): string {
  if (isModelTierAlias(binding)) return `Alias for ${binding.tier}`;
  return modelLabel(binding.model);
}

type Draft = {
  name: string;
  /// Set when the tier aliases another; otherwise the route fields apply.
  alias: string | null;
  providerId: string;
  apiKind: string;
  model: string;
  reasoningEffort: string;
  maxOutputTokens: string;
};

function draftFor(name: string | null, binding: ModelTierBinding | undefined): Draft {
  const base: Draft = { name: name ?? "", alias: null, providerId: "", apiKind: AGENT_MODEL_API_KINDS[0], model: "", reasoningEffort: "", maxOutputTokens: "" };
  if (!binding) return base;
  if (isModelTierAlias(binding)) return { ...base, alias: binding.tier };
  return {
    ...base,
    providerId: binding.model.providerId,
    apiKind: binding.model.apiKind,
    model: binding.model.model,
    reasoningEffort: binding.reasoningEffort ?? "",
    maxOutputTokens: binding.maxOutputTokens ? String(binding.maxOutputTokens) : "",
  };
}

function bindingFor(draft: Draft): ModelTierBinding {
  if (draft.alias !== null) return { tier: draft.alias.trim() };
  const maxOutputTokens = Number(draft.maxOutputTokens);
  return {
    model: { providerId: draft.providerId.trim(), apiKind: draft.apiKind, model: draft.model.trim() },
    ...(draft.reasoningEffort.trim() ? { reasoningEffort: draft.reasoningEffort.trim() } : {}),
    ...(Number.isInteger(maxOutputTokens) && maxOutputTokens > 0 ? { maxOutputTokens } : {}),
  };
}

/// The picker's tier entries: every universe tier this one could alias,
/// with the route each resolves to.
function aliasTargets(defaults: ModelDefaults, table: ModelTierTable, own: string): ModelPickerTier[] {
  const universe = { ...defaults, tiers: table };
  return [MODEL_TIER_DEFAULT, ...Object.keys(table)]
    .filter((tier) => tier !== own)
    .map((tier) => {
      try {
        const resolved = resolveModelTier(universe, undefined, tier);
        const settings = resolved.route.reasoningEffort ? ` · ${resolved.route.reasoningEffort}` : "";
        return { tier, layer: "Universe", hint: `${modelLabel(resolved.route.model)}${settings}` };
      } catch (error) {
        return { tier, layer: "Universe", hint: error instanceof Error ? error.message : "does not resolve" };
      }
    });
}

/// Adds or changes one tier by replacing the whole table under its revision.
/// One picker offers the other tiers (an alias) and discovered models (a
/// route); a route typed by hand is a route too.
export function ModelTierDialog({ universeId, initial, tier, onClose }: {
  universeId: string;
  initial: ModelDefaults;
  tier: string | null;
  onClose: () => void;
}) {
  const [revision, setRevision] = useState(initial.revision);
  const [table, setTable] = useState<ModelTierTable>(initial.tiers ?? {});
  const [draft, setDraft] = useState<Draft>(() => draftFor(tier, tier ? initial.tiers?.[tier] : undefined));
  const [reloading, setReloading] = useState(false);
  const [reloadError, setReloadError] = useState<string | null>(null);
  const discovery = useModelDiscovery(universeId);
  const queryClient = useQueryClient();
  const id = useId();
  const name = draft.name.trim();
  const nextTable: ModelTierTable = { ...table };
  if (tier && tier !== name) delete nextTable[tier];
  nextTable[name] = bindingFor(draft);
  const params: ModelTiersPutParams = { tiers: nextTable, expectedRevision: revision };
  const parsed = modelTiersPutSchema.safeParse(params);
  let resolveError: string | null = null;
  if (parsed.success) {
    try {
      resolveModelTier({ agentRun: initial.agentRun ?? { providerId: "x", apiKind: "openai:responses", model: "x" }, tiers: nextTable }, undefined, name);
    } catch (error) {
      resolveError = error instanceof Error ? error.message : "Tier does not resolve.";
    }
  }
  const valid = parsed.success && !resolveError;
  const save = useMutation({
    mutationFn: () => api<ModelDefaults>("PUT", `/api/v1/universes/${universeId}/models/tiers`, params),
    onSuccess: (value) => { queryClient.setQueryData(modelDefaultsKey(universeId), value); onClose(); },
  });
  const conflict = save.error instanceof ApiError && save.error.status === 409;
  const reload = async () => {
    setReloading(true);
    setReloadError(null);
    try {
      const latest = await api<ModelDefaults>("GET", `/api/v1/universes/${universeId}/models/defaults`);
      queryClient.setQueryData(modelDefaultsKey(universeId), latest);
      setRevision(latest.revision);
      setTable(latest.tiers ?? {});
      save.reset();
    } catch (error) { setReloadError(error instanceof Error ? error.message : "Could not reload tiers."); }
    finally { setReloading(false); }
  };
  const routes = (discovery.data?.models ?? []).filter((route) => (AGENT_MODEL_API_KINDS as readonly string[]).includes(route.apiKind));
  const currentRoute = draft.alias === null
    ? routes.find((route) => route.providerId === draft.providerId.trim() && route.apiKind === draft.apiKind && route.model === draft.model.trim())
    : undefined;
  const choice: ModelChoice = draft.alias !== null
    ? { kind: "tier", tier: draft.alias }
    : currentRoute ? { kind: "route", route: currentRoute } : { kind: "manual" };
  const update = (patch: Partial<Draft>) => setDraft((current) => ({ ...current, ...patch }));
  return <Dialog open onOpenChange={(open) => { if (!open && !save.isPending) onClose(); }}>
    <DialogContent className="max-h-[90dvh] overflow-y-auto sm:max-w-lg">
      <DialogHeader>
        <DialogTitle>{tier ? `Tier ${tier}` : "Add tier"}</DialogTitle>
        <DialogDescription>Point the tier at another tier or at a model. Profiles reference tiers by name.</DialogDescription>
      </DialogHeader>
      <form className="grid gap-4" onSubmit={(event) => { event.preventDefault(); if (valid && !conflict && !save.isPending) save.mutate(); }}>
        <fieldset className="grid gap-4" disabled={save.isPending || reloading}>
          <Field>
            <FieldLabel htmlFor={`${id}-name`}>Name</FieldLabel>
            <Input id={`${id}-name`} value={draft.name} placeholder="fast" onChange={(event) => update({ name: event.target.value })} />
            <FieldDescription>Letters, digits, '-', '_', or '.'; <span className="font-mono">default</span> is set under Defaults.</FieldDescription>
          </Field>
          <Field>
            <FieldLabel htmlFor={`${id}-target`}>Points at</FieldLabel>
            <ModelChoicePicker
              id={`${id}-target`}
              ariaLabel="Search tiers and models"
              value={choice}
              tiers={aliasTargets(initial, table, name)}
              routes={routes}
              onChange={(next) => {
                if (next.kind === "tier") update({ alias: next.tier });
                else if (next.kind === "route") update({ alias: null, providerId: next.route.providerId, apiKind: next.route.apiKind, model: next.route.model });
                else update({ alias: null });
              }}
            />
            <FieldDescription>
              {draft.alias !== null
                ? "An alias moves with its target: rebinding the target moves every profile that uses this tier."
                : discovery.isLoading ? "Loading suggestions…" : discovery.error ? "Suggestions are unavailable. You can still enter and save a route." : "Choose another tier, a discovered model, or enter a route below."}
            </FieldDescription>
          </Field>
          {draft.alias === null && (
            <>
              <div className="grid gap-4 sm:grid-cols-2">
                <Field><FieldLabel htmlFor={`${id}-provider`}>Provider</FieldLabel><Input id={`${id}-provider`} value={draft.providerId} list={`${id}-providers`} placeholder="Provider ID" onChange={(event) => update({ providerId: event.target.value })} /></Field>
                <datalist id={`${id}-providers`}>{discovery.data?.providers?.map((provider) => <option key={provider.providerId} value={provider.providerId} />)}</datalist>
                <Field><FieldLabel htmlFor={`${id}-api`}>API</FieldLabel><Select value={draft.apiKind} onValueChange={(value) => { if (value) update({ apiKind: value }); }}>
                  <SelectTrigger id={`${id}-api`} className="w-full"><SelectValue>{(value: string) => apiLabels[value] ?? value}</SelectValue></SelectTrigger>
                  <SelectContent>{AGENT_MODEL_API_KINDS.map((kind) => <SelectItem key={kind} value={kind}>{apiLabels[kind]}</SelectItem>)}</SelectContent>
                </Select></Field>
              </div>
              <Field><FieldLabel htmlFor={`${id}-model`}>Model</FieldLabel><Input id={`${id}-model`} value={draft.model} placeholder="Model name" onChange={(event) => update({ model: event.target.value })} /></Field>
              <div className="grid gap-4 sm:grid-cols-2">
                <Field><FieldLabel htmlFor={`${id}-effort`}>Reasoning effort</FieldLabel><Input id={`${id}-effort`} value={draft.reasoningEffort} placeholder="Provider default" onChange={(event) => update({ reasoningEffort: event.target.value })} /></Field>
                <Field><FieldLabel htmlFor={`${id}-tokens`}>Max output tokens</FieldLabel><Input id={`${id}-tokens`} inputMode="numeric" value={draft.maxOutputTokens} placeholder="Provider default" onChange={(event) => update({ maxOutputTokens: event.target.value })} /></Field>
              </div>
              <FieldDescription>Settings apply to sessions and calls that resolve to this tier unless they set their own.</FieldDescription>
            </>
          )}
        </fieldset>
        {!parsed.success && name && <p className="text-xs text-muted-foreground">{parsed.error.issues[0]?.message}</p>}
        {resolveError && <p role="alert" className="text-sm text-destructive">{resolveError}</p>}
        {save.error && <p role="alert" className="text-sm text-destructive">{conflict ? "Tiers changed elsewhere. Reload and review the saved table before making another change." : save.error.message}</p>}
        {reloadError && <p role="alert" className="text-sm text-destructive">{reloadError}</p>}
        <DialogFooter>
          <Button type="button" variant="outline" disabled={save.isPending} onClick={onClose}>Cancel</Button>
          {conflict ? <Button type="button" disabled={reloading} onClick={() => void reload()}>{reloading ? "Reloading…" : "Reload saved tiers"}</Button>
            : <Button type="submit" disabled={!valid || save.isPending}>{save.isPending ? "Saving…" : "Save tier"}</Button>}
        </DialogFooter>
      </form>
    </DialogContent>
  </Dialog>;
}
