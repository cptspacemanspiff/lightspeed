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
  kind: "route" | "alias";
  providerId: string;
  apiKind: string;
  model: string;
  reasoningEffort: string;
  maxOutputTokens: string;
  alias: string;
};

function draftFor(name: string | null, binding: ModelTierBinding | undefined): Draft {
  const base: Draft = { name: name ?? "", kind: "route", providerId: "", apiKind: AGENT_MODEL_API_KINDS[0], model: "", reasoningEffort: "", maxOutputTokens: "", alias: MODEL_TIER_DEFAULT };
  if (!binding) return base;
  if (isModelTierAlias(binding)) return { ...base, kind: "alias", alias: binding.tier };
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
  if (draft.kind === "alias") return { tier: draft.alias.trim() };
  const maxOutputTokens = Number(draft.maxOutputTokens);
  return {
    model: { providerId: draft.providerId.trim(), apiKind: draft.apiKind, model: draft.model.trim() },
    ...(draft.reasoningEffort.trim() ? { reasoningEffort: draft.reasoningEffort.trim() } : {}),
    ...(Number.isInteger(maxOutputTokens) && maxOutputTokens > 0 ? { maxOutputTokens } : {}),
  };
}

/// Adds or changes one tier by replacing the whole table under its revision.
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
  const aliasTargets = [MODEL_TIER_DEFAULT, ...Object.keys(table).filter((other) => other !== name)];
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
  const update = (patch: Partial<Draft>) => setDraft((current) => ({ ...current, ...patch }));
  return <Dialog open onOpenChange={(open) => { if (!open && !save.isPending) onClose(); }}>
    <DialogContent className="max-h-[90dvh] overflow-y-auto sm:max-w-lg">
      <DialogHeader>
        <DialogTitle>{tier ? `Tier ${tier}` : "Add tier"}</DialogTitle>
        <DialogDescription>Bind a route with its settings, or alias another tier. Profiles reference tiers by name.</DialogDescription>
      </DialogHeader>
      <form className="grid gap-4" onSubmit={(event) => { event.preventDefault(); if (valid && !conflict && !save.isPending) save.mutate(); }}>
        <fieldset className="grid gap-4" disabled={save.isPending || reloading}>
          <div className="grid gap-4 sm:grid-cols-2">
            <Field>
              <FieldLabel htmlFor={`${id}-name`}>Name</FieldLabel>
              <Input id={`${id}-name`} value={draft.name} placeholder="fast" onChange={(event) => update({ name: event.target.value })} />
              <FieldDescription>Letters, digits, '-', '_', or '.'; <span className="font-mono">default</span> is set under Defaults.</FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor={`${id}-kind`}>Binding</FieldLabel>
              <Select value={draft.kind} onValueChange={(value) => { if (value) update({ kind: value as Draft["kind"] }); }}>
                <SelectTrigger id={`${id}-kind`} className="w-full"><SelectValue>{(value: string) => value === "alias" ? "Alias for another tier" : "Route and settings"}</SelectValue></SelectTrigger>
                <SelectContent>
                  <SelectItem value="route">Route and settings</SelectItem>
                  <SelectItem value="alias">Alias for another tier</SelectItem>
                </SelectContent>
              </Select>
            </Field>
          </div>
          {draft.kind === "alias" ? (
            <Field>
              <FieldLabel htmlFor={`${id}-alias`}>Points at</FieldLabel>
              <Select value={draft.alias} onValueChange={(value) => { if (value) update({ alias: value }); }}>
                <SelectTrigger id={`${id}-alias`} className="w-full"><SelectValue>{(value: string) => value}</SelectValue></SelectTrigger>
                <SelectContent>{aliasTargets.map((target) => <SelectItem key={target} value={target}>{target}</SelectItem>)}</SelectContent>
              </Select>
              <FieldDescription>Rebinding the target moves every profile that uses this tier with it.</FieldDescription>
            </Field>
          ) : (
            <>
              <Field>
                <FieldLabel htmlFor={`${id}-route`}>Discovered model</FieldLabel>
                <Select value="" onValueChange={(key) => {
                  const route = routes.find((candidate) => JSON.stringify([candidate.providerId, candidate.apiKind, candidate.model]) === key);
                  if (route) update({ providerId: route.providerId, apiKind: route.apiKind, model: route.model });
                }}>
                  <SelectTrigger id={`${id}-route`} className="w-full"><SelectValue>{() => routes.length ? "Choose a discovered model…" : discovery.isLoading ? "Loading suggestions…" : "No suggestions; enter a route below"}</SelectValue></SelectTrigger>
                  <SelectContent>{routes.map((route) => {
                    const key = JSON.stringify([route.providerId, route.apiKind, route.model]);
                    return <SelectItem key={key} value={key}>{modelLabel(route)} · {apiLabels[route.apiKind] ?? route.apiKind}</SelectItem>;
                  })}</SelectContent>
                </Select>
              </Field>
              <div className="grid gap-4 sm:grid-cols-2">
                <Field><FieldLabel htmlFor={`${id}-provider`}>Provider</FieldLabel><Input id={`${id}-provider`} value={draft.providerId} placeholder="Provider ID" onChange={(event) => update({ providerId: event.target.value })} /></Field>
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
