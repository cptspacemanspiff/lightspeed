import { useActionPermissions } from "@/lib/permissions";
import { useMcpToolDiscoverySource } from "@/lib/mcp/tool-discovery";
import { useQuery } from "@tanstack/react-query";
import { modelTierNames, resolveModelTier } from "@lightspeed/platform-shared";
import { modelLabel, useModelDefaults, useModelDiscovery } from "@/lib/model-defaults";
import {
  api,
  type Environment,
  type ModelDefaults,
  type ProfileSummary,
} from "@/api";
import type { ModelPickerTier } from "@/components/models/model-choice-picker";
import type {
  McpServerOption,
  WorkspaceOption,
} from "@/components/session/session-config-editor";

/** Attachment choices for a session setup. */
export function useSessionConfigEditorOptions(
  universeId: string,
  enabled = true,
) {
  const servers = useQuery({
    queryKey: ["mcp-servers", universeId],
    queryFn: () => api<McpServerOption[]>("GET", `/api/v1/universes/${universeId}/mcp-servers`),
    enabled,
  });
  const workspaces = useQuery({
    queryKey: ["workspaces", universeId],
    queryFn: () =>
      api<WorkspaceOption[]>("GET", `/api/v1/universes/${universeId}/workspaces`),
    enabled,
  });
  const models = useModelDiscovery(universeId, enabled);
  const defaults = useModelDefaults(universeId, enabled);
  const profiles = useQuery({
    queryKey: ["profiles", universeId],
    queryFn: () => api<ProfileSummary[]>("GET", `/api/v1/universes/${universeId}/profiles`),
    enabled,
  });
  const environments = useQuery({
    queryKey: ["environments", universeId],
    queryFn: () => api<Environment[]>("GET", `/api/v1/universes/${universeId}/environments`),
    enabled,
  });
  const permissions = useActionPermissions(enabled ? universeId : undefined);
  const mcpToolDiscovery = useMcpToolDiscoverySource(universeId);
  return {
    mcpServers: servers.data,
    workspaces: workspaces.data,
    workspacesLoading: workspaces.isLoading,
    models: models.data?.models,
    defaultModelLabel: defaults.data?.agentRun ? `Universe · default · ${modelLabel(defaults.data.agentRun)}` : "Universe · default",
    modelTiers: defaults.data ? universeTierChoices(defaults.data) : undefined,
    profiles: profiles.data,
    environments: environments.data,
    mcpToolDiscovery: permissions.can("configure_resource") ? mcpToolDiscovery : undefined,
  };
}

/// Universe tiers other than `default` (the inherit choice), each with the
/// route it resolves to as a hint.
export function universeTierChoices(defaults: ModelDefaults): ModelPickerTier[] {
  return modelTierNames(defaults)
    .filter(({ tier }) => tier !== "default")
    .map(({ tier }) => {
      try {
        const resolved = resolveModelTier(defaults, undefined, tier);
        const settings = resolved.route.reasoningEffort ? ` · ${resolved.route.reasoningEffort}` : "";
        const chain = resolved.chain.length > 1 ? `${resolved.chain.slice(1).join(" → ")} · ` : "";
        return { tier, layer: "Universe", hint: `${chain}${modelLabel(resolved.route.model)}${settings}` };
      } catch (error) {
        return { tier, layer: "Universe", hint: error instanceof Error ? error.message : "does not resolve" };
      }
    });
}
