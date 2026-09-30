import { Fragment, useEffect, useState } from "react";
import {
  Combobox,
  ComboboxContent,
  ComboboxEmpty,
  ComboboxInput,
  ComboboxItem,
  ComboboxList,
  ComboboxSeparator,
} from "@/components/ui/combobox";

/// A tier the picker offers, labelled by the layer that defines it
/// ("Universe", "Profile", "Session") with an optional resolution hint.
export type ModelPickerTier = { tier: string; layer: string; hint?: string };

export type ModelPickerRoute = {
  providerId: string;
  apiKind: string;
  model: string;
  displayName: string;
  createdAtMs?: number | null;
};

/// What a model field holds: inherit the universe default (the field is
/// omitted), a tier name, a discovered route, or a route typed by hand.
export type ModelChoice =
  | { kind: "inherit" }
  | { kind: "tier"; tier: string }
  | { kind: "route"; route: ModelPickerRoute }
  | { kind: "manual" };

export function modelChoiceKey(choice: ModelChoice): string {
  switch (choice.kind) {
    case "inherit": return "inherit";
    case "tier": return `tier:${choice.tier}`;
    case "route": return `route:${JSON.stringify([choice.route.providerId, choice.route.apiKind, choice.route.model])}`;
    case "manual": return "manual";
  }
}

type Section = "inherit" | "tier" | "route" | "manual";
type Item = { key: string; section: Section; label: string; hint?: string; search: string; choice: ModelChoice };

/// One searchable list for every place a model is chosen: tiers first,
/// grouped by layer, then discovered models, then manual entry. Picking a
/// tier writes `{ tier }`; picking a model writes its route.
export function ModelChoicePicker({
  id,
  ariaLabel = "Search models",
  placeholder = "Search tiers and models...",
  value,
  tiers,
  routes,
  inheritLabel,
  manualLabel = "Enter model manually",
  emptyLabel = "No matching tiers or models.",
  className,
  formatCreatedAt,
  onChange,
}: {
  id?: string;
  ariaLabel?: string;
  placeholder?: string;
  value: ModelChoice;
  tiers: ModelPickerTier[];
  routes: ModelPickerRoute[];
  /// Offers "inherit" as the first choice when present.
  inheritLabel?: string;
  manualLabel?: string | null;
  emptyLabel?: string;
  className?: string;
  formatCreatedAt?: (createdAtMs: number) => string;
  onChange: (choice: ModelChoice) => void;
}) {
  const items: Item[] = [
    ...(inheritLabel ? [{ key: "inherit", section: "inherit" as const, label: inheritLabel, search: `inherit universe default ${inheritLabel}`, choice: { kind: "inherit" as const } }] : []),
    ...tiers.map((tier) => ({
      key: modelChoiceKey({ kind: "tier", tier: tier.tier }),
      section: "tier" as const,
      label: `${tier.layer} · ${tier.tier}`,
      hint: tier.hint,
      search: `tier ${tier.layer} ${tier.tier} ${tier.hint ?? ""}`,
      choice: { kind: "tier" as const, tier: tier.tier },
    })),
    ...routes.map((route) => ({
      key: modelChoiceKey({ kind: "route", route }),
      section: "route" as const,
      label: `${route.displayName} (${route.providerId})`,
      hint: [
        route.displayName !== route.model ? route.model : null,
        route.apiKind,
        route.createdAtMs && formatCreatedAt ? `created ${formatCreatedAt(route.createdAtMs)}` : null,
      ].filter(Boolean).join(" · "),
      search: `${route.displayName} ${route.providerId} ${route.apiKind} ${route.model}`,
      choice: { kind: "route" as const, route },
    })),
    ...(manualLabel ? [{ key: "manual", section: "manual" as const, label: manualLabel, search: "manual custom model route", choice: { kind: "manual" as const } }] : []),
  ];
  const selectedKey = modelChoiceKey(value);
  const selected = items.find((item) => item.key === selectedKey)
    ?? (value.kind === "route"
      ? { key: selectedKey, section: "route" as const, label: `${value.route.displayName} (${value.route.providerId})`, search: "", choice: value }
      : { key: "manual", section: "manual" as const, label: "Custom model", search: "", choice: { kind: "manual" as const } });
  const keys = items.map((item) => item.key);
  const [search, setSearch] = useState(selected.label);
  useEffect(() => {
    setSearch(selected.label);
  }, [selected.label]);
  return (
    <Combobox
      items={keys}
      value={selected.key}
      inputValue={search}
      autoHighlight
      itemToStringLabel={(key) => items.find((item) => item.key === key)?.label ?? key}
      filter={(key, query) => {
        const item = items.find((candidate) => candidate.key === key);
        return item?.search.toLocaleLowerCase().includes(query.toLocaleLowerCase()) ?? false;
      }}
      onInputValueChange={setSearch}
      onValueChange={(key) => {
        if (!key) return;
        const item = items.find((candidate) => candidate.key === key);
        if (!item) return;
        setSearch(item.label);
        onChange(item.choice);
      }}
    >
      <ComboboxInput id={id} className={className ?? "w-full"} placeholder={placeholder} aria-label={ariaLabel} />
      <ComboboxContent>
        <ComboboxEmpty>{emptyLabel}</ComboboxEmpty>
        <ComboboxList>
          {(key: string) => {
            const index = items.findIndex((item) => item.key === key);
            const item = items[index];
            if (!item) return null;
            const previous = items[index - 1];
            return (
              <Fragment key={item.key}>
                {previous && previous.section !== item.section && <ComboboxSeparator />}
                <ComboboxItem value={item.key} className="py-2">
                  <span className="min-w-0">
                    <span className="block truncate">{item.label}</span>
                    {item.hint && <span className="block truncate text-xs text-muted-foreground">{item.hint}</span>}
                  </span>
                </ComboboxItem>
              </Fragment>
            );
          }}
        </ComboboxList>
      </ComboboxContent>
    </Combobox>
  );
}
