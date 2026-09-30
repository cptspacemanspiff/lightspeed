# PNNN — Model tiers

**Status:** Shared tables, tier references, and the resolve view implemented,
2026-09-30. Builds on
[universe model defaults](p184-universe-model-defaults-and-transcription.md).

Let profiles, sessions, runs, and model-calling tools name a model by its
role, such as `default`, `fast`, or `grader-small`, and resolve that name to
a concrete provider route through layered tables. The universe sets the
meaning of each tier, a profile or session can override it, and every
admitted document records the concrete route it resolved to.

## Problem

Before this work every model reference was concrete. `ModelConfig` is
`{ providerId, apiKind, model }`, and profiles, session configs, run
overrides, and `features.generate` entries all carried one. The only
indirection was the universe `agentRun` default, used when a session config
named no model.

This caused three problems:

- **Profiles were not portable.** A profile written for one deployment named
  that deployment's providers. Moving it to a setup with a local model
  server, or a different subscription, meant editing every profile.
- **Callers had to know the backend.** A graph, or a `model_generate` grant,
  had to embed a provider id and model name. An LLM-authored graph would then
  encode one user's backend into the definition.
- **Cost and quality were spread across documents.** Moving every "small"
  job to a new model meant finding every profile that named the old one.

Bring-your-own-model installs make this sharper. Each user has a different
mix of API keys, subscriptions, and local models, but the application wants
to ship the same profiles to all of them.

## Concepts

- **Tier:** a name for a role a model plays, for example `default`, `fast`,
  or `grader-small`. It says nothing about the backend. Names are 1–64 ASCII
  letters, digits, `-`, `_`, or `.`; `/` stays reserved for a later
  provider-qualified form.
- **Binding:** what a tier resolves to. A binding is one of:
  - **route**: `{ model: { providerId, apiKind, model }, reasoningEffort?,
    maxOutputTokens? }`. Settings belong to the route because their valid
    values depend on the model.
  - **alias**: `{ tier: "<name>" }`. An alias carries only the name.
- **Tier table:** a map from tier names to bindings. Tables exist at two
  layers and merge by name.
- **Model reference:** the `model` field of a session config, run override,
  or generation entry: a concrete route or `{ "tier": "<name>" }`.

## Layers

| Layer | Owner | Holds |
|---|---|---|
| Universe | operators (`models/defaults/put`, `models/tiers/put`) | the `default` tier, which is the existing `agentRun` selection, plus the universe tier table stored beside it under the same revision |
| Configuration | whoever writes the document (`profiles/put`, `session/start`, `session/config/put`, `session/profiles/apply`) | `config.models`, a partial table that overrides the universe by name |

A session's configuration layer is one table: at creation the explicit
configuration's `models` merges over the profile's entry by entry, the same
way its other sections merge. There is no separate session layer beneath the
profile layer, because a session's stored configuration carries the routes
it resolved, not the table (see Resolution and pinning).

The universe table cannot define `default`; that name is the `agentRun`
selection so the existing defaults API and Platform page keep their meaning.
A configuration table may rebind `default`, which is how a profile changes
what an omitted model means for its sessions.

### Example

```text
default       → openai / openai:responses / gpt-5.5        (universe, agentRun)
fast          → openai / openai:responses / gpt-5.5-mini, reasoningEffort: low
large         → default                                    (universe alias)
grader-fast   → fast                                       (universe alias)
grader        → grader-fast                                (profile config.models)
```

A profile whose `model` is `{ "tier": "grader" }` resolves to `gpt-5.5-mini`
with low reasoning effort. Pointing the universe `fast` at another provider
moves every `grader` user with it, without touching any profile.

## Resolution and pinning

- **Lookup order.** A name is looked up in the most specific layer that
  defines it: the configuration table, then the universe.
- **Alias direction.** An alias resolves in its own layer or a more general
  one, never a more specific one. If the universe defines
  `grader-fast → default` and a profile rebinds `default`, a call for
  `grader-fast` still uses the universe's default. A profile that wants both
  changed rebinds both names.
- **Reach-through.** An alias to its own name reaches through to the next
  more general layer: a profile entry `fast: { tier: "fast" }` means the
  universe's `fast`. A universe tier cannot alias itself.
- **Cycles.** A (layer, name) pair is never visited twice, so cycles are
  rejected and chain length is bounded by the table sizes.
- **Settings.** The route at the end of the chain supplies the reasoning
  effort and output cap. They fill the document's `generation` fields (or a
  generation entry's own settings) only where those are unset, so an explicit
  session, run, or entry setting still wins.
- **Session model.** `config.model` may be a route or `{ tier }`. A tier is
  resolved once, when the document is admitted, and the route becomes the
  session's model. The existing provider pin then applies unchanged: later
  tier-table edits never move a session to another provider. A config with
  no model resolves `default` at creation and preserves the current model on
  replacement or profile application, as before.
- **Configuration table.** `config.models` is consumed at admission. A
  session's stored configuration, and the document read back from it, carry
  routes and no table. Profiles keep their table, so applying a profile again
  resolves against the current universe.
- **Run override.** `RunStartConfig.model` may name a tier. It resolves
  through the universe table alone, since the session holds no table, and
  must land on the session's pinned provider and API kind like a concrete
  override.
- **Generation entries.** `features.generate.models[].model` may name a
  tier. It resolves with the rest of the document, so the engine's grant
  holds routes and the `model_generate` tool advertises names as before.
- **Records.** Every admission that resolves a tier logs the chain it
  followed (`grader -> grader-fast -> fast`) with the session or profile it
  applied to. The concrete route is in session state as before, so tier-table
  changes never make history ambiguous.
- **Engine.** The engine only ever sees concrete `ModelSelection` values.
  Tiers are resolved in the gateway before engine translation.

## Admission and authority

- Every route binding is validated like a session model: an agent-generation
  API kind, and a reasoning effort from the adapter's vocabulary for that API
  kind. Provider credentials are still resolved at call time.
- A universe table is validated as a whole when written: names, routes, no
  `default`, and every alias resolves within the universe without cycles. An
  alias to `default` is accepted before a default is chosen, since the name
  always exists.
- A profile is validated when written: its `models` table and every tier
  reference must resolve against the current universe, except that an unset
  `default` is tolerated. A dangling name is a configuration error at write
  time rather than at the next session start.
- A later universe edit can still remove a name a profile uses. The next
  session start or profile application then fails with a clear error naming
  the field and the chain; running sessions keep their routes.
- Tier tables hold no credentials and grant no capability. What a tier means
  belongs to the table; who may use it belongs to the grant.

## API

- `models/defaults/read` returns the tier table beside the two default
  slots, under one revision.
- `models/tiers/put` replaces the universe table with an expected revision.
- `models/tiers/resolve` (viewer) returns every tier a configuration can
  name, with its defining layer, alias chain, and route, optionally under a
  profile's table and an extra configuration table. Entries that do not
  resolve carry an error instead of a route. This is the "why did this call
  use that model" view.
- `SessionConfig.model`, `RunStartConfig.model`, and
  `GenerateModelOption.model` accept a route or `{ tier }`.
- `SessionConfig.models` is the configuration-layer table.
- The CLI has `models tiers list|set|alias|remove|resolve`; Platform's
  Models page edits the table and the profile and session editors offer
  tiers beside discovered models.

## Implementation progress

- [x] `ModelRef`, tier bindings, tables, and the resolver with alias
      direction, reach-through, and cycle detection in `crates/api`.
- [x] Universe table stored beside the default slots (schema revision 11),
      `models/tiers/put` and `models/tiers/resolve`.
- [x] Resolution at session start, configuration replacement, profile
      application, run start, and for generation entries; profile writes
      validated against the universe.
- [x] CLI tier commands; regenerated API contract, TypeScript client,
      Configurator tools, profile config reference, and method roles.
- [x] Platform: tier section and dialog on Models, tier choices in the
      configuration editors, creation-model preview through tiers, demo
      backend parity.
- [ ] Provider-qualified tiers (`<provider_id>/<tier>`) supplied by provider
      records and deployment configuration, and the install flow that asks
      which one `default` should point at.
- [ ] Tier allowlists on grants, capability hints on bindings, and MCP
      sampling over tiers.

## Open questions

- Should a session keep its configuration table so run overrides can name
  profile-defined tiers? Today a run override resolves through the universe
  alone, which keeps session state free of tables.
- Fallbacks: should a binding list ordered candidates for when a provider is
  unavailable? The recorded route would keep the log accurate, but "which
  model ran" would then depend on availability.
- Whether Platform should warn, on a universe tier edit, about the profiles
  that reference the name being removed. The resolve view shows the effect
  for one profile at a time.
