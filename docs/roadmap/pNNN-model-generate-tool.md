# PNNN — Model generation tool

**Status:** Implemented, draft for review, 2026-09-29.

Give agents a `model_generate` tool that makes one standalone model call and
returns the reply, the way `agent_run` returns a sub-agent's result. The call
may pass a JSON input with an input schema, and may require a JSON reply
matching an output schema. The profile decides which models the tool may use.

## Outcome

- A profile grants the tool with `features.generate`. Without the grant the
  tool is absent, like every other feature.
- The grant lists named models. The tool's `model` parameter is an enum of
  those names, the first entry is the default, and any other name fails the
  call before anything runs. A grant with no models lets the tool use only
  the calling turn's own model.
- The model sees only its instructions and input: no conversation, files, or
  tools. A reply that breaks the output schema fails the call, and the
  calling agent sees the reply text and the validation errors.
- The tool is an ordinary workflow tool. Several calls in one turn run
  concurrently, and the session carries no generation-specific code.

## Configuration

```jsonc
"features": {
  "generate": {
    "models": [
      {
        "name": "grader",                   // what the agent passes as `model`
        "description": "Cheap classifier",  // shown to the agent
        "model": { "providerId": "openai", "apiKind": "openai:responses", "model": "gpt-5.5-mini" },
        "reasoningEffort": "low",           // optional, per entry
        "maxOutputTokens": 800              // optional, per entry
      }
    ]
  }
}
```

- Names are 1–64 ASCII letters, digits, `-`, `_`, or `.`, unique within the
  grant; at most 32 entries; descriptions at most 1024 bytes.
- Entries use agent-generation API kinds. Their provider routes are resolved
  and authenticated at call time like a session model, and may name a
  different provider from the session's own.

Call arguments:

```jsonc
{
  "model": "grader",                  // optional; the grant's first model
  "instructions": "Grade the claim.",
  "input": { "claim": "…" },          // text, or any JSON value
  "input_schema": { … },              // optional; checked at admission
  "output_schema": { … }              // optional; the reply must match
}
```

The reply envelope carries `status`, the concrete `model` it used (name and
route), `output_mode` (`native` or `prompted`), `text` or the validated
`json`, token `usage`, and on failure an `error` of kind `invalid_request`,
`provider`, `truncated`, `refused`, or `output_invalid`.

## Design

- **Binding.** When the grant is present, session preparation admits a
  system workflow-tool binding, `model-generate`
  (`lightspeed.model.generate.v1`): start-on-call, joined completion, with a
  30-minute deadline. The binding does not depend on the grant, so profile
  edits never conflict with the immutable binding.
- **Advertised models.** Each preparation publishes the grant's model names
  and descriptions into the tool's built-in settings in the session toolset,
  so the advertised schema enumerates them. Provider routes never reach the
  model. Argument validation for built-in workflow tools resolves the schema
  from the turn's advertised settings, which the call already carries.
- **Admission.** The engine pins the grant and the turn's effective model on
  each tool batch (`generate_policy`), as it pins the sub-agent grant. The
  worker validates the arguments, selects the model from the pinned grant,
  and stores the selection as the invocation's execution context. The model's
  arguments stay unchanged in CAS.
- **Execution.** `ModelGenerateWorkflow` runs on the sessions queue. It
  checks its start identity, runs one `model_generate` activity, and resolves
  the caller's `reply` promise. The holder's cancellation or Temporal
  cancellation abandons the provider call. It serves the recovery query.
- **Generation.** The activity sends one tool-less request through the
  universe's existing LLM runtime, so credentials, custom endpoints, and all
  three provider APIs work unchanged. Transient provider errors retry under
  the LLM activity policy; other failures resolve the call as failed.
- **Structured output.** `llm-runtime::standalone` lowers an object output
  schema into the provider's native setting when the schema fits that
  provider's subset: OpenAI Responses `text.format` (strict when every
  object is closed and fully required), built-in OpenAI Chat Completions
  `response_format`, and Anthropic `output_config.format` for closed schemas
  without unsupported keywords. Otherwise the schema is stated in the
  instructions. The activity always validates the reply against the full
  schema with `jsonschema`, whatever the provider guarantees.

## Invariants

- The grant's model list is the authority; the model can name only a listed
  entry, and the route it runs on is pinned at admission.
- A generation has no tools and no session context, so it cannot widen the
  calling session's capabilities.
- A completed call's `json`, if present, has passed Lightspeed's validation
  against the call's output schema.
- The engine stays deterministic: it records the pinned grant and the
  workflow-tool invocation; activities do all provider I/O and validation.

## Relationship to other drafts

- `pNNN-structured-output.md` proposes an output specification for whole
  runs. This tool's lowering and validation live in
  `llm-runtime::standalone`, which that work can share.
- `pNNN-model-tiers.md` adds model tiers. A grant entry's `model` may now
  name a tier (`{ "tier": "grader-small" }`), resolved to a route when the
  configuration is admitted; the tool still advertises entry names only.
  Tier allowlists on the grant remain future work there.

## Implementation progress

- [x] `features.generate` in the API, engine, projection, and validation.
- [x] System binding, per-preparation model publication, and admission
      pinning in the worker.
- [x] `ModelGenerateWorkflow`, the `model_generate` activity, and the
      standalone generation helper with native and prompted output.
- [x] Tool-call display, Platform profile editor, capability summary, and
      session resource chip.
- [x] Regenerated API contract, TypeScript client, Configurator tools, and
      profile config reference; the workflow contract is unchanged.
- [x] Unit tests across the engine, tools, runtime, gateway, and worker;
      catalog parity fixture; an ignored live suite
      (`model_generate_live`) with a scripted model.

## Open questions

- Should media (images, documents) be accepted as input for vision-capable
  entries?
- Should the workflow retry an `output_invalid` reply with a correction turn
  before failing the call, as the structured-output draft proposes for runs?
- Should a per-run call cap sit on the grant, beyond the generic per-run
  workflow-tool emission cap?
- Should the user-facing documentation describe the tool alongside
  sub-agents in the tools guide?
