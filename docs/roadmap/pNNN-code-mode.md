# PNNN — Code-mode sessions

**Status:** Proposed, draft for review, 2026-09-30. If accepted, this
replaces `pNNN-graph-workflows.md`.

## Outcome

Add a second kind of session whose runs execute a TypeScript program instead
of a model loop. A code-mode session appears in the session tree like any
other. A user opens it, watches its tool calls form a graph as the program
runs, opens the sub-agent sessions it started, and sees the JSON result it
returned to its parent.

An agent starts one with `code_run`, handing a fixed piece of work (fetch
these sources, judge each claim, merge the verdicts) to a program rather than
driving it one turn at a time. The API can also create a code-mode session and
start a script run directly, with no agent involved.

Scripts execute durably. A crashed worker resumes a script without repeating
completed calls, and a call that waits thirty minutes for a sub-agent does not
hold a sandbox open. The sandbox and its durability live in a standalone code
runtime that knows tool schemas and Temporal targets, not Lightspeed.

## Problem

Some tasks are pipelines. Today they are either a prompt the model may not
follow, or a custom plugin running outside Lightspeed's lineage, limits, and
retention.

The graph-workflows draft proposed a JSON graph format with a typed
expression language. A prototype showed that the type checking works, but a
DSL must grow control flow, error handling, and reshaping one feature at a
time, and models write TypeScript far better than a bespoke graph format.

Running model-authored code raises problems a graph interpreter avoided:

- **Isolation.** The code is untrusted and must reach only the tools it was
  given.
- **Durability.** A sandboxed isolate cannot be snapshotted or parked. Run as
  one long request, a crash repeats every side effect, and a long tool call
  pins an isolate for its whole duration.
- **Visibility.** A program's calls must be inspectable the way an agent's
  are: in the transcript, in lineage, and under the same retention.

## Prior art

- **Cloudflare code mode** (`@cloudflare/codemode`). Tools become a typed
  namespace, the model writes an async function body, and an `Executor` runs
  it in a sandbox that routes each call to a host-side function.
  `DynamicWorkerExecutor` uses a fresh Worker Loader isolate per execution with
  `globalOutbound: null`. `generateTypesFromJsonSchema` produces declarations
  from plain JSON Schema. Executions are not resumable.
- **`babariviere/code-mode`**, with a pi host adapter: the same model on
  QuickJS/WASM, with effect labels, policy intersection, and limits. Also not
  resumable.
- **Temporal** records every nondeterministic result and replays workflow code
  against that history. The code runtime applies the same idea to the script.
- **External harness sessions** (`p185-external-harness-sessions.md`) propose
  an explicit session backend, with a supervising workflow that owns a
  session's log without driving the native engine. Code-mode sessions are a
  second backend on the same split.

## Decision

- **A new session backend, `Code`.** It shares session identity, lifecycle,
  run facts, interactions, lineage, and retention with native sessions. It has
  its own supervising workflow and reducer, and never drives the native engine.
- **`CodeSessionWorkflow`** on the sessions queue owns the session's log and
  hosts its tools. Each run is one script execution.
- **A standalone code runtime executes scripts.** It is a TypeScript Temporal
  worker using codemode's `Executor` interface and a workerd sandbox. For each
  tool call it asks its host, the code session, to execute it.
- **Two histories, as for native sessions.** The runtime's Temporal history is
  authoritative for replaying the script. The session log is authoritative for
  what happened, and is what clients, lineage, and retention read. Neither is a
  backup of the other.
- **Authority is the session's configuration.** A code session's tools are
  configured like a native session's. A code session created by `code_run`
  gets a configuration that cannot exceed its parent's grant.

## Design

### 1. The session backend

A code session's configuration is fixed at creation:

- **Tools**, in the same shapes a native session uses: built-ins, the
  `generate` and `subagents` features, and workflow-tool bindings. These are
  the functions a script can call.
- **Limits**: calls, concurrency, generations, per-run deadline, and value
  sizes (section 4).
- **No model.** It does not require a model route or receive a model default.

Operations that only mean something for a model loop return typed capability
errors: context edits, compaction, steering, fork and clone, and model or
generation settings. Clients read the capabilities to omit those controls.

Runs use a new run source:

```jsonc
// session/runs/start
{
  "source": {
    "type": "script",
    "source": "…",          // TypeScript async function body
    "input": { },
    "outputSchema": { }     // optional
  }
}
```

One run is active at a time and later runs queue, as on native sessions. The
source is validated at admission: size, and a syntax check after stripping
types. A run completes with `output_json`, matching the structured-output
draft's run output, or fails with a typed error.

### 2. The session log

Code sessions use the shared lifecycle, work, and interaction event families
from the backend split, plus a code-execution family:

| Event | Records |
| --- | --- |
| `ScriptAccepted` | source ref, input ref, output schema ref, the declarations shown to the script |
| `CallRequested` | call id, key, tool, arguments ref, `depends_on`, `steps` |
| `CallRejected` | call id, typed reason (unknown tool, schema, limit, authority) |
| `CallCompleted` | call id, result ref or typed tool error |
| `LogAppended` | a bounded chunk of console output |

`CallRequested` commits before the call is dispatched and `CallCompleted`
after the result is validated, so the log alone describes every effect the
script caused. Arguments and results are content refs, so large values never
enter the log itself.

Calls project into the common tool-call views, so existing transcript readers
show a code session without changes. The graph view (section 7) is a second
projection over the same events.

### 3. `CodeSessionWorkflow`

The workflow has a small deterministic state machine: admit runs, start an
execution for the active run, host its calls, record outcomes, and handle
cancellation. It is the session's only append owner.

For each run it starts `CodeExecutionWorkflow` on the `code-runtime` queue as
a child, with the script, the manifest built from the session's tools
(section 4), and the run's limits. When the child completes, it records the
run outcome. Cancelling the run cancels the child, and that cancels in-flight
calls.

Each call from the runtime arrives as a `call_requested` signal carrying the
call id, tool, and arguments. The workflow:

1. Deduplicates on call id. A known call gets its recorded result again.
2. Checks authority against the session's tool configuration and records
   `CallRequested`, or records `CallRejected` and replies with a typed error.
3. Executes the call:
   - Inline built-ins run in an activity through the existing inline tool
     runtime.
   - Workflow tools (`model_generate`, `agent_run`, custom bindings) start
     their execution with this workflow as holder. It answers
     `deliver_emission`, serves the `workflow_tool_recovery` query, and
     propagates cancellation: the holder half of the workflow-tool protocol
     that the native session workflow implements.
   - A tool that needs approval raises a shared interaction on the session.
     The call waits until a decision is recorded.
4. Records `CallCompleted` and signals `call_completed` back to the
   execution.

The workflow checkpoints its reduced state and continues as new like the
native session workflow. Calls in flight survive rollover because their facts
are in the log.

### 4. The code runtime

The runtime is a Temporal worker on its own task queue, `code-runtime`, in
the same namespace as its hosts. It depends on the Temporal SDK and codemode,
not on Lightspeed. Lightspeed is one host; others can supply their own tools.

**Contract.** `CodeExecutionWorkflow` takes the source, the input, a manifest,
limits, and an optional output schema, and returns
`{ status, output, error, logs, stats }`. A manifest entry names one function
and its target:

```jsonc
{
  "name": "web.fetch",      // becomes tools.web.fetch(...)
  "description": "…",
  "input_schema": { },
  "output_schema": { },
  "target": { "type": "host", "workflow_id": "…" },
  "hold_ms": 5000
}
```

A `host` target is request and reply by signal with a named workflow, which
is how a code session hosts its calls. Other hosts can use `activity` or
`workflow` targets, which dispatch directly. The runtime also exports a
function that turns a manifest into TypeScript declarations with codemode's
JSON Schema type generator.

**Journal.** Every call from the sandbox reaches the workflow as a Temporal
Update. The call key is `(tool, sha256(canonical arguments), occurrence)`,
where `occurrence` counts earlier identical calls in the same generation, so
the key does not depend on the order concurrent calls arrive. `call_id` is a
hash of the execution id and the key. The Update handler:

1. rejects calls from any generation but the current one;
2. returns the recorded result if the key is complete;
3. validates the manifest entry, the input schema, and call limits;
4. dispatches the target if the key is not already in flight;
5. waits up to the entry's `hold_ms`, then replies with the validated result
   and the logical clock, or with `suspend`.

These Updates, recorded in the runtime workflow's history, are the journal.
Long journals continue-as-new.

**Generations.** A generation is one `runScript` activity attempt with a
fresh sandbox. It strips types, runs the script through the executor, and
turns each namespace call into a `call` Update. While catching up, the host
releases recorded results in their original completion order so the script
sees the same interleaving. The generation ends when the script returns,
throws, or when every outstanding call has replied `suspend`; the activity then
discards the sandbox. When a pending result arrives, the workflow starts the
next generation, which re-runs the script and catches up instantly on recorded
calls.

A re-run that issues a key the journal does not know, before re-issuing every
key an earlier generation issued, has diverged. The execution fails with a
divergence error rather than dispatch a call the original run never made.

**Nondeterminism.** The sandbox has no network access. Randomness and time
are synchronous in JavaScript and cannot round-trip through an Update, so the
workflow records their inputs instead. `Math.random`, `crypto.getRandomValues`,
and `crypto.randomUUID` use a PRNG seeded by the workflow at start.
`Date.now`, `new Date()`, and `performance.now` return a logical clock that
advances when a result is delivered. `setTimeout` and `setInterval` are
removed; `sleep(ms)` is a built-in call backed by a workflow timer.

**Sandbox.** The first executor is a workerd sidecar hosting an executor
Worker with the Worker Loader binding. Each generation gets a fresh isolate
with `globalOutbound: null` and no bindings except the host entrypoint. The
isolate calls the host entrypoint over Workers RPC, the host relays to the
activity over a WebSocket, and only the activity talks to Temporal. A
QuickJS/WASM executor in-process serves development and tests.

### 5. Starting a code session from an agent

`features.code` on a native session grants `code_run` and lists which of the
session's tools scripts may call, plus limits. Session setup adds the system
binding `code_run` (start target, joined completion), which takes
`{ "source": "…", "input": { }, "label": "…" }`. A context entry gives the
model the declarations for the granted tools. The tool runtime rejects
oversized or invalid source as an ordinary tool error.

The started execution, `CodeRunWorkflow`, mirrors the sub-agent execution:

1. It creates a child code session. Its origin is the `code_run` invocation,
   its parent is the calling session, and it counts against the root's depth,
   descendant, and concurrency limits.
2. The child's tool configuration is the `features.code` tool list,
   resolved against the parent's configuration and the policies the batch was
   dispatched under. It cannot exceed the parent's grant.
3. It starts the child's script run with a terminal notification to itself.
4. It resolves the caller's `reply` promise with
   `{ session_id, status, output, error }`, serves `workflow_tool_recovery`,
   and cancels the child's run if the holder cancels.

An asynchronous `code_spawn`, returning a promise like `agent_spawn`, can
reuse the same execution.

### 6. Nesting

A script's `agent_run` call creates an ordinary sub-agent session whose parent
is the code session. The tree reads naturally: agent session, then code
session, then the agents the script started. A script may call `code_run` if
its configuration grants it, bounded by the root's depth limit.

### 7. The graph view

The graph is a projection over the code-execution events:

- Nodes are calls; `depends_on` gives edges; `steps` gives named groups.
- A node shows its arguments, result or error, and timing.
- An `agent_run` or `code_run` node links to its child session.

The runtime's prelude records the annotations: `depends_on` lists the call
ids whose results the script had awaited before issuing a call, and
`step("name", async () => …)` sets the enclosing labels. The web client tails
the session's events and draws the graph as the script runs.

### 8. Relationship to external harness sessions

Both backends need the same extraction of shared session identity,
lifecycle, storage contracts, and public projections from native core
execution. Code sessions are a simpler first consumer: no environment bridge,
no external protocol, and every event is produced by Lightspeed's own
workflow. Whichever lands first performs the extraction, and the second
builds on it.

## Phases

1. **Standalone runtime.** `CodeExecutionWorkflow`, the Update protocol,
   generations, seed and clock, `sleep`, limits, the workerd and QuickJS
   executors, and declaration generation, with test tools. No Lightspeed
   changes.
2. **Session backend and API script runs.** The shared session extraction,
   the `Code` backend and configuration, `CodeSessionWorkflow` with inline
   built-ins, the code-execution events, and the common tool-view projection.
3. **`code_run` from native sessions.** The grant, binding, `CodeRunWorkflow`,
   child creation and limits, and structured output in the reply.
4. **Workflow tools and approvals from scripts.** The holder half in
   `CodeSessionWorkflow`, `agent_run` and `model_generate`, and approval
   interactions.
5. **Graph view.**
6. **Saved scripts**, named and versioned, runnable by id.

## Invariants

- The session log is authoritative for what a code session did. The runtime's
  history is authoritative only for replaying the script.
- `CodeSessionWorkflow` is the session's only append owner. A call is
  recorded before it is dispatched.
- A code session's authority is its tool configuration. A configuration
  derived from `code_run` never exceeds the parent's grant.
- The sandbox reaches nothing except its host entrypoint. A call id dispatches
  at most one execution, and only the current generation can dispatch.
- The runtime has no Lightspeed dependency, and the native engine learns
  nothing code-specific.
- Neither a code session nor anything it starts starts a run in its parent;
  the waiting parent run holds that session's only run slot.

## Testing

- Runtime: replay against recorded histories; killing a generation during a
  `Promise.all` and checking the re-run against the journal; divergence;
  stale-generation fencing; each limit; seed, clock, and `sleep` determinism.
- `CodeSessionWorkflow`: duplicate `call_requested` delivery; each rejection
  as a typed event; approval wait and decision; cancellation from the run to
  an in-flight sub-agent; rollover with calls in flight.
- Projections: a code session's calls in the common tool views, and the graph
  projection from recorded events.
- A live end-to-end suite, marked `#[ignore]`: an agent calls `code_run` with a
  script that fetches pages concurrently and runs `agent_run` on each.

## Open questions

- Where the runtime lives: a workspace under `platform/`, or its own
  repository.
- The origin kind for a child code session: a new `Code` kind, or the
  existing sub-agent origin with the backend recorded separately.
- Large values in the runtime. The session log stores refs, but the script
  needs real values, and Temporal payloads have size limits. Options: pass refs
  to the runtime and let the prelude resolve them through a host call.
- Whether the divergence rule is strict enough without rejecting legitimate
  scripts.
- Multiple runs: whether a later run can reuse an earlier run's completed
  calls, so that fixing a script and re-running it skips work already done.
- The cost of two log appends per call, and whether appends should batch
  calls that complete together.
- Operational prerequisites: Update support on the deployed Temporal server,
  Worker Loader in self-hosted workerd, and CPU limits for self-hosted
  isolates.
- Where agent-node limits come from when the parent has no
  `features.subagents` grant, and which profiles a script may run.

## Non-goals

- A graph definition format or expression language.
- Static proof that data between calls type-checks; schemas are enforced at
  every call instead.
- Network access, npm modules, or filesystem access from scripts.
- Stepping through or editing a running script.
