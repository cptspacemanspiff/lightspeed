# Program sessions: implementation plan

Working plan, not yet a roadmap document. It covers adding **program sessions** to
the Lightspeed backend. A program session is a session type whose runs are
executed by a pinned, deterministic, durable program instead of a model. The
first executors are JavaScript. The program calls ordinary tools, agent sessions
(sub-agents), and other program sessions (subprograms). Complex work therefore
breaks down into a tree of small units, each of which is as deterministic as it
can be:

```
bot ──▶ program session ──┬─▶ tools (VFS, web, env, MCP…)
                          ├─▶ agent session      (non-deterministic step)
                          └─▶ program session ─┬─▶ tools
                                               └─▶ agent session
```

Every node is a durable session with its own log and trace. A parent records each
child's result as a call outcome, so the parent's replay never re-runs a child.

File references are from a survey of `main` at `20f994bd`, re-verified against
that commit on 2026-10-13. workerd references are to `cloudflare/workerd` at
`7d452ca` (release 2026-10-14). Cloudflare code mode references are to
`@cloudflare/codemode` in `cloudflare/agents` at `b1e3614`, under
`packages/codemode/src` unless a `docs/` path is given.

**Prior art.** Two Cloudflare designs cover the two halves of this seam and are
used as references throughout. workerd defines how authority reaches untrusted
code and what a call looks like: capabilities arrive only through `env`, calls
are methods on stubs, the standard globals stay but the host owns them. The
`@cloudflare/codemode` durable runtime defines recording and replay: a per-call
log, replay-or-execute decisions, divergence as data, approval as pause and
resume. The plan departs from the second in two places where Lightspeed's
session-owned effects are stronger, called out in D7 and D8.

## Goals

- A bot (or client) creates a **program session** from a named, versioned program
  and submits runs to it with JSON input. Nothing else is needed: no model, no
  instructions.
- **Composability.** Programs call subprograms and sub-agents with typed input and
  output, using the same delegation machinery and tree limits as sub-agents today.
  Agents can call programs too.
- **Same authority model as agent sessions.** Every effect is an admitted tool call
  under the session's grants, with recorded outcomes. A program child's grants
  are a subset of its parent's. That subset check is new work (D13): delegation
  today only narrows numeric limits, and extending the check to sub-agents is a
  separate, behaviour-changing step.
- **A tool API made for programs.** Programs call canonical, versioned tool
  contracts. Model provider presentation never affects them, and session tooling
  refreshes don't break them.
- **Durable.** A program runs inside its Temporal workflow, so losing a worker
  replays it from the workflow's history. Completed calls, including whole
  child sessions, are never made again. Long waits hold no worker.
- **Observable.** Narration, spans, and every call with its args, result, reason
  and source location appear in the session event stream and web transcript.
- **Repairable.** A separate agent session with an inspect grant can read a failed
  program session's trace, dry-replay patched source, publish a new program
  revision, and resume the work without repeating completed calls.
- **Executor-independent.** An executor is a program instance the workflow
  steps (D9). In-process QuickJS (`crates/program-engine`) is the first and
  runs everywhere. A WebAssembly component or workerd behind the same step
  interface could give stronger isolation later (D9, D12). Neither choice
  changes anything above.

## Non-goals (for now)

- A separate reducer or storage backend. The session *type* is new, but the core
  is shared (D1). External harness sessions are a different problem: see
  `docs/roadmap/p185-external-harness-sessions.md`.
- Mixing model runs and program runs in one session. Handoff is between sessions
  (D10).
- Building program sessions *on* code mode. Program sessions and code mode become
  two clients of a shared scoped-call core (D3). Code mode's behaviour is
  unchanged.
- Checkpointing interpreter heaps. Durability comes from replaying the
  workflow's history; the only saved program state is an explicit JSON
  value (`ctx.continueAsNew` / `ctx.complete`, D7, planned).
- Direct network egress from program code. All I/O goes through tools.
- Re-executing recorded reads on replay to keep the log small, as Cloudflare's
  `replay: "reexecute"` does (`connectors/base.ts`). Results live in CAS by
  reference, so the journal stays small without giving up determinism.

## Key decisions

**Status (2026-10-14).** The user ratified D6, D7 and "programs get no turns"
first, then, after the D14 discussion, D4 (digest pinning now, named
versions later, with the caveat that tool versioning still needs real
design before the first incompatible change: see the open question), D8 as
written, D13 as written, and D14 with the recommended defaults recorded in
it. Questions for the maintainer stay under "Open questions".

**Revision (2026-10-15, user).** D7 and D9 changed after the first Phase 4
build. The program no longer re-executes in retryable activities against a
journal read from the session; it runs **inside its Temporal workflow**, on
a stepped QuickJS instance in its own crate (`crates/program-engine`,
separate from code mode's `codemode`). Temporal's history is the journal.
The activity design (attempts, hold window, suspension and wake) was built
and then removed. D14's `env.state` became the program context (`ctx.context`
/ `ctx.setContext`). The sections below are updated to match.

**Revision (2026-10-15, user), long runs.** Admission became a local
activity (built). Long runs use reference payloads, an explicit
`ctx.continueAsNew(state)`, `ctx.shouldContinue()` and a typed
`history_exhausted` failure (D7 "Long runs"); heap checkpoints, history
rollover and outcomes in the wake signal were considered and rejected.

**Revision (2026-10-16, user), program state and exits.** One carried
value, `ctx.state`, with two explicit exits: `ctx.continueAsNew(state)`
(same run, new execution) and `ctx.complete(result, state)` (the next run
starts from it). It replaces `ctx.context` / `ctx.setContext`. Every exit
cancels the calls the program still owns and waits, as a workflow await,
until each has a recorded outcome; `detach()` keeps a promise-backed call
alive. Every call gets `cancel()`. Limits bound the run: `max_calls` per
run (default 50,000), a new deterministic `max_compute`;
`max_wall_clock_ms` is removed. Details in D7 "Long runs".
After code checks the same day (agreed with the user): withdrawal at an
exit is the default and `detach()` the only explicit way to keep a
request; a cancelled input request or approval settles only when its
workflow confirms it, because the inbox row, not the session, decides an
answer racing a withdrawal.

### D1. A session type at the edges, the shared harness in the middle

**The session kind** is fixed when the session is created and recorded in the
session's lifecycle events:

```rust
enum SessionDriver {
    Agent,                                 // today's sessions (default; absent = Agent)
    Program { program: PinnedProgram },
}

struct PinnedProgram {
    program_id: Option<ProgramName>,  // named program, if any
    revision: Option<u64>,            // resolved revision at creation
    source_ref: BlobRef,              // module bundle (modules map + main module); retained via contains edge
    engine: ProgramEngine,            // { kind: Js, semantics_version }
    requires: BTreeMap<LogicalToolId, ContractPin>, // D4: schema digest now, named version later
    input_schema: Option<BlobRef>,    // JSON Schema, validated at run admission
    output_schema: Option<BlobRef>,   // validated at completion
    limits: ProgramLimits,            // calls per run, in-flight, compute per run, memory
}
```

- The pin is **resolved and frozen at creation**: a later revision of a named
  program never changes a running session. This is what makes replay valid.
- **Runs stay.** Bots, `notify_on_terminal`, run views and idempotent
  `submissionId` all work in terms of runs. A program session's run is one
  invocation of its program:
  - input JSON in, output JSON out
  - `RunRequestSource` (`crates/harness/src/core/components/run.rs:328-341`) is a
    tagged enum with a single `Input` variant whose encoding is documented as
    stable for future source kinds, so add `Program { input_ref }` (and mirror
    it in `RunSource`)
  - admission rejects `Input` runs on program sessions and `Program` runs on
    agent sessions.
- A **subprogram** child session normally gets one run and is closed afterwards,
  like a sub-agent child. A **bot lane** may keep a long-lived program session and
  submit one run per event.

**Why not a separate backend:** a program needs everything the native core does:

- tool admission against grants and effect application
- run-scoped promises
- sub-agent delegation
- VFS and environment state
- run terminal side effects, fork safety, projections

An external harness owns its tools, which is why p185 needs its own reducer. A
program owns none; all its effects are Lightspeed tool calls. The session-type
abstraction is therefore `driver` on a shared core: Agent (model planner) and
Program (program workflow) here, with External (own reducer) possible later.

### D2. Programs are a profile kind, not a new registry

Named programs need what profiles already have: a store, names, revisions,
validation, `Named | Inline` sources, and `SessionConfig` grants.
`ProfileDocument` is `{config, instructions, metadata, retention}`
(`crates/api/src/profiles.rs:179`). Add a `program` field: a profile with
`program` set describes a program session, and its `config` carries the grants
(features, tools, environments) the program may use. No model or instructions are
required.

Bots and delegation reuse profile resolution unchanged. For example,
`ensure_session` (`temporal-runtime/src/bots/sessions.rs:410-546`) already reads a
profile and starts a managed session. Pinning (D1) snapshots the resolved
revision into the session.

**Revision history is new, but only repair needs it.** The profile store keeps
one row per profile holding the latest document and a bumped `revision` counter
(`store-pg/src/profile.rs`, table `agent_profiles`, migration
`006_agent_profiles.sql`). A pinned session is safe because it retains
`source_ref`, and a subprogram call records the resolved revision and source
ref in its arguments (D13), so neither needs past revisions. Only
`program_source(program@rev)`, explicit `name@rev` pinning and resume from an
older revision (D10) do. Add an append-only `program_revisions` table keyed by
`(profile_id, revision)` holding the program spec, written on every put of a
program profile, in Phase 7. This is the main argument for a sibling document
type rather than a field on `ProfileDocument`; either shape works as long as
revisions are retained.

### D3. Extract a scoped-call core; code mode and programs are its two clients

**What a code-tool scope is today.** It is the session-side permission and
bookkeeping record that lets code running *outside* the session make tool calls
*as* the session. The code holds no authority of its own.

Its mechanism is exactly what programs need:

- **Session-held authority.** The script holds only an execution id and opaque
  binding handles, and every call is checked against grants.
- **A non-model admission path.** It runs while the model batch is parked.
- **One effect per request.** Calls are deduped by `(execution_id, request_id)`.
  Identical redelivery, even on a closed scope, is admitted as a no-op by the
  core (`code_tool.rs:525-533`) and the workflow handler serves the recorded
  outcome (`workflows/session/code_tools.rs:282-297`).
- **A single record of outcomes**, kept in the session.
- **The normal effect path** for VFS, environment selection and promise effects,
  with promise ids reserved per call.
- **A dispatcher** with concurrency and exclusivity barriers and environment
  readiness checks.
- **Close semantics:** stop admission, optionally cancel pending calls, report.

Its **policies** are specific to code mode and wrong for programs:

| Code-mode policy | Where | Why it's wrong for programs |
|---|---|---|
| Owned by a parked `code_execute` invocation; liveness = parent batch parked | `CodeToolScopeSpec.parent_invocation_id`, `validate_live_parent` (`code_tool.rs:303-341`) | A program run has no model call |
| Tool names and arguments follow the model's provider presentation (Claude-style `Bash`, Codex-style, canonical), pinned per execution | p191 "Model-facing specification and execution binding" | Program source outlives providers and presentations |
| Calls rejected when the session's toolset revision changes | `validate_call` (`code_tool.rs:402-483`) | Long-lived programs would die on every tooling refresh |
| Short-script budgets: 1024 calls, 64 in flight, 4096 bindings, 32 scopes/run, 256 waiters/session | `code_tool.rs:17-19` (4096 is an inline literal at `:351`), `session/code_tools.rs:14` | Programs are long and wide |
| Workflow-tool emissions capped at 32 per run, with pending code-tool workflow calls counted against the cap | `MAX_WORKFLOW_TOOL_EMISSIONS_PER_RUN` (`workflow_tool.rs:16`), counted in `validate_call` (`code_tool.rs:445-471`) | A citation pass over 100 sources issues more than 32 `agent.run` calls in one run (D13) |
| Completed call results held in `CoreAgentState` for the current run; prior-run scopes pruned only when the next run opens a scope | `CodeToolState{scopes}`, `code_tool.rs:999-1010` | Bounded by 32×1024 today, but one long program run would carry every result in state |
| `invoke_code_tool` blocks until the call is terminal, including while `Waiting` on a joined child | `session/code_tools.rs:85-112`, `is_terminal` at `code_tool.rs:201-203` | The open Update keeps `waiters > 0` and blocks continue-as-new (`wait_loop.rs:108-148`) for a child's whole lifetime; dropping the activity does not close the handler |
| Approval-required calls rejected | `approval_decision` cleared in `code_tool_request` (`code_tool.rs:974-983`); the rejection itself is in the per-call activity (`temporal-runtime/src/worker/activities/tools.rs:556-599`) | Pipelines will want "wait for a human" |

**Extraction.** Move the mechanism into a generic harness component, called
`scoped_call` here, with a per-scope **owner** and **policy**, both recorded at
open and immutable afterwards:

```rust
struct CallScopeSpec {
    scope_id: String,                 // today's execution_id
    owner: ScopeOwner,
    policy: ScopePolicy,
    bindings: BTreeMap<BindingId, ScopeBinding>,
}

enum ScopeOwner {
    ModelInvocation { parent_invocation_id: WorkflowToolInvocationId }, // code mode
    Run { run_id: RunId },                                             // program run
}

struct ScopePolicy {
    binding_mode: BindingMode,        // Presentation { variant } | Canonical (D4)
    revalidation: Revalidation,       // ToolsetRevision (code mode) | PerBindingGrant (programs)
    completion: Completion,           // Blocking (code mode) | Ack (programs)
    limits: ScopeLimits,              // max_calls, max_in_flight
    retention: Retention,             // FullInState (code mode) | Indexed (programs)
    approvals: ApprovalPolicy,        // Reject (both, initially) | Park (programs, later)
}
```

Code mode opens scopes with exactly today's behaviour:
`ModelInvocation` + `Presentation` + `ToolsetRevision` + `Blocking` + current
limits + `FullInState` + `Reject`. Programs open them with
`Run` + `Canonical` + `PerBindingGrant` + `Ack` + program limits + `Indexed`.

What goes where:

- **Owner view.** One helper, `scope_owner(state, spec) -> OwnerView { run_id, model_batch: Option<(TurnId, ToolBatchId)> }`,
  replaces the direct calls to `code_tool_parent` (`code_tool.rs:261-276`) in:
  - `interrupt_code_tools_for_run`
  - scope retention on open (`code_tool.rs:999-1010`)
  - per-run budgets
  - `joins()` (`code_tool.rs:485-499`)
  - `CallDeferred` validation (`code_tool.rs:1109-1121`)
  - `validate_emit_effect` and `validate_code_tool_invocation`
    (`workflow_tool.rs:1576-1668`, `1968-1993`).

  A `Run` owner stamps `run_id` in joins, with no turn or batch.
- **Liveness.** `ModelInvocation`: today's parked-parent check. `Run`: the run is
  Active, Parked or Cancelling, the session is a program session, and the scope
  id matches the run's execution.
- **Calls outside a model turn need a call context.** This is needed for any
  non-model caller, not just this design. Starting `agent_run` or `program_run`
  creates a `WorkflowToolInvocation`, and `ToolInvocationBatchRequest`
  (`core/io.rs:95-99`) requires `turn_id`/`batch_id`. Introduce
  `CallContext = ModelBatch { turn_id, batch_id } | Scope { scope_id } | Run`.
  `Run` is the context of the program run's own invocation (D6); `Scope` is a
  call made from inside a run-owned scope. Make the stored join fields `Option`
  with `serde(default)`, and replace
  `WorkflowToolInvocationId::for_call(..., turn_id, batch_id, ...)` derivations
  with one that takes a `CallContext`. Do not fabricate synthetic turns. Code
  mode never uses the new variants, so this lands in Phase 2, not Phase 1.
- **Completion `Ack`.** The invoke Update returns as soon as the call is admitted
  or deduped, carrying the recorded outcome only if the call is already
  terminal. Outcomes are then observed only through `scope_recorded_outcomes`
  (D7), never through an invoke Update's return value, so a replay sees
  completions in the same order. `Blocking` keeps today's wait-for-terminal
  behaviour for code mode. `Ack` is what lets the program's workflow wait for
  calls without an Update held open on the session.
- **Approvals.** The approval policy must reach the per-call activity
  (`activities/tools.rs:556-599`), which is where approval-required calls are
  rejected today; clearing `approval_decision` in the core is not the gate.
  `Park` follows Cloudflare's durable runtime (`runtime.ts` `decide`,
  `proxy-tool.ts` `resumeCodemode`): an approval-required call is admitted as
  a recorded `Pending` entry awaiting a decision, the program simply waits on
  it (D7), an approval completes the call through the ordinary
  per-call activity and wakes the run, a rejection completes it as a typed
  failed outcome the program can catch, and an unanswered pause expires after
  a configurable TTL (Cloudflare's default is 24 hours) as a rejection. The
  decision is a promise the session owns, so the executor needs nothing new:
  a parked call is just a slow call.
- **Retention `Indexed`.** State keeps full records only for non-terminal calls.
  Terminal calls shrink to a compact index entry:
  `request_id → { spec_digest, status, output_ref | error_ref }`. Admission dedup
  needs only the digest. Full specs and results stay in events and CAS, and the
  replaying executor fetches them through a batched
  `scope_recorded_outcomes(scope_id, after_request, limit)` query instead of one
  Update per recorded call.
- **Wire compatibility.**
  - Keep the stored kind strings `lightspeed.core.code_tool.*` and the existing
    field names. `CallScopeSpec` decodes legacy events (no `owner`/`policy`) as
    code-mode defaults, using the `CodeToolCallResultWire` shim pattern
    (`code_tool.rs:163-190`).
  - On the workflow side, `session/code_tools.rs` becomes `session/scoped_calls.rs`.
    Keep the existing Update names (`open_code_tool_scope`, `invoke_code_tool`,
    `close_code_tool_scope`, `code_tool_scope_report`) registered for code
    executions in flight during an upgrade. Program scopes use the same handlers
    with an owner/policy-bearing open request.
- **Invariant.** The extraction lands **before** any program behaviour, and the
  existing code-tool unit, replay and live suites pass unchanged
  (`core/drive.rs` code-tool tests, `code_tools_live.rs`, `code_workflow_live.rs`).
  Code mode is the regression gate for the refactor.

### D4. Canonical, versioned tool contracts for programs

Programs must not call `tools.Bash`, a name that exists only because a Claude-style
presentation was active. Add a program-facing contract layer in `crates/tools`:

- **Built-in tools.** Each logical operation (`vfs.read`, `env.run_process`,
  `web.fetch`, `agent.run`, `program.run`, `promise.await`, `time.sleep`, …)
  publishes a **contract**: a canonical argument schema, a result schema, and an
  adapter to the logical operation. The owned result schemas built for code mode
  (`FunctionDefinition.output_schema`, `tools/src/runtime/mod.rs:21-30`, filled
  in `tools::callable`) are the starting point. The first contract pin is the
  digest of the argument and result schemas: a program records the digest it was
  written against, and a changed schema fails that binding with a typed
  `ContractChanged` error. Named contract versions, with old versions kept alive
  while pinned programs use them, come when the first incompatible change to a
  built-in schema is actually needed, not before. Ratified with a caveat:
  digest pinning only detects a change, it does not keep an old program
  working, so how versions are named, how long old ones live, and how a
  pinned program moves to a new one need design before the first breaking
  change ships (open questions).
- **MCP tools.** Exposed as `mcp.<server>.<tool>`, with the contract pinned to the
  tool's input schema hash at session creation. A changed upstream schema fails
  that binding with a typed `ContractChanged` error, not silent drift.
- **Admission up front.** `PinnedProgram.requires` lists the contracts the program
  uses. Session creation fails if any required contract is ungranted or
  unsupported, instead of a program failing at call 400. The workflow
  re-checks `requires` when it loads the program, as Cloudflare's resume
  refuses a run whose connectors are no longer configured (`proxy-tool.ts`
  `resumeCodemode`), so a revoked binding fails the run with a typed error
  before it starts rather than at the next call. Each call is still checked
  against current grants (`PerBindingGrant`). Unrelated tooling changes don't
  invalidate bindings.
- **Waiting is a tool.** `time.sleep` is a durable wait that completes through
  the session's promise deadline machinery; it is an ordinary call with a
  recorded outcome, so replay returns the same value. The guest's `setTimeout`
  lowers to it (D8). Reading the clock is not a tool: `Date.now()` is a host
  channel fed by recorded timestamps (D8), the way workerd feeds it from its
  I/O context. Pipelines need deadlines, and this is the only way to express
  them under D8.
- **Guest surface, the workerd way.** A program is a module with a handler, not
  a script body with a global `tools`:

  ```js
  export default {
    async run(input, env, ctx) {
      const page = await env.web.fetch({ url: input.doi }, { why: "¶3 cites this" });
      return await env.agent.run({ agent: "verifier", input: { page, claim: input.claim } });
    }
  };
  ```

  workerd hands a worker its capabilities only through `env`, and Cloudflare's
  code mode exposes each tool namespace in the sandbox as a `Proxy` whose
  property access yields a function that makes one host call (`executor.ts`
  `proxyInits`). Here `env` is built by the host from the pinned bindings in
  `PinnedProgram.requires`, one namespace per contract family (`env.vfs.read`,
  `env.web.fetch`, `env.agent.run`, `env.program.run`, `env.mcp.<server>.<tool>`,
  `env.time.sleep`, and from D14 `env.input.request`). Each entry wraps an opaque binding handle from the scope
  (D3); the guest holds no names it can forge. `ctx` carries `say`, `step`,
  the run's `startedAt` (D11), and from D7 the program state (`ctx.state`,
  `ctx.continued`, `ctx.continueAsNew`, `ctx.complete`; this replaced D14's
  `ctx.context` / `ctx.setContext`). Every call returns a promise with
  `cancel()`; promise-backed calls also have `detach()`. Calls are methods only, arguments by value as
  JSON, no property reads across the boundary. The host never rejects across
  the bridge: a pause, a failed outcome or a host error come back as a control
  value and the generated wrapper throws locally, so the program's own
  `try/catch` sees an ordinary exception (`executor.ts`
  `CONNECTOR_CONTROL_KEY`). Argument and result schemas generate the
  TypeScript declaration of `env` for authors and models writing programs.
- **Rollback, later.** Cloudflare tool contracts may carry a `revert`
  (`connectors/base.ts`) and its runtime can roll back applied actions. A
  contract-level `revert` would make D10's orphaned-effects report
  actionable. Reserve the field; do not implement it in these phases.

### D5. Gate model-only behaviour on the session driver

Most gates become one check: `state.driver.is_program()`.

| Place | Program session behaviour |
|---|---|
| `turn::plan_next` (`turn.rs:50-92`) | never start a turn |
| `context::plan_next` / `missing_run_input_entries` (`context.rs:897-965`, `1143-1171`) | no model context at all |
| compaction and the context-failure terminal arm (`run.rs:473-499`) | never planned, never reached |
| `validate_run_config_for_state` (`config.rs:793-803`; callers `admit.rs:282`, `config.rs:1632`, `1662`) and `validate_recorded_run_config` (`config.rs:807-821`, `run.rs:655`) | skip model validation; validate input against `input_schema` |
| session config validation | a model is not required |
| `tool_invocation_request` `.expect("active run has a configured model")` (`drive.rs:947-958`) | `Canonical` bindings need no presentation variant |
| `terminal_run_proposal` (`run.rs:465-597`) | program runs complete only through D6 |
| Cancelling → Cancelled (`run.rs:802-806`) | also wait for the run-owned scope's calls to be terminal |
| steering, context edits, compaction controls, model changes | typed `UnsupportedForSessionDriver` errors |

### D6. Starting and finishing a program run

A program run is a **workflow-tool invocation owned by the run** rather than a
new session command. The generic workflow-tool protocol already provides
starts, replies, holder cancellation, recovery queries and idempotent promise
resolution, and AGENTS.md asks that workflow-backed work use it instead of a
feature-specific transport into the session worker. The invocation's call
context is `CallContext::Run` (D3).

- **Start.** When a run on a program session is Accepted/Started, the harness
  emits a start request for a `ProgramRunWorkflow` invocation on the
  `program` task queue (`Role::Program`, `DEFAULT_PROGRAM_TASK_QUEUE` in
  `temporal-runtime/src/config.rs`) with a deterministic execution id
  (`wtx:…`, derived from the invocation and the binding's recipe). `start_execution`
  (`worker/activities/workflow_tools.rs:28-105`, policy in
  `execution_start_options` at `:107-112`) uses `RejectDuplicate` because a
  code execution must never run twice. A program run must be restartable if
  its workflow fails before it completes the run, so the start policy becomes
  a per-kind option and program runs use `AllowDuplicateFailedOnly`, treating
  `AlreadyStarted` as success. The workflow never exits without replying: its
  top level catches every error and replies `Failed`.
- **Finish.** The workflow resolves the invocation's reply completion promise
  with `Completed { output } | Failed { report_ref }` through the ordinary
  `deliver_emission` path. The harness, on resolving a run-owned invocation's
  reply, validates `output` against `output_schema` and produces
  `Run::Completed`/`Failed`, reusing `with_run_terminal_side_effects`
  (`drive.rs:428-458`) and `notify_on_terminal`. Promise resolution is already
  idempotent, so a redelivered reply is a no-op and a differing one is a
  conflict. A schema failure is a `Failed` run with a program failure report.
- **Cancel.** `CancelRun` sends the existing holder cancellation
  (`InvocationCancellation` through `deliver_emission`) to the program
  workflow. The workflow closes the scope with `cancel_pending`, waits for
  terminal outcomes, and replies cancelled. Today `cancel_pending` reaches only
  `Waiting` joined calls whose promise is runtime-owned, by a best-effort signal
  (`code_tool.rs:876-924`, `promise_sources.rs:198-243`); `Pending` calls with a
  per-call activity in flight are not cancelled, so the drain could wait on a
  long `env.run_process`. For run-owned scopes, close-with-cancel must also
  request cancellation of pending per-call activities, and the drain gets a
  bounded deadline after which remaining calls are force-completed as
  interrupted, as `interrupt_code_tools_for_run` already does at run end
  (`code_tool.rs:30-75`, called from `run.rs:1041`).
- **Failure with work in flight.** When the program throws while calls
  are still pending (for example a child session still running), the workflow
  closes the scope with cancel and drains exactly as for cancel before it
  replies `Failed`. A program that throws must not leave orphaned children.
- **Recovery.** The existing `workflow_tool_recovery` query applies unchanged.
- **Retention.** Record contains edges session → `source_ref`/schemas and
  run → `input_ref`, following the pattern in `session_tools.rs:780-822`.

### D7. Durability: the program runs in its workflow

Revised 2026-10-15. The first design re-executed the program in retryable
`program_attempt` activities against a journal read from the session, with a
hold window, suspension and a wake emission. It was built and then removed:
it re-implemented Temporal's own replay. The program now runs **inside**
`ProgramRunWorkflow`, and the workflow's history is its journal. Code mode
keeps its single-attempt activity.

- **`ProgramRunWorkflow`** (`program` role) owns the run's scope for the run's
  lifetime and closes it only on a real terminal or a cancel. It hosts a
  stepped program instance (D9) on the workflow thread:

```
prepare          (activity)        → descriptor
program_load     (activity)        → source, input, context, contract bindings (pins re-checked)
start the instance                 → Waiting { requests } | Finished { requests, report }
loop:
  requests  → program_admit (one local activity per step): invoke Updates
              with Ack; refusals come back as failed completions for the next step
  finished  → program_conclude (activity): keep the context, close the scope,
              store the report → finalize (activity) replies to the session
  otherwise wait for a ScopeProgress signal from the session, or cancellation
  program_collect (local activity) → recorded outcomes after the cursor, as JSON
  step the instance with the refusals and collected outcomes
```

- **Determinism.** Every input to a step is in the workflow's history: the
  loaded program (an activity result), admission refusals and collected
  outcomes (local-activity markers) and wakes (signals).
  After a cache eviction or a lost worker, Temporal re-runs the workflow
  function, which builds a fresh instance and feeds it the same inputs in
  the same order, so it issues the same requests; Temporal's command check
  catches a divergence. Re-issued admissions are no-ops at the session,
  which dedups by request id.
- **Waking.** When a call in a run-owned scope completes, the session sends
  the program workflow a `ScopeProgress { execution_id, completed }`
  emission with the scope's count of recorded outcomes. The workflow
  collects when that count passes its cursor, so a wake that arrives early
  is never lost.
- **Waiting.** A waiting program is a workflow blocked on a signal: no
  interpreter thread, no activity slot, no Update held open on the session.
  Its heap stays in the worker's sticky cache until evicted; an evicted
  program replays its history when it next wakes. A wait is bounded by its
  own deadline (an input request's, a sleep's) and the program-execute
  binding's 30-day joined ceiling; a run has no deadline of its own (D7
  "Long runs" item 5).
- **Refusals in history.** Admission refusals (permissions, limits, a closed
  scope) reach the program as failed completions recorded in history, so a
  replay sees the same ones.
- **CPU.** Steps run on the workflow thread of the worker that holds the
  workflow (the Rust SDK runs a worker's workflow code on one thread). Each
  step has a deterministic budget (D9), and heavy work belongs in tool
  calls. Program workflows run on their own worker role and task queue
  (`program`), apart from sessions and code mode.
- **History budget.** Measured on a sequential program
  (`each_sequential_call_adds_a_bounded_number_of_history_events`), each
  further call costs **6 events**: the session's `ScopeProgress` signal,
  one workflow task (3), and the `program_collect` and `program_admit`
  markers. That is ~8K sequential calls under Temporal's ~51K-event limit;
  calls issued together share one admission and, when they complete in one
  session batch, one wake. Until `ctx.continueAsNew` is built, `max_calls`
  (default 1024) keeps runs inside it. Collected results are still inline in the collect markers and
  count toward the 50 MB limit. See "Long runs" below.
- **Admission is a local activity** (decided 2026-10-15). It was a
  server-scheduled activity, which cost 3 activity events and a second
  workflow task per step (11 events per call). A local activity runs on the
  program worker inside the current workflow task and leaves one marker.
  The tool calls themselves still run as the session's ordinary activities;
  only the hand-off to the session moved. A rerun after a lost result
  (worker died before the marker was recorded) is safe: the Update dedups by
  request id, the argument blob is content-addressed, and the program only
  ever sees the result history recorded. Options: 5 s per try, retries
  bounded by 120 s total (the old activity's budget); a stalled session
  costs extra workflow-task heartbeats and delays signals such as a holder
  cancel until the admission returns. Live test
  `a_lost_admission_result_is_admitted_again_without_calling_the_tool_twice`
  (the first attempt admits, then fails; the MCP server is called once).
- **Long runs** (agreed 2026-10-15, revised with the user 2026-10-16). In
  order:
  1. **Program values out of history** (built, `40af5026`..`0d258111`).
     Workflows see only blob references; activities read and write the
     bytes. The program workflow is the exception, since its JS needs
     values. `program_load` and `program_collect` return the session-owned
     references (source, input, state, tool output and error) over a
     threshold (16 KiB, `LIGHTSPEED_PROGRAM_INLINE_VALUE_BYTES`). A
     decode-only worker `PayloadCodec` fetches the bytes from the
     universe's content store, so replay sees the same values. The codec
     never encodes: the SDK gives it no workflow context, and ownership
     stays with the session's records. Values the program creates (its
     output and its handed-over state) go to local activities, whose
     arguments Temporal does not record, as with `program_admit`'s tool
     arguments.
  2. **One state, two clean exits.** A program carries exactly one JSON
     value forward, `ctx.state`, and ends a run's execution in one of two
     explicit ways:
     - `return ctx.continueAsNew(state)`: same run, same `input`, a new
       execution (Temporal's continue-as-new). `run` is called from the top
       with `ctx.state = state` and `ctx.continued = true`.
     - `return ctx.complete(result, state)`: the run ends with `result`; the
       next run (new input, whenever the session gets one) starts with
       `ctx.state = state` and `ctx.continued = false`.

     Both take JSON under the same rules and cap (64 KiB) and serialize when
     called. Each returns an unforgeable token; it takes effect only when
     `run` returns it, so a `try/catch` cannot swallow it (unlike the
     Temporal TypeScript SDK's throwing `continueAsNew`), and calling one
     without returning it, or twice, fails the run. Nothing else survives
     an exit: locals, closures and promises are gone, so the author writes
     the program as a loop that resumes from its state. A plain
     `return value` or a failure leaves the state as it was when the run
     started (a failed run does not hand over a mid-run checkpoint).
     `ctx.context` and `ctx.setContext` are replaced by this; internally the
     `complete` state is stored as today's program context entry when the
     run completes (retained by `context_retained`, shown in the session
     view). Typical shape:

     ```js
     const s = ctx.continued ? ctx.state : { i: 0, head: ctx.state?.head ?? null };
     // ... loop; every N items: return ctx.continueAsNew({ ...s, i: s.i + 1 });
     return ctx.complete({ handled }, { head });
     ```

     Small, bounded state only (cursors, counters, this run's delta, a
     reference). Data that grows with calls or runs belongs in workspace
     files (`vfs.*`) or CAS (`blob.put`, e.g. linked chunks), with a
     reference in the state; this is Temporal's own advice for
     continue-as-new input.
  3. **Exits settle every call.** One rule for every exit (`complete`,
     `continueAsNew`, a plain `return`, `throw`): every call the program
     still owns is cancelled, and the exit waits until each has a recorded
     outcome. Nothing in flight crosses an exit, so there is no
     `calls_in_flight` refusal and no per-contract kinds (wait, read,
     effect). Cancels resolve the way each call can: a sleep, await or
     read settles at once; an input request is withdrawn unless answered
     first; an effect stops or finishes and records which; a process is
     killed after its grace. Program scopes' tool activities use
     wait-cancellation-completed, so the recorded outcome is always the
     activity's real one, never an assumed "cancelled". Costs: a call the
     program did not await is cancelled (await what you need), a process
     can be stopped partway, and exits wait for acknowledgements (about one
     heartbeat, 2 s, plus a tool's cleanup).
     - **The cleanup is a workflow await, not JS.** The program has already
       returned, so the host does it: one local activity closes the scope
       with `cancel_pending` (close and return, no polling), then the
       workflow suspends on `ScopeProgress` wakes until
       `recorded == admitted`, exactly like a JS await but with nothing
       delivered to the engine, and reads the outcomes for the run report.
       It holds no worker and has no deadline (today's 10 s polling inside
       `program_conclude`, and its "outcome unknown", go away). Every way a
       call can end must emit `ScopeProgress`. Checked 2026-10-16: a call
       the session admitted but had not yet started when the cancel arrived
       is recorded through `CompleteCodeToolCall`, whose `CallCompleted`
       queues the wake like any completion, and cancelled waits resume
       through `ResumeCodeToolCall` the same way. (A call started and
       cancelled in the same step never reaches the session and needs no
       wake.)
     - **Withdrawal is the default; `h.detach()` is the only way to keep
       a request.** Keeping a request open past an exit is always explicit
       in the program; it is the escape hatch for carrying a wait across
       executions or runs, never an implicit default. `h.detach()` exists
       on `input.open` handles (model-owned promises; later jobs and
       sub-agents) through the existing `promise.detach`. The program puts
       `h.id` in its state and rebuilds the handle later with
       `env.input.handle(id)`, so a run can continue as new while it waits
       days for an approval. `input.request`'s promise is runtime-owned and
       cannot be detached; activity-backed calls cannot be detached either:
       their scope's close waits for them.
     - **The close withdraws the run's own undetached requests**, the
       promise and the inbox item, not only the call awaiting them. Checked
       2026-10-16: a cancelling close already cancels the pending reply
       promise of a waiting `input.request` (`close_code_tool_scope_proposals`),
       but not `input.open` handle promises. Those are run-scoped and are
       cancelled only when the run ends (`with_run_terminal_side_effects`),
       which is too late for `continueAsNew`, where the run goes on. The
       close of a program scope must cancel the run's pending model-owned
       promises; never the run's own runtime-owned completion promise, and
       never in code mode scopes, whose run's promises belong to the model.
     - **A withdrawal settles only when its workflow confirms it.** For
       input requests the inbox row decides the race between an answer and
       a withdrawal (`InputRequestWorkflow`), so the session is not the only
       writer. Today the session marks the promise `Cancelled` at once and
       then tells the workflow; if the answer was saved in the row first,
       the workflow replies `Resolved` and the session drops that reply
       (`admit.rs`, terminal promise). The person sees "answered", the run
       report says "withdrawn". The same holds for an approval: once
       approved, the workflow runs the parked call to the end
       (`execute_approved` does not watch for a cancel), so the call's real
       result is dropped while the program sees "cancelled". Fix, by the
       same rule as wait-cancellation-completed for activities: cancelling a
       promise owned by an input-request workflow records "cancel
       requested" and leaves it pending; it settles with what the workflow
       reports (withdrawn → `Cancelled`, answered or the approved call's
       result → that). The exit's cleanup waits for it like any call.
       Scope this to workflow-owned promises; the model's own
       `concurrency.cancel` keeps its behaviour.
  4. **Per-call cancel.** Every call returns a promise that also has
     `h.cancel()` (today only input handles do). A cancel is a request,
     not an outcome: the session records exactly one outcome per call,
     first wins, and the program reads it from the call's promise. A
     result that lands before the cancel is delivered as the result.
     `await h.cancel()` waits until the call has settled and reports how
     (user, 2026-10-16): `{ status }` with `cancelled`, `resolved` (it
     finished first) or `failed`. A cancel is not done until it is
     confirmed, so the program never sees an in-between "requested". The
     call's promise stays the authority on the outcome. Built in
     `prelude.js` (`withCancel`, `settledStatus`).
     - Guest to session: the engine emits a cancel for an issued request;
       `program_admit` sends it after the step's requests (a cancel always
       names a call from an earlier step) as the `cancel_code_tool` Update;
       the session appends "cancel requested". A call started and cancelled
       in the same step never leaves the engine and settles locally as
       cancelled (`tool_cancelled`), waking nothing.
     - In the session (exists for scope cancel): the dispatcher cancels the
       call's activity (`future.cancel()`); the activity sees it on its next
       heartbeat and drops its work. A call not yet dispatched is recorded
       cancelled without starting. Waits wake `Cancelled` through
       `code_tool_wake`, which today only checks the scope. Tool activities
       use the SDK's default try-cancel today (`tool_call_activity_options`
       sets no cancellation type), so the session records `Cancelled`
       without waiting; program scopes switch to wait-cancellation-completed
       (item 3).
     - Replay: the cancel is admitted like a call, so it is safe to send
       twice (a retried local activity resends it; the session answers with
       the call's current state). The race between a cancel and a result is
       decided once and recorded: by the session for activities and waits,
       by the input-request workflow's inbox row for requests (item 3,
       settled only when that workflow confirms). The program sees only the
       collected outcome, which is a marker in its own history.
       `clearTimeout` cancels its sleep.
     - `h.cancel()` on an input handle uses the same session-side cancel,
       not the `promise.cancel` tool, and waits for the request's answer
       (`input_cancelled` means withdrawn). Checked 2026-10-16: `promise.cancel`
       computes its reply from the promise's status when the call is
       prepared and applies its effect at append, so an answer landing in
       between makes it report `cancelled` for an answered promise (the
       effect itself is right: it skips a settled promise). The status is
       for logging only and the handle stays the authority, but the session
       path computes it when it records the cancel, so the race goes away.
  5. **Executions.** A run is one `ProgramRunWorkflow` (workflow id
     `wtx:…`); `continueAsNew` adds an execution to it (same id, new
     Temporal run id, fresh history). Replays after a lost worker re-run the
     same execution and are not new executions.
     - Each execution has its own scope: `wtx:…` for the first, `wtx:…~N`
       after. Call ids restart, so session state and history stop growing.
       When a run opens a new scope, its earlier closed, settled scopes
       leave session state (the log keeps them).
     - `ScopeProgress` for a run-owned scope goes to the run's workflow id
       (`program_run_execution_id`), not the scope id; the workflow ignores
       wakes for scopes other than its current one.
     - A `program_checkpoint` local activity stores the state (its
       arguments are not recorded), closes the settled scope and opens the
       next. The workflow's input becomes `{ start, continuation }`; the
       continuation carries the descriptor (no second prepare), the
       execution number, the state's reference, the engine clock and the
       calls and compute used so far. The next execution's random seed derives from
       the run and execution number; its clock starts where the last one
       stopped.
     - **Limits (agreed 2026-10-16).** Each bounds a run as a whole and is
       carried in the continuation:
       - `max_calls` now counts **calls per run**, across executions,
         default **50,000** (was 1024 per scope). An execution needs no
         author-set bound: its history is the bound, through
         `shouldContinue()` and `history_exhausted` (about 8,000 sequential
         calls at 6 events each).
       - `max_compute`, new: the run's total of the engine's QuickJS
         interrupt checks (the per-step `StepBudget` unit), summed across
         steps and executions. Deterministic, unlike CPU time, so replay
         stops at the same point. It stops runaways that `max_calls`
         misses, such as a chain of `continueAsNew` with no calls. Units
         are roughly tens of microseconds of tight-loop work; the default
         is about ten times what a realistic 50,000-call run measures (to
         be measured when building).
       - `max_in_flight` unchanged.
       - `max_wall_clock_ms` is removed, with the unused
         `CodeExecutionLimits.timeout_ms` and
         `DEFAULT_PROGRAM_ATTEMPT_TIMEOUT_MS` (10 min). Waiting costs
         nothing now, and waits have their own deadlines
         (`input_deadline_ms`, the sleep). Abandoned runs are cleaned up by
         cancelling them through the session API, or by a policy of whoever
         manages sessions (a bot setting, an operations job). Temporal's
         workflow execution timeout is not used: it ends the chain without
         settling calls or replying to the session. The field is still read
         in one place: it sets the deadline of the run's own completion
         promise (`program.rs`, `deadline_ms`), which goes with it.
       - Pinned programs exist only on this branch, so the changed meaning
         of `max_calls` needs no migration.
     - Signals arriving during the handover are not lost: the server
       rejects a continue-as-new completion with new events and redelivers
       them, and later signals reach the current execution by workflow id.
  6. **`ctx.shouldContinue()`** (true when Temporal suggests continue-as-new
     or our event or byte threshold is near, the same signals as the
     TypeScript SDK's `workflowInfo()`) and a typed
     **`history_exhausted`** failure before Temporal's own cap, so a program
     that ignores the signal fails with a clear error instead of being
     terminated by the server.
- **Considered for long runs and rejected** (2026-10-16, in addition to the
  list below):
  - *Separate memory and continuation channels* (`setContext` plus
    `ctx.continued`): two carry paths that collide when `setContext` is
    called mid-run. One state with two exits replaces both.
  - *Refusing an exit with calls in flight*: blocks a program on calls it no
    longer needs (the losing approvals of a 1-of-3 request).
  - *Classifying contracts as wait, read or effect* to pick per-call exit
    behaviour: cancel-and-settle for everything is one rule and needs no
    classification, which matters for MCP tools whose behaviour is unknown.
  - *A save point before the handover* (checkpoint, keep running, hand over
    later and replay the tail): a bounded form of the rejected history
    rollover. Temporal's model is that the handover is the save point.
  - *Cleanup in JS* (the prelude cancelling its own outstanding promises):
    the program has returned and may be the reason the run failed; the
    host's scope close does it in one message.
  - *A run deadline* (`max_wall_clock_ms` enforced as a durable timer): a
    waiting run costs nothing, waits have their own deadlines, and cleaning
    up abandoned runs is a session-management policy.
  - *A CPU-time limit*: CPU time differs between machines, so replay would
    stop at different points; `max_compute` counts deterministic work.
  - *Two call caps* (per execution and per run): the history already
    bounds an execution.
- **Considered for long runs and rejected** (2026-10-15):
  - *Checkpointing the interpreter heap.* JS cannot introspect its own
    closures or suspended frames. Between steps the JS stack is empty and
    every suspended `async` frame is a heap object, so restoring the heap
    would resume the program with no "goto"; but native QuickJS has no heap
    serializer (`JS_WriteObject` skips closures, async frames, promises and
    native functions) and its heap is absolute pointers. QuickJS compiled to
    WebAssembly could snapshot linear memory and globals cleanly. Rejected:
    host-side engine state (issued ids, budgets, delivered completions) must
    stay in lockstep with the snapshot, a slightly wrong restore corrupts
    silently instead of failing, and a snapshot only loads into the build
    that wrote it. Replay recomputes state and fails loudly
    (`Nondeterminism`) on any divergence.
  - *History rollover with replay from the scope* (start a fresh Temporal
    execution and rebuild the program by replaying every recorded call from
    the scope; this is not continue-as-new, since no program state is
    carried). It lifts only the event-count cap: the same data is stored
    (the scope already holds every call), replay cost grows with the run's
    length on every rollover (quadratic overall), the scope log in the
    session still grows without bound, and it adds a second, rarely
    exercised replay path. This is close to Cloudflare's design
    (`@cloudflare/codemode` `runtime.ts`: abort-and-replay against a log in
    the Durable Object's SQLite, re-running the code from the top on each
    resume), which needs no continue-as-new because that log has no cap.
  - *Outcomes carried in the `ScopeProgress` signal* instead of collected.
    It saves one event and a query per wake, but copies the scope's
    outcomes into a second place and needs gap detection with collection as
    a fallback. The scope is the truth, so the wake stays a nudge followed
    by a read, as `InputRequestWorkflow`'s `answered` signal is for the
    inbox row. (Results are carried in a signal where the receiver's state
    is the truth: a workflow tool's resolution to its session.)
- **Engine upgrades.** A run replays on whichever build picks it up, so a
  prelude or engine change could alter a replay. Temporal worker
  versioning (a running workflow stays on the build that started it, and
  moves at `continueAsNew`) is the intended answer (not built).
  `semantics_version` still pins the guest API.
- **Effects are never repeated.** Effects belong to the session and are
  deduped by request id, so a retried workflow task or a replay never
  applies one twice. This is the strongest reason for D3: Cloudflare's
  durable runtime records a call as `executing` before the effect runs and
  re-executes it on replay if the result was never recorded (`runtime.ts`
  `decide`, the `executing` arm), so a crash between effect and record
  applies the effect twice.
- A parent waiting on ten child sessions for a day holds no worker. Each
  child is a separate session workflow.

### D8. Determinism contract

This is part of the executor protocol (D9) and binds every executor:

1. **Quiescent delivery.** Completions are delivered only when the guest has no
   runnable work, one completion per quiescent point. Code mode's engine
   delivers between jobs so a self-scheduling microtask chain cannot starve
   its bridge; the program engine (`crates/program-engine`) delivers only
   when `execute_pending_job` finds nothing to run, and its step budget stops
   a chain that never quiesces. In V8/workerd it would mean one completion
   per macrotask over a single ordered channel.
2. **History-order delivery.** Completions are delivered in the order the
   program's workflow recorded them: refusals as the admission activity
   returned them, outcomes in the session's log order as `program_collect`
   read them from `scope_recorded_outcomes` (D7). Execution is then a pure
   function of the source, the input and that history, and `Promise.race`,
   process-as-completed loops and `Promise.allSettled` all work. Outcomes
   are never taken from an individual Update's return value. Delivery is
   well defined because a
   completion is logged only after its request was admitted: by the time a
   replaying guest reaches a quiescent point it has issued every request the
   live run had issued before that point, so the next completion in log order
   always names a request the guest already holds. Issuance order is not
   required.
3. **Host-owned globals, the workerd way.** workerd keeps the standard globals
   and makes the host own them instead of removing them
   (`src/workerd/server/v8-platform-impl.{h,c++}`, `src/workerd/io/io-util.c++`,
   `src/workerd/io/io-context.c++`, `src/workerd/jsg/resource.h`). Programs do
   the same:
   - **Clock.** workerd routes `Date.now()` through its V8 platform's
     `CurrentClockTimeMillis` override to `IoContext::now()`: a per-context time
     that is constant between ticks, monotonic across reads, and clamped to a
     fired timer's own deadline when its callback runs, so a timer sees exactly
     its scheduled time. Outside any I/O context it returns 0. Here `Date.now()`
     reads a host channel set to the run's recorded `started_at` at `Start` and
     to the recorded timestamp carried by each delivered `Complete` (D9), and
     never goes backwards. Between delivery points it does not move. The
     QuickJS prelude replaces `Date.now` and the zero-argument `Date`
     constructor; the workerd wrapper module (D12) does the same in module
     scope. There is no `time.now` tool.
   - **Timers.** workerd schedules `setTimeout` relative to `Date.now()` so the
     delay appears exact. Here `setTimeout(cb, ms)` and `setInterval` lower to
     `time.sleep` calls, so a timer is a recorded durable wait with a request
     id, and the clock is clamped to the deadline on delivery exactly as in
     workerd. `queueMicrotask` and promise jobs are untouched.
   - **Randomness.** workerd leaves `Math.random` on V8's own generator; it
     mitigates timing attacks, not replay. Programs need replay, so the prelude
     and wrapper replace `Math.random` with a PRNG seeded from the execution id
     and route `crypto.getRandomValues` and `crypto.randomUUID` through the
     same source.
   - **Dynamic code.** workerd disables `eval` and `new Function` with
     `AllowCodeGenerationFromStrings(false)`. Executors do the same. This is a
     sandboxing rule rather than a determinism one, but it is cheap and the
     program source is already pinned.
   - **Temporal.** workerd installs a constant-zero host clock for the Temporal
     API so the requirement is not forgotten when it is enabled. An executor
     that exposes Temporal must feed it the same channel as `Date.now()`.
4. **Request ids** are sequential per execution and assigned by the guest at
   issuance (`call-N`, as today at `engine.rs:324`). Narration uses a separate
   sequence (D11). This is deliberate: Cloudflare's durable runtime assigns the
   sequence on the host when a call arrives (`proxy-tool.ts` `createCursor`),
   so two calls issued by `Promise.all` can be numbered in either order, and
   its documentation tells authors to await connector calls one at a time in
   any run that might pause (`docs/codemode/runtime.md`). Guest-side issuance
   order is a function of the program alone, so fan-out is replay-safe here.
5. **Divergence.** A re-issued `call-N` whose binding or argument digest differs is
   a typed `Nondeterminism` failure, returned to the program workflow as data
   and recorded in the failure report (D10), never thrown across the bridge.
   Today this surfaces as `Conflict` from the workflow and reaches the script
   as `admission_rejected`; argument refs are CAS hashes of
   `serde_json::to_vec` (`temporal-runtime/src/code.rs:372-392`), which ties
   the journal to one serialiser's key order and number formatting.
   Cloudflare compares canonical JSON with recursively sorted keys
   (`runtime.ts` `stableStringify`). Program scopes digest a canonical form
   (sorted keys, no insignificant whitespace, fixed number formatting) so a
   journal written under QuickJS replays under workerd (D9). The stored
   argument blob itself stays as sent.
6. **Semantics version.** `ProgramEngine.semantics_version` pins the prelude and
   guest API. An executor refuses to resume an unsupported version.

Executor conformance tests: run a program delivering its completions in
random orders and groupings, then replay the recorded order in different
groupings. Every run must make identical calls and produce identical output.
Every executor must pass the same suite.

### D9. Executor: a stepped program instance

Revised 2026-10-15. The executor is a value the workflow steps, not a
process that streams messages:

| Call | Takes | Returns |
|---|---|---|
| `start` | module source, contract bindings (the shape of `env`), input, recorded `started_at`, random seed, program context, limits, step budget | `Waiting { requests }` or `Finished { requests, report }` |
| `step` | completions in delivery order: request id, outcome (a value, or `{ kind, message, value }`), recorded timestamp (feeds the clock, D8) | the same |

- **No thread, channel or clock.** Each call runs the program to quiescence,
  delivering one completion per quiescent point (D8). Requests issued in a
  step that also finishes the program are still returned to the host.
- **Budget.** QuickJS interrupt checks per step (default 2,000), counted at
  deterministic points. Exhausting it fails the program; it is terminal and
  never a replayed branch. Memory and stack limits apply as before. There
  is no wall-clock budget inside the executor. The per-step counts also
  add up to the run's `max_compute` (D7 "Long runs").
- **Divergence.** A completion for a request the program never issued is a
  typed `Nondeterminism` failure; a duplicate is an internal error. An
  oversized result reaches the program as a `payload_too_large` failure.
- **Direction of authority.** The workflow and its activities hold Temporal
  clients and keys; the executor holds none and sees only JSON.
- **Executors:**
  - **In-process QuickJS** (`crates/program-engine`). The only executor
    today; it runs on every platform. Code mode keeps its own engine in
    `crates/codemode`, which programs no longer touch.
  - **WebAssembly component** (later option). The Temporal Rust SDK has an
    experimental `wasm-workflows` feature: workflow code as a wasmtime
    component behind the unstable WIT package
    `temporal:workflow-runtime@0.1.0`. QuickJS compiled to a component
    could add memory isolation and fuel budgets without leaving the worker.
  - **workerd** (D12, later). The same step calls over IPC to a sidecar,
    one isolate per cached run; losing the sidecar fails the workflow task,
    which retries and replays. Linux only, and a less common pattern.
  - **Remote SDK** (optional, later): whether non-JS programs get a
    first-class path is an open question.

### D10. Repair through a separate agent session

- **Failure report.** A failed run records a `report_ref` containing:
  - the error and stack
  - the span path
  - the last N calls
  - the program's steps and admissions
  - nondeterminism, limit, schema or contract details

  Add `RunFailureKind::Program`. A failed child is a failed call outcome in the
  parent: the parent can catch it or fail in turn. The report points at the
  failing leaf session. Every terminal outcome, including divergence and limit
  failures, is returned as a value to the caller and never raised across a
  bridge, the rule Cloudflare's runtime applies so that an agent loop is never
  broken by a sandbox exception (`docs/codemode/approvals.md`).
- **Inspect grant.** A repair agent is an ordinary agent session with a `programs`
  feature grant scoped to sessions or program names. It gets these trace tools:
  - `program_trace(session, run, span|range)`
  - `program_call(session, run, request_id)`
  - `program_source(session | program@rev)` (the `@rev` form needs the
    revision history from D2)
  - `program_dry_replay(source, session, run)`
- **Dry replay.** Run an executor with a journal-only host. Completions come from
  the recorded outcomes. The first call with no matching record stops execution
  and is reported. There are no effects. Instrumentation is free, because notes
  don't shift `call-N`.
- **Publish.** `program_publish(name, source)` creates a new program profile
  revision, gated by policy: auto-publish or human approval.
- **Resume.** Create a new program session from the new revision with
  `resume_from: { session, run }`. Its host matches calls against the old journal
  by `(binding, args digest, occurrence count)`.
  - A match is admitted with `reuse: Some(prior_origin)`, copying result refs
    without re-executing or re-applying effects.
  - Matched child-session calls reuse the child's result.
  - Earlier effects the new run never reaches are reported as **orphaned effects**.

  This is a research item.

### D11. Observability

**Guest API:**

```js
await ctx.say("Found 132 citations; checking each");      // narration
await ctx.step("citation 14", async () => { ... });       // span
await env.web.fetch(args, { why: "¶3 cites this DOI" });  // per-call annotation
```

`ctx` plays the role of workerd's `ExecutionContext`: the handler's handle on
the host for things that are not tool calls. `ctx.step` is a span only. It
does not record its return value, unlike Cloudflare's `codemode.step`, which
records a closure's result as a log entry so nondeterministic local work
replays (`proxy-tool.ts` `STEP_PRELUDE`). With host-owned globals (D8) there is
no nondeterministic local work to capture, and a recorded step would shift
`call-N`, which D10's free instrumentation depends on.

**Harness:**
- Add a new `CoreAgentEvent::Program(ProgramEvent)` family with these variants:
  - `Narration { execution_id, seq, span_path, text_ref }`
  - `SpanOpened` / `SpanClosed { execution_id, seq, name, parent }`
- Narration and spans use their own sequence (`note-N`), flushed as `Notes`
  batches. Admission dedups by `(execution_id, seq)`.
- The scoped-call spec gains an optional `annotation`, **excluded from the dedup
  digest**, so a changed reason or line number isn't divergence.
- Run-owned scopes stamp `run_id` in joins, which supplies the run join that
  code-tool events lack today.
- Register new kinds in `core/codec.rs:105-211`. Use `serde(default)` for new state
  fields and add fixtures in `crates/harness/fixtures/`.

**Projection, API and web:**
- For run-owned scopes, project full call views: canonical tool name, args
  preview, output from `output_ref`, annotation, duration and attachments. This
  replaces the payload-free `CodeToolProgress` (`api-projection/src/lib.rs:675-709`)
  for those scopes. Code mode's projection is unchanged.
- Child-session calls link to the child session id, so the UI can drill down
  through the tree.
- Add `RunView.program: Option<ProgramRunView { spans, calls, narration }>`.
- Web: add the new kinds in `transcript.ts` (the `codeToolProgress` no-op is at :285)
  and in `tool-trace.tsx`, with spans as collapsible groups and narration as
  assistant text.

### D12. workerd runner in smolvm

Revised 2026-10-15: with the program running in its workflow (D7), workerd
would be a sidecar the workflow steps synchronously over IPC (D9), one
isolate per cached run, rather than a runner that streams the old D9
messages. A WebAssembly component is the in-process alternative. The
notes below still apply to the sidecar.

- **Reuse** the provider controller plane:
  - target lifecycle and power (`environment-protocol/src/control/*`)
  - the `ProviderBindingContext` per universe
  - the `ProviderController` connector (`temporal-runtime/src/environments/provider_controllers.rs`)
  - the gateway relay pattern.
- **New:**
  - a `program/*` data plane carrying the D9 messages
  - a "system runner" machine class that sessions can never select as an
    environment
  - a vsock/AF_UNIX relay: the Incus relay dials ws over a private IPv4
    (`environment-provider-incus/src/relay.rs:22`).
- **workerd:**
  - one isolate per execution, ideally through the Worker Loader binding (check
    its status)
  - `globalOutbound` set to none, and no `--net` on the VM
  - D8 delivery and the global overrides (`Date`, timers, `Math.random`,
    `crypto` randomness) in a wrapper module that runs before program source.
    Standalone workerd cannot host a custom V8 platform, so the clock override
    lives in the wrapper rather than in `CurrentClockTimeMillis` as it does
    inside workerd itself.

### D13. Composition: subprograms and sub-agents

- **New workflow tools** `program.run` (joined) and `program.spawn` (promises) sit
  beside `agent.run`/`agent.spawn`. Arguments are
  `{ program: name | inline, input, env? }`; the result is the child's validated
  output. The latest revision is resolved at admission and the resolved
  revision and `source_ref` are recorded in the call arguments, so parent
  replay is stable without reading past revisions. Explicit `name@rev` pinning
  arrives with revision history in Phase 7 (D2).
- **Capability passing.** The Worker Loader lets a parent decide a child's
  `env` (`worker-loader.h` `WorkerCode.env`), so attenuation is structural: a
  child can only hold what the parent put there. `env` in `program.run` and
  `agent.run` is the same idea at the session boundary: a list of the parent's
  own binding handles the child may use, recorded in the call arguments as the
  canonical contract ids they resolve to. The child's effective grants are its
  profile's grants intersected with that list; an omitted `env` means the full
  profile grant. Because the handles come from the parent's scope, a parent
  cannot name a capability it does not hold, so the common case of the subset
  check below is enforced by construction. It also lets a program hand a
  sub-agent a narrower toolset than the agent's profile allows, which is how a
  "verify this one citation" child gets `web.fetch` and nothing else.
- **Generalise `SubagentExecutionWorkflow` into a child-session execution
  workflow** (`workflows/subagent_execution.rs:40-171`,
  `temporal-runtime/src/subagents.rs:206-430`). The `subagent_prepare` activity
  creates the child and starts its run with `notify_on_terminal`; the workflow
  waits for `RunTerminal`, closes the child, then resolves the parent promise,
  and handles cancel and deadline. A program child differs only in profile kind
  and run source. One delegation tree means one set of depth, descendant and
  concurrency limits.
- **Grants.** A child's grants must be a subset of its parent's. This check does
  not exist today: `delegated_session_authority`
  (`gateway/service/authorization.rs:178-195`) only requires that the child's
  access row has a parent, and the only narrowing is numeric
  (`SubagentLimits::attenuated_by`, called at `subagents.rs:280-286`, defined
  in `harness/src/core/components/config.rs:492-499`). A child runs its
  profile's own config, applied inline at preparation
  (`subagents.rs:365-396`), and that profile may legitimately hold grants the
  parent lacks. Add a feature, tool and environment subset check at child
  preparation and fail the call with a typed error when the named profile asks
  for more than the parent holds. Apply it to program children from the start.
  Applying it to sub-agents is a **breaking change** for existing delegation
  configurations, so it ships separately behind its own decision and a
  documented migration, not as a side effect of this plan.
- **Fan-out budgets.** Two existing limits cap a program's fan-out well below
  the motivating case. `MAX_WORKFLOW_TOOL_EMISSIONS_PER_RUN` is 32
  (`workflow_tool.rs:16`), and code-tool admission already counts pending
  workflow calls against it (`code_tool.rs:445-471`). The delegation defaults
  are depth 2, 16 descendants and 4 concurrent children
  (`api/src/sessions.rs:592-602`). A citation pass over 100 sources needs more
  than 100 child calls in one run. For run-owned scopes the emission cap comes
  from `ProgramLimits`, bounded by the `FeaturesConfig.program` ceiling, rather
  than the constant, and program profiles set their own delegation limits
  within the parent's attenuated grant. Whether program children get a
  separate counter in the delegation tree stays an open question.
- **Typed boundaries.** Input and output schemas make subprograms typed functions.
  Agent children called from programs can be given an output schema too. A
  child's failure is a failed call outcome carrying the child's report ref,
  returned as a value; the parent's `env` wrapper throws locally (D4), so a
  parent can catch it, retry with a different child, or fail in turn.
- **Both directions.** Agent sessions can call `program.run`.

### D14. Waiting, human input and bots

A program that needs a person's decision should read as straight-line code,
not as a state machine spread over several runs:

```js
const findings = await env.agent.run({ agent: "pr-reviewer", input: { pr } });
const review   = await env.input.request({ ask: "Merge?", findings, assignees: ["alice"] });
if (review.approved) await env.bots.emit({ to: "merge-bot", data: { pr } });
return { outcome: "handled", summary: review.approved ? "queued for merge" : "rejected" };
```

**A wait is a suspended run, not a new run.** A run is one invocation: one
input, one terminal, one output. While the program waits, its run stays
`Active`, its workflow waits on a signal and holds no worker (D7), and only the
history and a pending promise remain. When the answer arrives the outcome is
delivered at the `await`; if the worker was lost meanwhile, the program is
first replayed from the workflow's history, and recorded calls, including the
sub-agent, are never made again. This needs D8's determinism contract and pins the
run to the program revision it started with. A finished run is never resumed:
a follow-up months later (a new comment on the issue) is a new run (below).

**Human input is a promise-returning tool, not an incoming event.** There are
two ways an answer can reach a session, and only one of them works while a run
waits:

| Arrives as | Path | Blocked by the waiting run? |
|---|---|---|
| A bot event (webhook, chat message) | bot controller → the session's run queue | Yes. It queues behind the run it should resume. |
| A promise resolution (a tool's reply) | delivered to that promise by id | No. This is how a run makes progress. |

So human input is an `input` contract family backed by a workflow tool with
a reply promise, like `agent_run`/`agent_spawn`. Its workflow presents the
request, waits durably for the answer, and resolves the reply. The first
backend is the **Lightspeed web inbox**: pending requests per universe, with
approve, reject or a schema-checked JSON answer. Chat, email and GitHub review
requests are later backends behind the same contract.

- **`env.input.open(spec)`** creates the request and returns a **handle**
  `{ id }` at once. The handle's operations are calls like any other:
  - `answer()`: the pending answer, a promise that may outlive many workers;
  - `remind()`: notify the assignees again through the backend;
  - `reassign({ to })`: replace the assignees without touching the run;
  - `cancel()`: withdraw the request; `answer()` then fails as cancelled,
    unless the answer was saved first, in which case it returns the answer
    (D7 "Long runs" item 3: a withdrawal settles only when the input
    workflow confirms it). `await cancel()` resolves once the request has
    settled, with `{ status: "cancelled" | "resolved" | "failed" }`;
  - `detach()`: keep the request open past the run's exit; without it,
    every exit withdraws it (D7 "Long runs" item 3).
- **`env.input.request(spec)`** is shorthand for `open` and an immediate
  `answer()`.
- **Spec:** `ask`, a payload to show, an optional response schema,
  assignees, an optional `deadline`, an optional `remind_after` interval and
  an optional escalation list.
- **Deadline.** Every request has one: there is no unbounded wait. The
  program profile's limits set the default (7 days unless set), the universe's
  `FeaturesConfig.program` caps it (at least 30 days recommended, so a
  two-week absence fits), and a call may ask for anything up to the cap.
- **Expiry.** If an escalation list is configured (the call's own, else the
  trigger's, else the bot's), the request is reassigned to it once with a
  fresh deadline and the program does not notice. Otherwise, or on the second
  expiry, `answer()` fails with a typed `DeadlineExceeded` the program can
  catch.
- **Built-in reminders.** With `remind_after`, the input workflow reminds the
  assignees on that interval until the answer or the deadline. This is the
  default for programs that want no custom logic.

**Program-side reminder policy.** Custom cadences, escalation to a PR's owner
and similar policies are written in the program itself, from a durable sleep
raced against the handle:

```js
const ask = await env.input.open({ ask: "Merge?", findings, assignees: ["alice"], deadline: "14d" });
let review;
for (let nudges = 0; ; nudges++) {
  const next = await Promise.race([ask.answer(), env.time.sleep({ ms: 3 * DAY }).then(() => null)]);
  if (next) { review = next; break; }
  await (nudges === 2 ? ask.reassign({ to: ["team-lead"] }) : ask.remind());
}
```

The race replays identically because outcomes are delivered in log order
(D8), and each wake is one step of the program in its workflow (D7). The
common patterns ship as a small JavaScript helper library with the program
runtime, for example `withReminders(ask, { every: "3d", max: 2, then:
"reassign" })`, built only on the primitives above. No separate service is
needed.

**Approval parking** (D3 `ApprovalPolicy::Park`) is the same mechanism: the
approval decision is a session-owned promise and a parked call is a slow
call. A parked approval happens in tool admission, not in program code, so
the program has no hook for a reminder loop: approvals use the built-in
`remind_after`, deadline and escalation settings, configured on the program
profile (overridable per bot). Both land together.

**Bots and waiting runs.** Today a bot lane waits up to
`BOT_EVENT_TERMINAL_TIMEOUT` (24 hours) for a run terminal and then records
`run_failed`, even if the run is still legitimately waiting. Changes:

- A program run that suspends on an `input.request` (or a parked approval)
  emits a non-terminal **waiting** notification to `notify_on_terminal`
  holders. A new run-notification body, so a workflow-contract change.
- The bot records the delivery `deferred` (its existing meaning: waiting on
  something), releases the lane, and stops the terminal timeout for that run.
  The eventual terminal updates the delivery's outcome and sends any receipt.
- The session stays busy while its run waits: one active run per session.
  With `perKey` and `perEvent` routing that blocks only that key; with the
  main session it blocks the whole bot. Bots whose profile has a program and
  whose triggers route to the main session should warn when a program can
  wait. Later events for the waiting key follow a per-trigger **while-waiting**
  policy: `queue` (default) or `supersede` (cancel the waiting run and start
  a fresh one, for example a new push to a PR under review). Start with
  these two; a filter that supersedes only on some events (a new commit,
  not a comment) waits for a real need.
- Delivering a bot event *into* a waiting run (the program analogue of
  steering) is not needed when answers come through `input.request`. Keep it
  out of scope until a use case needs it.

**A program run's verdict comes from its output.** An agent resolves a
delivery through `bot_event_resolve`, because the controller cannot read
free text. A program's output is already structured: if it has
`{ outcome, summary }` with a model-decision outcome (`handled`, `deferred`,
`ignored`, `blocked`), that is the delivery's outcome; a completed run without
one is `handled`. A program may still call `bot_event_resolve`; an explicit
resolve wins.

**Follow-up runs and key-scoped state.** A program session keeps nothing
between runs: a second run gets only its own input. A follow-up months later
therefore rebuilds its view from the outside world (the issue thread, labels)
or from state the program stored. A program session's equivalent of an
agent's context is a **program context**: a JSON document the program
keeps between runs. A run reads the newest as `ctx.state` (read when the
run starts and recorded with its inputs, so replay sees the same value)
and hands over the next with `return ctx.complete(result, state)` (D7
"Long runs", 2026-10-16; this replaced `ctx.context` / `ctx.setContext`);
the runtime appends it when the run completes, so a failed run, or one
that returns without `complete`, leaves the previous context in place. Each run's context is its own ordinary context entry (a
`ProgramState` kind, JSON content in the content store, tagged with the
run), so the active context holds a history of recent runs' states and a
later fork can start from the end of any retained run. Retention is the
program's compaction: after each run completes, older `ProgramState`
entries beyond `ProgramLimits.context_retained` (default 10) leave the
active context; the session log keeps them all. Nothing is summarized,
since each entry is already complete. This mirrors agent compaction (a
per-session policy with a default, applied automatically), measured in
entries and bytes because a program has no model context window. One
entry is capped at 64 KiB. It shows wherever context does (session view,
events). A routed key moves to a new session (its next generation) when
its program changes; the new session opens with the newest program
context seeded, as profile instructions are published at open. Not the
VFS: a program that wants a readable document writes it to a workspace
instead, and data that grows across runs goes to files or CAS with a
reference in the state.

## API surface

- **Session creation:** `SessionStartParams` / `ManagedSessionStartParams`
  (`crates/api/src/sessions.rs:28`, `:63`) accept a program profile. The session
  view exposes `driver: agent | program { programId, revision, semanticsVersion }`.
- **Profiles:** `ProfileDocument.program: Option<ProgramSpec { sourceRef, semanticsVersion, requires, inputSchema?, outputSchema?, limits? }>`
  (`crates/api/src/profiles.rs:179`).
- **Runs:** `RunStartSource::Program { input }` (`crates/api/src/runs.rs:76`).
  Mirror it in `RunViewSource` (`views.rs:224`) and the run-summary projection
  (`api-projection/src/lib.rs:209-290`).
- **Tool contracts:** a read method listing the available canonical contracts and
  versions, for authors and program-writing models.
- **Grants:** `FeaturesConfig.program` (`sessions.rs:387`) sets ceilings and
  supported semantics versions for program sessions. `FeaturesConfig.programs`
  grants calling, inspecting and publishing programs.
- **Regenerate:** `cargo run -p api --bin export-schema`, the SDK and
  configurator-mcp generators, the config and method-role scripts, and
  `cargo run -p temporal-workflow --bin export-workflow-contract`.
- **Bots** need no new public API for program runs (done in Phase 2,
  `d235d146`). A bot whose profile has a program creates its sessions from the
  named profile; `start_run` sends `Program { input }` with
  `{ events: [{ id, seq, event: <stored envelope>, media }] }`; steer and append
  deliveries queue on program sessions; a profile whose program differs from a
  session's pin rotates that session. D14 adds the waiting notification, the
  per-trigger while-waiting policy and output verdicts.

## Phases

Each phase lands green on its own (clippy with `-D warnings`, scoped tests,
regenerated contracts).

1. **Extract the scoped-call core** (D3). This is a pure refactor.
   - Changes: owner and policy with code-mode defaults, including the
     `Blocking | Ack` completion policy; the `scope_owner` helper; legacy
     decode; `session/scoped_calls.rs` with the existing Update names. Nothing
     that code mode does not exercise: `CallContext` and optional joins move to
     Phase 2.
   - Gate: every existing code-tool unit, replay and live test passes unchanged.
     Add codec fixtures for legacy and new scope events.
2. **Program session driver** (D1, D2, D5, D6).
   - Changes: `SessionDriver`; program profiles and pinning;
     `RunRequestSource::Program`; planner gating; `CallContext` and optional
     joins; run-owned scopes with program policy (`Ack` completion) and the
     `scope_recorded_outcomes` query; the run-owned workflow-tool invocation
     and its reply handling; per-kind start policy; cancel and failure drain;
     `ProgramRunWorkflow` with `AllowDuplicateFailedOnly` running one attempt
     against the in-process QuickJS executor, polling for outcomes.
   - Runs in this phase are **not durable**: a worker loss fails the run until
     Phase 4 adds replay.
   - Tests: harness replay tests for a program run with calls, including a joined
     child and an `await`, plus fork rejection. Live Temporal suite, serialised.
   - **Follow-up: routed program sessions kept a stale pin** (fixed in `d14c1e7d`).
     See "Known issue: routed program sessions keep a stale pin" below.
3. **Canonical contracts and human input** (D4, D14).
   - Changes: digest-pinned contracts for built-ins, including `time.sleep`;
     MCP schema pinning; `requires` admission at creation and at every attempt;
     the `env`/`ctx` guest surface with the host-owned globals from D8.
   - D14: the `input` contract (`open` with handle operations `answer`,
     `remind`, `reassign`, `cancel`; `request` shorthand) with the web inbox
     backend; deadlines, escalation lists and built-in `remind_after`; approval
     `Park` with the same settings; the `withReminders` helper library; the
     waiting run notification and its bot handling (`deferred`, no terminal
     timeout while waiting, `queue`/`supersede`); verdicts from program output;
     the program context (built as `ctx.context` / `ctx.setContext`, being
     replaced by `ctx.state` / `ctx.complete` in Phase 4's follow-ups; one context
     entry per run written at completion, the last `context_retained`
     (default 10) kept active, carried across a routed key's generations).
   - Until Phase 4 a wait held a live attempt, bounded by its wall clock;
     Phase 4 removed that limit.
4. **Durable execution** (D7, D8, D9). Done, redirected on 2026-10-15.
   - Built: quiescent delivery; the program engine in its own crate
     (`crates/program-engine`) with a stepped instance and deterministic step
     budget; the program running inside `ProgramRunWorkflow` with load,
     admit, collect and conclude activities and the `ScopeProgress` wake.
   - Tests: conformance (shuffled delivery, then replay in several
     groupings); a live test that kills the program worker while a program
     waits on a person, answers, and finishes on a replacement from
     history; a live timer test that replays the workflow history.
   - Built after: a separate `program` worker role and task queue;
     admission as a local activity (11 → 6 history events per sequential
     call, with a live test for a lost admission result and one that
     measures events per call).
   - Built after: program values out of history (reference payloads and a
     decode-only codec; `program_conclude` as a local activity).
   - Remaining, in order (D7 "Long runs"): exits that settle every call
     (withdrawals confirmed by the input workflow, wait-cancellation for
     tool activities), per-call `cancel()` and `detach()`; `ctx.state` with
     `ctx.continueAsNew` and `ctx.complete`, a scope per execution, and
     run-wide limits (`max_calls` per run, `max_compute`);
     `ctx.shouldContinue()` and `history_exhausted`; Temporal worker
     versioning.
5. **Composition** (D13).
   - Changes: the child-session workflow; `program.run`/`program.spawn`; schemas;
     program-owned fan-out budgets; `env` capability passing; the grant subset
     check for program children.
   - Tests: a live test of bot → program → {program, agent}.
6. **Observability** (D11).
   - Changes: notes and spans; annotations; program events; projections; web.
7. **Repair** (D10). First failure reports, inspect grants, program revision
   history in the profile store, trace tools and dry replay; then publish, and
   resume with journal matching (research).
8. **Stronger isolation** (D9, D12). A WebAssembly component on the SDK's
   `wasm-workflows` path, or workerd as a sidecar behind the step
   interface. Either must pass the Phase 4 conformance suite. QuickJS stays
   the in-process executor everywhere.

Phases 1–6 cover the issue-pipeline bots. Phase 7 delivers "hand it to an AI to
fix".

### Known issue: routed program sessions keep a stale pin

Found 2026-10-14, after `d235d146`. Fixed in `d14c1e7d` as described below,
with both tests.

- **Symptom.** A bot on a program profile with `perKey` (or `perEvent`)
  routing keeps running the program revision each routed session was created
  with, for as long as that session stays open. The default
  `routedSessionCloseAfterMs` is never, so a PR or issue thread opened months
  ago still runs the old program while new keys get the current one. The main
  session is not affected: it rotates on a profile revision change.
- **Cause.** `ensure_routed_session`
  (`temporal-workflow/src/workflows/bots/controller/lanes.rs`) returns early
  for a session the controller already tracks (`observe_routed_session`), so
  `ensure_session` never runs `apply_profile` for it, and the pin check in
  `check_profile_driver` (`gateway/service/profiles.rs`) never sees the new
  revision. The comment there states the intent: "Routed sessions take the
  profile at creation". That is right for agent bots, whose thread
  conversation should survive a profile edit. A program session has no
  conversation to keep, so a stale pin only does harm.
- **Fix.**
  - Record the profile revision each routed session was ensured under
    (`ManagedSession.profile_revision`, `serde(default)`, carried across
    continue-as-new).
  - When the bot's sessions are program sessions (`program_sessions`) and a
    known routed session's revision differs from the controller's applied
    revision, run the ordinary ensure for it. `apply_profile` refuses the
    differing pin, the result is `ProfileUnapplicable`, and the existing
    rotation moves the key to its next generation (`…-g2`) with the new
    program. The key, and so routing and coalescing, is unchanged.
  - Only an idle session rotates. A run that is active or waiting (D14) keeps
    its pinned program until it ends; the rotation happens on the next
    delivery after that. Agent bots keep today's behaviour.
- **Tests.** A controller unit test (a stale revision on a known program
  session triggers an ensure; on an agent session it does not), and a
  `bots_live` test: a `perKey` program bot handles an event for key `a`, the
  profile gets a new program revision, the next event for `a` runs the new
  program in `…-g2`.

## Open questions

- **Non-JS executors.** Should the remote SDK (D9) be first-class from the start,
  for example for Python pipelines? That decides whether the protocol is public
  API or internal.
- **Optional joins** (`CallContext`, D3): is that acceptable to the maintainer, or
  should scoped calls get a separate request type?
- **Tool versioning** (D4, flagged by the user as needing real thought):
  digest pinning only detects a changed contract. Before the first
  incompatible change to a built-in: how are versions named, how long do old
  ones stay callable while pinned programs and suspended runs (D14, possibly
  weeks long) still use them, who owns that promise, and how does a pinned
  program move to a new version (republish, or an adapter)? MCP tools change
  upstream without notice, so their side of this is a policy (fail, or adapt)
  rather than a support window.
- **Long runs** (D7): the design is agreed (reference payloads, built;
  `ctx.state` with `continueAsNew` and `complete`; exits that settle every
  call; per-call `cancel()` and `detach()`; `ctx.shouldContinue`,
  `history_exhausted`). Settled 2026-10-16: open `input.open` handles
  cross an exit only when detached, with their ids in the state; the
  names `ctx.state`, `ctx.continued`, `continueAsNew`, `complete`; the
  limits (`max_calls` per run, default 50,000; new `max_compute`;
  `max_wall_clock_ms` removed); a withdrawal settles only when the input
  workflow confirms it. Still open: `max_compute`'s default,
  measured when building.
- **Isolation** (D9, D12): untrusted program code now runs in a workflow
  worker. Is a separate `program` worker role enough for now, and which
  stronger option comes first: a WebAssembly component or workerd?
- **Profiles** (D2): should a program profile extend `ProfileDocument` or be a
  sibling document type in the same store? The need for revision history leans
  toward a sibling, pending the maintainer's view.
- **Tool naming** (D13): should `program.run` and `agent.run` merge into
  `session.run`, with the child kind taken from the profile?
- **Budgets** (D13): what are the per-program call, emission and wall-time
  ceilings, and do program children need separate counters in the delegation
  tree?
- **Sub-agent grant subsetting** (D13): when, and with what migration, does the
  subset check extend to agent children?
- **Env handles** (D13): is a child's `env` recorded as canonical contract ids,
  or as the parent's binding handles resolved again at child admission?
- **Input backends** (D14): after the web inbox, which come first: chat
  channels, email, or a GitHub review request? Does a backend answer by
  calling a public API method, or by its own workflow-tool emission?
- **Program context** (D14): may operators replace or clear a session's
  program context through the API (to repair a stuck key), or is it
  written only by runs?
- **Inspect grants** (D10): scoped to the subtree, to bot-owned sessions, or to
  explicit session ids?
