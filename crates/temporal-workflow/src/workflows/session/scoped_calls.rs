//! Session-owned admission and independent dispatch of scoped calls: tool
//! calls made as the session by code running outside it. Update handlers and
//! the dispatcher only queue work. This module's main-loop drain is the sole
//! writer of scoped-call facts, through the ordinary event append.
//!
//! The Update and query names stay `*_code_tool_*` for the code executions
//! that hold them; every scope owner and policy shares these handlers.

use super::*;
use crate::workflows::WorkflowContextExt as _;
use crate::{CodeToolRejection, CodeToolRejectionKind as Rejection};
use harness::{CodeToolCallStatus, CodeToolOrigin, ToolCallStatus, ToolParallelism};
use std::{pin::Pin, task::Poll};
use temporalio_sdk::{ActivityCancellationType, ActivityExecutionError, CancellableFuture};

type Key = (String, String);
type AdmissionResult = Result<(), CodeToolRejection>;
const MAX_UPDATE_WAITERS: usize = 256;

#[derive(Default)]
pub(super) struct CodeToolExecutionState {
    next_ticket: u64,
    waiters: usize,
    pending: Vec<(u64, Request)>,
    receipts: BTreeMap<u64, AdmissionResult>,
    completions: Vec<(CodeToolOrigin, crate::CodeToolInvokeActivityResult)>,
    // Kept until the result is appended, so a ready future cannot redispatch.
    inflight: BTreeMap<Key, bool>,
}

enum Request {
    Open(crate::OpenCodeToolScopeRequest),
    Invoke(crate::InvokeCodeToolRequest),
    Close(crate::CloseCodeToolScopeRequest),
    Cancel(crate::CancelCodeToolRequest),
}

fn rejected(kind: Rejection, message: impl Into<String>) -> CodeToolRejection {
    CodeToolRejection::new(kind, message)
}

fn key(origin: &CodeToolOrigin) -> Key {
    (origin.execution_id.clone(), origin.request_id.clone())
}

fn enqueue(state: &mut AgentSessionWorkflow, request: Request) -> Result<u64, CodeToolRejection> {
    if !state.ready || state.core_state.lifecycle.status != CoreAgentStatus::Open {
        return Err(rejected(
            Rejection::SessionNotReady,
            "session is not accepting code tool requests",
        ));
    }
    if state.code_tools.waiters >= MAX_UPDATE_WAITERS {
        return Err(rejected(
            Rejection::LimitExceeded,
            "too many code tool Update waiters",
        ));
    }
    state.code_tools.next_ticket += 1;
    let ticket = state.code_tools.next_ticket;
    state.code_tools.waiters += 1;
    state.code_tools.pending.push((ticket, request));
    Ok(ticket)
}

async fn await_admission(
    ctx: &WorkflowContext<AgentSessionWorkflow>,
    ticket: u64,
) -> AdmissionResult {
    ctx.wait_for_state(move |state| state.code_tools.receipts.contains_key(&ticket))
        .await;
    ctx.state_mut(|state| {
        state
            .code_tools
            .receipts
            .remove(&ticket)
            .expect("ready receipt")
    })
}

pub(super) async fn open(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
    request: crate::OpenCodeToolScopeRequest,
) -> crate::CodeToolScopeResult {
    let execution_id = request.execution_id.clone();
    let ticket = ctx.state_mut(|state| enqueue(state, Request::Open(request)))?;
    let admitted = await_admission(ctx, ticket).await;
    ctx.state_mut(|state| state.code_tools.waiters -= 1);
    admitted?;
    ctx.state(|state| report(state, &execution_id))
}

pub(super) async fn invoke(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
    request: crate::InvokeCodeToolRequest,
) -> crate::CodeToolInvocationResult {
    let origin = CodeToolOrigin {
        execution_id: request.execution_id.clone(),
        request_id: request.request_id.clone(),
    };
    let ticket = ctx.state_mut(|state| enqueue(state, Request::Invoke(request)))?;
    let admitted = await_admission(ctx, ticket).await;
    let result = if let Err(error) = admitted {
        Err(error)
    } else {
        // The scope's completion policy decides what the reply carries: the
        // terminal outcome, or only the admission with the outcome left to
        // the scope report.
        let completion = ctx.state(|state| {
            state
                .core_state
                .code_tools
                .scopes
                .get(&origin.execution_id)
                .map(|scope| scope.spec.policy.completion)
        });
        if completion == Some(harness::Completion::Blocking) {
            let wanted = origin.clone();
            ctx.wait_for_state(move |state| {
                harness::code_tool_call(&state.core_state, &wanted)
                    .is_some_and(|call| call.status.is_terminal())
            })
            .await;
        }
        ctx.state(|state| {
            harness::code_tool_call(&state.core_state, &origin)
                .map(crate::CodeToolCallOutcome::from)
                .ok_or_else(|| rejected(Rejection::UnknownScope, "code tool result unavailable"))
        })
    };
    ctx.state_mut(|state| state.code_tools.waiters -= 1);
    result
}

pub(super) async fn close(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
    request: crate::CloseCodeToolScopeRequest,
) -> crate::CodeToolScopeResult {
    let execution_id = request.execution_id.clone();
    let ticket = ctx.state_mut(|state| enqueue(state, Request::Close(request)))?;
    let admitted = await_admission(ctx, ticket).await;
    ctx.state_mut(|state| state.code_tools.waiters -= 1);
    admitted?;
    ctx.state(|state| report(state, &execution_id))
}

pub(super) async fn cancel(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
    request: crate::CancelCodeToolRequest,
) -> crate::CodeToolCancelResult {
    let ticket = ctx.state_mut(|state| enqueue(state, Request::Cancel(request.clone())))?;
    let admitted = await_admission(ctx, ticket).await;
    ctx.state_mut(|state| state.code_tools.waiters -= 1);
    admitted?;
    ctx.state(|state| cancel_status(state, &request))
}

/// Where a cancel's target stands now that the cancel is recorded, read from
/// the session's own state, so the reply never predates the record.
fn cancel_status(
    state: &AgentSessionWorkflow,
    request: &crate::CancelCodeToolRequest,
) -> crate::CodeToolCancelResult {
    use crate::CodeToolCancelStatus as Status;
    let status = match &request.target {
        harness::CodeToolCancelTarget::Call { request_id } => {
            let origin = CodeToolOrigin {
                execution_id: request.execution_id.clone(),
                request_id: request_id.clone(),
            };
            let call = harness::code_tool_call(&state.core_state, &origin)
                .ok_or_else(|| rejected(Rejection::UnknownScope, "unknown code tool request"))?;
            match &call.status {
                CodeToolCallStatus::Completed { result } => match result.status {
                    ToolCallStatus::Succeeded => Status::Resolved,
                    ToolCallStatus::Cancelled => Status::Cancelled,
                    _ => Status::Failed,
                },
                CodeToolCallStatus::Pending | CodeToolCallStatus::Waiting { .. } => {
                    Status::Requested
                }
            }
        }
        harness::CodeToolCancelTarget::Promise { promise_id } => {
            let promise = state
                .core_state
                .promises
                .promises
                .get(promise_id)
                .ok_or_else(|| rejected(Rejection::InvalidRequest, "unknown promise"))?;
            match promise.status {
                harness::PromiseStatus::Pending => Status::Requested,
                harness::PromiseStatus::Resolved => Status::Resolved,
                harness::PromiseStatus::Failed => Status::Failed,
                harness::PromiseStatus::Cancelled => Status::Cancelled,
            }
        }
    };
    Ok(crate::CodeToolCancelOutcome { status })
}

pub(super) fn recorded_outcomes(
    state: &AgentSessionWorkflow,
    request: &crate::ScopeRecordedOutcomesRequest,
) -> crate::ScopeRecordedOutcomesResult {
    let scope = state
        .core_state
        .code_tools
        .scopes
        .get(&request.execution_id)
        .ok_or_else(|| rejected(Rejection::UnknownScope, "unknown code tool execution scope"))?;
    if request.limit == 0 || request.limit > crate::MAX_RECORDED_OUTCOMES_PER_PAGE {
        return Err(rejected(
            Rejection::LimitExceeded,
            "recorded outcome page size is outside its bounded range",
        ));
    }
    let after = usize::try_from(request.after).unwrap_or(usize::MAX);
    let outcomes = scope
        .recorded_outcomes(after, request.limit as usize)
        .into_iter()
        .map(crate::CodeToolCallOutcome::from)
        .collect::<Vec<_>>();
    Ok(crate::ScopeRecordedOutcomes {
        execution_id: request.execution_id.clone(),
        closed: scope.closed,
        cancel_requested: scope.cancel_requested,
        next: request.after.saturating_add(outcomes.len() as u64),
        outcomes,
        pending: scope
            .calls
            .values()
            .filter(|call| !call.status.is_terminal())
            .map(|call| call.spec.origin.request_id.clone())
            .collect(),
    })
}

pub(super) fn report(
    state: &AgentSessionWorkflow,
    execution_id: &str,
) -> crate::CodeToolScopeResult {
    state
        .core_state
        .code_tools
        .scopes
        .get(execution_id)
        .map(|scope| crate::CodeToolScopeReport::of(&state.core_state, scope))
        .ok_or_else(|| rejected(Rejection::UnknownScope, "unknown code tool execution scope"))
}

/// All durable mutations run on the session loop, including completions that
/// raced a scope close. Infrastructure append failures still fail the workflow.
pub(super) async fn process_pending(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
) -> anyhow::Result<()> {
    let completions = ctx.state_mut(|state| std::mem::take(&mut state.code_tools.completions));
    for (origin, outcome) in completions {
        if ctx.state(|state| {
            harness::code_tool_call(&state.core_state, &origin)
                .is_some_and(|call| call.status.is_terminal())
        }) {
            // Forced run termination records an unavailable outcome. A late
            // activity reply cannot resurrect the run or reapply its effects.
            ctx.state_mut(|state| {
                state.code_tools.inflight.remove(&key(&origin));
            });
            continue;
        }
        let command = match outcome {
            crate::CodeToolInvokeActivityResult::Completed { result } => {
                CoreAgentCommand::CompleteCodeToolCall {
                    origin: origin.clone(),
                    result,
                }
            }
            crate::CodeToolInvokeActivityResult::Deferred { spec } => {
                CoreAgentCommand::DeferCodeToolCall {
                    origin: origin.clone(),
                    spec,
                }
            }
            crate::CodeToolInvokeActivityResult::EnvironmentNotReady { .. } => {
                unreachable!("dispatcher handles readiness")
            }
        };
        if let Err(error) = apply(ctx, command).await? {
            // Invalid wait targets or runtime result facts fail this call, not
            // the parent workflow. Admission did not append partial effects.
            let error_ref = tool_batches::put_boundary_error_blob(ctx, &error.message).await;
            apply_internal(
                ctx,
                CoreAgentCommand::CompleteCodeToolCall {
                    origin: origin.clone(),
                    result: tool_batches::boundary_call_result(
                        harness::code_tool_call_id(&origin),
                        ToolCallStatus::Failed,
                        error_ref,
                    ),
                },
            )
            .await?;
        }
        ctx.state_mut(|state| {
            state.code_tools.inflight.remove(&key(&origin));
        });
    }
    let pending = ctx.state_mut(|state| std::mem::take(&mut state.code_tools.pending));
    for (ticket, request) in pending {
        let result = match request {
            Request::Open(request) => admit_open(ctx, request).await?,
            Request::Invoke(request) => {
                let command = ctx.state(|state| invocation_command(state, request));
                match command {
                    Ok(command) => apply(ctx, command).await?,
                    Err(error) => Err(error),
                }
            }
            Request::Cancel(request) => {
                apply(
                    ctx,
                    CoreAgentCommand::CancelCodeTool {
                        execution_id: request.execution_id,
                        target: request.target,
                    },
                )
                .await?
            }
            Request::Close(request) => {
                apply(
                    ctx,
                    CoreAgentCommand::CloseCodeToolScope {
                        execution_id: request.execution_id,
                        cancel: request.cancel_pending,
                        program_context: request.program_context,
                    },
                )
                .await?
            }
        };
        ctx.state_mut(|state| {
            state.code_tools.receipts.insert(ticket, result);
        });
    }
    let expired = ctx.state(|state| {
        state
            .core_state
            .code_tools
            .scopes
            .values()
            .filter(|scope| {
                !scope.cancel_requested
                    && !harness::code_tool_scope_is_live(&state.core_state, &scope.spec)
            })
            .map(|scope| scope.spec.execution_id.clone())
            .collect::<Vec<_>>()
    });
    for execution_id in expired {
        apply_internal(
            ctx,
            CoreAgentCommand::CloseCodeToolScope {
                execution_id,
                cancel: true,
                program_context: None,
            },
        )
        .await?;
    }
    reconcile_waits(ctx).await
}

async fn apply(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
    command: CoreAgentCommand,
) -> anyhow::Result<AdmissionResult> {
    let mut drive = drive_from_state(ctx)?;
    match admit_and_append_command(ctx, &mut drive, command, None).await? {
        CommandAdmissionResult::Accepted => Ok(Ok(())),
        CommandAdmissionResult::Rejected(failure) => {
            Ok(Err(rejected(Rejection::InvalidRequest, failure.message)))
        }
    }
}

async fn apply_internal(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
    command: CoreAgentCommand,
) -> anyhow::Result<()> {
    apply(ctx, command)
        .await?
        .map_err(|error| anyhow::anyhow!("code tool completion rejected: {}", error.message))
}

fn invocation_command(
    state: &AgentSessionWorkflow,
    request: crate::InvokeCodeToolRequest,
) -> Result<CoreAgentCommand, CodeToolRejection> {
    let scope = state
        .core_state
        .code_tools
        .scopes
        .get(&request.execution_id)
        .ok_or_else(|| rejected(Rejection::UnknownScope, "unknown code tool scope"))?;
    if let Some(existing) = scope.calls.get(&request.request_id) {
        if existing.spec.binding_id != request.binding_id
            || existing.spec.arguments_ref != request.arguments_ref
        {
            return Err(rejected(
                Rejection::Conflict,
                "request identity already names different arguments or binding",
            ));
        }
    } else {
        if scope.closed {
            return Err(rejected(
                Rejection::ScopeClosed,
                "code tool execution scope is closed",
            ));
        }
        let limits = scope.spec.policy.limits;
        if scope.calls.len() >= limits.max_calls as usize
            || scope
                .calls
                .values()
                .filter(|call| !call.status.is_terminal())
                .count()
                >= limits.max_in_flight as usize
        {
            return Err(rejected(
                Rejection::LimitExceeded,
                "code tool execution call budget exhausted",
            ));
        }
    }
    let binding = scope
        .spec
        .bindings
        .get(&request.binding_id)
        .ok_or_else(|| {
            rejected(
                Rejection::PermissionDenied,
                "binding is not granted to this scope",
            )
        })?;
    Ok(CoreAgentCommand::AdmitCodeToolCall {
        call: harness::CodeToolCallSpec {
            origin: CodeToolOrigin {
                execution_id: request.execution_id,
                request_id: request.request_id,
            },
            binding_id: request.binding_id,
            tool_id: binding.tool_id.clone(),
            tool_name: binding.tool_name.clone(),
            arguments_ref: request.arguments_ref,
        },
    })
}

async fn admit_open(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
    request: crate::OpenCodeToolScopeRequest,
) -> anyhow::Result<AdmissionResult> {
    if let Some(existing) = ctx.state(|state| {
        state
            .core_state
            .code_tools
            .scopes
            .get(&request.execution_id)
            .cloned()
    }) {
        let ids = existing
            .spec
            .bindings
            .values()
            .map(|binding| binding.tool_id.clone())
            .collect::<BTreeSet<_>>();
        let requested = match scope_owner_request(&request) {
            Ok(owner) => owner,
            Err(error) => return Ok(Err(error)),
        };
        return Ok(
            if existing.spec.owner == requested
                && existing.spec.policy == scope_policy_request(&request, &requested)
                && request
                    .allowed_tools
                    .as_ref()
                    .is_none_or(|allowed| &ids == allowed)
            {
                Ok(())
            } else {
                Err(rejected(
                    Rejection::Conflict,
                    "execution identity already has a different scope",
                ))
            },
        );
    }
    if request
        .allowed_tools
        .as_ref()
        .is_some_and(|allowed| allowed.len() > 4096)
        || request.max_calls == 0
        || request.max_calls
            > if request.run_id.is_some() {
                harness::MAX_PROGRAM_CALLS
            } else {
                harness::MAX_CODE_TOOL_CALLS_PER_SCOPE
            }
        || request.max_in_flight == 0
        || request.max_in_flight > harness::MAX_CODE_TOOL_IN_FLIGHT
        || request.max_in_flight > request.max_calls
    {
        return Ok(Err(rejected(
            Rejection::LimitExceeded,
            "code tool scope exceeds limits",
        )));
    }
    let owner = match scope_owner_request(&request) {
        Ok(owner) => owner,
        Err(error) => return Ok(Err(error)),
    };
    let prepared = ctx.state(|state| {
        let run = state.core_state.runs.active.as_ref().ok_or_else(|| {
            rejected(
                Rejection::Unavailable,
                "code tool scope requires an active run",
            )
        })?;
        let model = run
            .run_config
            .model_override
            .clone()
            .or_else(|| {
                state
                    .core_state
                    .lifecycle
                    .config
                    .as_ref()
                    .map(|config| config.model.clone())
            })
            .ok_or_else(|| rejected(Rejection::SessionNotReady, "session has no model"))?;
        let owner_tool = match &owner {
            harness::ScopeOwner::ModelInvocation {
                parent_invocation_id,
            } => state
                .core_state
                .workflow_tools
                .start_requests
                .get(parent_invocation_id)
                .or_else(|| {
                    state
                        .core_state
                        .workflow_tools
                        .emissions
                        .get(parent_invocation_id)
                })
                .and_then(|parent| {
                    state
                        .core_state
                        .workflow_tools
                        .bindings
                        .get(&parent.tool_id)
                })
                .map(|parent| parent.definition.tool.name.clone())
                .ok_or_else(|| {
                    rejected(
                        Rejection::PermissionDenied,
                        "parent workflow tool is not admitted",
                    )
                })?,
            harness::ScopeOwner::Run { run_id } => {
                let program = state.core_state.lifecycle.driver.program().ok_or_else(|| {
                    rejected(
                        Rejection::PermissionDenied,
                        "run-owned scopes require a program session",
                    )
                })?;
                if run.run_id != *run_id
                    || harness::program_run_execution_id(&state.core_state, *run_id).as_deref()
                        != Some(harness::program_scope_execution(&request.execution_id).0)
                {
                    return Err(rejected(
                        Rejection::Unavailable,
                        "scope execution is not the run's pending program execution",
                    ));
                }
                if request.max_calls > program.limits.max_calls
                    || request.max_in_flight > program.limits.max_in_flight
                {
                    return Err(rejected(
                        Rejection::LimitExceeded,
                        "scope limits exceed the program's limits",
                    ));
                }
                harness::ToolName::new(harness::PROGRAM_EXECUTE_TOOL_NAME)
            }
        };
        let tools = select_code_tools(
            &state.core_state.tooling.tools,
            &owner_tool,
            request.allowed_tools.as_ref(),
        )?;
        Ok::<_, CodeToolRejection>((
            crate::CodeToolPrepareScopeActivityRequest {
                tools,
                model,
                binding_mode: scope_policy_request(&request, &owner).binding_mode,
            },
            state.core_state.tooling.revision,
        ))
    });
    let (prepared, revision) = match prepared {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let bindings = match ctx
        .execute_activity(
            WorkflowActivities::code_tool_prepare_scope,
            prepared,
            crate::activity_options(),
        )
        .await
    {
        Ok(result) => result.bindings,
        Err(error) => {
            return Ok(Err(rejected(
                Rejection::Unavailable,
                format!("prepare code tool bindings: {error}"),
            )));
        }
    };
    if ctx.state(|state| state.core_state.tooling.revision) != revision {
        return Ok(Err(rejected(
            Rejection::Conflict,
            "tool registry changed while preparing scope",
        )));
    }
    if request.allowed_tools.as_ref().is_some_and(|allowed| {
        bindings
            .values()
            .map(|binding| binding.tool_id.clone())
            .collect::<BTreeSet<_>>()
            != *allowed
    }) {
        return Ok(Err(rejected(
            Rejection::PermissionDenied,
            "requested tools lack callable bindings",
        )));
    }
    let scope = harness::CodeToolScopeSpec {
        execution_id: request.execution_id.clone(),
        policy: scope_policy_request(&request, &owner),
        owner,
        bindings,
    };
    apply(ctx, CoreAgentCommand::OpenCodeToolScope { scope }).await
}

/// A code execution's scope is owned by its parked parent call; a program
/// execution's scope by its run. Exactly one owner must be named.
fn scope_owner_request(
    request: &crate::OpenCodeToolScopeRequest,
) -> Result<harness::ScopeOwner, CodeToolRejection> {
    match (&request.parent_invocation_id, request.run_id) {
        (Some(parent_invocation_id), None) => Ok(harness::ScopeOwner::ModelInvocation {
            parent_invocation_id: parent_invocation_id.clone(),
        }),
        (None, Some(run_id)) => Ok(harness::ScopeOwner::Run { run_id }),
        _ => Err(rejected(
            Rejection::InvalidRequest,
            "scope must name exactly one owner: a parent invocation or a run",
        )),
    }
}

/// Code executions run under the code-mode policy and program executions
/// under the program policy, each with the limits the host requested.
fn scope_policy_request(
    request: &crate::OpenCodeToolScopeRequest,
    owner: &harness::ScopeOwner,
) -> harness::ScopePolicy {
    let limits = harness::ScopeLimits {
        max_calls: request.max_calls,
        max_in_flight: request.max_in_flight,
    };
    match owner {
        harness::ScopeOwner::ModelInvocation { .. } => harness::ScopePolicy::code_mode(limits),
        harness::ScopeOwner::Run { .. } => harness::ScopePolicy::program(limits),
    }
}

fn select_code_tools(
    tools: &BTreeMap<harness::ToolName, harness::ToolSpec>,
    parent: &harness::ToolName,
    allowed: Option<&BTreeSet<harness::ToolName>>,
) -> Result<Vec<harness::ToolSpec>, CodeToolRejection> {
    match allowed {
        Some(allowed) => allowed
            .iter()
            .map(|id| {
                tools
                    .get(id)
                    .filter(|tool| &tool.name != parent)
                    .cloned()
                    .ok_or_else(|| {
                        rejected(
                            Rejection::PermissionDenied,
                            "tool is not granted for code tool execution",
                        )
                    })
            })
            .collect(),
        None => Ok(tools
            .values()
            .filter(|tool| &tool.name != parent && tool.invokes_client_effect())
            .cloned()
            .collect()),
    }
}

async fn reconcile_waits(ctx: &mut WorkflowContext<AgentSessionWorkflow>) -> anyhow::Result<()> {
    let now = workflow_time_ms(ctx);
    let ready = ctx.state(|state| {
        state
            .core_state
            .code_tools
            .scopes
            .values()
            .flat_map(|scope| {
                scope.calls.values().filter_map(|call| {
                    if let CodeToolCallStatus::Waiting { suspension } = &call.status {
                        let sleep = call.spec.tool_id.as_str() == harness::TIME_SLEEP_TOOL_ID;
                        harness::code_tool_wake(&state.core_state, &call.spec.origin, now)
                            .map(|wake| (call.spec.origin.clone(), suspension.clone(), wake, sleep))
                    } else {
                        None
                    }
                })
            })
            .collect::<Vec<_>>()
    });
    for (origin, suspension, wake, sleep) in ready {
        // A cancelled or expired wait on a reply whose producer confirms
        // cancellations asks the producer first; the call resumes once the
        // reply settles, with whatever the producer recorded.
        if let harness::ToolBatchSuspension::JoinedWorkflowCalls { calls, .. } = &suspension
            && wake != harness::WakeReason::Terminal
        {
            let (requests, confirmed) = ctx.state(|state| {
                let events = calls
                    .iter()
                    .filter_map(|joined| {
                        harness::promise_cancel_event(&state.core_state, &joined.promise_id)
                            .map(|event| (joined.promise_id.clone(), event))
                    })
                    .collect::<Vec<_>>();
                let confirmed = events.iter().any(|(_, event)| {
                    matches!(event, harness::PromiseEvent::CancelRequested { .. })
                });
                (
                    events.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
                    confirmed,
                )
            });
            if confirmed {
                for promise_id in requests {
                    apply_internal(ctx, CoreAgentCommand::RequestPromiseCancel { promise_id })
                        .await?;
                }
                continue;
            }
        }
        let results =
            ctx.state(|state| awaits::promise_snapshot(suspension.spec(), &state.core_state));
        let result = match &suspension {
            // A sleep cut short by its scope's cancellation is cancelled.
            harness::ToolBatchSuspension::AwaitTool { call_id, .. }
                if sleep && wake == harness::WakeReason::Cancelled =>
            {
                harness::ToolInvocationResult {
                    call_id: call_id.clone(),
                    status: ToolCallStatus::Cancelled,
                    output_ref: None,
                    error_ref: None,
                    effects: vec![],
                    model_visible_context_entries: vec![],
                    attachments: vec![],
                    duration_ms: None,
                    output_bytes: None,
                    truncated: false,
                }
            }
            harness::ToolBatchSuspension::AwaitTool { call_id, spec } => {
                let materialized = ctx
                    .execute_activity(
                        WorkflowActivities::materialize_await_result,
                        crate::AwaitMaterializationRequest {
                            outcome: match wake {
                                harness::WakeReason::Cancelled => crate::AwaitOutcome::Cancelled,
                                harness::WakeReason::Timeout => crate::AwaitOutcome::Timeout,
                                harness::WakeReason::Terminal => crate::AwaitOutcome::Terminal,
                            },
                            results,
                            sleep_until_ms: spec.deadline_at_ms.filter(|_| sleep),
                        },
                        crate::activity_options(),
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                harness::ToolInvocationResult {
                    call_id: call_id.clone(),
                    status: ToolCallStatus::Succeeded,
                    output_ref: Some(materialized.result_ref),
                    error_ref: None,
                    effects: vec![],
                    model_visible_context_entries: vec![],
                    attachments: materialized.attachments,
                    duration_ms: None,
                    output_bytes: None,
                    truncated: false,
                }
            }
            harness::ToolBatchSuspension::JoinedWorkflowCalls { .. } => {
                let supplements = ctx
                    .execute_activity(
                        WorkflowActivities::prepare_joined_context,
                        crate::JoinedContextPreparationRequest { results },
                        crate::activity_options(),
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                let mut result = ctx.state(|state| {
                    harness::code_tool_joined_result(
                        &state.core_state,
                        &origin,
                        wake != harness::WakeReason::Terminal,
                    )
                })?;
                result.attachments = supplements
                    .into_iter()
                    .flat_map(|item| item.attachments)
                    .collect();
                result
            }
        };
        apply_internal(
            ctx,
            CoreAgentCommand::ResumeCodeToolCall {
                origin,
                result,
                claim_observed_at_ms: now,
            },
        )
        .await?;
    }
    Ok(())
}

pub(super) fn has_immediate_work(state: &AgentSessionWorkflow) -> bool {
    !state.code_tools.pending.is_empty()
        || !state.code_tools.completions.is_empty()
        || state.core_state.code_tools.scopes.values().any(|scope| {
            (!scope.cancel_requested
                && !harness::code_tool_scope_is_live(&state.core_state, &scope.spec))
                || scope.calls.values().any(|call| {
                    matches!(call.status, CodeToolCallStatus::Waiting { .. })
                        && harness::code_tool_wake(&state.core_state, &call.spec.origin, 0)
                            .is_some()
                })
        })
}

pub(super) fn nearest_wake_ms(state: &AgentSessionWorkflow) -> Option<u64> {
    state
        .core_state
        .code_tools
        .scopes
        .values()
        .flat_map(|scope| scope.calls.values())
        .filter(|call| !harness::code_tool_awaits_cancel_confirmation(&state.core_state, call))
        .filter_map(|call| match &call.status {
            CodeToolCallStatus::Waiting { suspension } => suspension.spec().deadline_at_ms,
            _ => None,
        })
        .min()
}

pub(super) fn is_quiescent(state: &AgentSessionWorkflow) -> bool {
    state.code_tools.waiters == 0
        && state.code_tools.pending.is_empty()
        && state.code_tools.completions.is_empty()
        && state.code_tools.inflight.is_empty()
}

pub(super) fn blocks_parent_resume(state: &AgentSessionWorkflow) -> bool {
    state
        .core_state
        .code_tools
        .scopes
        .values()
        .any(|scope| scope.calls.values().any(|call| !call.status.is_terminal()))
}

fn next_dispatch(state: &AgentSessionWorkflow) -> Option<(CodeToolOrigin, bool)> {
    if state.code_tools.inflight.len() >= crate::MAX_CONCURRENT_TOOL_CALLS_PER_BATCH
        || state
            .code_tools
            .inflight
            .values()
            .any(|exclusive| *exclusive)
    {
        return None;
    }
    let call = state
        .core_state
        .code_tools
        .scopes
        .values()
        .flat_map(|scope| scope.calls.values())
        .filter(|call| {
            matches!(call.status, CodeToolCallStatus::Pending)
                && !state
                    .code_tools
                    .inflight
                    .contains_key(&key(&call.spec.origin))
        })
        .min_by_key(|call| call.promise_id_base)?;
    let exclusive = state
        .core_state
        .tooling
        .tools
        .get(&call.spec.tool_id)
        .is_none_or(|tool| tool.parallelism != ToolParallelism::ParallelSafe);
    (!exclusive || state.code_tools.inflight.is_empty())
        .then(|| (call.spec.origin.clone(), exclusive))
}

type ActivityFuture<'a, T> =
    Pin<Box<dyn CancellableFuture<Output = Result<T, ActivityExecutionError>> + 'a>>;
enum TaskFuture<'a> {
    Invoke(ActivityFuture<'a, crate::CodeToolInvokeActivityResult>),
    Readiness(ActivityFuture<'a, crate::AwaitEnvironmentReadyActivityResult>),
    PrepareControls(ActivityFuture<'a, harness::PromiseControlArgumentFacts>),
}
enum TaskOutcome {
    Invoke(Result<crate::CodeToolInvokeActivityResult, ActivityExecutionError>),
    Readiness(Result<crate::AwaitEnvironmentReadyActivityResult, ActivityExecutionError>),
    PrepareControls(Result<harness::PromiseControlArgumentFacts, ActivityExecutionError>),
}
impl TaskFuture<'_> {
    fn cancel(&self) {
        match self {
            Self::Invoke(future) => future.as_ref().get_ref().cancel(),
            Self::Readiness(future) => future.as_ref().get_ref().cancel(),
            Self::PrepareControls(future) => future.as_ref().get_ref().cancel(),
        }
    }
    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> Poll<TaskOutcome> {
        match self {
            Self::Invoke(future) => future.as_mut().poll(cx).map(TaskOutcome::Invoke),
            Self::Readiness(future) => future.as_mut().poll(cx).map(TaskOutcome::Readiness),
            Self::PrepareControls(future) => {
                future.as_mut().poll(cx).map(TaskOutcome::PrepareControls)
            }
        }
    }
}
struct Inflight<'a> {
    origin: CodeToolOrigin,
    request: harness::ToolInvocationCallRequest,
    future: TaskFuture<'a>,
    cancel_sent: bool,
    readiness_attempted: bool,
}
fn start_invoke<'a>(
    ctx: &'a WorkflowContext<AgentSessionWorkflow>,
    origin: &CodeToolOrigin,
    request: &harness::ToolInvocationCallRequest,
) -> TaskFuture<'a> {
    let mut options = crate::tool_call_activity_options(request.execution);
    // A program run's calls record what the tool really did: a cancelled
    // activity is waited for until it reports, so a call that finished
    // anyway is recorded as finished, never assumed cancelled.
    if ctx.state(|state| run_owned(state, origin)) {
        options.cancellation_type = ActivityCancellationType::WaitCancellationCompleted;
    }
    TaskFuture::Invoke(Box::pin(ctx.execute_activity(
        WorkflowActivities::code_tool_invoke,
        crate::CodeToolInvokeActivityRequest {
            request: request.clone(),
        },
        options,
    )))
}
fn run_owned(state: &AgentSessionWorkflow, origin: &CodeToolOrigin) -> bool {
    state
        .core_state
        .code_tools
        .scopes
        .get(&origin.execution_id)
        .is_some_and(|scope| matches!(scope.spec.owner, harness::ScopeOwner::Run { .. }))
}
/// A call is cancelled by its own cancel or by its scope's.
fn call_cancelled(state: &AgentSessionWorkflow, origin: &CodeToolOrigin) -> bool {
    harness::code_tool_call_cancel_requested(&state.core_state, origin)
}
fn queue_completion(
    ctx: &WorkflowContext<AgentSessionWorkflow>,
    origin: CodeToolOrigin,
    outcome: crate::CodeToolInvokeActivityResult,
) {
    ctx.state_mut(|state| state.code_tools.completions.push((origin, outcome)));
}
async fn failed_result(
    ctx: &mut WorkflowContext<AgentSessionWorkflow>,
    origin: &CodeToolOrigin,
    status: ToolCallStatus,
    message: &str,
) -> crate::CodeToolInvokeActivityResult {
    let error_ref = tool_batches::put_boundary_error_blob(ctx, message).await;
    crate::CodeToolInvokeActivityResult::Completed {
        result: tool_batches::boundary_call_result(
            harness::code_tool_call_id(origin),
            status,
            error_ref,
        ),
    }
}

/// Poll SDK futures directly. Every readiness/preparation phase remains in the
/// same bounded window, so one blocked dependency cannot stall its siblings.
pub(super) async fn run_dispatch_loop(
    mut ctx: WorkflowContext<AgentSessionWorkflow>,
) -> anyhow::Result<()> {
    let activity_ctx = ctx.clone();
    let mut inflight: Vec<Inflight<'_>> = Vec::new();
    loop {
        while let Some((origin, exclusive)) = ctx.state(next_dispatch) {
            ctx.state_mut(|state| {
                state.code_tools.inflight.insert(key(&origin), exclusive);
            });
            if ctx.state(|state| call_cancelled(state, &origin)) {
                queue_completion(
                    &ctx,
                    origin.clone(),
                    crate::CodeToolInvokeActivityResult::Completed {
                        result: canceled_result(harness::code_tool_call_id(&origin)),
                    },
                );
                continue;
            }
            let batch = ctx.state(|state| {
                harness::code_tool_request(
                    state.session_id.as_ref().expect("initialized session"),
                    &state.core_state,
                    &origin.execution_id,
                    &origin.request_id,
                )
            });
            let batch = match batch {
                Ok(request) => request,
                Err(error) => {
                    let result = failed_result(
                        &mut ctx,
                        &origin,
                        ToolCallStatus::Failed,
                        &error.to_string(),
                    )
                    .await;
                    queue_completion(&ctx, origin, result);
                    continue;
                }
            };
            let execution = ctx.state(|state| {
                let call = &batch.calls[0];
                if call.remote_mcp.is_some() {
                    harness::ToolExecutionSpec::new(
                        harness::ToolExecutionClass::RemoteInteractive,
                        false,
                    )
                } else {
                    state
                        .core_state
                        .tooling
                        .tools
                        .get(call.tool_id.as_ref().expect("admitted tool"))
                        .map(|tool| tool.execution)
                        .unwrap_or_default()
                }
            });
            let request = batch
                .call_request(0, execution)
                .expect("single code tool call");
            let future = if let Some(controls) = batch.promise_control_argument_request() {
                TaskFuture::PrepareControls(Box::pin(activity_ctx.execute_activity(
                    WorkflowActivities::tool_prepare_promise_controls,
                    crate::ToolPreparePromiseControlsActivityRequest { request: controls },
                    crate::boundary_error_blob_activity_options(),
                )))
            } else {
                start_invoke(&activity_ctx, &origin, &request)
            };
            inflight.push(Inflight {
                origin,
                request,
                future,
                cancel_sent: false,
                readiness_attempted: false,
            });
        }
        for task in &mut inflight {
            if !task.cancel_sent && ctx.state(|state| call_cancelled(state, &task.origin)) {
                task.future.cancel();
                task.cancel_sent = true;
            }
        }
        let ready = {
            let uncanceled = inflight
                .iter()
                .filter(|task| !task.cancel_sent)
                .map(|task| task.origin.clone())
                .collect::<Vec<_>>();
            let wait = ctx.wait_for_state(move |state| {
                next_dispatch(state).is_some()
                    || uncanceled
                        .iter()
                        .any(|origin| call_cancelled(state, origin))
            });
            let next = futures::future::poll_fn(|cx| {
                for (index, task) in inflight.iter_mut().enumerate() {
                    if let Poll::Ready(outcome) = task.future.poll(cx) {
                        return Poll::Ready((index, outcome));
                    }
                }
                Poll::Pending
            })
            .fuse();
            pin_mut!(next, wait);
            // A ready completion can schedule result materialization. Prefer
            // it consistently over new dispatch to preserve replay ordering.
            futures::select_biased! { ready = next => Some(ready), _ = wait => None }
        };
        let Some((index, outcome)) = ready else {
            continue;
        };
        let mut task = inflight.remove(index);
        let cancel = ctx.state(|state| call_cancelled(state, &task.origin));
        let outcome = match outcome {
            TaskOutcome::Invoke(Ok(crate::CodeToolInvokeActivityResult::EnvironmentNotReady {
                environment_id,
            })) if !cancel && !task.readiness_attempted => {
                task.future = TaskFuture::Readiness(Box::pin(activity_ctx.execute_activity(
                    WorkflowActivities::await_environment_ready,
                    crate::AwaitEnvironmentReadyActivityRequest {
                        session_id: task.request.session_id.clone(),
                        environment_id,
                        environment_policy: task.request.environment_policy.clone(),
                    },
                    crate::environment_ready_activity_options(),
                )));
                task.readiness_attempted = true;
                inflight.push(task);
                continue;
            }
            TaskOutcome::Readiness(Ok(crate::AwaitEnvironmentReadyActivityResult::Ready))
                if !cancel =>
            {
                task.future = start_invoke(&activity_ctx, &task.origin, &task.request);
                inflight.push(task);
                continue;
            }
            TaskOutcome::PrepareControls(Ok(facts)) if !cancel => {
                let prepared = ctx.state(|state| {
                    harness::attach_promise_control_runtime(
                        &state.core_state,
                        task.request.clone().into_batch_request(),
                        facts,
                    )
                });
                match prepared {
                    Ok(batch) => {
                        task.request = batch
                            .call_request(0, task.request.execution)
                            .expect("single code tool call");
                        task.future = start_invoke(&activity_ctx, &task.origin, &task.request);
                        inflight.push(task);
                        continue;
                    }
                    Err(error) => {
                        failed_result(
                            &mut ctx,
                            &task.origin,
                            ToolCallStatus::Failed,
                            &error.to_string(),
                        )
                        .await
                    }
                }
            }
            // A completed effect remains authoritative even when it raced a close.
            TaskOutcome::Invoke(Ok(
                outcome @ crate::CodeToolInvokeActivityResult::Completed { .. },
            )) => outcome,
            TaskOutcome::Invoke(Ok(
                outcome @ crate::CodeToolInvokeActivityResult::Deferred { .. },
            )) => outcome,
            _ if cancel => crate::CodeToolInvokeActivityResult::Completed {
                result: canceled_result(task.request.call.call_id.clone()),
            },
            TaskOutcome::Invoke(Err(error))
            | TaskOutcome::Readiness(Err(error))
            | TaskOutcome::PrepareControls(Err(error)) => {
                failed_result(
                    &mut ctx,
                    &task.origin,
                    tool_batches::boundary_call_status(&error),
                    &error.to_string(),
                )
                .await
            }
            TaskOutcome::Readiness(Ok(result)) => {
                failed_result(
                    &mut ctx,
                    &task.origin,
                    ToolCallStatus::Failed,
                    &format!("environment unavailable: {result:?}"),
                )
                .await
            }
            TaskOutcome::Invoke(Ok(crate::CodeToolInvokeActivityResult::EnvironmentNotReady {
                ..
            })) => {
                failed_result(
                    &mut ctx,
                    &task.origin,
                    ToolCallStatus::Failed,
                    "environment remained unavailable after readiness wait",
                )
                .await
            }
            TaskOutcome::PrepareControls(Ok(_)) => {
                unreachable!("successful preparation handled above")
            }
        };
        queue_completion(&ctx, task.origin, outcome);
    }
}

fn canceled_result(call_id: harness::ToolCallId) -> harness::ToolInvocationResult {
    harness::ToolInvocationResult {
        call_id,
        status: ToolCallStatus::Cancelled,
        output_ref: None,
        error_ref: None,
        model_visible_context_entries: vec![],
        effects: vec![],
        attachments: vec![],
        duration_ms: None,
        output_bytes: None,
        truncated: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implicit_code_tool_catalog_excludes_the_parent_and_explicit_empty_grants_nothing() {
        let state = dispatcher_state();
        let parent = harness::ToolName::new("z-first");
        let tools = &state.core_state.tooling.tools;
        let selected = select_code_tools(tools, &parent, None).unwrap();
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|tool| tool.name != parent));
        assert!(
            select_code_tools(tools, &parent, Some(&BTreeSet::new()))
                .unwrap()
                .is_empty()
        );
        for forbidden in [parent.clone(), harness::ToolName::new("not-granted")] {
            let error = select_code_tools(tools, &parent, Some(&[forbidden].into())).unwrap_err();
            assert_eq!(error.kind, Rejection::PermissionDenied);
        }
    }

    fn dispatcher_state() -> AgentSessionWorkflow {
        let mut state = AgentSessionWorkflow::default();
        let mut calls = BTreeMap::new();
        let mut bindings = BTreeMap::new();
        // Lexical order deliberately differs from admission order.
        for (request_id, base, parallelism) in [
            ("z-first", 1, ToolParallelism::ParallelSafe),
            ("a-second", 33, ToolParallelism::Exclusive),
            ("b-third", 65, ToolParallelism::ParallelSafe),
        ] {
            let tool_id = harness::ToolName::new(request_id);
            state.core_state.tooling.tools.insert(
                tool_id.clone(),
                harness::ToolSpec {
                    name: tool_id.clone(),
                    kind: harness::ToolKind::Builtin(Default::default()),
                    parallelism,
                    execution: Default::default(),
                },
            );
            bindings.insert(
                request_id.to_owned(),
                harness::CodeToolBinding {
                    tool_id: tool_id.clone(),
                    tool_name: tool_id.clone(),
                    contract: None,
                },
            );
            let origin = CodeToolOrigin {
                execution_id: "scope".into(),
                request_id: request_id.into(),
            };
            calls.insert(
                request_id.into(),
                harness::CodeToolCall {
                    call_id: harness::code_tool_call_id(&origin),
                    spec: harness::CodeToolCallSpec {
                        origin,
                        binding_id: request_id.into(),
                        tool_id: tool_id.clone(),
                        tool_name: tool_id,
                        arguments_ref: BlobRef::from_bytes(b"{}"),
                    },
                    promise_id_base: base,
                    status: CodeToolCallStatus::Pending,
                    completed_at_ms: None,
                },
            );
        }
        state.core_state.code_tools.scopes.insert(
            "scope".into(),
            harness::CodeToolScope {
                toolset_revision: 0,
                spec: harness::CodeToolScopeSpec {
                    execution_id: "scope".into(),
                    owner: harness::ScopeOwner::ModelInvocation {
                        parent_invocation_id: harness::WorkflowToolInvocationId::new(format!(
                            "wti:sha256:{}",
                            "0".repeat(64)
                        )),
                    },
                    policy: harness::ScopePolicy::code_mode(harness::ScopeLimits {
                        max_calls: 10,
                        max_in_flight: 3,
                    }),
                    bindings,
                },
                closed: false,
                cancel_requested: false,
                calls,
                completed: Vec::new(),
                program_context: None,
                cancel_requested_calls: Default::default(),
                withdrawn: Default::default(),
            },
        );
        state
    }

    #[test]
    fn dispatch_respects_admission_order_and_exclusive_barriers() {
        let mut state = dispatcher_state();
        let (first, exclusive) = next_dispatch(&state).unwrap();
        assert_eq!(first.request_id, "z-first");
        assert!(!exclusive);
        state.code_tools.inflight.insert(key(&first), false);
        assert!(
            next_dispatch(&state).is_none(),
            "exclusive call must wait; later parallel call must not jump it"
        );
        state
            .core_state
            .code_tools
            .scopes
            .get_mut("scope")
            .unwrap()
            .calls
            .get_mut("z-first")
            .unwrap()
            .status = CodeToolCallStatus::Completed {
            result: canceled_result(harness::code_tool_call_id(&first)).into(),
        };
        state.code_tools.inflight.clear();
        let (second, exclusive) = next_dispatch(&state).unwrap();
        assert_eq!(second.request_id, "a-second");
        assert!(exclusive);
        state.code_tools.inflight.insert(key(&second), true);
        assert!(next_dispatch(&state).is_none());
    }

    #[test]
    fn closed_scopes_allow_identical_delivery_but_reject_new_or_conflicting_calls() {
        let mut state = dispatcher_state();
        state
            .core_state
            .code_tools
            .scopes
            .get_mut("scope")
            .unwrap()
            .closed = true;
        let request = crate::InvokeCodeToolRequest {
            execution_id: "scope".into(),
            request_id: "z-first".into(),
            binding_id: "z-first".into(),
            arguments_ref: BlobRef::from_bytes(b"{}"),
        };
        assert!(invocation_command(&state, request.clone()).is_ok());
        assert_eq!(
            invocation_command(
                &state,
                crate::InvokeCodeToolRequest {
                    arguments_ref: BlobRef::from_bytes(b"changed"),
                    ..request.clone()
                }
            )
            .unwrap_err()
            .kind,
            Rejection::Conflict
        );
        assert_eq!(
            invocation_command(
                &state,
                crate::InvokeCodeToolRequest {
                    request_id: "new".into(),
                    ..request
                }
            )
            .unwrap_err()
            .kind,
            Rejection::ScopeClosed
        );
    }

    #[test]
    fn active_transport_and_unsettled_calls_block_rollover_and_parent_resume() {
        let mut state = dispatcher_state();
        assert!(blocks_parent_resume(&state));
        state.code_tools.waiters = 1;
        assert!(!is_quiescent(&state));
        state.code_tools.waiters = 0;
        state
            .code_tools
            .inflight
            .insert(("scope".into(), "z-first".into()), false);
        assert!(!is_quiescent(&state));
        state.code_tools.inflight.clear();
        assert!(
            is_quiescent(&state),
            "pending durable calls can be dispatched after rehydration"
        );
    }
    #[test]
    fn last_update_waiter_wakes_closed_session_completion() {
        let mut state = AgentSessionWorkflow {
            initialized: true,
            ready: true,
            ..Default::default()
        };
        state.core_state.lifecycle.status = CoreAgentStatus::Closed;
        state.code_tools.waiters = 1;
        assert!(!wait_loop::workflow_state_is_closed_and_quiescent(&state));
        assert!(!wait_loop::workflow_state_has_immediate_work(&state));

        // The handler consumes its final outcome and returns without another
        // append or timer. This state change must wake the main session loop.
        state.code_tools.waiters -= 1;
        assert!(wait_loop::workflow_state_is_closed_and_quiescent(&state));
        assert!(wait_loop::workflow_state_has_immediate_work(&state));
    }
}
