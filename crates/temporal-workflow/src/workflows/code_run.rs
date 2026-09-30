//! One script execution, supervised: started on call by the calling
//! session's start-on-call `code_run` invocation. It starts the code
//! runtime's execution as a child and is that execution's host: each tool
//! call the script makes arrives here as a signal, runs as an activity
//! against the tools pinned at admission, and goes back to the runtime as a
//! signal. When the runtime returns, the result resolves the caller's
//! `reply` promise. The holder's cancellation or Temporal cancellation
//! cancels the runtime's execution. The session workflow carries no code.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{FutureExt, future::LocalBoxFuture, pin_mut, select_biased};
use temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy;
use temporalio_common::{SignalDefinition, WorkflowDefinition};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, ChildWorkflowOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};

use crate::{
    AgentSessionWorkflow, CodeHostCallCompleted, CodeHostCallRequest,
    CodeRunExecutionOutcome, CodeRunExecutionPhase, CodeRunExecutionSnapshot,
    CodeRunFinishActivityRequest, CodeRunPrepareActivityRequest, CodeRunPrepared,
    CodeToolCallActivityRequest, WorkflowActivities, WorkflowToolRecoveryResult,
    WorkflowToolStartArgs,
};

/// The code runtime's execution workflow, which lives in another worker.
/// Its input and result are the runtime's own JSON contract.
pub struct CodeExecution;

impl WorkflowDefinition for CodeExecution {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    fn name(&self) -> &str {
        tools_contract::CODE_EXECUTION_WORKFLOW_TYPE
    }
}

/// The runtime's reply signal for one call.
pub struct CallCompleted;

impl SignalDefinition for CallCompleted {
    type Workflow = CodeExecution;
    type Input = CodeHostCallCompleted;

    fn name(&self) -> &str {
        tools_contract::CALL_COMPLETED_SIGNAL
    }
}

/// Names shared with the code runtime's contract.
mod tools_contract {
    pub const CODE_EXECUTION_WORKFLOW_TYPE: &str = "CodeExecutionWorkflow";
    pub const CALL_COMPLETED_SIGNAL: &str = "call_completed";
}

/// Grace past the script's own deadline before Temporal ends the runtime's
/// execution, so the runtime reports its deadline itself.
const RUNTIME_TIMEOUT_GRACE: Duration = Duration::from_secs(5 * 60);

#[workflow(name = "CodeRunWorkflow")]
#[derive(Default)]
pub struct CodeRunWorkflow {
    snapshot: CodeRunExecutionSnapshot,
    holder_workflow_id: Option<String>,
    invocation_id: Option<engine::WorkflowToolInvocationId>,
    holder_cancelled: bool,
    /// Calls the runtime asked for, not yet picked up by the run loop.
    requested: VecDeque<CodeHostCallRequest>,
}

#[workflow_methods]
impl CodeRunWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        start: WorkflowToolStartArgs,
    ) -> WorkflowResult<()> {
        if ctx.workflow_id() != start.execution_id
            || start.universe_id != start.invocation.session_universe_id
        {
            return Err(anyhow::anyhow!(
                "code run identity is invalid: workflow id or universe mismatch"
            )
            .into());
        }
        let Some(reply_promise_id) = start
            .invocation
            .completion_promises
            .as_ref()
            .and_then(|promises| promises.get(engine::REPLY_COMPLETION_KEY))
            .cloned()
        else {
            return Err(anyhow::anyhow!(
                "code run invocation is missing its reply completion promise"
            )
            .into());
        };
        let Some(context_ref) = start.invocation.execution_context_ref.clone() else {
            return Err(
                anyhow::anyhow!("code run invocation is missing its execution context").into(),
            );
        };
        ctx.state_mut(|state| {
            state.holder_workflow_id = Some(start.holder_workflow_id.clone());
            state.invocation_id = Some(start.invocation.invocation_id.clone());
        });

        let prepared = ctx
            .start_activity(
                WorkflowActivities::code_run_prepare,
                CodeRunPrepareActivityRequest {
                    start: start.clone(),
                },
                crate::activity_options(),
            )
            .await;
        let (execution_input, task_queue, deadline_ms) = match prepared {
            Ok(CodeRunPrepared::Ready {
                execution_input,
                task_queue,
                deadline_ms,
            }) => (execution_input, task_queue, deadline_ms),
            Ok(CodeRunPrepared::Rejected { resolution }) => {
                resolve(ctx, &start, reply_promise_id, resolution).await;
                return Ok(());
            }
            Err(_) => {
                let resolution = engine::PromiseResolution::Failed { error_ref: None };
                resolve(ctx, &start, reply_promise_id, resolution).await;
                return Ok(());
            }
        };

        let runtime_id = format!("{}/runtime", start.execution_id);
        ctx.state_mut(|state| {
            state.snapshot.phase = CodeRunExecutionPhase::Running;
            state.snapshot.runtime_execution_id = Some(runtime_id.clone());
        });
        let child = ctx
            .child_workflow(
                CodeExecution,
                execution_input,
                ChildWorkflowOptions {
                    workflow_id: runtime_id.clone(),
                    task_queue: Some(task_queue),
                    parent_close_policy: ParentClosePolicy::RequestCancel,
                    execution_timeout: Some(
                        Duration::from_millis(deadline_ms) + RUNTIME_TIMEOUT_GRACE,
                    ),
                    ..Default::default()
                },
            )
            .await;
        let started = match child {
            Ok(started) => started,
            Err(error) => {
                let outcome = CodeRunExecutionOutcome::Lost {
                    message: format!("the code runtime did not start the script: {error}"),
                };
                finish(ctx, &start, reply_promise_id, outcome).await;
                return Ok(());
            }
        };

        let result = started.result();
        pin_mut!(result);
        let mut in_flight: Vec<LocalBoxFuture<'static, (String, serde_json::Value)>> = Vec::new();
        let mut in_flight_ids = BTreeSet::new();
        let mut completed: BTreeMap<String, serde_json::Value> = BTreeMap::new();

        let ending = loop {
            let requested = ctx.state_mut(|state| std::mem::take(&mut state.requested));
            for call in requested {
                if let Some(outcome) = completed.get(&call.call_id) {
                    // A repeated request gets the recorded outcome again.
                    reply(ctx, &runtime_id, &call.call_id, outcome.clone()).await;
                } else if in_flight_ids.insert(call.call_id.clone()) {
                    in_flight.push(execute_call(ctx, &start, &context_ref, call));
                }
            }

            let step = {
                let woken = ctx
                    .wait_condition(|state| !state.requested.is_empty() || state.holder_cancelled)
                    .fuse();
                let cancelled = ctx.cancelled().fuse();
                pin_mut!(woken, cancelled);
                select_biased! {
                    returned = result.as_mut() => Step::Returned(returned),
                    done = next_finished(&mut in_flight).fuse() => Step::CallDone(done),
                    _ = cancelled => Step::Cancelled,
                    _ = woken => Step::Woken,
                }
            };
            match step {
                Step::Returned(returned) => break Ending::Returned(returned),
                Step::CallDone((call_id, outcome)) => {
                    in_flight_ids.remove(&call_id);
                    completed.insert(call_id.clone(), outcome.clone());
                    ctx.state_mut(|state| state.snapshot.calls += 1);
                    reply(ctx, &runtime_id, &call_id, outcome).await;
                }
                Step::Cancelled => break Ending::Cancelled,
                Step::Woken => {
                    if ctx.state(|state| state.holder_cancelled) {
                        break Ending::HolderCancelled;
                    }
                }
            }
        };

        match ending {
            Ending::Returned(returned) => {
                let outcome = match returned {
                    Ok(result) => CodeRunExecutionOutcome::Returned { result },
                    Err(error) => CodeRunExecutionOutcome::Lost {
                        message: format!("the code runtime's execution failed: {error}"),
                    },
                };
                finish(ctx, &start, reply_promise_id, outcome).await;
                Ok(())
            }
            Ending::HolderCancelled | Ending::Cancelled => {
                // The runtime cancels its own in-flight calls; this host's
                // calls end with the workflow.
                result.as_ref().get_ref().cancel();
                let _ = result.await;
                ctx.state_mut(|state| state.snapshot.phase = CodeRunExecutionPhase::Cancelled);
                if matches!(ending, Ending::Cancelled) {
                    Err(temporalio_sdk::WorkflowTermination::Cancelled)
                } else {
                    // The holder already recorded the promise as cancelled.
                    Ok(())
                }
            }
        }
    }

    /// A call the code runtime asks this host to execute.
    #[signal(name = "call_requested")]
    pub fn call_requested(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        call: CodeHostCallRequest,
    ) {
        let expected = self.snapshot.runtime_execution_id.as_deref();
        if expected.is_some() && expected == Some(call.execution_id.as_str()) {
            self.requested.push_back(call);
        }
    }

    /// The holder's `invocation_cancellation` for this call's `reply` key.
    #[signal(name = "deliver_emission")]
    pub fn deliver_emission(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        envelope: engine::EmissionEnvelope,
    ) {
        if super::model_generate::is_holder_cancellation(
            self.holder_workflow_id.as_deref(),
            self.invocation_id.as_ref(),
            &envelope,
        ) {
            self.holder_cancelled = true;
        }
    }

    #[query(name = "snapshot")]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> CodeRunExecutionSnapshot {
        self.snapshot.clone()
    }

    #[query(name = "workflow_tool_recovery")]
    pub fn workflow_tool_recovery(&self, _ctx: &WorkflowContextView) -> WorkflowToolRecoveryResult {
        let mut resolutions = BTreeMap::new();
        if let Some(resolution) = &self.snapshot.resolution {
            resolutions.insert(engine::REPLY_COMPLETION_KEY.to_owned(), resolution.clone());
        }
        WorkflowToolRecoveryResult { resolutions }
    }
}

enum Step<R> {
    Returned(R),
    CallDone((String, serde_json::Value)),
    Cancelled,
    Woken,
}

enum Ending<R> {
    Returned(R),
    HolderCancelled,
    Cancelled,
}

/// The first in-flight call to finish. Each call is polled with the
/// workflow's own waker: combinators that install their own wakers, such as
/// `FuturesUnordered`, are rejected by the SDK as nondeterministic. With no
/// calls in flight this never completes.
fn next_finished<'a>(
    in_flight: &'a mut Vec<LocalBoxFuture<'static, (String, serde_json::Value)>>,
) -> impl Future<Output = (String, serde_json::Value)> + 'a {
    futures::future::poll_fn(move |cx: &mut Context<'_>| {
        for index in 0..in_flight.len() {
            if let Poll::Ready(done) = Pin::new(&mut in_flight[index]).poll(cx) {
                drop(in_flight.remove(index));
                return Poll::Ready(done);
            }
        }
        Poll::Pending
    })
}

fn execute_call(
    ctx: &WorkflowContext<CodeRunWorkflow>,
    start: &WorkflowToolStartArgs,
    context_ref: &engine::BlobRef,
    call: CodeHostCallRequest,
) -> LocalBoxFuture<'static, (String, serde_json::Value)> {
    let ctx = ctx.clone();
    let request = CodeToolCallActivityRequest {
        universe_id: start.universe_id,
        execution_context_ref: context_ref.clone(),
        call,
    };
    async move {
        let call_id = request.call.call_id.clone();
        let outcome = ctx
            .start_activity(
                WorkflowActivities::code_tool_call,
                request,
                crate::tool_call_activity_options(engine::ToolExecutionSpec::new(
                    engine::ToolExecutionClass::RemoteInteractive,
                    true,
                )),
            )
            .await
            .unwrap_or_else(|error| {
                serde_json::json!({
                    "ok": false,
                    "error": {
                        "code": "target_failed",
                        "message": format!("the tool call did not complete: {error}"),
                    },
                })
            });
        (call_id, outcome)
    }
    .boxed_local()
}

async fn reply(
    ctx: &WorkflowContext<CodeRunWorkflow>,
    runtime_id: &str,
    call_id: &str,
    outcome: serde_json::Value,
) {
    // The runtime may already have finished; a lost reply changes nothing.
    let _ = ctx
        .external_workflow(runtime_id.to_owned(), None)
        .signal(
            CallCompleted,
            CodeHostCallCompleted {
                call_id: call_id.to_owned(),
                outcome,
            },
        )
        .await;
}

async fn finish(
    ctx: &mut WorkflowContext<CodeRunWorkflow>,
    start: &WorkflowToolStartArgs,
    reply_promise_id: engine::PromiseId,
    outcome: CodeRunExecutionOutcome,
) {
    let resolution = ctx
        .start_activity(
            WorkflowActivities::code_run_finish,
            CodeRunFinishActivityRequest {
                universe_id: start.universe_id,
                outcome,
            },
            crate::activity_options(),
        )
        .await
        .unwrap_or(engine::PromiseResolution::Failed { error_ref: None });
    resolve(ctx, start, reply_promise_id, resolution).await;
}

async fn resolve(
    ctx: &mut WorkflowContext<CodeRunWorkflow>,
    start: &WorkflowToolStartArgs,
    reply_promise_id: engine::PromiseId,
    resolution: engine::PromiseResolution,
) {
    ctx.state_mut(|state| {
        state.snapshot.phase = CodeRunExecutionPhase::Resolved;
        state.snapshot.resolution = Some(resolution.clone());
    });
    let envelope = engine::EmissionEnvelope::source_resolution(
        start.universe_id,
        ctx.workflow_id().to_owned(),
        &start.holder_workflow_id,
        reply_promise_id,
        resolution,
    );
    let _ = ctx
        .external_workflow(start.holder_workflow_id.clone(), None)
        .signal(AgentSessionWorkflow::deliver_emission, envelope)
        .await;
}
