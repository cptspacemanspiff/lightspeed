//! A human-input request: one program call that waits for people. The
//! workflow opens the request in the universe's inbox, then sleeps until
//! the inbox nudges it with an answer, the deadline passes (escalating
//! once if the request names an escalation list), a reminder is due, or
//! the waiting call is withdrawn. The inbox row decides every race: an
//! answer recorded before an expiry or cancellation wins.

use std::time::{Duration, UNIX_EPOCH};

use futures::{FutureExt, select_biased};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

use crate::{
    AgentSessionWorkflow, InputRequestOpenActivityRequest, InputRequestOpenActivityResult,
    InputRequestSnapshot, InputRequestStep, InputRequestStepActivityRequest,
    InputRequestStepResult, WorkflowActivities, WorkflowToolRecoveryResult, WorkflowToolStartArgs,
    workflows::WorkflowContextExt as _,
};

#[workflow]
#[derive(Default)]
pub struct InputRequestWorkflow {
    snapshot: InputRequestSnapshot,
    start: Option<WorkflowToolStartArgs>,
    holder_cancelled: bool,
    answered: bool,
}

#[workflow_methods]
impl InputRequestWorkflow {
    #[run(name = "InputRequestWorkflow")]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        start: WorkflowToolStartArgs,
    ) -> WorkflowResult<()> {
        let reply = validate_start(ctx.workflow_id(), &start)
            .map_err(|message| temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!(message)))?;
        ctx.state_mut(|state| state.start = Some(start.clone()));
        let opened = ctx
            .execute_activity(
                WorkflowActivities::input_request_open,
                InputRequestOpenActivityRequest {
                    start: start.clone(),
                },
                crate::activity_options(),
            )
            .await
            .map_err(|error| {
                temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!(
                    "open input request: {error}"
                ))
            })?;
        let resolution = match opened {
            InputRequestOpenActivityResult::Rejected { error_ref } => {
                harness::PromiseResolution::Failed {
                    error_ref: Some(error_ref),
                }
            }
            InputRequestOpenActivityResult::Opened {
                request_id,
                deadline_at_ms,
                deadline_after_ms,
                remind_after_ms,
                escalates,
            } => {
                ctx.state_mut(|state| {
                    state.snapshot.request_id = Some(request_id);
                    state.snapshot.deadline_at_ms = Some(deadline_at_ms);
                });
                wait(
                    ctx,
                    &start,
                    Wait {
                        deadline_at_ms,
                        deadline_after_ms,
                        remind_after_ms,
                        escalates,
                    },
                )
                .await?
            }
        };
        ctx.state_mut(|state| state.snapshot.resolution = Some(resolution.clone()));
        let envelope = harness::EmissionEnvelope::source_resolution(
            start.universe_id,
            start.execution_id.clone(),
            &start.holder_workflow_id,
            reply,
            resolution,
        );
        let _ = ctx
            .external_workflow(start.holder_workflow_id, None)
            .signal(
                AgentSessionWorkflow::deliver_emission,
                envelope,
                crate::workflows::signal_options(),
            )
            .await;
        if ctx.cancellation_token().is_cancelled() {
            Err(temporalio_sdk::WorkflowTermination::cancelled())
        } else {
            Ok(())
        }
    }

    /// The inbox recorded an answer; the workflow reads it from there.
    #[signal(name = "answered")]
    pub fn answered(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.answered = true;
    }

    #[signal(name = "deliver_emission")]
    pub fn deliver_emission(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        envelope: harness::EmissionEnvelope,
    ) {
        if self
            .start
            .as_ref()
            .is_some_and(|start| crate::workflows::is_holder_reply_cancellation(start, &envelope))
        {
            self.holder_cancelled = true;
        }
    }

    #[query(name = "snapshot")]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> InputRequestSnapshot {
        self.snapshot.clone()
    }

    #[query(name = "workflow_tool_recovery")]
    pub fn workflow_tool_recovery(&self, _ctx: &WorkflowContextView) -> WorkflowToolRecoveryResult {
        WorkflowToolRecoveryResult {
            resolutions: self
                .snapshot
                .resolution
                .clone()
                .map(|resolution| [(harness::REPLY_COMPLETION_KEY.to_owned(), resolution)].into())
                .unwrap_or_default(),
        }
    }
}

struct Wait {
    deadline_at_ms: u64,
    deadline_after_ms: u64,
    remind_after_ms: Option<u64>,
    escalates: bool,
}

async fn wait(
    ctx: &WorkflowContext<InputRequestWorkflow>,
    start: &WorkflowToolStartArgs,
    mut wait: Wait,
) -> WorkflowResult<harness::PromiseResolution> {
    let mut escalated = false;
    let mut last_reminder_at_ms = now_ms(ctx);
    loop {
        let now = now_ms(ctx);
        let cancelled =
            ctx.state(|state| state.holder_cancelled) || ctx.cancellation_token().is_cancelled();
        let answered = ctx.state_mut(|state| std::mem::take(&mut state.answered));
        let step = if cancelled {
            Some(InputRequestStep::Cancel { at_ms: now })
        } else if answered {
            Some(InputRequestStep::Check)
        } else if now >= wait.deadline_at_ms {
            Some(InputRequestStep::Deadline {
                at_ms: now,
                escalate_deadline_at_ms: (wait.escalates && !escalated)
                    .then(|| now.saturating_add(wait.deadline_after_ms)),
            })
        } else if now
            >= crate::next_wake_at_ms(
                wait.deadline_at_ms,
                wait.remind_after_ms,
                last_reminder_at_ms,
            )
        {
            last_reminder_at_ms = now;
            let count = ctx.state_mut(|state| {
                state.snapshot.reminders += 1;
                state.snapshot.reminders
            });
            Some(InputRequestStep::Remind { count, at_ms: now })
        } else {
            None
        };
        if let Some(step) = step {
            match run_step(ctx, start, step).await? {
                InputRequestStepResult::Settled { resolution } => return Ok(resolution),
                InputRequestStepResult::Pending {
                    deadline_at_ms,
                    escalated: now_escalated,
                } => {
                    wait.deadline_at_ms = deadline_at_ms;
                    escalated = now_escalated;
                    ctx.state_mut(|state| {
                        state.snapshot.deadline_at_ms = Some(deadline_at_ms);
                        state.snapshot.escalated = now_escalated;
                    });
                }
            }
            // A cancellation always settles; never wait after one.
            if cancelled {
                return Err(temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!(
                    "input request stayed pending after its cancellation"
                ))
                .into());
            }
            continue;
        }
        let wake_at = crate::next_wake_at_ms(
            wait.deadline_at_ms,
            wait.remind_after_ms,
            last_reminder_at_ms,
        );
        // Every wake re-enters the loop, which decides what is due.
        let mut timer = ctx.timer_with_manual_cancellation(Duration::from_millis(
            wake_at.saturating_sub(now).max(1),
        ));
        {
            let signalled = ctx.wait_for_state(|state| state.answered || state.holder_cancelled);
            let external_cancel = ctx.cancelled().fuse();
            let elapsed = (&mut timer).fuse();
            futures::pin_mut!(signalled, external_cancel, elapsed);
            select_biased! {
                _ = external_cancel => {},
                _ = signalled => {},
                _ = elapsed => {},
            }
        }
        timer.cancel();
    }
}

async fn run_step(
    ctx: &WorkflowContext<InputRequestWorkflow>,
    start: &WorkflowToolStartArgs,
    step: InputRequestStep,
) -> WorkflowResult<InputRequestStepResult> {
    ctx.execute_activity(
        WorkflowActivities::input_request_step,
        InputRequestStepActivityRequest {
            universe_id: start.universe_id,
            execution_id: start.execution_id.clone(),
            step,
        },
        crate::activity_options(),
    )
    .await
    .map_err(|error| {
        temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!("input request step: {error}"))
            .into()
    })
}

/// Workflow time in Unix milliseconds: the SDK's deterministic clock.
fn now_ms(ctx: &WorkflowContext<InputRequestWorkflow>) -> u64 {
    ctx.workflow_time()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn validate_start(
    workflow_id: &str,
    start: &WorkflowToolStartArgs,
) -> Result<harness::PromiseId, &'static str> {
    if workflow_id != start.execution_id
        || start.universe_id != start.invocation.session_universe_id
        || start.holder_workflow_id
            != crate::compose_workflow_id(start.universe_id, &start.invocation.session_id)
    {
        return Err("input request workflow, holder, or universe identity mismatch");
    }
    if !INPUT_SEMANTIC_TYPES.contains(&start.invocation.semantic_type.as_str())
        || !matches!(start.invocation.context, harness::CallContext::Scope { .. })
    {
        return Err("input request start is not a program's scoped input call");
    }
    let promises = start
        .invocation
        .completion_promises
        .as_ref()
        .ok_or("input request requires one reply promise")?;
    if promises.len() != 1 {
        return Err("input request requires exactly one reply promise");
    }
    promises
        .get(harness::REPLY_COMPLETION_KEY)
        .cloned()
        .ok_or("input request is missing its reply promise")
}

/// Mirrors the semantic types of the `input.request` (joined) and
/// `input.open` (promise) bindings; the workflow crate cannot depend on the
/// tools crate.
const INPUT_SEMANTIC_TYPES: [&str; 2] = ["lightspeed.input.request.v1", "lightspeed.input.open.v1"];

#[cfg(test)]
mod tests {
    use super::*;
    use harness::{BlobRef, CallContext, PromiseId, RunId, SessionId, WorkflowToolInvocation};

    fn start_args(context: CallContext) -> WorkflowToolStartArgs {
        let universe_id = uuid::Uuid::from_u128(7);
        let session_id = SessionId::new("program-session");
        WorkflowToolStartArgs {
            universe_id,
            holder_workflow_id: crate::compose_workflow_id(universe_id, &session_id),
            execution_id: "wtx:sha256:input".into(),
            invocation: WorkflowToolInvocation {
                invocation_id: harness::WorkflowToolInvocationId::new(format!(
                    "wti:sha256:{}",
                    "0".repeat(64)
                )),
                tool_id: harness::WorkflowToolId::new("input-request"),
                semantic_type: INPUT_SEMANTIC_TYPES[0].into(),
                schema_revision: 1,
                binding_fingerprint: "fp".into(),
                session_universe_id: universe_id,
                session_id,
                run_id: RunId::new(1),
                context,
                arguments_ref: BlobRef::from_bytes(b"{}"),
                execution_context_ref: None,
                completion_promises: Some(
                    [(
                        harness::REPLY_COMPLETION_KEY.to_owned(),
                        PromiseId::from_number(1),
                    )]
                    .into(),
                ),
            },
        }
    }

    #[test]
    fn start_must_be_a_scoped_call_with_one_reply() {
        let scoped = start_args(CallContext::Scope {
            tool_call_id: harness::ToolCallId::new("call-1"),
        });
        assert_eq!(
            validate_start("wtx:sha256:input", &scoped),
            Ok(PromiseId::from_number(1))
        );
        assert!(validate_start("other", &scoped).is_err());
        assert!(validate_start("wtx:sha256:input", &start_args(CallContext::Run {})).is_err());
        let mut opened = scoped.clone();
        opened.invocation.semantic_type = INPUT_SEMANTIC_TYPES[1].into();
        assert!(validate_start("wtx:sha256:input", &opened).is_ok());
        opened.invocation.semantic_type = "lightspeed.other.v1".into();
        assert!(validate_start("wtx:sha256:input", &opened).is_err());
    }
}
