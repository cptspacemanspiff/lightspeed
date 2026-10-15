//! Durable supervision of a program run: one invocation of a program
//! session's pinned program. Session-owned activities still execute every
//! tool effect. An attempt runs until the program finishes or has waited
//! only on calls for the hold window; a suspended run holds no interpreter
//! and resumes, replaying its recorded calls, when the session reports that
//! one of them completed. An attempt lost with its worker is retried and
//! replays the same way.

use std::time::Duration;

use futures::{FutureExt, select_biased};
use temporalio_common::protos::temporal::api::common::v1::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityCancellationType, ActivityCloseTimeouts, ActivityOptions, CancellableFuture,
    ContinueAsNewOptions, SyncWorkflowContext, WorkflowCancellationToken, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};

use crate::{
    AgentSessionWorkflow, ProgramFinalizeActivityRequest, ProgramPrepareActivityRequest,
    ProgramPrepareActivityResult, ProgramRunDescriptor, ProgramRunInterruption, ProgramRunPhase,
    ProgramRunSnapshot, ProgramRunTerminal, WorkflowActivities, WorkflowToolRecoveryResult,
    WorkflowToolStartArgs, workflows::WorkflowContextExt as _,
};

const ACTIVITY_HEARTBEAT: Duration = Duration::from_secs(15);
const ACTIVITY_CANCELLATION_GRACE: Duration = Duration::from_secs(20);
const RUNNER_MAXIMUM_ATTEMPTS: i32 = 5;

#[workflow]
#[derive(Default)]
pub struct ProgramRunWorkflow {
    snapshot: ProgramRunSnapshot,
    start: Option<WorkflowToolStartArgs>,
    holder_cancelled: bool,
    /// The most recorded outcomes the session has reported for the run's
    /// scope. A suspended attempt resumes once this passes what it saw.
    recorded: u64,
}

#[workflow_methods]
impl ProgramRunWorkflow {
    #[run(name = "ProgramRunWorkflow")]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        start: WorkflowToolStartArgs,
    ) -> WorkflowResult<()> {
        let reply = validate_start(ctx.workflow_id(), &start)
            .map_err(|message| temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!(message)))?;
        ctx.state_mut(|state| {
            state.start = Some(start.clone());
            state.snapshot.phase = ProgramRunPhase::Preparing;
        });

        let mut prepare = ctx.execute_activity(
            WorkflowActivities::program_prepare,
            ProgramPrepareActivityRequest {
                start: start.clone(),
            },
            preparation_options(),
        );
        let prepared = {
            let holder_cancel = ctx.wait_for_state(|state| state.holder_cancelled);
            let external_cancel = ctx.cancelled().fuse();
            let work = (&mut prepare).fuse();
            futures::pin_mut!(holder_cancel, external_cancel, work);
            select_biased! {
                _ = holder_cancel => Err(ProgramRunInterruption::HolderCancelled),
                _ = external_cancel => Err(ProgramRunInterruption::WorkflowCancelled),
                result = work => Ok(result),
            }
        };
        let mut terminal = match prepared {
            Err(reason) => {
                prepare.cancel();
                // A prepare receipt can race with cancellation. Record it if
                // it arrives; finalization also works without it by scope id.
                let grace = ctx
                    .timer_with_manual_cancellation(ACTIVITY_CANCELLATION_GRACE)
                    .fuse();
                let work = prepare.fuse();
                futures::pin_mut!(grace, work);
                select_biased! {
                    result = work => {
                        if let Ok(ProgramPrepareActivityResult::Prepared { descriptor }) = result {
                            ctx.state_mut(|state| state.snapshot.descriptor = Some(*descriptor));
                        }
                    },
                    _ = grace => {},
                }
                ProgramRunTerminal::Interrupted {
                    reason,
                    result: None,
                }
            }
            Ok(Err(_)) => ProgramRunTerminal::Interrupted {
                reason: ProgramRunInterruption::PreparationFailed,
                result: None,
            },
            Ok(Ok(ProgramPrepareActivityResult::Rejected { error_ref })) => {
                ProgramRunTerminal::Rejected { error_ref }
            }
            Ok(Ok(ProgramPrepareActivityResult::Prepared { descriptor })) => {
                if !descriptor_matches_start(&descriptor, &start) {
                    ProgramRunTerminal::Interrupted {
                        reason: ProgramRunInterruption::PreparationFailed,
                        result: None,
                    }
                } else {
                    ctx.state_mut(|state| {
                        state.snapshot.descriptor = Some((*descriptor).clone());
                        state.snapshot.phase = ProgramRunPhase::Running;
                    });
                    run_attempts(ctx, &start, *descriptor).await?
                }
            }
        };

        ctx.state_mut(|state| {
            state.snapshot.phase = ProgramRunPhase::Finalizing;
            state.snapshot.terminal = Some(terminal.clone());
        });
        // Detached cancellation is intentional: external workflow cancellation
        // must still close the scope and persist a report of acknowledged work.
        let mut resolution = finalize(ctx, &start, &terminal).await?;
        if let Some(cancelled_terminal) = terminal_after_cancellation(
            &terminal,
            ctx.state(|state| state.holder_cancelled),
            ctx.cancellation_token().is_cancelled(),
        ) {
            // A cancelled scoped call can finish the program before the holder's
            // cancellation reaches this workflow. If cancellation arrived while
            // finalization was in flight, retain the runner receipt and publish
            // a cancellation report. This only repeats idempotent cleanup and
            // report storage, never the interpreter; cancellation is monotonic.
            terminal = cancelled_terminal;
            ctx.state_mut(|state| state.snapshot.terminal = Some(terminal.clone()));
            resolution = finalize(ctx, &start, &terminal).await?;
        }
        let cancelled = matches!(
            terminal,
            ProgramRunTerminal::Interrupted {
                reason: ProgramRunInterruption::HolderCancelled
                    | ProgramRunInterruption::WorkflowCancelled,
                ..
            }
        );
        ctx.state_mut(|state| {
            state.snapshot.resolution = Some(resolution.clone());
            state.snapshot.phase = if cancelled {
                ProgramRunPhase::Cancelled
            } else {
                ProgramRunPhase::Resolved
            };
        });
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

    #[signal(name = "deliver_emission")]
    pub fn deliver_emission(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        envelope: harness::EmissionEnvelope,
    ) {
        let Some(start) = self.start.as_ref() else {
            return;
        };
        if crate::workflows::is_holder_reply_cancellation(start, &envelope) {
            self.holder_cancelled = true;
        }
        if let Some(completed) = scope_progress(start, &envelope) {
            self.recorded = self.recorded.max(completed);
        }
    }

    #[query(name = "snapshot")]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> ProgramRunSnapshot {
        self.snapshot.clone()
    }

    #[query(name = "workflow_tool_recovery")]
    pub fn workflow_tool_recovery(&self, _ctx: &WorkflowContextView) -> WorkflowToolRecoveryResult {
        recovery_result(&self.snapshot)
    }
}

async fn finalize(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    start: &WorkflowToolStartArgs,
    terminal: &ProgramRunTerminal,
) -> WorkflowResult<harness::PromiseResolution> {
    ctx.execute_activity(
        WorkflowActivities::program_finalize,
        ProgramFinalizeActivityRequest {
            start: start.clone(),
            descriptor: ctx.state(|state| state.snapshot.descriptor.clone()),
            terminal: terminal.clone(),
        },
        finalization_options(),
    )
    .await
    .map_err(|error| {
        temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!(
            "program run finalization failed: {error}"
        ))
        .into()
    })
}

/// Cancellation can replace a non-cancelled terminal once. Preserve any known
/// runner receipt so a late signal cannot discard already selected output.
fn terminal_after_cancellation(
    terminal: &ProgramRunTerminal,
    holder_cancelled: bool,
    workflow_cancelled: bool,
) -> Option<ProgramRunTerminal> {
    if (!holder_cancelled && !workflow_cancelled)
        || matches!(
            terminal,
            ProgramRunTerminal::Interrupted {
                reason: ProgramRunInterruption::HolderCancelled
                    | ProgramRunInterruption::WorkflowCancelled,
                ..
            }
        )
    {
        return None;
    }
    let result = match terminal {
        ProgramRunTerminal::Completed { result } => Some(result.clone()),
        ProgramRunTerminal::Interrupted { result, .. } => result.clone(),
        ProgramRunTerminal::Rejected { .. } => None,
    };
    Some(ProgramRunTerminal::Interrupted {
        reason: if workflow_cancelled {
            ProgramRunInterruption::WorkflowCancelled
        } else {
            ProgramRunInterruption::HolderCancelled
        },
        result,
    })
}

/// The run's scope progress an emission reports, if it comes from the
/// run's own session about the run's own scope.
fn scope_progress(
    start: &WorkflowToolStartArgs,
    envelope: &harness::EmissionEnvelope,
) -> Option<u64> {
    let harness::EmissionProducer::Session {
        universe_id,
        session_id,
        ..
    } = &envelope.producer
    else {
        return None;
    };
    let harness::EmissionBody::ScopeProgress {
        execution_id,
        completed,
    } = &envelope.body
    else {
        return None;
    };
    (*universe_id == start.universe_id
        && *session_id == start.invocation.session_id
        && *execution_id == start.execution_id)
        .then_some(*completed)
}

/// Run attempts until one finishes. A suspended attempt holds nothing: the
/// workflow waits for the session to report progress on the run's scope,
/// then starts the next attempt, which replays the program to where it
/// stopped. Continues as new when its history grows long.
async fn run_attempts(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    start: &WorkflowToolStartArgs,
    descriptor: ProgramRunDescriptor,
) -> WorkflowResult<ProgramRunTerminal> {
    let mut attempt = 1;
    loop {
        let terminal = run_attempt(ctx, descriptor.clone(), attempt).await;
        let ProgramRunTerminal::Completed {
            result:
                crate::ProgramAttemptActivityResult {
                    outcome: crate::ProgramAttemptOutcome::Suspended { recorded, .. },
                    ..
                },
        } = terminal
        else {
            return Ok(terminal);
        };
        ctx.state_mut(|state| state.snapshot.phase = ProgramRunPhase::Suspended);
        let woken =
            ctx.wait_for_state(move |state| state.holder_cancelled || state.recorded > recorded);
        let external_cancel = ctx.cancelled().fuse();
        futures::pin_mut!(woken, external_cancel);
        select_biased! {
            _ = external_cancel => {
                return Ok(ProgramRunTerminal::Interrupted {
                    reason: ProgramRunInterruption::WorkflowCancelled,
                    result: None,
                });
            }
            _ = woken => {}
        }
        if ctx.state(|state| state.holder_cancelled) {
            return Ok(ProgramRunTerminal::Interrupted {
                reason: ProgramRunInterruption::HolderCancelled,
                result: None,
            });
        }
        if ctx.continue_as_new_suggested()
            || ctx.history_length() >= crate::DEFAULT_CONTINUE_AS_NEW_HISTORY_THRESHOLD
        {
            // The next run prepares again, which reopens the same scope and
            // builds the same descriptor, then replays from its journal.
            match ctx.continue_as_new(start.clone(), ContinueAsNewOptions::default()) {
                Ok(never) => match never {},
                Err(termination) => return Err(termination),
            }
        }
        ctx.state_mut(|state| state.snapshot.phase = ProgramRunPhase::Running);
        attempt += 1;
    }
}

async fn run_attempt(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    descriptor: ProgramRunDescriptor,
    attempt: u32,
) -> ProgramRunTerminal {
    let options = runner_options(descriptor.limits.timeout_ms);
    let mut activity = ctx.execute_activity(
        WorkflowActivities::program_attempt,
        crate::ProgramAttemptActivityRequest {
            descriptor,
            attempt,
        },
        options,
    );
    let outcome = {
        let holder_cancel = ctx.wait_for_state(|state| state.holder_cancelled);
        let external_cancel = ctx.cancelled().fuse();
        let work = (&mut activity).fuse();
        futures::pin_mut!(holder_cancel, external_cancel, work);
        select_biased! {
            _ = holder_cancel => Err(ProgramRunInterruption::HolderCancelled),
            _ = external_cancel => Err(ProgramRunInterruption::WorkflowCancelled),
            result = work => Ok(result),
        }
    };
    match outcome {
        Ok(Ok(result)) => ProgramRunTerminal::Completed { result },
        Ok(Err(error)) => ProgramRunTerminal::Interrupted {
            reason: if error.as_timeout().is_some() {
                ProgramRunInterruption::ActivityTimedOut
            } else {
                ProgramRunInterruption::ActivityFailed
            },
            result: None,
        },
        Err(reason) => {
            activity.cancel();
            let grace = ctx
                .timer_with_manual_cancellation(ACTIVITY_CANCELLATION_GRACE)
                .fuse();
            let work = activity.fuse();
            futures::pin_mut!(grace, work);
            let result = select_biased! { result = work => result.ok(), _ = grace => None };
            ProgramRunTerminal::Interrupted { reason, result }
        }
    }
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
        return Err("program run workflow, holder, or universe identity mismatch");
    }
    if start.invocation.tool_id.as_str() != harness::PROGRAM_EXECUTE_WORKFLOW_TOOL_ID
        || start.invocation.semantic_type != harness::PROGRAM_EXECUTE_WORKFLOW_SEMANTIC_TYPE
        || !matches!(start.invocation.context, harness::CallContext::Run {})
    {
        return Err("program run start is not the run's program execution");
    }
    let promises = start
        .invocation
        .completion_promises
        .as_ref()
        .ok_or("program run requires one joined reply promise")?;
    if promises.len() != 1 {
        return Err("program run requires exactly one joined reply promise");
    }
    promises
        .get(harness::REPLY_COMPLETION_KEY)
        .cloned()
        .ok_or("program run is missing its joined reply promise")
}

fn descriptor_matches_start(
    descriptor: &ProgramRunDescriptor,
    start: &WorkflowToolStartArgs,
) -> bool {
    descriptor.validate().is_ok()
        && descriptor.execution_id == start.execution_id
        && descriptor.session_workflow_id == start.holder_workflow_id
}

fn recovery_result(snapshot: &ProgramRunSnapshot) -> WorkflowToolRecoveryResult {
    WorkflowToolRecoveryResult {
        resolutions: snapshot
            .resolution
            .clone()
            .map(|resolution| [(harness::REPLY_COMPLETION_KEY.to_owned(), resolution)].into())
            .unwrap_or_default(),
    }
}

fn preparation_options() -> ActivityOptions {
    bounded_options(Duration::from_secs(60), Duration::from_secs(90), 3)
}

/// An attempt lost with its worker is retried: the next one replays the
/// run's recorded calls. Retries share the run's wall-clock budget.
fn runner_options(timeout_ms: u64) -> ActivityOptions {
    let timeout = Duration::from_millis(timeout_ms);
    bounded_options(
        timeout.saturating_add(Duration::from_secs(30)),
        timeout.saturating_add(Duration::from_secs(60)),
        RUNNER_MAXIMUM_ATTEMPTS,
    )
}

fn finalization_options() -> ActivityOptions {
    bounded_options(Duration::from_secs(30), Duration::from_secs(60), 3)
}

fn bounded_options(
    start_to_close: Duration,
    schedule_to_close: Duration,
    maximum_attempts: i32,
) -> ActivityOptions {
    ActivityOptions::with_close_timeouts(ActivityCloseTimeouts::ScheduleAndStartToClose {
        start_to_close,
        schedule_to_close,
    })
    .heartbeat_timeout(ACTIVITY_HEARTBEAT)
    .cancellation_type(ActivityCancellationType::WaitCancellationCompleted)
    .cancellation_token(WorkflowCancellationToken::new())
    .retry_policy(RetryPolicy {
        initial_interval: Some(
            Duration::from_secs(1)
                .try_into()
                .expect("static duration fits"),
        ),
        maximum_interval: Some(
            Duration::from_secs(5)
                .try_into()
                .expect("static duration fits"),
        ),
        backoff_coefficient: 2.0,
        maximum_attempts,
        non_retryable_error_types: Vec::new(),
    })
    .build()
}

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
            execution_id: "wtx:sha256:program".into(),
            invocation: WorkflowToolInvocation {
                invocation_id: harness::WorkflowToolInvocationId::new(format!(
                    "wti:sha256:{}",
                    "0".repeat(64)
                )),
                tool_id: harness::WorkflowToolId::new(harness::PROGRAM_EXECUTE_WORKFLOW_TOOL_ID),
                semantic_type: harness::PROGRAM_EXECUTE_WORKFLOW_SEMANTIC_TYPE.into(),
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
    fn start_must_be_the_runs_own_program_execution() {
        let start = start_args(CallContext::Run {});
        assert_eq!(
            validate_start("wtx:sha256:program", &start),
            Ok(PromiseId::from_number(1))
        );
        assert!(validate_start("other", &start).is_err());
        let scoped = start_args(CallContext::Scope {
            tool_call_id: harness::ToolCallId::new("call-1"),
        });
        assert!(validate_start("wtx:sha256:program", &scoped).is_err());
    }
}
