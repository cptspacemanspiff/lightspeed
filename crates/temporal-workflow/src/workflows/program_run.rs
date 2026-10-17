//! Durable execution of a program run: one invocation of a program
//! session's pinned program, stepped inside this workflow. Session-owned
//! activities still execute every tool effect. A waiting program holds no
//! worker; it steps again when the session reports that one of its calls
//! completed. A program lost with its worker replays from this workflow's
//! history to the same point. A program that continues as new hands its
//! run over to a new execution of this workflow, with fresh history and its
//! own scope.

use std::time::Duration;

use futures::{FutureExt, select_biased};
use temporalio_common::protos::temporal::api::common::v1::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityCancellationType, ActivityCloseTimeouts, ActivityOptions, CancellableFuture,
    LocalActivityOptions, SyncWorkflowContext, WorkflowCancellationToken, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};

use crate::{
    AgentSessionWorkflow, ProgramContinuation, ProgramFinalizeActivityRequest,
    ProgramPrepareActivityRequest, ProgramPrepareActivityResult, ProgramRunDescriptor,
    ProgramRunInput, ProgramRunInterruption, ProgramRunPhase, ProgramRunSnapshot,
    ProgramRunTerminal, WorkflowActivities, WorkflowToolRecoveryResult, WorkflowToolStartArgs,
    workflows::WorkflowContextExt as _,
};

const ACTIVITY_HEARTBEAT: Duration = Duration::from_secs(15);
const ACTIVITY_CANCELLATION_GRACE: Duration = Duration::from_secs(20);

#[workflow]
#[derive(Default)]
pub struct ProgramRunWorkflow {
    snapshot: ProgramRunSnapshot,
    start: Option<WorkflowToolStartArgs>,
    holder_cancelled: bool,
    /// Which execution of the run this is; its scope is the one it reads.
    execution: u32,
    /// The stored report of the execution this one continued.
    previous_report: Option<harness::BlobRef>,
    /// The most recorded outcomes the session has reported for this
    /// execution's scope. A suspended attempt resumes once this passes what
    /// it saw.
    recorded: u64,
    /// The most handles withdrawn at the run's close the session has
    /// reported settled. The exit waits for all of them.
    withdrawals_settled: u64,
}

#[workflow_methods]
impl ProgramRunWorkflow {
    #[run(name = "ProgramRunWorkflow")]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: ProgramRunInput,
    ) -> WorkflowResult<()> {
        let ProgramRunInput {
            start,
            continuation,
        } = input;
        let reply = validate_start(ctx.workflow_id(), &start)
            .map_err(|message| temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!(message)))?;
        ctx.state_mut(|state| {
            state.start = Some(start.clone());
            state.snapshot.phase = ProgramRunPhase::Preparing;
        });

        let end = match continuation {
            Some(continuation) => continue_run(ctx, &start, continuation).await,
            None => start_run(ctx, &start).await,
        };
        // A program that continues as new hands its run over once everything
        // it started has settled; the next execution starts from the state.
        let end = match end {
            ProgramEnd::Finished { descriptor, report }
                if report.error.is_none()
                    && report.exit == Some(program_engine::ProgramExit::ContinueAsNew) =>
            {
                match hand_over(ctx, &start, descriptor, report).await {
                    Ok(continuation) => {
                        let next = ProgramRunInput {
                            start,
                            continuation: Some(continuation),
                        };
                        return match ctx.continue_as_new(next, Default::default()) {
                            Err(termination) => Err(termination),
                            Ok(never) => match never {},
                        };
                    }
                    Err(end) => end,
                }
            }
            end => end,
        };

        // Every exit, finished or interrupted, cancels what the program
        // still owns and waits until each call has an outcome and each
        // withdrawn handle has settled: the same wait an await is, with the
        // outcomes not delivered to the program. Then the run concludes.
        ctx.state_mut(|state| state.snapshot.phase = ProgramRunPhase::Finalizing);
        let program_context = match &end {
            ProgramEnd::Finished { report, .. }
                if report.error.is_none()
                    && report.exit == Some(program_engine::ProgramExit::Complete) =>
            {
                report.state.clone()
            }
            _ => None,
        };
        let close_error = settle(ctx, &start, program_context).await;
        let mut terminal = match end {
            ProgramEnd::Finished { descriptor, report } => {
                conclude(ctx, &descriptor, *report, close_error).await
            }
            ProgramEnd::Terminal(terminal) => terminal,
        };
        ctx.state_mut(|state| state.snapshot.terminal = Some(terminal.clone()));
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
        let scope_id = harness::program_scope_id(&start.execution_id, self.execution);
        if let Some((completed, withdrawals_settled)) = scope_progress(start, &scope_id, &envelope)
        {
            self.recorded = self.recorded.max(completed);
            self.withdrawals_settled = self.withdrawals_settled.max(withdrawals_settled);
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

/// The first execution of a run: prepare the run's descriptor and scope,
/// then run the program.
async fn start_run(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    start: &WorkflowToolStartArgs,
) -> ProgramEnd {
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
    match prepared {
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
            ProgramEnd::Terminal(interrupted(reason))
        }
        Ok(Err(_)) => ProgramEnd::Terminal(interrupted(ProgramRunInterruption::PreparationFailed)),
        Ok(Ok(ProgramPrepareActivityResult::Rejected { error_ref })) => {
            ProgramEnd::Terminal(ProgramRunTerminal::Rejected { error_ref })
        }
        Ok(Ok(ProgramPrepareActivityResult::Prepared { descriptor })) => {
            if !descriptor_matches_start(&descriptor, start) {
                ProgramEnd::Terminal(interrupted(ProgramRunInterruption::PreparationFailed))
            } else {
                ctx.state_mut(|state| {
                    state.snapshot.descriptor = Some((*descriptor).clone());
                    state.snapshot.phase = ProgramRunPhase::Running;
                });
                run_program(ctx, *descriptor).await
            }
        }
    }
}

/// A later execution of a run that continued as new: the run's descriptor
/// comes with it, so it is not prepared again.
async fn continue_run(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    start: &WorkflowToolStartArgs,
    continuation: ProgramContinuation,
) -> ProgramEnd {
    let descriptor = continuation.descriptor;
    if descriptor.execution == 0 || !descriptor_matches_start(&descriptor, start) {
        return ProgramEnd::Terminal(interrupted(ProgramRunInterruption::PreparationFailed));
    }
    ctx.state_mut(|state| {
        state.execution = descriptor.execution;
        state.previous_report = Some(continuation.previous_report);
        state.snapshot.descriptor = Some(descriptor.clone());
        state.snapshot.phase = ProgramRunPhase::Running;
    });
    run_program(ctx, descriptor).await
}

/// Hand a run over to its next execution: settle this execution's scope as
/// every exit does, then store the state and this execution's report and
/// open the next scope. Returns how the run ends here instead when the
/// hand-over cannot happen.
async fn hand_over(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    start: &WorkflowToolStartArgs,
    descriptor: Box<ProgramRunDescriptor>,
    report: Box<program_engine::ExecutionReport>,
) -> Result<ProgramContinuation, ProgramEnd> {
    ctx.state_mut(|state| state.snapshot.phase = ProgramRunPhase::Finalizing);
    if settle(ctx, start, None).await.is_some() {
        // The scope did not close: the run concludes as a failed cleanup.
        return Err(ProgramEnd::Finished { descriptor, report });
    }
    let checkpoint = ctx
        .execute_local_activity(
            WorkflowActivities::program_checkpoint,
            crate::ProgramCheckpointActivityRequest {
                descriptor: (*descriptor).clone(),
                report: *report,
                previous_report: ctx.state(|state| state.previous_report.clone()),
            },
            conclude_options(),
        )
        .await;
    let continuation = match checkpoint {
        Ok(result) => result.continuation,
        Err(_) => {
            return Err(ProgramEnd::Terminal(interrupted(
                ProgramRunInterruption::ActivityFailed,
            )));
        }
    };
    // A cancel that arrived meanwhile ends the run here, in the next
    // execution's scope, which the exit then closes.
    let reason = if ctx.cancellation_token().is_cancelled() {
        Some(ProgramRunInterruption::WorkflowCancelled)
    } else if ctx.state(|state| state.holder_cancelled) {
        Some(ProgramRunInterruption::HolderCancelled)
    } else {
        None
    };
    match reason {
        Some(reason) => {
            ctx.state_mut(|state| {
                state.execution = continuation.descriptor.execution;
                state.previous_report = Some(continuation.previous_report.clone());
                state.snapshot.descriptor = Some(continuation.descriptor.clone());
                state.recorded = 0;
                state.withdrawals_settled = 0;
            });
            Err(ProgramEnd::Terminal(interrupted(reason)))
        }
        None => Ok(continuation),
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
            previous_report: ctx.state(|state| state.previous_report.clone()),
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

/// The scope progress an emission reports (recorded outcomes, settled
/// withdrawals), if it comes from the run's own session about this
/// execution's scope.
fn scope_progress(
    start: &WorkflowToolStartArgs,
    scope_id: &str,
    envelope: &harness::EmissionEnvelope,
) -> Option<(u64, u64)> {
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
        withdrawals_settled,
    } = &envelope.body
    else {
        return None;
    };
    (*universe_id == start.universe_id
        && *session_id == start.invocation.session_id
        && execution_id == scope_id)
        .then_some((*completed, *withdrawals_settled))
}

/// Run the pinned program in this workflow. The program is stepped on the
/// workflow thread: the requests a step issues are admitted to the run's
/// scope by one local activity, and the session's progress emissions wake the
/// workflow to collect outcomes, which feed the next step. Every input to a
/// step is in this workflow's history, so a replay after a lost worker or
/// cache eviction re-runs the program to the same point, and a waiting
/// program holds nothing but this workflow.
async fn run_program(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    descriptor: ProgramRunDescriptor,
) -> ProgramEnd {
    let loaded = match cancellable(
        ctx,
        ctx.execute_activity(
            WorkflowActivities::program_load,
            crate::ProgramLoadActivityRequest {
                descriptor: descriptor.clone(),
            },
            preparation_options(),
        ),
    )
    .await
    {
        Err(reason) => return ProgramEnd::Terminal(interrupted(reason)),
        Ok(Err(_)) => {
            return ProgramEnd::Terminal(interrupted(ProgramRunInterruption::PreparationFailed));
        }
        Ok(Ok(loaded)) => loaded,
    };
    let (source, input, context, bindings) = match loaded {
        crate::ProgramLoadActivityResult::Refused { result } => {
            return ProgramEnd::Terminal(ProgramRunTerminal::Completed { result });
        }
        crate::ProgramLoadActivityResult::Loaded {
            source,
            input,
            context,
            bindings,
        } => (source, input, context, bindings),
    };
    let started = program_engine::ProgramInstance::start(
        program_engine::ExecutionInput {
            source,
            bindings,
            limits: engine_limits(&descriptor.limits),
            program: program_engine::ProgramExecution {
                input,
                started_at_ms: descriptor.started_at_ms,
                random_seed: program_random_seed(&descriptor.scope_id()),
                state: context,
                continued: descriptor.execution > 0,
                clock_ms: descriptor.clock_ms,
            },
        },
        program_engine::StepBudget::default(),
    );
    let (mut instance, mut step) = match started {
        Ok(started) => started,
        Err(error) => {
            return ProgramEnd::Finished {
                descriptor: Box::new(descriptor),
                report: Box::new(failed_report(
                    program_engine::ExecutionErrorKind::Internal,
                    error.to_string(),
                )),
            };
        }
    };
    // Completions for the next step, in the order this history received
    // them: admission rejections and collected outcomes.
    let mut ready = Vec::new();
    // Recorded outcomes of the scope already collected.
    let mut collected = 0u64;
    loop {
        let (requests, cancels, finished) = match step {
            program_engine::Step::Waiting { requests, cancels } => (requests, cancels, None),
            program_engine::Step::Finished {
                requests,
                cancels,
                report,
            } => (requests, cancels, Some(report)),
        };
        if !requests.is_empty() || !cancels.is_empty() {
            let admitted = cancellable(
                ctx,
                ctx.execute_local_activity(
                    WorkflowActivities::program_admit,
                    crate::ProgramAdmitActivityRequest {
                        descriptor: descriptor.clone(),
                        requests,
                        cancels,
                    },
                    admission_options(),
                ),
            )
            .await;
            match admitted {
                Err(reason) => return ProgramEnd::Terminal(interrupted(reason)),
                Ok(Err(error)) => {
                    return ProgramEnd::Terminal(interrupted(if error.as_timeout().is_some() {
                        ProgramRunInterruption::ActivityTimedOut
                    } else {
                        ProgramRunInterruption::ActivityFailed
                    }));
                }
                Ok(Ok(result)) => ready.extend(result.rejected),
            }
        }
        if let Some(report) = finished {
            return ProgramEnd::Finished {
                descriptor: Box::new(descriptor),
                report: Box::new(report),
            };
        }
        if ready.is_empty() {
            ctx.state_mut(|state| state.snapshot.phase = ProgramRunPhase::Suspended);
            let progressed = ctx
                .wait_for_state(move |state| state.holder_cancelled || state.recorded > collected);
            let external_cancel = ctx.cancelled().fuse();
            futures::pin_mut!(progressed, external_cancel);
            select_biased! {
                _ = external_cancel => {
                    return ProgramEnd::Terminal(interrupted(
                        ProgramRunInterruption::WorkflowCancelled,
                    ));
                }
                _ = progressed => {}
            }
            if ctx.state(|state| state.holder_cancelled) {
                return ProgramEnd::Terminal(interrupted(ProgramRunInterruption::HolderCancelled));
            }
            ctx.state_mut(|state| state.snapshot.phase = ProgramRunPhase::Running);
        }
        if ctx.state(|state| state.recorded > collected) {
            let page = ctx
                .execute_local_activity(
                    WorkflowActivities::program_collect,
                    crate::ProgramCollectActivityRequest {
                        session_workflow_id: descriptor.session_workflow_id.clone(),
                        execution_id: descriptor.scope_id(),
                        after: collected,
                        max_result_bytes: descriptor.limits.max_result_bytes,
                    },
                    collect_options(),
                )
                .await;
            match page {
                Ok(page) => {
                    collected += page.completions.len() as u64;
                    ready.extend(page.completions);
                }
                Err(_) => {
                    return ProgramEnd::Terminal(interrupted(
                        ProgramRunInterruption::ActivityFailed,
                    ));
                }
            }
        }
        step = instance.step(std::mem::take(&mut ready));
    }
}

/// How the program part of a run ended.
enum ProgramEnd {
    /// The program finished: returned, threw, or failed in the engine.
    Finished {
        descriptor: Box<ProgramRunDescriptor>,
        report: Box<program_engine::ExecutionReport>,
    },
    /// The run's terminal is already known: refused before the program ran,
    /// or interrupted.
    Terminal(ProgramRunTerminal),
}

/// Close this execution's scope, cancelling what the program still owns and
/// withdrawing its undetached handles, then wait until every admitted call
/// has a recorded outcome and every withdrawn handle has settled. The wait
/// is the one a suspended program uses, woken by the session's progress
/// signals; it holds no worker and has no deadline. The close is a local
/// activity, so the context it keeps is never recorded in history. Returns
/// why the close did not go through, if it did not.
async fn settle(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    start: &WorkflowToolStartArgs,
    program_context: Option<serde_json::Value>,
) -> Option<String> {
    let closed = ctx
        .execute_local_activity(
            WorkflowActivities::program_close,
            crate::ProgramCloseActivityRequest {
                universe_id: start.universe_id,
                session_workflow_id: start.holder_workflow_id.clone(),
                execution_id: start.execution_id.clone(),
                execution: ctx.state(|state| state.execution),
                program_context,
            },
            conclude_options(),
        )
        .await;
    match closed {
        Ok(crate::ProgramCloseActivityResult::Closed {
            calls,
            recorded,
            withdrawals,
            withdrawals_settled,
            context_error,
        }) => {
            ctx.state_mut(|state| {
                state.recorded = state.recorded.max(recorded);
                state.withdrawals_settled = state.withdrawals_settled.max(withdrawals_settled);
            });
            ctx.wait_for_state(move |state| {
                state.recorded >= calls && state.withdrawals_settled >= withdrawals
            })
            .await;
            context_error
        }
        Ok(crate::ProgramCloseActivityResult::NoScope) => None,
        Err(error) => Some(format!("scope close failed: {error}")),
    }
}

/// Keep the finished program's output and report, once everything it
/// started has settled. A local activity: its arguments (the output the
/// program made) are not recorded in history, only its small result.
async fn conclude(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    descriptor: &ProgramRunDescriptor,
    report: program_engine::ExecutionReport,
    cleanup_error: Option<String>,
) -> ProgramRunTerminal {
    let concluded = ctx
        .execute_local_activity(
            WorkflowActivities::program_conclude,
            crate::ProgramConcludeActivityRequest {
                descriptor: descriptor.clone(),
                report,
                cleanup_error,
                previous_report: ctx.state(|state| state.previous_report.clone()),
            },
            conclude_options(),
        )
        .await;
    match concluded {
        Ok(result) => ProgramRunTerminal::Completed { result },
        Err(_) => interrupted(ProgramRunInterruption::ActivityFailed),
    }
}

fn interrupted(reason: ProgramRunInterruption) -> ProgramRunTerminal {
    ProgramRunTerminal::Interrupted {
        reason,
        result: None,
    }
}

fn failed_report(
    kind: program_engine::ExecutionErrorKind,
    message: String,
) -> program_engine::ExecutionReport {
    program_engine::ExecutionReport {
        return_value: None,
        exit: None,
        state: None,
        clock_ms: 0,
        error: Some(program_engine::ExecutionError { kind, message }),
        pending_request_ids: Vec::new(),
        metrics: Default::default(),
    }
}

/// Race `work` against the holder's or the workflow's cancellation.
async fn cancellable<T>(
    ctx: &WorkflowContext<ProgramRunWorkflow>,
    work: impl std::future::Future<Output = T>,
) -> Result<T, ProgramRunInterruption> {
    let holder_cancel = ctx.wait_for_state(|state| state.holder_cancelled);
    let external_cancel = ctx.cancelled().fuse();
    let work = work.fuse();
    futures::pin_mut!(holder_cancel, external_cancel, work);
    select_biased! {
        _ = holder_cancel => Err(ProgramRunInterruption::HolderCancelled),
        _ = external_cancel => Err(ProgramRunInterruption::WorkflowCancelled),
        result = work => Ok(result),
    }
}

fn engine_limits(limits: &crate::CodeExecutionLimits) -> program_engine::ExecutionLimits {
    program_engine::ExecutionLimits {
        max_memory_bytes: limits.max_memory_bytes,
        max_stack_bytes: limits.max_stack_bytes,
        max_source_bytes: limits.max_source_bytes,
        max_catalog_bytes: limits.max_catalog_bytes,
        max_request_bytes: limits.max_request_bytes,
        max_result_bytes: limits.max_result_bytes,
        max_output_bytes: limits.max_output_bytes,
        max_tool_calls: limits.max_tool_calls,
        max_outstanding_tool_calls: limits.max_outstanding_tool_calls,
    }
}

/// The seed of a program's randomness: a function of its execution's scope
/// id (the run and the execution number), so every replay draws the same
/// values and each execution of a continued run draws new ones.
pub fn program_random_seed(execution_id: &str) -> [u32; 4] {
    let digest = harness::BlobRef::from_bytes(execution_id.as_bytes());
    let hex = digest
        .as_str()
        .strip_prefix("sha256:")
        .expect("blob references are sha256 digests");
    std::array::from_fn(|word| {
        u32::from_str_radix(&hex[word * 8..word * 8 + 8], 16).expect("hex digest")
    })
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

/// Admission runs on the program worker inside the workflow task, so a step
/// costs one marker instead of an activity and a second workflow task. A
/// rerun after a lost result is safe: admission is idempotent per request id.
/// Short tries keep a stalled session from holding the task long; the total
/// matches the server-scheduled activity this replaced.
fn admission_options() -> LocalActivityOptions {
    LocalActivityOptions::builder()
        .schedule_to_close_timeout(Duration::from_secs(120))
        .start_to_close_timeout(Duration::from_secs(5))
        .retry_policy(
            temporalio_client::RetryPolicy::builder()
                .initial_interval(Duration::from_secs(1))
                .backoff_coefficient(2.0)
                .maximum_interval(Duration::from_secs(5))
                .build(),
        )
        .build()
}

fn collect_options() -> LocalActivityOptions {
    LocalActivityOptions::builder()
        .schedule_to_close_timeout(Duration::from_secs(60))
        .start_to_close_timeout(Duration::from_secs(30))
        .build()
}

/// The budget the server-scheduled conclusion had: closing the scope waits
/// at most 10 s for its calls to settle.
fn conclude_options() -> LocalActivityOptions {
    LocalActivityOptions::builder()
        .schedule_to_close_timeout(Duration::from_secs(60))
        .start_to_close_timeout(Duration::from_secs(30))
        .retry_policy(
            temporalio_client::RetryPolicy::builder()
                .initial_interval(Duration::from_secs(1))
                .backoff_coefficient(2.0)
                .maximum_interval(Duration::from_secs(5))
                .maximum_attempts(3)
                .build(),
        )
        .build()
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
    fn an_execution_reads_progress_only_for_its_own_scope() {
        let start = start_args(CallContext::Run {});
        let progress = |scope_id: &str| {
            harness::EmissionEnvelope::scope_progress(
                start.universe_id,
                start.invocation.session_id.clone(),
                harness::EventSeq::new(9),
                scope_id.to_owned(),
                3,
                1,
            )
        };
        let second = harness::program_scope_id(&start.execution_id, 1);
        assert_eq!(
            scope_progress(&start, &second, &progress(&second)),
            Some((3, 1))
        );
        assert_eq!(
            scope_progress(&start, &second, &progress(&start.execution_id)),
            None
        );
    }

    #[test]
    fn a_continued_execution_starts_from_the_input_the_session_started() {
        // The session starts a run with its start alone; a continued
        // execution's input carries the start beside its continuation.
        let start = start_args(CallContext::Run {});
        let input: ProgramRunInput =
            serde_json::from_value(serde_json::to_value(&start).unwrap()).unwrap();
        assert_eq!((input.start, input.continuation), (start, None));
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
