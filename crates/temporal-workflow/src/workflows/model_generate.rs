//! One standalone model generation, supervised: started on call by the
//! calling session's start-on-call `model_generate` invocation. It runs the
//! generation activity against the model pinned at admission and resolves
//! the caller's `reply` promise with the result envelope. The holder's
//! cancellation or Temporal cancellation abandons the provider call. To the
//! caller this is indistinguishable from any other started workflow tool;
//! the session workflow carries no generation code.

use futures::{FutureExt, pin_mut, select};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

use crate::{
    AgentSessionWorkflow, ModelGenerateActivityRequest, ModelGenerateExecutionPhase,
    ModelGenerateExecutionSnapshot, WorkflowActivities, WorkflowToolRecoveryResult,
    WorkflowToolStartArgs,
};

#[workflow(name = "ModelGenerateWorkflow")]
#[derive(Default)]
pub struct ModelGenerateWorkflow {
    snapshot: ModelGenerateExecutionSnapshot,
    /// Identity fixed at start: which invocation this execution serves and
    /// which holder may cancel it.
    holder_workflow_id: Option<String>,
    invocation_id: Option<engine::WorkflowToolInvocationId>,
    holder_cancelled: bool,
}

#[workflow_methods]
impl ModelGenerateWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        start: WorkflowToolStartArgs,
    ) -> WorkflowResult<()> {
        if ctx.workflow_id() != start.execution_id
            || start.universe_id != start.invocation.session_universe_id
        {
            return Err(anyhow::anyhow!(
                "model generation identity is invalid: workflow id or universe mismatch"
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
                "model generation invocation is missing its reply completion promise"
            )
            .into());
        };
        ctx.state_mut(|state| {
            state.holder_workflow_id = Some(start.holder_workflow_id.clone());
            state.invocation_id = Some(start.invocation.invocation_id.clone());
            state.snapshot.phase = ModelGenerateExecutionPhase::Generating;
        });

        let activity_ctx = ctx.clone();
        let activity = activity_ctx.start_activity(
            WorkflowActivities::model_generate,
            ModelGenerateActivityRequest {
                start: start.clone(),
            },
            crate::llm_activity_options(),
        );
        pin_mut!(activity);
        let outcome = {
            let holder_cancelled = ctx.wait_condition(|state| state.holder_cancelled).fuse();
            let cancelled = ctx.cancelled().fuse();
            pin_mut!(holder_cancelled, cancelled);
            select! {
                result = activity.as_mut() => Outcome::Finished(result),
                _ = holder_cancelled => Outcome::HolderCancelled,
                _ = cancelled => Outcome::Cancelled,
            }
        };

        match outcome {
            Outcome::Finished(result) => {
                // Provider transients exhausted their retries, or the
                // activity timed out: the call fails without a payload.
                let resolution =
                    result.unwrap_or(engine::PromiseResolution::Failed { error_ref: None });
                ctx.state_mut(|state| {
                    state.snapshot.phase = ModelGenerateExecutionPhase::Resolved;
                    state.snapshot.resolution = Some(resolution.clone());
                });
                emit_resolution(ctx, &start, reply_promise_id, resolution).await;
                Ok(())
            }
            Outcome::HolderCancelled | Outcome::Cancelled => {
                // `TryCancel` resolves the future at once; the worker learns
                // through its heartbeat and stops the provider call.
                activity.as_ref().get_ref().cancel();
                let _ = activity.await;
                ctx.state_mut(|state| {
                    state.snapshot.phase = ModelGenerateExecutionPhase::Cancelled;
                });
                if matches!(outcome, Outcome::Cancelled) {
                    Err(temporalio_sdk::WorkflowTermination::Cancelled)
                } else {
                    // The holder already recorded the promise as cancelled.
                    Ok(())
                }
            }
        }
    }

    /// The holder's `invocation_cancellation` for this call's `reply` key.
    #[signal(name = "deliver_emission")]
    pub fn deliver_emission(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        envelope: engine::EmissionEnvelope,
    ) {
        if is_holder_cancellation(
            self.holder_workflow_id.as_deref(),
            self.invocation_id.as_ref(),
            &envelope,
        ) {
            self.holder_cancelled = true;
        }
    }

    #[query(name = "snapshot")]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> ModelGenerateExecutionSnapshot {
        self.snapshot.clone()
    }

    #[query(name = "workflow_tool_recovery")]
    pub fn workflow_tool_recovery(&self, _ctx: &WorkflowContextView) -> WorkflowToolRecoveryResult {
        recovery_result(&self.snapshot)
    }
}

enum Outcome<T> {
    Finished(T),
    HolderCancelled,
    Cancelled,
}

/// Pure acceptance rule for cancellation: only the recorded holder may
/// cancel, and only this invocation's `reply` key.
pub(crate) fn is_holder_cancellation(
    holder_workflow_id: Option<&str>,
    invocation_id: Option<&engine::WorkflowToolInvocationId>,
    envelope: &engine::EmissionEnvelope,
) -> bool {
    let engine::EmissionBody::InvocationCancellation {
        invocation_id: cancelled,
        completion_key,
        ..
    } = &envelope.body
    else {
        return false;
    };
    let from_holder = match &envelope.producer {
        engine::EmissionProducer::Session {
            universe_id,
            session_id,
            ..
        } => {
            holder_workflow_id
                == Some(crate::compose_workflow_id(*universe_id, session_id).as_str())
        }
        engine::EmissionProducer::Workflow { .. } => false,
    };
    from_holder
        && invocation_id == Some(cancelled)
        && completion_key == engine::REPLY_COMPLETION_KEY
}

/// The holder-side recovery view: the `reply` resolution once produced.
pub(crate) fn recovery_result(
    snapshot: &ModelGenerateExecutionSnapshot,
) -> WorkflowToolRecoveryResult {
    let mut resolutions = std::collections::BTreeMap::new();
    if let Some(resolution) = &snapshot.resolution {
        resolutions.insert(engine::REPLY_COMPLETION_KEY.to_owned(), resolution.clone());
    }
    WorkflowToolRecoveryResult { resolutions }
}

async fn emit_resolution(
    ctx: &mut WorkflowContext<ModelGenerateWorkflow>,
    start: &WorkflowToolStartArgs,
    reply_promise_id: engine::PromiseId,
    resolution: engine::PromiseResolution,
) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cancellation(
        universe_id: uuid::Uuid,
        session_id: &str,
        invocation_id: &engine::WorkflowToolInvocationId,
        completion_key: &str,
    ) -> engine::EmissionEnvelope {
        let mut envelope = engine::EmissionEnvelope::source_resolution(
            universe_id,
            "unused".to_owned(),
            "unused",
            engine::PromiseId::new("promise_1"),
            engine::PromiseResolution::Failed { error_ref: None },
        );
        envelope.producer = engine::EmissionProducer::Session {
            universe_id,
            session_id: engine::SessionId::new(session_id),
            log_seq: engine::EventSeq::new(1),
        };
        envelope.body = engine::EmissionBody::InvocationCancellation {
            invocation_id: invocation_id.clone(),
            completion_key: completion_key.to_owned(),
            promise_id: engine::PromiseId::new("promise_1"),
        };
        envelope
    }

    #[test]
    fn only_the_holder_can_cancel_this_invocations_reply() {
        let universe_id = uuid::Uuid::from_u128(7);
        let holder = crate::compose_workflow_id(universe_id, &engine::SessionId::new("caller"));
        let invocation =
            engine::WorkflowToolInvocationId::new(format!("wti:sha256:{}", "a".repeat(64)));
        let other = engine::WorkflowToolInvocationId::new(format!("wti:sha256:{}", "b".repeat(64)));
        let accepts = |envelope: &engine::EmissionEnvelope| {
            is_holder_cancellation(Some(&holder), Some(&invocation), envelope)
        };
        assert!(accepts(&cancellation(
            universe_id,
            "caller",
            &invocation,
            engine::REPLY_COMPLETION_KEY
        )));
        assert!(!accepts(&cancellation(
            universe_id,
            "stranger",
            &invocation,
            engine::REPLY_COMPLETION_KEY
        )));
        assert!(!accepts(&cancellation(
            universe_id,
            "caller",
            &other,
            engine::REPLY_COMPLETION_KEY
        )));
        assert!(!accepts(&cancellation(
            universe_id,
            "caller",
            &invocation,
            "job-1"
        )));
    }

    #[test]
    fn recovery_reports_the_reply_once_resolved() {
        let mut snapshot = ModelGenerateExecutionSnapshot::default();
        assert!(recovery_result(&snapshot).resolutions.is_empty());
        snapshot.resolution = Some(engine::PromiseResolution::Failed { error_ref: None });
        assert!(
            recovery_result(&snapshot)
                .resolutions
                .contains_key(engine::REPLY_COMPLETION_KEY)
        );
    }
}
