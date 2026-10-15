use std::time::Duration;

use futures::{FutureExt, future::FusedFuture};
use temporalio_sdk::{
    CancellableFuture, SignalWorkflowOptions, TimerOptions, TimerResult, WaitConditionOptions,
    WorkflowCancellationToken, WorkflowContext,
};

// These workflows own cancellation through their control loops and explicitly cancel
// in-flight operations. Detached tokens keep cleanup activities, outbound signals,
// and their waits alive after the workflow cancellation notification arrives.
pub(crate) trait WorkflowContextExt<W> {
    fn wait_for_state<'a>(
        &'a self,
        condition: impl FnMut(&W) -> bool + 'a,
    ) -> impl FusedFuture<Output = ()> + 'a;

    fn timer_with_manual_cancellation(
        &self,
        duration: Duration,
    ) -> impl CancellableFuture<Output = TimerResult>;
}

impl<W> WorkflowContextExt<W> for WorkflowContext<W> {
    fn wait_for_state<'a>(
        &'a self,
        condition: impl FnMut(&W) -> bool + 'a,
    ) -> impl FusedFuture<Output = ()> + 'a {
        self.wait_condition_with_options(
            condition,
            WaitConditionOptions::builder()
                .cancellation_token(WorkflowCancellationToken::new())
                .build(),
        )
        .map(|result| result.expect("detached wait token is never cancelled"))
        .fuse()
    }

    fn timer_with_manual_cancellation(
        &self,
        duration: Duration,
    ) -> impl CancellableFuture<Output = TimerResult> {
        self.timer(
            TimerOptions::builder(duration)
                .cancellation_token(WorkflowCancellationToken::new())
                .build(),
        )
    }
}

pub(crate) fn signal_options() -> SignalWorkflowOptions {
    SignalWorkflowOptions::builder()
        .cancellation_token(WorkflowCancellationToken::new())
        .build()
}

/// Whether an emission is the holder session withdrawing a joined call's
/// reply: its own invocation and reply promise, from its own session.
pub(crate) fn is_joined_holder_cancellation(
    start: &crate::WorkflowToolStartArgs,
    envelope: &harness::EmissionEnvelope,
) -> bool {
    let harness::EmissionProducer::Session {
        universe_id,
        session_id,
        ..
    } = &envelope.producer
    else {
        return false;
    };
    let harness::EmissionBody::InvocationCancellation {
        invocation_id,
        completion_key,
        promise_id,
    } = &envelope.body
    else {
        return false;
    };
    *universe_id == start.universe_id
        && *session_id == start.invocation.session_id
        && crate::compose_workflow_id(*universe_id, session_id) == start.holder_workflow_id
        && *invocation_id == start.invocation.invocation_id
        && completion_key == harness::REPLY_COMPLETION_KEY
        && start
            .invocation
            .completion_promises
            .as_ref()
            .and_then(|promises| promises.get(harness::REPLY_COMPLETION_KEY))
            == Some(promise_id)
}
