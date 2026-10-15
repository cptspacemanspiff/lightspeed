//! Helpers for live tests of program runs: the session's reduced state, its
//! program run's workflow, and the attempts that workflow ran.

use harness::{
    CoreAgentState, SessionId, WorkflowToolTarget,
    storage::{ReadSessionEvents, SessionStore},
};
use temporal_workflow::{ReducedSession, reduce_session_entries_from};
use temporalio_common::protos::temporal::api::history::v1::{
    HistoryEvent, history_event::Attributes,
};

pub async fn session_state(
    store: &store_pg::PgStore,
    session_id: &SessionId,
) -> anyhow::Result<CoreAgentState> {
    let mut reduced = ReducedSession::default();
    let mut after = None;
    loop {
        let page = store
            .read_after(ReadSessionEvents {
                session_id: session_id.clone(),
                after,
                limit: 1000,
            })
            .await?;
        reduced = reduce_session_entries_from(reduced, &page.entries)?;
        if page.complete {
            return Ok(reduced.core_state);
        }
        after = page.next_after;
    }
}

/// Tool ids of the scoped calls the session admitted, in log order.
pub fn admitted_tools(state: &CoreAgentState) -> Vec<String> {
    let mut calls = state
        .code_tools
        .scopes
        .values()
        .flat_map(|scope| scope.calls.values())
        .map(|call| (call.promise_id_base, call.spec.tool_id.as_str().to_owned()))
        .collect::<Vec<_>>();
    calls.sort();
    calls.into_iter().map(|(_, tool)| tool).collect()
}

/// The workflow id of the session's program run.
pub fn program_execution_id(state: &CoreAgentState) -> anyhow::Result<String> {
    let invocation = state
        .workflow_tools
        .start_requests
        .values()
        .find(|request| request.tool_id.as_str() == harness::PROGRAM_EXECUTE_WORKFLOW_TOOL_ID)
        .ok_or_else(|| anyhow::anyhow!("program invocation missing"))?;
    let WorkflowToolTarget::Start { start } =
        &state.workflow_tools.bindings[&invocation.tool_id].target
    else {
        anyhow::bail!("program invocation must have a workflow start recipe")
    };
    Ok(harness::workflow_tool_execution_id(
        &invocation.invocation_id,
        &start.recipe_fingerprint,
    ))
}

/// For each `activity` scheduled in `events`, the Temporal attempts that
/// started (a retried activity records only its last start).
pub fn activity_attempts(events: &[HistoryEvent], activity: &str) -> Vec<Vec<i32>> {
    events
        .iter()
        .filter_map(|event| match &event.attributes {
            Some(Attributes::ActivityTaskScheduledEventAttributes(attributes))
                if attributes
                    .activity_type
                    .as_ref()
                    .is_some_and(|kind| kind.name == activity) =>
            {
                Some(event.event_id)
            }
            _ => None,
        })
        .map(|scheduled| {
            events
                .iter()
                .filter_map(|event| match &event.attributes {
                    Some(Attributes::ActivityTaskStartedEventAttributes(attributes))
                        if attributes.scheduled_event_id == scheduled =>
                    {
                        Some(attributes.attempt)
                    }
                    _ => None,
                })
                .collect()
        })
        .collect()
}
