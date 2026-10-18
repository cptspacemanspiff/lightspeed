//! The memory a session's workflow starts an execution with.
//!
//! A session's workflow rebuilds its state from the session log when it first
//! starts and at each continue-as-new, and that state has to fit in one
//! activity result. The log and its checkpoints keep the full record for every
//! other reader; the workflow needs only the work still in progress.
//! `trim_for_handoff` leaves out finished work that no decision of the
//! workflow reads again, so the loaded state stops growing with the session's
//! age.

use std::collections::BTreeSet;

use crate::{CoreAgentState, PromiseId, PromiseScope, RunId, ScopeOwner, WorkflowToolInvocation};

/// How many completed runs' summaries the workflow keeps, newest first. The
/// gateway pages runs and checks duplicate submissions against the complete
/// state; the workflow needs recent runs for the status its callers poll, for
/// submission races the gateway cannot see, and for close-on-terminal, which
/// needs at least one.
pub const HANDOFF_COMPLETED_RUNS: usize = 100;

/// Trim `state` to what a new execution of the session's workflow needs.
///
/// Live runs (the active run and queued ones) and anything still in progress
/// stay; what ended runs left finished goes.
/// - A code-tool scope stays while its run is live or it still has work: it is
///   open, a call has no outcome yet, or a withdrawn handle is pending.
/// - A workflow-tool invocation stays while its run is live, a kept scope
///   names it as its parent, or one of its completion promises is pending. Its
///   delivery or start failure stays with it.
/// - A promise stays while it is pending, detached (any later run may await a
///   session-scoped promise), scoped to a live run, or a completion promise of
///   a kept invocation.
/// - The newest `HANDOFF_COMPLETED_RUNS` completed runs stay.
///
/// Nothing left out is asked for again: admission treats a late delivery or
/// cancel for an issued promise it no longer holds as a no-op, and an
/// execution sends the cancels it owes settled starts before it hands over.
pub fn trim_for_handoff(state: &mut CoreAgentState) {
    let CoreAgentState {
        runs,
        promises,
        workflow_tools,
        code_tools,
        ..
    } = state;
    let live_runs: BTreeSet<RunId> = runs
        .active
        .iter()
        .map(|run| run.run_id)
        .chain(runs.queued.iter().map(|run| run.run_id))
        .collect();
    let pending = |promise_id: &PromiseId| {
        promises
            .promises
            .get(promise_id)
            .is_some_and(|promise| !promise.status.is_terminal())
    };

    let mut parents = BTreeSet::new();
    code_tools.scopes.retain(|_, scope| {
        let run_id = match &scope.spec.owner {
            ScopeOwner::Run { run_id } => Some(*run_id),
            ScopeOwner::ModelInvocation {
                parent_invocation_id,
            } => workflow_tools
                .emissions
                .get(parent_invocation_id)
                .or_else(|| workflow_tools.start_requests.get(parent_invocation_id))
                .map(|invocation| invocation.run_id),
        };
        let keep = run_id.is_some_and(|run_id| live_runs.contains(&run_id))
            || !scope.closed
            || scope.calls.values().any(|call| !call.status.is_terminal())
            || scope.withdrawn.iter().any(pending);
        if keep
            && let ScopeOwner::ModelInvocation {
                parent_invocation_id,
            } = &scope.spec.owner
        {
            parents.insert(parent_invocation_id.clone());
        }
        keep
    });

    let keep_invocation = |invocation: &WorkflowToolInvocation| {
        live_runs.contains(&invocation.run_id)
            || parents.contains(&invocation.invocation_id)
            || invocation
                .completion_promises
                .as_ref()
                .is_some_and(|keyed| keyed.values().any(pending))
    };
    workflow_tools
        .emissions
        .retain(|_, invocation| keep_invocation(invocation));
    workflow_tools
        .start_requests
        .retain(|_, invocation| keep_invocation(invocation));
    workflow_tools
        .delivery_failures
        .retain(|invocation_id, _| workflow_tools.emissions.contains_key(invocation_id));
    workflow_tools
        .start_failures
        .retain(|invocation_id, _| workflow_tools.start_requests.contains_key(invocation_id));

    let completions: BTreeSet<PromiseId> = workflow_tools
        .emissions
        .values()
        .chain(workflow_tools.start_requests.values())
        .filter_map(|invocation| invocation.completion_promises.as_ref())
        .flat_map(|keyed| keyed.values().cloned())
        .collect();
    promises.promises.retain(|promise_id, promise| {
        !promise.status.is_terminal()
            || match promise.scope {
                PromiseScope::Session => true,
                PromiseScope::Run { run_id } => live_runs.contains(&run_id),
            }
            || completions.contains(promise_id)
    });
    promises
        .cancel_requested
        .retain(|promise_id| promises.promises.contains_key(promise_id));

    let ended = runs.completed.len().saturating_sub(HANDOFF_COMPLETED_RUNS);
    runs.completed.drain(..ended);
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn invocation_id(n: u64) -> String {
        format!("wti:sha256:{n:064x}")
    }

    fn promise(n: u64, scope: Value, status: &str) -> crate::Promise {
        serde_json::from_value(json!({
            "promise_id": format!("promise_{n}"),
            "source": { "kind": "timer", "fire_at_ms": 0 },
            "scope": scope,
            "ownership": "model",
            "status": status,
            "payload_ref": null,
            "error_ref": null,
            "deadline_ms": null,
        }))
        .expect("promise")
    }

    fn run_scope(run_id: u64) -> Value {
        json!({ "kind": "run", "run_id": run_id })
    }

    fn invocation(n: u64, run_id: u64, promises: &[(&str, u64)]) -> WorkflowToolInvocation {
        let mut invocation = json!({
            "invocation_id": invocation_id(n),
            "tool_id": "subagent-run",
            "semantic_type": "lightspeed.subagent.run.v1",
            "schema_revision": 1,
            "binding_fingerprint": format!("wtb:sha256:{n:064x}"),
            "session_universe_id": "00000000-0000-0000-0000-000000000001",
            "session_id": "session-a",
            "run_id": run_id,
            "tool_call_id": format!("call-{n}"),
            "arguments_ref": format!("sha256:{n:064x}"),
            "execution_context_ref": null,
        });
        if !promises.is_empty() {
            invocation["completion_promises"] = promises
                .iter()
                .map(|(key, number)| ((*key).to_owned(), json!(format!("promise_{number}"))))
                .collect::<serde_json::Map<_, _>>()
                .into();
        }
        serde_json::from_value(invocation).expect("invocation")
    }

    fn scope(owner: Value, closed: bool, call_status: Value) -> crate::CodeToolScope {
        serde_json::from_value(json!({
            "toolset_revision": 1,
            "spec": {
                "execution_id": "wtx:scope",
                "owner": owner,
                "policy": {
                    "binding_mode": "contract",
                    "revalidation": "toolset_revision",
                    "completion": "ack",
                    "limits": { "max_calls": 10, "max_in_flight": 4 },
                    "retention": "full_in_state",
                    "approvals": "park",
                },
                "bindings": {},
            },
            "closed": closed,
            "cancel_requested": false,
            "calls": {
                "call-1": {
                    "spec": {
                        "origin": { "execution_id": "wtx:scope", "request_id": "call-1" },
                        "binding_id": format!("binding:sha256:{:064x}", 1),
                        "tool_id": "time.sleep",
                        "tool_name": "time_sleep",
                        "arguments_ref": format!("sha256:{:064x}", 1),
                    },
                    "call_id": "code-tool:call-1",
                    "promise_id_base": 1,
                    "status": call_status,
                },
            },
            "completed": [],
        }))
        .expect("scope")
    }

    fn completed_call() -> Value {
        json!({
            "kind": "completed",
            "result": {
                "call_id": "code-tool:call-1",
                "status": "succeeded",
                "output_ref": null,
                "error_ref": null,
            },
        })
    }

    fn completed_run(run_id: u64) -> crate::RunRecord {
        serde_json::from_value(json!({
            "run_id": run_id,
            "status": "completed",
            "submission_id": format!("submit-{run_id}"),
            "source": { "type": "input", "input": [] },
            "first_seq": run_id * 10,
            "terminal_seq": run_id * 10 + 9,
            "accepted_at_ms": 0,
            "completed_at_ms": 0,
            "output": null,
            "failure": null,
        }))
        .expect("run record")
    }

    fn active_run(run_id: u64) -> crate::ActiveRun {
        serde_json::from_value(json!({
            "context_recovery": { "attempts": 0, "recovered_turn_id": null },
            "run_id": run_id,
            "status": "active",
            "submission_id": null,
            "source": { "type": "input", "input": [] },
            "input_entry_ids": [],
            "input_consumed_by_turn_id": null,
            "run_config": {},
            "config_revision": 0,
            "first_seq": 2000,
            "accepted_at_ms": 0,
            "steering": [],
            "turns": {},
            "active_turn_id": null,
            "active_tool_batch_id": null,
            "tool_batches": {},
            "completed_tool_batches": {},
            "output": null,
            "failure": null,
        }))
        .expect("active run")
    }

    /// Runs 1 to 150 have ended; run 151 is active.
    fn session() -> CoreAgentState {
        let mut state = CoreAgentState::new();
        state.runs.completed = (1..=150).map(completed_run).collect();
        state.runs.active = Some(active_run(151));
        for promise in [
            promise(1, run_scope(1), "resolved"),
            promise(2, run_scope(1), "pending"),
            promise(3, json!({ "kind": "session" }), "resolved"),
            promise(4, run_scope(151), "resolved"),
            promise(5, run_scope(2), "resolved"),
            promise(6, run_scope(2), "pending"),
        ] {
            state
                .promises
                .promises
                .insert(promise.promise_id.clone(), promise);
        }
        state.id_cursors.last_promise_id = 6;
        let tools = &mut state.workflow_tools;
        for invocation in [
            // Ended run, settled: left out, with its start failure.
            invocation(10, 1, &[("reply", 1)]),
            // Ended run, one promise still pending: kept, both promises too.
            invocation(11, 2, &[("a", 5), ("b", 6)]),
            // The active run's own call.
            invocation(12, 151, &[("reply", 4)]),
            // Ended run, parent of a code-mode scope that is still open.
            invocation(13, 3, &[]),
        ] {
            tools
                .start_requests
                .insert(invocation.invocation_id.clone(), invocation);
        }
        // A notification from an ended run, with its delivery failure.
        let notified = invocation(14, 3, &[]);
        tools.delivery_failures.insert(
            notified.invocation_id.clone(),
            crate::BlobRef::from_bytes(b"undelivered"),
        );
        tools
            .emissions
            .insert(notified.invocation_id.clone(), notified);
        tools.start_failures.insert(
            crate::WorkflowToolInvocationId::try_new(invocation_id(10)).expect("id"),
            crate::BlobRef::from_bytes(b"refused"),
        );
        state.code_tools.scopes = [
            ("ended", scope(run_scope(1), true, completed_call())),
            ("active", scope(run_scope(151), true, completed_call())),
            (
                "open",
                scope(
                    json!({ "kind": "model_invocation", "parent_invocation_id": invocation_id(13) }),
                    false,
                    completed_call(),
                ),
            ),
            (
                "waiting",
                scope(run_scope(4), true, json!({ "kind": "pending" })),
            ),
        ]
        .into_iter()
        .map(|(id, scope)| (id.to_owned(), scope))
        .collect();
        state
    }

    fn promise_numbers(state: &CoreAgentState) -> Vec<u64> {
        state
            .promises
            .promises
            .keys()
            .map(PromiseId::number)
            .collect()
    }

    fn invocation_numbers(
        invocations: &std::collections::BTreeMap<
            crate::WorkflowToolInvocationId,
            WorkflowToolInvocation,
        >,
    ) -> Vec<String> {
        invocations
            .keys()
            .map(|id| id.as_str().to_owned())
            .collect()
    }

    #[test]
    fn a_handoff_keeps_live_runs_and_work_in_progress_and_leaves_finished_work_behind() {
        let mut state = session();
        trim_for_handoff(&mut state);

        assert_eq!(
            state
                .code_tools
                .scopes
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["active", "open", "waiting"],
            "an ended run's settled scope goes; a live run's, an open one and one with a call in progress stay"
        );
        assert_eq!(
            invocation_numbers(&state.workflow_tools.start_requests),
            [invocation_id(11), invocation_id(12), invocation_id(13)],
            "a settled call of an ended run goes; a pending one, the active run's and an open scope's parent stay"
        );
        assert!(state.workflow_tools.emissions.is_empty());
        assert!(state.workflow_tools.delivery_failures.is_empty());
        assert!(state.workflow_tools.start_failures.is_empty());
        assert_eq!(
            promise_numbers(&state),
            [2, 3, 4, 5, 6],
            "only promise 1, settled in an ended run with no kept invocation, goes"
        );
        assert_eq!(state.runs.completed.len(), HANDOFF_COMPLETED_RUNS);
        assert_eq!(
            state.runs.completed.first().map(|run| run.run_id.as_u64()),
            Some(51),
            "the newest completed runs stay"
        );
        assert_eq!(
            state.runs.active.as_ref().map(|run| run.run_id.as_u64()),
            Some(151)
        );
        assert_eq!(state.id_cursors.last_promise_id, 6);
    }

    #[test]
    fn a_handoff_trims_what_a_reload_would_and_nothing_twice() {
        let mut once = session();
        trim_for_handoff(&mut once);
        let mut twice = once.clone();
        trim_for_handoff(&mut twice);
        assert_eq!(once, twice);
    }
}
