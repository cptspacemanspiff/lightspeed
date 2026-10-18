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

use crate::{
    CallContext, CoreAgentState, PromiseId, PromiseOwnership, PromiseScope, RunId, ScopeOwner,
    ToolCallId, WorkflowToolInvocation,
};

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
///   open, a call has no outcome yet, or a withdrawn handle is pending. A kept
///   scope leaves out the finished calls its program acknowledged
///   (`CodeToolScope::leave_out_acknowledged`): the program never asks for
///   them again, and the scope refuses them if they are sent again.
/// - A workflow-tool invocation stays while its run is live, a kept scope
///   names it as its parent, or one of its completion promises is pending. Its
///   delivery or start failure stays with it.
/// - When `counts_dropped_emissions`, a live run's invocation made by a call
///   left out goes too, with its completion promises, if they all settled and
///   belong to the runtime (a joined call's reply, which only that call
///   waited on). `WorkflowToolState::emissions_dropped` counts it for the
///   per-run cap. A model-owned promise (a handle the program holds, as from
///   `agent.spawn`) may still be awaited, so its invocation stays.
/// - A promise stays while it is pending, detached (any later run may await a
///   session-scoped promise), scoped to a live run, or a completion promise of
///   a kept invocation.
/// - The newest `HANDOFF_COMPLETED_RUNS` completed runs stay.
///
/// Nothing left out is asked for again: admission treats a late delivery or
/// cancel for an issued promise it no longer holds as a no-op, and an
/// execution sends the cancels it owes settled starts before it hands over.
///
/// `counts_dropped_emissions` says the workflow loading the result reads
/// `emissions_dropped`. Code from before it would count only the invocations
/// it holds, admit calls past the cap, and append events the full log then
/// refuses to replay.
pub fn trim_for_handoff(state: &mut CoreAgentState, counts_dropped_emissions: bool) {
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
    let mut left_out = BTreeSet::<ToolCallId>::new();
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
        if keep {
            left_out.extend(
                scope
                    .leave_out_acknowledged()
                    .into_iter()
                    .map(|call| call.call_id),
            );
        }
        if keep
            && let ScopeOwner::ModelInvocation {
                parent_invocation_id,
            } = &scope.spec.owner
        {
            parents.insert(parent_invocation_id.clone());
        }
        keep
    });

    // A live run's settled invocations whose calls were left out, and their
    // runtime-owned completion promises.
    let settled_by_runtime = |promise_id: &PromiseId| {
        promises.promises.get(promise_id).is_some_and(|promise| {
            promise.status.is_terminal() && promise.ownership == PromiseOwnership::Runtime
        })
    };
    let made_by_left_out_call = |invocation: &WorkflowToolInvocation| {
        counts_dropped_emissions
            && live_runs.contains(&invocation.run_id)
            && matches!(&invocation.context, CallContext::Scope { tool_call_id }
                if left_out.contains(tool_call_id))
            && invocation
                .completion_promises
                .as_ref()
                .is_none_or(|keyed| keyed.values().all(settled_by_runtime))
    };
    let mut dropped_promises = BTreeSet::new();
    for invocations in [
        &mut workflow_tools.emissions,
        &mut workflow_tools.start_requests,
    ] {
        invocations.retain(|_, invocation| {
            if !made_by_left_out_call(invocation) {
                return true;
            }
            *workflow_tools
                .emissions_dropped
                .entry(invocation.run_id)
                .or_default()
                .entry(invocation.tool_id.clone())
                .or_default() += 1;
            dropped_promises.extend(
                invocation
                    .completion_promises
                    .iter()
                    .flat_map(|keyed| keyed.values().cloned()),
            );
            false
        });
    }

    // Only a live run can emit again.
    workflow_tools
        .emissions_dropped
        .retain(|run_id, _| live_runs.contains(run_id));

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
            || (!dropped_promises.contains(promise_id)
                && match promise.scope {
                    PromiseScope::Session => true,
                    PromiseScope::Run { run_id } => live_runs.contains(&run_id),
                })
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
        trim_for_handoff(&mut state, true);

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

    /// The active run's program scope: calls 1 to 3 finished in the order
    /// 2, 1, 3, call 4 is still running, and the program acknowledged
    /// `acknowledged` completions.
    fn acknowledged_scope(acknowledged: u64) -> crate::CodeToolScope {
        let mut scope = scope(run_scope(151), false, completed_call());
        let call = scope.calls["call-1"].clone();
        for (number, status) in [
            (2, completed_call()),
            (3, completed_call()),
            (4, json!({ "kind": "pending" })),
        ] {
            let mut call = call.clone();
            call.spec.origin.request_id = format!("call-{number}");
            call.call_id = ToolCallId::new(format!("code-tool:call-{number}"));
            call.status = serde_json::from_value(status).expect("status");
            scope.calls.insert(format!("call-{number}"), call);
        }
        scope.completed = ["call-2", "call-1", "call-3"].map(str::to_owned).to_vec();
        scope.cancel_requested_calls.insert("call-2".to_owned());
        scope.acknowledged = acknowledged;
        scope
    }

    #[test]
    fn a_kept_scope_leaves_out_the_finished_calls_its_program_acknowledged() {
        let mut state = session();
        state
            .code_tools
            .scopes
            .insert("program".to_owned(), acknowledged_scope(2));
        trim_for_handoff(&mut state, true);

        let scope = &state.code_tools.scopes["program"];
        assert_eq!(
            scope.calls.keys().map(String::as_str).collect::<Vec<_>>(),
            ["call-3", "call-4"],
            "the first two completions go; the third, unacknowledged, and the running call stay"
        );
        assert_eq!(scope.completed, ["call-3"]);
        assert!(scope.cancel_requested_calls.is_empty());
        assert_eq!(
            (
                scope.admitted(),
                scope.recorded(),
                scope.dropped_max_request
            ),
            (4, 3, 2),
            "counts stay absolute"
        );
        assert!(scope.recorded_outcomes(1, 10).is_none());
        assert_eq!(
            scope
                .recorded_outcomes(2, 10)
                .expect("held")
                .iter()
                .map(|call| call.spec.origin.request_id.as_str())
                .collect::<Vec<_>>(),
            ["call-3"]
        );
        assert!(scope.left_out("call-1") && scope.left_out("call-2"));
        assert!(!scope.left_out("call-3") && !scope.left_out("call-5"));

        // Without an acknowledgement, nothing is left out.
        let mut state = session();
        state
            .code_tools
            .scopes
            .insert("program".to_owned(), acknowledged_scope(0));
        trim_for_handoff(&mut state, true);
        assert_eq!(state.code_tools.scopes["program"], acknowledged_scope(0));
    }

    #[test]
    fn a_live_runs_workflow_calls_left_out_go_with_their_replies_and_still_count() {
        let session = || {
            let mut state = session();
            state
                .code_tools
                .scopes
                .insert("program".to_owned(), acknowledged_scope(2));
            for (n, ownership) in [(7, "runtime"), (8, "model"), (9, "runtime")] {
                let mut promise = promise(n, run_scope(151), "resolved");
                promise.ownership = serde_json::from_value(json!(ownership)).expect("ownership");
                state
                    .promises
                    .promises
                    .insert(promise.promise_id.clone(), promise);
            }
            state.id_cursors.last_promise_id = 9;
            // Calls 1 and 2 are left out, call 3 is not acknowledged. Call 1
            // joined its child (a runtime-owned reply); call 2 spawned one
            // (the program holds a model-owned handle).
            for (n, call, promise) in [(20, 1, 7), (21, 2, 8), (22, 3, 9)] {
                let mut invocation = invocation(n, 151, &[("reply", promise)]);
                invocation.context = CallContext::Scope {
                    tool_call_id: ToolCallId::new(format!("code-tool:call-{call}")),
                };
                state
                    .workflow_tools
                    .start_requests
                    .insert(invocation.invocation_id.clone(), invocation);
            }
            state
        };
        let tool = crate::WorkflowToolId::new("subagent-run");
        let run = RunId::new(151);
        let mut state = session();
        let counted = state.workflow_tools.emission_count(run, &tool);
        assert_eq!(counted, 4, "the active run's own call and calls 1 to 3");
        trim_for_handoff(&mut state, true);

        assert_eq!(
            invocation_numbers(&state.workflow_tools.start_requests),
            [
                invocation_id(11),
                invocation_id(12),
                invocation_id(13),
                invocation_id(21),
                invocation_id(22)
            ],
            "of the program's calls, only call 1's invocation goes"
        );
        assert_eq!(promise_numbers(&state), [2, 3, 4, 5, 6, 8, 9]);
        assert_eq!(
            state.workflow_tools.emissions_dropped,
            [(run, [(tool.clone(), 1)].into())].into()
        );
        assert_eq!(state.workflow_tools.emission_count(run, &tool), counted);
        // The count crosses the activity boundary with the state.
        let carried: crate::WorkflowToolState =
            serde_json::from_value(serde_json::to_value(&state.workflow_tools).unwrap()).unwrap();
        assert_eq!(carried, state.workflow_tools);

        // A workflow that does not read the count gets the invocation.
        let mut state = session();
        trim_for_handoff(&mut state, false);
        assert!(
            state.workflow_tools.start_requests.contains_key(
                &crate::WorkflowToolInvocationId::try_new(invocation_id(20)).unwrap()
            )
        );
        assert!(state.workflow_tools.emissions_dropped.is_empty());
        assert_eq!(promise_numbers(&state), [2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn only_program_request_ids_are_numbered() {
        assert_eq!(crate::program_request_number("call-12"), Some(12));
        for id in ["call-0", "call-012", "call-", "call-x", "12", "code-1"] {
            assert_eq!(crate::program_request_number(id), None, "{id}");
        }
    }

    #[test]
    fn a_handoff_trims_what_a_reload_would_and_nothing_twice() {
        let mut once = session();
        once.code_tools
            .scopes
            .insert("program".to_owned(), acknowledged_scope(2));
        trim_for_handoff(&mut once, true);
        let mut twice = once.clone();
        trim_for_handoff(&mut twice, true);
        assert_eq!(once, twice);
    }
}
