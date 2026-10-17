use super::*;

use serde_json::json;
use std::collections::HashSet;

fn program_input(source: &str, bindings: &[&str], seed: [u32; 4]) -> ExecutionInput {
    ExecutionInput {
        source: source.into(),
        bindings: bindings
            .iter()
            .map(|name| ToolBinding {
                name: (*name).into(),
                binding_id: format!("opaque-{name}"),
            })
            .collect(),
        limits: ExecutionLimits {
            max_memory_bytes: 16 * 1024 * 1024,
            max_stack_bytes: 256 * 1024,
            max_source_bytes: 64 * 1024,
            max_catalog_bytes: 64 * 1024,
            max_request_bytes: 16 * 1024,
            max_result_bytes: 16 * 1024,
            max_output_bytes: 16 * 1024,
            max_tool_calls: 32,
            max_outstanding_tool_calls: 8,
            max_compute: 1_000_000,
        },
        program: ProgramExecution {
            input: json!({"claim": "x"}),
            started_at_ms: 1_000,
            random_seed: seed,
            state: None,
            continued: false,
            clock_ms: None,
            calls_used: 0,
            compute_used: 0,
            should_continue: false,
        },
    }
}

/// Step a program to its end, answering every request it issues with the
/// completions `respond` gives for it (none holds the request), delivered
/// in issue order with the step after it was issued.
fn drive(
    input: ExecutionInput,
    mut respond: impl FnMut(&HostRequest) -> Vec<HostCompletion>,
) -> (Vec<HostRequest>, ExecutionReport) {
    let (mut instance, mut step) = ProgramInstance::start(input, StepBudget::default()).unwrap();
    let mut requests = Vec::new();
    loop {
        let (issued, finished) = match step {
            Step::Waiting { requests, .. } => (requests, None),
            Step::Finished {
                requests, report, ..
            } => (requests, Some(report)),
        };
        let ready = issued.iter().flat_map(&mut respond).collect::<Vec<_>>();
        requests.extend(issued);
        if let Some(report) = finished {
            return (requests, report);
        }
        assert!(
            !ready.is_empty(),
            "the program waits with nothing to deliver"
        );
        step = instance.step(ready);
    }
}

/// Drive a program, answering each request with the outcome and
/// completion time `complete` gives it.
fn run_program(
    input: ExecutionInput,
    mut complete: impl FnMut(&HostRequest) -> (Result<Value, HostError>, Option<u64>),
) -> (Vec<HostRequest>, ExecutionReport) {
    drive(input, |request| {
        let (outcome, completed_at_ms) = complete(request);
        vec![HostCompletion {
            request_id: request.request_id.clone(),
            outcome,
            completed_at_ms,
        }]
    })
}

const SEED: [u32; 4] = [1, 2, 3, 4];

#[test]
fn programs_run_a_module_handler_with_contract_namespaces() {
    let source = r#"
        export default {
            async run(input, env, ctx) {
                const page = await env.web.fetch({ url: "https://example.test" });
                const file = await env.vfs.read_file({ path: "/a" });
                return {
                    claim: input.claim,
                    startedAt: ctx.startedAt,
                    families: Object.keys(env).sort(),
                    frozen: Object.isFrozen(env) && Object.isFrozen(env.vfs),
                    results: [page, file],
                    scriptGlobals: [typeof tools, typeof text, typeof media],
                };
            }
        };
    "#;
    let (requests, report) = run_program(
        program_input(source, &["web.fetch", "vfs.read_file"], SEED),
        |request| (Ok(json!(request.binding_id)), None),
    );
    assert_eq!(report.error, None);
    assert_eq!(
        requests
            .iter()
            .map(|request| request.binding_id.as_str())
            .collect::<Vec<_>>(),
        ["opaque-web.fetch", "opaque-vfs.read_file"]
    );
    assert_eq!(
        report.return_value,
        Some(json!({
            "claim": "x",
            "startedAt": 1_000,
            "families": ["vfs", "web"],
            "frozen": true,
            "results": ["opaque-web.fetch", "opaque-vfs.read_file"],
            "scriptGlobals": ["undefined", "undefined", "undefined"],
        }))
    );
}

#[test]
fn a_program_reads_its_state_and_hands_the_next_one_over_by_returning_complete() {
    let source = r#"
        export default {
            async run(input, env, ctx) {
                const before = ctx.state;
                const next = { round: before.round + 1 };
                const errors = [];
                for (const value of [undefined, () => 1, "x".repeat(70000)]) {
                    try { ctx.complete(null, value); } catch (error) { errors.push(error.name); }
                }
                const done = ctx.complete({ before, errors, continued: ctx.continued }, next);
                next.round = 99;
                return done;
            }
        };
    "#;
    let mut input = program_input(source, &[], SEED);
    input.program.state = Some(json!({"round": 1}));
    let (_, report) = run_program(input, |_| unreachable!());
    assert_eq!(report.error, None);
    assert_eq!(
        report.return_value,
        Some(json!({
            "before": {"round": 1},
            "errors": ["TypeError", "TypeError", "RangeError"],
            "continued": false,
        }))
    );
    assert_eq!(report.state, Some(json!({"round": 2})));
    assert_eq!(report.exit, Some(ProgramExit::Complete));

    // A plain return hands nothing over; a run without state reads none.
    let source = r#"
        export default {
            async run(input, env, ctx) { return [ctx.state === undefined, Object.isFrozen(ctx)]; }
        };
    "#;
    let (_, report) = run_program(program_input(source, &[], SEED), |_| unreachable!());
    assert_eq!(report.return_value, Some(json!([true, true])));
    assert_eq!(report.state, None);
}

#[test]
fn complete_takes_effect_only_when_run_returns_it_once() {
    for (source, message) in [
        (
            "export default { async run(i, e, ctx) { ctx.complete(1, {}); return 1; } };",
            "ctx.complete() was called but run did not return its result",
        ),
        (
            "export default { async run(i, e, ctx) { ctx.complete(1, {}); return ctx.complete(2, {}); } };",
            "ctx.complete() or ctx.continueAsNew() was called more than once",
        ),
        (
            "export default { async run(i, e, ctx) { const done = ctx.complete(1, {}); throw new Error('later'); } };",
            "later",
        ),
    ] {
        let (_, report) = run_program(program_input(source, &[], SEED), |_| unreachable!());
        let error = report.error.expect("the run fails");
        assert_eq!(error.kind, ExecutionErrorKind::Javascript);
        assert!(error.message.contains(message), "{}", error.message);
        assert_eq!(report.state, None);
        assert_eq!(report.return_value, None);
    }

    let source = "export default { async run(i, e, ctx) { ctx.continueAsNew({}); return ctx.complete(1, {}); } };";
    let (_, report) = run_program(program_input(source, &[], SEED), |_| unreachable!());
    assert!(
        report
            .error
            .unwrap()
            .message
            .contains("called more than once")
    );
    assert_eq!((report.exit, report.state), (None, None));

    // A result is optional; the state is not.
    let source =
        "export default { async run(i, e, ctx) { return ctx.complete(undefined, null); } };";
    let (_, report) = run_program(program_input(source, &[], SEED), |_| unreachable!());
    assert_eq!(report.error, None);
    assert_eq!(report.return_value, None);
    assert_eq!(report.state, Some(Value::Null));
}

#[test]
fn continue_as_new_hands_its_state_to_the_next_execution_which_keeps_the_clock() {
    let source = r#"
        export default {
            async run(input, env, ctx) {
                const seen = { continued: ctx.continued, state: ctx.state ?? null, now: Date.now() };
                await env.web.fetch({ url: "https://example.test" });
                if (ctx.continued) return { ...seen, after: Date.now() };
                return ctx.continueAsNew({ i: 1, seen });
            }
        };
    "#;
    let complete_at = |at| move |_: &HostRequest| (Ok(Value::Null), Some(at));
    let (_, first) = run_program(
        program_input(source, &["web.fetch"], SEED),
        complete_at(5_000),
    );
    assert_eq!(first.error, None);
    assert_eq!(first.exit, Some(ProgramExit::ContinueAsNew));
    assert_eq!(first.return_value, None);
    assert_eq!(first.clock_ms, 5_000);
    assert_eq!(
        first.state,
        Some(json!({"i": 1, "seen": {"continued": false, "state": null, "now": 1_000}}))
    );

    let mut input = program_input(source, &["web.fetch"], SEED);
    input.program.state = first.state.clone();
    input.program.continued = true;
    input.program.clock_ms = Some(first.clock_ms);
    // A completion stamped before the carried clock does not move it back.
    let (_, second) = run_program(input, complete_at(4_000));
    assert_eq!(second.error, None);
    assert_eq!((second.exit, second.state), (None, None));
    assert_eq!(second.clock_ms, 5_000);
    assert_eq!(
        second.return_value,
        Some(json!({
            "continued": true,
            "state": {"i": 1, "seen": {"continued": false, "state": null, "now": 1_000}},
            "now": 5_000,
            "after": 5_000,
        }))
    );
}

/// Fetches until `ctx.shouldContinue()` says to stop, noting what it said
/// at each point.
const CHECKS_SHOULD_CONTINUE: &str = r#"
    export default {
        async run(input, env, ctx) {
            const seen = [ctx.shouldContinue()];
            while (!ctx.shouldContinue()) {
                await env.web.fetch({ url: "https://example.test" });
                seen.push(ctx.shouldContinue());
            }
            return ctx.continueAsNew({ seen });
        }
    };
"#;

#[test]
fn should_continue_reports_what_the_host_set_for_each_step() {
    let (mut instance, step) = ProgramInstance::start(
        program_input(CHECKS_SHOULD_CONTINUE, &["web.fetch"], SEED),
        StepBudget::default(),
    )
    .unwrap();
    let (requests, _) = waiting(step);
    assert_eq!(requests.len(), 1);
    let step = instance.step(vec![completed("call-1", Ok(Value::Null))]);
    let (requests, _) = waiting(step);
    assert_eq!(requests[0].request_id, "call-2");
    instance.set_should_continue(true);
    let step = instance.step(vec![completed("call-2", Ok(Value::Null))]);
    let (_, report) = finished(step);
    assert_eq!(report.error, None);
    assert_eq!(report.exit, Some(ProgramExit::ContinueAsNew));
    assert_eq!(report.state, Some(json!({"seen": [false, false, true]})));

    // An execution can start with it set: this one continues at once.
    let mut input = program_input(CHECKS_SHOULD_CONTINUE, &["web.fetch"], SEED);
    input.program.should_continue = true;
    let (_, step) = ProgramInstance::start(input, StepBudget::default()).unwrap();
    let (_, report) = finished(step);
    assert_eq!(report.state, Some(json!({"seen": [true]})));
}

#[test]
fn a_host_can_fail_a_waiting_program_and_keep_what_it_used() {
    let (mut instance, step) = ProgramInstance::start(
        program_input(CHECKS_SHOULD_CONTINUE, &["web.fetch"], SEED),
        StepBudget::default(),
    )
    .unwrap();
    waiting(step);
    waiting(instance.step(vec![completed("call-1", Ok(Value::Null))]));
    let (cancels, report) = finished(instance.fail(ExecutionError {
        kind: ExecutionErrorKind::HistoryExhausted,
        message: "history is nearly full".into(),
    }));
    assert!(cancels.is_empty());
    assert_eq!(
        terminal_error(&report),
        &ExecutionErrorKind::HistoryExhausted
    );
    assert_eq!(
        (report.return_value, report.exit, report.state),
        (None, None, None)
    );
    assert_eq!(report.metrics.tool_calls, 2);
    assert_eq!(report.pending_request_ids, ["call-2"]);
    // A finished program keeps its report.
    let (_, again) = finished(instance.step(Vec::new()));
    assert_eq!(
        terminal_error(&again),
        &ExecutionErrorKind::HistoryExhausted
    );
}

#[test]
fn a_program_needs_a_default_export_with_run_and_compiles_before_effects() {
    for source in [
        "export const run = async () => 1;",
        "export default { run: 1 };",
    ] {
        let (_, report) = run_program(program_input(source, &[], SEED), |_| {
            (Ok(Value::Null), None)
        });
        assert_eq!(
            report.error.unwrap().kind,
            ExecutionErrorKind::Javascript,
            "{source}"
        );
    }
    let (requests, report) = run_program(
        program_input(
            "setTimeout(() => {}, 1); export default { async run() { return 1 } ",
            &["time.sleep"],
            SEED,
        ),
        |_| (Ok(Value::Null), None),
    );
    assert!(requests.is_empty());
    assert_eq!(report.error.unwrap().kind, ExecutionErrorKind::Javascript);
    let (_, report) = run_program(
        program_input(
            r#"import fs from "fs"; export default { async run() { return 1 } };"#,
            &[],
            SEED,
        ),
        |_| (Ok(Value::Null), None),
    );
    assert!(report.error.is_some());
}

#[test]
fn the_clock_is_the_recorded_start_and_advances_only_with_completions() {
    let source = r#"
        export default {
            async run(input, env) {
                const seen = [Date.now(), new Date().getTime(), performance.now()];
                await env.test.call({});
                seen.push(Date.now());
                // An earlier timestamp never moves the clock back.
                await env.test.call({});
                seen.push(Date.now(), performance.now());
                const local = new Date(2026, 0, 2, 3, 4, 5);
                return {
                    seen,
                    offset: local.getTimezoneOffset(),
                    local: local.getTime() === Date.UTC(2026, 0, 2, 3, 4, 5),
                    hours: local.getHours(),
                    parsed: Date.parse("2026-01-02T03:04:05") === Date.UTC(2026, 0, 2, 3, 4, 5),
                    zoned: Date.parse("2026-01-02T03:04:05+01:00") === Date.UTC(2026, 0, 2, 2, 4, 5),
                    text: local.toString(),
                    called: typeof Date(),
                    subclass: (class Stamp extends Date {}).prototype instanceof Date,
                };
            }
        };
    "#;
    let mut times = [5_000, 4_000].into_iter();
    let (_, report) = run_program(program_input(source, &["test.call"], SEED), |_| {
        (Ok(Value::Null), times.next())
    });
    assert_eq!(report.error, None);
    assert_eq!(
        report.return_value,
        Some(json!({
            "seen": [1_000, 1_000, 0, 5_000, 5_000, 4_000],
            "offset": 0,
            "local": true,
            "hours": 3,
            "parsed": true,
            "zoned": true,
            "text": "Fri Jan 02 2026 03:04:05 GMT+0000 (Coordinated Universal Time)",
            "called": "string",
            "subclass": true,
        }))
    );
}

#[test]
fn randomness_is_drawn_from_the_execution_seed() {
    let source = r#"
        export default {
            async run() {
                const bytes = crypto.getRandomValues(new Uint8Array(6));
                return {
                    random: [Math.random(), Math.random()],
                    bytes: Array.from(bytes),
                    uuid: crypto.randomUUID(),
                };
            }
        };
    "#;
    let draw = |seed| {
        let (_, report) = run_program(program_input(source, &[], seed), |_| {
            (Ok(Value::Null), None)
        });
        assert_eq!(report.error, None);
        report.return_value.unwrap()
    };
    let first = draw(SEED);
    assert_eq!(first, draw(SEED));
    assert_ne!(first, draw([4, 3, 2, 1]));
    let uuid = first["uuid"].as_str().unwrap();
    assert_eq!(uuid.len(), 36);
    assert_eq!(&uuid[14..15], "4");
    assert!(first["random"][0].as_f64().unwrap() < 1.0);
}

#[test]
fn programs_cannot_generate_code_from_strings() {
    let source = r#"
        export default {
            async run() {
                const refused = [];
                for (const attempt of [
                    () => eval("1"),
                    () => (0, eval)("1"),
                    () => new Function("return 1"),
                    () => Function("return 1"),
                    () => (function () {}).constructor("return 1"),
                    () => (async function () {}).constructor("return 1"),
                    () => (function* () {}).constructor("yield 1"),
                    () => (async function* () {}).constructor("yield 1"),
                ]) {
                    try { attempt(); refused.push(false); } catch { refused.push(true); }
                }
                return { refused, instance: (() => {}) instanceof Function };
            }
        };
    "#;
    let (_, report) = run_program(program_input(source, &[], SEED), |_| {
        (Ok(Value::Null), None)
    });
    assert_eq!(report.error, None);
    assert_eq!(
        report.return_value,
        Some(
            json!({"refused": [true, true, true, true, true, true, true, true], "instance": true})
        )
    );
}

/// A durable sleep as the host records it: one call that completes with its
/// deadline. The recorded completion time here is earlier than the deadline,
/// so the clock's clamp to the deadline is visible.
fn sleep_outcome(request: &HostRequest) -> (Result<Value, HostError>, Option<u64>) {
    match request.arguments["ms"].as_u64() {
        Some(ms) => (Ok(json!({"fire_at_ms": 1_000 + ms})), Some(1_000)),
        None => (
            Err(HostError {
                kind: "invalid_arguments".into(),
                message: "ms must be a non-negative integer".into(),
                value: None,
            }),
            Some(1_000),
        ),
    }
}

#[test]
fn timers_are_durable_sleeps_through_the_time_sleep_contract() {
    let source = r#"
        export default {
            async run(input, env) {
                const order = [];
                const cleared = setTimeout(() => order.push("cleared"), 5);
                clearTimeout(cleared);
                await new Promise(resolve => setTimeout(value => { order.push(value); resolve(); }, 20, "fired"));
                const slept = await env.time.sleep({ ms: 30 });
                let rejected = false;
                try { await env.time.sleep({ ms: -1 }); } catch { rejected = true; }
                return { order, slept, now: Date.now(), rejected };
            }
        };
    "#;
    let (requests, report) =
        run_program(program_input(source, &["time.sleep"], SEED), sleep_outcome);
    assert_eq!(report.error, None);
    // Each wait, timer or explicit sleep, is exactly one recorded call. A
    // timer cleared in the step that set it never sends its sleep.
    assert!(
        requests
            .iter()
            .all(|request| request.binding_id == "opaque-time.sleep")
    );
    assert_eq!(
        requests
            .iter()
            .map(|request| request.arguments.clone())
            .collect::<Vec<_>>(),
        [json!({"ms": 20}), json!({"ms": 30}), json!({"ms": -1})]
    );
    assert_eq!(
        report.return_value,
        Some(json!({
            "order": ["fired"],
            "slept": {"fire_at_ms": 1_030},
            // The sleep was recorded at 1_000; the clock is clamped to its
            // later deadline.
            "now": 1_030,
            "rejected": true,
        }))
    );
}

#[test]
fn a_throwing_timer_fails_the_program_and_timers_need_the_contract() {
    let source = r#"
        export default {
            async run() {
                setTimeout(() => { throw new Error("timer failed"); }, 1);
                return await new Promise(() => {});
            }
        };
    "#;
    let (_, report) = run_program(program_input(source, &["time.sleep"], SEED), sleep_outcome);
    assert_eq!(report.error.unwrap().message, "timer failed");

    let (_, report) = run_program(
        program_input(
            "export default { async run() { setTimeout(() => {}, 1); } };",
            &[],
            SEED,
        ),
        |_| (Ok(Value::Null), None),
    );
    assert!(
        report
            .error
            .unwrap()
            .message
            .contains("time.sleep contract")
    );
}

#[test]
fn module_code_cannot_tamper_with_env_construction() {
    let source = r#"
        String.prototype.split = () => ["evil"];
        Array.prototype.slice = () => { throw new Error("tampered"); };
        export default {
            async run(input, env) {
                await new Promise(resolve => setTimeout(resolve, 1, "a"));
                return Object.keys(env);
            }
        };
    "#;
    let (_, report) = run_program(
        program_input(source, &["web.fetch", "time.sleep"], SEED),
        sleep_outcome,
    );
    assert_eq!(report.error, None);
    assert_eq!(report.return_value, Some(json!(["web", "time"])));
}

const INPUT_CONTRACTS: &[&str] = &[
    "input.open",
    "input.remind",
    "input.reassign",
    "promise.await",
    "promise.cancel",
    "time.sleep",
];

fn opened() -> Value {
    json!({
        "accepted": true,
        "invocationId": "wti:1",
        "executionId": "wtx:sha256:abc",
        "promise": "promise_3",
    })
}

fn control(status: &str) -> Value {
    json!({"request_id": "inp_abc", "status": status, "assignees": ["bob"], "reminder_count": 1})
}

#[test]
fn an_open_input_request_is_a_handle_over_ordinary_calls() {
    let source = r#"
        export default {
            async run(input, env) {
                const ask = await env.input.open({ ask: "Merge?", assignees: ["alice"] });
                const reminded = await ask.remind();
                const moved = await ask.reassign({ to: ["bob"] });
                const answer = await ask.answer();
                const again = await ask.answer();
                await ask.cancel();
                const late = await env.input.open({ ask: "Again?" });
                let failure = null;
                try { await late.answer(); } catch (error) { failure = [error.kind, error.message]; }
                return { id: ask.id, frozen: Object.isFrozen(ask), reminded, moved, answer, same: answer === again, failure };
            }
        };
    "#;
    let mut awaits = 0;
    let (requests, report) = run_program(program_input(source, INPUT_CONTRACTS, SEED), |request| {
        let outcome = match request.binding_id.as_str() {
            "opaque-input.open" => opened(),
            "opaque-input.remind" | "opaque-input.reassign" => control("pending"),
            "opaque-promise.await" => {
                awaits += 1;
                if awaits == 1 {
                    json!({"outcome": "completed", "results": [
                        {"promise_id": "promise_3", "status": "resolved", "output": {"approved": true}}
                    ]})
                } else {
                    json!({"outcome": "completed", "results": [
                        {"promise_id": "promise_3", "status": "failed",
                         "error": {"code": "deadline_exceeded", "message": "too late"}}
                    ]})
                }
            }
            "opaque-promise.cancel" => json!({"promises": []}),
            other => panic!("unexpected call {other}"),
        };
        (Ok(outcome), None)
    });
    assert_eq!(report.error, None);
    let calls = requests
        .iter()
        .map(|request| (request.binding_id.as_str(), request.arguments.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        calls,
        [
            (
                "opaque-input.open",
                json!({"ask": "Merge?", "assignees": ["alice"]})
            ),
            ("opaque-input.remind", json!({"id": "wtx:sha256:abc"})),
            (
                "opaque-input.reassign",
                json!({"id": "wtx:sha256:abc", "to": ["bob"]})
            ),
            // A handle awaits its promise once, however often it is asked.
            ("opaque-promise.await", json!({"promises": ["promise_3"]})),
            // `cancel()` asks the host directly; it is not a call.
            ("opaque-input.open", json!({"ask": "Again?"})),
            ("opaque-promise.await", json!({"promises": ["promise_3"]})),
        ]
    );
    assert_eq!(
        report.return_value,
        Some(json!({
            "id": "wtx:sha256:abc",
            "frozen": true,
            "reminded": control("pending"),
            "moved": control("pending"),
            "answer": {"approved": true},
            "same": true,
            "failure": ["deadline_exceeded", "too late"],
        }))
    );
}

#[test]
fn a_detached_request_is_taken_back_by_its_id() {
    let source = r#"
        export default {
            async run(input, env) {
                const ask = await env.input.open({ ask: "Ship?" });
                const kept = await ask.detach();
                const again = await env.input.handle(ask.id);
                const answer = await again.answer();
                let failure = null;
                try { await env.input.handle("wtx:sha256:other"); } catch (error) { failure = error.message; }
                return { kept, id: again.id, answer, failure };
            }
        };
    "#;
    let contracts = [INPUT_CONTRACTS, &["input.handle", "promise.detach"]].concat();
    let (requests, report) = run_program(program_input(source, &contracts, SEED), |request| {
        let outcome = match (request.binding_id.as_str(), &request.arguments) {
            ("opaque-input.open", _) => opened(),
            ("opaque-promise.detach", _) => {
                json!({"promises": [{"promise_id": "promise_3", "status": "detached"}]})
            }
            ("opaque-input.handle", arguments) if arguments["id"] == "wtx:sha256:abc" => json!({
                "executionId": "wtx:sha256:abc",
                "promise": "promise_3",
                "status": "pending",
            }),
            ("opaque-input.handle", _) => {
                return (
                    Err(HostError {
                        kind: "tool_failed".into(),
                        message: "unknown input request wtx:sha256:other".into(),
                        value: None,
                    }),
                    None,
                );
            }
            ("opaque-promise.await", _) => json!({"outcome": "completed", "results": [
                {"promise_id": "promise_3", "status": "resolved", "output": {"approved": true}}
            ]}),
            (other, _) => panic!("unexpected call {other}"),
        };
        (Ok(outcome), None)
    });
    assert_eq!(report.error, None);
    let calls = requests
        .iter()
        .map(|request| (request.binding_id.as_str(), request.arguments.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        calls,
        [
            ("opaque-input.open", json!({"ask": "Ship?"})),
            ("opaque-promise.detach", json!({"promises": ["promise_3"]})),
            ("opaque-input.handle", json!({"id": "wtx:sha256:abc"})),
            ("opaque-promise.await", json!({"promises": ["promise_3"]})),
            ("opaque-input.handle", json!({"id": "wtx:sha256:other"})),
        ]
    );
    assert_eq!(
        report.return_value,
        Some(json!({
            "kept": {"id": "wtx:sha256:abc"},
            "id": "wtx:sha256:abc",
            "answer": {"approved": true},
            "failure": "unknown input request wtx:sha256:other",
        }))
    );
}

#[test]
fn with_reminders_nudges_on_an_interval_then_reassigns() {
    let source = r#"
        export default {
            async run(input, env) {
                const ask = await env.input.open({ ask: "Merge?" });
                return await ask.withReminders({ every_ms: 60000, max: 2, then: "reassign", to: ["lead"] });
            }
        };
    "#;
    let mut held_await = None;
    let complete = |request_id: String, outcome: Value| HostCompletion {
        request_id,
        outcome: Ok(outcome),
        completed_at_ms: None,
    };
    let (requests, report) = drive(program_input(source, INPUT_CONTRACTS, SEED), |request| {
        let outcome = match request.binding_id.as_str() {
            "opaque-input.open" => opened(),
            // The answer arrives only once the request is reassigned.
            "opaque-promise.await" => {
                held_await = Some(request.request_id.clone());
                return Vec::new();
            }
            "opaque-time.sleep" => json!({"fire_at_ms": 61_000}),
            "opaque-input.remind" => control("pending"),
            "opaque-input.reassign" => {
                return vec![
                    complete(
                        held_await.take().expect("awaiting"),
                        json!({"outcome": "completed", "results": [
                            {"promise_id": "promise_3", "status": "resolved", "output": {"approved": false}}
                        ]}),
                    ),
                    complete(request.request_id.clone(), control("pending")),
                ];
            }
            other => panic!("unexpected call {other}"),
        };
        vec![complete(request.request_id.clone(), outcome)]
    });
    let calls = requests
        .iter()
        .map(|request| request.binding_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(report.error, None);
    assert_eq!(report.return_value, Some(json!({"approved": false})));
    assert_eq!(
        calls,
        [
            "opaque-input.open",
            "opaque-promise.await",
            "opaque-time.sleep",
            "opaque-input.remind",
            "opaque-time.sleep",
            "opaque-input.remind",
            "opaque-time.sleep",
            "opaque-input.reassign",
        ]
    );
}

/// One recorded completion: what the live host fed the program, in the
/// order the program received it.
type Journal = Vec<HostCompletion>;

const CONFORMANCE_PROGRAM: &str = r#"
    export default {
        async run(input, env, ctx) {
            const started = Date.now();
            const fan = [1, 2, 3, 4, 5, 6].map((n) =>
                env.test.call({ n, draw: Math.floor(Math.random() * 1000) }));
            const first = await Promise.race(fan.map((call) =>
                call.then((value) => value.n, (error) => error.message)));
            const settled = await Promise.allSettled(fan);
            const order = [];
            await Promise.all([7, 8, 9].map((n) => env.test.call({ n }).then((value) => {
                order.push(value.n);
                return env.test.call({ n: value.n * 10, after: order.length });
            })));
            for (let i = 0; i < 50; i++) await null;
            return {
                first,
                settled: settled.map((entry) => entry.status),
                order,
                elapsed: Date.now() - started,
                draw: Math.random(),
            };
        },
    };
"#;

fn conformance_outcome(request: &HostRequest) -> Result<Value, HostError> {
    if request.arguments["n"] == json!(4) {
        Err(HostError {
            kind: "tool_failed".into(),
            message: "four fails".into(),
            value: None,
        })
    } else {
        Ok(request.arguments.clone())
    }
}

/// Run the conformance program, delivering its outstanding completions in
/// an order and grouping drawn from `seed`, as a host whose calls finish at
/// arbitrary times would, and record the order delivered.
fn run_shuffled(seed: u64) -> (Vec<HostRequest>, ExecutionReport, Journal) {
    let (mut instance, mut step) = ProgramInstance::start(
        program_input(CONFORMANCE_PROGRAM, &["test.call"], SEED),
        StepBudget::default(),
    )
    .unwrap();
    let mut state = seed.max(1);
    let mut draw = move |below: usize| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % below as u64) as usize
    };
    let mut requests = Vec::new();
    let mut outstanding = Vec::new();
    let mut journal = Journal::new();
    loop {
        let (issued, finished) = match step {
            Step::Waiting { requests, .. } => (requests, None),
            Step::Finished {
                requests, report, ..
            } => (requests, Some(report)),
        };
        for request in &issued {
            outstanding.push((request.request_id.clone(), conformance_outcome(request)));
        }
        requests.extend(issued);
        if let Some(report) = finished {
            return (requests, report, journal);
        }
        let mut batch = Vec::new();
        for _ in 0..=draw(outstanding.len()) {
            let (request_id, outcome) = outstanding.remove(draw(outstanding.len()));
            batch.push(HostCompletion {
                request_id,
                outcome,
                completed_at_ms: Some(2_000 + (journal.len() + batch.len()) as u64),
            });
        }
        journal.extend(batch.iter().cloned());
        step = instance.step(batch);
    }
}

#[test]
fn a_program_makes_the_same_calls_and_returns_the_same_output_from_its_journal() {
    let mut journals = HashSet::new();
    for seed in 1..=24 {
        let (shuffled_requests, shuffled, journal) = run_shuffled(seed);
        assert_eq!(shuffled.error, None, "run {seed}");
        assert_eq!(shuffled_requests.len(), 12);
        journals.insert(
            journal
                .iter()
                .map(|completion| completion.request_id.clone())
                .collect::<Vec<_>>(),
        );
        // Replayed one completion per step, a few at a time or all at once,
        // the program does exactly what it did.
        for group in [1, 3, journal.len()] {
            let (requests, report) = step_through(&journal, group);
            assert_eq!(report.error, None, "run {seed} in groups of {group}");
            assert_eq!(
                requests, shuffled_requests,
                "run {seed} in groups of {group}"
            );
            assert_eq!(report.return_value, shuffled.return_value);
        }
    }
    // The orders must actually differ, or the test proves nothing.
    assert!(
        journals.len() > 4,
        "only {} distinct orders",
        journals.len()
    );
}

#[test]
fn a_recorded_completion_for_an_unissued_request_is_nondeterminism() {
    let source = r#"
        export default {
            async run(input, env) {
                await env.test.call({ n: 1 });
                return "done";
            },
        };
    "#;
    let (mut instance, step) =
        ProgramInstance::start(program_input(source, &["test.call"], SEED), STEP_BUDGET).unwrap();
    assert!(matches!(step, Step::Waiting { requests, .. } if requests.len() == 1));
    let Step::Finished { report, .. } = instance.step(vec![HostCompletion {
        request_id: "call-2".into(),
        outcome: Ok(Value::Null),
        completed_at_ms: None,
    }]) else {
        panic!("a divergent completion ends the program")
    };
    let error = report.error.unwrap();
    assert_eq!(error.kind, ExecutionErrorKind::Nondeterminism);
    assert!(error.message.contains("call-2"), "{}", error.message);
}

const STEP_BUDGET: StepBudget = StepBudget {
    interrupts_per_step: 100,
};

/// Step a program through `journal`, `group` completions per step, and
/// collect the requests it issued and how it finished.
fn step_through(journal: &Journal, group: usize) -> (Vec<HostRequest>, ExecutionReport) {
    let (mut instance, mut step) = ProgramInstance::start(
        program_input(CONFORMANCE_PROGRAM, &["test.call"], SEED),
        STEP_BUDGET,
    )
    .unwrap();
    let mut requests = Vec::new();
    let mut batches = journal.chunks(group.max(1));
    loop {
        match step {
            Step::Waiting {
                requests: issued, ..
            } => {
                requests.extend(issued);
                let batch = batches.next().expect("program waits past its journal");
                step = instance.step(batch.to_vec());
            }
            Step::Finished {
                requests: issued,
                report,
                ..
            } => {
                requests.extend(issued);
                assert!(instance.is_finished());
                assert_eq!(
                    instance.step(Vec::new()),
                    Step::Finished {
                        requests: Vec::new(),
                        cancels: Vec::new(),
                        report: report.clone()
                    }
                );
                return (requests, report);
            }
        }
    }
}

#[test]
fn a_stepped_program_fails_at_the_same_point_when_it_exceeds_its_budget() {
    for source in [
        "export default { run() { for (;;) {} } };",
        "export default { async run() { for (;;) await null; } };",
        "export default { run() { try { for (;;) {} } catch { return 'caught'; } } };",
    ] {
        let run = || {
            let (instance, step) =
                ProgramInstance::start(program_input(source, &[], SEED), STEP_BUDGET).unwrap();
            assert!(instance.is_finished());
            let Step::Finished { report, .. } = step else {
                panic!("a runaway program finishes in its first step")
            };
            report
        };
        let report = run();
        assert_eq!(
            report.error.as_ref().map(|error| &error.kind),
            Some(&ExecutionErrorKind::LimitExceeded),
            "{source}: {report:?}"
        );
        assert_eq!(report.return_value, None);
        assert_eq!(run(), report, "{source}");
    }
}

#[test]
fn a_step_budget_is_per_step() {
    // Each step does about the same work; a budget that covers one step
    // covers them all, however many there are.
    let source = r#"
        export default {
            async run(input, env) {
                let total = 0;
                for (let round = 0; round < 20; round++) {
                    for (let i = 0; i < 20000; i++) total += i % 7;
                    await env.test.call({ round });
                }
                return total;
            },
        };
    "#;
    let (mut instance, mut step) =
        ProgramInstance::start(program_input(source, &["test.call"], SEED), STEP_BUDGET).unwrap();
    let mut rounds = 0;
    while let Step::Waiting { requests, .. } = step {
        let [request] = <[_; 1]>::try_from(requests).unwrap();
        rounds += 1;
        step = instance.step(vec![HostCompletion {
            request_id: request.request_id,
            outcome: Ok(Value::Null),
            completed_at_ms: None,
        }]);
    }
    let Step::Finished { report, .. } = step else {
        unreachable!()
    };
    assert_eq!(report.error, None);
    assert_eq!(rounds, 20);
    assert!(report.return_value.is_some());
}

#[test]
fn stepped_instances_need_a_budget() {
    assert!(matches!(
        ProgramInstance::start(
            program_input("export default { run() {} };", &[], SEED),
            StepBudget {
                interrupts_per_step: 0
            }
        ),
        Err(StartError::InvalidInput(_))
    ));
}

fn terminal_error(report: &ExecutionReport) -> &ExecutionErrorKind {
    &report.error.as_ref().expect("an error").kind
}

#[test]
fn the_memory_limit_ends_the_program() {
    let source =
        "export default { run() { const a = []; for (;;) a.push(new Array(1e5).fill(1)); } };";
    let (_, report) = run_program(program_input(source, &[], SEED), |_| unreachable!());
    assert_eq!(terminal_error(&report), &ExecutionErrorKind::LimitExceeded);
}

#[test]
fn a_duplicate_completion_ends_the_program() {
    let source = r#"
        export default {
            async run(input, env) {
                await env.test.call({ n: 1 });
                await env.test.call({ n: 2 });
            },
        };
    "#;
    let done = |request_id: &str| HostCompletion {
        request_id: request_id.into(),
        outcome: Ok(Value::Null),
        completed_at_ms: None,
    };
    let (mut instance, _) =
        ProgramInstance::start(program_input(source, &["test.call"], SEED), STEP_BUDGET).unwrap();
    assert!(matches!(
        instance.step(vec![done("call-1")]),
        Step::Waiting { .. }
    ));
    // A completion already delivered is a faulty host, not a divergence.
    let Step::Finished { report, .. } = instance.step(vec![done("call-1")]) else {
        panic!("a duplicate completion ends the program")
    };
    assert_eq!(terminal_error(&report), &ExecutionErrorKind::Internal);
    assert_eq!(report.pending_request_ids, ["call-2"]);
}

#[test]
fn an_oversized_result_arrives_as_a_failure_the_program_can_catch() {
    let source = r#"
        export default {
            async run(input, env) {
                try {
                    await env.test.call({});
                } catch (error) {
                    return error.kind;
                }
            },
        };
    "#;
    let (_, report) = run_program(program_input(source, &["test.call"], SEED), |_| {
        (Ok(json!("x".repeat(32 * 1024))), None)
    });
    assert_eq!(report.return_value, Some(json!("payload_too_large")));
}

fn waiting(step: Step) -> (Vec<HostRequest>, Vec<HostCancel>) {
    match step {
        Step::Waiting { requests, cancels } => (requests, cancels),
        Step::Finished { report, .. } => panic!("the program finished: {report:?}"),
    }
}

fn finished(step: Step) -> (Vec<HostCancel>, ExecutionReport) {
    match step {
        Step::Finished {
            cancels, report, ..
        } => (cancels, report),
        Step::Waiting { .. } => panic!("the program is still waiting"),
    }
}

fn completed(request_id: &str, outcome: Result<Value, HostError>) -> HostCompletion {
    HostCompletion {
        request_id: request_id.into(),
        outcome,
        completed_at_ms: None,
    }
}

fn cancelled_call() -> HostError {
    HostError {
        kind: CANCELLED_CALL_KIND.into(),
        message: "cancelled".into(),
        value: None,
    }
}

#[test]
fn a_call_cancelled_in_the_step_that_issued_it_never_reaches_the_host() {
    let source = r#"
        export default {
            async run(input, env) {
                const h = env.web.fetch({ url: "a" });
                const cancelled = await h.cancel();
                let kind = null;
                try { await h; } catch (error) { kind = error.kind; }
                const other = await env.web.fetch({ url: "b" });
                return { cancelled, kind, other };
            }
        };
    "#;
    let (mut instance, step) =
        ProgramInstance::start(program_input(source, &["web.fetch"], SEED), STEP_BUDGET).unwrap();
    let (requests, cancels) = waiting(step);
    // Only the second fetch is sent; the first settled in the program.
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.request_id.as_str(), request.arguments.clone()))
            .collect::<Vec<_>>(),
        [("call-2", json!({"url": "b"}))]
    );
    assert!(cancels.is_empty());
    let (_, report) = finished(instance.step(vec![completed("call-2", Ok(json!("page")))]));
    assert_eq!(report.error, None);
    assert_eq!(
        report.return_value,
        Some(json!({
            "cancelled": {"status": "cancelled"},
            "kind": "tool_cancelled",
            "other": "page",
        }))
    );
}

#[test]
fn a_sent_call_is_cancelled_by_the_host_and_its_recorded_outcome_wins() {
    let source = r#"
        export default {
            async run(input, env) {
                const slow = env.web.fetch({ url: "slow" });
                const lost = env.web.fetch({ url: "lost" });
                await env.web.fetch({ url: "fast" });
                const [first, again] = await Promise.all([slow.cancel(), slow.cancel()]);
                const second = await lost.cancel();
                return { first, again, value: await slow, second };
            }
        };
    "#;
    let (mut instance, step) =
        ProgramInstance::start(program_input(source, &["web.fetch"], SEED), STEP_BUDGET).unwrap();
    let (requests, _) = waiting(step);
    assert_eq!(requests.len(), 3);
    // Both cancels go to the host once each, in order, after the calls
    // they name were sent; the program waits for their outcomes.
    let (requests, cancels) = waiting(instance.step(vec![completed("call-3", Ok(json!("fast")))]));
    assert!(requests.is_empty());
    assert_eq!(
        cancels,
        [HostCancel::Call {
            request_id: "call-1".into()
        }]
    );
    // The slow fetch finished before the cancel took effect.
    let (requests, cancels) =
        waiting(instance.step(vec![completed("call-1", Ok(json!("slow page")))]));
    assert!(requests.is_empty());
    assert_eq!(
        cancels,
        [HostCancel::Call {
            request_id: "call-2".into()
        }]
    );
    let (cancels, report) =
        finished(instance.step(vec![completed("call-2", Err(cancelled_call()))]));
    assert!(cancels.is_empty());
    assert_eq!(report.error, None);
    assert_eq!(
        report.return_value,
        Some(json!({
            "first": {"status": "resolved"},
            "again": {"status": "resolved"},
            "value": "slow page",
            "second": {"status": "cancelled"},
        }))
    );
}

#[test]
fn cancelling_an_input_handle_asks_the_host_and_waits_for_its_answer() {
    let source = r#"
        export default {
            async run(input, env) {
                const ask = await env.input.open({ ask: "Ship?" });
                return await ask.cancel();
            }
        };
    "#;
    let (mut instance, step) =
        ProgramInstance::start(program_input(source, INPUT_CONTRACTS, SEED), STEP_BUDGET).unwrap();
    let (requests, _) = waiting(step);
    assert_eq!(requests[0].binding_id, "opaque-input.open");
    // The withdrawal goes to the host; the answer comes from awaiting the
    // handle's promise.
    let (requests, cancels) = waiting(instance.step(vec![completed("call-1", Ok(opened()))]));
    assert_eq!(
        cancels,
        [HostCancel::Promise {
            promise_id: "promise_3".into()
        }]
    );
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.binding_id.as_str(), request.arguments.clone()))
            .collect::<Vec<_>>(),
        [("opaque-promise.await", json!({"promises": ["promise_3"]}))]
    );
    // The person answered before the withdrawal reached the inbox.
    let (_, report) = finished(instance.step(vec![completed(
        "call-2",
        Ok(json!({"outcome": "completed", "results": [
            {"promise_id": "promise_3", "status": "resolved", "output": {"approved": true}}
        ]})),
    )]));
    assert_eq!(report.error, None);
    assert_eq!(report.return_value, Some(json!({"status": "resolved"})));
}

#[test]
fn clearing_a_timer_cancels_its_sleep() {
    let source = r#"
        export default {
            async run(input, env) {
                let fired = false;
                const timer = setTimeout(() => { fired = true; }, 60000);
                await env.web.fetch({ url: "a" });
                clearTimeout(timer);
                return fired;
            }
        };
    "#;
    let (mut instance, step) = ProgramInstance::start(
        program_input(source, &["web.fetch", "time.sleep"], SEED),
        STEP_BUDGET,
    )
    .unwrap();
    let (requests, _) = waiting(step);
    assert_eq!(requests.len(), 2);
    let (cancels, report) = finished(instance.step(vec![completed("call-2", Ok(json!("page")))]));
    assert_eq!(
        cancels,
        [HostCancel::Call {
            request_id: "call-1".into()
        }]
    );
    assert_eq!(report.return_value, Some(json!(false)));
    assert_eq!(report.pending_request_ids, ["call-1"]);
}

#[test]
fn a_run_counts_calls_and_compute_across_its_executions() {
    // Calls the run's earlier executions made count toward its call budget.
    let source = r#"
        export default {
            async run(input, env, ctx) {
                const done = [];
                try {
                    for (;;) done.push(await env.web.fetch({ url: "x" }));
                } catch (error) {
                    return { made: done.length, message: error.message };
                }
            }
        };
    "#;
    let mut input = program_input(source, &["web.fetch"], SEED);
    input.program.calls_used = 30;
    let (_, report) = run_program(input, |_| (Ok(Value::Null), None));
    assert_eq!(report.metrics.tool_calls, 2);
    assert!(
        report
            .error
            .as_ref()
            .is_some_and(|error| error.kind == ExecutionErrorKind::LimitExceeded),
        "{report:?}"
    );

    // Compute is counted in interrupt checks, the same on every run, and the
    // run's budget includes what earlier executions used.
    let source = r#"
        export default {
            async run(input, env, ctx) {
                let x = 0;
                for (let call = 0; call < 3; call++) {
                    for (let i = 0; i < 200000; i++) x = (x + i) % 1000003;
                    await env.web.fetch({ url: "x" });
                }
                return x;
            }
        };
    "#;
    let measure = |compute_used: u64, max_compute: u64| {
        let mut input = program_input(source, &["web.fetch"], SEED);
        input.program.compute_used = compute_used;
        input.limits.max_compute = max_compute;
        run_program(input, |_| (Ok(Value::Null), None)).1
    };
    let full = measure(0, 1_000_000);
    assert_eq!(full.error, None);
    let used = full.metrics.compute;
    assert!(used > 0);
    assert_eq!(measure(0, 1_000_000).metrics.compute, used);
    assert_eq!(measure(1_000_000 - used, 1_000_000).error, None);
    let over = measure(1_000_000 - used + 1, 1_000_000);
    let error = over.error.expect("the run's compute runs out");
    assert_eq!(error.kind, ExecutionErrorKind::LimitExceeded);
    assert!(error.message.contains("compute"), "{}", error.message);
    assert_eq!(over.return_value, None);
}
