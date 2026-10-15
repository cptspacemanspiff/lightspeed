use super::*;

use serde_json::json;
use std::{collections::HashSet, time::Duration};

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
            timeout_ms: 2_000,
            max_memory_bytes: 16 * 1024 * 1024,
            max_stack_bytes: 256 * 1024,
            max_source_bytes: 64 * 1024,
            max_catalog_bytes: 64 * 1024,
            max_request_bytes: 16 * 1024,
            max_result_bytes: 16 * 1024,
            max_output_bytes: 16 * 1024,
            max_tool_calls: 32,
            max_outstanding_tool_calls: 8,
        },
        program: ProgramExecution {
            input: json!({"claim": "x"}),
            started_at_ms: 1_000,
            random_seed: seed,
            context: None,
        },
    }
}

/// Drive a program, answering each request in issue order with the
/// outcome and completion time `complete` gives it.
async fn run_program(
    input: ExecutionInput,
    mut complete: impl FnMut(&HostRequest) -> (Result<Value, HostError>, Option<u64>),
) -> (Vec<HostRequest>, ExecutionReport) {
    let mut execution = start(input, Cancellation::default()).unwrap();
    let sender = execution.completion_sender();
    let mut requests = Vec::new();
    while let Some(event) = execution.next_event().await {
        match event {
            ExecutionEvent::Request(request) => {
                let (outcome, completed_at_ms) = complete(&request);
                let result = sender.complete(HostCompletion {
                    request_id: request.request_id.clone(),
                    outcome,
                    completed_at_ms,
                });
                assert!(result.is_ok() || result == Err(CompletionError::Stopped));
                requests.push(request);
            }
            ExecutionEvent::Finished(report) => return (requests, report),
        }
    }
    panic!("execution ended without a report");
}

const SEED: [u32; 4] = [1, 2, 3, 4];

#[tokio::test(flavor = "current_thread")]
async fn programs_run_a_module_handler_with_contract_namespaces() {
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
    )
    .await;
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

#[tokio::test(flavor = "current_thread")]
async fn a_program_reads_its_context_and_keeps_the_last_value_it_sets() {
    let source = r#"
        export default {
            async run(input, env, ctx) {
                const before = ctx.context;
                const next = { round: before.round + 1 };
                ctx.setContext({ round: 0 });
                ctx.setContext(next);
                next.round = 99;
                const errors = [];
                for (const value of [undefined, () => 1, "x".repeat(70000)]) {
                    try { ctx.setContext(value); } catch (error) { errors.push(error.name); }
                }
                return { before, errors, frozen: Object.isFrozen(ctx) };
            }
        };
    "#;
    let mut input = program_input(source, &[], SEED);
    input.program.context = Some(json!({"round": 1}));
    let (_, report) = run_program(input, |_| unreachable!()).await;
    assert_eq!(report.error, None);
    assert_eq!(
        report.return_value,
        Some(json!({
            "before": {"round": 1},
            "errors": ["TypeError", "TypeError", "RangeError"],
            "frozen": true,
        }))
    );
    assert_eq!(report.program_context, Some(json!({"round": 2})));

    let source = r#"
        export default { async run(input, env, ctx) { return ctx.context === undefined; } };
    "#;
    let (_, report) = run_program(program_input(source, &[], SEED), |_| unreachable!()).await;
    assert_eq!(report.return_value, Some(json!(true)));
    assert_eq!(report.program_context, None);
}

#[tokio::test(flavor = "current_thread")]
async fn a_program_needs_a_default_export_with_run_and_compiles_before_effects() {
    for source in [
        "export const run = async () => 1;",
        "export default { run: 1 };",
    ] {
        let (_, report) = run_program(program_input(source, &[], SEED), |_| {
            (Ok(Value::Null), None)
        })
        .await;
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
    )
    .await;
    assert!(requests.is_empty());
    assert_eq!(report.error.unwrap().kind, ExecutionErrorKind::Javascript);
    let (_, report) = run_program(
        program_input(
            r#"import fs from "fs"; export default { async run() { return 1 } };"#,
            &[],
            SEED,
        ),
        |_| (Ok(Value::Null), None),
    )
    .await;
    assert!(report.error.is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn the_clock_is_the_recorded_start_and_advances_only_with_completions() {
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
    })
    .await;
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

#[tokio::test(flavor = "current_thread")]
async fn randomness_is_drawn_from_the_execution_seed() {
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
    let draw = |seed| async move {
        let (_, report) = run_program(program_input(source, &[], seed), |_| {
            (Ok(Value::Null), None)
        })
        .await;
        assert_eq!(report.error, None);
        report.return_value.unwrap()
    };
    let first = draw(SEED).await;
    assert_eq!(first, draw(SEED).await);
    assert_ne!(first, draw([4, 3, 2, 1]).await);
    let uuid = first["uuid"].as_str().unwrap();
    assert_eq!(uuid.len(), 36);
    assert_eq!(&uuid[14..15], "4");
    assert!(first["random"][0].as_f64().unwrap() < 1.0);
}

#[tokio::test(flavor = "current_thread")]
async fn programs_cannot_generate_code_from_strings() {
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
    })
    .await;
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

#[tokio::test(flavor = "current_thread")]
async fn timers_are_durable_sleeps_through_the_time_sleep_contract() {
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
        run_program(program_input(source, &["time.sleep"], SEED), sleep_outcome).await;
    assert_eq!(report.error, None);
    // Each wait, timer or explicit sleep, is exactly one recorded call.
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
        [
            json!({"ms": 5}),
            json!({"ms": 20}),
            json!({"ms": 30}),
            json!({"ms": -1})
        ]
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

#[tokio::test(flavor = "current_thread")]
async fn a_throwing_timer_fails_the_program_and_timers_need_the_contract() {
    let source = r#"
        export default {
            async run() {
                setTimeout(() => { throw new Error("timer failed"); }, 1);
                return await new Promise(() => {});
            }
        };
    "#;
    let (_, report) =
        run_program(program_input(source, &["time.sleep"], SEED), sleep_outcome).await;
    assert_eq!(report.error.unwrap().message, "timer failed");

    let (_, report) = run_program(
        program_input(
            "export default { async run() { setTimeout(() => {}, 1); } };",
            &[],
            SEED,
        ),
        |_| (Ok(Value::Null), None),
    )
    .await;
    assert!(
        report
            .error
            .unwrap()
            .message
            .contains("time.sleep contract")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn module_code_cannot_tamper_with_env_construction() {
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
    )
    .await;
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

#[tokio::test(flavor = "current_thread")]
async fn an_open_input_request_is_a_handle_over_ordinary_calls() {
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
    let (requests, report) = run_program(
        program_input(source, INPUT_CONTRACTS, SEED),
        |request| {
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
        },
    )
    .await;
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
            ("opaque-promise.cancel", json!({"promises": ["promise_3"]})),
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

#[tokio::test(flavor = "current_thread")]
async fn with_reminders_nudges_on_an_interval_then_reassigns() {
    let source = r#"
        export default {
            async run(input, env) {
                const ask = await env.input.open({ ask: "Merge?" });
                return await ask.withReminders({ every_ms: 60000, max: 2, then: "reassign", to: ["lead"] });
            }
        };
    "#;
    let mut execution = start(
        program_input(source, INPUT_CONTRACTS, SEED),
        Cancellation::default(),
    )
    .unwrap();
    let sender = execution.completion_sender();
    let mut held_await = None;
    let mut calls = Vec::new();
    let report = loop {
        match execution.next_event().await.expect("execution event") {
            ExecutionEvent::Request(request) => {
                calls.push(request.binding_id.clone());
                let outcome = match request.binding_id.as_str() {
                    "opaque-input.open" => Some(opened()),
                    // The answer arrives only once the request is reassigned.
                    "opaque-promise.await" => {
                        held_await = Some(request.request_id.clone());
                        None
                    }
                    "opaque-time.sleep" => Some(json!({"fire_at_ms": 61_000})),
                    "opaque-input.remind" => Some(control("pending")),
                    "opaque-input.reassign" => {
                        sender
                            .complete(HostCompletion {
                                request_id: held_await.take().expect("awaiting"),
                                outcome: Ok(json!({"outcome": "completed", "results": [
                                    {"promise_id": "promise_3", "status": "resolved", "output": {"approved": false}}
                                ]})),
                                completed_at_ms: None,
                            })
                            .unwrap();
                        Some(control("pending"))
                    }
                    other => panic!("unexpected call {other}"),
                };
                if let Some(outcome) = outcome {
                    sender
                        .complete(HostCompletion {
                            request_id: request.request_id,
                            outcome: Ok(outcome),
                            completed_at_ms: None,
                        })
                        .unwrap();
                }
            }
            ExecutionEvent::Finished(report) => break report,
        }
    };
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

/// Drive a program live, completing each request after a pseudo-random
/// delay drawn from `timing`, and record the completions in the order the
/// program was sent them.
async fn run_live(timing: u64) -> (Vec<HostRequest>, ExecutionReport, Journal) {
    let mut execution = start(
        program_input(CONFORMANCE_PROGRAM, &["test.call"], SEED),
        Cancellation::default(),
    )
    .unwrap();
    let sender = execution.completion_sender();
    let journal = std::sync::Arc::new(std::sync::Mutex::new(Journal::new()));
    let mut state = timing.max(1);
    let mut requests = Vec::new();
    let mut tasks = Vec::new();
    while let Some(event) = execution.next_event().await {
        match event {
            ExecutionEvent::Request(request) => {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let delay = Duration::from_micros(state % 4_000);
                let (sender, journal) = (sender.clone(), journal.clone());
                let outcome = conformance_outcome(&request);
                let request_id = request.request_id.clone();
                requests.push(request);
                tasks.push(tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let mut journal = journal.lock().unwrap();
                    let completion = HostCompletion {
                        request_id,
                        outcome,
                        completed_at_ms: Some(2_000 + journal.len() as u64),
                    };
                    if sender.complete(completion.clone()).is_ok() {
                        journal.push(completion);
                    }
                }));
            }
            ExecutionEvent::Finished(report) => {
                for task in tasks {
                    task.await.unwrap();
                }
                let journal = journal.lock().unwrap().clone();
                return (requests, report, journal);
            }
        }
    }
    panic!("execution ended without a report");
}

/// Replay a program against a journal, sending the recorded completions in
/// order as fast as the completion queue takes them, without waiting for
/// the program to issue their requests.
async fn replay(input: ExecutionInput, journal: Journal) -> (Vec<HostRequest>, ExecutionReport) {
    let mut execution = start(input, Cancellation::default()).unwrap();
    let sender = execution.completion_sender();
    let mut journal = journal.into_iter().peekable();
    let mut requests = Vec::new();
    loop {
        while let Some(completion) = journal.peek() {
            match sender.complete(completion.clone()) {
                Ok(()) => {
                    journal.next();
                }
                Err(CompletionError::Full | CompletionError::Stopped) => break,
                Err(error) => panic!("replay completion refused: {error}"),
            }
        }
        let event = tokio::time::timeout(Duration::from_millis(5), execution.next_event()).await;
        match event {
            Ok(Some(ExecutionEvent::Request(request))) => requests.push(request),
            Ok(Some(ExecutionEvent::Finished(report))) => return (requests, report),
            Ok(None) => panic!("execution ended without a report"),
            Err(_) => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_replayed_program_makes_the_same_calls_and_returns_the_same_output() {
    let mut journals = HashSet::new();
    for timing in 1..=24 {
        let (live_requests, live, journal) = run_live(timing).await;
        assert_eq!(live.error, None, "live run {timing} failed");
        assert_eq!(live_requests.len(), 12);
        journals.insert(
            journal
                .iter()
                .map(|completion| completion.request_id.clone())
                .collect::<Vec<_>>(),
        );
        let (replayed_requests, replayed) = replay(
            program_input(CONFORMANCE_PROGRAM, &["test.call"], SEED),
            journal,
        )
        .await;
        assert_eq!(replayed.error, None, "replay of run {timing} failed");
        assert_eq!(
            replayed_requests, live_requests,
            "run {timing} calls differ"
        );
        assert_eq!(
            replayed.return_value, live.return_value,
            "run {timing} output differs"
        );
    }
    // The timings must actually have produced different completion orders,
    // or the test proves nothing about order independence.
    assert!(
        journals.len() > 4,
        "only {} distinct orders",
        journals.len()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_recorded_completion_for_an_unissued_request_is_nondeterminism() {
    let source = r#"
        export default {
            async run(input, env) {
                await env.test.call({ n: 1 });
                return "done";
            },
        };
    "#;
    let journal = vec![HostCompletion {
        request_id: "call-2".into(),
        outcome: Ok(Value::Null),
        completed_at_ms: None,
    }];
    let (requests, report) = replay(program_input(source, &["test.call"], SEED), journal).await;
    assert_eq!(requests.len(), 1);
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
            Step::Waiting { requests: issued } => {
                requests.extend(issued);
                let batch = batches.next().expect("program waits past its journal");
                step = instance.step(batch.to_vec());
            }
            Step::Finished {
                requests: issued,
                report,
            } => {
                requests.extend(issued);
                assert!(instance.is_finished());
                assert_eq!(
                    instance.step(Vec::new()),
                    Step::Finished {
                        requests: Vec::new(),
                        report: report.clone()
                    }
                );
                return (requests, report);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_stepped_program_replays_a_live_run_however_its_completions_are_grouped() {
    for timing in 1..=8 {
        let (live_requests, live, journal) = run_live(timing).await;
        assert_eq!(live.error, None);
        for group in [1, 3, journal.len()] {
            let (requests, report) = step_through(&journal, group);
            assert_eq!(report.error, None, "run {timing} in groups of {group}");
            assert_eq!(requests, live_requests, "run {timing} in groups of {group}");
            assert_eq!(report.return_value, live.return_value);
            assert_eq!(report.metrics.tool_calls, live.metrics.tool_calls);
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
    while let Step::Waiting { requests } = step {
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

#[tokio::test(flavor = "current_thread")]
async fn the_deadline_stops_synchronous_and_microtask_loops() {
    for source in [
        "export default { run() { for (;;) {} } };",
        "export default { async run() { for (;;) await null; } };",
    ] {
        let mut input = program_input(source, &[], SEED);
        input.limits.timeout_ms = 100;
        let (_, report) = run_program(input, |_| unreachable!()).await;
        assert_eq!(
            terminal_error(&report),
            &ExecutionErrorKind::TimedOut,
            "{source}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_stops_a_running_and_a_waiting_program() {
    for source in [
        "export default { run() { for (;;) {} } };",
        "export default { async run(input, env) { await env.test.call({}); } };",
    ] {
        let cancellation = Cancellation::default();
        let mut execution = start(
            program_input(source, &["test.call"], SEED),
            cancellation.clone(),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancellation.cancel();
        let report = loop {
            match tokio::time::timeout(Duration::from_secs(1), execution.next_event())
                .await
                .expect("the program stops")
            {
                Some(ExecutionEvent::Finished(report)) => break report,
                Some(ExecutionEvent::Request(_)) => {}
                None => panic!("execution ended without a report"),
            }
        };
        assert_eq!(
            terminal_error(&report),
            &ExecutionErrorKind::Cancelled,
            "{source}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn the_memory_limit_ends_the_program() {
    let source =
        "export default { run() { const a = []; for (;;) a.push(new Array(1e5).fill(1)); } };";
    let (_, report) = run_program(program_input(source, &[], SEED), |_| unreachable!()).await;
    assert_eq!(terminal_error(&report), &ExecutionErrorKind::LimitExceeded);
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_and_duplicate_completions_end_the_program() {
    let source = r#"
        export default {
            async run(input, env) {
                await env.test.call({ n: 1 });
                await env.test.call({ n: 2 });
            },
        };
    "#;
    for duplicate in [false, true] {
        let mut execution = start(
            program_input(source, &["test.call"], SEED),
            Cancellation::default(),
        )
        .unwrap();
        let sender = execution.completion_sender();
        let Some(ExecutionEvent::Request(first)) = execution.next_event().await else {
            panic!("first request")
        };
        let complete = |request_id: &str| {
            sender
                .complete(HostCompletion {
                    request_id: request_id.into(),
                    outcome: Ok(Value::Null),
                    completed_at_ms: None,
                })
                .unwrap()
        };
        if duplicate {
            complete(&first.request_id);
            let Some(ExecutionEvent::Request(_)) = execution.next_event().await else {
                panic!("second request")
            };
            complete(&first.request_id);
        } else {
            complete("call-9");
        }
        let Some(ExecutionEvent::Finished(report)) = execution.next_event().await else {
            panic!("terminal report")
        };
        // A completion for a request never issued is a divergence from a
        // recorded run; one already delivered is a faulty host.
        assert_eq!(
            terminal_error(&report),
            if duplicate {
                &ExecutionErrorKind::Internal
            } else {
                &ExecutionErrorKind::Nondeterminism
            }
        );
    }
}
