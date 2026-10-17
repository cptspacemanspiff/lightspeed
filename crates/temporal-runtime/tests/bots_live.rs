//! Bots live proof: the bot controller, trigger fires, admission, and
//! the `bots/*` service against the local Temporal + PostgreSQL stack, with
//! a fake LLM for the mechanics and one real-model scenario for the resolve
//! round trip.
//!
//! Run serially against the local stack:
//!
//! ```bash
//! source scripts/dev/env.sh
//! cargo test -p temporal-runtime --test bots_live -- --ignored --test-threads=1
//! ```

mod support;

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_client::WorkflowExecutionStatus;

use api::{
    AgentApiService, AgentProfileInput, BotBreaker, BotCloseParams, BotCoalescePolicy,
    BotControllerStatus, BotCreateParams, BotDeleteParams, BotDocument, BotEventAdmitParams,
    BotEventInput, BotEventListParams, BotEventOutcome, BotEventView, BotId, BotInput,
    BotPutParams, BotReadParams, BotSessionRotateParams, BotSetupStatus, BotStateReadParams,
    BotTriggerDeleteParams, BotTriggerDocument, BotTriggerId, BotTriggerInput, BotTriggerPutParams,
    BotTriggerSpec, ProfileCreateParams, ProfileDocument, ProfileId, ProfileInstructions,
    SessionReadParams, SessionStatus, WebhookVerification,
};
use bots::ids::{bot_controller_workflow_id, bot_main_session_id, bot_schedule_id};
use harness::{CoreAgentLlm, CoreAgentTools, storage::BlobStore};
use support::live::{
    LIVE_TEST_LOCK, live_universe_id, require_openai_live_env, require_storage_live_env,
};
use temporal_runtime::{
    config::TaskQueues,
    gateway::GatewayAgentApi,
    pg_store_from_env,
    worker::{
        ActivityState, BotWorkerActivities, FakeLlm, FakeTools, ProgramWorkerActivities,
        SessionTools, WorkerActivities, bots_worker, program_worker, sessions_worker,
        worker_runtime,
    },
};
use temporal_workflow::{DEFAULT_TEMPORAL_NAMESPACE, DEFAULT_TEMPORAL_TARGET, connect_temporal};
use temporalio_client::{Client, WorkflowDescribeOptions};
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;

const WAIT: Duration = Duration::from_secs(90);

enum Llm {
    Fake,
    Real,
    /// No model: the bot's profile has a program, which runs on the program
    /// worker started beside the others.
    Program,
}

/// Run the sessions worker (fake or real LLM), the bots worker, and the
/// in-process service over one random deployment queue prefix.
async fn run_bots_live<F, Fut>(llm: Llm, body: F) -> anyhow::Result<()>
where
    F: FnOnce(Arc<GatewayAgentApi>, Client) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    require_storage_live_env()?;
    let _guard = LIVE_TEST_LOCK.lock().await;
    let universe = live_universe_id()?;
    let store = pg_store_from_env().await?;
    support::live::seed_agent_default(&store, &support::live::openai_live_model()).await?;
    let queues = TaskQueues::derived_from(format!(
        "lightspeed-bots-live-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let temporal_target =
        std::env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_TEMPORAL_TARGET.to_owned());
    let namespace = std::env::var("TEMPORAL_NAMESPACE")
        .unwrap_or_else(|_| DEFAULT_TEMPORAL_NAMESPACE.to_owned());
    let runtime = worker_runtime()?;
    let client = connect_temporal(&temporal_target, &namespace).await?;

    let builder = GatewayAgentApi::builder(client.clone(), store.clone())
        .with_task_queue(queues.sessions.clone())
        .with_bot_task_queue(queues.bots.clone())
        .with_channel_task_queue(queues.channels.clone())
        .with_code_task_queue(queues.code.clone())
        .with_program_task_queue(queues.program.clone());
    let activity_state = match llm {
        Llm::Fake => {
            let blobs: Arc<dyn BlobStore> = store.clone();
            let llm =
                Arc::new(FakeLlm::new(blobs.clone()).with_tool_rounds(0)) as Arc<dyn CoreAgentLlm>;
            let tools = Arc::new(FakeTools::new(blobs)) as Arc<dyn CoreAgentTools>;
            ActivityState::from_pg_store(store.clone(), llm, tools)
        }
        Llm::Real => ActivityState::from_pg_store_with_default_runtime(store.clone())?,
        Llm::Program => {
            let blobs: Arc<dyn BlobStore> = store.clone();
            let llm = Arc::new(FakeLlm::new(blobs)) as Arc<dyn CoreAgentLlm>;
            let tools = Arc::new(SessionTools::from_pg_store(store.clone()));
            ActivityState::from_pg_store(store.clone(), llm, tools.clone())
                .with_hosted_tools(tools)
                .with_workflow_tool_executions(client.clone())
                .with_program_task_queue(queues.program.clone())
        }
    };
    let program = matches!(llm, Llm::Program);
    let api = Arc::new(builder.build());

    let mut sessions_worker = sessions_worker(
        &runtime,
        client.clone(),
        queues.sessions.clone(),
        WorkerActivities::for_universe(universe, activity_state),
    )?;
    let mut bots = bots_worker(
        &runtime,
        client.clone(),
        queues.bots.clone(),
        BotWorkerActivities::for_universe(universe, api.clone()),
    )?;
    let mut programs = if program {
        Some(program_worker(
            &runtime,
            client.clone(),
            queues.program.clone(),
            ProgramWorkerActivities::for_universe(universe, api.clone()),
        )?)
    } else {
        None
    };
    let shutdown_sessions = sessions_worker.shutdown_handle();
    let shutdown_bots = bots.shutdown_handle();
    let shutdown_programs = programs.as_ref().map(|worker| worker.shutdown_handle());
    let workers = async {
        let programs = async {
            match programs.as_mut() {
                Some(worker) => worker.run().await,
                None => Ok(()),
            }
        };
        tokio::try_join!(sessions_worker.run(), bots.run(), programs)
            .map(|_| ())
            .map_err(anyhow::Error::from)
    };
    tokio::pin!(workers);
    let body = temporal_runtime::gateway::request_context::with_request_context(
        support::live::local_request_context().await?,
        body(api.clone(), client.clone()),
    );
    tokio::pin!(body);
    let result = tokio::select! {
        workers_result = workers.as_mut() => Err(anyhow::anyhow!("workers stopped early: {workers_result:?}")),
        body_result = support::live::bounded_live_test("bots_live", support::live::LIVE_TEST_BUDGET, body.as_mut()) => body_result,
    };
    shutdown_sessions();
    shutdown_bots();
    if let Some(shutdown_programs) = shutdown_programs {
        shutdown_programs();
    }
    let shutdown_result = support::live::bounded_live_test(
        "worker shutdown",
        Duration::from_secs(10),
        workers.as_mut(),
    )
    .await;
    result.and(shutdown_result)
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

async fn create_profile(api: &GatewayAgentApi, instructions: &str) -> anyhow::Result<ProfileId> {
    let profile_id = ProfileId::new(unique("bots-live"));
    api.create_profile(ProfileCreateParams {
        profile: AgentProfileInput {
            profile_id: profile_id.clone(),
            display_name: Some("bots live".to_owned()),
            description: None,
            document: ProfileDocument {
                program: None,
                instructions: Some(ProfileInstructions::Text {
                    text: instructions.to_owned(),
                }),
                ..ProfileDocument::default()
            },
        },
    })
    .await?;
    Ok(profile_id)
}

fn bot_document(profile_id: &ProfileId) -> BotDocument {
    BotDocument {
        display_name: Some("Live bot".to_owned()),
        description: Some("bots live test".to_owned()),
        profile_id: profile_id.clone(),
        brief: Some("Acknowledge every event briefly.".to_owned()),
        runs_per_day: None,
        breaker: None,
        routed_session_close_after_ms: None,
        self_config: false,
        emit: false,
        enabled: true,
        escalate_to: Vec::new(),
    }
}

async fn create_bot(
    api: &GatewayAgentApi,
    profile_id: &ProfileId,
    edit: impl FnOnce(&mut BotDocument),
    triggers: Vec<BotTriggerInput>,
) -> anyhow::Result<BotId> {
    let bot_id = BotId::new(unique("live"));
    let mut document = bot_document(profile_id);
    edit(&mut document);
    api.create_bot(BotCreateParams {
        bot: BotInput {
            bot_id: bot_id.clone(),
            document,
        },
        triggers,
    })
    .await?;
    Ok(bot_id)
}

fn manual_event(summary: &str) -> BotEventInput {
    BotEventInput {
        kind: "test.ping".to_owned(),
        summary: summary.to_owned(),
        data: Some(serde_json::json!({ "note": summary })),
        headers: Default::default(),
        event_id: None,
        occurred_at_ms: None,
        correlation_id: None,
        links: Vec::new(),
    }
}

async fn list_events(api: &GatewayAgentApi, bot_id: &BotId) -> anyhow::Result<Vec<BotEventView>> {
    Ok(api
        .list_bot_events(BotEventListParams {
            bot_id: bot_id.clone(),
            limit: Some(50),
            cursor: None,
        })
        .await?
        .result
        .events)
}

/// Wait until every event of the bot has an outcome.
async fn wait_for_outcomes(
    api: &GatewayAgentApi,
    bot_id: &BotId,
    expected: usize,
) -> anyhow::Result<Vec<BotEventView>> {
    let started = Instant::now();
    loop {
        let events = list_events(api, bot_id).await?;
        let resolved = events
            .iter()
            .filter(|event| event.outcome.is_some())
            .count();
        if events.len() >= expected && resolved >= expected {
            return Ok(events);
        }
        if started.elapsed() > WAIT {
            let state = api
                .read_bot_state(BotStateReadParams {
                    bot_id: bot_id.clone(),
                })
                .await?
                .result
                .state;
            anyhow::bail!(
                "timed out waiting for {expected} outcomes on bot {bot_id}; state: {state:#?}; events: {events:#?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_controller<F>(
    api: &GatewayAgentApi,
    bot_id: &BotId,
    predicate: F,
) -> anyhow::Result<Option<api::BotControllerSnapshot>>
where
    F: Fn(Option<&api::BotControllerSnapshot>) -> bool,
{
    let started = Instant::now();
    loop {
        let state = api
            .read_bot_state(BotStateReadParams {
                bot_id: bot_id.clone(),
            })
            .await?
            .result
            .state;
        if predicate(state.controller.as_ref()) {
            return Ok(state.controller);
        }
        if started.elapsed() > WAIT {
            anyhow::bail!("timed out waiting for bot {bot_id} controller state; last: {state:#?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Wait until the bot's controller shows `applied` and has nothing in
/// flight: no activity pending and no workflow task scheduled. A test that
/// ends right after a configuration change waits for this, or the session
/// reconcile the change caused is still talking to the session worker when
/// the workers shut down.
async fn wait_for_controller_settled<F>(
    api: &GatewayAgentApi,
    client: &Client,
    bot_id: &BotId,
    applied: F,
) -> anyhow::Result<()>
where
    F: Fn(&api::BotControllerSnapshot) -> bool,
{
    wait_for_controller(api, bot_id, |controller| controller.is_some_and(&applied)).await?;
    let handle = client.get_workflow_handle::<temporal_workflow::BotControllerWorkflow>(
        bot_controller_workflow_id(live_universe_id()?, bot_id),
    );
    let started = Instant::now();
    loop {
        let description = handle
            .describe(WorkflowDescribeOptions::default())
            .await?
            .raw_description;
        if description.pending_activities.is_empty() && description.pending_workflow_task.is_none()
        {
            return Ok(());
        }
        if started.elapsed() > WAIT {
            anyhow::bail!(
                "bot {bot_id} controller never settled: {} pending activities",
                description.pending_activities.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_manual_event_runs_and_records_outcome() -> anyhow::Result<()> {
    run_bots_live(Llm::Fake, |api, _client| async move {
        let profile_id = create_profile(&api, "You are a live-test bot.").await?;
        let bot_id = create_bot(&api, &profile_id, |_| {}, Vec::new()).await?;

        let admitted = api
            .admit_bot_event(BotEventAdmitParams {
                bot_id: bot_id.clone(),
                event: manual_event("first ping"),
            })
            .await?
            .result;
        assert_eq!(admitted.event.seq, 1);
        assert!(!admitted.duplicate);

        let events = wait_for_outcomes(&api, &bot_id, 1).await?;
        let event = &events[0];
        // The fake model never calls bot_event_resolve, so the delivery ends
        // unresolved — with the run that handled it recorded on the row.
        assert_eq!(event.outcome, Some(BotEventOutcome::Unresolved));
        assert!(event.run_id.is_some(), "{event:#?}");

        let snapshot = wait_for_controller(&api, &bot_id, |controller| {
            controller.is_some_and(|snapshot| snapshot.recent_deliveries.len() == 1)
        })
        .await?
        .expect("controller running");
        assert_eq!(snapshot.runs_today, 1);
        assert_eq!(snapshot.recent_deliveries[0].seqs, vec![1]);
        assert_eq!(snapshot.controller_status, BotControllerStatus::Idle);

        let main_session = bot_main_session_id(&bot_id, 1);
        let session = api
            .read_session(SessionReadParams {
                session_id: main_session.clone(),
                run_limit: None,
            })
            .await?
            .result
            .session;
        assert_eq!(session.status, SessionStatus::Idle);
        assert!(
            session.active_tools.tools.iter().any(|tool| {
                tool.tool_id == "bot_event_resolve"
                    || tool.tool_id == "lightspeed.bots.event.resolve.v1"
            }),
            "bot tools declared: {:?}",
            session
                .active_tools
                .tools
                .iter()
                .map(|tool| tool.tool_id.clone())
                .collect::<Vec<_>>()
        );

        // A duplicate admission keeps #N and wakes the controller again.
        let duplicate = api
            .admit_bot_event(BotEventAdmitParams {
                bot_id: bot_id.clone(),
                event: BotEventInput {
                    event_id: Some(admitted.event.event_id.clone()),
                    ..manual_event("first ping again")
                },
            })
            .await?
            .result;
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.event.seq, 1);
        Ok(())
    })
    .await
}

/// A program profile whose program reports which version ran and what it
/// was given, and keeps every note it has seen as its program context.
/// From version 2 it also decides its delivery: ignored.
fn program_profile_input(profile_id: &ProfileId, version: u32) -> AgentProfileInput {
    let verdict = if version >= 2 {
        "outcome: \"ignored\", summary: \"nothing to do\", "
    } else {
        ""
    };
    AgentProfileInput {
        profile_id: profile_id.clone(),
        display_name: Some("bots live program".to_owned()),
        description: None,
        document: ProfileDocument {
            program: Some(api::ProgramSpec {
                source: api::ProgramSource::Text {
                    text: format!(
                        "export default {{ async run(input, env, ctx) {{ \
                         const notes = input.events.map(e => e.event.data.note); \
                         const seen = ctx.state?.notes ?? []; \
                         return ctx.complete({{ {verdict}version: {version}, seqs: input.events.map(e => e.seq), \
                         kinds: input.events.map(e => e.event.kind), notes, seen }}, \
                         {{ notes: [...seen, ...notes] }}); }} }};"
                    ),
                },
                semantics_version: 1,
                input_schema: Some(serde_json::json!({
                    "type": "object",
                    "required": ["events"],
                    "properties": {"events": {"type": "array", "minItems": 1}},
                })),
                output_schema: Some(serde_json::json!({
                    "type": "object",
                    "required": ["version", "seqs"],
                })),
                limits: None,
                requires: Vec::new(),
                approvals: None,
            }),
            ..ProfileDocument::default()
        },
    }
}

/// The program output of a bot event's run on `session_id`.
async fn event_program_output(
    api: &GatewayAgentApi,
    store: &store_pg::PgStore,
    session_id: &str,
    event: &BotEventView,
) -> anyhow::Result<serde_json::Value> {
    let run_id = event.run_id.as_deref().expect("event ran");
    let run = support::live::wait_for_terminal_run(
        api,
        &harness::SessionId::new(session_id.to_owned()),
        run_id,
    )
    .await?;
    assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
    assert!(matches!(run.source, api::RunViewSource::Program { .. }));
    let output = run.output.expect("program output");
    Ok(serde_json::from_slice(
        &store
            .read_bytes(&harness::BlobRef::parse(&output.content_ref)?)
            .await?,
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_program_profile_runs_each_delivery_as_a_program_run() -> anyhow::Result<()> {
    run_bots_live(Llm::Program, |api, client| async move {
        let store = pg_store_from_env().await?;
        let profile_id = ProfileId::new(unique("bots-live-program"));
        api.create_profile(ProfileCreateParams {
            profile: program_profile_input(&profile_id, 1),
        })
        .await?;
        let bot_id = create_bot(&api, &profile_id, |_| {}, Vec::new()).await?;

        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("first ping"),
        })
        .await?;
        let events = wait_for_outcomes(&api, &bot_id, 1).await?;
        // The program's output names no outcome, so its completed run
        // handled the delivery.
        assert_eq!(events[0].outcome, Some(BotEventOutcome::Handled));
        assert_eq!(events[0].outcome_detail, None);
        let main = bot_main_session_id(&bot_id, 1);
        let session = api
            .read_session(SessionReadParams {
                session_id: main.clone(),
                run_limit: None,
            })
            .await?
            .result
            .session;
        assert_eq!(
            session.driver,
            api::SessionDriverView::Program {
                program_id: Some(profile_id.as_str().to_owned()),
                revision: Some(1),
                semantics_version: 1,
            }
        );
        assert_eq!(
            event_program_output(&api, &store, &main, &events[0]).await?,
            serde_json::json!({
                "version": 1, "seqs": [1], "kinds": ["test.ping"], "notes": ["first ping"],
                "seen": [],
            })
        );

        // A new program revision cannot apply to the pinned session, so the
        // bot rotates its main session and the next event runs the new one,
        // starting from the program context the old session kept.
        api.put_profile(api::ProfilePutParams {
            profile: program_profile_input(&profile_id, 2),
            expected_revision: Some(1),
        })
        .await?;
        wait_for_controller_settled(&api, &client, &bot_id, |snapshot| {
            snapshot.applied_profile_revision == Some(2)
        })
        .await?;
        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("second ping"),
        })
        .await?;
        let events = wait_for_outcomes(&api, &bot_id, 2).await?;
        let second = events
            .iter()
            .find(|event| event.seq == 2)
            .expect("second event");
        assert_eq!(
            event_program_output(&api, &store, &bot_main_session_id(&bot_id, 2), second).await?,
            serde_json::json!({
                "outcome": "ignored", "summary": "nothing to do",
                "version": 2, "seqs": [2], "kinds": ["test.ping"], "notes": ["second ping"],
                "seen": ["first ping"],
            })
        );
        // Its output's verdict decides the delivery.
        assert_eq!(second.outcome, Some(BotEventOutcome::Ignored));
        assert_eq!(second.outcome_detail.as_deref(), Some("nothing to do"));
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_program_verdict_keeps_the_output_out_of_controller_history() -> anyhow::Result<()>
{
    const PROBE: &str = "controller-history-probe;";
    run_bots_live(Llm::Program, |api, client| async move {
        let profile_id = ProfileId::new(unique("bots-live-large-output"));
        let mut profile = program_profile_input(&profile_id, 1);
        profile.document.program.as_mut().expect("program").source = api::ProgramSource::Text {
            text: format!(
                "export default {{ async run() {{ return {{ outcome: \"ignored\", \
                 summary: \"s\".repeat(3000), padding: \"{PROBE}\".repeat(8000) }}; }} }};"
            ),
        };
        profile
            .document
            .program
            .as_mut()
            .expect("program")
            .output_schema = None;
        api.create_profile(ProfileCreateParams { profile }).await?;
        let bot_id = create_bot(&api, &profile_id, |_| {}, Vec::new()).await?;
        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("large output"),
        })
        .await?;
        let events = wait_for_outcomes(&api, &bot_id, 1).await?;
        // The verdict still decides the delivery, with its summary cut to
        // the bot summary cap.
        assert_eq!(events[0].outcome, Some(BotEventOutcome::Ignored));
        let detail = events[0].outcome_detail.as_deref().expect("summary");
        assert!(!detail.is_empty() && detail.len() <= bots::validate::MAX_SUMMARY_LEN);
        assert!(detail.bytes().all(|byte| byte == b's'));

        // Only the verdict crossed into the controller's history: no
        // activity result carries the ~200 KB output.
        let handle = client.get_workflow_handle::<temporal_workflow::BotControllerWorkflow>(
            bot_controller_workflow_id(live_universe_id()?, &bot_id),
        );
        let history = handle
            .fetch_history(Default::default())
            .into_events()
            .await?;
        let mut largest = 0;
        for event in &history {
            let Some(Attributes::ActivityTaskCompletedEventAttributes(completed)) =
                &event.attributes
            else {
                continue;
            };
            for payload in completed.result.iter().flat_map(|result| &result.payloads) {
                largest = largest.max(payload.data.len());
                assert!(
                    !payload
                        .data
                        .windows(PROBE.len())
                        .any(|window| window == PROBE.as_bytes()),
                    "the program output reached the controller's history"
                );
            }
        }
        assert!(
            largest < 16 * 1024,
            "largest activity result {largest} bytes"
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_routed_program_session_moves_to_a_new_program_revision() -> anyhow::Result<()> {
    run_bots_live(Llm::Program, |api, client| async move {
        let store = pg_store_from_env().await?;
        let profile_id = ProfileId::new(unique("bots-live-program"));
        api.create_profile(ProfileCreateParams {
            profile: program_profile_input(&profile_id, 1),
        })
        .await?;
        let trigger_id = BotTriggerId::new("hook");
        let bot_id = create_bot(
            &api,
            &profile_id,
            |_| {},
            vec![BotTriggerInput {
                trigger_id: trigger_id.clone(),
                document: BotTriggerDocument {
                    spec: BotTriggerSpec::Webhook {
                        verification: WebhookVerification::Token,
                        preset: None,
                    },
                    filter: None,
                    route: Some(api::BotTriggerRoute::PerKey {
                        key: Some("data.key".to_owned()),
                    }),
                    coalesce: None,
                    deliver: None,
                    session_close_after_ms: None,
                    enabled: true,
                    escalate_to: None,
                },
                pairing_code: None,
            }],
        )
        .await?;
        let ingest_path = api
            .read_bot_trigger(api::BotTriggerReadParams {
                bot_id: bot_id.clone(),
                trigger_id: trigger_id.clone(),
            })
            .await?
            .result
            .trigger
            .ingest_path
            .expect("webhook ingest path");
        let token = ingest_path.rsplit('/').next().expect("token").to_owned();
        let ingest = |note: &'static str| {
            let api = api.clone();
            let bot_id = bot_id.clone();
            let trigger_id = trigger_id.clone();
            let token = token.clone();
            async move {
                let body = serde_json::json!({ "kind": "pr", "key": "a", "note": note });
                let outcome = api
                    .ingest_bot_webhook(
                        bot_id.as_str(),
                        trigger_id.as_str(),
                        &token,
                        Default::default(),
                        body.to_string().as_bytes(),
                    )
                    .await;
                assert!(
                    matches!(
                        outcome,
                        temporal_runtime::bots::hooks::WebhookIngestOutcome::Admitted { .. }
                    ),
                    "{outcome:?}"
                );
            }
        };
        let keyed = bots::ids::bot_keyed_session_id(&bot_id, "a");

        ingest("one").await;
        let events = wait_for_outcomes(&api, &bot_id, 1).await?;
        assert_eq!(
            event_program_output(&api, &store, &keyed, &events[0]).await?["version"],
            1
        );

        // The key's session pinned revision 1. After the profile moves on,
        // the next event for the key retires it and runs the new program in
        // the key's next generation, which carries on from the program
        // context the key's last session kept.
        api.put_profile(api::ProfilePutParams {
            profile: program_profile_input(&profile_id, 2),
            expected_revision: Some(1),
        })
        .await?;
        wait_for_controller_settled(&api, &client, &bot_id, |snapshot| {
            snapshot.applied_profile_revision == Some(2)
        })
        .await?;
        ingest("two").await;
        let events = wait_for_outcomes(&api, &bot_id, 2).await?;
        let second = events.iter().find(|event| event.seq == 2).expect("second");
        let successor = bots::ids::routed_session_generation_id(&keyed, 2);
        assert_eq!(
            event_program_output(&api, &store, &successor, second).await?,
            serde_json::json!({
                "outcome": "ignored", "summary": "nothing to do",
                "version": 2, "seqs": [2], "kinds": ["pr"], "notes": ["two"],
                "seen": ["one"],
            })
        );
        let retired = api
            .read_session(SessionReadParams {
                session_id: keyed,
                run_limit: None,
            })
            .await?
            .result
            .session;
        assert_eq!(retired.status, SessionStatus::Closed, "{retired:#?}");
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_reset_main_recovers_a_missing_session_row() -> anyhow::Result<()> {
    run_bots_live(Llm::Fake, |api, client| async move {
        let profile_id = create_profile(&api, "You are a live-test bot.").await?;
        let bot_id = create_bot(&api, &profile_id, |_| {}, Vec::new()).await?;
        let first = bot_main_session_id(&bot_id, 1);
        wait_for_controller_settled(&api, &client, &bot_id, |controller| {
            controller.setup_status == BotSetupStatus::Ready && controller.main_session_id == first
        })
        .await?;

        // Leave the original Temporal workflow running while removing its
        // projection, as can happen when local database state is reset.
        let store = pg_store_from_env().await?;
        let deleted =
            sqlx::query("DELETE FROM sessions WHERE universe_id = $1 AND session_id = $2")
                .bind(live_universe_id()?)
                .bind(&first)
                .execute(store.pool())
                .await?;
        assert_eq!(deleted.rows_affected(), 1);

        assert!(
            api.rotate_bot_session(BotSessionRotateParams {
                bot_id: bot_id.clone(),
                session_id: first.clone(),
            })
            .await?
            .result
            .accepted
        );
        let second = bot_main_session_id(&bot_id, 2);
        wait_for_controller_settled(&api, &client, &bot_id, |controller| {
            controller.setup_status == BotSetupStatus::Ready && controller.main_session_id == second
        })
        .await?;
        let session = api
            .read_session(SessionReadParams {
                session_id: second,
                run_limit: None,
            })
            .await?
            .result
            .session;
        assert_eq!(session.status, SessionStatus::Idle);
        let first_workflow = client.get_workflow_handle::<temporal_workflow::AgentSessionWorkflow>(
            temporal_workflow::compose_workflow_id(
                live_universe_id()?,
                &harness::session::SessionId::new(first),
            ),
        );
        assert_ne!(
            first_workflow
                .describe(WorkflowDescribeOptions::default())
                .await?
                .status(),
            WorkflowExecutionStatus::Running
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_hmac_webhook_verifies_without_a_caller() -> anyhow::Result<()> {
    run_bots_live(Llm::Fake, |api, _client| async move {
        let profile_id = create_profile(&api, "You are a live-test bot.").await?;
        let secret = format!("signing-{}", uuid::Uuid::new_v4().simple());
        let grant_id = api
            .import_auth_grant(api::AuthGrantImportParams {
                grant_id: None,
                provider_id: None,
                exposure: api::AuthGrantExposure::Retrievable,
                token: secret.clone(),
                display_name: Some("Webhook signing secret".to_owned()),
                subject_hint: None,
                scopes: Vec::new(),
                audience: None,
                expires_at_ms: None,
                metadata: None,
            })
            .await?
            .result
            .grant
            .grant_id;
        let trigger_id = BotTriggerId::new("signed");
        let bot_id = create_bot(
            &api,
            &profile_id,
            |_| {},
            vec![BotTriggerInput {
                trigger_id: trigger_id.clone(),
                document: BotTriggerDocument {
                    spec: BotTriggerSpec::Webhook {
                        verification: WebhookVerification::HmacSha256 {
                            grant_id,
                            header: "x-signature".to_owned(),
                            prefix: Some("sha256=".to_owned()),
                            audience: None,
                        },
                        preset: None,
                    },
                    filter: None,
                    route: None,
                    coalesce: None,
                    deliver: None,
                    session_close_after_ms: None,
                    enabled: true,
                    escalate_to: None,
                },
                pairing_code: None,
            }],
        )
        .await?;
        let trigger = api
            .read_bot_trigger(api::BotTriggerReadParams {
                bot_id: bot_id.clone(),
                trigger_id: trigger_id.clone(),
            })
            .await?
            .result
            .trigger;
        let ingest_path = trigger.ingest_path.expect("webhook ingest path");
        let token = ingest_path.rsplit('/').next().expect("token").to_owned();

        // The public ingest route has no caller: run it outside the test's
        // request context, exactly as the HTTP edge does. The stored trigger,
        // not a caller, names the signing secret.
        let deliver = |signature: String| {
            let (api, bot_id, trigger_id, token) = (
                api.clone(),
                bot_id.clone(),
                trigger_id.clone(),
                token.clone(),
            );
            tokio::spawn(async move {
                let headers = [("x-signature".to_owned(), signature)]
                    .into_iter()
                    .collect();
                api.ingest_bot_webhook(
                    bot_id.as_str(),
                    trigger_id.as_str(),
                    &token,
                    headers,
                    br#"{"kind":"deploy"}"#,
                )
                .await
            })
        };
        let signed = format!(
            "sha256={}",
            bots::webhook::hmac_sha256_hex(&secret, br#"{"kind":"deploy"}"#)
        );
        let admitted = deliver(signed).await?;
        assert!(
            matches!(
                admitted,
                temporal_runtime::bots::hooks::WebhookIngestOutcome::Admitted { .. }
            ),
            "{admitted:?}"
        );
        let forged = deliver(format!(
            "sha256={}",
            bots::webhook::hmac_sha256_hex("not-the-secret", br#"{"kind":"deploy"}"#)
        ))
        .await?;
        assert!(
            !matches!(
                forged,
                temporal_runtime::bots::hooks::WebhookIngestOutcome::Admitted { .. }
                    | temporal_runtime::bots::hooks::WebhookIngestOutcome::SecretUnavailable { .. }
            ),
            "{forged:?}"
        );
        wait_for_outcomes(&api, &bot_id, 1).await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_webhook_trigger_coalesces_events() -> anyhow::Result<()> {
    run_bots_live(Llm::Fake, |api, _client| async move {
        let profile_id = create_profile(&api, "You are a live-test bot.").await?;
        let trigger_id = BotTriggerId::new("hook");
        let bot_id = create_bot(
            &api,
            &profile_id,
            |_| {},
            vec![BotTriggerInput {
                trigger_id: trigger_id.clone(),
                document: BotTriggerDocument {
                    spec: BotTriggerSpec::Webhook {
                        verification: WebhookVerification::Token,
                        preset: None,
                    },
                    filter: Some("data.kind != 'noise'".to_owned()),
                    route: None,
                    coalesce: Some(BotCoalescePolicy {
                        debounce_ms: 500,
                        max_wait_ms: 3_000,
                        max_count: 10,
                    }),
                    deliver: None,
                    session_close_after_ms: None,
                    enabled: true,
                    escalate_to: None,
                },
                pairing_code: None,
            }],
        )
        .await?;
        let trigger = api
            .read_bot_trigger(api::BotTriggerReadParams {
                bot_id: bot_id.clone(),
                trigger_id: trigger_id.clone(),
            })
            .await?
            .result
            .trigger;
        let ingest_path = trigger.ingest_path.expect("webhook ingest path");
        let token = ingest_path.rsplit('/').next().expect("token").to_owned();

        for index in 0..3 {
            let body = serde_json::json!({ "kind": "deploy", "n": index }).to_string();
            let outcome = api
                .ingest_bot_webhook(
                    bot_id.as_str(),
                    trigger_id.as_str(),
                    &token,
                    Default::default(),
                    body.as_bytes(),
                )
                .await;
            assert!(
                matches!(
                    outcome,
                    temporal_runtime::bots::hooks::WebhookIngestOutcome::Admitted { .. }
                ),
                "{outcome:?}"
            );
        }
        // A filtered delivery stores nothing.
        let filtered = api
            .ingest_bot_webhook(
                bot_id.as_str(),
                trigger_id.as_str(),
                &token,
                Default::default(),
                br#"{"kind":"noise"}"#,
            )
            .await;
        assert!(
            matches!(
                filtered,
                temporal_runtime::bots::hooks::WebhookIngestOutcome::Filtered { .. }
            ),
            "{filtered:?}"
        );
        // A wrong token is indistinguishable from an unknown endpoint.
        let probe = api
            .ingest_bot_webhook(
                bot_id.as_str(),
                trigger_id.as_str(),
                "nope",
                Default::default(),
                br#"{"kind":"deploy"}"#,
            )
            .await;
        assert_eq!(
            probe,
            temporal_runtime::bots::hooks::WebhookIngestOutcome::UnknownEndpoint
        );

        let events = wait_for_outcomes(&api, &bot_id, 3).await?;
        assert_eq!(events.len(), 3, "the filtered event was never stored");
        let run_ids: std::collections::BTreeSet<_> = events
            .iter()
            .map(|event| event.run_id.clone().expect("run id"))
            .collect();
        assert_eq!(
            run_ids.len(),
            1,
            "coalesced into one delivery and one run: {events:#?}"
        );
        let snapshot = wait_for_controller(&api, &bot_id, |controller| {
            controller.is_some_and(|snapshot| snapshot.recent_deliveries.len() == 1)
        })
        .await?
        .expect("controller");
        assert_eq!(snapshot.runs_today, 1);
        assert_eq!(snapshot.recent_deliveries[0].seqs.len(), 3);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_budget_parks_pending_events() -> anyhow::Result<()> {
    run_bots_live(Llm::Fake, |api, _client| async move {
        let profile_id = create_profile(&api, "You are a live-test bot.").await?;
        let bot_id = create_bot(
            &api,
            &profile_id,
            |document| document.runs_per_day = Some(1),
            Vec::new(),
        )
        .await?;
        for summary in ["one", "two"] {
            api.admit_bot_event(BotEventAdmitParams {
                bot_id: bot_id.clone(),
                event: manual_event(summary),
            })
            .await?;
        }
        let events = wait_for_outcomes(&api, &bot_id, 1).await?;
        let snapshot = wait_for_controller(&api, &bot_id, |controller| {
            controller.is_some_and(|snapshot| {
                snapshot.controller_status == BotControllerStatus::BudgetExhausted
            })
        })
        .await?
        .expect("controller");
        assert_eq!(snapshot.runs_today, 1);
        assert_eq!(snapshot.pending_deliveries, 1);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.outcome.is_none())
                .count(),
            1,
            "the second event stays pending: {events:#?}"
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_schedule_trigger_reconciles_temporal_schedule() -> anyhow::Result<()> {
    run_bots_live(Llm::Fake, |api, client| async move {
        let profile_id = create_profile(&api, "You are a live-test bot.").await?;
        let bot_id = create_bot(&api, &profile_id, |_| {}, Vec::new()).await?;
        let trigger_id = BotTriggerId::new("nightly");
        let trigger = api
            .put_bot_trigger(BotTriggerPutParams {
                bot_id: bot_id.clone(),
                trigger: BotTriggerInput {
                    trigger_id: trigger_id.clone(),
                    document: BotTriggerDocument {
                        spec: BotTriggerSpec::Schedule {
                            cron: Some("*/5 * * * *".to_owned()),
                            at_ms: None,
                            timezone: "UTC".to_owned(),
                            summary: "check the queue".to_owned(),
                        },
                        filter: None,
                        route: None,
                        coalesce: None,
                        deliver: None,
                        session_close_after_ms: None,
                        enabled: true,
                        escalate_to: None,
                    },
                    pairing_code: None,
                },
                expected_revision: None,
            })
            .await?
            .result
            .trigger;
        assert_eq!(trigger.revision, 1);

        let schedule_id = bot_schedule_id(live_universe_id()?, &bot_id, &trigger_id);
        let handle = client.get_schedule_handle(schedule_id.clone());
        let described = handle.describe(Default::default()).await?;
        assert!(!described.paused());
        assert!(
            !described.future_action_times().is_empty(),
            "cron schedule has upcoming fires"
        );

        // A manual trigger of the Schedule admits one schedule event.
        handle
            .trigger(
                temporalio_client::schedules::ScheduleOverlapPolicy::AllowAll,
                Default::default(),
            )
            .await?;
        let events = wait_for_outcomes(&api, &bot_id, 1).await?;
        assert_eq!(events[0].kind, "schedule");
        assert_eq!(events[0].trigger_id.as_ref(), Some(&trigger_id));

        // Disabling the bot pauses its schedules; deleting the trigger drops it.
        let bot = api
            .read_bot(BotReadParams {
                bot_id: bot_id.clone(),
            })
            .await?
            .result
            .bot;
        let mut document = bot.document.clone();
        document.enabled = false;
        api.put_bot(BotPutParams {
            bot: BotInput {
                bot_id: bot_id.clone(),
                document,
            },
            expected_revision: Some(bot.revision),
        })
        .await?;
        assert!(handle.describe(Default::default()).await?.paused());

        api.delete_bot_trigger(BotTriggerDeleteParams {
            bot_id: bot_id.clone(),
            trigger_id: trigger_id.clone(),
        })
        .await?;
        assert!(
            handle.describe(Default::default()).await.is_err(),
            "schedule deleted with the trigger"
        );
        // Disabling reconciles the main session; let it finish before the
        // workers stop.
        wait_for_controller_settled(&api, &client, &bot_id, |controller| !controller.enabled)
            .await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_close_and_delete_preserve_history_and_recreate() -> anyhow::Result<()> {
    run_bots_live(Llm::Fake, |api, _client| async move {
        let profile_id = create_profile(&api, "You are a live-test bot.").await?;
        let bot_id = create_bot(&api, &profile_id, |_| {}, Vec::new()).await?;
        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("before close"),
        })
        .await?;
        wait_for_outcomes(&api, &bot_id, 1).await?;
        let main_session = bot_main_session_id(&bot_id, 1);

        let closed = api
            .close_bot(BotCloseParams {
                bot_id: bot_id.clone(),
            })
            .await?
            .result
            .bot;
        assert!(closed.closed_at_ms.is_some());
        assert!(!closed.document.enabled);

        // The controller tears down and completes: the query stops answering.
        wait_for_controller(&api, &bot_id, |controller| {
            controller.is_none()
                || controller.is_some_and(|snapshot| {
                    snapshot.controller_status == BotControllerStatus::Closed
                })
        })
        .await?;
        let started = Instant::now();
        loop {
            let bot = api
                .read_bot(BotReadParams {
                    bot_id: bot_id.clone(),
                })
                .await?
                .result
                .bot;
            if bot.closed_sessions.contains(&main_session) {
                break;
            }
            if started.elapsed() > WAIT {
                anyhow::bail!("closed sessions never recorded: {bot:#?}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let session = api
            .read_session(SessionReadParams {
                session_id: main_session.clone(),
                run_limit: None,
            })
            .await?
            .result
            .session;
        assert_eq!(session.status, SessionStatus::Closed);

        // Events admitted after close are refused on the row.
        let refused = api
            .admit_bot_event(BotEventAdmitParams {
                bot_id: bot_id.clone(),
                event: manual_event("after close"),
            })
            .await;
        assert!(refused.is_err(), "closed bots refuse events");

        // Bot sessions are shared with their universe.
        assert_eq!(session.access.visibility, api::Visibility::Universe);
        let deleted = api
            .delete_bot(BotDeleteParams {
                bot_id: bot_id.clone(),
            })
            .await?
            .result;
        assert!(deleted.deleted_sessions.is_empty());
        assert!(
            api.read_bot(BotReadParams {
                bot_id: bot_id.clone()
            })
            .await
            .is_err()
        );
        let retained = api
            .read_session(SessionReadParams {
                session_id: main_session.clone(),
                run_limit: None,
            })
            .await?
            .result
            .session;
        assert_eq!(retained.status, SessionStatus::Closed);
        assert_eq!(retained.access.visibility, api::Visibility::Universe);
        api.create_bot(BotCreateParams {
            bot: BotInput {
                bot_id: bot_id.clone(),
                document: bot_document(&profile_id),
            },
            triggers: vec![],
        })
        .await?;
        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("new incarnation"),
        })
        .await?;
        wait_for_outcomes(&api, &bot_id, 1).await?;
        let successor = api
            .read_session(SessionReadParams {
                session_id: bot_main_session_id(&bot_id, 2),
                run_limit: None,
            })
            .await?
            .result
            .session;
        assert_ne!(successor.id, retained.id);
        api.delete_bot(BotDeleteParams { bot_id }).await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local stack and OPENAI_API_KEY (costs real money)"]
async fn bots_live_real_model_resolves_delivery() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    require_openai_live_env()?;
    run_bots_live(Llm::Real, |api, _client| async move {
        let profile_id = create_profile(
            &api,
            "You are a bot in a live integration test. For every event delivered to you, call bot_event_resolve exactly once with outcome handled and a one-line summary, then stop.",
        )
        .await?;
        let bot_id = create_bot(
            &api,
            &profile_id,
            |document| {
                document.breaker = Some(BotBreaker {
                    fires: 100,
                    window_ms: 60_000,
                });
            },
            Vec::new(),
        )
        .await?;
        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("A customer asked whether the API supports webhooks."),
        })
        .await?;
        let events = wait_for_outcomes(&api, &bot_id, 1).await?;
        let event = &events[0];
        assert_eq!(
            event.outcome,
            Some(BotEventOutcome::Handled),
            "the model resolves the delivery: {event:#?}"
        );
        assert!(event.outcome_detail.is_some(), "summary recorded: {event:#?}");
        Ok(())
    })
    .await
}

/// A program that asks people about each delivery and lets their answer
/// decide it.
fn asking_program_input(profile_id: &ProfileId) -> AgentProfileInput {
    let mut profile = program_profile_input(profile_id, 1);
    let program = profile.document.program.as_mut().expect("program");
    program.source = api::ProgramSource::Text {
        text: "export default { async run(input, env) { \
               const note = input.events[0].event.data.note; \
               const answer = await env.input.request({ ask: 'Act on ' + note + '?' }); \
               return { outcome: answer.approved ? 'handled' : 'ignored', \
               summary: answer.comment ?? note, version: 1, seqs: input.events.map(e => e.seq) }; } };"
            .to_owned(),
    };
    program.requires = vec![api::ProgramContractRequirement {
        contract: "input.request".to_owned(),
        digest: None,
    }];
    profile
}

/// Poll the bot's events until `done` holds for them.
async fn wait_for_events(
    api: &GatewayAgentApi,
    bot_id: &BotId,
    what: &str,
    done: impl Fn(&[BotEventView]) -> bool,
) -> anyhow::Result<Vec<BotEventView>> {
    let started = Instant::now();
    loop {
        let events = list_events(api, bot_id).await?;
        if done(&events) {
            return Ok(events);
        }
        anyhow::ensure!(
            started.elapsed() < WAIT,
            "timed out waiting for {what} on bot {bot_id}: {events:#?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The one pending inbox request of a session, once it appears.
async fn pending_request(
    api: &GatewayAgentApi,
    session_id: &str,
) -> anyhow::Result<api::InputRequestView> {
    let started = Instant::now();
    loop {
        let requests = api
            .list_inbox_requests(api::InboxRequestListParams {
                status: Some(api::InputRequestStatus::Pending),
                session: Some(session_id.to_owned()),
                limit: None,
            })
            .await?
            .result
            .requests;
        if let [request] = requests.as_slice() {
            return Ok(request.clone());
        }
        anyhow::ensure!(
            started.elapsed() < WAIT,
            "no single pending request on {session_id}: {requests:#?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn event_seq(events: &[BotEventView], seq: u64) -> &BotEventView {
    events.iter().find(|event| event.seq == seq).expect("event")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_program_waiting_on_people_defers_its_delivery() -> anyhow::Result<()> {
    run_bots_live(Llm::Program, |api, _client| async move {
        let profile_id = ProfileId::new(unique("bots-live-asking"));
        api.create_profile(ProfileCreateParams {
            profile: asking_program_input(&profile_id),
        })
        .await?;
        let bot_id = create_bot(&api, &profile_id, |_| {}, Vec::new()).await?;
        let main = bot_main_session_id(&bot_id, 1);

        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("first"),
        })
        .await?;
        // While the program waits for its answer, the delivery is deferred.
        let events = wait_for_events(&api, &bot_id, "a deferred delivery", |events| {
            events.iter().any(|event| event.outcome == Some(BotEventOutcome::Deferred))
        })
        .await?;
        let first = event_seq(&events, 1);
        assert_eq!(
            first.outcome_detail.as_deref(),
            Some("waiting for people to answer")
        );
        assert!(first.run_id.is_some(), "{first:?}");
        let request = pending_request(&api, &main).await?;
        assert_eq!(request.ask, "Act on first?");

        // A later event waits for the session instead of failing.
        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("second"),
        })
        .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let events = list_events(&api, &bot_id).await?;
        assert_eq!(event_seq(&events, 2).outcome, None, "{events:#?}");

        // The answer completes the run; its verdict replaces the deferral.
        api.answer_inbox_request(api::InboxRequestAnswerParams {
            request_id: request.request_id,
            approved: false,
            value: None,
            comment: Some("not now".to_owned()),
        })
        .await?;
        let events = wait_for_events(&api, &bot_id, "the first verdict", |events| {
            events.iter().any(|event| {
                event.seq == 1 && event.outcome == Some(BotEventOutcome::Ignored)
            })
        })
        .await?;
        let first = event_seq(&events, 1);
        assert_eq!(first.outcome_detail.as_deref(), Some("not now"));

        // Then the second delivery runs and asks in turn.
        let request = pending_request(&api, &main).await?;
        assert_eq!(request.ask, "Act on second?");
        api.answer_inbox_request(api::InboxRequestAnswerParams {
            request_id: request.request_id,
            approved: true,
            value: None,
            comment: None,
        })
        .await?;
        let events = wait_for_events(&api, &bot_id, "the second verdict", |events| {
            events.iter().any(|event| {
                event.seq == 2 && event.outcome == Some(BotEventOutcome::Handled)
            })
        })
        .await?;
        assert_eq!(event_seq(&events, 2).outcome_detail.as_deref(), Some("second"));
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_superseding_trigger_cancels_a_waiting_program_run() -> anyhow::Result<()> {
    run_bots_live(Llm::Program, |api, _client| async move {
        let profile_id = ProfileId::new(unique("bots-live-asking"));
        api.create_profile(ProfileCreateParams {
            profile: asking_program_input(&profile_id),
        })
        .await?;
        let trigger_id = BotTriggerId::new("hook");
        let bot_id = create_bot(
            &api,
            &profile_id,
            |_| {},
            vec![BotTriggerInput {
                trigger_id: trigger_id.clone(),
                document: BotTriggerDocument {
                    spec: BotTriggerSpec::Webhook {
                        verification: WebhookVerification::Token,
                        preset: None,
                    },
                    filter: None,
                    route: Some(api::BotTriggerRoute::PerKey {
                        key: Some("data.key".to_owned()),
                    }),
                    coalesce: None,
                    deliver: Some(api::BotDeliverPolicy {
                        when_busy: api::BotWhenBusy::Queue,
                        while_waiting: api::BotWhileWaiting::Supersede,
                    }),
                    session_close_after_ms: None,
                    enabled: true,
                    escalate_to: None,
                },
                pairing_code: None,
            }],
        )
        .await?;
        let ingest_path = api
            .read_bot_trigger(api::BotTriggerReadParams {
                bot_id: bot_id.clone(),
                trigger_id: trigger_id.clone(),
            })
            .await?
            .result
            .trigger
            .ingest_path
            .expect("webhook ingest path");
        let token = ingest_path.rsplit('/').next().expect("token").to_owned();
        let ingest = |note: &'static str| {
            let api = api.clone();
            let bot_id = bot_id.clone();
            let trigger_id = trigger_id.clone();
            let token = token.clone();
            async move {
                let body = serde_json::json!({ "kind": "pr", "key": "a", "note": note });
                let outcome = api
                    .ingest_bot_webhook(
                        bot_id.as_str(),
                        trigger_id.as_str(),
                        &token,
                        Default::default(),
                        body.to_string().as_bytes(),
                    )
                    .await;
                assert!(
                    matches!(
                        outcome,
                        temporal_runtime::bots::hooks::WebhookIngestOutcome::Admitted { .. }
                    ),
                    "{outcome:?}"
                );
            }
        };
        let keyed = bots::ids::bot_keyed_session_id(&bot_id, "a");

        ingest("one").await;
        let first = pending_request(&api, &keyed).await?;
        assert_eq!(first.ask, "Act on one?");
        wait_for_events(&api, &bot_id, "a deferred delivery", |events| {
            events
                .iter()
                .any(|event| event.outcome == Some(BotEventOutcome::Deferred))
        })
        .await?;

        // A new event for the key replaces the run waiting on its question.
        ingest("two").await;
        let events = wait_for_events(&api, &bot_id, "the superseded delivery", |events| {
            events
                .iter()
                .any(|event| event.seq == 1 && event.outcome == Some(BotEventOutcome::Archived))
        })
        .await?;
        assert_eq!(
            event_seq(&events, 1).outcome_detail.as_deref(),
            Some("superseded by a later event")
        );
        let second = pending_request(&api, &keyed).await?;
        assert_eq!(second.ask, "Act on two?");
        let withdrawn = api
            .read_inbox_request(api::InboxRequestReadParams {
                request_id: first.request_id,
            })
            .await?
            .result
            .request;
        assert_eq!(withdrawn.status, api::InputRequestStatus::Cancelled);

        api.answer_inbox_request(api::InboxRequestAnswerParams {
            request_id: second.request_id,
            approved: true,
            value: None,
            comment: None,
        })
        .await?;
        wait_for_events(&api, &bot_id, "the second verdict", |events| {
            events
                .iter()
                .any(|event| event.seq == 2 && event.outcome == Some(BotEventOutcome::Handled))
        })
        .await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal + PostgreSQL stack (source scripts/dev/env.sh)"]
async fn bots_live_program_questions_escalate_to_the_trigger_else_the_bot() -> anyhow::Result<()> {
    run_bots_live(Llm::Program, |api, _client| async move {
        let profile_id = ProfileId::new(unique("bots-live-asking"));
        api.create_profile(ProfileCreateParams {
            profile: asking_program_input(&profile_id),
        })
        .await?;
        let trigger_id = BotTriggerId::new("hook");
        let bot_id = create_bot(
            &api,
            &profile_id,
            |bot| bot.escalate_to = vec!["oncall".to_owned()],
            vec![BotTriggerInput {
                trigger_id: trigger_id.clone(),
                document: BotTriggerDocument {
                    spec: BotTriggerSpec::Webhook {
                        verification: WebhookVerification::Token,
                        preset: None,
                    },
                    filter: None,
                    route: Some(api::BotTriggerRoute::PerKey {
                        key: Some("data.key".to_owned()),
                    }),
                    coalesce: None,
                    deliver: None,
                    session_close_after_ms: None,
                    enabled: true,
                    escalate_to: Some(vec!["lead".to_owned()]),
                },
                pairing_code: None,
            }],
        )
        .await?;
        let answer = |request: api::InputRequestView| {
            let api = api.clone();
            async move {
                api.answer_inbox_request(api::InboxRequestAnswerParams {
                    request_id: request.request_id,
                    approved: true,
                    value: None,
                    comment: None,
                })
                .await
            }
        };

        // A manual event has no trigger: its question takes the bot's list.
        api.admit_bot_event(BotEventAdmitParams {
            bot_id: bot_id.clone(),
            event: manual_event("manual"),
        })
        .await?;
        let request = pending_request(&api, &bot_main_session_id(&bot_id, 1)).await?;
        assert_eq!(request.escalate_to, vec!["oncall".to_owned()]);
        answer(request).await?;

        // A trigger's own list wins over the bot's.
        let ingest_path = api
            .read_bot_trigger(api::BotTriggerReadParams {
                bot_id: bot_id.clone(),
                trigger_id: trigger_id.clone(),
            })
            .await?
            .result
            .trigger
            .ingest_path
            .expect("webhook ingest path");
        let token = ingest_path.rsplit('/').next().expect("token").to_owned();
        let body = serde_json::json!({ "kind": "pr", "key": "a", "note": "hooked" });
        let outcome = api
            .ingest_bot_webhook(
                bot_id.as_str(),
                trigger_id.as_str(),
                &token,
                Default::default(),
                body.to_string().as_bytes(),
            )
            .await;
        assert!(
            matches!(
                outcome,
                temporal_runtime::bots::hooks::WebhookIngestOutcome::Admitted { .. }
            ),
            "{outcome:?}"
        );
        let keyed = bots::ids::bot_keyed_session_id(&bot_id, "a");
        let request = pending_request(&api, &keyed).await?;
        assert_eq!(request.escalate_to, vec!["lead".to_owned()]);
        answer(request).await?;
        // Deferred counts as an outcome; wait for both runs to finish.
        wait_for_events(&api, &bot_id, "both verdicts", |events| {
            events.len() == 2
                && events
                    .iter()
                    .all(|event| event.outcome == Some(BotEventOutcome::Handled))
        })
        .await?;
        Ok(())
    })
    .await
}
