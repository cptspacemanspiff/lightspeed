//! A program session runs its pinned program on the code queue end to end:
//! a profile with a program, a session created from it, a program run with
//! JSON input whose scoped calls use the session's real tools, and a reply
//! that completes the run with the program's output. No model is involved.

#[path = "support/programs.rs"]
mod programs;
mod support;

use std::{sync::Arc, time::Duration};

use api::AgentApiService;
use async_trait::async_trait;
use harness::{
    BlobRef, CoreAgentIoError, CoreAgentLlm, LlmGenerationRequest, LlmGenerationResult, SessionId,
    storage::{BlobStore, SessionStore},
};
use serde_json::{Value, json};
use support::live::{
    LIVE_TEST_LOCK, live_universe_id, read_session_view, require_storage_live_env,
    run_with_live_worker_builder, seed_agent_default, wait_for_terminal_run,
};
use temporal_runtime::{
    gateway::GatewayAgentApi,
    pg_store_from_env,
    program::ProgramRunner,
    worker::{
        ActivityState, ProgramWorkerActivities, SessionTools, WorkerActivities, program_worker,
        worker_runtime,
    },
};
use temporal_workflow::{ACTIVITY_PROGRAM_ADMIT, ACTIVITY_PROGRAM_LOAD};
use temporalio_common::error::ApplicationFailure;
use temporalio_macros::activities;
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};
use tools::human_input::{
    INPUT_DEFAULT_DEADLINE_ENV, INPUT_MIN_DEADLINE_ENV, INPUT_MIN_REMIND_AFTER_ENV, InputTimeouts,
};

/// A program session never plans a model turn, so any generation is a bug.
struct NoModel;

#[async_trait]
impl CoreAgentLlm for NoModel {
    async fn generate(
        &self,
        request: LlmGenerationRequest,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
        Err(CoreAgentIoError::Failed {
            message: format!(
                "program sessions must not generate; run {} turn {}",
                request.run_id, request.turn_id
            ),
        })
    }
}

const PROGRAM: &str = r#"
    export default {
        async run(input, env, ctx) {
            const first = await env.time.sleep({ ms: 5 });
            const both = await Promise.all([3, 4].map(ms => env.time.sleep({ ms })));
            const fired = await new Promise(resolve => setTimeout(() => resolve(Date.now()), 2));
            return {
                claim: input.claim,
                slept: 1 + both.length,
                clock: fired >= first.fire_at_ms && first.fire_at_ms >= ctx.startedAt,
                hasOnlyItsContracts: Object.keys(env).join(","),
            };
        }
    };
"#;

fn program_profile(
    profile_id: &api::ProfileId,
    source: &str,
    requires: &[(&str, Option<&str>)],
) -> api::AgentProfileInput {
    api::AgentProfileInput {
        profile_id: profile_id.clone(),
        display_name: Some("Live program".to_owned()),
        description: None,
        document: api::ProfileDocument {
            program: Some(api::ProgramSpec {
                source: api::ProgramSource::Text {
                    text: source.to_owned(),
                },
                semantics_version: 1,
                input_schema: Some(json!({
                    "type": "object",
                    "required": ["claim"],
                    "properties": {"claim": {"type": "string"}},
                })),
                output_schema: Some(json!({
                    "type": "object",
                    "required": ["claim", "slept"],
                })),
                limits: None,
                requires: requires
                    .iter()
                    .map(|(contract, digest)| api::ProgramContractRequirement {
                        contract: (*contract).to_owned(),
                        digest: digest.map(str::to_owned),
                    })
                    .collect(),
                approvals: None,
            }),
            config: Some(api::SessionConfig {
                features: Some(api::FeaturesConfig {
                    timers: Some(api::TimersFeature {
                        version: api::CURRENT_FEATURE_VERSION,
                    }),
                    ..api::FeaturesConfig::default()
                }),
                ..api::SessionConfig::default()
            }),
            instructions: None,
            metadata: Default::default(),
            retention: None,
        },
    }
}

async fn start_program_session(
    api: &GatewayAgentApi,
    session_id: &SessionId,
    profile_id: &api::ProfileId,
) -> anyhow::Result<api::SessionView> {
    api.start_session(api::SessionStartParams {
        session_id: Some(session_id.to_string()),
        display_name: None,
        access: None,
        metadata: Default::default(),
        profile: Some(api::ProfileSource::Named {
            profile_id: profile_id.clone(),
        }),
        delete_after_close_ms: None,
        config: None,
    })
    .await?;
    read_session_view(api, session_id).await
}

async fn start_program_run(
    api: &GatewayAgentApi,
    session_id: &SessionId,
    input: Value,
) -> Result<api::RunView, api::AgentApiError> {
    Ok(api
        .start_run(api::RunStartParams {
            notify_on_terminal: None,
            submission_id: None,
            session_id: session_id.to_string(),
            source: api::RunStartSource::Program {
                input,
                input_defaults: None,
            },
            config: None,
        })
        .await?
        .result
        .run)
}

/// The failure recorded on the session's last failed run.
async fn run_failure(
    store: &store_pg::PgStore,
    session_id: &SessionId,
) -> anyhow::Result<harness::RunFailure> {
    let mut after = None;
    let mut failure = None;
    loop {
        let page = store
            .read_after(harness::storage::ReadSessionEvents {
                session_id: session_id.clone(),
                after,
                limit: 1000,
            })
            .await?;
        for entry in page.entries {
            if let harness::CoreAgentEvent::Run(harness::RunEvent::Failed {
                failure: found, ..
            }) = harness::CoreAgentCodec.decode_event(&entry.event)?
            {
                failure = Some(found);
            }
        }
        if page.complete {
            return failure.ok_or_else(|| anyhow::anyhow!("no failed run recorded"));
        }
        after = page.next_after;
    }
}

/// The tool ids of every scoped call admitted on the session, in log order.
async fn scoped_call_tools(
    store: &store_pg::PgStore,
    session_id: &SessionId,
) -> anyhow::Result<Vec<String>> {
    let mut after = None;
    let mut tools = Vec::new();
    loop {
        let page = store
            .read_after(harness::storage::ReadSessionEvents {
                session_id: session_id.clone(),
                after,
                limit: 1000,
            })
            .await?;
        for entry in page.entries {
            if let harness::CoreAgentEvent::CodeTool(harness::CodeToolEvent::CallAdmitted {
                call,
                ..
            }) = harness::CoreAgentCodec.decode_event(&entry.event)?
            {
                tools.push(call.tool_id.as_str().to_owned());
            }
        }
        if page.complete {
            return Ok(tools);
        }
        after = page.next_after;
    }
}

async fn read_json(store: &store_pg::PgStore, reference: &str) -> anyhow::Result<Value> {
    Ok(serde_json::from_slice(
        &store.read_bytes(&BlobRef::parse(reference)?).await?,
    )?)
}

type ProgramTestBody = std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>>>>;

/// Run `body` against a gateway with a session worker and a program worker on
/// fresh queues, with no model and the given human-input timeouts.
async fn with_program_workers(
    input_timeouts: InputTimeouts,
    body: impl FnOnce(Arc<GatewayAgentApi>, Arc<store_pg::PgStore>, SessionId) -> ProgramTestBody,
) -> anyhow::Result<()> {
    with_program_workers_and_client(input_timeouts, |api, store, session_id, _| {
        body(api, store, session_id)
    })
    .await
}

/// A `program_admit` that admits a step for real and then fails its first
/// attempt, as if the worker had lost the result after the session accepted
/// the calls. Records each attempt it ran.
pub struct LosesFirstAdmit {
    runner: ProgramRunner,
    attempts: Arc<std::sync::Mutex<Vec<u32>>>,
}

#[activities]
impl LosesFirstAdmit {
    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_ADMIT)]
    pub async fn program_admit(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramAdmitActivityRequest,
    ) -> Result<temporal_workflow::ProgramAdmitActivityResult, ActivityError> {
        let attempt = ctx.info().attempt;
        self.attempts.lock().expect("attempts lock").push(attempt);
        let rejected = self
            .runner
            .admit_step(&request.descriptor, request.requests)
            .await
            .map_err(|error| {
                ActivityError::application(ApplicationFailure::non_retryable(anyhow::anyhow!(
                    error.to_string()
                )))
            })?;
        if attempt == 1 {
            return Err(ActivityError::application(ApplicationFailure::new(
                anyhow::anyhow!("admitted, but the result was lost"),
            )));
        }
        Ok(temporal_workflow::ProgramAdmitActivityResult { rejected })
    }
}

/// [`with_program_workers`] whose `body` also gets the Temporal client.
async fn with_program_workers_and_client(
    input_timeouts: InputTimeouts,
    body: impl FnOnce(
        Arc<GatewayAgentApi>,
        Arc<store_pg::PgStore>,
        SessionId,
        temporalio_client::Client,
    ) -> ProgramTestBody,
) -> anyhow::Result<()> {
    with_program_workers_admitting(input_timeouts, None, body).await
}

/// [`with_program_workers_and_client`] whose program worker, given
/// `lost_admits`, runs [`LosesFirstAdmit`] in place of `program_admit`.
async fn with_program_workers_admitting(
    input_timeouts: InputTimeouts,
    lost_admits: Option<Arc<std::sync::Mutex<Vec<u32>>>>,
    body: impl FnOnce(
        Arc<GatewayAgentApi>,
        Arc<store_pg::PgStore>,
        SessionId,
        temporalio_client::Client,
    ) -> ProgramTestBody,
) -> anyhow::Result<()> {
    let _lock = LIVE_TEST_LOCK.lock().await;
    require_storage_live_env()?;
    let store = pg_store_from_env().await?;
    seed_agent_default(&store, &support::live::openai_live_model()).await?;
    let program_queue = format!("program-live-{}", uuid::Uuid::new_v4().simple());
    let activity_queue = program_queue.clone();
    let activity_store = store.clone();
    run_with_live_worker_builder(
        move |client, _| async move {
            let tools = Arc::new(SessionTools::from_pg_store(activity_store.clone()));
            Ok(WorkerActivities::for_universe(
                activity_store.config().universe_id,
                ActivityState::from_pg_store(
                    activity_store.clone(),
                    Arc::new(NoModel),
                    tools.clone(),
                )
                .with_hosted_tools(tools)
                .with_workflow_tool_executions(client)
                .with_program_task_queue(activity_queue)
                .with_input_timeouts(input_timeouts)
                .with_native_mcp_from_pg_store(activity_store)?,
            ))
        },
        move |client, queue, session_id| async move {
            let api = Arc::new(
                GatewayAgentApi::builder(client.clone(), store.clone())
                    .with_task_queue(queue)
                    .with_program_task_queue(program_queue.clone())
                    .with_input_timeouts(input_timeouts)
                    .build(),
            );
            let activities = ProgramWorkerActivities::for_universe(live_universe_id()?, api.clone());
            let runtime = worker_runtime()?;
            let mut worker = match lost_admits {
                None => program_worker(&runtime, client.clone(), program_queue, activities)?,
                // A later registration under the same name replaces the
                // worker's own `program_admit`.
                Some(attempts) => temporalio_sdk::Worker::new(
                    &runtime,
                    client.clone(),
                    temporalio_sdk::WorkerOptions::new(program_queue)
                        .register_workflow::<temporal_workflow::ProgramRunWorkflow>()?
                        .register_activities(activities)
                        .register_activities(LosesFirstAdmit {
                            runner: ProgramRunner::new(
                                client.clone(),
                                store.clone(),
                                Duration::from_secs(10),
                            ),
                            attempts,
                        })
                        .build(),
                )?,
            };
            let shutdown = worker.shutdown_handle();
            let worker_run = worker.run();
            tokio::pin!(worker_run);
            let result = tokio::select! {
                result = body(api, store, session_id, client.clone()) => result,
                result = &mut worker_run => Err(anyhow::anyhow!("program worker stopped early: {result:?}")),
            };
            shutdown();
            let stopped = tokio::time::timeout(Duration::from_secs(15), &mut worker_run).await;
            result?;
            stopped.map_err(|_| anyhow::anyhow!("program worker did not stop"))??;
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn program_session_runs_its_pinned_program_and_returns_its_output() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                api.create_profile(api::ProfileCreateParams {
                    profile: program_profile(&profile_id, PROGRAM, &[("time.sleep", None)]),
                })
                .await?;

                // Contracts are admitted when the session is created: one the
                // session's configuration does not grant, or one pinned to a
                // different digest, refuses the session.
                for (index, requires) in [
                    [("vfs.read_file", None)],
                    [("time.sleep", Some("contract-sha256:0000000000000000000000000000000000000000000000000000000000000000"))],
                ]
                .into_iter()
                .enumerate()
                {
                    let refused_id = api::ProfileId::new(format!(
                        "live_program_{}",
                        uuid::Uuid::new_v4().simple()
                    ));
                    api.create_profile(api::ProfileCreateParams {
                        profile: program_profile(&refused_id, PROGRAM, &requires),
                    })
                    .await?;
                    let refused = start_program_session(
                        &api,
                        &SessionId::new(format!("{session_id}-refused-{index}")),
                        &refused_id,
                    )
                    .await;
                    assert!(
                        refused.as_ref().is_err_and(|error| {
                            matches!(
                                error.downcast_ref::<api::AgentApiError>().map(|error| &error.kind),
                                Some(api::AgentApiErrorKind::InvalidRequest)
                            )
                        }),
                        "{requires:?} must refuse the session: {refused:?}"
                    );
                    api.delete_profile(api::ProfileDeleteParams {
                        profile_id: refused_id,
                    })
                    .await?;
                }

                let session = start_program_session(&api, &session_id, &profile_id).await?;
                assert_eq!(
                    session.driver,
                    api::SessionDriverView::Program {
                        program_id: Some(profile_id.as_str().to_owned()),
                        revision: Some(1),
                        semantics_version: 1,
                    }
                );
                assert!(
                    session.active_context.entries.is_empty(),
                    "a program session has no instructions: {:?}",
                    session.active_context.entries
                );

                // Input is checked against the pinned schema before a run exists.
                let rejected = start_program_run(&api, &session_id, json!({"wrong": 1})).await;
                assert!(
                    rejected.is_err(),
                    "input violating the schema must be rejected: {rejected:?}"
                );

                let run = start_program_run(&api, &session_id, json!({"claim": "x"})).await?;
                assert!(matches!(run.source, api::RunViewSource::Program { .. }));
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output = run.output.as_ref().expect("program output");
                assert_eq!(output.media_type.as_deref(), Some("application/json"));
                assert_eq!(
                    read_json(&store, &output.content_ref).await?,
                    json!({"claim": "x", "slept": 3, "clock": true, "hasOnlyItsContracts": "time"})
                );
                // Three sleeps and a timer: each wait is one recorded call.
                assert_eq!(
                    scoped_call_tools(&store, &session_id).await?,
                    vec!["time.sleep"; 4]
                );

                // A thrown program fails the run with a program failure whose
                // message is the failure report.
                let failing_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                api.create_profile(api::ProfileCreateParams {
                    profile: program_profile(
                        &failing_id,
                        "export default { async run(input) { throw new Error('boom ' + input.claim); } };",
                        &[],
                    ),
                })
                .await?;
                let failing_session = SessionId::new(format!("{session_id}-failing"));
                start_program_session(&api, &failing_session, &failing_id).await?;
                let run = start_program_run(&api, &failing_session, json!({"claim": "y"})).await?;
                let run = wait_for_terminal_run(&api, &failing_session, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Failed, "{run:?}");
                let failure = run_failure(&store, &failing_session).await?;
                assert_eq!(failure.kind, harness::RunFailureKind::Program);
                let report = read_json(
                    &store,
                    failure.message_ref.as_ref().expect("failure report").as_str(),
                )
                .await?;
                assert_eq!(report["status"], "threw", "{report}");
                assert!(
                    report["error"].as_str().unwrap_or_default().contains("boom y"),
                    "{report}"
                );

                for id in [&session_id, &failing_session] {
                    let _ = api
                        .close_session(api::SessionCloseParams {
                            session_id: id.to_string(),
                            force: true,
                        })
                        .await;
                }
                api.delete_profile(api::ProfileDeleteParams {
                    profile_id: profile_id.clone(),
                })
                .await?;
                api.delete_profile(api::ProfileDeleteParams {
                    profile_id: failing_id,
                })
                .await?;
                Ok(())
        })
    })
    .await
}

const REMEMBERING_PROGRAM: &str = r#"
    export default {
        async run(input, env, ctx) {
            const before = ctx.context ?? null;
            ctx.setContext({
                round: (before?.round ?? 0) + 1,
                claims: [...(before?.claims ?? []), input.claim],
            });
            if (input.claim === "throw") throw new Error("after setting its context");
            return { claim: input.claim, slept: 0, before };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_reads_the_context_its_last_completed_run_kept() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            let mut profile = program_profile(&profile_id, REMEMBERING_PROGRAM, &[]);
            profile.document.program.as_mut().unwrap().limits = Some(api::ProgramLimitsInput {
                context_retained: Some(2),
                ..Default::default()
            });
            api.create_profile(api::ProfileCreateParams { profile })
                .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            let mut befores = Vec::new();
            for claim in ["a", "b", "throw", "c"] {
                let run = start_program_run(&api, &session_id, json!({ "claim": claim })).await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                if claim == "throw" {
                    // A failed run keeps nothing, though it set a context.
                    assert_eq!(run.status, api::RunStatus::Failed, "{run:?}");
                    continue;
                }
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output = run.output.as_ref().expect("program output");
                befores.push(read_json(&store, &output.content_ref).await?["before"].clone());
            }
            assert_eq!(
                befores,
                [
                    Value::Null,
                    json!({"round": 1, "claims": ["a"]}),
                    json!({"round": 2, "claims": ["a", "b"]}),
                ]
            );

            // Each completed run's context is its own entry; beyond the
            // program's `contextRetained`, the oldest leave the active context.
            let session = read_session_view(&api, &session_id).await?;
            let contexts = session
                .active_context
                .entries
                .iter()
                .filter(|entry| entry.kind == api::ContextEntryKindView::ProgramState)
                .collect::<Vec<_>>();
            assert_eq!(contexts.len(), 2, "{contexts:?}");
            let mut kept = Vec::new();
            for entry in &contexts {
                assert_eq!(
                    entry.content.media_type.as_deref(),
                    Some("application/json")
                );
                kept.push(read_json(&store, &entry.content.content_ref).await?);
            }
            assert_eq!(
                kept,
                [
                    json!({"round": 2, "claims": ["a", "b"]}),
                    json!({"round": 3, "claims": ["a", "b", "c"]}),
                ]
            );
            assert!(
                contexts[1]
                    .preview
                    .as_deref()
                    .is_some_and(|preview| preview.contains("\"round\":3")),
                "{contexts:?}"
            );

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            api.delete_profile(api::ProfileDeleteParams { profile_id })
                .await?;
            Ok(())
        })
    })
    .await
}

const ASKING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            if (input.claim === "unanswered") {
                try {
                    await env.input.request({ ask: "Anyone?", deadline_ms: 1000, escalate_to: ["lead"] });
                    return { claim: input.claim, slept: 0, kind: "answered" };
                } catch (error) {
                    return { claim: input.claim, slept: 0, kind: error.kind };
                }
            }
            const answer = await env.input.request({
                ask: "Merge " + input.claim + "?",
                payload: { pr: input.claim },
                assignees: ["alice"],
                response_schema: {
                    type: "object",
                    required: ["label"],
                    properties: { label: { type: "string" } },
                },
            });
            return { claim: input.claim, slept: 0, answer };
        }
    };
"#;

/// The session's requests in the inbox, once `count` exist.
async fn wait_for_requests(
    api: &GatewayAgentApi,
    session_id: &SessionId,
    count: usize,
) -> anyhow::Result<Vec<api::InputRequestView>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let requests = api
            .list_inbox_requests(api::InboxRequestListParams {
                session: Some(session_id.to_string()),
                ..Default::default()
            })
            .await?
            .result
            .requests;
        if requests.len() >= count {
            return Ok(requests);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "no input request appeared for {session_id}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_waits_for_a_human_answer_and_catches_an_expired_request() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(&profile_id, ASKING_PROGRAM, &[("input.request", None)]),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            // The run waits on its question, which the inbox shows.
            let run = start_program_run(&api, &session_id, json!({"claim": "pr-7"})).await?;
            let [request] = <[_; 1]>::try_from(wait_for_requests(&api, &session_id, 1).await?)
                .map_err(|requests| anyhow::anyhow!("one request expected: {requests:?}"))?;
            assert_eq!(request.status, api::InputRequestStatus::Pending);
            assert_eq!(request.ask, "Merge pr-7?");
            assert_eq!(request.payload, Some(json!({"pr": "pr-7"})));
            assert_eq!(request.assignees, vec!["alice".to_owned()]);
            assert!(
                request.deadline_at_ms >= request.created_at_ms + 7 * 24 * 60 * 60 * 1_000,
                "the default deadline is seven days: {request:?}"
            );
            let read = api
                .read_inbox_request(api::InboxRequestReadParams {
                    request_id: request.request_id.clone(),
                })
                .await?
                .result
                .request;
            assert_eq!(read, request);

            // An approval must satisfy the response schema.
            let invalid = api
                .answer_inbox_request(api::InboxRequestAnswerParams {
                    request_id: request.request_id.clone(),
                    approved: true,
                    value: Some(json!({"wrong": 1})),
                    comment: None,
                })
                .await;
            assert!(
                invalid
                    .as_ref()
                    .is_err_and(|error| error.kind == api::AgentApiErrorKind::InvalidRequest),
                "{invalid:?}"
            );
            let answer = |approved: bool| api::InboxRequestAnswerParams {
                request_id: request.request_id.clone(),
                approved,
                value: approved.then(|| json!({"label": "ship"})),
                comment: Some("lgtm".to_owned()),
            };
            let answered = api.answer_inbox_request(answer(true)).await?.result.request;
            assert_eq!(answered.status, api::InputRequestStatus::Answered);
            // The same answer again is accepted; a different one conflicts.
            api.answer_inbox_request(answer(true)).await?;
            let conflict = api.answer_inbox_request(answer(false)).await;
            assert!(
                conflict
                    .as_ref()
                    .is_err_and(|error| error.kind == api::AgentApiErrorKind::Conflict),
                "{conflict:?}"
            );

            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let output =
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
            assert_eq!(output["answer"]["approved"], json!(true), "{output}");
            assert_eq!(
                output["answer"]["value"],
                json!({"label": "ship"}),
                "{output}"
            );
            assert_eq!(output["answer"]["comment"], json!("lgtm"), "{output}");
            assert!(output["answer"]["answered_by"].is_string(), "{output}");
            assert_eq!(
                scoped_call_tools(&store, &session_id).await?,
                vec!["input.request"]
            );

            // Unanswered, the request escalates once with a fresh deadline,
            // then expires; the program catches the failure by its kind.
            let run = start_program_run(&api, &session_id, json!({"claim": "unanswered"})).await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            assert_eq!(
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                json!({"claim": "unanswered", "slept": 0, "kind": "deadline_exceeded"})
            );
            let expired = api
                .list_inbox_requests(api::InboxRequestListParams {
                    status: Some(api::InputRequestStatus::Expired),
                    session: Some(session_id.to_string()),
                    limit: None,
                })
                .await?
                .result
                .requests;
            let [expired] = <[_; 1]>::try_from(expired).map_err(|requests| {
                anyhow::anyhow!("one expired request expected: {requests:?}")
            })?;
            assert!(expired.escalated, "{expired:?}");
            assert_eq!(expired.assignees, vec!["lead".to_owned()]);
            assert!(expired.answer.is_none());

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            api.delete_profile(api::ProfileDeleteParams { profile_id })
                .await?;
            Ok(())
        })
    })
    .await
}

const REMINDED_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            try {
                await env.input.request({ ask: "Anyone?", remind_after_ms: 1000 });
                return { claim: input.claim, slept: 0, kind: "answered" };
            } catch (error) {
                return { claim: input.claim, slept: 0, kind: error.kind };
            }
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn environment_overrides_shorten_input_deadlines_and_reminders() -> anyhow::Result<()> {
    // The overrides as a deployment would read them from its environment.
    let overrides = [
        (INPUT_DEFAULT_DEADLINE_ENV, "5000"),
        (INPUT_MIN_DEADLINE_ENV, "100"),
        (INPUT_MIN_REMIND_AFTER_ENV, "500"),
    ];
    let timeouts = InputTimeouts::from_lookup(|name| {
        overrides
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_owned())
    })
    .map_err(anyhow::Error::msg)?;
    with_program_workers(timeouts, |api, store, session_id| {
        Box::pin(async move {
            // A program default below the stock one-second floor is
            // admitted under the lowered floor.
            let short_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            let mut short =
                program_profile(&short_id, REMINDED_PROGRAM, &[("input.request", None)]);
            if let Some(program) = short.document.program.as_mut() {
                program.limits = Some(api::ProgramLimitsInput {
                    input_deadline_ms: Some(500),
                    ..Default::default()
                });
            }
            api.create_profile(api::ProfileCreateParams { profile: short })
                .await?;
            api.delete_profile(api::ProfileDeleteParams {
                profile_id: short_id,
            })
            .await?;

            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(&profile_id, REMINDED_PROGRAM, &[("input.request", None)]),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            // A reminder interval under the stock hour is accepted, and the
            // overridden default deadline expires the request in seconds.
            let run = start_program_run(&api, &session_id, json!({"claim": "reminded"})).await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            assert_eq!(
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                json!({"claim": "reminded", "slept": 0, "kind": "deadline_exceeded"})
            );
            let [expired] = <[_; 1]>::try_from(wait_for_requests(&api, &session_id, 1).await?)
                .map_err(|requests| anyhow::anyhow!("one request expected: {requests:?}"))?;
            assert_eq!(expired.status, api::InputRequestStatus::Expired);
            assert_eq!(
                expired.deadline_at_ms - expired.created_at_ms,
                5_000,
                "{expired:?}"
            );
            assert_eq!(expired.remind_after_ms, Some(1_000));
            // Local Temporal timers fire up to a second late, so not all four.
            assert!(expired.reminder_count >= 2, "{expired:?}");

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            api.delete_profile(api::ProfileDeleteParams { profile_id })
                .await?;
            Ok(())
        })
    })
    .await
}

const OPENING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const ask = await env.input.open({ ask: "Ship " + input.claim + "?", assignees: ["alice"] });
            if (input.claim === "withdrawn") {
                await ask.cancel();
                try {
                    await ask.answer();
                    return { claim: input.claim, slept: 0, kind: "answered" };
                } catch (error) {
                    return { claim: input.claim, slept: 0, kind: error.kind };
                }
            }
            const answer = await ask.withReminders({ every_ms: 1000, max: 1, then: "reassign", to: ["lead"] });
            return { claim: input.claim, slept: 0, id: ask.id, answer };
        }
    };
"#;

const OPENING_CONTRACTS: &[(&str, Option<&str>)] = &[
    ("input.open", None),
    ("input.remind", None),
    ("input.reassign", None),
    ("promise.await", None),
    ("promise.cancel", None),
    ("time.sleep", None),
];

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn an_opened_request_is_reminded_reassigned_answered_and_withdrawn() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            // Opening a request is no use without awaiting its answer, so
            // a session cannot pin one without the other.
            let incomplete_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(&incomplete_id, OPENING_PROGRAM, &[("input.open", None)]),
            })
            .await?;
            let incomplete = start_program_session(
                &api,
                &SessionId::new(format!("{session_id}-incomplete")),
                &incomplete_id,
            )
            .await;
            assert!(
                incomplete.as_ref().is_err_and(|error| error
                    .downcast_ref::<api::AgentApiError>()
                    .is_some_and(|error| error.kind == api::AgentApiErrorKind::InvalidRequest)),
                "{incomplete:?}"
            );
            api.delete_profile(api::ProfileDeleteParams {
                profile_id: incomplete_id,
            })
            .await?;

            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(&profile_id, OPENING_PROGRAM, OPENING_CONTRACTS),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            // The program reminds alice once, then hands the request to lead.
            let run = start_program_run(&api, &session_id, json!({"claim": "pr-9"})).await?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let request = loop {
                let [request] = <[_; 1]>::try_from(wait_for_requests(&api, &session_id, 1).await?)
                    .map_err(|requests| anyhow::anyhow!("one request expected: {requests:?}"))?;
                if request.assignees == ["lead".to_owned()] {
                    break request;
                }
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "the request was never reassigned: {request:?}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            };
            assert_eq!(request.status, api::InputRequestStatus::Pending);
            assert_eq!(request.ask, "Ship pr-9?");
            assert_eq!(request.reminder_count, 1, "{request:?}");

            api.answer_inbox_request(api::InboxRequestAnswerParams {
                request_id: request.request_id.clone(),
                approved: true,
                value: None,
                comment: None,
            })
            .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let output =
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
            assert_eq!(output["answer"]["approved"], json!(true), "{output}");
            assert!(
                output["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("wtx:")),
                "{output}"
            );
            let tools = scoped_call_tools(&store, &session_id).await?;
            for expected in [
                "input.open",
                "concurrency.await",
                "time.sleep",
                "input.remind",
                "input.reassign",
            ] {
                assert!(tools.iter().any(|tool| tool == expected), "{tools:?}");
            }

            // Cancelling the promise withdraws the request.
            let run = start_program_run(&api, &session_id, json!({"claim": "withdrawn"})).await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            assert_eq!(
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                json!({"claim": "withdrawn", "slept": 0, "kind": "input_cancelled"})
            );
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let cancelled = api
                    .list_inbox_requests(api::InboxRequestListParams {
                        status: Some(api::InputRequestStatus::Cancelled),
                        session: Some(session_id.to_string()),
                        limit: None,
                    })
                    .await?
                    .result
                    .requests;
                if cancelled.len() == 1 {
                    break;
                }
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "the withdrawn request was never cancelled: {cancelled:?}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            api.delete_profile(api::ProfileDeleteParams { profile_id })
                .await?;
            Ok(())
        })
    })
    .await
}

/// A Streamable HTTP MCP server on loopback whose `createIssue` input
/// schema can change between listings, recording every call it serves.
struct McpFixture {
    state: Arc<tokio::sync::Mutex<McpFixtureState>>,
    url: String,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Default)]
struct McpFixtureState {
    /// Paths whose `createIssue` takes a second required argument.
    changed: std::collections::BTreeSet<String>,
    calls: Vec<Value>,
}

impl McpFixture {
    async fn start() -> anyhow::Result<Self> {
        let state = Arc::new(tokio::sync::Mutex::new(McpFixtureState::default()));
        let app = axum::Router::new()
            .route("/{path}", axum::routing::post(mcp_fixture_handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { state, url, task })
    }
}

impl Drop for McpFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn mcp_fixture_handler(
    axum::extract::Path(path): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<Arc<tokio::sync::Mutex<McpFixtureState>>>,
    axum::Json(request): axum::Json<Value>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let reply = |result: Value| {
        axum::Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response()
    };
    match request.get("method").and_then(Value::as_str) {
        Some("initialize") => {
            let mut response = reply(json!({
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "program-mcp-fixture", "version": "1"}
            }));
            response.headers_mut().insert(
                "mcp-session-id",
                axum::http::HeaderValue::from_static("fixture-session"),
            );
            response
        }
        Some("notifications/initialized") => axum::http::StatusCode::ACCEPTED.into_response(),
        Some("tools/list") => {
            let mut required = vec!["title"];
            if state.lock().await.changed.contains(&path) {
                required.push("labels");
            }
            let schema = |required: &[&str]| {
                json!({
                    "type": "object",
                    "properties": {"title": {"type": "string"}, "labels": {"type": "array"}},
                    "required": required,
                })
            };
            reply(json!({"tools": [
                {"name": "createIssue", "inputSchema": schema(&required)},
                {"name": "get-file", "inputSchema": schema(&[])},
                {"name": "get_file", "inputSchema": schema(&[])},
            ]}))
        }
        Some("tools/call") => {
            let params = request.get("params").cloned().unwrap_or(Value::Null);
            let title = params
                .pointer("/arguments/title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            state.lock().await.calls.push(json!({
                "name": params.get("name"),
                "arguments": params.get("arguments"),
            }));
            reply(json!({
                "content": [{"type": "text", "text": format!("created {title}")}],
                "structuredContent": {"number": 7, "title": title},
            }))
        }
        _ => axum::http::StatusCode::BAD_REQUEST.into_response(),
    }
}

const MCP_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const created = await env.mcp[input.server].create_issue({ title: input.claim });
            return {
                claim: input.claim,
                slept: 0,
                created,
                tools: Object.keys(env.mcp[input.server]),
            };
        }
    };
"#;

/// A program profile requiring `contracts`, on a session with `server_id`
/// attached.
async fn mcp_program_profile(
    api: &GatewayAgentApi,
    fixture: &McpFixture,
    server_id: &str,
    contracts: &[&str],
) -> anyhow::Result<api::ProfileId> {
    mcp_program_profile_with(
        api,
        fixture,
        server_id,
        contracts,
        MCP_PROGRAM,
        api::RemoteMcpApprovalPolicy::Never,
        None,
    )
    .await
}

async fn mcp_program_profile_with(
    api: &GatewayAgentApi,
    fixture: &McpFixture,
    server_id: &str,
    contracts: &[&str],
    source: &str,
    approval: api::RemoteMcpApprovalPolicy,
    approvals: Option<api::ProgramApprovals>,
) -> anyhow::Result<api::ProfileId> {
    api.put_mcp_server(api::McpServerPutParams {
        server: api::McpServerInput {
            server_id: server_id.to_owned(),
            display_name: None,
            server_url: format!("{}/{server_id}", fixture.url),
            default_server_label: server_id.to_owned(),
            description: None,
            allowed_tools: None,
            execution: api::RemoteMcpExecution::Native,
            exposure: api::RemoteMcpExposure::Inject,
            approval,
            defer_loading: None,
            allow_private_network: true,
            auth_policy: api::McpServerAuthPolicy::None,
            credential: None,
            status: api::McpServerStatus::Active,
        },
        expected_revision: None,
    })
    .await?;
    let profile_id = api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
    let requires = contracts
        .iter()
        .map(|contract| (*contract, None))
        .collect::<Vec<_>>();
    let mut profile = program_profile(&profile_id, source, &requires);
    if let Some(program) = profile.document.program.as_mut() {
        program.approvals = approvals;
        program.input_schema = Some(json!({
            "type": "object",
            "required": ["claim", "server"],
            "properties": {"claim": {"type": "string"}, "server": {"type": "string"}},
        }));
    }
    let features = profile
        .document
        .config
        .as_mut()
        .and_then(|config| config.features.as_mut())
        .expect("program profile features");
    features.mcp = Some(api::McpFeature {
        version: api::CURRENT_FEATURE_VERSION,
        servers: vec![api::McpServerAttachment {
            server_id: server_id.to_owned(),
            tools: None,
        }],
    });
    api.create_profile(api::ProfileCreateParams { profile })
        .await?;
    Ok(profile_id)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL, and LIGHTSPEED_MCP_PRIVATE_NETWORKS allowing 127.0.0.1; run serially"]
async fn a_program_calls_an_mcp_tool_by_contract_and_fails_when_its_schema_changes()
-> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let fixture = McpFixture::start().await?;
            let stable = format!(
                "prog_mcp_{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            );
            let drifting = format!(
                "prog_mcp_{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            );
            let create = |server: &str| format!("mcp.{server}.create_issue");

            // `get-file` and `get_file` both normalize to `get_file`, so
            // neither has a contract and requiring one refuses the session.
            let clashing = mcp_program_profile(
                &api,
                &fixture,
                &stable,
                &[&format!("mcp.{stable}.get_file")],
            )
            .await?;
            let refused = start_program_session(
                &api,
                &SessionId::new(format!("{session_id}-clash")),
                &clashing,
            )
            .await;
            assert!(
                refused
                    .as_ref()
                    .is_err_and(|error| error.to_string().contains("not granted to this session")),
                "a clashing MCP tool must have no contract: {refused:?}"
            );

            // The camelCase tool is called by its snake-case contract and
            // reaches the server under its own name.
            let profile_id =
                mcp_program_profile(&api, &fixture, &stable, &[&create(&stable)]).await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "first", "server": stable}),
            )
            .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let output = read_json(
                &store,
                &run.output.as_ref().expect("program output").content_ref,
            )
            .await?;
            assert_eq!(output["tools"], json!(["create_issue"]), "{output}");
            assert!(output["created"].to_string().contains("first"), "{output}");
            assert_eq!(
                fixture.state.lock().await.calls,
                vec![json!({"name": "createIssue", "arguments": {"title": "first"}})]
            );

            // A session pinned before the upstream schema changed fails its
            // next attempt instead of calling the reshaped tool.
            let drifting_profile =
                mcp_program_profile(&api, &fixture, &drifting, &[&create(&drifting)]).await?;
            let drifting_session = SessionId::new(format!("{session_id}-drift"));
            start_program_session(&api, &drifting_session, &drifting_profile).await?;
            fixture.state.lock().await.changed.insert(drifting.clone());
            let run = start_program_run(
                &api,
                &drifting_session,
                json!({"claim": "second", "server": drifting}),
            )
            .await?;
            let run = wait_for_terminal_run(&api, &drifting_session, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Failed, "{run:?}");
            let failure = run_failure(&store, &drifting_session).await?;
            assert_eq!(failure.kind, harness::RunFailureKind::Program);
            let report = read_json(
                &store,
                failure
                    .message_ref
                    .as_ref()
                    .expect("failure report")
                    .as_str(),
            )
            .await?;
            assert!(
                report.to_string().contains(&format!(
                    "tool contract mcp.{drifting}.create_issue changed"
                )),
                "the failure must name the changed contract: {report}"
            );
            assert_eq!(fixture.state.lock().await.calls.len(), 1);

            for id in [&session_id, &drifting_session] {
                let _ = api
                    .close_session(api::SessionCloseParams {
                        session_id: id.to_string(),
                        force: true,
                    })
                    .await;
            }
            for profile_id in [clashing, profile_id, drifting_profile] {
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
            }
            Ok(())
        })
    })
    .await
}

const APPROVING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            try {
                const created = await env.mcp[input.server].create_issue({ title: input.claim });
                return { claim: input.claim, slept: 0, created };
            } catch (error) {
                return { claim: input.claim, slept: 0, kind: error.kind };
            }
        }
    };
"#;

/// The session's pending request, once one is open.
async fn pending_request(
    api: &GatewayAgentApi,
    session_id: &SessionId,
) -> anyhow::Result<api::InputRequestView> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let pending = api
            .list_inbox_requests(api::InboxRequestListParams {
                status: Some(api::InputRequestStatus::Pending),
                session: Some(session_id.to_string()),
                limit: None,
            })
            .await?
            .result
            .requests;
        if let [request] = pending.as_slice() {
            return Ok(request.clone());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "no pending request appeared for {session_id}: {pending:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL, and LIGHTSPEED_MCP_PRIVATE_NETWORKS allowing 127.0.0.1; run serially"]
async fn a_program_call_that_needs_approval_waits_in_the_inbox_and_runs_once_approved()
-> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let fixture = McpFixture::start().await?;
            let server = format!(
                "prog_mcp_{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            );
            let profile_id = mcp_program_profile_with(
                &api,
                &fixture,
                &server,
                &[&format!("mcp.{server}.create_issue")],
                APPROVING_PROGRAM,
                api::RemoteMcpApprovalPolicy::Always,
                Some(api::ProgramApprovals {
                    assignees: vec!["alice".to_owned()],
                    ..Default::default()
                }),
            )
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let output = |run: api::RunView| {
                let store = store.clone();
                async move {
                    assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await
                }
            };

            // The call parks: the inbox shows what it would do, and the
            // server sees nothing until someone approves.
            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "first", "server": server}),
            )
            .await?;
            let request = pending_request(&api, &session_id).await?;
            assert_eq!(request.kind, api::InputRequestKind::Approval);
            assert_eq!(request.assignees, vec!["alice".to_owned()]);
            assert_eq!(
                request.payload,
                Some(json!({
                    "server": server,
                    "serverLabel": server,
                    "tool": "createIssue",
                    "arguments": {"title": "first"},
                }))
            );
            assert!(fixture.state.lock().await.calls.is_empty());
            api.answer_inbox_request(api::InboxRequestAnswerParams {
                request_id: request.request_id.clone(),
                approved: true,
                value: None,
                comment: None,
            })
            .await?;
            let approved = output(wait_for_terminal_run(&api, &session_id, &run.id).await?).await?;
            assert!(
                approved["created"].to_string().contains("first"),
                "{approved}"
            );
            assert_eq!(
                fixture.state.lock().await.calls,
                vec![json!({"name": "createIssue", "arguments": {"title": "first"}})]
            );

            // A rejection fails the call with a kind the program catches;
            // the run's starter may name other approvers.
            let run = api
                .start_run(api::RunStartParams {
                    notify_on_terminal: None,
                    submission_id: None,
                    session_id: session_id.to_string(),
                    source: api::RunStartSource::Program {
                        input: json!({"claim": "second", "server": server}),
                        input_defaults: Some(api::ProgramInputDefaults {
                            approvers: vec!["bob".to_owned()],
                            ..Default::default()
                        }),
                    },
                    config: None,
                })
                .await?
                .result
                .run;
            let request = pending_request(&api, &session_id).await?;
            assert_eq!(request.assignees, vec!["bob".to_owned()]);
            api.answer_inbox_request(api::InboxRequestAnswerParams {
                request_id: request.request_id.clone(),
                approved: false,
                value: None,
                comment: Some("not today".to_owned()),
            })
            .await?;
            assert_eq!(
                output(wait_for_terminal_run(&api, &session_id, &run.id).await?).await?,
                json!({"claim": "second", "slept": 0, "kind": "approval_rejected"})
            );
            assert_eq!(fixture.state.lock().await.calls.len(), 1);

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            api.delete_profile(api::ProfileDeleteParams { profile_id })
                .await?;
            Ok(())
        })
    })
    .await
}

const SLEEPING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const first = await env.time.sleep({ ms: 1500 });
            const second = await env.time.sleep({ ms: 1500 });
            return {
                claim: input.claim,
                slept: 2,
                inOrder: second.fire_at_ms > first.fire_at_ms && Date.now() >= second.fire_at_ms,
            };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL, and LIGHTSPEED_MCP_PRIVATE_NETWORKS allowing 127.0.0.1; run serially"]
async fn a_lost_admission_result_is_admitted_again_without_calling_the_tool_twice()
-> anyhow::Result<()> {
    let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
    with_program_workers_admitting(
        InputTimeouts::default(),
        Some(attempts.clone()),
        |api, store, session_id, client| {
            Box::pin(async move {
                let fixture = McpFixture::start().await?;
                let server = format!(
                    "prog_mcp_{}",
                    &uuid::Uuid::new_v4().simple().to_string()[..12]
                );
                let profile_id = mcp_program_profile(
                    &api,
                    &fixture,
                    &server,
                    &[&format!("mcp.{server}.create_issue")],
                )
                .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                let run = start_program_run(
                    &api,
                    &session_id,
                    json!({"claim": "once", "server": server}),
                )
                .await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output = read_json(
                    &store,
                    &run.output.as_ref().expect("program output").content_ref,
                )
                .await?;
                assert!(output["created"].to_string().contains("once"), "{output}");
                // The second admission found the call the first had admitted:
                // the server was called once and the scope holds one call.
                assert_eq!(
                    fixture.state.lock().await.calls,
                    vec![json!({"name": "createIssue", "arguments": {"title": "once"}})]
                );
                let state = programs::session_state(&store, &session_id).await?;
                assert_eq!(programs::admitted_tools(&state).len(), 1);
                // History keeps only the attempt whose result it recorded.
                let handle = client.get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(
                    programs::program_execution_id(&state)?,
                );
                let events = handle
                    .fetch_history(Default::default())
                    .into_events()
                    .await?;
                assert_eq!(
                    programs::local_activity_attempts(&events, ACTIVITY_PROGRAM_ADMIT),
                    [2]
                );
                WorkflowReplayer::new(
                    WorkflowReplayerOptions::new()
                        .register_workflow::<temporal_workflow::ProgramRunWorkflow>()?
                        .build(),
                )?
                .replay_workflow(handle.fetch_history(Default::default()))
                .await?;

                let _ = api
                    .close_session(api::SessionCloseParams {
                        session_id: session_id.to_string(),
                        force: true,
                    })
                    .await;
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
                Ok(())
            })
        },
    )
    .await?;
    assert_eq!(*attempts.lock().expect("attempts lock"), [1, 2]);
    Ok(())
}

const SEQUENTIAL_SLEEPS_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            for (let i = 0; i < input.sleeps; i++) {
                await env.time.sleep({ ms: 10 });
            }
            return { claim: input.claim, slept: input.sleeps };
        }
    };
"#;

/// The history events of one completed run of [`SEQUENTIAL_SLEEPS_PROGRAM`]
/// in a new session.
async fn sequential_sleeps_history(
    api: &GatewayAgentApi,
    store: &store_pg::PgStore,
    client: &temporalio_client::Client,
    session_id: &SessionId,
    profile_id: &api::ProfileId,
    sleeps: u64,
) -> anyhow::Result<Vec<temporalio_common::protos::temporal::api::history::v1::HistoryEvent>> {
    start_program_session(api, session_id, profile_id).await?;
    let run = start_program_run(api, session_id, json!({"claim": "sleeps", "sleeps": sleeps})).await?;
    let run = wait_for_terminal_run(api, session_id, &run.id).await?;
    assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
    let state = programs::session_state(store, session_id).await?;
    let events = client
        .get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(
            programs::program_execution_id(&state)?,
        )
        .fetch_history(Default::default())
        .into_events()
        .await?;
    let _ = api
        .close_session(api::SessionCloseParams {
            session_id: session_id.to_string(),
            force: true,
        })
        .await;
    Ok(events)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn each_sequential_call_adds_a_bounded_number_of_history_events() -> anyhow::Result<()> {
    with_program_workers_and_client(
        InputTimeouts::default(),
        |api, store, session_id, client| {
            Box::pin(async move {
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                api.create_profile(api::ProfileCreateParams {
                    profile: program_profile(
                        &profile_id,
                        SEQUENTIAL_SLEEPS_PROGRAM,
                        &[("time.sleep", None)],
                    ),
                })
                .await?;
                let one = sequential_sleeps_history(
                    &api,
                    &store,
                    &client,
                    &SessionId::new(format!("{session_id}-one")),
                    &profile_id,
                    1,
                )
                .await?;
                let many = sequential_sleeps_history(
                    &api, &store, &client, &session_id, &profile_id, 6,
                )
                .await?;
                // Each further call is the session's wake (a signal), one
                // workflow task, and the markers of its collection and its
                // admission: six events.
                assert_eq!(
                    many.len() - one.len(),
                    5 * 6,
                    "1 call: {:?}\n6 calls: {:?}",
                    programs::event_counts(&one),
                    programs::event_counts(&many),
                );
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
                Ok(())
            })
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_sleeping_program_waits_in_its_workflow_and_replays_from_history() -> anyhow::Result<()> {
    with_program_workers_and_client(
        InputTimeouts::default(),
        |api, store, session_id, client| {
            Box::pin(async move {
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                api.create_profile(api::ProfileCreateParams {
                    profile: program_profile(
                        &profile_id,
                        SLEEPING_PROGRAM,
                        &[("time.sleep", None)],
                    ),
                })
                .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                let run = start_program_run(&api, &session_id, json!({"claim": "nap"})).await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                assert_eq!(
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                    json!({"claim": "nap", "slept": 2, "inOrder": true})
                );
                // The program ran in its workflow: one load, one local admission
                // per step that issued a call, and no call admitted twice.
                let state = programs::session_state(&store, &session_id).await?;
                assert_eq!(
                    programs::admitted_tools(&state),
                    ["time.sleep", "time.sleep"]
                );
                let handle = client.get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(
                    programs::program_execution_id(&state)?,
                );
                let events = handle
                    .fetch_history(Default::default())
                    .into_events()
                    .await?;
                assert_eq!(
                    programs::activity_attempts(&events, ACTIVITY_PROGRAM_LOAD),
                    [vec![1]]
                );
                assert_eq!(
                    programs::local_activity_attempts(&events, ACTIVITY_PROGRAM_ADMIT),
                    [1, 1]
                );
                // Replaying the history runs the program again to the same
                // commands.
                WorkflowReplayer::new(
                    WorkflowReplayerOptions::new()
                        .register_workflow::<temporal_workflow::ProgramRunWorkflow>()?
                        .build(),
                )?
                .replay_workflow(handle.fetch_history(Default::default()))
                .await?;

                let _ = api
                    .close_session(api::SessionCloseParams {
                        session_id: session_id.to_string(),
                        force: true,
                    })
                    .await;
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
                Ok(())
            })
        },
    )
    .await
}
