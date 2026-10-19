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
    subagents::AgentApiSubagentRuntime,
    worker::{
        ActivityState, ProgramWorkerActivities, SessionTools, WorkerActivities, program_worker,
        program_worker_client, worker_runtime,
    },
};
use temporal_workflow::{ACTIVITY_PROGRAM_ADMIT, ACTIVITY_PROGRAM_LOAD, ProgramHistoryLimits};
use temporalio_common::error::ApplicationFailure;
use temporalio_macros::activities;
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};
use tools::human_input::{
    INPUT_DEFAULT_DEADLINE_ENV, INPUT_MIN_DEADLINE_ENV, INPUT_MIN_REMIND_AFTER_ENV, InputTimeouts,
};

/// The model of the child agents programs start. A brief `CHECK <word>` is
/// answered `checked <word>`; one containing `FAIL` fails the child's run. A
/// brief `PROGRAM <profile> <claim>` calls `program_run` with that claim and
/// answers with what came back. A program session never plans a model turn,
/// so a request without a brief is a bug.
struct ChildAgentModel {
    blobs: Arc<dyn BlobStore>,
}

impl ChildAgentModel {
    async fn brief(&self, request: &LlmGenerationRequest) -> Result<String, CoreAgentIoError> {
        let entry = request
            .request
            .context
            .entries
            .iter()
            .rev()
            .find(|entry| {
                entry.kind
                    == harness::ContextEntryKind::Message {
                        role: harness::ContextMessageRole::User,
                    }
            })
            .ok_or_else(|| CoreAgentIoError::Failed {
                message: format!(
                    "program sessions must not generate; run {} turn {}",
                    request.run_id, request.turn_id
                ),
            })?;
        self.blobs
            .read_text(&entry.content.content_ref)
            .await
            .map_err(|error| CoreAgentIoError::Failed {
                message: error.to_string(),
            })
    }

    /// `program_run` of `<profile> <claim>`, then the tool result as the
    /// answer.
    async fn call_program(
        &self,
        request: &LlmGenerationRequest,
        script: &str,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
        let mut facts = harness::LlmGenerationFacts {
            duration_ms: None,
            provider_response_id: None,
            finish: harness::LlmFinish::Stop,
            usage: None,
            tool_calls: Vec::new(),
            approval_requests: Vec::new(),
            context_token_estimate: None,
        };
        let answered = request
            .request
            .context
            .entries
            .iter()
            .rev()
            .find(|entry| matches!(entry.kind, harness::ContextEntryKind::ToolResult { .. }));
        let (kind, text, provider_kind) = match answered {
            Some(result) => {
                let result = self
                    .blobs
                    .read_text(&result.content.content_ref)
                    .await
                    .map_err(|error| CoreAgentIoError::Failed {
                        message: error.to_string(),
                    })?;
                (
                    harness::ContextEntryKind::Message {
                        role: harness::ContextMessageRole::Assistant,
                    },
                    serde_json::to_vec(
                        &json!([{ "type": "text", "text": format!("program said {result}") }]),
                    )
                    .expect("text blocks"),
                    harness::ANTHROPIC_MESSAGES_TEXT_BLOCKS_PROVIDER_KIND,
                )
            }
            None => {
                let mut words = script.split_whitespace();
                let program = words.next().unwrap_or_default();
                let claim = words.next().unwrap_or_default();
                let tool_name = tools::subagents::PROGRAM_RUN_TOOL_NAME;
                let tool_id =
                    test_support::scripted_tool_id(request, tool_name).ok_or_else(|| {
                        CoreAgentIoError::Failed {
                            message: format!("{tool_name} is not offered to this agent"),
                        }
                    })?;
                let arguments = serde_json::to_vec(&json!({
                    "program": program,
                    "input": { "claim": claim },
                }))
                .expect("arguments");
                let call_id =
                    harness::ToolCallId::new(format!("program_call_{}", request.turn_id.as_u64()));
                let arguments_ref = self.put(arguments.clone()).await?;
                facts.finish = harness::LlmFinish::ToolCalls;
                facts.tool_calls.push(harness::ObservedToolCall {
                    call_id: call_id.clone(),
                    tool_id: Some(tool_id),
                    tool_name: harness::ToolName::new(tool_name),
                    provider_kind: Some("program-script".to_owned()),
                    arguments_ref,
                    native_call_ref: None,
                });
                (
                    harness::ContextEntryKind::ToolCall {
                        call_id,
                        name: harness::ToolName::new(tool_name),
                    },
                    arguments,
                    "program-script",
                )
            }
        };
        let content_ref = self.put(text).await?;
        Ok(LlmGenerationResult {
            run_id: request.run_id,
            turn_id: request.turn_id,
            status: harness::LlmGenerationStatus::Succeeded,
            failure_ref: None,
            context_entries: vec![harness::ContextEntryInput {
                kind,
                content: harness::ContentRef {
                    content_ref,
                    media_type: Some("application/json".to_owned()),
                    provider_kind: Some(provider_kind.to_owned()),
                },
                preview: None,
                origin: None,
                provenance_ref: None,
                token_estimate: None,
            }],
            facts,
        })
    }

    async fn put(&self, bytes: Vec<u8>) -> Result<BlobRef, CoreAgentIoError> {
        self.blobs
            .put_bytes(bytes)
            .await
            .map_err(|error| CoreAgentIoError::Failed {
                message: error.to_string(),
            })
    }
}

#[async_trait]
impl CoreAgentLlm for ChildAgentModel {
    async fn generate(
        &self,
        request: LlmGenerationRequest,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
        let brief = self.brief(&request).await?;
        if let Some(script) = brief.strip_prefix("PROGRAM ") {
            return self.call_program(&request, script).await;
        }
        let facts = harness::LlmGenerationFacts {
            duration_ms: None,
            provider_response_id: None,
            finish: harness::LlmFinish::Stop,
            usage: None,
            tool_calls: Vec::new(),
            approval_requests: Vec::new(),
            context_token_estimate: None,
        };
        if brief.contains("FAIL") {
            return Ok(LlmGenerationResult {
                run_id: request.run_id,
                turn_id: request.turn_id,
                status: harness::LlmGenerationStatus::Failed,
                failure_ref: Some(self.put(b"child gave up".to_vec()).await?),
                context_entries: Vec::new(),
                facts,
            });
        }
        let word = brief
            .split("CHECK ")
            .nth(1)
            .and_then(|rest| rest.split(|c: char| !c.is_alphanumeric()).next())
            .unwrap_or("nothing");
        let output_ref = self
            .put(
                serde_json::to_vec(&json!([{ "type": "text", "text": format!("checked {word}") }]))
                    .expect("text blocks"),
            )
            .await?;
        Ok(LlmGenerationResult {
            run_id: request.run_id,
            turn_id: request.turn_id,
            status: harness::LlmGenerationStatus::Succeeded,
            failure_ref: None,
            context_entries: vec![harness::ContextEntryInput {
                kind: harness::ContextEntryKind::Message {
                    role: harness::ContextMessageRole::Assistant,
                },
                content: harness::ContentRef {
                    content_ref: output_ref,
                    media_type: Some("application/json".to_owned()),
                    provider_kind: Some(
                        harness::ANTHROPIC_MESSAGES_TEXT_BLOCKS_PROVIDER_KIND.to_owned(),
                    ),
                },
                preview: Some(format!("checked {word}")),
                origin: None,
                provenance_ref: None,
                token_estimate: None,
            }],
            facts,
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
            .admit_step(
                &request.descriptor,
                request.requests,
                request.cancels,
                request.acknowledged,
            )
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

/// A `program_close` that closes the run's scope for real, keeping the
/// program's context, and then fails its first attempt, as if the worker had
/// lost the result after the scope closed. Records each attempt it ran.
pub struct LosesFirstClose {
    activities: Arc<ProgramWorkerActivities>,
    attempts: Arc<std::sync::Mutex<Vec<u32>>>,
}

#[activities]
impl LosesFirstClose {
    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_CLOSE)]
    pub async fn program_close(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramCloseActivityRequest,
    ) -> Result<temporal_workflow::ProgramCloseActivityResult, ActivityError> {
        let attempt = ctx.info().attempt;
        self.attempts.lock().expect("attempts lock").push(attempt);
        let result = self.activities.close(ctx, request).await?;
        if attempt == 1 {
            return Err(ActivityError::application(ApplicationFailure::new(
                anyhow::anyhow!("closed, but the result was lost"),
            )));
        }
        Ok(result)
    }
}

/// Which program activity loses the result of its first attempt; the
/// attempts it ran are recorded.
enum LostResult {
    Admit(Arc<std::sync::Mutex<Vec<u32>>>),
    Close(Arc<std::sync::Mutex<Vec<u32>>>),
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
    with_program_workers_setup(
        ProgramWorkerSetup {
            input_timeouts,
            ..Default::default()
        },
        body,
    )
    .await
}

/// [`with_program_workers_and_client`] whose program worker uses `history`.
async fn with_program_workers_history(
    history: ProgramHistoryLimits,
    body: impl FnOnce(
        Arc<GatewayAgentApi>,
        Arc<store_pg::PgStore>,
        SessionId,
        temporalio_client::Client,
    ) -> ProgramTestBody,
) -> anyhow::Result<()> {
    with_program_workers_setup(
        ProgramWorkerSetup {
            history,
            ..Default::default()
        },
        body,
    )
    .await
}

/// How [`with_program_workers_setup`] sets up the workers and the API.
#[derive(Default)]
struct ProgramWorkerSetup {
    input_timeouts: InputTimeouts,
    history: ProgramHistoryLimits,
    /// The program worker runs [`LosesFirstAdmit`] or [`LosesFirstClose`]
    /// in place of the real activity.
    lost: Option<LostResult>,
    /// The history length at which sessions started through the API
    /// continue as new.
    session_continue_as_new_events: Option<u32>,
}

/// [`with_program_workers_and_client`] with the workers and API `setup`
/// describes.
async fn with_program_workers_setup(
    setup: ProgramWorkerSetup,
    body: impl FnOnce(
        Arc<GatewayAgentApi>,
        Arc<store_pg::PgStore>,
        SessionId,
        temporalio_client::Client,
    ) -> ProgramTestBody,
) -> anyhow::Result<()> {
    let ProgramWorkerSetup {
        input_timeouts,
        history,
        lost,
        session_continue_as_new_events,
    } = setup;
    let _lock = LIVE_TEST_LOCK.lock().await;
    require_storage_live_env()?;
    let store = pg_store_from_env().await?;
    seed_agent_default(&store, &support::live::openai_live_model()).await?;
    let program_queue = format!("program-live-{}", uuid::Uuid::new_v4().simple());
    let activity_queue = program_queue.clone();
    let activity_store = store.clone();
    run_with_live_worker_builder(
        move |client, queue| async move {
            let tools = Arc::new(SessionTools::from_pg_store(activity_store.clone()));
            // The child agents a program starts run on this worker too.
            let subagent_api = Arc::new(
                GatewayAgentApi::builder(client.clone(), activity_store.clone())
                    .with_task_queue(queue)
                    .with_program_task_queue(activity_queue.clone())
                    .with_input_timeouts(input_timeouts)
                    .build(),
            );
            Ok(WorkerActivities::for_universe(
                activity_store.config().universe_id,
                ActivityState::from_pg_store(
                    activity_store.clone(),
                    Arc::new(ChildAgentModel {
                        blobs: activity_store.clone(),
                    }),
                    tools.clone(),
                )
                .with_hosted_tools(tools)
                .with_workflow_tool_executions(client)
                .with_subagent_runtime(Arc::new(AgentApiSubagentRuntime::new(subagent_api)))
                .with_program_task_queue(activity_queue)
                .with_input_timeouts(input_timeouts)
                .with_native_mcp_from_pg_store(activity_store)?,
            ))
        },
        move |client, queue, session_id| async move {
            let mut builder = GatewayAgentApi::builder(client.clone(), store.clone())
                .with_task_queue(queue)
                .with_program_task_queue(program_queue.clone())
                .with_input_timeouts(input_timeouts);
            if let Some(events) = session_continue_as_new_events {
                builder = builder.with_continue_as_new_history_threshold(events);
            }
            let api = Arc::new(builder.build());
            let activities = ProgramWorkerActivities::for_universe(live_universe_id()?, api.clone())
                .with_history_limits(history);
            let runtime = worker_runtime()?;
            let mut worker = match lost {
                None => program_worker(&runtime, client.clone(), program_queue, activities)?,
                // A later registration under the same name replaces the
                // worker's own activity.
                Some(lost) => {
                    let worker_client = program_worker_client(client.clone(), &activities);
                    let mut options = temporalio_sdk::WorkerOptions::new(program_queue)
                        .register_workflow::<temporal_workflow::ProgramRunWorkflow>()?
                        .register_activities(activities);
                    options = match lost {
                        LostResult::Admit(attempts) => options.register_activities(LosesFirstAdmit {
                            runner: ProgramRunner::new(client.clone(), store.clone()),
                            attempts,
                        }),
                        LostResult::Close(attempts) => {
                            options.register_activities(LosesFirstClose {
                                activities: Arc::new(
                                    ProgramWorkerActivities::for_universe(
                                        live_universe_id()?,
                                        api.clone(),
                                    )
                                    .with_history_limits(history),
                                ),
                                attempts,
                            })
                        }
                    };
                    temporalio_sdk::Worker::new(&runtime, worker_client, options.build())?
                }
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
            const before = ctx.state ?? null;
            if (input.claim === "plain") return { claim: input.claim, slept: 0, before };
            const done = ctx.complete({ claim: input.claim, slept: 0, before }, {
                round: (before?.round ?? 0) + 1,
                claims: [...(before?.claims ?? []), input.claim],
            });
            if (input.claim === "throw") throw new Error("after completing");
            return done;
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_reads_the_state_its_last_completed_run_handed_over() -> anyhow::Result<()> {
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
            for claim in ["a", "b", "throw", "plain", "c"] {
                let run = start_program_run(&api, &session_id, json!({ "claim": claim })).await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                if claim == "throw" {
                    // A failed run hands nothing over, though it called
                    // `complete`.
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
                    // A plain return hands nothing over either.
                    json!({"round": 2, "claims": ["a", "b"]}),
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

const RACED_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const ask = await env.input.open({ ask: "Ship " + input.claim + "?", assignees: ["alice"] });
            await env.time.sleep({ ms: input.wait_ms });
            const cancelled = await ask.cancel();
            try {
                return { claim: input.claim, slept: input.wait_ms, cancelled, answer: await ask.answer() };
            } catch (error) {
                return { claim: input.claim, slept: input.wait_ms, kind: error.kind };
            }
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn an_answer_saved_before_a_withdrawal_reaches_the_program() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(&profile_id, RACED_PROGRAM, OPENING_CONTRACTS),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            // The answer lands in the inbox row, but its workflow has not
            // read it yet when the program withdraws the request: the row
            // decides, so the program gets the answer, not a cancellation.
            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "raced", "wait_ms": 4000}),
            )
            .await?;
            let [request] = <[_; 1]>::try_from(wait_for_requests(&api, &session_id, 1).await?)
                .map_err(|requests| anyhow::anyhow!("one request expected: {requests:?}"))?;
            store
                .answer_input_request(
                    &request.request_id,
                    &api::InputRequestAnswer {
                        approved: true,
                        value: None,
                        comment: None,
                        answered_by: api::Attribution::Local,
                        answered_at_ms: 1,
                    },
                )
                .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            if run.status == api::RunStatus::Failed {
                let failure = run_failure(&store, &session_id).await?;
                let message = match &failure.message_ref {
                    Some(reference) => {
                        String::from_utf8_lossy(&store.read_bytes(reference).await?).into_owned()
                    }
                    None => String::new(),
                };
                anyhow::bail!("{failure:?}: {message}");
            }
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let output =
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
            assert_eq!(output["answer"]["approved"], json!(true), "{output}");
            // The cancel reports what it found once the request settled.
            assert_eq!(
                output["cancelled"],
                json!({"status": "resolved"}),
                "{output}"
            );
            let requests = api
                .list_inbox_requests(api::InboxRequestListParams {
                    session: Some(session_id.to_string()),
                    ..Default::default()
                })
                .await?
                .result
                .requests;
            assert_eq!(
                requests
                    .iter()
                    .map(|request| request.status)
                    .collect::<Vec<_>>(),
                vec![api::InputRequestStatus::Answered],
                "{requests:?}"
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

const LONG_SLEEP_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            try {
                await env.time.sleep({ ms: 600000 });
                return { claim: input.claim, slept: 600000 };
            } catch (error) {
                return { claim: input.claim, slept: 0, kind: error.kind };
            }
        }
    };
"#;

/// The origin of the first scoped call that was deferred in the session.
async fn first_deferred_call(
    store: &store_pg::PgStore,
    session_id: &SessionId,
) -> anyhow::Result<harness::CodeToolOrigin> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let page = store
            .read_after(harness::storage::ReadSessionEvents {
                session_id: session_id.clone(),
                after: None,
                limit: 1000,
            })
            .await?;
        for entry in page.entries {
            if let harness::CoreAgentEvent::CodeTool(harness::CodeToolEvent::CallDeferred {
                origin,
                ..
            }) = harness::CoreAgentCodec.decode_event(&entry.event)?
            {
                return Ok(origin);
            }
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "no scoped call was deferred in {session_id}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_scoped_call_is_cancelled_alone_and_records_one_outcome() -> anyhow::Result<()> {
    with_program_workers_and_client(
        InputTimeouts::default(),
        |api, store, session_id, client| {
            Box::pin(async move {
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                api.create_profile(api::ProfileCreateParams {
                    profile: program_profile(
                        &profile_id,
                        LONG_SLEEP_PROGRAM,
                        &[("time.sleep", None)],
                    ),
                })
                .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                let run =
                    start_program_run(&api, &session_id, json!({"claim": "cut short"})).await?;

                // Cancel the ten-minute sleep by itself, twice: the second send
                // is the same request and changes nothing.
                let origin = first_deferred_call(&store, &session_id).await?;
                let code_tools = temporal_workflow::CodeToolClient::new(
                    client,
                    temporal_workflow::compose_workflow_id(live_universe_id()?, &session_id),
                );
                let request = temporal_workflow::CancelCodeToolRequest {
                    execution_id: origin.execution_id.clone(),
                    target: harness::CodeToolCancelTarget::Call {
                        request_id: origin.request_id.clone(),
                    },
                };
                for _ in 0..2 {
                    let outcome = code_tools
                        .cancel(request.clone(), Default::default())
                        .await?
                        .map_err(|rejection| anyhow::anyhow!("{rejection}"))?;
                    assert!(
                        matches!(
                            outcome.status,
                            temporal_workflow::CodeToolCancelStatus::Requested
                                | temporal_workflow::CodeToolCancelStatus::Cancelled
                        ),
                        "{outcome:?}"
                    );
                }
                // A request the scope never admitted is refused.
                let unknown = code_tools
                    .cancel(
                        temporal_workflow::CancelCodeToolRequest {
                            execution_id: origin.execution_id.clone(),
                            target: harness::CodeToolCancelTarget::Call {
                                request_id: "call-99".to_owned(),
                            },
                        },
                        Default::default(),
                    )
                    .await?;
                assert!(unknown.is_err(), "{unknown:?}");

                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                assert_eq!(
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                    json!({"claim": "cut short", "slept": 0, "kind": "tool_cancelled"})
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
        },
    )
    .await
}

const OWN_CANCEL_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const nap = env.time.sleep({ ms: 600000 });
            await env.time.sleep({ ms: 10 });
            const cancelled = await nap.cancel();
            let kind = null;
            try { await nap; } catch (error) { kind = error.kind; }
            return { claim: input.claim, slept: 0, cancelled, kind };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_cancels_its_own_call_and_a_lost_cancel_is_sent_again() -> anyhow::Result<()> {
    let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
    with_program_workers_setup(
        ProgramWorkerSetup {
            lost: Some(LostResult::Admit(attempts.clone())),
            ..Default::default()
        },
        |api, store, session_id, _| {
            Box::pin(async move {
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                api.create_profile(api::ProfileCreateParams {
                    profile: program_profile(
                        &profile_id,
                        OWN_CANCEL_PROGRAM,
                        &[("time.sleep", None)],
                    ),
                })
                .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                let run = start_program_run(&api, &session_id, json!({"claim": "own"})).await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                assert_eq!(
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                    json!({
                        "claim": "own",
                        "slept": 0,
                        "cancelled": {"status": "cancelled"},
                        "kind": "tool_cancelled",
                    })
                );
                // Both steps' admissions ran twice, the cancel's included,
                // and the session recorded the cancel once.
                assert_eq!(*attempts.lock().expect("attempts lock"), [1, 2, 1, 2]);
                let mut requested = 0;
                let mut after = None;
                loop {
                    let page = store
                        .read_after(harness::storage::ReadSessionEvents {
                            session_id: session_id.clone(),
                            after,
                            limit: 1000,
                        })
                        .await?;
                    for entry in page.entries {
                        if matches!(
                            harness::CoreAgentCodec.decode_event(&entry.event)?,
                            harness::CoreAgentEvent::CodeTool(
                                harness::CodeToolEvent::CallCancelRequested { .. }
                            )
                        ) {
                            requested += 1;
                        }
                    }
                    if page.complete {
                        break;
                    }
                    after = page.next_after;
                }
                assert_eq!(requested, 1);

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

const FIRST_OF_THREE_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const asks = await Promise.all(
                ["alice", "bob", "carol"].map(to => env.input.open({ ask: "Ship?", assignees: [to] }))
            );
            const first = await Promise.any(
                asks.map(ask => ask.answer().then(answer => ({ id: ask.id, answer })))
            );
            const others = await Promise.all(
                asks.filter(ask => ask.id !== first.id).map(ask => ask.cancel())
            );
            return { claim: input.claim, slept: 0, approved: first.answer.approved, others };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn the_first_of_three_approvers_decides_and_the_other_requests_are_withdrawn()
-> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(&profile_id, FIRST_OF_THREE_PROGRAM, OPENING_CONTRACTS),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let run = start_program_run(&api, &session_id, json!({"claim": "ship"})).await?;
            let requests = wait_for_requests(&api, &session_id, 3).await?;
            let bob = requests
                .iter()
                .find(|request| request.assignees == ["bob".to_owned()])
                .ok_or_else(|| anyhow::anyhow!("no request for bob: {requests:?}"))?;
            api.answer_inbox_request(api::InboxRequestAnswerParams {
                request_id: bob.request_id.clone(),
                approved: true,
                value: None,
                comment: None,
            })
            .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            assert_eq!(
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                json!({
                    "claim": "ship",
                    "slept": 0,
                    "approved": true,
                    "others": [{"status": "cancelled"}, {"status": "cancelled"}],
                })
            );
            // Each withdrawal settled before the program went on, so the
            // inbox already shows it.
            let mut statuses = api
                .list_inbox_requests(api::InboxRequestListParams {
                    session: Some(session_id.to_string()),
                    ..Default::default()
                })
                .await?
                .result
                .requests
                .into_iter()
                .map(|request| (request.assignees, request.status))
                .collect::<Vec<_>>();
            statuses.sort_by(|left, right| left.0.cmp(&right.0));
            assert_eq!(
                statuses,
                [
                    (vec!["alice".to_owned()], api::InputRequestStatus::Cancelled),
                    (vec!["bob".to_owned()], api::InputRequestStatus::Answered),
                    (vec!["carol".to_owned()], api::InputRequestStatus::Cancelled),
                ]
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

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn an_approved_call_does_not_start_or_is_stopped_once_withdrawn() -> anyhow::Result<()> {
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
                &[&format!("mcp.{server}.create_issue"), "time.sleep"],
                WITHDRAWN_APPROVAL_PROGRAM,
                api::RemoteMcpApprovalPolicy::Always,
                Some(api::ProgramApprovals {
                    assignees: vec!["alice".to_owned()],
                    ..Default::default()
                }),
            )
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let cancelled = |claim: &str| {
                json!({
                    "claim": claim,
                    "slept": 0,
                    "cancelled": {"status": "cancelled"},
                    "kind": "tool_cancelled",
                })
            };

            // Approved in the inbox, but the workflow has not read it when
            // the program withdraws the call: the call never starts.
            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "early", "server": server, "wait_ms": 3000}),
            )
            .await?;
            let request = pending_request(&api, &session_id).await?;
            store
                .answer_input_request(
                    &request.request_id,
                    &api::InputRequestAnswer {
                        approved: true,
                        value: None,
                        comment: None,
                        answered_by: api::Attribution::Local,
                        answered_at_ms: 1,
                    },
                )
                .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            assert_eq!(
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                cancelled("early")
            );
            assert!(fixture.state.lock().await.calls.is_empty());

            // Approved and running when the program withdraws it: the call
            // is stopped long before its minute is up.
            let started = tokio::time::Instant::now();
            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "slow", "server": server, "wait_ms": 3000}),
            )
            .await?;
            let request = pending_request(&api, &session_id).await?;
            api.answer_inbox_request(api::InboxRequestAnswerParams {
                request_id: request.request_id.clone(),
                approved: true,
                value: None,
                comment: None,
            })
            .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            assert_eq!(
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                cancelled("slow")
            );
            assert_eq!(
                fixture.state.lock().await.calls,
                vec![json!({"name": "createIssue", "arguments": {"title": "slow"}})]
            );
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the running call was not stopped: {:?}",
                started.elapsed()
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
            // A title starting with "slow" keeps the call running for a
            // minute, so a test can stop it partway.
            if title.starts_with("slow") {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            reply(json!({
                "content": [{"type": "text", "text": format!("created {title}")}],
                "structuredContent": {"number": 7, "title": title},
            }))
        }
        _ => axum::http::StatusCode::BAD_REQUEST.into_response(),
    }
}

const WITHDRAWN_APPROVAL_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const call = env.mcp[input.server].create_issue({ title: input.claim });
            await env.time.sleep({ ms: input.wait_ms });
            const cancelled = await call.cancel();
            let kind = null;
            try { await call; } catch (error) { kind = error.kind; }
            return { claim: input.claim, slept: 0, cancelled, kind };
        }
    };
"#;

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
    with_program_workers_setup(
        ProgramWorkerSetup {
            lost: Some(LostResult::Admit(attempts.clone())),
            ..Default::default()
        },
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
async fn a_program_acknowledges_the_outcomes_it_collected_in_earlier_workflow_tasks()
-> anyhow::Result<()> {
    with_program_workers_and_client(
        InputTimeouts::default(),
        |api, store, session_id, _client| {
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
                start_program_session(&api, &session_id, &profile_id).await?;
                let run = start_program_run(
                    &api,
                    &session_id,
                    json!({"claim": "acknowledged", "sleeps": 4}),
                )
                .await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let state = programs::session_state(&store, &session_id).await?;
                let scope = &state.code_tools.scopes[&programs::program_execution_id(&state)?];
                // Each sleep is collected in its own workflow task, and every
                // admission and the close acknowledge what earlier tasks
                // collected: never the outcome collected in the sending task.
                assert_eq!(scope.recorded(), 4);
                assert!(
                    (1..scope.recorded()).contains(&scope.acknowledged),
                    "acknowledged {} of {}",
                    scope.acknowledged,
                    scope.recorded()
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
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_run_keeps_going_while_its_session_continues_as_new_and_drops_what_it_acknowledged()
-> anyhow::Result<()> {
    with_program_workers_setup(
        ProgramWorkerSetup {
            // The session continues as new whenever it can, so the program's
            // calls keep arriving while a new execution loads the session.
            session_continue_as_new_events: Some(1),
            ..Default::default()
        },
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
                start_program_session(&api, &session_id, &profile_id).await?;
                let run = start_program_run(
                    &api,
                    &session_id,
                    json!({"claim": "continued", "sleeps": 12}),
                )
                .await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                let state = programs::session_state(&store, &session_id).await?;
                let execution_id = programs::program_execution_id(&state)?;
                if run.status != api::RunStatus::Completed {
                    let snapshot = client
                        .get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(
                            execution_id.clone(),
                        )
                        .query(
                            temporal_workflow::ProgramRunWorkflow::snapshot,
                            (),
                            temporalio_client::WorkflowQueryOptions::default(),
                        )
                        .await;
                    anyhow::bail!(
                        "the run did not complete: {:?}\nprogram: {snapshot:?}",
                        state.runs.completed.last()
                    );
                }
                let output =
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
                assert_eq!(output, json!({"claim": "continued", "slept": 12}));
                assert_eq!(state.code_tools.scopes[&execution_id].calls.len(), 12);
                // The session did continue as new during the run.
                let session = client
                    .get_workflow_handle::<temporal_workflow::AgentSessionWorkflow>(
                        temporal_workflow::compose_workflow_id(live_universe_id()?, &session_id),
                    );
                let events = session
                    .fetch_history(Default::default())
                    .into_events()
                    .await?;
                assert!(
                    continued_from(&events).is_some(),
                    "the session's current execution continued another"
                );
                // The run's report reads the session's memory at the end of
                // the run: it counts all twelve calls and holds only those the
                // program had not acknowledged by the last reload.
                let snapshot = client
                    .get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(execution_id)
                    .query(
                        temporal_workflow::ProgramRunWorkflow::snapshot,
                        (),
                        temporalio_client::WorkflowQueryOptions::default(),
                    )
                    .await?;
                let Some(temporal_workflow::ProgramRunTerminal::Completed { result }) =
                    snapshot.terminal
                else {
                    anyhow::bail!("the run completed: {snapshot:?}");
                };
                let report = read_json(&store, result.report_ref.as_str()).await?;
                let scope = &report["scope"];
                assert_eq!(
                    (scope["admitted"].as_u64(), scope["recorded"].as_u64()),
                    (Some(12), Some(12)),
                    "{scope}"
                );
                let held = scope["calls"].as_object().map_or(0, |calls| calls.len());
                assert!(held < 12, "a reload left acknowledged calls out: {scope}");

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

/// Every program run's workflow id on the session, in invocation order.
fn program_execution_ids(state: &harness::CoreAgentState) -> Vec<String> {
    state
        .workflow_tools
        .start_requests
        .values()
        .filter(|request| request.tool_id.as_str() == harness::PROGRAM_EXECUTE_WORKFLOW_TOOL_ID)
        .filter_map(
            |request| match &state.workflow_tools.bindings[&request.tool_id].target {
                harness::WorkflowToolTarget::Start { start } => {
                    Some(harness::workflow_tool_execution_id(
                        &request.invocation_id,
                        &start.recipe_fingerprint,
                    ))
                }
                _ => None,
            },
        )
        .collect()
}

/// Whether any event of `events` carries `probe` anywhere in its bytes.
fn history_contains(
    events: &[temporalio_common::protos::temporal::api::history::v1::HistoryEvent],
    probe: &str,
) -> bool {
    events.iter().any(|event| {
        prost::Message::encode_to_vec(event)
            .windows(probe.len())
            .any(|window| window == probe.as_bytes())
    })
}

const SOURCE_PROBE: &str = "source-history-probe;";
const INPUT_PROBE: &str = "input-history-probe;";
const CONTEXT_PROBE: &str = "context-history-probe;";
const OUTPUT_PROBE: &str = "output-history-probe;";

/// Calls the fixture's echoing tool with the claim, hands `input.keep` over
/// as its state, and returns `input.echo` beside the sizes it saw.
fn large_values_program() -> String {
    format!(
        r#"/* {} */
        export default {{
            async run(input, env, ctx) {{
                const created = await env.mcp[input.server].create_issue({{ title: input.claim }});
                return ctx.complete({{
                    claim: "ok",
                    slept: 0,
                    echo: input.echo,
                    claimLength: input.claim.length,
                    createdHasClaim: JSON.stringify(created).includes(input.claim),
                    contextLength: ctx.state?.kept?.length ?? 0,
                }}, {{ kept: input.keep }});
            }}
        }};"#,
        SOURCE_PROBE.repeat(1_000)
    )
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL, and LIGHTSPEED_MCP_PRIVATE_NETWORKS allowing 127.0.0.1; run serially"]
async fn large_program_values_travel_by_reference_and_replay() -> anyhow::Result<()> {
    with_program_workers_and_client(
        InputTimeouts::default(),
        |api, store, session_id, client| {
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
                    &large_values_program(),
                    api::RemoteMcpApprovalPolicy::Never,
                    None,
                )
                .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                let codec = ProgramWorkerActivities::for_universe(live_universe_id()?, api.clone())
                    .value_ref_codec();
                let claim = INPUT_PROBE.repeat(2_000);
                let keep = CONTEXT_PROBE.repeat(1_500);
                let echo = OUTPUT_PROBE.repeat(2_000);

                // Run 1: large source, input, tool result, output and kept
                // context.
                let run = start_program_run(
                    &api,
                    &session_id,
                    json!({"claim": claim, "server": server, "keep": keep, "echo": echo}),
                )
                .await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                assert_eq!(
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                    json!({"claim": "ok", "slept": 0, "echo": echo, "claimLength": claim.len(),
                           "createdHasClaim": true, "contextLength": 0})
                );
                let first =
                    program_execution_ids(&programs::session_state(&store, &session_id).await?);
                assert_eq!(first.len(), 1);

                // Run 2: the large context run 1 kept.
                let run = start_program_run(
                    &api,
                    &session_id,
                    json!({"claim": "small", "server": server, "keep": "", "echo": "e"}),
                )
                .await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                assert_eq!(
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?,
                    json!({"claim": "ok", "slept": 0, "echo": "e", "claimLength": 5,
                           "createdHasClaim": true, "contextLength": keep.len()})
                );
                let ids =
                    program_execution_ids(&programs::session_state(&store, &session_id).await?);
                let second = ids
                    .into_iter()
                    .find(|id| !first.contains(id))
                    .expect("second run's workflow");

                for (id, absent) in [
                    (
                        &first[0],
                        &[SOURCE_PROBE, INPUT_PROBE, CONTEXT_PROBE, OUTPUT_PROBE][..],
                    ),
                    (&second, &[SOURCE_PROBE, CONTEXT_PROBE][..]),
                ] {
                    let handle = client
                        .get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(id.clone());
                    let events = handle
                        .fetch_history(Default::default())
                        .into_events()
                        .await?;
                    for probe in absent {
                        assert!(
                            !history_contains(&events, probe),
                            "{probe} reached {id}'s history"
                        );
                    }
                    assert!(history_contains(&events, "lightspeed-ref:sha256:"));
                    // A replay with the codec reads the references back and
                    // takes the same path; without it the history is unreadable.
                    WorkflowReplayer::new(
                        WorkflowReplayerOptions::new()
                            .data_converter(codec.clone().data_converter())
                            .register_workflow::<temporal_workflow::ProgramRunWorkflow>()?
                            .build(),
                    )?
                    .replay_workflow(handle.fetch_history(Default::default()))
                    .await?;
                    assert!(
                        WorkflowReplayer::new(
                            WorkflowReplayerOptions::new()
                                .register_workflow::<temporal_workflow::ProgramRunWorkflow>()?
                                .build(),
                        )?
                        .replay_workflow(handle.fetch_history(Default::default()))
                        .await
                        .is_err()
                    );
                }

                let _ = api
                    .close_session(api::SessionCloseParams {
                        session_id: session_id.to_string(),
                        force: true,
                    })
                    .await;
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
                fixture.task.abort();
                Ok(())
            })
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL, and LIGHTSPEED_MCP_PRIVATE_NETWORKS allowing 127.0.0.1; run serially"]
async fn a_lost_close_result_closes_again_and_keeps_one_context() -> anyhow::Result<()> {
    let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
    with_program_workers_setup(
        ProgramWorkerSetup {
            lost: Some(LostResult::Close(attempts.clone())),
            ..Default::default()
        },
        |api, store, session_id, client| {
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
                    &large_values_program(),
                    api::RemoteMcpApprovalPolicy::Never,
                    None,
                )
                .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                for (keep, echo, context_length) in [("kept once", "first", 0), ("", "second", 9)] {
                    let run = start_program_run(
                        &api,
                        &session_id,
                        json!({"claim": "c", "server": server, "keep": keep, "echo": echo}),
                    )
                    .await?;
                    let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                    assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                    // The second close of the closed scope kept nothing more:
                    // the context the first kept is the one the next run
                    // reads.
                    assert_eq!(
                        read_json(&store, &run.output.as_ref().expect("output").content_ref)
                            .await?,
                        json!({"claim": "ok", "slept": 0, "echo": echo, "claimLength": 1,
                               "createdHasClaim": true, "contextLength": context_length})
                    );
                }
                let state = programs::session_state(&store, &session_id).await?;
                // One context per run, though each run's scope was closed twice.
                assert_eq!(
                    state
                        .context
                        .entries
                        .iter()
                        .filter(|entry| entry.kind == harness::ContextEntryKind::ProgramState)
                        .count(),
                    2
                );
                for id in program_execution_ids(&state) {
                    let handle =
                        client.get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(id);
                    let events = handle
                        .fetch_history(Default::default())
                        .into_events()
                        .await?;
                    // History keeps only the attempt whose result it recorded.
                    assert_eq!(
                        programs::local_activity_attempts(
                            &events,
                            temporal_workflow::ACTIVITY_PROGRAM_CLOSE
                        ),
                        [2]
                    );
                }

                let _ = api
                    .close_session(api::SessionCloseParams {
                        session_id: session_id.to_string(),
                        force: true,
                    })
                    .await;
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
                fixture.task.abort();
                Ok(())
            })
        },
    )
    .await?;
    assert_eq!(*attempts.lock().expect("attempts lock"), [1, 2, 1, 2]);
    Ok(())
}

const LEFT_OPEN_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            await env.input.open({ ask: "Ship " + input.claim + "?", assignees: ["alice"] });
            env.input.request({ ask: "Merge " + input.claim + "?", assignees: ["bob"] });
            await env.time.sleep({ ms: input.wait_ms });
            return { claim: input.claim, slept: input.wait_ms };
        }
    };
"#;

/// Each of the scope's recorded outcomes, sorted; panics on a call
/// without one.
fn recorded_statuses(scope: &harness::CodeToolScope) -> Vec<String> {
    let mut statuses = scope
        .calls
        .values()
        .map(|call| match &call.status {
            harness::CodeToolCallStatus::Completed { result } => format!("{:?}", result.status),
            other => panic!("call without an outcome: {:?}: {other:?}", call.spec),
        })
        .collect::<Vec<_>>();
    statuses.sort();
    statuses
}

/// The run's only program scope, from the session's state.
fn only_program_scope(state: &harness::CoreAgentState) -> &harness::CodeToolScope {
    let [execution_id] =
        <[_; 1]>::try_from(program_execution_ids(state)).expect("one program execution");
    &state.code_tools.scopes[&execution_id]
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_exit_withdraws_its_open_requests_and_the_run_waits_for_them()
-> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(
                    &profile_id,
                    LEFT_OPEN_PROGRAM,
                    &[
                        ("input.open", None),
                        ("input.request", None),
                        ("promise.await", None),
                        ("promise.cancel", None),
                        ("time.sleep", None),
                    ],
                ),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            // The program returns holding an opened request and a request it
            // never awaited. Alice's answer is saved before the exit, but its
            // workflow has not read it: the withdrawal finds it, so it stands.
            let run =
                start_program_run(&api, &session_id, json!({"claim": "left", "wait_ms": 4000}))
                    .await?;
            let requests = wait_for_requests(&api, &session_id, 2).await?;
            let opened = requests
                .iter()
                .find(|request| request.assignees == ["alice"])
                .expect("alice's request");
            store
                .answer_input_request(
                    &opened.request_id,
                    &api::InputRequestAnswer {
                        approved: true,
                        value: None,
                        comment: None,
                        answered_by: api::Attribution::Local,
                        answered_at_ms: 1,
                    },
                )
                .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");

            // Both had settled before the run ended: nothing is left open,
            // and nothing was cancelled without its workflow's word.
            let requests = api
                .list_inbox_requests(api::InboxRequestListParams {
                    session: Some(session_id.to_string()),
                    ..Default::default()
                })
                .await?
                .result
                .requests;
            let status = |assignee: &str| {
                requests
                    .iter()
                    .find(|request| request.assignees == [assignee])
                    .map(|request| request.status)
            };
            assert_eq!(
                (status("alice"), status("bob")),
                (
                    Some(api::InputRequestStatus::Answered),
                    Some(api::InputRequestStatus::Cancelled)
                ),
                "{requests:?}"
            );
            let state = programs::session_state(&store, &session_id).await?;
            let scope = only_program_scope(&state);
            let [handle] = <[_; 1]>::try_from(scope.withdrawn.iter().collect::<Vec<_>>())
                .map_err(|withdrawn| anyhow::anyhow!("one withdrawn handle: {withdrawn:?}"))?;
            assert_eq!(
                state.promises.promises[handle].status,
                harness::PromiseStatus::Resolved
            );
            // The opened request and the sleep succeeded; the request the
            // program never awaited was withdrawn, which its workflow
            // records as an `input_cancelled` failure.
            assert_eq!(
                recorded_statuses(scope),
                ["Failed", "Succeeded", "Succeeded"],
                "{:?}",
                scope.calls
            );
            let withdrawn = scope
                .calls
                .values()
                .find(|call| call.spec.tool_id.as_str() == "input.request")
                .expect("the request's call");
            let harness::CodeToolCallStatus::Completed { result } = &withdrawn.status else {
                anyhow::bail!("the request has no outcome");
            };
            let message = store
                .read_bytes(result.error_ref.as_ref().expect("its failure"))
                .await?;
            assert!(
                String::from_utf8_lossy(&message).contains("input_cancelled"),
                "{}",
                String::from_utf8_lossy(&message)
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

const LEFT_RUNNING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            env.mcp[input.server].create_issue({ title: input.claim });
            await env.time.sleep({ ms: input.wait_ms });
            return { claim: input.claim, slept: input.wait_ms };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL, and LIGHTSPEED_MCP_PRIVATE_NETWORKS allowing 127.0.0.1; run serially"]
async fn a_program_exit_stops_a_running_call_and_records_its_outcome() -> anyhow::Result<()> {
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
                &[&format!("mcp.{server}.create_issue"), "time.sleep"],
                LEFT_RUNNING_PROGRAM,
                api::RemoteMcpApprovalPolicy::Never,
                None,
            )
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            // The program returns while its call runs (a "slow" title takes a
            // minute): the exit stops the call and the run ends with the
            // call's own recorded outcome, long before the minute is up.
            let started = tokio::time::Instant::now();
            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "slow", "server": server, "wait_ms": 3000}),
            )
            .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the running call was not stopped: {:?}",
                started.elapsed()
            );
            assert_eq!(
                fixture.state.lock().await.calls,
                vec![json!({"name": "createIssue", "arguments": {"title": "slow"}})]
            );
            let state = programs::session_state(&store, &session_id).await?;
            let scope = only_program_scope(&state);
            // The call was stopped and recorded cancelled; the sleep it
            // outlived succeeded.
            assert_eq!(
                recorded_statuses(scope),
                ["Cancelled", "Succeeded"],
                "{:?}",
                scope.calls
            );

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            api.delete_profile(api::ProfileDeleteParams { profile_id })
                .await?;
            fixture.task.abort();
            Ok(())
        })
    })
    .await
}

const DETACHING_PROGRAM: &str = r#"
    export default {
        async run(input, env, ctx) {
            if (input.claim === "keep") {
                const ask = await env.input.open({ ask: "Ship it?", assignees: ["alice"] });
                await ask.detach();
                return ctx.complete({ claim: input.claim, slept: 0 }, { id: ask.id });
            }
            const ask = await env.input.handle(ctx.state.id);
            if (input.claim === "drop") return { claim: input.claim, slept: 0 };
            return { claim: input.claim, slept: 0, answer: await ask.answer() };
        }
    };
"#;

/// The session's inbox requests, by request id.
async fn inbox_statuses(
    api: &GatewayAgentApi,
    session_id: &SessionId,
) -> anyhow::Result<std::collections::BTreeMap<String, api::InputRequestStatus>> {
    Ok(api
        .list_inbox_requests(api::InboxRequestListParams {
            session: Some(session_id.to_string()),
            ..Default::default()
        })
        .await?
        .result
        .requests
        .into_iter()
        .map(|request| (request.request_id, request.status))
        .collect())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_detached_request_outlives_its_run_and_a_later_run_takes_it_back() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(
                    &profile_id,
                    DETACHING_PROGRAM,
                    &[
                        ("input.open", None),
                        ("input.handle", None),
                        ("promise.await", None),
                        ("promise.detach", None),
                    ],
                ),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let completed = |claim: &'static str| {
                let api = api.clone();
                let session_id = session_id.clone();
                async move {
                    let run =
                        start_program_run(&api, &session_id, json!({ "claim": claim })).await?;
                    let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                    anyhow::ensure!(run.status == api::RunStatus::Completed, "{run:?}");
                    Ok::<_, anyhow::Error>(run)
                }
            };

            // A detached request stays open after its run ends.
            completed("keep").await?;
            // Its workflow writes the inbox row; it may land after the run.
            wait_for_requests(&api, &session_id, 1).await?;
            let statuses = inbox_statuses(&api, &session_id).await?;
            let [(first, status)] = <[_; 1]>::try_from(statuses.into_iter().collect::<Vec<_>>())
                .map_err(|statuses| anyhow::anyhow!("one request: {statuses:?}"))?;
            assert_eq!(status, api::InputRequestStatus::Pending);

            // A later run takes it back and, not detaching it again, withdraws
            // it at its exit, before the run completes.
            completed("drop").await?;
            assert_eq!(
                inbox_statuses(&api, &session_id).await?[&first],
                api::InputRequestStatus::Cancelled
            );

            // Kept again, then taken back and answered.
            completed("keep").await?;
            let run = start_program_run(&api, &session_id, json!({ "claim": "answer" })).await?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let second = loop {
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "the second request never appeared"
                );
                let statuses = inbox_statuses(&api, &session_id).await?;
                if let Some((id, _)) = statuses.iter().find(|(id, status)| {
                    **id != first && **status == api::InputRequestStatus::Pending
                }) {
                    break id.clone();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            api.answer_inbox_request(api::InboxRequestAnswerParams {
                request_id: second.clone(),
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
            assert_eq!(
                inbox_statuses(&api, &session_id).await?[&second],
                api::InputRequestStatus::Answered
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

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_detached_request_answered_between_runs_waits_for_the_run_that_takes_it_back()
-> anyhow::Result<()> {
    with_program_workers_setup(
        ProgramWorkerSetup::default(),
        |api, store, session_id, client| {
            Box::pin(async move {
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                api.create_profile(api::ProfileCreateParams {
                    profile: program_profile(
                        &profile_id,
                        DETACHING_PROGRAM,
                        &[
                            ("input.open", None),
                            ("input.handle", None),
                            ("promise.await", None),
                            ("promise.detach", None),
                        ],
                    ),
                })
                .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                let run = start_program_run(&api, &session_id, json!({ "claim": "keep" })).await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                wait_for_requests(&api, &session_id, 1).await?;
                let request_id = inbox_statuses(&api, &session_id)
                    .await?
                    .into_keys()
                    .next()
                    .expect("request");

                // Answered while no run is going: the session records the
                // answer and starts nothing.
                api.answer_inbox_request(api::InboxRequestAnswerParams {
                    request_id,
                    approved: true,
                    value: None,
                    comment: None,
                })
                .await?;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                loop {
                    let state = programs::session_state(&store, &session_id).await?;
                    if state.promises.promises.values().any(|promise| {
                        promise.scope == harness::PromiseScope::Session
                            && promise.status == harness::PromiseStatus::Resolved
                    }) {
                        break;
                    }
                    anyhow::ensure!(
                        tokio::time::Instant::now() < deadline,
                        "the answer never reached the session"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
                let status = support::live::live_workflow_handle(&client, &session_id)?
                    .query(
                        temporal_workflow::AgentSessionWorkflow::status,
                        (),
                        temporalio_client::WorkflowQueryOptions::default(),
                    )
                    .await?;
                assert!(
                    status.admission_failures.is_empty(),
                    "{:?}",
                    status.admission_failures
                );
                assert!(status.active_run.is_none(), "{:?}", status.active_run);

                // The next run takes the answered request back.
                let run =
                    start_program_run(&api, &session_id, json!({ "claim": "answer" })).await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output =
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
                assert_eq!(output["answer"]["approved"], json!(true), "{output}");

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

/// Loops over three executions: each sleeps, records what it saw, and
/// continues as new until the third completes the run, handing the next run
/// how many runs have completed.
const CONTINUING_PROGRAM: &str = r#"
    export default {
        async run(input, env, ctx) {
            const s = ctx.continued ? ctx.state : { i: 0, runs: ctx.state?.runs ?? 0, seen: [], draws: [], at: 0 };
            await env.time.sleep({ ms: 1 });
            const seen = [...s.seen, { i: s.i, continued: ctx.continued, clockKept: Date.now() >= s.at }];
            const draws = [...s.draws, Math.random()];
            if (s.i < 2) {
                return ctx.continueAsNew({ ...s, i: s.i + 1, seen, draws, at: Date.now() });
            }
            return ctx.complete(
                { claim: input.claim, slept: seen.length, runs: s.runs, seen, distinctDraws: new Set(draws).size },
                { runs: s.runs + 1 },
            );
        }
    };
"#;

/// The run id of the execution a workflow execution's history continued, if
/// it continued one.
fn continued_from(
    events: &[temporalio_common::protos::temporal::api::history::v1::HistoryEvent],
) -> Option<String> {
    use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
    match events.first()?.attributes.as_ref()? {
        Attributes::WorkflowExecutionStartedEventAttributes(started) => {
            Some(started.continued_execution_run_id.clone()).filter(|id| !id.is_empty())
        }
        _ => None,
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_continues_as_new_and_each_execution_starts_from_its_state() -> anyhow::Result<()>
{
    with_program_workers_and_client(
        InputTimeouts::default(),
        |api, store, session_id, client| {
            Box::pin(async move {
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                api.create_profile(api::ProfileCreateParams {
                    profile: program_profile(
                        &profile_id,
                        CONTINUING_PROGRAM,
                        &[("time.sleep", None)],
                    ),
                })
                .await?;
                start_program_session(&api, &session_id, &profile_id).await?;

                let run = start_program_run(&api, &session_id, json!({"claim": "loop"})).await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output =
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
                // Each execution started from the state the last handed over,
                // with its clock where the last stopped and its own seed.
                assert_eq!(
                    output,
                    json!({
                        "claim": "loop",
                        "slept": 3,
                        "runs": 0,
                        "seen": [
                            {"i": 0, "continued": false, "clockKept": true},
                            {"i": 1, "continued": true, "clockKept": true},
                            {"i": 2, "continued": true, "clockKept": true},
                        ],
                        "distinctDraws": 3,
                    })
                );

                // The run is one workflow id; its third execution continued
                // the second, and replays from its own history.
                let state = programs::session_state(&store, &session_id).await?;
                let execution_id = programs::program_execution_id(&state)?;
                let handle = client.get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(
                    execution_id.clone(),
                );
                let events = handle
                    .fetch_history(Default::default())
                    .into_events()
                    .await?;
                assert!(
                    continued_from(&events).is_some(),
                    "the last execution continued"
                );
                WorkflowReplayer::new(
                    WorkflowReplayerOptions::new()
                        .register_workflow::<temporal_workflow::ProgramRunWorkflow>()?
                        .build(),
                )?
                .replay_workflow(handle.fetch_history(Default::default()))
                .await?;

                // The run's report links each execution's report to the one
                // before, back to the first.
                let snapshot = handle
                    .query(
                        temporal_workflow::ProgramRunWorkflow::snapshot,
                        (),
                        temporalio_client::WorkflowQueryOptions::default(),
                    )
                    .await?;
                let Some(temporal_workflow::ProgramRunTerminal::Completed { result }) =
                    snapshot.terminal
                else {
                    anyhow::bail!("the run completed: {snapshot:?}");
                };
                let mut chain = Vec::new();
                let mut next = Some(result.report_ref.as_str().to_owned());
                while let Some(report_ref) = next {
                    let report = read_json(&store, &report_ref).await?;
                    chain.push((
                        report["descriptor"]["execution"].as_u64().unwrap_or(0),
                        report["execution"]["exit"].clone(),
                        report["scope"]["calls"]
                            .as_object()
                            .map(|calls| calls.len()),
                    ));
                    next = report["previous"].as_str().map(str::to_owned);
                }
                assert_eq!(
                    chain,
                    [
                        (2, json!("complete"), Some(1)),
                        (1, json!("continue_as_new"), Some(1)),
                        (0, json!("continue_as_new"), Some(1)),
                    ]
                );

                // Each execution had its own scope, and the session keeps
                // only the last two.
                assert_eq!(
                    state.code_tools.scopes.keys().cloned().collect::<Vec<_>>(),
                    [
                        harness::program_scope_id(&execution_id, 1),
                        harness::program_scope_id(&execution_id, 2),
                    ]
                );
                assert!(
                    state
                        .code_tools
                        .scopes
                        .values()
                        .all(|scope| scope.calls.len() == 1 && scope.calls.contains_key("call-1"))
                );

                // The next run starts from the state the run completed with,
                // not continued.
                let run = start_program_run(&api, &session_id, json!({"claim": "again"})).await?;
                let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output =
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
                assert_eq!(output["runs"], json!(1), "{output}");
                assert_eq!(output["seen"][0]["continued"], json!(false), "{output}");

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

/// Opens two requests and detaches one before continuing as new; the next
/// execution takes the detached one back and waits for its answer, and
/// reads what became of the other.
const HANDING_OVER_PROGRAM: &str = r#"
    export default {
        async run(input, env, ctx) {
            if (!ctx.continued) {
                const dropped = await env.input.open({ ask: "Drop it?", assignees: ["alice"] });
                const kept = await env.input.open({ ask: "Keep it?", assignees: ["alice"] });
                await kept.detach();
                return ctx.continueAsNew({ dropped: dropped.id, kept: kept.id });
            }
            const dropped = await env.input.handle(ctx.state.dropped);
            const droppedAs = await dropped.answer().then(() => "answered", (error) => error.kind);
            const kept = await env.input.handle(ctx.state.kept);
            return { claim: input.claim, slept: 0, droppedAs, answer: await kept.answer() };
        }
    };
"#;

/// The session's inbox requests by their ask: id and status.
async fn inbox_by_ask(
    api: &GatewayAgentApi,
    session_id: &SessionId,
) -> anyhow::Result<std::collections::BTreeMap<String, (String, api::InputRequestStatus)>> {
    Ok(api
        .list_inbox_requests(api::InboxRequestListParams {
            session: Some(session_id.to_string()),
            ..Default::default()
        })
        .await?
        .result
        .requests
        .into_iter()
        .map(|request| (request.ask, (request.request_id, request.status)))
        .collect())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn continue_as_new_withdraws_undetached_requests_and_carries_detached_ones()
-> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(
                    &profile_id,
                    HANDING_OVER_PROGRAM,
                    &[
                        ("input.open", None),
                        ("input.handle", None),
                        ("promise.await", None),
                        ("promise.detach", None),
                    ],
                ),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let run = start_program_run(&api, &session_id, json!({"claim": "hand over"})).await?;

            // The handover withdrew the request the program did not detach;
            // the detached one waits for its answer in the next execution.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let kept = loop {
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "the handover never settled: {:?}",
                    inbox_by_ask(&api, &session_id).await?
                );
                let requests = inbox_by_ask(&api, &session_id).await?;
                if let (Some((_, dropped)), Some((kept, api::InputRequestStatus::Pending))) =
                    (requests.get("Drop it?"), requests.get("Keep it?"))
                    && *dropped == api::InputRequestStatus::Cancelled
                {
                    break kept.clone();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            api.answer_inbox_request(api::InboxRequestAnswerParams {
                request_id: kept,
                approved: true,
                value: None,
                comment: None,
            })
            .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let output =
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
            assert_eq!(output["droppedAs"], json!("input_cancelled"), "{output}");
            assert_eq!(output["answer"]["approved"], json!(true), "{output}");

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

/// Makes `input.calls` calls per execution, continuing as new between them,
/// or with `input.spin` set, only spins before continuing.
const BUDGETED_PROGRAM: &str = r#"
    export default {
        async run(input, env, ctx) {
            const s = ctx.continued ? ctx.state : { executions: 0, calls: 0 };
            if (input.spin) {
                let x = 0;
                for (let i = 0; i < 4000000; i++) x = (x + i) % 1000003;
            }
            for (let i = 0; i < input.calls; i++) await env.time.sleep({ ms: 0 });
            return ctx.continueAsNew({ executions: s.executions + 1, calls: s.calls + input.calls });
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_runs_calls_and_compute_are_bounded_across_its_executions() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            let mut profile =
                program_profile(&profile_id, BUDGETED_PROGRAM, &[("time.sleep", None)]);
            profile.document.program.as_mut().unwrap().limits = Some(api::ProgramLimitsInput {
                max_calls: Some(10),
                max_in_flight: Some(4),
                max_compute: Some(2_000),
                ..Default::default()
            });
            api.create_profile(api::ProfileCreateParams { profile })
                .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let failure = |input: Value| {
                let api = api.clone();
                let store = store.clone();
                let session_id = session_id.clone();
                async move {
                    let run = start_program_run(&api, &session_id, input).await?;
                    let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                    anyhow::ensure!(run.status == api::RunStatus::Failed, "{run:?}");
                    let failure = run_failure(&store, &session_id).await?;
                    let report = read_json(
                        &store,
                        failure
                            .message_ref
                            .as_ref()
                            .expect("failure report")
                            .as_str(),
                    )
                    .await?;
                    Ok::<_, anyhow::Error>(report["error"].as_str().unwrap_or_default().to_owned())
                }
            };

            // Four calls an execution: the third execution's third call is
            // the run's eleventh, over its budget of ten.
            let error = failure(json!({"claim": "calls", "calls": 4})).await?;
            assert!(error.contains("tool call budget exhausted"), "{error}");
            // No calls, only work: the executions add up to the run's compute.
            let error = failure(json!({"claim": "spin", "calls": 0, "spin": true})).await?;
            assert!(error.contains("compute budget"), "{error}");

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

/// Small history limits, so a run reaches them in a few calls.
const SHORT_HISTORY: ProgramHistoryLimits = ProgramHistoryLimits {
    continue_events: 60,
    exhausted_events: 200,
};

/// Sleeps until it has made `input.calls` calls, continuing as new whenever
/// `ctx.shouldContinue()` says to, and counts each execution's calls.
const SHOULD_CONTINUE_PROGRAM: &str = r#"
    export default {
        async run(input, env, ctx) {
            const s = ctx.continued ? ctx.state : { done: 0, executions: [] };
            let calls = 0;
            while (s.done < input.calls) {
                if (ctx.shouldContinue()) {
                    return ctx.continueAsNew({ done: s.done, executions: [...s.executions, calls] });
                }
                await env.time.sleep({ ms: 0 });
                s.done++;
                calls++;
            }
            return { claim: input.claim, slept: s.done, executions: [...s.executions, calls] };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_continues_as_new_when_its_history_grows_long() -> anyhow::Result<()> {
    with_program_workers_history(SHORT_HISTORY, |api, store, session_id, _| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(
                    &profile_id,
                    SHOULD_CONTINUE_PROGRAM,
                    &[("time.sleep", None)],
                ),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let run =
                start_program_run(&api, &session_id, json!({"claim": "long", "calls": 30})).await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let output =
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
            assert_eq!(output["slept"], json!(30), "{output}");
            // Each execution went on until its history passed the threshold
            // of 60 events, then handed over: none got far past it.
            let executions: Vec<u64> = serde_json::from_value(output["executions"].clone())?;
            assert!(executions.len() >= 3, "{output}");
            assert_eq!(executions.iter().sum::<u64>(), 30, "{output}");
            assert!(
                executions[..executions.len() - 1]
                    .iter()
                    .all(|calls| (1..=10).contains(calls)),
                "{output}"
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

/// Opens a request nobody answers, then sleeps without ever asking
/// `ctx.shouldContinue()`.
const IGNORING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            await env.input.open({ ask: "Ship " + input.claim + "?", assignees: ["alice"] });
            for (let i = 0; i < input.calls; i++) await env.time.sleep({ ms: 0 });
            return { claim: input.claim, slept: input.calls };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_that_ignores_should_continue_fails_with_history_exhausted() -> anyhow::Result<()>
{
    with_program_workers_history(SHORT_HISTORY, |api, store, session_id, _| {
        Box::pin(async move {
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(
                    &profile_id,
                    IGNORING_PROGRAM,
                    &[
                        ("input.open", None),
                        ("promise.await", None),
                        ("promise.cancel", None),
                        ("time.sleep", None),
                    ],
                ),
            })
            .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "endless", "calls": 1000}),
            )
            .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Failed, "{run:?}");
            let failure = run_failure(&store, &session_id).await?;
            let document = read_json(
                &store,
                failure
                    .message_ref
                    .as_ref()
                    .expect("failure document")
                    .as_str(),
            )
            .await?;
            let report = read_json(
                &store,
                document["attempt_report_ref"]
                    .as_str()
                    .expect("attempt report"),
            )
            .await?;
            let error = &report["execution"]["error"];
            assert_eq!(error["kind"], json!("history_exhausted"), "{report}");
            let calls = report["execution"]["metrics"]["tool_calls"]
                .as_u64()
                .expect("call count");
            assert!((2..1000).contains(&calls), "{report}");

            // The run settled its scope as any exit does: every call has an
            // outcome, and the open request was withdrawn and settled.
            let state = programs::session_state(&store, &session_id).await?;
            let scope = only_program_scope(&state);
            assert!(scope.closed);
            assert_eq!(recorded_statuses(scope).len(), scope.calls.len());
            assert_eq!(
                harness::code_tool_scope_withdrawals(&state, scope),
                (1, 1),
                "{scope:?}"
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

const DELEGATING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const agent = input.agent;
            const both = await Promise.all(["one", "two"].map(word =>
                env.agent.run({ agent, input: "CHECK " + word, label: word })));
            const spawned = await env.agent.spawn({ agent, input: "CHECK three" });
            const third = await spawned.result();
            let failed = null;
            try { await env.agent.run({ agent, input: "FAIL please" }); }
            catch (error) { failed = [error.kind, error.message, error.value.status]; }
            let refused = null;
            try { await env.agent.run({ agent, input: "CHECK five" }); }
            catch (error) { refused = error.kind; }
            const results = both.concat([third]);
            return {
                claim: input.claim,
                slept: 0,
                outputs: results.map(result => result.output),
                statuses: results.map(result => result.status),
                spawned: typeof spawned.id === "string",
                failed,
                refused,
            };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_runs_child_agents_and_catches_their_failures() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let child_id = api::ProfileId::new(format!(
                "live_program_child_{}",
                uuid::Uuid::new_v4().simple()
            ));
            api.create_profile(api::ProfileCreateParams {
                profile: api::AgentProfileInput {
                    profile_id: child_id.clone(),
                    display_name: Some("Live child".to_owned()),
                    description: Some("Answers CHECK briefs".to_owned()),
                    document: api::ProfileDocument {
                        program: None,
                        metadata: Default::default(),
                        config: Some(api::SessionConfig::default()),
                        instructions: Some(api::ProfileInstructions::Text {
                            text: "You are a scripted child agent.".to_owned(),
                        }),
                        retention: None,
                    },
                },
            })
            .await?;
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            let mut profile = program_profile(
                &profile_id,
                DELEGATING_PROGRAM,
                &[
                    ("agent.run", None),
                    ("agent.spawn", None),
                    ("promise.await", None),
                ],
            );
            // Four children in all: the fifth is refused.
            profile.document.config = Some(api::SessionConfig {
                features: Some(api::FeaturesConfig {
                    subagents: Some(api::SubagentsFeature {
                        programs: Vec::new(),
                        version: api::CURRENT_FEATURE_VERSION,
                        agents: vec![api::SubagentAgentRef {
                            profile_id: child_id.clone(),
                        }],
                        max_depth: 2,
                        max_descendants: Some(4),
                        max_per_run: 4,
                        max_concurrent: 4,
                        deadline_ms: 120_000,
                    }),
                    ..api::FeaturesConfig::default()
                }),
                ..api::SessionConfig::default()
            });
            api.create_profile(api::ProfileCreateParams { profile })
                .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "delegated", "agent": child_id.as_str()}),
            )
            .await?;
            let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let output = run.output.as_ref().expect("program output");
            assert_eq!(
                read_json(&store, &output.content_ref).await?,
                json!({
                    "claim": "delegated",
                    "slept": 0,
                    "outputs": ["checked one", "checked two", "checked three"],
                    "statuses": ["completed", "completed", "completed"],
                    "spawned": true,
                    "failed": ["agent_failed", "child gave up", "failed"],
                    "refused": "agent_rejected",
                })
            );
            assert_eq!(
                scoped_call_tools(&store, &session_id).await?,
                [
                    "subagent.run",
                    "subagent.run",
                    "subagent.spawn",
                    "concurrency.await",
                    "subagent.run",
                    "subagent.run",
                ]
            );

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            for profile_id in [profile_id, child_id] {
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
            }
            Ok(())
        })
    })
    .await
}

const CHECKER_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            if (input.claim === "bad") throw new Error("no sources for " + input.claim);
            return { claim: input.claim, slept: 0, verdict: "supported" };
        }
    };
"#;

/// Sleeps `ms` before answering.
const SLOW_CHECKER_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            if (input.ms) await env.time.sleep({ ms: input.ms });
            return { claim: input.claim, slept: input.ms ?? 0, verdict: "supported" };
        }
    };
"#;

/// Spawns a child and returns its id, detaching it when asked; or takes
/// back the child `take` names and returns its result.
const KEEPING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            if (input.take) {
                try {
                    const child = await env.program.handle(input.take);
                    return { claim: input.claim, slept: 0, result: await child.result() };
                } catch (error) {
                    return { claim: input.claim, slept: 0, failed: error.message };
                }
            }
            const child = await env.program.spawn({
                program: input.checker,
                input: { claim: input.claim, ms: input.ms },
            });
            if (input.detach) await child.detach();
            return { claim: input.claim, slept: 0, id: child.id };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_later_run_takes_back_a_child_program_an_earlier_run_detached() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            let checker = api::ProfileId::new(format!("live_checker_{suffix}"));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(&checker, SLOW_CHECKER_PROGRAM, &[("time.sleep", None)]),
            })
            .await?;
            let profile_id = api::ProfileId::new(format!("live_program_{suffix}"));
            let mut profile = program_profile(
                &profile_id,
                KEEPING_PROGRAM,
                &[
                    ("program.spawn", None),
                    ("program.handle", None),
                    ("promise.await", None),
                    ("promise.detach", None),
                ],
            );
            profile
                .document
                .config
                .as_mut()
                .and_then(|config| config.features.as_mut())
                .expect("features")
                .subagents = Some(api::SubagentsFeature {
                version: api::CURRENT_FEATURE_VERSION,
                agents: Vec::new(),
                programs: vec![api::SubagentProgramRef {
                    profile_id: checker.clone(),
                }],
                max_depth: 2,
                max_descendants: None,
                max_per_run: 4,
                max_concurrent: 4,
                deadline_ms: 120_000,
            });
            api.create_profile(api::ProfileCreateParams { profile })
                .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            let output = |input: Value| {
                let api = api.clone();
                let store = store.clone();
                let session_id = session_id.clone();
                async move {
                    let run = start_program_run(&api, &session_id, input).await?;
                    let run = wait_for_terminal_run(&api, &session_id, &run.id).await?;
                    anyhow::ensure!(run.status == api::RunStatus::Completed, "{run:?}");
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await
                }
            };
            let checker = checker.as_str();

            // A child still running when its run ends, taken back by the next
            // run, which awaits it.
            let kept =
                output(json!({"claim": "slow", "checker": checker, "ms": 3000, "detach": true}))
                    .await?;
            let taken = output(json!({"claim": "take", "take": kept["id"]})).await?;
            assert_eq!(
                taken["result"],
                json!({"claim": "slow", "slept": 3000, "verdict": "supported"}),
                "{taken}"
            );

            // A child that finished between runs. (It sleeps a little: a
            // child already finished cannot be detached.)
            let kept =
                output(json!({"claim": "quick", "checker": checker, "ms": 500, "detach": true}))
                    .await?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            while !programs::session_state(&store, &session_id)
                .await?
                .promises
                .promises
                .values()
                .any(|promise| {
                    promise.scope == harness::PromiseScope::Session
                        && promise.status == harness::PromiseStatus::Resolved
                })
            {
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "the detached child never finished"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let taken = output(json!({"claim": "take", "take": kept["id"]})).await?;
            assert_eq!(
                taken["result"],
                json!({"claim": "quick", "slept": 500, "verdict": "supported"}),
                "{taken}"
            );

            // A child its run did not detach was cancelled at that run's exit.
            let dropped =
                output(json!({"claim": "dropped", "checker": checker, "ms": 3000})).await?;
            let id = dropped["id"].as_str().expect("id");
            let refused = output(json!({"claim": "take", "take": id})).await?;
            assert_eq!(
                refused["failed"],
                json!(format!(
                    "child program {id} was not detached by the run that started it"
                )),
                "{refused}"
            );

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            for profile_id in [profile_id, api::ProfileId::new(checker)] {
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
            }
            Ok(())
        })
    })
    .await
}

const PROGRAM_CALLER_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const ran = await env.program.run({ program: input.checker, input: { claim: "one" } });
            const child = await env.program.spawn({ program: input.checker, input: { claim: "two" } });
            const spawned = await child.result();
            let failed = null;
            try { await env.program.run({ program: input.checker, input: { claim: "bad" } }); }
            catch (error) { failed = [error.kind, error.message, error.value.status]; }
            let refused = null;
            try { await env.program.run({ program: input.greedy, input: { claim: "x" } }); }
            catch (error) { refused = [error.kind, error.value.reason]; }
            let invalid = null;
            try { await env.program.run({ program: input.checker, input: { nope: 1 } }); }
            catch (error) { invalid = error.kind; }
            return { claim: input.claim, slept: 0, ran, spawned, failed, refused, invalid };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_runs_child_programs_within_its_own_grants() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            let checker = api::ProfileId::new(format!("live_checker_{suffix}"));
            api.create_profile(api::ProfileCreateParams {
                profile: program_profile(&checker, CHECKER_PROGRAM, &[]),
            })
            .await?;
            // A child that also wants web fetch, which its caller lacks.
            let greedy = api::ProfileId::new(format!("live_greedy_{suffix}"));
            let mut greedy_profile = program_profile(&greedy, CHECKER_PROGRAM, &[]);
            greedy_profile
                .document
                .config
                .as_mut()
                .and_then(|config| config.features.as_mut())
                .expect("features")
                .web = Some(api::WebFeature {
                version: api::CURRENT_FEATURE_VERSION,
                fetch: Some(api::WebFetchFeature {}),
                search: None,
            });
            api.create_profile(api::ProfileCreateParams {
                profile: greedy_profile,
            })
            .await?;
            let profile_id = api::ProfileId::new(format!("live_program_{suffix}"));
            let mut profile = program_profile(
                &profile_id,
                PROGRAM_CALLER_PROGRAM,
                &[
                    ("program.run", None),
                    ("program.spawn", None),
                    ("promise.await", None),
                ],
            );
            let features = profile
                .document
                .config
                .as_mut()
                .and_then(|config| config.features.as_mut())
                .expect("features");
            features.subagents = Some(api::SubagentsFeature {
                version: api::CURRENT_FEATURE_VERSION,
                agents: Vec::new(),
                programs: [&checker, &greedy]
                    .into_iter()
                    .map(|profile_id| api::SubagentProgramRef {
                        profile_id: profile_id.clone(),
                    })
                    .collect(),
                max_depth: 2,
                max_descendants: Some(8),
                max_per_run: 8,
                max_concurrent: 4,
                deadline_ms: 120_000,
            });
            api.create_profile(api::ProfileCreateParams { profile })
                .await?;
            start_program_session(&api, &session_id, &profile_id).await?;

            let run = start_program_run(
                &api,
                &session_id,
                json!({"claim": "composed", "checker": checker.as_str(), "greedy": greedy.as_str()}),
            )
            .await?;
            let run = support::live::wait_for_terminal_run_with_timeout(
                &api,
                &session_id,
                &run.id,
                Duration::from_secs(120),
            )
            .await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let output =
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
            assert_eq!(
                output,
                json!({
                    "claim": "composed",
                    "slept": 0,
                    "ran": {"claim": "one", "slept": 0, "verdict": "supported"},
                    "spawned": {"claim": "two", "slept": 0, "verdict": "supported"},
                    "failed": ["program_failed", "javascript: no sources for bad", "failed"],
                    "refused": ["program_rejected", "grant_exceeded"],
                    "invalid": "program_rejected",
                })
            );

            // Three children ran (the greedy one and the invalid input never
            // did); each is a closed program session pinned to its profile.
            let children = api
                .list_sessions(api::SessionListParams {
                    trees: vec![session_id.as_str().to_owned()],
                    subagent: Some(true),
                    ..Default::default()
                })
                .await?
                .result
                .sessions;
            assert_eq!(children.len(), 3, "{children:?}");
            for child in &children {
                let view = api
                    .read_session(api::SessionReadParams {
                        session_id: child.id.as_str().to_owned(),
                        run_limit: None,
                    })
                    .await?
                    .result
                    .session;
                assert_eq!(view.status, api::SessionStatus::Closed);
                assert!(
                    matches!(
                        &view.driver,
                        api::SessionDriverView::Program { program_id: Some(id), revision: Some(1), .. }
                            if id == checker.as_str()
                    ),
                    "{:?}",
                    view.driver
                );
            }

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            for profile_id in [profile_id, checker, greedy] {
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn an_agent_runs_a_program_from_its_catalog() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            let checker = api::ProfileId::new(format!("live_checker_{suffix}"));
            let mut checker_profile = program_profile(&checker, CHECKER_PROGRAM, &[]);
            checker_profile.description = Some("Checks one claim.".to_owned());
            api.create_profile(api::ProfileCreateParams {
                profile: checker_profile,
            })
            .await?;
            // The agent holds what the program asks for (timers) and lists it.
            let agent = api::ProfileId::new(format!("live_agent_{suffix}"));
            api.create_profile(api::ProfileCreateParams {
                profile: api::AgentProfileInput {
                    profile_id: agent.clone(),
                    display_name: Some("Live agent".to_owned()),
                    description: None,
                    document: api::ProfileDocument {
                        program: None,
                        metadata: Default::default(),
                        config: Some(api::SessionConfig {
                            features: Some(api::FeaturesConfig {
                                timers: Some(api::TimersFeature {
                                    version: api::CURRENT_FEATURE_VERSION,
                                }),
                                subagents: Some(api::SubagentsFeature {
                                    version: api::CURRENT_FEATURE_VERSION,
                                    agents: Vec::new(),
                                    programs: vec![api::SubagentProgramRef {
                                        profile_id: checker.clone(),
                                    }],
                                    max_depth: 2,
                                    max_descendants: Some(4),
                                    max_per_run: 4,
                                    max_concurrent: 4,
                                    deadline_ms: 120_000,
                                }),
                                ..api::FeaturesConfig::default()
                            }),
                            ..api::SessionConfig::default()
                        }),
                        instructions: Some(api::ProfileInstructions::Text {
                            text: "You are a scripted agent.".to_owned(),
                        }),
                        retention: None,
                    },
                },
            })
            .await?;
            start_program_session(&api, &session_id, &agent).await?;
            let run = api
                .start_run(api::RunStartParams {
                    notify_on_terminal: None,
                    submission_id: None,
                    session_id: session_id.to_string(),
                    source: api::RunStartSource::Input {
                        items: vec![api::InputItem::Text {
                            provenance_ref: None,
                            origin: None,
                            text: format!("PROGRAM {checker} sky"),
                        }],
                    },
                    config: None,
                })
                .await?
                .result
                .run;
            let run = support::live::wait_for_terminal_run_with_timeout(
                &api,
                &session_id,
                &run.id,
                Duration::from_secs(120),
            )
            .await?;
            assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
            let answer =
                read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
            let answer = answer[0]["text"].as_str().expect("answer text");
            let said: Value = serde_json::from_str(
                answer
                    .strip_prefix("program said ")
                    .expect("the agent quotes the program"),
            )?;
            assert_eq!(
                said,
                json!({"claim": "sky", "slept": 0, "verdict": "supported"})
            );

            // The agent's catalog lists the program with its input schema.
            let view = read_session_view(&api, &session_id).await?;
            let catalog = view
                .active_context
                .entries
                .iter()
                .find(|entry| entry.key.as_deref() == Some("runtime.catalog.subagents"))
                .expect("sub-agent catalog entry");
            let catalog = store
                .read_text(&BlobRef::parse(catalog.content.content_ref.clone())?)
                .await?;
            assert!(
                catalog.contains(&format!("- {checker}"))
                    && catalog.contains("Checks one claim.")
                    && catalog
                        .contains(r#"input schema: {"properties":{"claim":{"type":"string"}}"#),
                "{catalog}"
            );

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            for profile_id in [agent, checker] {
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
            }
            Ok(())
        })
    })
    .await
}

const SEQUENTIAL_AGENTS_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            let ran = 0;
            let refused = null;
            for (let i = 0; i < input.calls; i++) {
                try {
                    await env.agent.run({ agent: input.agent, input: "CHECK " + i });
                    ran++;
                } catch (error) {
                    refused = [i, error.kind];
                    break;
                }
            }
            return { claim: input.claim, slept: 0, ran, refused };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn each_run_of_a_program_may_create_its_own_children_per_run() -> anyhow::Result<()> {
    with_program_workers(InputTimeouts::default(), |api, store, session_id| {
        Box::pin(async move {
            let child_id = api::ProfileId::new(format!(
                "live_program_child_{}",
                uuid::Uuid::new_v4().simple()
            ));
            api.create_profile(api::ProfileCreateParams {
                profile: api::AgentProfileInput {
                    profile_id: child_id.clone(),
                    display_name: Some("Live child".to_owned()),
                    description: Some("Answers CHECK briefs".to_owned()),
                    document: api::ProfileDocument {
                        program: None,
                        metadata: Default::default(),
                        config: Some(api::SessionConfig::default()),
                        instructions: Some(api::ProfileInstructions::Text {
                            text: "You are a scripted child agent.".to_owned(),
                        }),
                        retention: None,
                    },
                },
            })
            .await?;
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            let mut profile = program_profile(
                &profile_id,
                SEQUENTIAL_AGENTS_PROGRAM,
                &[("agent.run", None)],
            );
            // Two children a run, and no lifetime limit.
            profile.document.config = Some(api::SessionConfig {
                features: Some(api::FeaturesConfig {
                    subagents: Some(api::SubagentsFeature {
                        programs: Vec::new(),
                        version: api::CURRENT_FEATURE_VERSION,
                        agents: vec![api::SubagentAgentRef {
                            profile_id: child_id.clone(),
                        }],
                        max_depth: 2,
                        max_descendants: None,
                        max_per_run: 2,
                        max_concurrent: 4,
                        deadline_ms: 120_000,
                    }),
                    ..api::FeaturesConfig::default()
                }),
                ..api::SessionConfig::default()
            });
            api.create_profile(api::ProfileCreateParams { profile })
                .await?;
            start_program_session(&api, &session_id, &profile_id).await?;
            for claim in ["first", "second"] {
                let run = start_program_run(
                    &api,
                    &session_id,
                    json!({"claim": claim, "agent": child_id.as_str(), "calls": 3}),
                )
                .await?;
                let run = support::live::wait_for_terminal_run_with_timeout(
                    &api,
                    &session_id,
                    &run.id,
                    Duration::from_secs(120),
                )
                .await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output =
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
                assert_eq!(output["ran"], json!(2), "{claim}: {output}");
                assert_eq!(
                    output["refused"],
                    json!([2, "agent_rejected"]),
                    "{claim}: {output}"
                );
            }

            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            for profile_id in [profile_id, child_id] {
                api.delete_profile(api::ProfileDeleteParams { profile_id })
                    .await?;
            }
            Ok(())
        })
    })
    .await
}

/// Spawns `calls` child agents, then awaits every handle.
const SPAWNING_AGENTS_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const handles = [];
            for (let i = 0; i < input.calls; i++) {
                handles.push(await env.agent.spawn({ agent: input.agent, input: "CHECK " + i }));
            }
            const answers = [];
            for (const handle of handles) answers.push((await handle.result()).output);
            return { claim: input.claim, slept: 0, spawned: handles.length, answers };
        }
    };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_spawns_more_children_than_a_model_may_and_awaits_them_across_session_reloads()
-> anyhow::Result<()> {
    with_program_workers_setup(
        ProgramWorkerSetup {
            // Each reload leaves out the spawns the program acknowledged,
            // once their children finish.
            session_continue_as_new_events: Some(1),
            ..Default::default()
        },
        |api, store, session_id, _client| {
            Box::pin(async move {
                let child_id = api::ProfileId::new(format!(
                    "live_program_child_{}",
                    uuid::Uuid::new_v4().simple()
                ));
                api.create_profile(api::ProfileCreateParams {
                    profile: api::AgentProfileInput {
                        profile_id: child_id.clone(),
                        display_name: Some("Live child".to_owned()),
                        description: Some("Answers CHECK briefs".to_owned()),
                        document: api::ProfileDocument {
                            program: None,
                            metadata: Default::default(),
                            config: Some(api::SessionConfig::default()),
                            instructions: Some(api::ProfileInstructions::Text {
                                text: "You are a scripted child agent.".to_owned(),
                            }),
                            retention: None,
                        },
                    },
                })
                .await?;
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                let mut profile = program_profile(
                    &profile_id,
                    SPAWNING_AGENTS_PROGRAM,
                    &[("agent.spawn", None), ("promise.await", None)],
                );
                let calls = harness::MAX_WORKFLOW_TOOL_EMISSIONS_PER_RUN + 8;
                profile.document.config = Some(api::SessionConfig {
                    features: Some(api::FeaturesConfig {
                        subagents: Some(api::SubagentsFeature {
                            programs: Vec::new(),
                            version: api::CURRENT_FEATURE_VERSION,
                            agents: vec![api::SubagentAgentRef {
                                profile_id: child_id.clone(),
                            }],
                            max_depth: 2,
                            max_descendants: Some(64),
                            max_per_run: 64,
                            max_concurrent: 64,
                            deadline_ms: 120_000,
                        }),
                        ..api::FeaturesConfig::default()
                    }),
                    ..api::SessionConfig::default()
                });
                api.create_profile(api::ProfileCreateParams { profile })
                    .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                let run = start_program_run(
                    &api,
                    &session_id,
                    json!({"claim": "many", "agent": child_id.as_str(), "calls": calls}),
                )
                .await?;
                let run = support::live::wait_for_terminal_run_with_timeout(
                    &api,
                    &session_id,
                    &run.id,
                    Duration::from_secs(240),
                )
                .await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output =
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
                assert_eq!(output["spawned"], json!(calls), "{output}");
                let answers = output["answers"].as_array().expect("answers");
                assert_eq!(answers.len(), calls as usize, "{output}");
                for (index, answer) in answers.iter().enumerate() {
                    assert_eq!(answer, &json!(format!("checked {index}")));
                }

                // The full log replays and counts every spawn.
                let state = programs::session_state(&store, &session_id).await?;
                assert_eq!(
                    state.workflow_tools.emission_count(
                        harness::RunId::new(1),
                        &harness::WorkflowToolId::new(
                            tools::subagents::AGENT_SPAWN_WORKFLOW_TOOL_ID
                        )
                    ),
                    calls
                );

                let _ = api
                    .close_session(api::SessionCloseParams {
                        session_id: session_id.to_string(),
                        force: true,
                    })
                    .await;
                for profile_id in [profile_id, child_id] {
                    api.delete_profile(api::ProfileDeleteParams { profile_id })
                        .await?;
                }
                Ok(())
            })
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn a_program_keeps_its_per_run_call_cap_while_its_session_drops_finished_workflow_calls()
-> anyhow::Result<()> {
    with_program_workers_setup(
        ProgramWorkerSetup {
            // Each reload leaves out the child runs the program acknowledged.
            session_continue_as_new_events: Some(1),
            ..Default::default()
        },
        |api, store, session_id, client| {
            Box::pin(async move {
                let child_id = api::ProfileId::new(format!(
                    "live_program_child_{}",
                    uuid::Uuid::new_v4().simple()
                ));
                api.create_profile(api::ProfileCreateParams {
                    profile: api::AgentProfileInput {
                        profile_id: child_id.clone(),
                        display_name: Some("Live child".to_owned()),
                        description: Some("Answers CHECK briefs".to_owned()),
                        document: api::ProfileDocument {
                            program: None,
                            metadata: Default::default(),
                            config: Some(api::SessionConfig::default()),
                            instructions: Some(api::ProfileInstructions::Text {
                                text: "You are a scripted child agent.".to_owned(),
                            }),
                            retention: None,
                        },
                    },
                })
                .await?;
                let profile_id =
                    api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
                let mut profile = program_profile(
                    &profile_id,
                    SEQUENTIAL_AGENTS_PROGRAM,
                    &[("agent.run", None)],
                );
                let cap = 32;
                profile.document.program.as_mut().unwrap().limits = Some(api::ProgramLimitsInput {
                    max_workflow_calls: Some(cap),
                    ..Default::default()
                });
                // Enough descendants that the per-run cap is the limit.
                profile.document.config = Some(api::SessionConfig {
                    features: Some(api::FeaturesConfig {
                        subagents: Some(api::SubagentsFeature {
                            programs: Vec::new(),
                            version: api::CURRENT_FEATURE_VERSION,
                            agents: vec![api::SubagentAgentRef {
                                profile_id: child_id.clone(),
                            }],
                            max_depth: 2,
                            max_descendants: Some(40),
                            max_per_run: 40,
                            max_concurrent: 4,
                            deadline_ms: 120_000,
                        }),
                        ..api::FeaturesConfig::default()
                    }),
                    ..api::SessionConfig::default()
                });
                api.create_profile(api::ProfileCreateParams { profile })
                    .await?;
                start_program_session(&api, &session_id, &profile_id).await?;
                let run = start_program_run(
                    &api,
                    &session_id,
                    json!({"claim": "capped", "agent": child_id.as_str(), "calls": cap + 1}),
                )
                .await?;
                // Thirty-three child agents one after another.
                let run = support::live::wait_for_terminal_run_with_timeout(
                    &api,
                    &session_id,
                    &run.id,
                    Duration::from_secs(180),
                )
                .await?;
                assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                let output =
                    read_json(&store, &run.output.as_ref().expect("output").content_ref).await?;
                // The cap still counts the child runs the session dropped.
                assert_eq!(output["ran"], json!(cap), "{output}");
                assert_eq!(output["refused"][0], json!(cap), "{output}");

                // The full log replays: nothing past the cap was recorded.
                let state = programs::session_state(&store, &session_id).await?;
                let execution_id = programs::program_execution_id(&state)?;
                assert_eq!(
                    state.code_tools.scopes[&execution_id].calls.len(),
                    cap as usize
                );
                assert_eq!(
                    state.workflow_tools.emission_count(
                        harness::RunId::new(1),
                        &harness::WorkflowToolId::new(tools::subagents::AGENT_RUN_WORKFLOW_TOOL_ID)
                    ),
                    cap
                );
                // The run's report reads the session's memory, which had left
                // acknowledged calls out.
                let snapshot = client
                    .get_workflow_handle::<temporal_workflow::ProgramRunWorkflow>(execution_id)
                    .query(
                        temporal_workflow::ProgramRunWorkflow::snapshot,
                        (),
                        temporalio_client::WorkflowQueryOptions::default(),
                    )
                    .await?;
                let Some(temporal_workflow::ProgramRunTerminal::Completed { result }) =
                    snapshot.terminal
                else {
                    anyhow::bail!("the run completed: {snapshot:?}");
                };
                let report = read_json(&store, result.report_ref.as_str()).await?;
                let held = report["scope"]["calls"]
                    .as_object()
                    .map_or(0, |calls| calls.len());
                assert!(held < cap as usize, "{}", report["scope"]);

                let _ = api
                    .close_session(api::SessionCloseParams {
                        session_id: session_id.to_string(),
                        force: true,
                    })
                    .await;
                for profile_id in [profile_id, child_id] {
                    api.delete_profile(api::ProfileDeleteParams { profile_id })
                        .await?;
                }
                Ok(())
            })
        },
    )
    .await
}
