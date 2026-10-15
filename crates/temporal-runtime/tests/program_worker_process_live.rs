//! Lose the code worker process while a program waits on a person, either
//! during its attempt or after the attempt suspended, answer, and check a
//! replacement process finishes the run by replaying the recorded calls
//! instead of making them again. Session workers run in this process; the
//! code worker is a real child process.

#[path = "support/programs.rs"]
mod programs;
mod support;

use std::{process::Stdio, sync::Arc, time::Duration};

use api::AgentApiService;
use async_trait::async_trait;
use harness::{
    CoreAgentIoError, CoreAgentLlm, LlmGenerationRequest, LlmGenerationResult, SessionId,
    storage::BlobStore,
};
use programs::{admitted_tools, attempt_activities, program_execution_id, session_state};
use serde_json::{Value, json};
use support::live::{
    LIVE_TEST_LOCK, require_storage_live_env, run_with_live_worker_builder, seed_agent_default,
    wait_for_terminal_run_with_timeout, wait_until,
};
use temporal_runtime::program::PROGRAM_HOLD_WINDOW_ENV;
use temporal_runtime::{
    gateway::GatewayAgentApi,
    pg_store_from_env,
    worker::{ActivityState, SessionTools, WorkerActivities},
};
use temporal_workflow::{ProgramRunPhase, ProgramRunWorkflow};
use temporalio_client::{WorkflowExecutionStatus, WorkflowQueryOptions};
use temporalio_sdk::workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions};
use tokio::process::{Child, Command};

/// A program session never plans a model turn, so any generation is a bug.
struct NoModel;

#[async_trait]
impl CoreAgentLlm for NoModel {
    async fn generate(
        &self,
        request: LlmGenerationRequest,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
        Err(CoreAgentIoError::Failed {
            message: format!("program sessions must not generate; run {}", request.run_id),
        })
    }
}

const RESUMING_PROGRAM: &str = r#"
    export default {
        async run(input, env) {
            const slept = await env.time.sleep({ ms: 5 });
            const answer = await env.input.request({ ask: `Resume ${input.claim}?` });
            return {
                claim: input.claim,
                slept: slept.fire_at_ms > 0 ? 1 : 0,
                approved: answer.approved,
            };
        }
    };
"#;

struct CodeProcess {
    child: Child,
    log: tempfile::NamedTempFile,
}

impl CodeProcess {
    fn start_with(
        session_queue: &str,
        code_queue: &str,
        env: &[(&str, &str)],
    ) -> anyhow::Result<Self> {
        let log = tempfile::NamedTempFile::new()?;
        let child = Command::new(env!("CARGO_BIN_EXE_lightspeed-runtime"))
            .args([
                "--roles",
                "code",
                "--task-queue",
                session_queue,
                "--code-task-queue",
                code_queue,
            ])
            .env("LIGHTSPEED_AUTH_MODE", "authenticated")
            // Programs here use no environment tools. These child-local
            // values satisfy the shared deployment client's configuration.
            .env("LIGHTSPEED_ENVIRONMENT_GATEWAY_URL", "http://127.0.0.1:1")
            .env(
                "LIGHTSPEED_ENVIRONMENT_GATEWAY_TOKEN",
                "unused-code-test-route",
            )
            .env("RUST_LOG", "warn")
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .stdout(log.as_file().try_clone()?)
            .stderr(log.as_file().try_clone()?)
            .kill_on_drop(true)
            .spawn()?;
        Ok(Self { child, log })
    }

    fn ensure_running(&mut self) -> anyhow::Result<()> {
        if let Some(status) = self.child.try_wait()? {
            let log = std::fs::read_to_string(self.log.path())?;
            anyhow::bail!("owned code worker exited with {status}: {log}");
        }
        Ok(())
    }

    async fn kill_and_reap(&mut self) -> anyhow::Result<()> {
        self.child.kill().await?;
        self.child.wait().await?;
        Ok(())
    }
}

fn profile(profile_id: &api::ProfileId) -> api::AgentProfileInput {
    api::AgentProfileInput {
        profile_id: profile_id.clone(),
        display_name: Some("Resuming program".to_owned()),
        description: None,
        document: api::ProfileDocument {
            program: Some(api::ProgramSpec {
                source: api::ProgramSource::Text {
                    text: RESUMING_PROGRAM.to_owned(),
                },
                semantics_version: 1,
                input_schema: Some(json!({
                    "type": "object",
                    "required": ["claim"],
                    "properties": {"claim": {"type": "string"}},
                })),
                output_schema: Some(json!({
                    "type": "object",
                    "required": ["claim", "slept", "approved"],
                })),
                limits: None,
                requires: ["time.sleep", "input.request"]
                    .into_iter()
                    .map(|contract| api::ProgramContractRequirement {
                        contract: contract.to_owned(),
                        digest: None,
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

/// How the code worker is lost in [`resume_after_losing_the_code_worker`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Loss {
    /// While the attempt holds the program waiting on its question.
    DuringAttempt,
    /// After the attempt suspended, when no attempt is running.
    WhileSuspended,
}

/// Run the resuming program on a child code worker, lose that worker as
/// `loss` says, answer the question, and finish on a replacement worker.
/// Checks the run's output and that no call was admitted twice, then
/// returns, for each attempt activity the run scheduled, the Temporal
/// attempts it started.
async fn resume_after_losing_the_code_worker(loss: Loss) -> anyhow::Result<Vec<Vec<i32>>> {
    let _lock = LIVE_TEST_LOCK.lock().await;
    require_storage_live_env()?;
    let store = pg_store_from_env().await?;
    seed_agent_default(&store, &support::live::openai_live_model()).await?;
    let activity_store = store.clone();
    let code_queue = format!("program-process-live-{}", uuid::Uuid::new_v4().simple());
    let activity_queue = code_queue.clone();
    // A short hold window suspends the waiting program within a second.
    let env: &[(&str, &str)] = match loss {
        Loss::DuringAttempt => &[],
        Loss::WhileSuspended => &[(PROGRAM_HOLD_WINDOW_ENV, "1000")],
    };
    let mut attempts = Vec::new();
    let attempts_out = &mut attempts;
    run_with_live_worker_builder(
        move |client, _| async move {
            let tools = Arc::new(SessionTools::from_pg_store(activity_store.clone()));
            Ok(WorkerActivities::for_universe(
                activity_store.config().universe_id,
                ActivityState::from_pg_store(activity_store, Arc::new(NoModel), tools.clone())
                    .with_hosted_tools(tools)
                    .with_workflow_tool_executions(client)
                    .with_code_task_queue(activity_queue),
            ))
        },
        move |client, session_queue, session_id| async move {
            let api = GatewayAgentApi::builder(client.clone(), store.clone())
                .with_task_queue(session_queue.clone())
                .with_code_task_queue(code_queue.clone())
                .build();
            let profile_id =
                api::ProfileId::new(format!("live_program_{}", uuid::Uuid::new_v4().simple()));
            api.create_profile(api::ProfileCreateParams {
                profile: profile(&profile_id),
            })
            .await?;
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
            let mut original = CodeProcess::start_with(&session_queue, &code_queue, env)?;
            let result = async {
                let run = api
                    .start_run(api::RunStartParams {
                        notify_on_terminal: None,
                        submission_id: None,
                        session_id: session_id.to_string(),
                        source: api::RunStartSource::Program {
                            input: json!({"claim": "pr-9"}),
                            input_defaults: None,
                        },
                        config: None,
                    })
                    .await?
                    .result
                    .run;

                // The attempt has slept and now waits on its question.
                let mut request = None;
                wait_until(
                    "the program's question",
                    Duration::from_secs(30),
                    async || {
                        original.ensure_running()?;
                        request = inbox(&api, &session_id).await?.into_iter().next();
                        Ok(request.is_some())
                    },
                )
                .await?;
                let request = request.expect("a request");
                assert_eq!(request.ask, "Resume pr-9?");
                let state = session_state(&store, &session_id).await?;
                assert_eq!(admitted_tools(&state), ["time.sleep", "input.request"]);
                let handle =
                    client.get_workflow_handle::<ProgramRunWorkflow>(program_execution_id(&state)?);
                if loss == Loss::WhileSuspended {
                    wait_until(
                        "the attempt's suspension",
                        Duration::from_secs(30),
                        async || {
                            original.ensure_running()?;
                            let snapshot = handle
                                .query(
                                    ProgramRunWorkflow::snapshot,
                                    (),
                                    WorkflowQueryOptions::default(),
                                )
                                .await?;
                            Ok(snapshot.phase == ProgramRunPhase::Suspended)
                        },
                    )
                    .await?;
                }

                // Lose the worker, then answer while a replacement takes over.
                original.kill_and_reap().await?;
                let mut replacement = CodeProcess::start_with(&session_queue, &code_queue, env)?;
                let resumed = async {
                    api.answer_inbox_request(api::InboxRequestAnswerParams {
                        request_id: request.request_id.clone(),
                        approved: true,
                        value: None,
                        comment: None,
                    })
                    .await?;
                    let run = wait_for_terminal_run_with_timeout(
                        &api,
                        &session_id,
                        &run.id,
                        Duration::from_secs(90),
                    )
                    .await?;
                    replacement.ensure_running()?;
                    assert_eq!(run.status, api::RunStatus::Completed, "{run:?}");
                    let output: Value = serde_json::from_slice(
                        &store
                            .read_bytes(&harness::BlobRef::parse(
                                &run.output.as_ref().expect("output").content_ref,
                            )?)
                            .await?,
                    )?;
                    assert_eq!(
                        output,
                        json!({"claim": "pr-9", "slept": 1, "approved": true})
                    );
                    // The replay re-issued both calls; the session admitted
                    // neither again and asked only once.
                    let state = session_state(&store, &session_id).await?;
                    assert_eq!(admitted_tools(&state), ["time.sleep", "input.request"]);
                    let requests = inbox(&api, &session_id).await?;
                    assert_eq!(requests.len(), 1, "{requests:?}");

                    wait_until(
                        "program workflow closure",
                        Duration::from_secs(15),
                        async || {
                            Ok(handle.describe(Default::default()).await?.status()
                                == WorkflowExecutionStatus::Completed)
                        },
                    )
                    .await?;
                    let events = handle
                        .fetch_history(Default::default())
                        .into_events()
                        .await?;
                    *attempts_out = attempt_activities(&events);
                    WorkflowReplayer::new(
                        WorkflowReplayerOptions::new()
                            .register_workflow::<ProgramRunWorkflow>()?
                            .build(),
                    )?
                    .replay_workflow(handle.fetch_history(Default::default()))
                    .await?;
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                let stopped = replacement.kill_and_reap().await;
                resumed.and(stopped)
            }
            .await;
            let _ = api
                .close_session(api::SessionCloseParams {
                    session_id: session_id.to_string(),
                    force: true,
                })
                .await;
            let _ = api
                .delete_profile(api::ProfileDeleteParams { profile_id })
                .await;
            result
        },
    )
    .await?;
    Ok(attempts)
}

async fn inbox(
    api: &GatewayAgentApi,
    session_id: &SessionId,
) -> anyhow::Result<Vec<api::InputRequestView>> {
    Ok(api
        .list_inbox_requests(api::InboxRequestListParams {
            session: Some(session_id.to_string()),
            ..Default::default()
        })
        .await?
        .result
        .requests)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; launches and kills its own code worker; run serially"]
async fn a_program_attempt_lost_with_its_worker_replays_on_another_process() -> anyhow::Result<()> {
    // One attempt activity, completed on its retry.
    assert_eq!(
        resume_after_losing_the_code_worker(Loss::DuringAttempt).await?,
        [vec![2]]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; launches and kills its own code worker; run serially"]
async fn a_suspended_program_resumes_on_a_new_worker_when_its_question_is_answered()
-> anyhow::Result<()> {
    // The first attempt suspended; the answer woke a second, which replayed
    // the program on the replacement worker. Neither was retried.
    assert_eq!(
        resume_after_losing_the_code_worker(Loss::WhileSuspended).await?,
        [vec![1], vec![1]]
    );
    Ok(())
}
