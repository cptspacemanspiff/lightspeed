//! A program session runs its pinned program on the code queue end to end:
//! a profile with a program, a session created from it, a program run with
//! JSON input whose scoped calls use the session's real tools, and a reply
//! that completes the run with the program's output. No model is involved.

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
    worker::{
        ActivityState, CodeWorkerActivities, SessionTools, WorkerActivities, code_worker,
        worker_runtime,
    },
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
            source: api::RunStartSource::Program { input },
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

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Temporal and PostgreSQL; run serially"]
async fn program_session_runs_its_pinned_program_and_returns_its_output() -> anyhow::Result<()> {
    let _lock = LIVE_TEST_LOCK.lock().await;
    require_storage_live_env()?;
    let store = pg_store_from_env().await?;
    seed_agent_default(&store, &support::live::openai_live_model()).await?;
    let code_queue = format!("program-live-{}", uuid::Uuid::new_v4().simple());
    let activity_queue = code_queue.clone();
    let activity_store = store.clone();
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
        move |client, queue, session_id| async move {
            let api = Arc::new(
                GatewayAgentApi::builder(client.clone(), store.clone())
                    .with_task_queue(queue)
                    .with_code_task_queue(code_queue.clone())
                    .build(),
            );
            let activities = CodeWorkerActivities::for_universe(live_universe_id()?, api.clone(), 2)?;
            let runtime = worker_runtime()?;
            let mut worker = code_worker(&runtime, client.clone(), code_queue, activities)?;
            let shutdown = worker.shutdown_handle();
            let worker_run = worker.run();
            tokio::pin!(worker_run);
            let body = Box::pin(async {
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
                Ok::<_, anyhow::Error>(())
            });
            let result = tokio::select! {
                result = body => result,
                result = &mut worker_run => Err(anyhow::anyhow!("code worker stopped early: {result:?}")),
            };
            shutdown();
            let stopped = tokio::time::timeout(Duration::from_secs(15), &mut worker_run).await;
            result?;
            stopped.map_err(|_| anyhow::anyhow!("code worker did not stop"))??;
            Ok(())
        },
    )
    .await
}
