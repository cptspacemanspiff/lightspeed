//! Live coverage for the `code_run` workflow tool: a session turn hands a
//! program to a code sub-agent, a child session whose model is the code
//! runtime. The program calls the parent's tools, including ones with
//! session effects, through the child's own engine; each pause is a turn in
//! the child, and the program's result resumes the parent's turn.
//!
//! Besides the local infrastructure, this needs Node and the repository's
//! npm dependencies: the test starts the code runtime's worker itself.

mod support;

use std::{process::Stdio, sync::Arc};

use api::{
    AgentApiService, InputItem, RunStartParams, RunStartSource, SessionConfig, SessionStartParams,
};
use api_projection::model_to_api;
use async_trait::async_trait;
use engine::{
    ContextEntryInput, ContextEntryKind, ContextMessageRole, CoreAgentIoError, CoreAgentLlm,
    CoreAgentTools, LlmFinish, LlmGenerationFacts, LlmGenerationRequest, LlmGenerationResult,
    LlmGenerationStatus, ModelSelection, ObservedToolCall, SessionId, ToolCallId, ToolName,
    storage::BlobStore,
};
use support::live::{
    LIVE_TEST_LOCK, final_assistant_text, require_storage_live_env, terminate_live_session,
    wait_for_terminal_run,
};
use temporal_server::{
    gateway::GatewayAgentApi,
    pg_store_from_env,
    subagents::AgentApiSubagentRuntime,
    worker::{
        ActivityState, CodeRuntimeDriver, SessionTools, WorkerActivities, core_runtime,
        worker_with_activities,
    },
};
use temporal_workflow::{DEFAULT_TEMPORAL_NAMESPACE, DEFAULT_TEMPORAL_TARGET, connect_temporal};
use tools::code_run::CODE_RUN_TOOL_NAME;

/// A program exercising the host path: a successful call to a granted tool,
/// a call to a tool the grant leaves out, and a result built from both.
const PROGRAM: &str = r#"
console.log("writing notes");
await tools.vfs_write_file({ path: "/workspace/notes.txt", content: "hello from a program" });
const [listing, text] = await Promise.all([
  tools.vfs_list_dir({ path: "/workspace" }),
  tools.vfs_read_file({ path: "/workspace/notes.txt" }),
]);
console.log("read back the notes");
return { sum: input.a + input.b, listed: JSON.stringify(listing).includes("notes.txt"), text };
"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires ./dev.sh infra or compatible Temporal + Postgres env, and Node"]
async fn temporal_live_code_run_calls_session_tools_from_a_program() -> anyhow::Result<()> {
    let _guard = LIVE_TEST_LOCK.lock().await;
    require_storage_live_env()?;

    let task_queue = format!("lightspeed-agent-live-{}", uuid::Uuid::new_v4().simple());
    let session_id = SessionId::new(format!("session_live_{}", uuid::Uuid::new_v4().simple()));
    let temporal_target =
        std::env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_TEMPORAL_TARGET.to_owned());
    let namespace = std::env::var("TEMPORAL_NAMESPACE")
        .unwrap_or_else(|_| DEFAULT_TEMPORAL_NAMESPACE.to_owned());

    let mut code_runtime = start_code_runtime(&temporal_target, &namespace)?;

    let runtime = core_runtime()?;
    let client = connect_temporal(&temporal_target, &namespace).await?;
    let store = pg_store_from_env().await?;
    let model = support::live::openai_live_model();
    support::live::seed_agent_default(&store, &model).await?;
    let api = Arc::new(
        GatewayAgentApi::builder(client.clone(), store.clone())
            .with_task_queue(task_queue.clone())
            .build(),
    );

    let blobs: Arc<dyn BlobStore> = store.clone();
    let driver = CodeRuntimeDriver::new(client.clone(), blobs.clone(), store.config().universe_id);
    let llm = Arc::new(CodeScriptedLlm { blobs, driver });
    let hosted = Arc::new(SessionTools::from_pg_store(store.clone()));
    let activities = WorkerActivities::for_universe(
        store.config().universe_id,
        ActivityState::from_pg_store(
            store.clone(),
            llm as Arc<dyn CoreAgentLlm>,
            hosted.clone() as Arc<dyn CoreAgentTools>,
        )
        .with_hosted_tools(hosted)
        .with_workflow_tool_executions(client.clone())
        .with_subagent_runtime(Arc::new(AgentApiSubagentRuntime::new(api.clone()))),
    );
    let mut worker =
        worker_with_activities(&runtime, client.clone(), task_queue.clone(), activities)?;
    let shutdown_worker = worker.shutdown_handle();
    let worker_future = worker.run();
    tokio::pin!(worker_future);

    let client_future = temporal_server::gateway::request_context::with_request_context(
        support::live::local_request_context().await?,
        run_program_client(api.clone(), &session_id, &model),
    );
    tokio::pin!(client_future);

    let client_result = tokio::select! {
        worker_result = worker_future.as_mut() => {
            return match worker_result {
                Ok(()) => Err(anyhow::anyhow!("Temporal worker stopped before the live code run test completed")),
                Err(error) => Err(error.context("Temporal worker failed")),
            };
        }
        exit = code_runtime.wait() => {
            return Err(anyhow::anyhow!("the code runtime worker exited early: {exit:?}"));
        }
        client_result = support::live::bounded_live_test("code_run_live", support::live::LIVE_TEST_BUDGET, client_future.as_mut()) => client_result,
    };

    terminate_live_session(&client, &session_id, "code run live test cleanup").await;
    shutdown_worker();
    let shutdown_result = support::live::bounded_live_test(
        "code run worker shutdown",
        std::time::Duration::from_secs(30),
        worker_future.as_mut(),
    )
    .await;
    let _ = code_runtime.kill().await;
    client_result.and(shutdown_result)
}

/// Start the code runtime's worker on its default task queue.
fn start_code_runtime(
    temporal_target: &str,
    namespace: &str,
) -> anyhow::Result<tokio::process::Child> {
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    // One process, with tsx as a loader, so killing it stops the worker.
    tokio::process::Command::new("node")
        .args(["--import", "tsx", "platform/code-runtime/src/main.ts"])
        .current_dir(&repository)
        .env("TEMPORAL_ADDRESS", temporal_target)
        .env("TEMPORAL_NAMESPACE", namespace)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| anyhow::anyhow!("could not start the code runtime with node: {error}"))
}

async fn run_program_client(
    api: Arc<GatewayAgentApi>,
    session_id: &SessionId,
    model: &ModelSelection,
) -> anyhow::Result<()> {
    let workspace = api
        .create_vfs_workspace(api::VfsWorkspaceCreateParams::default())
        .await?
        .result
        .workspace;
    api.start_session(SessionStartParams {
        access: None,
        metadata: Default::default(),
        session_id: Some(session_id.as_str().to_owned()),
        display_name: None,
        config: Some(SessionConfig {
            model: Some(model_to_api(model).into()),
            features: Some(api::FeaturesConfig {
                vfs: Some(api::VfsFeature {
                    version: api::CURRENT_FEATURE_VERSION,
                    working_directory: None,
                    workspaces: vec![api::WorkspaceAttachment {
                        path: "/workspace".to_owned(),
                        workspace_id: Some(workspace.workspace_id.clone()),
                        snapshot_ref: None,
                        access: api::WorkspaceAccess::Edit,
                    }],
                    prompts: None,
                    skills: None,
                }),
                code: Some(api::CodeFeature {
                    version: api::CURRENT_FEATURE_VERSION,
                    max_calls: Some(10),
                    max_concurrent_calls: None,
                    deadline_ms: Some(120_000),
                }),
                ..Default::default()
            }),
            ..SessionConfig::default()
        }),
        profile: None,
        delete_after_close_ms: None,
    })
    .await?;
    let run = api
        .start_run(RunStartParams {
            notify_on_terminal: None,
            submission_id: None,
            session_id: session_id.as_str().to_owned(),
            source: RunStartSource::Input {
                items: vec![InputItem::Text {
                    provenance_ref: None,
                    origin: None,
                    text: "RUN".to_owned(),
                }],
            },
            config: None,
        })
        .await?;
    let finished = wait_for_terminal_run(api.as_ref(), session_id, &run.result.run.id).await?;
    assert_eq!(finished.status, api::RunStatus::Completed);
    let output = final_assistant_text(&finished).expect("final assistant output");
    // The parent received the program's result through the sub-agent
    // envelope: the write took effect, and the program saw its own file.
    assert!(
        output.contains(r#""status":"completed""#) && output.contains(r#""agent":"code""#),
        "expected a completed code sub-agent, got: {output}"
    );
    assert!(
        output.contains(r#"\"sum\":5"#) && output.contains(r#"\"listed\":true"#),
        "expected the program's result, got: {output}"
    );
    assert!(
        output.contains("hello from a program"),
        "expected the file the program wrote and read, got: {output}"
    );

    // The child is a sub-agent session whose turns record the program's
    // progress: its log lines and the calls it paused on.
    let envelope_start = output.find('{').expect("envelope");
    let envelope: serde_json::Value =
        serde_json::from_str(&output[envelope_start..]).expect("envelope json");
    let child_id = envelope["session_id"]
        .as_str()
        .expect("child session id")
        .to_owned();
    let child = api
        .read_session(api::SessionReadParams {
            session_id: child_id.clone(),
            run_limit: Some(1),
        })
        .await?
        .result
        .session;
    assert_eq!(
        child
            .origin
            .as_ref()
            .map(|origin| origin.parent_session_id.as_str()),
        Some(session_id.as_str())
    );
    let child_run =
        support::live::read_run(api.as_ref(), &SessionId::new(child_id), &child.runs[0].id)
            .await?
            .expect("child run");
    let calls: Vec<_> = child_run
        .tool_batches
        .iter()
        .map(|batch| {
            batch
                .calls
                .iter()
                .map(|call| call.tool_name.clone())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        calls,
        [
            vec!["vfs_write_file".to_owned()],
            vec!["vfs_list_dir".to_owned(), "vfs_read_file".to_owned()]
        ],
        "each pause is one batch in the child"
    );
    let child_text = serde_json::to_string(&child_run).expect("child run json");
    assert!(
        child_text.contains("writing notes"),
        "expected the first log line in the child"
    );
    assert!(
        child_text.contains("read back the notes"),
        "expected the last log line in the child"
    );
    Ok(())
}

/// Scripted session model: `RUN` emits one `code_run` call, and any turn
/// that sees a tool result ends the run with it.
struct CodeScriptedLlm {
    blobs: Arc<dyn BlobStore>,
    /// Code sub-agents' generations go to the code runtime.
    driver: CodeRuntimeDriver,
}

impl CodeScriptedLlm {
    async fn texts(
        &self,
        request: &LlmGenerationRequest,
        matches_kind: impl Fn(&ContextEntryKind) -> bool,
    ) -> Result<Vec<String>, CoreAgentIoError> {
        let mut texts = Vec::new();
        for entry in &request.request.context.entries {
            if matches_kind(&entry.kind) {
                texts.push(
                    self.blobs
                        .read_text(&entry.content.content_ref)
                        .await
                        .map_err(io_error)?,
                );
            }
        }
        Ok(texts)
    }

    fn result(
        request: &LlmGenerationRequest,
        context_entries: Vec<ContextEntryInput>,
        finish: LlmFinish,
        tool_calls: Vec<ObservedToolCall>,
    ) -> LlmGenerationResult {
        LlmGenerationResult {
            run_id: request.run_id,
            turn_id: request.turn_id,
            status: LlmGenerationStatus::Succeeded,
            failure_ref: None,
            context_entries,
            facts: LlmGenerationFacts {
                duration_ms: None,
                provider_response_id: None,
                finish,
                usage: None,
                tool_calls,
                approval_requests: Vec::new(),
                context_token_estimate: None,
            },
        }
    }

    async fn text_result(
        &self,
        request: &LlmGenerationRequest,
        text: String,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
        let content_ref = self
            .blobs
            .put_bytes(text.into_bytes())
            .await
            .map_err(io_error)?;
        Ok(Self::result(
            request,
            vec![ContextEntryInput {
                kind: ContextEntryKind::Message {
                    role: ContextMessageRole::Assistant,
                },
                content: engine::ContentRef::text(content_ref),
                preview: None,
                origin: None,
                provenance_ref: None,
                token_estimate: None,
            }],
            LlmFinish::Stop,
            Vec::new(),
        ))
    }

    async fn program_call(
        &self,
        request: &LlmGenerationRequest,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
        let Some(tool_id) = test_support::scripted_tool_id(request, CODE_RUN_TOOL_NAME) else {
            return Err(CoreAgentIoError::Failed {
                message: "the code grant must advertise code_run".to_owned(),
            });
        };
        let arguments = serde_json::json!({
            "source": PROGRAM,
            "input": { "a": 2, "b": 3 },
            "label": "List the workspace",
        });
        let arguments_ref = self
            .blobs
            .put_bytes(serde_json::to_vec(&arguments).map_err(io_error)?)
            .await
            .map_err(io_error)?;
        let call_id = ToolCallId::new("code_call_0");
        let name = ToolName::new(CODE_RUN_TOOL_NAME);
        Ok(Self::result(
            request,
            vec![ContextEntryInput {
                kind: ContextEntryKind::ToolCall {
                    call_id: call_id.clone(),
                    name: name.clone(),
                },
                content: engine::ContentRef {
                    content_ref: arguments_ref.clone(),
                    media_type: Some("application/json".to_owned()),
                    provider_kind: Some("code-script".to_owned()),
                },
                preview: None,
                origin: None,
                provenance_ref: None,
                token_estimate: None,
            }],
            LlmFinish::ToolCalls,
            vec![ObservedToolCall {
                call_id,
                tool_id: Some(tool_id),
                tool_name: name,
                provider_kind: Some("code-script".to_owned()),
                arguments_ref,
                native_call_ref: None,
            }],
        ))
    }
}

#[async_trait]
impl CoreAgentLlm for CodeScriptedLlm {
    async fn generate(
        &self,
        request: LlmGenerationRequest,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
        if request.request.model.api_kind == engine::ProviderApiKind::CodeRuntime {
            return llm_runtime::LlmGenerationAdapter::generate(&self.driver, request)
                .await
                .map(|execution| execution.result)
                .map_err(io_error);
        }
        let results = self
            .texts(&request, |kind| {
                matches!(kind, ContextEntryKind::ToolResult { .. })
            })
            .await?;
        if !results.is_empty() {
            return self
                .text_result(&request, format!("results: {}", results.join(" | ")))
                .await;
        }
        let user_messages = self
            .texts(&request, |kind| {
                matches!(
                    kind,
                    ContextEntryKind::Message {
                        role: ContextMessageRole::User
                    }
                )
            })
            .await?;
        if user_messages.last().is_some_and(|text| text == "RUN") {
            return self.program_call(&request).await;
        }
        self.text_result(&request, "scripted run completed".to_owned())
            .await
    }
}

fn io_error(error: impl std::fmt::Display) -> CoreAgentIoError {
    CoreAgentIoError::Failed {
        message: error.to_string(),
    }
}
