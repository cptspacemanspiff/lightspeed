//! Live coverage for the `model_generate` workflow tool: a session turn calls
//! the tool, the start-on-call execution runs one standalone generation
//! through the worker's LLM runtime, and the joined reply resumes the turn.

mod support;

use std::{
    env,
    sync::{Arc, Mutex},
};

use api::{
    AgentApiService, InputItem, RunStartParams, RunStartSource, SessionConfig, SessionStartParams,
};
use api_projection::model_to_api;
use async_trait::async_trait;
use engine::{
    ContextEntryInput, ContextEntryKind, ContextMessageRole, CoreAgentIoError, CoreAgentLlm,
    CoreAgentTools, LlmFinish, LlmGenerationFacts, LlmGenerationRequest, LlmGenerationResult,
    LlmGenerationStatus, LlmRequest, ModelSelection, ObservedToolCall, SessionId, ToolCallId,
    ToolName, storage::BlobStore,
};
use support::live::{
    LIVE_TEST_LOCK, final_assistant_text, require_storage_live_env, terminate_live_session,
    wait_for_terminal_run,
};
use temporal_server::{
    gateway::GatewayAgentApi,
    pg_store_from_env,
    worker::{ActivityState, SessionTools, WorkerActivities, core_runtime, worker_with_activities},
};
use temporal_workflow::{DEFAULT_TEMPORAL_NAMESPACE, DEFAULT_TEMPORAL_TARGET, connect_temporal};
use tools::model_generate::MODEL_GENERATE_TOOL_NAME;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires ./dev.sh infra or compatible Temporal + Postgres env"]
async fn temporal_live_model_generate_returns_validated_json_inline() -> anyhow::Result<()> {
    let _guard = LIVE_TEST_LOCK.lock().await;
    require_storage_live_env()?;

    let task_queue = format!("lightspeed-agent-live-{}", uuid::Uuid::new_v4().simple());
    let session_id = SessionId::new(format!("session_live_{}", uuid::Uuid::new_v4().simple()));
    let temporal_target =
        env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_TEMPORAL_TARGET.to_owned());
    let namespace =
        env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| DEFAULT_TEMPORAL_NAMESPACE.to_owned());

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
    let llm = Arc::new(GenerateScriptedLlm::new(blobs.clone()));
    let hosted = Arc::new(SessionTools::from_pg_store(store.clone()));
    let activities = WorkerActivities::for_universe(
        store.config().universe_id,
        ActivityState::from_pg_store(
            store.clone(),
            llm.clone() as Arc<dyn CoreAgentLlm>,
            hosted.clone() as Arc<dyn CoreAgentTools>,
        )
        .with_hosted_tools(hosted)
        .with_workflow_tool_executions(client.clone()),
    );
    let mut worker =
        worker_with_activities(&runtime, client.clone(), task_queue.clone(), activities)?;
    let shutdown_worker = worker.shutdown_handle();
    let worker_future = worker.run();
    tokio::pin!(worker_future);

    let grader = ModelSelection {
        model: "grader-model".to_owned(),
        ..model.clone()
    };
    let client_future = temporal_server::gateway::request_context::with_request_context(
        support::live::local_request_context().await?,
        run_generation_client(api.clone(), &session_id, &model, &grader, llm.clone()),
    );
    tokio::pin!(client_future);

    let client_result = tokio::select! {
        worker_result = worker_future.as_mut() => {
            return match worker_result {
                Ok(()) => Err(anyhow::anyhow!("Temporal worker stopped before the live generation test completed")),
                Err(error) => Err(error.context("Temporal worker failed")),
            };
        }
        client_result = support::live::bounded_live_test("model_generate_live", support::live::LIVE_TEST_BUDGET, client_future.as_mut()) => client_result,
    };

    terminate_live_session(&client, &session_id, "model generation live test cleanup").await;
    shutdown_worker();
    let shutdown_result = support::live::bounded_live_test(
        "model generation worker shutdown",
        std::time::Duration::from_secs(30),
        worker_future.as_mut(),
    )
    .await;
    client_result.and(shutdown_result)
}

async fn run_generation_client(
    api: Arc<GatewayAgentApi>,
    session_id: &SessionId,
    model: &ModelSelection,
    grader: &ModelSelection,
    llm: Arc<GenerateScriptedLlm>,
) -> anyhow::Result<()> {
    api.start_session(SessionStartParams {
        access: None,
        metadata: Default::default(),
        session_id: Some(session_id.as_str().to_owned()),
        display_name: None,
        config: Some(SessionConfig {
            model: Some(model_to_api(model).into()),
            features: Some(api::FeaturesConfig {
                generate: Some(api::GenerateFeature {
                    version: api::CURRENT_FEATURE_VERSION,
                    models: vec![api::GenerateModelOption {
                        name: "grader".to_owned(),
                        description: Some("Grades claims".to_owned()),
                        model: model_to_api(grader).into(),
                        reasoning_effort: None,
                        max_output_tokens: Some(400),
                    }],
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
                    text: "GENERATE".to_owned(),
                }],
            },
            config: None,
        })
        .await?;
    let run_id = run.result.run.id;

    let finished = wait_for_terminal_run(api.as_ref(), session_id, &run_id).await?;
    assert_eq!(finished.status, api::RunStatus::Completed);
    let output = final_assistant_text(&finished).expect("final assistant output");
    // The granted call resolves with the validated value and the model it used.
    assert!(
        output.contains(r#""json":{"verdict":"pass"}"#) && output.contains(r#""name":"grader""#),
        "expected the validated generation inline, got: {output}"
    );
    // A reply that breaks the output schema fails its call with the text.
    assert!(
        output.contains("output_invalid") && output.contains("maybe"),
        "expected the schema failure inline, got: {output}"
    );
    // An ungranted model never starts a generation.
    assert!(
        output.contains(r#""large" is not granted"#),
        "expected the grant refusal inline, got: {output}"
    );
    let calls = finished
        .tool_batches
        .iter()
        .flat_map(|batch| &batch.calls)
        .filter(|call| call.tool_name == MODEL_GENERATE_TOOL_NAME)
        .count();
    assert_eq!(calls, 3, "expected three model_generate calls in one batch");

    let generations = llm.generations.lock().expect("lock").clone();
    assert_eq!(
        generations.len(),
        2,
        "only granted calls reach the provider"
    );
    for generation in &generations {
        assert_eq!(generation.model, *grader, "the grant pins the route");
        assert!(generation.tools.is_empty(), "generations have no tools");
        assert_eq!(generation.output_limit, Some(400));
        assert!(
            generation.params.is_some(),
            "a closed object schema lowers to native structured output"
        );
    }
    Ok(())
}

/// Scripted model for both sides of the call. Session turns are scripted by
/// the user message: `GENERATE` emits three `model_generate` calls (granted
/// and valid, granted but schema-breaking, and ungranted), and any turn that
/// sees tool results ends the run with them. Standalone generations are the
/// requests without tools; they answer from the claim in their input.
struct GenerateScriptedLlm {
    blobs: Arc<dyn BlobStore>,
    generations: Mutex<Vec<LlmRequest>>,
}

impl GenerateScriptedLlm {
    fn new(blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            blobs,
            generations: Mutex::new(Vec::new()),
        }
    }

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

    async fn generate_calls(
        &self,
        request: &LlmGenerationRequest,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
        let Some(tool_id) = test_support::scripted_tool_id(request, MODEL_GENERATE_TOOL_NAME)
        else {
            return Err(CoreAgentIoError::Failed {
                message: "the generate grant must advertise model_generate".to_owned(),
            });
        };
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "verdict": { "type": "string", "enum": ["pass", "fail"] } },
            "required": ["verdict"],
            "additionalProperties": false
        });
        let calls = [
            serde_json::json!({
                "model": "grader",
                "instructions": "Grade the claim.",
                "input": { "claim": "GOOD" },
                "output_schema": schema,
            }),
            serde_json::json!({
                "input": { "claim": "BAD" },
                "output_schema": schema,
            }),
            serde_json::json!({ "model": "large", "input": "anything" }),
        ];
        let mut context_entries = Vec::new();
        let mut tool_calls = Vec::new();
        for (index, arguments) in calls.into_iter().enumerate() {
            let arguments_ref = self
                .blobs
                .put_bytes(serde_json::to_vec(&arguments).map_err(io_error)?)
                .await
                .map_err(io_error)?;
            let call_id = ToolCallId::new(format!("generate_call_{index}"));
            let name = ToolName::new(MODEL_GENERATE_TOOL_NAME);
            context_entries.push(ContextEntryInput {
                kind: ContextEntryKind::ToolCall {
                    call_id: call_id.clone(),
                    name: name.clone(),
                },
                content: engine::ContentRef {
                    content_ref: arguments_ref.clone(),
                    media_type: Some("application/json".to_owned()),
                    provider_kind: Some("generate-script".to_owned()),
                },
                preview: None,
                origin: None,
                provenance_ref: None,
                token_estimate: None,
            });
            tool_calls.push(ObservedToolCall {
                call_id,
                tool_id: Some(tool_id.clone()),
                tool_name: name,
                provider_kind: Some("generate-script".to_owned()),
                arguments_ref,
                native_call_ref: None,
            });
        }
        Ok(Self::result(
            request,
            context_entries,
            LlmFinish::ToolCalls,
            tool_calls,
        ))
    }
}

#[async_trait]
impl CoreAgentLlm for GenerateScriptedLlm {
    async fn generate(
        &self,
        request: LlmGenerationRequest,
    ) -> Result<LlmGenerationResult, CoreAgentIoError> {
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
        if request.request.tools.is_empty() {
            self.generations
                .lock()
                .expect("lock")
                .push(request.request.clone());
            let input = user_messages.last().cloned().unwrap_or_default();
            let verdict = if input.contains("GOOD") {
                "pass"
            } else {
                "maybe"
            };
            return self
                .text_result(&request, format!(r#"{{"verdict":"{verdict}"}}"#))
                .await;
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
        if user_messages.last().is_some_and(|text| text == "GENERATE") {
            return self.generate_calls(&request).await;
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
