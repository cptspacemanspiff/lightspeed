//! Standalone model generation for the `model_generate` workflow tool: one
//! provider call through the universe's LLM runtime, from the model pinned
//! when the call was admitted, with the reply validated against the call's
//! output schema before it resolves the caller's promise.

use engine::{
    CoreAgentIoError, LlmFinish, LlmGenerationRequest, LlmGenerationStatus, PromiseResolution,
    RunId, SessionId, TurnId, storage::BlobStore,
};
use llm_runtime::standalone::{
    StandaloneGeneration, StructuredOutputMode, assistant_output_text, parse_json_reply,
    prepare_standalone_generation,
};
use serde_json::Value;
use temporal_workflow::ModelGenerateActivityRequest;
use temporalio_sdk::activities::ActivityError;
use tools::model_generate::{
    MAX_MODEL_GENERATE_OUTPUT_BYTES, MODEL_GENERATE_EXECUTION_CONTEXT_VERSION, ModelGenerateArgs,
    ModelGenerateError, ModelGenerateErrorKind as Kind, ModelGenerateExecutionContextV1,
    ModelGenerateModelView, ModelGenerateOutputMode, ModelGenerateResultEnvelope,
    ModelGenerateSelection, ModelGenerateStatus, ModelGenerateUsage, compile_schema,
    is_model_generate_binding,
};

use super::{
    common::{activity_error, transient_provider_failure},
    state::LlmActivityDeps,
};

pub(super) async fn execute(
    deps: &LlmActivityDeps,
    attempt: u32,
    request: ModelGenerateActivityRequest,
) -> Result<PromiseResolution, ActivityError> {
    let blobs = deps.blobs.as_ref();
    let invocation = &request.start.invocation;
    if !is_model_generate_binding(invocation.tool_id.as_str(), &invocation.semantic_type) {
        return Err(activity_error(anyhow::anyhow!(
            "model generation started for foreign workflow tool {}",
            invocation.tool_id
        )));
    }
    let context = match read_context(blobs, invocation.execution_context_ref.as_ref()).await {
        Ok(context) => context,
        Err(Failure::Retry(error)) => return Err(activity_error(error)),
        Err(Failure::Rejected(message)) => {
            return Err(activity_error(anyhow::anyhow!(
                "model generation context is unusable: {message}"
            )));
        }
    };
    let selection = context.selection.clone();
    let envelope = match generate(deps, attempt, &context, &invocation.arguments_ref).await {
        Ok(envelope) => envelope,
        Err(Outcome::Retry(error)) => return Err(error),
        Err(Outcome::Failed(error, text, output_mode)) => ModelGenerateResultEnvelope {
            status: ModelGenerateStatus::Failed,
            model: model_view(&selection),
            output_mode,
            text,
            json: None,
            usage: None,
            error: Some(error),
        },
    };
    let bytes = serde_json::to_vec(&envelope).map_err(activity_error)?;
    let payload_ref = blobs.put_bytes(bytes).await.map_err(activity_error)?;
    Ok(match envelope.status {
        ModelGenerateStatus::Completed => PromiseResolution::Resolved {
            payload_ref: Some(payload_ref),
        },
        ModelGenerateStatus::Failed => PromiseResolution::Failed {
            error_ref: Some(payload_ref),
        },
    })
}

enum Failure {
    Retry(anyhow::Error),
    Rejected(String),
}

/// Terminal outcomes carry any reply text and the output mode so the caller
/// can see what failed validation.
enum Outcome {
    Retry(ActivityError),
    Failed(
        ModelGenerateError,
        Option<String>,
        Option<ModelGenerateOutputMode>,
    ),
}

fn failed(kind: Kind, message: impl Into<String>) -> Outcome {
    Outcome::Failed(
        ModelGenerateError {
            kind,
            message: message.into(),
        },
        None,
        None,
    )
}

async fn read_json(blobs: &dyn BlobStore, blob_ref: &engine::BlobRef) -> Result<Value, Failure> {
    let bytes = match blobs.read_bytes(blob_ref).await {
        Ok(bytes) => bytes,
        Err(engine::storage::BlobStoreError::NotFound { .. }) => {
            return Err(Failure::Rejected(format!("{blob_ref} is missing")));
        }
        Err(error) => return Err(Failure::Retry(error.into())),
    };
    serde_json::from_slice(&bytes).map_err(|error| Failure::Rejected(error.to_string()))
}

async fn read_context(
    blobs: &dyn BlobStore,
    context_ref: Option<&engine::BlobRef>,
) -> Result<ModelGenerateExecutionContextV1, Failure> {
    let context_ref =
        context_ref.ok_or_else(|| Failure::Rejected("no execution context".to_owned()))?;
    let context: ModelGenerateExecutionContextV1 =
        serde_json::from_value(read_json(blobs, context_ref).await?)
            .map_err(|error| Failure::Rejected(error.to_string()))?;
    if context.version != MODEL_GENERATE_EXECUTION_CONTEXT_VERSION {
        return Err(Failure::Rejected(format!(
            "unsupported execution context version {}",
            context.version
        )));
    }
    Ok(context)
}

async fn generate(
    deps: &LlmActivityDeps,
    attempt: u32,
    context: &ModelGenerateExecutionContextV1,
    arguments_ref: &engine::BlobRef,
) -> Result<ModelGenerateResultEnvelope, Outcome> {
    let blobs = deps.blobs.as_ref();
    let arguments = read_json(blobs, arguments_ref)
        .await
        .map_err(|error| match error {
            Failure::Retry(error) => Outcome::Retry(activity_error(error)),
            Failure::Rejected(message) => failed(Kind::InvalidRequest, message),
        })?;
    let args: ModelGenerateArgs = serde_json::from_value(arguments)
        .map_err(|error| failed(Kind::InvalidRequest, error.to_string()))?;
    args.validate()
        .map_err(|error| failed(Kind::InvalidRequest, error.to_string()))?;
    let output_validator = args
        .output_schema
        .as_ref()
        .map(|schema| compile_schema("output_schema", schema))
        .transpose()
        .map_err(|error| failed(Kind::InvalidRequest, error.to_string()))?;

    let selection = &context.selection;
    let prepared = prepare_standalone_generation(
        blobs,
        &StandaloneGeneration {
            model: selection.model.clone(),
            instructions: args.instructions.clone(),
            input: args.input_text(),
            output_schema: args.output_schema.clone(),
            reasoning_effort: selection.reasoning_effort.clone(),
            max_output_tokens: selection.max_output_tokens,
        },
    )
    .await
    .map_err(|error| failed(Kind::Provider, error.to_string()))?;
    let output_mode = prepared.structured_output.map(|mode| match mode {
        StructuredOutputMode::Native => ModelGenerateOutputMode::Native,
        StructuredOutputMode::Prompted => ModelGenerateOutputMode::Prompted,
    });

    let result = match deps
        .llm
        .generate(LlmGenerationRequest {
            session_id: SessionId::new(context.session_id.clone()),
            run_id: RunId::new(context.run_id),
            turn_id: TurnId::new(0),
            request: prepared.request,
        })
        .await
    {
        Ok(result) => result,
        // Transient provider errors fail the attempt; Temporal owns the
        // bounded backoff, as for session generations.
        Err(CoreAgentIoError::Retryable {
            message,
            retry_after,
        }) => {
            return Err(Outcome::Retry(transient_provider_failure(
                "model generation",
                attempt,
                message,
                retry_after,
            )));
        }
        Err(CoreAgentIoError::Failed { message }) => {
            return Err(Outcome::Failed(
                ModelGenerateError {
                    kind: Kind::Provider,
                    message,
                },
                None,
                output_mode,
            ));
        }
    };

    let usage = result.facts.usage.as_ref().map(|usage| ModelGenerateUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        reasoning_tokens: usage.reasoning_tokens,
    });
    let text = assistant_output_text(blobs, &result.context_entries)
        .await
        .map_err(|error| failed(Kind::Provider, error.to_string()))?;
    let failure = |kind: Kind, message: String, text: Option<String>| {
        Outcome::Failed(ModelGenerateError { kind, message }, text, output_mode)
    };
    if result.status != LlmGenerationStatus::Succeeded {
        let message = match &result.failure_ref {
            Some(failure_ref) => llm_runtime::blob_io::read_text(blobs, failure_ref)
                .await
                .unwrap_or_else(|_| "the provider call failed".to_owned()),
            None => "the provider call failed".to_owned(),
        };
        let kind = match result.facts.finish {
            LlmFinish::Length => Kind::Truncated,
            LlmFinish::ContentFilter => Kind::Refused,
            _ => Kind::Provider,
        };
        return Err(failure(kind, message, non_empty(text)));
    }
    match result.facts.finish {
        LlmFinish::Length => {
            return Err(failure(
                Kind::Truncated,
                "the reply was cut off at the output-token cap".to_owned(),
                non_empty(text),
            ));
        }
        LlmFinish::ContentFilter => {
            return Err(failure(
                Kind::Refused,
                "the provider declined to answer".to_owned(),
                non_empty(text),
            ));
        }
        _ => {}
    }
    if text.len() > MAX_MODEL_GENERATE_OUTPUT_BYTES {
        return Err(failure(
            Kind::Truncated,
            format!("the reply exceeds {MAX_MODEL_GENERATE_OUTPUT_BYTES} bytes"),
            None,
        ));
    }

    let json = match &output_validator {
        None => None,
        Some(validator) => {
            let Some(value) = parse_json_reply(&text) else {
                return Err(failure(
                    Kind::OutputInvalid,
                    "the reply is not valid JSON".to_owned(),
                    Some(text),
                ));
            };
            let errors = validator
                .iter_errors(&value)
                .take(5)
                .map(|error| match error.instance_path.to_string() {
                    path if path.is_empty() => error.to_string(),
                    path => format!("{path}: {error}"),
                })
                .collect::<Vec<_>>();
            if !errors.is_empty() {
                return Err(failure(
                    Kind::OutputInvalid,
                    format!(
                        "the reply does not match output_schema: {}",
                        errors.join("; ")
                    ),
                    Some(text),
                ));
            }
            Some(value)
        }
    };
    Ok(ModelGenerateResultEnvelope {
        status: ModelGenerateStatus::Completed,
        model: model_view(selection),
        output_mode,
        // A validated JSON value is the result; the raw text would repeat it.
        text: if json.is_some() { None } else { Some(text) },
        json,
        usage,
        error: None,
    })
}

fn non_empty(text: String) -> Option<String> {
    (!text.trim().is_empty()).then_some(text)
}

fn model_view(selection: &ModelGenerateSelection) -> ModelGenerateModelView {
    ModelGenerateModelView {
        name: selection.name.clone(),
        provider_id: selection.model.provider_id.clone(),
        api_kind: api_projection::api_kind_to_str(&selection.model.api_kind).to_owned(),
        model: selection.model.model.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use engine::{
        ContentRef, ContextEntryInput, ContextEntryKind, ContextMessageRole, CoreAgentLlm,
        LlmGenerationFacts, LlmGenerationResult, ModelSelection, ProviderApiKind,
        storage::InMemoryBlobStore,
    };
    use serde_json::json;

    /// Replies with fixed text and records the request it was given.
    struct ScriptedLlm {
        blobs: Arc<InMemoryBlobStore>,
        reply: Result<(String, LlmFinish), CoreAgentIoError>,
        seen: Mutex<Option<engine::LlmRequest>>,
    }

    #[async_trait::async_trait]
    impl CoreAgentLlm for ScriptedLlm {
        async fn generate(
            &self,
            request: LlmGenerationRequest,
        ) -> Result<LlmGenerationResult, CoreAgentIoError> {
            *self.seen.lock().expect("lock") = Some(request.request.clone());
            let (text, finish) = self.reply.clone()?;
            let content_ref = self
                .blobs
                .put_bytes(text.into_bytes())
                .await
                .expect("put reply");
            Ok(LlmGenerationResult {
                run_id: request.run_id,
                turn_id: request.turn_id,
                status: LlmGenerationStatus::Succeeded,
                failure_ref: None,
                context_entries: vec![ContextEntryInput {
                    origin: None,
                    kind: ContextEntryKind::Message {
                        role: ContextMessageRole::Assistant,
                    },
                    content: ContentRef::text(content_ref),
                    preview: None,
                    provenance_ref: None,
                    token_estimate: None,
                }],
                facts: LlmGenerationFacts {
                    duration_ms: None,
                    provider_response_id: None,
                    finish,
                    usage: None,
                    tool_calls: Vec::new(),
                    approval_requests: Vec::new(),
                    context_token_estimate: None,
                },
            })
        }
    }

    fn scripted(
        reply: Result<(String, LlmFinish), CoreAgentIoError>,
    ) -> (LlmActivityDeps, Arc<ScriptedLlm>, Arc<InMemoryBlobStore>) {
        let blobs = Arc::new(InMemoryBlobStore::new());
        let llm = Arc::new(ScriptedLlm {
            blobs: blobs.clone(),
            reply,
            seen: Mutex::new(None),
        });
        (
            LlmActivityDeps {
                llm: llm.clone(),
                blobs: blobs.clone(),
            },
            llm,
            blobs,
        )
    }

    fn context() -> ModelGenerateExecutionContextV1 {
        ModelGenerateExecutionContextV1 {
            version: MODEL_GENERATE_EXECUTION_CONTEXT_VERSION,
            session_id: "caller".to_owned(),
            run_id: 3,
            selection: ModelGenerateSelection {
                name: Some("grader".to_owned()),
                model: ModelSelection {
                    api_kind: ProviderApiKind::OpenAiResponses,
                    provider_id: "openai".to_owned(),
                    model: "gpt-small".to_owned(),
                },
                reasoning_effort: Some("low".to_owned()),
                max_output_tokens: Some(300),
            },
        }
    }

    async fn run(
        deps: &LlmActivityDeps,
        blobs: &InMemoryBlobStore,
        arguments: Value,
    ) -> Result<ModelGenerateResultEnvelope, Outcome> {
        let arguments_ref = blobs
            .put_bytes(serde_json::to_vec(&arguments).expect("encode"))
            .await
            .expect("put arguments");
        generate(deps, 1, &context(), &arguments_ref).await
    }

    fn verdict_schema() -> Value {
        json!({
            "type": "object",
            "properties": { "verdict": { "type": "string", "enum": ["pass", "fail"] } },
            "required": ["verdict"],
            "additionalProperties": false
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn structured_reply_is_validated_and_returned_as_json() {
        let (deps, llm, blobs) =
            scripted(Ok(("{\"verdict\":\"pass\"}".to_owned(), LlmFinish::Stop)));
        let envelope = run(
            &deps,
            &blobs,
            json!({ "input": "water boils at 100C", "output_schema": verdict_schema() }),
        )
        .await
        .unwrap_or_else(|_| panic!("generation should complete"));
        assert_eq!(envelope.status, ModelGenerateStatus::Completed);
        assert_eq!(envelope.json, Some(json!({ "verdict": "pass" })));
        assert_eq!(envelope.text, None);
        assert_eq!(envelope.output_mode, Some(ModelGenerateOutputMode::Native));
        assert_eq!(envelope.model.name.as_deref(), Some("grader"));
        assert_eq!(envelope.model.api_kind, "openai:responses");

        let request = llm.seen.lock().expect("lock").clone().expect("request");
        assert_eq!(request.model.model, "gpt-small");
        assert_eq!(request.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(request.output_limit, Some(300));
        assert!(request.tools.is_empty());
        assert!(request.params.is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reply_that_breaks_the_schema_fails_with_its_text() {
        let (deps, _, blobs) =
            scripted(Ok(("{\"verdict\":\"maybe\"}".to_owned(), LlmFinish::Stop)));
        let Err(Outcome::Failed(error, text, _)) = run(
            &deps,
            &blobs,
            json!({ "input": "x", "output_schema": verdict_schema() }),
        )
        .await
        else {
            panic!("an invalid reply must fail");
        };
        assert_eq!(error.kind, Kind::OutputInvalid);
        assert!(error.message.contains("/verdict"), "{}", error.message);
        assert_eq!(text.as_deref(), Some("{\"verdict\":\"maybe\"}"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn text_reply_without_schema_and_truncation() {
        let (deps, _, blobs) = scripted(Ok(("Hello".to_owned(), LlmFinish::Stop)));
        let envelope = run(&deps, &blobs, json!({ "input": "Say hello" }))
            .await
            .unwrap_or_else(|_| panic!("generation should complete"));
        assert_eq!(envelope.text.as_deref(), Some("Hello"));
        assert_eq!(envelope.output_mode, None);

        let (deps, _, blobs) = scripted(Ok(("Hel".to_owned(), LlmFinish::Length)));
        let Err(Outcome::Failed(error, text, _)) =
            run(&deps, &blobs, json!({ "input": "Say hello" })).await
        else {
            panic!("a truncated reply must fail");
        };
        assert_eq!(error.kind, Kind::Truncated);
        assert_eq!(text.as_deref(), Some("Hel"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_transients_retry_and_terminal_errors_fail() {
        let (deps, _, blobs) = scripted(Err(CoreAgentIoError::Retryable {
            message: "overloaded".to_owned(),
            retry_after: None,
        }));
        assert!(matches!(
            run(&deps, &blobs, json!({ "input": "x" })).await,
            Err(Outcome::Retry(_))
        ));
        let (deps, _, blobs) = scripted(Err(CoreAgentIoError::Failed {
            message: "unknown model".to_owned(),
        }));
        let Err(Outcome::Failed(error, _, _)) = run(&deps, &blobs, json!({ "input": "x" })).await
        else {
            panic!("terminal provider errors must fail");
        };
        assert_eq!(error.kind, Kind::Provider);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_resolves_the_reply_with_the_envelope() {
        let (deps, _, blobs) = scripted(Ok(("Hello".to_owned(), LlmFinish::Stop)));
        let context_ref = blobs
            .put_bytes(serde_json::to_vec(&context()).expect("encode"))
            .await
            .expect("put context");
        let arguments_ref = blobs
            .put_bytes(serde_json::to_vec(&json!({ "input": "Say hello" })).expect("encode"))
            .await
            .expect("put arguments");
        let invocation = engine::WorkflowToolInvocation {
            invocation_id: engine::WorkflowToolInvocationId::new(format!(
                "wti:sha256:{}",
                "a".repeat(64)
            )),
            tool_id: engine::WorkflowToolId::new(
                tools::model_generate::MODEL_GENERATE_WORKFLOW_TOOL_ID,
            ),
            semantic_type: tools::model_generate::MODEL_GENERATE_WORKFLOW_SEMANTIC_TYPE.to_owned(),
            schema_revision: 1,
            binding_fingerprint: "binding".to_owned(),
            session_universe_id: uuid::Uuid::from_u128(1),
            session_id: SessionId::new("caller"),
            run_id: RunId::new(3),
            turn_id: TurnId::new(1),
            tool_batch_id: engine::ToolBatchId::new(1),
            tool_call_id: engine::ToolCallId::new("call_1"),
            arguments_ref,
            execution_context_ref: Some(context_ref),
            completion_promises: None,
        };
        let resolution = execute(
            &deps,
            1,
            ModelGenerateActivityRequest {
                start: temporal_workflow::WorkflowToolStartArgs {
                    universe_id: invocation.session_universe_id,
                    holder_workflow_id: "holder".to_owned(),
                    execution_id: "wtx".to_owned(),
                    invocation,
                },
            },
        )
        .await
        .expect("execute");
        let PromiseResolution::Resolved {
            payload_ref: Some(payload_ref),
        } = resolution
        else {
            panic!("a completed generation resolves the reply");
        };
        let envelope: Value =
            serde_json::from_slice(&blobs.read_bytes(&payload_ref).await.expect("payload"))
                .expect("json");
        assert_eq!(envelope["status"], "completed");
        assert_eq!(envelope["text"], "Hello");
    }
}
