//! Activities for the `code_run` workflow tool: building the code runtime's
//! input from a pinned call, executing each tool call a script makes against
//! the tools pinned at admission, and storing the script's result for the
//! caller.

use engine::{
    BlobRef, PromiseResolution, RunId, SessionId, ToolBatchId, ToolCallId, ToolCallStatus,
    ToolExecutionClass, ToolExecutionSpec, ToolInvocationCallRequest, ToolInvocationRequest,
    ToolName, TurnId, storage::BlobStore,
};
use serde_json::{Value, json};
use temporal_workflow::{
    CodeRunExecutionOutcome, CodeRunFinishActivityRequest, CodeRunPrepareActivityRequest,
    CodeRunPrepared, CodeToolCallActivityRequest, ToolInvokeCallActivityRequest,
    ToolInvokeCallActivityResult,
};
use temporalio_sdk::activities::ActivityError;
use tools::code_run::{
    CODE_RUN_EXECUTION_CONTEXT_VERSION, CodeRunArgs, CodeRunError, CodeRunExecutionContextV1,
    CodeRunResultEnvelope, CodeRunStatus, DEFAULT_CODE_RUNTIME_TASK_QUEUE, is_code_run_binding,
};

use super::{common::activity_error, state::ToolActivityDeps};

/// How long the runtime waits for this host to answer one call: the longest
/// a scriptable tool activity can take, retries included, plus a margin.
const HOST_CALL_TIMEOUT_MS: u64 = 11 * 60 * 1_000;

pub(super) async fn prepare(
    deps: &ToolActivityDeps,
    request: CodeRunPrepareActivityRequest,
) -> Result<CodeRunPrepared, ActivityError> {
    let blobs = deps.blobs.as_ref();
    let start = &request.start;
    let invocation = &start.invocation;
    if !is_code_run_binding(invocation.tool_id.as_str(), &invocation.semantic_type) {
        return Err(activity_error(anyhow::anyhow!(
            "code run started for foreign workflow tool {}",
            invocation.tool_id
        )));
    }
    let rejected = |message: String| async move {
        let envelope = failed_envelope("invalid_request", message);
        Ok::<_, ActivityError>(CodeRunPrepared::Rejected {
            resolution: PromiseResolution::Failed {
                error_ref: Some(put_json(blobs, &envelope).await?),
            },
        })
    };
    let context = match read_context(blobs, invocation.execution_context_ref.as_ref()).await {
        Ok(context) => context,
        Err(Failure::Retry(error)) => return Err(activity_error(error)),
        Err(Failure::Rejected(message)) => {
            return rejected(format!("the call's pinned context is unusable: {message}")).await;
        }
    };
    let args: CodeRunArgs = match read_json(blobs, &invocation.arguments_ref).await {
        Ok(value) => match serde_json::from_value(value) {
            Ok(args) => args,
            Err(error) => return rejected(format!("invalid arguments: {error}")).await,
        },
        Err(Failure::Retry(error)) => return Err(activity_error(error)),
        Err(Failure::Rejected(message)) => {
            return rejected(format!("the call's arguments are unusable: {message}")).await;
        }
    };
    if let Err(error) = args.validate() {
        return rejected(error.to_string()).await;
    }

    let tools = context
        .tools
        .iter()
        .map(|tool| {
            let mut entry = json!({
                "name": tool.name,
                "input_schema": tool.input_schema,
                "target": {
                    "type": "host",
                    "workflow_id": start.execution_id,
                    "timeout_ms": HOST_CALL_TIMEOUT_MS,
                },
            });
            if let Some(description) = &tool.description {
                entry["description"] = json!(description);
            }
            entry
        })
        .collect::<Vec<_>>();
    let mut limits = json!({ "deadline_ms": context.limits.deadline_ms });
    if let Some(max_calls) = context.limits.max_calls {
        limits["max_calls"] = json!(max_calls);
    }
    if let Some(max_concurrent_calls) = context.limits.max_concurrent_calls {
        limits["max_concurrent_calls"] = json!(max_concurrent_calls);
    }
    Ok(CodeRunPrepared::Ready {
        execution_input: json!({
            "source": args.source,
            "input": args.input.unwrap_or(Value::Null),
            "tools": tools,
            "limits": limits,
        }),
        task_queue: code_runtime_task_queue(),
        deadline_ms: context.limits.deadline_ms,
    })
}

/// `LIGHTSPEED_CODE_RUNTIME_TASK_QUEUE`, the queue the code runtime's worker
/// polls.
fn code_runtime_task_queue() -> String {
    std::env::var("LIGHTSPEED_CODE_RUNTIME_TASK_QUEUE")
        .ok()
        .filter(|queue| !queue.is_empty())
        .unwrap_or_else(|| DEFAULT_CODE_RUNTIME_TASK_QUEUE.to_owned())
}

/// Execute one script call. Only a tool pinned for this call can run, in the
/// form it was pinned; every tool-level failure is an outcome the script can
/// catch, and only blob-store failures fail the activity.
pub(super) async fn tool_call(
    deps: &ToolActivityDeps,
    request: CodeToolCallActivityRequest,
) -> Result<Value, ActivityError> {
    let blobs = deps.blobs.as_ref();
    let context = match read_context(blobs, Some(&request.execution_context_ref)).await {
        Ok(context) => context,
        Err(Failure::Retry(error)) => return Err(activity_error(error)),
        Err(Failure::Rejected(message)) => {
            return Ok(call_error(
                "target_failed",
                format!("the call's pinned context is unusable: {message}"),
            ));
        }
    };
    let call = request.call;
    let Some(tool) = context.tool(&call.tool) else {
        return Ok(call_error(
            "unknown_tool",
            format!("{} is not available to this program", call.tool),
        ));
    };
    let arguments = serde_json::to_vec(&call.arguments).map_err(activity_error)?;
    let arguments_ref = blobs
        .put_bytes(arguments)
        .await
        .map_err(|error| activity_error(anyhow::Error::from(error)))?;
    let request = ToolInvocationCallRequest {
        session_id: SessionId::new(context.session_id.as_str()),
        run_id: RunId::new(context.run_id),
        turn_id: TurnId::new(context.turn_id),
        batch_id: ToolBatchId::new(context.batch_id),
        workspace_attachments: context.workspace_attachments.clone(),
        vfs_working_directory: context.vfs_working_directory.clone(),
        active_environment_id: context.active_environment_id.clone(),
        environment_policy: context.environment_policy.clone(),
        subagents_policy: None,
        generate_policy: None,
        code_policy: None,
        promise_id_base: 0,
        call: ToolInvocationRequest {
            call_id: script_call_id(&call.call_id),
            tool_id: Some(ToolName::new(tool.name.as_str())),
            tool_name: tool.tool_name.clone(),
            arguments_ref,
            builtin: Some(tool.builtin.clone()),
            workflow_tool: None,
            promise_control: None,
            remote_mcp: None,
        },
        sibling_calls: Vec::new(),
        execution: ToolExecutionSpec::new(ToolExecutionClass::RemoteInteractive, true),
    };
    let result = match super::tools::invoke_call(deps, ToolInvokeCallActivityRequest { request })
        .await?
    {
        ToolInvokeCallActivityResult::Completed { result } => result,
        ToolInvokeCallActivityResult::EnvironmentNotReady { environment_id } => {
            return Ok(call_error(
                "tool_error",
                format!("environment {environment_id} is not ready"),
            ));
        }
        ToolInvokeCallActivityResult::NeedsApproval { .. } => {
            return Ok(call_error(
                "tool_error",
                "this call needs an approval, which programs cannot request",
            ));
        }
    };
    match (result.status, result.output_ref, result.error_ref) {
        (ToolCallStatus::Succeeded, Some(output_ref), _) => {
            match read_json(blobs, &output_ref).await {
                Ok(value) => Ok(json!({ "ok": true, "value": value })),
                Err(Failure::Retry(error)) => Err(activity_error(error)),
                Err(Failure::Rejected(message)) => Ok(call_error("target_failed", message)),
            }
        }
        (_, _, Some(error_ref)) => {
            let message = match blobs.read_bytes(&error_ref).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(engine::storage::BlobStoreError::NotFound { .. }) => {
                    "the tool call failed".to_owned()
                }
                Err(error) => return Err(activity_error(anyhow::Error::from(error))),
            };
            Ok(call_error("tool_error", message))
        }
        _ => Ok(call_error("tool_error", "the tool call returned no result")),
    }
}

/// Store the script's result as the caller's `reply` payload. A completed
/// script resolves the promise; a failed one fails it with the same
/// envelope, so the caller sees the error, logs, and stats either way.
pub(super) async fn finish(
    deps: &ToolActivityDeps,
    request: CodeRunFinishActivityRequest,
) -> Result<PromiseResolution, ActivityError> {
    let blobs = deps.blobs.as_ref();
    let envelope = match request.outcome {
        CodeRunExecutionOutcome::Returned { result } => {
            match serde_json::from_value::<CodeRunResultEnvelope>(result) {
                Ok(envelope) => envelope,
                Err(error) => failed_envelope(
                    "runtime_failed",
                    format!("the code runtime returned an unreadable result: {error}"),
                ),
            }
        }
        CodeRunExecutionOutcome::Lost { message } => failed_envelope("runtime_failed", message),
    };
    let blob_ref = put_json(blobs, &envelope).await?;
    Ok(match envelope.status {
        CodeRunStatus::Completed => PromiseResolution::Resolved {
            payload_ref: Some(blob_ref),
        },
        CodeRunStatus::Failed => PromiseResolution::Failed {
            error_ref: Some(blob_ref),
        },
    })
}

/// A tool call id derived from the runtime's call id, which may contain
/// characters tool call ids do not allow.
fn script_call_id(call_id: &str) -> ToolCallId {
    let digest = BlobRef::from_bytes(call_id.as_bytes());
    let hex = digest.as_str().trim_start_matches("sha256:");
    ToolCallId::new(format!("code-{}", &hex[..32.min(hex.len())]))
}

fn call_error(code: &str, message: impl Into<String>) -> Value {
    json!({ "ok": false, "error": { "code": code, "message": message.into() } })
}

fn failed_envelope(code: &str, message: String) -> CodeRunResultEnvelope {
    CodeRunResultEnvelope {
        status: CodeRunStatus::Failed,
        output: None,
        error: Some(CodeRunError {
            code: code.to_owned(),
            message,
            stack: None,
        }),
        logs: Vec::new(),
        logs_truncated: false,
        stats: Default::default(),
    }
}

async fn put_json(
    blobs: &dyn BlobStore,
    value: &impl serde::Serialize,
) -> Result<BlobRef, ActivityError> {
    let bytes = serde_json::to_vec(value).map_err(activity_error)?;
    blobs
        .put_bytes(bytes)
        .await
        .map_err(|error| activity_error(anyhow::Error::from(error)))
}

enum Failure {
    /// A storage failure worth retrying.
    Retry(anyhow::Error),
    /// Missing or malformed data that no retry can fix.
    Rejected(String),
}

async fn read_json(blobs: &dyn BlobStore, blob_ref: &BlobRef) -> Result<Value, Failure> {
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
    context_ref: Option<&BlobRef>,
) -> Result<CodeRunExecutionContextV1, Failure> {
    let context_ref =
        context_ref.ok_or_else(|| Failure::Rejected("no execution context".to_owned()))?;
    let context: CodeRunExecutionContextV1 =
        serde_json::from_value(read_json(blobs, context_ref).await?)
            .map_err(|error| Failure::Rejected(error.to_string()))?;
    if context.version != CODE_RUN_EXECUTION_CONTEXT_VERSION {
        return Err(Failure::Rejected(format!(
            "unsupported execution context version {}",
            context.version
        )));
    }
    Ok(context)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use engine::{
        CoreAgentIoError, CoreAgentTools, ProviderApiKind, ToolBatchOutcome,
        ToolInvocationBatchRequest, ToolInvocationResult, storage::InMemoryBlobStore,
    };
    use tools::code_run::{CodeRunLimits, ScriptTool};

    use super::*;

    /// Records the per-call request and answers from a script.
    struct RecordingTools {
        blobs: Arc<InMemoryBlobStore>,
        output: Result<Value, String>,
        received: Mutex<Vec<ToolInvocationCallRequest>>,
    }

    #[async_trait]
    impl CoreAgentTools for RecordingTools {
        async fn invoke_batch(
            &self,
            _request: ToolInvocationBatchRequest,
        ) -> Result<ToolBatchOutcome, CoreAgentIoError> {
            unreachable!("script calls run per call")
        }

        async fn invoke_call(
            &self,
            request: ToolInvocationCallRequest,
        ) -> Result<ToolInvocationResult, CoreAgentIoError> {
            let call_id = request.call.call_id.clone();
            self.received.lock().expect("lock").push(request);
            let (status, output_ref, error_ref) = match &self.output {
                Ok(value) => (
                    ToolCallStatus::Succeeded,
                    Some(self.put(serde_json::to_vec(value).expect("json")).await),
                    None,
                ),
                Err(message) => (
                    ToolCallStatus::Failed,
                    None,
                    Some(self.put(message.clone().into_bytes()).await),
                ),
            };
            Ok(ToolInvocationResult {
                call_id,
                status,
                output_ref,
                model_visible_context_entries: Vec::new(),
                error_ref,
                effects: Vec::new(),
                duration_ms: None,
                output_bytes: None,
                truncated: false,
            })
        }
    }

    impl RecordingTools {
        async fn put(&self, bytes: Vec<u8>) -> BlobRef {
            self.blobs.put_bytes(bytes).await.expect("put blob")
        }
    }

    fn deps(output: Result<Value, String>) -> (ToolActivityDeps, Arc<RecordingTools>) {
        let blobs = Arc::new(InMemoryBlobStore::new());
        let tools = Arc::new(RecordingTools {
            blobs: blobs.clone(),
            output,
            received: Mutex::new(Vec::new()),
        });
        (
            ToolActivityDeps {
                tools: tools.clone(),
                blobs,
                blob_graph: None,
                hosted: None,
                native_mcp: None,
            },
            tools,
        )
    }

    fn fetch_tool() -> ScriptTool {
        tools::code_run::script_tool(
            &engine::CodeToolRuntime {
                tool_id: ToolName::new("web.fetch"),
                spec: engine::BuiltinToolSpec::default(),
            },
            &engine::ModelSelection {
                api_kind: ProviderApiKind::AnthropicMessages,
                provider_id: "anthropic".to_owned(),
                model: "claude".to_owned(),
            },
        )
        .expect("web.fetch is scriptable")
    }

    async fn context_ref(deps: &ToolActivityDeps) -> BlobRef {
        let context = CodeRunExecutionContextV1 {
            version: CODE_RUN_EXECUTION_CONTEXT_VERSION,
            session_id: "caller".to_owned(),
            run_id: 3,
            turn_id: 2,
            batch_id: 1,
            workspace_attachments: Vec::new(),
            vfs_working_directory: None,
            active_environment_id: None,
            environment_policy: None,
            tools: vec![fetch_tool()],
            limits: CodeRunLimits {
                max_calls: Some(9),
                max_concurrent_calls: None,
                deadline_ms: 60_000,
            },
        };
        put_json(deps.blobs.as_ref(), &context).await.expect("put context")
    }

    fn call(tool: &str) -> temporal_workflow::CodeHostCallRequest {
        temporal_workflow::CodeHostCallRequest {
            execution_id: "wtx/runtime".to_owned(),
            call_id: "wtx/runtime/web.fetch:abc:0".to_owned(),
            tool: tool.to_owned(),
            arguments: json!({ "url": "https://example.com" }),
            context: Value::Null,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_builds_the_runtime_input_with_this_workflow_as_host() {
        let (deps, _) = deps(Ok(Value::Null));
        let context_ref = context_ref(&deps).await;
        let arguments_ref = put_json(
            deps.blobs.as_ref(),
            &json!({ "source": "return input;", "input": { "n": 1 } }),
        )
        .await
        .expect("put arguments");
        let prepared = prepare(
            &deps,
            CodeRunPrepareActivityRequest {
                start: temporal_workflow::WorkflowToolStartArgs {
                    universe_id: uuid::Uuid::from_u128(1),
                    holder_workflow_id: "holder".to_owned(),
                    execution_id: "wtx".to_owned(),
                    invocation: engine::WorkflowToolInvocation {
                        invocation_id: engine::WorkflowToolInvocationId::new(format!(
                            "wti:sha256:{}",
                            "a".repeat(64)
                        )),
                        tool_id: engine::WorkflowToolId::new(
                            tools::code_run::CODE_RUN_WORKFLOW_TOOL_ID,
                        ),
                        semantic_type: tools::code_run::CODE_RUN_WORKFLOW_SEMANTIC_TYPE
                            .to_owned(),
                        schema_revision: 1,
                        binding_fingerprint: "binding".to_owned(),
                        session_universe_id: uuid::Uuid::from_u128(1),
                        session_id: SessionId::new("caller"),
                        run_id: RunId::new(3),
                        turn_id: TurnId::new(2),
                        tool_batch_id: ToolBatchId::new(1),
                        tool_call_id: ToolCallId::new("call_1"),
                        arguments_ref,
                        execution_context_ref: Some(context_ref),
                        completion_promises: None,
                    },
                },
            },
        )
        .await
        .expect("prepare");
        let CodeRunPrepared::Ready {
            execution_input,
            deadline_ms,
            ..
        } = prepared
        else {
            panic!("a valid call is ready: {prepared:?}");
        };
        assert_eq!(deadline_ms, 60_000);
        assert_eq!(execution_input["source"], "return input;");
        assert_eq!(execution_input["input"], json!({ "n": 1 }));
        assert_eq!(
            execution_input["limits"],
            json!({ "deadline_ms": 60_000, "max_calls": 9 })
        );
        let tool = &execution_input["tools"][0];
        assert_eq!(tool["name"], "web.fetch");
        assert_eq!(tool["target"]["type"], "host");
        assert_eq!(tool["target"]["workflow_id"], "wtx");
        assert!(tool["input_schema"]["properties"]["url"].is_object());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tool_calls_run_the_pinned_tool_for_the_calling_session() {
        let (deps, recorded) = deps(Ok(json!({ "status": 200, "text": "hello" })));
        let context_ref = context_ref(&deps).await;
        let outcome = tool_call(
            &deps,
            CodeToolCallActivityRequest {
                universe_id: uuid::Uuid::from_u128(1),
                execution_context_ref: context_ref,
                call: call("web.fetch"),
            },
        )
        .await
        .expect("tool call");
        assert_eq!(
            outcome,
            json!({ "ok": true, "value": { "status": 200, "text": "hello" } })
        );
        let received = recorded.received.lock().expect("lock");
        let request = &received[0];
        assert_eq!(request.session_id.as_str(), "caller");
        assert_eq!(request.call.tool_id.as_ref().map(ToolName::as_str), Some("web.fetch"));
        assert_eq!(request.call.tool_name.as_str(), "web_fetch");
        assert!(request.call.call_id.as_str().starts_with("code-"));
        assert_eq!(
            request.call.builtin.as_ref().map(|builtin| &builtin.model.api_kind),
            Some(&ProviderApiKind::OpenAiCompletions)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unpinned_tools_and_tool_failures_are_outcomes() {
        let (deps, recorded) = deps(Err("fetch failed: 404".to_owned()));
        let context_ref = context_ref(&deps).await;
        let unknown = tool_call(
            &deps,
            CodeToolCallActivityRequest {
                universe_id: uuid::Uuid::from_u128(1),
                execution_context_ref: context_ref.clone(),
                call: call("vfs.write_file"),
            },
        )
        .await
        .expect("tool call");
        assert_eq!(unknown["error"]["code"], "unknown_tool");
        assert!(recorded.received.lock().expect("lock").is_empty());

        let failed = tool_call(
            &deps,
            CodeToolCallActivityRequest {
                universe_id: uuid::Uuid::from_u128(1),
                execution_context_ref: context_ref,
                call: call("web.fetch"),
            },
        )
        .await
        .expect("tool call");
        assert_eq!(
            failed,
            json!({ "ok": false, "error": { "code": "tool_error", "message": "fetch failed: 404" } })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finish_resolves_completed_runs_and_fails_the_rest() {
        let (deps, _) = deps(Ok(Value::Null));
        let completed = finish(
            &deps,
            CodeRunFinishActivityRequest {
                universe_id: uuid::Uuid::from_u128(1),
                outcome: CodeRunExecutionOutcome::Returned {
                    result: json!({
                        "status": "completed",
                        "output": { "sum": 5 },
                        "logs": [{ "level": "info", "text": "hi" }],
                        "logs_truncated": false,
                        "stats": { "generations": 1, "calls": 2, "rejected_calls": 0 }
                    }),
                },
            },
        )
        .await
        .expect("finish");
        let PromiseResolution::Resolved {
            payload_ref: Some(payload_ref),
        } = completed
        else {
            panic!("a completed run resolves: {completed:?}");
        };
        let envelope: CodeRunResultEnvelope = serde_json::from_value(
            read_json(deps.blobs.as_ref(), &payload_ref)
                .await
                .unwrap_or_else(|_| panic!("read envelope")),
        )
        .expect("envelope");
        assert_eq!(envelope.output, Some(json!({ "sum": 5 })));
        assert_eq!(envelope.stats.calls, 2);

        for outcome in [
            CodeRunExecutionOutcome::Returned {
                result: json!({
                    "status": "failed",
                    "error": { "code": "script_error", "message": "Error: boom" },
                    "logs": [],
                    "logs_truncated": false,
                    "stats": { "generations": 1, "calls": 0, "rejected_calls": 0 }
                }),
            },
            CodeRunExecutionOutcome::Lost {
                message: "the code runtime did not start".to_owned(),
            },
        ] {
            let resolution = finish(
                &deps,
                CodeRunFinishActivityRequest {
                    universe_id: uuid::Uuid::from_u128(1),
                    outcome,
                },
            )
            .await
            .expect("finish");
            assert!(
                matches!(resolution, PromiseResolution::Failed { error_ref: Some(_) }),
                "{resolution:?}"
            );
        }
    }
}
