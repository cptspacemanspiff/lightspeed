//! The code runtime as a session's model. A code sub-agent's generations
//! come here instead of a provider: the first starts the program's execution
//! in the code runtime, and every generation hands the previous turn's tool
//! results to the program and waits until it pauses again. A pause becomes a
//! turn of tool calls, preceded by the program's log lines since the last
//! turn; the program's end becomes the final turn with its result.
//!
//! The session's own engine executes every call, so a program reaches the
//! session's tools with their effects, approvals, and workflow tools, and
//! its progress is recorded in the session like any agent's.

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use engine::{
    BlobRef, ContentRef, ContextEntryInput, ContextEntryKind, ContextMessageRole, LlmFinish,
    LlmGenerationFacts, LlmGenerationRequest, LlmGenerationResult, LlmGenerationStatus,
    ObservedToolCall, ToolCallId, ToolKind, ToolName, storage::BlobStore,
};
use llm_runtime::{
    LlmAdapterError, LlmAdapterResult, LlmGenerationAdapter, LlmGenerationExecution,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use temporalio_client::{
    UntypedQuery, UntypedSignal, UntypedWorkflow, WorkflowGetResultOptions, WorkflowQueryOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::data_converters::{PayloadConverter, RawValue};
use tools::code_run::{
    CODE_EXECUTION_WORKFLOW_TYPE, CodeProgram, DEFAULT_CODE_RUNTIME_TASK_QUEUE, builtin_choices,
    is_identifier, is_program_tool,
};

const DRIVER_STATE_QUERY: &str = "driver_state";
const DRIVER_RESULTS_SIGNAL: &str = "driver_results";
const PROVIDER_KIND: &str = "code-runtime";
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Generation adapter for `ProviderApiKind::CodeRuntime`.
#[derive(Clone)]
pub struct CodeRuntimeDriver {
    client: temporalio_client::Client,
    blobs: Arc<dyn BlobStore>,
    universe_id: uuid::Uuid,
    task_queue: String,
}

impl CodeRuntimeDriver {
    pub fn new(
        client: temporalio_client::Client,
        blobs: Arc<dyn BlobStore>,
        universe_id: uuid::Uuid,
    ) -> Self {
        Self {
            client,
            blobs,
            universe_id,
            task_queue: code_runtime_task_queue(),
        }
    }

    async fn drive(&self, request: &LlmGenerationRequest) -> Result<Turn, DriveError> {
        let context = &request.request.context.entries;
        let program = self
            .program(request)
            .await?
            .ok_or_else(|| DriveError::Program("the run has no program to execute".to_owned()))?;
        let prefix = format!("code_r{}", request.run_id.as_u64());
        let catalog = self.catalog(request).await?;
        let execution_id = format!(
            "{}/{}/code/{}",
            self.universe_id,
            request.session_id.as_str(),
            request.run_id.as_u64()
        );
        let handle = self
            .client
            .get_workflow_handle::<UntypedWorkflow>(execution_id.clone());

        let input = json!({
            "source": program.source,
            "input": program.input,
            "driver_call_prefix": prefix,
            "tools": catalog.values().map(|tool| json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
                "target": { "type": "driver" },
            })).collect::<Vec<_>>(),
            "limits": limits_json(&program),
        });
        match self
            .client
            .start_workflow(
                UntypedWorkflow::new(CODE_EXECUTION_WORKFLOW_TYPE),
                RawValue::from_value(&input, &PayloadConverter::default()),
                WorkflowStartOptions::new(self.task_queue.clone(), execution_id.clone()).build(),
            )
            .await
        {
            Ok(_) | Err(temporalio_client::errors::WorkflowStartError::AlreadyStarted { .. }) => {}
            Err(error) => return Err(DriveError::Runtime(format!("start the program: {error}"))),
        }

        // Hand the program the results of every call it made that the
        // session has answered. Repeats are ignored by the runtime.
        let mut reported = BTreeMap::new();
        let mut results = Vec::new();
        for entry in context {
            match &entry.kind {
                ContextEntryKind::ToolCall { call_id, .. }
                    if call_id.as_str().starts_with(&prefix) =>
                {
                    reported.insert(call_id.as_str().to_owned(), ());
                }
                ContextEntryKind::ToolResult { call_id, is_error }
                    if call_id.as_str().starts_with(&prefix) =>
                {
                    let text = self.text(&entry.content.content_ref).await?;
                    results.push(json!({
                        "call_id": call_id.as_str(),
                        "outcome": outcome(&text, *is_error),
                    }));
                }
                _ => {}
            }
        }
        if !results.is_empty() {
            handle
                .signal(
                    UntypedSignal::new(DRIVER_RESULTS_SIGNAL),
                    RawValue::from_value(
                        &json!({ "results": results }),
                        &PayloadConverter::default(),
                    ),
                    WorkflowSignalOptions::default(),
                )
                .await
                .map_err(|error| DriveError::Runtime(format!("deliver results: {error}")))?;
        }

        // Wait for the program to pause on calls this session has not seen,
        // or to finish.
        loop {
            let state: DriverState = match handle
                .query(
                    UntypedQuery::new(DRIVER_STATE_QUERY),
                    RawValue::from_value(&(), &PayloadConverter::default()),
                    WorkflowQueryOptions::default(),
                )
                .await
            {
                Ok(raw) => decode(raw)?,
                // A closed execution may no longer answer queries; its
                // result says how it ended.
                Err(_) => DriverState {
                    status: "finished".to_owned(),
                    step: None,
                },
            };
            if let Some(step) = state.step
                && step
                    .calls
                    .iter()
                    .any(|call| !reported.contains_key(&call.call_id))
            {
                return Ok(Turn::Calls(step));
            }
            if state.status == "finished" {
                let raw = handle
                    .get_result(WorkflowGetResultOptions::default())
                    .await
                    .map_err(|error| DriveError::Runtime(format!("the program ended: {error}")))?;
                return Ok(Turn::Finished(decode(raw)?));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// The program is the run's input: the latest user message.
    async fn program(
        &self,
        request: &LlmGenerationRequest,
    ) -> Result<Option<CodeProgram>, DriveError> {
        let Some(entry) = request.request.context.entries.iter().rev().find(|entry| {
            matches!(
                entry.kind,
                ContextEntryKind::Message {
                    role: ContextMessageRole::User
                }
            )
        }) else {
            return Ok(None);
        };
        let text = self.text(&entry.content.content_ref).await?;
        let mut program = CodeProgram::parse(&text);
        program.limits = program.limits.bounded();
        Ok(Some(program))
    }

    /// The session's tools as a program calls them, by exposed name.
    async fn catalog(
        &self,
        request: &LlmGenerationRequest,
    ) -> Result<BTreeMap<String, CatalogTool>, DriveError> {
        let mut catalog = BTreeMap::new();
        for tool in &request.request.tools {
            match &tool.kind {
                ToolKind::Builtin(spec) => {
                    for choice in builtin_choices(&tool.name, spec) {
                        catalog.insert(
                            choice.name.clone(),
                            CatalogTool {
                                name: choice.name,
                                tool_id: tool.name.clone(),
                                description: choice.description,
                                input_schema: choice.input_schema,
                            },
                        );
                    }
                }
                ToolKind::Function(function) if is_program_tool(tool.name.as_str()) => {
                    let schema = self.text(&function.input_schema_ref).await?;
                    let description = match &function.description_ref {
                        Some(blob_ref) => Some(self.text(blob_ref).await?),
                        None => None,
                    };
                    catalog.insert(
                        tool.name.as_str().to_owned(),
                        CatalogTool {
                            name: tool.name.as_str().to_owned(),
                            tool_id: tool.name.clone(),
                            description,
                            input_schema: serde_json::from_str(&schema)
                                .unwrap_or_else(|_| json!({})),
                        },
                    );
                }
                _ => {}
            }
        }
        catalog.retain(|name, _| is_identifier(name));
        Ok(catalog)
    }

    async fn text(&self, blob_ref: &BlobRef) -> Result<String, DriveError> {
        let bytes = self
            .blobs
            .read_bytes(blob_ref)
            .await
            .map_err(|error| DriveError::Store(error.into()))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn put(&self, bytes: Vec<u8>) -> LlmAdapterResult<BlobRef> {
        Ok(self.blobs.put_bytes(bytes).await?)
    }

    async fn message(&self, text: String) -> LlmAdapterResult<ContextEntryInput> {
        Ok(ContextEntryInput {
            kind: ContextEntryKind::Message {
                role: ContextMessageRole::Assistant,
            },
            content: ContentRef::text(self.put(text.into_bytes()).await?),
            preview: None,
            origin: None,
            provenance_ref: None,
            token_estimate: None,
        })
    }

    async fn result(
        &self,
        request: &LlmGenerationRequest,
        turn: Turn,
        catalog: &BTreeMap<String, CatalogTool>,
    ) -> LlmAdapterResult<LlmGenerationResult> {
        let mut entries = Vec::new();
        let mut tool_calls = Vec::new();
        let (status, finish, failure_ref) = match turn {
            Turn::Calls(step) => {
                if let Some(text) = log_text(&step.logs) {
                    entries.push(self.message(text).await?);
                }
                for call in step.calls {
                    let arguments_ref = self
                        .put(serde_json::to_vec(&call.arguments).unwrap_or_else(|_| b"{}".to_vec()))
                        .await?;
                    let call_id = ToolCallId::new(call.call_id);
                    let name = ToolName::new(call.tool.as_str());
                    entries.push(ContextEntryInput {
                        kind: ContextEntryKind::ToolCall {
                            call_id: call_id.clone(),
                            name: name.clone(),
                        },
                        content: ContentRef {
                            content_ref: arguments_ref.clone(),
                            media_type: Some("application/json".to_owned()),
                            provider_kind: Some(PROVIDER_KIND.to_owned()),
                        },
                        preview: None,
                        origin: None,
                        provenance_ref: None,
                        token_estimate: None,
                    });
                    tool_calls.push(ObservedToolCall {
                        call_id,
                        tool_id: catalog.get(&call.tool).map(|tool| tool.tool_id.clone()),
                        tool_name: name,
                        provider_kind: Some(PROVIDER_KIND.to_owned()),
                        arguments_ref,
                        native_call_ref: None,
                    });
                }
                (LlmGenerationStatus::Succeeded, LlmFinish::ToolCalls, None)
            }
            Turn::Finished(result) => {
                let fresh = result.logs.get(result.logs_reported..).unwrap_or_default();
                if result.status == "completed" {
                    if let Some(text) = log_text(fresh) {
                        entries.push(self.message(text).await?);
                    }
                    let output = serde_json::to_string(&result.output.unwrap_or(Value::Null))
                        .unwrap_or_else(|_| "null".to_owned());
                    entries.push(self.message(output).await?);
                    (LlmGenerationStatus::Succeeded, LlmFinish::Stop, None)
                } else {
                    let error = result.error.unwrap_or(ProgramError {
                        code: "script_error".to_owned(),
                        message: "the program failed".to_owned(),
                        stack: None,
                    });
                    let mut text = format!("{}: {}", error.code, error.message);
                    if let Some(stack) = error.stack.filter(|stack| !stack.is_empty()) {
                        text.push_str(&format!("\n{stack}"));
                    }
                    if let Some(logs) = log_text(fresh) {
                        text.push_str(&format!("\n\n{logs}"));
                    }
                    let failure_ref = self.failure(request, &text).await?;
                    (
                        LlmGenerationStatus::Failed,
                        LlmFinish::Failed,
                        Some(failure_ref),
                    )
                }
            }
        };
        Ok(LlmGenerationResult {
            run_id: request.run_id,
            turn_id: request.turn_id,
            status,
            failure_ref,
            context_entries: entries,
            facts: LlmGenerationFacts {
                duration_ms: None,
                provider_response_id: None,
                finish,
                usage: None,
                tool_calls,
                approval_requests: Vec::new(),
                context_token_estimate: None,
            },
        })
    }

    /// Failure text in the worker's model-failure layout.
    async fn failure(
        &self,
        request: &LlmGenerationRequest,
        message: &str,
    ) -> LlmAdapterResult<BlobRef> {
        self.put(
            format!(
                "core agent LLM generation failed\nrun_id={}\nturn_id={}\nerror={message}\n",
                request.run_id, request.turn_id
            )
            .into_bytes(),
        )
        .await
    }
}

#[async_trait]
impl LlmGenerationAdapter for CodeRuntimeDriver {
    async fn generate(
        &self,
        request: LlmGenerationRequest,
    ) -> LlmAdapterResult<LlmGenerationExecution> {
        let turn = match self.drive(&request).await {
            Ok(turn) => turn,
            Err(DriveError::Store(error)) => {
                return Err(LlmAdapterError::BlobStore {
                    message: error.to_string(),
                });
            }
            // An unusable program or runtime ends the run with the reason.
            Err(DriveError::Program(message) | DriveError::Runtime(message)) => {
                Turn::Finished(ProgramResult {
                    status: "failed".to_owned(),
                    output: None,
                    error: Some(ProgramError {
                        code: "runtime_failed".to_owned(),
                        message,
                        stack: None,
                    }),
                    logs: Vec::new(),
                    logs_reported: 0,
                })
            }
        };
        let catalog = match self.catalog(&request).await {
            Ok(catalog) => catalog,
            Err(error) => {
                return Err(LlmAdapterError::BlobStore {
                    message: error.to_string(),
                });
            }
        };
        let result = self.result(&request, turn, &catalog).await?;
        Ok(LlmGenerationExecution {
            result,
            debug_dumps: None,
        })
    }
}

/// `LIGHTSPEED_CODE_RUNTIME_TASK_QUEUE`, the queue the code runtime's worker
/// polls.
pub fn code_runtime_task_queue() -> String {
    std::env::var("LIGHTSPEED_CODE_RUNTIME_TASK_QUEUE")
        .ok()
        .filter(|queue| !queue.is_empty())
        .unwrap_or_else(|| DEFAULT_CODE_RUNTIME_TASK_QUEUE.to_owned())
}

struct CatalogTool {
    name: String,
    tool_id: ToolName,
    description: Option<String>,
    input_schema: Value,
}

enum Turn {
    Calls(DriverStep),
    Finished(ProgramResult),
}

#[derive(Debug, thiserror::Error)]
enum DriveError {
    #[error("{0}")]
    Program(String),
    #[error("{0}")]
    Runtime(String),
    #[error("blob store: {0}")]
    Store(anyhow::Error),
}

#[derive(Deserialize)]
struct DriverState {
    status: String,
    #[serde(default)]
    step: Option<DriverStep>,
}

#[derive(Deserialize)]
struct DriverStep {
    calls: Vec<DriverCall>,
    #[serde(default)]
    logs: Vec<LogLine>,
}

#[derive(Deserialize)]
struct DriverCall {
    call_id: String,
    tool: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Deserialize)]
struct LogLine {
    level: String,
    text: String,
}

#[derive(Deserialize)]
struct ProgramResult {
    status: String,
    #[serde(default)]
    output: Option<Value>,
    #[serde(default)]
    error: Option<ProgramError>,
    #[serde(default)]
    logs: Vec<LogLine>,
    #[serde(default)]
    logs_reported: usize,
}

#[derive(Deserialize)]
struct ProgramError {
    code: String,
    message: String,
    #[serde(default)]
    stack: Option<String>,
}

fn decode<T: serde::de::DeserializeOwned>(raw: RawValue) -> Result<T, DriveError> {
    let payload = raw
        .payloads
        .first()
        .ok_or_else(|| DriveError::Runtime("the code runtime returned no payload".to_owned()))?;
    serde_json::from_slice(&payload.data)
        .map_err(|error| DriveError::Runtime(format!("unreadable code runtime payload: {error}")))
}

/// A tool result as the program receives it: the session's view of the
/// result, parsed when it is JSON.
fn outcome(text: &str, is_error: bool) -> Value {
    if is_error {
        return json!({ "ok": false, "error": { "code": "tool_error", "message": text } });
    }
    let value =
        serde_json::from_str::<Value>(text).unwrap_or_else(|_| Value::String(text.to_owned()));
    json!({ "ok": true, "value": value })
}

fn limits_json(program: &CodeProgram) -> Value {
    let mut limits = json!({ "deadline_ms": program.limits.deadline_ms });
    if let Some(max_calls) = program.limits.max_calls {
        limits["max_calls"] = json!(max_calls);
    }
    if let Some(max_concurrent_calls) = program.limits.max_concurrent_calls {
        limits["max_concurrent_calls"] = json!(max_concurrent_calls);
    }
    limits
}

/// The program's console output as one block of assistant text.
fn log_text(logs: &[LogLine]) -> Option<String> {
    if logs.is_empty() {
        return None;
    }
    let lines = logs
        .iter()
        .map(|line| match line.level.as_str() {
            "info" => line.text.clone(),
            level => format!("[{level}] {}", line.text),
        })
        .collect::<Vec<_>>()
        .join("\n");
    Some(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_reach_programs_parsed_when_they_are_json() {
        assert_eq!(
            outcome(r#"{"n":1}"#, false),
            json!({ "ok": true, "value": { "n": 1 } })
        );
        assert_eq!(
            outcome("plain text", false),
            json!({ "ok": true, "value": "plain text" })
        );
        assert_eq!(
            outcome("not found", true),
            json!({ "ok": false, "error": { "code": "tool_error", "message": "not found" } })
        );
    }

    #[test]
    fn log_lines_keep_their_level_unless_informational() {
        let lines = [
            LogLine {
                level: "info".to_owned(),
                text: "fetched 3 pages".to_owned(),
            },
            LogLine {
                level: "warn".to_owned(),
                text: "one page was empty".to_owned(),
            },
        ];
        assert_eq!(
            log_text(&lines).as_deref(),
            Some("fetched 3 pages\n[warn] one page was empty")
        );
        assert_eq!(log_text(&[]), None);
    }
}
