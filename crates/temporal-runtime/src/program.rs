//! The session side of a program run that its workflow drives: loading the
//! pinned program, admitting the requests each step issues to the run's
//! scope, collecting recorded outcomes in log order, and concluding the run
//! by keeping its output and context and closing the scope.
//!
//! The program itself runs in `ProgramRunWorkflow` on a
//! `program_engine::ProgramInstance`; nothing here holds an interpreter.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use harness::{BlobRef, storage::BlobStore};
use program_engine::{HostCompletion, HostError, HostRequest};
use serde_json::Value;
use temporal_workflow::{
    CodeToolCallOutcome as RecordedOutcome, CodeToolClient, CodeToolRejectionKind,
    CodeToolScopeReport, CodeToolScopeReportRequest, InvokeCodeToolRequest,
    MAX_RECORDED_OUTCOMES_PER_PAGE, ProgramRunDescriptor, ScopeRecordedOutcomesRequest,
};
use temporalio_client::Client;
use tokio::sync::Semaphore;
use tools::program::contracts::ContractError;

use crate::code::{CodeRunError, CodeRunner, read_bounded};

/// The contract bindings a program runs with, as stored by reference on
/// its descriptor.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramCatalog {
    pub version: u32,
    pub bindings: Vec<program_engine::ToolBinding>,
}

/// One finished attempt: what the program did and the run's scope as it
/// closed.
#[derive(Debug)]
pub struct CodeRunReport {
    pub execution: program_engine::ExecutionReport,
    /// Latest confirmed snapshot. It may be incomplete when cleanup failed.
    pub scope: Option<CodeToolScopeReport>,
    /// A missing final receipt means unfinished effect outcomes are unknown.
    pub cleanup_error: Option<String>,
}

fn host_error(kind: &str, message: impl Into<String>) -> HostError {
    HostError {
        kind: kind.into(),
        message: message.into(),
        value: None,
    }
}

/// A recorded outcome as the value or failure the program receives.
async fn materialize(
    blobs: &dyn BlobStore,
    outcome: &RecordedOutcome,
    limit: u64,
) -> Result<Value, HostError> {
    crate::code::materialize(blobs, outcome, limit)
        .await
        .map_err(|error| HostError {
            kind: error.kind,
            message: error.message,
            value: error.value,
        })
}

/// Bytes of a program context's JSON shown as its preview.
const PROGRAM_CONTEXT_PREVIEW_CHARS: usize = 256;

#[derive(Clone)]
pub struct ProgramRunner {
    client: Client,
    blobs: Arc<dyn BlobStore>,
    capacity: Arc<Semaphore>,
    cleanup_timeout: Duration,
}

impl ProgramRunner {
    pub fn with_shared_capacity(
        client: Client,
        blobs: Arc<dyn BlobStore>,
        capacity: Arc<Semaphore>,
        cleanup_timeout: Duration,
    ) -> Self {
        Self {
            client,
            blobs,
            capacity,
            cleanup_timeout,
        }
    }

    fn code_runner(&self) -> CodeRunner {
        CodeRunner::with_shared_capacity(
            self.client.clone(),
            self.blobs.clone(),
            self.capacity.clone(),
            self.cleanup_timeout,
        )
    }

    /// Close the run's scope and collect terminal outcomes.
    pub async fn cleanup(
        &self,
        code_tools: &CodeToolClient,
        execution_id: &str,
        latest: Option<CodeToolScopeReport>,
    ) -> (Option<CodeToolScopeReport>, Option<String>) {
        self.code_runner()
            .cleanup(code_tools, execution_id, latest)
            .await
    }

    /// What a workflow needs to run the program itself, with the pins
    /// re-checked against the run's open scope.
    pub async fn load(
        &self,
        descriptor: &ProgramRunDescriptor,
    ) -> Result<LoadedProgram, CodeRunError> {
        descriptor
            .validate()
            .map_err(|error| CodeRunError::Preparation(error.to_string()))?;
        let code_tools =
            CodeToolClient::new(self.client.clone(), descriptor.session_workflow_id.clone());
        let scope = code_tools
            .report(
                CodeToolScopeReportRequest {
                    execution_id: descriptor.execution_id.clone(),
                },
                Default::default(),
            )
            .await
            .map_err(|error| CodeRunError::Preparation(error.to_string()))?
            .map_err(|error| CodeRunError::Preparation(error.to_string()))?;
        if scope.execution_id != descriptor.execution_id || scope.closed {
            return Err(CodeRunError::Preparation(
                "program requires the run's open scope".into(),
            ));
        }
        let read = |reference: &BlobRef, limit: u64| {
            let blobs = self.blobs.clone();
            let reference = reference.clone();
            async move {
                read_bounded(blobs.as_ref(), &reference, limit)
                    .await
                    .map_err(|error| CodeRunError::Preparation(error.message))
            }
        };
        let source = String::from_utf8(
            read(&descriptor.source_ref, descriptor.limits.max_source_bytes).await?,
        )
        .map_err(|error| CodeRunError::Preparation(error.to_string()))?;
        let input: Value = serde_json::from_slice(
            &read(&descriptor.input_ref, descriptor.limits.max_request_bytes).await?,
        )
        .map_err(|error| CodeRunError::Preparation(format!("input is not JSON: {error}")))?;
        let context = match &descriptor.context_ref {
            Some(context_ref) => Some(
                serde_json::from_slice::<Value>(
                    &read(
                        context_ref,
                        program_engine::MAX_PROGRAM_CONTEXT_BYTES as u64,
                    )
                    .await?,
                )
                .map_err(|error| {
                    CodeRunError::Preparation(format!("program context is not JSON: {error}"))
                })?,
            ),
            None => None,
        };
        let catalog: ProgramCatalog = serde_json::from_slice(
            &read(&descriptor.catalog_ref, descriptor.limits.max_catalog_bytes).await?,
        )
        .map_err(|error| CodeRunError::Catalog(error.to_string()))?;
        if program_catalog(&scope, &descriptor.requires)?.bindings != catalog.bindings {
            return Err(CodeRunError::Catalog(
                "program catalog does not match the scope's contract bindings".into(),
            ));
        }
        Ok(LoadedProgram {
            source,
            input,
            context,
            bindings: catalog.bindings,
        })
    }

    /// Admit one step's requests to the run's scope, in issue order, and
    /// return the failures the program receives for those not admitted. A
    /// transport failure is returned as an error so the step is retried;
    /// admission is idempotent per request id.
    pub async fn admit_step(
        &self,
        descriptor: &ProgramRunDescriptor,
        requests: Vec<HostRequest>,
    ) -> Result<Vec<HostCompletion>, AdmitStepError> {
        let code_tools =
            CodeToolClient::new(self.client.clone(), descriptor.session_workflow_id.clone());
        let mut rejected = Vec::new();
        for request in requests {
            match self.admit(&code_tools, descriptor, request).await {
                Ok(_) => {}
                Err(Admission::Conflict(request_id)) => {
                    return Err(AdmitStepError::Diverged(request_id));
                }
                Err(Admission::Rejected(completion)) => {
                    if matches!(&completion.outcome, Err(error) if error.kind == "transport") {
                        return Err(AdmitStepError::Transport(completion.request_id));
                    }
                    rejected.push(completion);
                }
            }
        }
        Ok(rejected)
    }

    /// The scope's recorded outcomes after the first `after`, as the
    /// completions a program receives, in log order.
    pub async fn collect(
        &self,
        session_workflow_id: &str,
        execution_id: &str,
        after: u64,
        max_result_bytes: u64,
    ) -> Result<Vec<HostCompletion>, String> {
        let code_tools = CodeToolClient::new(self.client.clone(), session_workflow_id.to_owned());
        let page = code_tools
            .recorded_outcomes(
                ScopeRecordedOutcomesRequest {
                    execution_id: execution_id.to_owned(),
                    after,
                    limit: MAX_RECORDED_OUTCOMES_PER_PAGE,
                },
                Default::default(),
            )
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let mut completions = Vec::with_capacity(page.outcomes.len());
        for outcome in &page.outcomes {
            completions.push(HostCompletion {
                request_id: outcome.request_id.clone(),
                outcome: materialize(self.blobs.as_ref(), outcome, max_result_bytes)
                    .await
                    .map_err(structured_failure),
                completed_at_ms: outcome.completed_at_ms,
            });
        }
        Ok(completions)
    }

    /// Keep a finished program's context with the scope's close, and close
    /// the run's scope.
    pub async fn conclude(
        &self,
        descriptor: &ProgramRunDescriptor,
        report: program_engine::ExecutionReport,
    ) -> CodeRunReport {
        let code_tools =
            CodeToolClient::new(self.client.clone(), descriptor.session_workflow_id.clone());
        let mut cleanup_error = None;
        let program_context = match (&report.error, &report.program_context) {
            (None, Some(value)) => match self.keep_context(value).await {
                Ok(context) => Some(context),
                Err(error) => {
                    cleanup_error = Some(format!("program context not stored: {error}"));
                    None
                }
            },
            _ => None,
        };
        let (scope, close_error) = self
            .code_runner()
            .close_scope(&code_tools, &descriptor.execution_id, None, program_context)
            .await;
        CodeRunReport {
            execution: report,
            scope,
            cleanup_error: cleanup_error.or(close_error),
        }
    }

    async fn keep_context(&self, value: &Value) -> Result<harness::ProgramContextInput, String> {
        let json = serde_json::to_string(value).map_err(|error| error.to_string())?;
        let mut end = json.len().min(PROGRAM_CONTEXT_PREVIEW_CHARS);
        while !json.is_char_boundary(end) {
            end -= 1;
        }
        let preview = json[..end].to_owned();
        let content_ref = self
            .blobs
            .put_bytes(json.into_bytes())
            .await
            .map_err(|error| error.to_string())?;
        Ok(harness::ProgramContextInput {
            content_ref,
            preview: Some(preview),
        })
    }

    /// Admit one call. The reply only acknowledges admission; the outcome
    /// is read from the recorded feed later.
    async fn admit(
        &self,
        code_tools: &CodeToolClient,
        descriptor: &ProgramRunDescriptor,
        request: HostRequest,
    ) -> Result<String, Admission> {
        let request_id = request.request_id.clone();
        let admitted: Result<(), HostError> = async {
            let bytes = serde_json::to_vec(&request.arguments)
                .map_err(|error| host_error("invalid_arguments", error.to_string()))?;
            if bytes.len() as u64 > descriptor.limits.max_request_bytes {
                return Err(host_error(
                    "payload_too_large",
                    "arguments exceed request budget",
                ));
            }
            let arguments_ref = self
                .blobs
                .put_bytes(bytes)
                .await
                .map_err(|error| host_error("storage", error.to_string()))?;
            code_tools
                .invoke(
                    InvokeCodeToolRequest {
                        execution_id: descriptor.execution_id.clone(),
                        request_id: request.request_id.clone(),
                        binding_id: request.binding_id,
                        arguments_ref,
                    },
                    Default::default(),
                )
                .await
                .map_err(|error| host_error("transport", format!("tool outcome unknown: {error}")))?
                .map_err(|error| {
                    if error.kind == CodeToolRejectionKind::Conflict {
                        host_error(CONFLICT, error.to_string())
                    } else {
                        host_error("admission_rejected", error.to_string())
                    }
                })?;
            Ok(())
        }
        .await;
        match admitted {
            Ok(()) => Ok(request_id),
            Err(error) if error.kind == CONFLICT => Err(Admission::Conflict(request_id)),
            Err(error) => Err(Admission::Rejected(HostCompletion {
                request_id,
                outcome: Err(error),
                completed_at_ms: None,
            })),
        }
    }
}

/// A program as a workflow runs it.
#[derive(Debug)]
pub struct LoadedProgram {
    pub source: String,
    pub input: Value,
    pub context: Option<Value>,
    pub bindings: Vec<program_engine::ToolBinding>,
}

/// Why one step's requests could not all be admitted or refused.
#[derive(Debug, thiserror::Error)]
pub enum AdmitStepError {
    #[error("request {0} was admitted earlier with another binding or arguments")]
    Diverged(String),
    #[error("admission of {0} is unknown after a transport failure")]
    Transport(String),
}

/// Why a request was not admitted.
enum Admission {
    /// The program receives this failure in place of an outcome.
    Rejected(HostCompletion),
    /// The session holds the request with another binding or arguments.
    Conflict(String),
}

/// Marks a conflicting admission inside `admit`; never reaches a program.
const CONFLICT: &str = "conflict";

/// A tool failure whose error is a `{ code, message }` document (an
/// input request's `deadline_exceeded`, say) reaches the program with
/// `code` as the error's kind, so it can be caught by kind.
fn structured_failure(error: HostError) -> HostError {
    if error.kind != "tool_failed" {
        return error;
    }
    let Ok(document) = serde_json::from_str::<Value>(&error.message) else {
        return error;
    };
    match (
        document.get("code").and_then(Value::as_str),
        document.get("message").and_then(Value::as_str),
    ) {
        (Some(code), Some(message)) if is_failure_code(code) => HostError {
            kind: code.to_owned(),
            message: message.to_owned(),
            value: error.value,
        },
        _ => error,
    }
}

fn is_failure_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// A program's catalog: the scope's bindings for the contracts the program
/// was pinned to, named by contract. Fails when the scope no longer binds a
/// pinned contract at its digest.
pub(crate) fn program_catalog(
    scope: &CodeToolScopeReport,
    requires: &[harness::ContractPin],
) -> Result<ProgramCatalog, ContractError> {
    let current = scope
        .contracts
        .values()
        .map(|binding| (binding.contract.clone(), binding.digest.clone()))
        .collect::<BTreeMap<_, _>>();
    tools::program::contracts::check_pins(requires, &current)?;
    let required = requires
        .iter()
        .map(|pin| pin.contract.as_str())
        .collect::<BTreeSet<_>>();
    Ok(ProgramCatalog {
        version: 1,
        bindings: scope
            .contracts
            .iter()
            .filter(|(_, binding)| required.contains(binding.contract.as_str()))
            .map(|(handle, binding)| program_engine::ToolBinding {
                name: binding.contract.clone(),
                binding_id: handle.clone(),
            })
            .collect(),
    })
}

/// Store a JSON value and return its reference.
pub(crate) async fn put_json(blobs: &dyn BlobStore, value: &Value) -> Result<BlobRef, String> {
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    blobs
        .put_bytes(bytes)
        .await
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(contract: &str, digest: &str) -> harness::ContractPin {
        harness::ContractPin {
            contract: contract.into(),
            digest: digest.into(),
        }
    }

    fn binding(contract: &str, digest: &str) -> harness::ContractBinding {
        harness::ContractBinding {
            contract: contract.into(),
            digest: digest.into(),
        }
    }

    #[test]
    fn structured_tool_failures_surface_their_code_as_the_kind() {
        let failure = |message: &str| HostError {
            kind: "tool_failed".into(),
            message: message.into(),
            value: None,
        };
        let deadline = structured_failure(failure(
            r#"{"code":"deadline_exceeded","message":"not answered in time"}"#,
        ));
        assert_eq!(deadline.kind, "deadline_exceeded");
        assert_eq!(deadline.message, "not answered in time");
        for unchanged in [
            "plain text",
            r#"{"code":"Bad Code","message":"x"}"#,
            r#"{"code":"x"}"#,
        ] {
            assert_eq!(structured_failure(failure(unchanged)).kind, "tool_failed");
        }
    }

    #[test]
    fn program_catalogs_hold_only_pinned_contracts_and_fail_on_drift() {
        let read = harness::contract_digest(b"read");
        let sleep = harness::contract_digest(b"sleep");
        let fetch = harness::contract_digest(b"fetch");
        let scope = CodeToolScopeReport {
            execution_id: "wtx:test".into(),
            closed: false,
            cancel_requested: false,
            bindings: [
                ("h-read", "vfs_read_file"),
                ("h-sleep", "time_sleep"),
                ("h-fetch", "web_fetch"),
            ]
            .into_iter()
            .map(|(handle, name)| (handle.into(), harness::ToolName::new(name)))
            .collect(),
            contracts: [
                ("h-read", binding("vfs.read_file", &read)),
                ("h-sleep", binding("time.sleep", &sleep)),
                ("h-fetch", binding("web.fetch", &fetch)),
            ]
            .into_iter()
            .map(|(handle, binding)| (handle.into(), binding))
            .collect(),
            calls: Default::default(),
        };
        let requires = [pin("vfs.read_file", &read), pin("time.sleep", &sleep)];
        let catalog = program_catalog(&scope, &requires).unwrap();
        assert_eq!(
            catalog
                .bindings
                .iter()
                .map(|binding| (binding.name.as_str(), binding.binding_id.as_str()))
                .collect::<Vec<_>>(),
            [("vfs.read_file", "h-read"), ("time.sleep", "h-sleep")]
        );
        assert!(matches!(
            program_catalog(&scope, &[pin("vfs.read_file", &fetch)]),
            Err(ContractError::Changed { .. })
        ));
        assert!(matches!(
            program_catalog(&scope, &[pin("env.run_process", &read)]),
            Err(ContractError::Unavailable { .. })
        ));
    }
}
