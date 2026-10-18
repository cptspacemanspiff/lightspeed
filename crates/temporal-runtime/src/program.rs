//! The session side of a program run that its workflow drives: loading the
//! pinned program, admitting the requests each step issues to the run's
//! scope, collecting recorded outcomes in log order, closing the scope as
//! the run exits (keeping its context), and concluding the run by keeping
//! its output and report.
//!
//! The program itself runs in `ProgramRunWorkflow` on a
//! `program_engine::ProgramInstance`; nothing here holds an interpreter.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use harness::{BlobRef, storage::BlobStore};
use program_engine::{HostCancel, HostCompletion, HostError, HostRequest};
use serde_json::Value;
use temporal_workflow::{
    CancelCodeToolRequest, CloseCodeToolScopeRequest, CodeToolCallOutcome as RecordedOutcome,
    CodeToolClient, CodeToolRejectionKind, CodeToolScopeReport, CodeToolScopeReportRequest,
    InvokeCodeToolRequest, MAX_RECORDED_OUTCOMES_PER_PAGE, ProgramCloseActivityResult,
    ProgramRunDescriptor, ScopeRecordedOutcomesRequest,
};
use temporalio_client::Client;
use tools::program::contracts::ContractError;

use crate::code::{CodeRunError, read_bounded};

/// The contract bindings a program runs with, as stored by reference on
/// its descriptor.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramCatalog {
    pub version: u32,
    pub bindings: Vec<program_engine::ToolBinding>,
}

/// One finished program: what it did and the run's scope once everything
/// it started had settled.
#[derive(Debug)]
pub struct CodeRunReport {
    pub execution: program_engine::ExecutionReport,
    /// The settled scope. Missing, or unsettled, only when cleanup failed.
    pub scope: Option<CodeToolScopeReport>,
    /// Why the scope did not close, or could not be read, if it did not.
    pub cleanup_error: Option<String>,
}

fn host_error(kind: &str, message: impl Into<String>) -> HostError {
    HostError {
        kind: kind.into(),
        message: message.into(),
        value: None,
    }
}

/// A recorded outcome as the completion the program receives, and which of
/// its parts are a blob's content (so they can travel by reference).
pub struct CollectedCompletion {
    pub completion: HostCompletion,
    /// The blob holding the completion's value (`Ok`, or the error's `value`).
    pub value_ref: Option<BlobRef>,
    /// The blob holding the error's message.
    pub message_ref: Option<BlobRef>,
}

async fn materialize(
    blobs: &dyn BlobStore,
    outcome: &RecordedOutcome,
    limit: u64,
) -> CollectedCompletion {
    let parts = crate::code::materialize_parts(blobs, outcome, limit).await;
    CollectedCompletion {
        completion: HostCompletion {
            request_id: outcome.request_id.clone(),
            outcome: parts.result.map_err(|error| HostError {
                kind: error.kind,
                message: error.message,
                value: error.value,
            }),
            completed_at_ms: outcome.completed_at_ms,
        },
        value_ref: parts.value_ref,
        message_ref: parts.message_ref,
    }
}

/// Bytes of a program context's JSON shown as its preview.
const PROGRAM_CONTEXT_PREVIEW_CHARS: usize = 256;

#[derive(Clone)]
pub struct ProgramRunner {
    client: Client,
    blobs: Arc<dyn BlobStore>,
}

impl ProgramRunner {
    pub fn new(client: Client, blobs: Arc<dyn BlobStore>) -> Self {
        Self { client, blobs }
    }

    /// Close an execution's scope as it exits: the session cancels every
    /// call the program still owns and withdraws its undetached handles. A
    /// completed program's state is kept with the close as the session's
    /// program context. Returns at once with what the exit must wait for;
    /// idempotent, so a lost result closes again and keeps one context.
    /// `acknowledged` is the program's acknowledgement, as on `admit_step`.
    pub async fn close(
        &self,
        session_workflow_id: &str,
        scope_id: &str,
        program_context: Option<Value>,
        acknowledged: u64,
    ) -> Result<ProgramCloseActivityResult, String> {
        let code_tools = CodeToolClient::new(self.client.clone(), session_workflow_id.to_owned());
        let mut context_error = None;
        let program_context = match program_context {
            Some(value) => match self.keep_context(&value).await {
                Ok(context) => Some(context),
                Err(error) => {
                    context_error = Some(format!("program context not stored: {error}"));
                    None
                }
            },
            None => None,
        };
        let closed = code_tools
            .close_scope(
                CloseCodeToolScopeRequest {
                    execution_id: scope_id.to_owned(),
                    cancel_pending: true,
                    program_context,
                    acknowledged: (acknowledged > 0).then_some(acknowledged),
                },
                Default::default(),
            )
            .await
            .map_err(|error| error.to_string())?;
        let scope = match closed {
            Ok(scope) => scope,
            Err(rejection) => {
                // A run interrupted before it opened its scope has none.
                return match Self::report(&code_tools, scope_id).await {
                    Err(error) if error.kind == CodeToolRejectionKind::UnknownScope => {
                        Ok(ProgramCloseActivityResult::NoScope)
                    }
                    _ => Err(rejection.to_string()),
                };
            }
        };
        // A session worker from before the totals sends none: count the
        // calls it lists.
        Ok(ProgramCloseActivityResult::Closed {
            calls: scope.admitted.max(scope.calls.len() as u64),
            recorded: scope.recorded.max(
                scope
                    .calls
                    .values()
                    .filter(|call| call.status.is_terminal())
                    .count() as u64,
            ),
            withdrawals: scope.withdrawals,
            withdrawals_settled: scope.withdrawals_settled,
            context_error,
        })
    }

    /// The scope's report now, fresh from the session.
    pub async fn report(
        code_tools: &CodeToolClient,
        execution_id: &str,
    ) -> Result<CodeToolScopeReport, temporal_workflow::CodeToolRejection> {
        code_tools
            .report(
                CodeToolScopeReportRequest {
                    execution_id: execution_id.to_owned(),
                },
                Default::default(),
            )
            .await
            .map_err(|error| {
                temporal_workflow::CodeToolRejection::new(
                    CodeToolRejectionKind::Unavailable,
                    error.to_string(),
                )
            })?
    }

    /// What a workflow needs to run the program itself, with the pins
    /// re-checked against the execution's open scope.
    pub async fn load(
        &self,
        descriptor: &ProgramRunDescriptor,
    ) -> Result<LoadedProgram, CodeRunError> {
        descriptor
            .validate()
            .map_err(|error| CodeRunError::Preparation(error.to_string()))?;
        let code_tools =
            CodeToolClient::new(self.client.clone(), descriptor.session_workflow_id.clone());
        let scope_id = descriptor.scope_id();
        let scope = code_tools
            .report(
                CodeToolScopeReportRequest {
                    execution_id: scope_id.clone(),
                },
                Default::default(),
            )
            .await
            .map_err(|error| CodeRunError::Preparation(error.to_string()))?
            .map_err(|error| CodeRunError::Preparation(error.to_string()))?;
        if scope.execution_id != scope_id || scope.closed {
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
                    &read(context_ref, program_engine::MAX_PROGRAM_STATE_BYTES as u64).await?,
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

    /// Admit one step's requests to the execution's scope, in issue order, then
    /// send its cancels, and return the failures the program receives for
    /// requests not admitted. A transport failure is returned as an error so
    /// the step is retried; admission and cancels are idempotent. Each
    /// admission carries `acknowledged`, the outcomes the program had
    /// collected when its current workflow task began, which the session
    /// need not hold for it any more.
    pub async fn admit_step(
        &self,
        descriptor: &ProgramRunDescriptor,
        requests: Vec<HostRequest>,
        cancels: Vec<HostCancel>,
        acknowledged: u64,
    ) -> Result<Vec<HostCompletion>, AdmitStepError> {
        let code_tools =
            CodeToolClient::new(self.client.clone(), descriptor.session_workflow_id.clone());
        let mut rejected = Vec::new();
        for request in requests {
            match self
                .admit(&code_tools, descriptor, request, acknowledged)
                .await
            {
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
        for cancel in cancels {
            let target = match cancel {
                HostCancel::Call { request_id } => {
                    harness::CodeToolCancelTarget::Call { request_id }
                }
                HostCancel::Promise { promise_id } => {
                    // Handle ids come from the session; a malformed one is
                    // refused like any cancel the session would refuse.
                    let Ok(promise_id) = harness::PromiseId::try_new(promise_id.clone()) else {
                        tracing::warn!(%promise_id, "program cancel names no promise");
                        continue;
                    };
                    harness::CodeToolCancelTarget::Promise { promise_id }
                }
            };
            let sent = code_tools
                .cancel(
                    CancelCodeToolRequest {
                        execution_id: descriptor.scope_id(),
                        target: target.clone(),
                    },
                    Default::default(),
                )
                .await
                .map_err(|error| {
                    AdmitStepError::Transport(format!("cancel {target:?}: {error}"))
                })?;
            // A refused cancel changes nothing the program can see: its
            // target was never admitted (its refusal reaches the program as
            // that call's failure) or is not the program's to cancel.
            if let Err(rejection) = sent {
                tracing::warn!(
                    scope_id = %descriptor.scope_id(),
                    ?target,
                    %rejection,
                    "program cancel refused"
                );
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
    ) -> Result<Vec<CollectedCompletion>, String> {
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
            let mut collected = materialize(self.blobs.as_ref(), outcome, max_result_bytes).await;
            if let Err(error) = &mut collected.completion.outcome {
                let structured = structured_failure(error.clone());
                // A rewritten message is no longer the blob's content.
                if structured.message != error.message {
                    collected.message_ref = None;
                }
                *error = structured;
            }
            completions.push(collected);
        }
        Ok(completions)
    }

    /// What a finished program did, with its scope as it settled. The
    /// workflow has closed the scope and waited for it; this only reads it.
    pub async fn conclude(
        &self,
        descriptor: &ProgramRunDescriptor,
        report: program_engine::ExecutionReport,
        cleanup_error: Option<String>,
    ) -> CodeRunReport {
        let code_tools =
            CodeToolClient::new(self.client.clone(), descriptor.session_workflow_id.clone());
        let (scope, report_error) = match Self::report(&code_tools, &descriptor.scope_id()).await {
            Ok(scope) => (Some(scope), None),
            Err(error) => (None, Some(format!("scope report unavailable: {error}"))),
        };
        CodeRunReport {
            execution: report,
            scope,
            cleanup_error: cleanup_error.or(report_error),
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
        acknowledged: u64,
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
                        execution_id: descriptor.scope_id(),
                        request_id: request.request_id.clone(),
                        binding_id: request.binding_id,
                        arguments_ref,
                        acknowledged: (acknowledged > 0).then_some(acknowledged),
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
            withdrawals: 0,
            withdrawals_settled: 0,
            admitted: 0,
            recorded: 0,
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
