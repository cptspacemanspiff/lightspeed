//! Host bridge for one attempt of a program run. The attempt issues the
//! program's calls through the run's scope and learns their outcomes only
//! from the scope's recorded-outcome feed, in log order, so a replaying
//! attempt will see completions in the same order a live one did.
//!
//! The program is a module whose default export has a `run(input, env, ctx)`
//! handler. Its `env` holds only the contracts it was pinned to, and every
//! attempt re-checks those pins against the scope before the program starts.
//!
//! An attempt after a lost one replays: the program re-issues the calls an
//! earlier attempt made, which are checked against their recorded binding
//! and arguments instead of being admitted again, and receives the recorded
//! outcomes in log order before any new one.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use codemode::{ExecutionEvent, HostCompletion, HostError, HostRequest};
use futures_util::{StreamExt, stream::FuturesUnordered};
use harness::{BlobRef, storage::BlobStore};
use serde_json::Value;
use temporal_workflow::{
    CodeToolCallOutcome, CodeToolClient, CodeToolRejectionKind, CodeToolScopeReport,
    CodeToolScopeReportRequest, InvokeCodeToolRequest, MAX_RECORDED_OUTCOMES_PER_PAGE,
    ProgramRunDescriptor, ScopeRecordedOutcomesRequest,
};
use temporalio_client::Client;
use tokio::{sync::Semaphore, time::Instant};
use tools::program::contracts::ContractError;

use crate::code::{
    Cancellation, CodeRunError, CodeRunReport, CodeRunner, CodeToolCatalog,
    cancellable_preparation, engine_limits, host_error, materialize, read_bounded,
};

/// Bytes of a program context's JSON shown as its preview.
const PROGRAM_CONTEXT_PREVIEW_CHARS: usize = 256;

/// How often an attempt with calls in flight asks the session for outcomes.
const OUTCOME_POLL_INTERVAL: Duration = Duration::from_millis(100);

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

    /// Run one attempt to completion. Dropping the caller cancels the
    /// interpreter; the scope is closed on every ordinary exit.
    pub async fn run_attempt(
        &self,
        descriptor: ProgramRunDescriptor,
        cancellation: Cancellation,
    ) -> Result<CodeRunReport, CodeRunError> {
        struct CancelOnDrop(Cancellation);
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }
        let _guard = CancelOnDrop(cancellation.clone());
        let runner = self.clone();
        tokio::spawn(async move { runner.attempt(descriptor, cancellation).await })
            .await
            .map_err(|error| CodeRunError::HostStopped(error.to_string()))?
    }

    async fn attempt(
        &self,
        descriptor: ProgramRunDescriptor,
        cancellation: Cancellation,
    ) -> Result<CodeRunReport, CodeRunError> {
        descriptor
            .validate()
            .map_err(|error| CodeRunError::Preparation(error.to_string()))?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(descriptor.limits.timeout_ms))
            .ok_or_else(|| CodeRunError::Configuration("timeout is too large".into()))?;
        let code_tools =
            CodeToolClient::new(self.client.clone(), descriptor.session_workflow_id.clone());
        let scope = cancellable_preparation(deadline, &cancellation, async {
            code_tools
                .report(
                    CodeToolScopeReportRequest {
                        execution_id: descriptor.execution_id.clone(),
                    },
                    Default::default(),
                )
                .await
                .map_err(|error| CodeRunError::Preparation(error.to_string()))?
                .map_err(|error| CodeRunError::Preparation(error.to_string()))
        })
        .await?;
        if scope.execution_id != descriptor.execution_id || scope.closed {
            return Err(CodeRunError::Preparation(
                "program attempt requires the run's open scope".into(),
            ));
        }
        // Calls an earlier attempt of this run admitted. The program issues
        // them again as it replays, and must issue them the same way.
        let recorded = scope.calls.clone();
        let prepare = async {
            let permit = self
                .capacity
                .acquire()
                .await
                .map_err(|error| CodeRunError::Preparation(error.to_string()))?;
            let source = read_bounded(
                self.blobs.as_ref(),
                &descriptor.source_ref,
                descriptor.limits.max_source_bytes,
            )
            .await
            .map_err(|error| CodeRunError::Preparation(error.message))?;
            let source = String::from_utf8(source)
                .map_err(|error| CodeRunError::Preparation(error.to_string()))?;
            let input = read_bounded(
                self.blobs.as_ref(),
                &descriptor.input_ref,
                descriptor.limits.max_request_bytes,
            )
            .await
            .map_err(|error| CodeRunError::Preparation(error.message))?;
            let input: Value = serde_json::from_slice(&input).map_err(|error| {
                CodeRunError::Preparation(format!("input is not JSON: {error}"))
            })?;
            let context = match &descriptor.context_ref {
                Some(context_ref) => {
                    let bytes = read_bounded(
                        self.blobs.as_ref(),
                        context_ref,
                        codemode::MAX_PROGRAM_CONTEXT_BYTES as u64,
                    )
                    .await
                    .map_err(|error| CodeRunError::Preparation(error.message))?;
                    Some(serde_json::from_slice::<Value>(&bytes).map_err(|error| {
                        CodeRunError::Preparation(format!("program context is not JSON: {error}"))
                    })?)
                }
                None => None,
            };
            let catalog = read_bounded(
                self.blobs.as_ref(),
                &descriptor.catalog_ref,
                descriptor.limits.max_catalog_bytes,
            )
            .await
            .map_err(|error| CodeRunError::Preparation(error.message))?;
            let catalog: CodeToolCatalog = serde_json::from_slice(&catalog)
                .map_err(|error| CodeRunError::Catalog(error.to_string()))?;
            // Re-check the pins against the scope as it is now: a contract
            // that changed or was revoked fails this attempt before it runs.
            if program_catalog(&scope, &descriptor.requires)?.bindings != catalog.bindings {
                return Err(CodeRunError::Catalog(
                    "program catalog does not match the scope's contract bindings".into(),
                ));
            }
            if cancellation.is_cancelled() {
                return Err(CodeRunError::Cancelled);
            }
            Ok((permit, source, input, context, catalog))
        };
        let (_permit, source, input, context, catalog) =
            match cancellable_preparation(deadline, &cancellation, prepare).await {
                Ok(prepared) => prepared,
                Err(error) => {
                    return Err(self
                        .preparation_failed(&code_tools, &descriptor.execution_id, scope, error)
                        .await);
                }
            };
        let mut limits = engine_limits(&descriptor.limits);
        limits.timeout_ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        if limits.timeout_ms == 0 {
            return Err(self
                .preparation_failed(
                    &code_tools,
                    &descriptor.execution_id,
                    scope,
                    CodeRunError::PreparationTimedOut,
                )
                .await);
        }
        let mut execution = match codemode::start(
            codemode::ExecutionInput {
                source,
                bindings: catalog.bindings,
                limits,
                program: Some(codemode::ProgramExecution {
                    input,
                    started_at_ms: descriptor.started_at_ms,
                    random_seed: random_seed(&descriptor.execution_id),
                    context,
                }),
            },
            cancellation,
        ) {
            Ok(execution) => execution,
            Err(error) => {
                return Err(self
                    .preparation_failed(&code_tools, &descriptor.execution_id, scope, error.into())
                    .await);
            }
        };
        let completions = execution.completion_sender();
        let mut admissions = FuturesUnordered::new();
        // Requests the program has issued whose outcomes have not been read
        // from the recorded feed yet.
        let mut awaited = BTreeSet::new();
        // Completions in the order the program must receive them: the
        // recorded feed in log order, which a replaying attempt sends ahead
        // of the requests they answer.
        let mut feed = VecDeque::new();
        let mut journal_read = false;
        let mut cursor = 0u64;
        let mut poll = tokio::time::interval(OUTCOME_POLL_INTERVAL);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut bridge_failure: Option<codemode::ExecutionError> = None;
        let report = loop {
            if bridge_failure.is_none()
                && let Err(error) = send_feed(&completions, &mut feed)
            {
                bridge_failure = Some(error);
                execution.cancel();
            }
            tokio::select! {
                event = execution.next_event() => match event {
                    Some(ExecutionEvent::Request(request)) => match replay_check(&recorded, &request) {
                        // A recorded request is not admitted again: its
                        // outcome, or the wait for it, is in the feed.
                        Ok(true) => { awaited.insert(request.request_id); }
                        Ok(false) => admissions.push(self.admit(&code_tools, &descriptor, request)),
                        Err(error) => if bridge_failure.is_none() {
                            bridge_failure = Some(error);
                            execution.cancel();
                        },
                    },
                    Some(ExecutionEvent::Finished(mut report)) => {
                        if let Some(error) = bridge_failure.take() {
                            report.error = Some(error);
                        }
                        break report;
                    }
                    None => break codemode::ExecutionReport {
                        output: Vec::new(), selections: Vec::new(), return_value: None,
                        program_context: None,
                        error: Some(codemode::ExecutionError {
                            kind: codemode::ExecutionErrorKind::Internal,
                            message: "interpreter stopped without a report; consult scope outcomes".into(),
                        }),
                        pending_request_ids: Vec::new(), metrics: Default::default(),
                    },
                },
                Some(admitted) = admissions.next(), if !admissions.is_empty() => match admitted {
                    Ok(request_id) => { awaited.insert(request_id); }
                    Err(Admission::Rejected(completion)) => feed.push_back(completion),
                    // The session recorded this request with another binding
                    // or arguments: an earlier attempt issued it differently.
                    Err(Admission::Conflict(request_id)) => if bridge_failure.is_none() {
                        bridge_failure = Some(diverged(&request_id, "was admitted with another binding or arguments"));
                        execution.cancel();
                    },
                },
                _ = poll.tick(), if !journal_read || !awaited.is_empty() || !feed.is_empty() => {
                    if journal_read && awaited.is_empty() {
                        // Only the queue is waiting for room; the loop's
                        // next pass retries it.
                        continue;
                    }
                    let page = code_tools
                        .recorded_outcomes(
                            ScopeRecordedOutcomesRequest {
                                execution_id: descriptor.execution_id.clone(),
                                after: cursor,
                                limit: MAX_RECORDED_OUTCOMES_PER_PAGE,
                            },
                            Default::default(),
                        )
                        .await;
                    let page = match page {
                        Ok(Ok(page)) => page,
                        Ok(Err(error)) => {
                            if bridge_failure.is_none() {
                                bridge_failure = Some(codemode::ExecutionError {
                                    kind: codemode::ExecutionErrorKind::Internal,
                                    message: format!("recorded outcomes unavailable: {error}"),
                                });
                                execution.cancel();
                            }
                            continue;
                        }
                        Err(_) => continue,
                    };
                    journal_read |= page.outcomes.len() < MAX_RECORDED_OUTCOMES_PER_PAGE as usize;
                    for outcome in &page.outcomes {
                        cursor += 1;
                        awaited.remove(&outcome.request_id);
                        feed.push_back(HostCompletion {
                            request_id: outcome.request_id.clone(),
                            outcome: materialize(
                                self.blobs.as_ref(),
                                outcome,
                                descriptor.limits.max_result_bytes,
                            )
                            .await
                            .map_err(structured_failure),
                            completed_at_ms: outcome.completed_at_ms,
                        });
                    }
                }
            }
        };
        drop(admissions);
        drop(execution);
        // A program that returned keeps its context with the scope's close;
        // the session writes it only if the run completes.
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
            .close_scope(
                &code_tools,
                &descriptor.execution_id,
                Some(scope),
                program_context,
            )
            .await;
        let cleanup_error = cleanup_error.or(close_error);
        Ok(CodeRunReport {
            execution: report,
            scope,
            cleanup_error,
        })
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

    async fn preparation_failed(
        &self,
        code_tools: &CodeToolClient,
        execution_id: &str,
        scope: CodeToolScopeReport,
        source: CodeRunError,
    ) -> CodeRunError {
        let (_, cleanup_error) = self.cleanup(code_tools, execution_id, Some(scope)).await;
        match cleanup_error {
            Some(cleanup_error) => CodeRunError::Cleanup {
                source: Box::new(source),
                cleanup_error,
            },
            None => source,
        }
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

/// Why a request was not admitted.
enum Admission {
    /// The program receives this failure in place of an outcome.
    Rejected(HostCompletion),
    /// The session holds the request with another binding or arguments.
    Conflict(String),
}

/// Marks a conflicting admission inside `admit`; never reaches a program.
const CONFLICT: &str = "conflict";

/// Whether a request the program issued was recorded by an earlier
/// attempt: `Ok(true)` if it was, with the same binding and arguments,
/// `Ok(false)` if it is new, and a nondeterminism error if the program
/// issued it differently this time.
fn replay_check(
    recorded: &BTreeMap<String, CodeToolCallOutcome>,
    request: &HostRequest,
) -> Result<bool, codemode::ExecutionError> {
    let Some(call) = recorded.get(&request.request_id) else {
        return Ok(false);
    };
    if call
        .binding_id
        .as_ref()
        .is_some_and(|binding| *binding != request.binding_id)
    {
        return Err(diverged(&request.request_id, "names another binding"));
    }
    if let Some(arguments_ref) = &call.arguments_ref {
        // Argument blobs are serde_json's serialization of a `Value`, whose
        // objects keep their keys sorted: equal arguments give equal bytes
        // whatever order the program built them in.
        let bytes = serde_json::to_vec(&request.arguments)
            .map_err(|error| diverged(&request.request_id, &error.to_string()))?;
        if BlobRef::from_bytes(&bytes) != *arguments_ref {
            return Err(diverged(&request.request_id, "has other arguments"));
        }
    }
    Ok(true)
}

fn diverged(request_id: &str, how: &str) -> codemode::ExecutionError {
    codemode::ExecutionError {
        kind: codemode::ExecutionErrorKind::Nondeterminism,
        message: format!("replayed request {request_id} {how} than the recorded run"),
    }
}

/// Send queued completions in order until the program's completion queue
/// is full. One too large to deliver is replaced by a failure in place.
fn send_feed(
    completions: &codemode::CompletionSender,
    feed: &mut VecDeque<HostCompletion>,
) -> Result<(), codemode::ExecutionError> {
    while let Some(completion) = feed.front() {
        match completions.complete(completion.clone()) {
            Ok(()) => {
                feed.pop_front();
            }
            Err(codemode::CompletionError::Full) => return Ok(()),
            Err(codemode::CompletionError::TooLarge) => {
                let request_id = completion.request_id.clone();
                feed[0] = HostCompletion {
                    request_id,
                    outcome: Err(host_error(
                        "payload_too_large",
                        "host completion exceeds result budget",
                    )),
                    completed_at_ms: completion.completed_at_ms,
                };
            }
            Err(codemode::CompletionError::Stopped) => {
                feed.clear();
                return Ok(());
            }
        }
    }
    Ok(())
}

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
) -> Result<CodeToolCatalog, ContractError> {
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
    Ok(CodeToolCatalog {
        version: 1,
        bindings: scope
            .contracts
            .iter()
            .filter(|(_, binding)| required.contains(binding.contract.as_str()))
            .map(|(handle, binding)| codemode::ToolBinding {
                name: binding.contract.clone(),
                binding_id: handle.clone(),
            })
            .collect(),
    })
}

/// The seed of a program's randomness: a function of its execution id, so
/// every attempt of a run draws the same values.
pub(crate) fn random_seed(execution_id: &str) -> [u32; 4] {
    let digest = BlobRef::from_bytes(execution_id.as_bytes());
    let hex = digest
        .as_str()
        .strip_prefix("sha256:")
        .expect("blob references are sha256 digests");
    std::array::from_fn(|word| {
        u32::from_str_radix(&hex[word * 8..word * 8 + 8], 16).expect("hex digest")
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

    #[test]
    fn random_seeds_follow_the_execution() {
        assert_eq!(random_seed("wtx:a"), random_seed("wtx:a"));
        assert_ne!(random_seed("wtx:a"), random_seed("wtx:b"));
    }

    fn recorded(binding: &str, arguments: &Value) -> BTreeMap<String, CodeToolCallOutcome> {
        [(
            "call-1".to_owned(),
            CodeToolCallOutcome {
                request_id: "call-1".into(),
                call_id: harness::ToolCallId::new("tool-1"),
                binding_id: Some(binding.into()),
                arguments_ref: Some(BlobRef::from_bytes(&serde_json::to_vec(arguments).unwrap())),
                status: temporal_workflow::CodeToolCallStatus::Pending,
                output_ref: None,
                error_ref: None,
                attachments: Vec::new(),
                completed_at_ms: None,
            },
        )]
        .into()
    }

    fn request(id: &str, binding: &str, arguments: Value) -> HostRequest {
        HostRequest {
            request_id: id.into(),
            binding_id: binding.into(),
            arguments,
        }
    }

    #[test]
    fn replayed_requests_must_match_their_recorded_binding_and_arguments() {
        let journal = recorded("h-fetch", &serde_json::json!({"url": "a", "depth": 1}));
        // Key order is not part of the arguments.
        let same: Value = serde_json::from_str(r#"{"depth": 1, "url": "a"}"#).unwrap();
        assert_eq!(
            replay_check(&journal, &request("call-1", "h-fetch", same)),
            Ok(true)
        );
        assert_eq!(
            replay_check(&journal, &request("call-2", "h-other", Value::Null)),
            Ok(false)
        );
        for diverged in [
            request(
                "call-1",
                "h-other",
                serde_json::json!({"url": "a", "depth": 1}),
            ),
            request(
                "call-1",
                "h-fetch",
                serde_json::json!({"url": "b", "depth": 1}),
            ),
            request(
                "call-1",
                "h-fetch",
                serde_json::json!({"url": "a", "depth": 1.5}),
            ),
        ] {
            let error = replay_check(&journal, &diverged).unwrap_err();
            assert_eq!(error.kind, codemode::ExecutionErrorKind::Nondeterminism);
            assert!(error.message.contains("call-1"), "{}", error.message);
        }
    }
}
