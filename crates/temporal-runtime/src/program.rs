//! Host bridge for one attempt of a program run. The attempt issues the
//! program's calls through the run's scope and learns their outcomes only
//! from the scope's recorded-outcome feed, in log order, so a replaying
//! attempt will see completions in the same order a live one did.
//!
//! This phase runs one attempt per run and does not replay a journal yet.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use codemode::{ExecutionEvent, HostCompletion, HostError, HostRequest};
use futures_util::{StreamExt, stream::FuturesUnordered};
use harness::{BlobRef, storage::BlobStore};
use serde_json::Value;
use temporal_workflow::{
    CodeToolClient, CodeToolScopeReport, CodeToolScopeReportRequest, InvokeCodeToolRequest,
    MAX_RECORDED_OUTCOMES_PER_PAGE, ProgramRunDescriptor, ScopeRecordedOutcomesRequest,
};
use temporalio_client::Client;
use tokio::{sync::Semaphore, time::Instant};

use crate::code::{
    Cancellation, CodeRunError, CodeRunReport, CodeRunner, CodeToolCatalog,
    cancellable_preparation, engine_limits, host_error, materialize, read_bounded,
};

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
        if !scope.calls.is_empty() {
            // Re-execution against the recorded journal is not implemented
            // yet; an attempt after a lost one cannot safely continue.
            return Err(CodeRunError::Preparation(
                "program attempt found earlier calls in its scope; replay is not supported yet"
                    .into(),
            ));
        }
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
            let catalog = read_bounded(
                self.blobs.as_ref(),
                &descriptor.catalog_ref,
                descriptor.limits.max_catalog_bytes,
            )
            .await
            .map_err(|error| CodeRunError::Preparation(error.message))?;
            let catalog: CodeToolCatalog = serde_json::from_slice(&catalog)
                .map_err(|error| CodeRunError::Catalog(error.to_string()))?;
            catalog.validate(&scope)?;
            if cancellation.is_cancelled() {
                return Err(CodeRunError::Cancelled);
            }
            Ok((permit, compose_source(&source, &input), catalog))
        };
        let (_permit, source, catalog) =
            match cancellable_preparation(deadline, &cancellation, prepare).await {
                Ok(prepared) => prepared,
                Err(error) => {
                    return Err(self
                        .preparation_failed(&code_tools, &descriptor.execution_id, scope, error)
                        .await);
                }
            };
        let mut limits = engine_limits(&descriptor.limits);
        // The composed source carries the run input ahead of the program.
        limits.max_source_bytes = limits.max_source_bytes.max(source.len() as u64);
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
        let mut outstanding = BTreeSet::new();
        let mut cursor = 0u64;
        let mut poll = tokio::time::interval(OUTCOME_POLL_INTERVAL);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut bridge_failure: Option<codemode::ExecutionError> = None;
        // Deliver one completion; a bridge failure is returned so the loop
        // can record it and stop the interpreter.
        let deliver = |completion: HostCompletion| -> Result<(), codemode::ExecutionError> {
            let request_id = completion.request_id.clone();
            let result = match completions.complete(completion) {
                Err(codemode::CompletionError::TooLarge) => completions.complete(HostCompletion {
                    request_id,
                    outcome: Err(host_error(
                        "payload_too_large",
                        "host completion exceeds result budget",
                    )),
                }),
                result => result,
            };
            match result {
                Err(error) if error != codemode::CompletionError::Stopped => {
                    Err(codemode::ExecutionError {
                        kind: if error == codemode::CompletionError::TooLarge {
                            codemode::ExecutionErrorKind::LimitExceeded
                        } else {
                            codemode::ExecutionErrorKind::Internal
                        },
                        message: format!("cannot deliver host completion: {error}"),
                    })
                }
                _ => Ok(()),
            }
        };
        let report = loop {
            tokio::select! {
                event = execution.next_event() => match event {
                    Some(ExecutionEvent::Request(request)) => {
                        admissions.push(self.admit(&code_tools, &descriptor, request));
                    }
                    Some(ExecutionEvent::Finished(mut report)) => {
                        if let Some(error) = bridge_failure.take() {
                            report.error = Some(error);
                        }
                        break report;
                    }
                    None => break codemode::ExecutionReport {
                        output: Vec::new(), selections: Vec::new(), return_value: None,
                        error: Some(codemode::ExecutionError {
                            kind: codemode::ExecutionErrorKind::Internal,
                            message: "interpreter stopped without a report; consult scope outcomes".into(),
                        }),
                        pending_request_ids: Vec::new(), metrics: Default::default(),
                    },
                },
                Some(admitted) = admissions.next(), if !admissions.is_empty() => match admitted {
                    // Outcomes arrive only through the recorded feed, so
                    // every attempt sees them in the same order.
                    Ok(request_id) => { outstanding.insert(request_id); }
                    Err(completion) => if let Err(error) = deliver(completion) {
                        bridge_failure = Some(error);
                        execution.cancel();
                    },
                },
                _ = poll.tick(), if !outstanding.is_empty() => {
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
                            bridge_failure = Some(codemode::ExecutionError {
                                kind: codemode::ExecutionErrorKind::Internal,
                                message: format!("recorded outcomes unavailable: {error}"),
                            });
                            execution.cancel();
                            continue;
                        }
                        Err(_) => continue,
                    };
                    for outcome in &page.outcomes {
                        cursor += 1;
                        if !outstanding.remove(&outcome.request_id) {
                            continue;
                        }
                        let completion = HostCompletion {
                            request_id: outcome.request_id.clone(),
                            outcome: materialize(
                                self.blobs.as_ref(),
                                outcome,
                                descriptor.limits.max_result_bytes,
                            )
                            .await,
                        };
                        if let Err(error) = deliver(completion) {
                            bridge_failure = Some(error);
                            execution.cancel();
                            break;
                        }
                    }
                }
            }
        };
        drop(admissions);
        drop(execution);
        let (scope, cleanup_error) = self
            .cleanup(&code_tools, &descriptor.execution_id, Some(scope))
            .await;
        Ok(CodeRunReport {
            execution: report,
            scope,
            cleanup_error,
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
    ) -> Result<String, HostCompletion> {
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
                .map_err(|error| host_error("admission_rejected", error.to_string()))?;
            Ok(())
        }
        .await;
        match admitted {
            Ok(()) => Ok(request_id),
            Err(error) => Err(HostCompletion {
                request_id,
                outcome: Err(error),
            }),
        }
    }
}

/// The program runs as a script body with its run input bound to `input`.
/// The input is inlined as JSON, so the composed source is a pure function
/// of the pinned program and the run's input.
pub(crate) fn compose_source(source: &str, input: &Value) -> String {
    format!("const input = {input};\n{source}")
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

    #[test]
    fn composed_source_binds_the_input_ahead_of_the_program() {
        let composed = compose_source(
            "return input.claim;",
            &serde_json::json!({"claim": "x\u{2028}y"}),
        );
        assert!(composed.starts_with("const input = {\"claim\":\"x\u{2028}y\"};\n"));
        assert!(composed.ends_with("return input.claim;"));
    }
}
