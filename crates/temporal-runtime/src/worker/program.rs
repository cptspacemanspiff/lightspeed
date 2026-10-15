//! Code-role activities for program runs: prepare the run's scope and
//! descriptor, run one attempt, and finalize the run's reply.

use std::time::Duration;

use harness::{
    BlobRef, PromiseResolution,
    storage::{BlobStore, record_contains_edges},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use temporal_workflow::{
    CodeExecutionLimits, CodeToolClient, CodeToolRejectionKind, CodeToolScopeReport,
    CodeToolScopeReportRequest, DEFAULT_PROGRAM_ATTEMPT_TIMEOUT_MS, OpenCodeToolScopeRequest,
    ProgramAttemptActivityRequest, ProgramAttemptActivityResult, ProgramAttemptOutcome,
    ProgramFinalizeActivityRequest, ProgramPrepareActivityRequest, ProgramPrepareActivityResult,
    ProgramRunDescriptor, ProgramRunInterruption, ProgramRunTerminal, WorkflowToolStartArgs,
};
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use super::code::{
    CodeWorkerActivities, heartbeat, non_retryable, require_activity_owner, retryable,
};
use crate::{
    code::{Cancellation, CodeRunError, CodeRunReport, read_bounded},
    gateway::GatewayAgentApi,
    program::{ProgramRunner, program_catalog, put_json},
};

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const PREPARE_TIMEOUT: Duration = Duration::from_secs(45);
const SCHEMA_MAX_BYTES: u64 = 1024 * 1024;
const REPORT_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
enum PreparationError {
    #[error("invalid program run admission: {0}")]
    Invalid(String),
    #[error("program run preparation failed: {0}")]
    Transport(String),
}

fn runner(activities: &CodeWorkerActivities, api: &GatewayAgentApi) -> ProgramRunner {
    ProgramRunner::with_shared_capacity(
        api.temporal_client().clone(),
        api.store().clone(),
        activities.capacity(),
        CLEANUP_TIMEOUT,
    )
}

fn validate_start(start: &WorkflowToolStartArgs) -> Result<(), PreparationError> {
    if start.execution_id.is_empty()
        || start.universe_id != start.invocation.session_universe_id
        || start.holder_workflow_id
            != temporal_workflow::compose_workflow_id(
                start.universe_id,
                &start.invocation.session_id,
            )
        || !tools::program::is_program_execution_binding(
            start.invocation.tool_id.as_str(),
            &start.invocation.semantic_type,
        )
        || start.invocation.schema_revision != 1
        || !matches!(start.invocation.context, harness::CallContext::Run {})
    {
        return Err(PreparationError::Invalid(
            "workflow invocation identity or tool type does not match a program run".into(),
        ));
    }
    Ok(())
}

/// The program session's pinned program, checked against the start: the
/// invocation must be the run's pending program execution.
async fn pinned_program(
    api: &GatewayAgentApi,
    start: &WorkflowToolStartArgs,
) -> Result<(harness::PinnedProgram, u64, Option<BlobRef>), PreparationError> {
    validate_start(start)?;
    let loaded = api
        .load_session_state(&start.invocation.session_id)
        .await
        .map_err(|error| PreparationError::Transport(error.to_string()))?;
    let state = &loaded.state;
    let program = state
        .lifecycle
        .driver
        .program()
        .ok_or_else(|| PreparationError::Invalid("session is not a program session".into()))?;
    if harness::program_run_execution_id(state, start.invocation.run_id).as_deref()
        != Some(start.execution_id.as_str())
    {
        return Err(PreparationError::Invalid(
            "start is not the run's pending program execution".into(),
        ));
    }
    let started_at_ms = state
        .runs
        .active
        .as_ref()
        .filter(|run| run.run_id == start.invocation.run_id)
        .and_then(|run| run.started_at_ms)
        .ok_or_else(|| PreparationError::Invalid("program run has not started".into()))?;
    // The run reads the newest program context as it starts; the
    // descriptor records which, so a replay sees the same value.
    let context_ref =
        harness::program_context(state).map(|entry| entry.content.content_ref.clone());
    Ok((program.clone(), started_at_ms, context_ref))
}

fn execution_limits(
    program: &harness::PinnedProgram,
) -> Result<CodeExecutionLimits, PreparationError> {
    let defaults = harness::CodeModeLimits::default();
    let mut limits: CodeExecutionLimits = serde_json::from_value(
        serde_json::to_value(defaults)
            .map_err(|error| PreparationError::Invalid(error.to_string()))?,
    )
    .map_err(|error| PreparationError::Invalid(error.to_string()))?;
    limits.timeout_ms = program
        .limits
        .max_wall_clock_ms
        .unwrap_or(DEFAULT_PROGRAM_ATTEMPT_TIMEOUT_MS);
    limits.max_tool_calls = program.limits.max_calls;
    limits.max_outstanding_tool_calls = program.limits.max_in_flight;
    limits
        .validate()
        .map_err(|error| PreparationError::Invalid(error.to_string()))?;
    Ok(limits)
}

fn scope_request(
    start: &WorkflowToolStartArgs,
    program: &harness::PinnedProgram,
) -> OpenCodeToolScopeRequest {
    OpenCodeToolScopeRequest {
        execution_id: start.execution_id.clone(),
        parent_invocation_id: None,
        run_id: Some(start.invocation.run_id),
        allowed_tools: None,
        max_calls: program.limits.max_calls,
        max_in_flight: program.limits.max_in_flight,
    }
}

async fn prepare(
    api: &GatewayAgentApi,
    start: &WorkflowToolStartArgs,
) -> Result<ProgramRunDescriptor, PreparationError> {
    let blobs = api.store().as_ref();
    let (program, started_at_ms, context_ref) = pinned_program(api, start).await?;
    let limits = execution_limits(&program)?;
    let source = read_bounded(blobs, &program.source_ref, limits.max_source_bytes)
        .await
        .map_err(|error| PreparationError::Invalid(error.message))?;
    std::str::from_utf8(&source).map_err(|error| PreparationError::Invalid(error.to_string()))?;
    let code_tools = CodeToolClient::new(
        api.temporal_client().clone(),
        start.holder_workflow_id.clone(),
    );
    let scope = code_tools
        .open_scope(scope_request(start, &program), Default::default())
        .await
        .map_err(|error| PreparationError::Transport(error.to_string()))?
        .map_err(|error| PreparationError::Invalid(error.to_string()))?;
    if scope.closed {
        return Err(PreparationError::Invalid(
            "program run scope is closed".into(),
        ));
    }
    let catalog = program_catalog(&scope, &program.requires)
        .map_err(|error| PreparationError::Invalid(error.to_string()))?;
    let catalog = serde_json::to_vec(&catalog)
        .map_err(|error| PreparationError::Invalid(error.to_string()))?;
    let catalog_ref = blobs
        .put_bytes(catalog)
        .await
        .map_err(|error| PreparationError::Transport(error.to_string()))?;
    Ok(ProgramRunDescriptor {
        execution_id: start.execution_id.clone(),
        session_workflow_id: start.holder_workflow_id.clone(),
        run_id: start.invocation.run_id.as_u64(),
        source_ref: program.source_ref.clone(),
        input_ref: start.invocation.arguments_ref.clone(),
        catalog_ref,
        output_schema_ref: program.output_schema.clone(),
        semantics_version: program.engine.semantics_version,
        limits,
        requires: program.requires.clone(),
        started_at_ms,
        context_ref,
    })
}

pub(super) async fn prepare_activity(
    activities: &CodeWorkerActivities,
    ctx: ActivityContext,
    request: ProgramPrepareActivityRequest,
) -> Result<ProgramPrepareActivityResult, ActivityError> {
    require_activity_owner(&ctx, &request.start.execution_id)?;
    let work = async {
        let api = activities
            .universes()
            .api_for(request.start.universe_id)
            .await?;
        let result = tokio::time::timeout(PREPARE_TIMEOUT, prepare(&api, &request.start))
            .await
            .map_err(|_| retryable("program preparation timed out"))?;
        match result {
            Ok(descriptor) => Ok(ProgramPrepareActivityResult::Prepared {
                descriptor: Box::new(descriptor),
            }),
            Err(PreparationError::Invalid(message)) => {
                let error_ref = api
                    .store()
                    .put_bytes(message.into_bytes())
                    .await
                    .map_err(retryable)?;
                Ok(ProgramPrepareActivityResult::Rejected { error_ref })
            }
            Err(error) => Err(retryable(error)),
        }
    };
    tokio::pin!(work);
    let heartbeat = heartbeat(&ctx);
    tokio::pin!(heartbeat);
    tokio::select! {
        result = &mut work => result,
        () = &mut heartbeat => unreachable!("heartbeats run until activity exits"),
        () = ctx.cancelled() => Err(ActivityError::cancelled()),
    }
}

/// The durable account of one attempt, kept in CAS by reference.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgramRunReport {
    version: u32,
    execution_id: String,
    attempt: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    descriptor: Option<ProgramRunDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution: Option<codemode::ExecutionReport>,
    scope: Option<CodeToolScopeReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interruption: Option<ProgramRunInterruption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cleanup_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    report_unavailable: Option<String>,
}

async fn store_report(
    store: &store_pg::PgStore,
    report: &ProgramRunReport,
) -> Result<BlobRef, ActivityError> {
    let bytes = serde_json::to_vec(report).map_err(retryable)?;
    let reference = store.put_bytes(bytes).await.map_err(retryable)?;
    let mut children = Vec::new();
    if let Some(descriptor) = &report.descriptor {
        children.extend([
            descriptor.source_ref.clone(),
            descriptor.input_ref.clone(),
            descriptor.catalog_ref.clone(),
        ]);
        children.extend(descriptor.context_ref.iter().cloned());
    }
    if let Some(scope) = &report.scope {
        for call in scope.calls.values() {
            children.extend(call.output_ref.iter().cloned());
            children.extend(call.error_ref.iter().cloned());
            children.extend(
                call.attachments
                    .iter()
                    .flat_map(harness::Attachment::blob_refs),
            );
        }
    }
    record_contains_edges(Some(store), &reference, children)
        .await
        .map_err(retryable)?;
    Ok(reference)
}

pub(super) async fn attempt_activity(
    activities: &CodeWorkerActivities,
    ctx: ActivityContext,
    request: ProgramAttemptActivityRequest,
) -> Result<ProgramAttemptActivityResult, ActivityError> {
    // A retried attempt replays the run's recorded calls (see
    // `ProgramRunner`), so a lost worker is not a lost run.
    let attempt = ctx.info().attempt;
    let descriptor = request.descriptor;
    descriptor.validate().map_err(non_retryable)?;
    require_activity_owner(&ctx, &descriptor.execution_id)?;
    let (universe_id, _) = temporal_workflow::split_workflow_id(&descriptor.session_workflow_id)
        .ok_or_else(|| non_retryable("invalid program session identity"))?;
    let cancellation = Cancellation::default();
    let work = async {
        let api = activities.universes().api_for(universe_id).await?;
        let runner = runner(activities, &api);
        let report = match runner
            .run_attempt(descriptor.clone(), cancellation.clone())
            .await
        {
            Ok(report) => report,
            // A pinned contract that changed or was revoked fails the run
            // with its typed kind, before the program starts.
            Err(CodeRunError::Contract(error)) => {
                let report_ref = store_report(
                    api.store().as_ref(),
                    &ProgramRunReport {
                        version: 1,
                        execution_id: descriptor.execution_id.clone(),
                        attempt,
                        descriptor: Some(descriptor.clone()),
                        execution: None,
                        scope: None,
                        interruption: None,
                        cleanup_error: None,
                        report_unavailable: None,
                    },
                )
                .await?;
                return Ok(ProgramAttemptActivityResult {
                    report_ref,
                    outcome: ProgramAttemptOutcome::Threw {
                        error_kind: error.kind().to_owned(),
                        message: error.to_string(),
                    },
                });
            }
            Err(error) => return Err(non_retryable(error)),
        };
        let outcome = attempt_outcome(api.store().as_ref(), &report).await?;
        let report_ref = store_report(
            api.store().as_ref(),
            &ProgramRunReport {
                version: 1,
                execution_id: descriptor.execution_id.clone(),
                attempt,
                descriptor: Some(descriptor.clone()),
                execution: Some(report.execution.clone()),
                scope: report.scope.clone(),
                interruption: None,
                cleanup_error: report.cleanup_error.clone(),
                report_unavailable: None,
            },
        )
        .await?;
        Ok(ProgramAttemptActivityResult {
            report_ref,
            outcome,
        })
    };
    tokio::pin!(work);
    let heartbeats = heartbeat(&ctx);
    tokio::pin!(heartbeats);
    let mut cancellation_received = false;
    loop {
        tokio::select! {
            result = &mut work => return result,
            () = &mut heartbeats => unreachable!("heartbeats run until activity exits"),
            () = ctx.cancelled(), if !cancellation_received => {
                cancellation_received = true;
                cancellation.cancel();
            }
        }
    }
}

/// What the program did, from the runner's receipt: its return value is
/// the run's output, and any error is a thrown outcome.
async fn attempt_outcome(
    store: &dyn BlobStore,
    report: &CodeRunReport,
) -> Result<ProgramAttemptOutcome, ActivityError> {
    if let Some(error) = &report.execution.error {
        return Ok(ProgramAttemptOutcome::Threw {
            error_kind: format!("{:?}", error.kind).to_lowercase(),
            message: error.message.clone(),
        });
    }
    if let Some(cleanup_error) = &report.cleanup_error {
        return Ok(ProgramAttemptOutcome::Threw {
            error_kind: "cleanup".into(),
            message: cleanup_error.clone(),
        });
    }
    let output_ref = match &report.execution.return_value {
        Some(value) => Some(put_json(store, value).await.map_err(retryable)?),
        None => None,
    };
    Ok(ProgramAttemptOutcome::Returned { output_ref })
}

pub(super) async fn finalize_activity(
    activities: &CodeWorkerActivities,
    ctx: ActivityContext,
    request: ProgramFinalizeActivityRequest,
) -> Result<PromiseResolution, ActivityError> {
    require_activity_owner(&ctx, &request.start.execution_id)?;
    let work = async {
        let api = activities
            .universes()
            .api_for(request.start.universe_id)
            .await?;
        let runner = runner(activities, &api);
        finalize(&api, &runner, request).await
    };
    tokio::pin!(work);
    let heartbeats = heartbeat(&ctx);
    tokio::pin!(heartbeats);
    tokio::select! {
        result = &mut work => result,
        () = &mut heartbeats => unreachable!("heartbeats run until activity exits"),
    }
}

async fn finalize(
    api: &GatewayAgentApi,
    runner: &ProgramRunner,
    request: ProgramFinalizeActivityRequest,
) -> Result<PromiseResolution, ActivityError> {
    let start = &request.start;
    validate_start(start).map_err(non_retryable)?;
    let store = api.store();
    let code_tools = CodeToolClient::new(
        api.temporal_client().clone(),
        start.holder_workflow_id.clone(),
    );
    // Close the run's scope whatever happened to the attempt. The session
    // closes it itself once the run is cancelled; here it is closed after a
    // completed or lost attempt.
    let queried = tokio::time::timeout(
        Duration::from_secs(5),
        code_tools.report(
            CodeToolScopeReportRequest {
                execution_id: start.execution_id.clone(),
            },
            Default::default(),
        ),
    )
    .await;
    let (mut scope, mut cleanup_error) = match queried {
        Ok(Ok(Ok(scope))) => (Some(scope), None),
        Ok(Ok(Err(error))) if error.kind == CodeToolRejectionKind::UnknownScope => (None, None),
        Ok(Ok(Err(error))) => (None, Some(error.to_string())),
        Ok(Err(error)) => (None, Some(error.to_string())),
        Err(_) => (None, Some("scope report timed out".to_owned())),
    };
    if scope.as_ref().is_some_and(|scope| {
        !(scope.closed && scope.calls.values().all(|call| call.status.is_terminal()))
    }) {
        let (latest, error) = runner
            .cleanup(&code_tools, &start.execution_id, scope.take())
            .await;
        scope = latest;
        cleanup_error = error;
    }
    let attempt = match &request.terminal {
        ProgramRunTerminal::Completed { result } => Some(result),
        ProgramRunTerminal::Interrupted { result, .. } => result.as_ref(),
        ProgramRunTerminal::Rejected { .. } => None,
    };
    let report = ProgramRunReport {
        version: 1,
        execution_id: start.execution_id.clone(),
        attempt: 1,
        descriptor: request.descriptor.clone(),
        execution: None,
        scope,
        interruption: match &request.terminal {
            ProgramRunTerminal::Interrupted { reason, .. } => Some(*reason),
            _ => None,
        },
        cleanup_error,
        report_unavailable: None,
    };
    let report_ref = store_report(store.as_ref(), &report).await?;
    let failure = |status: &str, message: String, attempt_report: Option<&BlobRef>| {
        serde_json::json!({
            "status": status,
            "error": message,
            "report_ref": report_ref,
            "attempt_report_ref": attempt_report,
        })
    };
    let diagnostic = match (&request.terminal, attempt) {
        (
            ProgramRunTerminal::Completed { .. },
            Some(ProgramAttemptActivityResult {
                outcome: ProgramAttemptOutcome::Returned { output_ref },
                report_ref: attempt_ref,
            }),
        ) => {
            match validate_output(
                store.as_ref(),
                request
                    .descriptor
                    .as_ref()
                    .and_then(|d| d.output_schema_ref.as_ref()),
                output_ref.as_ref(),
            )
            .await
            {
                Ok(()) => {
                    if let Some(output_ref) = output_ref {
                        record_contains_edges(
                            Some(store.as_ref()),
                            output_ref,
                            [report_ref.clone(), attempt_ref.clone()],
                        )
                        .await
                        .map_err(retryable)?;
                    }
                    return Ok(PromiseResolution::Resolved {
                        payload_ref: output_ref.clone(),
                    });
                }
                Err(message) => failure("invalid_output", message, Some(attempt_ref)),
            }
        }
        (
            ProgramRunTerminal::Completed { .. },
            Some(ProgramAttemptActivityResult {
                outcome:
                    ProgramAttemptOutcome::Threw {
                        error_kind,
                        message,
                    },
                report_ref: attempt_ref,
            }),
        ) => failure(
            "threw",
            format!("{error_kind}: {message}"),
            Some(attempt_ref),
        ),
        (ProgramRunTerminal::Rejected { error_ref }, _) => {
            let bytes = read_bounded(store.as_ref(), error_ref, SCHEMA_MAX_BYTES)
                .await
                .map_err(|error| retryable(error.message))?;
            failure(
                "rejected",
                String::from_utf8_lossy(&bytes).into_owned(),
                None,
            )
        }
        (ProgramRunTerminal::Interrupted { reason, .. }, attempt) => failure(
            "interrupted",
            format!("{reason:?}"),
            attempt.map(|attempt| &attempt.report_ref),
        ),
        (ProgramRunTerminal::Completed { .. }, None) => {
            failure("interrupted", "attempt receipt missing".into(), None)
        }
    };
    let reference = put_json(store.as_ref(), &diagnostic)
        .await
        .map_err(retryable)?;
    record_contains_edges(Some(store.as_ref()), &reference, [report_ref])
        .await
        .map_err(retryable)?;
    Ok(PromiseResolution::Failed {
        error_ref: Some(reference),
    })
}

/// The program's output must satisfy the pinned output schema, when there
/// is one. A missing output is `null`.
async fn validate_output(
    store: &dyn BlobStore,
    schema_ref: Option<&BlobRef>,
    output_ref: Option<&BlobRef>,
) -> Result<(), String> {
    let Some(schema_ref) = schema_ref else {
        return Ok(());
    };
    let schema: Value = serde_json::from_slice(
        &read_bounded(store, schema_ref, SCHEMA_MAX_BYTES)
            .await
            .map_err(|error| error.message)?,
    )
    .map_err(|error| format!("output schema is not JSON: {error}"))?;
    let output: Value = match output_ref {
        Some(output_ref) => serde_json::from_slice(
            &read_bounded(store, output_ref, REPORT_MAX_BYTES)
                .await
                .map_err(|error| error.message)?,
        )
        .map_err(|error| format!("output is not JSON: {error}"))?,
        None => Value::Null,
    };
    jsonschema::validate(&schema, &output)
        .map_err(|error| format!("output does not satisfy the program's output schema: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_limits_follow_the_pinned_program() {
        let program = harness::PinnedProgram {
            program_id: None,
            revision: None,
            source_ref: BlobRef::from_bytes(b"x"),
            engine: harness::ProgramEngine {
                kind: harness::ProgramEngineKind::Js,
                semantics_version: 1,
            },
            input_schema: None,
            output_schema: None,
            limits: harness::ProgramLimits {
                max_calls: 300,
                max_in_flight: 20,
                max_wall_clock_ms: Some(5_000),
                input_deadline_ms: None,
                context_retained: 10,
            },
            requires: Vec::new(),
            approvals: None,
        };
        let limits = execution_limits(&program).unwrap();
        assert_eq!(
            (
                limits.timeout_ms,
                limits.max_tool_calls,
                limits.max_outstanding_tool_calls
            ),
            (5_000, 300, 20)
        );
        let unbounded = harness::PinnedProgram {
            limits: harness::ProgramLimits::default(),
            ..program
        };
        assert_eq!(
            execution_limits(&unbounded).unwrap().timeout_ms,
            DEFAULT_PROGRAM_ATTEMPT_TIMEOUT_MS
        );
    }
}
