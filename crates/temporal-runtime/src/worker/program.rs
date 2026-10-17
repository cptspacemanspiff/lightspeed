//! Program-role activities for program runs: prepare the run's scope and
//! descriptor, load the program, admit each step's requests, collect
//! recorded outcomes, close the scope as an execution exits, hand a run
//! that continues as new over to its next execution, conclude the run, and
//! finalize its reply. The program
//! itself runs in `ProgramRunWorkflow`; these activities hold no interpreter.

use std::{sync::Arc, time::Duration};

use harness::{
    BlobRef, PromiseResolution,
    storage::{BlobStore, record_contains_edges},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use temporal_workflow::{
    CodeToolClient, CodeToolRejectionKind, CodeToolScopeReport, OpenCodeToolScopeRequest,
    ProgramAttemptActivityResult, ProgramAttemptOutcome, ProgramExecutionLimits,
    ProgramFinalizeActivityRequest, ProgramHistoryLimits, ProgramPrepareActivityRequest,
    ProgramPrepareActivityResult, ProgramRunDescriptor, ProgramRunInterruption, ProgramRunTerminal,
    WorkflowToolStartArgs,
};
use temporalio_common::data_converters::RawValue;
use temporalio_common::error::ApplicationFailure;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use super::universes::WorkerUniverses;
use super::value_refs::{ValueRefCodec, ValueRefs};
use crate::{
    code::{CodeRunError, read_bounded},
    gateway::GatewayAgentApi,
    program::{AdmitStepError, CodeRunReport, ProgramRunner, program_catalog, put_json},
    universe::UniverseRuntime,
};

const PREPARE_TIMEOUT: Duration = Duration::from_secs(45);
const PROGRAM_CONTINUE_EVENTS_ENV: &str = "LIGHTSPEED_PROGRAM_CONTINUE_EVENTS";
const PROGRAM_EXHAUSTED_EVENTS_ENV: &str = "LIGHTSPEED_PROGRAM_EXHAUSTED_EVENTS";

/// The program history limits with every override the environment sets.
pub fn history_limits_from_env() -> Result<ProgramHistoryLimits, String> {
    history_limits_from_lookup(|name| std::env::var(name).ok())
}

/// The default history limits with every override `lookup` supplies; an
/// empty value keeps the default.
pub fn history_limits_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<ProgramHistoryLimits, String> {
    let read = |name: &str, default: u32| -> Result<u32, String> {
        match lookup(name).filter(|value| !value.trim().is_empty()) {
            None => Ok(default),
            Some(value) => value
                .trim()
                .parse::<u32>()
                .ok()
                .filter(|events| *events > 0)
                .ok_or_else(|| format!("{name} must be a positive number of history events")),
        }
    };
    let defaults = ProgramHistoryLimits::default();
    let limits = ProgramHistoryLimits {
        continue_events: read(PROGRAM_CONTINUE_EVENTS_ENV, defaults.continue_events)?,
        exhausted_events: read(PROGRAM_EXHAUSTED_EVENTS_ENV, defaults.exhausted_events)?,
    };
    limits.validate().map_err(|error| {
        format!("{PROGRAM_CONTINUE_EVENTS_ENV} and {PROGRAM_EXHAUSTED_EVENTS_ENV}: {error}")
    })?;
    Ok(limits)
}
const SCHEMA_MAX_BYTES: u64 = 1024 * 1024;
const REPORT_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
enum PreparationError {
    #[error("invalid program run admission: {0}")]
    Invalid(String),
    #[error("program run preparation failed: {0}")]
    Transport(String),
}

/// The `program` worker role's activities.
pub struct ProgramWorkerActivities {
    universes: WorkerUniverses,
    history: ProgramHistoryLimits,
}

impl ProgramWorkerActivities {
    pub fn for_universe(universe_id: uuid::Uuid, api: Arc<GatewayAgentApi>) -> Self {
        Self {
            universes: WorkerUniverses::Fixed { universe_id, api },
            history: ProgramHistoryLimits::default(),
        }
    }

    pub fn with_runtime(runtime: Arc<UniverseRuntime>) -> Self {
        Self {
            universes: WorkerUniverses::Runtime(runtime),
            history: ProgramHistoryLimits::default(),
        }
    }

    /// How far each program execution's history may grow.
    pub fn with_history_limits(mut self, history: ProgramHistoryLimits) -> Self {
        self.history = history;
        self
    }

    /// What `program_close` does, for a worker that wraps the activity.
    pub async fn close(
        &self,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramCloseActivityRequest,
    ) -> Result<temporal_workflow::ProgramCloseActivityResult, ActivityError> {
        close_activity(self, ctx, request).await
    }

    /// What `program_conclude` does, for a worker that wraps the activity.
    pub async fn conclude(
        &self,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramConcludeActivityRequest,
    ) -> Result<ProgramAttemptActivityResult, ActivityError> {
        conclude_activity(self, ctx, request).await
    }

    /// The codec that puts referenced values back, reading each universe's
    /// store the way these activities do.
    pub fn value_ref_codec(&self) -> ValueRefCodec {
        let universes = self.universes.clone();
        ValueRefCodec::new(Arc::new(move |universe_id| {
            let universes = universes.clone();
            Box::pin(async move {
                let api = universes
                    .api_for(universe_id)
                    .await
                    .map_err(|error| format!("universe {universe_id}: {error:?}"))?;
                let store: Arc<dyn BlobStore> = api.store().clone();
                Ok(store)
            })
        }))
    }
}

#[activities]
impl ProgramWorkerActivities {
    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_PREPARE)]
    pub async fn program_prepare(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: ProgramPrepareActivityRequest,
    ) -> Result<ProgramPrepareActivityResult, ActivityError> {
        prepare_activity(&self, ctx, request).await
    }

    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_LOAD)]
    pub async fn program_load(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramLoadActivityRequest,
    ) -> Result<RawValue, ActivityError> {
        let descriptor = request.descriptor.clone();
        let loaded = load_activity(&self, ctx, request).await?;
        let mut refs = ValueRefs::new(universe_of(&descriptor)?);
        if let temporal_workflow::ProgramLoadActivityResult::Loaded {
            source,
            input,
            context,
            ..
        } = &loaded
        {
            refs.offer_text("/source", &descriptor.source_ref, source);
            refs.offer_json("/input", &descriptor.input_ref, input);
            if let (Some(context), Some(context_ref)) = (context, &descriptor.context_ref) {
                refs.offer_json("/context", context_ref, context);
            }
        }
        refs.into_payload(&loaded).map_err(non_retryable)
    }

    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_ADMIT)]
    pub async fn program_admit(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramAdmitActivityRequest,
    ) -> Result<temporal_workflow::ProgramAdmitActivityResult, ActivityError> {
        admit_activity(&self, ctx, request).await
    }

    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_COLLECT)]
    pub async fn program_collect(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramCollectActivityRequest,
    ) -> Result<RawValue, ActivityError> {
        collect_activity(&self, ctx, request).await
    }

    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_CLOSE)]
    pub async fn program_close(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramCloseActivityRequest,
    ) -> Result<temporal_workflow::ProgramCloseActivityResult, ActivityError> {
        close_activity(&self, ctx, request).await
    }

    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_CHECKPOINT)]
    pub async fn program_checkpoint(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramCheckpointActivityRequest,
    ) -> Result<temporal_workflow::ProgramCheckpointActivityResult, ActivityError> {
        checkpoint_activity(&self, ctx, request).await
    }

    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_CONCLUDE)]
    pub async fn program_conclude(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: temporal_workflow::ProgramConcludeActivityRequest,
    ) -> Result<ProgramAttemptActivityResult, ActivityError> {
        conclude_activity(&self, ctx, request).await
    }

    #[activity(name = temporal_workflow::ACTIVITY_PROGRAM_FINALIZE)]
    pub async fn program_finalize(
        self: Arc<Self>,
        ctx: ActivityContext,
        request: ProgramFinalizeActivityRequest,
    ) -> Result<PromiseResolution, ActivityError> {
        finalize_activity(&self, ctx, request).await
    }
}

fn runner(api: &GatewayAgentApi) -> ProgramRunner {
    ProgramRunner::new(api.temporal_client().clone(), api.store().clone())
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

/// The run's budgets: the interpreter's are code mode's defaults; calls and
/// compute are the pinned program's, for the whole run.
fn execution_limits(
    program: &harness::PinnedProgram,
) -> Result<ProgramExecutionLimits, PreparationError> {
    let defaults = harness::CodeModeLimits::default();
    let limits = ProgramExecutionLimits {
        max_memory_bytes: defaults.max_memory_bytes,
        max_stack_bytes: defaults.max_stack_bytes,
        max_source_bytes: defaults.max_source_bytes,
        max_catalog_bytes: defaults.max_catalog_bytes,
        max_request_bytes: defaults.max_request_bytes,
        max_result_bytes: defaults.max_result_bytes,
        max_output_bytes: defaults.max_output_bytes,
        max_tool_calls: program.limits.max_calls,
        max_outstanding_tool_calls: program.limits.max_in_flight,
        max_compute: program.limits.max_compute,
    };
    limits
        .validate()
        .map_err(|error| PreparationError::Invalid(error.to_string()))?;
    Ok(limits)
}

/// The scope of one execution of a run, with the budgets the run was
/// prepared with.
fn scope_request(
    execution_id: &str,
    execution: u32,
    run_id: harness::RunId,
    limits: &ProgramExecutionLimits,
) -> OpenCodeToolScopeRequest {
    OpenCodeToolScopeRequest {
        execution_id: harness::program_scope_id(execution_id, execution),
        parent_invocation_id: None,
        run_id: Some(run_id),
        allowed_tools: None,
        max_calls: limits.max_tool_calls,
        max_in_flight: limits.max_outstanding_tool_calls,
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
        .open_scope(
            scope_request(&start.execution_id, 0, start.invocation.run_id, &limits),
            Default::default(),
        )
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
        execution: 0,
        clock_ms: None,
        calls_used: 0,
        compute_used: 0,
    })
}

async fn prepare_activity(
    activities: &ProgramWorkerActivities,
    ctx: ActivityContext,
    request: ProgramPrepareActivityRequest,
) -> Result<ProgramPrepareActivityResult, ActivityError> {
    require_activity_owner(&ctx, &request.start.execution_id)?;
    let work = async {
        let api = activities
            .universes
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

/// The durable account of one attempt, kept in CAS by reference. A run that
/// continued as new has one per execution, each linked to the one before.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgramRunReport {
    version: u32,
    execution_id: String,
    attempt: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    descriptor: Option<ProgramRunDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution: Option<program_engine::ExecutionReport>,
    scope: Option<CodeToolScopeReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interruption: Option<ProgramRunInterruption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cleanup_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    report_unavailable: Option<String>,
    /// The Temporal attempt of this attempt's activity: above 1 when an
    /// earlier try was lost with its worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activity_attempt: Option<u32>,
    /// The report of the execution this one continued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous: Option<BlobRef>,
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
    children.extend(report.previous.iter().cloned());
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

/// Run `work` for an activity, heartbeating until it finishes.
async fn heartbeating<T>(
    ctx: &ActivityContext,
    work: impl std::future::Future<Output = Result<T, ActivityError>>,
) -> Result<T, ActivityError> {
    tokio::pin!(work);
    let heartbeats = heartbeat(ctx);
    tokio::pin!(heartbeats);
    tokio::select! {
        result = &mut work => result,
        () = &mut heartbeats => unreachable!("heartbeats run until activity exits"),
    }
}

fn universe_of(descriptor: &ProgramRunDescriptor) -> Result<uuid::Uuid, ActivityError> {
    temporal_workflow::split_workflow_id(&descriptor.session_workflow_id)
        .map(|(universe_id, _)| universe_id)
        .ok_or_else(|| non_retryable("invalid program session identity"))
}

async fn load_activity(
    activities: &ProgramWorkerActivities,
    ctx: ActivityContext,
    request: temporal_workflow::ProgramLoadActivityRequest,
) -> Result<temporal_workflow::ProgramLoadActivityResult, ActivityError> {
    let descriptor = request.descriptor;
    require_activity_owner(&ctx, &descriptor.execution_id)?;
    let universe_id = universe_of(&descriptor)?;
    heartbeating(&ctx, async {
        let api = activities.universes.api_for(universe_id).await?;
        match runner(&api).load(&descriptor).await {
            Ok(loaded) => Ok(temporal_workflow::ProgramLoadActivityResult::Loaded {
                source: loaded.source,
                input: loaded.input,
                context: loaded.context,
                bindings: loaded.bindings,
                history: activities.history,
            }),
            // A pinned contract that changed or was revoked fails the run
            // with its typed kind, before the program starts.
            Err(CodeRunError::Contract(error)) => {
                let report_ref = store_report(
                    api.store().as_ref(),
                    &ProgramRunReport {
                        version: 1,
                        execution_id: descriptor.execution_id.clone(),
                        attempt: 1,
                        descriptor: Some(descriptor.clone()),
                        execution: None,
                        scope: None,
                        interruption: None,
                        cleanup_error: None,
                        report_unavailable: None,
                        activity_attempt: None,
                        previous: None,
                    },
                )
                .await?;
                Ok(temporal_workflow::ProgramLoadActivityResult::Refused {
                    result: ProgramAttemptActivityResult {
                        report_ref,
                        outcome: ProgramAttemptOutcome::Threw {
                            error_kind: error.kind().to_owned(),
                            message: error.to_string(),
                        },
                    },
                })
            }
            Err(CodeRunError::Preparation(message)) => Err(retryable(message)),
            Err(error) => Err(non_retryable(error)),
        }
    })
    .await
}

async fn admit_activity(
    activities: &ProgramWorkerActivities,
    ctx: ActivityContext,
    request: temporal_workflow::ProgramAdmitActivityRequest,
) -> Result<temporal_workflow::ProgramAdmitActivityResult, ActivityError> {
    let descriptor = request.descriptor;
    require_activity_owner(&ctx, &descriptor.execution_id)?;
    let universe_id = universe_of(&descriptor)?;
    heartbeating(&ctx, async {
        let api = activities.universes.api_for(universe_id).await?;
        match runner(&api)
            .admit_step(&descriptor, request.requests, request.cancels)
            .await
        {
            Ok(rejected) => Ok(temporal_workflow::ProgramAdmitActivityResult { rejected }),
            Err(error @ AdmitStepError::Transport(_)) => Err(retryable(error)),
            Err(error @ AdmitStepError::Diverged(_)) => Err(non_retryable(error)),
        }
    })
    .await
}

async fn collect_activity(
    activities: &ProgramWorkerActivities,
    ctx: ActivityContext,
    request: temporal_workflow::ProgramCollectActivityRequest,
) -> Result<RawValue, ActivityError> {
    let (universe_id, _) = temporal_workflow::split_workflow_id(&request.session_workflow_id)
        .ok_or_else(|| non_retryable("invalid program session identity"))?;
    heartbeating(&ctx, async {
        let api = activities.universes.api_for(universe_id).await?;
        let collected = runner(&api)
            .collect(
                &request.session_workflow_id,
                &request.execution_id,
                request.after,
                request.max_result_bytes,
            )
            .await
            .map_err(retryable)?;
        // Large values and messages travel by reference to the session's
        // blobs; the worker's codec puts them back.
        let mut refs = ValueRefs::new(universe_id);
        let mut completions = Vec::with_capacity(collected.len());
        for (index, part) in collected.into_iter().enumerate() {
            let at = format!("/completions/{index}/outcome");
            match (&part.completion.outcome, &part.value_ref) {
                (Ok(value), Some(blob)) => refs.offer_json(format!("{at}/Ok"), blob, value),
                (Err(error), value_ref) => {
                    if let (Some(value), Some(blob)) = (&error.value, value_ref) {
                        refs.offer_json(format!("{at}/Err/value"), blob, value);
                    }
                    if let Some(blob) = &part.message_ref {
                        refs.offer_text(format!("{at}/Err/message"), blob, &error.message);
                    }
                }
                (Ok(_), None) => {}
            }
            completions.push(part.completion);
        }
        refs.into_payload(&temporal_workflow::ProgramCollectActivityResult { completions })
            .map_err(non_retryable)
    })
    .await
}

async fn close_activity(
    activities: &ProgramWorkerActivities,
    ctx: ActivityContext,
    request: temporal_workflow::ProgramCloseActivityRequest,
) -> Result<temporal_workflow::ProgramCloseActivityResult, ActivityError> {
    require_activity_owner(&ctx, &request.execution_id)?;
    heartbeating(&ctx, async {
        let api = activities.universes.api_for(request.universe_id).await?;
        runner(&api)
            .close(
                &request.session_workflow_id,
                &harness::program_scope_id(&request.execution_id, request.execution),
                request.program_context,
            )
            .await
            .map_err(retryable)
    })
    .await
}

async fn conclude_activity(
    activities: &ProgramWorkerActivities,
    ctx: ActivityContext,
    request: temporal_workflow::ProgramConcludeActivityRequest,
) -> Result<ProgramAttemptActivityResult, ActivityError> {
    let descriptor = request.descriptor;
    require_activity_owner(&ctx, &descriptor.execution_id)?;
    let universe_id = universe_of(&descriptor)?;
    heartbeating(&ctx, async {
        let api = activities.universes.api_for(universe_id).await?;
        let report = runner(&api)
            .conclude(&descriptor, request.report, request.cleanup_error)
            .await;
        let outcome = attempt_outcome(api.store().as_ref(), &report).await?;
        let report_ref = store_report(
            api.store().as_ref(),
            &ProgramRunReport {
                version: 1,
                execution_id: descriptor.execution_id.clone(),
                attempt: 1,
                descriptor: Some(descriptor.clone()),
                execution: Some(report.execution.clone()),
                scope: report.scope.clone(),
                interruption: None,
                cleanup_error: report.cleanup_error.clone(),
                report_unavailable: None,
                activity_attempt: Some(ctx.info().attempt),
                previous: request.previous_report,
            },
        )
        .await?;
        Ok(ProgramAttemptActivityResult {
            report_ref,
            outcome,
        })
    })
    .await
}

/// Store the state `ctx.continueAsNew` handed over and this execution's
/// report, open the next execution's scope, and describe the next
/// execution. Safe to repeat: the settled scope stays in session state until
/// the execution after next opens its own, and opening a scope is
/// idempotent.
async fn checkpoint_activity(
    activities: &ProgramWorkerActivities,
    ctx: ActivityContext,
    request: temporal_workflow::ProgramCheckpointActivityRequest,
) -> Result<temporal_workflow::ProgramCheckpointActivityResult, ActivityError> {
    let descriptor = request.descriptor;
    require_activity_owner(&ctx, &descriptor.execution_id)?;
    let universe_id = universe_of(&descriptor)?;
    let execution = descriptor
        .execution
        .checked_add(1)
        .ok_or_else(|| non_retryable("program run has no next execution"))?;
    let state = request
        .report
        .state
        .clone()
        .ok_or_else(|| non_retryable("a program that continues as new hands over a state"))?;
    heartbeating(&ctx, async {
        let api = activities.universes.api_for(universe_id).await?;
        let store = api.store();
        let code_tools = CodeToolClient::new(
            api.temporal_client().clone(),
            descriptor.session_workflow_id.clone(),
        );
        let scope = ProgramRunner::report(&code_tools, &descriptor.scope_id())
            .await
            .map_err(retryable)?;
        let state_ref = put_json(store.as_ref(), &state).await.map_err(retryable)?;
        let clock_ms = request.report.clock_ms;
        let metrics = request.report.metrics.clone();
        let report_ref = store_report(
            store.as_ref(),
            &ProgramRunReport {
                version: 1,
                execution_id: descriptor.execution_id.clone(),
                attempt: 1,
                descriptor: Some(descriptor.clone()),
                execution: Some(request.report),
                scope: Some(scope),
                interruption: None,
                cleanup_error: None,
                report_unavailable: None,
                activity_attempt: None,
                previous: request.previous_report,
            },
        )
        .await?;
        let run_id = harness::RunId::new(descriptor.run_id);
        let opened = code_tools
            .open_scope(
                scope_request(
                    &descriptor.execution_id,
                    execution,
                    run_id,
                    &descriptor.limits,
                ),
                Default::default(),
            )
            .await
            .map_err(retryable)?
            .map_err(retryable)?;
        let mut next = descriptor.clone();
        next.execution = execution;
        next.context_ref = Some(state_ref);
        next.clock_ms = Some(clock_ms);
        next.calls_used = descriptor.calls_used.saturating_add(metrics.tool_calls);
        next.compute_used = descriptor.compute_used.saturating_add(metrics.compute);
        // A pinned contract that changed since keeps the old bindings here:
        // the next execution's load refuses the run with the change.
        if let Ok(catalog) = program_catalog(&opened, &next.requires) {
            let catalog = serde_json::to_vec(&catalog).map_err(non_retryable)?;
            next.catalog_ref = store.put_bytes(catalog).await.map_err(retryable)?;
        }
        Ok(temporal_workflow::ProgramCheckpointActivityResult {
            continuation: temporal_workflow::ProgramContinuation {
                descriptor: next,
                previous_report: report_ref,
            },
        })
    })
    .await
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

async fn finalize_activity(
    activities: &ProgramWorkerActivities,
    ctx: ActivityContext,
    request: ProgramFinalizeActivityRequest,
) -> Result<PromiseResolution, ActivityError> {
    require_activity_owner(&ctx, &request.start.execution_id)?;
    heartbeating(&ctx, async {
        let api = activities
            .universes
            .api_for(request.start.universe_id)
            .await?;
        let runner = runner(&api);
        finalize(&api, &runner, request).await
    })
    .await
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
    // The workflow closed the execution's scope and waited for everything
    // in it to settle before finalizing. Only if that close failed is the
    // scope still open: close it now, without waiting, and say so.
    let scope_id = request.descriptor.as_ref().map_or_else(
        || start.execution_id.clone(),
        ProgramRunDescriptor::scope_id,
    );
    let queried = tokio::time::timeout(
        Duration::from_secs(5),
        ProgramRunner::report(&code_tools, &scope_id),
    )
    .await;
    let (mut scope, mut cleanup_error) = match queried {
        Ok(Ok(scope)) => (Some(scope), None),
        Ok(Err(error)) if error.kind == CodeToolRejectionKind::UnknownScope => (None, None),
        Ok(Err(error)) => (None, Some(error.to_string())),
        Err(_) => (None, Some("scope report timed out".to_owned())),
    };
    if scope.as_ref().is_some_and(|scope| !scope.closed) {
        let closed = runner
            .close(&start.holder_workflow_id, &scope_id, None)
            .await;
        cleanup_error = Some(match closed {
            Ok(_) => "scope closed without waiting for its outcomes".to_owned(),
            Err(error) => format!("scope close failed: {error}"),
        });
        scope = ProgramRunner::report(&code_tools, &scope_id)
            .await
            .ok()
            .or(scope);
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
        activity_attempt: None,
        previous: request.previous_report.clone(),
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

fn require_activity_owner(ctx: &ActivityContext, execution_id: &str) -> Result<(), ActivityError> {
    if ctx.info().workflow_id.as_deref() != Some(execution_id) {
        return Err(non_retryable("program activity workflow identity mismatch"));
    }
    Ok(())
}

async fn heartbeat(ctx: &ActivityContext) {
    let mut ticks =
        tokio::time::interval(temporal_workflow::ACTIVITY_CANCELLATION_HEARTBEAT_INTERVAL);
    loop {
        ticks.tick().await;
        ctx.record_heartbeat(())
            .await
            .expect("unit heartbeat serializes");
    }
}

fn retryable(error: impl std::fmt::Display) -> ActivityError {
    ActivityError::application(ApplicationFailure::new(anyhow::anyhow!(error.to_string())))
}

fn non_retryable(error: impl std::fmt::Display) -> ActivityError {
    ActivityError::application(ApplicationFailure::non_retryable(anyhow::anyhow!(
        error.to_string()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn program_activity_names_match_the_workflow_contract() {
        use temporal_workflow::WorkflowActivities;
        for (worker, workflow) in [
            (
                ProgramWorkerActivities::program_prepare.name(),
                WorkflowActivities::program_prepare.name(),
            ),
            (
                ProgramWorkerActivities::program_load.name(),
                WorkflowActivities::program_load.name(),
            ),
            (
                ProgramWorkerActivities::program_admit.name(),
                WorkflowActivities::program_admit.name(),
            ),
            (
                ProgramWorkerActivities::program_collect.name(),
                WorkflowActivities::program_collect.name(),
            ),
            (
                ProgramWorkerActivities::program_close.name(),
                WorkflowActivities::program_close.name(),
            ),
            (
                ProgramWorkerActivities::program_checkpoint.name(),
                WorkflowActivities::program_checkpoint.name(),
            ),
            (
                ProgramWorkerActivities::program_conclude.name(),
                WorkflowActivities::program_conclude.name(),
            ),
            (
                ProgramWorkerActivities::program_finalize.name(),
                WorkflowActivities::program_finalize.name(),
            ),
        ] {
            assert_eq!(worker, workflow);
        }
    }

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
                max_compute: 9_000,
                input_deadline_ms: None,
                context_retained: 10,
            },
            requires: Vec::new(),
            approvals: None,
        };
        let limits = execution_limits(&program).unwrap();
        assert_eq!(
            (
                limits.max_tool_calls,
                limits.max_outstanding_tool_calls,
                limits.max_compute
            ),
            (300, 20, 9_000)
        );
        let defaults = harness::PinnedProgram {
            limits: harness::ProgramLimits::default(),
            ..program
        };
        let limits = execution_limits(&defaults).unwrap();
        assert_eq!(
            (limits.max_tool_calls, limits.max_compute),
            (
                harness::DEFAULT_PROGRAM_MAX_CALLS,
                harness::DEFAULT_PROGRAM_MAX_COMPUTE
            )
        );
    }

    #[test]
    fn history_limits_come_from_the_environment_or_default() {
        let lookup = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| (*value).to_owned())
            }
        };
        assert_eq!(
            history_limits_from_lookup(lookup(&[])).unwrap(),
            ProgramHistoryLimits::default()
        );
        assert_eq!(
            history_limits_from_lookup(lookup(&[
                (PROGRAM_CONTINUE_EVENTS_ENV, " 60 "),
                (PROGRAM_EXHAUSTED_EVENTS_ENV, "200"),
            ]))
            .unwrap(),
            ProgramHistoryLimits {
                continue_events: 60,
                exhausted_events: 200,
            }
        );
        // An empty value keeps the default.
        assert_eq!(
            history_limits_from_lookup(lookup(&[(PROGRAM_CONTINUE_EVENTS_ENV, "")]))
                .unwrap()
                .continue_events,
            ProgramHistoryLimits::default().continue_events
        );
        for invalid in [
            &[(PROGRAM_CONTINUE_EVENTS_ENV, "0")][..],
            &[(PROGRAM_EXHAUSTED_EVENTS_ENV, "many")],
            &[
                (PROGRAM_CONTINUE_EVENTS_ENV, "300"),
                (PROGRAM_EXHAUSTED_EVENTS_ENV, "200"),
            ],
        ] {
            assert!(
                history_limits_from_lookup(lookup(invalid)).is_err(),
                "{invalid:?}"
            );
        }
    }
}
