//! Durable inputs for a program run: one invocation of a program session's
//! pinned program. These types carry content references and admitted
//! budgets, never source, an interpreter context, or session authority.

use harness::BlobRef;
use serde::{Deserialize, Serialize};

use crate::CodeExecutionValidationError;

/// The stable workflow type the program-execute binding's recipe starts.
pub const PROGRAM_RUN_WORKFLOW_TYPE: &str = "ProgramRunWorkflow";

/// The budgets a program run executes under: its interpreter's, and the
/// run's calls and compute across all of its executions. A run has no
/// wall-clock budget.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramExecutionLimits {
    pub max_memory_bytes: u64,
    pub max_stack_bytes: u64,
    pub max_source_bytes: u64,
    pub max_catalog_bytes: u64,
    /// Serialized JSON bytes in one tool request.
    pub max_request_bytes: u64,
    /// Serialized JSON bytes in one completion the program receives.
    pub max_result_bytes: u64,
    /// Serialized JSON bytes of the program's output.
    pub max_output_bytes: u64,
    /// Calls the run may make.
    pub max_tool_calls: u32,
    /// Calls the program may have outstanding at once.
    pub max_outstanding_tool_calls: u32,
    /// QuickJS interrupt checks the run may use.
    pub max_compute: u64,
}

impl ProgramExecutionLimits {
    pub fn validate(&self) -> Result<(), CodeExecutionValidationError> {
        for (field, value) in [
            ("max_memory_bytes", self.max_memory_bytes),
            ("max_stack_bytes", self.max_stack_bytes),
            ("max_source_bytes", self.max_source_bytes),
            ("max_catalog_bytes", self.max_catalog_bytes),
            ("max_request_bytes", self.max_request_bytes),
            ("max_result_bytes", self.max_result_bytes),
            ("max_output_bytes", self.max_output_bytes),
            ("max_tool_calls", u64::from(self.max_tool_calls)),
            (
                "max_outstanding_tool_calls",
                u64::from(self.max_outstanding_tool_calls),
            ),
            ("max_compute", self.max_compute),
        ] {
            if value == 0 {
                return Err(CodeExecutionValidationError::ZeroLimit { field });
            }
        }
        if self.max_outstanding_tool_calls > self.max_tool_calls {
            return Err(CodeExecutionValidationError::OutstandingCallsExceedTotal {
                outstanding: self.max_outstanding_tool_calls,
                total: self.max_tool_calls,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramPrepareActivityRequest {
    pub start: crate::WorkflowToolStartArgs,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProgramPrepareActivityResult {
    Prepared {
        descriptor: Box<ProgramRunDescriptor>,
    },
    Rejected {
        error_ref: BlobRef,
    },
}

/// The program workflow's input: the session's start and, for an execution
/// that continues its run, what the run carries into it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramRunInput {
    #[serde(flatten)]
    pub start: crate::WorkflowToolStartArgs,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<ProgramContinuation>,
}

/// What a run that continued as new carries into its next execution. The
/// descriptor is the run's, with the next execution's number, scope
/// bindings, state and clock: a continued run is not prepared again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramContinuation {
    pub descriptor: ProgramRunDescriptor,
    /// The stored report of the execution this one continues; each report
    /// links to the one before.
    pub previous_report: BlobRef,
}

/// Small input shared by program-run orchestration and its runner, fixed
/// for an execution. The program session owns the run's scope and
/// bindings; holding this descriptor authorizes nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramRunDescriptor {
    /// The run's program execution: this workflow's id, and the first
    /// execution's scope id.
    pub execution_id: String,
    /// Which execution of the run this is: 0 for the first, one more each
    /// time the run continues as new. Each has its own scope.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub execution: u32,
    /// The program session's composed workflow id, without a Temporal run id.
    pub session_workflow_id: String,
    pub run_id: u64,
    /// UTF-8 program source in the session's universe-scoped CAS.
    pub source_ref: BlobRef,
    /// The run's JSON input.
    pub input_ref: BlobRef,
    /// Matched callable bindings of the run's scope.
    pub catalog_ref: BlobRef,
    /// JSON Schema the program's output must satisfy, if the program has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema_ref: Option<BlobRef>,
    pub semantics_version: u32,
    pub limits: ProgramExecutionLimits,
    /// The contracts the program was pinned to; every attempt re-checks
    /// them against the scope before it starts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<harness::ContractPin>,
    /// When the run started, as the session recorded it: the program's
    /// clock before its first completion.
    pub started_at_ms: u64,
    /// The state the execution starts with, the program's `ctx.state`: the
    /// session's newest program context when the run started, or the state
    /// `ctx.continueAsNew` handed to this execution. Absent before any run
    /// has handed one over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_ref: Option<BlobRef>,
    /// Where a continued execution's clock starts: where the previous
    /// execution's stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock_ms: Option<u64>,
    /// Calls the run's earlier executions made, toward `max_tool_calls`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub calls_used: u32,
    /// Compute the run's earlier executions used, toward `max_compute`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub compute_used: u64,
}

fn is_zero<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

impl ProgramRunDescriptor {
    /// The scope of this execution of the run.
    pub fn scope_id(&self) -> String {
        harness::program_scope_id(&self.execution_id, self.execution)
    }

    pub fn validate(&self) -> Result<(), CodeExecutionValidationError> {
        if self.execution_id.trim().is_empty() {
            return Err(CodeExecutionValidationError::EmptyExecutionId);
        }
        if crate::split_workflow_id(&self.session_workflow_id).is_none_or(
            |(universe_id, session_id)| {
                crate::compose_workflow_id(universe_id, &session_id) != self.session_workflow_id
            },
        ) {
            return Err(CodeExecutionValidationError::InvalidSessionWorkflowId);
        }
        for (field, reference) in [
            ("source_ref", Some(&self.source_ref)),
            ("input_ref", Some(&self.input_ref)),
            ("catalog_ref", Some(&self.catalog_ref)),
            ("output_schema_ref", self.output_schema_ref.as_ref()),
        ] {
            if let Some(reference) = reference {
                BlobRef::parse(reference.as_str()).map_err(|source| {
                    CodeExecutionValidationError::InvalidBlobReference { field, source }
                })?;
            }
        }
        self.limits.validate()
    }
}

/// Read what a workflow needs to run the program itself: source, input,
/// context and contract bindings, with the pins re-checked against the
/// scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramLoadActivityRequest {
    pub descriptor: ProgramRunDescriptor,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProgramLoadActivityResult {
    Loaded {
        source: String,
        input: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<serde_json::Value>,
        bindings: Vec<program_engine::ToolBinding>,
        /// The program worker's history limits, recorded with the load so a
        /// replay uses the values the execution first ran with.
        #[serde(default)]
        history: ProgramHistoryLimits,
    },
    /// A pinned contract changed or was revoked: the run fails with this
    /// stored outcome before the program starts.
    Refused {
        result: ProgramAttemptActivityResult,
    },
}

/// `ctx.shouldContinue()` turns true from this many history events on,
/// unless Temporal suggested continuing as new earlier (by default at 4,096
/// events or 4 MB). A backstop for a server whose suggestion is off.
pub const DEFAULT_PROGRAM_CONTINUE_EVENTS: u32 = 10 * 1024;
/// A run fails with `history_exhausted` from this many history events on:
/// 80% of Temporal's default cap of 51,200, leaving room for the run to
/// settle its calls.
pub const DEFAULT_PROGRAM_EXHAUSTED_EVENTS: u32 = 40 * 1024;

/// How far an execution's history may grow, in events: from
/// `continue_events` the program is told to continue as new, and from
/// `exhausted_events` the run fails with `history_exhausted` instead of
/// growing on toward Temporal's cap, where the server would terminate it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramHistoryLimits {
    pub continue_events: u32,
    pub exhausted_events: u32,
}

impl Default for ProgramHistoryLimits {
    fn default() -> Self {
        Self {
            continue_events: DEFAULT_PROGRAM_CONTINUE_EVENTS,
            exhausted_events: DEFAULT_PROGRAM_EXHAUSTED_EVENTS,
        }
    }
}

impl ProgramHistoryLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.continue_events == 0 || self.continue_events >= self.exhausted_events {
            return Err(format!(
                "a program's history must reach its continue threshold ({}) before it is exhausted ({})",
                self.continue_events, self.exhausted_events
            ));
        }
        Ok(())
    }

    /// What `ctx.shouldContinue()` reports with `history_length` events so
    /// far, when Temporal has or has not `suggested` continuing as new.
    pub fn should_continue(&self, history_length: u32, suggested: bool) -> bool {
        suggested || history_length >= self.continue_events
    }

    /// Whether a run with `history_length` events so far has exhausted its
    /// history.
    pub fn exhausted(&self, history_length: u32) -> bool {
        history_length >= self.exhausted_events
    }
}

/// Admit the requests one step of the program issued to the run's scope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProgramAdmitActivityRequest {
    pub descriptor: ProgramRunDescriptor,
    pub requests: Vec<program_engine::HostRequest>,
    /// Cancels the step asked for, sent after its requests: each names a
    /// call from an earlier step, or a promise the program holds.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cancels: Vec<program_engine::HostCancel>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProgramAdmitActivityResult {
    /// Requests the session did not admit, as the failures the program
    /// receives in their place.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejected: Vec<program_engine::HostCompletion>,
}

/// Read the scope's recorded outcomes after the first `after`, as the
/// completions the program receives, in log order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramCollectActivityRequest {
    pub session_workflow_id: String,
    /// The execution's scope.
    pub execution_id: String,
    pub after: u64,
    pub max_result_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProgramCollectActivityResult {
    pub completions: Vec<program_engine::HostCompletion>,
}

/// Close a program run's scope as the run exits, whatever the exit: cancel
/// every call the program still owns, withdraw its undetached handles, and
/// keep a finished program's context with the close. Returns at once; the
/// workflow then waits, holding no worker, until every call has an outcome
/// and every withdrawn handle has settled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProgramCloseActivityRequest {
    pub universe_id: uuid::Uuid,
    pub session_workflow_id: String,
    pub execution_id: String,
    /// Which execution's scope to close.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub execution: u32,
    /// The state a program handed over through `ctx.complete`, stored with
    /// the close as the session's program context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program_context: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProgramCloseActivityResult {
    /// The scope is closed. A closed scope admits nothing, so `calls` is
    /// final: the exit waits until that many outcomes are recorded and
    /// `withdrawals` handles have settled. `recorded` and
    /// `withdrawals_settled` are the counts as the close saw them.
    Closed {
        calls: u64,
        recorded: u64,
        #[serde(default)]
        withdrawals: u64,
        #[serde(default)]
        withdrawals_settled: u64,
        /// The program's context could not be stored; the run fails.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_error: Option<String>,
    },
    /// The run never opened its scope: nothing to wait for.
    NoScope,
}

/// Conclude a program the workflow ran, once everything it started has
/// settled: keep its output and report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProgramConcludeActivityRequest {
    pub descriptor: ProgramRunDescriptor,
    pub report: program_engine::ExecutionReport,
    /// Why the run's close did not go through, if it did not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_error: Option<String>,
    /// The stored report of the execution this one continued, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_report: Option<BlobRef>,
}

/// Hand a run over to its next execution, once everything the program
/// started has settled: store the state `ctx.continueAsNew` handed over and
/// this execution's report, open the next execution's scope, and describe
/// the next execution. A local activity, so the state is not recorded in
/// history. Safe to repeat: the settled scope stays readable until the
/// execution after next opens its own.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProgramCheckpointActivityRequest {
    pub descriptor: ProgramRunDescriptor,
    /// The finished execution's report; its `state` is the handed-over
    /// state.
    pub report: program_engine::ExecutionReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_report: Option<BlobRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramCheckpointActivityResult {
    /// What the next execution carries.
    pub continuation: ProgramContinuation,
}

/// What one attempt of the program did. The detailed report stays in CAS.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramAttemptActivityResult {
    pub report_ref: BlobRef,
    pub outcome: ProgramAttemptOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProgramAttemptOutcome {
    /// The handler returned; its JSON value, if any, is the run's output.
    Returned {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_ref: Option<BlobRef>,
    },
    /// The handler threw or the attempt could not complete it.
    Threw { error_kind: String, message: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramRunInterruption {
    HolderCancelled,
    WorkflowCancelled,
    PreparationFailed,
    ActivityFailed,
    ActivityTimedOut,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProgramRunTerminal {
    Completed {
        result: ProgramAttemptActivityResult,
    },
    Rejected {
        error_ref: BlobRef,
    },
    Interrupted {
        reason: ProgramRunInterruption,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<ProgramAttemptActivityResult>,
    },
}

/// Finalization is safe to repeat: it closes the run's scope and publishes
/// the run's result even when an attempt ended without a receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramFinalizeActivityRequest {
    pub start: crate::WorkflowToolStartArgs,
    pub descriptor: Option<ProgramRunDescriptor>,
    pub terminal: ProgramRunTerminal,
    /// The stored report of the execution this one continued, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_report: Option<BlobRef>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramRunPhase {
    #[default]
    Starting,
    Preparing,
    Running,
    /// The program waits for its calls to progress.
    Suspended,
    Finalizing,
    Resolved,
    Cancelled,
}

/// Queryable orchestration state of a program run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramRunSnapshot {
    pub phase: ProgramRunPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor: Option<ProgramRunDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<ProgramRunTerminal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<harness::PromiseResolution>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_limits_tell_a_program_to_continue_then_exhaust_its_run() {
        let limits = ProgramHistoryLimits::default();
        assert!(limits.validate().is_ok());
        assert!(!limits.should_continue(100, false));
        // Temporal's suggestion comes first; the threshold is the backstop.
        assert!(limits.should_continue(100, true));
        assert!(limits.should_continue(DEFAULT_PROGRAM_CONTINUE_EVENTS, false));
        assert!(!limits.exhausted(DEFAULT_PROGRAM_EXHAUSTED_EVENTS - 1));
        assert!(limits.exhausted(DEFAULT_PROGRAM_EXHAUSTED_EVENTS));
        for (continue_events, exhausted_events) in [(0, 10), (10, 10), (20, 10)] {
            let limits = ProgramHistoryLimits {
                continue_events,
                exhausted_events,
            };
            assert!(limits.validate().is_err(), "{limits:?}");
        }
    }

    #[test]
    fn a_load_recorded_without_history_limits_replays_with_the_defaults() {
        let recorded = serde_json::json!({
            "kind": "loaded",
            "source": "export default {}",
            "input": {},
            "bindings": [],
        });
        let ProgramLoadActivityResult::Loaded { history, .. } =
            serde_json::from_value(recorded).unwrap()
        else {
            panic!("a loaded program");
        };
        assert_eq!(history, ProgramHistoryLimits::default());
    }
}
