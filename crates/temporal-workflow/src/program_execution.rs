//! Durable inputs for a program run: one invocation of a program session's
//! pinned program. These types carry content references and admitted
//! budgets, never source, an interpreter context, or session authority.

use harness::BlobRef;
use serde::{Deserialize, Serialize};

use crate::{CodeExecutionLimits, CodeExecutionValidationError};

/// The stable workflow type the program-execute binding's recipe starts.
pub const PROGRAM_RUN_WORKFLOW_TYPE: &str = "ProgramRunWorkflow";

/// How long one attempt may run when the program has no wall-clock limit.
pub const DEFAULT_PROGRAM_ATTEMPT_TIMEOUT_MS: u64 = 10 * 60 * 1_000;

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

/// Small, immutable input shared by program-run orchestration and its
/// runner. The program session owns the run's scope and bindings; holding
/// this descriptor authorizes nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramRunDescriptor {
    /// The run's program execution: the scope id and this workflow's id.
    pub execution_id: String,
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
    pub limits: CodeExecutionLimits,
}

impl ProgramRunDescriptor {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramAttemptActivityRequest {
    pub descriptor: ProgramRunDescriptor,
    pub attempt: u32,
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
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramRunPhase {
    #[default]
    Starting,
    Preparing,
    Running,
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
