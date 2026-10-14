//! Program sessions: sessions whose runs are executed by a pinned,
//! deterministic program instead of a model. The driver is fixed when the
//! session opens and recorded in its lifecycle events, so a later revision of
//! a named program never changes a running session.
use serde::{Deserialize, Serialize};

use crate::{BlobRef, CoreAgentState, DomainError};

/// What drives a session's runs. `Agent` is today's model loop; `Program`
/// runs a pinned program whose only effects are admitted tool calls.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionDriver {
    #[default]
    Agent,
    Program {
        program: PinnedProgram,
    },
}

impl SessionDriver {
    pub fn is_agent(&self) -> bool {
        matches!(self, Self::Agent)
    }

    pub fn is_program(&self) -> bool {
        matches!(self, Self::Program { .. })
    }

    pub fn program(&self) -> Option<&PinnedProgram> {
        match self {
            Self::Agent => None,
            Self::Program { program } => Some(program),
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::Agent => Ok(()),
            Self::Program { program } => program.validate(),
        }
    }
}

/// A program resolved and frozen at session creation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedProgram {
    /// The named program this session was created from, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program_id: Option<String>,
    /// The revision of the named program resolved at creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    /// The module bundle the executor runs.
    pub source_ref: BlobRef,
    pub engine: ProgramEngine,
    /// JSON Schema for run input, checked at run admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<BlobRef>,
    /// JSON Schema for run output, checked at run completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<BlobRef>,
    #[serde(default)]
    pub limits: ProgramLimits,
}

impl PinnedProgram {
    pub fn validate(&self) -> Result<(), DomainError> {
        if let Some(program_id) = &self.program_id {
            crate::validate_general_string_id("program_id", program_id)
                .map_err(|error| DomainError::InvariantViolation(error.to_string()))?;
        } else if self.revision.is_some() {
            return Err(DomainError::InvariantViolation(
                "program revision requires a named program".into(),
            ));
        }
        self.engine.validate()?;
        self.limits.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramEngine {
    pub kind: ProgramEngineKind,
    /// Pins the guest prelude and API the program was written against. An
    /// executor refuses a version it does not implement.
    pub semantics_version: u32,
}

impl ProgramEngine {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.semantics_version == 0 {
            return Err(DomainError::InvariantViolation(
                "program semantics version must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramEngineKind {
    Js,
}

/// Per-run budgets for a program. Calls are admitted tool calls in the run's
/// scope; the wall clock bounds one run across all of its attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramLimits {
    pub max_calls: u32,
    pub max_in_flight: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_clock_ms: Option<u64>,
}

impl Default for ProgramLimits {
    fn default() -> Self {
        Self {
            max_calls: 1024,
            max_in_flight: 64,
            max_wall_clock_ms: None,
        }
    }
}

impl ProgramLimits {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.max_calls == 0
            || self.max_in_flight == 0
            || self.max_in_flight > self.max_calls
            || self.max_wall_clock_ms == Some(0)
        {
            return Err(DomainError::InvariantViolation(
                "program limits are outside their bounded ranges".into(),
            ));
        }
        Ok(())
    }
}

pub fn is_program_session(state: &CoreAgentState) -> bool {
    state.lifecycle.driver.is_program()
}

/// Model-only operations are typed rejections on a program session rather
/// than silent no-ops.
pub(crate) fn require_agent_driver(
    state: &CoreAgentState,
    operation: &str,
) -> Result<(), DomainError> {
    if is_program_session(state) {
        return Err(DomainError::UnsupportedForSessionDriver(format!(
            "{operation} is not supported on a program session"
        )));
    }
    Ok(())
}
