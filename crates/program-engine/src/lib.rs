//! The JavaScript engine of program sessions: a program is a module whose
//! default export has a `run(input, env, ctx)` handler, reaching the world
//! only through the contracts in `env`, over a JSON tool boundary.
//!
//! A program is replayable: it takes a completion only when it has no
//! runnable job, one per such quiescent point, and the host owns its clock,
//! randomness and timers. Fed the same completions in the same order it
//! issues the same requests and returns the same value, however the
//! completions were timed.
//!
//! [`ProgramInstance`] runs a program one step at a time on the caller's
//! thread, so a deterministic host such as a Temporal workflow can drive it
//! and replay it from its own history.

mod engine;
mod instance;

pub use instance::{ProgramInstance, Step, StepBudget};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolBinding {
    pub name: String,
    pub binding_id: String,
}

#[derive(Clone, Debug)]
pub struct ExecutionInput {
    pub source: String,
    pub bindings: Vec<ToolBinding>,
    pub limits: ExecutionLimits,
    pub program: ProgramExecution,
}

/// The deterministic inputs of one program execution. Everything the
/// program can observe besides its calls' outcomes comes from here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProgramExecution {
    /// The run input, `run`'s first argument.
    pub input: Value,
    /// The run's recorded start: `Date.now()` until the first completion
    /// carrying a later timestamp is delivered.
    pub started_at_ms: u64,
    /// Seeds `Math.random` and `crypto`: the same seed draws the same values.
    #[serde(rename = "seed")]
    pub random_seed: [u32; 4],
    /// The session's newest program context, `ctx.context`. Absent before
    /// any run has set one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,
}

/// The largest program context `ctx.setContext` accepts, in serialized
/// JSON bytes.
pub const MAX_PROGRAM_CONTEXT_BYTES: usize = 64 * 1024;

/// Every budget is positive; there are intentionally no deployment defaults.
#[derive(Clone, Debug)]
pub struct ExecutionLimits {
    pub max_memory_bytes: u64,
    pub max_stack_bytes: u64,
    pub max_source_bytes: u64,
    pub max_catalog_bytes: u64,
    pub max_request_bytes: u64,
    pub max_result_bytes: u64,
    pub max_output_bytes: u64,
    pub max_tool_calls: u32,
    pub max_outstanding_tool_calls: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HostRequest {
    pub request_id: String,
    pub binding_id: String,
    pub arguments: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HostError {
    pub kind: String,
    pub message: String,
    pub value: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HostCompletion {
    pub request_id: String,
    pub outcome: Result<Value, HostError>,
    /// When the host recorded the outcome. A program's clock advances to it
    /// on delivery; scripts ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionErrorKind {
    Javascript,
    UnsupportedValue,
    LimitExceeded,
    TimedOut,
    Cancelled,
    HostDisconnected,
    /// A program diverged from its recorded execution: a replayed
    /// completion named a request the program did not issue.
    Nondeterminism,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct ExecutionError {
    pub kind: ExecutionErrorKind,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    pub startup_micros: u64,
    pub elapsed_micros: u64,
    pub tool_calls: u32,
    pub pending_jobs: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub return_value: Option<Value>,
    /// The value a program last passed to `ctx.setContext`, if it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program_context: Option<Value>,
    pub error: Option<ExecutionError>,
    /// Requests issued before the program ended, whose completions have not
    /// entered this interpreter. Only the host knows their durable outcomes.
    pub pending_request_ids: Vec<String>,
    pub metrics: ExecutionMetrics,
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("invalid execution input: {0}")]
    InvalidInput(String),
    #[error("could not create the JavaScript engine: {0}")]
    Engine(String),
}

/// Count serialization without allocating another copy of a potentially large
/// host payload. Values have already crossed the caller's JSON boundary.
pub(crate) fn serialized_len(value: &impl Serialize) -> u64 {
    struct Counter(u64);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len() as u64);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => counter.0,
        Err(_) => u64::MAX,
    }
}

#[cfg(test)]
mod tests;
