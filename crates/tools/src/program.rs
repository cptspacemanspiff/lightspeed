//! Admission metadata for program sessions: the system workflow tool that
//! executes a session's pinned program.

pub use harness::{
    PROGRAM_EXECUTE_TOOL_NAME, PROGRAM_EXECUTE_WORKFLOW_SEMANTIC_TYPE,
    PROGRAM_EXECUTE_WORKFLOW_TOOL_ID,
};

pub const PROGRAM_RUN_WORKFLOW_TYPE: &str = "ProgramRunWorkflow";
/// The joined-completion ceiling of the program-execute binding. A run's own
/// deadline comes from the pinned program's wall-clock limit; this ceiling
/// only bounds the binding's declaration.
pub const PROGRAM_EXECUTION_DEADLINE_CEILING_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

pub fn is_program_execution_binding(tool_id: &str, semantic_type: &str) -> bool {
    tool_id == PROGRAM_EXECUTE_WORKFLOW_TOOL_ID
        && semantic_type == PROGRAM_EXECUTE_WORKFLOW_SEMANTIC_TYPE
}
