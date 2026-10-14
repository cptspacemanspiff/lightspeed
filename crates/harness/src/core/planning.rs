//! Core planning composition for deterministic session progress.

use crate::{
    CoreAgentEventProposal, CoreAgentState, PlanningError, SessionId,
    core::components::{approval, context, program, run, tooling, turn},
};

/// Consults the run, program, tool, context, and turn planners in order and
/// returns the first non-empty batch of proposals. The program planner needs
/// the session id because a run-owned invocation is identified by it.
pub fn plan_next(
    session_id: &SessionId,
    state: &CoreAgentState,
) -> Result<Vec<CoreAgentEventProposal>, PlanningError> {
    let proposals = approval::plan_approval_next(state)?;
    if !proposals.is_empty() {
        return Ok(proposals);
    }
    let proposals = run::plan_next(state)?;
    if !proposals.is_empty() {
        return Ok(proposals);
    }
    let proposals = program::plan_next(session_id, state)?;
    if !proposals.is_empty() {
        return Ok(proposals);
    }
    for plan in [tooling::plan_next, context::plan_next, turn::plan_next] {
        let proposals = plan(state)?;
        if !proposals.is_empty() {
            return Ok(proposals);
        }
    }
    Ok(Vec::new())
}
