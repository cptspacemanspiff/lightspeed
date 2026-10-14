//! Program sessions: sessions whose runs are executed by a pinned,
//! deterministic program instead of a model. The driver is fixed when the
//! session opens and recorded in its lifecycle events, so a later revision of
//! a named program never changes a running session.
use serde::{Deserialize, Serialize};

use crate::{
    ActiveRun, BlobRef, CallContext, CoreAgentEvent, CoreAgentEventProposal, CoreAgentJoins,
    CoreAgentState, DomainError, PlanningError, PromiseEvent, PromiseId, PromiseOwnership,
    PromiseScope, PromiseStatus, REPLY_COMPLETION_KEY, RunEvent, RunFailure, RunFailureKind, RunId,
    RunSource, RunStatus, SessionId, WorkflowToolBinding, WorkflowToolCompletion,
    WorkflowToolEvent, WorkflowToolId, WorkflowToolInvocation, WorkflowToolInvocationId,
    WorkflowToolTarget,
};

/// The system workflow tool that executes a program session's own program.
/// It is admitted into every program session and is never callable from the
/// program: the run owns its single invocation.
pub const PROGRAM_EXECUTE_WORKFLOW_TOOL_ID: &str = "program-execute";
pub const PROGRAM_EXECUTE_WORKFLOW_SEMANTIC_TYPE: &str = "lightspeed.program.execute.v1";
pub const PROGRAM_EXECUTE_TOOL_NAME: &str = "program.execute";

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

/// The run's own invocation of the program-execute workflow tool, once the
/// planner has requested its start.
pub fn program_run_invocation(
    state: &CoreAgentState,
    run_id: RunId,
) -> Option<&WorkflowToolInvocation> {
    state
        .workflow_tools
        .start_requests
        .values()
        .find(|invocation| {
            invocation.run_id == run_id && matches!(invocation.context, CallContext::Run {})
        })
}

/// The execution id of the run's program workflow, while its reply is still
/// pending. A run-owned scope must carry this id.
pub fn program_run_execution_id(state: &CoreAgentState, run_id: RunId) -> Option<String> {
    let invocation = program_run_invocation(state, run_id)?;
    if reply_promise(state, invocation)?.status != PromiseStatus::Pending {
        return None;
    }
    let binding = program_execute_binding(state)?;
    let WorkflowToolTarget::Start { start } = &binding.target else {
        return None;
    };
    Some(crate::workflow_tool_execution_id(
        &invocation.invocation_id,
        &start.recipe_fingerprint,
    ))
}

/// Whether the active run of a program session is waiting on something only
/// the outside world can advance: an active run on its program's reply, or a
/// cancelling run on its scope's calls draining. The planner has nothing to
/// propose for such a run, so the session must not drive it again until an
/// event arrives.
pub fn program_run_is_waiting(state: &CoreAgentState) -> bool {
    if !is_program_session(state) {
        return false;
    }
    let Some(run) = state.runs.active.as_ref() else {
        return false;
    };
    let reply_pending = program_run_invocation(state, run.run_id)
        .and_then(|invocation| reply_promise(state, invocation))
        .is_some_and(|promise| promise.status == PromiseStatus::Pending);
    match run.status {
        RunStatus::Active => reply_pending,
        RunStatus::Cancelling => !reply_pending && run_scope_has_pending_calls(state, run.run_id),
        RunStatus::Parked | RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled => {
            false
        }
    }
}

/// Whether the run's scope still has calls that have not reached a terminal
/// outcome.
fn run_scope_has_pending_calls(state: &CoreAgentState, run_id: RunId) -> bool {
    state
        .code_tools
        .scopes
        .values()
        .filter(|scope| scope.spec.owner == crate::ScopeOwner::Run { run_id })
        .any(|scope| scope.calls.values().any(|call| !call.status.is_terminal()))
}

fn program_execute_binding(state: &CoreAgentState) -> Option<&WorkflowToolBinding> {
    state
        .workflow_tools
        .bindings
        .get(&WorkflowToolId::new(PROGRAM_EXECUTE_WORKFLOW_TOOL_ID))
}

fn reply_promise<'a>(
    state: &'a CoreAgentState,
    invocation: &WorkflowToolInvocation,
) -> Option<&'a crate::Promise> {
    let promise_id = invocation
        .completion_promises
        .as_ref()?
        .get(REPLY_COMPLETION_KEY)?;
    state.promises.promises.get(promise_id)
}

/// A run-owned invocation is valid only as the single program execution of
/// an active program run: its arguments are the run's input and it carries
/// exactly the reply promise of a joined start.
pub(crate) fn validate_run_invocation(
    state: &CoreAgentState,
    active_run: &ActiveRun,
    binding: &WorkflowToolBinding,
    invocation: &WorkflowToolInvocation,
) -> Result<(), DomainError> {
    let invalid = |message: &str| DomainError::InvariantViolation(message.to_owned());
    if !is_program_session(state) {
        return Err(invalid("run-owned invocations require a program session"));
    }
    if binding.definition.tool_id.as_str() != PROGRAM_EXECUTE_WORKFLOW_TOOL_ID
        || !matches!(binding.target, WorkflowToolTarget::Start { .. })
        || !matches!(binding.completion, WorkflowToolCompletion::Joined { .. })
    {
        return Err(invalid(
            "run-owned invocations are limited to the joined program-execute start",
        ));
    }
    let RunSource::Program { input_ref } = &active_run.source else {
        return Err(invalid("run-owned invocation requires a program run"));
    };
    if &invocation.arguments_ref != input_ref || invocation.execution_context_ref.is_some() {
        return Err(invalid(
            "run-owned invocation arguments must be the run's program input",
        ));
    }
    if invocation
        .completion_promises
        .as_ref()
        .is_none_or(|promises| promises.len() != 1 || !promises.contains_key(REPLY_COMPLETION_KEY))
    {
        return Err(invalid(
            "run-owned invocation must carry exactly its reply promise",
        ));
    }
    if program_run_invocation(state, active_run.run_id)
        .is_some_and(|existing| existing.invocation_id != invocation.invocation_id)
    {
        return Err(invalid("program run already owns an invocation"));
    }
    Ok(())
}

/// Drives a program run: request the program execution when the run
/// starts, finish the run when the execution replies, and withdraw the
/// execution when the run is cancelled.
pub fn plan_next(
    session_id: &SessionId,
    state: &CoreAgentState,
) -> Result<Vec<CoreAgentEventProposal>, PlanningError> {
    if state.lifecycle.status != crate::CoreAgentStatus::Open || !is_program_session(state) {
        return Ok(Vec::new());
    }
    let Some(run) = state.runs.active.as_ref() else {
        return Ok(Vec::new());
    };
    let joins = CoreAgentJoins {
        run_id: Some(run.run_id),
        ..CoreAgentJoins::default()
    };
    let proposal = |event| CoreAgentEventProposal::new(joins.clone(), event);
    let invocation = program_run_invocation(state, run.run_id);
    match run.status {
        RunStatus::Active => match invocation {
            None => start_proposals(session_id, state, run),
            Some(invocation) => {
                let Some(promise) = reply_promise(state, invocation) else {
                    return Err(DomainError::InvariantViolation(
                        "program run invocation is missing its reply promise".to_owned(),
                    )
                    .into());
                };
                let event = match promise.status {
                    PromiseStatus::Pending => return Ok(Vec::new()),
                    PromiseStatus::Resolved => RunEvent::Completed {
                        run_id: run.run_id,
                        output: promise
                            .payload_ref
                            .clone()
                            .map(|content_ref| crate::ContentRef {
                                content_ref,
                                media_type: Some("application/json".to_owned()),
                                provider_kind: None,
                            }),
                    },
                    PromiseStatus::Failed | PromiseStatus::Cancelled => RunEvent::Failed {
                        run_id: run.run_id,
                        failure: RunFailure {
                            kind: RunFailureKind::Program,
                            message_ref: promise.error_ref.clone(),
                        },
                    },
                };
                Ok(vec![proposal(CoreAgentEvent::Run(event))])
            }
        },
        RunStatus::Cancelling => {
            // Withdrawing the reply promise tells the program workflow to
            // stop. The run's scope stops being live at the same moment, so
            // the session closes it and drains its calls; the run ends once
            // nothing of its execution is pending.
            if let Some(promise) =
                invocation.and_then(|invocation| reply_promise(state, invocation))
                && promise.status == PromiseStatus::Pending
            {
                return Ok(vec![proposal(CoreAgentEvent::Promise(
                    PromiseEvent::Cancelled {
                        promise_id: promise.promise_id.clone(),
                    },
                ))]);
            }
            if run_scope_has_pending_calls(state, run.run_id) {
                return Ok(Vec::new());
            }
            Ok(vec![proposal(CoreAgentEvent::Run(RunEvent::Cancelled {
                run_id: run.run_id,
            }))])
        }
        RunStatus::Parked | RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled => {
            Ok(Vec::new())
        }
    }
}

fn start_proposals(
    session_id: &SessionId,
    state: &CoreAgentState,
    run: &ActiveRun,
) -> Result<Vec<CoreAgentEventProposal>, PlanningError> {
    let joins = CoreAgentJoins {
        run_id: Some(run.run_id),
        ..CoreAgentJoins::default()
    };
    let RunSource::Program { input_ref } = &run.source else {
        return Err(DomainError::InvariantViolation(
            "program session run has no program input".to_owned(),
        )
        .into());
    };
    // A program session without its execution binding cannot run anything;
    // fail the run instead of leaving it active forever.
    let Some(binding) = program_execute_binding(state) else {
        return Ok(vec![CoreAgentEventProposal::new(
            joins,
            CoreAgentEvent::Run(RunEvent::Failed {
                run_id: run.run_id,
                failure: RunFailure {
                    kind: RunFailureKind::Program,
                    message_ref: None,
                },
            }),
        )]);
    };
    let WorkflowToolTarget::Start { start } = &binding.target else {
        return Err(DomainError::InvariantViolation(
            "program-execute binding must be a start target".to_owned(),
        )
        .into());
    };
    let context = CallContext::Run {};
    let invocation_id = WorkflowToolInvocationId::for_context(
        binding.session_universe_id,
        session_id,
        run.run_id,
        &context,
        &binding.binding_fingerprint,
    );
    let promise_id = PromiseId::from_number(
        state
            .id_cursors
            .last_promise_id
            .checked_add(1)
            .ok_or_else(|| DomainError::InvariantViolation("promise counter exhausted".into()))?,
    );
    let invocation = WorkflowToolInvocation {
        invocation_id: invocation_id.clone(),
        tool_id: binding.definition.tool_id.clone(),
        semantic_type: binding.definition.semantic_type.clone(),
        schema_revision: binding.definition.revision,
        binding_fingerprint: binding.binding_fingerprint.clone(),
        session_universe_id: binding.session_universe_id,
        session_id: session_id.clone(),
        run_id: run.run_id,
        context,
        arguments_ref: input_ref.clone(),
        execution_context_ref: None,
        completion_promises: Some(
            [(REPLY_COMPLETION_KEY.to_owned(), promise_id.clone())]
                .into_iter()
                .collect(),
        ),
    };
    let deadline_ms = state
        .lifecycle
        .driver
        .program()
        .and_then(|program| program.limits.max_wall_clock_ms)
        .and_then(|wall_clock| {
            run.started_at_ms
                .map(|started_at| started_at.saturating_add(wall_clock))
        });
    let promise = crate::Promise {
        promise_id,
        source: super::workflow_tool::completion_promise_source(
            binding,
            &invocation,
            REPLY_COMPLETION_KEY,
        )?,
        scope: PromiseScope::Run { run_id: run.run_id },
        ownership: PromiseOwnership::Runtime,
        status: PromiseStatus::Pending,
        payload_ref: None,
        error_ref: None,
        deadline_ms,
    };
    let execution_id =
        crate::workflow_tool_execution_id(&invocation.invocation_id, &start.recipe_fingerprint);
    Ok(vec![
        CoreAgentEventProposal::new(
            joins.clone(),
            CoreAgentEvent::Promise(PromiseEvent::Created { promise }),
        ),
        CoreAgentEventProposal::new(
            joins,
            CoreAgentEvent::WorkflowTool(WorkflowToolEvent::StartRequested {
                invocation,
                execution_id,
            }),
        ),
    ])
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
