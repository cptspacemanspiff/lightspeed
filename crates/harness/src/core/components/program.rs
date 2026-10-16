//! Program sessions: sessions whose runs are executed by a pinned,
//! deterministic program instead of a model. The driver is fixed when the
//! session opens and recorded in its lifecycle events, so a later revision of
//! a named program never changes a running session.
use serde::{Deserialize, Serialize};

use crate::{
    ActiveRun, BlobRef, CallContext, ContextEntry, ContextEntryInput, ContextEntryKind,
    ContextEntrySource, ContextEvent, ContextRemovalReason, CoreAgentEvent, CoreAgentEventProposal,
    CoreAgentJoins, CoreAgentState, DomainError, PlanningError, PromiseEvent, PromiseId,
    PromiseOwnership, PromiseScope, PromiseStatus, REPLY_COMPLETION_KEY, RunEvent, RunFailure,
    RunFailureKind, RunId, RunSource, RunStatus, SessionId, WorkflowToolBinding,
    WorkflowToolCompletion, WorkflowToolEvent, WorkflowToolId, WorkflowToolInvocation,
    WorkflowToolInvocationId, WorkflowToolTarget,
};

/// The system workflow tool that executes a program session's own program.
/// It is admitted into every program session and is never callable from the
/// program: the run owns its single invocation.
pub const PROGRAM_EXECUTE_WORKFLOW_TOOL_ID: &str = "program-execute";
pub const PROGRAM_EXECUTE_WORKFLOW_SEMANTIC_TYPE: &str = "lightspeed.program.execute.v1";
pub const PROGRAM_EXECUTE_TOOL_NAME: &str = "program.execute";

/// The system workflow tool a program's approval-required call waits
/// through: an inbox request for the decision that, once approved, runs the
/// call itself and replies with its result. It is admitted into every
/// program session and is never callable from the program; a call its scope
/// parks emits it in place of the call's own tool.
pub const APPROVAL_WORKFLOW_TOOL_ID: &str = "input-approval";
pub const APPROVAL_WORKFLOW_SEMANTIC_TYPE: &str = "lightspeed.input.approval.v1";
pub const APPROVAL_TOOL_NAME: &str = "input.approval";

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
    /// The tool contracts the program calls, each pinned to the digest of
    /// the contract it was admitted against. Only these reach the program.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<ContractPin>,
    /// Who decides the program's approval-required calls, and how long
    /// they wait. Without it anyone with inbox access decides, within the
    /// deployment's default deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvals: Option<Box<ProgramApprovals>>,
}

/// The program's settings for approval requests.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramApprovals {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignees: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remind_after_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<String>,
}

impl PinnedProgram {
    /// Whether two pins run the same program: the same source, engine,
    /// schemas, limits and contracts. Where a pin came from does not change
    /// what it runs, so the named profile's identity and revision are ignored.
    pub fn runs_same_program_as(&self, other: &PinnedProgram) -> bool {
        self.source_ref == other.source_ref
            && self.engine == other.engine
            && self.input_schema == other.input_schema
            && self.output_schema == other.output_schema
            && self.limits == other.limits
            && self.requires == other.requires
            && self.approvals == other.approvals
    }

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
        self.limits.validate()?;
        if self.approvals.as_ref().is_some_and(|approvals| {
            approvals.deadline_ms == Some(0) || approvals.remind_after_ms == Some(0)
        }) {
            return Err(DomainError::InvariantViolation(
                "program approval timeouts must be positive".into(),
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for pin in &self.requires {
            pin.validate()?;
            if !names.insert(pin.contract.as_str()) {
                return Err(DomainError::InvariantViolation(format!(
                    "program requires contract {} more than once",
                    pin.contract
                )));
            }
        }
        Ok(())
    }
}

/// A canonical tool contract a program calls, pinned by the digest of its
/// argument and result schemas. A contract whose schemas change no longer
/// matches the pin, so the program fails at its next attempt instead of
/// calling a tool it was not written against.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "contract", derive(schemars::JsonSchema))]
pub struct ContractPin {
    pub contract: String,
    pub digest: String,
}

impl ContractPin {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_contract_name(&self.contract)?;
        validate_contract_digest(&self.digest)
    }
}

/// The contract a scope binding serves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract", derive(schemars::JsonSchema))]
pub struct ContractBinding {
    pub contract: String,
    pub digest: String,
}

impl ContractBinding {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_contract_name(&self.contract)?;
        validate_contract_digest(&self.digest)
    }
}

/// Contract names are dotted lowercase paths, `<family>.<operation>` or
/// deeper (`mcp.<server>.<tool>`). Each segment becomes a property of the
/// program's `env`, so segments are plain identifiers.
pub fn validate_contract_name(name: &str) -> Result<(), DomainError> {
    let segments = name.split('.').collect::<Vec<_>>();
    if name.len() > 256 || segments.len() < 2 || !segments.iter().all(|s| is_contract_segment(s)) {
        return Err(DomainError::InvariantViolation(format!(
            "invalid contract name {name:?}: expected dotted lowercase identifiers"
        )));
    }
    Ok(())
}

fn is_contract_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    chars.next().is_some_and(|first| first.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && segment.len() <= 64
}

/// Contract digests carry their own prefix so they are never mistaken for
/// content references, which storage finds by scanning for `sha256:` strings.
const CONTRACT_DIGEST_PREFIX: &str = "contract-sha256:";

/// The digest of a contract's canonical shape.
pub fn contract_digest(canonical: &[u8]) -> String {
    use sha2::Digest;
    format!(
        "{CONTRACT_DIGEST_PREFIX}{}",
        hex::encode(sha2::Sha256::digest(canonical))
    )
}

fn validate_contract_digest(digest: &str) -> Result<(), DomainError> {
    let valid = digest
        .strip_prefix(CONTRACT_DIGEST_PREFIX)
        .is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
    if !valid {
        return Err(DomainError::InvariantViolation(format!(
            "invalid contract digest {digest:?}"
        )));
    }
    Ok(())
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
    /// How long a human-input request waits when the call sets no deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_deadline_ms: Option<u64>,
    /// How many runs' program contexts stay in the active context. Older
    /// ones leave it when a run completes; the session log keeps them all.
    #[serde(default = "default_context_retained")]
    pub context_retained: u32,
}

pub const DEFAULT_PROGRAM_CONTEXT_RETAINED: u32 = 10;
pub const MAX_PROGRAM_CONTEXT_RETAINED: u32 = 1000;

fn default_context_retained() -> u32 {
    DEFAULT_PROGRAM_CONTEXT_RETAINED
}

impl Default for ProgramLimits {
    fn default() -> Self {
        Self {
            max_calls: 1024,
            max_in_flight: 64,
            max_wall_clock_ms: None,
            input_deadline_ms: None,
            context_retained: DEFAULT_PROGRAM_CONTEXT_RETAINED,
        }
    }
}

impl ProgramLimits {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.max_calls == 0
            || self.max_in_flight == 0
            || self.max_in_flight > self.max_calls
            || self.max_wall_clock_ms == Some(0)
            || self.input_deadline_ms == Some(0)
            || self.context_retained == 0
            || self.context_retained > MAX_PROGRAM_CONTEXT_RETAINED
        {
            return Err(DomainError::InvariantViolation(
                "program limits are outside their bounded ranges".into(),
            ));
        }
        Ok(())
    }
}

/// Source label of a program context copied into a new session from the
/// session it succeeds (a routed key's next generation).
pub const PROGRAM_CONTEXT_SEED_SOURCE: &str = "program_context.seed";
const PROGRAM_CONTEXT_MEDIA_TYPE: &str = "application/json";
const PROGRAM_CONTEXT_PREVIEW_MAX_BYTES: usize = 1024;

/// A program context to keep: the JSON value in the content store, and a
/// short display preview. A run passes one when it closes its scope; it
/// becomes a context entry only if the run completes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract", derive(schemars::JsonSchema))]
pub struct ProgramContextInput {
    pub content_ref: BlobRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

impl ProgramContextInput {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self
            .preview
            .as_ref()
            .is_some_and(|preview| preview.len() > PROGRAM_CONTEXT_PREVIEW_MAX_BYTES)
        {
            return Err(DomainError::InvariantViolation(format!(
                "program context preview exceeds {PROGRAM_CONTEXT_PREVIEW_MAX_BYTES} bytes"
            )));
        }
        Ok(())
    }

    fn entry_input(&self) -> ContextEntryInput {
        ContextEntryInput {
            origin: None,
            kind: ContextEntryKind::ProgramState,
            content: crate::ContentRef {
                content_ref: self.content_ref.clone(),
                media_type: Some(PROGRAM_CONTEXT_MEDIA_TYPE.to_owned()),
                provider_kind: None,
            },
            preview: self.preview.clone(),
            provenance_ref: None,
            token_estimate: None,
        }
    }
}

/// The newest program context in the active context: what the session's
/// next run reads as `ctx.context`.
pub fn program_context(state: &CoreAgentState) -> Option<&ContextEntry> {
    state
        .context
        .entries
        .iter()
        .rev()
        .find(|entry| entry.kind == ContextEntryKind::ProgramState)
}

/// A program context entry belongs to a program session, is unkeyed JSON,
/// and comes either from the completing active run or, before any run, from
/// the session this one succeeds.
pub(crate) fn validate_program_context_entry(
    state: &CoreAgentState,
    entry: &ContextEntry,
) -> Result<(), DomainError> {
    let invalid = |message: &str| Err(DomainError::InvariantViolation(message.to_owned()));
    let from_run = match &entry.source {
        ContextEntrySource::ProgramRun { run_id } => Some(Some(*run_id)),
        ContextEntrySource::Runtime { label } if label == PROGRAM_CONTEXT_SEED_SOURCE => Some(None),
        _ => None,
    };
    let is_program_state = entry.kind == ContextEntryKind::ProgramState;
    let Some(from_run) = from_run else {
        return if is_program_state {
            invalid("program context entries come only from program runs or a seed")
        } else {
            Ok(())
        };
    };
    if !is_program_state {
        return invalid("a program run's context entry must be program state");
    }
    if !is_program_session(state) {
        return invalid("program context requires a program session");
    }
    if entry.key.is_some()
        || entry.content.media_type.as_deref() != Some(PROGRAM_CONTEXT_MEDIA_TYPE)
    {
        return invalid("program context entries are unkeyed JSON");
    }
    match from_run {
        Some(run_id) => {
            if state
                .runs
                .active
                .as_ref()
                .is_none_or(|run| run.run_id != run_id)
            {
                return invalid("program context must come from the active run");
            }
        }
        None => {
            if state.runs.active.is_some() || program_context(state).is_some() {
                return invalid("a program context seed must precede every run's context");
            }
        }
    }
    Ok(())
}

/// The context a completing run kept, as the entry to append and the
/// removals that keep the program's newest `context_retained` entries.
fn program_context_proposals(
    state: &CoreAgentState,
    run_id: RunId,
    joins: &CoreAgentJoins,
) -> Result<Vec<CoreAgentEventProposal>, PlanningError> {
    let Some(context) = state
        .code_tools
        .scopes
        .values()
        .filter(|scope| scope.spec.owner == crate::ScopeOwner::Run { run_id })
        .find_map(|scope| scope.program_context.as_ref())
    else {
        return Ok(Vec::new());
    };
    let retained = state
        .lifecycle
        .driver
        .program()
        .map_or(DEFAULT_PROGRAM_CONTEXT_RETAINED, |program| {
            program.limits.context_retained
        })
        .max(1) as usize;
    let entries = super::context::context_entries_from_inputs(
        state,
        vec![(
            None,
            ContextEntrySource::ProgramRun { run_id },
            context.entry_input(),
        )],
    )?;
    let mut proposals = vec![CoreAgentEventProposal::new(
        joins.clone(),
        CoreAgentEvent::Context(ContextEvent::EntriesApplied {
            base_revision: state.context.revision,
            entries,
        }),
    )];
    let existing = state
        .context
        .entries
        .iter()
        .filter(|entry| entry.kind == ContextEntryKind::ProgramState)
        .map(|entry| entry.entry_id)
        .collect::<Vec<_>>();
    // The new entry counts toward the retained ones.
    let excess = (existing.len() + 1).saturating_sub(retained);
    if excess > 0 {
        proposals.push(CoreAgentEventProposal::new(
            joins.clone(),
            CoreAgentEvent::Context(ContextEvent::EntriesRemoved {
                base_revision: state.context.revision.checked_add(1).ok_or_else(|| {
                    DomainError::InvariantViolation("context revision exhausted".to_owned())
                })?,
                entry_ids: existing.into_iter().take(excess).collect(),
                reason: ContextRemovalReason::ProgramContextRetention,
            }),
        ));
    }
    Ok(proposals)
}

/// Admission of a seed: it copies a predecessor's newest program
/// context into a program session before its first run.
pub(crate) fn seed_program_context_proposals(
    state: &CoreAgentState,
    context: ProgramContextInput,
) -> Result<Vec<CoreAgentEventProposal>, DomainError> {
    context.validate()?;
    if program_context(state).is_some_and(|current| {
        current.content.content_ref == context.content_ref
            && current.source
                == (ContextEntrySource::Runtime {
                    label: PROGRAM_CONTEXT_SEED_SOURCE.to_owned(),
                })
    }) {
        return Ok(Vec::new());
    }
    let entries = super::context::context_entries_from_inputs(
        state,
        vec![(
            None,
            ContextEntrySource::Runtime {
                label: PROGRAM_CONTEXT_SEED_SOURCE.to_owned(),
            },
            context.entry_input(),
        )],
    )?;
    // A seed that arrives too late is rejected at admission, not at append.
    for entry in &entries {
        validate_program_context_entry(state, entry)?;
    }
    Ok(vec![CoreAgentEventProposal::new(
        CoreAgentJoins::default(),
        CoreAgentEvent::Context(ContextEvent::EntriesApplied {
            base_revision: state.context.revision,
            entries,
        }),
    )])
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
/// outcome, or handles its close withdrew that have not settled.
fn run_scope_has_pending_calls(state: &CoreAgentState, run_id: RunId) -> bool {
    state
        .code_tools
        .scopes
        .values()
        .filter(|scope| scope.spec.owner == crate::ScopeOwner::Run { run_id })
        .any(|scope| {
            let (withdrawn, settled) = crate::code_tool_scope_withdrawals(state, scope);
            settled < withdrawn || scope.calls.values().any(|call| !call.status.is_terminal())
        })
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
    let RunSource::Program { input_ref, .. } = &active_run.source else {
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
                let mut proposals = Vec::new();
                let event = match promise.status {
                    PromiseStatus::Pending => return Ok(Vec::new()),
                    PromiseStatus::Resolved => {
                        // A completed run's context is written with its
                        // completion; any other end leaves the old one.
                        proposals = program_context_proposals(state, run.run_id, &joins)?;
                        RunEvent::Completed {
                            run_id: run.run_id,
                            output: promise.payload_ref.clone().map(|content_ref| {
                                crate::ContentRef {
                                    content_ref,
                                    media_type: Some("application/json".to_owned()),
                                    provider_kind: None,
                                }
                            }),
                        }
                    }
                    PromiseStatus::Failed | PromiseStatus::Cancelled => RunEvent::Failed {
                        run_id: run.run_id,
                        failure: RunFailure {
                            kind: RunFailureKind::Program,
                            message_ref: promise.error_ref.clone(),
                        },
                    },
                };
                proposals.push(proposal(CoreAgentEvent::Run(event)));
                Ok(proposals)
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
    let RunSource::Program { input_ref, .. } = &run.source else {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(contract: &str) -> ContractPin {
        ContractPin {
            contract: contract.to_owned(),
            digest: contract_digest(contract.as_bytes()),
        }
    }

    #[test]
    fn contract_names_are_dotted_lowercase_paths() {
        for valid in ["vfs.read_file", "time.sleep", "mcp.github.list_issues2"] {
            assert!(validate_contract_name(valid).is_ok(), "{valid}");
        }
        for invalid in [
            "sleep",
            "vfs.",
            ".read",
            "vfs.Read",
            "vfs.read-file",
            "vfs.1read",
        ] {
            assert!(validate_contract_name(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn pinned_programs_require_each_contract_once_with_a_sha256_digest() {
        let mut program = PinnedProgram {
            program_id: None,
            revision: None,
            source_ref: BlobRef::from_bytes(b"export default {}"),
            engine: ProgramEngine {
                kind: ProgramEngineKind::Js,
                semantics_version: 1,
            },
            input_schema: None,
            output_schema: None,
            limits: ProgramLimits::default(),
            requires: vec![pin("vfs.read_file"), pin("time.sleep")],
            approvals: None,
        };
        program.validate().unwrap();
        let other = PinnedProgram {
            requires: vec![pin("vfs.read_file")],
            ..program.clone()
        };
        assert!(!program.runs_same_program_as(&other));

        program.requires.push(pin("time.sleep"));
        assert!(program.validate().is_err());
        program.requires.pop();
        program.requires[0].digest = "contract-sha256:abc".into();
        assert!(program.validate().is_err());
        // A content reference is not a contract digest.
        program.requires[0].digest = BlobRef::from_bytes(b"x").as_str().to_owned();
        assert!(program.validate().is_err());
    }
}
