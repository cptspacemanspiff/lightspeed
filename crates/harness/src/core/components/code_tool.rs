//! Scoped calls: durable tool calls made as the session by code running
//! outside it. A scope records who owns it and the policy its calls follow;
//! both are fixed when the scope opens. The code holds only the scope id and
//! opaque binding handles, so every call is admitted against the session's
//! grants and its outcome is recorded once per request id.
//!
//! Code mode is the first owner: a scope beneath a parked joined workflow-tool
//! call. The parent batch supplies causal joins only; code tool results never
//! become a model turn or replace the parent batch's suspension.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    AwaitMode, AwaitSpec, BlobRef, CoreAgentEvent, CoreAgentEventProposal, CoreAgentJoins,
    CoreAgentState, DomainError, PromiseEvent, PromiseOwnership, PromiseStatus, RunId, RunStatus,
    SessionId, ToolBatchId, ToolBatchSuspension, ToolCallId, ToolCallStatus,
    ToolInvocationBatchRequest, ToolInvocationBatchResult, ToolInvocationResult, ToolName, TurnId,
    WakeReason, WorkflowToolCompletion, WorkflowToolInvocation, WorkflowToolInvocationId,
    core::components::workflow_tool::ModelBatchCall,
};

pub const MAX_CODE_TOOL_SCOPES_PER_RUN: usize = 32;
pub const MAX_CODE_TOOL_CALLS_PER_SCOPE: u32 = 1024;
pub const MAX_CODE_TOOL_IN_FLIGHT: u32 = 64;
pub const CODE_TOOL_PROMISE_SLOTS: u64 = crate::MAX_COMPLETION_PROMISES as u64;

/// Force recovery cannot prove whether an abandoned activity committed an
/// external mutation. Its durable outcome is unknown, not a successful cancel.
pub const CODE_TOOL_INTERRUPTED_CONTENT: &str = "Code tool execution was interrupted when its owning run ended. Its outcome is unknown; an external side effect may already have occurred.";

pub fn code_tool_interrupted_ref() -> BlobRef {
    BlobRef::from_bytes(CODE_TOOL_INTERRUPTED_CONTENT.as_bytes())
}

/// Run termination is itself the durable interruption fact. Ordinary
/// cancellation drains calls before terminating; force recovery must also
/// terminate Update waiters without pretending unfinished effects were undone.
pub(crate) fn interrupt_code_tools_for_run(state: &mut CoreAgentState, run_id: crate::RunId) {
    let scopes = state
        .code_tools
        .scopes
        .iter()
        .filter(|(_, scope)| {
            scope_owner(state, &scope.spec).is_ok_and(|owner| owner.run_id == run_id)
        })
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    for id in scopes {
        let scope = state
            .code_tools
            .scopes
            .get_mut(&id)
            .expect("scope was collected above");
        scope.closed = true;
        scope.cancel_requested = true;
        let interrupted = scope
            .calls
            .values()
            .filter(|call| !call.status.is_terminal())
            .map(|call| call.spec.origin.request_id.clone())
            .collect::<Vec<_>>();
        scope.completed.extend(interrupted);
        for call in scope
            .calls
            .values_mut()
            .filter(|call| !call.status.is_terminal())
        {
            call.status = CodeToolCallStatus::Completed {
                result: ToolInvocationResult {
                    call_id: call.call_id.clone(),
                    status: ToolCallStatus::Unavailable,
                    output_ref: None,
                    error_ref: Some(code_tool_interrupted_ref()),
                    model_visible_context_entries: Vec::new(),
                    effects: Vec::new(),
                    attachments: Vec::new(),
                    duration_ms: None,
                    output_bytes: None,
                    truncated: false,
                }
                .into(),
            };
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeToolBinding {
    pub tool_id: ToolName,
    pub tool_name: ToolName,
    /// Under contract binding, the contract (and part) this handle serves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<crate::ContractBinding>,
}

/// Who holds a scope open. The owner decides what keeps the scope live, which
/// run its calls join, and which model batch (if any) they are attributed to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeOwner {
    /// Code mode: a parked joined workflow-tool call. The scope is live while
    /// that call's reply promise is pending in the parked batch, and every
    /// call joins the parent's run, turn and batch.
    ModelInvocation {
        parent_invocation_id: WorkflowToolInvocationId,
    },
    /// A program run. The scope is live while the run is active and its
    /// program execution is pending; its calls join the run alone.
    Run { run_id: RunId },
}

/// How calls in a scope are admitted, completed and retained. Recorded at open
/// and immutable afterwards, so replay applies the same rules as the live run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopePolicy {
    pub binding_mode: BindingMode,
    pub revalidation: Revalidation,
    pub completion: Completion,
    pub limits: ScopeLimits,
    pub retention: Retention,
    pub approvals: ApprovalPolicy,
}

impl ScopePolicy {
    /// The policy every code-mode scope runs under.
    pub fn code_mode(limits: ScopeLimits) -> Self {
        Self {
            binding_mode: BindingMode::Presentation,
            revalidation: Revalidation::ToolsetRevision,
            completion: Completion::Blocking,
            limits,
            retention: Retention::FullInState,
            approvals: ApprovalPolicy::Reject,
        }
    }

    /// The policy a program run's scope runs under. Bindings are canonical
    /// contracts, calls are acknowledged on admission so an attempt can
    /// release the session while they run, and a call that needs approval
    /// waits for a decision.
    pub fn program(limits: ScopeLimits) -> Self {
        Self {
            binding_mode: BindingMode::Contract,
            completion: Completion::Ack,
            approvals: ApprovalPolicy::Park,
            ..Self::code_mode(limits)
        }
    }
}

/// How binding handles resolve to tool identities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingMode {
    /// Tool names and arguments follow the model presentation of the run that
    /// owns the scope, pinned when the scope opens.
    Presentation,
    /// Every binding serves a canonical tool contract, named and pinned by
    /// digest when the scope opens. No model presentation affects it.
    Contract,
}

/// What invalidates a scope's bindings after it opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Revalidation {
    /// Any toolset change rejects further calls.
    ToolsetRevision,
}

/// When the caller's invoke request returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completion {
    /// Return once the call is terminal.
    Blocking,
    /// Return once the call is admitted or deduplicated. Outcomes are then
    /// observed through the scope report, never through the invoke reply.
    Ack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeLimits {
    pub max_calls: u32,
    pub max_in_flight: u32,
}

/// What state keeps for terminal calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    /// Full records for the owning run; prior-run scopes are dropped when the
    /// next run opens one.
    FullInState,
}

/// What happens to a call whose tool needs an approval decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalPolicy {
    /// The call fails; the scope cannot wait for a decision.
    Reject,
    /// The call waits through the approval workflow tool
    /// (`APPROVAL_WORKFLOW_TOOL_ID`), which asks for the decision in the
    /// inbox and, once approved, runs the call and replies with its result.
    Park,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CodeToolScopeSpecWire")]
pub struct CodeToolScopeSpec {
    pub execution_id: String,
    pub owner: ScopeOwner,
    pub policy: ScopePolicy,
    pub bindings: BTreeMap<String, CodeToolBinding>,
}

/// Read scope records written before owners and policies were recorded. Those
/// scopes were all code-mode scopes, so their parent invocation and limits
/// decode as the code-mode owner and policy.
#[derive(Deserialize)]
struct CodeToolScopeSpecWire {
    execution_id: String,
    #[serde(default)]
    owner: Option<ScopeOwner>,
    #[serde(default)]
    policy: Option<ScopePolicy>,
    bindings: BTreeMap<String, CodeToolBinding>,
    #[serde(default)]
    parent_invocation_id: Option<WorkflowToolInvocationId>,
    #[serde(default)]
    max_calls: Option<u32>,
    #[serde(default)]
    max_in_flight: Option<u32>,
}

impl TryFrom<CodeToolScopeSpecWire> for CodeToolScopeSpec {
    type Error = &'static str;

    fn try_from(wire: CodeToolScopeSpecWire) -> Result<Self, Self::Error> {
        let legacy_owner =
            wire.parent_invocation_id
                .map(|parent_invocation_id| ScopeOwner::ModelInvocation {
                    parent_invocation_id,
                });
        let owner = match (wire.owner, legacy_owner) {
            (Some(owner), Some(legacy)) if owner != legacy => {
                return Err("code tool scope owner conflicts with its parent invocation");
            }
            (Some(owner), _) | (None, Some(owner)) => owner,
            (None, None) => return Err("code tool scope has no owner"),
        };
        let legacy_policy =
            wire.max_calls
                .zip(wire.max_in_flight)
                .map(|(max_calls, max_in_flight)| {
                    ScopePolicy::code_mode(ScopeLimits {
                        max_calls,
                        max_in_flight,
                    })
                });
        let policy = match (wire.policy, legacy_policy) {
            (Some(policy), Some(legacy)) if policy.limits != legacy.limits => {
                return Err("code tool scope policy conflicts with its legacy limits");
            }
            (Some(policy), _) | (None, Some(policy)) => policy,
            (None, None) => return Err("code tool scope has no policy"),
        };
        Ok(Self {
            execution_id: wire.execution_id,
            owner,
            policy,
            bindings: wire.bindings,
        })
    }
}

/// The owner facts every scope has, whatever its owner kind: the run its
/// calls belong to, and the model batch they are attributed to when the owner
/// is a model call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerView {
    pub run_id: RunId,
    pub model_batch: Option<ModelBatch>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModelBatch {
    pub turn_id: TurnId,
    pub tool_batch_id: ToolBatchId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeToolOrigin {
    pub execution_id: String,
    pub request_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeToolCallSpec {
    pub origin: CodeToolOrigin,
    pub binding_id: String,
    pub tool_id: ToolName,
    pub tool_name: ToolName,
    pub arguments_ref: BlobRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeToolCall {
    pub spec: CodeToolCallSpec,
    pub call_id: ToolCallId,
    pub promise_id_base: u64,
    pub status: CodeToolCallStatus,
    /// When the session recorded the call's outcome. A program's clock
    /// advances to it when the outcome is delivered, so replay sees the
    /// same time. Absent on calls completed before this was recorded and
    /// on calls interrupted by the end of their run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
}

/// Durable code tool outcome. Effect carriers are admitted as their own domain
/// events, and model-context projections do not belong to a code tool caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CodeToolCallResultWire")]
pub struct CodeToolCallResult {
    pub call_id: ToolCallId,
    pub status: ToolCallStatus,
    pub output_ref: Option<BlobRef>,
    pub error_ref: Option<BlobRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<crate::Attachment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
    // Plain hexadecimal, not a BlobRef: the original activity result need not
    // be stored. Include every field so changed effects cannot pass as retries.
    invocation_result_digest: String,
}

impl CodeToolCallResult {
    fn matches_invocation_result(&self, result: &ToolInvocationResult) -> bool {
        self.invocation_result_digest == invocation_result_digest(result)
    }
}

impl From<ToolInvocationResult> for CodeToolCallResult {
    fn from(result: ToolInvocationResult) -> Self {
        let invocation_result_digest = invocation_result_digest(&result);
        Self {
            call_id: result.call_id,
            status: result.status,
            output_ref: result.output_ref,
            error_ref: result.error_ref,
            attachments: result.attachments,
            duration_ms: result.duration_ms,
            output_bytes: result.output_bytes,
            truncated: result.truncated,
            invocation_result_digest,
        }
    }
}

fn invocation_result_digest(result: &ToolInvocationResult) -> String {
    let bytes = serde_json::to_vec(result).expect("tool invocation results are serializable");
    hex::encode(Sha256::digest(bytes))
}

/// Read earlier code tool event/snapshot records before discarding their full
/// context and effects. Compact records already carry the original digest.
#[derive(Deserialize)]
struct CodeToolCallResultWire {
    #[serde(default)]
    invocation_result_digest: Option<String>,
    #[serde(flatten)]
    result: ToolInvocationResult,
}

impl TryFrom<CodeToolCallResultWire> for CodeToolCallResult {
    type Error = &'static str;

    fn try_from(wire: CodeToolCallResultWire) -> Result<Self, Self::Error> {
        let mut result = Self::from(wire.result);
        if let Some(digest) = wire.invocation_result_digest {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err("invalid code tool invocation result digest");
            }
            result.invocation_result_digest = digest;
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CodeToolCallStatus {
    Pending,
    Waiting { suspension: ToolBatchSuspension },
    Completed { result: CodeToolCallResult },
}

impl CodeToolCallStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeToolScope {
    pub toolset_revision: u64,
    pub spec: CodeToolScopeSpec,
    pub closed: bool,
    pub cancel_requested: bool,
    pub calls: BTreeMap<String, CodeToolCall>,
    /// Request ids in the order their completions were recorded: the log
    /// order a replaying executor is fed outcomes in. It starts at absolute
    /// position `completed_base`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completed: Vec<String>,
    /// Completions recorded before `completed` begins: acknowledged by the
    /// program and left out of memory loaded at a handoff. Positions and
    /// counts stay absolute, so this memory and the full log decide alike.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub completed_base: u64,
    /// Admitted calls left out of `calls` the same way; all had outcomes.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub calls_dropped: u64,
    /// The highest program request number (`program_request_number`) among
    /// the calls left out. A missing request at or below it was admitted and
    /// acknowledged, so sending it again is refused rather than run twice.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub dropped_max_request: u64,
    /// Completions the scope's program has acknowledged: it had collected
    /// them when the workflow task it sent this from began, so its replays
    /// never ask for them again and never re-send a call issued before
    /// them. The highest acknowledgement received.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub acknowledged: u64,
    /// The program context a run-owned scope's program kept, passed when
    /// the scope closed. It is written only if the run completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program_context: Option<crate::ProgramContextInput>,
    /// Calls whose cancellation was requested one by one. A cancel is a
    /// request, not an outcome: each call still records exactly one outcome,
    /// which may be its result if that came first.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub cancel_requested_calls: BTreeSet<String>,
    /// The program run's handles this scope's cancelling close withdrew.
    /// They are not calls; the exit waits until each has settled.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub withdrawn: BTreeSet<crate::PromiseId>,
}

impl CodeToolScope {
    /// Recorded outcomes after the first `after` completions, in log order;
    /// `None` when some of them were left out of memory (before
    /// `completed_base`).
    pub fn recorded_outcomes(&self, after: u64, limit: usize) -> Option<Vec<&CodeToolCall>> {
        let skip = after.checked_sub(self.completed_base)?;
        Some(
            self.completed
                .iter()
                .skip(usize::try_from(skip).unwrap_or(usize::MAX))
                .take(limit)
                .filter_map(|request_id| self.calls.get(request_id))
                .collect(),
        )
    }

    /// Calls ever admitted to the scope, including those left out of memory.
    pub fn admitted(&self) -> u64 {
        self.calls_dropped
            .saturating_add(u64::try_from(self.calls.len()).unwrap_or(u64::MAX))
    }

    /// Completions ever recorded in the scope, including those left out of
    /// memory.
    pub fn recorded(&self) -> u64 {
        self.completed_base
            .saturating_add(u64::try_from(self.completed.len()).unwrap_or(u64::MAX))
    }

    /// Whether `request_id` names a call left out of memory: admitted,
    /// finished and acknowledged.
    pub fn left_out(&self, request_id: &str) -> bool {
        !self.calls.contains_key(request_id)
            && program_request_number(request_id)
                .is_some_and(|number| number <= self.dropped_max_request)
    }

    /// Leave out the finished calls the scope's program acknowledged: the
    /// first `acknowledged` completions. Counts and positions stay absolute.
    /// Only calls with program request ids are left out, in completion
    /// order, so `left_out` can tell them from requests never admitted.
    /// Returns the calls left out.
    pub(crate) fn leave_out_acknowledged(&mut self) -> Vec<CodeToolCall> {
        let acknowledged = self.acknowledged.saturating_sub(self.completed_base);
        let acknowledged = usize::try_from(acknowledged)
            .unwrap_or(usize::MAX)
            .min(self.completed.len());
        let numbered = self.completed[..acknowledged]
            .iter()
            .take_while(|request_id| program_request_number(request_id).is_some())
            .count();
        let mut left_out = Vec::with_capacity(numbered);
        for request_id in self.completed.drain(..numbered) {
            if let Some(call) = self.calls.remove(&request_id) {
                debug_assert!(call.status.is_terminal());
                left_out.push(call);
            }
            self.cancel_requested_calls.remove(&request_id);
            self.calls_dropped += 1;
            self.completed_base += 1;
            self.dropped_max_request = self
                .dropped_max_request
                .max(program_request_number(&request_id).expect("numbered above"));
        }
        left_out
    }
}

/// The number in a program's request id, `call-<n>`: the engine numbers a
/// scope's calls from 1 in the order the program issues them, which is the
/// order they are admitted in. Other ids (code mode's) have none.
pub fn program_request_number(request_id: &str) -> Option<u64> {
    let number = request_id.strip_prefix("call-")?;
    let parsed = number.parse::<u64>().ok()?;
    (parsed > 0 && parsed.to_string() == number).then_some(parsed)
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// A call left out of memory was admitted and finished; admitting its
/// request again would run it twice.
fn refuse_left_out(state: &CoreAgentState, origin: &CodeToolOrigin) -> Result<(), DomainError> {
    if state
        .code_tools
        .scopes
        .get(&origin.execution_id)
        .is_some_and(|scope| scope.left_out(&origin.request_id))
    {
        return Err(invalid(
            "code tool request was admitted and acknowledged; it cannot be admitted again",
        ));
    }
    Ok(())
}

/// An acknowledgement can cover only completions the scope has recorded.
fn validate_acknowledgement(scope: &CodeToolScope, acknowledged: u64) -> Result<(), DomainError> {
    if acknowledged > scope.recorded() {
        return Err(invalid(
            "acknowledgement covers completions the scope has not recorded",
        ));
    }
    Ok(())
}

fn acknowledge(scope: &mut CodeToolScope, acknowledged: Option<u64>) -> Result<(), DomainError> {
    if let Some(acknowledged) = acknowledged {
        validate_acknowledgement(scope, acknowledged)?;
        scope.acknowledged = scope.acknowledged.max(acknowledged);
    }
    Ok(())
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeToolState {
    pub scopes: BTreeMap<String, CodeToolScope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodeToolEvent {
    ScopeOpened {
        scope: CodeToolScopeSpec,
    },
    CallAdmitted {
        call: CodeToolCallSpec,
        promise_id_base: u64,
        /// The program's acknowledgement sent with the call
        /// (`CodeToolScope::acknowledged`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        acknowledged: Option<u64>,
    },
    CallCompleted {
        origin: CodeToolOrigin,
        result: CodeToolCallResult,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        completed_at_ms: Option<u64>,
    },
    CallDeferred {
        origin: CodeToolOrigin,
        suspension: ToolBatchSuspension,
    },
    ScopeClosed {
        execution_id: String,
        cancel: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        program_context: Option<crate::ProgramContextInput>,
        /// A program run's open handles (its pending model-owned run
        /// promises) the cancelling close withdrew. The run's exit waits for
        /// each to settle, like a call.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        withdrawn: Vec<crate::PromiseId>,
        /// The program's acknowledgement sent with the close
        /// (`CodeToolScope::acknowledged`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        acknowledged: Option<u64>,
    },
    /// The scope's owner asked to cancel one call. The call stops the way
    /// its kind can and still records one outcome.
    CallCancelRequested {
        origin: CodeToolOrigin,
    },
}

/// What a scope's owner asks to cancel: one of its calls, or a promise it
/// holds a handle to (an opened input request).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CodeToolCancelTarget {
    Call { request_id: String },
    Promise { promise_id: crate::PromiseId },
}

fn invalid(message: impl Into<String>) -> DomainError {
    DomainError::InvariantViolation(message.into())
}

fn validate_id(kind: &'static str, value: &str) -> Result<(), DomainError> {
    if value.len() > 256 {
        return Err(invalid(format!("code tool {kind} exceeds 256 bytes")));
    }
    crate::validate_general_string_id(kind, value).map_err(|error| invalid(error.to_string()))
}

pub fn code_tool_call_id(origin: &CodeToolOrigin) -> ToolCallId {
    let bytes = serde_json::to_vec(&(origin.execution_id.as_str(), origin.request_id.as_str()))
        .expect("string pair is serializable");
    ToolCallId::new(format!("code-tool:{}", hex::encode(Sha256::digest(bytes))))
}

/// Resolve the owner facts shared by every scope kind.
pub fn scope_owner(
    state: &CoreAgentState,
    scope: &CodeToolScopeSpec,
) -> Result<OwnerView, DomainError> {
    let facts = owner_facts(state, scope)?;
    Ok(OwnerView {
        run_id: facts.run_id,
        model_batch: facts
            .dispatch
            .model_batch()
            .map(|(turn_id, tool_batch_id)| ModelBatch {
                turn_id,
                tool_batch_id,
            }),
    })
}

/// What the call machinery needs to know about a scope's owner, whichever
/// kind it is.
pub(crate) struct OwnerFacts<'a> {
    pub run_id: RunId,
    /// The session the calls are made as.
    pub session_id: &'a SessionId,
    pub session_universe_id: uuid::Uuid,
    /// How the scope's calls are dispatched and what their invocations join.
    pub dispatch: crate::DispatchContext,
    /// The operation that owns the scope; a scope may never call it.
    pub owner_tool: &'a ToolName,
}

pub(crate) fn owner_facts<'a>(
    state: &'a CoreAgentState,
    scope: &CodeToolScopeSpec,
) -> Result<OwnerFacts<'a>, DomainError> {
    let (invocation, dispatch) = match &scope.owner {
        ScopeOwner::ModelInvocation { .. } => {
            let parent = code_tool_parent(state, scope)?;
            let batch = parent_batch(parent)?;
            (
                parent,
                crate::DispatchContext::ModelBatch {
                    turn_id: batch.turn_id,
                    batch_id: batch.tool_batch_id,
                },
            )
        }
        ScopeOwner::Run { run_id } => (
            super::program::program_run_invocation(state, *run_id)
                .ok_or_else(|| invalid("run-owned scope has no program execution"))?,
            crate::DispatchContext::Scope {
                scope_id: scope.execution_id.clone(),
            },
        ),
    };
    let owner_tool = &state
        .workflow_tools
        .bindings
        .get(&invocation.tool_id)
        .ok_or_else(|| invalid("scope owner binding is missing"))?
        .definition
        .tool
        .name;
    Ok(OwnerFacts {
        run_id: invocation.run_id,
        session_id: &invocation.session_id,
        session_universe_id: invocation.session_universe_id,
        dispatch,
        owner_tool,
    })
}

/// The model-batch call of a scope's parent invocation. A model-invocation
/// scope is only ever opened beneath a model call.
pub(crate) fn parent_batch(
    parent: &WorkflowToolInvocation,
) -> Result<&ModelBatchCall, DomainError> {
    parent
        .model_batch()
        .ok_or_else(|| invalid("code tool parent is not a model-batch call"))
}

/// The parked workflow-tool call that owns a model-invocation scope. Use
/// [`scope_owner`] unless the parent invocation itself is needed.
pub fn code_tool_parent<'a>(
    state: &'a CoreAgentState,
    scope: &CodeToolScopeSpec,
) -> Result<&'a WorkflowToolInvocation, DomainError> {
    let ScopeOwner::ModelInvocation {
        parent_invocation_id,
    } = &scope.owner
    else {
        return Err(invalid("scope is not owned by a model invocation"));
    };
    state
        .workflow_tools
        .emissions
        .get(parent_invocation_id)
        .or_else(|| {
            state
                .workflow_tools
                .start_requests
                .get(parent_invocation_id)
        })
        .ok_or_else(|| invalid("code tool scope references unknown parent workflow invocation"))
}

pub fn code_tool_call<'a>(
    state: &'a CoreAgentState,
    origin: &CodeToolOrigin,
) -> Option<&'a CodeToolCall> {
    state
        .code_tools
        .scopes
        .get(&origin.execution_id)?
        .calls
        .get(&origin.request_id)
}

pub fn code_tool_call_for_id<'a>(
    state: &'a CoreAgentState,
    call_id: &ToolCallId,
) -> Option<(&'a CodeToolScope, &'a CodeToolCall)> {
    state.code_tools.scopes.values().find_map(|scope| {
        scope
            .calls
            .values()
            .find(|call| &call.call_id == call_id)
            .map(|call| (scope, call))
    })
}

pub fn code_tool_scope_is_live(state: &CoreAgentState, scope: &CodeToolScopeSpec) -> bool {
    validate_live_owner(state, scope).is_ok()
}

/// Each owner kind defines its own liveness.
fn validate_live_owner(
    state: &CoreAgentState,
    scope: &CodeToolScopeSpec,
) -> Result<(), DomainError> {
    match &scope.owner {
        ScopeOwner::ModelInvocation { .. } => validate_live_parent(state, scope),
        ScopeOwner::Run { run_id } => validate_live_run(state, scope, *run_id),
    }
}

/// A run-owned scope lives while its program run is active and its program
/// execution is still pending; each execution of the run has its own scope
/// (`program_scope_id`). Cancelling the run ends liveness, which makes the
/// session close the scope and drain its calls before the run ends.
fn validate_live_run(
    state: &CoreAgentState,
    scope: &CodeToolScopeSpec,
    run_id: RunId,
) -> Result<(), DomainError> {
    if !crate::is_program_session(state) {
        return Err(invalid("run-owned scopes require a program session"));
    }
    let run = state
        .runs
        .active
        .as_ref()
        .filter(|run| run.run_id == run_id)
        .ok_or_else(|| invalid("run-owned scope run is not active"))?;
    if !matches!(run.status, RunStatus::Active | RunStatus::Parked) {
        return Err(invalid("run-owned scope run is not accepting calls"));
    }
    let execution_id = super::program::program_run_execution_id(state, run_id)
        .ok_or_else(|| invalid("run-owned scope has no pending program execution"))?;
    if crate::program_scope_execution(&scope.execution_id).0 != execution_id {
        return Err(invalid(
            "run-owned scope id does not match the run's program execution",
        ));
    }
    Ok(())
}

/// A model-invocation scope lives while its parent joined call is pending in
/// the parked batch.
fn validate_live_parent(
    state: &CoreAgentState,
    scope: &CodeToolScopeSpec,
) -> Result<(), DomainError> {
    let parent = code_tool_parent(state, scope)?;
    let run = state
        .runs
        .active
        .as_ref()
        .ok_or_else(|| invalid("code tool calls require an active run"))?;
    if run.run_id != parent.run_id || !matches!(run.status, RunStatus::Active | RunStatus::Parked) {
        return Err(invalid("code tool parent run is not accepting calls"));
    }
    let parked = run
        .parked_tool_batch
        .as_ref()
        .ok_or_else(|| invalid("code tool parent is not parked"))?;
    let ToolBatchSuspension::JoinedWorkflowCalls { calls, .. } = &parked.suspension else {
        return Err(invalid("code tool parent must be a joined workflow call"));
    };
    let batch = parent_batch(parent)?;
    if parked.batch_id != batch.tool_batch_id
        || !calls.iter().any(|call| {
            call.invocation_id == parent.invocation_id
                && call.call_id == batch.tool_call_id
                && state
                    .promises
                    .promises
                    .get(&call.promise_id)
                    .is_some_and(|promise| promise.status == PromiseStatus::Pending)
        })
    {
        return Err(invalid("code tool parent joined call is no longer pending"));
    }
    Ok(())
}

fn validate_scope(state: &CoreAgentState, scope: &CodeToolScopeSpec) -> Result<(), DomainError> {
    validate_id("execution_id", &scope.execution_id)?;
    validate_live_owner(state, scope)?;
    let limits = scope.policy.limits;
    if limits.max_calls == 0
        || limits.max_calls > max_calls_per_scope(&scope.owner)
        || limits.max_in_flight == 0
        || limits.max_in_flight > MAX_CODE_TOOL_IN_FLIGHT
        || limits.max_in_flight > limits.max_calls
        || scope.bindings.len() > 4096
    {
        return Err(invalid(
            "code tool scope limits are outside their bounded ranges",
        ));
    }
    // A scope may not call the operation that owns it.
    let facts = owner_facts(state, scope)?;
    let owner_tool = facts.owner_tool;
    for (binding_id, binding) in &scope.bindings {
        validate_id("binding_id", binding_id)?;
        match (scope.policy.binding_mode, &binding.contract) {
            (BindingMode::Presentation, None) => {}
            (BindingMode::Contract, Some(contract)) => contract.validate()?,
            _ => {
                return Err(invalid(
                    "code tool binding does not match the scope's binding mode",
                ));
            }
        }
        let tool_id = &binding.tool_id;
        let tool = state
            .tooling
            .tools
            .get(tool_id)
            .ok_or_else(|| invalid(format!("code tool {tool_id} is not granted")))?;
        if !tool.invokes_client_effect() || tool_id == owner_tool {
            return Err(invalid(format!(
                "tool {tool_id} is not available for code tool execution"
            )));
        }
    }
    let run_id = facts.run_id;
    let count = state
        .code_tools
        .scopes
        .values()
        .filter(|existing| {
            scope_owner(state, &existing.spec).is_ok_and(|existing| existing.run_id == run_id)
        })
        .count();
    if count >= MAX_CODE_TOOL_SCOPES_PER_RUN {
        return Err(invalid("code tool scope count exceeds the per-run limit"));
    }
    validate_owner_free(state, scope)
}

/// The most calls a scope may admit. A program run's scope may admit what
/// the run may make; its program counts the run's calls across executions.
pub fn max_calls_per_scope(owner: &ScopeOwner) -> u32 {
    match owner {
        ScopeOwner::ModelInvocation { .. } => MAX_CODE_TOOL_CALLS_PER_SCOPE,
        ScopeOwner::Run { .. } => crate::MAX_PROGRAM_CALLS,
    }
}

/// The scope of the previous execution of the program run whose later
/// execution `scope` belongs to, if it is one.
fn previous_execution_scope(scope: &CodeToolScopeSpec) -> Option<String> {
    let ScopeOwner::Run { .. } = scope.owner else {
        return None;
    };
    match crate::program_scope_execution(&scope.execution_id) {
        (execution_id, execution) if execution > 0 => {
            Some(crate::program_scope_id(execution_id, execution - 1))
        }
        _ => None,
    }
}

/// An owner holds one scope at a time. A program run that continues as new
/// opens its next execution's scope only after the previous execution's has
/// closed and settled: every call recorded, every withdrawn handle settled.
fn validate_owner_free(
    state: &CoreAgentState,
    scope: &CodeToolScopeSpec,
) -> Result<(), DomainError> {
    let mut owned = state
        .code_tools
        .scopes
        .values()
        .filter(|existing| existing.spec.owner == scope.owner);
    let Some(previous) = previous_execution_scope(scope) else {
        return if owned.next().is_none() {
            Ok(())
        } else {
            Err(invalid("scope owner already owns a code tool scope"))
        };
    };
    let previous = state
        .code_tools
        .scopes
        .get(&previous)
        .filter(|previous| previous.spec.owner == scope.owner)
        .ok_or_else(|| invalid("a program execution's scope must follow the previous one's"))?;
    if owned.any(|existing| !existing.closed) {
        return Err(invalid("the run's previous program scope is still open"));
    }
    let (withdrawn, settled) = code_tool_scope_withdrawals(state, previous);
    if settled < withdrawn
        || previous
            .calls
            .values()
            .any(|call| !call.status.is_terminal())
    {
        return Err(invalid("the run's previous program scope has not settled"));
    }
    Ok(())
}

fn validate_call(state: &CoreAgentState, call: &CodeToolCallSpec) -> Result<(), DomainError> {
    validate_id("request_id", &call.origin.request_id)?;
    let scope = state
        .code_tools
        .scopes
        .get(&call.origin.execution_id)
        .ok_or_else(|| invalid("unknown code tool execution scope"))?;
    if scope.closed {
        return Err(invalid("code tool execution scope is closed"));
    }
    validate_live_owner(state, &scope.spec)?;
    validate_bindings_current(state, scope)?;
    if !scope
        .spec
        .bindings
        .get(&call.binding_id)
        .is_some_and(|binding| {
            binding.tool_id == call.tool_id && binding.tool_name == call.tool_name
        })
        || !state
            .tooling
            .tools
            .get(&call.tool_id)
            .is_some_and(|tool| tool.invokes_client_effect())
    {
        return Err(invalid("code tool is outside the execution grant"));
    }
    let limits = scope.spec.policy.limits;
    if scope.admitted() >= u64::from(limits.max_calls) {
        return Err(invalid("code tool execution exhausted its call budget"));
    }
    if scope
        .calls
        .values()
        .filter(|call| !call.status.is_terminal())
        .count()
        >= limits.max_in_flight as usize
    {
        return Err(invalid("code tool execution exceeded its in-flight limit"));
    }
    if let Some(binding) = state.workflow_tools.binding_for_tool_name(&call.tool_id) {
        let run_id = scope_owner(state, &scope.spec)?.run_id;
        let pending = state
            .code_tools
            .scopes
            .values()
            .filter(|scope| {
                scope_owner(state, &scope.spec).is_ok_and(|owner| owner.run_id == run_id)
            })
            .flat_map(|scope| scope.calls.values())
            .filter(|other| {
                other.spec.tool_id == call.tool_id
                    && matches!(other.status, CodeToolCallStatus::Pending)
            })
            .count();
        if state
            .workflow_tools
            .emission_count(run_id, &binding.definition.tool_id) as usize
            + pending
            >= crate::MAX_WORKFLOW_TOOL_EMISSIONS_PER_RUN as usize
        {
            return Err(invalid(
                "workflow tool invoked from code exhausted its emission budget",
            ));
        }
    }
    let call_id = code_tool_call_id(&call.origin);
    if state.runs.active.as_ref().is_some_and(|run| {
        run.tool_batches
            .values()
            .any(|batch| batch.calls.iter().any(|call| call.call.call_id == call_id))
    }) {
        return Err(invalid(
            "code tool call identity collides with a model call",
        ));
    }
    Ok(())
}

/// The policy's revalidation rule, applied before a call is admitted or
/// dispatched.
fn validate_bindings_current(
    state: &CoreAgentState,
    scope: &CodeToolScope,
) -> Result<(), DomainError> {
    match scope.spec.policy.revalidation {
        Revalidation::ToolsetRevision if scope.toolset_revision != state.tooling.revision => Err(
            invalid("code tool scope catalog is stale after a toolset change"),
        ),
        Revalidation::ToolsetRevision => Ok(()),
    }
}

/// The approval binding a scoped call waits through when its scope parks
/// approval-required calls: only a remote MCP call, the one kind of call
/// that can need approval, in a `Park` scope of a session that binds it.
pub(crate) fn approval_binding_for_call<'a>(
    state: &'a CoreAgentState,
    scope: &CodeToolScope,
    call: &CodeToolCall,
) -> Option<&'a crate::WorkflowToolBinding> {
    if scope.spec.policy.approvals != ApprovalPolicy::Park
        || !state
            .tooling
            .tools
            .get(&call.spec.tool_id)
            .is_some_and(|tool| matches!(tool.kind, crate::ToolKind::RemoteMcp(_)))
    {
        return None;
    }
    state
        .workflow_tools
        .bindings
        .get(&crate::WorkflowToolId::new(
            crate::APPROVAL_WORKFLOW_TOOL_ID,
        ))
}

/// The joined workflow binding a scoped call waits through: its own tool's,
/// or the approval binding for a call its scope parks.
fn joined_binding_for_call<'a>(
    state: &'a CoreAgentState,
    scope: &CodeToolScope,
    call: &CodeToolCall,
) -> Option<&'a crate::WorkflowToolBinding> {
    state
        .workflow_tools
        .binding_for_tool_name(&call.spec.tool_id)
        .or_else(|| approval_binding_for_call(state, scope, call))
        .filter(|binding| matches!(binding.completion, WorkflowToolCompletion::Joined { .. }))
}

fn joins(state: &CoreAgentState, origin: &CodeToolOrigin) -> Result<CoreAgentJoins, DomainError> {
    let scope = state
        .code_tools
        .scopes
        .get(&origin.execution_id)
        .ok_or_else(|| invalid("unknown code tool execution scope"))?;
    let owner = scope_owner(state, &scope.spec)?;
    Ok(CoreAgentJoins {
        run_id: Some(owner.run_id),
        turn_id: owner.model_batch.map(|batch| batch.turn_id),
        tool_batch_id: owner.model_batch.map(|batch| batch.tool_batch_id),
        tool_call_id: Some(code_tool_call_id(origin)),
        ..Default::default()
    })
}

pub fn open_code_tool_scope_proposals(
    state: &CoreAgentState,
    scope: CodeToolScopeSpec,
) -> Result<Vec<CoreAgentEventProposal>, DomainError> {
    if let Some(existing) = state.code_tools.scopes.get(&scope.execution_id) {
        return if existing.spec == scope {
            Ok(Vec::new())
        } else {
            Err(invalid(
                "code tool execution id reused with a different scope",
            ))
        };
    }
    validate_scope(state, &scope)?;
    Ok(vec![CoreAgentEventProposal::new(
        CoreAgentJoins::default(),
        CoreAgentEvent::CodeTool(CodeToolEvent::ScopeOpened { scope }),
    )])
}

pub fn admit_code_tool_call_proposals(
    state: &CoreAgentState,
    call: CodeToolCallSpec,
    acknowledged: Option<u64>,
) -> Result<Vec<CoreAgentEventProposal>, DomainError> {
    if let Some(existing) = code_tool_call(state, &call.origin) {
        return if existing.spec == call {
            Ok(Vec::new())
        } else {
            Err(invalid(
                "code tool request id reused with different arguments or binding",
            ))
        };
    }
    refuse_left_out(state, &call.origin)?;
    validate_call(state, &call)?;
    if let Some(acknowledged) = acknowledged {
        validate_acknowledgement(
            &state.code_tools.scopes[&call.origin.execution_id],
            acknowledged,
        )?;
    }
    let promise_id_base = state
        .id_cursors
        .last_promise_id
        .checked_add(1)
        .ok_or_else(|| invalid("promise counter exhausted"))?;
    promise_id_base
        .checked_add(CODE_TOOL_PROMISE_SLOTS - 1)
        .ok_or_else(|| invalid("promise counter exhausted"))?;
    Ok(vec![CoreAgentEventProposal::new(
        joins(state, &call.origin)?,
        CoreAgentEvent::CodeTool(CodeToolEvent::CallAdmitted {
            call,
            promise_id_base,
            acknowledged,
        }),
    )])
}

pub fn complete_code_tool_call_proposals(
    state: &CoreAgentState,
    origin: CodeToolOrigin,
    result: ToolInvocationResult,
) -> Result<Vec<CoreAgentEventProposal>, DomainError> {
    let call =
        code_tool_call(state, &origin).ok_or_else(|| invalid("unknown code tool request"))?;
    validate_result(call, &result)?;
    // A joined preparation outcome records a wait, not its acknowledgement.
    // Redelivery of that same admitted effect is still idempotent, including
    // after the reply has completed the call.
    if !matches!(call.status, CodeToolCallStatus::Pending)
        && result.status == ToolCallStatus::Succeeded
        && result.effects.len() == 1
        && joined_binding_for_call(state, &state.code_tools.scopes[&origin.execution_id], call)
            .is_some()
        && let Some(invocation) =
            super::workflow_tool::invocation_from_emit_effect(&result.effects[0])?
        && invocation.tool_call_id() == Some(&call.call_id)
        && state
            .workflow_tools
            .emissions
            .get(&invocation.invocation_id)
            .or_else(|| {
                state
                    .workflow_tools
                    .start_requests
                    .get(&invocation.invocation_id)
            })
            == Some(&invocation)
    {
        let deadline =
            super::workflow_tool::completion_deadline_from_emit_effect(&result.effects[0])?;
        if invocation
            .completion_promises
            .as_ref()
            .is_some_and(|promises| {
                promises.values().all(|id| {
                    state
                        .promises
                        .promises
                        .get(id)
                        .is_some_and(|promise| promise.deadline_ms == deadline)
                })
            })
        {
            return Ok(Vec::new());
        }
    }
    if let CodeToolCallStatus::Completed { result: existing } = &call.status {
        return if existing.matches_invocation_result(&result) {
            Ok(Vec::new())
        } else {
            Err(invalid(
                "code tool completion conflicts with the recorded outcome",
            ))
        };
    }
    if !matches!(call.status, CodeToolCallStatus::Pending) {
        return Err(invalid("code tool request is already waiting"));
    }
    validate_result(call, &result)?;
    let scope = &state.code_tools.scopes[&origin.execution_id];
    let facts = owner_facts(state, &scope.spec)?;
    crate::core::drive::code_tool_result_proposals(
        state,
        facts.session_id,
        origin,
        ToolInvocationBatchResult {
            run_id: facts.run_id,
            context: facts.dispatch,
            results: vec![result],
        },
    )
}

fn validate_result(call: &CodeToolCall, result: &ToolInvocationResult) -> Result<(), DomainError> {
    validate_result_fields(call, &result.call_id, result.status)
}

fn validate_result_fields(
    call: &CodeToolCall,
    call_id: &ToolCallId,
    status: ToolCallStatus,
) -> Result<(), DomainError> {
    if call_id != &call.call_id
        || !matches!(
            status,
            ToolCallStatus::Succeeded | ToolCallStatus::Failed | ToolCallStatus::Cancelled
        )
    {
        return Err(invalid(
            "code tool result does not match its admitted call or is not terminal",
        ));
    }
    Ok(())
}

pub fn defer_code_tool_call_proposals(
    state: &CoreAgentState,
    origin: CodeToolOrigin,
    spec: AwaitSpec,
) -> Result<Vec<CoreAgentEventProposal>, DomainError> {
    let call =
        code_tool_call(state, &origin).ok_or_else(|| invalid("unknown code tool request"))?;
    match call.spec.tool_id.as_str() {
        crate::AWAIT_TOOL_ID => {
            if spec.promise_ids.is_empty()
                || spec.promise_ids.len() > 32
                || spec.promise_ids.iter().collect::<BTreeSet<_>>().len() != spec.promise_ids.len()
            {
                return Err(invalid(
                    "code tool await requires 1..=32 distinct promise ids",
                ));
            }
            let owner = scope_owner(state, &state.code_tools.scopes[&origin.execution_id].spec)?;
            crate::core::drive::validate_await_spec_for_active_run(state, owner.run_id, &spec)?;
        }
        // A sleep waits for its deadline alone.
        crate::TIME_SLEEP_TOOL_ID => {
            if !spec.promise_ids.is_empty() || spec.deadline_at_ms.is_none() {
                return Err(invalid("a sleep waits on a deadline and no promises"));
            }
        }
        _ => {
            return Err(invalid(
                "code tool suspension requires concurrency.await or time.sleep",
            ));
        }
    }
    let suspension = ToolBatchSuspension::AwaitTool {
        call_id: call.call_id.clone(),
        spec,
    };
    if let CodeToolCallStatus::Waiting {
        suspension: existing,
    } = &call.status
    {
        return if existing == &suspension {
            Ok(Vec::new())
        } else {
            Err(invalid("code tool await changed after admission"))
        };
    }
    if !matches!(call.status, CodeToolCallStatus::Pending) {
        return Err(invalid("code tool request already completed"));
    }
    Ok(vec![CoreAgentEventProposal::new(
        joins(state, &origin)?,
        CoreAgentEvent::CodeTool(CodeToolEvent::CallDeferred { origin, suspension }),
    )])
}

/// Whether a waiting call is held until its joined reply's producer confirms
/// a requested cancellation. Neither a cancel nor its deadline wakes it
/// again: the reply's settlement does, with the outcome the producer
/// recorded first.
pub fn code_tool_awaits_cancel_confirmation(state: &CoreAgentState, call: &CodeToolCall) -> bool {
    let CodeToolCallStatus::Waiting {
        suspension: ToolBatchSuspension::JoinedWorkflowCalls { calls, .. },
    } = &call.status
    else {
        return false;
    };
    calls
        .iter()
        .any(|joined| state.promises.cancel_requested.contains(&joined.promise_id))
}

pub fn code_tool_wake(
    state: &CoreAgentState,
    origin: &CodeToolOrigin,
    now_ms: u64,
) -> Option<WakeReason> {
    let scope = state.code_tools.scopes.get(&origin.execution_id)?;
    let call = scope.calls.get(&origin.request_id)?;
    let CodeToolCallStatus::Waiting { suspension } = &call.status else {
        return None;
    };
    if code_tool_awaits_cancel_confirmation(state, call) {
        return None;
    }
    if code_tool_call_cancel_requested(state, origin)
        || validate_live_owner(state, &scope.spec).is_err()
    {
        return Some(WakeReason::Cancelled);
    }
    let spec = suspension.spec();
    if spec
        .deadline_at_ms
        .is_some_and(|deadline| deadline <= now_ms)
    {
        return Some(WakeReason::Timeout);
    }
    let terminal = spec
        .promise_ids
        .iter()
        .filter(|id| {
            state
                .promises
                .promises
                .get(*id)
                .is_some_and(|promise| promise.status.is_terminal())
        })
        .count();
    match spec.mode {
        // A wait with no promises (a sleep) wakes only at its deadline.
        _ if spec.promise_ids.is_empty() => None,
        AwaitMode::All if terminal == spec.promise_ids.len() => Some(WakeReason::Terminal),
        AwaitMode::Any if terminal > 0 => Some(WakeReason::Terminal),
        _ => None,
    }
}

/// Project one joined reply without disturbing the outer model batch.
pub fn code_tool_joined_result(
    state: &CoreAgentState,
    origin: &CodeToolOrigin,
    cancel_pending: bool,
) -> Result<ToolInvocationResult, DomainError> {
    let call = code_tool_call(state, origin).ok_or_else(|| invalid("unknown code tool request"))?;
    let CodeToolCallStatus::Waiting {
        suspension: ToolBatchSuspension::JoinedWorkflowCalls { calls, .. },
    } = &call.status
    else {
        return Err(invalid(
            "code tool request is not waiting for a joined reply",
        ));
    };
    let promise = state
        .promises
        .promises
        .get(&calls[0].promise_id)
        .ok_or_else(|| invalid("code tool joined promise is missing"))?;
    let (status, output_ref, error_ref) = match promise.status {
        PromiseStatus::Resolved => (ToolCallStatus::Succeeded, promise.payload_ref.clone(), None),
        PromiseStatus::Failed => (
            ToolCallStatus::Failed,
            None,
            Some(
                promise
                    .error_ref
                    .clone()
                    .unwrap_or_else(crate::unavailable_tool_result_ref),
            ),
        ),
        PromiseStatus::Cancelled => (
            ToolCallStatus::Cancelled,
            None,
            Some(crate::cancelled_tool_result_ref()),
        ),
        PromiseStatus::Pending if cancel_pending => (
            ToolCallStatus::Cancelled,
            None,
            Some(crate::cancelled_tool_result_ref()),
        ),
        PromiseStatus::Pending => return Err(invalid("code tool joined promise is still pending")),
    };
    Ok(ToolInvocationResult {
        call_id: call.call_id.clone(),
        status,
        output_ref,
        error_ref,
        model_visible_context_entries: Vec::new(),
        effects: Vec::new(),
        attachments: Vec::new(),
        duration_ms: None,
        output_bytes: None,
        truncated: false,
    })
}

pub fn resume_code_tool_call_proposals(
    state: &CoreAgentState,
    origin: CodeToolOrigin,
    result: ToolInvocationResult,
    claim_observed_at_ms: u64,
    observed_at_ms: u64,
) -> Result<Vec<CoreAgentEventProposal>, DomainError> {
    let call =
        code_tool_call(state, &origin).ok_or_else(|| invalid("unknown code tool request"))?;
    if let CodeToolCallStatus::Completed { result: existing } = &call.status {
        return if existing.matches_invocation_result(&result) {
            Ok(Vec::new())
        } else {
            Err(invalid("code tool resume conflicts with recorded result"))
        };
    }
    validate_result(call, &result)?;
    let wake = code_tool_wake(state, &origin, claim_observed_at_ms);
    if claim_observed_at_ms > observed_at_ms || wake.is_none() {
        return Err(invalid("code tool resume has no satisfied wake"));
    }
    let mut proposals = Vec::new();
    if !result.effects.is_empty() {
        return Err(invalid("code tool resume cannot introduce tool effects"));
    }
    if let CodeToolCallStatus::Waiting {
        suspension: ToolBatchSuspension::JoinedWorkflowCalls { calls, .. },
    } = &call.status
    {
        let promise = &state.promises.promises[&calls[0].promise_id];
        let expected_status = match promise.status {
            PromiseStatus::Resolved => ToolCallStatus::Succeeded,
            PromiseStatus::Failed => ToolCallStatus::Failed,
            PromiseStatus::Pending | PromiseStatus::Cancelled => ToolCallStatus::Cancelled,
        };
        if result.status != expected_status
            || (promise.status == PromiseStatus::Resolved
                && result.output_ref != promise.payload_ref)
        {
            return Err(invalid(
                "code tool joined result does not match its completion promise",
            ));
        }
        // Scope closure can precede a still-running workflow preparation.
        // Its late effect creates the reply promise after close had nothing
        // to cancel. Settling that wait must emit the usual cancellation fact
        // so the receiver/owned workflow is cancelled as well as the waiter.
        // Already terminal replies remain authoritative; model-owned submitted
        // promises belong to separate acknowledged calls and are untouched.
        // A reply whose producer confirms cancellations is never cancelled
        // here: the session requests the cancellation first
        // (`RequestPromiseCancel`) and resumes the call once it settles.
        if matches!(wake, Some(WakeReason::Cancelled | WakeReason::Timeout)) {
            for joined in calls {
                if !state
                    .promises
                    .promises
                    .get(&joined.promise_id)
                    .is_some_and(|promise| promise.ownership == PromiseOwnership::Runtime)
                {
                    continue;
                }
                match crate::promise_cancel_event(state, &joined.promise_id) {
                    Some(event @ PromiseEvent::Cancelled { .. }) => {
                        proposals.push(CoreAgentEventProposal::new(
                            joins(state, &origin)?,
                            CoreAgentEvent::Promise(event),
                        ));
                    }
                    Some(_) => {
                        return Err(invalid(
                            "code tool joined reply must have its cancellation confirmed first",
                        ));
                    }
                    None => {}
                }
            }
        }
    }
    proposals.push(CoreAgentEventProposal::new(
        joins(state, &origin)?,
        CoreAgentEvent::CodeTool(CodeToolEvent::CallCompleted {
            origin,
            result: result.into(),
            completed_at_ms: None,
        }),
    ));
    Ok(proposals)
}

/// Stamp the scoped-call completions among `proposals` with the time the
/// session observed the command that produced them.
pub(crate) fn stamp_code_tool_completions(
    proposals: &mut [CoreAgentEventProposal],
    observed_at_ms: u64,
) {
    for proposal in proposals {
        if let CoreAgentEvent::CodeTool(CodeToolEvent::CallCompleted {
            completed_at_ms, ..
        }) = &mut proposal.event
        {
            *completed_at_ms = Some(observed_at_ms);
        }
    }
}

/// The handles a program scope's close withdrew, and how many of them have
/// settled (answered, failed or withdrawn). The run's exit waits for all.
pub fn code_tool_scope_withdrawals(state: &CoreAgentState, scope: &CodeToolScope) -> (u64, u64) {
    let settled = scope
        .withdrawn
        .iter()
        .filter(|promise_id| {
            state
                .promises
                .promises
                .get(*promise_id)
                .is_none_or(|promise| promise.status.is_terminal())
        })
        .count();
    (scope.withdrawn.len() as u64, settled as u64)
}

/// Whether a call's cancellation was requested, by its own cancel or by its
/// scope's.
pub fn code_tool_call_cancel_requested(state: &CoreAgentState, origin: &CodeToolOrigin) -> bool {
    state
        .code_tools
        .scopes
        .get(&origin.execution_id)
        .is_some_and(|scope| {
            scope.cancel_requested || scope.cancel_requested_calls.contains(&origin.request_id)
        })
}

/// Cancel one call of a scope, or a promise its owner holds a handle to.
/// Resending is a no-op: a settled target, or one whose cancellation was
/// already requested, needs nothing more. Every cancelled call still
/// records one outcome; a promise is cancelled the way its producer needs
/// (`promise_cancel_event`).
pub fn cancel_code_tool_proposals(
    state: &CoreAgentState,
    execution_id: &str,
    target: &CodeToolCancelTarget,
) -> Result<Vec<CoreAgentEventProposal>, DomainError> {
    let scope = state
        .code_tools
        .scopes
        .get(execution_id)
        .ok_or_else(|| invalid("unknown code tool execution scope"))?;
    match target {
        CodeToolCancelTarget::Call { request_id } => {
            // A call left out had finished: cancelling it records nothing.
            if scope.left_out(request_id) {
                return Ok(Vec::new());
            }
            let call = scope
                .calls
                .get(request_id)
                .ok_or_else(|| invalid("unknown code tool request"))?;
            let origin = &call.spec.origin;
            if call.status.is_terminal() || code_tool_call_cancel_requested(state, origin) {
                return Ok(Vec::new());
            }
            Ok(vec![CoreAgentEventProposal::new(
                joins(state, origin)?,
                CoreAgentEvent::CodeTool(CodeToolEvent::CallCancelRequested {
                    origin: origin.clone(),
                }),
            )])
        }
        CodeToolCancelTarget::Promise { promise_id } => {
            let run_id = scope_owner(state, &scope.spec)?.run_id;
            let promise = state
                .promises
                .promises
                .get(promise_id)
                .ok_or_else(|| invalid(format!("unknown promise {promise_id}")))?;
            if promise.ownership != PromiseOwnership::Model {
                return Err(invalid(format!(
                    "promise {promise_id} is runtime-owned and cannot be cancelled by its holder"
                )));
            }
            match promise.scope {
                crate::PromiseScope::Run { run_id: owner } if owner != run_id => {
                    return Err(invalid(format!(
                        "promise {promise_id} belongs to run {owner}, not run {run_id}"
                    )));
                }
                _ => {}
            }
            Ok(crate::promise_cancel_event(state, promise_id)
                .map(|event| {
                    CoreAgentEventProposal::new(
                        CoreAgentJoins {
                            run_id: Some(run_id),
                            ..CoreAgentJoins::default()
                        },
                        CoreAgentEvent::Promise(event),
                    )
                })
                .into_iter()
                .collect())
        }
    }
}

pub fn close_code_tool_scope_proposals(
    state: &CoreAgentState,
    execution_id: String,
    cancel: bool,
    program_context: Option<crate::ProgramContextInput>,
    acknowledged: Option<u64>,
) -> Result<Vec<CoreAgentEventProposal>, DomainError> {
    let scope = state
        .code_tools
        .scopes
        .get(&execution_id)
        .ok_or_else(|| invalid("unknown code tool execution scope"))?;
    // A context that arrives after the run stopped being active, or after
    // its scope closed, could never be written: it is dropped, not refused,
    // so the close itself still goes through.
    let program_context = match program_context {
        Some(context) => {
            validate_program_context_owner(state, scope, &context)?;
            program_context_keepable(state, scope).then_some(context)
        }
        None => None,
    };
    if scope.closed && (!cancel || scope.cancel_requested) {
        return Ok(Vec::new());
    }
    if let Some(acknowledged) = acknowledged {
        validate_acknowledgement(scope, acknowledged)?;
    }
    // A program run's exit withdraws the handles its program still holds
    // (opened input requests), as it cancels its calls. Detached handles
    // belong to the session and stay open; the run's own runtime-owned
    // promises are not the program's.
    let withdrawn = match (&scope.spec.owner, cancel) {
        (crate::ScopeOwner::Run { run_id }, true) => state
            .promises
            .pending_for_run(*run_id)
            .filter(|promise| promise.ownership == PromiseOwnership::Model)
            .map(|promise| promise.promise_id.clone())
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    let mut proposals = vec![CoreAgentEventProposal::new(
        CoreAgentJoins::default(),
        CoreAgentEvent::CodeTool(CodeToolEvent::ScopeClosed {
            execution_id,
            cancel,
            program_context,
            withdrawn: withdrawn.clone(),
            acknowledged,
        }),
    )];
    if let crate::ScopeOwner::Run { run_id } = scope.spec.owner {
        for promise_id in &withdrawn {
            if let Some(event) = crate::promise_cancel_event(state, promise_id) {
                proposals.push(CoreAgentEventProposal::new(
                    CoreAgentJoins {
                        run_id: Some(run_id),
                        ..CoreAgentJoins::default()
                    },
                    CoreAgentEvent::Promise(event),
                ));
            }
        }
    }
    if cancel {
        for call in scope.calls.values() {
            if let CodeToolCallStatus::Waiting {
                suspension: ToolBatchSuspension::JoinedWorkflowCalls { calls, .. },
            } = &call.status
            {
                for joined in calls {
                    if !state
                        .promises
                        .promises
                        .get(&joined.promise_id)
                        .is_some_and(|promise| promise.ownership == PromiseOwnership::Runtime)
                    {
                        continue;
                    }
                    if let Some(event) = crate::promise_cancel_event(state, &joined.promise_id) {
                        proposals.push(CoreAgentEventProposal::new(
                            joins(state, &call.spec.origin)?,
                            CoreAgentEvent::Promise(event),
                        ));
                    }
                }
            }
        }
    }
    Ok(proposals)
}

/// Reuse the ordinary request projection without opening another model batch.
pub fn code_tool_request(
    session_id: &SessionId,
    state: &CoreAgentState,
    execution_id: &str,
    request_id: &str,
) -> Result<ToolInvocationBatchRequest, DomainError> {
    let origin = CodeToolOrigin {
        execution_id: execution_id.to_owned(),
        request_id: request_id.to_owned(),
    };
    let call =
        code_tool_call(state, &origin).ok_or_else(|| invalid("unknown code tool request"))?;
    if !matches!(call.status, CodeToolCallStatus::Pending) {
        return Err(invalid("code tool request is not pending"));
    }
    let scope = &state.code_tools.scopes[execution_id];
    if scope.cancel_requested {
        return Err(invalid("code tool scope is cancelling"));
    }
    validate_live_owner(state, &scope.spec)?;
    validate_bindings_current(state, scope)?;
    let facts = owner_facts(state, &scope.spec)?;
    if facts.session_id != session_id {
        return Err(invalid(
            "code tool request session does not match its owner",
        ));
    }
    let mut request = crate::core::drive::tool_invocation_request(
        state,
        session_id,
        facts.run_id,
        facts.dispatch.clone(),
        call.promise_id_base,
        std::slice::from_ref(&crate::ObservedToolCall {
            call_id: call.call_id.clone(),
            tool_id: Some(call.spec.tool_id.clone()),
            tool_name: call.spec.tool_name.clone(),
            provider_kind: None,
            arguments_ref: call.spec.arguments_ref.clone(),
            native_call_ref: None,
        }),
    )?;
    // A run's approval of a model call never authorizes a scoped call. A
    // scope that parks such calls gives them the same call addressed to the
    // approval binding's tool, to wait through instead.
    let approval = match approval_binding_for_call(state, scope, call) {
        Some(binding) => {
            let mut parked = crate::core::drive::tool_invocation_request(
                state,
                session_id,
                facts.run_id,
                facts.dispatch.clone(),
                call.promise_id_base,
                std::slice::from_ref(&crate::ObservedToolCall {
                    call_id: call.call_id.clone(),
                    tool_id: Some(binding.definition.tool.name.clone()),
                    tool_name: binding.definition.tool.name.clone(),
                    provider_kind: None,
                    arguments_ref: call.spec.arguments_ref.clone(),
                    native_call_ref: None,
                }),
            )?;
            Some(Box::new(parked.calls.remove(0)))
        }
        None => None,
    };
    if let Some(remote) = &mut request.calls[0].remote_mcp {
        match remote {
            crate::RemoteMcpCallRuntime::Injected {
                approval_decision,
                park,
                ..
            } => {
                *approval_decision = None;
                *park = approval;
            }
            crate::RemoteMcpCallRuntime::Search {
                approval_decision, ..
            } => *approval_decision = None,
        }
    }
    Ok(request)
}

/// The request that runs a parked call once its approval is granted: the
/// call as its scope dispatches it, carrying the approval, for the approval
/// workflow `invocation_id` to execute. The call must still wait on that
/// approval in a live scope whose bindings are current.
pub fn approved_code_tool_request(
    session_id: &SessionId,
    state: &CoreAgentState,
    call_id: &ToolCallId,
    invocation_id: &WorkflowToolInvocationId,
) -> Result<crate::ToolInvocationCallRequest, DomainError> {
    let (scope, call) =
        code_tool_call_for_id(state, call_id).ok_or_else(|| invalid("unknown code tool call"))?;
    let CodeToolCallStatus::Waiting {
        suspension: ToolBatchSuspension::JoinedWorkflowCalls { calls, .. },
    } = &call.status
    else {
        return Err(invalid(
            "approved code tool call is not waiting for approval",
        ));
    };
    if calls.first().map(|joined| &joined.invocation_id) != Some(invocation_id)
        || approval_binding_for_call(state, scope, call).is_none()
    {
        return Err(invalid("approval does not match its parked call"));
    }
    if scope.cancel_requested {
        return Err(invalid("code tool scope is cancelling"));
    }
    validate_live_owner(state, &scope.spec)?;
    validate_bindings_current(state, scope)?;
    let facts = owner_facts(state, &scope.spec)?;
    if facts.session_id != session_id {
        return Err(invalid(
            "code tool request session does not match its owner",
        ));
    }
    let mut request = crate::core::drive::tool_invocation_request(
        state,
        session_id,
        facts.run_id,
        facts.dispatch,
        call.promise_id_base,
        std::slice::from_ref(&crate::ObservedToolCall {
            call_id: call.call_id.clone(),
            tool_id: Some(call.spec.tool_id.clone()),
            tool_name: call.spec.tool_name.clone(),
            provider_kind: None,
            arguments_ref: call.spec.arguments_ref.clone(),
            native_call_ref: None,
        }),
    )?;
    let Some(crate::RemoteMcpCallRuntime::Injected {
        approval_decision,
        park,
        ..
    }) = &mut request.calls[0].remote_mcp
    else {
        return Err(invalid("approved code tool call is not a native MCP call"));
    };
    *approval_decision = Some(true);
    *park = None;
    Ok(request
        .call_request(
            0,
            crate::ToolExecutionSpec::new(crate::ToolExecutionClass::RemoteInteractive, false),
        )
        .expect("one approved call"))
}

/// Only a program run's scope carries a program context.
fn validate_program_context_owner(
    state: &CoreAgentState,
    scope: &CodeToolScope,
    context: &crate::ProgramContextInput,
) -> Result<(), DomainError> {
    context.validate()?;
    if !matches!(scope.spec.owner, ScopeOwner::Run { .. }) || !crate::is_program_session(state) {
        return Err(invalid(
            "only a program run's scope keeps a program context",
        ));
    }
    Ok(())
}

/// The context is kept once, as the scope of a still-active run closes.
fn program_context_keepable(state: &CoreAgentState, scope: &CodeToolScope) -> bool {
    let ScopeOwner::Run { run_id } = scope.spec.owner else {
        return false;
    };
    !scope.closed
        && scope.program_context.is_none()
        && state
            .runs
            .active
            .as_ref()
            .is_some_and(|run| run.run_id == run_id && run.status == crate::RunStatus::Active)
}

pub(crate) fn apply_code_tool_event(
    state: &mut CoreAgentState,
    event: &CodeToolEvent,
) -> Result<(), DomainError> {
    match event {
        CodeToolEvent::ScopeOpened { scope } => {
            if state.code_tools.scopes.contains_key(&scope.execution_id) {
                return Err(invalid("duplicate code tool scope event"));
            }
            validate_scope(state, scope)?;
            match scope.policy.retention {
                // Retain closed outcomes for the current run, and drop
                // prior-run scope indexes. A program run's later execution
                // keeps only the previous execution's settled scope beside
                // its own. The immutable log remains the outcome archive.
                Retention::FullInState => {
                    let run_id = scope_owner(state, scope)?.run_id;
                    let previous = previous_execution_scope(scope);
                    let retain: BTreeSet<_> = state
                        .code_tools
                        .scopes
                        .iter()
                        .filter(|(id, existing)| {
                            scope_owner(state, &existing.spec)
                                .is_ok_and(|owner| owner.run_id == run_id)
                                && previous.as_ref().is_none_or(|previous| {
                                    *id == previous || existing.spec.owner != scope.owner
                                })
                        })
                        .map(|(id, _)| id.clone())
                        .collect();
                    state.code_tools.scopes.retain(|id, _| retain.contains(id));
                }
            }
            state.code_tools.scopes.insert(
                scope.execution_id.clone(),
                CodeToolScope {
                    toolset_revision: state.tooling.revision,
                    spec: scope.clone(),
                    closed: false,
                    cancel_requested: false,
                    calls: BTreeMap::new(),
                    completed: Vec::new(),
                    completed_base: 0,
                    calls_dropped: 0,
                    dropped_max_request: 0,
                    acknowledged: 0,
                    program_context: None,
                    cancel_requested_calls: BTreeSet::new(),
                    withdrawn: BTreeSet::new(),
                },
            );
        }
        CodeToolEvent::CallAdmitted {
            call,
            promise_id_base,
            acknowledged,
        } => {
            refuse_left_out(state, &call.origin)?;
            validate_call(state, call)?;
            if code_tool_call(state, &call.origin).is_some() {
                return Err(invalid("duplicate code tool admission event"));
            }
            if Some(*promise_id_base) != state.id_cursors.last_promise_id.checked_add(1) {
                return Err(invalid("code tool promise reservation is not contiguous"));
            }
            let last = promise_id_base
                .checked_add(CODE_TOOL_PROMISE_SLOTS - 1)
                .ok_or_else(|| invalid("promise counter exhausted"))?;
            let scope = state
                .code_tools
                .scopes
                .get_mut(&call.origin.execution_id)
                .expect("validated scope");
            acknowledge(scope, *acknowledged)?;
            scope.calls.insert(
                call.origin.request_id.clone(),
                CodeToolCall {
                    spec: call.clone(),
                    call_id: code_tool_call_id(&call.origin),
                    promise_id_base: *promise_id_base,
                    status: CodeToolCallStatus::Pending,
                    completed_at_ms: None,
                },
            );
            state.id_cursors.last_promise_id = last;
        }
        CodeToolEvent::CallCompleted {
            origin,
            result,
            completed_at_ms,
        } => {
            let call = code_tool_call(state, origin)
                .ok_or_else(|| invalid("unknown code tool request"))?;
            validate_result_fields(call, &result.call_id, result.status)?;
            if call.status.is_terminal() {
                return Err(invalid("duplicate code tool completion event"));
            }
            let scope = state
                .code_tools
                .scopes
                .get_mut(&origin.execution_id)
                .expect("validated scope");
            let call = scope
                .calls
                .get_mut(&origin.request_id)
                .expect("validated call");
            call.status = CodeToolCallStatus::Completed {
                result: result.clone(),
            };
            call.completed_at_ms = *completed_at_ms;
            scope.completed.push(origin.request_id.clone());
        }
        CodeToolEvent::CallDeferred { origin, suspension } => {
            let call = code_tool_call(state, origin)
                .ok_or_else(|| invalid("unknown code tool request"))?;
            if !matches!(call.status, CodeToolCallStatus::Pending) {
                return Err(invalid(
                    "code tool request was already deferred or completed",
                ));
            }
            match suspension {
                ToolBatchSuspension::AwaitTool { call_id, spec } => {
                    if call_id != &call.call_id {
                        return Err(invalid("code tool await call id mismatch"));
                    }
                    defer_code_tool_call_proposals(state, origin.clone(), spec.clone())?;
                }
                ToolBatchSuspension::JoinedWorkflowCalls { calls, spec } => {
                    if calls.len() != 1
                        || calls[0].call_id != call.call_id
                        || spec.promise_ids != vec![calls[0].promise_id.clone()]
                        || spec.mode != AwaitMode::All
                    {
                        return Err(invalid(
                            "code tool joined suspension does not match its call",
                        ));
                    }
                    let binding = joined_binding_for_call(
                        state,
                        &state.code_tools.scopes[&origin.execution_id],
                        call,
                    )
                    .ok_or_else(|| invalid("code tool suspension requires a joined binding"))?;
                    let promise = state
                        .promises
                        .promises
                        .get(&calls[0].promise_id)
                        .ok_or_else(|| invalid("code tool completion promise is missing"))?;
                    let facts =
                        owner_facts(state, &state.code_tools.scopes[&origin.execution_id].spec)?;
                    let expected_invocation_id = WorkflowToolInvocationId::for_context(
                        binding.session_universe_id,
                        facts.session_id,
                        facts.run_id,
                        &facts.dispatch.call_context(&call.call_id),
                        &binding.binding_fingerprint,
                    );
                    if calls[0].invocation_id != expected_invocation_id
                        || promise.ownership != PromiseOwnership::Runtime
                        || promise.scope
                            != (crate::PromiseScope::Run {
                                run_id: facts.run_id,
                            })
                        || promise.status != PromiseStatus::Pending
                        || promise.deadline_ms.is_none_or(|deadline| deadline == 0)
                        || !matches!(&promise.source, crate::PromiseSource::Workflow { invocation_id, completion_key, .. }
                            if invocation_id == expected_invocation_id.as_str() && completion_key == crate::REPLY_COMPLETION_KEY)
                        || calls[0]
                            .promise_id
                            .number()
                            .checked_sub(call.promise_id_base)
                            .is_none_or(|offset| offset >= CODE_TOOL_PROMISE_SLOTS)
                    {
                        return Err(invalid(
                            "code tool joined promise does not match its admitted invocation",
                        ));
                    }
                }
            }
            state
                .code_tools
                .scopes
                .get_mut(&origin.execution_id)
                .expect("validated scope")
                .calls
                .get_mut(&origin.request_id)
                .expect("validated call")
                .status = CodeToolCallStatus::Waiting {
                suspension: suspension.clone(),
            };
        }
        CodeToolEvent::CallCancelRequested { origin } => {
            let call = code_tool_call(state, origin)
                .ok_or_else(|| invalid("unknown code tool request"))?;
            if call.status.is_terminal() {
                return Err(invalid("a completed code tool request cannot be cancelled"));
            }
            let inserted = state
                .code_tools
                .scopes
                .get_mut(&origin.execution_id)
                .expect("validated scope")
                .cancel_requested_calls
                .insert(origin.request_id.clone());
            if !inserted {
                return Err(invalid(
                    "code tool request cancellation was already requested",
                ));
            }
        }
        CodeToolEvent::ScopeClosed {
            execution_id,
            cancel,
            program_context,
            withdrawn,
            acknowledged,
        } => {
            let scope = state
                .code_tools
                .scopes
                .get(execution_id)
                .ok_or_else(|| invalid("unknown code tool execution scope"))?;
            if let Some(context) = program_context {
                validate_program_context_owner(state, scope, context)?;
                if !program_context_keepable(state, scope) {
                    return Err(invalid(
                        "program context is kept only as an active run's scope closes",
                    ));
                }
            }
            if !withdrawn.is_empty() {
                let crate::ScopeOwner::Run { run_id } = scope.spec.owner else {
                    return Err(invalid("only a program run's scope withdraws handles"));
                };
                let owned = |promise_id: &crate::PromiseId| {
                    state
                        .promises
                        .promises
                        .get(promise_id)
                        .is_some_and(|promise| {
                            promise.scope == crate::PromiseScope::Run { run_id }
                                && promise.ownership == PromiseOwnership::Model
                        })
                };
                if !*cancel || !withdrawn.iter().all(owned) {
                    return Err(invalid(
                        "a scope close withdraws only its run's own handles, when it cancels",
                    ));
                }
            }
            let scope = state
                .code_tools
                .scopes
                .get_mut(execution_id)
                .expect("validated scope");
            acknowledge(scope, *acknowledged)?;
            scope.closed = true;
            scope.cancel_requested |= cancel;
            if program_context.is_some() {
                scope.program_context = program_context.clone();
            }
            scope.withdrawn.extend(withdrawn.iter().cloned());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invocation_id(fill: char) -> String {
        format!("wti:sha256:{}", fill.to_string().repeat(64))
    }

    /// A run-owned scope whose calls 1 and 2 a handoff left out, holding
    /// calls 3 and 4.
    fn scope_after_a_handoff() -> CodeToolScope {
        let call = |n: u64| {
            serde_json::json!({
                "spec": {
                    "origin": { "execution_id": "wtx:run", "request_id": format!("call-{n}") },
                    "binding_id": format!("binding:sha256:{n:064x}"),
                    "tool_id": "time.sleep",
                    "tool_name": "time_sleep",
                    "arguments_ref": format!("sha256:{n:064x}"),
                },
                "call_id": format!("code-tool:{n}"),
                "promise_id_base": n,
                "status": {
                    "kind": "completed",
                    "result": {
                        "call_id": format!("code-tool:{n}"),
                        "status": "succeeded",
                        "output_ref": null,
                        "error_ref": null,
                    },
                },
            })
        };
        serde_json::from_value(serde_json::json!({
            "toolset_revision": 1,
            "spec": {
                "execution_id": "wtx:run",
                "owner": { "kind": "run", "run_id": 1 },
                "policy": {
                    "binding_mode": "contract",
                    "revalidation": "toolset_revision",
                    "completion": "ack",
                    "limits": { "max_calls": 10, "max_in_flight": 4 },
                    "retention": "full_in_state",
                    "approvals": "park",
                },
                "bindings": {},
            },
            "closed": false,
            "cancel_requested": false,
            "calls": { "call-3": call(3), "call-4": call(4) },
            "completed": ["call-3", "call-4"],
            "completed_base": 2,
            "calls_dropped": 2,
        }))
        .expect("scope")
    }

    #[test]
    fn a_scope_keeps_counts_and_positions_absolute_when_calls_are_left_out() {
        let scope = scope_after_a_handoff();
        assert_eq!(scope.admitted(), 4);
        assert_eq!(scope.recorded(), 4);
        let outcomes = |after: u64| -> Option<Vec<String>> {
            let calls: Vec<&CodeToolCall> = scope.recorded_outcomes(after, 10)?;
            Some(
                calls
                    .into_iter()
                    .map(|call| call.spec.origin.request_id.clone())
                    .collect(),
            )
        };
        assert_eq!(outcomes(1), None, "position 1 was left out");
        assert_eq!(
            outcomes(2),
            Some(vec!["call-3".to_owned(), "call-4".to_owned()])
        );
        assert_eq!(outcomes(3), Some(vec!["call-4".to_owned()]));
        assert_eq!(outcomes(4), Some(Vec::new()));

        let mut whole = scope.clone();
        whole.completed_base = 0;
        whole.calls_dropped = 0;
        let stored = serde_json::to_value(&whole).expect("serialize");
        assert!(
            stored.get("completed_base").is_none() && stored.get("calls_dropped").is_none(),
            "a scope that left nothing out is stored as before"
        );
    }

    #[test]
    fn scope_spec_rejects_records_whose_legacy_fields_disagree_with_owner_or_policy() {
        let base = serde_json::json!({
            "execution_id": "execution-1",
            "bindings": {},
            "owner": { "kind": "model_invocation", "parent_invocation_id": invocation_id('0') },
            "policy": {
                "binding_mode": "presentation",
                "revalidation": "toolset_revision",
                "completion": "blocking",
                "limits": { "max_calls": 4, "max_in_flight": 2 },
                "retention": "full_in_state",
                "approvals": "reject"
            }
        });
        let decode = |patch: serde_json::Value| {
            let mut record = base.clone();
            record
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            serde_json::from_value::<CodeToolScopeSpec>(record)
        };
        let spec = decode(serde_json::json!({})).expect("current shape decodes");
        assert_eq!(spec.policy.completion, Completion::Blocking);
        let redundant = decode(serde_json::json!({
            "parent_invocation_id": invocation_id('0'),
            "max_calls": 4,
            "max_in_flight": 2
        }))
        .expect("matching legacy fields are accepted");
        assert_eq!(redundant, spec);
        assert!(decode(serde_json::json!({ "parent_invocation_id": invocation_id('f') })).is_err());
        assert!(decode(serde_json::json!({ "max_calls": 5, "max_in_flight": 2 })).is_err());
        let mut no_owner = base.clone();
        no_owner.as_object_mut().unwrap().remove("owner");
        assert!(serde_json::from_value::<CodeToolScopeSpec>(no_owner).is_err());
        let mut no_policy = base;
        no_policy.as_object_mut().unwrap().remove("policy");
        assert!(serde_json::from_value::<CodeToolScopeSpec>(no_policy).is_err());
    }
}
