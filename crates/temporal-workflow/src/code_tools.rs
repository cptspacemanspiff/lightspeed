//! Generic session-owned code tool invocation protocol.
//!
//! A trusted execution host narrows the session's existing grants when opening
//! a scope. Subsequent requests name only an opaque binding from that scope;
//! they cannot supply their own argument adapter or execution destination.

use std::collections::{BTreeMap, BTreeSet};

use harness::{Attachment, BlobRef, ToolCallId, ToolName, WorkflowToolInvocationId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use temporalio_client::{
    Client, RpcOptions, WorkflowExecuteUpdateOptions, WorkflowQueryOptions,
    errors::{WorkflowQueryError, WorkflowUpdateError},
};

use crate::AgentSessionWorkflow;

pub const OPEN_CODE_TOOL_SCOPE_UPDATE: &str = "open_code_tool_scope";
pub const INVOKE_CODE_TOOL_UPDATE: &str = "invoke_code_tool";
pub const CLOSE_CODE_TOOL_SCOPE_UPDATE: &str = "close_code_tool_scope";
pub const CANCEL_CODE_TOOL_UPDATE: &str = "cancel_code_tool";
pub const RECORD_CODE_TOOL_NOTES_UPDATE: &str = "record_code_tool_notes";
pub const CODE_TOOL_SCOPE_REPORT_QUERY: &str = "code_tool_scope_report";
pub const SCOPE_RECORDED_OUTCOMES_QUERY: &str = "scope_recorded_outcomes";

/// Trusted host request to open a scope. A code execution opens it beneath
/// its admitted, pending joined invocation; a program execution opens the
/// scope its run owns. The session resolves bindings itself; the allowlist
/// can only narrow grants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OpenCodeToolScopeRequest {
    pub execution_id: String,
    /// The parked joined invocation that owns a code execution's scope.
    /// Exactly one of this and `run_id` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_invocation_id: Option<WorkflowToolInvocationId>,
    /// The program run that owns a program execution's scope; the execution
    /// id must be the run's program execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<harness::RunId>,
    /// A supplied set narrows logical tool identities; an empty set grants no
    /// tools. None selects all currently host-callable tools except the parent
    /// operation. Reopening an existing scope retains its original bindings.
    #[serde(default)]
    pub allowed_tools: Option<BTreeSet<ToolName>>,
    #[schemars(range(min = 1))]
    pub max_calls: u32,
    #[schemars(range(min = 1))]
    pub max_in_flight: u32,
}

/// Small request forwarded by a trusted host after serializing guest arguments.
/// Identity is execution-local. Reusing it with different arguments or a
/// different binding is rejected by the session, including across RPC retries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InvokeCodeToolRequest {
    pub execution_id: String,
    pub request_id: String,
    pub binding_id: String,
    pub arguments_ref: BlobRef,
    /// From a program run: the completions it had collected when the
    /// workflow task sending this began. Its replays never ask for those
    /// again, so the session may stop holding them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged: Option<u64>,
    /// From a program: why it made the call (`{ why }`). Shown with the
    /// call; a resent request may carry a different one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotation: Option<String>,
    /// From a program: the step (`ctx.step`) the call was made in, by the
    /// seq of the note that began it. Shown with the call, like
    /// `annotation`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<u64>,
}

/// Close admission immediately. Cancellation additionally requests cleanup of
/// admitted work; closing a scope does not roll back any completed effects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CloseCodeToolScopeRequest {
    pub execution_id: String,
    pub cancel_pending: bool,
    /// The program context a program run's scope keeps as it closes;
    /// written as the session's newest only if the run completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program_context: Option<harness::ProgramContextInput>,
    /// As on `InvokeCodeToolRequest`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged: Option<u64>,
}

/// Cancel one call of a scope, or a promise its owner holds a handle to (an
/// opened input request). A cancel is a request, not an outcome: the target
/// still records exactly one outcome, which is its result if that came
/// first. Resending is safe; requests are admitted in order with calls.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CancelCodeToolRequest {
    pub execution_id: String,
    pub target: harness::CodeToolCancelTarget,
}

/// A program's notes on its work (`ctx.say`, `ctx.step`), in order. Notes
/// the session recorded before are skipped, so resending is safe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordCodeToolNotesRequest {
    pub execution_id: String,
    pub notes: Vec<harness::ProgramNote>,
}

/// Where the target stood once the cancel was recorded. `Requested` means
/// its outcome is still to come; the outcome itself is read where it always
/// is (a call's recorded outcome, a promise's resolution). This status is
/// for logging, not for deciding what happened.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CodeToolCancelOutcome {
    pub status: CodeToolCancelStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CodeToolCancelStatus {
    Requested,
    Cancelled,
    Resolved,
    Failed,
}

/// Read an authoritative snapshot, including after a runner loses its waiters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CodeToolScopeReportRequest {
    pub execution_id: String,
}

/// Read a scope's recorded outcomes in the order they were recorded, after
/// the first `after` of them. A replaying executor feeds these to the guest
/// as its journal, so every attempt sees completions in the same order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeRecordedOutcomesRequest {
    pub execution_id: String,
    #[serde(default)]
    pub after: u64,
    #[schemars(range(min = 1))]
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ScopeRecordedOutcomes {
    pub execution_id: String,
    pub closed: bool,
    pub cancel_requested: bool,
    /// Terminal outcomes in log order, starting after `after`.
    pub outcomes: Vec<CodeToolCallOutcome>,
    /// The cursor for the next page: `after` plus the outcomes returned.
    pub next: u64,
    /// Requests admitted but not yet terminal, so a replaying executor
    /// knows what it is still waiting on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<String>,
}

pub const MAX_RECORDED_OUTCOMES_PER_PAGE: u32 = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CodeToolScopeReport {
    pub execution_id: String,
    pub closed: bool,
    pub cancel_requested: bool,
    /// Exposed names keyed by the opaque handles accepted by invoke requests.
    pub bindings: BTreeMap<String, ToolName>,
    /// Under contract binding, the contract each handle serves.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub contracts: BTreeMap<String, harness::ContractBinding>,
    /// Requests are keyed by execution-local request id, never arrival order.
    /// Calls the program acknowledged may be left out; `admitted` and
    /// `recorded` count every call.
    pub calls: BTreeMap<String, CodeToolCallOutcome>,
    /// Calls ever admitted to the scope.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub admitted: u64,
    /// Calls ever given an outcome.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub recorded: u64,
    /// Handles a program run's close withdrew (opened input requests), and
    /// how many of them have settled. A run's exit waits for all of them.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub withdrawals: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub withdrawals_settled: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl CodeToolScopeReport {
    /// The scope's report, with its withdrawals read from the session.
    pub fn of(state: &harness::CoreAgentState, scope: &harness::CodeToolScope) -> Self {
        let (withdrawals, withdrawals_settled) = harness::code_tool_scope_withdrawals(state, scope);
        Self {
            withdrawals,
            withdrawals_settled,
            ..Self::from(scope)
        }
    }

    /// Whether every admitted call has an outcome and every withdrawn handle
    /// has settled: nothing of the scope is still pending.
    pub fn settled(&self) -> bool {
        self.closed
            && self.withdrawals_settled >= self.withdrawals
            && self.calls.values().all(|call| call.status.is_terminal())
    }
}

impl From<&harness::CodeToolScope> for CodeToolScopeReport {
    fn from(scope: &harness::CodeToolScope) -> Self {
        Self {
            withdrawals: 0,
            withdrawals_settled: 0,
            execution_id: scope.spec.execution_id.clone(),
            closed: scope.closed,
            cancel_requested: scope.cancel_requested,
            bindings: scope
                .spec
                .bindings
                .iter()
                .map(|(id, binding)| (id.clone(), binding.tool_name.clone()))
                .collect(),
            contracts: scope
                .spec
                .bindings
                .iter()
                .filter_map(|(id, binding)| {
                    binding
                        .contract
                        .as_ref()
                        .map(|contract| (id.clone(), contract.clone()))
                })
                .collect(),
            calls: scope
                .calls
                .iter()
                .map(|(id, call)| (id.clone(), CodeToolCallOutcome::from(call)))
                .collect(),
            admitted: scope.admitted(),
            recorded: scope.recorded(),
        }
    }
}

/// Script-facing completion metadata. Session effects, model context, and
/// activity configuration remain private to the owning session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CodeToolCallOutcome {
    pub request_id: String,
    pub call_id: ToolCallId,
    pub status: CodeToolCallStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<BlobRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_ref: Option<BlobRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    /// When the session recorded the outcome; a program's clock advances to
    /// it on delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
}

impl From<&harness::CodeToolCall> for CodeToolCallOutcome {
    fn from(call: &harness::CodeToolCall) -> Self {
        let mut outcome = Self {
            request_id: call.spec.origin.request_id.clone(),
            call_id: call.call_id.clone(),
            status: CodeToolCallStatus::Pending,
            output_ref: None,
            error_ref: None,
            attachments: Vec::new(),
            completed_at_ms: call.completed_at_ms,
        };
        match &call.status {
            harness::CodeToolCallStatus::Pending => {}
            harness::CodeToolCallStatus::Waiting { .. } => {
                outcome.status = CodeToolCallStatus::Waiting;
            }
            harness::CodeToolCallStatus::Completed { result } => {
                outcome.status = match result.status {
                    harness::ToolCallStatus::Succeeded => CodeToolCallStatus::Succeeded,
                    harness::ToolCallStatus::Cancelled => CodeToolCallStatus::Cancelled,
                    harness::ToolCallStatus::Unavailable => CodeToolCallStatus::Unavailable,
                    _ => CodeToolCallStatus::Failed,
                };
                outcome.output_ref = result.output_ref.clone();
                outcome.error_ref = result.error_ref.clone();
                outcome.attachments = result.attachments.clone();
            }
        }
        outcome
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CodeToolCallStatus {
    Pending,
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
    Unavailable,
}

impl CodeToolCallStatus {
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Waiting)
    }
}

/// Admission failures are successful protocol replies, distinct from transport
/// failures and the tool's own failed result. They never imply tool execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CodeToolRejection {
    pub kind: CodeToolRejectionKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CodeToolRejectionKind {
    InvalidRequest,
    UnknownScope,
    ScopeClosed,
    Conflict,
    Unavailable,
    PermissionDenied,
    LimitExceeded,
    SessionNotReady,
    Internal,
}

impl CodeToolRejection {
    pub fn new(kind: CodeToolRejectionKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CodeToolRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for CodeToolRejection {}

pub type CodeToolScopeResult = Result<CodeToolScopeReport, CodeToolRejection>;
pub type CodeToolInvocationResult = Result<CodeToolCallOutcome, CodeToolRejection>;
pub type ScopeRecordedOutcomesResult = Result<ScopeRecordedOutcomes, CodeToolRejection>;
pub type CodeToolCancelResult = Result<CodeToolCancelOutcome, CodeToolRejection>;
pub type CodeToolNotesResult = Result<(), CodeToolRejection>;

/// Temporal is the transport even when the runner and session share a process.
/// Dropping one of these futures only drops the client waiter; explicit scope
/// closure requests cancellation of already admitted operations.
#[derive(Clone)]
pub struct CodeToolClient {
    client: Client,
    session_workflow_id: String,
}

impl CodeToolClient {
    /// Use the stable workflow id, without a run id. Session-side request
    /// identity remains authoritative across workflow replay and rollover.
    pub fn new(client: Client, session_workflow_id: String) -> Self {
        Self {
            client,
            session_workflow_id,
        }
    }

    pub async fn open_scope(
        &self,
        request: OpenCodeToolScopeRequest,
        rpc_options: RpcOptions,
    ) -> Result<CodeToolScopeResult, WorkflowUpdateError> {
        let options = update_options(OPEN_CODE_TOOL_SCOPE_UPDATE, &request, rpc_options);
        self.client
            .get_workflow_handle::<AgentSessionWorkflow>(self.session_workflow_id.clone())
            .execute_update(AgentSessionWorkflow::open_code_tool_scope, request, options)
            .await
    }

    pub async fn invoke(
        &self,
        request: InvokeCodeToolRequest,
        rpc_options: RpcOptions,
    ) -> Result<CodeToolInvocationResult, WorkflowUpdateError> {
        let options = update_options(INVOKE_CODE_TOOL_UPDATE, &request, rpc_options);
        self.client
            .get_workflow_handle::<AgentSessionWorkflow>(self.session_workflow_id.clone())
            .execute_update(AgentSessionWorkflow::invoke_code_tool, request, options)
            .await
    }

    pub async fn close_scope(
        &self,
        request: CloseCodeToolScopeRequest,
        rpc_options: RpcOptions,
    ) -> Result<CodeToolScopeResult, WorkflowUpdateError> {
        let options = update_options(CLOSE_CODE_TOOL_SCOPE_UPDATE, &request, rpc_options);
        self.client
            .get_workflow_handle::<AgentSessionWorkflow>(self.session_workflow_id.clone())
            .execute_update(
                AgentSessionWorkflow::close_code_tool_scope,
                request,
                options,
            )
            .await
    }

    pub async fn cancel(
        &self,
        request: CancelCodeToolRequest,
        rpc_options: RpcOptions,
    ) -> Result<CodeToolCancelResult, WorkflowUpdateError> {
        let options = update_options(CANCEL_CODE_TOOL_UPDATE, &request, rpc_options);
        self.client
            .get_workflow_handle::<AgentSessionWorkflow>(self.session_workflow_id.clone())
            .execute_update(AgentSessionWorkflow::cancel_code_tool, request, options)
            .await
    }

    pub async fn record_notes(
        &self,
        request: RecordCodeToolNotesRequest,
        rpc_options: RpcOptions,
    ) -> Result<CodeToolNotesResult, WorkflowUpdateError> {
        let options = update_options(RECORD_CODE_TOOL_NOTES_UPDATE, &request, rpc_options);
        self.client
            .get_workflow_handle::<AgentSessionWorkflow>(self.session_workflow_id.clone())
            .execute_update(
                AgentSessionWorkflow::record_code_tool_notes,
                request,
                options,
            )
            .await
    }

    pub async fn recorded_outcomes(
        &self,
        request: ScopeRecordedOutcomesRequest,
        rpc_options: RpcOptions,
    ) -> Result<ScopeRecordedOutcomesResult, WorkflowQueryError> {
        self.client
            .get_workflow_handle::<AgentSessionWorkflow>(self.session_workflow_id.clone())
            .query(
                AgentSessionWorkflow::scope_recorded_outcomes,
                request,
                WorkflowQueryOptions::builder()
                    .rpc_options(rpc_options)
                    .build(),
            )
            .await
    }

    /// Queries do not reuse the cached result of an earlier Update. Repeated
    /// reports can therefore observe effects that finish after scope closure.
    pub async fn report(
        &self,
        request: CodeToolScopeReportRequest,
        rpc_options: RpcOptions,
    ) -> Result<CodeToolScopeResult, WorkflowQueryError> {
        self.client
            .get_workflow_handle::<AgentSessionWorkflow>(self.session_workflow_id.clone())
            .query(
                AgentSessionWorkflow::code_tool_scope_report,
                request,
                WorkflowQueryOptions::builder()
                    .rpc_options(rpc_options)
                    .build(),
            )
            .await
    }
}

fn update_options(
    operation: &str,
    request: &impl Serialize,
    rpc_options: RpcOptions,
) -> WorkflowExecuteUpdateOptions {
    WorkflowExecuteUpdateOptions::builder()
        .update_id(update_id(operation, request))
        .rpc_options(rpc_options)
        .build()
}

fn update_id(operation: &str, request: &impl Serialize) -> String {
    let request = serde_json::to_vec(request).expect("code tool request has a JSON wire encoding");
    let mut hasher = Sha256::new();
    for part in [
        b"lightspeed.code-tool.update.v1".as_slice(),
        operation.as_bytes(),
        &request,
    ] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    // Include the full request, not just its identity. Otherwise Temporal could
    // return a cached success for a conflicting reuse before session admission
    // sees the changed arguments and rejects it.
    format!("ctu:sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_identity_retries_exact_requests_but_preserves_conflict_checks() {
        let request = InvokeCodeToolRequest {
            execution_id: "execution-a".to_owned(),
            request_id: "request-a".to_owned(),
            binding_id: "binding-a".to_owned(),
            arguments_ref: BlobRef::from_bytes(b"{}"),
            acknowledged: None,
            annotation: None,
            step: None,
        };
        let id = update_id(INVOKE_CODE_TOOL_UPDATE, &request);
        assert_eq!(id, update_id(INVOKE_CODE_TOOL_UPDATE, &request));
        for different in [
            InvokeCodeToolRequest {
                execution_id: "execution-b".to_owned(),
                ..request.clone()
            },
            InvokeCodeToolRequest {
                request_id: "request-b".to_owned(),
                ..request.clone()
            },
            InvokeCodeToolRequest {
                binding_id: "binding-b".to_owned(),
                ..request.clone()
            },
            InvokeCodeToolRequest {
                arguments_ref: BlobRef::from_bytes(b"{\"changed\":true}"),
                ..request.clone()
            },
        ] {
            assert_ne!(id, update_id(INVOKE_CODE_TOOL_UPDATE, &different));
        }
        assert_ne!(id, update_id(CLOSE_CODE_TOOL_SCOPE_UPDATE, &request));
    }
}
