//! The program call index: one row per call a program run made, kept by a
//! session store as it appends the session log. A run can make up to
//! `MAX_PROGRAM_CALLS` calls and reducer state keeps only the current run's
//! scopes, so a run's calls are listed page by page from the index rather
//! than folded from the log or read from state.
//!
//! `program_call_changes` reads one appended entry; the store applies the
//! changes in the append's transaction. Only scopes a run owns are indexed:
//! a call joins the index when its scope opened as a program's.

use std::collections::BTreeMap;

use crate::{
    BlobRef, CallContext, CodecError, CoreAgentCodec, CoreAgentEvent, RunId, ScopeOwner,
    ToolCallId, ToolCallStatus, WorkflowToolEvent, storage::StoredSessionEntry,
};

/// Where an indexed call stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgramCallStatus {
    Pending,
    /// Waiting for an approval decision.
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
}

impl ProgramCallStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Waiting => "waiting",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        [
            Self::Pending,
            Self::Waiting,
            Self::Succeeded,
            Self::Failed,
            Self::Cancelled,
        ]
        .into_iter()
        .find(|status| status.as_str() == value)
    }

    /// A completed call's status: anything but success or cancellation
    /// failed.
    pub fn of_outcome(status: ToolCallStatus) -> Self {
        match status {
            ToolCallStatus::Succeeded => Self::Succeeded,
            ToolCallStatus::Cancelled => Self::Cancelled,
            _ => Self::Failed,
        }
    }
}

/// One indexed call, as a store lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgramCallRecord {
    /// The program execution's scope (`program_scope_id`).
    pub execution_id: String,
    /// The program's request id, `call-N` within the execution.
    pub request_id: String,
    pub run_id: RunId,
    /// The session log position that admitted it: the list's order and
    /// cursor.
    pub seq: u64,
    pub tool_call_id: ToolCallId,
    /// The contract the program called it through, when bound to one.
    pub contract: Option<String>,
    pub tool_id: String,
    pub tool_name: String,
    pub arguments_ref: BlobRef,
    /// Why the program made the call, as it said.
    pub annotation: Option<String>,
    pub status: ProgramCallStatus,
    pub cancel_requested: bool,
    pub output_ref: Option<BlobRef>,
    pub error_ref: Option<BlobRef>,
    pub duration_ms: Option<u64>,
    pub admitted_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    /// The workflow the call started: a child session's execution, an
    /// input request's or an approval's.
    pub started_execution_id: Option<String>,
    /// The inbox request the call opened (a question or an approval).
    pub input_request_id: Option<String>,
}

/// How many of a run's indexed calls are in each status.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProgramCallCounts {
    pub pending: u64,
    pub waiting: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub cancelled: u64,
}

impl ProgramCallCounts {
    pub fn total(&self) -> u64 {
        self.pending + self.waiting + self.succeeded + self.failed + self.cancelled
    }

    pub fn add(&mut self, status: ProgramCallStatus, count: u64) {
        let slot = match status {
            ProgramCallStatus::Pending => &mut self.pending,
            ProgramCallStatus::Waiting => &mut self.waiting,
            ProgramCallStatus::Succeeded => &mut self.succeeded,
            ProgramCallStatus::Failed => &mut self.failed,
            ProgramCallStatus::Cancelled => &mut self.cancelled,
        };
        *slot += count;
    }
}

/// What one appended entry changes in the program call index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProgramCallChange {
    /// A program run opened an execution's scope: its calls are indexed,
    /// with each binding's contract name.
    ScopeOpened {
        execution_id: String,
        run_id: RunId,
        contracts: BTreeMap<String, String>,
    },
    Admitted {
        execution_id: String,
        request_id: String,
        run_id: RunId,
        tool_call_id: ToolCallId,
        binding_id: String,
        tool_id: String,
        tool_name: String,
        arguments_ref: BlobRef,
        annotation: Option<String>,
        /// The entry's position: calls list in the order they were admitted.
        seq: u64,
        admitted_at_ms: u64,
    },
    /// The call waits for an approval decision.
    Waiting {
        execution_id: String,
        request_id: String,
    },
    CancelRequested {
        execution_id: String,
        request_id: String,
    },
    Completed {
        execution_id: String,
        request_id: String,
        status: ToolCallStatus,
        output_ref: Option<BlobRef>,
        error_ref: Option<BlobRef>,
        duration_ms: Option<u64>,
        completed_at_ms: u64,
    },
    /// The call started a workflow (a child session, an input request, an
    /// approval), named by its execution id.
    Started {
        tool_call_id: ToolCallId,
        started_execution_id: String,
    },
}

/// The index changes `entry` makes. Entries of other kinds change nothing
/// and are not decoded.
pub fn program_call_changes(
    entry: &StoredSessionEntry,
) -> Result<Vec<ProgramCallChange>, CodecError> {
    let kind = entry.event.kind.as_str();
    if !kind.starts_with("lightspeed.core.code_tool.")
        && kind != "lightspeed.core.workflow_tool.start_requested"
    {
        return Ok(Vec::new());
    }
    let decoded = CoreAgentCodec.decode_entry(entry)?;
    let observed_at_ms = decoded.observed_at_ms;
    let changes = match decoded.event {
        CoreAgentEvent::CodeTool(event) => match event {
            crate::CodeToolEvent::ScopeOpened { scope } => match scope.owner {
                ScopeOwner::Run { run_id } => vec![ProgramCallChange::ScopeOpened {
                    execution_id: scope.execution_id,
                    run_id,
                    contracts: scope
                        .bindings
                        .into_iter()
                        .filter_map(|(binding_id, binding)| {
                            binding
                                .contract
                                .map(|contract| (binding_id, contract.contract))
                        })
                        .collect(),
                }],
                ScopeOwner::ModelInvocation { .. } => Vec::new(),
            },
            crate::CodeToolEvent::CallAdmitted { call, .. } => {
                let (Some(run_id), Some(tool_call_id)) =
                    (decoded.joins.run_id, decoded.joins.tool_call_id)
                else {
                    return Ok(Vec::new());
                };
                vec![ProgramCallChange::Admitted {
                    execution_id: call.origin.execution_id,
                    request_id: call.origin.request_id,
                    run_id,
                    tool_call_id,
                    binding_id: call.binding_id,
                    tool_id: call.tool_id.as_str().to_owned(),
                    tool_name: call.tool_name.as_str().to_owned(),
                    arguments_ref: call.arguments_ref,
                    annotation: call.annotation,
                    seq: decoded.position.seq.as_u64(),
                    admitted_at_ms: observed_at_ms,
                }]
            }
            crate::CodeToolEvent::CallDeferred { origin, .. } => {
                vec![ProgramCallChange::Waiting {
                    execution_id: origin.execution_id,
                    request_id: origin.request_id,
                }]
            }
            crate::CodeToolEvent::CallCancelRequested { origin } => {
                vec![ProgramCallChange::CancelRequested {
                    execution_id: origin.execution_id,
                    request_id: origin.request_id,
                }]
            }
            crate::CodeToolEvent::CallCompleted {
                origin,
                result,
                completed_at_ms,
            } => vec![ProgramCallChange::Completed {
                execution_id: origin.execution_id,
                request_id: origin.request_id,
                status: result.status,
                output_ref: result.output_ref,
                error_ref: result.error_ref,
                duration_ms: result.duration_ms,
                completed_at_ms: completed_at_ms.unwrap_or(observed_at_ms),
            }],
            crate::CodeToolEvent::ScopeClosed { .. } => Vec::new(),
        },
        CoreAgentEvent::WorkflowTool(WorkflowToolEvent::StartRequested {
            invocation,
            execution_id,
        }) => match invocation.context {
            CallContext::Scope { tool_call_id } => vec![ProgramCallChange::Started {
                tool_call_id,
                started_execution_id: execution_id,
            }],
            CallContext::ModelBatch(_) | CallContext::Run {} => Vec::new(),
        },
        _ => Vec::new(),
    };
    Ok(changes)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        CodeToolCallResult, CodeToolCallSpec, CodeToolEvent, CodeToolOrigin, CoreAgentJoins,
        EventSeq, SessionPosition, UncommittedCoreAgentEvent,
    };

    fn stored(seq: u64, joins: CoreAgentJoins, event: CoreAgentEvent) -> StoredSessionEntry {
        let uncommitted = CoreAgentCodec
            .encode_uncommitted(&UncommittedCoreAgentEvent {
                observed_at_ms: 100 + seq,
                joins,
                event,
            })
            .expect("encode");
        StoredSessionEntry {
            position: SessionPosition {
                seq: EventSeq::new(seq),
            },
            observed_at_ms: uncommitted.observed_at_ms,
            joins: uncommitted.joins,
            event: uncommitted.event,
        }
    }

    fn origin() -> CodeToolOrigin {
        CodeToolOrigin {
            execution_id: "wtx:run.0".to_owned(),
            request_id: "call-1".to_owned(),
        }
    }

    #[test]
    fn a_program_scope_and_its_calls_change_the_index() {
        let scope = crate::CodeToolScopeSpec {
            execution_id: "wtx:run.0".to_owned(),
            owner: ScopeOwner::Run {
                run_id: RunId::new(3),
            },
            policy: crate::ScopePolicy::program(crate::ScopeLimits {
                max_calls: 10,
                max_in_flight: 4,
            }),
            bindings: [(
                "b1".to_owned(),
                crate::CodeToolBinding {
                    tool_id: crate::ToolName::new("web.fetch"),
                    tool_name: crate::ToolName::new("web.fetch"),
                    contract: Some(crate::ContractBinding {
                        contract: "web.fetch".to_owned(),
                        digest: crate::contract_digest(b"web.fetch"),
                    }),
                },
            )]
            .into(),
        };
        let opened = stored(
            1,
            CoreAgentJoins::default(),
            CoreAgentEvent::CodeTool(CodeToolEvent::ScopeOpened { scope }),
        );
        assert_eq!(
            program_call_changes(&opened).unwrap(),
            [ProgramCallChange::ScopeOpened {
                execution_id: "wtx:run.0".to_owned(),
                run_id: RunId::new(3),
                contracts: [("b1".to_owned(), "web.fetch".to_owned())].into(),
            }]
        );

        let call_id = ToolCallId::new("code-tool:abc");
        let joins = CoreAgentJoins {
            run_id: Some(RunId::new(3)),
            tool_call_id: Some(call_id.clone()),
            ..CoreAgentJoins::default()
        };
        let arguments_ref = BlobRef::from_bytes(b"{}");
        let admitted = stored(
            2,
            joins.clone(),
            CoreAgentEvent::CodeTool(CodeToolEvent::CallAdmitted {
                call: CodeToolCallSpec {
                    origin: origin(),
                    binding_id: "b1".to_owned(),
                    tool_id: crate::ToolName::new("web.fetch"),
                    tool_name: crate::ToolName::new("web.fetch"),
                    arguments_ref: arguments_ref.clone(),
                    annotation: Some("the claim cites it".to_owned()),
                },
                promise_id_base: 1,
                acknowledged: None,
            }),
        );
        assert_eq!(
            program_call_changes(&admitted).unwrap(),
            [ProgramCallChange::Admitted {
                execution_id: "wtx:run.0".to_owned(),
                request_id: "call-1".to_owned(),
                run_id: RunId::new(3),
                tool_call_id: call_id.clone(),
                binding_id: "b1".to_owned(),
                tool_id: "web.fetch".to_owned(),
                tool_name: "web.fetch".to_owned(),
                arguments_ref,
                annotation: Some("the claim cites it".to_owned()),
                seq: 2,
                admitted_at_ms: 102,
            }]
        );

        let output_ref = BlobRef::from_bytes(b"\"page\"");
        let result: CodeToolCallResult = serde_json::from_value(json!({
            "call_id": call_id.as_str(),
            "status": "succeeded",
            "output_ref": output_ref.as_str(),
            "error_ref": null,
            "duration_ms": 40,
        }))
        .expect("result");
        let completed = stored(
            3,
            joins,
            CoreAgentEvent::CodeTool(CodeToolEvent::CallCompleted {
                origin: origin(),
                result,
                completed_at_ms: Some(150),
            }),
        );
        assert_eq!(
            program_call_changes(&completed).unwrap(),
            [ProgramCallChange::Completed {
                execution_id: "wtx:run.0".to_owned(),
                request_id: "call-1".to_owned(),
                status: ToolCallStatus::Succeeded,
                output_ref: Some(output_ref),
                error_ref: None,
                duration_ms: Some(40),
                completed_at_ms: 150,
            }]
        );
    }

    #[test]
    fn entries_of_other_kinds_change_nothing() {
        let closed = stored(
            1,
            CoreAgentJoins::default(),
            CoreAgentEvent::CodeTool(CodeToolEvent::ScopeClosed {
                execution_id: "wtx:run.0".to_owned(),
                cancel: false,
                program_context: None,
                withdrawn: Vec::new(),
                acknowledged: None,
            }),
        );
        assert!(program_call_changes(&closed).unwrap().is_empty());
        let started = stored(
            2,
            CoreAgentJoins::default(),
            CoreAgentEvent::Lifecycle(crate::CoreAgentLifecycleEvent::Closed),
        );
        assert!(program_call_changes(&started).unwrap().is_empty());
    }
}
