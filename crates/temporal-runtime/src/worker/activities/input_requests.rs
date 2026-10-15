//! Activities of the input-request workflow: open a program's question, or
//! the approval of a call its scope parked, in the universe's inbox; settle
//! it from the inbox row; and run an approved call.

use api::{Attribution, InputRequestKind, InputRequestStatus};
use harness::{
    BlobRef, PromiseResolution,
    storage::{BlobStore, SessionStore},
};
use store_pg::{InputRequestClosure, InputRequestRecord, NewInputRequest, PgStore};
use temporal_workflow::{
    InputRequestOpenActivityRequest, InputRequestOpenActivityResult, InputRequestStep,
    InputRequestStepActivityRequest, InputRequestStepResult, WorkflowToolStartArgs,
};
use temporalio_sdk::activities::ActivityError;
use tools::human_input::{
    APPROVAL_REJECTED_CODE, ApprovalSettings, DEADLINE_EXCEEDED_CODE, INPUT_CANCELLED_CODE,
    InputAnswer, InputRequestArgs, InputTimeouts, MAX_PAYLOAD_BYTES, failure_document,
};

use super::common::activity_error;

/// The inbox id of the request an execution owns.
pub(crate) fn input_request_id(execution_id: &str) -> String {
    let digest = execution_id.rsplit(':').next().unwrap_or(execution_id);
    format!("inp_{}", &digest[..digest.len().min(32)])
}

pub(super) async fn open(
    store: &PgStore,
    timeouts: &InputTimeouts,
    request: InputRequestOpenActivityRequest,
) -> Result<InputRequestOpenActivityResult, ActivityError> {
    let start = request.start;
    validate_start(&start)?;
    if is_approval(&start) {
        return open_approval(store, timeouts, start).await;
    }
    let args = match read_args(store, timeouts, &start.invocation.arguments_ref).await? {
        Ok(args) => args,
        Err(message) => {
            return Ok(InputRequestOpenActivityResult::Rejected {
                error_ref: put_failure(store, "invalid_arguments", &message).await?,
            });
        }
    };
    let settings = program_input_settings(store, &start).await?;
    let deadline_after_ms = args.deadline_after_ms(settings.deadline_ms, timeouts);
    // The call's own escalation list wins, else the run's default.
    let escalate_to = if args.escalate_to.is_empty() {
        settings.escalate_to
    } else {
        args.escalate_to
    };
    let now = now_ms()?;
    let record = store
        .open_input_request(NewInputRequest {
            request_id: input_request_id(&start.execution_id),
            kind: InputRequestKind::Input,
            execution_id: start.execution_id.clone(),
            session_id: start.invocation.session_id.as_str().to_owned(),
            run_id: start.invocation.run_id.as_u64(),
            ask: args.ask,
            payload: args.payload,
            response_schema: args.response_schema,
            assignees: args.assignees,
            escalate_to: escalate_to.clone(),
            created_at_ms: now,
            deadline_at_ms: now.saturating_add(deadline_after_ms),
            remind_after_ms: args.remind_after_ms,
        })
        .await
        .map_err(activity_error)?;
    Ok(InputRequestOpenActivityResult::Opened {
        request_id: record.view.request_id,
        deadline_at_ms: record.view.deadline_at_ms,
        deadline_after_ms,
        remind_after_ms: record.view.remind_after_ms,
        escalates: !escalate_to.is_empty(),
    })
}

/// Open the approval of a parked call. Everything the inbox shows comes
/// from the session: the call's server, tool and arguments, and the
/// program's and run's approval settings.
async fn open_approval(
    store: &PgStore,
    timeouts: &InputTimeouts,
    start: WorkflowToolStartArgs,
) -> Result<InputRequestOpenActivityResult, ActivityError> {
    let state = session_state(store, &start.invocation.session_id).await?;
    let parked = start
        .invocation
        .context
        .tool_call_id()
        .and_then(|call_id| harness::code_tool_call_for_id(&state, call_id))
        .and_then(|(_, call)| {
            let harness::ToolKind::RemoteMcp(spec) =
                &state.tooling.tools.get(&call.spec.tool_id)?.kind
            else {
                return None;
            };
            let remote = call
                .spec
                .tool_name
                .as_str()
                .strip_prefix(&format!("{}__", call.spec.tool_id))?
                .to_owned();
            Some((spec.clone(), remote, call.spec.arguments_ref.clone()))
        });
    let Some((spec, remote, arguments_ref)) = parked else {
        return Ok(InputRequestOpenActivityResult::Rejected {
            error_ref: put_failure(
                store,
                "invalid_arguments",
                "the approval's parked call is not a remote MCP call of this session",
            )
            .await?,
        });
    };
    let mut arguments: serde_json::Value = serde_json::from_slice(
        &store
            .read_bytes(&arguments_ref)
            .await
            .map_err(activity_error)?,
    )
    .map_err(activity_error)?;
    // The arguments are shown whole unless they would overflow the payload.
    let shown = |arguments: &serde_json::Value| {
        serde_json::json!({
            "server": spec.server_id,
            "serverLabel": spec.server_label,
            "tool": remote,
            "arguments": arguments,
        })
    };
    if serde_json::to_vec(&shown(&arguments))
        .map_err(activity_error)?
        .len()
        > MAX_PAYLOAD_BYTES
    {
        arguments = serde_json::json!({
            "truncated": true,
            "argumentsRef": arguments_ref.as_str(),
        });
    }
    let settings = ApprovalSettings::from_program(
        state
            .lifecycle
            .driver
            .program()
            .and_then(|program| program.approvals.as_deref()),
        run_input_defaults(&state, start.invocation.run_id),
    );
    let deadline_after_ms = settings.deadline_after_ms(timeouts);
    let now = now_ms()?;
    let record = store
        .open_input_request(NewInputRequest {
            request_id: input_request_id(&start.execution_id),
            kind: InputRequestKind::Approval,
            execution_id: start.execution_id.clone(),
            session_id: start.invocation.session_id.as_str().to_owned(),
            run_id: start.invocation.run_id.as_u64(),
            ask: format!(
                "Allow the program to call {remote} on {}?",
                spec.server_label
            ),
            payload: Some(shown(&arguments)),
            response_schema: None,
            assignees: settings.assignees,
            escalate_to: settings.escalate_to.clone(),
            created_at_ms: now,
            deadline_at_ms: now.saturating_add(deadline_after_ms),
            remind_after_ms: settings.remind_after_ms,
        })
        .await
        .map_err(activity_error)?;
    Ok(InputRequestOpenActivityResult::Opened {
        request_id: record.view.request_id,
        deadline_at_ms: record.view.deadline_at_ms,
        deadline_after_ms,
        remind_after_ms: record.view.remind_after_ms,
        escalates: !settings.escalate_to.is_empty(),
    })
}

/// Run the call an approval request parked, once, with the approval: the
/// call's outcome is the approval's reply. A call that is no longer waiting
/// on this approval (its run ended, its scope closed) fails instead.
pub(super) async fn execute(
    store: &PgStore,
    tools: &super::ToolActivityDeps,
    request: temporal_workflow::InputRequestExecuteActivityRequest,
) -> Result<PromiseResolution, ActivityError> {
    let start = request.start;
    validate_start(&start)?;
    if !is_approval(&start) {
        return Err(activity_error(anyhow::anyhow!(
            "only an approval request runs a parked call"
        )));
    }
    let state = session_state(store, &start.invocation.session_id).await?;
    let call_id = start
        .invocation
        .context
        .tool_call_id()
        .ok_or_else(|| activity_error(anyhow::anyhow!("approval names no call")))?;
    let call_request = match harness::approved_code_tool_request(
        &start.invocation.session_id,
        &state,
        call_id,
        &start.invocation.invocation_id,
    ) {
        Ok(request) => request,
        Err(error) => {
            return failed(store, "approval_unavailable", &error.to_string()).await;
        }
    };
    let outcome = super::tools::invoke_call(
        tools,
        temporal_workflow::ToolInvokeCallActivityRequest {
            request: call_request,
        },
    )
    .await?;
    match outcome {
        temporal_workflow::ToolInvokeCallActivityResult::Completed { result }
            if result.status == harness::ToolCallStatus::Succeeded =>
        {
            Ok(PromiseResolution::Resolved {
                payload_ref: result.output_ref,
            })
        }
        temporal_workflow::ToolInvokeCallActivityResult::Completed { result } => {
            match result.error_ref {
                Some(error_ref) => Ok(PromiseResolution::Failed {
                    error_ref: Some(error_ref),
                }),
                None => failed(store, "tool_failed", "the approved call failed").await,
            }
        }
        temporal_workflow::ToolInvokeCallActivityResult::NeedsApproval { .. } => {
            failed(
                store,
                "approval_unavailable",
                "the server asked again for approval of an approved call",
            )
            .await
        }
        temporal_workflow::ToolInvokeCallActivityResult::EnvironmentNotReady { .. } => {
            failed(
                store,
                "tool_failed",
                "the approved call's environment is not ready",
            )
            .await
        }
    }
}

async fn failed(
    store: &PgStore,
    code: &str,
    message: &str,
) -> Result<PromiseResolution, ActivityError> {
    Ok(PromiseResolution::Failed {
        error_ref: Some(put_failure(store, code, message).await?),
    })
}

fn is_approval(start: &WorkflowToolStartArgs) -> bool {
    tools::human_input::is_approval_binding(
        start.invocation.tool_id.as_str(),
        &start.invocation.semantic_type,
    )
}

pub(super) async fn step(
    store: &PgStore,
    request: InputRequestStepActivityRequest,
) -> Result<InputRequestStepResult, ActivityError> {
    if request.universe_id != store.config().universe_id {
        return Err(activity_error(anyhow::anyhow!(
            "input request step for another universe"
        )));
    }
    let execution_id = request.execution_id.as_str();
    let record = match request.step {
        InputRequestStep::Check => store
            .read_input_request_by_execution(execution_id)
            .await
            .map_err(activity_error)?
            .ok_or_else(|| activity_error(anyhow::anyhow!("input request is not open")))?,
        InputRequestStep::Remind { at_ms, .. } => store
            .remind_input_request(execution_id, at_ms)
            .await
            .map_err(activity_error)?,
        InputRequestStep::Deadline {
            at_ms,
            escalate_deadline_at_ms,
        } => {
            let escalated = match escalate_deadline_at_ms {
                Some(deadline_at_ms) => Some(
                    store
                        .escalate_input_request(execution_id, deadline_at_ms)
                        .await
                        .map_err(activity_error)?,
                ),
                None => None,
            };
            match escalated {
                // Escalated now (or answered meanwhile): keep its state.
                Some(record)
                    if record.view.status != InputRequestStatus::Pending
                        || record.view.deadline_at_ms > at_ms =>
                {
                    record
                }
                _ => store
                    .close_input_request(execution_id, InputRequestClosure::Expired, at_ms)
                    .await
                    .map_err(activity_error)?,
            }
        }
        InputRequestStep::Cancel { at_ms } => store
            .close_input_request(execution_id, InputRequestClosure::Cancelled, at_ms)
            .await
            .map_err(activity_error)?,
    };
    settle(store, record).await
}

async fn settle(
    store: &PgStore,
    record: InputRequestRecord,
) -> Result<InputRequestStepResult, ActivityError> {
    let view = record.view;
    let resolution = match view.status {
        InputRequestStatus::Pending => {
            return Ok(InputRequestStepResult::Pending {
                deadline_at_ms: view.deadline_at_ms,
                escalated: view.escalated,
            });
        }
        InputRequestStatus::Answered => {
            let answer = view
                .answer
                .ok_or_else(|| activity_error(anyhow::anyhow!("answered request has no answer")))?;
            if view.kind == InputRequestKind::Approval {
                if answer.approved {
                    return Ok(InputRequestStepResult::Approved);
                }
                let by = attribution_label(&answer.answered_by);
                let message = match &answer.comment {
                    Some(comment) => format!("{by} rejected the call: {comment}"),
                    None => format!("{by} rejected the call"),
                };
                return Ok(InputRequestStepResult::Settled {
                    resolution: PromiseResolution::Failed {
                        error_ref: Some(
                            put_failure(store, APPROVAL_REJECTED_CODE, &message).await?,
                        ),
                    },
                });
            }
            let reply = InputAnswer {
                approved: answer.approved,
                value: answer.value,
                comment: answer.comment,
                answered_by: attribution_label(&answer.answered_by),
                answered_at_ms: answer.answered_at_ms,
            };
            PromiseResolution::Resolved {
                payload_ref: Some(
                    store
                        .put_bytes(serde_json::to_vec(&reply).map_err(activity_error)?)
                        .await
                        .map_err(activity_error)?,
                ),
            }
        }
        InputRequestStatus::Expired => PromiseResolution::Failed {
            error_ref: Some(
                put_failure(
                    store,
                    DEADLINE_EXCEEDED_CODE,
                    &format!("input request {} was not answered in time", view.request_id),
                )
                .await?,
            ),
        },
        InputRequestStatus::Cancelled => PromiseResolution::Failed {
            error_ref: Some(
                put_failure(
                    store,
                    INPUT_CANCELLED_CODE,
                    &format!("input request {} was cancelled", view.request_id),
                )
                .await?,
            ),
        },
    };
    Ok(InputRequestStepResult::Settled { resolution })
}

/// How an answer names who gave it, for the program.
pub(crate) fn attribution_label(attribution: &Attribution) -> String {
    match attribution {
        Attribution::Actor { id } => id.clone(),
        Attribution::Key { prefix } => format!("key:{prefix}"),
        Attribution::Local => "local".to_owned(),
        Attribution::Internal { component, .. } => format!("internal:{component}"),
    }
}

fn validate_start(start: &WorkflowToolStartArgs) -> Result<(), ActivityError> {
    if start.universe_id != start.invocation.session_universe_id
        || !tools::human_input::is_input_request_binding(
            start.invocation.tool_id.as_str(),
            &start.invocation.semantic_type,
        )
        || !matches!(start.invocation.context, harness::CallContext::Scope { .. })
    {
        return Err(activity_error(anyhow::anyhow!(
            "workflow invocation is not a program's input request"
        )));
    }
    Ok(())
}

async fn read_args(
    store: &PgStore,
    timeouts: &InputTimeouts,
    arguments_ref: &BlobRef,
) -> Result<Result<InputRequestArgs, String>, ActivityError> {
    let bytes = store
        .read_bytes(arguments_ref)
        .await
        .map_err(activity_error)?;
    Ok(serde_json::from_slice::<InputRequestArgs>(&bytes)
        .map_err(|error| format!("invalid input request: {error}"))
        .and_then(|args| {
            args.validate(timeouts)
                .map(|()| args)
                .map_err(|error| error.to_string())
        }))
}

/// What a request leaves out falls back on: the program's deadline from
/// its pin, and the escalation list its run was started with.
struct ProgramInputSettings {
    deadline_ms: Option<u64>,
    escalate_to: Vec<String>,
}

async fn program_input_settings(
    store: &PgStore,
    start: &WorkflowToolStartArgs,
) -> Result<ProgramInputSettings, ActivityError> {
    let state = &session_state(store, &start.invocation.session_id).await?;
    let escalate_to = run_input_defaults(state, start.invocation.run_id)
        .map(|defaults| defaults.escalate_to.clone())
        .unwrap_or_default();
    Ok(ProgramInputSettings {
        deadline_ms: state
            .lifecycle
            .driver
            .program()
            .and_then(|program| program.limits.input_deadline_ms),
        escalate_to,
    })
}

async fn session_state(
    store: &PgStore,
    session_id: &harness::SessionId,
) -> Result<harness::CoreAgentState, ActivityError> {
    let record = store
        .load_session(session_id)
        .await
        .map_err(activity_error)?
        .ok_or_else(|| activity_error(anyhow::anyhow!("input request session is missing")))?;
    let loaded = crate::checkpoint::load_reduction(store, store, &record)
        .await
        .map_err(|error| activity_error(anyhow::anyhow!(error.to_string())))?;
    Ok(loaded.reduced.core_state)
}

/// The defaults the run's starter chose for its requests.
fn run_input_defaults(
    state: &harness::CoreAgentState,
    run_id: harness::RunId,
) -> Option<&harness::ProgramInputDefaults> {
    state
        .runs
        .active
        .as_ref()
        .filter(|run| run.run_id == run_id)
        .and_then(|run| match &run.source {
            harness::RunSource::Program { input_defaults, .. } => input_defaults.as_ref(),
            harness::RunSource::Input { .. } => None,
        })
}

async fn put_failure(store: &PgStore, code: &str, message: &str) -> Result<BlobRef, ActivityError> {
    store
        .put_bytes(serde_json::to_vec(&failure_document(code, message)).map_err(activity_error)?)
        .await
        .map_err(activity_error)
}

fn now_ms() -> Result<u64, ActivityError> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(activity_error)?
        .as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_derive_from_the_execution() {
        let execution = format!("wtx:sha256:{}", "ab".repeat(32));
        assert_eq!(
            input_request_id(&execution),
            format!("inp_{}", "ab".repeat(16))
        );
    }
}
