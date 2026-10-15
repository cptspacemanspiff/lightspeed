//! Activities of the input-request workflow: open a program's question in
//! the universe's inbox, and settle it from the inbox row.

use api::{Attribution, InputRequestStatus};
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
    DEADLINE_EXCEEDED_CODE, INPUT_CANCELLED_CODE, InputAnswer, InputRequestArgs, InputTimeouts,
    failure_document,
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
    let args = match read_args(store, timeouts, &start.invocation.arguments_ref).await? {
        Ok(args) => args,
        Err(message) => {
            return Ok(InputRequestOpenActivityResult::Rejected {
                error_ref: put_failure(store, "invalid_arguments", &message).await?,
            });
        }
    };
    let deadline_after_ms =
        args.deadline_after_ms(program_input_deadline(store, &start).await?, timeouts);
    let now = now_ms()?;
    let record = store
        .open_input_request(NewInputRequest {
            request_id: input_request_id(&start.execution_id),
            execution_id: start.execution_id.clone(),
            session_id: start.invocation.session_id.as_str().to_owned(),
            run_id: start.invocation.run_id.as_u64(),
            ask: args.ask,
            payload: args.payload,
            response_schema: args.response_schema,
            assignees: args.assignees,
            escalate_to: args.escalate_to.clone(),
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
        escalates: !args.escalate_to.is_empty(),
    })
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

/// The program's default deadline for input requests, from its pin.
async fn program_input_deadline(
    store: &PgStore,
    start: &WorkflowToolStartArgs,
) -> Result<Option<u64>, ActivityError> {
    let record = store
        .load_session(&start.invocation.session_id)
        .await
        .map_err(activity_error)?
        .ok_or_else(|| activity_error(anyhow::anyhow!("input request session is missing")))?;
    let loaded = crate::checkpoint::load_reduction(store, store, &record)
        .await
        .map_err(|error| activity_error(anyhow::anyhow!(error.to_string())))?;
    Ok(loaded
        .reduced
        .core_state
        .lifecycle
        .driver
        .program()
        .and_then(|program| program.limits.input_deadline_ms))
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
