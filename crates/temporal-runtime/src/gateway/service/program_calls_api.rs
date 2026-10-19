//! A program run's calls, a page at a time from the store's program call
//! index, with previews of what went in and came out and links to what each
//! call started.
use harness::storage::program_calls::{ProgramCallCounts, ProgramCallRecord, ProgramCallStatus};

use super::*;

const DEFAULT_CALL_LIMIT: u32 = 50;
const MAX_CALL_LIMIT: u32 = 200;

impl GatewayAgentApi {
    pub(super) async fn list_run_calls_impl(
        &self,
        params: api::RunCallsListParams,
    ) -> Result<AgentApiOutcome<api::RunCallsListResponse>, AgentApiError> {
        self.authorize_method(
            api::METHOD_SESSION_RUNS_CALLS_LIST,
            Some(ResourceRef::Session(params.session_id.clone())),
        )
        .await?;
        let session_id = SessionId::try_new(params.session_id).map_err(|error| {
            AgentApiError::invalid_request(format!("invalid session id: {error}"))
        })?;
        let run_id = api_projection::parse_api_run_id(&params.run_id)?;
        let limit = params.limit.unwrap_or(DEFAULT_CALL_LIMIT);
        if limit == 0 || limit > MAX_CALL_LIMIT {
            return Err(AgentApiError::invalid_request(format!(
                "limit must be between 1 and {MAX_CALL_LIMIT}"
            )));
        }
        let after = params
            .cursor
            .as_deref()
            .map(|cursor| {
                cursor
                    .parse::<u64>()
                    .map_err(|_| AgentApiError::invalid_request("invalid call cursor"))
            })
            .transpose()?;
        let status = params.status.map(|status| match status {
            api::ProgramCallStatusView::Pending => ProgramCallStatus::Pending,
            api::ProgramCallStatusView::Waiting => ProgramCallStatus::Waiting,
            api::ProgramCallStatusView::Succeeded => ProgramCallStatus::Succeeded,
            api::ProgramCallStatusView::Failed => ProgramCallStatus::Failed,
            api::ProgramCallStatusView::Cancelled => ProgramCallStatus::Cancelled,
        });
        let mut records = self
            .store
            .list_program_calls(&session_id, run_id, after, status, limit + 1)
            .await
            .map_err(api_projection::map_session_store_error)?;
        let next_cursor = (records.len() > limit as usize).then(|| {
            records.truncate(limit as usize);
            records
                .last()
                .expect("a full page has a last call")
                .seq
                .to_string()
        });
        let counts = self
            .store
            .count_program_calls(&session_id, run_id)
            .await
            .map_err(api_projection::map_session_store_error)?;
        let mut calls = futures::future::try_join_all(
            records.iter().map(|record| self.program_call_view(record)),
        )
        .await?;
        // A refused child's workflow started but never created its session.
        let children = calls
            .iter()
            .filter_map(|call| call.child_session_id.clone())
            .collect::<Vec<_>>();
        let existing = self
            .store
            .existing_session_ids(&children)
            .await
            .map_err(api_projection::map_session_store_error)?;
        for call in &mut calls {
            call.child_session_id = call
                .child_session_id
                .take()
                .filter(|child| existing.contains(child));
        }
        Ok(AgentApiOutcome::new(api::RunCallsListResponse {
            calls,
            next_cursor,
            counts: program_call_counts_view(counts),
        }))
    }

    async fn program_call_view(
        &self,
        record: &ProgramCallRecord,
    ) -> Result<api::ProgramCallView, AgentApiError> {
        let child_session_id = record
            .started_execution_id
            .as_deref()
            .filter(|_| starts_a_child(&record.tool_id))
            .map(|execution_id| {
                crate::subagents::child_session_id(execution_id)
                    .as_str()
                    .to_owned()
            });
        Ok(api::ProgramCallView {
            id: record.tool_call_id.as_str().to_owned(),
            execution: harness::program_scope_execution(&record.execution_id).1,
            request_id: record.request_id.clone(),
            contract: record.contract.clone(),
            tool: record.tool_name.clone(),
            annotation: record.annotation.clone(),
            status: match record.status {
                ProgramCallStatus::Pending => api::ProgramCallStatusView::Pending,
                ProgramCallStatus::Waiting => api::ProgramCallStatusView::Waiting,
                ProgramCallStatus::Succeeded => api::ProgramCallStatusView::Succeeded,
                ProgramCallStatus::Failed => api::ProgramCallStatusView::Failed,
                ProgramCallStatus::Cancelled => api::ProgramCallStatusView::Cancelled,
            },
            cancel_requested: record.cancel_requested,
            arguments: self.content_preview(&record.arguments_ref).await?,
            output: match &record.output_ref {
                Some(output_ref) => Some(self.content_preview(output_ref).await?),
                None => None,
            },
            error: match &record.error_ref {
                Some(error_ref) => Some(self.content_preview(error_ref).await?),
                None => None,
            },
            admitted_at_ms: record.admitted_at_ms,
            completed_at_ms: record.completed_at_ms,
            duration_ms: record.duration_ms,
            child_session_id,
            input_request_id: record.input_request_id.clone(),
        })
    }

    /// The start of a stored value's text, cut at a character boundary.
    async fn content_preview(
        &self,
        blob_ref: &BlobRef,
    ) -> Result<api::ContentPreviewView, AgentApiError> {
        let bytes = self
            .store
            .read_bytes(blob_ref)
            .await
            .map_err(map_blob_store_error)?;
        let truncated = bytes.len() > api::CONTENT_PREVIEW_BYTES;
        let mut preview =
            String::from_utf8_lossy(&bytes[..bytes.len().min(api::CONTENT_PREVIEW_BYTES)])
                .into_owned();
        if truncated && preview.ends_with(char::REPLACEMENT_CHARACTER) {
            preview.pop();
        }
        Ok(api::ContentPreviewView {
            content_ref: blob_ref.as_str().to_owned(),
            preview,
            preview_truncated: truncated,
        })
    }
}

/// Whether a call to the session tool `tool_id` starts a child session.
fn starts_a_child(tool_id: &str) -> bool {
    tools::subagents::SubagentToolKind::AGENTS
        .into_iter()
        .chain(tools::subagents::SubagentToolKind::PROGRAMS)
        .any(|kind| kind.tool_id() == tool_id)
}

fn program_call_counts_view(counts: ProgramCallCounts) -> api::ProgramCallCountsView {
    api::ProgramCallCountsView {
        total: counts.total(),
        pending: counts.pending,
        waiting: counts.waiting,
        succeeded: counts.succeeded,
        failed: counts.failed,
        cancelled: counts.cancelled,
    }
}
