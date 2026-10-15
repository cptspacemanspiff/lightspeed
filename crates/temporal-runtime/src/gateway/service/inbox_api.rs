//! The human-input inbox: questions program runs are waiting on, and the
//! answers that complete them.
use super::*;

const DEFAULT_LIST_LIMIT: u32 = 50;
const MAX_LIST_LIMIT: u32 = 200;

fn map_store_error(error: store_pg::InputRequestStoreError) -> AgentApiError {
    match error {
        store_pg::InputRequestStoreError::NotFound(id) => {
            AgentApiError::not_found(format!("input request not found: {id}"))
        }
        store_pg::InputRequestStoreError::Settled(record) => AgentApiError::conflict(format!(
            "input request {} is already {}",
            record.view.request_id,
            record.view.status.as_str()
        )),
        other => AgentApiError::internal(other.to_string()),
    }
}

impl GatewayAgentApi {
    pub(super) async fn list_inbox_requests_impl(
        &self,
        params: api::InboxRequestListParams,
    ) -> Result<AgentApiOutcome<api::InboxRequestListResponse>, AgentApiError> {
        self.authorize_method(api::METHOD_INBOX_REQUESTS_LIST, None)
            .await?;
        let limit = params.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        if limit == 0 || limit > MAX_LIST_LIMIT {
            return Err(AgentApiError::invalid_request(format!(
                "limit must be between 1 and {MAX_LIST_LIMIT}"
            )));
        }
        let filter = self.access_filter(store_pg::AccessFilter {
            visible_to: None,
            within_root: None,
        });
        let requests = self
            .store
            .list_input_requests(params.status, params.session.as_deref(), limit, &filter)
            .await
            .map_err(map_store_error)?
            .into_iter()
            .map(|record| record.view)
            .collect();
        Ok(AgentApiOutcome::new(api::InboxRequestListResponse {
            requests,
        }))
    }

    pub(super) async fn read_inbox_request_impl(
        &self,
        params: api::InboxRequestReadParams,
    ) -> Result<AgentApiOutcome<api::InboxRequestResponse>, AgentApiError> {
        let record = self.input_request(&params.request_id).await?;
        self.authorize_method(
            api::METHOD_INBOX_REQUESTS_READ,
            record.as_ref().map(request_session),
        )
        .await?;
        let record = found(record, &params.request_id)?;
        Ok(AgentApiOutcome::new(api::InboxRequestResponse {
            request: record.view,
        }))
    }

    pub(super) async fn answer_inbox_request_impl(
        &self,
        params: api::InboxRequestAnswerParams,
    ) -> Result<AgentApiOutcome<api::InboxRequestResponse>, AgentApiError> {
        let record = self.input_request(&params.request_id).await?;
        self.authorize_method(
            api::METHOD_INBOX_REQUESTS_ANSWER,
            record.as_ref().map(request_session),
        )
        .await?;
        let record = found(record, &params.request_id)?;
        tools::human_input::validate_decision(
            record.view.response_schema.as_ref(),
            params.approved,
            params.value.as_ref(),
            params.comment.as_deref(),
        )
        .map_err(|error| AgentApiError::invalid_request(error.to_string()))?;
        let answer = api::InputRequestAnswer {
            approved: params.approved,
            value: params.value,
            comment: params.comment,
            answered_by: self.attribution()?,
            answered_at_ms: now_unix_ms(),
        };
        let answered = match self
            .store
            .answer_input_request(&params.request_id, &answer)
            .await
        {
            Ok(record) => record,
            // A retried answer is accepted again, and re-delivered.
            Err(store_pg::InputRequestStoreError::Settled(existing))
                if existing.view.answer.as_ref().is_some_and(|recorded| {
                    recorded.approved == answer.approved
                        && recorded.value == answer.value
                        && recorded.comment == answer.comment
                        && recorded.answered_by == answer.answered_by
                }) =>
            {
                *existing
            }
            Err(error) => return Err(map_store_error(error)),
        };
        self.nudge_input_request(&answered.execution_id).await?;
        Ok(AgentApiOutcome::new(api::InboxRequestResponse {
            request: answered.view,
        }))
    }

    /// A request, which the caller must then be authorized on through its
    /// session before anything about it, even its absence, is revealed.
    async fn input_request(
        &self,
        request_id: &str,
    ) -> Result<Option<store_pg::InputRequestRecord>, AgentApiError> {
        self.store
            .read_input_request(request_id)
            .await
            .map_err(map_store_error)
    }

    /// Wake the request's workflow; it reads the answer from the inbox. A
    /// workflow that has already finished has nothing left to wake.
    async fn nudge_input_request(&self, execution_id: &str) -> Result<(), AgentApiError> {
        match self
            .client
            .get_workflow_handle::<temporal_workflow::InputRequestWorkflow>(execution_id)
            .signal(
                temporal_workflow::InputRequestWorkflow::answered,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
        {
            Ok(()) | Err(WorkflowInteractionError::NotFound(_)) => Ok(()),
            Err(error) => Err(map_workflow_interaction_error(error)),
        }
    }
}

fn request_session(record: &store_pg::InputRequestRecord) -> ResourceRef {
    ResourceRef::Session(record.view.session_id.clone())
}

fn found(
    record: Option<store_pg::InputRequestRecord>,
    request_id: &str,
) -> Result<store_pg::InputRequestRecord, AgentApiError> {
    record.ok_or_else(|| {
        map_store_error(store_pg::InputRequestStoreError::NotFound(
            request_id.to_owned(),
        ))
    })
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
