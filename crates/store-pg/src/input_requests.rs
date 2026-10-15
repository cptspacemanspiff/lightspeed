//! The universe's human-input inbox. The input-request workflow opens a
//! row per request and settles it; the inbox API records answers. Every
//! transition is guarded on `pending`, so an answer and an expiry cannot
//! both win.

use api::{Attribution, InputRequestAnswer, InputRequestStatus, InputRequestView};
use serde_json::Value;
use sqlx::Row;
use thiserror::Error;

use crate::PgStore;

#[derive(Debug, Error)]
pub enum InputRequestStoreError {
    #[error("input request {0} not found")]
    NotFound(String),
    /// The request was settled before this change; it carries the record.
    #[error("input request {} is already {}", .0.view.request_id, .0.view.status.as_str())]
    Settled(Box<InputRequestRecord>),
    #[error("input request storage failure: {0}")]
    Postgres(#[from] sqlx::Error),
    #[error("invalid stored input request: {0}")]
    Decode(String),
}

/// A stored request: what the inbox shows, and the workflow that owns it.
#[derive(Clone, Debug, PartialEq)]
pub struct InputRequestRecord {
    pub view: InputRequestView,
    pub execution_id: String,
}

/// A request as its workflow opens it.
#[derive(Clone, Debug, PartialEq)]
pub struct NewInputRequest {
    pub request_id: String,
    pub execution_id: String,
    pub session_id: String,
    pub run_id: u64,
    pub ask: String,
    pub payload: Option<Value>,
    pub response_schema: Option<Value>,
    pub assignees: Vec<String>,
    pub escalate_to: Vec<String>,
    pub created_at_ms: u64,
    pub deadline_at_ms: u64,
    pub remind_after_ms: Option<u64>,
}

/// How a pending request closes without an answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputRequestClosure {
    Expired,
    Cancelled,
}

const COLUMNS: &str = "i.request_id, i.execution_id, i.session_id, i.run_id, i.ask, i.payload,
    i.response_schema, i.assignees, i.escalate_to, i.escalated, i.status, i.created_at_ms,
    i.deadline_at_ms, i.remind_after_ms, i.reminder_count, i.last_reminded_at_ms, i.approved,
    i.answer_value, i.answer_comment, i.answered_by, i.closed_at_ms";

impl PgStore {
    /// Record a request once: a retried open returns the stored row.
    pub async fn open_input_request(
        &self,
        request: NewInputRequest,
    ) -> Result<InputRequestRecord, InputRequestStoreError> {
        sqlx::query(
            "INSERT INTO input_requests (universe_id, request_id, execution_id, session_id, run_id,
                 ask, payload, response_schema, assignees, escalate_to, status, created_at_ms,
                 deadline_at_ms, remind_after_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending', $11, $12, $13)
             ON CONFLICT DO NOTHING",
        )
        .bind(self.config.universe_id)
        .bind(&request.request_id)
        .bind(&request.execution_id)
        .bind(&request.session_id)
        .bind(to_i64(request.run_id)?)
        .bind(&request.ask)
        .bind(&request.payload)
        .bind(&request.response_schema)
        .bind(Value::from(request.assignees.clone()))
        .bind(Value::from(request.escalate_to.clone()))
        .bind(to_i64(request.created_at_ms)?)
        .bind(to_i64(request.deadline_at_ms)?)
        .bind(request.remind_after_ms.map(to_i64).transpose()?)
        .execute(&self.pool)
        .await?;
        let record = self
            .read_input_request_by_execution(&request.execution_id)
            .await?
            .ok_or_else(|| InputRequestStoreError::NotFound(request.request_id.clone()))?;
        if record.view.request_id != request.request_id {
            return Err(InputRequestStoreError::Decode(format!(
                "execution {} already owns request {}",
                request.execution_id, record.view.request_id
            )));
        }
        Ok(record)
    }

    pub async fn read_input_request(
        &self,
        request_id: &str,
    ) -> Result<Option<InputRequestRecord>, InputRequestStoreError> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM input_requests i WHERE i.universe_id = $1 AND i.request_id = $2"
        )))
        .bind(self.config.universe_id)
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(decode).transpose()
    }

    pub async fn read_input_request_by_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<InputRequestRecord>, InputRequestStoreError> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM input_requests i WHERE i.universe_id = $1 AND i.execution_id = $2"
        )))
        .bind(self.config.universe_id)
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(decode).transpose()
    }

    /// Requests newest first, limited to sessions the filter admits.
    pub async fn list_input_requests(
        &self,
        status: Option<InputRequestStatus>,
        session_id: Option<&str>,
        limit: u32,
        filter: &crate::AccessFilter,
    ) -> Result<Vec<InputRequestRecord>, InputRequestStoreError> {
        let (access_predicate, access_binds) = filter.session_clause(5);
        let root_join = crate::access::SESSION_ROOT_JOIN;
        let query = format!(
            "SELECT {COLUMNS} FROM input_requests i
             JOIN sessions s ON s.universe_id = i.universe_id AND s.session_id = i.session_id
             {root_join}
             WHERE i.universe_id = $1 AND s.deleted_at_ms IS NULL
               AND ($2::text IS NULL OR i.status = $2)
               AND ($3::text IS NULL OR i.session_id = $3)
               {access_predicate}
             ORDER BY i.created_at_ms DESC, i.request_id DESC
             LIMIT $4"
        );
        let sql = sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(self.config.universe_id)
            .bind(status.map(InputRequestStatus::as_str))
            .bind(session_id)
            .bind(i64::from(limit));
        let rows = access_binds.bind(sql).fetch_all(&self.pool).await?;
        rows.iter().map(decode).collect()
    }

    /// Record the answer to a pending request.
    pub async fn answer_input_request(
        &self,
        request_id: &str,
        answer: &InputRequestAnswer,
    ) -> Result<InputRequestRecord, InputRequestStoreError> {
        let answered_by = serde_json::to_value(&answer.answered_by)
            .map_err(|error| InputRequestStoreError::Decode(error.to_string()))?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE input_requests i SET status = 'answered', approved = $3, answer_value = $4,
                 answer_comment = $5, answered_by = $6, closed_at_ms = $7
             WHERE i.universe_id = $1 AND i.request_id = $2 AND i.status = 'pending'
             RETURNING {COLUMNS}"
        )))
        .bind(self.config.universe_id)
        .bind(request_id)
        .bind(answer.approved)
        .bind(&answer.value)
        .bind(&answer.comment)
        .bind(answered_by)
        .bind(to_i64(answer.answered_at_ms)?)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => decode(&row),
            None => Err(self.unchanged(request_id).await),
        }
    }

    /// Count the `count`th reminder of a pending request. Repeating the
    /// same reminder changes nothing.
    pub async fn remind_input_request(
        &self,
        execution_id: &str,
        count: u32,
        at_ms: u64,
    ) -> Result<(), InputRequestStoreError> {
        sqlx::query(
            "UPDATE input_requests SET reminder_count = $3, last_reminded_at_ms = $4
             WHERE universe_id = $1 AND execution_id = $2 AND status = 'pending'
               AND reminder_count < $3",
        )
        .bind(self.config.universe_id)
        .bind(execution_id)
        .bind(i32::try_from(count).map_err(|_| decode_error("reminder count"))?)
        .bind(to_i64(at_ms)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Hand a pending request to its escalation list with a fresh
    /// deadline, once. Returns the record as it now stands.
    pub async fn escalate_input_request(
        &self,
        execution_id: &str,
        deadline_at_ms: u64,
    ) -> Result<InputRequestRecord, InputRequestStoreError> {
        sqlx::query(
            "UPDATE input_requests SET assignees = escalate_to, escalated = true,
                 deadline_at_ms = $3
             WHERE universe_id = $1 AND execution_id = $2 AND status = 'pending'
               AND NOT escalated AND jsonb_array_length(escalate_to) > 0",
        )
        .bind(self.config.universe_id)
        .bind(execution_id)
        .bind(to_i64(deadline_at_ms)?)
        .execute(&self.pool)
        .await?;
        self.read_input_request_by_execution(execution_id)
            .await?
            .ok_or_else(|| InputRequestStoreError::NotFound(execution_id.to_owned()))
    }

    /// Close a pending request unanswered. Returns the record as it now
    /// stands: an answer that arrived first is kept.
    pub async fn close_input_request(
        &self,
        execution_id: &str,
        closure: InputRequestClosure,
        at_ms: u64,
    ) -> Result<InputRequestRecord, InputRequestStoreError> {
        sqlx::query(
            "UPDATE input_requests SET status = $3, closed_at_ms = $4
             WHERE universe_id = $1 AND execution_id = $2 AND status = 'pending'",
        )
        .bind(self.config.universe_id)
        .bind(execution_id)
        .bind(match closure {
            InputRequestClosure::Expired => "expired",
            InputRequestClosure::Cancelled => "cancelled",
        })
        .bind(to_i64(at_ms)?)
        .execute(&self.pool)
        .await?;
        self.read_input_request_by_execution(execution_id)
            .await?
            .ok_or_else(|| InputRequestStoreError::NotFound(execution_id.to_owned()))
    }

    async fn unchanged(&self, request_id: &str) -> InputRequestStoreError {
        match self.read_input_request(request_id).await {
            Ok(Some(record)) => InputRequestStoreError::Settled(Box::new(record)),
            Ok(None) => InputRequestStoreError::NotFound(request_id.to_owned()),
            Err(error) => error,
        }
    }
}

fn decode(row: &sqlx::postgres::PgRow) -> Result<InputRequestRecord, InputRequestStoreError> {
    let status: String = row.try_get("status")?;
    let status = InputRequestStatus::parse(&status).ok_or_else(|| decode_error("status"))?;
    let people = |column: &str| -> Result<Vec<String>, InputRequestStoreError> {
        serde_json::from_value(row.try_get::<Value, _>(column)?)
            .map_err(|error| InputRequestStoreError::Decode(format!("{column}: {error}")))
    };
    let closed_at_ms = row
        .try_get::<Option<i64>, _>("closed_at_ms")?
        .map(to_u64)
        .transpose()?;
    let answer = match row.try_get::<Option<bool>, _>("approved")? {
        Some(approved) => Some(InputRequestAnswer {
            approved,
            value: row.try_get("answer_value")?,
            comment: row.try_get("answer_comment")?,
            answered_by: serde_json::from_value::<Attribution>(
                row.try_get::<Option<Value>, _>("answered_by")?
                    .ok_or_else(|| decode_error("answered_by"))?,
            )
            .map_err(|error| InputRequestStoreError::Decode(format!("answered_by: {error}")))?,
            answered_at_ms: closed_at_ms.ok_or_else(|| decode_error("closed_at_ms"))?,
        }),
        None => None,
    };
    Ok(InputRequestRecord {
        execution_id: row.try_get("execution_id")?,
        view: InputRequestView {
            request_id: row.try_get("request_id")?,
            session_id: row.try_get("session_id")?,
            run_id: to_u64(row.try_get("run_id")?)?,
            ask: row.try_get("ask")?,
            payload: row.try_get("payload")?,
            response_schema: row.try_get("response_schema")?,
            assignees: people("assignees")?,
            escalate_to: people("escalate_to")?,
            escalated: row.try_get("escalated")?,
            status,
            created_at_ms: to_u64(row.try_get("created_at_ms")?)?,
            deadline_at_ms: to_u64(row.try_get("deadline_at_ms")?)?,
            remind_after_ms: row
                .try_get::<Option<i64>, _>("remind_after_ms")?
                .map(to_u64)
                .transpose()?,
            reminder_count: u32::try_from(row.try_get::<i32, _>("reminder_count")?)
                .map_err(|_| decode_error("reminder_count"))?,
            last_reminded_at_ms: row
                .try_get::<Option<i64>, _>("last_reminded_at_ms")?
                .map(to_u64)
                .transpose()?,
            answer,
            closed_at_ms,
        },
    })
}

fn to_i64(value: u64) -> Result<i64, InputRequestStoreError> {
    i64::try_from(value).map_err(|_| decode_error("an out-of-range integer"))
}

fn to_u64(value: i64) -> Result<u64, InputRequestStoreError> {
    u64::try_from(value).map_err(|_| decode_error("a negative integer"))
}

fn decode_error(what: &str) -> InputRequestStoreError {
    InputRequestStoreError::Decode(format!("invalid {what}"))
}
