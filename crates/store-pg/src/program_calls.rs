//! The program call index (`harness::storage::program_calls`): kept in the
//! transaction that appends the entries changing it, and read a page at a
//! time by run.

use harness::storage::{
    SessionStoreError, StoredSessionEntry,
    program_calls::{
        ProgramCallChange, ProgramCallCounts, ProgramCallRecord, ProgramCallStatus,
        program_call_changes,
    },
};
use harness::{BlobRef, RunId, SessionId, ToolCallId};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use crate::{
    PgStore,
    shared::{i64_to_u64, session_sql_error, u64_to_i64},
};

/// Apply the index changes of `entries`, just appended to `session_id`.
pub(crate) async fn index_program_calls(
    tx: &mut Transaction<'_, Postgres>,
    universe_id: Uuid,
    session_id: &SessionId,
    entries: &[StoredSessionEntry],
) -> Result<(), SessionStoreError> {
    for entry in entries {
        let changes = program_call_changes(entry).map_err(|error| SessionStoreError::Store {
            message: format!("decode entry for the program call index: {error}"),
        })?;
        for change in changes {
            apply(tx, universe_id, session_id, change).await?;
        }
    }
    Ok(())
}

async fn apply(
    tx: &mut Transaction<'_, Postgres>,
    universe_id: Uuid,
    session_id: &SessionId,
    change: ProgramCallChange,
) -> Result<(), SessionStoreError> {
    let session_id = session_id.as_str();
    let query = match change {
        ProgramCallChange::ScopeOpened {
            execution_id,
            run_id,
            contracts,
        } => sqlx::query(
            r#"
            INSERT INTO program_scopes (universe_id, session_id, execution_id, run_id, contracts)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(universe_id)
        .bind(session_id)
        .bind(execution_id)
        .bind(u64_to_i64(run_id.as_u64(), "run id")?)
        .bind(serde_json::to_value(contracts).expect("contract names are JSON")),
        // Only a program scope's calls are indexed: code mode's scope was
        // never recorded.
        ProgramCallChange::Admitted {
            execution_id,
            request_id,
            run_id,
            tool_call_id,
            binding_id,
            tool_id,
            tool_name,
            arguments_ref,
            seq,
            admitted_at_ms,
        } => sqlx::query(
            r#"
            INSERT INTO program_calls (
                universe_id, session_id, execution_id, request_id, run_id, seq,
                tool_call_id, binding_id, tool_id, tool_name, arguments_ref, status,
                admitted_at_ms
            )
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', $12
            WHERE EXISTS (
                SELECT 1 FROM program_scopes
                WHERE universe_id = $1 AND session_id = $2 AND execution_id = $3
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(universe_id)
        .bind(session_id)
        .bind(execution_id)
        .bind(request_id)
        .bind(u64_to_i64(run_id.as_u64(), "run id")?)
        .bind(u64_to_i64(seq, "event sequence")?)
        .bind(tool_call_id.as_str().to_owned())
        .bind(binding_id)
        .bind(tool_id)
        .bind(tool_name)
        .bind(arguments_ref.as_str().to_owned())
        .bind(u64_to_i64(admitted_at_ms, "admitted_at_ms")?),
        ProgramCallChange::Waiting {
            execution_id,
            request_id,
        } => sqlx::query(
            r#"
            UPDATE program_calls SET status = 'waiting'
            WHERE universe_id = $1 AND session_id = $2 AND execution_id = $3
                AND request_id = $4 AND status = 'pending'
            "#,
        )
        .bind(universe_id)
        .bind(session_id)
        .bind(execution_id)
        .bind(request_id),
        ProgramCallChange::CancelRequested {
            execution_id,
            request_id,
        } => sqlx::query(
            r#"
            UPDATE program_calls SET cancel_requested = true
            WHERE universe_id = $1 AND session_id = $2 AND execution_id = $3
                AND request_id = $4
            "#,
        )
        .bind(universe_id)
        .bind(session_id)
        .bind(execution_id)
        .bind(request_id),
        ProgramCallChange::Completed {
            execution_id,
            request_id,
            status,
            output_ref,
            error_ref,
            duration_ms,
            completed_at_ms,
        } => sqlx::query(
            r#"
            UPDATE program_calls
            SET status = $5, output_ref = $6, error_ref = $7, duration_ms = $8,
                completed_at_ms = $9
            WHERE universe_id = $1 AND session_id = $2 AND execution_id = $3
                AND request_id = $4
            "#,
        )
        .bind(universe_id)
        .bind(session_id)
        .bind(execution_id)
        .bind(request_id)
        .bind(ProgramCallStatus::of_outcome(status).as_str())
        .bind(output_ref.map(|blob| blob.as_str().to_owned()))
        .bind(error_ref.map(|blob| blob.as_str().to_owned()))
        .bind(
            duration_ms
                .map(|duration| u64_to_i64(duration, "duration_ms"))
                .transpose()?,
        )
        .bind(u64_to_i64(completed_at_ms, "completed_at_ms")?),
        ProgramCallChange::Started {
            tool_call_id,
            started_execution_id,
        } => sqlx::query(
            r#"
            UPDATE program_calls SET started_execution_id = $4
            WHERE universe_id = $1 AND session_id = $2 AND tool_call_id = $3
            "#,
        )
        .bind(universe_id)
        .bind(session_id)
        .bind(tool_call_id.as_str().to_owned())
        .bind(started_execution_id),
    };
    query
        .execute(&mut **tx)
        .await
        .map_err(|error| session_sql_error("update the program call index", error))?;
    Ok(())
}

impl PgStore {
    /// Up to `limit` of `run_id`'s calls admitted after the position
    /// `after`, in admission order, of `status` when given.
    pub async fn list_program_calls(
        &self,
        session_id: &SessionId,
        run_id: RunId,
        after: Option<u64>,
        status: Option<ProgramCallStatus>,
        limit: u32,
    ) -> Result<Vec<ProgramCallRecord>, SessionStoreError> {
        let rows = sqlx::query(
            r#"
            SELECT c.execution_id, c.request_id, c.run_id, c.seq, c.tool_call_id,
                s.contracts ->> c.binding_id AS contract, c.tool_id, c.tool_name,
                c.arguments_ref, c.status, c.cancel_requested, c.output_ref, c.error_ref,
                c.duration_ms, c.admitted_at_ms, c.completed_at_ms, c.started_execution_id,
                r.request_id AS input_request_id
            FROM program_calls c
            JOIN program_scopes s
                ON s.universe_id = c.universe_id AND s.session_id = c.session_id
                AND s.execution_id = c.execution_id
            LEFT JOIN input_requests r
                ON r.universe_id = c.universe_id AND r.execution_id = c.started_execution_id
            WHERE c.universe_id = $1 AND c.session_id = $2 AND c.run_id = $3 AND c.seq > $4
                AND ($5::text IS NULL OR c.status = $5)
            ORDER BY c.seq
            LIMIT $6
            "#,
        )
        .bind(self.config.universe_id)
        .bind(session_id.as_str())
        .bind(u64_to_i64(run_id.as_u64(), "run id")?)
        .bind(u64_to_i64(after.unwrap_or(0), "call cursor")?)
        .bind(status.map(ProgramCallStatus::as_str))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| session_sql_error("list program calls", error))?;
        rows.iter().map(program_call_from_row).collect()
    }

    /// Which of `session_ids` exist (and are not deleted).
    pub async fn existing_session_ids(
        &self,
        session_ids: &[String],
    ) -> Result<std::collections::BTreeSet<String>, SessionStoreError> {
        if session_ids.is_empty() {
            return Ok(Default::default());
        }
        let rows = sqlx::query(
            r#"
            SELECT session_id FROM sessions
            WHERE universe_id = $1 AND session_id = ANY($2) AND deleted_at_ms IS NULL
            "#,
        )
        .bind(self.config.universe_id)
        .bind(session_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| session_sql_error("find sessions", error))?;
        rows.iter()
            .map(|row| {
                row.try_get("session_id")
                    .map_err(|error| session_sql_error("decode session id", error))
            })
            .collect()
    }

    /// How many of `run_id`'s calls are in each status.
    pub async fn count_program_calls(
        &self,
        session_id: &SessionId,
        run_id: RunId,
    ) -> Result<ProgramCallCounts, SessionStoreError> {
        let rows = sqlx::query(
            r#"
            SELECT status, count(*) AS calls
            FROM program_calls
            WHERE universe_id = $1 AND session_id = $2 AND run_id = $3
            GROUP BY status
            "#,
        )
        .bind(self.config.universe_id)
        .bind(session_id.as_str())
        .bind(u64_to_i64(run_id.as_u64(), "run id")?)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| session_sql_error("count program calls", error))?;
        let mut counts = ProgramCallCounts::default();
        for row in rows {
            let status: String = row
                .try_get("status")
                .map_err(|error| session_sql_error("decode program call status", error))?;
            let calls: i64 = row
                .try_get("calls")
                .map_err(|error| session_sql_error("decode program call count", error))?;
            counts.add(
                parse_status(&status)?,
                i64_to_u64(calls, "program call count").map_err(store_error)?,
            );
        }
        Ok(counts)
    }
}

fn program_call_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ProgramCallRecord, SessionStoreError> {
    let get = |error: sqlx::Error| session_sql_error("decode program call", error);
    let blob = |value: String| {
        BlobRef::parse(value.clone()).map_err(|error| SessionStoreError::Store {
            message: format!("invalid program call blob ref {value}: {error}"),
        })
    };
    let unsigned = |value: i64, name: &str| i64_to_u64(value, name).map_err(store_error);
    let status: String = row.try_get("status").map_err(get)?;
    Ok(ProgramCallRecord {
        execution_id: row.try_get("execution_id").map_err(get)?,
        request_id: row.try_get("request_id").map_err(get)?,
        run_id: RunId::new(unsigned(row.try_get("run_id").map_err(get)?, "run_id")?),
        seq: unsigned(row.try_get("seq").map_err(get)?, "seq")?,
        tool_call_id: ToolCallId::new(row.try_get::<String, _>("tool_call_id").map_err(get)?),
        contract: row.try_get("contract").map_err(get)?,
        tool_id: row.try_get("tool_id").map_err(get)?,
        tool_name: row.try_get("tool_name").map_err(get)?,
        arguments_ref: blob(row.try_get("arguments_ref").map_err(get)?)?,
        status: parse_status(&status)?,
        cancel_requested: row.try_get("cancel_requested").map_err(get)?,
        output_ref: row
            .try_get::<Option<String>, _>("output_ref")
            .map_err(get)?
            .map(blob)
            .transpose()?,
        error_ref: row
            .try_get::<Option<String>, _>("error_ref")
            .map_err(get)?
            .map(blob)
            .transpose()?,
        duration_ms: row
            .try_get::<Option<i64>, _>("duration_ms")
            .map_err(get)?
            .map(|value| unsigned(value, "duration_ms"))
            .transpose()?,
        admitted_at_ms: unsigned(
            row.try_get("admitted_at_ms").map_err(get)?,
            "admitted_at_ms",
        )?,
        completed_at_ms: row
            .try_get::<Option<i64>, _>("completed_at_ms")
            .map_err(get)?
            .map(|value| unsigned(value, "completed_at_ms"))
            .transpose()?,
        started_execution_id: row.try_get("started_execution_id").map_err(get)?,
        input_request_id: row.try_get("input_request_id").map_err(get)?,
    })
}

fn parse_status(value: &str) -> Result<ProgramCallStatus, SessionStoreError> {
    ProgramCallStatus::parse(value).ok_or_else(|| SessionStoreError::Store {
        message: format!("invalid program call status {value}"),
    })
}

fn store_error(message: String) -> SessionStoreError {
    SessionStoreError::Store { message }
}
