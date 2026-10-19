-- The program call index: one row per call a program run made, written in
-- the transaction that appends the session log entries admitting and
-- settling it, so a run's calls list page by page however many it made.
-- A program execution's scope is recorded with its bindings' contract
-- names when it opens; only calls of a recorded scope are indexed.
CREATE TABLE program_scopes (
    universe_id uuid NOT NULL,
    session_id text NOT NULL,
    execution_id text NOT NULL,
    run_id bigint NOT NULL CHECK (run_id > 0),
    contracts jsonb NOT NULL CHECK (jsonb_typeof(contracts) = 'object'),
    PRIMARY KEY (universe_id, session_id, execution_id),
    FOREIGN KEY (universe_id, session_id)
        REFERENCES sessions (universe_id, session_id) ON DELETE CASCADE
);

CREATE TABLE program_calls (
    universe_id uuid NOT NULL,
    session_id text NOT NULL,
    execution_id text NOT NULL,
    request_id text NOT NULL,
    run_id bigint NOT NULL CHECK (run_id > 0),
    seq bigint NOT NULL CHECK (seq > 0),
    tool_call_id text NOT NULL,
    binding_id text NOT NULL,
    tool_id text NOT NULL,
    tool_name text NOT NULL,
    arguments_ref text NOT NULL,
    status text NOT NULL
        CHECK (status IN ('pending', 'waiting', 'succeeded', 'failed', 'cancelled')),
    cancel_requested boolean NOT NULL DEFAULT false,
    output_ref text,
    error_ref text,
    duration_ms bigint CHECK (duration_ms >= 0),
    admitted_at_ms bigint NOT NULL CHECK (admitted_at_ms >= 0),
    completed_at_ms bigint CHECK (completed_at_ms >= 0),
    started_execution_id text,
    PRIMARY KEY (universe_id, session_id, execution_id, request_id),
    FOREIGN KEY (universe_id, session_id, execution_id)
        REFERENCES program_scopes (universe_id, session_id, execution_id) ON DELETE CASCADE
);

CREATE INDEX program_calls_run_idx
    ON program_calls (universe_id, session_id, run_id, seq);

CREATE INDEX program_calls_tool_call_idx
    ON program_calls (universe_id, session_id, tool_call_id);
