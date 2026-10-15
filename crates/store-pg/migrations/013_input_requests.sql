-- The universe's human-input inbox: requests a program run is waiting on,
-- each owned by the input-request workflow named by execution_id. The
-- workflow opens and settles a row; the inbox API records answers on it.
CREATE TABLE input_requests (
    universe_id uuid NOT NULL,
    request_id text NOT NULL,
    execution_id text NOT NULL,
    session_id text NOT NULL,
    run_id bigint NOT NULL CHECK (run_id > 0),
    ask text NOT NULL,
    payload jsonb,
    response_schema jsonb,
    assignees jsonb NOT NULL CHECK (jsonb_typeof(assignees) = 'array'),
    escalate_to jsonb NOT NULL CHECK (jsonb_typeof(escalate_to) = 'array'),
    escalated boolean NOT NULL DEFAULT false,
    status text NOT NULL CHECK (status IN ('pending', 'answered', 'expired', 'cancelled')),
    created_at_ms bigint NOT NULL,
    deadline_at_ms bigint NOT NULL,
    remind_after_ms bigint CHECK (remind_after_ms > 0),
    reminder_count integer NOT NULL DEFAULT 0 CHECK (reminder_count >= 0),
    last_reminded_at_ms bigint,
    approved boolean,
    answer_value jsonb,
    answer_comment text,
    answered_by jsonb,
    closed_at_ms bigint,
    PRIMARY KEY (universe_id, request_id),
    UNIQUE (universe_id, execution_id),
    FOREIGN KEY (universe_id, session_id)
        REFERENCES sessions (universe_id, session_id) ON DELETE CASCADE,
    CONSTRAINT input_requests_answer_shape CHECK (
        (status = 'answered') = (approved IS NOT NULL AND answered_by IS NOT NULL)
    ),
    CONSTRAINT input_requests_closed_shape CHECK (
        (status = 'pending') = (closed_at_ms IS NULL)
    )
);

CREATE INDEX input_requests_pending_idx
    ON input_requests (universe_id, created_at_ms, request_id)
    WHERE status = 'pending';
CREATE INDEX input_requests_session_idx
    ON input_requests (universe_id, session_id, created_at_ms);
