-- A program's notes on its work (`ctx.say`, `ctx.step`), and the step each
-- of its calls was made in. Notes are numbered per program execution like
-- calls; `after_calls` places each among the execution's calls.
ALTER TABLE program_calls ADD COLUMN step bigint CHECK (step > 0);

CREATE TABLE program_notes (
    universe_id uuid NOT NULL,
    session_id text NOT NULL,
    execution_id text NOT NULL,
    note_seq bigint NOT NULL CHECK (note_seq > 0),
    run_id bigint NOT NULL CHECK (run_id > 0),
    seq bigint NOT NULL CHECK (seq > 0),
    kind text NOT NULL CHECK (kind IN ('said', 'step_started', 'step_ended')),
    -- Narration's step, or the step that ended.
    step bigint CHECK (step > 0),
    -- A started step's enclosing step.
    parent bigint CHECK (parent > 0),
    -- Narration's text, or a started step's name.
    text text,
    after_calls bigint NOT NULL CHECK (after_calls >= 0),
    recorded_at_ms bigint NOT NULL CHECK (recorded_at_ms >= 0),
    PRIMARY KEY (universe_id, session_id, execution_id, note_seq),
    FOREIGN KEY (universe_id, session_id, execution_id)
        REFERENCES program_scopes (universe_id, session_id, execution_id) ON DELETE CASCADE
);

CREATE INDEX program_notes_run_idx
    ON program_notes (universe_id, session_id, run_id, seq, note_seq);
