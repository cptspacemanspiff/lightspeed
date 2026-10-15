-- Approval requests share the inbox with questions: a program call that
-- needs approval waits on a row of kind 'approval', answered approve or
-- reject like any request.
ALTER TABLE input_requests
    ADD COLUMN kind text NOT NULL DEFAULT 'input' CHECK (kind IN ('input', 'approval'));
