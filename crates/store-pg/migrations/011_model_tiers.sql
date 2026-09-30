-- The universe tier table shares the defaults revision: a tier put and a slot
-- put guard against each other, and `default` stays the agent_run column.
ALTER TABLE universe_model_defaults
    ADD COLUMN tiers jsonb NOT NULL DEFAULT '{}'::jsonb;
