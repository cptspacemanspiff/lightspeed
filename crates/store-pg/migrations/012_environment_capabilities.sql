-- Provider-reported capabilities observed with each incarnation's target.
ALTER TABLE environment_incarnations
    ADD COLUMN capabilities_json jsonb NOT NULL DEFAULT '{}';
ALTER TABLE environment_incarnations
    ADD CONSTRAINT environment_incarnations_capabilities_object
        CHECK (jsonb_typeof(capabilities_json) = 'object');
