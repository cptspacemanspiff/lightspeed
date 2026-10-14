use super::api_config::harness_session_config_from_api;
use super::*;
use ::profiles::{ProfileError, ProfileSourceExt, ProfileStore};

pub(super) fn merge_profile_start_metadata(
    profile_metadata: Option<&BTreeMap<String, String>>,
    explicit_metadata: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut metadata = profile_metadata.cloned().unwrap_or_default();
    metadata.extend(explicit_metadata);
    metadata
}

pub(super) fn merge_profile_start_retention(
    profile_default: Option<u64>,
    explicit: Option<Option<u64>>,
) -> Option<u64> {
    explicit.unwrap_or(profile_default)
}

impl GatewayAgentApi {
    pub(super) async fn create_profile_record(
        &self,
        params: ProfileCreateParams,
    ) -> Result<ProfileCreateResponse, AgentApiError> {
        let created_at_ms = now_ms()?;
        let profile = self
            .store
            .create_agent_profile(params.profile, created_at_ms)
            .await
            .map_err(map_profile_error)?;
        Ok(ProfileCreateResponse { profile })
    }

    pub(super) async fn read_profile_record(
        &self,
        params: ProfileReadParams,
    ) -> Result<ProfileReadResponse, AgentApiError> {
        let profile = self
            .store
            .read_agent_profile(&params.profile_id)
            .await
            .map_err(map_profile_error)?;
        Ok(ProfileReadResponse { profile })
    }

    pub(super) async fn list_profile_records(
        &self,
        _params: ProfileListParams,
    ) -> Result<ProfileListResponse, AgentApiError> {
        let profiles = self
            .store
            .list_agent_profiles()
            .await
            .map_err(map_profile_error)?;
        Ok(ProfileListResponse { profiles })
    }

    pub(super) async fn put_profile_record(
        &self,
        params: ProfilePutParams,
    ) -> Result<ProfilePutResponse, AgentApiError> {
        let profile = self
            .store
            .put_agent_profile(params.profile, params.expected_revision, now_ms()?)
            .await
            .map_err(map_profile_error)?;
        // Bots pin the profile by id: every open bot on it re-applies the
        // new revision at its controller's next idle boundary.
        self.signal_bots_for_profile(&profile.profile_id).await;
        Ok(ProfilePutResponse { profile })
    }

    pub(super) async fn delete_profile_record(
        &self,
        params: ProfileDeleteParams,
    ) -> Result<ProfileDeleteResponse, AgentApiError> {
        let profile = self
            .store
            .delete_agent_profile(&params.profile_id)
            .await
            .map_err(map_profile_error)?;
        Ok(ProfileDeleteResponse { profile })
    }

    pub(super) async fn apply_profile_to_session(
        &self,
        params: ProfileApplyParams,
    ) -> Result<ProfileApplyResponse, AgentApiError> {
        let session_id = SessionId::try_new(params.session_id).map_err(|error| {
            AgentApiError::invalid_request(format!("invalid session id: {error}"))
        })?;
        let resolved = self.resolve_profile(params.profile).await?;
        let loaded = self.load_session_state(&session_id).await?;
        self.check_profile_driver(&loaded.state.lifecycle.driver, &resolved)
            .await?;
        let resolved = resolved.document;
        let revision = loaded.state.lifecycle.config_revision;
        if let Some(expected) = params.expected_config_revision
            && expected != revision
        {
            return Err(AgentApiError::conflict(format!(
                "expected config revision {expected}, got {revision}"
            )));
        }
        let profile = Self::profile_intent(
            &resolved,
            Some(model_defaults::current_session_model(&loaded.state)?),
        )?;
        let applied = self
            .prepare_session_operation(
                &session_id,
                temporal_workflow::SessionOperation::ApplyProfile {
                    profile,
                    expected_config_revision: Some(revision),
                    expected_tools_revision: params.expected_tools_revision,
                },
            )
            .await?;
        let session = self.project_session_by_id(&session_id).await?;
        Ok(ProfileApplyResponse { session, applied })
    }

    /// A session's driver is fixed at creation, so a profile applies only
    /// when it describes the same driver: no program for an agent session,
    /// and for a program session a program that pins to exactly what the
    /// session runs. Anything else needs a new session.
    async fn check_profile_driver(
        &self,
        driver: &harness::SessionDriver,
        resolved: &ResolvedProfile,
    ) -> Result<(), AgentApiError> {
        match (driver, resolved.document.program.as_ref()) {
            (harness::SessionDriver::Agent, None) => Ok(()),
            (harness::SessionDriver::Agent, Some(_)) => Err(AgentApiError::invalid_request(
                "the profile has a program, but the session is an agent session; start a new session from the profile",
            )),
            (harness::SessionDriver::Program { .. }, None) => Err(AgentApiError::invalid_request(
                "the profile has no program, but the session is a program session; start a new session from the profile",
            )),
            (harness::SessionDriver::Program { program }, Some(spec)) => {
                let proposed = self.pin_program(spec, resolved.named.as_ref()).await?;
                if program.runs_same_program_as(&proposed) {
                    Ok(())
                } else {
                    Err(AgentApiError::invalid_request(
                        "the profile's program differs from the one the session pinned at creation; start a new session from the profile",
                    ))
                }
            }
        }
    }

    /// Resolve a profile source, keeping the named profile's identity and
    /// revision so a program pinned from it records where it came from.
    pub(super) async fn resolve_profile(
        &self,
        source: ProfileSource,
    ) -> Result<ResolvedProfile, AgentApiError> {
        source.validate().map_err(map_profile_error)?;
        match source {
            ProfileSource::Named { profile_id } => {
                let profile = self
                    .store
                    .read_agent_profile(&profile_id)
                    .await
                    .map_err(map_profile_error)?;
                Ok(ResolvedProfile {
                    document: profile.document,
                    named: Some((profile_id, profile.revision)),
                })
            }
            ProfileSource::Inline { profile } => Ok(ResolvedProfile {
                document: profile.document,
                named: None,
            }),
        }
    }

    /// Pin a profile's program for a new session: the source and schemas go
    /// to CAS by content, so the session retains exactly what it was created
    /// from.
    pub(super) async fn pin_program(
        &self,
        spec: &api::ProgramSpec,
        named: Option<&(api::ProfileId, u64)>,
    ) -> Result<harness::PinnedProgram, AgentApiError> {
        let source_ref = match &spec.source {
            api::ProgramSource::Text { text } => {
                if text.trim().is_empty() {
                    return Err(AgentApiError::invalid_request(
                        "program source must not be empty",
                    ));
                }
                self.store
                    .put_bytes(text.clone().into_bytes())
                    .await
                    .map_err(map_blob_store_error)?
            }
            api::ProgramSource::TextRef { blob_ref } => {
                let reference = parse_blob_ref(blob_ref)?;
                if !self
                    .store
                    .has_blob(&reference)
                    .await
                    .map_err(map_blob_store_error)?
                {
                    return Err(AgentApiError::not_found(format!(
                        "program source blob not found: {reference}"
                    )));
                }
                reference
            }
        };
        let schema_ref = async |schema: &Option<serde_json::Value>| match schema {
            Some(schema) => {
                let bytes = serde_json::to_vec(schema).map_err(|error| {
                    AgentApiError::invalid_request(format!("invalid program schema: {error}"))
                })?;
                Ok::<_, AgentApiError>(Some(
                    self.store
                        .put_bytes(bytes)
                        .await
                        .map_err(map_blob_store_error)?,
                ))
            }
            None => Ok(None),
        };
        let input_schema = schema_ref(&spec.input_schema).await?;
        let output_schema = schema_ref(&spec.output_schema).await?;
        let defaults = harness::ProgramLimits::default();
        let limits = spec.limits.clone().unwrap_or_default();
        let program = harness::PinnedProgram {
            program_id: named.map(|(profile_id, _)| profile_id.as_str().to_owned()),
            revision: named.map(|(_, revision)| *revision),
            source_ref,
            engine: harness::ProgramEngine {
                kind: harness::ProgramEngineKind::Js,
                semantics_version: spec.semantics_version,
            },
            input_schema,
            output_schema,
            limits: harness::ProgramLimits {
                max_calls: limits.max_calls.unwrap_or(defaults.max_calls),
                max_in_flight: limits.max_in_flight.unwrap_or(defaults.max_in_flight),
                max_wall_clock_ms: limits.max_wall_clock_ms,
            },
        };
        program
            .validate()
            .map_err(|error| AgentApiError::invalid_request(error.to_string()))?;
        Ok(program)
    }

    pub(super) fn merge_profile_start_config(
        profile_config: Option<api::SessionConfig>,
        explicit_config: Option<api::SessionConfig>,
    ) -> Option<api::SessionConfig> {
        let Some(profile_config) = profile_config else {
            return explicit_config;
        };
        let Some(explicit_config) = explicit_config else {
            return Some(profile_config);
        };
        Some(api::SessionConfig {
            model: explicit_config.model.or(profile_config.model),
            generation: explicit_config.generation.or(profile_config.generation),
            limits: explicit_config.limits.or(profile_config.limits),
            context: explicit_config.context.or(profile_config.context),
            features: explicit_config.features.or(profile_config.features),
        })
    }

    /// With a current model, apply the profile configuration and preserve an
    /// omitted model. On creation the caller has already merged configuration
    /// and sets the environment candidate itself.
    pub(super) fn profile_intent(
        profile: &ProfileDocument,
        current_model: Option<ModelSelection>,
    ) -> Result<temporal_workflow::SessionProfileIntent, AgentApiError> {
        let config = if let Some(current_model) = current_model {
            profile
                .config
                .clone()
                .map(|config| harness_session_config_from_api(config, current_model))
                .transpose()?
        } else {
            None
        };
        let environment = config
            .as_ref()
            .map(|config| default_environment_id(&config.features))
            .transpose()?
            .flatten();
        Ok(temporal_workflow::SessionProfileIntent {
            config,
            instructions: profile.instructions.clone(),
            environment,
        })
    }
}

/// A profile document with the identity it was read under, if it was named.
pub(super) struct ResolvedProfile {
    pub document: ProfileDocument,
    pub named: Option<(api::ProfileId, u64)>,
}

/// The attachment a profile activates when the session has no active
/// environment.
pub(super) fn default_environment_id(
    features: &harness::FeaturesConfig,
) -> Result<Option<harness::EnvironmentId>, AgentApiError> {
    features
        .environments
        .as_ref()
        .and_then(|environments| environments.default_attachment())
        .map(|attachment| {
            harness::EnvironmentId::try_new(attachment.environment_id.clone())
                .map_err(|error| AgentApiError::invalid_request(error.to_string()))
        })
        .transpose()
}

pub(super) fn map_profile_error(error: ProfileError) -> AgentApiError {
    match error {
        ProfileError::AlreadyExists { profile_id } => {
            AgentApiError::conflict(format!("agent profile already exists: {profile_id}"))
        }
        ProfileError::NotFound { profile_id } => {
            AgentApiError::not_found(format!("agent profile not found: {profile_id}"))
        }
        ProfileError::RevisionConflict {
            profile_id,
            expected,
            actual,
        } => AgentApiError::conflict(format!(
            "agent profile revision conflict for {profile_id}: expected {expected}, got {actual}"
        )),
        ProfileError::InvalidInput { message } => AgentApiError::invalid_request(message),
        ProfileError::Store { message } => AgentApiError::internal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_environment_ids_are_fallible_even_before_config_validation() {
        let mut features = harness::FeaturesConfig::default();
        assert_eq!(default_environment_id(&features).unwrap(), None);
        features.environments = Some(harness::EnvironmentsFeature {
            environments: vec![harness::EnvironmentAttachment {
                environment_id: "env_ok".into(),
                default: true,
                access: harness::EnvironmentAccess::Read,
                working_directory: None,
            }],
            ..Default::default()
        });
        assert_eq!(
            default_environment_id(&features).unwrap(),
            Some(harness::EnvironmentId::new("env_ok"))
        );
        features.environments.as_mut().unwrap().environments[0].default = false;
        assert_eq!(default_environment_id(&features).unwrap(), None);
        features.environments.as_mut().unwrap().environments[0].default = true;
        for invalid in ["".to_owned(), "env/bad".into(), "a".repeat(129)] {
            features.environments.as_mut().unwrap().environments[0].environment_id = invalid;
            let error = default_environment_id(&features).unwrap_err();
            assert_eq!(error.kind, AgentApiErrorKind::InvalidRequest);
        }
    }

    #[test]
    fn explicit_start_metadata_overrides_profile_defaults() {
        let profile = BTreeMap::from([
            ("campaign".to_owned(), "profile-campaign".to_owned()),
            ("owner".to_owned(), "evaluation".to_owned()),
        ]);
        let explicit = BTreeMap::from([
            ("campaign".to_owned(), "request-campaign".to_owned()),
            ("trial".to_owned(), "42".to_owned()),
        ]);

        assert_eq!(
            merge_profile_start_metadata(Some(&profile), explicit),
            BTreeMap::from([
                ("campaign".to_owned(), "request-campaign".to_owned()),
                ("owner".to_owned(), "evaluation".to_owned()),
                ("trial".to_owned(), "42".to_owned()),
            ])
        );
    }

    #[test]
    fn explicit_start_retention_overrides_or_clears_profile_default() {
        assert_eq!(merge_profile_start_retention(Some(100), None), Some(100));
        assert_eq!(merge_profile_start_retention(Some(100), Some(None)), None);
        assert_eq!(
            merge_profile_start_retention(Some(100), Some(Some(200))),
            Some(200)
        );
    }
}
