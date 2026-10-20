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
                let mut proposed = self.pin_program(spec, resolved.named.as_ref()).await?;
                // Contracts were pinned against the session's tools at
                // creation. The profile names the same ones when it requires
                // the same contracts, at the same digests where it gives one.
                proposed.requires = spec
                    .requires
                    .iter()
                    .map(|requirement| harness::ContractPin {
                        contract: requirement.contract.clone(),
                        digest: requirement.digest.clone().unwrap_or_else(|| {
                            program
                                .requires
                                .iter()
                                .find(|pin| pin.contract == requirement.contract)
                                .map(|pin| pin.digest.clone())
                                .unwrap_or_default()
                        }),
                    })
                    .collect();
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
        let mut contracts = std::collections::BTreeSet::new();
        for requirement in &spec.requires {
            harness::validate_contract_name(&requirement.contract)
                .map_err(|error| AgentApiError::invalid_request(error.to_string()))?;
            if !tools::program::contracts::is_known(&requirement.contract) {
                return Err(AgentApiError::invalid_request(format!(
                    "program requires unknown tool contract {}",
                    requirement.contract
                )));
            }
            if !contracts.insert(requirement.contract.as_str()) {
                return Err(AgentApiError::invalid_request(format!(
                    "program requires tool contract {} more than once",
                    requirement.contract
                )));
            }
        }
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
                max_compute: limits.max_compute.unwrap_or(defaults.max_compute),
                max_workflow_calls: limits
                    .max_workflow_calls
                    .unwrap_or(defaults.max_workflow_calls),
                input_deadline_ms: limits.input_deadline_ms,
                context_retained: limits.context_retained.unwrap_or(defaults.context_retained),
            },
            // Pinned against the session's tools once its configuration is
            // known; see `pin_program_contracts`.
            requires: Vec::new(),
            approvals: spec.approvals.clone().map(|approvals| {
                Box::new(harness::ProgramApprovals {
                    assignees: approvals.assignees,
                    deadline_ms: approvals.deadline_ms,
                    remind_after_ms: approvals.remind_after_ms,
                    escalate_to: approvals.escalate_to,
                })
            }),
        };
        program
            .validate()
            .map_err(|error| AgentApiError::invalid_request(error.to_string()))?;
        if let Some(deadline) = limits.input_deadline_ms
            && !self.input_timeouts.allows_deadline(deadline)
        {
            return Err(AgentApiError::invalid_request(format!(
                "inputDeadlineMs must be between {} and {}",
                self.input_timeouts.min_deadline_ms, self.input_timeouts.max_deadline_ms
            )));
        }
        if let Some(approvals) = &program.approvals {
            tools::human_input::ApprovalSettings::from_program(Some(approvals), None)
                .validate(&self.input_timeouts)
                .map_err(|error| AgentApiError::invalid_request(format!("approvals: {error}")))?;
        }
        Ok(program)
    }

    /// Check a program the way a session started from it would be, plus
    /// compiling its source and its schemas, then store it as a program
    /// profile. The first problem refuses it.
    pub(super) async fn put_program_record(
        &self,
        params: api::ProgramPutParams,
    ) -> Result<api::ProgramPutResponse, AgentApiError> {
        let refused = |message: String| {
            AgentApiError::invalid_request(format!("program {}: {message}", params.program_id))
        };
        match self
            .read_profile(ProfileReadParams {
                profile_id: params.program_id.clone(),
            })
            .await
        {
            Ok(existing) if existing.result.profile.document.program.is_none() => {
                return Err(refused(
                    "an agent profile has this id; programs/put replaces only programs".to_owned(),
                ));
            }
            Ok(_) => {}
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(error),
        }
        let session_config = self.session_config_for_start(params.config.clone()).await?;
        let pinned = self.pin_program(&params.program, None).await?;
        let source = match &params.program.source {
            api::ProgramSource::Text { text } => text.clone(),
            api::ProgramSource::TextRef { .. } => {
                let bytes = self
                    .store
                    .read_bytes(&pinned.source_ref)
                    .await
                    .map_err(map_blob_store_error)?;
                String::from_utf8(bytes)
                    .map_err(|_| refused("the source is not UTF-8".to_owned()))?
            }
        };
        let limits = harness::CodeModeLimits::default();
        program_engine::check_source(&source, limits.max_source_bytes, limits.max_memory_bytes)
            .map_err(|error| refused(format!("the source does not compile: {error}")))?;
        for (field, schema) in [
            ("inputSchema", &params.program.input_schema),
            ("outputSchema", &params.program.output_schema),
        ] {
            if let Some(schema) = schema {
                jsonschema::validator_for(schema)
                    .map_err(|error| refused(format!("{field} is not a JSON Schema: {error}")))?;
            }
        }
        let available = self
            .program_contracts_for(
                &params.program.requires,
                &session_config,
                &[],
                &BTreeMap::new(),
            )
            .await?;
        tools::program::contracts::pin_requirements(
            &available,
            params
                .program
                .requires
                .iter()
                .map(|requirement| (requirement.contract.as_str(), requirement.digest.as_deref())),
        )
        .map_err(|error| refused(error.to_string()))?;
        let contracts = params
            .program
            .requires
            .iter()
            .filter_map(|requirement| {
                available
                    .iter()
                    .find(|contract| contract.name == requirement.contract)
            })
            .map(program_contract_view)
            .collect();
        let profile = self
            .put_profile_record(ProfilePutParams {
                profile: api::AgentProfileInput {
                    profile_id: params.program_id,
                    display_name: params.display_name,
                    description: params.description,
                    document: api::ProfileDocument {
                        config: params.config,
                        program: Some(params.program),
                        ..api::ProfileDocument::default()
                    },
                },
                expected_revision: params.expected_revision,
            })
            .await?
            .profile;
        Ok(api::ProgramPutResponse { profile, contracts })
    }

    /// The contracts a program with these grants may require.
    pub(super) async fn list_program_contracts_record(
        &self,
        params: api::ProgramContractsListParams,
    ) -> Result<api::ProgramContractsListResponse, AgentApiError> {
        let with_mcp = params
            .config
            .as_ref()
            .and_then(|config| config.features.as_ref())
            .is_some_and(|features| features.mcp.is_some());
        let session_config = self.session_config_for_start(params.config).await?;
        let mut contracts = self
            .preparation_service()
            .program_contracts(&session_config, with_mcp, &[], &BTreeMap::new())
            .await?
            .iter()
            .map(program_contract_view)
            .collect::<Vec<_>>();
        contracts.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(api::ProgramContractsListResponse { contracts })
    }

    /// The contracts a session with this configuration and these declared
    /// tools offers, reaching the MCP servers only when one is required.
    async fn program_contracts_for(
        &self,
        requires: &[api::ProgramContractRequirement],
        session_config: &SessionConfig,
        declared: &[harness::ToolSpec],
        held: &BTreeMap<String, auth::AuthGrantId>,
    ) -> Result<Vec<tools::program::contracts::BoundContract>, AgentApiError> {
        if requires.is_empty() {
            return Ok(Vec::new());
        }
        let with_mcp = requires.iter().any(|requirement| {
            requirement
                .contract
                .starts_with(&format!("{}.", tools::program::contracts::MCP_FAMILY))
        });
        self.preparation_service()
            .program_contracts(session_config, with_mcp, declared, held)
            .await
    }

    /// Pin a program's required contracts to what a session with this
    /// configuration and these declared tools offers. A contract the session
    /// is not granted, or whose digest differs from the one the program
    /// names, refuses the session up front rather than failing the program
    /// at that call.
    pub(super) async fn pin_program_contracts(
        &self,
        spec: &api::ProgramSpec,
        session_config: &SessionConfig,
        declared: &[harness::ToolSpec],
        held: &BTreeMap<String, auth::AuthGrantId>,
    ) -> Result<Vec<harness::ContractPin>, AgentApiError> {
        if spec.requires.is_empty() {
            return Ok(Vec::new());
        }
        let available = self
            .program_contracts_for(&spec.requires, session_config, declared, held)
            .await?;
        tools::program::contracts::pin_requirements(
            &available,
            spec.requires
                .iter()
                .map(|requirement| (requirement.contract.as_str(), requirement.digest.as_deref())),
        )
        .map_err(|error| AgentApiError::invalid_request(error.to_string()))
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

fn program_contract_view(
    contract: &tools::program::contracts::BoundContract,
) -> api::ProgramContractView {
    api::ProgramContractView {
        name: contract.name.clone(),
        digest: contract.digest.clone(),
        description: contract.description.clone(),
        input_schema: contract.input_schema.clone(),
        output_schema: contract.output_schema.clone(),
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
