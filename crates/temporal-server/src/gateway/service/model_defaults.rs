use super::*;

pub(super) fn map_store_error(error: store_pg::ModelDefaultsStoreError) -> AgentApiError {
    match error {
        store_pg::ModelDefaultsStoreError::Conflict { .. } => {
            AgentApiError::conflict(error.to_string())
        }
        store_pg::ModelDefaultsStoreError::Invalid(error) => error,
        other => AgentApiError::internal(other.to_string()),
    }
}

/// A configuration document with every model tier reference replaced by
/// the route it resolved to, ready for the engine translation.
#[derive(Debug)]
pub(super) struct ResolvedSessionConfig {
    pub config: api::SessionConfig,
    pub model: ModelSelection,
    /// One line per resolved reference, for the admission log.
    pub chains: Vec<String>,
}

/// Whether admitting this document needs the universe tier table: a tier
/// reference anywhere, or a configuration table whose aliases may reach it.
pub(super) fn config_uses_model_tiers(config: &api::SessionConfig) -> bool {
    config.models.is_some()
        || config
            .model
            .as_ref()
            .is_some_and(|model| model.tier_name().is_some())
        || config
            .features
            .as_ref()
            .and_then(|features| features.generate.as_ref())
            .is_some_and(|generate| {
                generate
                    .models
                    .iter()
                    .any(|entry| entry.model.tier_name().is_some())
            })
}

/// Resolves every tier reference in a configuration document. With a
/// current model, an omitted `model` preserves it; at creation an omitted
/// model resolves the `default` tier. A route's settings apply where the
/// document leaves the matching setting unset. The `models` table is
/// consumed here: sessions store routes, not tables.
pub(super) fn resolve_session_config(
    mut config: api::SessionConfig,
    universe: &api::ModelDefaults,
    current_model: Option<&ModelSelection>,
) -> Result<ResolvedSessionConfig, AgentApiError> {
    let table = config.models.take();
    if let Some(table) = &table {
        api::ModelTierResolver::validate_config_table(table, universe)?;
    }
    let resolver = api::ModelTierResolver::new(universe, table.as_ref());
    let mut chains = Vec::new();
    let model = match config.model.take() {
        Some(api::ModelRef::Route(route)) => route,
        Some(api::ModelRef::Tier(tier)) => {
            apply_session_tier(&resolver, &tier.tier, &mut config.generation, &mut chains)?
        }
        None => match current_model {
            Some(current) => api_projection::model_to_api(current),
            None => apply_session_tier(
                &resolver,
                api::MODEL_TIER_DEFAULT,
                &mut config.generation,
                &mut chains,
            )?,
        },
    };
    validate_session_model(&model)?;
    let selection = api_config::model_selection_from_api(model.clone())?;
    if let Some(generate) = config
        .features
        .as_mut()
        .and_then(|features| features.generate.as_mut())
    {
        for entry in &mut generate.models {
            let Some(name) = entry.model.tier_name().map(str::to_owned) else {
                continue;
            };
            let resolved = resolver
                .resolve(&name)
                .map_err(|error| with_context(error, format!("generate model {:?}", entry.name)))?;
            if entry.reasoning_effort.is_none() {
                entry.reasoning_effort = resolved.route.reasoning_effort.clone();
            }
            if entry.max_output_tokens.is_none() {
                entry.max_output_tokens = resolved.route.max_output_tokens;
            }
            chains.push(format!(
                "generate {}: {}",
                entry.name,
                resolved.chain_display()
            ));
            entry.model = api::ModelRef::Route(resolved.route.model);
        }
    }
    config.model = Some(api::ModelRef::Route(model));
    Ok(ResolvedSessionConfig {
        config,
        model: selection,
        chains,
    })
}

fn apply_session_tier(
    resolver: &api::ModelTierResolver<'_>,
    name: &str,
    generation: &mut Option<api::GenerationConfig>,
    chains: &mut Vec<String>,
) -> Result<ModelConfig, AgentApiError> {
    let resolved = resolver
        .resolve(name)
        .map_err(|error| with_context(error, "model"))?;
    if resolved.route.reasoning_effort.is_some() || resolved.route.max_output_tokens.is_some() {
        let generation = generation.get_or_insert_with(Default::default);
        if generation.reasoning_effort.is_none() {
            generation.reasoning_effort = resolved.route.reasoning_effort.clone();
        }
        if generation.max_output_tokens.is_none() {
            generation.max_output_tokens = resolved.route.max_output_tokens;
        }
    }
    chains.push(format!("model: {}", resolved.chain_display()));
    Ok(resolved.route.model)
}

/// Resolves a run override's tier through the universe table alone: a
/// session stores routes, so its configuration table is not available to
/// later runs. The route's settings apply where the run leaves them unset.
pub(super) fn resolve_run_start_config(
    mut config: RunStartConfig,
    universe: &api::ModelDefaults,
) -> Result<(RunStartConfig, Option<String>), AgentApiError> {
    let Some(api::ModelRef::Tier(tier)) = config.model.take() else {
        return Ok((config, None));
    };
    let resolved = api::ModelTierResolver::new(universe, None)
        .resolve(&tier.tier)
        .map_err(|error| with_context(error, "run model"))?;
    if resolved.route.reasoning_effort.is_some() || resolved.route.max_output_tokens.is_some() {
        let generation = config.generation.get_or_insert_with(Default::default);
        if generation.reasoning_effort.is_none() {
            generation.reasoning_effort = resolved.route.reasoning_effort.clone();
        }
        if generation.max_output_tokens.is_none() {
            generation.max_output_tokens = resolved.route.max_output_tokens;
        }
    }
    let chain = resolved.chain_display();
    config.model = Some(api::ModelRef::Route(resolved.route.model));
    Ok((config, Some(chain)))
}

/// Keeps the error kind (an unset default stays typed) while naming the
/// field that referenced the tier.
fn with_context(error: AgentApiError, context: impl std::fmt::Display) -> AgentApiError {
    AgentApiError {
        message: format!("{context}: {}", error.message),
        ..error
    }
}

pub(super) fn current_session_model(
    state: &engine::CoreAgentState,
) -> Result<ModelSelection, AgentApiError> {
    state
        .lifecycle
        .config
        .as_ref()
        .map(|config| config.model.clone())
        .ok_or_else(|| AgentApiError::rejected("session has no configuration"))
}

impl GatewayAgentApi {
    pub(super) async fn universe_model_defaults(
        &self,
    ) -> Result<api::ModelDefaults, AgentApiError> {
        self.store
            .read_model_defaults()
            .await
            .map_err(map_store_error)
    }

    /// The universe layer a document's admission needs. An explicit route
    /// with no tier references never consults universe policy.
    pub(super) async fn universe_for_config(
        &self,
        config: &api::SessionConfig,
        creation: bool,
    ) -> Result<api::ModelDefaults, AgentApiError> {
        if config_uses_model_tiers(config) || (creation && config.model.is_none()) {
            self.universe_model_defaults().await
        } else {
            Ok(api::ModelDefaults::default())
        }
    }

    /// Profiles are written with their tier references checked against the
    /// universe, so a dangling name is a configuration error at write time
    /// rather than at the next session start. An unset `default` is not a
    /// profile error: the default may be chosen later.
    pub(super) async fn validate_profile_model_tiers(
        &self,
        document: &api::ProfileDocument,
    ) -> Result<(), AgentApiError> {
        let Some(config) = document.config.as_ref() else {
            return Ok(());
        };
        if !config_uses_model_tiers(config) {
            return Ok(());
        }
        let universe = self.universe_model_defaults().await?;
        match resolve_session_config(config.clone(), &universe, None) {
            Ok(_) => Ok(()),
            Err(error) if error.kind == AgentApiErrorKind::ModelDefaultUnset => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// A session runs on an agent model, or on the code runtime when it is a
/// code sub-agent. The code runtime is never a universe default.
fn validate_session_model(model: &api::ModelConfig) -> Result<(), AgentApiError> {
    let code_runtime = api_projection::api_kind_to_str(&engine::ProviderApiKind::CodeRuntime);
    if model.api_kind != code_runtime {
        return api::ModelDefaultSlot::AgentRun.validate_model(model);
    }
    if model.provider_id != tools::code_run::CODE_RUNTIME_PROVIDER_ID {
        return Err(AgentApiError::invalid_request(format!(
            "API kind {code_runtime} requires provider {}",
            tools::code_run::CODE_RUNTIME_PROVIDER_ID
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(provider: &str, kind: &str, model: &str) -> ModelConfig {
        ModelConfig {
            provider_id: provider.into(),
            api_kind: kind.into(),
            model: model.into(),
        }
    }

    fn universe() -> api::ModelDefaults {
        api::ModelDefaults {
            revision: 1,
            agent_run: Some(route("openai", "openai:responses", "gpt-5.5")),
            speech_to_text: None,
            tiers: BTreeMap::from([
                (
                    "fast".to_owned(),
                    api::ModelTierBinding::Route(api::ModelTierRoute {
                        model: route("openai", "openai:responses", "gpt-5.5-mini"),
                        reasoning_effort: Some("low".into()),
                        max_output_tokens: Some(800),
                    }),
                ),
                ("grader".to_owned(), api::ModelTierBinding::alias("fast")),
            ]),
        }
    }

    #[test]
    fn explicit_routes_do_not_need_universe_policy() {
        let explicit = api::SessionConfig {
            model: Some(route("private", "openai:completions", "custom").into()),
            ..Default::default()
        };
        assert!(!config_uses_model_tiers(&explicit));
        let resolved =
            resolve_session_config(explicit, &api::ModelDefaults::default(), None).unwrap();
        assert_eq!(resolved.model.provider_id, "private");
        assert_eq!(resolved.model.api_kind, ProviderApiKind::OpenAiCompletions);
        assert!(resolved.chains.is_empty());
        assert!(config_uses_model_tiers(&api::SessionConfig {
            model: Some(api::ModelRef::tier("fast")),
            ..Default::default()
        }));
        assert!(config_uses_model_tiers(&api::SessionConfig {
            models: Some(BTreeMap::new()),
            ..Default::default()
        }));
    }

    #[test]
    fn omitted_model_resolves_default_at_creation_and_preserves_current_later() {
        let universe = universe();
        let created =
            resolve_session_config(api::SessionConfig::default(), &universe, None).unwrap();
        assert_eq!(created.model.model, "gpt-5.5");
        assert_eq!(created.chains, vec!["model: default"]);
        assert_eq!(
            created.config.model,
            Some(route("openai", "openai:responses", "gpt-5.5").into())
        );
        let current = ModelSelection {
            provider_id: "anthropic".into(),
            api_kind: ProviderApiKind::AnthropicMessages,
            model: "pinned".into(),
        };
        let existing =
            resolve_session_config(api::SessionConfig::default(), &universe, Some(&current))
                .unwrap();
        assert_eq!(existing.model, current);
        let mut unset = universe.clone();
        unset.agent_run = None;
        let error =
            resolve_session_config(api::SessionConfig::default(), &unset, None).unwrap_err();
        assert_eq!(error.kind, AgentApiErrorKind::ModelDefaultUnset);
        assert_eq!(
            error.model_default_slot,
            Some(api::ModelDefaultSlot::AgentRun)
        );
    }

    #[test]
    fn tier_settings_fill_unset_generation_fields_only() {
        let universe = universe();
        let resolved = resolve_session_config(
            api::SessionConfig {
                model: Some(api::ModelRef::tier("grader")),
                ..Default::default()
            },
            &universe,
            None,
        )
        .unwrap();
        assert_eq!(resolved.model.model, "gpt-5.5-mini");
        let generation = resolved.config.generation.unwrap();
        assert_eq!(generation.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(generation.max_output_tokens, Some(800));
        assert_eq!(resolved.chains, vec!["model: grader -> fast"]);

        let resolved = resolve_session_config(
            api::SessionConfig {
                model: Some(api::ModelRef::tier("grader")),
                generation: Some(api::GenerationConfig {
                    reasoning_effort: Some("high".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &universe,
            None,
        )
        .unwrap();
        let generation = resolved.config.generation.unwrap();
        assert_eq!(generation.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(generation.max_output_tokens, Some(800));
    }

    #[test]
    fn configuration_tables_rebind_names_and_are_consumed() {
        let universe = universe();
        let config = api::SessionConfig {
            model: Some(api::ModelRef::tier("default")),
            models: Some(BTreeMap::from([
                (
                    "default".to_owned(),
                    api::ModelTierBinding::route(route(
                        "anthropic",
                        "anthropic:messages",
                        "claude",
                    )),
                ),
                ("small".to_owned(), api::ModelTierBinding::alias("grader")),
            ])),
            features: Some(api::FeaturesConfig {
                generate: Some(api::GenerateFeature {
                    version: 1,
                    models: vec![api::GenerateModelOption {
                        name: "judge".into(),
                        description: None,
                        model: api::ModelRef::tier("small"),
                        reasoning_effort: None,
                        max_output_tokens: Some(200),
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = resolve_session_config(config, &universe, None).unwrap();
        assert_eq!(resolved.model.provider_id, "anthropic");
        assert!(resolved.config.models.is_none());
        let entry = &resolved.config.features.unwrap().generate.unwrap().models[0];
        assert_eq!(
            entry.model,
            route("openai", "openai:responses", "gpt-5.5-mini").into()
        );
        assert_eq!(entry.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(entry.max_output_tokens, Some(200));
        assert_eq!(
            resolved.chains,
            vec!["model: default", "generate judge: small -> grader -> fast"]
        );
    }

    #[test]
    fn unknown_tiers_fail_admission_with_the_referencing_field() {
        let universe = universe();
        let error = resolve_session_config(
            api::SessionConfig {
                model: Some(api::ModelRef::tier("huge")),
                ..Default::default()
            },
            &universe,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind, AgentApiErrorKind::InvalidRequest);
        assert!(
            error.message.starts_with("model: unknown model tier"),
            "{}",
            error.message
        );
        let error = resolve_session_config(
            api::SessionConfig {
                models: Some(BTreeMap::from([(
                    "x".to_owned(),
                    api::ModelTierBinding::alias("nowhere"),
                )])),
                ..Default::default()
            },
            &universe,
            None,
        )
        .unwrap_err();
        assert!(error.message.starts_with("models.x:"), "{}", error.message);
        let error = resolve_session_config(
            api::SessionConfig {
                features: Some(api::FeaturesConfig {
                    generate: Some(api::GenerateFeature {
                        version: 1,
                        models: vec![api::GenerateModelOption {
                            name: "judge".into(),
                            description: None,
                            model: api::ModelRef::tier("missing"),
                            reasoning_effort: None,
                            max_output_tokens: None,
                        }],
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &universe,
            None,
        )
        .unwrap_err();
        assert!(
            error.message.starts_with("generate model \"judge\":"),
            "{}",
            error.message
        );
    }

    #[test]
    fn run_overrides_resolve_tiers_through_the_universe_table() {
        let universe = universe();
        let (config, chain) = resolve_run_start_config(
            RunStartConfig {
                model: Some(api::ModelRef::tier("grader")),
                generation: Some(api::GenerationConfig {
                    max_output_tokens: Some(50),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &universe,
        )
        .unwrap();
        assert_eq!(chain.as_deref(), Some("grader -> fast"));
        assert_eq!(
            config.model,
            Some(route("openai", "openai:responses", "gpt-5.5-mini").into())
        );
        let generation = config.generation.unwrap();
        assert_eq!(generation.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(generation.max_output_tokens, Some(50));
        let (untouched, chain) =
            resolve_run_start_config(RunStartConfig::default(), &universe).unwrap();
        assert_eq!(untouched, RunStartConfig::default());
        assert!(chain.is_none());
    }

    #[test]
    fn profile_merge_precedes_universe_resolution() {
        let profile = api::SessionConfig {
            model: Some(route("anthropic", "anthropic:messages", "profile").into()),
            ..Default::default()
        };
        for explicit in [None, Some(route("local", "openai:completions", "explicit"))] {
            let expected = explicit
                .clone()
                .map(api::ModelRef::from)
                .or(profile.model.clone());
            let merged = GatewayAgentApi::merge_profile_start_config(
                Some(profile.clone()),
                Some(api::SessionConfig {
                    model: explicit.map(Into::into),
                    ..Default::default()
                }),
            )
            .unwrap();
            assert_eq!(merged.model, expected);
            let resolved =
                resolve_session_config(merged, &api::ModelDefaults::default(), None).unwrap();
            assert_eq!(
                Some(api_projection::model_to_api(&resolved.model).into()),
                expected
            );
        }
    }

    #[test]
    fn applying_sparse_profiles_preserves_model_but_replaces_features() {
        let model =
            api_config::model_selection_from_api(route("local", "openai:completions", "pinned"))
                .unwrap();
        let mut previous =
            engine_session_config_from_api(api::SessionConfig::default(), model.clone()).unwrap();
        previous.features.web = Some(engine::WebFeature {
            version: 1,
            fetch: Some(engine::WebFetchFeature {}),
            search: None,
        });
        let profile = api::ProfileDocument {
            config: Some(api::SessionConfig::default()),
            ..Default::default()
        };
        let applied = GatewayAgentApi::profile_intent(
            &profile,
            previous.model.clone(),
            &api::ModelDefaults::default(),
        )
        .unwrap();
        let config = applied.config.unwrap();
        assert_eq!(config.model, model);
        assert_eq!(config.features.web, None);
        let replaced =
            engine_session_config_from_api(api::SessionConfig::default(), previous.model).unwrap();
        assert_eq!(replaced, config);
        assert!(
            GatewayAgentApi::profile_setup_intent(&profile)
                .config
                .is_none(),
            "creation already merged its profile configuration"
        );
    }

    #[test]
    fn universe_routes_are_independent_and_resolution_is_a_snapshot() {
        let mut first = api::ModelDefaults {
            revision: 1,
            agent_run: Some(route("anthropic", "anthropic:messages", "first")),
            ..Default::default()
        };
        let second = api::ModelDefaults {
            revision: 1,
            agent_run: Some(route("private", "openai:completions", "second")),
            ..Default::default()
        };
        let first_model = resolve_session_config(api::SessionConfig::default(), &first, None)
            .unwrap()
            .model;
        let second_model = resolve_session_config(api::SessionConfig::default(), &second, None)
            .unwrap()
            .model;
        let existing =
            engine_session_config_from_api(api::SessionConfig::default(), first_model).unwrap();
        first.agent_run = None;
        let error =
            resolve_session_config(api::SessionConfig::default(), &first, None).unwrap_err();
        assert_eq!(error.kind, AgentApiErrorKind::ModelDefaultUnset);
        assert_eq!(existing.model.provider_id, "anthropic");
        assert_eq!(second_model.provider_id, "private");
        let replaced =
            engine_session_config_from_api(api::SessionConfig::default(), existing.model.clone())
                .unwrap();
        assert_eq!(replaced.model, existing.model);
    }
}
