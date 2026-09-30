use super::*;

/// The tier a configuration with no model resolves. In the universe layer it
/// is the `agentRun` default; a configuration table may rebind it.
pub const MODEL_TIER_DEFAULT: &str = "default";
pub const MODEL_TIER_NAME_MAX_BYTES: usize = 64;
pub const MODEL_TIER_TABLE_MAX_ENTRIES: usize = 64;

/// Tier names are short identifiers. `/` is reserved for provider-qualified
/// names, which are not part of the shared tables.
pub fn validate_model_tier_name(name: &str) -> Result<(), AgentApiError> {
    if name.is_empty() || name.len() > MODEL_TIER_NAME_MAX_BYTES {
        return Err(AgentApiError::invalid_request(format!(
            "model tier name {name:?} must contain 1..={MODEL_TIER_NAME_MAX_BYTES} bytes"
        )));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AgentApiError::invalid_request(format!(
            "model tier name {name:?} may contain only ASCII letters, digits, '-', '_', or '.'"
        )));
    }
    Ok(())
}

/// A tier bound to a concrete route and the settings that route is used
/// with. Settings live with the route because their valid values depend on
/// the model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelTierRoute {
    pub model: ModelConfig,
    /// Provider-native reasoning effort for calls that resolve to this tier.
    /// A session's or run's explicit setting still wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Output-token cap for calls that resolve to this tier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
}

/// A tier that names another tier. An alias carries only the name; the
/// route at the end of the chain supplies every setting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelTierAlias {
    pub tier: String,
}

/// What a tier resolves to: a concrete route or an alias for another tier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum ModelTierBinding {
    Alias(ModelTierAlias),
    Route(ModelTierRoute),
}

impl ModelTierBinding {
    pub fn alias(tier: impl Into<String>) -> Self {
        Self::Alias(ModelTierAlias { tier: tier.into() })
    }

    pub fn route(model: ModelConfig) -> Self {
        Self::Route(ModelTierRoute {
            model,
            reasoning_effort: None,
            max_output_tokens: None,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelTierBindingWire {
    tier: Option<String>,
    model: Option<ModelConfig>,
    reasoning_effort: Option<String>,
    max_output_tokens: Option<u32>,
}

impl<'de> Deserialize<'de> for ModelTierBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ModelTierBindingWire::deserialize(deserializer)?;
        match wire {
            ModelTierBindingWire {
                tier: Some(tier),
                model: None,
                reasoning_effort: None,
                max_output_tokens: None,
            } => Ok(Self::Alias(ModelTierAlias { tier })),
            ModelTierBindingWire {
                tier: None,
                model: Some(model),
                reasoning_effort,
                max_output_tokens,
            } => Ok(Self::Route(ModelTierRoute {
                model,
                reasoning_effort,
                max_output_tokens,
            })),
            ModelTierBindingWire { tier: Some(_), .. } => Err(serde::de::Error::custom(
                "a tier alias carries only `tier`; settings belong on the route it points at",
            )),
            _ => Err(serde::de::Error::custom(
                "a tier binding is either `{ tier }` or `{ model, reasoningEffort?, maxOutputTokens? }`",
            )),
        }
    }
}

/// A map from tier names to bindings. Tables at several layers merge by
/// name, the more specific layer replacing the more general one.
pub type ModelTierTable = BTreeMap<String, ModelTierBinding>;

/// The layer that defines a tier.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum ModelTierLayer {
    /// The universe table, including the `default` tier from the `agentRun`
    /// default.
    Universe,
    /// The `models` table of a profile or session configuration.
    Config,
}

/// A tier fully resolved to a route, with the chain that led there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelTierResolution {
    pub tier: String,
    pub layer: ModelTierLayer,
    /// Tier names from the requested name to the one bound to the route.
    pub chain: Vec<String>,
    pub route: ModelTierRoute,
}

impl ModelTierResolution {
    pub fn chain_display(&self) -> String {
        self.chain.join(" -> ")
    }
}

/// Resolves tier names through the universe table and an optional
/// configuration table. A name is looked up in the most specific layer that
/// defines it. An alias resolves in its own layer or more general ones,
/// never in a more specific one, so a configuration cannot change what a
/// universe alias means. An alias to its own name reaches through to the
/// next more general layer, which lets a configuration adjust a universe
/// tier by name without restating its route.
#[derive(Clone, Copy, Debug)]
pub struct ModelTierResolver<'a> {
    universe: &'a ModelDefaults,
    config: Option<&'a ModelTierTable>,
}

impl<'a> ModelTierResolver<'a> {
    pub fn new(universe: &'a ModelDefaults, config: Option<&'a ModelTierTable>) -> Self {
        Self { universe, config }
    }

    pub fn universe(&self) -> &'a ModelDefaults {
        self.universe
    }

    pub fn config(&self) -> Option<&'a ModelTierTable> {
        self.config
    }

    /// Every tier name visible to a configuration, most specific layer first.
    pub fn names(&self) -> Vec<(String, ModelTierLayer)> {
        let mut names = Vec::new();
        let mut seen = BTreeSet::new();
        if let Some(config) = self.config {
            for name in config.keys() {
                if seen.insert(name.clone()) {
                    names.push((name.clone(), ModelTierLayer::Config));
                }
            }
        }
        for name in self.universe.tier_names() {
            if seen.insert(name.clone()) {
                names.push((name, ModelTierLayer::Universe));
            }
        }
        names
    }

    /// Looks a name up from `floor` toward the universe. `Ok(None)` means
    /// no layer at or beyond the floor defines it.
    fn lookup(
        &self,
        name: &str,
        floor: ModelTierLayer,
    ) -> Result<Option<(ModelTierLayer, ModelTierBinding)>, AgentApiError> {
        if floor == ModelTierLayer::Config
            && let Some(binding) = self.config.and_then(|config| config.get(name))
        {
            return Ok(Some((ModelTierLayer::Config, binding.clone())));
        }
        self.universe
            .tier_binding(name)
            .map(|binding| binding.map(|binding| (ModelTierLayer::Universe, binding)))
    }

    pub fn resolve(&self, name: &str) -> Result<ModelTierResolution, AgentApiError> {
        validate_model_tier_name(name)?;
        let mut chain = vec![name.to_owned()];
        let mut visited = BTreeSet::new();
        let mut current = name.to_owned();
        let mut floor = ModelTierLayer::Config;
        let mut layer = None;
        // An alias resolves in its own layer or a more general one; an alias
        // to its own name reaches through to the next more general layer.
        // A (layer, name) pair is never revisited, so the walk is bounded by
        // the table sizes.
        loop {
            let Some((found_layer, binding)) = self.lookup(&current, floor)? else {
                return Err(AgentApiError::invalid_request(if chain.len() == 1 {
                    format!("unknown model tier {name:?}")
                } else {
                    format!(
                        "model tier {name:?} does not resolve: {} names unknown tier {current:?}",
                        chain.join(" -> ")
                    )
                }));
            };
            let layer = *layer.get_or_insert(found_layer);
            if !visited.insert((found_layer, current.clone())) {
                return Err(AgentApiError::invalid_request(format!(
                    "model tier {name:?} does not resolve: alias cycle {}",
                    chain.join(" -> ")
                )));
            }
            match binding {
                ModelTierBinding::Route(route) => {
                    return Ok(ModelTierResolution {
                        tier: name.to_owned(),
                        layer,
                        chain,
                        route,
                    });
                }
                ModelTierBinding::Alias(alias) => {
                    validate_model_tier_name(&alias.tier).map_err(|error| {
                        AgentApiError::invalid_request(format!(
                            "model tier {name:?} does not resolve: {}",
                            error.message
                        ))
                    })?;
                    floor = if alias.tier == current {
                        match found_layer {
                            ModelTierLayer::Config => ModelTierLayer::Universe,
                            ModelTierLayer::Universe => {
                                return Err(AgentApiError::invalid_request(format!(
                                    "model tier {name:?} does not resolve: universe tier {current:?} aliases itself"
                                )));
                            }
                        }
                    } else {
                        found_layer
                    };
                    chain.push(alias.tier.clone());
                    current = alias.tier;
                }
            }
        }
    }

    /// Validates a configuration-layer table against a universe: names,
    /// routes, and that every alias resolves.
    pub fn validate_config_table(
        table: &ModelTierTable,
        universe: &ModelDefaults,
    ) -> Result<(), AgentApiError> {
        validate_tier_table_shape(table, false)?;
        let resolver = ModelTierResolver::new(universe, Some(table));
        for name in table.keys() {
            match resolver.resolve(name) {
                Ok(_) => {}
                // An unset default is a deployment state, not a table error.
                Err(error) if error.kind == AgentApiErrorKind::ModelDefaultUnset => {}
                Err(error) => {
                    return Err(AgentApiError::invalid_request(format!(
                        "models.{name}: {}",
                        error.message
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Names, entry count, and route protocols. `default` is rejected in the
/// universe table because that layer binds it through the `agentRun`
/// default; a configuration table may rebind it.
pub fn validate_tier_table_shape(
    table: &ModelTierTable,
    universe_layer: bool,
) -> Result<(), AgentApiError> {
    if table.len() > MODEL_TIER_TABLE_MAX_ENTRIES {
        return Err(AgentApiError::invalid_request(format!(
            "a model tier table holds at most {MODEL_TIER_TABLE_MAX_ENTRIES} tiers"
        )));
    }
    for (name, binding) in table {
        validate_model_tier_name(name)?;
        if universe_layer && name == MODEL_TIER_DEFAULT {
            return Err(AgentApiError::invalid_request(
                "the universe `default` tier is the agentRun default; set it with models/defaults/put",
            ));
        }
        match binding {
            ModelTierBinding::Route(route) => {
                ModelDefaultSlot::AgentRun
                    .validate_model(&route.model)
                    .map_err(|error| {
                        AgentApiError::invalid_request(format!("tier {name:?}: {}", error.message))
                    })?;
                if let Some(effort) = &route.reasoning_effort
                    && (effort.is_empty() || effort.len() > 64)
                {
                    return Err(AgentApiError::invalid_request(format!(
                        "tier {name:?}: reasoningEffort must contain 1..=64 bytes"
                    )));
                }
                if route.max_output_tokens == Some(0) {
                    return Err(AgentApiError::invalid_request(format!(
                        "tier {name:?}: maxOutputTokens must be positive"
                    )));
                }
            }
            ModelTierBinding::Alias(alias) => {
                validate_model_tier_name(&alias.tier).map_err(|error| {
                    AgentApiError::invalid_request(format!("tier {name:?}: {}", error.message))
                })?;
                if universe_layer && alias.tier == *name {
                    return Err(AgentApiError::invalid_request(format!(
                        "tier {name:?} cannot alias itself; a universe tier has no more general layer to reach"
                    )));
                }
            }
        }
    }
    Ok(())
}

impl ModelDefaults {
    /// The universe layer's binding for a name: the table entry, or for
    /// `default` the `agentRun` default. `Ok(None)` means the name is
    /// unknown; an unset `default` is the typed missing-default error.
    pub fn tier_binding(&self, name: &str) -> Result<Option<ModelTierBinding>, AgentApiError> {
        if name == MODEL_TIER_DEFAULT {
            return self
                .agent_run
                .clone()
                .map(|model| Some(ModelTierBinding::route(model)))
                .ok_or_else(|| AgentApiError::model_default_unset(ModelDefaultSlot::AgentRun));
        }
        Ok(self.tiers.get(name).cloned())
    }

    /// Every name the universe layer defines, `default` first.
    pub fn tier_names(&self) -> Vec<String> {
        std::iter::once(MODEL_TIER_DEFAULT.to_owned())
            .chain(self.tiers.keys().cloned())
            .collect()
    }

    /// Validates a replacement universe table: shape, no `default`, and every
    /// alias resolves within the universe layer without cycles.
    pub fn validate_tier_table(&self, table: &ModelTierTable) -> Result<(), AgentApiError> {
        validate_tier_table_shape(table, true)?;
        let candidate = ModelDefaults {
            tiers: table.clone(),
            ..self.clone()
        };
        let resolver = ModelTierResolver::new(&candidate, None);
        for name in table.keys() {
            match resolver.resolve(name) {
                Ok(_) => {}
                // An unset default is a deployment state, not a table error:
                // aliases may point at `default` before it is chosen.
                Err(error) if error.kind == AgentApiErrorKind::ModelDefaultUnset => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelTiersPutParams {
    /// The complete universe tier table; omitted names are removed. The
    /// `default` tier is not part of it.
    pub tiers: ModelTierTable,
    /// Revision returned by models/defaults/read; zero for a universe with no
    /// updates. Tiers and default slots share one revision.
    pub expected_revision: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelTiersResolveParams {
    /// A profile whose `config.models` table forms the configuration layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<ProfileSource>,
    /// A configuration table merged over the profile's, entry by entry, the
    /// way an explicit session configuration merges over its profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models: Option<ModelTierTable>,
}

/// One tier of an effective table with its resolution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedModelTier {
    pub tier: String,
    pub layer: ModelTierLayer,
    /// The binding at the defining layer; absent when `default` is unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<ModelTierBinding>,
    /// Tier names from this tier to the one bound to the route.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain: Vec<String>,
    /// The concrete route and settings this tier uses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<ModelTierRoute>,
    /// Why the tier does not resolve, when it does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelTiersResolveResponse {
    /// Every visible tier, configuration entries first, then the universe.
    pub tiers: Vec<ResolvedModelTier>,
}

/// The effective table with every entry resolved, for the debugging view.
pub fn resolve_model_tier_table(resolver: &ModelTierResolver<'_>) -> Vec<ResolvedModelTier> {
    resolver
        .names()
        .into_iter()
        .map(|(tier, layer)| {
            let binding = match layer {
                ModelTierLayer::Config => resolver
                    .config()
                    .and_then(|table| table.get(&tier).cloned()),
                ModelTierLayer::Universe => resolver.universe().tier_binding(&tier).ok().flatten(),
            };
            match resolver.resolve(&tier) {
                Ok(resolution) => ResolvedModelTier {
                    tier,
                    layer,
                    binding,
                    chain: resolution.chain,
                    route: Some(resolution.route),
                    error: None,
                },
                Err(error) => ResolvedModelTier {
                    tier,
                    layer,
                    binding,
                    chain: Vec::new(),
                    route: None,
                    error: Some(error.message),
                },
            }
        })
        .collect()
}

/// Merges a more specific table over a more general one, entry by entry.
pub fn merge_model_tier_tables(
    general: Option<ModelTierTable>,
    specific: Option<ModelTierTable>,
) -> Option<ModelTierTable> {
    match (general, specific) {
        (None, None) => None,
        (Some(table), None) | (None, Some(table)) => Some(table),
        (Some(mut general), Some(specific)) => {
            general.extend(specific);
            Some(general)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(provider: &str, model: &str) -> ModelConfig {
        ModelConfig {
            provider_id: provider.into(),
            api_kind: "openai:responses".into(),
            model: model.into(),
        }
    }

    fn universe() -> ModelDefaults {
        ModelDefaults {
            revision: 1,
            agent_run: Some(route("openai", "gpt-5.5")),
            speech_to_text: None,
            tiers: BTreeMap::from([
                (
                    "fast".to_owned(),
                    ModelTierBinding::Route(ModelTierRoute {
                        model: route("openai", "gpt-5.5-mini"),
                        reasoning_effort: Some("low".into()),
                        max_output_tokens: Some(800),
                    }),
                ),
                ("grader-fast".to_owned(), ModelTierBinding::alias("fast")),
                ("large".to_owned(), ModelTierBinding::alias("default")),
            ]),
        }
    }

    #[test]
    fn model_refs_decode_tiers_and_routes_with_clear_errors() {
        assert_eq!(
            serde_json::from_value::<ModelRef>(serde_json::json!({"tier": "fast"})).unwrap(),
            ModelRef::tier("fast")
        );
        assert_eq!(
            serde_json::from_value::<ModelRef>(serde_json::json!({
                "providerId": "openai", "apiKind": "openai:responses", "model": "m"
            }))
            .unwrap(),
            ModelRef::Route(route("openai", "m"))
        );
        let error =
            serde_json::from_value::<ModelRef>(serde_json::json!({"tier": "fast", "model": "m"}))
                .unwrap_err()
                .to_string();
        assert!(error.contains("carries only `tier`"), "{error}");
        let error = serde_json::from_value::<ModelRef>(serde_json::json!({"providerId": "openai"}))
            .unwrap_err()
            .to_string();
        assert!(error.contains("complete route"), "{error}");
        assert!(
            serde_json::from_value::<ModelRef>(serde_json::json!({"tier": "fast", "extra": 1}))
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(ModelRef::tier("fast")).unwrap(),
            serde_json::json!({"tier": "fast"})
        );
    }

    #[test]
    fn bindings_decode_aliases_and_routes() {
        let alias: ModelTierBinding =
            serde_json::from_value(serde_json::json!({"tier": "fast"})).unwrap();
        assert_eq!(alias, ModelTierBinding::alias("fast"));
        let bound: ModelTierBinding = serde_json::from_value(serde_json::json!({
            "model": {"providerId": "openai", "apiKind": "openai:responses", "model": "m"},
            "reasoningEffort": "low"
        }))
        .unwrap();
        assert!(
            matches!(bound, ModelTierBinding::Route(ref r) if r.reasoning_effort.as_deref() == Some("low"))
        );
        assert!(
            serde_json::from_value::<ModelTierBinding>(
                serde_json::json!({"tier": "fast", "reasoningEffort": "low"})
            )
            .is_err()
        );
        assert_eq!(
            serde_json::to_value(&alias).unwrap(),
            serde_json::json!({"tier": "fast"})
        );
    }

    #[test]
    fn resolution_follows_alias_chains_to_the_route_settings() {
        let universe = universe();
        let resolver = ModelTierResolver::new(&universe, None);
        let resolved = resolver.resolve("grader-fast").unwrap();
        assert_eq!(resolved.chain, vec!["grader-fast", "fast"]);
        assert_eq!(resolved.layer, ModelTierLayer::Universe);
        assert_eq!(resolved.route.model.model, "gpt-5.5-mini");
        assert_eq!(resolved.route.reasoning_effort.as_deref(), Some("low"));
        let large = resolver.resolve("large").unwrap();
        assert_eq!(large.chain, vec!["large", "default"]);
        assert_eq!(large.route.model.model, "gpt-5.5");
        assert_eq!(large.route.reasoning_effort, None);
    }

    #[test]
    fn config_tables_override_by_name_without_changing_universe_aliases() {
        let universe = universe();
        let config = BTreeMap::from([
            (
                "default".to_owned(),
                ModelTierBinding::route(route("anthropic", "claude")),
            ),
            ("grader".to_owned(), ModelTierBinding::alias("grader-fast")),
            ("fast".to_owned(), ModelTierBinding::alias("fast")),
        ]);
        let resolver = ModelTierResolver::new(&universe, Some(&config));
        // The config rebinds `default`; the universe alias `large -> default`
        // still uses the universe's own default.
        assert_eq!(
            resolver.resolve("default").unwrap().route.model.provider_id,
            "anthropic"
        );
        assert_eq!(
            resolver.resolve("large").unwrap().route.model.provider_id,
            "openai"
        );
        // A config alias resolves in the config layer first, then the universe.
        let grader = resolver.resolve("grader").unwrap();
        assert_eq!(grader.layer, ModelTierLayer::Config);
        assert_eq!(grader.chain, vec!["grader", "grader-fast", "fast"]);
        assert_eq!(grader.route.model.model, "gpt-5.5-mini");
        // A config alias of the same name reaches through to the universe.
        let fast = resolver.resolve("fast").unwrap();
        assert_eq!(fast.chain, vec!["fast", "fast"]);
        assert_eq!(fast.route.model.model, "gpt-5.5-mini");
        let names = resolver.names();
        assert_eq!(names[0], ("default".to_owned(), ModelTierLayer::Config));
        assert!(names.contains(&("large".to_owned(), ModelTierLayer::Universe)));
        assert_eq!(names.iter().filter(|(name, _)| name == "fast").count(), 1);
    }

    #[test]
    fn unknown_tiers_cycles_and_unset_defaults_are_distinct_errors() {
        let mut universe = universe();
        let resolver = ModelTierResolver::new(&universe, None);
        let error = resolver.resolve("missing").unwrap_err();
        assert_eq!(error.kind, AgentApiErrorKind::InvalidRequest);
        assert!(
            error.message.contains("unknown model tier"),
            "{}",
            error.message
        );
        assert!(resolver.resolve("bad/name").is_err());

        universe
            .tiers
            .insert("a".to_owned(), ModelTierBinding::alias("b"));
        universe
            .tiers
            .insert("b".to_owned(), ModelTierBinding::alias("a"));
        let error = ModelTierResolver::new(&universe, None)
            .resolve("a")
            .unwrap_err();
        assert!(
            error.message.contains("alias cycle a -> b -> a"),
            "{}",
            error.message
        );
        universe
            .tiers
            .insert("dangling".to_owned(), ModelTierBinding::alias("nowhere"));
        let error = ModelTierResolver::new(&universe, None)
            .resolve("dangling")
            .unwrap_err();
        assert!(
            error.message.contains("unknown tier \"nowhere\""),
            "{}",
            error.message
        );

        universe.agent_run = None;
        let error = ModelTierResolver::new(&universe, None)
            .resolve("large")
            .unwrap_err();
        assert_eq!(error.kind, AgentApiErrorKind::ModelDefaultUnset);
    }

    #[test]
    fn universe_table_validation_rejects_default_cycles_and_bad_routes() {
        let universe = universe();
        universe.validate_tier_table(&universe.tiers).unwrap();
        let mut table = universe.tiers.clone();
        table.insert("default".to_owned(), ModelTierBinding::alias("fast"));
        assert!(
            universe
                .validate_tier_table(&table)
                .unwrap_err()
                .message
                .contains("agentRun")
        );
        let mut table = universe.tiers.clone();
        table.insert("loop".to_owned(), ModelTierBinding::alias("loop"));
        assert!(universe.validate_tier_table(&table).is_err());
        let mut table = universe.tiers.clone();
        table.insert(
            "speech".to_owned(),
            ModelTierBinding::route(ModelConfig {
                provider_id: "openai".into(),
                api_kind: "openai:audio-transcriptions".into(),
                model: "whisper".into(),
            }),
        );
        assert!(universe.validate_tier_table(&table).is_err());
        // An alias to `default` is valid before the default is chosen.
        let unset = ModelDefaults {
            agent_run: None,
            ..universe.clone()
        };
        unset.validate_tier_table(&universe.tiers).unwrap();
    }

    #[test]
    fn config_table_validation_rejects_dangling_and_self_referential_entries() {
        let universe = universe();
        let ok = BTreeMap::from([("grader".to_owned(), ModelTierBinding::alias("fast"))]);
        ModelTierResolver::validate_config_table(&ok, &universe).unwrap();
        let dangling = BTreeMap::from([("grader".to_owned(), ModelTierBinding::alias("nope"))]);
        let error = ModelTierResolver::validate_config_table(&dangling, &universe).unwrap_err();
        assert!(
            error.message.starts_with("models.grader:"),
            "{}",
            error.message
        );
        let selfish = BTreeMap::from([("own".to_owned(), ModelTierBinding::alias("own"))]);
        assert!(ModelTierResolver::validate_config_table(&selfish, &universe).is_err());
    }

    #[test]
    fn resolved_table_view_reports_layers_chains_and_errors() {
        let mut universe = universe();
        universe
            .tiers
            .insert("broken".to_owned(), ModelTierBinding::alias("nowhere"));
        let config =
            BTreeMap::from([("grader".to_owned(), ModelTierBinding::alias("grader-fast"))]);
        let view = resolve_model_tier_table(&ModelTierResolver::new(&universe, Some(&config)));
        let grader = view.iter().find(|entry| entry.tier == "grader").unwrap();
        assert_eq!(grader.layer, ModelTierLayer::Config);
        assert_eq!(grader.chain, vec!["grader", "grader-fast", "fast"]);
        assert_eq!(grader.route.as_ref().unwrap().model.model, "gpt-5.5-mini");
        let broken = view.iter().find(|entry| entry.tier == "broken").unwrap();
        assert!(broken.route.is_none());
        assert!(broken.error.as_deref().unwrap().contains("nowhere"));
        let default = view.iter().find(|entry| entry.tier == "default").unwrap();
        assert_eq!(default.layer, ModelTierLayer::Universe);
        assert_eq!(default.chain, vec!["default"]);
    }

    #[test]
    fn table_merge_prefers_the_specific_layer_by_name() {
        let general = BTreeMap::from([
            ("a".to_owned(), ModelTierBinding::alias("fast")),
            ("b".to_owned(), ModelTierBinding::alias("fast")),
        ]);
        let specific = BTreeMap::from([("b".to_owned(), ModelTierBinding::alias("large"))]);
        let merged = merge_model_tier_tables(Some(general.clone()), Some(specific)).unwrap();
        assert_eq!(merged["a"], ModelTierBinding::alias("fast"));
        assert_eq!(merged["b"], ModelTierBinding::alias("large"));
        assert_eq!(merge_model_tier_tables(None, None), None);
        assert_eq!(
            merge_model_tier_tables(Some(general.clone()), None),
            Some(general)
        );
    }
}
