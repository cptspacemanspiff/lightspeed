use anyhow::Result;
use clap::{Args, Subcommand};

use crate::api_client::HttpAgentApi;

#[derive(Debug, Args)]
pub struct ModelTiersArgs {
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: ModelTiersCommand,
}

#[derive(Debug, Subcommand)]
enum ModelTiersCommand {
    /// List this universe's tier table; `default` is the agent-run default.
    List,
    /// Bind a tier to a complete provider/API/model route and its settings.
    Set {
        name: String,
        #[arg(long)]
        provider: String,
        #[arg(long)]
        api_kind: String,
        #[arg(long)]
        model: String,
        #[arg(long)]
        reasoning_effort: Option<String>,
        #[arg(long)]
        max_output_tokens: Option<u32>,
        /// Use this revision; otherwise read it immediately before updating.
        #[arg(long)]
        expected_revision: Option<u64>,
    },
    /// Make a tier an alias for another universe tier, including `default`.
    Alias {
        name: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        expected_revision: Option<u64>,
    },
    /// Remove a tier; sessions that resolved it keep their model.
    Remove {
        name: String,
        #[arg(long)]
        expected_revision: Option<u64>,
    },
    /// Show every tier a configuration can name with its layer, alias
    /// chain, and concrete route, optionally under a profile's table.
    Resolve {
        #[arg(long)]
        profile: Option<String>,
    },
}

pub async fn run(args: ModelTiersArgs) -> Result<()> {
    let client = HttpAgentApi::new("");
    let defaults = match args.command {
        ModelTiersCommand::List => read(&client).await?,
        ModelTiersCommand::Set {
            name,
            provider,
            api_kind,
            model,
            reasoning_effort,
            max_output_tokens,
            expected_revision,
        } => {
            let route = api::ModelTierRoute {
                model: api::ModelConfig {
                    provider_id: provider,
                    api_kind,
                    model,
                },
                reasoning_effort,
                max_output_tokens,
            };
            api::validate_model_tier_name(&name)?;
            api::ModelDefaultSlot::AgentRun.validate_model(&route.model)?;
            let mut current = current(&client, expected_revision).await?;
            current
                .tiers
                .insert(name, api::ModelTierBinding::Route(route));
            put(&client, current).await?
        }
        ModelTiersCommand::Alias {
            name,
            to,
            expected_revision,
        } => {
            api::validate_model_tier_name(&name)?;
            api::validate_model_tier_name(&to)?;
            let mut current = current(&client, expected_revision).await?;
            current.tiers.insert(name, api::ModelTierBinding::alias(to));
            put(&client, current).await?
        }
        ModelTiersCommand::Remove {
            name,
            expected_revision,
        } => {
            let mut current = current(&client, expected_revision).await?;
            if current.tiers.remove(&name).is_none() {
                anyhow::bail!("tier {name:?} is not in the universe table");
            }
            put(&client, current).await?
        }
        ModelTiersCommand::Resolve { profile } => {
            let response: api::AgentApiOutcome<api::ModelTiersResolveResponse> = client
                .request(
                    api::METHOD_MODELS_TIERS_RESOLVE,
                    api::ModelTiersResolveParams {
                        profile: profile
                            .map(|profile_id| {
                                api::ProfileId::try_new(profile_id)
                                    .map(|profile_id| api::ProfileSource::Named { profile_id })
                            })
                            .transpose()?,
                        models: None,
                    },
                )
                .await?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&response.result)?);
            } else {
                print_resolved(&response.result.tiers);
            }
            return Ok(());
        }
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&defaults)?);
    } else {
        print_table(&defaults);
    }
    Ok(())
}

fn print_table(defaults: &api::ModelDefaults) {
    println!("Model tiers (revision {})", defaults.revision);
    match &defaults.agent_run {
        Some(model) => println!("default: {}", route_label(model)),
        None => println!("default: unset (models defaults set agent-run)"),
    }
    for (name, binding) in &defaults.tiers {
        println!("{name}: {}", binding_label(binding));
    }
}

fn print_resolved(tiers: &[api::ResolvedModelTier]) {
    for tier in tiers {
        let layer = match tier.layer {
            api::ModelTierLayer::Universe => "universe",
            api::ModelTierLayer::Config => "config",
        };
        match (&tier.route, &tier.error) {
            (Some(route), _) => println!(
                "{} [{layer}] {} = {}",
                tier.tier,
                tier.chain.join(" -> "),
                route_settings_label(route)
            ),
            (None, Some(error)) => println!("{} [{layer}] unresolved: {error}", tier.tier),
            (None, None) => println!("{} [{layer}] unresolved", tier.tier),
        }
    }
}

fn route_label(model: &api::ModelConfig) -> String {
    format!(
        "{} / {} / {}",
        model.provider_id, model.api_kind, model.model
    )
}

fn route_settings_label(route: &api::ModelTierRoute) -> String {
    let mut label = route_label(&route.model);
    if let Some(effort) = &route.reasoning_effort {
        label.push_str(&format!(" (reasoning {effort})"));
    }
    if let Some(cap) = route.max_output_tokens {
        label.push_str(&format!(" (max output {cap})"));
    }
    label
}

fn binding_label(binding: &api::ModelTierBinding) -> String {
    match binding {
        api::ModelTierBinding::Alias(alias) => format!("-> {}", alias.tier),
        api::ModelTierBinding::Route(route) => route_settings_label(route),
    }
}

async fn read(client: &HttpAgentApi) -> Result<api::ModelDefaults> {
    let response: api::AgentApiOutcome<api::ModelDefaultsResponse> = client
        .request(
            api::METHOD_MODELS_DEFAULTS_READ,
            api::ModelDefaultsReadParams {},
        )
        .await?;
    Ok(response.result.defaults)
}

/// The table an edit starts from. With an explicit revision the caller has
/// reviewed that state; otherwise the read immediately precedes the write.
async fn current(client: &HttpAgentApi, revision: Option<u64>) -> Result<api::ModelDefaults> {
    let mut defaults = read(client).await?;
    if let Some(revision) = revision {
        defaults.revision = revision;
    }
    Ok(defaults)
}

async fn put(client: &HttpAgentApi, defaults: api::ModelDefaults) -> Result<api::ModelDefaults> {
    let response: api::AgentApiOutcome<api::ModelDefaultsResponse> = client
        .request(
            api::METHOD_MODELS_TIERS_PUT,
            api::ModelTiersPutParams {
                tiers: defaults.tiers,
                expected_revision: defaults.revision,
            },
        )
        .await?;
    // A concurrent change is returned to the caller; never retry a write
    // against a newer revision without the caller reviewing that state.
    Ok(response.result.defaults)
}

#[cfg(test)]
mod tests {
    use crate::Cli;
    use clap::Parser;

    #[test]
    fn tier_commands_parse_routes_aliases_and_removals() {
        for args in [
            vec!["lightspeed", "models", "tiers", "list", "--json"],
            vec![
                "lightspeed",
                "models",
                "tiers",
                "set",
                "fast",
                "--provider",
                "openai",
                "--api-kind",
                "openai:responses",
                "--model",
                "gpt-5.5-mini",
                "--reasoning-effort",
                "low",
                "--max-output-tokens",
                "800",
            ],
            vec![
                "lightspeed",
                "model",
                "tiers",
                "alias",
                "grader",
                "--to",
                "fast",
            ],
            vec!["lightspeed", "model", "tiers", "remove", "grader"],
            vec![
                "lightspeed",
                "model",
                "tiers",
                "resolve",
                "--profile",
                "reviewer",
                "--json",
            ],
        ] {
            Cli::try_parse_from(args).unwrap();
        }
        assert!(
            Cli::try_parse_from([
                "lightspeed",
                "models",
                "tiers",
                "set",
                "fast",
                "--model",
                "m"
            ])
            .is_err()
        );
        assert!(Cli::try_parse_from(["lightspeed", "models", "tiers", "alias", "grader"]).is_err());
    }
}
