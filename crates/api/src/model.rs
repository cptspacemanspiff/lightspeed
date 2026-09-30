use super::*;

/// A concrete provider route: where a call goes and which wire protocol it
/// speaks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelConfig {
    pub provider_id: String,
    pub api_kind: String,
    pub model: String,
}

/// A model tier reference: the name is resolved through the universe and
/// configuration tier tables when the document is admitted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelTierRef {
    pub tier: String,
}

/// A model in configuration input: either a concrete route or a tier name.
/// Tier names resolve to a concrete route when the configuration is admitted,
/// so documents read back from a session always carry a route.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum ModelRef {
    Tier(ModelTierRef),
    Route(ModelConfig),
}

impl ModelRef {
    pub fn tier(name: impl Into<String>) -> Self {
        Self::Tier(ModelTierRef { tier: name.into() })
    }

    pub fn route(&self) -> Option<&ModelConfig> {
        match self {
            Self::Route(route) => Some(route),
            Self::Tier(_) => None,
        }
    }

    pub fn tier_name(&self) -> Option<&str> {
        match self {
            Self::Tier(tier) => Some(tier.tier.as_str()),
            Self::Route(_) => None,
        }
    }
}

impl From<ModelConfig> for ModelRef {
    fn from(route: ModelConfig) -> Self {
        Self::Route(route)
    }
}

/// Wire shape shared by both variants so mistakes name the missing field
/// instead of an opaque untagged-enum failure.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelRefWire {
    tier: Option<String>,
    provider_id: Option<String>,
    api_kind: Option<String>,
    model: Option<String>,
}

impl<'de> Deserialize<'de> for ModelRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ModelRefWire::deserialize(deserializer)?;
        match wire {
            ModelRefWire {
                tier: Some(tier),
                provider_id: None,
                api_kind: None,
                model: None,
            } => Ok(Self::Tier(ModelTierRef { tier })),
            ModelRefWire {
                tier: None,
                provider_id: Some(provider_id),
                api_kind: Some(api_kind),
                model: Some(model),
            } => Ok(Self::Route(ModelConfig {
                provider_id,
                api_kind,
                model,
            })),
            ModelRefWire { tier: Some(_), .. } => Err(serde::de::Error::custom(
                "a model tier reference carries only `tier`; a route carries providerId, apiKind, and model",
            )),
            _ => Err(serde::de::Error::custom(
                "a model is either `{ tier }` or a complete route with providerId, apiKind, and model",
            )),
        }
    }
}
