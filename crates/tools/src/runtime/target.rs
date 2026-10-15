//! Model and provider target for model-visible tool shaping.

use harness::{ModelSelection, ProviderApiKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolTarget {
    pub api_kind: ProviderApiKind,
    pub provider_id: String,
    pub model: String,
}

impl ToolTarget {
    pub fn new(
        api_kind: ProviderApiKind,
        provider_id: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            api_kind,
            provider_id: provider_id.into(),
            model: model.into(),
        }
    }

    pub fn api_kind(api_kind: ProviderApiKind) -> Self {
        Self::new(api_kind, "", "")
    }

    /// The target program bindings resolve against. A program has no model
    /// provider, so it can call only host-executed tools: the Completions
    /// API kind hosts none itself, so every tool with a host function
    /// resolves to it (`web.fetch` included) and provider-hosted tools
    /// such as `web.search` have no program binding.
    pub fn program() -> Self {
        Self::api_kind(ProviderApiKind::OpenAiCompletions)
    }
}

impl From<&harness::BuiltinToolCallRuntime> for ToolTarget {
    /// The target a built-in call executes against: the program target for
    /// a program's call, else the model the call was presented to.
    fn from(builtin: &harness::BuiltinToolCallRuntime) -> Self {
        if builtin.program {
            Self::program()
        } else {
            Self::from(&builtin.model)
        }
    }
}

impl From<&ModelSelection> for ToolTarget {
    fn from(model: &ModelSelection) -> Self {
        Self {
            api_kind: model.api_kind.clone(),
            provider_id: model.provider_id.clone(),
            model: model.model.clone(),
        }
    }
}
