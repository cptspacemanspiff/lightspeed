//! Model generation tool contract: `model_generate` runs one standalone
//! model call per tool call and returns its reply (joined completion). It is
//! a system workflow-tool binding whose start-on-call recipe runs the model
//! generation workflow; the session runtime only admits the call, selects
//! the model from the pinned grant, and hands the invocation to the generic
//! start machinery.
//!
//! The generation sees only the call's `instructions` and `input`: no
//! conversation, files, or tools. An optional `input_schema` validates a JSON
//! `input` at admission, and an optional `output_schema` requires the reply to
//! be a JSON value that the execution validates before resolving the call.

use engine::{GenerateModelConfig, GeneratePolicy, ModelSelection};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::error::{ToolError, ToolResult};

pub const MODEL_GENERATE_LOGICAL_ID: &str = "model.generate";
pub const MODEL_GENERATE_TOOL_NAME: &str = "model_generate";
pub const MODEL_GENERATE_WORKFLOW_TOOL_ID: &str = "model-generate";
pub const MODEL_GENERATE_WORKFLOW_SEMANTIC_TYPE: &str = "lightspeed.model.generate.v1";
pub const MODEL_GENERATE_WORKFLOW_TYPE: &str = "ModelGenerateWorkflow";
/// Hard bound on one generation, provider retries included; the joined
/// promise fails when it passes.
pub const MODEL_GENERATE_DEADLINE_AFTER_MS: u64 = 30 * 60 * 1_000;
pub const MAX_MODEL_GENERATE_INSTRUCTIONS_BYTES: usize = 64 * 1024;
pub const MAX_MODEL_GENERATE_INPUT_BYTES: usize = 512 * 1024;
pub const MAX_MODEL_GENERATE_SCHEMA_BYTES: usize = 64 * 1024;
/// Largest reply text returned to the caller; longer replies fail.
pub const MAX_MODEL_GENERATE_OUTPUT_BYTES: usize = 512 * 1024;
pub const MODEL_GENERATE_EXECUTION_CONTEXT_VERSION: u32 = 1;

/// Model-facing names and descriptions of the granted models, carried in the
/// tool's built-in settings so the advertised schema can enumerate them.
/// Provider routes never reach the model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerateModelChoice {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl From<&GenerateModelConfig> for GenerateModelChoice {
    fn from(entry: &GenerateModelConfig) -> Self {
        Self {
            name: entry.name.clone(),
            description: entry.description.clone(),
        }
    }
}

pub fn is_model_generate_binding(tool_id: &str, semantic_type: &str) -> bool {
    tool_id == MODEL_GENERATE_WORKFLOW_TOOL_ID
        && semantic_type == MODEL_GENERATE_WORKFLOW_SEMANTIC_TYPE
}

/// Model arguments of `model_generate`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelGenerateArgs {
    /// Granted model name; the grant's first model when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    /// Text, or any JSON value sent to the model as JSON.
    pub input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}

impl ModelGenerateArgs {
    pub fn validate(&self) -> ToolResult<()> {
        if let Some(instructions) = &self.instructions
            && instructions.len() > MAX_MODEL_GENERATE_INSTRUCTIONS_BYTES
        {
            return Err(invalid(format!(
                "instructions must be at most {MAX_MODEL_GENERATE_INSTRUCTIONS_BYTES} bytes"
            )));
        }
        match &self.input {
            Value::Null => return Err(invalid("input must not be null")),
            Value::String(text) if text.trim().is_empty() => {
                return Err(invalid("input must be non-empty"));
            }
            _ => {}
        }
        if self.input_text().len() > MAX_MODEL_GENERATE_INPUT_BYTES {
            return Err(invalid(format!(
                "input must be at most {MAX_MODEL_GENERATE_INPUT_BYTES} bytes"
            )));
        }
        if let Some(schema) = &self.input_schema {
            let validator = compile_schema("input_schema", schema)?;
            if let Err(error) = validator.validate(&self.input) {
                return Err(invalid(format!(
                    "input does not match input_schema: {error}"
                )));
            }
        }
        if let Some(schema) = &self.output_schema {
            compile_schema("output_schema", schema)?;
        }
        Ok(())
    }

    /// The user message sent to the model: text verbatim, other JSON
    /// values pretty-printed.
    pub fn input_text(&self) -> String {
        match &self.input {
            Value::String(text) => text.clone(),
            value => serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
        }
    }
}

/// Compile a caller-supplied JSON Schema, bounding its size first.
pub fn compile_schema(field: &str, schema: &Value) -> ToolResult<jsonschema::Validator> {
    if !matches!(schema, Value::Object(_) | Value::Bool(_)) {
        return Err(invalid(format!("{field} must be a JSON Schema object")));
    }
    let bytes = serde_json::to_vec(schema)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if bytes > MAX_MODEL_GENERATE_SCHEMA_BYTES {
        return Err(invalid(format!(
            "{field} must be at most {MAX_MODEL_GENERATE_SCHEMA_BYTES} bytes"
        )));
    }
    jsonschema::validator_for(schema)
        .map_err(|error| invalid(format!("{field} is not a valid JSON Schema: {error}")))
}

/// The model a call runs on, resolved from the pinned grant at admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelGenerateSelection {
    /// Granted entry name; absent when the call uses the turn's model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub model: ModelSelection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
}

/// Select the call's model from the grant. The grant's list is the
/// authority: a listed name selects its entry, an omitted name selects the
/// first entry, and an empty list admits only the calling turn's model.
pub fn select_model(
    policy: &GeneratePolicy,
    requested: Option<&str>,
) -> ToolResult<ModelGenerateSelection> {
    let models = &policy.feature.models;
    if models.is_empty() {
        if let Some(name) = requested {
            return Err(invalid(format!(
                "model {name:?} is not available: this session grants no selectable models; \
                 omit model to use the session's model"
            )));
        }
        return Ok(ModelGenerateSelection {
            name: None,
            model: policy.turn_model.clone(),
            reasoning_effort: None,
            max_output_tokens: None,
        });
    }
    let entry = match requested {
        None => &models[0],
        Some(name) => policy.feature.model(name).ok_or_else(|| {
            invalid(format!(
                "model {name:?} is not granted; choose one of: {}",
                models
                    .iter()
                    .map(|model| model.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?,
    };
    Ok(ModelGenerateSelection {
        name: Some(entry.name.clone()),
        model: entry.model.clone(),
        reasoning_effort: entry.reasoning_effort.clone(),
        max_output_tokens: entry.max_output_tokens,
    })
}

/// Runtime-owned facts pinned when a call is admitted. The execution reads
/// this through the invocation's opaque execution context; it is not part of
/// the model-facing schema, so the model can never name a provider route.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelGenerateExecutionContextV1 {
    pub version: u32,
    pub session_id: String,
    pub run_id: u64,
    pub selection: ModelGenerateSelection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelGenerateStatus {
    Completed,
    Failed,
}

/// How an output schema was enforced. Lightspeed validates the reply in
/// either mode; `native` also constrains the provider's decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelGenerateOutputMode {
    Native,
    Prompted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelGenerateErrorKind {
    /// The call's arguments or pinned context were rejected.
    InvalidRequest,
    /// The provider or its configuration rejected the request.
    Provider,
    /// The reply hit the output-token cap.
    Truncated,
    /// The provider declined to answer.
    Refused,
    /// The reply is not JSON matching `output_schema`.
    OutputInvalid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelGenerateError {
    pub kind: ModelGenerateErrorKind,
    pub message: String,
}

/// The model identity a generation used; the provider route is shown so
/// results and logs name the concrete model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelGenerateModelView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub provider_id: String,
    pub api_kind: String,
    pub model: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelGenerateUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u32>,
}

/// The `reply` payload, for both outcomes. A completed call carries `text`
/// and, with an output schema, the validated `json`; a failed call carries
/// `error` and any reply text that failed validation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelGenerateResultEnvelope {
    pub status: ModelGenerateStatus,
    pub model: ModelGenerateModelView,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_mode: Option<ModelGenerateOutputMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ModelGenerateUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ModelGenerateError>,
}

pub fn model_generate_tool_definition(
    models: &[GenerateModelChoice],
) -> crate::runtime::FunctionDefinition {
    let mut description = "Run one standalone model call and return its reply. The model sees \
        only instructions and input: not this conversation, files, or tools. Pass \
        output_schema (a JSON Schema) to get a JSON reply validated against it in the json \
        field; a reply that does not match fails the call. Several calls in one turn run \
        concurrently."
        .to_owned();
    let mut properties = serde_json::Map::new();
    if let Some(default) = models.first() {
        description.push_str(&format!("\n\nAvailable models (default {}):", default.name));
        for model in models {
            match &model.description {
                Some(text) => description.push_str(&format!("\n- {}: {text}", model.name)),
                None => description.push_str(&format!("\n- {}", model.name)),
            }
        }
        properties.insert(
            "model".to_owned(),
            json!({
                "type": "string",
                "enum": models.iter().map(|model| model.name.as_str()).collect::<Vec<_>>(),
                "description": format!(
                    "Model to run; defaults to {}.",
                    default.name
                ),
            }),
        );
    }
    properties.insert(
        "instructions".to_owned(),
        json!({
            "type": "string",
            "description": "System instructions for the model."
        }),
    );
    properties.insert(
        "input".to_owned(),
        json!({
            "description": "The user message: text, or any JSON value, which the model receives as JSON."
        }),
    );
    properties.insert(
        "input_schema".to_owned(),
        json!({
            "type": "object",
            "description": "Optional JSON Schema that input must match; checked before the model runs."
        }),
    );
    properties.insert(
        "output_schema".to_owned(),
        json!({
            "type": "object",
            "description": "Optional JSON Schema the reply must match. Prefer an object schema with declared properties, required fields, and additionalProperties false."
        }),
    );
    crate::runtime::FunctionDefinition::new(
        MODEL_GENERATE_TOOL_NAME,
        description,
        json!({
            "type": "object",
            "properties": Value::Object(properties),
            "required": ["input"],
            "additionalProperties": false
        }),
    )
}

fn invalid(message: impl Into<String>) -> ToolError {
    ToolError::InvalidRequest {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::{GenerateFeature, ProviderApiKind};

    fn model(name: &str) -> GenerateModelConfig {
        GenerateModelConfig {
            name: name.to_owned(),
            description: None,
            model: ModelSelection {
                api_kind: ProviderApiKind::OpenAiResponses,
                provider_id: "openai".to_owned(),
                model: format!("{name}-model"),
            },
            reasoning_effort: Some("low".to_owned()),
            max_output_tokens: Some(512),
        }
    }

    fn policy(models: Vec<GenerateModelConfig>) -> GeneratePolicy {
        GeneratePolicy {
            feature: GenerateFeature {
                models,
                ..GenerateFeature::default()
            },
            turn_model: ModelSelection {
                api_kind: ProviderApiKind::AnthropicMessages,
                provider_id: "anthropic".to_owned(),
                model: "turn-model".to_owned(),
            },
        }
    }

    #[test]
    fn select_model_admits_only_granted_names() {
        let grant = policy(vec![model("grader"), model("vision")]);
        let default = select_model(&grant, None).expect("default");
        assert_eq!(default.name.as_deref(), Some("grader"));
        assert_eq!(default.model.model, "grader-model");
        assert_eq!(default.reasoning_effort.as_deref(), Some("low"));
        let chosen = select_model(&grant, Some("vision")).expect("listed");
        assert_eq!(chosen.model.model, "vision-model");
        assert!(matches!(
            select_model(&grant, Some("large")),
            Err(ToolError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn empty_grant_uses_turn_model_and_rejects_names() {
        let grant = policy(Vec::new());
        let selected = select_model(&grant, None).expect("turn model");
        assert_eq!(selected.name, None);
        assert_eq!(selected.model.model, "turn-model");
        assert!(matches!(
            select_model(&grant, Some("turn-model")),
            Err(ToolError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn args_validate_input_against_input_schema() {
        let args: ModelGenerateArgs = serde_json::from_value(json!({
            "input": { "claim": "water boils at 100C" },
            "input_schema": {
                "type": "object",
                "properties": { "claim": { "type": "string" } },
                "required": ["claim"]
            },
            "output_schema": { "type": "object" }
        }))
        .expect("decode");
        args.validate().expect("valid");
        assert!(args.input_text().contains("\"claim\""));

        let mismatched = ModelGenerateArgs {
            input: json!({ "claim": 7 }),
            ..args.clone()
        };
        assert!(matches!(
            mismatched.validate(),
            Err(ToolError::InvalidRequest { .. })
        ));
        let bad_schema = ModelGenerateArgs {
            output_schema: Some(json!({ "type": 5 })),
            ..args
        };
        assert!(matches!(
            bad_schema.validate(),
            Err(ToolError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn args_reject_blank_input_and_unknown_fields() {
        let blank: ModelGenerateArgs =
            serde_json::from_value(json!({ "input": "  " })).expect("decode");
        assert!(matches!(
            blank.validate(),
            Err(ToolError::InvalidRequest { .. })
        ));
        assert!(
            serde_json::from_value::<ModelGenerateArgs>(json!({
                "input": "x",
                "provider": "openai"
            }))
            .is_err()
        );
    }

    #[test]
    fn definition_enumerates_granted_models_only_when_listed() {
        let open = model_generate_tool_definition(&[]);
        assert!(open.input_schema["properties"].get("model").is_none());

        let listed = model_generate_tool_definition(&[
            GenerateModelChoice {
                name: "grader".to_owned(),
                description: Some("Cheap classifier".to_owned()),
            },
            GenerateModelChoice {
                name: "vision".to_owned(),
                description: None,
            },
        ]);
        assert_eq!(
            listed.input_schema["properties"]["model"]["enum"],
            json!(["grader", "vision"])
        );
        let description = listed.description.expect("description");
        assert!(description.contains("- grader: Cheap classifier"));
        assert!(description.contains("default grader"));
    }
}
