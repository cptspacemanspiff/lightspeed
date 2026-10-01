//! Standalone generation: one provider call with no session conversation,
//! tools, or continuation, used by model-calling tools.
//!
//! An output schema is lowered into the provider's native structured-output
//! setting when the schema fits that provider's supported subset, and is
//! otherwise stated in the instructions. Either way the caller validates the
//! reply against the full schema; native mode only improves the odds.

use engine::{
    ContentRef, ContextEntry, ContextEntryId, ContextEntryInput, ContextEntryKind,
    ContextEntrySource, ContextMessageRole, ContextSnapshot, LlmRequest, ModelSelection,
    ProviderApiKind, ProviderParams, storage::BlobStore,
};
use serde_json::{Value, json};

use crate::{
    blob_io::{put_text, read_json, read_text},
    error::{LlmAdapterError, LlmAdapterResult},
    params::{AnthropicMessagesParams, anthropic_thinking_from_effort},
};

/// Name given to the schema where a provider requires one.
const OUTPUT_SCHEMA_NAME: &str = "result";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneGeneration {
    pub model: ModelSelection,
    pub instructions: Option<String>,
    pub input: String,
    pub output_schema: Option<Value>,
    pub reasoning_effort: Option<String>,
    pub max_output_tokens: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StructuredOutputMode {
    /// The provider constrains the reply to the schema.
    Native,
    /// The schema is stated in the instructions.
    Prompted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedStandaloneGeneration {
    pub request: LlmRequest,
    pub structured_output: Option<StructuredOutputMode>,
}

/// Build the provider-neutral request for one standalone generation: an
/// optional instructions entry and one user message, with no tools.
pub async fn prepare_standalone_generation(
    blobs: &dyn BlobStore,
    generation: &StandaloneGeneration,
) -> LlmAdapterResult<PreparedStandaloneGeneration> {
    let model = &generation.model;
    let mut instructions = generation.instructions.clone().unwrap_or_default();
    let mut params = None;
    let structured_output = match &generation.output_schema {
        None => None,
        Some(schema) => {
            match native_output_params(model, schema, generation.reasoning_effort.as_deref())? {
                Some(native) => {
                    params = Some(native);
                    Some(StructuredOutputMode::Native)
                }
                None => {
                    append_paragraph(&mut instructions, &prompted_output_instructions(schema));
                    Some(StructuredOutputMode::Prompted)
                }
            }
        }
    };

    let mut entries = Vec::with_capacity(2);
    if !instructions.trim().is_empty() {
        entries.push(entry(
            1,
            ContextEntryKind::Instructions,
            put_text(blobs, instructions).await?,
        ));
    }
    entries.push(entry(
        entries.len() as u64 + 1,
        ContextEntryKind::Message {
            role: ContextMessageRole::User,
        },
        put_text(blobs, generation.input.clone()).await?,
    ));

    Ok(PreparedStandaloneGeneration {
        request: LlmRequest {
            model: model.clone(),
            request_fingerprint: "standalone".to_owned(),
            context: ContextSnapshot {
                api_kind: model.api_kind.clone(),
                context_revision: 0,
                entries,
                token_estimate: None,
            },
            tools: Vec::new(),
            tool_choice: None,
            output_limit: generation.max_output_tokens,
            reasoning_effort: generation.reasoning_effort.clone(),
            parallel_tool_use: None,
            processing_tier: None,
            provider_response_id: None,
            compaction: None,
            params,
        },
        structured_output,
    })
}

fn entry(id: u64, kind: ContextEntryKind, content_ref: engine::BlobRef) -> ContextEntry {
    ContextEntry {
        origin: None,
        entry_id: ContextEntryId::new(id),
        key: None,
        kind,
        source: ContextEntrySource::ContextEdit,
        content: ContentRef::text(content_ref),
        preview: None,
        provenance_ref: None,
        token_estimate: None,
        supersedes: None,
    }
}

fn append_paragraph(text: &mut String, paragraph: &str) {
    if !text.trim().is_empty() {
        text.push_str("\n\n");
    }
    text.push_str(paragraph);
}

fn prompted_output_instructions(schema: &Value) -> String {
    let schema = serde_json::to_string_pretty(schema).unwrap_or_else(|_| schema.to_string());
    format!(
        "Respond with only a JSON value, without code fences or any other text, that is \
         valid against this JSON Schema:\n{schema}"
    )
}

/// Provider params that constrain the reply natively, or `None` when this
/// route or schema needs prompted output instead.
fn native_output_params(
    model: &ModelSelection,
    schema: &Value,
    reasoning_effort: Option<&str>,
) -> LlmAdapterResult<Option<ProviderParams>> {
    if !is_object_schema(schema) {
        return Ok(None);
    }
    let body = match model.api_kind {
        ProviderApiKind::OpenAiResponses => json!({
            "text": { "format": {
                "type": "json_schema",
                "name": OUTPUT_SCHEMA_NAME,
                "schema": schema,
                "strict": openai_strict_compatible(schema),
            }}
        }),
        // Compatible Chat Completions servers vary in which response formats
        // they accept; only the built-in provider is known to support schemas.
        ProviderApiKind::OpenAiCompletions if model.provider_id == "openai" => json!({
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": OUTPUT_SCHEMA_NAME,
                    "schema": schema,
                    "strict": openai_strict_compatible(schema),
                }
            }
        }),
        ProviderApiKind::OpenAiCompletions | ProviderApiKind::CodeRuntime => return Ok(None),
        ProviderApiKind::AnthropicMessages => {
            if !anthropic_native_compatible(schema) {
                return Ok(None);
            }
            // The adapter derives thinking and effort only when params leave
            // output_config unset, so merge them here next to the format.
            let mut params = AnthropicMessagesParams::default();
            let mut output_config = serde_json::Map::new();
            if let Some(effort) = reasoning_effort {
                let derived = anthropic_thinking_from_effort(effort)?;
                params.thinking = Some(derived.thinking);
                if let Some(Value::Object(derived)) = derived.output_config {
                    output_config.extend(derived);
                }
            }
            output_config.insert(
                "format".to_owned(),
                json!({ "type": "json_schema", "schema": schema }),
            );
            params.output_config = Some(Value::Object(output_config));
            serde_json::to_value(params).map_err(|error| {
                LlmAdapterError::InvalidProviderRequest {
                    message: format!("encode structured output params: {error}"),
                }
            })?
        }
    };
    let params = ProviderParams::new(model.api_kind.clone(), body);
    crate::params::validate_provider_params(&params)?;
    Ok(Some(params))
}

fn is_object_schema(schema: &Value) -> bool {
    schema.get("type").and_then(Value::as_str) == Some("object")
}

/// Keywords outside the subset both strict OpenAI schemas and Anthropic
/// structured outputs accept. A schema using any of them is enforced only by
/// Lightspeed's own validation.
const UNPORTABLE_KEYWORDS: &[&str] = &[
    "$ref",
    "$dynamicRef",
    "allOf",
    "oneOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "dependentRequired",
    "patternProperties",
    "propertyNames",
    "unevaluatedProperties",
    "unevaluatedItems",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "pattern",
    "minItems",
    "maxItems",
    "uniqueItems",
    "contains",
    "minProperties",
    "maxProperties",
];

/// Anthropic accepts a subset of JSON Schema and requires every object to
/// close its properties.
fn anthropic_native_compatible(schema: &Value) -> bool {
    walk_schema(schema, &|object| {
        !UNPORTABLE_KEYWORDS
            .iter()
            .any(|keyword| object.contains_key(*keyword))
            && (!declares_object(object)
                || object.get("additionalProperties") == Some(&Value::Bool(false)))
    })
}

/// OpenAI strict mode additionally requires every declared property to be
/// required; a schema that is not strict-compatible is still sent natively
/// with `strict: false`.
fn openai_strict_compatible(schema: &Value) -> bool {
    walk_schema(schema, &|object| {
        if UNPORTABLE_KEYWORDS
            .iter()
            .any(|keyword| object.contains_key(*keyword))
        {
            return false;
        }
        if !declares_object(object) {
            return true;
        }
        if object.get("additionalProperties") != Some(&Value::Bool(false)) {
            return false;
        }
        let required = object
            .get("required")
            .and_then(Value::as_array)
            .map(|required| {
                required
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        object
            .get("properties")
            .and_then(Value::as_object)
            .is_none_or(|properties| {
                properties
                    .keys()
                    .all(|key| required.contains(&key.as_str()))
            })
    })
}

fn declares_object(object: &serde_json::Map<String, Value>) -> bool {
    object.get("type").and_then(Value::as_str) == Some("object")
        || object.contains_key("properties")
}

/// Apply `check` to every subschema object; `false` from any stops the walk.
fn walk_schema(schema: &Value, check: &dyn Fn(&serde_json::Map<String, Value>) -> bool) -> bool {
    let Value::Object(object) = schema else {
        return true;
    };
    if !check(object) {
        return false;
    }
    let map_children = ["properties", "$defs", "definitions"];
    let single_children = ["items", "additionalProperties"];
    let list_children = ["anyOf", "prefixItems"];
    map_children.iter().all(|key| {
        object
            .get(*key)
            .and_then(Value::as_object)
            .is_none_or(|children| children.values().all(|child| walk_schema(child, check)))
    }) && single_children.iter().all(|key| {
        object
            .get(*key)
            .is_none_or(|child| walk_schema(child, check))
    }) && list_children.iter().all(|key| {
        object
            .get(*key)
            .and_then(Value::as_array)
            .is_none_or(|children| children.iter().all(|child| walk_schema(child, check)))
    })
}

/// The assistant's reply text from a generation's committed entries, across
/// every provider's native message encoding. Reasoning and other entry kinds
/// are skipped.
pub async fn assistant_output_text(
    blobs: &dyn BlobStore,
    entries: &[ContextEntryInput],
) -> LlmAdapterResult<String> {
    let mut parts = Vec::new();
    for entry in entries {
        if !matches!(
            entry.kind,
            ContextEntryKind::Message {
                role: ContextMessageRole::Assistant
            }
        ) {
            continue;
        }
        let content = &entry.content;
        if content.media_type.as_deref() == Some("application/json") {
            let raw = read_json(blobs, &content.content_ref).await?;
            let text = match content.provider_kind.as_deref() {
                Some(engine::ANTHROPIC_MESSAGES_TEXT_BLOCKS_PROVIDER_KIND) => {
                    llm_clients::content::anthropic_text_blocks(&raw)
                }
                Some(engine::OPENAI_RESPONSES_MESSAGE_PROVIDER_KIND) => {
                    llm_clients::content::openai_response_message(&raw)
                }
                Some(llm_clients::content::OPENAI_COMPLETIONS_MESSAGE_PROVIDER_KIND) => {
                    llm_clients::content::openai_completion_message(&raw)
                }
                _ => None,
            };
            if let Some(text) = text {
                parts.push(text);
            }
        } else {
            parts.push(read_text(blobs, &content.content_ref).await?);
        }
    }
    Ok(parts.join("\n\n"))
}

/// Parse a JSON reply, tolerating one surrounding Markdown code fence that
/// prompted replies sometimes add despite instructions.
pub fn parse_json_reply(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str(trimmed) {
        return Some(value);
    }
    let fenced = trimmed.strip_prefix("```")?.strip_suffix("```")?;
    let body = fenced
        .strip_prefix("json")
        .or_else(|| fenced.strip_prefix("JSON"))
        .unwrap_or(fenced);
    serde_json::from_str(body.trim()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::storage::InMemoryBlobStore;

    fn model(api_kind: ProviderApiKind, provider_id: &str) -> ModelSelection {
        ModelSelection {
            api_kind,
            provider_id: provider_id.to_owned(),
            model: "m".to_owned(),
        }
    }

    fn closed_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "verdict": { "type": "string", "enum": ["pass", "fail"] },
                "notes": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["verdict", "notes"],
            "additionalProperties": false
        })
    }

    #[test]
    fn openai_responses_lowers_schema_with_strictness_from_shape() {
        let responses = model(ProviderApiKind::OpenAiResponses, "openai");
        let params = native_output_params(&responses, &closed_schema(), None)
            .expect("lower")
            .expect("native");
        assert_eq!(params.body["text"]["format"]["type"], "json_schema");
        assert_eq!(params.body["text"]["format"]["strict"], true);

        let open = json!({ "type": "object", "properties": { "a": { "type": "string" } } });
        let params = native_output_params(&responses, &open, None)
            .expect("lower")
            .expect("native");
        assert_eq!(params.body["text"]["format"]["strict"], false);
    }

    #[test]
    fn anthropic_merges_effort_with_format_and_rejects_unportable_schemas() {
        let anthropic = model(ProviderApiKind::AnthropicMessages, "anthropic");
        let params = native_output_params(&anthropic, &closed_schema(), Some("low"))
            .expect("lower")
            .expect("native");
        assert_eq!(params.body["output_config"]["effort"], "low");
        assert_eq!(
            params.body["output_config"]["format"]["type"],
            "json_schema"
        );
        assert_eq!(params.body["thinking"]["type"], "adaptive");

        let bounded = json!({
            "type": "object",
            "properties": { "score": { "type": "integer", "minimum": 0 } },
            "additionalProperties": false
        });
        assert_eq!(
            native_output_params(&anthropic, &bounded, None).expect("lower"),
            None
        );
        let open = json!({ "type": "object", "properties": {} });
        assert_eq!(
            native_output_params(&anthropic, &open, None).expect("lower"),
            None
        );
    }

    #[test]
    fn compatible_completions_and_non_object_schemas_use_prompted_output() {
        let custom = model(ProviderApiKind::OpenAiCompletions, "local-llama");
        assert_eq!(
            native_output_params(&custom, &closed_schema(), None).expect("lower"),
            None
        );
        let builtin = model(ProviderApiKind::OpenAiCompletions, "openai");
        assert!(
            native_output_params(&builtin, &closed_schema(), None)
                .expect("lower")
                .is_some()
        );
        let responses = model(ProviderApiKind::OpenAiResponses, "openai");
        assert_eq!(
            native_output_params(&responses, &json!({ "type": "array" }), None).expect("lower"),
            None
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_request_has_no_tools_and_states_prompted_schemas() {
        let blobs = InMemoryBlobStore::new();
        let prepared = prepare_standalone_generation(
            &blobs,
            &StandaloneGeneration {
                model: model(ProviderApiKind::OpenAiCompletions, "local-llama"),
                instructions: Some("Grade the claim.".to_owned()),
                input: "water boils at 100C".to_owned(),
                output_schema: Some(closed_schema()),
                reasoning_effort: None,
                max_output_tokens: Some(200),
            },
        )
        .await
        .expect("prepare");
        assert_eq!(
            prepared.structured_output,
            Some(StructuredOutputMode::Prompted)
        );
        let request = prepared.request;
        assert!(request.tools.is_empty());
        assert_eq!(request.params, None);
        assert_eq!(request.output_limit, Some(200));
        assert_eq!(request.context.entries.len(), 2);
        let instructions = read_text(&blobs, &request.context.entries[0].content.content_ref)
            .await
            .expect("instructions");
        assert!(instructions.starts_with("Grade the claim."));
        assert!(instructions.contains("\"verdict\""));
        let input = read_text(&blobs, &request.context.entries[1].content.content_ref)
            .await
            .expect("input");
        assert_eq!(input, "water boils at 100C");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_request_without_instructions_has_only_the_input() {
        let blobs = InMemoryBlobStore::new();
        let prepared = prepare_standalone_generation(
            &blobs,
            &StandaloneGeneration {
                model: model(ProviderApiKind::OpenAiResponses, "openai"),
                instructions: None,
                input: "hello".to_owned(),
                output_schema: None,
                reasoning_effort: Some("low".to_owned()),
                max_output_tokens: None,
            },
        )
        .await
        .expect("prepare");
        assert_eq!(prepared.structured_output, None);
        assert_eq!(prepared.request.context.entries.len(), 1);
        assert_eq!(prepared.request.reasoning_effort.as_deref(), Some("low"));
    }

    #[test]
    fn json_replies_parse_bare_or_fenced() {
        assert_eq!(parse_json_reply(" {\"a\":1} "), Some(json!({ "a": 1 })));
        assert_eq!(
            parse_json_reply("```json\n{\"a\":1}\n```"),
            Some(json!({ "a": 1 }))
        );
        assert_eq!(parse_json_reply("The answer is {\"a\":1}"), None);
    }
}
