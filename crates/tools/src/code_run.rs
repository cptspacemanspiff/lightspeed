//! Code-mode tool contract: `code_run` hands a TypeScript program to a code
//! sub-agent and returns its result (joined completion). The program runs in
//! a child session of the caller whose model is the code runtime: each time
//! the program waits on tool calls, the child records a turn with the
//! program's log lines since the last turn and those calls; the child's own
//! engine executes them with the session's full tool machinery, and their
//! results resume the program. The child's final turn is the program's
//! result, which becomes the call's reply.
//!
//! The child is configured like the caller, so a program can call every tool
//! the calling session has, in the form it takes under the code runtime.

use engine::{
    BuiltinToolSpec, EnvironmentId, ModelSelection, ProviderApiKind, SubagentLimits, ToolName,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::{
    definitions::{Definition, resolve},
    error::{ToolError, ToolResult},
    runtime::{FunctionDefinition, ToolTarget},
};

pub const CODE_RUN_LOGICAL_ID: &str = "code.run";
pub const CODE_RUN_TOOL_NAME: &str = "code_run";
pub const CODE_RUN_WORKFLOW_TOOL_ID: &str = "code-run";
pub const CODE_RUN_WORKFLOW_SEMANTIC_TYPE: &str = "lightspeed.code.run.v1";
/// The code runtime's execution workflow and its default task queue.
pub const CODE_EXECUTION_WORKFLOW_TYPE: &str = "CodeExecutionWorkflow";
pub const DEFAULT_CODE_RUNTIME_TASK_QUEUE: &str = "code-runtime";
/// The origin profile id recorded on code-run child sessions.
pub const CODE_RUN_AGENT: &str = "code";
/// The model route of code-run child sessions.
pub const CODE_RUNTIME_PROVIDER_ID: &str = "code";
pub const CODE_RUNTIME_MODEL: &str = "typescript";
/// A program's own deadline when the grant sets none.
pub const DEFAULT_CODE_DEADLINE_MS: u64 = 10 * 60 * 1_000;
/// Hard bound on one call, above the longest deadline a grant may set so
/// the program's own deadline always fires first.
pub const CODE_RUN_DEADLINE_AFTER_MS: u64 = engine::CODE_MAX_DEADLINE_MS + 5 * 60 * 1_000;
pub const MAX_CODE_RUN_SOURCE_BYTES: usize = 256 * 1024;
pub const MAX_CODE_RUN_INPUT_BYTES: usize = 512 * 1024;
pub const MAX_CODE_RUN_LABEL_BYTES: usize = 200;
pub const CODE_RUN_EXECUTION_CONTEXT_VERSION: u32 = 1;

pub fn is_code_run_binding(tool_id: &str, semantic_type: &str) -> bool {
    tool_id == CODE_RUN_WORKFLOW_TOOL_ID && semantic_type == CODE_RUN_WORKFLOW_SEMANTIC_TYPE
}

/// The model route of a code-run child session.
pub fn code_runtime_model() -> ModelSelection {
    ModelSelection {
        api_kind: ProviderApiKind::CodeRuntime,
        provider_id: CODE_RUNTIME_PROVIDER_ID.to_owned(),
        model: CODE_RUNTIME_MODEL.to_owned(),
    }
}

/// Model arguments of `code_run`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeRunArgs {
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl CodeRunArgs {
    pub fn validate(&self) -> ToolResult<()> {
        if self.source.trim().is_empty() {
            return Err(invalid("source must not be empty"));
        }
        if self.source.len() > MAX_CODE_RUN_SOURCE_BYTES {
            return Err(invalid(format!(
                "source must be at most {MAX_CODE_RUN_SOURCE_BYTES} bytes"
            )));
        }
        if let Some(input) = &self.input {
            let bytes = serde_json::to_vec(input).map(|bytes| bytes.len());
            if bytes.map_or(true, |bytes| bytes > MAX_CODE_RUN_INPUT_BYTES) {
                return Err(invalid(format!(
                    "input must be at most {MAX_CODE_RUN_INPUT_BYTES} bytes of JSON"
                )));
            }
        }
        if self
            .label
            .as_ref()
            .is_some_and(|label| label.len() > MAX_CODE_RUN_LABEL_BYTES)
        {
            return Err(invalid(format!(
                "label must be at most {MAX_CODE_RUN_LABEL_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

/// Limits for one program, resolved from the grant at admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeRunLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_calls: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_calls: Option<u32>,
    pub deadline_ms: u64,
}

impl From<&engine::CodeFeature> for CodeRunLimits {
    fn from(feature: &engine::CodeFeature) -> Self {
        Self {
            max_calls: feature.max_calls,
            max_concurrent_calls: feature.max_concurrent_calls,
            deadline_ms: feature.deadline_ms.unwrap_or(DEFAULT_CODE_DEADLINE_MS),
        }
    }
}

impl CodeRunLimits {
    /// A run input can come from anyone who may start runs on the child, so
    /// a program always runs within the runtime's bounds.
    pub fn bounded(self) -> Self {
        Self {
            max_calls: self
                .max_calls
                .map(|calls| calls.clamp(1, engine::CODE_MAX_CALLS)),
            max_concurrent_calls: self
                .max_concurrent_calls
                .map(|calls| calls.clamp(1, engine::CODE_MAX_CONCURRENT_CALLS)),
            deadline_ms: self.deadline_ms.clamp(1, engine::CODE_MAX_DEADLINE_MS),
        }
    }
}

/// The run input of a code-run child session: what its model, the code
/// runtime, executes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeProgram {
    pub source: String,
    #[serde(default)]
    pub input: Value,
    pub limits: CodeRunLimits,
}

impl CodeProgram {
    /// Read a child run's input. Plain text that is not a program document
    /// is taken as source with default limits, so an operator can also start
    /// a code session by hand.
    pub fn parse(text: &str) -> Self {
        serde_json::from_str::<Self>(text).unwrap_or_else(|_| Self {
            source: text.to_owned(),
            input: Value::Null,
            limits: CodeRunLimits {
                max_calls: None,
                max_concurrent_calls: None,
                deadline_ms: DEFAULT_CODE_DEADLINE_MS,
            },
        })
    }
}

/// Runtime-owned facts pinned when a call is admitted. The execution reads
/// this through the invocation's opaque execution context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeRunExecutionContextV1 {
    pub version: u32,
    pub parent_session_id: String,
    pub parent_run_id: u64,
    /// Depth, descendant, and concurrency limits the child counts against,
    /// with the program's deadline as its run deadline.
    pub grant_limits: SubagentLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_active_environment_id: Option<EnvironmentId>,
    pub limits: CodeRunLimits,
}

/// A tool as a program sees it, carried in the `code.run` built-in settings
/// so the advertised tool description can declare it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeToolChoice {
    /// The exposed tool name: `read_file` is `tools.read_file(args)`.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
}

/// Tools that make no sense to call from a program: `code_run` itself, and
/// promise controls, which programs replace with ordinary promises.
pub fn is_program_tool(logical_id: &str) -> bool {
    logical_id != CODE_RUN_LOGICAL_ID && !logical_id.starts_with("concurrency.")
}

/// The function forms of a built-in tool under the code runtime, which is
/// what a code-run child advertises and executes.
pub fn builtin_choices(id: &ToolName, spec: &BuiltinToolSpec) -> Vec<CodeToolChoice> {
    if !is_program_tool(id.as_str()) {
        return Vec::new();
    }
    let target = ToolTarget::from(&code_runtime_model());
    let Ok(resolved) = resolve(id, spec, &target) else {
        return Vec::new();
    };
    resolved
        .into_iter()
        .filter_map(|resolved| match (resolved.definition, resolved.binding) {
            (Definition::Function(definition), Some(_)) => Some(CodeToolChoice {
                name: resolved.name.as_str().to_owned(),
                description: definition.description,
                input_schema: definition.input_schema,
            }),
            _ => None,
        })
        .collect()
}

pub fn code_run_tool_definition(tools: &[CodeToolChoice]) -> FunctionDefinition {
    let description = format!(
        "Run a TypeScript program as a code sub-agent and return its result. Write the body of \
         an async function: `input` is in scope, `return` a JSON value, and call tools with \
         `await tools.<name>(args)`. Independent calls can run concurrently with Promise.all. \
         A failed call throws a ToolError. Tool results arrive as this session would see them: \
         parsed JSON when the result is JSON, otherwise text. There is no network, file \
         system, or timer access except through tools; use `await sleep(ms)` to wait. \
         console.log output is reported in the sub-agent's session as the program runs. \
         Prefer this over many separate tool calls for fixed, multi-step work.\n\n\
         Available in the program:\n```ts\n{}```",
        declarations(tools)
    );
    FunctionDefinition::new(
        CODE_RUN_TOOL_NAME,
        description,
        json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Body of an async TypeScript function."
                },
                "input": {
                    "description": "JSON value available to the program as `input`."
                },
                "label": {
                    "type": "string",
                    "description": "Short description of what the program does."
                }
            },
            "required": ["source"],
            "additionalProperties": false
        }),
    )
}

/// TypeScript declarations of everything a program can reach.
pub fn declarations(tools: &[CodeToolChoice]) -> String {
    let mut out = String::from(
        "declare const input: unknown;\n\
         declare function sleep(ms: number): Promise<void>;\n\
         declare class ToolError extends Error { tool: string; code: string; details: unknown; }\n\
         declare const tools: {\n",
    );
    for tool in tools {
        if let Some(description) = &tool.description {
            push_comment(description, "  ", &mut out);
        }
        let name = if is_identifier(&tool.name) {
            tool.name.clone()
        } else {
            Value::String(tool.name.clone()).to_string()
        };
        out.push_str(&format!(
            "  {name}(args: {}): Promise<unknown>;\n",
            type_of(&tool.input_schema, "  ", &tool.input_schema, 0)
        ));
    }
    out.push_str("};\n");
    out
}

fn push_comment(text: &str, indent: &str, out: &mut String) {
    let text = text.replace("*/", "*\\/");
    let mut lines = text.lines();
    match (lines.next(), lines.next()) {
        (Some(line), None) => out.push_str(&format!("{indent}/** {line} */\n")),
        _ => {
            out.push_str(&format!("{indent}/**\n"));
            for line in text.lines() {
                out.push_str(&format!("{indent} * {line}\n"));
            }
            out.push_str(&format!("{indent} */\n"));
        }
    }
}

/// A TypeScript type for a JSON Schema. Unfamiliar constructs widen to
/// `unknown`: the declarations guide the author, and the child session
/// validates every call against the real schema.
fn type_of(schema: &Value, indent: &str, root: &Value, depth: usize) -> String {
    let Value::Object(object) = schema else {
        return match schema {
            Value::Bool(false) => "never".to_owned(),
            _ => "unknown".to_owned(),
        };
    };
    let recurse = |child: &Value, child_indent: &str| type_of(child, child_indent, root, depth + 1);
    if let Some(Value::String(reference)) = object.get("$ref") {
        return match (depth < 8)
            .then(|| resolve_pointer(root, reference))
            .flatten()
        {
            Some(target) => recurse(target, indent),
            None => "unknown".to_owned(),
        };
    }
    if let Some(value) = object.get("const") {
        return value.to_string();
    }
    if let Some(Value::Array(values)) = object.get("enum") {
        return join_union(values.iter().map(Value::to_string).collect());
    }
    for keyword in ["anyOf", "oneOf"] {
        if let Some(Value::Array(options)) = object.get(keyword) {
            return join_union(
                options
                    .iter()
                    .map(|option| recurse(option, indent))
                    .collect(),
            );
        }
    }
    if let Some(Value::Array(types)) = object.get("type") {
        return join_union(
            types
                .iter()
                .map(|kind| {
                    let mut single = object.clone();
                    single.insert("type".to_owned(), kind.clone());
                    recurse(&Value::Object(single), indent)
                })
                .collect(),
        );
    }
    match object.get("type").and_then(Value::as_str) {
        Some("string") => "string".to_owned(),
        Some("number" | "integer") => "number".to_owned(),
        Some("boolean") => "boolean".to_owned(),
        Some("null") => "null".to_owned(),
        Some("array") => {
            let item = object
                .get("items")
                .map_or_else(|| "unknown".to_owned(), |items| recurse(items, indent));
            if item.chars().all(|c| c.is_alphanumeric() || c == '_') {
                format!("{item}[]")
            } else {
                format!("Array<{item}>")
            }
        }
        Some("object") => object_type(object, indent, &recurse),
        None if object.contains_key("properties") => object_type(object, indent, &recurse),
        _ => "unknown".to_owned(),
    }
}

fn object_type(
    object: &Map<String, Value>,
    indent: &str,
    recurse: &dyn Fn(&Value, &str) -> String,
) -> String {
    let inner = format!("{indent}  ");
    let required: Vec<&str> = object
        .get("required")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut lines = String::new();
    if let Some(Value::Object(properties)) = object.get("properties") {
        for (name, property) in properties {
            if let Some(description) = property.get("description").and_then(Value::as_str) {
                push_comment(description, &inner, &mut lines);
            }
            let key = if is_identifier(name) {
                name.clone()
            } else {
                Value::String(name.clone()).to_string()
            };
            let optional = if required.contains(&name.as_str()) {
                ""
            } else {
                "?"
            };
            lines.push_str(&format!(
                "{inner}{key}{optional}: {};\n",
                recurse(property, &inner)
            ));
        }
    }
    match object.get("additionalProperties") {
        Some(extra @ Value::Object(_)) => {
            lines.push_str(&format!(
                "{inner}[key: string]: {};\n",
                recurse(extra, &inner)
            ));
        }
        Some(Value::Bool(false)) if lines.is_empty() => return "{}".to_owned(),
        _ if lines.is_empty() => return "{ [key: string]: unknown }".to_owned(),
        _ => {}
    }
    format!("{{\n{lines}{indent}}}")
}

fn join_union(types: Vec<String>) -> String {
    let mut unique: Vec<String> = Vec::new();
    for kind in types {
        if !unique.contains(&kind) {
            unique.push(kind);
        }
    }
    if unique.iter().any(|kind| kind == "unknown") {
        return "unknown".to_owned();
    }
    if unique.is_empty() {
        return "never".to_owned();
    }
    unique.join(" | ")
}

pub fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_' || first == '$')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

fn resolve_pointer<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let pointer = reference.strip_prefix('#')?;
    root.pointer(pointer)
}

fn invalid(message: impl Into<String>) -> ToolError {
    ToolError::InvalidRequest {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_take_their_code_runtime_function_form() {
        let spec = BuiltinToolSpec::default();
        // Anthropic sessions present web.fetch as a provider-native tool;
        // under the code runtime it is Lightspeed's own function.
        let fetch = builtin_choices(&ToolName::new("web.fetch"), &spec);
        assert_eq!(fetch.len(), 1);
        assert_eq!(fetch[0].name, "web_fetch");
        assert!(fetch[0].input_schema["properties"]["url"].is_object());

        let write = builtin_choices(&ToolName::new("vfs.write_file"), &spec);
        assert_eq!(write[0].name, "vfs_write_file");

        // Provider-native search has no function form, and promise controls
        // and code_run itself are not program tools.
        assert!(builtin_choices(&ToolName::new("web.search"), &spec).is_empty());
        assert!(builtin_choices(&ToolName::new("concurrency.await"), &spec).is_empty());
        assert!(builtin_choices(&ToolName::new(CODE_RUN_LOGICAL_ID), &spec).is_empty());
    }

    #[test]
    fn programs_parse_from_documents_or_plain_source() {
        let limits = CodeRunLimits {
            max_calls: Some(5),
            max_concurrent_calls: None,
            deadline_ms: 1_000,
        };
        let document = serde_json::to_string(&CodeProgram {
            source: "return input;".to_owned(),
            input: json!({ "n": 1 }),
            limits,
        })
        .expect("encode");
        assert_eq!(CodeProgram::parse(&document).limits, limits);
        let plain = CodeProgram::parse("return 1;");
        assert_eq!(plain.source, "return 1;");
        assert_eq!(plain.limits.deadline_ms, DEFAULT_CODE_DEADLINE_MS);
    }

    #[test]
    fn limits_stay_within_the_runtime_bounds() {
        let bounded = CodeRunLimits {
            max_calls: Some(u32::MAX),
            max_concurrent_calls: Some(0),
            deadline_ms: u64::MAX,
        }
        .bounded();
        assert_eq!(bounded.max_calls, Some(engine::CODE_MAX_CALLS));
        assert_eq!(bounded.max_concurrent_calls, Some(1));
        assert_eq!(bounded.deadline_ms, engine::CODE_MAX_DEADLINE_MS);
    }

    #[test]
    fn arguments_are_bounded() {
        let args = |source: &str| CodeRunArgs {
            source: source.to_owned(),
            input: None,
            label: None,
        };
        assert!(args("return 1;").validate().is_ok());
        assert!(args("  ").validate().is_err());
        assert!(
            args(&"x".repeat(MAX_CODE_RUN_SOURCE_BYTES + 1))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn declarations_render_every_tool_by_name() {
        let declared = declarations(&[
            CodeToolChoice {
                name: "web_fetch".to_owned(),
                description: Some("Fetch a page.".to_owned()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "url": { "type": "string", "description": "Absolute URL." },
                        "max_chars": { "type": ["integer", "null"] }
                    },
                    "required": ["url"]
                }),
            },
            CodeToolChoice {
                name: "list_dir".to_owned(),
                description: None,
                input_schema: json!({ "type": "object", "additionalProperties": false }),
            },
        ]);
        assert!(declared.ends_with(
            "declare const tools: {\n  /** Fetch a page. */\n  web_fetch(args: {\n    \
             max_chars?: number | null;\n    /** Absolute URL. */\n    url: string;\n  }): \
             Promise<unknown>;\n  list_dir(args: {}): Promise<unknown>;\n};\n"
        ));
    }
}
