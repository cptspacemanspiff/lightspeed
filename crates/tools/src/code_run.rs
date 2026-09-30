//! Code-mode tool contract: `code_run` hands a TypeScript program to the code
//! runtime and returns its result (joined completion). The program calls
//! tools itself, concurrently when it likes, so a fixed pipeline runs without
//! a model turn per step. It is a system workflow-tool binding whose
//! start-on-call recipe runs the code-run workflow; the session runtime only
//! admits the call and pins the tools the program may reach.
//!
//! A program reaches only the session's own built-in tools that the `code`
//! grant lists and that are safe to run outside the session's turn: tools
//! whose results carry no session effects. Each one is always presented in
//! its canonical function form, whatever the session's provider, because the
//! program runs outside any provider request.

use engine::{
    BuiltinToolCallRuntime, BuiltinToolSpec, CodePolicy, CodeToolRuntime, EnvironmentId,
    EnvironmentsFeature, ModelSelection, ProviderApiKind, ToolName, WorkspaceAttachment,
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
pub const CODE_RUN_WORKFLOW_TYPE: &str = "CodeRunWorkflow";
/// The code runtime's execution workflow and its default task queue.
pub const CODE_EXECUTION_WORKFLOW_TYPE: &str = "CodeExecutionWorkflow";
pub const DEFAULT_CODE_RUNTIME_TASK_QUEUE: &str = "code-runtime";
/// A script's own deadline when the grant sets none.
pub const DEFAULT_CODE_DEADLINE_MS: u64 = 10 * 60 * 1_000;
/// Hard bound on one call, above the longest deadline a grant may set so
/// the script's own deadline always fires first.
pub const CODE_RUN_DEADLINE_AFTER_MS: u64 = engine::CODE_MAX_DEADLINE_MS + 5 * 60 * 1_000;
pub const MAX_CODE_RUN_SOURCE_BYTES: usize = 256 * 1024;
pub const MAX_CODE_RUN_INPUT_BYTES: usize = 512 * 1024;
pub const MAX_CODE_RUN_LABEL_BYTES: usize = 200;
pub const CODE_RUN_EXECUTION_CONTEXT_VERSION: u32 = 1;

/// Built-in tools a script may call. Each runs to completion inside one call
/// and returns no session effects, so running it outside the session's turn
/// cannot leave session state behind.
pub const SCRIPTABLE_TOOL_IDS: &[&str] = &[
    "web.fetch",
    "vfs.read_file",
    "vfs.grep",
    "vfs.glob",
    "vfs.list_dir",
];

pub fn is_scriptable_tool(id: &str) -> bool {
    SCRIPTABLE_TOOL_IDS.contains(&id)
}

pub fn is_code_run_binding(tool_id: &str, semantic_type: &str) -> bool {
    tool_id == CODE_RUN_WORKFLOW_TOOL_ID && semantic_type == CODE_RUN_WORKFLOW_SEMANTIC_TYPE
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

/// A tool as a script sees it, carried in the `code.run` built-in settings
/// so the advertised tool description can declare it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeToolChoice {
    /// The logical id, which is also the script's call path:
    /// `vfs.read_file` is `tools.vfs.read_file(args)`.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
}

/// A granted tool resolved for script use: what the script is told, and the
/// admitted runtime facts that execute it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptTool {
    pub name: String,
    pub tool_name: ToolName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
    pub builtin: BuiltinToolCallRuntime,
}

impl ScriptTool {
    pub fn choice(&self) -> CodeToolChoice {
        CodeToolChoice {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
        }
    }
}

/// Resolve one of the session's built-in tools for script use. The canonical
/// presentation and a Chat Completions target select the tool's plain
/// function form, which Lightspeed executes itself; the session's own
/// provider might otherwise present it as a provider-native tool.
pub fn script_tool(tool: &CodeToolRuntime, turn_model: &ModelSelection) -> ToolResult<ScriptTool> {
    let id = tool.tool_id.as_str();
    if !is_scriptable_tool(id) {
        return Err(invalid(format!("{id} cannot be called from a script")));
    }
    let mut settings = match &tool.spec.settings {
        Value::Object(settings) => settings.clone(),
        Value::Null => Map::new(),
        _ => return Err(invalid(format!("invalid settings for built-in {id}"))),
    };
    settings.insert("presentation".to_owned(), json!("canonical"));
    let spec = BuiltinToolSpec {
        settings: Value::Object(settings),
    };
    let model = ModelSelection {
        api_kind: ProviderApiKind::OpenAiCompletions,
        ..turn_model.clone()
    };
    let resolved = resolve(&tool.tool_id, &spec, &ToolTarget::from(&model))?;
    let (tool_name, definition) = resolved
        .into_iter()
        .find_map(|resolved| match (resolved.definition, resolved.binding) {
            (Definition::Function(definition), Some(_)) => Some((resolved.name, definition)),
            _ => None,
        })
        .ok_or_else(|| invalid(format!("{id} has no function form for scripts")))?;
    Ok(ScriptTool {
        name: id.to_owned(),
        tool_name,
        description: definition.description,
        input_schema: definition.input_schema,
        builtin: BuiltinToolCallRuntime { spec, model },
    })
}

/// Every granted tool a script can use, in grant order. Tools the session
/// lacks never reach the policy; tools that cannot run from a script are
/// skipped.
pub fn script_tools(policy: &CodePolicy) -> Vec<ScriptTool> {
    policy
        .tools
        .iter()
        .filter_map(|tool| script_tool(tool, &policy.turn_model).ok())
        .collect()
}

/// Limits for one script execution, resolved from the grant at admission.
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

/// Runtime-owned facts pinned when a call is admitted: the tools the script
/// may call and the batch facts needed to execute them for the calling
/// session. The model never sees or supplies any of it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeRunExecutionContextV1 {
    pub version: u32,
    pub session_id: String,
    pub run_id: u64,
    pub turn_id: u64,
    pub batch_id: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workspace_attachments: Vec<WorkspaceAttachment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vfs_working_directory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_environment_id: Option<EnvironmentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment_policy: Option<EnvironmentsFeature>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ScriptTool>,
    pub limits: CodeRunLimits,
}

impl CodeRunExecutionContextV1 {
    pub fn tool(&self, name: &str) -> Option<&ScriptTool> {
        self.tools.iter().find(|tool| tool.name == name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodeRunStatus {
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeRunError {
    /// The runtime's error code, such as `script_error` or `deadline_exceeded`.
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeRunLog {
    pub level: String,
    pub text: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeRunStats {
    #[serde(default)]
    pub generations: u32,
    #[serde(default)]
    pub calls: u32,
    #[serde(default)]
    pub rejected_calls: u32,
}

/// The `reply` payload, for both outcomes: the runtime's execution result as
/// the model reads it. A completed run carries `output`; a failed one carries
/// `error`. Both carry the script's console output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CodeRunResultEnvelope {
    pub status: CodeRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<CodeRunError>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<CodeRunLog>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub logs_truncated: bool,
    #[serde(default)]
    pub stats: CodeRunStats,
}

pub fn code_run_tool_definition(tools: &[CodeToolChoice]) -> FunctionDefinition {
    let description = format!(
        "Run a TypeScript program and return its result. Write the body of an async function: \
         `input` is in scope, `return` a JSON value, and call tools with `await tools.<path>(args)`. \
         Independent calls can run concurrently with Promise.all. A failed call throws a \
         ToolError. There is no network, file system, or timer access except through tools; \
         use `await sleep(ms)` to wait. console.log output is returned with the result. \
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

/// TypeScript declarations of everything a script can reach.
pub fn declarations(tools: &[CodeToolChoice]) -> String {
    let mut root = Namespace::default();
    for tool in tools {
        let mut node = &mut root;
        let parts: Vec<&str> = tool.name.split('.').collect();
        let (last, parents) = parts.split_last().expect("split yields one part");
        for part in parents {
            node = node.child(part);
        }
        node.members.push((last.to_string(), Member::Tool(tool.clone())));
    }
    let mut out = String::from(
        "declare const input: unknown;\n\
         declare function sleep(ms: number): Promise<void>;\n\
         declare class ToolError extends Error { tool: string; code: string; details: unknown; }\n",
    );
    out.push_str("declare const tools: ");
    render_namespace(&root, "", &mut out);
    out.push_str(";\n");
    out
}

#[derive(Default)]
struct Namespace {
    members: Vec<(String, Member)>,
}

enum Member {
    Tool(CodeToolChoice),
    Namespace(Namespace),
}

impl Namespace {
    fn child(&mut self, name: &str) -> &mut Namespace {
        let index = self
            .members
            .iter()
            .position(|(member, kind)| member == name && matches!(kind, Member::Namespace(_)));
        let index = index.unwrap_or_else(|| {
            self.members
                .push((name.to_owned(), Member::Namespace(Namespace::default())));
            self.members.len() - 1
        });
        match &mut self.members[index].1 {
            Member::Namespace(namespace) => namespace,
            Member::Tool(_) => unreachable!("position matched a namespace"),
        }
    }
}

fn render_namespace(namespace: &Namespace, indent: &str, out: &mut String) {
    let inner = format!("{indent}  ");
    out.push_str("{\n");
    for (name, member) in &namespace.members {
        match member {
            Member::Tool(tool) => {
                if let Some(description) = &tool.description {
                    push_comment(description, &inner, out);
                }
                out.push_str(&format!(
                    "{inner}{name}(args: {}): Promise<unknown>;\n",
                    type_of(&tool.input_schema, &inner, &tool.input_schema, 0)
                ));
            }
            Member::Namespace(child) => {
                out.push_str(&format!("{inner}{name}: "));
                render_namespace(child, &inner, out);
                out.push_str(";\n");
            }
        }
    }
    out.push_str(indent);
    out.push('}');
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
/// `unknown`: the declarations guide the author, and the runtime validates
/// every call against the real schema.
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
            return join_union(options.iter().map(|option| recurse(option, indent)).collect());
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
            let optional = if required.contains(&name.as_str()) { "" } else { "?" };
            lines.push_str(&format!(
                "{inner}{key}{optional}: {};\n",
                recurse(property, &inner)
            ));
        }
    }
    match object.get("additionalProperties") {
        Some(extra @ Value::Object(_)) => {
            lines.push_str(&format!("{inner}[key: string]: {};\n", recurse(extra, &inner)));
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

fn is_identifier(name: &str) -> bool {
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

    fn turn_model(api_kind: ProviderApiKind) -> ModelSelection {
        ModelSelection {
            api_kind,
            provider_id: "anthropic".to_owned(),
            model: "claude-test".to_owned(),
        }
    }

    fn runtime(id: &str) -> CodeToolRuntime {
        CodeToolRuntime {
            tool_id: ToolName::new(id),
            spec: BuiltinToolSpec {
                settings: json!({ "presentation": "claude_code_like" }),
            },
        }
    }

    #[test]
    fn scripts_get_the_function_form_whatever_the_session_provider() {
        // Anthropic sessions present web.fetch as a provider-native tool.
        let tool = script_tool(
            &runtime("web.fetch"),
            &turn_model(ProviderApiKind::AnthropicMessages),
        )
        .expect("web.fetch resolves");
        assert_eq!(tool.name, "web.fetch");
        assert_eq!(tool.tool_name.as_str(), "web_fetch");
        assert_eq!(tool.builtin.model.api_kind, ProviderApiKind::OpenAiCompletions);
        assert_eq!(tool.builtin.model.provider_id, "anthropic");
        assert!(tool.input_schema["properties"]["url"].is_object());

        let read = script_tool(
            &runtime("vfs.read_file"),
            &turn_model(ProviderApiKind::AnthropicMessages),
        )
        .expect("vfs.read_file resolves");
        assert_eq!(read.builtin.spec.settings["presentation"], "canonical");
    }

    #[test]
    fn only_effect_free_tools_are_scriptable() {
        let error = script_tool(
            &runtime("vfs.write_file"),
            &turn_model(ProviderApiKind::OpenAiResponses),
        )
        .expect_err("writes carry session effects");
        assert!(error.to_string().contains("cannot be called from a script"));

        let policy = CodePolicy {
            feature: engine::CodeFeature::default(),
            tools: vec![runtime("vfs.write_file"), runtime("web.fetch")],
            turn_model: turn_model(ProviderApiKind::OpenAiResponses),
        };
        let names: Vec<_> = script_tools(&policy)
            .into_iter()
            .map(|tool| tool.name)
            .collect();
        assert_eq!(names, ["web.fetch"]);
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
        assert!(args(&"x".repeat(MAX_CODE_RUN_SOURCE_BYTES + 1)).validate().is_err());
        assert!(
            CodeRunArgs {
                label: Some("x".repeat(MAX_CODE_RUN_LABEL_BYTES + 1)),
                ..args("return 1;")
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn declarations_render_a_typed_namespace() {
        let declared = declarations(&[
            CodeToolChoice {
                name: "web.fetch".to_owned(),
                description: Some("Fetch a page.".to_owned()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "url": { "type": "string", "description": "Absolute URL." },
                        "max_chars": { "type": ["integer", "null"] },
                        "mode": { "enum": ["text", "raw"] }
                    },
                    "required": ["url"]
                }),
            },
            CodeToolChoice {
                name: "vfs.list_dir".to_owned(),
                description: None,
                input_schema: json!({ "type": "object", "additionalProperties": false }),
            },
        ]);
        assert!(declared.contains(
            "declare const tools: {\n  web: {\n    /** Fetch a page. */\n    fetch(args: {\n      \
             max_chars?: number | null;\n      mode?: \"text\" | \"raw\";\n      \
             /** Absolute URL. */\n      url: string;\n    }): Promise<unknown>;\n  };\n  vfs: {\n    \
             list_dir(args: {}): Promise<unknown>;\n  };\n};\n"
        ));
    }

    #[test]
    fn the_definition_declares_the_granted_tools() {
        let definition = code_run_tool_definition(&[CodeToolChoice {
            name: "web.fetch".to_owned(),
            description: None,
            input_schema: json!({ "type": "object" }),
        }]);
        assert_eq!(definition.name.as_str(), CODE_RUN_TOOL_NAME);
        let description = definition.description.expect("description");
        assert!(description.contains("fetch(args: { [key: string]: unknown }): Promise<unknown>;"));
        assert_eq!(definition.input_schema["required"], json!(["source"]));
    }
}
