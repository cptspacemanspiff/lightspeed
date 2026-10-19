//! Canonical tool contracts: the names and shapes programs call tools by.
//!
//! A model sees a tool through its provider's presentation (`Bash`, `Read`,
//! `exec_command`), which changes with the model. A program instead calls a
//! contract: a stable dotted name such as `vfs.read_file`, bound to the
//! tool's canonical presentation. A contract's digest covers its argument
//! and result schemas, so a program pinned to a digest notices when the
//! contract it was written against has changed instead of calling a tool of
//! a different shape.

use std::collections::BTreeMap;

use harness::{ContractBinding, ContractPin, ToolName};
use serde_json::json;
use thiserror::Error;

use crate::callable::CallableTool;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContractDefinition {
    pub name: &'static str,
    pub tool_id: &'static str,
}

macro_rules! contracts {
    ($($name:literal => $tool_id:expr),* $(,)?) => {
        &[$(ContractDefinition { name: $name, tool_id: $tool_id }),*]
    };
}

/// Every contract, one tool each. Built-in tools keep their logical ids; the
/// promise controls take the `promise` family. Of the workflow-backed tools
/// the input and child-session tools have contracts yet (jobs do not).
const CONTRACTS: &[ContractDefinition] = contracts! {
    "blob.info" => "blob.info",
    "blob.read" => "blob.read",
    "blob.put" => "blob.put",
    "vfs.read_file" => "vfs.read_file",
    "vfs.write_file" => "vfs.write_file",
    "vfs.edit_file" => "vfs.edit_file",
    "vfs.apply_patch" => "vfs.apply_patch",
    "vfs.grep" => "vfs.grep",
    "vfs.glob" => "vfs.glob",
    "vfs.list_dir" => "vfs.list_dir",
    "vfs.reference" => "vfs.reference",
    "vfs.materialize" => "vfs.materialize",
    "vfs.capture" => "vfs.capture",
    "env.read_file" => "env.read_file",
    "env.write_file" => "env.write_file",
    "env.edit_file" => "env.edit_file",
    "env.apply_patch" => "env.apply_patch",
    "env.grep" => "env.grep",
    "env.glob" => "env.glob",
    "env.list_dir" => "env.list_dir",
    "env.reference" => "env.reference",
    "env.run_process" => "env.run_process",
    "env.continue_process" => "env.continue_process",
    "env.job_read" => "env.job_read",
    "environment.read" => "environment.read",
    "environment.list" => "environment.list",
    "environment.activate" => "environment.activate",
    "environment.deactivate" => "environment.deactivate",
    "web.fetch" => "web.fetch",
    "promise.await" => "concurrency.await",
    "promise.cancel" => "concurrency.cancel",
    "promise.detach" => "concurrency.detach",
    "time.sleep" => crate::concurrency::TIME_SLEEP_TOOL_ID,
    "input.request" => crate::human_input::INPUT_REQUEST_TOOL_ID,
    "input.open" => crate::human_input::INPUT_OPEN_TOOL_ID,
    "input.remind" => crate::human_input::INPUT_REMIND_TOOL_ID,
    "input.reassign" => crate::human_input::INPUT_REASSIGN_TOOL_ID,
    "input.handle" => crate::human_input::INPUT_HANDLE_TOOL_ID,
    "agent.run" => crate::subagents::AGENT_RUN_TOOL_ID,
    "agent.spawn" => crate::subagents::AGENT_SPAWN_TOOL_ID,
    "program.run" => crate::subagents::PROGRAM_RUN_TOOL_ID,
    "program.spawn" => crate::subagents::PROGRAM_SPAWN_TOOL_ID,
    "agent.handle" => crate::subagents::AGENT_HANDLE_TOOL_ID,
    "program.handle" => crate::subagents::PROGRAM_HANDLE_TOOL_ID,
};

/// Every contract a program can require, in name order.
pub fn definitions() -> impl Iterator<Item = &'static ContractDefinition> {
    let mut all = CONTRACTS.iter().collect::<Vec<_>>();
    all.sort_by_key(|definition| definition.name);
    all.into_iter()
}

pub fn definition(name: &str) -> Option<&'static ContractDefinition> {
    CONTRACTS.iter().find(|definition| definition.name == name)
}

/// The family of contracts bound from the session's MCP servers.
pub const MCP_FAMILY: &str = "mcp";

/// Whether a contract can exist: a built-in one, or any well-formed name in
/// the MCP family, whose members depend on the servers a session attaches.
pub fn is_known(name: &str) -> bool {
    definition(name).is_some() || is_mcp_contract(name)
}

fn is_mcp_contract(name: &str) -> bool {
    let mut segments = name.split('.');
    segments.next() == Some(MCP_FAMILY)
        && segments.count() == 2
        && harness::validate_contract_name(name).is_ok()
}

/// One tool of a natively executed MCP server, as the session's inventory
/// lists it.
#[derive(Clone, Debug, PartialEq)]
pub struct McpToolShape {
    /// The scope-callable name, `<server tool id>__<remote name>`.
    pub exposed_name: String,
    pub remote_name: String,
    pub input_schema: serde_json::Value,
    /// The schema of the result's `structuredContent`, when the server
    /// declares one.
    pub output_schema: Option<serde_json::Value>,
}

/// An MCP server's tools, served by the session tool `tool_id`.
#[derive(Clone, Debug, PartialEq)]
pub struct McpServerTools {
    pub tool_id: ToolName,
    pub server_label: String,
    pub tools: Vec<McpToolShape>,
}

/// The contracts the session's MCP tools serve: `mcp.<server>.<tool>`, the
/// server label lowercased and the tool name in snake case. A tool whose
/// name has no valid contract form, or whose contract name another tool
/// also takes, gets no contract; those are returned by name.
pub fn bind_mcp(servers: &[McpServerTools]) -> (Vec<BoundContract>, Vec<String>) {
    let mut claimed = BTreeMap::<String, Vec<(&McpServerTools, &McpToolShape)>>::new();
    let mut unbindable = Vec::new();
    for server in servers {
        for tool in &server.tools {
            match mcp_contract_name(&server.server_label, &tool.remote_name) {
                Some(name) => claimed.entry(name).or_default().push((server, tool)),
                None => unbindable.push(format!("{}/{}", server.server_label, tool.remote_name)),
            }
        }
    }
    let mut bound = Vec::new();
    for (name, claims) in claimed {
        let [(server, tool)] = claims.as_slice() else {
            unbindable.extend(
                claims
                    .iter()
                    .map(|(server, tool)| format!("{}/{}", server.server_label, tool.remote_name)),
            );
            continue;
        };
        let Ok(tool_name) = ToolName::try_new(tool.exposed_name.clone()) else {
            unbindable.push(format!("{}/{}", server.server_label, tool.remote_name));
            continue;
        };
        bound.push(BoundContract {
            digest: digest(&name, &tool.input_schema, tool.output_schema.as_ref()),
            name,
            tool_id: server.tool_id.clone(),
            tool_name,
            description: None,
            input_schema: tool.input_schema.clone(),
            output_schema: tool.output_schema.clone(),
        });
    }
    unbindable.sort();
    (bound, unbindable)
}

fn mcp_contract_name(server_label: &str, remote_name: &str) -> Option<String> {
    let name = format!(
        "{MCP_FAMILY}.{}.{}",
        contract_segment(server_label, false)?,
        contract_segment(remote_name, true)?
    );
    harness::validate_contract_name(&name).ok()?;
    Some(name)
}

/// A name as a contract segment: lowercase, with every run of other
/// characters one `_`. With `split_case`, a lowercase-to-uppercase step and
/// the end of an acronym also start a new word (`getHTTPStatus` becomes
/// `get_http_status`).
fn contract_segment(name: &str, split_case: bool) -> Option<String> {
    let chars = name.chars().collect::<Vec<_>>();
    let mut out = String::new();
    for (index, &c) in chars.iter().enumerate() {
        if c.is_ascii_alphanumeric() {
            if split_case && c.is_ascii_uppercase() && index > 0 {
                let previous = chars[index - 1];
                let next_is_lower = chars.get(index + 1).is_some_and(char::is_ascii_lowercase);
                if previous.is_ascii_lowercase()
                    || previous.is_ascii_digit()
                    || (previous.is_ascii_uppercase() && next_is_lower)
                {
                    out.push('_');
                }
            }
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    let segment = out.trim_matches('_');
    segment
        .starts_with(|c: char| c.is_ascii_lowercase())
        .then(|| segment.to_owned())
}

/// A contract available in a session: its digest, the session tool serving
/// it, and the shape a program sees (the digest covers both schemas).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundContract {
    pub name: String,
    pub digest: String,
    pub tool_id: ToolName,
    pub tool_name: ToolName,
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
    pub output_schema: Option<serde_json::Value>,
}

impl BoundContract {
    pub fn pin(&self) -> ContractPin {
        ContractPin {
            contract: self.name.clone(),
            digest: self.digest.clone(),
        }
    }

    /// The contract's scope binding.
    pub fn binding(&self) -> ContractBinding {
        ContractBinding {
            contract: self.name.clone(),
            digest: self.digest.clone(),
        }
    }
}

/// The contracts a session's callable tools serve. A contract is bound only
/// when its tool has exactly one callable; callables are expected on the
/// canonical presentation, where each built-in renders one tool.
pub fn bind(callables: &[CallableTool]) -> Vec<BoundContract> {
    let mut by_id = BTreeMap::<&str, Vec<&CallableTool>>::new();
    for callable in callables {
        by_id
            .entry(callable.tool_id.as_str())
            .or_default()
            .push(callable);
    }
    definitions()
        .filter_map(|definition| {
            let [callable] = by_id.get(definition.tool_id)?.as_slice() else {
                return None;
            };
            Some(BoundContract {
                name: definition.name.to_owned(),
                digest: digest(
                    definition.name,
                    &callable.definition.input_schema,
                    callable.definition.output_schema.as_ref(),
                ),
                tool_id: callable.tool_id.clone(),
                tool_name: callable.definition.name.clone(),
                description: callable.definition.description.clone(),
                input_schema: callable.definition.input_schema.clone(),
                output_schema: callable.definition.output_schema.clone(),
            })
        })
        .collect()
}

/// The digest of a contract's name and its argument and result schemas,
/// over a canonical JSON form (sorted keys, no whitespace) so it does not
/// depend on how a schema was built or serialized.
fn digest(name: &str, arguments: &serde_json::Value, result: Option<&serde_json::Value>) -> String {
    let shape = json!({ "contract": name, "arguments": arguments, "result": result });
    let mut canonical = String::new();
    write_canonical(&shape, &mut canonical);
    harness::contract_digest(canonical.as_bytes())
}

fn write_canonical(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            out.push('{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical(value, out);
            }
            out.push('}');
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// Why a program's contracts cannot be satisfied.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ContractError {
    #[error("unknown tool contract {contract}")]
    Unknown { contract: String },
    #[error("tool contract {contract} is not granted to this session")]
    Unavailable { contract: String },
    #[error("tool contract {contract} changed: pinned {pinned}, now {current}")]
    Changed {
        contract: String,
        pinned: String,
        current: String,
    },
    #[error("tool contract {contract} needs {needs}: require it too")]
    Incomplete {
        contract: String,
        needs: &'static str,
    },
}

impl ContractError {
    /// A stable machine-readable kind for failure reports.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Unknown { .. } => "contract_unknown",
            Self::Unavailable { .. } => "contract_unavailable",
            Self::Changed { .. } => "contract_changed",
            Self::Incomplete { .. } => "contract_incomplete",
        }
    }
}

/// Contracts that are no use without another: an open input request is
/// answered by awaiting its promise, and so is one taken back, and a spawned
/// child agent or program, whether just spawned or taken back.
const COMPANIONS: &[(&str, &str)] = &[
    ("input.open", "promise.await"),
    ("input.handle", "promise.await"),
    ("agent.spawn", "promise.await"),
    ("program.spawn", "promise.await"),
    ("agent.handle", "promise.await"),
    ("program.handle", "promise.await"),
];

/// Pin a program's required contracts against what a session offers. A
/// requirement that names a digest must match it; one without is pinned to
/// the current digest.
pub fn pin_requirements<'a>(
    available: &[BoundContract],
    requirements: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<Vec<ContractPin>, ContractError> {
    let requirements = requirements.into_iter().collect::<Vec<_>>();
    for (contract, needs) in COMPANIONS {
        let requires = |name: &str| requirements.iter().any(|(required, _)| *required == name);
        if requires(contract) && !requires(needs) {
            return Err(ContractError::Incomplete {
                contract: (*contract).to_owned(),
                needs,
            });
        }
    }
    requirements
        .into_iter()
        .map(|(contract, digest)| {
            let bound = find(available, contract)?;
            match digest {
                Some(pinned) if pinned != bound.digest => Err(ContractError::Changed {
                    contract: contract.to_owned(),
                    pinned: pinned.to_owned(),
                    current: bound.digest.clone(),
                }),
                _ => Ok(bound.pin()),
            }
        })
        .collect()
}

/// Check pinned contracts against what a scope binds now, as every attempt
/// does before it starts. `bound` maps each contract to its current digest.
pub fn check_pins(
    pins: &[ContractPin],
    bound: &BTreeMap<String, String>,
) -> Result<(), ContractError> {
    for pin in pins {
        match bound.get(&pin.contract) {
            None if !is_known(&pin.contract) => {
                return Err(ContractError::Unknown {
                    contract: pin.contract.clone(),
                });
            }
            None => {
                return Err(ContractError::Unavailable {
                    contract: pin.contract.clone(),
                });
            }
            Some(current) if *current != pin.digest => {
                return Err(ContractError::Changed {
                    contract: pin.contract.clone(),
                    pinned: pin.digest.clone(),
                    current: current.clone(),
                });
            }
            Some(_) => {}
        }
    }
    Ok(())
}

fn find<'a>(
    available: &'a [BoundContract],
    contract: &str,
) -> Result<&'a BoundContract, ContractError> {
    if !is_known(contract) {
        return Err(ContractError::Unknown {
            contract: contract.to_owned(),
        });
    }
    available
        .iter()
        .find(|bound| bound.name == contract)
        .ok_or_else(|| ContractError::Unavailable {
            contract: contract.to_owned(),
        })
}

/// Resolve a program session's tools against the program target and bind
/// the contracts they serve. Program sessions render built-ins on the
/// canonical presentation and carry no provider-hosted tools.
pub async fn bind_toolset(
    blobs: &dyn harness::storage::BlobStore,
    tools: &[harness::ToolSpec],
) -> Result<Vec<BoundContract>, crate::callable::CatalogError> {
    // Only tools a contract names are resolved, so a tool with no program
    // binding (a provider-hosted one, say) cannot fail the rest.
    let tools = tools
        .iter()
        .filter(|tool| definitions().any(|definition| definition.tool_id == tool.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let callables = crate::callable::resolve(blobs, &crate::runtime::ToolTarget::program(), &tools)
        .await?
        .into_iter()
        .filter_map(crate::callable::ResolvedTool::into_callable)
        .collect::<Vec<_>>();
    Ok(bind(&callables))
}

#[cfg(test)]
mod tests {
    use harness::storage::InMemoryBlobStore;

    use super::*;
    use crate::toolset::{
        BuiltinToolPresentation, BuiltinToolsetConfig, ToolsetConfig, register_toolset,
    };

    fn config(timers: bool) -> ToolsetConfig {
        let mut config = ToolsetConfig::empty();
        config.builtin = BuiltinToolsetConfig::workspace();
        config.builtin.presentation = BuiltinToolPresentation::Canonical;
        config.builtin.environment.run_process = true;
        config.builtin.environment.continue_process = true;
        config.web.fetch = true;
        config.web.search = Some(crate::web::search::WebSearchToolConfig::new(
            Vec::new(),
            Vec::new(),
        ));
        if timers {
            config.concurrency = crate::concurrency::ConcurrencyToolsetConfig::timer();
            config.concurrency.durable_sleep = true;
        }
        config
    }

    async fn bound(config: &ToolsetConfig) -> Vec<BoundContract> {
        let tools = register_toolset(config)
            .unwrap()
            .tools
            .into_values()
            .collect::<Vec<_>>();
        bind_toolset(&InMemoryBlobStore::new(), &tools)
            .await
            .unwrap()
    }

    fn names(bound: &[BoundContract]) -> Vec<&str> {
        bound
            .iter()
            .map(|contract| contract.name.as_str())
            .collect()
    }

    #[test]
    fn contract_names_are_unique_and_valid() {
        let mut seen = std::collections::BTreeSet::new();
        for definition in definitions() {
            harness::validate_contract_name(definition.name).unwrap();
            assert!(seen.insert(definition.name), "{}", definition.name);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canonical_host_tools_bind_contracts() {
        let bound = bound(&config(true)).await;
        let names = names(&bound);
        for expected in [
            "blob.read",
            "env.continue_process",
            "env.run_process",
            "promise.await",
            "time.sleep",
            "vfs.read_file",
            "web.fetch",
        ] {
            assert!(
                names.contains(&expected),
                "{expected} missing from {names:?}"
            );
        }
        let read = bound
            .iter()
            .find(|contract| contract.name == "vfs.read_file")
            .unwrap();
        assert_eq!(read.tool_id.as_str(), "vfs.read_file");
        assert!(read.digest.starts_with("contract-sha256:"));
        let sleep = bound
            .iter()
            .find(|contract| contract.name == "time.sleep")
            .unwrap();
        assert_eq!(
            (sleep.tool_id.as_str(), sleep.tool_name.as_str()),
            ("time.sleep", "time_sleep")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn contracts_follow_the_granted_tools_and_a_changed_shape_changes_the_digest() {
        let without_timer = bound(&config(false)).await;
        assert!(!names(&without_timer).contains(&"time.sleep"));

        // Without the handle path, run_process takes its one-shot shape.
        let full = bound(&config(true)).await;
        let mut one_shot_config = config(true);
        one_shot_config.builtin.environment.continue_process = false;
        let one_shot = bound(&one_shot_config).await;
        let digest = |bound: &[BoundContract], name: &str| {
            bound
                .iter()
                .find(|contract| contract.name == name)
                .map(|contract| contract.digest.clone())
        };
        assert_ne!(
            digest(&full, "env.run_process"),
            digest(&one_shot, "env.run_process")
        );
        assert_eq!(
            digest(&full, "vfs.read_file"),
            digest(&one_shot, "vfs.read_file")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn requirements_pin_current_digests_and_attempts_detect_changes() {
        let available = bound(&config(true)).await;
        let pins =
            pin_requirements(&available, [("vfs.read_file", None), ("time.sleep", None)]).unwrap();
        assert_eq!(pins[0].contract, "vfs.read_file");

        let stale = harness::contract_digest(b"stale");
        assert!(matches!(
            pin_requirements(&available, [("vfs.read_file", Some(stale.as_str()))]),
            Err(ContractError::Changed { .. })
        ));
        assert!(matches!(
            pin_requirements(&available, [("vfs.nope", None)]),
            Err(ContractError::Unknown { .. })
        ));
        let without_timer = bound(&config(false)).await;
        assert_eq!(
            pin_requirements(&without_timer, [("time.sleep", None)])
                .unwrap_err()
                .kind(),
            "contract_unavailable"
        );

        let mut current = available
            .iter()
            .map(|contract| (contract.name.to_owned(), contract.digest.clone()))
            .collect::<BTreeMap<_, _>>();
        check_pins(&pins, &current).unwrap();
        current.insert("vfs.read_file".into(), stale);
        assert_eq!(
            check_pins(&pins, &current).unwrap_err().kind(),
            "contract_changed"
        );
        current.remove("vfs.read_file");
        assert_eq!(
            check_pins(&pins, &current).unwrap_err().kind(),
            "contract_unavailable"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn input_handles_bind_and_opening_requires_awaiting() {
        let mut config = config(true);
        config.input_controls = true;
        let mut tools = register_toolset(&config)
            .unwrap()
            .tools
            .into_values()
            .collect::<Vec<_>>();
        tools.push(crate::human_input::input_open_tool_spec());
        let available = bind_toolset(&InMemoryBlobStore::new(), &tools)
            .await
            .unwrap();
        let names = names(&available);
        for expected in ["input.open", "input.remind", "input.reassign"] {
            assert!(names.contains(&expected), "{expected} missing");
        }
        assert_eq!(
            pin_requirements(&available, [("input.open", None)])
                .unwrap_err()
                .kind(),
            "contract_incomplete"
        );
        pin_requirements(&available, [("input.open", None), ("promise.await", None)]).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn agent_contracts_bind_with_the_subagents_grant_and_declare_their_results() {
        let mut tools = register_toolset(&config(true))
            .unwrap()
            .tools
            .into_values()
            .collect::<Vec<_>>();
        let blobs = InMemoryBlobStore::new();
        let without = bind_toolset(&blobs, &tools).await.unwrap();
        assert!(!names(&without).contains(&"agent.run"));
        let program = crate::subagents::ChildCaller::Program;
        tools.push(crate::subagents::SubagentToolKind::Run.tool_spec(program));
        tools.push(crate::subagents::SubagentToolKind::Spawn.tool_spec(program));
        let available = bind_toolset(&blobs, &tools).await.unwrap();
        for name in ["agent.run", "agent.spawn"] {
            let contract = available
                .iter()
                .find(|contract| contract.name == name)
                .unwrap_or_else(|| panic!("{name} missing"));
            assert!(contract.tool_name.as_str().starts_with("agent_"));
        }
        // Both declare what comes back, so a pinned digest covers it.
        for kind in [
            crate::subagents::SubagentToolKind::Run,
            crate::subagents::SubagentToolKind::Spawn,
        ] {
            let definition = crate::subagents::subagent_tool_definition(kind, program).unwrap();
            assert!(definition.output_schema.is_some(), "{kind:?}");
        }
        assert_eq!(
            pin_requirements(&available, [("agent.spawn", None)])
                .unwrap_err()
                .kind(),
            "contract_incomplete"
        );
        pin_requirements(&available, [("agent.spawn", None), ("promise.await", None)]).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn program_contracts_bind_with_the_program_tools_and_spawn_needs_await() {
        let mut config = config(true);
        config.child_handles = crate::subagents::SubagentToolKind::PROGRAMS.to_vec();
        let mut tools = register_toolset(&config)
            .unwrap()
            .tools
            .into_values()
            .collect::<Vec<_>>();
        let blobs = InMemoryBlobStore::new();
        for kind in crate::subagents::SubagentToolKind::PROGRAMS {
            tools.push(kind.tool_spec(crate::subagents::ChildCaller::Program));
        }
        let available = bind_toolset(&blobs, &tools).await.unwrap();
        for (name, tool) in [
            ("program.run", "program_run"),
            ("program.spawn", "program_spawn"),
            ("program.handle", "program_handle"),
        ] {
            let contract = available
                .iter()
                .find(|contract| contract.name == name)
                .unwrap_or_else(|| panic!("{name} missing"));
            assert_eq!(contract.tool_name.as_str(), tool);
        }
        assert!(!names(&available).contains(&"agent.run"));
        assert!(!names(&available).contains(&"agent.handle"));
        for handle in ["program.spawn", "program.handle"] {
            assert_eq!(
                pin_requirements(&available, [(handle, None)])
                    .unwrap_err()
                    .kind(),
                "contract_incomplete",
                "{handle}"
            );
        }
        pin_requirements(
            &available,
            [("program.spawn", None), ("promise.await", None)],
        )
        .unwrap();
    }

    fn mcp_tool(server: &str, remote_name: &str, input: serde_json::Value) -> McpToolShape {
        McpToolShape {
            exposed_name: format!("mcp_{server}__{remote_name}"),
            remote_name: remote_name.to_owned(),
            input_schema: input,
            output_schema: None,
        }
    }

    #[test]
    fn mcp_tools_bind_snake_case_contracts_and_skip_clashes() {
        let schema = json!({ "type": "object" });
        let servers = [
            McpServerTools {
                tool_id: ToolName::new("mcp_GitHub-Prod"),
                server_label: "GitHub-Prod".into(),
                tools: vec![
                    mcp_tool("GitHub-Prod", "createIssue", schema.clone()),
                    mcp_tool("GitHub-Prod", "getHTTPStatus", schema.clone()),
                    mcp_tool("GitHub-Prod", "repo.search", schema.clone()),
                    mcp_tool("GitHub-Prod", "get-file", schema.clone()),
                    mcp_tool("GitHub-Prod", "get_file", schema.clone()),
                    mcp_tool("GitHub-Prod", "9lives", schema.clone()),
                ],
            },
            McpServerTools {
                tool_id: ToolName::new("mcp_docs"),
                server_label: "docs".into(),
                tools: vec![mcp_tool("docs", "search", schema.clone())],
            },
        ];
        let (bound, unbindable) = bind_mcp(&servers);
        assert_eq!(
            names(&bound),
            [
                "mcp.docs.search",
                "mcp.github_prod.create_issue",
                "mcp.github_prod.get_http_status",
                "mcp.github_prod.repo_search",
            ]
        );
        assert_eq!(
            unbindable,
            [
                "GitHub-Prod/9lives",
                "GitHub-Prod/get-file",
                "GitHub-Prod/get_file"
            ]
        );
        let create = &bound[1];
        assert_eq!(create.tool_id.as_str(), "mcp_GitHub-Prod");
        assert_eq!(create.tool_name.as_str(), "mcp_GitHub-Prod__createIssue");

        // The digest follows the upstream schema.
        let mut changed = servers.clone();
        changed[1].tools[0].input_schema = json!({ "type": "object", "required": ["q"] });
        let (rebound, _) = bind_mcp(&changed);
        assert_ne!(bound[0].digest, rebound[0].digest);
        assert_eq!(bound[1].digest, rebound[1].digest);
    }

    #[test]
    fn mcp_requirements_pin_like_built_ins() {
        let (available, _) = bind_mcp(&[McpServerTools {
            tool_id: ToolName::new("mcp_docs"),
            server_label: "docs".into(),
            tools: vec![mcp_tool("docs", "search", json!({ "type": "object" }))],
        }]);
        let pins = pin_requirements(&available, [("mcp.docs.search", None)]).unwrap();
        assert_eq!(pins[0].digest, available[0].digest);
        // A server-level name is not a contract; a missing tool is
        // unavailable, not unknown.
        assert_eq!(
            pin_requirements(&available, [("mcp.docs", None)])
                .unwrap_err()
                .kind(),
            "contract_unknown"
        );
        assert_eq!(
            pin_requirements(&available, [("mcp.docs.fetch", None)])
                .unwrap_err()
                .kind(),
            "contract_unavailable"
        );
        assert_eq!(
            check_pins(&pins, &BTreeMap::new()).unwrap_err().kind(),
            "contract_unavailable"
        );
    }

    #[test]
    fn canonical_json_sorts_keys_at_every_depth() {
        let mut out = String::new();
        write_canonical(
            &serde_json::from_str::<serde_json::Value>(r#"{"b":[{"z":1,"a":" "}],"a":null}"#)
                .unwrap(),
            &mut out,
        );
        assert_eq!(out, "{\"a\":null,\"b\":[{\"a\":\"\u{2028}\",\"z\":1}]}");
    }
}
