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
/// only `input.request` and `input.open` have contracts yet (jobs and
/// sub-agents do not).
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

/// A contract available in a session: its digest and the session tool
/// serving it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundContract {
    pub name: &'static str,
    pub digest: String,
    pub tool_id: ToolName,
    pub tool_name: ToolName,
}

impl BoundContract {
    pub fn pin(&self) -> ContractPin {
        ContractPin {
            contract: self.name.to_owned(),
            digest: self.digest.clone(),
        }
    }

    /// The contract's scope binding.
    pub fn binding(&self) -> ContractBinding {
        ContractBinding {
            contract: self.name.to_owned(),
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
                name: definition.name,
                digest: digest(
                    definition.name,
                    &callable.definition.input_schema,
                    callable.definition.output_schema.as_ref(),
                ),
                tool_id: callable.tool_id.clone(),
                tool_name: callable.definition.name.clone(),
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
/// answered by awaiting its promise.
const COMPANIONS: &[(&str, &str)] = &[("input.open", "promise.await")];

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
            None if definition(&pin.contract).is_none() => {
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
    if definition(contract).is_none() {
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
        bound.iter().map(|contract| contract.name).collect()
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
