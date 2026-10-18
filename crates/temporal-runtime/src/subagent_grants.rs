//! The grants a child session's profile may ask for: no more than its parent
//! holds. A profile can legitimately hold grants a given parent lacks, so the
//! check runs when a child is prepared, against that parent's own config.
//! It applies to program children; sub-agents keep running their profile's
//! own config until that change ships with its own migration.

use api::{
    EnvironmentAccess, EnvironmentsFeature, FeaturesConfig, McpFeature, SubagentsFeature,
    VfsFeature, WebFeature, WorkspaceAccess,
};

/// The first grant in `child` that `parent` does not hold, described for the
/// caller, or `None` when the parent holds every one. A feature counts by its
/// presence; within one, attached resources (workspaces, environments, MCP
/// servers and their tools, listed children) must each be attached to the
/// parent too, with at least the access the child asks for, and an allow-list
/// the parent narrows must stay inside the parent's.
pub(crate) fn excess_grant(
    child: Option<&FeaturesConfig>,
    parent: Option<&FeaturesConfig>,
) -> Option<String> {
    let child = child?;
    let empty = FeaturesConfig::default();
    let parent = parent.unwrap_or(&empty);
    let missing = |feature: &str| Some(format!("the {feature} feature"));
    if let Some(vfs) = &child.vfs {
        let Some(held) = &parent.vfs else {
            return missing("vfs");
        };
        if let Some(excess) = vfs_excess(vfs, held) {
            return Some(excess);
        }
    }
    if let Some(web) = &child.web {
        let Some(held) = &parent.web else {
            return missing("web");
        };
        if let Some(excess) = web_excess(web, held) {
            return Some(excess);
        }
    }
    if let Some(subagents) = &child.subagents {
        let Some(held) = &parent.subagents else {
            return missing("subagents");
        };
        if let Some(excess) = subagents_excess(subagents, held) {
            return Some(excess);
        }
    }
    if let Some(code_mode) = &child.code_mode {
        let Some(held) = &parent.code_mode else {
            return missing("codeMode");
        };
        if let Some(excess) = narrowed_list_excess(
            "code mode tool",
            code_mode.allowed_tools.as_deref(),
            held.allowed_tools.as_deref(),
        ) {
            return Some(excess);
        }
    }
    if child.timers.is_some() && parent.timers.is_none() {
        return missing("timers");
    }
    if let Some(environments) = &child.environments {
        let Some(held) = &parent.environments else {
            return missing("environments");
        };
        if let Some(excess) = environments_excess(environments, held) {
            return Some(excess);
        }
    }
    if let Some(mcp) = &child.mcp {
        let Some(held) = &parent.mcp else {
            return missing("mcp");
        };
        if let Some(excess) = mcp_excess(mcp, held) {
            return Some(excess);
        }
    }
    None
}

fn vfs_excess(child: &VfsFeature, parent: &VfsFeature) -> Option<String> {
    let rank = |access: &WorkspaceAccess| match access {
        WorkspaceAccess::Read => 0,
        WorkspaceAccess::Edit => 1,
    };
    child.workspaces.iter().find_map(|wanted| {
        let held = parent.workspaces.iter().find(|held| {
            held.workspace_id == wanted.workspace_id && held.snapshot_ref == wanted.snapshot_ref
        });
        match held {
            Some(held) if rank(&held.access) >= rank(&wanted.access) => None,
            _ => Some(format!(
                "workspace {} with {:?} access",
                wanted
                    .workspace_id
                    .as_deref()
                    .or(wanted.snapshot_ref.as_deref())
                    .unwrap_or("(unnamed)"),
                wanted.access
            )),
        }
    })
}

fn web_excess(child: &WebFeature, parent: &WebFeature) -> Option<String> {
    if child.fetch.is_some() && parent.fetch.is_none() {
        return Some("web fetch".to_owned());
    }
    let search = child.search.as_ref()?;
    let Some(held) = &parent.search else {
        return Some("web search".to_owned());
    };
    if let Some(excess) = narrowed_list_excess(
        "web search domain",
        search.allowed_domains.as_deref(),
        held.allowed_domains.as_deref(),
    ) {
        return Some(excess);
    }
    held.blocked_domains
        .iter()
        .find(|domain| !search.blocked_domains.contains(domain))
        .map(|domain| format!("web search of {domain}, which its caller blocks"))
}

fn subagents_excess(child: &SubagentsFeature, parent: &SubagentsFeature) -> Option<String> {
    let agent = child
        .agents
        .iter()
        .find(|wanted| {
            !parent
                .agents
                .iter()
                .any(|held| held.profile_id == wanted.profile_id)
        })
        .map(|wanted| format!("the child agent {}", wanted.profile_id));
    agent.or_else(|| {
        child
            .programs
            .iter()
            .find(|wanted| {
                !parent
                    .programs
                    .iter()
                    .any(|held| held.profile_id == wanted.profile_id)
            })
            .map(|wanted| format!("the child program {}", wanted.profile_id))
    })
}

fn environments_excess(
    child: &EnvironmentsFeature,
    parent: &EnvironmentsFeature,
) -> Option<String> {
    let rank = |access: &EnvironmentAccess| match access {
        EnvironmentAccess::Read => 0,
        EnvironmentAccess::Edit => 1,
        EnvironmentAccess::Exec => 2,
        EnvironmentAccess::Jobs => 3,
    };
    if child.selection && !parent.selection {
        return Some("environment selection".to_owned());
    }
    // Checked after `resolve_inherited_environment`, which turns an
    // inherited attachment into the caller's active machine or drops it.
    child
        .environments
        .iter()
        .filter(|wanted| !wanted.inherit)
        .find_map(|wanted| {
            let held = parent
                .environments
                .iter()
                .find(|held| held.environment_id == wanted.environment_id);
            match held {
                Some(held) if rank(&held.access) >= rank(&wanted.access) => None,
                _ => Some(format!(
                    "environment {} with {:?} access",
                    wanted
                        .environment_id
                        .as_ref()
                        .map_or("(unnamed)", |id| id.as_str()),
                    wanted.access
                )),
            }
        })
}

fn mcp_excess(child: &McpFeature, parent: &McpFeature) -> Option<String> {
    child.servers.iter().find_map(|wanted| {
        let Some(held) = parent
            .servers
            .iter()
            .find(|held| held.server_id == wanted.server_id)
        else {
            return Some(format!("the MCP server {}", wanted.server_id));
        };
        narrowed_list_excess(
            &format!("MCP server {} tool", wanted.server_id),
            wanted.tools.as_deref(),
            held.tools.as_deref(),
        )
    })
}

/// An allow-list the parent narrows (`Some`) bounds the child's: the child
/// must narrow it too, to a subset. A parent with no list allows anything.
fn narrowed_list_excess(
    noun: &str,
    child: Option<&[String]>,
    parent: Option<&[String]>,
) -> Option<String> {
    let parent = parent?;
    match child {
        None => Some(format!(
            "every {noun}; its caller allows only {}",
            parent.join(", ")
        )),
        Some(child) => child
            .iter()
            .find(|item| !parent.contains(item))
            .map(|item| format!("the {noun} {item}")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn features(value: serde_json::Value) -> FeaturesConfig {
        serde_json::from_value(value).expect("features")
    }

    #[test]
    fn a_child_may_hold_what_its_parent_holds_and_less() {
        let parent = features(json!({
            "timers": {},
            "web": { "fetch": {}, "search": { "allowedDomains": ["a.org", "b.org"] } },
            "mcp": { "servers": [{ "serverId": "github", "tools": ["list", "read"] }] },
            "subagents": {
                "agents": [{ "profileId": "reviewer" }],
                "programs": [{ "profileId": "checker" }],
            },
        }));
        for child in [
            json!({}),
            json!({ "timers": {} }),
            json!({ "web": { "search": { "allowedDomains": ["a.org"] } } }),
            json!({ "mcp": { "servers": [{ "serverId": "github", "tools": ["read"] }] } }),
            json!({ "subagents": { "programs": [{ "profileId": "checker" }] } }),
        ] {
            assert_eq!(
                excess_grant(Some(&features(child.clone())), Some(&parent)),
                None,
                "{child}"
            );
        }
        assert_eq!(excess_grant(None, None), None);
    }

    #[test]
    fn a_child_asking_for_more_than_its_parent_names_the_first_excess() {
        let parent = features(json!({
            "web": { "search": { "allowedDomains": ["a.org"] } },
            "mcp": { "servers": [{ "serverId": "github", "tools": ["read"] }] },
            "environments": {
                "environments": [{ "environmentId": "env-1", "access": "edit" }],
            },
            "subagents": { "agents": [{ "profileId": "reviewer" }] },
        }));
        for (child, expected) in [
            (json!({ "timers": {} }), "the timers feature"),
            (json!({ "web": { "fetch": {} } }), "web fetch"),
            (
                json!({ "web": { "search": {} } }),
                "every web search domain; its caller allows only a.org",
            ),
            (
                json!({ "mcp": { "servers": [{ "serverId": "slack" }] } }),
                "the MCP server slack",
            ),
            (
                json!({ "mcp": { "servers": [{ "serverId": "github", "tools": ["write"] }] } }),
                "the MCP server github tool write",
            ),
            (
                json!({ "environments": {
                    "environments": [{ "environmentId": "env-1", "access": "exec" }],
                } }),
                "environment env-1 with Exec access",
            ),
            (
                json!({ "subagents": { "programs": [{ "profileId": "checker" }] } }),
                "the child program checker",
            ),
        ] {
            assert_eq!(
                excess_grant(Some(&features(child.clone())), Some(&parent)).as_deref(),
                Some(expected),
                "{child}"
            );
        }
        assert_eq!(
            excess_grant(Some(&features(json!({ "timers": {} }))), None).as_deref(),
            Some("the timers feature")
        );
    }
}
