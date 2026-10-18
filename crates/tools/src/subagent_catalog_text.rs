//! Catalog text rendered once by the publisher.

use crate::subagents::{
    AGENT_RUN_TOOL_NAME, AGENT_SPAWN_TOOL_NAME, PROGRAM_RUN_TOOL_NAME, PROGRAM_SPAWN_TOOL_NAME,
    SubagentCatalogSnapshot,
};

pub(crate) fn subagent_catalog_text(catalog: &SubagentCatalogSnapshot) -> String {
    let mut text = String::new();
    if catalog.agents.is_empty() && catalog.programs.is_empty() {
        text.push_str("No sub-agents are currently available.");
        return text;
    }
    if !catalog.agents.is_empty() {
        text.push_str(&format!(
            "You may delegate work to these agents with {AGENT_RUN_TOOL_NAME} (waits and returns the result inline; several calls in one turn run concurrently and return together) or {AGENT_SPAWN_TOOL_NAME} (returns a promise to await later). Pass the profile id as `agent`. A sub-agent sees only your brief plus its own instructions, so make each brief complete and self-contained.\n\n",
        ));
        let mut agents: Vec<_> = catalog.agents.iter().collect();
        agents.sort_by_key(|agent| &agent.profile_id);
        for agent in agents {
            push_entry(
                &mut text,
                &agent.profile_id,
                agent.display_name.as_deref(),
                agent.description.as_deref(),
                agent.revision.is_some(),
            );
        }
    }
    if !catalog.programs.is_empty() {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&format!(
            "You may run these programs with {PROGRAM_RUN_TOOL_NAME} (waits and returns the program's output inline; several calls in one turn run concurrently and return together) or {PROGRAM_SPAWN_TOOL_NAME} (returns a promise to await later). Pass the profile id as `program` and an `input` that matches the program's input schema; its output matches its output schema.\n\n",
        ));
        let mut programs: Vec<_> = catalog.programs.iter().collect();
        programs.sort_by_key(|program| &program.profile_id);
        for program in programs {
            push_entry(
                &mut text,
                &program.profile_id,
                program.display_name.as_deref(),
                program.description.as_deref(),
                program.revision.is_some(),
            );
            for (label, schema) in [
                ("input", &program.input_schema),
                ("output", &program.output_schema),
            ] {
                match schema {
                    Some(schema) => text.push_str(&format!("  {label} schema: {schema}\n")),
                    None => text.push_str(&format!("  {label} schema: any JSON\n")),
                }
            }
        }
    }
    text.push_str(&format!(
        "\nLimits for this session\'s tree: depth {}, {} descendants in total, {} running at once, {} s per child.",
        catalog.limits.max_depth,
        catalog.limits.max_descendants,
        catalog.limits.max_concurrent,
        catalog.limits.deadline_ms / 1_000,
    ));
    text
}

/// One menu entry: its id, a display name that adds something, its
/// description, and a warning when its profile is gone.
fn push_entry(
    text: &mut String,
    profile_id: &str,
    display_name: Option<&str>,
    description: Option<&str>,
    exists: bool,
) {
    let name = display_name
        .filter(|name| !name.trim().is_empty() && *name != profile_id)
        .map(|name| format!(" ({name})"))
        .unwrap_or_default();
    text.push_str(&format!("- {profile_id}{name}\n"));
    match description.filter(|d| !d.trim().is_empty()) {
        Some(description) => text.push_str(&format!("  {description}\n")),
        None => text.push_str("  (no description)\n"),
    }
    if !exists {
        text.push_str(
            "  (this profile is currently missing; runs will fail until it is restored)\n",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subagents::{SubagentCatalogAgent, SubagentCatalogProgram};

    #[test]
    fn catalog_text_lists_programs_with_their_schemas() {
        let catalog = SubagentCatalogSnapshot::new(
            Vec::new(),
            vec![SubagentCatalogProgram {
                profile_id: "checker".to_owned(),
                display_name: Some("Citation checker".to_owned()),
                description: Some("Checks one citation.".to_owned()),
                revision: Some(2),
                input_schema: Some(serde_json::json!({"type": "object", "required": ["claim"]})),
                output_schema: None,
            }],
            harness::SubagentLimits::default(),
        );
        let text = subagent_catalog_text(&catalog);
        assert!(!text.contains(AGENT_RUN_TOOL_NAME), "{text}");
        assert!(text.contains(PROGRAM_RUN_TOOL_NAME) && text.contains("`program`"));
        assert!(text.contains("- checker (Citation checker)\n  Checks one citation."));
        assert!(
            text.contains(r#"input schema: {"required":["claim"],"type":"object"}"#),
            "{text}"
        );
        assert!(text.contains("output schema: any JSON"));
        assert!(text.contains("depth 2, 16 descendants"));
    }

    #[test]
    fn catalog_text_lists_agents_descriptions_and_limits() {
        let catalog = SubagentCatalogSnapshot::new(
            vec![
                SubagentCatalogAgent {
                    profile_id: "reviewer".to_owned(),
                    display_name: Some("Reviewer".to_owned()),
                    description: Some("Reviews a PR for correctness.".to_owned()),
                    revision: Some(3),
                },
                SubagentCatalogAgent {
                    profile_id: "gone".to_owned(),
                    display_name: None,
                    description: None,
                    revision: None,
                },
            ],
            Vec::new(),
            harness::SubagentLimits::default(),
        );
        let text = subagent_catalog_text(&catalog);
        assert!(text.contains("- reviewer (Reviewer)"));
        assert!(text.contains("Reviews a PR for correctness."));
        assert!(text.contains("currently missing"));
        assert!(text.contains("depth 2, 16 descendants"));
        assert!(text.contains(AGENT_RUN_TOOL_NAME));
        let mut changed = catalog.clone();
        changed.agents[0].revision = Some(99);
        changed.agents.reverse();
        assert_eq!(subagent_catalog_text(&changed), text);
    }
}
