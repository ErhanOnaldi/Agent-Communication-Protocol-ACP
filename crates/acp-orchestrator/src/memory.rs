use acp_protocol::SkillDefinition;

use crate::StepResult;

#[derive(Debug, Clone, Default)]
pub struct HandoffContext {
    pub summary: String,
    pub key_decisions: Vec<String>,
    pub active_files: Vec<String>,
}

pub fn build_task(action: &str, context: &HandoffContext, skill: Option<&SkillDefinition>) -> String {
    let mut task = action.to_string();
    if let Some(skill) = skill {
        if !skill.system_prompt.trim().is_empty() {
            task = format!(
                "{task}\n\nRole context ({}):\n{}",
                skill.name, skill.system_prompt
            );
        }
    }
    if !context.summary.trim().is_empty() {
        task = format!(
            "{task}\n\nACP handoff context:\nSummary: {}\nKey decisions: {}\nActive files: {}",
            context.summary,
            context.key_decisions.join("; "),
            context.active_files.join(", ")
        );
    }
    task
}

pub fn context_from_failure(action: &str, result: &StepResult) -> HandoffContext {
    HandoffContext {
        summary: format!(
            "Previous runtime failed during {action} with health={}. Continue from this point.",
            result.health
        ),
        key_decisions: vec![
            "Preserve prior workflow intent and avoid restarting unrelated work.".to_string(),
        ],
        active_files: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use acp_protocol::SkillDefinition;

    use super::*;

    #[test]
    fn injects_handoff_context_into_retry_prompt() {
        let prompt = build_task(
            "backend.implement",
            &HandoffContext {
                summary: "rate limit after editing auth".to_string(),
                key_decisions: vec!["keep public API stable".to_string()],
                active_files: vec!["src/auth.rs".to_string()],
            },
            None,
        );
        assert!(prompt.contains("ACP handoff context"));
        assert!(prompt.contains("rate limit after editing auth"));
        assert!(prompt.contains("src/auth.rs"));
    }

    #[test]
    fn skill_system_prompt_injected() {
        let skill = SkillDefinition {
            name: "rust-backend".to_string(),
            description: "Rust expert".to_string(),
            system_prompt: "You write idiomatic Rust.".to_string(),
            capabilities: vec!["rust".to_string()],
        };
        let prompt = build_task("backend.implement", &HandoffContext::default(), Some(&skill));
        assert!(prompt.contains("You write idiomatic Rust."));
        assert!(prompt.contains("Role context"));
    }
}
