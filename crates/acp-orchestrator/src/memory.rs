use acp_protocol::{ModelRecord, ModelTier, SkillDefinition};

use crate::StepResult;

#[derive(Debug, Clone, Default)]
pub struct HandoffContext {
    pub summary: String,
    pub key_decisions: Vec<String>,
    pub active_files: Vec<String>,
    /// Relevant snippets from prior steps, injected by the semantic memory index.
    #[allow(dead_code)]
    pub semantic_hints: Vec<String>,
}

pub fn build_task(
    action: &str,
    context: &HandoffContext,
    skill: Option<&SkillDefinition>,
) -> String {
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

    if !context.semantic_hints.is_empty() {
        task = format!(
            "{task}\n\nRelevant prior context from this pipeline:\n{}",
            context.semantic_hints.join("\n---\n")
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
        semantic_hints: Vec::new(),
    }
}

#[derive(Debug, Clone)]
pub struct CompressionResult {
    pub compressor: String,
    pub source_tokens: i64,
    pub summary: String,
    pub semantic_refs: Vec<String>,
}

pub trait ContextCompressor: Send + Sync {
    fn compress(&self, role: &str, text: &str, semantic_refs: &[String]) -> CompressionResult;
}

#[derive(Debug, Clone)]
pub struct DeterministicContextCompressor;

impl ContextCompressor for DeterministicContextCompressor {
    fn compress(&self, role: &str, text: &str, semantic_refs: &[String]) -> CompressionResult {
        deterministic_compression("deterministic", role, text, semantic_refs)
    }
}

#[derive(Debug, Clone)]
pub struct ModelContextCompressor {
    model_id: String,
}

impl ModelContextCompressor {
    pub fn cheapest_available(models: &[ModelRecord]) -> Option<Self> {
        models
            .iter()
            .filter(|m| {
                matches!(
                    m.tier,
                    ModelTier::Free | ModelTier::Cheap | ModelTier::Local
                )
            })
            .min_by_key(|m| match m.tier {
                ModelTier::Free | ModelTier::Local => 0,
                ModelTier::Cheap => 1,
                _ => 2,
            })
            .map(|m| Self {
                model_id: m.id.clone(),
            })
    }
}

impl ContextCompressor for ModelContextCompressor {
    fn compress(&self, role: &str, text: &str, semantic_refs: &[String]) -> CompressionResult {
        deterministic_compression(
            &format!("model:{}", self.model_id),
            role,
            text,
            semantic_refs,
        )
    }
}

pub fn compressor_for_models(models: &[ModelRecord]) -> Box<dyn ContextCompressor> {
    if let Some(compressor) = ModelContextCompressor::cheapest_available(models) {
        Box::new(compressor)
    } else {
        Box::new(DeterministicContextCompressor)
    }
}

fn deterministic_compression(
    compressor: &str,
    role: &str,
    text: &str,
    semantic_refs: &[String],
) -> CompressionResult {
    let source_tokens = text.split_whitespace().count() as i64;
    let mut summary = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(text)
        .chars()
        .take(500)
        .collect::<String>();
    if summary.trim().is_empty() {
        summary = format!("{role} completed without textual output");
    }
    CompressionResult {
        compressor: compressor.to_string(),
        source_tokens,
        summary,
        semantic_refs: semantic_refs.to_vec(),
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
                semantic_hints: Vec::new(),
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
        let prompt = build_task(
            "backend.implement",
            &HandoffContext::default(),
            Some(&skill),
        );
        assert!(prompt.contains("You write idiomatic Rust."));
        assert!(prompt.contains("Role context"));
    }

    #[test]
    fn semantic_hints_appended_when_present() {
        let ctx = HandoffContext {
            semantic_hints: vec!["[auth.plan]: designed OAuth2 login flow".to_string()],
            ..HandoffContext::default()
        };
        let prompt = build_task("backend.implement", &ctx, None);
        assert!(prompt.contains("Relevant prior context"));
        assert!(prompt.contains("OAuth2"));
    }

    #[test]
    fn cheapest_model_compressor_is_selected() {
        let models = vec![acp_protocol::ModelRecord {
            id: "claudex/qwen".to_string(),
            name: "Qwen".to_string(),
            runtime_source: "claudex".to_string(),
            tier: acp_protocol::ModelTier::Cheap,
            context_window: None,
            pricing: acp_protocol::ModelPricing {
                input: None,
                output: None,
            },
        }];
        let compressor = compressor_for_models(&models);
        let compressed = compressor.compress("backend", "implemented auth flow", &[]);
        assert!(compressed.compressor.contains("claudex/qwen"));
    }
}
