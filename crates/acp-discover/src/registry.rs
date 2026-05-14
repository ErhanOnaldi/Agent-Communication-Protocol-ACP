use acp_protocol::{ModelPricing, ModelRecord, ModelTier, ProviderConfig, RuntimeHealth, RuntimeRecord, RuntimeType};

pub fn build_model_registry(
    runtimes: &[RuntimeRecord],
    providers: &[ProviderConfig],
) -> Vec<ModelRecord> {
    let mut models = Vec::new();
    for runtime in runtimes
        .iter()
        .filter(|r| r.health == RuntimeHealth::Healthy)
    {
        models.push(ModelRecord {
            id: format!("{}/default", runtime.runtime_type),
            name: format!("{} default", runtime.name),
            runtime_source: runtime.runtime_type.to_string(),
            tier: match runtime.runtime_type {
                RuntimeType::ClaudeCode | RuntimeType::Codex => ModelTier::Premium,
                RuntimeType::Gemini | RuntimeType::Copilot => ModelTier::Standard,
                RuntimeType::Claudex => ModelTier::Cheap,
            },
            context_window: None,
            pricing: ModelPricing {
                input: None,
                output: None,
            },
        });
    }
    for provider in providers {
        models.extend(provider.models.clone());
    }
    models.sort_by(|a, b| a.id.cmp(&b.id));
    models
}
