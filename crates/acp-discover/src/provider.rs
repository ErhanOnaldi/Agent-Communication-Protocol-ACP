use std::env;

use acp_protocol::ProviderConfig;
use anyhow::Context;

use crate::DiscoveryConfig;

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProviderStatus {
    pub name: String,
    pub base_url: String,
    pub api_key_env: String,
    pub configured: bool,
    pub model_count: usize,
}

pub fn load_providers(config: &DiscoveryConfig) -> anyhow::Result<Vec<ProviderConfig>> {
    let dir = config.acp_home.join("providers");
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut providers = Vec::new();
    for entry in
        std::fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|v| v.to_str()) != Some("yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read provider {}", path.display()))?;
        let provider: ProviderConfig = serde_yaml::from_str(&text)
            .with_context(|| format!("failed to parse provider {}", path.display()))?;
        providers.push(provider);
    }
    providers.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(providers)
}

pub fn provider_statuses(providers: &[ProviderConfig]) -> Vec<ProviderStatus> {
    providers
        .iter()
        .map(|p| ProviderStatus {
            name: p.name.clone(),
            base_url: p.base_url.clone(),
            api_key_env: p.api_key_env.clone(),
            configured: env::var_os(&p.api_key_env).is_some(),
            model_count: p.models.len(),
        })
        .collect()
}
