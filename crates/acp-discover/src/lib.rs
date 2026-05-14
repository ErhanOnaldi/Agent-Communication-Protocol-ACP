use std::{
    env,
    path::{Path, PathBuf},
};

use acp_protocol::{
    ModelPricing, ModelRecord, ModelTier, ProviderConfig, RuntimeHealth, RuntimeRecord, RuntimeType,
};
use anyhow::Context;
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub acp_home: PathBuf,
}

impl DiscoveryConfig {
    pub fn from_env() -> Self {
        let acp_home = env::var_os("ACP_HOME")
            .map(PathBuf::from)
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".acp")))
            .unwrap_or_else(|| PathBuf::from(".acp"));
        Self { acp_home }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DoctorReport {
    pub runtimes: Vec<RuntimeRecord>,
    pub providers: Vec<ProviderStatus>,
    pub models: Vec<ModelRecord>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProviderStatus {
    pub name: String,
    pub base_url: String,
    pub api_key_env: String,
    pub configured: bool,
    pub model_count: usize,
}

pub async fn discover_runtimes() -> Vec<RuntimeRecord> {
    let specs = [
        (RuntimeType::ClaudeCode, "Claude Code", "claude"),
        (RuntimeType::Codex, "Codex CLI", "codex"),
        (RuntimeType::Gemini, "Gemini CLI", "gemini"),
        (RuntimeType::Copilot, "GitHub Copilot CLI", "copilot"),
    ];
    let mut records = Vec::new();
    for (runtime_type, name, binary) in specs {
        let path = which(binary);
        let version = if path.is_some() {
            command_version(binary).await.ok()
        } else {
            None
        };
        records.push(RuntimeRecord {
            runtime_type,
            name: name.to_string(),
            binary: binary.to_string(),
            path: path.map(|path| path.display().to_string()),
            version,
            health: if which(binary).is_some() {
                RuntimeHealth::Healthy
            } else {
                RuntimeHealth::Missing
            },
            message: None,
        });
    }
    records
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
        if path.extension().and_then(|value| value.to_str()) != Some("yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read provider {}", path.display()))?;
        let provider: ProviderConfig = serde_yaml::from_str(&text)
            .with_context(|| format!("failed to parse provider {}", path.display()))?;
        providers.push(provider);
    }
    providers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(providers)
}

pub fn provider_statuses(providers: &[ProviderConfig]) -> Vec<ProviderStatus> {
    providers
        .iter()
        .map(|provider| ProviderStatus {
            name: provider.name.clone(),
            base_url: provider.base_url.clone(),
            api_key_env: provider.api_key_env.clone(),
            configured: env::var_os(&provider.api_key_env).is_some(),
            model_count: provider.models.len(),
        })
        .collect()
}

pub fn build_model_registry(
    runtimes: &[RuntimeRecord],
    providers: &[ProviderConfig],
) -> Vec<ModelRecord> {
    let mut models = Vec::new();
    for runtime in runtimes
        .iter()
        .filter(|runtime| runtime.health == RuntimeHealth::Healthy)
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
    models.sort_by(|left, right| left.id.cmp(&right.id));
    models
}

pub async fn doctor(config: &DiscoveryConfig) -> anyhow::Result<DoctorReport> {
    let runtimes = discover_runtimes().await;
    let providers = load_providers(config)?;
    let statuses = provider_statuses(&providers);
    let models = build_model_registry(&runtimes, &providers);
    Ok(DoctorReport {
        runtimes,
        providers: statuses,
        models,
    })
}

fn which(binary: &str) -> Option<PathBuf> {
    let candidate = Path::new(binary);
    if candidate.is_absolute() && candidate.exists() {
        return Some(candidate.to_path_buf());
    }
    let paths = env::var_os("PATH")?;
    env::split_paths(&paths)
        .map(|path| path.join(binary))
        .find(|path| path.exists())
}

async fn command_version(binary: &str) -> anyhow::Result<String> {
    let output = Command::new(binary).arg("--version").output().await?;
    let text = if output.stdout.is_empty() {
        String::from_utf8_lossy(&output.stderr).to_string()
    } else {
        String::from_utf8_lossy(&output.stdout).to_string()
    };
    Ok(text.lines().next().unwrap_or("").trim().to_string())
}
