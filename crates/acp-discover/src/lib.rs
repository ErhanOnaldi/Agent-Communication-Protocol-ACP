use std::{env, path::PathBuf};

pub mod health;
pub mod mcp;
pub mod provider;
pub mod registry;
pub mod skills;
pub mod subscription;

pub use health::{doctor, DoctorReport};
pub use mcp::{load_mcp_config, McpConfig, McpLaunch, McpManager, McpServerConfig, McpServerMode};
pub use provider::{load_providers, provider_statuses, ProviderStatus};
pub use registry::build_model_registry;
pub use skills::load_skills;
pub use subscription::discover_runtimes;

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
