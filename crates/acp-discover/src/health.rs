use acp_protocol::{McpHealth, ModelRecord, RuntimeHealth};
use std::time::Duration;
use tokio::time::timeout;

use crate::{
    mcp::load_mcp_config,
    provider::{load_providers, provider_statuses, ProviderStatus},
    registry::build_model_registry,
    subscription::discover_runtimes,
    DiscoveryConfig,
};

#[derive(Debug, Clone, serde::Serialize)]
pub struct DoctorReport {
    pub runtimes: Vec<acp_protocol::RuntimeRecord>,
    pub providers: Vec<ProviderStatus>,
    pub models: Vec<ModelRecord>,
    pub mcp: Vec<McpHealth>,
}

pub async fn doctor(config: &DiscoveryConfig) -> anyhow::Result<DoctorReport> {
    let runtimes = discover_runtimes().await;
    let providers = load_providers(config)?;
    let statuses = provider_statuses(&providers);
    let models = build_model_registry(&runtimes, &providers);
    let mcp_config = load_mcp_config(config)?;
    let mut mcp = Vec::new();
    for (name, server) in mcp_config.servers {
        let probe = timeout(
            Duration::from_secs(3),
            tokio::process::Command::new(&server.command)
                .arg("--version")
                .output(),
        )
        .await;
        let status = match probe {
            Ok(Ok(output)) if output.status.success() => RuntimeHealth::Healthy,
            Ok(Ok(_)) => RuntimeHealth::Degraded,
            Ok(Err(_)) => RuntimeHealth::Missing,
            Err(_) => RuntimeHealth::Degraded,
        };
        mcp.push(McpHealth {
            name,
            status,
            pid: None,
            message: Some(server.command),
            checked_at: chrono::Utc::now(),
        });
    }
    Ok(DoctorReport {
        runtimes,
        providers: statuses,
        models,
        mcp,
    })
}
