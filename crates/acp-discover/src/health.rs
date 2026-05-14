use acp_protocol::{McpHealth, ModelRecord, RuntimeHealth};

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
        let status = match tokio::process::Command::new(&server.command)
            .arg("--version")
            .output()
            .await
        {
            Ok(output) if output.status.success() => RuntimeHealth::Healthy,
            Ok(_) => RuntimeHealth::Degraded,
            Err(_) => RuntimeHealth::Missing,
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
