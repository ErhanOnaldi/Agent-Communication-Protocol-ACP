use acp_protocol::ModelRecord;

use crate::{
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
