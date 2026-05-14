use acp_protocol::{ModelRecord, PipelineRecord, StepMetricsRecord};

#[derive(Default)]
pub struct DashboardState {
    pub pipelines: Vec<PipelineRecord>,
    pub models: Vec<ModelRecord>,
    pub events: Vec<String>,
    pub metrics: Vec<StepMetricsRecord>,
    pub quit: bool,
}
