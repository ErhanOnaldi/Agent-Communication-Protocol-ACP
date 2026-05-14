use acp_protocol::{ModelRecord, PipelineRecord};

#[derive(Default)]
pub struct DashboardState {
    pub pipelines: Vec<PipelineRecord>,
    pub models: Vec<ModelRecord>,
    pub events: Vec<String>,
    pub quit: bool,
}
