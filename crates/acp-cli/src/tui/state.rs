use acp_protocol::{
    ContextCompressionRecord, McpHealth, ModelRecord, PipelineRecord, SchedulerDecision,
    SemanticMemoryRecord, StepMetricsRecord,
};

#[derive(Clone, Default)]
pub struct DashboardState {
    pub pipelines: Vec<PipelineRecord>,
    pub models: Vec<ModelRecord>,
    pub events: Vec<String>,
    pub metrics: Vec<StepMetricsRecord>,
    pub scheduler: Vec<SchedulerDecision>,
    pub compressions: Vec<ContextCompressionRecord>,
    pub semantic_memory: Vec<SemanticMemoryRecord>,
    pub mcp_health: Vec<McpHealth>,
    pub quit: bool,
}
