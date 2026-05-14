pub mod adaptive;
pub mod memory;
pub mod pipeline;
pub mod recovery;
pub mod scheduler;
pub mod semantic;
pub mod slots;

pub use adaptive::AdaptiveController;
pub use memory::HandoffContext;
pub use pipeline::{parse_workflow, run_local_pipeline, run_local_pipeline_with_events};
pub use recovery::FailureAction;
pub use scheduler::{Assignment, Scheduler};
pub use semantic::MemoryIndex;
pub use slots::SlotLifecycleEvent;

use std::collections::BTreeMap;

use acp_protocol::{RuntimeHealth, RuntimeType, SchedulerInsights};

#[derive(Debug, Clone)]
pub struct PipelineRunReport {
    pub workflow_id: String,
    pub assignments: Vec<Assignment>,
    pub step_results: Vec<StepResult>,
    pub slot_events: Vec<SlotLifecycleEvent>,
    pub handoff_contexts: BTreeMap<String, HandoffContext>,
}

#[derive(Debug, Clone)]
pub struct StepResult {
    pub step: String,
    pub role: String,
    pub runtime_type: RuntimeType,
    pub model_id: Option<String>,
    pub health: RuntimeHealth,
    pub stdout: String,
    pub stderr: String,
    pub conflict: Option<ConflictInfo>,
    /// Wall-clock duration of the step execution in milliseconds.
    pub latency_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ConflictInfo {
    pub branch: String,
    pub details: String,
}

#[derive(Debug, Clone)]
pub enum OrchestratorEvent {
    Slot(SlotLifecycleEvent),
    Step(StepResult),
    Handoff {
        role: String,
        context: HandoffContext,
    },
    MergeConflict {
        role: String,
        branch: String,
        details: String,
    },
    SchedulerDecision {
        role: String,
        insights: SchedulerInsights,
        reason: String,
    },
    ContextCompressed {
        role: String,
        compressor: String,
        source_tokens: i64,
        summary: String,
        semantic_refs: Vec<String>,
    },
    SemanticMemory {
        item_id: String,
        content: String,
        embedding_provider: String,
        embedding_model: String,
        embedding: Vec<f32>,
    },
}
