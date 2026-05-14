pub mod memory;
pub mod pipeline;
pub mod recovery;
pub mod scheduler;
pub mod slots;

pub use memory::HandoffContext;
pub use pipeline::{parse_workflow, run_local_pipeline, run_local_pipeline_with_events};
pub use recovery::FailureAction;
pub use scheduler::{Assignment, Scheduler};
pub use slots::SlotLifecycleEvent;

use std::collections::BTreeMap;

use acp_protocol::{RuntimeHealth, RuntimeType};

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
}
