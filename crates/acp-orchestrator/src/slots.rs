use std::sync::Arc;

use acp_protocol::SlotStatus;
use tokio::sync::{mpsc::UnboundedSender, Mutex};

use crate::{
    scheduler::Assignment,
    OrchestratorEvent, StepResult,
};

#[derive(Debug, Clone)]
pub struct SlotLifecycleEvent {
    pub role: String,
    pub status: SlotStatus,
    pub reason: String,
    pub runtime_type: Option<acp_protocol::RuntimeType>,
    pub model_id: Option<String>,
}

pub fn emit_slot(
    slot_events: &mut Vec<SlotLifecycleEvent>,
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
    event: SlotLifecycleEvent,
) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::Slot(event.clone()));
    }
    slot_events.push(event);
}

pub async fn push_slot(
    slot_events: &Arc<Mutex<Vec<SlotLifecycleEvent>>>,
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
    event: SlotLifecycleEvent,
) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::Slot(event.clone()));
    }
    slot_events.lock().await.push(event);
}

pub fn emit_step(event_sink: &Option<UnboundedSender<OrchestratorEvent>>, result: StepResult) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::Step(result));
    }
}

pub fn emit_conflict(
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
    role: String,
    branch: String,
    details: String,
) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::MergeConflict {
            role,
            branch,
            details,
        });
    }
}

pub fn fallback_assignment(
    assignments: &[Assignment],
    role: &str,
    failed: &StepResult,
) -> anyhow::Result<Assignment> {
    let current = assignments.iter().find(|a| a.role == role);
    let fallback = assignments
        .iter()
        .find(|a| {
            a.role == role
                && current
                    .is_none_or(|c| a.runtime_type != c.runtime_type || a.model_id != c.model_id)
        })
        .cloned()
        .or_else(|| current.cloned());
    fallback.ok_or_else(|| anyhow::anyhow!("no fallback for role {role} after {}", failed.health))
}

#[cfg(test)]
mod tests {
    use acp_protocol::RuntimeHealth;

    use crate::{ConflictInfo, StepResult};

    use super::*;

    #[test]
    fn fallback_prefers_alternate_candidate_for_same_role() {
        use acp_protocol::RuntimeType;

        let assignments = vec![
            Assignment {
                role: "backend".to_string(),
                runtime_type: RuntimeType::ClaudeCode,
                model_id: Some("claude-code/default".to_string()),
                score: 0.9,
            },
            Assignment {
                role: "backend".to_string(),
                runtime_type: RuntimeType::Codex,
                model_id: Some("codex/default".to_string()),
                score: 0.8,
            },
        ];
        let failed = StepResult {
            step: "backend.implement".to_string(),
            role: "backend".to_string(),
            runtime_type: RuntimeType::ClaudeCode,
            model_id: Some("claude-code/default".to_string()),
            health: RuntimeHealth::RateLimited,
            stdout: String::new(),
            stderr: String::new(),
            conflict: None::<ConflictInfo>,
            latency_ms: 0,
        };
        let fallback = fallback_assignment(&assignments, "backend", &failed).unwrap();
        assert_eq!(fallback.runtime_type, RuntimeType::Codex);
    }
}
