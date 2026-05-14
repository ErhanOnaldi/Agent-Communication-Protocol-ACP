use std::{collections::BTreeMap, sync::Arc};

use acp_protocol::{
    AgentSpec, RuntimeHealth, RuntimeType, SkillDefinition, SlotStatus, WorkflowFailurePolicy,
    WorkflowSlot,
};
use acp_runtime::{adapter_for, RuntimeAdapter};
use acp_workspace::WorkspaceEngine;
use anyhow::bail;
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tracing::instrument;

use crate::{
    memory::{build_task, context_from_failure, HandoffContext},
    slots::{emit_conflict, fallback_assignment, push_slot, SlotLifecycleEvent},
    OrchestratorEvent, StepResult, ConflictInfo,
};
use crate::scheduler::Assignment;

#[derive(Debug, Clone, Copy)]
pub enum FailureAction {
    Retry(u32),
    Skip,
    AskUser,
    Fail,
}

pub fn parse_failure_action(s: &str) -> FailureAction {
    let s = s.trim();
    if let Some(inner) = s.strip_prefix("retry(").and_then(|t| t.strip_suffix(')')) {
        FailureAction::Retry(inner.trim().parse().unwrap_or(1))
    } else {
        match s {
            "skip" => FailureAction::Skip,
            "ask_user" => FailureAction::AskUser,
            _ => FailureAction::Fail,
        }
    }
}

pub fn failure_action_for_role(
    policy: Option<&WorkflowFailurePolicy>,
    role: &str,
) -> FailureAction {
    if let Some(pol) = policy {
        if let Some(s) = pol.overrides.get(role) {
            return parse_failure_action(s);
        }
        if let Some(s) = &pol.default {
            return parse_failure_action(s);
        }
    }
    FailureAction::Retry(1)
}

pub fn action_role(action: &str) -> String {
    action
        .split_once('.')
        .map(|(role, _)| role)
        .unwrap_or(action)
        .to_string()
}

pub fn find_skill(
    skills: &[SkillDefinition],
    role: &str,
    slots: &BTreeMap<String, WorkflowSlot>,
) -> Option<SkillDefinition> {
    let slot = slots.get(role)?;
    skills
        .iter()
        .find(|s| s.name == role || s.name == slot.role)
        .or_else(|| {
            skills.iter().find(|s| {
                !s.capabilities.is_empty()
                    && s.capabilities
                        .iter()
                        .any(|c| slot.required_capabilities.contains(c))
            })
        })
        .cloned()
}

pub(crate) fn is_recoverable(health: RuntimeHealth) -> bool {
    matches!(
        health,
        RuntimeHealth::RateLimited | RuntimeHealth::AuthExpired | RuntimeHealth::Crashed
    )
}

pub struct ActionCtx {
    pub assignments: Arc<Vec<Assignment>>,
    pub workspace: WorkspaceEngine,
    pub handoff_contexts: Arc<Mutex<BTreeMap<String, HandoffContext>>>,
    pub slot_events: Arc<Mutex<Vec<SlotLifecycleEvent>>>,
    pub event_sink: Option<UnboundedSender<OrchestratorEvent>>,
    pub failure_action: FailureAction,
    pub skill: Option<SkillDefinition>,
}

#[instrument(skip(ctx), fields(action = %action))]
pub async fn run_action_with_recovery(
    action: &str,
    ctx: ActionCtx,
) -> anyhow::Result<StepResult> {
    let ActionCtx {
        assignments,
        workspace,
        handoff_contexts,
        slot_events,
        event_sink,
        failure_action,
        skill,
    } = ctx;
    let role = action_role(action);
    let max_attempts = match failure_action {
        FailureAction::Retry(n) => n + 1,
        _ => 1,
    };

    push_slot(
        &slot_events,
        &event_sink,
        SlotLifecycleEvent {
            role: role.clone(),
            status: SlotStatus::Active,
            reason: format!("starting {action}"),
            runtime_type: None,
            model_id: None,
        },
    )
    .await;

    let mut current_override: Option<Assignment> = None;
    let mut last_failed: Option<StepResult> = None;

    for attempt in 0..max_attempts {
        push_slot(
            &slot_events,
            &event_sink,
            SlotLifecycleEvent {
                role: role.clone(),
                status: SlotStatus::Working,
                reason: if attempt == 0 {
                    format!("executing {action}")
                } else {
                    format!("retry attempt {attempt} for {action}")
                },
                runtime_type: current_override.as_ref().map(|a| a.runtime_type),
                model_id: current_override.as_ref().and_then(|a| a.model_id.clone()),
            },
        )
        .await;

        let context = handoff_contexts
            .lock()
            .await
            .get(&role)
            .cloned()
            .unwrap_or_default();
        let result = run_action_with_context(
            action,
            &assignments,
            &workspace,
            context,
            current_override.clone(),
            skill.as_ref(),
            &event_sink,
        )
        .await?;

        if !is_recoverable(result.health) {
            push_slot(
                &slot_events,
                &event_sink,
                SlotLifecycleEvent {
                    role: role.clone(),
                    status: SlotStatus::Waiting,
                    reason: format!("completed {action}"),
                    runtime_type: None,
                    model_id: None,
                },
            )
            .await;
            return Ok(result);
        }

        push_slot(
            &slot_events,
            &event_sink,
            SlotLifecycleEvent {
                role: role.clone(),
                status: SlotStatus::Vacant,
                reason: format!("recoverable failure: {}", result.health),
                runtime_type: None,
                model_id: None,
            },
        )
        .await;

        let ctx = context_from_failure(action, &result);
        handoff_contexts
            .lock()
            .await
            .insert(role.clone(), ctx.clone());
        if let Some(sink) = &event_sink {
            let _ = sink.send(OrchestratorEvent::Handoff {
                role: role.clone(),
                context: ctx,
            });
        }

        if attempt + 1 < max_attempts {
            match fallback_assignment(&assignments, &role, &result) {
                Ok(fb) => {
                    push_slot(
                        &slot_events,
                        &event_sink,
                        SlotLifecycleEvent {
                            role: role.clone(),
                            status: SlotStatus::Assigned,
                            reason: format!(
                                "fallback assigned {} {}",
                                fb.runtime_type,
                                fb.model_id.as_deref().unwrap_or("default")
                            ),
                            runtime_type: Some(fb.runtime_type),
                            model_id: fb.model_id.clone(),
                        },
                    )
                    .await;
                    current_override = Some(fb);
                }
                Err(_) => {
                    last_failed = Some(result);
                    break;
                }
            }
        } else {
            last_failed = Some(result);
        }
    }

    match failure_action {
        FailureAction::Skip => {
            push_slot(
                &slot_events,
                &event_sink,
                SlotLifecycleEvent {
                    role: role.clone(),
                    status: SlotStatus::Disabled,
                    reason: format!("step {action} skipped after exhausting retries"),
                    runtime_type: None,
                    model_id: None,
                },
            )
            .await;
            Ok(last_failed.unwrap_or_else(|| StepResult {
                step: action.to_string(),
                role,
                runtime_type: RuntimeType::ClaudeCode,
                model_id: None,
                health: RuntimeHealth::Healthy,
                stdout: String::new(),
                stderr: String::new(),
                conflict: None,
            }))
        }
        FailureAction::AskUser => bail!(
            "step {action} failed after {max_attempts} attempts; manual intervention required"
        ),
        _ => bail!("step {action} failed after {max_attempts} attempts"),
    }
}

#[instrument(skip(assignments, workspace, context, override_assignment, skill, event_sink))]
pub async fn run_action_with_context(
    action: &str,
    assignments: &[Assignment],
    workspace: &WorkspaceEngine,
    context: HandoffContext,
    override_assignment: Option<Assignment>,
    skill: Option<&SkillDefinition>,
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
) -> anyhow::Result<StepResult> {
    let role = action_role(action);
    let assignment = override_assignment
        .or_else(|| assignments.iter().find(|a| a.role == role).cloned())
        .ok_or_else(|| anyhow::anyhow!("no assignment for workflow step {action}"))?;
    let agent_workspace = workspace.create_agent_workspace(&role, None).await?;
    let adapter = adapter_for(assignment.runtime_type);
    let task = build_task(action, &context, skill);
    let output = adapter
        .spawn(AgentSpec {
            agent_id: agent_workspace.agent_id.clone(),
            role: role.clone(),
            runtime_type: assignment.runtime_type,
            model: assignment.model_id.clone(),
            task,
            workspace: Some(agent_workspace.path.display().to_string()),
            allowed_tools: Vec::new(),
            env: Default::default(),
        })
        .await?;

    let conflict = match workspace.simulate_merge(&agent_workspace.branch).await {
        Ok(sim) if !sim.clean => {
            let details = format!("{}\n{}", sim.stdout, sim.stderr);
            emit_conflict(
                event_sink,
                role.clone(),
                agent_workspace.branch.clone(),
                details.clone(),
            );
            Some(ConflictInfo {
                branch: agent_workspace.branch,
                details,
            })
        }
        _ => None,
    };

    Ok(StepResult {
        step: action.to_string(),
        role,
        runtime_type: assignment.runtime_type,
        model_id: assignment.model_id.clone(),
        health: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
        conflict,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_action_parsed_correctly() {
        assert!(matches!(
            parse_failure_action("retry(3)"),
            FailureAction::Retry(3)
        ));
        assert!(matches!(
            parse_failure_action("retry(1)"),
            FailureAction::Retry(1)
        ));
        assert!(matches!(parse_failure_action("skip"), FailureAction::Skip));
        assert!(matches!(
            parse_failure_action("ask_user"),
            FailureAction::AskUser
        ));
        assert!(matches!(parse_failure_action("fail"), FailureAction::Fail));
    }
}
