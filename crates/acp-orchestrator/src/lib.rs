use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use acp_protocol::{
    AgentSpec, ModelRecord, RuntimeHealth, RuntimeType, SchedulerProfile, SlotStatus,
    WorkflowConfig, WorkflowSlot, WorkflowStep,
};
use acp_runtime::{adapter_for, RuntimeAdapter};
use acp_workspace::WorkspaceEngine;
use anyhow::{bail, Context};
use tokio::{
    sync::{mpsc::UnboundedSender, Mutex},
    task::JoinSet,
    time::{timeout, Instant},
};

#[derive(Debug, Clone)]
pub struct Scheduler {
    models: Vec<ModelRecord>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub role: String,
    pub runtime_type: RuntimeType,
    pub model_id: Option<String>,
    pub score: f64,
}

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
}

#[derive(Debug, Clone)]
pub struct SlotLifecycleEvent {
    pub role: String,
    pub status: SlotStatus,
    pub reason: String,
    pub runtime_type: Option<RuntimeType>,
    pub model_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct HandoffContext {
    pub summary: String,
    pub key_decisions: Vec<String>,
    pub active_files: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum OrchestratorEvent {
    Slot(SlotLifecycleEvent),
    Step(StepResult),
    Handoff {
        role: String,
        context: HandoffContext,
    },
}

impl Scheduler {
    pub fn new(models: Vec<ModelRecord>) -> Self {
        Self { models }
    }

    pub fn assign(
        &self,
        role: &str,
        slot: &WorkflowSlot,
        profile: SchedulerProfile,
    ) -> anyhow::Result<Assignment> {
        let mut best = None;
        for preference in &slot.preferred {
            let model = preference
                .model
                .as_ref()
                .and_then(|model_id| self.models.iter().find(|model| model.id == *model_id));
            let score = self.score(slot, preference.runtime, model, profile);
            if best
                .as_ref()
                .is_none_or(|assignment: &Assignment| score > assignment.score)
            {
                best = Some(Assignment {
                    role: role.to_string(),
                    runtime_type: preference.runtime,
                    model_id: preference.model.clone(),
                    score,
                });
            }
        }
        best.or_else(|| {
            self.models.first().map(|model| Assignment {
                role: role.to_string(),
                runtime_type: model
                    .runtime_source
                    .parse()
                    .unwrap_or(RuntimeType::ClaudeCode),
                model_id: Some(model.id.clone()),
                score: 0.5,
            })
        })
        .ok_or_else(|| anyhow::anyhow!("no runtime candidates available for role {role}"))
    }

    pub fn candidates(
        &self,
        role: &str,
        slot: &WorkflowSlot,
        profile: SchedulerProfile,
    ) -> anyhow::Result<Vec<Assignment>> {
        let mut candidates = Vec::new();
        for preference in &slot.preferred {
            let model = preference
                .model
                .as_ref()
                .and_then(|model_id| self.models.iter().find(|model| model.id == *model_id));
            candidates.push(Assignment {
                role: role.to_string(),
                runtime_type: preference.runtime,
                model_id: preference.model.clone(),
                score: self.score(slot, preference.runtime, model, profile),
            });
        }
        if candidates.is_empty() {
            candidates.push(self.assign(role, slot, profile)?);
        }
        candidates.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(candidates)
    }

    fn score(
        &self,
        slot: &WorkflowSlot,
        runtime_type: RuntimeType,
        model: Option<&ModelRecord>,
        profile: SchedulerProfile,
    ) -> f64 {
        let capability_match = if slot.required_capabilities.is_empty() {
            1.0
        } else {
            0.75
        };
        let runtime_quality = match runtime_type {
            RuntimeType::ClaudeCode | RuntimeType::Codex => 1.0,
            RuntimeType::Gemini | RuntimeType::Copilot => 0.75,
            RuntimeType::Claudex => 0.65,
        };
        let cost_efficiency = model
            .map(|model| match model.tier.to_string().as_str() {
                "free" | "local" => 1.0,
                "cheap" => 0.85,
                "standard" => 0.65,
                "premium" => 0.35,
                _ => 0.5,
            })
            .unwrap_or(0.5);
        let context_fit = model
            .and_then(|model| model.context_window)
            .map(|window| if window >= 128_000 { 1.0 } else { 0.75 })
            .unwrap_or(0.7);
        let latency = if runtime_type == RuntimeType::Claudex {
            0.8
        } else {
            0.7
        };
        let mut score = (capability_match * 0.30)
            + (runtime_quality * 0.25)
            + (cost_efficiency * 0.20)
            + (context_fit * 0.15)
            + (latency * 0.10);
        match profile {
            SchedulerProfile::BudgetFirst => score += cost_efficiency * 0.10,
            SchedulerProfile::SpeedFirst => score += latency * 0.10,
            SchedulerProfile::QualityFirst => score += runtime_quality * 0.10,
        }
        score
    }
}

pub fn parse_workflow(yaml: &str) -> anyhow::Result<WorkflowConfig> {
    serde_yaml::from_str(yaml).context("invalid workflow yaml")
}

pub async fn run_local_pipeline(
    yaml: &str,
    models: Vec<ModelRecord>,
    repo_root: PathBuf,
) -> anyhow::Result<PipelineRunReport> {
    run_local_pipeline_with_events(yaml, models, repo_root, None).await
}

pub async fn run_local_pipeline_with_events(
    yaml: &str,
    models: Vec<ModelRecord>,
    repo_root: PathBuf,
    event_sink: Option<UnboundedSender<OrchestratorEvent>>,
) -> anyhow::Result<PipelineRunReport> {
    let config = parse_workflow(yaml)?;
    let scheduler = Scheduler::new(models);
    let workspace = WorkspaceEngine::new(repo_root);
    let step_timeout = config
        .workflow
        .timeouts
        .as_ref()
        .and_then(|timeouts| timeouts.step_minutes)
        .map(|minutes| Duration::from_secs(minutes * 60))
        .unwrap_or_else(|| Duration::from_secs(30 * 60));
    let pipeline_deadline = config
        .workflow
        .timeouts
        .as_ref()
        .and_then(|timeouts| timeouts.pipeline_minutes)
        .map(|minutes| Instant::now() + Duration::from_secs(minutes * 60));
    let mut assignments = Vec::new();
    let mut slot_events = Vec::new();
    for (slot_name, slot) in &config.workflow.slots {
        let candidates = scheduler.candidates(slot_name, slot, config.workflow.profile)?;
        let assignment = candidates.first().cloned().ok_or_else(|| {
            anyhow::anyhow!("no runtime candidates available for role {slot_name}")
        })?;
        let event = SlotLifecycleEvent {
            role: slot_name.clone(),
            status: SlotStatus::Assigned,
            reason: format!(
                "assigned {} {}",
                assignment.runtime_type,
                assignment.model_id.as_deref().unwrap_or("default")
            ),
            runtime_type: Some(assignment.runtime_type),
            model_id: assignment.model_id.clone(),
        };
        emit_slot(&mut slot_events, &event_sink, event);
        assignments.extend(candidates);
    }
    let assignments = Arc::new(assignments);
    let handoff_contexts = Arc::new(Mutex::new(BTreeMap::new()));
    let slot_events = Arc::new(Mutex::new(slot_events));
    let mut step_results = Vec::new();
    for step in &config.workflow.steps {
        match step {
            WorkflowStep::Action(action) => {
                enforce_pipeline_deadline(pipeline_deadline)?;
                let result = timeout(
                    effective_step_timeout(step_timeout, pipeline_deadline),
                    run_action_with_recovery(
                        action,
                        assignments.clone(),
                        workspace.clone(),
                        handoff_contexts.clone(),
                        slot_events.clone(),
                        event_sink.clone(),
                    ),
                )
                .await
                .with_context(|| format!("workflow step timed out: {action}"))??;
                emit_step(&event_sink, result.clone());
                step_results.push(result);
            }
            WorkflowStep::Parallel { parallel } => {
                let mut join_set = JoinSet::new();
                for action in parallel {
                    enforce_pipeline_deadline(pipeline_deadline)?;
                    let action = action.clone();
                    let assignments = assignments.clone();
                    let workspace = workspace.clone();
                    let contexts = handoff_contexts.clone();
                    let slot_events = slot_events.clone();
                    let event_sink = event_sink.clone();
                    join_set.spawn(async move {
                        timeout(
                            step_timeout,
                            run_action_with_recovery(
                                &action,
                                assignments,
                                workspace,
                                contexts,
                                slot_events,
                                event_sink.clone(),
                            ),
                        )
                        .await
                        .with_context(|| format!("workflow step timed out: {action}"))?
                    });
                }
                while let Some(result) = join_set.join_next().await {
                    let result = result.context("parallel workflow task panicked")??;
                    emit_step(&event_sink, result.clone());
                    step_results.push(result);
                }
            }
        }
    }
    let assignments =
        Arc::try_unwrap(assignments).unwrap_or_else(|assignments| (*assignments).clone());
    let handoff_contexts = Arc::try_unwrap(handoff_contexts)
        .map_err(|_| anyhow::anyhow!("handoff context still shared"))?
        .into_inner();
    let slot_events = Arc::try_unwrap(slot_events)
        .map_err(|_| anyhow::anyhow!("slot events still shared"))?
        .into_inner();
    Ok(PipelineRunReport {
        workflow_id: config.workflow.id,
        assignments,
        step_results,
        slot_events,
        handoff_contexts,
    })
}

fn enforce_pipeline_deadline(deadline: Option<Instant>) -> anyhow::Result<()> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        bail!("workflow pipeline timed out");
    }
    Ok(())
}

fn effective_step_timeout(step_timeout: Duration, deadline: Option<Instant>) -> Duration {
    deadline
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
        .filter(|remaining| *remaining < step_timeout)
        .unwrap_or(step_timeout)
}

fn emit_slot(
    slot_events: &mut Vec<SlotLifecycleEvent>,
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
    event: SlotLifecycleEvent,
) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::Slot(event.clone()));
    }
    slot_events.push(event);
}

async fn push_slot(
    slot_events: &Arc<Mutex<Vec<SlotLifecycleEvent>>>,
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
    event: SlotLifecycleEvent,
) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::Slot(event.clone()));
    }
    slot_events.lock().await.push(event);
}

fn emit_step(event_sink: &Option<UnboundedSender<OrchestratorEvent>>, result: StepResult) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::Step(result));
    }
}

fn emit_handoff(
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
    role: String,
    context: HandoffContext,
) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::Handoff { role, context });
    }
}

async fn run_action_with_recovery(
    action: &str,
    assignments: Arc<Vec<Assignment>>,
    workspace: WorkspaceEngine,
    handoff_contexts: Arc<Mutex<BTreeMap<String, HandoffContext>>>,
    slot_events: Arc<Mutex<Vec<SlotLifecycleEvent>>>,
    event_sink: Option<UnboundedSender<OrchestratorEvent>>,
) -> anyhow::Result<StepResult> {
    let role = action_role(action);
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
    push_slot(
        &slot_events,
        &event_sink,
        SlotLifecycleEvent {
            role: role.clone(),
            status: SlotStatus::Working,
            reason: format!("executing {action}"),
            runtime_type: None,
            model_id: None,
        },
    )
    .await;
    let context = {
        let contexts = handoff_contexts.lock().await;
        contexts.get(&role).cloned().unwrap_or_default()
    };
    let result = run_action_with_context(action, &assignments, &workspace, context, None).await?;
    if is_recoverable(result.health) {
        push_slot(
            &slot_events,
            &event_sink,
            SlotLifecycleEvent {
                role: role.clone(),
                status: SlotStatus::Vacant,
                reason: format!("recoverable runtime failure: {}", result.health),
                runtime_type: None,
                model_id: None,
            },
        )
        .await;
        let context = context_from_failure(action, &result);
        {
            let mut contexts = handoff_contexts.lock().await;
            contexts.insert(role.clone(), context.clone());
        }
        emit_handoff(&event_sink, role.clone(), context.clone());
        let fallback = fallback_assignment(&assignments, &role, &result)?;
        push_slot(
            &slot_events,
            &event_sink,
            SlotLifecycleEvent {
                role: role.clone(),
                status: SlotStatus::Assigned,
                reason: format!(
                    "fallback assigned {} {}",
                    fallback.runtime_type,
                    fallback.model_id.as_deref().unwrap_or("default")
                ),
                runtime_type: Some(fallback.runtime_type),
                model_id: fallback.model_id.clone(),
            },
        )
        .await;
        push_slot(
            &slot_events,
            &event_sink,
            SlotLifecycleEvent {
                role: role.clone(),
                status: SlotStatus::Working,
                reason: "resuming with handoff context".to_string(),
                runtime_type: Some(fallback.runtime_type),
                model_id: fallback.model_id.clone(),
            },
        )
        .await;
        let retry = run_action_with_context(
            action,
            &assignments,
            &workspace,
            context,
            Some(fallback.clone()),
        )
        .await?;
        if is_recoverable(retry.health) {
            push_slot(
                &slot_events,
                &event_sink,
                SlotLifecycleEvent {
                    role,
                    status: SlotStatus::Vacant,
                    reason: format!("fallback failed: {}", retry.health),
                    runtime_type: None,
                    model_id: None,
                },
            )
            .await;
        } else {
            push_slot(
                &slot_events,
                &event_sink,
                SlotLifecycleEvent {
                    role,
                    status: SlotStatus::Waiting,
                    reason: "step completed after recovery".to_string(),
                    runtime_type: Some(fallback.runtime_type),
                    model_id: fallback.model_id.clone(),
                },
            )
            .await;
        }
        return Ok(retry);
    }
    push_slot(
        &slot_events,
        &event_sink,
        SlotLifecycleEvent {
            role,
            status: SlotStatus::Waiting,
            reason: format!("completed {action}"),
            runtime_type: None,
            model_id: None,
        },
    )
    .await;
    Ok(result)
}

async fn run_action_with_context(
    action: &str,
    assignments: &[Assignment],
    workspace: &WorkspaceEngine,
    context: HandoffContext,
    override_assignment: Option<Assignment>,
) -> anyhow::Result<StepResult> {
    let role = action_role(action);
    let assignment = override_assignment
        .or_else(|| {
            assignments
                .iter()
                .find(|assignment| assignment.role == role)
                .cloned()
        })
        .ok_or_else(|| anyhow::anyhow!("no assignment for workflow step {action}"))?;
    let agent_workspace = workspace.create_agent_workspace(&role, None).await?;
    let adapter = adapter_for(assignment.runtime_type);
    let task = inject_context(action, &context);
    let output = adapter
        .spawn(AgentSpec {
            agent_id: agent_workspace.agent_id,
            role: role.clone(),
            runtime_type: assignment.runtime_type,
            model: assignment.model_id.clone(),
            task,
            workspace: Some(agent_workspace.path.display().to_string()),
            allowed_tools: Vec::new(),
            env: Default::default(),
        })
        .await?;
    Ok(StepResult {
        step: action.to_string(),
        role,
        runtime_type: assignment.runtime_type,
        model_id: assignment.model_id.clone(),
        health: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

fn action_role(action: &str) -> String {
    action
        .split_once('.')
        .map(|(role, _)| role)
        .unwrap_or(action)
        .to_string()
}

fn is_recoverable(health: RuntimeHealth) -> bool {
    matches!(
        health,
        RuntimeHealth::RateLimited | RuntimeHealth::AuthExpired | RuntimeHealth::Crashed
    )
}

fn context_from_failure(action: &str, result: &StepResult) -> HandoffContext {
    HandoffContext {
        summary: format!(
            "Previous runtime failed during {action} with health={}. Continue from this point.",
            result.health
        ),
        key_decisions: vec![
            "Preserve prior workflow intent and avoid restarting unrelated work.".to_string(),
        ],
        active_files: Vec::new(),
    }
}

fn fallback_assignment(
    assignments: &[Assignment],
    role: &str,
    failed: &StepResult,
) -> anyhow::Result<Assignment> {
    let current = assignments
        .iter()
        .find(|assignment| assignment.role == role);
    let fallback = assignments
        .iter()
        .find(|assignment| {
            assignment.role == role
                && current.is_none_or(|current| {
                    assignment.runtime_type != current.runtime_type
                        || assignment.model_id != current.model_id
                })
        })
        .cloned()
        .or_else(|| current.cloned());
    fallback.ok_or_else(|| {
        anyhow::anyhow!(
            "no fallback assignment available for role {role} after {}",
            failed.health
        )
    })
}

fn inject_context(action: &str, context: &HandoffContext) -> String {
    if context.summary.trim().is_empty() {
        return action.to_string();
    }
    format!(
        "{action}\n\nACP handoff context:\nSummary: {}\nKey decisions: {}\nActive files: {}",
        context.summary,
        context.key_decisions.join("; "),
        context.active_files.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_step_timeout_respects_pipeline_deadline() {
        let step_timeout = Duration::from_secs(60);
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(effective_step_timeout(step_timeout, Some(deadline)) <= Duration::from_secs(5));
    }

    #[test]
    fn parses_timeout_fields() {
        let workflow = parse_workflow(
            r#"
workflow:
  id: quick
  name: Quick
  profile: speed-first
  slots: {}
  steps: []
  timeouts:
    step_minutes: 1
    pipeline_minutes: 2
"#,
        )
        .unwrap();
        let timeouts = workflow.workflow.timeouts.unwrap();
        assert_eq!(timeouts.step_minutes, Some(1));
        assert_eq!(timeouts.pipeline_minutes, Some(2));
    }

    #[test]
    fn injects_handoff_context_into_retry_prompt() {
        let prompt = inject_context(
            "backend.implement",
            &HandoffContext {
                summary: "rate limit after editing auth".to_string(),
                key_decisions: vec!["keep public API stable".to_string()],
                active_files: vec!["src/auth.rs".to_string()],
            },
        );
        assert!(prompt.contains("ACP handoff context"));
        assert!(prompt.contains("rate limit after editing auth"));
        assert!(prompt.contains("src/auth.rs"));
    }

    #[test]
    fn fallback_prefers_alternate_candidate_for_same_role() {
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
        };
        let fallback = fallback_assignment(&assignments, "backend", &failed).unwrap();
        assert_eq!(fallback.runtime_type, RuntimeType::Codex);
    }

    #[tokio::test]
    async fn pipeline_emits_live_events_with_fake_codex_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let bin = temp.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let codex = bin.join("codex");
        std::fs::write(
            &codex,
            "#!/bin/sh\nif [ \"$1\" = \"exec\" ]; then printf '{\"type\":\"done\"}\\n'; exit 0; fi\nprintf 'codex fake\\n'\n",
        )
        .unwrap();
        std::process::Command::new("chmod")
            .arg("+x")
            .arg(&codex)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .current_dir(temp.path())
            .args(["init"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .current_dir(temp.path())
            .args(["config", "user.email", "test@example.com"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .current_dir(temp.path())
            .args(["config", "user.name", "Test"])
            .status()
            .unwrap();
        std::fs::write(temp.path().join("README.md"), "test").unwrap();
        std::process::Command::new("git")
            .current_dir(temp.path())
            .args(["add", "."])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .current_dir(temp.path())
            .args(["commit", "-m", "init"])
            .status()
            .unwrap();

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{old_path}", bin.display()));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let yaml = r#"
workflow:
  id: integration
  name: Integration
  profile: speed-first
  slots:
    one:
      role: one
      preferred:
        - runtime: codex
          model: codex/default
    two:
      role: two
      preferred:
        - runtime: codex
          model: codex/default
  steps:
    - parallel:
        - one.implement
        - two.implement
  timeouts:
    step_minutes: 1
    pipeline_minutes: 2
"#;
        let models = vec![ModelRecord {
            id: "codex/default".to_string(),
            name: "Codex default".to_string(),
            runtime_source: "codex".to_string(),
            tier: acp_protocol::ModelTier::Premium,
            context_window: None,
            pricing: acp_protocol::ModelPricing {
                input: None,
                output: None,
            },
        }];
        let report =
            run_local_pipeline_with_events(yaml, models, temp.path().to_path_buf(), Some(tx))
                .await
                .unwrap();
        std::env::set_var("PATH", old_path);

        assert_eq!(report.step_results.len(), 2);
        let mut event_count = 0;
        while rx.try_recv().is_ok() {
            event_count += 1;
        }
        assert!(event_count >= 6);
    }
}
