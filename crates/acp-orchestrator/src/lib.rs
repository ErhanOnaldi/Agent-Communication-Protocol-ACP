use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use acp_protocol::{
    AgentSpec, CapabilityScoreRecord, ModelRecord, RuntimeHealth, RuntimeType, SchedulerProfile,
    SkillDefinition, SlotStatus, WorkflowConfig, WorkflowFailurePolicy, WorkflowSlot, WorkflowStep,
};
use acp_runtime::{adapter_for, RuntimeAdapter};
use acp_workspace::WorkspaceEngine;
use anyhow::{bail, Context};
use tokio::{
    sync::{mpsc::UnboundedSender, Mutex},
    task::JoinSet,
    time::{timeout, Instant},
};
use tracing::instrument;

#[derive(Debug, Clone)]
pub struct Scheduler {
    models: Vec<ModelRecord>,
    capability_scores: Vec<CapabilityScoreRecord>,
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
    pub conflict: Option<ConflictInfo>,
}

#[derive(Debug, Clone)]
pub struct ConflictInfo {
    pub branch: String,
    pub details: String,
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
    MergeConflict {
        role: String,
        branch: String,
        details: String,
    },
}

#[derive(Debug, Clone, Copy)]
enum FailureAction {
    Retry(u32),
    Skip,
    AskUser,
    Fail,
}

fn parse_failure_action(s: &str) -> FailureAction {
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

fn failure_action_for_role(policy: Option<&WorkflowFailurePolicy>, role: &str) -> FailureAction {
    if let Some(pol) = policy {
        if let Some(override_str) = pol.overrides.get(role) {
            return parse_failure_action(override_str);
        }
        if let Some(default_str) = &pol.default {
            return parse_failure_action(default_str);
        }
    }
    FailureAction::Retry(1)
}

impl Scheduler {
    pub fn new(models: Vec<ModelRecord>) -> Self {
        Self {
            models,
            capability_scores: Vec::new(),
        }
    }

    pub fn with_scores(mut self, scores: Vec<CapabilityScoreRecord>) -> Self {
        self.capability_scores = scores;
        self
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
                .and_then(|model_id| self.models.iter().find(|m| m.id == *model_id));
            let score = self.score(slot, preference.runtime, model, profile);
            if best.as_ref().is_none_or(|a: &Assignment| score > a.score) {
                best = Some(Assignment {
                    role: role.to_string(),
                    runtime_type: preference.runtime,
                    model_id: preference.model.clone(),
                    score,
                });
            }
        }
        best.or_else(|| {
            self.models.first().map(|m| Assignment {
                role: role.to_string(),
                runtime_type: m.runtime_source.parse().unwrap_or(RuntimeType::ClaudeCode),
                model_id: Some(m.id.clone()),
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
                .and_then(|model_id| self.models.iter().find(|m| m.id == *model_id));
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
        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
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
            .map(|m| match m.tier.to_string().as_str() {
                "free" | "local" => 1.0,
                "cheap" => 0.85,
                "standard" => 0.65,
                "premium" => 0.35,
                _ => 0.5,
            })
            .unwrap_or(0.5);
        let context_fit = model
            .and_then(|m| m.context_window)
            .map(|w| if w >= 128_000 { 1.0 } else { 0.75 })
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
        // Apply learned adjustment from capability scores
        if let Some(model) = model {
            score += self.learned_boost(runtime_type, &model.id, &slot.required_capabilities);
        }
        score
    }

    fn learned_boost(
        &self,
        runtime_type: RuntimeType,
        model_id: &str,
        capabilities: &[String],
    ) -> f64 {
        if capabilities.is_empty() || self.capability_scores.is_empty() {
            return 0.0;
        }
        let mut total = 0.0;
        let mut count = 0usize;
        for cap in capabilities {
            if let Some(rec) = self.capability_scores.iter().find(|s| {
                s.runtime_type == runtime_type && s.model_id == model_id && s.capability == *cap
            }) {
                let n = rec.success_count + rec.failure_count;
                if n >= 5 {
                    let rate = rec.success_count as f64 / n as f64;
                    // Maps success_rate [0,1] -> learned adjustment [-0.10, +0.10]
                    total += (rate - 0.5) * 0.20;
                    count += 1;
                }
            }
        }
        if count > 0 {
            total / count as f64
        } else {
            0.0
        }
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
    run_local_pipeline_with_events(yaml, models, Vec::new(), Vec::new(), repo_root, None).await
}

#[instrument(skip(yaml, models, capability_scores, skills, event_sink))]
pub async fn run_local_pipeline_with_events(
    yaml: &str,
    models: Vec<ModelRecord>,
    capability_scores: Vec<CapabilityScoreRecord>,
    skills: Vec<SkillDefinition>,
    repo_root: PathBuf,
    event_sink: Option<UnboundedSender<OrchestratorEvent>>,
) -> anyhow::Result<PipelineRunReport> {
    let config = parse_workflow(yaml)?;
    let scheduler = Scheduler::new(models).with_scores(capability_scores);
    let workspace = WorkspaceEngine::new(repo_root);
    let step_timeout = config
        .workflow
        .timeouts
        .as_ref()
        .and_then(|t| t.step_minutes)
        .map(|m| Duration::from_secs(m * 60))
        .unwrap_or_else(|| Duration::from_secs(30 * 60));
    let pipeline_deadline = config
        .workflow
        .timeouts
        .as_ref()
        .and_then(|t| t.pipeline_minutes)
        .map(|m| Instant::now() + Duration::from_secs(m * 60));
    let failure_policy = config.workflow.failure.as_ref();
    let skills = Arc::new(skills);

    let mut assignments = Vec::new();
    let mut slot_events = Vec::new();
    for (slot_name, slot) in &config.workflow.slots {
        let candidates = scheduler.candidates(slot_name, slot, config.workflow.profile)?;
        let assignment = candidates
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no runtime candidates for role {slot_name}"))?;
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
    let handoff_contexts: Arc<Mutex<BTreeMap<String, HandoffContext>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let slot_events = Arc::new(Mutex::new(slot_events));
    let mut step_results = Vec::new();

    for step in &config.workflow.steps {
        match step {
            WorkflowStep::Action(action) => {
                enforce_pipeline_deadline(pipeline_deadline)?;
                let role = action_role(action);
                let fa = failure_action_for_role(failure_policy, &role);
                let skill = find_skill(&skills, &role, &config.workflow.slots);
                let result = timeout(
                    effective_step_timeout(step_timeout, pipeline_deadline),
                    run_action_with_recovery(
                        action,
                        ActionCtx {
                            assignments: assignments.clone(),
                            workspace: workspace.clone(),
                            handoff_contexts: handoff_contexts.clone(),
                            slot_events: slot_events.clone(),
                            event_sink: event_sink.clone(),
                            failure_action: fa,
                            skill,
                        },
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
                    let role = action_role(&action);
                    let fa = failure_action_for_role(failure_policy, &role);
                    let skill = find_skill(&skills, &role, &config.workflow.slots);
                    let parallel_timeout = effective_step_timeout(step_timeout, pipeline_deadline);
                    // Clone shared state before moving into the async block
                    let a = assignments.clone();
                    let w = workspace.clone();
                    let hc = handoff_contexts.clone();
                    let se = slot_events.clone();
                    let es = event_sink.clone();
                    join_set.spawn(async move {
                        timeout(
                            parallel_timeout,
                            run_action_with_recovery(
                                &action,
                                ActionCtx {
                                    assignments: a,
                                    workspace: w,
                                    handoff_contexts: hc,
                                    slot_events: se,
                                    event_sink: es,
                                    failure_action: fa,
                                    skill,
                                },
                            ),
                        )
                        .await
                        .with_context(|| format!("workflow step timed out: {action}"))?
                    });
                }
                while let Some(res) = join_set.join_next().await {
                    let result = res.context("parallel workflow task panicked")??;
                    emit_step(&event_sink, result.clone());
                    step_results.push(result);
                }
            }
        }
    }

    let assignments = Arc::try_unwrap(assignments).unwrap_or_else(|a| (*a).clone());
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

fn find_skill(
    skills: &[SkillDefinition],
    role: &str,
    slots: &std::collections::BTreeMap<String, WorkflowSlot>,
) -> Option<SkillDefinition> {
    let slot = slots.get(role)?;
    // Match by role name first, then by capability overlap
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

fn enforce_pipeline_deadline(deadline: Option<Instant>) -> anyhow::Result<()> {
    if deadline.is_some_and(|d| Instant::now() >= d) {
        bail!("workflow pipeline timed out");
    }
    Ok(())
}

fn effective_step_timeout(step_timeout: Duration, deadline: Option<Instant>) -> Duration {
    deadline
        .map(|d| d.saturating_duration_since(Instant::now()))
        .filter(|r| *r < step_timeout)
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

fn emit_conflict(
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

struct ActionCtx {
    assignments: Arc<Vec<Assignment>>,
    workspace: WorkspaceEngine,
    handoff_contexts: Arc<Mutex<BTreeMap<String, HandoffContext>>>,
    slot_events: Arc<Mutex<Vec<SlotLifecycleEvent>>>,
    event_sink: Option<UnboundedSender<OrchestratorEvent>>,
    failure_action: FailureAction,
    skill: Option<SkillDefinition>,
}

#[instrument(skip(ctx), fields(action = %action))]
async fn run_action_with_recovery(action: &str, ctx: ActionCtx) -> anyhow::Result<StepResult> {
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

        let context = {
            handoff_contexts
                .lock()
                .await
                .get(&role)
                .cloned()
                .unwrap_or_default()
        };
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

        // Recoverable failure
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
        {
            handoff_contexts
                .lock()
                .await
                .insert(role.clone(), ctx.clone());
        }
        emit_handoff(&event_sink, role.clone(), ctx);

        // Try to find a different fallback for next attempt
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

    // All attempts exhausted
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
        FailureAction::AskUser => {
            bail!(
                "step {action} failed after {max_attempts} attempts; manual intervention required"
            )
        }
        _ => bail!("step {action} failed after {max_attempts} attempts"),
    }
}

#[instrument(skip(
    assignments,
    workspace,
    context,
    override_assignment,
    skill,
    event_sink
))]
async fn run_action_with_context(
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

    // Post-step merge conflict detection
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

fn build_task(action: &str, context: &HandoffContext, skill: Option<&SkillDefinition>) -> String {
    let mut task = action.to_string();
    if let Some(skill) = skill {
        if !skill.system_prompt.trim().is_empty() {
            task = format!(
                "{task}\n\nRole context ({}):\n{}",
                skill.name, skill.system_prompt
            );
        }
    }
    if !context.summary.trim().is_empty() {
        task = format!(
            "{task}\n\nACP handoff context:\nSummary: {}\nKey decisions: {}\nActive files: {}",
            context.summary,
            context.key_decisions.join("; "),
            context.active_files.join(", ")
        );
    }
    task
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
        let prompt = build_task(
            "backend.implement",
            &HandoffContext {
                summary: "rate limit after editing auth".to_string(),
                key_decisions: vec!["keep public API stable".to_string()],
                active_files: vec!["src/auth.rs".to_string()],
            },
            None,
        );
        assert!(prompt.contains("ACP handoff context"));
        assert!(prompt.contains("rate limit after editing auth"));
        assert!(prompt.contains("src/auth.rs"));
    }

    #[test]
    fn skill_system_prompt_injected() {
        let skill = SkillDefinition {
            name: "rust-backend".to_string(),
            description: "Rust expert".to_string(),
            system_prompt: "You write idiomatic Rust.".to_string(),
            capabilities: vec!["rust".to_string()],
        };
        let prompt = build_task(
            "backend.implement",
            &HandoffContext::default(),
            Some(&skill),
        );
        assert!(prompt.contains("You write idiomatic Rust."));
        assert!(prompt.contains("Role context"));
    }

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

    #[test]
    fn adaptive_score_boosts_high_success_rate() {
        let model = ModelRecord {
            id: "codex/default".to_string(),
            name: "Codex".to_string(),
            runtime_source: "codex".to_string(),
            tier: acp_protocol::ModelTier::Premium,
            context_window: None,
            pricing: acp_protocol::ModelPricing {
                input: None,
                output: None,
            },
        };
        let scores = vec![CapabilityScoreRecord {
            runtime_type: RuntimeType::Codex,
            model_id: "codex/default".to_string(),
            capability: "rust".to_string(),
            success_count: 9,
            failure_count: 1,
        }];
        let scheduler = Scheduler::new(vec![model]).with_scores(scores);
        let slot = WorkflowSlot {
            role: "backend".to_string(),
            runtime_mode: None,
            preferred: vec![],
            required_capabilities: vec!["rust".to_string()],
            optional: false,
        };
        let base = scheduler.score(
            &slot,
            RuntimeType::Codex,
            None,
            SchedulerProfile::QualityFirst,
        );
        let with_model = scheduler.score(
            &slot,
            RuntimeType::Codex,
            scheduler.models.first(),
            SchedulerProfile::QualityFirst,
        );
        // Model with high success rate on "rust" should score higher than no model
        assert!(with_model > base);
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
            conflict: None,
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
        let result = run_local_pipeline_with_events(
            yaml,
            models,
            Vec::new(),
            Vec::new(),
            temp.path().to_path_buf(),
            Some(tx),
        )
        .await;
        assert!(result.is_ok(), "pipeline failed: {result:?}");
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        assert!(
            !events.is_empty(),
            "expected orchestrator events to be emitted"
        );
    }
}
