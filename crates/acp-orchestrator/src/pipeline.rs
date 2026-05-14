use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use acp_protocol::{
    CapabilityScoreRecord, ModelRecord, RuntimeHealth, SchedulerProfile, SkillDefinition,
    SlotStatus, WorkflowConfig, WorkflowStep,
};
use acp_workspace::WorkspaceEngine;
use anyhow::{bail, Context};
use tokio::{
    sync::{mpsc::UnboundedSender, Mutex, RwLock},
    task::JoinSet,
    time::{timeout, Instant},
};
use tracing::instrument;

use crate::{
    adaptive::AdaptiveController,
    memory::{compressor_for_models, ContextCompressor, HandoffContext},
    recovery::{
        action_role, failure_action_for_role, find_skill, run_action_with_recovery, ActionCtx,
    },
    scheduler::Scheduler,
    semantic::{EmbeddingProvider, HashedEmbeddingProvider, MemoryIndex},
    slots::{emit_slot, emit_step, SlotLifecycleEvent},
    OrchestratorEvent, PipelineRunReport,
};

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
    let compressor = compressor_for_models(&models);
    let embedding_provider = HashedEmbeddingProvider;
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
        let candidates_with_insights =
            scheduler.candidates_with_insights(slot_name, slot, config.workflow.profile)?;
        let candidates: Vec<_> = candidates_with_insights
            .iter()
            .map(|(assignment, _)| assignment.clone())
            .collect();
        let assignment = candidates
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no runtime candidates for role {slot_name}"))?;
        if let Some((_, insights)) = candidates_with_insights.first() {
            emit_scheduler_decision(
                &event_sink,
                slot_name.clone(),
                insights.clone(),
                "initial assignment".to_string(),
            );
        }
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

    let assignments = Arc::new(RwLock::new(assignments));
    let handoff_contexts: Arc<Mutex<BTreeMap<String, HandoffContext>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let slot_events = Arc::new(Mutex::new(slot_events));
    let mut step_results = Vec::new();

    // Phase 3: semantic memory index and adaptive profile controller.
    let mut memory = MemoryIndex::new();
    let mut current_profile = config.workflow.profile;
    let mut controller = AdaptiveController::new(current_profile);
    // Track last step health for conditional step evaluation.
    let mut last_health: Option<RuntimeHealth> = None;

    for step in &config.workflow.steps {
        match step {
            WorkflowStep::Action(action) => {
                enforce_pipeline_deadline(pipeline_deadline)?;
                let role = action_role(action);
                let fa = failure_action_for_role(failure_policy, &role);
                let skill = find_skill(&skills, &role, &config.workflow.slots);

                // Inject semantic context from prior steps.
                inject_semantic_context(&memory, action, &handoff_contexts).await;

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

                // Update semantic index with step output.
                let snippet = result.stdout.chars().take(500).collect::<String>();
                memory.add(&result.step, &snippet);
                record_step_memory(
                    &event_sink,
                    compressor.as_ref(),
                    &embedding_provider,
                    &result.role,
                    &result.step,
                    &result.stdout,
                    &result.stderr,
                );

                last_health = Some(result.health);
                let updated_profile = controller.record_step(result.health);
                if updated_profile != current_profile {
                    current_profile = updated_profile;
                    refresh_assignments(
                        &scheduler,
                        &config,
                        &assignments,
                        current_profile,
                        &event_sink,
                    )
                    .await?;
                    tracing::info!(profile = %updated_profile, "adaptive: profile updated mid-pipeline");
                }

                emit_step(&event_sink, result.clone());
                step_results.push(result);
            }
            WorkflowStep::Parallel { parallel } => {
                // Pre-compute semantic context for each role before spawning.
                for action in parallel.iter() {
                    inject_semantic_context(&memory, action, &handoff_contexts).await;
                }

                let mut join_set = JoinSet::new();
                for action in parallel {
                    enforce_pipeline_deadline(pipeline_deadline)?;
                    let action = action.clone();
                    let role = action_role(&action);
                    let fa = failure_action_for_role(failure_policy, &role);
                    let skill = find_skill(&skills, &role, &config.workflow.slots);
                    let parallel_timeout = effective_step_timeout(step_timeout, pipeline_deadline);
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
                    let snippet = result.stdout.chars().take(500).collect::<String>();
                    memory.add(&result.step, &snippet);
                    record_step_memory(
                        &event_sink,
                        compressor.as_ref(),
                        &embedding_provider,
                        &result.role,
                        &result.step,
                        &result.stdout,
                        &result.stderr,
                    );
                    last_health = Some(result.health);
                    let updated_profile = controller.record_step(result.health);
                    if updated_profile != current_profile {
                        current_profile = updated_profile;
                        refresh_assignments(
                            &scheduler,
                            &config,
                            &assignments,
                            current_profile,
                            &event_sink,
                        )
                        .await?;
                    }
                    emit_step(&event_sink, result.clone());
                    step_results.push(result);
                }
            }
            WorkflowStep::Conditional {
                action,
                when_healthy,
            } => {
                // Only run if the previous step health matches the condition.
                if *when_healthy {
                    match last_health {
                        Some(h) if h != RuntimeHealth::Healthy => {
                            tracing::info!(
                                action = %action,
                                prev_health = %h,
                                "skipping conditional step: previous step was not healthy"
                            );
                            continue;
                        }
                        _ => {}
                    }
                }

                enforce_pipeline_deadline(pipeline_deadline)?;
                let role = action_role(action);
                let fa = failure_action_for_role(failure_policy, &role);
                let skill = find_skill(&skills, &role, &config.workflow.slots);

                inject_semantic_context(&memory, action, &handoff_contexts).await;

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

                let snippet = result.stdout.chars().take(500).collect::<String>();
                memory.add(&result.step, &snippet);
                record_step_memory(
                    &event_sink,
                    compressor.as_ref(),
                    &embedding_provider,
                    &result.role,
                    &result.step,
                    &result.stdout,
                    &result.stderr,
                );
                last_health = Some(result.health);
                let updated_profile = controller.record_step(result.health);
                if updated_profile != current_profile {
                    current_profile = updated_profile;
                    refresh_assignments(
                        &scheduler,
                        &config,
                        &assignments,
                        current_profile,
                        &event_sink,
                    )
                    .await?;
                }

                emit_step(&event_sink, result.clone());
                step_results.push(result);
            }
        }
    }

    let assignments = assignments.read().await.clone();
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

/// Queries the semantic index and injects relevant snippets into the handoff context for a role.
async fn inject_semantic_context(
    memory: &MemoryIndex,
    action: &str,
    handoff_contexts: &Arc<Mutex<BTreeMap<String, HandoffContext>>>,
) {
    let hints = memory.search(action, 3);
    if !hints.is_empty() {
        let role = action_role(action);
        let mut ctx = handoff_contexts.lock().await;
        ctx.entry(role).or_default().semantic_hints = hints;
    }
}

async fn refresh_assignments(
    scheduler: &Scheduler,
    config: &WorkflowConfig,
    assignments: &Arc<RwLock<Vec<crate::scheduler::Assignment>>>,
    profile: SchedulerProfile,
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
) -> anyhow::Result<()> {
    let mut refreshed = Vec::new();
    for (slot_name, slot) in &config.workflow.slots {
        let candidates_with_insights =
            scheduler.candidates_with_insights(slot_name, slot, profile)?;
        if let Some((primary, insights)) = candidates_with_insights.first() {
            emit_scheduler_decision(
                event_sink,
                slot_name.clone(),
                insights.clone(),
                format!("adaptive reassignment using {profile} profile"),
            );
            emit_slot(
                &mut Vec::new(),
                event_sink,
                SlotLifecycleEvent {
                    role: slot_name.clone(),
                    status: SlotStatus::Assigned,
                    reason: format!(
                        "adaptive reassigned {} {} using {} profile",
                        primary.runtime_type,
                        primary.model_id.as_deref().unwrap_or("default"),
                        profile
                    ),
                    runtime_type: Some(primary.runtime_type),
                    model_id: primary.model_id.clone(),
                },
            );
        }
        refreshed.extend(
            candidates_with_insights
                .into_iter()
                .map(|(assignment, _)| assignment),
        );
    }
    *assignments.write().await = refreshed;
    Ok(())
}

fn emit_scheduler_decision(
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
    role: String,
    insights: acp_protocol::SchedulerInsights,
    reason: String,
) {
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::SchedulerDecision {
            role,
            insights,
            reason,
        });
    }
}

fn record_step_memory(
    event_sink: &Option<UnboundedSender<OrchestratorEvent>>,
    compressor: &dyn ContextCompressor,
    embedding_provider: &dyn EmbeddingProvider,
    role: &str,
    item_id: &str,
    stdout: &str,
    stderr: &str,
) {
    let content = format!("{stdout}\n{stderr}");
    let semantic_refs = vec![item_id.to_string()];
    let compressed = compressor.compress(role, &content, &semantic_refs);
    if let Some(sink) = event_sink {
        let _ = sink.send(OrchestratorEvent::ContextCompressed {
            role: role.to_string(),
            compressor: compressed.compressor,
            source_tokens: compressed.source_tokens,
            summary: compressed.summary,
            semantic_refs: compressed.semantic_refs,
        });
        let _ = sink.send(OrchestratorEvent::SemanticMemory {
            item_id: item_id.to_string(),
            content,
            embedding_provider: embedding_provider.provider_name().to_string(),
            embedding_model: embedding_provider.model_name().to_string(),
            embedding: embedding_provider.embed(stdout),
        });
    }
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
    fn parses_conditional_step() {
        let workflow = parse_workflow(
            r#"
workflow:
  id: cond-test
  name: Conditional Test
  profile: quality-first
  slots: {}
  steps:
    - architect.plan
    - action: reviewer.audit
      when_healthy: true
"#,
        )
        .unwrap();
        assert_eq!(workflow.workflow.steps.len(), 2);
        assert!(matches!(
            &workflow.workflow.steps[1],
            WorkflowStep::Conditional {
                when_healthy: true,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn pipeline_emits_live_events_with_fake_codex_runtime() {
        use acp_protocol::{ModelPricing, ModelTier};

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
            tier: ModelTier::Premium,
            context_window: None,
            pricing: ModelPricing {
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
