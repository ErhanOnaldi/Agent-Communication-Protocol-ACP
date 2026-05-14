use acp_orchestrator::OrchestratorEvent;
use acp_protocol::{
    ArtifactCreateRequest, CapabilityScoreUpdateRequest, ContextCompressionCreateRequest,
    PipelineEventCreateRequest, RuntimeHealth, SchedulerDecisionCreateRequest,
    SemanticMemoryCreateRequest, SlotUpdateRequest, StepMetricCreateRequest,
    WorkingContextUpsertRequest,
};
use agent_client::AgentClient;
use uuid::Uuid;

pub async fn persist_orchestrator_event(
    client: &AgentClient,
    pipeline_id: Uuid,
    event: OrchestratorEvent,
) -> anyhow::Result<()> {
    match event {
        OrchestratorEvent::Slot(event) => {
            client
                .update_pipeline_slot(
                    pipeline_id,
                    &event.role,
                    &SlotUpdateRequest {
                        status: event.status,
                        runtime_type: event.runtime_type,
                        model_id: event.model_id.clone(),
                        agent_id: None,
                        clear_assignment: false,
                    },
                )
                .await?;
            client
                .create_pipeline_event(
                    pipeline_id,
                    &PipelineEventCreateRequest {
                        pipeline_id,
                        agent_id: Some(event.role.clone()),
                        event_type: "slot_lifecycle".to_string(),
                        payload: serde_json::json!({
                            "role": event.role,
                            "status": event.status,
                            "reason": event.reason,
                        }),
                        correlation_id: None,
                        causation_id: None,
                    },
                )
                .await?;
        }
        OrchestratorEvent::Step(result) => {
            let model_id = result
                .model_id
                .clone()
                .unwrap_or_else(|| format!("{}/default", result.runtime_type));
            client
                .update_capability_score(&CapabilityScoreUpdateRequest {
                    runtime_type: result.runtime_type,
                    model_id,
                    capability: result.role.clone(),
                    success: result.health == RuntimeHealth::Healthy,
                    latency_ms: Some(result.latency_ms),
                    had_conflict: result.conflict.is_some(),
                    retry_count: 0,
                })
                .await?;
            client
                .create_pipeline_event(
                    pipeline_id,
                    &PipelineEventCreateRequest {
                        pipeline_id,
                        agent_id: Some(result.role.clone()),
                        event_type: "step_completed".to_string(),
                        payload: serde_json::json!({
                            "step": result.step,
                            "health": result.health,
                            "runtime_type": result.runtime_type,
                            "model_id": result.model_id,
                            "latency_ms": result.latency_ms,
                        }),
                        correlation_id: None,
                        causation_id: None,
                    },
                )
                .await?;
            client
                .create_artifact(
                    pipeline_id,
                    &ArtifactCreateRequest {
                        pipeline_id,
                        stage_name: result.step.clone(),
                        artifact_type: "runtime_output".to_string(),
                        content: format!(
                            "stdout:\n{}\n\nstderr:\n{}",
                            result.stdout, result.stderr
                        ),
                        created_by: result.role.clone(),
                    },
                )
                .await?;
            client
                .create_step_metric(
                    pipeline_id,
                    &StepMetricCreateRequest {
                        pipeline_id,
                        step_name: result.step.clone(),
                        role: result.role.clone(),
                        runtime_type: Some(result.runtime_type),
                        model_id: result.model_id.clone(),
                        latency_ms: Some(result.latency_ms),
                        health: result.health,
                    },
                )
                .await?;
        }
        OrchestratorEvent::Handoff { role, context } => {
            let summary = context.summary.clone();
            let semantic_refs = context.semantic_hints.clone();
            client
                .upsert_working_context(
                    pipeline_id,
                    &role,
                    &WorkingContextUpsertRequest {
                        summary: context.summary,
                        key_decisions: serde_json::json!(context.key_decisions),
                        active_files: context.active_files,
                    },
                )
                .await?;
            client
                .create_context_compression(
                    pipeline_id,
                    &ContextCompressionCreateRequest {
                        pipeline_id,
                        role,
                        compressor: "handoff".to_string(),
                        source_tokens: summary.split_whitespace().count() as i64,
                        summary,
                        semantic_refs,
                    },
                )
                .await?;
        }
        OrchestratorEvent::MergeConflict {
            role,
            branch,
            details,
        } => {
            client
                .create_pipeline_event(
                    pipeline_id,
                    &PipelineEventCreateRequest {
                        pipeline_id,
                        agent_id: Some(role.clone()),
                        event_type: "merge_conflict".to_string(),
                        payload: serde_json::json!({
                            "role": role,
                            "branch": branch,
                            "details": details,
                        }),
                        correlation_id: None,
                        causation_id: None,
                    },
                )
                .await?;
        }
        OrchestratorEvent::SchedulerDecision {
            role,
            insights,
            reason,
        } => {
            client
                .create_scheduler_decision(
                    pipeline_id,
                    &SchedulerDecisionCreateRequest {
                        pipeline_id,
                        role,
                        runtime_type: insights.runtime_type,
                        model_id: insights.model_id,
                        base_score: insights.base_score,
                        learned_delta: insights.learned_delta,
                        profile_boost: insights.profile_boost,
                        final_score: insights.final_score,
                        reason,
                    },
                )
                .await?;
        }
        OrchestratorEvent::ContextCompressed {
            role,
            compressor,
            source_tokens,
            summary,
            semantic_refs,
        } => {
            client
                .create_context_compression(
                    pipeline_id,
                    &ContextCompressionCreateRequest {
                        pipeline_id,
                        role,
                        compressor,
                        source_tokens,
                        summary,
                        semantic_refs,
                    },
                )
                .await?;
        }
        OrchestratorEvent::SemanticMemory {
            item_id,
            content,
            embedding_provider,
            embedding_model,
            embedding,
        } => {
            client
                .create_semantic_memory(
                    pipeline_id,
                    &SemanticMemoryCreateRequest {
                        pipeline_id,
                        item_id,
                        content,
                        embedding_provider,
                        embedding_model,
                        embedding,
                    },
                )
                .await?;
        }
    }
    Ok(())
}
