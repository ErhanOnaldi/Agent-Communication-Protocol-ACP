use std::path::PathBuf;

use acp_discover::{load_skills, DiscoveryConfig};
use acp_orchestrator::{parse_workflow, run_local_pipeline_with_events};
use acp_protocol::{
    PipelineCreateRequest, PipelineEventCreateRequest, PipelineStatus, PipelineStatusUpdateRequest,
};
use anyhow::Context;
use uuid::Uuid;

use crate::client::{client, print_yaml};
use crate::commands::persist::persist_orchestrator_event;
use crate::Cli;

#[derive(Debug, Clone, clap::Subcommand)]
pub enum PipelineCommand {
    Run {
        workflow: PathBuf,
        #[arg(long)]
        approve_assignments: bool,
        #[arg(long)]
        execute: bool,
        #[arg(long, default_value = ".")]
        repo: PathBuf,
    },
    List,
    Status {
        pipeline_id: Uuid,
    },
}

pub async fn handle_pipeline(
    command: PipelineCommand,
    cli: &Cli,
    config: &DiscoveryConfig,
) -> anyhow::Result<()> {
    let hub_client = client(cli)?;
    match command {
        PipelineCommand::Run {
            workflow,
            approve_assignments,
            execute,
            repo,
        } => {
            let workflow_yaml = std::fs::read_to_string(&workflow)
                .with_context(|| format!("failed to read {}", workflow.display()))?;
            parse_workflow(&workflow_yaml)?;
            let pipeline = hub_client
                .create_pipeline(&PipelineCreateRequest {
                    workflow_yaml,
                    approve_assignments,
                })
                .await?;
            if execute {
                if !approve_assignments {
                    anyhow::bail!(
                        "pipeline assignments require approval; rerun with --approve-assignments --execute"
                    );
                }
                hub_client
                    .update_pipeline_status(
                        pipeline.id,
                        &PipelineStatusUpdateRequest {
                            status: PipelineStatus::Running,
                            completed: false,
                        },
                    )
                    .await?;
                hub_client
                    .create_pipeline_event(
                        pipeline.id,
                        &PipelineEventCreateRequest {
                            pipeline_id: pipeline.id,
                            agent_id: None,
                            event_type: "local_execution_started".to_string(),
                            payload: serde_json::json!({ "repo": repo }),
                            correlation_id: None,
                            causation_id: None,
                        },
                    )
                    .await?;

                let models = hub_client.models().await?;
                let capability_scores = hub_client.capability_scores().await.unwrap_or_default();
                let skills = load_skills(config).unwrap_or_default();

                let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
                let event_client = client(cli)?;
                let event_pipeline_id = pipeline.id;
                let event_task = tokio::spawn(async move {
                    while let Some(event) = event_rx.recv().await {
                        persist_orchestrator_event(&event_client, event_pipeline_id, event).await?;
                    }
                    anyhow::Ok(())
                });

                match run_local_pipeline_with_events(
                    &pipeline.workflow_yaml,
                    models,
                    capability_scores,
                    skills,
                    repo,
                    Some(event_tx),
                )
                .await
                {
                    Ok(report) => {
                        event_task.await??;
                        let pipeline = hub_client
                            .update_pipeline_status(
                                pipeline.id,
                                &PipelineStatusUpdateRequest {
                                    status: PipelineStatus::Succeeded,
                                    completed: true,
                                },
                            )
                            .await?;
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&serde_json::json!({
                                    "pipeline": pipeline,
                                    "report": {
                                        "workflow_id": report.workflow_id,
                                        "assignments": report.assignments.iter().map(|a| serde_json::json!({
                                            "role": a.role,
                                            "runtime_type": a.runtime_type,
                                            "model_id": a.model_id,
                                            "score": a.score,
                                        })).collect::<Vec<_>>(),
                                        "step_count": report.step_results.len(),
                                        "slot_event_count": report.slot_events.len(),
                                    }
                                }))?
                            );
                        } else {
                            println!(
                                "Pipeline {} succeeded with {} executed steps",
                                pipeline.id,
                                report.step_results.len()
                            );
                        }
                        return Ok(());
                    }
                    Err(err) => {
                        event_task.abort();
                        hub_client
                            .create_pipeline_event(
                                pipeline.id,
                                &PipelineEventCreateRequest {
                                    pipeline_id: pipeline.id,
                                    agent_id: None,
                                    event_type: "local_execution_failed".to_string(),
                                    payload: serde_json::json!({ "error": err.to_string() }),
                                    correlation_id: None,
                                    causation_id: None,
                                },
                            )
                            .await?;
                        hub_client
                            .update_pipeline_status(
                                pipeline.id,
                                &PipelineStatusUpdateRequest {
                                    status: PipelineStatus::Failed,
                                    completed: true,
                                },
                            )
                            .await?;
                        return Err(err);
                    }
                }
            }
            print_yaml(cli.json, &pipeline)?;
        }
        PipelineCommand::List => {
            let pipelines = hub_client.pipelines().await?;
            print_yaml(cli.json, &pipelines)?;
        }
        PipelineCommand::Status { pipeline_id } => {
            let pipeline = hub_client.pipeline(pipeline_id).await?;
            let slots = hub_client.pipeline_slots(pipeline_id).await?;
            let events = hub_client.pipeline_events(pipeline_id).await?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "pipeline": pipeline,
                        "slots": slots,
                        "events": events,
                    }))?
                );
            } else {
                println!("Pipeline: {} {}", pipeline.id, pipeline.status);
                println!("Slots: {}", slots.len());
                println!("Events: {}", events.len());
            }
        }
    }
    Ok(())
}
