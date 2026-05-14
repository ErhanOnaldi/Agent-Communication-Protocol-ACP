use std::{fs, path::PathBuf};

use acp_discover::{doctor, load_providers, provider_statuses, DiscoveryConfig};
use acp_orchestrator::{parse_workflow, run_local_pipeline_with_events, OrchestratorEvent};
use acp_protocol::{
    ArtifactCreateRequest, CapabilityScoreUpdateRequest, PipelineCreateRequest,
    PipelineEventCreateRequest, PipelineStatus, PipelineStatusUpdateRequest, ProviderConfig,
    RuntimeHealth, SlotStatus, SlotUpdateRequest, WorkingContextUpsertRequest,
};
use acp_workspace::WorkspaceEngine;
use agent_client::AgentClient;
use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "acp", about = "ACP runtime orchestration CLI")]
struct Cli {
    #[arg(long, env = "ACP_HUB_URL", default_value = "http://127.0.0.1:8787")]
    hub_url: String,
    #[arg(long, env = "ACP_TOKEN")]
    token: Option<String>,
    #[arg(long, env = "ACP_HOME")]
    acp_home: Option<PathBuf>,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    Discover,
    Models {
        #[arg(long)]
        tier: Option<String>,
    },
    Runtimes,
    Provider {
        #[command(subcommand)]
        command: ProviderCommand,
    },
    Pipeline {
        #[command(subcommand)]
        command: PipelineCommand,
    },
    Slot {
        #[command(subcommand)]
        command: SlotCommand,
    },
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
    Doctor,
    Dashboard,
}

#[derive(Debug, Clone, Subcommand)]
enum ProviderCommand {
    Add(ProviderAddArgs),
    List,
    Validate,
}

#[derive(Debug, Clone, Args)]
struct ProviderAddArgs {
    name: String,
    #[arg(long)]
    base_url: String,
    #[arg(long)]
    api_key_env: String,
}

#[derive(Debug, Clone, Subcommand)]
enum PipelineCommand {
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

#[derive(Debug, Clone, Subcommand)]
enum SlotCommand {
    List {
        pipeline_id: Uuid,
    },
    Assign {
        pipeline_id: Uuid,
        role: String,
        runtime: String,
        #[arg(long)]
        model: Option<String>,
    },
    Vacate {
        pipeline_id: Uuid,
        role: String,
    },
}

#[derive(Debug, Clone, Subcommand)]
enum WorkspaceCommand {
    Status {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut config = DiscoveryConfig::from_env();
    if let Some(acp_home) = &cli.acp_home {
        config.acp_home = acp_home.clone();
    }
    let command = cli.command.clone();
    match command {
        Command::Discover | Command::Runtimes => {
            let runtimes = acp_discover::discover_runtimes().await;
            print_yaml(cli.json, &runtimes)?;
        }
        Command::Models { tier } => {
            let client = client(&cli)?;
            let mut models = client.models().await?;
            if let Some(tier) = tier {
                models.retain(|model| model.tier.to_string() == tier);
            }
            print_yaml(cli.json, &models)?;
        }
        Command::Provider { command } => handle_provider(command, &config, cli.json).await?,
        Command::Pipeline { command } => handle_pipeline(command, &cli).await?,
        Command::Slot { command } => handle_slot(command, &cli).await?,
        Command::Workspace { command } => match command {
            WorkspaceCommand::Status { repo } => {
                let status = WorkspaceEngine::new(repo).status().await?;
                print!("{status}");
            }
        },
        Command::Doctor => {
            let report = doctor(&config).await?;
            println!("Runtimes:");
            for runtime in report.runtimes {
                println!(
                    "- {}: {} ({})",
                    runtime.runtime_type,
                    runtime.health,
                    runtime.path.unwrap_or_else(|| "missing".to_string())
                );
            }
            println!("Providers:");
            for provider in report.providers {
                println!(
                    "- {}: env {} configured={} models={}",
                    provider.name, provider.api_key_env, provider.configured, provider.model_count
                );
            }
            println!("Models: {}", report.models.len());
        }
        Command::Dashboard => {
            let client = client(&cli)?;
            let pipelines = client.pipelines().await?;
            let models = client.models().await?;
            render_dashboard(&pipelines, &models)?;
        }
    }
    Ok(())
}

fn render_dashboard(
    pipelines: &[acp_protocol::PipelineRecord],
    models: &[acp_protocol::ModelRecord],
) -> anyhow::Result<()> {
    use ratatui::{
        backend::CrosstermBackend,
        layout::{Constraint, Direction, Layout},
        style::{Color, Style},
        text::Line,
        widgets::{Block, Borders, List, ListItem, Paragraph},
        Terminal,
    };
    use std::io;

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.draw(|frame| {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(8)])
            .split(frame.area());
        let summary = Paragraph::new(format!(
            "models: {} | pipelines: {} | latest: {}",
            models.len(),
            pipelines.len(),
            pipelines
                .first()
                .map(|pipeline| pipeline.status.to_string())
                .unwrap_or_else(|| "none".to_string())
        ))
        .block(
            Block::default()
                .title("ACP Dashboard")
                .borders(Borders::ALL),
        );
        frame.render_widget(summary, chunks[0]);

        let rows = pipelines
            .iter()
            .take(12)
            .map(|pipeline| {
                ListItem::new(Line::from(format!(
                    "{}  {}  {}",
                    pipeline.id, pipeline.status, pipeline.profile
                )))
            })
            .collect::<Vec<_>>();
        let list = List::new(rows)
            .block(Block::default().title("Pipelines").borders(Borders::ALL))
            .style(Style::default().fg(Color::White));
        frame.render_widget(list, chunks[1]);
    })?;
    Ok(())
}

async fn handle_provider(
    command: ProviderCommand,
    config: &DiscoveryConfig,
    json: bool,
) -> anyhow::Result<()> {
    match command {
        ProviderCommand::Add(args) => {
            let dir = config.acp_home.join("providers");
            fs::create_dir_all(&dir)?;
            let provider = ProviderConfig {
                name: args.name.clone(),
                base_url: args.base_url,
                api_key_env: args.api_key_env,
                models: Vec::new(),
            };
            let path = dir.join(format!("{}.yaml", args.name));
            fs::write(&path, serde_yaml::to_string(&provider)?)?;
            println!("{}", path.display());
        }
        ProviderCommand::List => {
            let providers = load_providers(config)?;
            print_yaml(json, &providers)?;
        }
        ProviderCommand::Validate => {
            let providers = load_providers(config)?;
            print_yaml(json, &provider_statuses(&providers))?;
        }
    }
    Ok(())
}

async fn handle_pipeline(command: PipelineCommand, cli: &Cli) -> anyhow::Result<()> {
    let hub_client = client(cli)?;
    match command {
        PipelineCommand::Run {
            workflow,
            approve_assignments,
            execute,
            repo,
        } => {
            let workflow_yaml = fs::read_to_string(&workflow)
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
                                                "assignments": report.assignments.iter().map(|assignment| serde_json::json!({
                                                    "role": assignment.role,
                                                    "runtime_type": assignment.runtime_type,
                                                    "model_id": assignment.model_id,
                                                    "score": assignment.score,
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

async fn handle_slot(command: SlotCommand, cli: &Cli) -> anyhow::Result<()> {
    let client = client(cli)?;
    match command {
        SlotCommand::List { pipeline_id } => {
            let slots = client.pipeline_slots(pipeline_id).await?;
            print_yaml(cli.json, &slots)?;
        }
        SlotCommand::Assign {
            pipeline_id,
            role,
            runtime,
            model,
        } => {
            let runtime_type = runtime
                .replace('-', "_")
                .parse()
                .map_err(anyhow::Error::msg)?;
            let slot = client
                .update_pipeline_slot(
                    pipeline_id,
                    &role,
                    &SlotUpdateRequest {
                        status: SlotStatus::Assigned,
                        runtime_type: Some(runtime_type),
                        model_id: model,
                        agent_id: None,
                        clear_assignment: false,
                    },
                )
                .await?;
            print_yaml(cli.json, &slot)?;
        }
        SlotCommand::Vacate { pipeline_id, role } => {
            let slot = client
                .update_pipeline_slot(
                    pipeline_id,
                    &role,
                    &SlotUpdateRequest {
                        status: SlotStatus::Vacant,
                        runtime_type: None,
                        model_id: None,
                        agent_id: None,
                        clear_assignment: true,
                    },
                )
                .await?;
            print_yaml(cli.json, &slot)?;
        }
    }
    Ok(())
}

fn client(cli: &Cli) -> anyhow::Result<AgentClient> {
    let token = cli
        .token
        .clone()
        .or_else(|| std::env::var("AGENT_TOKEN").ok())
        .context("ACP_TOKEN or AGENT_TOKEN must be set for hub commands")?;
    AgentClient::new(&cli.hub_url, token)
}

async fn persist_orchestrator_event(
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
        }
        OrchestratorEvent::Handoff { role, context } => {
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
        }
    }
    Ok(())
}

fn print_yaml<T: serde::Serialize>(json: bool, value: &T) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        print!("{}", serde_yaml::to_string(value)?);
    }
    Ok(())
}
