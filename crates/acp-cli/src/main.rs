use std::{
    fs, io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use acp_discover::{doctor, load_providers, load_skills, provider_statuses, DiscoveryConfig};
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
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Terminal,
};
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
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
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
enum SkillCommand {
    List,
    Show { name: String },
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
        Command::Pipeline { command } => handle_pipeline(command, &cli, &config).await?,
        Command::Slot { command } => handle_slot(command, &cli).await?,
        Command::Skill { command } => handle_skill(command, &config, cli.json).await?,
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
            let hub_client = client(&cli)?;
            run_live_dashboard(hub_client).await?;
        }
    }
    Ok(())
}

// ── Live dashboard ────────────────────────────────────────────────────────────

#[derive(Default)]
struct DashboardState {
    pipelines: Vec<acp_protocol::PipelineRecord>,
    models: Vec<acp_protocol::ModelRecord>,
    events: Vec<String>,
    quit: bool,
}

async fn run_live_dashboard(hub_client: AgentClient) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let state = Arc::new(Mutex::new(DashboardState::default()));
    let state_bg = state.clone();

    // Background refresh task — fetches pipelines, models, and recent events every 3 s
    let client_bg = hub_client.clone();
    let refresh_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let pipelines = client_bg.pipelines().await.unwrap_or_default();
            let models = client_bg.models().await.unwrap_or_default();
            let events: Vec<String> = if let Some(p) = pipelines.first() {
                client_bg
                    .pipeline_events(p.id)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .rev()
                    .take(10)
                    .map(|e| {
                        format!(
                            "{} — {}",
                            e.event_type,
                            e.agent_id.as_deref().unwrap_or("-")
                        )
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let mut s = state_bg.lock().unwrap();
            s.pipelines = pipelines;
            s.models = models;
            s.events = events;
            if s.quit {
                break;
            }
        }
    });

    loop {
        {
            let s = state.lock().unwrap();
            if s.quit {
                break;
            }
            let pipelines = s.pipelines.clone();
            let models = s.models.clone();
            let events = s.events.clone();
            drop(s);
            terminal.draw(|frame| draw_dashboard(frame, &pipelines, &models, &events))?;
        }

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => {
                        state.lock().unwrap().quit = true;
                        break;
                    }
                    KeyCode::Char('r') => {
                        // Manual refresh
                        if let Ok(pipelines) = hub_client.pipelines().await {
                            state.lock().unwrap().pipelines = pipelines;
                        }
                        if let Ok(models) = hub_client.models().await {
                            state.lock().unwrap().models = models;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    refresh_task.abort();
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}

fn draw_dashboard(
    frame: &mut ratatui::Frame,
    pipelines: &[acp_protocol::PipelineRecord],
    models: &[acp_protocol::ModelRecord],
    events: &[String],
) {
    let area = frame.area();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(5),
        ])
        .split(area);

    // Header
    let header = Paragraph::new(format!(
        " ACP Dashboard  |  models: {}  |  pipelines: {}  |  [r] refresh  [q] quit",
        models.len(),
        pipelines.len(),
    ))
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, vertical[0]);

    // Middle: pipelines + models side by side
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(vertical[1]);

    let pipeline_items: Vec<ListItem> = pipelines
        .iter()
        .take(20)
        .map(|p| {
            let color = match p.status {
                PipelineStatus::Succeeded => Color::Green,
                PipelineStatus::Failed => Color::Red,
                PipelineStatus::Running => Color::Yellow,
                _ => Color::White,
            };
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:.8}  ", p.id),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("{:<12}", p.status.to_string()),
                    Style::default().fg(color),
                ),
                Span::raw(p.profile.to_string()),
            ]))
        })
        .collect();
    let pipeline_list =
        List::new(pipeline_items).block(Block::default().title("Pipelines").borders(Borders::ALL));
    frame.render_widget(pipeline_list, horizontal[0]);

    let model_items: Vec<ListItem> = models
        .iter()
        .take(20)
        .map(|m| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<8}", m.tier.to_string()),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw(format!("  {}", m.name)),
            ]))
        })
        .collect();
    let model_list =
        List::new(model_items).block(Block::default().title("Models").borders(Borders::ALL));
    frame.render_widget(model_list, horizontal[1]);

    // Events log at the bottom
    let recent: Vec<ListItem> = events
        .iter()
        .rev()
        .take(3)
        .map(|e| ListItem::new(Line::from(Span::raw(e.as_str()))))
        .collect();
    let log = List::new(recent)
        .block(
            Block::default()
                .title("Recent events")
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::Gray));
    frame.render_widget(log, vertical[2]);
}

// ── Providers ─────────────────────────────────────────────────────────────────

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

// ── Skills ────────────────────────────────────────────────────────────────────

async fn handle_skill(
    command: SkillCommand,
    config: &DiscoveryConfig,
    json: bool,
) -> anyhow::Result<()> {
    let skills = load_skills(config)?;
    match command {
        SkillCommand::List => {
            if json {
                println!("{}", serde_json::to_string_pretty(&skills)?);
            } else {
                for skill in &skills {
                    println!("{:<20} {}", skill.name, skill.description);
                }
                if skills.is_empty() {
                    println!(
                        "No skills found in {}",
                        config.acp_home.join("skills").display()
                    );
                }
            }
        }
        SkillCommand::Show { name } => {
            let skill = skills
                .iter()
                .find(|s| s.name == name)
                .with_context(|| format!("skill '{name}' not found"))?;
            print_yaml(json, skill)?;
        }
    }
    Ok(())
}

// ── Pipelines ─────────────────────────────────────────────────────────────────

async fn handle_pipeline(
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

// ── Slots ─────────────────────────────────────────────────────────────────────

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

// ── Helpers ───────────────────────────────────────────────────────────────────

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
                            "runtime_type": result.runtime_type,
                            "model_id": result.model_id,
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
