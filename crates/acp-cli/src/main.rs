use std::path::PathBuf;

use acp_discover::DiscoveryConfig;
use clap::{Parser, Subcommand};

mod client;
mod commands;
mod tui;

use commands::{
    analytics::{handle_analytics, AnalyticsCommand},
    discover::{handle_doctor, handle_models, handle_provider, ProviderCommand},
    mcp::{handle_mcp, McpCommand},
    memory::{handle_memory, MemoryCommand},
    pipeline::{handle_pipeline, PipelineCommand},
    runtime::{handle_runtime, RuntimeCommand},
    skill::{handle_skill, SkillCommand},
    slot::{handle_slot, SlotCommand},
    workspace::handle_workspace_status,
};

#[derive(Debug, Parser)]
#[command(name = "acp", about = "ACP runtime orchestration CLI")]
pub struct Cli {
    #[arg(long, env = "ACP_HUB_URL", default_value = "http://127.0.0.1:8787")]
    pub hub_url: String,
    #[arg(long, env = "ACP_TOKEN")]
    pub token: Option<String>,
    #[arg(long, env = "ACP_HOME")]
    pub acp_home: Option<PathBuf>,
    #[arg(long, global = true)]
    pub json: bool,
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
    Analytics {
        #[command(subcommand)]
        command: AnalyticsCommand,
    },
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
    Runtime {
        #[command(subcommand)]
        command: RuntimeCommand,
    },
    Memory {
        #[command(subcommand)]
        command: MemoryCommand,
    },
    Workspace {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
    },
    Doctor,
    Dashboard,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut config = DiscoveryConfig::from_env();
    if let Some(acp_home) = &cli.acp_home {
        config.acp_home = acp_home.clone();
    }
    match cli.command.clone() {
        Command::Discover | Command::Runtimes => {
            let runtimes = acp_discover::discover_runtimes().await;
            client::print_yaml(cli.json, &runtimes)?;
        }
        Command::Models { tier } => handle_models(tier, &cli).await?,
        Command::Provider { command } => handle_provider(command, &config, cli.json).await?,
        Command::Pipeline { command } => handle_pipeline(command, &cli, &config).await?,
        Command::Slot { command } => handle_slot(command, &cli).await?,
        Command::Skill { command } => handle_skill(command, &config, cli.json).await?,
        Command::Analytics { command } => handle_analytics(command, &cli).await?,
        Command::Mcp { command } => handle_mcp(command, &config, &cli).await?,
        Command::Runtime { command } => handle_runtime(command, &cli).await?,
        Command::Memory { command } => handle_memory(command, &cli).await?,
        Command::Workspace { repo } => handle_workspace_status(repo).await?,
        Command::Doctor => handle_doctor(&config).await?,
        Command::Dashboard => {
            let hub_client = client::client(&cli)?;
            tui::run_live_dashboard(hub_client).await?;
        }
    }
    Ok(())
}
