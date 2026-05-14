use acp_discover::{load_mcp_config, McpManager, McpServerMode};
use clap::Subcommand;

use crate::{client::print_yaml, Cli};

#[derive(Debug, Clone, Subcommand)]
pub enum McpCommand {
    List,
    Start {
        name: String,
        #[arg(long)]
        shared: bool,
        #[arg(long)]
        agent: Option<String>,
    },
    Health {
        name: Option<String>,
    },
    Shutdown {
        name: String,
    },
    Doctor,
}

pub async fn handle_mcp(
    command: McpCommand,
    config: &acp_discover::DiscoveryConfig,
    cli: &Cli,
) -> anyhow::Result<()> {
    let mcp = load_mcp_config(config)?;
    match command {
        McpCommand::List => print_yaml(cli.json, &mcp)?,
        McpCommand::Doctor => {
            let report: Vec<_> = mcp
                .servers
                .iter()
                .map(|(name, server)| {
                    serde_json::json!({
                        "name": name,
                        "command": server.command,
                        "mode": server.mode,
                        "auto_start": server.auto_start,
                        "requires_agent_id": server.mode == McpServerMode::Isolated,
                    })
                })
                .collect();
            print_yaml(cli.json, &report)?;
        }
        McpCommand::Start {
            name,
            shared,
            agent,
        } => {
            let server = mcp
                .servers
                .get(&name)
                .ok_or_else(|| anyhow::anyhow!("MCP server {name} not found"))?;
            if !shared && server.mode == McpServerMode::Isolated && agent.is_none() {
                anyhow::bail!("isolated MCP server {name} requires --agent <id>");
            }
            let manager = McpManager::default();
            let launch = manager.start(&name, server, agent.as_deref()).await?;
            print_yaml(cli.json, &launch)?;
        }
        McpCommand::Health { name } => {
            let client = crate::client::client(cli)?;
            if let Some(name) = name {
                let health = client.mcp_health(&name).await?;
                print_yaml(cli.json, &health)?;
            } else {
                let servers = client.mcp_servers().await?;
                let mut health = Vec::new();
                for server in servers {
                    health.push(client.mcp_health(&server.name).await?);
                }
                print_yaml(cli.json, &health)?;
            }
        }
        McpCommand::Shutdown { name } => {
            let manager = McpManager::default();
            let launch = manager.shutdown(&name).await?;
            print_yaml(cli.json, &launch)?;
        }
    }
    Ok(())
}
