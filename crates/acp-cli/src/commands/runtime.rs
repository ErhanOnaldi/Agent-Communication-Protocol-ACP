use clap::Subcommand;

use crate::{client::print_yaml, Cli};

#[derive(Debug, Clone, Subcommand)]
pub enum RuntimeCommand {
    Interrupt { agent_id: String },
    Shutdown { agent_id: String },
}

pub async fn handle_runtime(command: RuntimeCommand, cli: &Cli) -> anyhow::Result<()> {
    let client = crate::client::client(cli)?;
    match command {
        RuntimeCommand::Interrupt { agent_id } => {
            let response = client.interrupt_runtime(&agent_id).await?;
            print_yaml(cli.json, &response)?;
        }
        RuntimeCommand::Shutdown { agent_id } => {
            let response = client.shutdown_runtime(&agent_id).await?;
            print_yaml(cli.json, &response)?;
        }
    }
    Ok(())
}
