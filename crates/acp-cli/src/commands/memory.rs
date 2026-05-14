use clap::Subcommand;
use uuid::Uuid;

use crate::{client::print_yaml, Cli};

#[derive(Debug, Clone, Subcommand)]
pub enum MemoryCommand {
    Search { pipeline_id: Uuid, query: String },
}

pub async fn handle_memory(command: MemoryCommand, cli: &Cli) -> anyhow::Result<()> {
    let client = crate::client::client(cli)?;
    match command {
        MemoryCommand::Search { pipeline_id, query } => {
            let result = client.memory_search(pipeline_id, &query).await?;
            print_yaml(cli.json, &result)?;
        }
    }
    Ok(())
}
