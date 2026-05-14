use acp_protocol::{SlotStatus, SlotUpdateRequest};
use uuid::Uuid;

use crate::client::{client, print_yaml};
use crate::Cli;

#[derive(Debug, Clone, clap::Subcommand)]
pub enum SlotCommand {
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

pub async fn handle_slot(command: SlotCommand, cli: &Cli) -> anyhow::Result<()> {
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
