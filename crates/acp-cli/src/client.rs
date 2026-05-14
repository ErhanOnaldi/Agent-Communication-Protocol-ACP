use agent_client::AgentClient;
use anyhow::Context;

use crate::Cli;

pub fn client(cli: &Cli) -> anyhow::Result<AgentClient> {
    let token = cli
        .token
        .clone()
        .or_else(|| std::env::var("AGENT_TOKEN").ok())
        .context("ACP_TOKEN or AGENT_TOKEN must be set for hub commands")?;
    AgentClient::new(&cli.hub_url, token)
}

pub fn print_yaml<T: serde::Serialize>(json: bool, value: &T) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        print!("{}", serde_yaml::to_string(value)?);
    }
    Ok(())
}
