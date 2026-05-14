use acp_discover::{load_providers, provider_statuses, DiscoveryConfig};
use acp_protocol::ProviderConfig;

use crate::client::print_yaml;
use crate::Cli;

#[derive(Debug, Clone, clap::Subcommand)]
pub enum ProviderCommand {
    Add(ProviderAddArgs),
    List,
    Validate,
}

#[derive(Debug, Clone, clap::Args)]
pub struct ProviderAddArgs {
    pub name: String,
    #[arg(long)]
    pub base_url: String,
    #[arg(long)]
    pub api_key_env: String,
}

pub async fn handle_provider(
    command: ProviderCommand,
    config: &DiscoveryConfig,
    json: bool,
) -> anyhow::Result<()> {
    match command {
        ProviderCommand::Add(args) => {
            let dir = config.acp_home.join("providers");
            std::fs::create_dir_all(&dir)?;
            let provider = ProviderConfig {
                name: args.name.clone(),
                base_url: args.base_url,
                api_key_env: args.api_key_env,
                models: Vec::new(),
                embedding_model: None,
                embedding_base_url: None,
            };
            let path = dir.join(format!("{}.yaml", args.name));
            std::fs::write(&path, serde_yaml::to_string(&provider)?)?;
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

pub async fn handle_doctor(config: &DiscoveryConfig) -> anyhow::Result<()> {
    let report = acp_discover::doctor(config).await?;
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
    println!("MCP:");
    for server in report.mcp {
        println!(
            "- {}: {} ({})",
            server.name,
            server.status,
            server.message.unwrap_or_else(|| "configured".to_string())
        );
    }
    Ok(())
}

pub async fn handle_models(tier: Option<String>, cli: &Cli) -> anyhow::Result<()> {
    let client = crate::client::client(cli)?;
    let mut models = client.models().await?;
    if let Some(tier) = tier {
        models.retain(|m| m.tier.to_string() == tier);
    }
    print_yaml(cli.json, &models)
}
