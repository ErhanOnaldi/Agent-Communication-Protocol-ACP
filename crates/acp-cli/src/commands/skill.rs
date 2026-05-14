use acp_discover::{load_skills, DiscoveryConfig};
use anyhow::Context;

use crate::client::print_yaml;

#[derive(Debug, Clone, clap::Subcommand)]
pub enum SkillCommand {
    List,
    Show { name: String },
}

pub async fn handle_skill(
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
