use acp_protocol::SkillDefinition;
use anyhow::Context;

use crate::DiscoveryConfig;

pub fn load_skills(config: &DiscoveryConfig) -> anyhow::Result<Vec<SkillDefinition>> {
    let dir = config.acp_home.join("skills");
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut skills = Vec::new();
    for entry in
        std::fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read skill {}", path.display()))?;
        let skill: SkillDefinition = serde_yaml::from_str(&text)
            .with_context(|| format!("failed to parse skill {}", path.display()))?;
        skills.push(skill);
    }
    skills.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(skills)
}
