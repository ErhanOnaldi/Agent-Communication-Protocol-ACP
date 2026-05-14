use std::{env, path::PathBuf};

#[derive(Debug, Clone)]
pub struct ClaudexProvider {
    pub base_url: String,
    pub api_key: String,
    pub config_dir: PathBuf,
}

impl ClaudexProvider {
    pub fn from_env() -> Self {
        let base_url = env::var("ACP_CLAUDEX_BASE_URL")
            .or_else(|_| env::var("ANTHROPIC_BASE_URL"))
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());
        let api_key = env::var("ACP_CLAUDEX_AUTH_TOKEN")
            .or_else(|_| env::var("ANTHROPIC_AUTH_TOKEN"))
            .unwrap_or_default();
        let config_dir = env::var_os("ACP_CLAUDEX_CONFIG_DIR")
            .map(PathBuf::from)
            .or_else(|| env::var_os("CLAUDE_CONFIG_DIR").map(PathBuf::from))
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".acp/claudex")))
            .unwrap_or_else(|| PathBuf::from(".acp/claudex"));
        Self {
            base_url,
            api_key,
            config_dir,
        }
    }
}
