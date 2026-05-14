use std::env;
use std::path::{Path, PathBuf};

use acp_protocol::{RuntimeHealth, RuntimeRecord, RuntimeType};
use tokio::process::Command;

pub async fn discover_runtimes() -> Vec<RuntimeRecord> {
    let specs = [
        (RuntimeType::ClaudeCode, "Claude Code", "claude"),
        (RuntimeType::Codex, "Codex CLI", "codex"),
        (RuntimeType::Gemini, "Gemini CLI", "gemini"),
        (RuntimeType::Copilot, "GitHub Copilot CLI", "copilot"),
    ];
    let mut records = Vec::new();
    for (runtime_type, name, binary) in specs {
        let path = which(binary);
        let version = if path.is_some() {
            command_version(binary).await.ok()
        } else {
            None
        };
        records.push(RuntimeRecord {
            runtime_type,
            name: name.to_string(),
            binary: binary.to_string(),
            path: path.map(|p| p.display().to_string()),
            version,
            health: if which(binary).is_some() {
                RuntimeHealth::Healthy
            } else {
                RuntimeHealth::Missing
            },
            message: None,
        });
    }
    records
}

pub fn which(binary: &str) -> Option<PathBuf> {
    let candidate = Path::new(binary);
    if candidate.is_absolute() && candidate.exists() {
        return Some(candidate.to_path_buf());
    }
    let paths = env::var_os("PATH")?;
    env::split_paths(&paths)
        .map(|p| p.join(binary))
        .find(|p| p.exists())
}

async fn command_version(binary: &str) -> anyhow::Result<String> {
    let output = Command::new(binary).arg("--version").output().await?;
    let text = if output.stdout.is_empty() {
        String::from_utf8_lossy(&output.stderr).to_string()
    } else {
        String::from_utf8_lossy(&output.stdout).to_string()
    };
    Ok(text.lines().next().unwrap_or("").trim().to_string())
}
