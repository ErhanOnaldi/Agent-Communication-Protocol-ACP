pub mod claudex;

use std::time::Duration;

use acp_protocol::{AgentSpec, RuntimeHealth, RuntimeOutput, RuntimeType};
use anyhow::{bail, Context};
use async_trait::async_trait;
use tokio::{process::Command, time::timeout};
use tracing::instrument;

pub use claudex::ClaudexProvider;

use crate::output::{classify_output, parse_stream_json_events};

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn spawn(&self, spec: AgentSpec) -> anyhow::Result<RuntimeOutput>;
    async fn health(&self) -> RuntimeHealth;
}

#[derive(Debug, Clone)]
pub struct ProcessRuntimeAdapter {
    pub(crate) runtime_type: RuntimeType,
    binary: String,
    timeout: Duration,
    pub(crate) claudex: Option<ClaudexProvider>,
}

impl ProcessRuntimeAdapter {
    pub fn external(runtime_type: RuntimeType, binary: impl Into<String>) -> Self {
        Self {
            runtime_type,
            binary: binary.into(),
            timeout: Duration::from_secs(60 * 30),
            claudex: None,
        }
    }

    pub fn claudex(provider: ClaudexProvider) -> Self {
        Self {
            runtime_type: RuntimeType::Claudex,
            binary: "claude".to_string(),
            timeout: Duration::from_secs(60 * 30),
            claudex: Some(provider),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub(crate) fn command_for(&self, spec: &AgentSpec) -> anyhow::Result<Command> {
        let mut command = Command::new(&self.binary);
        match self.runtime_type {
            RuntimeType::ClaudeCode => {
                command.arg("-p").arg(&spec.task);
                command.arg("--output-format").arg("stream-json");
                command.arg("--bare");
            }
            RuntimeType::Codex => {
                command.arg("exec").arg(&spec.task);
            }
            RuntimeType::Gemini => {
                command.arg("-p").arg(&spec.task);
            }
            RuntimeType::Copilot => {
                command.arg("-p").arg(&spec.task).arg("--no-ask-user");
            }
            RuntimeType::Claudex => {
                let Some(provider) = &self.claudex else {
                    bail!("claudex provider is required");
                };
                command
                    .env("ANTHROPIC_BASE_URL", &provider.base_url)
                    .env("ANTHROPIC_AUTH_TOKEN", &provider.api_key)
                    .env("CLAUDE_CONFIG_DIR", &provider.config_dir)
                    .arg("-p")
                    .arg(&spec.task)
                    .arg("--output-format")
                    .arg("stream-json")
                    .arg("--bare");
                if let Some(model) = &spec.model {
                    command.env("ANTHROPIC_MODEL", model);
                }
                if !spec.allowed_tools.is_empty() {
                    command
                        .arg("--allowedTools")
                        .arg(spec.allowed_tools.join(","));
                }
            }
        }
        if let Some(workspace) = &spec.workspace {
            command.current_dir(workspace);
        }
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        Ok(command)
    }
}

#[async_trait]
impl RuntimeAdapter for ProcessRuntimeAdapter {
    #[instrument(skip(self, spec), fields(runtime = %self.runtime_type, role = %spec.role))]
    async fn spawn(&self, spec: AgentSpec) -> anyhow::Result<RuntimeOutput> {
        let mut command = self.command_for(&spec)?;
        let output = timeout(self.timeout, command.output())
            .await
            .context("runtime timed out")?
            .with_context(|| format!("failed to spawn {}", self.binary))?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let status = classify_output(output.status.code(), &stdout, &stderr);
        let stream_events = parse_stream_json_events(&stdout);
        Ok(RuntimeOutput {
            status,
            exit_code: output.status.code(),
            stdout,
            stderr,
            stream_events,
        })
    }

    #[instrument(skip(self), fields(binary = %self.binary))]
    async fn health(&self) -> RuntimeHealth {
        match Command::new(&self.binary).arg("--version").output().await {
            Ok(output) if output.status.success() => RuntimeHealth::Healthy,
            Ok(_) => RuntimeHealth::Degraded,
            Err(_) => RuntimeHealth::Missing,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter_for;

    #[test]
    fn claudex_adapter_uses_claudex_mode() {
        let adapter = adapter_for(RuntimeType::Claudex);
        assert_eq!(adapter.runtime_type, RuntimeType::Claudex);
        assert!(adapter.claudex.is_some());
    }
}
