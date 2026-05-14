use std::{env, path::PathBuf, time::Duration};

use acp_protocol::{AgentSpec, RuntimeHealth, RuntimeOutput, RuntimeStreamEvent, RuntimeType};
use anyhow::{bail, Context};
use async_trait::async_trait;
use tokio::{process::Command, time::timeout};

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn spawn(&self, spec: AgentSpec) -> anyhow::Result<RuntimeOutput>;
    async fn health(&self) -> RuntimeHealth;
}

#[derive(Debug, Clone)]
pub struct ProcessRuntimeAdapter {
    runtime_type: RuntimeType,
    binary: String,
    timeout: Duration,
    claudex: Option<ClaudexProvider>,
}

#[derive(Debug, Clone)]
pub struct ClaudexProvider {
    pub base_url: String,
    pub api_key: String,
    pub config_dir: PathBuf,
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

    fn command_for(&self, spec: &AgentSpec) -> anyhow::Result<Command> {
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

    async fn health(&self) -> RuntimeHealth {
        match Command::new(&self.binary).arg("--version").output().await {
            Ok(output) if output.status.success() => RuntimeHealth::Healthy,
            Ok(_) => RuntimeHealth::Degraded,
            Err(_) => RuntimeHealth::Missing,
        }
    }
}

pub fn parse_stream_json_events(stdout: &str) -> Vec<RuntimeStreamEvent> {
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .map(|payload| {
            let event_type = payload
                .get("type")
                .or_else(|| payload.get("event"))
                .and_then(|value| value.as_str())
                .unwrap_or("runtime_event")
                .to_string();
            RuntimeStreamEvent {
                event_type,
                payload,
            }
        })
        .collect()
}

pub fn adapter_for(runtime_type: RuntimeType) -> ProcessRuntimeAdapter {
    match runtime_type {
        RuntimeType::ClaudeCode => ProcessRuntimeAdapter::external(runtime_type, "claude"),
        RuntimeType::Codex => ProcessRuntimeAdapter::external(runtime_type, "codex"),
        RuntimeType::Gemini => ProcessRuntimeAdapter::external(runtime_type, "gemini"),
        RuntimeType::Copilot => ProcessRuntimeAdapter::external(runtime_type, "copilot"),
        RuntimeType::Claudex => ProcessRuntimeAdapter::claudex(ClaudexProvider::from_env()),
    }
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

pub fn classify_output(exit_code: Option<i32>, stdout: &str, stderr: &str) -> RuntimeHealth {
    let combined = format!("{stdout}\n{stderr}").to_lowercase();
    if combined.contains("rate limit")
        || combined.contains("rate_limit")
        || combined.contains("too many requests")
        || combined.contains("429")
    {
        RuntimeHealth::RateLimited
    } else if combined.contains("auth")
        || combined.contains("unauthorized")
        || combined.contains("invalid api key")
        || combined.contains("401")
    {
        RuntimeHealth::AuthExpired
    } else if exit_code.is_some_and(|code| code != 0) {
        RuntimeHealth::Crashed
    } else {
        RuntimeHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_rate_limits_before_crashes() {
        assert_eq!(
            classify_output(Some(1), "", "429 too many requests: rate limit"),
            RuntimeHealth::RateLimited
        );
    }

    #[test]
    fn classifies_auth_failures() {
        assert_eq!(
            classify_output(Some(1), "", "401 unauthorized invalid api key"),
            RuntimeHealth::AuthExpired
        );
    }

    #[test]
    fn parses_line_delimited_stream_json() {
        let events = parse_stream_json_events(
            r#"{"type":"assistant","message":"hi"}
not json
{"event":"tool_result","ok":true}"#,
        );
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "assistant");
        assert_eq!(events[1].event_type, "tool_result");
    }

    #[test]
    fn claudex_adapter_uses_claudex_mode() {
        let adapter = adapter_for(RuntimeType::Claudex);
        assert_eq!(adapter.runtime_type, RuntimeType::Claudex);
        assert!(adapter.claudex.is_some());
    }
}
