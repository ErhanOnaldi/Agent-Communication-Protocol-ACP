use std::{collections::BTreeMap, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use acp_protocol::RuntimeHealth;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, process::Child, process::Command, sync::Mutex};

use crate::DiscoveryConfig;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: BTreeMap<String, McpServerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
    #[serde(default)]
    pub mode: McpServerMode,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub auto_start: bool,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpServerMode {
    /// One shared read-only server can serve all agents.
    #[default]
    Shared,
    /// Mutable servers are started per agent/workspace for isolation.
    Isolated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpLaunch {
    pub name: String,
    pub mode: McpServerMode,
    pub pid: Option<u32>,
    pub isolated_agent_id: Option<String>,
    pub health: RuntimeHealth,
    pub message: Option<String>,
}

#[derive(Clone, Default)]
pub struct McpManager {
    shared: Arc<Mutex<BTreeMap<String, McpProcess>>>,
}

struct McpProcess {
    pid: Option<u32>,
    child: Child,
    health: RuntimeHealth,
}

pub fn load_mcp_config(config: &DiscoveryConfig) -> anyhow::Result<McpConfig> {
    let path = mcp_config_path(config);
    if !path.exists() {
        return Ok(McpConfig::default());
    }
    let data = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&data).with_context(|| format!("invalid MCP config {}", path.display()))
}

pub fn mcp_config_path(config: &DiscoveryConfig) -> PathBuf {
    config.acp_home.join("mcp.json")
}

impl McpManager {
    pub async fn start(
        &self,
        name: &str,
        server: &McpServerConfig,
        agent_id: Option<&str>,
    ) -> anyhow::Result<McpLaunch> {
        if server.mode == McpServerMode::Shared {
            if let Some(process) = self.shared.lock().await.get(name) {
                return Ok(McpLaunch {
                    name: name.to_string(),
                    mode: server.mode,
                    pid: process.pid,
                    isolated_agent_id: None,
                    health: process.health,
                    message: Some("reused shared MCP server".to_string()),
                });
            }
        }

        let mut command = Command::new(&server.command);
        command
            .args(&server.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in &server.env {
            command.env(key, value);
        }
        if let Some(working_dir) = &server.working_dir {
            command.current_dir(working_dir);
        }
        if server.mode == McpServerMode::Isolated {
            let agent = agent_id.context("isolated MCP server requires an agent id")?;
            command.env("ACP_AGENT_ID", agent);
        }
        if let Some(agent) = agent_id {
            command.env("ACP_AGENT_ID", agent);
        }

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to start MCP server {name}"))?;
        let pid = child.id();
        let init_result = initialize_stdio(name, &mut child, server.timeout_ms).await;
        let (health, message) = match init_result {
            Ok(message) => (RuntimeHealth::Healthy, Some(message)),
            Err(err) => (RuntimeHealth::Degraded, Some(err.to_string())),
        };
        if server.mode == McpServerMode::Shared {
            self.shared
                .lock()
                .await
                .insert(name.to_string(), McpProcess { pid, child, health });
        } else {
            let _ = shutdown_stdio(name, &mut child).await;
        }
        Ok(McpLaunch {
            name: name.to_string(),
            mode: server.mode,
            pid,
            isolated_agent_id: agent_id.map(ToString::to_string),
            health,
            message,
        })
    }

    pub async fn shared_pids(&self) -> BTreeMap<String, u32> {
        self.shared
            .lock()
            .await
            .iter()
            .filter_map(|(name, process)| process.pid.map(|pid| (name.clone(), pid)))
            .collect()
    }

    pub async fn shutdown(&self, name: &str) -> anyhow::Result<McpLaunch> {
        let mut process = self
            .shared
            .lock()
            .await
            .remove(name)
            .ok_or_else(|| anyhow::anyhow!("shared MCP server {name} is not running"))?;
        let _ = shutdown_stdio(name, &mut process.child).await;
        let _ = process.child.start_kill();
        Ok(McpLaunch {
            name: name.to_string(),
            mode: McpServerMode::Shared,
            pid: process.pid,
            isolated_agent_id: None,
            health: RuntimeHealth::Healthy,
            message: Some("shutdown requested".to_string()),
        })
    }
}

async fn initialize_stdio(
    name: &str,
    child: &mut Child,
    timeout_ms: Option<u64>,
) -> anyhow::Result<String> {
    let timeout = Duration::from_millis(timeout_ms.unwrap_or(3_000));
    let Some(stdin) = child.stdin.as_mut() else {
        return Ok("MCP server has no stdin; skipped initialize".to_string());
    };
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "clientInfo": { "name": "acp", "version": env!("CARGO_PKG_VERSION") },
            "capabilities": {}
        }
    });
    tokio::time::timeout(timeout, async {
        stdin.write_all(request.to_string().as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await
    })
    .await
    .with_context(|| format!("MCP initialize timed out for {name}"))??;
    Ok("initialize request sent".to_string())
}

async fn shutdown_stdio(name: &str, child: &mut Child) -> anyhow::Result<()> {
    let Some(stdin) = child.stdin.as_mut() else {
        return Ok(());
    };
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "shutdown",
        "params": {}
    });
    stdin
        .write_all(request.to_string().as_bytes())
        .await
        .with_context(|| format!("failed to send MCP shutdown to {name}"))?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_mcp_config_loads_empty_registry() {
        let dir = tempfile::tempdir().unwrap();
        let config = DiscoveryConfig {
            acp_home: dir.path().to_path_buf(),
        };
        let loaded = load_mcp_config(&config).unwrap();
        assert!(loaded.servers.is_empty());
    }

    #[test]
    fn mcp_config_parses_shared_and_isolated_servers() {
        let dir = tempfile::tempdir().unwrap();
        let config = DiscoveryConfig {
            acp_home: dir.path().to_path_buf(),
        };
        std::fs::write(
            mcp_config_path(&config),
            r#"{
              "servers": {
                "docs": { "command": "docs-mcp", "mode": "shared", "auto_start": true },
                "fs": { "command": "fs-mcp", "args": ["."], "mode": "isolated" }
              }
            }"#,
        )
        .unwrap();

        let loaded = load_mcp_config(&config).unwrap();
        assert_eq!(loaded.servers["docs"].mode, McpServerMode::Shared);
        assert_eq!(loaded.servers["fs"].mode, McpServerMode::Isolated);
        assert_eq!(loaded.servers["fs"].args, vec!["."]);
    }
}
