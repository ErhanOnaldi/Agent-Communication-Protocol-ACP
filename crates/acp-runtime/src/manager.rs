use std::{collections::BTreeMap, process::Stdio, sync::Arc, time::Duration};

use acp_protocol::{
    AgentHandle, AgentSpec, RuntimeCommandResponse, RuntimeLifecycleStatus, RuntimeOutput,
    TaskHandle,
};
use anyhow::Context;
use chrono::Utc;
use tokio::{process::Child, sync::Mutex, time::sleep};
use uuid::Uuid;

use crate::{output::classify_output, ProcessRuntimeAdapter};

#[derive(Clone, Default)]
pub struct RuntimeManager {
    children: Arc<Mutex<BTreeMap<String, ManagedChild>>>,
}

struct ManagedChild {
    handle: AgentHandle,
    child: Child,
}

impl RuntimeManager {
    pub async fn spawn_handle(
        &self,
        adapter: &ProcessRuntimeAdapter,
        spec: AgentSpec,
    ) -> anyhow::Result<AgentHandle> {
        let mut command = adapter.command_for(&spec)?;
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = command
            .spawn()
            .with_context(|| format!("failed to spawn supervised agent {}", spec.agent_id))?;
        let handle = AgentHandle {
            agent_id: spec.agent_id.clone(),
            pid: child.id(),
            runtime_type: spec.runtime_type,
            started_at: Utc::now(),
            status: RuntimeLifecycleStatus::Running,
        };
        self.children.lock().await.insert(
            spec.agent_id,
            ManagedChild {
                handle: handle.clone(),
                child,
            },
        );
        Ok(handle)
    }

    pub async fn send_task(
        &self,
        adapter: &ProcessRuntimeAdapter,
        spec: AgentSpec,
    ) -> anyhow::Result<TaskHandle> {
        let handle = self.spawn_handle(adapter, spec).await?;
        Ok(TaskHandle {
            task_id: Uuid::new_v4(),
            agent_id: handle.agent_id,
            pid: handle.pid,
            runtime_type: handle.runtime_type,
            started_at: handle.started_at,
            status: handle.status,
        })
    }

    pub async fn interrupt(&self, agent_id: &str) -> anyhow::Result<RuntimeCommandResponse> {
        self.signal_then_kill(agent_id, RuntimeLifecycleStatus::Interrupted, true)
            .await
    }

    pub async fn shutdown(&self, agent_id: &str) -> anyhow::Result<RuntimeCommandResponse> {
        self.signal_then_kill(agent_id, RuntimeLifecycleStatus::Shutdown, false)
            .await
    }

    pub async fn wait_output(&self, agent_id: &str) -> anyhow::Result<RuntimeOutput> {
        let child = self
            .children
            .lock()
            .await
            .remove(agent_id)
            .ok_or_else(|| anyhow::anyhow!("agent {agent_id} is not supervised"))?
            .child;
        let output = child
            .wait_with_output()
            .await
            .with_context(|| format!("failed waiting for {agent_id}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let stream_events = crate::parse_stream_json_events(&stdout);
        Ok(RuntimeOutput {
            status: classify_output(output.status.code(), &stdout, &stderr),
            exit_code: output.status.code(),
            stdout,
            stderr,
            stream_events,
        })
    }

    async fn signal_then_kill(
        &self,
        agent_id: &str,
        status: RuntimeLifecycleStatus,
        interrupt: bool,
    ) -> anyhow::Result<RuntimeCommandResponse> {
        let mut children = self.children.lock().await;
        let Some(managed) = children.get_mut(agent_id) else {
            return Ok(RuntimeCommandResponse {
                agent_id: agent_id.to_string(),
                status,
                message: "agent is not currently supervised".to_string(),
            });
        };
        if let Some(pid) = managed.handle.pid {
            send_signal(pid, interrupt).await;
        }
        sleep(Duration::from_millis(100)).await;
        if let Ok(None) = managed.child.try_wait() {
            let _ = managed.child.start_kill();
        }
        managed.handle.status = status;
        Ok(RuntimeCommandResponse {
            agent_id: agent_id.to_string(),
            status,
            message: "runtime lifecycle command applied".to_string(),
        })
    }
}

async fn send_signal(pid: u32, interrupt: bool) {
    #[cfg(unix)]
    {
        let signal = if interrupt { "-INT" } else { "-TERM" };
        let _ = tokio::process::Command::new("kill")
            .arg(signal)
            .arg(pid.to_string())
            .status()
            .await;
    }
    #[cfg(windows)]
    {
        let _ = tokio::process::Command::new("taskkill")
            .arg("/PID")
            .arg(pid.to_string())
            .arg(if interrupt { "/T" } else { "/F" })
            .status()
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use acp_protocol::RuntimeType;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn supervised_runtime_can_be_interrupted() {
        let dir = tempdir().unwrap();
        let bin = dir.path().join("fake-runtime");
        fs::write(&bin, "#!/bin/sh\nsleep 30\n").unwrap();
        fs::set_permissions(&bin, fs::Permissions::from_mode(0o755)).unwrap();
        let adapter =
            ProcessRuntimeAdapter::external(RuntimeType::Codex, bin.display().to_string());
        let manager = RuntimeManager::default();
        let handle = manager
            .spawn_handle(
                &adapter,
                AgentSpec {
                    agent_id: "agent-1".to_string(),
                    role: "tester".to_string(),
                    runtime_type: RuntimeType::Codex,
                    model: None,
                    task: "noop".to_string(),
                    workspace: None,
                    allowed_tools: Vec::new(),
                    env: Default::default(),
                    mcp_servers: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert!(handle.pid.is_some());
        let response = manager.interrupt("agent-1").await.unwrap();
        assert_eq!(response.status, RuntimeLifecycleStatus::Interrupted);
    }
}
