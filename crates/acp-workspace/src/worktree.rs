use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use tokio::process::Command;
use tracing::instrument;
use uuid::Uuid;

use crate::WorkspaceEngine;

#[derive(Debug, Clone)]
pub struct AgentWorkspace {
    pub agent_id: String,
    pub role: String,
    pub task_id: String,
    pub branch: String,
    pub path: PathBuf,
}

impl WorkspaceEngine {
    #[instrument(skip(self), fields(role = %role))]
    pub async fn create_agent_workspace(
        &self,
        role: &str,
        task_id: Option<&str>,
    ) -> anyhow::Result<AgentWorkspace> {
        tokio::fs::create_dir_all(&self.worktrees_root).await?;
        let task_id = task_id
            .map(ToString::to_string)
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let agent_id = format!("{}-{}", super::sanitize(role), &task_id[..task_id.len().min(8)]);
        let branch = format!(
            "acp/{}/{}",
            super::sanitize(role),
            super::sanitize(&task_id)
        );
        let path = self.worktrees_root.join(&agent_id);
        let output = Command::new("git")
            .current_dir(&self.repo_root)
            .arg("worktree")
            .arg("add")
            .arg("-B")
            .arg(&branch)
            .arg(&path)
            .output()
            .await
            .context("failed to run git worktree add")?;
        if !output.status.success() {
            bail!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(AgentWorkspace {
            agent_id,
            role: role.to_string(),
            task_id,
            branch,
            path,
        })
    }

    pub async fn remove_agent_workspace(&self, path: &Path) -> anyhow::Result<()> {
        let output = Command::new("git")
            .current_dir(&self.repo_root)
            .arg("worktree")
            .arg("remove")
            .arg("--force")
            .arg(path)
            .output()
            .await
            .context("failed to run git worktree remove")?;
        if !output.status.success() {
            bail!(
                "git worktree remove failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    pub async fn status(&self) -> anyhow::Result<String> {
        let output = Command::new("git")
            .current_dir(&self.repo_root)
            .arg("worktree")
            .arg("list")
            .output()
            .await?;
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}
