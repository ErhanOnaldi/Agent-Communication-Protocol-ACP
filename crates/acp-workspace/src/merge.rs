use anyhow::Context;
use tokio::process::Command;
use tracing::instrument;

use crate::WorkspaceEngine;

#[derive(Debug, Clone)]
pub struct MergeSimulation {
    pub clean: bool,
    pub stdout: String,
    pub stderr: String,
}

impl WorkspaceEngine {
    #[instrument(skip(self), fields(branch = %branch))]
    pub async fn simulate_merge(&self, branch: &str) -> anyhow::Result<MergeSimulation> {
        let output = Command::new("git")
            .current_dir(&self.repo_root)
            .arg("merge")
            .arg("--no-commit")
            .arg("--no-ff")
            .arg(branch)
            .output()
            .await
            .context("failed to run git merge simulation")?;
        let clean = output.status.success();
        // Always abort: on success this resets the fast-forward; on conflict it
        // clears the in-progress merge so the worktree is never left poisoned.
        let _ = Command::new("git")
            .current_dir(&self.repo_root)
            .arg("merge")
            .arg("--abort")
            .output()
            .await;
        Ok(MergeSimulation {
            clean,
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }
}
