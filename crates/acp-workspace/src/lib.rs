use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use tokio::process::Command;
use tracing::instrument;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WorkspaceEngine {
    repo_root: PathBuf,
    worktrees_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct AgentWorkspace {
    pub agent_id: String,
    pub role: String,
    pub task_id: String,
    pub branch: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct MergeSimulation {
    pub clean: bool,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone)]
pub struct ValidationConfig {
    pub commands: Vec<Vec<String>>,
    pub merge_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ValidationStep {
    pub command: Vec<String>,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub steps: Vec<ValidationStep>,
    pub merge: Option<MergeSimulation>,
}

#[derive(Debug, Clone)]
pub struct WorkspaceSnapshot {
    pub branch: String,
    pub commit: String,
}

impl WorkspaceEngine {
    pub fn new(repo_root: impl Into<PathBuf>) -> Self {
        let repo_root = repo_root.into();
        let worktrees_root = repo_root.join(".acp").join("worktrees");
        Self {
            repo_root,
            worktrees_root,
        }
    }

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
        let agent_id = format!("{}-{}", sanitize(role), &task_id[..task_id.len().min(8)]);
        let branch = format!("acp/{}/{}", sanitize(role), sanitize(&task_id));
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

    #[instrument(skip(self, config))]
    pub async fn validate(&self, config: ValidationConfig) -> anyhow::Result<ValidationReport> {
        let mut steps = Vec::new();
        for command in config.commands {
            let Some((program, args)) = command.split_first() else {
                continue;
            };
            let output = Command::new(program)
                .current_dir(&self.repo_root)
                .args(args)
                .output()
                .await
                .with_context(|| format!("failed to run validation command: {command:?}"))?;
            let success = output.status.success();
            steps.push(ValidationStep {
                command,
                success,
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            });
            if !success {
                return Ok(ValidationReport { steps, merge: None });
            }
        }
        let merge = if let Some(branch) = config.merge_branch {
            Some(self.simulate_merge(&branch).await?)
        } else {
            None
        };
        Ok(ValidationReport { steps, merge })
    }

    pub async fn snapshot(&self, name: &str) -> anyhow::Result<WorkspaceSnapshot> {
        let commit = git_stdout(&self.repo_root, ["rev-parse", "HEAD"]).await?;
        let branch = format!("acp/snapshot/{}", sanitize(name));
        git_stdout(&self.repo_root, ["branch", "-f", &branch, commit.trim()]).await?;
        Ok(WorkspaceSnapshot {
            branch,
            commit: commit.trim().to_string(),
        })
    }

    pub async fn rollback(&self, snapshot: &WorkspaceSnapshot) -> anyhow::Result<()> {
        git_stdout(&self.repo_root, ["reset", "--hard", &snapshot.commit]).await?;
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

async fn git_stdout<const N: usize>(repo_root: &Path, args: [&str; N]) -> anyhow::Result<String> {
    let output = Command::new("git")
        .current_dir(repo_root)
        .args(args)
        .output()
        .await
        .context("failed to run git")?;
    if !output.status.success() {
        bail!("git failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn validation_runs_commands_until_failure() {
        let temp = tempfile::tempdir().unwrap();
        let engine = WorkspaceEngine::new(temp.path());
        let report = engine
            .validate(ValidationConfig {
                commands: vec![
                    vec!["sh".to_string(), "-c".to_string(), "printf ok".to_string()],
                    vec!["sh".to_string(), "-c".to_string(), "exit 7".to_string()],
                    vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
                ],
                merge_branch: None,
            })
            .await
            .unwrap();
        assert_eq!(report.steps.len(), 2);
        assert!(report.steps[0].success);
        assert!(!report.steps[1].success);
    }
}
