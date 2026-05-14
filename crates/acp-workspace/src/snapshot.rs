use crate::WorkspaceEngine;

#[derive(Debug, Clone)]
pub struct WorkspaceSnapshot {
    pub branch: String,
    pub commit: String,
}

impl WorkspaceEngine {
    pub async fn snapshot(&self, name: &str) -> anyhow::Result<WorkspaceSnapshot> {
        let commit = super::git_stdout(&self.repo_root, ["rev-parse", "HEAD"]).await?;
        let branch = format!("acp/snapshot/{}", super::sanitize(name));
        super::git_stdout(&self.repo_root, ["branch", "-f", &branch, commit.trim()]).await?;
        Ok(WorkspaceSnapshot {
            branch,
            commit: commit.trim().to_string(),
        })
    }

    pub async fn rollback(&self, snapshot: &WorkspaceSnapshot) -> anyhow::Result<()> {
        super::git_stdout(&self.repo_root, ["reset", "--hard", &snapshot.commit]).await?;
        Ok(())
    }
}
