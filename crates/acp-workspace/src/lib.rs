use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use tokio::process::Command;

pub mod merge;
pub mod snapshot;
pub mod validation;
pub mod worktree;

pub use merge::MergeSimulation;
pub use snapshot::WorkspaceSnapshot;
pub use validation::{ValidationConfig, ValidationReport, ValidationStep};
pub use worktree::AgentWorkspace;

#[derive(Debug, Clone)]
pub struct WorkspaceEngine {
    pub(crate) repo_root: PathBuf,
    pub(crate) worktrees_root: PathBuf,
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
}

pub(crate) async fn git_stdout<const N: usize>(
    repo_root: &Path,
    args: [&str; N],
) -> anyhow::Result<String> {
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

pub(crate) fn sanitize(value: &str) -> String {
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
