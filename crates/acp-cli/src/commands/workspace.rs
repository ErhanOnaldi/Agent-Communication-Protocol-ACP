use std::path::PathBuf;

use acp_workspace::WorkspaceEngine;

pub async fn handle_workspace_status(repo: PathBuf) -> anyhow::Result<()> {
    let status = WorkspaceEngine::new(repo).status().await?;
    print!("{status}");
    Ok(())
}
