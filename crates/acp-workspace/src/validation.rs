use anyhow::Context;
use tokio::process::Command;
use tracing::instrument;

use crate::{merge::MergeSimulation, WorkspaceEngine};

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

impl WorkspaceEngine {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkspaceEngine;

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
