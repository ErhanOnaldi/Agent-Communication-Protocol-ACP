use acp_protocol::RuntimeHealth;
use agent_client::AgentClient;
use uuid::Uuid;

use crate::client::print_yaml;
use crate::Cli;

#[derive(Debug, Clone, clap::Subcommand)]
pub enum AnalyticsCommand {
    /// Print per-step timing and health summary for a pipeline.
    Pipeline {
        /// Pipeline UUID
        id: Uuid,
    },
    Scheduler {
        /// Pipeline UUID
        id: Uuid,
    },
}

pub async fn handle_analytics(command: AnalyticsCommand, cli: &Cli) -> anyhow::Result<()> {
    let client = crate::client::client(cli)?;
    match command {
        AnalyticsCommand::Pipeline { id } => print_pipeline_analytics(&client, id, cli.json).await,
        AnalyticsCommand::Scheduler { id } => {
            print_scheduler_analytics(&client, id, cli.json).await
        }
    }
}

async fn print_scheduler_analytics(
    client: &AgentClient,
    id: Uuid,
    json: bool,
) -> anyhow::Result<()> {
    let decisions = client.scheduler_decisions(id).await?;
    if json {
        print_yaml(json, &decisions)?;
        return Ok(());
    }
    println!("Scheduler decisions for {id}");
    if decisions.is_empty() {
        println!("No scheduler decisions recorded yet.");
        return Ok(());
    }
    println!(
        "{:<20} {:<14} {:<28} {:<8} REASON",
        "ROLE", "RUNTIME", "MODEL", "SCORE"
    );
    println!("{}", "─".repeat(96));
    for decision in decisions {
        println!(
            "{:<20} {:<14} {:<28} {:<8.3} {}",
            truncate(&decision.role, 19),
            decision.runtime_type,
            truncate(decision.model_id.as_deref().unwrap_or("default"), 27),
            decision.final_score,
            decision.reason
        );
    }
    Ok(())
}

async fn print_pipeline_analytics(
    client: &AgentClient,
    id: Uuid,
    json: bool,
) -> anyhow::Result<()> {
    let analytics = client.pipeline_analytics(id).await?;
    if json {
        print_yaml(json, &analytics)?;
        return Ok(());
    }

    println!("Pipeline analytics for {id}");
    println!(
        "Steps: {}  |  Healthy: {}  |  Failed: {}  |  P50: {}  |  P95: {}",
        analytics.total_steps,
        analytics.succeeded,
        analytics.failed,
        analytics
            .p50_latency_ms
            .map(|v| format!("{v}ms"))
            .unwrap_or_else(|| "–".to_string()),
        analytics
            .p95_latency_ms
            .map(|v| format!("{v}ms"))
            .unwrap_or_else(|| "–".to_string()),
    );
    println!();

    if analytics.steps.is_empty() {
        println!("No step metrics recorded yet.");
        return Ok(());
    }

    println!(
        "{:<30} {:<20} {:<14} {:<12} LATENCY",
        "STEP", "ROLE", "RUNTIME", "HEALTH"
    );
    println!("{}", "─".repeat(90));

    for step in &analytics.steps {
        let health_str = if step.health == RuntimeHealth::Healthy {
            "healthy".to_string()
        } else {
            step.health.to_string()
        };
        let latency_str = step
            .latency_ms
            .map(|v| format!("{v}ms"))
            .unwrap_or_else(|| "–".to_string());
        let runtime_str = step
            .runtime_type
            .map(|r| r.to_string())
            .unwrap_or_else(|| "–".to_string());
        println!(
            "{:<30} {:<20} {:<14} {:<12} {}",
            truncate(&step.step_name, 29),
            truncate(&step.role, 19),
            truncate(&runtime_str, 13),
            truncate(&health_str, 11),
            latency_str,
        );
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}
