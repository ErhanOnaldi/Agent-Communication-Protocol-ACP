use std::{env, net::SocketAddr, str::FromStr};

use agent_hub::{app, init_db, HubState};
use anyhow::Context;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let bind = env::var("AGENT_HUB_BIND").unwrap_or_else(|_| "0.0.0.0:8787".to_string());
    let database_url =
        env::var("AGENT_HUB_DATABASE_URL").unwrap_or_else(|_| "sqlite://agent-hub.db".to_string());
    let token = env::var("AGENT_TOKEN").context("AGENT_TOKEN must be set")?;

    let connect_options = SqliteConnectOptions::from_str(&database_url)
        .with_context(|| format!("invalid sqlite URL: {database_url}"))?
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(connect_options)
        .await
        .with_context(|| format!("failed to connect to {database_url}"))?;
    init_db(&pool).await?;

    let state = HubState::new(pool, token);
    let app = app(state);
    let addr: SocketAddr = bind.parse().context("invalid AGENT_HUB_BIND")?;
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("agent-hub listening on http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
