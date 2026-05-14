use acp_protocol::{McpHealth, McpServerRecord, RuntimeHealth};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::Utc;

use crate::{
    authorize,
    db::{list_mcp_servers, mcp_health, upsert_mcp_health, upsert_mcp_server},
    ApiError, HubState,
};

pub(crate) async fn list_mcp(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<McpServerRecord>>, ApiError> {
    authorize(&state, &headers)?;
    list_mcp_servers(state.pool()).await.map(Json)
}

pub(crate) async fn upsert_mcp(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(server): Json<McpServerRecord>,
) -> Result<(StatusCode, Json<McpServerRecord>), ApiError> {
    authorize(&state, &headers)?;
    let server = upsert_mcp_server(state.pool(), &server).await?;
    let health = McpHealth {
        name: server.name.clone(),
        status: RuntimeHealth::Healthy,
        pid: None,
        message: Some("configured".to_string()),
        checked_at: Utc::now(),
    };
    let _ = upsert_mcp_health(state.pool(), &health).await;
    Ok((StatusCode::CREATED, Json(server)))
}

pub(crate) async fn get_mcp_health(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<McpHealth>, ApiError> {
    authorize(&state, &headers)?;
    match mcp_health(state.pool(), &name).await {
        Ok(health) => Ok(Json(health)),
        Err(_) => Ok(Json(McpHealth {
            name,
            status: RuntimeHealth::Missing,
            pid: None,
            message: Some("no health record".to_string()),
            checked_at: Utc::now(),
        })),
    }
}
