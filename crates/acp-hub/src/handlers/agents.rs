use acp_protocol::{AgentRecord, AgentStatus, HeartbeatRequest, UpdateAgentStatusRequest};
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use chrono::Utc;

use crate::{
    authorize,
    db::{get_agent_by_id, row_to_agent, write_event},
    ApiError, HubState,
};

pub(crate) async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

pub(crate) async fn heartbeat(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<HeartbeatRequest>,
) -> Result<Json<AgentRecord>, ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    let status = req.status.unwrap_or(AgentStatus::Online);
    sqlx::query(
        r#"
        INSERT INTO agents (id, role, hostname, status, current_task, branch, last_seen_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(id) DO UPDATE SET
            role = excluded.role,
            hostname = excluded.hostname,
            status = excluded.status,
            current_task = excluded.current_task,
            branch = excluded.branch,
            last_seen_at = excluded.last_seen_at
        "#,
    )
    .bind(&req.agent_id)
    .bind(&req.role)
    .bind(&req.hostname)
    .bind(status.to_string())
    .bind(&req.current_task)
    .bind(&req.branch)
    .bind(now.to_rfc3339())
    .execute(state.pool())
    .await?;
    write_event(
        state.pool(),
        &req.agent_id,
        "agent_registered",
        serde_json::json!({ "role": req.role, "status": status }),
    )
    .await?;
    get_agent_by_id(state.pool(), &req.agent_id).await.map(Json)
}

pub(crate) async fn list_agents(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<AgentRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query(
        "SELECT id, role, hostname, status, current_task, branch, last_seen_at FROM agents ORDER BY id",
    )
    .fetch_all(state.pool())
    .await?;
    rows.into_iter()
        .map(row_to_agent)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn get_agent(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
) -> Result<Json<AgentRecord>, ApiError> {
    authorize(&state, &headers)?;
    get_agent_by_id(state.pool(), &agent_id).await.map(Json)
}

pub(crate) async fn update_agent_status(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
    Json(req): Json<UpdateAgentStatusRequest>,
) -> Result<Json<AgentRecord>, ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    sqlx::query(
        "UPDATE agents SET status = ?1, current_task = ?2, branch = ?3, last_seen_at = ?4 WHERE id = ?5",
    )
    .bind(req.status.to_string())
    .bind(req.current_task)
    .bind(req.branch)
    .bind(now.to_rfc3339())
    .bind(&agent_id)
    .execute(state.pool())
    .await?;
    write_event(
        state.pool(),
        &agent_id,
        "agent_status_changed",
        serde_json::json!({ "status": req.status }),
    )
    .await?;
    get_agent_by_id(state.pool(), &agent_id).await.map(Json)
}
