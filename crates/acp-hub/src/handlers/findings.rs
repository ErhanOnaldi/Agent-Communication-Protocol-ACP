use acp_protocol::{FindingCreateRequest, FindingRecord};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    authorize,
    db::{query_findings, row_to_finding, write_event},
    ApiError, HubState,
};

#[derive(Debug, Deserialize)]
pub(crate) struct FindingSearchQuery {
    q: String,
}

pub(crate) async fn create_finding(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<FindingCreateRequest>,
) -> Result<(StatusCode, Json<FindingRecord>), ApiError> {
    authorize(&state, &headers)?;
    let finding = FindingRecord {
        id: Uuid::new_v4(),
        agent_id: req.agent_id,
        kind: req.kind,
        title: req.title,
        body: req.body,
        files: req.files,
        confidence: req.confidence,
        created_at: Utc::now(),
    };
    sqlx::query("INSERT INTO findings (id, agent_id, kind, title, body, files_json, confidence, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")
        .bind(finding.id.to_string())
        .bind(&finding.agent_id)
        .bind(finding.kind.to_string())
        .bind(&finding.title)
        .bind(&finding.body)
        .bind(serde_json::to_string(&finding.files).unwrap_or_else(|_| "[]".to_string()))
        .bind(finding.confidence.to_string())
        .bind(finding.created_at.to_rfc3339())
        .execute(state.pool())
        .await?;
    write_event(
        state.pool(),
        &finding.agent_id,
        "finding_published",
        serde_json::to_value(&finding).unwrap_or_default(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(finding)))
}

pub(crate) async fn list_findings(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<FindingRecord>>, ApiError> {
    authorize(&state, &headers)?;
    query_findings(state.pool(), None).await.map(Json)
}

pub(crate) async fn search_findings(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<FindingSearchQuery>,
) -> Result<Json<Vec<FindingRecord>>, ApiError> {
    authorize(&state, &headers)?;
    query_findings(state.pool(), Some(&query.q)).await.map(Json)
}

pub(crate) async fn get_finding(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<FindingRecord>, ApiError> {
    authorize(&state, &headers)?;
    let row = sqlx::query("SELECT id, agent_id, kind, title, body, files_json, confidence, created_at FROM findings WHERE id = ?1")
        .bind(id.to_string())
        .fetch_optional(state.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("finding not found"))?;
    row_to_finding(row).map(Json)
}
