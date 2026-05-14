use acp_protocol::{FileClaimRecord, FileClaimRequest, FileClaimResponse};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{TimeDelta, Utc};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    authorize,
    db::{active_file_claims_for_path, row_to_file_claim, write_event},
    ApiError, HubState,
};

#[derive(Debug, Deserialize)]
pub(crate) struct FileClaimQuery {
    path: Option<String>,
}

pub(crate) async fn create_file_claim(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<FileClaimRequest>,
) -> Result<(StatusCode, Json<FileClaimResponse>), ApiError> {
    authorize(&state, &headers)?;
    let existing = active_file_claims_for_path(state.pool(), &req.file_path).await?;
    let warnings = existing
        .iter()
        .filter(|claim| claim.claimed_by != req.claimed_by)
        .map(|claim| {
            format!(
                "{} is already claimed by {} ({})",
                claim.file_path,
                claim.claimed_by,
                claim
                    .reason
                    .clone()
                    .unwrap_or_else(|| "no reason".to_string())
            )
        })
        .collect::<Vec<_>>();
    let now = Utc::now();
    let expires_at = req
        .ttl_seconds
        .and_then(|ttl| TimeDelta::try_seconds(ttl).map(|delta| now + delta));
    let claim = FileClaimRecord {
        id: Uuid::new_v4(),
        file_path: req.file_path,
        claimed_by: req.claimed_by,
        task_id: req.task_id,
        branch: req.branch,
        reason: req.reason,
        created_at: now,
        expires_at,
        stale: false,
    };
    sqlx::query("INSERT INTO file_claims (id, file_path, claimed_by, task_id, branch, reason, created_at, expires_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")
        .bind(claim.id.to_string())
        .bind(&claim.file_path)
        .bind(&claim.claimed_by)
        .bind(claim.task_id.map(|id| id.to_string()))
        .bind(&claim.branch)
        .bind(&claim.reason)
        .bind(claim.created_at.to_rfc3339())
        .bind(claim.expires_at.map(|dt| dt.to_rfc3339()))
        .execute(state.pool())
        .await?;
    write_event(
        state.pool(),
        &claim.claimed_by,
        "file_claimed",
        serde_json::to_value(&claim).unwrap_or_default(),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(FileClaimResponse { claim, warnings }),
    ))
}

pub(crate) async fn list_file_claims(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<FileClaimQuery>,
) -> Result<Json<Vec<FileClaimRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = if let Some(path) = query.path {
        sqlx::query("SELECT id, file_path, claimed_by, task_id, branch, reason, created_at, expires_at FROM file_claims WHERE file_path = ?1 ORDER BY created_at DESC")
            .bind(path)
            .fetch_all(state.pool())
            .await?
    } else {
        sqlx::query("SELECT id, file_path, claimed_by, task_id, branch, reason, created_at, expires_at FROM file_claims ORDER BY created_at DESC")
            .fetch_all(state.pool())
            .await?
    };
    rows.into_iter()
        .map(row_to_file_claim)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn delete_file_claim(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&state, &headers)?;
    sqlx::query("DELETE FROM file_claims WHERE id = ?1")
        .bind(id.to_string())
        .execute(state.pool())
        .await?;
    Ok(Json(serde_json::json!({ "deleted": id })))
}
