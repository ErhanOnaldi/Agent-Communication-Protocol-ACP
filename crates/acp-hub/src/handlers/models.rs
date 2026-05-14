use acp_protocol::{CapabilityScoreRecord, CapabilityScoreUpdateRequest, ModelRecord};
use axum::{extract::State, http::HeaderMap, Json};
use chrono::Utc;

use crate::{
    authorize,
    db::{row_to_capability_score, row_to_model},
    ApiError, HubState,
};

pub(crate) async fn list_models(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ModelRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query(
        "SELECT id, name, runtime_source, tier, context_window, pricing_input, pricing_output FROM models ORDER BY runtime_source, name",
    )
    .fetch_all(state.pool())
    .await?;
    rows.into_iter()
        .map(row_to_model)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn list_capability_scores(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<CapabilityScoreRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query("SELECT runtime_type, model_id, capability, success_count, failure_count, last_updated_at FROM capability_scores ORDER BY capability, runtime_type, model_id")
        .fetch_all(state.pool())
        .await?;
    rows.into_iter()
        .map(row_to_capability_score)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn update_capability_score(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<CapabilityScoreUpdateRequest>,
) -> Result<Json<CapabilityScoreRecord>, ApiError> {
    authorize(&state, &headers)?;
    sqlx::query(
        r#"
        INSERT INTO capability_scores (runtime_type, model_id, capability, success_count, failure_count, last_updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(runtime_type, model_id, capability) DO UPDATE SET
            success_count = success_count + ?4,
            failure_count = failure_count + ?5,
            last_updated_at = ?6
        "#,
    )
    .bind(req.runtime_type.to_string())
    .bind(&req.model_id)
    .bind(&req.capability)
    .bind(if req.success { 1 } else { 0 })
    .bind(if req.success { 0 } else { 1 })
    .bind(Utc::now().to_rfc3339())
    .execute(state.pool())
    .await?;
    let row = sqlx::query("SELECT runtime_type, model_id, capability, success_count, failure_count, last_updated_at FROM capability_scores WHERE runtime_type = ?1 AND model_id = ?2 AND capability = ?3")
        .bind(req.runtime_type.to_string())
        .bind(req.model_id)
        .bind(req.capability)
        .fetch_one(state.pool())
        .await?;
    row_to_capability_score(row).map(Json)
}
