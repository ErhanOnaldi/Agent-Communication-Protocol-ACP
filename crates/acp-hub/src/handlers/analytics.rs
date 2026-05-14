use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use uuid::Uuid;

use crate::{authorize, db::pipeline_analytics, ApiError, HubState};
use acp_protocol::PipelineAnalyticsResponse;

pub(crate) async fn get_pipeline_analytics(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(pipeline_id): Path<Uuid>,
) -> Result<Json<PipelineAnalyticsResponse>, ApiError> {
    authorize(&state, &headers)?;
    let analytics = pipeline_analytics(&state.inner.pool, pipeline_id).await?;
    Ok(Json(analytics))
}
