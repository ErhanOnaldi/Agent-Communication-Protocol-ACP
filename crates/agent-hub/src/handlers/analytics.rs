use axum::{
    extract::{Path, State},
    Json,
};
use uuid::Uuid;

use crate::{db::pipeline_analytics, ApiError, HubState};
use agent_protocol::PipelineAnalyticsResponse;

pub(crate) async fn get_pipeline_analytics(
    State(state): State<HubState>,
    Path(pipeline_id): Path<Uuid>,
) -> Result<Json<PipelineAnalyticsResponse>, ApiError> {
    let analytics = pipeline_analytics(&state.inner.pool, pipeline_id).await?;
    Ok(Json(analytics))
}
