use acp_protocol::{RuntimeCommandResponse, RuntimeLifecycleStatus};
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};

use crate::{
    authorize,
    db::{get_runtime_handle, set_runtime_status},
    ApiError, HubState,
};

pub(crate) async fn interrupt_runtime(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
) -> Result<Json<RuntimeCommandResponse>, ApiError> {
    authorize(&state, &headers)?;
    let handle = match set_runtime_status(
        state.pool(),
        &agent_id,
        RuntimeLifecycleStatus::Interrupted,
    )
    .await
    {
        Ok(handle) => handle,
        Err(_) => get_runtime_handle(state.pool(), &agent_id).await?,
    };
    Ok(Json(RuntimeCommandResponse {
        agent_id,
        status: handle.status,
        message: "interrupt requested".to_string(),
    }))
}

pub(crate) async fn shutdown_runtime(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
) -> Result<Json<RuntimeCommandResponse>, ApiError> {
    authorize(&state, &headers)?;
    let handle =
        match set_runtime_status(state.pool(), &agent_id, RuntimeLifecycleStatus::Shutdown).await {
            Ok(handle) => handle,
            Err(_) => get_runtime_handle(state.pool(), &agent_id).await?,
        };
    Ok(Json(RuntimeCommandResponse {
        agent_id,
        status: handle.status,
        message: "shutdown requested".to_string(),
    }))
}
