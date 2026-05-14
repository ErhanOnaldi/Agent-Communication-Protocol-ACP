use acp_protocol::{
    TaskClaimRequest, TaskCreateRequest, TaskPriority, TaskRecord, TaskStatus, TaskStatusRequest,
};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::Utc;
use uuid::Uuid;

use crate::{
    authorize,
    db::{get_task_by_id, row_to_task, set_task_status, upsert_task, write_event},
    ApiError, HubState,
};

pub(crate) async fn create_task(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<TaskCreateRequest>,
) -> Result<(StatusCode, Json<TaskRecord>), ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    let task = TaskRecord {
        id: Uuid::new_v4(),
        title: req.title,
        body: req.body,
        status: TaskStatus::Open,
        owner: req.owner,
        priority: req.priority.unwrap_or(TaskPriority::Medium),
        branch: req.branch,
        created_by: req.created_by,
        created_at: now,
        updated_at: now,
    };
    upsert_task(state.pool(), &task).await?;
    write_event(
        state.pool(),
        &task.created_by,
        "task_created",
        serde_json::to_value(&task).unwrap_or_default(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(task)))
}

pub(crate) async fn list_tasks(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<TaskRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query("SELECT id, title, body, status, owner, priority, branch, created_by, created_at, updated_at FROM tasks ORDER BY updated_at DESC")
        .fetch_all(state.pool())
        .await?;
    rows.into_iter()
        .map(row_to_task)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn get_task(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<TaskRecord>, ApiError> {
    authorize(&state, &headers)?;
    get_task_by_id(state.pool(), id).await.map(Json)
}

pub(crate) async fn claim_task(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<TaskClaimRequest>,
) -> Result<Json<TaskRecord>, ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    sqlx::query("UPDATE tasks SET owner = ?1, branch = ?2, status = 'claimed', updated_at = ?3 WHERE id = ?4")
        .bind(&req.agent_id)
        .bind(&req.branch)
        .bind(now.to_rfc3339())
        .bind(id.to_string())
        .execute(state.pool())
        .await?;
    write_event(
        state.pool(),
        &req.agent_id,
        "task_claimed",
        serde_json::json!({ "task_id": id }),
    )
    .await?;
    get_task_by_id(state.pool(), id).await.map(Json)
}

pub(crate) async fn update_task_status(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<TaskStatusRequest>,
) -> Result<Json<TaskRecord>, ApiError> {
    authorize(&state, &headers)?;
    set_task_status(state.pool(), id, req.status).await?;
    let task = get_task_by_id(state.pool(), id).await?;
    write_event(
        state.pool(),
        task.owner.as_deref().unwrap_or(&task.created_by),
        "task_update",
        serde_json::to_value(&task).unwrap_or_default(),
    )
    .await?;
    Ok(Json(task))
}

pub(crate) async fn done_task(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(_req): Json<TaskStatusRequest>,
) -> Result<Json<TaskRecord>, ApiError> {
    authorize(&state, &headers)?;
    set_task_status(state.pool(), id, TaskStatus::Done).await?;
    get_task_by_id(state.pool(), id).await.map(Json)
}
