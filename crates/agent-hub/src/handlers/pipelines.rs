use agent_protocol::{
    ArtifactCreateRequest, ArtifactRecord, PipelineCreateRequest, PipelineEventCreateRequest,
    PipelineEventRecord, PipelineRecord, PipelineStatus, PipelineStatusUpdateRequest, RoleSlot,
    SlotStatus, SlotUpdateRequest, WorkflowConfig, WorkingContext, WorkingContextUpsertRequest,
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
    db::{
        get_pipeline_by_id, get_pipeline_event_by_id, get_slot_by_role, insert_pipeline_event,
        row_to_artifact, row_to_pipeline, row_to_pipeline_event, row_to_slot,
        row_to_working_context,
    },
    ApiError, HubState,
};

pub(crate) async fn create_pipeline(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<PipelineCreateRequest>,
) -> Result<(StatusCode, Json<PipelineRecord>), ApiError> {
    authorize(&state, &headers)?;
    let workflow: WorkflowConfig =
        serde_yaml::from_str(&req.workflow_yaml).map_err(ApiError::bad_request)?;
    let now = Utc::now();
    let pipeline = PipelineRecord {
        id: Uuid::new_v4(),
        workflow_yaml: req.workflow_yaml,
        status: if req.approve_assignments {
            PipelineStatus::Pending
        } else {
            PipelineStatus::AwaitingApproval
        },
        profile: workflow.workflow.profile,
        created_at: now,
        completed_at: None,
    };
    sqlx::query("INSERT INTO pipelines (id, workflow_yaml, status, profile, created_at, completed_at) VALUES (?1, ?2, ?3, ?4, ?5, NULL)")
        .bind(pipeline.id.to_string())
        .bind(&pipeline.workflow_yaml)
        .bind(pipeline.status.to_string())
        .bind(pipeline.profile.to_string())
        .bind(pipeline.created_at.to_rfc3339())
        .execute(state.pool())
        .await?;

    for (slot_name, slot) in workflow.workflow.slots {
        let runtime_type = slot.preferred.first().map(|pref| pref.runtime);
        let model_id = slot.preferred.first().and_then(|pref| pref.model.clone());
        let role = if slot.role.trim().is_empty() {
            slot_name
        } else {
            slot.role
        };
        sqlx::query("INSERT INTO slots (id, pipeline_id, role, runtime_type, model_id, agent_id, status, capabilities_json) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7)")
            .bind(Uuid::new_v4().to_string())
            .bind(pipeline.id.to_string())
            .bind(role)
            .bind(runtime_type.map(|runtime| runtime.to_string()))
            .bind(model_id)
            .bind(SlotStatus::Assigned.to_string())
            .bind(serde_json::to_string(&slot.required_capabilities).unwrap_or_else(|_| "[]".to_string()))
            .execute(state.pool())
            .await?;
    }
    insert_pipeline_event(
        state.pool(),
        pipeline.id,
        None,
        "pipeline_created",
        serde_json::json!({ "status": pipeline.status, "profile": pipeline.profile }),
        None,
        None,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(pipeline)))
}

pub(crate) async fn list_pipelines(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<PipelineRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query(
        "SELECT id, workflow_yaml, status, profile, created_at, completed_at FROM pipelines ORDER BY created_at DESC",
    )
    .fetch_all(state.pool())
    .await?;
    rows.into_iter()
        .map(row_to_pipeline)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn get_pipeline(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<PipelineRecord>, ApiError> {
    authorize(&state, &headers)?;
    get_pipeline_by_id(state.pool(), id).await.map(Json)
}

pub(crate) async fn update_pipeline_status(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<PipelineStatusUpdateRequest>,
) -> Result<Json<PipelineRecord>, ApiError> {
    authorize(&state, &headers)?;
    let completed_at = if req.completed {
        Some(Utc::now().to_rfc3339())
    } else {
        None
    };
    sqlx::query(
        "UPDATE pipelines SET status = ?1, completed_at = CASE WHEN ?2 IS NULL THEN completed_at ELSE ?2 END WHERE id = ?3",
    )
    .bind(req.status.to_string())
    .bind(completed_at)
    .bind(id.to_string())
    .execute(state.pool())
    .await?;
    insert_pipeline_event(
        state.pool(),
        id,
        None,
        "pipeline_status_updated",
        serde_json::json!({ "status": req.status, "completed": req.completed }),
        None,
        None,
    )
    .await?;
    get_pipeline_by_id(state.pool(), id).await.map(Json)
}

pub(crate) async fn list_pipeline_slots(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<RoleSlot>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query("SELECT id, pipeline_id, role, runtime_type, model_id, agent_id, status, capabilities_json FROM slots WHERE pipeline_id = ?1 ORDER BY role")
        .bind(id.to_string())
        .fetch_all(state.pool())
        .await?;
    rows.into_iter()
        .map(row_to_slot)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn update_pipeline_slot(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((id, role)): Path<(Uuid, String)>,
    Json(req): Json<SlotUpdateRequest>,
) -> Result<Json<RoleSlot>, ApiError> {
    authorize(&state, &headers)?;
    sqlx::query(
        r#"
        UPDATE slots
        SET status = ?1,
            runtime_type = CASE WHEN ?2 THEN NULL ELSE COALESCE(?3, runtime_type) END,
            model_id = CASE WHEN ?2 THEN NULL ELSE COALESCE(?4, model_id) END,
            agent_id = CASE WHEN ?2 THEN NULL ELSE COALESCE(?5, agent_id) END
        WHERE pipeline_id = ?6 AND role = ?7
        "#,
    )
    .bind(req.status.to_string())
    .bind(req.clear_assignment)
    .bind(req.runtime_type.map(|runtime| runtime.to_string()))
    .bind(req.model_id)
    .bind(req.agent_id)
    .bind(id.to_string())
    .bind(&role)
    .execute(state.pool())
    .await?;
    insert_pipeline_event(
        state.pool(),
        id,
        None,
        "slot_status_changed",
        serde_json::json!({ "role": role, "status": req.status }),
        None,
        None,
    )
    .await?;
    get_slot_by_role(state.pool(), id, &role).await.map(Json)
}

pub(crate) async fn list_pipeline_events(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<PipelineEventRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query("SELECT id, pipeline_id, agent_id, event_type, payload, correlation_id, causation_id, created_at FROM pipeline_events WHERE pipeline_id = ?1 ORDER BY id ASC")
        .bind(id.to_string())
        .fetch_all(state.pool())
        .await?;
    rows.into_iter()
        .map(row_to_pipeline_event)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn create_pipeline_event(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(mut req): Json<PipelineEventCreateRequest>,
) -> Result<(StatusCode, Json<PipelineEventRecord>), ApiError> {
    authorize(&state, &headers)?;
    req.pipeline_id = id;
    let event_id = insert_pipeline_event(
        state.pool(),
        req.pipeline_id,
        req.agent_id,
        &req.event_type,
        req.payload,
        req.correlation_id,
        req.causation_id,
    )
    .await?;
    let event = get_pipeline_event_by_id(state.pool(), event_id).await?;
    Ok((StatusCode::CREATED, Json(event)))
}

pub(crate) async fn list_pipeline_artifacts(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<ArtifactRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query("SELECT id, pipeline_id, stage_name, artifact_type, content, created_by, created_at FROM artifacts WHERE pipeline_id = ?1 ORDER BY created_at ASC")
        .bind(id.to_string())
        .fetch_all(state.pool())
        .await?;
    rows.into_iter()
        .map(row_to_artifact)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

pub(crate) async fn create_artifact(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(mut req): Json<ArtifactCreateRequest>,
) -> Result<(StatusCode, Json<ArtifactRecord>), ApiError> {
    authorize(&state, &headers)?;
    req.pipeline_id = id;
    let artifact = ArtifactRecord {
        id: Uuid::new_v4(),
        pipeline_id: req.pipeline_id,
        stage_name: req.stage_name,
        artifact_type: req.artifact_type,
        content: req.content,
        created_by: req.created_by,
        created_at: Utc::now(),
    };
    sqlx::query("INSERT INTO artifacts (id, pipeline_id, stage_name, artifact_type, content, created_by, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")
        .bind(artifact.id.to_string())
        .bind(artifact.pipeline_id.to_string())
        .bind(&artifact.stage_name)
        .bind(&artifact.artifact_type)
        .bind(&artifact.content)
        .bind(&artifact.created_by)
        .bind(artifact.created_at.to_rfc3339())
        .execute(state.pool())
        .await?;
    insert_pipeline_event(
        state.pool(),
        artifact.pipeline_id,
        Some(artifact.created_by.clone()),
        "artifact_created",
        serde_json::json!({ "artifact_id": artifact.id, "stage_name": artifact.stage_name, "artifact_type": artifact.artifact_type }),
        None,
        None,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(artifact)))
}

pub(crate) async fn get_working_context(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((pipeline_id, role)): Path<(Uuid, String)>,
) -> Result<Json<WorkingContext>, ApiError> {
    authorize(&state, &headers)?;
    let row = sqlx::query("SELECT pipeline_id, role, summary, key_decisions, active_files, updated_at FROM working_context WHERE pipeline_id = ?1 AND role = ?2")
        .bind(pipeline_id.to_string())
        .bind(role)
        .fetch_optional(state.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("working context not found"))?;
    row_to_working_context(row).map(Json)
}

pub(crate) async fn upsert_working_context(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((pipeline_id, role)): Path<(Uuid, String)>,
    Json(req): Json<WorkingContextUpsertRequest>,
) -> Result<Json<WorkingContext>, ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO working_context (pipeline_id, role, summary, key_decisions, active_files, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(pipeline_id, role) DO UPDATE SET
            summary = excluded.summary,
            key_decisions = excluded.key_decisions,
            active_files = excluded.active_files,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(pipeline_id.to_string())
    .bind(&role)
    .bind(req.summary)
    .bind(req.key_decisions.to_string())
    .bind(serde_json::to_string(&req.active_files).unwrap_or_else(|_| "[]".to_string()))
    .bind(now.to_rfc3339())
    .execute(state.pool())
    .await?;
    get_working_context(State(state), headers, Path((pipeline_id, role))).await
}
