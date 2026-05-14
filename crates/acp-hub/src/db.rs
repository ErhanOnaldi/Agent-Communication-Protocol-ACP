use acp_protocol::{
    AgentHandle, AgentRecord, ArtifactRecord, CapabilityScoreRecord,
    ContextCompressionCreateRequest, ContextCompressionRecord, FileClaimRecord, FindingKind,
    FindingRecord, McpHealth, McpServerRecord, MessageCreateRequest, MessageKind, MessageRecord,
    MessageStatus, ModelPricing, ModelRecord, ModelTier, PipelineAnalyticsResponse,
    PipelineEventRecord, PipelineRecord, PipelineStatus, RoleSlot, RuntimeHealth,
    RuntimeLifecycleStatus, RuntimeType, SchedulerDecision, SchedulerDecisionCreateRequest,
    SchedulerProfile, SemanticMemoryCreateRequest, SemanticMemoryRecord, SlotStatus,
    StepMetricCreateRequest, StepMetricsRecord, TaskRecord, TaskStatus, ThreadRecord,
    WorkingContext,
};
use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use uuid::Uuid;

use crate::{ApiError, HubState};

pub(crate) fn row_to_agent(row: SqliteRow) -> Result<AgentRecord, ApiError> {
    let status: String = row.try_get("status")?;
    Ok(AgentRecord {
        id: row.try_get("id")?,
        role: row.try_get("role")?,
        hostname: row.try_get("hostname")?,
        status: status.parse().map_err(ApiError::bad_request)?,
        current_task: row.try_get("current_task")?,
        branch: row.try_get("branch")?,
        last_seen_at: parse_time(row.try_get::<String, _>("last_seen_at")?)?,
    })
}

pub(crate) fn row_to_message(row: SqliteRow) -> Result<MessageRecord, ApiError> {
    let kind: String = row.try_get("kind")?;
    let status: String = row.try_get("status")?;
    Ok(MessageRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        from_agent: row.try_get("from_agent")?,
        to_agent: row.try_get("to_agent")?,
        kind: kind.parse::<MessageKind>().map_err(ApiError::bad_request)?,
        subject: row.try_get("subject")?,
        body: row.try_get("body")?,
        thread_id: parse_uuid(row.try_get::<String, _>("thread_id")?)?,
        reply_to: row
            .try_get::<Option<String>, _>("reply_to")?
            .map(parse_uuid)
            .transpose()?,
        status: status
            .parse::<MessageStatus>()
            .map_err(ApiError::bad_request)?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
        read_at: row
            .try_get::<Option<String>, _>("read_at")?
            .map(parse_time)
            .transpose()?,
    })
}

pub(crate) fn row_to_thread(row: SqliteRow) -> Result<ThreadRecord, ApiError> {
    let status: String = row.try_get("status")?;
    Ok(ThreadRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        subject: row.try_get("subject")?,
        status: status.parse().map_err(ApiError::bad_request)?,
        summary: row.try_get("summary")?,
        created_by: row.try_get("created_by")?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
        closed_at: row
            .try_get::<Option<String>, _>("closed_at")?
            .map(parse_time)
            .transpose()?,
        message_count: row.try_get("message_count")?,
    })
}

pub(crate) fn row_to_task(row: SqliteRow) -> Result<TaskRecord, ApiError> {
    let status: String = row.try_get("status")?;
    let priority: String = row.try_get("priority")?;
    Ok(TaskRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        title: row.try_get("title")?,
        body: row.try_get("body")?,
        status: status.parse().map_err(ApiError::bad_request)?,
        owner: row.try_get("owner")?,
        priority: priority.parse().map_err(ApiError::bad_request)?,
        branch: row.try_get("branch")?,
        created_by: row.try_get("created_by")?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
        updated_at: parse_time(row.try_get::<String, _>("updated_at")?)?,
    })
}

pub(crate) fn row_to_file_claim(row: SqliteRow) -> Result<FileClaimRecord, ApiError> {
    let expires_at = row
        .try_get::<Option<String>, _>("expires_at")?
        .map(parse_time)
        .transpose()?;
    let stale = expires_at.is_some_and(|expires_at| expires_at < Utc::now());
    Ok(FileClaimRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        file_path: row.try_get("file_path")?,
        claimed_by: row.try_get("claimed_by")?,
        task_id: row
            .try_get::<Option<String>, _>("task_id")?
            .map(parse_uuid)
            .transpose()?,
        branch: row.try_get("branch")?,
        reason: row.try_get("reason")?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
        expires_at,
        stale,
    })
}

pub(crate) fn row_to_finding(row: SqliteRow) -> Result<FindingRecord, ApiError> {
    let kind: String = row.try_get("kind")?;
    let confidence: String = row.try_get("confidence")?;
    let files_json: String = row.try_get("files_json")?;
    Ok(FindingRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        agent_id: row.try_get("agent_id")?,
        kind: kind.parse::<FindingKind>().map_err(ApiError::bad_request)?,
        title: row.try_get("title")?,
        body: row.try_get("body")?,
        files: serde_json::from_str(&files_json).unwrap_or_default(),
        confidence: confidence.parse().map_err(ApiError::bad_request)?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
    })
}

pub(crate) fn row_to_model(row: SqliteRow) -> Result<ModelRecord, ApiError> {
    let tier: String = row.try_get("tier")?;
    Ok(ModelRecord {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        runtime_source: row.try_get("runtime_source")?,
        tier: tier.parse::<ModelTier>().map_err(ApiError::bad_request)?,
        context_window: row.try_get("context_window")?,
        pricing: ModelPricing {
            input: row.try_get("pricing_input")?,
            output: row.try_get("pricing_output")?,
        },
    })
}

pub(crate) fn row_to_capability_score(row: SqliteRow) -> Result<CapabilityScoreRecord, ApiError> {
    let runtime_type: String = row.try_get("runtime_type")?;
    let last_updated_at = row
        .try_get::<Option<String>, _>("last_updated_at")?
        .map(parse_time)
        .transpose()?;
    Ok(CapabilityScoreRecord {
        runtime_type: runtime_type
            .parse::<RuntimeType>()
            .map_err(ApiError::bad_request)?,
        model_id: row.try_get("model_id")?,
        capability: row.try_get("capability")?,
        success_count: row.try_get("success_count")?,
        failure_count: row.try_get("failure_count")?,
        last_updated_at,
    })
}

pub(crate) fn row_to_pipeline(row: SqliteRow) -> Result<PipelineRecord, ApiError> {
    let status: String = row.try_get("status")?;
    let profile: String = row.try_get("profile")?;
    Ok(PipelineRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        workflow_yaml: row.try_get("workflow_yaml")?,
        status: status
            .parse::<PipelineStatus>()
            .map_err(ApiError::bad_request)?,
        profile: profile
            .parse::<SchedulerProfile>()
            .map_err(ApiError::bad_request)?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
        completed_at: row
            .try_get::<Option<String>, _>("completed_at")?
            .map(parse_time)
            .transpose()?,
    })
}

pub(crate) fn row_to_slot(row: SqliteRow) -> Result<RoleSlot, ApiError> {
    let runtime_type = row
        .try_get::<Option<String>, _>("runtime_type")?
        .map(|value| value.parse::<RuntimeType>().map_err(ApiError::bad_request))
        .transpose()?;
    let status: String = row.try_get("status")?;
    let capabilities_json: String = row.try_get("capabilities_json")?;
    Ok(RoleSlot {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        pipeline_id: parse_uuid(row.try_get::<String, _>("pipeline_id")?)?,
        role: row.try_get("role")?,
        runtime_type,
        model_id: row.try_get("model_id")?,
        agent_id: row.try_get("agent_id")?,
        status: status
            .parse::<SlotStatus>()
            .map_err(ApiError::bad_request)?,
        capabilities: serde_json::from_str(&capabilities_json).unwrap_or_default(),
    })
}

pub(crate) fn row_to_pipeline_event(row: SqliteRow) -> Result<PipelineEventRecord, ApiError> {
    let payload: String = row.try_get("payload")?;
    Ok(PipelineEventRecord {
        id: row.try_get("id")?,
        pipeline_id: parse_uuid(row.try_get::<String, _>("pipeline_id")?)?,
        agent_id: row.try_get("agent_id")?,
        event_type: row.try_get("event_type")?,
        payload: serde_json::from_str(&payload).unwrap_or_else(|_| serde_json::json!({})),
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
    })
}

pub(crate) fn row_to_artifact(row: SqliteRow) -> Result<ArtifactRecord, ApiError> {
    Ok(ArtifactRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        pipeline_id: parse_uuid(row.try_get::<String, _>("pipeline_id")?)?,
        stage_name: row.try_get("stage_name")?,
        artifact_type: row.try_get("artifact_type")?,
        content: row.try_get("content")?,
        created_by: row.try_get("created_by")?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
    })
}

pub(crate) fn row_to_working_context(row: SqliteRow) -> Result<WorkingContext, ApiError> {
    let key_decisions: String = row.try_get("key_decisions")?;
    let active_files: String = row.try_get("active_files")?;
    Ok(WorkingContext {
        pipeline_id: parse_uuid(row.try_get::<String, _>("pipeline_id")?)?,
        role: row.try_get("role")?,
        summary: row.try_get("summary")?,
        key_decisions: serde_json::from_str(&key_decisions)
            .unwrap_or_else(|_| serde_json::json!({})),
        active_files: serde_json::from_str(&active_files).unwrap_or_default(),
        updated_at: parse_time(row.try_get::<String, _>("updated_at")?)?,
    })
}

pub(crate) fn parse_uuid(value: String) -> Result<Uuid, ApiError> {
    Uuid::parse_str(&value).map_err(|err| ApiError::bad_request(err.to_string()))
}

pub(crate) fn parse_time(value: String) -> Result<DateTime<Utc>, ApiError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| ApiError::bad_request(err.to_string()))
}

pub(crate) async fn active_agents(pool: &SqlitePool) -> Result<Vec<AgentRecord>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, role, hostname, status, current_task, branch, last_seen_at FROM agents WHERE status != 'offline' ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_agent).collect()
}

pub(crate) async fn get_agent_by_id(pool: &SqlitePool, id: &str) -> Result<AgentRecord, ApiError> {
    let row = sqlx::query(
        "SELECT id, role, hostname, status, current_task, branch, last_seen_at FROM agents WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("agent not found"))?;
    row_to_agent(row)
}

pub(crate) async fn insert_message(
    pool: &SqlitePool,
    req: MessageCreateRequest,
) -> Result<MessageRecord, ApiError> {
    if req.subject.trim().is_empty() {
        return Err(ApiError::bad_request("subject cannot be empty"));
    }
    let id = Uuid::new_v4();
    let thread_id = req.thread_id.unwrap_or(id);
    let now = Utc::now();
    ensure_thread(pool, thread_id, &req.subject, &req.from, now).await?;
    let message = MessageRecord {
        id,
        from_agent: req.from,
        to_agent: req.to,
        kind: req.kind,
        subject: req.subject,
        body: req.body,
        thread_id,
        reply_to: req.reply_to,
        status: MessageStatus::Unread,
        created_at: now,
        read_at: None,
    };
    sqlx::query(
        r#"
        INSERT INTO messages (id, from_agent, to_agent, kind, subject, body, thread_id, reply_to, status, created_at, read_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL)
        "#,
    )
    .bind(message.id.to_string())
    .bind(&message.from_agent)
    .bind(&message.to_agent)
    .bind(message.kind.to_string())
    .bind(&message.subject)
    .bind(&message.body)
    .bind(message.thread_id.to_string())
    .bind(message.reply_to.map(|id| id.to_string()))
    .bind(message.status.to_string())
    .bind(message.created_at.to_rfc3339())
    .execute(pool)
    .await?;
    Ok(message)
}

pub(crate) async fn ensure_thread(
    pool: &SqlitePool,
    id: Uuid,
    subject: &str,
    created_by: &str,
    now: DateTime<Utc>,
) -> Result<(), ApiError> {
    sqlx::query(
        r#"
        INSERT INTO threads (id, subject, status, summary, created_by, created_at, closed_at)
        VALUES (?1, ?2, 'open', NULL, ?3, ?4, NULL)
        ON CONFLICT(id) DO NOTHING
        "#,
    )
    .bind(id.to_string())
    .bind(subject)
    .bind(created_by)
    .bind(now.to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn get_message(pool: &SqlitePool, id: Uuid) -> Result<MessageRecord, ApiError> {
    let row = sqlx::query(
        r#"
        SELECT id, from_agent, to_agent, kind, subject, body, thread_id, reply_to, status, created_at, read_at
        FROM messages
        WHERE id = ?1
        "#,
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("message not found"))?;
    row_to_message(row)
}

pub(crate) async fn get_thread(pool: &SqlitePool, id: Uuid) -> Result<ThreadRecord, ApiError> {
    let row = sqlx::query(
        r#"
        SELECT t.id, t.subject, t.status, t.summary, t.created_by, t.created_at, t.closed_at,
               COUNT(m.id) AS message_count
        FROM threads t
        LEFT JOIN messages m ON m.thread_id = t.id
        WHERE t.id = ?1
        GROUP BY t.id
        "#,
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("thread not found"))?;
    row_to_thread(row)
}

pub(crate) async fn upsert_task(pool: &SqlitePool, task: &TaskRecord) -> Result<(), ApiError> {
    sqlx::query("INSERT INTO tasks (id, title, body, status, owner, priority, branch, created_by, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)")
        .bind(task.id.to_string())
        .bind(&task.title)
        .bind(&task.body)
        .bind(task.status.to_string())
        .bind(&task.owner)
        .bind(task.priority.to_string())
        .bind(&task.branch)
        .bind(&task.created_by)
        .bind(task.created_at.to_rfc3339())
        .bind(task.updated_at.to_rfc3339())
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn get_task_by_id(pool: &SqlitePool, id: Uuid) -> Result<TaskRecord, ApiError> {
    let row = sqlx::query("SELECT id, title, body, status, owner, priority, branch, created_by, created_at, updated_at FROM tasks WHERE id = ?1")
        .bind(id.to_string())
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| ApiError::not_found("task not found"))?;
    row_to_task(row)
}

pub(crate) async fn get_pipeline_by_id(
    pool: &SqlitePool,
    id: Uuid,
) -> Result<PipelineRecord, ApiError> {
    let row = sqlx::query(
        "SELECT id, workflow_yaml, status, profile, created_at, completed_at FROM pipelines WHERE id = ?1",
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("pipeline not found"))?;
    row_to_pipeline(row)
}

pub(crate) async fn get_slot_by_role(
    pool: &SqlitePool,
    pipeline_id: Uuid,
    role: &str,
) -> Result<RoleSlot, ApiError> {
    let row = sqlx::query("SELECT id, pipeline_id, role, runtime_type, model_id, agent_id, status, capabilities_json FROM slots WHERE pipeline_id = ?1 AND role = ?2")
        .bind(pipeline_id.to_string())
        .bind(role)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| ApiError::not_found("slot not found"))?;
    row_to_slot(row)
}

pub(crate) async fn insert_pipeline_event(
    pool: &SqlitePool,
    pipeline_id: Uuid,
    agent_id: Option<String>,
    event_type: &str,
    payload: serde_json::Value,
    correlation_id: Option<String>,
    causation_id: Option<String>,
) -> Result<i64, ApiError> {
    let result = sqlx::query("INSERT INTO pipeline_events (pipeline_id, agent_id, event_type, payload, correlation_id, causation_id, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")
        .bind(pipeline_id.to_string())
        .bind(agent_id)
        .bind(event_type)
        .bind(payload.to_string())
        .bind(correlation_id)
        .bind(causation_id)
        .bind(Utc::now().to_rfc3339())
        .execute(pool)
        .await?;
    Ok(result.last_insert_rowid())
}

pub(crate) async fn get_pipeline_event_by_id(
    pool: &SqlitePool,
    id: i64,
) -> Result<PipelineEventRecord, ApiError> {
    let row = sqlx::query("SELECT id, pipeline_id, agent_id, event_type, payload, correlation_id, causation_id, created_at FROM pipeline_events WHERE id = ?1")
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| ApiError::not_found("pipeline event not found"))?;
    row_to_pipeline_event(row)
}

pub(crate) async fn set_task_status(
    pool: &SqlitePool,
    id: Uuid,
    status: TaskStatus,
) -> Result<(), ApiError> {
    sqlx::query("UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3")
        .bind(status.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn active_file_claims_for_path(
    pool: &SqlitePool,
    path: &str,
) -> Result<Vec<FileClaimRecord>, ApiError> {
    let rows = sqlx::query("SELECT id, file_path, claimed_by, task_id, branch, reason, created_at, expires_at FROM file_claims WHERE file_path = ?1")
        .bind(path)
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_file_claim).collect()
}

pub(crate) async fn query_findings(
    pool: &SqlitePool,
    query: Option<&str>,
) -> Result<Vec<FindingRecord>, ApiError> {
    let rows = if let Some(query) = query {
        let like = format!("%{query}%");
        sqlx::query("SELECT id, agent_id, kind, title, body, files_json, confidence, created_at FROM findings WHERE title LIKE ?1 OR body LIKE ?1 OR files_json LIKE ?1 ORDER BY created_at DESC")
            .bind(like)
            .fetch_all(pool)
            .await?
    } else {
        sqlx::query("SELECT id, agent_id, kind, title, body, files_json, confidence, created_at FROM findings ORDER BY created_at DESC")
            .fetch_all(pool)
            .await?
    };
    rows.into_iter().map(row_to_finding).collect()
}

pub(crate) async fn emit_message(
    state: &HubState,
    message: &MessageRecord,
    event_type: &str,
) -> Result<(), ApiError> {
    write_event(
        state.pool(),
        &message.to_agent,
        event_type,
        serde_json::to_value(message).unwrap_or_default(),
    )
    .await?;
    let _ = state.inner.events.send(message.clone());
    Ok(())
}

pub(crate) async fn write_event(
    pool: &SqlitePool,
    agent_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), ApiError> {
    sqlx::query("INSERT INTO events (agent_id, event_type, payload_json, created_at) VALUES (?1, ?2, ?3, ?4)")
        .bind(agent_id)
        .bind(event_type)
        .bind(payload.to_string())
        .bind(Utc::now().to_rfc3339())
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn insert_step_metric(
    pool: &SqlitePool,
    req: &StepMetricCreateRequest,
) -> Result<StepMetricsRecord, ApiError> {
    let pipeline_id = req.pipeline_id.to_string();
    let runtime_type = req.runtime_type.map(|r| r.to_string());
    let health = req.health.to_string();
    let now = Utc::now().to_rfc3339();
    let latency = req.latency_ms.map(|v| v as i64);
    let id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO step_metrics (pipeline_id, step_name, role, runtime_type, model_id, latency_ms, health, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) RETURNING id",
    )
    .bind(&pipeline_id)
    .bind(&req.step_name)
    .bind(&req.role)
    .bind(&runtime_type)
    .bind(&req.model_id)
    .bind(latency)
    .bind(&health)
    .bind(&now)
    .fetch_one(pool)
    .await?;
    Ok(StepMetricsRecord {
        id,
        pipeline_id: req.pipeline_id,
        step_name: req.step_name.clone(),
        role: req.role.clone(),
        runtime_type: req.runtime_type,
        model_id: req.model_id.clone(),
        latency_ms: req.latency_ms,
        health: req.health,
        created_at: parse_time(now)?,
    })
}

pub(crate) async fn pipeline_step_metrics(
    pool: &SqlitePool,
    pipeline_id: Uuid,
) -> Result<Vec<StepMetricsRecord>, ApiError> {
    let pid = pipeline_id.to_string();
    let rows = sqlx::query(
        "SELECT id, pipeline_id, step_name, role, runtime_type, model_id, latency_ms, health, created_at
         FROM step_metrics WHERE pipeline_id = ?1 ORDER BY id ASC",
    )
    .bind(&pid)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_step_metric).collect()
}

fn row_to_step_metric(row: SqliteRow) -> Result<StepMetricsRecord, ApiError> {
    let pipeline_id = parse_uuid(row.try_get::<String, _>("pipeline_id")?)?;
    let runtime_type = row
        .try_get::<Option<String>, _>("runtime_type")?
        .map(|s| s.parse::<RuntimeType>().map_err(ApiError::bad_request))
        .transpose()?;
    let health: String = row.try_get("health")?;
    let latency_ms = row
        .try_get::<Option<i64>, _>("latency_ms")?
        .map(|v| v as u64);
    Ok(StepMetricsRecord {
        id: row.try_get("id")?,
        pipeline_id,
        step_name: row.try_get("step_name")?,
        role: row.try_get("role")?,
        runtime_type,
        model_id: row.try_get("model_id")?,
        latency_ms,
        health: health
            .parse::<RuntimeHealth>()
            .map_err(ApiError::bad_request)?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
    })
}

pub(crate) fn row_to_scheduler_decision(row: SqliteRow) -> Result<SchedulerDecision, ApiError> {
    let runtime_type: String = row.try_get("runtime_type")?;
    Ok(SchedulerDecision {
        id: row.try_get("id")?,
        pipeline_id: parse_uuid(row.try_get::<String, _>("pipeline_id")?)?,
        role: row.try_get("role")?,
        runtime_type: runtime_type
            .parse::<RuntimeType>()
            .map_err(ApiError::bad_request)?,
        model_id: row.try_get("model_id")?,
        base_score: row.try_get("base_score")?,
        learned_delta: row.try_get("learned_delta")?,
        profile_boost: row.try_get("profile_boost")?,
        final_score: row.try_get("final_score")?,
        reason: row.try_get("reason")?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
    })
}

pub(crate) fn row_to_context_compression(
    row: SqliteRow,
) -> Result<ContextCompressionRecord, ApiError> {
    let semantic_refs: String = row.try_get("semantic_refs")?;
    Ok(ContextCompressionRecord {
        id: row.try_get("id")?,
        pipeline_id: parse_uuid(row.try_get::<String, _>("pipeline_id")?)?,
        role: row.try_get("role")?,
        compressor: row.try_get("compressor")?,
        source_tokens: row.try_get("source_tokens")?,
        summary: row.try_get("summary")?,
        semantic_refs: serde_json::from_str(&semantic_refs).unwrap_or_default(),
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
    })
}

pub(crate) fn row_to_semantic_memory(row: SqliteRow) -> Result<SemanticMemoryRecord, ApiError> {
    let embedding: String = row.try_get("embedding")?;
    Ok(SemanticMemoryRecord {
        id: row.try_get("id")?,
        pipeline_id: parse_uuid(row.try_get::<String, _>("pipeline_id")?)?,
        item_id: row.try_get("item_id")?,
        content: row.try_get("content")?,
        embedding_provider: row.try_get("embedding_provider")?,
        embedding_model: row.try_get("embedding_model")?,
        embedding: serde_json::from_str(&embedding).unwrap_or_default(),
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
    })
}

pub(crate) fn row_to_agent_handle(row: SqliteRow) -> Result<AgentHandle, ApiError> {
    let runtime_type: String = row.try_get("runtime_type")?;
    let status: String = row.try_get("status")?;
    Ok(AgentHandle {
        agent_id: row.try_get("agent_id")?,
        pid: row.try_get::<Option<i64>, _>("pid")?.map(|pid| pid as u32),
        runtime_type: runtime_type
            .parse::<RuntimeType>()
            .map_err(ApiError::bad_request)?,
        started_at: parse_time(row.try_get::<String, _>("started_at")?)?,
        status: status
            .parse::<RuntimeLifecycleStatus>()
            .map_err(ApiError::bad_request)?,
    })
}

pub(crate) fn row_to_mcp_server(row: SqliteRow) -> Result<McpServerRecord, ApiError> {
    let args: String = row.try_get("args")?;
    let env: String = row.try_get("env")?;
    let capabilities: String = row.try_get("capabilities")?;
    Ok(McpServerRecord {
        name: row.try_get("name")?,
        command: row.try_get("command")?,
        args: serde_json::from_str(&args).unwrap_or_default(),
        env: serde_json::from_str(&env).unwrap_or_default(),
        working_dir: row.try_get("working_dir")?,
        mode: row.try_get("mode")?,
        timeout_ms: row
            .try_get::<Option<i64>, _>("timeout_ms")?
            .map(|v| v as u64),
        auto_start: row.try_get::<i64, _>("auto_start")? != 0,
        capabilities: serde_json::from_str(&capabilities).unwrap_or_default(),
    })
}

pub(crate) fn row_to_mcp_health(row: SqliteRow) -> Result<McpHealth, ApiError> {
    let status: String = row.try_get("status")?;
    Ok(McpHealth {
        name: row.try_get("name")?,
        status: status
            .parse::<RuntimeHealth>()
            .map_err(ApiError::bad_request)?,
        pid: row.try_get::<Option<i64>, _>("pid")?.map(|pid| pid as u32),
        message: row.try_get("message")?,
        checked_at: parse_time(row.try_get::<String, _>("checked_at")?)?,
    })
}

pub(crate) async fn pipeline_analytics(
    pool: &SqlitePool,
    pipeline_id: Uuid,
) -> Result<PipelineAnalyticsResponse, ApiError> {
    let steps = pipeline_step_metrics(pool, pipeline_id).await?;
    let total_steps = steps.len();
    let succeeded = steps
        .iter()
        .filter(|s| s.health == RuntimeHealth::Healthy)
        .count();
    let failed = total_steps - succeeded;

    let mut latencies: Vec<u64> = steps.iter().filter_map(|s| s.latency_ms).collect();
    latencies.sort_unstable();
    let p50 = percentile(&latencies, 50);
    let p95 = percentile(&latencies, 95);

    Ok(PipelineAnalyticsResponse {
        pipeline_id,
        total_steps,
        succeeded,
        failed,
        p50_latency_ms: p50,
        p95_latency_ms: p95,
        steps,
    })
}

pub(crate) async fn insert_scheduler_decision(
    pool: &SqlitePool,
    req: SchedulerDecisionCreateRequest,
) -> Result<SchedulerDecision, ApiError> {
    let id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO scheduler_decisions
            (pipeline_id, role, runtime_type, model_id, base_score, learned_delta, profile_boost, final_score, reason, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        RETURNING id
        "#,
    )
    .bind(req.pipeline_id.to_string())
    .bind(&req.role)
    .bind(req.runtime_type.to_string())
    .bind(&req.model_id)
    .bind(req.base_score)
    .bind(req.learned_delta)
    .bind(req.profile_boost)
    .bind(req.final_score)
    .bind(&req.reason)
    .bind(Utc::now().to_rfc3339())
    .fetch_one(pool)
    .await?;
    let row = sqlx::query("SELECT id, pipeline_id, role, runtime_type, model_id, base_score, learned_delta, profile_boost, final_score, reason, created_at FROM scheduler_decisions WHERE id = ?1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    row_to_scheduler_decision(row)
}

pub(crate) async fn pipeline_scheduler_decisions(
    pool: &SqlitePool,
    pipeline_id: Uuid,
) -> Result<Vec<SchedulerDecision>, ApiError> {
    let rows = sqlx::query("SELECT id, pipeline_id, role, runtime_type, model_id, base_score, learned_delta, profile_boost, final_score, reason, created_at FROM scheduler_decisions WHERE pipeline_id = ?1 ORDER BY id ASC")
        .bind(pipeline_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_scheduler_decision).collect()
}

pub(crate) async fn insert_context_compression(
    pool: &SqlitePool,
    req: ContextCompressionCreateRequest,
) -> Result<ContextCompressionRecord, ApiError> {
    let id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO context_compressions
            (pipeline_id, role, compressor, source_tokens, summary, semantic_refs, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        RETURNING id
        "#,
    )
    .bind(req.pipeline_id.to_string())
    .bind(&req.role)
    .bind(&req.compressor)
    .bind(req.source_tokens)
    .bind(&req.summary)
    .bind(serde_json::to_string(&req.semantic_refs).unwrap_or_else(|_| "[]".to_string()))
    .bind(Utc::now().to_rfc3339())
    .fetch_one(pool)
    .await?;
    let row = sqlx::query("SELECT id, pipeline_id, role, compressor, source_tokens, summary, semantic_refs, created_at FROM context_compressions WHERE id = ?1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    row_to_context_compression(row)
}

pub(crate) async fn pipeline_context_compressions(
    pool: &SqlitePool,
    pipeline_id: Uuid,
) -> Result<Vec<ContextCompressionRecord>, ApiError> {
    let rows = sqlx::query("SELECT id, pipeline_id, role, compressor, source_tokens, summary, semantic_refs, created_at FROM context_compressions WHERE pipeline_id = ?1 ORDER BY id ASC")
        .bind(pipeline_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_context_compression).collect()
}

pub(crate) async fn insert_semantic_memory(
    pool: &SqlitePool,
    req: SemanticMemoryCreateRequest,
) -> Result<SemanticMemoryRecord, ApiError> {
    let id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO semantic_memory
            (pipeline_id, item_id, content, embedding_provider, embedding_model, embedding, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        RETURNING id
        "#,
    )
    .bind(req.pipeline_id.to_string())
    .bind(&req.item_id)
    .bind(&req.content)
    .bind(&req.embedding_provider)
    .bind(&req.embedding_model)
    .bind(serde_json::to_string(&req.embedding).unwrap_or_else(|_| "[]".to_string()))
    .bind(Utc::now().to_rfc3339())
    .fetch_one(pool)
    .await?;
    let row = sqlx::query("SELECT id, pipeline_id, item_id, content, embedding_provider, embedding_model, embedding, created_at FROM semantic_memory WHERE id = ?1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    row_to_semantic_memory(row)
}

pub(crate) async fn pipeline_semantic_memory(
    pool: &SqlitePool,
    pipeline_id: Uuid,
) -> Result<Vec<SemanticMemoryRecord>, ApiError> {
    let rows = sqlx::query("SELECT id, pipeline_id, item_id, content, embedding_provider, embedding_model, embedding, created_at FROM semantic_memory WHERE pipeline_id = ?1 ORDER BY id ASC")
        .bind(pipeline_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_semantic_memory).collect()
}

#[allow(dead_code)]
pub(crate) async fn upsert_runtime_handle(
    pool: &SqlitePool,
    handle: &AgentHandle,
) -> Result<AgentHandle, ApiError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        r#"
        INSERT INTO runtime_handles (agent_id, pid, runtime_type, started_at, status, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(agent_id) DO UPDATE SET
            pid = excluded.pid,
            runtime_type = excluded.runtime_type,
            status = excluded.status,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&handle.agent_id)
    .bind(handle.pid.map(|pid| pid as i64))
    .bind(handle.runtime_type.to_string())
    .bind(handle.started_at.to_rfc3339())
    .bind(handle.status.to_string())
    .bind(now)
    .execute(pool)
    .await?;
    get_runtime_handle(pool, &handle.agent_id).await
}

pub(crate) async fn get_runtime_handle(
    pool: &SqlitePool,
    agent_id: &str,
) -> Result<AgentHandle, ApiError> {
    let row = sqlx::query("SELECT agent_id, pid, runtime_type, started_at, status FROM runtime_handles WHERE agent_id = ?1")
        .bind(agent_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| ApiError::not_found("runtime handle not found"))?;
    row_to_agent_handle(row)
}

pub(crate) async fn set_runtime_status(
    pool: &SqlitePool,
    agent_id: &str,
    status: RuntimeLifecycleStatus,
) -> Result<AgentHandle, ApiError> {
    sqlx::query("UPDATE runtime_handles SET status = ?1, updated_at = ?2 WHERE agent_id = ?3")
        .bind(status.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(agent_id)
        .execute(pool)
        .await?;
    get_runtime_handle(pool, agent_id).await
}

pub(crate) async fn list_mcp_servers(pool: &SqlitePool) -> Result<Vec<McpServerRecord>, ApiError> {
    let rows = sqlx::query("SELECT name, command, args, env, working_dir, mode, timeout_ms, auto_start, capabilities FROM mcp_servers ORDER BY name")
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_mcp_server).collect()
}

pub(crate) async fn upsert_mcp_server(
    pool: &SqlitePool,
    server: &McpServerRecord,
) -> Result<McpServerRecord, ApiError> {
    sqlx::query(
        r#"
        INSERT INTO mcp_servers (name, command, args, env, working_dir, mode, timeout_ms, auto_start, capabilities, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(name) DO UPDATE SET
            command = excluded.command,
            args = excluded.args,
            env = excluded.env,
            working_dir = excluded.working_dir,
            mode = excluded.mode,
            timeout_ms = excluded.timeout_ms,
            auto_start = excluded.auto_start,
            capabilities = excluded.capabilities,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&server.name)
    .bind(&server.command)
    .bind(serde_json::to_string(&server.args).unwrap_or_else(|_| "[]".to_string()))
    .bind(serde_json::to_string(&server.env).unwrap_or_else(|_| "{}".to_string()))
    .bind(&server.working_dir)
    .bind(&server.mode)
    .bind(server.timeout_ms.map(|v| v as i64))
    .bind(if server.auto_start { 1 } else { 0 })
    .bind(serde_json::to_string(&server.capabilities).unwrap_or_else(|_| "[]".to_string()))
    .bind(Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    let row = sqlx::query("SELECT name, command, args, env, working_dir, mode, timeout_ms, auto_start, capabilities FROM mcp_servers WHERE name = ?1")
        .bind(&server.name)
        .fetch_one(pool)
        .await?;
    row_to_mcp_server(row)
}

pub(crate) async fn mcp_health(pool: &SqlitePool, name: &str) -> Result<McpHealth, ApiError> {
    let row = sqlx::query(
        "SELECT name, status, pid, message, checked_at FROM mcp_health WHERE name = ?1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("MCP health not found"))?;
    row_to_mcp_health(row)
}

pub(crate) async fn upsert_mcp_health(
    pool: &SqlitePool,
    health: &McpHealth,
) -> Result<McpHealth, ApiError> {
    sqlx::query(
        r#"
        INSERT INTO mcp_health (name, status, pid, message, checked_at)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(name) DO UPDATE SET
            status = excluded.status,
            pid = excluded.pid,
            message = excluded.message,
            checked_at = excluded.checked_at
        "#,
    )
    .bind(&health.name)
    .bind(health.status.to_string())
    .bind(health.pid.map(|pid| pid as i64))
    .bind(&health.message)
    .bind(health.checked_at.to_rfc3339())
    .execute(pool)
    .await?;
    mcp_health(pool, &health.name).await
}

fn percentile(sorted: &[u64], pct: usize) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = (pct * sorted.len()).saturating_sub(1) / 100;
    Some(sorted[idx.min(sorted.len() - 1)])
}
