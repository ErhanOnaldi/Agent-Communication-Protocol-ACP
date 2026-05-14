use agent_protocol::{
    AgentRecord, ArtifactRecord, CapabilityScoreRecord, FileClaimRecord, FindingKind, FindingRecord,
    MessageCreateRequest, MessageKind, MessageRecord, MessageStatus, ModelPricing, ModelRecord,
    ModelTier, PipelineEventRecord, PipelineRecord, PipelineStatus, RoleSlot, RuntimeType,
    SchedulerProfile, SlotStatus, TaskRecord, TaskStatus, ThreadRecord, WorkingContext,
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
        status: status.parse::<MessageStatus>().map_err(ApiError::bad_request)?,
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
    Ok(CapabilityScoreRecord {
        runtime_type: runtime_type
            .parse::<RuntimeType>()
            .map_err(ApiError::bad_request)?,
        model_id: row.try_get("model_id")?,
        capability: row.try_get("capability")?,
        success_count: row.try_get("success_count")?,
        failure_count: row.try_get("failure_count")?,
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

pub(crate) async fn get_pipeline_by_id(pool: &SqlitePool, id: Uuid) -> Result<PipelineRecord, ApiError> {
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

pub(crate) async fn set_task_status(pool: &SqlitePool, id: Uuid, status: TaskStatus) -> Result<(), ApiError> {
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
