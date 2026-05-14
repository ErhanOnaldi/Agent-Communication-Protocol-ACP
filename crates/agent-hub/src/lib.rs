use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use agent_protocol::{
    AgentRecord, AgentStatus, ArtifactCreateRequest, BroadcastRequest, CapabilityScoreRecord,
    CapabilityScoreUpdateRequest, ErrorResponse, FileClaimRecord, FileClaimRequest,
    FileClaimResponse, FindingCreateRequest, FindingKind, FindingRecord, HeartbeatRequest,
    MessageCreateRequest, MessageKind, MessageRecord, MessageStatus, ModelPricing, ModelRecord,
    ModelTier, PipelineCreateRequest, PipelineEventCreateRequest, PipelineEventRecord,
    PipelineRecord, PipelineStatus, PipelineStatusUpdateRequest, ReplyRequest, RoleMessageRequest,
    RoleSlot, RuntimeType, SchedulerProfile, SlotStatus, SlotUpdateRequest, TaskClaimRequest,
    TaskCreateRequest, TaskPriority, TaskRecord, TaskStatus, TaskStatusRequest, ThreadDetail,
    ThreadRecord, UpdateAgentStatusRequest, WorkflowConfig, WorkingContext,
    WorkingContextUpsertRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{delete, get, post},
    Json, Router,
};
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::stream::Stream;
use serde::Deserialize;
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use tokio::sync::broadcast;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

#[derive(Clone)]
pub struct HubState {
    inner: Arc<HubStateInner>,
}

struct HubStateInner {
    pool: SqlitePool,
    token: String,
    events: broadcast::Sender<MessageRecord>,
    rate_limits: Mutex<HashMap<String, RateBucket>>,
}

#[derive(Debug, Clone)]
struct RateBucket {
    window_started: Instant,
    count: u32,
}

impl HubState {
    pub fn new(pool: SqlitePool, token: String) -> Self {
        let (events, _) = broadcast::channel(512);
        Self {
            inner: Arc::new(HubStateInner {
                pool,
                token,
                events,
                rate_limits: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.inner.pool
    }
}

pub fn app(state: HubState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/agents", get(list_agents))
        .route("/api/agents/heartbeat", post(heartbeat))
        .route("/api/agents/:agent_id", get(get_agent))
        .route("/api/agents/:agent_id/status", post(update_agent_status))
        .route("/api/messages", post(create_message).get(list_messages))
        .route("/api/messages/broadcast", post(broadcast_message))
        .route("/api/messages/to-role/:role", post(message_to_role))
        .route("/api/messages/:id/read", post(mark_read))
        .route("/api/messages/:id/reply", post(reply_to_message))
        .route("/api/threads", get(list_threads))
        .route("/api/threads/:id", get(get_thread_detail))
        .route("/api/threads/:id/reply", post(reply_to_thread))
        .route("/api/threads/:id/close", post(close_thread))
        .route("/api/tasks", post(create_task).get(list_tasks))
        .route("/api/tasks/:id", get(get_task))
        .route("/api/tasks/:id/claim", post(claim_task))
        .route("/api/tasks/:id/status", post(update_task_status))
        .route("/api/tasks/:id/done", post(done_task))
        .route("/api/models", get(list_models))
        .route(
            "/api/capability-scores",
            post(update_capability_score).get(list_capability_scores),
        )
        .route("/api/pipelines", post(create_pipeline).get(list_pipelines))
        .route("/api/pipelines/:id", get(get_pipeline))
        .route("/api/pipelines/:id/status", post(update_pipeline_status))
        .route("/api/pipelines/:id/slots", get(list_pipeline_slots))
        .route("/api/pipelines/:id/slots/:role", post(update_pipeline_slot))
        .route(
            "/api/pipelines/:id/events",
            post(create_pipeline_event).get(list_pipeline_events),
        )
        .route(
            "/api/pipelines/:id/artifacts",
            post(create_artifact).get(list_pipeline_artifacts),
        )
        .route(
            "/api/memory/:pipeline_id/:role",
            get(get_working_context).put(upsert_working_context),
        )
        .route(
            "/api/file-claims",
            post(create_file_claim).get(list_file_claims),
        )
        .route("/api/file-claims/:id", delete(delete_file_claim))
        .route("/api/findings", post(create_finding).get(list_findings))
        .route("/api/findings/search", get(search_findings))
        .route("/api/findings/:id", get(get_finding))
        .route("/api/stream", get(stream))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn init_db(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
        .execute(pool)
        .await?;
    run_migration(pool, 1, MIGRATION_1).await?;
    run_migration(pool, 2, MIGRATION_2).await?;
    run_migration(pool, 3, MIGRATION_3).await?;
    Ok(())
}

async fn run_migration(pool: &SqlitePool, version: i64, sql: &str) -> anyhow::Result<()> {
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations WHERE version = ?1")
            .bind(version)
            .fetch_optional(pool)
            .await?;
    if exists.is_some() {
        return Ok(());
    }
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(statement).execute(pool).await?;
    }
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)")
        .bind(version)
        .bind(Utc::now().to_rfc3339())
        .execute(pool)
        .await?;
    Ok(())
}

const MIGRATION_1: &str = r#"
CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    hostname TEXT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    from_agent TEXT NOT NULL,
    to_agent TEXT NOT NULL,
    kind TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    reply_to TEXT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    read_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"#;

const MIGRATION_2: &str = r#"
ALTER TABLE agents ADD COLUMN status TEXT NOT NULL DEFAULT 'online';
ALTER TABLE agents ADD COLUMN current_task TEXT NULL;
ALTER TABLE agents ADD COLUMN branch TEXT NULL;
CREATE TABLE IF NOT EXISTS threads (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    closed_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    status TEXT NOT NULL,
    owner TEXT NULL,
    priority TEXT NOT NULL,
    branch TEXT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_claims (
    id TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    claimed_by TEXT NOT NULL,
    task_id TEXT NULL,
    branch TEXT NULL,
    reason TEXT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS findings (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    files_json TEXT NOT NULL,
    confidence TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"#;

const MIGRATION_3: &str = r#"
CREATE TABLE IF NOT EXISTS models (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    runtime_source TEXT NOT NULL,
    tier TEXT NOT NULL,
    context_window INTEGER NULL,
    pricing_input REAL NULL,
    pricing_output REAL NULL
);
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    workflow_yaml TEXT NOT NULL,
    status TEXT NOT NULL,
    profile TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS slots (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    role TEXT NOT NULL,
    runtime_type TEXT NULL,
    model_id TEXT NULL,
    agent_id TEXT NULL,
    status TEXT NOT NULL DEFAULT 'empty',
    capabilities_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS pipeline_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id TEXT NOT NULL,
    agent_id TEXT NULL,
    event_type TEXT NOT NULL,
    payload JSON NOT NULL,
    correlation_id TEXT NULL,
    causation_id TEXT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    content TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS working_context (
    pipeline_id TEXT NOT NULL,
    role TEXT NOT NULL,
    summary TEXT NOT NULL,
    key_decisions JSON NOT NULL,
    active_files JSON NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (pipeline_id, role)
);
CREATE TABLE IF NOT EXISTS capability_scores (
    runtime_type TEXT NOT NULL,
    model_id TEXT NOT NULL,
    capability TEXT NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (runtime_type, model_id, capability)
);
INSERT OR IGNORE INTO models (id, name, runtime_source, tier, context_window, pricing_input, pricing_output)
VALUES
    ('claude-code/default', 'Claude Code default', 'claude_code', 'premium', NULL, NULL, NULL),
    ('codex/default', 'Codex default', 'codex', 'premium', NULL, NULL, NULL),
    ('gemini/default', 'Gemini default', 'gemini', 'standard', NULL, NULL, NULL),
    ('copilot/default', 'GitHub Copilot default', 'copilot', 'standard', NULL, NULL, NULL),
    ('claudex/qwen3-coder', 'Qwen3 Coder via Claudex', 'claudex', 'cheap', NULL, NULL, NULL),
    ('claudex/deepseek', 'DeepSeek via Claudex', 'claudex', 'cheap', NULL, NULL, NULL);
"#;

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn heartbeat(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<HeartbeatRequest>,
) -> Result<Json<AgentRecord>, ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    let status = req.status.unwrap_or(AgentStatus::Online);
    sqlx::query(
        r#"
        INSERT INTO agents (id, role, hostname, status, current_task, branch, last_seen_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(id) DO UPDATE SET
            role = excluded.role,
            hostname = excluded.hostname,
            status = excluded.status,
            current_task = excluded.current_task,
            branch = excluded.branch,
            last_seen_at = excluded.last_seen_at
        "#,
    )
    .bind(&req.agent_id)
    .bind(&req.role)
    .bind(&req.hostname)
    .bind(status.to_string())
    .bind(&req.current_task)
    .bind(&req.branch)
    .bind(now.to_rfc3339())
    .execute(state.pool())
    .await?;
    write_event(
        state.pool(),
        &req.agent_id,
        "agent_registered",
        serde_json::json!({ "role": req.role, "status": status }),
    )
    .await?;
    get_agent_by_id(state.pool(), &req.agent_id).await.map(Json)
}

async fn list_agents(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<AgentRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query(
        "SELECT id, role, hostname, status, current_task, branch, last_seen_at FROM agents ORDER BY id",
    )
    .fetch_all(state.pool())
    .await?;
    rows.into_iter()
        .map(row_to_agent)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn get_agent(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
) -> Result<Json<AgentRecord>, ApiError> {
    authorize(&state, &headers)?;
    get_agent_by_id(state.pool(), &agent_id).await.map(Json)
}

async fn update_agent_status(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
    Json(req): Json<UpdateAgentStatusRequest>,
) -> Result<Json<AgentRecord>, ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    sqlx::query(
        "UPDATE agents SET status = ?1, current_task = ?2, branch = ?3, last_seen_at = ?4 WHERE id = ?5",
    )
    .bind(req.status.to_string())
    .bind(req.current_task)
    .bind(req.branch)
    .bind(now.to_rfc3339())
    .bind(&agent_id)
    .execute(state.pool())
    .await?;
    write_event(
        state.pool(),
        &agent_id,
        "agent_status_changed",
        serde_json::json!({ "status": req.status }),
    )
    .await?;
    get_agent_by_id(state.pool(), &agent_id).await.map(Json)
}

async fn create_message(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<MessageCreateRequest>,
) -> Result<(StatusCode, Json<MessageRecord>), ApiError> {
    authorize(&state, &headers)?;
    let message = insert_message(state.pool(), req).await?;
    emit_message(&state, &message, "message_sent").await?;
    Ok((StatusCode::CREATED, Json(message)))
}

async fn broadcast_message(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<BroadcastRequest>,
) -> Result<(StatusCode, Json<Vec<MessageRecord>>), ApiError> {
    authorize(&state, &headers)?;
    let agents = active_agents(state.pool()).await?;
    let mut messages = Vec::new();
    for agent in agents {
        if req.exclude_self && agent.id == req.from {
            continue;
        }
        let message = insert_message(
            state.pool(),
            MessageCreateRequest {
                from: req.from.clone(),
                to: agent.id,
                kind: req.kind.clone(),
                subject: req.subject.clone(),
                body: req.body.clone(),
                thread_id: None,
                reply_to: None,
            },
        )
        .await?;
        emit_message(&state, &message, "message_broadcast").await?;
        messages.push(message);
    }
    Ok((StatusCode::CREATED, Json(messages)))
}

async fn message_to_role(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(role): Path<String>,
    Json(mut req): Json<RoleMessageRequest>,
) -> Result<(StatusCode, Json<Vec<MessageRecord>>), ApiError> {
    authorize(&state, &headers)?;
    req.role = role;
    let rows = sqlx::query(
        "SELECT id, role, hostname, status, current_task, branch, last_seen_at FROM agents WHERE role = ?1 AND status != 'offline' ORDER BY id",
    )
    .bind(&req.role)
    .fetch_all(state.pool())
    .await?;
    let agents = rows
        .into_iter()
        .map(row_to_agent)
        .collect::<Result<Vec<_>, _>>()?;
    let mut messages = Vec::new();
    for agent in agents {
        if req.exclude_self && agent.id == req.from {
            continue;
        }
        let message = insert_message(
            state.pool(),
            MessageCreateRequest {
                from: req.from.clone(),
                to: agent.id,
                kind: req.kind.clone(),
                subject: req.subject.clone(),
                body: req.body.clone(),
                thread_id: None,
                reply_to: None,
            },
        )
        .await?;
        emit_message(&state, &message, "message_to_role").await?;
        messages.push(message);
    }
    Ok((StatusCode::CREATED, Json(messages)))
}

#[derive(Debug, Deserialize)]
struct ListMessageQuery {
    agent_id: String,
    status: Option<String>,
    kind: Option<String>,
}

async fn list_messages(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<ListMessageQuery>,
) -> Result<Json<Vec<MessageRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let status = query
        .status
        .as_deref()
        .map(str::parse::<MessageStatus>)
        .transpose()
        .map_err(ApiError::bad_request)?;
    let kind = query
        .kind
        .as_deref()
        .map(str::parse::<MessageKind>)
        .transpose()
        .map_err(ApiError::bad_request)?;

    let rows = sqlx::query(
        r#"
        SELECT id, from_agent, to_agent, kind, subject, body, thread_id, reply_to, status, created_at, read_at
        FROM messages
        WHERE to_agent = ?1
          AND (?2 IS NULL OR status = ?2)
          AND (?3 IS NULL OR kind = ?3)
        ORDER BY created_at ASC
        "#,
    )
    .bind(&query.agent_id)
    .bind(status.map(|s| s.to_string()))
    .bind(kind.map(|k| k.to_string()))
    .fetch_all(state.pool())
    .await?;
    rows.into_iter()
        .map(row_to_message)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn mark_read(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<MessageRecord>, ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    sqlx::query("UPDATE messages SET status = 'read', read_at = ?1 WHERE id = ?2")
        .bind(now.to_rfc3339())
        .bind(id.to_string())
        .execute(state.pool())
        .await?;
    get_message(state.pool(), id).await.map(Json)
}

async fn reply_to_message(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<ReplyRequest>,
) -> Result<(StatusCode, Json<MessageRecord>), ApiError> {
    authorize(&state, &headers)?;
    let original = get_message(state.pool(), id).await?;
    let subject = req
        .subject
        .unwrap_or_else(|| format!("Re: {}", original.subject));
    let message = insert_message(
        state.pool(),
        MessageCreateRequest {
            from: req.from,
            to: original.from_agent,
            kind: MessageKind::Answer,
            subject,
            body: req.body,
            thread_id: Some(original.thread_id),
            reply_to: Some(original.id),
        },
    )
    .await?;
    emit_message(&state, &message, "message_replied").await?;
    Ok((StatusCode::CREATED, Json(message)))
}

#[derive(Debug, Deserialize)]
struct ListThreadQuery {
    agent_id: Option<String>,
}

async fn list_threads(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<ListThreadQuery>,
) -> Result<Json<Vec<ThreadRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = if let Some(agent_id) = query.agent_id {
        sqlx::query(
            r#"
            SELECT t.id, t.subject, t.status, t.summary, t.created_by, t.created_at, t.closed_at,
                   COUNT(m.id) AS message_count
            FROM threads t
            JOIN messages m ON m.thread_id = t.id
            WHERE m.to_agent = ?1 OR m.from_agent = ?1
            GROUP BY t.id
            ORDER BY t.created_at DESC
            "#,
        )
        .bind(agent_id)
        .fetch_all(state.pool())
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT t.id, t.subject, t.status, t.summary, t.created_by, t.created_at, t.closed_at,
                   COUNT(m.id) AS message_count
            FROM threads t
            LEFT JOIN messages m ON m.thread_id = t.id
            GROUP BY t.id
            ORDER BY t.created_at DESC
            "#,
        )
        .fetch_all(state.pool())
        .await?
    };
    rows.into_iter()
        .map(row_to_thread)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn get_thread_detail(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<ThreadDetail>, ApiError> {
    authorize(&state, &headers)?;
    let thread = get_thread(state.pool(), id).await?;
    let rows = sqlx::query(
        r#"
        SELECT id, from_agent, to_agent, kind, subject, body, thread_id, reply_to, status, created_at, read_at
        FROM messages WHERE thread_id = ?1 ORDER BY created_at ASC
        "#,
    )
    .bind(id.to_string())
    .fetch_all(state.pool())
    .await?;
    let messages = rows
        .into_iter()
        .map(row_to_message)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ThreadDetail { thread, messages }))
}

async fn reply_to_thread(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<ReplyRequest>,
) -> Result<(StatusCode, Json<MessageRecord>), ApiError> {
    authorize(&state, &headers)?;
    let detail = get_thread_detail(State(state.clone()), headers.clone(), Path(id))
        .await?
        .0;
    let first = detail
        .messages
        .first()
        .ok_or_else(|| ApiError::not_found("thread has no messages"))?;
    let to = detail
        .messages
        .iter()
        .rev()
        .find(|message| message.from_agent != req.from)
        .map(|message| message.from_agent.clone())
        .unwrap_or_else(|| first.from_agent.clone());
    let subject = req
        .subject
        .unwrap_or_else(|| format!("Re: {}", detail.thread.subject));
    let message = insert_message(
        state.pool(),
        MessageCreateRequest {
            from: req.from,
            to,
            kind: MessageKind::Answer,
            subject,
            body: req.body,
            thread_id: Some(id),
            reply_to: detail.messages.last().map(|message| message.id),
        },
    )
    .await?;
    emit_message(&state, &message, "thread_replied").await?;
    Ok((StatusCode::CREATED, Json(message)))
}

async fn close_thread(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<ThreadRecord>, ApiError> {
    authorize(&state, &headers)?;
    let now = Utc::now();
    sqlx::query("UPDATE threads SET status = 'closed', closed_at = ?1 WHERE id = ?2")
        .bind(now.to_rfc3339())
        .bind(id.to_string())
        .execute(state.pool())
        .await?;
    get_thread(state.pool(), id).await.map(Json)
}

async fn create_task(
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

async fn list_tasks(
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

async fn get_task(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<TaskRecord>, ApiError> {
    authorize(&state, &headers)?;
    get_task_by_id(state.pool(), id).await.map(Json)
}

async fn claim_task(
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

async fn update_task_status(
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

async fn done_task(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(_req): Json<TaskStatusRequest>,
) -> Result<Json<TaskRecord>, ApiError> {
    authorize(&state, &headers)?;
    set_task_status(state.pool(), id, TaskStatus::Done).await?;
    get_task_by_id(state.pool(), id).await.map(Json)
}

async fn list_models(
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

async fn list_capability_scores(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<CapabilityScoreRecord>>, ApiError> {
    authorize(&state, &headers)?;
    let rows = sqlx::query("SELECT runtime_type, model_id, capability, success_count, failure_count FROM capability_scores ORDER BY capability, runtime_type, model_id")
        .fetch_all(state.pool())
        .await?;
    rows.into_iter()
        .map(row_to_capability_score)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn update_capability_score(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<CapabilityScoreUpdateRequest>,
) -> Result<Json<CapabilityScoreRecord>, ApiError> {
    authorize(&state, &headers)?;
    sqlx::query(
        r#"
        INSERT INTO capability_scores (runtime_type, model_id, capability, success_count, failure_count)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(runtime_type, model_id, capability) DO UPDATE SET
            success_count = success_count + ?4,
            failure_count = failure_count + ?5
        "#,
    )
    .bind(req.runtime_type.to_string())
    .bind(&req.model_id)
    .bind(&req.capability)
    .bind(if req.success { 1 } else { 0 })
    .bind(if req.success { 0 } else { 1 })
    .execute(state.pool())
    .await?;
    let row = sqlx::query("SELECT runtime_type, model_id, capability, success_count, failure_count FROM capability_scores WHERE runtime_type = ?1 AND model_id = ?2 AND capability = ?3")
        .bind(req.runtime_type.to_string())
        .bind(req.model_id)
        .bind(req.capability)
        .fetch_one(state.pool())
        .await?;
    row_to_capability_score(row).map(Json)
}

async fn create_pipeline(
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

async fn list_pipelines(
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

async fn get_pipeline(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<PipelineRecord>, ApiError> {
    authorize(&state, &headers)?;
    get_pipeline_by_id(state.pool(), id).await.map(Json)
}

async fn update_pipeline_status(
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

async fn list_pipeline_slots(
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

async fn update_pipeline_slot(
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

async fn list_pipeline_events(
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

async fn create_pipeline_event(
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

async fn list_pipeline_artifacts(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<agent_protocol::ArtifactRecord>>, ApiError> {
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

async fn create_artifact(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(mut req): Json<ArtifactCreateRequest>,
) -> Result<(StatusCode, Json<agent_protocol::ArtifactRecord>), ApiError> {
    authorize(&state, &headers)?;
    req.pipeline_id = id;
    let artifact = agent_protocol::ArtifactRecord {
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

async fn get_working_context(
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

async fn upsert_working_context(
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

#[derive(Debug, Deserialize)]
struct FileClaimQuery {
    path: Option<String>,
}

async fn create_file_claim(
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

async fn list_file_claims(
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

async fn delete_file_claim(
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

#[derive(Debug, Deserialize)]
struct FindingSearchQuery {
    q: String,
}

async fn create_finding(
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

async fn list_findings(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<Vec<FindingRecord>>, ApiError> {
    authorize(&state, &headers)?;
    query_findings(state.pool(), None).await.map(Json)
}

async fn search_findings(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<FindingSearchQuery>,
) -> Result<Json<Vec<FindingRecord>>, ApiError> {
    authorize(&state, &headers)?;
    query_findings(state.pool(), Some(&query.q)).await.map(Json)
}

async fn get_finding(
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

#[derive(Debug, Deserialize)]
struct StreamQuery {
    agent_id: String,
    last_event_id: Option<Uuid>,
    since: Option<String>,
}

async fn stream(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    authorize(&state, &headers)?;
    let mut rx = state.inner.events.subscribe();
    let agent_id = query.agent_id;
    let replay = replay_messages(state.pool(), &agent_id, query.last_event_id, query.since).await?;
    let stream = async_stream::stream! {
        for message in replay {
            let data = serde_json::to_string(&message).unwrap_or_else(|_| "{}".to_string());
            yield Ok(Event::default().event("message").id(message.id.to_string()).data(data));
        }
        loop {
            match rx.recv().await {
                Ok(message) if message.to_agent == agent_id => {
                    let data = serde_json::to_string(&message).unwrap_or_else(|_| "{}".to_string());
                    yield Ok(Event::default().event("message").id(message.id.to_string()).data(data));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    yield Ok(Event::default().event("lagged").data("{}"));
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn replay_messages(
    pool: &SqlitePool,
    agent_id: &str,
    last_event_id: Option<Uuid>,
    since: Option<String>,
) -> Result<Vec<MessageRecord>, ApiError> {
    let since_time = if let Some(last_event_id) = last_event_id {
        sqlx::query_scalar::<_, Option<String>>("SELECT created_at FROM messages WHERE id = ?1")
            .bind(last_event_id.to_string())
            .fetch_optional(pool)
            .await?
            .flatten()
    } else {
        since
    };
    let Some(since_time) = since_time else {
        return Ok(Vec::new());
    };
    let rows = sqlx::query(
        r#"
        SELECT id, from_agent, to_agent, kind, subject, body, thread_id, reply_to, status, created_at, read_at
        FROM messages
        WHERE to_agent = ?1 AND created_at > ?2
        ORDER BY created_at ASC
        "#,
    )
    .bind(agent_id)
    .bind(since_time)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_message).collect()
}

async fn active_agents(pool: &SqlitePool) -> Result<Vec<AgentRecord>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, role, hostname, status, current_task, branch, last_seen_at FROM agents WHERE status != 'offline' ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_agent).collect()
}

async fn get_agent_by_id(pool: &SqlitePool, id: &str) -> Result<AgentRecord, ApiError> {
    let row = sqlx::query(
        "SELECT id, role, hostname, status, current_task, branch, last_seen_at FROM agents WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("agent not found"))?;
    row_to_agent(row)
}

async fn insert_message(
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

async fn ensure_thread(
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

async fn get_message(pool: &SqlitePool, id: Uuid) -> Result<MessageRecord, ApiError> {
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

async fn get_thread(pool: &SqlitePool, id: Uuid) -> Result<ThreadRecord, ApiError> {
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

async fn upsert_task(pool: &SqlitePool, task: &TaskRecord) -> Result<(), ApiError> {
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

async fn get_task_by_id(pool: &SqlitePool, id: Uuid) -> Result<TaskRecord, ApiError> {
    let row = sqlx::query("SELECT id, title, body, status, owner, priority, branch, created_by, created_at, updated_at FROM tasks WHERE id = ?1")
        .bind(id.to_string())
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| ApiError::not_found("task not found"))?;
    row_to_task(row)
}

async fn get_pipeline_by_id(pool: &SqlitePool, id: Uuid) -> Result<PipelineRecord, ApiError> {
    let row = sqlx::query(
        "SELECT id, workflow_yaml, status, profile, created_at, completed_at FROM pipelines WHERE id = ?1",
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("pipeline not found"))?;
    row_to_pipeline(row)
}

async fn get_slot_by_role(
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

async fn insert_pipeline_event(
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

async fn get_pipeline_event_by_id(
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

async fn set_task_status(pool: &SqlitePool, id: Uuid, status: TaskStatus) -> Result<(), ApiError> {
    sqlx::query("UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3")
        .bind(status.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

async fn active_file_claims_for_path(
    pool: &SqlitePool,
    path: &str,
) -> Result<Vec<FileClaimRecord>, ApiError> {
    let rows = sqlx::query("SELECT id, file_path, claimed_by, task_id, branch, reason, created_at, expires_at FROM file_claims WHERE file_path = ?1")
        .bind(path)
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_file_claim).collect()
}

async fn query_findings(
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

async fn emit_message(
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

async fn write_event(
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

fn row_to_agent(row: SqliteRow) -> Result<AgentRecord, ApiError> {
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

fn row_to_message(row: SqliteRow) -> Result<MessageRecord, ApiError> {
    let kind: String = row.try_get("kind")?;
    let status: String = row.try_get("status")?;
    Ok(MessageRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        from_agent: row.try_get("from_agent")?,
        to_agent: row.try_get("to_agent")?,
        kind: kind.parse().map_err(ApiError::bad_request)?,
        subject: row.try_get("subject")?,
        body: row.try_get("body")?,
        thread_id: parse_uuid(row.try_get::<String, _>("thread_id")?)?,
        reply_to: row
            .try_get::<Option<String>, _>("reply_to")?
            .map(parse_uuid)
            .transpose()?,
        status: status.parse().map_err(ApiError::bad_request)?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
        read_at: row
            .try_get::<Option<String>, _>("read_at")?
            .map(parse_time)
            .transpose()?,
    })
}

fn row_to_thread(row: SqliteRow) -> Result<ThreadRecord, ApiError> {
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

fn row_to_task(row: SqliteRow) -> Result<TaskRecord, ApiError> {
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

fn row_to_file_claim(row: SqliteRow) -> Result<FileClaimRecord, ApiError> {
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

fn row_to_finding(row: SqliteRow) -> Result<FindingRecord, ApiError> {
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

fn row_to_model(row: SqliteRow) -> Result<ModelRecord, ApiError> {
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

fn row_to_capability_score(row: SqliteRow) -> Result<CapabilityScoreRecord, ApiError> {
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

fn row_to_pipeline(row: SqliteRow) -> Result<PipelineRecord, ApiError> {
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

fn row_to_slot(row: SqliteRow) -> Result<RoleSlot, ApiError> {
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

fn row_to_pipeline_event(row: SqliteRow) -> Result<PipelineEventRecord, ApiError> {
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

fn row_to_artifact(row: SqliteRow) -> Result<agent_protocol::ArtifactRecord, ApiError> {
    Ok(agent_protocol::ArtifactRecord {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        pipeline_id: parse_uuid(row.try_get::<String, _>("pipeline_id")?)?,
        stage_name: row.try_get("stage_name")?,
        artifact_type: row.try_get("artifact_type")?,
        content: row.try_get("content")?,
        created_by: row.try_get("created_by")?,
        created_at: parse_time(row.try_get::<String, _>("created_at")?)?,
    })
}

fn row_to_working_context(row: SqliteRow) -> Result<WorkingContext, ApiError> {
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

fn parse_uuid(value: String) -> Result<Uuid, ApiError> {
    Uuid::parse_str(&value).map_err(|err| ApiError::bad_request(err.to_string()))
}

fn parse_time(value: String) -> Result<DateTime<Utc>, ApiError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| ApiError::bad_request(err.to_string()))
}

fn authorize(state: &HubState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(ApiError::unauthorized("missing bearer token"));
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(ApiError::unauthorized("missing bearer token"));
    };
    if token != state.inner.token {
        return Err(ApiError::unauthorized("invalid bearer token"));
    }
    check_rate_limit(state, token)?;
    Ok(())
}

fn check_rate_limit(state: &HubState, token: &str) -> Result<(), ApiError> {
    const WINDOW: Duration = Duration::from_secs(60);
    const MAX_REQUESTS: u32 = 600;

    let now = Instant::now();
    let mut limits = state
        .inner
        .rate_limits
        .lock()
        .map_err(|_| ApiError::too_many_requests("rate limiter unavailable"))?;
    let bucket = limits.entry(token.to_string()).or_insert(RateBucket {
        window_started: now,
        count: 0,
    });
    if now.duration_since(bucket.window_started) >= WINDOW {
        bucket.window_started = now;
        bucket.count = 0;
    }
    bucket.count += 1;
    if bucket.count > MAX_REQUESTS {
        return Err(ApiError::too_many_requests("rate limit exceeded"));
    }
    Ok(())
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl ToString) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.to_string(),
        }
    }

    fn unauthorized(message: impl ToString) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.to_string(),
        }
    }

    fn not_found(message: impl ToString) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.to_string(),
        }
    }

    fn too_many_requests(message: impl ToString) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: message.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(value: sqlx::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: value.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_protocol::{
        ArtifactCreateRequest, Confidence, FileClaimRequest, FindingCreateRequest, FindingKind,
        HeartbeatRequest, PipelineCreateRequest, PipelineEventCreateRequest,
        PipelineStatusUpdateRequest, SlotUpdateRequest, TaskCreateRequest, ThreadStatus,
    };
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request},
    };
    use sqlx::sqlite::SqlitePoolOptions;
    use tower::ServiceExt;

    async fn test_app() -> Router {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_db(&pool).await.unwrap();
        app(HubState::new(pool, "secret".to_string()))
    }

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_db(&pool).await.unwrap();
        pool
    }

    fn request(
        method: Method,
        uri: &str,
        body: impl serde::Serialize,
        token: &str,
    ) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Body::empty())
            .unwrap()
    }

    async fn decode<T: serde::de::DeserializeOwned>(response: Response) -> T {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn rejects_invalid_token() {
        let app = test_app().await;
        let response = app
            .oneshot(request(
                Method::POST,
                "/api/agents/heartbeat",
                HeartbeatRequest {
                    agent_id: "frontend".to_string(),
                    role: "frontend".to_string(),
                    hostname: None,
                    status: None,
                    current_task: None,
                    branch: None,
                },
                "wrong",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn register_is_idempotent_and_status_can_update() {
        let app = test_app().await;
        let heartbeat = HeartbeatRequest {
            agent_id: "frontend".to_string(),
            role: "frontend".to_string(),
            hostname: Some("macbook".to_string()),
            status: Some(AgentStatus::Working),
            current_task: Some("charts".to_string()),
            branch: Some("agent/frontend/charts".to_string()),
        };
        assert_eq!(
            app.clone()
                .oneshot(request(
                    Method::POST,
                    "/api/agents/heartbeat",
                    &heartbeat,
                    "secret"
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(request(
                    Method::POST,
                    "/api/agents/heartbeat",
                    &heartbeat,
                    "secret"
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        let response = app
            .oneshot(request(
                Method::POST,
                "/api/agents/frontend/status",
                UpdateAgentStatusRequest {
                    status: AgentStatus::Blocked,
                    current_task: Some("waiting backend".to_string()),
                    branch: None,
                },
                "secret",
            ))
            .await
            .unwrap();
        let agent: AgentRecord = decode(response).await;
        assert_eq!(agent.status, AgentStatus::Blocked);
        assert_eq!(agent.current_task.as_deref(), Some("waiting backend"));
    }

    #[tokio::test]
    async fn message_broadcast_and_threads_work() {
        let app = test_app().await;
        for (agent_id, role) in [("frontend", "frontend"), ("backend", "backend")] {
            app.clone()
                .oneshot(request(
                    Method::POST,
                    "/api/agents/heartbeat",
                    HeartbeatRequest {
                        agent_id: agent_id.to_string(),
                        role: role.to_string(),
                        hostname: None,
                        status: None,
                        current_task: None,
                        branch: None,
                    },
                    "secret",
                ))
                .await
                .unwrap();
        }

        let response = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/messages/broadcast",
                BroadcastRequest {
                    from: "frontend".to_string(),
                    kind: MessageKind::ContractChange,
                    subject: "DTO changed".to_string(),
                    body: "name -> display_name".to_string(),
                    exclude_self: true,
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let messages: Vec<MessageRecord> = decode(response).await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].to_agent, "backend");

        let thread_id = messages[0].thread_id;
        let detail: ThreadDetail = decode(
            app.oneshot(get(&format!("/api/threads/{thread_id}")))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(detail.thread.status, ThreadStatus::Open);
        assert_eq!(detail.messages.len(), 1);
    }

    #[tokio::test]
    async fn task_file_claim_and_finding_flows_work() {
        let app = test_app().await;
        let task_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/tasks",
                TaskCreateRequest {
                    title: "Fix auth panic".to_string(),
                    body: "Missing token panics".to_string(),
                    priority: None,
                    owner: None,
                    branch: None,
                    created_by: "backend".to_string(),
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(task_response.status(), StatusCode::CREATED);
        let task: TaskRecord = decode(task_response).await;

        let claim_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/file-claims",
                FileClaimRequest {
                    file_path: "src/auth/middleware.rs".to_string(),
                    claimed_by: "backend".to_string(),
                    task_id: Some(task.id),
                    branch: None,
                    reason: Some("fix panic".to_string()),
                    ttl_seconds: None,
                },
                "secret",
            ))
            .await
            .unwrap();
        let claim: FileClaimResponse = decode(claim_response).await;
        assert!(claim.warnings.is_empty());

        let second_claim_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/file-claims",
                FileClaimRequest {
                    file_path: "src/auth/middleware.rs".to_string(),
                    claimed_by: "frontend".to_string(),
                    task_id: None,
                    branch: None,
                    reason: None,
                    ttl_seconds: None,
                },
                "secret",
            ))
            .await
            .unwrap();
        let second_claim: FileClaimResponse = decode(second_claim_response).await;
        assert_eq!(second_claim.warnings.len(), 1);

        let finding_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/findings",
                FindingCreateRequest {
                    agent_id: "backend".to_string(),
                    kind: FindingKind::RootCause,
                    title: "Missing auth header panic".to_string(),
                    body: "Parser unwraps missing Authorization.".to_string(),
                    files: vec!["src/auth/middleware.rs".to_string()],
                    confidence: Confidence::High,
                },
                "secret",
            ))
            .await
            .unwrap();
        let finding: FindingRecord = decode(finding_response).await;
        assert_eq!(finding.kind, FindingKind::RootCause);

        let findings: Vec<FindingRecord> = decode(
            app.oneshot(get("/api/findings/search?q=Authorization"))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(findings.len(), 1);
    }

    #[tokio::test]
    async fn pipeline_models_slots_events_and_memory_work() {
        let app = test_app().await;
        let models: Vec<ModelRecord> =
            decode(app.clone().oneshot(get("/api/models")).await.unwrap()).await;
        assert!(!models.is_empty());

        let workflow_yaml = r#"
workflow:
  id: quick-fix
  name: Quick Fix
  profile: quality-first
  slots:
    architect:
      role: architect
      preferred:
        - runtime: claude-code
          model: claude-code/default
      required_capabilities: [architecture]
  steps:
    - architect.plan
"#;
        let response = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/pipelines",
                PipelineCreateRequest {
                    workflow_yaml: workflow_yaml.to_string(),
                    approve_assignments: true,
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let pipeline: PipelineRecord = decode(response).await;

        let slots: Vec<RoleSlot> = decode(
            app.clone()
                .oneshot(get(&format!("/api/pipelines/{}/slots", pipeline.id)))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(slots.len(), 1);

        let slot_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                &format!("/api/pipelines/{}/slots/architect", pipeline.id),
                SlotUpdateRequest {
                    status: SlotStatus::Working,
                    runtime_type: None,
                    model_id: None,
                    agent_id: Some("architect-agent".to_string()),
                    clear_assignment: false,
                },
                "secret",
            ))
            .await
            .unwrap();
        let updated_slot: RoleSlot = decode(slot_response).await;
        assert_eq!(updated_slot.status, SlotStatus::Working);
        assert_eq!(updated_slot.agent_id.as_deref(), Some("architect-agent"));

        let events: Vec<PipelineEventRecord> = decode(
            app.clone()
                .oneshot(get(&format!("/api/pipelines/{}/events", pipeline.id)))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(events.len(), 2);

        let status_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                &format!("/api/pipelines/{}/status", pipeline.id),
                PipelineStatusUpdateRequest {
                    status: PipelineStatus::Running,
                    completed: false,
                },
                "secret",
            ))
            .await
            .unwrap();
        let updated: PipelineRecord = decode(status_response).await;
        assert_eq!(updated.status, PipelineStatus::Running);

        let event_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                &format!("/api/pipelines/{}/events", pipeline.id),
                PipelineEventCreateRequest {
                    pipeline_id: pipeline.id,
                    agent_id: Some("architect".to_string()),
                    event_type: "step_completed".to_string(),
                    payload: serde_json::json!({ "step": "architect.plan" }),
                    correlation_id: None,
                    causation_id: None,
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(event_response.status(), StatusCode::CREATED);

        let artifact_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                &format!("/api/pipelines/{}/artifacts", pipeline.id),
                ArtifactCreateRequest {
                    pipeline_id: pipeline.id,
                    stage_name: "architect.plan".to_string(),
                    artifact_type: "runtime_output".to_string(),
                    content: "ok".to_string(),
                    created_by: "architect".to_string(),
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(artifact_response.status(), StatusCode::CREATED);

        let artifacts: Vec<agent_protocol::ArtifactRecord> = decode(
            app.clone()
                .oneshot(get(&format!("/api/pipelines/{}/artifacts", pipeline.id)))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(artifacts.len(), 1);

        let memory_response = app
            .clone()
            .oneshot(request(
                Method::PUT,
                &format!("/api/memory/{}/architect", pipeline.id),
                WorkingContextUpsertRequest {
                    summary: "handoff".to_string(),
                    key_decisions: serde_json::json!(["use existing hub"]),
                    active_files: vec!["crates/agent-hub/src/lib.rs".to_string()],
                },
                "secret",
            ))
            .await
            .unwrap();
        let memory: WorkingContext = decode(memory_response).await;
        assert_eq!(memory.summary, "handoff");
    }

    #[tokio::test]
    async fn stream_replay_uses_last_event_id() {
        let pool = test_pool().await;
        let first = insert_message(
            &pool,
            MessageCreateRequest {
                from: "frontend".to_string(),
                to: "backend".to_string(),
                kind: MessageKind::StatusUpdate,
                subject: "first".to_string(),
                body: "one".to_string(),
                thread_id: None,
                reply_to: None,
            },
        )
        .await
        .unwrap();
        let second = insert_message(
            &pool,
            MessageCreateRequest {
                from: "frontend".to_string(),
                to: "backend".to_string(),
                kind: MessageKind::StatusUpdate,
                subject: "second".to_string(),
                body: "two".to_string(),
                thread_id: None,
                reply_to: None,
            },
        )
        .await
        .unwrap();

        let replay = replay_messages(&pool, "backend", Some(first.id), None)
            .await
            .unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].id, second.id);
    }
}
