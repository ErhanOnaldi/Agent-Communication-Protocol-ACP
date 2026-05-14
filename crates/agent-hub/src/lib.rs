use std::{convert::Infallible, sync::Arc, time::Duration};

use agent_protocol::{
    AgentRecord, AgentStatus, BroadcastRequest, ErrorResponse, FileClaimRecord, FileClaimRequest,
    FileClaimResponse, FindingCreateRequest, FindingKind, FindingRecord, HeartbeatRequest,
    MessageCreateRequest, MessageKind, MessageRecord, MessageStatus, ReplyRequest,
    RoleMessageRequest, TaskClaimRequest, TaskCreateRequest, TaskPriority, TaskRecord, TaskStatus,
    TaskStatusRequest, ThreadDetail, ThreadRecord, UpdateAgentStatusRequest,
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
}

impl HubState {
    pub fn new(pool: SqlitePool, token: String) -> Self {
        let (events, _) = broadcast::channel(512);
        Self {
            inner: Arc::new(HubStateInner {
                pool,
                token,
                events,
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
}

async fn stream(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    authorize(&state, &headers)?;
    let mut rx = state.inner.events.subscribe();
    let agent_id = query.agent_id;
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(message) if message.to_agent == agent_id => {
                    let data = serde_json::to_string(&message).unwrap_or_else(|_| "{}".to_string());
                    yield Ok(Event::default().event("message").data(data));
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
        Confidence, FileClaimRequest, FindingCreateRequest, FindingKind, HeartbeatRequest,
        TaskCreateRequest, ThreadStatus,
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
}
