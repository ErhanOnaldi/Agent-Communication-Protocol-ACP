use acp_protocol::{
    BroadcastRequest, MessageCreateRequest, MessageKind, MessageRecord, MessageStatus,
    ReplyRequest, RoleMessageRequest, ThreadDetail, ThreadRecord,
};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    authorize,
    db::{
        active_agents, emit_message, get_message, get_thread, insert_message, row_to_agent,
        row_to_message, row_to_thread,
    },
    ApiError, HubState,
};

pub(crate) async fn create_message(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<MessageCreateRequest>,
) -> Result<(StatusCode, Json<MessageRecord>), ApiError> {
    authorize(&state, &headers)?;
    let message = insert_message(state.pool(), req).await?;
    emit_message(&state, &message, "message_sent").await?;
    Ok((StatusCode::CREATED, Json(message)))
}

pub(crate) async fn broadcast_message(
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

pub(crate) async fn message_to_role(
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
pub(crate) struct ListMessageQuery {
    agent_id: String,
    status: Option<String>,
    kind: Option<String>,
}

pub(crate) async fn list_messages(
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

pub(crate) async fn mark_read(
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

pub(crate) async fn reply_to_message(
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
pub(crate) struct ListThreadQuery {
    agent_id: Option<String>,
}

pub(crate) async fn list_threads(
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

pub(crate) async fn get_thread_detail(
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

pub(crate) async fn reply_to_thread(
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

pub(crate) async fn close_thread(
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
