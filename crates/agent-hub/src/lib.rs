use std::{convert::Infallible, sync::Arc, time::Duration};

use agent_protocol::{
    AgentRecord, ErrorResponse, HeartbeatRequest, MessageCreateRequest, MessageKind, MessageRecord,
    MessageStatus, ReplyRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
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
        let (events, _) = broadcast::channel(256);
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
        .route("/api/agents/heartbeat", post(heartbeat))
        .route("/api/messages", post(create_message).get(list_messages))
        .route("/api/messages/:id/read", post(mark_read))
        .route("/api/messages/:id/reply", post(reply))
        .route("/api/stream", get(stream))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn init_db(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS agents (
            id TEXT PRIMARY KEY,
            role TEXT NOT NULL,
            hostname TEXT NULL,
            last_seen_at TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
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
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

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
    sqlx::query(
        r#"
        INSERT INTO agents (id, role, hostname, last_seen_at)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(id) DO UPDATE SET
            role = excluded.role,
            hostname = excluded.hostname,
            last_seen_at = excluded.last_seen_at
        "#,
    )
    .bind(&req.agent_id)
    .bind(&req.role)
    .bind(&req.hostname)
    .bind(now.to_rfc3339())
    .execute(state.pool())
    .await?;
    write_event(
        state.pool(),
        &req.agent_id,
        "heartbeat",
        serde_json::json!({ "role": req.role }),
    )
    .await?;
    Ok(Json(AgentRecord {
        id: req.agent_id,
        role: req.role,
        hostname: req.hostname,
        last_seen_at: now,
    }))
}

async fn create_message(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(req): Json<MessageCreateRequest>,
) -> Result<(StatusCode, Json<MessageRecord>), ApiError> {
    authorize(&state, &headers)?;
    let message = insert_message(state.pool(), req).await?;
    write_event(
        state.pool(),
        &message.to_agent,
        "message_created",
        serde_json::to_value(&message).unwrap_or_default(),
    )
    .await?;
    let _ = state.inner.events.send(message.clone());
    Ok((StatusCode::CREATED, Json(message)))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    agent_id: String,
    status: Option<String>,
    kind: Option<String>,
}

async fn list_messages(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
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
    let message = get_message(state.pool(), id).await?;
    Ok(Json(message))
}

async fn reply(
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
    let _ = state.inner.events.send(message.clone());
    Ok((StatusCode::CREATED, Json(message)))
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

fn row_to_message(row: SqliteRow) -> Result<MessageRecord, ApiError> {
    let kind: String = row.try_get("kind")?;
    let status: String = row.try_get("status")?;
    let created_at: String = row.try_get("created_at")?;
    let read_at: Option<String> = row.try_get("read_at")?;
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
        created_at: parse_time(created_at)?,
        read_at: read_at.map(parse_time).transpose()?,
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
    use agent_protocol::{HeartbeatRequest, MessageCreateRequest};
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
                },
                "wrong",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn agent_can_register_send_reply_and_mark_read() {
        let app = test_app().await;

        let heartbeat = request(
            Method::POST,
            "/api/agents/heartbeat",
            HeartbeatRequest {
                agent_id: "frontend".to_string(),
                role: "frontend".to_string(),
                hostname: Some("macbook".to_string()),
            },
            "secret",
        );
        assert_eq!(
            app.clone().oneshot(heartbeat).await.unwrap().status(),
            StatusCode::OK
        );

        let response = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/messages",
                MessageCreateRequest {
                    from: "frontend".to_string(),
                    to: "backend".to_string(),
                    kind: MessageKind::Question,
                    subject: "Payload".to_string(),
                    body: "Ready?".to_string(),
                    thread_id: None,
                    reply_to: None,
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let message: MessageRecord = decode(response).await;
        assert_eq!(message.thread_id, message.id);

        let inbox_req = Request::builder()
            .method(Method::GET)
            .uri("/api/messages?agent_id=backend&status=unread")
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Body::empty())
            .unwrap();
        let inbox_response = app.clone().oneshot(inbox_req).await.unwrap();
        let inbox: Vec<MessageRecord> = decode(inbox_response).await;
        assert_eq!(inbox.len(), 1);

        let reply_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                &format!("/api/messages/{}/reply", message.id),
                ReplyRequest {
                    from: "backend".to_string(),
                    body: "Ready.".to_string(),
                    subject: None,
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(reply_response.status(), StatusCode::CREATED);
        let reply: MessageRecord = decode(reply_response).await;
        assert_eq!(reply.kind, MessageKind::Answer);
        assert_eq!(reply.thread_id, message.thread_id);
        assert_eq!(reply.reply_to, Some(message.id));

        let read_req = request(
            Method::POST,
            &format!("/api/messages/{}/read", message.id),
            serde_json::json!({}),
            "secret",
        );
        let read_response = app.clone().oneshot(read_req).await.unwrap();
        let read_message: MessageRecord = decode(read_response).await;
        assert_eq!(read_message.status, MessageStatus::Read);

        let inbox_req = Request::builder()
            .method(Method::GET)
            .uri("/api/messages?agent_id=backend&status=unread")
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Body::empty())
            .unwrap();
        let inbox: Vec<MessageRecord> = decode(app.oneshot(inbox_req).await.unwrap()).await;
        assert!(inbox.is_empty());
    }
}
