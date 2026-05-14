use std::convert::Infallible;
use std::time::Duration;

use agent_protocol::MessageRecord;
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::stream::Stream;
use serde::Deserialize;
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    authorize,
    db::row_to_message,
    ApiError, HubState,
};

#[derive(Debug, Deserialize)]
pub(crate) struct StreamQuery {
    agent_id: String,
    last_event_id: Option<Uuid>,
    since: Option<String>,
}

pub(crate) async fn stream(
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

pub(crate) async fn replay_messages(
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
