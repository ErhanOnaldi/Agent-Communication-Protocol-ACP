use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use acp_protocol::{ErrorResponse, MessageRecord};
use axum::{
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tower_http::trace::TraceLayer;

mod db;
pub mod handlers;
mod migrations;

pub use migrations::init_db;

use handlers::{
    agents::{get_agent, health, heartbeat, list_agents, update_agent_status},
    analytics::get_pipeline_analytics,
    files::{create_file_claim, delete_file_claim, list_file_claims},
    findings::{create_finding, get_finding, list_findings, search_findings},
    mcp::{get_mcp_health, list_mcp, upsert_mcp},
    messages::{
        broadcast_message, close_thread, create_message, get_thread_detail, list_messages,
        list_threads, mark_read, message_to_role, reply_to_message, reply_to_thread,
    },
    models::{list_capability_scores, list_models, update_capability_score},
    pipelines::{
        create_artifact, create_context_compression, create_pipeline, create_pipeline_event,
        create_scheduler_decision, create_semantic_memory, create_step_metric, get_pipeline,
        get_working_context, list_context_compressions, list_pipeline_artifacts,
        list_pipeline_events, list_pipeline_slots, list_pipelines, list_scheduler_decisions,
        list_semantic_memory, search_semantic_memory, update_pipeline_slot, update_pipeline_status,
        upsert_working_context,
    },
    runtime::{interrupt_runtime, shutdown_runtime},
    stream::stream,
    tasks::{claim_task, create_task, done_task, get_task, list_tasks, update_task_status},
};

#[derive(Clone)]
pub struct HubState {
    pub(crate) inner: Arc<HubStateInner>,
}

pub(crate) struct HubStateInner {
    pub(crate) pool: SqlitePool,
    pub(crate) token: String,
    pub(crate) events: broadcast::Sender<MessageRecord>,
    pub(crate) rate_limits: Mutex<HashMap<String, RateBucket>>,
}

#[derive(Debug, Clone)]
pub(crate) struct RateBucket {
    pub(crate) window_started: Instant,
    pub(crate) count: u32,
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
            "/api/pipelines/:id/scheduler-decisions",
            post(create_scheduler_decision).get(list_scheduler_decisions),
        )
        .route(
            "/api/pipelines/:id/context-compressions",
            post(create_context_compression).get(list_context_compressions),
        )
        .route(
            "/api/pipelines/:id/semantic-memory",
            post(create_semantic_memory).get(list_semantic_memory),
        )
        .route(
            "/api/pipelines/:id/memory-search",
            get(search_semantic_memory),
        )
        .route(
            "/api/pipelines/:id/artifacts",
            post(create_artifact).get(list_pipeline_artifacts),
        )
        .route("/api/pipelines/:id/metrics", post(create_step_metric))
        .route("/api/analytics/pipelines/:id", get(get_pipeline_analytics))
        .route("/api/mcp", get(list_mcp).post(upsert_mcp))
        .route("/api/mcp/:name/health", get(get_mcp_health))
        .route("/api/runtime/:agent_id/interrupt", post(interrupt_runtime))
        .route("/api/runtime/:agent_id/shutdown", post(shutdown_runtime))
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

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub(crate) fn bad_request(message: impl ToString) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.to_string(),
        }
    }

    pub(crate) fn unauthorized(message: impl ToString) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.to_string(),
        }
    }

    pub(crate) fn not_found(message: impl ToString) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.to_string(),
        }
    }

    pub(crate) fn too_many_requests(message: impl ToString) -> Self {
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

pub(crate) fn authorize(state: &HubState, headers: &HeaderMap) -> Result<(), ApiError> {
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

pub(crate) fn check_rate_limit(state: &HubState, token: &str) -> Result<(), ApiError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use acp_protocol::{
        AgentStatus, ArtifactCreateRequest, BroadcastRequest, Confidence, FileClaimRequest,
        FindingCreateRequest, FindingKind, HeartbeatRequest, MessageKind, MessageRecord,
        ModelRecord, PipelineCreateRequest, PipelineEventCreateRequest, PipelineRecord,
        PipelineStatus, PipelineStatusUpdateRequest, RoleSlot, SlotStatus, SlotUpdateRequest,
        TaskCreateRequest, TaskRecord, ThreadDetail, ThreadStatus, UpdateAgentStatusRequest,
        WorkingContext, WorkingContextUpsertRequest,
    };
    use axum::{
        body::{to_bytes, Body},
        http::{header, Method, Request},
        response::Response,
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

    fn get_no_auth(uri: &str) -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri(uri)
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
        let agent: acp_protocol::AgentRecord = decode(response).await;
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
        let claim: acp_protocol::FileClaimResponse = decode(claim_response).await;
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
        let second_claim: acp_protocol::FileClaimResponse = decode(second_claim_response).await;
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
        let finding: acp_protocol::FindingRecord = decode(finding_response).await;
        assert_eq!(finding.kind, FindingKind::RootCause);

        let findings: Vec<acp_protocol::FindingRecord> = decode(
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

        let events: Vec<acp_protocol::PipelineEventRecord> = decode(
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

        let artifacts: Vec<acp_protocol::ArtifactRecord> = decode(
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
    async fn analytics_requires_authorization() {
        let app = test_app().await;
        let workflow_yaml = r#"
workflow:
  id: analytics-auth
  name: Analytics Auth
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
        let pipeline: PipelineRecord = decode(response).await;

        let unauthorized = app
            .oneshot(get_no_auth(&format!(
                "/api/analytics/pipelines/{}",
                pipeline.id
            )))
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn phase_three_records_and_runtime_controls_work() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_db(&pool).await.unwrap();
        let app = app(HubState::new(pool.clone(), "secret".to_string()));
        let workflow_yaml = r#"
workflow:
  id: advanced-records
  name: Advanced Records
  profile: quality-first
  slots:
    architect:
      role: architect
      preferred:
        - runtime: codex
          model: codex/default
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
        let pipeline: PipelineRecord = decode(response).await;

        let scheduler_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                &format!("/api/pipelines/{}/scheduler-decisions", pipeline.id),
                acp_protocol::SchedulerDecisionCreateRequest {
                    pipeline_id: pipeline.id,
                    role: "architect".to_string(),
                    runtime_type: acp_protocol::RuntimeType::Codex,
                    model_id: Some("codex/default".to_string()),
                    base_score: 0.8,
                    learned_delta: 0.1,
                    profile_boost: 0.1,
                    final_score: 1.0,
                    reason: "test".to_string(),
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(scheduler_response.status(), StatusCode::OK);

        let compression_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                &format!("/api/pipelines/{}/context-compressions", pipeline.id),
                acp_protocol::ContextCompressionCreateRequest {
                    pipeline_id: pipeline.id,
                    role: "architect".to_string(),
                    compressor: "deterministic".to_string(),
                    source_tokens: 10,
                    summary: "planned architecture".to_string(),
                    semantic_refs: vec!["architect.plan".to_string()],
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(compression_response.status(), StatusCode::OK);

        let semantic_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                &format!("/api/pipelines/{}/semantic-memory", pipeline.id),
                acp_protocol::SemanticMemoryCreateRequest {
                    pipeline_id: pipeline.id,
                    item_id: "architect.plan".to_string(),
                    content: "authentication token architecture".to_string(),
                    embedding_provider: "offline".to_string(),
                    embedding_model: "hashed".to_string(),
                    embedding: vec![0.1, 0.2],
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(semantic_response.status(), StatusCode::OK);

        let search: acp_protocol::SemanticSearchResponse = decode(
            app.clone()
                .oneshot(get(&format!(
                    "/api/pipelines/{}/memory-search?q=authentication",
                    pipeline.id
                )))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(search.results.len(), 1);

        let mcp_response = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/mcp",
                acp_protocol::McpServerRecord {
                    name: "docs".to_string(),
                    command: "docs-mcp".to_string(),
                    args: Vec::new(),
                    env: Default::default(),
                    working_dir: None,
                    mode: "shared".to_string(),
                    timeout_ms: Some(1000),
                    auto_start: true,
                    capabilities: vec!["docs".to_string()],
                },
                "secret",
            ))
            .await
            .unwrap();
        assert_eq!(mcp_response.status(), StatusCode::CREATED);
        let health: acp_protocol::McpHealth = decode(
            app.clone()
                .oneshot(get("/api/mcp/docs/health"))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(health.name, "docs");

        crate::db::upsert_runtime_handle(
            &pool,
            &acp_protocol::AgentHandle {
                agent_id: "agent-1".to_string(),
                pid: Some(42),
                runtime_type: acp_protocol::RuntimeType::Codex,
                started_at: chrono::Utc::now(),
                status: acp_protocol::RuntimeLifecycleStatus::Running,
            },
        )
        .await
        .unwrap();
        let interrupt: acp_protocol::RuntimeCommandResponse = decode(
            app.oneshot(request(
                Method::POST,
                "/api/runtime/agent-1/interrupt",
                serde_json::json!({}),
                "secret",
            ))
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(
            interrupt.status,
            acp_protocol::RuntimeLifecycleStatus::Interrupted
        );
    }

    #[tokio::test]
    async fn stream_replay_uses_last_event_id() {
        use crate::db::insert_message;
        use acp_protocol::MessageCreateRequest;
        use handlers::stream::replay_messages;

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
