use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! string_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let value = match self {
                    $(Self::$variant => $value),+
                };
                f.write_str(value)
            }
        }

        impl std::str::FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    other => Err(format!("unsupported {}: {other}", stringify!($name))),
                }
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageKind {
    StatusUpdate,
    Proposal,
    Question,
    Answer,
    ContractChange,
    ReviewRequest,
    ReviewResult,
    Blocker,
    Handoff,
    Done,
    Finding,
    Decision,
    TaskUpdate,
    FileClaim,
    BranchUpdate,
    TestResult,
    Custom(String),
}

impl MessageKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::StatusUpdate => "status_update",
            Self::Proposal => "proposal",
            Self::Question => "question",
            Self::Answer => "answer",
            Self::ContractChange => "contract_change",
            Self::ReviewRequest => "review_request",
            Self::ReviewResult => "review_result",
            Self::Blocker => "blocker",
            Self::Handoff => "handoff",
            Self::Done => "done",
            Self::Finding => "finding",
            Self::Decision => "decision",
            Self::TaskUpdate => "task_update",
            Self::FileClaim => "file_claim",
            Self::BranchUpdate => "branch_update",
            Self::TestResult => "test_result",
            Self::Custom(value) => value.as_str(),
        }
    }
}

impl std::fmt::Display for MessageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MessageKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "status_update" => Ok(Self::StatusUpdate),
            "proposal" => Ok(Self::Proposal),
            "question" => Ok(Self::Question),
            "answer" => Ok(Self::Answer),
            "contract_change" => Ok(Self::ContractChange),
            "review_request" => Ok(Self::ReviewRequest),
            "review_result" => Ok(Self::ReviewResult),
            "blocker" => Ok(Self::Blocker),
            "handoff" => Ok(Self::Handoff),
            "done" => Ok(Self::Done),
            "finding" => Ok(Self::Finding),
            "decision" => Ok(Self::Decision),
            "task_update" => Ok(Self::TaskUpdate),
            "file_claim" => Ok(Self::FileClaim),
            "branch_update" => Ok(Self::BranchUpdate),
            "test_result" => Ok(Self::TestResult),
            custom if custom.starts_with("custom:") && custom.len() > "custom:".len() => {
                Ok(Self::Custom(custom.to_string()))
            }
            other => Err(format!(
                "unsupported message kind: {other}; use a standard kind or custom:<name>"
            )),
        }
    }
}

impl Serialize for MessageKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for MessageKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

string_enum!(MessageStatus {
    Unread => "unread",
    Read => "read",
});

string_enum!(AgentStatus {
    Online => "online",
    Idle => "idle",
    Investigating => "investigating",
    Working => "working",
    Blocked => "blocked",
    Reviewing => "reviewing",
    WaitingForReview => "waiting_for_review",
    Done => "done",
    Offline => "offline",
});

string_enum!(ThreadStatus {
    Open => "open",
    Closed => "closed",
});

string_enum!(TaskStatus {
    Open => "open",
    Proposed => "proposed",
    Claimed => "claimed",
    InProgress => "in_progress",
    Blocked => "blocked",
    NeedsReview => "needs_review",
    ChangesRequested => "changes_requested",
    Approved => "approved",
    Done => "done",
    Cancelled => "cancelled",
});

string_enum!(TaskPriority {
    Low => "low",
    Medium => "medium",
    High => "high",
    Urgent => "urgent",
});

string_enum!(FindingKind {
    RootCause => "root_cause",
    Bug => "bug",
    Risk => "risk",
    TestGap => "test_gap",
    ContractIssue => "contract_issue",
    ImplementationIdea => "implementation_idea",
    Regression => "regression",
    PerformanceIssue => "performance_issue",
    SecurityIssue => "security_issue",
    Question => "question",
});

string_enum!(Confidence {
    Low => "low",
    Medium => "medium",
    High => "high",
});

string_enum!(RuntimeType {
    ClaudeCode => "claude_code",
    Codex => "codex",
    Gemini => "gemini",
    Copilot => "copilot",
    Claudex => "claudex",
});

string_enum!(RuntimeHealth {
    Healthy => "healthy",
    Degraded => "degraded",
    RateLimited => "rate_limited",
    AuthExpired => "auth_expired",
    Crashed => "crashed",
    Missing => "missing",
});

string_enum!(SlotStatus {
    Empty => "empty",
    Assigned => "assigned",
    Active => "active",
    Working => "working",
    Waiting => "waiting",
    Vacant => "vacant",
    Disabled => "disabled",
});

string_enum!(SchedulerProfile {
    QualityFirst => "quality_first",
    BudgetFirst => "budget_first",
    SpeedFirst => "speed_first",
});

string_enum!(PipelineStatus {
    Pending => "pending",
    AwaitingApproval => "awaiting_approval",
    Running => "running",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});

string_enum!(RuntimeLifecycleStatus {
    Starting => "starting",
    Running => "running",
    Completed => "completed",
    Interrupted => "interrupted",
    Shutdown => "shutdown",
    Failed => "failed",
});

string_enum!(ModelTier {
    Free => "free",
    Cheap => "cheap",
    Standard => "standard",
    Premium => "premium",
    Local => "local",
    Unknown => "unknown",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPricing {
    pub input: Option<f64>,
    pub output: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRecord {
    pub id: String,
    pub name: String,
    pub runtime_source: String,
    pub tier: ModelTier,
    pub context_window: Option<i64>,
    pub pricing: ModelPricing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub base_url: String,
    pub api_key_env: String,
    #[serde(default)]
    pub models: Vec<ModelRecord>,
    #[serde(default)]
    pub embedding_model: Option<String>,
    #[serde(default)]
    pub embedding_base_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRecord {
    pub runtime_type: RuntimeType,
    pub name: String,
    pub binary: String,
    pub path: Option<String>,
    pub version: Option<String>,
    pub health: RuntimeHealth,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePreference {
    #[serde(deserialize_with = "deserialize_runtime_type")]
    pub runtime: RuntimeType,
    pub model: Option<String>,
    pub provider: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSlot {
    pub role: String,
    #[serde(default)]
    pub runtime_mode: Option<String>,
    #[serde(default)]
    pub preferred: Vec<RuntimePreference>,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkflowStep {
    Action(String),
    Parallel {
        parallel: Vec<String>,
    },
    /// Only runs if the previous step completed with Healthy status.
    Conditional {
        action: String,
        when_healthy: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowFailurePolicy {
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub overrides: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTimeouts {
    pub step_minutes: Option<u64>,
    pub pipeline_minutes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub id: String,
    pub name: String,
    #[serde(
        default = "default_scheduler_profile",
        deserialize_with = "deserialize_scheduler_profile"
    )]
    pub profile: SchedulerProfile,
    #[serde(default)]
    pub slots: std::collections::BTreeMap<String, WorkflowSlot>,
    #[serde(default)]
    pub steps: Vec<WorkflowStep>,
    #[serde(default)]
    pub failure: Option<WorkflowFailurePolicy>,
    #[serde(default)]
    pub timeouts: Option<WorkflowTimeouts>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowConfig {
    pub workflow: WorkflowDefinition,
}

fn default_scheduler_profile() -> SchedulerProfile {
    SchedulerProfile::QualityFirst
}

fn deserialize_runtime_type<'de, D>(deserializer: D) -> Result<RuntimeType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value
        .replace('-', "_")
        .parse()
        .map_err(serde::de::Error::custom)
}

fn deserialize_scheduler_profile<'de, D>(deserializer: D) -> Result<SchedulerProfile, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value
        .replace('-', "_")
        .parse()
        .map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineCreateRequest {
    pub workflow_yaml: String,
    #[serde(default)]
    pub approve_assignments: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatusUpdateRequest {
    pub status: PipelineStatus,
    #[serde(default)]
    pub completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRecord {
    pub id: Uuid,
    pub workflow_yaml: String,
    pub status: PipelineStatus,
    pub profile: SchedulerProfile,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleSlot {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub role: String,
    pub runtime_type: Option<RuntimeType>,
    pub model_id: Option<String>,
    pub agent_id: Option<String>,
    pub status: SlotStatus,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotUpdateRequest {
    pub status: SlotStatus,
    pub runtime_type: Option<RuntimeType>,
    pub model_id: Option<String>,
    pub agent_id: Option<String>,
    #[serde(default)]
    pub clear_assignment: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineEventRecord {
    pub id: i64,
    pub pipeline_id: Uuid,
    pub agent_id: Option<String>,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineEvent {
    RuntimeSpawned {
        runtime_type: RuntimeType,
        model_id: Option<String>,
    },
    TaskAssigned {
        role: String,
        step: String,
    },
    PatchApplied {
        files: Vec<String>,
    },
    RateLimitHit {
        role: String,
        runtime_type: RuntimeType,
    },
    AuthExpired {
        role: String,
        runtime_type: RuntimeType,
    },
    RuntimeCrash {
        role: String,
        runtime_type: RuntimeType,
        message: String,
    },
    MergeConflict {
        branch: String,
        files: Vec<String>,
    },
    ToolFailure {
        tool: String,
        message: String,
    },
    ContextOverflow {
        role: String,
    },
    ValidationFailure {
        command: Vec<String>,
        stderr: String,
    },
    SlotLifecycle {
        role: String,
        status: SlotStatus,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineEventCreateRequest {
    pub pipeline_id: Uuid,
    pub agent_id: Option<String>,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactCreateRequest {
    pub pipeline_id: Uuid,
    pub stage_name: String,
    pub artifact_type: String,
    pub content: String,
    pub created_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub stage_name: String,
    pub artifact_type: String,
    pub content: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingContext {
    pub pipeline_id: Uuid,
    pub role: String,
    pub summary: String,
    #[serde(default)]
    pub key_decisions: serde_json::Value,
    #[serde(default)]
    pub active_files: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingContextUpsertRequest {
    pub summary: String,
    #[serde(default)]
    pub key_decisions: serde_json::Value,
    #[serde(default)]
    pub active_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSpec {
    pub agent_id: String,
    pub role: String,
    pub runtime_type: RuntimeType,
    pub model: Option<String>,
    pub task: String,
    pub workspace: Option<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub mcp_servers: Vec<McpServerRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHandle {
    pub agent_id: String,
    pub pid: Option<u32>,
    pub runtime_type: RuntimeType,
    pub started_at: DateTime<Utc>,
    pub status: RuntimeLifecycleStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHandle {
    pub task_id: Uuid,
    pub agent_id: String,
    pub pid: Option<u32>,
    pub runtime_type: RuntimeType,
    pub started_at: DateTime<Utc>,
    pub status: RuntimeLifecycleStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCommandResponse {
    pub agent_id: String,
    pub status: RuntimeLifecycleStatus,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeOutput {
    pub status: RuntimeHealth,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub stream_events: Vec<RuntimeStreamEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStreamEvent {
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityScoreRecord {
    pub runtime_type: RuntimeType,
    pub model_id: String,
    pub capability: String,
    pub success_count: i64,
    pub failure_count: i64,
    #[serde(default)]
    pub last_updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityScoreUpdateRequest {
    pub runtime_type: RuntimeType,
    pub model_id: String,
    pub capability: String,
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub latency_ms: Option<u64>,
    #[serde(default)]
    pub had_conflict: bool,
    #[serde(default)]
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerRecord {
    pub name: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub working_dir: Option<String>,
    pub mode: String,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub auto_start: bool,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpHealth {
    pub name: String,
    pub status: RuntimeHealth,
    pub pid: Option<u32>,
    pub message: Option<String>,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerDecision {
    pub id: i64,
    pub pipeline_id: Uuid,
    pub role: String,
    pub runtime_type: RuntimeType,
    pub model_id: Option<String>,
    pub base_score: f64,
    pub learned_delta: f64,
    pub profile_boost: f64,
    pub final_score: f64,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerDecisionCreateRequest {
    pub pipeline_id: Uuid,
    pub role: String,
    pub runtime_type: RuntimeType,
    pub model_id: Option<String>,
    pub base_score: f64,
    pub learned_delta: f64,
    pub profile_boost: f64,
    pub final_score: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextCompressionRecord {
    pub id: i64,
    pub pipeline_id: Uuid,
    pub role: String,
    pub compressor: String,
    pub source_tokens: i64,
    pub summary: String,
    pub semantic_refs: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextCompressionCreateRequest {
    pub pipeline_id: Uuid,
    pub role: String,
    pub compressor: String,
    pub source_tokens: i64,
    pub summary: String,
    #[serde(default)]
    pub semantic_refs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticMemoryRecord {
    pub id: i64,
    pub pipeline_id: Uuid,
    pub item_id: String,
    pub content: String,
    pub embedding_provider: String,
    pub embedding_model: String,
    pub embedding: Vec<f32>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticMemoryCreateRequest {
    pub pipeline_id: Uuid,
    pub item_id: String,
    pub content: String,
    pub embedding_provider: String,
    pub embedding_model: String,
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticSearchResponse {
    pub pipeline_id: Uuid,
    pub query: String,
    pub results: Vec<SemanticMemoryRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub agent_id: String,
    pub role: String,
    pub hostname: Option<String>,
    pub status: Option<AgentStatus>,
    pub current_task: Option<String>,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateAgentStatusRequest {
    pub status: AgentStatus,
    pub current_task: Option<String>,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub id: String,
    pub role: String,
    pub hostname: Option<String>,
    pub status: AgentStatus,
    pub current_task: Option<String>,
    pub branch: Option<String>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageCreateRequest {
    pub from: String,
    pub to: String,
    pub kind: MessageKind,
    pub subject: String,
    pub body: String,
    pub thread_id: Option<Uuid>,
    pub reply_to: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastRequest {
    pub from: String,
    pub kind: MessageKind,
    pub subject: String,
    pub body: String,
    pub exclude_self: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleMessageRequest {
    pub from: String,
    pub role: String,
    pub kind: MessageKind,
    pub subject: String,
    pub body: String,
    pub exclude_self: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyRequest {
    pub from: String,
    pub body: String,
    pub subject: Option<String>,
    pub thread_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRecord {
    pub id: Uuid,
    pub from_agent: String,
    pub to_agent: String,
    pub kind: MessageKind,
    pub subject: String,
    pub body: String,
    pub thread_id: Uuid,
    pub reply_to: Option<Uuid>,
    pub status: MessageStatus,
    pub created_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadRecord {
    pub id: Uuid,
    pub subject: String,
    pub status: ThreadStatus,
    pub summary: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub message_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadDetail {
    pub thread: ThreadRecord,
    pub messages: Vec<MessageRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCreateRequest {
    pub title: String,
    pub body: String,
    pub priority: Option<TaskPriority>,
    pub owner: Option<String>,
    pub branch: Option<String>,
    pub created_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusRequest {
    pub status: TaskStatus,
    pub body: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClaimRequest {
    pub agent_id: String,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: Uuid,
    pub title: String,
    pub body: String,
    pub status: TaskStatus,
    pub owner: Option<String>,
    pub priority: TaskPriority,
    pub branch: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileClaimRequest {
    pub file_path: String,
    pub claimed_by: String,
    pub task_id: Option<Uuid>,
    pub branch: Option<String>,
    pub reason: Option<String>,
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileClaimRecord {
    pub id: Uuid,
    pub file_path: String,
    pub claimed_by: String,
    pub task_id: Option<Uuid>,
    pub branch: Option<String>,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileClaimResponse {
    pub claim: FileClaimRecord,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindingCreateRequest {
    pub agent_id: String,
    pub kind: FindingKind,
    pub title: String,
    pub body: String,
    pub files: Vec<String>,
    pub confidence: Confidence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindingRecord {
    pub id: Uuid,
    pub agent_id: String,
    pub kind: FindingKind,
    pub title: String,
    pub body: String,
    pub files: Vec<String>,
    pub confidence: Confidence,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDefinition {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub system_prompt: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepMetricCreateRequest {
    pub pipeline_id: Uuid,
    pub step_name: String,
    pub role: String,
    pub runtime_type: Option<RuntimeType>,
    pub model_id: Option<String>,
    pub latency_ms: Option<u64>,
    pub health: RuntimeHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepMetricsRecord {
    pub id: i64,
    pub pipeline_id: Uuid,
    pub step_name: String,
    pub role: String,
    pub runtime_type: Option<RuntimeType>,
    pub model_id: Option<String>,
    pub latency_ms: Option<u64>,
    pub health: RuntimeHealth,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineAnalyticsResponse {
    pub pipeline_id: Uuid,
    pub total_steps: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub p50_latency_ms: Option<u64>,
    pub p95_latency_ms: Option<u64>,
    pub steps: Vec<StepMetricsRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerInsights {
    pub role: String,
    pub runtime_type: RuntimeType,
    pub model_id: Option<String>,
    pub base_score: f64,
    pub learned_delta: f64,
    pub profile_boost: f64,
    pub final_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_kind_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&MessageKind::ContractChange).unwrap(),
            "\"contract_change\""
        );
        assert_eq!(
            "question".parse::<MessageKind>().unwrap(),
            MessageKind::Question
        );
    }

    #[test]
    fn custom_message_kind_requires_prefix() {
        assert_eq!(
            "custom:handover_note".parse::<MessageKind>().unwrap(),
            MessageKind::Custom("custom:handover_note".to_string())
        );
        assert!("handover_note".parse::<MessageKind>().is_err());
    }

    #[test]
    fn workflow_yaml_accepts_plan_values() {
        let yaml = r#"
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
  steps:
    - architect.plan
"#;
        let workflow: WorkflowConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(workflow.workflow.profile, SchedulerProfile::QualityFirst);
        assert_eq!(
            workflow.workflow.slots["architect"].preferred[0].runtime,
            RuntimeType::ClaudeCode
        );
    }
}
