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
}
