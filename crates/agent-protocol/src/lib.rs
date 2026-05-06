use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Notice,
    Question,
    Answer,
    ContractChange,
    Handoff,
}

impl std::fmt::Display for MessageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Notice => "notice",
            Self::Question => "question",
            Self::Answer => "answer",
            Self::ContractChange => "contract_change",
            Self::Handoff => "handoff",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for MessageKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "notice" => Ok(Self::Notice),
            "question" => Ok(Self::Question),
            "answer" => Ok(Self::Answer),
            "contract_change" => Ok(Self::ContractChange),
            "handoff" => Ok(Self::Handoff),
            other => Err(format!("unsupported message kind: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatus {
    Unread,
    Read,
}

impl std::fmt::Display for MessageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unread => f.write_str("unread"),
            Self::Read => f.write_str("read"),
        }
    }
}

impl std::str::FromStr for MessageStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unread" => Ok(Self::Unread),
            "read" => Ok(Self::Read),
            other => Err(format!("unsupported message status: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub agent_id: String,
    pub role: String,
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub id: String,
    pub role: String,
    pub hostname: Option<String>,
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
pub struct ReplyRequest {
    pub from: String,
    pub body: String,
    pub subject: Option<String>,
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
}
