use std::time::Duration;

use agent_protocol::{
    AgentRecord, BroadcastRequest, FileClaimRecord, FileClaimRequest, FileClaimResponse,
    FindingCreateRequest, FindingRecord, HeartbeatRequest, MessageCreateRequest, MessageKind,
    MessageRecord, MessageStatus, ReplyRequest, RoleMessageRequest, TaskClaimRequest,
    TaskCreateRequest, TaskRecord, TaskStatusRequest, ThreadDetail, ThreadRecord,
    UpdateAgentStatusRequest,
};
use anyhow::{bail, Context};
use futures_util::StreamExt;
use reqwest::Client;
use uuid::Uuid;

pub struct AgentClient {
    base_url: String,
    token: String,
    client: Client,
}

impl AgentClient {
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> anyhow::Result<Self> {
        let token = token.into();
        if token.trim().is_empty() {
            bail!("AGENT_TOKEN cannot be empty");
        }
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token,
            client: Client::new(),
        })
    }

    pub async fn register(&self, req: &HeartbeatRequest) -> anyhow::Result<AgentRecord> {
        self.post("/api/agents/heartbeat", req).await
    }

    pub async fn agents(&self) -> anyhow::Result<Vec<AgentRecord>> {
        self.get("/api/agents").await
    }

    pub async fn agent(&self, agent_id: &str) -> anyhow::Result<AgentRecord> {
        self.get(&format!("/api/agents/{agent_id}")).await
    }

    pub async fn update_status(
        &self,
        agent_id: &str,
        req: &UpdateAgentStatusRequest,
    ) -> anyhow::Result<AgentRecord> {
        self.post(&format!("/api/agents/{agent_id}/status"), req)
            .await
    }

    pub async fn send(&self, req: &MessageCreateRequest) -> anyhow::Result<MessageRecord> {
        self.post("/api/messages", req).await
    }

    pub async fn broadcast(&self, req: &BroadcastRequest) -> anyhow::Result<Vec<MessageRecord>> {
        self.post("/api/messages/broadcast", req).await
    }

    pub async fn send_to_role(
        &self,
        req: &RoleMessageRequest,
    ) -> anyhow::Result<Vec<MessageRecord>> {
        self.post(&format!("/api/messages/to-role/{}", req.role), req)
            .await
    }

    pub async fn reply(&self, id: Uuid, req: &ReplyRequest) -> anyhow::Result<MessageRecord> {
        self.post(&format!("/api/messages/{id}/reply"), req).await
    }

    pub async fn reply_to_thread(
        &self,
        thread_id: Uuid,
        req: &ReplyRequest,
    ) -> anyhow::Result<MessageRecord> {
        self.post(&format!("/api/threads/{thread_id}/reply"), req)
            .await
    }

    pub async fn mark_read(&self, id: Uuid) -> anyhow::Result<MessageRecord> {
        self.post(&format!("/api/messages/{id}/read"), &serde_json::json!({}))
            .await
    }

    pub async fn inbox(
        &self,
        agent_id: &str,
        status: Option<MessageStatus>,
        kind: Option<MessageKind>,
    ) -> anyhow::Result<Vec<MessageRecord>> {
        let mut path = format!("/api/messages?agent_id={agent_id}");
        if let Some(status) = status {
            path.push_str("&status=");
            path.push_str(&status.to_string());
        }
        if let Some(kind) = kind {
            path.push_str("&kind=");
            path.push_str(&kind.to_string());
        }
        self.get(&path).await
    }

    pub async fn threads(&self, agent_id: Option<&str>) -> anyhow::Result<Vec<ThreadRecord>> {
        let path = agent_id
            .map(|id| format!("/api/threads?agent_id={id}"))
            .unwrap_or_else(|| "/api/threads".to_string());
        self.get(&path).await
    }

    pub async fn thread(&self, id: Uuid) -> anyhow::Result<ThreadDetail> {
        self.get(&format!("/api/threads/{id}")).await
    }

    pub async fn close_thread(&self, id: Uuid) -> anyhow::Result<ThreadRecord> {
        self.post(&format!("/api/threads/{id}/close"), &serde_json::json!({}))
            .await
    }

    pub async fn create_task(&self, req: &TaskCreateRequest) -> anyhow::Result<TaskRecord> {
        self.post("/api/tasks", req).await
    }

    pub async fn tasks(&self) -> anyhow::Result<Vec<TaskRecord>> {
        self.get("/api/tasks").await
    }

    pub async fn task(&self, id: Uuid) -> anyhow::Result<TaskRecord> {
        self.get(&format!("/api/tasks/{id}")).await
    }

    pub async fn claim_task(&self, id: Uuid, req: &TaskClaimRequest) -> anyhow::Result<TaskRecord> {
        self.post(&format!("/api/tasks/{id}/claim"), req).await
    }

    pub async fn update_task(
        &self,
        id: Uuid,
        req: &TaskStatusRequest,
    ) -> anyhow::Result<TaskRecord> {
        self.post(&format!("/api/tasks/{id}/status"), req).await
    }

    pub async fn done_task(&self, id: Uuid, req: &TaskStatusRequest) -> anyhow::Result<TaskRecord> {
        self.post(&format!("/api/tasks/{id}/done"), req).await
    }

    pub async fn claim_file(&self, req: &FileClaimRequest) -> anyhow::Result<FileClaimResponse> {
        self.post("/api/file-claims", req).await
    }

    pub async fn file_claims(&self, path: Option<&str>) -> anyhow::Result<Vec<FileClaimRecord>> {
        let query = path.map(|path| format!("?path={path}")).unwrap_or_default();
        self.get(&format!("/api/file-claims{query}")).await
    }

    pub async fn release_file_claim(&self, id: Uuid) -> anyhow::Result<serde_json::Value> {
        self.delete(&format!("/api/file-claims/{id}")).await
    }

    pub async fn create_finding(
        &self,
        req: &FindingCreateRequest,
    ) -> anyhow::Result<FindingRecord> {
        self.post("/api/findings", req).await
    }

    pub async fn findings(&self, query: Option<&str>) -> anyhow::Result<Vec<FindingRecord>> {
        let path = query
            .map(|q| format!("/api/findings/search?q={q}"))
            .unwrap_or_else(|| "/api/findings".to_string());
        self.get(&path).await
    }

    pub async fn finding(&self, id: Uuid) -> anyhow::Result<FindingRecord> {
        self.get(&format!("/api/findings/{id}")).await
    }

    pub async fn watch<F>(
        &self,
        agent_id: &str,
        wait: Option<(Option<MessageKind>, Duration)>,
        mut on_message: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(MessageRecord) -> anyhow::Result<bool>,
    {
        let response = self
            .client
            .get(format!(
                "{}/api/stream?agent_id={}",
                self.base_url, agent_id
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|err| friendly_request_error(err, &self.base_url))?;
        if !response.status().is_success() {
            bail!("hub rejected stream request: {}", response.text().await?);
        }
        let deadline = wait
            .as_ref()
            .map(|(_, duration)| tokio::time::Instant::now() + *duration);
        let expected_kind = wait.and_then(|(kind, _)| kind);
        let mut stream = response.bytes_stream();
        let mut buffer = String::new();

        loop {
            if let Some(deadline) = deadline {
                tokio::select! {
                    chunk = stream.next() => {
                        if !handle_stream_chunk(chunk, &mut buffer, expected_kind.clone(), &mut on_message)? {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => bail!("timed out waiting for message"),
                }
            } else if !handle_stream_chunk(stream.next().await, &mut buffer, None, &mut on_message)?
            {
                return Ok(());
            }
        }
    }

    async fn get<R>(&self, path: &str) -> anyhow::Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|err| friendly_request_error(err, &self.base_url))?;
        decode_response(response).await
    }

    async fn post<T, R>(&self, path: &str, body: &T) -> anyhow::Result<R>
    where
        T: serde::Serialize + ?Sized,
        R: serde::de::DeserializeOwned,
    {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|err| friendly_request_error(err, &self.base_url))?;
        decode_response(response).await
    }

    async fn delete<R>(&self, path: &str) -> anyhow::Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        let response = self
            .client
            .delete(format!("{}{}", self.base_url, path))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|err| friendly_request_error(err, &self.base_url))?;
        decode_response(response).await
    }
}

async fn decode_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> anyhow::Result<T> {
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        if status == reqwest::StatusCode::UNAUTHORIZED {
            bail!("authentication failed: check AGENT_TOKEN ({text})");
        }
        bail!("hub request failed ({status}): {text}");
    }
    Ok(serde_json::from_str(&text).with_context(|| format!("invalid response: {text}"))?)
}

fn friendly_request_error(err: reqwest::Error, base_url: &str) -> anyhow::Error {
    if err.is_connect() || err.is_timeout() {
        anyhow::anyhow!("cannot reach agent hub at {base_url}: {err}")
    } else {
        anyhow::anyhow!(err)
    }
}

fn handle_stream_chunk<F>(
    chunk: Option<Result<bytes::Bytes, reqwest::Error>>,
    buffer: &mut String,
    expected_kind: Option<MessageKind>,
    on_message: &mut F,
) -> anyhow::Result<bool>
where
    F: FnMut(MessageRecord) -> anyhow::Result<bool>,
{
    let Some(chunk) = chunk else {
        return Ok(false);
    };
    let chunk = chunk?;
    buffer.push_str(std::str::from_utf8(&chunk)?);
    while let Some(pos) = buffer.find("\n\n") {
        let frame = buffer[..pos].to_string();
        buffer.drain(..pos + 2);
        for line in frame.lines() {
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();
            if data.is_empty() || data == "keep-alive" || data == "{}" {
                continue;
            }
            let message: MessageRecord = serde_json::from_str(data)?;
            if expected_kind
                .as_ref()
                .is_some_and(|kind| kind != &message.kind)
            {
                continue;
            }
            return on_message(message);
        }
    }
    Ok(true)
}
