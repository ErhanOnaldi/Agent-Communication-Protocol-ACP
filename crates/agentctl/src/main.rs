use std::time::Duration;

use agent_protocol::{
    HeartbeatRequest, MessageCreateRequest, MessageKind, MessageRecord, MessageStatus, ReplyRequest,
};
use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use reqwest::Client;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "agentctl", about = "CLI client for LAN Agent Messenger")]
struct Cli {
    #[arg(long, env = "AGENT_HUB_URL", default_value = "http://127.0.0.1:8787")]
    hub_url: String,
    #[arg(long, env = "AGENT_TOKEN")]
    token: String,
    #[arg(long, env = "AGENT_ID")]
    agent_id: Option<String>,
    #[arg(long, env = "AGENT_ROLE")]
    agent_role: Option<String>,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Register {
        #[arg(long)]
        hostname: Option<String>,
    },
    Send {
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "notice")]
        kind: MessageKind,
        #[arg(long)]
        subject: String,
        #[arg(long)]
        body: String,
        #[arg(long)]
        thread_id: Option<Uuid>,
        #[arg(long)]
        reply_to: Option<Uuid>,
    },
    Ask {
        #[arg(long)]
        to: String,
        #[arg(long)]
        subject: String,
        #[arg(long)]
        body: String,
    },
    Reply {
        #[arg(long = "message-id")]
        message_id: Uuid,
        #[arg(long)]
        body: String,
        #[arg(long)]
        subject: Option<String>,
    },
    Inbox {
        #[arg(long)]
        unread: bool,
        #[arg(long)]
        kind: Option<MessageKind>,
    },
    MarkRead {
        #[arg(long = "message-id")]
        message_id: Uuid,
    },
    Watch,
    Wait {
        #[arg(long)]
        kind: Option<MessageKind>,
        #[arg(long, default_value = "30m")]
        timeout: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = ApiClient::new(cli.hub_url, cli.token)?;

    match cli.command {
        Command::Register { hostname } => {
            let agent_id = required(cli.agent_id, "AGENT_ID or --agent-id")?;
            let role = required(cli.agent_role, "AGENT_ROLE or --agent-role")?;
            let record = client.register(&agent_id, &role, hostname).await?;
            print_value(
                cli.json,
                &record,
                &format!("registered {} ({})", record.id, record.role),
            )?;
        }
        Command::Send {
            to,
            kind,
            subject,
            body,
            thread_id,
            reply_to,
        } => {
            let from = required(cli.agent_id, "AGENT_ID or --agent-id")?;
            let message = client
                .send(MessageCreateRequest {
                    from,
                    to,
                    kind,
                    subject,
                    body,
                    thread_id,
                    reply_to,
                })
                .await?;
            print_message(cli.json, &message)?;
        }
        Command::Ask { to, subject, body } => {
            let from = required(cli.agent_id, "AGENT_ID or --agent-id")?;
            let message = client
                .send(MessageCreateRequest {
                    from,
                    to,
                    kind: MessageKind::Question,
                    subject,
                    body,
                    thread_id: None,
                    reply_to: None,
                })
                .await?;
            print_message(cli.json, &message)?;
        }
        Command::Reply {
            message_id,
            body,
            subject,
        } => {
            let from = required(cli.agent_id, "AGENT_ID or --agent-id")?;
            let message = client
                .reply(
                    message_id,
                    ReplyRequest {
                        from,
                        body,
                        subject,
                    },
                )
                .await?;
            print_message(cli.json, &message)?;
        }
        Command::Inbox { unread, kind } => {
            let agent_id = required(cli.agent_id, "AGENT_ID or --agent-id")?;
            let status = unread.then_some(MessageStatus::Unread);
            let messages = client.inbox(&agent_id, status, kind).await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&messages)?);
            } else if messages.is_empty() {
                println!("inbox empty");
            } else {
                for message in messages {
                    print_message(false, &message)?;
                }
            }
        }
        Command::MarkRead { message_id } => {
            let message = client.mark_read(message_id).await?;
            print_message(cli.json, &message)?;
        }
        Command::Watch => {
            let agent_id = required(cli.agent_id, "AGENT_ID or --agent-id")?;
            client.watch(&agent_id, cli.json, None).await?;
        }
        Command::Wait { kind, timeout } => {
            let agent_id = required(cli.agent_id, "AGENT_ID or --agent-id")?;
            let timeout = parse_duration(&timeout)?;
            client
                .watch(&agent_id, cli.json, Some((kind, timeout)))
                .await?;
        }
    }
    Ok(())
}

struct ApiClient {
    base_url: String,
    token: String,
    client: Client,
}

impl ApiClient {
    fn new(base_url: String, token: String) -> anyhow::Result<Self> {
        if token.trim().is_empty() {
            bail!("AGENT_TOKEN cannot be empty");
        }
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
            client: Client::new(),
        })
    }

    async fn register(
        &self,
        agent_id: &str,
        role: &str,
        hostname: Option<String>,
    ) -> anyhow::Result<agent_protocol::AgentRecord> {
        self.post(
            "/api/agents/heartbeat",
            &HeartbeatRequest {
                agent_id: agent_id.to_string(),
                role: role.to_string(),
                hostname,
            },
        )
        .await
    }

    async fn send(&self, request: MessageCreateRequest) -> anyhow::Result<MessageRecord> {
        self.post("/api/messages", &request).await
    }

    async fn reply(&self, id: Uuid, request: ReplyRequest) -> anyhow::Result<MessageRecord> {
        self.post(&format!("/api/messages/{id}/reply"), &request)
            .await
    }

    async fn mark_read(&self, id: Uuid) -> anyhow::Result<MessageRecord> {
        self.post(&format!("/api/messages/{id}/read"), &serde_json::json!({}))
            .await
    }

    async fn inbox(
        &self,
        agent_id: &str,
        status: Option<MessageStatus>,
        kind: Option<MessageKind>,
    ) -> anyhow::Result<Vec<MessageRecord>> {
        let mut url = format!("{}/api/messages?agent_id={}", self.base_url, agent_id);
        if let Some(status) = status {
            url.push_str("&status=");
            url.push_str(&status.to_string());
        }
        if let Some(kind) = kind {
            url.push_str("&kind=");
            url.push_str(&kind.to_string());
        }
        let response = self.client.get(url).bearer_auth(&self.token).send().await?;
        decode_response(response).await
    }

    async fn watch(
        &self,
        agent_id: &str,
        json: bool,
        wait: Option<(Option<MessageKind>, Duration)>,
    ) -> anyhow::Result<()> {
        let url = format!("{}/api/stream?agent_id={}", self.base_url, agent_id);
        let response = self.client.get(url).bearer_auth(&self.token).send().await?;
        if !response.status().is_success() {
            bail!("request failed: {}", response.text().await?);
        }
        let deadline = wait.map(|(_, duration)| tokio::time::Instant::now() + duration);
        let expected_kind = wait.and_then(|(kind, _)| kind);
        let mut stream = response.bytes_stream();
        let mut buffer = String::new();

        loop {
            if let Some(deadline) = deadline {
                tokio::select! {
                    chunk = stream.next() => {
                        if !handle_stream_chunk(chunk, &mut buffer, expected_kind, json)? {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => bail!("timed out waiting for message"),
                }
            } else if !handle_stream_chunk(stream.next().await, &mut buffer, expected_kind, json)? {
                return Ok(());
            }
        }
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
            .await?;
        decode_response(response).await
    }
}

async fn decode_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> anyhow::Result<T> {
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("request failed ({status}): {text}");
    }
    Ok(serde_json::from_str(&text).with_context(|| format!("invalid response: {text}"))?)
}

fn handle_stream_chunk(
    chunk: Option<Result<bytes::Bytes, reqwest::Error>>,
    buffer: &mut String,
    expected_kind: Option<MessageKind>,
    json: bool,
) -> anyhow::Result<bool> {
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
            if expected_kind.is_some_and(|kind| kind != message.kind) {
                continue;
            }
            print_message(json, &message)?;
            return Ok(expected_kind.is_none());
        }
    }
    Ok(true)
}

fn required(value: Option<String>, name: &str) -> anyhow::Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{name} must be set"))
}

fn print_value<T: serde::Serialize>(json: bool, value: &T, human: &str) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{human}");
    }
    Ok(())
}

fn print_message(json: bool, message: &MessageRecord) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(message)?);
    } else {
        println!(
            "[{}] {} -> {} {} {}\nsubject: {}\n{}\nthread: {} id: {}",
            message.kind,
            message.from_agent,
            message.to_agent,
            message.created_at,
            message.status,
            message.subject,
            message.body,
            message.thread_id,
            message.id
        );
    }
    Ok(())
}

fn parse_duration(value: &str) -> anyhow::Result<Duration> {
    let value = value.trim();
    let (number, multiplier) = if let Some(minutes) = value.strip_suffix('m') {
        (minutes, 60)
    } else if let Some(seconds) = value.strip_suffix('s') {
        (seconds, 1)
    } else if let Some(hours) = value.strip_suffix('h') {
        (hours, 60 * 60)
    } else {
        (value, 1)
    };
    let amount: u64 = number.parse().context("invalid timeout")?;
    Ok(Duration::from_secs(amount * multiplier))
}
