use std::time::Duration;

use agent_client::AgentClient;
use agent_protocol::{
    AgentStatus, BroadcastRequest, Confidence, FileClaimRequest, FindingCreateRequest, FindingKind,
    HeartbeatRequest, MessageCreateRequest, MessageKind, MessageRecord, MessageStatus,
    ReplyRequest, RoleMessageRequest, TaskClaimRequest, TaskCreateRequest, TaskPriority,
    TaskStatus, TaskStatusRequest, UpdateAgentStatusRequest,
};
use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "agentctl",
    about = "CLI client for Agent Communication Protocol"
)]
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

#[derive(Debug, Clone, Subcommand)]
enum Command {
    Register(RegisterArgs),
    Heartbeat,
    Agents {
        #[command(subcommand)]
        command: AgentsCommand,
    },
    Status {
        #[command(subcommand)]
        command: StatusCommand,
    },
    Send(SendArgs),
    Broadcast(BroadcastArgs),
    Ask {
        #[arg(long)]
        to: String,
        #[arg(long)]
        subject: String,
        #[arg(long)]
        body: String,
    },
    Reply(ReplyArgs),
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
    Threads {
        #[command(subcommand)]
        command: ThreadsCommand,
    },
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    File {
        #[command(subcommand)]
        command: FileCommand,
    },
    Finding {
        #[command(subcommand)]
        command: FindingCommand,
    },
    Findings {
        #[command(subcommand)]
        command: FindingsCommand,
    },
    Watch,
    Wait {
        #[arg(long)]
        kind: Option<MessageKind>,
        #[arg(long, default_value = "30m")]
        timeout: String,
    },
}

#[derive(Debug, Clone, Args)]
struct RegisterArgs {
    #[arg(long)]
    hostname: Option<String>,
    #[arg(long)]
    status: Option<AgentStatus>,
    #[arg(long)]
    task: Option<String>,
    #[arg(long)]
    branch: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct SendArgs {
    #[arg(long)]
    to: Option<String>,
    #[arg(long = "to-role")]
    to_role: Option<String>,
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
    #[arg(long)]
    exclude_self: bool,
}

#[derive(Debug, Clone, Args)]
struct BroadcastArgs {
    #[arg(long, default_value = "status_update")]
    kind: MessageKind,
    #[arg(long)]
    subject: String,
    #[arg(long)]
    body: String,
    #[arg(long)]
    exclude_self: bool,
}

#[derive(Debug, Clone, Args)]
struct ReplyArgs {
    #[arg(long = "message-id")]
    message_id: Option<Uuid>,
    #[arg(long = "thread-id")]
    thread_id: Option<Uuid>,
    #[arg(long)]
    body: String,
    #[arg(long)]
    subject: Option<String>,
}

#[derive(Debug, Clone, Subcommand)]
enum AgentsCommand {
    List,
    Show { agent_id: String },
}

#[derive(Debug, Clone, Subcommand)]
enum StatusCommand {
    Set {
        #[arg(long)]
        status: AgentStatus,
        #[arg(long)]
        task: Option<String>,
        #[arg(long)]
        branch: Option<String>,
    },
    Clear,
}

#[derive(Debug, Clone, Subcommand)]
enum ThreadsCommand {
    List,
    Show { thread_id: Uuid },
    Close { thread_id: Uuid },
}

#[derive(Debug, Clone, Subcommand)]
enum TaskCommand {
    Create {
        #[arg(long)]
        title: String,
        #[arg(long)]
        body: String,
        #[arg(long)]
        priority: Option<TaskPriority>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        branch: Option<String>,
    },
    List,
    Show {
        task_id: Uuid,
    },
    Claim {
        task_id: Uuid,
        #[arg(long)]
        branch: Option<String>,
    },
    Update {
        task_id: Uuid,
        #[arg(long)]
        status: TaskStatus,
        #[arg(long)]
        body: Option<String>,
    },
    Done {
        task_id: Uuid,
        #[arg(long)]
        body: Option<String>,
    },
}

#[derive(Debug, Clone, Subcommand)]
enum FileCommand {
    Claim {
        path: String,
        #[arg(long)]
        task: Option<Uuid>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long = "ttl-seconds")]
        ttl_seconds: Option<i64>,
    },
    Release {
        claim_id: Uuid,
    },
    Claims {
        #[arg(long)]
        path: Option<String>,
    },
    Check {
        path: String,
    },
}

#[derive(Debug, Clone, Subcommand)]
enum FindingCommand {
    Publish {
        #[arg(long)]
        kind: FindingKind,
        #[arg(long)]
        title: String,
        #[arg(long)]
        body: String,
        #[arg(long = "file")]
        files: Vec<String>,
        #[arg(long, default_value = "medium")]
        confidence: Confidence,
    },
}

#[derive(Debug, Clone, Subcommand)]
enum FindingsCommand {
    List,
    Show { finding_id: Uuid },
    Search { query: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = AgentClient::new(cli.hub_url.clone(), cli.token.clone())?;

    match cli.command.clone() {
        Command::Register(args) => {
            let req = heartbeat_request(&cli, args.status, args.task, args.branch, args.hostname)?;
            print_value(cli.json, &client.register(&req).await?)?;
        }
        Command::Heartbeat => {
            let req = heartbeat_request(&cli, Some(AgentStatus::Online), None, None, None)?;
            print_value(cli.json, &client.register(&req).await?)?;
        }
        Command::Agents { command } => match command {
            AgentsCommand::List => print_value(cli.json, &client.agents().await?)?,
            AgentsCommand::Show { agent_id } => {
                print_value(cli.json, &client.agent(&agent_id).await?)?
            }
        },
        Command::Status { command } => {
            let agent_id = required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?;
            let req = match command {
                StatusCommand::Set {
                    status,
                    task,
                    branch,
                } => UpdateAgentStatusRequest {
                    status,
                    current_task: task,
                    branch,
                },
                StatusCommand::Clear => UpdateAgentStatusRequest {
                    status: AgentStatus::Idle,
                    current_task: None,
                    branch: None,
                },
            };
            print_value(cli.json, &client.update_status(agent_id, &req).await?)?;
        }
        Command::Send(args) => {
            let from = required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string();
            if let Some(role) = args.to_role {
                let req = RoleMessageRequest {
                    from,
                    role,
                    kind: args.kind,
                    subject: args.subject,
                    body: args.body,
                    exclude_self: args.exclude_self,
                };
                print_value(cli.json, &client.send_to_role(&req).await?)?;
            } else {
                let to = args.to.context("use --to <agent-id> or --to-role <role>")?;
                let req = MessageCreateRequest {
                    from,
                    to,
                    kind: args.kind,
                    subject: args.subject,
                    body: args.body,
                    thread_id: args.thread_id,
                    reply_to: args.reply_to,
                };
                print_message(cli.json, &client.send(&req).await?)?;
            }
        }
        Command::Broadcast(args) => {
            let req = BroadcastRequest {
                from: required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string(),
                kind: args.kind,
                subject: args.subject,
                body: args.body,
                exclude_self: args.exclude_self,
            };
            print_value(cli.json, &client.broadcast(&req).await?)?;
        }
        Command::Ask { to, subject, body } => {
            let req = MessageCreateRequest {
                from: required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string(),
                to,
                kind: MessageKind::Question,
                subject,
                body,
                thread_id: None,
                reply_to: None,
            };
            print_message(cli.json, &client.send(&req).await?)?;
        }
        Command::Reply(args) => {
            let req = ReplyRequest {
                from: required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string(),
                body: args.body,
                subject: args.subject,
                thread_id: args.thread_id,
            };
            let message = match (args.message_id, args.thread_id) {
                (Some(id), _) => client.reply(id, &req).await?,
                (None, Some(thread_id)) => client.reply_to_thread(thread_id, &req).await?,
                (None, None) => anyhow::bail!("use --message-id <id> or --thread-id <id>"),
            };
            print_message(cli.json, &message)?;
        }
        Command::Inbox { unread, kind } => {
            let agent_id = required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?;
            let status = unread.then_some(MessageStatus::Unread);
            print_value(cli.json, &client.inbox(agent_id, status, kind).await?)?;
        }
        Command::MarkRead { message_id } => {
            print_message(cli.json, &client.mark_read(message_id).await?)?;
        }
        Command::Threads { command } => match command {
            ThreadsCommand::List => {
                let agent_id = cli.agent_id.as_deref();
                print_value(cli.json, &client.threads(agent_id).await?)?;
            }
            ThreadsCommand::Show { thread_id } => {
                print_value(cli.json, &client.thread(thread_id).await?)?
            }
            ThreadsCommand::Close { thread_id } => {
                print_value(cli.json, &client.close_thread(thread_id).await?)?;
            }
        },
        Command::Task { command } => handle_task(&cli, &client, command).await?,
        Command::File { command } => handle_file(&cli, &client, command).await?,
        Command::Finding { command } => handle_finding(&cli, &client, command).await?,
        Command::Findings { command } => match command {
            FindingsCommand::List => print_value(cli.json, &client.findings(None).await?)?,
            FindingsCommand::Show { finding_id } => {
                print_value(cli.json, &client.finding(finding_id).await?)?;
            }
            FindingsCommand::Search { query } => {
                print_value(cli.json, &client.findings(Some(&query)).await?)?;
            }
        },
        Command::Watch => {
            let agent_id = required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?;
            client
                .watch(agent_id, None, |message| {
                    print_message(cli.json, &message)?;
                    Ok(true)
                })
                .await?;
        }
        Command::Wait { kind, timeout } => {
            let agent_id = required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?;
            let timeout = parse_duration(&timeout)?;
            client
                .watch(agent_id, Some((kind, timeout)), |message| {
                    print_message(cli.json, &message)?;
                    Ok(false)
                })
                .await?;
        }
    }
    Ok(())
}

async fn handle_task(cli: &Cli, client: &AgentClient, command: TaskCommand) -> anyhow::Result<()> {
    match command {
        TaskCommand::Create {
            title,
            body,
            priority,
            owner,
            branch,
        } => {
            let req = TaskCreateRequest {
                title,
                body,
                priority,
                owner,
                branch,
                created_by: required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string(),
            };
            print_value(cli.json, &client.create_task(&req).await?)?;
        }
        TaskCommand::List => print_value(cli.json, &client.tasks().await?)?,
        TaskCommand::Show { task_id } => print_value(cli.json, &client.task(task_id).await?)?,
        TaskCommand::Claim { task_id, branch } => {
            let req = TaskClaimRequest {
                agent_id: required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string(),
                branch,
            };
            print_value(cli.json, &client.claim_task(task_id, &req).await?)?;
        }
        TaskCommand::Update {
            task_id,
            status,
            body,
        } => {
            let req = TaskStatusRequest { status, body };
            print_value(cli.json, &client.update_task(task_id, &req).await?)?;
        }
        TaskCommand::Done { task_id, body } => {
            let req = TaskStatusRequest {
                status: TaskStatus::Done,
                body,
            };
            print_value(cli.json, &client.done_task(task_id, &req).await?)?;
        }
    }
    Ok(())
}

async fn handle_file(cli: &Cli, client: &AgentClient, command: FileCommand) -> anyhow::Result<()> {
    match command {
        FileCommand::Claim {
            path,
            task,
            branch,
            reason,
            ttl_seconds,
        } => {
            let req = FileClaimRequest {
                file_path: path,
                claimed_by: required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string(),
                task_id: task,
                branch,
                reason,
                ttl_seconds,
            };
            print_value(cli.json, &client.claim_file(&req).await?)?;
        }
        FileCommand::Release { claim_id } => {
            print_value(cli.json, &client.release_file_claim(claim_id).await?)?;
        }
        FileCommand::Claims { path } => {
            print_value(cli.json, &client.file_claims(path.as_deref()).await?)?
        }
        FileCommand::Check { path } => {
            print_value(cli.json, &client.file_claims(Some(&path)).await?)?
        }
    }
    Ok(())
}

async fn handle_finding(
    cli: &Cli,
    client: &AgentClient,
    command: FindingCommand,
) -> anyhow::Result<()> {
    match command {
        FindingCommand::Publish {
            kind,
            title,
            body,
            files,
            confidence,
        } => {
            let req = FindingCreateRequest {
                agent_id: required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string(),
                kind,
                title,
                body,
                files,
                confidence,
            };
            print_value(cli.json, &client.create_finding(&req).await?)?;
        }
    }
    Ok(())
}

fn heartbeat_request(
    cli: &Cli,
    status: Option<AgentStatus>,
    current_task: Option<String>,
    branch: Option<String>,
    hostname: Option<String>,
) -> anyhow::Result<HeartbeatRequest> {
    Ok(HeartbeatRequest {
        agent_id: required(cli.agent_id.as_ref(), "AGENT_ID or --agent-id")?.to_string(),
        role: required(cli.agent_role.as_ref(), "AGENT_ROLE or --agent-role")?.to_string(),
        hostname,
        status,
        current_task,
        branch,
    })
}

fn required<'a>(value: Option<&'a String>, name: &str) -> anyhow::Result<&'a str> {
    value
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{name} must be set"))
}

fn print_value<T: serde::Serialize>(json: bool, value: &T) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(value)?);
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
