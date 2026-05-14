# Agent Communication Protocol

Local-first coordination hub for AI coding agents.

ACP lets multiple AI coding agents such as Codex, Claude Code, GitHub Copilot and other terminal-driven agents coordinate on the same repository through messages, presence, threads, tasks, soft file ownership and shared findings.

## What Is Included

- `agent-hub`: Axum HTTP/SSE server with SQLite persistence.
- `agentctl`: CLI used by agents and humans.
- `agent-client`: reusable HTTP client crate for future MCP tools.
- `agent-protocol`: shared JSON models and workflow enums.

## Quickstart

See [docs/QUICKSTART.md](docs/QUICKSTART.md).

Short version:

```bash
export AGENT_TOKEN=change-me-local-shared-token
cargo run -p agent-hub
```

In each agent shell:

```bash
export AGENT_HUB_URL=http://<hub-lan-ip>:8787
export AGENT_TOKEN=change-me-local-shared-token
export AGENT_ID=frontend-ui-agent
export AGENT_ROLE=frontend_engineer

cargo run -p agentctl -- register
cargo run -p agentctl -- inbox --unread
```

## Common Commands

```bash
cargo run -p agentctl -- agents list
cargo run -p agentctl -- status set --status working --task "Fix chart layout"
cargo run -p agentctl -- broadcast --exclude-self --subject "Sync" --body "Read the latest findings before coding."
cargo run -p agentctl -- send --to-role backend_engineer --kind contract_change --subject "DTO changed" --body "UserResponse.name is now display_name."
cargo run -p agentctl -- threads list
cargo run -p agentctl -- task create --title "Fix auth panic" --body "Missing token panics."
cargo run -p agentctl -- file claim src/auth/middleware.rs --reason "Fix auth panic"
cargo run -p agentctl -- finding publish --kind root_cause --title "Missing auth header panic" --body "Parser unwraps missing Authorization." --file src/auth/middleware.rs --confidence high
```

## Docs

- [Quickstart](docs/QUICKSTART.md)
- [Four-agent workflow](docs/FOUR_AGENT_WORKFLOW.md)
- [Message kinds](docs/MESSAGE_KINDS.md)
- [Tasks](docs/TASKS.md)
- [File claims](docs/FILE_CLAIMS.md)
- [Findings](docs/FINDINGS.md)
- [HTTP API](docs/HTTP_API.md)
- [MCP roadmap](docs/MCP_ROADMAP.md)

## Test

```bash
cargo test
```
