# ACP — AI Control Plane

ACP is an AI Runtime Operating System that discovers, schedules, and orchestrates multiple AI coding agents on your local machine.

It does **not** write a new agent runtime. It composes the ones you already have — Claude Code, Codex CLI, Gemini CLI, GitHub Copilot — into coordinated multi-agent workflows with shared memory, adaptive scheduling, and live observability.

```
ACP Orchestrator
├── Claude Code     (subprocess)
├── Codex CLI       (subprocess)
├── Gemini CLI      (subprocess)
├── Copilot CLI     (subprocess)
└── Claudex         (Claude Code binary + API key env override)
    ├── Qwen3-Coder  (via OpenRouter)
    ├── DeepSeek     (via DeepSeek API)
    └── any Anthropic-compatible model
```

## What ACP Does

- **Discovers** AI coding runtimes installed on your machine and API key providers configured on your network
- **Scores and assigns** runtimes to workflow roles using a capability scorer that learns from past runs
- **Runs multi-agent pipelines** from a declarative YAML workflow — sequential steps, parallel branches, and conditional steps based on prior health
- **Recovers** from runtime failures (rate limits, crashes) by hot-swapping to a fallback runtime in the same role slot
- **Injects semantic context** from prior steps into each agent's prompt using a TF cosine-similarity memory index
- **Adapts** the scheduler profile automatically when the pipeline failure rate crosses a threshold
- **Persists** per-step latency and health to SQLite and exposes P50/P95 analytics
- **Displays** everything in a live ratatui terminal dashboard

## Crates

| Crate | Role |
|---|---|
| `acp-cli` | Interactive TUI home screen, all CLI subcommands, live dashboard |
| `acp-hub` | Axum HTTP/SSE server with SQLite persistence |
| `acp-orchestrator` | Workflow engine, scheduler, slot lifecycle, semantic memory, adaptive controller |
| `acp-discover` | Runtime and provider discovery, capability scoring, skill loading |
| `acp-protocol` | Shared types used across all crates |
| `acp-runtime` | Subprocess adapter for spawning agent runtimes |
| `acp-workspace` | Git worktree isolation for parallel agent execution |
| `agent-client` | Typed HTTP client for the hub API |
| `agent-hub` | Legacy coordination hub (retained for compatibility) |
| `agent-protocol` | Legacy shared protocol types |
| `agentctl` | Legacy CLI for the coordination hub |

## Quickstart

See [docs/QUICKSTART.md](docs/QUICKSTART.md).

Short version:

```bash
# Terminal 1 — hub
export ACP_TOKEN=local-dev-token
export ACP_HUB_DATABASE_URL=sqlite://acp-hub.db
cargo run -p acp-hub

# Terminal 2 — CLI
export ACP_TOKEN=local-dev-token
cargo run -p acp-cli -- doctor
cargo run -p acp-cli -- dashboard
```

## Running a Workflow

```bash
cargo run -p acp-cli -- pipeline run templates/workflows/quick_fix.yaml \
  --repo . --approve-assignments --execute
```

Check results:

```bash
cargo run -p acp-cli -- pipeline list
cargo run -p acp-cli -- pipeline status <pipeline-id>
cargo run -p acp-cli -- analytics pipeline <pipeline-id>
```

## Interactive Mode

Running `acp` with no arguments opens the interactive home screen:

```
acp
```

Navigate with arrow keys or `j`/`k`, press the shortcut letter or Enter to run the highlighted action. After each command completes you are returned to the menu. Press `q` or Esc to quit.

## Docs

- [Quickstart](docs/QUICKSTART.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Workflow YAML reference](docs/WORKFLOW_YAML.md)
- [CLI reference](docs/CLI_REFERENCE.md)
- [HTTP API](docs/HTTP_API.md)

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
