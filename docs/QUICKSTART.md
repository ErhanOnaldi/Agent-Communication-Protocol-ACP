# Quickstart

## 1. Build the CLI

```bash
cargo build -p acp-cli
```

Or install it so `acp` is available globally:

```bash
cargo install --path crates/acp-cli
```

## 2. Start the Hub

The hub is an Axum HTTP server with SQLite persistence. It must be running before you use any command that talks to it (dashboard, pipeline run, models, analytics).

```bash
export ACP_TOKEN=local-dev-token
export ACP_HUB_DATABASE_URL=sqlite://acp-hub.db
cargo run -p acp-hub
```

The hub listens on `http://127.0.0.1:8787` by default.

## 3. Configure the CLI

In every terminal that runs `acp`:

```bash
export ACP_TOKEN=local-dev-token
export ACP_HUB_URL=http://127.0.0.1:8787   # default, can omit
```

## 4. Check Your Runtimes

```bash
acp doctor
```

This discovers which AI coding runtimes are installed on your machine (Claude Code, Codex CLI, Gemini CLI, Copilot CLI) and reports their health. A runtime is healthy only if its terminal binary is present and responds correctly.

```bash
which claude && claude --version
which codex  && codex --version
which gemini && gemini --version
```

## 5. Register API Providers (Optional)

To use models via OpenRouter, DeepSeek, or any Anthropic-compatible endpoint:

```bash
acp provider add openrouter \
  --base-url https://openrouter.ai/api/v1 \
  --api-key-env OPENROUTER_API_KEY

acp provider list
acp provider validate
```

These providers are used by Claudex instances (the Claude Code binary with overridden API credentials) to run cheaper or specialised models for implementation slots.

## 6. List Available Models

```bash
acp models
```

Shows every model visible to ACP — locally discovered runtimes plus any registered API providers.

## 7. Run a Workflow

Pick a template from `templates/workflows/` or write your own (see [WORKFLOW_YAML.md](WORKFLOW_YAML.md)):

```bash
# Quick single-agent fix
acp pipeline run templates/workflows/quick_fix.yaml \
  --repo . --approve-assignments --execute

# Full multi-agent dev cycle
acp pipeline run templates/workflows/full_dev.yaml \
  --repo . --approve-assignments --execute
```

Both flags are required for execution:

- `--approve-assignments` — confirms you accept the runtime assignments ACP proposes
- `--execute` — actually runs the pipeline (without it the pipeline is registered but not started)

## 8. Inspect Results

```bash
acp pipeline list
acp pipeline status <pipeline-id>
acp analytics pipeline <pipeline-id>
```

The analytics command shows per-step health, runtime, role, and latency alongside pipeline-level P50/P95 statistics.

## 9. Live Dashboard

```bash
acp dashboard
```

Opens a ratatui terminal dashboard that refreshes every 3 seconds. Shows:

- All pipelines and their status (green = succeeded, yellow = running, red = failed)
- Registered models and their tier
- Recent pipeline events
- Step analytics for the latest pipeline

Press `r` to force a refresh, `q` or Esc to quit.

## 10. Interactive Mode

Running `acp` with no arguments opens a menu-driven home screen. Use arrow keys or the shortcut letters shown to launch any of the above actions. After each command finishes you are returned to the menu.
