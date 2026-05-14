# CLI Reference

## Global Flags

```
--hub-url <URL>     Hub base URL  [env: ACP_HUB_URL]  [default: http://127.0.0.1:8787]
--token <TOKEN>     Auth token    [env: ACP_TOKEN]
--acp-home <PATH>   ACP home dir  [env: ACP_HOME]     [default: ~/.acp]
--json              Output JSON instead of human-readable text
```

## Interactive Mode

```
acp
```

Opens the home screen. Use arrow keys or `j`/`k` to navigate, the shortcut letter or Enter to launch an action, `q` / Esc to quit. After each command completes you are returned to the menu.

---

## doctor

Check which AI runtimes are installed and healthy.

```bash
acp doctor
```

Reports discovery status for Claude Code, Codex CLI, Gemini CLI, and Copilot CLI. A runtime is only healthy if its binary is present and responds to a version check.

---

## discover / runtimes

Discover all runtimes and print their records.

```bash
acp discover
acp runtimes    # alias
```

---

## models

List all models visible to ACP.

```bash
acp models
acp models --tier flagship
```

---

## dashboard

Open the live ratatui dashboard.

```bash
acp dashboard
```

Refreshes every 3 seconds. Panels:

- **Pipelines** — ID, status (colour-coded), profile name
- **Models** — tier and name
- **Recent events** — last 3 pipeline events
- **Step analytics** — last 4 steps with health (green/red) and latency

Keys: `r` refresh, `q` / Esc quit.

---

## pipeline

### run

Parse and register a workflow; optionally execute it immediately.

```bash
acp pipeline run <workflow.yaml> [--repo <path>] [--approve-assignments] [--execute]
```

- `--approve-assignments` — required to accept the scheduler's runtime assignments
- `--execute` — required to start execution (must be combined with `--approve-assignments`)
- `--repo` — working directory for agent subprocess (default: `.`)

Without `--execute` the pipeline is created in `pending` state and can be inspected before committing.

### list

```bash
acp pipeline list
```

### status

```bash
acp pipeline status <pipeline-id>
```

Prints pipeline record, slot list, and event count.

---

## analytics

### pipeline

```bash
acp analytics pipeline <pipeline-id>
```

Prints a table with per-step step name, role, runtime, health, and latency, plus pipeline-level P50/P95 latency.

---

## provider

Manage API providers used for Claudex instances.

```bash
acp provider add <name> --base-url <url> --api-key-env <ENV_VAR>
acp provider list
acp provider validate
```

Providers are stored in `~/.acp/providers.json`.

---

## slot

Inspect and manually manage role slots within a pipeline.

```bash
acp slot list <pipeline-id>
acp slot assign <pipeline-id> <role> <runtime> [--model <model-id>]
acp slot vacate <pipeline-id> <role>
```

---

## skill

Skills are task templates loaded from `~/.acp/skills/`.

```bash
acp skill list
acp skill show <name>
```

---

## mcp

Manage MCP server registrations.

```bash
acp mcp list
acp mcp start <name>
acp mcp stop <name>
```

---

## memory

Query the semantic memory index for a pipeline.

```bash
acp memory search <pipeline-id> <query>
```

Returns the most relevant prior step output snippets for the given query string, using TF cosine similarity.

---

## runtime

Send control signals to a running agent.

```bash
acp runtime interrupt <agent-id>
acp runtime shutdown <agent-id>
```

---

## workspace

Show git worktree status for the current repository.

```bash
acp workspace [--repo <path>]
```
