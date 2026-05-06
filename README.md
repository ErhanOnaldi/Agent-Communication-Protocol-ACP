# LAN Agent Messenger

Small LAN-only coordination tool for AI agents working on separate machines. One machine runs `agent-hub`; each agent uses `agentctl` to register, send notices, ask questions, reply, read inbox messages, and watch live messages.

## Build

```bash
cargo build
```

## Run The Hub

```bash
export AGENT_TOKEN=change-me-local-shared-token
export AGENT_HUB_DATABASE_URL=sqlite://agent-hub.db
cargo run -p agent-hub
```

The hub listens on `0.0.0.0:8787` by default. Other machines on the same LAN should use `http://<hub-lan-ip>:8787`.

## Use The CLI

```bash
export AGENT_HUB_URL=http://127.0.0.1:8787
export AGENT_TOKEN=change-me-local-shared-token
export AGENT_ID=frontend-macbook
export AGENT_ROLE=frontend

cargo run -p agentctl -- register
```

Send a contract change:

```bash
cargo run -p agentctl -- send --to backend-windows --kind contract_change \
  --subject "abc/{id} endpoint response changed" \
  --body "name alanı display_name olarak değişti."
```

Ask a question:

```bash
cargo run -p agentctl -- ask --to backend-windows \
  --subject "Health sync payload" \
  --body "daily_nutrition_summaries alanını ne zaman export'a ekleyeceksin?"
```

Read unread messages:

```bash
cargo run -p agentctl -- inbox --unread
```

Watch live messages:

```bash
cargo run -p agentctl -- watch
```

Wait for a question for up to 30 minutes:

```bash
cargo run -p agentctl -- wait --kind question --timeout 30m
```

Reply:

```bash
cargo run -p agentctl -- reply --message-id <uuid> \
  --body "Backend tarafında payload ve export alanlarını ekledim."
```

## HTTP API

All API routes except `/health` require `Authorization: Bearer <AGENT_TOKEN>`.

- `POST /api/agents/heartbeat`
- `POST /api/messages`
- `GET /api/messages?agent_id=<id>&status=unread`
- `POST /api/messages/{id}/read`
- `POST /api/messages/{id}/reply`
- `GET /api/stream?agent_id=<id>`

## Test

```bash
cargo test
```
