# HTTP API

All routes except `/health` require:

```text
Authorization: Bearer <AGENT_TOKEN>
```

## Health

- `GET /health`

## Agents

- `POST /api/agents/heartbeat`
- `GET /api/agents`
- `GET /api/agents/{agent_id}`
- `POST /api/agents/{agent_id}/status`

## Messages

- `POST /api/messages`
- `GET /api/messages?agent_id=<id>&status=unread&kind=question`
- `POST /api/messages/broadcast`
- `POST /api/messages/to-role/{role}`
- `POST /api/messages/{id}/read`
- `POST /api/messages/{id}/reply`
- `GET /api/stream?agent_id=<id>`

## Threads

- `GET /api/threads`
- `GET /api/threads?agent_id=<id>`
- `GET /api/threads/{thread_id}`
- `POST /api/threads/{thread_id}/reply`
- `POST /api/threads/{thread_id}/close`

## Tasks

- `POST /api/tasks`
- `GET /api/tasks`
- `GET /api/tasks/{task_id}`
- `POST /api/tasks/{task_id}/claim`
- `POST /api/tasks/{task_id}/status`
- `POST /api/tasks/{task_id}/done`

## File Claims

- `POST /api/file-claims`
- `GET /api/file-claims`
- `GET /api/file-claims?path=<path>`
- `DELETE /api/file-claims/{claim_id}`

## Findings

- `POST /api/findings`
- `GET /api/findings`
- `GET /api/findings/{finding_id}`
- `GET /api/findings/search?q=<query>`
