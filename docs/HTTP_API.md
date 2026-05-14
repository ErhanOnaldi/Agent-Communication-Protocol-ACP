# HTTP API

All routes except `/health` require:

```
Authorization: Bearer <ACP_TOKEN>
```

The hub listens on `http://127.0.0.1:8787` by default.

## Health

```
GET  /health
```

## Models

```
GET  /api/models
```

## Capability Scores

```
GET  /api/capability-scores
POST /api/capability-scores
```

## Pipelines

```
POST /api/pipelines                          create pipeline
GET  /api/pipelines                          list pipelines
GET  /api/pipelines/:id                      get pipeline
POST /api/pipelines/:id/status               update status
GET  /api/pipelines/:id/slots                list role slots
POST /api/pipelines/:id/slots/:role          update a slot
POST /api/pipelines/:id/events               create event
GET  /api/pipelines/:id/events               list events
POST /api/pipelines/:id/scheduler-decisions  record scheduler decision
GET  /api/pipelines/:id/scheduler-decisions  list scheduler decisions
POST /api/pipelines/:id/context-compressions record context compression
GET  /api/pipelines/:id/context-compressions list context compressions
POST /api/pipelines/:id/semantic-memory      add memory entry
GET  /api/pipelines/:id/semantic-memory      list memory entries
GET  /api/pipelines/:id/memory-search        semantic search (?q=<query>)
POST /api/pipelines/:id/artifacts            upload artifact
GET  /api/pipelines/:id/artifacts            list artifacts
POST /api/pipelines/:id/metrics              record step metric
```

## Analytics

```
GET  /api/analytics/pipelines/:id
```

Returns `PipelineAnalyticsResponse`:

```json
{
  "pipeline_id": "uuid",
  "total_steps": 4,
  "succeeded": 3,
  "failed": 1,
  "p50_latency_ms": 12400,
  "p95_latency_ms": 31000,
  "steps": [
    {
      "step_name": "backend.implement",
      "role": "backend",
      "runtime_type": "Codex",
      "model_id": "codex/default",
      "health": "Healthy",
      "latency_ms": 11200
    }
  ]
}
```

## MCP Servers

```
GET  /api/mcp                 list MCP servers
POST /api/mcp                 upsert MCP server
GET  /api/mcp/:name/health    get server health
```

## Runtime Control

```
POST /api/runtime/:agent_id/interrupt
POST /api/runtime/:agent_id/shutdown
```

## Working Context (Handoff Memory)

```
GET  /api/memory/:pipeline_id/:role
PUT  /api/memory/:pipeline_id/:role
```

## SSE Stream

```
GET  /api/stream
```

Real-time event stream for pipeline and slot lifecycle events.

## Legacy (agent-hub)

The following routes are retained from the original coordination hub and are served by `agent-hub`, not `acp-hub`:

```
POST /api/agents/heartbeat
GET  /api/agents
GET  /api/agents/:id
POST /api/agents/:id/status
POST /api/messages
GET  /api/messages
POST /api/messages/broadcast
POST /api/messages/to-role/:role
POST /api/messages/:id/read
POST /api/messages/:id/reply
GET  /api/threads
GET  /api/threads/:id
POST /api/threads/:id/reply
POST /api/threads/:id/close
POST /api/tasks
GET  /api/tasks
GET  /api/tasks/:id
POST /api/tasks/:id/claim
POST /api/tasks/:id/status
POST /api/tasks/:id/done
POST /api/file-claims
GET  /api/file-claims
DELETE /api/file-claims/:id
POST /api/findings
GET  /api/findings
GET  /api/findings/:id
GET  /api/findings/search
GET  /api/stream
```
