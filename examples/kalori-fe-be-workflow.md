# Kalori FE/BE Agent Workflow

## Roles

- Frontend agent: `frontend-macbook`
- Backend agent: `backend-windows`
- Hub URL: `http://<hub-lan-ip>:8787`

## Session Start

Each agent registers at the start of a work session:

```bash
agentctl register
```

Recommended environment for frontend:

```bash
export AGENT_ID=frontend-macbook
export AGENT_ROLE=frontend
```

Recommended environment for backend:

```bash
export AGENT_ID=backend-windows
export AGENT_ROLE=backend
```

## Contract Change

When backend changes an endpoint or payload:

```bash
agentctl send --to frontend-macbook --kind contract_change \
  --subject "POST /api/health-sync payload changed" \
  --body "daily_nutrition_summaries now accepts summary_date, sodium_mg, dietary_energy_kcal."
```

When frontend depends on a backend contract:

```bash
agentctl ask --to backend-windows \
  --subject "Export nutrition fields" \
  --body "Will /api/v1/export include sodium_mg as null when missing?"
```

## Inbox Discipline

Agents should check unread messages before starting coordinated work:

```bash
agentctl inbox --unread
```

After handling a message:

```bash
agentctl mark-read --message-id <uuid>
```
