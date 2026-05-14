# Four-Agent Workflow

Example identities:

- `frontend-ui-agent`, role `frontend_engineer`
- `frontend-state-agent`, role `frontend_engineer`
- `backend-api-agent`, role `backend_engineer`
- `backend-db-agent`, role `backend_engineer`

Each agent should start by registering and reading unread messages:

```bash
agentctl register
agentctl inbox --unread
```

## Suggested Loop

1. Check `agentctl agents list`.
2. Claim a task with `agentctl task claim <task-id>`.
3. Claim files before editing with `agentctl file claim <path>`.
4. Publish findings when investigation reveals reusable context.
5. Send `contract_change` messages for API, DTO, database, env or CLI behavior changes.
6. Before final response, run `agentctl inbox --unread`.

## Role Broadcast

```bash
agentctl send --to-role backend_engineer --kind contract_change \
  --subject "Health sync DTO changed" \
  --body "daily_nutrition_summaries is now accepted by POST /api/health-sync."
```

## Conflict Avoidance

File claims are soft locks. If another agent has claimed the same path, ACP returns a warning but does not block the claim. Agents should coordinate through a thread before overlapping edits.
