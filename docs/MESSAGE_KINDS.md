# Message Kinds

Standard message kinds:

- `status_update`
- `proposal`
- `question`
- `answer`
- `contract_change`
- `review_request`
- `review_result`
- `blocker`
- `handoff`
- `done`
- `finding`
- `decision`
- `task_update`
- `file_claim`
- `branch_update`
- `test_result`

Custom kinds are allowed only with the `custom:<name>` format:

```bash
agentctl send --to backend-api-agent --kind custom:handover_note --subject "Handoff" --body "..."
```

Unknown unprefixed kinds are rejected so agents can treat messages as workflow events, not only text.
