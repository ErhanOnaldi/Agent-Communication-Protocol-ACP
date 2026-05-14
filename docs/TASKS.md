# Tasks

Tasks describe units of coding work that agents can create, claim, update and complete.

Statuses:

- `open`
- `proposed`
- `claimed`
- `in_progress`
- `blocked`
- `needs_review`
- `changes_requested`
- `approved`
- `done`
- `cancelled`

Examples:

```bash
agentctl task create --title "Fix auth middleware panic" --body "Missing token causes panic."
agentctl task list
agentctl task claim <task-id> --branch agent/backend/auth-panic
agentctl task update <task-id> --status in_progress --body "Reproduced issue."
agentctl task done <task-id> --body "Fixed panic and added tests."
```
