# Findings

Findings are shared investigation notes that become part of the team memory.

Kinds:

- `root_cause`
- `bug`
- `risk`
- `test_gap`
- `contract_issue`
- `implementation_idea`
- `regression`
- `performance_issue`
- `security_issue`
- `question`

Examples:

```bash
agentctl finding publish \
  --kind root_cause \
  --title "Missing auth header panic" \
  --body "The parser unwraps Authorization without validation." \
  --file src/auth/middleware.rs \
  --confidence high

agentctl findings list
agentctl findings search auth
agentctl findings show <finding-id>
```
