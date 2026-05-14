# File Claims

File claims are soft ownership records. They warn agents about overlapping work but do not hard-block edits.

Examples:

```bash
agentctl file claim src/auth/middleware.rs --task <task-id> --reason "Fix missing token panic"
agentctl file claims
agentctl file check src/auth/middleware.rs
agentctl file release <claim-id>
```

Claims can expire:

```bash
agentctl file claim src/auth/middleware.rs --ttl-seconds 3600
```

Expired claims are marked as `stale` in API and CLI output.
