# MCP Roadmap

MCP should be a thin adapter over ACP, not a second coordination system.

The current pre-MCP shape is:

- `agent-protocol`: stable JSON types
- `agent-client`: reusable HTTP client
- `agent-hub`: coordination runtime
- `agentctl`: CLI adapter

Future crate:

```text
crates/agent-mcp
```

Initial MCP tools:

- `register_agent`
- `list_agents`
- `get_agent_status`
- `update_agent_status`
- `send_message`
- `broadcast_message`
- `read_inbox`
- `get_thread`
- `reply_to_thread`
- `create_task`
- `claim_task`
- `update_task`
- `claim_files`
- `release_files`
- `list_file_claims`
- `publish_finding`
- `list_findings`
- `search_findings`

Later tools should cover reviews, contract changes, git status, decisions, shared context and events.
