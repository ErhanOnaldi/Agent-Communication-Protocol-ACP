# Quickstart

## Start The Hub

Run this on one machine that is reachable on the LAN:

```bash
export AGENT_TOKEN=change-me-local-shared-token
export AGENT_HUB_DATABASE_URL=sqlite://agent-hub.db
cargo run -p agent-hub
```

The hub listens on `0.0.0.0:8787` by default. Other machines should use `http://<hub-lan-ip>:8787`.

## Register An Agent

```bash
export AGENT_HUB_URL=http://<hub-lan-ip>:8787
export AGENT_TOKEN=change-me-local-shared-token
export AGENT_ID=frontend-ui-agent
export AGENT_ROLE=frontend_engineer

cargo run -p agentctl -- register
```

`AGENT_ID` is the routing identity. Multiple agents can run on the same machine as long as each has a different `AGENT_ID`.

## Check Presence

```bash
cargo run -p agentctl -- agents list
cargo run -p agentctl -- agents show frontend-ui-agent
```

## Update Status

```bash
cargo run -p agentctl -- status set --status working --task "Implement weekly chart labels"
cargo run -p agentctl -- status clear
```

## Send Messages

```bash
cargo run -p agentctl -- ask --to backend-api-agent \
  --subject "Export field shape" \
  --body "Will sodium_mg be null when missing?"

cargo run -p agentctl -- broadcast --exclude-self \
  --kind status_update \
  --subject "Read findings before coding" \
  --body "A root cause finding was published for auth middleware."
```

## Coordinate Work

```bash
cargo run -p agentctl -- task create --title "Fix auth panic" --body "Missing token causes panic."
cargo run -p agentctl -- file claim src/auth/middleware.rs --reason "Fix auth panic"
cargo run -p agentctl -- finding publish --kind root_cause --title "Missing auth header panic" --body "Parser unwraps missing Authorization." --file src/auth/middleware.rs --confidence high
```
