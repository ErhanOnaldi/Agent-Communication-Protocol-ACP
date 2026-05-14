# Workflow YAML Reference

A workflow file describes the roles, runtime preferences, step sequence, failure policy, and timeouts for a pipeline run.

## Minimal Example

```yaml
workflow:
  id: my-workflow
  name: "My Workflow"
  profile: speed-first
  slots:
    implementer:
      role: implementer
      runtime_mode: auto
      preferred:
        - runtime: codex
          model: codex/default
        - runtime: claude-code
          model: claude-code/default
      required_capabilities: [coding]
      optional: false
  steps:
    - implementer.implement
  failure:
    default: retry(1)
  timeouts:
    step_minutes: 20
    pipeline_minutes: 45
```

## Top-Level Fields

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique identifier (used in logs and analytics) |
| `name` | string | Human-readable display name |
| `profile` | enum | Scheduler profile: `quality-first`, `speed-first`, `budget-first` |
| `slots` | map | Role slot definitions keyed by slot name |
| `steps` | list | Ordered list of steps to execute |
| `failure` | object | Failure handling policy |
| `timeouts` | object | Per-step and total pipeline timeout in minutes |

## Scheduler Profiles

| Profile | Effect |
|---|---|
| `quality-first` | Prefers Flagship-tier models (Claude Opus, GPT-5) |
| `speed-first` | Prefers Standard-tier models, faster and more available |
| `budget-first` | Prefers the cheapest available runtime |

The adaptive controller may switch `quality-first` to `speed-first` automatically when the pipeline failure rate exceeds 30% after at least 4 steps.

## Slot Definition

```yaml
slots:
  architect:
    role: architect              # role name used in step references
    runtime_mode: external       # external | auto
    preferred:
      - runtime: claude-code
        model: claude-code/default
      - runtime: codex
        model: codex/default
    required_capabilities:
      - architecture
      - planning
    optional: false              # if true, slot failure is skipped not fatal
```

### `runtime_mode`

- `external` — expects a runtime already running (IDE extension, long-lived process)
- `auto` — ACP spawns and manages the subprocess

### `preferred`

Ordered list of `(runtime, model)` pairs. The scheduler scores all candidates and picks the best match based on capability scores and the active profile. The list is the fallback order if the top candidate fails.

Supported runtime names: `claude-code`, `codex`, `gemini`, `copilot`, `claudex`

### `required_capabilities`

A list of capability tags the assigned runtime must have records for. Tags are hierarchical: `rust/tokio` satisfies a requirement of `rust` but not the reverse.

## Steps

### Action Step

A simple `role.action` string:

```yaml
steps:
  - architect.plan
  - backend.implement
  - reviewer.audit
```

The string format is `<slot-name>.<action-name>`. The action name is passed to the runtime as a task description.

### Parallel Step

Run multiple actions concurrently. All must complete before the next step begins.

```yaml
steps:
  - parallel:
      - backend.implement
      - frontend.implement
```

### Conditional Step

Run a step only when the previous step's health matches the condition.

```yaml
steps:
  - implementer.implement
  - conditional:
      action: reviewer.audit
      when_healthy: true    # only run if previous step was Healthy
```

Set `when_healthy: false` to run a step only after a failure (e.g. a recovery or notification step).

## Failure Policy

```yaml
failure:
  default: retry(2)          # retry up to 2 times before failing the pipeline
  overrides:
    reviewer: skip            # skip reviewer slot failures instead of retrying
```

Supported values:

- `retry(<n>)` — retry the step up to `n` additional times
- `skip` — mark the slot as skipped and continue the pipeline

## Timeouts

```yaml
timeouts:
  step_minutes: 30      # timeout for a single step
  pipeline_minutes: 180 # total pipeline timeout
```

## Template Workflows

Three ready-to-use templates are in `templates/workflows/`:

| File | Profile | Slots | Steps |
|---|---|---|---|
| `quick_fix.yaml` | speed-first | implementer | implement |
| `full_dev.yaml` | quality-first | architect, backend, reviewer | plan → implement (parallel) → audit |
| `review_only.yaml` | quality-first | reviewer | audit |
