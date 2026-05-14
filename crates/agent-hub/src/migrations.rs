use sqlx::SqlitePool;
use chrono::Utc;

pub(crate) const MIGRATION_1: &str = r#"
CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    hostname TEXT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    from_agent TEXT NOT NULL,
    to_agent TEXT NOT NULL,
    kind TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    reply_to TEXT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    read_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"#;

pub(crate) const MIGRATION_2: &str = r#"
ALTER TABLE agents ADD COLUMN status TEXT NOT NULL DEFAULT 'online';
ALTER TABLE agents ADD COLUMN current_task TEXT NULL;
ALTER TABLE agents ADD COLUMN branch TEXT NULL;
CREATE TABLE IF NOT EXISTS threads (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    closed_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    status TEXT NOT NULL,
    owner TEXT NULL,
    priority TEXT NOT NULL,
    branch TEXT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_claims (
    id TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    claimed_by TEXT NOT NULL,
    task_id TEXT NULL,
    branch TEXT NULL,
    reason TEXT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS findings (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    files_json TEXT NOT NULL,
    confidence TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"#;

pub(crate) const MIGRATION_3: &str = r#"
CREATE TABLE IF NOT EXISTS models (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    runtime_source TEXT NOT NULL,
    tier TEXT NOT NULL,
    context_window INTEGER NULL,
    pricing_input REAL NULL,
    pricing_output REAL NULL
);
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    workflow_yaml TEXT NOT NULL,
    status TEXT NOT NULL,
    profile TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS slots (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    role TEXT NOT NULL,
    runtime_type TEXT NULL,
    model_id TEXT NULL,
    agent_id TEXT NULL,
    status TEXT NOT NULL DEFAULT 'empty',
    capabilities_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS pipeline_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id TEXT NOT NULL,
    agent_id TEXT NULL,
    event_type TEXT NOT NULL,
    payload JSON NOT NULL,
    correlation_id TEXT NULL,
    causation_id TEXT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    content TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS working_context (
    pipeline_id TEXT NOT NULL,
    role TEXT NOT NULL,
    summary TEXT NOT NULL,
    key_decisions JSON NOT NULL,
    active_files JSON NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (pipeline_id, role)
);
CREATE TABLE IF NOT EXISTS capability_scores (
    runtime_type TEXT NOT NULL,
    model_id TEXT NOT NULL,
    capability TEXT NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (runtime_type, model_id, capability)
);
INSERT OR IGNORE INTO models (id, name, runtime_source, tier, context_window, pricing_input, pricing_output)
VALUES
    ('claude-code/default', 'Claude Code default', 'claude_code', 'premium', NULL, NULL, NULL),
    ('codex/default', 'Codex default', 'codex', 'premium', NULL, NULL, NULL),
    ('gemini/default', 'Gemini default', 'gemini', 'standard', NULL, NULL, NULL),
    ('copilot/default', 'GitHub Copilot default', 'copilot', 'standard', NULL, NULL, NULL),
    ('claudex/qwen3-coder', 'Qwen3 Coder via Claudex', 'claudex', 'cheap', NULL, NULL, NULL),
    ('claudex/deepseek', 'DeepSeek via Claudex', 'claudex', 'cheap', NULL, NULL, NULL);
"#;

pub async fn init_db(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
        .execute(pool)
        .await?;
    run_migration(pool, 1, MIGRATION_1).await?;
    run_migration(pool, 2, MIGRATION_2).await?;
    run_migration(pool, 3, MIGRATION_3).await?;
    Ok(())
}

pub(crate) async fn run_migration(pool: &SqlitePool, version: i64, sql: &str) -> anyhow::Result<()> {
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations WHERE version = ?1")
            .bind(version)
            .fetch_optional(pool)
            .await?;
    if exists.is_some() {
        return Ok(());
    }
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(statement).execute(pool).await?;
    }
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)")
        .bind(version)
        .bind(Utc::now().to_rfc3339())
        .execute(pool)
        .await?;
    Ok(())
}
