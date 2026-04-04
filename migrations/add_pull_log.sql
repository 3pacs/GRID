-- Pull accountability: every puller run must log success/failure with evidence
CREATE TABLE IF NOT EXISTS pull_log (
    id              SERIAL PRIMARY KEY,
    puller_name     TEXT NOT NULL,
    source_id       INTEGER REFERENCES source_catalog(id),
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')),
    rows_inserted   INTEGER DEFAULT 0,
    rows_expected   INTEGER,
    error_message   TEXT,
    features_affected INTEGER[],
    node_name       TEXT DEFAULT 'grid-svr'
);

CREATE INDEX IF NOT EXISTS idx_pull_log_status ON pull_log(status, completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_pull_log_puller ON pull_log(puller_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pull_log_recent ON pull_log(puller_name, status, completed_at DESC)
    WHERE status IN ('SUCCESS', 'PARTIAL');
