-- Track data gaps discovered during user queries
CREATE TABLE IF NOT EXISTS query_data_gaps (
    id              SERIAL PRIMARY KEY,
    feature_id      INTEGER REFERENCES feature_registry(id),
    feature_name    TEXT NOT NULL,
    family          TEXT NOT NULL,
    gap_type        TEXT NOT NULL,          -- 'missing' or 'stale'
    last_data_date  DATE,
    days_stale      INTEGER NOT NULL,
    sla_days        INTEGER NOT NULL,
    query_text      TEXT,
    query_ticker    TEXT,
    scanned_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ            -- set when data is collected
);

CREATE INDEX IF NOT EXISTS idx_qdg_feature ON query_data_gaps(feature_id);
CREATE INDEX IF NOT EXISTS idx_qdg_scanned ON query_data_gaps(scanned_at);
CREATE INDEX IF NOT EXISTS idx_qdg_unresolved ON query_data_gaps(resolved_at) WHERE resolved_at IS NULL;
