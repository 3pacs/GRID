-- Event bus: append-only event log for cross-node coordination
CREATE TABLE IF NOT EXISTS event_bus (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type  TEXT NOT NULL,
    source_node TEXT NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eb_type_created ON event_bus(event_type, created_at DESC);
-- Auto-prune: events older than 7 days are candidates for deletion
CREATE INDEX IF NOT EXISTS idx_eb_prune ON event_bus(created_at) WHERE created_at < NOW() - INTERVAL '7 days';

-- Task queue: persistent work backlog for distributed execution
CREATE TABLE IF NOT EXISTS task_queue (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type   TEXT NOT NULL,
    priority    INTEGER NOT NULL DEFAULT 3,
    node_target TEXT,
    payload     JSONB NOT NULL DEFAULT '{}',
    status      TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued','running','completed','failed','cancelled')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at  TIMESTAMPTZ,
    claimed_by  TEXT,
    completed_at TIMESTAMPTZ,
    result      JSONB,
    error       TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3
);

CREATE INDEX IF NOT EXISTS idx_tq_claimable ON task_queue(priority, created_at)
    WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_tq_node ON task_queue(node_target, status)
    WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_tq_running ON task_queue(claimed_by, status)
    WHERE status = 'running';
