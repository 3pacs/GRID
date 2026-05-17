CREATE TABLE IF NOT EXISTS fleet_state (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ DEFAULT NOW(),
    host TEXT NOT NULL,
    ok BOOLEAN DEFAULT TRUE,
    error TEXT,
    gpu_index INTEGER,
    gpu_name TEXT,
    gpu_uuid TEXT,
    util_pct INTEGER,
    mem_used_mb INTEGER,
    mem_total_mb INTEGER,
    procs JSONB DEFAULT '[]'::jsonb,
    services_running JSONB DEFAULT '[]'::jsonb,
    queue_depths JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_fleet_state_ts ON fleet_state(ts DESC);
CREATE INDEX IF NOT EXISTS idx_fleet_state_host_ts ON fleet_state(host, ts DESC);
CREATE INDEX IF NOT EXISTS idx_fleet_state_gpu_ts ON fleet_state(host, gpu_index, ts DESC);

-- Task #39: free-form per-row state blob. Non-GPU rows (intelligence-layer
-- snapshots written as host='intelligence') stuff their counts/timestamps in
-- here. Idempotent via IF NOT EXISTS.
ALTER TABLE fleet_state ADD COLUMN IF NOT EXISTS state JSONB;

-- Task #40 — retention plan: ~96 rows/host/day (15-min cadence) + 1
-- intelligence row per pass. prune_fleet_state(keep_days=90) is called at the
-- END of every audit run from fleet_audit.py and deletes rows older than 90d.
-- 90d retains ~70k rows total, keeps the (ts DESC) index hot, and preserves
-- a useful trend window. To extend retention, bump the default in
-- fleet_audit.py (search for prune_fleet_state). No separate cron today.
