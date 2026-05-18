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
