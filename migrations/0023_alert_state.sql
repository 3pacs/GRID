-- Migration: 0023_alert_state
-- Purpose: Persistent state for the supply chain alert watchdog.
--
-- Tables:
--   alert_state                   — dedup key for "we already alerted on X".
--                                   The alert runner writes (alert_type, entity_id)
--                                   rows after successfully emitting a finding so
--                                   repeated runs don't spam the same event.
--   supply_chain_edge_snapshots   — per-edge snapshot of chokepoint_score and
--                                   pct_downstream_cogs, used to compute deltas
--                                   between runs (degradation / concentration shift).
--
-- Idempotent: all DDL uses IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS alert_state (
    id SERIAL PRIMARY KEY,
    alert_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    seen_at TIMESTAMPTZ DEFAULT NOW(),
    payload JSONB,
    UNIQUE (alert_type, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_alert_state_type ON alert_state(alert_type);
CREATE INDEX IF NOT EXISTS idx_alert_state_seen_at ON alert_state(seen_at DESC);

CREATE TABLE IF NOT EXISTS supply_chain_edge_snapshots (
    edge_key TEXT PRIMARY KEY,  -- "{upstream}|{downstream}|{relationship}"
    chokepoint_score NUMERIC,
    pct_downstream_cogs NUMERIC,
    annual_usd NUMERIC,
    snapshotted_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_supply_edge_snapshots_snapshotted_at
    ON supply_chain_edge_snapshots(snapshotted_at DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON alert_state                 TO grid;
GRANT ALL ON supply_chain_edge_snapshots TO grid;
GRANT USAGE, SELECT ON SEQUENCE alert_state_id_seq TO grid;
-- supply_chain_edge_snapshots uses TEXT primary key (no sequence to grant).
