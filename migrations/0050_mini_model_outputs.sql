-- Migration: 0050_mini_model_outputs.sql
-- Author: feat/wire-mini-models
-- Applies via: sudo -u postgres psql griddb -f migrations/0050_mini_model_outputs.sql
--
-- Persists outputs from the Gemma 270M mini-models that were running but had
-- no production caller wired in:
--   - anomaly_narrator   (port 8083) → anomaly_narratives
--   - knowledge_mapper   (port 8085) → signal_knowledge_entries
--
-- The helpers in ingestion/signal_classifier.py (narrate_anomalies,
-- map_signal_knowledge) return lists of dicts; hermes_operator now calls
-- them every cycle and persists their output here so downstream consumers
-- (briefings, alerts, dashboards) can read.
--
-- Idempotent: every CREATE uses IF NOT EXISTS.

-- ====== SCHEMA CHANGES ======

-- One row per narrated anomaly. Keyed by (signal_source, ticker, valid_from)
-- so re-runs of narrate_anomalies don't duplicate. Narrative is the
-- one-line plain-English summary produced by the anomaly_narrator model.
CREATE TABLE IF NOT EXISTS anomaly_narratives (
    id            BIGSERIAL PRIMARY KEY,
    ticker        TEXT,
    source_module TEXT NOT NULL,
    z_score       DOUBLE PRECISION NOT NULL,
    narrative     TEXT NOT NULL,
    signal_ts     TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_module, ticker, signal_ts)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_narratives_created
    ON anomaly_narratives (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_narratives_ticker
    ON anomaly_narratives (ticker, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_narratives_z
    ON anomaly_narratives (ABS(z_score) DESC);

-- One row per knowledge entry. signal_id references signal_registry.id
-- (the source signal that the knowledge_mapper enriched). Entry text is
-- a wiki-style block with [[backlinks]] surfacing cross-domain connections.
CREATE TABLE IF NOT EXISTS signal_knowledge_entries (
    id              BIGSERIAL PRIMARY KEY,
    signal_id       BIGINT NOT NULL,
    ticker          TEXT,
    category        TEXT,
    knowledge_entry TEXT NOT NULL,
    signal_ts       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (signal_id)
);

CREATE INDEX IF NOT EXISTS idx_skw_entries_created
    ON signal_knowledge_entries (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_skw_entries_ticker
    ON signal_knowledge_entries (ticker, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_skw_entries_category
    ON signal_knowledge_entries (category, created_at DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======

GRANT ALL ON anomaly_narratives TO grid;
GRANT USAGE, SELECT ON SEQUENCE anomaly_narratives_id_seq TO grid;

GRANT ALL ON signal_knowledge_entries TO grid;
GRANT USAGE, SELECT ON SEQUENCE signal_knowledge_entries_id_seq TO grid;
