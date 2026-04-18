-- Migration: 0021_supply_chain_and_capital_flows
-- Purpose: Foundation schema for the supply chain + capital flow intelligence layer.
--
-- Tables:
--   supply_chain_nodes  — every actor (ticker, private company, commodity, country, utility)
--                         that shows up as an upstream/downstream in a chain.
--   supply_chain_edges  — directed upstream -> downstream relationships with tier,
--                         % revenue/cogs exposure, and confidence labels.
--   capital_flows       — per-actor, per-period, per-flow-type USD flows pulled from
--                         10-K cash flow + income statements.
--
-- Idempotent: all DDL uses IF NOT EXISTS. Unique constraints allow the loader to
-- perform ON CONFLICT DO UPDATE without creating duplicates.
--
-- ============================================================================
-- supply_chain_edges — COLUMN OWNERSHIP MATRIX (updated 2026-04-13)
-- ----------------------------------------------------------------------------
-- The edges table is written by 7 modules. To avoid silent collisions, each
-- writer owns a specific subset of columns. When editing any of these
-- modules, do NOT touch columns marked "owned by another module" unless you
-- update this matrix and the owning module in the same commit.
--
-- Base columns defined in this migration (0021):
--   upstream_id, downstream_id, relationship, tier, input_type, annual_usd,
--   pct_upstream_revenue, pct_downstream_cogs, confidence, as_of, source,
--   chokepoint_score
-- Validation columns added in migration 0030_supply_chain_edge_adjustments.sql:
--   relationship_weak, weak_since, last_validation_at, validation_correlation
-- Backtest columns added in migration 0032_supply_chain_edge_validation.sql:
--   backtest_validated, last_backtest_at
--
-- Writer ownership (INSERT and UPDATE rights):
--
--   1. ingestion/altdata/supply_chain_parser.py  (regex over 10-K filings)
--      INSERT: upstream_id, downstream_id, relationship, tier, input_type,
--              annual_usd, pct_upstream_revenue, pct_downstream_cogs,
--              confidence='derived', as_of, source (SEC URL)
--      Never touches: chokepoint_score, relationship_weak, validation_*,
--                     backtest_validated, last_backtest_at
--
--   2. ingestion/altdata/apple_supplier_list.py  (Apple CSR annual supplier list)
--      INSERT/UPSERT: upstream_id, downstream_id='aapl', relationship='supplier',
--                     tier=1, confidence='confirmed', source (Apple URL)
--      COALESCE-only updates on pct_upstream_revenue / pct_downstream_cogs
--      (will not overwrite values written by pct_cogs_enrichment).
--
--   3. ingestion/altdata/sec_item_1c_cyber.py  (10-K Item 1C cyber dependencies)
--      INSERT: cyber-vendor → registrant edges with relationship='component',
--              tier=1, confidence='derived', source (10-K section URL).
--
--   4. ingestion/altdata/regulatory_events.py  (DOJ/FTC/SEC enforcement)
--      INSERT ONLY: relationship='regulatory_threat' edges. This writer does
--      not touch pct_* or chokepoint_* columns.
--
--   5. intelligence/pct_cogs_enrichment.py  (LLM pass over 10-K text)
--      UPDATE ONLY: pct_downstream_cogs, pct_upstream_revenue, confidence
--                   (sets to 'derived'), source (citation). Never inserts
--                   fresh edges unless a harvester finding has a matching
--                   counterparty node (see `_create_new_edge`), in which
--                   case it inserts base columns + pct_* with confidence='derived'.
--      Never touches: chokepoint_score, validation_*, backtest_*
--
--   6. intelligence/supply_chokepoints.py  (graph scoring)
--      UPDATE ONLY: chokepoint_score. Guarded by `WHERE chokepoint_score IS NULL`
--      so a human-curated value is never overwritten.
--      Never touches any other column.
--
--   7. intelligence/supply_chain_edge_validator.py  (weekly correlation check)
--      UPDATE ONLY: relationship_weak, weak_since, last_validation_at,
--                   validation_correlation. Uses the pair of price series
--                   associated with upstream and downstream tickers.
--      Never touches pct_*, chokepoint_score, or base columns.
--
--   8. intelligence/postmortem.py  (backtest feedback loop)
--      UPDATE ONLY: backtest_validated, last_backtest_at, and (in the
--                   low-accuracy branch) decays pct_downstream_cogs toward
--                   the value consistent with observed price moves, writing
--                   back with confidence='derived_from_backtest'.
--      Pure confirmation path never mutates pct_*.
--
-- Invariant: pct_downstream_cogs and pct_upstream_revenue have THREE legitimate
-- writers (supply_chain_parser as initial regex, pct_cogs_enrichment as LLM
-- refinement, postmortem as backtest-adjustment). All three must agree that
-- later writers only raise confidence, never silently overwrite a higher-
-- confidence value with a lower-confidence one. Grep `confidence =` in each
-- module before changing any write path.
-- ============================================================================

CREATE TABLE IF NOT EXISTS supply_chain_nodes (
    id              TEXT PRIMARY KEY,              -- slug (e.g. "barry_callebaut") or ticker ("HSY")
    name            TEXT NOT NULL,
    type            TEXT NOT NULL,                 -- ticker | private_company | commodity | country | region | utility
    country         TEXT,
    region          TEXT,
    chokepoint_flag BOOLEAN DEFAULT FALSE,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_supply_chain_nodes_type ON supply_chain_nodes(type);

CREATE TABLE IF NOT EXISTS supply_chain_edges (
    id                    SERIAL PRIMARY KEY,
    upstream_id           TEXT NOT NULL,
    downstream_id         TEXT NOT NULL,
    relationship          TEXT NOT NULL,           -- raw_material | component | contract_mfg |
                                                   -- distribution | customer | licensor | franchisee
    tier                  INT DEFAULT 1,           -- distance from the closest focal actor
    input_type            TEXT,                    -- "cocoa beans", "aluminum cans", ...
    annual_usd            NUMERIC,
    pct_upstream_revenue  NUMERIC,                 -- supplier's % revenue from this buyer
    pct_downstream_cogs   NUMERIC,                 -- buyer's % cogs from this supplier
    chokepoint_score      NUMERIC,                 -- 0..1, null until P2-d
    confidence            TEXT NOT NULL,           -- confirmed | derived | estimated | rumored | inferred
    as_of                 DATE,
    source                TEXT NOT NULL,           -- URL or citation
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (upstream_id, downstream_id, relationship, as_of)
);
CREATE INDEX IF NOT EXISTS idx_supply_edges_upstream    ON supply_chain_edges(upstream_id);
CREATE INDEX IF NOT EXISTS idx_supply_edges_downstream  ON supply_chain_edges(downstream_id);
CREATE INDEX IF NOT EXISTS idx_supply_edges_relationship ON supply_chain_edges(relationship);

CREATE TABLE IF NOT EXISTS capital_flows (
    id              SERIAL PRIMARY KEY,
    actor_id        TEXT NOT NULL,                 -- ticker or slug
    fiscal_period   DATE NOT NULL,                 -- period end date
    period_type     TEXT NOT NULL,                 -- annual | quarter | ttm
    flow_type       TEXT NOT NULL,                 -- revenue | cogs | opex | r_and_d | capex |
                                                   -- interest_paid | tax | dividends | buybacks |
                                                   -- acquisitions | debt_issuance | equity_issuance |
                                                   -- fcf_to_equity | working_capital_delta
    direction       TEXT NOT NULL,                 -- in | out
    amount_usd      NUMERIC NOT NULL,
    counterparty_id TEXT,                          -- nullable
    source_filing   TEXT,                          -- "10-K 2024", "8-K 2025-03-10"
    confidence      TEXT NOT NULL,
    as_of           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (actor_id, fiscal_period, period_type, flow_type, counterparty_id, source_filing)
);
CREATE INDEX IF NOT EXISTS idx_capital_flows_actor ON capital_flows(actor_id, fiscal_period DESC);
CREATE INDEX IF NOT EXISTS idx_capital_flows_type  ON capital_flows(flow_type);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON supply_chain_nodes TO grid;
GRANT ALL ON supply_chain_edges TO grid;
GRANT ALL ON capital_flows      TO grid;
GRANT USAGE, SELECT ON SEQUENCE supply_chain_edges_id_seq TO grid;
GRANT USAGE, SELECT ON SEQUENCE capital_flows_id_seq      TO grid;
