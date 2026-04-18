-- Migration: 0027_supply_chain_enrichment_log
-- Purpose: Audit every LLM extraction attempt against a supply_chain_edges row
--          so we can prove the anti-hallucination guardrails are working and
--          measure acceptance rate over time.
--
-- Populated by: intelligence.pct_cogs_enrichment.PctCogsEnricher
-- Consumed by:  scripts/run_pct_cogs_enrichment.py audit summary
--               (also queryable from psql for ad-hoc inspection)
--
-- Idempotent: all DDL uses IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS supply_chain_enrichment_log (
    id              SERIAL PRIMARY KEY,
    edge_id         INT NOT NULL,                 -- supply_chain_edges.id
    ticker          TEXT NOT NULL,                -- downstream ticker queried
    upstream_id     TEXT NOT NULL,                -- supplier node id
    upstream_label  TEXT,                         -- human-readable supplier name at attempt time
    field           TEXT NOT NULL,                -- pct_downstream_cogs | pct_upstream_revenue
    attempted_at    TIMESTAMPTZ DEFAULT NOW(),
    llm_provider    TEXT NOT NULL,                -- ollama-qwen2.5:7b | llamacpp-nemotron-120b | etc
    llm_model       TEXT,
    passage_chars   INT,                          -- length of the input window
    raw_response    TEXT,                         -- full LLM response, truncated to 4 KB
    parsed_pct      NUMERIC,                      -- parsed pct (may be null even when accepted=false)
    parsed_citation TEXT,                         -- the verbatim quote, may be null
    accepted        BOOLEAN NOT NULL,             -- was the value written back?
    reason          TEXT NOT NULL                 -- ok | no_passage | no_llm_response | bad_json |
                                                  -- pct_out_of_range | citation_not_in_text |
                                                  -- citation_missing | not_disclosed |
                                                  -- update_failed | duplicate
);

CREATE INDEX IF NOT EXISTS idx_enrichment_log_edge       ON supply_chain_enrichment_log(edge_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_ticker     ON supply_chain_enrichment_log(ticker);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_accepted   ON supply_chain_enrichment_log(accepted);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_attempt_at ON supply_chain_enrichment_log(attempted_at DESC);
