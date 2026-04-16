-- Migration: 0046_canvas_speed_indexes.sql
-- Purpose: Reduce canvas graph population latency on hot actor, signal,
-- supply-chain, and capital-flow lookups.
-- Applies via: sudo -u postgres psql griddb -f migrations/0046_canvas_speed_indexes.sql
--
-- CREATE INDEX CONCURRENTLY keeps reads/writes available while these build.
-- Run this file outside a transaction block.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_capital_flows_actor_period
    ON capital_flows(actor_id, period_type, fiscal_period DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_supply_chain_edges_up_down
    ON supply_chain_edges(upstream_id, downstream_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_actors_name_lower
    ON actors(LOWER(name));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signal_data_actor_date
    ON signal_data(actor, signal_date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signal_data_upper_ticker_date
    ON signal_data(UPPER(ticker), signal_date DESC)
    WHERE ticker IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_actor_connections_a_strength
    ON actor_connections(actor_a, strength DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_actor_connections_b_strength
    ON actor_connections(actor_b, strength DESC);
