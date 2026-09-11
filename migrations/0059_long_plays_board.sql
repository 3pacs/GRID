-- Migration: 0059_long_plays_board.sql
-- Author: long-plays agent (task #27)
-- Applies via: sudo -u postgres psql griddb -f migrations/0059_long_plays_board.sql
--
-- Purpose: one row per run of the weekly Long Plays board
--          (intelligence/long_plays.py::build_long_plays_board). The board is
--          a JSON snapshot of multi-year candidates with explicit multiple
--          math; ``candidates`` carries the full per-ticker payload so the
--          API (GET /api/v1/conviction/long-plays) can serve the latest board
--          without recomputing it.
--
-- Populated by: intelligence.long_plays.persist_board (Sunday 05:30 job in
--               intelligence/scheduler.py, and the admin refresh endpoint).
-- Consumed by:  intelligence.long_plays.load_latest_board,
--               api.routers.conviction (/long-plays), alerts.email.daily_digest.
--
-- Idempotent: every CREATE uses IF NOT EXISTS; GRANTs are no-ops on re-run.
-- The module also runs the same DDL via ensure_long_plays_table() so a missing
-- migration degrades to a self-created table rather than a failed job.

-- ====== SCHEMA CHANGES ======

CREATE TABLE IF NOT EXISTS long_plays_board (
    id                BIGSERIAL PRIMARY KEY,
    as_of             DATE NOT NULL,                 -- PIT cut-off used for every read
    generated_at      TIMESTAMPTZ NOT NULL,
    universe_size     INTEGER NOT NULL DEFAULT 0,
    candidates        JSONB NOT NULL,                -- ordered by asymmetry_score DESC
    stand_down_reason TEXT,                          -- NULL when >= 1 entry_candidate
    method_notes      JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_long_plays_board_generated_at
    ON long_plays_board (generated_at DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON long_plays_board TO grid;
GRANT USAGE, SELECT ON SEQUENCE long_plays_board_id_seq TO grid;
