-- Migration: 0044_feature_importance_horizon.sql
-- Purpose: ALPHA-6 / task #109 — per-horizon feature importance.
--
-- The ALPHA-3 horizon-conditional oracle (migration 0042) split every
-- model into 4 horizon buckets (1d / 7d / 30d / 90d). Feature importance
-- should be bucketed the same way — a feature that leads 1d moves isn't
-- necessarily the same as one that leads 30d moves, so the feature
-- selection pipeline needs per-horizon scores, not one scalar.
--
-- This migration adds a horizon_days column to feature_importance_log
-- and a covering index for the per-horizon lookups that record_importance
-- + get_rankings_by_horizon will do.
--
-- Idempotent via ADD COLUMN IF NOT EXISTS. Target DB: griddb.

ALTER TABLE feature_importance_log
    ADD COLUMN IF NOT EXISTS horizon_days INTEGER;

-- Covering index for (feature_id, horizon_days, as_of_date DESC) lookups
CREATE INDEX IF NOT EXISTS idx_feature_importance_horizon
    ON feature_importance_log (feature_id, horizon_days, as_of_date DESC)
    WHERE horizon_days IS NOT NULL;

-- Covering index for (model_version_id, horizon_days) rankings
CREATE INDEX IF NOT EXISTS idx_feature_importance_model_horizon
    ON feature_importance_log (model_version_id, horizon_days, as_of_date DESC)
    WHERE horizon_days IS NOT NULL;

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
GRANT ALL ON feature_importance_log TO grid;
