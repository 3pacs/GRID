-- Migration: 0038_oracle_running_metrics
-- Purpose: Running Brier / ECE counters for the calibration handler (SYNTH-21).
--
-- The contracts.handlers.calibration.on_prediction_scored handler delegates to
-- oracle.calibration.update_running_metrics(), which maintains incremental
-- running averages on every PredictionScored event. These columns back that
-- running state so the calibration subsystem never has to re-scan the full
-- oracle_predictions table after each score cycle.
--
-- Additive only: existing oracle_models rows keep their prior weight/hit
-- counters untouched. New columns default to NULL / 0 and are populated the
-- first time a prediction for that model is scored through the contracts
-- dispatcher.
--
-- Deployment: this migration is NOT applied automatically by scripts/deploy.py
-- — it ships to both trees so an operator can run it manually with
--   psql -h <host> -U grid -d grid -f migrations/0038_oracle_running_metrics.sql

BEGIN;

ALTER TABLE oracle_models
    ADD COLUMN IF NOT EXISTS running_brier DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS running_ece DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS scored_prediction_count INTEGER DEFAULT 0;

COMMENT ON COLUMN oracle_models.running_brier IS
    'Incremental running-mean Brier score updated by contracts.handlers.calibration.';
COMMENT ON COLUMN oracle_models.running_ece IS
    'Incremental running-mean absolute calibration error (ECE proxy).';
COMMENT ON COLUMN oracle_models.scored_prediction_count IS
    'Denominator for the running averages above.';

GRANT ALL ON oracle_models TO grid;

COMMIT;
