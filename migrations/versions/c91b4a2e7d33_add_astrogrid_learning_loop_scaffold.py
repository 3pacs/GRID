"""add astrogrid learning loop scaffold

Revision ID: c91b4a2e7d33
Revises: b6bba10f0fdb
Create Date: 2026-03-28 23:30:00.000000
"""

from typing import Sequence, Union

from alembic import op


revision: str = "c91b4a2e7d33"
down_revision: Union[str, Sequence[str], None] = "b6bba10f0fdb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


DDL = """
CREATE TABLE IF NOT EXISTS astrogrid.weight_version (
    id                BIGSERIAL PRIMARY KEY,
    version_key       TEXT NOT NULL UNIQUE,
    status            TEXT NOT NULL CHECK (status IN ('active', 'shadow', 'retired')),
    grid_weights      JSONB NOT NULL DEFAULT '{}'::jsonb,
    mystical_weights  JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes             TEXT,
    approved_by       TEXT,
    approved_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_weight_version_status
    ON astrogrid.weight_version (status, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.prediction_run (
    id                          BIGSERIAL PRIMARY KEY,
    prediction_id               TEXT NOT NULL UNIQUE,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    as_of_ts                    TIMESTAMPTZ NOT NULL,
    horizon_label               TEXT NOT NULL CHECK (horizon_label IN ('macro', 'swing')),
    target_universe             TEXT NOT NULL,
    target_symbols              JSONB NOT NULL DEFAULT '[]'::jsonb,
    question                    TEXT NOT NULL,
    call                        TEXT NOT NULL,
    timing                      TEXT NOT NULL,
    setup                       TEXT NOT NULL,
    invalidation                TEXT NOT NULL,
    note                        TEXT,
    seer_summary                TEXT,
    market_overlay_snapshot     JSONB NOT NULL DEFAULT '{}'::jsonb,
    mystical_feature_payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
    grid_feature_payload        JSONB NOT NULL DEFAULT '{}'::jsonb,
    weight_version              TEXT NOT NULL,
    model_version               TEXT NOT NULL,
    live_or_local               TEXT NOT NULL CHECK (live_or_local IN ('live', 'local', 'archive', 'hybrid')),
    status                      TEXT NOT NULL CHECK (status IN ('pending', 'scored', 'invalidated', 'expired')),
    comparable_publish_status   TEXT NOT NULL DEFAULT 'not_attempted' CHECK (comparable_publish_status IN ('not_attempted', 'published', 'failed')),
    comparable_prediction_ref   TEXT,
    comparable_publish_payload  JSONB NOT NULL DEFAULT '{}'::jsonb,
    lens_set_id                 BIGINT REFERENCES astrogrid.lens_set(id),
    sky_snapshot_id             BIGINT REFERENCES astrogrid.sky_snapshot(id),
    seer_run_id                 BIGINT REFERENCES astrogrid.seer_run(id),
    persona_run_id              BIGINT REFERENCES astrogrid.persona_run(id)
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_run_created
    ON astrogrid.prediction_run (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_run_status
    ON astrogrid.prediction_run (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_run_horizon
    ON astrogrid.prediction_run (horizon_label, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.prediction_postmortem (
    id                          BIGSERIAL PRIMARY KEY,
    prediction_run_id           BIGINT NOT NULL UNIQUE REFERENCES astrogrid.prediction_run(id),
    state                       TEXT NOT NULL CHECK (state IN ('pending', 'scored', 'finalized')),
    summary                     TEXT NOT NULL,
    dominant_grid_drivers       JSONB NOT NULL DEFAULT '[]'::jsonb,
    dominant_mystical_drivers   JSONB NOT NULL DEFAULT '[]'::jsonb,
    invalidation_rule           TEXT,
    feature_family_summary      JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_payload                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_postmortem_state
    ON astrogrid.prediction_postmortem (state, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.prediction_score (
    id                          BIGSERIAL PRIMARY KEY,
    prediction_run_id           BIGINT NOT NULL UNIQUE REFERENCES astrogrid.prediction_run(id),
    scored_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    benchmark_symbol            TEXT,
    realized_return             DOUBLE PRECISION,
    benchmark_return            DOUBLE PRECISION,
    alpha_vs_benchmark          DOUBLE PRECISION,
    verdict                     TEXT CHECK (verdict IN ('hit', 'miss', 'partial', 'invalidated', 'expired')),
    invalidation_status         TEXT CHECK (invalidation_status IN ('respected', 'violated', 'not_triggered', 'unknown')),
    max_favorable_excursion     DOUBLE PRECISION,
    max_adverse_excursion       DOUBLE PRECISION,
    regime_context              JSONB NOT NULL DEFAULT '{}'::jsonb,
    attribution_grid            JSONB NOT NULL DEFAULT '[]'::jsonb,
    attribution_mystical        JSONB NOT NULL DEFAULT '[]'::jsonb,
    attribution_noise           JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_payload                 JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_score_scored
    ON astrogrid.prediction_score (scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_score_verdict
    ON astrogrid.prediction_score (verdict, scored_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.backtest_run (
    id                          BIGSERIAL PRIMARY KEY,
    run_key                     TEXT NOT NULL UNIQUE,
    strategy_variant            TEXT NOT NULL CHECK (strategy_variant IN ('grid_only', 'grid_plus_mystical', 'mystical_only')),
    horizon_label               TEXT NOT NULL CHECK (horizon_label IN ('macro', 'swing')),
    target_universe             TEXT NOT NULL,
    started_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at                TIMESTAMPTZ,
    status                      TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    window_start                DATE,
    window_end                  DATE,
    params_payload              JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary_payload             JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_backtest_run_variant
    ON astrogrid.backtest_run (strategy_variant, started_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.backtest_result (
    id                          BIGSERIAL PRIMARY KEY,
    backtest_run_id             BIGINT NOT NULL REFERENCES astrogrid.backtest_run(id),
    result_key                  TEXT NOT NULL,
    strategy_variant            TEXT NOT NULL CHECK (strategy_variant IN ('grid_only', 'grid_plus_mystical', 'mystical_only')),
    target_symbol               TEXT,
    as_of_date                  DATE,
    alpha_vs_benchmark          DOUBLE PRECISION,
    metrics_payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    attribution_grid            JSONB NOT NULL DEFAULT '[]'::jsonb,
    attribution_mystical        JSONB NOT NULL DEFAULT '[]'::jsonb,
    attribution_noise           JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (backtest_run_id, result_key)
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_backtest_result_variant
    ON astrogrid.backtest_result (strategy_variant, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.review_run (
    id                          BIGSERIAL PRIMARY KEY,
    review_key                  TEXT NOT NULL UNIQUE,
    provider_mode               TEXT NOT NULL CHECK (provider_mode IN ('deterministic', 'llm', 'hybrid')),
    model_name                  TEXT,
    based_on_prediction_count   INTEGER NOT NULL DEFAULT 0,
    based_on_backtest_window    JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_payload               JSONB NOT NULL DEFAULT '{}'::jsonb,
    review_payload              JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                      TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_review_run_created
    ON astrogrid.review_run (created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.weight_proposal (
    id                          BIGSERIAL PRIMARY KEY,
    weight_proposal_id          TEXT NOT NULL UNIQUE,
    review_run_id               BIGINT REFERENCES astrogrid.review_run(id),
    based_on_prediction_count   INTEGER NOT NULL DEFAULT 0,
    based_on_backtest_window    JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_grid_weights       JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_mystical_weights   JSONB NOT NULL DEFAULT '{}'::jsonb,
    reasoning_summary           TEXT,
    confidence                  DOUBLE PRECISION CHECK (confidence BETWEEN 0 AND 1),
    status                      TEXT NOT NULL CHECK (status IN ('pending_review', 'approved', 'rejected', 'superseded')),
    approved_weight_version_id  BIGINT REFERENCES astrogrid.weight_version(id),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_weight_proposal_status
    ON astrogrid.weight_proposal (status, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.weight_proposal_decision (
    id                          BIGSERIAL PRIMARY KEY,
    decision_key                TEXT NOT NULL UNIQUE,
    weight_proposal_id          TEXT NOT NULL REFERENCES astrogrid.weight_proposal(weight_proposal_id),
    decision                    TEXT NOT NULL CHECK (decision IN ('approved', 'rejected', 'superseded')),
    decided_by                  TEXT,
    notes                       TEXT,
    approved_weight_version_id  BIGINT REFERENCES astrogrid.weight_version(id),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_weight_proposal_decision_created
    ON astrogrid.weight_proposal_decision (weight_proposal_id, created_at DESC);

DROP TRIGGER IF EXISTS trg_astrogrid_weight_version_no_mutation ON astrogrid.weight_version;
CREATE TRIGGER trg_astrogrid_weight_version_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.weight_version
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_prediction_run_no_mutation ON astrogrid.prediction_run;
CREATE TRIGGER trg_astrogrid_prediction_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.prediction_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_prediction_postmortem_no_mutation ON astrogrid.prediction_postmortem;
CREATE TRIGGER trg_astrogrid_prediction_postmortem_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.prediction_postmortem
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_prediction_score_no_mutation ON astrogrid.prediction_score;
CREATE TRIGGER trg_astrogrid_prediction_score_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.prediction_score
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_backtest_run_no_mutation ON astrogrid.backtest_run;
CREATE TRIGGER trg_astrogrid_backtest_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.backtest_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_backtest_result_no_mutation ON astrogrid.backtest_result;
CREATE TRIGGER trg_astrogrid_backtest_result_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.backtest_result
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_review_run_no_mutation ON astrogrid.review_run;
CREATE TRIGGER trg_astrogrid_review_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.review_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_weight_proposal_no_mutation ON astrogrid.weight_proposal;
CREATE TRIGGER trg_astrogrid_weight_proposal_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.weight_proposal
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_weight_proposal_decision_no_mutation ON astrogrid.weight_proposal_decision;
CREATE TRIGGER trg_astrogrid_weight_proposal_decision_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.weight_proposal_decision
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();
"""


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS astrogrid.weight_proposal_decision;
        DROP TABLE IF EXISTS astrogrid.weight_proposal;
        DROP TABLE IF EXISTS astrogrid.review_run;
        DROP TABLE IF EXISTS astrogrid.backtest_result;
        DROP TABLE IF EXISTS astrogrid.backtest_run;
        DROP TABLE IF EXISTS astrogrid.prediction_score;
        DROP TABLE IF EXISTS astrogrid.prediction_postmortem;
        DROP TABLE IF EXISTS astrogrid.prediction_run;
        DROP TABLE IF EXISTS astrogrid.weight_version;
        """
    )
