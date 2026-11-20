"""Immutable model-attributed forward forecasts; legacy rows remain unchanged.

Revision ID: oracle_forward_v2_20261110
Revises: ag_backtest_invalid_20261110

Cutover is intentionally transactional (no concurrently-built invalid index):
apply this migration before swapping/restarting all three updated writers.
Quiescing Oracle generation, API publishes and vault sync avoids transient
publish failures, but is not required for integrity: pre-cutover writes finish
before the exclusive lock and remain legacy; old post-cutover writes fail.
Hourly scoring updates outcomes only and does not insert forecasts.
The old arbiter cannot coexist with the new one: it rejects distinct models.
Old ON CONFLICT statements fail after cutover rather than overwrite forecasts.
A lock/build timeout rolls back the entire schema change; never retry by
rewriting dedup_keep or historical forecasts. Existing legacy rows stay NULL.
Downgrade is refused because collapsing models would destroy the new identity.
Rollback requires a v2-compatible application or forward fix; an old release
can read existing records but cannot resume forecast publishing.
"""
from alembic import op

revision = "oracle_forward_v2_20261110"
down_revision = "ag_backtest_invalid_20261110"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '120s'")
    op.execute("""
        DO $$
        BEGIN
            IF to_regclass('oracle_predictions') IS NULL THEN
                RETURN;
            END IF;
            LOCK TABLE oracle_predictions IN ACCESS EXCLUSIVE MODE;
            ALTER TABLE oracle_predictions ADD COLUMN IF NOT EXISTS recording_policy TEXT;
            DROP INDEX IF EXISTS oracle_predictions_dedup_unique;
            CREATE UNIQUE INDEX oracle_predictions_dedup_unique
            ON oracle_predictions (
                ticker,
                direction,
                expiry,
                prediction_type,
                model_name,
                (COALESCE(model_version, '')),
                ((created_at AT TIME ZONE 'UTC')::date),
                (COALESCE(recording_policy, 'legacy'))
            ) WHERE dedup_keep = TRUE;
        END
        $$
    """)
    # GRANT footer: no new table or sequence; preserve runtime access.
    op.execute("""
        DO $$
        BEGIN
            IF to_regclass('oracle_predictions') IS NOT NULL
               AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                GRANT SELECT, INSERT, UPDATE ON oracle_predictions TO grid;
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    raise RuntimeError("Forward identity cannot be collapsed; restore compatible application code instead")
