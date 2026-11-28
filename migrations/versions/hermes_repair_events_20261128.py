"""hermes_repair_events: Hermes's repair outcomes and long skips, kept across restarts.

Hermes kept every repair cooldown and every long skip in process memory, so
each restart forgot them. On 2026-11-27 grid-hermes started 15 times, and the
sources whose repeated-failure skip had tripped (TIINGO, polygon,
Asset_Registries, yfinance_options) were repaired again after every start.
Repairs also left no row anywhere: pull_log records scheduler runs only.

This revision adds one append-only table. ``scripts/hermes_repair_store.py``
writes an event for each failed repair it counts (with the digit-normalized
reason the repeated-failure rule compares), each recovery, each long skip
(with its end time and how many long skips the source has had), and each
explicit clear an operator records; on the first pass after a start Hermes
reads the last eight days back. Without the table Hermes behaves as before,
in memory only.

Running the upgrade again is a no-op. Downgrade drops the table: it holds
only repair history, which Hermes rebuilds as it goes.
"""
from alembic import op

revision = 'hermes_repair_events_20261128'
down_revision = 'df_13f_observed_20261127'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS hermes_repair_events (
            id           BIGSERIAL PRIMARY KEY,
            recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            source_key   TEXT NOT NULL,
            source_name  TEXT NOT NULL,
            event        TEXT NOT NULL
                         CHECK (event IN ('repair_failed', 'recovered', 'long_skip', 'cleared')),
            fingerprint  TEXT,
            reason       TEXT,
            skip_until   TIMESTAMPTZ,
            strikes      INTEGER,
            release      TEXT,
            CHECK (event <> 'repair_failed' OR fingerprint IS NOT NULL),
            CHECK (event <> 'long_skip' OR (skip_until IS NOT NULL AND strikes IS NOT NULL)),
            CHECK (event <> 'cleared' OR reason IS NOT NULL)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_hermes_repair_events_recorded "
        "ON hermes_repair_events (recorded_at)"
    )
    # GRANT footer (migrations/_TEMPLATE.sql): alembic runs as the owner role;
    # grid-hermes, as `grid`, inserts and reads the rows.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT ALL ON hermes_repair_events TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE hermes_repair_events_id_seq TO grid';
            END IF;
        END
        $$
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS hermes_repair_events")
