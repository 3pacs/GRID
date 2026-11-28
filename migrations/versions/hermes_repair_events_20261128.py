"""hermes_repair_events: Hermes's repair outcomes and long skips, kept across restarts.

Hermes kept its repeated-failure counts and long skips in process memory, so
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

Order: each writer takes a per-source transaction advisory lock before it
inserts and numbers its rows ``seq`` = the source's highest ``seq`` + 1 under
that lock, so for one source ``seq`` is the commit order (a unique index on
``(source_key, seq)`` refuses a writer that skipped the lock). Wall time is
not used for order: two rows can share a timestamp and a clock can step back.
``recorded_at`` (``clock_timestamp()``) only bounds the eight-day window.
``occurred_at`` is the instant the live code acted on (a recovery's time,
which a restart compares with failure times exactly as the process did).

The table is append-only for every role: a trigger refuses UPDATE, DELETE
and TRUNCATE, and ``grid`` is granted SELECT and INSERT only. Retention is not
automatic; the rows are small (one per counted failed repair, long skip,
recovery or clear) and Hermes reads only the last eight days.

Running the upgrade again is a no-op. Downgrade drops the table and its
trigger function: they hold only repair history, which Hermes rebuilds as it
goes.
"""
from alembic import op

revision = 'hermes_repair_events_20261128'
# After #1518's df_etf_flow_basis_20261128, which landed first (coordinator's order).
down_revision = 'df_etf_flow_basis_20261128'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS hermes_repair_events (
            id           BIGSERIAL PRIMARY KEY,
            recorded_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            seq          BIGINT NOT NULL,
            occurred_at  TIMESTAMPTZ,
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
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_hermes_repair_events_clears "
        "ON hermes_repair_events (recorded_at) WHERE event = 'cleared'"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_hermes_repair_events_source_seq "
        "ON hermes_repair_events (source_key, seq)"
    )
    # Append-only for every role, the owner included: a row is history, and
    # rewriting one would change what a restart restores. Grants alone do not
    # do this where the application role owns the table.
    op.execute("""
        CREATE OR REPLACE FUNCTION hermes_repair_events_append_only() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'hermes_repair_events is append-only (% refused)', TG_OP;
        END
        $$
    """)
    op.execute("DROP TRIGGER IF EXISTS hermes_repair_events_no_rewrite ON hermes_repair_events")
    op.execute(
        "CREATE TRIGGER hermes_repair_events_no_rewrite BEFORE UPDATE OR DELETE "
        "ON hermes_repair_events FOR EACH ROW EXECUTE FUNCTION hermes_repair_events_append_only()"
    )
    op.execute("DROP TRIGGER IF EXISTS hermes_repair_events_no_truncate ON hermes_repair_events")
    op.execute(
        "CREATE TRIGGER hermes_repair_events_no_truncate BEFORE TRUNCATE "
        "ON hermes_repair_events FOR EACH STATEMENT EXECUTE FUNCTION hermes_repair_events_append_only()"
    )
    # Grants: alembic runs as the owner role; grid-hermes, as `grid`, only
    # inserts and reads. The table is append-only, so `grid` gets no UPDATE,
    # DELETE or TRUNCATE (unlike the GRANT ALL footer of migrations/_TEMPLATE.sql).
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT SELECT, INSERT ON hermes_repair_events TO grid';
                EXECUTE 'GRANT USAGE ON SEQUENCE hermes_repair_events_id_seq TO grid';
            END IF;
        END
        $$
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS hermes_repair_events")
    op.execute("DROP FUNCTION IF EXISTS hermes_repair_events_append_only()")
