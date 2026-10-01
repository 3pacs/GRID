"""Keep every options capture batch; publish the latest complete batch as a view.

Before this revision ``options_snapshots`` was a table and every writer
deleted a ticker/day before inserting, so a later capture (e.g. the scheduler's
after-close pull) silently destroyed an earlier one (e.g. GEM's 10:05 NY
batch).

After this revision:

* ``options_snapshots_all`` (the renamed table) keeps every row of every
  capture. Rows are immutable: UPDATE, DELETE and TRUNCATE raise.
* ``options_capture_batches`` registers one row per complete capture batch
  (ticker, snap_date, capture_ordinal, start/completion, row_count, spot,
  writer). A snapshot row written after this revision must carry full batch
  metadata and reference its registered batch (NOT VALID CHECK + FK, so the
  existing rows are left untouched and unscanned by the constraints).
* ``options_snapshots`` becomes a view that shows, per (ticker, snap_date),
  only the rows of the highest-ordinal registered batch -- or the legacy rows
  when that day has no registered batch. That is exactly what the old
  delete-then-insert table held (the last complete capture), so every existing
  reader -- including the frozen, pinned GEX-levels v1 paper-log code, which
  sums every row of a snap_date -- sees unchanged semantics, while earlier
  batches stay replayable from ``options_snapshots_all`` by capture_batch_id.

No row is deleted or rewritten. Existing consistent batches (every one on
production at 2026-09-30) are registered with ``backfilled = true``.

Revision ID: options_append_only_20260930
Revises: security_master_20260927
"""

from alembic import op

revision = "options_append_only_20260930"
down_revision = "security_master_20260927"
branch_labels = None
depends_on = None



def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '15min'")

    # Lock order (review note): everything that scans the 2 GB table runs
    # BEFORE the rename, under locks that only block writers (SHARE for the
    # index build, ACCESS SHARE for the backfill scan). The ACCESS EXCLUSIVE
    # rename comes last, so readers are blocked only for the catalog-only
    # statements that follow it.
    # Many batches per ticker/day: uniqueness is per batch, not per day.
    op.execute("""
        CREATE UNIQUE INDEX options_snapshots_all_batch_contract_key
            ON options_snapshots (capture_batch_id, expiry, opt_type, strike)
            WHERE capture_batch_id IS NOT NULL
    """)

    op.execute("""
        CREATE TABLE options_capture_batches (
            capture_batch_id     TEXT PRIMARY KEY,
            ticker               TEXT NOT NULL,
            snap_date            DATE NOT NULL,
            capture_ordinal      BIGINT NOT NULL CHECK (capture_ordinal > 0),
            capture_started_at   TIMESTAMPTZ NOT NULL,
            capture_completed_at TIMESTAMPTZ NOT NULL,
            row_count            INTEGER NOT NULL CHECK (row_count > 0),
            spot_price           DOUBLE PRECISION,
            capture_source       TEXT NOT NULL,
            backfilled           BOOLEAN NOT NULL DEFAULT false,
            registered_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CHECK (capture_started_at <= capture_completed_at),
            CONSTRAINT options_capture_batches_identity_key
                UNIQUE (capture_batch_id, capture_ordinal, ticker, snap_date),
            -- One ordinal per ticker/day, so "latest batch" is never a tie.
            CONSTRAINT options_capture_batches_day_ordinal_key
                UNIQUE (ticker, snap_date, capture_ordinal)
        )
    """)

    # Register existing batches only when their rows are internally
    # consistent (one ticker/day/ordinal/start/completion). An inconsistent
    # batch stays unregistered and is still shown only while no registered
    # batch outranks it; it can never satisfy a batch replay.
    op.execute("""
        INSERT INTO options_capture_batches (
            capture_batch_id, ticker, snap_date, capture_ordinal,
            capture_started_at, capture_completed_at, row_count,
            spot_price, capture_source, backfilled)
        SELECT capture_batch_id, MIN(ticker), MIN(snap_date), MIN(capture_ordinal),
               MIN(capture_started_at), MIN(capture_completed_at), COUNT(*),
               NULL, 'pre_append_only', true
        FROM options_snapshots
        WHERE capture_batch_id IS NOT NULL
        GROUP BY capture_batch_id
        HAVING COUNT(DISTINCT ticker) = 1
           AND COUNT(DISTINCT snap_date) = 1
           AND COUNT(DISTINCT capture_ordinal) = 1
           AND COUNT(DISTINCT capture_started_at) = 1
           AND COUNT(DISTINCT capture_completed_at) = 1
           AND bool_and(capture_ordinal IS NOT NULL AND capture_ordinal > 0
                        AND capture_started_at IS NOT NULL
                        AND capture_completed_at IS NOT NULL)
           AND MIN(capture_started_at) <= MIN(capture_completed_at)
    """)

    op.execute("ALTER TABLE options_snapshots RENAME TO options_snapshots_all")
    op.execute("""
        ALTER TABLE options_snapshots_all
            DROP CONSTRAINT options_snapshots_ticker_snap_date_expiry_opt_type_strike_key
    """)

    # New rows must be full batch members of a registered batch. NOT VALID:
    # enforced for every future INSERT, existing rows are not rechecked.
    op.execute("""
        ALTER TABLE options_snapshots_all
            ADD CONSTRAINT options_snapshots_all_batch_required CHECK (
                capture_batch_id IS NOT NULL AND capture_ordinal IS NOT NULL
                AND capture_started_at IS NOT NULL
                AND capture_completed_at IS NOT NULL
                AND provider_regular_market_at IS NOT NULL
            ) NOT VALID
    """)
    op.execute("""
        ALTER TABLE options_snapshots_all
            ADD CONSTRAINT options_snapshots_all_batch_fk
            FOREIGN KEY (capture_batch_id, capture_ordinal, ticker, snap_date)
            REFERENCES options_capture_batches
                (capture_batch_id, capture_ordinal, ticker, snap_date)
            NOT VALID
    """)

    op.execute("""
        CREATE FUNCTION options_capture_append_only_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'options capture history is append-only: % on % refused',
                TG_OP, TG_TABLE_NAME USING ERRCODE = 'restrict_violation';
        END
        $$
    """)
    op.execute("""
        CREATE TRIGGER options_snapshots_all_no_row_mutation
            BEFORE UPDATE OR DELETE ON options_snapshots_all
            FOR EACH ROW EXECUTE FUNCTION options_capture_append_only_guard()
    """)
    op.execute("""
        CREATE TRIGGER options_snapshots_all_no_truncate
            BEFORE TRUNCATE ON options_snapshots_all
            FOR EACH STATEMENT EXECUTE FUNCTION options_capture_append_only_guard()
    """)
    op.execute("""
        CREATE TRIGGER options_capture_batches_no_row_mutation
            BEFORE UPDATE OR DELETE ON options_capture_batches
            FOR EACH ROW EXECUTE FUNCTION options_capture_append_only_guard()
    """)
    op.execute("""
        CREATE TRIGGER options_capture_batches_no_truncate
            BEFORE TRUNCATE ON options_capture_batches
            FOR EACH STATEMENT EXECUTE FUNCTION options_capture_append_only_guard()
    """)

    # Latest complete batch per ticker/day. A row is hidden when a different
    # registered batch for the same ticker/day has an ordinal at least as
    # high (legacy NULL-ordinal rows count as ordinal 0).
    op.execute("""
        CREATE VIEW options_snapshots AS
        SELECT s.id, s.ticker, s.snap_date, s.expiry, s.opt_type, s.strike,
               s.last_price, s.bid, s.ask, s.volume, s.open_interest,
               s.implied_vol, s.in_the_money, s.created_at,
               s.capture_batch_id, s.capture_ordinal, s.capture_started_at,
               s.capture_completed_at, s.provider_regular_market_at
        FROM options_snapshots_all s
        WHERE NOT EXISTS (
            SELECT 1 FROM options_capture_batches b
            WHERE b.ticker = s.ticker
              AND b.snap_date = s.snap_date
              AND b.capture_batch_id IS DISTINCT FROM s.capture_batch_id
              AND b.capture_ordinal >= COALESCE(s.capture_ordinal, 0)
        )
    """)


def downgrade() -> None:
    """Restore the single-table layout only if no day holds two batches.

    Refuses (raises) rather than deleting any captured batch.
    """
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '15min'")
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM options_snapshots_all
                GROUP BY ticker, snap_date
                HAVING COUNT(DISTINCT COALESCE(capture_batch_id, '')) > 1
            ) THEN
                RAISE EXCEPTION 'downgrade refused: options_snapshots_all holds more than one '
                                'capture batch for a ticker/day; export or retire them explicitly';
            END IF;
        END
        $$
    """)
    op.execute("DROP VIEW options_snapshots")
    op.execute("DROP TRIGGER options_snapshots_all_no_truncate ON options_snapshots_all")
    op.execute("DROP TRIGGER options_snapshots_all_no_row_mutation ON options_snapshots_all")
    op.execute("DROP TRIGGER options_capture_batches_no_truncate ON options_capture_batches")
    op.execute("DROP TRIGGER options_capture_batches_no_row_mutation ON options_capture_batches")
    op.execute("DROP FUNCTION options_capture_append_only_guard()")
    op.execute("ALTER TABLE options_snapshots_all DROP CONSTRAINT options_snapshots_all_batch_fk")
    op.execute("ALTER TABLE options_snapshots_all DROP CONSTRAINT options_snapshots_all_batch_required")
    op.execute("DROP INDEX options_snapshots_all_batch_contract_key")
    op.execute("""
        ALTER TABLE options_snapshots_all
            ADD CONSTRAINT options_snapshots_ticker_snap_date_expiry_opt_type_strike_key
            UNIQUE (ticker, snap_date, expiry, opt_type, strike)
    """)
    op.execute("ALTER TABLE options_snapshots_all RENAME TO options_snapshots")
    op.execute("DROP TABLE options_capture_batches")
