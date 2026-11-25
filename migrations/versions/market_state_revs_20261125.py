"""market_state_daily keeps every version it replaces (#1191).

Revision ID: market_state_revs_20261125
Revises: gdelt_doc_retire_20261123
Create Date: 2026-11-25 00:00:00.000000

``scripts/run_market_state.py --replace`` used ``INSERT ... ON CONFLICT (as_of_date) DO UPDATE``, so a replace
overwrote a session's decision record and its ``computed_at`` in place. A consumer that joins on ``computed_at``
(the desk brief, #1191's as-of join) then never saw the revised record early, but a replay at a time between the
original write and the revision saw neither version: the table could no longer say what it had said. #1191's
2026-11-16 addendum asks for late and revised rows to keep their original vintage. This is the same design as
regime_history's archive (#1302, migrations/versions/regime_history_revs_20261122.py).

After this migration:

* ``market_state_daily_revisions`` (append-only) holds one row per superseded version: its rule and record, when
  it was written (``computed_at``), when it was superseded, the operation, and the reason and run that replaced it.
* ``market_state_daily_keep_revisions`` (BEFORE INSERT OR UPDATE OR DELETE, per row) owns every version clock.
  Writers cannot set ``computed_at``: an INSERT gets the mutation clock, an UPDATE that changes neither ``rule``
  nor ``record`` keeps the old clock and writes no revision, and a changing UPDATE or DELETE copies OLD into the
  revisions.
* Clocks are naive UTC, like ``computed_at`` has always been on griddb (session TimeZone Etc/UTC): the trigger
  reads ``clock_timestamp() AT TIME ZONE 'UTC'``, so a session in another time zone cannot shift them.
  ``now()`` (the old ``CURRENT_TIMESTAMP``) would not do: it is the transaction's start, so a transaction that
  began before another committed a change, then replaced that change, would archive it with a time before it
  existed. A changed row's new version starts exactly when the old one ends.
* The versions of one session never overlap (Codex 6052801895). Every INSERT, changing UPDATE and DELETE first
  takes that session's transaction-scoped advisory lock, held to commit, and only then reads the clock. The clock
  must be later than the epoch, than the version it replaces and than every superseded_at already archived for
  the session; otherwise the change is refused (a clock that stepped back is not bumped forward). So a session
  deleted and inserted again starts its new version after the deleted one ended, even when the two transactions
  race, and the changes to one session are stamped in the order they commit.
* A mutation clock is not commit visibility. A version is written at T and visible to readers only from its
  commit, later by the rest of its transaction. Reconstruction from the clocks answers "what had been written by
  T"; a reader at T saw what had committed by T. For one session the two differ only while a change's
  transaction is open: the next change cannot read its clock before that commit. Each revision records
  ``superseded_by_xid`` (``pg_current_xact_id()``).
* ``market_state_daily_revision_epoch`` holds the moment revisions began. Unlike regime_history's ``created_at``,
  the old upsert did move ``computed_at`` on every replace, so a version written before the epoch keeps its
  writer-stamped clock. The upgrade refuses a row whose clock is at or after the epoch (griddb had none on
  2026-11-25), and the trigger never stamps a clock at or before it, so ``computed_at_by_trigger``
  (``computed_at >= epoch``) is exact: true only for clocks the trigger stamped.
* Revisions are written only by the trigger: a direct INSERT is refused (``pg_trigger_depth()``), so a replay
  cannot be shown a version that was never in the table.
* A writer names its reason and run with ``set_config('grid.market_state_reason', ..., true)`` and
  ``grid.market_state_run`` (``store_market_state`` always does on a replace). A change without them (a hand edit,
  an older release) is archived, not refused: ``attributed`` is false, the reason says so and the run is the
  session's ``application_name``.
* as_of_date cannot change. TRUNCATE of the table is refused, and revisions and the epoch refuse UPDATE, DELETE
  and TRUNCATE.

``market_state_daily`` itself is created here if it is missing (``ensure_table`` used to be its only definition),
with the DDL ``ensure_table`` runs, so a fresh database gets the archive with it. The upgrade then takes a
writer-conflicting lock on it before it stamps the epoch and holds it until the triggers are installed, so no row
can be written after the epoch without the trigger's clock. Existing rows are not touched.

``schema.sql`` declares the same objects (the migration replay applies schema.sql first, so every statement is
idempotent). Downgrade refuses while any revision exists: dropping the table would destroy the only copy of what
market_state_daily used to say.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "market_state_revs_20261125"
down_revision: Union[str, Sequence[str], None] = "gdelt_doc_retire_20261123"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# SHARE ROW EXCLUSIVE conflicts with every INSERT, UPDATE and DELETE (ROW EXCLUSIVE) but not with SELECT.
# Not in schema.sql: a fresh database has no concurrent writers.
WRITE_LOCK = "LOCK TABLE market_state_daily IN SHARE ROW EXCLUSIVE MODE"

# Each statement below also appears verbatim in schema.sql (tests/test_market_state_revisions.py), and
# MARKET_STATE_TABLE is intelligence/regime/market_state.py's _TABLE_SQL.
MARKET_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS market_state_daily (
    as_of_date   DATE PRIMARY KEY,
    rule         TEXT NOT NULL,
    record       JSONB NOT NULL,
    computed_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

EPOCH_TABLE = """
CREATE TABLE IF NOT EXISTS market_state_daily_revision_epoch (
    singleton       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    revisions_from  TIMESTAMP NOT NULL
)
"""

# Stamped once. A row already stamped at or after the epoch would read as trigger-stamped and could end a version
# after a later one began, so the first stamp refuses it.
EPOCH_ROW = """
DO $$
DECLARE
    stamped TIMESTAMP;
    ahead BIGINT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM market_state_daily_revision_epoch) THEN
        stamped := clock_timestamp() AT TIME ZONE 'UTC';
        SELECT count(*) INTO ahead FROM market_state_daily WHERE computed_at >= stamped;
        IF ahead > 0 THEN
            RAISE EXCEPTION 'market_state_daily: % row(s) stamped at or after the revisions epoch %; refused',
                ahead, stamped;
        END IF;
        INSERT INTO market_state_daily_revision_epoch (revisions_from) VALUES (stamped);
    END IF;
END
$$
"""

REVISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS market_state_daily_revisions (
    id                      BIGSERIAL PRIMARY KEY,
    as_of_date              DATE NOT NULL,
    rule                    TEXT NOT NULL,
    record                  JSONB NOT NULL,
    computed_at             TIMESTAMP NOT NULL,
    computed_at_by_trigger  BOOLEAN NOT NULL,
    superseded_at           TIMESTAMP NOT NULL,
    superseded_by_xid       TEXT NOT NULL,
    operation               TEXT NOT NULL CHECK (operation IN ('UPDATE', 'DELETE')),
    reason                  TEXT NOT NULL CHECK (btrim(reason) <> ''),
    run_tag                 TEXT NOT NULL CHECK (btrim(run_tag) <> ''),
    attributed              BOOLEAN NOT NULL,
    CHECK (superseded_at > computed_at)
)
"""

REVISIONS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_market_state_daily_revisions_as_of
    ON market_state_daily_revisions (as_of_date, superseded_at)
"""

KEEP_REVISIONS_FUNCTION = """
CREATE OR REPLACE FUNCTION market_state_daily_keep_revisions()
RETURNS TRIGGER AS $$
DECLARE
    why TEXT := nullif(btrim(coalesce(current_setting('grid.market_state_reason', true), '')), '');
    run TEXT := nullif(btrim(coalesce(current_setting('grid.market_state_run', true), '')), '');
    app TEXT := nullif(btrim(coalesce(current_setting('application_name', true), '')), '');
    clock TIMESTAMP;
    epoch TIMESTAMP;
    latest TIMESTAMP;
    session_day DATE;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'market_state_daily keeps every version: TRUNCATE refused';
    END IF;
    IF TG_OP = 'INSERT' THEN
        session_day := NEW.as_of_date;
    ELSE
        session_day := OLD.as_of_date;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.as_of_date IS DISTINCT FROM OLD.as_of_date THEN
            RAISE EXCEPTION 'market_state_daily: as_of_date % cannot change', OLD.as_of_date;
        END IF;
        IF (NEW.rule, NEW.record) IS NOT DISTINCT FROM (OLD.rule, OLD.record) THEN
            NEW.computed_at := OLD.computed_at;
            RETURN NEW;
        END IF;
    END IF;
    -- One session's changes, one at a time: the lock is held to commit and the clock is read after it.
    PERFORM pg_advisory_xact_lock(hashtext('market_state_daily'), session_day - DATE '2000-01-01');
    clock := clock_timestamp() AT TIME ZONE 'UTC';
    SELECT revisions_from INTO epoch FROM market_state_daily_revision_epoch;
    IF epoch IS NULL THEN
        RAISE EXCEPTION 'market_state_daily: no revisions epoch; refused';
    END IF;
    SELECT max(superseded_at) INTO latest FROM market_state_daily_revisions WHERE as_of_date = session_day;
    latest := greatest(epoch, latest);
    IF TG_OP <> 'INSERT' THEN
        latest := greatest(latest, OLD.computed_at);
    END IF;
    IF clock <= latest THEN
        RAISE EXCEPTION 'market_state_daily %: database clock % is not later than %, already recorded for it; refused',
            session_day, clock, latest;
    END IF;
    IF TG_OP = 'INSERT' THEN
        NEW.computed_at := clock;
        RETURN NEW;
    END IF;
    INSERT INTO market_state_daily_revisions
        (as_of_date, rule, record, computed_at, computed_at_by_trigger, superseded_at, superseded_by_xid,
         operation, reason, run_tag, attributed)
    VALUES
        (OLD.as_of_date, OLD.rule, OLD.record, OLD.computed_at, OLD.computed_at >= epoch,
         clock, pg_current_xact_id()::text,
         TG_OP, coalesce(why, 'unattributed: the writer named no grid.market_state_reason'),
         coalesce(run, 'unattributed: application_name ' || coalesce(app, '(none)')),
         why IS NOT NULL AND run IS NOT NULL);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    NEW.computed_at := clock;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""

APPEND_ONLY_FUNCTION = """
CREATE OR REPLACE FUNCTION market_state_daily_revisions_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % refused', TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql
"""

# Depth 1 is a direct INSERT; the keep-revisions trigger's own INSERT runs at depth 2.
FROM_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION market_state_daily_revisions_from_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() < 2 THEN
        RAISE EXCEPTION 'market_state_daily_revisions is written only by market_state_daily_keep_revisions: '
                        'direct INSERT refused';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""

TRIGGERS = (
    "DROP TRIGGER IF EXISTS market_state_daily_keep_revisions ON market_state_daily",
    """CREATE TRIGGER market_state_daily_keep_revisions
    BEFORE INSERT OR UPDATE OR DELETE ON market_state_daily
    FOR EACH ROW EXECUTE FUNCTION market_state_daily_keep_revisions()""",
    "DROP TRIGGER IF EXISTS market_state_daily_no_truncate ON market_state_daily",
    """CREATE TRIGGER market_state_daily_no_truncate
    BEFORE TRUNCATE ON market_state_daily
    FOR EACH STATEMENT EXECUTE FUNCTION market_state_daily_keep_revisions()""",
    "DROP TRIGGER IF EXISTS market_state_daily_revisions_from_trigger ON market_state_daily_revisions",
    """CREATE TRIGGER market_state_daily_revisions_from_trigger
    BEFORE INSERT ON market_state_daily_revisions
    FOR EACH ROW EXECUTE FUNCTION market_state_daily_revisions_from_trigger()""",
    "DROP TRIGGER IF EXISTS market_state_daily_revisions_no_mutation ON market_state_daily_revisions",
    """CREATE TRIGGER market_state_daily_revisions_no_mutation
    BEFORE UPDATE OR DELETE ON market_state_daily_revisions
    FOR EACH ROW EXECUTE FUNCTION market_state_daily_revisions_append_only()""",
    "DROP TRIGGER IF EXISTS market_state_daily_revisions_no_truncate ON market_state_daily_revisions",
    """CREATE TRIGGER market_state_daily_revisions_no_truncate
    BEFORE TRUNCATE ON market_state_daily_revisions
    FOR EACH STATEMENT EXECUTE FUNCTION market_state_daily_revisions_append_only()""",
    "DROP TRIGGER IF EXISTS market_state_daily_revision_epoch_no_mutation ON market_state_daily_revision_epoch",
    """CREATE TRIGGER market_state_daily_revision_epoch_no_mutation
    BEFORE UPDATE OR DELETE ON market_state_daily_revision_epoch
    FOR EACH ROW EXECUTE FUNCTION market_state_daily_revisions_append_only()""",
    "DROP TRIGGER IF EXISTS market_state_daily_revision_epoch_no_truncate ON market_state_daily_revision_epoch",
    """CREATE TRIGGER market_state_daily_revision_epoch_no_truncate
    BEFORE TRUNCATE ON market_state_daily_revision_epoch
    FOR EACH STATEMENT EXECUTE FUNCTION market_state_daily_revisions_append_only()""",
)

# The writer role inserts revisions through the trigger (which runs as the invoker); it never needs to change them.
# On griddb the application role (grid) also owns market_state_daily, so these grants are not a privilege boundary:
# the integrity of the history rests on the triggers (append-only, clocks owned by the trigger), not on roles.
GRANTS = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON market_state_daily TO grid';
        EXECUTE 'GRANT SELECT ON market_state_daily_revision_epoch TO grid';
        EXECUTE 'GRANT SELECT, INSERT ON market_state_daily_revisions TO grid';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE market_state_daily_revisions_id_seq TO grid';
    END IF;
END
$$
"""


def upgrade() -> None:
    # A writer mid-transaction makes the lock wait; past 10 s the deploy's migration fails and can be rerun, rather
    # than hold every later writer queued behind it.
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    # Block every market_state_daily writer before the epoch is stamped, and keep blocking them until the triggers
    # are installed (the lock lasts to the end of the migration's transaction). Readers are not blocked.
    op.execute(MARKET_STATE_TABLE)
    op.execute(WRITE_LOCK)
    op.execute(EPOCH_TABLE)
    op.execute(EPOCH_ROW)
    op.execute(REVISIONS_TABLE)
    op.execute(REVISIONS_INDEX)
    op.execute(KEEP_REVISIONS_FUNCTION)
    op.execute(APPEND_ONLY_FUNCTION)
    op.execute(FROM_TRIGGER_FUNCTION)
    for statement in TRIGGERS:
        op.execute(statement)
    op.execute(GRANTS)


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM market_state_daily_revisions) THEN
                RAISE EXCEPTION 'downgrade refused: market_state_daily_revisions holds superseded '
                                'market_state_daily versions, and dropping it would destroy them';
            END IF;
        END
        $$
        """
    )
    op.execute("DROP TRIGGER IF EXISTS market_state_daily_keep_revisions ON market_state_daily")
    op.execute("DROP TRIGGER IF EXISTS market_state_daily_no_truncate ON market_state_daily")
    op.execute("DROP TABLE IF EXISTS market_state_daily_revisions")
    op.execute("DROP TABLE IF EXISTS market_state_daily_revision_epoch")
    op.execute("DROP FUNCTION IF EXISTS market_state_daily_keep_revisions()")
    op.execute("DROP FUNCTION IF EXISTS market_state_daily_revisions_append_only()")
    op.execute("DROP FUNCTION IF EXISTS market_state_daily_revisions_from_trigger()")
