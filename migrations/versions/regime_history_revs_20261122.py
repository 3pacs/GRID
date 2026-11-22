"""regime_history keeps every version it replaces (#1302).

Revision ID: regime_history_revs_20261122
Revises: agent_report_ask_checks_20261122
Create Date: 2026-11-22 00:00:00.000000

``scripts/auto_regime.py`` used to upsert ``regime_history`` with ``ON CONFLICT
(obs_date) DO UPDATE``, so a later run rewrote the label that an earlier reader
(trial_signal's BUY gate, the chat regime context) had already acted on, and the
table could no longer say what it had said. ``trial_signals.regime_at_signal``
kept a copy; ``regime_history`` kept nothing.

After this migration:

* ``regime_history_revisions`` (append-only) holds one row per superseded
  version: its values, when it was written (``created_at``), when it was
  superseded, the operation, and the reason and run that replaced it.
* ``regime_history_keep_revisions`` (BEFORE INSERT OR UPDATE OR DELETE, per row)
  owns every version clock. Writers cannot set ``created_at``: an INSERT gets the
  mutation clock, an UPDATE that changes no value keeps the old clock and writes
  no revision, and a changing UPDATE or DELETE copies OLD into the revisions.
* Version clocks are mutation clocks: ``clock_timestamp()`` read inside the
  trigger, after the row lock is held, and forced strictly later than the
  version being replaced. ``now()`` would not do: it is the transaction's start,
  so a transaction that began before another committed a change, then replaced
  that change, would archive it with a time before it existed. A changed row's
  new version starts exactly when the old one ends.
* A mutation clock is not commit visibility. A version can be written at T but
  committed, and so visible to readers, later. Each revision records
  ``superseded_by_xid`` (``pg_current_xact_id()``) so commit order can be
  established separately, for example with ``pg_xact_commit_timestamp`` where
  ``track_commit_timestamp`` is on. Reconstruction from the clocks answers "what
  had been written by T", not "what a reader at T could see".
* ``regime_history_revision_epoch`` holds the moment revisions began. A
  version written before it (any row present at migration time) has an unknown
  start, because the old upsert never moved ``created_at``. Its revision
  therefore records ``created_at`` NULL, never the misleading first-insert time.
* A writer names its reason and run with ``set_config('grid.regime_history_reason',
  ..., true)`` and ``grid.regime_history_run`` (scripts/auto_regime.py always does).
  A change without them is archived, not refused: ``attributed`` is false, the
  reason says so and the run is the session's ``application_name``. The scheduler
  is pinned to an older release (grid-scheduler's zz-release-worktree.conf, at
  235e9467 on 2026-11-22) whose auto_regime still upserts without a reason.
  Refusing would freeze each day's label at its first run, so changes are kept
  until a follow-up tightens this.
* obs_date cannot change. TRUNCATE of either table is refused, and revisions
  refuse UPDATE and DELETE.

Existing rows are not touched. ``schema.sql`` declares the same objects (the
migration replay applies schema.sql first, so every statement is idempotent).
Downgrade refuses while any revision exists: dropping the table would destroy
the only copy of what regime_history used to say.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "regime_history_revs_20261122"
down_revision: Union[str, Sequence[str], None] = "agent_report_ask_checks_20261122"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Each statement also appears verbatim in schema.sql (tests/test_regime_history_revisions.py).
REVISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS regime_history_revisions (
    id             BIGSERIAL PRIMARY KEY,
    obs_date       DATE NOT NULL,
    regime         TEXT NOT NULL,
    confidence     DOUBLE PRECISION,
    source         TEXT,
    data_as_of     DATE,
    created_at     TIMESTAMPTZ,
    superseded_at  TIMESTAMPTZ NOT NULL,
    superseded_by_xid TEXT NOT NULL,
    operation      TEXT NOT NULL CHECK (operation IN ('UPDATE', 'DELETE')),
    reason         TEXT NOT NULL CHECK (btrim(reason) <> ''),
    run_tag        TEXT NOT NULL CHECK (btrim(run_tag) <> ''),
    attributed     BOOLEAN NOT NULL
)
"""

EPOCH_TABLE = """
CREATE TABLE IF NOT EXISTS regime_history_revision_epoch (
    singleton       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    revisions_from  TIMESTAMPTZ NOT NULL
)
"""

EPOCH_ROW = """
INSERT INTO regime_history_revision_epoch (revisions_from)
SELECT clock_timestamp() WHERE NOT EXISTS (SELECT 1 FROM regime_history_revision_epoch)
"""

REVISIONS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_regime_history_revisions_obs_date
    ON regime_history_revisions (obs_date, superseded_at)
"""

KEEP_REVISIONS_FUNCTION = """
CREATE OR REPLACE FUNCTION regime_history_keep_revisions()
RETURNS TRIGGER AS $$
DECLARE
    why TEXT := nullif(btrim(coalesce(current_setting('grid.regime_history_reason', true), '')), '');
    run TEXT := nullif(btrim(coalesce(current_setting('grid.regime_history_run', true), '')), '');
    app TEXT := nullif(btrim(coalesce(current_setting('application_name', true), '')), '');
    clock TIMESTAMPTZ := clock_timestamp();
    epoch TIMESTAMPTZ;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'regime_history keeps every version: TRUNCATE refused';
    END IF;
    IF TG_OP = 'INSERT' THEN
        NEW.created_at := clock;
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.obs_date IS DISTINCT FROM OLD.obs_date THEN
        RAISE EXCEPTION 'regime_history: obs_date % cannot change', OLD.obs_date;
    END IF;
    IF TG_OP = 'UPDATE' AND (NEW.regime, NEW.confidence, NEW.source, NEW.data_as_of)
            IS NOT DISTINCT FROM (OLD.regime, OLD.confidence, OLD.source, OLD.data_as_of) THEN
        NEW.created_at := OLD.created_at;
        RETURN NEW;
    END IF;
    IF OLD.created_at IS NOT NULL AND clock <= OLD.created_at THEN
        clock := OLD.created_at + interval '1 microsecond';
    END IF;
    SELECT revisions_from INTO epoch FROM regime_history_revision_epoch;
    INSERT INTO regime_history_revisions
        (obs_date, regime, confidence, source, data_as_of, created_at, superseded_at, superseded_by_xid,
         operation, reason, run_tag, attributed)
    VALUES
        (OLD.obs_date, OLD.regime, OLD.confidence, OLD.source, OLD.data_as_of,
         CASE WHEN epoch IS NULL OR OLD.created_at IS NULL OR OLD.created_at < epoch
              THEN NULL ELSE OLD.created_at END,
         clock, pg_current_xact_id()::text,
         TG_OP, coalesce(why, 'unattributed: the writer named no grid.regime_history_reason'),
         coalesce(run, 'unattributed: application_name ' || coalesce(app, '(none)')),
         why IS NOT NULL AND run IS NOT NULL);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    NEW.created_at := clock;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""

APPEND_ONLY_FUNCTION = """
CREATE OR REPLACE FUNCTION regime_history_revisions_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % refused', TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql
"""

TRIGGERS = (
    "DROP TRIGGER IF EXISTS regime_history_keep_revisions ON regime_history",
    """CREATE TRIGGER regime_history_keep_revisions
    BEFORE INSERT OR UPDATE OR DELETE ON regime_history
    FOR EACH ROW EXECUTE FUNCTION regime_history_keep_revisions()""",
    "DROP TRIGGER IF EXISTS regime_history_no_truncate ON regime_history",
    """CREATE TRIGGER regime_history_no_truncate
    BEFORE TRUNCATE ON regime_history
    FOR EACH STATEMENT EXECUTE FUNCTION regime_history_keep_revisions()""",
    "DROP TRIGGER IF EXISTS regime_history_revisions_no_mutation ON regime_history_revisions",
    """CREATE TRIGGER regime_history_revisions_no_mutation
    BEFORE UPDATE OR DELETE ON regime_history_revisions
    FOR EACH ROW EXECUTE FUNCTION regime_history_revisions_append_only()""",
    "DROP TRIGGER IF EXISTS regime_history_revisions_no_truncate ON regime_history_revisions",
    """CREATE TRIGGER regime_history_revisions_no_truncate
    BEFORE TRUNCATE ON regime_history_revisions
    FOR EACH STATEMENT EXECUTE FUNCTION regime_history_revisions_append_only()""",
    "DROP TRIGGER IF EXISTS regime_history_revision_epoch_no_mutation ON regime_history_revision_epoch",
    """CREATE TRIGGER regime_history_revision_epoch_no_mutation
    BEFORE UPDATE OR DELETE ON regime_history_revision_epoch
    FOR EACH ROW EXECUTE FUNCTION regime_history_revisions_append_only()""",
    "DROP TRIGGER IF EXISTS regime_history_revision_epoch_no_truncate ON regime_history_revision_epoch",
    """CREATE TRIGGER regime_history_revision_epoch_no_truncate
    BEFORE TRUNCATE ON regime_history_revision_epoch
    FOR EACH STATEMENT EXECUTE FUNCTION regime_history_revisions_append_only()""",
)

# The writer role inserts revisions through the trigger (which runs as the invoker); it never needs to change them.
# On griddb the application role also owns regime_history, so these grants are not a privilege boundary: the
# integrity of the history rests on the triggers (append-only, clocks owned by the trigger), not on roles.
GRANTS = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
        EXECUTE 'GRANT SELECT ON regime_history_revision_epoch TO grid';
        EXECUTE 'GRANT SELECT, INSERT ON regime_history_revisions TO grid';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE regime_history_revisions_id_seq TO grid';
    END IF;
END
$$
"""


def upgrade() -> None:
    op.execute(EPOCH_TABLE)
    op.execute(EPOCH_ROW)
    op.execute(REVISIONS_TABLE)
    op.execute(REVISIONS_INDEX)
    op.execute(KEEP_REVISIONS_FUNCTION)
    op.execute(APPEND_ONLY_FUNCTION)
    for statement in TRIGGERS:
        op.execute(statement)
    op.execute(GRANTS)


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM regime_history_revisions) THEN
                RAISE EXCEPTION 'downgrade refused: regime_history_revisions holds superseded '
                                'regime_history versions, and dropping it would destroy them';
            END IF;
        END
        $$
        """
    )
    op.execute("DROP TRIGGER IF EXISTS regime_history_keep_revisions ON regime_history")
    op.execute("DROP TRIGGER IF EXISTS regime_history_no_truncate ON regime_history")
    op.execute("DROP TABLE IF EXISTS regime_history_revisions")
    op.execute("DROP TABLE IF EXISTS regime_history_revision_epoch")
    op.execute("DROP FUNCTION IF EXISTS regime_history_keep_revisions()")
    op.execute("DROP FUNCTION IF EXISTS regime_history_revisions_append_only()")
