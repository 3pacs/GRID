"""Add resolved_series_retractions: point-in-time retraction of resolved rows.

Revision ID: resolved_retractions_20260927
Revises: raw_series_quarantined_20260926

Why: GRID-RERESOLVE-PLAN-20260927 found 70,633 (feature, obs_date) cells whose
LATEST_AS_OF value in ``resolved_series`` is a wrong-instrument value with no
clean replacement at all (pre-listing ETH/BTC/GLD/XLRE dates, weekends and
exchange holidays), and 76,204 such FIRST_RELEASE cells. The correct answer
for those cells is "no observation". ``resolved_series.value`` is NOT NULL, a
new vintage cannot say "no value", a NaN tombstone is deleted by
``intelligence/resolution_audit.auto_fix_issues()``, and deleting the rows
would rewrite history. A retraction says it without touching the row.

Semantics (implemented by ``store/pit.py`` and the PIT readers listed in the PR):

* A retraction names one exact resolved row by its unique key
  ``(feature_id, obs_date, vintage_date)`` -- the same key as
  ``uq_resolved_series_composite``. ``vintage_date`` is NOT NULL: a wildcard
  "every vintage" retraction would also hide clean vintages written later,
  and PostgreSQL 14 (production) has no ``UNIQUE NULLS NOT DISTINCT``.
* The row is excluded from a read whose ``as_of_ts >= retracted_at``. A
  date-valued ``as_of`` (``store/pit.py``) means "known by the end of that UTC
  day", the same day-level convention as ``release_date <= as_of``. A read
  with an earlier ``as_of`` still sees the row: replays before the retraction
  reproduce what the system actually served.
* Retracting a cell's only vintage makes the cell unavailable (no row). If an
  earlier, non-retracted vintage exists, the policy picks it as usual.

Guarantees enforced in the database:

* ``fk_resolved_series_retractions_row``: a retraction must name an existing
  resolved row, and a retracted resolved row cannot be deleted (nothing is
  deleted; the evidence stays).
* ``trg_resolved_series_retractions_guard``: append-only. ``retracted_at``
  cannot be earlier than the inserting transaction's start (``now()``), so a
  retraction can never be backdated into history that was already served;
  UPDATE and DELETE are refused. Undoing a retraction is a deliberate owner
  action (disable the trigger), never a routine path.

Locks and timeouts: CREATE TABLE with the composite foreign key takes SHARE
ROW EXCLUSIVE on ``resolved_series`` -- it does not conflict with reads (ACCESS
SHARE), only waits for in-flight resolver writes, and is held for the
milliseconds of catalog work. ``lock_timeout = 5s`` bounds the wait; on
timeout nothing changes and the migration can simply be re-run.

Downgrade refuses while any retraction exists (dropping the table would
silently un-retract rows). Dropping the table also removes the FK's triggers
from ``resolved_series``, which needs a stronger lock there; with the same
5s lock_timeout it fails safely behind long readers -- re-run in a quiet
window.

GRANT footer: new table + its id sequence to ``grid`` (guarded, so a
developer database without that role still migrates).
"""

from alembic import op

revision = "resolved_retractions_20260927"
down_revision = "raw_series_quarantined_20260926"
branch_labels = None
depends_on = None

_LOCK_TIMEOUT = "5s"
_STATEMENT_TIMEOUT = "20s"

# Literal SQL throughout (no string building): reviewers read exactly what runs.

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS resolved_series_retractions (
        id            BIGSERIAL PRIMARY KEY,
        feature_id    INTEGER NOT NULL,
        obs_date      DATE NOT NULL,
        vintage_date  DATE NOT NULL,
        retracted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        reason        TEXT NOT NULL CHECK (btrim(reason) <> ''),
        run_tag       TEXT NOT NULL CHECK (btrim(run_tag) <> ''),
        CONSTRAINT uq_resolved_series_retractions_key
            UNIQUE (feature_id, obs_date, vintage_date) INCLUDE (retracted_at),
        CONSTRAINT fk_resolved_series_retractions_row
            FOREIGN KEY (feature_id, obs_date, vintage_date)
            REFERENCES resolved_series (feature_id, obs_date, vintage_date)
    )
"""

_CREATE_GUARD_FUNCTION = """
    CREATE OR REPLACE FUNCTION resolved_series_retractions_guard()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            IF NEW.retracted_at < now() THEN
                RAISE EXCEPTION
                    'resolved_series_retractions: retracted_at % is before now() %; '
                    'a retraction cannot be backdated into already-served history',
                    NEW.retracted_at, now();
            END IF;
            RETURN NEW;
        END IF;
        RAISE EXCEPTION
            'resolved_series_retractions is append-only (% refused)', TG_OP;
    END
    $$
"""

_CREATE_GUARD_TRIGGER = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_resolved_series_retractions_guard'
              AND tgrelid = 'resolved_series_retractions'::regclass
        ) THEN
            CREATE TRIGGER trg_resolved_series_retractions_guard
                BEFORE INSERT OR UPDATE OR DELETE ON resolved_series_retractions
                FOR EACH ROW EXECUTE FUNCTION resolved_series_retractions_guard();
        END IF;
    END
    $$
"""

_COMMENT = """
    COMMENT ON TABLE resolved_series_retractions IS
        'Append-only point-in-time retractions of resolved_series rows. A row '
        'is hidden from PIT reads with as_of_ts >= retracted_at and stays '
        'visible to earlier as_of. See store/pit.py and '
        'migrations/versions/resolved_retractions_20260927.py.'
"""

_GRANTS = """
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
            EXECUTE 'GRANT ALL ON resolved_series_retractions TO grid';
            EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE resolved_series_retractions_id_seq TO grid';
        END IF;
    END
    $$
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '20s'")
    op.execute(_CREATE_TABLE)
    op.execute(_CREATE_GUARD_FUNCTION)
    op.execute(_CREATE_GUARD_TRIGGER)
    op.execute(_COMMENT)
    # GRANT footer (migrations/_TEMPLATE.sql).
    op.execute(_GRANTS)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '20s'")
    # Lock first so no retraction can be written between the check and the drop.
    op.execute("""
        DO $$
        BEGIN
            IF to_regclass('resolved_series_retractions') IS NOT NULL THEN
                LOCK TABLE resolved_series_retractions IN ACCESS EXCLUSIVE MODE;
                IF EXISTS (SELECT 1 FROM resolved_series_retractions LIMIT 1) THEN
                    RAISE EXCEPTION
                        'resolved_series_retractions has rows; dropping it would '
                        'un-retract them. Refusing to downgrade '
                        'resolved_retractions_20260927.';
                END IF;
            END IF;
        END
        $$
    """)
    op.execute("DROP TABLE IF EXISTS resolved_series_retractions")
    op.execute("DROP FUNCTION IF EXISTS resolved_series_retractions_guard()")
