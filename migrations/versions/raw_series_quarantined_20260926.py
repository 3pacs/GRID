"""Allow pull_status = 'QUARANTINED' in raw_series.

Revision ID: raw_series_quarantined_20260926
Revises: robinhood_guards_20260924

QUARANTINED marks a raw_series row that was once accepted but has since been
found untrustworthy (for example a vintage written under the wrong price
basis). It is neither an observation nor a pull failure:

* ``store/observations.py`` and ``normalization/resolver.py`` read
  ``pull_status = 'SUCCESS'`` only, so a quarantined row stops being served;
* the failure counters (``scripts/hermes_health.py``, ``scripts/hermes_fixers.py``,
  ``intelligence/source_quality_ablation.py``) count ``= 'FAILED'`` /
  ``= 'PARTIAL'`` only, so it does not page anyone as a failed pull;
* ``scripts/hermes_health.check_db_health`` reports it on its own, as
  ``quarantined_rows``.

Pullers never write it. It is set only by an explicit quarantine operation
that keeps its own backup of the rows it relabels.

Why this is safe on production raw_series (~1.9e9 rows, 512 GB, plain heap
table on PostgreSQL 14: not partitioned, no TimescaleDB, no inheritance
children, no dependent views, checked read-only on 2026-09-26):

* ``ADD CONSTRAINT ... CHECK (...) NOT VALID`` records the constraint in the
  catalog without scanning existing rows. It is enforced for every INSERT and
  UPDATE from the moment the transaction commits.
* ``DROP CONSTRAINT`` and ``RENAME CONSTRAINT`` are catalog-only as well.
* All three take ACCESS EXCLUSIVE on raw_series, held only until this
  transaction commits (milliseconds of catalog work). ``lock_timeout = 5s``
  bounds how long the migration waits behind a long-running reader/writer --
  and so how long new raw_series queries queue behind the migration's
  pending lock request. If it times out, nothing changed; just re-run.
* The constraint keeps its original name (``raw_series_pull_status_check``,
  the name PostgreSQL generated from schema.sql's inline CHECK) via
  add-under-a-temporary-name, drop-old, rename. A fresh database built from
  schema.sql and production therefore carry the same constraint name.

``VALIDATE CONSTRAINT`` is deliberately NOT run here: it scans the whole
table (SHARE UPDATE EXCLUSIVE, does not block reads/writes, but reads 512 GB).
Every existing row already satisfied the stricter old constraint, so
validation can only succeed; it is optional later maintenance, e.g. in a
quiet window::

    SET statement_timeout = 0;
    ALTER TABLE raw_series VALIDATE CONSTRAINT raw_series_pull_status_check;

Downgrade restores the three-value constraint, also NOT VALID. Because
NOT VALID does not check existing rows, the downgrade first checks
(index-backed, via idx_raw_series_status_source_pull) that no QUARANTINED rows
remain and aborts otherwise: the quarantine must be reversed from its backup
before downgrading, or those rows would silently violate the restored
constraint (and any later UPDATE of them, or a VALIDATE, would fail).

No GRANT footer: no table, sequence, or schema is created, and constraint
changes do not touch privileges.
"""

from alembic import op

revision = "raw_series_quarantined_20260926"
down_revision = "robinhood_guards_20260924"
branch_labels = None
depends_on = None

_LOCK_TIMEOUT = "5s"
_STATEMENT_TIMEOUT = "15s"

# Literal SQL throughout (no string building): every identifier and value
# below is fixed, and reviewers can read exactly what runs on production.


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '15s'")
    # First statement takes ACCESS EXCLUSIVE; the rest run under the same lock.
    op.execute(
        "ALTER TABLE raw_series ADD CONSTRAINT raw_series_pull_status_check_next "
        "CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED', 'QUARANTINED')) "
        "NOT VALID"
    )
    op.execute(
        "ALTER TABLE raw_series DROP CONSTRAINT IF EXISTS raw_series_pull_status_check"
    )
    op.execute(
        "ALTER TABLE raw_series RENAME CONSTRAINT raw_series_pull_status_check_next "
        "TO raw_series_pull_status_check"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '15s'")
    # Take the lock before checking, so no QUARANTINED row can be written
    # between the check and the constraint swap.
    op.execute("LOCK TABLE raw_series IN ACCESS EXCLUSIVE MODE")
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM raw_series WHERE pull_status = 'QUARANTINED' LIMIT 1
            ) THEN
                RAISE EXCEPTION
                    'raw_series still has QUARANTINED rows; reverse the quarantine '
                    'from its backup before downgrading raw_series_quarantined_20260926';
            END IF;
        END
        $$
    """)
    op.execute(
        "ALTER TABLE raw_series ADD CONSTRAINT raw_series_pull_status_check_next "
        "CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED')) NOT VALID"
    )
    op.execute(
        "ALTER TABLE raw_series DROP CONSTRAINT IF EXISTS raw_series_pull_status_check"
    )
    op.execute(
        "ALTER TABLE raw_series RENAME CONSTRAINT raw_series_pull_status_check_next "
        "TO raw_series_pull_status_check"
    )
