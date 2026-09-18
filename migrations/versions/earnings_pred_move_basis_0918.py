"""Record which inputs an earnings prediction's expected move was built from.

Revision ID: earnings_pred_move_basis_0918
Revises: god_view_market_tables_20260918
Create Date: 2026-09-18

Why this exists
---------------
``intelligence/earnings_intel.py`` used to publish ``predicted_move_pct`` as a
fixed blend::

    predicted_move = hist_avg * 0.3 + signals.get("expected_move_options", 2.0) * 0.7

With no options snapshot the "IV-implied" 70% of that number was the literal
``2.0`` -- a constant wearing the label of a market-implied move (fake-data
audit findings B-H8 / B-M20). The honest replacement drops each term that has
no input, so ``predicted_move_pct`` can now be history-only, options-only, or
``NULL``. A stored row therefore has to say *which*, or a consumer cannot tell
a blended forecast from a single-sided one:

* ``predicted_move_basis`` -- ``history_and_options`` / ``options_only`` /
  ``history_only`` / ``unavailable``.
* ``expected_move_options`` -- the IV-implied term on its own, ``NULL`` when no
  ``options_daily_signals`` row produced one. Read by the PWA earnings view,
  which renders "Exp Move (no IV)" instead of a number when it is ``NULL``.

Rows written before this revision carry ``NULL`` in both. ``NULL`` basis is not
``unavailable``: it means the row predates the distinction and nothing is known
about which inputs it used.

Why ``IF EXISTS`` on the table
------------------------------
``earnings_predictions`` is created by ``intelligence/earnings_intel.py``'s
``_ensure_tables()`` (``CREATE TABLE IF NOT EXISTS``), never by alembic, and it
is *not* in ``schema.sql``. On a database where no earnings endpoint has run
yet the table is simply absent, and the module's own ``CREATE TABLE`` already
carries the new shape -- so the guard makes this revision a no-op there rather
than a hard failure. ``ADD COLUMN IF NOT EXISTS`` likewise makes it a no-op on
any database where the superseded runtime ``ALTER`` in that module already ran.

That "silently skipped" property is the hazard: if the module's DDL and this
revision drift apart, neither statement complains. ``tests/
test_earnings_predictions_schema_parity.py`` compares the two column-for-column
so the drift is caught in CI instead of in production.

No GRANT footer: this alters (or skips) an existing table whose privileges are
already held by the ``grid`` role that every service connects as.

Locking
-------
Both statements are ``ADD COLUMN`` of a nullable column with no default, which
on PostgreSQL 14 is a catalog-only change -- no table rewrite, no scan. It
still takes ``ACCESS EXCLUSIVE`` on ``earnings_predictions`` for the duration,
so it queues behind any open transaction holding a lock on the table and blocks
every reader behind it while it waits. ``earnings_predictions`` is small (it is
written only by the earnings endpoints, at most 50 predictions per cycle), and
alembic runs at deploy time rather than inside request handling, so no bound is
set here beyond the deploy's own.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
# 29 characters -- alembic_version.version_num is VARCHAR(32); see
# tests/test_alembic_single_head.py::test_revision_ids_fit_the_version_column.
revision: str = "earnings_pred_move_basis_0918"
down_revision: str | Sequence[str] | None = "god_view_market_tables_20260918"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE IF EXISTS earnings_predictions "
        "ADD COLUMN IF NOT EXISTS predicted_move_basis TEXT"
    )
    op.execute(
        "ALTER TABLE IF EXISTS earnings_predictions "
        "ADD COLUMN IF NOT EXISTS expected_move_options DOUBLE PRECISION"
    )


def downgrade() -> None:
    # Data loss, stated plainly: dropping these columns discards, for every row
    # written since the upgrade, the record of which inputs produced
    # predicted_move_pct and what the IV-implied term was. It is not
    # recomputable after the fact -- the options_daily_signals row the move was
    # derived from is keyed by signal_date and has moved on.
    #
    # Nothing else in the schema references either column (no FK, no index, no
    # view, no generated column), and predicted_move_pct itself is untouched,
    # so the drop is otherwise contained: rows survive, they just go back to
    # being silent about their provenance.
    op.execute(
        "ALTER TABLE IF EXISTS earnings_predictions "
        "DROP COLUMN IF EXISTS expected_move_options"
    )
    op.execute(
        "ALTER TABLE IF EXISTS earnings_predictions "
        "DROP COLUMN IF EXISTS predicted_move_basis"
    )
