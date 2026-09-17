"""Record the scanner score behind every options recommendation.

Revision ID: options_rec_scanner_score_0917
Revises: snapshot_actor_col_20260914
Create Date: 2026-09-17

Why this exists
---------------
``trading/options_recommender.py`` used to derive a win probability from an
affine function of the scanner score::

    base_prob = 0.30 + (score - 5.0) * 0.06

Nothing in that line was ever checked against an outcome, yet it fed
``kelly_fraction``, ``suggested_contracts`` and ``expected_return`` — it
decided position size (fake-data audit finding C-H6).

The honest replacement reads the realised win rate for the score bucket the
candidate falls in, out of ``options_recommendations`` rows that already
closed WIN or LOSS. That lookup needs the score each row was generated at,
and the table never stored it — ``confidence`` is a *different*, separately
tuned number and using it as a score proxy would be the same defect wearing
a new label.

This revision adds the column. Until enough rows accumulate a score AND an
outcome, ``win_probability`` is ``NULL`` and every number derived from it is
``NULL`` too — which is the point.

Idempotent: ``ADD COLUMN IF NOT EXISTS`` is a no-op where it already ran.
No GRANT footer is needed — this alters an existing table whose privileges
are already held by the ``grid`` role.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "options_rec_scanner_score_0917"
down_revision: str | Sequence[str] | None = "snapshot_actor_col_20260914"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE options_recommendations "
        "ADD COLUMN IF NOT EXISTS scanner_score DOUBLE PRECISION"
    )
    op.execute(
        "COMMENT ON COLUMN options_recommendations.scanner_score IS "
        "'discovery.options_scanner composite score (0-10) this recommendation "
        "was generated from. Buckets the empirical win-rate lookup in "
        "trading.options_recommender._empirical_win_probability; NULL on rows "
        "written before 2026-09-17, which are therefore excluded from it.'"
    )
    # Supports the bucketed COUNT(*) FILTER over closed rows.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_options_rec_score_outcome "
        "ON options_recommendations (scanner_score, outcome) "
        "WHERE scanner_score IS NOT NULL AND outcome IN ('WIN', 'LOSS')"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_options_rec_score_outcome")
    op.execute(
        "ALTER TABLE options_recommendations DROP COLUMN IF EXISTS scanner_score"
    )
