"""signal_sources.trust_score — drop the 0.5 default so NULL means "unscored".

Revision ID: signal_sources_trust_nodefault
Revises: oracle_pred_nullable_0918 (this PR is stacked on #537, which is stacked on #544 at the top of the remediation chain; the chain stays linear)
Create Date: 2026-09-17 00:00:00.000000

``schema.sql`` declared ``trust_score NUMERIC DEFAULT 0.5``. Every row the
ingestion path wrote therefore arrived already carrying 0.5, and stayed at 0.5
until ``intelligence/trust_scorer.py`` scored the source — which for most
sources it never has. That value is not a prior a reader can recognise as one:
it is byte-identical to a source the scorer genuinely measured at 0.5, and it
reached users as a half-filled trust bar on ``/api/v1/watchlist/{t}/edge``, as
the mean behind ``convergence.combined_confidence``, and as the number carried
on a high-severity convergence WebSocket alert. Audit finding C-M22
(``docs/audits/fake-data-2026-09-17/``); vocabulary from
``docs/reference/CONFIDENCE_POLICY.md`` and
``docs/reference/AVAILABILITY_CONTRACT.md``: a missing measurement is ``null``
plus a basis, never the midpoint.

After this revision a freshly ingested row has ``trust_score IS NULL`` until a
scorer writes one, and the readers fixed alongside it
(``trust_scorer.get_insider_edge`` / ``detect_convergence`` and
``api/routers/watchlist_overview.py``) serve ``null`` + a basis for it rather
than 0.5.

Existing rows are deliberately NOT rewritten
--------------------------------------------
There is no ``UPDATE`` here. Historical repair is on hold, and it could not be
done honestly from inside a migration anyway: **rows written before this change
still carry 0.5, and nothing in the table distinguishes a defaulted 0.5 from a
0.5 the scorer measured.** ``scored_at`` is the only column that would tell
them apart, and it is NULL on both the never-scored rows and on rows written by
the pre-#534 writer paths, so a blanket ``UPDATE signal_sources SET trust_score
= NULL WHERE trust_score = 0.5`` would erase genuine measurements along with the
defaults. Leaving the old rows alone is the smaller error: the value is wrong
but the row count is honest, and the ambiguity is bounded to rows created before
this deploy.

Read-side note
--------------
Until a full ``update_trust_scores`` pass has covered the table, a reader cannot
treat ``trust_score = 0.5`` on a pre-existing row as measured. Consumers that
need certainty should require ``scored_at IS NOT NULL`` alongside
``trust_score IS NOT NULL``; consumers that only need to avoid *inventing* a
number (the ``/edge`` router, ``detect_convergence``) are already correct,
because they now pass NULL through as ``null`` instead of substituting 0.5.
Once the scorer has run over the backlog, the remaining 0.5 values are measured
ones and the distinction stops mattering.

Downgrade restores ``DEFAULT 0.5`` so the column matches the pre-revision DDL
exactly. It does not, and cannot, restore which rows were defaulted.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "signal_sources_trust_nodefault"
down_revision: Union[str, Sequence[str], None] = "oracle_pred_nullable_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Guarded on the table: signal_sources is created by schema.sql and, on some
    # deployments, lazily by intelligence/trust_scorer.py::_ensure_tables, so a
    # database that has not run either yet must not fail the whole upgrade.
    # DROP DEFAULT is a catalog-only change — no table rewrite, no row touched.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'signal_sources'
            ) THEN
                ALTER TABLE signal_sources
                    ALTER COLUMN trust_score DROP DEFAULT;
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'signal_sources'
            ) THEN
                ALTER TABLE signal_sources
                    ALTER COLUMN trust_score SET DEFAULT 0.5;
            END IF;
        END
        $$
        """
    )
