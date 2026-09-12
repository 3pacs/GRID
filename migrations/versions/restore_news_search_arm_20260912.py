"""Restore the news arm of intelligence_search.

Revision ID: restore_news_search_arm_20260912
Revises: reapply_snapshot_fts_20260911
Create Date: 2026-09-12 00:45:00.000000

``phase4_fts_news`` (revision ``phase4_fts_002``) added a fifth ``UNION ALL``
branch to ``intelligence_search`` so ``news_articles`` — the highest-velocity
feed in the system — is searchable alongside actors, signals, hypotheses and
snapshots.

``reapply_snapshot_fts_20260911`` then rebuilt the view to correct the
snapshot arm, but it rebuilt it from ``phase4_fts_001``'s four-arm definition,
which predates the news branch. Both revisions are recorded applied on griddb,
so the live view is the four-arm one and the news branch is simply gone:

    source_type | rows
    ------------+---------
    actor       | 3389147
    hypothesis  |  215629
    signal      |  199421
    snapshot    |  556042

with 75,230 rows in ``news_articles`` indexed by nothing. Nothing errors —
search just quietly returns no news, which is the same failure mode the
snapshot arm had.

Alembic will not re-run ``phase4_fts_002``, so converging needs a new
revision. This one rebuilds the view with all five arms: the four the reapply
left in place, unchanged, plus the news branch restored verbatim from
``phase4_fts_002``.

Rebuilding a 1.4 GB materialized view is not free, so the upgrade first reads
the live view definition and returns without touching anything when the news
branch is already present — a fresh database that ran ``phase4_fts_002`` last
is already correct and pays nothing.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "restore_news_search_arm_20260912"
down_revision: Union[str, Sequence[str], None] = "reapply_snapshot_fts_20260911"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Four arms exactly as reapply_snapshot_fts_20260911 left them, plus the news
# arm from phase4_fts_002. Every id is cast to text so the UNION column types
# agree (actors.id and discovered_hypotheses.id are text; news_articles.id,
# signal_data.id and analytical_snapshots.id are integer/bigint).
_VIEW_SQL = """
CREATE MATERIALIZED VIEW intelligence_search AS
SELECT 'actor' AS source_type, id::text AS source_id, name AS title,
       COALESCE(category, '') || ' ' || COALESCE(title, '') || ' ' || name AS body,
       to_tsvector('english', COALESCE(name, '') || ' ' || COALESCE(category, '') || ' ' || COALESCE(title, '')) AS tsv
FROM actors WHERE name IS NOT NULL
UNION ALL
SELECT 'signal' AS source_type, id::text,
       COALESCE(signal_type, '') || ': ' || COALESCE(ticker, '') AS title,
       COALESCE(description, '') AS body,
       to_tsvector('english', COALESCE(description, '') || ' ' || COALESCE(ticker, '') || ' ' || COALESCE(actor, '')) AS tsv
FROM signal_data WHERE description IS NOT NULL
UNION ALL
SELECT 'hypothesis' AS source_type, id::text,
       COALESCE(thesis, '') AS title,
       COALESCE(thesis, '') || ' ' || COALESCE(pattern_type, '') AS body,
       to_tsvector('english', COALESCE(thesis, '')) AS tsv
FROM discovered_hypotheses WHERE thesis IS NOT NULL
UNION ALL
SELECT 'snapshot' AS source_type, id::text,
       COALESCE(category, '') AS title,
       COALESCE(subcategory, '') AS body,
       to_tsvector('english', COALESCE(category, '') || ' ' || COALESCE(subcategory, '')) AS tsv
FROM analytical_snapshots WHERE category IS NOT NULL
UNION ALL
SELECT 'news' AS source_type, id::text,
       COALESCE(title, '') AS title,
       COALESCE(NULLIF(llm_summary, ''), summary, '') || ' ' || COALESCE(array_to_string(tickers, ' '), '') AS body,
       to_tsvector('english',
           COALESCE(title, '') || ' '
           || COALESCE(NULLIF(llm_summary, ''), summary, '') || ' '
           || COALESCE(array_to_string(tickers, ' '), '') || ' '
           || COALESCE(source, '')
       ) AS tsv
FROM news_articles WHERE title IS NOT NULL
"""


def _view_definition(bind: sa.engine.Connection) -> str | None:
    """Return the live definition of ``intelligence_search``, or None.

    Parameters:
        bind: Connection to read catalog metadata through.

    Returns:
        str | None: The materialized view's SQL, or None if it does not exist.
    """
    return bind.execute(
        sa.text(
            "SELECT pg_get_viewdef(c.oid, true) "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = :name AND c.relkind = 'm' "
            "  AND n.nspname = ANY(current_schemas(false))"
        ),
        {"name": "intelligence_search"},
    ).scalar()


def upgrade() -> None:
    bind = op.get_bind()

    existing = _view_definition(bind)
    if existing is not None and "news_articles" in existing:
        # Already five-armed (fresh install, or this revision re-run). Skip the
        # rebuild rather than churn a 1.4 GB matview and its GIN index.
        return

    op.execute(sa.text("DROP MATERIALIZED VIEW IF EXISTS intelligence_search"))
    op.execute(sa.text(_VIEW_SQL))

    op.execute(sa.text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_intelligence_search_pk
        ON intelligence_search(source_type, source_id)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_intelligence_search_tsv
        ON intelligence_search USING GIN(tsv)
    """))


def downgrade() -> None:
    # Nothing to undo: the four arms this revision preserves are
    # reapply_snapshot_fts_20260911's, and that revision stays applied
    # underneath. Reversing would mean rebuilding a view that omits 75k news
    # rows on purpose, which is not a state worth returning to.
    pass
