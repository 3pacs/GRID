"""Re-apply the corrected analytical_snapshots FTS objects.

Revision ID: reapply_snapshot_fts_20260911
Revises: regime_history_data_as_of
Create Date: 2026-09-11 18:30:00.000000

`phase4_fts_intelligence_search` (revision ``phase4_fts_001``) built the
``analytical_snapshots`` tsvector from ``title`` and ``summary``. That table
has neither — its text columns are ``category`` and ``subcategory``. #473
corrected the migration source, but griddb had *already* run and recorded
``phase4_fts_001``, and alembic never re-runs an applied revision. So on the
live database that correction is inert:

  - the broken trigger was installed 2026-09-11 05:55 UTC, failed every write
    to the table with ``record "new" has no field "title"``, and was dropped
    by hand to unblock writes;
  - the corrected trigger was therefore never installed;
  - ``search_vector`` is NULL across every row, so the GIN index indexes
    nothing and the 'snapshot' arm of ``intelligence_search`` is empty;
  - nothing errors any more, because with no trigger there is nothing to
    fail — the corpus is simply, silently, unsearchable.

A correction to an already-applied revision only helps a fresh install. This
revision re-applies the ``analytical_snapshots`` half of ``phase4_fts_001`` in
its corrected form, so a database that already ran the broken version
converges on the same state as a fresh one.

Every statement is idempotent, so this is safe whatever the live database
currently holds: the column and GIN index are guarded, the function is
CREATE OR REPLACE, the trigger is dropped before being created, the backfill
only touches NULL rows, and the materialized view is rebuilt from the
corrected definition.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "reapply_snapshot_fts_20260911"
down_revision: Union[str, Sequence[str], None] = "regime_history_data_as_of"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── column + index (no-ops where phase4_fts_001 already made them) ──
    op.execute(sa.text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'analytical_snapshots'
                  AND column_name = 'search_vector'
            ) THEN
                ALTER TABLE analytical_snapshots ADD COLUMN search_vector tsvector;
            END IF;
        END $$
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_analytical_snapshots_search_vector
        ON analytical_snapshots USING GIN(search_vector)
    """))

    # ── the corrected trigger: category/subcategory, not title/summary ──
    op.execute(sa.text("""
        CREATE OR REPLACE FUNCTION analytical_snapshots_search_vector_update()
        RETURNS trigger AS $$
        BEGIN
            NEW.search_vector := to_tsvector(
                'english',
                COALESCE(NEW.category, '') || ' ' || COALESCE(NEW.subcategory, '')
            );
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql
    """))

    op.execute(sa.text("""
        DROP TRIGGER IF EXISTS trg_analytical_snapshots_search_vector
        ON analytical_snapshots
    """))

    op.execute(sa.text("""
        CREATE TRIGGER trg_analytical_snapshots_search_vector
        BEFORE INSERT OR UPDATE ON analytical_snapshots
        FOR EACH ROW EXECUTE FUNCTION analytical_snapshots_search_vector_update()
    """))

    # ── backfill the rows the original run left NULL ──
    op.execute(sa.text("""
        UPDATE analytical_snapshots
        SET search_vector = to_tsvector(
            'english',
            COALESCE(category, '') || ' ' || COALESCE(subcategory, '')
        )
        WHERE search_vector IS NULL
    """))

    # ── rebuild the view so its snapshot arm matches the real columns ──
    op.execute(sa.text("DROP MATERIALIZED VIEW IF EXISTS intelligence_search"))

    op.execute(sa.text("""
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
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_intelligence_search_tsv
        ON intelligence_search USING GIN(tsv)
    """))

    op.execute(sa.text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_intelligence_search_pk
        ON intelligence_search(source_type, source_id)
    """))


def downgrade() -> None:
    # Deliberately does not drop the column, index, trigger or view: those are
    # phase4_fts_001's objects, and that revision stays applied underneath this
    # one. Undoing this revision would otherwise mean reconstructing a *broken*
    # trigger, which is not a state worth returning to — so the downgrade only
    # clears the vectors this revision backfilled.
    op.execute(sa.text("UPDATE analytical_snapshots SET search_vector = NULL"))
