"""Phase 4.1: Full-text intelligence search

Revision ID: phase4_fts_001
Revises: a1b2c3d4e5f6
Create Date: 2026-04-08 22:00:00.000000

Adds PostgreSQL full-text search (tsvector + GIN) across the intelligence
corpus: actors, signal_data, discovered_hypotheses, analytical_snapshots.

Creates a materialized view `intelligence_search` that UNIONs all searchable
content with pre-computed tsvector columns for fast ranked retrieval.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'phase4_fts_001'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Add tsvector columns ─────────────────────────────────────

    # actors.search_vector
    op.execute(sa.text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'actors' AND column_name = 'search_vector'
            ) THEN
                ALTER TABLE actors ADD COLUMN search_vector tsvector;
            END IF;
        END $$
    """))

    # signal_data.search_vector
    op.execute(sa.text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'signal_data' AND column_name = 'search_vector'
            ) THEN
                ALTER TABLE signal_data ADD COLUMN search_vector tsvector;
            END IF;
        END $$
    """))

    # discovered_hypotheses.search_vector
    op.execute(sa.text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'discovered_hypotheses' AND column_name = 'search_vector'
            ) THEN
                ALTER TABLE discovered_hypotheses ADD COLUMN search_vector tsvector;
            END IF;
        END $$
    """))

    # analytical_snapshots.search_vector
    op.execute(sa.text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'analytical_snapshots' AND column_name = 'search_vector'
            ) THEN
                ALTER TABLE analytical_snapshots ADD COLUMN search_vector tsvector;
            END IF;
        END $$
    """))

    # ── GIN indexes on tsvector columns ──────────────────────────

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_actors_search_vector
        ON actors USING GIN(search_vector)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_signal_data_search_vector
        ON signal_data USING GIN(search_vector)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_discovered_hypotheses_search_vector
        ON discovered_hypotheses USING GIN(search_vector)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_analytical_snapshots_search_vector
        ON analytical_snapshots USING GIN(search_vector)
    """))

    # ── Backfill existing rows ───────────────────────────────────

    op.execute(sa.text("""
        UPDATE actors
        SET search_vector = to_tsvector(
            'english',
            COALESCE(name, '') || ' ' || COALESCE(category, '') || ' ' || COALESCE(title, '')
        )
        WHERE search_vector IS NULL
    """))

    op.execute(sa.text("""
        UPDATE signal_data
        SET search_vector = to_tsvector(
            'english',
            COALESCE(description, '') || ' ' || COALESCE(ticker, '') || ' ' || COALESCE(actor, '')
        )
        WHERE search_vector IS NULL
    """))

    op.execute(sa.text("""
        UPDATE discovered_hypotheses
        SET search_vector = to_tsvector(
            'english',
            COALESCE(thesis, '') || ' ' || COALESCE(pattern_type, '')
        )
        WHERE search_vector IS NULL
    """))

    op.execute(sa.text("""
        UPDATE analytical_snapshots
        SET search_vector = to_tsvector(
            'english',
            COALESCE(title, '') || ' ' || COALESCE(summary, '')
        )
        WHERE search_vector IS NULL
    """))

    # ── Triggers to auto-update search_vector on INSERT/UPDATE ───

    # actors trigger
    op.execute(sa.text("""
        CREATE OR REPLACE FUNCTION actors_search_vector_update() RETURNS trigger AS $$
        BEGIN
            NEW.search_vector := to_tsvector(
                'english',
                COALESCE(NEW.name, '') || ' ' || COALESCE(NEW.category, '') || ' ' || COALESCE(NEW.title, '')
            );
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql
    """))

    op.execute(sa.text("""
        DROP TRIGGER IF EXISTS trg_actors_search_vector ON actors
    """))

    op.execute(sa.text("""
        CREATE TRIGGER trg_actors_search_vector
        BEFORE INSERT OR UPDATE ON actors
        FOR EACH ROW EXECUTE FUNCTION actors_search_vector_update()
    """))

    # signal_data trigger
    op.execute(sa.text("""
        CREATE OR REPLACE FUNCTION signal_data_search_vector_update() RETURNS trigger AS $$
        BEGIN
            NEW.search_vector := to_tsvector(
                'english',
                COALESCE(NEW.description, '') || ' ' || COALESCE(NEW.ticker, '') || ' ' || COALESCE(NEW.actor, '')
            );
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql
    """))

    op.execute(sa.text("""
        DROP TRIGGER IF EXISTS trg_signal_data_search_vector ON signal_data
    """))

    op.execute(sa.text("""
        CREATE TRIGGER trg_signal_data_search_vector
        BEFORE INSERT OR UPDATE ON signal_data
        FOR EACH ROW EXECUTE FUNCTION signal_data_search_vector_update()
    """))

    # discovered_hypotheses trigger
    op.execute(sa.text("""
        CREATE OR REPLACE FUNCTION discovered_hypotheses_search_vector_update() RETURNS trigger AS $$
        BEGIN
            NEW.search_vector := to_tsvector(
                'english',
                COALESCE(NEW.thesis, '') || ' ' || COALESCE(NEW.pattern_type, '')
            );
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql
    """))

    op.execute(sa.text("""
        DROP TRIGGER IF EXISTS trg_discovered_hypotheses_search_vector ON discovered_hypotheses
    """))

    op.execute(sa.text("""
        CREATE TRIGGER trg_discovered_hypotheses_search_vector
        BEFORE INSERT OR UPDATE ON discovered_hypotheses
        FOR EACH ROW EXECUTE FUNCTION discovered_hypotheses_search_vector_update()
    """))

    # analytical_snapshots trigger
    op.execute(sa.text("""
        CREATE OR REPLACE FUNCTION analytical_snapshots_search_vector_update() RETURNS trigger AS $$
        BEGIN
            NEW.search_vector := to_tsvector(
                'english',
                COALESCE(NEW.title, '') || ' ' || COALESCE(NEW.summary, '')
            );
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql
    """))

    op.execute(sa.text("""
        DROP TRIGGER IF EXISTS trg_analytical_snapshots_search_vector ON analytical_snapshots
    """))

    op.execute(sa.text("""
        CREATE TRIGGER trg_analytical_snapshots_search_vector
        BEFORE INSERT OR UPDATE ON analytical_snapshots
        FOR EACH ROW EXECUTE FUNCTION analytical_snapshots_search_vector_update()
    """))

    # ── Materialized view: intelligence_search ───────────────────

    op.execute(sa.text("""
        DROP MATERIALIZED VIEW IF EXISTS intelligence_search
    """))

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
               COALESCE(title, '') AS title,
               COALESCE(summary, '') AS body,
               to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(summary, '')) AS tsv
        FROM analytical_snapshots WHERE summary IS NOT NULL
    """))

    op.execute(sa.text("""
        CREATE INDEX idx_intelligence_search_tsv
        ON intelligence_search USING GIN(tsv)
    """))

    op.execute(sa.text("""
        CREATE UNIQUE INDEX idx_intelligence_search_pk
        ON intelligence_search(source_type, source_id)
    """))


def downgrade() -> None:
    op.execute(sa.text("DROP MATERIALIZED VIEW IF EXISTS intelligence_search"))

    op.execute(sa.text("DROP TRIGGER IF EXISTS trg_actors_search_vector ON actors"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS actors_search_vector_update()"))

    op.execute(sa.text("DROP TRIGGER IF EXISTS trg_signal_data_search_vector ON signal_data"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS signal_data_search_vector_update()"))

    op.execute(sa.text("DROP TRIGGER IF EXISTS trg_discovered_hypotheses_search_vector ON discovered_hypotheses"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS discovered_hypotheses_search_vector_update()"))

    op.execute(sa.text("DROP TRIGGER IF EXISTS trg_analytical_snapshots_search_vector ON analytical_snapshots"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS analytical_snapshots_search_vector_update()"))

    op.execute(sa.text("DROP INDEX IF EXISTS idx_actors_search_vector"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_signal_data_search_vector"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_discovered_hypotheses_search_vector"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_analytical_snapshots_search_vector"))

    op.execute(sa.text("ALTER TABLE actors DROP COLUMN IF EXISTS search_vector"))
    op.execute(sa.text("ALTER TABLE signal_data DROP COLUMN IF EXISTS search_vector"))
    op.execute(sa.text("ALTER TABLE discovered_hypotheses DROP COLUMN IF EXISTS search_vector"))
    op.execute(sa.text("ALTER TABLE analytical_snapshots DROP COLUMN IF EXISTS search_vector"))
