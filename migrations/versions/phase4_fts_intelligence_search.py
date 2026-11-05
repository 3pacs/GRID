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


# ── Fresh-database bootstrap of the two runtime-owned tables ─────────────
#
# Neither ``discovered_hypotheses`` nor ``analytical_snapshots`` is created by
# ``schema.sql`` or by any earlier revision: both come from runtime DDL
# (``intelligence/hypothesis_engine.py::_SCHEMA_SQL`` and
# ``store/snapshots.py::ANALYTICAL_SNAPSHOTS_DDL``). On a database that has
# been running GRID they already exist, so these are no-ops there. On a
# genuinely fresh database (schema.sql -> stamp 7e4dfecce247 -> upgrade head)
# the ALTERs below would otherwise fail with UndefinedTable (issue #523).
#
# These are frozen copies of the runtime DDL at the time of writing.
# ``tests/test_migration_runtime_ddl_parity.py`` checks that the columns the
# migration chain gives each table stay a superset of the runtime DDL's, so a
# runtime column added later fails CI until a migration adds it too.
_BOOTSTRAP_DISCOVERED_HYPOTHESES = """
    CREATE TABLE IF NOT EXISTS discovered_hypotheses (
        id               TEXT PRIMARY KEY,
        thesis           TEXT NOT NULL,
        pattern_type     TEXT,
        evidence         JSONB,
        test_criteria    JSONB,
        invalidation     TEXT,
        confidence       DOUBLE PRECISION DEFAULT 0.5,
        status           TEXT DEFAULT 'active',
        times_tested     INTEGER DEFAULT 0,
        times_correct    INTEGER DEFAULT 0,
        created_at       TIMESTAMPTZ DEFAULT NOW(),
        last_tested      TIMESTAMPTZ,
        role             TEXT DEFAULT 'thesis',
        pair_id          TEXT,
        kill_reason      TEXT,
        killed_at        TIMESTAMPTZ
    )
"""

_BOOTSTRAP_ANALYTICAL_SNAPSHOTS = """
    CREATE TABLE IF NOT EXISTS analytical_snapshots (
        id            BIGSERIAL PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        category      TEXT NOT NULL,
        subcategory   TEXT,
        as_of_date    DATE NOT NULL,
        payload       JSONB NOT NULL,
        actor_name    TEXT,
        metrics       JSONB,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
"""


def upgrade() -> None:
    # ── Bootstrap runtime-owned tables on a fresh database ───────
    op.execute(sa.text(_BOOTSTRAP_DISCOVERED_HYPOTHESES))
    op.execute(sa.text(_BOOTSTRAP_ANALYTICAL_SNAPSHOTS))

    # GRANT footer (migrations/_TEMPLATE.sql). Re-granting on a table that
    # already exists is a no-op; guarded on the role so a developer database
    # without a `grid` role still migrates.
    op.execute(sa.text("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT ALL ON discovered_hypotheses TO grid';
                EXECUTE 'GRANT ALL ON analytical_snapshots TO grid';
                IF to_regclass('analytical_snapshots_id_seq') IS NOT NULL THEN
                    EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE analytical_snapshots_id_seq TO grid';
                END IF;
            END IF;
        END $$
    """))

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

    # analytical_snapshots has no title/summary: its text columns are
    # category / subcategory, with the detail carried in jsonb payload and
    # metrics. Naming title here made the statement fail outright, and made
    # the trigger below reject every write to the table.
    op.execute(sa.text("""
        UPDATE analytical_snapshots
        SET search_vector = to_tsvector(
            'english',
            COALESCE(category, '') || ' ' || COALESCE(subcategory, '')
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
                COALESCE(NEW.category, '') || ' ' || COALESCE(NEW.subcategory, '')
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
               COALESCE(category, '') AS title,
               COALESCE(subcategory, '') AS body,
               to_tsvector('english', COALESCE(category, '') || ' ' || COALESCE(subcategory, '')) AS tsv
        FROM analytical_snapshots WHERE category IS NOT NULL
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
