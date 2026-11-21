"""Research Desk findings: insert-only, one row per record per catalogue version (analysis/research_findings.py)."""
from alembic import op

revision = "research_findings_20261121"
down_revision = "merge_av_wing_20261120"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS research_findings_loads (
            catalogue_sha256  TEXT PRIMARY KEY CHECK (catalogue_sha256 ~ '^[0-9a-f]{64}$'),
            record_count      INTEGER NOT NULL CHECK (record_count > 0),
            repo_commit       TEXT,
            source            TEXT NOT NULL,
            loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""")
    op.execute("""
        CREATE TABLE IF NOT EXISTS research_findings (
            finding_id        TEXT NOT NULL,
            catalogue_sha256  TEXT NOT NULL REFERENCES research_findings_loads (catalogue_sha256),
            family            TEXT NOT NULL,
            area              TEXT NOT NULL,
            kind              TEXT NOT NULL,
            verdict           TEXT NOT NULL,
            status            TEXT NOT NULL,
            question          TEXT NOT NULL,
            rule              TEXT NOT NULL,
            window_from       DATE,
            window_to         DATE,
            sample_n          DOUBLE PRECISION,
            sample_unit       TEXT,
            effect_value      DOUBLE PRECISION,
            effect_unit       TEXT,
            ci_low            DOUBLE PRECISION,
            ci_high           DOUBLE PRECISION,
            t_stat            DOUBLE PRECISION,
            data_spent        JSONB NOT NULL DEFAULT '[]'::jsonb,
            record            JSONB NOT NULL,
            source_path       TEXT NOT NULL,
            source_sha256     TEXT,
            recorded_at       DATE NOT NULL,
            loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (finding_id, catalogue_sha256)
        )""")
    op.execute("CREATE INDEX IF NOT EXISTS idx_research_findings_family ON research_findings (family)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_research_findings_area_verdict ON research_findings (area, verdict)")
    op.execute("""
        CREATE OR REPLACE FUNCTION research_findings_insert_only() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'research findings are insert-only'; END $$""")
    op.execute("DROP TRIGGER IF EXISTS research_findings_insert_only ON research_findings")
    op.execute("CREATE TRIGGER research_findings_insert_only BEFORE UPDATE OR DELETE ON research_findings "
               "FOR EACH ROW EXECUTE FUNCTION research_findings_insert_only()")
    op.execute("DROP TRIGGER IF EXISTS research_findings_loads_insert_only ON research_findings_loads")
    op.execute("CREATE TRIGGER research_findings_loads_insert_only BEFORE UPDATE OR DELETE ON research_findings_loads "
               "FOR EACH ROW EXECUTE FUNCTION research_findings_insert_only()")
    op.execute("""
        CREATE OR REPLACE VIEW research_findings_current AS
        SELECT f.* FROM research_findings f
        WHERE f.catalogue_sha256 = (SELECT catalogue_sha256 FROM research_findings_loads ORDER BY loaded_at DESC, catalogue_sha256 LIMIT 1)""")


def downgrade():
    raise RuntimeError("Refusing to drop the research findings record")
