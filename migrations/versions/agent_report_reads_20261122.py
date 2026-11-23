"""Agent report reads: one insert-only read row per agent report, and every failed attempt (intelligence/report_reader.py).

report_id is agent_reports.id. No foreign key: agent_reports comes from migrations/0050_agent_reports.sql, not alembic,
so the alembic replay has no such table; the reader joins on the id."""
from alembic import op

revision = "agent_report_reads_20261122"
down_revision = "merge_sec_main_20261121"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS agent_report_reads (
            report_id    UUID PRIMARY KEY,
            reader       TEXT NOT NULL,
            how          TEXT NOT NULL CHECK (how IN ('structured', 'routine', 'llm', 'unreadable')),
            summary      TEXT NOT NULL,
            kind         TEXT NOT NULL CHECK (kind IN ('result', 'change', 'incident', 'blocked', 'routine', 'other')),
            needs_anik   BOOLEAN NOT NULL DEFAULT FALSE,
            open_items   JSONB NOT NULL DEFAULT '[]'::jsonb,
            results      JSONB NOT NULL DEFAULT '[]'::jsonb,
            refs         JSONB NOT NULL DEFAULT '{}'::jsonb,
            read_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""")
    op.execute("CREATE INDEX IF NOT EXISTS idx_agent_report_reads_needs_anik ON agent_report_reads (read_at DESC) WHERE needs_anik")
    op.execute("""
        CREATE TABLE IF NOT EXISTS agent_report_read_attempts (
            id           BIGSERIAL PRIMARY KEY,
            report_id    UUID NOT NULL,
            error        TEXT NOT NULL,
            attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""")
    op.execute("CREATE INDEX IF NOT EXISTS idx_agent_report_read_attempts_report ON agent_report_read_attempts (report_id)")
    op.execute("""
        CREATE OR REPLACE FUNCTION agent_report_reads_insert_only() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'agent report reads are insert-only'; END $$""")
    op.execute("DROP TRIGGER IF EXISTS agent_report_reads_insert_only ON agent_report_reads")
    op.execute("CREATE TRIGGER agent_report_reads_insert_only BEFORE UPDATE OR DELETE ON agent_report_reads "
               "FOR EACH ROW EXECUTE FUNCTION agent_report_reads_insert_only()")
    op.execute("DROP TRIGGER IF EXISTS agent_report_read_attempts_insert_only ON agent_report_read_attempts")
    op.execute("CREATE TRIGGER agent_report_read_attempts_insert_only BEFORE UPDATE OR DELETE ON agent_report_read_attempts "
               "FOR EACH ROW EXECUTE FUNCTION agent_report_reads_insert_only()")


def downgrade():
    raise RuntimeError("Refusing to drop the agent report read record")
