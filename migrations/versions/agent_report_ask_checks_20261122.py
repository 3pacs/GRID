"""Asks to the owner, checked against later reports: one insert-only row per check (intelligence/report_reader.py)."""
from alembic import op

revision = "agent_report_ask_checks_20261122"
down_revision = "agent_report_reads_20261122"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS agent_report_ask_checks (
            id          BIGSERIAL PRIMARY KEY,
            ask_key     TEXT NOT NULL,
            report_id   UUID NOT NULL,
            ask         TEXT NOT NULL,
            settled     BOOLEAN NOT NULL,
            settled_by  UUID,
            how         TEXT NOT NULL DEFAULT '',
            later_seen  INTEGER NOT NULL CHECK (later_seen >= 0),
            checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CHECK (NOT settled OR settled_by IS NOT NULL OR how LIKE 'github:%'),
            CHECK (settled OR settled_by IS NULL)
        )""")
    op.execute("CREATE INDEX IF NOT EXISTS idx_agent_report_ask_checks_key ON agent_report_ask_checks (ask_key, checked_at DESC)")
    op.execute("DROP TRIGGER IF EXISTS agent_report_ask_checks_insert_only ON agent_report_ask_checks")
    op.execute("CREATE TRIGGER agent_report_ask_checks_insert_only BEFORE UPDATE OR DELETE ON agent_report_ask_checks "
               "FOR EACH ROW EXECUTE FUNCTION agent_report_reads_insert_only()")


def downgrade():
    raise RuntimeError("Refusing to drop the ask check record")
