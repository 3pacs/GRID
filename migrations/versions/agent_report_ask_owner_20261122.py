"""Asks closed by the owner: agent_report_ask_checks.source gains 'owner' (intelligence/report_reader.py close).

The check constraints are re-created by name. An owner closure, like a GitHub one, is settled with no settling
report; a settled model verdict must still name the report that settled it.
"""
from alembic import op

revision = "agent_report_ask_owner_20261122"
down_revision = "agent_report_ask_checks_20261122"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        DO $$
        DECLARE c record;
        BEGIN
            FOR c IN SELECT conname FROM pg_constraint
                     WHERE conrelid = 'agent_report_ask_checks'::regclass AND contype = 'c' LOOP
                EXECUTE format('ALTER TABLE agent_report_ask_checks DROP CONSTRAINT %I', c.conname);
            END LOOP;
        END $$""")
    op.execute("ALTER TABLE agent_report_ask_checks ADD CONSTRAINT ask_checks_source "
               "CHECK (source IN ('github', 'model', 'owner'))")
    op.execute("ALTER TABLE agent_report_ask_checks ADD CONSTRAINT ask_checks_later_seen CHECK (later_seen >= 0)")
    op.execute("ALTER TABLE agent_report_ask_checks ADD CONSTRAINT ask_checks_model_names_report "
               "CHECK (source <> 'model' OR NOT settled OR settled_by IS NOT NULL)")
    op.execute("ALTER TABLE agent_report_ask_checks ADD CONSTRAINT ask_checks_github_owner_settle "
               "CHECK (source = 'model' OR (settled AND settled_by IS NULL))")
    op.execute("ALTER TABLE agent_report_ask_checks ADD CONSTRAINT ask_checks_open_names_nothing "
               "CHECK (settled OR settled_by IS NULL)")


def downgrade():
    raise RuntimeError("Refusing to narrow the ask check record")
