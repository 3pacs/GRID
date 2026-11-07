"""Sleuth lead origin and declared outcome on investigation_leads.

Revision ID: sleuth_intersections_20261107
Revises: hyp_registry_labels_20261107

``investigation_leads`` was created only at runtime by
``intelligence.sleuth.ensure_tables``; this revision creates it when missing
(same DDL) so the schema is reproducible, then adds:

* ``origin``: ``intersection`` (the scheduled path,
  ``intelligence.sleuth_intersections``), ``chat`` (a user question, never
  queued) or NULL (legacy rows before 2026-11-07).
* ``outcome`` / ``outcome_reason``: ``hypothesis_declared`` or ``dead_end``
  and why.
* ``declared_hypothesis_id``: the ``discovered_hypotheses`` row a lead
  declared.

Nullable, no default: no existing row is rewritten.
"""

from alembic import op

revision = "sleuth_intersections_20261107"
down_revision = "hyp_registry_labels_20261107"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS investigation_leads (
            id TEXT PRIMARY KEY,
            question TEXT NOT NULL,
            category TEXT NOT NULL,
            priority NUMERIC DEFAULT 0.5,
            evidence JSONB,
            status TEXT DEFAULT 'new',
            findings TEXT,
            follow_up_leads JSONB DEFAULT '[]',
            hypotheses JSONB DEFAULT '[]',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at TIMESTAMPTZ
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_leads_status ON investigation_leads (status, priority DESC)")
    op.execute("ALTER TABLE investigation_leads ADD COLUMN IF NOT EXISTS origin TEXT")
    op.execute("ALTER TABLE investigation_leads ADD COLUMN IF NOT EXISTS outcome TEXT")
    op.execute("ALTER TABLE investigation_leads ADD COLUMN IF NOT EXISTS outcome_reason TEXT")
    op.execute("ALTER TABLE investigation_leads ADD COLUMN IF NOT EXISTS declared_hypothesis_id TEXT")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_leads_origin_created
        ON investigation_leads (origin, created_at DESC)
    """)
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT SELECT, INSERT, UPDATE ON investigation_leads TO grid';
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP INDEX IF EXISTS idx_leads_origin_created")
    # The columns stay: declared outcomes may already be in them.
