"""Append-only labels for legacy hypothesis_registry verdicts; v2 verdict columns.

Revision ID: hyp_registry_labels_20261107
Revises: actor_universe_20261106

``hypothesis_registry`` PASSED/TESTING rows were never validated findings
(audit 2026-11-06, #1008). They stay exactly as written; this revision adds
``hypothesis_registry_labels``, one row per (registry row, label version),
written by ``scripts/label_legacy_hypotheses.py --apply``
(``governance.hypothesis_labels``). The table rejects UPDATE, DELETE and
TRUNCATE; a correction is a new label version, never an edit.

It also creates the v2 verdict columns on ``discovered_hypotheses``
(``scoring_version``, ``trial_p_value``, ``trial_q_value``,
``trial_ledger_sha256``; ``intelligence.hypothesis_trials``) so readers can
tell a ledgered v2 verdict from a v1 one (``scoring_version IS NULL``) before
the first v2 batch runs. Nullable, no default: no row is rewritten.

GRANT footer per ``migrations/_TEMPLATE.sql``: ``grid`` gets SELECT, INSERT.
"""

from alembic import op

revision = "hyp_registry_labels_20261107"
down_revision = "actor_universe_20261106"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS hypothesis_registry_labels (
            id             BIGSERIAL PRIMARY KEY,
            registry_id    INTEGER NOT NULL REFERENCES hypothesis_registry(id),
            label          TEXT NOT NULL
                           CHECK (label IN ('LEGACY_UNVALIDATED', 'LEGACY_LLM_FLAGGED',
                                            'LEGACY_INCONCLUSIVE')),
            prior_state    TEXT NOT NULL,
            reason         TEXT NOT NULL,
            label_version  TEXT NOT NULL,
            decided_by     TEXT NOT NULL CHECK (length(btrim(decided_by)) > 0),
            created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (registry_id, label_version)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_hypothesis_registry_labels_label
        ON hypothesis_registry_labels (label_version, label)
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION grid_reject_hypothesis_label_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION '% is append-only: % refused (add a new label version instead)',
                TG_TABLE_NAME, TG_OP USING ERRCODE = 'restrict_violation';
        END
        $$
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_hypothesis_registry_labels_append_only ON hypothesis_registry_labels")
    op.execute("""
        CREATE TRIGGER trg_hypothesis_registry_labels_append_only
        BEFORE UPDATE OR DELETE ON hypothesis_registry_labels
        FOR EACH ROW EXECUTE FUNCTION grid_reject_hypothesis_label_mutation()
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_hypothesis_registry_labels_no_truncate ON hypothesis_registry_labels")
    op.execute("""
        CREATE TRIGGER trg_hypothesis_registry_labels_no_truncate
        BEFORE TRUNCATE ON hypothesis_registry_labels
        FOR EACH STATEMENT EXECUTE FUNCTION grid_reject_hypothesis_label_mutation()
    """)

    op.execute("ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS scoring_version TEXT")
    op.execute("ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS trial_p_value DOUBLE PRECISION")
    op.execute("ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS trial_q_value DOUBLE PRECISION")
    op.execute("ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS trial_ledger_sha256 TEXT")

    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT SELECT, INSERT ON hypothesis_registry_labels TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE hypothesis_registry_labels_id_seq TO grid';
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS hypothesis_registry_labels")
    op.execute("DROP FUNCTION IF EXISTS grid_reject_hypothesis_label_mutation()")
    # The discovered_hypotheses columns stay: v2 verdicts may already be in them.
