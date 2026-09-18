"""Let the decision journal record an UNSCORED decision instead of omitting it.

Revision ID: journal_unscored_conf_0918
Revises: options_rec_scanner_score_0917
Create Date: 2026-09-18

Why this exists
---------------
``trading/contagion_to_ticket.py::write_ticket_to_journal`` used to write a
fabricated ``0.5`` ``state_confidence`` for tickets whose shock type has no
``contagion_backtest_results`` history at all (audit C-H4 / C-H6). PR #539
stopped that, but the only available alternative under a ``NOT NULL`` column
was to skip the journal row entirely — so the ticket left no trace in the
append-only audit log. Silence is not honesty: the operator cannot review a
decision that was never recorded.

This revision introduces the third state explicitly:

* ``state_confidence IS NULL``  — no confidence was ever measured.
* ``confidence_reason``         — why, in words, mandatory whenever the
                                  confidence is NULL.

``state_confidence`` keeps its ``BETWEEN 0 AND 1`` CHECK. A CHECK constraint
in PostgreSQL passes when it evaluates to NULL, so ``NULL BETWEEN 0 AND 1``
is accepted once ``NOT NULL`` is gone — no CHECK surgery is needed.

The append-only trigger ``enforce_journal_immutability`` compares with
``IS DISTINCT FROM``, which is NULL-safe, so an unscored row is protected the
same as any other. This revision extends the trigger to also refuse changes
to ``confidence_reason`` — the reason is part of the permanent record, not an
annotation.

Downgrade is DELIBERATELY ONE-WAY on nullability
------------------------------------------------
``confidence_reason`` is dropped, and with it the reason text for every
unscored row — that data cannot be reconstructed. ``NOT NULL`` is only
restored when the table holds no NULL ``state_confidence``; if unscored rows
exist, restoring it would require either deleting append-only rows or
inventing a number, and both are worse than leaving the column nullable. In
that case the column stays nullable and the migration logs why.
"""

import logging
from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
# NOTE: alembic_version.version_num is VARCHAR(32) on this database, so the
# id is abbreviated (see revision options_rec_scanner_score_0917's sibling
# fix 5670470a). len("journal_unscored_conf_0918") == 26.
revision: str = "journal_unscored_conf_0918"
down_revision: str | Sequence[str] | None = "options_rec_scanner_score_0917"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

CK_UNSCORED = "ck_decision_journal_unscored_has_reason"


_TRIGGER_FN_WITH_REASON = """
CREATE OR REPLACE FUNCTION enforce_journal_immutability()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.decision_timestamp IS DISTINCT FROM NEW.decision_timestamp THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify decision_timestamp';
    END IF;
    IF OLD.model_version_id IS DISTINCT FROM NEW.model_version_id THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify model_version_id';
    END IF;
    IF OLD.inferred_state IS DISTINCT FROM NEW.inferred_state THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify inferred_state';
    END IF;
    IF OLD.state_confidence IS DISTINCT FROM NEW.state_confidence THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify state_confidence';
    END IF;
    IF OLD.confidence_reason IS DISTINCT FROM NEW.confidence_reason THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify confidence_reason';
    END IF;
    IF OLD.transition_probability IS DISTINCT FROM NEW.transition_probability THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify transition_probability';
    END IF;
    IF OLD.contradiction_flags IS DISTINCT FROM NEW.contradiction_flags THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify contradiction_flags';
    END IF;
    IF OLD.grid_recommendation IS DISTINCT FROM NEW.grid_recommendation THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify grid_recommendation';
    END IF;
    IF OLD.baseline_recommendation IS DISTINCT FROM NEW.baseline_recommendation THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify baseline_recommendation';
    END IF;
    IF OLD.action_taken IS DISTINCT FROM NEW.action_taken THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify action_taken';
    END IF;
    IF OLD.counterfactual IS DISTINCT FROM NEW.counterfactual THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify counterfactual';
    END IF;
    IF OLD.operator_confidence IS DISTINCT FROM NEW.operator_confidence THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify operator_confidence';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

# The pre-#539 body, restored on downgrade once confidence_reason is gone
# (a trigger referencing a dropped column raises at UPDATE time).
_TRIGGER_FN_WITHOUT_REASON = _TRIGGER_FN_WITH_REASON.replace(
    """    IF OLD.confidence_reason IS DISTINCT FROM NEW.confidence_reason THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify confidence_reason';
    END IF;
""",
    "",
)


def upgrade() -> None:
    op.execute(
        "ALTER TABLE decision_journal "
        "ADD COLUMN IF NOT EXISTS confidence_reason TEXT"
    )
    op.execute(
        "ALTER TABLE decision_journal ALTER COLUMN state_confidence DROP NOT NULL"
    )

    # Re-upgrading after a downgrade: the downgrade drops confidence_reason
    # (destroying the text) but deliberately leaves the NULL confidences in
    # place, because deleting append-only rows is worse. Those rows come back
    # here as NULL/NULL and would fail the CHECK below, so the migration would
    # refuse to re-apply. Verified against PostgreSQL 14 on 2026-09-18: without
    # this backfill, `upgrade head` after `downgrade` raises
    #   CheckViolation: ck_decision_journal_unscored_has_reason is violated
    #
    # The backfill states a fact about the record's provenance. It does NOT
    # invent a confidence: state_confidence stays NULL. It touches only rows
    # that are already unscored AND already have no reason, and it runs before
    # the trigger below starts guarding confidence_reason.
    op.execute(
        "UPDATE decision_journal "
        "SET confidence_reason = 'unscored: reason text unavailable - it was "
        "dropped by a downgrade of journal_unscored_conf_0918 and cannot be "
        "reconstructed. The confidence itself was never measured.' "
        "WHERE state_confidence IS NULL AND confidence_reason IS NULL"
    )

    # An unscored row must say why it is unscored. Without this, dropping
    # NOT NULL would simply re-open the door to a silent, unexplained NULL.
    op.execute(
        f"ALTER TABLE decision_journal DROP CONSTRAINT IF EXISTS {CK_UNSCORED}"
    )
    op.execute(
        f"ALTER TABLE decision_journal ADD CONSTRAINT {CK_UNSCORED} "
        "CHECK (state_confidence IS NOT NULL OR confidence_reason IS NOT NULL)"
    )
    op.execute(
        "COMMENT ON COLUMN decision_journal.state_confidence IS "
        "'Measured confidence in the inferred state, 0-1. NULL means UNSCORED: "
        "no confidence was ever measured for this decision. NULL is never a "
        "substitute for a number and must never be read as 0 — an unscored row "
        "is excluded from calibration and scoring, not counted as a zero.'"
    )
    op.execute(
        "COMMENT ON COLUMN decision_journal.confidence_reason IS "
        "'Why state_confidence is what it is. REQUIRED when state_confidence "
        "IS NULL (ck_decision_journal_unscored_has_reason), e.g. "
        "''unscored: no contagion backtest history (basis=no_backtest_history, n=0)''. "
        "Immutable, like every other pre-outcome column.'"
    )
    op.execute(_TRIGGER_FN_WITH_REASON)
    # CREATE OR REPLACE FUNCTION alone protects nothing if the trigger was
    # never attached (a database built by alembic rather than by schema.sql).
    # Re-attaching is idempotent and matches schema.sql exactly.
    op.execute(
        "DROP TRIGGER IF EXISTS trg_journal_immutability ON decision_journal"
    )
    op.execute(
        "CREATE TRIGGER trg_journal_immutability "
        "BEFORE UPDATE ON decision_journal "
        "FOR EACH ROW EXECUTE FUNCTION enforce_journal_immutability()"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_decision_journal_unscored "
        "ON decision_journal (decision_timestamp DESC) "
        "WHERE state_confidence IS NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_decision_journal_unscored")
    op.execute(
        f"ALTER TABLE decision_journal DROP CONSTRAINT IF EXISTS {CK_UNSCORED}"
    )

    # Restore the trigger body FIRST: it must stop referencing
    # confidence_reason before that column disappears.
    op.execute(_TRIGGER_FN_WITHOUT_REASON)
    op.execute(
        "ALTER TABLE decision_journal DROP COLUMN IF EXISTS confidence_reason"
    )

    # Guarded, one-way-by-design: only re-impose NOT NULL when nothing would
    # have to be deleted or invented to satisfy it.
    conn = op.get_bind()
    unscored = conn.exec_driver_sql(
        "SELECT count(*) FROM decision_journal WHERE state_confidence IS NULL"
    ).scalar()
    if unscored:
        log.warning(
            "decision_journal.state_confidence left NULLABLE on downgrade: "
            "%s unscored row(s) exist. Restoring NOT NULL would require "
            "deleting append-only rows or inventing a confidence. The "
            "reason text for those rows was dropped with confidence_reason "
            "and cannot be recovered.",
            unscored,
        )
    else:
        op.execute(
            "ALTER TABLE decision_journal ALTER COLUMN state_confidence SET NOT NULL"
        )
