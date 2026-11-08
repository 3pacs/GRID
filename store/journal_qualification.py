"""Narrow read guard for journals used as current regime/feedback inputs.

This excludes explicitly provisional signal records and directly identified
fundamental-divergence contract records. It does not certify that any remaining
journal has complete financial ancestry. Historical/audit reads must retain raw
rows; no records, outcomes or operator actions are changed by this predicate.

The SQL uses existing, unaliased decision_journal columns. The select builder
applies the fixed predicate before caller-provided ordering and limits. JSON
extraction is null-safe even for scalar/array/wrong-shape JSONB; missing evidence does not invent lineage. A source_contract_id alone is
not enough to classify a record as a provisional signal.
"""

from sqlalchemy import column, select, table, text
from sqlalchemy.sql import Select


NON_PROVISIONAL_JOURNAL_PREDICATE = """(
    NOT (
        LEFT(UPPER(COALESCE(inferred_state, '')), 12) = 'PROVISIONAL_'
        OR UPPER(COALESCE(action_taken, '')) = 'PROVISIONAL'
        OR LEFT(UPPER(COALESCE(action_taken, '')), 12) = 'PROVISIONAL '
        OR LEFT(LOWER(COALESCE(counterfactual, '')), 26) = 'provisional-from-contract:'
        OR COALESCE(contradiction_flags ->> 'signal_type', '') = 'fundamental_divergence'
        OR COALESCE(contradiction_flags #>> '{context,signal_type}', '') = 'fundamental_divergence'
        OR COALESCE(contradiction_flags ->> 'producer_module', '') = 'intelligence.fundamental_divergence'
        OR COALESCE(contradiction_flags #>> '{context,producer_module}', '') = 'intelligence.fundamental_divergence'
        OR COALESCE(contradiction_flags ->> 'source', '') = 'fundamental_divergence'
        OR LEFT(COALESCE(contradiction_flags ->> 'source', ''), 23) = 'fundamental_divergence:'
        OR COALESCE(contradiction_flags #>> '{context,source}', '') = 'fundamental_divergence'
        OR LEFT(COALESCE(contradiction_flags #>> '{context,source}', ''), 23) = 'fundamental_divergence:'
    )
)"""


# A fixed read-only SQL expression, not reflected metadata or a schema change.
# Only these known columns may be selected or used in caller-side clauses.
DECISION_JOURNAL = table(
    "decision_journal",
    column("id"),
    column("decision_timestamp"),
    column("model_version_id"),
    column("inferred_state"),
    column("state_confidence"),
    column("transition_probability"),
    column("contradiction_flags"),
    column("grid_recommendation"),
    column("baseline_recommendation"),
    column("counterfactual"),
    column("operator_confidence"),
    column("verdict"),
    column("action_taken"),
)


def non_provisional_journal_select(*column_names: str) -> Select:
    """Select fixed journal columns with the narrow non-provisional guard.

    Column names are looked up in a fixed collection, never interpolated into
    SQL. Callers compose filters, ordering, DISTINCT ON and limits as SQLAlchemy
    clauses and bind all values. This is not a full financial-lineage guarantee.
    """
    if not column_names:
        raise ValueError("Select at least one fixed journal column")
    return select(*(DECISION_JOURNAL.c[name] for name in column_names)).where(
        text(NON_PROVISIONAL_JOURNAL_PREDICATE)
    )
