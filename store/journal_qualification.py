"""Narrow read guard for journals used as current regime/feedback inputs.

This excludes explicitly provisional signal records and directly identified
fundamental-divergence contract records. It does not certify that any remaining
journal has complete financial ancestry. Historical/audit reads must retain raw
rows; no records, outcomes or operator actions are changed by this predicate.

The SQL uses existing, unaliased decision_journal columns. The select builder
applies the fixed predicate before caller-provided ordering and limits. JSON
extraction is null-safe even for scalar/array/wrong-shape JSONB; missing evidence
does not invent lineage. A source_contract_id alone is not enough to classify a
record as a provisional signal. Structured financial markers use the event
guard's outer-whitespace and case normalization, preserving exact family
boundaries. Legacy provisional state/action/counterfactual markers are unchanged.
"""

from sqlalchemy import column, select, table, text
from sqlalchemy.sql import Select


# The complete Unicode whitespace repertoire stripped by Python str.strip().
# Bind it as data: PostgreSQL BTRIM's default removes only the ASCII space.
_JOURNAL_MARKER_WHITESPACE = (
    "\t\n\v\f\r\x1c\x1d\x1e\x1f \x85\xa0\u1680"
    "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
    "\u2028\u2029\u202f\u205f\u3000"
)

# These exact family names contain only ASCII letters with no non-ASCII
# casefold equivalents. C collation makes LOWER independent of DB locale and
# matches casefold for the marker prefix; a source's suffix is not classified.
NON_PROVISIONAL_JOURNAL_PREDICATE = """(
    NOT (
        LEFT(UPPER(COALESCE(inferred_state, '')), 12) = 'PROVISIONAL_'
        OR UPPER(COALESCE(action_taken, '')) = 'PROVISIONAL'
        OR LEFT(UPPER(COALESCE(action_taken, '')), 12) = 'PROVISIONAL '
        OR LEFT(LOWER(COALESCE(counterfactual, '')), 26) = 'provisional-from-contract:'
        OR LOWER(BTRIM(COALESCE(contradiction_flags ->> 'signal_type', ''), :journal_marker_whitespace) COLLATE "C") = 'fundamental_divergence'
        OR LOWER(BTRIM(COALESCE(contradiction_flags #>> '{context,signal_type}', ''), :journal_marker_whitespace) COLLATE "C") = 'fundamental_divergence'
        OR LOWER(BTRIM(COALESCE(contradiction_flags ->> 'producer_module', ''), :journal_marker_whitespace) COLLATE "C") = 'intelligence.fundamental_divergence'
        OR LOWER(BTRIM(COALESCE(contradiction_flags #>> '{context,producer_module}', ''), :journal_marker_whitespace) COLLATE "C") = 'intelligence.fundamental_divergence'
        OR LOWER(BTRIM(COALESCE(contradiction_flags ->> 'source', ''), :journal_marker_whitespace) COLLATE "C") = 'fundamental_divergence'
        OR LEFT(LOWER(BTRIM(COALESCE(contradiction_flags ->> 'source', ''), :journal_marker_whitespace) COLLATE "C"), 23) = 'fundamental_divergence:'
        OR LOWER(BTRIM(COALESCE(contradiction_flags #>> '{context,source}', ''), :journal_marker_whitespace) COLLATE "C") = 'fundamental_divergence'
        OR LEFT(LOWER(BTRIM(COALESCE(contradiction_flags #>> '{context,source}', ''), :journal_marker_whitespace) COLLATE "C"), 23) = 'fundamental_divergence:'
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
        text(NON_PROVISIONAL_JOURNAL_PREDICATE).bindparams(
            journal_marker_whitespace=_JOURNAL_MARKER_WHITESPACE
        )
    )
