"""Narrow read guard for journals used as current regime/feedback inputs.

This excludes explicitly provisional signal records and directly identified
fundamental-divergence contract records. It does not certify that any remaining
journal has complete financial ancestry. Historical/audit reads must retain raw
rows; no records, outcomes or operator actions are changed by this predicate.

The SQL uses existing, unaliased decision_journal columns. Add it to WHERE before
ORDER BY/LIMIT. JSON extraction is null-safe even for scalar/array/wrong-shape
JSONB; missing evidence does not invent lineage. A source_contract_id alone is
not enough to classify a record as a provisional signal.
"""

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
