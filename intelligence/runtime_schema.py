"""One explicit bounded transaction for thesis and Task11 publication schema.

No application/provider import, connection factory, runtime flag or lazy caller.
"""
from intelligence.schema_readiness import ColumnRequirement as C
from intelligence.schema_readiness import bounded_schema_transaction, quote_schema, require_relation
from intelligence.thesis_schema import _prepare_thesis_schema_in_transaction

# Exact original Task11 contract, not inferred from stored values or timestamps.
PUBLICATION_CONTRACT_SHA256 = '18d098a5a94c616a7e6c746011ce725110d5443d5d0bbdc76d700b4df665e7b6'
PUBLICATION_COLUMNS = {
    'id': C('int4', nullable=False, no_default=False, default_kind='generated_integer'),
    'briefing_type': C('text', nullable=False, no_default=False, default_kind=None),
    'briefing_date': C('date', nullable=False, no_default=False, default_kind=None),
    'content': C('text', nullable=False, no_default=False, default_kind=None),
    'sentiment_score': C('float4', nullable=True, no_default=False, default_kind=None),
    'sentiment_label': C('text', nullable=True, no_default=False, default_kind=None),
    'sentiment_data': C('jsonb', nullable=True, no_default=False, default_kind=None),
    'snapshot_data': C('jsonb', nullable=True, no_default=False, default_kind=None),
    'created_at': C('timestamptz', nullable=True, no_default=False, default_kind='transaction_timestamp'),
}
_PUBLICATION_DDL = (
    """CREATE TABLE IF NOT EXISTS {schema}.market_briefings (
        id SERIAL PRIMARY KEY, briefing_type TEXT NOT NULL,
        briefing_date DATE NOT NULL, content TEXT NOT NULL,
        sentiment_score REAL, sentiment_label TEXT,
        sentiment_data JSONB, snapshot_data JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW())""",
    "CREATE INDEX IF NOT EXISTS idx_market_briefings_date ON {schema}.market_briefings (briefing_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_market_briefings_type ON {schema}.market_briefings (briefing_type, briefing_date DESC)",
)


def prepare_runtime_schema(engine, *, thesis_schema: str, publication_schema: str):
    """Explicit preparation only, with both namespaces supplied by the operator.

    One transaction: either both functional contracts are ready or all this
    invocation's DDL rolls back. Never changes existing rows/defaults/keys,
    creates namespaces, grants privileges or invokes publication/thesis writers.
    """
    quote_schema(thesis_schema)
    quoted_publication = quote_schema(publication_schema)
    relation = quoted_publication + '.market_briefings'
    with bounded_schema_transaction(engine) as (connection, execute):
        def verify_publication():
            execute.check_budget()
            require_relation(connection, relation, PUBLICATION_COLUMNS,
                             expected_schema=publication_schema, primary_key=('id',))
            execute.check_budget()
        if execute('SELECT pg_catalog.to_regclass(:relation) IS NOT NULL',
                   {'relation': relation}).scalar_one():
            verify_publication()  # Wrong publication shape refuses before any DDL.
        _prepare_thesis_schema_in_transaction(connection, execute, thesis_schema)
        for statement in _PUBLICATION_DDL:
            execute(statement.format(schema=quoted_publication))
        verify_publication()
    return {'state':'ready', 'thesis_schema':thesis_schema,
            'publication_schema':publication_schema, 'historical_rows_changed':False}
