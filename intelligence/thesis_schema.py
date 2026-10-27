"""Thesis schema contracts; preparation must be called explicitly by an admitted owner."""
from intelligence.schema_readiness import (
    ColumnRequirement as C, SchemaNotReady, bounded_schema_transaction,
    quote_schema, require_relation, require_relations,
)
from sqlalchemy import text

SNAPSHOT_COLUMNS = {
    "id": C("int4", False, default_kind="generated_integer"), "timestamp": C("timestamptz", False, default_kind="transaction_timestamp"),
    "overall_direction": C("text", False), "conviction": C("numeric", True),
    "conviction_unit": C("text", True, True), "key_drivers": C("jsonb"),
    "risk_factors": C("jsonb"), "model_states": C("jsonb"),
    "narrative": C("text"), "outcome": C("text"), "actual_market_move": C("numeric"),
    "scored_at": C("timestamptz"),
}
POSTMORTEM_COLUMNS = {
    "id": C("int4", False, default_kind="generated_integer"), "snapshot_id": C("int4"),
    "thesis_direction": C("text"), "actual_direction": C("text"),
    "models_right": C("jsonb"), "models_wrong": C("jsonb"),
    "what_we_missed": C("text"), "root_cause": C("text"), "lesson": C("text"),
    "generated_at": C("timestamptz", default_kind="transaction_timestamp"),
}
THESIS_CONTRACTS = {"thesis_snapshots": SNAPSHOT_COLUMNS,
                    "thesis_postmortems": POSTMORTEM_COLUMNS}


def require_thesis_connection(connection, *, expected_schema=None, allow_legacy_unit=False):
    namespace = expected_schema
    for name, columns in THESIS_CONTRACTS.items():
        if allow_legacy_unit and name == 'thesis_snapshots':
            columns = {k:v for k,v in columns.items() if k != 'conviction_unit'}
        resolved = require_relation(connection, name, columns, expected_schema=namespace,
            primary_key=('id',), foreign_keys=(("snapshot_id","thesis_snapshots","id"),)
            if name == 'thesis_postmortems' else ())
        namespace = resolved
    return namespace


def require_thesis_schema(engine):
    with engine.connect() as connection:
        return require_thesis_connection(connection)


_DDL = ('\n            CREATE TABLE IF NOT EXISTS {schema}.thesis_snapshots (\n                id SERIAL PRIMARY KEY,\n                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n                overall_direction TEXT NOT NULL,\n                conviction NUMERIC,\n                key_drivers JSONB,\n                risk_factors JSONB,\n                model_states JSONB,\n                narrative TEXT,\n                outcome TEXT,\n                actual_market_move NUMERIC,\n                scored_at TIMESTAMPTZ\n            )\n        ', 'ALTER TABLE {schema}.thesis_snapshots ADD COLUMN IF NOT EXISTS conviction_unit TEXT', '\n            CREATE INDEX IF NOT EXISTS idx_thesis_snapshots_ts\n                ON {schema}.thesis_snapshots (timestamp DESC)\n        ', '\n            CREATE INDEX IF NOT EXISTS idx_thesis_snapshots_unscored\n                ON {schema}.thesis_snapshots (timestamp)\n                WHERE outcome IS NULL\n        ', '\n            CREATE TABLE IF NOT EXISTS {schema}.thesis_postmortems (\n                id SERIAL PRIMARY KEY,\n                snapshot_id INT REFERENCES {schema}.thesis_snapshots(id),\n                thesis_direction TEXT,\n                actual_direction TEXT,\n                models_right JSONB,\n                models_wrong JSONB,\n                what_we_missed TEXT,\n                root_cause TEXT,\n                lesson TEXT,\n                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n            )\n        ', '\n            CREATE INDEX IF NOT EXISTS idx_thesis_pm_snapshot\n                ON {schema}.thesis_postmortems (snapshot_id)\n        ')


def _prepare_thesis_schema_in_transaction(connection, execute, schema: str):
    """Shared preparation body; caller owns the bounded transaction."""
    quoted = quote_schema(schema)
    def verify(relation, columns):
        execute.check_budget()
        name = relation.split('.')[-1]
        result = require_relation(connection, relation, columns, expected_schema=schema,
            primary_key=('id',), foreign_keys=(("snapshot_id","thesis_snapshots","id"),)
            if name == 'thesis_postmortems' else ())
        execute.check_budget()
        return result
    # Refuse incompatible existing tables before attempting additive DDL.
    for name, columns in THESIS_CONTRACTS.items():
        relation = quoted + '.' + name
        exists = execute("SELECT pg_catalog.to_regclass(:relation) IS NOT NULL",
                         {"relation": relation}).scalar_one()
        if exists:
            old_columns = {k:v for k,v in columns.items() if k != 'conviction_unit'}
            verify(relation, old_columns)
            if name == 'thesis_snapshots':
                unit_exists = execute("""SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute
                    WHERE attrelid=pg_catalog.to_regclass(:relation)
                    AND attname='conviction_unit' AND attnum>0 AND NOT attisdropped)""",
                    {"relation": relation}).scalar_one()
                if unit_exists:
                    verify(relation, columns)
    for statement in _DDL:
        execute(statement.format(schema=quoted))
    for name, columns in THESIS_CONTRACTS.items():
        verify(quoted+'.'+name, columns)

def prepare_thesis_schema(engine, schema: str):
    """Explicit component preparation; full release uses prepare_runtime_schema.

    No schema creation/grants/backfill; incompatible functional contracts refuse.
    """
    quote_schema(schema)  # Validate before obtaining a connection.
    with bounded_schema_transaction(engine) as (connection, execute):
        _prepare_thesis_schema_in_transaction(connection, execute, schema)
    return {"schema": schema, "state": "ready", "historical_rows_changed": False}
