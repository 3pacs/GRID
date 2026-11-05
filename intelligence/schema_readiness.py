"""Read-only schema contracts and explicitly invoked bounded schema transactions.

Ordinary callers use require_relation(s); those functions never create, alter,
index or repair anything. Preparation is an explicit operator/deployment action,
not a first-read/first-write fallback. No credentials, flags or target defaults.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import re
import time

from sqlalchemy import text


class SchemaNotReady(RuntimeError):
    """The caller must stop; preparation/reconciliation is a separate action."""


class SchemaPreparationTimeout(SchemaNotReady):
    """The bounded preparation transaction must roll back, without retry."""


@dataclass(frozen=True)
class ColumnRequirement:
    pg_type: str
    nullable: bool | None = None
    no_default: bool = False
    default_kind: str | None = None


_CATALOG = """
SELECT c.relkind, n.nspname, a.attname, t.typname, tn.nspname,
       a.attnotnull, a.atthasdef,
       EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
               WHERE i.inhrelid=c.oid OR i.inhparent=c.oid),
       CASE WHEN a.attidentity IN ('a','d') THEN 'generated_integer'
            WHEN pg_catalog.pg_get_serial_sequence(
                     pg_catalog.quote_ident(n.nspname)||'.'||pg_catalog.quote_ident(c.relname),a.attname) IS NOT NULL
                 AND pg_catalog.pg_get_expr(ad.adbin,ad.adrelid) = pg_catalog.format(
                     'nextval(%L::regclass)', pg_catalog.pg_get_serial_sequence(
                     pg_catalog.quote_ident(n.nspname)||'.'||pg_catalog.quote_ident(c.relname),a.attname)::pg_catalog.regclass::text)
                 THEN 'generated_integer'
            WHEN pg_catalog.pg_get_expr(ad.adbin,ad.adrelid) IN
                 ('now()', 'CURRENT_TIMESTAMP', 'transaction_timestamp()')
                 THEN 'transaction_timestamp'
            ELSE NULL END
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
     AND a.attnum>0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
LEFT JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
LEFT JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace
WHERE c.oid=pg_catalog.to_regclass(:relation)
"""


_CONSTRAINTS = """
SELECT co.contype,
       ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid=co.conrelid AND a.attnum=ANY(co.conkey)
             ORDER BY pg_catalog.array_position(co.conkey,a.attnum)),
       rn.nspname, rc.relname,
       ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid=co.confrelid AND a.attnum=ANY(co.confkey)
             ORDER BY pg_catalog.array_position(co.confkey,a.attnum)), co.convalidated
FROM pg_catalog.pg_constraint co
LEFT JOIN pg_catalog.pg_class rc ON rc.oid=co.confrelid
LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid=rc.relnamespace
WHERE co.conrelid=pg_catalog.to_regclass(:relation) AND co.contype IN ('p','f')
"""


def require_relation(connection, relation: str, columns: dict[str, ColumnRequirement],
                     *, expected_schema: str | None = None,
                     primary_key: tuple[str, ...] = (),
                     foreign_keys: tuple[tuple[str, str, str], ...] = ()) -> str:
    """Validate actual resolution on this connection, using catalogs only.

    Returns the resolved schema. Caller owns the connection/transaction. This
    is not a lock reservation or a guarantee about later concurrent DDL.
    """
    rows = connection.execute(text(_CATALOG), {"relation": relation}).all()
    if not rows:
        raise SchemaNotReady("required relation is absent; explicit preparation required")
    namespace = rows[0][1]
    if expected_schema is not None and namespace != expected_schema:
        raise SchemaNotReady("required relation resolved outside the declared schema")
    if any(r[0] != 'r' or r[1] != namespace or r[7] for r in rows):
        raise SchemaNotReady("required relation is not an ordinary non-inherited table")
    actual = {r[2]: r for r in rows}
    for name, requirement in columns.items():
        row = actual.get(name)
        if row is None or row[3:5] != (requirement.pg_type, 'pg_catalog'):
            raise SchemaNotReady("required column is missing or has an incompatible type")
        if requirement.nullable is not None and (not row[5]) != requirement.nullable:
            raise SchemaNotReady("required column has incompatible nullability")
        if requirement.no_default and row[6]:
            raise SchemaNotReady("required column must not have a default")
        if requirement.default_kind is not None and row[8] != requirement.default_kind:
            raise SchemaNotReady("required column lacks its functional generation/default contract")
    if primary_key or foreign_keys:
        constraints = connection.execute(text(_CONSTRAINTS), {"relation": relation}).all()
        if primary_key and not any(r[0] == 'p' and tuple(r[1]) == primary_key and r[5] for r in constraints):
            raise SchemaNotReady("required primary key is absent or incompatible")
        for local, target, remote in foreign_keys:
            if not any(r[0] == 'f' and tuple(r[1]) == (local,) and r[2] == namespace
                       and r[3] == target and tuple(r[4]) == (remote,) and r[5] for r in constraints):
                raise SchemaNotReady("required validated foreign key is absent or incompatible")
    return namespace


def require_relations(engine, contracts: dict[str, dict[str, ColumnRequirement]]) -> str:
    """Read-only readiness; all required relations must resolve in one schema."""
    with engine.connect() as connection:
        namespaces = {require_relation(connection, name, columns)
                      for name, columns in contracts.items()}
    if len(namespaces) != 1:
        raise SchemaNotReady("required relations resolve in different schemas")
    return namespaces.pop()


def quote_schema(schema: str) -> str:
    """Explicit preparation schema only; never default to public/search_path."""
    if not isinstance(schema, str) or not re.fullmatch(r'[a-z_][a-z0-9_]{0,62}', schema):
        raise ValueError("an explicit simple PostgreSQL schema identifier is required")
    if schema == 'information_schema' or schema.startswith('pg_'):
        raise ValueError("system schemas are not preparation targets")
    return '"' + schema + '"'


@contextmanager
def bounded_schema_transaction(engine):
    """Explicit preparation only: one transaction, no retry or implicit grant.

    Lock waits <=500ms, individual statements <=2s, idle transaction <=5s.
    Cooperative elapsed checks stop work after5s and roll back. They do not
    bound driver/network/commit stalls; the admitted executor must supply its
    external lifecycle bound and treat lost completion as uncertain.
    """
    started = time.monotonic()
    def check():
        if time.monotonic() - started >= 5:
            raise SchemaPreparationTimeout("schema preparation elapsed budget exceeded")
    with engine.begin() as connection:
        for statement in ("SET LOCAL lock_timeout = '500ms'",
                          "SET LOCAL statement_timeout = '2000ms'",
                          "SET LOCAL idle_in_transaction_session_timeout = '5000ms'",
                          "SET LOCAL search_path = pg_catalog"):
            check()
            connection.execute(text(statement))
        def execute(statement, parameters=None):
            check()
            result = connection.execute(text(statement), parameters or {})
            check()
            return result
        execute.check_budget = check
        check()  # Refuse an expired SET sequence before entering caller code.
        yield connection, execute
        check()  # Raised before the transaction context can commit.
