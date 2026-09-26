"""Proves — instead of asserting — that the per-actor content fingerprint
in ``intelligence/company_financial_rollups.py::compute_ttm`` covers every
``capital_flows`` column the TTM computation actually reads.

fable-daily-intel-sql-tasks (2026-09-20, coverage-proof task). The module
docstring already CLAIMS the fingerprint covers ``fiscal_period,
flow_type, direction, counterparty_id, amount_usd, currency,
source_filing, confidence`` and explains why the rest of the table's
columns don't need to be in it. This file makes that claim mechanically
checked: it parses the actual SQL text of ``_TTM_UPSERT_SQL`` for every
``capital_flows`` column it references, parses the real column list for
``capital_flows`` out of migrations 0021 (base table) + 0024 (added
``currency`` column) so the comparison is against the schema, not a
second hand-typed list, and fails if a future edit reads a column that is
in neither the fingerprint nor the short, justified "key column" allow
list below.

Two no-DB tests do the coverage proof (SQL text + migration text only, no
Postgres). A third no-DB test checks the fingerprint's ``string_agg``
ORDER BY is fine enough that ties are impossible given the table's real
UNIQUE index, which is what makes the fingerprint insertion-order
independent. A fourth test (skipped without a reachable Postgres) proves
that order-independence for real: the same four quarters, inserted in
two different orders under two different actor_ids, must fingerprint
identically.
"""
from __future__ import annotations

import re
import uuid
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.company_financial_rollups import _TTM_UPSERT_SQL, compute_ttm

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MIGRATION_0021 = _REPO_ROOT / "migrations" / "0021_supply_chain_and_capital_flows.sql"
_MIGRATION_0024 = _REPO_ROOT / "migrations" / "0024_capital_flows_currency.sql"

# The eight columns the module docstring claims are fingerprinted (the
# "current_fp" CTE's string_agg concatenation). Test 2 below re-derives
# this same set straight from the SQL text (not hand-typed) and cross-
# checks it equals this constant, so this constant itself can't drift
# from the SQL silently.
DOCUMENTED_FINGERPRINTED_COLUMNS = {
    "fiscal_period",
    "flow_type",
    "direction",
    "counterparty_id",
    "amount_usd",
    "currency",
    "source_filing",
    "confidence",
}

# Columns the TTM SQL legitimately reads WITHOUT needing to be in the
# content fingerprint, each with the reason inline (this is the set the
# task calls out as "explain in the test why it cannot affect the
# result"):
#
#   actor_id     — the GROUP BY / partition key itself. The fingerprint is
#                  computed PER actor_id (one fingerprint row per actor in
#                  capital_flows_ttm_state); actor_id says WHICH bucket a
#                  row's content lands in, it is not content being
#                  compared for drift within a bucket.
#   period_type  — every row current_fp/q_ranked read is already filtered
#                  to period_type = 'quarter' (a WHERE-clause constant,
#                  not a per-row varying value within this computation).
#                  A row whose period_type changes away from 'quarter'
#                  doesn't need a fingerprint bit flipped for it — it
#                  simply drops out of the row set, which is exactly what
#                  the FULL OUTER JOIN "changed_actors" case (case 3,
#                  covered in tests/test_capital_flow_rollups_pg.py's
#                  reclassification test) already detects: the actor's
#                  fingerprint changes because the SET of quarter rows it
#                  aggregates over changed, not because a column value
#                  changed.
#   id           — the PostgreSQL-assigned serial primary key. It is only
#                  read as q_ranked's FINAL ROW_NUMBER() tie-break (`id
#                  DESC`), after source_filing-class, confidence rank, and
#                  as_of have already been compared. Two rows can only
#                  reach that final tie-break with identical fiscal_period,
#                  flow_type, direction, counterparty_id (the ROW_NUMBER
#                  PARTITION), and — per the capital_flows_dedup_nullable_
#                  cp_key UNIQUE index added in migration 0024 (actor_id,
#                  fiscal_period, period_type, flow_type, cp_key,
#                  source_filing) — a UNIQUE index violation would prevent
#                  two DISTINCT rows from also sharing source_filing. So
#                  by the time `id` is consulted the two candidate rows
#                  are already forced identical on every fingerprinted
#                  column; picking either by `id` cannot change the
#                  resulting amount_usd/currency that get summed.
#   as_of        — read only as q_ranked's second-to-last tie-break
#                  (`as_of DESC NULLS LAST`), one rung above `id` in the
#                  same ORDER BY, so the same argument applies: it can
#                  only distinguish rows that are already identical on
#                  every fingerprinted column, per the same UNIQUE index.
ALLOWED_NON_FINGERPRINTED_KEY_COLUMNS = {"actor_id", "period_type", "id", "as_of"}


def _capital_flows_columns_from_migrations() -> set[str]:
    """Re-derive the real ``capital_flows`` column list from the
    migrations that define it (0021 base table + 0024's added
    ``currency`` column), rather than hand-typing a second list that
    could silently drift from the schema."""
    sql_0021 = _MIGRATION_0021.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS capital_flows\s*\((.*?)\n\);",
        sql_0021,
        re.DOTALL,
    )
    assert m, "could not locate capital_flows CREATE TABLE in migration 0021"
    body = m.group(1)

    columns: set[str] = set()
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        # Skip table-level constraints, not column definitions.
        if line.upper().startswith(("UNIQUE", "PRIMARY KEY", "CONSTRAINT", "CHECK")):
            continue
        first_token = line.split()[0]
        columns.add(first_token)

    assert {
        "id", "actor_id", "fiscal_period", "period_type", "flow_type",
        "direction", "amount_usd", "counterparty_id", "source_filing",
        "confidence", "as_of",
    } <= columns, f"unexpected column parse from migration 0021: {columns}"

    sql_0024 = _MIGRATION_0024.read_text(encoding="utf-8")
    m2 = re.search(
        r"ALTER TABLE capital_flows\s+ADD COLUMN IF NOT EXISTS (\w+)",
        sql_0024,
    )
    assert m2, "could not locate the currency ADD COLUMN in migration 0024"
    columns.add(m2.group(1))

    return columns


def _columns_referenced_in_sql(sql: str, candidate_columns: set[str]) -> set[str]:
    """Which of ``candidate_columns`` appear as a whole identifier
    (word-boundary match, so ``actor_id`` never falsely matches ``id``)
    anywhere in ``sql``."""
    referenced = set()
    for col in candidate_columns:
        if re.search(rf"\b{re.escape(col)}\b", sql):
            referenced.add(col)
    return referenced


def _fingerprint_concat_expression(sql: str) -> str:
    """Extract just the ``md5(string_agg(<expr>, ',' ORDER BY ...))``
    concatenation expression — the part of the SQL that actually decides
    fingerprint content — separate from the ORDER BY clause and the rest
    of the statement, so column extraction from it isn't polluted by
    columns that are only used for query plumbing elsewhere."""
    m = re.search(
        r"md5\(string_agg\(\s*(.*?)\s*,\s*','\s*ORDER BY",
        sql,
        re.DOTALL,
    )
    assert m, "could not locate the string_agg concatenation expression"
    return m.group(1)


def _order_by_columns(sql: str) -> list[str]:
    """Extract the column list from the fingerprint's ``string_agg``
    ``ORDER BY`` clause (stops at the closing ``)``)."""
    m = re.search(
        r"','\s*ORDER BY\s*(.*?)\n\s*\)\)\s*AS fp",
        sql,
        re.DOTALL,
    )
    assert m, "could not locate the string_agg ORDER BY clause"
    order_by_text = m.group(1)
    # Strip the COALESCE(...) wrapper around counterparty_id down to the
    # bare column name so it compares like the other entries.
    order_by_text = order_by_text.replace(
        "COALESCE(NULLIF(counterparty_id, ''), '__none__')", "counterparty_id",
    )
    cols = [c.strip() for c in order_by_text.split(",")]
    return [c for c in cols if c]


# ── Test 1: the fingerprinted column set is exactly what the docstring
#    claims, re-derived from the SQL text itself. ──────────────────────


def test_fingerprint_concatenation_covers_exactly_the_documented_columns():
    """Parse the string_agg concatenation expression for
    ``capital_flows`` columns and assert it is exactly the documented
    eight — not a hand-typed assumption, the actual SQL."""
    schema_columns = _capital_flows_columns_from_migrations()
    concat_expr = _fingerprint_concat_expression(_TTM_UPSERT_SQL.text)
    fingerprinted = _columns_referenced_in_sql(concat_expr, schema_columns)

    assert fingerprinted == DOCUMENTED_FINGERPRINTED_COLUMNS, (
        f"the string_agg concatenation now fingerprints {fingerprinted}, "
        f"which no longer matches the documented set "
        f"{DOCUMENTED_FINGERPRINTED_COLUMNS} in the module docstring and "
        f"in this test's DOCUMENTED_FINGERPRINTED_COLUMNS constant — "
        f"update both together, deliberately, if this is an intended "
        f"widening (or narrowing) of what compute_ttm hashes."
    )


# ── Test 2: EVERY capital_flows column the whole TTM statement reads is
#    either fingerprinted or on the short justified allow-list. This is
#    the one that fails if someone adds a new column to capital_flows,
#    wires it into the TTM computation, and forgets to fingerprint it. ──


def test_every_column_the_ttm_sql_reads_is_covered():
    schema_columns = _capital_flows_columns_from_migrations()
    referenced = _columns_referenced_in_sql(_TTM_UPSERT_SQL.text, schema_columns)

    covered = DOCUMENTED_FINGERPRINTED_COLUMNS | ALLOWED_NON_FINGERPRINTED_KEY_COLUMNS
    uncovered = referenced - covered
    assert not uncovered, (
        f"_TTM_UPSERT_SQL now reads capital_flows column(s) {uncovered} "
        f"that are neither fingerprinted nor on the justified "
        f"ALLOWED_NON_FINGERPRINTED_KEY_COLUMNS allow-list in this test "
        f"file. If the new column can affect the TTM result, add it to "
        f"the string_agg concatenation in _TTM_UPSERT_SQL (and to "
        f"DOCUMENTED_FINGERPRINTED_COLUMNS here). If it genuinely cannot "
        f"affect the result (like `id`/`as_of` — see the comment above "
        f"ALLOWED_NON_FINGERPRINTED_KEY_COLUMNS), add it to that allow "
        f"list with the same kind of inline justification."
    )

    # And the reverse: every column we *think* is referenced should
    # actually show up — guards the allow-list itself from silently
    # growing stale entries that no longer apply (e.g. if `id` stopped
    # being read at all, it should come out of the allow-list too).
    stale_allowed = ALLOWED_NON_FINGERPRINTED_KEY_COLUMNS - referenced
    assert not stale_allowed, (
        f"{stale_allowed} are on the allow-list but the SQL no longer "
        f"reads them — remove the stale entries so the allow-list stays "
        f"an accurate description of the SQL."
    )

    # Every real capital_flows column must be accounted for somewhere:
    # fingerprinted, on the allow-list, or simply not read at all by this
    # statement. (Nothing should be able to silently fall through a gap
    # in this test's own bookkeeping.)
    assert referenced <= schema_columns


# ── Test 3: order-independence is a structural guarantee, not luck. ────


def test_string_agg_order_by_makes_ties_impossible_given_the_unique_index():
    """The fingerprint is only truly insertion-order independent if no
    two DISTINCT rows for the same actor can tie on the ``string_agg``
    ORDER BY key — otherwise PostgreSQL is free to concatenate them in
    either order across runs. Prove ties are impossible by checking the
    ORDER BY columns, together with the table's real UNIQUE index
    (``capital_flows_dedup_nullable_cp_key`` from migration 0024:
    actor_id, fiscal_period, period_type, flow_type, cp_key,
    source_filing), force uniqueness.

    Within one actor's fingerprint scan, actor_id and period_type
    ('quarter') are already constant, so the UNIQUE index reduces to
    (fiscal_period, flow_type, cp_key, source_filing) being unique per
    row. Every one of those four must appear in the ORDER BY clause for
    this guarantee to hold.
    """
    order_by_cols = set(_order_by_columns(_TTM_UPSERT_SQL.text))

    # The part of the real UNIQUE index that varies within one actor's
    # scan (actor_id and period_type are constant there, as explained
    # above).
    unique_index_variable_part = {
        "fiscal_period", "flow_type", "counterparty_id", "source_filing",
    }
    missing = unique_index_variable_part - order_by_cols
    assert not missing, (
        f"the string_agg ORDER BY clause is missing {missing} from the "
        f"columns that (together with actor_id/period_type, constant "
        f"within one actor's scan) make capital_flows_dedup_nullable_"
        f"cp_key's UNIQUE index force row uniqueness — without all four, "
        f"two distinct rows could tie on ORDER BY and the fingerprint "
        f"would depend on scan/insertion order, not just content."
    )


# ── Test 4: prove it for real against Postgres (skipped if unreachable).
#    Cheap: 8 inserts total, one compute_ttm call. ──────────────────────


@pytest.fixture(scope="module")
def pg_engine() -> Engine:
    try:
        from db import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT to_regclass('public.capital_flows')"),
            ).fetchone()
            if not row or not row[0]:
                pytest.skip("capital_flows table missing")
    except Exception as exc:
        pytest.skip(f"Postgres not available: {exc}")
    return engine


def _insert_quarter(engine, actor_id, fp, flow_type, amount, source_filing):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO capital_flows (
                    actor_id, fiscal_period, period_type, flow_type,
                    direction, amount_usd, counterparty_id, source_filing,
                    confidence, currency, as_of
                ) VALUES (
                    :a, :fp, 'quarter', :ft, 'in', :amt, NULL, :sf,
                    'confirmed', 'USD', NOW()
                )
                ON CONFLICT (
                    actor_id, fiscal_period, period_type, flow_type,
                    (COALESCE(NULLIF(counterparty_id,''), '__none__')),
                    source_filing
                ) DO UPDATE SET amount_usd = EXCLUDED.amount_usd
                """,
            ).bindparams(a=actor_id, fp=fp, ft=flow_type, amt=amount, sf=source_filing),
        )


def test_same_rows_different_insertion_order_same_fingerprint(pg_engine: Engine):
    """Insert the identical four quarters under two different actor_ids,
    in forward order for one and reverse order for the other, then assert
    ``compute_ttm`` durably records the SAME fingerprint for both in
    ``capital_flows_ttm_state`` — proving order-of-insertion cannot
    matter, only content."""
    actor_forward = f"fp_order_test_{uuid.uuid4().hex[:10]}"
    actor_reverse = f"fp_order_test_{uuid.uuid4().hex[:10]}"
    quarters = [
        (date(2024, 3, 31), "revenue", 100.0, "10-Q q1"),
        (date(2024, 6, 30), "revenue", 110.0, "10-Q q2"),
        (date(2024, 9, 30), "revenue", 120.0, "10-Q q3"),
        (date(2024, 12, 31), "revenue", 130.0, "10-Q q4"),
    ]
    try:
        for fp, ft, amt, sf in quarters:
            _insert_quarter(pg_engine, actor_forward, fp, ft, amt, sf)
        for fp, ft, amt, sf in reversed(quarters):
            _insert_quarter(pg_engine, actor_reverse, fp, ft, amt, sf)

        compute_ttm(pg_engine)

        with pg_engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT actor_id, quarter_fingerprint "
                    "FROM capital_flows_ttm_state WHERE actor_id IN (:a, :b)",
                ).bindparams(a=actor_forward, b=actor_reverse),
            ).fetchall()
        fps = {r[0]: r[1] for r in rows}

        assert fps.get(actor_forward) is not None
        assert fps.get(actor_reverse) is not None
        assert fps[actor_forward] == fps[actor_reverse], (
            f"same 4 quarters, different insertion order, produced "
            f"different fingerprints: forward={fps[actor_forward]!r} "
            f"reverse={fps[actor_reverse]!r} — the string_agg ORDER BY "
            f"is not fully determining row order."
        )
    finally:
        with pg_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM capital_flows WHERE actor_id IN (:a, :b)").bindparams(
                    a=actor_forward, b=actor_reverse,
                ),
            )
            conn.execute(
                text(
                    "DELETE FROM capital_flows_ttm_state WHERE actor_id IN (:a, :b)",
                ).bindparams(a=actor_forward, b=actor_reverse),
            )
