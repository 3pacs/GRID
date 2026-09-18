"""Test-only bootstrap of runtime-created tables the suite needs.

This is "bootstrap current application schema for tests" — explicitly NOT a
migration-chain repair. It does not touch ``schema.sql`` or any Alembic
revision (both belong to the release lead's lane), and it does not change
what any test asserts.

Context (see ``grid-fake-data-DB-GATED-COVERAGE.md`` / the CI-fidelity
follow-up doc, 2026-09-18): after the CI workflow bootstraps the database
with ``schema.sql`` -> ``alembic stamp`` -> ``alembic upgrade head``, three
groups of tests still fail or skip because their tables are created only by
runtime application code, never by ``schema.sql`` and never applied by the
Alembic chain the stamp exercises:

* ``contracts_dead_letter`` (+ ``contracts_audit``, needed by the same
  fixture's teardown) — created by the raw SQL migration
  ``scripts/migrations/20260411_contracts_infrastructure.sql`` (idempotent:
  every statement is ``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT
  EXISTS``). Nothing in the Alembic chain or ``schema.sql`` runs it.
  Needed by: ``tests/contracts/test_dead_letter.py`` (3 tests).

* ``capital_flows`` (+ the harmless-to-create ``supply_chain_nodes`` /
  ``supply_chain_edges`` from the same file) — created by the raw SQL
  migration ``migrations/0021_supply_chain_and_capital_flows.sql`` (also
  idempotent ``IF NOT EXISTS`` DDL only). Needed by:
  ``tests/test_capital_flow_rollups.py`` (7), ``tests/test_holder_deal_overlap.py``
  (7), ``tests/test_acquisition_decomposition.py`` (4) — 18 tests total.

* ``canvas_boards`` / ``canvas_nodes`` / ``canvas_edges`` — created by the
  idempotent Python function
  ``api.routers.canvas_board_store.ensure_legacy_canvas_tables(conn)``. The
  application code that mirrors writes into these "legacy shadow" tables
  (``api/routers/canvas_graph.py``) only ever calls the sibling
  ``ensure_investigation_boards_table``, never this one, so on a database
  where these tables were never created lazily by some *other* path they are
  simply missing. Needed by:
  ``tests/test_canvas_graph_state_store.py::TestCanvasGraphStateCompatibility``
  (the 2 tests that don't already call ``ensure_legacy_canvas_tables``
  themselves).

Each of the two SQL-migration-backed groups is bootstrapped by executing the
migration file's own text, the same way
``tests/contracts/test_migration.py`` already proves those two tables get
created (it runs the contracts migration file directly). Running the whole
file is deliberate: `IF NOT EXISTS` makes re-running it a no-op, and the
extra tables in the same file (``supply_chain_nodes``/``supply_chain_edges``,
``contracts_audit``) are prerequisites of other tests in the same gated set.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

_ROOT = Path(__file__).resolve().parent.parent

_CONTRACTS_MIGRATION = _ROOT / "scripts" / "migrations" / "20260411_contracts_infrastructure.sql"
_CAPITAL_FLOWS_MIGRATION = _ROOT / "migrations" / "0021_supply_chain_and_capital_flows.sql"

_bootstrapped = False


def bootstrap_test_prerequisites(engine: Engine) -> None:
    """Create the runtime-only tables the database-gated tests need.

    Idempotent and safe to call once per test session (guarded by a module
    global) or repeatedly — every statement it runs is ``IF NOT EXISTS``.
    Tables created, and the existing idempotent creator used for each:

    * ``contracts_audit``, ``contracts_dead_letter`` — via the DDL in
      ``scripts/migrations/20260411_contracts_infrastructure.sql``.
    * ``supply_chain_nodes``, ``supply_chain_edges``, ``capital_flows`` — via
      the DDL in ``migrations/0021_supply_chain_and_capital_flows.sql``.
    * ``canvas_boards``, ``canvas_nodes``, ``canvas_edges`` — via
      ``api.routers.canvas_board_store.ensure_legacy_canvas_tables(conn)``.

    Does not touch ``schema.sql`` or any Alembic revision. This proves only
    that the tables the current application code expects exist for the
    tests that exercise it directly — it is not a claim that the Alembic
    migration history replays cleanly (a separate, unrelated problem; see
    the CI-fidelity follow-up doc's Problem B).
    """
    global _bootstrapped
    if _bootstrapped:
        return

    from api.routers.canvas_board_store import ensure_legacy_canvas_tables

    with engine.begin() as conn:
        conn.execute(text(_CONTRACTS_MIGRATION.read_text(encoding="utf-8")))
        conn.execute(text(_CAPITAL_FLOWS_MIGRATION.read_text(encoding="utf-8")))
        ensure_legacy_canvas_tables(conn)

    _bootstrapped = True
