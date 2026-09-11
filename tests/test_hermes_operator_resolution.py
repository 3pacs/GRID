"""
Tests for scripts/hermes_operator.py's per-cycle resolution step.

Regression coverage for the frozen-price incident: the cycle's "fast SQL
resolution" step used to hand-roll an INSERT ... SELECT that joined a
nonexistent `entity_map` table and wrote to columns resolved_series
doesn't have, with an ON CONFLICT target one column short of the real
unique index. It raised on every cycle and was swallowed by log.debug,
so resolved_series silently stopped advancing (equity `_full` price
features among them) while raw ingestion kept flowing untouched.
"""

from __future__ import annotations

import ast
import inspect
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import scripts.hermes_operator as hermes


def _executable_body_source(func) -> str:
    """Source of `func`'s body, excluding its docstring.

    Lets a regression test assert what the code actually does without
    tripping over the docstring's own prose describing the historical bug.
    """
    src = inspect.getsource(func)
    func_node = ast.parse(src).body[0]
    body_nodes = func_node.body
    if (
        body_nodes
        and isinstance(body_nodes[0], ast.Expr)
        and isinstance(body_nodes[0].value, ast.Constant)
        and isinstance(body_nodes[0].value.value, str)
    ):
        body_nodes = body_nodes[1:]
    return "\n".join(ast.get_source_segment(src, n) for n in body_nodes)


def test_run_fast_resolution_delegates_to_resolver(monkeypatch):
    """run_fast_resolution builds a Resolver on the given engine and calls
    resolve_pending with a bounded lookback, returning its summary as-is."""
    fake_engine = MagicMock()
    calls: dict = {}

    class FakeResolver:
        def __init__(self, db_engine):
            calls["engine"] = db_engine

        def resolve_pending(self, lookback_days=30, workers=8):
            calls["lookback_days"] = lookback_days
            calls["workers"] = workers
            return {"resolved": 3, "conflicts_found": 0, "errors": 0}

    monkeypatch.setitem(
        sys.modules,
        "normalization.resolver",
        SimpleNamespace(Resolver=FakeResolver),
    )

    result = hermes.run_fast_resolution(fake_engine)

    assert result == {"resolved": 3, "conflicts_found": 0, "errors": 0}
    assert calls["engine"] is fake_engine
    # Bounded so the per-cycle hot loop stays cheap — not the 30-day default.
    assert calls["lookback_days"] <= 7
    assert calls["workers"] >= 1


def test_run_fast_resolution_swallows_resolver_exception(monkeypatch):
    """A Resolver failure is caught, logged, and returned as an error dict —
    it must never bubble up and abort the rest of the Hermes cycle."""
    fake_engine = MagicMock()

    class BoomResolver:
        def __init__(self, db_engine):
            pass

        def resolve_pending(self, lookback_days=30, workers=8):
            raise RuntimeError("boom")

    monkeypatch.setitem(
        sys.modules,
        "normalization.resolver",
        SimpleNamespace(Resolver=BoomResolver),
    )

    result = hermes.run_fast_resolution(fake_engine)

    assert "error" in result
    assert "boom" in result["error"]


def test_run_fast_resolution_never_reintroduces_the_broken_raw_sql():
    """Regression guard: the old hand-rolled INSERT referenced a table that
    was never declared in schema.sql (`entity_map`), columns resolved_series
    doesn't have (`resolved_at`, `source_id`), and an ON CONFLICT target
    (feature_id, obs_date) one column short of the real unique index
    uq_resolved_series_composite (feature_id, obs_date, vintage_date).

    Assert the function body no longer contains any of that shape, so a
    future "let's inline this for speed" edit can't reintroduce the exact
    silent failure — it should go through normalization.resolver.Resolver,
    the one place that already gets the schema right.
    """
    body = _executable_body_source(hermes.run_fast_resolution)

    assert "entity_map" not in body
    assert "resolved_at" not in body
    assert "ON CONFLICT (feature_id, obs_date)" not in body
    assert "Resolver" in body
    assert "resolve_pending" in body


def test_run_cycle_resolution_branch_calls_run_fast_resolution():
    """The main cycle's non-dry-run resolution branch must call
    run_fast_resolution(engine) rather than running its own inline SQL —
    inspected at the source level since exercising all of run_cycle's other
    steps end-to-end is out of scope for this regression test."""
    src = inspect.getsource(hermes.run_cycle)

    # Isolate the resolution step's block (from its comment marker to the
    # next numbered step) so this assertion is specific to that branch.
    marker = "# 3b. Resolution"
    assert marker in src
    start = src.index(marker)
    end = src.index("\n    # 4.", start)
    block = src[start:end]

    assert "run_fast_resolution(engine)" in block
    assert "entity_map" not in block
    assert "resolved_at" not in block
