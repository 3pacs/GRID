"""Tests for api/routers/explain.py — the "why did this move?" endpoint.

We build a ``FakeEngine`` that dispatches SQL prefixes to canned rows
so the business logic can be exercised without a live Postgres.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import Any

import pytest

from api.routers import explain as exp_mod
from api.routers.explain import (
    TYPE_WEIGHTS,
    _build_explain,
    _narrate,
    _recency_weight,
    _score,
    get_actor_explain,
)


# ── Fake DB ──────────────────────────────────────────────────────────


class _Row:
    """Tuple-indexable row stand-in."""

    def __init__(self, *values: Any) -> None:
        self._v = values

    def __getitem__(self, idx: int) -> Any:
        return self._v[idx]

    def __iter__(self):
        return iter(self._v)


class _Result:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class FakeConn:
    """Minimal ``engine.connect()`` context manager + execute() stub.

    ``tables`` is the set of existing table names.
    ``rows`` maps a matcher string → list[_Row] to return.
    """

    def __init__(
        self,
        tables: set[str] | None = None,
        rows: dict[str, list[Any]] | None = None,
    ) -> None:
        self.tables = tables or set()
        self.rows = rows or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def connect(self):
        return self

    def execute(self, stmt, params=None):
        sql = str(stmt).lower()
        # Resolve the bound param value if passed via .bindparams(n=...)
        name = None
        if params and isinstance(params, dict):
            name = params.get("n")
        if name is None:
            try:
                # TextClause .compile() exposes bound params via .params
                compiled = stmt.compile()
                name = compiled.params.get("n")
            except Exception:
                name = None
        if "to_regclass" in sql:
            return _Result(
                [_Row(name if name in self.tables else None)]
            )
        # Pick the first matcher that appears in sql.
        for matcher, canned in self.rows.items():
            if matcher in sql:
                return _Result(canned)
        return _Result([])


class FakeEngine:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    def connect(self):
        return self._conn


# ── Helpers ──────────────────────────────────────────────────────────


PIVOT = date(2026, 4, 5)
WINDOW = 5


def _empty_engine() -> FakeEngine:
    return FakeEngine(FakeConn(tables=set()))


# ── Tests ────────────────────────────────────────────────────────────


def test_window_boundaries():
    engine = _empty_engine()
    out = _build_explain(engine, "aapl", PIVOT, WINDOW)
    # start = pivot - window_days, end = pivot + window_days//2
    assert out["window"]["pivot"] == "2026-04-05"
    assert out["window"]["start"] == "2026-03-31"
    assert out["window"]["end"] == "2026-04-07"
    assert out["window"]["window_days"] == WINDOW


def test_recency_weight_decays():
    # At pivot → full 1.0
    assert _recency_weight(PIVOT, PIVOT, 5) == 1.0
    # ±1 day within 5d window → 0.9
    assert abs(_recency_weight(PIVOT - timedelta(days=1), PIVOT, 5) - 0.9) < 1e-6
    # Edge of window → 0.5
    assert _recency_weight(PIVOT - timedelta(days=5), PIVOT, 5) == 0.5
    # Missing date → 0.75 midpoint
    assert _recency_weight(None, PIVOT, 5) == 0.75


def test_score_applies_type_weight_and_recency():
    ev = {"type": "contagion_prediction", "date": "2026-04-05"}
    # base 0.85 * recency 1.0 = 0.85
    assert _score(ev, PIVOT, 5) == pytest.approx(0.85, abs=1e-3)
    ev2 = {"type": "dark_pool", "date": "2026-04-05"}
    assert _score(ev2, PIVOT, 5) == pytest.approx(TYPE_WEIGHTS["dark_pool"], abs=1e-3)


def test_evidence_ranking_orders_by_strength():
    # Two tables: insider_trades (strong) + dark_pool_weekly (weaker)
    tables = {"insider_trades", "dark_pool_weekly"}
    rows = {
        "from insider_trades": [
            _Row(
                8921,                        # id
                date(2026, 4, 5),            # trade_date
                "Tim Cook",                  # insider_name
                "CEO",                       # title
                "S",                         # trade_type
                10_000,                      # shares
                14_000_000,                  # value
            )
        ],
        "from dark_pool_weekly": [
            _Row(
                1,                           # id
                date(2026, 4, 5),            # report_date
                58.0,                        # short_volume
                100.0,                       # total_volume
                0.58,                        # short_pct
            )
        ],
    }
    engine = FakeEngine(FakeConn(tables=tables, rows=rows))
    out = _build_explain(engine, "aapl", PIVOT, WINDOW)
    evidence = out["evidence"]
    # Both collected; insider should rank above dark_pool because type weight
    # is higher (0.55 vs 0.35).
    types = [ev["type"] for ev in evidence]
    assert "insider_trade" in types
    assert "dark_pool" in types
    assert types.index("insider_trade") < types.index("dark_pool")
    # Strength monotonic non-increasing
    strengths = [ev["strength"] for ev in evidence]
    assert strengths == sorted(strengths, reverse=True)


def test_graceful_degradation_all_sources_empty():
    engine = _empty_engine()
    out = _build_explain(engine, "aapl", PIVOT, WINDOW)
    # Should not raise, should return empty evidence list and a narrative
    assert out["evidence"] == []
    assert isinstance(out["summary"], str)
    assert "No ranked evidence" in out["summary"] or "activity" in out["summary"]
    assert out["provenance"]["sources_checked"] >= 7
    assert out["provenance"]["evidence_rows"] == 0


def test_narrative_cites_top_evidence():
    top = [
        {"type": "supply_shock_attribution", "strength": 0.82},
        {"type": "contagion_prediction", "strength": 0.70},
        {"type": "insider_trade", "strength": 0.55},
    ]
    move = {"pct": -0.0258}
    out = _narrate("Apple", move, top, 5)
    assert "Apple" in out
    assert "2.58%" in out
    assert "supply shock attribution" in out
    assert "82%" in out
    # At least one reinforcer mentioned
    assert "contagion" in out or "insider" in out


def test_cache_hits_second_call_is_same_object():
    # Use the async endpoint with an empty engine + monkey-patched
    # get_db_engine to return our fake.
    engine = _empty_engine()

    async def _run():
        # Patch get_db_engine just for this call.
        import api.dependencies as deps
        orig = deps.get_db_engine
        deps.get_db_engine = lambda: engine  # type: ignore[assignment]
        try:
            exp_mod._explain_cache.clear()
            # Use a ticker-shaped id so the 404 unknown-actor branch
            # doesn't fire (atype becomes "ticker" via the heuristic).
            r1 = await get_actor_explain("aapl", "2026-04-05", 5, "testtoken")
            r2 = await get_actor_explain("aapl", "2026-04-05", 5, "testtoken")
        finally:
            deps.get_db_engine = orig
        return r1, r2

    r1, r2 = asyncio.run(_run())
    # Cache returns the exact same dict instance on the second call
    assert r1 is r2


def test_unknown_actor_returns_404_when_no_evidence():
    from fastapi import HTTPException

    engine = _empty_engine()

    async def _run():
        import api.dependencies as deps
        orig = deps.get_db_engine
        deps.get_db_engine = lambda: engine  # type: ignore[assignment]
        try:
            exp_mod._explain_cache.clear()
            return await get_actor_explain(
                "zzz_nonexistent_slug_12345", "2026-04-05", 5, "testtoken"
            )
        finally:
            deps.get_db_engine = orig

    with pytest.raises(HTTPException) as ei:
        asyncio.run(_run())
    assert ei.value.status_code == 404


def test_supply_shock_and_contagion_rank_highest():
    """Contagion prediction + supply shock should outrank insider by type weight."""
    tables = {
        "insider_trades",
        "supply_shock_attributions",
        "contagion_predictions",
    }
    rows = {
        "from insider_trades": [
            _Row(1, date(2026, 4, 4), "Someone", "Officer", "P", 1, 100.0)
        ],
        "from supply_shock_attributions": [
            _Row(
                42,                           # id
                "tsmc",                       # upstream_id
                date(2026, 4, 4),             # shock_date
                -0.042,                       # shock_magnitude
                -0.025,                       # downstream_move_pct
                0,                            # lag_days
                0.62,                         # correlation
                "TSMC wafer supply pressure", # evidence
                "lagged_correlation",         # method
            )
        ],
        "from contagion_predictions": [
            _Row(
                12,                                       # id
                "cocoa_beans",                            # shock_node
                "price_increase",                         # shock_type
                0.30,                                     # magnitude
                datetime(2026, 4, 1, 12, 0, 0),           # simulated_at
                {"summary_text": "cocoa + 30%"},          # summary JSONB
                {"tickers": [                             # ranked_impact JSONB
                    {"ticker": "AAPL", "margin_impact_pct": -0.023}
                ]},
            )
        ],
    }
    engine = FakeEngine(FakeConn(tables=tables, rows=rows))
    out = _build_explain(engine, "aapl", PIVOT, WINDOW)
    types = [ev["type"] for ev in out["evidence"]]
    assert types[0] in ("contagion_prediction", "supply_shock_attribution")
    # Both high-strength types outrank insider_trade
    assert types.index("insider_trade") > types.index("supply_shock_attribution")
    assert types.index("insider_trade") > types.index("contagion_prediction")

    # Contagion summary should be the deterministic template format
    contagion_ev = next(
        ev for ev in out["evidence"] if ev["type"] == "contagion_prediction"
    )
    assert "#12" in contagion_ev["summary"]
    assert "cocoa_beans" in contagion_ev["summary"]
