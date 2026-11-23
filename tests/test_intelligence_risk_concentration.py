"""Concentration block of ``GET /api/v1/intelligence/risk-map``.

The block read ``SELECT ticker, allocation_pct, sector FROM watchlist WHERE
active = true``. The watchlist has none of those columns (it has ticker,
display_name, asset_type, added_at, notes, weight), so every call failed
with UndefinedColumn: on griddb (2026-11-23), "column allocation_pct does
not exist".

Pinned here:

* the read names only real columns, and position size is ``weight``;
* only names with a positive, finite weight are scored. With none, the
  block is unavailable with an honest reason, and equal weights are never
  assumed;
* sectors come from ``watchlist_helpers.watchlist_sectors``, the helper the
  portfolio view uses.

No database: the engine is a ``MagicMock`` and the dealer gamma engine is
stubbed. ``tests/test_intelligence_risk_concentration_pg.py`` runs the
read against the real watchlist DDL.
"""

from __future__ import annotations

import sys
import types
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from api.routers import intelligence_risk as ir
from api.routers import watchlist_helpers as wh


def _engine(watchlist_rows, statements):
    engine = MagicMock()
    conn = MagicMock()

    def execute(stmt, params=None, *args, **kwargs):
        sql = str(getattr(stmt, "text", stmt))
        statements.append(sql)
        res = MagicMock()
        rows = watchlist_rows if "FROM watchlist" in sql else []
        res.fetchall.return_value = rows
        res.fetchone.return_value = rows[0] if rows else None
        return res

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


@pytest.fixture()
def build(monkeypatch):
    """Run ``_build_risk_map`` over the given watchlist rows; return (result, statements)."""
    mod = types.ModuleType("physics.dealer_gamma")

    class DealerGammaEngine:  # test stub
        def __init__(self, engine):
            pass

        def compute_gex_profile(self, ticker):
            return {"error": "no chain"}

    mod.DealerGammaEngine = DealerGammaEngine
    monkeypatch.setitem(sys.modules, "physics.dealer_gamma", mod)
    ir._risk_map_cache.clear()

    def run(rows):
        statements: list[str] = []
        monkeypatch.setattr(ir, "get_db_engine", lambda: _engine(rows, statements))
        return ir._build_risk_map(), statements

    yield run
    ir._risk_map_cache.clear()


def _fixed_sectors(monkeypatch, mapping):
    monkeypatch.setattr(wh, "watchlist_sectors", lambda items: {t: mapping[t] for t, _ in items})


def test_read_names_only_columns_the_watchlist_has(build):
    _, statements = build([])
    reads = [s for s in statements if "FROM watchlist" in s]
    assert len(reads) == 1
    assert "SELECT ticker, asset_type, weight FROM watchlist" in reads[0]
    for missing in ("allocation_pct", "sector", "active"):
        assert missing not in reads[0], missing


def test_empty_watchlist_is_unavailable(build):
    block = build([])[0]["concentration_risk"]
    assert block == {"risk_level": "unknown", "available": False, "reason": "no watchlist rows"}


def test_no_weights_is_unavailable_not_equal_weighted(build):
    """griddb on 2026-11-23: 21 names and no weights. Equal weights would
    score the list's length (top-5 = 5/21, low)."""
    rows = [(f"T{i}", "stock", None) for i in range(21)]
    result = build(rows)[0]

    block = result["concentration_risk"]
    assert block["available"] is False
    assert block["reason"] == "no watchlist position weights set (21 names, none with weight > 0)"
    assert "top_5_watchlist_weight" not in block
    assert "Concentration" in result["unavailable_subsystems"]


def test_weighted_names_are_scored_unweighted_reported(build, monkeypatch):
    _fixed_sectors(monkeypatch, {"NVDA": "Technology", "MSFT": "Technology", "AAPL": "Technology",
                                 "BTC": "Crypto"})
    rows = [
        ("NVDA", "stock", Decimal("40")),
        ("MSFT", "stock", Decimal("30")),
        ("VUZI", "stock", None),
        ("AAPL", "stock", Decimal("10")),
        ("BTC", "stock", Decimal("20")),
        ("SLS", "stock", None),
    ]
    block = build(rows)[0]["concentration_risk"]

    assert block["available"] is True
    assert block["weighted_names"] == 4
    assert block["unweighted_names"] == 2
    assert block["top_5_watchlist_weight"] == 1.0
    assert block["single_name_max"] == {"ticker": "NVDA", "weight": 0.4}
    assert block["sector_concentration"] == {"Technology": 0.8, "Crypto": 0.2}
    # 0.15 + 0.35 (top-5 > 0.7) + 0.25 (sector > 0.4) + 0.2 (name > 0.2)
    assert block["risk_level"] == "critical"


def test_spread_weights_score_low(build, monkeypatch):
    names = [f"N{i}" for i in range(20)]
    _fixed_sectors(monkeypatch, {n: f"S{i % 5}" for i, n in enumerate(names)})
    block = build([(n, "stock", Decimal("5")) for n in names])[0]["concentration_risk"]

    assert block["top_5_watchlist_weight"] == 0.25
    assert block["single_name_max"]["weight"] == 0.05
    assert block["risk_level"] == "low"  # 0.15, nothing crosses a threshold


@pytest.mark.parametrize("bad", [Decimal("0"), Decimal("-3"), Decimal("NaN"), float("inf")])
def test_non_positive_or_non_finite_weights_are_not_positions(build, monkeypatch, bad):
    _fixed_sectors(monkeypatch, {"AAA": "X", "BBB": "Y"})
    block = build([("AAA", "stock", Decimal("10")), ("BBB", "stock", bad)])[0]["concentration_risk"]

    assert block["weighted_names"] == 1
    assert block["unweighted_names"] == 1
    assert block["single_name_max"] == {"ticker": "AAA", "weight": 1.0}


def test_weighted_names_use_the_effective_asset_type_for_sectors(build, monkeypatch):
    """BTC stored as 'stock' (the old default) is sectored as crypto, as in the portfolio view."""
    seen: list[tuple[str, str]] = []

    def sectors(items):
        seen.extend(items)
        return {t: "Crypto" if at == "crypto" else "Other" for t, at in items}

    monkeypatch.setattr(wh, "watchlist_sectors", sectors)
    block = build([("BTC", "stock", Decimal("1")), ("VUZI", "stock", Decimal("1"))])[0]["concentration_risk"]

    assert ("BTC", "crypto") in seen
    assert block["sector_concentration"] == {"Crypto": 0.5, "Other": 0.5}


# ── watchlist_sectors ────────────────────────────────────────────────────

def _fake_sector_map(monkeypatch, sector_map):
    mod = types.ModuleType("analysis.sector_map")
    mod.SECTOR_MAP = sector_map
    monkeypatch.setitem(sys.modules, "analysis.sector_map", mod)


def test_watchlist_sectors_actor_map_then_asset_type(monkeypatch):
    _fake_sector_map(monkeypatch, {
        "Technology": {"subsectors": {"Chips": {"actors": [{"ticker": "NVDA"}, {"ticker": "AAPL"}]}}},
        "Communication Services": {"subsectors": {"Devices": {"actors": [{"ticker": "AAPL"}]}}},
    })
    items = [("NVDA", "stock"), ("AAPL", "stock"), ("BTC", "crypto"), ("SPY", "etf"),
             ("CL", "commodity"), ("VUZI", "stock"), ("EURUSD", "forex")]

    assert wh.watchlist_sectors(items) == {
        "NVDA": "Technology",
        "AAPL": "Communication Services",  # several sectors: the last in map order
        "BTC": "Crypto", "SPY": "ETF", "CL": "Commodities",
        "VUZI": "Other", "EURUSD": "Other",
    }


def test_watchlist_sectors_without_a_sector_map_falls_back_to_asset_type(monkeypatch):
    monkeypatch.setitem(sys.modules, "analysis.sector_map", None)  # import raises ImportError

    assert wh.watchlist_sectors([("NVDA", "stock"), ("ETH", "crypto")]) == {"NVDA": "Other", "ETH": "Crypto"}
