"""The risk map's concentration read runs against the real watchlist DDL.

The block once read ``allocation_pct``, ``sector`` and ``active``, which the
watchlist never had, and failed with UndefinedColumn on every call. This
creates the table with ``watchlist_helpers._ensure_watchlist_table``, the
DDL the app runs, in a scratch schema. It then checks two things: the read
succeeds, and it scores only weighted names. Runs on ``pg_engine``
(``GRID_TEST_DB_URL``) and skips without PostgreSQL.
"""

from __future__ import annotations

import sys
import types
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture()
def scratch(pg_engine: Engine, monkeypatch):
    from api.routers import intelligence_risk as ir
    from api.routers import watchlist_helpers as wh

    schema = f"risk_map_conc_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    monkeypatch.setattr(wh, "get_db_engine", lambda: engine)
    monkeypatch.setattr(ir, "get_db_engine", lambda: engine)
    mod = types.ModuleType("physics.dealer_gamma")

    class DealerGammaEngine:  # test stub
        def __init__(self, engine):
            pass

        def compute_gex_profile(self, ticker):
            return {"error": "no chain"}

    mod.DealerGammaEngine = DealerGammaEngine
    monkeypatch.setitem(sys.modules, "physics.dealer_gamma", mod)
    ir._risk_map_cache.clear()
    try:
        wh._ensure_watchlist_table()
        yield engine
    finally:
        ir._risk_map_cache.clear()
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _insert(engine: Engine, rows) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO watchlist (ticker, asset_type, weight) VALUES (:t, :a, :w)"),
            [{"t": t, "a": a, "w": w} for t, a, w in rows],
        )


def test_unweighted_watchlist_reads_cleanly_and_reports_why(scratch):
    from api.routers import intelligence_risk as ir

    _insert(scratch, [("AAPL", "stock", None), ("BTC", "stock", None), ("SPY", "etf", None)])

    block = ir._build_risk_map()["concentration_risk"]

    assert block["available"] is False
    assert block["reason"] == "no watchlist position weights set (3 names, none with weight > 0)"


def test_weighted_watchlist_is_scored_from_weight(scratch):
    from api.routers import intelligence_risk as ir

    _insert(scratch, [("BTC", "stock", 60), ("ETH", "crypto", 20), ("SPY", "etf", 20), ("VUZI", "stock", None)])

    block = ir._build_risk_map()["concentration_risk"]

    assert block["available"] is True, block
    assert block["weighted_names"] == 3
    assert block["unweighted_names"] == 1
    assert block["single_name_max"] == {"ticker": "BTC", "weight": 0.6}
    assert block["sector_concentration"] == {"Crypto": 0.8, "ETF": 0.2}
    assert block["risk_level"] == "critical"  # 0.15 + 0.35 + 0.25 + 0.2
