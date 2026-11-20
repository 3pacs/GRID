"""Every listed supply-chain graph node gets a routine Tiingo close (2026-11-16)."""
from __future__ import annotations

from unittest.mock import MagicMock

from ingestion import tiingo_pull as tp


class _Conn:
    def __init__(self, ids, types):
        self.ids, self.types = ids, types

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if "supply_chain_edges" in sql:
            return _Result([(i,) for i in self.ids])
        assert "supply_chain_nodes" in sql
        return _Result([(i, t) for i, t in self.types.items() if i in params["ids"]])


class _Result(list):
    def fetchall(self):
        return list(self)


class _Engine:
    def __init__(self, conn):
        self.conn = conn

    def connect(self):
        return self.conn


IDS = ["glw", "corning", "aapl", "lonza", "000660.ks", "cargill", "k", "wba", "hon", "untyped"]
TYPES = {"glw": "ticker", "corning": "ticker", "aapl": "ticker", "lonza": "ticker",
         "000660.ks": "ticker", "cargill": "private_company", "k": "ticker", "wba": "ticker", "hon": "ticker"}


def test_graph_symbols_are_priced_through_the_shared_node_rule():
    got = tp.TiingoPuller.supply_chain_tickers(_Engine(_Conn(IDS, TYPES)))
    # corning and glw are one symbol; foreign, private, delisted and untyped nodes add nothing
    assert got == ["AAPL", "GLW", "HON"]


def test_default_tickers_include_the_graph(monkeypatch):
    for name in ("oracle_tickers", "hypothesis_tickers", "insider_buyer_tickers"):
        monkeypatch.setattr(tp.TiingoPuller, name, staticmethod(lambda e: []))
    monkeypatch.setattr(tp.TiingoPuller, "supply_chain_tickers", staticmethod(lambda e: ["GLW", "SWKS"]))
    assert {"GLW", "SWKS"} <= set(tp.TiingoPuller.default_tickers(object()))


def test_an_unreachable_db_adds_nothing():
    eng = MagicMock()
    eng.connect.side_effect = RuntimeError("db down")
    assert tp.TiingoPuller.supply_chain_tickers(eng) == []
