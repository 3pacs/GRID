"""The freshness-audit universe gets a routine Tiingo close.

Twelve Data was the only writer keeping ``ticker_metrics_daily`` closes
current for these tickers (grid-svr 2026-11-23: 209 of the 618 had no stored
close of their own in the last 5 days). With Twelve Data dropped they join the
routine Tiingo list, and ``scripts/close_backfill_universe.py`` copies the
stored closes across.

Local fakes only: no network, no database.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import ingestion.tiingo_pull as tp
from ingestion.tiingo_pull import TiingoPuller

ROOT = Path(__file__).resolve().parents[1]


class _Engine:
    def __init__(self, rows: list[tuple] | None = None, error: Exception | None = None) -> None:
        self.rows, self.error = rows or [], error
        self.calls: list[tuple[str, dict]] = []

    def connect(self) -> "_Engine":
        if self.error:
            raise self.error
        return self

    def __enter__(self) -> "_Engine":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, sql: Any, params: dict | None = None) -> Any:
        self.calls.append((" ".join(str(sql).split()), dict(params or {})))
        return type("R", (), {"fetchall": lambda _self: list(self.rows)})()


def test_reads_the_audits_own_rule_with_bound_parameters():
    engine = _Engine([("AAPL",), ("XLF",)])
    assert TiingoPuller.freshness_universe_tickers(engine) == ["AAPL", "XLF"]
    statement, params = engine.calls[0]
    assert "FROM signal_registry" in statement and "source_module = ANY(:modules)" in statement
    assert params == {"modules": list(tp.FRESHNESS_UNIVERSE_MODULES), "cap": tp.FRESHNESS_UNIVERSE_MAX_TICKERS}


def test_the_module_list_is_the_one_the_audit_sql_uses():
    sql = (ROOT / "scripts" / "freshness_audit_universe.sql").read_text(encoding="utf-8")
    block = sql.split("INSERT INTO data_freshness_universe", 1)[1].split(";", 1)[0]
    in_list = re.search(r"source_module IN \(([^)]*)\)", block).group(1)
    assert tuple(re.findall(r"'([^']+)'", in_list)) == tp.FRESHNESS_UNIVERSE_MODULES
    assert "ticker ~ '^[A-Z]{1,6}$'" in block and "ticker ~ '^[A-Z]{1,6}$'" in tp.FRESHNESS_UNIVERSE_TICKERS_SQL


def test_non_equities_and_placeholders_are_left_out():
    engine = _Engine([("AAPL",), ("VFIAX",), (None,), ("",), ("MACRO",)])
    tickers = TiingoPuller.freshness_universe_tickers(engine)
    assert "AAPL" in tickers and None not in tickers and "" not in tickers
    assert all(tp.is_tiingo_equity(t) for t in tickers)


def test_an_unreachable_database_yields_nothing_and_does_not_raise():
    assert TiingoPuller.freshness_universe_tickers(_Engine(error=RuntimeError("db down"))) == []


def test_default_tickers_include_the_freshness_universe(monkeypatch):
    for name in ("oracle_tickers", "hypothesis_tickers", "insider_buyer_tickers", "supply_chain_tickers"):
        monkeypatch.setattr(TiingoPuller, name, staticmethod(lambda *_a, **_k: []))
    monkeypatch.setattr(TiingoPuller, "freshness_universe_tickers", staticmethod(lambda _e: ["ZVRA", "XMTR"]))
    tickers = TiingoPuller.default_tickers(object())
    assert {"ZVRA", "XMTR"} <= set(tickers)
    assert "ZVRA" not in TiingoPuller.default_tickers(None)      # no database, static lists only
