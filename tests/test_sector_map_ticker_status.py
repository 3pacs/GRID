"""Sector-map ticker lifecycle sidecar and the Tiingo price list built from it.

2026-11-08 00:33Z: the routine Tiingo pull fetched 103 non-current tickers for
0 rows. Sector-map actors under old symbols (NYCB, SJW, TPX, ERJ...) got no
prices, dead names were fetched every night, and ``=F`` futures were sent as
bare equity symbols (ZS=F -> Zscaler). analysis/sector_map_ticker_status.yaml
marks each map ticker that no longer trades under that symbol; the map itself
is unchanged (sha256-pinned by the insider-density pre-registration, and dead
actors must stay for survivorship-free history).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import requests

from analysis import sector_map as sm
from ingestion.tiingo_pull import TiingoPuller, is_tiingo_equity


def _map_tickers() -> set[str]:
    out: set[str] = set()
    for sector in sm.SECTOR_MAP.values():
        if sector.get("etf"):
            out.add(sector["etf"])
        for sub in sector.get("subsectors", {}).values():
            out.update(a["ticker"] for a in sub.get("actors", []) if a.get("ticker"))
    return out


# ── the committed sidecar ────────────────────────────────────────────────


def test_every_status_key_is_a_sector_map_ticker() -> None:
    assert set(sm.TICKER_STATUS) <= _map_tickers()


def test_dead_actors_stay_in_the_map() -> None:
    # Survivorship: the panels read the map historically, so nothing is dropped.
    tickers = _map_tickers()
    assert {"NYCB", "HES", "DFS", "WBA", "PTR", "AVB"} <= tickers


def test_successors_are_tiingo_equities_and_live() -> None:
    for ticker, entry in sm.TICKER_STATUS.items():
        if entry["status"] == "renamed":
            live = sm.price_ticker(ticker)
            assert live is not None and live not in sm.TICKER_STATUS, ticker
            assert is_tiingo_equity(live), ticker


@pytest.mark.parametrize("old, new", [("NYCB", "FLG"), ("SJW", "HTO"), ("TPX", "SGI"), ("ERJ", "EMBJ"),
                                      ("MOG.A", "MOG-A"), ("KELY.A", "KELYA"), ("EQR", "VMRK")])
def test_renamed_symbols_price_under_the_successor(old: str, new: str) -> None:
    assert sm.price_ticker(old) == new


@pytest.mark.parametrize("dead", ["HES", "JNPR", "DFS", "WBA", "SKX", "CFLT", "PTR", "UMG", "FCPO"])
def test_dead_and_foreign_symbols_price_nothing(dead: str) -> None:
    assert sm.price_ticker(dead) is None


def test_price_tickers_swaps_and_skips() -> None:
    tickers = set(sm.price_tickers())
    assert {"FLG", "HTO", "SGI", "EMBJ", "XLK", "NVDA"} <= tickers
    assert not tickers & set(sm.TICKER_STATUS)
    assert sm.price_ticker("NVDA") == "NVDA"


# ── loader validation ────────────────────────────────────────────────────


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "status.yaml"
    path.write_text(body, encoding="utf-8")
    return path


@pytest.mark.parametrize("body, msg", [
    ("OTHER: {}\n", "TICKER_STATUS"),
    ("TICKER_STATUS:\n  AAA: {status: gone}\n", "unknown status"),
    ("TICKER_STATUS:\n  AAA: {status: renamed}\n", "successor"),
    ("TICKER_STATUS:\n  AAA: {status: acquired, successor: BBB}\n", "successor"),
    ("TICKER_STATUS:\n  AAA: {status: renamed, successor: BBB}\n  BBB: {status: renamed, successor: AAA}\n",
     "cycle"),
])
def test_loader_refuses_bad_sidecars(tmp_path: Path, body: str, msg: str) -> None:
    with pytest.raises(ValueError, match=msg):
        sm._load_ticker_status(_write(tmp_path, body))


def test_loader_reads_a_successor_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = ("TICKER_STATUS:\n  AAA: {status: renamed, successor: BBB}\n"
            "  BBB: {status: renamed, successor: CCC}\n  DDD: {status: delisted}\n")
    monkeypatch.setattr(sm, "TICKER_STATUS", sm._load_ticker_status(_write(tmp_path, body)))
    assert sm.price_ticker("AAA") == "CCC"
    assert sm.price_ticker("DDD") is None


# ── Tiingo instrument rule ───────────────────────────────────────────────


@pytest.mark.parametrize("sym, ok", [
    ("ZS=F", False), ("CL=F", False), ("GC=F", False),     # futures -> unrelated equities
    ("EURUSD=X", True), ("USDJPY=X", True),                # real FX on Tiingo
    ("BRL=X", False), ("MXN=X", False),                    # 3-letter yfinance shorthand
    ("ATD.TO", False), ("L.TO", False), ("DX-Y.NYB", False),
    ("MOG-A", True), ("BRK-B", True), ("MOG.A", True), ("FLG", True),
])
def test_is_tiingo_equity_instrument_classes(sym: str, ok: bool) -> None:
    assert is_tiingo_equity(sym) is ok


def test_default_tickers_send_only_live_tiingo_equities() -> None:
    tickers = set(TiingoPuller.default_tickers())
    assert {"FLG", "HTO", "SGI", "EMBJ", "MOG-A", "SPY", "EURUSD=X"} <= tickers
    assert not tickers & {"NYCB", "HES", "PTR", "UMG", "ZS=F", "GC=F", "^VIX", "BTC-USD", "DX-Y.NYB", "ATD.TO"}
    assert all(is_tiingo_equity(t) for t in tickers)


def test_pull_ticker_never_fetches_a_non_tiingo_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    def _no_http(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("fetched")

    monkeypatch.setattr(requests, "get", _no_http)
    puller = TiingoPuller.__new__(TiingoPuller)
    puller.source_id = 1
    res = puller.pull_ticker("ZS=F", start_date="2026-11-01")
    assert res["status"] == "PARTIAL" and res["rows_inserted"] == 0
    assert "not a Tiingo equity" in res["errors"][0]
