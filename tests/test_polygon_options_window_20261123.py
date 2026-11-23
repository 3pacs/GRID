"""Polygon options snapshot: valid request, whole-window paging, fail-fast on 400/429.

From 2026-11-11 every Polygon run logged "30 of 30 tickers failed" (HTTP 400).
The cause was this puller's own query: ``sort=volume`` is not a sort key of
``/v3/snapshot/options/{underlying}`` (Polygon: "Invalid value for sort
parameter."). These tests pin the replacement request (an expiry window read
whole through ``next_url``), keep the key off any other host, and make a
rejected request cost one call instead of thirty.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any
from unittest.mock import patch

import pytest

from ingestion.smart_scheduler import _classify_outcome
from tests.test_puller_write_contract_20261104 import SECRET, FakeEngine, Resp, _bare

AS_OF = date(2026, 11, 23)
BASE = "https://api.polygon.io/v3/snapshot/options/"


def _contract(kind: str, oi: int, vol: int = 10, iv: float = 0.2,
              delta: float = 0.5, gamma: float = 0.01) -> dict[str, Any]:
    return {"details": {"contract_type": kind}, "open_interest": oi, "day": {"volume": vol},
            "implied_volatility": iv, "greeks": {"delta": delta, "gamma": gamma}}


class Recorder:
    """Fake requests.get: serves queued responses per URL prefix and records each call."""

    def __init__(self, pages: dict[str, list[Resp]]) -> None:
        self.pages = {k: list(v) for k, v in pages.items()}
        self.calls: list[dict[str, Any]] = []

    def __call__(self, url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Resp:
        self.calls.append({"url": url, "params": params, "headers": headers})
        for prefix, queue in self.pages.items():
            if url.startswith(prefix):
                return queue.pop(0)
        raise AssertionError(f"unexpected URL {url}")


def _puller(**kw: Any) -> Any:
    from ingestion.altdata.polygon_puller import PolygonPuller

    return _bare(PolygonPuller, **{"api_key": SECRET, **kw})


def _two_pages(ticker: str) -> list[Resp]:
    return [
        Resp(200, {"status": "OK", "results": [_contract("call", 100), _contract("put", 50, delta=-0.4)],
                   "next_url": f"{BASE}{ticker}?cursor=abc"}),
        Resp(200, {"status": "OK", "results": [_contract("call", 30), _contract("put", 120, delta=-0.3)]}),
    ]


def test_first_request_is_the_expiry_window_without_sort() -> None:
    import ingestion.altdata.polygon_puller as mod

    rec = Recorder({f"{BASE}SPY": [Resp(200, {"status": "OK", "results": [_contract("call", 1)]})]})
    with patch.object(mod.requests, "get", side_effect=rec):
        _puller().pull_options_chain("SPY", as_of=AS_OF)
    params = rec.calls[0]["params"]
    assert "sort" not in params and "order" not in params
    assert params == {
        "limit": 250,
        "expiration_date.gte": "2026-11-23",
        "expiration_date.lte": (AS_OF + timedelta(days=mod.OPTIONS_WINDOW_DAYS)).isoformat(),
    }


def test_follows_next_url_and_summarizes_every_page() -> None:
    import ingestion.altdata.polygon_puller as mod

    rec = Recorder({f"{BASE}SPY": _two_pages("SPY")})
    with patch.object(mod.requests, "get", side_effect=rec):
        summary = _puller().pull_options_greeks_summary("SPY", as_of=AS_OF)
    assert len(rec.calls) == 2
    # The cursor URL carries the filters; no params are re-sent with it.
    assert rec.calls[1]["url"] == f"{BASE}SPY?cursor=abc" and rec.calls[1]["params"] is None
    # The key travels in the header on every page and never in a URL.
    for call in rec.calls:
        assert call["headers"]["Authorization"] == f"Bearer {SECRET}"
        assert SECRET not in call["url"] and SECRET not in str(call["params"])
    assert summary["contracts_count"] == 4
    assert summary["total_call_oi"] == 130 and summary["total_put_oi"] == 170
    assert summary["put_call_ratio"] == round(170 / 130, 4)
    assert summary["pages"] == 2
    assert summary["method"] == mod.SUMMARY_METHOD
    assert summary["expiration_gte"] == "2026-11-23"
    assert summary["expiration_lte"] == (AS_OF + timedelta(days=mod.OPTIONS_WINDOW_DAYS)).isoformat()


def test_next_url_off_polygon_never_gets_the_key() -> None:
    import ingestion.altdata.polygon_puller as mod

    rec = Recorder({
        f"{BASE}SPY": [Resp(200, {"status": "OK", "results": [_contract("call", 1)],
                                  "next_url": "https://elsewhere.example/v3/snapshot/options/SPY?cursor=x"})],
    })
    with patch.object(mod.requests, "get", side_effect=rec), pytest.raises(mod._PolygonHTTPError):
        _puller().pull_options_chain("SPY", as_of=AS_OF)
    assert [c["url"] for c in rec.calls] == [f"{BASE}SPY"]


def test_chain_past_the_page_cap_is_not_summarized_or_stored() -> None:
    import ingestion.altdata.polygon_puller as mod

    endless = [Resp(200, {"status": "OK", "results": [_contract("call", 1)],
                          "next_url": f"{BASE}SPY?cursor={i}"}) for i in range(5)]
    rec = Recorder({f"{BASE}SPY": endless})
    engine = FakeEngine()
    with patch.object(mod, "OPTIONS_TICKERS", ["SPY"]), patch.object(mod, "MAX_CHAIN_PAGES", 3), \
         patch.object(mod.requests, "get", side_effect=rec), patch.object(mod.time, "sleep"):
        out = _puller(engine=engine).pull()
    assert len(rec.calls) == 3
    assert engine.conn.inserts() == []
    assert out["status"] == "FAILED" and out["tickers_failed"] == 1
    assert _classify_outcome(out)[0] == "FAILED"


def test_rejected_request_stops_after_one_call_with_polygons_reason() -> None:
    import ingestion.altdata.polygon_puller as mod

    rec = Recorder({BASE: [Resp(400, {"status": "ERROR", "request_id": "r",
                                      "error": "Invalid value for sort parameter."})] * 30})
    sleeps: list[float] = []
    with patch.object(mod.requests, "get", side_effect=rec), \
         patch.object(mod.time, "sleep", side_effect=sleeps.append):
        out = _puller().pull()
    assert len(rec.calls) == 1 and sleeps == []
    assert out["status"] == "FAILED" and out["api_calls"] == 1
    assert "Invalid value for sort parameter." in out["error"]
    assert SECRET not in out["error"]
    assert _classify_outcome(out)[0] == "FAILED"


def test_rate_limit_stops_the_run_and_keeps_committed_rows() -> None:
    import ingestion.altdata.polygon_puller as mod

    rec = Recorder({
        f"{BASE}SPY": [Resp(200, {"status": "OK", "results": [_contract("call", 100), _contract("put", 50)]})],
        f"{BASE}QQQ": [Resp(429, {"status": "ERROR", "error": "exceeded the maximum requests per minute"})],
    })
    engine = FakeEngine()
    with patch.object(mod, "OPTIONS_TICKERS", ["SPY", "QQQ", "IWM"]), \
         patch.object(mod.requests, "get", side_effect=rec), patch.object(mod.time, "sleep"):
        out = _puller(engine=engine).pull()
    assert len(rec.calls) == 2  # IWM is never requested
    assert out["status"] == "PARTIAL" and out["tickers_pulled"] == 1 and out["tickers_failed"] == 2
    assert out["rows_inserted"] == 6 == len(engine.conn.inserts())


def test_api_calls_counts_pages_and_stored_payload_carries_coverage() -> None:
    import ingestion.altdata.polygon_puller as mod

    rec = Recorder({f"{BASE}SPY": _two_pages("SPY"), f"{BASE}QQQ": _two_pages("QQQ")})
    engine = FakeEngine()
    with patch.object(mod, "OPTIONS_TICKERS", ["SPY", "QQQ"]), \
         patch.object(mod.requests, "get", side_effect=rec), patch.object(mod.time, "sleep"):
        out = _puller(engine=engine).pull()
    assert out["status"] == "SUCCESS" and out["api_calls"] == 4 == len(rec.calls)
    assert _classify_outcome(out) == ("SUCCESS", 12, None)
    pcr_rows = [p for p in engine.conn.inserts() if p["sid"].endswith(":put_call_ratio")]
    assert [p["sid"] for p in pcr_rows] == ["polygon:SPY:put_call_ratio", "polygon:QQQ:put_call_ratio"]
    payloads = [json.loads(p["payload"]) for p in pcr_rows]
    assert all(pl["pages"] == 2 and pl["method"] == mod.SUMMARY_METHOD
               and pl["expiration_gte"] == date.today().isoformat() for pl in payloads)
