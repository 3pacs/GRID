"""Polygon options snapshot: valid request, validated whole-window paging, honest values, paced runs.

From 2026-11-11 every Polygon run logged "30 of 30 tickers failed" (HTTP 400).
The cause was this puller's own query: ``sort=volume`` is not a sort key of
``/v3/snapshot/options/{underlying}`` (Polygon: "Invalid value for sort
parameter."). These tests pin the replacement request: an expiry window read
whole through ``next_url``, with the key kept off any other host, and a
rejected request costs one call instead of thirty.

Codex review 6030035125 (GRID#1401) added four groups, also pinned here:
- pagination integrity: a malformed page, a repeated contract or a cursor
  loop commits nothing for that ticker;
- no known-invalid values: absent IV or OI is not zero, a zero call-OI
  denominator is not a ratio, and net gamma/delta are not emitted. Every
  stored family carries coverage;
- per-request pacing, a DEFERRED rate-limited result, and a 429 hold
  honoured across runs;
- Hermes's stop and deadline, and a default-off gate.
"""

from __future__ import annotations

import inspect
import json
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import ingestion.altdata.polygon_puller as mod
from ingestion.smart_scheduler import _classify_outcome
from tests.test_puller_write_contract_20261104 import SECRET, FakeEngine, Resp, _bare

REAL_GATE = mod.options_writes_enabled
TODAY = date.today()
BASE = "https://api.polygon.io/v3/snapshot/options/"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Gate on, a private hold file, and no hold latched from another test."""
    monkeypatch.setenv("GRID_POLYGON_HOLD_FILE", str(tmp_path / "polygon-hold.json"))
    monkeypatch.setattr(mod, "_held_until_epoch", 0.0)
    monkeypatch.setattr(mod, "options_writes_enabled", lambda: True)


def _c(symbol: str, kind: str = "call", oi: Any = 100, vol: Any = 10, iv: Any = 0.2,
       delta: float = 0.5, gamma: float = 0.01, expiry: date | None = None,
       underlying: str = "SPY") -> dict[str, Any]:
    """One snapshot contract shaped like Polygon's; None drops that field."""
    contract: dict[str, Any] = {
        "details": {"ticker": symbol, "contract_type": kind,
                    "expiration_date": (expiry or TODAY + timedelta(days=7)).isoformat()},
        "underlying_asset": {"ticker": underlying},
        "open_interest": oi,
        "day": {"volume": vol},
        "implied_volatility": iv,
        "greeks": {"delta": delta, "gamma": gamma},
    }
    for key, value in (("open_interest", oi), ("implied_volatility", iv)):
        if value is None:
            del contract[key]
    if vol is None:
        contract["day"] = {}
    return contract


def _page(results: Any, next_url: str | None = None, status: str = "OK") -> Resp:
    body: dict[str, Any] = {"status": status, "request_id": "r", "results": results}
    if next_url:
        body["next_url"] = next_url
    return Resp(200, body)


def _clean(ticker: str) -> list[Resp]:
    return [_page([_c(f"O:{ticker}C1", underlying=ticker),
                   _c(f"O:{ticker}P1", "put", oi=80, delta=-0.4, underlying=ticker)])]


def _two_pages(ticker: str) -> list[Resp]:
    return [
        _page([_c(f"O:{ticker}C1", oi=100, underlying=ticker),
               _c(f"O:{ticker}P1", "put", oi=50, delta=-0.4, underlying=ticker)],
              next_url=f"{BASE}{ticker}?cursor=abc"),
        _page([_c(f"O:{ticker}C2", oi=30, vol=30, iv=0.4, underlying=ticker),
               _c(f"O:{ticker}P2", "put", oi=120, delta=-0.3, underlying=ticker)]),
    ]


class Recorder:
    """Fake requests.get: serves queued responses per URL prefix and records each call."""

    def __init__(self, pages: dict[str, list[Resp]]) -> None:
        self.pages = {k: list(v) for k, v in pages.items()}
        self.calls: list[dict[str, Any]] = []

    def __call__(self, url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Resp:
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        for prefix, queue in self.pages.items():
            if url.startswith(prefix):
                return queue.pop(0)
        raise AssertionError(f"unexpected URL {url}")


def _puller(**kw: Any) -> Any:
    return _bare(mod.PolygonPuller, **{"api_key": SECRET, **kw})


def _run(pages: dict[str, list[Resp]], tickers: list[str], **pull_kw: Any
         ) -> tuple[dict[str, Any], Recorder, FakeEngine, list[float]]:
    rec = Recorder(pages)
    engine = FakeEngine()
    sleeps: list[float] = []
    with patch.object(mod, "OPTIONS_TICKERS", tickers), \
         patch.object(mod.requests, "get", side_effect=rec), \
         patch.object(mod.time, "sleep", side_effect=sleeps.append):
        out = _puller(engine=engine).pull(**pull_kw)
    return out, rec, engine, sleeps


def _rows(engine: FakeEngine, ticker: str) -> dict[str, dict[str, Any]]:
    """Inserted rows of one ticker by family, payload decoded."""
    rows = {}
    for p in engine.conn.inserts():
        if p["sid"].startswith(f"polygon:{ticker}:"):
            rows[p["sid"].rsplit(":", 1)[1]] = {**p, "payload": json.loads(p["payload"])}
    return rows


# ── request shape and paging ────────────────────────────────────────────


def test_first_request_is_the_expiry_window_without_sort() -> None:
    rec = Recorder({f"{BASE}SPY": [_page([_c("O:SPYC1")])]})
    with patch.object(mod.requests, "get", side_effect=rec):
        _puller().pull_options_chain("SPY", as_of=TODAY)
    params = rec.calls[0]["params"]
    assert "sort" not in params and "order" not in params
    assert params == {
        "limit": 250,
        "expiration_date.gte": TODAY.isoformat(),
        "expiration_date.lte": (TODAY + timedelta(days=mod.OPTIONS_WINDOW_DAYS)).isoformat(),
    }
    assert rec.calls[0]["timeout"] == mod.REQUEST_TIMEOUT_S


def test_follows_next_url_and_summarizes_every_page() -> None:
    rec = Recorder({f"{BASE}SPY": _two_pages("SPY")})
    with patch.object(mod.requests, "get", side_effect=rec), patch.object(mod.time, "sleep"):
        summary = _puller().pull_options_greeks_summary("SPY", as_of=TODAY)
    assert len(rec.calls) == 2
    # The cursor URL carries the filters; no params are re-sent with it.
    assert rec.calls[1]["url"] == f"{BASE}SPY?cursor=abc" and rec.calls[1]["params"] is None
    # The key travels in the header on every page and never in a URL.
    for call in rec.calls:
        assert call["headers"]["Authorization"] == f"Bearer {SECRET}"
        assert SECRET not in call["url"] and SECRET not in str(call["params"])
    fam = summary["families"]
    assert fam["call_oi"] == 130 and fam["put_oi"] == 170
    assert fam["put_call_ratio"] == round(170 / 130, 4)
    # Volume-weighted: three contracts at 0.2 x 10, one at 0.4 x 30.
    assert fam["avg_iv"] == round((0.2 * 10 * 3 + 0.4 * 30) / 60, 4)
    cov = summary["coverage"]
    assert cov["pages"] == 2 and cov["contracts_count"] == 4 and cov["method"] == mod.SUMMARY_METHOD
    assert cov["expiration_gte"] == TODAY.isoformat()
    assert cov["expiration_lte"] == (TODAY + timedelta(days=mod.OPTIONS_WINDOW_DAYS)).isoformat()
    assert cov["provider_statuses"] == ["OK"]


@pytest.mark.parametrize("next_url", [
    "https://elsewhere.example/v3/snapshot/options/SPY?cursor=x",  # another host
    f"{BASE}QQQ?cursor=x",                                          # another ticker
    f"{BASE}SPY",                                                   # the first URL again
])
def test_a_cursor_off_this_endpoint_is_never_requested(next_url: str) -> None:
    rec = Recorder({f"{BASE}SPY": [_page([_c("O:SPYC1")], next_url=next_url)]})
    with patch.object(mod.requests, "get", side_effect=rec), pytest.raises(mod.PolygonChainInvalid):
        _puller().pull_options_chain("SPY", as_of=TODAY)
    assert [c["url"] for c in rec.calls] == [f"{BASE}SPY"]


def test_chain_past_the_page_cap_is_not_summarized_or_stored() -> None:
    endless = [_page([_c(f"O:SPYC{i}")], next_url=f"{BASE}SPY?cursor={i}") for i in range(5)]
    with patch.object(mod, "MAX_CHAIN_PAGES", 3):
        out, rec, engine, _ = _run({f"{BASE}SPY": endless}, ["SPY"])
    assert len(rec.calls) == 3
    assert engine.conn.inserts() == []
    assert out["status"] == "FAILED" and out["tickers_failed"] == 1
    assert _classify_outcome(out)[0] == "FAILED"


# ── pagination integrity: the ticker commits nothing ────────────────────

_NEXT = f"{BASE}SPY?cursor=p2"
INTEGRITY_CASES = {
    "continuation without results": [_page([_c("O:SPYC1")], _NEXT), Resp(200, {"status": "OK"})],
    "continuation results not a list": [_page([_c("O:SPYC1")], _NEXT),
                                        Resp(200, {"status": "OK", "results": {"x": 1}})],
    "continuation provider error": [_page([_c("O:SPYC1")], _NEXT),
                                    _page([_c("O:SPYC2")], status="ERROR")],
    "continuation without status": [_page([_c("O:SPYC1")], _NEXT),
                                    Resp(200, {"results": [_c("O:SPYC2")]})],
    "continuation body not JSON": [_page([_c("O:SPYC1")], _NEXT), Resp(200, None)],
    "empty continuation": [_page([_c("O:SPYC1")], _NEXT), _page([])],
    "duplicate contract": [_page([_c("O:SPYC1")], _NEXT), _page([_c("O:SPYC1")])],
    "cursor loop": [_page([_c("O:SPYC1")], _NEXT), _page([_c("O:SPYC2")], next_url=_NEXT)],
    "foreign underlying": [_page([_c("O:SPYC1"), _c("O:QQQC1", underlying="QQQ")])],
    "expiry outside the window": [
        _page([_c("O:SPYC9", expiry=TODAY + timedelta(days=mod.OPTIONS_WINDOW_DAYS + 1))])],
    "no option symbol": [_page([_c("")])],
    "unknown contract type": [_page([_c("O:SPYX1", kind="future")])],
    "contract not an object": [_page(["O:SPYC1"])],
}


@pytest.mark.parametrize("case", sorted(INTEGRITY_CASES))
def test_a_broken_chain_commits_nothing_for_that_ticker(case: str) -> None:
    out, _, engine, _ = _run({f"{BASE}SPY": INTEGRITY_CASES[case], f"{BASE}QQQ": _clean("QQQ")},
                             ["SPY", "QQQ"])
    assert _rows(engine, "SPY") == {}
    assert set(_rows(engine, "QQQ")) == {"put_call_ratio", "avg_iv", "call_oi", "put_oi"}
    assert out["failed_symbols"] == ["SPY"] and out["tickers_pulled"] == 1
    assert out["status"] == "PARTIAL" and _classify_outcome(out)[0] == "PARTIAL"


def test_delayed_pages_are_accepted_and_recorded() -> None:
    out, _, engine, _ = _run({f"{BASE}SPY": [_page([_c("O:SPYC1"), _c("O:SPYP1", "put", delta=-0.5)],
                                                    status="DELAYED")]}, ["SPY"])
    rows = _rows(engine, "SPY")
    assert out["status"] == "SUCCESS" and rows
    assert all(r["payload"]["provider_statuses"] == ["DELAYED"] for r in rows.values())


def test_index_options_may_name_their_underlying_with_the_index_prefix() -> None:
    vix = [_page([_c("O:VIXC1", underlying="I:VIX"), _c("O:VIXP1", "put", underlying="I:VIX")])]
    out, _, engine, _ = _run({f"{BASE}VIX": vix}, ["VIX"])
    assert out["status"] == "SUCCESS" and "call_oi" in _rows(engine, "VIX")


# ── no known-invalid values ─────────────────────────────────────────────


def test_net_gamma_and_net_delta_are_not_emitted() -> None:
    # Put deltas arrive signed (-0.4); the old summary negated them again.
    out, _, engine, _ = _run({f"{BASE}SPY": _clean("SPY")}, ["SPY"])
    rows = _rows(engine, "SPY")
    assert "net_delta" not in rows and "net_gamma" not in rows
    for row in rows.values():
        assert row["payload"]["withheld"]["net_delta"].startswith("not emitted")
        assert row["payload"]["withheld"]["net_gamma"].startswith("not emitted")
    assert out["withheld_families"]["net_delta"] == 1


def test_missing_iv_is_withheld_not_stored_as_zero() -> None:
    # VIX came back with no IV on 2026-11-23; avg_iv was stored as 0.
    chain = [_page([_c("O:VIXC1", iv=None, underlying="VIX"),
                    _c("O:VIXP1", "put", iv=None, underlying="VIX")])]
    out, _, engine, _ = _run({f"{BASE}VIX": chain}, ["VIX"])
    rows = _rows(engine, "VIX")
    assert "avg_iv" not in rows
    assert set(rows) == {"put_call_ratio", "call_oi", "put_oi"}
    assert "implied volatility" in rows["call_oi"]["payload"]["withheld"]["avg_iv"]
    assert rows["call_oi"]["payload"]["contracts_with_iv"] == 0


def test_zero_call_open_interest_is_not_a_ratio() -> None:
    chain = [_page([_c("O:SPYC1", oi=0), _c("O:SPYP1", "put", oi=40, delta=-0.4)])]
    _, _, engine, _ = _run({f"{BASE}SPY": chain}, ["SPY"])
    rows = _rows(engine, "SPY")
    assert "put_call_ratio" not in rows
    # Every call reported 0 open interest: a measured zero, stored as such.
    assert rows["call_oi"]["val"] == 0.0 and rows["put_oi"]["val"] == 40.0
    assert "undefined" in rows["put_oi"]["payload"]["withheld"]["put_call_ratio"]


def test_absent_open_interest_withholds_that_side_and_the_ratio() -> None:
    chain = [_page([_c("O:SPYC1", oi=10), _c("O:SPYP1", "put", oi=None, delta=-0.4),
                    _c("O:SPYP2", "put", oi=5, delta=-0.2)])]
    _, _, engine, _ = _run({f"{BASE}SPY": chain}, ["SPY"])
    rows = _rows(engine, "SPY")
    assert "put_oi" not in rows and "put_call_ratio" not in rows
    assert rows["call_oi"]["val"] == 10.0
    assert rows["call_oi"]["payload"]["withheld"]["put_oi"] == "1 of 2 put contracts have no open interest"


def test_a_chain_with_nothing_measurable_is_no_data() -> None:
    chain = [_page([_c("O:SPYC1", oi=None, iv=None), _c("O:SPYP1", "put", oi=None, iv=None)])]
    out, _, engine, _ = _run({f"{BASE}SPY": chain}, ["SPY"])
    assert engine.conn.inserts() == [] and out["tickers_no_data"] == 1
    assert _classify_outcome(out)[0] == "NO_NEW_DATA"


def test_every_stored_family_carries_coverage_and_its_definition() -> None:
    out, _, engine, _ = _run({f"{BASE}SPY": _two_pages("SPY"), f"{BASE}QQQ": _two_pages("QQQ")},
                             ["SPY", "QQQ"])
    assert out["status"] == "SUCCESS" and out["api_calls"] == 4
    assert _classify_outcome(out) == ("SUCCESS", 8, None)
    for ticker in ("SPY", "QQQ"):
        rows = _rows(engine, ticker)
        assert set(rows) == {"put_call_ratio", "avg_iv", "call_oi", "put_oi"}
        for family, row in rows.items():
            pl = row["payload"]
            assert pl["family"] == family and pl["definition"] == mod.FAMILY_DEFINITIONS[family]
            assert pl["method"] == mod.SUMMARY_METHOD and pl["pages"] == 2
            assert pl["contracts_count"] == 4 and pl["contracts_with_iv"] == 4
            assert pl["contracts_with_greeks"] == 4 and pl["contracts_with_oi"] == 4
            assert pl["expiration_gte"] == TODAY.isoformat()


# ── 400 / pacing / 429 ──────────────────────────────────────────────────


def test_rejected_request_stops_after_one_call_with_polygons_reason() -> None:
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


def test_requests_are_paced_per_request_pages_included() -> None:
    out, rec, _, sleeps = _run({f"{BASE}SPY": _two_pages("SPY"), f"{BASE}QQQ": _two_pages("QQQ")},
                               ["SPY", "QQQ"])
    assert len(rec.calls) == 4
    # Every request after the first waits for the interval, a page as much as a ticker.
    assert len(sleeps) == 3 and all(0 < s <= mod.REQUEST_INTERVAL_S for s in sleeps)
    assert max(sleeps) <= mod.REQUEST_INTERVAL_S  # no 12.5 s per-ticker sleep any more


def test_rate_limit_is_deferred_keeps_rows_and_holds_later_runs(tmp_path: Path) -> None:
    pages = {f"{BASE}SPY": _clean("SPY"),
             f"{BASE}QQQ": [Resp(429, {"status": "ERROR", "error": "exceeded the maximum requests per minute"},
                                 headers={"Retry-After": "1200"})]}
    before = time.time()
    out, rec, engine, _ = _run(pages, ["SPY", "QQQ", "IWM"])
    assert len(rec.calls) == 2  # IWM is never requested
    assert out["status"] == "DEFERRED" and out["rate_limited"] is True
    assert out["stopped_by_budget"] is True and out["tickers_not_attempted"] == ["QQQ", "IWM"]
    assert out["rows_inserted"] == 4 == len(engine.conn.inserts())
    assert _classify_outcome(out)[0] == "PARTIAL"  # not FAILED
    hold = json.loads(Path(mod.hold_file()).read_text())
    held = datetime.strptime(hold["held_until"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    assert hold["status"] == 429 and hold["retry_after_s"] == 1200.0
    assert before + 1200 - 2 <= held <= time.time() + 1200 + 1

    # The next run makes no request while the hold lasts, and keeps the backlog.
    again, rec2, engine2, _ = _run({BASE: [_page([])] * 3}, ["SPY", "QQQ", "IWM"], ticker_list=["QQQ", "IWM"])
    assert rec2.calls == [] and engine2.conn.inserts() == []
    assert again["status"] == "DEFERRED" and again["tickers_not_attempted"] == ["QQQ", "IWM"]
    assert _classify_outcome(again)[0] == "SKIPPED"


def test_the_hold_survives_a_restart_through_its_file(monkeypatch: pytest.MonkeyPatch) -> None:
    mod.record_rate_limit(time.time(), None)
    monkeypatch.setattr(mod, "_held_until_epoch", 0.0)  # a new process: no in-memory latch
    held = mod.rate_limit_hold()
    assert held is not None and held - time.time() == pytest.approx(mod.RATE_LIMIT_HOLD_SECONDS, abs=5)


def test_retry_after_is_read_as_seconds_or_http_date_and_capped() -> None:
    now = time.time()
    when = format_datetime(datetime.fromtimestamp(now + 600, timezone.utc), usegmt=True)
    assert mod._retry_after_seconds(Resp(429, {}, headers={"Retry-After": "30"})) == 30.0
    assert mod._retry_after_seconds(Resp(429, {}, headers={"Retry-After": when}), now) == pytest.approx(600, abs=2)
    assert mod._retry_after_seconds(Resp(429, {}, headers={"Retry-After": "soon"})) is None
    assert mod._retry_after_seconds(Resp(429, {})) is None
    until = mod.record_rate_limit(now, 10 * 24 * 3600.0)
    assert until - now == mod.MAX_RATE_LIMIT_HOLD_SECONDS
    assert mod.record_rate_limit(now, 30.0) - now == mod.MAX_RATE_LIMIT_HOLD_SECONDS  # the latch never shortens


def test_an_unreadable_or_foreign_hold_fails_open() -> None:
    path = Path(mod.hold_file())
    path.write_text("{not json")
    assert mod.rate_limit_hold() is None
    far = datetime.fromtimestamp(time.time() + 30 * 24 * 3600, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path.write_text(json.dumps({"held_until": far}))
    assert mod.rate_limit_hold() is None


# ── Hermes stop, deadline and resume ────────────────────────────────────


def test_stop_request_ends_the_run_without_storing_the_interrupted_ticker() -> None:
    calls = {"n": 0}

    def should_continue() -> bool:
        calls["n"] += 1
        return calls["n"] <= 2  # SPY (1 page) and QQQ page 1; refuse QQQ page 2

    out, rec, engine, _ = _run({f"{BASE}SPY": _clean("SPY"), f"{BASE}QQQ": _two_pages("QQQ")},
                               ["SPY", "QQQ", "IWM"], should_continue=should_continue)
    assert len(rec.calls) == 2
    assert set(_rows(engine, "SPY")) and _rows(engine, "QQQ") == {}
    assert out["stopped_by_budget"] is True and out["stop_reason"] == "stop_requested"
    assert out["tickers_not_attempted"] == ["QQQ", "IWM"]
    assert out["status"] == "PARTIAL" and _classify_outcome(out)[0] == "PARTIAL"


def test_the_run_budget_stops_before_a_request() -> None:
    out, rec, engine, _ = _run({BASE: []}, ["SPY", "QQQ"], max_seconds=0)
    assert rec.calls == [] and engine.conn.inserts() == []
    assert out["stopped_by_budget"] is True and out["stop_reason"] == "run_budget"
    assert out["tickers_not_attempted"] == ["SPY", "QQQ"]


def test_a_resume_list_runs_only_known_tickers_in_its_order() -> None:
    out, rec, _, _ = _run({f"{BASE}QQQ": _clean("QQQ"), f"{BASE}SPY": _clean("SPY")},
                          ["SPY", "QQQ"], ticker_list=["QQQ", "NOT_A_TICKER"])
    assert [c["url"] for c in rec.calls] == [f"{BASE}QQQ"]
    assert out["total"] == 1 and out["status"] == "SUCCESS"


def test_pull_takes_hermes_stop_and_resume_and_fits_its_deadline() -> None:
    from scripts import hermes_fixers as hf

    params = inspect.signature(mod.PolygonPuller.pull).parameters
    assert "should_continue" in params and "ticker_list" in params
    # Stop starting requests early enough that one in-flight request and the
    # ticker's insert still finish inside Hermes's per-source join.
    assert (mod.RUN_BUDGET_SECONDS + mod.REQUEST_INTERVAL_S + mod.REQUEST_TIMEOUT_S + 5
            <= hf.PER_SOURCE_REPAIR_SECONDS)


def test_hermes_retry_resumes_from_the_backlog_a_stopped_run_left(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import hermes_fixers as hf
    from scripts.hermes_health import OperatorState

    engine = FakeEngine()
    puller = _puller(engine=engine)
    monkeypatch.setattr(hf, "_resolve_puller", lambda name, eng: (puller, "pull", {}))
    state = OperatorState()
    pages = {f"{BASE}SPY": _clean("SPY"), f"{BASE}QQQ": _clean("QQQ"), f"{BASE}IWM": _clean("IWM")}
    rec = Recorder(pages)
    budget = {"left": 1}

    def should_continue() -> bool:
        budget["left"] -= 1
        return budget["left"] >= 0

    with patch.object(mod, "OPTIONS_TICKERS", ["SPY", "QQQ", "IWM"]), \
         patch.object(mod.requests, "get", side_effect=rec), patch.object(mod.time, "sleep"):
        first = hf._retry_source("polygon", engine, state=state, should_continue=should_continue)
        assert first["stopped_by_budget"] is True
        assert state.repair_backlog["polygon"] == ["QQQ", "IWM"]
        second = hf._retry_source("polygon", engine, state=state, should_continue=lambda: True)
    assert [c["url"] for c in rec.calls] == [f"{BASE}SPY", f"{BASE}QQQ", f"{BASE}IWM"]
    assert second["outcome"] == "SUCCESS" and "polygon" not in state.repair_backlog


# ── default-off gate ────────────────────────────────────────────────────


def test_the_gate_is_off_when_unset_and_declared_in_env_example(monkeypatch: pytest.MonkeyPatch) -> None:
    from config import Settings, settings

    assert Settings.model_fields["POLYGON_OPTIONS_PULL_ENABLED"].default is False
    assert settings.POLYGON_OPTIONS_PULL_ENABLED is False
    assert "POLYGON_OPTIONS_PULL_ENABLED=false" in Path(".env.example").read_text()
    monkeypatch.setattr(mod, "options_writes_enabled", REAL_GATE)
    assert mod.options_writes_enabled() is False


def test_hermes_repair_writes_nothing_while_the_gate_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import hermes_fixers as hf

    monkeypatch.setattr(mod, "options_writes_enabled", REAL_GATE)
    engine = FakeEngine()
    monkeypatch.setattr(hf, "_resolve_puller", lambda name, eng: (_puller(engine=engine), "pull", {}))
    get = MagicMock()
    with patch.object(mod.requests, "get", get):
        result = hf._retry_source("polygon", engine, should_continue=lambda: True)
    get.assert_not_called()
    assert engine.conn.inserts() == []
    assert result["outcome"] == "SKIPPED"
    assert "POLYGON_OPTIONS_PULL_ENABLED" in hf.retry_not_fresh_reason(result)
