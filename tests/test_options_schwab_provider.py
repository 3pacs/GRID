"""Schwab Trader API chains as an owner-selected OptionsPuller provider.

Every body here is synthetic, written by hand in the provider's record shape
(field names measured 2026-11-21). Schwab data may not be redistributed, so
no captured response is stored in this repository. No test touches the network
or a real token file.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Self

import pytest

from ingestion import options, options_schwab, schwab_market, schwab_usage

# 16:30 ET on Thursday 2026-11-05.
SESSION_NOW = datetime(2026, 11, 5, 21, 30, tzinfo=timezone.utc)
TODAY = date(2026, 11, 5)
LAST_TRADE_MS = int(datetime(2026, 11, 5, 20, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
QUOTE_MS = LAST_TRADE_MS + 500
NOV06 = int(datetime(2026, 11, 6, tzinfo=timezone.utc).timestamp())
NOV13 = int(datetime(2026, 11, 13, tzinfo=timezone.utc).timestamp())
STRIKES = (65.0, 67.5, 70.0, 72.5, 75.0)


def _occ(root: str, expiry: date, side: str, strike: float) -> str:
    return f"{root:<6}{expiry:%y%m%d}{side}{round(strike * 1000):08d}"


def _entry(root: str, expiry: date, side: str, strike: float, **over: Any) -> dict[str, Any]:
    entry = {
        "putCall": "CALL" if side == "C" else "PUT", "symbol": _occ(root, expiry, side, strike),
        "strikePrice": strike, "bid": 1.0, "ask": 1.1, "last": 1.05, "volatility": 25.0,
        "openInterest": 120, "totalVolume": 30, "quoteTimeInLong": QUOTE_MS, "tradeTimeInLong": LAST_TRADE_MS,
        "inTheMoney": (strike < 70.2) if side == "C" else (strike > 70.2), "nonStandard": False, "mini": False,
    }
    entry.update(over)
    return entry


def _body(symbol: str = "KO", expiries: tuple[date, ...] = (date(2026, 11, 6), date(2026, 11, 13)),
          **over: Any) -> dict[str, Any]:
    root = symbol.replace("/", "")
    maps: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {"callExpDateMap": {}, "putExpDateMap": {}}
    for expiry in expiries:
        key = f"{expiry.isoformat()}:{(expiry - TODAY).days}"
        for name, side in (("callExpDateMap", "C"), ("putExpDateMap", "P")):
            maps[name][key] = {str(k): [_entry(root, expiry, side, k)] for k in STRIKES}
    body: dict[str, Any] = {
        "symbol": symbol, "status": "SUCCESS", "isDelayed": False, "isChainTruncated": False,
        "underlying": {"symbol": symbol, "last": 70.2, "mark": 70.21, "delayed": False,
                       "quoteTime": QUOTE_MS, "tradeTime": LAST_TRADE_MS},
        "underlyingPrice": 70.2, **maps,
    }
    body.update(over)
    return body


class _Market:
    """Stands in for SchwabMarketClient.option_chain: queued bodies or exceptions."""

    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def option_chain(self, symbol: str, **params: Any) -> Any:
        self.calls.append((symbol, params))
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def _client(*answers: Any, **kw: Any) -> tuple[options_schwab.SchwabOptionsClient, _Market, list[float]]:
    market, sleeps = _Market(*answers), []
    clock = iter(range(10_000))
    kw.setdefault("sleep", sleeps.append)
    kw.setdefault("monotonic", lambda: float(next(clock)))
    return options_schwab.SchwabOptionsClient(market, **kw), market, sleeps


def _error(cls: type[schwab_market.SchwabError], status: int | None) -> schwab_market.SchwabError:
    exc = cls("synthetic")
    exc.status = status
    return exc


@pytest.fixture(autouse=True)
def _session_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options, "_utc_now", lambda: SESSION_NOW)


# ── parsing ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("ticker", "symbol"), [("KO", "KO"), ("brk-b", "BRK/B"), (" spy ", "SPY")])
def test_schwab_symbol(ticker: str, symbol: str) -> None:
    assert options_schwab.schwab_symbol(ticker) == symbol


def test_chain_pages_keep_standard_contracts_of_live_expiries() -> None:
    body = _body(expiries=(date(2026, 11, 4), date(2026, 11, 6), date(2026, 11, 13)))
    nov06 = body["callExpDateMap"]["2026-11-06:1"]
    nov06["70.0"][0].update(volatility=-999.0, inTheMoney=None)
    nov06["72.5"][0].update(volatility=0.0)
    nov06["75.0"].append(_entry("KO", date(2026, 11, 6), "C", 75.0, symbol="KO1   261106C00075000"))
    nov06["77.5"] = [_entry("KO", date(2026, 11, 6), "C", 77.5, nonStandard=True)]
    nov06["80.0"] = [_entry("KO", date(2026, 11, 13), "C", 80.0)]   # names another expiry
    nov06["82.5"] = [_entry("KO", date(2026, 11, 6), "P", 82.5)]    # a put in the call map

    chain = options_schwab.schwab_chain_pages([body], "KO", TODAY)

    assert chain is not None and chain["expirations"] == [NOV06, NOV13]  # 11-04 already expired
    assert chain["skipped"] == 4
    calls = {c["strike"]: c for c in chain["pages"][NOV06]["calls"]}
    assert sorted(calls) == list(STRIKES) and len(chain["pages"][NOV06]["puts"]) == 5
    assert calls[65.0]["impliedVolatility"] == pytest.approx(0.25)   # percent -> fraction
    assert calls[70.0]["impliedVolatility"] is None                   # sentinel is missing, not zero
    assert calls[72.5]["impliedVolatility"] is None
    assert calls[70.0]["inTheMoney"] is True                          # computed when the flag is absent
    assert calls[65.0]["contractSymbol"] == "KO261106C00065000"
    assert calls[65.0]["openInterest"] == 120 and calls[65.0]["volume"] == 30
    assert calls[65.0]["clockMetadata"] == {"contract_quote_time_ms": QUOTE_MS,
                                            "contract_last_trade_ms": LAST_TRADE_MS}
    quote = chain["quote"]
    assert quote["regularMarketPrice"] == 70.2 and quote["regularMarketTime"] == LAST_TRADE_MS / 1000.0
    clocks = quote["clockMetadata"]
    assert clocks["schema_version"] == 2 and clocks["provider"] == "schwab"
    assert clocks["bid_ask_updated_at"] is None and clocks["bid_ask_freshness"] == "per_contract_quote_time"
    assert clocks["audience"] == "private" and clocks["redistribution"] == "prohibited"
    assert clocks["provider_delayed"] is False


def test_chain_pages_join_two_windows_and_flag_a_delayed_body() -> None:
    first = _body(expiries=(date(2026, 11, 6),), isDelayed=True)
    second = _body(expiries=(date(2027, 1, 15),))
    chain = options_schwab.schwab_chain_pages([first, second], "KO", TODAY)
    assert chain is not None and len(chain["expirations"]) == 2
    assert chain["quote"]["clockMetadata"]["provider_delayed"] is True


def _duplicate() -> dict[str, Any]:
    body = _body()
    body["callExpDateMap"]["2026-11-06:1"]["70.0"].append(_entry("KO", date(2026, 11, 6), "C", 70.0))
    return body


@pytest.mark.parametrize("bodies", [
    [], ["text"], [_body(status="FAILED")], [_body(symbol="PEP")], [_body(isChainTruncated=True)],
    [_body(underlying=None)], [_body(underlying={"symbol": "PEP", "last": 70.2, "tradeTime": LAST_TRADE_MS})],
    [_body(callExpDateMap=None, putExpDateMap=None)], [_body(callExpDateMap=[])],
    [_body(callExpDateMap={"next friday": {}})], [_body(callExpDateMap={"2026-11-06:1": {"70.0": "x"}})],
    [_duplicate()], [_body(), _body()],
], ids=["none", "not-a-mapping", "failed", "other-symbol", "truncated", "no-underlying", "other-underlying",
        "no-maps", "map-not-mapping", "bad-expiry-key", "bad-strike-bucket", "duplicate", "repeat-across-windows"])
def test_chain_pages_refuse_incomplete_or_ambiguous_bodies(bodies: list[Any]) -> None:
    assert options_schwab.schwab_chain_pages(bodies, "KO", TODAY) is None


def test_missing_underlying_clock_leaves_the_capture_undated() -> None:
    body = _body()
    body["underlying"]["tradeTime"] = None
    chain = options_schwab.schwab_chain_pages([body], "KO", TODAY)
    assert chain is not None and chain["quote"]["regularMarketTime"] is None
    page = {"quote": chain["quote"]}
    assert options._page_quote_time(page, TODAY) is None  # the puller refuses it as undated


# ── client ───────────────────────────────────────────────────────────────


def test_one_request_when_the_first_window_holds_enough_expiries() -> None:
    client, market, sleeps = _client(_body(), max_expirations=2)
    first = client.get_options("KO")
    assert first["expirations"] == [NOV06, NOV13] and len(first["calls"]) == 5
    assert client.get_options("KO", NOV13)["puts"] and client.get_options("KO", 1) is None
    assert client.get_options("PEP", NOV13) is None  # another ticker is never served from this download
    assert market.calls == [("KO", {"contractType": "ALL", "includeUnderlyingQuote": "true", "strategy": "SINGLE",
                                    "fromDate": "2026-11-05", "toDate": "2026-12-16"})]
    assert sleeps == [] and client.requests_made == 1


def test_second_window_only_when_the_first_is_short_and_requests_are_paced() -> None:
    client, market, sleeps = _client(_body(), _body(expiries=(date(2027, 1, 15),)))
    first = client.get_options("KO")
    assert len(first["expirations"]) == 3
    assert [c[1]["fromDate"] for c in market.calls] == ["2026-11-05", "2026-12-17"]
    assert market.calls[1][1]["toDate"] == "2027-12-10"
    # The fake clock advances one second per reading, so one second remains of the 2 s spacing.
    assert sleeps == [pytest.approx(options_schwab.MIN_REQUEST_INTERVAL_SECONDS - 1.0)]


def test_brk_b_is_requested_by_its_schwab_symbol() -> None:
    client, market, _sleeps = _client(_body("BRK/B"), max_expirations=2)
    first = client.get_options("BRK-B")
    assert market.calls[0][0] == "BRK/B"
    assert first["calls"][0]["contractSymbol"].startswith("BRKB261106C")


def test_no_live_contract_is_no_chain() -> None:
    client, market, _sleeps = _client(_body(expiries=(date(2026, 11, 4),)), _body(expiries=(date(2026, 11, 3),)))
    assert client.get_options("KO") is options.NO_CHAIN
    assert len(market.calls) == 2


def test_refused_body_is_a_failed_fetch_without_a_second_request() -> None:
    client, market, _sleeps = _client(_body(status="FAILED"))
    assert client.get_options("KO") is None and len(market.calls) == 1
    assert client.get_options("KO", NOV06) is None


def test_ticker_outside_the_core_is_never_requested() -> None:
    client, market, _sleeps = _client()
    assert "NUOX" not in options.EQUITY_TICKERS
    assert client.get_options("NUOX") is None and market.calls == []


@pytest.mark.parametrize(("error", "message", "status"), [
    (_error(schwab_market.SchwabTransientError, 429), "schwab rate limited (HTTP 429)", 429),
    (_error(schwab_market.SchwabTransientError, 503), "schwab unavailable (HTTP 503)", 503),
    (_error(schwab_market.SchwabTransientError, None), "schwab unavailable", 0),
    (_error(schwab_market.SchwabAuthError, None),
     "schwab not authorised (operator must re-run the login helper) (HTTP 401)", 401),
], ids=["429", "503", "network", "auth"])
def test_a_refusal_or_outage_stops_the_run_and_is_never_retried(
        error: schwab_market.SchwabError, message: str, status: int) -> None:
    client, market, _sleeps = _client(error)
    with pytest.raises(options.OptionsSourceStopped) as caught:
        client.get_options("KO")
    assert str(caught.value) == message and caught.value.http_status == status
    assert caught.value.provider == "schwab" and caught.value.reason == "source_stopped"
    for ticker, expiry in (("PEP", None), ("KO", NOV06)):
        with pytest.raises(options.OptionsSourceStopped):
            client.get_options(ticker, expiry)
    assert len(market.calls) == 1


def test_a_bad_request_fails_one_ticker_without_stopping_the_run() -> None:
    client, market, _sleeps = _client(_error(schwab_market.SchwabError, 400), _body(), max_expirations=2)
    assert client.get_options("KO") is None
    assert client.get_options("PEP") is None  # the queued body is for KO: refused, and no latch either way
    assert len(market.calls) == 2


def test_default_client_never_retries_inside_the_shared_allowance(monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[dict[str, Any]] = []
    monkeypatch.setattr(schwab_market, "SchwabMarketClient", lambda **kw: built.append(kw) or _Market(_body()))
    client = options_schwab.SchwabOptionsClient(sleep=lambda _s: None, max_expirations=2)
    assert client.get_options("KO")
    assert built == [{"client": "grid-options", "max_retries": 0}]


# ── the shared call ledger holds a run back ──────────────────────────────


def test_capture_hold_reads_the_shared_ledger(tmp_path: Path) -> None:
    now = SESSION_NOW.timestamp()
    def hold() -> str | None:
        return options_schwab.schwab_capture_hold(now, usage_dir=tmp_path, configured=lambda: True)

    assert options_schwab.schwab_capture_hold(now, usage_dir=tmp_path, configured=lambda: False) == "schwab_not_configured"
    assert hold() is None  # no ledger yet
    schwab_usage.record(tmp_path, "gex-watch", "GET", "/quotes", 200, now - 5)
    schwab_usage.record(tmp_path, "gex-watch", "GET", "/quotes", 429, now - options_schwab.RATE_LIMIT_HOLD_SECONDS - 5)
    schwab_usage.record(tmp_path, "grid-options", "GET", "/chains", 200, now - options_schwab.MIN_RUN_GAP_SECONDS - 5)
    assert hold() is None  # another client's ordinary calls, an old refusal and an old capture do not hold
    schwab_usage.record(tmp_path, "grid-options", "GET", "/chains", 200, now - 60)
    assert hold() == "schwab_recent_capture"
    schwab_usage.record(tmp_path, "gex-watch", "GET", "/quotes", 429, now - 30)
    assert hold() == "schwab_recent_rate_limit"  # anyone's refusal outranks our own pacing


# ── end to end through the capture guards ────────────────────────────────


class _Result:
    def __init__(self, row: tuple) -> None:
        self.row = row
        self.rowcount = 1

    def fetchone(self) -> tuple:
        return self.row


class _DB:
    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.batches: list[dict] = []
        self.signals: list[dict] = []

    def begin(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: Any, params: dict | None = None) -> _Result:
        sql = str(statement)
        values = params or {}
        if "txid_current()" in sql:
            return _Result((1, SESSION_NOW))
        if "SELECT clock_timestamp()" in sql:
            return _Result((SESSION_NOW + timedelta(minutes=1),))
        if "MAX(capture_ordinal)" in sql:
            return _Result((None,))
        if "INSERT INTO options_capture_batches" in sql:
            self.batches.append(dict(values))
        elif "INSERT INTO options_snapshots_all" in sql:
            self.rows.append(dict(values))
        elif "INSERT INTO options_daily_signals" in sql:
            self.signals.append(dict(values))
        return _Result((None,))


def _puller(monkeypatch: pytest.MonkeyPatch) -> Any:
    puller = options.OptionsPuller.__new__(options.OptionsPuller)
    puller.engine = _DB()
    monkeypatch.setattr(puller, "_push_to_resolved", lambda *_a, **_k: 0)
    monkeypatch.setattr(options.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(options, "_NO_OPTIONS_UNTIL", {})
    return puller


def test_pull_ticker_publishes_a_schwab_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    puller = _puller(monkeypatch)
    client, market, _sleeps = _client(_body(), max_expirations=2)
    puller._yahoo = client

    result = puller._pull_ticker("KO", "2026-11-05")

    assert result["status"] == "SUCCESS", result
    assert len(market.calls) == 1
    db = puller.engine
    assert len(db.batches) == 1 and db.batches[0]["row_count"] == 20
    assert db.batches[0]["spot_provider"] == "schwab" and db.batches[0]["spot"] == pytest.approx(70.2)
    batch_clocks = json.loads(db.batches[0]["clock_metadata"])
    assert batch_clocks["provider"] == "schwab" and batch_clocks["redistribution"] == "prohibited"
    assert {r["expiry"] for r in db.rows} == {"2026-11-06", "2026-11-13"}
    assert all(r["provider_regular_market_at"] == datetime.fromtimestamp(LAST_TRADE_MS / 1000.0, timezone.utc)
               for r in db.rows)
    assert all(set(json.loads(r["clock_metadata"])) == {"contract_quote_time_ms", "contract_last_trade_ms"}
               for r in db.rows)
    assert db.signals


def test_previous_session_underlying_trade_is_refused_as_undated(monkeypatch: pytest.MonkeyPatch) -> None:
    puller = _puller(monkeypatch)
    body = _body()
    body["underlying"]["tradeTime"] = LAST_TRADE_MS - 24 * 3600 * 1000
    puller._yahoo, _market, _sleeps = _client(body, max_expirations=2)

    result = puller._pull_ticker("KO", "2026-11-05")

    assert result["status"] == "FAILED" and "1 undated quote(s)" in result["error"]
    assert puller.engine.batches == [] and puller.engine.rows == []


def _schwab_run(monkeypatch: pytest.MonkeyPatch, tickers: list[str], catalysts: list[str], *answers: Any,
                hold: str | None = None) -> tuple[Any, _Market, Any]:
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", "schwab")
    monkeypatch.setattr(options, "EQUITY_TICKERS", tickers)
    monkeypatch.setattr(options, "catalyst_options_universe", lambda *_a, **_kw: catalysts)
    monkeypatch.setattr(options_schwab, "schwab_capture_hold", lambda: hold)
    client, market, _sleeps = _client(*answers, max_expirations=2)
    monkeypatch.setattr(options_schwab, "SchwabOptionsClient", lambda: client)
    for name in ("YahooOptionsClient", "CboeFirstOptionsClient"):
        monkeypatch.setattr(options, name, lambda name=name: pytest.fail(f"{name} must not be built under schwab"))
    puller = _puller(monkeypatch)
    monkeypatch.setattr(puller, "_mark_catalog_pulled",
                        lambda **_kw: pytest.fail("a core-only run must not publish source freshness"))
    return puller, market, puller.pull_all()


def test_primary_provider_knob_accepts_schwab(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", " Schwab ")
    assert options.options_primary_provider() == "schwab"
    monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER")
    assert options.options_primary_provider() == "cboe"  # the default is unchanged


def test_default_run_reads_the_core_and_never_requests_catalyst_names(monkeypatch: pytest.MonkeyPatch) -> None:
    puller, market, results = _schwab_run(monkeypatch, ["KO"], ["NUOX", "OLMA"], _body())

    assert [r["status"] for r in results] == ["SUCCESS", "SKIPPED", "SKIPPED"]
    assert results[1] == {"ticker": "NUOX", "status": "SKIPPED", "rows_inserted": 0,
                          "reason": "provider_core_list_only", "provider": "schwab"}
    assert [c[0] for c in market.calls] == ["KO"]
    assert results.summary["status"] == "PARTIAL"
    assert "2 tickers not captured (NUOX, OLMA)" in results.summary["error"]
    assert results.summary["tickers_no_options"] == results.summary["tickers_no_chain"] == 0
    assert options._NO_OPTIONS_UNTIL == {}  # not requested is not "has no options"
    assert len(puller.engine.batches) == 1


def test_rate_limit_stops_the_run_and_keeps_the_completed_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    puller, market, results = _schwab_run(
        monkeypatch, ["KO", "PEP", "PG"], [], _body(), _error(schwab_market.SchwabTransientError, 429))

    assert results[0]["status"] == "SUCCESS"
    assert results[1] == {"ticker": "PEP", "status": "FAILED", "rows_inserted": 0, "reason": "source_stopped",
                          "error": "schwab rate limited (HTTP 429)", "provider": "schwab", "http_status": 429}
    assert results[2] == {"ticker": "PG", "status": "SKIPPED", "rows_inserted": 0,
                          "reason": "source_stopped_before_request", "provider": "schwab",
                          "blocked_by_http_status": 429}
    assert len(market.calls) == 2  # PG is never requested
    assert results.summary["status"] == "PARTIAL"
    assert results.summary["error"] == "schwab rate limited (HTTP 429); remaining tickers not requested"
    assert len(puller.engine.batches) == 1 and options._NO_OPTIONS_UNTIL == {}


def test_a_held_run_makes_no_request_and_builds_no_client(monkeypatch: pytest.MonkeyPatch) -> None:
    _puller, market, results = _schwab_run(monkeypatch, ["KO", "PEP"], ["NUOX"], hold="schwab_recent_rate_limit")

    assert market.calls == []
    assert all(r == {"ticker": r["ticker"], "status": "SKIPPED", "rows_inserted": 0,
                     "reason": "schwab_recent_rate_limit", "provider": "schwab"} for r in results)
    assert [r["ticker"] for r in results] == ["KO", "PEP", "NUOX"]
    assert results.summary["status"] == "SKIPPED" and "schwab_recent_rate_limit" in results.summary["reason"]
