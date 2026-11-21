"""Schwab Trader API chains as an owner-selected OptionsPuller provider.

Every body here is synthetic, written by hand in the provider's record shape
(field names measured 2026-11-21). Schwab data may not be redistributed, so
no captured response is stored in this repository. No test touches the network
or a real token file.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Self

import pytest

from ingestion import options, options_schwab, schwab_market, schwab_usage
from tests.test_schwab_market import (
    FAKE_KEY,
    FAKE_SECRET,
    NOW,
    FakeResponse,
    FakeSession,
    write_token_file,
)

# 15:30 ET on Thursday 2026-11-05, inside the regular session (cash close 21:00Z).
SESSION_NOW = datetime(2026, 11, 5, 20, 30, tzinfo=timezone.utc)
TODAY = date(2026, 11, 5)
LAST_TRADE_MS = int(datetime(2026, 11, 5, 20, 29, 59, tzinfo=timezone.utc).timestamp() * 1000)
QUOTE_MS = LAST_TRADE_MS + 500
NOV06 = int(datetime(2026, 11, 6, tzinfo=timezone.utc).timestamp())
NOV13 = int(datetime(2026, 11, 13, tzinfo=timezone.utc).timestamp())
STRIKES = (65.0, 67.5, 70.0, 72.5, 75.0)
_SCHWAB_CLIENT = options_schwab.SchwabOptionsClient   # the real class, whatever a run helper patches


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
    return _SCHWAB_CLIENT(market, **kw), market, sleeps


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
    nov06["85.0"] = [_entry("KO", date(2026, 11, 6), "C", 87.5)]    # sits in another strike's bucket
    assert options_schwab.schwab_chain_pages([body], "KO", TODAY)["skipped"] == 5
    del nov06["85.0"]
    chain = options_schwab.schwab_chain_pages([body], "KO", TODAY)
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
    assert clocks["provider_delayed"] is False and clocks["underlying_trade_in_regular_session"] is True


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
    [_body(callExpDateMap={"2026-11-06:1": {"seventy": []}})],
    [_duplicate()], [_body(), _body()],
], ids=["none", "not-a-mapping", "failed", "other-symbol", "truncated", "no-underlying", "other-underlying",
        "no-maps", "map-not-mapping", "bad-expiry-key", "bad-strike-bucket", "bad-strike-key", "duplicate",
        "repeat-across-windows"])
def test_chain_pages_refuse_incomplete_or_ambiguous_bodies(bodies: list[Any]) -> None:
    assert options_schwab.schwab_chain_pages(bodies, "KO", TODAY) is None


def test_missing_underlying_clock_leaves_the_capture_undated() -> None:
    body = _body()
    body["underlying"]["tradeTime"] = None
    chain = options_schwab.schwab_chain_pages([body], "KO", TODAY)
    assert chain is not None and chain["quote"]["regularMarketTime"] is None
    page = {"quote": chain["quote"]}
    assert options._page_quote_time(page, TODAY) is None  # the puller refuses it as undated


@pytest.mark.parametrize(("when", "in_session"), [
    (datetime(2026, 11, 5, 14, 29, 59, tzinfo=timezone.utc), False),   # 09:29:59 New York: pre-market
    (datetime(2026, 11, 5, 14, 30, 0, tzinfo=timezone.utc), True),     # the open
    (datetime(2026, 11, 5, 21, 0, 0, tzinfo=timezone.utc), True),      # the cash close
    (datetime(2026, 11, 5, 21, 0, 1, tzinfo=timezone.utc), False),     # one second into extended hours
    (datetime(2026, 11, 5, 22, 41, 7, tzinfo=timezone.utc), False),    # an evening print
    (datetime(2026, 11, 4, 20, 59, 59, tzinfo=timezone.utc), False),   # the previous session
], ids=["pre-market", "open", "close", "after-close", "evening", "previous-session"])
def test_only_a_regular_session_trade_dates_the_spot_price(when: datetime, in_session: bool) -> None:
    body = _body()
    body["underlying"]["tradeTime"] = int(when.timestamp() * 1000)
    quote = options_schwab.schwab_chain_pages([body], "KO", TODAY)["quote"]
    assert (quote["regularMarketTime"] is not None) is in_session
    assert quote["clockMetadata"]["underlying_trade_in_regular_session"] is in_session
    assert quote["clockMetadata"]["underlying_last_trade_raw"] == str(int(when.timestamp() * 1000))  # kept either way


def test_half_day_session_ends_at_the_early_close() -> None:
    day = date(2026, 11, 27)   # 13:00 New York close
    opened, closed = options_schwab.regular_session(day)
    assert (opened, closed) == (datetime(2026, 11, 27, 14, 30, tzinfo=timezone.utc),
                                datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc))


# ── client ───────────────────────────────────────────────────────────────


def test_one_request_when_the_first_window_holds_enough_expiries() -> None:
    client, market, sleeps = _client(_body(), max_expirations=2)
    first = client.get_options("KO")
    assert first["expirations"] == [NOV06, NOV13] and len(first["calls"]) == 5
    assert client.get_options("KO", NOV13)["puts"] and client.get_options("KO", 1) is None
    assert client.get_options("PEP", NOV13) is None  # another ticker is never served from this download
    assert market.calls == [("KO", {"contractType": "ALL", "includeUnderlyingQuote": "true", "strategy": "SINGLE",
                                    "fromDate": "2026-11-05", "toDate": "2026-12-16"})]
    assert client.client_name == "grid-options/options_puller"
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
    assert built == [{"client": "grid-options/options_puller", "max_retries": 0}]
    built.clear()
    gem = options_schwab.SchwabOptionsClient(sleep=lambda _s: None, max_expirations=2, writer="gem_daily")
    assert gem.get_options("KO") and built == [{"client": "grid-options/gem_daily", "max_retries": 0}]


def _real_client(tmp_path: Path, session: FakeSession) -> options_schwab.SchwabOptionsClient:
    token_file = tmp_path / "tokens.json"
    write_token_file(token_file, access_expires_at=NOW + 1500, refresh_expires_at=NOW + 5 * 86400)
    market = schwab_market.SchwabMarketClient(
        app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=token_file, session=session,
        sleep=lambda _s: None, now=lambda: NOW, client="grid-options/options_puller", max_retries=0)
    return options_schwab.SchwabOptionsClient(market, sleep=lambda _s: None)


@pytest.mark.parametrize(("status", "calls"), [(401, ["GET", "POST", "GET"]), (403, ["GET"])])
def test_a_lasting_refusal_through_the_real_client_stops_the_run(tmp_path: Path, status: int, calls: list[str]) -> None:
    # The real client raises a plain SchwabError for a 401 that survives its one refresh and for any
    # 403 (review of #1299, item 2). With a dead login every ticker used to refresh the shared token file.
    session = FakeSession()
    session.get_queue += [FakeResponse(status)] * 10
    session.post_queue += [FakeResponse(200, {"access_token": f"new{i}", "expires_in": 1800}) for i in range(10)]
    client = _real_client(tmp_path, session)

    with pytest.raises(options.OptionsSourceStopped) as caught:
        client.get_options("SPY")
    assert str(caught.value) == f"schwab refused access (HTTP {status})" and caught.value.http_status == status
    for ticker in ("QQQ", "IWM"):
        with pytest.raises(options.OptionsSourceStopped):
            client.get_options(ticker)
    assert [c["method"] for c in session.calls] == calls   # nothing after the first ticker


def test_another_4xx_through_the_real_client_fails_one_ticker_only(tmp_path: Path) -> None:
    session = FakeSession()
    session.get_queue += [FakeResponse(400), FakeResponse(200, _body("PEP"))]
    client = options_schwab.SchwabOptionsClient(
        _real_client(tmp_path, session)._market, sleep=lambda _s: None, max_expirations=2)
    assert client.get_options("KO") is None
    assert client.get_options("PEP")["calls"]


# ── the shared call ledger holds a run back ──────────────────────────────


def test_capture_hold_reads_the_shared_ledger(tmp_path: Path) -> None:
    now = SESSION_NOW.timestamp()
    def hold() -> str | None:
        return options_schwab.schwab_capture_hold(now, usage_dir=tmp_path, configured=lambda: True)

    assert options_schwab.schwab_capture_hold(now, usage_dir=tmp_path, configured=lambda: False) == "schwab_not_configured"
    assert hold() is None  # no ledger yet
    schwab_usage.record(tmp_path, "gex-watch", "GET", "/quotes", 200, now - 5)
    schwab_usage.record(tmp_path, "gex-watch", "GET", "/quotes", 429, now - options_schwab.RATE_LIMIT_HOLD_SECONDS - 5)
    mine = "grid-options/options_puller"
    schwab_usage.record(tmp_path, mine, "GET", "/chains", 200, now - options_schwab.MIN_RUN_GAP_SECONDS - 5)
    schwab_usage.record(tmp_path, "grid-options-shape-probe", "GET", "/chains", 200, now - 20)
    assert hold() is None  # another client's ordinary calls, an old refusal and an old capture do not hold
    schwab_usage.record(tmp_path, mine, "GET", "/chains", 200, now - 60)
    assert hold() == "schwab_recent_capture"
    # The gap is per writer: the scheduler's pull does not hold GEM or the oracle pass, and each holds itself.
    for writer in ("gem_daily", "oracle"):
        assert options_schwab.schwab_capture_hold(now, writer=writer, usage_dir=tmp_path, configured=lambda: True) is None
    schwab_usage.record(tmp_path, "grid-options/gem_daily", "GET", "/chains", 200, now - 10)
    assert options_schwab.schwab_capture_hold(now, writer="gem_daily", usage_dir=tmp_path, configured=lambda: True) == "schwab_recent_capture"
    assert options_schwab.schwab_capture_hold(now, writer="oracle", usage_dir=tmp_path, configured=lambda: True) is None
    schwab_usage.record(tmp_path, "gex-watch", "GET", "/quotes", 429, now - 30)
    assert hold() == "schwab_recent_rate_limit"  # anyone's refusal outranks our own pacing
    assert options_schwab.schwab_capture_hold(now, writer="oracle", usage_dir=tmp_path, configured=lambda: True) == "schwab_recent_rate_limit"


def test_core_is_not_requested_after_the_cash_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab, "schwab_capture_hold", lambda *_a, **_kw: None)
    block = options_schwab.schwab_core_block
    assert block(datetime(2026, 11, 5, 20, 59, 59, tzinfo=timezone.utc)) is None
    assert block(datetime(2026, 11, 5, 21, 0, 0, tzinfo=timezone.utc)) is None
    assert block(datetime(2026, 11, 5, 21, 0, 1, tzinfo=timezone.utc)) == "post-session"
    assert block(datetime(2026, 11, 27, 18, 30, tzinfo=timezone.utc)) == "post-session"   # half day
    monkeypatch.setattr(options_schwab, "schwab_capture_hold", lambda _t, *, writer=None: f"held:{writer}")
    assert block(SESSION_NOW, writer="gem_daily") == "held:gem_daily"


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


def test_a_schwab_capture_is_never_written_to_resolved_series(monkeypatch: pytest.MonkeyPatch) -> None:
    # resolved_series feeds every point-in-time reader with no viewer filter
    # (store/options_provenance.py), so the batch and signals are kept for
    # admin reads but the options features are not pushed.
    puller = _puller(monkeypatch)
    pushed: list[Any] = []
    monkeypatch.setattr(puller, "_push_to_resolved", lambda *a, **_k: pushed.append(a) or 0)
    puller._yahoo, _market, _sleeps = _client(_body(), max_expirations=2)

    result = puller._pull_ticker("KO", "2026-11-05")

    assert result["status"] == "SUCCESS", result
    assert puller.engine.batches and puller.engine.signals
    assert pushed == []


def test_previous_session_underlying_trade_is_refused_as_undated(monkeypatch: pytest.MonkeyPatch) -> None:
    puller = _puller(monkeypatch)
    body = _body()
    body["underlying"]["tradeTime"] = LAST_TRADE_MS - 24 * 3600 * 1000
    puller._yahoo, _market, _sleeps = _client(body, max_expirations=2)

    result = puller._pull_ticker("KO", "2026-11-05")

    assert result["status"] == "FAILED" and "1 undated quote(s)" in result["error"]
    assert puller.engine.batches == [] and puller.engine.rows == []


class _FakeYahoo:
    """Yahoo stand-in: a dated two-expiry chain for any ticker except the ones it has no chain for."""

    provider = "yahoo"
    remote_expiry_pages = True
    last_no_chain = False

    def __init__(self, *, available: bool = True, no_chain: tuple[str, ...] = (),
                 limited_at_request: int | None = None, init_status: int | None = 200) -> None:
        self.is_available = available
        self.init_status = init_status
        self.no_chain = set(no_chain)
        self.limited_at_request = limited_at_request   # 1-based request number answered 429
        self.last_status: int | None = None
        self.calls: list[tuple[str, int | None]] = []

    def get_options(self, ticker: str, expiry_ts: int | None = None) -> Any:
        self.calls.append((ticker, expiry_ts))
        if self.limited_at_request is not None and len(self.calls) >= self.limited_at_request:
            self.last_status = 429
            return None
        self.last_status = 200
        if ticker in self.no_chain:
            return options.NO_CHAIN
        chain = options_schwab.schwab_chain_pages([_body(ticker)], ticker, TODAY)
        page = chain["pages"][expiry_ts if expiry_ts is not None else chain["expirations"][0]]
        return {"expirations": list(chain["expirations"]), "strikes": list(STRIKES),
                "calls": list(page["calls"]), "puts": list(page["puts"]),
                "quote": {"regularMarketPrice": 70.2, "regularMarketTime": LAST_TRADE_MS / 1000.0}}


def _schwab_run(monkeypatch: pytest.MonkeyPatch, core: list[str], catalysts: list[str], *answers: Any,
                block: str | None = None, yahoo: _FakeYahoo | None = None, publish: bool = False,
                **pull_kw: Any) -> tuple[Any, _Market, _FakeYahoo, Any]:
    if "GRID_OPTIONS_YAHOO_HOLD_FILE" not in os.environ:   # never the operator's real marker
        monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(Path(tempfile.mkdtemp()) / "hold.json"))
    monkeypatch.setattr(options_schwab.time, "sleep", lambda _s: None)
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", "schwab")
    monkeypatch.setenv("GRID_OPTIONS_SCHWAB_READERS", "admin-only")
    monkeypatch.setattr(options, "EQUITY_TICKERS", core)
    monkeypatch.setattr(options, "catalyst_options_universe", lambda *_a, **_kw: catalysts)
    writers: list[str | None] = []
    monkeypatch.setattr(options_schwab, "schwab_core_block",
                        lambda _now, *, writer=None: writers.append(writer) or block)
    schwab, market, _sleeps = _client(*answers, max_expirations=2)
    monkeypatch.setattr(options_schwab, "SchwabOptionsClient", lambda **_kw: schwab)
    yahoo = yahoo if yahoo is not None else _FakeYahoo()
    monkeypatch.setattr(options, "YahooOptionsClient", lambda: yahoo)
    monkeypatch.setattr(options, "CboeFirstOptionsClient",
                        lambda: pytest.fail("the Cboe client must not be built under schwab"))
    puller = _puller(monkeypatch)
    published: list[bool] = []
    monkeypatch.setattr(puller, "_mark_catalog_pulled", lambda **_kw: (
        published.append(True) or True) if publish else pytest.fail("this run must not publish source freshness"))
    results = puller.pull_all(**pull_kw)
    puller.writers, puller.published = writers, published
    return puller, market, yahoo, results


def test_schwab_is_selected_only_with_the_reader_acknowledgement(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", " Schwab ")
    monkeypatch.delenv("GRID_OPTIONS_SCHWAB_READERS", raising=False)
    assert options.options_primary_provider() == "cboe"   # Schwab rows would reach non-admin readers
    monkeypatch.setenv("GRID_OPTIONS_SCHWAB_READERS", "yes")
    assert options.options_primary_provider() == "cboe"
    monkeypatch.setenv("GRID_OPTIONS_SCHWAB_READERS", " Admin-Only ")
    assert options.options_primary_provider() == "schwab"
    monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER")
    assert options.options_primary_provider() == "cboe"  # the default is unchanged


def test_without_the_acknowledgement_a_schwab_setting_runs_the_default_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", "schwab")
    monkeypatch.delenv("GRID_OPTIONS_SCHWAB_READERS", raising=False)
    built: list[str] = []
    monkeypatch.setattr(options, "CboeFirstOptionsClient", lambda: built.append("cboe") or _FakeYahoo())
    monkeypatch.setattr(options_schwab, "SchwabFirstOptionsClient",
                        lambda **_kw: pytest.fail("Schwab must not be used without the acknowledgement"))
    monkeypatch.setattr(options_schwab, "schwab_core_block", lambda *_a, **_kw: pytest.fail("no ledger read either"))
    puller = _puller(monkeypatch)
    results = puller.pull_all(tickers=["KO"], max_expirations=6)
    assert built == ["cboe"] and results[0]["status"] == "SUCCESS"
    assert puller.engine.batches[0]["spot_provider"] == "yahoo"   # the stand-in's label, not schwab


def test_core_goes_to_schwab_and_every_other_name_to_yahoo_and_the_run_is_complete(
        monkeypatch: pytest.MonkeyPatch) -> None:
    puller, market, yahoo, results = _schwab_run(
        monkeypatch, ["KO"], ["NUOX", "OLMA"], _body(), yahoo=_FakeYahoo(no_chain=("NUOX",)), publish=True)

    assert [(r["ticker"], r["status"]) for r in results] == [("KO", "SUCCESS"), ("NUOX", "NO_NEW_DATA"), ("OLMA", "SUCCESS")]
    assert [c[0] for c in market.calls] == ["KO"]                          # catalyst names never reach Schwab
    assert {t for t, _e in yahoo.calls} == {"NUOX", "OLMA"}                # and the core never reaches Yahoo
    assert puller._yahoo.provider_by_ticker == {"KO": "schwab", "NUOX": "yahoo", "OLMA": "yahoo"}
    by_ticker = {b["ticker"]: b["spot_provider"] for b in puller.engine.batches}
    assert by_ticker == {"KO": "schwab", "OLMA": "yahoo"}                  # one provider per batch
    assert results.summary["status"] == "SUCCESS" and results.summary["tickers_no_options"] == 1
    assert puller.published == [True]                                       # freshness moves again
    assert puller.writers == ["options_puller"]


def test_yahoo_being_unavailable_fails_only_the_names_it_serves(monkeypatch: pytest.MonkeyPatch) -> None:
    _puller, market, yahoo, results = _schwab_run(
        monkeypatch, ["KO"], ["OLMA"], _body(), yahoo=_FakeYahoo(available=False))
    assert [(r["ticker"], r["status"]) for r in results] == [("KO", "SUCCESS"), ("OLMA", "FAILED")]
    assert yahoo.calls == [] and len(market.calls) == 1
    assert results.summary["status"] == "PARTIAL" and "1 tickers not captured (OLMA)" in results.summary["error"]


def test_rate_limit_stops_the_run_for_every_remaining_name(monkeypatch: pytest.MonkeyPatch) -> None:
    puller, market, yahoo, results = _schwab_run(
        monkeypatch, ["KO", "PEP", "PG"], ["OLMA"], _body(), _error(schwab_market.SchwabTransientError, 429))

    assert results[0]["status"] == "SUCCESS"
    assert results[1] == {"ticker": "PEP", "status": "FAILED", "rows_inserted": 0, "reason": "source_stopped",
                          "error": "schwab rate limited (HTTP 429)", "provider": "schwab", "http_status": 429}
    assert results[2] == {"ticker": "PG", "status": "SKIPPED", "rows_inserted": 0,
                          "reason": "source_stopped_before_request", "provider": "schwab",
                          "blocked_by_http_status": 429}
    assert results[3]["ticker"] == "OLMA" and results[3]["reason"] == "source_stopped_before_request"
    assert len(market.calls) == 2 and yahoo.calls == []   # PG and the Yahoo name are never requested
    assert results.summary["status"] == "PARTIAL"
    assert results.summary["error"] == "schwab rate limited (HTTP 429); remaining tickers not requested"
    assert len(puller.engine.batches) == 1 and options._NO_OPTIONS_UNTIL == {}


@pytest.mark.parametrize("block", ["schwab_recent_rate_limit", "schwab_recent_capture"])
def test_a_blocked_core_makes_no_schwab_request_while_other_names_are_still_read(
        monkeypatch: pytest.MonkeyPatch, block: str) -> None:
    _puller, market, yahoo, results = _schwab_run(monkeypatch, ["KO", "PEP"], ["OLMA"], block=block)

    assert market.calls == []
    assert results[:2] == [{"ticker": t, "status": "SKIPPED", "rows_inserted": 0, "reason": block,
                            "provider": "schwab"} for t in ("KO", "PEP")]
    assert results[2]["status"] == "SUCCESS" and {t for t, _e in yahoo.calls} == {"OLMA"}
    assert results.summary["status"] == "PARTIAL" and "2 tickers not captured (KO, PEP)" in results.summary["error"]
    assert options._NO_OPTIONS_UNTIL == {}   # not requested is not "has no options"


def test_a_run_without_core_names_never_consults_the_schwab_ledger(monkeypatch: pytest.MonkeyPatch) -> None:
    # GEM passes its own tickers and its own writer label; none is a core name.
    puller, market, _yahoo, results = _schwab_run(
        monkeypatch, ["KO"], [], block="schwab_recent_rate_limit",
        tickers=["OLMA", "NUOX"], max_expirations=6, capture_source="gem_daily")
    assert puller.writers == [] and market.calls == []
    assert [r["status"] for r in results] == ["SUCCESS", "SUCCESS"]
    assert {b["spot_provider"] for b in puller.engine.batches} == {"yahoo"}
    assert {b["source"] for b in puller.engine.batches} == {"gem_daily"}


def test_the_writer_label_reaches_the_ledger_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    puller, _market, _yahoo, _results = _schwab_run(
        monkeypatch, ["KO"], [], _body(), tickers=["KO"], capture_source="daily_scheduler")
    assert puller.writers == ["daily_scheduler"]


# ── the Yahoo names in schwab mode (re-review of #1299) ──────────────────


def _paced(yahoo: _FakeYahoo, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
           step: float = 0.25) -> tuple[options_schwab.SchwabFirstOptionsClient, list[float]]:
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(tmp_path / "state" / "hold.json"))
    monkeypatch.setattr(options, "YahooOptionsClient", lambda: yahoo)
    sleeps: list[float] = []
    ticks = iter(i * step for i in range(100_000))
    schwab, _market, _s = _client()
    client = options_schwab.SchwabFirstOptionsClient(
        schwab, sleep=sleeps.append, monotonic=lambda: next(ticks), now=lambda: SESSION_NOW.timestamp())
    return client, sleeps


def test_yahoo_requests_are_spaced_at_least_a_second_apart(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    client, sleeps = _paced(_FakeYahoo(), monkeypatch, tmp_path)
    assert client.get_options("OLMA") and client.get_options("OLMA", NOV13) and client.get_options("NUOX")
    # The fake clock moves 0.25 s per reading, so 0.75 s remains before each later request.
    assert sleeps == [pytest.approx(0.75), pytest.approx(0.75)] and client.yahoo_requests == 3
    assert options_schwab.YAHOO_MIN_REQUEST_INTERVAL_SECONDS >= 1.0


def test_a_429_latches_for_the_run_and_writes_the_marker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    yahoo = _FakeYahoo(limited_at_request=2)
    client, _sleeps = _paced(yahoo, monkeypatch, tmp_path)

    assert client.get_options("OLMA")                       # first page served
    assert client.get_options("OLMA", NOV13) is None        # 429 on its second expiry
    assert client.get_options("OLMA", NOV06) is None        # the rest of that ticker: no request
    with pytest.raises(options.OptionsSourceStopped) as stopped:
        client.get_options("NUOX")                          # the next non-core name ends the run
    assert str(stopped.value) == "yahoo rate limited (HTTP 429)" and stopped.value.provider == "yahoo"
    assert len(yahoo.calls) == 2 and client.yahoo_requests == 2
    marker = tmp_path / "state" / "hold.json"
    assert json.loads(marker.read_text()) == {"held_until": "2026-11-05T21:00:00Z", "status": 429}
    assert marker.stat().st_mode & 0o777 == 0o600
    assert marker.parent.stat().st_mode & 0o777 == 0o700   # only because this run created the directory
    assert options_schwab.yahoo_tail_hold(SESSION_NOW.timestamp()) == "yahoo_recent_rate_limit"


def test_cross_run_hold_lasts_thirty_minutes_and_fails_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    marker, now = tmp_path / "hold.json", SESSION_NOW.timestamp()
    hold = lambda t: options_schwab.yahoo_tail_hold(t, marker)
    warnings: list[str] = []
    monkeypatch.setattr(options_schwab.log, "warning", lambda message, **_kw: warnings.append(message))

    assert hold(now) is None and warnings == []            # no marker, no hold, nothing logged
    assert options_schwab.record_yahoo_rate_limit(now, marker) is True
    assert hold(now) == hold(now + 1799) == "yahoo_recent_rate_limit"
    assert hold(now + 1800) is None and hold(now + 86400) is None
    assert hold(now - 600) is None                          # a "held until" too far ahead was not written by this code
    nested = b"[" * 200_000 + b"]" * 200_000               # past the JSON parser's depth: RecursionError
    for bad in (b"", b"not json", b"[]", b'{"held_until": 5}', b'{"held_until": "tomorrow"}', b'{"status": 429}',
                b"\xff", b"\xff\xfe\x00", nested, b"null", b'"2026-11-05T21:00:00Z"'):
        marker.write_bytes(bad)
        assert hold(now) is None                            # whatever is in it, a bad marker never blocks
    assert len(warnings) == 11 and all("treated as not held" in w for w in warnings)
    marker.unlink()
    marker.mkdir()                                          # unreadable as a file
    assert hold(now) is None and len(warnings) == 12
    assert options_schwab.record_yahoo_rate_limit(now, tmp_path / "hold.json" / "x" / "y") in (True, False)  # never raises


def test_marker_path_comes_from_the_state_directory_or_the_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRID_OPTIONS_YAHOO_HOLD_FILE", raising=False)
    assert options_schwab.yahoo_hold_file() == Path("~/.local/state/grid/options_yahoo_hold.json").expanduser()
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", "/var/tmp/hold.json")
    assert options_schwab.yahoo_hold_file() == Path("/var/tmp/hold.json")


def test_a_yahoo_429_stops_the_run_and_the_next_run_is_held(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(tmp_path / "hold.json"))
    yahoo = _FakeYahoo(limited_at_request=2)
    puller, market, _yahoo, results = _schwab_run(monkeypatch, ["KO"], ["OLMA", "NUOX", "ACIU"], _body(), yahoo=yahoo)

    assert results[0]["status"] == "SUCCESS" and len(market.calls) == 1
    assert results[1]["ticker"] == "OLMA" and results[1]["status"] == "FAILED"
    assert "incomplete options chain response" in results[1]["error"]       # refused, not stored half-read
    assert results[2] == {"ticker": "NUOX", "status": "FAILED", "rows_inserted": 0, "reason": "source_stopped",
                          "error": "yahoo rate limited (HTTP 429)", "provider": "yahoo", "http_status": 429}
    assert results[3]["reason"] == "source_stopped_before_request" and results[3]["provider"] == "yahoo"
    assert len(yahoo.calls) == 2
    assert [b["ticker"] for b in puller.engine.batches] == ["KO"]
    assert results.summary["error"] == "yahoo rate limited (HTTP 429); remaining tickers not requested"

    again = _FakeYahoo()
    _puller, market, _yahoo, results = _schwab_run(monkeypatch, ["KO"], ["OLMA", "NUOX"], _body(), yahoo=again)
    assert again.calls == [] and len(market.calls) == 1                      # the core is still read
    assert [r.get("reason") for r in results[1:]] == ["yahoo_recent_rate_limit"] * 2
    assert all(r["status"] == "SKIPPED" and r["provider"] == "yahoo" for r in results[1:])


def test_names_past_the_run_budget_are_skipped_not_started(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_schwab, "YAHOO_RUN_REQUEST_BUDGET", 3)
    yahoo = _FakeYahoo()
    _puller, _market, _yahoo, results = _schwab_run(monkeypatch, ["KO"], ["OLMA", "NUOX", "ACIU", "AGEN"], _body(), yahoo=yahoo)

    # Two requests a name (two expiries): OLMA and NUOX run (4 >= 3 after NUOX), the rest are not started.
    assert [(r["ticker"], r["status"], r.get("reason")) for r in results[1:]] == [
        ("OLMA", "SUCCESS", None), ("NUOX", "SUCCESS", None),
        ("ACIU", "SKIPPED", "yahoo_request_budget"), ("AGEN", "SKIPPED", "yahoo_request_budget")]
    assert len(yahoo.calls) == 4 and {t for t, _e in yahoo.calls} == {"OLMA", "NUOX"}
    assert results.summary["status"] == "PARTIAL"


def test_the_default_budget_covers_a_full_run_of_non_core_names() -> None:
    # About 160 catalyst names at six expiries each (re-review of #1299: 600 forced every full run PARTIAL).
    assert options_schwab.YAHOO_RUN_REQUEST_BUDGET == 1000
    assert options_schwab.YAHOO_RUN_REQUEST_BUDGET >= 160 * options_schwab.YAHOO_MAX_EXPIRATIONS
    client = options_schwab.SchwabFirstOptionsClient(_client()[0])
    client.yahoo_requests = 999
    assert client.yahoo_budget_spent is False
    client.yahoo_requests = 1000
    assert client.yahoo_budget_spent is True


def test_non_core_names_are_read_to_six_expiries_and_the_core_to_the_run_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, int] = {}

    def _pull_ticker(self: Any, ticker: str, _day: str, **kw: Any) -> dict:
        seen[ticker] = kw["max_expirations"]
        return {"ticker": ticker, "status": "SUCCESS", "rows_inserted": 1}

    monkeypatch.setattr(options.OptionsPuller, "_pull_ticker", _pull_ticker)
    _schwab_run(monkeypatch, ["KO"], ["OLMA"], publish=True)
    assert seen == {"KO": 12, "OLMA": 6} and options_schwab.YAHOO_MAX_EXPIRATIONS == 6
    seen.clear()
    _schwab_run(monkeypatch, ["KO"], [], tickers=["KO", "OLMA"], max_expirations=4)
    assert seen == {"KO": 4, "OLMA": 4}                    # a smaller run cap still wins


@pytest.mark.parametrize("when", [datetime(2026, 11, 5, 21, 0, 1, tzinfo=timezone.utc),
                                  datetime(2026, 11, 5, 22, 0, 0, tzinfo=timezone.utc)], ids=["just-after", "22:00Z"])
def test_after_the_cash_close_nothing_is_requested_from_either_source(
        monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    monkeypatch.setattr(options, "_utc_now", lambda: when)
    monkeypatch.setattr(options_schwab, "SchwabFirstOptionsClient",
                        lambda **_kw: pytest.fail("no client is built after the close"))
    puller, market, yahoo, results = _schwab_run(monkeypatch, ["KO"], ["OLMA"], block="unused")

    assert market.calls == [] and yahoo.calls == [] and puller.writers == []
    assert list(results) == [
        {"ticker": "KO", "status": "SKIPPED", "rows_inserted": 0, "reason": "post-session", "provider": "schwab"},
        {"ticker": "OLMA", "status": "SKIPPED", "rows_inserted": 0, "reason": "post-session", "provider": "yahoo"}]
    assert results.summary["status"] == "SKIPPED"


def test_the_default_path_keeps_twelve_expiries_and_no_yahoo_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER", raising=False)
    monkeypatch.setattr(options, "EQUITY_TICKERS", ["KO"])
    monkeypatch.setattr(options, "catalyst_options_universe", lambda *_a, **_kw: ["OLMA"])
    for name in ("schwab_post_session", "yahoo_tail_hold", "schwab_core_block"):
        monkeypatch.setattr(options_schwab, name, lambda *_a, **_kw: pytest.fail("schwab-mode only"))
    monkeypatch.setattr(options, "CboeFirstOptionsClient", lambda: _FakeYahoo())
    seen: dict[str, int] = {}
    monkeypatch.setattr(options.OptionsPuller, "_pull_ticker", lambda self, ticker, _day, **kw: (
        seen.update({ticker: kw["max_expirations"]}) or {"ticker": ticker, "status": "SUCCESS", "rows_inserted": 1}))
    puller = _puller(monkeypatch)
    monkeypatch.setattr(puller, "_mark_catalog_pulled", lambda **_kw: True)
    puller.pull_all()
    assert seen == {"KO": 12, "OLMA": 12}


# ── review of 0305998e ───────────────────────────────────────────────────


@pytest.mark.parametrize("content", [b"\xff", b"\xff\xfe\x00", b"[" * 200_000 + b"]" * 200_000],
                         ids=["not-utf8", "not-utf8-bom", "nested"])
def test_a_garbled_marker_never_aborts_a_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, content: bytes) -> None:
    # F1: this used to raise out of pull_all before any ticker, core included, on every run.
    marker = tmp_path / "hold.json"
    marker.write_bytes(content)
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(marker))
    puller, market, yahoo, results = _schwab_run(monkeypatch, ["KO"], ["OLMA"], _body(), publish=True)
    assert [r["status"] for r in results] == ["SUCCESS", "SUCCESS"] and len(market.calls) == 1
    assert {t for t, _e in yahoo.calls} == {"OLMA"} and puller.published == [True]


def test_a_429_on_the_crumb_handshake_latches_and_writes_the_hold(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # F2: the handshake is the likeliest place to meet Yahoo's block.
    marker = tmp_path / "hold.json"
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(marker))
    yahoo = _FakeYahoo(available=False, init_status=429)
    puller, market, _yahoo, results = _schwab_run(monkeypatch, ["KO"], ["OLMA", "NUOX", "ACIU"], _body(), yahoo=yahoo)

    assert yahoo.calls == [] and len(market.calls) == 1
    assert results[1]["ticker"] == "OLMA" and results[1]["status"] == "FAILED"
    assert results[1]["rate_limited"] == "yahoo rate limited (HTTP 429)"
    assert results[2]["reason"] == "source_stopped" and results[2]["error"] == "yahoo rate limited (HTTP 429)"
    assert results[3]["reason"] == "source_stopped_before_request"
    assert json.loads(marker.read_text()) == {"held_until": "2026-11-05T21:00:00Z", "status": 429}
    assert puller._yahoo.yahoo_rate_limited is True


@pytest.mark.parametrize("status", [None, 401, 503])
def test_another_handshake_failure_writes_no_hold(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, status: Any) -> None:
    marker = tmp_path / "hold.json"
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(marker))
    yahoo = _FakeYahoo(available=False, init_status=status)
    puller, _market, _yahoo, results = _schwab_run(monkeypatch, ["KO"], ["OLMA", "NUOX"], _body(), yahoo=yahoo)
    assert [r["status"] for r in results] == ["SUCCESS", "FAILED", "FAILED"]   # not a stop: just unavailable
    assert not marker.exists() and puller._yahoo.yahoo_rate_limited is False


def test_an_existing_shared_state_directory_keeps_its_mode(tmp_path: Path) -> None:
    # F3: ~/.local/state/grid already holds scheduler and AV state; the marker must not tighten it.
    shared = tmp_path / "state"
    shared.mkdir(mode=0o755)
    shared.chmod(0o755)
    marker = shared / "options_yahoo_hold.json"
    assert options_schwab.record_yahoo_rate_limit(SESSION_NOW.timestamp(), marker) is True
    assert shared.stat().st_mode & 0o777 == 0o755 and marker.stat().st_mode & 0o777 == 0o600
    assert sorted(p.name for p in shared.iterdir()) == ["options_yahoo_hold.json"]   # no temp file left behind


def test_a_429_on_the_last_name_shows_in_the_run_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # F4: with no later name to stop on, the summary used to read as an ordinary incomplete chain.
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(tmp_path / "hold.json"))
    yahoo = _FakeYahoo(limited_at_request=4)        # the second expiry of the second, and last, name
    _puller, _market, _yahoo, results = _schwab_run(monkeypatch, ["KO"], ["OLMA", "NUOX"], _body(), yahoo=yahoo)

    assert [r["status"] for r in results] == ["SUCCESS", "SUCCESS", "FAILED"]
    assert results[2]["rate_limited"] == "yahoo rate limited (HTTP 429)" and "rate_limited" not in results[1]
    assert results.summary["status"] == "PARTIAL"
    assert results.summary["error"].endswith("1 tickers not captured (NUOX); yahoo rate limited (HTTP 429)")
    assert (tmp_path / "hold.json").exists()
