"""Cboe delayed-quotes chains as the primary OptionsPuller provider.

Yahoo /v7/finance/options has answered 429 to grid-svr since 2026-09-29
(YFINANCE_OPTIONS stale 30+ days). The fixture is a trimmed copy of the live
https://cdn.cboe.com/api/global/delayed_quotes/options/KO.json document
captured 2026-11-06 (6 real contracts plus 4 synthetic edge rows in the same
record shape). No test here touches the network.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Self

import pytest

from ingestion import options

FIXTURE = Path(__file__).parent / "fixtures" / "cboe" / "KO_trimmed.json"
# 16:30 ET on Thursday 2026-11-05, after the fixture's 15:59:59 ET last trade.
SESSION_NOW = datetime(2026, 11, 5, 21, 30, tzinfo=timezone.utc)
NOV06 = int(datetime(2026, 11, 6, tzinfo=timezone.utc).timestamp())
NOV13 = int(datetime(2026, 11, 13, tzinfo=timezone.utc).timestamp())


def _doc() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text())


class _Resp:
    def __init__(self, status: int, body: Any = None) -> None:
        self.status_code = status
        self._body = body

    def json(self) -> Any:
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class _Session:
    def __init__(self, *responses: _Resp) -> None:
        self.responses = list(responses)
        self.urls: list[str] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, **kwargs: Any) -> _Resp:
        self.urls.append(url)
        assert kwargs.get("allow_redirects") is True  # cdn.cboe.com 307 -> cdn-api.cboe.com
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def _session_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options, "_utc_now", lambda: SESSION_NOW)


# ── parsing ──────────────────────────────────────────────────────────────


def test_parse_occ_symbol() -> None:
    assert options.parse_occ_symbol("KO261224C00099000") == ("KO", date(2026, 12, 24), "C", 99.0)
    assert options.parse_occ_symbol("BRKB261218P00395000") == ("BRKB", date(2026, 12, 18), "P", 395.0)
    assert options.parse_occ_symbol("SPY261106P00687500")[3] == 687.5
    assert options.parse_occ_symbol("KO261399C00099000") is None  # month 13
    assert options.parse_occ_symbol("not-an-option") is None
    assert options.parse_occ_symbol(None) is None


@pytest.mark.parametrize(("ticker", "symbol"), [
    ("SPY", "SPY"), ("brk-b", "BRK.B"), ("SPX", "_SPX"), ("XSP", "_XSP"),
])
def test_cboe_symbol(ticker: str, symbol: str) -> None:
    assert options.cboe_symbol(ticker) == symbol


def test_chain_pages_keep_standard_live_contracts_only() -> None:
    chain = options.cboe_chain_pages(_doc(), "KO", date(2026, 11, 5))
    assert chain is not None
    assert chain["expirations"] == [NOV06, NOV13]  # expired 10-30 row dropped
    near = chain["pages"][NOV06]
    # KO1 (adjusted deliverable) and the junk symbol are excluded.
    assert sorted(o["strike"] for o in near["calls"]) == [95.0, 96.0]
    assert sorted(o["strike"] for o in near["puts"]) == [95.0, 96.0]
    assert all(o["contractSymbol"].startswith("KO2611") for o in near["calls"] + near["puts"])
    far_calls = {o["strike"]: o for o in chain["pages"][NOV13]["calls"]}
    assert far_calls[100.0]["impliedVolatility"] is None  # iv 0.0 is missing, not zero vol
    quote = chain["quote"]
    assert quote["regularMarketPrice"] == pytest.approx(95.4576)
    # last_trade_time 2026-11-05T15:59:59 read as America/New_York (EST).
    assert quote["regularMarketTime"] == int(datetime(2026, 11, 5, 20, 59, 59, tzinfo=timezone.utc).timestamp())
    sample = near["calls"][0]
    assert set(sample) >= {"strike", "lastPrice", "bid", "ask", "volume", "openInterest",
                           "impliedVolatility", "inTheMoney"}
    assert isinstance(sample["volume"], int) and isinstance(sample["openInterest"], int)
    k95_call = next(o for o in near["calls"] if o["strike"] == 95.0)
    k96_put = next(o for o in near["puts"] if o["strike"] == 96.0)
    assert k95_call["inTheMoney"] is True and k96_put["inTheMoney"] is True


def test_chain_pages_reject_other_symbol_and_bad_shapes() -> None:
    assert options.cboe_chain_pages(_doc(), "PEP", date(2026, 11, 5)) is None
    assert options.cboe_chain_pages({"data": None}, "KO", date(2026, 11, 5)) is None
    assert options.cboe_chain_pages([], "KO", date(2026, 11, 5)) is None


# ── client ───────────────────────────────────────────────────────────────


def test_client_one_request_serves_every_expiry() -> None:
    session = _Session(_Resp(200, _doc()))
    client = options.CboeOptionsClient(session=session)
    first = client.get_options("KO")
    assert first["expirations"] == [NOV06, NOV13]
    assert len(first["calls"]) == 2 and len(first["puts"]) == 2
    second = client.get_options("KO", NOV13)
    assert {o["strike"] for o in second["calls"]} == {95.0, 100.0}
    assert session.urls == ["https://cdn.cboe.com/api/global/delayed_quotes/options/KO.json"]
    assert client.get_options("KO", 123) is None  # unknown expiry: failed page
    assert client.get_options("PEP", NOV13) is None  # no cached chain for PEP


def test_client_maps_ticker_to_cboe_symbol() -> None:
    session = _Session(_Resp(403))
    options.CboeOptionsClient(session=session).get_options("BRK-B")
    assert session.urls == ["https://cdn.cboe.com/api/global/delayed_quotes/options/BRK.B.json"]


@pytest.mark.parametrize(("response", "expected"), [
    (_Resp(403), options.NO_CHAIN),          # S3 AccessDenied: no listed options
    (_Resp(404), options.NO_CHAIN),
    (_Resp(429), None),                      # transport/provider failure
    (_Resp(500), None),
    (_Resp(200, ValueError("bad json")), None),
    (_Resp(200, {"data": {"symbol": "PEP", "options": []}, "symbol": "PEP"}), None),
])
def test_client_failure_classes(response: _Resp, expected: Any) -> None:
    got = options.CboeOptionsClient(session=_Session(response)).get_options("KO")
    assert got is expected


def test_client_chain_with_only_expired_contracts_is_no_chain() -> None:
    doc = _doc()
    doc["data"]["options"] = [o for o in doc["data"]["options"] if o["option"].startswith("KO261030")]
    got = options.CboeOptionsClient(session=_Session(_Resp(200, doc))).get_options("KO")
    assert got is options.NO_CHAIN


def test_transport_exception_is_a_failed_fetch() -> None:
    class _Boom(_Session):
        def get(self, url: str, **kwargs: Any) -> _Resp:
            raise ConnectionError("reset")

    assert options.CboeOptionsClient(session=_Boom()).get_options("KO") is None


# ── Cboe first, Yahoo fallback ───────────────────────────────────────────


class _FakeYahoo:
    provider = "yahoo"
    remote_expiry_pages = True
    is_available = True
    last_no_chain = False

    def __init__(self) -> None:
        self.calls: list[tuple[str, int | None]] = []

    def get_options(self, ticker: str, expiry_ts: int | None = None) -> dict:
        self.calls.append((ticker, expiry_ts))
        return {"expirations": [NOV06], "calls": [], "puts": [], "quote": {}}


def _composite(monkeypatch: pytest.MonkeyPatch, *responses: _Resp) -> tuple[Any, list[_FakeYahoo]]:
    made: list[_FakeYahoo] = []

    def _yahoo() -> _FakeYahoo:
        made.append(_FakeYahoo())
        return made[-1]

    monkeypatch.setattr(options, "YahooOptionsClient", _yahoo)
    client = options.CboeFirstOptionsClient()
    client.cboe = options.CboeOptionsClient(session=_Session(*responses))
    return client, made


def test_cboe_success_never_initialises_yahoo(monkeypatch: pytest.MonkeyPatch) -> None:
    client, made = _composite(monkeypatch, _Resp(200, _doc()))
    assert client.get_options("KO")["expirations"] == [NOV06, NOV13]
    assert client.get_options("KO", NOV13) is not None
    assert made == []
    assert client.provider_by_ticker == {"KO": "cboe"}
    assert client.remote_expiry_pages is False


def test_cboe_failure_falls_back_to_yahoo_for_the_whole_ticker(monkeypatch: pytest.MonkeyPatch) -> None:
    client, made = _composite(monkeypatch, _Resp(429), _Resp(429))
    client.get_options("KO")
    client.get_options("KO", NOV13)
    client.get_options("PEP")
    assert len(made) == 1  # one lazy Yahoo handshake per run
    assert made[0].calls == [("KO", None), ("KO", NOV13), ("PEP", None)]
    assert client.provider_by_ticker == {"KO": "yahoo", "PEP": "yahoo"}


def test_cboe_no_chain_is_authoritative(monkeypatch: pytest.MonkeyPatch) -> None:
    client, made = _composite(monkeypatch, _Resp(403))
    assert client.get_options("NUOX") is options.NO_CHAIN
    assert made == []


def test_primary_provider_knob(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER", raising=False)
    assert options.options_primary_provider() == "cboe"
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", " Yahoo ")
    assert options.options_primary_provider() == "yahoo"
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", "nonsense")
    assert options.options_primary_provider() == "cboe"


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


def test_pull_ticker_publishes_a_cboe_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    puller = options.OptionsPuller.__new__(options.OptionsPuller)
    puller.engine = _DB()
    monkeypatch.setattr(puller, "_push_to_resolved", lambda *_a, **_k: 0)
    sleeps: list[float] = []
    monkeypatch.setattr(options.time, "sleep", sleeps.append)
    session = _Session(_Resp(200, _doc()))
    client, made = _composite(monkeypatch)
    client.cboe = options.CboeOptionsClient(session=session)
    puller._yahoo = client

    result = puller._pull_ticker("KO", "2026-11-05")

    assert result["status"] == "SUCCESS", result
    assert len(session.urls) == 1 and made == []
    assert sleeps == []  # cached expiry pages are not paced like Yahoo requests
    db = puller.engine
    assert len(db.batches) == 1 and db.batches[0]["row_count"] == 7
    assert db.batches[0]["spot"] == pytest.approx(95.4576)
    assert {r["expiry"] for r in db.rows} == {"2026-11-06", "2026-11-13"}
    assert all(r["provider_regular_market_at"].date() == date(2026, 11, 5) for r in db.rows)
    assert db.signals and db.signals[0]["ne"] == "2026-11-13"  # 1-DTE skipped as illiquid


def test_pull_all_defaults_to_cboe_first(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER", raising=False)
    puller = options.OptionsPuller.__new__(options.OptionsPuller)
    seen: list[Any] = []

    def _pull_ticker(self: Any, ticker: str, _day: str, **_kw: Any) -> dict:
        seen.append(self._yahoo)
        return {"ticker": ticker, "status": "SUCCESS", "rows_inserted": 1}

    monkeypatch.setattr(options.OptionsPuller, "_pull_ticker", _pull_ticker)
    monkeypatch.setattr(options.time, "sleep", lambda _s: None)
    monkeypatch.setattr(options, "YahooOptionsClient",
                        lambda: pytest.fail("Yahoo must not initialise before a Cboe failure"))
    puller.pull_all(tickers=["KO"], max_expirations=6)
    assert isinstance(seen[0], options.CboeFirstOptionsClient)
