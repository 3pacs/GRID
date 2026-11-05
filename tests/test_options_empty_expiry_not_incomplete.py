"""An explicit empty expiry is data; a missing, null or malformed page fails closed.

grid-hermes 2026-10-22 (release 60446c6, invocation c336d7c6): 416 catalyst-universe
tickers ended ``FAILED "incomplete options chain response"`` on every session run,
paired with 264 ``get_options`` warnings "Yahoo options <T> failed: list index out of
range", and ``trading.options_recommender`` rejected the same 416 for DATA_QUALITY.
The warning is ``result.get("options", [{}])[0]`` on a well-formed chain whose
``options`` list is explicitly empty: Yahoo lists the expiry but serves no contracts
for it (thin names). ``get_options`` turned that into ``None``, indistinguishable from
a transport failure, and ``_pull_ticker`` failed the whole ticker.

Contract pinned here:
* ``"options": []`` (explicit) and explicit empty sides are the chain as served.
* A missing, null or non-list ``options``, an element without list ``calls`` AND
  ``puts``, an empty ``result`` and a non-200 response are not a chain: the page is
  a failed fetch and the ticker fails closed even when other expiries have contracts.
* Every accepted page, empty or not, must carry a quote time on the session day that
  is not after the publish clock; before this, an empty page's future quote time
  escaped the snapshot-row check because it contributed no rows.
* A failed ticker writes nothing: no batch header, no rows, no daily signals, no
  resolution push.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Self
from unittest.mock import MagicMock

import pytest
from loguru import logger as log

from ingestion import options

SESSION_NOW = datetime(2026, 9, 25, 19, tzinfo=timezone.utc)  # Friday 15:00 ET
QUOTE_AT = int((SESSION_NOW - timedelta(hours=2)).timestamp())
PUBLISH_AT = SESSION_NOW + timedelta(minutes=1)  # the fake DB's clock_timestamp()
FUTURE_QUOTE_AT = int((SESSION_NOW + timedelta(hours=3)).timestamp())  # same ET day, after publish
PROVIDER_ERROR = {"code": "Service Unavailable", "description": "SYNTHETIC-UPSTREAM-FAILURE-7f3a"}


class _FrozenDatetime(datetime):
    """``datetime.now`` inside ingestion.options is SESSION_NOW; everything else is inherited."""

    @classmethod
    def now(cls, tz: Any = None) -> datetime:
        return SESSION_NOW if tz is not None else SESSION_NOW.replace(tzinfo=None)


class _Result:
    def __init__(self, row: tuple) -> None:
        self.row = row
        self.rowcount = 1

    def fetchone(self) -> tuple:
        return self.row


class _DB:
    """Offline append-only store double (same contract as test_options_capture_batch)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.rows: list[dict] = []
        self.batches: list[dict] = []
        self.next_xid = 0

    def begin(self) -> Self:
        return self

    def __enter__(self) -> Self:
        self._before = [row.copy() for row in self.rows]
        self._before_batches = [batch.copy() for batch in self.batches]
        return self

    def __exit__(self, exc_type: object, *_args: object) -> None:
        if exc_type is not None:
            self.rows = self._before
            self.batches = self._before_batches

    def execute(self, statement: Any, params: dict | None = None) -> _Result:
        sql = str(statement)
        values = params or {}
        self.calls.append((sql, values))
        if "txid_current()" in sql:
            self.next_xid += 1
            return _Result((self.next_xid, SESSION_NOW))
        if "SELECT clock_timestamp()" in sql:
            return _Result((PUBLISH_AT,))
        if "MAX(capture_ordinal) FROM options_capture_batches" in sql:
            ordinals = [b["ordinal"] for b in self.batches if b["ticker"] == values["ticker"]
                        and b["snap_date"] == values["snap_date"]]
            return _Result((max(ordinals) if ordinals else None,))
        if "DELETE" in sql.upper().split() or sql.lstrip().upper().startswith("UPDATE OPTIONS"):
            raise AssertionError(f"append-only store mutated: {sql}")
        if "INSERT INTO options_capture_batches" in sql:
            self.batches.append(values.copy())
        elif "INSERT INTO options_snapshots_all" in sql:
            assert any(b["batch_id"] == values["batch_id"] for b in self.batches), \
                "row written before its batch was registered"
            self.rows.append(values.copy())
        return _Result((None,))

    def wrote_signals(self) -> bool:
        return any("INSERT INTO options_daily_signals" in sql for sql, _ in self.calls)


def _expirations(*days: int) -> list[int]:
    return [int((SESSION_NOW + timedelta(days=d)).timestamp()) for d in days]


def _exp_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _contract(strike: float) -> dict[str, Any]:
    return {"strike": strike, "volume": 3, "openInterest": 10, "impliedVolatility": 0.2,
            "lastPrice": 2.0, "bid": 1.0, "ask": 3.0, "inTheMoney": False}


def _page(expirations: list[int], calls: list[float], puts: list[float], *,
          dated: bool = True, quote_at: int = QUOTE_AT) -> dict:
    """What a well-formed get_options returns (sides are always lists)."""
    quote: dict[str, Any] = {"regularMarketPrice": 100.0}
    if dated:
        quote["regularMarketTime"] = quote_at
    return {"quote": quote, "expirations": expirations, "strikes": sorted({*calls, *puts}),
            "calls": [_contract(s) for s in calls], "puts": [_contract(s) for s in puts]}


def _body(expirations: list[int], calls: list[float] | None, puts: list[float] | None, *,
          options_value: Any = "sides", quote_at: int = QUOTE_AT, error: Any = None) -> dict:
    """A raw Yahoo v7 body. ``options_value`` overrides the ``options`` list for malformed cases.

    ``error`` is the ``optionChain.error`` member: ``None`` is the normal successful
    body (explicit null), the sentinel ``"absent"`` omits the key, anything else is a
    present non-null provider error.
    """
    result: dict[str, Any] = {
        "underlyingSymbol": "ACIU", "expirationDates": expirations, "strikes": [],
        "quote": {"regularMarketPrice": 100.0, "regularMarketTime": quote_at},
    }
    if options_value == "sides":
        element: dict[str, Any] = {"expirationDate": expirations[0]}
        if calls is not None:
            element["calls"] = [_contract(s) for s in calls]
        if puts is not None:
            element["puts"] = [_contract(s) for s in puts]
        result["options"] = [element]
    elif options_value != "absent":
        result["options"] = options_value
    chain: dict[str, Any] = {"result": [result]}
    if error != "absent":
        chain["error"] = error
    return {"optionChain": chain}


class _Yahoo:
    """Scripted provider double at the get_options boundary: page i answers request i."""

    def __init__(self, pages: list[dict | None]) -> None:
        self.pages = pages
        self.calls = 0

    def get_options(self, _ticker: str, _expiry: int | None = None) -> dict | None:
        page = self.pages[self.calls]
        self.calls += 1
        return page


class _Response:
    def __init__(self, status_code: int, payload: Any) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class _Session:
    """Scripted HTTP session: response i answers request i."""

    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict | None = None, timeout: float | None = None) -> _Response:
        self.requests.append((url, dict(params or {})))
        return self.responses[len(self.requests) - 1]


def _client(*responses: _Response) -> options.YahooOptionsClient:
    """A real YahooOptionsClient over scripted bodies, so get_options' parsing is exercised."""
    client = options.YahooOptionsClient.__new__(options.YahooOptionsClient)
    client.crumb = "synthetic-crumb"
    client.session = _Session(list(responses))
    return client


@pytest.fixture
def puller(monkeypatch: pytest.MonkeyPatch) -> options.OptionsPuller:
    obj = options.OptionsPuller.__new__(options.OptionsPuller)
    obj.engine = _DB()
    monkeypatch.setattr(obj, "_push_to_resolved", MagicMock(return_value=0))
    monkeypatch.setattr(options.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(options, "datetime", _FrozenDatetime)
    monkeypatch.setattr(options, "_utc_now", lambda: SESSION_NOW)
    monkeypatch.setattr(options, "compute_max_pain", lambda *_args: 100.0)
    monkeypatch.setattr(options, "compute_iv_skew", lambda *_args: 0.0)
    monkeypatch.setattr(options, "_compute_atm_iv", lambda *_args: 0.2)
    monkeypatch.setattr(options, "_compute_wing_iv", lambda *_args, **_kwargs: 0.2)
    monkeypatch.setattr(options, "_compute_oi_concentration", lambda *_args: 0.5)
    return obj


def _pull(puller: options.OptionsPuller, provider: Any, ticker: str = "ACIU") -> dict:
    puller._yahoo = provider
    return puller._pull_ticker(ticker, SESSION_NOW.date().isoformat())


def _assert_nothing_written(puller: options.OptionsPuller, result: dict) -> None:
    """A failed ticker leaves no header, rows, signals or resolution behind."""
    assert result["status"] == "FAILED", result
    assert result["rows_inserted"] == 0
    assert puller.engine.batches == []
    assert puller.engine.rows == []
    assert not puller.engine.wrote_signals()
    assert puller._push_to_resolved.call_count == 0


# ── YahooOptionsClient.get_options: explicit empty vs malformed ─────────────

def test_get_options_explicit_empty_options_list_is_an_empty_chain() -> None:
    """The journal's "list index out of range": a listed expiry served with no contracts."""
    exp = _expirations(10, 20)
    chain = _client(_Response(200, _body(exp, None, None, options_value=[]))).get_options("ADRX", exp[1])
    assert chain == {
        "expirations": exp, "strikes": [], "calls": [], "puts": [],
        "quote": {"regularMarketPrice": 100.0, "regularMarketTime": QUOTE_AT},
    }


def test_get_options_explicit_empty_sides_are_an_empty_chain() -> None:
    exp = _expirations(10)
    chain = _client(_Response(200, _body(exp, [], []))).get_options("ADRX", exp[0])
    assert chain is not None and chain["calls"] == [] and chain["puts"] == []


@pytest.mark.parametrize("options_value", [None, "absent", "not-a-list", [None], [{}], [{"calls": []}]])
def test_get_options_missing_null_or_malformed_options_is_a_failed_fetch(options_value: Any) -> None:
    exp = _expirations(10)
    assert _client(_Response(200, _body(exp, None, None, options_value=options_value))).get_options("ACIU", exp[0]) is None


@pytest.mark.parametrize("calls, puts", [([100.0], None), (None, [100.0]), (None, None)])
def test_get_options_missing_side_is_a_failed_fetch(calls: list | None, puts: list | None) -> None:
    exp = _expirations(10)
    assert _client(_Response(200, _body(exp, calls, puts))).get_options("ACIU", exp[0]) is None


def test_get_options_null_side_is_a_failed_fetch() -> None:
    exp = _expirations(10)
    body = _body(exp, [100.0], [100.0])
    body["optionChain"]["result"][0]["options"][0]["puts"] = None
    assert _client(_Response(200, body)).get_options("ACIU", exp[0]) is None


def test_get_options_empty_result_list_is_a_failed_fetch() -> None:
    assert _client(_Response(200, {"optionChain": {"result": [], "error": None}})).get_options("NOPE") is None


def test_get_options_non_200_is_a_failed_fetch() -> None:
    assert _client(_Response(429, {})).get_options("SPY") is None


def test_get_options_served_contracts_unchanged() -> None:
    exp = _expirations(10)
    chain = _client(_Response(200, _body(exp, [100.0], [100.0]))).get_options("SPY")
    assert chain is not None and chain["calls"] == [_contract(100.0)] and chain["puts"] == [_contract(100.0)]


# ── OptionsPuller._pull_ticker: explicit empty pages are data ──────────────

def test_listed_expiry_with_no_contracts_does_not_fail_the_ticker(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20, 30)
    yahoo = _Yahoo([_page(exp, [100.0], [100.0]), _page(exp, [], []), _page(exp, [100.0], [100.0])])
    result = _pull(puller, yahoo)
    assert result["status"] == "SUCCESS", result
    assert yahoo.calls == 3
    assert result["snapshots_inserted"] == 4 and len(puller.engine.rows) == 4
    assert {row["expiry"] for row in puller.engine.rows} == {_exp_date(exp[0]), _exp_date(exp[2])}
    (batch,) = puller.engine.batches
    assert batch["row_count"] == 4 and result["latest_batch"] is True
    assert puller.engine.wrote_signals() and puller._push_to_resolved.call_count == 1


def test_explicit_empty_page_through_the_real_parser(puller: options.OptionsPuller) -> None:
    """End to end from the raw body: ``"options": []`` on a later page, ticker succeeds."""
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, None, None, options_value=[])))
    result = _pull(puller, client)
    assert result["status"] == "SUCCESS", result
    assert len(puller.engine.rows) == 2 and len(puller.engine.batches) == 1


def test_explicit_empty_side_is_stored_as_served(puller: options.OptionsPuller) -> None:
    """``"puts": []`` with calls served: the side served is the chain; the missing side is zero."""
    exp = _expirations(10, 20)
    result = _pull(puller, _Yahoo([_page(exp, [100.0], [100.0]), _page(exp, [105.0], [])]))
    assert result["status"] == "SUCCESS", result
    assert len(puller.engine.rows) == 3
    far = [row for row in puller.engine.rows if row["expiry"] == _exp_date(exp[1])]
    assert [(row["opt_type"], row["strike"]) for row in far] == [("call", 105.0)]
    assert result["signals"]["total_oi"] == 30


def test_every_expiry_empty_is_incomplete_not_a_success(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    result = _pull(puller, _Yahoo([_page(exp, [], []), _page(exp, [], [])]))
    _assert_nothing_written(puller, result)
    assert result["error"].startswith("incomplete options chain response")
    assert "2 empty of 2 expiries" in result["error"] and "0 contracts" in result["error"]


def test_empty_near_expiry_keeps_per_expiry_chain_alignment(
    puller: options.OptionsPuller, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skipping a near expiry (<2 DTE) must rebuild signals from the right chain."""
    seen: list[list[float]] = []

    def record_max_pain(calls, _puts, _spot):
        seen.append([] if calls.empty else sorted(calls["strike"]))
        return 100.0

    monkeypatch.setattr(options, "compute_max_pain", record_max_pain)
    exp = _expirations(1, 10, 20)
    result = _pull(puller, _Yahoo([_page(exp, [], []), _page(exp, [110.0], [110.0]), _page(exp, [120.0], [120.0])]))
    assert result["status"] == "SUCCESS", result
    assert seen == [[110.0]]  # the 10-day chain, not the 20-day one shifted into its slot
    assert len(puller.engine.rows) == 4


# ── OptionsPuller._pull_ticker: everything else fails closed, writing nothing ──

def test_failed_expiry_fetch_fails_closed_despite_contracts_elsewhere(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    result = _pull(puller, _Yahoo([_page(exp, [100.0], [100.0]), None]))
    _assert_nothing_written(puller, result)
    assert result["error"].startswith("incomplete options chain response")
    assert "1 failed expiry fetch" in result["error"]


@pytest.mark.parametrize("options_value", [None, "absent", [None], [{}], [{"calls": []}]])
def test_malformed_later_page_fails_closed_through_the_real_parser(
    puller: options.OptionsPuller, options_value: Any,
) -> None:
    """A null, missing or side-less ``options`` on page 2 is not an empty page."""
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, None, None, options_value=options_value)))
    result = _pull(puller, client)
    _assert_nothing_written(puller, result)
    assert "1 failed expiry fetch" in result["error"]


def test_missing_side_on_later_page_fails_closed(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, [105.0], None)))
    result = _pull(puller, client)
    _assert_nothing_written(puller, result)
    assert "1 failed expiry fetch" in result["error"]


def test_undated_later_page_fails_closed(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    result = _pull(puller, _Yahoo([_page(exp, [100.0], [100.0]), _page(exp, [100.0], [100.0], dated=False)]))
    _assert_nothing_written(puller, result)
    assert result["error"].startswith("incomplete options chain response")
    assert "1 undated quote" in result["error"]


def test_future_dated_later_page_with_contracts_fails_closed(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    result = _pull(puller, _Yahoo([_page(exp, [100.0], [100.0]), _page(exp, [100.0], [100.0], quote_at=FUTURE_QUOTE_AT)]))
    _assert_nothing_written(puller, result)
    assert "quote time is future" in result["error"]


def test_future_dated_empty_later_page_fails_closed(puller: options.OptionsPuller) -> None:
    """The escape: an accepted empty page has no rows, so its quote time was never checked."""
    exp = _expirations(10, 20)
    result = _pull(puller, _Yahoo([_page(exp, [100.0], [100.0]), _page(exp, [], [], quote_at=FUTURE_QUOTE_AT)]))
    _assert_nothing_written(puller, result)
    assert "quote time is future" in result["error"]


def test_future_dated_empty_page_through_the_real_parser(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, None, None, options_value=[], quote_at=FUTURE_QUOTE_AT)))
    result = _pull(puller, client)
    _assert_nothing_written(puller, result)
    assert "quote time is future" in result["error"]


# ── v3: a present non-null optionChain.error is a failed fetch, never an empty chain ──

@pytest.fixture
def warnings_captured():
    records: list[str] = []
    sink_id = log.add(lambda msg: records.append(str(msg)), level="WARNING")
    yield records
    log.remove(sink_id)


def test_get_options_absent_error_key_is_accepted() -> None:
    exp = _expirations(10)
    chain = _client(_Response(200, _body(exp, [100.0], [100.0], error="absent"))).get_options("SPY")
    assert chain is not None and chain["calls"] == [_contract(100.0)]


@pytest.mark.parametrize("calls, puts, options_value", [
    ([100.0], [100.0], "sides"),   # populated result beside an error
    (None, None, []),              # explicit empty page beside an error
    ([105.0], [], "sides"),        # explicit one-sided empty beside an error
])
def test_get_options_non_null_provider_error_is_a_failed_fetch(
    calls: list | None, puts: list | None, options_value: Any, warnings_captured: list[str],
) -> None:
    exp = _expirations(10, 20)
    body = _body(exp, calls, puts, options_value=options_value, error=PROVIDER_ERROR)
    assert _client(_Response(200, body)).get_options("ACIU", exp[1]) is None
    assert any("provider error in chain response" in r for r in warnings_captured)
    assert not any("SYNTHETIC-UPSTREAM-FAILURE-7f3a" in r for r in warnings_captured)


@pytest.mark.parametrize("error", [{"code": "x"}, "string error", 1, True, [1]])
def test_get_options_any_non_null_error_shape_is_a_failed_fetch(error: Any) -> None:
    exp = _expirations(10)
    assert _client(_Response(200, _body(exp, [100.0], [100.0], error=error))).get_options("SPY") is None


def test_get_options_non_null_error_with_empty_result_is_a_failed_fetch() -> None:
    assert _client(_Response(200, {"optionChain": {"result": [], "error": PROVIDER_ERROR}})).get_options("NOPE") is None


def test_get_options_error_null_explicit_empty_is_still_an_empty_chain() -> None:
    exp = _expirations(10, 20)
    chain = _client(_Response(200, _body(exp, None, None, options_value=[], error=None))).get_options("ADRX", exp[1])
    assert chain is not None and chain["calls"] == [] and chain["puts"] == []


def test_error_bearing_empty_later_page_fails_closed(puller: options.OptionsPuller) -> None:
    """Valid populated E1, then an HTTP 200 E2 with a non-null error, a residual valid
    same-session quote and options: [] — not measured zero coverage, no publication."""
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, None, None, options_value=[], error=PROVIDER_ERROR)))
    result = _pull(puller, client)
    _assert_nothing_written(puller, result)
    assert "1 failed expiry fetch" in result["error"]


def test_error_bearing_one_sided_empty_later_page_fails_closed(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, [105.0], [], error=PROVIDER_ERROR)))
    result = _pull(puller, client)
    _assert_nothing_written(puller, result)
    assert "1 failed expiry fetch" in result["error"]


def test_error_bearing_populated_later_page_fails_closed(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, [105.0], [105.0], error=PROVIDER_ERROR)))
    result = _pull(puller, client)
    _assert_nothing_written(puller, result)


def test_error_null_empty_later_page_still_succeeds(puller: options.OptionsPuller) -> None:
    """Positive control: the identical later page with error: null is an explicit empty page."""
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, None, None, options_value=[], error=None)))
    result = _pull(puller, client)
    assert result["status"] == "SUCCESS", result
    assert len(puller.engine.rows) == 2 and len(puller.engine.batches) == 1


def test_error_null_one_sided_later_page_still_succeeds(puller: options.OptionsPuller) -> None:
    exp = _expirations(10, 20)
    client = _client(_Response(200, _body(exp, [100.0], [100.0])),
                     _Response(200, _body(exp, [105.0], [], error=None)))
    result = _pull(puller, client)
    assert result["status"] == "SUCCESS", result
    assert len(puller.engine.rows) == 3
