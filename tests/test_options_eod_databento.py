"""End-of-day option-chain archive from Databento OPRA history.

Every row here is synthetic, written by hand in the provider's CSV column
names. No test touches the network, a real key file or a shared store.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import requests

from ingestion import options_eod_databento as eod

DAY = date(2026, 11, 20)          # a Friday session
HALF_DAY = date(2026, 11, 27)     # the day after Thanksgiving closes at 13:00 New York
SERVED_TO = datetime(2026, 11, 21, 14, 30, tzinfo=timezone.utc)
FETCHED = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)


def _sym(root: str, expiry: str, side: str, strike: float) -> str:
    return f"{root:<6}{expiry[2:4]}{expiry[5:7]}{expiry[8:10]}{side}{round(strike * 1000):08d}"


def _oi(root: str = "KO") -> list[dict[str, str]]:
    call, put = _sym(root, "2026-12-18", "C", 70), _sym(root, "2026-12-18", "P", 70)
    return [
        {"symbol": call, "stat_type": "9", "quantity": "120", "publisher_id": "30"},
        {"symbol": call, "stat_type": "9", "quantity": "120", "publisher_id": "31"},
        {"symbol": call, "stat_type": "9", "quantity": "118", "publisher_id": "32"},   # one exchange lags
        {"symbol": put, "stat_type": "9", "quantity": "40", "publisher_id": "30"},
        {"symbol": put, "stat_type": "7", "quantity": "999", "publisher_id": "30"},     # not open interest
        {"symbol": _sym(root + "1", "2026-12-18", "C", 70), "stat_type": "9", "quantity": "5"},  # adjusted root
        {"symbol": _sym("PEP", "2026-12-18", "C", 70), "stat_type": "9", "quantity": "5"},       # another root
        {"symbol": call, "stat_type": "9", "quantity": "", "publisher_id": "33"},
    ]


def _close(root: str = "KO") -> list[dict[str, str]]:
    call, put = _sym(root, "2026-12-18", "C", 70), _sym(root, "2026-12-18", "P", 70)
    return [
        {"symbol": call, "bid_px_00": "1.05", "ask_px_00": "1.10", "ts_recv": "2026-11-20T20:59:10.000000000Z",
         "price": "1.07", "ts_event": "2026-11-20T19:02:11.000000000Z"},
        {"symbol": call, "bid_px_00": "1.06", "ask_px_00": "1.11", "ts_recv": "2026-11-20T20:59:58.000000000Z",
         "price": "1.08", "ts_event": "2026-11-20T20:41:03.000000000Z"},
        {"symbol": call, "bid_px_00": "1.00", "ask_px_00": "1.20", "ts_recv": "2026-11-20T20:59:30.000000000Z",
         "price": "1.07", "ts_event": "2026-11-20T19:02:11.000000000Z"},
        # Never traded that day: no last sale, and its empty event time is not kept as a clock.
        {"symbol": put, "bid_px_00": "", "ask_px_00": "0.45", "ts_recv": "2026-11-20T20:59:40.000000000Z",
         "price": "", "ts_event": ""},
    ]


def _bars(root: str = "KO") -> list[dict[str, str]]:
    call = _sym(root, "2026-12-18", "C", 70)   # one bar per exchange that traded it
    return [{"symbol": call, "close": "1.08", "volume": "300", "publisher_id": "30"},
            {"symbol": call, "close": "1.07", "volume": "10", "publisher_id": "31"}]


class _Api:
    """Stands in for DatabentoHistorical: prices and rows per (schema, root)."""

    def __init__(self, *, costs: dict[str, float] | None = None, rows: dict[str, Any] | None = None,
                 end: datetime = SERVED_TO) -> None:
        self.costs = costs or {}
        self.data = {"statistics": _oi(), "cbbo-1m": _close(), "ohlcv-1d": _bars()} if rows is None else rows
        self.end = end
        self.priced: list[tuple[str, str]] = []
        self.fetched: list[tuple[str, str, datetime, datetime]] = []

    def dataset_end(self, _schema: str) -> datetime:
        return self.end

    def cost(self, schema: str, root: str, _start: datetime, _end: datetime) -> float:
        self.priced.append((schema, root))
        return self.costs.get(schema, 0.0)

    def rows(self, schema: str, root: str, start: datetime, end: datetime) -> Any:
        self.fetched.append((schema, root, start, end))
        answer = self.data[schema]
        if isinstance(answer, Exception):
            raise answer
        return iter(answer)


def _run(api: _Api, tmp_path: Path, **kw: Any) -> tuple[dict[str, Any], Any, list[str]]:
    con, lines = eod.open_store(tmp_path), []
    kw.setdefault("now", lambda: FETCHED)
    result = eod.capture(api, con, kw.pop("tickers", ["KO"]), kw.pop("days", [DAY]), out=lines.append, **kw)
    return result, con, lines


# ── pure helpers ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(("symbol", "parsed"), [
    ("KO    261218C00070000", ("KO", "2026-12-18", "C", 70000)),
    ("BRKB  270115P00480500", ("BRKB", "2027-01-15", "P", 480500)),
    ("KO261218C00070000", ("KO", "2026-12-18", "C", 70000)),
    ("KO1   261218C00070000", None), ("KO    261318C00070000", None), ("KO    261218X00070000", None),
    ("short", None), (None, None), (70000, None),
])
def test_parse_osi(symbol: Any, parsed: Any) -> None:
    assert eod.parse_osi(symbol) == parsed


def test_opra_root() -> None:
    assert [eod.opra_root(t) for t in ("KO", "brk-b", " spy ")] == ["KO", "BRKB", "SPY"]


def test_part_windows_follow_the_session_calendar() -> None:
    def utc(*parts: int) -> datetime:
        return datetime(*parts, tzinfo=timezone.utc)

    assert eod.part_window("oi", DAY) == (utc(2026, 11, 20, 10, 0), utc(2026, 11, 20, 12, 30))
    assert eod.part_window("close", DAY) == (utc(2026, 11, 20, 20, 59), utc(2026, 11, 20, 21, 0))
    assert eod.part_window("close", HALF_DAY) == (utc(2026, 11, 27, 17, 59), utc(2026, 11, 27, 18, 0))
    assert eod.part_window("close", date(2026, 7, 6)) == (utc(2026, 7, 6, 19, 59), utc(2026, 7, 6, 20, 0))  # summer time
    assert eod.part_window("bars", DAY) == (utc(2026, 11, 20), utc(2026, 11, 21))
    with pytest.raises(ValueError, match="unknown part"):
        eod.part_window("greeks", DAY)


def test_open_interest_keeps_the_most_common_value_of_standard_contracts() -> None:
    chosen, disagree = eod.parse_oi(_oi(), "KO")
    assert chosen == {("2026-12-18", "C", 70000): 120, ("2026-12-18", "P", 70000): 40}
    assert disagree == 1
    tie = [{"symbol": _sym("KO", "2026-12-18", "C", 70), "stat_type": "9", "quantity": q} for q in ("5", "9")]
    assert eod.parse_oi(tie, "KO")[0] == {("2026-12-18", "C", 70000): 9}  # a tie goes to the larger


def test_closing_quote_is_the_latest_row_and_a_missing_side_stays_missing() -> None:
    quotes = eod.parse_close(_close(), "KO")
    assert quotes[("2026-12-18", "C", 70000)] == (1.06, 1.11, "2026-11-20T20:59:58.000000000Z",
                                                  1.08, "2026-11-20T20:41:03.000000000Z")
    assert quotes[("2026-12-18", "P", 70000)] == (None, 0.45, "2026-11-20T20:59:40.000000000Z", None, None)


def test_bars_sum_volume_over_exchanges_and_refuse_bad_numbers() -> None:
    rows = _bars() + [{"symbol": _sym("KO", "2026-12-18", "P", 70), "close": "nan", "volume": "-3"},
                      {"symbol": _sym("KO", "2026-12-18", "P", 70), "close": "0.4", "volume": "2.5"}]
    assert eod.parse_bars(rows, "KO") == {("2026-12-18", "C", 70000): 310}


def test_recent_sessions_skip_weekends_holidays_and_today() -> None:
    # Monday 2026-11-30: the five sessions before it, without Thanksgiving (11-26) or the weekend.
    assert eod.recent_sessions(date(2026, 11, 30)) == [
        date(2026, 11, 20), date(2026, 11, 23), date(2026, 11, 24), date(2026, 11, 25), date(2026, 11, 27)]
    assert eod.sessions(date(2026, 11, 20), date(2026, 11, 23)) == [date(2026, 11, 20), date(2026, 11, 23)]


# ── capture ──────────────────────────────────────────────────────────────


def test_free_day_is_stored_once_and_read_back_with_its_clocks(tmp_path: Path) -> None:
    api = _Api()
    result, con, lines = _run(api, tmp_path)

    assert result == {"counts": {"ok": 3}, "estimated_usd": 0.0, "cap_usd": 0.0}
    assert [f[0] for f in api.fetched] == ["statistics", "cbbo-1m", "ohlcv-1d"]
    assert api.fetched[1][2:] == eod.part_window("close", DAY)
    assert len(lines) == 3 and lines[0] == "2026-11-20 KO oi ok 2 contracts $0.0000"
    chain = eod.read_chain(con, DAY, "KO")
    assert chain["parts"] == {p: {"status": "ok", "rows": n, "known_at": "2026-11-21T12:00:00Z"}
                              for p, n in (("oi", 2), ("close", 2), ("bars", 1))}
    assert chain["contracts"] == [
        {"expiry": "2026-12-18", "opt_type": "call", "strike": 70.0, "open_interest": 120, "bid": 1.06, "ask": 1.11,
         "quote_ts": "2026-11-20T20:59:58.000000000Z", "last_price": 1.08,
         "last_ts": "2026-11-20T20:41:03.000000000Z", "volume": 310},
        {"expiry": "2026-12-18", "opt_type": "put", "strike": 70.0, "open_interest": 40, "bid": None, "ask": 0.45,
         "quote_ts": "2026-11-20T20:59:40.000000000Z", "last_price": None, "last_ts": None, "volume": None},
    ]
    assert chain["provider"] == "databento" and chain["audience"] == "private"
    assert "derived at read" in chain["implied_vol"]
    detail = con.execute("select detail from fetch_log where part = 'oi'").fetchone()[0]
    assert detail == "1 contract(s) with differing values"

    again = _Api()
    result, _con, _lines = _run(again, tmp_path)
    assert result["counts"] == {"already": 3} and again.priced == [] and again.fetched == []
    assert eod.status(con) == {"parts": {"bars:ok": 1, "close:ok": 1, "oi:ok": 1}, "first_day": "2026-11-20",
                               "last_day": "2026-11-20", "days": 1, "contracts": 2, "no_instruments": []}


def test_a_priced_part_is_never_fetched_and_is_retried_later(tmp_path: Path) -> None:
    api = _Api(costs={"cbbo-1m": 0.37})
    result, con, lines = _run(api, tmp_path)

    assert result["counts"] == {"ok": 2, "priced": 1} and result["estimated_usd"] == 0.0
    assert [f[0] for f in api.fetched] == ["statistics", "ohlcv-1d"]
    assert "2026-11-20 KO close priced $0.3700 (cap $0.00): not fetched" in lines
    assert con.execute("select status, cost_usd from fetch_log where part = 'close'").fetchone() == ("priced", 0.37)
    assert eod.read_chain(con, DAY, "KO")["contracts"][0]["bid"] is None

    later = _Api()  # the plan covers it again
    assert _run(later, tmp_path)[0]["counts"] == {"already": 2, "ok": 1}
    assert [f[0] for f in later.fetched] == ["cbbo-1m"]


def test_cap_counts_the_running_total_of_estimates(tmp_path: Path) -> None:
    api = _Api(costs={"statistics": 0.6, "cbbo-1m": 0.6, "ohlcv-1d": 0.1})
    result, _con, _lines = _run(api, tmp_path, cap_usd=1.0)
    assert result["counts"] == {"ok": 2, "priced": 1} and result["estimated_usd"] == 0.7
    assert [f[0] for f in api.fetched] == ["statistics", "ohlcv-1d"]  # 0.6 + 0.6 would pass the cap; 0.6 + 0.1 does not


def test_price_only_writes_nothing(tmp_path: Path) -> None:
    api = _Api(costs={"cbbo-1m": 0.37})
    result, con, _lines = _run(api, tmp_path, fetch=False)
    assert result["counts"] == {"free": 2, "priced": 1} and api.fetched == []
    assert con.execute("select count(*) from fetch_log").fetchone()[0] == 0
    assert con.execute("select count(*) from contracts").fetchone()[0] == 0


def test_a_window_databento_does_not_serve_yet_is_left_for_a_later_run(tmp_path: Path) -> None:
    api = _Api(end=datetime(2026, 11, 20, 14, 30, tzinfo=timezone.utc))  # served up to the open only
    result, con, _lines = _run(api, tmp_path)
    assert result["counts"] == {"ok": 1, "pending": 2} and [f[0] for f in api.fetched] == ["statistics"]
    assert con.execute("select part from fetch_log").fetchall() == [("oi",)]


def test_no_rows_and_no_instruments_are_final_and_an_error_is_retryable(tmp_path: Path) -> None:
    api = _Api(rows={"statistics": [], "cbbo-1m": eod.DatabentoError(422, "symbology_invalid_request"),
                     "ohlcv-1d": eod.DatabentoError(503, "error")})
    result, con, lines = _run(api, tmp_path)
    assert result["counts"] == {"no_data": 2, "error": 1}
    assert dict(con.execute("select part, status from fetch_log")) == {"oi": "no_data", "close": "no_data", "bars": "error"}
    assert "2026-11-20 KO close no_data (no instruments for the root; not fetched again)" in lines
    assert eod.status(con)["no_instruments"] == ["2026-11-20 KO close"]   # the empty oi part is not listed

    retry = _Api()
    assert _run(retry, tmp_path)[0]["counts"] == {"already": 2, "ok": 1}
    assert [f[0] for f in retry.fetched] == ["ohlcv-1d"]


def test_one_failed_ticker_does_not_stop_the_next(tmp_path: Path) -> None:
    class _Flaky(_Api):
        def rows(self, schema: str, root: str, start: datetime, end: datetime) -> Any:
            if root == "KO":
                raise eod.DatabentoError(500, "error")
            return super().rows(schema, root, start, end)

    api = _Flaky(rows={"statistics": _oi("PEP"), "cbbo-1m": _close("PEP"), "ohlcv-1d": _bars("PEP")})
    result, con, _lines = _run(api, tmp_path, tickers=["KO", "PEP"])
    assert result["counts"] == {"error": 3, "ok": 3}
    assert len(eod.read_chain(con, DAY, "PEP")["contracts"]) == 2 and eod.read_chain(con, DAY, "KO")["contracts"] == []


def test_brk_b_is_requested_and_stored_under_its_osi_root(tmp_path: Path) -> None:
    api = _Api(rows={"statistics": _oi("BRKB"), "cbbo-1m": _close("BRKB"), "ohlcv-1d": _bars("BRKB")})
    _result, con, _lines = _run(api, tmp_path, tickers=["BRK-B"])
    assert {f[1] for f in api.fetched} == {"BRKB"}
    assert len(eod.read_chain(con, DAY, "BRK-B")["contracts"]) == 2


# ── the HTTP client ──────────────────────────────────────────────────────


class _Resp:
    def __init__(self, status: int, body: Any = None, lines: list[str] | None = None) -> None:
        self.status_code, self._body, self._lines, self.encoding = status, body, lines or [], None

    def json(self) -> Any:
        if isinstance(self._body, Exception):
            raise self._body
        return self._body

    def iter_lines(self, decode_unicode: bool = False) -> Any:
        assert decode_unicode is True
        return iter(self._lines)


class _Session:
    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.calls: list[dict[str, Any]] = []

    def _answer(self, method: str, url: str, **kw: Any) -> Any:
        self.calls.append({"method": method, "url": url, **kw})
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def post(self, url: str, **kw: Any) -> Any:
        return self._answer("POST", url, **kw)

    def get(self, url: str, **kw: Any) -> Any:
        return self._answer("GET", url, **kw)


def _client(*answers: Any) -> tuple[eod.DatabentoHistorical, _Session, list[float]]:
    session, sleeps = _Session(*answers), []
    return eod.DatabentoHistorical("synthetic-key", session=session, sleep=sleeps.append), session, sleeps


def test_client_prices_and_streams_csv_with_the_parent_symbol() -> None:
    start, end = eod.part_window("close", DAY)
    client, session, _sleeps = _client(
        _Resp(200, 0.0),
        _Resp(200, lines=["ts_recv,bid_px_00,ask_px_00,symbol", "2026-11-20T20:59:58Z,1.06,1.11,KO    261218C00070000"]))

    assert client.cost("cbbo-1m", "KO", start, end) == 0.0
    rows = list(client.rows("cbbo-1m", "KO", start, end))

    assert rows == [{"ts_recv": "2026-11-20T20:59:58Z", "bid_px_00": "1.06", "ask_px_00": "1.11",
                     "symbol": "KO    261218C00070000"}]
    query = {"dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "symbols": "KO.OPT", "stype_in": "parent",
             "start": "2026-11-20T20:59:00Z", "end": "2026-11-20T21:00:00Z"}
    assert session.calls[0]["url"].endswith("/v0/metadata.get_cost") and session.calls[0]["data"] == query
    assert session.calls[1]["url"].endswith("/v0/timeseries.get_range") and session.calls[1]["stream"] is True
    assert session.calls[1]["data"] == dict(query, encoding="csv", pretty_px="true", pretty_ts="true", map_symbols="true")
    assert [c["method"] for c in session.calls] == ["POST", "POST"]
    assert all(c["headers"]["Authorization"].startswith("Basic ") for c in session.calls)
    assert all("synthetic-key" not in json.dumps(c["data"]) and "synthetic-key" not in c["url"] for c in session.calls)


def test_client_reads_the_served_range_once() -> None:
    client, session, _sleeps = _client(_Resp(200, {
        "start": "2013-04-01T00:00:00.000000000Z", "end": "2026-11-21T14:30:00.000000000Z",
        "schema": {"cbbo-1m": {"start": "2013-04-01T00:00:00.000000000Z", "end": "2026-11-21T00:00:00.000000000Z"}}}))
    assert client.dataset_end("cbbo-1m") == datetime(2026, 11, 21, tzinfo=timezone.utc)
    assert client.dataset_end("statistics") == datetime(2026, 11, 21, 14, 30, tzinfo=timezone.utc)  # dataset end
    assert len(session.calls) == 1
    assert session.calls[0]["method"] == "GET" and session.calls[0]["params"] == {"dataset": "OPRA.PILLAR"}


def test_client_retries_gateway_errors_then_reports_a_code_without_the_body() -> None:
    client, session, sleeps = _client(_Resp(503), requests.ConnectionError("boom"), _Resp(200, 0.25))
    start, end = eod.part_window("oi", DAY)
    assert client.cost("statistics", "KO", start, end) == 0.25
    assert sleeps == [5.0, 20.0] and len(session.calls) == 3

    client, _session, _sleeps = _client(_Resp(422, {"detail": {"case": "symbology_invalid_request",
                                                               "message": "private text"}}))
    with pytest.raises(eod.DatabentoError) as refused:
        client.cost("statistics", "ZZZZ", start, end)
    assert refused.value.status == 422 and refused.value.detail == "symbology_invalid_request"
    assert "private text" not in str(refused.value)

    client, _session, sleeps = _client(*[requests.ConnectionError("boom")] * 4)
    with pytest.raises(eod.DatabentoError, match="network"):
        client.cost("statistics", "KO", start, end)
    assert sleeps == [10.0, 20.0, 30.0]   # no wait after the last attempt


# ── key file and command line ────────────────────────────────────────────


def test_key_file_must_be_owner_only_and_present(tmp_path: Path) -> None:
    key = tmp_path / "api-key"
    with pytest.raises(eod.DatabentoError, match="missing"):
        eod.load_key(key)
    key.write_text("synthetic-key\n")
    key.chmod(0o644)
    with pytest.raises(eod.DatabentoError, match="owner only") as refused:
        eod.load_key(key)
    assert "synthetic-key" not in str(refused.value)
    key.chmod(0o600)
    assert eod.load_key(key) == "synthetic-key"
    key.write_text("\n")
    with pytest.raises(eod.DatabentoError, match="empty"):
        eod.load_key(key)


def test_command_line_refuses_tickers_outside_the_core_and_unfinished_days(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.setattr(eod, "load_key", lambda: pytest.fail("no key is read before the request is valid"))
    assert eod.main(["price", "--store", str(tmp_path), "--tickers", "KO,NUOX"]) == 2
    assert "not in the hand-listed core: NUOX" in capsys.readouterr().err
    assert eod.main(["price", "--store", str(tmp_path), "--day", "2026-11-21"]) == 2   # a Saturday
    assert eod.main(["run", "--store", str(tmp_path), "--day", "2099-01-05"]) == 2     # not finished yet
    assert eod.main(["status", "--store", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)["contracts"] == 0


def test_command_line_runs_a_capture_into_the_given_store(tmp_path: Path, capsys, monkeypatch) -> None:
    api = _Api()
    monkeypatch.setattr(eod, "load_key", lambda: "synthetic-key")
    monkeypatch.setattr(eod, "DatabentoHistorical", lambda _key: api)
    assert eod.main(["run", "--store", str(tmp_path), "--day", "2026-11-20", "--tickers", "ko"]) == 0
    assert '"ok": 3' in capsys.readouterr().out
    assert len(eod.read_chain(eod.open_store(tmp_path), DAY, "KO")["contracts"]) == 2
    monkeypatch.setattr(eod, "DatabentoHistorical", lambda _key: _Api(costs={"statistics": 1.0}))
    assert eod.main(["run", "--store", str(tmp_path), "--day", "2026-11-19", "--tickers", "KO"]) == 1  # a priced part


# ── spend cap and per-part failures (review of 663ba264) ─────────────────


@pytest.mark.parametrize("cap", [float("nan"), float("inf"), -0.01, 5.01, 1e9, True, "1", None])
def test_cap_must_be_finite_and_inside_the_hard_ceiling(tmp_path: Path, cap: Any) -> None:
    api = _Api()
    with pytest.raises(ValueError, match="cap_usd must be a finite amount from 0 to 5.00"):
        eod.capture(api, eod.open_store(tmp_path), ["KO"], [DAY], cap_usd=cap, out=lambda _line: None)
    assert api.priced == [] and api.fetched == []


def test_the_hard_ceiling_itself_is_allowed(tmp_path: Path) -> None:
    api = _Api(costs={"statistics": 2.0, "cbbo-1m": 2.0, "ohlcv-1d": 2.0})
    result, _con, _lines = _run(api, tmp_path, cap_usd=eod.HARD_CAP_USD)
    assert result["counts"] == {"ok": 2, "priced": 1} and result["estimated_usd"] == 4.0


@pytest.mark.parametrize("cost", [float("nan"), float("inf"), -1.0, None, "0", True])
def test_an_unusable_price_is_an_error_and_nothing_is_fetched(tmp_path: Path, cost: Any) -> None:
    api = _Api(costs={"cbbo-1m": cost})
    result, con, _lines = _run(api, tmp_path, cap_usd=5.0)
    assert result["counts"] == {"ok": 2, "error": 1} and result["estimated_usd"] == 0.0
    assert [f[0] for f in api.fetched] == ["statistics", "ohlcv-1d"]
    assert con.execute("select status, detail from fetch_log where part = 'close'").fetchone() == ("error", "ValueError")


def _cut_mid_stream() -> Any:
    yield _close()[0]
    raise requests.exceptions.ChunkedEncodingError("https://example.invalid/?key=synthetic-key connection broken")


@pytest.mark.parametrize(("failure", "kind"), [
    (_cut_mid_stream, "ChunkedEncodingError"),
    (lambda: (_ for _ in ()).throw(eod.csv.Error("line contains NUL")), "Error"),
    (lambda: (_ for _ in ()).throw(KeyError("symbol")), "KeyError"),
    (lambda: (_ for _ in ()).throw(ValueError("not JSON")), "ValueError"),
], ids=["stream-cut", "csv", "missing-field", "bad-body"])
def test_one_broken_part_is_logged_and_the_rest_of_the_run_continues(tmp_path: Path, failure: Any, kind: str) -> None:
    class _Broken(_Api):
        def rows(self, schema: str, root: str, start: datetime, end: datetime) -> Any:
            if (schema, root) == ("cbbo-1m", "KO"):
                self.fetched.append((schema, root, start, end))
                return failure()
            return super().rows(schema, root, start, end)

    api = _Broken(rows={"statistics": _oi("PEP"), "cbbo-1m": _close("PEP"), "ohlcv-1d": _bars("PEP")})
    result, con, _lines = _run(api, tmp_path, tickers=["KO", "PEP"])

    assert result["counts"]["error"] == 1 and result["counts"]["ok"] == 3   # all of PEP still captured
    status, detail = con.execute("select status, detail from fetch_log where root = 'KO' and part = 'close'").fetchone()
    assert (status, detail) == ("error", kind) and "synthetic-key" not in detail
    # Nothing from the half-read part was stored: the write happens only after the whole part parses.
    assert con.execute("select count(*) from contracts where root = 'KO' and bid is not null").fetchone()[0] == 0
    assert len(eod.read_chain(con, DAY, "PEP")["contracts"]) == 2


def test_a_malformed_range_answer_fails_parts_not_the_run(tmp_path: Path) -> None:
    class _NoRange(_Api):
        def dataset_end(self, _schema: str) -> datetime:
            raise KeyError("end")

    result, con, _lines = _run(_NoRange(), tmp_path)
    assert result["counts"] == {"error": 3}
    assert {d for (d,) in con.execute("select detail from fetch_log")} == {"KeyError"}


@pytest.mark.parametrize("cap", ["nan", "inf", "-1", "5.01", "ten"])
def test_command_line_refuses_a_cap_outside_the_ceiling(tmp_path: Path, capsys, monkeypatch, cap: str) -> None:
    monkeypatch.setattr(eod, "load_key", lambda: pytest.fail("no key is read for a refused cap"))
    with pytest.raises(SystemExit) as stopped:
        eod.main(["run", "--store", str(tmp_path), "--day", "2026-11-20", f"--cap-usd={cap}"])
    assert stopped.value.code == 2
    assert "--cap-usd" in capsys.readouterr().err
