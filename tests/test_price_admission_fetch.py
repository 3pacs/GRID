"""VS1 v6 vendor fetches (TwelveData, Tiingo metadata): files only, fake transport, synthetic data."""

from __future__ import annotations

import gzip
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from analysis import price_admission_fetch as fetch

KEY = "SECRET-KEY-must-never-appear"
T0 = datetime(2026, 9, 28, 11, 0, tzinfo=timezone.utc)


def _weekdays(start: date, end: date) -> list[date]:
    out, d = [], start
    while d < end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def td_body(closes: dict[date, float], symbol: str = "XLK") -> bytes:
    values = [{"datetime": d.isoformat(), "open": f"{v:.5f}", "high": f"{v:.5f}", "low": f"{v:.5f}",
               "close": f"{v:.5f}", "volume": "100"} for d, v in sorted(closes.items(), reverse=True)]
    return json.dumps({"meta": {"symbol": symbol, "interval": "1day", "currency": "USD", "exchange": "NYSE",
                                "mic_code": "ARCX", "type": "ETF", "exchange_timezone": "America/New_York"},
                       "values": values, "status": "ok"}).encode()


class FakeVendors:
    """TwelveData + Tiingo stand-in: records every call; serves synthetic closes."""

    def __init__(self, known=("XLK", "AAA"), *, daily_usage=100, limit=800, rate_limit_first=0, first_date=None):
        self.calls: list[tuple[str, dict, dict]] = []
        self.known = set(known)
        self.usage = daily_usage
        self.limit = limit
        self.rate_limit_left = rate_limit_first
        self.first_date = first_date

    def __call__(self, url, params, headers):
        self.calls.append((url, dict(params), dict(headers)))
        if url == fetch.TD_USAGE_URL:
            return 200, json.dumps({"daily_usage": self.usage, "plan_daily_limit": self.limit, "plan_limit": 8,
                                    "plan_category": "basic", "current_usage": 1}).encode()
        if url == fetch.TD_URL:
            assert headers["Authorization"] == f"apikey {KEY}" and "apikey" not in params
            self.usage += 1
            if self.rate_limit_left:
                self.rate_limit_left -= 1
                return 200, json.dumps({"code": 429, "message": "You have run out of API credits", "status": "error"}).encode()
            sym = params["symbol"]
            if sym not in self.known:
                return 200, json.dumps({"code": 400, "message": f"symbol not found: {sym}", "status": "error"}).encode()
            start = self.first_date or date.fromisoformat(params["start_date"])
            days = _weekdays(start, date.fromisoformat(params["end_date"]))  # end_date exclusive, as TwelveData
            scale = 1.0 if params["adjust"] == "none" else 0.9
            return 200, td_body({d: scale * (50 + (i % 7)) for i, d in enumerate(days)}, sym)
        if url.startswith("https://api.tiingo.com/tiingo/daily/"):
            assert headers["Authorization"] == f"Token {KEY}"
            t = url.rsplit("/", 1)[-1]
            if t not in self.known:
                return 404, b'{"detail":"Not found."}'
            return 200, json.dumps({"ticker": t, "name": f"{t} Corp", "exchangeCode": "NASDAQ",
                                    "startDate": "2010-06-29", "endDate": "2026-09-26",
                                    "description": "synthetic"}).encode()
        raise AssertionError(url)


def _run_td(tmp_path, vendors, tickers=("AAA", "BBB"), **kw):
    return fetch.fetch_twelvedata(list(tickers), tmp_path / "td", benchmark="XLK", key=KEY, http_get=vendors,
                                  sleep=lambda s: None, now=lambda: T0, spacing_s=0, **kw)


def test_request_is_the_pinned_one_and_never_reaches_the_holdout():
    p = fetch.td_params("AAA", "all")
    assert p == {"symbol": "AAA", "interval": "1day", "start_date": "2011-11-02", "end_date": "2019-12-31",
                 "adjust": "all", "outputsize": 5000}
    assert fetch.td_params("BRK-B", "none")["symbol"] == "BRK.B"
    with pytest.raises(fetch.FetchStopped, match="holdout"):
        fetch.check_request_window("2011-11-02", "2020-01-01")
    with pytest.raises(ValueError):
        fetch.td_params("AAA", "splits")


def test_twelvedata_fetch_writes_receipted_files_and_resumes(tmp_path):
    vendors = FakeVendors()
    result = _run_td(tmp_path, vendors)
    assert result["stopped"] is None and result["ok"] == 4 and result["unavailable"] == 2
    td_calls = [c for c in vendors.calls if c[0] == fetch.TD_URL]
    assert [c[1]["symbol"] for c in td_calls[:2]] == ["XLK", "XLK"]  # benchmark first: the plan check
    log = fetch.FetchLog(tmp_path / "td" / "fetch_log.jsonl")
    entries = log.entries()
    raw_log = (tmp_path / "td" / "fetch_log.jsonl").read_text()
    assert KEY not in raw_log and "apikey" not in raw_log
    for e in entries:
        body = gzip.decompress((tmp_path / "td" / e["file"]).read_bytes())
        assert fetch.sha256_bytes(body) == e["body_sha256"]
        assert fetch.sha256_bytes((tmp_path / "td" / e["file"]).read_bytes()) == e["file_sha256"]
        assert e["params"]["end_date"] == "2019-12-31" and e["fetched_at"] == T0.isoformat()
    bbb = [e for e in entries if e["ticker"] == "BBB"]
    assert {e["outcome"] for e in bbb} == {"unavailable"} and bbb[0]["td_code"] == 400
    assert set(log.final()) == {"XLK|all", "XLK|none", "AAA|all", "AAA|none", "BBB|all", "BBB|none"}
    again = FakeVendors()
    second = _run_td(tmp_path, again)
    assert second["skipped_done"] == 6 and not [c for c in again.calls if c[0] == fetch.TD_URL]


def test_twelvedata_fetch_retries_rate_limits_and_refetches_a_tampered_file(tmp_path):
    vendors = FakeVendors(rate_limit_first=2)
    assert _run_td(tmp_path, vendors, tickers=("AAA",))["ok"] == 4
    f = tmp_path / "td" / "AAA" / "all.json.gz"
    f.write_bytes(gzip.compress(b"{}", mtime=0))  # a file that no longer hashes to its receipt is not done
    again = FakeVendors()
    assert _run_td(tmp_path, again, tickers=("AAA",))["ok"] == 1
    assert [c[1]["adjust"] for c in again.calls if c[0] == fetch.TD_URL] == ["all"]


def test_twelvedata_daily_cap_keeps_the_reserve_and_stops(tmp_path):
    vendors = FakeVendors(daily_usage=545, limit=800)
    result = _run_td(tmp_path, vendors, daily_reserve=250)
    assert result["stopped"] == "daily_cap" and result["ok"] + result["unavailable"] == 5
    stops = [e for e in fetch.FetchLog(tmp_path / "td" / "fetch_log.jsonl").entries() if e["key"] == "_stop"]
    assert stops and stops[-1]["outcome"] == "daily_cap"


def test_twelvedata_waits_for_the_utc_reset_when_asked(tmp_path):
    vendors = FakeVendors(daily_usage=549, limit=800)
    slept = []

    def sleep(s):
        slept.append(s)
        if s > 3600:
            vendors.usage = 0  # the reset

    result = fetch.fetch_twelvedata(["AAA"], tmp_path / "td", benchmark="XLK", key=KEY, http_get=vendors,
                                    sleep=sleep, now=lambda: T0, spacing_s=0, daily_reserve=250,
                                    wait_for_reset=True)
    assert result["stopped"] is None and result["ok"] == 4 and max(slept) > 3600


def test_plan_that_cannot_serve_the_window_stops_the_fetch(tmp_path):
    vendors = FakeVendors(first_date=date(2015, 1, 2))
    with pytest.raises(fetch.FetchStopped, match="owner decides"):
        _run_td(tmp_path, vendors)
    assert [c[1]["symbol"] for c in vendors.calls if c[0] == fetch.TD_URL] == ["XLK"]


def test_load_td_closes_is_window_bounded_and_verified(tmp_path):
    vendors = FakeVendors()
    _run_td(tmp_path, vendors, tickers=("AAA",))
    got = fetch.load_td_closes(tmp_path / "td", "AAA")
    assert set(got["receipts"]) == {"all", "none"}
    assert min(got["all"]) == "2011-11-02" and max(got["all"]) == "2019-12-30"
    assert got["all"]["2011-11-02"] == pytest.approx(0.9 * got["none"]["2011-11-02"])
    # a vendor body carrying a 2020 date: refused and counted, never returned
    body = td_body({date(2019, 12, 30): 10.0, date(2020, 1, 2): 11.0}, "CCC")
    out = tmp_path / "td2"
    log = fetch.FetchLog(out / "fetch_log.jsonl")
    for adjust in ("all", "none"):
        name = f"CCC/{adjust}.json.gz"
        log.append({"key": f"CCC|{adjust}", "outcome": "ok", "file": name, "file_sha256": fetch.write_gz(out / name, body)})
    got = fetch.load_td_closes(out, "CCC")
    assert list(got["all"]) == ["2019-12-30"] and got["refused_holdout_dates"] == 2
    assert fetch.load_td_closes(out, "ZZZ") == {"receipts": {}, "refused_holdout_dates": 0}


def test_tiingo_meta_fetch_and_load(tmp_path):
    vendors = FakeVendors()
    result = fetch.fetch_tiingo_meta(["AAA", "BBB", "XLK"], tmp_path / "meta", key=KEY, http_get=vendors,
                                     sleep=lambda s: None, now=lambda: T0)
    assert (result["ok"], result["unavailable"]) == (2, 1)
    assert KEY not in (tmp_path / "meta" / "fetch_log.jsonl").read_text()
    aaa = fetch.load_tiingo_meta(tmp_path / "meta", "AAA")
    assert aaa["meta"] == {"ticker": "AAA", "name": "AAA Corp", "exchangeCode": "NASDAQ", "startDate": "2010-06-29",
                           "endDate": "2026-09-26"}
    assert aaa["receipt"]["outcome"] == "ok" and len(aaa["receipt"]["body_sha256"]) == 64
    assert fetch.load_tiingo_meta(tmp_path / "meta", "BBB")["meta"] is None
    assert fetch.load_tiingo_meta(tmp_path / "meta", "ZZZ") is None
    again = FakeVendors()
    assert fetch.fetch_tiingo_meta(["AAA", "BBB", "XLK"], tmp_path / "meta", key=KEY, http_get=again,
                                   sleep=lambda s: None)["skipped_done"] == 3
    assert again.calls == []


def test_network_errors_carry_no_url_or_key(monkeypatch):
    import requests

    def boom(*a, **k):
        raise requests.ConnectionError(f"HTTPSConnectionPool(host='x', url=/time_series?apikey={KEY})")

    monkeypatch.setattr(requests, "get", boom)
    with pytest.raises(ConnectionError) as info:
        fetch.requests_get(fetch.TD_URL, {"symbol": "AAA"}, {"Authorization": f"apikey {KEY}"})
    assert KEY not in str(info.value) and info.value.__cause__ is None


def test_missing_key_stops_before_any_request(monkeypatch):
    monkeypatch.delenv(fetch.TD_KEY_ENV, raising=False)
    with pytest.raises(fetch.FetchStopped, match=fetch.TD_KEY_ENV):
        fetch.api_key(fetch.TD_KEY_ENV)


def test_tickers_file_forms(tmp_path):
    p = tmp_path / "t.json"
    p.write_text(json.dumps({"issuers": [{"ticker": "aaa"}, {"ticker": "BBB"}]}))
    assert fetch.tickers_from_file(p) == ["AAA", "BBB"]
    p.write_text(json.dumps(["ccc", "AAA", "ccc"]))
    assert fetch.tickers_from_file(p) == ["AAA", "CCC"]
    p.write_text(json.dumps([""]))
    with pytest.raises(ValueError):
        fetch.tickers_from_file(p)
    assert isinstance(Path(p), Path)
