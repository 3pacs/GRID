"""A ticker Tiingo itself stopped serving does not make the price run PARTIAL.

2026-11-10..23 on grid-svr: ``smart:tiingo`` and ``Tiingo_Prices`` had no
SUCCESS row for two weeks while each run wrote the session's closes for about
1,700 tickers. Every run named a few tickers whose last close was inside
``LAPSED_WINDOW_DAYS``; Tiingo's own listing (``/tiingo/daily/{ticker}``) showed
each had stopped there (KPTI ``endDate`` 2026-11-10, ROKU 2026-11-06, BKH no
dates, GWHT not found). Now the run asks the listing:

* stopped before the prior session, no dates, or not found: named in
  ``ended_at_source``, not in ``lapsed_tickers``;
* ``endDate`` on the prior session or later, or no usable answer: still
  lapsed, still PARTIAL (the 2026-11-08 no-false-green rule is unchanged).

Local fakes only: no network, no database.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest
import requests

import ingestion.tiingo_pull as tp
from ingestion.tiingo_pull import TiingoPuller, ended_at_source

_NOW = datetime(2026, 11, 21, 1, 0, tzinfo=timezone.utc)   # Fri 20:00 EST
_SESSION = date(2026, 11, 20)                               # Friday
_PRIOR = date(2026, 11, 19)
_OK = {"status": "SUCCESS", "latest_obs": _SESSION, "rows_inserted": 6}
_EMPTY = {"status": "PARTIAL", "errors": ["No data returned"]}


class _Puller(TiingoPuller):
    """pull_incremental with the lookups, the lock, the fetch and the listing scripted."""

    def __init__(self, latest, fetch, listing) -> None:
        self.engine = MagicMock()
        self.source_id = 524
        self.latest, self.fetch, self.listing = latest, fetch, listing
        self.listing_calls: list[str] = []

    def _latest_tiingo_obs(self, ticker):
        return self.latest.get(ticker)

    def _try_acquire_advisory_lock(self):
        return object()

    def _release_advisory_lock(self, conn):
        pass

    def pull_ticker(self, ticker, start_date="2020-01-01", end_date=None, limiter=None):
        return {"ticker": ticker, "rows_inserted": 0, "errors": [], **self.fetch[ticker]}

    def _coverage_end(self, ticker, limiter=None):
        self.listing_calls.append(ticker)
        return self.listing[ticker]


def _run(latest, fetch, listing, **kwargs):
    puller = _Puller(latest, fetch, listing)
    return puller, puller.pull_incremental(list(latest), now=_NOW, **kwargs)


def test_expected_session_of_the_fixture():
    assert tp.expected_latest_session(_NOW) == _SESSION


@pytest.mark.parametrize(("coverage_end", "ended"), [
    (None, True),                      # no price history listed (BKH), or not found (GWHT)
    (date(2026, 11, 10), True),        # KPTI
    (date(2026, 11, 18), True),        # two sessions back
    (_PRIOR, False),                   # may only be late with the newest bar (UTZ on 11-20)
    (_SESSION, False),                 # Tiingo has the bar: our fetch missed it
])
def test_ended_at_source_needs_a_whole_missed_session(coverage_end, ended):
    assert ended_at_source(coverage_end, _SESSION) is ended


def test_prior_session_skips_the_weekend_and_a_holiday():
    monday = date(2026, 11, 23)
    assert ended_at_source(date(2026, 11, 20), monday) is False     # Friday is the prior session
    assert ended_at_source(date(2026, 11, 19), monday) is True
    after_thanksgiving = date(2026, 11, 27)
    assert ended_at_source(date(2026, 11, 25), after_thanksgiving) is False   # Thu 11-26 closed
    assert ended_at_source(date(2026, 11, 24), after_thanksgiving) is True


def test_the_run_of_2026_11_20_is_success_with_the_stopped_tickers_named():
    latest = {"SPY": _PRIOR, "BKH": date(2026, 11, 13), "GWHT": date(2026, 11, 13),
              "KPTI": date(2026, 11, 10)}
    puller, out = _run(
        latest,
        {"SPY": _OK, "BKH": _EMPTY, "GWHT": _EMPTY, "KPTI": _EMPTY},
        {"BKH": (True, None), "GWHT": (True, None), "KPTI": (True, date(2026, 11, 10))},
    )
    assert out["status"] == "SUCCESS" and "error" not in out
    assert out["lapsed_tickers"] == []
    assert out["ended_at_source"] == {"BKH": None, "GWHT": None, "KPTI": "2026-11-10"}
    assert out["behind"] == 3 and out["no_data_tickers"] == ["BKH", "GWHT", "KPTI"]
    assert puller.listing_calls == ["BKH", "GWHT", "KPTI"]       # never the current ticker


@pytest.mark.parametrize("listing", [
    (True, _PRIOR),          # Tiingo is one session behind: may be late
    (True, _SESSION),        # Tiingo has the close; the fetch did not get it
    (False, None),           # no usable answer from the listing
])
def test_a_ticker_tiingo_still_lists_stays_lapsed_and_partial(listing):
    _, out = _run({"SPY": _PRIOR, "UTZ": _PRIOR}, {"SPY": _OK, "UTZ": _EMPTY}, {"UTZ": listing})
    assert out["status"] == "PARTIAL"
    assert out["lapsed_tickers"] == ["UTZ"] and out["ended_at_source"] == {}
    assert "UTZ" in out["error"]


def test_one_stopped_and_one_still_listed_is_partial_and_names_each_once():
    _, out = _run(
        {"KPTI": date(2026, 11, 10), "UTZ": _PRIOR},
        {"KPTI": _EMPTY, "UTZ": {"status": "FAILED", "errors": ["read timeout"]}},
        {"KPTI": (True, date(2026, 11, 10)), "UTZ": (True, _SESSION)},
    )
    assert out["status"] == "PARTIAL"
    assert out["lapsed_tickers"] == ["UTZ"] and out["ended_at_source"] == {"KPTI": "2026-11-10"}
    assert "KPTI" not in out["error"]


def test_listing_lookups_are_capped_and_the_rest_stay_lapsed(monkeypatch):
    monkeypatch.setattr(tp, "MAX_COVERAGE_CHECKS", 2)
    latest = {t: date(2026, 11, 13) for t in ("A", "B", "C")}
    puller, out = _run(latest, {t: _EMPTY for t in latest}, {t: (True, None) for t in latest})
    assert puller.listing_calls == ["A", "B"]
    assert out["ended_at_source"] == {"A": None, "B": None}
    assert out["lapsed_tickers"] == ["C"] and out["status"] == "PARTIAL"


def test_no_listing_lookup_once_the_run_was_told_to_stop():
    calls = iter([True, True, False, False, False])
    latest = {"A": date(2026, 11, 13), "B": date(2026, 11, 13)}
    puller, out = _run(latest, {t: _EMPTY for t in latest}, {t: (True, None) for t in latest},
                       max_workers=1, should_continue=lambda: next(calls, False))
    assert puller.listing_calls == []
    assert out["status"] == "PARTIAL" and out["ended_at_source"] == {}


def _real_puller() -> TiingoPuller:
    puller = TiingoPuller.__new__(TiingoPuller)
    puller.engine, puller.source_id = MagicMock(), 524
    return puller


@pytest.mark.parametrize(("status", "body", "expected"), [
    (200, {"ticker": "KPTI", "startDate": "2026-11-05", "endDate": "2026-11-10"}, (True, date(2026, 11, 10))),
    (200, {"ticker": "UTZ", "endDate": "2026-11-19T00:00:00.000Z"}, (True, date(2026, 11, 19))),
    (200, {"ticker": "BKH", "startDate": None, "endDate": None}, (True, None)),
    (404, {"detail": "Not found."}, (True, None)),
    (429, {"detail": "rate limit"}, (False, None)),
    (500, {}, (False, None)),
    (401, {"detail": "Invalid token."}, (False, None)),
    (200, ["not", "a", "listing"], (False, None)),
    (200, {"endDate": "soon"}, (False, None)),
])
def test_coverage_end_reads_tiingos_listing_and_fails_closed(monkeypatch, status, body, expected):
    seen: list[str] = []
    resp = MagicMock(status_code=status)
    resp.json.return_value = body

    def _get(url, headers, timeout):
        seen.append(url)
        return resp

    monkeypatch.setattr(tp.requests, "get", _get)
    assert _real_puller()._coverage_end("KPTI") == expected
    assert seen == ["https://api.tiingo.com/tiingo/daily/KPTI"]


@pytest.mark.parametrize("exc", [requests.ConnectionError("down"), requests.Timeout("slow")])
def test_coverage_end_network_error_is_unknown(monkeypatch, exc):
    def _get(url, headers, timeout):
        raise exc

    monkeypatch.setattr(tp.requests, "get", _get)
    assert _real_puller()._coverage_end("KPTI") == (False, None)


def test_coverage_end_bad_json_is_unknown(monkeypatch):
    resp = MagicMock(status_code=200)
    resp.json.side_effect = ValueError("not json")
    monkeypatch.setattr(tp.requests, "get", lambda url, headers, timeout: resp)
    assert _real_puller()._coverage_end("KPTI") == (False, None)
