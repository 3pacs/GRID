"""Tiingo's own listing is reported next to each lapsed ticker, and never
changes the run's status.

2026-11-10..23 on grid-svr: ``smart:tiingo`` and ``Tiingo_Prices`` had no
SUCCESS row while each run wrote the session's closes for about 1,700 tickers.
Every run named a few tickers Tiingo had stopped serving (KPTI ``endDate``
2026-11-10, ROKU 2026-11-06, BKH no dates, GWHT not found).

An earlier head of this change let such a listing clear the PARTIAL. Codex
HOLD 6031219314: a listing without dates can be an incomplete record of a
live ticker, and an old ``endDate`` can be a late vendor, so neither
establishes a delisting. Now the listing is evidence only: the error text and
``lapsed_listing`` say what it shows, and the run stays PARTIAL until the
ticker is marked in ``analysis/sector_map_ticker_status.yaml``.

Local fakes only: no network, no database.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest
import requests

import ingestion.tiingo_pull as tp
from ingestion.tiingo_pull import TiingoPuller, listing_state

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

    def _listing(self, ticker, limiter=None):
        self.listing_calls.append(ticker)
        return self.listing[ticker]


def _run(latest, fetch, listing, **kwargs):
    puller = _Puller(latest, fetch, listing)
    return puller, puller.pull_incremental(list(latest), now=_NOW, **kwargs)


def test_expected_session_of_the_fixture():
    assert tp.expected_latest_session(_NOW) == _SESSION


@pytest.mark.parametrize(("found", "end", "text"), [
    (None, None, "listing not read"),
    (False, None, "not found in Tiingo's listing"),
    (True, None, "no dates in Tiingo's listing"),
    (True, date(2026, 11, 10), "listing ends 2026-11-10"),
])
def test_listing_state_words(found, end, text):
    assert listing_state(found, end) == text


@pytest.mark.parametrize(("name", "listing"), [
    ("stopped weeks ago", (True, date(2026, 11, 10))),          # KPTI
    ("not found", (False, None)),                               # GWHT
    ("incomplete record of a live ticker", (True, None)),       # BKH's shape; cannot tell live from ended
    ("delayed listing of a live ticker", (True, date(2026, 11, 17))),   # vendor three sessions late
    ("one session behind", (True, _PRIOR)),
    ("listing has the session; our fetch missed it", (True, _SESSION)),
    ("listing not read", (None, None)),
])
def test_no_listing_answer_clears_a_lapsed_ticker(name, listing):
    """The control cases Codex asked for: every one stays lapsed and PARTIAL."""
    _, out = _run({"SPY": _PRIOR, "XYZ": date(2026, 11, 13)}, {"SPY": _OK, "XYZ": _EMPTY}, {"XYZ": listing})
    assert out["status"] == "PARTIAL", name
    assert out["lapsed_tickers"] == ["XYZ"]
    assert out["lapsed_listing"] == {"XYZ": listing_state(*listing)}
    assert f"XYZ ({listing_state(*listing)})" in out["error"]
    assert "ended_at_source" not in out


def test_the_run_of_2026_11_20_stays_partial_and_says_what_the_listing_shows():
    latest = {"SPY": _PRIOR, "BKH": date(2026, 11, 13), "GWHT": date(2026, 11, 13),
              "KPTI": date(2026, 11, 10)}
    puller, out = _run(
        latest,
        {"SPY": _OK, "BKH": _EMPTY, "GWHT": _EMPTY, "KPTI": _EMPTY},
        {"BKH": (True, None), "GWHT": (False, None), "KPTI": (True, date(2026, 11, 10))},
    )
    assert out["status"] == "PARTIAL"
    assert out["lapsed_tickers"] == ["BKH", "GWHT", "KPTI"]
    assert out["error"].endswith(
        "got none for it: BKH (no dates in Tiingo's listing), "
        "GWHT (not found in Tiingo's listing), KPTI (listing ends 2026-11-10)")
    assert puller.listing_calls == ["BKH", "GWHT", "KPTI"]       # never the current ticker


def test_a_clean_run_reads_no_listing():
    puller, out = _run({"SPY": _PRIOR}, {"SPY": _OK}, {})
    assert out["status"] == "SUCCESS" and out["lapsed_listing"] == {} and puller.listing_calls == []


def test_listing_reads_are_capped_and_the_rest_are_still_named(monkeypatch):
    monkeypatch.setattr(tp, "MAX_COVERAGE_CHECKS", 2)
    latest = {t: date(2026, 11, 13) for t in ("A", "B", "C")}
    puller, out = _run(latest, {t: _EMPTY for t in latest}, {t: (False, None) for t in latest})
    assert puller.listing_calls == ["A", "B"]
    assert out["lapsed_tickers"] == ["A", "B", "C"] and out["status"] == "PARTIAL"
    assert out["error"].endswith("A (not found in Tiingo's listing), B (not found in Tiingo's listing), C")


def test_no_listing_read_once_the_run_was_told_to_stop():
    calls = iter([True, True, False, False, False])
    latest = {"A": date(2026, 11, 13), "B": date(2026, 11, 13)}
    puller, out = _run(latest, {t: _EMPTY for t in latest}, {t: (False, None) for t in latest},
                       max_workers=1, should_continue=lambda: next(calls, False))
    assert puller.listing_calls == []
    assert out["status"] == "PARTIAL" and out["lapsed_listing"] == {}


def _real_puller() -> TiingoPuller:
    puller = TiingoPuller.__new__(TiingoPuller)
    puller.engine, puller.source_id = MagicMock(), 524
    return puller


@pytest.mark.parametrize(("status", "body", "expected"), [
    (200, {"ticker": "KPTI", "startDate": "2026-11-05", "endDate": "2026-11-10"}, (True, date(2026, 11, 10))),
    (200, {"ticker": "UTZ", "endDate": "2026-11-19T00:00:00.000Z"}, (True, date(2026, 11, 19))),
    (200, {"ticker": "BKH", "startDate": None, "endDate": None}, (True, None)),
    (404, {"detail": "Not found."}, (False, None)),
    (429, {"detail": "rate limit"}, (None, None)),
    (500, {}, (None, None)),
    (401, {"detail": "Invalid token."}, (None, None)),
    (200, ["not", "a", "listing"], (None, None)),
    (200, {"endDate": "soon"}, (None, None)),
])
def test_listing_reads_tiingos_record(monkeypatch, status, body, expected):
    seen: list[str] = []
    resp = MagicMock(status_code=status)
    resp.json.return_value = body

    def _get(url, headers, timeout):
        seen.append(url)
        return resp

    monkeypatch.setattr(tp.requests, "get", _get)
    assert _real_puller()._listing("KPTI") == expected
    assert seen == ["https://api.tiingo.com/tiingo/daily/KPTI"]


@pytest.mark.parametrize("exc", [requests.ConnectionError("down"), requests.Timeout("slow")])
def test_listing_network_error_is_not_read(monkeypatch, exc):
    def _get(url, headers, timeout):
        raise exc

    monkeypatch.setattr(tp.requests, "get", _get)
    assert _real_puller()._listing("KPTI") == (None, None)


def test_listing_bad_json_is_not_read(monkeypatch):
    resp = MagicMock(status_code=200)
    resp.json.side_effect = ValueError("not json")
    monkeypatch.setattr(tp.requests, "get", lambda url, headers, timeout: resp)
    assert _real_puller()._listing("KPTI") == (None, None)
