"""The Tiingo price pull names what it did not get and never reports a
recently trading ticker's missing close as SUCCESS.

2026-11-08 00:33Z: "SUCCESS: 1325 current, 103 fetched, 50 ok, 53 no data
... 0 rows". The 53 were never named, and a stock that stopped arriving
looked the same as a renamed or long-dead symbol. Now:

* every no-data ticker is named (``no_data_tickers``, capped);
* a ticker with a close inside ``LAPSED_WINDOW_DAYS`` before the expected
  session that still has none for it (no data, a failed fetch, data that
  stops earlier) makes the run PARTIAL and is named in ``lapsed_tickers``
  and ``error``;
* never-served and long-dead symbols are counted in ``behind`` but do not
  fail the run.

Local fakes only: no network, no database.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ingestion.tiingo_pull as tp
from ingestion.tiingo_pull import TiingoPuller, still_behind

_NOW = datetime(2026, 9, 30, 0, 0, tzinfo=timezone.utc)   # Tue 20:00 EDT
_SESSION = date(2026, 9, 29)


class _Puller(TiingoPuller):
    """pull_incremental with the lookups, the lock and the fetch scripted."""

    def __init__(self, latest: dict[str, date | None], fetch: dict[str, dict]) -> None:
        self.engine = MagicMock()
        self.source_id = 524
        self.latest, self.fetch = latest, fetch

    def _latest_tiingo_obs(self, ticker):
        return self.latest.get(ticker)

    def _try_acquire_advisory_lock(self):
        return object()

    def _release_advisory_lock(self, conn):
        pass

    def pull_ticker(self, ticker, start_date="2020-01-01", end_date=None, limiter=None):
        return {"ticker": ticker, "rows_inserted": 0, "errors": [], **self.fetch[ticker]}


_OK = {"status": "SUCCESS", "latest_obs": _SESSION, "rows_inserted": 6}
_EMPTY = {"status": "PARTIAL", "errors": ["No data returned"]}


def _run(latest, fetch):
    return _Puller(latest, fetch).pull_incremental(list(latest), now=_NOW)


def test_dead_and_never_served_symbols_are_named_but_do_not_fail_the_run():
    out = _run({"AAPL": _SESSION - timedelta(days=1), "BRL=X": None,
                "HES": date(2025, 7, 18)},
               {"AAPL": _OK, "BRL=X": _EMPTY, "HES": _EMPTY})
    assert out["status"] == "SUCCESS"
    assert out["no_data_tickers"] == ["BRL=X", "HES"]
    assert out["behind"] == 2 and out["lapsed_tickers"] == []
    assert "error" not in out


@pytest.mark.parametrize("fetch", [
    _EMPTY,                                                          # no data
    {"status": "FAILED", "errors": ["read timeout"]},                # failed fetch
    {"status": "SUCCESS", "latest_obs": _SESSION - timedelta(days=2)},  # stops earlier
])
def test_a_recently_trading_ticker_without_the_session_close_is_partial(fetch):
    out = _run({"AAPL": _SESSION - timedelta(days=1), "IMMP": _SESSION - timedelta(days=4)},
               {"AAPL": _OK, "IMMP": fetch})
    assert out["status"] == "PARTIAL"
    assert out["lapsed_tickers"] == ["IMMP"]
    assert "IMMP" in out["error"] and _SESSION.isoformat() in out["error"]


def test_the_lapsed_window_edge():
    edge = _SESSION - timedelta(days=tp.LAPSED_WINDOW_DAYS)
    out = _run({"OLD": edge - timedelta(days=1), "EDGE": edge}, {"OLD": _EMPTY, "EDGE": _EMPTY})
    assert out["lapsed_tickers"] == ["EDGE"]


def test_current_tickers_are_never_behind_and_named_lists_are_capped():
    latest = {f"T{i}": _SESSION - timedelta(days=1) for i in range(tp.MAX_NAMED_TICKERS + 5)}
    latest["CUR"] = _SESSION
    out = _run(latest, {t: _EMPTY for t in latest})
    assert out["current"] == 1 and out["behind"] == tp.MAX_NAMED_TICKERS + 5
    assert len(out["no_data_tickers"]) == len(out["lapsed_tickers"]) == tp.MAX_NAMED_TICKERS
    assert "CUR" not in out["no_data_tickers"]


def test_still_behind_uses_the_newer_of_stored_and_fetched_closes():
    results = [
        {"ticker": "A", "status": "SUCCESS", "last_before": _SESSION - timedelta(days=3),
         "latest_obs": _SESSION},
        {"ticker": "B", "status": "SUCCESS", "last_before": None, "latest_obs": None},
        {"ticker": "C", "status": "CURRENT"},
        {"ticker": "D", "status": "UNATTEMPTED"},
        {"ticker": "E", "status": "FAILED", "last_before": _SESSION - timedelta(days=1)},
    ]
    assert still_behind(results, _SESSION) == [("B", None), ("E", _SESSION - timedelta(days=1))]


def test_pull_ticker_reports_the_latest_close_it_fetched(monkeypatch):
    puller = TiingoPuller.__new__(TiingoPuller)
    puller.engine, puller.source_id = MagicMock(), 524
    resp = MagicMock(status_code=200)
    resp.json.return_value = [
        {"date": "2026-09-28T00:00:00.000Z", "close": 10.0, "adjClose": 10.0},
        {"date": "2026-09-29T00:00:00.000Z", "close": 11.0, "adjClose": 11.0},
    ]
    monkeypatch.setattr(tp.requests, "get", lambda *a, **k: resp)
    monkeypatch.setattr(puller, "_split_rows", lambda ticker, df: [])
    monkeypatch.setattr(puller, "_insert_rows", lambda rows: len(rows))
    out = puller.pull_ticker("AAPL", start_date="2026-09-20")
    assert out["status"] == "SUCCESS" and out["latest_obs"] == _SESSION
