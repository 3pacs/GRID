"""Write-count contract fixes, 2026-11-09.

pull_log on grid-svr (last 7 days) still showed "puller did not report
committed write count" for Google_Trends, ICIJ_Full, OpenCorporates,
Asset_Registries, wikidata_persons and FARA. ICIJ_Full, wikidata_persons,
FARA and Asset_Registries were already fixed on main after their last run
(tests/test_puller_write_contract_20261104.py, ..._20261105.py,
..._20261105b.py cover them). Two needed code:

* Google_Trends (grid-scheduler weekly, ``pull_watchlist_breakouts``)
  returned ``{"entities_pulled", "breakouts"}`` with no row count, so the
  2026-11-08 03:18Z run, which scanned 48/48 entities and committed one
  attention_anomaly row (Jensen Huang), was recorded FAILED.
* OpenCorporates already returned an honest SKIPPED for its missing token,
  but SmartScheduler only writes pull_log rows for skips whose reason starts
  with ``MISSING_API_KEY_PREFIX``, so pull_log (and pipeline-health) kept the
  last pre-fix FAILED row from 2026-11-05 forever.

Every HTTP call is mocked; Google Trends inserts run against SQLite.
"""

from __future__ import annotations

import sys
import threading
import types
from datetime import date, datetime, timezone
from typing import Any, ClassVar
from unittest.mock import patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from ingestion.smart_scheduler import (
    MISSING_API_KEY_PREFIX,
    SMART_PULL_LOG_PREFIX,
    SmartScheduler,
    _classify_outcome,
)

# ── Google Trends watchlist breakouts ───────────────────────────────────


def _anomaly_engine() -> Any:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE attention_anomaly (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "entity_name TEXT NOT NULL, ticker TEXT, anomaly_date DATE NOT NULL, "
            "wikipedia_zscore REAL, trends_breakout REAL, combined_score REAL NOT NULL, "
            "price_move_5d REAL, source TEXT NOT NULL)"
        ))
    return engine


def _series(breakout: bool) -> list[float]:
    # 26 weekly points; a breakout's last 4 weeks are > 2x the baseline.
    return [10.0] * 22 + ([40.0] * 4 if breakout else [10.0] * 4)


class FakeTrendReq:
    """pytrends.request.TrendReq stand-in: one DataFrame (or error) per batch."""

    breakout_entities: ClassVar[set[str]] = set()
    fail_batches: ClassVar[dict[int, Exception]] = {}
    empty_batches: ClassVar[set[int]] = set()
    calls: ClassVar[int] = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._batch: list[str] = []

    def build_payload(self, batch: list[str], timeframe: str = "") -> None:
        self._batch = list(batch)

    def interest_over_time(self) -> pd.DataFrame:
        idx = FakeTrendReq.calls
        FakeTrendReq.calls += 1
        if idx in FakeTrendReq.fail_batches:
            raise FakeTrendReq.fail_batches[idx]
        if idx in FakeTrendReq.empty_batches:
            return pd.DataFrame()
        dates = pd.date_range("2026-05-10", periods=26, freq="W")
        return pd.DataFrame(
            {e: _series(e in FakeTrendReq.breakout_entities) for e in self._batch}, index=dates
        )


WATCHLIST = {f"Entity{i}": f"Entity_{i}" for i in range(12)}  # 3 batches of 5/5/2


@pytest.fixture()
def trends(monkeypatch: pytest.MonkeyPatch) -> Any:
    FakeTrendReq.breakout_entities = {"Entity3"}
    FakeTrendReq.fail_batches = {}
    FakeTrendReq.empty_batches = set()
    FakeTrendReq.calls = 0
    fake_mod = types.ModuleType("pytrends.request")
    fake_mod.TrendReq = FakeTrendReq  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pytrends", types.ModuleType("pytrends"))
    monkeypatch.setitem(sys.modules, "pytrends.request", fake_mod)
    import ingestion.altdata.google_trends as gt
    import ingestion.altdata.wikipedia_text as wt

    monkeypatch.setattr(wt, "WATCHLIST", WATCHLIST)
    monkeypatch.setattr(gt.time, "sleep", lambda _s: None)
    p = gt.GoogleTrendsPuller.__new__(gt.GoogleTrendsPuller)
    p.engine = _anomaly_engine()
    p.source_id = 119
    return p


def _stored(p: Any) -> list[tuple]:
    with p.engine.connect() as conn:
        return conn.execute(text(
            "SELECT entity_name, anomaly_date, source FROM attention_anomaly"
        )).fetchall()


class TestGoogleTrendsWatchlist:
    def test_pre_fix_return_shape_is_what_the_wrapper_rejected(self) -> None:
        # The 2026-11-08 03:18Z return: one breakout committed, no count.
        old = {"entities_pulled": 48, "breakouts": [{"entity": "Jensen Huang"}]}
        assert _classify_outcome(old) == (
            "FAILED", None, "puller did not report committed write count")

    def test_full_scan_reports_committed_rows(self, trends: Any) -> None:
        out = trends.pull_watchlist_breakouts()
        assert out["status"] == "SUCCESS"
        assert (out["rows_inserted"], out["succeeded"], out["total"]) == (1, 12, 12)
        assert _classify_outcome(out) == ("SUCCESS", 1, None)
        assert _stored(trends) == [("Entity3", date.today().isoformat(), "trends")]  # noqa: DTZ011 (puller uses local date)

    def test_same_day_rerun_writes_nothing_and_is_no_new_data(self, trends: Any) -> None:
        trends.pull_watchlist_breakouts()
        FakeTrendReq.calls = 0
        out = trends.pull_watchlist_breakouts()
        assert out["rows_inserted"] == 0 and len(out["breakouts"]) == 1
        assert _classify_outcome(out) == ("NO_NEW_DATA", 0, "run completed, 0 rows written")
        assert len(_stored(trends)) == 1

    def test_no_breakouts_is_a_completed_zero_write_check(self, trends: Any) -> None:
        FakeTrendReq.breakout_entities = set()
        out = trends.pull_watchlist_breakouts()
        assert _classify_outcome(out) == ("NO_NEW_DATA", 0, "run completed, 0 rows written")

    def test_failed_batch_is_partial_with_committed_rows(self, trends: Any) -> None:
        FakeTrendReq.fail_batches = {2: RuntimeError("connection reset")}
        out = trends.pull_watchlist_breakouts()
        assert out["status"] == "PARTIAL" and out["succeeded"] == 10
        assert _classify_outcome(out)[:2] == ("PARTIAL", 1)
        assert "connection reset" in out["error"]

    def test_empty_batch_is_not_a_clean_check(self, trends: Any) -> None:
        FakeTrendReq.empty_batches = {1}
        out = trends.pull_watchlist_breakouts()
        assert _classify_outcome(out)[:2] == ("PARTIAL", 1)

    def test_rate_limit_stops_the_remaining_batches(self, trends: Any) -> None:
        FakeTrendReq.fail_batches = {1: RuntimeError("The request failed: Google returned a response with code 429")}
        out = trends.pull_watchlist_breakouts()
        assert FakeTrendReq.calls == 2  # third batch never requested
        assert out["status"] == "PARTIAL" and out["succeeded"] == 5
        assert "2 entities not attempted" in out["error"]
        assert _classify_outcome(out)[:2] == ("PARTIAL", 1)

    def test_nothing_returned_is_failed(self, trends: Any) -> None:
        FakeTrendReq.fail_batches = {i: RuntimeError("boom") for i in range(3)}
        out = trends.pull_watchlist_breakouts()
        assert _classify_outcome(out)[:2] == ("FAILED", 0)
        assert _stored(trends) == []

    def test_missing_pytrends_is_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import ingestion.altdata.google_trends as gt

        monkeypatch.setitem(sys.modules, "pytrends.request", None)
        p = gt.GoogleTrendsPuller.__new__(gt.GoogleTrendsPuller)
        out = p.pull_watchlist_breakouts()
        assert _classify_outcome(out) == ("FAILED", 0, "pytrends not installed")

    def test_grid_scheduler_path_records_rows(self, trends: Any) -> None:
        """run_pull_group raises PullerReportedFailure only on FAILED."""
        outcome, rows, _note = _classify_outcome(trends.pull())
        assert outcome != "FAILED" and rows == 1


# ── OpenCorporates missing token → pull_log skipped_config ──────────────


def _pull_log_engine() -> Any:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE pull_log (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "puller_name TEXT NOT NULL, source_id INTEGER, started_at TIMESTAMP NOT NULL, "
            "completed_at TIMESTAMP, status TEXT NOT NULL, rows_inserted INTEGER DEFAULT 0, "
            "rows_expected INTEGER, error_message TEXT, node_name TEXT)"
        ))
    return engine


class TestOpenCorporatesMissingToken:
    def test_reason_uses_the_scheduler_missing_key_prefix(self) -> None:
        from ingestion.altdata.opencorporates import _NO_TOKEN_REASON

        assert _NO_TOKEN_REASON.startswith(MISSING_API_KEY_PREFIX + "OPENCORPORATES_API_TOKEN")

    def test_smart_scheduler_logs_the_skip_as_skipped_config(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from ingestion import base
        from ingestion.source_freshness import RUN_SKIPPED, run_outcome

        def _init(self: Any, db_engine: Any) -> None:
            self.engine = db_engine
            self.source_id = 512

        monkeypatch.setattr(base.BasePuller, "__init__", _init)
        monkeypatch.delenv("OPENCORPORATES_API_TOKEN", raising=False)
        engine = _pull_log_engine()
        sched = SmartScheduler.__new__(SmartScheduler)
        sched.engine = engine
        sched._catalog_ids = {"opencorporates": 512}
        sched._state = {}
        sched._thread_semaphore = threading.Semaphore(1)
        sched._active_threads = set()
        sched._threads_lock = threading.Lock()
        sched._orphan_thread_count = 0
        entry = {"name": "opencorporates", "mod": "ingestion.altdata.opencorporates",
                 "cls": "OpenCorporatesPuller", "method": "pull_all", "timeout_s": 10}
        import ingestion.altdata.opencorporates as oc

        with patch.object(oc.requests, "get") as get:
            result = sched._run_puller(entry)
        get.assert_not_called()
        assert result["status"] == "SKIPPED"
        sched._log_run("opencorporates", datetime.now(timezone.utc), result)
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT puller_name, source_id, status, rows_inserted, error_message FROM pull_log"
            )).fetchall()
        assert len(rows) == 1
        name, sid, status, n, msg = rows[0]
        assert (name, sid, status, n) == (SMART_PULL_LOG_PREFIX + "opencorporates", 512, "SUCCESS", 0)
        assert msg.startswith("SKIPPED: Missing API key: OPENCORPORATES_API_TOKEN")
        # pipeline-health reads this row as a config skip, not a failure.
        assert run_outcome(status, n, msg) == RUN_SKIPPED
