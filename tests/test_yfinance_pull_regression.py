"""Regression tests for ingestion/yfinance_pull.py.

Locks in the fixes for the 2026-04-08 incident where yfinance's MultiIndex
flattening leaked a header string ("Open") into the DataFrame index and the
puller happily fed it to PostgreSQL as obs_date:

    (psycopg2.errors.InvalidDatetimeFormat) invalid input syntax for type
    date: "Open"
    [parameters: {'sid': 'YF:TLT:open', 'src': 2, 'od': 'Open', ...}]
"""

from __future__ import annotations

import json
import sys
import types

# Some sandboxed environments cannot build `multitasking` (C extension).
# yfinance imports it unconditionally at module load — install a minimal
# shim before it is imported so these tests are runnable anywhere.
if "multitasking" not in sys.modules:
    _shim = types.ModuleType("multitasking")
    _shim.task = lambda f: f
    _shim.set_max_threads = lambda n: None
    _shim.wait_for_tasks = lambda *a, **k: None
    sys.modules["multitasking"] = _shim

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, create_autospec, patch

import pandas as pd
import pytest
from sqlalchemy.engine import Engine


@pytest.fixture
def engine_recording_inserts():
    """Engine whose .begin() context manager records every execute() call."""
    engine = create_autospec(Engine, instance=True)
    conn = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = (1,)
    result.fetchall.return_value = []
    conn.execute.return_value = result
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _make_poisoned_frame() -> pd.DataFrame:
    """Frame that reproduces the TLT bug: duplicate columns + non-date index."""
    frame = pd.DataFrame(
        {
            "Open": [87.35, 87.40],
            "High": [87.50, 87.60],
            "Low": [87.20, 87.25],
            "Close": [87.45, 87.55],
            "Volume": [1_000_000, 1_100_000],
            "Adj Close": [87.45, 87.55],
        },
        index=pd.Index(["Open", pd.Timestamp("2026-04-08")], name="Date"),
    )
    return frame


def test_string_index_entries_never_reach_obs_date(engine_recording_inserts):
    """A header string leaked into the index must not be inserted as a date."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=_make_poisoned_frame()):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker("TLT", start_date="2026-04-01")

    # Inspect every recorded INSERT and make sure no obs_date is a string.
    offending = []
    for call in conn.execute.call_args_list:
        if len(call.args) < 2:
            continue
        params = call.args[1]
        if not isinstance(params, dict):
            continue
        od = params.get("od")
        if od is None:
            continue
        if isinstance(od, str):
            offending.append(od)
        else:
            # Must be a date (not a Timestamp, not a string, not None).
            assert isinstance(od, date), f"obs_date has wrong type: {type(od)} ({od!r})"

    assert not offending, (
        f"obs_date received string values — regression of the TLT bug: {offending}"
    )
    assert result["status"] in ("SUCCESS", "PARTIAL")


def test_spy_daily_close_repull_is_not_frozen_by_earlier_value(engine_recording_inserts):
    """A provisional daily value cannot block a later post-period capture."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    obs_date = date(2026, 9, 22)
    early = pd.DataFrame({"Close": [680.0]}, index=pd.DatetimeIndex([pd.Timestamp(obs_date)]))
    completed = pd.DataFrame({"Close": [685.0]}, index=pd.DatetimeIndex([pd.Timestamp(obs_date)]))

    def execute(statement, params=None):
        result = MagicMock()
        result.fetchone.return_value = None
        result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", side_effect=[set(), {obs_date}]), \
         patch.object(yfinance_pull, "_utc_now", side_effect=[
             datetime(2026, 9, 22, 13, 33, tzinfo=timezone.utc),
             datetime(2026, 9, 23, 0, 30, tzinfo=timezone.utc),
         ]), \
         patch.object(yfinance_pull.yf, "download", side_effect=[early, completed]):
        puller = yfinance_pull.YFinancePuller(engine)
        first = puller.pull_ticker("SPY", start_date=obs_date)
        second = puller.pull_ticker("SPY", start_date=obs_date)

    assert first["rows_inserted"] == 0
    assert second["rows_inserted"] == 1
    assert second["outcome"] == "inserted"
    marked = [c.args[1] for c in conn.execute.call_args_list
              if "INSERT INTO raw_series" in str(c.args[0])]
    assert len(marked) == 1
    assert marked[0]["sid"] == "YF:SPY:close"
    assert marked[0]["val"] == 685.0


def test_bounded_completed_spy_close_writes_only_marked_requested_day(engine_recording_inserts):
    """Today's scheduler start can fetch yesterday without other OHLCV writes."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [680.0, 684.0, 687.0],
         "Close": [681.0, 685.0, 688.0],
         "Adj Close": [681.0, 685.0, 688.0]},
        index=pd.DatetimeIndex([
            pd.Timestamp("2026-09-21"),
            pd.Timestamp("2026-09-22"),
            pd.Timestamp("2026-09-23"),
        ]),
    )
    conn.execute.return_value.fetchone.return_value = None
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull, "_utc_now", return_value=datetime(
             2026, 9, 23, 13, 30, tzinfo=timezone.utc)), \
         patch.object(yfinance_pull.yf, "download", return_value=frame) as download:
        result = yfinance_pull.YFinancePuller(engine).pull_ticker(
            "SPY", start_date=date(2026, 9, 22), end_date=date(2026, 9, 23),
            interval="1d", only_fields=frozenset({"close"}),
        )

    assert result["rows_inserted"] == 1
    assert result["outcome"] == "inserted"
    download.assert_called_once()
    assert download.call_args.kwargs["start"] == "2026-09-22"
    assert download.call_args.kwargs["end"] == "2026-09-23"
    assert download.call_args.kwargs["auto_adjust"] is False
    inserted = [call.args[1] for call in conn.execute.call_args_list
                if "INSERT INTO raw_series" in str(call.args[0])]
    assert len(inserted) == 1
    assert inserted[0]["sid"] == "YF:SPY:close"
    assert inserted[0]["od"] == date(2026, 9, 22)
    assert inserted[0]["val"] == 685.0
    assert json.loads(inserted[0]["payload"]) == {
        "price_contract_version": "spy_close_v1",
        "capture_policy": "post_utc_day_end_v1",
        "price_basis": "YF:SPY:close",
        "interval": "1d",
        "obs_date": "2026-09-22",
        "provider_certified_final": False,
    }


def test_duplicate_columns_dont_iterate_column_names(engine_recording_inserts):
    """MultiIndex flattening producing duplicate column headers must not
    turn `df[col].items()` into a column-name iteration (which was the root
    cause of the 'Open' obs_date poisoning)."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts

    # Duplicate "Open" columns simulate what level-0 flattening gives you
    # when yfinance returns ((Open, TLT), (Open, TLT)) for some reason.
    frame = pd.DataFrame(
        [[87.35, 87.36]],
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-08")], name="Date"),
        columns=pd.Index(["Open", "Open"]),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        puller.pull_ticker("TLT", start_date="2026-04-01")

    for call in conn.execute.call_args_list:
        if len(call.args) < 2:
            continue
        params = call.args[1]
        if not isinstance(params, dict):
            continue
        od = params.get("od")
        if od is not None:
            assert not isinstance(od, str), f"obs_date={od!r} should never be a string"


def test_existing_dates_check_is_bounded_to_the_requested_window(engine_recording_inserts):
    """The dedup lookup must not scan a series' entire history to answer a
    question only about the window this call actually fetched.

    On a series with a very long, heavily-attempted pull history (millions
    of rows for a single series_id/source_id), the previously-unbounded
    `SELECT DISTINCT obs_date ... WHERE series_id = ... AND source_id = ...`
    scanned every row ever inserted just to de-duplicate a handful of newly
    fetched dates, and was slow enough to hit the statement timeout. Since
    the fetched frame can only ever contain dates inside
    [start_date, end_date] (yf.download() itself bounds the response), the
    lookup only needs to cover that same window.
    """
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(
             yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()
         ) as mock_get_existing, \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        puller.pull_ticker("^DJI", start_date="2026-09-11", end_date="2026-09-14")

    assert mock_get_existing.called, "_get_existing_dates must still be called"
    _, kwargs = mock_get_existing.call_args
    assert kwargs.get("start_date") == date(2026, 9, 11)
    # One day before the requested end_date: yfinance's own `end` is
    # exclusive (see test_existing_dates_end_bound_matches_yfinances_exclusive_end),
    # so 09-14 requested means col_data can contain at most through 09-13.
    assert kwargs.get("end_date") == date(2026, 9, 13)


def test_existing_dates_check_leaves_end_open_when_end_date_omitted(engine_recording_inserts):
    """`end_date=None` means 'through today' upstream — the existence check
    must not invent an artificial upper bound that could hide a date the
    fetch actually returned."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(
             yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()
         ) as mock_get_existing, \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        puller.pull_ticker("^DJI", start_date="2026-09-11")

    _, kwargs = mock_get_existing.call_args
    assert kwargs.get("start_date") == date(2026, 9, 11)
    assert kwargs.get("end_date") is None


def test_existing_dates_end_bound_matches_yfinances_exclusive_end(engine_recording_inserts):
    """yfinance's own `end` is exclusive — confirmed against the live API:
    start="2026-09-10", end="2026-09-11" returns only 09-10, never 09-11.
    _get_existing_dates's end_date is a normal inclusive bound, so the value
    passed through must be one day before the requested end_date, not
    end_date itself — otherwise the existence check would include a day
    col_data can never contain, silently widening the scan for no benefit
    (harmless today, but not what "bounded to the fetch window" should mean).
    """
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-10")], name="Date"),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(
             yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()
         ) as mock_get_existing, \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        puller.pull_ticker("^DJI", start_date="2026-09-10", end_date="2026-09-11")

    _, kwargs = mock_get_existing.call_args
    assert kwargs.get("start_date") == date(2026, 9, 10)
    assert kwargs.get("end_date") == date(2026, 9, 10), (
        "end_date passed to _get_existing_dates must be one day before the "
        "requested end_date (yfinance excludes the end date itself)"
    )


def test_pull_all_backfill_still_passes_a_wide_open_ended_bound(engine_recording_inserts):
    """`pull_all()` never passes an end_date (its signature has none) — a
    backfill (`backfill_all(start_date="1970-01-01")`, or this module's own
    `__main__` calling `pull_all(start_date="2020-01-01")`) must still reach
    _get_existing_dates with that same wide start_date and an open end, not
    something narrowed to "recent days". The bound only ever tightens the
    routine daily-schedule case (start_date=today); a broad backfill request
    stays exactly as broad as it asks to be.
    """
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2020-01-02")], name="Date"),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(
             yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()
         ) as mock_get_existing, \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        puller.pull_all(ticker_list=["^DJI"], start_date="2020-01-01")

    _, kwargs = mock_get_existing.call_args
    assert kwargs.get("start_date") == date(2020, 1, 1)
    assert kwargs.get("end_date") is None


# ─── Freshness-semantics review (fable-hermes-repair-bound follow-up,
# 2026-09-19): per-ticker outcome classification and the bounded timeout
# passed to yf.download when the installed yfinance version supports it.


def test_outcome_is_duplicate_only_when_all_dates_already_exist(engine_recording_inserts):
    """A non-empty download whose every date is already present is a
    successful CHECK (0 rows inserted, status SUCCESS) — not a "no_data"
    or error outcome."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value={date(2026, 9, 11)}), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker("SPY", start_date="2026-09-11")

    assert result["rows_inserted"] == 0
    assert result["status"] == "SUCCESS"
    assert result["outcome"] == "duplicate_only"


def test_outcome_is_no_data_when_provider_returns_empty_frame(engine_recording_inserts):
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.yf, "download", return_value=pd.DataFrame()):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker("ZZZZ", start_date="2026-09-11")

    assert result["status"] == "PARTIAL"
    assert result["outcome"] == "no_data"


def test_outcome_is_inserted_when_new_rows_land(engine_recording_inserts):
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker("SPY", start_date="2026-09-11")

    assert result["rows_inserted"] > 0
    assert result["outcome"] == "inserted"


def test_outcome_is_error_for_invalid_ticker(engine_recording_inserts):
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker("N/A", start_date="2026-09-11")

    assert result["status"] == "SKIPPED"
    assert result["outcome"] == "error"


def test_download_receives_bounded_timeout_when_supported(engine_recording_inserts):
    """Check 2a: pull_ticker passes a bounded `timeout` to yf.download when
    the installed yfinance version's signature accepts one."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )
    captured: dict = {}

    def _fake_download(ticker, **kwargs):
        captured.update(kwargs)
        return frame

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull, "_YF_DOWNLOAD_ACCEPTS_TIMEOUT", True), \
         patch.object(yfinance_pull.yf, "download", side_effect=_fake_download):
        puller = yfinance_pull.YFinancePuller(engine)
        puller.pull_ticker("SPY", start_date="2026-09-11")

    assert captured.get("timeout") == yfinance_pull._YF_DOWNLOAD_TIMEOUT_SECONDS


def test_download_omits_timeout_when_unsupported(engine_recording_inserts):
    """If the installed yfinance version's yf.download() doesn't accept a
    `timeout` kwarg, pull_ticker must not pass one (would raise TypeError)."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )
    captured: dict = {}

    def _fake_download(ticker, **kwargs):
        captured.update(kwargs)
        return frame

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull, "_YF_DOWNLOAD_ACCEPTS_TIMEOUT", False), \
         patch.object(yfinance_pull.yf, "download", side_effect=_fake_download):
        puller = yfinance_pull.YFinancePuller(engine)
        puller.pull_ticker("SPY", start_date="2026-09-11")

    assert "timeout" not in captured


def test_pull_all_single_flight_skips_when_already_running(engine_recording_inserts):
    """Fix #1 (GRID-YF-CLOSE-REPAIR-20260926): a second concurrent
    pull_all() must skip entirely — not even the first ticker attempted —
    when a previous run's single-flight lock is still held (simulating an
    orphaned thread the scheduler abandoned after a timeout)."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.yf, "download") as mock_download:
        puller = yfinance_pull.YFinancePuller(engine)
        acquired = yfinance_pull._PULL_ALL_LOCK.acquire(blocking=False)
        assert acquired, "test setup: lock should be free before this test runs"
        try:
            result = puller.pull_all(ticker_list=["AAA", "BBB"], start_date="2026-09-11")
        finally:
            yfinance_pull._PULL_ALL_LOCK.release()

    assert result == []
    mock_download.assert_not_called()


def test_pull_all_single_flight_skip_reports_dict_shape_with_should_continue(engine_recording_inserts):
    """When called with should_continue (the shape hermes_fixers._retry_source
    consumes), a lock-skip must report stopped_by_budget=True — that's what
    the caller uses to decide NOT to advance source_catalog.last_pull_at."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.yf, "download") as mock_download:
        puller = yfinance_pull.YFinancePuller(engine)
        yfinance_pull._PULL_ALL_LOCK.acquire(blocking=False)
        try:
            result = puller.pull_all(
                ticker_list=["AAA", "BBB"],
                start_date="2026-09-11",
                should_continue=lambda: True,
            )
        finally:
            yfinance_pull._PULL_ALL_LOCK.release()

    assert result["status"] == "SKIPPED"
    assert result["stopped_by_budget"] is True
    assert result["tickers_not_attempted"] == ["AAA", "BBB"]
    assert result["counts"]["unattempted"] == 2
    assert sum(result["counts"].values()) == 2
    mock_download.assert_not_called()


def test_pull_all_releases_lock_after_a_normal_run(engine_recording_inserts):
    """The lock must not leak across calls — a normal (non-orphaned) run
    releases it so the next scheduled tick can proceed."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        first = puller.pull_all(ticker_list=["AAA"], start_date="2026-09-11")
        second = puller.pull_all(ticker_list=["BBB"], start_date="2026-09-11")

    assert isinstance(first, list) and len(first) == 1
    assert isinstance(second, list) and len(second) == 1
    assert not yfinance_pull._PULL_ALL_LOCK.locked()


def test_pull_all_concurrent_calls_only_one_attempts_tickers(engine_recording_inserts):
    """End-to-end proof of single-flight: two REAL concurrent pull_all()
    calls (separate threads, one genuinely in-flight inside yf.download)
    must result in exactly one of them attempting any ticker at all."""
    import threading

    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    entered_first = threading.Event()
    release_first = threading.Event()
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )

    def slow_download(ticker, **kwargs):
        entered_first.set()
        release_first.wait(timeout=5)
        return frame

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", side_effect=slow_download):
        puller = yfinance_pull.YFinancePuller(engine)
        results = {}

        def run_first():
            results["first"] = puller.pull_all(ticker_list=["AAA"], start_date="2026-09-11")

        t1 = threading.Thread(target=run_first)
        t1.start()
        assert entered_first.wait(timeout=5), "first call never started its download"

        # Second call starts while the first is still inside pull_all().
        results["second"] = puller.pull_all(ticker_list=["BBB"], start_date="2026-09-11")
        release_first.set()
        t1.join(timeout=5)

    assert results["second"] == [], "concurrent call must skip — no ticker attempted"
    assert len(results["first"]) == 1
    assert results["first"][0]["ticker"] == "AAA"


def test_multiindex_ticker_mismatch_refuses_whole_ticker(engine_recording_inserts):
    """Fix #3: if the downloaded frame's MultiIndex ticker level doesn't
    match the ticker we requested, refuse the WHOLE ticker instead of
    silently dropping the ticker level and writing data that may belong to
    a different instrument — the confirmed April 2026 mechanism (donor
    tickers like CL=F, GBPUSD=X landing under SPY/QQQ/etc. series ids)."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        [[100.0, 68.5]],
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
        columns=pd.MultiIndex.from_tuples(
            [("Close", "SPY"), ("Close", "CL=F")], names=["Price", "Ticker"]
        ),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker("SPY", start_date="2026-09-11")

    assert result["rows_inserted"] == 0
    assert result["status"] == "SKIPPED"
    assert result["outcome"] == "error"
    assert any("CL=F" in e for e in result["errors"])
    for call in conn.execute.call_args_list:
        assert "INSERT INTO raw_series" not in str(call.args[0] if call.args else "")


def test_multiindex_matching_ticker_still_writes_normally(engine_recording_inserts):
    """Sanity counterpart: a MultiIndex frame whose ticker level DOES match
    the requested ticker must be unaffected by the new guard."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        [[100.0]],
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
        columns=pd.MultiIndex.from_tuples([("Close", "TLT")], names=["Price", "Ticker"]),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker("TLT", start_date="2026-09-11")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 1
    assert result["outcome"] == "inserted"


def test_wild_ratio_guard_refuses_write_for_wrong_instrument_values(engine_recording_inserts):
    """Fix #4: refuse to write when a series' freshly downloaded values
    disagree wildly (median ratio outside [0.5, 2]) with that exact
    series' own existing recent SUCCESS values on overlapping dates — cheap
    defense against a wrong-instrument frame that otherwise looks
    well-formed (single ticker column, valid dates, numeric values)."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    # Recent (within _WILD_RATIO_LOOKBACK_DAYS), relative to the real clock
    # rather than a hardcoded date — the guard's own lookup is now bounded
    # to "recent" (see _median_ratio_vs_existing's docstring).
    dates = [date.today() - timedelta(days=17 - i) for i in range(8)]
    frame = pd.DataFrame(
        # ~30-37: plausible for some instrument, wildly off vs the
        # existing ~680-687 SPY-scale values on the very same dates.
        {"Close": [30.0 + i for i in range(len(dates))]},
        index=pd.DatetimeIndex([pd.Timestamp(d) for d in dates], name="Date"),
    )
    existing_rows = [(d, 680.0 + i) for i, d in enumerate(dates)]

    def execute(statement, params=None):
        result = MagicMock()
        if "SELECT obs_date, value FROM raw_series" in str(statement):
            result.fetchall.return_value = existing_rows
        else:
            result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result

    conn.execute.side_effect = execute

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker(
            "TLT", start_date=dates[0].isoformat(),
            end_date=(dates[-1] + timedelta(days=1)).isoformat(),
        )

    assert result["rows_inserted"] == 0
    assert any("wild median ratio" in e for e in result["errors"])
    for call in conn.execute.call_args_list:
        if len(call.args) >= 2 and "INSERT INTO raw_series" in str(call.args[0]):
            pytest.fail(f"must not write when the wrong-instrument guard trips: {call.args[1]}")


def test_wild_ratio_guard_allows_a_small_normal_revision(engine_recording_inserts):
    """Counterpart: an ordinary close-to-1 ratio (ordinary same-instrument
    values) must not be refused by the wrong-instrument guard."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    dates = [date.today() - timedelta(days=10 - i) for i in range(5)]
    frame = pd.DataFrame(
        {"Close": [680.1, 681.2, 682.0, 683.5, 684.0]},
        index=pd.DatetimeIndex([pd.Timestamp(d) for d in dates], name="Date"),
    )
    existing_rows = [(d, 680.0 + i) for i, d in enumerate(dates)]

    def execute(statement, params=None):
        result = MagicMock()
        if "SELECT obs_date, value FROM raw_series" in str(statement):
            result.fetchall.return_value = existing_rows
        else:
            result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result

    conn.execute.side_effect = execute

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker(
            "TLT", start_date=dates[0].isoformat(),
            end_date=(dates[-1] + timedelta(days=1)).isoformat(),
        )

    assert result["errors"] == []
    assert result["rows_inserted"] == 5


def test_wild_ratio_guard_needs_minimum_overlap_before_judging(engine_recording_inserts):
    """Fewer than _WILD_RATIO_MIN_OVERLAP overlapping dates must not trip
    the guard — a single stale/off-by-one existing row is too noisy."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    recent_date = date.today() - timedelta(days=5)
    frame = pd.DataFrame(
        {"Close": [5.0]},  # wildly different from the one existing row...
        index=pd.DatetimeIndex([pd.Timestamp(recent_date)], name="Date"),
    )
    existing_rows = [(recent_date, 680.0)]  # ...but only 1 overlap.

    def execute(statement, params=None):
        result = MagicMock()
        if "SELECT obs_date, value FROM raw_series" in str(statement):
            result.fetchall.return_value = existing_rows
        else:
            result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result

    conn.execute.side_effect = execute

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_ticker("TLT", start_date=recent_date.isoformat())

    assert result["errors"] == []
    assert result["rows_inserted"] == 1


def test_pull_all_skips_when_pg_advisory_lock_held_by_another_process(engine_recording_inserts):
    """Cross-process guard (coordinator review of PR #672): pg_try_advisory_lock
    returning falsy (another OS process already holds it — grid-scheduler
    and grid-hermes both call pull_all independently) must skip the whole
    run just like the in-process thread lock, release the in-process lock
    again (so this process's own next attempt isn't blocked by it too), and
    must not call pg_advisory_unlock for a lock it never actually took."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    engine.connect.return_value.execute.return_value.scalar.return_value = False

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.yf, "download") as mock_download:
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_all(ticker_list=["AAA"], start_date="2026-09-11")

    assert result == []
    mock_download.assert_not_called()
    assert not yfinance_pull._PULL_ALL_LOCK.locked()
    engine.connect.return_value.close.assert_called()
    unlock_calls = [
        c for c in engine.connect.return_value.execute.call_args_list
        if "pg_advisory_unlock" in str(c.args[0] if c.args else "")
    ]
    assert not unlock_calls, "must not unlock a lock that was never acquired"


def test_pull_all_skip_reason_names_the_cross_process_guard(engine_recording_inserts):
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    engine.connect.return_value.execute.return_value.scalar.return_value = False

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.yf, "download"):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_all(
            ticker_list=["AAA"], start_date="2026-09-11", should_continue=lambda: True,
        )

    assert result["status"] == "SKIPPED"
    assert result["stopped_by_budget"] is True
    assert "cross-process" in result["skipped_reason"]


def test_pull_all_releases_pg_advisory_lock_after_a_normal_run(engine_recording_inserts):
    """When the advisory lock IS acquired, a normal run must release it
    (pg_advisory_unlock) and close that connection afterward — otherwise
    the lock would starve every future pull_all() call, in this process or
    any other, exactly like an orphaned thread starves _PULL_ALL_LOCK."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    engine.connect.return_value.execute.return_value.scalar.return_value = True
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        result = puller.pull_all(ticker_list=["AAA"], start_date="2026-09-11")

    assert len(result) == 1
    unlock_calls = [
        c for c in engine.connect.return_value.execute.call_args_list
        if "pg_advisory_unlock" in str(c.args[0] if c.args else "")
    ]
    assert len(unlock_calls) == 1
    engine.connect.return_value.close.assert_called()


def test_advisory_lock_key_is_deterministic_across_processes(engine_recording_inserts):
    """Python's hash() is randomised per process (PYTHONHASHSEED) — the key
    two separate OS processes need to agree on must come from something
    deterministic instead."""
    from ingestion import yfinance_pull

    key1 = yfinance_pull._stable_advisory_lock_key("grid:yfinance:pull_all")
    key2 = yfinance_pull._stable_advisory_lock_key("grid:yfinance:pull_all")
    assert key1 == key2 == yfinance_pull._PULL_ALL_ADVISORY_LOCK_KEY
    assert isinstance(key1, int)
    assert -(2**63) <= key1 < 2**63  # must fit Postgres's signed bigint


def test_pull_all_with_should_continue_reports_per_outcome_counts(engine_recording_inserts):
    """Check 1a: pull_all's dict-shaped (should_continue given) return
    carries top-level counts per outcome, including "unattempted" for
    tickers never reached because the budget ran out."""
    from ingestion import yfinance_pull

    engine, conn = engine_recording_inserts
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )
    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", side_effect=lambda *a, **kw: set()), \
         patch.object(yfinance_pull.yf, "download", return_value=frame):
        puller = yfinance_pull.YFinancePuller(engine)
        calls = {"n": 0}

        def _should_continue():
            calls["n"] += 1
            return calls["n"] <= 2  # allow tickers 0 and 1, stop before 2

        result = puller.pull_all(
            ticker_list=["AAA", "BBB", "CCC"],
            start_date="2026-09-11",
            should_continue=_should_continue,
        )

    assert isinstance(result, dict)
    assert result["stopped_by_budget"] is True
    assert result["tickers_not_attempted"] == ["CCC"]
    assert result["counts"]["inserted"] == 2
    assert result["counts"]["unattempted"] == 1
    assert sum(result["counts"].values()) == 3
