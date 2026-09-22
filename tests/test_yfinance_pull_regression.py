"""Regression tests for ingestion/yfinance_pull.py.

Locks in the fixes for the 2026-04-08 incident where yfinance's MultiIndex
flattening leaked a header string ("Open") into the DataFrame index and the
puller happily fed it to PostgreSQL as obs_date:

    (psycopg2.errors.InvalidDatetimeFormat) invalid input syntax for type
    date: "Open"
    [parameters: {'sid': 'YF:TLT:open', 'src': 2, 'od': 'Open', ...}]
"""

from __future__ import annotations

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

from datetime import date
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


@pytest.mark.xfail(strict=True, reason="Existing-date de-dup freezes an earlier SPY daily close")
def test_spy_daily_close_repull_is_not_frozen_by_earlier_value(engine_recording_inserts):
    """Characterize the provisional-close freeze without choosing its repair."""
    from ingestion import yfinance_pull

    engine, _conn = engine_recording_inserts
    obs_date = date(2026, 9, 22)
    early = pd.DataFrame({"Close": [680.0]}, index=pd.DatetimeIndex([pd.Timestamp(obs_date)]))
    completed = pd.DataFrame({"Close": [685.0]}, index=pd.DatetimeIndex([pd.Timestamp(obs_date)]))

    with patch.object(yfinance_pull.YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(yfinance_pull.YFinancePuller, "_get_existing_dates", side_effect=[set(), {obs_date}]), \
         patch.object(yfinance_pull.yf, "download", side_effect=[early, completed]):
        puller = yfinance_pull.YFinancePuller(engine)
        first = puller.pull_ticker("SPY", start_date=obs_date)
        second = puller.pull_ticker("SPY", start_date=obs_date)

    assert first["rows_inserted"] == 1
    assert second["outcome"] != "duplicate_only"


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
