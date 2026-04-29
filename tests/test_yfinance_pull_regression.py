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
