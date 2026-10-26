"""FullYieldCurvePuller FRED observation parsing against the real fedfred shape.

Regression for the grid-hermes run of 2026-10-20 04:19-04:21Z, where every
``smart:yield_curve_full`` series ended ``Invalid FRED observation date or
value`` after ``FRED DGS1: 260 values coerced to NaN`` (ingestion.base
wrapper:275 / yield_curve_full._pull_raw_series:259). fedfred 3.x returns
the observation date as the frame index (fedfred/helpers.py ``to_pd_df``);
the puller built its "date" column from that index and then assigned the
index-bearing value Series onto a RangeIndex frame, so pandas aligned on
index and every value became NaN.

These tests use real pandas (the AST-projection suite in
``test_puller_outcome_contracts.py`` uses an inert fake and cannot see
index alignment).
"""

from __future__ import annotations

import math
import sys
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingestion.altdata.yield_curve_full import FullYieldCurvePuller

# Raw FRED ``series/observations`` payload shape, including the "." the API
# emits for a daily series on a market holiday (2026-10-12 Columbus Day).
_OBSERVATIONS = [
    {
        "realtime_start": "2026-10-20",
        "realtime_end": "2026-10-20",
        "date": "2026-10-09",
        "value": "4.12",
    },
    {
        "realtime_start": "2026-10-20",
        "realtime_end": "2026-10-20",
        "date": "2026-10-12",
        "value": ".",
    },
    {
        "realtime_start": "2026-10-20",
        "realtime_end": "2026-10-20",
        "date": "2026-10-13",
        "value": "4.10",
    },
    {
        "realtime_start": "2026-10-20",
        "realtime_end": "2026-10-20",
        "date": "2026-10-14",
        "value": "4.07",
    },
]


def _fedfred_frame(observations: list[dict[str, str]]) -> pd.DataFrame:
    """Replica of fedfred.helpers.FredHelpers.to_pd_df (fedfred 3.0.0, L59-62)."""
    df = pd.DataFrame(observations)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _install_fake_fred(monkeypatch, frame: pd.DataFrame) -> SimpleNamespace:
    calls = SimpleNamespace(count=0)

    class _FakeFred:
        def __init__(self, _api_key):
            pass

        def get_series_observations(self, *_args, **_kwargs):
            calls.count += 1
            return frame

    monkeypatch.setitem(sys.modules, "fedfred", SimpleNamespace(FredAPI=_FakeFred))
    # retry_on_failure sleeps between attempts; keep the suite fast.
    monkeypatch.setattr("time.sleep", lambda _s: None)
    return calls


def _puller() -> FullYieldCurvePuller:
    puller = FullYieldCurvePuller.__new__(FullYieldCurvePuller)
    puller._api_key = "test"
    puller.source_id = 1
    return puller


def test_replica_matches_installed_fedfred_shape():
    fedfred = pytest.importorskip("fedfred")
    from fedfred.helpers import FredHelpers

    real = FredHelpers.to_pd_df({"observations": _OBSERVATIONS})
    replica = _fedfred_frame(_OBSERVATIONS)
    assert isinstance(real.index, pd.DatetimeIndex) and real.index.name == "date", (
        fedfred.__version__
    )
    pd.testing.assert_frame_equal(real, replica)


def test_indexed_frame_with_missing_marker_keeps_valid_rows(monkeypatch):
    calls = _install_fake_fred(monkeypatch, _fedfred_frame(_OBSERVATIONS))

    frame = _puller()._fetch_fred_series("DGS1", date(2026, 10, 9), date(2026, 10, 14))

    assert calls.count == 1, "valid payload must not be retried"
    assert list(frame.columns) == ["date", "value"]
    assert list(frame["date"].dt.date) == [
        date(2026, 10, 9),
        date(2026, 10, 13),
        date(2026, 10, 14),
    ]
    assert list(frame["value"]) == [4.12, 4.10, 4.07]
    assert all(math.isfinite(v) for v in frame["value"])


def test_pull_raw_series_writes_only_observed_rows(monkeypatch):
    _install_fake_fred(monkeypatch, _fedfred_frame(_OBSERVATIONS))
    puller = _puller()
    puller.engine = MagicMock()
    puller._row_exists = lambda *_a: False
    inserted: list[tuple[date, float]] = []

    def _insert_raw(conn, series_id, obs_date, value, raw_payload, return_result=False):
        inserted.append((obs_date, value))
        return SimpleNamespace(rowcount=1)

    puller._insert_raw = _insert_raw

    result = puller._pull_raw_series(
        "yc_1y", "DGS1", date(2026, 10, 9), date(2026, 10, 14)
    )

    assert result == {"feature": "yc_1y", "status": "SUCCESS", "rows_inserted": 3}
    assert inserted == [
        (date(2026, 10, 9), 4.12),
        (date(2026, 10, 13), 4.10),
        (date(2026, 10, 14), 4.07),
    ]
    # The holiday "." row is skipped, never written as 0.
    assert all(obs_date != date(2026, 10, 12) for obs_date, _ in inserted)
    assert all(value != 0 for _, value in inserted)


def test_all_missing_is_failure_not_clean(monkeypatch):
    all_missing = [dict(row, value=".") for row in _OBSERVATIONS]
    calls = _install_fake_fred(monkeypatch, _fedfred_frame(all_missing))
    puller = _puller()
    puller.engine = MagicMock()

    with pytest.raises(ValueError, match="Invalid FRED observation date or value"):
        puller._fetch_fred_series("DGS1", date(2026, 10, 9), date(2026, 10, 14))
    assert calls.count == 3, "retry wrapper still exhausts attempts on a bad payload"

    result = puller._pull_raw_series(
        "yc_1y", "DGS1", date(2026, 10, 9), date(2026, 10, 14)
    )
    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert "Invalid FRED observation date or value" in result["error"]
    puller.engine.begin.assert_not_called()


def test_non_finite_row_is_skipped_but_series_survives(monkeypatch):
    frame = _fedfred_frame(_OBSERVATIONS)
    frame.loc[pd.Timestamp("2026-10-13"), "value"] = float("inf")
    _install_fake_fred(monkeypatch, frame)

    out = _puller()._fetch_fred_series("DGS1", date(2026, 10, 9), date(2026, 10, 14))

    assert list(out["date"].dt.date) == [date(2026, 10, 9), date(2026, 10, 14)]
    assert list(out["value"]) == [4.12, 4.07]


def test_legacy_date_column_shape_unchanged(monkeypatch):
    legacy = pd.DataFrame(
        {"date": ["2026-10-09", "2026-10-13"], "value": ["4.12", "4.10"]}
    )
    _install_fake_fred(monkeypatch, legacy)

    out = _puller()._fetch_fred_series("DGS1", date(2026, 10, 9), date(2026, 10, 13))

    assert list(out["date"].dt.date) == [date(2026, 10, 9), date(2026, 10, 13)]
    assert list(out["value"]) == [4.12, 4.10]


def test_valid_empty_frame_is_no_new_data(monkeypatch):
    _install_fake_fred(monkeypatch, pd.DataFrame(columns=["date", "value"]))
    puller = _puller()
    puller.engine = MagicMock()

    result = puller._pull_raw_series(
        "yc_1y", "DGS1", date(2026, 10, 9), date(2026, 10, 14)
    )

    assert result == {"feature": "yc_1y", "status": "NO_NEW_DATA", "rows_inserted": 0}
    puller.engine.begin.assert_not_called()
