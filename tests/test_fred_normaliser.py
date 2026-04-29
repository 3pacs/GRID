"""Regression tests for FRED observation-frame normalisation.

These pin the contract that produced the 2,010-row error-log flood
documented in PR #61: ``fedfred`` returned frames with the actual
observation date in the ``DataFrame.index`` while a ``date`` column held
the realtime vintage date. The normaliser was failing silently, the
caller raised ``KeyError('date')``, and ``loguru`` logged the bare
repr — which produced one ERROR per series per cycle.

We never want to fall back into that hole, so each shape ``fedfred`` is
known to emit lives in a test below. New shapes that cause silent
failure should add a case here before the production fix.
"""

from __future__ import annotations

import pandas as pd
import pytest

# fedfred is required to import ``ingestion.fred`` — skip the whole
# module when it isn't installed (e.g. minimal CI lanes) rather than
# failing collection.
pytest.importorskip("fedfred")

from ingestion.fred import _normalise_observation_frame  # noqa: E402


@pytest.mark.unit
class TestFREDNormaliserShapes:
    """Each shape ``fedfred`` has emitted in the wild must normalise cleanly."""

    def test_datetime_index_with_value_column(self):
        """Canonical shape: DatetimeIndex + ``value`` column."""
        df = pd.DataFrame(
            {"value": [1.1, 2.2, 3.3]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        )
        out = _normalise_observation_frame(df, "TEST")
        assert out is not None
        assert list(out.columns) == ["date", "value"]
        assert len(out) == 3
        assert out["value"].iloc[0] == pytest.approx(1.1)

    def test_date_column_and_value_column(self):
        """Plain ``date`` + ``value`` columns survive untouched."""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "value": [10.0, 20.0],
        })
        out = _normalise_observation_frame(df, "TEST")
        assert out is not None
        assert len(out) == 2

    def test_observation_date_alias(self):
        """``observation_date`` is recognised as the date column."""
        df = pd.DataFrame({
            "observation_date": ["2024-01-01"],
            "value": [42.0],
        })
        out = _normalise_observation_frame(df, "TEST")
        assert out is not None
        assert out["value"].iloc[0] == pytest.approx(42.0)

    def test_series_id_as_value_column(self):
        """fedfred sometimes uses the series ID as the value-column header."""
        df = pd.DataFrame(
            {"DGS10": [3.5, 3.6]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        out = _normalise_observation_frame(df, "DGS10")
        assert out is not None
        assert out["value"].iloc[1] == pytest.approx(3.6)

    def test_realtime_columns_are_excluded_from_value_fallback(self):
        """``realtime_start`` / ``realtime_end`` are never picked as the value."""
        df = pd.DataFrame({
            "realtime_start": ["2024-01-01"],
            "realtime_end": ["2024-01-01"],
            "value": [99.0],
        }, index=pd.to_datetime(["2024-01-01"]))
        out = _normalise_observation_frame(df, "TEST")
        assert out is not None
        assert out["value"].iloc[0] == pytest.approx(99.0)

    def test_named_index_aliases(self):
        """A non-DatetimeIndex named ``date`` is parsed and reused."""
        idx = pd.Index(["2024-01-01", "2024-01-02"], name="date")
        df = pd.DataFrame({"value": [1.0, 2.0]}, index=idx)
        out = _normalise_observation_frame(df, "TEST")
        assert out is not None
        assert len(out) == 2

    def test_returns_none_when_no_date_signal(self):
        """A frame with no parseable date returns None — caller handles it."""
        df = pd.DataFrame({"value": [1.0, 2.0]})  # default RangeIndex, no date col
        out = _normalise_observation_frame(df, "TEST")
        assert out is None

    def test_realtime_only_frame_yields_all_nan_values(self):
        """A frame with only realtime columns has value coerced to NaN.

        The caller drops NaN values in pull_series, so the contract is "no
        rows survive" rather than the normaliser returning ``None`` here.
        """
        df = pd.DataFrame(
            {"realtime_start": ["2024-01-01"], "realtime_end": ["2024-01-01"]},
            index=pd.to_datetime(["2024-01-01"]),
        )
        out = _normalise_observation_frame(df, "TEST")
        assert out is not None
        assert out["value"].isna().all()

    def test_integer_index_does_not_corrupt_dates(self):
        """A RangeIndex is never coerced into the date column."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "value": [1.0],
        })
        out = _normalise_observation_frame(df, "TEST")
        assert out is not None
        # Make sure the integer RangeIndex didn't override the date column
        first_date = pd.Timestamp(out["date"].iloc[0])
        assert first_date.year == 2024
