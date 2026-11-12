"""Unit tests for compute_derived_features.changed_rows (2026-11-11).

The builder used to re-insert its entire computed history as a new vintage
on every run. Now that it runs daily, only new or changed obs_dates are
written; an unchanged history is a no-op.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scripts.compute_derived_features import changed_rows


def _s(values: dict[str, float]) -> pd.Series:
    return pd.Series(list(values.values()), index=pd.DatetimeIndex(list(values)), dtype=float)


@pytest.mark.unit
def test_nothing_on_file_writes_everything() -> None:
    computed = _s({"2026-11-09": 1.0, "2026-11-10": 2.0})
    assert changed_rows(computed, pd.Series(dtype=float)).equals(computed)


@pytest.mark.unit
def test_unchanged_history_writes_only_the_new_day() -> None:
    existing = _s({"2026-11-09": 1.0, "2026-11-10": 2.0})
    computed = _s({"2026-11-09": 1.0, "2026-11-10": 2.0, "2026-11-11": 3.0})
    out = changed_rows(computed, existing)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-11-11"]


@pytest.mark.unit
def test_revised_value_is_written() -> None:
    existing = _s({"2026-11-09": 1.0, "2026-11-10": 2.0})
    computed = _s({"2026-11-09": 1.0, "2026-11-10": 2.5})
    out = changed_rows(computed, existing)
    assert out.to_dict() == {pd.Timestamp("2026-11-10"): 2.5}


@pytest.mark.unit
def test_float_noise_is_not_a_change() -> None:
    existing = _s({"2026-11-10": 0.1 + 0.2})
    computed = _s({"2026-11-10": 0.3})
    assert changed_rows(computed, existing).empty
