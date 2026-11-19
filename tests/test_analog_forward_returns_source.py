"""Analog forward returns read one source per series through store.observations.

Reading raw_series by series_id alone mixed sources and vintages; yfinance's
wrong-instrument SUCCESS rows (TLT 414.95 = GLD's price on 2026-03-30) made the
best analog episode show GLD +103% / TLT +88% in 7 days (live, 2026-11-08).
"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from intelligence.regime import episode_matcher as em


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Engine:
    def connect(self):
        return _Conn()


def _obs(start: date, values: list[float]) -> list[SimpleNamespace]:
    return [SimpleNamespace(obs_date=start + timedelta(days=i), value=v) for i, v in enumerate(values)]


@pytest.fixture
def reads(monkeypatch):
    from store import observations

    calls: list[tuple[str, str]] = []
    data: dict[tuple[str, str], list] = {}

    def read_window(conn, series_id, *, source=None, start=None, as_of=None, as_of_ts=None):
        calls.append((series_id, source))
        return data.get((series_id, source), [])

    monkeypatch.setattr(observations, "read_window", read_window)
    monkeypatch.setattr(em, "_FORWARD_TICKERS", {"TLT": "YF:TLT:adj_close", "VIX": "VIXCLS"})
    return calls, data


def test_prefers_tiingo_and_never_mixes_sources(reads) -> None:
    calls, data = reads
    d0 = date(2026, 3, 30)
    data[("YF:TLT:adj_close", "TIINGO")] = _obs(d0, [86.78] + [86.0] * 8)
    data[("YF:TLT:close", "yfinance")] = _obs(d0, [46.04] + [414.95] * 8)  # wrong-instrument rows
    out = em._compute_forward_returns(_Engine(), d0, horizons=[7])
    assert out["TLT"][7] == pytest.approx(86.0 / 86.78 - 1)
    assert ("YF:TLT:close", "yfinance") not in calls


def test_falls_back_to_yfinance_when_tiingo_has_no_rows(reads) -> None:
    calls, data = reads
    d0 = date(2007, 8, 2)
    data[("YF:TLT:close", "yfinance")] = _obs(d0, [90.0] + [99.0] * 8)
    out = em._compute_forward_returns(_Engine(), d0, horizons=[7])
    assert out["TLT"][7] == pytest.approx(0.10)
    assert calls[:2] == [("YF:TLT:adj_close", "TIINGO"), ("YF:TLT:close", "yfinance")]


def test_fred_series_read_from_fred_as_absolute_change(reads) -> None:
    calls, data = reads
    d0 = date(2025, 4, 4)
    data[("VIXCLS", "FRED")] = _obs(d0, [45.0] + [30.0] * 8)
    out = em._compute_forward_returns(_Engine(), d0, horizons=[7])
    assert out["VIX"][7] == pytest.approx(-15.0)
    assert ("VIXCLS", "FRED") in calls and ("VIXCLS", "TIINGO") not in calls


def test_split_day_does_not_read_as_a_crash(reads) -> None:
    """TIINGO's raw close halves on XLE's 2025-12-05 split; adj_close does not."""
    calls, data = reads
    em._FORWARD_TICKERS = {"XLE": "YF:XLE:adj_close"}
    d0 = date(2025, 11, 25)
    data[("YF:XLE:adj_close", "TIINGO")] = _obs(d0, [43.65] + [45.0] * 20)
    data[("YF:XLE:close", "TIINGO")] = _obs(d0, [88.61] + [45.9] * 20)  # never read
    out = em._compute_forward_returns(_Engine(), d0, horizons=[14])
    assert out["XLE"][14] == pytest.approx(45.0 / 43.65 - 1)
    assert ("YF:XLE:close", "TIINGO") not in calls
