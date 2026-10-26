"""Official shutdown release boundaries; synthetic SQLite, no provider access."""
from datetime import date, datetime

import pytest

from tests.test_regime_state_vector_pit import _insert_many, _row, engine  # noqa: F401


@pytest.mark.parametrize('obs,release', [
    (date(2013, 9, 1), date(2013, 10, 22)),
    (date(2013, 10, 1), date(2013, 11, 8)),
    (date(2025, 9, 1), date(2025, 11, 20)),
    (date(2025, 11, 1), date(2025, 12, 16)),
])
def test_unrate_shutdown_not_visible_before_actual_release(engine, obs, release):
    from datetime import timedelta
    from intelligence.regime.state_vector import _fetch_series
    from store.observations import read_window_known_at
    from intelligence.regime.state_vector import PUBLICATION_LAGS

    _insert_many(engine, [_row('UNRATE', obs, 4.2)])
    assert _fetch_series(engine, 'UNRATE', release - timedelta(days=1)).empty
    assert _fetch_series(engine, 'UNRATE', release).to_list() == [4.2]
    with engine.connect() as conn:
        result = read_window_known_at(conn, 'UNRATE', as_of=release,
                                     lag=PUBLICATION_LAGS['UNRATE'])
    assert result[0].known_at == release


@pytest.mark.parametrize('as_of', [date(2025, 11, 15), date(2025, 12, 16), date(2026, 9, 29)])
def test_cancelled_october2025_unrate_is_never_synthetic_success(engine, as_of):
    from intelligence.regime.state_vector import _fetch_series

    # Even a mislabeled SUCCESS/backfilled value is not an official CPS observation.
    _insert_many(engine, [_row('UNRATE', date(2025, 10, 1), 0.0,
                             ts=datetime(2025, 11, 1, 9))])
    assert _fetch_series(engine, 'UNRATE', as_of).empty


def test_normal_unrate_proxy_uses42days_and_keeps_real_pull_evidence(engine):
    from intelligence.regime.state_vector import _fetch_series
    _insert_many(engine, [_row('UNRATE', date(2025, 5, 1), 4.1)])
    assert _fetch_series(engine, 'UNRATE', date(2025, 6, 11)).empty
    assert _fetch_series(engine, 'UNRATE', date(2025, 6, 12)).to_list() == [4.1]
    _insert_many(engine, [_row('UNRATE', date(2025, 6, 1), 4.2,
                             ts=datetime(2025, 7, 3, 13))])
    assert _fetch_series(engine, 'UNRATE', date(2025, 7, 3)).iloc[-1] == 4.2


def test_same_compute_input_has_unchanged_stale_thresholds(monkeypatch):
    import pandas as pd
    from intelligence.regime import state_vector as sv

    target = date(2026, 9, 29)
    old = pd.Series([4.0] * 30, index=pd.date_range(end='2026-05-21', periods=30).date)
    calls = []
    def window(self, sid):
        calls.append(sid)
        return old if len(calls) == 1 else pd.Series([99.0] * 30,
                 index=pd.date_range(end=target, periods=30).date)
    monkeypatch.setattr(sv, 'STATE_DIMENSIONS', [sv.DimensionSpec(
        'unemployment_level', 'UNRATE', 'raw', 1, min_history=30)])
    monkeypatch.setattr(sv._AsOfReader, 'window', window)
    monkeypatch.setattr(sv, '_get_normalization_stats', lambda *_: {})
    monkeypatch.setattr(sv, '_fetch_spy_prices', lambda *_: (pd.Series(dtype=float), None))
    got = sv.compute_state_vector(None, target)
    assert calls == ['UNRATE']
    assert got.values == (4.0,)
    assert got.stale_dimensions == ('unemployment_level',)
    assert [sv._stale_threshold_days(s) for s in ('ICSA', 'UNRATE')] == [30, 70]
    monkeypatch.setattr(sv, 'QUARTERLY_FRED_SERIES', frozenset({'ICSA'}))
    assert sv._stale_threshold_days('ICSA') == 160
