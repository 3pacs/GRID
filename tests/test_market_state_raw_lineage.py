"""Synthetic source-selection fingerprint regressions; no providers or production."""
import json
from dataclasses import replace
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from intelligence.regime import market_state as M
from store.observations import MixedSourceError, Observation

D = date(2026, 11, 12)
A = date(2026, 11, 13)


def obs(value=20.0, **kw):
    row = Observation('CBOE:VIX', D, value, datetime(2026, 11, 13, tzinfo=timezone.utc),
                      'CBOE', A, 'pulled')
    return replace(row, **kw)


def receipt(rows):
    selected = pd.Series({o.obs_date: o.value for o in rows if o.value is not None}, dtype=float).sort_index()
    selected = selected[np.isfinite(selected.to_numpy())]
    return M.raw_selection_receipt(rows, selected, series_id='CBOE:VIX', as_of=A, start=D)


def test_selected_row_receipt_is_json_safe_and_retains_clocks():
    r = receipt([obs()])
    assert json.loads(json.dumps(r, allow_nan=False)) == r
    assert r['latest_observation']['pull_timestamp'] == '2026-11-13T00:00:00+00:00'
    assert r['known_at_basis_counts'] == {'pulled': 1}
    assert r['source_filter'] == 'CBOE'
    assert r['publication_policy']['days'] == 1
    assert r['pit_qualified'] is False


@pytest.mark.parametrize('change', [
    {'value': 21.0}, {'source': 'other'}, {'pull_timestamp': datetime(2026, 11, 14)},
    {'known_at': D}, {'known_at_basis': 'modeled_lag'}, {'pull_timestamp': None},
])
def test_hash_tracks_value_and_every_provenance_field(change):
    before = receipt([obs()])
    after = receipt([replace(obs(), **change)])
    assert before['selected_rows_sha256'] != after['selected_rows_sha256']


def test_hash_ignores_order_and_excluded_nonfinite_values():
    earlier = obs(obs_date=date(2026, 11, 11))
    assert receipt([obs(), earlier]) == receipt([earlier, obs()])
    bad = obs(value=float('nan'), obs_date=date(2026, 11, 10))
    assert receipt([bad, obs()]) == receipt([obs()])


def test_collapse_hashes_the_row_actually_consumed():
    old, new = obs(value=19), obs(value=20)
    assert receipt([old, new]) == receipt([new])


def test_unknown_and_modeled_clocks_never_become_proven_availability():
    r = receipt([obs(known_at_basis='modeled_lag', pull_timestamp=None)])
    assert r['known_at_basis_counts'] == {'modeled_lag': 1}
    assert r['missing_pull_timestamps'] == 1
    assert r['pit_qualified'] is False
    unknown = receipt([obs(known_at=None, known_at_basis=None)])
    assert unknown['known_at_basis_counts'] == {'unknown': 1}


def test_raw_read_memoizes_receipt_from_same_selected_rows(monkeypatch):
    calls = []
    def read(*a, **kw):
        calls.append(kw)
        return [obs()]
    monkeypatch.setattr(M, 'read_window_known_at', read)
    r = M._Reads(create_engine('sqlite://'), A, 2)
    assert r.series('CBOE:VIX').iloc[0] == 20
    assert r.series('CBOE:VIX').iloc[0] == 20
    assert len(calls) == 1
    assert r.raw_lineage['CBOE:VIX']['rows'] == 1


def test_mixed_source_is_distinguishable_from_empty_read(monkeypatch):
    def refuse(*a, **kw):
        raise MixedSourceError('synthetic')
    monkeypatch.setattr(M, 'read_window_known_at', refuse)
    r = M._Reads(create_engine('sqlite://'), A, 2)
    assert r.series('CBOE:VIX').empty
    assert r.raw_lineage['CBOE:VIX']['status'] == 'refused_mixed_source'
    assert r.raw_lineage['CBOE:VIX']['rows'] == 0


def test_compute_and_storage_retain_partial_receipt_without_promoting(monkeypatch):
    monkeypatch.setattr(M, 'VARIABLES', [M.VarSpec('vix', 'volatility', 'daily_fast',
                                                lambda r: r.series('CBOE:VIX'), 'points')])
    monkeypatch.setattr(M, 'read_window_known_at', lambda *a, **kw: [obs()])
    engine = create_engine('sqlite://')
    state = M.compute_market_state(engine, A, calendar={})
    lineage = state.to_dict()['source_lineage']
    assert lineage['status'] == 'partial' and lineage['pit_qualified'] is False
    assert lineage['raw_series']['CBOE:VIX']['rows'] == 1
    assert len(lineage['unqualified_read_paths']) == 4
    M.store_market_state(engine, state)
    from sqlalchemy import text
    with engine.connect() as c:
        saved = json.loads(c.execute(text('SELECT record FROM market_state_daily')).scalar_one())
    assert saved['source_lineage'] == lineage


def test_release_overrides_and_unpublished_dates_are_json_safe():
    row = obs(series_id='UNRATE')
    r = M.raw_selection_receipt([row], pd.Series({D: row.value}), series_id='UNRATE', as_of=A, start=D)
    policy = json.loads(json.dumps(r))['publication_policy']
    assert policy['unpublished_dates'] == ['2025-10-01']
    assert ['2025-11-01', '2025-12-16'] in policy['release_overrides']


def test_empty_read_is_not_missing_provenance_of_an_observed_value():
    r = receipt([])
    assert r['rows'] == 0 and r['latest_observation'] is None
    assert r['known_at_basis_counts'] == {} and r['status'] == 'read'
    assert r['pit_qualified'] is False
