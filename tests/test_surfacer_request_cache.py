"""Synthetic orchestration regressions; run with offline auth/dependency stubs."""
from collections import Counter
from copy import deepcopy


def test_conviction_reuses_keys_without_skipping_candidates(monkeypatch):
    from api.routers import surfacer
    calls = Counter()
    def fetch(name, value):
        def run(*args):
            calls[name] += 1
            return deepcopy(value)
        return run
    for helper, name, value in [
        ('_fetch_options_context', 'options', {}),
        ('_fetch_track_record', 'track', {'samples': 0}),
        ('_fetch_signal_scorecards', 'cards', []),
        ('_fetch_signal_confirmation', 'confirmation', {}),
    ]:
        monkeypatch.setattr(surfacer, helper, fetch(name, value))
    monkeypatch.setattr(surfacer, '_merge_track_records', lambda track, cards: track)
    monkeypatch.setattr(surfacer, '_build_conviction_gate', lambda candidate, **kwargs: kwargs)
    monkeypatch.setattr(surfacer, '_apply_surface_policy', lambda candidate: candidate)
    a = {'id': 'synthetic', 'tickers': ['TEST'], 'direction': 'bullish',
         'freshness': {'label': 'stale'}, 'evidence': [{'source': 'fixture'}]}
    candidates = [dict(deepcopy(a), id=str(i)) for i in range(76)]
    before = deepcopy(candidates)
    result = surfacer._attach_conviction(object(), candidates)
    assert calls == Counter(options=1, track=1, cards=1, confirmation=1)
    assert candidates == before
    assert [c['id'] for c in result] == [c['id'] for c in before]
    assert all(c['freshness'] == a['freshness'] and c['evidence'] == a['evidence'] for c in result)
    surfacer._attach_conviction(object(), [a])
    assert calls == Counter(options=2, track=2, cards=2, confirmation=2)
