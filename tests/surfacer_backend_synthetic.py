"""Execute only the extracted pure orchestration function with synthetic fetches."""
import ast
import copy
from pathlib import Path
from typing import Any
from collections import Counter

root = Path(__file__).resolve().parents[1]
tree = ast.parse((root / 'api/routers/surfacer.py').read_text())
node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_attach_conviction')
calls = Counter()
def fetch(name, value):
    def run(*args):
        calls[name] += 1
        return copy.deepcopy(value)
    return run
namespace = dict(Any=Any, _fetch_options_context=fetch('options', {}),
                 _fetch_track_record=fetch('track', {'samples': 0}),
                 _fetch_signal_scorecards=fetch('scorecards', []),
                 _fetch_signal_confirmation=fetch('confirmation', {'samples': 0}),
                 _horizon_days=lambda c: c.get('horizon', 5),
                 _merge_track_records=lambda track, cards: dict(track, cards=cards),
                 _build_conviction_gate=lambda c, **kw: kw,
                 _apply_surface_policy=lambda c: c)
exec(compile(ast.Module(body=[node], type_ignores=[]), '<isolated function>', 'exec'), namespace)
attach = namespace['_attach_conviction']
a = {'id': 'a', 'tickers': ['TEST'], 'direction': 'up', 'model_name': 'model',
     'calibration': {'regime': 'r', 'signal_contributions': {'a': 1}}, 'stale': True, 'provenance': 'fixture'}
items = [copy.deepcopy(a) for _ in range(76)]
before = copy.deepcopy(items)
out = attach(object(), items)
assert calls == Counter(options=1, track=1, scorecards=1, confirmation=1), calls
assert items == before and len(out) == 76
assert all(o['stale'] and o['provenance'] == 'fixture' for o in out)
assert out[0]['conviction'] == out[-1]['conviction']
# New request cannot reuse another request's fetches, even for identical keys.
attach(object(), [a])
assert calls == Counter(options=2, track=2, scorecards=2, confirmation=2)
# Preserve the existing distinct key dimensions rather than inventing broader caching.
variants = []
for field, val in [('tickers', ['OTHER']), ('direction', 'down'), ('model_name', 'other'), ('horizon', 10)]:
    b = copy.deepcopy(a); b[field] = val; variants.append(b)
b = copy.deepcopy(a); b['calibration']['regime'] = 'other'; variants.append(b)
b = copy.deepcopy(a); b['calibration']['signal_contributions'] = {'b': 1}; variants.append(b)
calls.clear(); attach(object(), [a, *variants])
assert calls == Counter(options=2, track=6, scorecards=4, confirmation=3), calls
calls.clear(); attach(object(), [{'tickers': []}]); assert not calls
print('PASS backend: 76 repeated keys, empty cached values, distinct key dimensions, request isolation, preserved input/order/labels, tickerless candidate')
