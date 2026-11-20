"""Fail-closed publication containment for unreconciled SPY share-volume inputs.

No provider fetch, storage mutation, source promotion, or imputation is performed.
A future verified volume adapter needs separate review, not a status-string toggle.
"""
from copy import deepcopy
import math

SCHEMA = 'gex-volume-quality/1'
REASON = ('SPY interval volume is unreconciled: Yahoo contains cumulative-sized minute bars. '
          'Median replacements are estimates, not observed trades. VWAP, relative volume, '
          'volume spikes and dependent composite signals are withheld.')
TAINTED = frozenset(('vwap_stretch', 'vol_ignition', 'volume_ignition', 'STACK', 'stack', 'flip_reclaim', 'flip_loss'))


def quality():
    return {'schema': SCHEMA, 'status': 'unavailable', 'reason': REASON,
            'code': 'unreconciled_interval_volume', 'current_interpretation': False,
            'imputation_is_validation': False, 'baseline_verified': False}


def unavailable(schema=None):
    out = {'status': 'unavailable', 'reason': REASON, 'volume_quality': quality()}
    if schema:
        out['schema'] = schema
    return out


def tainted(row):
    if not isinstance(row, dict):
        return isinstance(row, str) and row.split('|', 1)[0] in TAINTED
    return any(isinstance(row.get(k), str) and row[k].split('|', 1)[0] in TAINTED
               for k in ('type', 'key', 'engine', 'signal', 'trigger'))


def filter_events(value):
    """Drop identified dependent records, never mutate cached or persisted inputs."""
    if tainted(value):
        return None
    if isinstance(value, list):
        return [filter_events(v) for v in value if not tainted(v)]
    if isinstance(value, dict):
        return {k: ([] if k == 'stacks' else filter_events(v)) for k, v in value.items()
                if (not isinstance(k, str) or k.split('|', 1)[0] not in TAINTED) and not tainted(v)}
    return deepcopy(value)


def guard_panel(payload):
    if not isinstance(payload, dict):
        return payload
    out = unavailable(payload.get('schema', 'gex-panel/1'))
    for k in ('generated_at', 'served_at', 'age_s', 'file_age_s', 'day'):
        if k in payload:
            out[k] = payload[k]
    current = payload.get('current') if isinstance(payload.get('current'), dict) else {}
    out['current'] = {'call': 'WAIT', 'trigger': None, 'reason': REASON,
                      'spot': current.get('spot'), 'ts_utc': current.get('ts_utc'),
                      'stop': None, 'until_utc': None,
                      'grade': {'status': 'unavailable', 'reasons': [REASON]}}
    out['data_grade'] = 'UNRELIABLE'
    out['data_grade_reasons'] = [REASON]
    return out


def guard_scoreboard(payload):
    if not isinstance(payload, dict):
        return payload
    out = filter_events(payload)
    if not isinstance(out, dict):
        return unavailable('gex-scoreboard/1')
    out['status'] = 'degraded'
    out['volume_quality'] = quality()
    v = out.get('volume') if isinstance(out.get('volume'), dict) else {}
    # Keep price-only high/low values; discard volume-qualified single-print claims.
    hilo = v.get('hilo')
    if isinstance(hilo, dict):
        hilo = {k: val for k, val in hilo.items() if k in ('high','low','high_ts','low_ts','high_et','low_et','updated_at')}
    out['volume'] = {**unavailable(v.get('schema', 'gex-volume/1')), 'hilo': hilo,
                     'relative_volume': {'status': 'unavailable', 'value': None, 'reason': REASON},
                     'spikes': {'status': 'unavailable', 'top': [], 'n_today': None}}
    # These composites use volume/VWAP features; do not publish a partial score as valid.
    if 'analogs' in out:
        out['analogs'] = unavailable('gex-analogs/1')
    if 'panel' in out:
        out['panel'] = guard_panel(out['panel'])
    return out


def guard_trail(payload):
    if not isinstance(payload, dict):
        return payload
    out = filter_events(payload)
    if not isinstance(out, dict):
        return unavailable()
    mk = out.get('markers')
    if isinstance(mk, dict):
        if isinstance(mk.get('hilo'), dict):
            mk['hilo'] = {k: v for k, v in mk['hilo'].items() if k in ('high','low','high_ts','low_ts','high_et','low_et','updated_at')}
            mk['hilo']['basis'] = 'price-only bars; trade counts unavailable'
        mk['spikes'] = []
        mk['status'] = dict(mk['status']) if isinstance(mk.get('status'), dict) else {}
        mk['reasons'] = dict(mk['reasons']) if isinstance(mk.get('reasons'), dict) else {}
        mk['status']['spikes'] = 'unavailable'
        mk['reasons']['spikes'] = REASON
    out['volume_quality'] = quality()
    return out


def guard_feed(payload):
    out = filter_events(payload)
    if isinstance(payload, dict) and not isinstance(out, dict):
        return unavailable()
    if isinstance(out, dict):
        out['volume_quality'] = quality()
    return out


def safe_previous_close(meta, prior_high, prior_low):
    """Yahoo chartPreviousClose is a range baseline, never a previous-session close.

    Admit only meta.previousClose with a finite price inside the last completed
    regular-session range. Missing/contradictory metadata returns None.
    """
    def number(x):
        try:
            return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)
        except (OverflowError, ValueError):
            return False
    candidate = meta.get('previousClose') if isinstance(meta, dict) else None
    if not all(number(x) for x in (candidate, prior_high, prior_low)):
        return None
    if candidate <= 0 or prior_low <= 0 or prior_low > prior_high:
        return None
    return float(candidate) if prior_low <= candidate <= prior_high else None
