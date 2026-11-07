"""Synthetic M1 public contracts, bound to GammaWatchPublicContract and Options.

No provider/database imports. Clock is injectable for deterministic contract tests;
the local HTTP server supplies its current clock so browser freshness is meaningful.
"""
from __future__ import annotations


def gamma_state(scenario: str, now: float) -> tuple[dict, int]:
    if scenario == 'unavailable':
        return {'status': 'unavailable', 'data': None,
                'reason': 'Gamma Watch collector unavailable or invalid; no cached substitute.'}, 503
    if scenario == 'error':
        return {'error': 'synthetic upstream transport failure'}, 502
    age = 7200 if scenario == 'partial' else 5
    frame = {
        'schema': 'gex-screen/2', 'symbol': 'SPY', 'served_at': now - 1,
        'evaluation_clock': 'ok',
        'public_price': {
            'status': 'unverified-public-latency',
            'source': 'Yahoo chart · public, latency not guaranteed',
            'source_time': now - age, 'receipt_time': now - 2,
            'values_available': True, 'current_interpretation': False,
            'max_context_age_seconds': 120, 'price': 600,
        },
        'delayed_gex': {
            'status': 'unverified-delayed', 'source': 'ZeroGEX · delayed ~15 minutes',
            'source_time': now - (7200 if scenario == 'partial' else 900),
            'receipt_time': now - 2, 'values_available': True,
            'current_interpretation': False, 'max_context_age_seconds': 1800,
            'spot': 599, 'levels': {'put_wall': 590, 'call_wall': 605,
                                   'gamma_flip': 0, 'max_pain': None},
        },
        'research_model': {
            'status': 'frozen-research', 'current_interpretation': False,
            'chain_as_of': now - 86400, 'computed_at': now - 3600,
            'grid': [590, 600, 610],
            'variants': [{'name': 'paired OTM IV', 'iv_shock': 0, 'values': [-1, 0, 1]}],
        },
        'sectors': {'status': 'unavailable', 'rows': []},
        'structural': {'official': [], 'rebalance': {'status': 'modeled-history-unqualified',
            'computed_at': now - 60, 'rows': [{'period': 'month', 'baseline': '2026-10-30',
            'as_of_date': '2026-11-05', 'equity_return': 0, 'bond_return': None,
            'scenarios': [{'equity_weight': .6, 'equity_purchase_per_initial_billion': 0}]}]}},
    }
    if scenario == 'empty':
        for key in ('public_price', 'delayed_gex', 'research_model'):
            frame[key] = {'status': 'unavailable'}
        frame['structural'] = {'official': [], 'rebalance': {'status': 'unavailable'}}
    return {'status': 'available', 'data': frame,
            'note': 'Synthetic fixture only. No signal promotion.'}, 200


def options_signals() -> dict:
    # Real Signals surface has no per-source timestamp rendering. An empty
    # response avoids manufacturing a provenance claim that the UI cannot show.
    return {'signals': []}


def dealer_gex(scenario: str) -> dict:
    if scenario in ('empty', 'unavailable', 'error'):
        return {'error': 'synthetic: no completed SPY options-chain capture', 'ticker': 'SPY'}
    return {
        'ticker': 'SPY', 'spot': 600, 'regime': 'LONG_GAMMA',
        'gex_aggregate': 0, 'gamma_flip': 600, 'call_wall': 610, 'put_wall': 590,
        'per_strike': [{'strike': 590, 'net_gex': -1000}, {'strike': 600, 'net_gex': 0},
                       {'strike': 610, 'net_gex': 1000}],
        'basis': 'Synthetic assumed dealer positions; fixture, not a market observation',
        'chain_snap_date': '2026-11-05',
        'chain_capture_completed_at': '2026-11-05T20:00:00Z',
        'chain_provider_regular_market_at_min': '2026-11-05T19:40:00Z',
        'chain_provider_regular_market_at_max': '2026-11-05T19:45:00Z',
        'spot_source': 'synthetic-fixture', 'spot_basis': 'fixed round-number reference',
        'spot_obs_date': '2026-11-05', 'spot_available_at': '2026-11-05T20:00:00Z',
        'stale': scenario == 'partial',
    }
