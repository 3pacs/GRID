"""Synthetic price provenance checks; no providers or historical evaluations."""
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pytest

from analysis.vol_surface import VolSurfaceEngine
from discovery.options_scanner import OptionsScanner

DAY = date(2026, 11, 10)


def make_engine(primary=None, secondary=None):
    db = MagicMock()
    queries = []

    def execute(statement, params):
        sql = str(statement)
        queries.append((sql, params))
        result = MagicMock()
        if 'FROM options_daily_signals' in sql:
            result.fetchone.return_value = primary
        elif 'FROM resolved_series' in sql:
            result.fetchone.return_value = secondary
        elif 'SELECT strike FROM options_snapshots' in sql:
            result.fetchone.return_value = (800,)
        else:
            raise AssertionError('Unexpected query')
        return result

    db.connect.return_value.__enter__.return_value.execute.side_effect = execute
    return VolSurfaceEngine(db), db, queries


def chain():
    return pd.DataFrame([dict(strike=750, opt_type='call', oi=100, iv=.20,
        expiry=DAY + timedelta(days=30), dte=30, last_price=10, bid=9, ask=11, volume=40)])


def test_max_oi_strike_is_never_a_spot_price():
    engine, _, queries = make_engine()
    assert engine._get_spot_price('SPY', DAY) == 0.0
    assert len(queries) == 2
    assert all('FROM options_snapshots' not in sql for sql, _ in queries)


@pytest.mark.parametrize('invalid', [None, 0, -1, float('nan'), float('inf'),
    -float('inf'), 'bad', True, False, Decimal('NaN')])
def test_bad_primary_uses_valid_stored_close(invalid):
    engine, _, queries = make_engine((invalid,), (Decimal('749.25'),))
    assert engine._get_spot_price('SPY', DAY) == 749.25
    assert len(queries) == 2


@pytest.mark.parametrize('invalid', [None, 0, -1, float('nan'), float('inf'),
    -float('inf'), 'bad', True, Decimal('NaN')])
def test_bad_secondary_is_unavailable(invalid):
    engine, _, queries = make_engine(None, (invalid,))
    assert engine._get_spot_price('SPY', DAY) == 0.0
    assert len(queries) == 2


def test_valid_primary_preserved_with_bound_identity_and_date():
    engine, _, queries = make_engine((Decimal('750.25'),), (749,))
    assert engine._get_spot_price('SPY', DAY) == 750.25
    assert len(queries) == 1
    assert queries[0][1] == {'ticker': 'SPY', 'd': DAY}
    assert 'signal_date <= :d' in queries[0][0]


def test_valid_secondary_preserved_with_bound_identity_and_date():
    engine, _, queries = make_engine(None, (749.25,))
    assert engine._get_spot_price('SPY', DAY) == 749.25
    assert queries[1][1] == {'name1': 'spy_close', 'name2': 'spy', 'd': DAY}
    assert 'rs.obs_date <= :d' in queries[1][0]


@pytest.mark.parametrize('method, expected', [
    ('build_surface', {'error': 'No spot price for SPY', 'ticker': 'SPY'}),
    ('compute_skew', []),
    ('compute_term_structure', {'term_structure': [], 'slope': None, 'is_inverted': None}),
    ('compute_greeks_grid', {'error': 'No spot price for SPY', 'grid': []}),
    ('variance_swap_level', None),
])
def test_no_observed_price_blocks_every_surface_consumer(method, expected):
    engine, _, _ = make_engine()
    engine._get_options_data = MagicMock(return_value=chain())
    assert getattr(engine, method)('SPY', DAY) == expected


def test_real_scanner_path_adds_no_direction_from_missing_price(monkeypatch):
    engine, db, _ = make_engine()
    engine._get_options_data = MagicMock(return_value=chain())
    engine.detect_arbitrage = MagicMock()
    monkeypatch.setattr('analysis.vol_surface.VolSurfaceEngine', lambda db: engine)
    score, direction, meta = OptionsScanner(db_engine=db)._score_vol_surface('SPY', DAY)
    assert score == 0
    assert direction == ''
    assert meta['front_skew'] is None
    engine.detect_arbitrage.assert_not_called()
