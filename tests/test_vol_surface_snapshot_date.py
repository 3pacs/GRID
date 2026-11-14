"""Synthetic exact-session tests; no empirical panel or provider access."""
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from analysis.vol_surface import VolSurfaceEngine


DAY = date(2026, 11, 10)


def engine_with_snapshots(snapshots):
    engine = MagicMock()
    calls = []

    def execute(statement, params):
        sql = str(statement)
        calls.append((sql, params))
        result = MagicMock()
        if "MAX(snap_date)" in sql:
            result.fetchone.return_value = (max(snapshots) if snapshots else None,)
        else:
            assert "snap_date = :snap_date" in sql
            assert "ticker = :ticker" in sql
            result.fetchall.return_value = snapshots.get(params["snap_date"], [])
        return result

    engine.connect.return_value.__enter__.return_value.execute.side_effect = execute
    return VolSurfaceEngine(engine), calls


def row(day):
    return (750, "call", 100, .20, day + timedelta(days=30), 30, 10, 9, 11, 40)


@pytest.mark.parametrize("offset", [-2, 1, 30])
def test_missing_requested_session_never_substitutes_other_date(offset):
    other = DAY + timedelta(days=offset)
    engine, calls = engine_with_snapshots({other: [row(other)]})
    assert engine._get_options_data("SPY", DAY).empty
    assert len(calls) == 1
    assert calls[0][1] == {"ticker": "SPY", "snap_date": DAY}


def test_exact_session_preserves_contract_and_dte():
    engine, calls = engine_with_snapshots({DAY: [row(DAY)]})
    result = engine._get_options_data("SPY", DAY)
    assert len(result) == 1
    assert result.iloc[0]["strike"] == 750
    assert result.iloc[0]["dte"] == 30
    assert result.iloc[0]["expiry"] == DAY + timedelta(days=30)
    assert len(calls) == 1


def test_unavailable_surface_does_not_read_spot_or_fit():
    later = DAY + timedelta(days=1)
    engine, _ = engine_with_snapshots({later: [row(later)]})
    engine._get_spot_price = MagicMock(return_value=750)
    engine.fit_svi_slice = MagicMock()
    result = engine.build_surface("SPY", DAY)
    assert result["error"] == "No options data for SPY"
    engine._get_spot_price.assert_not_called()
    engine.fit_svi_slice.assert_not_called()


def test_exact_session_surface_keeps_requested_date():
    engine, _ = engine_with_snapshots({DAY: [row(DAY)]})
    engine._get_spot_price = MagicMock(return_value=750)
    result = engine.build_surface("SPY", DAY)
    assert "error" not in result
    assert result["as_of_date"] == DAY.isoformat()
    assert result["raw_points"][0]["dte"] == 30


@pytest.mark.parametrize("method, expected", [
    ("compute_skew", []),
    ("compute_term_structure", {"term_structure": [], "slope": None, "is_inverted": None}),
    ("compute_greeks_grid", {"error": "No options data for SPY", "grid": []}),
    ("variance_swap_level", None),
])
def test_other_surface_consumers_preserve_unavailable(method, expected):
    later = DAY + timedelta(days=1)
    engine, calls = engine_with_snapshots({later: [row(later)]})
    engine._get_spot_price = MagicMock(return_value=750)
    assert getattr(engine, method)("SPY", DAY) == expected
    engine._get_spot_price.assert_not_called()
    assert len(calls) == 1
