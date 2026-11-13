"""Malformed chain numerics must not produce a partial, apparently valid GEX."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
import pytest
from physics.dealer_gamma import DealerGammaEngine

DAY = date(2026, 10, 20)
END = datetime(2026, 10, 20, 22, 21, tzinfo=timezone.utc)

def load(monkeypatch, *, column=None, value=None):
    row = [750.0, "call", 100, .2, DAY + timedelta(days=7), timedelta(days=7),
           END - timedelta(seconds=2), "11111111-1111-4111-8111-111111111111", 1,
           END - timedelta(seconds=5), END, END - timedelta(hours=3), None]
    good = tuple(row)
    if column is not None:
        row[column] = value
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [good, tuple(row)]
    return DealerGammaEngine(engine)._load_chain("SPY", DAY, cutoff=END)

@pytest.mark.parametrize("column", [0, 2, 3])
@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan"), None, "broken", True])
def test_malformed_numeric_fails_whole_chain(monkeypatch, column, value):
    assert load(monkeypatch, column=column, value=value).empty

@pytest.mark.parametrize("value", [0, -1])
def test_nonpositive_strike_fails_whole_chain(monkeypatch, value):
    assert load(monkeypatch, column=0, value=value).empty

@pytest.mark.parametrize("column,value", [(2, 0), (3, 0), (2, -1), (3, -1)])
def test_existing_zero_or_negative_oi_iv_filter_is_preserved(monkeypatch, column, value):
    assert len(load(monkeypatch, column=column, value=value)) == 1

@pytest.mark.parametrize("column,value", [(0, Decimal("751")), (2, Decimal("102")), (3, Decimal("0.25"))])
def test_finite_decimal_inputs_survive(monkeypatch, column, value):
    assert len(load(monkeypatch, column=column, value=value)) == 2
