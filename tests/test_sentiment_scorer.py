"""Regression tests for intelligence/sentiment_scorer.py::_score_signals.

Production hit `unsupported operand type(s) for +=: 'float' and
'decimal.Decimal'` every hourly cycle: `signal_sources.trust_score` comes
back from psycopg2 as `decimal.Decimal` (NUMERIC column), but
`bull_score`/`bear_score` accumulators are seeded as Python floats.
`_score_signals` must convert each row's trust value to float at the
DB boundary before accumulating, matching the pattern already used by
`_score_flows` (`float(amount or 0)`).
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from intelligence.sentiment_scorer import BEARISH_SIGNALS, BULLISH_SIGNALS, _score_signals


def _mock_engine(rows):
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    conn.execute.return_value.fetchall.return_value = rows
    return engine


class TestScoreSignalsDecimalBoundary:
    def test_decimal_trust_scores_do_not_raise(self):
        bull_type = next(iter(BULLISH_SIGNALS))
        bear_type = next(iter(BEARISH_SIGNALS))
        rows = [
            (bull_type, Decimal("0.80")),
            (bull_type, Decimal("0.65")),
            (bear_type, Decimal("0.40")),
        ]
        engine = _mock_engine(rows)

        component = _score_signals(engine)

        # No exception, and the raw_value reflects the Decimal inputs
        # converted to float (0.80 + 0.65 - 0.40 = 1.05).
        assert component.name == "signals"
        assert round(component.raw_value, 2) == 1.05
        assert isinstance(component.raw_value, float)
        assert component.detail.startswith("3 signals")

    def test_mixed_decimal_and_float_trust_scores(self):
        bull_type = next(iter(BULLISH_SIGNALS))
        rows = [
            (bull_type, Decimal("0.5")),
            (bull_type, 0.5),
        ]
        engine = _mock_engine(rows)

        component = _score_signals(engine)

        assert round(component.raw_value, 2) == 1.0

    def test_no_rows_returns_zero_component(self):
        engine = _mock_engine([])
        component = _score_signals(engine)
        assert component.score == 0.0
        assert component.detail == "No scored signals available"
