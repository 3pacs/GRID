"""``OracleEngine._gather_signals`` reads feature history through ``PITStore``.

Until 2026-09-10 the live predictor queried ``resolved_series`` directly with
no ``release_date`` filter — lookahead on the inference path. These tests pin
that the read goes through ``store.pit.PITStore.get_feature_matrix`` with
``as_of_date = today`` and ``LATEST_AS_OF``, that no raw ``resolved_series``
SQL is issued, and that a PIT failure degrades to options-only signals.
"""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import pandas as pd

from oracle import engine as oe


def _engine_with(feat_rows, opt_row=None) -> MagicMock:
    conn = MagicMock()

    def execute(stmt, params=None):
        result = MagicMock()
        sql = str(stmt)
        if "feature_registry" in sql and "resolved_series" not in sql:
            result.fetchall.return_value = feat_rows
        elif "options_daily_signals" in sql:
            result.fetchone.return_value = opt_row
        else:
            raise AssertionError(f"unexpected SQL on the live predictor path: {sql}")
        return result

    conn.execute.side_effect = execute
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine


def _oracle(engine: MagicMock, pit: MagicMock) -> oe.OracleEngine:
    obj = oe.OracleEngine.__new__(oe.OracleEngine)
    obj.engine = engine
    obj._pit_store_instance = pit
    return obj


def test_gather_signals_uses_pit_matrix_and_never_resolved_series_directly() -> None:
    today = date.today()
    idx = pd.DatetimeIndex([today - timedelta(days=i) for i in range(9, -1, -1)])
    matrix = pd.DataFrame({7: [1.0] * 9 + [5.0]}, index=idx)  # spike on the last day
    pit = MagicMock()
    pit.get_feature_matrix.return_value = matrix
    engine = _engine_with([(7, "vix_spot", "vol")])

    signals = _oracle(engine, pit)._gather_signals("SPY", ["vol"])

    pit.get_feature_matrix.assert_called_once()
    args, kwargs = pit.get_feature_matrix.call_args
    assert args[0] == [7]
    assert kwargs["as_of_date"] == today
    assert kwargs["end_date"] == today
    assert (today - kwargs["start_date"]).days == 30
    assert kwargs["vintage_policy"] == "LATEST_AS_OF"

    issued = [str(c.args[0]) for c in engine.connect.return_value.execute.call_args_list]
    assert not any("resolved_series" in sql for sql in issued)

    (vix,) = [s for s in signals if s.name == "vix_spot"]
    assert vix.family == "vol"
    assert vix.value == 5.0
    assert vix.direction == "bullish"
    assert vix.z_score > 0.5
    assert vix.freshness_hours == 0


def test_gather_signals_skips_short_history_and_survives_pit_failure() -> None:
    pit = MagicMock()
    pit.get_feature_matrix.side_effect = RuntimeError("pit unavailable")
    opt_row = (1.5, 0.2, None, 90.0, 100.0, 5000, None, None)
    engine = _engine_with([(7, "vix_spot", "vol")], opt_row=opt_row)

    signals = _oracle(engine, pit)._gather_signals("SPY", ["vol"])

    names = [s.name for s in signals]
    assert "vix_spot" not in names
    assert "pcr" in names and "iv_atm" in names and "max_pain_gap" in names


def test_gather_signals_requires_five_observations() -> None:
    today = date.today()
    idx = pd.DatetimeIndex([today - timedelta(days=i) for i in range(3, -1, -1)])
    pit = MagicMock()
    pit.get_feature_matrix.return_value = pd.DataFrame({7: [1.0, 2.0, 3.0, 4.0]}, index=idx)
    engine = _engine_with([(7, "vix_spot", "vol")])

    assert _oracle(engine, pit)._gather_signals("SPY", ["vol"]) == []


def test_gather_signals_without_eligible_features_never_touches_pit() -> None:
    pit = MagicMock()
    engine = _engine_with([])

    assert _oracle(engine, pit)._gather_signals("SPY", ["vol"]) == []
    pit.get_feature_matrix.assert_not_called()
