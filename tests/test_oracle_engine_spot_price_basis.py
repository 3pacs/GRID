"""Regression: OracleEngine._get_spot_price must stay on the raw price basis.

The value this method returns becomes ``entry_price`` on new
``oracle_predictions`` rows. Its ``raw_series`` fallback used to prefer
``YF:{ticker}:adj_close`` over ``YF:{ticker}:close`` -- silently returning a
dividend/split-adjusted price where the primary source
(``options_daily_signals.spot_price``) and the exit-side scorer
(``scripts/score_oracle_trades.py``'s ``fetch_prices()``, ``auto_adjust=False``
since PR #516) are both raw. Comparing a raw exit price against an adjusted
entry price manufactures a spurious return equal to the cumulative
adjustment factor -- the same mixed-basis bug PR #503 fixed for
``intelligence/trust_scorer.py`` and ``trading/options_tracker.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from oracle.engine import OracleEngine
from scripts.score_oracle_trades import fetch_prices


class _FakeConn:
    """Minimal stand-in for a SQLAlchemy Connection: routes SELECTs by table."""

    def __init__(self, raw_series_rows: dict[str, float]):
        self._raw_series_rows = raw_series_rows

    def execute(self, stmt, params=None):
        params = params or {}
        sql = str(stmt)
        result = MagicMock()
        if "options_daily_signals" in sql:
            # Force the fallback path -- these tests are about the fallback.
            result.fetchone.return_value = None
        elif "raw_series" in sql:
            value = self._raw_series_rows.get(params.get("sid"))
            result.fetchone.return_value = (value,) if value is not None else None
        else:
            result.fetchone.return_value = None
        return result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self, raw_series_rows: dict[str, float]):
        self._raw_series_rows = raw_series_rows

    def connect(self):
        return _FakeConn(self._raw_series_rows)


def _make_engine(monkeypatch, raw_series_rows: dict[str, float]) -> OracleEngine:
    # Same bypass pattern as tests/test_oracle_schema_guards.py -- skip the
    # real DDL/model-loading side effects of __init__, they're irrelevant here.
    monkeypatch.setattr(OracleEngine, "_ensure_tables", lambda self: None)
    monkeypatch.setattr(OracleEngine, "_load_models", lambda self: [])
    return OracleEngine(_FakeEngine(raw_series_rows))


def test_get_spot_price_prefers_raw_close_over_adjusted(monkeypatch):
    """Both adj_close and close exist (a real dividend adjustment) — raw wins."""
    engine = _make_engine(monkeypatch, {
        "YF:AAPL:close": 182.0,
        "YF:AAPL:adj_close": 178.4,  # ~2% back-adjusted out for a dividend
    })

    assert engine._get_spot_price("AAPL") == pytest.approx(182.0)


def test_get_spot_price_returns_none_without_raw_close(monkeypatch):
    """Only adj_close is stored — must return None, not silently substitute it.

    Explicit missing-price handling: the caller
    (OracleEngine.generate_predictions_for_ticker) already treats a falsy
    return as "no data" and stores a NO_DATA placeholder. Returning the
    adjusted value instead would silently mix bases; returning None routes
    the row through the existing, correct degradation path.
    """
    engine = _make_engine(monkeypatch, {"YF:AAPL:adj_close": 178.4})

    assert engine._get_spot_price("AAPL") is None


def _single_ticker_frame(dates, close):
    return pd.DataFrame({"Close": list(close)}, index=pd.to_datetime(dates))


def test_entry_and_exit_price_share_the_same_raw_basis(monkeypatch):
    """Integration: the fixed entry-price fallback agrees with the fixed scorer.

    entry_price (OracleEngine._get_spot_price, this fix) and the actual/exit
    price (score_oracle_trades.fetch_prices, PR #516) must land on the same
    raw close -- proving the two independently-fixed call sites are now on a
    consistent basis, not just individually "raw" in isolation.
    """
    engine = _make_engine(monkeypatch, {
        "YF:AAPL:close": 182.0,
        "YF:AAPL:adj_close": 178.4,
    })
    entry_price = engine._get_spot_price("AAPL")

    frame = _single_ticker_frame(["2026-03-10", "2026-03-11"], [180.0, 182.0])
    with patch("yfinance.download", return_value=frame):
        prices = fetch_prices(["AAPL"], "2026-03-01", "2026-03-12")

    assert any(v == pytest.approx(entry_price) for v in prices["AAPL"].values()), (
        "entry price (OracleEngine._get_spot_price) must match a raw close "
        "actually reachable via the exit-side scorer (fetch_prices) — both "
        "must agree on 182.0, the raw close, not 178.4, the adjusted one"
    )
