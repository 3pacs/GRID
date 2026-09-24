"""Regression: DealerGammaEngine never publishes an options strike as ``spot``.

``_get_spot`` used to fall back to the highest-open-interest call strike when
resolved_series had no close. That strike was served as ``spot`` and the flip,
walls, regime, market summary and forced-flow waterfall score were computed
around it. With no measured close the profile is now explicitly unavailable,
the market summary carries null SPY fields (not ``spy_gex: 0``) and the
forced-flow briefing reads UNKNOWN instead of scoring a regime.
"""

from __future__ import annotations

import sys
import types
from datetime import date
from typing import Any, Self

import pandas as pd
import pytest

from physics.dealer_gamma import DealerGammaEngine

SNAP_DATE = date(2026, 9, 24)
TOP_OI_CALL_STRIKE = 785.0
MEASURED_FIELDS = (
    "spot", "gex_aggregate", "gex_normalized", "gamma_flip", "gamma_wall",
    "put_wall", "call_wall", "dealer_delta", "vanna_exposure",
    "charm_exposure", "regime", "profile", "per_strike",
)


class _Result:
    def __init__(self, row: tuple | None) -> None:
        self._row = row

    def fetchone(self) -> tuple | None:
        return self._row


class _Conn:
    def __init__(self, db: _FakeDB) -> None:
        self._db = db

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, statement: Any, params: dict | None = None) -> _Result:
        sql = str(statement)
        self._db.statements.append(sql)
        if "resolved_series" in sql:
            close = self._db.closes.get((params or {}).get("name2"))
            return _Result((close,) if close is not None else None)
        if "options_snapshots" in sql:
            # What the removed fallback read: the top-OI call strike.
            return _Result((TOP_OI_CALL_STRIKE,))
        raise AssertionError(f"unexpected SQL: {sql}")


class _FakeDB:
    """Answers only the two queries the old spot lookup could issue."""

    def __init__(self, closes: dict[str, float] | None = None) -> None:
        self.closes = closes or {}
        self.statements: list[str] = []

    def connect(self) -> _Conn:
        return _Conn(self)


def _chain() -> pd.DataFrame:
    # Heavy call open interest at the 785 strike: priced at spot 785 (the old
    # fallback) this chain classifies as SHORT_GAMMA.
    return pd.DataFrame([
        {"strike": 750.0, "opt_type": "put", "open_interest": 200_000.0,
         "implied_volatility": 0.15, "dte": 30.0},
        {"strike": TOP_OI_CALL_STRIKE, "opt_type": "call",
         "open_interest": 1_000_000.0, "implied_volatility": 0.15, "dte": 30.0},
    ])


def test_get_spot_without_measured_close_is_zero_not_a_strike() -> None:
    db = _FakeDB()
    engine = DealerGammaEngine(db)

    assert engine._get_spot("SPY", SNAP_DATE) == 0.0
    assert not any("options_snapshots" in sql for sql in db.statements)


def test_get_spot_returns_the_measured_close() -> None:
    engine = DealerGammaEngine(_FakeDB({"spy": 767.12}))

    assert engine._get_spot("SPY", SNAP_DATE) == 767.12


@pytest.mark.parametrize("bad_close", [float("nan"), float("inf"), -1.0, "invalid"])
def test_get_spot_rejects_invalid_resolved_close(bad_close: object) -> None:
    engine = DealerGammaEngine(_FakeDB({"spy": bad_close}))

    assert engine._get_spot("SPY", SNAP_DATE) == 0.0


def test_profile_with_measured_close_labels_its_spot_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = DealerGammaEngine(_FakeDB({"spy": 767.12}))
    monkeypatch.setattr(engine, "_load_chain", lambda _ticker, _snap_date: _chain())

    profile = engine.compute_gex_profile("SPY", SNAP_DATE)

    assert profile["spot"] == 767.12
    assert profile["spot_source"] == "resolved_series"


def test_profile_without_measured_spot_is_explicitly_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = DealerGammaEngine(_FakeDB())
    monkeypatch.setattr(engine, "_load_chain", lambda _ticker, _snap_date: _chain())

    result = engine.compute_gex_profile("SPY", SNAP_DATE)

    assert result["available"] is False
    assert result["status"] == "unavailable"
    assert "no measured close for SPY" in result["reason"]
    assert result["as_of"] is None
    assert result["ticker"] == "SPY"
    # Every consumer keys on `error`; it must still be present and truthy.
    assert result["error"] == "No spot price for SPY"
    for field in MEASURED_FIELDS:
        assert result.get(field) is None, field


def test_market_summary_reports_missing_spy_as_null_not_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = DealerGammaEngine(_FakeDB())
    qqq = {
        "ticker": "QQQ", "spot": 600.0, "gex_aggregate": -5.0e8,
        "vanna_exposure": 1.0, "charm_exposure": 2.0,
        "regime": "SHORT_GAMMA", "gamma_flip": 590.0,
    }
    monkeypatch.setattr(engine, "compute_all_tickers", lambda _snap_date=None: [qqq])

    summary = engine.get_market_gex_summary()

    assert summary["market_regime"] == "UNKNOWN"
    assert summary["spy_gex"] is None
    for key in ("spy_spot", "spy_gamma_flip", "spy_put_wall", "spy_call_wall"):
        assert summary[key] is None, key
    assert [t["ticker"] for t in summary["tickers"]] == ["QQQ"]


def test_forced_flow_briefing_does_not_score_a_regime_off_a_strike(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from intelligence.forced_flow_monitor import build_morning_briefing

    monkeypatch.setitem(
        sys.modules, "ingestion.options",
        types.SimpleNamespace(EQUITY_TICKERS=["SPY", "QQQ"]),
    )
    monkeypatch.setattr(
        DealerGammaEngine, "_load_chain",
        lambda self, _ticker, _snap_date: _chain(),
    )
    # QQQ has a measured close; SPY has none.
    briefing = build_morning_briefing(_FakeDB({"qqq": 600.0}))

    assert briefing.regime.regime == "UNKNOWN"
    assert briefing.regime.spot == 0.0
    assert briefing.regime.gamma_flip is None
    assert briefing.regime.put_wall is None
    assert briefing.regime.call_wall is None
    tripped = {t.name for t in briefing.thresholds if t.tripped}
    assert "short_gamma_regime" not in tripped
    assert "compound_regime_catalyst" not in tripped
    assert briefing.posture["lever"].startswith("Regime unknown")
