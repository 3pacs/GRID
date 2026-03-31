"""
Adapter to publish alpha research signals into GRID's SignalRegistry.

Converts computed signal DataFrames into RegisteredSignal objects
and registers them for Oracle consumption.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from intelligence.signal_registry import (
    Direction,
    RegisteredSignal,
    SignalRegistry,
    SignalType,
    make_signal_id,
)


def publish_factor_signals(
    engine: Engine,
    signal_name: str,
    signal_panel: pd.DataFrame,
    as_of_date: date | None = None,
    top_pct: float = 0.20,
    confidence: float = 0.6,
    valid_hours: int = 24,
) -> int:
    """
    Publish a cross-sectional factor signal to SignalRegistry.

    Takes the latest row of the signal panel and converts ticker ranks
    into directional signals:
      - Top percentile → bearish (overbought, mean-reversion target)
      - Bottom percentile → bullish (oversold)
      - Middle → neutral

    For momentum signals, invert: top = bullish, bottom = bearish.
    vol_price_divergence is contrarian (top = overbought = sell).

    Returns number of signals registered.
    """
    if as_of_date is None:
        as_of_date = date.today()

    if signal_panel.empty:
        return 0

    latest = signal_panel.iloc[-1].dropna()
    if len(latest) < 5:
        return 0

    now = datetime.now(timezone.utc)
    valid_until = now + timedelta(hours=valid_hours)
    source = f"alpha_research:{signal_name}"

    signals: list[RegisteredSignal] = []
    top_threshold = 1.0 - top_pct
    bottom_threshold = top_pct

    for ticker, rank_value in latest.items():
        if np.isnan(rank_value):
            continue

        # For contrarian signals (vol_price_divergence):
        # high rank = overbought = bearish direction
        if rank_value >= top_threshold:
            direction = Direction.BEARISH
        elif rank_value <= bottom_threshold:
            direction = Direction.BULLISH
        else:
            direction = Direction.NEUTRAL

        # Signal strength: distance from 0.5 center
        strength = abs(rank_value - 0.5) * 2  # 0 at center, 1 at extremes

        sig = RegisteredSignal(
            signal_id=make_signal_id(source, f"{ticker}:{as_of_date}"),
            source_module=source,
            signal_type=SignalType.DIRECTIONAL,
            direction=direction,
            value=float(strength),
            confidence=confidence,
            valid_from=now,
            valid_until=valid_until,
            ticker=str(ticker),
            z_score=float(rank_value - 0.5) * 2,
            freshness_hours=float(valid_hours),
            metadata={"signal_name": signal_name, "rank": float(rank_value)},
            provenance=f"alpha_research/{signal_name} as_of {as_of_date}",
        )
        signals.append(sig)

    if not signals:
        return 0

    return SignalRegistry.register(signals, engine)


def publish_regime_signal(
    engine: Engine,
    signal_name: str,
    state: str,
    confidence: float,
    metadata: dict[str, Any] | None = None,
    valid_hours: int = 24,
) -> int:
    """
    Publish a regime signal (VIX exposure, credit cycle, etc.).

    Regime signals are ticker-agnostic and apply to the whole portfolio.
    """
    now = datetime.now(timezone.utc)
    valid_until = now + timedelta(hours=valid_hours)
    source = f"alpha_research:{signal_name}"

    direction_map = {
        "calm": Direction.BULLISH,
        "expansion": Direction.BULLISH,
        "elevated": Direction.NEUTRAL,
        "stressed": Direction.BEARISH,
        "contraction": Direction.BEARISH,
        "risk-on": Direction.BULLISH,
        "neutral": Direction.NEUTRAL,
        "risk-off": Direction.BEARISH,
    }

    direction = direction_map.get(state, Direction.NEUTRAL)

    sig = RegisteredSignal(
        signal_id=make_signal_id(source, f"regime:{state}:{now.date()}"),
        source_module=source,
        signal_type=SignalType.REGIME,
        direction=direction,
        value=confidence,
        confidence=confidence,
        valid_from=now,
        valid_until=valid_until,
        freshness_hours=float(valid_hours),
        metadata={"state": state, **(metadata or {})},
        provenance=f"alpha_research/{signal_name}",
    )

    return SignalRegistry.register([sig], engine)


def publish_all_alpha_signals(engine: Engine, as_of_date: date | None = None) -> dict:
    """
    Run all alpha research signals and publish to SignalRegistry.

    Returns dict of signal_name → count registered.
    """
    from alpha_research.data.panel_builder import build_price_panel
    from alpha_research.signals.credit_cycle import compute_credit_cycle
    from alpha_research.signals.exposure_scaler import compute_vix_exposure_scalar
    from alpha_research.signals.quanta_alpha import (
        vol_price_divergence,
        vol_regime_adaptive_equity,
        dual_horizon_equity,
    )

    if as_of_date is None:
        as_of_date = date.today()

    results = {}

    # 1. Vol-Price Divergence (proven signal: Sharpe 0.94 on GRID data)
    prices = build_price_panel(engine, start_date=as_of_date - timedelta(days=120), end_date=as_of_date)
    if not prices.empty:
        vpd_signal = vol_price_divergence(prices)
        results["vol_price_divergence"] = publish_factor_signals(
            engine, "vol_price_divergence", vpd_signal, as_of_date
        )

        # 1b. Equity-tuned momentum signals
        vol_regime_eq = vol_regime_adaptive_equity(prices)
        results["vol_regime_equity"] = publish_factor_signals(
            engine, "vol_regime_equity", vol_regime_eq, as_of_date,
            top_pct=0.20, confidence=0.5,
        )

        dual_eq = dual_horizon_equity(prices)
        results["dual_horizon_equity"] = publish_factor_signals(
            engine, "dual_horizon_equity", dual_eq, as_of_date,
            top_pct=0.20, confidence=0.5,
        )

    # 2. VIX Exposure Scalar
    vix_result = compute_vix_exposure_scalar(engine, as_of_date)
    if vix_result.get("regime_hint") != "unknown":
        results["vix_exposure"] = publish_regime_signal(
            engine,
            "vix_exposure",
            vix_result["regime_hint"],
            confidence=min(abs(vix_result.get("ratio", 1.0) - 1.0) * 2, 1.0),
            metadata=vix_result,
        )

    # 3. Credit Cycle
    credit = compute_credit_cycle(engine, as_of_date)
    if credit.get("confidence", 0) > 0:
        results["credit_cycle"] = publish_regime_signal(
            engine,
            "credit_cycle",
            credit["state"],
            confidence=credit["confidence"],
            metadata=credit,
        )

    return results
