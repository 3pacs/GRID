"""
Oracle ↔ TimesFM Adapter.

Bridges TimesFM probabilistic forecasts into the Oracle's signal pipeline.
Converts forecast results into Signal objects that the OracleEngine can
consume alongside its existing signal families (rates, credit, vol, etc.).

Also provides a standalone prediction generator that creates OraclePrediction
objects directly from TimesFM output — used when the oracle wants a
"timeseries_enhanced" model that incorporates forward-looking forecasts.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from oracle.engine import (
    AntiSignal,
    OraclePrediction,
    PredictionType,
    Signal,
    Verdict,
)


def forecast_to_signals(
    forecast_result: Any,
    current_price: float | None = None,
) -> list[Signal]:
    """Convert a TimesFM ForecastResult into Oracle Signal objects.

    Creates signals from the forecast that can be consumed by the
    OracleEngine's signal aggregation pipeline.

    Parameters:
        forecast_result: A ForecastResult from TimesFMForecaster.
        current_price: Current price for directional comparison.

    Returns:
        list[Signal]: Signals derived from the forecast.
    """
    signals: list[Signal] = []

    if not forecast_result or not forecast_result.predictions:
        return signals

    preds = forecast_result.predictions
    lower = forecast_result.lower_bound
    upper = forecast_result.upper_bound
    stds = forecast_result.forecast_std

    # 1. Direction signal — is the forecast trending up or down?
    final_pred = preds[-1]
    first_pred = preds[0]

    if current_price is not None:
        move_pct = (final_pred - current_price) / current_price * 100
        ref_price = current_price
    else:
        move_pct = (final_pred - first_pred) / first_pred * 100 if first_pred != 0 else 0
        ref_price = first_pred

    if move_pct > 1.0:
        direction = "bullish"
    elif move_pct < -1.0:
        direction = "bearish"
    else:
        direction = "neutral"

    # Z-score from forecast magnitude relative to uncertainty
    avg_std = np.mean(stds) if stds else 1.0
    z_score = move_pct / (avg_std / ref_price * 100) if avg_std > 0 and ref_price > 0 else 0.0

    signals.append(Signal(
        name=f"timesfm_direction_{forecast_result.horizon}d",
        family="timeseries_forecast",
        value=round(move_pct, 3),
        z_score=round(z_score, 3),
        direction=direction,
        weight=1.5,  # Higher weight — model-based forward-looking signal
        freshness_hours=0.0,
    ))

    # 2. Confidence signal — how tight is the forecast interval?
    avg_interval_width = np.mean([u - l for u, l in zip(upper, lower)])
    interval_pct = avg_interval_width / ref_price * 100 if ref_price > 0 else 0

    # Tight intervals = high confidence
    if interval_pct < 3.0:
        conf_direction = direction  # Confirms the direction
        conf_value = 0.8
    elif interval_pct < 6.0:
        conf_direction = direction
        conf_value = 0.5
    else:
        conf_direction = "neutral"  # Wide intervals = low confidence
        conf_value = 0.2

    signals.append(Signal(
        name=f"timesfm_confidence_{forecast_result.horizon}d",
        family="timeseries_forecast",
        value=round(conf_value, 3),
        z_score=round(1.0 / (interval_pct + 0.1), 3),
        direction=conf_direction,
        weight=1.0,
        freshness_hours=0.0,
    ))

    # 3. Momentum signal — is the forecast accelerating or decelerating?
    if len(preds) >= 3:
        early_slope = preds[len(preds) // 2] - preds[0]
        late_slope = preds[-1] - preds[len(preds) // 2]
        momentum = late_slope - early_slope  # Positive = accelerating

        if momentum > 0:
            mom_dir = "bullish" if direction == "bullish" else "neutral"
        elif momentum < 0:
            mom_dir = "bearish" if direction == "bearish" else "neutral"
        else:
            mom_dir = "neutral"

        signals.append(Signal(
            name=f"timesfm_momentum_{forecast_result.horizon}d",
            family="timeseries_forecast",
            value=round(momentum, 3),
            z_score=round(momentum / (avg_std + 1e-10), 3),
            direction=mom_dir,
            weight=0.8,
            freshness_hours=0.0,
        ))

    return signals


def forecast_to_anti_signals(
    forecast_result: Any,
    current_signals: list[Signal],
) -> list[AntiSignal]:
    """Check if the TimesFM forecast contradicts existing signals.

    Parameters:
        forecast_result: A ForecastResult from TimesFMForecaster.
        current_signals: Existing signals from other families.

    Returns:
        list[AntiSignal]: Anti-signals where forecast contradicts consensus.
    """
    anti_signals: list[AntiSignal] = []

    if not forecast_result or not forecast_result.predictions:
        return anti_signals

    preds = forecast_result.predictions
    move = preds[-1] - preds[0]
    forecast_dir = "bullish" if move > 0 else "bearish" if move < 0 else "neutral"

    # Count consensus direction from existing signals
    bullish = sum(1 for s in current_signals if s.direction == "bullish")
    bearish = sum(1 for s in current_signals if s.direction == "bearish")

    consensus = "bullish" if bullish > bearish else "bearish" if bearish > bullish else "neutral"

    # If forecast disagrees with consensus, create anti-signal
    if forecast_dir != "neutral" and consensus != "neutral" and forecast_dir != consensus:
        severity = min(1.0, abs(move) / (abs(preds[0]) * 0.05 + 1e-10))

        anti_signals.append(AntiSignal(
            name=f"timesfm_contradicts_{consensus}",
            family="timeseries_forecast",
            value=round(move, 3),
            z_score=0.0,
            contradiction=(
                f"TimesFM {forecast_result.horizon}d forecast is {forecast_dir} "
                f"(move={move:.2f}) but signal consensus is {consensus} "
                f"({bullish}B/{bearish}B)"
            ),
            severity=round(severity, 3),
        ))

    return anti_signals


def forecast_to_prediction(
    forecast_result: Any,
    ticker: str,
    current_price: float,
    signals: list[Signal] | None = None,
    anti_signals: list[AntiSignal] | None = None,
) -> OraclePrediction | None:
    """Create an OraclePrediction directly from a TimesFM forecast.

    Used by the "timeseries_enhanced" oracle model to generate
    standalone predictions backed by the TimesFM foundation model.

    Parameters:
        forecast_result: A ForecastResult from TimesFMForecaster.
        ticker: The ticker symbol.
        current_price: Current market price.
        signals: Supporting signals (TimesFM + other).
        anti_signals: Contradicting anti-signals.

    Returns:
        OraclePrediction or None if forecast is too uncertain.
    """
    if not forecast_result or not forecast_result.predictions:
        return None

    preds = forecast_result.predictions
    target_price = preds[-1]
    expected_move_pct = (target_price - current_price) / current_price * 100

    # Direction
    if expected_move_pct > 0.5:
        direction = "CALL"
    elif expected_move_pct < -0.5:
        direction = "PUT"
    else:
        return None  # Too flat to predict

    # Confidence from interval width
    avg_std = np.mean(forecast_result.forecast_std) if forecast_result.forecast_std else 1.0
    interval_pct = avg_std / current_price * 100 if current_price > 0 else 50
    confidence = max(0.1, min(0.95, 1.0 - interval_pct / 10.0))

    # Expiry from horizon
    expiry = date.today() + timedelta(days=forecast_result.horizon)

    # Build prediction ID
    pred_id = hashlib.sha256(
        f"timesfm:{ticker}:{date.today().isoformat()}:{forecast_result.horizon}".encode()
    ).hexdigest()[:16]

    # Compute signal strength
    all_signals = signals or []
    bull_weight = sum(s.weight for s in all_signals if s.direction == "bullish")
    bear_weight = sum(s.weight for s in all_signals if s.direction == "bearish")
    total_weight = bull_weight + bear_weight
    signal_strength = (bull_weight - bear_weight) / total_weight if total_weight > 0 else 0

    # Coherence
    n_directional = sum(1 for s in all_signals if s.direction != "neutral")
    n_agreeing = sum(
        1 for s in all_signals
        if (s.direction == "bullish" and direction == "CALL")
        or (s.direction == "bearish" and direction == "PUT")
    )
    coherence = n_agreeing / n_directional if n_directional > 0 else 0.0

    return OraclePrediction(
        id=pred_id,
        timestamp=datetime.now(timezone.utc),
        ticker=ticker,
        prediction_type=PredictionType.DIRECTION,
        direction=direction,
        target_price=round(target_price, 2),
        current_price=current_price,
        expiry=expiry,
        confidence=round(confidence, 3),
        expected_move_pct=round(expected_move_pct, 3),
        signals=all_signals,
        anti_signals=anti_signals or [],
        signal_strength=round(signal_strength, 3),
        coherence=round(coherence, 3),
        model_name="timeseries_enhanced",
        model_version=forecast_result.model_version,
        flow_context={
            "forecast_horizon": forecast_result.horizon,
            "forecast_lower": forecast_result.lower_bound[-1] if forecast_result.lower_bound else None,
            "forecast_upper": forecast_result.upper_bound[-1] if forecast_result.upper_bound else None,
        },
    )


def run_timesfm_forecast_cycle(
    engine: Engine,
    tickers: list[str] | None = None,
    horizon: int = 7,
) -> dict[str, Any]:
    """Run TimesFM forecasts for all active tickers and store results.

    Called by the Hermes operator on schedule. Fetches historical data,
    runs batch inference, and stores forecasts in the database.

    Parameters:
        engine: SQLAlchemy database engine.
        tickers: Specific tickers to forecast (None = all active).
        horizon: Forecast horizon in days.

    Returns:
        dict: Summary of the forecast cycle.
    """
    try:
        from timeseries.timesfm_forecaster import get_forecaster
        forecaster = get_forecaster()
    except Exception as exc:
        log.warning("TimesFM forecaster not available: {e}", e=str(exc))
        return {"error": str(exc), "forecasts": 0}

    if not forecaster.is_available:
        return {"error": "timesfm package not installed", "forecasts": 0}

    # Get active tickers if not specified
    if tickers is None:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ticker FROM options_daily_signals
                WHERE signal_date >= CURRENT_DATE - 7
                AND total_oi >= 1000
                ORDER BY ticker
            """)).fetchall()
            tickers = [r[0] for r in rows]

    if not tickers:
        return {"error": "no active tickers", "forecasts": 0}

    # Fetch historical data for all tickers
    series_dict: dict[str, np.ndarray] = {}
    for ticker in tickers:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT obs_date, value FROM raw_series
                WHERE series_id = :sid AND pull_status = 'SUCCESS'
                ORDER BY obs_date ASC
                LIMIT 2048
            """).bindparams(sid=f"YF:{ticker}:close")).fetchall()

        if rows and len(rows) >= 30:
            series_dict[ticker] = np.array(
                [float(r[1]) for r in rows], dtype=np.float32
            )

    if not series_dict:
        return {"error": "no historical data available", "forecasts": 0}

    log.info("TimesFM forecast cycle — {n} tickers", n=len(series_dict))

    # Run batch forecast
    batch_result = forecaster.batch_forecast(
        series_dict=series_dict,
        horizon=horizon,
    )

    # Store forecasts in database
    _ensure_forecast_table(engine)
    stored = 0
    with engine.begin() as conn:
        for sid, fr in batch_result.forecasts.items():
            conn.execute(text("""
                INSERT INTO timeseries_forecasts
                    (ticker, forecast_date, horizon, predictions,
                     lower_bound, upper_bound, forecast_std, model_version)
                VALUES (:ticker, :fdate, :horizon, :preds,
                        :lower, :upper, :std, :model)
                ON CONFLICT (ticker, forecast_date, horizon) DO UPDATE SET
                    predictions = EXCLUDED.predictions,
                    lower_bound = EXCLUDED.lower_bound,
                    upper_bound = EXCLUDED.upper_bound,
                    forecast_std = EXCLUDED.forecast_std,
                    model_version = EXCLUDED.model_version
            """).bindparams(
                ticker=sid,
                fdate=fr.forecast_date,
                horizon=fr.horizon,
                preds=str(fr.predictions),
                lower=str(fr.lower_bound),
                upper=str(fr.upper_bound),
                std=str(fr.forecast_std),
                model=fr.model_version,
            ))
            stored += 1

    log.info(
        "TimesFM forecast cycle complete — {n} forecasts in {t:.1f}s",
        n=stored,
        t=batch_result.elapsed_seconds,
    )

    return {
        "forecasts": stored,
        "tickers": list(series_dict.keys()),
        "elapsed_seconds": round(batch_result.elapsed_seconds, 2),
        "model_version": batch_result.model_version,
    }


def _ensure_forecast_table(engine: Engine) -> None:
    """Create the timeseries_forecasts table if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS timeseries_forecasts (
                ticker TEXT NOT NULL,
                forecast_date DATE NOT NULL,
                horizon INTEGER NOT NULL,
                predictions TEXT NOT NULL,
                lower_bound TEXT NOT NULL,
                upper_bound TEXT NOT NULL,
                forecast_std TEXT NOT NULL,
                model_version TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (ticker, forecast_date, horizon)
            )
        """))
