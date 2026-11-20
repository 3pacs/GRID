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
    if not current_price or current_price <= 0:
        return None
    # A one-step-ahead forecast far from today's price means the history fed
    # to TimesFM ended somewhere else (2026-11-07: series ending in 2012 gave
    # CORT $4.04 against $72.77). The clamp below would hide that as -30%.
    anchor_gap_pct = abs(preds[0] - current_price) / current_price * 100
    if anchor_gap_pct > MAX_ANCHOR_GAP_PCT:
        log.warning(
            "TimesFM forecast for {t} starts at {p0:.2f} vs price {px:.2f} ({g:.0f}% gap); "
            "history does not end at today's price, no prediction",
            t=ticker, p0=float(preds[0]), px=current_price, g=anchor_gap_pct,
        )
        return None
    target_price = preds[-1]
    expected_move_pct = (target_price - current_price) / current_price * 100

    # Clamp expected move to ±30%
    if expected_move_pct < -30.0 or expected_move_pct > 30.0:
        log.debug("Clamping expected_move_pct from {orig} to ±30% for {ticker}",
                   orig=expected_move_pct, ticker=ticker)
        expected_move_pct = max(-30.0, min(30.0, expected_move_pct))

    # Direction
    if expected_move_pct > 0.5:
        direction = "CALL"
    elif expected_move_pct < -0.5:
        direction = "PUT"
    else:
        return None  # Too flat to predict

    # Confidence from interval width; routed through per-model reliability
    # curve when calibrated, falls back to the raw cap otherwise.
    from intelligence.confidence_calibration import calibrate_confidence_default
    avg_std = np.mean(forecast_result.forecast_std) if forecast_result.forecast_std else 1.0
    interval_pct = avg_std / current_price * 100 if current_price > 0 else 50
    confidence = calibrate_confidence_default(
        max(0.1, min(0.95, 1.0 - interval_pct / 10.0)),
        "forecaster_adapter",
    )

    # Use one UTC decision clock for the day, expiry and stored timestamp.
    # A same-day direction or model-version change is a different natural
    # key in oracle_predictions. Reusing the old ticker/day/horizon ID for
    # that key raised a primary-key conflict and rolled back the whole cycle.
    now = datetime.now(timezone.utc)
    expiry = now.date() + timedelta(days=forecast_result.horizon)
    pred_id = hashlib.sha256(
        f"timesfm:v2:{ticker}:{now.date().isoformat()}:{expiry.isoformat()}:"
        f"{direction}:{forecast_result.model_version}".encode()
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
        timestamp=now,
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


# Context for TimesFM: the newest ~4 years of daily adjusted closes. The old
# reader took ``ORDER BY obs_date ASC LIMIT 2048`` over adj_close and close
# together, i.e. the OLDEST 2048 rows: CORT's series ended 2012-05-30 at $4.02
# and INTC's 1988 at $0.57, so every forecast was a years-old price compared
# with today's (66 of 100 active predictions on 2026-11-07 implied -74% to
# -99% or +310% in a week).
HISTORY_POINTS = 1024
# Tiingo is the canonical writer of YF:{ticker}:adj_close (analysis/panel_prices.py);
# yfinance is the fallback. KAGGLE_BULK only holds 2004-2014 closes.
HISTORY_SOURCES: tuple[str, ...] = ("TIINGO", "yfinance")
MAX_HISTORY_STALENESS_DAYS = 7
MIN_HISTORY_POINTS = 30
# One step ahead, a daily close rarely moves this far; beyond it the forecast
# was not anchored on the current price.
MAX_ANCHOR_GAP_PCT = 25.0


def recent_close_history(
    conn: Any,
    ticker: str,
    *,
    today: date | None = None,
    n: int = HISTORY_POINTS,
) -> tuple[np.ndarray | None, str]:
    """Oldest-to-newest adjusted closes ending at the latest accepted date.

    Reads through ``store.observations`` (SUCCESS rows, one vintage per date,
    one source at a time). Returns ``(None, reason)`` when no source has at
    least ``MIN_HISTORY_POINTS`` dates or the newest close is older than
    ``MAX_HISTORY_STALENESS_DAYS``: a forecast from a stale series would be
    compared with today's price.
    """
    from store import observations

    today = today or date.today()
    start = today - timedelta(days=int(n * 1.5) + 14)  # ~252 sessions a year
    reasons: list[str] = []
    for source in HISTORY_SOURCES:
        obs = observations.read_latest_n(conn, f"YF:{ticker}:adj_close", n, source=source, start=start)
        if len(obs) < MIN_HISTORY_POINTS:
            reasons.append(f"{source}: {len(obs)} dates")
            continue
        age = (today - obs[0].obs_date).days
        if age > MAX_HISTORY_STALENESS_DAYS:
            reasons.append(f"{source}: newest close {obs[0].obs_date} ({age}d old)")
            continue
        return np.array([o.value for o in reversed(obs)], dtype=np.float32), source
    return None, "; ".join(reasons) or "no source"


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

    # Fetch the most recent close history for every ticker
    series_dict: dict[str, np.ndarray] = {}
    skipped: dict[str, str] = {}
    with engine.connect() as conn:
        for ticker in tickers:
            history, reason = recent_close_history(conn, ticker)
            if history is None:
                skipped[ticker] = reason
            else:
                series_dict[ticker] = history
    if skipped:
        log.info("TimesFM: {n} tickers skipped (e.g. {ex})", n=len(skipped),
                 ex=dict(list(skipped.items())[:3]))

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
