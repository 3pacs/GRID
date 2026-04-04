"""
GRID — TimesFM Forecasting Service.

Zero-shot time-series forecasting using Google's TimesFM 2.5 (200M params).
Runs on the local 3090 GPU.  Provides:
  - Batch forecasting of all resolved signals
  - Quantile bands (10th–90th percentile) for probabilistic scoring
  - Per-signal forecast caching with configurable TTL
  - Integration point for thesis_scorer (forward-looking signal inputs)

Architecture:
  resolved_series (DB) → extract → TimesFM → signal_forecasts (DB)
  thesis_scorer reads signal_forecasts for forward-looking model inputs.
"""

from __future__ import annotations

import os
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Sequence

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────

MODEL_REPO = "google/timesfm-2.5-200m-pytorch"
FALLBACK_REPO = "google/timesfm-1.0-200m-pytorch"

DEFAULT_CONTEXT = 2048      # 8 years daily on CPU; bump to 16256 when P100 GPU available
DEFAULT_HORIZON = 128       # 128 trading days ahead (~6 months)
DEFAULT_BATCH_SIZE = 32     # per-core batch size
MIN_SERIES_LENGTH = 64      # skip signals shorter than this
FORECAST_TTL_HOURS = 4      # re-forecast if older than this
STALE_FORECAST_HOURS = 24   # mark forecast as stale after this


# ── Forecast result (immutable) ──────────────────────────────────────

@dataclass(frozen=True)
class SignalForecast:
    """Immutable forecast output for one signal."""
    feature_id: int
    feature_name: str
    horizon: int
    point_forecast: tuple[float, ...]          # (horizon,)
    quantile_10: tuple[float, ...]             # pessimistic
    quantile_50: tuple[float, ...]             # median
    quantile_90: tuple[float, ...]             # optimistic
    last_observed: float                       # final input value
    last_obs_date: str                         # ISO date
    forecast_start_date: str                   # first forecast date
    direction: str                             # UP / DOWN / FLAT
    expected_move_pct: float                   # median endpoint vs last
    confidence_band_pct: float                 # (q90 - q10) / q50 width
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ── Model loader (shared pool) ───────────────────────────────────────


def _load_model(
    context: int = DEFAULT_CONTEXT,
    horizon: int = DEFAULT_HORIZON,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    """Return the shared TimesFM model from the pool.

    Delegates to timeseries._model_pool so both inference/timesfm_service
    and timeseries/timesfm_forecaster share one GPU instance.
    """
    from timeseries._model_pool import get_timesfm_model

    model, _version = get_timesfm_model(
        context_len=context,
        horizon_len=horizon,
        batch_size=batch_size,
    )
    return model


# ── Data extraction ──────────────────────────────────────────────────

def _extract_signals(
    engine: Engine,
    feature_ids: list[int] | None = None,
    min_length: int = MIN_SERIES_LENGTH,
) -> list[dict[str, Any]]:
    """Pull time-series data from resolved_series.

    Returns list of {feature_id, name, family, dates, values} dicts,
    sorted by obs_date ascending.  Skips series shorter than min_length.
    """
    with engine.connect() as conn:
        if feature_ids:
            # Specific features
            rows = conn.execute(text("""
                SELECT fr.id, fr.name, fr.family,
                       rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.id = ANY(:ids)
                AND rs.value IS NOT NULL
                ORDER BY fr.id, rs.obs_date ASC
            """), {"ids": feature_ids}).fetchall()
        else:
            # All model-eligible features with enough data
            rows = conn.execute(text("""
                SELECT fr.id, fr.name, fr.family,
                       rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.model_eligible = true
                AND rs.value IS NOT NULL
                ORDER BY fr.id, rs.obs_date ASC
            """)).fetchall()

    # Group by feature
    signals: dict[int, dict] = {}
    for fid, name, family, obs_date, value in rows:
        if fid not in signals:
            signals[fid] = {
                "feature_id": fid,
                "name": name,
                "family": family,
                "dates": [],
                "values": [],
            }
        signals[fid]["dates"].append(obs_date)
        signals[fid]["values"].append(float(value))

    # Filter by minimum length
    result = [s for s in signals.values() if len(s["values"]) >= min_length]
    log.info("Extracted {n} signals (≥{min} obs) for forecasting",
             n=len(result), min=min_length)
    return result


# ── Core forecasting ─────────────────────────────────────────────────

def forecast_signals(
    engine: Engine,
    feature_ids: list[int] | None = None,
    horizon: int = DEFAULT_HORIZON,
    context: int = DEFAULT_CONTEXT,
    force: bool = False,
) -> list[SignalForecast]:
    """Run TimesFM on extracted signals and return forecasts.

    Args:
        engine: SQLAlchemy engine.
        feature_ids: Specific features to forecast (None = all eligible).
        horizon: Forecast steps ahead.
        context: Input context window.
        force: Re-forecast even if recent forecast exists.

    Returns:
        List of SignalForecast objects.
    """
    model = _load_model(context=context, horizon=horizon)
    signals = _extract_signals(engine, feature_ids=feature_ids)

    if not signals:
        log.warning("No signals to forecast")
        return []

    # Skip signals that already have fresh forecasts (unless forced)
    if not force:
        signals = _filter_stale(engine, signals)
        if not signals:
            log.info("All forecasts are fresh — nothing to do")
            return []

    # Prepare batch input
    inputs = []
    for sig in signals:
        arr = np.array(sig["values"], dtype=np.float64)
        # Use last `context` values if series is longer
        if len(arr) > context:
            arr = arr[-context:]
        inputs.append(arr)

    log.info("Forecasting {n} signals, horizon={h}, context={c}",
             n=len(inputs), h=horizon, c=context)

    from timeseries._model_pool import get_timesfm_model
    _, version = get_timesfm_model(context_len=context, horizon_len=horizon)

    t0 = _time.time()
    try:
        if version.startswith("v2.5"):
            # v2.5 API: forecast(horizon=N, inputs=[np arrays])
            point_fc, quantile_fc = model.forecast(horizon=horizon, inputs=inputs)
        else:
            # v1 API: forecast(inputs, freq)
            freq = [0] * len(inputs)  # 0 = daily
            point_fc, quantile_fc = model.forecast(inputs, freq)
    except Exception as exc:
        log.error("TimesFM forecast failed: {e}", e=str(exc))
        raise
    elapsed = _time.time() - t0
    log.info("TimesFM inference done in {t:.1f}s ({n} signals)",
             t=elapsed, n=len(inputs))

    # Build forecast objects
    forecasts: list[SignalForecast] = []
    for i, sig in enumerate(signals):
        point = point_fc[i]            # shape: (horizon,)
        quants = quantile_fc[i]        # shape: (horizon, 10) — [mean, q10..q90]

        q10 = quants[:, 1] if quants.ndim == 2 else point
        q50 = quants[:, 5] if quants.ndim == 2 else point
        q90 = quants[:, 9] if quants.ndim == 2 else point

        last_val = sig["values"][-1]
        last_date = sig["dates"][-1]
        median_endpoint = float(q50[-1])

        # Direction and move
        if last_val != 0:
            move_pct = (median_endpoint - last_val) / abs(last_val) * 100
        else:
            move_pct = 0.0

        if move_pct > 1.0:
            direction = "UP"
        elif move_pct < -1.0:
            direction = "DOWN"
        else:
            direction = "FLAT"

        # Confidence band width at endpoint
        q10_end = float(q10[-1])
        q90_end = float(q90[-1])
        if abs(median_endpoint) > 1e-10:
            band_pct = (q90_end - q10_end) / abs(median_endpoint) * 100
        else:
            band_pct = 0.0

        fc = SignalForecast(
            feature_id=sig["feature_id"],
            feature_name=sig["name"],
            horizon=horizon,
            point_forecast=tuple(float(x) for x in point),
            quantile_10=tuple(float(x) for x in q10),
            quantile_50=tuple(float(x) for x in q50),
            quantile_90=tuple(float(x) for x in q90),
            last_observed=last_val,
            last_obs_date=str(last_date),
            forecast_start_date=str(last_date),
            direction=direction,
            expected_move_pct=round(move_pct, 2),
            confidence_band_pct=round(band_pct, 2),
        )
        forecasts.append(fc)

    # Persist to database
    _save_forecasts(engine, forecasts)

    return forecasts


def _filter_stale(
    engine: Engine,
    signals: list[dict],
) -> list[dict]:
    """Filter out signals that already have fresh forecasts."""
    fids = [s["feature_id"] for s in signals]
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT feature_id, generated_at
            FROM signal_forecasts
            WHERE feature_id = ANY(:ids)
            AND generated_at > NOW() - INTERVAL ':ttl hours'
        """.replace(":ttl hours", f"{FORECAST_TTL_HOURS} hours")),
            {"ids": fids}
        ).fetchall()

    fresh_ids = {r[0] for r in rows}
    stale = [s for s in signals if s["feature_id"] not in fresh_ids]
    log.debug("Filtered: {fresh} fresh, {stale} need refresh",
              fresh=len(fresh_ids), stale=len(stale))
    return stale


# ── Persistence ──────────────────────────────────────────────────────

def _save_forecasts(engine: Engine, forecasts: list[SignalForecast]) -> None:
    """Upsert forecasts into signal_forecasts table."""
    if not forecasts:
        return

    with engine.begin() as conn:
        for fc in forecasts:
            conn.execute(text("""
                INSERT INTO signal_forecasts
                    (feature_id, feature_name, horizon,
                     point_forecast, quantile_10, quantile_50, quantile_90,
                     last_observed, last_obs_date, forecast_start_date,
                     direction, expected_move_pct, confidence_band_pct,
                     generated_at)
                VALUES
                    (:fid, :fname, :horizon,
                     :point, :q10, :q50, :q90,
                     :last_val, :last_date, :fc_start,
                     :direction, :move_pct, :band_pct,
                     :gen_at)
                ON CONFLICT (feature_id)
                DO UPDATE SET
                    horizon = EXCLUDED.horizon,
                    point_forecast = EXCLUDED.point_forecast,
                    quantile_10 = EXCLUDED.quantile_10,
                    quantile_50 = EXCLUDED.quantile_50,
                    quantile_90 = EXCLUDED.quantile_90,
                    last_observed = EXCLUDED.last_observed,
                    last_obs_date = EXCLUDED.last_obs_date,
                    forecast_start_date = EXCLUDED.forecast_start_date,
                    direction = EXCLUDED.direction,
                    expected_move_pct = EXCLUDED.expected_move_pct,
                    confidence_band_pct = EXCLUDED.confidence_band_pct,
                    generated_at = EXCLUDED.generated_at
            """), {
                "fid": fc.feature_id,
                "fname": fc.feature_name,
                "horizon": fc.horizon,
                "point": list(fc.point_forecast),
                "q10": list(fc.quantile_10),
                "q50": list(fc.quantile_50),
                "q90": list(fc.quantile_90),
                "last_val": fc.last_observed,
                "last_date": fc.last_obs_date,
                "fc_start": fc.forecast_start_date,
                "direction": fc.direction,
                "move_pct": fc.expected_move_pct,
                "band_pct": fc.confidence_band_pct,
                "gen_at": fc.generated_at,
            })

    log.info("Saved {n} forecasts to signal_forecasts", n=len(forecasts))


# ── Query helpers (for thesis_scorer integration) ────────────────────

def get_forecast(engine: Engine, feature_name: str) -> dict[str, Any] | None:
    """Get the latest forecast for a named feature.

    Returns None if no forecast exists or it's too stale.
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT feature_id, feature_name, horizon,
                   point_forecast, quantile_10, quantile_50, quantile_90,
                   last_observed, last_obs_date, forecast_start_date,
                   direction, expected_move_pct, confidence_band_pct,
                   generated_at
            FROM signal_forecasts
            WHERE feature_name = :name
            ORDER BY generated_at DESC LIMIT 1
        """), {"name": feature_name}).fetchone()

    if not row:
        return None

    gen_at = row[13]
    if isinstance(gen_at, str):
        gen_at = datetime.fromisoformat(gen_at)
    age_hours = (datetime.now(timezone.utc) - gen_at.replace(tzinfo=timezone.utc)).total_seconds() / 3600

    return {
        "feature_id": row[0],
        "feature_name": row[1],
        "horizon": row[2],
        "point_forecast": row[3],
        "quantile_10": row[4],
        "quantile_50": row[5],
        "quantile_90": row[6],
        "last_observed": row[7],
        "last_obs_date": str(row[8]),
        "direction": row[10],
        "expected_move_pct": row[11],
        "confidence_band_pct": row[12],
        "age_hours": round(age_hours, 1),
        "is_stale": age_hours > STALE_FORECAST_HOURS,
    }


def get_forecasts_by_family(
    engine: Engine,
    family: str,
) -> list[dict[str, Any]]:
    """Get all forecasts for a feature family (e.g., 'equity', 'macro')."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sf.feature_name, sf.direction, sf.expected_move_pct,
                   sf.confidence_band_pct, sf.generated_at
            FROM signal_forecasts sf
            JOIN feature_registry fr ON sf.feature_id = fr.id
            WHERE fr.family = :family
            ORDER BY ABS(sf.expected_move_pct) DESC
        """), {"family": family}).fetchall()

    return [
        {
            "feature_name": r[0],
            "direction": r[1],
            "expected_move_pct": r[2],
            "confidence_band_pct": r[3],
            "generated_at": str(r[4]),
        }
        for r in rows
    ]


def get_forecast_summary(engine: Engine) -> dict[str, Any]:
    """Aggregate forecast statistics across all signals.

    Returns a summary useful for the thesis scorer:
    - How many signals are forecasted UP / DOWN / FLAT
    - Average expected move by family
    - Overall market direction from signal consensus
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sf.direction, COUNT(*) as cnt,
                   AVG(sf.expected_move_pct) as avg_move,
                   AVG(sf.confidence_band_pct) as avg_band
            FROM signal_forecasts sf
            WHERE sf.generated_at > NOW() - INTERVAL '24 hours'
            GROUP BY sf.direction
        """)).fetchall()

        family_rows = conn.execute(text("""
            SELECT fr.family, sf.direction,
                   COUNT(*) as cnt,
                   AVG(sf.expected_move_pct) as avg_move
            FROM signal_forecasts sf
            JOIN feature_registry fr ON sf.feature_id = fr.id
            WHERE sf.generated_at > NOW() - INTERVAL '24 hours'
            GROUP BY fr.family, sf.direction
            ORDER BY fr.family
        """)).fetchall()

    direction_counts = {r[0]: {"count": r[1], "avg_move": round(r[2], 2), "avg_band": round(r[3], 2)} for r in rows}
    total = sum(d["count"] for d in direction_counts.values())

    # Build family breakdown
    families: dict[str, dict] = {}
    for fam, direction, cnt, avg_move in family_rows:
        if fam not in families:
            families[fam] = {"UP": 0, "DOWN": 0, "FLAT": 0, "net_move": 0.0}
        families[fam][direction] = cnt
        families[fam]["net_move"] += avg_move * cnt

    for fam in families:
        fam_total = families[fam]["UP"] + families[fam]["DOWN"] + families[fam]["FLAT"]
        if fam_total > 0:
            families[fam]["net_move"] = round(families[fam]["net_move"] / fam_total, 2)

    # Consensus direction
    up_count = direction_counts.get("UP", {}).get("count", 0)
    down_count = direction_counts.get("DOWN", {}).get("count", 0)
    if total > 0:
        up_pct = up_count / total * 100
        down_pct = down_count / total * 100
    else:
        up_pct = down_pct = 0

    if up_pct > 60:
        consensus = "BULLISH"
    elif down_pct > 60:
        consensus = "BEARISH"
    else:
        consensus = "MIXED"

    return {
        "total_forecasted": total,
        "direction_counts": direction_counts,
        "consensus": consensus,
        "up_pct": round(up_pct, 1),
        "down_pct": round(down_pct, 1),
        "families": families,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
