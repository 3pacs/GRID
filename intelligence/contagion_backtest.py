"""Contagion backtest scorer.

Given a window in days (7/14/30), this module:

1. Pulls ``contagion_predictions`` whose ``simulated_at`` falls inside
   ``[NOW - (days + 1), NOW - days]`` — i.e. predictions that are exactly
   ``days`` days old (±1 day of slack to accommodate the daily scheduler).
2. For each ticker inside each prediction's ``ranked_impact``, fetches:
     - the close price at ``simulated_at`` (or the most recent before)
     - the close price at ``simulated_at + days`` (or the most recent
       before the horizon)
   from ``raw_series`` using the ``YF:{ticker}:close`` series id convention.
3. Computes the actual realised move pct = (end - start) / start.
4. Scores the prediction against the predicted margin-impact sign and
   magnitude:

   Accuracy policy:
       - 0.0  -> wrong direction
       - 0.5  -> right direction, magnitude ratio outside 50–150% band
       - 1.0  -> right direction, magnitude ratio within 50–150% band

   Interpolated linearly between 0.5 and 1.0 when within the band.

5. Upserts one row per (prediction, ticker, days) into
   ``contagion_backtest_results`` — so re-running the scorer is idempotent.

The scorer never raises on bad data: unscoreable predictions (missing
prices, zero start, etc.) are recorded with ``accuracy_score = NULL`` so
the operator can see *how many* rows failed, not just the successes.

Public API
----------
    score_predictions(engine, as_of_days_ago: int = 7) -> int
    score_all_windows(engine) -> dict[int, int]
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import NAMESPACE_URL, UUID, uuid5

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from contracts.correlation import (
    get_current_correlation_id,
    new_correlation_id,
)
from contracts.emit import emit
from contracts.schemas import PredictionScored


SCORE_WINDOWS: tuple[int, ...] = (7, 14, 30)

# Producer module tag for emitted PredictionScored contracts.
_PRODUCER_MODULE: str = "intelligence.contagion_backtest"

# Deterministic UUID namespace for mapping int contagion_predictions.id →
# UUID prediction_id. The PredictionScored contract schema requires UUIDs,
# but contagion_predictions is a bigint-keyed table.
_CONTAGION_NAMESPACE: UUID = uuid5(
    NAMESPACE_URL, "grid://contagion_predictions"
)

# Accuracy → verdict bucket thresholds.
_VERDICT_HIT_THRESHOLD: float = 0.8
_VERDICT_PARTIAL_THRESHOLD: float = 0.5

# Predicted margin hits are sometimes 0.0001% — scoring a ~0 prediction
# against a real move is meaningless. Skip anything below this threshold.
_MIN_PREDICTED_MAGNITUDE: float = 1e-5

# Band around the predicted magnitude that still counts as "right magnitude".
_MAG_RATIO_LOW: float = 0.5
_MAG_RATIO_HIGH: float = 1.5


@dataclass(frozen=True)
class _Prediction:
    id: int
    simulated_at: datetime
    ranked_impact: list[dict[str, Any]]


def _fetch_predictions(
    conn: Any,
    days: int,
) -> list[_Prediction]:
    """Return predictions whose ``simulated_at`` is between NOW - (days + 1)
    and NOW - days (i.e. approximately ``days`` days old)."""
    try:
        rows = conn.execute(
            text(
                """
                SELECT id, simulated_at, ranked_impact
                FROM contagion_predictions
                WHERE simulated_at <= NOW() - (:days || ' days')::INTERVAL
                  AND simulated_at >  NOW() - ((:days + 1) || ' days')::INTERVAL
                ORDER BY id
                """
            ),
            {"days": int(days)},
        ).fetchall()
    except Exception as exc:
        log.warning("contagion_backtest: prediction fetch failed: {e}", e=str(exc))
        return []

    out: list[_Prediction] = []
    for r in rows:
        pid = int(r[0])
        sim_at = r[1]
        raw_impact = r[2]
        if isinstance(raw_impact, str):
            try:
                raw_impact = json.loads(raw_impact)
            except Exception:
                raw_impact = []
        if not isinstance(raw_impact, list):
            raw_impact = []
        out.append(_Prediction(id=pid, simulated_at=sim_at, ranked_impact=raw_impact))
    return out


def _fetch_close_price(
    conn: Any,
    ticker: str,
    target: datetime,
) -> tuple[float | None, datetime | None]:
    """Return the most recent close price on or before ``target``.

    Uses the ``YF:{ticker}:close`` series id convention. Returns
    ``(None, None)`` on any failure or no data.
    """
    if not ticker:
        return None, None
    series_id = f"YF:{ticker.upper()}:close"
    try:
        row = conn.execute(
            text(
                """
                SELECT value, obs_date
                FROM raw_series
                WHERE series_id = :sid
                  AND obs_date <= :d
                  AND value IS NOT NULL
                ORDER BY obs_date DESC
                LIMIT 1
                """
            ),
            {"sid": series_id, "d": target.date() if hasattr(target, "date") else target},
        ).fetchone()
    except Exception as exc:
        log.debug(
            "contagion_backtest: price fetch failed for {t}: {e}",
            t=ticker, e=str(exc),
        )
        return None, None
    if row is None or row[0] is None:
        return None, None
    try:
        return float(row[0]), row[1]
    except (TypeError, ValueError):
        return None, None


def compute_accuracy(
    predicted_margin_impact_pct: float,
    actual_price_move_pct: float,
) -> float:
    """Score a prediction against an actual move.

    Returns a value in [0.0, 1.0]:
        - 0.0 if the directional sign is wrong (or one side is ~0)
        - 0.5 if direction matches but magnitude is off (>1.5x or <0.5x)
        - 1.0 if direction matches and magnitude ratio is in [0.5, 1.5]
        - linearly interpolated in between.

    A predicted margin hit of -5% means we expect the stock to go DOWN
    roughly 5%. ``actual_price_move_pct`` is the realised move, e.g.
    -0.018 for -1.8%.
    """
    if predicted_margin_impact_pct is None or actual_price_move_pct is None:
        return 0.0
    p = float(predicted_margin_impact_pct)
    a = float(actual_price_move_pct)

    if abs(p) < _MIN_PREDICTED_MAGNITUDE:
        # Prediction was effectively "no move" — can't score it.
        return 0.0

    # Directional check. Treat ~0 actual as "wrong" if prediction was non-zero.
    if a == 0.0 or (p > 0) != (a > 0):
        # Sign mismatch (or actual == 0) => 0.0
        # Special case: both negative or both positive => same sign => score later
        if (p < 0 and a < 0) or (p > 0 and a > 0):
            pass
        else:
            return 0.0

    ratio = abs(a) / abs(p)
    if _MAG_RATIO_LOW <= ratio <= _MAG_RATIO_HIGH:
        # Within band: linearly interpolate 0.5 -> 1.0 -> 0.5
        # Peak at ratio = 1.0
        if ratio <= 1.0:
            # 0.5 -> 1.0 as ratio goes 0.5 -> 1.0
            return 0.5 + (ratio - _MAG_RATIO_LOW) / (1.0 - _MAG_RATIO_LOW) * 0.5
        # 1.0 -> 0.5 as ratio goes 1.0 -> 1.5
        return 1.0 - (ratio - 1.0) / (_MAG_RATIO_HIGH - 1.0) * 0.5

    # Right direction, wrong magnitude.
    return 0.5


def _upsert_result(
    conn: Any,
    prediction_id: int,
    ticker: str,
    predicted_margin: float | None,
    predicted_rev_risk: float | None,
    scored_at_days: int,
    actual_move: float | None,
    price_start: float | None,
    price_end: float | None,
    accuracy: float | None,
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO contagion_backtest_results (
                prediction_id, ticker,
                predicted_margin_impact_pct, predicted_revenue_at_risk_usd,
                scored_at_days, actual_price_move_pct,
                price_start, price_end, accuracy_score
            ) VALUES (
                :pid, :ticker,
                :pm, :prr,
                :days, :actual,
                :ps, :pe, :acc
            )
            ON CONFLICT (prediction_id, ticker, scored_at_days) DO UPDATE SET
                predicted_margin_impact_pct = EXCLUDED.predicted_margin_impact_pct,
                predicted_revenue_at_risk_usd = EXCLUDED.predicted_revenue_at_risk_usd,
                actual_price_move_pct = EXCLUDED.actual_price_move_pct,
                price_start = EXCLUDED.price_start,
                price_end = EXCLUDED.price_end,
                accuracy_score = EXCLUDED.accuracy_score,
                scored_at = NOW()
            """
        ),
        {
            "pid": int(prediction_id),
            "ticker": ticker,
            "pm": float(predicted_margin) if predicted_margin is not None else None,
            "prr": float(predicted_rev_risk) if predicted_rev_risk is not None else None,
            "days": int(scored_at_days),
            "actual": float(actual_move) if actual_move is not None else None,
            "ps": float(price_start) if price_start is not None else None,
            "pe": float(price_end) if price_end is not None else None,
            "acc": float(accuracy) if accuracy is not None else None,
        },
    )


def _iter_impact_tickers(
    ranked_impact: Iterable[dict[str, Any]],
    limit: int = 25,
) -> list[tuple[str, float, float]]:
    """Extract (ticker, predicted_margin, predicted_rev_risk) triples from
    the ``ranked_impact`` list. Only keeps the top ``limit`` by |margin|
    to avoid scoring every long-tail downstream node.
    """
    out: list[tuple[str, float, float]] = []
    for entry in ranked_impact or []:
        if not isinstance(entry, dict):
            continue
        tid = entry.get("id") or entry.get("ticker")
        if not tid or not isinstance(tid, str):
            continue
        # Only "ticker-like" ids — commodity seeds like "cocoa_beans" have no
        # price series, skip them cheaply.
        if "_" in tid or len(tid) > 8:
            continue
        try:
            pm = float(entry.get("margin_impact_pct") or 0.0)
        except (TypeError, ValueError):
            pm = 0.0
        try:
            prr = float(entry.get("revenue_at_risk_usd") or 0.0)
        except (TypeError, ValueError):
            prr = 0.0
        out.append((tid.strip().upper(), pm, prr))
    out.sort(key=lambda x: abs(x[1]), reverse=True)
    return out[:limit]


def _prediction_uuid(prediction_id: int, ticker: str, days: int) -> UUID:
    """Return a deterministic UUID for a (prediction, ticker, window) row.

    The ``PredictionScored`` contract requires a UUID ``prediction_id``, but
    ``contagion_predictions`` uses a bigint primary key and each prediction
    fans out to many (ticker, days) backtest rows. We derive a stable UUID5
    from the triple so replays and postmortems can resolve back to the
    original row without a lookup table.
    """
    name = f"{int(prediction_id)}::{ticker.upper()}::{int(days)}"
    return uuid5(_CONTAGION_NAMESPACE, name)


def _verdict_for(accuracy: float | None) -> str:
    """Bucket a scalar accuracy into the PredictionScored Literal set."""
    if accuracy is None:
        return "MISS"
    if accuracy >= _VERDICT_HIT_THRESHOLD:
        return "HIT"
    if accuracy >= _VERDICT_PARTIAL_THRESHOLD:
        return "PARTIAL"
    return "MISS"


def _direction_for(value: float | None) -> str:
    """Bucket a signed move into the PredictionScored direction Literal set."""
    if value is None:
        return "FLAT"
    if value > 0:
        return "UP"
    if value < 0:
        return "DOWN"
    return "FLAT"


def _emit_prediction_scored(
    *,
    prediction_id: int,
    ticker: str,
    scored_at_days: int,
    predicted_margin: float | None,
    actual_move: float | None,
    accuracy: float | None,
) -> None:
    """Emit a ``PredictionScored`` contract for one scored backtest row.

    Failure is non-fatal: any exception (serialisation, bus down, audit
    write) is logged at warning level and swallowed so that the scoring
    loop always makes forward progress. This is the SYNTH-37 hook that
    unblocks SYNTH-19/20/21 downstream handlers.
    """
    try:
        ambient_cid = get_current_correlation_id() or new_correlation_id()
        acc_value = float(accuracy) if accuracy is not None else 0.0
        # Brier component proxy: squared distance from the ideal 1.0 score.
        # Real Brier uses (probability - outcome)^2; we reuse the shape so
        # calibration dashboards can consume the series uniformly.
        brier_component = (1.0 - acc_value) ** 2
        emit(
            PredictionScored(
                producer_module=_PRODUCER_MODULE,
                correlation_id=ambient_cid,
                prediction_id=_prediction_uuid(
                    prediction_id, ticker, scored_at_days
                ),
                decision_id=int(prediction_id),
                ticker=ticker.upper(),
                verdict=_verdict_for(accuracy),
                expected_direction=_direction_for(predicted_margin),
                realized_direction=_direction_for(actual_move),
                confidence=acc_value,
                brier_component=brier_component,
                signals_used=[],
                model_weights_at_prediction={},
            )
        )
    except Exception as exc:
        log.warning(
            "contagion_backtest emit failed (non-fatal) for pid={pid} "
            "ticker={t} days={d}: {e}",
            pid=prediction_id, t=ticker, d=scored_at_days, e=str(exc),
        )


def score_predictions(engine: Engine, as_of_days_ago: int = 7) -> int:
    """Score every ``contagion_predictions`` row that is exactly
    ``as_of_days_ago`` days old against actual ``raw_series`` moves.

    Returns the number of ``contagion_backtest_results`` rows written
    (including upserts). See module docstring for the scoring algorithm.
    """
    if as_of_days_ago <= 0:
        raise ValueError("as_of_days_ago must be > 0")

    written = 0
    try:
        with engine.connect() as conn:
            predictions = _fetch_predictions(conn, as_of_days_ago)
    except Exception as exc:
        log.warning("contagion_backtest: cannot open connection: {e}", e=str(exc))
        return 0

    if not predictions:
        log.debug(
            "contagion_backtest: no predictions ~{d}d old",
            d=as_of_days_ago,
        )
        return 0

    for pred in predictions:
        sim_at = pred.simulated_at
        if sim_at is None:
            continue
        # Ensure tz-aware for arithmetic
        if sim_at.tzinfo is None:
            sim_at = sim_at.replace(tzinfo=timezone.utc)
        horizon = sim_at + timedelta(days=as_of_days_ago)

        impact_tickers = _iter_impact_tickers(pred.ranked_impact)
        if not impact_tickers:
            continue

        try:
            with engine.begin() as conn:
                for ticker, predicted_margin, predicted_rev_risk in impact_tickers:
                    price_start, _ = _fetch_close_price(conn, ticker, sim_at)
                    price_end, _ = _fetch_close_price(conn, ticker, horizon)

                    actual_move: float | None = None
                    accuracy: float | None = None
                    if (
                        price_start is not None
                        and price_end is not None
                        and price_start > 0
                    ):
                        actual_move = (price_end - price_start) / price_start
                        accuracy = compute_accuracy(predicted_margin, actual_move)

                    _upsert_result(
                        conn,
                        prediction_id=pred.id,
                        ticker=ticker,
                        predicted_margin=predicted_margin,
                        predicted_rev_risk=predicted_rev_risk,
                        scored_at_days=as_of_days_ago,
                        actual_move=actual_move,
                        price_start=price_start,
                        price_end=price_end,
                        accuracy=accuracy,
                    )
                    written += 1

                    # SYNTH-37: fan the scored row out onto the contracts
                    # bus so downstream handlers (oracle weight updates,
                    # journal writers, calibration dashboards) can react.
                    # Emit is non-fatal — see ``_emit_prediction_scored``.
                    _emit_prediction_scored(
                        prediction_id=pred.id,
                        ticker=ticker,
                        scored_at_days=as_of_days_ago,
                        predicted_margin=predicted_margin,
                        actual_move=actual_move,
                        accuracy=accuracy,
                    )
        except Exception as exc:
            log.warning(
                "contagion_backtest: scoring prediction {pid} failed: {e}",
                pid=pred.id, e=str(exc),
            )

    log.info(
        "contagion_backtest: wrote {n} rows for {d}d window",
        n=written, d=as_of_days_ago,
    )
    return written


def score_all_windows(engine: Engine) -> dict[int, int]:
    """Run ``score_predictions`` for each of the 7/14/30 day windows and
    return a per-window row count."""
    result: dict[int, int] = {}
    for days in SCORE_WINDOWS:
        try:
            result[days] = score_predictions(engine, as_of_days_ago=days)
        except Exception as exc:
            log.warning(
                "contagion_backtest: {d}d window failed: {e}",
                d=days, e=str(exc),
            )
            result[days] = 0
    return result


__all__ = [
    "SCORE_WINDOWS",
    "compute_accuracy",
    "score_predictions",
    "score_all_windows",
]
