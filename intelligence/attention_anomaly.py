"""
Attention Anomaly Detector — combines Wikipedia + Google Trends signals.

Scores entities 0-100 based on attention spikes, cross-references with
price action to find attention-before-move patterns.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class AttentionSignal:
    """An attention anomaly signal for an entity."""
    entity_name: str
    score: float            # 0-100
    wikipedia_zscore: float | None
    trends_breakout: float | None
    anomaly_date: date
    ticker: str | None
    price_move_5d: float | None


def score_attention(engine: Engine, lookback_days: int = 7) -> list[AttentionSignal]:
    """Score all entities with recent attention anomalies.

    Combines Wikipedia Z-scores and Google Trends breakout ratios
    into a unified 0-100 attention score.

    Args:
        engine: SQLAlchemy engine.
        lookback_days: Days to look back for anomalies.

    Returns:
        List of AttentionSignal, sorted by score descending.
    """
    cutoff = date.today() - timedelta(days=lookback_days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT entity_name, "
                "MAX(wikipedia_zscore) AS max_wiki_z, "
                "MAX(trends_breakout) AS max_trends, "
                "MAX(combined_score) AS max_score, "
                "MAX(anomaly_date) AS latest_date, "
                "MAX(ticker) AS ticker, "
                "MAX(price_move_5d) AS price_5d "
                "FROM attention_anomaly "
                "WHERE anomaly_date >= :cutoff "
                "GROUP BY entity_name "
                "ORDER BY max_score DESC"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    signals: list[AttentionSignal] = []
    for row in rows:
        # Combine signals: wiki z-score * 20 + trends ratio * 25, cap at 100
        wiki_component = min(abs(row[1] or 0) * 20, 60)
        trends_component = min((row[2] or 0) * 25, 60)
        combined = min(wiki_component + trends_component, 100)

        signals.append(AttentionSignal(
            entity_name=row[0],
            score=round(combined, 1),
            wikipedia_zscore=float(row[1]) if row[1] else None,
            trends_breakout=float(row[2]) if row[2] else None,
            anomaly_date=row[4],
            ticker=row[5],
            price_move_5d=float(row[6]) if row[6] else None,
        ))

    log.info("Attention scoring: {n} entities with anomalies", n=len(signals))
    return signals


def enrich_with_price_action(engine: Engine, signals: list[AttentionSignal]) -> list[AttentionSignal]:
    """Cross-reference attention signals with subsequent price moves.

    Looks up 5-day forward returns for entities that have tickers.

    Args:
        engine: SQLAlchemy engine.
        signals: Attention signals to enrich.

    Returns:
        Enriched signals with price_move_5d populated where possible.
    """
    enriched: list[AttentionSignal] = []

    for sig in signals:
        price_move = None
        if sig.ticker:
            price_move = _get_forward_return(engine, sig.ticker, sig.anomaly_date, days=5)

        enriched.append(AttentionSignal(
            entity_name=sig.entity_name,
            score=sig.score,
            wikipedia_zscore=sig.wikipedia_zscore,
            trends_breakout=sig.trends_breakout,
            anomaly_date=sig.anomaly_date,
            ticker=sig.ticker,
            price_move_5d=price_move if price_move is not None else sig.price_move_5d,
        ))

    return enriched


def _get_forward_return(
    engine: Engine,
    ticker: str,
    start_date: date,
    days: int = 5,
) -> float | None:
    """Get forward return for a ticker from the resolved_series.

    Args:
        ticker: Stock ticker.
        start_date: Date of the attention signal.
        days: Forward return window.

    Returns:
        Percentage return, or None if data unavailable.
    """
    end_date = start_date + timedelta(days=days + 3)  # buffer for weekends

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT obs_date, value FROM resolved_series "
                    "WHERE feature_id IN (SELECT id FROM feature_registry WHERE name = :ticker) "
                    "AND obs_date BETWEEN :start AND :end "
                    "ORDER BY obs_date"
                ),
                {"ticker": f"price:{ticker}", "start": start_date, "end": end_date},
            ).fetchall()

        if len(rows) >= 2:
            start_price = float(rows[0][1])
            end_price = float(rows[-1][1])
            if start_price > 0:
                return round((end_price - start_price) / start_price * 100, 2)
    except Exception as exc:
        log.debug("Price lookup failed for {t}: {e}", t=ticker, e=str(exc))

    return None


def get_alerts(engine: Engine, threshold: float = 60.0) -> list[dict[str, Any]]:
    """Get high-priority attention alerts.

    Args:
        engine: SQLAlchemy engine.
        threshold: Minimum score to alert on.

    Returns:
        List of alert dicts for entities above threshold.
    """
    signals = score_attention(engine)
    alerts: list[dict[str, Any]] = []

    for sig in signals:
        if sig.score >= threshold:
            alerts.append({
                "entity": sig.entity_name,
                "score": sig.score,
                "wikipedia_zscore": sig.wikipedia_zscore,
                "trends_breakout": sig.trends_breakout,
                "date": sig.anomaly_date.isoformat(),
                "ticker": sig.ticker,
                "price_move_5d": sig.price_move_5d,
                "confidence": "confirmed" if sig.wikipedia_zscore and sig.trends_breakout else "estimated",
            })

    return alerts
