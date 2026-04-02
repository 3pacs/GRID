"""
Gemma 270M signal classification for the ingestion pipeline.

Automatically classifies incoming signals by domain and urgency
using the Gemma 3 270M signal_classifier micro model. Runs on CPU
so it doesn't compete with GPU-bound inference.

Integration points:
  - Called after new signals are inserted into signal_registry
  - Updates signals with category + urgency labels
  - Used by the Hermes operator for triage and alerting
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class ClassificationResult:
    """Result of classifying a signal.

    Attributes:
        signal_id: ID of the classified signal.
        category: Domain category (rates, credit, equity, etc.).
        urgency: Urgency level (critical, high, medium, low).
        reason: One-sentence explanation.
        raw_output: Full model output for debugging.
    """

    signal_id: int | str
    category: str
    urgency: str
    reason: str
    raw_output: str


def classify_signal_text(signal_text: str) -> ClassificationResult | None:
    """Classify a signal using Gemma 270M.

    Parameters:
        signal_text: Description of the signal to classify.

    Returns:
        ClassificationResult or None if the model is unavailable.
    """
    try:
        from gemma.micro import get_micro_pool
        pool = get_micro_pool()
    except Exception as exc:
        log.debug("Gemma micro pool not available: {e}", e=str(exc))
        return None

    result = pool.classify_signal(signal_text)
    if result is None:
        return None

    return _parse_classification(result, signal_id="manual")


def classify_recent_signals(
    engine: Engine,
    limit: int = 50,
) -> dict[str, Any]:
    """Classify recently inserted signals that lack category labels.

    Fetches unclassified signals from signal_registry, runs them
    through Gemma 270M, and updates the records.

    Parameters:
        engine: SQLAlchemy database engine.
        limit: Maximum signals to classify per batch.

    Returns:
        dict: Summary of classification results.
    """
    try:
        from gemma.micro import get_micro_pool
        pool = get_micro_pool()
    except Exception as exc:
        return {"error": str(exc), "classified": 0}

    # Check if signal_registry has the classification columns
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "SELECT signal_category FROM signal_registry LIMIT 0"
            ))
    except Exception:
        # Add classification columns if missing
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    ALTER TABLE signal_registry
                    ADD COLUMN IF NOT EXISTS signal_category TEXT,
                    ADD COLUMN IF NOT EXISTS signal_urgency TEXT,
                    ADD COLUMN IF NOT EXISTS classification_reason TEXT
                """))
        except Exception as exc:
            log.debug("Cannot add classification columns: {e}", e=str(exc))
            return {"error": "signal_registry not ready", "classified": 0}

    # Fetch unclassified signals
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, source_module, signal_type, ticker,
                   value, confidence, direction
            FROM signal_registry
            WHERE signal_category IS NULL
            ORDER BY valid_from DESC
            LIMIT :lim
        """).bindparams(lim=limit)).fetchall()

    if not rows:
        return {"classified": 0, "message": "no unclassified signals"}

    classified = 0
    for row in rows:
        sig_id, src, stype, ticker, val, conf, direction = row

        # Build signal description for the classifier
        signal_text = (
            f"Signal from {src}: type={stype}, ticker={ticker}, "
            f"value={val}, confidence={conf}, direction={direction}"
        )

        result = pool.classify_signal(signal_text)
        if result is None:
            continue

        parsed = _parse_classification(result, signal_id=sig_id)
        if parsed is None:
            continue

        # Update the signal record
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE signal_registry
                    SET signal_category = :cat,
                        signal_urgency = :urg,
                        classification_reason = :reason
                    WHERE id = :sid
                """).bindparams(
                    cat=parsed.category,
                    urg=parsed.urgency,
                    reason=parsed.reason,
                    sid=sig_id,
                ))
                classified += 1
        except Exception as exc:
            log.debug("Failed to update signal {id}: {e}", id=sig_id, e=str(exc))

    log.info("Signal classification: {n}/{t} classified", n=classified, t=len(rows))

    return {
        "classified": classified,
        "total_unclassified": len(rows),
    }


def narrate_anomalies(
    engine: Engine,
    z_threshold: float = 3.0,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Find high-z-score signals and generate anomaly narratives.

    Uses Gemma 270M anomaly_narrator to produce one-line summaries
    of notable anomalies in the signal stream.

    Parameters:
        engine: SQLAlchemy database engine.
        z_threshold: Minimum absolute z-score to consider anomalous.
        limit: Maximum anomalies to narrate.

    Returns:
        list[dict]: Anomaly narratives with metadata.
    """
    try:
        from gemma.micro import get_micro_pool
        pool = get_micro_pool()
    except Exception:
        return []

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT source_module, signal_type, ticker,
                   value, z_score, confidence, direction, valid_from
            FROM signal_registry
            WHERE ABS(z_score) >= :threshold
            AND valid_from >= NOW() - INTERVAL '24 hours'
            ORDER BY ABS(z_score) DESC
            LIMIT :lim
        """).bindparams(threshold=z_threshold, lim=limit)).fetchall()

    narratives: list[dict[str, Any]] = []
    for row in rows:
        src, stype, ticker, val, z, conf, direction, vfrom = row

        anomaly_data = (
            f"Source: {src}, Type: {stype}, Ticker: {ticker}, "
            f"Value: {val}, Z-score: {z:.2f}, "
            f"Confidence: {conf}, Direction: {direction}"
        )

        narrative = pool.narrate_anomaly(anomaly_data)
        if narrative:
            narratives.append({
                "ticker": ticker,
                "source": src,
                "z_score": float(z),
                "narrative": narrative.strip(),
                "timestamp": vfrom.isoformat() if vfrom else None,
            })

    return narratives


def _parse_classification(
    raw_output: str,
    signal_id: int | str,
) -> ClassificationResult | None:
    """Parse Gemma 270M classification output.

    Expected format:
        CATEGORY: <category>
        URGENCY: <urgency>
        REASON: <one sentence>

    Parameters:
        raw_output: Raw model output text.
        signal_id: Signal identifier.

    Returns:
        ClassificationResult or None if parsing fails.
    """
    category = "unknown"
    urgency = "medium"
    reason = ""

    for line in raw_output.strip().split("\n"):
        line = line.strip()
        if line.upper().startswith("CATEGORY:"):
            category = line.split(":", 1)[1].strip().lower()
        elif line.upper().startswith("URGENCY:"):
            urgency = line.split(":", 1)[1].strip().lower()
        elif line.upper().startswith("REASON:"):
            reason = line.split(":", 1)[1].strip()

    valid_categories = {
        "rates", "credit", "equity", "volatility", "flows", "macro",
        "geopolitical", "insider", "options", "crypto", "commodities", "fx",
    }
    if category not in valid_categories:
        category = "unknown"

    valid_urgencies = {"critical", "high", "medium", "low"}
    if urgency not in valid_urgencies:
        urgency = "medium"

    return ClassificationResult(
        signal_id=signal_id,
        category=category,
        urgency=urgency,
        reason=reason,
        raw_output=raw_output,
    )
