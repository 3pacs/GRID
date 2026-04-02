"""
AutoBNN-powered changepoint detection for GRID's discovery pipeline.

Integrates AutoBNN's structural decomposition with the existing
ClusterDiscovery regime detection. Detects changepoints across
all model-eligible features and publishes regime change signals.

This runs alongside the existing GMM/KMeans clustering — it adds
interpretable changepoint detection rather than replacing anything.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from timeseries.autobnn import AutoBNNDecomposer, RegimeChangeSignal


@dataclass
class ChangeReport:
    """Summary of changepoint detection across multiple features.

    Attributes:
        timestamp: When the detection was run.
        features_scanned: Number of features analysed.
        changepoints_found: Total changepoints detected.
        regime_changes: Significant regime change signals.
        elapsed_seconds: Time taken for the analysis.
    """

    timestamp: datetime
    features_scanned: int
    changepoints_found: int
    regime_changes: list[RegimeChangeSignal]
    elapsed_seconds: float


def scan_for_changepoints(
    engine: Engine,
    min_confidence: float = 0.5,
    lookback_days: int = 365,
    max_features: int = 50,
) -> ChangeReport:
    """Scan model-eligible features for structural changepoints.

    Uses AutoBNN's changepoint detection (with moving-average fallback
    if JAX is not installed) to find regime changes across GRID's
    feature universe.

    Parameters:
        engine: SQLAlchemy database engine.
        min_confidence: Minimum confidence threshold for changepoints.
        lookback_days: How far back to look for each feature.
        max_features: Maximum features to scan per cycle.

    Returns:
        ChangeReport with detected regime changes.
    """
    import time as _time

    start = _time.monotonic()
    decomposer = AutoBNNDecomposer(num_samples=100, num_chains=1)

    # Get model-eligible features with recent data
    with engine.connect() as conn:
        features = conn.execute(text("""
            SELECT fr.id, fr.name, fr.family
            FROM feature_registry fr
            WHERE fr.model_eligible = TRUE
            ORDER BY fr.family, fr.name
            LIMIT :lim
        """).bindparams(lim=max_features)).fetchall()

    if not features:
        log.info("Changepoint scan: no eligible features found")
        return ChangeReport(
            timestamp=datetime.now(timezone.utc),
            features_scanned=0,
            changepoints_found=0,
            regime_changes=[],
            elapsed_seconds=0.0,
        )

    all_changes: list[RegimeChangeSignal] = []
    scanned = 0

    for feat_id, feat_name, feat_family in features:
        # Fetch historical values
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT obs_date, value
                FROM resolved_series
                WHERE feature_id = :fid
                AND obs_date >= CURRENT_DATE - :days
                ORDER BY obs_date ASC
            """).bindparams(fid=feat_id, days=lookback_days)).fetchall()

        if len(rows) < 30:
            continue

        scanned += 1
        values = np.array([float(r[1]) for r in rows], dtype=np.float64)
        dates = [r[0] for r in rows]

        changes = decomposer.detect_regime_changes(
            series=values,
            dates=dates,
            series_id=feat_name,
            min_confidence=min_confidence,
        )

        all_changes.extend(changes)

    elapsed = _time.monotonic() - start

    log.info(
        "Changepoint scan complete — {n} features scanned, {c} changes found in {t:.1f}s",
        n=scanned,
        c=len(all_changes),
        t=elapsed,
    )

    return ChangeReport(
        timestamp=datetime.now(timezone.utc),
        features_scanned=scanned,
        changepoints_found=len(all_changes),
        regime_changes=all_changes,
        elapsed_seconds=elapsed,
    )


def publish_regime_signals(
    engine: Engine,
    report: ChangeReport,
) -> int:
    """Publish detected changepoints as signals to the signal_registry.

    Parameters:
        engine: SQLAlchemy database engine.
        report: ChangeReport from scan_for_changepoints.

    Returns:
        int: Number of signals published.
    """
    if not report.regime_changes:
        return 0

    published = 0
    now = datetime.now(timezone.utc)

    with engine.begin() as conn:
        # Ensure signal_registry table exists
        try:
            conn.execute(text("SELECT 1 FROM signal_registry LIMIT 0"))
        except Exception:
            log.debug("signal_registry table not found — skipping publish")
            return 0

        for change in report.regime_changes:
            try:
                conn.execute(text("""
                    INSERT INTO signal_registry
                        (source_module, signal_type, ticker, value,
                         z_score, confidence, direction,
                         valid_from, metadata)
                    VALUES
                        (:src, :stype, :ticker, :val,
                         :zscore, :conf, :direction,
                         :vfrom, :meta)
                    ON CONFLICT DO NOTHING
                """).bindparams(
                    src="discovery.changepoint_detector",
                    stype="regime_change",
                    ticker=change.series_id,
                    val=change.magnitude,
                    zscore=change.confidence * 3.0,  # Scale to z-score-like
                    conf=change.confidence,
                    direction=(
                        "bullish" if change.post_regime == "rising"
                        else "bearish" if change.post_regime == "falling"
                        else "neutral"
                    ),
                    vfrom=now,
                    meta=f'{{"pre_regime": "{change.pre_regime}", '
                         f'"post_regime": "{change.post_regime}", '
                         f'"change_index": {change.change_index}}}',
                ))
                published += 1
            except Exception as exc:
                log.debug("Failed to publish changepoint signal: {e}", e=str(exc))

    if published:
        log.info("Published {n} regime change signals", n=published)

    return published


def run_changepoint_cycle(
    engine: Engine,
    min_confidence: float = 0.5,
) -> dict[str, Any]:
    """Full changepoint detection cycle: scan + publish.

    Called by the Hermes operator. Scans features for structural
    breaks and publishes results as signals.

    Parameters:
        engine: SQLAlchemy database engine.
        min_confidence: Minimum confidence threshold.

    Returns:
        dict: Cycle summary.
    """
    report = scan_for_changepoints(engine, min_confidence=min_confidence)
    published = publish_regime_signals(engine, report)

    return {
        "features_scanned": report.features_scanned,
        "changepoints_found": report.changepoints_found,
        "signals_published": published,
        "elapsed_seconds": round(report.elapsed_seconds, 2),
        "top_changes": [
            {
                "series": c.series_id,
                "pre": c.pre_regime,
                "post": c.post_regime,
                "confidence": round(c.confidence, 3),
            }
            for c in sorted(report.regime_changes, key=lambda c: c.confidence, reverse=True)[:5]
        ],
    }
