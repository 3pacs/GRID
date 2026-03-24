#!/usr/bin/env python3
"""
GRID — Full pipeline runner.

Runs the complete ingestion → resolution → features → discovery → signals
pipeline in one shot. Designed to be run via cron or manually.

Usage:
    python3 scripts/run_full_pipeline.py              # daily incremental
    python3 scripts/run_full_pipeline.py --historical  # full backfill
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

# Ensure grid/ is on sys.path regardless of working directory
_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log


def _safe_run(label: str, fn, *args, **kwargs) -> Any:
    """Run a function, log errors, send alert on failure."""
    try:
        log.info("=== {l} ===", l=label)
        result = fn(*args, **kwargs)
        log.info("{l} — OK", l=label)
        return result
    except Exception as exc:
        log.error("{l} — FAILED: {e}", l=label, e=str(exc))
        try:
            from alerts.email import alert_on_failure
            alert_on_failure(label, str(exc))
        except Exception:
            pass
        return None


def run_pipeline(historical: bool = False) -> dict:
    """Execute the full GRID pipeline.

    Steps:
    1. Ingest from all sources (domestic + international + alt + celestial)
    2. Resolve multi-source conflicts into resolved_series
    3. Compute derived features (z-scores, slopes, ratios)
    4. Run orthogonality audit
    5. Run clustering / regime detection
    6. Run options mispricing scanner
    7. Compute feature importance
    8. Send daily digest email

    Parameters:
        historical: If True, pull from 1990-01-01. Otherwise incremental.

    Returns:
        Summary dict with step results.
    """
    from config import settings
    from db import get_engine

    engine = get_engine()
    # Historical flag sets the floor — pullers auto-skip existing data
    # and use incremental start dates when they have prior data
    start_date = "1990-01-01" if historical else date.today().isoformat()
    summary: dict[str, Any] = {"started": date.today().isoformat(), "steps": {}}
    log.info("Pipeline start_date={sd}, historical={h}", sd=start_date, h=historical)

    # -----------------------------------------------------------------------
    # STEP 1: Domestic ingestion (FRED, yfinance, EDGAR, options, celestial, altdata)
    # -----------------------------------------------------------------------
    def _domestic_ingest():
        from ingestion.scheduler import run_daily_pulls
        run_daily_pulls(start_date=start_date)
    summary["steps"]["domestic_ingest"] = _safe_run("Domestic Ingestion", _domestic_ingest)

    # -----------------------------------------------------------------------
    # STEP 2: International + trade + physical ingestion (all groups)
    # NOTE: scheduler_v2 is deprecated (#39) — use scheduler.py patterns.
    # Keeping v2 call because it has the international puller registry;
    # scheduler.py only covers domestic.  TODO: unify into scheduler.py.
    # -----------------------------------------------------------------------
    def _international_ingest():
        from ingestion.scheduler_v2 import run_pull_group
        for group in ["daily", "weekly", "monthly"]:
            try:
                run_pull_group(group, engine)
            except Exception as exc:
                log.warning("Pull group {g} failed: {e}", g=group, e=str(exc))
    summary["steps"]["international_ingest"] = _safe_run("International Ingestion", _international_ingest)

    # -----------------------------------------------------------------------
    # STEP 3: Crypto + DeFi ingestion
    # -----------------------------------------------------------------------
    def _crypto_ingest():
        try:
            from ingestion.crypto_bootstrap import CryptoBootstrapPuller
            puller = CryptoBootstrapPuller(db_engine=engine)
            result = puller.pull_all()
            log.info("Crypto bootstrap — {r}", r=result)
        except Exception as exc:
            log.warning("Crypto bootstrap failed: {e}", e=str(exc))
        try:
            from ingestion.dexscreener import DexScreenerPuller
            puller = DexScreenerPuller(db_engine=engine)
            result = puller.pull_all()
            log.info("DexScreener — {r}", r=result)
        except Exception as exc:
            log.warning("DexScreener failed: {e}", e=str(exc))
        try:
            from ingestion.pumpfun import PumpFunPuller
            puller = PumpFunPuller(db_engine=engine)
            result = puller.pull_all()
            log.info("PumpFun — {r}", r=result)
        except Exception as exc:
            log.warning("PumpFun failed: {e}", e=str(exc))
    summary["steps"]["crypto_ingest"] = _safe_run("Crypto Ingestion", _crypto_ingest)

    # -----------------------------------------------------------------------
    # STEP 3b: Crucix bridge (25+ intelligence sources from Crucix app)
    # -----------------------------------------------------------------------
    def _crucix_ingest():
        from ingestion.crucix_bridge import CrucixBridgePuller
        puller = CrucixBridgePuller(db_engine=engine)
        result = puller.pull_all()
        log.info("Crucix bridge — {r}", r=result)
        return result
    summary["steps"]["crucix_ingest"] = _safe_run("Crucix Bridge Ingestion", _crucix_ingest)

    # -----------------------------------------------------------------------
    # STEP 4: Multi-source conflict resolution
    # -----------------------------------------------------------------------
    def _resolve():
        from normalization.resolver import Resolver
        resolver = Resolver(db_engine=engine)
        result = resolver.resolve_pending()
        log.info("Resolution complete — {r}", r=result)
        return result
    summary["steps"]["resolution"] = _safe_run("Conflict Resolution", _resolve)

    # -----------------------------------------------------------------------
    # STEP 5: Feature engineering (derived features from resolved series)
    # -----------------------------------------------------------------------
    def _compute_features():
        from store.pit import PITStore
        from features.lab import FeatureLab

        pit = PITStore(engine)
        lab = FeatureLab(db_engine=engine, pit_store=pit)
        result = lab.compute_derived_features(as_of_date=date.today())
        log.info("Feature computation — {r}", r=result)
        return result
    summary["steps"]["features"] = _safe_run("Feature Engineering", _compute_features)

    # -----------------------------------------------------------------------
    # STEP 6: Orthogonality audit
    # -----------------------------------------------------------------------
    def _orthogonality():
        from store.pit import PITStore
        from discovery.orthogonality import OrthogonalityAudit

        pit = PITStore(engine)
        audit = OrthogonalityAudit(db_engine=engine, pit_store=pit)
        result = audit.run_full_audit()
        log.info("Orthogonality audit — {r}", r=result)
        return result
    summary["steps"]["orthogonality"] = _safe_run("Orthogonality Audit", _orthogonality)

    # -----------------------------------------------------------------------
    # STEP 7: Regime clustering + detection
    # -----------------------------------------------------------------------
    def _regime():
        from scripts.auto_regime import run
        run()
    summary["steps"]["regime"] = _safe_run("Regime Detection", _regime)

    # -----------------------------------------------------------------------
    # STEP 8: Options mispricing scan
    # -----------------------------------------------------------------------
    def _options_scan():
        from discovery.options_scanner import OptionsScanner
        scanner = OptionsScanner(engine)
        opps = scanner.scan_all(min_score=5.0)
        n_100x = sum(1 for o in opps if o.is_100x)
        if opps:
            scanner.persist_scan(opps)
        log.info("Options scan — {n} opportunities, {x} 100x+", n=len(opps), x=n_100x)

        # Email alert for any 100x+ finds
        for opp in opps:
            if opp.is_100x:
                try:
                    from alerts.email import alert_on_100x_opportunity
                    alert_on_100x_opportunity(opp.ticker, opp.score, opp.direction, opp.thesis)
                except Exception:
                    pass
        return {"opportunities": len(opps), "100x": n_100x}
    summary["steps"]["options_scan"] = _safe_run("Options Mispricing Scan", _options_scan)

    # -----------------------------------------------------------------------
    # STEP 9: Feature importance tracking
    # -----------------------------------------------------------------------
    def _importance():
        from store.pit import PITStore
        from features.importance import FeatureImportanceTracker
        from sqlalchemy import text as sa_text

        pit = PITStore(engine)
        tracker = FeatureImportanceTracker(db_engine=engine, pit_store=pit)

        # Find production model to compute importance for
        with engine.connect() as conn:
            row = conn.execute(
                sa_text(
                    "SELECT id FROM model_registry "
                    "WHERE state = 'PRODUCTION' LIMIT 1"
                )
            ).fetchone()
        if row is None:
            log.warning("No production model — skipping importance")
            return {"skipped": "no_production_model"}
        model_id = row[0]
        result = tracker.get_importance_report(model_id, as_of_date=date.today())
        log.info("Feature importance — {r}", r=type(result).__name__)
        return result
    summary["steps"]["importance"] = _safe_run("Feature Importance", _importance)

    # -----------------------------------------------------------------------
    # STEP 10: Persist analytical snapshots to database
    # -----------------------------------------------------------------------
    def _snapshots():
        from store.snapshots import AnalyticalSnapshotStore
        snap = AnalyticalSnapshotStore(db_engine=engine)
        saved = snap.save_pipeline_snapshots(summary["steps"])
        log.info("Analytical snapshots persisted — {n} saved", n=saved)
        return {"snapshots_saved": saved}
    summary["steps"]["snapshots"] = _safe_run("Analytical Snapshots", _snapshots)

    # -----------------------------------------------------------------------
    # STEP 11: Daily digest email
    # -----------------------------------------------------------------------
    def _digest():
        from alerts.email import daily_digest
        daily_digest()
    summary["steps"]["digest"] = _safe_run("Daily Digest Email", _digest)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    log.info("=" * 60)
    log.info("GRID full pipeline complete")
    for step, result in summary["steps"].items():
        status = "OK" if result is not None else "FAILED"
        log.info("  {s}: {st}", s=step, st=status)
    log.info("=" * 60)

    return summary


if __name__ == "__main__":
    historical = "--historical" in sys.argv
    if historical:
        log.info("Running FULL HISTORICAL pipeline")
    else:
        log.info("Running DAILY INCREMENTAL pipeline")
    run_pipeline(historical=historical)
