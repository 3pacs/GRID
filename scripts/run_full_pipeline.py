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


# Source-specific diagnostic/fix commands for failure alerts
_FIX_COMMANDS: dict[str, dict[str, str]] = {
    "FRED": {
        "diagnose": 'curl -s "https://api.stlouisfed.org/fred/series?api_key=$FRED_API_KEY&series_id=DFF&file_type=json" | head -5',
        "fix": "Check FRED_API_KEY in .env — get one at https://fred.stlouisfed.org/docs/api/api_key.html",
        "retry": 'cd /data/grid_v4/grid_repo/grid && python -c "from ingestion.fred import FREDPuller; from config import settings; from db import get_engine; FREDPuller(settings.FRED_API_KEY, get_engine()).pull_all()"',
        "file": "grid/ingestion/fred.py",
    },
    "yfinance": {
        "diagnose": "python -c \"import yfinance; print(yfinance.Ticker('SPY').info.get('regularMarketPrice'))\"",
        "fix": "pip install --upgrade yfinance",
        "retry": 'cd /data/grid_v4/grid_repo/grid && python -c "from ingestion.yfinance_pull import YFinancePuller; from db import get_engine; YFinancePuller(get_engine()).pull_all()"',
        "file": "grid/ingestion/yfinance_pull.py",
    },
    "EDGAR": {
        "diagnose": 'curl -s "https://efts.sec.gov/LATEST/search-index?q=form-type%3D%224%22&dateRange=custom&startdt=$(date -d yesterday +%Y-%m-%d)&enddt=$(date +%Y-%m-%d)" | head -5',
        "retry": 'cd /data/grid_v4/grid_repo/grid && python -c "from ingestion.edgar import EDGARPuller; from db import get_engine; EDGARPuller(get_engine()).pull_form4_transactions()"',
        "file": "grid/ingestion/edgar.py",
    },
    "BLS": {
        "diagnose": 'curl -s "https://api.bls.gov/publicAPI/v2/timeseries/data/LNS14000000" | head -5',
        "retry": 'cd /data/grid_v4/grid_repo/grid && python -c "from ingestion.bls import BLSPuller; from db import get_engine; BLSPuller(get_engine()).pull_series()"',
        "file": "grid/ingestion/bls.py",
    },
    "Options": {
        "diagnose": "python -c \"import yfinance; print(yfinance.Ticker('SPY').options)\"",
        "retry": 'cd /data/grid_v4/grid_repo/grid && python -c "from ingestion.options import OptionsPuller; from db import get_engine; OptionsPuller(get_engine()).pull_all()"',
        "file": "grid/ingestion/options.py",
    },
    "Crucix": {
        "diagnose": "curl -s http://localhost:3117/api/health | head -5",
        "fix": "Check Crucix service: sudo systemctl status grid-crucix",
        "retry": 'cd /data/grid_v4/grid_repo/grid && python -c "from ingestion.crucix_bridge import CrucixBridgePuller; from db import get_engine; CrucixBridgePuller(get_engine()).pull_all()"',
        "file": "grid/ingestion/crucix_bridge.py",
    },
    "PostgreSQL": {
        "diagnose": "pg_isready -h localhost -p 5432 -U grid",
        "fix": "sudo systemctl restart postgresql",
        "retry": "cd /data/grid_v4/grid_repo/grid && python scripts/run_full_pipeline.py",
        "file": "grid/db.py",
    },
}


def _get_fix_commands(label: str) -> dict[str, str]:
    """Look up fix commands for a source label, falling back to defaults."""
    # Try exact match, then prefix match
    if label in _FIX_COMMANDS:
        return _FIX_COMMANDS[label]
    for key, cmds in _FIX_COMMANDS.items():
        if key.lower() in label.lower():
            return cmds
    return {
        "diagnose": 'cd /data/grid_v4/grid_repo/grid && python -c "from db import get_engine; print(get_engine().connect().execute(text(\'SELECT 1\')).scalar())"',
        "retry": "cd /data/grid_v4/grid_repo/grid && python scripts/run_full_pipeline.py",
        "file": "grid/scripts/run_full_pipeline.py",
    }


def _safe_run(label: str, fn, *args, **kwargs) -> Any:
    """Run a function, log errors, send alert with fix commands on failure."""
    try:
        log.info("=== {l} ===", l=label)
        result = fn(*args, **kwargs)
        log.info("{l} — OK", l=label)
        return result
    except Exception as exc:
        log.error("{l} — FAILED: {e}", l=label, e=str(exc))
        try:
            from alerts.email import alert_on_failure_with_fix
            alert_on_failure_with_fix(label, str(exc), _get_fix_commands(label))
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
    # Uses unified scheduler.py which contains all puller registries.
    # -----------------------------------------------------------------------
    def _international_ingest():
        from ingestion.scheduler import run_pull_group
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
    # STEP 4b: Post-resolution audit
    # -----------------------------------------------------------------------
    def _resolution_audit():
        from intelligence.resolution_audit import audit_after_resolve
        result = audit_after_resolve(engine)
        log.info("Resolution audit — {r}", r=result.get("summary", {}))
        return result.get("summary", {})
    summary["steps"]["resolution_audit"] = _safe_run("Resolution Audit", _resolution_audit)

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
    # STEP 7b: Smart discovery insights (chains ortho → clustering → alerts)
    # -----------------------------------------------------------------------
    def _smart_discovery():
        from store.pit import PITStore
        from discovery.orthogonality import OrthogonalityAudit
        from discovery.clustering import ClusterDiscovery

        pit = PITStore(engine)
        ortho = OrthogonalityAudit(db_engine=engine, pit_store=pit)

        # 1. Get orthogonal feature set
        ortho_result = ortho.get_orthogonal_features(corr_threshold=0.8)
        log.info(
            "Orthogonal features: {n}/{t} (removed {r} redundant pairs)",
            n=len(ortho_result["orthogonal_ids"]),
            t=ortho_result["total_features"],
            r=len(ortho_result["redundant_pairs"]),
        )

        # 2. Get transition leaders from latest clustering
        cluster = ClusterDiscovery(db_engine=engine, pit_store=pit)
        leaders = cluster.get_transition_leaders(top_n=5)

        # 3. Alert if noteworthy
        alerts_sent = []

        if leaders:
            try:
                from alerts.email import alert_on_transition_leaders
                alert_on_transition_leaders(leaders, {"best_k": "?"})
                alerts_sent.append("transition_leaders")
                log.info("Transition leaders: {l}", l=[l["feature"] for l in leaders])
            except Exception as exc:
                log.debug("Transition leader alert skipped: {e}", e=str(exc))

        # Dimensionality shift detection
        try:
            from sqlalchemy import text as sa_text
            with engine.connect() as conn:
                prev = conn.execute(sa_text(
                    "SELECT (result_data::jsonb)->>'true_dimensionality' "
                    "FROM analytical_snapshots "
                    "WHERE snapshot_type = 'orthogonality' "
                    "ORDER BY created_at DESC OFFSET 1 LIMIT 1"
                )).fetchone()
            if prev and prev[0]:
                prev_dims = int(prev[0])
                curr_dims = ortho_result["true_dimensionality"]
                if abs(curr_dims - prev_dims) >= 2:
                    from alerts.email import alert_on_discovery_insight
                    direction = "compressing" if curr_dims < prev_dims else "expanding"
                    alert_on_discovery_insight(
                        "Dimensionality Shift",
                        f"True dimensionality changed from {prev_dims} to {curr_dims}. "
                        f"Market structure is {direction}.",
                        ortho_result,
                    )
                    alerts_sent.append("dimensionality_shift")
        except Exception:
            pass

        # Redundancy warning
        n_total = ortho_result["total_features"]
        n_redundant = len(ortho_result["redundant_pairs"])
        if n_total > 0 and n_redundant / n_total > 0.3:
            try:
                from alerts.email import alert_on_discovery_insight
                alert_on_discovery_insight(
                    "Feature Redundancy Warning",
                    f"{n_redundant} redundant feature pairs detected out of {n_total}. "
                    f"Consider pruning to {len(ortho_result['orthogonal_ids'])} orthogonal features.",
                    ortho_result,
                )
                alerts_sent.append("redundancy_warning")
            except Exception:
                pass

        return {
            "orthogonal_features": len(ortho_result["orthogonal_ids"]),
            "by_family": {k: len(v) for k, v in ortho_result["by_family"].items()},
            "transition_leaders": [l["feature"] for l in leaders] if leaders else [],
            "alerts_sent": alerts_sent,
        }
    summary["steps"]["smart_discovery"] = _safe_run("Smart Discovery Insights", _smart_discovery)

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
    # STEP 12: File rotation / cleanup (insights, briefings, error archives)
    # -----------------------------------------------------------------------
    def _cleanup():
        cleaned = {}
        try:
            from outputs.llm_logger import cleanup_old_insights
            cleaned["insights"] = cleanup_old_insights(max_age_days=90)
        except Exception as exc:
            log.debug("Insight cleanup skipped: {e}", e=str(exc))
        try:
            from ollama.market_briefing import MarketBriefingGenerator
            cleaned["briefings"] = MarketBriefingGenerator.cleanup_old_briefings(max_age_days=90)
        except Exception as exc:
            log.debug("Briefing cleanup skipped: {e}", e=str(exc))
        return cleaned
    summary["steps"]["cleanup"] = _safe_run("File Rotation Cleanup", _cleanup)

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
