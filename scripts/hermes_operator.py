#!/usr/bin/env python3
"""
GRID Hermes Operator — autonomous 24/7 self-healing daemon.

Hermes (the local llama.cpp model) runs continuously, performing:

1. HEALTH MONITOR — checks DB, data freshness, LLM availability every cycle
2. PULL FIXER — detects failed ingestion pulls, diagnoses why, retries with fixes
3. PIPELINE RUNNER — runs the full pipeline on schedule (or when data arrives)
4. DATA GATHERER — fills historical gaps, pulls missing series
5. AUTORESEARCH — generates and tests hypotheses when system is healthy
6. SELF-DIAGNOSTICS — reads its own error logs, proposes and applies fixes

Each cycle:
  - Check system health
  - Fix anything broken
  - Run any due scheduled work
  - If healthy, gather data or research
  - Log everything to analytical_snapshots + server_log

Usage:
    python scripts/hermes_operator.py                # run forever
    python scripts/hermes_operator.py --once          # single cycle
    python scripts/hermes_operator.py --dry-run       # diagnose only, don't fix
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Ensure grid/ is on sys.path
_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log


# ─── Configuration ───────────────────────────────────────────────────

CYCLE_INTERVAL_SECONDS = 300          # 5 minutes between cycles
CYCLE_TIMEOUT_SECONDS = 900           # 15 min max per cycle — abort if stuck
PIPELINE_INTERVAL_HOURS = 6           # run full pipeline every 6 hours
DATA_FRESHNESS_THRESHOLD_HOURS = 26   # flag stale sources after 26h
MAX_PULL_RETRIES = 3                  # retry failed pulls up to 3 times
AUTORESEARCH_MAX_ITER = 5             # hypothesis iterations per cycle
HERMES_TEMPERATURE = 0.3              # LLM temperature for diagnostics
GIT_SYNC_ENABLED = True               # pull/push on each cycle
GIT_REMOTE = "origin"
GIT_BRANCH = "main"

# Per-source cooldown: don't retry a source more often than this
SOURCE_COOLDOWN_MINUTES = 30          # min minutes between retries of same source
SOURCE_MAX_CONSECUTIVE_FAILS = 5      # after N consecutive fails, extend cooldown to 6h
TIMEOUT_BLACKLIST_HOURS = 24          # blacklist sources that cause cycle timeouts

# Source name → (module_path, class_name, needs_api_key, pull_method)
# This registry replaces the hardcoded if/elif chain and covers ALL pullers.
_SOURCE_REGISTRY: dict[str, dict[str, Any]] = {
    "fred":              {"mod": "ingestion.fred",                "cls": "FREDPuller",             "api_key": "FRED_API_KEY"},
    "yfinance":          {"mod": "ingestion.yfinance_pull",       "cls": "YFinancePuller"},
    "yfinance_options":  {"mod": "ingestion.options",             "cls": "OptionsPuller"},
    "edgar":             {"mod": "ingestion.edgar",               "cls": "EDGARPuller",            "pull_method": "pull_form4_transactions", "pull_kwargs": {"days_back": 3}},
    "crucix":            {"mod": "ingestion.crucix_bridge",       "cls": "CrucixBridgePuller"},
    "bls":               {"mod": "ingestion.bls",                 "cls": "BLSPuller",              "api_key": "BLS_API_KEY"},
    "googletrends":      {"mod": "ingestion.altdata.google_trends", "cls": "GoogleTrendsPuller",   "pull_kwargs": {"days_back": 30}},
    "cboe":              {"mod": "ingestion.altdata.cboe_indices", "cls": "CBOEIndicesPuller",     "pull_kwargs": {"days_back": 30}},
    "fedspeeches":       {"mod": "ingestion.altdata.fed_speeches", "cls": "FedSpeechPuller",      "pull_kwargs": {"days_back": 30}},
    "fear_greed":        {"mod": "ingestion.altdata.fear_greed",   "cls": "FearGreedPuller"},
    "baltic_exchange":   {"mod": "ingestion.altdata.baltic_dry",   "cls": "BalticDryPuller"},
    "ny_fed":            {"mod": "ingestion.altdata.nyfed",        "cls": "NYFedPuller"},
    "aaii_sentiment":    {"mod": "ingestion.altdata.aaii_sentiment", "cls": "AAIISentimentPuller"},
    "cftc_cot":          {"mod": "ingestion.altdata.cftc_cot",     "cls": "CFTCCOTPuller"},
    "finra_ats":         {"mod": "ingestion.altdata.finra_ats",    "cls": "FINRAATSPuller"},
    "kalshi":            {"mod": "ingestion.altdata.kalshi",       "cls": "KalshiPuller"},
    "ads_index":         {"mod": "ingestion.altdata.ads_index",    "cls": "ADSIndexPuller"},
    "noaa_swpc":         {"mod": "ingestion.celestial.solar",      "cls": "SolarActivityPuller"},
    "lunar_ephemeris":   {"mod": "ingestion.celestial.lunar",      "cls": "LunarCyclePuller"},
    "planetary_ephemeris": {"mod": "ingestion.celestial.planetary", "cls": "PlanetaryAspectPuller"},
    "vedic_jyotish":     {"mod": "ingestion.celestial.vedic",      "cls": "VedicAstroPuller"},
    "chinese_calendar":  {"mod": "ingestion.celestial.chinese",    "cls": "ChineseCalendarPuller"},

    # -- High-priority altdata pullers (previously dormant) --

    "congressional":          {"mod": "ingestion.altdata.congressional",          "cls": "CongressionalTradingPuller"},
    "insider_filings":        {"mod": "ingestion.altdata.insider_filings",        "cls": "InsiderFilingsPuller",
                               "pull_kwargs": {"days_back": 3}},
    "dark_pool":              {"mod": "ingestion.altdata.dark_pool",              "cls": "DarkPoolPuller"},
    "fed_liquidity":          {"mod": "ingestion.altdata.fed_liquidity",          "cls": "FedLiquidityPuller",
                               "api_key": "FRED_API_KEY"},
    "institutional_flows":    {"mod": "ingestion.altdata.institutional_flows",    "cls": "InstitutionalFlowsPuller"},
    "gov_contracts":          {"mod": "ingestion.altdata.gov_contracts",          "cls": "GovContractsPuller",
                               "pull_kwargs": {"days_back": 7}},
    "legislation":            {"mod": "ingestion.altdata.legislation",            "cls": "LegislationPuller",
                               "pull_kwargs": {"days_back": 7}},
    "gdelt":                  {"mod": "ingestion.altdata.gdelt",                  "cls": "GDELTPuller"},
    "alphavantage_sentiment": {"mod": "ingestion.altdata.alphavantage_sentiment", "cls": "AlphaVantageSentimentPuller"},
    "prediction_odds":        {"mod": "ingestion.altdata.prediction_odds",        "cls": "PredictionOddsPuller"},
    "unusual_whales":         {"mod": "ingestion.altdata.unusual_whales",         "cls": "UnusualWhalesPuller"},
    "smart_money":            {"mod": "ingestion.altdata.smart_money",            "cls": "SmartMoneyPuller"},
    "supply_chain":           {"mod": "ingestion.altdata.supply_chain",           "cls": "SupplyChainPuller",
                               "api_key": "FRED_API_KEY"},

    # -- Lower-priority altdata pullers (batch 2) --

    "earnings_calendar":  {"mod": "ingestion.altdata.earnings_calendar",  "cls": "EarningsCalendarPuller"},
    "lobbying":           {"mod": "ingestion.altdata.lobbying",           "cls": "LobbyingPuller"},
    "repo_market":        {"mod": "ingestion.altdata.repo_market",        "cls": "RepoMarketPuller",         "api_key": "FRED_API_KEY"},
    "yield_curve_full":   {"mod": "ingestion.altdata.yield_curve_full",   "cls": "FullYieldCurvePuller",     "api_key": "FRED_API_KEY"},
    "world_news":         {"mod": "ingestion.altdata.world_news",         "cls": "WorldNewsPuller"},
    "social_attention":   {"mod": "ingestion.altdata.social_attention",   "cls": "WikipediaAttentionPuller"},
    "hf_financial_news":  {"mod": "ingestion.altdata.hf_financial_news",  "cls": "HFFinancialNewsPuller"},
    "news_scraper":       {"mod": "ingestion.altdata.news_scraper",       "cls": "NewsScraperPuller"},
    "noaa_ais":           {"mod": "ingestion.altdata.noaa_ais",           "cls": "NOAAAISPuller"},
    "foia_cables":        {"mod": "ingestion.altdata.foia_cables",        "cls": "FOIACablesPuller"},
    "offshore_leaks":     {"mod": "ingestion.altdata.offshore_leaks",     "cls": "OffshoreLeaksPuller"},
    "export_controls":    {"mod": "ingestion.altdata.export_controls",    "cls": "ExportControlsPuller"},
    "fara":               {"mod": "ingestion.altdata.fara",               "cls": "FARAPuller"},

    # -- New upgraded data sources (2026-03-31) --

    "gdelt_news":         {"mod": "ingestion.altdata.gdelt_news",        "cls": "GdeltNewsPuller"},
    "nyfed_gscpi":        {"mod": "ingestion.altdata.nyfed_gscpi",       "cls": "NYFedGSCPIPuller"},
    "polymarket":         {"mod": "ingestion.altdata.polymarket",        "cls": "PolymarketPuller"},
    "kalshi_markets":     {"mod": "ingestion.altdata.kalshi_markets",    "cls": "KalshiMarketsPuller"},
    "stocktwits":         {"mod": "ingestion.altdata.stocktwits",        "cls": "StockTwitsPuller"},
    "pmxt_archive":       {"mod": "ingestion.altdata.pmxt_archive",      "cls": "PmxtArchivePuller"},
    "tiingo":             {"mod": "ingestion.tiingo_pull",               "cls": "TiingoPuller",           "api_key": "TIINGO_API_KEY"},
}


# ─── Git sync ────────────────────────────────────────────────────────

def _git(args: list[str], cwd: str | Path | None = None) -> tuple[int, str]:
    """Run a git command and return (returncode, output)."""
    if cwd is None:
        cwd = _GRID_DIR
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode, (result.stdout + result.stderr).strip()
    except Exception as exc:
        return 1, str(exc)


def git_pull() -> dict[str, Any]:
    """Pull latest changes from remote."""
    if not GIT_SYNC_ENABLED:
        return {"skipped": "disabled"}

    log.info("Git pull — syncing latest changes")
    rc, out = _git(["pull", "--rebase", GIT_REMOTE, GIT_BRANCH])
    if rc == 0:
        log.info("Git pull OK: {o}", o=out[:200])
        return {"status": "ok", "output": out[:200]}
    else:
        log.warning("Git pull failed: {o}", o=out[:300])
        # Try without rebase
        rc2, out2 = _git(["pull", GIT_REMOTE, GIT_BRANCH])
        if rc2 == 0:
            return {"status": "ok", "output": out2[:200], "fallback": True}
        return {"status": "failed", "output": out[:300]}


def git_push_outputs() -> dict[str, Any]:
    """Commit and push any new analytical outputs."""
    if not GIT_SYNC_ENABLED:
        return {"skipped": "disabled"}

    # Check for changes in outputs/ and .server-logs/
    rc, status = _git(["status", "--porcelain", "outputs/", ".server-logs/"])
    if rc != 0 or not status.strip():
        return {"status": "nothing_to_push"}

    changed_files = [line.strip().split(maxsplit=1)[-1] for line in status.strip().split("\n") if line.strip()]
    log.info("Git push — {n} changed output files", n=len(changed_files))

    # Stage output files only (never code)
    _git(["add", "outputs/", ".server-logs/"])

    # Commit
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    rc, out = _git(["commit", "-m", f"[hermes-operator] analytical outputs {ts}"])
    if rc != 0:
        log.warning("Git commit failed: {o}", o=out[:200])
        return {"status": "commit_failed", "output": out[:200]}

    # Push with retry
    for attempt in range(4):
        rc, out = _git(["push", GIT_REMOTE, GIT_BRANCH])
        if rc == 0:
            log.info("Git push OK")
            return {"status": "ok", "files": len(changed_files)}
        wait = 2 ** (attempt + 1)
        log.warning("Git push attempt {a} failed, retry in {w}s", a=attempt + 1, w=wait)
        time.sleep(wait)

    return {"status": "push_failed", "output": out[:200]}


# ─── Health, State, and Issue Tracking (extracted to hermes_health.py) ──
from scripts.hermes_health import (  # noqa: E402, F401
    _ensure_issues_table,
    log_issue,
    export_issues,
    SourceCooldown,
    OperatorState,
    check_db_health,
    check_hermes_health,
    check_system_health,
)

# ─── Pull Fixers, Pipeline, Diagnostics (extracted to hermes_fixers.py) ──
from scripts.hermes_fixers import (  # noqa: E402, F401
    _resolve_puller,
    _retry_source,
    diagnose_and_fix_pulls,
    maybe_run_pipeline,
    fill_data_gaps,
    run_self_diagnostics,
    maybe_run_autoresearch,
    save_cycle_snapshot,
    _run_intel_task,
    _hours_since,
    _refresh_signal_registry,
)


# ─── Intelligence task runner (remains in this file) ────────────────────

def run_intelligence_tasks(
    engine: Any,
    state: OperatorState,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run all intelligence module tasks on their respective schedules.

    Schedule:
        Every 4 hours:
            - trust_scorer.run_trust_cycle
            - options_recommender.generate_recommendations
            - cross_reference.run_all_checks (checks only, no LLM narrative)

        Every 6 hours (aligned with oracle cycle):
            - options_tracker.score_expired_recommendations
            - lever_pullers.identify_lever_pullers
            - actor_network.track_wealth_migration

        Daily at 2:00 AM:
            - source_audit.run_full_audit
            - backtest_scanner.run_full_scan (with LLM sanity check)
            - postmortem.batch_postmortem
            - options_tracker.run_improvement_cycle
            - backtest_scanner.review_existing_hypotheses

        Weekly (Sunday 3:00 AM):
            - cross_reference.run_all_checks (full, with LLM narrative)
            - lever_pullers.generate_lever_report
            - trust_scorer.generate_trust_report
            - actor_network.generate_actor_report
    """
    results: dict[str, Any] = {}
    now = datetime.now(timezone.utc)

    if dry_run:
        log.info("[DRY RUN] Would run intelligence tasks")
        return {"skipped": "dry_run"}

    # ── Every 4 hours ────────────────────────────────────────────────

    if _hours_since(state.last_trust_cycle) >= 4:
        try:
            from intelligence.trust_scorer import run_trust_cycle
            results["trust_cycle"] = _run_intel_task(
                "trust_cycle", run_trust_cycle, state, engine,
            )
        except Exception as exc:
            log.warning("Trust cycle import failed: {e}", e=str(exc))

        # TimesFM signal forecasts: run before thesis scorer so forecasts are fresh
        if _hours_since(state.last_signal_forecasts) >= 4:
            try:
                from inference.timesfm_service import forecast_signals
                fc_results = forecast_signals(engine, horizon=30)
                results["signal_forecasts"] = {
                    "forecasted": len(fc_results),
                    "directions": {
                        "UP": sum(1 for f in fc_results if f.direction == "UP"),
                        "DOWN": sum(1 for f in fc_results if f.direction == "DOWN"),
                        "FLAT": sum(1 for f in fc_results if f.direction == "FLAT"),
                    },
                }
                log.info("TimesFM forecasted {n} signals", n=len(fc_results))
            except Exception as exc:
                log.warning("TimesFM forecast cycle failed: {e}", e=str(exc))
            state.last_signal_forecasts = now

        # Thesis snapshot: score current thesis and persist for accuracy tracking
        try:
            from analysis.thesis_scorer import score_thesis, snapshot_thesis
            thesis = score_thesis(engine)
            snap_id = snapshot_thesis(engine, thesis)
            results["thesis_snapshot"] = {
                "direction": thesis["direction"],
                "score": thesis["score"],
                "conviction": thesis["conviction"],
                "snapshot_id": snap_id,
            }
            log.info("Thesis snapshot: {d} score={s} id={id}",
                     d=thesis["direction"], s=thesis["score"], id=snap_id)
        except Exception as exc:
            log.warning("Thesis snapshot failed: {e}", e=str(exc))

        state.last_trust_cycle = now

    if _hours_since(state.last_options_recommendations) >= 4:
        try:
            from trading.options_recommender import OptionsRecommender
            recommender = OptionsRecommender(db_engine=engine)
            results["options_recommendations"] = _run_intel_task(
                "options_recommendations",
                recommender.generate_recommendations,
                state,
                engine=engine,
            )
        except Exception as exc:
            log.warning("Options recommender import failed: {e}", e=str(exc))
        state.last_options_recommendations = now

    if _hours_since(state.last_cross_reference_checks) >= 4:
        try:
            from intelligence.cross_reference import run_all_checks
            results["cross_reference_checks"] = _run_intel_task(
                "cross_reference_checks",
                run_all_checks,
                state,
                engine,
                skip_narrative=True,
            )
        except Exception as exc:
            log.warning("Cross-reference import failed: {e}", e=str(exc))
        state.last_cross_reference_checks = now

    # ── Every 2 hours — signal registry refresh ──────────────────────

    if _hours_since(state.last_signal_registry) >= 2:
        _refresh_signal_registry(engine)
        state.last_signal_registry = now
        results["signal_registry"] = "refreshed"

    # ── Every 6 hours (alongside oracle) ─────────────────────────────

    if _hours_since(state.last_options_scoring) >= 6:
        try:
            from trading.options_tracker import score_expired_recommendations
            results["options_scoring"] = _run_intel_task(
                "options_scoring",
                score_expired_recommendations,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Options scoring import failed: {e}", e=str(exc))
        state.last_options_scoring = now

    if _hours_since(state.last_lever_pullers) >= 6:
        try:
            from intelligence.lever_pullers import identify_lever_pullers
            results["lever_pullers"] = _run_intel_task(
                "lever_pullers",
                identify_lever_pullers,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Lever pullers import failed: {e}", e=str(exc))
        state.last_lever_pullers = now

    if _hours_since(state.last_actor_wealth) >= 6:
        try:
            from intelligence.actor_network import track_wealth_migration
            results["actor_wealth_migration"] = _run_intel_task(
                "actor_wealth_migration",
                track_wealth_migration,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Actor network import failed: {e}", e=str(exc))
        state.last_actor_wealth = now

    # ── Daily at 2:00 AM ─────────────────────────────────────────────

    is_daily_window = (now.hour == 2 and now.minute < 10)
    daily_due = is_daily_window and _hours_since(state.last_daily_intel) >= 20

    if daily_due:
        log.info("Running daily intelligence batch (2:00 AM)")

        try:
            from intelligence.source_audit import run_full_audit
            results["source_audit"] = _run_intel_task(
                "source_audit", run_full_audit, state, engine,
            )
        except Exception as exc:
            log.warning("Source audit import failed: {e}", e=str(exc))

        try:
            from analysis.backtest_scanner import run_full_scan
            results["backtest_scan"] = _run_intel_task(
                "backtest_scan", run_full_scan, state, engine,
            )
        except Exception as exc:
            log.warning("Backtest scanner import failed: {e}", e=str(exc))

        try:
            from intelligence.postmortem import batch_postmortem
            results["postmortem_batch"] = _run_intel_task(
                "postmortem_batch", batch_postmortem, state, engine,
            )
        except Exception as exc:
            log.warning("Postmortem import failed: {e}", e=str(exc))

        try:
            from trading.options_tracker import run_improvement_cycle
            results["options_improvement"] = _run_intel_task(
                "options_improvement",
                run_improvement_cycle,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Options improvement import failed: {e}", e=str(exc))

        try:
            from analysis.backtest_scanner import review_existing_hypotheses
            results["hypothesis_review"] = _run_intel_task(
                "hypothesis_review",
                review_existing_hypotheses,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Hypothesis review import failed: {e}", e=str(exc))

        # Hypothesis discovery — auto-discover new hypotheses from data patterns
        if _hours_since(state.last_hypothesis_discovery) >= 20:
            try:
                from intelligence.hypothesis_engine import HypothesisGenerator
                hyp_engine = HypothesisGenerator(engine)
                discovered = hyp_engine.auto_discover()
                results["hypothesis_discovery"] = {
                    "new_hypotheses": len(discovered),
                }
                log.info(
                    "Hypothesis discovery: {n} new hypotheses generated",
                    n=len(discovered),
                )
            except Exception as exc:
                log.warning("Hypothesis discovery failed: {e}", e=str(exc))
            state.last_hypothesis_discovery = now

        # RAG index refresh — re-embed latest intelligence data
        if _hours_since(state.last_rag_index) >= 20:
            try:
                from intelligence.rag import RAGIndexer
                indexer = RAGIndexer(engine)
                indexer.ensure_tables()
                snap_count = indexer.index_snapshots()
                actor_count = indexer.index_actors()
                results["rag_index"] = {
                    "snapshots_indexed": snap_count,
                    "actors_indexed": actor_count,
                }
                log.info(
                    "RAG index refreshed: {s} snapshot chunks, {a} actor chunks",
                    s=snap_count, a=actor_count,
                )
            except Exception as exc:
                log.warning("RAG indexing failed: {e}", e=str(exc))
            state.last_rag_index = now

        state.last_daily_intel = now

    # ── Weekly (Sunday 3:00 AM) ──────────────────────────────────────

    is_sunday = now.weekday() == 6
    is_weekly_window = is_sunday and (now.hour == 3 and now.minute < 10)
    weekly_due = is_weekly_window and _hours_since(state.last_weekly_intel) >= 160

    if weekly_due:
        log.info("Running weekly intelligence reports (Sunday 3:00 AM)")

        try:
            from intelligence.cross_reference import run_all_checks
            results["weekly_cross_reference"] = _run_intel_task(
                "weekly_cross_reference",
                run_all_checks,
                state,
                engine,
                skip_narrative=False,
            )
        except Exception as exc:
            log.warning("Weekly cross-reference import failed: {e}", e=str(exc))

        try:
            from intelligence.lever_pullers import generate_lever_report
            results["weekly_lever_report"] = _run_intel_task(
                "weekly_lever_report",
                generate_lever_report,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Lever report import failed: {e}", e=str(exc))

        try:
            from intelligence.trust_scorer import generate_trust_report
            results["weekly_trust_report"] = _run_intel_task(
                "weekly_trust_report",
                generate_trust_report,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Trust report import failed: {e}", e=str(exc))

        try:
            from intelligence.actor_network import generate_actor_report
            results["weekly_actor_report"] = _run_intel_task(
                "weekly_actor_report",
                generate_actor_report,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Actor report import failed: {e}", e=str(exc))

        state.last_weekly_intel = now

    return results


# ─── Main loop ───────────────────────────────────────────────────────

def run_cycle(state: OperatorState, dry_run: bool = False) -> dict[str, Any]:
    """Execute one operator cycle."""
    state.cycle_count += 1
    cycle_start = time.monotonic()
    cycle_result: dict[str, Any] = {
        "cycle": state.cycle_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    log.info("═══ Hermes Operator — Cycle {n} ═══", n=state.cycle_count)

    # 0. Git pull — sync latest code/config
    try:
        pull_result = git_pull()
        cycle_result["git_pull"] = pull_result
    except Exception as exc:
        log.warning("Git pull failed: {e}", e=str(exc))

    # 1. Health check
    try:
        from db import get_engine
        engine = get_engine()
        health = check_system_health(engine)
        cycle_result["health"] = health
        hermes_ok = health["hermes"]["healthy"]
        db_ok = health["db"]["healthy"]
        log.info(
            "Health: DB={db}, Hermes={h}, stale={s}, failed_24h={f}",
            db=db_ok, h=hermes_ok,
            s=len(health["db"].get("stale_sources", [])),
            f=health["db"].get("failed_pulls_24h", 0),
        )
    except Exception as exc:
        log.error("Health check failed: {e}", e=str(exc))
        cycle_result["health"] = {"error": str(exc)}
        state.consecutive_failures += 1
        return cycle_result

    if not db_ok:
        log.error("Database unhealthy — skipping all work this cycle")
        state.consecutive_failures += 1
        # Can't log to DB if DB is down, but log the state
        return cycle_result

    # Ensure issues table exists (first cycle only)
    try:
        _ensure_issues_table(engine)
    except Exception as exc:
        log.debug("Hermes: issues table ensure failed: {e}", e=str(exc))

    state.consecutive_failures = 0

    # 2. Fix broken pulls (with cooldown + smart retry)
    try:
        state.current_step = "diagnose_and_fix_pulls"
        pull_result = diagnose_and_fix_pulls(engine, hermes_ok, state, dry_run=dry_run)
        cycle_result["pull_fixer"] = pull_result
        state.pulls_retried += pull_result.get("retried", 0)
        state.fixes_applied += pull_result.get("fixed", 0)
        state.errors_diagnosed += pull_result.get("diagnosed", 0)
    except Exception as exc:
        log.error("Pull fixer failed: {e}", e=str(exc))
        cycle_result["pull_fixer"] = {"error": str(exc)}

    # 2b. Proactively re-pull stale sources (not just failed ones)
    stale_sources = health["db"].get("stale_sources", [])
    if stale_sources and not dry_run:
        stale_repulled = 0
        for stale in stale_sources[:5]:  # limit to 5 per cycle
            src = stale["source"]
            state.current_step = f"stale_refresh:{src}"
            if state.cooldowns.can_retry(src):
                try:
                    _retry_source(src, engine, attempt=1)
                    state.cooldowns.record_attempt(src, success=True)
                    stale_repulled += 1
                    log.info("Proactively refreshed stale source: {s}", s=src)
                except ValueError:
                    pass  # no handler
                except Exception as exc:
                    state.cooldowns.record_attempt(src, success=False, error=str(exc))
                    log.warning("Stale refresh for {s} failed: {e}", s=src, e=str(exc))
        cycle_result["stale_refreshed"] = stale_repulled

    # 3. Smart ingestion — run only due/stale pullers (replaces full pipeline)
    try:
        state.current_step = "smart_ingestion"
        from ingestion.smart_scheduler import SmartScheduler
        if not hasattr(state, "_smart_sched") or state._smart_sched is None:
            state._smart_sched = SmartScheduler(engine)
        tick_result = state._smart_sched.tick()
        cycle_result["ingestion"] = tick_result
        log.info(
            "Smart ingestion: {ok}/{ran} succeeded, {due} still due",
            ok=tick_result["succeeded"], ran=tick_result["ran"],
            due=len(tick_result.get("still_due", [])),
        )
    except Exception as exc:
        log.error("Smart ingestion failed: {e}", e=str(exc))
        cycle_result["ingestion"] = {"error": str(exc)}

    # 3b. Fast SQL resolution (skip slow Python resolver — use INSERT SELECT)
    try:
        state.current_step = "resolution"
        with engine.begin() as conn:
            # Set statement timeout to avoid blocking the cycle
            conn.execute(text("SET LOCAL statement_timeout = '120s'"))
            # Fast bulk resolve: INSERT into resolved_series from raw_series
            # for any rows pulled in the last hour that don't have resolved entries
            result = conn.execute(text("""
                INSERT INTO resolved_series (feature_id, obs_date, value, source_id, resolved_at)
                SELECT em.feature_id, rs.obs_date, rs.value, rs.source_id, NOW()
                FROM raw_series rs
                JOIN entity_map em ON em.series_id = rs.series_id
                WHERE rs.pull_timestamp > NOW() - INTERVAL '1 hour'
                AND rs.pull_status = 'SUCCESS'
                AND NOT EXISTS (
                    SELECT 1 FROM resolved_series res
                    WHERE res.feature_id = em.feature_id
                    AND res.obs_date = rs.obs_date
                )
                ON CONFLICT (feature_id, obs_date) DO NOTHING
            """))
            res_count = result.rowcount
        cycle_result["resolution"] = {"rows_resolved": res_count}
        if res_count:
            log.info("Fast resolution: {n} new rows", n=res_count)
    except Exception as exc:
        log.debug("Resolution: {e}", e=str(exc))

    # 4. Fill data gaps — SKIP: SmartScheduler handles freshness now
    # The old gap filler re-pulled entire sources which was slow.
    # SmartScheduler's frequency tracking replaces this.
    cycle_result["data_gaps"] = {"skipped": "handled_by_smart_scheduler"}

    # 5. Self-diagnostics — only every 6th cycle (30 min)
    if state.cycle_count % 6 == 0:
        try:
            state.current_step = "diagnostics"
            diag = run_self_diagnostics(engine, hermes_ok, health, state, dry_run=dry_run)
            cycle_result["diagnostics"] = diag
        except Exception as exc:
            log.warning("Self-diagnostics failed: {e}", e=str(exc))

    # 6. Autoresearch — only every 12th cycle (1 hour)
    if state.cycle_count % 12 == 0 and health.get("overall_healthy") and hermes_ok:
        try:
            state.current_step = "autoresearch"
            ar_result = maybe_run_autoresearch(state, dry_run=dry_run)
            if ar_result is not None:
                cycle_result["autoresearch"] = ar_result
        except Exception as exc:
            log.warning("Autoresearch failed: {e}", e=str(exc))

    # 7. UX Audit — only every 72nd cycle (~6 hours)
    if state.cycle_count % 72 == 0 and health.get("overall_healthy") and hermes_ok:
        try:
            state.current_step = "ux_audit"
            from scripts.ux_auditor import maybe_run_ux_audit
            ux_result = maybe_run_ux_audit(state, engine, dry_run=dry_run)
            if ux_result is not None:
                cycle_result["ux_audit"] = ux_result
        except Exception as exc:
            log.warning("UX audit failed: {e}", e=str(exc))

    # 7b. Daily digest email (once per day)
    try:
        from scripts.daily_digest import maybe_send_daily_digest
        digest_result = maybe_send_daily_digest(state, engine, dry_run=dry_run)
        if digest_result is not None:
            cycle_result["daily_digest"] = digest_result
    except Exception as exc:
        log.warning("Daily digest failed: {e}", e=str(exc))

    # 7c. 100x Digest (every 4 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_100x = 999
        if state.last_100x_digest is not None:
            hours_since_100x = (now - state.last_100x_digest).total_seconds() / 3600
        if hours_since_100x >= 4:
            log.info("Running 100x digest scan...")
            if not dry_run:
                from alerts.hundredx_digest import run_100x_digest
                digest_100x = run_100x_digest()
                cycle_result["100x_digest"] = digest_100x
                state.last_100x_digest = now
            else:
                log.info("[DRY RUN] Would run 100x digest")
    except Exception as exc:
        log.warning("100x digest failed: {e}", e=str(exc))

    # 7d. Oracle prediction cycle (every 6 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_oracle = 999
        if state.last_oracle_cycle is not None:
            hours_since_oracle = (now - state.last_oracle_cycle).total_seconds() / 3600
        if hours_since_oracle >= 6:
            log.info("Running Oracle prediction cycle...")
            if not dry_run:
                from oracle.engine import OracleEngine
                from oracle.report import send_oracle_report
                oracle = OracleEngine(db_engine=engine)
                oracle_result = oracle.run_cycle()
                cycle_result["oracle"] = {
                    "predictions": oracle_result["new_predictions"],
                    "scoring": oracle_result["scoring"],
                    "leaderboard": oracle_result.get("leaderboard", [])[:3],
                }
                if oracle_result["new_predictions"] > 0:
                    send_oracle_report(oracle_result)
                state.last_oracle_cycle = now
            else:
                log.info("[DRY RUN] Would run Oracle cycle")
    except Exception as exc:
        log.warning("Oracle cycle failed: {e}", e=str(exc))

    # 7d-ii. TimesFM forecast cycle (every 6 hours, alongside oracle)
    try:
        now = datetime.now(timezone.utc)
        hours_since_timesfm = 999
        last_timesfm = getattr(state, "last_timesfm_cycle", None)
        if last_timesfm is not None:
            hours_since_timesfm = (now - last_timesfm).total_seconds() / 3600
        if hours_since_timesfm >= 6:
            log.info("Running TimesFM forecast cycle...")
            if not dry_run:
                from oracle.forecaster_adapter import run_timesfm_forecast_cycle
                tfm_result = run_timesfm_forecast_cycle(engine)
                cycle_result["timesfm"] = tfm_result
                state.last_timesfm_cycle = now
                log.info(
                    "TimesFM: {n} forecasts generated",
                    n=tfm_result.get("forecasts", 0),
                )
            else:
                log.info("[DRY RUN] Would run TimesFM forecast cycle")
    except Exception as exc:
        log.warning("TimesFM forecast cycle failed: {e}", e=str(exc))

    # 7d-iii. AutoBNN changepoint detection (every 12 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_changepoint = 999
        last_cp = getattr(state, "last_changepoint_cycle", None)
        if last_cp is not None:
            hours_since_changepoint = (now - last_cp).total_seconds() / 3600
        if hours_since_changepoint >= 12:
            log.info("Running AutoBNN changepoint detection...")
            if not dry_run:
                from discovery.changepoint_detector import run_changepoint_cycle
                cp_result = run_changepoint_cycle(engine)
                cycle_result["changepoint_detection"] = cp_result
                state.last_changepoint_cycle = now
                log.info(
                    "Changepoint: {n} changes in {f} features",
                    n=cp_result.get("changepoints_found", 0),
                    f=cp_result.get("features_scanned", 0),
                )
            else:
                log.info("[DRY RUN] Would run changepoint detection")
    except Exception as exc:
        log.warning("Changepoint detection failed: {e}", e=str(exc))

    # 7d-iv. Gemma micro signal classification (every cycle)
    try:
        if not dry_run:
            from ingestion.signal_classifier import classify_recent_signals
            cls_result = classify_recent_signals(engine, limit=30)
            if cls_result.get("classified", 0) > 0:
                cycle_result["signal_classification"] = cls_result
                log.info(
                    "Signal classification: {n} signals classified",
                    n=cls_result["classified"],
                )
    except Exception as exc:
        log.debug("Signal classification skipped: {e}", e=str(exc))

    # 7e. Alpha research heartbeat + signal publishing (every cycle)
    try:
        from alpha_research.heartbeat import run_heartbeat, format_alerts
        from alpha_research.adapters.signal_adapter import publish_all_alpha_signals

        hb_alerts = run_heartbeat(engine)
        if hb_alerts:
            log.info(format_alerts(hb_alerts))
        cycle_result["alpha_heartbeat"] = {
            "alerts": len(hb_alerts),
            "critical": sum(1 for a in hb_alerts if a.level == "CRITICAL"),
        }

        if not dry_run:
            pub_result = publish_all_alpha_signals(engine)
            cycle_result["alpha_signals_published"] = pub_result
            log.info("Alpha signals published: {r}", r=pub_result)
        else:
            log.info("[DRY RUN] Would publish alpha signals")
    except Exception as exc:
        log.warning("Alpha research heartbeat failed: {e}", e=str(exc))

    # 7f. Intelligence modules — trust scoring, cross-reference, lever pullers,
    #     actor network, source audit, postmortem, options tracking, backtests
    try:
        intel_result = run_intelligence_tasks(engine, state, dry_run=dry_run)
        if intel_result:
            cycle_result["intelligence"] = intel_result
    except Exception as exc:
        log.warning("Intelligence tasks failed: {e}", e=str(exc))

    # 7g. Rotation paper trading — daily after 17:00 UTC (market close)
    try:
        now_utc = datetime.now(timezone.utc)
        # Run once per day between 17:00-17:30 UTC (after US market close)
        if 17 <= now_utc.hour < 18 and now_utc.minute < 30:
            last_rotation = getattr(state, "_last_rotation_date", None)
            if last_rotation != now_utc.date():
                log.info("Running rotation paper trader...")
                if not dry_run:
                    from scripts.rotation_paper_trader import run_paper_trading
                    rotation_result = run_paper_trading(engine)
                    cycle_result["rotation_paper_trading"] = rotation_result
                    state._last_rotation_date = now_utc.date()
                else:
                    log.info("[DRY RUN] Would run rotation paper trader")
    except Exception as exc:
        log.warning("Rotation paper trading failed: {e}", e=str(exc))

    # 7h. Tiingo bulk data pull — overnight (02:00-06:00 UTC) to maximize 40GB/mo
    try:
        now_utc = datetime.now(timezone.utc)
        if 2 <= now_utc.hour < 6:
            last_tiingo_bulk = getattr(state, "_last_tiingo_bulk_date", None)
            if last_tiingo_bulk != now_utc.date():
                log.info("Running Tiingo bulk data pull (overnight window)...")
                if not dry_run:
                    try:
                        from ingestion.tiingo_pull import TiingoPuller
                        tp = TiingoPuller(engine)
                        # Pull all tracked tickers (daily update)
                        tiingo_result = tp.pull_all(start_date=str(now_utc.date() - timedelta(days=5)))
                        cycle_result["tiingo_daily"] = {
                            "succeeded": sum(1 for r in tiingo_result if r["status"] == "SUCCESS"),
                            "total": len(tiingo_result),
                        }
                    except Exception as exc:
                        log.warning("Tiingo price pull failed: {e}", e=str(exc))

                    try:
                        from ingestion.tiingo_news_pull import TiingoNewsPuller
                        tnp = TiingoNewsPuller(engine)
                        news_result = tnp.pull_all(start_date=str(now_utc.date() - timedelta(days=3)))
                        cycle_result["tiingo_news"] = {
                            "articles": sum(r.get("articles", 0) for r in news_result),
                            "tickers": len(news_result),
                        }
                    except Exception as exc:
                        log.warning("Tiingo news pull failed: {e}", e=str(exc))

                    state._last_tiingo_bulk_date = now_utc.date()
                else:
                    log.info("[DRY RUN] Would run Tiingo bulk pull")
    except Exception as exc:
        log.warning("Tiingo bulk pull failed: {e}", e=str(exc))

    # 8. Git push — commit and push any new outputs
    try:
        push_result = git_push_outputs()
        cycle_result["git_push"] = push_result
    except Exception as exc:
        log.warning("Git push failed: {e}", e=str(exc))

    # 8b. LLM Task Queue status — report throughput and queue depth
    try:
        from orchestration.llm_taskqueue import get_task_queue
        tq = get_task_queue(engine)
        cycle_result["llm_taskqueue"] = tq.get_status()
    except Exception as exc:
        log.debug("Hermes: LLM task queue status failed: {e}", e=str(exc))

    # 9. Save cycle snapshot
    elapsed = time.monotonic() - cycle_start
    cycle_result["elapsed_seconds"] = round(elapsed, 1)
    cycle_result["operator_state"] = state.to_dict()
    save_cycle_snapshot(engine, cycle_result)

    log.info(
        "═══ Cycle {n} complete — {t:.1f}s ═══",
        n=state.cycle_count, t=elapsed,
    )
    return cycle_result


def main(args: list[str] | None = None) -> None:
    """Entry point for the Hermes operator daemon."""
    parser = argparse.ArgumentParser(description="GRID Hermes Operator — 24/7 self-healing daemon")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="Diagnose only, don't fix anything")
    parser.add_argument(
        "--interval", type=int, default=CYCLE_INTERVAL_SECONDS,
        help=f"Seconds between cycles (default: {CYCLE_INTERVAL_SECONDS})",
    )
    opts = parser.parse_args(args)

    log.info("╔══════════════════════════════════════════╗")
    log.info("║   GRID Hermes Operator — Starting Up     ║")
    log.info("║   Mode: {m:33s}║", m="single cycle" if opts.once else f"continuous ({opts.interval}s)")
    log.info("║   Dry run: {d:30s}║", d=str(opts.dry_run))
    log.info("╚══════════════════════════════════════════╝")

    state = OperatorState()

    # Share state with the API for the /hermes-status endpoint
    try:
        from api.routers.system import set_hermes_state
        set_hermes_state(state)
        log.info("Hermes state shared with API for /hermes-status endpoint")
    except Exception as exc:
        log.debug("Hermes: state share with API failed (API may not be running): {e}", e=str(exc))

    # Start the LLM task queue as a background daemon thread so the
    # onboard model is never idle — processes real-time, scheduled, and
    # background tasks continuously.
    _tq_thread = None
    if not opts.dry_run:
        try:
            from orchestration.llm_taskqueue import start_task_queue_thread
            _tq_thread = start_task_queue_thread()
            log.info("LLM Task Queue daemon thread launched")
        except Exception as exc:
            log.warning("Failed to start LLM task queue: {e}", e=str(exc))

    # Run DB model migrations once on startup (idempotent)
    try:
        from oracle.model_factory import migrate_default_models
        migrate_default_models(engine)
    except Exception as exc:
        log.debug("migrate_default_models: {e}", e=str(exc))

    if opts.once:
        result = run_cycle(state, dry_run=opts.dry_run)
        print(json.dumps(result, default=str, indent=2))
        return

    # Continuous loop with per-cycle timeout
    import signal
    import threading

    def _run_cycle_with_timeout(state, dry_run, timeout):
        """Run a cycle in a thread with a hard timeout."""
        result = [None]
        error = [None]
        def _target():
            try:
                result[0] = run_cycle(state, dry_run=dry_run)
            except Exception as exc:
                error[0] = exc
        t = threading.Thread(target=_target, daemon=True)
        t.start()
        t.join(timeout=timeout)
        if t.is_alive():
            stuck_on = state.current_step
            log.error(
                "Cycle {n} TIMED OUT after {s}s (stuck on: {step}) "
                "— blacklisting and starting fresh",
                n=state.cycle_count, s=timeout,
                step=stuck_on or "unknown",
            )
            # Blacklist whatever was running when we timed out
            if stuck_on:
                state.cooldowns.blacklist_for_timeout(stuck_on)
            return  # Thread is daemon, will be abandoned
        if error[0]:
            raise error[0]

    while True:
        try:
            _run_cycle_with_timeout(state, opts.dry_run, CYCLE_TIMEOUT_SECONDS)
        except KeyboardInterrupt:
            log.info("Operator shutting down (keyboard interrupt)")
            break
        except Exception as exc:
            log.error("Unexpected error in operator cycle: {e}", e=str(exc))
            log.error(traceback.format_exc())
            state.consecutive_failures += 1
            if state.consecutive_failures > 10:
                log.error("10 consecutive failures — sleeping 30 minutes before retry")
                time.sleep(1800)
                state.consecutive_failures = 0

        log.info("Next cycle in {s}s...", s=opts.interval)
        time.sleep(opts.interval)


if __name__ == "__main__":
    main()
