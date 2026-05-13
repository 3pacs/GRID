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

NOTE (hermes/scheduler split, verified 2026-04-13):
    `ingestion/scheduler.py` is the canonical per-puller scheduler (~48 pullers
    registered there, see `scheduler.build_puller_list`). This operator layers
    ADDITIONAL intelligence-side tasks on top:
        - `intelligence.icij_linker.link_actors`
        - `intelligence.milestone_tracker.scan_all_tickers`
        - `intelligence.obsidian_agent.run_agent_cycle`
        - `intelligence.attention_anomaly.get_alerts`
        - `intelligence.actor_researcher` + `actor_discovery`
    These are intentionally NOT in `ingestion/scheduler.py` because they run
    AFTER ingestion (they consume the raw data the scheduler just pulled).
    Do not "reconcile" by deleting them or by moving them into scheduler.py —
    they are the intentional hermes-only half of the split. Previous drift
    references to `power_mapper` and `gdelt_news` have been removed; the
    three above are the live surface.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Ensure grid/ is on sys.path
_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log
from sqlalchemy import text  # used by run_cycle DB writes


# ─── Configuration ───────────────────────────────────────────────────

CYCLE_INTERVAL_SECONDS = 300          # 5 minutes between cycles
CYCLE_TIMEOUT_SECONDS = 4500          # 75 min max per cycle (oracle dominates one in N cycles)
                                       # (per-step timeouts kick in earlier; this
                                       # is a safety net for unforeseen hangs)
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

# Per-step timeouts — caps how long a single step can hold up the cycle.
# Hung LLM calls used to consume the full 900s cycle budget; these caps + the
# cooldown blacklist break the loop after a single timeout.
ORACLE_CYCLE_TIMEOUT_SECONDS = 4000           # oracle.run_cycle: 41 tickers x ~80s + headroom (was 300, caused 24h blacklist loop)
SIGNAL_CLASSIFICATION_TIMEOUT_SECONDS = 120   # gemma micro classifier batch
ANOMALY_NARRATION_TIMEOUT_SECONDS = 90        # gemma micro anomaly narrator
KNOWLEDGE_MAP_TIMEOUT_SECONDS = 120           # gemma micro knowledge mapper
DIAGNOSE_PULLS_TIMEOUT_SECONDS = 240          # Hermes pull diagnosis/fix step — bumped 2026-05-08 because diagnose runs per-source retry which can chain HTTP calls
SMART_INGESTION_TIMEOUT_SECONDS = 300         # smart_scheduler.tick() — matches TICK_TIME_BUDGET_S in ingestion/smart_scheduler.py so Hermes doesn't pull the plug while SmartScheduler is mid-shutdown
TIMESFM_TIMEOUT_SECONDS = 240                 # oracle/forecaster_adapter.run_timesfm_forecast_cycle
INTELLIGENCE_TASKS_TIMEOUT_SECONDS = 360      # trust/forecasts/thesis/cross-ref/options + daily-window backtest_scanner.review_existing_hypotheses (LLM-bound). Bumped 2026-05-08 from 180s after the timeout machinery was actually working — 180s was empirical-untested guess; 360s reflects observed daily run length with LLM calls


def _run_with_timeout(name: str, fn, timeout_s: int, state):
    """Execute fn() with a hard timeout. On timeout, blacklist via cooldown.

    Uses concurrent.futures so the call returns even if the worker thread is
    still alive (it becomes a daemon-like orphan). This is acceptable because
    the orphan eventually finishes (LLM eventually returns) and no destructive
    side-effect is in flight on these read-mostly steps.

    NOTE: do NOT use `with ThreadPoolExecutor(...) as ex:`. The context
    manager calls `shutdown(wait=True)` on exit, which blocks until the
    orphan worker finishes — completely defeating the timeout. This bug
    silently broke every stage timeout in Hermes for months: the cycle
    appeared to time out at the cycle-level (600s) "stuck on stage X"
    when actually the stage HAD already passed its budget but the
    shutdown() call was waiting for the still-running thread.

    Fix: explicit shutdown(wait=False) on timeout so we genuinely hand
    back control. The orphan thread keeps running but as a true daemon;
    the next cycle starts on schedule.

    Returns:
        (result, ok) — fn's return value (or None on timeout/error), success bool.
    """
    import concurrent.futures
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(fn)
    try:
        result = fut.result(timeout=timeout_s)
        ex.shutdown(wait=True)  # success path — let the worker tear down cleanly
        return result, True
    except concurrent.futures.TimeoutError:
        log.warning(
            "Step '{n}' timed out after {s}s — blacklisting for {h}h",
            n=name, s=timeout_s, h=TIMEOUT_BLACKLIST_HOURS,
        )
        state.cooldowns.blacklist_for_timeout(name)
        # CRITICAL: wait=False so we don't block on the orphan thread.
        # cancel_futures=True attempts to cancel anything still queued
        # (no-op here since we only submitted one future).
        ex.shutdown(wait=False, cancel_futures=True)
        return None, False
    except Exception as exc:
        log.warning("Step '{n}' raised: {e}", n=name, e=str(exc))
        state.cooldowns.record_attempt(name, success=False, error=str(exc))
        ex.shutdown(wait=False, cancel_futures=True)
        return None, False

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
    "ag_commodity_futures":   {"mod": "ingestion.altdata.ag_commodity_futures",   "cls": "AgCommodityFuturesPuller",
                               "interval_h": 24},
    "sec_13f_live":           {"mod": "ingestion.altdata.sec_13f_live",           "fn": "run",
                               "interval_h": 168},
    "sec_xbrl_financials":    {"mod": "ingestion.altdata.sec_xbrl_financials",    "cls": "SECXBRLFinancialsPuller",
                               "pull_kwargs": {"limit": 200}},
    "sec_xbrl_shares":        {"mod": "ingestion.altdata.sec_xbrl_shares",        "cls": "SECXBRLSharesPuller",
                               "pull_kwargs": {"limit": 200, "backfill_days": 90}},
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
    "supply_chain_parser":    {"mod": "ingestion.altdata.supply_chain_parser",    "fn": "run_weekly",
                               "interval_h": 168},
    "pct_cogs_enrichment":    {"mod": "intelligence.pct_cogs_enrichment",         "fn": "run_weekly",
                               "interval_h": 168},
    "supply_chain_edge_validator": {"mod": "intelligence.supply_chain_edge_validator", "fn": "run_weekly",
                               "interval_h": 168},
    "apple_supplier_list":    {"mod": "ingestion.altdata.apple_supplier_list",    "fn": "run_annual",
                               "interval_h": 8760},
    "sec_item_1c_cyber":      {"mod": "ingestion.altdata.sec_item_1c_cyber",      "fn": "run_weekly",
                               "interval_h": 168},
    "regulatory_events":      {"mod": "ingestion.altdata.regulatory_events",      "fn": "run_weekly",
                               "interval_h": 168},

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

    # -- Margin debt materializer --
    "margin_debt":        {"mod": "ingestion.altdata.margin_debt",   "cls": "MarginDebtPuller"},

    # -- DeFi Llama (TVL, stablecoin flows, yields) --
    "defillama":          {"mod": "ingestion.altdata.defi_llama_puller", "cls": "DefiLlamaPuller"},

    # -- Dune Analytics (smart money, CEX flows, narrative heat) --
    "dune":               {"mod": "ingestion.altdata.dune_puller",       "cls": "DunePuller",
                           "api_key": "DUNE_API_KEY", "interval_h": 6},

    # -- New upgraded data sources (2026-03-31) --

    "nyfed_gscpi":        {"mod": "ingestion.altdata.nyfed_gscpi",       "cls": "NYFedGSCPIPuller"},
    "polymarket":         {"mod": "ingestion.altdata.polymarket",        "cls": "PolymarketPuller"},
    "kalshi_markets":     {"mod": "ingestion.altdata.kalshi_markets",    "cls": "KalshiMarketsPuller"},
    "stocktwits":         {"mod": "ingestion.altdata.stocktwits",        "cls": "StockTwitsPuller"},
    "pmxt_archive":       {"mod": "ingestion.altdata.pmxt_archive",      "cls": "PmxtArchivePuller"},
    "tiingo":             {"mod": "ingestion.tiingo_pull",               "cls": "TiingoPuller"},

    # -- Historical prediction market dataset (Jon Becker, one-time bulk load) --
    "pm_history":         {"mod": "ingestion.altdata.prediction_market_history", "cls": "PredictionMarketHistoryPuller"},

    # -- Obsidian vault sync (every ~5 min) --
    "obsidian":           {"mod": "ingestion.altdata.obsidian_sync",     "fn": "run_sync",                "interval_h": 0.083},

    # -- Clinical trial signal ingestor (daily) --
    "trial_ingestor":     {"mod": "grid.ingestors.trial_ingestor",      "fn": "main",                    "interval_h": 24},

    # -- Dark alt-data pullers adopted from orphan triage (2026-04-14) --
    # BasePuller-compatible (instantiated via _resolve_puller with db_engine=engine, pull_all default).
    "fx_rates":           {"mod": "ingestion.altdata.fx_rates",            "cls": "FXRatesPuller",         "interval_h": 24},
    "tiingo_news":        {"mod": "ingestion.altdata.tiingo_news",         "cls": "TiingoNewsPuller",      "interval_h": 1},
    "warn_layoffs":       {"mod": "ingestion.altdata.warn_layoffs",        "cls": "WARNLayoffsPuller",     "interval_h": 24},
    "wikidata_persons":   {"mod": "ingestion.altdata.wikidata_persons",    "cls": "WikidataPersonPuller",  "interval_h": 168},
    # Module-level fn entry — pull_all(engine) at module scope.
    "quiverquant":        {"mod": "ingestion.altdata.quiverquant",         "fn": "pull_all",                "interval_h": 24},
    # SKIPPED at runtime: these classes use __init__(self, engine) (not db_engine=) and pull() (not pull_all),
    # so _resolve_puller will fail. Registered for audit/discovery; needs a wrapper or _resolve_puller upgrade
    # before runtime execution. Track via TODO.
    "crypto_etf_flows":   {"mod": "ingestion.altdata.crypto_etf_flows",    "cls": "CryptoETFPuller",       "interval_h": 24,  "skip_runtime": "engine= ctor / pull() method mismatch"},
    "hyperliquid_puller": {"mod": "ingestion.altdata.hyperliquid_puller",  "cls": "HyperliquidPuller",     "interval_h": 1,   "skip_runtime": "engine= ctor / pull() method mismatch"},
    "onchain_rpc":        {"mod": "ingestion.altdata.onchain_rpc",         "cls": "OnChainRPCPoller",      "interval_h": 1,   "skip_runtime": "engine= ctor / pull() method mismatch"},
    "whale_alert":        {"mod": "ingestion.altdata.whale_alert",         "cls": "WhaleAlertPuller",      "interval_h": 1,   "skip_runtime": "engine= ctor / pull() method mismatch"},
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
    """Pull latest changes from remote.

    Safe-by-default semantics — never overwrites operator-applied local
    commits or uncommitted edits:

      1. **Skip on non-target branch.** If the operator has checked out
         a feature branch (e.g. mid-hotfix), auto-pulling ``main`` over
         it is almost never what they want. We bail with a clear log
         line and let them merge/rebase manually when ready.

      2. **Fast-forward only.** We use ``git pull --ff-only`` so the pull
         either applies cleanly (no divergence) or refuses (returns
         non-zero). Prior implementation used ``--rebase`` plus a
         ``pull`` fallback, both of which could silently rewrite local
         commits or merge ``main`` into a feature branch.

    The pre-2026-05-13 implementation lost a session's worth of
    cherry-picked hot-fixes once (caught in time because the working
    tree happened to be dirty); this guards against the next time.
    """
    if not GIT_SYNC_ENABLED:
        return {"skipped": "disabled"}

    rc_repo, out_repo = _git(["rev-parse", "--is-inside-work-tree"])
    if rc_repo != 0 or out_repo.strip().splitlines()[-1:] != ["true"]:
        log.info("Git pull skipped: {o}", o=out_repo[:200])
        return {"skipped": "not_a_git_worktree", "output": out_repo[:200]}

    rc_branch, current_branch = _git(["rev-parse", "--abbrev-ref", "HEAD"])
    branch_name = current_branch.strip() if rc_branch == 0 else ""
    if branch_name and branch_name != GIT_BRANCH:
        log.info(
            "Git pull skipped: on branch {b}, not {target}. "
            "Merge or rebase to {target} manually when ready.",
            b=branch_name, target=GIT_BRANCH,
        )
        return {"skipped": "non_target_branch", "branch": branch_name}

    log.info("Git pull — syncing latest changes (--ff-only)")
    rc, out = _git(["pull", "--ff-only", GIT_REMOTE, GIT_BRANCH])
    if rc == 0:
        log.info("Git pull OK: {o}", o=out[:200])
        return {"status": "ok", "output": out[:200]}

    log.warning(
        "Git pull failed (not fast-forward — local branch has unique "
        "commits or working tree dirty): {o}",
        o=out[:300],
    )
    return {"status": "failed_non_ff", "output": out[:300]}


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
            tc_result = _run_intel_task(
                "trust_cycle", run_trust_cycle, state, engine,
            )
            results["trust_cycle"] = tc_result
            # Mirror to Obsidian session log (best-effort).
            try:
                from intelligence.obsidian_log import log_trust_cycle
                scoring = (tc_result or {}).get("scoring") or {}
                if isinstance(scoring, dict):
                    log_trust_cycle(scoring)
            except Exception as exc:  # noqa: BLE001
                log.debug("Obsidian log_trust_cycle skipped: {e}", e=str(exc))
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

        # 13F mining block (power_mapper module deleted in Wave 1 — was zero-caller orphan).
        # Replaced by the canonical actor network + institutional_holdings queries above.
        try:
            pass
        except Exception as exc:
            log.warning("Power mapping failed: {e}", e=str(exc))

        state.last_actor_wealth = now

    # ── Daily at 2:00 AM (with catch-up) ─────────────────────────────
    # Fires if (a) we're in the 2:00-2:10 UTC window, OR (b) we're past 2 AM
    # UTC today and haven't run yet today (catches restarts, cycle timeouts,
    # long cycles, or any case where the 10-minute window was missed).
    # The _hours_since(last_daily_intel) >= 20 guard prevents double-runs.

    is_daily_window = (now.hour == 2 and now.minute < 10)
    is_catch_up = (
        now.hour >= 2
        and (state.last_daily_intel is None or state.last_daily_intel.date() < now.date())
    )
    daily_due = (is_daily_window or is_catch_up) and _hours_since(state.last_daily_intel) >= 20

    if daily_due:
        log.info(
            "Running daily intelligence batch (window={w} catch_up={c})",
            w=is_daily_window, c=(is_catch_up and not is_daily_window),
        )

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

        # ── 13F mining + actor enrichment + milestone scoring ────────

        # Actor research — LLM enriches sparse actors, follows rabbit holes
        try:
            from intelligence.actor_researcher import research_batch
            actor_result = research_batch(engine, batch_size=20)
            results["actor_research"] = actor_result
            log.info(
                "Actor research: {u} enriched, {n} new actors, {r} rabbit holes",
                u=actor_result.get("updated", 0),
                n=actor_result.get("new_actors", 0),
                r=actor_result.get("rabbit_holes", 0),
            )
        except Exception as exc:
            log.warning("Actor research failed: {e}", e=str(exc))

        # ICIJ cross-reference — fuzzy match actors against offshore entities
        try:
            from intelligence.icij_linker import link_actors
            icij_result = link_actors(engine, min_similarity=0.6, limit=500)
            results["icij_linking"] = {"matches": len(icij_result)}
            log.info("ICIJ linking: {n} matches found", n=len(icij_result))
        except Exception as exc:
            log.warning("ICIJ linking failed: {e}", e=str(exc))

        # Milestone scoring — execution scorecards for all companies
        try:
            from intelligence.milestone_tracker import scan_all_tickers
            milestones = scan_all_tickers(engine)
            results["milestone_scoring"] = {"companies_scored": len(milestones)}
            log.info("Milestone scoring: {n} companies scored", n=len(milestones))
        except Exception as exc:
            log.warning("Milestone scoring failed: {e}", e=str(exc))

        # Attention anomaly — Wikipedia + Trends spike detection
        try:
            from intelligence.attention_anomaly import get_alerts
            alerts = get_alerts(engine, threshold=60.0)
            results["attention_alerts"] = {"high_alerts": len(alerts)}
            if alerts:
                log.info("ATTENTION: {n} entities with unusual attention", n=len(alerts))
        except Exception as exc:
            log.warning("Attention anomaly failed: {e}", e=str(exc))

        # EDGAR transcripts — 8-K filings with LLM milestone extraction
        try:
            from ingestion.altdata.edgar_transcripts import EdgarTranscriptPuller
            edgar = EdgarTranscriptPuller(engine)
            edgar_result = edgar.pull(days_back=30)
            results["edgar_transcripts"] = edgar_result
            log.info("EDGAR: {f} filings, {g} guidance phrases",
                     f=edgar_result.get("filings_processed", 0),
                     g=edgar_result.get("guidance_extracted", 0))
        except Exception as exc:
            log.warning("EDGAR transcripts failed: {e}", e=str(exc))

        # Corporate actions — regex-mine 8-Ks for M&A, buybacks,
        # dividends, debt, equity issuance. Writes capital_flows rows
        # with period_type='announcement'. Daily: last 30 days of 8-Ks.
        try:
            from ingestion.altdata.corporate_actions_parser import (
                CorporateActionsParser,
            )
            corp = CorporateActionsParser(engine)
            try:
                corp_result = corp.pull(days_back=30)
            finally:
                corp.close()
            results["corporate_actions"] = corp_result
            log.info(
                "corporate_actions: {r} rows from {f} filings "
                "({h} tickers with hits)",
                r=corp_result.get("rows_inserted", 0),
                f=corp_result.get("filings_scanned", 0),
                h=corp_result.get("tickers_with_hits", 0),
            )
        except Exception as exc:
            log.warning("corporate_actions failed: {e}", e=str(exc))

        # Capital-flow rollups — derives ttm rows from quarterly XBRL
        # data and folds announcement rows into annual_rolled rows so
        # the API layer can show M&A / buyback events inside annual
        # totals without losing the original event records.
        # Runs daily AFTER the XBRL ingestor + corporate_actions so it
        # always sees the freshest base rows.
        try:
            from intelligence.company_financial_rollups import run_all as cf_rollup_run
            cf_stats = cf_rollup_run(engine)
            results["capital_flow_rollups"] = cf_stats
            log.info(
                "capital_flow_rollups: ttm={t} rolled={r}",
                t=cf_stats.get("ttm_rows", 0),
                r=cf_stats.get("rolled_rows", 0),
            )
        except Exception as exc:
            log.warning("capital_flow_rollups failed: {e}", e=str(exc))

        # Fundamental-vs-price divergence — snapshot daily so the
        # `fundamental_divergence` table always has a fresh row per
        # ticker in the latest snapshot. Runs AFTER capital_flow_rollups
        # so it sees the freshest revenue / margin rows.
        try:
            from intelligence.fundamental_divergence import (
                snapshot_all as fd_snapshot_all,
            )
            fd_stats = fd_snapshot_all(engine)
            results["fundamental_divergence"] = fd_stats
            log.info(
                "fundamental_divergence: wrote={w} long={l} short={s}",
                w=fd_stats.get("written", 0),
                l=(fd_stats.get("counts") or {}).get("long_candidate", 0),
                s=(fd_stats.get("counts") or {}).get("short_candidate", 0),
            )
        except Exception as exc:
            log.warning("fundamental_divergence failed: {e}", e=str(exc))

        # Holder / deal overlap — pre-positioning detector. Cross-
        # references institutional_holdings 13F snapshots against
        # capital_flows acquisition announcements to find filers that
        # held BOTH the acquirer and the target before the deal was
        # announced. Must run AFTER corporate_actions (announcement
        # rows) and AFTER the 13F ingestor. Writes holder_deal_overlap.
        try:
            from intelligence.holder_deal_overlap import run as hdo_run
            hdo_stats = hdo_run(engine)
            results["holder_deal_overlap"] = hdo_stats
            log.info(
                "holder_deal_overlap: deals={d} overlaps={o} pre={p}",
                d=hdo_stats.get("deals_scanned", 0),
                o=hdo_stats.get("overlaps_written", 0),
                p=hdo_stats.get("pre_positioned", 0),
            )
        except Exception as exc:
            log.warning("holder_deal_overlap failed: {e}", e=str(exc))

        # ── Daily file rotation (audit #49, #61) ────────────────────
        # Insight files in outputs/llm_insights/ accumulate forever
        # without cleanup; the dir hit 100k+ files (45 days, ~22k/day
        # peaks) before this hook was wired in. 30-day retention caps
        # steady-state at ~660k worst case, manageable.
        try:
            from outputs.llm_logger import cleanup_old_insights
            n_cleaned = cleanup_old_insights(max_age_days=30)
            if n_cleaned:
                log.info("Insight cleanup: deleted {n} files (>30d)", n=n_cleaned)
        except Exception as exc:
            log.warning("Insight cleanup failed: {e}", e=str(exc))

        # Market briefings — same pattern, 90-day retention since these
        # are higher-value artifacts (full market write-ups).
        try:
            from ollama.market_briefing import MarketBriefingEngine
            n_briefings = MarketBriefingEngine.cleanup_old_briefings(max_age_days=90)
            if n_briefings:
                log.info("Briefing cleanup: deleted {n} files (>90d)", n=n_briefings)
        except Exception as exc:
            log.warning("Briefing cleanup failed: {e}", e=str(exc))

        # errors.jsonl — append-only log, just truncate to last 5000 lines
        # (~3-4 days of errors at current rate). Cheap, atomic.
        try:
            from pathlib import Path
            errfile = Path(_GRID_DIR) / ".server-logs" / "errors.jsonl"
            if errfile.exists() and errfile.stat().st_size > 1_000_000:
                lines = errfile.read_text(encoding="utf-8", errors="replace").splitlines()
                if len(lines) > 5000:
                    keep = lines[-5000:]
                    tmp = errfile.with_suffix(".jsonl.tmp")
                    tmp.write_text("\n".join(keep) + "\n", encoding="utf-8")
                    tmp.replace(errfile)
                    log.info("errors.jsonl rotated: {n} → 5000 lines",
                             n=len(lines))
        except Exception as exc:
            log.warning("errors.jsonl rotation failed: {e}", e=str(exc))

        state.last_daily_intel = now

    # ── Daily at 3:00 AM UTC — sector health snapshot ───────────────
    # Computes the composite health score for every sector in SECTOR_MAP
    # and upserts one row per (sector, today) into sector_health_snapshots.
    # The row ~30 days back is read by the API to label trend_30d.

    is_sector_health_window = (now.hour == 3 and now.minute < 10)
    sector_health_due = (
        is_sector_health_window
        and _hours_since(state.last_sector_health) >= 20
    )

    if sector_health_due:
        log.info("Running daily sector health snapshot (3:00 AM UTC)")
        try:
            from intelligence.sector_health import snapshot_all_sectors
            sh_result = snapshot_all_sectors(engine)
            results["sector_health_snapshot"] = sh_result
            log.info(
                "sector_health: {n} snapshots written",
                n=sh_result.get("snapshots_written", 0),
            )
        except Exception as exc:
            log.warning("sector_health snapshot failed: {e}", e=str(exc))
            results["sector_health_snapshot"] = {
                "status": "failed", "error": str(exc),
            }
        state.last_sector_health = now

    # ── Daily at 6:30 UTC — forced-flow waterfall briefing ──────────
    # Implements docs/playbooks/opex_waterfall.md. Runs once per day,
    # pre-US-market-open, emits a LEVER/CONDITION/THESIS/INVALIDATION
    # posture and fires waterfall_watch alerts when >= 2 of the 5
    # forced-flow conditions are simultaneously tripped.

    is_forced_flow_window = (now.hour == 6 and now.minute < 40)
    forced_flow_due = (
        is_forced_flow_window
        and _hours_since(state.last_forced_flow_brief) >= 20
    )

    if forced_flow_due:
        log.info("Running forced-flow waterfall briefing (06:30 UTC)")
        try:
            from intelligence.forced_flow_monitor import run_forced_flow_cycle
            results["forced_flow_brief"] = _run_intel_task(
                "forced_flow_brief",
                run_forced_flow_cycle,
                state,
                engine,
            )
        except Exception as exc:
            log.warning("Forced flow monitor import failed: {e}", e=str(exc))
            results["forced_flow_brief"] = {"status": "failed", "error": str(exc)}
        state.last_forced_flow_brief = now

    # ── Daily at 4:00 AM — connection enrichment ────────────────────

    is_enrich_window = (now.hour == 4 and now.minute < 10)
    enrich_due = is_enrich_window and _hours_since(state.last_enrich_connections) >= 20

    if enrich_due:
        log.info("Running daily connection enrichment (4:00 AM)")
        try:
            from scripts.enrich_connections import main as enrich_main
            enrich_main()
            results["enrich_connections"] = {"status": "ok"}
            log.info("Connection enrichment complete")
        except Exception as exc:
            log.warning("Connection enrichment failed: {e}", e=str(exc))
            results["enrich_connections"] = {"status": "failed", "error": str(exc)}
        state.last_enrich_connections = now

        # News-to-signals: convert intelligence tables to signal_data
        try:
            from scripts.news_to_signals import main as news_signals_main
            n_signals = news_signals_main()
            results["news_to_signals"] = {"status": "ok", "signals": n_signals}
            log.info("News-to-signals complete: {n} signals", n=n_signals)
        except Exception as exc:
            log.warning("News-to-signals failed: {e}", e=str(exc))
            results["news_to_signals"] = {"status": "failed", "error": str(exc)}

    # ── Hourly catch-up — contagion backtest scoring ─────────────────
    #
    # Walks matured contagion_predictions rows and scores them against the
    # realised downstream price move in raw_series. The scorer is idempotent
    # and catches up older unscored rows, so hourly runs are safe.

    is_contagion_bt_window = now.minute < 10
    contagion_bt_due = (
        is_contagion_bt_window
        and _hours_since(state.last_contagion_backtest) >= 1
    )

    if contagion_bt_due:
        log.info("Running contagion backtest scoring (hourly catch-up)")
        try:
            from intelligence.contagion_backtest import score_all_windows
            bt_result = score_all_windows(engine)
            results["contagion_backtest"] = bt_result
            window_summary = " ".join(
                f"{days}d={rows}" for days, rows in sorted(bt_result.items())
            )
            log.info("contagion_backtest: {summary} rows", summary=window_summary)
        except Exception as exc:
            log.warning("contagion_backtest failed: {e}", e=str(exc))
            results["contagion_backtest"] = {"status": "failed", "error": str(exc)}
        state.last_contagion_backtest = now

        # Close the loop: decay/validate supply_chain_edges from the
        # freshly scored backtests. Runs immediately after contagion
        # backtest so the feedback sees the newest rows.
        try:
            from intelligence.postmortem import apply_contagion_feedback
            fb_result = apply_contagion_feedback(engine, since_hours=24)
            results["contagion_feedback"] = fb_result
            log.info(
                "contagion_feedback: decayed={d} confirmed={h} "
                "no_edge={ne} errors={e}",
                d=fb_result.get("decayed", 0),
                h=fb_result.get("confirmed", 0),
                ne=fb_result.get("skipped_no_edge", 0),
                e=fb_result.get("errors", 0),
            )
        except Exception as exc:
            log.warning("contagion_feedback failed: {e}", e=str(exc))
            results["contagion_feedback"] = {"status": "failed", "error": str(exc)}
        state.last_contagion_feedback = now

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


# ─── Obsidian vault sync ─────────────────────────────────────────────

def _run_obsidian_cycle(engine: Any) -> dict[str, Any]:
    """Run vault sync + agent loop, return combined result dict."""
    try:
        from ingestion.altdata.obsidian_sync import run_sync, regenerate_dashboard
        from intelligence.obsidian_agent import run_agent_cycle

        # 1. Sync vault <-> Postgres
        sync_result = run_sync(engine)
        log.info("Obsidian sync: {r}", r=sync_result)

        # 2. Run active agent
        agent_result = run_agent_cycle(engine)
        log.info("Obsidian agent: {r}", r=agent_result)

        # 3. Regenerate dashboard if anything changed
        total_changes = (
            sync_result.get("inserted", 0) + sync_result.get("updated", 0)
            + sync_result.get("outbound_written", 0)
            + agent_result.get("enriched", 0) + agent_result.get("acted", 0)
        )
        if total_changes > 0:
            regenerate_dashboard(engine)

        # 4. Refresh concept stub pages (idempotent — only writes if backlinks exist)
        stubs_created = 0
        try:
            from scripts.create_concept_stubs import CONCEPTS, WIKI_DIR, find_backlinks, create_stub
            from pathlib import Path as _Path

            WIKI_DIR.mkdir(parents=True, exist_ok=True)
            docs_dir = _Path(__file__).resolve().parent.parent / "docs"
            for target, (category, description, source_path) in CONCEPTS.items():
                target_file = WIKI_DIR / f"{target}.md"
                backlinks = find_backlinks(target, docs_dir)
                if backlinks:
                    content = create_stub(target, category, description, source_path, backlinks)
                    target_file.write_text(content, encoding="utf-8")
                    stubs_created += 1
            if stubs_created:
                log.info("Obsidian concept stubs: {n} pages refreshed", n=stubs_created)
        except Exception as exc:
            log.debug("Concept stubs skipped: {e}", e=str(exc))

        # 5. Add wikilinks to docs (only if concept stubs changed)
        backlinks_added = 0
        if stubs_created > 0:
            try:
                from scripts.obsidian_backlinks import (
                    collect_markdown_files, build_doc_registry,
                    add_wikilinks, CONCEPT_LINKS,
                )

                files = collect_markdown_files()
                doc_registry = build_doc_registry(files)
                all_entities = {**CONCEPT_LINKS}
                skip_stems = {"README", "CLAUDE", "index", "plan", "config"}
                for stem, target in doc_registry.items():
                    if stem not in skip_stems and len(stem) > 3:
                        all_entities[stem] = target

                for f in files:
                    content = f.read_text(encoding="utf-8", errors="replace")
                    new_content, changes = add_wikilinks(content, f, all_entities)
                    if changes:
                        f.write_text(new_content, encoding="utf-8")
                        backlinks_added += len(changes)

                if backlinks_added:
                    log.info("Obsidian backlinks: {n} links added", n=backlinks_added)
            except Exception as exc:
                log.debug("Backlinks skipped: {e}", e=str(exc))

        return {
            "sync": sync_result, "agent": agent_result,
            "dashboard_triggered": total_changes > 0,
            "concept_stubs": stubs_created,
            "backlinks_added": backlinks_added,
        }

    except Exception as e:
        log.error("Obsidian cycle failed: {e}", e=e)
        return {"error": str(e)}


# ─── Main loop ───────────────────────────────────────────────────────

def run_cycle(state: OperatorState, dry_run: bool = False) -> dict[str, Any]:
    """Execute one operator cycle."""
    state.cycle_count += 1
    cycle_start = time.monotonic()
    cycle_result: dict[str, Any] = {
        "cycle": state.cycle_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": bool(dry_run),
    }

    log.info("═══ Hermes Operator — Cycle {n} ═══", n=state.cycle_count)

    # 0. Git pull — sync latest code/config
    if dry_run:
        cycle_result["git_pull"] = {"skipped": "dry_run"}
    else:
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
        # Audit #31 — fire alerts on threshold transitions. Best-effort,
        # cooldown-throttled (6h default), email via alerts.email.
        try:
            from alerts.health_alerter import check_and_alert
            fired = check_and_alert(health)
            if fired:
                log.warning("Health alerts fired: {f}", f=", ".join(fired))
                cycle_result["alerts_fired"] = fired
        except Exception as exc:
            log.debug("Health alerter skipped: {e}", e=str(exc))
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
    if dry_run:
        cycle_result["issues_table"] = {"skipped": "dry_run"}
    else:
        try:
            _ensure_issues_table(engine)
        except Exception as exc:
            log.debug("Hermes: issues table ensure failed: {e}", e=str(exc))

    state.consecutive_failures = 0

    # 1b. Obsidian vault sync + agent cycle (every cycle, fast ~5 min cadence)
    if not dry_run:
        try:
            state.current_step = "obsidian_cycle"
            obsidian_result = _run_obsidian_cycle(engine)
            cycle_result["obsidian"] = obsidian_result
        except Exception as exc:
            log.warning("Obsidian cycle failed: {e}", e=str(exc))

    # 2. Fix broken pulls (with cooldown + smart retry)
    try:
        state.current_step = "diagnose_and_fix_pulls"
        pull_result, ok = _run_with_timeout(
            "diagnose_and_fix_pulls",
            lambda: diagnose_and_fix_pulls(
                engine,
                hermes_ok,
                state,
                dry_run=dry_run,
            ),
            DIAGNOSE_PULLS_TIMEOUT_SECONDS,
            state,
        )
        if ok and pull_result:
            cycle_result["pull_fixer"] = pull_result
            state.pulls_retried += pull_result.get("retried", 0)
            state.fixes_applied += pull_result.get("fixed", 0)
            state.errors_diagnosed += pull_result.get("diagnosed", 0)
        else:
            cycle_result["pull_fixer"] = {"timeout": True}
    except Exception as exc:
        log.error("Pull fixer failed: {e}", e=str(exc))
        cycle_result["pull_fixer"] = {"error": str(exc)}

    # 2b. Proactively re-pull stale sources (not just failed ones)
    stale_sources = health["db"].get("stale_sources", [])
    if stale_sources and not dry_run:
        stale_repulled = 0
        for stale in stale_sources[:15]:  # up to 15 per cycle
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
    if dry_run:
        cycle_result["ingestion"] = {"skipped": "dry_run"}
        log.info("[DRY RUN] Would run smart ingestion")
    else:
        try:
            state.current_step = "smart_ingestion"
            from ingestion.smart_scheduler import SmartScheduler
            if not hasattr(state, "_smart_sched") or state._smart_sched is None:
                state._smart_sched = SmartScheduler(engine)
            tick_result, ok = _run_with_timeout(
                "smart_ingestion",
                state._smart_sched.tick,
                SMART_INGESTION_TIMEOUT_SECONDS,
                state,
            )
            if ok and tick_result:
                cycle_result["ingestion"] = tick_result
                log.info(
                    "Smart ingestion: {ok}/{ran} succeeded, {due} still due",
                    ok=tick_result["succeeded"], ran=tick_result["ran"],
                    due=len(tick_result.get("still_due", [])),
                )
            else:
                cycle_result["ingestion"] = {"timeout": True}
        except Exception as exc:
            log.error("Smart ingestion failed: {e}", e=str(exc))
            cycle_result["ingestion"] = {"error": str(exc)}

    # 3b. Fast SQL resolution (skip slow Python resolver — use INSERT SELECT)
    if dry_run:
        cycle_result["resolution"] = {"skipped": "dry_run"}
        log.info("[DRY RUN] Would run fast resolution")
    else:
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
        state.current_step = "daily_digest"
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
            state.current_step = "hundredx_digest"
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

    # 7c-ii. Solana top-volume universe snapshot (every 4 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_universe = 999
        last_universe = getattr(state, "last_solana_universe", None)
        if last_universe is not None:
            hours_since_universe = (now - last_universe).total_seconds() / 3600
        if hours_since_universe >= 4:
            log.info("Running Solana top-volume universe snapshot...")
            if not dry_run:
                from config import settings as _settings
                from ingestion.solana.top_volume import (
                    JupiterDexScreenerProvider,
                    TopVolumeIngestor,
                )
                from trading.solana import (
                    DeployerRegistry,
                    HeliusClient,
                    SafetyConfig,
                    SolanaSafetyChecker,
                    parse_mint_blocklist,
                )

                helius = HeliusClient(
                    api_key=getattr(_settings, "HELIUS_API_KEY", "") or None
                )
                deployer_registry = DeployerRegistry(engine=engine, provider=helius)
                safety_config = SafetyConfig(
                    blocked_mints=parse_mint_blocklist(
                        getattr(_settings, "SOLANA_MINT_BLOCKLIST", "") or ""
                    ),
                )
                safety = SolanaSafetyChecker(config=safety_config)

                provider = JupiterDexScreenerProvider(
                    jupiter_tokens_url=_settings.SOLANA_UNIVERSE_JUPITER_URL,
                    batch_size=_settings.SOLANA_UNIVERSE_DEX_BATCH,
                )
                try:
                    ingestor = TopVolumeIngestor(
                        engine=engine,
                        provider=provider,
                        safety=safety,
                        deploy_provider=helius,
                        deployer_registry=deployer_registry,
                        limit=_settings.SOLANA_UNIVERSE_LIMIT,
                        enrich_on_insert=_settings.SOLANA_UNIVERSE_ENRICH_ON_INSERT,
                    )
                    universe_summary = ingestor.ingest_once()
                    cycle_result["solana_universe"] = universe_summary.to_dict()
                    log.info(
                        "Solana universe: {n} tokens, {e} enriched, "
                        "{er} errors",
                        n=universe_summary.tokens_written,
                        e=universe_summary.new_mints_enriched,
                        er=universe_summary.enrichment_errors,
                    )
                finally:
                    provider.close()
                    helius.close()
                state.last_solana_universe = now
            else:
                log.info("[DRY RUN] Would run Solana universe snapshot")
    except Exception as exc:
        log.warning("Solana universe snapshot failed: {e}", e=str(exc))

    # 7c-iii. Supply Chain Pulse watchdog (every 6 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_scp = 999
        last_scp = getattr(state, "last_supply_chain_pulse", None)
        if last_scp is not None:
            hours_since_scp = (now - last_scp).total_seconds() / 3600
        if hours_since_scp >= 6:
            state.current_step = "supply_chain_pulse"
            log.info("Running Supply Chain Pulse watchdog...")
            if not dry_run:
                from alerts.supply_chain_alerts import run_all as run_supply_chain_alerts
                scp_result = run_supply_chain_alerts(
                    engine, since_hours=24, send_email=True
                )
                cycle_result["supply_chain_pulse"] = {
                    "total": scp_result.get("total", 0),
                    "sent": scp_result.get("sent", False),
                    "snapshots": scp_result.get("snapshots_written", 0),
                    "counts": {
                        k: len(v)
                        for k, v in scp_result.get("findings", {}).items()
                    },
                }
                setattr(state, "last_supply_chain_pulse", now)
            else:
                log.info("[DRY RUN] Would run Supply Chain Pulse")
    except Exception as exc:
        log.warning("Supply Chain Pulse failed: {e}", e=str(exc))

    # 7c-iv. News contagion listener (every 15 minutes)
    #
    # Scans news_articles for shock-worthy events (bankruptcies, halts,
    # recalls, sanctions, commodity spikes) and auto-fires chain_contagion
    # simulations, persisting results with source='news_listener' and a
    # trigger_news_id back-pointer to the article that fired the shock.
    try:
        now = datetime.now(timezone.utc)
        last_ncl = getattr(state, "last_news_contagion", None)
        minutes_since_ncl = 9999.0
        if last_ncl is not None:
            minutes_since_ncl = (now - last_ncl).total_seconds() / 60
        if minutes_since_ncl >= 15:
            state.current_step = "news_contagion_listener"
            log.info("Running news_contagion_listener...")
            if not dry_run:
                from intelligence.news_contagion_listener import run_once as ncl_run
                ncl_result = ncl_run(
                    engine, since_hours=1, dry_run=False, limit=500
                )
                cycle_result["news_contagion_listener"] = {
                    "scanned": ncl_result.get("scanned_articles", 0),
                    "resolved": ncl_result.get("resolved", 0),
                    "fired": ncl_result.get("fired", 0),
                    "skipped_duplicate": ncl_result.get("skipped_duplicate", 0),
                    "errors": ncl_result.get("errors", 0),
                }
                setattr(state, "last_news_contagion", now)
                log.info(
                    "news_contagion: scanned={s} fired={f} dup={d}",
                    s=ncl_result.get("scanned_articles", 0),
                    f=ncl_result.get("fired", 0),
                    d=ncl_result.get("skipped_duplicate", 0),
                )
            else:
                log.info("[DRY RUN] Would run news_contagion_listener")
    except Exception as exc:
        log.warning("news_contagion_listener failed: {e}", e=str(exc))

    # 7d. Oracle prediction cycle (every 6 hours)
    try:
        now = datetime.now(timezone.utc)
        hours_since_oracle = 999
        if state.last_oracle_cycle is not None:
            hours_since_oracle = (now - state.last_oracle_cycle).total_seconds() / 3600
        if hours_since_oracle >= 6 and state.cooldowns.can_retry("oracle_cycle"):
            state.current_step = "oracle_cycle"
            log.info("Running Oracle prediction cycle...")
            # Record cycle start eagerly: even if the inner timeout fires
            # the orphan thread keeps running and writes predictions to DB.
            # We must NOT refire oracle for another 6h regardless. (2026-05-09)
            state.last_oracle_cycle = now
            if not dry_run:
                from oracle.engine import OracleEngine
                from oracle.report import send_oracle_report

                def _oracle_call():
                    oracle = OracleEngine(db_engine=engine)
                    return oracle.run_cycle()

                oracle_result, ok = _run_with_timeout(
                    "oracle_cycle", _oracle_call,
                    ORACLE_CYCLE_TIMEOUT_SECONDS, state,
                )
                if ok and oracle_result:
                    cycle_result["oracle"] = {
                        "predictions": oracle_result["new_predictions"],
                        "scoring": oracle_result["scoring"],
                        "leaderboard": oracle_result.get("leaderboard", [])[:3],
                    }
                    if oracle_result["new_predictions"] > 0:
                        send_oracle_report(oracle_result)
                    state.last_oracle_cycle = now
                    state.cooldowns.record_attempt("oracle_cycle", success=True)
            else:
                log.info("[DRY RUN] Would run Oracle cycle")
        elif hours_since_oracle >= 6:
            log.info(
                "Skipping oracle_cycle — blacklisted (timed out previously, "
                "blacklist clears in {h}h)",
                h=TIMEOUT_BLACKLIST_HOURS,
            )
    except Exception as exc:
        log.warning("Oracle cycle failed: {e}", e=str(exc))
        state.cooldowns.record_attempt("oracle_cycle", success=False, error=str(exc))

    # 7d-ii. TimesFM forecast cycle (every 6 hours, alongside oracle)
    try:
        now = datetime.now(timezone.utc)
        hours_since_timesfm = 999
        last_timesfm = getattr(state, "last_timesfm_cycle", None)
        if last_timesfm is not None:
            hours_since_timesfm = (now - last_timesfm).total_seconds() / 3600
        if hours_since_timesfm >= 6:
            state.current_step = "timesfm_cycle"
            log.info("Running TimesFM forecast cycle...")
            if not dry_run:
                from oracle.forecaster_adapter import run_timesfm_forecast_cycle
                # Wrap with the cycle's stage-timeout pattern. Without
                # this, run_timesfm_forecast_cycle could hang the entire
                # Hermes cycle past CYCLE_TIMEOUT_SECONDS — which is
                # exactly what cycle 292 logged (`TIMED OUT after 600s
                # stuck on: timesfm_cycle`). The function takes (engine,)
                # — _run_with_timeout calls fn() so wrap as a thunk.
                tfm_result, ok = _run_with_timeout(
                    "timesfm_cycle",
                    lambda: run_timesfm_forecast_cycle(engine),
                    TIMESFM_TIMEOUT_SECONDS,
                    state,
                )
                if ok and tfm_result:
                    cycle_result["timesfm"] = tfm_result
                    state.last_timesfm_cycle = now
                    log.info(
                        "TimesFM: {n} forecasts generated",
                        n=tfm_result.get("forecasts", 0),
                    )
                else:
                    cycle_result["timesfm"] = {"timeout": True}
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
            state.current_step = "changepoint_detection"
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
        if not state.cooldowns.can_retry("signal_classification"):
            log.debug(
                "Skipping signal_classification — blacklisted (timed out previously, "
                "blacklist clears in {h}h)",
                h=TIMEOUT_BLACKLIST_HOURS,
            )
        else:
            state.current_step = "signal_classification"
            if not dry_run:
                from ingestion.signal_classifier import classify_recent_signals

                def _classify_call():
                    return classify_recent_signals(engine, limit=30)

                cls_result, ok = _run_with_timeout(
                    "signal_classification", _classify_call,
                    SIGNAL_CLASSIFICATION_TIMEOUT_SECONDS, state,
                )
                if ok and cls_result and cls_result.get("classified", 0) > 0:
                    cycle_result["signal_classification"] = cls_result
                    log.info(
                        "Signal classification: {n} signals classified",
                        n=cls_result["classified"],
                    )
                if ok:
                    state.cooldowns.record_attempt("signal_classification", success=True)
    except Exception as exc:
        log.debug("Signal classification skipped: {e}", e=str(exc))
        state.cooldowns.record_attempt("signal_classification", success=False, error=str(exc))

    # 7d-v. Gemma micro anomaly narration (every cycle, after classification)
    # Reads recent high-z signals from signal_registry, asks the
    # anomaly_narrator (port 8083) for a one-line plain-English summary,
    # persists into anomaly_narratives. Idempotent: UNIQUE constraint on
    # (source_module, ticker, signal_ts) means re-runs are no-ops.
    try:
        if not state.cooldowns.can_retry("anomaly_narration"):
            log.debug(
                "Skipping anomaly_narration — blacklisted (timed out previously, "
                "blacklist clears in {h}h)",
                h=TIMEOUT_BLACKLIST_HOURS,
            )
        else:
            state.current_step = "anomaly_narration"
            if not dry_run:
                from ingestion.signal_classifier import narrate_anomalies

                def _narrate_call():
                    return narrate_anomalies(engine, z_threshold=3.0, limit=20)

                narratives, ok = _run_with_timeout(
                    "anomaly_narration", _narrate_call,
                    ANOMALY_NARRATION_TIMEOUT_SECONDS, state,
                )
                if ok and narratives:
                    inserted = 0
                    for n in narratives:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO anomaly_narratives
                                        (ticker, source_module, z_score,
                                         narrative, signal_ts)
                                    VALUES (:ticker, :src, :z, :narr, :ts)
                                    ON CONFLICT (source_module, ticker, signal_ts)
                                    DO NOTHING
                                """).bindparams(
                                    ticker=n.get("ticker"),
                                    src=n["source"],
                                    z=n["z_score"],
                                    narr=n["narrative"],
                                    ts=n.get("timestamp"),
                                ))
                                inserted += 1
                        except Exception as exc:
                            log.debug(
                                "Failed to persist narrative: {e}",
                                e=str(exc),
                            )
                    if inserted:
                        cycle_result["anomaly_narration"] = {
                            "narratives_generated": len(narratives),
                            "persisted": inserted,
                        }
                        log.info(
                            "Anomaly narration: {n} narratives persisted",
                            n=inserted,
                        )
                if ok:
                    state.cooldowns.record_attempt("anomaly_narration", success=True)
    except Exception as exc:
        log.debug("Anomaly narration skipped: {e}", e=str(exc))
        state.cooldowns.record_attempt("anomaly_narration", success=False, error=str(exc))

    # 7d-vi. Gemma micro knowledge mapping (every cycle, after classification)
    # Takes recently classified high-urgency signals and asks the
    # knowledge_mapper (port 8085) for a wiki-style entry with [[backlinks]].
    # Persists into signal_knowledge_entries. The helper itself flips
    # signal_registry.knowledge_mapped=TRUE so signals are processed once.
    try:
        if not state.cooldowns.can_retry("knowledge_mapping"):
            log.debug(
                "Skipping knowledge_mapping — blacklisted (timed out previously, "
                "blacklist clears in {h}h)",
                h=TIMEOUT_BLACKLIST_HOURS,
            )
        else:
            state.current_step = "knowledge_mapping"
            if not dry_run:
                from ingestion.signal_classifier import map_signal_knowledge

                def _map_call():
                    return map_signal_knowledge(
                        engine, urgency_filter="high", limit=10,
                    )

                entries, ok = _run_with_timeout(
                    "knowledge_mapping", _map_call,
                    KNOWLEDGE_MAP_TIMEOUT_SECONDS, state,
                )
                if ok and entries:
                    inserted = 0
                    for e in entries:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO signal_knowledge_entries
                                        (signal_id, ticker, category,
                                         knowledge_entry, signal_ts)
                                    VALUES (:sid, :ticker, :cat, :entry, :ts)
                                    ON CONFLICT (signal_id) DO NOTHING
                                """).bindparams(
                                    sid=e["signal_id"],
                                    ticker=e.get("ticker"),
                                    cat=e.get("category"),
                                    entry=e["knowledge_entry"],
                                    ts=e.get("timestamp"),
                                ))
                                inserted += 1
                        except Exception as exc:
                            log.debug(
                                "Failed to persist knowledge entry: {e}",
                                e=str(exc),
                            )
                    if inserted:
                        cycle_result["knowledge_mapping"] = {
                            "entries_generated": len(entries),
                            "persisted": inserted,
                        }
                        log.info(
                            "Knowledge mapping: {n} entries persisted",
                            n=inserted,
                        )
                if ok:
                    state.cooldowns.record_attempt("knowledge_mapping", success=True)
    except Exception as exc:
        log.debug("Knowledge mapping skipped: {e}", e=str(exc))
        state.cooldowns.record_attempt("knowledge_mapping", success=False, error=str(exc))

    # 7e. Alpha research heartbeat + signal publishing (every cycle)
    try:
        state.current_step = "alpha_heartbeat"
        from alpha_research.heartbeat import run_heartbeat, format_alerts

        hb_alerts = run_heartbeat(engine)
        if hb_alerts:
            log.info(format_alerts(hb_alerts))
        cycle_result["alpha_heartbeat"] = {
            "alerts": len(hb_alerts),
            "critical": sum(1 for a in hb_alerts if a.level == "CRITICAL"),
        }

        if not dry_run:
            from alpha_research.adapters.signal_adapter import publish_all_alpha_signals
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
        state.current_step = "intelligence_tasks"
        intel_result, ok = _run_with_timeout(
            "intelligence_tasks",
            lambda: run_intelligence_tasks(engine, state, dry_run=dry_run),
            INTELLIGENCE_TASKS_TIMEOUT_SECONDS,
            state,
        )
        if ok and intel_result:
            cycle_result["intelligence"] = intel_result
        elif not ok:
            cycle_result["intelligence"] = {"timeout": True}
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
    if dry_run:
        cycle_result["git_push"] = {"skipped": "dry_run"}
    else:
        try:
            state.current_step = "git_push"
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
    state.current_step = "save_cycle_snapshot"
    elapsed = time.monotonic() - cycle_start
    cycle_result["elapsed_seconds"] = round(elapsed, 1)
    cycle_result["operator_state"] = state.to_dict()
    if dry_run:
        cycle_result["snapshot"] = {"skipped": "dry_run"}
        cycle_result["obsidian_report"] = {"skipped": "dry_run"}
    else:
        save_cycle_snapshot(engine, cycle_result)
        cycle_result["snapshot"] = {"status": "attempted"}

        # Fan a heartbeat / event report into the fleet-wide Obsidian agent-hub.
        # Idempotent: skips on quiet cycles unless the hourly heartbeat is due.
        _emit_obsidian_cycle_report(state, cycle_result)
        cycle_result["obsidian_report"] = {"status": "attempted"}

    log.info(
        "═══ Cycle {n} complete — {t:.1f}s ═══",
        n=state.cycle_count, t=elapsed,
    )
    state.current_step = "idle"
    return cycle_result


# ─── Obsidian fan-out ────────────────────────────────────────────────

# How often Hermes emits a "everything's fine" heartbeat report to the
# agent hub. Default 12 cycles ≈ 60 minutes at the 5-min cycle interval.
# Override with HERMES_OBSIDIAN_REPORT_EVERY_N_CYCLES env var; set 0 to
# disable cadence-based heartbeats entirely (event-only reports remain on).
HERMES_OBSIDIAN_REPORT_EVERY_N_CYCLES = int(
    os.environ.get("HERMES_OBSIDIAN_REPORT_EVERY_N_CYCLES", "12") or "0"
)
HERMES_OBSIDIAN_REPORT_CMD = os.environ.get(
    "HERMES_OBSIDIAN_REPORT_CMD", "/usr/local/bin/agent-report"
)


def _cycle_did_something(cycle_result: dict[str, Any]) -> tuple[bool, list[str]]:
    """Returns (did_something, event_labels). Used to decide whether to
    file an event-driven Obsidian report on top of the cadence heartbeat."""
    events: list[str] = []
    health = cycle_result.get("health") or {}
    if health.get("overall_healthy") is False:
        events.append("health-degraded")
    pull_fixer = cycle_result.get("pull_fixer") or {}
    retried = int(pull_fixer.get("retried") or 0)
    if retried > 0:
        events.append(f"pulls-retried={retried}")
    if cycle_result.get("pipeline"):
        events.append("pipeline-ran")
    weekly = cycle_result.get("weekly_intelligence_reports")
    if weekly:
        events.append(f"weekly-reports={len(weekly)}")
    autoresearch = cycle_result.get("autoresearch") or {}
    hypotheses = autoresearch.get("hypotheses_generated") or 0
    if hypotheses:
        events.append(f"hypotheses={hypotheses}")
    return (bool(events), events)


def _build_obsidian_cycle_body(
    cycle_result: dict[str, Any], events: list[str]
) -> str:
    """Build a short Markdown body for the agent-hub report. Cycle context
    + health snapshot + any noteworthy events. Kept under ~30 lines so the
    Obsidian feed stays scannable."""
    health = cycle_result.get("health") or {}
    db = health.get("db") or {}
    hermes = health.get("hermes") or {}
    cycle_n = cycle_result.get("cycle", "?")
    elapsed = cycle_result.get("elapsed_seconds", "?")
    raw_count = db.get("raw_series_count")
    latest_pull = db.get("latest_pull")
    failed_24h = db.get("failed_pulls_24h")
    failed_1h = db.get("failed_pulls_1h")
    overall = health.get("overall_healthy")
    overall_str = "healthy" if overall else "degraded"

    lines = [
        f"# Hermes cycle {cycle_n} — {overall_str}",
        "",
        f"- elapsed: {elapsed}s",
        f"- db.healthy: {db.get('healthy')!r}",
        f"- hermes.healthy: {hermes.get('healthy')!r}",
    ]
    if raw_count is not None:
        lines.append(f"- raw_series rows: {raw_count:,}")
    if latest_pull:
        lines.append(f"- latest pull: {latest_pull}")
    if failed_1h is not None:
        lines.append(f"- failed pulls (1h / 24h): {failed_1h} / {failed_24h}")
    if events:
        lines.append("")
        lines.append("## Events this cycle")
        for ev in events:
            lines.append(f"- {ev}")
    return "\n".join(lines) + "\n"


def _emit_obsidian_cycle_report(state: Any, cycle_result: dict[str, Any]) -> None:
    """Fan a hermes-cycle report into the agent hub. Fail-soft: any error
    is logged and swallowed so a broken hub never breaks the operator loop."""
    try:
        cycle_n = int(cycle_result.get("cycle") or state.cycle_count)
        did_something, events = _cycle_did_something(cycle_result)

        cadence = HERMES_OBSIDIAN_REPORT_EVERY_N_CYCLES
        cadence_due = (cadence > 0 and cycle_n % cadence == 0)

        if not did_something and not cadence_due:
            return  # quiet cycle, skip to keep the feed scannable

        if not os.path.exists(HERMES_OBSIDIAN_REPORT_CMD):
            log.debug(
                "obsidian-report wrapper missing at {p}; skipping fan-out",
                p=HERMES_OBSIDIAN_REPORT_CMD,
            )
            return

        body = _build_obsidian_cycle_body(cycle_result, events)
        # Slug: short, sortable, low-collision. Hub dedups on
        # (date, agent, host, slug).
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ")
        slug = f"hermes-cycle-{cycle_n}-{timestamp}"

        # Write body to /tmp and invoke agent-report. Cap stdout / stderr
        # at 2KB each so any noisy wrapper output doesn't fill the log.
        body_path = f"/tmp/hermes-{slug}.md"
        with open(body_path, "w", encoding="utf-8") as fh:
            fh.write(body)

        rc = subprocess.run(
            [HERMES_OBSIDIAN_REPORT_CMD, "hermes-operator", slug, body_path],
            capture_output=True, text=True, timeout=20,
        )
        if rc.returncode == 0:
            log.info(
                "obsidian-report: filed hermes-cycle-{n} ({events})",
                n=cycle_n, events=",".join(events) or "heartbeat",
            )
        else:
            log.warning(
                "obsidian-report: agent-report rc={rc} stderr={se}",
                rc=rc.returncode, se=(rc.stderr or "")[:512],
            )
    except Exception as exc:
        log.warning("obsidian-report fan-out failed: {e}", e=str(exc))


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

    # Hydrate last_* timestamps from the most recent snapshot so a restart
    # doesn't re-fire schedules that already ran today. Silent on failure —
    # first boot (no snapshot yet) is a normal fresh-start.
    try:
        from db import get_engine as _get_engine_for_hydrate
        _hydrate_engine = _get_engine_for_hydrate()
        if state.hydrate_from_snapshot(_hydrate_engine):
            log.info(
                "Hermes state hydrated from snapshot "
                "(last_daily_intel={d}, last_autoresearch={a}, last_hypothesis_discovery={h})",
                d=state.last_daily_intel, a=state.last_autoresearch,
                h=state.last_hypothesis_discovery,
            )
        else:
            log.info("Hermes state: no prior snapshot found, fresh start")
    except Exception as exc:
        log.debug("Hermes state hydrate failed (starting fresh): {e}", e=str(exc))

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
        from db import get_engine as _get_engine_for_migrate
        from oracle.model_factory import migrate_default_models
        migrate_default_models(_get_engine_for_migrate())
    except Exception as exc:
        log.debug("migrate_default_models: {e}", e=str(exc))

    if opts.once:
        result = run_cycle(state, dry_run=opts.dry_run)
        print(json.dumps(result, default=str, indent=2))
        return

    # Continuous loop with per-cycle timeout
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
            # WARNING — the cycle timeout is a handled degrade: the stuck
            # step gets blacklisted and the next cycle starts fresh. Real
            # failures inside cycle steps log ERROR at the call site.
            log.warning(
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
