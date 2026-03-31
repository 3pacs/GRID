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
    "supply_chain":           {"mod": "ingestion.altdata.supply_chain",           "cls": "SupplyChainPuller"},

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


# ─── Issue tracker ────────────────────────────────────────────────────

def _ensure_issues_table(engine: Any) -> None:
    """Create the operator_issues table if it doesn't exist."""
    from sqlalchemy import text
    ddl = text("""
        CREATE TABLE IF NOT EXISTS operator_issues (
            id            BIGSERIAL PRIMARY KEY,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            category      TEXT NOT NULL,
            severity      TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
            source        TEXT,
            title         TEXT NOT NULL,
            detail        TEXT,
            stack_trace   TEXT,
            hermes_diagnosis TEXT,
            fix_applied   TEXT,
            fix_result    TEXT CHECK (fix_result IN ('SUCCESS', 'FAILED', 'PENDING', 'SKIPPED')),
            resolved_at   TIMESTAMPTZ,
            cycle_number  INTEGER
        )
    """)
    idx = text("""
        CREATE INDEX IF NOT EXISTS idx_operator_issues_created
            ON operator_issues (created_at DESC)
    """)
    idx_cat = text("""
        CREATE INDEX IF NOT EXISTS idx_operator_issues_category
            ON operator_issues (category, severity)
    """)
    try:
        with engine.begin() as conn:
            conn.execute(ddl)
            conn.execute(idx)
            conn.execute(idx_cat)
    except Exception as exc:
        log.warning("Could not ensure operator_issues table: {e}", e=str(exc))


def log_issue(
    engine: Any,
    category: str,
    severity: str,
    title: str,
    detail: str | None = None,
    stack_trace: str | None = None,
    hermes_diagnosis: str | None = None,
    fix_applied: str | None = None,
    fix_result: str | None = None,
    source: str | None = None,
    cycle_number: int | None = None,
) -> int | None:
    """Log an issue/bug/fix to the operator_issues table.

    Every problem Hermes encounters gets logged with full context:
    what broke, the stack trace, Hermes' diagnosis, what fix was attempted,
    and whether it worked. This creates a rich debugging history that
    can be exported and fed to a more capable model for deeper analysis.

    Returns:
        int: Issue row ID, or None on failure.
    """
    from sqlalchemy import text
    _ensure_issues_table(engine)
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO operator_issues "
                    "(category, severity, source, title, detail, stack_trace, "
                    " hermes_diagnosis, fix_applied, fix_result, cycle_number, "
                    " resolved_at) "
                    "VALUES (:cat, :sev, :src, :title, :detail, :st, "
                    " :diag, :fix, :result, :cycle, "
                    " CASE WHEN :result = 'SUCCESS' THEN NOW() END) "
                    "RETURNING id"
                ),
                {
                    "cat": category,
                    "sev": severity,
                    "src": source,
                    "title": title,
                    "detail": detail,
                    "st": stack_trace,
                    "diag": hermes_diagnosis,
                    "fix": fix_applied,
                    "result": fix_result,
                    "cycle": cycle_number,
                },
            ).fetchone()
        issue_id = row[0] if row else None
        log.info(
            "Issue #{id} logged — [{sev}] {title}",
            id=issue_id, sev=severity, title=title[:80],
        )
        return issue_id
    except Exception as exc:
        log.warning("Failed to log issue: {e}", e=str(exc))
        return None


def export_issues(engine: Any, days_back: int = 30) -> list[dict[str, Any]]:
    """Export recent issues for external model analysis.

    Returns a list of issue dicts that can be serialized to JSON and
    fed to a smarter model for root cause analysis.
    """
    from sqlalchemy import text
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, created_at, category, severity, source, title, "
                "       detail, stack_trace, hermes_diagnosis, fix_applied, "
                "       fix_result, resolved_at, cycle_number "
                "FROM operator_issues "
                "WHERE created_at > NOW() - :interval * INTERVAL '1 day' "
                "ORDER BY created_at DESC"
            ),
            {"interval": days_back},
        ).fetchall()
    return [
        {
            "id": r[0], "created_at": r[1].isoformat() if r[1] else None,
            "category": r[2], "severity": r[3], "source": r[4],
            "title": r[5], "detail": r[6], "stack_trace": r[7],
            "hermes_diagnosis": r[8], "fix_applied": r[9],
            "fix_result": r[10], "resolved_at": r[11].isoformat() if r[11] else None,
            "cycle_number": r[12],
        }
        for r in rows
    ]


# ─── State ───────────────────────────────────────────────────────────

class SourceCooldown:
    """Track per-source retry state to prevent retry spam."""

    def __init__(self) -> None:
        # source_name → {last_attempt, consecutive_fails, last_error}
        self._sources: dict[str, dict[str, Any]] = {}

    def can_retry(self, source: str) -> bool:
        """Check if enough time has passed since last retry for this source."""
        info = self._sources.get(source.lower())
        if info is None:
            return True
        last = info["last_attempt"]
        fails = info.get("consecutive_fails", 0)
        # Timeout blacklist: 24 hours if the source caused a cycle timeout
        if info.get("timeout_blacklisted"):
            bl_until = info.get("blacklisted_until")
            if bl_until and datetime.now(timezone.utc) < bl_until:
                return False
            # Blacklist expired — clear it
            info["timeout_blacklisted"] = False
        # After SOURCE_MAX_CONSECUTIVE_FAILS, extend cooldown to 6 hours
        cooldown_min = SOURCE_COOLDOWN_MINUTES if fails < SOURCE_MAX_CONSECUTIVE_FAILS else 360
        elapsed = (datetime.now(timezone.utc) - last).total_seconds() / 60
        return elapsed >= cooldown_min

    def record_attempt(self, source: str, success: bool, error: str | None = None) -> None:
        """Record a retry attempt result."""
        key = source.lower()
        info = self._sources.get(key, {"consecutive_fails": 0})
        info["last_attempt"] = datetime.now(timezone.utc)
        if success:
            info["consecutive_fails"] = 0
            info["last_error"] = None
            info["timeout_blacklisted"] = False
        else:
            info["consecutive_fails"] = info.get("consecutive_fails", 0) + 1
            info["last_error"] = error
        self._sources[key] = info

    def blacklist_for_timeout(self, source: str) -> None:
        """Blacklist a source for TIMEOUT_BLACKLIST_HOURS after it caused
        a cycle timeout. The source won't be retried until the blacklist
        expires. This prevents the same slow source from blocking every cycle."""
        key = source.lower()
        info = self._sources.get(key, {"consecutive_fails": 0})
        info["timeout_blacklisted"] = True
        info["blacklisted_until"] = (
            datetime.now(timezone.utc) + timedelta(hours=TIMEOUT_BLACKLIST_HOURS)
        )
        info["last_attempt"] = datetime.now(timezone.utc)
        self._sources[key] = info
        log.warning(
            "Source {s} blacklisted for {h}h after causing cycle timeout",
            s=source, h=TIMEOUT_BLACKLIST_HOURS,
        )

    def get_status(self, source: str) -> dict[str, Any] | None:
        return self._sources.get(source.lower())

    def skipped_sources(self) -> list[str]:
        """Return sources currently in cooldown."""
        return [s for s in self._sources if not self.can_retry(s)]

    def blacklisted_sources(self) -> list[dict[str, Any]]:
        """Return sources blacklisted due to timeout with expiry times."""
        result = []
        for s, info in self._sources.items():
            if info.get("timeout_blacklisted"):
                bl_until = info.get("blacklisted_until")
                result.append({
                    "source": s,
                    "blacklisted_until": bl_until.isoformat() if bl_until else None,
                    "last_error": info.get("last_error"),
                })
        return result


class OperatorState:
    """Mutable state persisted across cycles."""

    def __init__(self) -> None:
        self.last_pipeline_run: datetime | None = None
        self.last_autoresearch: datetime | None = None
        self.last_ux_audit: datetime | None = None
        self.last_daily_digest: datetime | None = None
        self.last_100x_digest: datetime | None = None
        self.last_oracle_cycle: datetime | None = None
        self.consecutive_failures: int = 0
        self.cycle_count: int = 0
        self.fixes_applied: int = 0
        self.pulls_retried: int = 0
        self.hypotheses_tested: int = 0
        self.errors_diagnosed: int = 0
        self.cooldowns: SourceCooldown = SourceCooldown()
        self.current_step: str | None = None  # tracks what's running for timeout blacklisting

        # Intelligence module tracking
        self.last_trust_cycle: datetime | None = None
        self.last_options_recommendations: datetime | None = None
        self.last_cross_reference_checks: datetime | None = None
        self.last_options_scoring: datetime | None = None
        self.last_lever_pullers: datetime | None = None
        self.last_actor_wealth: datetime | None = None
        self.last_daily_intel: datetime | None = None      # 2:00 AM daily batch
        self.last_weekly_intel: datetime | None = None      # Sunday 3:00 AM weekly batch
        self.last_signal_registry: datetime | None = None   # Every 2 hours

        # Hermes status log: task_name -> {last_run, success, duration_s, error}
        self.task_status: dict[str, dict[str, Any]] = {}

    def record_task(
        self,
        task_name: str,
        success: bool,
        duration_s: float,
        error: str | None = None,
    ) -> None:
        """Record the outcome of a scheduled task for the status endpoint."""
        self.task_status[task_name] = {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "success": success,
            "duration_s": round(duration_s, 2),
            "error": error,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_pipeline_run": self.last_pipeline_run.isoformat() if self.last_pipeline_run else None,
            "last_autoresearch": self.last_autoresearch.isoformat() if self.last_autoresearch else None,
            "consecutive_failures": self.consecutive_failures,
            "cycle_count": self.cycle_count,
            "fixes_applied": self.fixes_applied,
            "pulls_retried": self.pulls_retried,
            "hypotheses_tested": self.hypotheses_tested,
            "errors_diagnosed": self.errors_diagnosed,
            "sources_in_cooldown": self.cooldowns.skipped_sources(),
            "sources_blacklisted": self.cooldowns.blacklisted_sources(),
            "last_trust_cycle": self.last_trust_cycle.isoformat() if self.last_trust_cycle else None,
            "last_options_recommendations": self.last_options_recommendations.isoformat() if self.last_options_recommendations else None,
            "last_cross_reference_checks": self.last_cross_reference_checks.isoformat() if self.last_cross_reference_checks else None,
            "last_lever_pullers": self.last_lever_pullers.isoformat() if self.last_lever_pullers else None,
            "last_actor_wealth": self.last_actor_wealth.isoformat() if self.last_actor_wealth else None,
            "last_daily_intel": self.last_daily_intel.isoformat() if self.last_daily_intel else None,
            "last_weekly_intel": self.last_weekly_intel.isoformat() if self.last_weekly_intel else None,
            "last_signal_registry": self.last_signal_registry.isoformat() if self.last_signal_registry else None,
            "task_status": self.task_status,
        }


# ─── Health checks ───────────────────────────────────────────────────

def check_db_health(engine: Any) -> dict[str, Any]:
    """Check database connectivity and basic stats."""
    from sqlalchemy import text
    result: dict[str, Any] = {"healthy": False}
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            result["healthy"] = True

            # Raw series count
            row = conn.execute(text("SELECT COUNT(*) FROM raw_series")).fetchone()
            result["raw_series_count"] = row[0] if row else 0

            # Latest pull
            row = conn.execute(
                text("SELECT MAX(pull_timestamp) FROM raw_series WHERE pull_status = 'SUCCESS'")
            ).fetchone()
            result["latest_pull"] = row[0].isoformat() if row and row[0] else None

            # Failed pulls in last 24h
            row = conn.execute(
                text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE pull_status = 'FAILED' "
                    "AND pull_timestamp > NOW() - INTERVAL '24 hours'"
                )
            ).fetchone()
            result["failed_pulls_24h"] = row[0] if row else 0

            # Source freshness
            rows = conn.execute(
                text(
                    "SELECT sc.name, MAX(rs.pull_timestamp) AS last_pull "
                    "FROM source_catalog sc "
                    "LEFT JOIN raw_series rs ON rs.source_id = sc.id AND rs.pull_status = 'SUCCESS' "
                    "WHERE sc.active = TRUE "
                    "GROUP BY sc.name "
                    "ORDER BY last_pull ASC NULLS FIRST "
                    "LIMIT 20"
                )
            ).fetchall()
            stale: list[dict[str, Any]] = []
            cutoff = datetime.now(timezone.utc) - timedelta(hours=DATA_FRESHNESS_THRESHOLD_HOURS)
            for r in rows:
                if r[1] is None or r[1] < cutoff:
                    stale.append({
                        "source": r[0],
                        "last_pull": r[1].isoformat() if r[1] else "never",
                    })
            result["stale_sources"] = stale

    except Exception as exc:
        result["error"] = str(exc)
    return result


def check_hermes_health() -> dict[str, Any]:
    """Check if Hermes (llama.cpp) is responding."""
    try:
        from llamacpp.client import get_client
        client = get_client()
        hc = client.health_check()
        return {
            "healthy": hc.get("available", False),
            "latency_ms": hc.get("latency_ms"),
            "models": hc.get("models", []),
        }
    except Exception as exc:
        return {"healthy": False, "error": str(exc)}


def check_system_health(engine: Any) -> dict[str, Any]:
    """Full system health check."""
    db = check_db_health(engine)
    hermes = check_hermes_health()
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "db": db,
        "hermes": hermes,
        "overall_healthy": db["healthy"],  # hermes is optional
    }


# ─── Pull fixer ──────────────────────────────────────────────────────

def _resolve_puller(source_name: str, engine: Any) -> tuple[Any, str, dict[str, Any]]:
    """Resolve a source name to a puller instance using the registry.

    Returns:
        (puller_instance, pull_method_name, pull_kwargs)

    Raises:
        ValueError: If no handler found for the source.
    """
    import importlib

    source_lower = source_name.lower().replace(" ", "_").replace("-", "_")

    # Direct registry match
    entry = _SOURCE_REGISTRY.get(source_lower)

    # Fuzzy match: try prefix matching for names like "FRED_xxx"
    if entry is None:
        for key, val in _SOURCE_REGISTRY.items():
            if source_lower.startswith(key) or key.startswith(source_lower):
                entry = val
                break

    if entry is None:
        raise ValueError(f"No puller registered for source: {source_name}")

    mod = importlib.import_module(entry["mod"])
    cls = getattr(mod, entry["cls"])

    # Build constructor kwargs
    ctor_kwargs: dict[str, Any] = {"db_engine": engine}
    if "api_key" in entry:
        from config import settings
        ctor_kwargs["api_key"] = getattr(settings, entry["api_key"])

    puller = cls(**ctor_kwargs)
    method = entry.get("pull_method", "pull_all")
    kwargs = entry.get("pull_kwargs", {})

    return puller, method, kwargs


def _retry_source(source_name: str, engine: Any, attempt: int = 1) -> dict[str, Any]:
    """Retry a single source pull with strategy variation per attempt.

    Attempt 1: standard pull (recent data only)
    Attempt 2: pull with extended lookback
    Attempt 3: full historical backfill for last 7 days

    Returns:
        dict with pull result info.
    """
    log.info("Retrying {s} (attempt {a}/{m})", s=source_name, a=attempt, m=MAX_PULL_RETRIES)

    puller, method, kwargs = _resolve_puller(source_name, engine)

    # Vary strategy per attempt
    if attempt >= 2:
        # Extend lookback on retry — pull more historical data
        if "days_back" in kwargs:
            kwargs["days_back"] = kwargs["days_back"] * (attempt + 1)
        elif hasattr(puller, "pull_all"):
            # For pullers with start_date, go further back on retry
            from datetime import timedelta
            kwargs["start_date"] = (date.today() - timedelta(days=7 * attempt)).isoformat()

    pull_fn = getattr(puller, method)
    result = pull_fn(**kwargs)
    return result if isinstance(result, dict) else {"status": "ok"}


def diagnose_and_fix_pulls(
    engine: Any,
    hermes_available: bool,
    state: OperatorState,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Find failed/stale pulls, diagnose with Hermes, and actively fix them.

    Improvements over naive retry:
    - Per-source cooldown prevents retry spam
    - Hermes diagnosis is parsed into actionable fix categories
    - Retry strategy varies per attempt (standard → extended → backfill)
    - Failures are tracked with full context for pattern analysis
    """
    from sqlalchemy import text
    result: dict[str, Any] = {
        "retried": 0, "fixed": 0, "diagnosed": 0,
        "skipped_cooldown": 0, "skipped_no_handler": 0,
    }

    # Find sources with recent failures + their error details
    with engine.connect() as conn:
        failed = conn.execute(
            text(
                "SELECT sc.name, COUNT(*) AS fail_count, "
                "       MAX(rs.pull_timestamp) AS last_fail, "
                "       MAX(rs.raw_payload::text) AS last_error "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON rs.source_id = sc.id "
                "WHERE rs.pull_status = 'FAILED' "
                "AND rs.pull_timestamp > NOW() - INTERVAL '24 hours' "
                "GROUP BY sc.name "
                "ORDER BY fail_count DESC"
            )
        ).fetchall()

    if not failed:
        log.info("No failed pulls in last 24h")
        return result

    failed_info = [
        {"source": r[0], "fail_count": r[1],
         "last_fail": r[2].isoformat() if r[2] else None,
         "last_error": (r[3] or "")[:200]}
        for r in failed
    ]
    failed_sources = [f["source"] for f in failed_info]
    log.warning("Failed sources in last 24h: {s}", s=failed_sources)

    # If Hermes is available, get structured diagnosis with fix actions
    diagnosis_text: str | None = None
    fix_actions: dict[str, str] = {}  # source → recommended action

    if hermes_available:
        try:
            from llamacpp.client import get_client
            client = get_client()
            diagnosis_text = client.chat(
                messages=[
                    {"role": "system", "content": (
                        "You are GRID's operations agent. Diagnose data pull failures and "
                        "recommend specific actions. For EACH source, output one line:\n"
                        "SOURCE_NAME: ACTION — reason\n\n"
                        "ACTION must be one of:\n"
                        "- RETRY — transient error, just retry\n"
                        "- SKIP — known outage or maintenance, don't waste cycles\n"
                        "- BACKFILL — data gap, pull extended history\n"
                        "- CHECK_KEY — API key may be expired or rate-limited\n"
                        "- ESCALATE — needs human attention\n\n"
                        "Be specific and concise. No preamble."
                    )},
                    {"role": "user", "content": (
                        f"Failed sources with details:\n"
                        + json.dumps(failed_info, default=str, indent=2)
                        + f"\nCurrent date: {date.today()}"
                    )},
                ],
                temperature=HERMES_TEMPERATURE,
            )
            if diagnosis_text:
                log.info("Hermes diagnosis:\n{d}", d=diagnosis_text[:800])
                result["diagnosis"] = diagnosis_text
                result["diagnosed"] = len(failed_sources)

                # Parse structured actions from Hermes response
                for line in diagnosis_text.strip().split("\n"):
                    line = line.strip()
                    if ":" in line and "—" in line:
                        parts = line.split(":", 1)
                        src = parts[0].strip().lower().replace(" ", "_")
                        action = parts[1].split("—")[0].strip().upper()
                        if action in ("RETRY", "SKIP", "BACKFILL", "CHECK_KEY", "ESCALATE"):
                            fix_actions[src] = action

        except Exception as exc:
            log.warning("Hermes diagnosis failed: {e}", e=str(exc))

    if dry_run:
        log.info("Dry run — not retrying pulls")
        result["fix_actions"] = fix_actions
        return result

    # Retry each failed source with cooldown awareness and strategy variation
    for source_name in failed_sources:
        source_key = source_name.lower().replace(" ", "_")

        # Check Hermes recommendation
        action = fix_actions.get(source_key, "RETRY")
        if action == "SKIP":
            log.info("Hermes says SKIP {s} — known outage", s=source_name)
            result["skipped_cooldown"] += 1
            continue
        if action == "ESCALATE":
            log.warning("Hermes says ESCALATE {s} — needs human attention", s=source_name)
            log_issue(
                engine, category="ingestion", severity="CRITICAL",
                source=source_name,
                title=f"Hermes escalation — {source_name} needs human attention",
                hermes_diagnosis=diagnosis_text,
                fix_result="SKIPPED",
                cycle_number=state.cycle_count,
            )
            continue

        # Check cooldown
        if not state.cooldowns.can_retry(source_name):
            cooldown_info = state.cooldowns.get_status(source_name)
            fails = cooldown_info.get("consecutive_fails", 0) if cooldown_info else 0
            log.info(
                "Skipping {s} — in cooldown ({f} consecutive fails)",
                s=source_name, f=fails,
            )
            result["skipped_cooldown"] += 1
            continue

        # Determine attempt number from cooldown state
        cooldown_info = state.cooldowns.get_status(source_name)
        attempt = (cooldown_info.get("consecutive_fails", 0) + 1) if cooldown_info else 1
        attempt = min(attempt, MAX_PULL_RETRIES)

        try:
            # Use BACKFILL strategy if Hermes recommends it
            if action == "BACKFILL":
                attempt = MAX_PULL_RETRIES  # force extended lookback

            pull_result = _retry_source(source_name, engine, attempt=attempt)
            result["retried"] += 1
            result["fixed"] += 1
            state.cooldowns.record_attempt(source_name, success=True)
            log_issue(
                engine, category="ingestion", severity="INFO",
                source=source_name,
                title=f"Pull recovered — {source_name}",
                detail=f"Fixed on attempt {attempt} (strategy: {action})",
                hermes_diagnosis=diagnosis_text,
                fix_applied=f"Retried with strategy={action}, attempt={attempt}",
                fix_result="SUCCESS",
                cycle_number=state.cycle_count,
            )
        except ValueError as exc:
            # No handler registered for this source
            log.info("No handler for {s}: {e}", s=source_name, e=str(exc))
            result["skipped_no_handler"] += 1
        except Exception as exc:
            log.warning("Retry for {s} failed: {e}", s=source_name, e=str(exc))
            state.cooldowns.record_attempt(source_name, success=False, error=str(exc))
            result["retried"] += 1
            log_issue(
                engine, category="ingestion", severity="ERROR",
                source=source_name,
                title=f"Pull retry failed — {source_name} (attempt {attempt})",
                detail=str(exc),
                stack_trace=traceback.format_exc(),
                hermes_diagnosis=diagnosis_text,
                fix_applied=f"Retried with strategy={action}, attempt={attempt}",
                fix_result="FAILED",
                cycle_number=state.cycle_count,
            )

    return result


# ─── Pipeline runner ─────────────────────────────────────────────────

def maybe_run_pipeline(state: OperatorState, dry_run: bool = False) -> dict[str, Any] | None:
    """Run the full pipeline if enough time has passed."""
    now = datetime.now(timezone.utc)
    if state.last_pipeline_run is not None:
        hours_since = (now - state.last_pipeline_run).total_seconds() / 3600
        if hours_since < PIPELINE_INTERVAL_HOURS:
            log.info(
                "Pipeline ran {h:.1f}h ago (threshold={t}h) — skipping",
                h=hours_since, t=PIPELINE_INTERVAL_HOURS,
            )
            return None

    if dry_run:
        log.info("Dry run — not running pipeline")
        return {"skipped": "dry_run"}

    log.info("Running full pipeline")
    try:
        from scripts.run_full_pipeline import run_pipeline
        summary = run_pipeline(historical=False)
        state.last_pipeline_run = now
        return summary
    except Exception as exc:
        log.error("Pipeline failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ─── Data gap filler ─────────────────────────────────────────────────

def fill_data_gaps(engine: Any, state: OperatorState, dry_run: bool = False) -> dict[str, Any]:
    """Find gaps in historical data and actively fill them by re-pulling sources.

    Strategy:
    1. Find features with sparse data or stale last observations
    2. Map each feature back to its source via source_catalog
    3. Re-pull the source with extended lookback to fill the gap
    """
    from sqlalchemy import text
    result: dict[str, Any] = {"gaps_found": 0, "gaps_filled": 0, "sources_repulled": []}

    try:
        with engine.connect() as conn:
            # Find features with gaps: sparse data OR stale (>7 days old)
            rows = conn.execute(
                text(
                    "SELECT fr.name, COUNT(rs.id) AS obs_count, "
                    "       MIN(rs.obs_date) AS first_obs, MAX(rs.obs_date) AS last_obs, "
                    "       sc.name AS source_name "
                    "FROM feature_registry fr "
                    "LEFT JOIN resolved_series rs ON rs.feature_id = fr.id "
                    "LEFT JOIN raw_series raw ON raw.series_id = fr.name "
                    "LEFT JOIN source_catalog sc ON raw.source_id = sc.id "
                    "WHERE fr.model_eligible = TRUE "
                    "GROUP BY fr.name, sc.name "
                    "HAVING COUNT(rs.id) < 100 OR MAX(rs.obs_date) < CURRENT_DATE - 7 "
                    "ORDER BY COUNT(rs.id) ASC "
                    "LIMIT 15"
                )
            ).fetchall()

        if not rows:
            log.info("No data gaps found")
            return result

        result["gaps_found"] = len(rows)
        sources_to_repull: dict[str, dict[str, Any]] = {}

        for r in rows:
            feature_name, obs_count, first_obs, last_obs, source_name = r
            log.info(
                "Data gap: {name} — {count} obs, range {first} to {last} (source: {src})",
                name=feature_name, count=obs_count,
                first=first_obs if first_obs else "none",
                last=last_obs if last_obs else "none",
                src=source_name or "unknown",
            )
            if source_name and source_name not in sources_to_repull:
                # Calculate how far back to pull based on the gap
                days_back = 90  # default
                if last_obs:
                    gap_days = (date.today() - last_obs).days
                    days_back = max(gap_days + 7, 30)  # at least 30 days
                sources_to_repull[source_name] = {"days_back": days_back, "features": []}
            if source_name:
                sources_to_repull[source_name]["features"].append(feature_name)

        if dry_run:
            log.info("Dry run — identified {n} sources to re-pull: {s}",
                     n=len(sources_to_repull), s=list(sources_to_repull.keys()))
            return result

        # Actually re-pull each source
        for source_name, info in sources_to_repull.items():
            # Respect cooldowns
            if not state.cooldowns.can_retry(source_name):
                log.info("Skipping gap-fill for {s} — in cooldown", s=source_name)
                continue

            try:
                log.info(
                    "Gap-filling {s} — {n} features, {d} days back",
                    s=source_name, n=len(info["features"]), d=info["days_back"],
                )
                _retry_source(source_name, engine, attempt=2)  # use extended strategy
                result["gaps_filled"] += len(info["features"])
                result["sources_repulled"].append(source_name)
                state.cooldowns.record_attempt(source_name, success=True)
                log.info("Gap-fill for {s} succeeded", s=source_name)
            except ValueError:
                log.info("No handler for gap-fill source: {s}", s=source_name)
            except Exception as exc:
                log.warning("Gap-fill for {s} failed: {e}", s=source_name, e=str(exc))
                state.cooldowns.record_attempt(source_name, success=False, error=str(exc))

    except Exception as exc:
        log.warning("Gap analysis failed: {e}", e=str(exc))

    return result


# ─── Self-diagnostics ────────────────────────────────────────────────

def run_self_diagnostics(
    engine: Any,
    hermes_available: bool,
    health: dict,
    state: OperatorState,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Have Hermes analyze the system state and actively execute fixes.

    Hermes outputs structured commands that get executed:
    - RUN_REGIME: re-run regime detection
    - RUN_FEATURES: recompute feature matrix
    - REPULL:<source>: re-pull a specific data source
    - RUN_PIPELINE: trigger full pipeline
    - VACUUM_DB: run VACUUM ANALYZE on key tables
    """
    if not hermes_available:
        return {"skipped": "hermes_unavailable"}

    result: dict[str, Any] = {"actions_taken": []}

    try:
        from llamacpp.client import get_client
        client = get_client()

        # Include recent issues in the report so Hermes has memory
        recent_issues: list[dict[str, Any]] = []
        try:
            recent_issues = export_issues(engine, days_back=1)[:10]
        except Exception:
            pass

        status_report = json.dumps({
            "date": date.today().isoformat(),
            "cycle": state.cycle_count,
            "db_healthy": health["db"]["healthy"],
            "stale_sources": health["db"].get("stale_sources", []),
            "failed_pulls_24h": health["db"].get("failed_pulls_24h", 0),
            "raw_series_count": health["db"].get("raw_series_count", 0),
            "latest_pull": health["db"].get("latest_pull"),
            "sources_in_cooldown": state.cooldowns.skipped_sources(),
            "recent_issues": [
                {"title": i["title"], "severity": i["severity"],
                 "fix_result": i["fix_result"], "created_at": i["created_at"]}
                for i in recent_issues
            ],
            "operator_stats": {
                "fixes_applied": state.fixes_applied,
                "pulls_retried": state.pulls_retried,
                "consecutive_failures": state.consecutive_failures,
            },
        }, default=str, indent=2)

        response = client.chat(
            messages=[
                {"role": "system", "content": (
                    "You are GRID's self-diagnostics agent. Analyze the system and output "
                    "STRUCTURED COMMANDS to fix issues. Output format — one command per line:\n\n"
                    "SEVERITY: OK|WARNING|CRITICAL\n"
                    "ACTION: <command>\n"
                    "ACTION: <command>\n"
                    "SUMMARY: <one line summary>\n\n"
                    "Available commands:\n"
                    "- RUN_REGIME — re-run regime detection\n"
                    "- RUN_FEATURES — recompute feature importance\n"
                    "- REPULL:<source_name> — re-pull a specific source\n"
                    "- RUN_PIPELINE — trigger full pipeline\n"
                    "- VACUUM_DB — run VACUUM ANALYZE\n"
                    "- NONE — system is healthy, no action needed\n\n"
                    "Max 5 actions. Be specific. No prose except in SUMMARY."
                )},
                {"role": "user", "content": f"System status:\n{status_report}"},
            ],
            temperature=0.2,
            system_knowledge=[
                "01_architecture", "02_data_sources", "03_pit_store",
                "04_conflict_resolution", "05_features", "06_clustering",
                "07_regime", "08_options", "09_journal", "10_governance",
                "11_autoresearch",
            ],
        )

        if not response:
            return {"skipped": "empty_response"}

        log.info("Hermes self-diagnostic:\n{r}", r=response[:600])
        result["assessment"] = response

        if dry_run:
            return result

        # Parse and execute structured commands
        for line in response.strip().split("\n"):
            line = line.strip()
            if not line.startswith("ACTION:"):
                continue
            cmd = line.split("ACTION:", 1)[1].strip()

            if cmd == "NONE":
                continue

            try:
                if cmd == "RUN_REGIME":
                    log.info("Hermes action: running regime detection")
                    from scripts.auto_regime import run
                    regime_result = run()
                    result["actions_taken"].append({
                        "cmd": cmd, "status": "ok",
                        "regime": regime_result.get("regime"),
                    })

                elif cmd == "RUN_FEATURES":
                    log.info("Hermes action: recomputing features")
                    from features.lab import recompute_importance
                    recompute_importance(engine)
                    result["actions_taken"].append({"cmd": cmd, "status": "ok"})

                elif cmd.startswith("REPULL:"):
                    source = cmd.split(":", 1)[1].strip()
                    if state.cooldowns.can_retry(source):
                        log.info("Hermes action: re-pulling {s}", s=source)
                        _retry_source(source, engine, attempt=1)
                        state.cooldowns.record_attempt(source, success=True)
                        result["actions_taken"].append({"cmd": cmd, "status": "ok"})
                    else:
                        log.info("Hermes wants REPULL:{s} but source in cooldown", s=source)
                        result["actions_taken"].append({"cmd": cmd, "status": "cooldown"})

                elif cmd == "RUN_PIPELINE":
                    log.info("Hermes action: triggering pipeline")
                    from scripts.run_full_pipeline import run_pipeline
                    run_pipeline(historical=False)
                    state.last_pipeline_run = datetime.now(timezone.utc)
                    result["actions_taken"].append({"cmd": cmd, "status": "ok"})

                elif cmd == "VACUUM_DB":
                    log.info("Hermes action: vacuuming database")
                    from sqlalchemy import text as sa_text
                    with engine.connect() as conn:
                        conn.execution_options(isolation_level="AUTOCOMMIT")
                        conn.execute(sa_text("VACUUM ANALYZE raw_series"))
                        conn.execute(sa_text("VACUUM ANALYZE resolved_series"))
                    result["actions_taken"].append({"cmd": cmd, "status": "ok"})

                elif cmd.startswith("FIX_DATA_QUALITY"):
                    # FIX_DATA_QUALITY or FIX_DATA_QUALITY:source_name
                    target = cmd.split(":", 1)[1].strip() if ":" in cmd else None
                    log.info("Hermes action: data quality check{t}", t=f" for {target}" if target else "")
                    from sqlalchemy import text as sa_text
                    dq_issues = []
                    with engine.connect() as conn:
                        # Check for NaN/null values in recent resolved_series
                        q_nulls = (
                            "SELECT fr.name, COUNT(*) AS null_count "
                            "FROM resolved_series rs "
                            "JOIN feature_registry fr ON fr.id = rs.feature_id "
                            "WHERE rs.obs_date >= CURRENT_DATE - INTERVAL '7 days' "
                            "AND rs.value IS NULL "
                        )
                        if target:
                            q_nulls += "AND fr.family = :target "
                        q_nulls += "GROUP BY fr.name HAVING COUNT(*) > 0 ORDER BY null_count DESC LIMIT 20"
                        params = {"target": target} if target else {}
                        null_rows = conn.execute(sa_text(q_nulls), params).fetchall()
                        for r in null_rows:
                            dq_issues.append({"type": "null_values", "feature": r[0], "count": r[1]})

                        # Check for duplicate timestamps
                        q_dupes = (
                            "SELECT fr.name, rs.obs_date, COUNT(*) AS n "
                            "FROM resolved_series rs "
                            "JOIN feature_registry fr ON fr.id = rs.feature_id "
                            "WHERE rs.obs_date >= CURRENT_DATE - INTERVAL '7 days' "
                        )
                        if target:
                            q_dupes += "AND fr.family = :target "
                        q_dupes += "GROUP BY fr.name, rs.obs_date HAVING COUNT(*) > 1 LIMIT 20"
                        dupe_rows = conn.execute(sa_text(q_dupes), params).fetchall()
                        for r in dupe_rows:
                            dq_issues.append({"type": "duplicate", "feature": r[0], "date": str(r[1]), "count": r[2]})

                        # Check for extreme outliers (|z| > 10)
                        q_outliers = (
                            "SELECT fr.name, rs.value, rs.obs_date "
                            "FROM resolved_series rs "
                            "JOIN feature_registry fr ON fr.id = rs.feature_id "
                            "WHERE rs.obs_date >= CURRENT_DATE - INTERVAL '7 days' "
                            "AND rs.value IS NOT NULL "
                            "AND ABS(rs.value) > 1e15 "
                        )
                        if target:
                            q_outliers += "AND fr.family = :target "
                        q_outliers += "LIMIT 20"
                        outlier_rows = conn.execute(sa_text(q_outliers), params).fetchall()
                        for r in outlier_rows:
                            dq_issues.append({"type": "outlier", "feature": r[0], "value": float(r[1]), "date": str(r[2])})

                        # Auto-fix: remove exact duplicates (keep latest)
                        fixes = 0
                        if dupe_rows:
                            for r in dupe_rows:
                                try:
                                    conn.execute(sa_text(
                                        "DELETE FROM resolved_series WHERE ctid NOT IN ("
                                        "  SELECT MIN(ctid) FROM resolved_series rs "
                                        "  JOIN feature_registry fr ON fr.id = rs.feature_id "
                                        "  WHERE fr.name = :fname AND rs.obs_date = :odate "
                                        "  GROUP BY rs.feature_id, rs.obs_date"
                                        ") AND feature_id = (SELECT id FROM feature_registry WHERE name = :fname) "
                                        "AND obs_date = :odate"
                                    ), {"fname": r[0], "odate": r[1]})
                                    fixes += 1
                                except Exception:
                                    pass
                            conn.commit()

                    # Log issues as operator_issues
                    severity = "WARNING" if len(dq_issues) < 5 else "ERROR" if len(dq_issues) < 20 else "CRITICAL"
                    if dq_issues:
                        log_issue(
                            engine,
                            category="system", severity=severity,
                            source=target or "all",
                            title=f"Data quality: {len(dq_issues)} issues found",
                            detail=str(dq_issues[:10]),
                            fix_applied="dedup" if fixes > 0 else None,
                            fix_result="SUCCESS" if fixes > 0 else None,
                            cycle_number=getattr(state, 'cycle_count', None),
                        )

                    result["actions_taken"].append({
                        "cmd": cmd, "status": "ok",
                        "issues_found": len(dq_issues),
                        "duplicates_fixed": fixes,
                    })

                else:
                    log.warning("Unknown Hermes command: {c}", c=cmd)

            except Exception as exc:
                log.warning("Hermes action {c} failed: {e}", c=cmd, e=str(exc))
                result["actions_taken"].append({
                    "cmd": cmd, "status": "failed", "error": str(exc),
                })

    except Exception as exc:
        log.warning("Self-diagnostics failed: {e}", e=str(exc))
        return {"skipped": "error", "error": str(exc)}

    return result


# ─── Autoresearch trigger ────────────────────────────────────────────

def maybe_run_autoresearch(state: OperatorState, dry_run: bool = False) -> dict[str, Any] | None:
    """Run autoresearch if system is healthy and enough time has passed."""
    now = datetime.now(timezone.utc)

    # Only run autoresearch every 12 hours
    if state.last_autoresearch is not None:
        hours_since = (now - state.last_autoresearch).total_seconds() / 3600
        if hours_since < 12:
            return None

    if dry_run:
        return {"skipped": "dry_run"}

    log.info("Running autoresearch cycle")
    try:
        from scripts.autoresearch import run_autoresearch
        result = run_autoresearch(max_iterations=AUTORESEARCH_MAX_ITER)
        state.last_autoresearch = now
        state.hypotheses_tested += result.get("iterations", 0)
        return result
    except Exception as exc:
        log.warning("Autoresearch failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ─── Snapshot persistence ────────────────────────────────────────────

def save_cycle_snapshot(engine: Any, cycle_result: dict[str, Any]) -> None:
    """Save the operator cycle result as an analytical snapshot."""
    try:
        from store.snapshots import AnalyticalSnapshotStore
        snap = AnalyticalSnapshotStore(db_engine=engine)
        snap.save_snapshot(
            category="pipeline_summary",
            subcategory="hermes_operator",
            payload=cycle_result,
            metrics={
                "overall_healthy": cycle_result.get("health", {}).get("overall_healthy"),
                "pulls_retried": cycle_result.get("pull_fixer", {}).get("retried", 0),
                "pipeline_ran": cycle_result.get("pipeline") is not None,
                "cycle": cycle_result.get("cycle"),
            },
        )
    except Exception as exc:
        log.warning("Failed to save cycle snapshot: {e}", e=str(exc))


# ─── Intelligence task runner ────────────────────────────────────────

def _run_intel_task(
    name: str,
    fn: Any,
    state: OperatorState,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run an intelligence task with timing, logging, and error isolation.

    Every task is wrapped so that a single failure never kills the loop.
    Timing and success/failure are recorded in state.task_status for the
    hermes-status API endpoint.

    Returns:
        The task result, or None on failure.
    """
    t0 = time.monotonic()
    try:
        result = fn(*args, **kwargs)
        elapsed = time.monotonic() - t0
        state.record_task(name, success=True, duration_s=elapsed)
        log.info(
            "Intel task '{n}' completed in {t:.1f}s",
            n=name, t=elapsed,
        )
        return result
    except Exception as exc:
        elapsed = time.monotonic() - t0
        state.record_task(name, success=False, duration_s=elapsed, error=str(exc))
        log.warning(
            "Intel task '{n}' failed after {t:.1f}s: {e}",
            n=name, t=elapsed, e=str(exc),
        )
        return None


def _hours_since(ts: datetime | None) -> float:
    """Return hours elapsed since *ts*, or 999 if ts is None."""
    if ts is None:
        return 999.0
    return (datetime.now(timezone.utc) - ts).total_seconds() / 3600


def _refresh_signal_registry(engine: Any) -> None:
    """Refresh all signal adapters and prune expired signals."""
    try:
        from intelligence.adapters import ALL_ADAPTERS
        from intelligence.adapters.base import AdapterRegistry
        from intelligence.signal_registry import SignalRegistry

        registry = AdapterRegistry([cls() for cls in ALL_ADAPTERS])
        results = registry.refresh_all(engine)
        SignalRegistry.prune_expired(engine, days_old=7)
        log.info("Signal registry refreshed: {r}", r=results)
    except Exception as exc:
        log.error("Signal registry refresh failed: {e}", e=str(exc))


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
    except Exception:
        pass

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

    # 7e. Intelligence modules — trust scoring, cross-reference, lever pullers,
    #     actor network, source audit, postmortem, options tracking, backtests
    try:
        intel_result = run_intelligence_tasks(engine, state, dry_run=dry_run)
        if intel_result:
            cycle_result["intelligence"] = intel_result
    except Exception as exc:
        log.warning("Intelligence tasks failed: {e}", e=str(exc))

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
    except Exception:
        pass

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
    except Exception:
        pass  # API may not be running in same process

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
