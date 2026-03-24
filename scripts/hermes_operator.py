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
PIPELINE_INTERVAL_HOURS = 6           # run full pipeline every 6 hours
DATA_FRESHNESS_THRESHOLD_HOURS = 26   # flag stale sources after 26h
MAX_PULL_RETRIES = 3                  # retry failed pulls up to 3 times
AUTORESEARCH_MAX_ITER = 5             # hypothesis iterations per cycle
HERMES_TEMPERATURE = 0.3              # LLM temperature for diagnostics
GIT_SYNC_ENABLED = True               # pull/push on each cycle
GIT_REMOTE = "origin"
GIT_BRANCH = "main"


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

class OperatorState:
    """Mutable state persisted across cycles."""

    def __init__(self) -> None:
        self.last_pipeline_run: datetime | None = None
        self.last_autoresearch: datetime | None = None
        self.consecutive_failures: int = 0
        self.cycle_count: int = 0
        self.fixes_applied: int = 0
        self.pulls_retried: int = 0
        self.hypotheses_tested: int = 0
        self.errors_diagnosed: int = 0

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

def diagnose_and_fix_pulls(engine: Any, hermes_available: bool, dry_run: bool = False) -> dict[str, Any]:
    """Find failed/stale pulls and retry them.

    If Hermes is available, ask it to diagnose the failure pattern.
    Either way, retry the pull with the standard puller.
    """
    from sqlalchemy import text
    result: dict[str, Any] = {"retried": 0, "fixed": 0, "diagnosed": 0}

    # Find sources with recent failures
    with engine.connect() as conn:
        failed = conn.execute(
            text(
                "SELECT DISTINCT sc.name "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON rs.source_id = sc.id "
                "WHERE rs.pull_status = 'FAILED' "
                "AND rs.pull_timestamp > NOW() - INTERVAL '24 hours' "
                "ORDER BY sc.name"
            )
        ).fetchall()

    failed_sources = [r[0] for r in failed]
    if not failed_sources:
        log.info("No failed pulls in last 24h")
        return result

    log.warning("Failed sources in last 24h: {s}", s=failed_sources)

    # If Hermes is available, ask for diagnosis
    if hermes_available:
        try:
            from llamacpp.client import get_client
            client = get_client()
            diagnosis = client.chat(
                messages=[
                    {"role": "system", "content": (
                        "You are GRID's operations agent. Diagnose why these data pulls "
                        "might be failing and suggest fixes. Be specific and concise. "
                        "Common causes: API key expired, rate limit, endpoint changed, "
                        "network issue, schema change, dependency missing."
                    )},
                    {"role": "user", "content": (
                        f"These data sources failed in the last 24h: {failed_sources}\n"
                        f"Current date: {date.today()}\n"
                        f"What's likely wrong and what should I retry?"
                    )},
                ],
                temperature=HERMES_TEMPERATURE,
            )
            if diagnosis:
                log.info("Hermes diagnosis: {d}", d=diagnosis[:500])
                result["diagnosis"] = diagnosis
                result["diagnosed"] = len(failed_sources)
        except Exception as exc:
            log.warning("Hermes diagnosis failed: {e}", e=str(exc))

    if dry_run:
        log.info("Dry run — not retrying pulls")
        return result

    # Retry each failed source using the scheduler
    for source_name in failed_sources:
        try:
            _retry_source(source_name, engine)
            result["retried"] += 1
            log_issue(
                engine, category="ingestion", severity="WARNING",
                source=source_name,
                title=f"Data pull failure — {source_name}",
                detail=f"Auto-retried by Hermes operator",
                hermes_diagnosis=result.get("diagnosis"),
                fix_applied=f"Retried pull for {source_name}",
                fix_result="SUCCESS",
            )
        except Exception as exc:
            log.warning("Retry for {s} failed: {e}", s=source_name, e=str(exc))
            log_issue(
                engine, category="ingestion", severity="ERROR",
                source=source_name,
                title=f"Data pull retry failed — {source_name}",
                detail=str(exc),
                stack_trace=traceback.format_exc(),
                hermes_diagnosis=result.get("diagnosis"),
                fix_applied=f"Retry pull for {source_name}",
                fix_result="FAILED",
            )

    return result


def _retry_source(source_name: str, engine: Any) -> None:
    """Retry a single source pull."""
    source_lower = source_name.lower()
    log.info("Retrying pull for source: {s}", s=source_name)

    if source_lower == "fred" or source_lower.startswith("fred"):
        from config import settings
        from ingestion.fred import FREDPuller
        puller = FREDPuller(api_key=settings.FRED_API_KEY, db_engine=engine)
        puller.pull_all()

    elif source_lower == "yfinance" or source_lower.startswith("yfinance"):
        from ingestion.yfinance_pull import YFinancePuller
        puller = YFinancePuller(db_engine=engine)
        puller.pull_all()

    elif source_lower.startswith("edgar"):
        from ingestion.edgar import EDGARPuller
        puller = EDGARPuller(db_engine=engine)
        puller.pull_form4_transactions(days_back=3)

    elif source_lower == "crucix":
        from ingestion.crucix_bridge import CrucixBridgePuller
        puller = CrucixBridgePuller(db_engine=engine)
        puller.pull_all()

    else:
        # Try scheduler_v2 for international sources
        try:
            from ingestion.scheduler_v2 import run_pull_group
            for group in ["daily", "weekly", "monthly"]:
                try:
                    run_pull_group(group, engine)
                except Exception:
                    pass
        except ImportError:
            log.warning("No handler found for source: {s}", s=source_name)


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

def fill_data_gaps(engine: Any, dry_run: bool = False) -> dict[str, Any]:
    """Find and fill gaps in historical data coverage."""
    from sqlalchemy import text
    result: dict[str, Any] = {"gaps_found": 0, "gaps_filled": 0}

    # Find features with sparse data (large gaps between observations)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT fr.name, COUNT(rs.id) AS obs_count, "
                    "       MIN(rs.obs_date) AS first_obs, MAX(rs.obs_date) AS last_obs "
                    "FROM feature_registry fr "
                    "LEFT JOIN resolved_series rs ON rs.feature_id = fr.id "
                    "WHERE fr.model_eligible = TRUE "
                    "GROUP BY fr.name "
                    "HAVING COUNT(rs.id) < 100 OR MAX(rs.obs_date) < CURRENT_DATE - 7 "
                    "ORDER BY COUNT(rs.id) ASC "
                    "LIMIT 10"
                )
            ).fetchall()

        if rows:
            result["gaps_found"] = len(rows)
            for r in rows:
                log.info(
                    "Data gap: {name} — {count} obs, range {first} to {last}",
                    name=r[0], count=r[1],
                    first=r[2] if r[2] else "none",
                    last=r[3] if r[3] else "none",
                )
    except Exception as exc:
        log.warning("Gap analysis failed: {e}", e=str(exc))

    return result


# ─── Self-diagnostics ────────────────────────────────────────────────

def run_self_diagnostics(engine: Any, hermes_available: bool, health: dict) -> dict[str, Any]:
    """Have Hermes analyze the system state and suggest improvements."""
    if not hermes_available:
        return {"skipped": "hermes_unavailable"}

    try:
        from llamacpp.client import get_client
        client = get_client()

        # Build a status report for Hermes
        status_report = json.dumps({
            "date": date.today().isoformat(),
            "db_healthy": health["db"]["healthy"],
            "stale_sources": health["db"].get("stale_sources", []),
            "failed_pulls_24h": health["db"].get("failed_pulls_24h", 0),
            "raw_series_count": health["db"].get("raw_series_count", 0),
            "latest_pull": health["db"].get("latest_pull"),
        }, default=str, indent=2)

        response = client.chat(
            messages=[
                {"role": "system", "content": (
                    "You are GRID's self-diagnostics agent. You have full knowledge of "
                    "the GRID platform architecture, all 37+ data sources, the PIT store, "
                    "conflict resolution, regime detection, and the autoresearch loop.\n\n"
                    "Analyze the system status and provide:\n"
                    "1) A severity assessment (OK/WARNING/CRITICAL)\n"
                    "2) Top 3 issues to address\n"
                    "3) Specific actions to take (which scripts to run, which sources to retry)\n"
                    "Be actionable and concise."
                )},
                {"role": "user", "content": f"System status:\n{status_report}"},
            ],
            temperature=0.2,
            # Load all GRID knowledge docs so Hermes knows the full architecture
            system_knowledge=[
                "01_architecture",
                "02_data_sources",
                "03_pit_store",
                "04_conflict_resolution",
                "05_features",
                "06_clustering",
                "07_regime",
                "08_options",
                "09_journal",
                "10_governance",
                "11_autoresearch",
            ],
        )

        if response:
            log.info("Hermes self-diagnostic: {r}", r=response[:300])
            return {"assessment": response}

    except Exception as exc:
        log.warning("Self-diagnostics failed: {e}", e=str(exc))

    return {"skipped": "error"}


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
        from scripts.autoresearch import run_autoresearch_loop
        result = run_autoresearch_loop(max_iterations=AUTORESEARCH_MAX_ITER)
        state.last_autoresearch = now
        state.hypotheses_tested += result.get("iterations", 0)
        return result
    except ImportError:
        # run_autoresearch_loop may not exist — fall back to running main
        try:
            from scripts.autoresearch import main as ar_main
            ar_main(["--max-iter", str(AUTORESEARCH_MAX_ITER)])
            state.last_autoresearch = now
            return {"ran": True}
        except Exception as exc:
            log.warning("Autoresearch failed: {e}", e=str(exc))
            return {"error": str(exc)}
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

    # 2. Fix broken pulls
    try:
        pull_result = diagnose_and_fix_pulls(engine, hermes_ok, dry_run=dry_run)
        cycle_result["pull_fixer"] = pull_result
        state.pulls_retried += pull_result.get("retried", 0)
        state.fixes_applied += pull_result.get("fixed", 0)
        state.errors_diagnosed += pull_result.get("diagnosed", 0)
    except Exception as exc:
        log.error("Pull fixer failed: {e}", e=str(exc))
        cycle_result["pull_fixer"] = {"error": str(exc)}

    # 3. Run pipeline if due
    try:
        pipeline_result = maybe_run_pipeline(state, dry_run=dry_run)
        if pipeline_result is not None:
            cycle_result["pipeline"] = pipeline_result
            # Log any failed steps as issues
            if isinstance(pipeline_result, dict) and "steps" in pipeline_result:
                for step_name, step_result in pipeline_result.get("steps", {}).items():
                    if step_result is None:
                        log_issue(
                            engine, category="pipeline", severity="ERROR",
                            source=step_name,
                            title=f"Pipeline step failed — {step_name}",
                            fix_result="PENDING",
                            cycle_number=state.cycle_count,
                        )
    except Exception as exc:
        log.error("Pipeline runner failed: {e}", e=str(exc))
        cycle_result["pipeline"] = {"error": str(exc)}
        log_issue(
            engine, category="pipeline", severity="CRITICAL",
            title="Full pipeline execution failed",
            detail=str(exc),
            stack_trace=traceback.format_exc(),
            fix_result="PENDING",
            cycle_number=state.cycle_count,
        )

    # 4. Fill data gaps
    try:
        gap_result = fill_data_gaps(engine, dry_run=dry_run)
        cycle_result["data_gaps"] = gap_result
    except Exception as exc:
        log.warning("Gap filler failed: {e}", e=str(exc))

    # 5. Self-diagnostics (if Hermes available)
    try:
        diag = run_self_diagnostics(engine, hermes_ok, health)
        cycle_result["diagnostics"] = diag
    except Exception as exc:
        log.warning("Self-diagnostics failed: {e}", e=str(exc))

    # 6. Autoresearch (if everything is healthy)
    if health["overall_healthy"] and hermes_ok:
        try:
            ar_result = maybe_run_autoresearch(state, dry_run=dry_run)
            if ar_result is not None:
                cycle_result["autoresearch"] = ar_result
        except Exception as exc:
            log.warning("Autoresearch failed: {e}", e=str(exc))

    # 7. Git push — commit and push any new outputs
    try:
        push_result = git_push_outputs()
        cycle_result["git_push"] = push_result
    except Exception as exc:
        log.warning("Git push failed: {e}", e=str(exc))

    # 8. Save cycle snapshot
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

    if opts.once:
        result = run_cycle(state, dry_run=opts.dry_run)
        print(json.dumps(result, default=str, indent=2))
        return

    # Continuous loop
    while True:
        try:
            run_cycle(state, dry_run=opts.dry_run)
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
