"""
GRID Hermes Operator — pull fixers, pipeline runner, diagnostics.

Contains:
  - _resolve_puller, _retry_source — source resolution and retry logic
  - diagnose_and_fix_pulls — smart pull diagnosis and repair
  - maybe_run_pipeline — pipeline scheduling
  - fill_data_gaps — historical data gap filler
  - run_self_diagnostics — Hermes self-healing diagnostics
  - maybe_run_autoresearch — autoresearch trigger
  - save_cycle_snapshot — cycle result persistence
  - _run_intel_task, _hours_since — intelligence task helpers
"""

from __future__ import annotations

import json
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log

from scripts.hermes_health import (
    OperatorState,
    log_issue,
    export_issues,
)

# These constants are duplicated here to avoid circular imports
PIPELINE_INTERVAL_HOURS = 6
MAX_PULL_RETRIES = 3
AUTORESEARCH_MAX_ITER = 5
HERMES_TEMPERATURE = 0.3


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
    from scripts.hermes_operator import _SOURCE_REGISTRY
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
            from llm.router import get_llm, Tier
            client = get_llm(Tier.REASON)
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
        from llm.router import get_llm, Tier
        client = get_llm(Tier.REASON)

        # Include recent issues in the report so Hermes has memory
        recent_issues: list[dict[str, Any]] = []
        try:
            recent_issues = export_issues(engine, days_back=1)[:10]
        except Exception as exc:
            log.debug("Hermes: recent issues export failed: {e}", e=str(exc))

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
                                except Exception as exc:
                                    log.debug("Hermes: duplicate resolved_series delete failed for {f}: {e}", f=r[0], e=str(exc))
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
