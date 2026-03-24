"""
GRID Daily Digest — once-per-day email with all errors, UX audit, and system status.

Collects:
  1. All errors/issues logged in the past 24 hours
  2. Latest UX audit results (score, friction points, improvements)
  3. System health summary (data freshness, LLM status, pipeline runs)
  4. Operator stats (fixes applied, pulls retried, hypotheses tested)

Sent via the existing alerts/email.py infrastructure.

Runs as step 7b in the Hermes operator cycle, or standalone:
    python scripts/daily_digest.py               # send now
    python scripts/daily_digest.py --dry-run      # preview without sending
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DIGEST_HOUR_UTC = 8    # send digest at ~8am UTC
DIGEST_MIN_GAP_HOURS = 20  # don't send more often than every 20 hours


# ---------------------------------------------------------------------------
# Data collectors
# ---------------------------------------------------------------------------

_ISSUES_SQL = text("""
    SELECT id, category, severity, source, title, detail,
           hermes_diagnosis, fix_applied, fix_result, created_at
    FROM operator_issues
    WHERE created_at >= NOW() - INTERVAL '24 hours'
    ORDER BY
        CASE severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'ERROR' THEN 2
            WHEN 'WARNING' THEN 3
            ELSE 4
        END,
        created_at DESC
    LIMIT 100
""")

_UX_AUDIT_SQL = text("""
    SELECT score, total_endpoints, endpoints_ok, avg_latency_ms,
           journey_pass, journey_total, priority_fix,
           friction_points, improvements, audit_timestamp
    FROM ux_audit_results
    WHERE audit_timestamp >= NOW() - INTERVAL '24 hours'
    ORDER BY audit_timestamp DESC
    LIMIT 1
""")

_SNAPSHOT_STATS_SQL = text("""
    SELECT
        COUNT(*) FILTER (WHERE category = 'pipeline_summary') as pipeline_runs,
        COUNT(*) FILTER (WHERE category = 'autoresearch') as research_runs,
        MAX(created_at) as last_snapshot
    FROM analytical_snapshots
    WHERE created_at >= NOW() - INTERVAL '24 hours'
""")

_PULL_STATS_SQL = text("""
    SELECT
        COUNT(DISTINCT source_id) as sources_pulled,
        COUNT(*) as total_rows,
        MIN(pull_timestamp) as earliest_pull,
        MAX(pull_timestamp) as latest_pull
    FROM raw_series
    WHERE pull_timestamp >= NOW() - INTERVAL '24 hours'
""")

_FAILED_PULLS_SQL = text("""
    SELECT source, title, detail, fix_result, created_at
    FROM operator_issues
    WHERE category = 'pull_failure'
      AND created_at >= NOW() - INTERVAL '24 hours'
    ORDER BY created_at DESC
    LIMIT 20
""")


def _collect_issues(engine: Any) -> list[dict[str, Any]]:
    """Collect all issues from the past 24 hours."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(_ISSUES_SQL).fetchall()
            return [
                {
                    "id": r[0], "category": r[1], "severity": r[2],
                    "source": r[3], "title": r[4], "detail": r[5],
                    "diagnosis": r[6], "fix_applied": r[7],
                    "fix_result": r[8],
                    "time": r[9].strftime("%H:%M") if r[9] else "",
                }
                for r in rows
            ]
    except Exception as exc:
        log.debug("Could not collect issues: {e}", e=str(exc))
        return []


def _collect_ux_audit(engine: Any) -> dict[str, Any] | None:
    """Get the latest UX audit from past 24h."""
    try:
        with engine.connect() as conn:
            row = conn.execute(_UX_AUDIT_SQL).fetchone()
            if not row:
                return None
            return {
                "score": row[0],
                "total_endpoints": row[1],
                "endpoints_ok": row[2],
                "avg_latency_ms": row[3],
                "journey_pass": row[4],
                "journey_total": row[5],
                "priority_fix": row[6],
                "friction_points": json.loads(row[7]) if row[7] else [],
                "improvements": json.loads(row[8]) if row[8] else [],
                "timestamp": row[9].isoformat() if row[9] else "",
            }
    except Exception as exc:
        log.debug("Could not collect UX audit: {e}", e=str(exc))
        return None


def _collect_system_stats(engine: Any) -> dict[str, Any]:
    """Collect system-level stats for the past 24h."""
    stats: dict[str, Any] = {}
    try:
        with engine.connect() as conn:
            # Snapshot stats
            row = conn.execute(_SNAPSHOT_STATS_SQL).fetchone()
            if row:
                stats["pipeline_runs"] = row[0]
                stats["research_runs"] = row[1]
                stats["last_snapshot"] = row[2].isoformat() if row[2] else None

            # Pull stats
            row = conn.execute(_PULL_STATS_SQL).fetchone()
            if row:
                stats["sources_pulled"] = row[0]
                stats["total_rows_ingested"] = row[1]
                stats["latest_pull"] = row[3].isoformat() if row[3] else None

            # Failed pulls
            rows = conn.execute(_FAILED_PULLS_SQL).fetchall()
            stats["failed_pulls"] = [
                {"source": r[0], "title": r[1], "fix": r[3],
                 "time": r[4].strftime("%H:%M") if r[4] else ""}
                for r in rows
            ]
    except Exception as exc:
        log.debug("Could not collect system stats: {e}", e=str(exc))
    return stats


# ---------------------------------------------------------------------------
# Email builder
# ---------------------------------------------------------------------------

def _build_digest_sections(
    issues: list[dict[str, Any]],
    ux_audit: dict[str, Any] | None,
    system_stats: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build email sections for the daily digest."""
    sections: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    # ── Error Summary ──
    critical = [i for i in issues if i["severity"] == "CRITICAL"]
    errors = [i for i in issues if i["severity"] == "ERROR"]
    warnings = [i for i in issues if i["severity"] == "WARNING"]

    error_body = f"<strong>{len(issues)}</strong> issues in the past 24h"
    if critical:
        error_body += f"<br><span style='color:#EF4444'>CRITICAL: {len(critical)}</span>"
    if errors:
        error_body += f"<br><span style='color:#F59E0B'>ERROR: {len(errors)}</span>"
    if warnings:
        error_body += f"<br><span style='color:#5A7A96'>WARNING: {len(warnings)}</span>"

    if not issues:
        error_body = "<span style='color:#22C55E'>No issues in the past 24 hours</span>"
        accent = "green"
    elif critical:
        accent = "red"
    elif errors:
        accent = "amber"
    else:
        accent = ""

    sections.append({"title": "Error Summary", "body": error_body, "accent": accent})

    # ── Error Details (top 15) ──
    if issues:
        rows_html = ""
        for i in issues[:15]:
            sev_color = {"CRITICAL": "#EF4444", "ERROR": "#F59E0B", "WARNING": "#5A7A96"}.get(
                i["severity"], "#5A7A96"
            )
            fix_icon = {
                "FIXED": "&#10003;", "PENDING": "&#9711;", "FAILED": "&#10007;",
            }.get(i.get("fix_result", ""), "&#8212;")

            rows_html += (
                f"<tr>"
                f"<td style='color:{sev_color};font-weight:700;font-size:11px'>{i['severity']}</td>"
                f"<td style='font-size:13px'>{i['time']}</td>"
                f"<td style='font-size:13px'>{i['title'][:80]}</td>"
                f"<td style='font-size:13px'>{i.get('source', '')}</td>"
                f"<td style='font-size:13px;text-align:center'>{fix_icon}</td>"
                f"</tr>"
            )

        table = (
            "<table class='data-table'>"
            "<tr><th>SEV</th><th>TIME</th><th>ISSUE</th><th>SOURCE</th><th>FIX</th></tr>"
            f"{rows_html}</table>"
        )
        if len(issues) > 15:
            table += f"<br><em style='color:#5A7A96'>...and {len(issues) - 15} more</em>"

        sections.append({"title": "Error Details", "body": table})

    # ── UX Audit ──
    if ux_audit:
        score = ux_audit.get("score", "?")
        score_color = "#22C55E" if (score and score >= 7) else (
            "#F59E0B" if (score and score >= 4) else "#EF4444"
        )

        ux_body = (
            f"<div class='kpi-row'>"
            f"<span class='kpi-label'>UX Score</span>"
            f"<span class='kpi-value' style='color:{score_color}'>{score}/10</span>"
            f"</div>"
            f"<div class='kpi-row'>"
            f"<span class='kpi-label'>Endpoints</span>"
            f"<span class='kpi-value'>{ux_audit.get('endpoints_ok', '?')}/{ux_audit.get('total_endpoints', '?')} OK</span>"
            f"</div>"
            f"<div class='kpi-row'>"
            f"<span class='kpi-label'>Avg Latency</span>"
            f"<span class='kpi-value'>{ux_audit.get('avg_latency_ms', '?'):.0f}ms</span>"
            f"</div>"
            f"<div class='kpi-row'>"
            f"<span class='kpi-label'>User Journeys</span>"
            f"<span class='kpi-value'>{ux_audit.get('journey_pass', '?')}/{ux_audit.get('journey_total', '?')} pass</span>"
            f"</div>"
        )

        if ux_audit.get("priority_fix"):
            ux_body += (
                f"<br><strong style='color:#F59E0B'>Priority Fix:</strong> "
                f"{ux_audit['priority_fix']}"
            )

        if ux_audit.get("friction_points"):
            ux_body += "<br><br><strong>Friction Points:</strong><ul>"
            for fp in ux_audit["friction_points"][:5]:
                ux_body += f"<li style='font-size:13px;color:#C8D8E8'>{fp}</li>"
            ux_body += "</ul>"

        if ux_audit.get("improvements"):
            ux_body += "<strong>Suggested Improvements:</strong><ul>"
            for imp in ux_audit["improvements"][:5]:
                ux_body += f"<li style='font-size:13px;color:#C8D8E8'>{imp}</li>"
            ux_body += "</ul>"

        ux_accent = "green" if (score and score >= 7) else ("amber" if (score and score >= 4) else "red")
        sections.append({"title": "UX Audit", "body": ux_body, "accent": ux_accent})
    else:
        sections.append({
            "title": "UX Audit",
            "body": "<span style='color:#5A7A96'>No UX audit ran in the past 24 hours</span>",
        })

    # ── System Health ──
    sys_body = ""
    if system_stats.get("sources_pulled"):
        sys_body += (
            f"<div class='kpi-row'>"
            f"<span class='kpi-label'>Sources Pulled</span>"
            f"<span class='kpi-value'>{system_stats['sources_pulled']}</span>"
            f"</div>"
            f"<div class='kpi-row'>"
            f"<span class='kpi-label'>Rows Ingested</span>"
            f"<span class='kpi-value'>{system_stats.get('total_rows_ingested', 0):,}</span>"
            f"</div>"
        )
    if system_stats.get("pipeline_runs") is not None:
        sys_body += (
            f"<div class='kpi-row'>"
            f"<span class='kpi-label'>Pipeline Runs</span>"
            f"<span class='kpi-value'>{system_stats['pipeline_runs']}</span>"
            f"</div>"
        )
    if system_stats.get("research_runs") is not None:
        sys_body += (
            f"<div class='kpi-row'>"
            f"<span class='kpi-label'>Research Cycles</span>"
            f"<span class='kpi-value'>{system_stats['research_runs']}</span>"
            f"</div>"
        )

    # Failed pulls detail
    failed = system_stats.get("failed_pulls", [])
    if failed:
        sys_body += "<br><strong style='color:#F59E0B'>Failed Pulls:</strong><ul>"
        for fp in failed[:8]:
            sys_body += (
                f"<li style='font-size:13px;color:#C8D8E8'>"
                f"{fp['source']} @ {fp['time']} — {fp.get('fix', 'pending')}</li>"
            )
        sys_body += "</ul>"

    if not sys_body:
        sys_body = "<span style='color:#5A7A96'>No system data available</span>"

    sections.append({"title": "System Health (24h)", "body": sys_body})

    return sections


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_daily_digest(engine: Any, dry_run: bool = False) -> dict[str, Any]:
    """Collect all data and send the daily digest email.

    Parameters:
        engine: SQLAlchemy engine for data collection.
        dry_run: If True, build the digest but don't send.

    Returns:
        dict: Summary of what was collected and sent.
    """
    log.info("Building daily digest")

    # Collect data
    issues = _collect_issues(engine)
    ux_audit = _collect_ux_audit(engine)
    system_stats = _collect_system_stats(engine)

    result: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "issues_count": len(issues),
        "ux_score": ux_audit.get("score") if ux_audit else None,
        "has_ux_audit": ux_audit is not None,
    }

    # Build email
    sections = _build_digest_sections(issues, ux_audit, system_stats)
    now = datetime.now(timezone.utc)
    subject = (
        f"GRID Daily Digest — {now.strftime('%b %d')} | "
        f"{len(issues)} issues"
    )
    if ux_audit and ux_audit.get("score"):
        subject += f" | UX: {ux_audit['score']}/10"

    if dry_run:
        result["dry_run"] = True
        result["subject"] = subject
        result["sections"] = len(sections)
        log.info("Daily digest built (dry run) — {n} sections", n=len(sections))
        return result

    # Send via existing email infrastructure
    try:
        from alerts.email import _send
        _send(subject, sections, footer_note="Sent by Hermes Operator — Daily Digest")
        result["sent"] = True
        log.info("Daily digest sent — {subj}", subj=subject)
    except Exception as exc:
        result["sent"] = False
        result["error"] = str(exc)
        log.warning("Daily digest send failed: {e}", e=str(exc))

    return result


def maybe_send_daily_digest(
    state: Any,
    engine: Any,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """Send daily digest if it's time.

    Designed to be called from hermes_operator.run_cycle().

    Parameters:
        state: OperatorState with last_daily_digest timestamp.
        engine: SQLAlchemy engine.
        dry_run: If True, skip actual send.

    Returns:
        dict: Digest result, or None if skipped.
    """
    now = datetime.now(timezone.utc)

    last_digest = getattr(state, "last_daily_digest", None)
    if last_digest is not None:
        hours_since = (now - last_digest).total_seconds() / 3600
        if hours_since < DIGEST_MIN_GAP_HOURS:
            return None

    # Send around the configured hour (within a 1-hour window)
    if not (DIGEST_HOUR_UTC <= now.hour < DIGEST_HOUR_UTC + 1):
        # Allow first-ever digest at any time
        if last_digest is not None:
            return None

    try:
        result = send_daily_digest(engine, dry_run=dry_run)
        if not dry_run:
            state.last_daily_digest = now  # type: ignore[attr-defined]
        return result
    except Exception as exc:
        log.warning("Daily digest failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Standalone
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path

    _GRID_DIR = str(Path(__file__).resolve().parent.parent)
    if _GRID_DIR not in sys.path:
        sys.path.insert(0, _GRID_DIR)

    parser = argparse.ArgumentParser(description="GRID Daily Digest")
    parser.add_argument("--dry-run", action="store_true", help="Build but don't send")
    args = parser.parse_args()

    from db import get_engine
    engine = get_engine()

    result = send_daily_digest(engine, dry_run=args.dry_run)
    print(json.dumps(result, indent=2, default=str))
