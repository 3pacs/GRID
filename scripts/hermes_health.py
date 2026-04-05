"""
GRID Hermes Operator — health checks, issue tracking, and state management.

Contains:
  - Issue tracker (log_issue, export_issues, _ensure_issues_table)
  - SourceCooldown (per-source retry throttling)
  - OperatorState (mutable state persisted across cycles)
  - Health checks (check_db_health, check_hermes_health, check_system_health)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log


# Import constants from the main module
CYCLE_INTERVAL_SECONDS = 300
DATA_FRESHNESS_THRESHOLD_HOURS = 26
SOURCE_COOLDOWN_MINUTES = 30
SOURCE_MAX_CONSECUTIVE_FAILS = 5
TIMEOUT_BLACKLIST_HOURS = 24


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
        self.last_hypothesis_discovery: datetime | None = None
        self.last_rag_index: datetime | None = None
        self.last_trust_cycle: datetime | None = None
        self.last_options_recommendations: datetime | None = None
        self.last_cross_reference_checks: datetime | None = None
        self.last_options_scoring: datetime | None = None
        self.last_lever_pullers: datetime | None = None
        self.last_actor_wealth: datetime | None = None
        self.last_daily_intel: datetime | None = None      # 2:00 AM daily batch
        self.last_weekly_intel: datetime | None = None      # Sunday 3:00 AM weekly batch
        self.last_signal_registry: datetime | None = None   # Every 2 hours
        self.last_signal_forecasts: datetime | None = None  # TimesFM every 4 hours

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
            "last_hypothesis_discovery": self.last_hypothesis_discovery.isoformat() if self.last_hypothesis_discovery else None,
            "last_rag_index": self.last_rag_index.isoformat() if self.last_rag_index else None,
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
    """Check if any LLM provider is responding."""
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.LOCAL)
        hc = client.health_check()
        return {
            "healthy": hc.get("available", False),
            "latency_ms": hc.get("latency_ms"),
            "models": hc.get("models", []),
            "provider": hc.get("provider", "unknown"),
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

