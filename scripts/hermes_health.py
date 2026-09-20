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
OPERATOR_ISSUE_DEDUPE_HOURS = 20


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
            if fix_result != "SUCCESS":
                existing = conn.execute(
                    text(
                        "SELECT id FROM operator_issues "
                        "WHERE category = :cat "
                        "AND severity = :sev "
                        "AND source IS NOT DISTINCT FROM :src "
                        "AND title = :title "
                        "AND resolved_at IS NULL "
                        "AND created_at >= NOW() - :hours * INTERVAL '1 hour' "
                        "ORDER BY created_at DESC LIMIT 1"
                    ),
                    {
                        "cat": category,
                        "sev": severity,
                        "src": source,
                        "title": title,
                        "hours": OPERATOR_ISSUE_DEDUPE_HOURS,
                    },
                ).fetchone()
                if existing:
                    issue_id = existing[0]
                    log.info(
                        "duplicate issue suppressed — existing #{id} [{sev}] {title}",
                        id=issue_id, sev=severity, title=title[:80],
                    )
                    return issue_id

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


def resolve_source_issues(engine: Any, source: str, cycle_number: int | None = None) -> int:
    """Mark unresolved WARNING/ERROR/CRITICAL issues for a recovered source resolved."""
    from sqlalchemy import text

    if not source:
        return 0
    _ensure_issues_table(engine)
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "UPDATE operator_issues "
                    "SET resolved_at = NOW(), "
                    "    fix_result = COALESCE(fix_result, 'SUCCESS'), "
                    "    detail = COALESCE(detail, '') || "
                    "        CASE WHEN detail IS NULL OR detail = '' THEN '' ELSE E'\\n' END || "
                    "        :note "
                    "WHERE source = :source "
                    "  AND resolved_at IS NULL "
                    "  AND severity IN ('WARNING', 'ERROR', 'CRITICAL') "
                    "RETURNING id"
                ),
                {
                    "source": source,
                    "note": (
                        f"Resolved by successful Hermes recovery"
                        f"{f' in cycle {cycle_number}' if cycle_number is not None else ''}."
                    ),
                },
            ).fetchall()
        resolved = len(row)
        if resolved:
            log.info("Resolved {n} prior operator issues for {source}", n=resolved, source=source)
        return resolved
    except Exception as exc:
        log.warning("Failed to resolve source issues for {source}: {e}", source=source, e=str(exc))
        return 0


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
        self.last_enrich_connections: datetime | None = None  # Daily connection enrichment at 4 AM
        self.last_solana_universe: datetime | None = None    # Top-N by volume, every 4 hours
        self.last_forced_flow_brief: datetime | None = None  # Daily forced-flow waterfall briefing ~06:30 UTC
        self.last_contagion_backtest: datetime | None = None  # Daily contagion backtest at 5 AM
        self.last_contagion_feedback: datetime | None = None  # Daily contagion feedback loop right after backtest
        self.last_sector_health: datetime | None = None  # Daily sector health snapshot, due-period opens 3 AM UTC (marks the due period done — success, no_eligible_sectors, or superseded)
        self.last_sector_health_attempt: datetime | None = None  # Last sector-health attempt (success or failure) — drives retry backoff
        self.sector_health_attempt_count: int = 0  # Attempts made in the current sector-health due period — capped, reset each new period
        self.last_sector_health_outcome: str | None = None  # "success" | "no_eligible_sectors" | "superseded" | "failure" — see _maybe_run_sector_health_snapshot
        self.sector_health_attempt_token: int = 0  # Incremented at each attempt start; a worker's result is only committed if this still matches at completion (guards against an abandoned _run_with_timeout worker writing stale state after a later cycle's attempt has already started)
        self.last_active_hypo_scoring: datetime | None = None  # Periodic batch scoring of overdue active hypos (30 min)
        self.last_earnings_calendar_sync: datetime | None = None  # earnings_events → earnings_calendar back-compat sync (30 min)
        self.last_resolution: datetime | None = None  # raw_series → resolved_series watermark (start time of the last clean resolver run)

        # Daily-intel per-task ledger (fable-daily-intel-resumable,
        # 2026-09-20). See scripts/hermes_operator.py::DAILY_INTEL_TASKS and
        # _run_daily_intel_block for the executor. Replaces the old
        # all-or-nothing `state.last_daily_intel = now` (set only at the very
        # end of the block, after every task ran) with a per-task record so a
        # cycle-budget cutoff or a restart resumes from the first undone task
        # instead of re-running the whole ~20-task block from the top.
        #
        # daily_intel_period: ISO date of the due period (boundary_hour=
        # DAILY_INTEL_BOUNDARY_HOUR=2) the three dicts below belong to. When
        # a new evaluation's due-period date differs from this, the ledger
        # has rolled over: _run_daily_intel_block clears daily_intel_done,
        # daily_intel_skipped_for_period and daily_intel_attempts and sets
        # this to the new period before doing anything else.
        self.daily_intel_period: str | None = None
        # daily_intel_done: task name -> ISO date of the due period it
        # completed (or was skipped_for_period) for. A task present here
        # with the CURRENT period's date is not re-run this period —
        # checked regardless of whether it got there via success or via
        # DAILY_INTEL_MAX_ATTEMPTS-exhaustion (see daily_intel_skipped_for_
        # period below), so a permanently-broken task cannot block the
        # tasks scheduled after it.
        self.daily_intel_done: dict[str, str] = {}
        # daily_intel_skipped_for_period: task name -> ISO date of the due
        # period it was marked skipped_for_period for (attempts reached
        # DAILY_INTEL_MAX_ATTEMPTS without a success). Every entry here also
        # has a matching entry in daily_intel_done (same date) — this dict
        # exists only to distinguish "skipped" from "succeeded" for the
        # per-cycle summary log line and for tests; it is never consulted on
        # its own to decide whether to (re)run a task.
        self.daily_intel_skipped_for_period: dict[str, str] = {}
        # daily_intel_attempts: task name -> attempts made in the CURRENT
        # period (timeouts and exceptions both count; see
        # _run_daily_intel_block). Reset to {} on period rollover along with
        # daily_intel_done/daily_intel_skipped_for_period above.
        self.daily_intel_attempts: dict[str, int] = {}
        # daily_intel_task_outcome: task name -> "done" | "done_queued" |
        # "skipped_for_period" | "held" | "in_flight", for the CURRENT
        # period only (reset to {} on the same rollover as
        # daily_intel_done/skipped_for_period/attempts above). This is a
        # strictly additive, human/test-facing view over the same facts
        # daily_intel_done/daily_intel_skipped_for_period already encode —
        # "done"/"done_queued" and "skipped_for_period" are written at the
        # exact same points those two dicts are (see _run_daily_intel_block)
        # — plus two states neither of those dicts can represent: "held"
        # (task is not in DAILY_INTEL_INITIAL_ALLOWLIST this period — never
        # attempted, never counted toward daily_intel_done, and therefore
        # invisible to the "period complete" check) and "in_flight" (a
        # retry this cycle was skipped because the previous attempt's
        # worker thread was still alive — see _DAILY_INTEL_IN_FLIGHT in
        # scripts/hermes_operator.py). A held task can never carry "done"/
        # "done_queued" or "skipped_for_period" here, by construction —
        # _run_daily_intel_block never runs a held task's fn, so there is
        # no code path that could write either value for it.
        #
        # "done_queued" (fable-hermes-daily-intel-resumable review, part C,
        # 2026-09-20): a task whose own step only ENQUEUES a goal_queue row
        # for a separate subagent process (currently just
        # storage_maintenance_subagent — see DailyIntelTask.
        # reports_done_queued in scripts/hermes_operator.py) reports
        # "done_queued" instead of "done" the moment the enqueue call
        # returns, deliberately distinct from "done" so this ledger cannot
        # be misread as "the subagent's work finished." The subagent's own
        # completion (or failure) is tracked separately, by goal_queue's
        # state column and the goal_results table
        # (intelligence/goal_queue.py) — NOT by this ledger. "done_queued"
        # still counts toward daily_intel_done/period completion exactly
        # like "done" does; it only changes what the outcome label claims
        # happened.
        self.daily_intel_task_outcome: dict[str, str] = {}
        # daily_intel_period_outcome: "complete" | "complete_with_skips" |
        # "complete_for_enabled_tasks" | "complete_for_enabled_tasks_with_
        # skips" | None. Set (alongside state.last_daily_intel = now) the
        # moment every ALLOW-LISTED task for the current period has a
        # daily_intel_done entry.
        #
        # The "_for_enabled_tasks" suffix (fable-hermes-daily-intel-
        # resumable review, part E, 2026-09-20) reports honestly that a
        # held subset of DAILY_INTEL_TASKS did NOT run this period — the
        # bare "complete"/"complete_with_skips" values are reserved for the
        # (currently hypothetical) case where DAILY_INTEL_INITIAL_ALLOWLIST
        # covers every DAILY_INTEL_TASKS entry (no held tasks at all). As
        # long as any task is held — true today, 13 of 21 allow-listed —
        # the period outcome is always one of the "_for_enabled_tasks"
        # values, never the bare ones, so "complete" can never be read as
        # "the whole daily-intel batch ran."
        #   - "complete_for_enabled_tasks": all allow-listed tasks done/
        #     done_queued, none needed skipped_for_period, at least one
        #     task is held.
        #   - "complete_for_enabled_tasks_with_skips": same, but at least
        #     one allow-listed task got there via skipped_for_period.
        #   - "complete" / "complete_with_skips": same two conditions, but
        #     with zero held tasks.
        # None while the period is still in progress, and reset to None on
        # period rollover (same trigger as the four ledger dicts above) so
        # a stale prior period's outcome can never be read as the current
        # period's.
        self.daily_intel_period_outcome: str | None = None

        # Bounded-repair backlog (fable-hermes-repair-bound, 2026-09-19):
        # source_key (lowercased source_catalog name) -> list of tickers/ids
        # not yet attempted, left over when a repair pull in
        # scripts/hermes_fixers.py::_retry_source stops early because
        # REPAIR_BUDGET_SECONDS ran out. Persisted so the NEXT repair
        # attempt for that source resumes from the remainder instead of
        # restarting from the first ticker every cycle. Cleared once a
        # repair for that source completes without being stopped by budget.
        self.repair_backlog: dict[str, list[str]] = {}

        # Freshness-semantics fix (fable-hermes-repair-bound follow-up,
        # 2026-09-19 review): source_key -> the single most recent bounded-
        # repair check summary for that source (see _retry_source in
        # scripts/hermes_fixers.py). Bounded to the LAST summary only — this
        # is a status snapshot, not a history log. Persisted so a checked-
        # but-not-fully-current source's most recent check is visible after
        # a restart, not just in that cycle's log line.
        self.repair_last_check: dict[str, dict[str, Any]] = {}

        # source_key -> the most recent record of repair coverage that did
        # NOT reach every ticker (attempt cap hit, or per-ticker no_data/
        # error outcomes). This is the compensating signal for Check 1b:
        # source_catalog.last_pull_at (and repair_last_check above) can look
        # "checked" while some tickers are still outside the repair window's
        # reach — this field is what tells a human or the diagnostics LLM
        # that a separately authorised backfill, not another REPULL, is
        # what would actually close the gap. Bounded to the last record per
        # source, persisted the same way as repair_backlog/repair_last_check.
        self.repair_uncovered: dict[str, dict[str, Any]] = {}

        # Hermes status log: task_name -> {last_run, success, duration_s, error}
        self.task_status: dict[str, dict[str, Any]] = {}

    def record_task(
        self,
        task_name: str,
        success: bool,
        duration_s: float,
        error: str | None = None,
        transient: bool = False,
    ) -> None:
        """Record the outcome of a scheduled task for the status endpoint.

        Args:
            task_name: Cycle step the outcome belongs to.
            success: Whether the step completed cleanly.
            duration_s: Wall time the step consumed.
            error: Failure detail, prefixed with the exception class where
                the caller knows it.
            transient: True when the failure is operational (a statement
                timeout, a dropped connection, a step abandoned at its
                budget) rather than a defect. Health surfaces read this to
                avoid paging on a slow database.
        """
        self.task_status[task_name] = {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "success": success,
            "duration_s": round(duration_s, 2),
            "error": error,
            "transient": transient,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_pipeline_run": self.last_pipeline_run.isoformat() if self.last_pipeline_run else None,
            "last_autoresearch": self.last_autoresearch.isoformat() if self.last_autoresearch else None,
            "last_ux_audit": self.last_ux_audit.isoformat() if self.last_ux_audit else None,
            "last_daily_digest": self.last_daily_digest.isoformat() if self.last_daily_digest else None,
            "last_100x_digest": self.last_100x_digest.isoformat() if self.last_100x_digest else None,
            "last_oracle_cycle": self.last_oracle_cycle.isoformat() if self.last_oracle_cycle else None,
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
            "last_signal_forecasts": self.last_signal_forecasts.isoformat() if self.last_signal_forecasts else None,
            "last_enrich_connections": self.last_enrich_connections.isoformat() if self.last_enrich_connections else None,
            "last_contagion_backtest": self.last_contagion_backtest.isoformat() if self.last_contagion_backtest else None,
            "last_contagion_feedback": self.last_contagion_feedback.isoformat() if self.last_contagion_feedback else None,
            "last_sector_health": self.last_sector_health.isoformat() if self.last_sector_health else None,
            "last_sector_health_attempt": self.last_sector_health_attempt.isoformat() if self.last_sector_health_attempt else None,
            "sector_health_attempt_count": self.sector_health_attempt_count,
            "last_sector_health_outcome": self.last_sector_health_outcome,
            "last_active_hypo_scoring": self.last_active_hypo_scoring.isoformat() if self.last_active_hypo_scoring else None,
            "last_earnings_calendar_sync": self.last_earnings_calendar_sync.isoformat() if self.last_earnings_calendar_sync else None,
            "last_resolution": self.last_resolution.isoformat() if self.last_resolution else None,
            "last_options_scoring": self.last_options_scoring.isoformat() if self.last_options_scoring else None,
            "task_status": self.task_status,
            "repair_backlog": self.repair_backlog,
            "repair_last_check": self.repair_last_check,
            "repair_uncovered": self.repair_uncovered,
            "daily_intel_period": self.daily_intel_period,
            "daily_intel_done": self.daily_intel_done,
            "daily_intel_skipped_for_period": self.daily_intel_skipped_for_period,
            "daily_intel_attempts": self.daily_intel_attempts,
            "daily_intel_task_outcome": self.daily_intel_task_outcome,
            "daily_intel_period_outcome": self.daily_intel_period_outcome,
        }

    def hydrate_from_snapshot(self, engine: Any) -> bool:
        """Restore last_* timestamps from the most-recent hermes_operator snapshot.

        Called once on daemon startup so schedule memory survives restarts.
        Only populates timestamp fields that are currently None — never
        overwrites live state. Silent on any failure (fresh start is always OK).

        Returns True if at least one field was hydrated, False otherwise.
        """
        import json
        from sqlalchemy import text

        try:
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT payload FROM analytical_snapshots "
                    "WHERE subcategory = 'hermes_operator' "
                    "ORDER BY created_at DESC LIMIT 1"
                )).fetchone()
            if not row:
                return False
            payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            op_state = payload.get("operator_state", {})
        except Exception:
            return False

        # Fields to restore (must match attribute names on self)
        restorable = [
            "last_pipeline_run", "last_autoresearch", "last_daily_intel",
            "last_weekly_intel", "last_hypothesis_discovery", "last_rag_index",
            "last_trust_cycle", "last_options_recommendations",
            "last_cross_reference_checks", "last_options_scoring",
            "last_lever_pullers", "last_actor_wealth", "last_signal_registry",
            "last_signal_forecasts", "last_enrich_connections",
            "last_contagion_backtest", "last_contagion_feedback",
            "last_sector_health", "last_sector_health_attempt", "last_active_hypo_scoring",
            "last_earnings_calendar_sync", "last_resolution", "last_ux_audit",
            "last_daily_digest", "last_100x_digest", "last_oracle_cycle",
        ]
        hydrated_any = False
        for field in restorable:
            raw = op_state.get(field)
            if not raw or getattr(self, field, "missing") is not None:
                continue
            try:
                dt = datetime.fromisoformat(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                setattr(self, field, dt)
                hydrated_any = True
            except Exception:
                continue

        # Counters and task_status are cumulative — carry them forward too.
        for int_field in ("cycle_count", "fixes_applied", "pulls_retried",
                          "hypotheses_tested", "errors_diagnosed",
                          "sector_health_attempt_count"):
            val = op_state.get(int_field)
            if isinstance(val, int) and getattr(self, int_field, 0) == 0:
                setattr(self, int_field, val)

        ts = op_state.get("task_status")
        if isinstance(ts, dict) and not self.task_status:
            self.task_status = ts

        backlog = op_state.get("repair_backlog")
        if isinstance(backlog, dict) and not self.repair_backlog:
            self.repair_backlog = {
                str(k): list(v) for k, v in backlog.items() if isinstance(v, list)
            }
            hydrated_any = hydrated_any or bool(self.repair_backlog)

        last_check = op_state.get("repair_last_check")
        if isinstance(last_check, dict) and not self.repair_last_check:
            self.repair_last_check = {
                str(k): v for k, v in last_check.items() if isinstance(v, dict)
            }
            hydrated_any = hydrated_any or bool(self.repair_last_check)

        uncovered = op_state.get("repair_uncovered")
        if isinstance(uncovered, dict) and not self.repair_uncovered:
            self.repair_uncovered = {
                str(k): v for k, v in uncovered.items() if isinstance(v, dict)
            }
            hydrated_any = hydrated_any or bool(self.repair_uncovered)

        # Daily-intel ledger (fable-daily-intel-resumable, 2026-09-20) —
        # same "only if currently unset" rule as repair_backlog above, so a
        # restart mid-period resumes from the first undone task instead of
        # re-running everything.
        daily_intel_done = op_state.get("daily_intel_done")
        if isinstance(daily_intel_done, dict) and not self.daily_intel_done:
            self.daily_intel_done = {
                str(k): str(v) for k, v in daily_intel_done.items() if v is not None
            }
            hydrated_any = hydrated_any or bool(self.daily_intel_done)

        daily_intel_skipped = op_state.get("daily_intel_skipped_for_period")
        if isinstance(daily_intel_skipped, dict) and not self.daily_intel_skipped_for_period:
            self.daily_intel_skipped_for_period = {
                str(k): str(v) for k, v in daily_intel_skipped.items() if v is not None
            }
            hydrated_any = hydrated_any or bool(self.daily_intel_skipped_for_period)

        daily_intel_attempts = op_state.get("daily_intel_attempts")
        if isinstance(daily_intel_attempts, dict) and not self.daily_intel_attempts:
            self.daily_intel_attempts = {
                str(k): int(v) for k, v in daily_intel_attempts.items()
                if isinstance(v, int)
            }
            hydrated_any = hydrated_any or bool(self.daily_intel_attempts)

        daily_intel_task_outcome = op_state.get("daily_intel_task_outcome")
        if isinstance(daily_intel_task_outcome, dict) and not self.daily_intel_task_outcome:
            self.daily_intel_task_outcome = {
                str(k): str(v) for k, v in daily_intel_task_outcome.items() if v is not None
            }
            hydrated_any = hydrated_any or bool(self.daily_intel_task_outcome)

        # Plain string fields (not timestamps, not counters) — restore
        # verbatim, same "only if currently unset" rule as the datetime
        # fields above.
        for str_field in (
            "last_sector_health_outcome", "daily_intel_period",
            "daily_intel_period_outcome",
        ):
            val = op_state.get(str_field)
            if isinstance(val, str) and getattr(self, str_field, None) is None:
                setattr(self, str_field, val)
                hydrated_any = True

        return hydrated_any


# ─── Health checks ───────────────────────────────────────────────────

def check_db_health(engine: Any) -> dict[str, Any]:
    """Check database connectivity and basic stats."""
    from sqlalchemy import text
    result: dict[str, Any] = {"healthy": False}
    try:
        with engine.connect() as conn:
            conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            conn.execute(text("SELECT 1"))
            result["healthy"] = True

            # Avoid a full raw_series scan during health checks. On the live
            # GRID DB this table is large enough that COUNT(*) can consume the
            # whole Hermes cycle before any repair work starts.
            row = conn.execute(text(
                "SELECT COALESCE(("
                "  SELECT reltuples::bigint "
                "  FROM pg_class "
                "  WHERE oid = to_regclass('public.raw_series')"
                "), 0)"
            )).fetchone()
            result["raw_series_count"] = row[0] if row else 0
            result["raw_series_count_estimated"] = True

            # Latest pull
            row = conn.execute(
                text("SELECT MAX(pull_timestamp) FROM raw_series WHERE pull_status = 'SUCCESS'")
            ).fetchone()
            result["latest_pull"] = row[0].isoformat() if row and row[0] else None

            # Failed pulls — both windows. The 24h count is for
            # historical reporting / dashboards; the 1h count drives
            # the alerter so a brief outage that's already over
            # auto-resolves instead of paging for 23 more hours.
            row = conn.execute(
                text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE pull_status = 'FAILED' "
                    "AND pull_timestamp > NOW() - INTERVAL '24 hours'"
                )
            ).fetchone()
            result["failed_pulls_24h"] = row[0] if row else 0
            row = conn.execute(
                text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE pull_status = 'FAILED' "
                    "AND pull_timestamp > NOW() - INTERVAL '1 hour'"
                )
            ).fetchone()
            result["failed_pulls_1h"] = row[0] if row else 0

            # Source freshness from source_catalog only. The old fallback
            # joined every active source to raw_series and did an aggregate
            # over the entire table, which made Hermes dry-runs exceed 90s.
            rows = conn.execute(
                text(
                    "SELECT name, last_pull_at "
                    "FROM source_catalog "
                    "WHERE active = TRUE "
                    "ORDER BY last_pull_at ASC NULLS FIRST"
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

    # SQLAlchemy connection pool stats — feeds the pool.exhausted
    # alert condition in alerts/health_alerter.py. The QueuePool
    # exposes size(), checkedout(), overflow() at runtime; surface
    # the three that matter into the health dict.
    try:
        pool = getattr(engine, "pool", None)
        if pool is not None:
            result["pool"] = {
                "size": int(pool.size()) if hasattr(pool, "size") else 0,
                "checked_out": int(pool.checkedout()) if hasattr(pool, "checkedout") else 0,
                "overflow": int(pool.overflow()) if hasattr(pool, "overflow") else 0,
            }
    except Exception as exc:
        result["pool_error"] = str(exc)

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
