"""
Scheduled TradingAgents runs.

Integrates with GRID's existing scheduler pattern to run agent
deliberations on a configurable cron schedule (default: weekdays 5 PM).
"""

from __future__ import annotations

import threading
import time
from datetime import date

import schedule
from loguru import logger as log

from config import settings


_scheduler_thread: threading.Thread | None = None
_scheduler_running = False


def _run_scheduled_agents() -> None:
    """Execute a scheduled agent run for the default ticker."""
    log.info("Scheduled agent run triggered")
    try:
        from db import get_engine
        from agents.runner import AgentRunner

        engine = get_engine()
        runner = AgentRunner(engine)
        result = runner.run(ticker=settings.AGENTS_DEFAULT_TICKER, as_of_date=date.today())
        log.info(
            "Scheduled agent run complete — decision={d}, run_id={id}",
            d=result.get("final_decision"),
            id=result.get("run_id"),
        )
    except Exception as exc:
        log.error("Scheduled agent run failed: {e}", e=str(exc))


def _parse_cron_to_schedule(cron_expr: str) -> None:
    """Parse a simple cron expression and register with the schedule library.

    Supports: ``minute hour * * days`` where days is 0-6 or 1-5 or *.
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        log.warning("Invalid AGENTS_SCHEDULE_CRON: {c}", c=cron_expr)
        return

    minute, hour, _, _, day_of_week = parts
    time_str = f"{int(hour):02d}:{int(minute):02d}"

    if day_of_week in ("*", "0-6"):
        schedule.every().day.at(time_str).do(_run_scheduled_agents)
    elif day_of_week == "1-5":
        schedule.every().monday.at(time_str).do(_run_scheduled_agents)
        schedule.every().tuesday.at(time_str).do(_run_scheduled_agents)
        schedule.every().wednesday.at(time_str).do(_run_scheduled_agents)
        schedule.every().thursday.at(time_str).do(_run_scheduled_agents)
        schedule.every().friday.at(time_str).do(_run_scheduled_agents)
    else:
        # Try individual days
        day_map = {
            "0": "sunday", "1": "monday", "2": "tuesday",
            "3": "wednesday", "4": "thursday", "5": "friday", "6": "saturday",
        }
        for d in day_of_week.split(","):
            d = d.strip()
            if d in day_map:
                getattr(schedule.every(), day_map[d]).at(time_str).do(_run_scheduled_agents)

    log.info("Agent schedule configured: {c} → {t}", c=cron_expr, t=time_str)


def _scheduler_loop() -> None:
    """Run pending scheduled jobs in a background loop."""
    global _scheduler_running
    _scheduler_running = True
    log.info("Agent scheduler loop started")
    while _scheduler_running:
        schedule.run_pending()
        time.sleep(30)
    log.info("Agent scheduler loop stopped")


def start_agent_scheduler() -> None:
    """Start the agent scheduler in a background thread."""
    global _scheduler_thread

    if not settings.AGENTS_ENABLED or not settings.AGENTS_SCHEDULE_ENABLED:
        log.info("Agent scheduler not started (disabled)")
        return

    if _scheduler_thread and _scheduler_thread.is_alive():
        log.info("Agent scheduler already running")
        return

    _parse_cron_to_schedule(settings.AGENTS_SCHEDULE_CRON)
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="agent-scheduler")
    _scheduler_thread.start()
    log.info("Agent scheduler started in background")


def stop_agent_scheduler() -> None:
    """Stop the background scheduler."""
    global _scheduler_running
    _scheduler_running = False
    log.info("Agent scheduler stop requested")


def get_schedule_status() -> dict:
    """Return current schedule status."""
    jobs = schedule.get_jobs()
    agent_jobs = [j for j in jobs if j.job_func and j.job_func.__name__ == "_run_scheduled_agents"]
    next_run = min((j.next_run for j in agent_jobs), default=None) if agent_jobs else None

    return {
        "schedule_enabled": settings.AGENTS_SCHEDULE_ENABLED,
        "cron": settings.AGENTS_SCHEDULE_CRON,
        "scheduled_jobs": len(agent_jobs),
        "next_run": next_run.isoformat() if next_run else None,
        "running": _scheduler_running,
    }
