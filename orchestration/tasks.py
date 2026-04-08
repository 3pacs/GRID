"""
Reusable Prefect tasks for GRID data pipelines.

Each task is a self-contained unit of work with retries and logging.
Tasks communicate via return values — no shared state.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

try:
    from prefect import task
except ImportError:
    # Fallback decorator if Prefect not installed
    def task(**kwargs):
        def wrapper(fn):
            fn._is_prefect_task = True
            return fn
        return wrapper


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def run_puller(puller_name: str) -> dict:
    """Run a single data puller and return result stats."""
    start = time.time()
    log.info(f"Running puller: {puller_name}")

    try:
        from ingestion.scheduler import get_puller_func
        func = get_puller_func(puller_name)
        if not func:
            return {"puller": puller_name, "status": "not_found", "rows": 0}

        result = func()
        elapsed = time.time() - start
        rows = result.get("rows_inserted", 0) if isinstance(result, dict) else 0

        log.info(f"Puller {puller_name}: {rows} rows in {elapsed:.1f}s")
        return {"puller": puller_name, "status": "success", "rows": rows, "elapsed": elapsed}
    except Exception as e:
        log.error(f"Puller {puller_name} failed: {e}")
        return {"puller": puller_name, "status": "error", "error": str(e)}


@task(retries=2, retry_delay_seconds=10, log_prints=True)
def resolve_conflicts(source_type: str | None = None) -> dict:
    """Run multi-source conflict resolution."""
    log.info(f"Resolving conflicts for: {source_type or 'all sources'}")

    try:
        from normalization.resolver import resolve_all
        result = resolve_all(source_type=source_type)
        resolved = result.get("resolved", 0) if isinstance(result, dict) else 0
        return {"status": "success", "resolved": resolved}
    except Exception as e:
        log.error(f"Conflict resolution failed: {e}")
        return {"status": "error", "error": str(e)}


@task(retries=2, retry_delay_seconds=15, log_prints=True)
def score_hypotheses() -> dict:
    """Score all active hypotheses."""
    log.info("Scoring hypotheses")

    try:
        from db import get_connection
        from intelligence.hypothesis_engine import score_all

        with get_connection() as conn:
            results = score_all(conn)

        scored = len(results) if isinstance(results, list) else 0
        return {"status": "success", "scored": scored}
    except Exception as e:
        log.error(f"Hypothesis scoring failed: {e}")
        return {"status": "error", "error": str(e)}


@task(retries=1, retry_delay_seconds=5, log_prints=True)
def run_trust_cycle() -> dict:
    """Run trust scoring cycle."""
    log.info("Running trust cycle")

    try:
        from db import get_connection
        from intelligence.trust_scorer import run_trust_cycle

        with get_connection() as conn:
            result = run_trust_cycle(conn)

        return {"status": "success", "result": str(result)[:500]}
    except Exception as e:
        log.error(f"Trust cycle failed: {e}")
        return {"status": "error", "error": str(e)}


@task(retries=1, retry_delay_seconds=5, log_prints=True)
def check_alerts() -> dict:
    """Check for triggered alerts and send notifications."""
    log.info("Checking alerts")

    try:
        from alerts.alert_engine import check_all_alerts
        triggered = check_all_alerts()
        return {"status": "success", "triggered": triggered}
    except Exception as e:
        log.error(f"Alert check failed: {e}")
        return {"status": "error", "error": str(e)}


@task(retries=2, retry_delay_seconds=10, log_prints=True)
def run_graph_analytics() -> dict:
    """Run graph analytics batch (PageRank, communities)."""
    log.info("Running graph analytics")

    try:
        from scripts.graph_analytics import run_analytics
        result = run_analytics()
        return {"status": "success", "result": str(result)[:500]}
    except Exception as e:
        log.error(f"Graph analytics failed: {e}")
        return {"status": "error", "error": str(e)}


@task(retries=1, retry_delay_seconds=5, log_prints=True)
def refresh_materialized_views() -> dict:
    """Refresh materialized views (intelligence_search, etc)."""
    log.info("Refreshing materialized views")

    try:
        from db import get_connection

        with get_connection() as conn:
            conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY intelligence_search")

        return {"status": "success"}
    except Exception as e:
        log.error(f"Materialized view refresh failed: {e}")
        return {"status": "error", "error": str(e)}


@task(log_prints=True)
def emit_event(topic: str, payload: dict) -> bool:
    """Emit an event to the event stream."""
    try:
        from events.producer import emit
        return emit(topic, payload)
    except Exception as e:
        log.warning(f"Event emit failed: {e}")
        return False
