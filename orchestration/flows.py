"""
GRID Prefect flows — orchestrated data pipelines.

Flows are the top-level units of work. Each flow composes tasks into
a pipeline with dependencies, retries, and observability.

Flows:
  - ingest_flow: Run all data pullers -> resolve conflicts -> refresh views
  - score_flow: Score hypotheses -> check kills -> run trust cycle
  - alert_flow: Check alerts -> emit notifications
  - nightly_flow: Full nightly pipeline (all of the above + analytics)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

try:
    from prefect import flow, get_run_logger
    from prefect.deployments import Deployment
except ImportError:
    # Fallback if Prefect not installed
    def flow(**kwargs):
        def wrapper(fn):
            fn._is_prefect_flow = True
            return fn
        return wrapper

from orchestration.tasks import (
    run_puller,
    resolve_conflicts,
    score_hypotheses,
    run_trust_cycle,
    check_alerts,
    run_graph_analytics,
    refresh_materialized_views,
    emit_event,
)


# -- Core puller groups -------------------------------------------------------

CRITICAL_PULLERS = [
    "fred", "bls_cpi", "treasury_yields", "ecb_rates",
    "congressional", "insider_filings", "dark_pool",
]

MARKET_PULLERS = [
    "tiingo_eod", "tiingo_crypto", "tiingo_forex",
    "unusual_whales", "prediction_odds", "fed_liquidity",
]

ALTDATA_PULLERS = [
    "fara", "foia_cables", "gdelt", "supply_chain",
    "institutional_flows", "smart_money",
]


@flow(name="GRID Ingest", log_prints=True, retries=1)
def ingest_flow(puller_groups: list[str] | None = None) -> dict:
    """Run data pullers -> resolve conflicts -> refresh materialized views."""

    groups = puller_groups or ["critical", "market"]
    pullers: list[str] = []
    if "critical" in groups:
        pullers.extend(CRITICAL_PULLERS)
    if "market" in groups:
        pullers.extend(MARKET_PULLERS)
    if "altdata" in groups:
        pullers.extend(ALTDATA_PULLERS)

    log.info(f"Ingest flow starting: {len(pullers)} pullers from groups {groups}")

    # Run pullers (Prefect handles parallelism via task runner)
    results = []
    for name in pullers:
        result = run_puller(name)
        results.append(result)

    # Resolve conflicts after all pullers finish
    resolve_result = resolve_conflicts()

    # Refresh materialized views
    refresh_result = refresh_materialized_views()

    # Emit completion event
    total_rows = sum(r.get("rows", 0) for r in results if isinstance(r, dict))
    emit_event("ingestion", {
        "event_type": "ingest_complete",
        "pullers_run": len(pullers),
        "total_rows": total_rows,
        "groups": groups,
    })

    log.info(f"Ingest flow complete: {total_rows} total rows from {len(pullers)} pullers")

    return {
        "status": "complete",
        "puller_results": results,
        "resolve_result": resolve_result,
        "refresh_result": refresh_result,
        "total_rows": total_rows,
    }


@flow(name="GRID Score", log_prints=True, retries=1)
def score_flow() -> dict:
    """Score hypotheses -> run trust cycle -> emit events."""

    log.info("Score flow starting")

    # Score all active hypotheses
    score_result = score_hypotheses()

    # Run trust scoring cycle
    trust_result = run_trust_cycle()

    # Emit completion event
    emit_event("predictions", {
        "event_type": "scoring_complete",
        "scored": score_result.get("scored", 0) if isinstance(score_result, dict) else 0,
    })

    log.info("Score flow complete")

    return {
        "status": "complete",
        "score_result": score_result,
        "trust_result": trust_result,
    }


@flow(name="GRID Alert", log_prints=True)
def alert_flow() -> dict:
    """Check alerts -> send notifications."""

    log.info("Alert flow starting")

    alert_result = check_alerts()

    triggered = alert_result.get("triggered", 0) if isinstance(alert_result, dict) else 0
    if triggered:
        emit_event("alerts", {
            "event_type": "alerts_triggered",
            "count": triggered,
        })

    return {"status": "complete", "alert_result": alert_result}


@flow(name="GRID Nightly", log_prints=True, retries=1)
def nightly_flow() -> dict:
    """Full nightly pipeline: ingest -> score -> analytics -> alerts."""

    log.info("Nightly flow starting")

    # 1. Full ingest (all groups)
    ingest_result = ingest_flow(puller_groups=["critical", "market", "altdata"])

    # 2. Score hypotheses
    score_result = score_flow()

    # 3. Graph analytics
    analytics_result = run_graph_analytics()

    # 4. Check alerts
    alert_result = alert_flow()

    log.info("Nightly flow complete")

    return {
        "status": "complete",
        "ingest": ingest_result,
        "scoring": score_result,
        "analytics": analytics_result,
        "alerts": alert_result,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


@flow(name="GRID Quick Cycle", log_prints=True)
def quick_cycle_flow() -> dict:
    """Quick 6-hour cycle: critical pullers -> score -> alerts."""

    log.info("Quick cycle starting")

    ingest_result = ingest_flow(puller_groups=["critical"])
    score_result = score_flow()
    alert_result = alert_flow()

    return {
        "status": "complete",
        "ingest": ingest_result,
        "scoring": score_result,
        "alerts": alert_result,
    }


# -- CLI entry point ----------------------------------------------------------

def register_deployments() -> list[dict]:
    """Register Prefect deployments with schedules."""
    try:
        from prefect.client.schemas.schedules import CronSchedule

        deployments = [
            {
                "flow": quick_cycle_flow,
                "name": "quick-cycle-6h",
                "schedule": CronSchedule(cron="0 */6 * * *"),  # Every 6 hours
                "description": "Quick cycle: critical pullers -> score -> alerts",
            },
            {
                "flow": nightly_flow,
                "name": "nightly-full",
                "schedule": CronSchedule(cron="0 2 * * *"),  # 2am daily
                "description": "Full nightly pipeline: all pullers -> score -> analytics -> alerts",
            },
            {
                "flow": ingest_flow,
                "name": "market-data-hourly",
                "schedule": CronSchedule(cron="0 * * * 1-5"),  # Hourly on weekdays
                "description": "Market data pullers only",
                "parameters": {"puller_groups": ["market"]},
            },
        ]

        for d in deployments:
            log.info(f"Registered deployment: {d['name']}")

        return deployments
    except ImportError:
        log.warning("Prefect not installed — deployments not registered")
        return []


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "nightly":
            nightly_flow()
        elif cmd == "quick":
            quick_cycle_flow()
        elif cmd == "ingest":
            groups = sys.argv[2:] if len(sys.argv) > 2 else ["critical", "market"]
            ingest_flow(puller_groups=groups)
        elif cmd == "score":
            score_flow()
        elif cmd == "register":
            register_deployments()
        else:
            print(f"Unknown command: {cmd}")
            print("Usage: python -m orchestration.flows [nightly|quick|ingest|score|register]")
    else:
        print("Usage: python -m orchestration.flows [nightly|quick|ingest|score|register]")
