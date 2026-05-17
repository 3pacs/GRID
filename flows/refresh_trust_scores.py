"""
Nightly Prefect flow: refresh source_trust_scores_cached.

The unified intelligence dashboard previously called
``intelligence.trust_scorer.update_trust_scores`` on every cold cache hit,
which recomputes Bayesian trust for ~2,771 signal sources from ~60K rows
in ``signal_sources``. That cost ~12s of the dashboard's cold load.

This flow runs ``update_trust_scores`` once per night and persists the
result into ``source_trust_scores_cached`` so the dashboard can read the
pre-computed snapshot in <100ms.

Schedule: 02:30 UTC daily (after the 02:00 nightly ingest flow).

Run manually:
    python -m flows.refresh_trust_scores
    python -m flows.refresh_trust_scores register   # register deployment

The dashboard cold path falls back to a live ``update_trust_scores`` if
the cache is missing or older than 24h (see
``trust_scorer.load_trust_scores_cached``), so this flow failing for one
night does not break the UI — it just means the next dashboard render
will be slow.
"""

from __future__ import annotations

from datetime import datetime, timezone

from loguru import logger as log

try:
    from prefect import flow, task
except ImportError:
    # Graceful fallback so the module can still be invoked without Prefect
    # installed — useful for ad-hoc cron runs from systemd.
    def flow(**kwargs):
        def wrapper(fn):
            fn._is_prefect_flow = True
            return fn

        return wrapper

    def task(**kwargs):
        def wrapper(fn):
            fn._is_prefect_task = True
            return fn

        return wrapper


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def _recompute_and_cache() -> dict:
    """Recompute trust scores and persist to source_trust_scores_cached."""
    from api.dependencies import get_db_engine
    from intelligence.trust_scorer import (
        update_trust_scores,
        write_trust_scores_cache,
    )

    engine = get_db_engine()
    started = datetime.now(timezone.utc)

    payload = update_trust_scores(engine)
    n_sources = len(payload.get("sources", []))

    rows_written = write_trust_scores_cache(engine, payload)
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()

    log.info(
        "refresh_trust_scores: recomputed {n} sources, "
        "wrote {w} rows in {e:.1f}s",
        n=n_sources,
        w=rows_written,
        e=elapsed,
    )
    return {
        "status": "success",
        "sources_recomputed": n_sources,
        "rows_written": rows_written,
        "elapsed_seconds": elapsed,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


@flow(name="GRID Refresh Trust Scores", log_prints=True, retries=1)
def refresh_trust_scores_flow() -> dict:
    """Nightly refresh of the trust score cache used by the dashboard."""
    log.info("refresh_trust_scores_flow: starting")
    result = _recompute_and_cache()
    log.info("refresh_trust_scores_flow: complete — {r}", r=result)
    return result


def register_deployment() -> dict | None:
    """Register the nightly deployment with Prefect (cron 02:30 UTC).

    Mirrors the pattern in ``orchestration/flows.py::register_deployments``.
    """
    try:
        from prefect.client.schemas.schedules import CronSchedule

        deployment = {
            "flow": refresh_trust_scores_flow,
            "name": "refresh-trust-scores-nightly",
            "schedule": CronSchedule(cron="30 2 * * *"),  # 02:30 UTC daily
            "description": (
                "Recompute source trust scores and write to "
                "source_trust_scores_cached so the intelligence dashboard "
                "can serve them in <100ms instead of recomputing on every "
                "cold cache hit (~12s)."
            ),
        }
        log.info("Registered deployment: {n}", n=deployment["name"])
        return deployment
    except ImportError:
        log.warning("Prefect not installed — deployment not registered")
        return None


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "register":
        register_deployment()
    else:
        refresh_trust_scores_flow()
