"""System status and health endpoints."""

from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.schemas.system import (
    DatabaseStatus,
    GridStats,
    HealthResponse,
    HyperspaceStatus,
    LogsResponse,
    RestartResponse,
    SystemStatusResponse,
)

router = APIRouter(prefix="/api/v1/system", tags=["system"])

_start_time = time.time()


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Health check — no auth required.

    Checks database connectivity, data freshness, connection pool,
    and LLM availability.
    """
    checks: dict[str, bool] = {}
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            checks["database"] = True

            # Verify feature registry is populated
            r = conn.execute(text("SELECT COUNT(*) FROM feature_registry")).fetchone()
            checks["features_registered"] = (r[0] if r else 0) > 0

            # Check for recent data pulls (within last 7 days)
            r = conn.execute(
                text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE pull_timestamp >= NOW() - INTERVAL '7 days'"
                )
            ).fetchone()
            checks["recent_data"] = (r[0] if r else 0) > 0

        # Connection pool health
        pool = engine.pool
        checks["pool_healthy"] = pool.checkedout() < pool.size() + pool.overflow()
    except Exception:
        checks["database"] = False

    # LLM availability (non-blocking)
    try:
        from llamacpp.client import LlamaCppClient
        client = LlamaCppClient()
        checks["llm_available"] = client.is_available
    except Exception:
        checks["llm_available"] = False

    # API key audit (how many sources are configured)
    try:
        from config import settings
        key_audit = settings.audit_api_keys()
        checks["api_keys_configured"] = sum(key_audit.values())
        checks["api_keys_total"] = len(key_audit)
    except Exception:
        pass

    all_ok = checks.get("database", False)
    return HealthResponse(status="ok" if all_ok else "degraded")


@router.get("/status", response_model=SystemStatusResponse)
async def status(_token: str = Depends(require_auth)) -> SystemStatusResponse:
    """Comprehensive system status."""
    engine = get_db_engine()

    # Database status
    db_status = DatabaseStatus()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            db_status.connected = True
            row = conn.execute(
                text("SELECT pg_database_size(current_database()) AS size")
            ).fetchone()
            if row:
                db_status.size_mb = round(row[0] / (1024 * 1024), 2)
    except Exception as exc:
        log.warning("DB status check failed: {e}", e=str(exc))

    # Grid stats
    grid_stats = GridStats()
    try:
        with engine.connect() as conn:
            r = conn.execute(text("SELECT COUNT(*) FROM feature_registry")).fetchone()
            grid_stats.features_total = r[0] if r else 0

            r = conn.execute(
                text("SELECT COUNT(*) FROM feature_registry WHERE model_eligible = TRUE")
            ).fetchone()
            grid_stats.features_model_eligible = r[0] if r else 0

            r = conn.execute(text("SELECT COUNT(*) FROM hypothesis_registry")).fetchone()
            grid_stats.hypotheses_total = r[0] if r else 0

            r = conn.execute(
                text(
                    "SELECT COUNT(*) FROM model_registry WHERE state = 'PRODUCTION'"
                )
            ).fetchone()
            grid_stats.hypotheses_in_production = r[0] if r else 0

            r = conn.execute(text("SELECT COUNT(*) FROM decision_journal")).fetchone()
            grid_stats.journal_entries_total = r[0] if r else 0

            r = conn.execute(
                text(
                    "SELECT COUNT(*) FROM decision_journal WHERE outcome_recorded_at IS NOT NULL"
                )
            ).fetchone()
            grid_stats.journal_entries_with_outcomes = r[0] if r else 0
    except Exception as exc:
        log.warning("Grid stats query failed: {e}", e=str(exc))

    # Hyperspace status
    hs_status = HyperspaceStatus()
    try:
        from hyperspace.client import get_client

        client = get_client()
        if client.is_available:
            hs_status.node_online = True
            hs_status.api_available = True
            health = client.health_check()
            models = health.get("models", [])
            if models:
                hs_status.model_loaded = models[0]

        from hyperspace.monitor import HyperspaceMonitor

        monitor = HyperspaceMonitor(client)
        node_status = monitor.get_node_status()
        if node_status:
            hs_status.peer_id = node_status.get("peer_id")
            hs_status.connected_peers = node_status.get("connected_peers")
            points = monitor.get_points_summary()
            if points:
                hs_status.points = points.get("total_points")
    except Exception:
        pass

    return SystemStatusResponse(
        database=db_status,
        hyperspace=hs_status,
        grid=grid_stats,
        uptime_seconds=round(time.time() - _start_time, 1),
        server_time=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/logs", response_model=LogsResponse)
async def get_logs(
    source: str = "api",
    lines: int = 50,
    _token: str = Depends(require_auth),
) -> LogsResponse:
    """Return recent log lines."""
    log_files = {
        "api": "/var/log/grid-api.log",
        "hyperspace": "/var/log/hyperspace.log",
        "system": "/var/log/syslog",
    }
    path = log_files.get(source, log_files["api"])
    try:
        result = subprocess.run(
            ["tail", "-n", str(min(lines, 500)), path],
            capture_output=True,
            text=True,
            timeout=5,
        )
        output_lines = result.stdout.strip().split("\n") if result.stdout else []
    except Exception:
        output_lines = [f"Could not read {path}"]

    return LogsResponse(source=source, lines=output_lines)


@router.post("/restart-hyperspace", response_model=RestartResponse)
async def restart_hyperspace(
    _token: str = Depends(require_auth),
) -> RestartResponse:
    """Restart the Hyperspace node."""
    try:
        subprocess.run(["pkill", "-f", "hyperspace"], timeout=5)
        subprocess.Popen(
            ["bash", "hyperspace_setup/start_node.sh"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return RestartResponse(status="restarting", message="Hyperspace node restarting")
    except Exception as exc:
        return RestartResponse(status="error", message=str(exc))
