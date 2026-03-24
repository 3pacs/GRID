"""System status and health endpoints."""

from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth, require_role
from api.dependencies import get_db_engine
from api.schemas.system import (
    DatabaseStatus,
    GridStats,
    HealthResponse,
    HyperspaceStatus,
    LogsResponse,
    RestartResponse,
    ServerHealth,
    SystemStatusResponse,
)

router = APIRouter(prefix="/api/v1/system", tags=["system"])

_start_time = time.time()


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Health check — no auth required.

    Checks database connectivity, data freshness, connection pool,
    scheduler threads, WebSocket clients, disk space, and LLM availability.
    """
    import shutil
    import threading

    checks: dict[str, object] = {}
    degraded_reasons: list[str] = []

    # --- Database & data freshness ---
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            checks["database"] = True

            r = conn.execute(text("SELECT COUNT(*) FROM feature_registry")).fetchone()
            feature_count = r[0] if r else 0
            checks["features_registered"] = feature_count > 0
            if feature_count == 0:
                degraded_reasons.append("no features registered")

            r = conn.execute(
                text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE pull_timestamp >= NOW() - INTERVAL '7 days'"
                )
            ).fetchone()
            recent = (r[0] if r else 0) > 0
            checks["recent_data"] = recent
            if not recent:
                degraded_reasons.append("no data pulled in 7 days")

        # Connection pool details
        pool = engine.pool
        pool_ok = pool.checkedout() < pool.size() + pool.overflow()
        checks["pool_healthy"] = pool_ok
        checks["pool_size"] = pool.size()
        checks["pool_checked_out"] = pool.checkedout()
        checks["pool_overflow"] = pool.overflow()
        if not pool_ok:
            degraded_reasons.append("connection pool exhausted")
    except Exception:
        checks["database"] = False
        degraded_reasons.append("database unreachable")

    # --- Scheduler threads ---
    expected_threads = {"ingestion", "agent-scheduler"}
    live_threads = {t.name for t in threading.enumerate()}
    for name in expected_threads:
        alive = name in live_threads
        checks[f"thread_{name}"] = alive
        if not alive:
            degraded_reasons.append(f"thread '{name}' not running")

    # --- WebSocket clients ---
    try:
        from api.main import _ws_clients
        checks["ws_clients"] = len(_ws_clients)
    except Exception:
        checks["ws_clients"] = -1

    # --- Disk space ---
    try:
        usage = shutil.disk_usage("/")
        disk_pct = round(usage.used / usage.total * 100, 1)
        checks["disk_percent"] = disk_pct
        checks["disk_free_gb"] = round(usage.free / (1024**3), 1)
        if disk_pct > 95:
            degraded_reasons.append(f"disk {disk_pct}% full")
    except Exception:
        pass

    # --- LLM availability ---
    try:
        from llamacpp.client import LlamaCppClient
        client = LlamaCppClient()
        checks["llm_available"] = client.is_available
    except Exception:
        checks["llm_available"] = False

    # --- API key audit ---
    try:
        from config import settings
        key_audit = settings.audit_api_keys()
        checks["api_keys_configured"] = sum(key_audit.values())
        checks["api_keys_total"] = len(key_audit)
    except Exception:
        pass

    db_ok = checks.get("database", False)
    if db_ok and not degraded_reasons:
        status = "ok"
    elif db_ok:
        status = "degraded"
    else:
        status = "degraded"

    return HealthResponse(status=status)


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

    # Server health
    server_health = ServerHealth()
    try:
        import shutil
        import os
        import glob

        # Disk
        usage = shutil.disk_usage("/")
        server_health.disk_total_gb = round(usage.total / (1024**3), 2)
        server_health.disk_used_gb = round(usage.used / (1024**3), 2)
        server_health.disk_free_gb = round(usage.free / (1024**3), 2)
        server_health.disk_percent = round(usage.used / usage.total * 100, 1)

        # Load average
        load1, load5, load15 = os.getloadavg()
        server_health.load_avg_1m = round(load1, 2)
        server_health.load_avg_5m = round(load5, 2)
        server_health.load_avg_15m = round(load15, 2)

        # Memory from /proc/meminfo
        with open("/proc/meminfo") as f:
            meminfo: dict[str, int] = {}
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    meminfo[parts[0].rstrip(":")] = int(parts[1])
        mem_total = meminfo.get("MemTotal", 0) / (1024 * 1024)  # KB to GB
        mem_available = meminfo.get("MemAvailable", 0) / (1024 * 1024)
        server_health.memory_total_gb = round(mem_total, 2)
        server_health.memory_used_gb = round(mem_total - mem_available, 2)
        server_health.memory_percent = (
            round((mem_total - mem_available) / mem_total * 100, 1) if mem_total else 0
        )

        # CPU usage from /proc/stat (snapshot)
        with open("/proc/stat") as f:
            cpu_line = f.readline()
        vals = [int(x) for x in cpu_line.split()[1:]]
        idle = vals[3] if len(vals) > 3 else 0
        total = sum(vals)
        server_health.cpu_percent = round((1 - idle / total) * 100, 1) if total else 0

        # Temperature from thermal zones
        for tz in sorted(glob.glob("/sys/class/thermal/thermal_zone*/temp")):
            try:
                with open(tz) as f:
                    temp = int(f.read().strip()) / 1000
                if temp > 0 and server_health.cpu_temp_c is None:
                    server_health.cpu_temp_c = round(temp, 1)
            except Exception:
                pass

        # GPU temp via nvidia-smi if available
        try:
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=temperature.gpu",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=3,
            )
            if result.returncode == 0 and result.stdout.strip():
                server_health.gpu_temp_c = float(
                    result.stdout.strip().split("\n")[0]
                )
        except Exception:
            pass
    except Exception as exc:
        log.debug("Server health check failed: {e}", e=str(exc))

    return SystemStatusResponse(
        database=db_status,
        hyperspace=hs_status,
        grid=grid_stats,
        server=server_health,
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


@router.get("/alerts")
async def alerts(_token: str = Depends(require_auth)) -> dict:
    """Return active server alerts for critical conditions."""
    import shutil
    import os

    active_alerts: list[dict[str, str]] = []

    # Disk space alert
    try:
        usage = shutil.disk_usage("/")
        pct = usage.used / usage.total * 100
        if pct > 90:
            active_alerts.append({
                "severity": "critical",
                "source": "disk",
                "message": f"Disk {pct:.0f}% full — only {usage.free / (1024**3):.1f} GB free",
            })
        elif pct > 80:
            active_alerts.append({
                "severity": "warning",
                "source": "disk",
                "message": f"Disk {pct:.0f}% full",
            })
    except Exception:
        pass

    # Temperature alert
    try:
        import glob

        for tz in sorted(glob.glob("/sys/class/thermal/thermal_zone*/temp")):
            with open(tz) as f:
                temp = int(f.read().strip()) / 1000
            if temp > 85:
                active_alerts.append({
                    "severity": "critical",
                    "source": "cpu_temp",
                    "message": f"CPU temperature {temp:.0f}C — throttling likely",
                })
            elif temp > 75:
                active_alerts.append({
                    "severity": "warning",
                    "source": "cpu_temp",
                    "message": f"CPU temperature {temp:.0f}C",
                })
            break
    except Exception:
        pass

    # Memory alert
    try:
        with open("/proc/meminfo") as f:
            meminfo: dict[str, int] = {}
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    meminfo[parts[0].rstrip(":")] = int(parts[1])
        mem_total = meminfo.get("MemTotal", 1)
        mem_available = meminfo.get("MemAvailable", 0)
        mem_pct = (1 - mem_available / mem_total) * 100
        if mem_pct > 95:
            active_alerts.append({
                "severity": "critical",
                "source": "memory",
                "message": f"Memory {mem_pct:.0f}% used — OOM risk",
            })
        elif mem_pct > 85:
            active_alerts.append({
                "severity": "warning",
                "source": "memory",
                "message": f"Memory {mem_pct:.0f}% used",
            })
    except Exception:
        pass

    # Load average alert
    try:
        load1, _, _ = os.getloadavg()
        cpu_count = os.cpu_count() or 1
        if load1 > cpu_count * 2:
            active_alerts.append({
                "severity": "critical",
                "source": "load",
                "message": f"Load average {load1:.1f} (CPUs: {cpu_count})",
            })
        elif load1 > cpu_count:
            active_alerts.append({
                "severity": "warning",
                "source": "load",
                "message": f"Load average {load1:.1f} (CPUs: {cpu_count})",
            })
    except Exception:
        pass

    return {"alerts": active_alerts, "count": len(active_alerts)}


@router.post("/restart-hyperspace", response_model=RestartResponse)
async def restart_hyperspace(
    _token: str = Depends(require_role("admin")),
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
