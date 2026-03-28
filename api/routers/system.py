"""System status and health endpoints."""

from __future__ import annotations

import os
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
    FamilyFreshness,
    FreshnessResponse,
    GridStats,
    HealthResponse,
    HermesStatusResponse,
    HermesTaskStatus,
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
    expected_threads = {"ingestion"}
    try:
        from config import settings as _settings
        if _settings.AGENTS_ENABLED and _settings.AGENTS_SCHEDULE_ENABLED:
            expected_threads.add("agent-scheduler")
    except Exception:
        pass
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

    return HealthResponse(status=status, checks=checks, degraded_reasons=degraded_reasons)


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


@router.get("/freshness", response_model=FreshnessResponse)
async def freshness(_token: str = Depends(require_auth)) -> FreshnessResponse:
    """Per-family data freshness report.

    GREEN = >80% fresh today, YELLOW = 50-80%, RED = <50%.
    """
    engine = get_db_engine()
    families: list[FamilyFreshness] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT fr.family, "
                "  COUNT(*) AS total, "
                "  COUNT(*) FILTER (WHERE rs.latest_date >= CURRENT_DATE) AS fresh_today "
                "FROM feature_registry fr "
                "LEFT JOIN LATERAL ("
                "  SELECT MAX(obs_date) AS latest_date "
                "  FROM resolved_series WHERE feature_id = fr.id"
                ") rs ON TRUE "
                "WHERE fr.model_eligible = TRUE "
                "GROUP BY fr.family ORDER BY fr.family"
            )).fetchall()
            for row in rows:
                family, total, fresh = row[0], row[1], row[2]
                stale = total - fresh
                pct = (fresh / total * 100) if total > 0 else 0
                if pct >= 80:
                    status = "GREEN"
                elif pct >= 50:
                    status = "YELLOW"
                else:
                    status = "RED"
                families.append(FamilyFreshness(
                    family=family, total=total, fresh_today=fresh,
                    stale=stale, status=status,
                ))
    except Exception as exc:
        log.warning("Freshness query failed: {e}", e=str(exc))

    # Overall status: worst family status
    if not families or any(f.status == "RED" for f in families):
        overall = "RED"
    elif any(f.status == "YELLOW" for f in families):
        overall = "YELLOW"
    else:
        overall = "GREEN"

    return FreshnessResponse(families=families, overall_status=overall)


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


# ── UX Audit endpoints ─────────────────────────────────────────

@router.post("/ux-audit")
async def trigger_ux_audit(
    _token: str = Depends(require_role("admin")),
) -> dict:
    """Trigger an immediate UX audit (admin only)."""
    try:
        from scripts.ux_auditor import run_ux_audit
        engine = get_db_engine()
        report = run_ux_audit(engine=engine)
        return {
            "status": "completed",
            "summary": report.get("summary"),
            "analysis": report.get("analysis"),
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@router.get("/ux-audits")
async def list_ux_audits(
    limit: int = 10,
    _token: str = Depends(require_auth),
) -> list[dict]:
    """List recent UX audit results."""
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, audit_timestamp, score, total_endpoints, "
                    "endpoints_ok, avg_latency_ms, journey_pass, journey_total, "
                    "priority_fix, acted_on "
                    "FROM ux_audit_results "
                    "ORDER BY audit_timestamp DESC "
                    "LIMIT :lim"
                ).bindparams(lim=min(limit, 50)),
            ).fetchall()
            return [
                {
                    "id": r[0],
                    "timestamp": r[1].isoformat() if r[1] else None,
                    "score": r[2],
                    "endpoints_ok": f"{r[4]}/{r[3]}",
                    "avg_latency_ms": r[5],
                    "journeys_pass": f"{r[6]}/{r[7]}",
                    "priority_fix": r[8],
                    "acted_on": r[9],
                }
                for r in rows
            ]
    except Exception:
        return []


@router.post("/send-digest")
async def trigger_daily_digest(
    _token: str = Depends(require_role("admin")),
) -> dict:
    """Trigger an immediate daily digest email (admin only)."""
    try:
        from scripts.daily_digest import send_daily_digest
        engine = get_db_engine()
        result = send_daily_digest(engine)
        return {"status": "sent" if result.get("sent") else "failed", **result}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@router.post("/taxonomy-audit")
async def run_taxonomy_audit_endpoint(
    _token: str = Depends(require_auth),
) -> dict:
    """Run taxonomy audit — detects misclassifications, stale data, missing features, impossible values."""
    try:
        from analysis.taxonomy_audit import run_taxonomy_audit
        engine = get_db_engine()
        return run_taxonomy_audit(engine)
    except Exception as exc:
        return {"error": str(exc)}


# ── Hermes operator status ────────────────────────────────────────

# Shared reference to the running OperatorState — set by hermes_operator
# when it starts up, so the API can read live task status.
_hermes_state: object | None = None


def set_hermes_state(state: object) -> None:
    """Called by hermes_operator to share its OperatorState with the API."""
    global _hermes_state
    _hermes_state = state


@router.get("/hermes-status", response_model=HermesStatusResponse)
async def hermes_status(_token: str = Depends(require_auth)) -> HermesStatusResponse:
    """Show what the Hermes operator has run, when, and whether it succeeded.

    Returns per-task timing, success/failure, and the current operator state
    including schedule tracking timestamps.
    """
    if _hermes_state is None:
        # Hermes not running in this process — try reading from DB snapshot
        try:
            engine = get_db_engine()
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT payload FROM analytical_snapshots "
                    "WHERE subcategory = 'hermes_operator' "
                    "ORDER BY created_at DESC LIMIT 1"
                )).fetchone()
            if row:
                import json
                payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                op_state = payload.get("operator_state", {})
                task_status_raw = op_state.get("task_status", {})
                task_status = {
                    k: HermesTaskStatus(**v) for k, v in task_status_raw.items()
                }
                return HermesStatusResponse(
                    running=False,
                    cycle_count=payload.get("cycle", 0),
                    task_status=task_status,
                    operator_state=op_state,
                    uptime_seconds=0,
                )
        except Exception as exc:
            log.debug("Could not load hermes status from DB: {e}", e=str(exc))

        return HermesStatusResponse(running=False)

    # Live state from running operator
    state = _hermes_state
    task_status_raw = getattr(state, "task_status", {})
    task_status = {
        k: HermesTaskStatus(**v) for k, v in task_status_raw.items()
    }
    return HermesStatusResponse(
        running=True,
        cycle_count=getattr(state, "cycle_count", 0),
        task_status=task_status,
        operator_state=state.to_dict() if hasattr(state, "to_dict") else {},
        uptime_seconds=round(time.time() - _start_time, 1),
    )


# ── Settings endpoints ────────────────────────────────────────────

# Secret fields that should be redacted in settings responses
_SECRET_FIELDS = {
    "DB_PASSWORD", "FRED_API_KEY", "BLS_API_KEY", "TRADINGVIEW_WEBHOOK_SECRET",
    "KOSIS_API_KEY", "COMTRADE_API_KEY", "JQUANTS_PASSWORD", "USDA_NASS_API_KEY",
    "NOAA_TOKEN", "EIA_API_KEY", "GDELT_API_KEY", "WORLDNEWS_API_KEY",
    "COINGECKO_API_KEY", "ALPHAVANTAGE_API_KEY", "TWELVEDATA_API_KEY",
    "OPENAI_API_KEY", "GRID_MASTER_PASSWORD_HASH", "GRID_JWT_SECRET",
    "POLYMARKET_API_KEY", "POLYMARKET_PRIVATE_KEY", "KALSHI_PASSWORD",
    "AGENTS_OPENAI_API_KEY", "AGENTS_ANTHROPIC_API_KEY",
    "HYPERLIQUID_PRIVATE_KEY", "ALERT_SMTP_PASSWORD",
}


@router.get("/settings")
async def get_settings(
    _token: str = Depends(require_auth),
) -> dict:
    """Return current configuration with secrets redacted."""
    from config import settings

    result: dict[str, object] = {}
    for field_name in settings.model_fields:
        val = getattr(settings, field_name, None)
        if field_name.upper() in _SECRET_FIELDS or field_name in _SECRET_FIELDS:
            result[field_name] = "***" if val else ""
        else:
            result[field_name] = val
    return {"settings": result}


@router.post("/settings")
async def update_settings(
    payload: dict,
    _token: str = Depends(require_role("admin")),
) -> dict:
    """Update configuration — writes changed values to .env file.

    Only non-secret, non-database fields may be updated via this endpoint.
    Restart is required for changes to take effect.
    """
    import re

    blocked = _SECRET_FIELDS | {"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"}
    updates = {k: v for k, v in payload.items() if k not in blocked and k != "settings"}

    if not updates:
        return {"status": "no_changes", "message": "No updatable fields provided"}

    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
    env_path = os.path.normpath(env_path)

    # Read existing .env
    existing_lines: list[str] = []
    if os.path.exists(env_path):
        with open(env_path) as f:
            existing_lines = f.readlines()

    updated_keys: set[str] = set()
    new_lines: list[str] = []

    for line in existing_lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                new_lines.append(f"{key}={updates[key]}\n")
                updated_keys.add(key)
                continue
        new_lines.append(line)

    # Append new keys that weren't in the file
    for key, val in updates.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={val}\n")
            updated_keys.add(key)

    with open(env_path, "w") as f:
        f.writelines(new_lines)

    return {
        "status": "updated",
        "updated_keys": sorted(updated_keys),
        "message": "Restart required for changes to take effect",
    }


@router.get("/api-keys")
async def get_api_keys(
    _token: str = Depends(require_auth),
) -> dict:
    """List all API keys with configured/missing status."""
    from config import settings

    # All API key fields from config
    api_key_fields = {
        "FRED_API_KEY": "FRED economic data",
        "BLS_API_KEY": "Bureau of Labor Statistics",
        "KOSIS_API_KEY": "Korean Statistical Information (KOSIS)",
        "COMTRADE_API_KEY": "UN Comtrade international trade",
        "JQUANTS_EMAIL": "J-Quants (Japan) email",
        "JQUANTS_PASSWORD": "J-Quants (Japan) password",
        "USDA_NASS_API_KEY": "USDA agricultural data",
        "NOAA_TOKEN": "NOAA weather/climate data",
        "EIA_API_KEY": "Energy Information Administration",
        "GDELT_API_KEY": "GDELT global events",
        "WORLDNEWS_API_KEY": "World News API",
        "COINGECKO_API_KEY": "CoinGecko crypto data",
        "ALPHAVANTAGE_API_KEY": "Alpha Vantage market data",
        "TWELVEDATA_API_KEY": "Twelve Data market data",
        "OPENAI_API_KEY": "OpenAI LLM (cloud)",
        "TRADINGVIEW_WEBHOOK_SECRET": "TradingView webhook",
        "POLYMARKET_API_KEY": "Polymarket prediction market",
        "POLYMARKET_PRIVATE_KEY": "Polymarket private key",
        "KALSHI_EMAIL": "Kalshi prediction market email",
        "KALSHI_PASSWORD": "Kalshi prediction market password",
        "AGENTS_OPENAI_API_KEY": "TradingAgents OpenAI key",
        "AGENTS_ANTHROPIC_API_KEY": "TradingAgents Anthropic key",
        "HYPERLIQUID_PRIVATE_KEY": "Hyperliquid perp trading",
        "ALERT_SMTP_PASSWORD": "SMTP email alerts",
    }

    keys = []
    for field, description in api_key_fields.items():
        val = getattr(settings, field, "")
        keys.append({
            "name": field,
            "description": description,
            "status": "configured" if val else "missing",
        })

    configured = sum(1 for k in keys if k["status"] == "configured")
    return {
        "keys": keys,
        "configured": configured,
        "total": len(keys),
        "summary": f"{configured} of {len(keys)} keys configured",
    }


@router.get("/services")
async def get_services(
    _token: str = Depends(require_auth),
) -> dict:
    """Check status of all GRID services."""
    import shutil

    from config import settings

    services = []

    # 1. API (this process — always online if we're responding)
    services.append({
        "name": "API",
        "status": "online",
        "uptime_seconds": round(time.time() - _start_time, 1),
    })

    # 2. Database
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        services.append({"name": "Database", "status": "online"})
    except Exception:
        services.append({"name": "Database", "status": "offline"})

    # 3. Hermes (check systemd or process)
    hermes_online = False
    try:
        result = subprocess.run(
            ["pgrep", "-f", "hermes_operator"],
            capture_output=True, text=True, timeout=3,
        )
        hermes_online = result.returncode == 0
    except Exception:
        pass
    services.append({"name": "Hermes", "status": "online" if hermes_online else "offline"})

    # 4. llama.cpp
    llamacpp_online = False
    try:
        import urllib.request
        req = urllib.request.Request(
            f"{settings.LLAMACPP_BASE_URL}/health",
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            llamacpp_online = resp.status == 200
    except Exception:
        pass
    services.append({
        "name": "LlamaCpp",
        "status": "online" if llamacpp_online else "offline",
        "url": settings.LLAMACPP_BASE_URL,
    })

    # 5. Crucix (celestial ingestion bridge)
    crucix_online = False
    try:
        result = subprocess.run(
            ["pgrep", "-f", "crucix"],
            capture_output=True, text=True, timeout=3,
        )
        crucix_online = result.returncode == 0
    except Exception:
        pass
    services.append({"name": "Crucix", "status": "online" if crucix_online else "offline"})

    # 6. Hyperspace
    hs_online = False
    try:
        from hyperspace.client import get_client
        client = get_client()
        hs_online = client.is_available
    except Exception:
        pass
    services.append({"name": "Hyperspace", "status": "online" if hs_online else "offline"})

    # 7. TAO Miner (check process)
    tao_online = False
    try:
        result = subprocess.run(
            ["pgrep", "-f", "tao_miner|bittensor"],
            capture_output=True, text=True, timeout=3,
        )
        tao_online = result.returncode == 0
    except Exception:
        pass
    services.append({"name": "TAO Miner", "status": "online" if tao_online else "offline"})

    # Disk & Memory (summary for quick access)
    resource_info = {}
    try:
        usage = shutil.disk_usage("/")
        resource_info["disk_percent"] = round(usage.used / usage.total * 100, 1)
        resource_info["disk_free_gb"] = round(usage.free / (1024**3), 1)
    except Exception:
        pass
    try:
        with open("/proc/meminfo") as f:
            meminfo: dict[str, int] = {}
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    meminfo[parts[0].rstrip(":")] = int(parts[1])
        mem_total = meminfo.get("MemTotal", 1) / (1024 * 1024)
        mem_available = meminfo.get("MemAvailable", 0) / (1024 * 1024)
        resource_info["memory_percent"] = round((mem_total - mem_available) / mem_total * 100, 1) if mem_total else 0
        resource_info["memory_total_gb"] = round(mem_total, 2)
        resource_info["memory_used_gb"] = round(mem_total - mem_available, 2)
    except Exception:
        # macOS fallback
        try:
            import psutil
            vm = psutil.virtual_memory()
            resource_info["memory_percent"] = vm.percent
            resource_info["memory_total_gb"] = round(vm.total / (1024**3), 2)
            resource_info["memory_used_gb"] = round(vm.used / (1024**3), 2)
        except Exception:
            pass

    online_count = sum(1 for s in services if s["status"] == "online")
    return {
        "services": services,
        "online": online_count,
        "total": len(services),
        "resources": resource_info,
        "start_time": datetime.fromtimestamp(_start_time, tz=timezone.utc).isoformat(),
    }


@router.get("/hermes-status")
async def get_hermes_status(
    limit: int = 20,
    _token: str = Depends(require_auth),
) -> dict:
    """Return Hermes operator task history — what ran, when, success/failure."""
    engine = get_db_engine()

    tasks: list[dict] = []
    schedule_info = {
        "cycle_interval": "5 minutes",
        "pipeline_interval": "6 hours",
        "autoresearch": "weekdays 2 AM",
        "daily_briefing": "weekdays 6 AM",
        "weekly_briefing": "Monday 7 AM",
        "data_freshness_threshold": "26 hours",
    }

    # Recent operator issues (task runs)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, created_at, category, severity, source, title, "
                    "fix_result, resolved_at, cycle_number "
                    "FROM operator_issues "
                    "ORDER BY created_at DESC "
                    "LIMIT :lim"
                ).bindparams(lim=min(limit, 100)),
            ).fetchall()
            for r in rows:
                tasks.append({
                    "id": r[0],
                    "timestamp": r[1].isoformat() if r[1] else None,
                    "category": r[2],
                    "severity": r[3],
                    "source": r[4],
                    "title": r[5],
                    "result": r[6],
                    "resolved_at": r[7].isoformat() if r[7] else None,
                    "cycle_number": r[8],
                })
    except Exception as exc:
        log.debug("Could not query operator_issues: {e}", e=str(exc))

    # Recent analytical snapshots (cycle summaries)
    snapshots: list[dict] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT snapshot_timestamp, snapshot_type, payload "
                    "FROM analytical_snapshots "
                    "WHERE snapshot_type = 'hermes_cycle' "
                    "ORDER BY snapshot_timestamp DESC "
                    "LIMIT :lim"
                ).bindparams(lim=min(limit, 20)),
            ).fetchall()
            for r in rows:
                snap = {
                    "timestamp": r[0].isoformat() if r[0] else None,
                    "type": r[1],
                }
                if r[2]:
                    payload = r[2] if isinstance(r[2], dict) else {}
                    snap["health"] = payload.get("health", {})
                    snap["actions"] = payload.get("actions_taken", [])
                    snap["issues_found"] = payload.get("issues_found", 0)
                    snap["issues_fixed"] = payload.get("issues_fixed", 0)
                snapshots.append(snap)
    except Exception as exc:
        log.debug("Could not query analytical_snapshots: {e}", e=str(exc))

    return {
        "schedule": schedule_info,
        "tasks": tasks,
        "snapshots": snapshots,
        "task_count": len(tasks),
    }
