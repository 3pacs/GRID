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
    FamilyCoverage,
    FamilyFreshness,
    FreshnessResponse,
    GridStats,
    HealthResponse,
    HermesStatusResponse,
    HermesTaskStatus,
    HyperspaceStatus,
    LogsResponse,
    PipelineError,
    PipelineHealthResponse,
    PipelineSourceStatus,
    PipelineSummary,
    ResolverStatus,
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
    except Exception as exc:
        log.warning("Health: database connectivity check failed: {e}", e=str(exc))
        checks["database"] = False
        degraded_reasons.append("database unreachable")

    # --- Scheduler threads ---
    expected_threads = {"ingestion"}
    try:
        from config import settings as _settings
        if _settings.AGENTS_ENABLED and _settings.AGENTS_SCHEDULE_ENABLED:
            expected_threads.add("agent-scheduler")
    except Exception as exc:
        log.debug("Health: could not load settings for thread check: {e}", e=str(exc))
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
    except Exception as exc:
        log.debug("Health: could not read ws_clients: {e}", e=str(exc))
        checks["ws_clients"] = -1

    # --- Disk space ---
    try:
        usage = shutil.disk_usage("/")
        disk_pct = round(usage.used / usage.total * 100, 1)
        checks["disk_percent"] = disk_pct
        checks["disk_free_gb"] = round(usage.free / (1024**3), 1)
        if disk_pct > 95:
            degraded_reasons.append(f"disk {disk_pct}% full")
    except Exception as exc:
        log.debug("Health: disk usage check failed: {e}", e=str(exc))

    # --- LLM availability ---
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.LOCAL)
        checks["llm_available"] = client.is_available
    except Exception as exc:
        log.debug("Health: LLM availability check failed: {e}", e=str(exc))
        checks["llm_available"] = False

    # --- API key audit ---
    try:
        from config import settings
        key_audit = settings.audit_api_keys()
        checks["api_keys_configured"] = sum(key_audit.values())
        checks["api_keys_total"] = len(key_audit)
    except Exception as exc:
        log.debug("Health: API key audit failed: {e}", e=str(exc))

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
    except Exception as exc:
        log.debug("System: hyperspace status check failed: {e}", e=str(exc))

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
            except Exception as exc:
                log.debug("System: thermal zone read failed for {tz}: {e}", tz=tz, e=str(exc))

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
        except Exception as exc:
            log.debug("System: nvidia-smi GPU temp failed: {e}", e=str(exc))
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

    # Add blacklisted/stale source info for frontend staleness indicators
    stale_sources: list[dict] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT sc.name, sc.last_pull_at "
                "FROM source_catalog sc "
                "WHERE sc.last_pull_at < NOW() - INTERVAL '48 hours' "
                "OR sc.last_pull_at IS NULL "
                "ORDER BY sc.last_pull_at ASC NULLS FIRST "
                "LIMIT 20"
            )).fetchall()
            stale_sources = [
                {
                    "source": r[0],
                    "last_pull": r[1].isoformat() if r[1] else None,
                    "stale": True,
                }
                for r in rows
            ]
    except Exception as exc:
        log.debug("System: stale sources query failed: {e}", e=str(exc))

    resp = FreshnessResponse(families=families, overall_status=overall)
    # Attach stale sources as extra field (not in the pydantic model,
    # but FastAPI will include it in the response)
    resp_dict = resp.dict()
    resp_dict["stale_sources"] = stale_sources
    return resp_dict


# ── Source type classification ─────────────────────────────────────

_SOURCE_TYPE_MAP: dict[str, str] = {
    "FRED": "macro", "BLS": "macro", "ECB_SDW": "macro", "BCB_BR": "macro",
    "MAS_SG": "macro", "AKShare": "macro", "IMF_IFS": "macro", "OECD_SDMX": "macro",
    "BIS": "macro", "RBI": "macro", "ABS_AU": "macro", "KOSIS": "macro",
    "Eurostat": "macro", "DBnomics": "macro", "NYFed": "macro", "OFR": "macro",
    "Fed_Liquidity": "macro",
    "JQuants": "market", "EDINET": "market", "yfinance": "market",
    "ETF_Flows": "flows", "SEC_13F": "flows", "DarkPool": "flows",
    "Unusual_Whales": "flows",
    "WorldNews": "sentiment", "GDELT": "sentiment", "OppInsights": "sentiment",
    "alphavantage_news_sentiment": "sentiment", "hf_financial_news": "sentiment",
    "Smart_Money": "sentiment",
    "Congress_Trading": "altdata", "SEC_Insider": "altdata",
    "Prediction_Odds": "altdata", "Supply_Chain": "altdata",
    "Comtrade": "trade", "CEPII_BACI": "trade", "Atlas_ECI": "trade",
    "WIOD": "trade",
    "NOAA_AIS": "physical", "VIIRS": "physical", "USDA_NASS": "physical",
    "EU_KLEMS": "physical", "USPTO_PV": "physical",
}

# Schedule frequency determines staleness thresholds (hours)
_SOURCE_SCHEDULE: dict[str, tuple[str, int]] = {
    # (schedule_group, stale_threshold_hours)
    # daily sources: stale after 48h
    "ECB_SDW": ("daily", 48), "BCB_BR": ("daily", 48), "MAS_SG": ("daily", 48),
    "AKShare": ("daily", 48), "OppInsights": ("daily", 48), "GDELT": ("daily", 48),
    "WorldNews": ("daily", 48), "OFR": ("daily", 48), "JQuants": ("daily", 48),
    "EDINET": ("daily", 48), "alphavantage_news_sentiment": ("daily", 48),
    "NYFed": ("daily", 48), "Congress_Trading": ("daily", 48),
    "SEC_Insider": ("daily", 48), "Unusual_Whales": ("daily", 48),
    "Prediction_Odds": ("daily", 48), "Smart_Money": ("daily", 48),
    "Fed_Liquidity": ("daily", 48), "ETF_Flows": ("daily", 48),
    # weekly sources: stale after 10 days
    "OECD_SDMX": ("weekly", 240), "BIS": ("weekly", 240), "IMF_IFS": ("weekly", 240),
    "RBI": ("weekly", 240), "ABS_AU": ("weekly", 240), "KOSIS": ("weekly", 240),
    "USDA_NASS": ("weekly", 240), "DBnomics": ("weekly", 240),
    "hf_financial_news": ("weekly", 240), "DarkPool": ("weekly", 240),
    "Supply_Chain": ("weekly", 240), "SEC_13F": ("weekly", 240),
    # monthly sources: stale after 45 days
    "Comtrade": ("monthly", 1080), "Eurostat": ("monthly", 1080),
    "NOAA_AIS": ("monthly", 1080), "VIIRS": ("monthly", 1080),
    "CEPII_BACI": ("monthly", 1080),
    # annual sources: stale after 400 days
    "Atlas_ECI": ("annual", 9600), "WIOD": ("annual", 9600),
    "EU_KLEMS": ("annual", 9600), "USPTO_PV": ("annual", 9600),
}


@router.get("/pipeline-health", response_model=PipelineHealthResponse)
async def pipeline_health(
    _token: str = Depends(require_auth),
) -> PipelineHealthResponse:
    """Comprehensive pipeline health view.

    Shows per-source status, coverage by family, recent errors,
    and resolver throughput — everything an operator needs to see
    what is flowing, stale, or broken.
    """
    engine = get_db_engine()

    sources: list[PipelineSourceStatus] = []
    coverage: dict[str, dict] = {}
    recent_errors: list[PipelineError] = []
    resolver = ResolverStatus()

    try:
        with engine.connect() as conn:
            # ── Per-source pull status ─────────────────────────────────
            source_rows = conn.execute(text(
                "SELECT sc.name, "
                "  MAX(rs.pull_timestamp) AS last_pull, "
                "  COUNT(*) FILTER (WHERE rs.pull_timestamp >= NOW() - INTERVAL '48 hours') AS recent_rows, "
                "  COUNT(DISTINCT rs.series_key) AS series_count "
                "FROM source_catalog sc "
                "LEFT JOIN raw_series rs ON rs.source_id = sc.id "
                "GROUP BY sc.name "
                "ORDER BY sc.name"
            )).fetchall()

            for row in source_rows:
                src_name = row[0]
                last_pull = row[1]
                recent_rows = row[2]
                series_count = row[3]

                src_type = _SOURCE_TYPE_MAP.get(src_name, "unknown")
                schedule_info = _SOURCE_SCHEDULE.get(src_name)
                stale_hours = schedule_info[1] if schedule_info else 168  # default 7 days

                # Determine status and freshness
                if last_pull is None:
                    status = "broken"
                    freshness = "red"
                else:
                    from datetime import timedelta as _td

                    age = datetime.now(timezone.utc) - last_pull.replace(
                        tzinfo=timezone.utc
                    ) if last_pull.tzinfo is None else datetime.now(timezone.utc) - last_pull
                    age_hours = age.total_seconds() / 3600

                    if age_hours <= stale_hours:
                        status = "healthy"
                        freshness = "green"
                    elif age_hours <= stale_hours * 2:
                        status = "stale"
                        freshness = "yellow"
                    else:
                        status = "broken"
                        freshness = "red"

                # Compute next_scheduled (approximate)
                next_scheduled = None
                if schedule_info and last_pull:
                    freq = schedule_info[0]
                    from datetime import timedelta

                    delta_map = {
                        "daily": timedelta(days=1),
                        "weekly": timedelta(weeks=1),
                        "monthly": timedelta(days=30),
                        "annual": timedelta(days=365),
                    }
                    delta = delta_map.get(freq, timedelta(days=1))
                    next_dt = last_pull + delta
                    # Ensure tzinfo
                    if next_dt.tzinfo is None:
                        next_dt = next_dt.replace(tzinfo=timezone.utc)
                    next_scheduled = next_dt.isoformat()

                sources.append(PipelineSourceStatus(
                    name=src_name,
                    type=src_type,
                    status=status,
                    last_pull=last_pull.isoformat() if last_pull else None,
                    rows_last_pull=recent_rows,
                    next_scheduled=next_scheduled,
                    freshness=freshness,
                    series_count=series_count,
                ))

            # ── Coverage by family ─────────────────────────────────────
            cov_rows = conn.execute(text(
                "SELECT fr.family, "
                "  COUNT(*) AS total, "
                "  COUNT(*) FILTER (WHERE rs.has_data) AS with_data "
                "FROM feature_registry fr "
                "LEFT JOIN LATERAL ("
                "  SELECT EXISTS("
                "    SELECT 1 FROM resolved_series WHERE feature_id = fr.id LIMIT 1"
                "  ) AS has_data"
                ") rs ON TRUE "
                "WHERE fr.model_eligible = TRUE "
                "GROUP BY fr.family ORDER BY fr.family"
            )).fetchall()

            by_family: dict[str, dict] = {}
            for row in cov_rows:
                family, total, with_data = row[0], row[1], row[2]
                pct = round(with_data / total * 100, 1) if total > 0 else 0.0
                by_family[family] = {
                    "total": total,
                    "with_data": with_data,
                    "pct": pct,
                }
            coverage = {"by_family": by_family}

            # ── Recent errors from server_log ──────────────────────────
            try:
                err_rows = conn.execute(text(
                    "SELECT created_at, source, message "
                    "FROM server_log "
                    "WHERE level IN ('ERROR', 'CRITICAL') "
                    "ORDER BY created_at DESC "
                    "LIMIT 25"
                )).fetchall()
                for row in err_rows:
                    recent_errors.append(PipelineError(
                        timestamp=row[0].isoformat() if row[0] else None,
                        source=row[1] or "",
                        message=row[2] or "",
                    ))
            except Exception as exc:
                log.debug("Pipeline: server_log table unavailable: {e}", e=str(exc))

            # ── Resolver status ────────────────────────────────────────
            try:
                r = conn.execute(text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE pull_status = 'SUCCESS' "
                    "AND series_key NOT IN (SELECT DISTINCT series_key FROM resolved_series)"
                )).fetchone()
                resolver.pending = r[0] if r else 0

                r = conn.execute(text(
                    "SELECT MAX(resolved_at) FROM resolved_series"
                )).fetchone()
                if r and r[0]:
                    resolver.last_run = r[0].isoformat()

                r = conn.execute(text(
                    "SELECT COUNT(*) FROM resolved_series "
                    "WHERE resolved_at >= NOW() - INTERVAL '24 hours'"
                )).fetchone()
                resolver.last_resolved = r[0] if r else 0
            except Exception as exc:
                log.debug("Pipeline: resolver status query failed: {e}", e=str(exc))

    except Exception as exc:
        log.warning("Pipeline health query failed: {e}", e=str(exc))

    # ── Build summary ──────────────────────────────────────────────
    healthy = sum(1 for s in sources if s.status == "healthy")
    stale = sum(1 for s in sources if s.status == "stale")
    broken = sum(1 for s in sources if s.status == "broken")
    summary = PipelineSummary(
        total_sources=len(sources),
        healthy=healthy,
        stale=stale,
        broken=broken,
    )

    return PipelineHealthResponse(
        summary=summary,
        sources=sources,
        coverage=coverage,
        recent_errors=recent_errors,
        resolver_status=resolver,
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
    except Exception as exc:
        log.warning("System: log read failed for {p}: {e}", p=path, e=str(exc))
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
    except Exception as exc:
        log.debug("Alerts: disk usage check failed: {e}", e=str(exc))

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
    except Exception as exc:
        log.debug("Alerts: CPU temperature check failed: {e}", e=str(exc))

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
    except Exception as exc:
        log.debug("Alerts: memory check failed: {e}", e=str(exc))

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
    except Exception as exc:
        log.debug("Alerts: load average check failed: {e}", e=str(exc))

    return {"alerts": active_alerts, "count": len(active_alerts)}


@router.post("/restart-hyperspace", response_model=RestartResponse)
async def restart_hyperspace(
    _token: str = Depends(require_role("admin")),
) -> RestartResponse:
    """Restart the Hyperspace node."""
    try:
        subprocess.run(["pkill", "-f", "hyperspace"], timeout=5)
        from pathlib import Path
        _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
        _HYPERSPACE_SCRIPT = _PROJECT_ROOT / "hyperspace_setup" / "start_node.sh"
        if not _HYPERSPACE_SCRIPT.is_file():
            return RestartResponse(status="error", message="Hyperspace start script not found")
        subprocess.Popen(
            ["bash", str(_HYPERSPACE_SCRIPT)],
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
    except Exception as exc:
        log.warning("System: UX issues query failed: {e}", e=str(exc))
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
                    "SELECT payload, created_at FROM analytical_snapshots "
                    "WHERE subcategory = 'hermes_operator' "
                    "ORDER BY created_at DESC LIMIT 1"
                )).fetchone()

                # Check LLM task heartbeat (recent completions = Hermes alive)
                tq_row = conn.execute(text(
                    "SELECT created_at FROM analytical_snapshots "
                    "WHERE category LIKE 'llm_task_%%' "
                    "ORDER BY created_at DESC LIMIT 1"
                )).fetchone()

            if row:
                import json
                payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                snapshot_time = row[1]
                op_state = payload.get("operator_state", {})
                task_status_raw = op_state.get("task_status", {})
                task_status = {
                    k: HermesTaskStatus(**v) for k, v in task_status_raw.items()
                }
                # Hermes is "running" if last snapshot is < 10 minutes old
                is_running = False
                from datetime import timezone as tz
                if snapshot_time:
                    if hasattr(snapshot_time, "tzinfo") and snapshot_time.tzinfo is None:
                        snapshot_time = snapshot_time.replace(tzinfo=tz.utc)
                    age_seconds = (datetime.now(tz.utc) - snapshot_time).total_seconds()
                    is_running = age_seconds < 600  # 10 min
                # Also check for recent LLM task completions as a heartbeat
                if not is_running and tq_row and tq_row[0]:
                    tq_time = tq_row[0]
                    if hasattr(tq_time, "tzinfo") and tq_time.tzinfo is None:
                        tq_time = tq_time.replace(tzinfo=tz.utc)
                    tq_age = (datetime.now(tz.utc) - tq_time).total_seconds()
                    is_running = tq_age < 300  # 5 min
                return HermesStatusResponse(
                    running=is_running,
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
    except Exception as exc:
        log.debug("Services: database ping failed: {e}", e=str(exc))
        services.append({"name": "Database", "status": "offline"})

    # 3. Hermes (check systemd or process)
    hermes_online = False
    try:
        result = subprocess.run(
            ["pgrep", "-f", "hermes_operator"],
            capture_output=True, text=True, timeout=3,
        )
        hermes_online = result.returncode == 0
    except Exception as exc:
        log.debug("Services: Hermes process check failed: {e}", e=str(exc))
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
    except Exception as exc:
        log.debug("Services: llamacpp health check failed: {e}", e=str(exc))
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
    except Exception as exc:
        log.debug("Services: Crucix process check failed: {e}", e=str(exc))
    services.append({"name": "Crucix", "status": "online" if crucix_online else "offline"})

    # 6. Hyperspace
    hs_online = False
    try:
        from hyperspace.client import get_client
        client = get_client()
        hs_online = client.is_available
    except Exception as exc:
        log.debug("Services: Hyperspace check failed: {e}", e=str(exc))
    services.append({"name": "Hyperspace", "status": "online" if hs_online else "offline"})

    # 7. TAO Miner (check process)
    tao_online = False
    try:
        result = subprocess.run(
            ["pgrep", "-f", "tao_miner|bittensor"],
            capture_output=True, text=True, timeout=3,
        )
        tao_online = result.returncode == 0
    except Exception as exc:
        log.debug("Services: TAO Miner process check failed: {e}", e=str(exc))
    services.append({"name": "TAO Miner", "status": "online" if tao_online else "offline"})

    # Disk & Memory (summary for quick access)
    resource_info = {}
    try:
        usage = shutil.disk_usage("/")
        resource_info["disk_percent"] = round(usage.used / usage.total * 100, 1)
        resource_info["disk_free_gb"] = round(usage.free / (1024**3), 1)
    except Exception as exc:
        log.debug("Services: disk usage check failed: {e}", e=str(exc))
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
    except Exception as exc:
        log.debug("Services: /proc/meminfo read failed, trying psutil fallback: {e}", e=str(exc))
        try:
            import psutil
            vm = psutil.virtual_memory()
            resource_info["memory_percent"] = vm.percent
            resource_info["memory_total_gb"] = round(vm.total / (1024**3), 2)
            resource_info["memory_used_gb"] = round(vm.used / (1024**3), 2)
        except Exception as exc2:
            log.debug("Services: psutil memory check also failed: {e}", e=str(exc2))

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


# ── Architecture introspection ────────────────────────────────────


def _count_files(directory: str, extensions: tuple[str, ...] = (".py",)) -> int:
    """Count files with given extensions in a directory tree."""
    from pathlib import Path

    base = Path(__file__).parent.parent.parent / directory
    if not base.exists():
        return 0
    count = 0
    for ext in extensions:
        count += len(list(base.rglob(f"*{ext}")))
    return count


def _list_view_files() -> list[str]:
    """List .jsx view files from pwa/src/views/."""
    from pathlib import Path

    views_dir = Path(__file__).parent.parent.parent / "pwa" / "src" / "views"
    if not views_dir.exists():
        return []
    return sorted([f.stem for f in views_dir.glob("*.jsx")])


def _count_routes(app_instance) -> int:
    """Count all registered API routes."""
    try:
        return len([r for r in app_instance.routes if hasattr(r, "methods")])
    except Exception as exc:
        log.debug("System: route count failed: {e}", e=str(exc))
        return 0


def _count_test_files() -> int:
    """Count test files in tests/."""
    return _count_files("tests", (".py",))


def _get_puller_stats(engine) -> list[dict]:
    """Query source_catalog for puller stats."""
    pullers = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT sc.name, "
                "  COUNT(rs.id) AS row_count, "
                "  MAX(rs.pull_timestamp) AS last_pull, "
                "  COUNT(DISTINCT rs.series_key) AS series_count "
                "FROM source_catalog sc "
                "LEFT JOIN raw_series rs ON rs.source_id = sc.id "
                "GROUP BY sc.name "
                "ORDER BY sc.name"
            )).fetchall()
            for row in rows:
                name = row[0]
                row_count = row[1] or 0
                last_pull = row[2]
                series_count = row[3] or 0

                # Determine status
                if last_pull is None:
                    status = "new"
                    last_run = None
                else:
                    from datetime import timedelta

                    lp = last_pull.replace(tzinfo=timezone.utc) if last_pull.tzinfo is None else last_pull
                    age = datetime.now(timezone.utc) - lp
                    age_hours = age.total_seconds() / 3600

                    schedule_info = _SOURCE_SCHEDULE.get(name)
                    stale_hours = schedule_info[1] if schedule_info else 168

                    if age_hours <= stale_hours:
                        status = "healthy"
                    elif age_hours <= stale_hours * 2:
                        status = "stale"
                    else:
                        status = "broken"

                    # Human-readable last run
                    if age_hours < 1:
                        last_run = f"{int(age.total_seconds() / 60)}m ago"
                    elif age_hours < 48:
                        last_run = f"{int(age_hours)}h ago"
                    else:
                        last_run = f"{int(age_hours / 24)}d ago"

                pullers.append({
                    "id": name.lower().replace(" ", "_"),
                    "label": f"{name} ({series_count} series)" if series_count else name,
                    "type": "puller",
                    "status": status,
                    "last_run": last_run,
                    "rows": row_count,
                    "source_type": _SOURCE_TYPE_MAP.get(name, "unknown"),
                })
    except Exception as exc:
        log.warning("Puller stats query failed: {e}", e=str(exc))
    return pullers


def _get_feature_count(engine) -> int:
    """Query feature_registry for total feature count."""
    try:
        with engine.connect() as conn:
            r = conn.execute(text("SELECT COUNT(*) FROM feature_registry")).fetchone()
            return r[0] if r else 0
    except Exception as exc:
        log.debug("System: feature_registry count failed: {e}", e=str(exc))
        return 0


def _get_resolved_count(engine) -> int:
    """Query resolved_series for row count."""
    try:
        with engine.connect() as conn:
            r = conn.execute(text(
                "SELECT reltuples::bigint FROM pg_class WHERE relname = 'resolved_series'"
            )).fetchone()
            return r[0] if r and r[0] > 0 else 0
    except Exception as exc:
        log.debug("System: resolved_series count failed: {e}", e=str(exc))
        return 0


def _get_raw_count(engine) -> int:
    """Query raw_series for approximate row count."""
    try:
        with engine.connect() as conn:
            r = conn.execute(text(
                "SELECT reltuples::bigint FROM pg_class WHERE relname = 'raw_series'"
            )).fetchone()
            return r[0] if r and r[0] > 0 else 0
    except Exception as exc:
        log.debug("System: raw_series count failed: {e}", e=str(exc))
        return 0


@router.get("/architecture")
async def architecture(_token: str = Depends(require_auth)) -> dict:
    """System architecture introspection.

    Returns a complete map of all GRID modules, data flows, connections,
    and health status -- a meta-view of the running system.
    """
    engine = get_db_engine()

    # Gather puller stats from DB
    puller_nodes = _get_puller_stats(engine)
    feature_count = _get_feature_count(engine)
    resolved_count = _get_resolved_count(engine)
    raw_count = _get_raw_count(engine)

    # Count routes from the running app
    try:
        from api.main import app as _app
        api_endpoint_count = _count_routes(_app)
    except Exception as exc:
        log.debug("System: could not import app for route count: {e}", e=str(exc))
        api_endpoint_count = 0

    # List frontend views
    view_files = _list_view_files()
    view_nodes = [
        {"id": v.lower(), "label": v, "type": "view"}
        for v in view_files
    ]

    # Count test files
    test_count = _count_test_files()

    # Build modules structure
    modules = [
        {
            "id": "ingestion",
            "label": "Data Ingestion",
            "type": "layer",
            "children": puller_nodes if puller_nodes else [
                {"id": "fred", "label": "FRED (35 series)", "type": "puller", "status": "unknown"},
                {"id": "yfinance", "label": "yFinance (50 tickers)", "type": "puller", "status": "unknown"},
                {"id": "ecb_sdw", "label": "ECB SDW", "type": "puller", "status": "unknown"},
                {"id": "bcb_br", "label": "BCB Brazil", "type": "puller", "status": "unknown"},
                {"id": "congressional", "label": "Congressional Trades", "type": "puller", "status": "unknown"},
                {"id": "sec_insider", "label": "SEC Insider", "type": "puller", "status": "unknown"},
                {"id": "dark_pool", "label": "Dark Pool", "type": "puller", "status": "unknown"},
                {"id": "unusual_whales", "label": "Unusual Whales", "type": "puller", "status": "unknown"},
                {"id": "prediction_odds", "label": "Polymarket", "type": "puller", "status": "unknown"},
                {"id": "gdelt", "label": "GDELT", "type": "puller", "status": "unknown"},
            ],
        },
        {
            "id": "normalization",
            "label": "Normalization",
            "type": "layer",
            "children": [
                {"id": "resolver", "label": "Conflict Resolver", "type": "processor",
                 "status": "healthy" if resolved_count > 0 else "new"},
                {"id": "entity_map", "label": f"Entity Map", "type": "processor",
                 "status": "healthy"},
            ],
        },
        {
            "id": "store",
            "label": "PIT Store",
            "type": "layer",
            "children": [
                {"id": "pit_engine", "label": f"PIT Query Engine ({resolved_count:,} rows)", "type": "engine",
                 "status": "healthy" if resolved_count > 0 else "new",
                 "rows": resolved_count},
                {"id": "feature_registry", "label": f"Feature Registry ({feature_count:,} features)", "type": "engine",
                 "status": "healthy" if feature_count > 0 else "new",
                 "rows": feature_count},
                {"id": "raw_store", "label": f"Raw Series ({raw_count:,} rows)", "type": "engine",
                 "status": "healthy" if raw_count > 0 else "new",
                 "rows": raw_count},
            ],
        },
        {
            "id": "intelligence",
            "label": "Intelligence",
            "type": "layer",
            "children": [
                {"id": "trust_scorer", "label": "Trust Scoring", "type": "engine", "status": "healthy"},
                {"id": "cross_reference", "label": "Cross-Reference", "type": "engine", "status": "healthy"},
                {"id": "sleuth", "label": "Sleuth (Investigator)", "type": "engine", "status": "healthy"},
                {"id": "lever_pullers", "label": "Lever Pullers", "type": "engine", "status": "healthy"},
                {"id": "actor_network", "label": "Actor Network", "type": "engine", "status": "healthy"},
                {"id": "source_audit", "label": "Source Audit", "type": "engine", "status": "healthy"},
                {"id": "postmortem", "label": "Postmortem", "type": "engine", "status": "healthy"},
                {"id": "thesis_tracker", "label": "Thesis Tracker", "type": "engine", "status": "healthy"},
                {"id": "trend_tracker", "label": "Trend Tracker", "type": "engine", "status": "healthy"},
            ],
        },
        {
            "id": "trading",
            "label": "Trading",
            "type": "layer",
            "children": [
                {"id": "recommender", "label": "Options Recommender", "type": "engine", "status": "healthy"},
                {"id": "tracker", "label": "Outcome Tracker", "type": "engine", "status": "healthy"},
                {"id": "paper_engine", "label": "Paper Trading", "type": "engine", "status": "healthy"},
                {"id": "signal_executor", "label": "Signal Executor", "type": "engine", "status": "healthy"},
            ],
        },
        {
            "id": "frontend",
            "label": "Frontend Views",
            "type": "layer",
            "children": view_nodes,
        },
    ]

    # Data flows between modules
    data_flows = [
        # Ingestion -> Normalization
        {"from": "ingestion", "to": "resolver", "label": "raw_series", "color": "#22C55E"},
        # Normalization -> Store
        {"from": "resolver", "to": "pit_engine", "label": "resolved_series", "color": "#22C55E"},
        {"from": "entity_map", "to": "resolver", "label": "entity_mapping", "color": "#3B82F6"},
        # Store -> Intelligence
        {"from": "pit_engine", "to": "trust_scorer", "label": "pit_queries", "color": "#3B82F6"},
        {"from": "pit_engine", "to": "cross_reference", "label": "macro_vs_physical", "color": "#3B82F6"},
        {"from": "pit_engine", "to": "lever_pullers", "label": "actor_signals", "color": "#8B5CF6"},
        {"from": "pit_engine", "to": "actor_network", "label": "wealth_flows", "color": "#8B5CF6"},
        {"from": "pit_engine", "to": "sleuth", "label": "investigation_data", "color": "#8B5CF6"},
        {"from": "pit_engine", "to": "trend_tracker", "label": "trend_data", "color": "#3B82F6"},
        # Intelligence -> Trading
        {"from": "trust_scorer", "to": "recommender", "label": "convergence", "color": "#F59E0B"},
        {"from": "lever_pullers", "to": "recommender", "label": "actor_signals", "color": "#F59E0B"},
        {"from": "cross_reference", "to": "recommender", "label": "reality_check", "color": "#F59E0B"},
        # Trading -> Frontend
        {"from": "recommender", "to": "dashboard", "label": "recommendations", "color": "#F59E0B"},
        {"from": "tracker", "to": "dashboard", "label": "outcomes", "color": "#F59E0B"},
        {"from": "paper_engine", "to": "dashboard", "label": "paper_trades", "color": "#F59E0B"},
        # Intelligence -> Frontend
        {"from": "trust_scorer", "to": "inteldashboard", "label": "trust_scores", "color": "#8B5CF6"},
        {"from": "cross_reference", "to": "crossreference", "label": "lie_detector", "color": "#8B5CF6"},
        {"from": "actor_network", "to": "actornetwork", "label": "power_map", "color": "#8B5CF6"},
        {"from": "trend_tracker", "to": "trendtracker", "label": "trends", "color": "#3B82F6"},
        # Store -> Frontend
        {"from": "pit_engine", "to": "moneyflow", "label": "capital_flows", "color": "#22C55E"},
        {"from": "pit_engine", "to": "globeview", "label": "global_data", "color": "#22C55E"},
        {"from": "feature_registry", "to": "signals", "label": "features", "color": "#3B82F6"},
    ]

    # Aggregate stats
    total_pullers = len(puller_nodes)
    total_modules = sum(len(m.get("children", [])) for m in modules)

    stats = {
        "total_modules": total_modules,
        "total_pullers": total_pullers,
        "total_features": feature_count,
        "total_resolved": resolved_count,
        "total_raw": raw_count,
        "api_endpoints": api_endpoint_count,
        "frontend_views": len(view_files),
        "tests": test_count,
    }

    # Detect gaps: modules with no data
    gaps = []
    for m in modules:
        for child in m.get("children", []):
            if child.get("status") in ("new", "broken"):
                gaps.append({
                    "module": child["id"],
                    "label": child["label"],
                    "layer": m["id"],
                    "status": child.get("status"),
                })

    return {
        "modules": modules,
        "data_flows": data_flows,
        "stats": stats,
        "gaps": gaps,
    }


# ---------------------------------------------------------------------------
# Resolution audit endpoints
# ---------------------------------------------------------------------------


@router.get("/resolution-audit")
async def get_resolution_audit(_token: str = Depends(require_auth)) -> dict:
    """Return the latest resolution audit findings from the database."""
    engine = get_db_engine()
    try:
        from intelligence.resolution_audit import get_latest_audit_results
        return get_latest_audit_results(engine, limit=200)
    except Exception as exc:
        log.error("Failed to load resolution audit results: {e}", e=str(exc))
        return {"findings": [], "summary": {}, "error": str(exc)}


@router.post("/resolution-audit/run")
async def run_resolution_audit(_token: str = Depends(require_auth)) -> dict:
    """Trigger a full resolution audit and return results."""
    engine = get_db_engine()
    try:
        from intelligence.resolution_audit import run_full_audit
        return run_full_audit(engine)
    except Exception as exc:
        log.error("Resolution audit failed: {e}", e=str(exc))
        return {"findings": [], "summary": {}, "error": str(exc)}
