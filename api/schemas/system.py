"""System status schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class HyperspaceStatus(BaseModel):
    node_online: bool = False
    api_available: bool = False
    peer_id: str | None = None
    points: float | None = None
    connected_peers: int | None = None
    model_loaded: str | None = None


class DatabaseStatus(BaseModel):
    connected: bool = False
    size_mb: float = 0.0


class GridStats(BaseModel):
    features_total: int = 0
    features_model_eligible: int = 0
    hypotheses_total: int = 0
    hypotheses_in_production: int = 0
    journal_entries_total: int = 0
    journal_entries_with_outcomes: int = 0


class ServerHealth(BaseModel):
    disk_total_gb: float = 0.0
    disk_used_gb: float = 0.0
    disk_free_gb: float = 0.0
    disk_percent: float = 0.0
    cpu_percent: float = 0.0
    memory_total_gb: float = 0.0
    memory_used_gb: float = 0.0
    memory_percent: float = 0.0
    cpu_temp_c: float | None = None
    gpu_temp_c: float | None = None
    load_avg_1m: float = 0.0
    load_avg_5m: float = 0.0
    load_avg_15m: float = 0.0


class SystemStatusResponse(BaseModel):
    database: DatabaseStatus
    hyperspace: HyperspaceStatus
    grid: GridStats
    server: ServerHealth = ServerHealth()
    uptime_seconds: float
    server_time: str


class HealthResponse(BaseModel):
    status: str
    checks: dict[str, object] = {}
    degraded_reasons: list[str] = []


class LogsResponse(BaseModel):
    source: str
    lines: list[str]


class RestartResponse(BaseModel):
    status: str
    message: str


class FamilyFreshness(BaseModel):
    family: str
    total: int
    fresh_today: int
    stale: int
    status: str  # GREEN, YELLOW, RED


class FreshnessResponse(BaseModel):
    families: list[FamilyFreshness]
    overall_status: str  # GREEN, YELLOW, RED


class HermesTaskStatus(BaseModel):
    last_run: str | None = None
    success: bool = False
    duration_s: float = 0.0
    error: str | None = None


class HermesStatusResponse(BaseModel):
    running: bool = False
    cycle_count: int = 0
    task_status: dict[str, HermesTaskStatus] = {}
    operator_state: dict[str, Any] = {}
    uptime_seconds: float = 0.0


# ── Pipeline Health schemas ──────────────────────────────────────


class PipelineSourceStatus(BaseModel):
    name: str
    type: str = "unknown"
    status: str  # healthy, stale, broken
    last_pull: str | None = None
    rows_last_pull: int | None = None
    next_scheduled: str | None = None
    freshness: str = "red"  # green, yellow, red
    series_count: int | None = None
    error: str | None = None


class PipelineSummary(BaseModel):
    total_sources: int = 0
    healthy: int = 0
    stale: int = 0
    broken: int = 0


class FamilyCoverage(BaseModel):
    total: int = 0
    with_data: int = 0
    pct: float = 0.0


class ResolverStatus(BaseModel):
    pending: int = 0
    last_run: str | None = None
    last_resolved: int = 0


class PipelineError(BaseModel):
    timestamp: str | None = None
    source: str = ""
    message: str = ""


class PipelineHealthResponse(BaseModel):
    summary: PipelineSummary = PipelineSummary()
    sources: list[PipelineSourceStatus] = []
    coverage: dict[str, Any] = {}
    recent_errors: list[PipelineError] = []
    resolver_status: ResolverStatus = ResolverStatus()
