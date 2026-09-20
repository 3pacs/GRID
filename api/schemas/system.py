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


class StaleSource(BaseModel):
    source: str
    last_pull: str | None = None
    stale: bool = True
    # Contract addition (store/availability_fields.py::FieldRecord.to_dict()).
    # Carries availability/provenance/stale_reason/ingested_at for this
    # source's freshness fact so a page's staleness text always traces back
    # to a record instead of being recomputed ad hoc. None for responses
    # built before this field existed.
    field_record: dict[str, Any] | None = None


class FreshnessResponse(BaseModel):
    families: list[FamilyFreshness]
    overall_status: str  # GREEN, YELLOW, RED
    stale_sources: list[StaleSource] = []
    # Contract addition: whole-response availability. "available" (default,
    # preserves prior behaviour) unless the underlying queries failed, in
    # which case this is "unavailable" and stale_reason names the category —
    # never silently returned as empty families/stale_sources with no signal.
    availability: str = "available"
    stale_reason: str | None = None


class HermesTaskStatus(BaseModel):
    last_run: str | None = None
    success: bool = False
    duration_s: float = 0.0
    error: str | None = None
    # True when the failure is operational (statement timeout, dropped
    # connection, a step abandoned at its budget) rather than a defect —
    # set by OperatorState.record_task. Defaults False so snapshots written
    # before the field existed still validate.
    transient: bool = False


class HermesStatusResponse(BaseModel):
    running: bool = False
    cycle_count: int = 0
    task_status: dict[str, HermesTaskStatus] = {}
    operator_state: dict[str, Any] = {}
    uptime_seconds: float = 0.0
    schedule: dict[str, Any] = {}
    tasks: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    task_count: int = 0


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
    # Contract addition (store/availability_fields.py::FieldRecord.to_dict()).
    # Restates this source's status as a per-field record: availability
    # (available/unavailable), provenance (measured, when we have a real
    # last-pull timestamp), ingested_at, and stale_reason. None for
    # responses built before this field existed.
    field_record: dict[str, Any] | None = None


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
    # Contract addition: whole-response availability. "available" (default,
    # preserves prior behaviour) unless the pipeline-health query itself
    # failed, in which case this is "unavailable" and stale_reason names the
    # failure category. Previously an exception here produced a plain 200
    # with every list empty and no signal that the *computation* — not the
    # pipeline — was what failed; a client could not tell "0 sources exist"
    # from "we could not compute this".
    availability: str = "available"
    stale_reason: str | None = None
