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


class LogsResponse(BaseModel):
    source: str
    lines: list[str]


class RestartResponse(BaseModel):
    status: str
    message: str
