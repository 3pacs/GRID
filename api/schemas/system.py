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


class SystemStatusResponse(BaseModel):
    database: DatabaseStatus
    hyperspace: HyperspaceStatus
    grid: GridStats
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
