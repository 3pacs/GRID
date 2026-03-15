"""Regime schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class RegimeDriver(BaseModel):
    feature: str
    direction: str
    magnitude: float


class RegimeCurrentResponse(BaseModel):
    state: str
    confidence: float = 0.0
    transition_probability: float = 0.0
    top_drivers: list[RegimeDriver] = []
    contradiction_flags: list[str] = []
    model_version: str = ""
    as_of: str = ""
    baseline_comparison: str = ""


class RegimeHistoryEntry(BaseModel):
    date: str
    state: str
    confidence: float


class RegimeHistoryResponse(BaseModel):
    history: list[RegimeHistoryEntry]


class RegimeTransition(BaseModel):
    date: str
    from_state: str
    to_state: str
    confidence: float


class RegimeTransitionsResponse(BaseModel):
    transitions: list[RegimeTransition]
