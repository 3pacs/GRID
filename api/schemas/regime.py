"""Regime schemas."""

from __future__ import annotations


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
    # Additive staleness fields. ``as_of`` is the full decision timestamp;
    # ``as_of_date`` is the calendar date the reading describes and
    # ``staleness_days`` is how many days old that is, so a surface can say
    # "as of 5 months ago" instead of presenting a frozen read as current.
    as_of_date: str = ""
    staleness_days: int | None = None


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
