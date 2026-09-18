"""Journal schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, field_validator, model_validator


class JournalEntryCreate(BaseModel):
    model_version_id: int
    inferred_state: str
    # None = UNSCORED: no confidence was ever measured. Permitted only with
    # confidence_reason (enforced by the model validator below and, at the
    # storage layer, by ck_decision_journal_unscored_has_reason). Never a
    # stand-in for a number and never to be read as 0.
    state_confidence: float | None = None
    confidence_reason: str | None = None
    transition_probability: float
    contradiction_flags: dict[str, Any] = {}
    grid_recommendation: str
    baseline_recommendation: str
    action_taken: str
    counterfactual: str
    operator_confidence: str

    @field_validator("operator_confidence")
    @classmethod
    def validate_confidence(cls, v: str) -> str:
        if v not in ("LOW", "MEDIUM", "HIGH"):
            raise ValueError("Must be LOW, MEDIUM, or HIGH")
        return v

    @model_validator(mode="after")
    def unscored_requires_reason(self) -> "JournalEntryCreate":
        if self.state_confidence is None and not (
            self.confidence_reason and self.confidence_reason.strip()
        ):
            raise ValueError(
                "state_confidence is null (unscored) - confidence_reason is "
                "required to say why. The journal is append-only; an "
                "unexplained NULL can never be annotated after the fact."
            )
        return self


class JournalOutcomeRecord(BaseModel):
    outcome_value: float
    verdict: str
    annotation: str | None = None

    @field_validator("verdict")
    @classmethod
    def validate_verdict(cls, v: str) -> str:
        if v not in ("HELPED", "HARMED", "NEUTRAL", "INSUFFICIENT_DATA"):
            raise ValueError(
                "Must be HELPED, HARMED, NEUTRAL, or INSUFFICIENT_DATA"
            )
        return v


class JournalEntryResponse(BaseModel):
    id: int
    model_version_id: int
    inferred_state: str
    # None = UNSCORED (see JournalEntryCreate.state_confidence).
    state_confidence: float | None = None
    confidence_reason: str | None = None
    transition_probability: float
    contradiction_flags: Any
    grid_recommendation: str
    baseline_recommendation: str
    action_taken: str
    counterfactual: str
    operator_confidence: str
    decision_timestamp: str
    outcome_value: float | None = None
    outcome_recorded_at: str | None = None
    verdict: str | None = None
    annotation: str | None = None

    model_config = {"from_attributes": True}


class JournalStatsResponse(BaseModel):
    total_decisions: int
    outcomes_recorded: int
    helped: int
    harmed: int
    neutral: int
    avg_outcome_value: float
    helped_rate: float
    by_state: dict[str, Any]
    by_operator_confidence: dict[str, Any]
