"""Model registry schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ModelFromHypothesisRequest(BaseModel):
    name: str = ""
    version: str = ""


class ModelTransitionRequest(BaseModel):
    new_state: str
    reason: str = ""


class ModelRollbackRequest(BaseModel):
    reason: str = "Operator rollback"


class ModelResponse(BaseModel):
    id: int
    name: str
    layer: str
    version: str
    state: str
    hypothesis_id: int | None = None
    feature_set: Any = None
    parameter_snapshot: Any = None
    created_at: str | None = None
    promoted_at: str | None = None
    promoted_by: str | None = None
    retired_at: str | None = None
    retire_reason: str | None = None

    model_config = {"from_attributes": True}


class ProductionModelsResponse(BaseModel):
    models: dict[str, ModelResponse | None]
