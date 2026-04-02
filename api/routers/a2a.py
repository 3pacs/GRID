"""A2A Protocol endpoints — Agent Card discovery and task management."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from loguru import logger as log

from api.auth import require_auth

router = APIRouter(tags=["a2a"])


# ── Request / Response Models ─────────────────────────────────────────────

class TaskSubmitRequest(BaseModel):
    """A2A task submission from an external agent."""

    id: str | None = Field(None, description="Pre-assigned task ID")
    skill_id: str = Field(..., description="Skill to invoke")
    input: dict[str, str] = Field(..., description="Input with 'text' key")
    metadata: dict[str, Any] = Field(default_factory=dict)


class TaskResponse(BaseModel):
    """A2A task status response."""

    id: str
    state: str
    skill_id: str
    input: dict[str, str]
    output: dict[str, str] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── GET /.well-known/agent.json ───────────────────────────────────────────

@router.get("/.well-known/agent.json")
async def get_agent_card() -> dict[str, Any]:
    """Serve the GRID A2A Agent Card for agent discovery.

    This is the standard A2A discovery endpoint. External agents
    fetch this to learn GRID's capabilities, supported skills,
    and authentication requirements.
    """
    try:
        from config import settings
        from a2a.agent_card import build_grid_agent_card

        card = build_grid_agent_card(settings.A2A_BASE_URL)
        return card.to_dict()
    except Exception as exc:
        log.warning("Failed to build Agent Card: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail="Agent Card generation failed")


# ── POST /a2a/tasks ───────────────────────────────────────────────────────

@router.post("/a2a/tasks", response_model=TaskResponse)
async def submit_task(
    req: TaskSubmitRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Accept a task submission from an external agent.

    The task is processed synchronously if a handler is registered,
    or queued for async processing otherwise.
    """
    from a2a.server import A2ATaskManager

    manager = _get_task_manager()
    input_text = req.input.get("text", "")
    if not input_text:
        raise HTTPException(status_code=400, detail="Input must contain 'text' field")

    task = manager.submit_task(
        skill_id=req.skill_id,
        input_text=input_text,
        task_id=req.id,
        metadata=req.metadata,
    )

    return {
        "id": task.id,
        "state": task.state.value,
        "skill_id": task.skill_id,
        "input": {"text": task.input_text},
        "output": {"text": task.output} if task.output else None,
        "metadata": task.metadata,
    }


# ── GET /a2a/tasks/{task_id} ─────────────────────────────────────────────

@router.get("/a2a/tasks/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get the status/result of a previously submitted task."""
    manager = _get_task_manager()
    task = manager.get_task(task_id)

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "id": task.id,
        "state": task.state.value,
        "skill_id": task.skill_id,
        "input": {"text": task.input_text},
        "output": {"text": task.output} if task.output else None,
        "metadata": task.metadata,
    }


# ── DELETE /a2a/tasks/{task_id} ──────────────────────────────────────────

@router.delete("/a2a/tasks/{task_id}")
async def cancel_task(
    task_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Cancel a pending or working task."""
    manager = _get_task_manager()
    success = manager.cancel_task(task_id)

    if not success:
        raise HTTPException(
            status_code=404,
            detail="Task not found or already in terminal state",
        )

    return {"id": task_id, "state": "canceled"}


# ── GET /a2a/tasks ────────────────────────────────────────────────────────

@router.get("/a2a/tasks")
async def list_tasks(
    state: str | None = None,
    limit: int = 50,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """List tasks, optionally filtered by state."""
    from a2a.client import TaskState

    manager = _get_task_manager()

    filter_state = None
    if state:
        try:
            filter_state = TaskState(state)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid state: {state}. Valid: {[s.value for s in TaskState]}",
            )

    tasks = manager.list_tasks(state=filter_state, limit=limit)

    return {
        "tasks": [
            {
                "id": t.id,
                "state": t.state.value,
                "skill_id": t.skill_id,
                "created_at": t.created_at,
                "has_output": t.output is not None,
            }
            for t in tasks
        ],
        "total": manager.task_count,
        "registered_skills": manager.registered_skills,
    }


# ── Singleton task manager ────────────────────────────────────────────────

_task_manager: A2ATaskManager | None = None


def _get_task_manager() -> A2ATaskManager:
    """Return a cached A2ATaskManager singleton."""
    global _task_manager
    if _task_manager is None:
        from a2a.server import A2ATaskManager
        _task_manager = A2ATaskManager()
    return _task_manager
