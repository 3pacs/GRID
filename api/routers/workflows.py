"""
GRID API — Workflow management endpoints.

Provides REST API access to the declarative workflow system:
  GET  /api/v1/workflows          — List all workflows
  GET  /api/v1/workflows/enabled  — List enabled workflows only
  POST /api/v1/workflows/{name}/enable   — Enable a workflow
  POST /api/v1/workflows/{name}/disable  — Disable a workflow
  POST /api/v1/workflows/{name}/run      — Execute a workflow
  GET  /api/v1/workflows/{name}/validate — Validate a workflow file
  GET  /api/v1/workflows/waves    — Show wave execution plan
  GET  /api/v1/workflows/schedule — Show scheduled workflows
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger as log

from api.auth import require_auth, require_role

router = APIRouter(
    prefix="/api/v1/workflows",
    tags=["workflows"],
    dependencies=[Depends(require_auth)],
)

# Write operations use admin-only dependency inline


def _api_selection(request: Request) -> list[dict[str, Any]] | None:
    """An absent binding preserves legacy behavior; any supplied binding must prove state."""
    from workflows.loader import WorkflowStateUnknown, read_api_selection

    absent = object()
    binding = getattr(request.app.state, "workflow_state_binding", absent)
    if binding is absent:
        return None
    try:
        return read_api_selection(binding)
    except WorkflowStateUnknown as exc:
        raise HTTPException(status_code=503, detail="Workflow state is unknown") from exc


def _require_legacy_mutation(selection: list[dict[str, Any]] | None) -> None:
    if selection is not None:
        raise HTTPException(status_code=503, detail="Isolated workflow mutation/dispatch is unavailable")


@router.get("")
async def list_workflows(selection: list[dict[str, Any]] | None = Depends(_api_selection)) -> dict[str, Any]:
    """List all available workflows with enabled/disabled status."""
    from workflows.loader import load_all_available

    workflows = load_all_available() if selection is None else selection
    return {
        "workflows": workflows,
        "total": len(workflows),
        "enabled": sum(1 for w in workflows if w["enabled"]),
    }


@router.get("/enabled")
async def list_enabled(selection: list[dict[str, Any]] | None = Depends(_api_selection)) -> dict[str, Any]:
    """List only enabled workflows."""
    from workflows.loader import load_enabled

    workflows = load_enabled() if selection is None else [w for w in selection if w["enabled"]]
    return {"workflows": workflows, "total": len(workflows)}


@router.post("/{name}/enable", dependencies=[Depends(require_role("admin"))])
async def enable(name: str, selection: list[dict[str, Any]] | None = Depends(_api_selection)) -> dict[str, Any]:
    """Enable a workflow by name."""
    from workflows.loader import enable_workflow

    _require_legacy_mutation(selection)
    success = enable_workflow(name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")
    return {"status": "enabled", "name": name}


@router.post("/{name}/disable", dependencies=[Depends(require_role("admin"))])
async def disable(name: str, selection: list[dict[str, Any]] | None = Depends(_api_selection)) -> dict[str, Any]:
    """Disable a workflow by name."""
    from workflows.loader import disable_workflow

    _require_legacy_mutation(selection)
    success = disable_workflow(name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")
    return {"status": "disabled", "name": name}


@router.post("/{name}/run", dependencies=[Depends(require_role("admin"))])
async def run_workflow(name: str, selection: list[dict[str, Any]] | None = Depends(_api_selection)) -> dict[str, Any]:
    """Execute a workflow by name (synchronous — may take a while)."""
    from workflows.loader import load_all_available

    _require_legacy_mutation(selection)
    workflows = load_all_available() if selection is None else selection
    wf = next((w for w in workflows if w["name"] == name), None)
    if wf is None:
        raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")

    log.info("API: Running workflow '{n}'", n=name)

    # Return the workflow info — actual execution is dispatched via CLI
    # (API triggers are async-safe but long-running tasks should use workers)
    return {
        "status": "accepted",
        "name": name,
        "group": wf["group"],
        "description": wf["description"],
        "message": f"Workflow '{name}' execution triggered. "
        f"Use CLI 'python cli.py run {name}' for synchronous execution.",
    }


@router.get("/{name}/validate")
async def validate(name: str, selection: list[dict[str, Any]] | None = Depends(_api_selection)) -> dict[str, Any]:
    """Validate a workflow file for correctness."""
    from workflows.loader import validate_workflow

    if selection is not None:
        workflow = next((w for w in selection if w["name"] == name), None)
        if workflow is None:
            raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")
        errors = validate_workflow(Path(workflow["file_path"]))
        return {"name": name, "valid": not errors, "errors": errors}

    available_dir = Path(__file__).parent.parent.parent / "workflows" / "available"
    path = available_dir / f"{name}.md"

    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")

    errors = validate_workflow(path)
    return {
        "name": name,
        "valid": len(errors) == 0,
        "errors": errors,
    }


@router.get("/waves")
async def get_waves(selection: list[dict[str, Any]] | None = Depends(_api_selection)) -> dict[str, Any]:
    """Show the wave execution plan for enabled workflows."""
    from physics.waves import WaveTask, build_execution_waves
    from workflows.loader import load_enabled

    workflows = load_enabled() if selection is None else [w for w in selection if w["enabled"]]
    if not workflows:
        return {"waves": [], "message": "No enabled workflows"}

    tasks = [
        WaveTask(
            name=wf["name"],
            callable=lambda: None,
            depends_on=wf.get("depends_on", []),
        )
        for wf in workflows
    ]

    try:
        waves = build_execution_waves(tasks)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    wave_plan = []
    for i, wave in enumerate(waves):
        wave_plan.append({
            "wave": i,
            "tasks": [t.name for t in wave],
            "parallel": len(wave) > 1,
        })

    return {
        "waves": wave_plan,
        "total_waves": len(waves),
        "total_tasks": len(tasks),
    }


@router.get("/schedule")
async def get_schedule(selection: list[dict[str, Any]] | None = Depends(_api_selection)) -> dict[str, Any]:
    """Show scheduled workflows and their timing."""
    from workflows.loader import load_enabled, parse_schedule

    workflows = load_enabled() if selection is None else [w for w in selection if w["enabled"]]
    schedule_list = []

    for wf in workflows:
        sched = parse_schedule(wf["schedule"])
        schedule_list.append({
            "name": wf["name"],
            "group": wf["group"],
            **sched,
        })

    return {"schedules": schedule_list, "total": len(schedule_list)}
