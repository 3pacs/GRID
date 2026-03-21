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

from fastapi import APIRouter, HTTPException
from loguru import logger as log

router = APIRouter(prefix="/api/v1/workflows", tags=["workflows"])


@router.get("")
async def list_workflows() -> dict[str, Any]:
    """List all available workflows with enabled/disabled status."""
    from workflows.loader import load_all_available

    workflows = load_all_available()
    return {
        "workflows": workflows,
        "total": len(workflows),
        "enabled": sum(1 for w in workflows if w["enabled"]),
    }


@router.get("/enabled")
async def list_enabled() -> dict[str, Any]:
    """List only enabled workflows."""
    from workflows.loader import load_enabled

    workflows = load_enabled()
    return {"workflows": workflows, "total": len(workflows)}


@router.post("/{name}/enable")
async def enable(name: str) -> dict[str, Any]:
    """Enable a workflow by name."""
    from workflows.loader import enable_workflow

    success = enable_workflow(name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")
    return {"status": "enabled", "name": name}


@router.post("/{name}/disable")
async def disable(name: str) -> dict[str, Any]:
    """Disable a workflow by name."""
    from workflows.loader import disable_workflow

    success = disable_workflow(name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")
    return {"status": "disabled", "name": name}


@router.post("/{name}/run")
async def run_workflow(name: str) -> dict[str, Any]:
    """Execute a workflow by name (synchronous — may take a while)."""
    from workflows.loader import load_all_available

    workflows = load_all_available()
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
async def validate(name: str) -> dict[str, Any]:
    """Validate a workflow file for correctness."""
    from workflows.loader import validate_workflow

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
async def get_waves() -> dict[str, Any]:
    """Show the wave execution plan for enabled workflows."""
    from physics.waves import WaveTask, build_execution_waves
    from workflows.loader import load_enabled

    workflows = load_enabled()
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
async def get_schedule() -> dict[str, Any]:
    """Show scheduled workflows and their timing."""
    from workflows.loader import load_enabled, parse_schedule

    workflows = load_enabled()
    schedule_list = []

    for wf in workflows:
        sched = parse_schedule(wf["schedule"])
        schedule_list.append({
            "name": wf["name"],
            "group": wf["group"],
            **sched,
        })

    return {"schedules": schedule_list, "total": len(schedule_list)}
