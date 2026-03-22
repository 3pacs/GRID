#!/usr/bin/env python3
"""
GRID CLI — workflow management and operations.

Usage:
    python cli.py list                     Show all workflows with status
    python cli.py enable <name>            Enable a workflow
    python cli.py disable <name>           Disable a workflow
    python cli.py run <name>               Execute a workflow
    python cli.py validate [name]          Validate workflow file(s)
    python cli.py status                   Show last run status for enabled workflows
    python cli.py schedule                 Show scheduled workflows and next run times
    python cli.py waves                    Show wave execution plan for enabled workflows
    python cli.py verify                   Run market physics verification
    python cli.py conventions              List all financial conventions
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from loguru import logger as log


def cmd_list() -> None:
    """List all available workflows with enabled/disabled status."""
    from workflows.loader import load_all_available

    workflows = load_all_available()
    if not workflows:
        print("No workflows found in workflows/available/")
        return

    # Column widths
    max_name = max(len(w["name"]) for w in workflows)
    max_group = max(len(w["group"]) for w in workflows)

    print(f"\n{'NAME':<{max_name+2}} {'GROUP':<{max_group+2}} {'STATUS':<10} {'SCHEDULE':<28} DESCRIPTION")
    print("-" * (max_name + max_group + 80))

    for wf in workflows:
        status = "ENABLED" if wf["enabled"] else "disabled"
        status_marker = "*" if wf["enabled"] else " "
        print(
            f"{status_marker}{wf['name']:<{max_name+1}} "
            f"{wf['group']:<{max_group+2}} "
            f"{status:<10} "
            f"{wf['schedule']:<28} "
            f"{wf['description'][:60]}"
        )

    enabled_count = sum(1 for w in workflows if w["enabled"])
    print(f"\n{enabled_count}/{len(workflows)} workflows enabled")


def cmd_enable(name: str) -> None:
    """Enable a workflow by name."""
    from workflows.loader import enable_workflow

    if enable_workflow(name):
        print(f"Enabled: {name}")
    else:
        print(f"Failed to enable: {name}", file=sys.stderr)
        sys.exit(1)


def cmd_disable(name: str) -> None:
    """Disable a workflow by name."""
    from workflows.loader import disable_workflow

    if disable_workflow(name):
        print(f"Disabled: {name}")
    else:
        print(f"Failed to disable: {name}", file=sys.stderr)
        sys.exit(1)


def cmd_run(name: str) -> None:
    """Execute a workflow by name."""
    from workflows.loader import load_all_available

    workflows = load_all_available()
    wf = next((w for w in workflows if w["name"] == name), None)
    if wf is None:
        print(f"Workflow '{name}' not found", file=sys.stderr)
        sys.exit(1)

    print(f"Running workflow: {wf['name']}")
    print(f"  Group: {wf['group']}")
    print(f"  Description: {wf['description']}")
    print(f"  Dependencies: {wf['depends_on'] or 'none'}")
    print()

    # Dispatch based on group and name
    group = wf["group"]
    wf_name = wf["name"]

    if group == "ingestion":
        _run_ingestion_workflow(wf_name)
    elif group == "features":
        _run_feature_workflow(wf_name)
    elif group == "discovery":
        _run_discovery_workflow(wf_name)
    elif group == "physics":
        _run_physics_workflow(wf_name)
    elif group == "validation":
        _run_validation_workflow(wf_name)
    elif group == "governance":
        _run_governance_workflow(wf_name)
    else:
        print(f"No runner implemented for group '{group}'")
        sys.exit(1)


def _run_ingestion_workflow(name: str) -> None:
    """Dispatch ingestion workflows to scheduler_v2."""
    from db import get_engine
    from ingestion.scheduler_v2 import run_pull_group

    engine = get_engine()

    # Map workflow names to pull groups
    group_map = {
        "pull-fred": "daily",
        "pull-ecb": "daily",
        "pull-yfinance": "daily",
        "pull-bls": "daily",
        "pull-weekly-intl": "weekly",
        "pull-monthly-trade": "monthly",
        "pull-annual-datasets": "annual",
    }

    group = group_map.get(name)
    if group:
        result = run_pull_group(group, engine)
        print(f"Pull group '{group}': {result['success_count']} succeeded, {result['failure_count']} failed")
    else:
        print(f"No ingestion mapping for workflow '{name}'")


def _run_feature_workflow(name: str) -> None:
    """Dispatch feature workflows."""
    from db import get_engine
    from features.lab import FeatureLab
    from store.pit import PITStore

    engine = get_engine()
    pit = PITStore(engine)
    lab = FeatureLab(engine, pit)

    if name == "compute-features":
        results = lab.compute_derived_features(date.today())
        non_null = sum(1 for v in results.values() if v is not None)
        print(f"Computed {non_null}/{len(results)} derived features")


def _run_discovery_workflow(name: str) -> None:
    """Dispatch discovery workflows."""
    from db import get_engine
    from store.pit import PITStore

    engine = get_engine()
    pit = PITStore(engine)

    if name == "run-clustering":
        from discovery.clustering import ClusterDiscovery
        cd = ClusterDiscovery(engine, pit)
        summary = cd.run_cluster_discovery(n_components=5)
        print(f"Best k: {summary.get('best_k')}")

    elif name == "audit-orthogonality":
        from discovery.orthogonality import OrthogonalityAudit
        audit = OrthogonalityAudit(engine, pit)
        results = audit.run_full_audit()
        print(f"Orthogonality audit complete: {results.get('true_dimensionality', '?')} true dimensions")

    elif name == "check-regime":
        print("Running auto-regime detection...")
        # Delegates to scripts/auto_regime.py logic
        from discovery.clustering import ClusterDiscovery
        cd = ClusterDiscovery(engine, pit)
        summary = cd.run_cluster_discovery(n_components=5)
        print(f"Regime detection complete — best k={summary.get('best_k')}")


def _run_physics_workflow(name: str) -> None:
    """Dispatch physics workflows."""
    from db import get_engine
    from store.pit import PITStore

    engine = get_engine()
    pit = PITStore(engine)

    if name == "verify-physics":
        from physics.verify import MarketPhysicsVerifier
        verifier = MarketPhysicsVerifier(engine, pit)
        results = verifier.verify_all()
        summary = results.get("_summary", {})
        print(f"Physics verification: {summary.get('passed', 0)}/{summary.get('total_checks', 0)} passed")
        print(f"Average score: {summary.get('avg_score', 0):.2f}")

        # Print warnings
        for check_name, check_result in results.items():
            if check_name == "_summary":
                continue
            warns = check_result.get("warnings", [])
            if warns:
                print(f"\n  [{check_name}] Warnings:")
                for w in warns[:5]:
                    print(f"    - {w}")

    elif name == "sweep-parameters":
        print("Parameter sweep is a manual, long-running operation.")
        print("Use the wave executor for parallel sweep execution.")


def _run_validation_workflow(name: str) -> None:
    """Dispatch validation workflows."""
    print(f"Validation workflow '{name}' — requires database and model state.")
    print("Run via: python -m validation.backtest")


def _run_governance_workflow(name: str) -> None:
    """Dispatch governance workflows."""
    print(f"Governance workflow '{name}' — requires operator confirmation.")
    print("Use: python -c \"from governance.registry import ModelRegistry; ...\"")


def cmd_validate(name: str | None = None) -> None:
    """Validate workflow file(s)."""
    from workflows.loader import validate_workflow

    available_dir = Path(__file__).parent / "workflows" / "available"

    if name:
        path = available_dir / f"{name}.md"
        if not path.exists():
            print(f"Workflow '{name}' not found", file=sys.stderr)
            sys.exit(1)
        errors = validate_workflow(path)
        if errors:
            print(f"{name}: INVALID")
            for e in errors:
                print(f"  - {e}")
            sys.exit(1)
        else:
            print(f"{name}: valid")
    else:
        # Validate all
        total = 0
        invalid = 0
        for path in sorted(available_dir.glob("*.md")):
            total += 1
            errors = validate_workflow(path)
            if errors:
                invalid += 1
                print(f"{path.stem}: INVALID")
                for e in errors:
                    print(f"  - {e}")
            else:
                print(f"{path.stem}: valid")
        print(f"\n{total - invalid}/{total} valid")


def cmd_status() -> None:
    """Show status of enabled workflows."""
    from workflows.loader import load_enabled

    workflows = load_enabled()
    if not workflows:
        print("No enabled workflows. Use 'python cli.py enable <name>' to enable.")
        return

    print(f"\n{'NAME':<30} {'GROUP':<15} {'SCHEDULE'}")
    print("-" * 75)
    for wf in workflows:
        print(f"{wf['name']:<30} {wf['group']:<15} {wf['schedule']}")

    print(f"\n{len(workflows)} workflows enabled")


def cmd_schedule() -> None:
    """Show scheduled workflows and their timing."""
    from workflows.loader import load_enabled, parse_schedule

    workflows = load_enabled()
    if not workflows:
        print("No enabled workflows.")
        return

    print(f"\n{'NAME':<28} {'FREQUENCY':<12} {'TIME':<8} {'DAYS'}")
    print("-" * 75)

    for wf in workflows:
        sched = parse_schedule(wf["schedule"])
        freq = sched.get("frequency", "manual")
        time_str = sched.get("time", "-")
        days = sched.get("days", [])
        day_str = ", ".join(days) if days else "-"

        if sched.get("day_of_month"):
            day_str = f"day {sched['day_of_month']}"
        if sched.get("month"):
            day_str = f"{sched['month']} {day_str}"

        print(f"{wf['name']:<28} {freq:<12} {time_str:<8} {day_str}")


def cmd_waves() -> None:
    """Show wave execution plan for enabled workflows."""
    from physics.waves import WaveTask, build_execution_waves
    from workflows.loader import load_enabled

    workflows = load_enabled()
    if not workflows:
        print("No enabled workflows.")
        return

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
        print(f"Cannot build waves: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"\nExecution plan: {len(waves)} waves, {len(tasks)} tasks\n")
    for i, wave in enumerate(waves):
        names = [t.name for t in wave]
        parallel = "parallel" if len(names) > 1 else "single"
        print(f"  Wave {i} ({parallel}): {', '.join(names)}")


def cmd_verify() -> None:
    """Run market physics verification."""
    _run_physics_workflow("verify-physics")


def cmd_conventions() -> None:
    """List all financial conventions."""
    from physics.conventions import CONVENTIONS

    print(f"\n{'DOMAIN':<14} {'UNIT':<18} {'ANNUAL':<8} {'METHOD':<12} NOTES")
    print("-" * 90)

    for name, conv in CONVENTIONS.items():
        print(
            f"{conv.domain:<14} "
            f"{conv.unit:<18} "
            f"{'yes' if conv.annualized else 'no':<8} "
            f"{(conv.method or '-'):<12} "
            f"{conv.notes[:45]}"
        )


def main() -> None:
    """CLI entry point."""
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    command = sys.argv[1].lower()

    if command == "list":
        cmd_list()
    elif command == "enable" and len(sys.argv) >= 3:
        cmd_enable(sys.argv[2])
    elif command == "disable" and len(sys.argv) >= 3:
        cmd_disable(sys.argv[2])
    elif command == "run" and len(sys.argv) >= 3:
        cmd_run(sys.argv[2])
    elif command == "validate":
        name = sys.argv[2] if len(sys.argv) >= 3 else None
        cmd_validate(name)
    elif command == "status":
        cmd_status()
    elif command == "schedule":
        cmd_schedule()
    elif command == "waves":
        cmd_waves()
    elif command == "verify":
        cmd_verify()
    elif command == "conventions":
        cmd_conventions()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
