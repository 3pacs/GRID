#!/usr/bin/env python3
"""audit_wiring.py — bird's-eye view of what's CONNECTED vs what's DARK.

Runs three orthogonal checks against the repo and prints a report you can
read in 60 seconds to answer "what modules exist but aren't plugged in
anywhere?":

  1. Import-graph analysis — orphans (imported by nothing) and
     dead-ends (importing nothing local).  Flags modules that live in
     canonical intelligence/features/discovery/oracle dirs but no other
     module pulls them.

  2. API router coverage — every api/routers/*.py should be registered
     in api/main.py via include_router(). Lists routers that exist on
     disk but aren't mounted.

  3. Scheduler coverage — every ingestion/altdata/*.py is expected to
     be registered in scripts/hermes_operator.py (or
     ingestion/scheduler.py) so the daemon actually pulls from it.
     Lists pullers that exist but aren't scheduled.

Offline only. No DB, no network. Runs in ~2 seconds.

    python3 -m scripts.audit_wiring

Prints counts at the top + named-module lists for each gap category.
"""
from __future__ import annotations

import ast
import re
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCAN_DIRS = [
    "intelligence",
    "features",
    "discovery",
    "oracle",
    "physics",
    "analysis",
    "inference",
    "trading",
    "ingestion",
    "normalization",
    "store",
    "journal",
    "validation",
    "governance",
    "alerts",
    "agents",
    "api",
    "signals",
    "ingestors",
    "alpha_research",
]
# Dirs that are *entrypoints* — if something in here imports a module, it's
# considered "live" because these are the services/scripts that actually run.
ENTRY_DIRS = {"api", "scripts", "alerts"}


def _module_name(path: Path) -> str:
    """Convert repo-relative .py path to dotted module name."""
    rel = path.relative_to(REPO).with_suffix("")
    return ".".join(rel.parts)


def _local_imports(path: Path, known: set[str]) -> set[str]:
    """Parse a file and return the set of *local* modules it imports."""
    try:
        tree = ast.parse(path.read_text(errors="ignore"))
    except Exception:
        return set()
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            name = node.module
            parts = name.split(".")
            # Match on any prefix that's a known module
            for i in range(len(parts), 0, -1):
                candidate = ".".join(parts[:i])
                if candidate in known:
                    out.add(candidate)
                    break
        elif isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                for i in range(len(parts), 0, -1):
                    candidate = ".".join(parts[:i])
                    if candidate in known:
                        out.add(candidate)
                        break
    return out


def _scan_modules() -> list[Path]:
    out: list[Path] = []
    for d in SCAN_DIRS:
        root = REPO / d
        if not root.is_dir():
            continue
        for p in root.rglob("*.py"):
            if "__pycache__" in p.parts or "worktrees" in p.parts:
                continue
            if p.name == "__init__.py":
                continue
            out.append(p)
    return out


def _audit_api_routers() -> tuple[list[str], list[str]]:
    routers_dir = REPO / "api" / "routers"
    if not routers_dir.is_dir():
        return [], []
    on_disk = [p.stem for p in sorted(routers_dir.glob("*.py")) if p.name != "__init__.py"]
    main = (REPO / "api" / "main.py").read_text(errors="ignore")
    mounted: list[str] = []
    missing: list[str] = []
    for r in on_disk:
        # Look for any form of import + include: api.routers.X or from api.routers import X
        if re.search(rf"\b{re.escape(r)}\b", main):
            mounted.append(r)
        else:
            missing.append(r)
    return mounted, missing


def _audit_puller_scheduling() -> tuple[list[str], list[str]]:
    pullers_dir = REPO / "ingestion" / "altdata"
    if not pullers_dir.is_dir():
        return [], []
    on_disk = [p.stem for p in sorted(pullers_dir.glob("*.py")) if p.name != "__init__.py"]
    # Concatenate every likely scheduler file
    sched_files = [
        REPO / "scripts" / "hermes_operator.py",
        REPO / "ingestion" / "scheduler.py",
        REPO / "intelligence" / "scheduler.py",
    ]
    text = ""
    for f in sched_files:
        if f.exists():
            text += "\n" + f.read_text(errors="ignore")
    registered: list[str] = []
    unregistered: list[str] = []
    for p in on_disk:
        if re.search(rf"\b{re.escape(p)}\b", text):
            registered.append(p)
        else:
            unregistered.append(p)
    return registered, unregistered


def _audit_import_graph() -> dict:
    paths = _scan_modules()
    known = {_module_name(p) for p in paths}
    imports: dict[str, set[str]] = {}
    imported_by: dict[str, set[str]] = defaultdict(set)
    for p in paths:
        mod = _module_name(p)
        deps = _local_imports(p, known)
        imports[mod] = deps
        for d in deps:
            imported_by[d].add(mod)

    # Orphans: nobody imports them AND they're not in an entry dir
    orphans = sorted(
        [
            m
            for m in known
            if not imported_by[m] and not any(m.startswith(e + ".") for e in ENTRY_DIRS)
        ]
    )
    # Dead-ends: import nothing local (can't contribute anything except as leaf)
    dead_ends = sorted([m for m in known if not imports[m]])

    # Reachability from api/scripts/alerts entry points
    reachable: set[str] = set()
    stack = [m for m in known if any(m.startswith(e + ".") for e in ENTRY_DIRS)]
    while stack:
        m = stack.pop()
        if m in reachable:
            continue
        reachable.add(m)
        for d in imports.get(m, ()):
            if d not in reachable:
                stack.append(d)
    unreachable = sorted(known - reachable)

    return {
        "total_modules": len(known),
        "orphans": orphans,
        "dead_ends": dead_ends,
        "unreachable": unreachable,
        "reachable_count": len(reachable),
    }


def main() -> int:
    print("═" * 80)
    print("  GRID wiring audit — what's connected vs what's dark")
    print("═" * 80)

    mounted, missing_routers = _audit_api_routers()
    print(f"\n— API router coverage — {len(mounted)} mounted, {len(missing_routers)} DARK")
    if missing_routers:
        for r in missing_routers:
            print(f"   ✗ api/routers/{r}.py  not wired in api/main.py")
    else:
        print("   ✓ every router is mounted")

    registered, unregistered = _audit_puller_scheduling()
    print(f"\n— Puller scheduling — {len(registered)} registered, {len(unregistered)} DARK")
    if unregistered:
        for p in unregistered:
            print(f"   ✗ ingestion/altdata/{p}.py  not registered in any scheduler")

    graph = _audit_import_graph()
    print(
        f"\n— Import graph — {graph['total_modules']} modules, "
        f"{graph['reachable_count']} reachable from api/scripts/alerts"
    )
    n_orphan = len(graph["orphans"])
    n_unreach = len(graph["unreachable"])
    print(f"  orphans     (no importer):        {n_orphan}")
    print(f"  unreachable (from entry dirs):    {n_unreach}")

    print("\n— Top 30 ORPHANS (high-signal — check if they were supposed to be wired) —")
    for m in graph["orphans"][:30]:
        print(f"   ○ {m}")

    print("\n— Top 30 UNREACHABLE (not traced from api/scripts/alerts) —")
    for m in graph["unreachable"][:30]:
        if m not in graph["orphans"]:  # don't double-count
            print(f"   ● {m}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
