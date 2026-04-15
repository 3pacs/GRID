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
    # Include scripts/ so the hermes_operator scheduler (and every other
    # CLI entrypoint) is scanned as an importer. Without this, every puller
    # dispatched via hermes_operator's registry dict shows up as an orphan
    # because the only module that references it isn't in the corpus.
    "scripts",
]
# Dirs that are *entrypoints* — if something in here imports a module, it's
# considered "live" because these are the services/scripts that actually run.
ENTRY_DIRS = {"api", "scripts", "alerts"}


def _module_name(path: Path) -> str:
    """Convert repo-relative .py path to dotted module name."""
    rel = path.relative_to(REPO).with_suffix("")
    return ".".join(rel.parts)


def _local_imports(path: Path, known: set[str]) -> set[str]:
    """Parse a file and return the set of *local* modules it imports.

    Catches both static imports (``import X`` / ``from X import Y``) AND
    dynamic imports that are invisible to a naive AST walker:

      - ``importlib.import_module("alpha_research.signals.credit_cycle")``
      - ``__import__("intelligence.pattern_library")``
      - ``get_signal_class("dual_horizon_equity")`` — a SignalRegistry
        reflective dispatch where the string literal IS the module name

    Without this second pass the audit falsely flagged the alpha_research
    subtree as orphaned when in reality 3 of its signals are consumed
    live by oracle/engine.py via dynamic dispatch.
    """
    try:
        source = path.read_text(errors="ignore")
        tree = ast.parse(source)
    except Exception:
        return set()
    out: set[str] = set()

    # ── Pass 1: static imports ────────────────────────────────────────
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            name = node.module
            parts = name.split(".")
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

    # ── Pass 2: dynamic imports ───────────────────────────────────────
    # Walk call expressions looking for import_module / __import__ calls
    # whose first argument is a string literal. Also collect any bare
    # string literal that's a known module path (catches SignalRegistry-
    # style reflective dispatch where the module name is keyed by a
    # top-level constant).
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            is_dynamic_import = False
            if isinstance(fn, ast.Attribute) and fn.attr == "import_module":
                is_dynamic_import = True
            elif isinstance(fn, ast.Name) and fn.id in ("__import__", "import_module"):
                is_dynamic_import = True
            if is_dynamic_import and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    parts = first.value.split(".")
                    for i in range(len(parts), 0, -1):
                        candidate = ".".join(parts[:i])
                        if candidate in known:
                            out.add(candidate)
                            break

    # ── Pass 3: string-literal module references ──────────────────────
    # Crude but effective: any string constant anywhere in the file that
    # exactly matches a known module path is treated as a dependency.
    # This catches plugin registries / entry-point dicts like
    # SIGNAL_CLASSES = {"credit_cycle": "alpha_research.signals.credit_cycle"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            s = node.value
            if "." in s and s in known:
                out.add(s)

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
    """Two-pass mount check.

    Pass 1: router is named directly in api/main.py.
    Pass 2: router is re-exported from another already-mounted router
            that uses a facade pattern (e.g. astrogrid.py mounts
            astrogrid_core/predictions/celestial; intelligence.py mounts
            intelligence_*). We follow the import chain up to 3 hops
            from a main.py-mounted router. Without this, any decomposed
            facade file shows up as "dark" even when it's actually live.
    """
    routers_dir = REPO / "api" / "routers"
    if not routers_dir.is_dir():
        return [], []
    on_disk = [p.stem for p in sorted(routers_dir.glob("*.py")) if p.name != "__init__.py"]
    main_text = (REPO / "api" / "main.py").read_text(errors="ignore")

    # Pass 1 — directly mounted from main.py.
    directly_mounted = {
        r for r in on_disk if re.search(rf"\b{re.escape(r)}\b", main_text)
    }

    # Precompute the (imports, includes) edges between router files so we
    # can walk the facade chain. A router X is considered "facade-mounted"
    # if some already-live router Y both imports X AND calls
    # include_router on X's router.
    includes_graph: dict[str, set[str]] = {}
    for r in on_disk:
        path = routers_dir / f"{r}.py"
        try:
            txt = path.read_text(errors="ignore")
        except Exception:
            continue
        hits: set[str] = set()
        for other in on_disk:
            if other == r:
                continue
            # Require BOTH an import of the other router file AND an
            # include_router call — otherwise a file that just imports
            # a helper still wouldn't mount its routes.
            has_import = re.search(
                rf"from api\.routers\.{re.escape(other)} import\b", txt
            )
            has_include = re.search(r"\.include_router\(", txt)
            if has_import and has_include:
                hits.add(other)
        includes_graph[r] = hits

    # BFS from the directly-mounted set through the includes_graph for up
    # to 3 hops — a router is live if any reachable ancestor is live.
    live: set[str] = set(directly_mounted)
    for _ in range(3):
        grew = False
        for parent in list(live):
            for child in includes_graph.get(parent, ()):
                if child not in live:
                    live.add(child)
                    grew = True
        if not grew:
            break

    mounted = sorted(live)
    missing = sorted(set(on_disk) - live)
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
    # Also compute the set of known PACKAGE prefixes — a directory with
    # an __init__.py is a package, and importing it pulls the whole
    # subtree. We match against both package and module names.
    package_roots: set[str] = set()
    for d in SCAN_DIRS:
        root = REPO / d
        if not root.is_dir():
            continue
        for init in root.rglob("__init__.py"):
            if "__pycache__" in init.parts or "worktrees" in init.parts:
                continue
            pkg = ".".join(init.parent.relative_to(REPO).parts)
            package_roots.add(pkg)
    # Merge packages into the "known" set so _local_imports can match
    # package-level imports too.
    known_with_packages = known | package_roots

    imports: dict[str, set[str]] = {}
    imported_by: dict[str, set[str]] = defaultdict(set)
    for p in paths:
        mod = _module_name(p)
        raw_deps = _local_imports(p, known_with_packages)
        # Expand every package hit to also mark every submodule as
        # reached — the package __init__.py may re-export them.
        expanded: set[str] = set()
        for d in raw_deps:
            if d in package_roots:
                for candidate in known:
                    if candidate == d or candidate.startswith(d + "."):
                        expanded.add(candidate)
            if d in known:
                expanded.add(d)
        imports[mod] = expanded
        for d in expanded:
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

    print(f"\n— All {n_orphan} ORPHANS (high-signal — check if they were supposed to be wired) —")
    for m in graph["orphans"]:
        print(f"   ○ {m}")

    print(f"\n— All {n_unreach - n_orphan} additional UNREACHABLE modules (not traced from api/scripts/alerts) —")
    for m in graph["unreachable"]:
        if m not in graph["orphans"]:  # don't double-count
            print(f"   ● {m}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
