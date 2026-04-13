#!/usr/bin/env python3
"""lint_module_inventory.py — Staleness gate for docs/MODULE_INVENTORY.md.

Fails CI (non-zero exit) when the authoritative module inventory at
`docs/MODULE_INVENTORY.md` drifts from the real filesystem. Three failure modes:

    1. Date stale   — "Generated: YYYY-MM-DD" frontmatter is > 7 days old
    2. Count drift  — the "Total modules: N" line disagrees with the real .py count
    3. File drift   — any .py file is added to or deleted from the repo vs the
                       inventory's enumerated module list

The walker reuses the DEFAULT_SCAN_DIRS list from `pre_create_check.py` so the
two tools stay in lockstep on which directories count as canonical modules.

Usage:
    python3 scripts/lint_module_inventory.py
    python3 scripts/lint_module_inventory.py --verbose
    python3 scripts/lint_module_inventory.py --max-stale-days 14
    python3 scripts/lint_module_inventory.py --rebuild  # delegate to rebuilder

Exit codes:
    0 — inventory is fresh (date <= max-stale-days AND file lists agree)
    1 — stale by date (Generated line is older than --max-stale-days)
    2 — module count mismatch (Total modules line disagrees with fs count)
    3 — module file list mismatch (adds and/or deletes detected)

Install as a git pre-push hook via the bootstrap at the bottom of this file, or
drop the following into `.git/hooks/pre-push`:

    python3 scripts/lint_module_inventory.py || {
        echo "MODULE_INVENTORY.md stale; run scripts/rebuild_module_inventory.py"
        exit 1
    }
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

# Reuse the same canonical scan dirs as pre_create_check.py. This list is the
# source of truth for "what counts as a GRID module". Keep in sync.
SCAN_DIRS: list[str] = [
    "intelligence",
    "physics",
    "features",
    "discovery",
    "inference",
    "alpha_research",
    "agents",
    "oracle",
    "trading",
    "analysis",
    "normalization",
    "governance",
    "store",
    "outputs",
    "hyperspace",
    "llamacpp",
    "gemma",
    "rag",
    "ollama",
    "subnet",
    "a2a",
    "alerts",
    "backtest",
    "validation",
    "timeseries",
    "derivatives",
    "ingestion",
    "journal",
    "events",
    "contracts",
    "api",
]

SKIP_DIR_NAMES: set[str] = {
    "__pycache__",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    "node_modules",
    ".venv",
    "venv",
    "dist",
    "build",
    ".next",
    "tests",  # inventory explicitly excludes tests/
    "pwa",
    "pwa_dist",
    "docs",
    "notebooks",
}

# Exit code sentinels (keep in sync with docstring).
EXIT_OK = 0
EXIT_STALE_DATE = 1
EXIT_COUNT_MISMATCH = 2
EXIT_FILE_MISMATCH = 3

# Frontmatter parsing patterns.
GENERATED_RE = re.compile(r"^Generated:\s*(\d{4}-\d{2}-\d{2})\s*$", re.MULTILINE)
TOTAL_MODULES_RE = re.compile(r"^Total modules:\s*(\d+)\s*$", re.MULTILINE)
# Module-entry headings look like:  #### `path/to/module.py` — 1234 LOC
MODULE_HEADING_RE = re.compile(r"^####\s+`([^`]+\.py)`", re.MULTILINE)

INVENTORY_REL = Path("docs") / "MODULE_INVENTORY.md"


@dataclass(frozen=True)
class InventoryState:
    """Parsed frontmatter + enumerated module list from MODULE_INVENTORY.md."""

    generated: date | None
    total_modules: int | None
    modules: frozenset[str]


@dataclass
class LintResult:
    """Structured lint outcome for reporting + exit-code dispatch."""

    exit_code: int
    inventory: InventoryState
    real_modules: frozenset[str]
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    age_days: int | None = None
    messages: list[str] = field(default_factory=list)


def find_repo_root(start: Path) -> Path:
    """Walk upward from `start` until we find the GRID repo root."""
    cur = start.resolve()
    markers = {"intelligence", "api", "ingestion"}
    while cur != cur.parent:
        if cur.is_dir():
            names = {p.name for p in cur.iterdir() if p.is_dir()}
            if len(markers & names) >= 2:
                return cur
        cur = cur.parent
    return start.resolve()


def parse_inventory(path: Path) -> InventoryState:
    """Parse the frontmatter + module entries from MODULE_INVENTORY.md."""
    if not path.exists():
        return InventoryState(generated=None, total_modules=None, modules=frozenset())

    text = path.read_text(encoding="utf-8", errors="replace")

    gen: date | None = None
    m = GENERATED_RE.search(text)
    if m:
        try:
            gen = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            gen = None

    total: int | None = None
    m = TOTAL_MODULES_RE.search(text)
    if m:
        try:
            total = int(m.group(1))
        except ValueError:
            total = None

    modules = frozenset(MODULE_HEADING_RE.findall(text))
    return InventoryState(generated=gen, total_modules=total, modules=modules)


def walk_real_modules(root: Path) -> frozenset[str]:
    """Return the set of .py file paths (relative to root) under the scan dirs."""
    found: set[str] = set()
    for rel in SCAN_DIRS:
        base = root / rel
        if not base.exists() or not base.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES]
            for fn in filenames:
                if fn.endswith(".py"):
                    p = Path(dirpath) / fn
                    try:
                        rel_path = p.relative_to(root)
                    except ValueError:
                        continue
                    found.add(str(rel_path).replace(os.sep, "/"))
    return frozenset(found)


def evaluate(
    root: Path,
    max_stale_days: int,
    today: date | None = None,
) -> LintResult:
    """Run the three checks against MODULE_INVENTORY.md and return a result."""
    today = today or date.today()
    inventory = parse_inventory(root / INVENTORY_REL)
    real = walk_real_modules(root)

    result = LintResult(exit_code=EXIT_OK, inventory=inventory, real_modules=real)

    # --- Check 1: date freshness --------------------------------------------
    if inventory.generated is None:
        result.exit_code = EXIT_STALE_DATE
        result.messages.append(
            "Missing or unparseable 'Generated: YYYY-MM-DD' frontmatter in "
            f"{INVENTORY_REL}"
        )
        return result

    age = (today - inventory.generated).days
    result.age_days = age
    if age > max_stale_days:
        result.exit_code = EXIT_STALE_DATE
        result.messages.append(
            f"MODULE_INVENTORY.md is {age} days old (max allowed: {max_stale_days}). "
            f"Generated {inventory.generated.isoformat()}, today is {today.isoformat()}."
        )
        return result

    # --- Check 2: module count ----------------------------------------------
    real_count = len(real)
    if inventory.total_modules is None:
        result.exit_code = EXIT_COUNT_MISMATCH
        result.messages.append(
            "Missing or unparseable 'Total modules: N' frontmatter in "
            f"{INVENTORY_REL}"
        )
        return result

    if inventory.total_modules != real_count:
        result.exit_code = EXIT_COUNT_MISMATCH
        result.messages.append(
            f"Module count mismatch: inventory says {inventory.total_modules}, "
            f"filesystem has {real_count}."
        )
        # fall through so we still compute the diff below

    # --- Check 3: module file list ------------------------------------------
    added = sorted(real - inventory.modules)
    removed = sorted(inventory.modules - real)
    result.added = added
    result.removed = removed

    if added or removed:
        if result.exit_code == EXIT_OK:
            result.exit_code = EXIT_FILE_MISMATCH
        result.messages.append(
            f"File list drift: {len(added)} added, {len(removed)} removed."
        )

    return result


def render_report(result: LintResult, verbose: bool) -> str:
    """Render a human-readable diff report for stdout."""
    lines: list[str] = []
    inv = result.inventory

    lines.append("=" * 72)
    lines.append("MODULE_INVENTORY.md staleness lint")
    lines.append("=" * 72)
    lines.append(
        f"Generated:     {inv.generated.isoformat() if inv.generated else '<missing>'}"
    )
    if result.age_days is not None:
        lines.append(f"Age:           {result.age_days} day(s)")
    lines.append(
        f"Inventory says: {inv.total_modules if inv.total_modules is not None else '<missing>'} modules"
    )
    lines.append(f"Filesystem has: {len(result.real_modules)} modules")
    lines.append(
        f"Delta:          +{len(result.added)} added, -{len(result.removed)} removed"
    )
    lines.append("")

    if not result.messages and result.exit_code == EXIT_OK:
        lines.append("OK — inventory is up-to-date.")
        return "\n".join(lines)

    for msg in result.messages:
        lines.append(f"!! {msg}")

    if result.added:
        lines.append("")
        lines.append(f"ADDED ({len(result.added)}) — new .py files not in inventory:")
        show = result.added if verbose else result.added[:25]
        for p in show:
            lines.append(f"  + {p}")
        if not verbose and len(result.added) > 25:
            lines.append(f"  ... and {len(result.added) - 25} more (use --verbose)")

    if result.removed:
        lines.append("")
        lines.append(f"REMOVED ({len(result.removed)}) — inventory lists non-existent files:")
        show = result.removed if verbose else result.removed[:25]
        for p in show:
            lines.append(f"  - {p}")
        if not verbose and len(result.removed) > 25:
            lines.append(f"  ... and {len(result.removed) - 25} more (use --verbose)")

    lines.append("")
    lines.append(
        "Fix: run `python3 scripts/rebuild_module_inventory.py` to regenerate "
        "docs/MODULE_INVENTORY.md, then commit the updated file."
    )
    return "\n".join(lines)


def invoke_rebuild(root: Path) -> int:
    """Delegate to the rebuild script. Stub today, full impl queued as follow-up."""
    rebuild = root / "scripts" / "rebuild_module_inventory.py"
    if not rebuild.exists():
        print(
            f"rebuild script not found at {rebuild}; cannot --rebuild",
            file=sys.stderr,
        )
        return 2
    print(f"[lint_module_inventory] invoking {rebuild}")
    proc = subprocess.run(
        [sys.executable, str(rebuild)],
        cwd=str(root),
        check=False,
    )
    return proc.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="CI gate: detect when docs/MODULE_INVENTORY.md is stale."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show the full added/removed file lists (no truncation).",
    )
    parser.add_argument(
        "--max-stale-days",
        type=int,
        default=7,
        help="Fail if inventory is older than this many days (default: 7).",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Invoke scripts/rebuild_module_inventory.py instead of linting.",
    )
    parser.add_argument(
        "--root",
        default=None,
        help="Repository root (auto-detected if omitted).",
    )
    args = parser.parse_args(argv)

    script_root = Path(__file__).resolve().parent.parent
    root = Path(args.root).resolve() if args.root else find_repo_root(script_root)

    if args.rebuild:
        return invoke_rebuild(root)

    result = evaluate(root, max_stale_days=args.max_stale_days)
    print(render_report(result, verbose=args.verbose))
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
