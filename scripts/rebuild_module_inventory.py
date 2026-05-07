#!/usr/bin/env python3
"""rebuild_module_inventory.py — Regenerator for docs/MODULE_INVENTORY.md.

Walks the canonical SCAN_DIRS from `lint_module_inventory.py` and emits a
fresh inventory: frontmatter (date, count, LOC), directory summary table,
and one `#### ` entry per module with its docstring, public symbols, and
imports. Output matches the format the linter parses, so running this
script makes `python3 scripts/lint_module_inventory.py` exit 0.

Usage:
    python3 scripts/rebuild_module_inventory.py
    python3 scripts/rebuild_module_inventory.py --root /path/to/grid_repo
    python3 scripts/rebuild_module_inventory.py --dry-run

The walker, scan dirs, and skip set are imported from
`scripts.lint_module_inventory` to keep the two tools in lockstep.
"""

from __future__ import annotations

import argparse
import ast
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

# Single source of truth for which dirs are canonical and which to skip.
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
from lint_module_inventory import (  # noqa: E402
    SCAN_DIRS,
    SKIP_DIR_NAMES,
    find_repo_root,
    walk_real_modules,
)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _line_count(text: str) -> int:
    if not text:
        return 0
    n = text.count("\n")
    if not text.endswith("\n"):
        n += 1
    return n


def _first_docstring_line(tree: ast.AST) -> str:
    doc = ast.get_docstring(tree)  # type: ignore[arg-type]
    if not doc:
        return ""
    first = doc.strip().splitlines()[0].strip()
    return first


def _public_symbols(tree: ast.Module) -> list[str]:
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if not node.name.startswith("_"):
                names.append(node.name)
    return names


def _imports(tree: ast.Module) -> list[str]:
    """Return sorted unique top-level module names imported by this file."""
    seen: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                seen.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                seen.add(node.module.split(".")[0])
    return sorted(seen)


def _imported_by(root: Path, modules: list[str]) -> dict[str, list[str]]:
    """Map module path → list of module paths that import it.

    Cheap heuristic: build a set of `from <pkg.subpkg> import` and `import
    <pkg.subpkg>` paths per file, then for each module check who imports
    its dotted path. Skips deep AST analysis for speed; this matches the
    quality of the existing inventory (best-effort, not exhaustive).
    """
    # Map dotted-path prefix → file path
    dotted_to_path: dict[str, str] = {}
    for rel in modules:
        parts = rel[:-3].split("/")  # strip .py
        # Top-level package + module
        if parts[-1] == "__init__":
            dotted = ".".join(parts[:-1])
        else:
            dotted = ".".join(parts)
        dotted_to_path[dotted] = rel

    imports_by_file: dict[str, set[str]] = {}
    for rel in modules:
        path = root / rel
        text = _read_text(path)
        if not text:
            imports_by_file[rel] = set()
            continue
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError:
            imports_by_file[rel] = set()
            continue
        seen: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    seen.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    seen.add(node.module)
        imports_by_file[rel] = seen

    result: dict[str, list[str]] = defaultdict(list)
    for importer, imp_set in imports_by_file.items():
        for imp in imp_set:
            # Match exact or any prefix that resolves to a module path
            if imp in dotted_to_path:
                target = dotted_to_path[imp]
                if target != importer:
                    result[target].append(importer)
    for k in result:
        result[k] = sorted(set(result[k]))
    return result


def _module_entry(
    rel_path: str,
    text: str,
    imported_by: list[str],
) -> tuple[str, int]:
    """Render a single `#### ` block for one module. Returns (block, loc)."""
    loc = _line_count(text)

    docstring_line = ""
    funcs: list[str] = []
    imports: list[str] = []
    if text:
        try:
            tree = ast.parse(text, filename=rel_path)
            docstring_line = _first_docstring_line(tree)
            funcs = _public_symbols(tree)
            imports = _imports(tree)
        except SyntaxError:
            docstring_line = "(unparseable — syntax error)"

    lines: list[str] = []
    lines.append(f"#### `{rel_path}` — {loc} LOC")
    if docstring_line:
        lines.append(f"**Docstring:** {docstring_line}")
    if funcs:
        funcs_fmt = ", ".join(f"`{f}`" for f in funcs)
        lines.append(f"**Functions:** {funcs_fmt}")
    if imports:
        imp_fmt = ", ".join(f"`{i}`" for i in imports)
        lines.append(f"**Reads:** {imp_fmt}")
    if imported_by:
        ib_fmt = ", ".join(f"`{p}`" for p in imported_by[:8])
        if len(imported_by) > 8:
            ib_fmt += f", … (+{len(imported_by) - 8})"
        lines.append(f"**Imported by:** {ib_fmt}")
    lines.append("")
    return "\n".join(lines), loc


def render_inventory(root: Path) -> str:
    modules = sorted(walk_real_modules(root))
    imported_by = _imported_by(root, modules)

    # Per-directory aggregation (top-level scan dir → module count + LOC)
    dir_count: dict[str, int] = defaultdict(int)
    dir_loc: dict[str, int] = defaultdict(int)

    # Build module entries
    by_dir: dict[str, list[str]] = defaultdict(list)
    total_loc = 0
    for rel in modules:
        text = _read_text(root / rel)
        block, loc = _module_entry(rel, text, imported_by.get(rel, []))
        top = rel.split("/", 1)[0]
        dir_count[top] += 1
        dir_loc[top] += loc
        total_loc += loc
        by_dir[top].append(block)

    # Frontmatter
    today = date.today().isoformat()
    excluded = ", ".join(f"`{n}/`" for n in sorted(SKIP_DIR_NAMES))
    out: list[str] = []
    out.append("# GRID Module Inventory")
    out.append("")
    out.append(f"Generated: {today}")
    out.append(f"Total modules: {len(modules)}")
    out.append(f"Total LOC: {total_loc:,}")
    out.append("")
    out.append(
        "This is the authoritative inventory of every `.py` file in the GRID "
        "intelligence/data/serving stack."
    )
    out.append(f"Excludes {excluded}.")
    out.append("")

    # Directory summary
    out.append("## Directory summary")
    out.append("")
    out.append("| Directory | Module count | LOC |")
    out.append("|---|---|---|")
    ordered_dirs = sorted(dir_count.keys(), key=lambda d: dir_loc[d], reverse=True)
    for d in ordered_dirs:
        out.append(f"| `{d}/` | {dir_count[d]} | {dir_loc[d]:,} |")
    out.append("")

    # Per-directory module entries
    for d in ordered_dirs:
        out.append(f"## `{d}/`")
        out.append("")
        for block in by_dir[d]:
            out.append(block)
    return "\n".join(out).rstrip() + "\n"


def write_atomic(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Regenerate docs/MODULE_INVENTORY.md from the filesystem."
    )
    parser.add_argument(
        "--root",
        default=None,
        help="Repository root (auto-detected if omitted).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the rendered inventory to stdout instead of writing.",
    )
    args = parser.parse_args(argv)

    root = Path(args.root).resolve() if args.root else find_repo_root(Path.cwd())
    inventory_path = root / "docs" / "MODULE_INVENTORY.md"

    rendered = render_inventory(root)

    if args.dry_run:
        sys.stdout.write(rendered)
        return 0

    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    write_atomic(inventory_path, rendered)
    n_modules = rendered.count("\n#### `")
    print(
        f"[rebuild_module_inventory] wrote {inventory_path} "
        f"({n_modules} modules)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
