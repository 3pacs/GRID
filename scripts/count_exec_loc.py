#!/usr/bin/env python3
"""count_exec_loc.py — Executable-LOC + cyclomatic-complexity walker for GRID.

Raw LOC is a misleading success metric: YAML-extract refactors (Dedupe Waves
4/5) grow the line count of the tree while measurably reducing Python
complexity. Wave 1 dead-code deletes, by contrast, reduce both. To tell the
two apart we need metrics that track *executable* Python, not bytes.

This walker reports:

    executable_loc      Lines that are neither blank, comment, nor docstring.
                        Comments are stripped via `tokenize`; docstrings are
                        stripped via `ast` (module / class / function heads).
    file_count          Count of .py files walked.
    raw_loc             Total physical lines (for backwards comparison).
    cyclomatic          Sum of per-function CC. Uses `radon` if available,
                        otherwise a stdlib `ast` visitor that counts 1 per
                        function body plus 1 per decision point
                        (if/elif/for/while/and/or/except/with/assert/if-expr).
    functions           Count of function/method/lambda definitions.
    per-directory       Rollup of the same metrics grouped by top-level dir.

Flags:
    --json              Emit the whole snapshot as machine-readable JSON.
    --dir <path>        Focus a single directory (relative to repo root).
    --delta <file>      Load a previous JSON snapshot and show deltas.
    --save <file>       Write the current snapshot to JSON for later deltas.

The script is stdlib-only by default; `radon` is optional.

Exit code is always 0 — this is a reporting tool, not a gate.
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import os
import sys
import tokenize
from dataclasses import dataclass, field, asdict
from datetime import date
from pathlib import Path
from typing import Iterable


SKIP_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "dist",
    "build",
    ".next",
    ".grid_backups",
    "pwa_dist",
    "site-packages",
}

SKIP_TOP_DIRS = {
    # Large vendored / non-source trees the dedupe plan wouldn't touch.
    ".claude",
    ".git",
    "pwa_dist",
    "pwa/node_modules",
}


try:  # radon is optional — keep the script stdlib-only by default.
    from radon.complexity import cc_visit as _radon_cc_visit  # type: ignore

    _HAS_RADON = True
except Exception:  # noqa: BLE001 — any import failure falls back to stdlib.
    _HAS_RADON = False


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------


@dataclass
class FileMetrics:
    path: str
    raw_loc: int = 0
    executable_loc: int = 0
    cyclomatic: int = 0
    functions: int = 0
    parse_error: bool = False


@dataclass
class DirectoryRollup:
    name: str
    file_count: int = 0
    raw_loc: int = 0
    executable_loc: int = 0
    cyclomatic: int = 0
    functions: int = 0


@dataclass
class Snapshot:
    generated_at: str
    root: str
    radon_available: bool
    file_count: int
    raw_loc: int
    executable_loc: int
    cyclomatic: int
    functions: int
    per_directory: dict[str, DirectoryRollup] = field(default_factory=dict)
    files: list[FileMetrics] = field(default_factory=list)


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------


def find_repo_root(start: Path) -> Path:
    """Walk up until we see two canonical GRID top-level dirs."""
    cur = start.resolve()
    markers = {"intelligence", "migrations", "api", "ingestion"}
    while cur != cur.parent:
        try:
            names = {p.name for p in cur.iterdir() if p.is_dir()}
        except OSError:
            names = set()
        if len(markers & names) >= 2:
            return cur
        cur = cur.parent
    return start.resolve()


def iter_python_files(root: Path, focus: Path | None) -> Iterable[Path]:
    base = focus if focus is not None else root
    if not base.exists():
        return
    if base.is_file() and base.suffix == ".py":
        yield base
        return
    for dirpath, dirnames, filenames in os.walk(base):
        # Prune noisy / vendored directories in-place.
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES]
        rel_dir = Path(dirpath).resolve().relative_to(root) if root in Path(dirpath).resolve().parents or Path(dirpath).resolve() == root else None
        if rel_dir is not None:
            parts = rel_dir.parts
            if parts and any(p in SKIP_TOP_DIRS for p in parts):
                dirnames[:] = []
                continue
        for fn in filenames:
            if fn.endswith(".py"):
                yield Path(dirpath) / fn


# ---------------------------------------------------------------------------
# Executable-LOC via tokenize + ast
# ---------------------------------------------------------------------------


def _docstring_line_numbers(tree: ast.AST) -> set[int]:
    """Return the set of physical line numbers occupied by docstrings."""
    doc_lines: set[int] = set()

    def walk(node: ast.AST) -> None:
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
                const = body[0].value
                start = const.lineno
                end = getattr(const, "end_lineno", start)
                for line in range(start, end + 1):
                    doc_lines.add(line)
        for child in ast.iter_child_nodes(node):
            walk(child)

    walk(tree)
    return doc_lines


def _executable_lines(source: str) -> tuple[int, int]:
    """Return (raw_loc, executable_loc).

    Executable = non-blank, non-comment, non-docstring, non-pure-string-literal.
    We use tokenize to strip comments, ast to strip docstrings, then count
    physical lines that still contain at least one non-trivial token.
    """
    raw_lines = source.splitlines()
    raw_loc = len(raw_lines)

    # Parse for docstring line numbers. If the file doesn't parse we still
    # count tokens (falling back to a raw-minus-comments estimate).
    try:
        tree = ast.parse(source)
        doc_lines = _docstring_line_numbers(tree)
    except SyntaxError:
        doc_lines = set()

    exec_lines: set[int] = set()
    try:
        tokens = tokenize.generate_tokens(io.StringIO(source).readline)
        for tok in tokens:
            tok_type = tok.type
            if tok_type in (tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.ENCODING, tokenize.ENDMARKER):
                continue
            if tok_type == tokenize.STRING:
                # Standalone string expressions: treat as docstring/data literal
                # if they sit alone on the line. The ast pass already caught
                # real docstrings; this catches pure string constants used as
                # comments. We count them as non-executable.
                continue
            start_line = tok.start[0]
            if start_line in doc_lines:
                continue
            exec_lines.add(start_line)
    except (tokenize.TokenizeError, IndentationError):
        # Fallback: count non-blank, non-comment lines directly.
        for i, line in enumerate(raw_lines, 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if i in doc_lines:
                continue
            exec_lines.add(i)

    return raw_loc, len(exec_lines)


# ---------------------------------------------------------------------------
# Cyclomatic complexity
# ---------------------------------------------------------------------------


class _CCVisitor(ast.NodeVisitor):
    """Stdlib cyclomatic-complexity visitor.

    For each function / method: start at 1 (the entry edge) and add 1 per
    decision point. This matches radon's default behavior closely enough for
    trend-tracking — we care about direction and relative weight, not
    absolute parity with external tools.
    """

    DECISION_NODES = (
        ast.If,
        ast.For,
        ast.AsyncFor,
        ast.While,
        ast.ExceptHandler,
        ast.With,
        ast.AsyncWith,
        ast.Assert,
        ast.IfExp,
        ast.comprehension,  # each generator adds one
    )

    def __init__(self) -> None:
        self.total_cc = 0
        self.function_count = 0
        self._current_cc: int | None = None

    def _enter_function(self) -> None:
        if self._current_cc is not None:
            # Nested function — flush parent first by stashing and resetting.
            # Simpler approach: treat every function independently by using a
            # stack.
            pass

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._handle_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._handle_function(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802
        # Lambdas count as 1-CC mini-functions.
        self.function_count += 1
        self.total_cc += 1 + _count_decisions(node.body)
        self.generic_visit(node)

    def _handle_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.function_count += 1
        cc = 1
        for child in ast.walk(node):
            if child is node:
                continue
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                # Don't double-count nested function bodies — they'll be
                # visited on their own.
                continue
            cc += _decision_weight(child)
        self.total_cc += cc
        # Descend into nested functions so they get counted too.
        for child in node.body:
            self.visit(child)


def _decision_weight(node: ast.AST) -> int:
    if isinstance(node, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.ExceptHandler, ast.With, ast.AsyncWith, ast.Assert, ast.IfExp)):
        return 1
    if isinstance(node, ast.BoolOp):
        # `a and b and c` → 2 extra decisions (n - 1).
        return max(0, len(node.values) - 1)
    if isinstance(node, ast.comprehension):
        # Each `for` clause in a comprehension plus each `if` filter.
        return 1 + len(node.ifs)
    return 0


def _count_decisions(node: ast.AST) -> int:
    total = 0
    for child in ast.walk(node):
        total += _decision_weight(child)
    return total


def _cc_via_ast(source: str) -> tuple[int, int]:
    """Return (total_cc, function_count) via stdlib ast visitor."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return 0, 0
    visitor = _CCVisitor()
    visitor.visit(tree)
    return visitor.total_cc, visitor.function_count


def _cc_via_radon(source: str) -> tuple[int, int]:
    try:
        blocks = _radon_cc_visit(source)
    except Exception:  # noqa: BLE001
        return _cc_via_ast(source)
    total = sum(getattr(b, "complexity", 0) for b in blocks)
    return total, len(blocks)


def compute_cc(source: str) -> tuple[int, int]:
    if _HAS_RADON:
        return _cc_via_radon(source)
    return _cc_via_ast(source)


# ---------------------------------------------------------------------------
# File-level analysis
# ---------------------------------------------------------------------------


def analyze_file(path: Path, root: Path) -> FileMetrics:
    rel = path.resolve().relative_to(root)
    try:
        source = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return FileMetrics(path=str(rel), parse_error=True)
    raw_loc, exec_loc = _executable_lines(source)
    cc, fn_count = compute_cc(source)
    return FileMetrics(
        path=str(rel),
        raw_loc=raw_loc,
        executable_loc=exec_loc,
        cyclomatic=cc,
        functions=fn_count,
    )


def _top_level_dir(rel_path: str) -> str:
    parts = Path(rel_path).parts
    if not parts:
        return "(root)"
    if len(parts) == 1:
        return "(root)"
    return parts[0] + "/"


def build_snapshot(root: Path, focus: Path | None) -> Snapshot:
    per_dir: dict[str, DirectoryRollup] = {}
    files: list[FileMetrics] = []
    total_raw = 0
    total_exec = 0
    total_cc = 0
    total_fn = 0
    for py_path in iter_python_files(root, focus):
        try:
            py_path.resolve().relative_to(root)
        except ValueError:
            continue
        metrics = analyze_file(py_path, root)
        files.append(metrics)
        total_raw += metrics.raw_loc
        total_exec += metrics.executable_loc
        total_cc += metrics.cyclomatic
        total_fn += metrics.functions
        top = _top_level_dir(metrics.path)
        roll = per_dir.setdefault(top, DirectoryRollup(name=top))
        roll.file_count += 1
        roll.raw_loc += metrics.raw_loc
        roll.executable_loc += metrics.executable_loc
        roll.cyclomatic += metrics.cyclomatic
        roll.functions += metrics.functions
    return Snapshot(
        generated_at=date.today().isoformat(),
        root=str(root),
        radon_available=_HAS_RADON,
        file_count=len(files),
        raw_loc=total_raw,
        executable_loc=total_exec,
        cyclomatic=total_cc,
        functions=total_fn,
        per_directory=per_dir,
        files=files,
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def render_text(snap: Snapshot, delta: Snapshot | None = None) -> str:
    out: list[str] = []
    out.append(f"GRID Executable Code Metrics — {snap.generated_at}")
    out.append("==========================================")
    out.append(f"Root:                    {snap.root}")
    out.append(f"Radon available:         {snap.radon_available}")
    out.append(f"Total files:             {_fmt_int(snap.file_count)}")
    out.append(f"Total raw LOC:           {_fmt_int(snap.raw_loc)}")
    out.append(f"Total executable LOC:    {_fmt_int(snap.executable_loc)}")
    out.append(f"Total cyclomatic:        {_fmt_int(snap.cyclomatic)}")
    out.append(f"Total functions:         {_fmt_int(snap.functions)}")
    avg_cc = (snap.cyclomatic / snap.file_count) if snap.file_count else 0.0
    avg_fn_cc = (snap.cyclomatic / snap.functions) if snap.functions else 0.0
    exec_ratio = (snap.executable_loc / snap.raw_loc * 100) if snap.raw_loc else 0.0
    out.append(f"Avg CC per file:         {avg_cc:.1f}")
    out.append(f"Avg CC per function:     {avg_fn_cc:.1f}")
    out.append(f"Executable / raw ratio:  {exec_ratio:.1f}%")
    out.append("")
    out.append("By directory:")
    rows = sorted(snap.per_directory.values(), key=lambda r: -r.executable_loc)
    name_w = max((len(r.name) for r in rows), default=10)
    for r in rows:
        out.append(
            f"  {r.name:<{name_w}}  {r.file_count:>4} files  "
            f"{_fmt_int(r.executable_loc):>9} exec  "
            f"{_fmt_int(r.cyclomatic):>7} cc  "
            f"{_fmt_int(r.raw_loc):>9} raw"
        )
    if delta is not None:
        out.append("")
        out.append(f"Delta vs {delta.generated_at} ({delta.root})")
        out.append("------------------------------------------")
        out.append(_delta_line("files", snap.file_count, delta.file_count))
        out.append(_delta_line("raw loc", snap.raw_loc, delta.raw_loc))
        out.append(_delta_line("exec loc", snap.executable_loc, delta.executable_loc))
        out.append(_delta_line("cyclomatic", snap.cyclomatic, delta.cyclomatic))
        out.append(_delta_line("functions", snap.functions, delta.functions))
        out.append("")
        out.append("By directory (delta):")
        all_names = sorted(set(snap.per_directory) | set(delta.per_directory))
        for name in all_names:
            cur = snap.per_directory.get(name, DirectoryRollup(name=name))
            old = delta.per_directory.get(name, DirectoryRollup(name=name))
            dloc = cur.executable_loc - old.executable_loc
            dcc = cur.cyclomatic - old.cyclomatic
            dfiles = cur.file_count - old.file_count
            if dloc == 0 and dcc == 0 and dfiles == 0:
                continue
            out.append(
                f"  {name:<{name_w}}  files {dfiles:+d}  "
                f"exec {dloc:+,}  cc {dcc:+,}"
            )
    return "\n".join(out)


def _delta_line(label: str, current: int, prior: int) -> str:
    diff = current - prior
    pct = (diff / prior * 100) if prior else 0.0
    sign = "+" if diff >= 0 else ""
    return f"  {label:<12} {_fmt_int(current):>12}  ({sign}{_fmt_int(diff)}, {sign}{pct:.1f}%)"


def snapshot_to_dict(snap: Snapshot) -> dict:
    return {
        "generated_at": snap.generated_at,
        "root": snap.root,
        "radon_available": snap.radon_available,
        "file_count": snap.file_count,
        "raw_loc": snap.raw_loc,
        "executable_loc": snap.executable_loc,
        "cyclomatic": snap.cyclomatic,
        "functions": snap.functions,
        "per_directory": {k: asdict(v) for k, v in snap.per_directory.items()},
        "files": [asdict(f) for f in snap.files],
    }


def snapshot_from_dict(data: dict) -> Snapshot:
    per_dir = {k: DirectoryRollup(**v) for k, v in data.get("per_directory", {}).items()}
    files = [FileMetrics(**f) for f in data.get("files", [])]
    return Snapshot(
        generated_at=data.get("generated_at", ""),
        root=data.get("root", ""),
        radon_available=data.get("radon_available", False),
        file_count=data.get("file_count", 0),
        raw_loc=data.get("raw_loc", 0),
        executable_loc=data.get("executable_loc", 0),
        cyclomatic=data.get("cyclomatic", 0),
        functions=data.get("functions", 0),
        per_directory=per_dir,
        files=files,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--dir", default=None, help="Focus a single directory (relative to repo root).")
    parser.add_argument("--delta", default=None, help="Compare against a saved JSON baseline.")
    parser.add_argument("--save", default=None, help="Save current snapshot to this JSON path.")
    parser.add_argument("--root", default=None, help="Repo root (auto-detected if omitted).")
    args = parser.parse_args(argv)

    if args.root:
        root = Path(args.root).resolve()
    else:
        script_root = Path(__file__).resolve().parent.parent
        root = find_repo_root(script_root)

    focus: Path | None = None
    if args.dir:
        focus = (root / args.dir).resolve() if not Path(args.dir).is_absolute() else Path(args.dir).resolve()

    snap = build_snapshot(root, focus)

    delta_snap: Snapshot | None = None
    if args.delta:
        delta_path = Path(args.delta).resolve()
        if delta_path.exists():
            try:
                delta_snap = snapshot_from_dict(json.loads(delta_path.read_text()))
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                print(f"WARN: could not load baseline {delta_path}: {exc}", file=sys.stderr)
        else:
            print(f"WARN: baseline {delta_path} does not exist", file=sys.stderr)

    if args.save:
        save_path = Path(args.save).resolve()
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_text(json.dumps(snapshot_to_dict(snap), indent=2))

    if args.json:
        payload = {"current": snapshot_to_dict(snap)}
        if delta_snap is not None:
            payload["baseline"] = snapshot_to_dict(delta_snap)
        print(json.dumps(payload, indent=2))
    else:
        print(render_text(snap, delta_snap))

    return 0


if __name__ == "__main__":
    sys.exit(main())
