#!/usr/bin/env python3
"""pre_create_check.py — Coverage probe for GRID modules.

Call this BEFORE creating a new module to check whether the concept is
already covered somewhere in the repo. Walks the canonical module
directories and migrations, and reports every file / function / table
that already touches the keyword.

Usage:
    python3 scripts/pre_create_check.py <keyword>
    python3 scripts/pre_create_check.py chokepoint --verbose
    python3 scripts/pre_create_check.py bottleneck --synonyms "chokepoint,single-source"
    python3 scripts/pre_create_check.py <keyword> --json

Exit codes:
    0 — coverage exists (extend instead of creating)
    1 — no coverage (safe to create)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterable

# Directories to walk. Relative to repo root.
DEFAULT_SCAN_DIRS = [
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
    "api/routers",
]

MIGRATION_DIRS = ["migrations", "alembic/versions"]
SCHEMA_FILES = ["schema.sql", "db/schema.sql"]

SKIP_DIR_NAMES = {
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
}

PY_EXT = {".py"}
SQL_EXT = {".sql"}

DEF_RE = re.compile(r"^\s*(?:async\s+)?def\s+([a-zA-Z_][\w]*)\s*\(([^)]*)\)(\s*->\s*[^:]+)?:")
DOCSTRING_RE = re.compile(r'^\s*(?:"""|\'\'\')(.*?)(?:"""|\'\'\')', re.DOTALL | re.MULTILINE)
READS_RE = re.compile(r'(?:\bFROM|\bJOIN)\s+([a-zA-Z_][\w]*)', re.IGNORECASE)
WRITES_RE = re.compile(r'(?:INSERT\s+INTO|\bUPDATE\s+|UPSERT\s+INTO)\s+([a-zA-Z_][\w]*)', re.IGNORECASE)
# Python keywords / common import/module tokens we never want reported as "tables".
NON_TABLE_NOISE = {
    "__future__", "dataclasses", "datetime", "loguru", "sqlalchemy", "collections",
    "typing", "pathlib", "json", "os", "sys", "re", "time", "math", "random",
    "functools", "itertools", "asyncio", "logging", "argparse", "enum", "dataclass",
    "dual", "select", "where", "and", "or", "not", "in", "is", "none", "true", "false",
    "import", "imports", "api", "analysis", "intelligence", "physics", "ingestion",
    "features", "discovery", "inference", "agents", "oracle", "trading",
}
TABLE_CREATE_RE = re.compile(r'CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-zA-Z_][\w\.]*)', re.IGNORECASE)
ALTER_ADD_COL_RE = re.compile(r'ALTER\s+TABLE\s+([a-zA-Z_][\w\.]*)\s+ADD\s+COLUMN(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-zA-Z_][\w]*)', re.IGNORECASE)
COLUMN_IN_CREATE_RE = re.compile(r'^\s*([a-zA-Z_][\w]*)\s+(?:TEXT|INTEGER|BIGINT|REAL|NUMERIC|BOOLEAN|TIMESTAMP|DATE|JSON|JSONB|UUID|DOUBLE|FLOAT|VARCHAR|CHAR|SMALLINT)', re.IGNORECASE | re.MULTILINE)


@dataclass
class FileHit:
    path: str
    line_count: int
    mention_count: int
    functions: list[dict] = field(default_factory=list)  # {name, signature, returns, matches_keyword}
    docstring: str | None = None
    reads: list[str] = field(default_factory=list)
    writes: list[str] = field(default_factory=list)
    first_match_line: int | None = None


@dataclass
class MigrationHit:
    file: str
    table: str
    column: str | None
    migration_id: str


@dataclass
class Report:
    keyword: str
    synonyms: list[str]
    files: list[FileHit]
    migrations: list[MigrationHit]
    schema_hits: list[dict]
    decision: str
    coverage_exists: bool


def iter_files(root: Path, rel_dirs: Iterable[str], extensions: set[str], include_tests: bool) -> Iterable[Path]:
    seen: set[Path] = set()
    for rel in rel_dirs:
        base = root / rel
        if not base.exists() or not base.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            # prune
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES and (include_tests or d != "tests")]
            for fn in filenames:
                p = Path(dirpath) / fn
                if p.suffix.lower() in extensions and p not in seen:
                    seen.add(p)
                    yield p


def compile_patterns(terms: list[str]) -> re.Pattern:
    # Case-insensitive OR. Terms may contain hyphens/underscores; escape them.
    parts = [re.escape(t) for t in terms if t.strip()]
    if not parts:
        parts = [""]
    return re.compile("|".join(parts), re.IGNORECASE)


def read_text_safe(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except (OSError, UnicodeDecodeError):
        return None


def extract_docstring(text: str) -> str | None:
    # First module-level triple-quoted literal.
    m = DOCSTRING_RE.search(text)
    if not m:
        return None
    doc = m.group(1).strip()
    return doc.splitlines()[0][:200] if doc else None


def extract_functions(text: str, pattern: re.Pattern) -> list[dict]:
    funcs: list[dict] = []
    for m in DEF_RE.finditer(text):
        name = m.group(1)
        params = m.group(2).strip()
        returns = (m.group(3) or "").strip()
        sig = f"{name}({params}){returns}".strip()
        matches_keyword = bool(pattern.search(name))
        funcs.append({
            "name": name,
            "signature": sig,
            "matches_keyword": matches_keyword,
        })
    return funcs


def extract_tables(text: str) -> tuple[list[str], list[str]]:
    # Only scan inside SQL string literals to avoid matching Python `from x import y`.
    sql_blobs: list[str] = []
    for m in re.finditer(r'(?:"""|\'\'\'|"|\')([^"\'`]{20,3000}?)(?:"""|\'\'\'|"|\')', text, re.DOTALL):
        blob = m.group(1)
        if re.search(r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|JOIN|FROM)\b', blob, re.IGNORECASE):
            sql_blobs.append(blob)
    joined = "\n".join(sql_blobs)
    reads = sorted(set(m.group(1).lower() for m in READS_RE.finditer(joined)))
    writes = sorted(set(m.group(1).lower() for m in WRITES_RE.finditer(joined)))
    reads = [r for r in reads if r not in NON_TABLE_NOISE and len(r) > 2]
    writes = [w for w in writes if w not in NON_TABLE_NOISE and len(w) > 2]
    return reads, writes


def scan_code_file(path: Path, pattern: re.Pattern, root: Path) -> FileHit | None:
    text = read_text_safe(path)
    if text is None:
        return None
    matches = list(pattern.finditer(text))
    if not matches:
        return None
    lines = text.splitlines()
    line_count = len(lines)
    # Find first match line
    first_line = None
    offset = 0
    for i, line in enumerate(lines, 1):
        if pattern.search(line):
            first_line = i
            break
        offset += len(line) + 1
    funcs = extract_functions(text, pattern)
    # Keep only funcs whose name matches OR that are defined close to a mention.
    # For simplicity, if the file matches, keep keyword-matching funcs, otherwise top 5.
    keyword_funcs = [f for f in funcs if f["matches_keyword"]]
    if keyword_funcs:
        shown_funcs = keyword_funcs
    else:
        shown_funcs = funcs[:5]
    reads, writes = extract_tables(text)
    return FileHit(
        path=str(path.relative_to(root)),
        line_count=line_count,
        mention_count=len(matches),
        functions=shown_funcs,
        docstring=extract_docstring(text),
        reads=reads,
        writes=writes,
        first_match_line=first_line,
    )


def scan_migrations(root: Path, pattern: re.Pattern) -> list[MigrationHit]:
    hits: list[MigrationHit] = []
    for mig_rel in MIGRATION_DIRS:
        base = root / mig_rel
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.sql")):
            text = read_text_safe(path)
            if text is None or not pattern.search(text):
                continue
            mig_id = path.stem.split("_", 1)[0]
            matched_tables: set[str] = set()
            # Tables created in this migration that match keyword
            for m in TABLE_CREATE_RE.finditer(text):
                tbl = m.group(1)
                if pattern.search(tbl):
                    matched_tables.add(tbl)
                    hits.append(MigrationHit(file=str(path.relative_to(root)), table=tbl, column=None, migration_id=mig_id))
            # ALTER TABLE ... ADD COLUMN matching keyword
            for m in ALTER_ADD_COL_RE.finditer(text):
                tbl, col = m.group(1), m.group(2)
                if pattern.search(col) or pattern.search(tbl):
                    hits.append(MigrationHit(file=str(path.relative_to(root)), table=tbl, column=col, migration_id=mig_id))
            # Columns inside CREATE TABLE statements
            # Crude: scan CREATE TABLE blocks for column definitions.
            for ct in re.finditer(r'CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-zA-Z_][\w\.]*)\s*\((.*?)\)\s*;', text, re.IGNORECASE | re.DOTALL):
                tbl = ct.group(1)
                body = ct.group(2)
                for cm in COLUMN_IN_CREATE_RE.finditer(body):
                    col = cm.group(1)
                    if pattern.search(col):
                        hits.append(MigrationHit(file=str(path.relative_to(root)), table=tbl, column=col, migration_id=mig_id))
    # Dedup
    seen: set[tuple] = set()
    unique: list[MigrationHit] = []
    for h in hits:
        k = (h.file, h.table, h.column)
        if k in seen:
            continue
        seen.add(k)
        unique.append(h)
    return unique


def scan_schema(root: Path, pattern: re.Pattern) -> list[dict]:
    hits: list[dict] = []
    for rel in SCHEMA_FILES:
        p = root / rel
        if not p.exists():
            continue
        text = read_text_safe(p)
        if text is None:
            continue
        for ct in re.finditer(r'CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-zA-Z_][\w\.]*)\s*\((.*?)\)\s*;', text, re.IGNORECASE | re.DOTALL):
            tbl = ct.group(1)
            body = ct.group(2)
            if pattern.search(tbl):
                hits.append({"file": rel, "table": tbl, "column": None})
            for cm in COLUMN_IN_CREATE_RE.finditer(body):
                col = cm.group(1)
                if pattern.search(col):
                    hits.append({"file": rel, "table": tbl, "column": col})
    return hits


def build_report(root: Path, keyword: str, synonyms: list[str], include_tests: bool, max_files: int) -> Report:
    terms = [keyword] + synonyms
    pattern = compile_patterns(terms)

    file_hits: list[FileHit] = []
    for path in iter_files(root, DEFAULT_SCAN_DIRS, PY_EXT, include_tests):
        hit = scan_code_file(path, pattern, root)
        if hit is not None:
            file_hits.append(hit)

    # Sort: files where keyword appears in the name first, then by mention count.
    def sort_key(h: FileHit) -> tuple:
        name_match = bool(pattern.search(Path(h.path).stem))
        return (0 if name_match else 1, -h.mention_count, h.path)

    file_hits.sort(key=sort_key)
    if max_files > 0:
        file_hits = file_hits[:max_files]

    migration_hits = scan_migrations(root, pattern)
    schema_hits = scan_schema(root, pattern)

    coverage_exists = bool(file_hits or migration_hits or schema_hits)
    if coverage_exists and file_hits:
        best = file_hits[0].path
        decision = f"Coverage EXISTS. Extend {best} instead of creating a new module."
    elif coverage_exists:
        # only migration/schema hits
        decision = "Schema coverage EXISTS but no Python module. Consider where the logic belongs."
    else:
        decision = "No existing coverage. Safe to create."

    return Report(
        keyword=keyword,
        synonyms=synonyms,
        files=file_hits,
        migrations=migration_hits,
        schema_hits=schema_hits,
        decision=decision,
        coverage_exists=coverage_exists,
    )


def render_text(report: Report, verbose: bool) -> str:
    out: list[str] = []
    if not report.coverage_exists:
        out.append(f'No existing coverage for "{report.keyword}".')
        if report.synonyms:
            out.append(f'(also searched: {", ".join(report.synonyms)})')
        out.append("Safe to create a new module.")
        out.append("")
        out.append(f"DECISION: {report.decision}")
        return "\n".join(out)

    out.append(f'Existing coverage for "{report.keyword}":')
    if report.synonyms:
        out.append(f'(also searched: {", ".join(report.synonyms)})')
    out.append("")
    if report.files:
        out.append(f"FILES ({len(report.files)}):")
        for h in report.files:
            out.append(f"  {h.path} ({h.line_count} lines, {h.mention_count} mentions)")
            if verbose and h.docstring:
                out.append(f"    doc: {h.docstring}")
            shown = h.functions[:8] if not verbose else h.functions
            for f in shown:
                marker = "*" if f["matches_keyword"] else "-"
                out.append(f"    {marker} {f['signature']}")
            if h.reads:
                out.append(f"    reads: {', '.join(h.reads[:6])}")
            if h.writes:
                out.append(f"    writes: {', '.join(h.writes[:6])}")
        out.append("")
    if report.migrations:
        out.append("DATABASE TABLES / COLUMNS:")
        for m in report.migrations:
            col = f".{m.column}" if m.column else ""
            out.append(f"  {m.table}{col}  (migration {m.migration_id}: {m.file})")
        out.append("")
    if report.schema_hits:
        out.append("SCHEMA.SQL HITS:")
        for h in report.schema_hits:
            col = f".{h['column']}" if h["column"] else ""
            out.append(f"  {h['table']}{col}  ({h['file']})")
        out.append("")
    out.append(f"DECISION: {report.decision}")
    return "\n".join(out)


def render_json(report: Report) -> str:
    payload = {
        "keyword": report.keyword,
        "synonyms": report.synonyms,
        "coverage_exists": report.coverage_exists,
        "decision": report.decision,
        "files": [asdict(f) for f in report.files],
        "migrations": [asdict(m) for m in report.migrations],
        "schema_hits": report.schema_hits,
    }
    return json.dumps(payload, indent=2, default=str)


def find_repo_root(start: Path) -> Path:
    # Walk up until we see a known marker. Fall back to start.
    cur = start.resolve()
    markers = {"intelligence", "migrations", "api"}
    while cur != cur.parent:
        names = {p.name for p in cur.iterdir() if p.is_dir()} if cur.is_dir() else set()
        if len(markers & names) >= 2:
            return cur
        cur = cur.parent
    return start.resolve()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check existing coverage before creating a new GRID module.")
    parser.add_argument("keyword", help="Concept / keyword to probe (case-insensitive).")
    parser.add_argument("--verbose", action="store_true", help="Show docstrings and all functions.")
    parser.add_argument("--json", action="store_true", help="Emit JSON for programmatic use.")
    parser.add_argument("--max-files", type=int, default=25, help="Cap reported file count (0 = unlimited).")
    parser.add_argument("--include-tests", action="store_true", help="Also scan tests/ directories.")
    parser.add_argument("--synonyms", default="", help="Comma-separated synonyms to also search for.")
    parser.add_argument("--root", default=None, help="Repository root (auto-detected if omitted).")
    args = parser.parse_args(argv)

    if args.root:
        root = Path(args.root).resolve()
    else:
        # Prefer the script's parent-of-scripts dir, else cwd.
        script_root = Path(__file__).resolve().parent.parent
        root = script_root if (script_root / "intelligence").exists() or (script_root / "api").exists() else Path.cwd()
        root = find_repo_root(root)

    synonyms = [s.strip() for s in args.synonyms.split(",") if s.strip()]
    report = build_report(root, args.keyword, synonyms, args.include_tests, args.max_files)

    if args.json:
        print(render_json(report))
    else:
        print(render_text(report, args.verbose))

    return 0 if report.coverage_exists else 1


if __name__ == "__main__":
    sys.exit(main())
