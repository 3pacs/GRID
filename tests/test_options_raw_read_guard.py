"""Guard: no new read of the options tables that goes around the Schwab filter.

Schwab-captured options rows reach admin requests only (#1299, #1322). Every
reader of ``options_daily_signals``, ``options_snapshots`` (view and
``options_snapshots_all``) and ``options_capture_batches`` reads through the
``VISIBLE_*`` / ``LATEST_*`` fragments of ``store/options_provenance.py``
(with ``visible_sql``). This test scans every Python module outside
``tests/`` and ``migrations/`` for SQL text that names one of those tables
after ``FROM`` or ``JOIN`` and freezes the reads that are allowed to, each
with the reason. Counts may only go down. A new raw read fails the build with
its file; route it through the fragments, or add it here with a reason that
says why no Schwab value can leave through it.

What is scanned: string constants (docstrings excluded), case-insensitive.
"""

from __future__ import annotations

import ast
import pathlib
import re
import warnings

REPO = pathlib.Path(__file__).resolve().parent.parent
SKIP_DIRS = {"tests", "migrations", "notebooks", "pwa", "node_modules", ".git", ".venv", "venv",
             "__pycache__", "site-packages"}
SANCTIONED = {"store/options_provenance.py"}
_RAW_READ = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:options_daily_signals|options_snapshots_all|options_snapshots|options_capture_batches)\b",
    re.I)

# Allowed raw reads on 2026-11-21 (#1322). Lower or delete an entry when a
# file moves onto the fragments.
ALLOWED_RAW_READS: dict[str, int] = {
    # Admin-only: served by gamma_watch /board, require_role("admin").
    "api/gamma_board.py": 2,
    # Writer: next capture ordinal, and the run's own completion count.
    "ingestion/options.py": 2,
    # Ticker universe only (which tickers have rows), never a value.
    "ingestion/altdata/earnings_calendar.py": 1,
    "oracle/engine.py": 1,
    "oracle/forecaster_adapter.py": 1,
    # Change detection for the precompute job: latest date and a row count.
    "intelligence/api_precompute.py": 2,
    # Freshness probe: max(signal_date), max(created_at).
    "store/materialization_provenance.py": 1,
    # Opt-in diagnostic, not imported by the app (operator-run research panel).
    "analysis/options_capture_panel.py": 2,
    # Operator tooling: the GEM capture's own post-write verification and the
    # Hermes freshness fixer read batch ids and counts of a run they just made.
    "scripts/gem_daily_capture.py": 2,
    "scripts/hermes_fixers.py": 1,
    # Disposable-container schema check (docker, no GRID database).
    "scripts/checks/cboe_clock_pg.py": 4,
}


def _docstring_ids(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list):
            for stmt in body:
                if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) \
                        and isinstance(stmt.value.value, str):
                    ids.add(id(stmt.value))
    return ids


def raw_reads() -> dict[str, int]:
    found: dict[str, int] = {}
    for path in REPO.rglob("*.py"):
        rel = path.relative_to(REPO)
        if SKIP_DIRS.intersection(rel.parts) or rel.as_posix() in SANCTIONED:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SyntaxWarning)
                tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        skip = _docstring_ids(tree)
        count = sum(len(_RAW_READ.findall(node.value)) for node in ast.walk(tree)
                    if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skip)
        if count:
            found[rel.as_posix()] = count
    return found


def test_no_new_raw_options_reads() -> None:
    over = {path: n for path, n in raw_reads().items() if n > ALLOWED_RAW_READS.get(path, 0)}
    assert not over, (
        "Raw read of an options table outside store/options_provenance.py (Schwab rows would reach "
        f"non-admin readers): {over}. Read through VISIBLE_*/LATEST_* with visible_sql(), or add an "
        "ALLOWED_RAW_READS entry saying why no Schwab value can leave through it."
    )


def test_allowed_entries_are_still_accurate() -> None:
    found = raw_reads()
    stale = {path: (n, found.get(path, 0)) for path, n in ALLOWED_RAW_READS.items() if found.get(path, 0) != n}
    assert not stale, f"ALLOWED_RAW_READS out of date (allowed, found): {stale}; lower or delete the entry"
