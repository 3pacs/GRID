"""Guard: no ``resolved_series`` read fences ``release_date`` against an as-of without ``vintage_date``.

Knowable-at for a resolved row is the later of ``release_date`` and
``vintage_date``. The resolver stamps both with the pull date, but the schema
does not enforce it: the TwelveData bridge
(``scripts/backfill_resolved_series_from_ticker_metrics.sql``, E1-V7c) wrote
``release_date = obs_date`` with ``vintage_date`` = the fetch date, up to ~60
days later. A point-in-time read that bounds only ``release_date <= :as_of``
serves those rows to every ``as_of`` before GRID held them. ``store/pit.py``
fences both (GRID#1365); the direct readers were fenced with
``tests/test_direct_readers_vintage_fence_pg.py``.

Rule: in one SQL statement that reads ``resolved_series`` (``FROM`` or
``JOIN``), every ``release_date`` upper bound against a bound parameter
(``release_date <= :as_of``, ``<= CAST(:cutoff AS date)``, ``<= %(x)s``,
``<= %s``) needs a matching ``vintage_date`` upper bound. A statement is one
Python string literal (implicit concatenation, f-string text parts and ``+``
chains of literals are joined) or one ``;``-separated statement of a ``.sql``
file. Comparisons against a column (``r.release_date <= r.obs_date``) are not
as-of fences and are ignored. Read through ``store.pit.PITStore`` where you
can; it fences both and honours retractions.
"""

from __future__ import annotations

import ast
import os
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parent.parent
_SKIP_DIRS = frozenset({
    ".git", ".claude", "tests", "evals", "node_modules", "pwa", "pwa_dist", "__pycache__",
    ".venv", "venv", ".pytest_cache", ".server-logs",
})

_READ = re.compile(r"\b(?:from|join)\s+resolved_series\b", re.I)
_PARAM = r"(?:CAST\s*\(\s*)?(?::[A-Za-z_]\w*|%\(\w+\)s|%s)"


def _fence(col: str) -> re.Pattern[str]:
    return re.compile(
        rf"\b{col}\s*<=?\s*{_PARAM}|{_PARAM}(?:\s+AS\s+\w+\s*\))?\s*>=?\s*(?:\w+\.)?{col}\b", re.I,
    )


_RELEASE_FENCE = _fence("release_date")
_VINTAGE_FENCE = _fence("vintage_date")

# Files whose release-only fences are fixed by another open PR. A ceiling, not
# an allowance for new reads: GRID#1365 (claude/pit-vintage-fence) adds
# ``vintage_date <= :aod`` to get_pit's two statements and
# PIT_WINDOW_SELECT_SQL. Delete this entry once #1365 is on main.
PENDING_ELSEWHERE: dict[str, int] = {
    "store/pit.py": 3,
}


def _string_value(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value if isinstance(v, ast.Constant) else "{}" for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _string_value(node.left), _string_value(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def python_statements(source: str) -> list[str]:
    """Every maximal string literal (or literal ``+`` chain) in *source*."""
    out: list[str] = []
    consumed: set[int] = set()
    for node in ast.walk(ast.parse(source)):
        if id(node) in consumed:
            continue
        value = _string_value(node)
        if value is None:
            continue
        out.append(value)
        consumed.update(id(n) for n in ast.walk(node))
    return out


def sql_statements(source: str) -> list[str]:
    no_comments = re.sub(r"--[^\n]*", "", source)
    return [s for s in no_comments.split(";") if s.strip()]


def unfenced(statement: str) -> int:
    """Release-only as-of fences in one statement that reads resolved_series."""
    if not _READ.search(statement):
        return 0
    return max(0, len(_RELEASE_FENCE.findall(statement)) - len(_VINTAGE_FENCE.findall(statement)))


def _files():
    for dirpath, dirnames, filenames in os.walk(REPO):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for name in filenames:
            if name.endswith((".py", ".sql")):
                path = pathlib.Path(dirpath) / name
                yield path.relative_to(REPO).as_posix(), path


def scan() -> dict[str, int]:
    found: dict[str, int] = {}
    for rel, path in _files():
        source = path.read_text(encoding="utf-8", errors="ignore")
        if "resolved_series" not in source:
            continue
        if path.suffix == ".py":
            try:
                statements = python_statements(source)
            except SyntaxError:
                continue
        else:
            statements = sql_statements(source)
        n = sum(unfenced(s) for s in statements)
        if n:
            found[rel] = n
    return found


def test_no_release_only_as_of_fence_on_resolved_series():
    found = scan()
    regressions = {p: (n, PENDING_ELSEWHERE.get(p, 0)) for p, n in found.items()
                   if n > PENDING_ELSEWHERE.get(p, 0)}
    assert not regressions, (
        "resolved_series read(s) bound release_date by an as-of without vintage_date "
        f"(file: found, allowed): {regressions}. Add `AND rs.vintage_date <= :as_of` next to "
        "the release fence, or read through store.pit.PITStore. See this module's docstring."
    )


def test_scanner_flags_a_release_only_read():
    bad = '''
from sqlalchemy import text
Q = text("""
    SELECT rs.value FROM resolved_series rs
    WHERE rs.feature_id = :f AND rs.release_date <= :as_of
""")
'''
    assert sum(unfenced(s) for s in python_statements(bad)) == 1


def test_scanner_accepts_a_fenced_read_and_ignores_column_comparisons():
    good = '''
def q(registry):
    a = text(f"""SELECT 1 FROM resolved_series rs JOIN {registry} r ON r.id = rs.feature_id
                 WHERE (CAST(:cut AS date) IS NULL
                        OR (rs.release_date <= CAST(:cut AS date) AND rs.vintage_date <= CAST(:cut AS date)))""")
    b = "SELECT count(*) FROM resolved_series r WHERE r.release_date <= r.obs_date"
    c = ("SELECT 1 FROM resolved_series WHERE release_date <= %(d)s "
         "AND vintage_date <= %(d)s")
    return a, b, c
'''
    assert sum(unfenced(s) for s in python_statements(good)) == 0


def test_scanner_joins_concatenated_literals_and_reads_sql_files():
    split = 'Q = ("SELECT v FROM resolved_series rs " + "WHERE rs.release_date <= :d")\n'
    assert sum(unfenced(s) for s in python_statements(split)) == 1
    reversed_ = 'Q = "SELECT v FROM resolved_series WHERE :d >= release_date"\n'
    assert sum(unfenced(s) for s in python_statements(reversed_)) == 1
    sql = ("-- a reader\nSELECT 1 FROM resolved_series WHERE release_date <= :d;\n"
           "SELECT 1 FROM resolved_series WHERE release_date <= :d AND vintage_date <= :d;")
    assert sum(unfenced(s) for s in sql_statements(sql)) == 1


def test_scan_sees_the_fenced_direct_readers():
    """A scanner that stopped matching would pass everything: pin what it must see."""
    seen: dict[str, int] = {}
    for rel in ("alpha_research/conviction_scorer.py", "alpha_research/data/panel_builder.py",
                "alpha_research/signals/credit_cycle.py", "oracle/prediction_context.py",
                "store/astrogrid.py"):
        statements = python_statements((REPO / rel).read_text(encoding="utf-8"))
        seen[rel] = sum(1 for s in statements if _READ.search(s) and _RELEASE_FENCE.search(s))
    assert seen == {
        "alpha_research/conviction_scorer.py": 3,
        "alpha_research/data/panel_builder.py": 1,
        "alpha_research/signals/credit_cycle.py": 1,
        "oracle/prediction_context.py": 1,
        "store/astrogrid.py": 2,
    }
