"""E1 gate 3: provenance of every ``raw_series`` row.

Static (AST) scan of every non-test module for SQL that inserts into
``raw_series``; per writer file, every insert must:

* name ``source_id`` and ``pull_status`` (NOT NULL, no default in
  ``schema.sql`` -- an insert without them cannot record where or how a value
  came from, and on production it fails, i.e. the writer is dead);
* get ``pull_timestamp`` explicitly or from the schema's ``DEFAULT NOW()``
  (asserted below), never from anything else;
* use only real ``raw_series`` columns;
* write only ``SUCCESS`` / ``PARTIAL`` / ``FAILED`` (``QUARANTINED`` is set by
  an explicit quarantine, never by a writer);
* never rewrite a stored row in place (``ON CONFLICT ... DO UPDATE`` of the
  value/time/identity): ``raw_series`` is an append-only pull log and a
  rewritten vintage destroys the point-in-time record.

One meaning per source: every module that downloads yfinance prices and
writes ``raw_series`` is declared in :data:`PRICE_WRITERS` with the
``source_catalog`` name it writes under and its ``auto_adjust`` flag; the
flag must be passed explicitly at every download and two writers sharing a
source name must share the flag (the April 2026 contamination:
``scripts/fill_missing_features.py`` wrote ``auto_adjust=True`` closes under
yfinance's source next to ``yfinance_pull``'s ``auto_adjust=False``).

The fixture test (``test_gates_pg.py``) runs the shared writer
(``BasePuller._insert_raw``) against the real DDL on PostgreSQL.
"""

from __future__ import annotations

import ast
import functools
import os
import re
from dataclasses import dataclass
from pathlib import Path

import pytest

from evals.e1.known_violations import known_violation

REPO = Path(__file__).resolve().parents[2]
_SKIP_TOP = frozenset({"tests", "evals", "pwa", "notebooks", "data", "outputs", "output", "docs",
                       "migrations", "node_modules", ".git", ".github", ".claude", "server_log"})
_INSERT = re.compile(
    r"insert\s+into\s+(?:public\.)?raw_series\b\s*(\((?P<cols>[^)]*)\))?"
    r"(?P<rest>.*?)(?=insert\s+into|\Z)",
    re.I | re.S,
)
_VALUES = re.compile(r"^\s*values\s*\((?P<vals>.*)\)", re.I | re.S)
_REWRITE = re.compile(r"on\s+conflict[^;]*?do\s+update\s+set\s+(?P<sets>.*)", re.I | re.S)
_IDENTITY_OR_VALUE = ("value", "pull_timestamp", "obs_date", "source_id", "series_id", "pull_status")
ALLOWED_WRITE_STATUS = frozenset({"SUCCESS", "PARTIAL", "FAILED"})


def schema_columns() -> dict[str, str]:
    """``raw_series`` column -> its DDL line, from schema.sql."""
    text = (REPO / "schema.sql").read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE IF NOT EXISTS raw_series \((.*?)\n\);", text, re.S).group(1)
    cols = {}
    for line in body.splitlines():
        line = line.strip()
        m = re.match(r"([a-z_]+)\s+[A-Z]", line)
        if m and not line.startswith("--"):
            cols[m.group(1)] = line
    return cols


def _python_files():
    for dirpath, dirnames, filenames in os.walk(REPO):
        rel = Path(dirpath).relative_to(REPO)
        if rel == Path("."):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_TOP]
        dirnames[:] = [d for d in dirnames if d not in ("__pycache__", "node_modules", ".git")]
        for name in filenames:
            if name.endswith(".py"):
                yield Path(dirpath) / name


def _string_nodes(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.lineno, node.value
        elif isinstance(node, ast.JoinedStr):
            yield node.lineno, "".join(
                v.value if isinstance(v, ast.Constant) else "{dynamic}" for v in node.values
            )


@dataclass(frozen=True)
class InsertSite:
    path: str
    line: int
    columns: tuple[str, ...] | None
    values: tuple[str, ...] | None
    rewrite_sets: str | None


def _split_top(s: str) -> list[str]:
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def scan_source(rel: str, source: str) -> list[InsertSite]:
    if "raw_series" not in source.lower():
        return []
    sites = []
    for line, s in _string_nodes(ast.parse(source)):
        for m in _INSERT.finditer(s):
            rest = (m.group("rest") or "").lstrip().lower()
            if m.group("cols") is None and rest and not rest.startswith(("values", "select")):
                continue  # prose ("... and insert into raw_series."), not a statement
            cols = tuple(c.strip().lower() for c in m.group("cols").split(",")) if m.group("cols") else None
            vals_m = _VALUES.match(m.group("rest") or "")
            vals = tuple(_split_top(vals_m.group("vals"))) if vals_m else None
            rw = _REWRITE.search(m.group("rest") or "")
            sites.append(InsertSite(rel, line, cols, vals, rw.group("sets") if rw else None))
    return sites


def site_problems(site: InsertSite, columns: dict[str, str]) -> list[str]:
    where = f"{site.path}:{site.line}"
    if site.columns is None:
        return [f"{where}: INSERT INTO raw_series without an explicit column list"]
    if any("{dynamic}" in c for c in site.columns):
        return [f"{where}: dynamic column list cannot be verified"]
    problems = []
    for required in ("source_id", "pull_status"):
        if required not in site.columns:
            problems.append(f"{where}: does not set {required}")
    unknown = [c for c in site.columns if c not in columns]
    if unknown:
        problems.append(f"{where}: writes columns raw_series does not have {unknown}")
    if site.values and len(site.values) == len(site.columns) and "pull_status" in site.columns:
        status = site.values[site.columns.index("pull_status")].strip()
        literal = re.fullmatch(r"'([A-Z_]+)'", status)
        if literal and literal.group(1) not in ALLOWED_WRITE_STATUS:
            problems.append(f"{where}: writes pull_status {literal.group(1)}")
    if site.rewrite_sets:
        touched = [c for c in _IDENTITY_OR_VALUE if re.search(rf"\b{c}\s*=", site.rewrite_sets, re.I)]
        if touched:
            problems.append(f"{where}: ON CONFLICT DO UPDATE rewrites a stored row ({', '.join(touched)})")
    return problems


@functools.lru_cache(maxsize=1)
def _module_sources() -> dict[str, str]:
    out = {}
    for path in _python_files():
        try:
            out[path.relative_to(REPO).as_posix()] = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
    return out


def _all_sites() -> dict[str, list[InsertSite]]:
    by_file: dict[str, list[InsertSite]] = {}
    for rel, source in _module_sources().items():
        sites = scan_source(rel, source)
        if sites:
            by_file[rel] = sites
    return by_file


_SITES = _all_sites()

# Writer files on main that violate the rules above, as path -> known_violations
# ID. Empty since e1-v1.2 (the five former writers are fixed; the watchlist
# price cache no longer writes raw_series at all).
_KNOWN_BAD_WRITERS: dict[str, str] = {}


def _writer_params():
    for rel in sorted(_SITES):
        marks = [known_violation(_KNOWN_BAD_WRITERS[rel])] if rel in _KNOWN_BAD_WRITERS else []
        yield pytest.param(rel, id=rel, marks=marks)


def test_scan_finds_the_known_writer_population():
    # A scanner that silently stopped matching would make every writer "pass".
    assert len(_SITES) >= 60, sorted(_SITES)
    for must in ("ingestion/base.py", "ingestion/yfinance_pull.py", "ingestion/fred.py"):
        assert must in _SITES


@pytest.mark.parametrize("rel", list(_writer_params()))
def test_raw_series_writer_records_provenance_and_never_rewrites(rel):
    columns = schema_columns()
    problems = [p for site in _SITES[rel] for p in site_problems(site, columns)]
    assert not problems, "\n".join(problems)


def test_known_bad_writers_are_still_writers():
    assert set(_KNOWN_BAD_WRITERS) <= set(_SITES)


def test_schema_stamps_pull_time_and_requires_source_and_status():
    cols = schema_columns()
    assert "DEFAULT NOW()" in cols["pull_timestamp"].upper() and "NOT NULL" in cols["pull_timestamp"].upper()
    for name in ("source_id", "pull_status", "obs_date", "series_id"):
        assert "NOT NULL" in cols[name].upper() and "DEFAULT" not in cols[name].upper(), cols[name]


def test_no_raw_series_writer_bypasses_the_scan():
    """Writers the SQL scan cannot see (ORM/pandas) are refused outright."""
    hidden = []
    pattern = re.compile(r"""to_sql\(\s*['"]raw_series|Table\(\s*['"]raw_series|__tablename__\s*=\s*['"]raw_series""")
    for rel, source in _module_sources().items():
        if "raw_series" in source and pattern.search(source):
            hidden.append(rel)
    assert not hidden, hidden


def test_scanner_self_test_flags_each_rule():
    cols = schema_columns()
    bad = '''
q1 = "INSERT INTO raw_series (series_id, source_id, obs_date, value) VALUES (:a, :b, :c, :d)"
q2 = "INSERT INTO raw_series (series_id, source_id, obs_date, value, release_date, pull_status) VALUES (:a,:b,:c,:d,:e,'SUCCESS')"
q3 = "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) VALUES (:a,:b,:c,:d,'QUARANTINED')"
q4 = ("INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) VALUES (:a,:b,:c,:d,'SUCCESS') "
      "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO UPDATE SET value = EXCLUDED.value")
'''
    problems = [p for s in scan_source("fixture.py", bad) for p in site_problems(s, cols)]
    text = "\n".join(problems)
    assert "does not set pull_status" in text
    assert "release_date" in text
    assert "QUARANTINED" in text
    assert "rewrites a stored row (value)" in text
    good = 'q = "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) VALUES (:a,:b,:c,:d,:s)"'
    assert [p for s in scan_source("ok.py", good) for p in site_problems(s, cols)] == []


# ── one meaning per source ─────────────────────────────────────────────

# module -> (source_catalog name it writes raw_series under, auto_adjust at every yfinance download)
PRICE_WRITERS: dict[str, tuple[str, bool]] = {
    "ingestion/yfinance_pull.py": ("yfinance", False),
    "ingestion/altdata/fx_rates.py": ("yfinance", False),
    "api/routers/watchlist_helpers.py": ("yfinance", False),
    "ingestion/altdata/ag_commodity_futures.py": ("YFINANCE_COMMODITY_FUTURES", False),
    "ingestion/altdata/institutional_flows.py": ("INSTITUTIONAL_FLOWS", True),
    "scripts/fill_missing_features.py": ("yfinance_adjusted_extended", True),
}


def yfinance_downloads(source: str) -> list[tuple[int, object]]:
    """(line, auto_adjust) for every yfinance download/history call; ``"implicit"`` when absent."""
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("download", "history"):
            kw = [k for k in node.keywords if k.arg == "auto_adjust"]
            if not kw:
                calls.append((node.lineno, "implicit"))
            elif isinstance(kw[0].value, ast.Constant):
                calls.append((node.lineno, kw[0].value.value))
            else:
                calls.append((node.lineno, "dynamic"))
    return calls


def writes_raw_series(source: str) -> bool:
    return bool(re.search(r"insert\s+into\s+(?:public\.)?raw_series", source, re.I)) or "_insert_raw(" in source


def price_writer_problems(declared: dict[str, tuple[str, bool]], sources: dict[str, str]) -> list[str]:
    problems = []
    for rel, source in sorted(sources.items()):
        if "yfinance" in source and writes_raw_series(source) and yfinance_downloads(source) and rel not in declared:
            problems.append(f"{rel}: downloads yfinance prices and writes raw_series but is not declared")
    by_source: dict[str, set[bool]] = {}
    for rel, (name, flag) in declared.items():
        source = sources.get(rel)
        if source is None:
            problems.append(f"{rel}: declared price writer does not exist")
            continue
        if f'"{name}"' not in source and f"'{name}'" not in source:
            problems.append(f"{rel}: never names the source {name!r} it is declared to write under")
        for line, got in yfinance_downloads(source):
            if got != flag:
                problems.append(f"{rel}:{line}: auto_adjust={got} but the writer declares {flag}")
        by_source.setdefault(name.lower(), set()).add(flag)
    for name, flags in sorted(by_source.items()):
        if len(flags) > 1:
            problems.append(f"source {name!r} has writers with different auto_adjust flags {sorted(flags)}")
    return problems


def test_one_price_basis_per_source():
    problems = price_writer_problems(PRICE_WRITERS, _module_sources())
    assert not problems, "\n".join(problems)


def test_one_meaning_self_test_catches_the_april_2026_contamination():
    """The pre-fix fill_missing_features: adjusted closes under yfinance's own source."""
    contaminated = '''
import yfinance as yf
def pull(engine):
    source_id = _ensure_source(engine, "yfinance", {})
    data = yf.download(["SPY"], period="5y", auto_adjust=True)
    conn.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                      "VALUES (:s, :src, :d, :v, 'SUCCESS')"))
'''
    sources = {**_module_sources(), "scripts/fill_missing_features.py": contaminated}
    declared = {**PRICE_WRITERS, "scripts/fill_missing_features.py": ("yfinance", True)}
    problems = price_writer_problems(declared, sources)
    assert any("different auto_adjust flags" in p and "'yfinance'" in p for p in problems), problems
    undeclared = price_writer_problems(PRICE_WRITERS, {**sources, "scripts/new_writer.py": contaminated})
    assert any("scripts/new_writer.py" in p and "not declared" in p for p in undeclared)
    implicit = contaminated.replace(", auto_adjust=True", "")
    assert any("auto_adjust=implicit" in p for p in
               price_writer_problems({"m.py": ("yfinance", False)}, {"m.py": implicit}))
