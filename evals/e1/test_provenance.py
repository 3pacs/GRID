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
A writer that must read raw *and* adjusted closes to store a source's
adjusted basis as of a past provider state (a repair recomputing a stored
vintage on its own basis) is declared in :data:`DERIVED_BASIS_WRITERS`
instead: the basis it stores must equal the source's declared writers'
flag, its own download flag must be explicit at every call, and it names
parity tests (under ``tests/``) that compare its stored values with the
declared writer's on the same bars and cover how the past basis is chosen.
Its parity tests live in one file under ``tests/``. That file and every
``tests.*`` module it imports, transitively, are pinned whole by one sha256
(an import that cannot be resolved fails), so any change to a test, fixture,
helper or support module fails the gate until a new E1 version is released.
The gate runs the named tests itself, isolated from everything not pinned (no
conftest, no pytest configuration file, no auto-loaded plugins), and fails
unless each is collected and passes (none skipped): the parity claim is
executed on every E1 run, not just named. The pin is a tripwire against the
evidence being weakened or bypassed, not a proof that the evidence is
sufficient: the code under test is deliberately not pinned (so, for one, the
missing-session test and the repair share ingestion.market_calendar and
cannot see a change to it), and whether these tests are enough is the
reviewer's and the owner's judgement at sign-off. It is not an exemption: an undeclared writer, an implicit
flag, a stored basis that differs, a changed parity file or support module,
or a missing, skipped or failing parity test fails the gate.

The fixture test (``test_gates_pg.py``) runs the shared writer
(``BasePuller._insert_raw``) against the real DDL on PostgreSQL.

``resolved_series`` vintages (E1-V7, :mod:`evals.e1.vintage_scan`): outside
``normalization/resolver.py`` no writer may stamp ``release_date`` /
``vintage_date`` with the row's ``obs_date`` (a backdated vintage), rewrite a
stored vintage with ``ON CONFLICT ... DO UPDATE``, omit the NOT NULL date
columns, or bind them in a way the scan cannot verify.
"""

from __future__ import annotations

import ast
import functools
import os
import re
from dataclasses import dataclass
from typing import Any
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

# module -> (source name, auto_adjust basis it stores, auto_adjust at every
# yfinance download, {"file": tests/<file>.py, "sha256": parity_file_sha256 of
# it, "tests": names in it}: the parity evidence this gate pins and runs)
DERIVED_BASIS_WRITERS: dict[str, tuple[str, bool, bool, dict[str, Any]]] = {
    # Recomputes ETF flow rows stored from a partial bar on the price basis of
    # the run that stored them: needs the raw close (Close, auto_adjust=False)
    # and today's Adj Close to state that run's adjusted close
    # (AdjNow(k) * Raw(S) / AdjNow(S)), i.e. the auto_adjust=True basis the
    # live writer stores.
    "scripts/repair_etf_flow_partial_bars.py": (
        "INSTITUTIONAL_FLOWS", True, False, {
            "file": "tests/test_etf_flow_repair_parity.py",
            "sha256": "f85c719522a504daf5e7eddd0d4e7d9a009b2d753ba377105467f40adb17ed29",
            "tests": (
                # stored values equal the live writer's run the morning after, on
                # rows whose original-versus-current adjustment factor is not 1
                "test_a_repaired_value_is_what_the_fixed_writer_stores_from_the_settled_bars",
                # same-day ex-dividend basis: an after-close row keeps the day before's
                "test_an_after_close_row_on_its_ex_dividend_date_keeps_the_day_befores_basis",
                # a missing open session (or a bar on a closed day) refuses; a holiday does not
                "test_a_missing_exchange_session_refuses_the_etf_but_a_holiday_does_not",
            ),
        },
    ),
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


def parity_file_sha256(rel: str, tests: dict[str, str] | None = None) -> str | None:
    """sha256 over the whole of ``rel`` and every ``tests.*`` module it imports, transitively.

    None when ``rel`` or any of those modules cannot be read, or when any of
    them makes a relative import (it could reach something unpinned).
    """
    import hashlib

    def read(path: str) -> str | None:
        if tests is not None:
            text = tests.get(path)
        else:
            try:
                text = (REPO / path).read_text(encoding="utf-8")
            except OSError:
                return None
        return None if text is None else text.replace("\r\n", "\n")

    if not rel.startswith("tests/") or not rel.endswith(".py"):
        return None
    parts: dict[str, str] = {}
    todo = [rel]
    while todo:
        path = todo.pop()
        if path in parts:
            continue
        source = read(path)
        if source is None:
            return None
        parts[path] = source
        # Every package __init__ on the way is imported too ("from tests import x").
        package = path.rsplit("/", 1)[0]
        while package:
            init = f"{package}/__init__.py"
            if init not in parts and read(init) is not None:
                todo.append(init)
            package = package.rsplit("/", 1)[0] if "/" in package else ""
        for node in ast.walk(ast.parse(source)):
            names: list[str] = []
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    return None
                names = [node.module or ""] + [f"{node.module}.{a.name}" for a in node.names]
            elif isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            for name in names:
                if name == "tests" or not name.startswith("tests."):
                    continue
                module = name.replace(".", "/") + ".py"
                if read(module) is not None:
                    todo.append(module)
                elif read(name.rsplit(".", 1)[0].replace(".", "/") + ".py") is None:
                    return None                           # neither a module nor a name in one
    blob = "\n".join(f"== {k}\n{v}" for k, v in sorted(parts.items()))
    return hashlib.sha256(blob.encode()).hexdigest()


def price_writer_problems(declared: dict[str, tuple[str, bool]], sources: dict[str, str],
                          derived: dict[str, tuple[str, bool, bool, dict[str, Any]]] | None = None,
                          tests: dict[str, str] | None = None) -> list[str]:
    derived = derived or {}
    problems = []
    for rel, source in sorted(sources.items()):
        if ("yfinance" in source and writes_raw_series(source) and yfinance_downloads(source)
                and rel not in declared and rel not in derived):
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
    for rel, (name, stored, download, parity) in sorted(derived.items()):
        source = sources.get(rel)
        if source is None:
            problems.append(f"{rel}: declared derived-basis writer does not exist")
            continue
        if rel in declared:
            problems.append(f"{rel}: declared both as a price writer and a derived-basis writer")
        if f'"{name}"' not in source and f"'{name}'" not in source:
            problems.append(f"{rel}: never names the source {name!r} it is declared to write under")
        if name.lower() not in by_source:
            problems.append(f"{rel}: derived-basis writer for {name!r}, which has no declared price writer")
        for line, got in yfinance_downloads(source):
            if got != download:
                problems.append(f"{rel}:{line}: auto_adjust={got} but the derived-basis writer declares {download}")
        if (not isinstance(parity, dict) or not parity.get("tests") or not parity.get("file")
                or not isinstance(parity.get("tests"), tuple)):
            problems.append(f"{rel}: derived-basis writer names no parity tests ({{file, sha256, tests}})")
        else:
            got = parity_file_sha256(parity["file"], tests)
            source = None if got is None else (tests.get(parity["file"]) if tests is not None
                                               else (REPO / parity["file"]).read_text(encoding="utf-8"))
            if got is None:
                problems.append(f"{rel}: parity file {parity['file']!r} or a tests.* module it imports does not exist")
            elif got != parity.get("sha256"):
                problems.append(f"{rel}: parity file {parity['file']!r} or a tests.* module it imports changed "
                                "since it was pinned (re-release E1 with its new sha256)")
            for test in parity["tests"]:
                if source is not None and not re.search(rf"^def {re.escape(test)}\(", source, re.M):
                    problems.append(f"{rel}: parity test {parity['file']}::{test} does not exist")
        by_source.setdefault(name.lower(), set()).add(stored)
    for name, flags in sorted(by_source.items()):
        if len(flags) > 1:
            problems.append(f"source {name!r} has writers with different auto_adjust flags {sorted(flags)}")
    return problems


def test_one_price_basis_per_source():
    problems = price_writer_problems(PRICE_WRITERS, _module_sources(), DERIVED_BASIS_WRITERS)
    assert not problems, "\n".join(problems)


def parity_run_problems(refs: list[str], cwd: Path) -> list[str]:
    """Run ``refs`` with pytest in ``cwd``; the problems unless every one is collected and passes."""
    import subprocess
    import sys

    # Isolated from what is not pinned: no conftest, no pytest config file, no
    # auto-loaded plugins; the repository on the path for its own imports.
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_")}
    env.update({"TZ": os.environ.get("TZ", "UTC"), "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
                "PYTHONPATH": str(cwd)})
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", *refs, "-q", "-rA", "--noconftest", "-c", os.devnull,
         "--rootdir", str(cwd), "-p", "no:cacheprovider"],
        cwd=cwd, env=env, capture_output=True, text=True, timeout=600,
    )
    out = proc.stdout + proc.stderr
    problems = []
    if proc.returncode != 0:
        problems.append(f"parity tests exited {proc.returncode}")
    passed = set(re.findall(r"^PASSED (\S+)", out, re.M))
    for ref in refs:
        if ref not in passed:
            problems.append(f"parity test {ref} did not pass")
    if re.search(r"^(SKIPPED|XFAIL|XPASS|ERROR|FAILED)\b", out, re.M):
        problems.append("a parity test was skipped, xfailed or failed")
    return problems


@pytest.mark.parametrize("rel", sorted(DERIVED_BASIS_WRITERS))
def test_derived_basis_writers_parity_tests_run_and_pass(rel):
    """Run each declared parity test here, so the claim is executed, not only named."""
    parity = DERIVED_BASIS_WRITERS[rel][3]
    problems = parity_run_problems([f"{parity['file']}::{t}" for t in parity["tests"]], REPO)
    assert not problems, "\n".join(problems)


def test_parity_runner_self_test_fails_a_skipped_or_failing_test(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_p.py").write_text(
        "import pytest\n"
        "def test_ok():\n    pass\n"
        "def test_bad():\n    assert False\n"
        "def test_skip():\n    pytest.skip('no database')\n")
    assert parity_run_problems(["tests/test_p.py::test_ok"], tmp_path) == []
    # A plugin named in the environment is not loaded into the isolated run.
    (tmp_path / "evil_plugin.py").write_text(
        "def pytest_pyfunc_call(pyfuncitem):\n    return True\n")
    for key, value in (("PYTEST_PLUGINS", "evil_plugin"), ("PYTEST_ADDOPTS", "-p evil_plugin")):
        os.environ[key] = value
        try:
            assert any("did not pass" in p for p in parity_run_problems(["tests/test_p.py::test_bad"], tmp_path)), key
        finally:
            del os.environ[key]
    assert any("did not pass" in p for p in parity_run_problems(["tests/test_p.py::test_bad"], tmp_path))
    assert any("skipped" in p for p in parity_run_problems(["tests/test_p.py::test_skip"], tmp_path))
    assert any("did not pass" in p for p in parity_run_problems(["tests/test_p.py::test_absent"], tmp_path))


def test_derived_basis_self_test_catches_each_rule():
    writer = """
import yfinance as yf
def repair(conn):
    name = "SRC"
    data = yf.download("SPY", auto_adjust=False, actions=True)
    conn.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                      "VALUES (:s, :src, :d, :v, 'SUCCESS')"))
"""
    live = writer.replace("auto_adjust=False, actions=True", "auto_adjust=True")
    sources = {"live.py": live, "repair.py": writer}
    declared = {"live.py": ("SRC", True)}
    tests = {"tests/test_p.py": "from tests import helpers as h\ndef test_parity():\n    assert h.ok()\n",
             "tests/helpers.py": "from tests.deeper import X\ndef ok():\n    return X\n",
             "tests/deeper.py": "X = True\n"}
    pin = parity_file_sha256("tests/test_p.py", tests)
    parity = {"file": "tests/test_p.py", "sha256": pin, "tests": ("test_parity",)}
    ok = {"repair.py": ("SRC", True, False, parity)}
    assert pin and price_writer_problems(declared, sources, ok, tests) == []

    def problems(derived=ok, srcs=sources, tst=tests):
        return "\n".join(price_writer_problems(declared, srcs, derived, tst))

    assert "different auto_adjust flags" in problems({"repair.py": ("SRC", False, False, parity)})
    assert "derived-basis writer declares True" in problems({"repair.py": ("SRC", True, True, parity)})
    assert "auto_adjust=implicit" in problems(
        srcs={**sources, "repair.py": writer.replace(", auto_adjust=False, actions=True", "")})
    assert "no declared price writer" in problems({"repair.py": ("OTHER", True, False, parity)})
    assert "names no parity tests" in problems({"repair.py": ("SRC", True, False, {**parity, "tests": ()})})
    assert "names no parity tests" in problems({"repair.py": ("SRC", True, False, "tests/test_p.py::test_parity")})
    assert "does not exist" in problems({"repair.py": ("SRC", True, False, {**parity, "tests": ("test_absent",)})})
    assert "does not exist" in problems({"repair.py": ("SRC", True, False, {**parity, "file": "evals/x.py"})})
    for path, old, new in (("tests/test_p.py", "assert h.ok()", "assert True"),     # the test
                           ("tests/helpers.py", "return X", "return True"),         # a support module
                           ("tests/deeper.py", "X = True", "X = 1"),                # imported by that one
                           ("tests/test_p.py", "def test_parity", "import pytest\n@pytest.mark.skip\ndef test_parity")):
        edited = {**tests, path: tests[path].replace(old, new)}
        assert "changed since it was pinned" in problems(tst=edited), (path, old)
    for bad in ({**tests, "tests/helpers.py": "from tests.missing import Y\n"},       # an unresolvable import
                {**tests, "tests/test_p.py": "from .helpers import ok\n" + tests["tests/test_p.py"]}):  # relative
        assert "does not exist" in problems(tst=bad)
    with_init = {**tests, "tests/__init__.py": '"""tests"""\n'}
    init_pin = {"repair.py": ("SRC", True, False, {**parity, "sha256": parity_file_sha256("tests/test_p.py", with_init)})}
    assert problems(init_pin, tst=with_init) == ""
    patched = {**with_init, "tests/__init__.py": "import scripts.x as x\nx.f = lambda: 1\n"}
    assert "changed since it was pinned" in problems(init_pin, tst=patched)          # the package __init__ counts
    assert "repair.py: downloads yfinance prices and writes raw_series but is not declared" in problems({})



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


# ── resolved_series: no direct writer backdates a vintage (E1-V7) ──────

from evals.e1 import vintage_scan  # noqa: E402

_RESOLVED_WRITERS = vintage_scan.all_writers()

# Direct resolved_series writer files on main that violate the rules in
# evals/e1/vintage_scan.py (see known_violations.py). One ID per writer, so
# fixing (or retiring) one turns exactly its xfail into an XPASS.
_KNOWN_BACKDATING_WRITERS = {
    "analysis/research_agent.py": "E1-V7a",
    "scripts/backfill_celestial_ephemeris.py": "E1-V7b",
    "scripts/bridge_crucix.py": "E1-V7d",
    "scripts/load_alt_data.py": "E1-V7e",
    "scripts/load_more_data.py": "E1-V7f",
    "scripts/load_ticker_deep.py": "E1-V7g",
    "scripts/load_wave2.py": "E1-V7h",
    "scripts/load_wave3.py": "E1-V7i",
    "scripts/load_yfinance.py": "E1-V7j",
    "scripts/migrate_and_load.py": "E1-V7k",
    "scripts/parse_edgar.py": "E1-V7l",
    "scripts/parse_eia.py": "E1-V7m",
    "scripts/parse_gdelt.py": "E1-V7n",
    "scripts/pull_intraday.py": "E1-V7o",
}


def _resolved_writer_params():
    for rel in sorted(_RESOLVED_WRITERS):
        marks = [known_violation(_KNOWN_BACKDATING_WRITERS[rel])] if rel in _KNOWN_BACKDATING_WRITERS else []
        yield pytest.param(rel, id=rel, marks=marks)


def test_resolved_scan_finds_the_known_writer_population():
    # A scanner that silently stopped matching would make every writer "pass".
    assert len(_RESOLVED_WRITERS) >= 15, sorted(_RESOLVED_WRITERS)
    for must in ("scripts/load_yfinance.py", "scripts/compute_derived_features.py", "ingestion/options.py"):
        assert must in _RESOLVED_WRITERS
    assert vintage_scan.RESOLVER not in _RESOLVED_WRITERS


@pytest.mark.parametrize("rel", list(_resolved_writer_params()))
def test_resolved_series_writer_never_backdates_or_rewrites_a_vintage(rel):
    problems = _RESOLVED_WRITERS[rel]
    assert not problems, "\n".join(str(p) for p in problems)


def test_known_backdating_writers_are_still_writers():
    assert set(_KNOWN_BACKDATING_WRITERS) <= set(_RESOLVED_WRITERS)


def test_no_resolved_series_writer_bypasses_the_scan():
    """Writers the SQL scan cannot see (ORM/pandas/COPY) are refused outright."""
    assert vintage_scan.hidden_writers() == []


def test_resolved_schema_requires_release_and_vintage_dates():
    text = (REPO / "schema.sql").read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE IF NOT EXISTS resolved_series \((.*?)\n\);", text, re.S).group(1)
    for col in ("obs_date", "release_date", "vintage_date"):
        line = next(ln for ln in body.splitlines() if ln.strip().startswith(col + " "))
        assert "NOT NULL" in line.upper() and "DEFAULT" not in line.upper(), line


def _problems(files: dict[str, str]) -> dict[str, list[str]]:
    return {rel: [p.kind for p in ps] for rel, ps in vintage_scan.all_writers(files).items()}


_BAD_POSITIONAL = '''
def ins(cur, fid, d, val, sid):
    cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (fid, d, d, d, val, sid))
'''
_BAD_SAME_NAME = '''
def fill(conn, fid, hist):
    for dt, row in hist.iterrows():
        conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                          "source_priority_used) VALUES (:fid, :od, :od, :od, :val, 1) ON CONFLICT DO NOTHING"),
                     {"fid": fid, "od": dt.date(), "val": row["Close"]})
'''
_BAD_BATCH = '''
def run(conn, days):
    batch = []
    for d in days:
        batch.append({"fid": 1, "od": d, "rd": d, "vd": d, "val": 1.0, "src": 2})
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                      "source_priority_used) VALUES (:fid, :od, :rd, :vd, :val, :src)"), batch)
'''
_BAD_SELECT_SQL = """INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, source_priority_used)
SELECT fr.id, t.obs_date, t.obs_date AS release_date, t.as_of::date, t.close, 7 FROM t JOIN fr ON true
ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING;"""
_BAD_REWRITE = '''
def save(conn, r):
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, value, source_priority_used) "
                      "VALUES (:fid, :d, :d, :v, :src) ON CONFLICT (feature_id, obs_date, vintage_date) "
                      "DO UPDATE SET value = :v"),
                 {"fid": 1, "d": r["date"], "v": r["price"], "src": 3})
'''


def test_vintage_scanner_self_test_catches_backdating_writers():
    got = _problems({"a.py": _BAD_POSITIONAL, "b.py": _BAD_SAME_NAME, "c.py": _BAD_BATCH,
                     "d.sql": _BAD_SELECT_SQL, "e.py": _BAD_REWRITE})
    assert got["a.py"] == ["backdate", "backdate"]
    assert got["b.py"] == ["backdate", "backdate"]
    assert got["c.py"] == ["backdate", "backdate"]
    assert got["d.sql"] == ["backdate"]
    assert sorted(got["e.py"]) == ["backdate", "missing", "rewrite"]


_GOOD_RUN_DATE = '''
def insert(conn, fid, series, run_date):
    rows = [{"feature_id": fid, "obs_date": d, "release_date": run_date, "vintage_date": run_date, "value": v}
            for d, v in series.items()]
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                      "VALUES (:feature_id, :obs_date, :release_date, :vintage_date, :value) "
                      "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING"), rows)
'''
_GOOD_SAME_DAY = '''
def push(conn, fid, val):
    snap = date.today()
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                      "source_priority_used) VALUES (:fid, :d, :d, :d, :v, 1) ON CONFLICT DO NOTHING"),
                 {"fid": fid, "d": snap, "v": val})
'''
_GOOD_PULL_DATE = """
def bulk(conn):
    conn.execute(text('''INSERT INTO resolved_series (feature_id, obs_date, value, release_date, vintage_date,
                         source_priority_used) SELECT :fid, w.obs_date, w.value, w.pull_timestamp::date,
                         w.pull_timestamp::date, 1 FROM winners w ON CONFLICT DO NOTHING'''), {"fid": 1})
"""


def test_vintage_scanner_self_test_passes_honest_writers_and_exempts_the_resolver():
    got = _problems({
        "a.py": _GOOD_RUN_DATE,       # stamped at computation time (compute_derived_features)
        "b.py": _GOOD_SAME_DAY,       # same-day snapshot (options live push)
        "c.py": _GOOD_PULL_DATE,      # vintage from the pull timestamp (bulk_resolve)
        vintage_scan.RESOLVER: _BAD_SAME_NAME,  # the resolver is exempt whatever it holds
        "d.py": 'DOC = "pulls prices and insert into resolved_series."',  # prose, not a writer
    })
    assert got == {"a.py": [], "b.py": [], "c.py": []}, got


def test_vintage_scanner_self_test_reports_dynamic_sql_as_unverifiable():
    got = _problems({
        "a.py": '''
def w(conn, cols, row):
    conn.execute(text(f"INSERT INTO resolved_series ({cols}) VALUES (:fid, :od, :rd, :vd, :v)"), row)
''',
        "b.py": '''
def w(cur, rows):
    for row in rows:
        cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value) "
                    "VALUES (%s,%s,%s,%s,%s)", row)
''',
        "c.py": '''
def w(conn, params):
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                      "VALUES (:fid, :od, :rd, :vd, :v)"), params)
''',
        "d.py": 'Q = "INSERT INTO resolved_series VALUES (1, 2, 3)"',
    })
    assert set(got) == {"a.py", "b.py", "c.py", "d.py"}, got
    for rel, kinds in got.items():
        assert kinds and set(kinds) == {vintage_scan.UNVERIFIABLE}, (rel, kinds)
    hidden = vintage_scan.hidden_writers({"x.py": "df.to_sql('resolved_series', engine)",
                                          "y.sql": "COPY resolved_series FROM STDIN;",
                                          "z.sql": "COPY resolved_series_retractions TO STDOUT;"})
    assert hidden == ["x.py", "y.sql"]


# Review round 1 (DFa): casts, the timedelta backfill loop, loop targets named
# "today", disagreeing module dicts, now(), shell heredocs.
_BAD_CAST_POSITIONAL = '''
def ins(cur, fid, d, v):
    cur.execute("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                "VALUES (%s, %s, %s::date, %s::date, %s)", (fid, d, d, d, v))
'''
_BAD_CAST_NAMED = '''
def ins(conn, fid, d, v):
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                      "VALUES (:fid, :d, CAST(:d AS date), (:d)::date, :v)"), {"fid": fid, "d": d, "v": v})
'''
_BAD_CAST_SQL = """INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, source_priority_used)
SELECT fr.id, t.obs_date, t.obs_date::date, date(t.obs_date), t.close, 7 FROM t JOIN fr ON true;"""
_BAD_TIMEDELTA_LOOP = '''
def backfill(cur, fid, prices):
    for i in range(1300):
        d = date.today() - timedelta(days=i)
        cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value) "
                    "VALUES (%s,%s,%s,%s,%s)", (fid, d, d, d, prices[i]))
'''
_BAD_TODAY_LOOP_NAME = '''
def backfill(cur, fid, hist):
    for today_row, v in hist.items():
        cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value) "
                    "VALUES (%s,%s,%s,%s,%s)", (fid, today_row, today_row, today_row, v))
'''
_BAD_SHELL = """#!/bin/sh
psql -d griddb <<'SQL'
INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, source_priority_used)
SELECT 1, obs_date, obs_date, obs_date, value, 2 FROM staging;
SQL
"""


def test_vintage_scanner_self_test_sees_through_casts_loops_and_names():
    got = _problems({"a.py": _BAD_CAST_POSITIONAL, "b.py": _BAD_CAST_NAMED, "c.sql": _BAD_CAST_SQL,
                     "d.py": _BAD_TIMEDELTA_LOOP, "e.py": _BAD_TODAY_LOOP_NAME, "f.sh": _BAD_SHELL})
    for rel in ("a.py", "b.py", "c.sql", "d.py", "e.py", "f.sh"):
        assert got[rel] == ["backdate", "backdate"], (rel, got[rel])


def test_vintage_scanner_self_test_unverifiable_when_module_dicts_disagree():
    disagree = '''
def run(conn, days, run_date):
    batch = [{"fid": 1, "od": d, "vd": d} for d in days]
    other = [{"fid": 1, "od": d, "vd": run_date} for d in days]
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                      "VALUES (:fid, :od, CURRENT_DATE, :vd, 1.0)"), batch + other)
'''
    assert _problems({"a.py": disagree}) == {"a.py": [vintage_scan.UNVERIFIABLE]}


def test_vintage_scanner_self_test_same_day_sql_and_reviewed_exemption():
    same_day_sql = '''
def push(conn, fid, v):
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                      "VALUES (:fid, now()::date, now()::date, CURRENT_DATE, :v)"), {"fid": fid, "v": v})
'''
    param_today = '''
def _push_to_resolved(conn, today_str, fid, val):
    conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                      "VALUES (:fid, :od, :rd, :vd, :val)"),
                 {"fid": fid, "od": today_str, "rd": today_str, "vd": today_str, "val": val})
'''
    got = _problems({"a.py": same_day_sql, "ingestion/options.py": param_today, "x.py": param_today})
    assert got["a.py"] == []
    # a parameter is not provably today: only the reviewed (file, function) entry passes
    assert got["ingestion/options.py"] == []
    assert got["x.py"] == ["backdate", "backdate"]
    assert ("ingestion/options.py", "_push_to_resolved") in vintage_scan.REVIEWED_SAME_DAY


# Review round 2 (DFa): unbalanced wrappers, look-alike "now" helpers, aliases,
# coalesce fallbacks, and the reviewed same-day exemption's callers.
def test_vintage_scanner_self_test_round_two_residuals():
    lookalike_now = '''
def backfill(cur, fid, prices):
    for i in range(100):
        d = my_now(i)
        cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value) "
                    "VALUES (%s,%s,%s,%s,%s)", (fid, d, d, d, prices[i]))
'''
    alias = '''
def backfill(cur, fid, hist):
    for d, v in hist.items():
        rel = d
        cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value) "
                    "VALUES (%s,%s,%s,%s,%s)", (fid, d, rel, rel, v))
'''
    coalesce_sql = ("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                    "SELECT 1, t.d, t.pulled::date, coalesce(t.as_of, t.d), t.v FROM t;")
    unbalanced = ("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                  "SELECT 1, date(t.a) + date(t.b), t.pulled::date, t.pulled::date, t.v FROM t;")
    got = _problems({"a.py": lookalike_now, "b.py": alias, "c.sql": coalesce_sql, "d.sql": unbalanced})
    assert got["a.py"] == ["backdate", "backdate"]
    assert got["b.py"] == ["backdate", "backdate"]
    assert got["c.sql"] == [vintage_scan.UNVERIFIABLE]
    assert got["d.sql"] == []  # wrapper not stripped: compared as written, and it differs


def test_reviewed_same_day_exemption_callers_are_pinned():
    """options.py::_push_to_resolved is exempt only while its date provably comes from
    pull_all's own clock: called only by _pull_ticker, which is called only by pull_all,
    where today_str is derived from _utc_now()."""
    import ast as _ast

    def callers(attr: str) -> set[tuple[str, str]]:
        found = set()
        for rel, text in vintage_scan.sources().items():
            if not rel.endswith(".py") or attr not in text:
                continue
            module = vintage_scan._Module(rel, text)
            for node in _ast.walk(module.tree):
                if isinstance(node, _ast.Call) and isinstance(node.func, _ast.Attribute) and node.func.attr == attr:
                    found.add((rel, getattr(module.function_of(node), "name", "<module>")))
        return found

    assert callers("_push_to_resolved") == {("ingestion/options.py", "_pull_ticker")}
    # other pullers have their own _pull_ticker; only options.py's matters here
    assert {c for c in callers("_pull_ticker") if c[0] == "ingestion/options.py"} == {
        ("ingestion/options.py", "pull_all")}
    module = vintage_scan._Module("ingestion/options.py", vintage_scan.sources()["ingestion/options.py"])
    call = next(n for n in _ast.walk(module.tree) if isinstance(n, _ast.Call)
                and isinstance(n.func, _ast.Attribute) and n.func.attr == "_pull_ticker")
    date_arg = call.args[1]
    assert isinstance(date_arg, _ast.Name) and date_arg.id == "today_str"
    assert module.is_today(date_arg)
