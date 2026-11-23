"""Guard (#1312): the provisional market-state record stays display only.

Owner's decision (bring-up plan rev 100): #1191's previous-session rule stays for anything that decides. A record
computed at the session's close is display only, labelled provisional, and no decision path reads it.

What this file holds fixed:

* Only three non-test modules may name the provisional table or its store functions: the writer
  (``scripts/run_market_state.py``), ``intelligence/regime/market_state.py`` and the desk brief's adapter
  (``analysis/desk_brief_sources.py``). Docs (``.md``) may describe it. Inside those three, every mention sits in a
  reviewed definition (``ALLOWED_DEFINITIONS``); a new function that names it fails here and needs review.
* Every module that reads ``market_state_daily`` (on 2026-11-22: the writer's metadata check and the desk brief, both
  non-deciding; no module decides from it yet) never names the provisional table, and no SQL that reads
  ``market_state_daily`` mentions it. So any reader of the decision table, a future decider included, gets the
  previous-session record and nothing else.
* The decision writer still refuses today's date and still targets the prior session, also when the provisional
  writer has already run for today after the close.
* A provisional record cannot land in ``market_state_daily``.
* The provisional path never refreshes the FRED vintages, no flag can turn a refresh on, and its compute always
  passes the availability cutoff (Codex HOLD on #1360).
* Only the reviewed migration creates the table outside SQLite tests: production never runs CREATE TABLE for it.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import os
from datetime import date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from intelligence.regime import market_state as M

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("state_writer_guard", ROOT / "scripts/run_market_state.py")
W = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(W)

TABLE = "market_state_provisional"
WRITE_API = ("store_provisional_market_state", "ensure_provisional_table", "provisional_record", "PROVISIONAL_TABLE")
WRITER = "scripts/run_market_state.py"
RECORD = "intelligence/regime/market_state.py"
DISPLAY = "analysis/desk_brief_sources.py"
MIGRATION = "migrations/versions/market_state_prov_20261122.py"
# token -> the only non-test .py files that may contain it
ALLOWED_FILES = {TABLE: {WRITER, RECORD, DISPLAY, MIGRATION}, **{name: {WRITER, RECORD} for name in WRITE_API}}
# file -> the top-level definitions whose string literals may name the provisional table
ALLOWED_DEFINITIONS = {
    RECORD: {"store_market_state",            # its refusal message names the right table
             "PROVISIONAL_TABLE", "PROVISIONAL_MIGRATION", "_PROVISIONAL_TABLE_SQL", "_PROVISIONAL_INSERT_SQL",
             "ensure_provisional_table"},     # SQLite only; its docstring names the migration
    WRITER: {"<module docstring>", "existing_provisional_row", "run_provisional"},
    DISPLAY: {"<module docstring>", "_MSP_SQL", "_provisional", "market_state"},
    MIGRATION: {"<module docstring>", "TABLE", "GRANTS", "downgrade"},
}
SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", ".claude", ".pytest_cache", "pwa_dist",
             "tests"}


def _source_files() -> dict[str, str]:
    out = {}
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".venv")]
        for name in filenames:
            if not name.endswith(".py") or name == "conftest.py":
                continue
            path = Path(dirpath) / name
            try:
                out[path.relative_to(ROOT).as_posix()] = path.read_text(errors="replace")
            except OSError:
                continue
    return out


SOURCES = _source_files()


def _definitions_naming(source: str, needle: str) -> set[str]:
    """Top-level definitions (function, class or assignment target) holding a string literal that contains needle."""
    tree = ast.parse(source)
    found: set[str] = set()
    for i, node in enumerate(tree.body):
        if i == 0 and isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) \
                and isinstance(node.value.value, str):
            if needle in node.value.value:
                found.add("<module docstring>")
            continue
        hits = [n for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)
                and needle in n.value]
        if not hits:
            continue
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            found.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            found |= {t.id for t in targets if isinstance(t, ast.Name)}
        else:
            found.add(f"<line {node.lineno}>")
    return found


def test_scan_sees_the_three_modules():
    assert {WRITER, RECORD, DISPLAY} <= set(SOURCES)
    assert len(SOURCES) > 500                                   # the whole tree, not a subdirectory


def test_only_the_writer_the_record_and_the_brief_name_the_provisional_record():
    offenders = {token: sorted(f for f, src in SOURCES.items() if token in src and f not in allowed)
                 for token, allowed in ALLOWED_FILES.items()}
    assert offenders == {token: [] for token in ALLOWED_FILES}, (
        "only the writer, market_state.py and desk_brief_sources.py may name the provisional market-state record "
        "(#1312: display only); a decision path must read market_state_daily")
    for token in ALLOWED_FILES:
        assert token in SOURCES[RECORD]                         # the guard is not vacuous
    assert TABLE in SOURCES[WRITER] and TABLE in SOURCES[DISPLAY]


@pytest.mark.parametrize("path", sorted(ALLOWED_DEFINITIONS))
def test_provisional_table_is_named_only_in_reviewed_definitions(path):
    assert _definitions_naming(SOURCES[path], TABLE) <= ALLOWED_DEFINITIONS[path]


def test_the_brief_reads_the_provisional_record_with_one_select_by_session():
    tree = ast.parse(SOURCES[DISPLAY])
    sql = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)
           and f"FROM {TABLE}" in n.value]
    assert sql == [f"SELECT as_of_date, computed_at, rule, record FROM {TABLE} WHERE as_of_date = :d"]


def test_every_reader_of_market_state_daily_never_reads_the_provisional_table():
    readers = {f for f, src in SOURCES.items() if "market_state_daily" in src}
    assert {WRITER, DISPLAY, RECORD} <= readers
    for f in sorted(readers):
        src = SOURCES[f]
        if f not in ALLOWED_DEFINITIONS:
            assert TABLE not in src and not any(name in src for name in WRITE_API), f
            continue
        tree = ast.parse(src)
        for node in tree.body:
            literals = [n.value for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
            if any("FROM market_state_daily" in s for s in literals):
                # A definition that reads the decision table never names the provisional one.
                assert not any(TABLE in s for s in literals), (f, getattr(node, "name", node.lineno))
        for s in (n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)):
            if "market_state_daily" in s and ("SELECT" in s or "JOIN" in s or "UNION" in s):
                assert TABLE not in s, (f, s)


def test_decision_writer_main_only_dispatches_to_the_provisional_mode():
    tree = ast.parse(SOURCES[WRITER])
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    named = set()
    for n in ast.walk(main):
        if isinstance(n, ast.Name) and "provisional" in n.id.lower():
            named.add(n.id)
        elif isinstance(n, ast.Attribute) and "provisional" in n.attr.lower():
            named.add(n.attr)
        elif isinstance(n, ast.Constant) and isinstance(n.value, str) and "provisional" in n.value.lower():
            named.add(n.value)
    assert named == {"provisional", "run_provisional"}          # `if a.provisional: return run_provisional(...)`
    for fn in ("existing_row", "readiness", "sessions"):
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == fn)
        assert "provisional" not in ast.unparse(node).lower(), fn


def _function(path: str, name: str) -> ast.FunctionDef:
    return next(n for n in ast.parse(SOURCES[path]).body if isinstance(n, ast.FunctionDef) and n.name == name)


def test_the_provisional_path_has_no_refresh_and_always_passes_the_cutoff():
    fn = _function(WRITER, "run_provisional")
    called = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)} | {
        n.attr for n in ast.walk(fn) if isinstance(n, ast.Attribute)}
    assert not called & {"refresh", "refresh_vintages", "backfill", "fetch", "no_refresh", "SERIES"}
    imports = {n.module for n in ast.walk(fn) if isinstance(n, ast.ImportFrom)}
    assert not any(m and "fred_vintages" in m for m in imports), imports
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "compute_market_state"]
    assert len(calls) == 1 and [k.arg for k in calls[0].keywords if k.arg == "known_by"] == ["known_by"]


def test_no_flag_turns_a_vintage_refresh_on():
    options = {o for a in W.build_parser()._actions for o in a.option_strings if "refresh" in o}
    assert options == {"--no-refresh"}, "a refresh-on flag must be refused with --provisional before it lands"


def test_only_the_reviewed_migration_creates_the_table_outside_sqlite():
    ddl = ("CREATE TABLE IF NOT EXISTS market_state_provisional", "CREATE TABLE market_state_provisional")
    creators = sorted(f for f, src in SOURCES.items() if any(d in src for d in ddl))
    assert creators == sorted({MIGRATION, RECORD})
    # In market_state.py the SQLite DDL is executed only by ensure_provisional_table, which refuses other databases.
    users = {node.name for node in ast.parse(SOURCES[RECORD]).body if isinstance(node, ast.FunctionDef)
             and any(isinstance(n, ast.Name) and n.id == "_PROVISIONAL_TABLE_SQL" for n in ast.walk(node))}
    assert users == {"ensure_provisional_table"}
    pg = create_engine("postgresql+psycopg2://")  # a PostgreSQL dialect, never connected; no credentials
    with pytest.raises(RuntimeError, match="market_state_prov_20261122"):
        M.ensure_provisional_table(pg)


# ── behaviour: the decision writer and the decision table ────────────────────

def _state(target: date, known_by: datetime | None = None) -> M.MarketState:
    st = M.MarketState(target)
    st.gamma_state = {"status": "ok", "obs_date": target.isoformat()}
    st.variables = {"vix": {"status": "ok", "obs_date": target.isoformat(), "age_days": 0}}
    st.source_lineage = ({"availability_cutoff": {"at": known_by.isoformat(), "reads": dict(M.CUTOFF_READS)}}
                         if known_by is not None else {})
    return st


@pytest.fixture()
def engine():
    return create_engine("sqlite://")


@pytest.fixture(autouse=True)
def bounded(monkeypatch):
    monkeypatch.setattr(M, "load_calendar", dict)
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: _state(t, kw.get("known_by")))


def _clock(monkeypatch, now: str) -> None:
    instant = datetime.fromisoformat(now.replace("Z", "+00:00"))
    monkeypatch.setattr(W, "utc_now", lambda: instant)
    monkeypatch.setattr(W, "utc_today", lambda: instant.date())


_READ = {"market_state_daily": text("SELECT as_of_date, record FROM market_state_daily ORDER BY as_of_date"),
         TABLE: text("SELECT as_of_date, record FROM market_state_provisional ORDER BY as_of_date")}


def _rows(engine, table: str) -> list[tuple[str, dict]]:
    if not inspect(engine).has_table(table):
        return []
    with engine.connect() as c:
        return [(str(r[0]), json.loads(r[1])) for r in c.execute(_READ[table])]


@pytest.mark.parametrize("now, prior", [
    ("2026-11-20T21:30:00Z", "2026-11-19"),      # after Friday's close: the decision record is still Thursday's
    ("2026-11-27T18:30:00Z", "2026-11-25"),      # after the early close; Thanksgiving is not a session
    ("2026-07-01T20:30:00Z", "2026-06-30"),      # summer close
])
def test_decision_writer_still_targets_the_prior_session_after_a_provisional_run(engine, monkeypatch, capsys,
                                                                                 now, prior):
    _clock(monkeypatch, now)
    today = now[:10]
    assert W.main(["--provisional", "--no-refresh", "--json"], engine) == 0
    assert W.main(["--no-refresh", "--json"], engine) == 0          # the nightly writer, same clock
    out = capsys.readouterr().out.strip().splitlines()
    assert json.loads(out[-1])["outcomes"][0]["target_session"] == prior
    daily, provisional = _rows(engine, "market_state_daily"), _rows(engine, TABLE)
    assert [d for d, _ in daily] == [prior] and [d for d, _ in provisional] == [today]
    rec = daily[0][1]
    assert "provisional" not in rec and "decision_use" not in rec and "label" not in rec
    assert rec["writer_provenance"]["mode"] == "daily"
    assert provisional[0][1]["provisional"] is True


@pytest.mark.parametrize("extra", [["--as-of", "2026-11-20"], ["--from", "2026-11-19", "--to", "2026-11-20"]])
def test_decision_writer_still_refuses_today_after_the_close(engine, monkeypatch, capsys, extra):
    _clock(monkeypatch, "2026-11-20T21:30:00Z")
    assert W.main(["--no-refresh", *extra], engine) == 2
    assert "on or after today" in capsys.readouterr().err
    assert inspect(engine).get_table_names() == []


def test_a_provisional_record_cannot_land_in_market_state_daily(engine, monkeypatch, capsys):
    st = _state(date(2026, 11, 20))
    st.writer_provenance = {"mode": M.PROVISIONAL_MODE}
    with pytest.raises(ValueError, match="decision records only"):
        M.store_market_state(engine, st)
    with pytest.raises(ValueError, match="decision records only"):
        M.store_market_state(engine, st, replace=True)
    assert not inspect(engine).has_table("market_state_daily")
    # The provisional writer never calls the decision store.
    monkeypatch.setattr(M, "store_market_state", lambda *a, **kw: pytest.fail("provisional run used the decision store"))
    _clock(monkeypatch, "2026-11-20T21:15:00Z")
    assert W.main(["--provisional", "--no-refresh", "--json"], engine) == 0
    assert W.main(["--provisional", "--no-refresh", "--json", "--replace"], engine) == 0
    capsys.readouterr()
    assert not inspect(engine).has_table("market_state_daily")
    assert [d for d, _ in _rows(engine, TABLE)] == ["2026-11-20"]
