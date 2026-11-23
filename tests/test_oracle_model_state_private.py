"""A private verdict never moves what a contributor sees in the model stats (2026-11-23).

Answers Codex review 6029863205 on GRID#1396: ``oracle_models`` holds running
counters, cumulative P&L, weights and timestamps that the scorer and weight
evolution update from every scored row, the Oracle's Schwab-built
(private-use) predictions included. ``/oracle/scorecard`` served them to every
authenticated viewer, and the scoreboard served weights. Now an admin request
gets that model state; everyone else gets public-cohort counts and no
model-state column. The contract: adding a tagged verdict (and the counter and
weight updates the scorer and evolver make for it) leaves the contributor view
byte-for-byte unchanged; an ordinary verdict, the control, changes it.
Offline: SQLite only.
"""
from __future__ import annotations

import ast
import json
import pathlib
import re
from datetime import date, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine, text

from store import options_provenance as op

ROOT = pathlib.Path(__file__).resolve().parents[1]
TAG = {op.ORACLE_PRIVATE_USE_KEY: op.ORACLE_PRIVATE_USE_SCHWAB}
PAST = (date.today() - timedelta(days=1)).isoformat()


@pytest.fixture
def db():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE oracle_predictions (id TEXT, ticker TEXT, model_name TEXT, verdict TEXT, "
            "pnl_pct REAL, expiry DATE, dedup_keep BOOLEAN, recording_policy TEXT, flow_context TEXT)"))
        conn.execute(text(
            "CREATE TABLE oracle_models (name TEXT, weight REAL, predictions_made INTEGER, hits INTEGER, "
            "misses INTEGER, partials INTEGER, cumulative_pnl REAL, sharpe REAL, last_updated TEXT, "
            "description TEXT)"))
        conn.execute(text("INSERT INTO oracle_models VALUES "
                          "('m1', 1.0, 0, 0, 0, 0, 0, 0.4, NULL, 'one'), "
                          "('m2', 1.2, 0, 0, 0, 0, 0, 0.1, NULL, 'two')"))
    _score(engine, "pub-1", "m1", "hit", 1.0, tagged=False)
    _score(engine, "pub-2", "m2", "miss", -0.5, tagged=False)
    yield engine
    engine.dispose()


def _score(engine: Any, pid: str, model: str, verdict: str, pnl: float, *, tagged: bool) -> None:
    """A scored prediction plus what the scorer and weight evolution write for it."""
    ctx = {"entry_price_basis": {}, **(TAG if tagged else {})}
    column = {"hit": "hits", "miss": "misses", "partial": "partials"}[verdict]
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO oracle_predictions VALUES (:i, 'SPY', :m, :v, :p, :e, 1, "
                          "'oracle_forward_v2', :c)"),
                     {"i": pid, "m": model, "v": verdict, "p": pnl, "e": PAST, "c": json.dumps(ctx)})
        conn.execute(text(f"UPDATE oracle_models SET {column} = {column} + 1, "  # noqa: S608 - literal column
                          "predictions_made = predictions_made + 1, cumulative_pnl = cumulative_pnl + :p, "
                          "weight = weight + 0.05, last_updated = :t WHERE name = :m"),
                     {"p": pnl, "m": model, "t": f"2026-11-23T21:{len(pid):02d}:00"})


def _views(engine: Any, monkeypatch, admin: bool) -> dict[str, Any]:
    from api.routers import oracle as route
    from oracle import scoreboard

    monkeypatch.setattr(route, "get_db_engine", lambda: engine)
    return {"scorecard": route.get_scorecard(_token="t", include_schwab=admin),
            "models": scoreboard._fetch_model_rows(engine, include_schwab=admin)}


def test_a_private_verdict_leaves_the_contributor_view_unchanged(db, monkeypatch) -> None:
    before = json.dumps(_views(db, monkeypatch, admin=False), sort_keys=True)
    admin_before = _views(db, monkeypatch, admin=True)
    _score(db, "schwab-1", "m1", "hit", 9.0, tagged=True)
    assert json.dumps(_views(db, monkeypatch, admin=False), sort_keys=True) == before
    admin_after = _views(db, monkeypatch, admin=True)
    assert admin_after != admin_before  # the owner's view does move
    m1 = {m["model"]: m for m in admin_after["scorecard"]["models"]}["m1"]
    assert m1["hits"] == 2 and m1["cumulative_pnl"] == 10.0


def test_an_ordinary_verdict_still_reaches_contributors(db, monkeypatch) -> None:
    before = _views(db, monkeypatch, admin=False)
    _score(db, "pub-3", "m1", "miss", -2.0, tagged=False)
    after = _views(db, monkeypatch, admin=False)
    assert after != before
    m1 = {m["model"]: m for m in after["scorecard"]["models"]}["m1"]
    assert (m1["hits"], m1["misses"], m1["cumulative_pnl"]) == (1, 1, -1.0)
    assert after["scorecard"]["totals"]["predictions"] == 3


def test_contributors_get_no_model_state(db, monkeypatch) -> None:
    views = _views(db, monkeypatch, admin=False)
    for row in views["scorecard"]["models"]:
        assert row["weight"] is None and row["sharpe"] is None and row["last_updated"] is None
    for row in views["models"]:
        assert row["weight"] is None and row["sharpe"] is None
    assert [r["name"] for r in views["models"]] == ["m1", "m2"]  # by name, not by weight
    assert views["scorecard"]["basis"].endswith("public cohort")
    admin = _views(db, monkeypatch, admin=True)
    assert all(r["weight"] is not None for r in admin["scorecard"]["models"])
    assert [r["name"] for r in admin["models"]] == ["m2", "m1"]  # weight order for the owner


@pytest.mark.parametrize("flag", [None, 1, "true", "admin"])
def test_only_an_exact_true_gets_model_state(db, monkeypatch, flag) -> None:
    from api.routers import oracle as route

    monkeypatch.setattr(route, "get_db_engine", lambda: db)
    rows = route.get_scorecard(_token="t", include_schwab=flag)["models"]
    assert all(r["weight"] is None for r in rows)


def test_scoreboard_route_passes_the_viewer(monkeypatch) -> None:
    from api.routers import oracle as route

    seen: list[Any] = []
    monkeypatch.setattr(route, "get_db_engine", lambda: "engine")
    monkeypatch.setattr(route, "build_oracle_scoreboard",
                        lambda engine, include_schwab=False: seen.append(include_schwab) or {})
    route.get_scoreboard(_token="t", include_schwab=False)
    route.get_scoreboard(_token="t", include_schwab=True)
    assert seen == [False, True]


def test_chat_context_reads_the_contributor_scoreboard() -> None:
    """Chat builds the scoreboard with no viewer flag, so it never carries model state."""
    tree = ast.parse((ROOT / "api/routers/chat.py").read_text(encoding="utf-8"))
    calls = [c for c in ast.walk(tree) if isinstance(c, ast.Call)
             and getattr(c.func, "id", None) == "build_oracle_scoreboard"]
    assert calls and all(not c.keywords and len(c.args) == 1 for c in calls)


# ── Guard: served reads of oracle_models ──────────────────────────────

_MODELS_READ = re.compile(r"\b(?:FROM|JOIN)\s+oracle_models\b", re.I)
SERVING = ("api/", "astrogrid_api/", "mcp_server.py")
# Each allowed read, and why no private verdict reaches a non-admin through it.
ALLOWED_SERVED_MODEL_READS = {
    "api/routers/oracle.py": (1, "the admin branch of /oracle/scorecard"),
}


def _served_model_reads() -> dict[str, int]:
    found: dict[str, int] = {}
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if not rel.startswith(SERVING):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        n = sum(len(_MODELS_READ.findall(node.value)) for node in ast.walk(tree)
                if isinstance(node, ast.Constant) and isinstance(node.value, str))
        if n:
            found[rel] = n
    return found


def test_no_new_served_read_of_model_state() -> None:
    allowed = {path: n for path, (n, _why) in ALLOWED_SERVED_MODEL_READS.items()}
    assert _served_model_reads() == allowed, (
        "A route reads oracle_models: its counters, P&L, weights and timestamps include the "
        "Oracle's private-use verdicts. Gate it to admin (include_schwab_rows) or build it from "
        "the public cohort, then update ALLOWED_SERVED_MODEL_READS with the reason."
    )


def test_the_allowed_scorecard_read_is_inside_the_admin_branch() -> None:
    source = (ROOT / "api/routers/oracle.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    (fn,) = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "get_scorecard"]
    admin_ifs = [n for n in ast.walk(fn) if isinstance(n, ast.If)
                 and isinstance(n.test, ast.Name) and n.test.id == "admin"]
    reads_in_branch = sum(len(_MODELS_READ.findall(c.value)) for branch in admin_ifs for stmt in branch.body
                          for c in ast.walk(stmt) if isinstance(c, ast.Constant) and isinstance(c.value, str))
    assert reads_in_branch == 1
    assignments = [n for n in ast.walk(fn) if isinstance(n, ast.Assign)
                   and any(getattr(t, "id", None) == "admin" for t in n.targets)]
    assert assignments and all(getattr(a.value.func, "id", None) == "include_schwab_rows" for a in assignments)
