"""Schwab-built Oracle predictions reach admin requests only (2026-11-23).

The Oracle engine tags a prediction it built from Schwab captures in
``flow_context`` (``store.options_provenance.ORACLE_PRIVATE_USE_KEY``). Readers
that serve people read ``VISIBLE_ORACLE_PREDICTIONS``, which drops tagged rows,
and an admin request reads the plain table. Offline: SQLite plus recording
fakes, no PostgreSQL, provider or network access.
"""
from __future__ import annotations

import ast
import json
import pathlib
from typing import Any

import pytest
from sqlalchemy import create_engine, text

from store import options_provenance as op

ROOT = pathlib.Path(__file__).resolve().parents[1]
TAG = {op.ORACLE_PRIVATE_USE_KEY: op.ORACLE_PRIVATE_USE_SCHWAB}


@pytest.fixture
def predictions_db():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE oracle_predictions (id TEXT, ticker TEXT, entry_price REAL, "
                          "flow_context TEXT)"))
        for pid, ctx in (("schwab-built", {"entry_price_basis": {"price": 724.1}, **TAG}),
                         ("cboe-built", {"entry_price_basis": {"price": 714.0}}),
                         ("other-tag", {op.ORACLE_PRIVATE_USE_KEY: "something-else"}),
                         ("no-context", None)):
            conn.execute(text("INSERT INTO oracle_predictions VALUES (:i, 'SPY', 1.0, :c)"),
                         {"i": pid, "c": json.dumps(ctx) if ctx is not None else None})
    yield engine
    engine.dispose()


def _ids(engine: Any, include: Any) -> set[str]:
    sql = "SELECT id FROM " + op.VISIBLE_ORACLE_PREDICTIONS + " oracle_predictions"
    with engine.connect() as conn:
        return {r[0] for r in conn.execute(op.visible_sql(sql, include))}


def test_contributors_never_see_a_schwab_built_prediction(predictions_db) -> None:
    assert _ids(predictions_db, False) == {"cboe-built", "other-tag", "no-context"}


def test_admin_reads_every_prediction(predictions_db) -> None:
    assert _ids(predictions_db, True) == {"schwab-built", "cboe-built", "other-tag", "no-context"}


@pytest.mark.parametrize("flag", [None, 1, "true", "admin"])
def test_only_an_exact_true_admits_schwab_built_rows(predictions_db, flag) -> None:
    assert "schwab-built" not in _ids(predictions_db, flag)


def test_oracle_predictions_from_matches_visible_sql(predictions_db) -> None:
    assert op.oracle_predictions_from(True) == "oracle_predictions"
    assert op.oracle_predictions_from(False) == op.VISIBLE_ORACLE_PREDICTIONS
    assert op.oracle_predictions_from(object()) == op.VISIBLE_ORACLE_PREDICTIONS
    with predictions_db.connect() as conn:
        rows = conn.execute(text("SELECT id FROM " + op.oracle_predictions_from(False)
                                 + " oracle_predictions WHERE ticker = 'SPY'")).fetchall()
    assert {r[0] for r in rows} == {"cboe-built", "other-tag", "no-context"}


def test_fragment_has_no_bound_value_and_no_or() -> None:
    fragment = op.VISIBLE_ORACLE_PREDICTIONS
    assert ":" not in fragment.replace("::", "") and " OR " not in fragment.upper()


# ── Routes: contributors get the fragment, admins the plain table ─────


class _Result:
    def fetchone(self):
        return (0,)

    def fetchall(self):
        return []

    def scalar(self):
        return 0

    def mappings(self):
        return self

    def __iter__(self):
        return iter(())


class _Recorder:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def connect(self):
        return self

    begin = connect

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, *_a, **_k):
        self.sql.append(str(stmt))
        return _Result()


def _route_calls() -> list[tuple[str, str, dict[str, Any]]]:
    return [
        ("api.routers.oracle", "get_predictions",
         dict(ticker=None, model=None, status=None, limit=10, offset=0, cohort="all", _token="t")),
        ("api.routers.oracle", "get_latest", dict(_token="t")),
        ("api.routers.intel", "intel_predictions_active",
         dict(ticker=None, model=None, limit=10, offset=0, _token="t")),
        ("api.routers.intel", "intel_ticker", dict(symbol="SPY", days=30, _token="t")),
        ("api.routers.intel", "intel_search", dict(q="SPY", type="ticker", limit=5, offset=0, _token="t")),
        ("api.routers.intel", "intel_predictions_track_record",
         dict(model=None, ticker=None, timeframe="all", _token="t")),
        ("api.routers.mcp_export", "mcp_predictions",
         dict(symbol="SPY", lookback_days=14, limit=5, _token="t")),
        ("api.routers.valuation", "catalyst_timeline",
         dict(ticker="SPY", months_forward=12, months_back=6, _token="t")),
    ]


@pytest.mark.parametrize("module,func,kwargs", _route_calls(), ids=lambda v: v if isinstance(v, str) else "")
def test_route_reads_filter_for_contributors_only(monkeypatch, module, func, kwargs) -> None:
    import importlib

    mod = importlib.import_module(module)
    seen: dict[bool, list[str]] = {}
    for flag in (False, True):
        recorder = _Recorder()
        monkeypatch.setattr(mod, "get_db_engine", lambda r=recorder: r)
        try:
            getattr(mod, func)(**kwargs, include_schwab=flag)
        except Exception:  # noqa: BLE001 - empty fakes may trip later formatting; the reads already ran
            pass
        seen[flag] = [s for s in recorder.sql if "oracle_predictions" in s]
    assert seen[False], f"{func} issued no oracle_predictions read"
    assert all("schwab_o" in s for s in seen[False]), seen[False]
    assert seen[True] and not any("schwab_o" in s for s in seen[True]), seen[True]


def test_routes_take_the_flag_only_from_viewer_is_admin() -> None:
    """In api/, oracle_predictions_from() is passed a parameter that defaults to Depends(viewer_is_admin)."""
    bad: list[str] = []
    for path in (ROOT / "api").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            calls = [c for c in ast.walk(fn) if isinstance(c, ast.Call)
                     and getattr(c.func, "id", getattr(c.func, "attr", None)) == "oracle_predictions_from"]
            if not calls:
                continue
            params = fn.args.args + fn.args.kwonlyargs
            defaults = dict(zip([a.arg for a in fn.args.args][-len(fn.args.defaults):], fn.args.defaults)) \
                if fn.args.defaults else {}
            defaults.update({a.arg: d for a, d in zip(fn.args.kwonlyargs, fn.args.kw_defaults) if d is not None})
            for call in calls:
                arg = call.args[0] if call.args else None
                name = arg.id if isinstance(arg, ast.Name) else None
                default = defaults.get(name) if name else None
                ok = (name in {p.arg for p in params} and isinstance(default, ast.Call)
                      and getattr(default.func, "id", None) == "Depends" and default.args
                      and getattr(default.args[0], "id", None) == "viewer_is_admin")
                if not ok:
                    bad.append(f"{path.relative_to(ROOT)}:{call.lineno} {fn.name}")
    assert not bad, bad
