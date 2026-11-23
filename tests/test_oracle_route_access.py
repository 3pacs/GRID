"""Who may call each Oracle route, now that the Oracle has private-use verdicts (2026-11-23).

Answers Codex review 6030272662 on GRID#1403. ``POST /oracle/evolve`` scores
expired predictions and moves model weights, and its response (per-model
weight changes, hit rates, scoring summaries) is built from every scored row,
the Oracle's Schwab-built ones included: it is admin-only. The inventory below
classifies every route of ``api/routers/oracle.py``, so a new route cannot be
added without saying how it keeps private-use rows from a contributor.
Offline: FastAPI TestClient with real tokens and a fake engine.
"""
from __future__ import annotations

import ast
import pathlib
from typing import Any

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]

# method, path -> (rule, why)
ORACLE_ROUTE_ACCESS: dict[tuple[str, str], tuple[str, str]] = {
    ("GET", "/api/v1/oracle/predictions"): ("viewer", "rows through oracle_predictions_from(viewer_is_admin)"),
    ("GET", "/api/v1/oracle/latest"): ("viewer", "rows through VISIBLE_ORACLE_PREDICTIONS / admin form"),
    ("GET", "/api/v1/oracle/scoreboard"): ("viewer", "public-cohort counts; model state admin-only"),
    ("GET", "/api/v1/oracle/scorecard"): ("viewer", "running counters admin-only; public cohort otherwise"),
    ("GET", "/api/v1/oracle/evidence"): ("public", "built from the public cohort; cohort label checked"),
    ("GET", "/api/v1/oracle/predict-live/{ticker}"): (
        "public", "EnsemblePredictor never reads the Oracle's private-use flag"),
    ("GET", "/api/v1/oracle/guard"): ("public", "a fresh engine: no verdicts of a stored cycle"),
    ("POST", "/api/v1/oracle/publish"): ("public", "writes the request's own AstroGrid record; reads nothing private"),
    ("POST", "/api/v1/oracle/evolve"): ("admin", "maintenance write; summary built from private-inclusive state"),
}


def _routes() -> dict[tuple[str, str], Any]:
    from api.routers import oracle

    return {(method, route.path): route for route in oracle.router.routes for method in route.methods}


def test_every_oracle_route_is_classified() -> None:
    assert set(_routes()) == set(ORACLE_ROUTE_ACCESS), (
        "Classify each Oracle route in ORACLE_ROUTE_ACCESS: how does it keep the Oracle's "
        "private-use (Schwab-built) predictions and model state from a contributor?"
    )
    assert all(why for _rule, why in ORACLE_ROUTE_ACCESS.values())


def _dependency_calls(route: Any) -> list[ast.Call]:
    source = pathlib.Path(route.endpoint.__code__.co_filename).read_text(encoding="utf-8")
    fn = next(n for n in ast.walk(ast.parse(source))
              if isinstance(n, ast.FunctionDef) and n.name == route.endpoint.__name__)
    defaults = list(fn.args.defaults) + [d for d in fn.args.kw_defaults if d is not None]
    return [d for d in defaults if isinstance(d, ast.Call) and getattr(d.func, "id", None) == "Depends"]


def _depends_on(route: Any) -> set[str]:
    names: set[str] = set()
    for call in _dependency_calls(route):
        arg = call.args[0] if call.args else None
        if isinstance(arg, ast.Name):
            names.add(arg.id)
        elif isinstance(arg, ast.Call) and getattr(arg.func, "id", None) == "require_role":
            names.add("require_role:" + ",".join(a.value for a in arg.args if isinstance(a, ast.Constant)))
    return names


@pytest.mark.parametrize("key", sorted(ORACLE_ROUTE_ACCESS), ids=lambda k: f"{k[0]} {k[1]}")
def test_route_dependencies_match_its_rule(key) -> None:
    rule, _why = ORACLE_ROUTE_ACCESS[key]
    deps = _depends_on(_routes()[key])
    if rule == "admin":
        assert deps == {"require_role:admin"}, deps
    elif rule == "viewer":
        assert {"require_auth", "viewer_is_admin"} <= deps, deps
    else:
        assert "require_auth" in deps, deps


# ── /evolve role contract over HTTP ───────────────────────────────────


@pytest.fixture
def client(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api.routers import oracle

    calls: list[str] = []

    class _Engine:
        def __init__(self, db_engine=None):
            calls.append("built")

        def score_expired_predictions(self):
            calls.append("scored")
            return {"scored": 1, "private_inclusive": True}

        def evolve_weights(self):
            calls.append("evolved")
            return {"m1": {"old": 1.0, "new": 1.05, "hit_rate": 0.6}}

    monkeypatch.setattr(oracle, "OracleEngine", _Engine)
    monkeypatch.setattr(oracle, "get_db_engine", lambda: "engine")
    app = FastAPI()
    app.include_router(oracle.router)
    return TestClient(app), calls


def _headers(role: str | None) -> dict[str, str]:
    from api import auth

    if role is None:
        return {}
    return {"Authorization": "Bearer " + auth.create_token(role=role, username=f"test-{role}")}


@pytest.mark.parametrize("role,status", [(None, 401), ("contributor", 403)])
def test_non_admins_cannot_run_evolve_or_read_its_summary(client, role, status) -> None:
    http, calls = client
    response = http.post("/api/v1/oracle/evolve", headers=_headers(role))
    assert response.status_code == status
    assert "evolution" not in response.text and "hit_rate" not in response.text
    assert calls == []  # nothing scored, nothing evolved


def test_admin_runs_evolve(client) -> None:
    http, calls = client
    response = http.post("/api/v1/oracle/evolve", headers=_headers("admin"))
    assert response.status_code == 200
    assert response.json()["evolution"]["m1"]["new"] == 1.05
    assert calls == ["built", "scored", "evolved"]


def test_a_forged_role_claim_is_refused(client) -> None:
    http, calls = client
    response = http.post("/api/v1/oracle/evolve", headers={"Authorization": "Bearer not-a-jwt"})
    assert response.status_code == 401 and calls == []
