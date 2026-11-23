"""Shared Oracle aggregates are built from the public cohort only (2026-11-23).

Answers Codex review 6027282293 on GRID#1396: a scoreboard row, an evidence
window or a realized-alpha day can hold a single Schwab-built prediction, and
its mean return then gives the Schwab entry back from the public close. So every
aggregate whose result is shared (scoreboard, evidence and its precompute,
calibration, realized alpha, hypothesis and Brier trackers) reads
``VISIBLE_ORACLE_PREDICTIONS``, for admins and contributors alike. The control
in each test: the same rows without the tag are all counted, so ordinary shared
results stay available. Offline: SQLite and recording fakes.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine, text

from store import options_provenance as op

TAG = {op.ORACLE_PRIVATE_USE_KEY: op.ORACLE_PRIVATE_USE_SCHWAB}
TODAY = date.today()


@pytest.fixture
def mixed_db():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE oracle_predictions (id TEXT, ticker TEXT, model_name TEXT, verdict TEXT, "
            "pnl_pct REAL, expiry DATE, dedup_keep BOOLEAN, recording_policy TEXT, flow_context TEXT)"))
        conn.execute(text("CREATE TABLE oracle_models (name TEXT, weight REAL, sharpe REAL, description TEXT)"))
        conn.execute(text("INSERT INTO oracle_models VALUES ('m1', 1.0, 0, ''), ('m2', 0.5, 0, '')"))
        past, future = (TODAY - timedelta(days=1)).isoformat(), (TODAY + timedelta(days=1)).isoformat()
        for row in (
            ("pub-spy", "SPY", "m1", "hit", 1.0, past, "legacy", None),
            ("pub-qqq", "QQQ", "m1", "miss", -2.0, past, "oracle_forward_v2", {"entry_price_basis": {}}),
            ("schwab-spy", "SPY", "m1", "hit", 9.0, past, "oracle_forward_v2", {"entry_price_basis": {}, **TAG}),
            ("schwab-iwm", "IWM", "m2", "pending", None, future, "oracle_forward_v2", TAG),
        ):
            conn.execute(text("INSERT INTO oracle_predictions VALUES (:i, :t, :m, :v, :p, :e, 1, :r, :c)"),
                         {"i": row[0], "t": row[1], "m": row[2], "v": row[3], "p": row[4], "e": row[5],
                          "r": row[6], "c": json.dumps(row[7]) if row[7] is not None else None})
    yield engine
    engine.dispose()


def _untag(engine: Any) -> None:
    with engine.begin() as conn:
        conn.execute(text("UPDATE oracle_predictions SET flow_context = NULL"))


# ── Scoreboard (Oracle route, chat context, AstroGrid rollups) ────────


def test_scoreboard_overall_counts_the_public_cohort_only(mixed_db) -> None:
    from oracle.scoreboard import _fetch_overall_stats

    stats = _fetch_overall_stats(mixed_db)
    assert stats["total_predictions"] == 2 and stats["hits"] == 1 and stats["pending"] == 0
    assert stats["mean_signed_return_pct"] == -0.5  # (1.0 - 2.0) / 2; the Schwab 9.0 is not in it
    _untag(mixed_db)
    assert _fetch_overall_stats(mixed_db)["total_predictions"] == 4


def test_scoreboard_ticker_rows_never_carry_a_schwab_return(mixed_db) -> None:
    from oracle.scoreboard import _fetch_ticker_rows

    rows = {r["ticker"]: r for r in _fetch_ticker_rows(mixed_db)}
    assert set(rows) == {"SPY", "QQQ"}
    assert rows["SPY"]["total"] == 1 and rows["SPY"]["avg_pnl"] == 1.0
    _untag(mixed_db)
    rows = {r["ticker"]: r for r in _fetch_ticker_rows(mixed_db)}
    assert rows["SPY"]["avg_pnl"] == 5.0 and "IWM" in rows


def test_scoreboard_model_rows_and_recording_cohorts(mixed_db) -> None:
    from oracle.scoreboard import _fetch_model_rows, _fetch_recording_cohorts

    models = {m["name"]: m for m in _fetch_model_rows(mixed_db)}
    assert models["m1"]["predictions_made"] == 2 and models["m1"]["cumulative_pnl"] == -1.0
    assert models["m2"]["predictions_made"] == 0
    forward = _fetch_recording_cohorts(mixed_db)["oracle_forward_v2"]
    assert forward["total"] == 1 and forward["mean_signed_return_pct"] == -2.0
    _untag(mixed_db)
    assert _fetch_recording_cohorts(mixed_db)["oracle_forward_v2"]["total"] == 3


# ── Evidence: the builder, its precompute, and the route's caches ─────


def test_evidence_reads_the_public_cohort() -> None:
    from oracle import evidence

    assert op.VISIBLE_ORACLE_PREDICTIONS + " p" in evidence._OUTCOMES_SQL
    assert "FROM oracle_predictions p" not in evidence._OUTCOMES_SQL
    assert evidence.build_evidence([])["cohort"] == evidence.EVIDENCE_COHORT == "public"


class _Cache:
    def __init__(self, value: Any = None) -> None:
        self.value = value

    def get(self, _key: str) -> Any:
        return self.value

    def set(self, _key: str, value: Any) -> None:
        self.value = value


def _route(monkeypatch, stored: Any, cached: Any = None) -> tuple[dict[str, Any], list[str]]:
    import api.precompute_serving as serving
    from api.routers import oracle as route
    from oracle import evidence

    built: list[str] = []
    monkeypatch.setattr(serving, "read_precomputed", lambda key: (stored, {"source": "precompute"}))
    monkeypatch.setattr(route, "_evidence_cache", _Cache(cached))
    monkeypatch.setattr(route, "get_db_engine", lambda: "engine")
    monkeypatch.setattr(evidence, "load_outcomes", lambda engine: built.append(engine) or [])
    return route.get_evidence(_token="t"), built


def _payload(**extra: Any) -> dict[str, Any]:
    from oracle import evidence

    return {"evidence_class": evidence.EVIDENCE_CLASS, "summary": {"marker": "stored"}, **extra}


def test_a_stored_snapshot_without_the_public_label_is_rebuilt(monkeypatch) -> None:
    """Cold path: a precompute written by a worker that predates the filter."""
    result, built = _route(monkeypatch, stored=_payload())
    assert built == ["engine"] and result["cohort"] == "public"
    assert result.get("summary", {}).get("marker") != "stored"


def test_a_cached_result_without_the_public_label_is_rebuilt(monkeypatch) -> None:
    """Warm path: the in-process cache holds a result from before the filter."""
    result, built = _route(monkeypatch, stored=None, cached=_payload())
    assert built == ["engine"] and result["cohort"] == "public"


def test_public_snapshots_and_cache_are_still_served(monkeypatch) -> None:
    result, built = _route(monkeypatch, stored=_payload(cohort="public"))
    assert built == [] and result["summary"]["marker"] == "stored" and result["precompute"]
    result, built = _route(monkeypatch, stored=None, cached=_payload(cohort="public"))
    assert built == [] and result["summary"]["marker"] == "stored"


def test_the_evidence_precompute_job_builds_from_the_public_loader(monkeypatch) -> None:
    from types import SimpleNamespace

    from intelligence import api_precompute as ap
    from oracle import evidence

    monkeypatch.setattr(evidence, "load_outcomes", lambda engine: [])
    ctx = SimpleNamespace(engine="engine", now=None)
    (output,) = ap.build_oracle_evidence(ctx)
    assert output.payload["cohort"] == "public"


# ── Realized alpha and the other stored aggregates ────────────────────


class _Recorder:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, *_a, **_k):
        self.sql.append(str(stmt))
        return self

    def fetchall(self):
        return []


def test_realized_alpha_collects_the_public_cohort() -> None:
    from alpha_research import realized_alpha

    recorder = _Recorder()
    assert realized_alpha._collect_oracle_predictions(recorder, TODAY) == []
    (sql,) = recorder.sql
    assert "schwab_o" in sql and op.ORACLE_PRIVATE_USE_KEY in sql


def test_calibration_reads_the_public_cohort() -> None:
    from oracle import calibration

    recorder = _Recorder()
    try:
        calibration.compute_calibration(recorder)
    except Exception:  # noqa: BLE001 - the empty fake may trip later arithmetic; the read already ran
        pass
    reads = [s for s in recorder.sql if "oracle_predictions" in s]
    assert reads and all("schwab_o" in s for s in reads), reads
