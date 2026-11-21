"""Tests for the scheduled ``astrogrid.engine_run`` producer.

Background (issue #500, ``docs/astrogrid-schema.md``): ``engine_run`` rows came
only from ``engine_outputs`` in a ``POST /astrogrid/interpret`` request. The
scheduled celestial cycle sent none, so the table was empty by construction.

``oracle.astrogrid_engines`` runs the shipped JS lens engines under Node;
``AstroGridStore.save_engine_runs`` stores them once per sky_snapshot; the
cycle wires the two behind ``ASTROGRID_ENGINE_RUNS_ENABLED`` (default off).
Everything here is offline: the database is a recording double, and the one
test that starts Node runs a local script with no network.
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import shutil
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

REPO = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


def _engine_output(engine_id: str = "western", confidence: Any = 0.65) -> dict[str, Any]:
    return {
        "engine_id": engine_id,
        "family": "greco-occult",
        "direction_label": "bullish",
        "confidence": confidence,
        "horizon": "weeks",
        "reading": f"{engine_id} reading",
        "omen": "omen.",
        "prediction": "Press on confirm.",
        "claims": [],
        "rationale": [],
        "contradictions": [],
        "feature_trace": {"score": 0.1},
    }


def _completed(stdout: str = "", returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["node"], returncode=returncode, stdout=stdout, stderr=stderr)


def _runner_stdout(outputs: list[dict[str, Any]]) -> str:
    return json.dumps({"engine_outputs": outputs, "code_sha256": {"astrogrid_shared/prophecy.js": "abc"}})


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, db: _FakeDB) -> None:
        self._db = db

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        sql = str(statement)
        self._db.calls.append((sql, dict(params or {})))
        if self._db.fail:
            raise RuntimeError("db down")
        if sql.lstrip().upper().startswith("SELECT") and "engine_run" in sql:
            return _Result([(i,) for i in self._db.existing])
        if "INSERT INTO" in sql:
            self._db.next_id += 1
            return _Result([(self._db.next_id,)])
        return _Result([])


class _FakeDB:
    """Stands in for a SQLAlchemy engine: records every statement and its params."""

    def __init__(self, *, existing: list[int] | None = None, fail: bool = False) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.existing = existing or []
        self.fail = fail
        self.next_id = 100

    def begin(self) -> _FakeDB:
        return self

    def __enter__(self) -> _Conn:
        return _Conn(self)

    def __exit__(self, *exc: object) -> bool:
        return False

    def inserts(self, table: str) -> list[dict[str, Any]]:
        return [p for sql, p in self.calls if f".{table} (" in sql and "INSERT INTO" in sql]


def _store(db: _FakeDB):
    from store.astrogrid import AstroGridStore

    return AstroGridStore(db)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# oracle.astrogrid_engines
# ---------------------------------------------------------------------------


def test_pinned_snapshot_uses_the_days_stable_label_without_mutating_input() -> None:
    from oracle.astrogrid_engines import pinned_engine_snapshot

    snapshot = {"date": "2026-11-21", "timestamp": "2026-11-21T05:07:31+00:00"}
    pinned = pinned_engine_snapshot(snapshot)

    assert pinned["timestamp"] == "2026-11-21T12:00:00+00:00"
    assert snapshot["timestamp"] == "2026-11-21T05:07:31+00:00"


def test_successful_run_stamps_code_hash_and_build_time(monkeypatch: pytest.MonkeyPatch) -> None:
    import oracle.astrogrid_engines as eng

    seen: dict[str, Any] = {}

    def _fake_run(argv, **kwargs):
        seen["argv"] = argv
        seen["input"] = json.loads(kwargs["input"])
        return _completed(_runner_stdout([_engine_output("western"), _engine_output("vedic")]))

    monkeypatch.setattr(eng.subprocess, "run", _fake_run)
    result = eng.compute_engine_outputs(
        {"date": "2026-11-21", "timestamp": "2026-11-21T05:07:31+00:00"}, node_bin="node"
    )

    assert result["error"] is None
    assert [o["engine_id"] for o in result["engine_outputs"]] == ["western", "vedic"]
    assert all(o["snapshot_built_at"] == "2026-11-21T05:07:31+00:00" for o in result["engine_outputs"])
    assert all(o["code_sha256"] == {"astrogrid_shared/prophecy.js": "abc"} for o in result["engine_outputs"])
    # The engines see the pinned label, never the wall-clock build time.
    assert seen["input"]["snapshot"]["timestamp"] == "2026-11-21T12:00:00+00:00"
    assert seen["argv"][-1] == str(eng.RUNNER)


@pytest.mark.parametrize(
    "side_effect, expected",
    [
        (FileNotFoundError("node"), "not found"),
        (subprocess.TimeoutExpired(cmd="node", timeout=30), "timed out"),
        (PermissionError("denied"), "could not start"),
    ],
)
def test_runner_failures_degrade_to_no_outputs(
    monkeypatch: pytest.MonkeyPatch, side_effect: Exception, expected: str
) -> None:
    import oracle.astrogrid_engines as eng

    def _raise(*_a, **_k):
        raise side_effect

    monkeypatch.setattr(eng.subprocess, "run", _raise)
    result = eng.compute_engine_outputs({"date": "2026-11-21"}, node_bin="node")

    assert result["engine_outputs"] == []
    assert expected in result["error"]


@pytest.mark.parametrize(
    "stdout, returncode, expected",
    [
        ("", 1, "exited 1"),
        ("not json", 0, "not JSON"),
        (json.dumps({"engine_outputs": []}), 0, "no engine outputs"),
        (_runner_stdout([_engine_output("western"), _engine_output("vedic", 1.5)]), 0, "not in [0, 1]"),
        (_runner_stdout([_engine_output("western", None)]), 0, "not in [0, 1]"),
        (_runner_stdout([_engine_output("western", True)]), 0, "not in [0, 1]"),
        (_runner_stdout([{**_engine_output(), "engine_id": ""}]), 0, "no engine_id"),
    ],
)
def test_malformed_output_fails_closed(
    monkeypatch: pytest.MonkeyPatch, stdout: str, returncode: int, expected: str
) -> None:
    """One bad engine voids the set: a partial set would later read as complete."""
    import oracle.astrogrid_engines as eng

    monkeypatch.setattr(eng.subprocess, "run", lambda *_a, **_k: _completed(stdout, returncode, "boom"))
    result = eng.compute_engine_outputs({"date": "2026-11-21"}, node_bin="node")

    assert result["engine_outputs"] == []
    assert expected in result["error"]


def test_snapshot_without_a_date_is_refused_before_node_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    import oracle.astrogrid_engines as eng

    def _must_not_run(*_a, **_k):
        raise AssertionError("node must not start")

    monkeypatch.setattr(eng.subprocess, "run", _must_not_run)
    result = eng.compute_engine_outputs({"timestamp": "x"}, node_bin="node")

    assert result["engine_outputs"] == []
    assert "no usable date" in result["error"]


def test_node_without_the_esm_flag_is_retried_plainly(monkeypatch: pytest.MonkeyPatch) -> None:
    import oracle.astrogrid_engines as eng

    argvs: list[list[str]] = []

    def _fake_run(argv, **_k):
        argvs.append(list(argv))
        if eng._ESM_FLAG in argv:
            return _completed("", 9, f"node: bad option: {eng._ESM_FLAG}")
        return _completed(_runner_stdout([_engine_output()]))

    monkeypatch.setattr(eng.subprocess, "run", _fake_run)
    result = eng.compute_engine_outputs({"date": "2026-11-21"}, node_bin="node")

    assert result["error"] is None
    assert len(argvs) == 2 and eng._ESM_FLAG not in argvs[1]


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_real_runner_over_a_real_snapshot_is_the_shipped_twelve_and_deterministic() -> None:
    """End to end over the server's own snapshot shape, no database, no network."""
    from api.routers.astrogrid_helpers import build_snapshot
    from oracle.astrogrid_engines import compute_engine_outputs

    class _NoDB:
        def connect(self):
            raise RuntimeError("no database in this test")

        def begin(self):
            raise RuntimeError("no database in this test")

    snapshot = build_snapshot(date(2026, 11, 21), _NoDB())
    first = compute_engine_outputs(snapshot)
    # A later hourly build of the same day carries a different wall-clock
    # timestamp; pinning must make the engines agree anyway.
    later = compute_engine_outputs({**snapshot, "timestamp": "2026-11-21T23:59:00+00:00"})

    assert first["error"] is None, first["error"]
    ids = [o["engine_id"] for o in first["engine_outputs"]]
    assert len(ids) == 12 and len(set(ids)) == 12

    def _strip(outputs):
        return [{k: v for k, v in o.items() if k != "snapshot_built_at"} for o in outputs]

    assert _strip(first["engine_outputs"]) == _strip(later["engine_outputs"])
    for path in ("astrogrid_shared/prophecy.js", "astrogrid_shared/snapshot.js"):
        assert first["code_sha256"][path] == hashlib.sha256((REPO / path).read_bytes()).hexdigest()


def test_web_copy_of_the_engines_matches_the_shared_copy() -> None:
    """The browser serves astrogrid_web/lib; the server runs astrogrid_shared.

    If the two diverge, the server's engine_run rows stop describing what the
    site shows.
    """
    for name in ("prophecy.js", "snapshot.js"):
        assert (REPO / "astrogrid_web/lib" / name).read_bytes() == (REPO / "astrogrid_shared" / name).read_bytes()


# ---------------------------------------------------------------------------
# store.astrogrid.AstroGridStore
# ---------------------------------------------------------------------------


def test_save_engine_runs_inserts_each_engine_with_producer_stamp() -> None:
    db = _FakeDB()
    result = _store(db).save_engine_runs(
        42, 7, [_engine_output("western"), _engine_output("vedic")], producer="astrogrid_cycle"
    )

    assert result == {"engine_run_ids": [101, 102], "reused": False}
    rows = db.inserts("engine_run")
    assert [r["engine_key"] for r in rows] == ["western", "vedic"]
    assert all(r["sky_snapshot_id"] == 42 and r["lens_set_id"] == 7 for r in rows)
    assert all(json.loads(r["raw_output"])["producer"] == "astrogrid_cycle" for r in rows)
    # Values travel as bound parameters, never inside the SQL text.
    assert all("western" not in sql for sql, _ in db.calls)


def test_save_engine_runs_reuses_existing_rows_and_writes_nothing() -> None:
    db = _FakeDB(existing=[11, 12])
    result = _store(db).save_engine_runs(42, 7, [_engine_output()], producer="astrogrid_cycle")

    assert result == {"engine_run_ids": [11, 12], "reused": True}
    assert db.inserts("engine_run") == []
    select_sql, select_params = db.calls[0]
    assert "raw_output->>'producer' = :producer" in select_sql
    assert select_params == {"sky_snapshot_id": 42, "producer": "astrogrid_cycle"}


def test_save_engine_runs_caps_at_twelve() -> None:
    db = _FakeDB()
    _store(db).save_engine_runs(42, None, [_engine_output(f"e{i}") for i in range(20)], producer="p")
    assert len(db.inserts("engine_run")) == 12


def test_save_engine_runs_failure_is_reported_not_raised() -> None:
    result = _store(_FakeDB(fail=True)).save_engine_runs(42, 7, [_engine_output()], producer="p")
    assert result["engine_run_ids"] == [] and "db down" in result["error"]


def test_interpret_route_path_still_writes_client_engine_outputs_and_cites_them() -> None:
    """Regression for extracting the INSERT: the interpret path is unchanged."""
    db = _FakeDB()
    store = _store(db)
    store.save_snapshot = lambda snapshot: 42  # type: ignore[method-assign]
    store.ensure_lens_set = lambda mode, lens_ids: 7  # type: ignore[method-assign]

    ids = store.save_interpretation(
        {"snapshot": {"date": "2026-11-21"}, "engine_outputs": [_engine_output("western")]},
        {"seer": {"reading": "r", "prediction": "p"}, "summary": "s"},
    )

    engine_rows = db.inserts("engine_run")
    assert [r["engine_key"] for r in engine_rows] == ["western"]
    from store.astrogrid import CLIENT_PRODUCER

    assert json.loads(engine_rows[0]["raw_output"])["producer"] == CLIENT_PRODUCER
    seer_rows = db.inserts("seer_run")
    assert json.loads(seer_rows[0]["source_engine_runs"]) == [101]
    assert ids["seer_run_id"] == 102


def test_client_cannot_claim_the_scheduled_producers_provenance() -> None:
    """A browser-sent output naming the server producer is restamped as client.

    Otherwise it would match save_engine_runs' once-per-snapshot dedupe
    (raw_output->>'producer') and the day's server run would be skipped,
    leaving client output passed off as server-computed.
    """
    from oracle.astrogrid_engines import ENGINE_PRODUCER
    from store.astrogrid import CLIENT_PRODUCER

    db = _FakeDB()
    store = _store(db)
    store.save_snapshot = lambda snapshot: 42  # type: ignore[method-assign]
    store.ensure_lens_set = lambda mode, lens_ids: 7  # type: ignore[method-assign]
    spoofed = {**_engine_output("western"), "producer": ENGINE_PRODUCER,
               "code_sha256": {"prophecy.js": "f" * 64}, "snapshot_built_at": "2026-11-21T12:00:00Z"}

    store.save_interpretation(
        {"snapshot": {"date": "2026-11-21"}, "engine_outputs": [spoofed]},
        {"seer": {"reading": "r", "prediction": "p"}, "summary": "s"},
    )

    raw = json.loads(db.inserts("engine_run")[0]["raw_output"])
    assert raw["producer"] == CLIENT_PRODUCER != ENGINE_PRODUCER
    assert "code_sha256" not in raw and "snapshot_built_at" not in raw
    assert spoofed["producer"] == ENGINE_PRODUCER  # caller's dict not mutated


def test_non_mapping_client_output_is_stored_as_an_empty_client_row() -> None:
    from store.astrogrid import CLIENT_PRODUCER, _client_engine_output

    assert _client_engine_output("junk") == {"producer": CLIENT_PRODUCER}


# ---------------------------------------------------------------------------
# oracle.astrogrid_cycle wiring
# ---------------------------------------------------------------------------


class _CycleStore:
    def __init__(self, snapshot_id: int | None = 42) -> None:
        self.snapshot_id = snapshot_id
        self.engine_saves: list[tuple[Any, ...]] = []
        self.interpretations: list[dict[str, Any]] = []

    def save_snapshot(self, snapshot: dict[str, Any]) -> int | None:
        return self.snapshot_id

    def ensure_lens_set(self, mode: str, lens_ids: list[str]) -> int:
        return 7

    def save_engine_runs(self, snapshot_id, lens_set_id, outputs, *, producer):
        self.engine_saves.append((snapshot_id, lens_set_id, outputs, producer))
        return {"engine_run_ids": [1, 2], "reused": False}

    def save_interpretation(self, request_payload, response_payload):
        self.interpretations.append(request_payload)
        return {"seer_run_id": 2, "persona_run_id": 3}


@pytest.fixture
def cycle(monkeypatch: pytest.MonkeyPatch):
    import api.dependencies as deps
    import api.routers.astrogrid_helpers as helpers
    import oracle.astrogrid_engines as eng

    calls: dict[str, Any] = {"compute": []}
    store = _CycleStore()
    monkeypatch.setattr(helpers, "build_snapshot", lambda target, engine: {"date": str(target), "seer": {}})
    monkeypatch.setattr(helpers, "build_interpretation", lambda req: {"summary": "s", "seer": {}, "used_llm": False})
    monkeypatch.setattr(deps, "get_astrogrid_store", lambda: store)

    def _compute(snapshot, **kwargs):
        calls["compute"].append((snapshot, kwargs))
        return calls.get("compute_result") or {
            "engine_outputs": [_engine_output()],
            "code_sha256": {"astrogrid_shared/prophecy.js": "abc"},
            "error": None,
        }

    monkeypatch.setattr(eng, "compute_engine_outputs", _compute)
    calls["store"] = store
    return calls


def _run(**kwargs):
    from oracle.astrogrid_cycle import run_celestial_cycle

    return run_celestial_cycle(object(), **kwargs)


def test_flag_off_by_default_never_starts_the_engines(cycle, monkeypatch: pytest.MonkeyPatch) -> None:
    from config import settings

    monkeypatch.setattr(settings, "ASTROGRID_ENGINE_RUNS_ENABLED", False)
    result = _run()

    assert "engine_runs" not in result
    assert cycle["compute"] == []
    assert cycle["store"].engine_saves == []


def test_config_default_is_off() -> None:
    from config import Settings

    assert Settings.model_fields["ASTROGRID_ENGINE_RUNS_ENABLED"].default is False


def test_flag_on_stores_engines_against_todays_snapshot(cycle, monkeypatch: pytest.MonkeyPatch) -> None:
    from config import settings

    monkeypatch.setattr(settings, "ASTROGRID_ENGINE_RUNS_ENABLED", True)
    result = _run()

    assert result["engine_runs"]["engine_run_ids"] == [1, 2]
    assert result["errors"] == []
    (snapshot_id, lens_set_id, outputs, producer) = cycle["store"].engine_saves[0]
    assert (snapshot_id, lens_set_id, producer) == (42, 7, "astrogrid_cycle")
    assert outputs[0]["engine_id"] == "western"


def test_engine_outputs_stay_out_of_the_interpretation_request(cycle) -> None:
    """The Seer must not cite engine runs it never read (see _run_engines)."""
    _run(engine_runs=True)
    assert cycle["store"].interpretations[0]["engine_outputs"] == []


def test_past_target_is_never_back_computed(cycle) -> None:
    result = _run(engine_runs=True, target=date.today() - timedelta(days=1))

    assert result["engine_runs"]["skipped"] == "not_today"
    assert cycle["compute"] == []
    assert cycle["store"].engine_saves == []


def test_dry_run_computes_but_writes_nothing(cycle) -> None:
    result = _run(engine_runs=True, persist=False, interpret=False)

    assert len(cycle["compute"]) == 1
    assert result["engine_runs"]["skipped"] == "no_stored_snapshot"
    assert cycle["store"].engine_saves == []


def test_unstored_snapshot_gets_no_engine_rows(cycle) -> None:
    cycle["store"].snapshot_id = None
    result = _run(engine_runs=True)

    assert result["engine_runs"]["skipped"] == "no_stored_snapshot"
    assert cycle["store"].engine_saves == []


def test_engine_failure_is_reported_and_the_interpretation_still_runs(cycle) -> None:
    cycle["compute_result"] = {"engine_outputs": [], "code_sha256": {}, "error": "node executable not found: node"}
    result = _run(engine_runs=True)

    assert any(e.startswith("engine_runs: node executable not found") for e in result["errors"])
    assert cycle["store"].engine_saves == []
    assert len(cycle["store"].interpretations) == 1


def test_hermes_leaves_the_producer_to_the_config_flag() -> None:
    """Hermes must not force the producer on; the default-off flag decides."""
    from scripts import hermes_operator as ho

    body = ast.parse(inspect.getsource(ho.run_cycle))
    calls = [
        node for node in ast.walk(body)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "run_celestial_cycle"
    ]
    assert len(calls) == 1
    assert "engine_runs" not in {kw.arg for kw in calls[0].keywords}
