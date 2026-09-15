"""Tests for the AstroGrid celestial production cycle.

Background
----------
AstroGrid's celestial half had no scheduler. ``save_snapshot`` was reachable
only from ``GET /astrogrid/snapshot`` and ``save_interpretation`` only from
``POST /astrogrid/interpret``, so ``astrogrid.sky_snapshot`` stopped gaining
rows on 2026-04-28 (187 rows), ``persona_run`` on 2026-05-01 (3 rows), and
``seer_run``/``engine_run`` never held one — while the learning half kept
running hourly on its own timer the whole time.

``oracle.astrogrid_cycle.run_celestial_cycle`` is the missing producer. These
tests drive the real function against recording doubles; none of them pins a
constant for its own sake.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest


class _RecordingStore:
    """Stands in for AstroGridStore, recording what the cycle persists."""

    def __init__(
        self,
        *,
        snapshot_id: int | None = 42,
        snapshot_exc: Exception | None = None,
        interpretation_exc: Exception | None = None,
    ) -> None:
        self.snapshots: list[dict[str, Any]] = []
        self.interpretations: list[tuple[dict[str, Any], dict[str, Any]]] = []
        self._snapshot_id = snapshot_id
        self._snapshot_exc = snapshot_exc
        self._interpretation_exc = interpretation_exc

    def save_snapshot(self, snapshot: dict[str, Any]) -> int | None:
        if self._snapshot_exc is not None:
            raise self._snapshot_exc
        self.snapshots.append(snapshot)
        return self._snapshot_id

    def save_interpretation(
        self, request_payload: dict[str, Any], response_payload: dict[str, Any]
    ) -> dict[str, int | None]:
        if self._interpretation_exc is not None:
            raise self._interpretation_exc
        self.interpretations.append((request_payload, response_payload))
        return {"engine_run_id": 1, "seer_run_id": 2, "persona_run_id": 3}


def _fake_snapshot(target: date) -> dict[str, Any]:
    return {
        "date": str(target),
        "timestamp": "2026-09-14T08:00:00+00:00",
        "source": "analysis.ephemeris",
        "objects": [],
        "aspects": [{"a": "MARS", "b": "SATURN", "type": "square"}],
        "events": [{"kind": "lunar", "label": "waxing gibbous"}],
        "seer": {"reading": "deterministic", "prediction": "none", "why": []},
        "signals": {"planetaryStress": 3},
    }


@pytest.fixture
def patched_cycle(monkeypatch: pytest.MonkeyPatch):
    """Patch the helper imports the cycle performs lazily, inside the function.

    ``run_celestial_cycle`` imports from ``api.routers.astrogrid_helpers`` at
    call time to keep FastAPI out of Hermes' import graph, so patching must
    target that module rather than ``oracle.astrogrid_cycle``.
    """
    import api.routers.astrogrid_helpers as helpers

    calls: dict[str, Any] = {"snapshot": 0, "interpretation": 0, "requests": []}

    def _build_snapshot(target: date, engine: Any) -> dict[str, Any]:
        calls["snapshot"] += 1
        calls["snapshot_target"] = target
        calls["snapshot_engine"] = engine
        return _fake_snapshot(target)

    def _build_interpretation(req: Any) -> dict[str, Any]:
        calls["interpretation"] += 1
        calls["requests"].append(req)
        return {"summary": "s", "seer": {}, "used_llm": True}

    monkeypatch.setattr(helpers, "build_snapshot", _build_snapshot)
    monkeypatch.setattr(helpers, "build_interpretation", _build_interpretation)
    return calls


def _run(store: _RecordingStore, monkeypatch: pytest.MonkeyPatch, **kwargs):
    import api.dependencies as deps
    from oracle.astrogrid_cycle import run_celestial_cycle

    monkeypatch.setattr(deps, "get_astrogrid_store", lambda: store)
    return run_celestial_cycle(object(), **kwargs)


def test_cycle_persists_snapshot_and_interpretation(
    patched_cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole point: one call writes both halves of the celestial state."""
    store = _RecordingStore()
    result = _run(store, monkeypatch, target=date(2026, 9, 14))

    assert len(store.snapshots) == 1
    assert len(store.interpretations) == 1
    assert result["snapshot_id"] == 42
    assert result["interpretation_ids"] == {
        "engine_run_id": 1,
        "seer_run_id": 2,
        "persona_run_id": 3,
    }
    assert result["errors"] == []


def test_interpretation_reads_the_snapshot_that_was_stored(
    patched_cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The seer must interpret the stored sky, not a separately recomputed one.

    Building the snapshot twice would let the stored state and the
    interpretation disagree across a minute boundary.
    """
    store = _RecordingStore()
    _run(store, monkeypatch, target=date(2026, 9, 14))

    assert patched_cycle["snapshot"] == 1, "snapshot must be built exactly once"
    req = patched_cycle["requests"][0]
    assert req.snapshot == store.snapshots[0]


def test_persist_false_builds_but_never_writes(
    patched_cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _RecordingStore()
    result = _run(store, monkeypatch, target=date(2026, 9, 14), persist=False)

    assert store.snapshots == []
    assert store.interpretations == []
    assert patched_cycle["snapshot"] == 1
    assert patched_cycle["interpretation"] == 1
    assert result["persisted"] is False


def test_interpret_false_skips_the_llm_call(
    patched_cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Hermes dry run must not spend a local-LLM call on a rehearsal.

    It still builds the snapshot: that path is sub-second, deterministic and
    side-effect free, so exercising it is the point of a dry run.
    """
    store = _RecordingStore()
    result = _run(
        store, monkeypatch, target=date(2026, 9, 14), persist=False, interpret=False
    )

    assert patched_cycle["snapshot"] == 1, "dry run should still build the sky"
    assert patched_cycle["interpretation"] == 0, "dry run must not call the model"
    assert result["interpretation_skipped"] is True
    assert result["errors"] == []


def test_hermes_dry_run_passes_both_flags() -> None:
    """A dry-run cycle must neither write nor call the model."""
    import inspect

    from scripts import hermes_operator as ho

    src = inspect.getsource(ho.run_cycle)
    start = src.index("# 7c2. AstroGrid celestial cycle")
    step = src[start : src.index("# 7d. Oracle prediction cycle", start)]
    assert "persist=not dry_run" in step
    assert "interpret=not dry_run" in step, (
        "dry run must skip the interpretation too, or `hermes_operator "
        "--dry-run` spends up to ASTROGRID_CELESTIAL_TIMEOUT_SECONDS on a "
        "local-LLM call that produces nothing"
    )


def test_defaults_to_today_when_no_target_given(
    patched_cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _RecordingStore()
    result = _run(store, monkeypatch)

    assert patched_cycle["snapshot_target"] == date.today()
    assert result["date"] == date.today().isoformat()


def test_snapshot_build_failure_does_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing celestial cycle must not take the Hermes cycle down."""
    import api.routers.astrogrid_helpers as helpers

    def _boom(target: date, engine: Any) -> dict[str, Any]:
        raise RuntimeError("ephemeris unavailable")

    monkeypatch.setattr(helpers, "build_snapshot", _boom)
    store = _RecordingStore()
    result = _run(store, monkeypatch, target=date(2026, 9, 14))

    assert result["snapshot_id"] is None
    assert any("ephemeris unavailable" in e for e in result["errors"])
    assert store.interpretations == [], "must not interpret a sky it failed to build"


def test_snapshot_persist_failure_still_interprets(
    patched_cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A store write failure is reported, not fatal — the build still happened."""
    store = _RecordingStore(snapshot_exc=RuntimeError("db down"))
    result = _run(store, monkeypatch, target=date(2026, 9, 14))

    assert result["snapshot_id"] is None
    assert any("snapshot_persist" in e for e in result["errors"])
    assert patched_cycle["interpretation"] == 1


def test_interpretation_persist_failure_is_reported_not_raised(
    patched_cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _RecordingStore(interpretation_exc=RuntimeError("db down"))
    result = _run(store, monkeypatch, target=date(2026, 9, 14))

    assert len(store.snapshots) == 1
    assert any("interpretation_persist" in e for e in result["errors"])


def test_llm_absence_is_recorded_not_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Graceful degradation: no local model means a deterministic reading."""
    import api.routers.astrogrid_helpers as helpers

    monkeypatch.setattr(helpers, "build_snapshot", _fake_snapshot_builder())
    monkeypatch.setattr(
        helpers,
        "build_interpretation",
        lambda req: {"summary": "s", "seer": {}, "used_llm": False},
    )
    store = _RecordingStore()
    result = _run(store, monkeypatch, target=date(2026, 9, 14))

    assert result["used_llm"] is False
    assert result["errors"] == []
    assert len(store.interpretations) == 1, "a fallback reading is still worth storing"


def _fake_snapshot_builder():
    def _builder(target: date, engine: Any) -> dict[str, Any]:
        return _fake_snapshot(target)

    return _builder


# ── Single-code-path invariants ──────────────────────────────────────────
#
# The scheduler and the HTTP route must stay on one implementation. If a
# future edit re-inlines the build into a handler, the two paths drift and the
# scheduled snapshot stops matching the one the UI shows — the exact class of
# divergence that makes "is it producing?" unanswerable.


def test_snapshot_route_delegates_to_the_shared_builder() -> None:
    import inspect

    from api.routers import astrogrid_core as core

    src = inspect.getsource(core.get_snapshot)
    assert "build_snapshot(" in src, "route must call the shared builder"
    assert "build_astrological_ephemeris" not in src, (
        "snapshot construction must live in astrogrid_helpers.build_snapshot, "
        "not inline in the route — the Hermes celestial step calls the same "
        "function and the two must not diverge"
    )


def test_interpret_route_delegates_to_the_shared_interpreter() -> None:
    import inspect

    from api.routers import astrogrid_core as core

    src = inspect.getsource(core.interpret_snapshot)
    assert "build_interpretation(" in src, "route must call the shared interpreter"
    assert "get_llm" not in src, (
        "the LLM call must live in astrogrid_helpers.build_interpretation, not "
        "inline in the route — the Hermes celestial step calls the same function"
    )


def test_hermes_celestial_step_fits_inside_the_cycle_budget() -> None:
    """The step's own budget must be small enough to ever run."""
    from scripts import hermes_operator as ho

    assert ho.ASTROGRID_CELESTIAL_TIMEOUT_SECONDS < ho.CYCLE_TIMEOUT_SECONDS


def test_celestial_step_runs_before_the_oracle() -> None:
    """Order is the point of this placement, so pin it.

    The step first shipped as 7i, last in the cycle, and never ran once in
    two hours on 2026-09-14: four grid-hermes restarts each reset the
    in-memory oracle gate, so every cycle spent itself on a fresh oracle pass
    and nothing behind step 7d executed. A sub-second sky build plus one
    bounded LLM call has no dependency on the oracle, so it belongs in front
    of it. If someone moves it back, this fails.
    """
    import inspect

    from scripts import hermes_operator as ho

    src = inspect.getsource(ho.run_cycle)
    assert src.index("# 7c2. AstroGrid celestial cycle") < src.index(
        "# 7d. Oracle prediction cycle"
    ), "the celestial step must run before the oracle, not behind it"


# NOTE: an earlier test asserted the deferral guard was *reachable*, on the
# grounds that the budgeted steps ahead of the celestial step summed to more
# than CYCLE_TIMEOUT_SECONDS. Moving the step in front of the oracle makes
# that false -- only diagnose (240) + smart ingestion (300) + resolution (420)
# are budgeted before it now. That test existed to force exactly this
# decision, and the decision is: keep the guard, because the unbudgeted steps
# that still run first have no cap and a slow cycle can arrive here late; drop
# the reachability claim, which no longer holds.


# ---------------------------------------------------------------------------
# The real builder.
#
# Every test above replaces ``build_snapshot`` with a double, which is correct
# for testing the *cycle* but left the extracted builder itself uncovered. It
# shipped in #493 with ``datetime.now(timezone.utc)`` on its last line and no
# ``timezone`` import -- the name came from ``astrogrid_core``, which imports
# it, and did not travel with the code. 263 tests stayed green because not one
# of them called the function. In production it raised NameError on every
# Hermes cycle *and* on every ``GET /astrogrid/snapshot``.
#
# These two drive the unpatched builder, so a bare name anywhere in it fails
# here instead of on grid-svr.
# ---------------------------------------------------------------------------


class _DeadEngine:
    """An engine whose every connection attempt fails.

    ``_get_latest_resolved`` and ``_get_market_regime`` both swallow engine
    exceptions and degrade to ``None``, so the builder is exercised end to end
    with no database: the solar and regime lookups take their failure path and
    everything else is pure ephemeris arithmetic.
    """

    def connect(self) -> Any:
        raise RuntimeError("no database in this test")


def test_real_build_snapshot_returns_a_payload_without_a_database() -> None:
    """The unpatched builder must run to completion and return the payload."""
    from api.routers.astrogrid_helpers import build_snapshot

    target = date(2026, 9, 14)
    snapshot = build_snapshot(target, _DeadEngine())

    # The last statement of the function is the return dict, so a bare name
    # there is only caught by reading what comes back. Parse the timestamp
    # rather than pin its calendar year: the point is that it's a real,
    # UTC-aware `datetime.now()` call, not that it happened in 2026.
    assert snapshot["date"] == str(target)
    parsed_timestamp = datetime.fromisoformat(snapshot["timestamp"])
    assert parsed_timestamp.tzinfo is not None
    assert parsed_timestamp.utcoffset() == timedelta(0)
    assert abs(datetime.now(timezone.utc) - parsed_timestamp) < timedelta(minutes=5)
    for key in ("objects", "aspects", "events", "signals", "seer", "grid"):
        assert key in snapshot, f"{key} missing from the snapshot payload"

    # Degradation, not silence: the DB is gone, so the DB-backed solar
    # features are None while the computed fallback still lands.
    solar = snapshot["grid"]["solar"]
    assert solar["geomagnetic_kp_index"] is None
    assert isinstance(solar["solar_cycle_phase"], float)


def test_cycle_persists_a_snapshot_built_by_the_real_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Hermes path end to end, with nothing but the store faked.

    ``patched_cycle`` is deliberately absent here. This is the exact call
    Hermes step 7c2 makes, and it is the one that was failing in production
    while every mocked test passed.
    """
    store = _RecordingStore()
    result = _run(store, monkeypatch, target=date(2026, 9, 14), interpret=False)

    assert result["errors"] == [], f"real builder reported errors: {result['errors']}"
    assert result["snapshot_id"] == 42
    assert len(store.snapshots) == 1
    assert store.snapshots[0]["date"] == "2026-09-14"
    assert store.snapshots[0]["timestamp"]
