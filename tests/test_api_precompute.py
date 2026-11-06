"""W3-2: precomputed API results (store, runner, serving, builders).

Offline: the store is an in-memory fake (the real-PostgreSQL proof of the
table, upserts and advisory lock is ``test_api_precomputed_results_pg.py``).
"""

from __future__ import annotations

import json
import math
import os
import sys
import threading
import types
from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("DB_PASSWORD", "testpass")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-ci")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth
from intelligence import api_precompute as ap
from store import precomputed as store

NOW = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------


class FakeStore:
    """Mimics the four store calls the runner and the serving layer make."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], store.PrecomputedRow] = {}
        self.success_writes: list[tuple[str, dict]] = []

    def put(self, key: str, params: dict, payload: Any, *, computed_at: datetime = NOW,
            as_of: Any = None, status: str = "ok", error: str | None = None,
            version: str = "1", inputs: dict | None = None,
            last_attempt_at: datetime | None = None) -> None:
        ph = store.params_hash(params)
        self.rows[(key, ph)] = store.PrecomputedRow(
            key=key, params_hash=ph, params=params,
            payload_json=store.dumps(payload) if payload is not None else None,
            computed_at=computed_at if payload is not None else None,
            as_of=store._as_utc(as_of), version=version, inputs=inputs or {},
            duration_s=1.5, last_attempt_at=last_attempt_at or computed_at,
            last_status=status, last_error=error,
        )

    # store API
    def read_result(self, _engine, key, params):
        return self.rows.get((key, store.params_hash(params)))

    def read_key_rows(self, _engine, keys):
        return [r for (k, _), r in self.rows.items() if k in keys]

    def write_success(self, _engine, *, key, params, payload, as_of, version, inputs=None,
                      duration_s=None, computed_at=None):
        self.success_writes.append((key, dict(params)))
        self.put(key, dict(params), payload, computed_at=computed_at or datetime.now(timezone.utc),
                 as_of=as_of, version=version, inputs=dict(inputs or {}))

    def record_attempt_failure(self, _engine, *, key, params, status, error, version, duration_s=None):
        ph = store.params_hash(params)
        old = self.rows.get((key, ph))
        now = datetime.now(timezone.utc)
        if old is None:
            self.rows[(key, ph)] = store.PrecomputedRow(
                key=key, params_hash=ph, params=dict(params), payload_json=None, computed_at=None,
                as_of=None, version=version, inputs=None, duration_s=duration_s,
                last_attempt_at=now, last_status=status, last_error=error)
        else:
            self.rows[(key, ph)] = store.PrecomputedRow(
                **{**old.__dict__, "last_attempt_at": now, "last_status": status, "last_error": error})


@pytest.fixture
def fake_store(monkeypatch) -> FakeStore:
    fs = FakeStore()
    for name in ("read_result", "read_key_rows", "write_success", "record_attempt_failure"):
        monkeypatch.setattr(store, name, getattr(fs, name))
    monkeypatch.setattr(ap, "_touch_attempt", lambda *_a, **_k: None)
    return fs


class _SqliteEngine:
    """Enough of an Engine for the runner's dialect check (no advisory lock)."""

    class dialect:
        name = "sqlite"


ENGINE = _SqliteEngine()


# ---------------------------------------------------------------------------
# store helpers
# ---------------------------------------------------------------------------


class TestStoreHelpers:
    def test_params_hash_is_canonical(self):
        assert store.params_hash({"a": 1, "b": None}) == store.params_hash({"b": None, "a": 1})
        assert store.params_hash({"days": 252}) != store.params_hash({"days": 504})
        assert store.params_hash({}) == store.params_hash(None)

    def test_dumps_drops_nan_and_handles_numpy_and_dates(self):
        out = json.loads(store.dumps({
            "nan": float("nan"), "inf": float("inf"), "np": np.float64(1.5), "npnan": np.float64("nan"),
            "arr": np.array([1, 2]), "d": date(2026, 11, 6), "nested": [{"x": math.nan}],
        }))
        assert out == {"nan": None, "inf": None, "np": 1.5, "npnan": None, "arr": [1, 2],
                       "d": "2026-11-06", "nested": [{"x": None}]}

    def test_splice_meta_produces_valid_json(self):
        meta = {"computed_at": "2026-11-06T12:00:00+00:00", "stale": False}
        spliced = json.loads(store.splice_meta('{"a": [1, 2], "b": {"c": null}}', meta))
        assert spliced == {"a": [1, 2], "b": {"c": None}, "precompute": meta}
        assert json.loads(store.splice_meta("{}", meta)) == {"precompute": meta}
        with pytest.raises(ValueError):
            store.splice_meta("[1, 2]", meta)

    def test_freshness_meta_labels_age_and_staleness(self):
        fs = FakeStore()
        fs.put("k", {}, {"x": 1}, computed_at=NOW - timedelta(hours=3), as_of=date(2026, 11, 5))
        row = fs.read_result(None, "k", {})
        fresh = store.freshness_meta(row, key="k", params={}, stale_after_s=6 * 3600, cadence_s=3600,
                                     now=NOW)
        assert fresh["status"] == "fresh" and fresh["stale"] is False
        assert fresh["age_seconds"] == 3 * 3600
        assert fresh["as_of"] == "2026-11-05"  # a date stays a date, not the wall clock
        stale = store.freshness_meta(row, key="k", params={}, stale_after_s=3600, cadence_s=600, now=NOW)
        assert stale["status"] == "stale" and stale["stale"] is True

    def test_freshness_meta_never_computed_is_unknown_not_fresh(self):
        meta = store.freshness_meta(None, key="k", params={}, stale_after_s=60, cadence_s=60, now=NOW)
        assert meta["status"] == "not_computed"
        assert meta["stale"] is None and meta["computed_at"] is None and meta["as_of"] is None


# ---------------------------------------------------------------------------
# Due rule and runner
# ---------------------------------------------------------------------------


def _job(build, **kw) -> ap.Job:
    defaults = {
        "name": "t", "cadence_s": 3600, "stale_after_s": 7200, "version": "1", "build": build,
        "outputs": (("t.key", ap._p(days=5)),), "empty_fields": {"t.key": ("items",)},
    }
    defaults.update(kw)
    return ap.Job(**defaults)


class TestDueRule:
    def test_never_attempted_is_due(self):
        assert "never attempted" in ap.due_reason(_job(lambda c: []), [], NOW)

    def test_within_cadence_is_not_due_and_past_cadence_is(self, fake_store):
        job = _job(lambda c: [])
        fake_store.put("t.key", {"days": 5}, {"items": []}, computed_at=NOW - timedelta(minutes=30))
        assert ap.due_reason(job, fake_store.read_key_rows(None, ["t.key"]), NOW) is None
        fake_store.put("t.key", {"days": 5}, {"items": []}, computed_at=NOW - timedelta(minutes=61))
        assert ap.due_reason(job, fake_store.read_key_rows(None, ["t.key"]), NOW)

    def test_failed_attempt_retries_after_fifteen_minutes(self, fake_store):
        job = _job(lambda c: [])
        fake_store.put("t.key", {"days": 5}, {"items": []}, computed_at=NOW - timedelta(hours=5),
                       status="error", last_attempt_at=NOW - timedelta(minutes=10))
        assert ap.due_reason(job, fake_store.read_key_rows(None, ["t.key"]), NOW) is None
        fake_store.put("t.key", {"days": 5}, {"items": []}, computed_at=NOW - timedelta(hours=5),
                       status="error", last_attempt_at=NOW - timedelta(minutes=16))
        assert ap.due_reason(job, fake_store.read_key_rows(None, ["t.key"]), NOW)

    def test_version_bump_is_due(self, fake_store):
        fake_store.put("t.key", {"days": 5}, {"items": []}, version="1")
        assert "version" in ap.due_reason(_job(lambda c: [], version="2"),
                                          fake_store.read_key_rows(None, ["t.key"]), NOW)


class TestRunner:
    def test_success_is_written_with_as_of(self, fake_store):
        job = _job(lambda c: [ap.Output("t.key", {"days": 5}, {"items": [1]}, date(2026, 11, 5))])
        summary = ap.run_job(ENGINE, job)
        assert summary["ran"] and summary["written"] == 1
        row = fake_store.read_result(None, "t.key", {"days": 5})
        assert row.payload() == {"items": [1]}
        assert row.as_of.date() == date(2026, 11, 5)

    def test_unavailable_never_overwrites_a_good_payload(self, fake_store):
        fake_store.put("t.key", {"days": 5}, {"items": ["good"]}, computed_at=NOW - timedelta(days=2))
        job = _job(lambda c: [ap.Output("t.key", {"days": 5}, None, None, unavailable_reason="thin")])
        summary = ap.run_job(ENGINE, job, force=True)
        assert summary["unavailable"] == 1 and summary["written"] == 0
        row = fake_store.read_result(None, "t.key", {"days": 5})
        assert row.payload() == {"items": ["good"]}  # kept, with its real computed_at
        assert row.computed_at == NOW - timedelta(days=2)
        assert row.last_status == "unavailable" and row.last_error == "thin"

    def test_payload_marked_unavailable_is_not_persisted(self, fake_store):
        job = _job(lambda c: [ap.Output("t.key", {"days": 5},
                                        {"available": False, "reason": "no data"}, None)])
        ap.run_job(ENGINE, job, force=True)
        row = fake_store.read_result(None, "t.key", {"days": 5})
        assert not row.has_payload and row.last_error == "no data"

    def test_exception_is_recorded_on_declared_outputs(self, fake_store):
        def boom(_ctx):
            raise RuntimeError("db gone")

        summary = ap.run_job(ENGINE, _job(boom))
        assert "db gone" in summary["error"]
        row = fake_store.read_result(None, "t.key", {"days": 5})
        assert row.last_status == "error" and "db gone" in row.last_error

    def test_signature_unchanged_skips_the_build(self, fake_store):
        calls: list[int] = []

        def build(ctx):
            calls.append(1)
            return [ap.Output("t.key", {"days": 5}, {"items": []}, None, inputs={"signature": ctx.signature})]

        job = _job(build, signature=lambda _e: "2026-11-05:474", max_age_s=12 * 3600, cadence_s=60)
        ap.run_job(ENGINE, job)
        assert calls == [1]
        # Past the cadence, same inputs: skipped.
        old = fake_store.read_result(None, "t.key", {"days": 5})
        fake_store.rows[(old.key, old.params_hash)] = store.PrecomputedRow(
            **{**old.__dict__, "last_attempt_at": old.last_attempt_at - timedelta(minutes=5)})
        summary = ap.run_job(ENGINE, job)
        assert calls == [1] and "unchanged" in summary["skipped"]
        # New options pull: rebuilt.
        job2 = _job(build, signature=lambda _e: "2026-11-06:480", max_age_s=12 * 3600, cadence_s=60)
        fake_store.rows[(old.key, old.params_hash)] = store.PrecomputedRow(
            **{**fake_store.read_result(None, "t.key", {"days": 5}).__dict__,
               "last_attempt_at": NOW - timedelta(days=1)})
        ap.run_job(ENGINE, job2)
        assert calls == [1, 1]

    def test_run_due_isolates_failures(self, fake_store, monkeypatch):
        good = _job(lambda c: [ap.Output("g.key", {}, {"x": 1}, None)], name="good",
                    outputs=(("g.key", ap._p()),), empty_fields={"g.key": ()})
        bad = _job(lambda c: (_ for _ in ()).throw(ValueError("x")), name="bad",
                   outputs=(("b.key", ap._p()),), empty_fields={"b.key": ()})
        monkeypatch.setattr(ap, "JOBS", (bad, good))
        results = ap.run_due(ENGINE)
        assert [r["job"] for r in results] == ["bad", "good"]
        assert results[1]["written"] == 1

    def test_registry_keys_are_unique_and_declared(self):
        seen: dict[str, str] = {}
        for job in ap.JOBS:
            assert job.outputs, job.name
            for key in job.keys:
                assert key not in seen, f"{key} in {seen.get(key)} and {job.name}"
                seen[key] = job.name
                assert key in job.empty_fields
            assert job.stale_after_s >= job.cadence_s


class TestBackgroundTick:
    def test_one_worker_at_a_time_and_kill_switch(self, monkeypatch):
        release = threading.Event()
        runs: list[int] = []
        monkeypatch.setattr(ap, "run_due", lambda _e: (runs.append(1), release.wait(5)))
        monkeypatch.setattr(ap, "_worker", None)
        assert ap.start_background_tick(lambda: ENGINE) is True
        assert ap.start_background_tick(lambda: ENGINE) is False  # still running
        release.set()
        ap._worker.join(2)
        assert runs == [1]
        monkeypatch.setenv("GRID_API_PRECOMPUTE_ENABLED", "0")
        assert ap.start_background_tick(lambda: ENGINE) is False

    def test_scheduler_registers_the_tick(self):
        from pathlib import Path

        src = (Path(__file__).resolve().parents[1] / "intelligence/scheduler.py").read_text()
        assert "_sched.every(5).minutes.do(_api_precompute_tick)" in src
        assert "start_background_tick()" in src


# ---------------------------------------------------------------------------
# Serving
# ---------------------------------------------------------------------------


ROUTES: list[tuple[str, str, str, dict]] = [
    ("api.routers.intelligence_regime", "/api/v1/intelligence/regime/analogs?n=20&include_timesfm=true",
     "intelligence.regime_analogs", {"n": 20, "min_quality": 0.4}),
    ("api.routers.intelligence", "/api/v1/intelligence/globe", "intelligence.globe", {}),
    ("api.routers.intelligence", "/api/v1/intelligence/influence/hypocrisy",
     "intelligence.influence_hypocrisy", {}),
    ("api.routers.intelligence", "/api/v1/intelligence/influence/circular-flows",
     "intelligence.influence_circular_flows", {}),
    ("api.routers.signals", "/api/v1/signals/snapshot", "signals.snapshot", {}),
    ("api.routers.discovery", "/api/v1/discovery/smart-heatmap?orthogonal_only=true",
     "discovery.smart_heatmap", {"family": None, "orthogonal_only": True, "corr_threshold": 0.8}),
    ("api.routers.intel_cross_reference", "/api/v1/intelligence/cross-reference/narrative",
     "intelligence.cross_reference_narrative", {}),
    ("api.routers.associations", "/api/v1/associations/correlation-matrix?days=252",
     "associations.correlation_matrix", {"days": 252, "min_corr": 0.3, "show_trivial": False}),
    ("api.routers.associations", "/api/v1/associations/anomalies?sigma_threshold=2.5",
     "associations.anomalies", {"sigma_threshold": 2.5, "days": 252}),
    ("api.routers.associations", "/api/v1/associations/regime-features?days=504",
     "associations.regime_features", {"days": 504}),
    ("api.routers.options", "/api/v1/options/100x", "options.100x", {}),
]


def _client(module_name: str) -> TestClient:
    import importlib

    module = importlib.import_module(module_name)
    app = FastAPI()
    app.include_router(module.router)
    app.dependency_overrides[require_auth] = lambda: "fixture"
    return TestClient(app)


class TestServing:
    @pytest.mark.parametrize(("module", "url", "key", "params"), ROUTES)
    def test_route_serves_the_stored_body_with_freshness(self, fake_store, module, url, key, params):
        fake_store.put(key, params, {"items": [1, 2], "timesfm": {"x": 1}},
                       computed_at=datetime.now(timezone.utc) - timedelta(minutes=5),
                       as_of=date(2026, 11, 5))
        r = _client(module).get(url)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["items"] == [1, 2]
        meta = body["precompute"]
        assert meta["key"] == key and meta["status"] == "fresh"
        assert meta["as_of"] == "2026-11-05" and 290 <= meta["age_seconds"] <= 400

    @pytest.mark.parametrize(("module", "url", "key", "params"), ROUTES)
    def test_route_without_result_is_503_unavailable(self, fake_store, module, url, key, params):
        r = _client(module).get(url)
        assert r.status_code == 503
        assert r.headers["Retry-After"] == "300"
        body = r.json()
        assert body["available"] is False and body["status"] == "unavailable"
        assert body["as_of"] is None and "never computes inline" in body["reason"]
        for f in ap.job_for_key(key).empty_fields[key]:
            assert body[f] is None
        assert body["precompute"]["status"] == "not_computed"

    def test_undeclared_params_say_so(self, fake_store):
        r = _client("api.routers.associations").get("/api/v1/associations/correlation-matrix?days=90")
        assert r.status_code == 503
        body = r.json()
        assert "not precomputed" in body["reason"]
        assert {"days": 252, "min_corr": 0.3, "show_trivial": False} in body["precomputed_params"]

    def test_failed_job_reason_is_served(self, fake_store):
        fake_store.record_attempt_failure(None, key="intelligence.globe", params={}, status="error",
                                          error="OperationalError: timeout", version="1")
        body = _client("api.routers.intelligence").get("/api/v1/intelligence/globe").json()
        assert "OperationalError: timeout" in body["reason"]

    def test_stale_payload_is_labelled_stale(self, fake_store):
        fake_store.put("intelligence.globe", {}, {"countries": []},
                       computed_at=datetime.now(timezone.utc) - timedelta(days=2))
        body = _client("api.routers.intelligence").get("/api/v1/intelligence/globe").json()
        assert body["precompute"]["stale"] is True and body["precompute"]["status"] == "stale"

    def test_analogs_without_timesfm_drops_the_section(self, fake_store):
        fake_store.put("intelligence.regime_analogs", {"n": 20, "min_quality": 0.4},
                       {"available": True, "timesfm": {"x": 1}, "matches": {}})
        body = _client("api.routers.intelligence_regime").get(
            "/api/v1/intelligence/regime/analogs?n=20&include_timesfm=false").json()
        assert "timesfm" not in body and body["available"] is True and "precompute" in body

    def test_store_read_failure_is_503_not_compute(self, monkeypatch):
        def broken(*_a, **_k):
            raise RuntimeError('relation "api_precomputed_results" does not exist')

        monkeypatch.setattr(store, "read_result", broken)
        r = _client("api.routers.signals").get("/api/v1/signals/snapshot")
        assert r.status_code == 503 and "unreadable" in r.json()["reason"]

    def test_architecture_combines_stored_stats_with_live_parts(self, fake_store, monkeypatch):
        from api.routers import system

        monkeypatch.setattr(system, "_get_puller_stats", lambda *_a: pytest.fail("computed inline"))
        r = _client("api.routers.system").get("/api/v1/system/architecture")
        body = r.json()
        assert body["stats"]["total_features"] is None  # unknown, not 0
        assert body["precompute"]["status"] == "not_computed"
        fake_store.put("system.architecture_db", {}, {
            "puller_nodes": [{"id": "fred", "label": "FRED", "type": "puller", "status": "healthy"}],
            "feature_count": 10, "resolved_count": 5, "raw_count": 7},
            computed_at=datetime.now(timezone.utc))  # served against the wall clock
        body = _client("api.routers.system").get("/api/v1/system/architecture").json()
        assert body["stats"]["total_features"] == 10 and body["stats"]["total_pullers"] == 1
        assert body["precompute"]["status"] == "fresh"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _check(name, assessment="consistent", div=0.5):
    from intelligence.cross_reference import CrossRefCheck

    return CrossRefCheck(name=name, category="gdp", official_source="o", official_value=1.0,
                         physical_source="p", physical_value=1.0, expected_relationship="",
                         actual_divergence=div, assessment=assessment, implication="i",
                         confidence=0.5, checked_at="2026-11-06T06:40:07+00:00")


class TestNarrativeBuilder:
    def _report(self, checks):
        from intelligence.cross_reference import LieDetectorReport

        red = [c for c in checks if c.assessment in ("major_divergence", "contradiction")]
        return LieDetectorReport(checks=checks, red_flags=red, narrative="",
                                 generated_at="2026-11-06T06:40:07+00:00")

    def test_llm_called_once_per_input_hash(self, fake_store, monkeypatch):
        import intelligence.cross_reference as xref

        checks = [_check("a"), _check("b", "major_divergence", 2.4)]
        calls: list[int] = []
        monkeypatch.setattr(xref, "load_recent_report", lambda *_a, **_k: self._report(checks))
        monkeypatch.setattr(xref, "generate_narrative_with_source",
                            lambda c, r: (calls.append(1), ("prose", "llm"))[1])
        job = ap.JOBS_BY_NAME["cross_reference_narrative"]
        ap.run_job(ENGINE, job, force=True)
        ap.run_job(ENGINE, job, force=True)
        assert calls == [1]
        row = fake_store.read_result(None, "intelligence.cross_reference_narrative", {})
        body = row.payload()
        assert body["narrative"] == "prose" and body["narrative_source"] == "llm"
        assert row.as_of.date() == date(2026, 11, 6)
        # Checks changed -> regenerated.
        checks.append(_check("c", "contradiction", 3.1))
        ap.run_job(ENGINE, job, force=True)
        assert calls == [1, 1]

    def test_fallback_narrative_is_labelled_and_retried(self, fake_store, monkeypatch):
        import intelligence.cross_reference as xref

        calls: list[int] = []
        monkeypatch.setattr(xref, "load_recent_report", lambda *_a, **_k: self._report([_check("a")]))
        monkeypatch.setattr(xref, "generate_narrative_with_source",
                            lambda c, r: (calls.append(1), ("summary", "fallback"))[1])
        job = ap.JOBS_BY_NAME["cross_reference_narrative"]
        ap.run_job(ENGINE, job, force=True)
        ap.run_job(ENGINE, job, force=True)
        assert calls == [1, 1]
        body = fake_store.read_result(None, "intelligence.cross_reference_narrative", {}).payload()
        assert body["narrative_source"] == "fallback"

    def test_no_recent_checks_is_unavailable(self, fake_store, monkeypatch):
        import intelligence.cross_reference as xref

        monkeypatch.setattr(xref, "load_recent_report", lambda *_a, **_k: None)
        [out] = ap.build_cross_reference_narrative(ap.BuildContext(engine=ENGINE, now=NOW))
        assert out.payload is None and "no cross-reference checks" in out.unavailable_reason

    def test_generate_narrative_with_source_reports_fallback(self, monkeypatch):
        import intelligence.cross_reference as xref

        monkeypatch.setitem(sys.modules, "llm.router", types.SimpleNamespace(
            get_llm=lambda _t: None, Tier=types.SimpleNamespace(REASON="reason")))
        text, source = xref.generate_narrative_with_source([_check("a")], [])
        assert source == "fallback" and "Cross-reference engine ran 1 checks" in text
        assert xref._generate_narrative([_check("a")], []) == text


class TestAnomalyAlertOnChange:
    def test_alert_only_when_extreme_set_changes(self, fake_store, monkeypatch):
        from api.routers import associations

        sent: list[list[str]] = []
        monkeypatch.setattr(associations, "send_extreme_anomaly_alert",
                            lambda ex: sent.append([a["feature"] for a in ex]))
        ctx = ap.BuildContext(engine=ENGINE, now=NOW)
        scan = {"anomalies": [{"feature": "vix", "zscore": 4.0}, {"feature": "dxy", "zscore": 2.6}]}
        ap._alert_on_new_extremes(ctx, scan)
        assert sent == [["vix"]]
        fake_store.put("associations.anomalies", {"sigma_threshold": 2.5, "days": 252}, scan)
        ap._alert_on_new_extremes(ctx, scan)
        assert sent == [["vix"]]  # unchanged set: no repeat e-mail
        ap._alert_on_new_extremes(ctx, {"anomalies": [{"feature": "hy", "zscore": -3.9}]})
        assert sent == [["vix"], ["hy"]]


class TestAssociationsSharedRead:
    def _raw(self) -> pd.DataFrame:
        rng = np.random.default_rng(7)
        idx = pd.date_range(date.today() - timedelta(days=503), date.today(), freq="D")  # noqa: DTZ011
        data = {fid: rng.normal(size=len(idx)).cumsum() for fid in (1, 2, 3, 4)}
        data[4][:300] = np.nan  # sparse in the long window, dense in the short one
        return pd.DataFrame(data, index=idx)

    def test_window_from_raw_matches_a_bounded_read(self):
        from api.routers import associations

        raw = self._raw()

        class Pit:
            def get_feature_matrix(self, feature_ids, start_date, end_date, as_of_date, vintage_policy):
                return raw.loc[pd.to_datetime(raw.index) >= pd.Timestamp(start_date), feature_ids]

        for days in (252, 504):
            direct = associations._build_feature_matrix(Pit(), [1, 2, 3, 4], days)
            reused = associations._build_feature_matrix(None, [1, 2, 3, 4], days, raw=raw)
            pd.testing.assert_frame_equal(direct, reused)

    def test_vectorised_pairs_match_the_pairwise_loop(self, monkeypatch):
        from api.routers import associations

        raw = self._raw()
        registry = pd.DataFrame([(1, "treasury_2y", "rates", ""), (2, "treasury_10y", "rates", ""),
                                 (3, "vix", "vol", ""), (4, "hy_spread", "credit", "")],
                                columns=["id", "name", "family", "description"])
        monkeypatch.setattr(associations, "get_db_engine", lambda: None)
        monkeypatch.setattr(associations, "get_pit_store", lambda: None)
        body = associations._compute_correlation_matrix(504, 0.0, True, raw=raw, registry=registry)

        m = associations._build_feature_matrix(None, [1, 2, 3, 4], 504, raw=raw)
        m.columns = [dict(zip(registry["id"], registry["name"]))[c] for c in m.columns]
        corr = m.corr()
        expected = set()
        for i in range(len(m.columns)):
            for j in range(i + 1, len(m.columns)):
                c = corr.iloc[i, j]
                if not math.isnan(c):
                    expected.add((m.columns[i], m.columns[j], round(float(c), 4)))
        assert {(p["a"], p["b"], p["corr"]) for p in body["strong_pairs"]} == expected

    def test_vectorised_broken_correlations_match_series_corr(self, monkeypatch):
        from api.routers import associations

        idx = pd.date_range(date.today() - timedelta(days=99), date.today(), freq="D")  # noqa: DTZ011
        rng = np.random.default_rng(3)
        base = rng.normal(size=100).cumsum()
        a = base.copy()
        a[-1] += 60.0
        b = np.concatenate([base[:70], -base[70:]])  # correlation flips in the last 30 days
        c = rng.normal(size=100)
        raw = pd.DataFrame({1: a, 2: b, 3: c}, index=idx)
        registry = pd.DataFrame([(1, "a", "x", ""), (2, "b", "y", ""), (3, "c", "z", "")],
                                columns=["id", "name", "family", "description"])
        monkeypatch.setattr(associations, "get_db_engine", lambda: None)
        monkeypatch.setattr(associations, "get_pit_store", lambda: None)
        body = associations._compute_anomalies(2.5, 99, raw=raw, registry=registry, alert=False)
        [anom] = [x for x in body["anomalies"] if x["feature"] == "a"]

        m = raw.rename(columns={1: "a", 2: "b", 3: "c"})
        hist, recent = m.head(70), m.tail(30)
        expected = []
        for other in ("b", "c"):
            h, r = float(hist["a"].corr(hist[other])), float(recent["a"].corr(recent[other]))
            if abs(h - r) > 0.5:
                expected.append((other, round(h, 3), round(r, 3)))
        got = [(x["partner"], x["historical_corr"], x["recent_corr"]) for x in anom["broken_correlations"]]
        assert sorted(got) == sorted(expected) and got


class TestSmartHeatmap:
    def test_family_variants_slice_one_matrix_and_null_missing_z(self):
        from discovery.smart_heatmap import heatmap_payload

        idx = pd.date_range("2026-10-01", periods=30, freq="D")
        full = pd.DataFrame({1: np.arange(30.0), 2: np.arange(30.0) ** 1.5, 3: np.sin(np.arange(30.0))},
                            index=idx)
        full.loc[idx[-1], 2] = np.nan  # feature 2 missing on the last row
        names = {1: "a", 2: "b", 3: "c"}
        fams = {1: "rates", 2: "rates", 3: "vol"}
        all_body = heatmap_payload(full, [1, 2, 3], names, fams, family=None, orthogonal_only=True,
                                   redundant_pairs=[], total_count=3, all_families=["rates", "vol"])
        assert all_body["features"] == ["a", "b", "c"] and len(all_body["matrix"]) == 3
        z = {r["feature"]: r["zscore"] for r in all_body["z_scores"]}
        assert z["b"] is None and z["a"] is not None  # missing is null, not 0
        assert all_body["as_of_date"] == "2026-10-30"

        vol = heatmap_payload(full, [1, 2, 3], names, fams, family="vol", orthogonal_only=True,
                              redundant_pairs=[], total_count=3, all_families=["rates", "vol"])
        assert vol["features"] == ["c"] and vol["filtered_count"] == 1 and vol["family_filter"] == "vol"

    def test_builder_reads_the_matrix_once_for_audit_and_heatmap(self, monkeypatch):
        import discovery.smart_heatmap as sh
        from discovery import orthogonality

        reads: list[tuple] = []
        idx = pd.date_range("2026-10-01", periods=40, freq="D")
        full = pd.DataFrame({1: np.arange(40.0), 2: np.cos(np.arange(40.0)), 3: np.arange(40.0) * 2},
                            index=idx)

        class Pit:
            def get_feature_matrix(self, **kw):
                reads.append(tuple(sorted(kw)))
                return full

        monkeypatch.setattr(sh, "_registry", lambda _e: [(1, "a", "rates"), (2, "b", "vol"), (3, "c", "rates")])
        seen: dict[str, Any] = {}

        def ortho(self, corr_threshold=0.8, matrix=None):
            seen["matrix"] = matrix
            return {"orthogonal_ids": [1, 2], "redundant_pairs": [{"a": "a", "b": "c", "correlation": 1.0}]}

        monkeypatch.setattr(orthogonality.OrthogonalityAudit, "__init__", lambda self, **kw: None)
        monkeypatch.setattr(orthogonality.OrthogonalityAudit, "get_orthogonal_features", ortho)
        out = sh.build_smart_heatmap_payloads(MagicMock(), Pit(), today=date(2026, 11, 9))
        assert len(reads) == 1 and seen["matrix"] is full
        assert [p["family"] for p, _b, _d in out] == [None, "rates", "vol"]
        assert out[0][1]["features"] == ["a", "b"]


class TestInfluenceLegislationReadOnce:
    def test_hypocrisy_reads_legislation_once(self, monkeypatch):
        import intelligence.influence_network as inf

        trades = [("Rep A", "NVDA", date(2026, 10, 1), "BUY", "{}"),
                  ("Rep A", "INTC", date(2026, 10, 2), "SELL", "{}"),
                  ("Rep B", "NVDA", date(2026, 10, 3), "BUY", "{}")]
        bill = {"bill_id": "HR1", "title": "Chips", "affected_tickers": ["NVDA", "INTC"],
                "votes": [{"member": "Rep A", "vote": "NO"}, {"member": "Rep B", "vote": "YEA"}]}
        executed: list[str] = []

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = str(stmt)
                executed.append(sql)
                res = MagicMock()
                if "LEGISLATION" in str(params):
                    res.fetchall.return_value = [("LEGISLATION:HR1", date(2026, 9, 1), json.dumps(bill))]
                else:
                    res.fetchall.return_value = trades
                return res

        engine = MagicMock()
        engine.connect.return_value = Conn()
        flags = inf.vote_trade_hypocrisy(engine)
        legislation_reads = [s for s in executed if "raw_series" in s]
        assert len(legislation_reads) == 1
        assert {(f["member"], f["ticker"], f["hypocrisy_type"]) for f in flags} == {
            ("Rep A", "NVDA", "VOTED_NO_BUT_BOUGHT")}


class TestGlobe:
    def test_globe_uses_stored_checks_and_bounded_trade_reads(self, monkeypatch):
        import intelligence.cross_reference as xref
        from api.routers import intelligence_risk as ir

        statements: list[str] = []

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                statements.append(str(stmt))
                res = MagicMock()
                res.fetchall.return_value = []
                if "DISTINCT ON" in str(stmt) and params and params.get("d") == date.today():  # noqa: DTZ011
                    res.fetchall.return_value = [("INDPRO", 101.0, date(2026, 10, 1))]
                return res

        engine = MagicMock()
        engine.connect.return_value = Conn()
        monkeypatch.setattr(ir, "get_db_engine", lambda: engine)
        monkeypatch.setattr(xref, "load_recent_report", lambda *_a, **_k: MagicMock(red_flags=[]))
        monkeypatch.setattr(xref, "run_all_checks", lambda *_a, **_k: pytest.fail("re-ran the checks"))
        body = ir._build_globe_data()
        assert body["as_of"] == "2026-10-01"
        assert not any("LIKE" in s and "series_id = ANY" not in s and "source_id" not in s
                       for s in statements), statements
