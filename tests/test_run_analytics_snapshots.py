"""Unit tests for the analytics snapshots job and the fixes it relies on.

No PostgreSQL: engines and PIT stores are mocked. The SQL itself is covered
by tests/test_analytics_snapshots_pg.py.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from discovery.matrix_guard import drop_stale_columns, matrix_window
from scripts import run_analytics_snapshots as job

AS_OF = date(2026, 9, 27)


def _matrix(end: date, n: int = 120, cols=(1, 2, 3), seed: int = 0) -> pd.DataFrame:
    idx = pd.date_range(end=pd.Timestamp(end), periods=n, freq="B")
    rng = np.random.default_rng(seed)
    return pd.DataFrame(rng.standard_normal((n, len(cols))), index=idx, columns=list(cols))


# ---------------------------------------------------------------------------
# matrix_guard
# ---------------------------------------------------------------------------

def test_drop_stale_columns_removes_dead_series_and_reports_last_date():
    m = _matrix(AS_OF - timedelta(days=1))
    dead_last = m.index[-60]
    m.loc[m.index > dead_last, 3] = np.nan          # feature 3 died 60 rows ago
    m[4] = np.nan                                    # feature 4 never observed
    kept, excluded = drop_stale_columns(m, AS_OF, 10)
    assert list(kept.columns) == [1, 2]
    assert excluded == {3: dead_last.date().isoformat(), 4: None}
    # Without the guard the dead column would truncate the matrix end.
    assert m.ffill(limit=5).dropna(subset=[1, 2, 3]).index.max() < m.index.max()
    assert matrix_window(kept.dropna())["matrix_end"] == m.index.max().date().isoformat()


def test_drop_stale_columns_disabled_is_identity():
    m = _matrix(date(2020, 1, 1))
    kept, excluded = drop_stale_columns(m, AS_OF, None)
    assert kept is m and excluded == {}


# ---------------------------------------------------------------------------
# Orthogonality / clustering honour the guard and carry provenance
# ---------------------------------------------------------------------------

def _wire_engine(engine: MagicMock, ids: list[int]) -> None:
    conn = MagicMock()

    def _exec(stmt, params=None):
        sql = str(stmt)
        res = MagicMock()
        if "ANY(:ids)" in sql:
            res.fetchall.return_value = [(i, f"feat_{i}") for i in ids]
        else:
            res.fetchall.return_value = [(i,) for i in ids]
        return res

    conn.execute.side_effect = _exec
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)


def test_orthogonality_excludes_stale_feature_and_reports_window(tmp_path, monkeypatch):
    import matplotlib.figure

    from discovery.orthogonality import OrthogonalityAudit

    # Rendering PNGs is not under test and dominates the runtime.
    monkeypatch.setattr(OrthogonalityAudit, "_save_heatmap", lambda self, *a, **k: None)
    monkeypatch.setattr(matplotlib.figure.Figure, "savefig", lambda self, *a, **k: None)
    monkeypatch.setattr("discovery.orthogonality.sns.heatmap", lambda *a, **k: None)

    m = _matrix(AS_OF - timedelta(days=1), n=120, cols=(1, 2, 3, 4))
    m.loc[m.index > m.index[-60], 4] = np.nan        # stale ~3 months
    engine, pit = MagicMock(), MagicMock()
    _wire_engine(engine, [1, 2, 3, 4])
    pit.get_feature_matrix.return_value = m

    audit = OrthogonalityAudit(db_engine=engine, pit_store=pit)
    out = audit.run_full_audit(
        as_of_date=AS_OF, output_dir=str(tmp_path), max_staleness_days=10,
        semantic_similarity=False, persist=False,
    )
    assert out["n_features_analyzed"] == 3
    assert list(out["excluded_stale_features"]) == ["feat_4"]
    # The stale column did not truncate the window to its last date.
    assert out["matrix_end"] == m.index.max().date().isoformat()
    assert out["as_of_date"] == AS_OF.isoformat()
    assert "snapshot_id" not in out                  # persist=False wrote nothing


# ---------------------------------------------------------------------------
# Orthogonality gets a bounded per-job statement_timeout (not the app-wide
# default from db.py), scoped to just this step's own connections.
# ---------------------------------------------------------------------------

class _RecordingConn:
    """Connection stub that records executed SQL and returns no rows."""

    def __init__(self, log: list[str]) -> None:
        self._log = log

    def execute(self, stmt, params=None):
        self._log.append(str(getattr(stmt, "text", stmt)))
        res = MagicMock()
        res.fetchall.return_value = []
        return res

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _RecordingEngine:
    """Engine stub whose ``connect()`` yields a recording connection."""

    def __init__(self) -> None:
        self.sql_log: list[str] = []

    def connect(self):
        return _RecordingConn(self.sql_log)


def test_scoped_timeout_engine_lifts_statement_timeout_first():
    real = _RecordingEngine()
    scoped = job._ScopedTimeoutEngine(real, "600s")

    with scoped.connect() as conn:
        conn.execute("SELECT 1")

    assert real.sql_log == ["SET LOCAL statement_timeout = '600s'", "SELECT 1"]


def test_scoped_timeout_engine_uses_set_local_not_global():
    real = _RecordingEngine()
    scoped = job._ScopedTimeoutEngine(real, "600s")
    with scoped.connect():
        pass
    assert real.sql_log == ["SET LOCAL statement_timeout = '600s'"]
    assert real.sql_log[0].strip().startswith("SET LOCAL "), (
        "must be transaction-scoped SET LOCAL, never a bare global SET "
        "that would change the default for every other caller on the conn"
    )


def test_scoped_timeout_engine_passes_through_other_attributes():
    real = MagicMock()
    real.dialect = "postgresql"
    scoped = job._ScopedTimeoutEngine(real, "600s")
    assert scoped.dialect == "postgresql"


def test_orthogonality_step_uses_a_dedicated_scoped_engine(monkeypatch):
    """Job.orthogonality() must not hand its own self.engine / self.pit
    (shared with every other step) to OrthogonalityAudit -- it needs a
    private _ScopedTimeoutEngine + PITStore pair so the timeout bump can
    never leak into feature_engineering / clustering / etc."""
    from store.pit import PITStore

    captured: dict = {}

    class _FakeAudit:
        def __init__(self, db_engine, pit_store):
            captured["db_engine"] = db_engine
            captured["pit_store"] = pit_store

        def run_full_audit(self, **kwargs):
            return {"error": "stubbed, not under test"}

    monkeypatch.setattr("discovery.orthogonality.OrthogonalityAudit", _FakeAudit)

    shared_engine = MagicMock(name="shared_engine")
    j = job.Job(shared_engine, AS_OF, dry_run=True, gate={"ok": None, "run_tag": None})
    out = j.orthogonality()

    assert out["status"] == "skipped"
    scoped_engine = captured["db_engine"]
    assert isinstance(scoped_engine, job._ScopedTimeoutEngine)
    assert scoped_engine._engine is shared_engine
    assert scoped_engine._timeout == job.ORTHOGONALITY_STATEMENT_TIMEOUT
    # A fresh PITStore bound to the scoped engine, not the job's shared self.pit.
    assert isinstance(captured["pit_store"], PITStore)
    assert captured["pit_store"] is not j.pit
    assert captured["pit_store"].engine is scoped_engine


def _fast_clustering(monkeypatch):
    """Stub the expensive k-sweep and plot; the plumbing is under test here."""
    from discovery.clustering import ClusterDiscovery

    def _eval(self, feats, k, dates):
        return {"k": k, "kmeans_silhouette": 1.0 / k, "gmm_persistence": 3.0}

    monkeypatch.setattr(ClusterDiscovery, "_evaluate_k", _eval)
    monkeypatch.setattr(ClusterDiscovery, "_save_summary_plot", lambda self, df, fp: None)


def test_clustering_partition_labels_and_no_side_effects(tmp_path, monkeypatch):
    from discovery.clustering import ClusterDiscovery

    _fast_clustering(monkeypatch)
    m = _matrix(AS_OF - timedelta(days=1), n=80, cols=(11, 12, 13))
    engine, pit = MagicMock(), MagicMock()
    _wire_engine(engine, [11, 12, 13])
    pit.get_feature_matrix.return_value = m

    saved = []
    monkeypatch.setattr(
        "store.snapshots.AnalyticalSnapshotStore.save_snapshot",
        lambda self, **kw: saved.append(kw) or 1,
    )
    cd = ClusterDiscovery(db_engine=engine, pit_store=pit)
    out = cd.run_cluster_discovery(
        n_components=2, as_of_date=AS_OF, output_dir=str(tmp_path),
        feature_ids=[11, 12, 13], max_staleness_days=10, interpret=False, persist=False,
        partition={"scope": "sector", "sector": "Technology"},
    )
    assert "error" not in out
    assert out["cluster_labels"] == [f"CLUSTER_{i}" for i in range(out["best_k"])]
    assert out["partition"]["sector"] == "Technology"
    assert out["feature_ids"] == [11, 12, 13]
    assert saved == []
    # The restriction reaches the eligibility query as a bound parameter.
    conn = engine.connect.return_value.__enter__.return_value
    params = [c.args[1] for c in conn.execute.call_args_list if len(c.args) > 1 and c.args[1]]
    assert any(p.get("restrict") == [11, 12, 13] for p in params)


def test_clustering_persist_uses_sector_category(tmp_path, monkeypatch):
    from discovery.clustering import ClusterDiscovery

    _fast_clustering(monkeypatch)
    m = _matrix(AS_OF - timedelta(days=1), n=80, cols=(11, 12, 13))
    engine, pit = MagicMock(), MagicMock()
    _wire_engine(engine, [11, 12, 13])
    pit.get_feature_matrix.return_value = m
    saved = []
    monkeypatch.setattr("store.snapshots.ensure_analytical_snapshots_table", lambda e: None)
    monkeypatch.setattr(
        "store.snapshots.AnalyticalSnapshotStore.save_snapshot",
        lambda self, **kw: saved.append(kw) or 7,
    )
    cd = ClusterDiscovery(db_engine=engine, pit_store=pit)
    out = cd.run_cluster_discovery(
        n_components=2, as_of_date=AS_OF, output_dir=str(tmp_path),
        snapshot_category="clustering_sector", snapshot_subcategory="Energy",
        interpret=False,
    )
    assert out["snapshot_id"] == 7
    assert saved[0]["category"] == "clustering_sector"
    assert saved[0]["subcategory"] == "Energy"


# ---------------------------------------------------------------------------
# FeatureLab: stale inputs are missing, with provenance
# ---------------------------------------------------------------------------

def test_feature_lab_treats_stale_input_as_missing():
    from features.lab import FeatureLab

    engine, pit = MagicMock(), MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (42,)
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    old = [AS_OF - timedelta(days=200 + i) for i in range(100)]
    pit.get_pit.return_value = pd.DataFrame(
        {"feature_id": 42, "obs_date": old, "value": np.arange(100.0)}
    )
    lab = FeatureLab(engine, pit, max_input_age_days=10, input_age_overrides={"cpi_yoy": 75})
    assert lab._get_pit_series("hy_spread_proxy", AS_OF) is None
    prov = lab.input_provenance["hy_spread_proxy"]
    assert prov["status"] == "stale" and prov["max_age_days"] == 10
    assert prov["last_obs_date"] == (AS_OF - timedelta(days=200)).isoformat()

    fresh = [AS_OF - timedelta(days=i) for i in range(100)]
    pit.get_pit.return_value = pd.DataFrame(
        {"feature_id": 42, "obs_date": fresh, "value": np.arange(100.0)}
    )
    s = lab._get_pit_series("spy_pcr", AS_OF)
    assert s is not None and lab.input_provenance["spy_pcr"]["status"] == "ok"

    # Legacy behaviour (no limit) is unchanged.
    pit.get_pit.return_value = pd.DataFrame(
        {"feature_id": 42, "obs_date": old, "value": np.arange(100.0)}
    )
    assert FeatureLab(engine, pit)._get_pit_series("x", AS_OF) is not None


# ---------------------------------------------------------------------------
# Feature importance: no fabricated components
# ---------------------------------------------------------------------------

def test_importance_summary_uses_none_for_unmeasured_components():
    from features.importance import FeatureImportanceTracker

    t = FeatureImportanceTracker(db_engine=MagicMock(), pit_store=MagicMock())
    t._get_model_info = MagicMock(return_value={
        "id": 3, "name": "m", "layer": "x", "version": 1,
        "feature_set": [1, 2], "parameter_snapshot": {}, "hypothesis_id": None,
    })
    t._get_feature_names = MagicMock(return_value={1: "a", 2: "b"})
    t.compute_permutation_importance = MagicMock(return_value={"a": 1.0})
    t.compute_regime_correlation = MagicMock(return_value={})       # no journal
    t.compute_rolling_stability = MagicMock(return_value={"a": {"stability_score": 0.5}})

    rep = t.get_importance_report(3, as_of_date=AS_OF, persist=False)
    t.compute_permutation_importance.assert_called_once_with(3, AS_OF, persist=False)
    rows = {r["feature_name"]: r for r in rep["summary"]}
    assert rows["a"]["regime_correlation"] is None
    assert rows["a"]["regime_p_value"] is None
    assert rows["a"]["composite_score"] == pytest.approx((0.5 * 1.0 + 0.2 * 0.5) / 0.7)
    assert rows["b"]["composite_score"] is None and rows["b"]["components_measured"] == 0
    assert [r["feature_name"] for r in rep["summary"]] == ["a", "b"]


def test_permutation_importance_persist_false_skips_log():
    from features.importance import FeatureImportanceTracker

    t = FeatureImportanceTracker(db_engine=MagicMock(), pit_store=MagicMock())
    t._get_model_info = MagicMock(return_value={
        "id": 1, "name": "m", "layer": "x", "version": 1,
        "feature_set": [1, 2], "parameter_snapshot": {}, "hypothesis_id": None,
    })
    t._get_feature_names = MagicMock(return_value={1: "a", 2: "b"})
    t._build_feature_matrix = MagicMock(return_value=_matrix(AS_OF, n=60, cols=(1, 2)))
    t._persist_importance = MagicMock()
    out = t.compute_permutation_importance(1, AS_OF, n_repeats=2, persist=False)
    assert out
    t._persist_importance.assert_not_called()


# ---------------------------------------------------------------------------
# Regime state vector: partial vectors are not pinned in the cache
# ---------------------------------------------------------------------------

def test_state_vector_low_completeness_not_cached(monkeypatch):
    from intelligence.regime import state_vector as sv_mod

    low = sv_mod.StateVector(
        as_of_date=AS_OF, values=tuple([None] * len(sv_mod.DIM_NAMES)),
        completeness=0.0, stale_dimensions=(),
    )
    cached = []
    monkeypatch.setattr(sv_mod, "compute_state_vector", lambda e, a: low)
    monkeypatch.setattr(sv_mod, "cache_state_vector", lambda e, s: cached.append(s))
    out = sv_mod.get_or_compute_state_vector(MagicMock(), AS_OF, force_recompute=True)
    assert out is low and cached == []

    full = sv_mod.StateVector(
        as_of_date=AS_OF, values=tuple([0.1] * len(sv_mod.DIM_NAMES)),
        completeness=1.0, stale_dimensions=(),
    )
    monkeypatch.setattr(sv_mod, "compute_state_vector", lambda e, a: full)
    sv_mod.get_or_compute_state_vector(MagicMock(), AS_OF, force_recompute=True)
    assert cached == [full]


def test_state_vector_low_completeness_cached_row_is_recomputed(monkeypatch):
    from intelligence.regime import state_vector as sv_mod

    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (AS_OF, {}, 0.1, [])
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(sv_mod, "_ensure_cache_table", lambda e: None)
    fresh = sv_mod.StateVector(
        as_of_date=AS_OF, values=tuple([0.1] * len(sv_mod.DIM_NAMES)),
        completeness=1.0, stale_dimensions=(),
    )
    monkeypatch.setattr(sv_mod, "compute_state_vector", lambda e, a: fresh)
    monkeypatch.setattr(sv_mod, "cache_state_vector", lambda e, s: None)
    assert sv_mod.get_or_compute_state_vector(engine, AS_OF) is fresh


# ---------------------------------------------------------------------------
# Job orchestration
# ---------------------------------------------------------------------------

def test_run_refuses_without_gate_and_computes_nothing(monkeypatch):
    called = []
    monkeypatch.setattr(job, "Job", lambda *a, **k: called.append(a))
    out = job.run(MagicMock(), AS_OF, gate_fn=lambda e, a: {"ok": False, "checks": []})
    assert out["status"] == "not_ready" and out["steps"] == {} and called == []


def test_gate_bypass_only_with_dry_run():
    with pytest.raises(ValueError):
        job.run(MagicMock(), AS_OF, skip_gate=True, dry_run=False)
    with pytest.raises(SystemExit):
        job.main(["--no-readiness-gate"])


def test_run_isolates_step_failures(monkeypatch):
    class FakeJob:
        def __init__(self, *a):
            pass

        def orthogonality(self):
            raise RuntimeError("boom")

        def options_scan(self):
            return {"status": "ok"}

    monkeypatch.setattr(job, "Job", FakeJob)
    out = job.run(MagicMock(), AS_OF, steps=("orthogonality", "options_scan"),
                  gate_fn=lambda e, a: {"ok": True, "checks": []})
    assert out["steps"]["orthogonality"]["status"] == "failed"
    assert out["steps"]["options_scan"]["status"] == "ok"
    assert out["status"] == "failed"


def test_job_save_dry_run_writes_nothing_and_tags_provenance(monkeypatch):
    monkeypatch.setattr("store.snapshots.ensure_analytical_snapshots_table",
                        lambda e: pytest.fail("dry run must not create tables"))
    j = job.Job(MagicMock(), AS_OF, dry_run=True, gate={"ok": None, "run_tag": None})
    j.store.save_snapshot = MagicMock()
    assert j.save("orthogonality", {"x": 1}) is None
    j.store.save_snapshot.assert_not_called()
    assert j.base_provenance["job"] == "run_analytics_snapshots"
    assert j.base_provenance["as_of_date"] == AS_OF.isoformat()


def test_sector_candidate_names_are_exact_ticker_shapes():
    out = job.sector_candidate_names({
        "Financials": {"etf": "XLF", "subsectors": {
            "banks": {"actors": [{"ticker": "C"}, {"ticker": "BRK-B"}, {"name": "no ticker"}]},
        }},
        "junk": "not a dict",
    })
    assert set(out) == {"Financials"}
    names = set(out["Financials"]["names"])
    assert {"c_full", "brk_b_full", "xlf_iv_atm", "xlf_etf_close"} <= names
    assert all(n.split("_")[0] in {"c", "brk", "xlf"} for n in names)
    assert out["Financials"]["n_tickers"] == 3


def test_all_steps_are_job_methods():
    for step in job.ALL_STEPS:
        assert callable(getattr(job.Job, step))


# ---------------------------------------------------------------------------
# API: persisted results are served, cluster ids are not given regime names
# ---------------------------------------------------------------------------

def test_association_clusters_serve_snapshot_with_neutral_labels(monkeypatch):
    from api.routers import associations, discovery

    snap = {
        "result": {
            "best_k": 3,
            "all_metrics": [{"k": 3, "gmm_persistence": 4.5}],
            "transition_matrix": [[0.9, 0.1, 0.0], [0.1, 0.8, 0.1], [0.0, 0.2, 0.8]],
            "n_observations": 900, "variance_explained": 0.7,
            "pca_components_used": 3, "current_cluster": 1,
        },
        "source": "analytical_snapshots", "as_of_date": AS_OF.isoformat(),
        "created_at": None, "snapshot_id": 5,
    }
    monkeypatch.setattr(discovery, "_latest_job_result", lambda t: None)
    monkeypatch.setattr(discovery, "latest_snapshot_result", lambda c, s=None: snap)
    out = associations.get_clusters(_token="t")
    assert [c["label"] for c in out["clusters"]] == ["CLUSTER_0", "CLUSTER_1", "CLUSTER_2"]
    assert out["clusters"][0]["persistence"] == 4.5
    assert out["source"] == "analytical_snapshots" and out["as_of_date"] == AS_OF.isoformat()
    assert out["current_cluster"] == 1

    monkeypatch.setattr(discovery, "latest_snapshot_result", lambda c, s=None: None)
    empty = associations.get_clusters(_token="t")
    assert empty["n_clusters"] == 0 and empty["clusters"] == []


def test_discovery_results_prefer_in_process_job_then_snapshot(monkeypatch):
    from api.routers import discovery

    calls = []
    monkeypatch.setattr(discovery, "latest_snapshot_result",
                        lambda c, s=None: calls.append((c, s)) or {"result": {"k": 1},
                                                                   "source": "analytical_snapshots"})
    monkeypatch.setattr(discovery, "_latest_job_result", lambda t: None)
    assert discovery.get_clustering_results(sector=None, _token="t")["source"] == "analytical_snapshots"
    assert discovery.get_clustering_results(sector="Energy", _token="t")["result"] == {"k": 1}
    assert calls == [("clustering", None), ("clustering_sector", "Energy")]

    monkeypatch.setattr(discovery, "_latest_job_result", lambda t: {"best_k": 2})
    out = discovery.get_orthogonality_results(_token="t")
    assert out == {"result": {"best_k": 2}, "source": "in_process_job"}


def test_cross_asset_features_are_the_maintained_set():
    from api.routers.discovery import CROSS_ASSET_FEATURES

    names = [n for n, _ in CROSS_ASSET_FEATURES]
    legacy = {"spy_close", "qqq_close", "iwm_close", "treasury_10y", "treasury_2y",
              "yield_curve_10y2y", "gold_price", "crude_oil", "btc_price",
              "dollar_index", "vix", "hy_spread", "ig_spread"}
    assert not legacy & set(names)
    assert len(names) == len(set(names))
