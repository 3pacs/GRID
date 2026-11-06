"""S10 — exploratory oracle recalibration (oracle/calibration.py).

Covers the expiry-clustered block bootstrap, the time-block split and its
embargo, the isotonic recalibrator, the held-out evaluation, causality of the
ACI loop, and the daily job end to end on SQLite: point-in-time reads, and
writes confined to ``oracle_calibration_exploratory``.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest
from sqlalchemy import create_engine, event, text

from oracle import calibration as cal
from oracle.calibration import (
    EXPLORATORY_METHOD_VERSION,
    EXPLORATORY_TABLE,
    Recalibrator,
    ScoredOutcome,
    block_bootstrap_weights,
    causal_aci_intervals,
    cluster_bootstrap_mean,
    evaluate_holdout,
    exploratory_conformal_interval,
    fit_recalibrator,
    load_latest_exploratory_state,
    load_scored_outcomes,
    model_family,
    reliability_with_bands,
    run_exploratory_recalibration,
    time_blocks,
)

D0 = date(2026, 4, 1)


def _rows(
    n_dates: int = 60,
    per_date: int = 60,
    *,
    seed: int = 0,
    stated=lambda t: t,
    horizon_days: int = 7,
) -> list[ScoredOutcome]:
    """Synthetic scored calls: true P(right) = t; stated confidence = stated(t)."""
    rng = np.random.default_rng(seed)
    out = []
    for d in range(n_dates):
        expiry = D0 + timedelta(days=3 * d)
        for k in range(per_date):
            t = float(rng.uniform(0.2, 0.9))
            y = int(rng.uniform() < t)
            out.append(ScoredOutcome(
                prediction_id=f"p{d:03d}-{k:03d}",
                model_name="options_flow_mut_abc" if k % 2 else "timeseries_enhanced",
                family="options_flow" if k % 2 else "timeseries_enhanced",
                horizon="7d",
                confidence=float(np.clip(stated(t), 0, 1)),
                y=y,
                hit=int(y and rng.uniform() < 0.5),
                expiry=expiry,
                created_date=expiry - timedelta(days=horizon_days),
                source="rescore_hindsight",
            ))
    return out


class TestModelFamily:
    @pytest.mark.parametrize("name,fam", [
        ("options_flow_mut_7g7lxd", "options_flow"),
        ("options_flow_mut_abcdef_mut_xyz", "options_flow"),
        ("hyp_hyp_a1ba89b0", "hypothesis"),
        ("cross_options_fl_contagion_1wbi", "crossover"),
        ("timeseries_enhanced", "timeseries_enhanced"),
        ("", "unknown"),
        (None, "unknown"),
    ])
    def test_family(self, name, fam):
        assert model_family(name) == fam


class TestBlockBootstrap:
    def test_weights_sum_to_cluster_count(self):
        W = block_bootstrap_weights(37, 200, np.random.default_rng(1), block_len=3)
        assert W.shape == (200, 37)
        assert np.all(W.sum(1) == 37)

    def test_point_metrics_match_direct_formulas(self):
        rng = np.random.default_rng(3)
        p = rng.uniform(size=500)
        y = (rng.uniform(size=500) < p).astype(int)
        clusters = rng.integers(0, 25, size=500)
        rel = reliability_with_bands(p, y, clusters, n_boot=200)
        assert rel["brier"] == pytest.approx(np.mean((p - y) ** 2), abs=1e-4)
        edges = np.linspace(0, 1, 11)
        b = np.clip(np.searchsorted(edges, p, side="right") - 1, 0, 9)
        ece = sum(abs(p[b == i].sum() - y[b == i].sum()) for i in range(10)) / 500
        assert rel["ece"] == pytest.approx(ece, abs=1e-4)
        assert rel["brier_lo"] <= rel["brier"] <= rel["brier_hi"]
        assert rel["ece_lo"] <= rel["ece_hi"]
        assert sum(x["count"] for x in rel["bins"]) == 500
        json.dumps(rel)  # NaN-free, JSON-ready

    def test_clustered_outcomes_shrink_effective_n(self):
        rng = np.random.default_rng(5)
        K, m = 80, 25
        clusters = np.repeat(np.arange(K), m)
        p = np.full(K * m, 0.5)
        y_indep = (rng.uniform(size=K * m) < 0.5).astype(int)
        y_clustered = np.repeat((rng.uniform(size=K) < 0.5).astype(int), m)
        n_indep = reliability_with_bands(p, y_indep, clusters, block_len=1)["n_eff"]
        n_clust = reliability_with_bands(p, y_clustered, clusters, block_len=1)["n_eff"]
        assert n_clust < 3 * K < n_indep
        assert n_indep > 0.5 * K * m

    def test_single_cluster_has_no_band(self):
        rel = reliability_with_bands([0.6, 0.7], [1, 0], ["d", "d"])
        assert rel["brier_lo"] is None and rel["n_eff"] is None and rel["n_clusters"] == 1

    def test_cluster_mean(self):
        out = cluster_bootstrap_mean([1, 0, 1, 1], ["a", "a", "b", "c"], n_boot=100)
        assert out["mean"] == 0.75 and out["n_clusters"] == 3
        assert out["lo"] <= out["mean"] <= out["hi"]


class TestTimeBlocks:
    def test_embargo_and_order(self):
        rows = _rows(n_dates=40, per_date=10, horizon_days=10)
        blocks = time_blocks(rows)
        t1, t2 = blocks["fit_end"], blocks["conformal_end"]
        assert all(r.expiry <= t1 for r in blocks["fit"])
        assert all(t1 < r.expiry <= t2 and r.created_date > t1 for r in blocks["conformal"])
        assert all(r.expiry > t2 and r.created_date > t2 for r in blocks["test"])
        kept = len(blocks["fit"]) + len(blocks["conformal"]) + len(blocks["test"])
        assert kept + blocks["dropped_embargo"] == len(rows)
        assert blocks["dropped_embargo"] > 0

    def test_too_few_dates(self):
        assert time_blocks(_rows(n_dates=2, per_date=5))["test"] == []


class TestRecalibrator:
    def test_monotone_and_roundtrip(self):
        rows = _rows(n_dates=20, per_date=50, stated=lambda t: t ** 2)
        recal = fit_recalibrator(rows, min_family_rows=300)
        assert set(recal.family_knots) == {"options_flow", "timeseries_enhanced"}
        grid = np.linspace(0, 1, 21)
        preds = [recal.predict(x, "options_flow") for x in grid]
        assert all(b >= a - 1e-12 for a, b in zip(preds, preds[1:]))
        again = Recalibrator.from_dict(json.loads(json.dumps(recal.to_dict())))
        assert [again.predict(x, "options_flow") for x in grid] == preds
        # Unknown family falls back to the global map.
        assert again.predict(0.5, "nope") == recal.predict(0.5, None)


class TestEvaluateHoldout:
    def test_miscalibrated_confidence_is_fixed_and_coverage_is_reported(self):
        rows = _rows(stated=lambda t: t ** 2, seed=11)
        report, state = evaluate_holdout(rows)
        assert report["status"] == "ok"
        assert report["after"]["ece"] < report["before"]["ece"]
        assert report["after"]["brier"] < report["before"]["brier"]
        for name in ("split", "aci"):
            block = report[name]
            assert block["nominal_coverage"] == 0.9
            assert 0.8 <= block["coverage"]["mean"] <= 1.0
            assert 0.0 <= block["abstention_rate"]["mean"] <= 1.0
            assert sum(s["n"] for s in block["stances"].values()) == report["blocks"]["test_rows"]
        # Both hit-rate views are always present: all test rows, and per stance.
        assert report["hit_rate_all_test_rows"]["direction_correct"]["n"] == report["blocks"]["test_rows"]
        assert "strict_hit" in report["hit_rate_all_test_rows"]
        assert report["gamma"] == 0.005 and "not tuned" in report["gamma_rule"]
        assert state["official"] is False and state["method_version"] == EXPLORATORY_METHOD_VERSION
        json.dumps(cal._json_safe(report))

    def test_insufficient(self):
        report, state = evaluate_holdout(_rows(n_dates=5, per_date=4))
        assert report["status"] == "insufficient_data" and state is None


def test_aci_is_causal():
    """Flipping one outcome moves no interval served before that outcome existed."""
    rows = _rows(n_dates=30, per_date=20, seed=4)
    test = [r for r in rows if r.expiry >= D0 + timedelta(days=30)]
    p_hat = {r.prediction_id: r.confidence for r in test}
    scores = sorted(abs(r.y - r.confidence) for r in rows if r.expiry < D0 + timedelta(days=30))
    base, _, _ = causal_aci_intervals(test, p_hat, scores, alpha=0.1, gamma=0.05)
    target = test[len(test) // 2]
    flipped = [
        ScoredOutcome(**{**r.__dict__, "y": 1 - r.y}) if r.prediction_id == target.prediction_id else r
        for r in test
    ]
    after, _, _ = causal_aci_intervals(flipped, p_hat, scores, alpha=0.1, gamma=0.05)
    for r in test:
        if r.created_date <= target.expiry:
            assert after[r.prediction_id] == base[r.prediction_id], r.prediction_id
    assert any(after[r.prediction_id] != base[r.prediction_id]
               for r in test if r.created_date > target.expiry)


class TestExposure:
    def test_interval_from_state(self):
        state = {
            "alpha": 0.1,
            "recalibrator": {"global": {"x": [0.0, 1.0], "y": [0.0, 1.0]}, "family": {}},
            "split_q": 0.2, "split_n_cal": 500,
            "aci_q": None, "aci_alpha_t": -0.3, "aci_n_cal": 900,
        }
        take = exploratory_conformal_interval(state, 0.9, "x", method="split")
        assert take.stance == "take" and take.method == "split"
        full = exploratory_conformal_interval(state, 0.9, "x")
        assert (full.lower, full.upper) == (0.0, 1.0) and full.abstain
        assert full.alpha_used == -0.3 and full.official is False


# ── the daily job on SQLite ────────────────────────────────────────────────

_DDL = [
    """CREATE TABLE oracle_predictions (
        id TEXT PRIMARY KEY, created_at TIMESTAMP, ticker TEXT, direction TEXT,
        expiry DATE, confidence REAL, model_name TEXT, verdict TEXT,
        scored_at TIMESTAMP, dedup_keep BOOLEAN, horizon_days INTEGER)""",
    """CREATE TABLE oracle_prediction_rescores (
        prediction_id TEXT, scoring_version TEXT, score_class TEXT,
        official BOOLEAN, verdict TEXT, rescored_at TIMESTAMP,
        PRIMARY KEY (prediction_id, scoring_version))""",
    """CREATE TABLE oracle_models (name TEXT PRIMARY KEY, running_brier REAL)""",
    """CREATE TABLE oracle_calibration_history (id INTEGER PRIMARY KEY, model_name TEXT)""",
]
_EXPLORATORY_DDL = """CREATE TABLE oracle_calibration_exploratory (
    id INTEGER PRIMARY KEY AUTOINCREMENT, as_of DATE, method_version TEXT, scope TEXT,
    group_key TEXT, target TEXT, n_rows INTEGER, n_clusters INTEGER, n_eff REAL,
    base_rate REAL, brier REAL, brier_lo REAL, brier_hi REAL, ece REAL, ece_lo REAL,
    ece_hi REAL, payload TEXT, score_class TEXT, official BOOLEAN,
    UNIQUE (as_of, method_version, scope, group_key))"""

AS_OF = datetime(2026, 11, 6, 2, 35, tzinfo=timezone.utc)


def _seed(engine, *, with_exploratory: bool = True) -> None:
    rng = np.random.default_rng(9)
    with engine.begin() as conn:
        for ddl in _DDL + ([_EXPLORATORY_DDL] if with_exploratory else []):
            conn.execute(text(ddl))
        conn.execute(text("INSERT INTO oracle_models VALUES ('m', 0.25)"))
        for d in range(40):
            expiry = date(2026, 5, 1) + timedelta(days=4 * d)
            for k in range(30):
                pid = f"r{d}-{k}"
                t = float(rng.uniform(0.3, 0.8))
                verdict = "miss" if rng.uniform() > t else ("hit" if rng.uniform() < 0.5 else "partial")
                official = k < 10
                conn.execute(text(
                    "INSERT INTO oracle_predictions VALUES (:id, :c, 'AAA', 'CALL', :e, :conf, :m, "
                    ":v, :s, 1, NULL)"
                ), {"id": pid, "c": datetime.combine(expiry - timedelta(days=7), datetime.min.time()),
                    "e": expiry, "conf": t * t, "m": "hyp_x" if k % 3 else "options_flow",
                    "v": verdict if official else "no_data",
                    "s": datetime.combine(expiry, datetime.min.time()) + timedelta(days=1)})
                if not official:
                    conn.execute(text(
                        "INSERT INTO oracle_prediction_rescores VALUES "
                        "(:id, 'oracle_rescore_hindsight_v1', 'exploratory_hindsight', 0, :v, :rs)"
                    ), {"id": pid, "v": verdict, "rs": datetime(2026, 11, 6, 1, 49)})
        # Excluded: future expiry, rescored after as_of, dedup dropped, astrogrid,
        # no confidence, other rescore version.
        conn.execute(text(
            "INSERT INTO oracle_predictions VALUES ('future', '2026-11-01', 'A', 'CALL', '2026-11-20', "
            "0.5, 'm', 'hit', '2026-11-01', 1, NULL)"))
        conn.execute(text(
            "INSERT INTO oracle_predictions VALUES ('dropped', '2026-06-01', 'A', 'CALL', '2026-06-10', "
            "0.5, 'm', 'hit', '2026-06-11', 0, NULL)"))
        conn.execute(text(
            "INSERT INTO oracle_predictions VALUES ('astrogrid:1', '2026-06-01', 'A', 'CALL', '2026-06-10', "
            "0.5, 'm', 'hit', '2026-06-11', 1, NULL)"))
        conn.execute(text(
            "INSERT INTO oracle_predictions VALUES ('noconf', '2026-06-01', 'A', 'CALL', '2026-06-10', "
            "NULL, 'm', 'hit', '2026-06-11', 1, NULL)"))
        conn.execute(text(
            "INSERT INTO oracle_predictions VALUES ('late', '2026-06-01', 'A', 'CALL', '2026-06-10', "
            "0.5, 'm', 'no_data', NULL, 1, NULL)"))
        conn.execute(text(
            "INSERT INTO oracle_prediction_rescores VALUES "
            "('late', 'oracle_rescore_hindsight_v1', 'exploratory_hindsight', 0, 'hit', '2026-11-07')"))
        conn.execute(text(
            "INSERT INTO oracle_prediction_rescores VALUES "
            "('r0-15', 'some_future_version', 'exploratory_hindsight', 0, 'hit', '2026-11-01')"))


@pytest.fixture()
def sqlite_engine():
    engine = create_engine("sqlite://")
    yield engine
    engine.dispose()


def test_loader_is_point_in_time(sqlite_engine):
    _seed(sqlite_engine)
    rows = load_scored_outcomes(sqlite_engine, AS_OF)
    ids = {r.prediction_id for r in rows}
    assert len(rows) == 40 * 30
    assert not ids & {"future", "dropped", "astrogrid:1", "noconf", "late"}
    assert {r.source for r in rows} == {"official_forward", "rescore_hindsight"}
    assert {r.family for r in rows} == {"hypothesis", "options_flow"}
    assert all(r.horizon == "7d" for r in rows)
    earlier = load_scored_outcomes(sqlite_engine, datetime(2026, 8, 1, tzinfo=timezone.utc))
    assert earlier and len(earlier) < len(rows)
    assert all(r.source == "official_forward" and r.expiry <= date(2026, 8, 1) for r in earlier)


def test_daily_job_writes_only_the_exploratory_table(sqlite_engine):
    _seed(sqlite_engine)
    statements: list[str] = []
    event.listen(sqlite_engine, "before_cursor_execute",
                 lambda conn, cur, stmt, params, ctx, many: statements.append(stmt))
    summary = run_exploratory_recalibration(sqlite_engine, as_of=AS_OF)
    assert summary["status"] == "ok" and summary["official"] is False
    assert summary["inserted"] == summary["groups"] + 1
    writes = [s for s in statements
              if s.lstrip().split(None, 1)[0].upper() in {"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"}]
    assert writes and all(EXPLORATORY_TABLE in s for s in writes)
    assert all("INSERT INTO oracle_calibration_exploratory" in s for s in writes)
    with sqlite_engine.connect() as conn:
        scopes = dict(conn.execute(text(
            "SELECT scope, count(*) FROM oracle_calibration_exploratory GROUP BY scope")).fetchall())
        assert scopes["run"] == 1 and scopes["global"] == 1 and scopes["family"] == 2
        assert conn.execute(text(
            "SELECT count(*) FROM oracle_calibration_exploratory "
            "WHERE official != 0 OR score_class != 'exploratory'")).scalar() == 0
        assert conn.execute(text("SELECT running_brier FROM oracle_models")).scalar() == 0.25
    # Same UTC day again: nothing new.
    assert run_exploratory_recalibration(sqlite_engine, as_of=AS_OF)["inserted"] == 0


def test_daily_job_without_table_writes_nothing(sqlite_engine):
    _seed(sqlite_engine, with_exploratory=False)
    summary = run_exploratory_recalibration(sqlite_engine, as_of=AS_OF)
    assert summary["status"] == "ok" and summary["inserted"] == 0


def test_dry_run_does_not_persist(sqlite_engine):
    _seed(sqlite_engine)
    assert run_exploratory_recalibration(sqlite_engine, as_of=AS_OF, persist=False)["inserted"] == 0
    with sqlite_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM oracle_calibration_exploratory")).scalar() == 0


def test_load_latest_state(sqlite_engine):
    assert load_latest_exploratory_state(sqlite_engine) is None
    with sqlite_engine.begin() as conn:
        conn.execute(text(_EXPLORATORY_DDL))
        for as_of, q in (("2026-11-05", 0.1), ("2026-11-06", 0.3)):
            conn.execute(text(
                "INSERT INTO oracle_calibration_exploratory (as_of, method_version, scope, group_key, "
                "target, payload, score_class, official) VALUES (:a, :v, 'run', 'all', 't', :p, "
                "'exploratory', 0)"
            ), {"a": as_of, "v": EXPLORATORY_METHOD_VERSION, "p": json.dumps({"state": {"split_q": q}})})
    assert load_latest_exploratory_state(sqlite_engine) == {"split_q": 0.3}


def test_rescore_version_matches_the_rescorer():
    import scripts.score_oracle_trades as sot

    assert cal.RESCORE_SCORING_VERSION == sot.RESCORE_SCORING_VERSION


def test_job_is_scheduled_after_live_snapshot():
    src = (Path(__file__).resolve().parents[1] / "intelligence/scheduler.py").read_text()
    assert "from oracle.calibration import run_exploratory_recalibration" in src
    assert '_sched.every().day.at("02:35").do(_oracle_recalibration_exploratory_daily)' in src


def test_nan_safe_json():
    assert cal._json_safe({"a": math.nan, "b": [np.float64(math.inf), 1.5], "c": date(2026, 1, 2)}) == {
        "a": None, "b": [None, 1.5], "c": "2026-01-02",
    }
