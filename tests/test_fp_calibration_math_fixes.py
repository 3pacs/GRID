"""Regression tests for verified calibration, Brier weighting, and publisher-gate defects.

Written test-first (no database, no network) covering:
1. oracle.calibration.update_running_metrics: true 10-bin running ECE instead of MAE.
2. features.per_signal_brier / features.regime_conditional_brier / scripts.bootstrap_per_signal_brier:
   proper weighted mean Σ(w·se)/Σw, refusal to drift on incremental updates without Σw,
   and minimum effective sample floor (Σw >= 5) for conviction multiplier.
3. oracle.publisher_gate / oracle.firewall: honest zero-claims ("no_claims", score=None),
   flagged checked before all-insufficient, [UNVERIFIED] marking for all-insufficient /
   flagged-warn / avg confidence < 0.85 without blocking chat, and unchanged reject on
   contradicted claims.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from features.per_signal_brier import (
    compute_conviction_weight,
    get_full_scorecard_table,
    get_signal_scorecard,
    get_skipped_live_update_counts as get_flat_live_skip_counts,
    rank_signals_by_horizon,
    record_scored_prediction as record_flat_brier,
)
from features.regime_conditional_brier import (
    bootstrap_from_oracle_predictions,
    get_regime_conditional_scorecard,
    get_skipped_live_update_counts as get_regime_live_skip_counts,
    rank_signals_by_regime,
    record_scored_prediction as record_regime_brier,
)
from oracle.calibration import (
    compute_calibration,
    compute_per_horizon_calibration,
    detect_calibration_drift,
    snapshot_calibration_history,
    update_running_metrics,
)
from oracle.claim_extractor import Claim
from oracle.claim_verifier import VerifiedClaim
from oracle.firewall import _SAFE_FALLBACK, _audit_claims, verify_output
from oracle.publisher_gate import PublishDecision, gate_decision
from oracle.sanity_checker import CheckedClaim, SanityResult
from scripts import bootstrap_per_signal_brier as boot


# ── Helpers for Defect 1 (in-memory oracle_models store) ─────────────────


class _FakeCalibrationEngine:
    """Minimal in-memory SQLAlchemy engine for oracle_models + oracle_predictions + history."""

    def __init__(self) -> None:
        self.models: dict[str, dict] = {}
        self.predictions: list[tuple[float, str]] = []
        self.history: list[dict] = []

    def begin(self):
        return _FakeCalibrationEngine._Ctx(self)

    def connect(self):
        return _FakeCalibrationEngine._Ctx(self)

    class _Ctx:
        def __init__(self, parent: _FakeCalibrationEngine) -> None:
            self.parent = parent

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, params=None):
            sql = " ".join(str(query).split())
            params = params or {}
            res = MagicMock()

            if "FROM oracle_predictions" in sql:
                res.fetchall.return_value = list(self.parent.predictions)
                return res

            if sql.startswith("SELECT name FROM oracle_models"):
                res.fetchall.return_value = [(name,) for name in self.parent.models]
                return res

            if sql.startswith("INSERT INTO oracle_calibration_history"):
                self.parent.history.append({
                    "model_name": params["m"],
                    "horizon_days": int(params["h"]),
                    "brier": params["b"],
                    "ece": params["e"],
                    "scored_count": int(params["s"]),
                    "bucket_weight": params["w"],
                    "snapshot_at": datetime.now(timezone.utc),
                })
                return res

            if "FROM oracle_calibration_history" in sql:
                m = params["m"]
                h = int(params["h"])
                rows = [
                    (r["brier"], r["ece"], r["snapshot_at"])
                    for r in self.parent.history
                    if r["model_name"] == m and r["horizon_days"] == h
                ]
                res.fetchall.return_value = rows
                return res

            if sql.startswith("SELECT running_brier, running_ece, scored_prediction_count FROM oracle_models"):
                row = self.parent.models.get(params["m"])
                if row is None:
                    res.fetchone.return_value = None
                else:
                    res.fetchone.return_value = (
                        row["running_brier"],
                        row["running_ece"],
                        row["scored_prediction_count"],
                    )
                return res

            if sql.startswith("SELECT horizon_buckets FROM oracle_models"):
                row = self.parent.models.get(params["m"])
                res.fetchone.return_value = (row["horizon_buckets"],) if row else None
                return res

            if sql.startswith("INSERT INTO oracle_models"):
                m = params["m"]
                if m not in self.parent.models:
                    self.parent.models[m] = {
                        "running_brier": float(params["b"]),
                        "running_ece": float(params["e"]),
                        "scored_prediction_count": 1,
                        "horizon_buckets": None,
                    }
                return res

            if sql.startswith("UPDATE oracle_models SET horizon_buckets = CAST(:hb AS JSONB)"):
                row = self.parent.models.get(params["m"])
                if row is not None and row["horizon_buckets"] is None:
                    row["horizon_buckets"] = json.loads(params["hb"])
                return res

            if sql.startswith("UPDATE oracle_models SET horizon_buckets = jsonb_set"):
                row = self.parent.models[params["m"]]
                hb = dict(row["horizon_buckets"] or {})
                # Apply primary :path / :bucket
                path_key = params["path"].strip("{}")
                hb[path_key] = json.loads(params["bucket"])
                # Apply optional :ece_path / :ece_bins (nested jsonb_set)
                if "ece_bins" in params:
                    ece_key = params.get("ece_path", "{_ece_bins}").strip("{}")
                    hb[ece_key] = json.loads(params["ece_bins"])
                row["horizon_buckets"] = hb
                return res

            if sql.startswith("UPDATE oracle_models SET running_brier = :b"):
                row = self.parent.models[params["m"]]
                row["running_brier"] = float(params["b"])
                row["running_ece"] = float(params["e"])
                row["scored_prediction_count"] = int(params["n"])
                return res

            return res


# ── Helpers for Defect 2 (in-memory Brier tables without weight_sum col) ─


class _FakeBrierEngine:
    """In-memory stand-in matching the current 7/8-column Brier tables (no weight_sum column)."""

    def __init__(self) -> None:
        self.flat: dict[tuple[str, int], dict] = {}
        self.regime: dict[tuple[str, int, str], dict] = {}
        self.oracle_rows: list[tuple] = []
        self.update_calls: int = 0

    def begin(self):
        return _FakeBrierEngine._Ctx(self)

    def connect(self):
        return _FakeBrierEngine._Ctx(self)

    class _Ctx:
        def __init__(self, parent: _FakeBrierEngine) -> None:
            self.parent = parent

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, params=None):
            sql = " ".join(str(query).split())
            params = params or {}
            res = MagicMock()

            if sql.startswith("CREATE TABLE") or sql.startswith("CREATE INDEX"):
                return res
            if sql.startswith("TRUNCATE TABLE per_signal_brier_history"):
                self.parent.flat.clear()
                return res

            if sql.startswith("SELECT id, created_at, expiry, confidence"):
                res.fetchall.return_value = list(self.parent.oracle_rows)
                return res

            # Flat table reads
            if sql.startswith("SELECT scored_count, running_brier, running_ece, hit_count FROM per_signal_brier_history"):
                row = self.parent.flat.get((params["s"], int(params["h"])))
                res.fetchone.return_value = (
                    (row["scored_count"], row["running_brier"], row["running_ece"], row["hit_count"])
                    if row
                    else None
                )
                return res

            if sql.startswith("SELECT signal_source, horizon_days, scored_count") and "FROM per_signal_brier_history" in sql:
                if "WHERE signal_source" in sql:
                    row = self.parent.flat.get((params["s"], int(params["h"])))
                    res.fetchone.return_value = (
                        (
                            row["signal_source"],
                            row["horizon_days"],
                            row["scored_count"],
                            row["running_brier"],
                            row["running_ece"],
                            row["hit_count"],
                            row["last_updated"],
                        )
                        if row
                        else None
                    )
                    return res
                if "WHERE horizon_days" in sql and "scored_count >=" in sql:
                    h = int(params["h"])
                    n = int(params["n"])
                    rows = [
                        (
                            v["signal_source"],
                            v["horizon_days"],
                            v["scored_count"],
                            v["running_brier"],
                            v["running_ece"],
                            v["hit_count"],
                            v["last_updated"],
                        )
                        for k, v in self.parent.flat.items()
                        if k[1] == h and v["scored_count"] >= n
                    ]
                    rows.sort(key=lambda r: r[3])
                    res.fetchall.return_value = rows
                    return res
                rows = sorted(
                    (
                        (
                            v["signal_source"],
                            v["horizon_days"],
                            v["scored_count"],
                            v["running_brier"],
                            v["running_ece"],
                            v["hit_count"],
                            v["last_updated"],
                        )
                        for v in self.parent.flat.values()
                    ),
                    key=lambda r: (r[0], r[1]),
                )
                res.fetchall.return_value = rows
                return res

            if sql.startswith("SELECT signal_source, horizon_days, scored_count") and "FROM regime_conditional_brier_history" in sql:
                h = int(params["h"])
                r_key = params["r"]
                n = int(params["n"])
                rows = [
                    (
                        v["signal_source"],
                        v["horizon_days"],
                        v["scored_count"],
                        v["running_brier"],
                        v["running_ece"],
                        v["hit_count"],
                        v["last_updated"],
                    )
                    for k, v in self.parent.regime.items()
                    if k[1] == h and k[2] == r_key and v["scored_count"] >= n
                ]
                rows.sort(key=lambda r: r[3])
                res.fetchall.return_value = rows
                return res

            if sql.startswith("SELECT scored_count, running_brier, running_ece, hit_count, last_updated FROM regime_conditional_brier_history"):
                row = self.parent.regime.get((params["s"], int(params["h"]), params["r"]))
                res.fetchone.return_value = (
                    (
                        row["scored_count"],
                        row["running_brier"],
                        row["running_ece"],
                        row["hit_count"],
                        row["last_updated"],
                    )
                    if row
                    else None
                )
                return res

            # Flat table writes
            if sql.startswith("INSERT INTO per_signal_brier_history"):
                key = (params["s"], int(params["h"]))
                count = int(params.get("n", 1))
                stamp = params.get("scored_at") or datetime.now(timezone.utc)
                if key in self.parent.flat and "DO NOTHING" in sql:
                    res.rowcount = 0
                    return res
                existing = self.parent.flat.get(key)
                if existing is not None and existing.get("last_updated") is not None and stamp < existing["last_updated"]:
                    stamp = existing["last_updated"]
                self.parent.flat[key] = {
                    "signal_source": params["s"],
                    "horizon_days": int(params["h"]),
                    "scored_count": count,
                    "running_brier": float(params["b"]),
                    "running_ece": float(params["e"]),
                    "hit_count": int(params["hit"]),
                    "last_updated": stamp,
                }
                res.rowcount = 1
                return res

            if sql.startswith("UPDATE per_signal_brier_history"):
                self.parent.update_calls += 1
                key = (params["s"], int(params["hz"]))
                if key in self.parent.flat:
                    self.parent.flat[key].update(
                        {
                            "scored_count": int(params["n"]),
                            "running_brier": float(params["b"]),
                            "running_ece": float(params["e"]),
                            "hit_count": int(params["h"]),
                        }
                    )
                return res

            # Regime table reads/writes
            if sql.startswith("SELECT scored_count, running_brier, running_ece, hit_count FROM regime_conditional_brier_history"):
                row = self.parent.regime.get((params["s"], int(params["h"]), params["r"]))
                res.fetchone.return_value = (
                    (row["scored_count"], row["running_brier"], row["running_ece"], row["hit_count"])
                    if row
                    else None
                )
                return res

            if sql.startswith("INSERT INTO regime_conditional_brier_history"):
                key = (params["s"], int(params["h"]), params["r"])
                count = int(params.get("n", 1))
                if key in self.parent.regime and "DO NOTHING" in sql:
                    res.rowcount = 0
                    return res
                self.parent.regime[key] = {
                    "signal_source": params["s"],
                    "horizon_days": int(params["h"]),
                    "regime": params["r"],
                    "scored_count": count,
                    "running_brier": float(params["b"]),
                    "running_ece": float(params["e"]),
                    "hit_count": int(params["hit"]),
                    "last_updated": datetime.now(timezone.utc),
                }
                res.rowcount = 1
                return res

            if sql.startswith("UPDATE regime_conditional_brier_history"):
                self.parent.update_calls += 1
                key = (params["s"], int(params["hz"]), params["r"])
                if key in self.parent.regime:
                    self.parent.regime[key].update(
                        {
                            "scored_count": int(params["n"]),
                            "running_brier": float(params["b"]),
                            "running_ece": float(params["e"]),
                            "hit_count": int(params["h"]),
                        }
                    )
                return res

            return res


# ── Helpers for Defect 3 ─────────────────────────────────────────────────


def _checked(
    verdict: str,
    confidence: float = 0.9,
    checks: tuple[SanityResult, ...] = (),
    critical_fail: bool = False,
    reason: str = "",
    text: str = "SPY at $500",
    span: tuple[int, int] = (0, 11),
) -> CheckedClaim:
    claim = Claim(text=text, claim_type="price", ticker="SPY", value=500.0, source_span=span)
    vc = VerifiedClaim(claim=claim, verdict=verdict, confidence=confidence, reason=reason)
    return CheckedClaim(verified=vc, checks=checks, critical_fail=critical_fail)


# ══════════════════════════════════════════════════════════════════════════
# DEFECT 1: Binned running ECE in oracle/calibration.py
# ══════════════════════════════════════════════════════════════════════════


class TestDefect1RunningBinnedECE:
    def test_calibrated_half_model_beats_overconfident_model(self):
        engine = _FakeCalibrationEngine()

        # Model A: perfectly calibrated p=0.50 (10 hits, 10 misses) -> true ECE = 0.0
        for i in range(20):
            out_a = update_running_metrics(
                engine,
                model_id="calibrated_50",
                prediction=0.50,
                actual=1.0 if i < 10 else 0.0,
                horizon=7,
            )

        # Model B: overconfident p=0.99 (16 hits, 4 misses -> 80% hit rate) -> true ECE = 0.19
        for i in range(20):
            out_b = update_running_metrics(
                engine,
                model_id="overconfident_99",
                prediction=0.99,
                actual=1.0 if i < 16 else 0.0,
                horizon=7,
            )

        assert out_a["running_ece"] == pytest.approx(0.0, abs=1e-6)
        assert out_b["running_ece"] == pytest.approx(0.19, abs=1e-6)
        assert out_a["running_ece"] < out_b["running_ece"]

        per_h_a = compute_per_horizon_calibration(engine, "calibrated_50")
        per_h_b = compute_per_horizon_calibration(engine, "overconfident_99")
        assert per_h_a[7]["ece"] == pytest.approx(0.0, abs=1e-6)
        assert per_h_b[7]["ece"] == pytest.approx(0.19, abs=1e-6)

    def test_running_binned_ece_matches_batch_compute_calibration(self):
        engine = _FakeCalibrationEngine()
        samples = [
            (0.25, 0.0), (0.25, 0.0), (0.25, 1.0), (0.25, 0.0),  # bin 2: mean_p=0.25, mean_y=0.25 -> err 0
            (0.55, 1.0), (0.55, 0.0), (0.55, 1.0), (0.55, 0.0),  # bin 5: mean_p=0.55, mean_y=0.50 -> err 0.05
            (0.85, 1.0), (0.85, 0.0),                             # bin 8: mean_p=0.85, mean_y=0.50 -> err 0.35
        ]
        # True ECE = (4/10)*0.0 + (4/10)*0.05 + (2/10)*0.35 = 0.02 + 0.07 = 0.09
        for p, y in samples:
            out = update_running_metrics(engine, model_id="multi_bin", prediction=p, actual=y, horizon=7)
            engine.predictions.append((p, "hit" if y == 1.0 else "miss"))

        batch = compute_calibration(engine, n_bins=10)
        assert out["running_ece"] == pytest.approx(0.09, abs=1e-6)
        assert out["running_ece"] == pytest.approx(batch.calibration_error, abs=1e-6)

    def test_ece_bins_preserved_across_engine_bucket_updates(self):
        """ModelRegistry only overwrites {7d} via jsonb_set; sibling ECE bins must survive."""
        from oracle.engine import _parse_horizon_buckets

        engine = _FakeCalibrationEngine()
        update_running_metrics(engine, model_id="m1", prediction=0.50, actual=1.0, horizon=7)

        # Simulate ModelRegistry._nudge_single_model overwriting only {7d}
        hb = engine.models["m1"]["horizon_buckets"]
        parsed = _parse_horizon_buckets(hb)
        parsed["7d"]["weight"] = 1.05
        hb["7d"] = parsed["7d"]
        engine.models["m1"]["horizon_buckets"] = hb

        out = update_running_metrics(engine, model_id="m1", prediction=0.50, actual=0.0, horizon=7)
        # Both (0.5, 1.0) and (0.5, 0.0) are in bin 5 -> ECE = 0.0
        assert out["running_ece"] == pytest.approx(0.0, abs=1e-6)


# ══════════════════════════════════════════════════════════════════════════
# DEFECT 2: Proper weighted mean Brier + no-drift guard + Σw >= 5 floor
# ══════════════════════════════════════════════════════════════════════════


class TestDefect2WeightedBrier:
    def test_live_flat_updater_skips_new_and_existing_without_inserting_count1(self):
        from features import per_signal_brier as psb

        psb._reset_skipped_live_update_counts()
        engine = _FakeBrierEngine()
        # Brand-new bucket: must NOT insert a dead count=1 row.
        u_new = record_flat_brier(
            engine,
            horizon_days=7,
            confidence=0.5,
            outcome=1.0,
            signal_contributions={"small_noise": 0.05, "dominant": 0.95},
        )
        assert u_new == {}
        assert engine.flat == {}
        assert engine.update_calls == 0

        # Existing bucket (seeded via bootstrap): must also skip without UPDATE drift.
        engine.flat[("small_noise", 7)] = {
            "signal_source": "small_noise",
            "horizon_days": 7,
            "scored_count": 25,
            "running_brier": 0.25,
            "running_ece": 0.50,
            "hit_count": 12,
            "last_updated": datetime.now(timezone.utc),
        }
        before = dict(engine.flat[("small_noise", 7)])
        u_existing = record_flat_brier(
            engine,
            horizon_days=7,
            confidence=0.9,
            outcome=0.0,
            signal_contributions={"small_noise": 0.05},
        )
        assert u_existing == {}
        assert engine.update_calls == 0
        assert engine.flat[("small_noise", 7)] == before
        counts = get_flat_live_skip_counts()
        assert counts[("small_noise", 7)] == 2
        assert counts[("dominant", 7)] == 1

    def test_live_flat_updater_logs_once_per_process_per_source_horizon_at_info(self):
        from loguru import logger
        from features import per_signal_brier as psb

        psb._reset_skipped_live_update_counts()
        records: list[tuple[str, str]] = []
        hid = logger.add(
            lambda msg: records.append((msg.record["level"].name, msg.record["message"])),
            level="DEBUG",
        )
        try:
            engine = _FakeBrierEngine()
            for _ in range(4):
                record_flat_brier(
                    engine,
                    horizon_days=7,
                    confidence=0.6,
                    outcome=1.0,
                    signal_contributions={"sig_alpha": 0.5, "sig_beta": 0.5},
                )
            for _ in range(2):
                record_flat_brier(
                    engine,
                    horizon_days=30,
                    confidence=0.6,
                    outcome=1.0,
                    signal_contributions={"sig_alpha": 1.0},
                )
        finally:
            logger.remove(hid)

        warnings = [m for lvl, m in records if lvl == "WARNING"]
        info_or_debug = [m for lvl, m in records if lvl in ("INFO", "DEBUG") and "per_signal_brier" in m]
        assert warnings == []
        # Distinct (source, horizon) keys: (sig_alpha, 7), (sig_beta, 7), (sig_alpha, 30)
        assert len(info_or_debug) == 3
        counts = get_flat_live_skip_counts()
        assert counts == {("sig_alpha", 7): 4, ("sig_beta", 7): 4, ("sig_alpha", 30): 2}

    def test_live_regime_updater_skips_new_and_existing_and_logs_once_per_source_horizon(self):
        from loguru import logger
        from features import regime_conditional_brier as rcb

        rcb._reset_skipped_live_update_counts()
        records: list[tuple[str, str]] = []
        hid = logger.add(
            lambda msg: records.append((msg.record["level"].name, msg.record["message"])),
            level="DEBUG",
        )
        try:
            engine = _FakeBrierEngine()
            # Brand-new bucket -> must not insert count=1 row.
            u1 = record_regime_brier(
                engine,
                horizon_days=7,
                confidence=0.5,
                outcome=1.0,
                signal_contributions={"small_noise": 0.05},
                regime="EXPANSION",
            )
            assert u1 == {}
            assert engine.regime == {}

            # Existing bucket -> must not update; repeated calls across regimes
            # for same (source, horizon) must log only once per process.
            engine.regime[("small_noise", 7, "EXPANSION")] = {
                "signal_source": "small_noise",
                "horizon_days": 7,
                "regime": "EXPANSION",
                "scored_count": 20,
                "running_brier": 0.25,
                "running_ece": 0.50,
                "hit_count": 10,
                "last_updated": datetime.now(timezone.utc),
            }
            for reg in ("EXPANSION", "CRISIS", "EXPANSION"):
                u2 = record_regime_brier(
                    engine,
                    horizon_days=7,
                    confidence=0.9,
                    outcome=0.0,
                    signal_contributions={"small_noise": 0.05},
                    regime=reg,
                )
                assert u2 == {}
        finally:
            logger.remove(hid)

        assert engine.update_calls == 0
        assert engine.regime[("small_noise", 7, "EXPANSION")]["scored_count"] == 20
        assert ("small_noise", 7, "CRISIS") not in engine.regime
        warnings = [m for lvl, m in records if lvl == "WARNING"]
        info_or_debug = [
            m for lvl, m in records if lvl in ("INFO", "DEBUG") and "regime_conditional_brier" in m
        ]
        assert warnings == []
        assert len(info_or_debug) == 1
        assert get_regime_live_skip_counts()[("small_noise", 7)] == 4

    def test_read_path_conservative_floor_on_flat_and_regime_tables(self):
        engine = _FakeBrierEngine()
        now = datetime.now(timezone.utc)
        engine.flat[("under_floor", 7)] = {
            "signal_source": "under_floor",
            "horizon_days": 7,
            "scored_count": 19,
            "running_brier": 0.01,
            "running_ece": 0.05,
            "hit_count": 19,
            "last_updated": now,
        }
        engine.flat[("at_floor", 7)] = {
            "signal_source": "at_floor",
            "horizon_days": 7,
            "scored_count": 20,
            "running_brier": 0.01,
            "running_ece": 0.05,
            "hit_count": 20,
            "last_updated": now,
        }
        engine.regime[("regime_mid", 7, "EXPANSION")] = {
            "signal_source": "regime_mid",
            "horizon_days": 7,
            "regime": "EXPANSION",
            "scored_count": 15,  # >= MIN_REGIME_SAMPLES (10), < MIN_CALIBRATED_SAMPLES (20)
            "running_brier": 0.01,
            "running_ece": 0.05,
            "hit_count": 15,
            "last_updated": now,
        }
        engine.regime[("regime_ready", 7, "EXPANSION")] = {
            "signal_source": "regime_ready",
            "horizon_days": 7,
            "regime": "EXPANSION",
            "scored_count": 20,
            "running_brier": 0.01,
            "running_ece": 0.05,
            "hit_count": 20,
            "last_updated": now,
        }

        # Flat read paths
        c_under = get_signal_scorecard(engine, "under_floor", 7)
        c_at = get_signal_scorecard(engine, "at_floor", 7)
        assert c_under is not None and c_under.is_calibrated is False and c_under.conviction_weight == 1.0
        assert c_at is not None and c_at.is_calibrated is True and c_at.conviction_weight == 1.5

        ranked_flat = {c.signal_source: c for c in rank_signals_by_horizon(engine, 7, min_samples=0)}
        assert ranked_flat["under_floor"].conviction_weight == 1.0
        assert ranked_flat["at_floor"].conviction_weight == 1.5

        full_flat = {c.signal_source: c for c in get_full_scorecard_table(engine)}
        assert full_flat["under_floor"].conviction_weight == 1.0
        assert full_flat["at_floor"].conviction_weight == 1.5

        # Regime read paths
        rc_mid = get_regime_conditional_scorecard(engine, "regime_mid", 7, "EXPANSION")
        rc_ready = get_regime_conditional_scorecard(engine, "regime_ready", 7, "EXPANSION")
        assert rc_mid is not None and rc_mid.is_calibrated is False and rc_mid.conviction_weight == 1.0
        assert rc_ready is not None and rc_ready.is_calibrated is True and rc_ready.conviction_weight == 1.5

        ranked_reg = {c.signal_source: c for c in rank_signals_by_regime(engine, "EXPANSION", 7)}
        assert ranked_reg["regime_mid"].conviction_weight == 1.0
        assert ranked_reg["regime_ready"].conviction_weight == 1.5

    def test_bootstrap_computes_exact_weighted_mean_not_diluted_count_mean(self):
        engine = _FakeBrierEngine()
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = []
        # 100 coin flips (conf=0.5, hit/miss -> se=0.25): small_noise w=0.05 (Σw=5.0), big_noise w=0.95 (Σw=95.0)
        for i in range(100):
            rows.append({
                "id": f"cf-{i}",
                "ticker": "SPY",
                "created_at": base + timedelta(days=i),
                "expiry": base + timedelta(days=i + 7),
                "confidence": 0.5,
                "verdict": "hit" if i % 2 == 0 else "miss",
                "model_name": None,
                "signals": None,
                "signal_contributions": {"small_noise": 0.05, "big_noise": 0.95},
                "model_weights": None,
                "scored_at": base + timedelta(days=i + 7),
            })
        # Varying weights: 10 rows (w=0.1, se=0.01) + 10 rows (w=0.9, se=0.81) -> Σ(w·se)/Σw = 0.73
        for i in range(10):
            rows.append({
                "id": f"vw-good-{i}",
                "ticker": "QQQ",
                "created_at": base + timedelta(days=i),
                "expiry": base + timedelta(days=i + 7),
                "confidence": 0.9,
                "verdict": "hit",  # se = 0.01
                "model_name": None,
                "signals": None,
                "signal_contributions": {"varied_w": 0.1, "other": 0.9},
                "model_weights": None,
                "scored_at": base + timedelta(days=i + 7),
            })
        for i in range(10):
            rows.append({
                "id": f"vw-bad-{i}",
                "ticker": "QQQ",
                "created_at": base + timedelta(days=10 + i),
                "expiry": base + timedelta(days=17 + i),
                "confidence": 0.9,
                "verdict": "miss",  # se = 0.81
                "model_name": None,
                "signals": None,
                "signal_contributions": {"varied_w": 0.9, "other": 0.1},
                "model_weights": None,
                "scored_at": base + timedelta(days=17 + i),
            })

        with patch.object(boot, "_fetch_scored_predictions", return_value=rows), patch.object(
            boot, "_load_oracle_models_lookup", return_value={}
        ):
            boot.replay_predictions(engine, days=365)

        small = get_signal_scorecard(engine, "small_noise", 7)
        big = get_signal_scorecard(engine, "big_noise", 7)
        varied = get_signal_scorecard(engine, "varied_w", 7)

        assert small is not None and small.running_brier == pytest.approx(0.25, abs=1e-6)
        assert small.conviction_weight == 0.0  # coin flip is NOT best-in-class 1.5x!
        assert big is not None and big.running_brier == pytest.approx(0.25, abs=1e-6)
        assert varied is not None and varied.running_brier == pytest.approx(0.73, abs=1e-6)

    def test_regime_bootstrap_computes_exact_weighted_mean(self):
        engine = _FakeBrierEngine()
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        # 10 rows (w=0.1, se=0.01) + 10 rows (w=0.9, se=0.81) -> Σ(w·se)/Σw = 0.73
        for i in range(10):
            engine.oracle_rows.append((
                f"r-good-{i}", base + timedelta(days=i), (base + timedelta(days=i + 7)).date(),
                0.9, "hit", "m", None, '{"varied_w": 0.1, "other": 0.9}', "EXPANSION",
            ))
        for i in range(10):
            engine.oracle_rows.append((
                f"r-bad-{i}", base + timedelta(days=10 + i), (base + timedelta(days=17 + i)).date(),
                0.9, "miss", "m", None, '{"varied_w": 0.9, "other": 0.1}', "EXPANSION",
            ))

        bootstrap_from_oracle_predictions(engine, days=365)
        row = engine.regime[("varied_w", 7, "EXPANSION")]
        assert row["scored_count"] == 20
        assert row["running_brier"] == pytest.approx(0.73, abs=1e-6)

    def test_minimum_effective_weight_sum_gates_conviction_multiplier(self):
        # Below Σw = 5.0 -> neutral 1.0 even with count >= 20 and tiny Brier
        assert compute_conviction_weight(0.01, 100, weight_sum=4.99) == 1.0
        assert compute_conviction_weight(0.40, 100, weight_sum=1.25) == 1.0
        # At/above Σw = 5.0 -> normal conviction curve applies
        assert compute_conviction_weight(0.01, 100, weight_sum=5.0) == 1.5
        assert compute_conviction_weight(0.25, 100, weight_sum=5.0) == 0.0

    def test_bootstrap_fails_closed_when_effective_weight_below_five(self):
        """25 lucky predictions at w=0.05 (Σw=1.25 < 5) must NOT produce a boosted scorecard."""
        engine = _FakeBrierEngine()
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            {
                "id": f"lucky-{i}",
                "ticker": "SPY",
                "created_at": base + timedelta(days=i),
                "expiry": base + timedelta(days=i + 7),
                "confidence": 0.9,
                "verdict": "hit",  # se = 0.01
                "model_name": None,
                "signals": None,
                "signal_contributions": {"tiny_lucky": 0.05, "main_driver": 0.95},
                "model_weights": None,
                "scored_at": base + timedelta(days=i + 7),
            }
            for i in range(25)
        ]
        with patch.object(boot, "_fetch_scored_predictions", return_value=rows), patch.object(
            boot, "_load_oracle_models_lookup", return_value={}
        ):
            boot.replay_predictions(engine, days=365)

        # main_driver has Σw = 23.75 >= 5 -> calibrated 1.5x
        main = get_signal_scorecard(engine, "main_driver", 7)
        assert main is not None and main.conviction_weight == 1.5
        # tiny_lucky has Σw = 1.25 < 5 -> fails closed (either None or neutral 1.0)
        tiny = get_signal_scorecard(engine, "tiny_lucky", 7)
        assert tiny is None or tiny.conviction_weight == 1.0


# ══════════════════════════════════════════════════════════════════════════
# DEFECT 3: Honest publisher gate and [UNVERIFIED] marking
# ══════════════════════════════════════════════════════════════════════════


class TestDefect3PublisherGate:
    def test_zero_claims_publishes_no_claims_with_none_score(self):
        d = gate_decision([])
        assert d.decision == "publish"
        assert d.verification == "no_claims"
        assert d.score is None

    def test_all_insufficient_marks_unverified_and_never_blocks_chat(self):
        text = "SPY at $500 and QQQ at $400."
        c1 = _checked("insufficient", 0.5, text="$500", span=(7, 11))
        c2 = _checked("insufficient", 0.5, text="$400", span=(23, 27))

        d = gate_decision([c1, c2])
        assert d.decision == "review"
        assert d.verification == "unverified"

        engine = MagicMock()
        with patch("oracle.firewall.extract_claims", return_value=[c1.verified.claim, c2.verified.claim]), \
            patch("oracle.firewall.verify_claims", return_value=[c1.verified, c2.verified]), \
            patch("oracle.firewall.run_sanity_checks", return_value=[c1, c2]), \
            patch("oracle.firewall._audit_claims"):
            fw = verify_output(text, engine)

        assert fw.output_text != _SAFE_FALLBACK
        assert "[UNVERIFIED]" in fw.output_text
        assert fw.output_text.count("[UNVERIFIED]") == 2
        assert "$500" in fw.output_text and "$400" in fw.output_text

    def test_all_insufficient_with_warn_flag_hits_flagged_branch_first(self):
        warn = SanityResult("range_check", "warn", "near upper bound")
        c = _checked("insufficient", 0.5, checks=(warn,))
        d = gate_decision([c])
        assert d.decision == "review"
        assert d.verification == "flagged"
        assert any("flagged" in r for r in d.reasons)

    def test_low_confidence_unflagged_mix_marked_unverified(self):
        text = "SPY at $500."
        c = _checked("supported", 0.10, text="$500", span=(7, 11))
        d = gate_decision([c])
        assert d.decision == "review"
        assert d.verification == "low_confidence"
        assert d.score == pytest.approx(0.10)

        engine = MagicMock()
        with patch("oracle.firewall.extract_claims", return_value=[c.verified.claim]), \
            patch("oracle.firewall.verify_claims", return_value=[c.verified]), \
            patch("oracle.firewall.run_sanity_checks", return_value=[c]), \
            patch("oracle.firewall._audit_claims"):
            fw = verify_output(text, engine)

        assert fw.output_text != _SAFE_FALLBACK
        assert "[UNVERIFIED] $500" in fw.output_text

    def test_threshold_085_separates_clean_publish_from_marked_review(self):
        below = gate_decision([_checked("supported", 0.84)])
        at_thresh = gate_decision([_checked("supported", 0.85)])
        assert below.decision == "review"
        assert at_thresh.decision == "publish"
        assert at_thresh.verification == "verified"

    def test_contradicted_keeps_reject_behavior(self):
        text = "SPY at $500."
        c = _checked("contradicted", 0.9, reason="DB shows $400", text="$500", span=(7, 11))
        d = gate_decision([c])
        assert d.decision == "reject"
        assert d.score == 0.0

        engine = MagicMock()
        with patch("oracle.firewall.extract_claims", return_value=[c.verified.claim]), \
            patch("oracle.firewall.verify_claims", return_value=[c.verified]), \
            patch("oracle.firewall.run_sanity_checks", return_value=[c]), \
            patch("oracle.firewall._audit_claims"):
            fw = verify_output(text, engine)

        assert fw.output_text == _SAFE_FALLBACK


# ══════════════════════════════════════════════════════════════════════════
# FOLLOW-UP ISSUE 2: ECE cutover drift protection (30-sample bin threshold)
# ══════════════════════════════════════════════════════════════════════════


class TestFollowup2ECECutoverDriftProtection:
    def _seed_legacy_model_and_mae_history(self) -> _FakeCalibrationEngine:
        engine = _FakeCalibrationEngine()
        engine.models["legacy_m"] = {
            "running_brier": 0.20,
            "running_ece": 0.42,
            "scored_prediction_count": 200,
            "horizon_buckets": {
                "7d": {
                    "scored": 200,
                    "brier": 0.20,
                    "ece": 0.42,  # pre-cutover MAE
                    "weight": 1.0,
                    "hits": 100,
                    "misses": 100,
                    "partials": 0,
                },
            },
        }
        now = datetime.now(timezone.utc)
        for day in range(1, 21):
            engine.history.append({
                "model_name": "legacy_m",
                "horizon_days": 7,
                "brier": 0.20 if day % 2 == 0 else 0.21,
                "ece": 0.41 if day % 2 == 0 else 0.43,
                "scored_count": 200 - day,
                "bucket_weight": 1.0,
                "snapshot_at": now - timedelta(days=day),
            })
        return engine

    def test_legacy_bucket_first_update_keeps_ece_untouched_snapshots_null_no_alert(self):
        engine = self._seed_legacy_model_and_mae_history()
        # 1st post-cutover sample: |0.95 - 1.0| = 0.05 (37σ from legacy MAE 0.42!)
        update_running_metrics(
            engine,
            model_id="legacy_m",
            prediction=0.95,
            actual=1.0,
            horizon=7,
        )

        # Legacy bucket ece must NOT be overwritten by a 1-sample binned ECE.
        assert engine.models["legacy_m"]["horizon_buckets"]["7d"]["ece"] == pytest.approx(0.42)
        per_h = compute_per_horizon_calibration(engine, "legacy_m")
        assert per_h[7]["ece_bin_samples"] == 1
        assert per_h[7]["ece_valid"] is False
        assert per_h[7]["ece_binned"] is None

        # Snapshot must persist NULL for ece until 30 binned samples exist.
        pre_len = len(engine.history)
        snapshot_calibration_history(engine)
        assert len(engine.history) == pre_len + 1
        assert engine.history[-1]["ece"] is None
        assert engine.history[-1]["brier"] is not None

        # Drift detector must emit ZERO ECE alerts during warmup.
        alerts = detect_calibration_drift(engine, sigma_threshold=2.0)
        assert [a for a in alerts if a.metric == "ece"] == []

    def test_after_30_binned_samples_real_ece_used_and_pre_cutover_history_ignored(self):
        engine = self._seed_legacy_model_and_mae_history()

        # Feed 29 perfectly-calibrated samples at p=0.50 -> still warming up (< 30)
        for i in range(29):
            update_running_metrics(
                engine,
                model_id="legacy_m",
                prediction=0.50,
                actual=1.0 if i < 15 else 0.0,
                horizon=7,
            )
        per_h_29 = compute_per_horizon_calibration(engine, "legacy_m")
        assert per_h_29[7]["ece_bin_samples"] == 29
        assert per_h_29[7]["ece_valid"] is False
        assert engine.models["legacy_m"]["horizon_buckets"]["7d"]["ece"] == pytest.approx(0.42)
        assert [a for a in detect_calibration_drift(engine, sigma_threshold=2.0) if a.metric == "ece"] == []

        # 30th sample completes 15 hits + 15 misses at p=0.50 -> true binned ECE = 0.0
        update_running_metrics(
            engine,
            model_id="legacy_m",
            prediction=0.50,
            actual=0.0,
            horizon=7,
        )
        per_h_30 = compute_per_horizon_calibration(engine, "legacy_m")
        assert per_h_30[7]["ece_bin_samples"] == 30
        assert per_h_30[7]["ece_valid"] is True
        assert per_h_30[7]["ece_binned"] == pytest.approx(0.0, abs=1e-6)
        assert per_h_30[7]["ece"] == pytest.approx(0.0, abs=1e-6)
        assert per_h_30[7]["ece_valid_since"] is not None

        # Snapshot now records real binned ECE (0.0), not NULL.
        snapshot_calibration_history(engine)
        assert engine.history[-1]["ece"] == pytest.approx(0.0, abs=1e-6)

        # Immediately after cutover, the 20 pre-cutover MAE rows (snapshot_at < valid_since)
        # must be ignored, so still NO spurious ECE drift alert!
        assert [a for a in detect_calibration_drift(engine, sigma_threshold=2.0) if a.metric == "ece"] == []

        # Simulate 10 post-cutover binned-ECE snapshots (mean=0.015, std~0.005) while
        # the 20 old 0.42 MAE rows are still in the table.
        now = datetime.now(timezone.utc)
        cutover_ts = now - timedelta(days=12)
        engine.models["legacy_m"]["horizon_buckets"]["_ece_bins"]["_valid_since"]["7d"] = cutover_ts.isoformat()
        # Ensure old MAE rows are strictly before cutover_ts.
        for idx, row in enumerate(engine.history[:20]):
            row["snapshot_at"] = cutover_ts - timedelta(days=idx + 1)
        # Drop the just-taken snapshot from above and append 10 clean post-cutover snapshots.
        engine.history = engine.history[:20]
        for day in range(1, 11):
            engine.history.append({
                "model_name": "legacy_m",
                "horizon_days": 7,
                "brier": 0.20 if day % 2 == 0 else 0.21,
                "ece": 0.01 if day % 2 == 0 else 0.02,
                "scored_count": 230 + day,
                "bucket_weight": 1.0,
                "snapshot_at": cutover_ts + timedelta(days=day),
            })

        # Drift real binned ECE up by feeding 30 bad misses at p=0.90, actual=0.0 -> ECE = 0.45.
        for _ in range(30):
            update_running_metrics(
                engine,
                model_id="legacy_m",
                prediction=0.90,
                actual=0.0,
                horizon=7,
            )

        alerts = detect_calibration_drift(engine, sigma_threshold=2.0)
        ece_alerts = [a for a in alerts if a.metric == "ece"]
        assert len(ece_alerts) == 1
        # Baseline mean MUST come solely from post-cutover 0.01/0.02 snapshots, NOT 0.42 MAE!
        assert ece_alerts[0].baseline_mean == pytest.approx(0.015, abs=1e-6)
        assert ece_alerts[0].current == pytest.approx(0.45, abs=1e-6)


# ══════════════════════════════════════════════════════════════════════════
# FOLLOW-UP ISSUE 3: Chat review answers (audit published + auto-label gate)
# ══════════════════════════════════════════════════════════════════════════


class TestFollowup3ChatReviewAndAuditPublished:
    def test_audit_claims_marks_review_published_true_and_reject_false(self):
        cc = _checked("insufficient", 0.5)
        for dec_name, expected in (("publish", True), ("review", True), ("reject", False)):
            eng = MagicMock()
            conn = eng.connect.return_value.__enter__.return_value
            _audit_claims(
                eng,
                [cc],
                PublishDecision(decision=dec_name, score=None, claims=(cc,), reasons=()),
            )
            params = conn.execute.call_args[0][1]
            assert params["published"] is expected, dec_name

    def _run_chat_with_firewall_result(self, fw_result):
        from api.routers import chat as chat_router

        req = chat_router.ChatAskRequest(question="What is SPY doing?", context_ticker="SPY")
        fake_llm = MagicMock()
        fake_llm.model = "test-llm"
        fake_llm.chat.return_value = fw_result.original_text

        with patch.object(chat_router, "_lf_set_input"), \
            patch.object(chat_router, "_maybe_trigger_timesfm"), \
            patch.object(chat_router, "_build_context_block", return_value=("ctx", ["db"])), \
            patch.object(chat_router, "_get_llm_client", return_value=(fake_llm, "local")), \
            patch.object(chat_router, "_get_db_engine", return_value=MagicMock()), \
            patch.object(chat_router, "_auto_label_failure") as auto_label, \
            patch("oracle.firewall.verify_output", return_value=fw_result), \
            patch("intelligence.post_query_scanner.spawn_post_query_scan"):
            resp = chat_router._ask_grid_impl(
                req=req,
                question=req.question,
                ticker="SPY",
                timeframe=None,
                now=datetime.now(timezone.utc),
            )
        return resp, auto_label

    def test_chat_review_unverified_and_low_confidence_do_not_auto_label(self):
        from oracle.firewall import FirewallResult

        # 1. All-insufficient -> review (verification="unverified")
        c_ins = _checked("insufficient", 0.5, text="$500", span=(7, 11))
        d_ins = gate_decision([c_ins])
        fw_ins = FirewallResult(
            original_text="SPY at $500.",
            output_text="SPY at [UNVERIFIED] $500.",
            decision=d_ins,
            claim_count=1,
            flagged_count=0,
        )
        resp_ins, auto_ins = self._run_chat_with_firewall_result(fw_ins)
        assert resp_ins.firewall_decision == "review"
        assert "[UNVERIFIED]" in resp_ins.answer
        auto_ins.assert_not_called()

        # 2. Low-confidence -> review (verification="low_confidence")
        c_low = _checked("supported", 0.50, text="$500", span=(7, 11))
        d_low = gate_decision([c_low])
        fw_low = FirewallResult(
            original_text="SPY at $500.",
            output_text="SPY at [UNVERIFIED] $500.",
            decision=d_low,
            claim_count=1,
            flagged_count=0,
        )
        resp_low, auto_low = self._run_chat_with_firewall_result(fw_low)
        assert resp_low.firewall_decision == "review"
        assert "[UNVERIFIED]" in resp_low.answer
        auto_low.assert_not_called()

    def test_chat_review_flagged_or_contradicted_and_reject_do_auto_label(self):
        from oracle.firewall import FirewallResult

        # 1. Flagged-warn review (flagged_count=0, but claim has warn sanity check)
        warn = SanityResult("range_check", "warn", "near upper bound")
        c_warn = _checked("supported", 0.90, checks=(warn,), text="$500", span=(7, 11))
        d_warn = gate_decision([c_warn])
        assert d_warn.decision == "review" and d_warn.verification == "flagged"
        fw_warn = FirewallResult(
            original_text="SPY at $500.",
            output_text="SPY at [UNVERIFIED] $500.",
            decision=d_warn,
            claim_count=1,
            flagged_count=0,
        )
        resp_warn, auto_warn = self._run_chat_with_firewall_result(fw_warn)
        assert resp_warn.firewall_decision == "review"
        assert "[UNVERIFIED]" in resp_warn.answer
        auto_warn.assert_called_once()

        # 2. Review containing a contradicted claim
        c_contra = _checked("contradicted", 0.90, reason="DB shows $400", text="$500", span=(7, 11))
        d_review_contra = PublishDecision(
            decision="review",
            score=0.5,
            claims=(c_contra,),
            reasons=("CONTRADICTED: DB shows $400",),
            verification="flagged",
        )
        fw_review_contra = FirewallResult(
            original_text="SPY at $500.",
            output_text="SPY at [UNVERIFIED] $500.",
            decision=d_review_contra,
            claim_count=1,
            flagged_count=1,
        )
        _, auto_rc = self._run_chat_with_firewall_result(fw_review_contra)
        auto_rc.assert_called_once()

        # 3. Hard reject
        d_rej = gate_decision([c_contra])
        fw_rej = FirewallResult(
            original_text="SPY at $500.",
            output_text=_SAFE_FALLBACK,
            decision=d_rej,
            claim_count=1,
            flagged_count=1,
        )
        resp_rej, auto_rej = self._run_chat_with_firewall_result(fw_rej)
        assert resp_rej.firewall_decision == "reject"
        assert resp_rej.answer == _SAFE_FALLBACK
        auto_rej.assert_called_once()


# ══════════════════════════════════════════════════════════════════════════
# FOLLOW-UP ISSUE 4: _write_bootstrapped_buckets counts only on commit success
# ══════════════════════════════════════════════════════════════════════════


class TestFollowup4BootstrapWriteStats:
    def _sample_buckets(self) -> dict:
        now = datetime.now(timezone.utc)
        return {
            ("sig_a", 7): {
                "scored_count": 25,
                "weight_sum": 12.5,
                "weighted_brier_sum": 1.25,
                "weighted_ece_sum": 2.5,
                "hit_count": 20,
                "last_updated": now,
            },
            ("sig_b", 7): {
                "scored_count": 25,
                "weight_sum": 12.5,
                "weighted_brier_sum": 2.5,
                "weighted_ece_sum": 3.0,
                "hit_count": 15,
                "last_updated": now,
            },
        }

    def test_written_stays_zero_when_mid_batch_execute_fails(self):
        call_n = {"n": 0}

        class _FailSecondEngine:
            def begin(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, query, params=None):
                sql = str(query).strip()
                if sql.startswith("CREATE"):
                    return MagicMock()
                call_n["n"] += 1
                if call_n["n"] == 2:
                    raise RuntimeError("mid-transaction failure")
                res = MagicMock()
                res.rowcount = 1
                return res

        stats = boot._write_bootstrapped_buckets(_FailSecondEngine(), self._sample_buckets())
        assert stats["written"] == 0

    def test_written_stays_zero_when_commit_fails(self):
        class _FailCommitEngine:
            def __init__(self):
                self._in_ddl = True

            def begin(self):
                return _FailCommitEngine._Ctx(self)

            class _Ctx:
                def __init__(self, parent):
                    self.parent = parent
                    self.saw_insert = False

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    if exc_type is None and self.saw_insert:
                        raise RuntimeError("commit failed")
                    return False

                def execute(self, query, params=None):
                    if "INSERT INTO per_signal_brier_history" in str(query):
                        self.saw_insert = True
                    res = MagicMock()
                    res.rowcount = 1
                    return res

        stats = boot._write_bootstrapped_buckets(_FailCommitEngine(), self._sample_buckets())
        assert stats["written"] == 0
