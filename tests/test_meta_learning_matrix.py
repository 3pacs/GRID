"""Tests for ``intelligence/meta_learning_matrix.py``.

Uses an in-memory FakeEngine modeled on the walk-forward validator's
pattern — services every SQL statement the module emits via substring
markers in the text so we can exercise the full read/write/bootstrap
path without touching a real database.

Covers:
- pure bucket helpers (horizon / fci / vol)
- ConditionTuple / MetaEdgeRow dataclass behavior
- record_scored_prediction write semantics + shapley-weight gating
- get_edge_row / get_weight_multiplier read semantics
- get_aggregate_weight_multiplier harmonic-mean aggregation
- rank_signals_by_edge sort order
- bootstrap_from_oracle_predictions replay
- _ensure_schema idempotency
- DB-failure safety fallbacks
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from intelligence import meta_learning_matrix as mlm
from intelligence.meta_learning_matrix import (
    MAX_EDGE_MULTIPLIER,
    MIN_EDGE_MULTIPLIER,
    MIN_META_SAMPLES,
    ConditionTuple,
    MetaEdgeRow,
    _reset_initialized_engines,
    bootstrap_from_oracle_predictions,
    bucket_fci,
    bucket_horizon,
    bucket_vol,
    build_condition_tuple,
    get_aggregate_weight_multiplier,
    get_edge_row,
    get_weight_multiplier,
    iter_condition_cube,
    rank_signals_by_edge,
    record_scored_prediction,
)


# ── FakeEngine ────────────────────────────────────────────────────────────


class _FakeConnCtx:
    def __init__(self, engine: "FakeEngine") -> None:
        self._engine = engine

    def __enter__(self) -> "FakeEngine":
        return self._engine

    def __exit__(self, *args: Any) -> None:
        return None


def _row_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        row["signal_source"],
        row["horizon_bucket"],
        row["liquidity_regime"],
        row["fci_bucket"],
        row["vol_regime"],
    )


class FakeEngine:
    """Minimal engine that services every SQL statement in meta_learning_matrix.

    Stores matrix cells in ``self.cells`` keyed by the 5-tuple
    (signal_source, horizon_bucket, liquidity_regime, fci_bucket, vol_regime).
    Inspects SQL by substring markers. Mirrors the shape of the
    walk-forward validator's FakeEngine (single object for
    ``connect()`` / ``begin()`` contexts).
    """

    def __init__(
        self,
        *,
        predictions: list[dict[str, Any]] | None = None,
        oracle_models: list[tuple[str, Any]] | None = None,
        fail_reads: bool = False,
        fail_writes: bool = False,
        fail_schema: bool = False,
    ) -> None:
        self.cells: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        self.predictions = predictions or []
        self.oracle_models = oracle_models or []
        self.fail_reads = fail_reads
        self.fail_writes = fail_writes
        self.fail_schema = fail_schema
        self.schema_calls = 0
        self.select_calls = 0
        self.insert_calls = 0
        self.update_calls = 0

    def connect(self) -> _FakeConnCtx:
        return _FakeConnCtx(self)

    def begin(self) -> _FakeConnCtx:
        return _FakeConnCtx(self)

    def execute(
        self, stmt: Any, params: dict[str, Any] | None = None
    ) -> "FakeResult":
        sql = str(stmt)
        params = params or {}
        sql_upper = sql.upper()

        if "CREATE TABLE" in sql_upper or "CREATE INDEX" in sql_upper:
            self.schema_calls += 1
            if self.fail_schema:
                raise RuntimeError("schema init failed")
            return FakeResult([])

        if "FROM oracle_models" in sql:
            return FakeResult(
                [(n, fams) for n, fams in self.oracle_models]
            )

        if "FROM oracle_predictions" in sql:
            rows = [
                (
                    p["id"],
                    p["ticker"],
                    p["created_at"],
                    p["expiry"],
                    p["confidence"],
                    p["verdict"],
                    p.get("model_name"),
                    p.get("signals"),
                    p.get("signal_contributions"),
                    p.get("model_weights"),
                )
                for p in self.predictions
            ]
            return FakeResult(rows)

        if "SELECT" in sql_upper and "meta_learning_matrix" in sql:
            self.select_calls += 1
            if self.fail_reads:
                raise RuntimeError("select failed")

            # rank_signals_by_edge — returns all cells matching condition
            if "ORDER BY n_firings" in sql or "ORDER BY" in sql_upper:
                hb = params.get("hb")
                lq = params.get("lq")
                fci = params.get("fci")
                vol = params.get("vol")
                min_n = int(params.get("min_n") or 0)
                matched: list[tuple[Any, ...]] = []
                for key, cell in self.cells.items():
                    if (
                        key[1] == hb
                        and key[2] == lq
                        and key[3] == fci
                        and key[4] == vol
                        and int(cell["n_firings"]) >= min_n
                    ):
                        matched.append(
                            (
                                cell["signal_source"],
                                cell["n_predictions"],
                                cell["n_firings"],
                                cell["n_hits"],
                                cell["sum_scaled_edge"],
                                cell["last_updated"],
                            )
                        )
                matched.sort(key=lambda r: (-int(r[2]), str(r[0])))
                return FakeResult(matched)

            # get_edge_row / record_scored_prediction SELECT
            key = (
                params.get("s"),
                params.get("hb"),
                params.get("lq"),
                params.get("fci"),
                params.get("vol"),
            )
            cell = self.cells.get(key)
            if cell is None:
                return FakeResult([])
            if "last_updated" in sql:
                return FakeResult(
                    [
                        (
                            cell["n_predictions"],
                            cell["n_firings"],
                            cell["n_hits"],
                            cell["sum_scaled_edge"],
                            cell["last_updated"],
                        )
                    ]
                )
            # record_scored_prediction inner SELECT
            return FakeResult(
                [
                    (
                        cell["n_predictions"],
                        cell["n_firings"],
                        cell["n_hits"],
                        cell["sum_scaled_edge"],
                    )
                ]
            )

        if "INSERT INTO meta_learning_matrix" in sql:
            self.insert_calls += 1
            if self.fail_writes:
                raise RuntimeError("insert failed")
            key = (
                params["s"],
                params["hb"],
                params["lq"],
                params["fci"],
                params["vol"],
            )
            self.cells[key] = {
                "signal_source": params["s"],
                "horizon_bucket": params["hb"],
                "liquidity_regime": params["lq"],
                "fci_bucket": params["fci"],
                "vol_regime": params["vol"],
                "n_predictions": 1,
                "n_firings": int(params["nf"]),
                "n_hits": float(params["nh"]),
                "sum_scaled_edge": float(params["sse"]),
                "last_updated": datetime.now(timezone.utc),
            }
            return FakeResult([])

        if "UPDATE meta_learning_matrix" in sql:
            self.update_calls += 1
            if self.fail_writes:
                raise RuntimeError("update failed")
            key = (
                params["s"],
                params["hb"],
                params["lq"],
                params["fci"],
                params["vol"],
            )
            cell = self.cells.get(key)
            if cell is not None:
                cell["n_predictions"] = int(params["np"])
                cell["n_firings"] = int(params["nf"])
                cell["n_hits"] = float(params["nh"])
                cell["sum_scaled_edge"] = float(params["sse"])
                cell["last_updated"] = datetime.now(timezone.utc)
            return FakeResult([])

        return FakeResult([])


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = list(rows)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


@pytest.fixture(autouse=True)
def _reset_schema_cache():
    _reset_initialized_engines()
    yield
    _reset_initialized_engines()


# ── bucket_horizon ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "days, expected",
    [
        (1, "SHORT"),
        (2, "SHORT"),
        (3, "MID"),
        (7, "MID"),
        (14, "MID"),
        (15, "LONG"),
        (30, "LONG"),
        (90, "LONG"),
        (365, "LONG"),
    ],
)
def test_bucket_horizon_truth_table(days, expected):
    assert bucket_horizon(days) == expected


def test_bucket_horizon_zero_and_negative_default_to_mid():
    assert bucket_horizon(0) == "MID"
    assert bucket_horizon(-5) == "MID"


def test_bucket_horizon_non_numeric_defaults_to_mid():
    assert bucket_horizon("banana") == "MID"  # type: ignore[arg-type]
    assert bucket_horizon(None) == "MID"  # type: ignore[arg-type]


# ── bucket_fci ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("TIGHT", "TIGHT"),
        ("tight", "TIGHT"),
        ("TIGHTENING", "TIGHT"),
        ("EASY", "EASY"),
        ("LOOSE", "EASY"),
        ("EXPANSION", "EASY"),
        ("NEUTRAL", "NEUTRAL"),
        ("unknown_regime", "NEUTRAL"),
        ("", "NEUTRAL"),
        (None, "NEUTRAL"),
    ],
)
def test_bucket_fci_truth_table(raw, expected):
    assert bucket_fci(raw) == expected


# ── bucket_vol ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "vix, expected",
    [
        (None, "NORMAL"),
        (-1.0, "NORMAL"),
        (0.0, "LOW"),
        (10.0, "LOW"),
        (14.99, "LOW"),
        (15.0, "NORMAL"),
        (20.0, "NORMAL"),
        (22.0, "NORMAL"),
        (22.01, "HIGH"),
        (25.0, "HIGH"),
        (float("nan"), "NORMAL"),
    ],
)
def test_bucket_vol_truth_table(vix, expected):
    assert bucket_vol(vix) == expected


def test_bucket_vol_non_numeric_defaults_to_normal():
    assert bucket_vol("banana") == "NORMAL"  # type: ignore[arg-type]


# ── build_condition_tuple ─────────────────────────────────────────────────


def test_build_condition_tuple_happy_path():
    ct = build_condition_tuple(
        horizon_days=7,
        liquidity_regime="EXPANSION",
        fci_regime="EASY",
        vix_level=18.0,
    )
    assert ct.horizon_bucket == "MID"
    assert ct.liquidity_regime == "EXPANSION"
    assert ct.fci_bucket == "EASY"
    assert ct.vol_regime == "NORMAL"


def test_build_condition_tuple_coerces_none_paths():
    ct = build_condition_tuple(
        horizon_days=30,
        liquidity_regime=None,
        fci_regime=None,
        vix_level=None,
    )
    assert ct.horizon_bucket == "LONG"
    # None regime collapses to NEUTRAL via _canonical_regime
    assert ct.liquidity_regime == "NEUTRAL"
    assert ct.fci_bucket == "NEUTRAL"
    assert ct.vol_regime == "NORMAL"


def test_build_condition_tuple_unknown_liquidity_falls_back_to_neutral():
    ct = build_condition_tuple(
        horizon_days=1,
        liquidity_regime="SOMETHING_ELSE",
        fci_regime="TIGHT",
        vix_level=30.0,
    )
    assert ct.liquidity_regime == "NEUTRAL"
    assert ct.horizon_bucket == "SHORT"
    assert ct.fci_bucket == "TIGHT"
    assert ct.vol_regime == "HIGH"


# ── ConditionTuple / MetaEdgeRow dataclass ────────────────────────────────


def test_condition_tuple_to_key_and_dict():
    ct = ConditionTuple("MID", "NEUTRAL", "EASY", "LOW")
    assert ct.to_key() == ("MID", "NEUTRAL", "EASY", "LOW")
    assert ct.to_dict() == {
        "horizon_bucket": "MID",
        "liquidity_regime": "NEUTRAL",
        "fci_bucket": "EASY",
        "vol_regime": "LOW",
    }


def test_condition_tuple_is_frozen():
    ct = ConditionTuple("MID", "NEUTRAL", "EASY", "LOW")
    with pytest.raises(FrozenInstanceError):
        ct.horizon_bucket = "SHORT"  # type: ignore[misc]


def test_meta_edge_row_is_frozen_and_to_dict_roundtrips():
    ct = ConditionTuple("MID", "NEUTRAL", "EASY", "LOW")
    row = MetaEdgeRow(
        signal_source="alpha",
        condition=ct,
        n_predictions=20,
        n_firings=20,
        hit_rate=0.65,
        scaled_edge=0.15,
        weight_multiplier=1.30,
        last_updated="2026-04-14T00:00:00+00:00",
    )
    with pytest.raises(FrozenInstanceError):
        row.hit_rate = 0.90  # type: ignore[misc]
    d = row.to_dict()
    assert d["signal_source"] == "alpha"
    assert d["condition"] == ct.to_dict()
    assert d["n_firings"] == 20
    assert d["hit_rate"] == pytest.approx(0.65)
    assert d["scaled_edge"] == pytest.approx(0.15)
    assert d["weight_multiplier"] == pytest.approx(1.30)


# ── record_scored_prediction ─────────────────────────────────────────────


def _record(engine, source, outcome, shapley_weight=0.5, **overrides):
    kwargs = {
        "signal_source": source,
        "shapley_weight": shapley_weight,
        "confidence": 0.7,
        "direction": "bullish",
        "outcome": outcome,
        "horizon_days": 7,
        "liquidity_regime": "NEUTRAL",
        "fci_regime": "NEUTRAL",
        "vix_level": 18.0,
    }
    kwargs.update(overrides)
    record_scored_prediction(engine, **kwargs)


def test_record_scored_prediction_creates_cell_on_first_call():
    engine = FakeEngine()
    _record(engine, "alpha", "hit")
    assert engine.insert_calls == 1
    assert engine.update_calls == 0
    cell_key = ("alpha", "MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    assert engine.cells[cell_key]["n_firings"] == 1
    assert engine.cells[cell_key]["n_hits"] == 1.0


def test_record_scored_prediction_updates_cell_on_second_call():
    engine = FakeEngine()
    _record(engine, "alpha", "hit")
    _record(engine, "alpha", "miss")
    assert engine.insert_calls == 1
    assert engine.update_calls == 1
    cell_key = ("alpha", "MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    cell = engine.cells[cell_key]
    assert cell["n_firings"] == 2
    assert cell["n_hits"] == 1.0  # one hit, one miss
    assert cell["n_predictions"] == 2


def test_record_scored_prediction_partial_scores_half():
    engine = FakeEngine()
    _record(engine, "alpha", "partial")
    _record(engine, "alpha", "partial")
    cell_key = ("alpha", "MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    assert engine.cells[cell_key]["n_hits"] == 1.0  # 0.5 + 0.5


def test_record_scored_prediction_skips_low_shapley_weight():
    engine = FakeEngine()
    _record(engine, "alpha", "hit", shapley_weight=0.05)
    assert engine.insert_calls == 0
    assert len(engine.cells) == 0


def test_record_scored_prediction_skips_empty_source():
    engine = FakeEngine()
    _record(engine, "", "hit")
    _record(engine, "   ", "hit")
    assert engine.insert_calls == 0


def test_record_scored_prediction_handles_bad_shapley_weight():
    engine = FakeEngine()
    _record(engine, "alpha", "hit", shapley_weight="banana")  # type: ignore[arg-type]
    assert engine.insert_calls == 0


def test_record_scored_prediction_never_raises_on_write_failure():
    engine = FakeEngine(fail_writes=True)
    # Should not raise even if INSERT blows up.
    _record(engine, "alpha", "hit")


def test_record_scored_prediction_splits_cells_by_condition():
    engine = FakeEngine()
    _record(engine, "alpha", "hit", horizon_days=1)  # SHORT
    _record(engine, "alpha", "hit", horizon_days=7)  # MID
    _record(engine, "alpha", "hit", horizon_days=30)  # LONG
    assert len(engine.cells) == 3


# ── get_edge_row / get_weight_multiplier ──────────────────────────────────


def _fill_cell(engine, source, condition, n_firings, n_hits):
    key = (
        source,
        condition.horizon_bucket,
        condition.liquidity_regime,
        condition.fci_bucket,
        condition.vol_regime,
    )
    engine.cells[key] = {
        "signal_source": source,
        "horizon_bucket": condition.horizon_bucket,
        "liquidity_regime": condition.liquidity_regime,
        "fci_bucket": condition.fci_bucket,
        "vol_regime": condition.vol_regime,
        "n_predictions": n_firings,
        "n_firings": n_firings,
        "n_hits": float(n_hits),
        "sum_scaled_edge": 0.0,
        "last_updated": datetime.now(timezone.utc),
    }


def test_get_edge_row_returns_none_when_cell_missing():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    assert get_edge_row(engine, signal_source="alpha", condition=ct) is None


def test_get_edge_row_strong_positive_edge_maps_to_1_30():
    """15 firings, 12 hits → hit_rate 0.80 → edge 0.30 →
    scaled_edge = 0.30 × min(15/20, 1) = 0.225 → multiplier 1.30"""
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=15, n_hits=12)
    row = get_edge_row(engine, signal_source="alpha", condition=ct)
    assert row is not None
    assert row.hit_rate == pytest.approx(0.80)
    assert row.scaled_edge == pytest.approx(0.225)
    assert row.weight_multiplier == pytest.approx(1.30)


def test_get_edge_row_mild_negative_edge_maps_to_0_70():
    """20 firings, 8 hits → hit_rate 0.40 → edge -0.10 → scaled_edge -0.10 → 0.70"""
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=20, n_hits=8)
    row = get_edge_row(engine, signal_source="alpha", condition=ct)
    assert row is not None
    assert row.hit_rate == pytest.approx(0.40)
    assert row.scaled_edge == pytest.approx(-0.10)
    assert row.weight_multiplier == pytest.approx(0.70)


def test_get_edge_row_strong_negative_edge_maps_to_0_40():
    """25 firings, 5 hits → hit_rate 0.20 → edge -0.30 → scaled_edge -0.30 → 0.40"""
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=25, n_hits=5)
    row = get_edge_row(engine, signal_source="alpha", condition=ct)
    assert row is not None
    assert row.weight_multiplier == pytest.approx(0.40)


def test_get_weight_multiplier_cold_start_returns_1():
    """n < MIN_META_SAMPLES → multiplier 1.0 even if scaled_edge looks strong"""
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=10, n_hits=10)  # 100% hit rate, too few samples
    mult = get_weight_multiplier(engine, signal_source="alpha", condition=ct)
    assert mult == 1.0


def test_get_weight_multiplier_missing_cell_returns_1():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    assert get_weight_multiplier(engine, signal_source="alpha", condition=ct) == 1.0


def test_get_weight_multiplier_db_failure_returns_1():
    engine = FakeEngine(fail_reads=True)
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    assert get_weight_multiplier(engine, signal_source="alpha", condition=ct) == 1.0


def test_get_weight_multiplier_is_clamped():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=20, n_hits=20)  # 100% hits
    mult = get_weight_multiplier(engine, signal_source="alpha", condition=ct)
    assert MIN_EDGE_MULTIPLIER <= mult <= MAX_EDGE_MULTIPLIER
    assert mult == pytest.approx(1.30)  # ladder caps strong edge at 1.30


# ── get_aggregate_weight_multiplier ───────────────────────────────────────


def test_aggregate_weight_multiplier_empty_contributions_returns_1():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    assert (
        get_aggregate_weight_multiplier(
            engine, signal_contributions={}, condition=ct
        )
        == 1.0
    )


def test_aggregate_weight_multiplier_no_calibrated_cells_returns_1():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    # None of these signals have any history, so aggregate should be 1.0
    mult = get_aggregate_weight_multiplier(
        engine,
        signal_contributions={"alpha": 0.5, "beta": 0.5},
        condition=ct,
    )
    assert mult == 1.0


def test_aggregate_weight_multiplier_harmonic_mean_two_signals():
    """Two firing signals, equal weights:
        alpha: multiplier 1.30 (15f, 12h — scaled_edge 0.225)
        beta:  multiplier 0.70 (20f, 8h  — scaled_edge -0.10)

    Weighted harmonic (equal weights 0.5/0.5):
        harmonic = total_w / sum(w_i / m_i)
                 = 1.0 / (0.5/1.30 + 0.5/0.70)
                 = 1.0 / (0.3846 + 0.7143)
                 = 1.0 / 1.0989
                 ≈ 0.910
    """
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=15, n_hits=12)
    _fill_cell(engine, "beta", ct, n_firings=20, n_hits=8)
    mult = get_aggregate_weight_multiplier(
        engine,
        signal_contributions={"alpha": 0.5, "beta": 0.5},
        condition=ct,
    )
    expected = 1.0 / (0.5 / 1.30 + 0.5 / 0.70)
    assert mult == pytest.approx(expected, abs=1e-4)


def test_aggregate_weight_multiplier_skips_low_shapley_weight():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=20, n_hits=16)  # mult 1.30
    _fill_cell(engine, "beta", ct, n_firings=20, n_hits=4)  # mult 0.40
    # beta's weight is below firing threshold, so it should be ignored.
    mult = get_aggregate_weight_multiplier(
        engine,
        signal_contributions={"alpha": 0.5, "beta": 0.01},
        condition=ct,
    )
    # Only alpha participates → multiplier == alpha's
    assert mult == pytest.approx(1.30)


def test_aggregate_weight_multiplier_skips_cold_cells():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=20, n_hits=16)  # mult 1.30
    _fill_cell(engine, "beta", ct, n_firings=5, n_hits=5)  # below MIN_META_SAMPLES
    mult = get_aggregate_weight_multiplier(
        engine,
        signal_contributions={"alpha": 0.5, "beta": 0.5},
        condition=ct,
    )
    # Only alpha counts; beta's cell is cold. Harmonic of one = 1.30.
    assert mult == pytest.approx(1.30)


def test_aggregate_weight_multiplier_clamped_to_bounds():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=20, n_hits=20)  # 1.30
    _fill_cell(engine, "beta", ct, n_firings=20, n_hits=20)  # 1.30
    mult = get_aggregate_weight_multiplier(
        engine,
        signal_contributions={"alpha": 0.5, "beta": 0.5},
        condition=ct,
    )
    assert MIN_EDGE_MULTIPLIER <= mult <= MAX_EDGE_MULTIPLIER


def test_aggregate_weight_multiplier_db_failure_returns_1():
    engine = FakeEngine(fail_reads=True)
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    mult = get_aggregate_weight_multiplier(
        engine,
        signal_contributions={"alpha": 0.5, "beta": 0.5},
        condition=ct,
    )
    assert mult == 1.0


# ── rank_signals_by_edge ──────────────────────────────────────────────────


def test_rank_signals_by_edge_sort_order():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=20, n_hits=16)  # scaled_edge +0.30
    _fill_cell(engine, "beta", ct, n_firings=20, n_hits=10)  # scaled_edge 0.0
    _fill_cell(engine, "gamma", ct, n_firings=20, n_hits=4)  # scaled_edge -0.30
    ranked = rank_signals_by_edge(engine, condition=ct)
    assert [r.signal_source for r in ranked] == ["alpha", "beta", "gamma"]


def test_rank_signals_by_edge_filters_thin_cells():
    engine = FakeEngine()
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct, n_firings=20, n_hits=15)
    _fill_cell(engine, "beta", ct, n_firings=5, n_hits=5)  # below MIN_META_SAMPLES
    ranked = rank_signals_by_edge(engine, condition=ct)
    sources = [r.signal_source for r in ranked]
    assert "alpha" in sources
    assert "beta" not in sources


def test_rank_signals_by_edge_db_failure_returns_empty():
    engine = FakeEngine(fail_reads=True)
    ct = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    assert rank_signals_by_edge(engine, condition=ct) == []


def test_rank_signals_by_edge_only_returns_matching_condition():
    engine = FakeEngine()
    ct_mid = ConditionTuple("MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    ct_long = ConditionTuple("LONG", "NEUTRAL", "NEUTRAL", "NORMAL")
    _fill_cell(engine, "alpha", ct_mid, n_firings=20, n_hits=15)
    _fill_cell(engine, "beta", ct_long, n_firings=20, n_hits=15)
    ranked = rank_signals_by_edge(engine, condition=ct_mid)
    assert [r.signal_source for r in ranked] == ["alpha"]


# ── bootstrap_from_oracle_predictions ────────────────────────────────────


def _make_prediction(
    *,
    pid: str,
    horizon_days: int = 7,
    verdict: str = "hit",
    confidence: float = 0.72,
    contributions: dict[str, float] | None = None,
    regime: str = "NEUTRAL",
    fci: str = "NEUTRAL",
    vix: float = 18.0,
) -> dict[str, Any]:
    created = datetime(2026, 3, 15, tzinfo=timezone.utc)
    return {
        "id": pid,
        "ticker": "SPY",
        "created_at": created,
        "expiry": created + timedelta(days=horizon_days),
        "confidence": confidence,
        "verdict": verdict,
        "model_name": None,
        "signals": {
            "direction": "bullish",
            "regime": regime,
            "fci_regime": fci,
            "vix_level": vix,
        },
        "signal_contributions": contributions or {"alpha": 0.5, "beta": 0.5},
        "model_weights": None,
    }


def test_bootstrap_replays_distributed_predictions():
    preds = []
    for i in range(30):
        preds.append(
            _make_prediction(
                pid=f"p{i}",
                verdict="hit" if i % 2 == 0 else "miss",
            )
        )
    engine = FakeEngine(predictions=preds)
    n = bootstrap_from_oracle_predictions(engine, days=30)
    assert n == 30
    # Both signals should have accumulated firings in the MID/NEUTRAL cell.
    cell_alpha = engine.cells.get(
        ("alpha", "MID", "NEUTRAL", "NEUTRAL", "NORMAL")
    )
    assert cell_alpha is not None
    assert cell_alpha["n_firings"] == 30


def test_bootstrap_empty_oracle_returns_zero():
    engine = FakeEngine(predictions=[])
    assert bootstrap_from_oracle_predictions(engine, days=365) == 0


def test_bootstrap_skips_invalid_verdict():
    preds = [_make_prediction(pid="p1", verdict="unknown")]
    engine = FakeEngine(predictions=preds)
    assert bootstrap_from_oracle_predictions(engine, days=30) == 0


def test_bootstrap_handles_db_failure_returns_zero():
    engine = FakeEngine(fail_reads=True, predictions=[])
    assert bootstrap_from_oracle_predictions(engine, days=30) == 0


def test_bootstrap_routes_predictions_by_condition():
    preds = [
        _make_prediction(
            pid="p1", regime="EXPANSION_STRONG", fci="EASY", vix=12.0
        ),
        _make_prediction(
            pid="p2", regime="CRISIS", fci="TIGHT", vix=35.0
        ),
    ]
    engine = FakeEngine(predictions=preds)
    assert bootstrap_from_oracle_predictions(engine, days=30) == 2
    # Two distinct cells for alpha (one per condition tuple).
    alpha_cells = [k for k in engine.cells if k[0] == "alpha"]
    assert len(alpha_cells) == 2
    conditions = {k[1:] for k in alpha_cells}
    assert ("MID", "EXPANSION_STRONG", "EASY", "LOW") in conditions
    assert ("MID", "CRISIS", "TIGHT", "HIGH") in conditions


# ── _ensure_schema idempotency ───────────────────────────────────────────


def test_ensure_schema_is_idempotent():
    engine = FakeEngine()
    # First record triggers schema init (3 DDL statements).
    _record(engine, "alpha", "hit")
    first_schema_calls = engine.schema_calls
    assert first_schema_calls >= 1
    # Subsequent record_scored_prediction calls should NOT hit DDL again.
    _record(engine, "alpha", "miss")
    _record(engine, "beta", "hit")
    assert engine.schema_calls == first_schema_calls


def test_ensure_schema_failure_does_not_raise():
    engine = FakeEngine(fail_schema=True)
    # Should fall through to the cold-start path without raising.
    _record(engine, "alpha", "hit")


def test_reset_initialized_engines_reinitialises_schema():
    engine = FakeEngine()
    _record(engine, "alpha", "hit")
    first_schema_calls = engine.schema_calls
    _reset_initialized_engines()
    _record(engine, "alpha", "miss")
    assert engine.schema_calls > first_schema_calls


# ── iter_condition_cube ──────────────────────────────────────────────────


def test_iter_condition_cube_cardinality_is_135():
    cube = list(iter_condition_cube())
    assert len(cube) == 3 * 5 * 3 * 3  # 135


def test_iter_condition_cube_all_unique():
    cube = list(iter_condition_cube())
    keys = {ct.to_key() for ct in cube}
    assert len(keys) == len(cube)


# ── Cross-module import proof ─────────────────────────────────────────────


def test_imports_reuse_upstream_helpers():
    """Guard: every reused helper must actually come from its upstream
    module. Breaks loudly if someone copies the constants inline."""
    from features import per_signal_brier as psb
    from oracle import regime_router as rr
    from scripts import bootstrap_per_signal_brier as bpsb

    assert mlm.CANONICAL_HORIZONS is psb.CANONICAL_HORIZONS
    assert mlm.MIN_CALIBRATED_SAMPLES is psb.MIN_CALIBRATED_SAMPLES
    assert mlm._canonical_regime is rr._canonical_regime
    assert mlm.CANONICAL_REGIMES is rr.REGIME_STATES
    assert mlm.verdict_to_outcome is bpsb.verdict_to_outcome
    assert (
        mlm.extract_signal_contributions
        is bpsb.extract_signal_contributions
    )
    assert mlm._coerce_horizon_days is bpsb._coerce_horizon_days
    assert mlm.ORACLE_AGGREGATE_SOURCE == bpsb.ORACLE_AGGREGATE_SOURCE
