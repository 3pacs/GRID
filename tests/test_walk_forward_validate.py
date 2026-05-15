"""Tests for ``scripts/walk_forward_validate.py``.

Uses a minimal FakeEngine that serves in-memory rows so the walk-forward
harness can be exercised end-to-end without touching a real database.
Covers every pure helper plus the happy/edge paths of the DB-touching
functions (reconstruct, walk_forward, persist).

Critical invariant: the lookahead filter in
``_reconstruct_historical_scorecards`` must strictly exclude scorecards
updated AFTER the prediction's timestamp — ``test_reconstruct_no_lookahead``
is the regression guard.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import pytest

from features.per_signal_brier import SignalScorecard
from scripts import walk_forward_validate as wfv


# ── FakeEngine ────────────────────────────────────────────────────────────


class _FakeConnCtx:
    """Context manager emulation around ``engine.connect()``."""

    def __init__(self, engine: "FakeEngine") -> None:
        self._engine = engine

    def __enter__(self) -> "FakeEngine":
        return self._engine

    def __exit__(self, *args: Any) -> None:
        return None


class FakeEngine:
    """Minimal engine that services the two queries the walker issues.

    We detect the query by inspecting substring markers in the SQL text.
    Keeps the test file free of SQLAlchemy Dialect gymnastics.
    """

    def __init__(
        self,
        *,
        predictions: list[dict[str, Any]] | None = None,
        scorecard_rows: list[tuple[Any, ...]] | None = None,
        models_lookup_rows: list[tuple[str, list[str]]] | None = None,
    ) -> None:
        self.predictions = predictions or []
        self.scorecard_rows = scorecard_rows or []
        self.models_lookup_rows = models_lookup_rows or []
        self.inserts: list[dict[str, Any]] = []

    def connect(self) -> _FakeConnCtx:
        return _FakeConnCtx(self)

    def begin(self) -> _FakeConnCtx:
        return _FakeConnCtx(self)

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> "FakeResult":
        sql = str(stmt)
        params = params or {}
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
        if "per_signal_brier_history" in sql and "SELECT" in sql.upper():
            as_of = params.get("as_of")
            rows = [
                r
                for r in self.scorecard_rows
                if as_of is None or (r[6] is not None and r[6] <= as_of)
            ]
            return FakeResult(rows)
        if "FROM oracle_models" in sql:
            return FakeResult([(n, fams) for n, fams in self.models_lookup_rows])
        if "INSERT INTO backtest_results" in sql:
            self.inserts.append(params)
            return FakeResult([(1,)])
        if "CREATE TABLE" in sql or "CREATE INDEX" in sql:
            return FakeResult([])
        return FakeResult([])


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = list(rows)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def mappings(self) -> "FakeResult":
        return self

    def all(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


def _make_prediction(
    *,
    pid: str = "p1",
    ticker: str = "SPY",
    created_at: datetime | None = None,
    horizon_days: int = 7,
    confidence: float = 0.72,
    verdict: str = "hit",
    direction: str = "bullish",
    signal_contributions: dict[str, float] | None = None,
) -> dict[str, Any]:
    created = created_at or datetime(2026, 3, 15, tzinfo=timezone.utc)
    return {
        "id": pid,
        "ticker": ticker,
        "created_at": created,
        "expiry": created + timedelta(days=horizon_days),
        "confidence": confidence,
        "verdict": verdict,
        "model_name": None,
        "signals": {
            "direction": direction,
            "regime": "GROWTH",
            "fci_regime": "NEUTRAL",
            "confidence_lower": max(0.0, confidence - 0.08),
            "confidence_upper": min(1.0, confidence + 0.08),
        },
        "signal_contributions": signal_contributions or {"alpha": 0.5, "beta": 0.5},
        "model_weights": None,
    }


def _make_scorecard_row(
    source: str,
    horizon: int,
    last_updated: datetime,
    count: int = 40,
    brier: float = 0.12,
) -> tuple[Any, ...]:
    return (source, horizon, count, brier, 0.12, int(count * 0.6), last_updated)


# ── verdict_to_outcome ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "verdict, expected",
    [
        ("hit", 1.0),
        ("partial", 0.5),
        ("miss", 0.0),
        ("unknown", 0.0),
        ("", 0.0),
        (None, 0.0),
    ],
)
def test_verdict_to_outcome_known_and_unknown(verdict, expected):
    assert wfv.verdict_to_outcome(verdict) == expected


# ── compute_sharpe ────────────────────────────────────────────────────────


def test_compute_sharpe_happy_path_positive():
    sharpe = wfv.compute_sharpe([0.01, 0.02, -0.005])
    assert sharpe > 0.0
    assert sharpe == sharpe  # not NaN


def test_compute_sharpe_empty_returns_zero():
    assert wfv.compute_sharpe([]) == 0.0


def test_compute_sharpe_zero_std_returns_zero():
    # All-identical returns → stdev = 0 → defensive zero.
    assert wfv.compute_sharpe([0.01, 0.01, 0.01, 0.01]) == 0.0


# ── compute_max_drawdown ──────────────────────────────────────────────────


def test_compute_max_drawdown_all_positive_zero():
    assert wfv.compute_max_drawdown([0.01, 0.02, 0.005]) == 0.0


def test_compute_max_drawdown_dip_detected():
    # 5% up, then 10% down → drawdown = 0.10 / 1.05 ≈ 0.0952
    dd = wfv.compute_max_drawdown([0.05, -0.10, 0.02])
    assert dd > 0.08
    assert dd < 0.11


def test_compute_max_drawdown_empty_zero():
    assert wfv.compute_max_drawdown([]) == 0.0


# ── classify_hit ──────────────────────────────────────────────────────────


def test_classify_hit_directional_truth_table():
    assert wfv.classify_hit("bullish", "hit") is True
    assert wfv.classify_hit("bullish", "miss") is False
    assert wfv.classify_hit("bearish", "hit") is True
    assert wfv.classify_hit("bearish", "miss") is False
    assert wfv.classify_hit("bullish", "partial") is True


# ── build_time_frozen_provenance ──────────────────────────────────────────


def test_build_time_frozen_provenance_happy_path():
    row = _make_prediction()
    scorecard = SignalScorecard(
        signal_source="alpha",
        horizon_days=7,
        scored_count=50,
        running_brier=0.1,
        running_ece=0.1,
        hit_rate=0.6,
        last_updated=datetime(2026, 3, 10, tzinfo=timezone.utc),
        is_calibrated=True,
        conviction_weight=1.3,
    )
    report = wfv.build_time_frozen_provenance(row, {"alpha": scorecard})
    assert report.ticker == "SPY"
    assert report.horizon_days == 7
    assert report.direction == "bullish"
    assert len(report.signal_evidence) == 2
    classes = {ev.signal_source: ev.classification for ev in report.signal_evidence}
    assert classes["alpha"] == "strong"
    assert classes["beta"] == "no_history"


def test_build_time_frozen_provenance_empty_scorecards_cold_start():
    row = _make_prediction()
    report = wfv.build_time_frozen_provenance(row, {})
    assert len(report.signal_evidence) == 2
    assert all(ev.classification == "no_history" for ev in report.signal_evidence)
    # With all cold-start signals, aggregate is the weight-sum × neutral = 1.0
    # (fragility=1.0, disagreement=0.0, red_team=0.0, fudge=0).
    assert pytest.approx(report.aggregate_conviction, abs=1e-9) == 1.0


# ── aggregate_per_verdict_stats ───────────────────────────────────────────


def _make_trade(verdict: str, hit: bool, return_: float = 0.01, **kw: Any) -> wfv.BacktestTrade:
    defaults = dict(
        prediction_id=kw.pop("prediction_id", "x"),
        ticker=kw.pop("ticker", "T"),
        prediction_date=kw.pop("prediction_date", "2026-01-01T00:00:00+00:00"),
        verdict=verdict,
        aggregate_conviction=kw.pop("aggregate_conviction", 1.0),
        robustness_label=kw.pop("robustness_label", "moderate"),
        robustness_score=kw.pop("robustness_score", 0.8),
        oracle_confidence=kw.pop("oracle_confidence", 0.7),
        oracle_direction=kw.pop("oracle_direction", "bullish"),
        outcome_verdict=kw.pop("outcome_verdict", "hit" if hit else "miss"),
        realized_return=return_,
        horizon_days=kw.pop("horizon_days", 7),
        hit=hit,
    )
    defaults.update(kw)
    return wfv.BacktestTrade(**defaults)


def test_aggregate_per_verdict_stats_high_bucket_3_of_5_hits():
    trades = [
        _make_trade("high", True, 0.02),
        _make_trade("high", True, 0.02),
        _make_trade("high", True, 0.02),
        _make_trade("high", False, -0.02),
        _make_trade("high", False, -0.02),
    ]
    stats = wfv.aggregate_per_verdict_stats(trades)
    high = stats["high"]
    assert high.n_trades == 5
    assert pytest.approx(high.hit_rate, abs=1e-9) == 0.6


def test_aggregate_per_verdict_stats_empty_bucket_safe_defaults():
    stats = wfv.aggregate_per_verdict_stats([])
    for verdict in wfv.CONVICTION_BUCKETS:
        s = stats[verdict]
        assert s.n_trades == 0
        # No NaN / no infinity — all floats.
        for field_name in ("hit_rate", "mean_return", "std_return", "sharpe", "max_drawdown"):
            val = getattr(s, field_name)
            assert isinstance(val, float)
            assert val == val  # not NaN
            assert val == 0.0


# ── compute_confusion_matrix ──────────────────────────────────────────────


def test_compute_confusion_matrix_counts_by_verdict_and_outcome():
    trades = [
        _make_trade("high", True, outcome_verdict="hit"),
        _make_trade("high", True, outcome_verdict="hit"),
        _make_trade("high", False, outcome_verdict="miss"),
        _make_trade("medium", True, outcome_verdict="hit"),
    ]
    m = wfv.compute_confusion_matrix(trades)
    assert m["high|hit"] == 2
    assert m["high|miss"] == 1
    assert m["medium|hit"] == 1


# ── measure_stress_test_calibration ───────────────────────────────────────


def test_measure_stress_test_calibration_positive_lift():
    # Robust trades hit more often than fragile trades → positive lift.
    trades = [
        _make_trade("high", True, robustness_label="robust"),
        _make_trade("high", True, robustness_label="robust"),
        _make_trade("high", True, robustness_label="robust"),
        _make_trade("high", False, robustness_label="fragile"),
        _make_trade("high", False, robustness_label="fragile"),
        _make_trade("high", True, robustness_label="fragile"),
    ]
    cal = wfv.measure_stress_test_calibration(trades)
    # Fragile: 2/3 failed = 0.667; Robust: 0/3 failed = 0.0; lift = 0.667
    assert cal["fragile_failure_rate"] > cal["robust_failure_rate"]
    assert cal["lift"] > 0.0


def test_measure_stress_test_calibration_backwards_negative_lift():
    # Fragile hits more than robust → stress test is calibrated backwards.
    trades = [
        _make_trade("high", False, robustness_label="robust"),
        _make_trade("high", False, robustness_label="robust"),
        _make_trade("high", True, robustness_label="fragile"),
        _make_trade("high", True, robustness_label="fragile"),
    ]
    cal = wfv.measure_stress_test_calibration(trades)
    assert cal["lift"] < 0.0


def test_measure_stress_test_calibration_empty_fragile_returns_none_lift():
    # When n_fragile == 0 the lift is undefined — must NOT report
    # `lift = -robust_failure_rate`, which historically masqueraded as
    # "stress test is inverted" in 2026-05-11 backtest reports.
    trades = [
        _make_trade("high", False, robustness_label="robust"),
        _make_trade("high", False, robustness_label="robust"),
        _make_trade("high", True, robustness_label="robust"),
        _make_trade("high", True, robustness_label="moderate"),
    ]
    cal = wfv.measure_stress_test_calibration(trades)
    assert cal["n_fragile"] == 0
    assert cal["n_robust"] == 3
    assert cal["fragile_failure_rate"] is None
    assert cal["lift"] is None, (
        "lift must be None when one bucket is empty — a real-valued lift "
        "with an empty fragile bucket is the 'stress test inverted' bug"
    )
    # robust_failure_rate is still measurable on its own
    assert cal["robust_failure_rate"] == pytest.approx(2 / 3)


def test_measure_stress_test_calibration_empty_robust_returns_none_lift():
    # Symmetric: empty robust bucket also blocks lift.
    trades = [
        _make_trade("high", False, robustness_label="fragile"),
        _make_trade("high", True, robustness_label="fragile"),
        _make_trade("high", True, robustness_label="moderate"),
    ]
    cal = wfv.measure_stress_test_calibration(trades)
    assert cal["n_fragile"] == 2
    assert cal["n_robust"] == 0
    assert cal["robust_failure_rate"] is None
    assert cal["lift"] is None


# ── _reconstruct_historical_scorecards (NO LOOKAHEAD) ─────────────────────


def test_reconstruct_no_lookahead_filters_future_rows():
    as_of = datetime(2026, 3, 1, tzinfo=timezone.utc)
    earlier = datetime(2026, 2, 1, tzinfo=timezone.utc)
    later = datetime(2026, 4, 1, tzinfo=timezone.utc)
    engine = FakeEngine(
        scorecard_rows=[
            _make_scorecard_row("alpha", 7, earlier),
            _make_scorecard_row("beta", 7, later),       # must NOT appear
            _make_scorecard_row("gamma", 30, earlier),   # wrong horizon
        ]
    )
    out = wfv._reconstruct_historical_scorecards(engine, as_of=as_of, horizon_days=7)
    assert "alpha" in out
    assert "beta" not in out, "Lookahead leak — future scorecard returned"
    assert "gamma" not in out


def test_reconstruct_snaps_raw_horizon_to_canonical_bucket():
    # features.per_signal_brier._canonical_horizon snaps 3/4/5d → 7d when
    # writing per_signal_brier_history rows. The reader MUST snap too —
    # otherwise oracle predictions with horizon=3..6 day never find their
    # scorecards, which made the 2026-05-11 backtest produce identical
    # output before/after the bootstrap (the bug: filter rejected every
    # bootstrap row because they live at canonical h=7).
    as_of = datetime(2026, 3, 1, tzinfo=timezone.utc)
    earlier = datetime(2026, 2, 1, tzinfo=timezone.utc)
    engine = FakeEngine(
        scorecard_rows=[
            _make_scorecard_row("alpha", 7, earlier),   # canonical 7d bucket
            _make_scorecard_row("beta", 1, earlier),    # canonical 1d bucket
            _make_scorecard_row("gamma", 30, earlier),  # canonical 30d
        ]
    )
    # Prediction with horizon=4 days → canonical 7d → "alpha" should be hit.
    out_4d = wfv._reconstruct_historical_scorecards(engine, as_of=as_of, horizon_days=4)
    assert "alpha" in out_4d, (
        "horizon_days=4 must snap to canonical 7d bucket and find the "
        "alpha scorecard — see features.per_signal_brier._canonical_horizon"
    )
    assert "beta" not in out_4d
    assert "gamma" not in out_4d
    # Sanity: horizon=2 days → canonical 1d bucket → "beta".
    out_2d = wfv._reconstruct_historical_scorecards(engine, as_of=as_of, horizon_days=2)
    assert "beta" in out_2d
    assert "alpha" not in out_2d


# ── walk_forward (end-to-end against FakeEngine) ──────────────────────────


def test_walk_forward_happy_path_generates_trades():
    preds = [
        _make_prediction(pid=f"p{i}", verdict="hit" if i % 2 == 0 else "miss")
        for i in range(5)
    ]
    engine = FakeEngine(predictions=preds)
    report = wfv.walk_forward(engine, days=365, dry_run=True)
    assert report.total_predictions_walked == 5
    assert report.trades_generated == 5
    assert "high" in report.verdict_stats
    assert sum(s.n_trades for s in report.verdict_stats.values()) == 5


def test_walk_forward_empty_oracle_predictions_valid_narrative():
    engine = FakeEngine(predictions=[])
    report = wfv.walk_forward(engine, days=365, dry_run=True)
    assert report.total_predictions_walked == 0
    assert report.trades_generated == 0
    assert report.narrative  # non-empty
    # Narrative wording was tightened — accept either the legacy phrasing
    # or the current "0 scored predictions matched the last-Nd query" form.
    assert (
        "no scored oracle_predictions" in report.narrative
        or "0 scored predictions" in report.narrative
    )


def test_walk_forward_dry_run_does_not_persist():
    preds = [_make_prediction(pid="p1")]
    engine = FakeEngine(predictions=preds)
    with patch.object(wfv, "persist_report") as persist:
        wfv.walk_forward(engine, days=30, dry_run=True)
    persist.assert_not_called()


def test_walk_forward_limit_caps_walk():
    preds = [_make_prediction(pid=f"p{i}") for i in range(20)]
    engine = FakeEngine(predictions=preds)
    report = wfv.walk_forward(engine, days=365, limit=5, dry_run=True)
    assert report.total_predictions_walked == 5
    assert report.trades_generated == 5


def test_backtest_report_narrative_non_empty_on_empty_walk():
    engine = FakeEngine(predictions=[])
    report = wfv.walk_forward(engine, days=7, dry_run=True)
    assert isinstance(report.narrative, str)
    assert len(report.narrative) > 0


# ── PIT price replay helpers ──────────────────────────────────────────────


class _FakePITStore:
    """Minimal PITStore stand-in backed by an in-memory dict keyed by
    (feature_id, date). Returns an empty DataFrame when the key is missing
    so the fallback path is exercised.
    """

    def __init__(self, prices: dict[tuple[int, Any], float]) -> None:
        self._prices = prices
        self.calls: list[tuple[int, Any]] = []

    def get_pit(self, feature_ids, as_of_date, vintage_policy="LATEST_AS_OF"):
        import pandas as pd

        rows = []
        for fid in feature_ids:
            self.calls.append((fid, as_of_date))
            price = self._prices.get((fid, as_of_date))
            if price is not None:
                rows.append(
                    {
                        "feature_id": fid,
                        "obs_date": as_of_date,
                        "value": price,
                        "release_date": as_of_date,
                        "vintage_date": as_of_date,
                    }
                )
        return pd.DataFrame(rows)


class _FeatureRegistryEngine(FakeEngine):
    """FakeEngine that also responds to the ticker → feature_id lookup
    the PIT replay path issues. ``feature_map`` maps lower-cased
    ``feature_registry.name`` entries to their integer id.
    """

    def __init__(self, *, feature_map: dict[str, int] | None = None, **kw: Any) -> None:
        super().__init__(**kw)
        self.feature_map = feature_map or {}

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if "FROM feature_registry" in sql:
            name = (params or {}).get("name", "")
            fid = self.feature_map.get(name)
            return FakeResult([(fid,)] if fid is not None else [])
        return super().execute(stmt, params)


def test_resolve_ticker_feature_id_tries_name_patterns_and_caches():
    engine = _FeatureRegistryEngine(feature_map={"tsm_full": 42})
    cache: dict[str, int | None] = {}
    fid = wfv._resolve_ticker_feature_id(engine, "TSM", cache)
    assert fid == 42
    # Cached — second call must not re-query (mutate map to prove cache).
    engine.feature_map.clear()
    assert wfv._resolve_ticker_feature_id(engine, "tsm", cache) == 42


def test_resolve_ticker_feature_id_missing_caches_none():
    engine = _FeatureRegistryEngine(feature_map={})
    cache: dict[str, int | None] = {}
    assert wfv._resolve_ticker_feature_id(engine, "ZZZZ", cache) is None
    assert cache["zzzz"] is None


def test_pit_price_on_or_before_returns_value():
    d = datetime(2026, 3, 15).date()
    store = _FakePITStore({(42, d): 215.5})
    assert wfv._pit_price_on_or_before(store, 42, d) == 215.5


def test_pit_price_on_or_before_empty_returns_none():
    store = _FakePITStore({})
    assert wfv._pit_price_on_or_before(store, 42, datetime(2026, 3, 15).date()) is None


def test_realized_return_from_pit_bullish_up():
    entry = datetime(2026, 3, 15).date()
    exit_ = datetime(2026, 3, 22).date()
    store = _FakePITStore({(42, entry): 100.0, (42, exit_): 105.0})
    r = wfv._realized_return_from_pit(store, 42, "bullish", entry, exit_)
    assert r is not None
    assert pytest.approx(r, rel=1e-6) == 0.05


def test_realized_return_from_pit_bearish_down_positive():
    # A -5% move on a bearish call is a +5% P&L.
    entry = datetime(2026, 3, 15).date()
    exit_ = datetime(2026, 3, 22).date()
    store = _FakePITStore({(42, entry): 100.0, (42, exit_): 95.0})
    r = wfv._realized_return_from_pit(store, 42, "bearish", entry, exit_)
    assert r is not None
    assert pytest.approx(r, rel=1e-6) == 0.05


def test_realized_return_from_pit_missing_entry_returns_none():
    entry = datetime(2026, 3, 15).date()
    exit_ = datetime(2026, 3, 22).date()
    store = _FakePITStore({(42, exit_): 105.0})  # no entry
    assert wfv._realized_return_from_pit(store, 42, "bullish", entry, exit_) is None


def test_realized_return_from_pit_zero_entry_returns_none():
    entry = datetime(2026, 3, 15).date()
    exit_ = datetime(2026, 3, 22).date()
    store = _FakePITStore({(42, entry): 0.0, (42, exit_): 1.0})
    # _pit_price_on_or_before rejects non-positive entry → None propagates.
    assert wfv._realized_return_from_pit(store, 42, "bullish", entry, exit_) is None


def test_walk_forward_pit_path_overrides_proxy_when_feature_resolves():
    """End-to-end: when feature_registry resolves + PITStore has prices,
    the walker uses the PIT-derived return instead of the ±2% proxy.
    """
    created = datetime(2026, 3, 15, tzinfo=timezone.utc)
    preds = [_make_prediction(pid="p1", ticker="TSM", created_at=created, verdict="miss")]
    engine = _FeatureRegistryEngine(
        predictions=preds,
        feature_map={"tsm_full": 42},
    )
    entry_date = created.date()
    exit_date = entry_date + timedelta(days=7)
    fake_store = _FakePITStore({(42, entry_date): 100.0, (42, exit_date): 110.0})
    with patch.object(wfv, "PITStore", return_value=fake_store):
        report = wfv.walk_forward(engine, days=365, dry_run=True)
    assert report.trades_generated == 1
    # +10% on a bullish call — not ±2% from the outcome proxy.
    rr = [s for s in report.verdict_stats.values() if s.n_trades == 1][0].mean_return
    assert pytest.approx(rr, rel=1e-6) == 0.10


def test_walk_forward_falls_back_to_proxy_when_feature_missing():
    """When the ticker has no feature_registry entry, realized returns
    come from the outcome proxy (±2%), not PIT.
    """
    created = datetime(2026, 3, 15, tzinfo=timezone.utc)
    preds = [_make_prediction(pid="p1", ticker="ZZZZ", created_at=created, verdict="hit")]
    engine = _FeatureRegistryEngine(predictions=preds, feature_map={})
    report = wfv.walk_forward(engine, days=365, dry_run=True)
    assert report.trades_generated == 1
    rr = [s for s in report.verdict_stats.values() if s.n_trades == 1][0].mean_return
    assert pytest.approx(rr, abs=1e-9) == 0.02
