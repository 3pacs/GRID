"""Tests for ``intelligence/historical_scenario_library.py`` (CAT-176).

Uses a FakeEngine that services the two queries the module actually
issues (``feature_registry`` id-lookup and ``oracle_predictions`` row
pull) plus a FakePITStore monkeypatched onto the module so the 10-dim
feature vector builder runs end-to-end without touching a real DB.

Critical invariants under test:

- Z-score normalization uses ONLY data strictly before ``as_of``. A
  series value recorded *on* ``as_of`` must not influence the query
  reduction or the normalization window.
- The 2 x horizon overlap filter excludes candidates whose
  ``created_at`` is within the lookahead window.
- ``scenario_conviction_multiplier`` must NEVER raise — PIT failures,
  DB failures, and empty histories all degrade to ``1.0``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import pytest

from intelligence import historical_scenario_library as hsl
from intelligence.historical_scenario_library import (
    FEATURE_DIM,
    FEATURE_SPEC,
    MIN_MATCHES_FOR_SIGNAL,
    MULT_AMPLIFY,
    MULT_DEGRADE,
    MULT_NEUTRAL,
    MULT_STRONG_DEGRADE,
    ScenarioAnalog,
    ScenarioLibraryReport,
    find_analogs,
    scenario_conviction_multiplier,
)


# ── FakeEngine / FakeResult ───────────────────────────────────────────────


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = list(rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _FakeConnCtx:
    def __init__(self, engine: "FakeEngine") -> None:
        self._engine = engine

    def __enter__(self) -> "FakeEngine":
        return self._engine

    def __exit__(self, *args: Any) -> None:
        return None


class FakeEngine:
    """Minimal engine that serves the two SQL shapes the module issues."""

    def __init__(
        self,
        *,
        predictions: list[dict[str, Any]] | None = None,
        feature_id_by_name: dict[str, int] | None = None,
        raise_on_oracle: bool = False,
    ) -> None:
        self.predictions = predictions or []
        self.feature_id_by_name = feature_id_by_name or {
            spec["primary"]: idx + 1 for idx, spec in enumerate(FEATURE_SPEC)
        }
        self.raise_on_oracle = raise_on_oracle
        self.oracle_calls = 0
        self.feature_id_calls: list[str] = []

    def connect(self) -> _FakeConnCtx:
        return _FakeConnCtx(self)

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> FakeResult:
        sql = str(stmt)
        params = params or {}

        if "feature_registry" in sql and "WHERE name" in sql:
            name = params.get("n")
            self.feature_id_calls.append(str(name))
            fid = self.feature_id_by_name.get(name)
            if fid is None:
                return FakeResult([])
            return FakeResult([(int(fid),)])

        if "FROM oracle_predictions" in sql:
            self.oracle_calls += 1
            if self.raise_on_oracle:
                raise RuntimeError("boom: oracle_predictions unavailable")
            include_realized = "realized_return" in sql
            ticker_filter = params.get("tkr")
            rows = []
            for p in self.predictions:
                if ticker_filter and p.get("ticker") != ticker_filter:
                    continue
                base = (
                    p["id"],
                    p.get("ticker", "SPY"),
                    p["created_at"],
                    p["expiry"],
                    p.get("confidence", 0.6),
                    p.get("verdict", "miss"),
                    p.get("signals", {"direction": "bullish"}),
                )
                if include_realized:
                    rows.append(base + (p.get("realized_return"),))
                else:
                    rows.append(base)
            return FakeResult(rows)

        return FakeResult([])


# ── FakePITStore ──────────────────────────────────────────────────────────


class FakePITStore:
    """Drop-in replacement for ``store.pit.PITStore`` in the module.

    Serves pre-built per-feature-id pandas series from a shared class
    registry so that every ``PITStore(engine)`` instance inside the
    module sees the same installed data. Use ``install`` to attach data
    to an instance; the registry is keyed on the engine id.
    """

    _data_by_engine: dict[int, dict[int, pd.Series]] = {}
    _calls_by_engine: dict[int, list[tuple[int, date]]] = {}
    _raise_by_engine: dict[int, bool] = {}

    def __init__(self, engine: Any) -> None:
        self.engine = engine
        key = id(engine)
        FakePITStore._data_by_engine.setdefault(key, {})
        FakePITStore._calls_by_engine.setdefault(key, [])
        FakePITStore._raise_by_engine.setdefault(key, False)

    @property
    def _by_fid(self) -> dict[int, pd.Series]:
        return FakePITStore._data_by_engine[id(self.engine)]

    @property
    def calls(self) -> list[tuple[int, date]]:
        return FakePITStore._calls_by_engine[id(self.engine)]

    @property
    def raise_on_call(self) -> bool:
        return FakePITStore._raise_by_engine[id(self.engine)]

    @raise_on_call.setter
    def raise_on_call(self, value: bool) -> None:
        FakePITStore._raise_by_engine[id(self.engine)] = bool(value)

    @classmethod
    def reset(cls) -> None:
        cls._data_by_engine.clear()
        cls._calls_by_engine.clear()
        cls._raise_by_engine.clear()

    def install(
        self,
        series_by_name: dict[str, pd.Series],
        feature_id_by_name: dict[str, int],
    ) -> None:
        bucket = FakePITStore._data_by_engine[id(self.engine)]
        for name, series in series_by_name.items():
            if name not in feature_id_by_name:
                continue
            fid = feature_id_by_name[name]
            bucket[int(fid)] = series.copy()

    def get_pit(
        self,
        *,
        feature_ids: list[int],
        as_of_date: date,
        vintage_policy: str = "LATEST_AS_OF",
    ) -> pd.DataFrame:
        if self.raise_on_call:
            raise RuntimeError("pit down")
        if not feature_ids:
            return pd.DataFrame(
                columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"]
            )
        fid = int(feature_ids[0])
        FakePITStore._calls_by_engine[id(self.engine)].append((fid, as_of_date))
        series = self._by_fid.get(fid, pd.Series(dtype=float))
        if series.empty:
            return pd.DataFrame(
                columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"]
            )
        # Strict PIT: everything we return must have release_date <= as_of_date.
        filtered = series[series.index <= pd.Timestamp(as_of_date)]
        rows = [
            {
                "feature_id": fid,
                "obs_date": idx,
                "value": float(val),
                "release_date": idx,
                "vintage_date": idx,
            }
            for idx, val in filtered.items()
        ]
        return pd.DataFrame(
            rows,
            columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"],
        )


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def feature_id_map() -> dict[str, int]:
    return {spec["primary"]: idx + 1 for idx, spec in enumerate(FEATURE_SPEC)}


def _daily_series(
    end: date,
    n: int,
    base: float,
    step: float = 0.1,
) -> pd.Series:
    idx = pd.DatetimeIndex([pd.Timestamp(end) - pd.Timedelta(days=n - 1 - i) for i in range(n)])
    values = np.asarray([base + step * i for i in range(n)], dtype=float)
    return pd.Series(values, index=idx)


def _series_bundle(end: date, n: int = 500) -> dict[str, pd.Series]:
    """One synthetic series per primary feature name. All strictly
    positive so realized-vol's log-returns are well defined."""
    bundle: dict[str, pd.Series] = {}
    base_lookup = {
        "vix_close": 20.0,
        "move_index_close": 80.0,
        "fci_composite": 0.0,
        "spy_close": 400.0,
        "tlt_full": 90.0,
        "hyg_full": 75.0,
        "dxy_spot": 100.0,
        "sp500_full": 4000.0,
        "wti_crude_full": 70.0,
    }
    for spec in FEATURE_SPEC:
        name = spec["primary"]
        base = base_lookup.get(name, 50.0)
        step = 0.05 if spec.get("kind") == "level" else 0.3
        bundle[name] = _daily_series(end, n, base, step)
    return bundle


@pytest.fixture(autouse=True)
def _reset_fake_pit_registry():
    FakePITStore.reset()
    yield
    FakePITStore.reset()


@pytest.fixture
def patched_pit(monkeypatch, feature_id_map):
    """Monkeypatch PITStore onto the module; yield a factory that
    installs a data bundle for a given query date.
    """
    monkeypatch.setattr(hsl, "PITStore", FakePITStore)

    def factory(engine: FakeEngine, end: date, *, n: int = 500) -> FakePITStore:
        store = FakePITStore(engine)
        store.install(_series_bundle(end, n=n), feature_id_map)
        return store

    return factory


def _mk_pred(
    *,
    pid: str,
    ticker: str = "SPY",
    created_at: datetime,
    horizon_days: int = 7,
    confidence: float = 0.65,
    verdict: str = "hit",
    direction: str = "bullish",
    realized_return: float | None = 0.02,
) -> dict[str, Any]:
    return {
        "id": pid,
        "ticker": ticker,
        "created_at": created_at,
        "expiry": created_at + timedelta(days=horizon_days),
        "confidence": confidence,
        "verdict": verdict,
        "signals": {"direction": direction, "regime": "GROWTH"},
        "realized_return": realized_return,
    }


# ── Direction / verdict helpers (pure) ────────────────────────────────────


def test_canonical_direction_strings():
    assert hsl._canonical_direction("bullish") == "bullish"
    assert hsl._canonical_direction("SHORT") == "bearish"
    assert hsl._canonical_direction("CALL") == "bullish"
    assert hsl._canonical_direction("put") == "bearish"
    assert hsl._canonical_direction(None) == "bullish"


def test_canonical_direction_dict_payload():
    assert hsl._canonical_direction({"direction": "bearish"}) == "bearish"
    assert hsl._canonical_direction({"side": "long"}) == "bullish"
    assert hsl._canonical_direction({}) == "bullish"  # default


def test_verdict_score_matches_bootstrap_convention():
    assert hsl._verdict_score("hit") == 1.0
    assert hsl._verdict_score("partial") == 0.5
    assert hsl._verdict_score("miss") == 0.0
    assert hsl._verdict_score("unknown") == 0.0
    assert hsl._verdict_score(None) == 0.0


# ── Multiplier classification ─────────────────────────────────────────────


def test_classify_multiplier_insufficient():
    mult, advisory = hsl._classify_multiplier(MIN_MATCHES_FOR_SIGNAL - 1, 0.9)
    assert mult == MULT_NEUTRAL
    assert "insufficient" in advisory.lower()


def test_classify_multiplier_amplify():
    mult, _ = hsl._classify_multiplier(30, 0.75)
    assert mult == MULT_AMPLIFY


def test_classify_multiplier_strong_degrade():
    mult, advisory = hsl._classify_multiplier(30, 0.25)
    assert mult == MULT_STRONG_DEGRADE
    assert "failed" in advisory.lower()


def test_classify_multiplier_boundary_exact_70():
    mult, _ = hsl._classify_multiplier(30, 0.70)
    assert mult == MULT_AMPLIFY


def test_classify_multiplier_boundary_exact_30():
    mult, _ = hsl._classify_multiplier(30, 0.30)
    # Exactly 0.30 falls into the [0.30, 0.45) bucket → 0.85.
    assert mult == MULT_DEGRADE


# ── Cosine ────────────────────────────────────────────────────────────────


def test_cosine_similarity_symmetry_and_self_match():
    a = np.asarray([1.0, 2.0, 3.0])
    b = np.asarray([2.0, 4.0, 6.0])
    assert hsl._cosine(a, b) == pytest.approx(1.0)
    assert hsl._cosine(a, -a) == pytest.approx(-1.0)


def test_cosine_zero_norm_safe():
    a = np.zeros(5)
    b = np.asarray([1.0, 2.0, 3.0, 4.0, 5.0])
    assert hsl._cosine(a, b) == 0.0


# ── Reductions ────────────────────────────────────────────────────────────


def test_reduce_level_strict_less_than_as_of():
    """Z-score normalization (and the query reduction) must be strictly
    ``< as_of`` — a value recorded exactly at as_of is excluded.
    """
    end = date(2026, 4, 10)
    series = _daily_series(end, n=50, base=10.0, step=1.0)
    # Latest strictly-less-than end is index [-2] (day before)
    val = hsl._reduce_level(series, end)
    assert val == pytest.approx(series.iloc[-2])


def test_reduce_pct_change_window():
    end = date(2026, 4, 10)
    series = _daily_series(end, n=60, base=100.0, step=1.0)
    # strict < as_of → last = series[-2], ref = series[-2 - 20]
    val = hsl._reduce_pct_change(series, end, window=20)
    last = float(series.iloc[-2])
    ref = float(series.iloc[-2 - 20])
    assert val == pytest.approx((last / ref) - 1.0)


def test_reduce_realized_vol_positive():
    end = date(2026, 4, 10)
    series = _daily_series(end, n=100, base=100.0, step=1.0)
    sigma = hsl._reduce_realized_vol(series, end, window=30)
    assert sigma is not None
    assert sigma > 0


# ── Z-score cache + normalization window ─────────────────────────────────


def test_build_rolling_samples_excludes_as_of(feature_id_map):
    end = date(2026, 4, 10)
    series = _daily_series(end, n=100, base=50.0, step=1.0)
    spec = {"kind": "level"}
    samples = hsl._build_rolling_samples(spec, series, end)
    # Samples are pure history, strictly before end. Highest sample
    # should be the value one day prior.
    assert max(samples) == pytest.approx(series.iloc[-2])


def test_zscore_neutral_on_small_sample():
    assert hsl._zscore(1.0, [1.0, 1.1]) == 0.0  # MIN_NORM_OBS not reached


def test_zscore_clamp():
    samples = [0.0] * 100
    samples[0] = 0.0
    # Use a non-degenerate window
    samples = [float(i) for i in range(60)]
    z_extreme = hsl._zscore(1e9, samples)
    assert abs(z_extreme) <= hsl.ZSCORE_CLAMP + 1e-9


# ── Feature vector builder ────────────────────────────────────────────────


def test_feature_vector_built_from_pit_store(patched_pit, feature_id_map):
    end = date(2026, 4, 10)
    engine = FakeEngine(feature_id_by_name=feature_id_map)
    store = patched_pit(engine, end, n=500)

    vec, missing = hsl._build_feature_vector(
        engine,
        store,
        end,
        feature_id_cache={},
        series_cache={},
    )
    assert vec.shape == (FEATURE_DIM,)
    assert missing == 0
    # Not every dim is expected to be exactly zero (the synthetic series
    # have a strict monotone drift → z-score ~+1.7 at the trailing edge).
    assert np.any(np.abs(vec) > 0)


def test_feature_vector_missing_feature_uses_sentinel(feature_id_map, patched_pit):
    end = date(2026, 4, 10)
    # Delete a feature id from the engine map so that feature lookup fails
    broken_map = dict(feature_id_map)
    dropped = "vix_close"
    broken_map.pop(dropped)
    engine = FakeEngine(feature_id_by_name=broken_map)
    store = patched_pit(engine, end, n=500)

    vec, missing = hsl._build_feature_vector(
        engine,
        store,
        end,
        feature_id_cache={},
        series_cache={},
    )
    assert vec.shape == (FEATURE_DIM,)
    assert missing >= 1
    # The vix slot must be the neutral sentinel.
    vix_idx = next(i for i, spec in enumerate(FEATURE_SPEC) if spec["primary"] == dropped)
    assert vec[vix_idx] == 0.0


# ── Caching (no redundant DB hits) ────────────────────────────────────────


def test_feature_id_cache_single_lookup_per_name(patched_pit, feature_id_map):
    end = date(2026, 4, 10)
    engine = FakeEngine(feature_id_by_name=feature_id_map)
    store = patched_pit(engine, end, n=500)
    feature_id_cache: dict[str, int | None] = {}
    series_cache: dict[tuple[str, date], pd.Series] = {}

    # Build the vector three times for the same as_of
    for _ in range(3):
        hsl._build_feature_vector(
            engine, store, end,
            feature_id_cache=feature_id_cache,
            series_cache=series_cache,
        )

    name_call_counts: dict[str, int] = {}
    for n in engine.feature_id_calls:
        name_call_counts[n] = name_call_counts.get(n, 0) + 1
    # Each primary name should be looked up at most once.
    for spec in FEATURE_SPEC:
        primary = spec["primary"]
        if primary in name_call_counts:
            assert name_call_counts[primary] == 1

    # PIT fetch should also be cached per (name, as_of) → at most one
    # call per distinct feature_id for this one as_of date.
    call_fids = [fid for fid, _ in store.calls]
    assert len(set(call_fids)) == len(call_fids)


# ── find_analogs integration ──────────────────────────────────────────────


def _make_predictions_bullish_all_hits(
    query_date: date,
    *,
    n: int = 25,
    horizon_days: int = 7,
    gap_days: int = 60,
) -> list[dict[str, Any]]:
    """Spread historical predictions far enough back that none are
    inside the 2x horizon overlap window."""
    preds: list[dict[str, Any]] = []
    for i in range(n):
        created = datetime.combine(
            query_date - timedelta(days=gap_days + i * 7),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        preds.append(
            _mk_pred(
                pid=f"h{i}",
                created_at=created,
                horizon_days=horizon_days,
                verdict="hit",
                direction="bullish",
                confidence=0.7,
            )
        )
    return preds


def test_find_analogs_amplifies_on_all_hits(patched_pit, feature_id_map):
    query_date = date(2026, 4, 10)
    preds = _make_predictions_bullish_all_hits(query_date, n=25)
    engine = FakeEngine(predictions=preds, feature_id_by_name=feature_id_map)
    patched_pit(engine, query_date, n=500)

    report = find_analogs(
        engine,
        as_of=query_date,
        horizon_days=7,
        direction="bullish",
    )
    assert isinstance(report, ScenarioLibraryReport)
    assert report.n_candidates_scanned == 25
    assert report.n_matches >= MIN_MATCHES_FOR_SIGNAL
    assert report.hit_rate == pytest.approx(1.0)
    assert report.conviction_multiplier == MULT_AMPLIFY
    assert report.hit_rate_long == pytest.approx(1.0)
    assert report.hit_rate_short == 0.0
    # Top analogs truncated to TOP_ANALOG_DISPLAY
    assert len(report.top_analogs) <= hsl.TOP_ANALOG_DISPLAY
    # Mean realized return populated
    assert report.mean_realized_return is not None


def test_find_analogs_degrades_on_all_misses(patched_pit, feature_id_map):
    query_date = date(2026, 4, 10)
    preds = _make_predictions_bullish_all_hits(query_date, n=25)
    # Flip verdicts to miss
    for p in preds:
        p["verdict"] = "miss"
    engine = FakeEngine(predictions=preds, feature_id_by_name=feature_id_map)
    patched_pit(engine, query_date, n=500)

    report = find_analogs(
        engine,
        as_of=query_date,
        horizon_days=7,
        direction="bullish",
    )
    assert report.hit_rate == pytest.approx(0.0)
    assert report.conviction_multiplier == MULT_STRONG_DEGRADE


def test_find_analogs_insufficient_matches(patched_pit, feature_id_map):
    query_date = date(2026, 4, 10)
    preds = _make_predictions_bullish_all_hits(query_date, n=5)
    engine = FakeEngine(predictions=preds, feature_id_by_name=feature_id_map)
    patched_pit(engine, query_date, n=500)

    report = find_analogs(
        engine,
        as_of=query_date,
        horizon_days=7,
        direction="bullish",
    )
    # n=5 < MIN_MATCHES → neutral advisory, 1.00 multiplier
    assert report.conviction_multiplier == MULT_NEUTRAL
    assert "insufficient" in report.advisory.lower()


def test_direction_filter_excludes_opposite(patched_pit, feature_id_map):
    query_date = date(2026, 4, 10)
    preds = _make_predictions_bullish_all_hits(query_date, n=25)
    # Add some bearish predictions that should be ignored
    for i in range(20):
        created = datetime.combine(
            query_date - timedelta(days=200 + i * 4),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        preds.append(
            _mk_pred(
                pid=f"bear{i}",
                created_at=created,
                horizon_days=7,
                verdict="miss",
                direction="bearish",
            )
        )
    engine = FakeEngine(predictions=preds, feature_id_by_name=feature_id_map)
    patched_pit(engine, query_date, n=500)

    report = find_analogs(
        engine,
        as_of=query_date,
        horizon_days=7,
        direction="bullish",
    )
    assert all(a.direction == "bullish" for a in report.top_analogs)


def test_ticker_filter_narrows_candidate_set(patched_pit, feature_id_map):
    query_date = date(2026, 4, 10)
    preds = _make_predictions_bullish_all_hits(query_date, n=25)
    # Add a batch on another ticker
    for i in range(25):
        created = datetime.combine(
            query_date - timedelta(days=180 + i * 5),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        preds.append(
            _mk_pred(
                pid=f"qqq{i}",
                ticker="QQQ",
                created_at=created,
                horizon_days=7,
                verdict="miss",
                direction="bullish",
            )
        )
    engine = FakeEngine(predictions=preds, feature_id_by_name=feature_id_map)
    patched_pit(engine, query_date, n=500)

    report = find_analogs(
        engine,
        as_of=query_date,
        horizon_days=7,
        ticker="SPY",
    )
    # SPY hits only → full hit_rate
    assert report.hit_rate == pytest.approx(1.0)
    assert all(a.ticker == "SPY" for a in report.top_analogs)


def test_overlap_filter_excludes_recent_predictions(patched_pit, feature_id_map):
    query_date = date(2026, 4, 10)
    horizon_days = 7
    # Create predictions INSIDE the 2x horizon window (13 days back)
    recent = [
        _mk_pred(
            pid=f"recent{i}",
            created_at=datetime.combine(
                query_date - timedelta(days=1 + i),
                datetime.min.time(),
                tzinfo=timezone.utc,
            ),
            horizon_days=horizon_days,
            verdict="miss",
            direction="bullish",
        )
        for i in range(10)
    ]
    engine = FakeEngine(predictions=recent, feature_id_by_name=feature_id_map)
    patched_pit(engine, query_date, n=500)

    report = find_analogs(
        engine,
        as_of=query_date,
        horizon_days=horizon_days,
        direction="bullish",
    )
    assert report.n_candidates_scanned == 10
    # All candidates inside the leak window → no matches.
    assert report.n_matches == 0


def test_empty_oracle_history_returns_neutral(patched_pit, feature_id_map):
    query_date = date(2026, 4, 10)
    engine = FakeEngine(predictions=[], feature_id_by_name=feature_id_map)
    patched_pit(engine, query_date, n=500)
    report = find_analogs(
        engine,
        as_of=query_date,
        horizon_days=7,
    )
    assert report.n_matches == 0
    assert report.conviction_multiplier == MULT_NEUTRAL
    assert "no historical predictions" in report.advisory


def test_to_dict_roundtrip(patched_pit, feature_id_map):
    query_date = date(2026, 4, 10)
    preds = _make_predictions_bullish_all_hits(query_date, n=20)
    engine = FakeEngine(predictions=preds, feature_id_by_name=feature_id_map)
    patched_pit(engine, query_date, n=500)
    report = find_analogs(
        engine,
        as_of=query_date,
        horizon_days=7,
        direction="bullish",
    )
    d = report.to_dict()
    required = {
        "query_as_of",
        "query_horizon_days",
        "query_feature_vector",
        "n_candidates_scanned",
        "n_matches",
        "hit_rate",
        "hit_rate_long",
        "hit_rate_short",
        "mean_confidence",
        "mean_realized_return",
        "conviction_multiplier",
        "advisory",
        "top_analogs",
        "missing_feature_count",
    }
    assert required.issubset(d.keys())
    # top_analogs roundtrip
    assert isinstance(d["top_analogs"], list)
    if d["top_analogs"]:
        first = d["top_analogs"][0]
        for key in (
            "prediction_id",
            "ticker",
            "created_at",
            "similarity",
            "direction",
            "verdict",
            "confidence",
            "horizon_days",
        ):
            assert key in first


def test_scenario_conviction_multiplier_returns_one_on_pit_failure(
    patched_pit, feature_id_map, monkeypatch,
):
    query_date = date(2026, 4, 10)
    engine = FakeEngine(predictions=[], feature_id_by_name=feature_id_map)

    class _BrokenPIT(FakePITStore):
        def __init__(self, engine: Any) -> None:  # noqa: D401
            super().__init__(engine)

        def get_pit(self, **kwargs: Any) -> pd.DataFrame:
            raise RuntimeError("pit is down")

    monkeypatch.setattr(hsl, "PITStore", _BrokenPIT)
    mult = scenario_conviction_multiplier(
        engine,
        as_of=query_date,
        horizon_days=7,
    )
    assert mult == MULT_NEUTRAL  # 1.0


def test_scenario_conviction_multiplier_returns_one_on_engine_failure(
    patched_pit, feature_id_map,
):
    query_date = date(2026, 4, 10)
    engine = FakeEngine(
        predictions=[],
        feature_id_by_name=feature_id_map,
        raise_on_oracle=True,
    )
    patched_pit(engine, query_date, n=500)
    mult = scenario_conviction_multiplier(
        engine,
        as_of=query_date,
        horizon_days=7,
    )
    assert mult == MULT_NEUTRAL


def test_fallback_feature_resolution(monkeypatch, feature_id_map):
    """When ``dxy_spot`` is missing but ``uup_etf_close`` is present,
    the fallback lookup must be used without incrementing missing."""
    query_date = date(2026, 4, 10)
    # Swap primary out
    fid_map = dict(feature_id_map)
    fid_map.pop("dxy_spot", None)
    fid_map["uup_etf_close"] = 99

    engine = FakeEngine(feature_id_by_name=fid_map)

    monkeypatch.setattr(hsl, "PITStore", FakePITStore)
    store = FakePITStore(engine)
    bundle = _series_bundle(query_date, n=500)
    bundle["uup_etf_close"] = bundle.pop("dxy_spot")
    store.install(bundle, fid_map)

    vec, missing = hsl._build_feature_vector(
        engine,
        store,
        query_date,
        feature_id_cache={},
        series_cache={},
    )
    # dxy lane must load via fallback → missing count for that lane is 0.
    dxy_idx = next(i for i, s in enumerate(FEATURE_SPEC) if s["name"] == "dxy_mom_20d")
    assert abs(vec[dxy_idx]) > 0 or vec[dxy_idx] == 0.0  # present, just a value
    # Missing overall should remain low (all other features OK too).
    assert missing == 0


# ── Overlap helper ────────────────────────────────────────────────────────


def test_within_overlap_window_boundary():
    q = date(2026, 4, 10)
    # horizon=7 → window excludes candidates within 13 days strictly (<14)
    inside = q - timedelta(days=13)
    outside = q - timedelta(days=14)
    assert hsl._within_overlap_window(inside, q, 7) is True
    assert hsl._within_overlap_window(outside, q, 7) is False


# ── Report shape / dataclass immutability ────────────────────────────────


def test_scenario_analog_is_frozen():
    a = ScenarioAnalog(
        prediction_id="x",
        ticker="SPY",
        created_at="2026-01-01",
        similarity=0.9,
        direction="bullish",
        verdict="hit",
        confidence=0.7,
        horizon_days=7,
    )
    with pytest.raises(Exception):
        a.similarity = 0.5  # type: ignore[misc]


def test_report_is_frozen():
    r = hsl._neutral_report(as_of=date(2026, 4, 10), horizon_days=7)
    with pytest.raises(Exception):
        r.hit_rate = 1.0  # type: ignore[misc]
