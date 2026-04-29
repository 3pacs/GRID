"""Tests for intelligence/prediction_market_arbitrage.py (CAT-183)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest
from sqlalchemy.exc import OperationalError, ProgrammingError

from intelligence.prediction_market_arbitrage import (
    ArbitrageReport,
    _advisory,
    _canon_direction,
    _direction_aware_multiplier,
    _signal_strength,
    arbitrage_conviction_multiplier,
    build_arbitrage_report,
    get_market_implied_prob,
    get_oracle_vs_market_calibration,
)


# ══════════════════════════════════════════════════════════════════════
# FakeEngine — substring-matched SQL router
# ══════════════════════════════════════════════════════════════════════


class FakeResult:
    """Mimics a SQLAlchemy Result."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class FakeConnection:
    """Context-manager connection that routes SQL by substring match."""

    def __init__(
        self,
        *,
        prediction_rows: list[tuple[Any, ...]] | None = None,
        oracle_rows: list[tuple[Any, ...]] | None = None,
        raise_on_prediction: Exception | None = None,
        raise_on_oracle: Exception | None = None,
    ) -> None:
        self._prediction_rows = prediction_rows if prediction_rows is not None else []
        self._oracle_rows = oracle_rows if oracle_rows is not None else []
        self._raise_on_prediction = raise_on_prediction
        self._raise_on_oracle = raise_on_oracle
        self.executed: list[tuple[str, dict[str, Any]]] = []

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> FakeResult:
        sql = str(stmt).lower()
        self.executed.append((sql, params or {}))
        if "prediction_odds" in sql:
            if self._raise_on_prediction is not None:
                raise self._raise_on_prediction
            return FakeResult(self._prediction_rows)
        if "oracle_predictions" in sql:
            if self._raise_on_oracle is not None:
                raise self._raise_on_oracle
            return FakeResult(self._oracle_rows)
        return FakeResult([])

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class FakeEngine:
    """Minimal SQLAlchemy-ish engine mock."""

    def __init__(
        self,
        *,
        prediction_rows: list[tuple[Any, ...]] | None = None,
        oracle_rows: list[tuple[Any, ...]] | None = None,
        raise_on_prediction: Exception | None = None,
        raise_on_oracle: Exception | None = None,
        should_fail: bool = False,
    ) -> None:
        self._prediction_rows = prediction_rows
        self._oracle_rows = oracle_rows
        self._raise_on_prediction = raise_on_prediction
        self._raise_on_oracle = raise_on_oracle
        self._should_fail = should_fail
        self.connections_made = 0

    def connect(self) -> FakeConnection:
        self.connections_made += 1
        if self._should_fail:
            raise RuntimeError("engine.connect blew up")
        return FakeConnection(
            prediction_rows=self._prediction_rows,
            oracle_rows=self._oracle_rows,
            raise_on_prediction=self._raise_on_prediction,
            raise_on_oracle=self._raise_on_oracle,
        )


# ══════════════════════════════════════════════════════════════════════
# Constants & helpers
# ══════════════════════════════════════════════════════════════════════


AS_OF = date(2026, 4, 14)
HORIZON = 30
TICKER = "BTC"


def _calibrated_oracle_rows(n: int = 25, oracle_edge: int = 10) -> list[tuple[int, int]]:
    """Return ``n`` rows where oracle beats the market by ``oracle_edge`` hits."""
    rows: list[tuple[int, int]] = []
    for i in range(n):
        oracle_hit = 1 if i < (n // 2 + oracle_edge) else 0
        market_hit = 1 if i < (n // 2) else 0
        rows.append((oracle_hit, market_hit))
    return rows


def _uncalibrated_oracle_rows(n: int = 25) -> list[tuple[int, int]]:
    """Oracle and market both hit the same rate → no edge."""
    return [(1, 1) if i < n // 2 else (0, 0) for i in range(n)]


# ══════════════════════════════════════════════════════════════════════
# 1. Pure scoring helpers
# ══════════════════════════════════════════════════════════════════════


class TestSignalStrength:
    def test_magnitude_below_noise_is_zero(self) -> None:
        assert _signal_strength(0.04) == 0.0

    def test_magnitude_small_is_half(self) -> None:
        assert _signal_strength(0.08) == 0.5

    def test_magnitude_medium_is_eight_tenths(self) -> None:
        assert _signal_strength(0.15) == 0.8

    def test_magnitude_extreme_is_one(self) -> None:
        assert _signal_strength(0.30) == 1.0

    def test_exact_thresholds_round_up_bucket(self) -> None:
        assert _signal_strength(0.05) == 0.5
        assert _signal_strength(0.10) == 0.8
        assert _signal_strength(0.20) == 1.0


class TestCanonDirection:
    def test_bullish_synonyms(self) -> None:
        for syn in ("bullish", "LONG", "Up", "call"):
            assert _canon_direction(syn) == "bullish"

    def test_bearish_synonyms(self) -> None:
        for syn in ("bearish", "SHORT", "down", "put"):
            assert _canon_direction(syn) == "bearish"

    def test_unknown_passes_through_lowered(self) -> None:
        assert _canon_direction("FLAT") == "flat"

    def test_non_string_returns_empty(self) -> None:
        assert _canon_direction(None) == ""  # type: ignore[arg-type]


class TestDirectionAwareMultiplier:
    def test_aligned_bullish_full_signal_gives_max_boost(self) -> None:
        mult = _direction_aware_multiplier(
            disagreement=0.25, trade_direction="bullish", trusted_signal=1.0,
        )
        assert mult == pytest.approx(1.10)

    def test_aligned_bearish_full_signal_gives_max_boost(self) -> None:
        mult = _direction_aware_multiplier(
            disagreement=-0.25, trade_direction="bearish", trusted_signal=1.0,
        )
        assert mult == pytest.approx(1.10)

    def test_misaligned_bullish_gives_haircut(self) -> None:
        mult = _direction_aware_multiplier(
            disagreement=-0.25, trade_direction="bullish", trusted_signal=1.0,
        )
        assert mult == pytest.approx(0.95)

    def test_misaligned_bearish_gives_haircut(self) -> None:
        mult = _direction_aware_multiplier(
            disagreement=0.25, trade_direction="bearish", trusted_signal=1.0,
        )
        assert mult == pytest.approx(0.95)

    def test_flat_direction_neutral(self) -> None:
        mult = _direction_aware_multiplier(
            disagreement=0.25, trade_direction="flat", trusted_signal=1.0,
        )
        assert mult == 1.00

    def test_zero_signal_neutral(self) -> None:
        mult = _direction_aware_multiplier(
            disagreement=0.25, trade_direction="bullish", trusted_signal=0.0,
        )
        assert mult == 1.00

    def test_half_trusted_aligned_is_halfway(self) -> None:
        mult = _direction_aware_multiplier(
            disagreement=0.25, trade_direction="bullish", trusted_signal=0.5,
        )
        assert mult == pytest.approx(1.05)


# ══════════════════════════════════════════════════════════════════════
# 2. get_market_implied_prob
# ══════════════════════════════════════════════════════════════════════


class TestGetMarketImpliedProb:
    def test_returns_yes_price_when_row_matches(self) -> None:
        engine = FakeEngine(prediction_rows=[(0.45, AS_OF + timedelta(days=30), AS_OF)])
        p = get_market_implied_prob(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert p == 0.45

    def test_returns_none_when_no_rows(self) -> None:
        engine = FakeEngine(prediction_rows=[])
        p = get_market_implied_prob(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert p is None

    def test_missing_table_programming_error_returns_none(self) -> None:
        engine = FakeEngine(
            raise_on_prediction=ProgrammingError("stmt", {}, Exception("no such table")),
        )
        p = get_market_implied_prob(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert p is None

    def test_operational_error_returns_none(self) -> None:
        engine = FakeEngine(
            raise_on_prediction=OperationalError("stmt", {}, Exception("conn dropped")),
        )
        p = get_market_implied_prob(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert p is None

    def test_non_numeric_yes_price_returns_none(self) -> None:
        engine = FakeEngine(prediction_rows=[("notanumber", AS_OF, AS_OF)])
        p = get_market_implied_prob(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert p is None

    def test_out_of_range_yes_price_returns_none(self) -> None:
        engine = FakeEngine(prediction_rows=[(1.5, AS_OF, AS_OF)])
        p = get_market_implied_prob(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert p is None

    def test_zero_horizon_returns_none(self) -> None:
        engine = FakeEngine(prediction_rows=[(0.5, AS_OF, AS_OF)])
        p = get_market_implied_prob(
            engine, ticker=TICKER, direction="bullish", horizon_days=0, as_of=AS_OF,
        )
        assert p is None

    def test_lookahead_guard_bind_present(self) -> None:
        """The SQL executed must include the as_of bind."""
        rec_engine = _RecordingEngine(prediction_rows=[(0.5, AS_OF, AS_OF)])
        get_market_implied_prob(
            rec_engine, ticker=TICKER, direction="bullish", horizon_days=7, as_of=AS_OF,
        )
        assert rec_engine.last_params is not None
        assert rec_engine.last_params["as_of"] == AS_OF
        assert "created_at <= :as_of" in rec_engine.last_sql

    def test_fuzzy_window_7d_horizon_is_6_to_8(self) -> None:
        rec_engine = _RecordingEngine(prediction_rows=[(0.5, AS_OF, AS_OF)])
        get_market_implied_prob(
            rec_engine, ticker=TICKER, direction="bullish", horizon_days=7, as_of=AS_OF,
        )
        lower = rec_engine.last_params["lower_resolve"]
        upper = rec_engine.last_params["upper_resolve"]
        # 7 * 0.8 = 5.6 → round → 6; 7 * 1.2 = 8.4 → round → 8
        assert (lower - AS_OF).days == 6
        assert (upper - AS_OF).days == 8

    def test_fuzzy_window_14d_horizon_excludes_6d_question(self) -> None:
        """A 6-day-out question should not match a 14-day horizon query."""
        rec_engine = _RecordingEngine(prediction_rows=[(0.5, AS_OF, AS_OF)])
        get_market_implied_prob(
            rec_engine, ticker=TICKER, direction="bullish", horizon_days=14, as_of=AS_OF,
        )
        lower = rec_engine.last_params["lower_resolve"]
        # 14 * 0.8 = 11.2 → round → 11; a 6d question is below that.
        assert (lower - AS_OF).days == 11


# A recording engine used to inspect the SQL and params pushed by production code.
class _RecordingEngine(FakeEngine):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.last_sql: str = ""
        self.last_params: dict[str, Any] = {}

    def connect(self) -> FakeConnection:
        conn = super().connect()
        orig_execute = conn.execute

        def _recording_execute(stmt: Any, params: dict[str, Any] | None = None) -> FakeResult:
            self.last_sql = str(stmt).lower()
            self.last_params = dict(params or {})
            return orig_execute(stmt, params)

        conn.execute = _recording_execute  # type: ignore[method-assign]
        return conn


# ══════════════════════════════════════════════════════════════════════
# 3. get_oracle_vs_market_calibration
# ══════════════════════════════════════════════════════════════════════


class TestGetOracleVsMarketCalibration:
    def test_calibrated_when_sufficient_samples_and_edge(self) -> None:
        engine = FakeEngine(oracle_rows=_calibrated_oracle_rows(n=25, oracle_edge=10))
        calibrated, n = get_oracle_vs_market_calibration(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert n == 25
        assert calibrated is True

    def test_uncalibrated_when_too_few_samples(self) -> None:
        engine = FakeEngine(oracle_rows=[(1, 0)] * 10)
        calibrated, n = get_oracle_vs_market_calibration(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert n == 10
        assert calibrated is False

    def test_uncalibrated_when_edge_too_small(self) -> None:
        engine = FakeEngine(oracle_rows=_uncalibrated_oracle_rows(n=25))
        calibrated, _n = get_oracle_vs_market_calibration(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert calibrated is False

    def test_missing_column_programming_error_returns_false_zero(self) -> None:
        engine = FakeEngine(
            raise_on_oracle=ProgrammingError("stmt", {}, Exception("oracle_hit missing")),
        )
        calibrated, n = get_oracle_vs_market_calibration(
            engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert calibrated is False
        assert n == 0

    def test_lookahead_bind_present(self) -> None:
        rec_engine = _RecordingEngine(oracle_rows=[])
        get_oracle_vs_market_calibration(
            rec_engine, ticker=TICKER, direction="bullish", horizon_days=HORIZON, as_of=AS_OF,
        )
        assert rec_engine.last_params["as_of"] == AS_OF
        assert "created_at <= :as_of" in rec_engine.last_sql


# ══════════════════════════════════════════════════════════════════════
# 4. build_arbitrage_report — end to end
# ══════════════════════════════════════════════════════════════════════


class TestBuildArbitrageReport:
    def test_no_market_coverage_returns_neutral(self) -> None:
        engine = FakeEngine(prediction_rows=[], oracle_rows=[])
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        assert r.conviction_multiplier == 1.00
        assert r.market_implied_prob is None
        assert r.disagreement == 0.0
        assert r.advisory == "no prediction market coverage"

    def test_calibrated_bullish_aligned_max_boost(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.45, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_calibrated_oracle_rows(n=25, oracle_edge=10),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        # disagreement = 0.72 - 0.45 = 0.27 → extreme → 1.0
        assert r.signal_strength == 1.0
        assert r.oracle_calibrated_vs_market is True
        assert r.conviction_multiplier == pytest.approx(1.10)
        assert "calibrated" in r.advisory

    def test_uncalibrated_disagreement_is_haircut_in_half(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.45, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_uncalibrated_oracle_rows(n=25),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        # trusted_signal = 1.0 * 0.5 = 0.5 → multiplier = 1.00 + 0.10 * 0.5 = 1.05
        assert r.conviction_multiplier == pytest.approx(1.05)
        assert r.oracle_calibrated_vs_market is False

    def test_aligned_bullish_oracle_more_bearish_gives_haircut(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.80, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_calibrated_oracle_rows(n=25, oracle_edge=10),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.50,
        )
        # disagreement = -0.30 → |mag|=0.30 → signal 1.0
        # trade bullish, oracle bearish → misaligned → 1.00 - 0.05 * 1.0 = 0.95
        assert r.conviction_multiplier == pytest.approx(0.95)

    def test_noise_magnitude_gives_neutral_multiplier(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.70, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_calibrated_oracle_rows(n=25, oracle_edge=10),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        # disagreement = 0.02 → noise → signal 0.0 → multiplier 1.00
        assert r.signal_strength == 0.0
        assert r.conviction_multiplier == 1.00

    def test_small_magnitude_calibrated_gives_half_boost(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.64, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_calibrated_oracle_rows(n=25, oracle_edge=10),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        # disagreement = 0.08 → signal 0.5 → 1.00 + 0.10 * 0.5 = 1.05
        assert r.signal_strength == 0.5
        assert r.conviction_multiplier == pytest.approx(1.05)

    def test_medium_magnitude_calibrated_gives_eight_tenths_boost(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.57, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_calibrated_oracle_rows(n=25, oracle_edge=10),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        # disagreement = 0.15 → signal 0.8 → 1.00 + 0.10 * 0.8 = 1.08
        assert r.signal_strength == 0.8
        assert r.conviction_multiplier == pytest.approx(1.08)

    def test_flat_direction_returns_neutral(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.45, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_calibrated_oracle_rows(n=25, oracle_edge=10),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="flat",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        assert r.conviction_multiplier == 1.00

    def test_zero_confidence_safe(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.40, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_calibrated_oracle_rows(n=25, oracle_edge=10),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.0,
        )
        # disagreement = -0.40 → |mag|=0.40 → signal 1.0 → oracle very bearish
        # trade bullish → misaligned calibrated → 0.95
        assert r.conviction_multiplier == pytest.approx(0.95)

    def test_nan_confidence_coerced_to_zero(self) -> None:
        engine = FakeEngine(prediction_rows=[], oracle_rows=[])
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=float("nan"),
        )
        assert r.oracle_confidence == 0.0

    def test_confidence_above_one_clamped(self) -> None:
        engine = FakeEngine(prediction_rows=[], oracle_rows=[])
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=1.5,
        )
        assert r.oracle_confidence == 1.0

    def test_missing_prediction_odds_table_neutral(self) -> None:
        engine = FakeEngine(
            raise_on_prediction=ProgrammingError("stmt", {}, Exception("no table")),
        )
        r = build_arbitrage_report(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        assert r.conviction_multiplier == 1.00
        assert r.advisory == "no prediction market coverage"


# ══════════════════════════════════════════════════════════════════════
# 5. arbitrage_conviction_multiplier (live-path entry point)
# ══════════════════════════════════════════════════════════════════════


class TestArbitrageConvictionMultiplier:
    def test_returns_float(self) -> None:
        engine = FakeEngine(
            prediction_rows=[(0.45, AS_OF + timedelta(days=30), AS_OF)],
            oracle_rows=_calibrated_oracle_rows(),
        )
        m = arbitrage_conviction_multiplier(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        assert isinstance(m, float)
        assert m == pytest.approx(1.10)

    def test_returns_one_on_engine_connect_failure(self) -> None:
        engine = FakeEngine(should_fail=True)
        m = arbitrage_conviction_multiplier(
            engine, ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        # engine.connect() raises; get_market_implied_prob catches in its
        # broad except → returns None → report is neutral → multiplier 1.00
        assert m == 1.00

    def test_returns_one_when_db_raises_unknown_exception(self) -> None:
        class ExplodingEngine:
            def connect(self) -> Any:
                raise RuntimeError("boom")

        m = arbitrage_conviction_multiplier(
            ExplodingEngine(),  # type: ignore[arg-type]
            ticker=TICKER, as_of=AS_OF, direction="bullish",
            horizon_days=HORIZON, oracle_confidence=0.72,
        )
        assert m == 1.0

    def test_never_raises_on_bad_inputs(self) -> None:
        engine = FakeEngine(prediction_rows=[], oracle_rows=[])
        m = arbitrage_conviction_multiplier(
            engine, ticker="", as_of=AS_OF, direction="nonsense",
            horizon_days=-5, oracle_confidence=float("inf"),
        )
        assert m == 1.0


# ══════════════════════════════════════════════════════════════════════
# 6. ArbitrageReport.to_dict round-trip
# ══════════════════════════════════════════════════════════════════════


class TestArbitrageReportToDict:
    def test_to_dict_has_all_fields(self) -> None:
        report = ArbitrageReport(
            ticker="BTC",
            as_of="2026-04-14",
            oracle_confidence=0.72,
            market_implied_prob=0.45,
            disagreement=0.27,
            signal_strength=1.0,
            oracle_calibrated_vs_market=True,
            n_head_to_head=25,
            conviction_multiplier=1.10,
            advisory="calibrated disagreement",
        )
        d = report.to_dict()
        assert d["ticker"] == "BTC"
        assert d["as_of"] == "2026-04-14"
        assert d["oracle_confidence"] == 0.72
        assert d["market_implied_prob"] == 0.45
        assert d["disagreement"] == 0.27
        assert d["signal_strength"] == 1.0
        assert d["oracle_calibrated_vs_market"] is True
        assert d["n_head_to_head"] == 25
        assert d["conviction_multiplier"] == 1.10
        assert d["advisory"] == "calibrated disagreement"

    def test_to_dict_handles_none_market(self) -> None:
        report = ArbitrageReport(
            ticker="X", as_of="2026-04-14", oracle_confidence=0.5,
            market_implied_prob=None, disagreement=0.0, signal_strength=0.0,
            oracle_calibrated_vs_market=False, n_head_to_head=0,
            conviction_multiplier=1.0, advisory="no prediction market coverage",
        )
        d = report.to_dict()
        assert d["market_implied_prob"] is None


# ══════════════════════════════════════════════════════════════════════
# 7. Advisory strings
# ══════════════════════════════════════════════════════════════════════


class TestAdvisory:
    def test_noise_advisory(self) -> None:
        out = _advisory(magnitude=0.02, disagreement=0.02, direction="bullish", calibrated=True)
        assert "noise" in out

    def test_calibrated_aligned_advisory(self) -> None:
        out = _advisory(magnitude=0.25, disagreement=0.25, direction="bullish", calibrated=True)
        assert "calibrated" in out
        assert "aligned" in out

    def test_uncalibrated_fighting_advisory(self) -> None:
        out = _advisory(magnitude=0.25, disagreement=-0.25, direction="bullish", calibrated=False)
        assert "uncalibrated" in out
        assert "fighting" in out

    def test_unknown_direction_advisory(self) -> None:
        out = _advisory(magnitude=0.25, disagreement=0.25, direction="flat", calibrated=True)
        assert "unknown" in out
