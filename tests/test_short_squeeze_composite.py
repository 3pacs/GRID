"""Tests for intelligence.short_squeeze_composite (CAT-138 / #250)."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from intelligence import short_squeeze_composite as ssc
from intelligence.short_squeeze_composite import (
    MOMENTUM_LOOKBACK_DAYS,
    SqueezeComponent,
    SqueezeReport,
    _COMPONENT_WEIGHTS,
    _advisory_for,
    _clamp01,
    _multiplier_for,
    _normalize_borrow_fee,
    _normalize_days_to_cover,
    _normalize_gex_sign,
    _normalize_momentum,
    _normalize_short_interest,
    _normalize_social_heat,
    compute_squeeze_report,
    rank_universe_by_squeeze,
    squeeze_conviction_multiplier,
)


# ---------------------------------------------------------------------------
# Fakes — minimal SQLAlchemy engine + PITStore replacement
# ---------------------------------------------------------------------------


class _FakeConnection:
    def __init__(self, feature_ids: dict[str, int]):
        self._feature_ids = feature_ids
        self._pending: str | None = None

    def execute(self, stmt, params=None):
        # We only model the feature_registry name→id lookup.
        self._pending = params.get("n") if params else None
        return self

    def fetchone(self):
        if self._pending is None:
            return None
        fid = self._feature_ids.get(self._pending)
        return (fid,) if fid is not None else None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeEngine:
    """Minimal SQLAlchemy-engine substitute for the feature_registry path."""

    def __init__(self, feature_ids: dict[str, int] | None = None):
        self.feature_ids = feature_ids or {}

    def connect(self):
        return _FakeConnection(self.feature_ids)


class _FakePITStore:
    """Mimics store.pit.PITStore.get_pit with an in-memory feature map.

    ``data`` maps feature_id → list[(obs_date, value)]. ``get_pit`` filters
    to rows with obs_date <= as_of_date, matching real PIT semantics.
    """

    def __init__(self, data: dict[int, list[tuple[date, float]]]):
        self.data = data
        self.calls: list[tuple[list[int], date, str]] = []

    def get_pit(self, feature_ids, as_of_date, vintage_policy="LATEST_AS_OF"):
        self.calls.append((list(feature_ids), as_of_date, vintage_policy))
        rows = []
        for fid in feature_ids:
            series = self.data.get(fid, [])
            for obs_date, value in series:
                if obs_date <= as_of_date:
                    rows.append(
                        {
                            "feature_id": fid,
                            "obs_date": obs_date,
                            "value": value,
                            "release_date": obs_date,
                            "vintage_date": obs_date,
                        }
                    )
        if not rows:
            return pd.DataFrame(
                columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"]
            )
        return pd.DataFrame(rows)


@pytest.fixture
def install_fake_pit(monkeypatch):
    """Install a fake PITStore constructor bound to arbitrary test data."""

    holder: dict[str, _FakePITStore | None] = {"store": None}

    def _setup(pit_data: dict[int, list[tuple[date, float]]]):
        store = _FakePITStore(pit_data)
        holder["store"] = store
        monkeypatch.setattr(ssc, "PITStore", lambda engine: store)
        return store

    return _setup


# ---------------------------------------------------------------------------
# Weights / constants
# ---------------------------------------------------------------------------


class TestWeights:
    def test_weights_sum_to_one(self):
        total = sum(_COMPONENT_WEIGHTS.values())
        assert abs(total - 1.0) < 1e-9

    def test_momentum_lookback_is_20(self):
        assert MOMENTUM_LOOKBACK_DAYS == 20

    def test_weight_keys_match_canonical_order(self):
        assert set(_COMPONENT_WEIGHTS.keys()) == set(ssc._CANONICAL_ORDER)


# ---------------------------------------------------------------------------
# Normalization truth tables
# ---------------------------------------------------------------------------


class TestShortInterestNormalization:
    def test_zero(self):
        assert _normalize_short_interest(0.0) == 0.0

    def test_threshold(self):
        assert _normalize_short_interest(30.0) == 1.0

    def test_half(self):
        assert _normalize_short_interest(15.0) == pytest.approx(0.5)

    def test_overshoot_clamps(self):
        assert _normalize_short_interest(60.0) == 1.0

    def test_negative_clamps(self):
        assert _normalize_short_interest(-5.0) == 0.0


class TestDaysToCoverNormalization:
    def test_zero(self):
        assert _normalize_days_to_cover(0.0) == 0.0

    def test_threshold(self):
        assert _normalize_days_to_cover(10.0) == 1.0

    def test_double(self):
        assert _normalize_days_to_cover(20.0) == 1.0

    def test_five(self):
        assert _normalize_days_to_cover(5.0) == pytest.approx(0.5)


class TestBorrowFeeNormalization:
    def test_zero(self):
        assert _normalize_borrow_fee(0.0) == 0.0

    def test_threshold(self):
        assert _normalize_borrow_fee(30.0) == 1.0

    def test_double(self):
        assert _normalize_borrow_fee(60.0) == 1.0

    def test_ten(self):
        assert _normalize_borrow_fee(10.0) == pytest.approx(1.0 / 3.0)


class TestMomentumNormalization:
    def test_zero(self):
        assert _normalize_momentum(0.0) == 0.0

    def test_positive_threshold(self):
        assert _normalize_momentum(0.10) == 1.0

    def test_double(self):
        assert _normalize_momentum(0.20) == 1.0

    def test_negative_yields_zero(self):
        assert _normalize_momentum(-0.05) == 0.0

    def test_negative_large_yields_zero(self):
        assert _normalize_momentum(-0.50) == 0.0


class TestSocialHeatNormalization:
    def test_zero_z(self):
        assert _normalize_social_heat(0.0) == pytest.approx(1.0 / 3.0)

    def test_plus_two_z(self):
        assert _normalize_social_heat(2.0) == 1.0

    def test_minus_one_z(self):
        assert _normalize_social_heat(-1.0) == 0.0

    def test_minus_two_z_clamps_zero(self):
        assert _normalize_social_heat(-2.0) == 0.0

    def test_plus_three_z_clamps_one(self):
        assert _normalize_social_heat(3.0) == 1.0


class TestGexSignNormalization:
    def test_negative_is_one(self):
        assert _normalize_gex_sign(-1.0) == 1.0

    def test_zero_is_zero(self):
        assert _normalize_gex_sign(0.0) == 0.0

    def test_positive_is_zero(self):
        assert _normalize_gex_sign(1.0) == 0.0

    def test_large_negative(self):
        assert _normalize_gex_sign(-1e9) == 1.0


class TestClamp:
    def test_mid(self):
        assert _clamp01(0.5) == 0.5

    def test_nan_becomes_zero(self):
        assert _clamp01(float("nan")) == 0.0

    def test_inf_becomes_zero(self):
        assert _clamp01(float("inf")) == 0.0


# ---------------------------------------------------------------------------
# compute_squeeze_report — happy path + missing features
# ---------------------------------------------------------------------------


def _feature_ids_for(ticker: str) -> dict[str, int]:
    """Canonical mapping feature_name → fake id for a ticker."""
    return {
        f"{ticker}_short_interest_pct": 1,
        f"{ticker}_days_to_cover": 2,
        f"{ticker}_borrow_fee": 3,
        f"{ticker}_close": 4,
        f"{ticker}_reddit_mentions_z": 5,
        f"{ticker}_gex_net": 6,
    }


class TestComputeReport:
    def test_all_max_gives_probability_one(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        # Close series: 21 days, prior = 100, latest = 120 → +20% → clamps to 1
        close_series = [(as_of - timedelta(days=i), 100.0) for i in range(20, 0, -1)]
        close_series.append((as_of, 120.0))
        install_fake_pit(
            {
                1: [(as_of, 50.0)],   # 50% short interest → 1.0
                2: [(as_of, 20.0)],   # 20 days → 1.0
                3: [(as_of, 60.0)],   # 60% borrow fee → 1.0
                4: close_series,      # +20% momentum → 1.0
                5: [(as_of, 3.0)],    # z=3 → 1.0
                6: [(as_of, -1e9)],   # negative gex → 1.0
            }
        )
        rep = compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        assert rep.squeeze_probability == pytest.approx(1.0)
        assert rep.missing_component_count == 0
        assert rep.advisory == "EXTREME_SQUEEZE_RISK"

    def test_all_missing_yields_zero(self, install_fake_pit):
        engine = FakeEngine({})  # no features registered
        install_fake_pit({})
        rep = compute_squeeze_report(
            engine, ticker="XYZ", as_of=date(2026, 4, 1)
        )
        assert rep.squeeze_probability == 0.0
        assert rep.missing_component_count == 6
        assert rep.advisory == "LOW_SQUEEZE_RISK"

    def test_partial_missing_counter(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("GME"))
        install_fake_pit(
            {
                1: [(as_of, 30.0)],  # short interest only
                # everything else missing
            }
        )
        rep = compute_squeeze_report(engine, ticker="GME", as_of=as_of)
        # 0.25 * 1.0 = 0.25
        assert rep.squeeze_probability == pytest.approx(0.25)
        assert rep.missing_component_count == 5

    def test_hand_computed_mid_case(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("MID"))
        close_series = [(as_of - timedelta(days=i), 100.0) for i in range(20, 0, -1)]
        close_series.append((as_of, 105.0))  # +5% → 0.5
        install_fake_pit(
            {
                1: [(as_of, 15.0)],          # 0.5
                2: [(as_of, 5.0)],           # 0.5
                3: [(as_of, 15.0)],          # 0.5
                4: close_series,             # 0.5
                5: [(as_of, 0.5)],           # (0.5+1)/3 = 0.5
                6: [(as_of, 1.0)],           # positive → 0.0
            }
        )
        rep = compute_squeeze_report(engine, ticker="MID", as_of=as_of)
        # 0.25*0.5 + 0.20*0.5 + 0.20*0.5 + 0.15*0.5 + 0.10*0.5 + 0.10*0.0
        # = 0.125 + 0.100 + 0.100 + 0.075 + 0.050 + 0.000 = 0.45
        assert rep.squeeze_probability == pytest.approx(0.45)
        assert rep.advisory == "MODERATE_SQUEEZE_RISK"
        assert rep.missing_component_count == 0

    def test_components_in_canonical_order(self, install_fake_pit):
        engine = FakeEngine(_feature_ids_for("ABC"))
        install_fake_pit({})
        rep = compute_squeeze_report(
            engine, ticker="ABC", as_of=date(2026, 4, 1)
        )
        names = [c.name for c in rep.components]
        assert names == list(ssc._CANONICAL_ORDER)

    def test_weighted_contributions_sum_equals_probability(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        close_series = [(as_of - timedelta(days=i), 100.0) for i in range(20, 0, -1)]
        close_series.append((as_of, 107.0))
        install_fake_pit(
            {
                1: [(as_of, 10.0)],
                2: [(as_of, 3.0)],
                3: [(as_of, 5.0)],
                4: close_series,
                5: [(as_of, 0.0)],
                6: [(as_of, -5.0)],
            }
        )
        rep = compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        total = sum(c.weighted_contribution for c in rep.components)
        assert rep.squeeze_probability == pytest.approx(total)


# ---------------------------------------------------------------------------
# Momentum specifics
# ---------------------------------------------------------------------------


class TestMomentumLookback:
    def test_uses_20d_prior_not_10d(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        # 21 obs: most-recent = 150, 10 days ago = 120, 20 days ago = 100
        rows: list[tuple[date, float]] = []
        for i, px in enumerate([100.0] + [110.0] * 9 + [120.0] + [130.0] * 9 + [150.0]):
            rows.append((as_of - timedelta(days=20 - i), px))
        install_fake_pit({4: rows})
        rep = compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        mom = [c for c in rep.components if c.name == "momentum_20d"][0]
        # (150 - 100) / 100 = +0.5 → sub-score clamps to 1.0
        assert mom.normalized == pytest.approx(1.0)
        assert mom.raw_value == pytest.approx(0.5)

    def test_zero_denominator_yields_zero(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        rows = [(as_of - timedelta(days=i), 0.0) for i in range(20, -1, -1)]
        rows[-1] = (as_of, 100.0)  # latest != 0, but prior == 0
        install_fake_pit({4: rows})
        rep = compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        mom = [c for c in rep.components if c.name == "momentum_20d"][0]
        assert mom.raw_value == 0.0
        assert mom.normalized == 0.0

    def test_insufficient_history_is_missing(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        rows = [(as_of - timedelta(days=i), 100.0 + i) for i in range(5)]
        install_fake_pit({4: rows})
        rep = compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        # Momentum missing → counted
        assert rep.missing_component_count >= 1

    def test_negative_momentum_yields_zero_subscore(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        close = [(as_of - timedelta(days=i), 100.0) for i in range(20, 0, -1)]
        close.append((as_of, 80.0))  # -20%
        install_fake_pit({4: close})
        rep = compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        mom = [c for c in rep.components if c.name == "momentum_20d"][0]
        assert mom.normalized == 0.0


# ---------------------------------------------------------------------------
# PIT safety
# ---------------------------------------------------------------------------


class TestPITSafety:
    def test_pit_called_only_with_dates_at_or_before_as_of(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        store = install_fake_pit(
            {
                1: [(as_of, 25.0), (date(2026, 5, 1), 99.0)],  # future row
            }
        )
        compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        for call in store.calls:
            _, passed_as_of, policy = call
            assert passed_as_of == as_of
            assert policy == "LATEST_AS_OF"

    def test_future_values_not_leaked_into_raw(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        install_fake_pit(
            {
                1: [(as_of, 10.0), (date(2026, 5, 1), 99.0)],
            }
        )
        rep = compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        si = [c for c in rep.components if c.name == "short_interest_pct"][0]
        assert si.raw_value == 10.0  # not 99


# ---------------------------------------------------------------------------
# Conviction multiplier
# ---------------------------------------------------------------------------


class TestMultiplierMapping:
    def test_bullish_extreme(self):
        assert _multiplier_for(0.80, "bullish") == 1.15

    def test_bullish_high(self):
        assert _multiplier_for(0.65, "bullish") == 1.10

    def test_bullish_moderate(self):
        assert _multiplier_for(0.50, "bullish") == 1.05

    def test_bullish_low(self):
        assert _multiplier_for(0.20, "bullish") == 1.00

    def test_bearish_high_taper(self):
        assert _multiplier_for(0.80, "bearish") == 0.90

    def test_bearish_moderate_taper(self):
        assert _multiplier_for(0.50, "bearish") == 0.90

    def test_bearish_low_neutral(self):
        assert _multiplier_for(0.20, "bearish") == 1.00

    def test_unknown_direction_neutral(self):
        assert _multiplier_for(0.80, "sideways") == 1.00

    def test_empty_direction_neutral(self):
        assert _multiplier_for(0.80, "") == 1.00

    def test_synonym_long(self):
        assert _multiplier_for(0.80, "long") == 1.15

    def test_synonym_call(self):
        assert _multiplier_for(0.80, "call") == 1.15

    def test_synonym_put(self):
        assert _multiplier_for(0.80, "put") == 0.90

    def test_boundary_at_moderate(self):
        assert _multiplier_for(0.45, "bullish") == 1.05

    def test_boundary_at_high(self):
        assert _multiplier_for(0.60, "bullish") == 1.10

    def test_boundary_at_extreme(self):
        assert _multiplier_for(0.75, "bullish") == 1.15


class TestSqueezeConvictionLivePath:
    def test_bullish_on_high_prob(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        close = [(as_of - timedelta(days=i), 100.0) for i in range(20, 0, -1)]
        close.append((as_of, 120.0))
        install_fake_pit(
            {
                1: [(as_of, 50.0)],
                2: [(as_of, 20.0)],
                3: [(as_of, 60.0)],
                4: close,
                5: [(as_of, 3.0)],
                6: [(as_of, -5.0)],
            }
        )
        mult = squeeze_conviction_multiplier(
            engine, ticker="ABC", as_of=as_of, trade_direction="bullish"
        )
        assert mult == 1.15

    def test_bearish_on_high_prob(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        close = [(as_of - timedelta(days=i), 100.0) for i in range(20, 0, -1)]
        close.append((as_of, 120.0))
        install_fake_pit(
            {
                1: [(as_of, 50.0)],
                2: [(as_of, 20.0)],
                3: [(as_of, 60.0)],
                4: close,
                5: [(as_of, 3.0)],
                6: [(as_of, -5.0)],
            }
        )
        mult = squeeze_conviction_multiplier(
            engine, ticker="ABC", as_of=as_of, trade_direction="bearish"
        )
        assert mult == 0.90

    def test_unknown_direction_neutral(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        engine = FakeEngine(_feature_ids_for("ABC"))
        install_fake_pit({1: [(as_of, 50.0)]})
        mult = squeeze_conviction_multiplier(
            engine, ticker="ABC", as_of=as_of, trade_direction="sideways"
        )
        assert mult == 1.00

    def test_returns_one_on_pit_exception(self, monkeypatch):
        class _ExplodingPIT:
            def __init__(self, engine):
                pass

            def get_pit(self, *args, **kwargs):
                raise RuntimeError("boom")

        class _ExplodingEngine:
            feature_ids = {"ABC_short_interest_pct": 1}

            def connect(self):
                raise RuntimeError("db down")

        monkeypatch.setattr(ssc, "PITStore", _ExplodingPIT)
        mult = squeeze_conviction_multiplier(
            _ExplodingEngine(),
            ticker="ABC",
            as_of=date(2026, 4, 1),
            trade_direction="bullish",
        )
        assert mult == 1.0

    def test_live_path_never_raises(self, monkeypatch):
        def _broken_compute(*args, **kwargs):
            raise ValueError("bad data")

        monkeypatch.setattr(ssc, "compute_squeeze_report", _broken_compute)
        mult = squeeze_conviction_multiplier(
            FakeEngine({}),
            ticker="ABC",
            as_of=date(2026, 4, 1),
            trade_direction="bullish",
        )
        assert mult == 1.0


# ---------------------------------------------------------------------------
# Universe ranking
# ---------------------------------------------------------------------------


class TestRankUniverse:
    def test_empty_list(self, install_fake_pit):
        install_fake_pit({})
        engine = FakeEngine({})
        out = rank_universe_by_squeeze(
            engine, tickers=[], as_of=date(2026, 4, 1)
        )
        assert out == []

    def test_sorted_descending(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        # Build three tickers with varying short interest.
        fids = {}
        for i, t in enumerate(("AAA", "BBB", "CCC"), start=1):
            fids[f"{t}_short_interest_pct"] = i * 10
            fids[f"{t}_days_to_cover"] = i * 10 + 1
            fids[f"{t}_borrow_fee"] = i * 10 + 2
            fids[f"{t}_close"] = i * 10 + 3
            fids[f"{t}_reddit_mentions_z"] = i * 10 + 4
            fids[f"{t}_gex_net"] = i * 10 + 5
        engine = FakeEngine(fids)

        def _close(start_px, end_px):
            rows = [(as_of - timedelta(days=d), start_px) for d in range(20, 0, -1)]
            rows.append((as_of, end_px))
            return rows

        # AAA: very high → ~1.0
        # BBB: moderate → ~0.45
        # CCC: low → ~0.1
        pit_data: dict[int, list[tuple[date, float]]] = {
            10: [(as_of, 50.0)],
            11: [(as_of, 20.0)],
            12: [(as_of, 60.0)],
            13: _close(100.0, 120.0),
            14: [(as_of, 3.0)],
            15: [(as_of, -1.0)],
            20: [(as_of, 15.0)],
            21: [(as_of, 5.0)],
            22: [(as_of, 15.0)],
            23: _close(100.0, 105.0),
            24: [(as_of, 0.5)],
            25: [(as_of, 1.0)],
            30: [(as_of, 1.0)],
            31: [(as_of, 0.5)],
            32: [(as_of, 1.0)],
            33: _close(100.0, 99.0),
            34: [(as_of, -0.5)],
            35: [(as_of, 1.0)],
        }
        install_fake_pit(pit_data)

        out = rank_universe_by_squeeze(
            engine,
            tickers=["CCC", "AAA", "BBB"],
            as_of=as_of,
            min_probability=0.0,
        )
        assert [r.ticker for r in out] == ["AAA", "BBB", "CCC"]
        assert (
            out[0].squeeze_probability
            >= out[1].squeeze_probability
            >= out[2].squeeze_probability
        )

    def test_filters_below_min_probability(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        fids = {}
        for i, t in enumerate(("AAA", "BBB"), start=1):
            fids[f"{t}_short_interest_pct"] = i * 10
            fids[f"{t}_days_to_cover"] = i * 10 + 1
            fids[f"{t}_borrow_fee"] = i * 10 + 2
            fids[f"{t}_close"] = i * 10 + 3
            fids[f"{t}_reddit_mentions_z"] = i * 10 + 4
            fids[f"{t}_gex_net"] = i * 10 + 5
        engine = FakeEngine(fids)

        def _close(start_px, end_px):
            rows = [(as_of - timedelta(days=d), start_px) for d in range(20, 0, -1)]
            rows.append((as_of, end_px))
            return rows

        install_fake_pit(
            {
                10: [(as_of, 50.0)],
                11: [(as_of, 20.0)],
                12: [(as_of, 60.0)],
                13: _close(100.0, 120.0),
                14: [(as_of, 3.0)],
                15: [(as_of, -1.0)],
                20: [(as_of, 1.0)],
                21: [(as_of, 0.1)],
                22: [(as_of, 1.0)],
                23: _close(100.0, 99.0),
                24: [(as_of, -1.0)],
                25: [(as_of, 1.0)],
            }
        )
        out = rank_universe_by_squeeze(
            engine,
            tickers=["AAA", "BBB"],
            as_of=as_of,
            min_probability=0.45,
        )
        assert len(out) == 1
        assert out[0].ticker == "AAA"

    def test_default_min_matches_moderate_threshold(self, install_fake_pit):
        install_fake_pit({})
        engine = FakeEngine({})
        # Sanity — default should not explode
        out = rank_universe_by_squeeze(engine, tickers=["X"], as_of=date(2026, 4, 1))
        assert out == []


# ---------------------------------------------------------------------------
# Advisory + to_dict round-trip
# ---------------------------------------------------------------------------


class TestAdvisory:
    def test_extreme(self):
        assert _advisory_for(0.80) == "EXTREME_SQUEEZE_RISK"

    def test_high(self):
        assert _advisory_for(0.60) == "HIGH_SQUEEZE_RISK"

    def test_moderate(self):
        assert _advisory_for(0.45) == "MODERATE_SQUEEZE_RISK"

    def test_low(self):
        assert _advisory_for(0.10) == "LOW_SQUEEZE_RISK"


class TestToDictRoundTrip:
    def test_to_dict_shape(self):
        comp = SqueezeComponent(
            name="short_interest_pct",
            raw_value=25.0,
            normalized=25.0 / 30.0,
            weight=0.25,
            weighted_contribution=0.25 * 25.0 / 30.0,
        )
        report = SqueezeReport(
            ticker="GME",
            as_of="2026-04-01",
            components=[comp],
            squeeze_probability=0.5,
            missing_component_count=5,
            advisory="MODERATE_SQUEEZE_RISK",
        )
        d = report.to_dict()
        assert d["ticker"] == "GME"
        assert d["as_of"] == "2026-04-01"
        assert d["squeeze_probability"] == 0.5
        assert d["missing_component_count"] == 5
        assert d["advisory"] == "MODERATE_SQUEEZE_RISK"
        assert isinstance(d["components"], list)
        assert d["components"][0]["name"] == "short_interest_pct"
        assert d["components"][0]["weight"] == 0.25

    def test_to_dict_json_serializable(self):
        import json

        comp = SqueezeComponent(
            name="gex_sign",
            raw_value=-1.0,
            normalized=1.0,
            weight=0.1,
            weighted_contribution=0.1,
        )
        report = SqueezeReport(
            ticker="X",
            as_of="2026-04-01",
            components=[comp],
            squeeze_probability=0.1,
            missing_component_count=0,
            advisory="LOW_SQUEEZE_RISK",
        )
        s = json.dumps(report.to_dict())
        # Should round-trip through JSON cleanly
        loaded = json.loads(s)
        assert loaded["ticker"] == "X"
        assert loaded["components"][0]["normalized"] == 1.0


# ---------------------------------------------------------------------------
# Social-heat fallback
# ---------------------------------------------------------------------------


class TestSocialHeatFallback:
    def test_fallback_to_social_heat_z(self, install_fake_pit):
        as_of = date(2026, 4, 1)
        # Only the fallback name is registered.
        engine = FakeEngine({"ABC_social_heat_z": 99})
        install_fake_pit({99: [(as_of, 2.0)]})
        rep = compute_squeeze_report(engine, ticker="ABC", as_of=as_of)
        social = [c for c in rep.components if c.name == "social_heat_z"][0]
        assert social.raw_value == 2.0
        assert social.normalized == 1.0
