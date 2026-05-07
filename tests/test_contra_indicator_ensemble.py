"""
Tests for ``intelligence.contra_indicator_ensemble``.

The module is intentionally DB-agnostic under test: we use a ``_FakeEngine``
that hands back parameterised result sets and a ``_FakePITStore`` that
returns pre-baked DataFrames keyed by feature_id. No real PostgreSQL is
touched — the pure math helpers and data-shape plumbing can be exercised
entirely from Python.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from intelligence import contra_indicator_ensemble as contra_mod
from intelligence.contra_indicator_ensemble import (
    BEARISH_THRESHOLD,
    BULLISH_THRESHOLD,
    CONTRA_BEARISH,
    CONTRA_BULLISH,
    INDICATOR_SPECS,
    MULT_MEDIUM_ALIGNED,
    MULT_MEDIUM_OPPOSED,
    MULT_NEUTRAL,
    MULT_STRONG_ALIGNED,
    MULT_STRONG_OPPOSED,
    NORMALIZATION_WINDOW_DAYS,
    SAME_DIRECTION,
    Z_CLAMP,
    ContraEnsembleReport,
    ContraIndicator,
    _classify_bias,
    _clamp,
    _compute_zscore,
    _contribution_from_z,
    _latest_and_history,
    _multiplier_from_report,
    build_contra_report,
    contra_conviction_multiplier,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakePITStore:
    """Returns pre-baked DataFrames keyed by feature_id.

    ``get_pit([fid], as_of, vintage_policy)`` looks up ``fid`` in
    ``payloads`` and returns that DataFrame (defaulting to empty). Tracks
    ``calls`` so tests can assert caching behaviour.
    """

    def __init__(
        self,
        payloads: dict[int, pd.DataFrame] | None = None,
        raise_exc: Exception | None = None,
    ) -> None:
        self.payloads = payloads or {}
        self.calls: list[tuple[tuple[int, ...], Any, str]] = []
        self.raise_exc = raise_exc

    def get_pit(
        self,
        feature_ids: list[int],
        as_of_date,
        vintage_policy: str = "LATEST_AS_OF",
    ) -> pd.DataFrame:
        if self.raise_exc is not None:
            raise self.raise_exc
        key = tuple(feature_ids)
        self.calls.append((key, as_of_date, vintage_policy))
        if len(feature_ids) == 1:
            fid = feature_ids[0]
            return self.payloads.get(
                fid,
                pd.DataFrame(
                    columns=[
                        "feature_id",
                        "obs_date",
                        "value",
                        "release_date",
                        "vintage_date",
                    ]
                ),
            )
        # Multi-feature lookups: concatenate.
        frames = [
            self.payloads[f]
            for f in feature_ids
            if f in self.payloads
        ]
        if not frames:
            return pd.DataFrame(
                columns=[
                    "feature_id",
                    "obs_date",
                    "value",
                    "release_date",
                    "vintage_date",
                ]
            )
        return pd.concat(frames, ignore_index=True)


def _make_engine_with_registry(
    name_to_id: dict[str, int] | None = None,
    raise_exc: Exception | None = None,
) -> MagicMock:
    """Build a MagicMock engine that answers the feature_registry lookup.

    ``name_to_id`` is a dict of ``feature_registry.name -> id``. Any name
    absent from the dict will not be returned by the mock, simulating a
    missing feature row.
    """
    engine = MagicMock()

    if raise_exc is not None:
        engine.connect.side_effect = raise_exc
        return engine

    mapping = name_to_id or {}

    class _Conn:
        def __enter__(self_inner):
            return self_inner

        def __exit__(self_inner, *exc):
            return False

        def execute(self_inner, query, params=None):
            names = (params or {}).get("names", [])
            rows = [(mapping[n], n) for n in names if n in mapping]
            result = MagicMock()
            result.fetchall.return_value = rows
            return result

    engine.connect.side_effect = lambda: _Conn()
    return engine


def _series_with_history(
    feature_id: int,
    as_of: date,
    history_values: list[float],
    latest_value: float,
    *,
    with_release: bool = True,
) -> pd.DataFrame:
    """Build a PIT-shaped DataFrame.

    ``history_values`` occupy obs_dates before ``as_of`` going backward one
    day at a time, with ``latest_value`` on ``as_of - 1``.
    """
    total = history_values + [latest_value]
    rows = []
    # Walk backward so the last item is the most recent (as_of - 1 day).
    n = len(total)
    for idx, val in enumerate(total):
        obs = as_of - timedelta(days=(n - idx))
        rows.append(
            {
                "feature_id": feature_id,
                "obs_date": obs,
                "value": val,
                "release_date": obs if with_release else obs,
                "vintage_date": obs,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Pure-math helpers
# ---------------------------------------------------------------------------


class TestClamp:

    def test_inside_range(self):
        assert _clamp(0.5, -1.0, 1.0) == 0.5

    def test_below_low(self):
        assert _clamp(-5.0, -1.0, 1.0) == -1.0

    def test_above_high(self):
        assert _clamp(5.0, -1.0, 1.0) == 1.0

    def test_nan_returns_zero(self):
        assert _clamp(float("nan"), -1.0, 1.0) == 0.0


class TestComputeZscore:

    def test_hand_computed_ten_point_series(self):
        # Series that averages to 5 with known std, then drop in a latest
        # value at 8 — well above one std.
        history = pd.Series([4, 5, 6, 4, 5, 6, 4, 5, 6, 5] * 3, dtype=float)
        z = _compute_zscore(history, 8.0)
        # mean=5.0, pop std = 0.78..., so z ~ (8-5)/0.78 ~ 3.84 — clamped to 4.
        assert z > 3.0
        assert z <= Z_CLAMP

    def test_exact_mean_gives_zero(self):
        history = pd.Series([1, 2, 3] * 15, dtype=float)
        # mean = 2.0
        assert _compute_zscore(history, 2.0) == 0.0

    def test_insufficient_history_returns_zero(self):
        history = pd.Series([1.0, 2.0, 3.0])
        assert _compute_zscore(history, 5.0) == 0.0

    def test_zero_std_returns_zero(self):
        history = pd.Series([7.0] * 100)
        assert _compute_zscore(history, 42.0) == 0.0

    def test_nan_latest_returns_zero(self):
        history = pd.Series(list(range(100)), dtype=float)
        assert _compute_zscore(history, float("nan")) == 0.0

    def test_clamp_upper(self):
        history = pd.Series([0.0, 1.0] * 50)
        # huge outlier
        z = _compute_zscore(history, 1_000.0)
        assert z == Z_CLAMP

    def test_clamp_lower(self):
        history = pd.Series([0.0, 1.0] * 50)
        z = _compute_zscore(history, -1_000.0)
        assert z == -Z_CLAMP


class TestContributionFromZ:

    def test_below_threshold_is_zero(self):
        assert _contribution_from_z(1.0, 2.0, CONTRA_BEARISH) == 0.0

    def test_at_threshold_is_zero(self):
        assert _contribution_from_z(2.0, 2.0, CONTRA_BEARISH) == 0.0

    def test_retail_bullish_extreme_contributes_bearish(self):
        # AAII retail bull spread z = +3 -> contribution should be NEGATIVE
        # (lean bearish) because contrarian_direction = -1.
        contrib = _contribution_from_z(3.0, 2.0, CONTRA_BEARISH)
        assert contrib < 0.0

    def test_retail_bearish_extreme_contributes_bullish(self):
        contrib = _contribution_from_z(-3.0, 2.0, CONTRA_BEARISH)
        assert contrib > 0.0

    def test_bofa_high_z_contributes_bullish(self):
        # BofA sell-side: contrarian_direction = +1. High z -> bullish.
        contrib = _contribution_from_z(2.5, 1.5, CONTRA_BULLISH)
        assert contrib > 0.0

    def test_smart_money_same_direction_positive(self):
        # smart_money positive z should give POSITIVE contribution, not
        # flipped.
        contrib = _contribution_from_z(2.0, 1.0, SAME_DIRECTION)
        assert contrib == pytest.approx((2.0 - 1.0) / (Z_CLAMP - 1.0))
        assert contrib > 0.0

    def test_smart_money_same_direction_negative(self):
        contrib = _contribution_from_z(-2.0, 1.0, SAME_DIRECTION)
        assert contrib < 0.0

    def test_max_z_gives_max_contribution(self):
        contrib = _contribution_from_z(Z_CLAMP, 2.0, CONTRA_BULLISH)
        assert contrib == pytest.approx(1.0)

    def test_contribution_clamped_to_unit_interval(self):
        # Artificially ask with threshold just below z, magnitude still <=1.
        contrib = _contribution_from_z(Z_CLAMP, 0.0, CONTRA_BULLISH)
        assert -1.0 <= contrib <= 1.0


class TestClassifyBias:

    def test_bullish(self):
        assert _classify_bias(BULLISH_THRESHOLD + 0.01) == "bullish"

    def test_bearish(self):
        assert _classify_bias(BEARISH_THRESHOLD - 0.01) == "bearish"

    def test_neutral_zero(self):
        assert _classify_bias(0.0) == "neutral"

    def test_neutral_at_bullish_threshold(self):
        # Strict > BULLISH_THRESHOLD
        assert _classify_bias(BULLISH_THRESHOLD) == "neutral"


# ---------------------------------------------------------------------------
# Multiplier lookup
# ---------------------------------------------------------------------------


def _mk_report(
    *,
    score: float,
    n_active: int,
    bias: str | None = None,
) -> ContraEnsembleReport:
    inds = [
        ContraIndicator(
            name=spec.name,
            feature_registry_name=spec.feature_registry_name,
            z_score=0.0,
            threshold=spec.threshold,
            contrarian_direction=spec.contrarian_direction,
            contribution=0.0,
            active=False,
        )
        for spec in INDICATOR_SPECS
    ]
    return ContraEnsembleReport(
        as_of="2026-04-14",
        indicators=inds,
        ensemble_score=score,
        n_active=n_active,
        contrarian_bias=bias or _classify_bias(score),
        advisory="",
        missing_indicator_count=0,
    )


class TestMultiplier:

    def test_strong_aligned_bullish(self):
        r = _mk_report(score=0.6, n_active=4)
        assert _multiplier_from_report(r, "bullish") == MULT_STRONG_ALIGNED

    def test_medium_aligned_bullish(self):
        r = _mk_report(score=0.3, n_active=3)
        assert _multiplier_from_report(r, "bullish") == MULT_MEDIUM_ALIGNED

    def test_medium_opposed_bullish(self):
        # Contrarian bias bearish, trade bullish, |score| 0.3 -> MEDIUM opp.
        r = _mk_report(score=-0.3, n_active=3)
        assert _multiplier_from_report(r, "bullish") == MULT_MEDIUM_OPPOSED

    def test_strong_opposed_bullish(self):
        r = _mk_report(score=-0.6, n_active=4)
        assert _multiplier_from_report(r, "bullish") == MULT_STRONG_OPPOSED

    def test_small_abs_score_is_neutral(self):
        r = _mk_report(score=0.05, n_active=5)
        assert _multiplier_from_report(r, "bullish") == MULT_NEUTRAL

    def test_unknown_direction_is_neutral(self):
        r = _mk_report(score=0.6, n_active=4)
        assert _multiplier_from_report(r, "flat") == MULT_NEUTRAL
        assert _multiplier_from_report(r, "") == MULT_NEUTRAL
        assert _multiplier_from_report(r, "sideways") == MULT_NEUTRAL

    def test_aligned_bearish_strong(self):
        r = _mk_report(score=-0.6, n_active=5)
        assert _multiplier_from_report(r, "bearish") == MULT_STRONG_ALIGNED

    def test_aligned_below_n_active(self):
        # Strong score but only 2 active -> no uplift.
        r = _mk_report(score=0.6, n_active=2)
        assert _multiplier_from_report(r, "bullish") == MULT_NEUTRAL


# ---------------------------------------------------------------------------
# _latest_and_history PIT safety
# ---------------------------------------------------------------------------


class TestLatestAndHistory:

    def test_rolling_window_excludes_as_of(self):
        as_of = date(2026, 4, 14)
        # observations on as_of-1, as_of, as_of+1 — the latter two must drop.
        rows = [
            {
                "feature_id": 1,
                "obs_date": as_of - timedelta(days=1),
                "value": 10.0,
                "release_date": as_of - timedelta(days=1),
                "vintage_date": as_of - timedelta(days=1),
            },
            {
                "feature_id": 1,
                "obs_date": as_of,
                "value": 999.0,
                "release_date": as_of,
                "vintage_date": as_of,
            },
            {
                "feature_id": 1,
                "obs_date": as_of + timedelta(days=1),
                "value": 999.0,
                "release_date": as_of + timedelta(days=1),
                "vintage_date": as_of + timedelta(days=1),
            },
        ]
        df = pd.DataFrame(rows)
        latest, history = _latest_and_history(df, as_of)
        assert latest == 10.0
        assert len(history) == 0  # only one obs, history excludes it

    def test_window_length_respected(self):
        as_of = date(2026, 4, 14)
        # 400 days of history — should be trimmed to 252.
        rows = []
        for d_back in range(1, 401):
            obs = as_of - timedelta(days=d_back)
            rows.append(
                {
                    "feature_id": 1,
                    "obs_date": obs,
                    "value": float(d_back),
                    "release_date": obs,
                    "vintage_date": obs,
                }
            )
        df = pd.DataFrame(rows)
        latest, history = _latest_and_history(df, as_of)
        # Window starts at as_of - 252d (inclusive). We then peel off the
        # latest value, so history holds 252 - 1 = 251 points.
        assert latest is not None
        assert len(history) == 251

    def test_empty_df_returns_none(self):
        latest, history = _latest_and_history(pd.DataFrame(), date(2026, 4, 14))
        assert latest is None
        assert history is None


# ---------------------------------------------------------------------------
# Integration — build_contra_report
# ---------------------------------------------------------------------------


def _populate_all_six(
    as_of: date,
    *,
    # (z_direction, magnitude) per indicator or None to leave neutral.
    aaii_shift: float = 0.0,
    bofa_shift: float = 0.0,
    pcr_shift: float = 0.0,
    retail_shift: float = 0.0,
    smart_shift: float = 0.0,
    cftc_shift: float = 0.0,
) -> tuple[dict[int, pd.DataFrame], dict[str, int]]:
    """Build a payload dict + name->id map for all six indicators.

    ``*_shift`` is added to the latest observation of that indicator on top
    of a flat-100.0 history; the z_score will be ``shift / std``. Use zero
    to keep an indicator inactive.
    """
    name_to_id = {
        spec.feature_registry_name: (idx + 1)
        for idx, spec in enumerate(INDICATOR_SPECS)
    }
    shifts = {
        "aaii_bull_bear_spread": aaii_shift,
        "bofa_sellside_indicator": bofa_shift,
        "put_call_ratio_10d": pcr_shift,
        "retail_options_net_call_volume": retail_shift,
        "smart_money_flow_index": smart_shift,
        "cftc_noncomm_net_long": cftc_shift,
    }
    payloads: dict[int, pd.DataFrame] = {}
    for spec in INDICATOR_SPECS:
        fid = name_to_id[spec.feature_registry_name]
        # Small-noise history so std is non-zero but predictable.
        history = [100.0 + (i % 5) * 0.1 for i in range(80)]
        latest = 100.0 + shifts[spec.feature_registry_name]
        payloads[fid] = _series_with_history(
            fid, as_of, history, latest,
        )
    return payloads, name_to_id


class TestBuildContraReportIntegration:

    def test_all_missing_returns_neutral(self):
        as_of = date(2026, 4, 14)
        engine = _make_engine_with_registry({})  # no features resolvable
        pit = _FakePITStore()
        report = build_contra_report(engine, as_of=as_of, pit_store=pit)
        assert report.ensemble_score == 0.0
        assert report.n_active == 0
        assert report.contrarian_bias == "neutral"
        assert report.missing_indicator_count == len(INDICATOR_SPECS)
        assert len(report.indicators) == len(INDICATOR_SPECS)

    def test_happy_path_bearish_retail_euphoria(self):
        as_of = date(2026, 4, 14)
        # Drive AAII + retail calls + CFTC long all strongly positive.
        # These three are CONTRA_BEARISH, so ensemble should tilt bearish.
        payloads, mapping = _populate_all_six(
            as_of,
            aaii_shift=50.0,   # massive spike -> z clamps at +4
            retail_shift=50.0,
            cftc_shift=50.0,
        )
        engine = _make_engine_with_registry(mapping)
        pit = _FakePITStore(payloads)
        report = build_contra_report(engine, as_of=as_of, pit_store=pit)
        assert report.missing_indicator_count == 0
        assert report.ensemble_score < 0.0  # bearish lean
        assert report.n_active >= 3
        assert report.contrarian_bias == "bearish"

    def test_happy_path_bullish_wall_of_worry(self):
        as_of = date(2026, 4, 14)
        # Drive BofA sell-side + put/call both high; contrarian bullish.
        payloads, mapping = _populate_all_six(
            as_of,
            bofa_shift=50.0,
            pcr_shift=50.0,
        )
        engine = _make_engine_with_registry(mapping)
        pit = _FakePITStore(payloads)
        report = build_contra_report(engine, as_of=as_of, pit_store=pit)
        assert report.ensemble_score > 0.0
        assert report.contrarian_bias == "bullish"
        assert report.n_active >= 2

    def test_smart_money_same_direction_does_not_flip(self):
        as_of = date(2026, 4, 14)
        # ONLY drive smart_money positive. Same-direction -> bullish tilt.
        payloads, mapping = _populate_all_six(
            as_of, smart_shift=50.0,
        )
        engine = _make_engine_with_registry(mapping)
        pit = _FakePITStore(payloads)
        report = build_contra_report(engine, as_of=as_of, pit_store=pit)
        sm = [i for i in report.indicators if i.name == "smart_money_flow_index"][0]
        assert sm.active is True
        assert sm.contribution > 0.0  # same-direction positive

    def test_feature_not_in_registry_is_missing(self):
        as_of = date(2026, 4, 14)
        # Only resolve one of the six features.
        mapping = {"aaii_bull_bear_spread": 1}
        payloads = {
            1: _series_with_history(
                1, as_of, [100.0] * 80, 100.0
            )
        }
        engine = _make_engine_with_registry(mapping)
        pit = _FakePITStore(payloads)
        report = build_contra_report(engine, as_of=as_of, pit_store=pit)
        assert report.missing_indicator_count == len(INDICATOR_SPECS) - 1
        # The one resolved indicator sits at z=0 (flat history & latest),
        # so it's inactive; however it's NOT counted as missing.
        aaii = [
            i for i in report.indicators
            if i.name == "aaii_bull_bear_spread"
        ][0]
        assert aaii.active is False

    def test_pit_exception_neutralises_indicator(self):
        as_of = date(2026, 4, 14)
        mapping = {spec.feature_registry_name: idx + 1
                   for idx, spec in enumerate(INDICATOR_SPECS)}
        engine = _make_engine_with_registry(mapping)
        pit = _FakePITStore(raise_exc=RuntimeError("pit broke"))
        report = build_contra_report(engine, as_of=as_of, pit_store=pit)
        # Every indicator should be treated as missing.
        assert report.missing_indicator_count == len(INDICATOR_SPECS)
        assert report.ensemble_score == 0.0
        assert report.n_active == 0

    def test_pit_cache_hits_once_per_feature(self):
        as_of = date(2026, 4, 14)
        payloads, mapping = _populate_all_six(as_of)
        engine = _make_engine_with_registry(mapping)
        pit = _FakePITStore(payloads)
        build_contra_report(engine, as_of=as_of, pit_store=pit)
        # One call per distinct feature_id — 6 indicators -> 6 distinct ids.
        assert len(pit.calls) == len(INDICATOR_SPECS)
        distinct_keys = {c[0] for c in pit.calls}
        assert len(distinct_keys) == len(INDICATOR_SPECS)

    def test_engine_exception_returns_neutral(self):
        as_of = date(2026, 4, 14)
        engine = _make_engine_with_registry(
            raise_exc=RuntimeError("db down"),
        )
        pit = _FakePITStore()
        report = build_contra_report(engine, as_of=as_of, pit_store=pit)
        assert report.ensemble_score == 0.0
        assert report.missing_indicator_count == len(INDICATOR_SPECS)
        assert report.contrarian_bias == "neutral"

    def test_report_to_dict_round_trip(self):
        as_of = date(2026, 4, 14)
        payloads, mapping = _populate_all_six(as_of, bofa_shift=50.0)
        engine = _make_engine_with_registry(mapping)
        pit = _FakePITStore(payloads)
        report = build_contra_report(engine, as_of=as_of, pit_store=pit)
        d = report.to_dict()
        # Top-level fields.
        for key in (
            "as_of",
            "indicators",
            "ensemble_score",
            "n_active",
            "contrarian_bias",
            "advisory",
            "missing_indicator_count",
        ):
            assert key in d
        # Indicator fields.
        assert len(d["indicators"]) == len(INDICATOR_SPECS)
        for ind in d["indicators"]:
            for key in (
                "name",
                "feature_registry_name",
                "z_score",
                "threshold",
                "contrarian_direction",
                "contribution",
                "active",
            ):
                assert key in ind


# ---------------------------------------------------------------------------
# contra_conviction_multiplier
# ---------------------------------------------------------------------------


class TestContraConvictionMultiplier:

    def test_neutral_report_returns_one(self):
        as_of = date(2026, 4, 14)
        engine = _make_engine_with_registry({})
        mult = contra_conviction_multiplier(
            engine, as_of=as_of, trade_direction="bullish"
        )
        assert mult == MULT_NEUTRAL

    def test_engine_exception_returns_one(self, monkeypatch):
        as_of = date(2026, 4, 14)

        def _boom(*a, **k):
            raise RuntimeError("bad")

        monkeypatch.setattr(contra_mod, "build_contra_report", _boom)
        engine = MagicMock()
        mult = contra_conviction_multiplier(
            engine, as_of=as_of, trade_direction="bullish"
        )
        assert mult == MULT_NEUTRAL

    def test_pit_exception_returns_one(self, monkeypatch):
        as_of = date(2026, 4, 14)
        engine = _make_engine_with_registry(
            raise_exc=RuntimeError("db down"),
        )
        mult = contra_conviction_multiplier(
            engine, as_of=as_of, trade_direction="bullish"
        )
        assert mult == MULT_NEUTRAL

    def test_unknown_direction_returns_one(self, monkeypatch):
        as_of = date(2026, 4, 14)
        # Stub build_contra_report with a strong-signal report so any
        # non-neutral result would be obvious.
        strong = _mk_report(score=0.8, n_active=5)
        monkeypatch.setattr(
            contra_mod, "build_contra_report", lambda *a, **k: strong
        )
        engine = MagicMock()
        assert contra_conviction_multiplier(
            engine, as_of=as_of, trade_direction="flat"
        ) == MULT_NEUTRAL
        assert contra_conviction_multiplier(
            engine, as_of=as_of, trade_direction=""
        ) == MULT_NEUTRAL

    def test_aligned_strong_bullish(self, monkeypatch):
        as_of = date(2026, 4, 14)
        strong = _mk_report(score=0.6, n_active=4)
        monkeypatch.setattr(
            contra_mod, "build_contra_report", lambda *a, **k: strong
        )
        engine = MagicMock()
        mult = contra_conviction_multiplier(
            engine, as_of=as_of, trade_direction="bullish"
        )
        assert mult == MULT_STRONG_ALIGNED


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------


class TestConstants:

    def test_six_indicators(self):
        assert len(INDICATOR_SPECS) == 6

    def test_thresholds_match_spec(self):
        lookup = {s.name: s for s in INDICATOR_SPECS}
        assert lookup["aaii_bull_bear_spread"].threshold == 2.0
        assert lookup["bofa_sellside_indicator"].threshold == 1.5
        assert lookup["put_call_ratio_10d"].threshold == 1.5
        assert lookup["retail_options_net_call_volume"].threshold == 2.0
        assert lookup["smart_money_flow_index"].threshold == 1.0
        assert lookup["cftc_noncomm_net_long"].threshold == 2.0

    def test_contrarian_directions_match_spec(self):
        lookup = {s.name: s for s in INDICATOR_SPECS}
        assert lookup["aaii_bull_bear_spread"].contrarian_direction == -1
        assert lookup["bofa_sellside_indicator"].contrarian_direction == 1
        assert lookup["put_call_ratio_10d"].contrarian_direction == 1
        assert lookup["retail_options_net_call_volume"].contrarian_direction == -1
        assert lookup["smart_money_flow_index"].contrarian_direction == 0
        assert lookup["cftc_noncomm_net_long"].contrarian_direction == -1

    def test_multiplier_constants_bounded(self):
        assert MULT_STRONG_OPPOSED < MULT_MEDIUM_OPPOSED < MULT_NEUTRAL
        assert MULT_NEUTRAL < MULT_MEDIUM_ALIGNED < MULT_STRONG_ALIGNED

    def test_normalization_window_is_one_year(self):
        assert NORMALIZATION_WINDOW_DAYS == 252
