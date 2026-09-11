"""``intelligence/catalyst_ev.py`` — scoring the event instead of the trend.

The Long Plays board ranked multi-year candidates by extrapolating
historical CAGR/vol, which reads a 90 % drawdown as the forward
distribution and so disqualifies exactly the setup that produces a
multi-bagger. This file pins the replacement: base-rate-anchored
P(success), a net-cash downside floor, an options-implied upside, the
runway-covers-catalyst gate, and the EV that combines them.

Pure functions only — no DB, no network.
"""
from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence import catalyst_ev as ce

AS_OF = date(2026, 9, 10)


# ── normalize_phase / lookups ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("PHASE3", "PHASE3"),
        ("Phase 3", "PHASE3"),
        ("phase-3", "PHASE3"),
        ("PHASE2", "PHASE2"),
        ("PHASE1", "PHASE1"),
        # a combined phase takes the earlier, harder leg
        ("PHASE2/PHASE3", "PHASE2"),
        ("PHASE1/PHASE2", "PHASE1"),
        ("NDA", "FILED"),
        ("BLA submitted", "FILED"),
        ("PHASE4", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_phase(raw: Any, expected: str | None) -> None:
    assert ce.normalize_phase(raw) == expected


def test_indication_multiplier_is_tolerant_of_free_text() -> None:
    assert ce.indication_multiplier("oncology") == 0.67
    assert ce.indication_multiplier("Oncology") == 0.67
    assert ce.indication_multiplier("metastatic oncology, 2L") == 0.67
    assert ce.indication_multiplier("rare-disease") == 1.20
    assert ce.indication_multiplier("something unmapped") == 1.0
    assert ce.indication_multiplier(None) == 1.0
    # oncology is harder than average, haematology easier — the direction matters
    assert ce.indication_multiplier("oncology") < 1.0 < ce.indication_multiplier("hematology")


@pytest.mark.parametrize(
    "condition, area",
    [
        ("Melanoma", "oncology"),
        ("Metastatic Non-Small Cell Lung Cancer", "oncology"),
        # a haematologic malignancy is oncology risk, not the 1.35 of
        # non-malignant haematology — mapping it otherwise inflates the prior
        ("Chronic Lymphocytic Leukemia", "oncology"),
        ("Relapsed Multiple Myeloma", "oncology"),
        ("Sickle Cell Disease", "hematology"),
        ("Hemophilia A", "hematology"),
        ("Alzheimer Disease", "neurology"),
        ("Relapsing Multiple Sclerosis", "neurology"),
        ("Major Depressive Disorder", "psychiatry"),
        ("Type 2 Diabetes Mellitus", "metabolic"),
        ("Geographic Atrophy Secondary to Macular Degeneration", "ophthalmology"),
        ("Cystic Fibrosis", "respiratory"),
        ("Ulcerative Colitis", "autoimmune"),
    ],
)
def test_ct_gov_condition_names_map_onto_a_therapeutic_area(condition: str, area: str) -> None:
    """A registry disease name must not silently score a neutral 1.0."""
    assert ce.indication_multiplier(condition) == ce.INDICATION_LOA_MULTIPLIER[area]


def test_an_unrecognised_condition_is_neutral_not_guessed() -> None:
    assert ce.indication_multiplier("Idiopathic Widget Deficiency") == 1.0


def test_designation_multiplier_takes_the_strongest_match() -> None:
    assert ce.designation_multiplier("Breakthrough Therapy") == 1.25
    assert ce.designation_multiplier("Fast Track") == 1.10
    # a trial can carry several; the strongest wins
    assert ce.designation_multiplier("Orphan Drug, Breakthrough Therapy") == 1.25
    assert ce.designation_multiplier(None) == 1.0
    assert ce.designation_multiplier("") == 1.0
    assert ce.designation_multiplier("not a real designation") == 1.0


# ── success_probability ───────────────────────────────────────────────────


def test_bare_phase_returns_the_base_rate_and_says_it_is_a_prior() -> None:
    out = ce.success_probability(phase="PHASE3")
    assert out["p_success"] == pytest.approx(0.58)
    assert out["p_success_base_rate"] == pytest.approx(0.58)
    assert out["p_success_phase"] == "PHASE3"
    assert out["p_success_factors"] == {}
    assert out["p_success_basis"] == "literature_prior"


def test_phase2_is_the_hardest_gate() -> None:
    p1 = ce.success_probability(phase="PHASE1")["p_success"]
    p2 = ce.success_probability(phase="PHASE2")["p_success"]
    p3 = ce.success_probability(phase="PHASE3")["p_success"]
    filed = ce.success_probability(phase="FILED")["p_success"]
    assert p2 < p1 < p3 < filed


def test_unknown_phase_falls_back_to_the_default_rate() -> None:
    out = ce.success_probability(phase="PHASE4")
    assert out["p_success"] == pytest.approx(ce.DEFAULT_PHASE_BASE_RATE)
    assert out["p_success_phase"] is None
    assert ce.success_probability(phase=None)["p_success"] == pytest.approx(ce.DEFAULT_PHASE_BASE_RATE)


def test_evidence_moves_the_prior_in_the_right_direction() -> None:
    base = ce.success_probability(phase="PHASE3")["p_success"]

    onc = ce.success_probability(phase="PHASE3", indication="oncology")["p_success"]
    assert onc < base

    btd = ce.success_probability(phase="PHASE3", fda_designation="Breakthrough Therapy")["p_success"]
    assert btd > base

    clear = ce.success_probability(phase="PHASE3", endpoint_clarity=1.0)["p_success"]
    murky = ce.success_probability(phase="PHASE3", endpoint_clarity=0.0)["p_success"]
    assert murky < base < clear
    # 0.5 clarity is neutral and must not move the prior at all
    assert ce.success_probability(phase="PHASE3", endpoint_clarity=0.5)["p_success"] == pytest.approx(base)

    full = ce.success_probability(phase="PHASE3", enrollment_pct=100.0)["p_success"]
    thin = ce.success_probability(phase="PHASE3", enrollment_pct=20.0)["p_success"]
    assert thin < base < full
    # mid-enrollment is neutral
    assert ce.success_probability(phase="PHASE3", enrollment_pct=75.0)["p_success"] == pytest.approx(base)


def test_factors_record_every_applied_modifier() -> None:
    out = ce.success_probability(
        phase="PHASE3",
        indication="oncology",
        endpoint_clarity=0.9,
        fda_designation="Fast Track",
        enrollment_pct=100.0,
    )
    assert set(out["p_success_factors"]) == {
        "indication", "endpoint_clarity", "fda_designation", "enrollment",
    }
    # the row can be re-derived from what it carries
    rebuilt = out["p_success_base_rate"]
    for mult in out["p_success_factors"].values():
        rebuilt *= mult
    assert out["p_success"] == pytest.approx(round(rebuilt, 4), abs=1e-3)


def test_probability_is_bounded_no_matter_how_favourable_the_evidence() -> None:
    stacked = ce.success_probability(
        phase="FILED",
        indication="hematology",
        endpoint_clarity=1.0,
        fda_designation="Breakthrough Therapy",
        enrollment_pct=100.0,
    )
    assert stacked["p_success"] <= ce.P_SUCCESS_CEILING
    floored = ce.success_probability(
        phase="PHASE1", indication="oncology", endpoint_clarity=0.0, enrollment_pct=1.0,
    )
    assert floored["p_success"] >= ce.P_SUCCESS_FLOOR


def test_nan_and_garbage_inputs_degrade_to_the_base_rate() -> None:
    out = ce.success_probability(
        phase="PHASE3", endpoint_clarity=float("nan"), enrollment_pct=float("inf"),
    )
    assert out["p_success"] == pytest.approx(0.58)


# ── empirical_phase_outcomes: retiring the literature priors ──────────────


def test_empirical_outcomes_need_enough_samples_before_they_replace_the_prior() -> None:
    thin = [{"trial_phase": "PHASE3", "fwd_return_30d": 0.4} for _ in range(5)]
    fitted, counts = ce.empirical_phase_outcomes(thin, min_samples=30)
    assert fitted is None
    assert counts == {"PHASE3": 5}


def test_empirical_outcomes_fit_a_rate_per_phase() -> None:
    rows = (
        [{"trial_phase": "PHASE3", "fwd_return_30d": 0.5}] * 30
        + [{"trial_phase": "PHASE3", "fwd_return_30d": -0.3}] * 10
        + [{"trial_phase": "PHASE2", "fwd_return_30d": 0.1}] * 3   # under the floor
    )
    fitted, counts = ce.empirical_phase_outcomes(rows, min_samples=30)
    assert fitted == {"PHASE3": pytest.approx(0.75)}
    assert counts == {"PHASE3": 40, "PHASE2": 3}
    # and a fitted table actually drives the probability
    out = ce.success_probability(phase="PHASE3", base_rates=fitted, basis=ce.BASIS_EMPIRICAL)
    assert out["p_success"] == pytest.approx(0.75)
    assert out["p_success_basis"] == "grid_realized_outcomes"


def test_empirical_outcomes_ignore_unscored_and_unphased_rows() -> None:
    rows = [
        {"trial_phase": "PHASE3", "fwd_return_30d": None},
        {"trial_phase": None, "fwd_return_30d": 0.5},
        {"trial_phase": "PHASE3", "fwd_return_30d": 0.5},
    ]
    _, counts = ce.empirical_phase_outcomes(rows, min_samples=1)
    assert counts == {"PHASE3": 1}


# ── downside: the net-cash floor ──────────────────────────────────────────


def test_downside_is_the_haircut_net_cash_floor() -> None:
    out = ce.downside_multiple(price=10.0, net_cash_per_share=8.0, failure_haircut=0.5)
    assert out is not None
    assert out["downside_multiple"] == pytest.approx(0.4)   # 8 * 0.5 / 10
    assert out["net_cash_to_price"] == pytest.approx(0.8)


def test_burn_to_the_catalyst_eats_the_floor() -> None:
    dry = ce.downside_multiple(
        price=10.0, net_cash_per_share=8.0, months_to_catalyst=6.0,
        monthly_burn_per_share=1.0, failure_haircut=0.5,
    )
    assert dry is not None
    assert dry["burn_to_catalyst_per_share"] == pytest.approx(6.0)
    assert dry["downside_multiple"] == pytest.approx(0.1)   # (8 - 6) * 0.5 / 10
    # burn can exhaust the floor entirely, never take it negative
    gone = ce.downside_multiple(
        price=10.0, net_cash_per_share=2.0, months_to_catalyst=12.0,
        monthly_burn_per_share=1.0,
    )
    assert gone is not None and gone["downside_multiple"] == 0.0


def test_downside_is_capped_at_one_and_needs_both_anchors() -> None:
    # a name trading below its own cash cannot have a "downside" above 1x price
    rich = ce.downside_multiple(price=1.0, net_cash_per_share=10.0, failure_haircut=1.0)
    assert rich is not None and rich["downside_multiple"] == 1.0
    # missing anchors produce no row rather than a silent zero-risk read
    assert ce.downside_multiple(price=None, net_cash_per_share=5.0) is None
    assert ce.downside_multiple(price=10.0, net_cash_per_share=None) is None
    assert ce.downside_multiple(price=0.0, net_cash_per_share=5.0) is None
    # negative net cash (debt-laden) is a real zero floor, not a missing one
    broke = ce.downside_multiple(price=10.0, net_cash_per_share=-4.0)
    assert broke is not None and broke["downside_multiple"] == 0.0


# ── upside: inverted from the option chain ────────────────────────────────


def test_norm_ppf_matches_known_quantiles() -> None:
    assert ce._norm_ppf(0.5) == pytest.approx(0.0, abs=1e-9)
    assert ce._norm_ppf(0.975) == pytest.approx(1.959964, abs=1e-5)
    assert ce._norm_ppf(0.025) == pytest.approx(-1.959964, abs=1e-5)
    assert ce._norm_ppf(0.85) == pytest.approx(1.036433, abs=1e-5)
    assert ce._norm_ppf(0.001) == pytest.approx(-3.090232, abs=1e-5)
    assert ce._norm_ppf(0.0) == -math.inf and ce._norm_ppf(1.0) == math.inf


def test_upside_round_trips_through_the_existing_implied_prob_module() -> None:
    """The target we quote back must be the one the market actually prices.

    ``upside_multiple_from_iv`` inverts the same lognormal that
    ``market_implied_prob.options_implied_probability_from_iv`` evaluates,
    so feeding the target price back in must return the tail probability
    we asked for. This is the invariant that keeps the two modules honest
    about each other.
    """
    from intelligence.market_implied_prob import options_implied_probability_from_iv

    checked = 0
    for tail in (0.05, 0.15, 0.30):
        for iv in (0.6, 1.2, 2.0):
            out = ce.upside_multiple_from_iv(
                spot=10.0, iv=iv, days_to_expiry=120, tail_probability=tail,
            )
            if out is None:
                continue  # degenerate: the tail is not above spot at this vol
            back = options_implied_probability_from_iv(
                spot=10.0, target_price=out["upside_target_price"],
                iv=iv, days_to_expiry=120,
            )
            assert back == pytest.approx(tail, abs=1e-3)
            checked += 1
    assert checked >= 7, "the round-trip must actually exercise most of the grid"


def test_a_tail_below_spot_is_refused_rather_than_returned_as_upside() -> None:
    """A 200 %-IV lognormal has its median well below spot.

    At a fat tail probability the solved target lands under spot, which is
    not an upside — quoting it as one would understate the bet. The
    function refuses instead.
    """
    degenerate = ce.upside_multiple_from_iv(
        spot=10.0, iv=2.0, days_to_expiry=120, tail_probability=0.30,
    )
    assert degenerate is None
    # the default 0.15 tail stays in the right tail even at that vol
    usable = ce.upside_multiple_from_iv(spot=10.0, iv=2.0, days_to_expiry=120)
    assert usable is not None and usable["upside_multiple"] > 1.0


def test_upside_grows_with_vol_and_time() -> None:
    low = ce.upside_multiple_from_iv(spot=10.0, iv=0.5, days_to_expiry=90)
    high = ce.upside_multiple_from_iv(spot=10.0, iv=2.5, days_to_expiry=90)
    assert low is not None and high is not None
    assert high["upside_multiple"] > low["upside_multiple"] > 1.0

    near = ce.upside_multiple_from_iv(spot=10.0, iv=1.2, days_to_expiry=30)
    far = ce.upside_multiple_from_iv(spot=10.0, iv=1.2, days_to_expiry=365)
    assert near is not None and far is not None
    assert far["upside_multiple"] > near["upside_multiple"]


def test_a_fatter_tail_probability_means_a_nearer_target() -> None:
    aggressive = ce.upside_multiple_from_iv(spot=10.0, iv=1.0, days_to_expiry=180, tail_probability=0.05)
    modest = ce.upside_multiple_from_iv(spot=10.0, iv=1.0, days_to_expiry=180, tail_probability=0.35)
    assert aggressive is not None and modest is not None
    assert aggressive["upside_multiple"] > modest["upside_multiple"]


def test_upside_returns_none_rather_than_a_default_on_bad_input() -> None:
    assert ce.upside_multiple_from_iv(spot=None, iv=1.0, days_to_expiry=90) is None
    assert ce.upside_multiple_from_iv(spot=10.0, iv=None, days_to_expiry=90) is None
    assert ce.upside_multiple_from_iv(spot=10.0, iv=1.0, days_to_expiry=0) is None
    assert ce.upside_multiple_from_iv(spot=10.0, iv=-1.0, days_to_expiry=90) is None
    assert ce.upside_multiple_from_iv(spot=10.0, iv=float("nan"), days_to_expiry=90) is None
    out = ce.upside_multiple_from_iv(spot=10.0, iv=1.0, days_to_expiry=90)
    assert out is not None and out["upside_basis"] == "options_iv_lognormal_tail"


# ── the runway gate ───────────────────────────────────────────────────────


def test_runway_must_cover_the_catalyst_plus_a_buffer() -> None:
    ok = ce.runway_covers_catalyst(runway_months=12.0, months_to_catalyst=4.0)
    assert ok["runway_covers_catalyst"] is True
    assert ok["runway_margin_months"] == pytest.approx(8.0)

    tight = ce.runway_covers_catalyst(runway_months=5.0, months_to_catalyst=4.0)
    assert tight["runway_covers_catalyst"] is False   # 1.0 mo margin < 3.0 buffer
    assert "margin" in tight["reason"]

    exact = ce.runway_covers_catalyst(runway_months=7.0, months_to_catalyst=4.0)
    assert exact["runway_covers_catalyst"] is True    # margin == buffer passes


def test_olma_shape_passes_the_gate_the_absolute_floor_rejected() -> None:
    """3.2 months of runway, readout ~1.7 months out.

    The absolute ``cash_runway_score >= 0.4`` floor (~9.6 months) rejects
    this. The cash still covers the readout, which is the question that
    decides whether the equity survives to the event.
    """
    out = ce.runway_covers_catalyst(runway_months=3.2, months_to_catalyst=1.7, buffer_months=1.0)
    assert out["runway_covers_catalyst"] is True
    assert out["runway_margin_months"] == pytest.approx(1.5)
    # and at the default 3-month buffer it is honestly too tight
    assert ce.runway_covers_catalyst(runway_months=3.2, months_to_catalyst=1.7)["runway_covers_catalyst"] is False


def test_unknown_runway_fails_closed() -> None:
    for kw in (
        {"runway_months": None, "months_to_catalyst": 4.0},
        {"runway_months": 12.0, "months_to_catalyst": None},
        {"runway_months": 12.0, "months_to_catalyst": -2.0},
    ):
        out = ce.runway_covers_catalyst(**kw)  # type: ignore[arg-type]
        assert out["runway_covers_catalyst"] is False
        assert out["runway_margin_months"] is None


def test_months_between() -> None:
    assert ce.months_between(date(2026, 9, 10), date(2026, 10, 31)) == pytest.approx(1.67, abs=0.02)
    assert ce.months_between(date(2026, 9, 10), date(2026, 9, 10)) == 0.0
    assert ce.months_between(date(2026, 9, 10), date(2026, 8, 10)) < 0


# ── expected value ────────────────────────────────────────────────────────


def test_ev_is_the_probability_weighted_multiple() -> None:
    out = ce.catalyst_ev(p_success=0.5, upside_multiple=5.0, downside_multiple=0.4)
    assert out is not None
    assert out["expected_value_multiple"] == pytest.approx(0.5 * 5.0 + 0.5 * 0.4)
    assert out["reward_to_risk"] == pytest.approx(4.0 / 0.6, abs=1e-3)
    # the probability at which the bet is exactly fair
    assert out["breakeven_probability"] == pytest.approx(0.6 / 4.6, abs=1e-3)


def test_breakeven_probability_is_the_ev_equals_one_point() -> None:
    up, down = 8.0, 0.25
    out = ce.catalyst_ev(p_success=0.5, upside_multiple=up, downside_multiple=down)
    assert out is not None
    p_star = out["breakeven_probability"]
    ev_at_star = ce.catalyst_ev(p_success=p_star, upside_multiple=up, downside_multiple=down)
    assert ev_at_star is not None
    assert ev_at_star["expected_value_multiple"] == pytest.approx(1.0, abs=1e-3)


def test_edge_vs_market_is_grid_minus_the_option_chain() -> None:
    out = ce.catalyst_ev(
        p_success=0.58, upside_multiple=4.0, downside_multiple=0.3, market_implied_p=0.22,
    )
    assert out is not None
    assert out["market_implied_probability"] == pytest.approx(0.22)
    assert out["edge_vs_market"] == pytest.approx(0.36)
    # a market that prices the readout as *more* likely than the evidence is negative edge
    against = ce.catalyst_ev(
        p_success=0.20, upside_multiple=4.0, downside_multiple=0.3, market_implied_p=0.45,
    )
    assert against is not None and against["edge_vs_market"] < 0
    # absent a chain there is simply no edge claim
    no_chain = ce.catalyst_ev(p_success=0.5, upside_multiple=4.0, downside_multiple=0.3)
    assert no_chain is not None and "edge_vs_market" not in no_chain


def test_a_partial_ev_is_no_ev() -> None:
    assert ce.catalyst_ev(p_success=None, upside_multiple=4.0, downside_multiple=0.3) is None
    assert ce.catalyst_ev(p_success=0.5, upside_multiple=None, downside_multiple=0.3) is None
    assert ce.catalyst_ev(p_success=0.5, upside_multiple=4.0, downside_multiple=None) is None
    assert ce.catalyst_ev(p_success=float("nan"), upside_multiple=4.0, downside_multiple=0.3) is None


def test_ev_ranks_the_crashed_catalyst_name_above_the_flat_compounder() -> None:
    """The whole point: the old model preferred the compounder.

    A beaten-down microcap with a Phase 3 readout and a cash floor beats a
    flat name with no event on expected value, even though its historical
    CAGR and drawdown are far worse — which is exactly the ranking the
    trend-extrapolation proxy could not produce.
    """
    crashed = ce.catalyst_ev(
        p_success=ce.success_probability(phase="PHASE3", fda_designation="Fast Track")["p_success"],
        upside_multiple=6.0,     # deep OTM tail off a 150 % IV chain
        downside_multiple=0.45,  # trades near net cash
    )
    compounder = ce.catalyst_ev(p_success=0.5, upside_multiple=1.4, downside_multiple=0.9)
    assert crashed is not None and compounder is not None
    assert crashed["expected_value_multiple"] > compounder["expected_value_multiple"]
    assert crashed["reward_to_risk"] > compounder["reward_to_risk"]


# ── board assembly ────────────────────────────────────────────────────────


def _patch_board(
    monkeypatch: pytest.MonkeyPatch,
    *,
    scored: list[dict[str, Any]] | None = None,
    prices: dict[str, float] | None = None,
) -> None:
    """Stub the two cross-module reads the board makes.

    ``_load_spot_prices`` goes through ``long_plays._load_adj_close``, so
    patching that keeps this module's own loader in the path.
    """
    monkeypatch.setattr(
        "intelligence.trial_outcomes.load_scored_outcomes",
        lambda engine, as_of=None: list(scored or []),
    )
    default = {t: 6.0 for t in ("GEMX", "NOOPT", "NOBS", "LOWP", "HIGHP", "MIDP")}
    series = {t: [(AS_OF, px)] for t, px in (prices if prices is not None else default).items()}
    monkeypatch.setattr(
        "intelligence.long_plays._load_adj_close",
        lambda engine, tickers, years, as_of: series,
    )


def _board_engine(
    *,
    catalysts: list[Any],
    profiles: list[Any],
    options: list[Any],
) -> MagicMock:
    """Engine routing each loader's SQL to its fixture rows."""
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    def execute(stmt: Any, params: Any = None) -> MagicMock:
        sql = str(stmt)
        result = MagicMock()
        if "FROM catalyst_calendar" in sql:
            result.fetchall.return_value = catalysts
        elif "FROM company_profiles" in sql:
            result.fetchall.return_value = profiles
        elif "FROM options_daily_signals" in sql:
            result.fetchall.return_value = options
        else:
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn
    engine.begin.return_value = conn
    return engine


def _catalyst_row(
    ticker: str = "GEMX",
    *,
    days_out: int = 120,
    phase: str | None = "PHASE3",
    indication: str | None = "oncology",
    designation: str | None = "Fast Track",
    runway: float | None = 18.0,
    registry_phases: Any = None,
    registry_conditions: Any = None,
) -> tuple:
    """One ``_CATALYST_NAMES_SQL`` row: trial_signals evidence, then the
    ct.gov registry fallback columns read off ``trial_cache.raw_json``."""
    return (
        ticker, "READOUT", AS_OF + timedelta(days=days_out), phase, indication,
        designation, 0.8, 100.0, 0.66, "BUY", 700.0, runway,
        registry_phases, registry_conditions,
    )


def _profile_row(
    ticker: str = "GEMX",
    *,
    cash: float | None = 3.0e8,
    debt: float | None = 2.0e7,
    shares: float | None = 5.0e7,
    burn_q: float | None = 3.0e7,
    runway: float | None = 18.0,
) -> tuple:
    return (
        ticker,
        {
            "cash": cash, "total_debt": debt, "shares_outstanding": shares,
            "quarterly_burn": burn_q, "cash_runway_months": runway,
            "market_cap": 7.0e8, "sector": "Healthcare", "industry": "Biotechnology",
        },
    )


def _option_row(ticker: str = "GEMX", *, spot: float = 6.0, iv: float = 1.1) -> tuple:
    return (ticker, spot, iv, AS_OF - timedelta(days=1))


def test_a_fully_anchored_catalyst_gets_ranked_with_its_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[_catalyst_row()], profiles=[_profile_row()], options=[_option_row()],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)

    assert board["catalysts_considered"] == 1
    assert board["ranked_total"] == 1 and not board["unranked"]
    row = board["ranked"][0]

    assert row["ticker"] == "GEMX"
    assert row["expected_value_multiple"] is not None
    # the evidence that produced P(success) travels with the row
    assert row["p_success_phase"] == "PHASE3"
    assert set(row["p_success_factors"]) == {"indication", "endpoint_clarity", "fda_designation", "enrollment"}
    assert row["p_success_basis"] == "literature_prior"
    # both anchors are real
    assert row["upside_basis"] == "options_iv_lognormal_tail"
    assert row["net_cash_per_share"] == pytest.approx((3.0e8 - 2.0e7) / 5.0e7)
    assert row["total_debt_known"] is True
    assert row["runway_covers_catalyst"] is True
    assert row["blocking_reasons"] == []
    json.dumps(board)


def test_the_runway_gate_is_hard_even_when_the_arithmetic_looks_good(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cash that does not reach the readout means the holders get diluted."""
    _patch_board(monkeypatch)
    engine = _board_engine(
        # readout 12 months out, 4 months of cash
        catalysts=[_catalyst_row(days_out=365, runway=4.0)],
        profiles=[_profile_row(runway=4.0)],
        options=[_option_row()],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)
    assert board["ranked_total"] == 0
    row = board["unranked"][0]
    assert row["runway_covers_catalyst"] is False
    assert "runway_short" in row["blocking_reasons"]
    assert board["unranked_reasons"]["runway_short"] == 1
    # the EV is withheld rather than shown alongside a failed hard gate
    assert row["expected_value_multiple"] is None


def test_a_name_with_no_chain_is_unranked_not_bad(monkeypatch: pytest.MonkeyPatch) -> None:
    """"Cannot price this yet" and "bad bet" must not look the same."""
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[_catalyst_row("NOOPT")], profiles=[_profile_row("NOOPT")], options=[],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)
    assert board["ranked_total"] == 0
    assert board["unranked"][0]["blocking_reasons"] == ["no_option_chain"]
    assert board["unranked_reasons"] == {"no_option_chain": 1}
    # and the board says what to do about it rather than staying silent
    assert any("Run the options puller" in n for n in board["method_notes"])


def test_an_unmeasured_balance_sheet_blocks_rather_than_assuming_no_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[_catalyst_row("NOBS")],
        profiles=[_profile_row("NOBS", cash=None, shares=None)],
        options=[_option_row("NOBS")],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)
    assert board["ranked_total"] == 0
    assert "balance_sheet_unknown" in board["unranked"][0]["blocking_reasons"]


def test_unknown_debt_is_penalised_not_assumed_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ignoring debt overstates a cash floor, which understates the risk."""
    _patch_board(monkeypatch)
    known = ce.build_catalyst_board(
        _board_engine(catalysts=[_catalyst_row()], profiles=[_profile_row(debt=0.0)], options=[_option_row()]),
        as_of=AS_OF,
    )["ranked"][0]
    unknown = ce.build_catalyst_board(
        _board_engine(catalysts=[_catalyst_row()], profiles=[_profile_row(debt=None)], options=[_option_row()]),
        as_of=AS_OF,
    )["ranked"][0]
    assert known["total_debt_known"] is True and unknown["total_debt_known"] is False
    # same cash, but the unmeasured balance sheet gets a smaller credited floor
    assert unknown["downside_multiple"] < known["downside_multiple"]


def test_ranking_is_by_expected_value_descending(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[
            _catalyst_row("LOWP", phase="PHASE1", designation=None),
            _catalyst_row("HIGHP", phase="FILED", indication="hematology"),
            _catalyst_row("MIDP", phase="PHASE3"),
        ],
        profiles=[_profile_row("LOWP"), _profile_row("HIGHP"), _profile_row("MIDP")],
        options=[_option_row("LOWP"), _option_row("HIGHP"), _option_row("MIDP")],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)
    evs = [r["expected_value_multiple"] for r in board["ranked"]]
    assert evs == sorted(evs, reverse=True)
    # a filed-and-haematology name outranks a phase 1 oncology one on the same chart
    assert board["ranked"][0]["ticker"] == "HIGHP"


def test_the_option_tenor_outlives_the_catalyst(monkeypatch: pytest.MonkeyPatch) -> None:
    """An expiry before the readout prices the wrong event."""
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[_catalyst_row(days_out=200)], profiles=[_profile_row()], options=[_option_row()],
    )
    row = ce.build_catalyst_board(engine, as_of=AS_OF)["ranked"][0]
    assert row["days_to_expiry"] == 200 + ce.OPTION_TENOR_BUFFER_DAYS


def test_the_board_reports_which_regime_produced_p_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Literature prior vs GRID's own tape must never be ambiguous."""
    _patch_board(monkeypatch)
    thin = ce.build_catalyst_board(
        _board_engine(catalysts=[_catalyst_row()], profiles=[_profile_row()], options=[_option_row()]),
        as_of=AS_OF,
    )
    assert thin["p_success_basis"] == "literature_prior"
    assert any("literature priors" in n and "floor is 30" in n for n in thin["method_notes"])

    _patch_board(
        monkeypatch, scored=[{"trial_phase": "PHASE3", "fwd_return_30d": 0.5}] * 40,
    )
    fitted = ce.build_catalyst_board(
        _board_engine(catalysts=[_catalyst_row()], profiles=[_profile_row()], options=[_option_row()]),
        as_of=AS_OF,
    )
    assert fitted["p_success_basis"] == "grid_realized_outcomes"
    assert fitted["ranked"][0]["p_success_base_rate"] == pytest.approx(1.0)
    assert any("fitted from 40 scored GRID readouts" in n for n in fitted["method_notes"])


def test_every_loader_is_pit_bounded_and_parameterised(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[_catalyst_row()], profiles=[_profile_row()], options=[_option_row()],
    )
    ce.build_catalyst_board(engine, as_of=AS_OF)
    conn = engine.connect.return_value
    seen = {str(c.args[0]): (c.args[1] if len(c.args) > 1 else {}) for c in conn.execute.call_args_list}

    catalyst_sql = next(s for s in seen if "FROM catalyst_calendar" in s)
    assert "expected_date >= :as_of" in catalyst_sql and "expected_date <= :max_date" in catalyst_sql
    assert "ts.created_at <= :as_of_ts" in catalyst_sql
    # the registry fallback is a PIT read too — a study cached after the
    # decision date is not evidence that was available at the decision date
    assert "tc.parsed_at <= :as_of_ts" in catalyst_sql
    assert seen[catalyst_sql]["as_of"] == AS_OF

    profile_sql = next(s for s in seen if "FROM company_profiles" in s)
    assert "last_analyzed <= :as_of_ts" in profile_sql
    assert seen[profile_sql]["as_of_ts"].date() == AS_OF

    options_sql = next(s for s in seen if "FROM options_daily_signals" in s)
    # bounded on both sides — this reads a dated snapshot table
    assert "signal_date >= :start" in options_sql and "signal_date <= :as_of" in options_sql
    assert seen[options_sql]["as_of"] == AS_OF

    src = Path(ce.__file__).read_text(encoding="utf-8")
    assert 'f"""' not in src and "f'''" not in src and ".format(" not in src


# ── the no-evidence gate ──────────────────────────────────────────────────
#
# Measured on 2026-09-11 the board ranked MANE, ORKA and TARS — none of which
# had a trial phase on file — above IDYA, whose Phase 2 melanoma evidence
# correctly lowered its prior to 0.247. The three unmeasured names were
# competing on DEFAULT_PHASE_BASE_RATE (0.29), a number invented for them.
# That rewards the absence of evidence, and these tests pin it shut.


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, None),
        ([], None),
        (["PHASE2"], "PHASE2"),
        (["PHASE2", "PHASE3"], "PHASE2/PHASE3"),
        ('["PHASE1", "PHASE2"]', "PHASE1/PHASE2"),
        ("PHASE3", "PHASE3"),
        (["NA"], "NA"),
        ([None, ""], None),
    ],
)
def test_registry_phases_flatten_to_a_normalisable_string(raw: Any, expected: str | None) -> None:
    assert ce._registry_phase(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, None),
        ([], None),
        (["Melanoma", "Solid Tumor"], "Melanoma"),
        ('["Sickle Cell Disease"]', "Sickle Cell Disease"),
        ("Alzheimer Disease", "Alzheimer Disease"),
        ([" ", "Asthma"], "Asthma"),
    ],
)
def test_registry_conditions_yield_the_primary_indication(raw: Any, expected: str | None) -> None:
    assert ce._registry_indication(raw) == expected


def test_a_name_with_no_trial_phase_is_unranked_not_scored_on_the_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[_catalyst_row(phase=None, indication=None)],
        profiles=[_profile_row()],
        options=[_option_row()],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)

    assert board["ranked_total"] == 0
    row = board["unranked"][0]
    assert "no_trial_evidence" in row["blocking_reasons"]
    assert board["unranked_reasons"]["no_trial_evidence"] == 1
    # the EV is withheld, not shown next to a fabricated prior
    assert row["expected_value_multiple"] is None
    assert row["trial_phase_source"] is None


def test_an_unphased_name_cannot_outrank_a_name_with_harder_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regression itself: the unmeasured must not beat the measured."""
    monkeypatch.setattr(
        "intelligence.trial_outcomes.load_scored_outcomes", lambda engine, as_of=None: []
    )
    monkeypatch.setattr(
        "intelligence.long_plays._load_adj_close",
        lambda engine, tickers, years, as_of: {t: [(AS_OF, 6.0)] for t in ("BLANK", "KNOWN")},
    )
    engine = _board_engine(
        catalysts=[
            # no phase at all — previously scored on the 0.29 default
            _catalyst_row("BLANK", phase=None, indication=None, designation=None),
            # phase 2 oncology: real evidence, and the multiplier marks it hard
            _catalyst_row("KNOWN", phase="PHASE2", indication="oncology", designation=None),
        ],
        profiles=[_profile_row("BLANK"), _profile_row("KNOWN")],
        options=[_option_row("BLANK"), _option_row("KNOWN")],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)

    assert [r["ticker"] for r in board["ranked"]] == ["KNOWN"]
    assert [r["ticker"] for r in board["unranked"]] == ["BLANK"]


def test_the_registry_supplies_the_phase_when_grid_has_not_scored_the_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The phase was already ingested into trial_cache — use it, don't block."""
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[
            _catalyst_row(
                phase=None,
                indication=None,
                registry_phases=["PHASE2", "PHASE3"],
                registry_conditions=["Melanoma"],
            )
        ],
        profiles=[_profile_row()],
        options=[_option_row()],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)

    assert board["ranked_total"] == 1
    row = board["ranked"][0]
    assert "no_trial_evidence" not in row["blocking_reasons"]
    # a 2/3 trial still has to clear phase 2
    assert row["p_success_phase"] == "PHASE2"
    assert row["trial_phase_source"] == ce.PHASE_SOURCE_REGISTRY
    assert row["primary_indication"] == "Melanoma"
    assert row["primary_indication_source"] == ce.PHASE_SOURCE_REGISTRY
    # and the registry indication actually reaches the prior
    assert "indication" in row["p_success_factors"]


def test_grid_s_own_scored_evidence_wins_over_the_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[
            _catalyst_row(
                phase="PHASE3", indication="hematology",
                registry_phases=["PHASE1"], registry_conditions=["Melanoma"],
            )
        ],
        profiles=[_profile_row()],
        options=[_option_row()],
    )
    row = ce.build_catalyst_board(engine, as_of=AS_OF)["ranked"][0]
    assert row["p_success_phase"] == "PHASE3"
    assert row["trial_phase_source"] == ce.PHASE_SOURCE_SIGNAL
    assert row["primary_indication"] == "hematology"
    assert row["primary_indication_source"] == ce.PHASE_SOURCE_SIGNAL


def test_a_non_applicable_registry_phase_is_not_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ct.gov writes NA for trials with no phase — that is absence, not data."""
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[_catalyst_row(phase=None, indication=None, registry_phases=["NA"])],
        profiles=[_profile_row()],
        options=[_option_row()],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)
    assert board["ranked_total"] == 0
    assert "no_trial_evidence" in board["unranked"][0]["blocking_reasons"]


def test_the_board_reports_where_every_phase_came_from(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_board(monkeypatch)
    engine = _board_engine(
        catalysts=[
            _catalyst_row("GEMX"),
            _catalyst_row("MIDP", phase=None, registry_phases=["PHASE3"]),
            _catalyst_row("LOWP", phase=None),
        ],
        profiles=[_profile_row("GEMX"), _profile_row("MIDP"), _profile_row("LOWP")],
        options=[_option_row("GEMX"), _option_row("MIDP"), _option_row("LOWP")],
    )
    board = ce.build_catalyst_board(engine, as_of=AS_OF)
    assert any(
        "1 from GRID's own trial_signals" in n and "1 from the ct.gov registry" in n
        and "1 with none" in n
        for n in board["method_notes"]
    )


def test_a_failing_source_degrades_the_board_without_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_board(monkeypatch)
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("relation does not exist")
    board = ce.build_catalyst_board(broken, as_of=AS_OF)
    assert board["ranked"] == [] and board["catalysts_considered"] == 0
    notes = "\n".join(board["method_notes"])
    assert "catalyst_calendar: unavailable" in notes
    assert "company_profiles: unavailable" in notes
