"""Freeze tests for the D1 ranking implementation (contract §5.1; analysis/d1_ranking.py).

Synthetic inputs only. Each pin below fails if the ranking changes; a change is a new ranking
version with a new pin, reviewed, never an edit of an expected value to match new behaviour.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import statistics

import pytest

from analysis import d1_ranking as R

# Pins. A deliberate ranking change updates these in the same reviewed commit as its new version name.
SPEC_SHA256 = "5d9a3b2a7d4f32dd32c3e5561150bedb5f566b832e5b7615b9624eed08c1fe41"
PANEL_SHA256 = "dc2b9a5ff8ea0db13f2ba27c2d8f9756ad111a3ea3fc9ea338c3c07c92d193e9"


def _closes(last: float = 100.0, vol: float = 0.01, seed: int = 1) -> list[float]:
    rng = random.Random(seed)
    out = [last]
    for _ in range(R.VOL_RETURNS):
        out.append(out[-1] * (1.0 + (rng.random() - 0.5) * 2 * vol))
    return out


# ---------------------------------------------------------------- the pins
def test_the_ranking_spec_is_pinned():
    assert R.RANKING_VERSION == "d1-ranking-v1"
    assert R.spec_digest() == SPEC_SHA256


def test_the_contract_numbers_are_in_the_spec():
    assert R.BREAKPOINT_PROBS == (0.2, 0.4, 0.6, 0.8)  # four breakpoints, five quintiles (contract §5.1)
    assert R.VOL_RETURNS == 60  # "60-session close-to-close volatility"
    assert R.F1_ITEMS == {"2.02"} and R.F2_ITEMS == {"1.01", "2.01", "1.02"}


def _panel_digest() -> str:
    rng = random.Random(20261125)
    events = []
    for i in range(500):
        closes = [100.0]
        for _ in range(R.VOL_RETURNS):
            closes.append(closes[-1] * (1.0 + (rng.random() - 0.5) * 0.04))
        c0 = closes[-1] * (1.0 + (rng.random() - 0.5) * 0.12)
        events.append({"accession": f"0000000000-26-{i:06d}", "tier": int(rng.random() * 2),
                       "liquidity": 2e7 + rng.random() * 1e8, "closes": closes, "c0": c0})
    strengths = [R.event_strength(e["closes"], e["c0"]).value for e in events]
    bps = R.compute_breakpoints(strengths[:300])
    assert bps.status == R.OK
    rows = [{"accession": e["accession"], "tier": e["tier"], "strength": s, "liquidity": e["liquidity"]}
            for e, s in zip(events[300:], strengths[300:])]
    ranked, excluded = R.rank_events(rows, bps)
    assert excluded == []
    out = {"breakpoints": list(bps.values), "ranked": [[r.accession, r.tier, r.quintile] for r in ranked]}
    return hashlib.sha256(json.dumps(out, separators=(",", ":")).encode()).hexdigest()


def test_a_seeded_panel_ranks_exactly_as_pinned():
    assert _panel_digest() == PANEL_SHA256


# ---------------------------------------------------------------- strength and volatility
def test_volatility_is_the_sample_std_of_60_simple_returns():
    closes = _closes(seed=7)
    rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, 61)]
    assert R.daily_volatility(closes) == pytest.approx(statistics.stdev(rets), rel=1e-12)
    assert len(rets) == 60 and len(closes) == 61


def test_strength_is_the_absolute_day0_move_over_volatility():
    closes = _closes(seed=3)
    vol = statistics.stdev([closes[i] / closes[i - 1] - 1.0 for i in range(1, 61)])
    s = R.event_strength(closes, closes[-1] * 1.05)
    assert s.excluded is None and s.value == pytest.approx(0.05 / vol, rel=1e-9)
    down = R.event_strength(closes, closes[-1] * 0.95)
    assert down.value == pytest.approx(0.05 / vol, rel=1e-9)  # absolute move: direction is not strength


@pytest.mark.parametrize("closes", [[100.0] * 61], ids=["flat prices: zero volatility"])
def test_zero_volatility_excludes_the_event_with_no_substitute(closes):
    s = R.event_strength(closes, 105.0)
    assert s.value is None and s.excluded == R.EXCLUDED_INPUT and "zero" in s.detail


@pytest.mark.parametrize("name,closes,c0", [
    ("too few closes", _closes()[:-1], 101.0),
    ("too many closes", [100.0] + _closes(), 101.0),
    ("a None close", [None] + _closes()[1:], 101.0),
    ("a NaN close", _closes()[:30] + [float("nan")] + _closes()[31:], 101.0),
    ("a zero close", _closes()[:30] + [0.0] + _closes()[31:], 101.0),
    ("a negative close", _closes()[:30] + [-1.0] + _closes()[31:], 101.0),
    ("a boolean close", _closes()[:30] + [True] + _closes()[31:], 101.0),
    ("missing C0", _closes(), None),
    ("zero C0", _closes(), 0.0),
    ("infinite C0", _closes(), float("inf")),
    ("no closes", [], 101.0),
])
def test_missing_or_invalid_inputs_exclude_the_event(name, closes, c0):
    s = R.event_strength(closes, c0)
    assert s.value is None and s.excluded == R.EXCLUDED_INPUT, name


# ---------------------------------------------------------------- families
@pytest.mark.parametrize("items,family", [
    (["2.02"], R.F1), (["2.02", "1.01"], R.F1), (["1.01", "2.02", "2.01", "1.02"], R.F1),
    (["1.01"], R.F2), (["2.01"], R.F2), (["1.02"], R.F2), (["1.01", "2.01"], R.F2), ([" 1.02 "], R.F2),
    (["5.02"], None), ([], None), (["2.2"], None),
])
def test_one_event_per_accession_and_item_2_02_means_f1_only(items, family):
    assert R.family_for_items(items) == family


# ---------------------------------------------------------------- the estimator
def test_type7_matches_known_values():
    s = [float(i) for i in range(1, 11)]
    assert [R.quantile_type7(s, p) for p in R.BREAKPOINT_PROBS] == pytest.approx([2.8, 4.6, 6.4, 8.2])
    assert R.quantile_type7([5.0], 0.5) == 5.0
    assert R.quantile_type7([1.0, 3.0], 0.5) == 2.0


def test_type7_matches_numpy_linear_when_numpy_is_present():
    np = pytest.importorskip("numpy")
    rng = random.Random(11)
    for n in (2, 3, 7, 50, 301):
        vals = sorted(rng.random() * 10 for _ in range(n))
        for p in R.BREAKPOINT_PROBS:
            assert R.quantile_type7(vals, p) == pytest.approx(float(np.quantile(vals, p, method="linear")), rel=1e-12)


def test_breakpoints_are_strictly_increasing_when_they_exist():
    bp = R.compute_breakpoints([float(i) for i in range(1, 11)])
    assert bp.status == R.OK and bp.n == 10
    assert list(bp.values) == pytest.approx([2.8, 4.6, 6.4, 8.2])
    assert all(a < b for a, b in zip(bp.values, bp.values[1:]))


def test_input_order_does_not_change_the_breakpoints():
    vals = [float(i) for i in range(1, 40)]
    shuffled = vals[:]
    random.Random(5).shuffle(shuffled)
    assert R.compute_breakpoints(vals) == R.compute_breakpoints(shuffled)


# ---------------------------------------------------------------- HOLD, never an invented cutoff
def test_tied_development_strengths_put_the_family_on_hold():
    bp = R.compute_breakpoints([1.0] * 9 + [2.0])
    assert bp.status == R.HOLD and bp.values is None and bp.cause == R.CAUSE_DUPLICATE_BREAKPOINTS


def test_two_equal_breakpoints_are_a_hold_even_if_others_differ():
    # sorted [1,1,1,1,1,5,6,7,8,9]: b1 = b2 = 1.0, so strict ordering fails
    bp = R.compute_breakpoints([1, 1, 1, 1, 1, 5, 6, 7, 8, 9])
    assert bp.status == R.HOLD and bp.cause == R.CAUSE_DUPLICATE_BREAKPOINTS


@pytest.mark.parametrize("vals,cause", [
    ([], R.CAUSE_TOO_FEW), ([3.0], R.CAUSE_TOO_FEW),
    ([1.0, 2.0, float("nan")], R.CAUSE_BAD_STRENGTH), ([1.0, 2.0, -0.1], R.CAUSE_BAD_STRENGTH),
    ([1.0, 2.0, None], R.CAUSE_BAD_STRENGTH), ([1.0, 2.0, True], R.CAUSE_BAD_STRENGTH),
])
def test_unusable_development_data_is_a_hold_with_its_cause(vals, cause):
    bp = R.compute_breakpoints(vals)
    assert bp.status == R.HOLD and bp.cause == cause and bp.values is None


def test_a_hold_family_ranks_nothing():
    bp = R.compute_breakpoints([1.0] * 10)
    with pytest.raises(ValueError, match="HOLD"):
        R.rank_events([{"accession": "a", "tier": 0, "strength": 1.0, "liquidity": 1.0}], bp)


# ---------------------------------------------------------------- quintiles and ties
BP = (2.0, 4.0, 6.0, 8.0)


@pytest.mark.parametrize("strength,q", [(0.0, 1), (1.999, 1), (2.0, 2), (3.0, 2), (4.0, 3), (5.9, 3),
                                        (6.0, 4), (7.5, 4), (8.0, 5), (1e9, 5)])
def test_a_strength_equal_to_a_breakpoint_goes_to_the_higher_quintile(strength, q):
    assert R.quintile(strength, BP) == q


def test_quintile_refuses_bad_breakpoints_and_strengths():
    for bad in [(1, 1, 2, 3), (3, 2, 1, 0), (1, 2, 3), (1, 2, 3, 4, 5)]:
        with pytest.raises(ValueError):
            R.quintile(1.0, bad)
    for s in (-0.1, float("nan"), None, True):
        with pytest.raises(ValueError):
            R.quintile(s, BP)


def _bp():
    return R.Breakpoints(R.OK, BP, None, 100)


def _ev(acc, strength, liq, tier=0):
    return {"accession": acc, "tier": tier, "strength": strength, "liquidity": liq}


def test_order_is_tier_then_quintile_then_liquidity_then_accession():
    ranked, excluded = R.rank_events([
        _ev("0003", 9.0, 5e7), _ev("0001", 9.0, 5e7), _ev("0002", 9.0, 9e7),   # same bucket 5: liquidity, then accession
        _ev("0004", 5.0, 1e9),                                                  # bucket 3 beats nothing above it
        _ev("0005", 9.0, 1e6, tier=1),                                          # a worse tier ranks below every tier 0
        _ev("0006", 1.0, 1e9),
    ], _bp())
    assert excluded == []
    assert [r.accession for r in ranked] == ["0002", "0001", "0003", "0004", "0006", "0005"]
    assert [r.quintile for r in ranked] == [5, 5, 5, 3, 1, 5]


def test_inside_a_bucket_raw_strength_does_not_break_a_tie():
    ranked, _ = R.rank_events([_ev("A", 8.1, 1e7), _ev("B", 99.0, 2e7)], _bp())  # both quintile 5
    assert [r.accession for r in ranked] == ["B", "A"]  # liquidity decides, not the larger strength
    ranked, _ = R.rank_events([_ev("A", 8.1, 2e7), _ev("B", 99.0, 1e7)], _bp())
    assert [r.accession for r in ranked] == ["A", "B"]


def test_equal_everything_breaks_by_accession_ascending():
    ranked, _ = R.rank_events([_ev("0000000000-26-000020", 5.0, 1e7), _ev("0000000000-26-000003", 5.0, 1e7)], _bp())
    assert [r.accession for r in ranked] == ["0000000000-26-000003", "0000000000-26-000020"]


def test_ranking_does_not_depend_on_input_order():
    evs = [_ev(f"{i:04d}", float(i % 11), 1e7 + (i * 7919) % 1000, tier=i % 2) for i in range(60)]
    base, _ = R.rank_events(evs, _bp())
    shuffled = evs[:]
    random.Random(9).shuffle(shuffled)
    again, _ = R.rank_events(shuffled, _bp())
    assert base == again


def test_events_without_strength_or_liquidity_are_excluded_and_returned_not_ranked():
    ranked, excluded = R.rank_events([_ev("a", None, 1e7), _ev("b", 5.0, None), _ev("c", 5.0, float("nan")),
                                      _ev("d", float("nan"), 1e7), _ev("e", 5.0, 1e7)], _bp())
    assert [r.accession for r in ranked] == ["e"]
    assert sorted(x["accession"] for x in excluded) == ["a", "b", "c", "d"]
    assert all(x["reason"] == R.EXCLUDED_INPUT for x in excluded)


def test_duplicate_accession_and_missing_tier_are_refused():
    with pytest.raises(ValueError, match="more than once"):
        R.rank_events([_ev("a", 1.0, 1.0), _ev("a", 2.0, 1.0)], _bp())
    with pytest.raises(ValueError, match="tier"):
        R.rank_events([{"accession": "a", "strength": 1.0, "liquidity": 1.0}], _bp())
    with pytest.raises(ValueError, match="tier"):
        R.rank_events([{"accession": "a", "tier": True, "strength": 1.0, "liquidity": 1.0}], _bp())
    with pytest.raises(ValueError, match="accession"):
        R.rank_events([{"tier": 0, "strength": 1.0, "liquidity": 1.0}], _bp())


def test_an_event_whose_cutoff_is_later_is_never_needed_to_place_an_earlier_one():
    """The bucket of an event depends on the frozen breakpoints and its own strength only (contract: 'known at its own D')."""
    bp = R.compute_breakpoints([float(i) for i in range(1, 101)])
    a = R.quintile(37.5, bp.values)
    assert all(R.quintile(37.5, bp.values) == a for _ in range(3))
    assert R.quintile(37.5, R.compute_breakpoints([float(i) for i in range(1, 101)]).values) == a


def test_module_reads_no_data_source():
    src = open(R.__file__, encoding="utf-8").read()
    for banned in ("import psycopg", "sqlalchemy", "requests", "urllib", "open(", "datetime", "time.time"):
        assert banned not in src.replace("open(R.__file__", ""), banned
