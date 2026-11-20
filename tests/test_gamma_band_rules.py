"""The detector consumes only the completed bar and an already-observed frame."""

import json
import pytest

from api.gamma_band_rules import step

LEVELS = {99: 1, 100: -3, 101: -1, 102: 2}


def b(close, *, high=None, low=None):
    return dict(
        open=close,
        high=close if high is None else high,
        low=close if low is None else low,
        close=close,
    )


def run(seq, levels=LEVELS):
    state = None
    flags = []
    for minute, bar in enumerate(seq, 10):
        state, ev = step(
            json.loads(json.dumps(state)), minute=minute, bar=bar, levels=levels
        )
        flags.extend((minute, e) for e in ev)
    return state, flags


def test_first_and_second_setups_with_retries_and_frozen_trio():
    # First call hits upper later. The down call can only be emitted after three
    # strictly ordered closes below / above / below. Repeating it is disallowed.
    bars = [
        b(100),
        b(100.3),
        b(101.8, high=102.1),
        b(99.8),
        b(100),
        b(100.2),
        b(99.9),
        b(100.1),
        b(99.6),
    ]
    state, flags = run(bars)
    assert flags == [
        (
            11,
            dict(
                kind="first_cross",
                red=100,
                lower=99,
                upper=102,
                target=102,
                close=100.3,
            ),
        ),
        (
            16,
            dict(
                kind="failed_reclaim",
                red=100,
                lower=99,
                upper=102,
                target=99,
                close=99.9,
            ),
        ),
    ]
    saved = json.loads(json.dumps(state))
    assert step(saved, minute=18, bar=b(100.3), levels=LEVELS) == (saved, [])
    assert step(saved, minute=10, bar=b(100.3), levels=LEVELS) == (saved, [])
    assert saved == state


@pytest.mark.parametrize(
    "lv",
    [
        {99: -1, 100: -1, 101: 1},
        {98: 1, 100: -1, 101: 1},
        {99: 1, 100: -1, 102: 1},
        {99: 1, 100: -1, 101: 0, 102: 1},
        {99: 1, 100: 0, 101: 1},
        {99: 1, 100: -1, 101: float("nan"), 102: 1},
    ],
)
def test_no_implicit_strikes_or_zero_bridges(lv):
    assert run([b(99.7), b(100.1)], lv)[1] == []


def test_gap_clears_setup_without_rearming_seen_level():
    state, flags = run([b(99.6), b(100.1), b(99.8)])
    assert len(flags) == 1
    for minute, price, lv in [
        (14, 100.4, LEVELS),
        (15, 99.4, LEVELS),
        (16, 100.4, None),
        (17, 99.8, LEVELS),
        (18, 100.2, LEVELS),
    ]:
        state, flags = step(state, minute=minute, bar=b(price), levels=lv)
        assert not flags
    assert state["seen"] == [100]


def test_target_reached_before_decision_consumes_setup():
    assert not run([b(99.8), b(102), b(99.8), b(101)])[1]
    _, flags = run([b(99.8), b(100.3), b(99.6), b(100.2), b(98.9)])
    assert [e["kind"] for _, e in flags] == ["first_cross"]


def test_lower_touched_while_waiting_for_reclaim_ends_setup():
    _, flags = run([b(99.8), b(100.2), b(99.7), b(100.2, low=98.95), b(99.8)])
    assert len(flags) == 1


@pytest.mark.parametrize(
    "sequence",
    [
        [b(100.5, high=102.1), b(99.7)],
        [b(100.5), b(99.7, high=102.1)],
    ],
)
def test_successful_or_ambiguous_reclaim_requires_new_attempt(sequence):
    _, flags = run([b(99.8), b(100.1), b(99.7), *sequence, b(100.5), b(99.6)])
    assert [(m, e["kind"]) for m, e in flags] == [
        (11, "first_cross"),
        (16, "failed_reclaim"),
    ]


def test_frozen_signs_rechecked_at_failure_no_retargeting():
    state, _ = run([b(99.8), b(100.1), b(99.7), b(100.3)])
    changed = {**LEVELS, 101: 2}
    state, flags = step(state, minute=14, bar=b(99.8), levels=changed)
    assert not flags and not state["active"]


def test_invalid_bar_breaks_continuity_and_does_not_mutate_prior_state():
    state, _ = run([b(99.8), b(100.1), b(99.7)])
    before = json.loads(json.dumps(state))
    s, events = step(state, minute=13, bar=b(100.5, high=99), levels=LEVELS)
    assert not events and s["close"] is None and not s["active"] and state == before
