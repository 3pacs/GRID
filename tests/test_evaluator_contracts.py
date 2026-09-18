"""Behavioral contract tests for the walk-forward evaluators.

W3 (evaluator correctness) review claims, verified against source and
reproduced here with pure-Python deterministic fixtures (no database,
no network):

    1. ``validation/backtest.py``'s ``WalkForwardBacktest._compute_era_metrics``
       accepted a ``predict_fn`` but computed returns from
       ``matrix.iloc[:, 0]`` unconditionally, so changing the predictor
       never changed the result, and the "target" column was picked by
       position rather than identity (permuting the feature matrix's
       column order silently changed which series was traded).
    2. ``backtest/engine.py``'s ``PitchBacktester.run_historical_regime``
       fetched its whole training window with a single
       ``as_of_date=end_date`` and then filled gaps with
       ``ffill().bfill()`` — both are lookahead: a later revision/release
       (only relevant under ``LATEST_AS_OF``) or a future observation can
       leak into an earlier decision point.

Every test below is written to FAIL against the pre-fix code. See the
session report for the exact `pytest -q` failure output captured by
running this file against a `git stash`ed copy of the source changes.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from backtest.engine import PitchBacktester, _pit_safe_fill
from tests.fixtures.evaluator.fake_pit_store import FakePITStore, Row
from validation.backtest import WalkForwardBacktest


def _bt() -> WalkForwardBacktest:
    """A WalkForwardBacktest with no real DB/PIT store wired up.

    ``_store_result`` swallows the AttributeError from calling
    ``.begin()`` on ``None`` (see validation/backtest.py:338-365), so this
    is safe to construct without touching a database, and none of the
    tests below call ``run_validation`` (which would need a real
    ``pit_store``) — they exercise the pure computation methods directly.
    """
    return WalkForwardBacktest(db_engine=None, pit_store=None)


# ---------------------------------------------------------------------------
# (a) changing predict_fn must change the result
# ---------------------------------------------------------------------------


def test_predict_fn_actually_drives_the_return():
    bt = _bt()
    dates = pd.DatetimeIndex([date(2024, 1, 1), date(2024, 1, 2)], name="obs_date")
    matrix = pd.DataFrame({1: [100.0, 110.0]}, index=dates)  # +10% single move

    long_signal = lambda m: pd.Series(1.0, index=m.index)
    short_signal = lambda m: pd.Series(-1.0, index=m.index)
    zero_signal = lambda m: pd.Series(0.0, index=m.index)

    long_result = bt._compute_era_metrics(matrix, long_signal, cost_bps=0.0)
    short_result = bt._compute_era_metrics(matrix, short_signal, cost_bps=0.0)
    zero_result = bt._compute_era_metrics(matrix, zero_signal, cost_bps=0.0)

    assert long_result["return"] == pytest.approx(0.10)
    # Opposite predictor must flip the sign exactly (single-period fixture
    # avoids compounding ambiguity).
    assert short_result["return"] == pytest.approx(-long_result["return"])
    # Zero/no-op predictor must yield ~zero return.
    assert zero_result["return"] == pytest.approx(0.0)
    # The three predictors must not collapse to the same number.
    assert len({long_result["return"], short_result["return"], zero_result["return"]}) == 3


def test_predict_fn_none_falls_back_to_fully_invested_baseline():
    bt = _bt()
    dates = pd.DatetimeIndex([date(2024, 1, 1), date(2024, 1, 2)], name="obs_date")
    matrix = pd.DataFrame({1: [100.0, 110.0]}, index=dates)

    baseline_result = bt._compute_era_metrics(matrix, None, cost_bps=0.0)
    long_result = bt._compute_era_metrics(
        matrix, lambda m: pd.Series(1.0, index=m.index), cost_bps=0.0
    )
    assert baseline_result["return"] == pytest.approx(long_result["return"])


# ---------------------------------------------------------------------------
# (b) permuting feature column order must not change the target or result
# ---------------------------------------------------------------------------


def test_column_order_does_not_change_target_or_result():
    bt = _bt()
    dates = pd.DatetimeIndex(
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)], name="obs_date"
    )
    data = {10: [100.0, 105.0, 103.0], 20: [50.0, 49.0, 51.0], 30: [1.0, 1.0, 1.0]}

    ordered = pd.DataFrame(data, index=dates)[[10, 20, 30]]
    permuted = pd.DataFrame(data, index=dates)[[30, 10, 20]]

    result_ordered = bt._compute_era_metrics(ordered, None, cost_bps=10.0)
    result_permuted = bt._compute_era_metrics(permuted, None, cost_bps=10.0)

    assert result_ordered == result_permuted


def test_baseline_metrics_column_order_invariant():
    bt = _bt()
    dates = pd.DatetimeIndex(
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)], name="obs_date"
    )
    data = {10: [100.0, 105.0, 103.0], 20: [50.0, 49.0, 51.0]}
    ordered = pd.DataFrame(data, index=dates)[[10, 20]]
    permuted = pd.DataFrame(data, index=dates)[[20, 10]]

    assert bt._compute_baseline_metrics(ordered) == bt._compute_baseline_metrics(permuted)


# ---------------------------------------------------------------------------
# (c) appending a future row/revision must not change an earlier result
# ---------------------------------------------------------------------------


def test_future_revision_does_not_leak_into_earlier_day_validation_backtest():
    bt = _bt()
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)

    store = FakePITStore(
        rows=[
            Row(feature_id=1, obs_date=d1, value=10.0, release_date=d1, vintage_date=d1),
            Row(feature_id=1, obs_date=d2, value=20.0, release_date=d2, vintage_date=d2),
            Row(feature_id=1, obs_date=d3, value=30.0, release_date=d3, vintage_date=d3),
        ]
    )
    bt.pit_store = store

    before = bt._fetch_pit_correct_matrix(
        feature_ids=[1], start_date=d1, end_date=d3, vintage_policy="LATEST_AS_OF"
    )
    assert before.loc[pd.Timestamp(d2), 1] == 20.0  # sanity: original value for d2

    # A revision for d2 lands (release/vintage date d3, i.e. "known later" but
    # still inside the window) -- this is exactly the kind of future
    # correction LATEST_AS_OF is meant to pick up, but ONLY from d3 onward.
    store.rows.append(
        Row(feature_id=1, obs_date=d2, value=999.0, release_date=d3, vintage_date=d3)
    )

    after = bt._fetch_pit_correct_matrix(
        feature_ids=[1], start_date=d1, end_date=d3, vintage_policy="LATEST_AS_OF"
    )
    assert after.loc[pd.Timestamp(d2), 1] == 20.0, (
        "the d2 row must not see a revision released on d3 -- that is "
        "lookahead relative to d2's own decision point"
    )

    # Reproduce the *old* defect directly against the same fixture: a single
    # get_feature_matrix call with as_of_date pinned to the window's end
    # (what the pre-fix call site did) DOES leak the revision, proving the
    # bug was real and this fixture actually exercises it.
    old_style = store.get_feature_matrix(
        feature_ids=[1], start_date=d1, end_date=d3, as_of_date=d3, vintage_policy="LATEST_AS_OF"
    )
    assert old_style.loc[pd.Timestamp(d2), 1] == 999.0


def test_future_revision_does_not_leak_into_earlier_day_engine():
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    store = FakePITStore(
        rows=[
            Row(feature_id=1, obs_date=d1, value=10.0, release_date=d1, vintage_date=d1),
            Row(feature_id=1, obs_date=d2, value=20.0, release_date=d2, vintage_date=d2),
            Row(feature_id=1, obs_date=d3, value=30.0, release_date=d3, vintage_date=d3),
        ]
    )
    bt = PitchBacktester(db_engine=None, pit_store=store)

    before = bt._fetch_pit_correct_matrix(
        feature_ids=[1], start_date=d1, end_date=d3, vintage_policy="LATEST_AS_OF"
    )
    assert before.loc[pd.Timestamp(d2), 1] == 20.0

    store.rows.append(
        Row(feature_id=1, obs_date=d2, value=999.0, release_date=d3, vintage_date=d3)
    )
    after = bt._fetch_pit_correct_matrix(
        feature_ids=[1], start_date=d1, end_date=d3, vintage_policy="LATEST_AS_OF"
    )
    assert after.loc[pd.Timestamp(d2), 1] == 20.0


# ---------------------------------------------------------------------------
# (d) a gap must not be filled from the future
# ---------------------------------------------------------------------------


def test_pit_safe_fill_does_not_backfill_a_leading_gap():
    dates = pd.DatetimeIndex(
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        name="obs_date",
    )
    # No observation exists yet for the first two rows -- there is nothing
    # to forward-fill from. The old ffill().bfill() would smear the first
    # *future* known value (5.0) backwards onto them.
    matrix = pd.DataFrame({1: [np.nan, np.nan, 5.0, 6.0]}, index=dates)

    filled = _pit_safe_fill(matrix)

    assert len(filled) == 2, "rows before the column's first observation must be dropped, not guessed"
    assert list(filled[1]) == [5.0, 6.0]
    assert filled.index[0] == pd.Timestamp(date(2024, 1, 3)), (
        "the leading gap rows (1/1, 1/2) must be dropped, not backfilled with the 1/3 value"
    )


def test_pit_safe_fill_still_forward_fills_an_interior_gap():
    dates = pd.DatetimeIndex(
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)], name="obs_date"
    )
    matrix = pd.DataFrame({1: [1.0, np.nan, 3.0]}, index=dates)

    filled = _pit_safe_fill(matrix)

    assert len(filled) == 3
    assert list(filled[1]) == [1.0, 1.0, 3.0]


# ---------------------------------------------------------------------------
# (e) duplicate records must not inflate the sample count
# ---------------------------------------------------------------------------


def test_duplicate_rows_do_not_inflate_sample_count():
    bt = _bt()
    dates = pd.DatetimeIndex(
        [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        name="obs_date",
    )
    # d1 appears twice with identical data -- simulates a duplicated
    # (feature_id, obs_date) record surviving into the matrix.
    matrix = pd.DataFrame({1: [100.0, 100.0, 105.0, 103.0]}, index=dates)

    deduped = pd.DataFrame(
        {1: [100.0, 105.0, 103.0]},
        index=pd.DatetimeIndex(
            [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)], name="obs_date"
        ),
    )

    dup_result = bt._compute_era_metrics(matrix, None, cost_bps=0.0)
    clean_result = bt._compute_era_metrics(deduped, None, cost_bps=0.0)

    assert dup_result["n_total"] == clean_result["n_total"] == 3
    assert dup_result == clean_result


def test_fetch_pit_correct_matrix_dedupes_across_day_fetches():
    d1 = date(2024, 1, 1)
    store = FakePITStore(
        rows=[
            Row(feature_id=1, obs_date=d1, value=10.0, release_date=d1, vintage_date=d1),
        ]
    )
    bt = PitchBacktester(db_engine=None, pit_store=store)
    matrix = bt._fetch_pit_correct_matrix(feature_ids=[1], start_date=d1, end_date=d1)
    assert len(matrix) == 1


# ---------------------------------------------------------------------------
# (f) no-move and missing-data rows are explicit and counted, not dropped
#     silently or scored as WRONG
# ---------------------------------------------------------------------------


def test_no_move_and_missing_rows_are_counted_explicitly():
    bt = _bt()
    dates = pd.DatetimeIndex(
        [
            date(2024, 1, 1),
            date(2024, 1, 2),  # no-move day (0.0 return) -- must be scored, not dropped
            date(2024, 1, 3),  # missing observation
            date(2024, 1, 4),  # still missing (prior value was NaN)
            date(2024, 1, 5),
        ],
        name="obs_date",
    )
    matrix = pd.DataFrame({1: [100.0, 100.0, np.nan, 100.0, 105.0]}, index=dates)

    result = bt._compute_era_metrics(matrix, None, cost_bps=0.0)

    assert result["n_total"] == 5
    # Row 0 has no prior value (structural), rows 2 and 3 are missing data.
    assert result["n_missing"] == 3
    # Rows 1 (0.0, no-move) and 4 (0.05) are the only scored observations.
    assert result["n_days"] == 2
    # The no-move day contributed a factor of (1+0)=1, not "wrong"/dropped:
    # compounding [0.0, 0.05] gives exactly 5%.
    assert result["return"] == pytest.approx(0.05)
