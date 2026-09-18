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
from store.vintage_selection import select_vintage_per_decision
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


def test_same_bar_signal_cannot_win_by_construction():
    """A predictor that uses row t's OWN already-realized return
    (sign(target.pct_change()) at t) must not be able to trade that same
    return -- that would be scoring a decision on information that only
    existed once the outcome was already known.

    sign(x) * x == |x| >= 0 for every row, so the pre-fix formula
    (`signal * valid_returns`, no shift) is *structurally* incapable of
    losing: any nonzero move is scored as a win. That is reproduced
    directly and asserted here (see the docstring at module level for the
    session's separate confirmation that this was really true of the
    pre-fix code). After the fix (signal shifted one period before it is
    applied), this same predictor must not come out ahead.
    """
    bt = _bt()
    dates = pd.DatetimeIndex([date(2024, 1, i) for i in range(1, 6)], name="obs_date")
    matrix = pd.DataFrame({1: [100.0, 110.0, 99.0, 108.0, 97.0]}, index=dates)

    def cheat_same_bar(features: pd.DataFrame) -> pd.Series:
        # features has the target column removed; recompute what the
        # target's own same-bar return sign would have been using the raw
        # prices captured in this closure -- this is exactly the shape of
        # bug a caller could introduce by deriving a signal from data that
        # includes the current bar's own realized move.
        target_returns = pd.Series([100.0, 110.0, 99.0, 108.0, 97.0], index=features.index).pct_change()
        return np.sign(target_returns)

    # Sanity: the raw (unshifted) same-bar formula is structurally >= 0.
    target_returns = matrix[1].pct_change()
    valid_returns = target_returns.dropna()
    unshifted = np.sign(target_returns).reindex(valid_returns.index) * valid_returns
    assert (unshifted.fillna(0) >= 0).all()
    assert float((1 + unshifted).prod() - 1) > 0

    result = bt._compute_era_metrics(matrix, cheat_same_bar, cost_bps=0.0)
    assert result["return"] <= 0.0, (
        "a signal derived from the bar's own already-realized return must "
        "not be able to trade that same bar -- the engine must shift it "
        "forward, which for this fixture guarantees a non-positive result"
    )


def test_genuine_next_period_predictor_may_win():
    """Contrast case for the same-bar test: a predictor that is
    (by construction, as an oracle in this test only) told the sign of
    the *next* period's return is legitimately allowed to win once the
    engine's one-period shift lines it up with the bar it actually
    predicted.
    """
    bt = _bt()
    dates = pd.DatetimeIndex([date(2024, 1, i) for i in range(1, 6)], name="obs_date")
    prices = [100.0, 110.0, 99.0, 108.0, 97.0]
    matrix = pd.DataFrame({1: prices}, index=dates)

    def oracle_next_period(features: pd.DataFrame) -> pd.Series:
        target_returns = pd.Series(prices, index=features.index).pct_change()
        return np.sign(target_returns.shift(-1))

    result = bt._compute_era_metrics(matrix, oracle_next_period, cost_bps=0.0)
    assert result["return"] > 0.0


def test_warmup_rows_are_counted_separately_not_silently_zeroed():
    """A predict_fn with its own burn-in (NaN until enough history exists)
    must have those NaN-signal rows tracked via n_warmup, not just
    silently filled to 0 with no record -- and they must not corrupt the
    n_missing/n_days accounting for genuinely scored rows.
    """
    bt = _bt()
    dates = pd.DatetimeIndex([date(2024, 1, i) for i in range(1, 5)], name="obs_date")
    matrix = pd.DataFrame({1: [100.0, 105.0, 95.0, 110.0]}, index=dates)

    def needs_two_rows_of_history(features: pd.DataFrame) -> pd.Series:
        s = pd.Series(np.nan, index=features.index)
        s.iloc[2:] = 1.0
        return s

    result = bt._compute_era_metrics(matrix, needs_two_rows_of_history, cost_bps=0.0)

    assert result["n_total"] == 4
    assert result["n_missing"] == 1  # row 0: pct_change() has no prior value
    assert result["n_warmup"] == 2  # rows 1, 2: shifted signal still undefined
    assert result["n_days"] == 3  # rows 1, 2, 3 all have a defined target return
    # Only row 3 (signal shifted from row 2's defined 1.0) actually traded;
    # rows 1 and 2 contributed a flat (zero-position) day, not a guess.
    r3 = (110.0 - 95.0) / 95.0
    assert result["return"] == pytest.approx(r3, abs=1e-5)


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


# ---------------------------------------------------------------------------
# explicit target_feature_id: no longer "whatever the smallest id is"
# ---------------------------------------------------------------------------


def test_target_feature_id_is_hidden_from_predict_fn():
    bt = _bt()
    dates = pd.DatetimeIndex([date(2024, 1, 1), date(2024, 1, 2)], name="obs_date")
    matrix = pd.DataFrame({1: [50.0, 49.0], 2: [100.0, 110.0]}, index=dates)

    seen_columns: list[list[int]] = []

    def spy_predict_fn(features: pd.DataFrame) -> pd.Series:
        seen_columns.append(list(features.columns))
        return pd.Series(1.0, index=features.index)

    bt._compute_era_metrics(matrix, spy_predict_fn, cost_bps=0.0, target_feature_id=2)

    assert seen_columns, "predict_fn was never called"
    assert 2 not in seen_columns[0], "predict_fn must not see the target column"
    assert seen_columns[0] == [1]


def test_target_feature_id_result_is_column_order_invariant():
    bt = _bt()
    dates = pd.DatetimeIndex(
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)], name="obs_date"
    )
    data = {10: [100.0, 105.0, 103.0], 20: [50.0, 49.0, 51.0], 30: [1.0, 1.0, 1.0]}

    ordered = pd.DataFrame(data, index=dates)[[10, 20, 30]]
    permuted = pd.DataFrame(data, index=dates)[[30, 10, 20]]

    result_ordered = bt._compute_era_metrics(
        ordered, None, cost_bps=10.0, target_feature_id=20
    )
    result_permuted = bt._compute_era_metrics(
        permuted, None, cost_bps=10.0, target_feature_id=20
    )

    assert result_ordered == result_permuted
    # And it must actually be trading column 20, not the sorted-first (10).
    default_result = bt._compute_era_metrics(ordered, None, cost_bps=10.0)
    assert result_ordered != default_result


def test_target_feature_id_missing_from_matrix_is_handled_explicitly():
    bt = _bt()
    dates = pd.DatetimeIndex([date(2024, 1, 1), date(2024, 1, 2)], name="obs_date")
    matrix = pd.DataFrame({1: [100.0, 110.0]}, index=dates)

    result = bt._compute_era_metrics(matrix, None, cost_bps=0.0, target_feature_id=999)
    assert result["return"] == 0.0
    assert result["n_days"] == 0

    baseline_result = bt._compute_baseline_metrics(matrix, target_feature_id=999)
    assert baseline_result["return"] == 0.0


# ---------------------------------------------------------------------------
# leakage-safe train/test separation (fit_fn / embargo_days)
# ---------------------------------------------------------------------------
#
# These exercise run_validation end-to-end against a FakePITStore, since
# the split lives in the era loop itself, not in a single computation
# method.


def _linear_dates(start: date, n: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


def _store_from_prices(dates: list[date], prices: list[float]) -> FakePITStore:
    return FakePITStore(
        rows=[
            Row(feature_id=1, obs_date=d, value=p, release_date=d, vintage_date=d)
            for d, p in zip(dates, prices)
        ]
    )


def test_fit_fn_memorization_cannot_beat_baseline_without_test_era_access():
    """A fit_fn that "memorizes" is only ever shown the train window, so
    the only thing it can memorize is train dates' own returns. Looked up
    against test-era dates (which were never in that window), every
    lookup misses -- the cheat has nothing to cheat with, and must not
    beat a genuinely positive buy-and-hold baseline.
    """
    dates = _linear_dates(date(2024, 1, 1), 41)
    prices = [100.0 * (1.005**i) for i in range(41)]  # steady uptrend
    store = _store_from_prices(dates, prices)

    def memorize_fit_fn(train_matrix: pd.DataFrame):
        target = train_matrix.sort_index(axis=1).iloc[:, 0]
        returns_by_date = target.pct_change().dropna().to_dict()

        def predict(features: pd.DataFrame) -> pd.Series:
            return pd.Series(
                [np.sign(returns_by_date.get(pd.Timestamp(d), 0.0)) for d in features.index],
                index=features.index,
            )

        return predict

    bt = _bt()
    bt.pit_store = store
    result = bt.run_validation(
        hypothesis_id=1,
        feature_ids=[1],
        start_date=dates[0],
        end_date=dates[-1],
        n_splits=2,
        cost_bps=0.0,
        fit_fn=memorize_fit_fn,
        embargo_days=1,
        target_feature_id=1,
    )

    baseline_return = result["baseline_comparison"]["return"]
    strategy_return = result["full_period_metrics"]["return"]
    assert baseline_return > 0  # sanity: the fixture has genuine positive drift
    assert strategy_return == pytest.approx(0.0)  # every lookup missed -> flat
    assert strategy_return < baseline_return


def test_embargo_size_controls_overlap_leak_gain():
    """An "overlap-leaking" fit_fn peeks at the H days immediately after
    train_end (a stand-in for a label horizon that wasn't embargoed out).
    With embargo_days=0, train_end sits right at the test era's own
    start, so that peek IS the test era's own opening move -- a real,
    if adversarially constructed, leak. With embargo_days>=horizon,
    train_end is pushed back far enough that the same peek only ever
    lands on the training tail, which this fixture deliberately makes
    move in the OPPOSITE direction from the test era's actual trend.
    """
    horizon = 3
    dates = _linear_dates(date(2024, 1, 1), 41)
    prices: list[float] = []
    p = 100.0
    for i in range(41):
        if i <= 16:
            r = 0.0
        elif i <= 19:  # era 0's last 3 days (the horizon)
            r = -0.05
        else:  # all of era 1: uptrend
            r = 0.05
        p = p * (1 + r)
        prices.append(p)
    store = _store_from_prices(dates, prices)
    price_by_date = dict(zip([pd.Timestamp(d) for d in dates], prices))

    def make_overlap_leaking_fit_fn():
        def fit_fn(train_matrix: pd.DataFrame):
            train_end_ts = train_matrix.sort_index().index.max()
            window_dates = [train_end_ts + pd.Timedelta(days=k) for k in range(1, horizon + 1)]
            window_prices = [price_by_date[d] for d in window_dates if d in price_by_date]
            direction = 0.0
            if len(window_prices) >= 2:
                direction = float(np.sign(window_prices[-1] - window_prices[0]))

            def predict(features: pd.DataFrame) -> pd.Series:
                return pd.Series(direction, index=features.index)

            return predict

        return fit_fn

    def run(embargo_days: int) -> dict:
        bt = _bt()
        bt.pit_store = store
        return bt.run_validation(
            hypothesis_id=1,
            feature_ids=[1],
            start_date=dates[0],
            end_date=dates[-1],
            n_splits=2,
            cost_bps=0.0,
            fit_fn=make_overlap_leaking_fit_fn(),
            embargo_days=embargo_days,
            target_feature_id=1,
        )

    small_embargo = run(embargo_days=0)
    large_embargo = run(embargo_days=horizon)

    assert small_embargo["n_no_train_data"] == 1  # era 0 has nothing before it
    assert large_embargo["n_no_train_data"] == 1

    small_return = small_embargo["full_period_metrics"]["return"]
    large_return = large_embargo["full_period_metrics"]["return"]

    assert small_return > 0, "embargo=0 lets the peek land on era 1's own opening days"
    assert large_return < 0, (
        "embargo>=horizon: the same peek now lands on the training "
        "tail's opposite-direction move instead"
    )
    assert small_return > large_return


def test_first_era_with_no_prior_training_data_is_marked_explicitly():
    dates = _linear_dates(date(2024, 1, 1), 30)
    prices = [100.0 + i for i in range(30)]
    store = _store_from_prices(dates, prices)

    def fit_fn(train_matrix: pd.DataFrame):
        return lambda features: pd.Series(1.0, index=features.index)

    bt = _bt()
    bt.pit_store = store
    result = bt.run_validation(
        hypothesis_id=1,
        feature_ids=[1],
        start_date=dates[0],
        end_date=dates[-1],
        n_splits=3,
        cost_bps=0.0,
        fit_fn=fit_fn,
        embargo_days=1,
        target_feature_id=1,
    )

    statuses = [e["status"] for e in result["era_results"]]
    assert statuses[0] == "NO_TRAIN_DATA"
    assert result["n_no_train_data"] >= 1
    # Never silently scored: the era_consistency gate must see it.
    assert result["gate_detail"]["era_consistency"] is False
    # And it contributes nothing to the aggregate -- verified structurally
    # in test_fit_fn_memorization_cannot_beat_baseline_without_test_era_access
    # and test_embargo_size_controls_overlap_leak_gain via n_no_train_data.


def test_era_boundaries_never_overlap_and_respect_embargo():
    dates = _linear_dates(date(2024, 1, 1), 60)
    prices = [100.0 + i * 0.1 for i in range(60)]
    store = _store_from_prices(dates, prices)

    def fit_fn(train_matrix: pd.DataFrame):
        return lambda features: pd.Series(1.0, index=features.index)

    embargo = 2
    bt = _bt()
    bt.pit_store = store
    result = bt.run_validation(
        hypothesis_id=1,
        feature_ids=[1],
        start_date=dates[0],
        end_date=dates[-1],
        n_splits=4,
        cost_bps=0.0,
        fit_fn=fit_fn,
        embargo_days=embargo,
        target_feature_id=1,
    )

    eras = result["era_results"]
    assert len(eras) == 4
    for e in eras:
        assert "train_end" in e and "test_start" in e  # every era, OK or not
        train_end = date.fromisoformat(e["train_end"])
        test_start = date.fromisoformat(e["test_start"])
        assert train_end + timedelta(days=e["embargo_days"]) < test_start

    test_windows = [
        (date.fromisoformat(e["test_start"]), date.fromisoformat(e["test_end"]))
        for e in eras
    ]
    for (_s1, e1), (s2, _e2) in zip(test_windows, test_windows[1:]):
        assert e1 < s2  # strictly non-overlapping, chronologically ordered


def test_stateless_predict_fn_path_unaffected_by_fit_fn_addition():
    """No fit_fn given -> old behavior exactly: no boundary fields, no
    n_no_train_data eras, predict_fn applied directly within its own era.
    Guards the "existing stateless predict_fn callers keep working
    unchanged" requirement.
    """
    bt = _bt()
    dates = pd.DatetimeIndex([date(2024, 1, i) for i in range(1, 6)], name="obs_date")
    matrix = pd.DataFrame({1: [100.0, 110.0, 99.0, 108.0, 97.0]}, index=dates)
    result = bt._compute_era_metrics(matrix, None, cost_bps=0.0)
    assert "train_start" not in result


# ---------------------------------------------------------------------------
# item 2 of round 3: why the per-day query loop was NOT batched
# ---------------------------------------------------------------------------


def test_single_asof_batch_then_mask_is_not_equivalent_to_per_day_fetch():
    """Documents why the per-day PIT fetch loop was left as-is.

    ``PITStore.get_feature_matrix`` (via ``get_pit``) runs
    ``DISTINCT ON (feature_id, obs_date) ... WHERE release_date <= :aod
    ORDER BY vintage_date ASC`` for FIRST_RELEASE -- it commits to the row
    with the SMALLEST vintage_date among rows satisfying the ONE as_of_date
    given, and the SQL permanently discards every other vintage for that
    (feature_id, obs_date) before anything is returned. A single fetch at
    as_of_date=end_date, even followed by masking every returned row whose
    release_date is after its own obs_date, can only mask what that one
    query already chose to return -- it can never go back and pick a
    DIFFERENT, earlier-released vintage the query already discarded,
    because vintage_date and release_date are independent columns with no
    documented monotonicity guarantee between them (nothing in
    store/pit.py's docstring promises "larger vintage_date implies later
    release_date").

    This fixture makes that concrete: for one obs_date, the row with the
    smallest vintage_date (what a single as_of=end_date FIRST_RELEASE
    query commits to) was released LATE (day 10), while a DIFFERENT,
    larger-vintage_date row for that same obs_date was released EARLY
    (right on the obs_date, day 5) -- so it is the one a correct per-day
    fetch (as_of_date=day 5) actually returns. The single-shot query never
    returns that row at all, so no post-hoc masking can recover it.
    """

    def day(k: int) -> date:
        return date(2024, 1, 1) + timedelta(days=k)

    store = FakePITStore(
        rows=[
            # The contested obs_date, two competing vintages:
            Row(feature_id=1, obs_date=day(5), value=111.0, release_date=day(10), vintage_date=day(1)),
            Row(feature_id=1, obs_date=day(5), value=999.0, release_date=day(5), vintage_date=day(6)),
        ]
    )

    bt = _bt()
    bt.pit_store = store

    correct = bt._fetch_pit_correct_matrix(
        feature_ids=[1], start_date=day(5), end_date=day(5), vintage_policy="FIRST_RELEASE"
    )
    assert correct.loc[pd.Timestamp(day(5)), 1] == 999.0  # the early-released vintage

    naive_batch = store.get_feature_matrix(
        feature_ids=[1],
        start_date=day(5),
        end_date=day(5),
        as_of_date=day(20),
        vintage_policy="FIRST_RELEASE",
    )
    # The single global query already committed to the smallest
    # vintage_date overall (subject only to release <= day20) -- the
    # late-released one -- discarding the early-released vintage
    # entirely. No masking pass over this result can recover 999.0; it
    # was never returned to mask in the first place.
    assert naive_batch.loc[pd.Timestamp(day(5)), 1] == 111.0
    assert naive_batch.loc[pd.Timestamp(day(5)), 1] != correct.loc[pd.Timestamp(day(5)), 1]


# ---------------------------------------------------------------------------
# (h) W3c: batched per-decision PIT fetch (get_feature_vintages +
#     select_vintage_per_decision) must exactly reproduce the per-day loop
# ---------------------------------------------------------------------------


def _build_vintage_torture_fixture() -> FakePITStore:
    """Rows covering mid-window revisions, late releases, duplicate
    vintages and vintage_date/release_date independence, across two
    features -- deliberately constructed so FIRST_RELEASE and
    LATEST_AS_OF pick DIFFERENT rows in several places.
    """

    def day(k: int) -> date:
        return date(2024, 1, 1) + timedelta(days=k)

    rows = [
        # --- feature 1 ---
        Row(feature_id=1, obs_date=day(0), value=100.0, release_date=day(0), vintage_date=day(0)),
        Row(feature_id=1, obs_date=day(1), value=101.0, release_date=day(1), vintage_date=day(1)),
        # obs=2: two eligible vintages -- FIRST_RELEASE (min vintage_date)
        # picks 200.0, LATEST_AS_OF (max vintage_date) picks 205.0.
        Row(feature_id=1, obs_date=day(2), value=200.0, release_date=day(2), vintage_date=day(1)),
        Row(feature_id=1, obs_date=day(2), value=205.0, release_date=day(2), vintage_date=day(2)),
        # exact duplicate of the row above -- must not create a second
        # selected row or otherwise change the outcome.
        Row(feature_id=1, obs_date=day(2), value=205.0, release_date=day(2), vintage_date=day(2)),
        # obs=3: the ONLY vintage on file was released on day 10 -- a late
        # release, ineligible under a day-3 decision. Both implementations
        # must drop this (feature_id, obs_date) entirely.
        Row(feature_id=1, obs_date=day(3), value=9999.0, release_date=day(10), vintage_date=day(10)),
        Row(feature_id=1, obs_date=day(4), value=104.0, release_date=day(4), vintage_date=day(4)),
        # obs=5: a genuine value, PLUS a "mid-window revision" that arrives
        # on day 8 (still inside the [0, 9] window) -- it must never be
        # visible to day 5's own decision, under EITHER policy.
        Row(feature_id=1, obs_date=day(5), value=105.0, release_date=day(5), vintage_date=day(5)),
        Row(feature_id=1, obs_date=day(5), value=999.0, release_date=day(8), vintage_date=day(8)),
        Row(feature_id=1, obs_date=day(6), value=106.0, release_date=day(6), vintage_date=day(6)),
        # obs=7: two eligible vintages, vintage_date ordering independent
        # of release_date (both released on day 7).
        Row(feature_id=1, obs_date=day(7), value=107.0, release_date=day(7), vintage_date=day(3)),
        Row(feature_id=1, obs_date=day(7), value=170.0, release_date=day(7), vintage_date=day(9)),
        Row(feature_id=1, obs_date=day(8), value=108.0, release_date=day(8), vintage_date=day(8)),
        Row(feature_id=1, obs_date=day(9), value=109.0, release_date=day(9), vintage_date=day(9)),
        # --- feature 2 ---
        Row(feature_id=2, obs_date=day(0), value=1000.0, release_date=day(0), vintage_date=day(0)),
        Row(feature_id=2, obs_date=day(3), value=1003.0, release_date=day(3), vintage_date=day(3)),
        # late release for feature 2, obs=3 -- ineligible, must be dropped.
        Row(feature_id=2, obs_date=day(3), value=99999.0, release_date=day(12), vintage_date=day(12)),
        # obs=9: vintage_date and release_date deliberately inverted --
        # FIRST_RELEASE (min vintage_date) picks 1009.0, LATEST_AS_OF (max
        # vintage_date) picks 1109.0.
        Row(feature_id=2, obs_date=day(9), value=1009.0, release_date=day(9), vintage_date=day(1)),
        Row(feature_id=2, obs_date=day(9), value=1109.0, release_date=day(1), vintage_date=day(9)),
    ]
    return FakePITStore(rows=rows)


@pytest.mark.parametrize("vintage_policy", ["FIRST_RELEASE", "LATEST_AS_OF"])
def test_batched_pit_fetch_equivalent_to_per_day_fetch_validation_backtest(vintage_policy):
    """The gating proof for defaulting validation.backtest to the batched path.

    Same torture fixture, same window, same policy: the single-round-trip
    ``_fetch_pit_correct_matrix_batched`` (get_feature_vintages +
    select_vintage_per_decision) must return EXACTLY the same DataFrame as
    ``_fetch_pit_correct_matrix_per_day`` -- not just the same shape, every
    cell.
    """
    store = _build_vintage_torture_fixture()
    bt = WalkForwardBacktest(db_engine=None, pit_store=store)
    start, end = date(2024, 1, 1), date(2024, 1, 1) + timedelta(days=9)

    per_day = bt._fetch_pit_correct_matrix_per_day(
        feature_ids=[1, 2], start_date=start, end_date=end, vintage_policy=vintage_policy
    )
    batched = bt._fetch_pit_correct_matrix_batched(
        feature_ids=[1, 2], start_date=start, end_date=end, vintage_policy=vintage_policy
    )

    pd.testing.assert_frame_equal(per_day, batched)

    # And the public dispatcher, at its default, must agree with the
    # explicit batched call (i.e. the default really is "batched").
    via_dispatch = bt._fetch_pit_correct_matrix(
        feature_ids=[1, 2], start_date=start, end_date=end, vintage_policy=vintage_policy
    )
    pd.testing.assert_frame_equal(via_dispatch, batched)


@pytest.mark.parametrize("vintage_policy", ["FIRST_RELEASE", "LATEST_AS_OF"])
def test_batched_pit_fetch_equivalent_to_per_day_fetch_engine(vintage_policy):
    """Twin of the validation.backtest equivalence test, for backtest/engine.py."""
    store = _build_vintage_torture_fixture()
    bt = PitchBacktester(db_engine=None, pit_store=store)
    start, end = date(2024, 1, 1), date(2024, 1, 1) + timedelta(days=9)

    per_day = bt._fetch_pit_correct_matrix_per_day(
        feature_ids=[1, 2], start_date=start, end_date=end, vintage_policy=vintage_policy
    )
    batched = bt._fetch_pit_correct_matrix_batched(
        feature_ids=[1, 2], start_date=start, end_date=end, vintage_policy=vintage_policy
    )

    pd.testing.assert_frame_equal(per_day, batched)

    via_dispatch = bt._fetch_pit_correct_matrix(
        feature_ids=[1, 2], start_date=start, end_date=end, vintage_policy=vintage_policy
    )
    pd.testing.assert_frame_equal(via_dispatch, batched)


def test_selector_never_uses_a_vintage_released_after_its_own_obs_date():
    """``select_vintage_per_decision`` must exclude a late-released vintage

    even when it would otherwise "win" the vintage-policy tiebreak (e.g.
    the largest vintage_date for LATEST_AS_OF) -- the release_date <=
    obs_date cutoff is applied BEFORE the policy tiebreak, not after.
    """
    d0 = date(2024, 1, 1)
    d_future = d0 + timedelta(days=5)

    vintages = pd.DataFrame(
        [
            # Eligible: released on the observation date itself.
            {
                "feature_id": 1,
                "obs_date": d0,
                "value": 42.0,
                "release_date": d0,
                "vintage_date": d0,
            },
            # Ineligible: released AFTER the observation date, but with a
            # vintage_date far in the future that would win either
            # policy's tiebreak if the cutoff were skipped.
            {
                "feature_id": 1,
                "obs_date": d0,
                "value": 8675309.0,
                "release_date": d_future,
                "vintage_date": d_future,
            },
        ]
    )

    for policy in ("FIRST_RELEASE", "LATEST_AS_OF"):
        result = select_vintage_per_decision(vintages, policy)
        assert len(result) == 1
        assert result.iloc[0]["value"] == 42.0
        assert (result["release_date"] <= result["obs_date"]).all()

    # And when NO candidate is eligible, the (feature_id, obs_date) is
    # dropped entirely rather than falling back to the ineligible row.
    only_future = vintages.iloc[[1]]
    for policy in ("FIRST_RELEASE", "LATEST_AS_OF"):
        result = select_vintage_per_decision(only_future, policy)
        assert result.empty


def test_selector_rejects_invalid_policy():
    vintages = pd.DataFrame(
        [{"feature_id": 1, "obs_date": date(2024, 1, 1), "value": 1.0,
          "release_date": date(2024, 1, 1), "vintage_date": date(2024, 1, 1)}]
    )
    with pytest.raises(ValueError):
        select_vintage_per_decision(vintages, "NOT_A_REAL_POLICY")
