"""
GRID walk-forward backtesting engine.

Provides rigorous walk-forward validation with PIT-correct data access,
era-based evaluation, and baseline comparison.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Sequence

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# PIT price plumbing is shared with the realized-alpha truth gate. Imported
# into this namespace (rather than referenced via the module) so tests can
# ``monkeypatch`` them on ``validation.backtest`` without a live database.
from alpha_research.realized_alpha import (
    _resolve_ticker_feature_id,
    compute_trade_alpha,
    load_price_path,
    normalize_direction,
    price_at,
    resolve_spy_feature,
)
from store.pit import PITStore
from store.vintage_selection import select_vintage_per_decision

HOLD_MIN_SCORED_FOR_VERDICT: int = 5
HOLD_PASS_HIT_RATE: float = 0.5
HOLD_FAIL_HIT_RATE: float = 0.4
_HOLD_PATH_LOOKBACK_DAYS: int = 10

# Default fetch strategy for _fetch_pit_correct_matrix. The batched path
# (one get_feature_vintages round trip per window + select_vintage_per_decision)
# is proven exactly equivalent to the per-day reference loop by
# tests/test_evaluator_contracts.py's
# test_batched_pit_fetch_equivalent_to_per_day_fetch -- that equivalence test
# is what gates this default to True. Flip to False (or pass
# use_batched_fetch=False) to fall back to the one-query-per-calendar-day
# reference implementation if that equivalence is ever in doubt.
USE_BATCHED_PIT_FETCH: bool = True


class WalkForwardBacktest:
    """Walk-forward backtesting engine with PIT-correct data access.

    Splits the evaluation period into non-overlapping eras and computes
    performance metrics in each era to detect overfitting and regime
    dependence.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        pit_store: PITStore for point-in-time data access.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        """Initialise the backtester.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            pit_store: PITStore instance for PIT-correct data.
        """
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("WalkForwardBacktest initialised")

    def run_validation(
        self,
        hypothesis_id: int,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        n_splits: int = 5,
        vintage_policy: str = "FIRST_RELEASE",
        cost_bps: float = 10.0,
        predict_fn: Any = None,
        target_feature_id: int | None = None,
        fit_fn: Any = None,
        embargo_days: int = 0,
    ) -> dict[str, Any]:
        """Run a full walk-forward validation.

        Parameters:
            hypothesis_id: ID of the hypothesis being tested.
            feature_ids: Feature IDs to use.
            start_date: Start of the evaluation period.
            end_date: End of the evaluation period.
            n_splits: Number of walk-forward splits (eras).
            vintage_policy: PIT vintage policy ('FIRST_RELEASE' or 'LATEST_AS_OF').
            cost_bps: Transaction cost assumption in basis points.
            predict_fn: Callable that takes a feature DataFrame and returns
                        predictions. If None, uses a simple baseline. Used
                        directly, unchanged, when ``fit_fn`` is not given --
                        this is the original stateless-predictor path and
                        existing callers keep working exactly as before.
            target_feature_id: Which of ``feature_ids`` is the
                target/underlying return series to trade and score against.
                ``None`` keeps the old implicit "lowest feature id" default
                (logged as a warning naming the id actually used) --
                callers should pass this explicitly rather than relying on
                whatever the smallest id happens to be.
            fit_fn: Optional ``fit_fn(train_matrix) -> predict_fn``. When
                given, each era gets its OWN predictor, fitted only on data
                strictly before that era (and ``predict_fn`` is ignored).
                This is the leakage-safe walk-forward path: the predictor
                for era N can never have seen era N's own rows, or any row
                inside the embargo gap immediately before it.
            embargo_days: Gap, in days, between the end of the training
                window and the start of the test era -- must be at least
                the predictor's own label/prediction horizon, or a training
                example near the boundary can encode information that only
                existed once the test era had already started (see the
                round-3 report for a worked example). Only meaningful when
                ``fit_fn`` is given; ignored otherwise.

        Returns:
            dict: Comprehensive validation results suitable for storing in
                  validation_results table.
        """
        log.info(
            "Running walk-forward validation — hypothesis={h}, {sd} to {ed}, "
            "{n} splits, vintage={v}",
            h=hypothesis_id,
            sd=start_date,
            ed=end_date,
            n=n_splits,
            v=vintage_policy,
        )

        # Build walk-forward splits
        total_days = (end_date - start_date).days
        split_days = total_days // n_splits

        era_results: list[dict[str, Any]] = []
        # Only populated when fit_fn is given: each OK era's own realized,
        # out-of-sample return series, concatenated below into the
        # leakage-safe full-period aggregate instead of re-running one
        # predictor across era boundaries (there is no single stateless
        # predictor in the fit_fn path -- every era has its own).
        era_returns_for_aggregate: list[pd.Series] = []
        n_no_train_data = 0

        for i in range(n_splits):
            era_start = start_date + timedelta(days=i * split_days)
            era_end = era_start + timedelta(days=split_days - 1)
            if i == n_splits - 1:
                era_end = end_date

            log.info("Era {i}/{n}: {s} to {e}", i=i + 1, n=n_splits, s=era_start, e=era_end)

            era_predict_fn = predict_fn
            boundary_fields: dict[str, Any] = {}

            if fit_fn is not None:
                # Train window = everything from the overall start up to
                # train_end; train_end is pushed back from the era's own
                # start by embargo_days + 1 so train_end + embargo_days is
                # STRICTLY before test_start -- no day is ever in both.
                train_start = start_date
                train_end = era_start - timedelta(days=embargo_days + 1)
                boundary_fields = {
                    "train_start": train_start.isoformat(),
                    "train_end": train_end.isoformat(),
                    "test_start": era_start.isoformat(),
                    "test_end": era_end.isoformat(),
                    "embargo_days": embargo_days,
                }

                if train_end < train_start:
                    log.warning(
                        "Era {i}: no training data before the embargo "
                        "(train_end={te} < train_start={ts}); skipping, "
                        "not scoring",
                        i=i + 1, te=train_end, ts=train_start,
                    )
                    n_no_train_data += 1
                    era_results.append({
                        "era": i + 1,
                        "start": era_start.isoformat(),
                        "end": era_end.isoformat(),
                        "n_observations": 0,
                        "status": "NO_TRAIN_DATA",
                        **boundary_fields,
                    })
                    continue

                train_matrix = self._fetch_pit_correct_matrix(
                    feature_ids=feature_ids,
                    start_date=train_start,
                    end_date=train_end,
                    vintage_policy=vintage_policy,
                )
                train_matrix = train_matrix.ffill().dropna()

                if train_matrix.empty or train_matrix.shape[0] < 10:
                    log.warning(
                        "Era {i}: insufficient training data ({n} rows "
                        "after embargo); skipping, not scoring",
                        i=i + 1, n=len(train_matrix),
                    )
                    n_no_train_data += 1
                    era_results.append({
                        "era": i + 1,
                        "start": era_start.isoformat(),
                        "end": era_end.isoformat(),
                        "n_observations": 0,
                        "status": "NO_TRAIN_DATA",
                        **boundary_fields,
                    })
                    continue

                # Fit strictly on the train window; the returned predictor
                # is the only thing that carries information across the
                # train/test boundary, and it never saw a row at or after
                # train_end + embargo_days.
                era_predict_fn = fit_fn(train_matrix)

            # Get PIT-correct feature matrix for this era. Fetched one
            # calendar day at a time (see _fetch_pit_correct_matrix) so a
            # revision or release that only became available later in the
            # era can never leak into an earlier day's row.
            matrix = self._fetch_pit_correct_matrix(
                feature_ids=feature_ids,
                start_date=era_start,
                end_date=era_end,
                vintage_policy=vintage_policy,
            )

            if matrix.empty or matrix.shape[0] < 10:
                log.warning("Era {i} has insufficient data ({n} rows)", i=i + 1, n=len(matrix))
                era_results.append({
                    "era": i + 1,
                    "start": era_start.isoformat(),
                    "end": era_end.isoformat(),
                    "n_observations": len(matrix),
                    "status": "INSUFFICIENT_DATA",
                    **boundary_fields,
                })
                continue

            # Forward-fill and drop NaN
            matrix = matrix.ffill().dropna()

            # Compute era metrics
            era_metric, era_adjusted_returns = self._compute_era_metrics_with_returns(
                matrix, era_predict_fn, cost_bps, target_feature_id
            )
            if fit_fn is not None and era_adjusted_returns is not None:
                era_returns_for_aggregate.append(era_adjusted_returns)
            era_metric.update(boundary_fields)
            era_metric["era"] = i + 1
            era_metric["start"] = era_start.isoformat()
            era_metric["end"] = era_end.isoformat()
            era_metric["n_observations"] = len(matrix)
            era_metric["status"] = "OK"
            era_results.append(era_metric)

        if fit_fn is not None:
            # Leakage-safe aggregate: concatenate each OK era's own
            # out-of-sample returns rather than re-fitting or re-running a
            # single predictor across the whole start..end span (there is
            # no such single predictor in this path -- that is the point).
            if era_returns_for_aggregate:
                combined_returns = pd.concat(era_returns_for_aggregate).sort_index()
                full_metrics = self._summarize_returns(combined_returns)
            else:
                full_metrics = {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "n_days": 0}

            # Baseline is just "what would buy-and-hold have done over the
            # whole window" -- it has no predictor and nothing to leak, so
            # it is unaffected by the train/test split.
            baseline_matrix = self._fetch_pit_correct_matrix(
                feature_ids=feature_ids,
                start_date=start_date,
                end_date=end_date,
                vintage_policy=vintage_policy,
            )
            baseline_matrix = baseline_matrix.ffill().dropna()
            baseline = self._compute_baseline_metrics(baseline_matrix, target_feature_id)
        else:
            # Compute full-period metrics (same per-day PIT fetch as each era).
            full_matrix = self._fetch_pit_correct_matrix(
                feature_ids=feature_ids,
                start_date=start_date,
                end_date=end_date,
                vintage_policy=vintage_policy,
            )
            full_matrix = full_matrix.ffill().dropna()
            full_metrics = self._compute_era_metrics(
                full_matrix, predict_fn, cost_bps, target_feature_id
            )

            # Baseline comparison (buy-and-hold equivalent)
            baseline = self._compute_baseline_metrics(full_matrix, target_feature_id)

        # Simplicity comparison
        simplicity = self._compute_simplicity_comparison(
            full_metrics, baseline, len(feature_ids)
        )

        # Overall verdict
        verdict = self._determine_verdict(era_results, full_metrics, baseline)

        result = {
            "hypothesis_id": hypothesis_id,
            "vintage_policy": vintage_policy,
            "era_results": era_results,
            "full_period_metrics": full_metrics,
            "baseline_comparison": baseline,
            "simplicity_comparison": simplicity,
            "walk_forward_splits": n_splits,
            "cost_assumption_bps": cost_bps,
            "overall_verdict": verdict,
            # Eras skipped for having no (or too little) leakage-safe
            # training data -- excluded from every aggregate above, never
            # silently folded into "OK". Always present (0 when fit_fn
            # wasn't used) so a caller doesn't have to special-case it.
            "n_no_train_data": n_no_train_data,
            "gate_detail": {
                "era_consistency": all(
                    e.get("status") == "OK" for e in era_results
                ),
                "beats_baseline": full_metrics.get("sharpe", 0) > baseline.get("sharpe", 0),
                "positive_in_all_eras": all(
                    e.get("return", 0) > 0 for e in era_results if e.get("status") == "OK"
                ),
            },
        }

        # Store in validation_results
        self._store_result(result)

        log.info("Validation complete — verdict={v}", v=verdict)
        return result

    def _fetch_pit_correct_matrix(
        self,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        vintage_policy: str,
        use_batched_fetch: bool = USE_BATCHED_PIT_FETCH,
    ) -> pd.DataFrame:
        """Build a feature matrix where every row is only as current as its
        own observation date.

        Dispatches to one of two implementations that are proven exactly
        equivalent (see
        ``tests/test_evaluator_contracts.py::test_batched_pit_fetch_equivalent_to_per_day_fetch``):

        - ``use_batched_fetch=True`` (the default -- gated on that
          equivalence test passing): ``_fetch_pit_correct_matrix_batched``,
          a single ``get_feature_vintages`` round trip for the whole
          window plus a pure-Python per-row selection.
        - ``use_batched_fetch=False``: ``_fetch_pit_correct_matrix_per_day``,
          the original one-``get_feature_matrix``-call-per-calendar-day
          reference implementation, kept as the correctness baseline.

        Parameters mirror both implementations; see their docstrings for
        the correctness argument each relies on.
        """
        if use_batched_fetch:
            return self._fetch_pit_correct_matrix_batched(
                feature_ids=feature_ids,
                start_date=start_date,
                end_date=end_date,
                vintage_policy=vintage_policy,
            )
        return self._fetch_pit_correct_matrix_per_day(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=end_date,
            vintage_policy=vintage_policy,
        )

    def _fetch_pit_correct_matrix_per_day(
        self,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        vintage_policy: str,
    ) -> pd.DataFrame:
        """Reference implementation: build the matrix one calendar day at a time.

        ``PITStore.get_feature_matrix`` takes a single ``as_of_date`` for
        the whole call. Passing the era/window's *end* date (as the old
        code did) means a row near the start of the window can pick up a
        release or revision that only became available later in the
        window but still before that single cutoff -- lookahead relative
        to that row's own decision point, and a real problem under
        ``LATEST_AS_OF`` vintage policy where later revisions are exactly
        what gets selected.

        Fetching one calendar day at a time, with ``as_of_date`` pinned to
        that same day, removes the leak: nothing dated after day ``d`` can
        ever appear in day ``d``'s row, regardless of vintage policy. Kept
        as the correctness reference that ``_fetch_pit_correct_matrix_batched``
        is checked against — see CLAUDE.md's PIT-correctness guardrail.
        """
        frames: list[pd.DataFrame] = []
        current = start_date
        while current <= end_date:
            daily = self.pit_store.get_feature_matrix(
                feature_ids=feature_ids,
                start_date=current,
                end_date=current,
                as_of_date=current,
                vintage_policy=vintage_policy,
            )
            if not daily.empty:
                frames.append(daily)
            current += timedelta(days=1)

        if not frames:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="obs_date"))

        combined = pd.concat(frames)
        # Defensive de-dup: a duplicate (feature_id, obs_date) row can never
        # inflate the sample count seen downstream.
        combined = combined[~combined.index.duplicated(keep="first")]
        return combined.sort_index()

    def _fetch_pit_correct_matrix_batched(
        self,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        vintage_policy: str,
    ) -> pd.DataFrame:
        """Single-round-trip PIT fetch: get_feature_vintages + per-decision selection.

        Fetches every vintage for (feature_id, obs_date) in
        [start_date, end_date] ONCE (``PITStore.get_feature_vintages``,
        pre-filtered on ``release_date <= end_date`` as a throughput
        optimisation only), then applies
        ``select_vintage_per_decision`` -- which enforces
        ``release_date <= obs_date`` PER ROW, not against a single shared
        cutoff -- to pick exactly the vintage
        ``_fetch_pit_correct_matrix_per_day`` would have picked for that
        row. This is what makes batching safe: the coarse SQL pre-filter
        can only ever be looser than the per-row cutoff applied afterwards
        in Python, never tighter, so it cannot discard a vintage the
        per-day loop would have kept.

        Proven exactly equivalent to the per-day loop by
        ``tests/test_evaluator_contracts.py::test_batched_pit_fetch_equivalent_to_per_day_fetch``
        across mid-window revisions, late releases, duplicate vintages,
        and both vintage policies -- that test is what gates this as the
        default. Real-database throughput is unmeasured (no local
        Postgres available to this change); only row-for-row equivalence
        against the reference loop is verified here.
        """
        vintages = self.pit_store.get_feature_vintages(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=end_date,
            as_of_date=end_date,
        )

        selected = select_vintage_per_decision(vintages, vintage_policy)
        if selected.empty:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="obs_date"))

        matrix = selected.pivot_table(
            index="obs_date", columns="feature_id", values="value", aggfunc="first"
        )
        matrix.index = pd.DatetimeIndex(matrix.index, name="obs_date")
        return matrix.sort_index()

    def _compute_era_metrics(
        self,
        matrix: pd.DataFrame,
        predict_fn: Any,
        cost_bps: float,
        target_feature_id: int | None = None,
    ) -> dict[str, Any]:
        """Compute performance metrics for a single era.

        Thin wrapper around ``_compute_era_metrics_with_returns`` that drops
        the raw per-row returns series -- kept so existing callers (and
        tests) that only want the summary dict don't need to change.
        """
        metrics, _adjusted_returns = self._compute_era_metrics_with_returns(
            matrix, predict_fn, cost_bps, target_feature_id
        )
        return metrics

    def _compute_era_metrics_with_returns(
        self,
        matrix: pd.DataFrame,
        predict_fn: Any,
        cost_bps: float,
        target_feature_id: int | None = None,
    ) -> tuple[dict[str, Any], pd.Series | None]:
        """Compute performance metrics for a single era.

        Parameters:
            matrix: Feature matrix for the era.
            predict_fn: Callable taking the feature matrix (column-sorted,
                target column removed) and returning a position/signal per
                row (aligned to ``matrix.index``). ``None`` falls back to a
                fully-invested buy-and-hold baseline.
            cost_bps: Cost assumption in basis points.
            target_feature_id: Which column is the target/underlying return
                series. ``None`` keeps the old "lowest feature id, whatever
                it is" default (a ``log.warning`` names which one was
                picked, since that's an arbitrary choice the caller should
                usually override). When set, that column is both the return
                series AND excluded from what ``predict_fn`` gets to see.

        Returns:
            tuple: ``(metrics_dict, adjusted_returns)``. ``adjusted_returns``
            is the per-row cost-adjusted strategy-return series actually
            scored (or ``None`` when there was nothing to score) -- used by
            ``run_validation`` to build a leakage-safe full-period
            aggregate by concatenating each era's own out-of-sample
            returns, rather than re-running a single predictor across era
            boundaries.
        """
        if matrix.empty:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "n_days": 0}, None

        # Column order must never affect the result: sort by label instead
        # of relying on positional iloc (which silently picked up whatever
        # column happened to land first), and collapse duplicate obs_date
        # rows before they can inflate the sample count.
        matrix = matrix.sort_index(axis=1)
        matrix = matrix[~matrix.index.duplicated(keep="first")]

        if matrix.shape[1] == 0:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "n_days": 0}, None

        if target_feature_id is not None:
            if target_feature_id not in matrix.columns:
                log.warning(
                    "target_feature_id {t} not present in this era's "
                    "matrix (columns={c}); nothing to score against",
                    t=target_feature_id, c=list(matrix.columns),
                )
                return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "n_days": 0}, None
            target_col = target_feature_id
        else:
            # Sorted-label default: order-independent, but still an
            # arbitrary pick among the requested feature_ids -- name it so
            # a caller relying on the default can see what actually got
            # traded.
            target_col = matrix.columns[0]
            log.warning(
                "No target_feature_id given; defaulting to the lowest "
                "feature id ({t}) as the target/underlying return series",
                t=target_col,
            )

        target = matrix[target_col]
        # predict_fn never sees the target column -- it should not be
        # able to read the very series it's trying to predict.
        features = matrix.drop(columns=[target_col])

        target_returns = target.pct_change()
        n_total = int(len(target_returns))
        valid_returns = target_returns.dropna()
        n_missing = n_total - int(len(valid_returns))

        if valid_returns.empty:
            return {
                "return": 0.0,
                "sharpe": 0.0,
                "max_drawdown": 0.0,
                "n_days": 0,
                "n_missing": n_missing,
                "n_total": n_total,
            }, None

        # Predictions actually drive the result now. A missing/None
        # prediction for a row is treated as "no position" (0), not as a
        # wrong call and not dropped from the denominator.
        if predict_fn is not None:
            raw_signal = pd.Series(predict_fn(features), index=matrix.index)
        else:
            raw_signal = pd.Series(1.0, index=matrix.index)

        # Same-bar lookahead guard: a signal computed from row t's own
        # features (which includes t's own already-realized return) has
        # not been decided yet when target_returns[t] is realized -- it can
        # only be acted on starting the NEXT bar. Shift by one period so
        # the position decided at t earns the return from t to t+1, not
        # the return that already happened getting to t.
        lagged_signal = raw_signal.shift(1).reindex(valid_returns.index)
        n_warmup = int(lagged_signal.isna().sum())
        signal = lagged_signal.astype(float).fillna(0.0)

        strategy_returns = signal * valid_returns

        # Apply cost adjustment
        cost_adjustment = cost_bps / 10000.0
        adjusted_returns = strategy_returns - cost_adjustment / 252  # Daily cost

        metrics = self._summarize_returns(adjusted_returns)
        metrics["n_missing"] = n_missing
        metrics["n_warmup"] = n_warmup
        metrics["n_total"] = n_total
        return metrics, adjusted_returns

    @staticmethod
    def _summarize_returns(adjusted_returns: pd.Series) -> dict[str, Any]:
        """Turn a per-row cost-adjusted return series into summary stats.

        Shared by ``_compute_era_metrics_with_returns`` (one era) and
        ``run_validation``'s leakage-safe aggregate (many eras'
        out-of-sample returns concatenated) so both report numbers the
        same way.
        """
        if adjusted_returns is None or adjusted_returns.empty:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "n_days": 0}

        cum_return = float((1 + adjusted_returns).prod() - 1)
        ann_return = float((1 + cum_return) ** (252 / max(len(adjusted_returns), 1)) - 1)
        ann_vol = float(adjusted_returns.std() * np.sqrt(252))
        sharpe = ann_return / ann_vol if ann_vol > 0 else 0.0

        # Max drawdown
        cum = (1 + adjusted_returns).cumprod()
        peak = cum.expanding().max()
        drawdown = (cum - peak) / peak
        max_dd = float(drawdown.min())

        return {
            "return": round(cum_return, 6),
            "annualised_return": round(ann_return, 6),
            "annualised_vol": round(ann_vol, 6),
            "sharpe": round(sharpe, 4),
            "max_drawdown": round(max_dd, 6),
            "n_days": len(adjusted_returns),
        }

    def _compute_baseline_metrics(
        self,
        matrix: pd.DataFrame,
        target_feature_id: int | None = None,
    ) -> dict[str, Any]:
        """Compute baseline (buy-and-hold) metrics.

        Parameters:
            matrix: Full period feature matrix.
            target_feature_id: Same meaning as in ``_compute_era_metrics``.
                The baseline has no ``predict_fn`` to hide the target from,
                so this only picks which column to buy-and-hold.

        Returns:
            dict: Baseline performance metrics.
        """
        if matrix.empty or matrix.shape[1] == 0:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        matrix = matrix.sort_index(axis=1)
        matrix = matrix[~matrix.index.duplicated(keep="first")]

        if target_feature_id is not None:
            if target_feature_id not in matrix.columns:
                log.warning(
                    "target_feature_id {t} not present in the baseline "
                    "matrix (columns={c}); nothing to score against",
                    t=target_feature_id, c=list(matrix.columns),
                )
                return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}
            target_col = target_feature_id
        else:
            target_col = matrix.columns[0]
            log.warning(
                "No target_feature_id given; defaulting to the lowest "
                "feature id ({t}) as the baseline's buy-and-hold series",
                t=target_col,
            )

        returns = matrix[target_col].pct_change().dropna()
        if returns.empty:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        cum_return = float((1 + returns).prod() - 1)
        ann_vol = float(returns.std() * np.sqrt(252))
        ann_return = float((1 + cum_return) ** (252 / max(len(returns), 1)) - 1)
        sharpe = ann_return / ann_vol if ann_vol > 0 else 0.0

        return {
            "return": round(cum_return, 6),
            "sharpe": round(sharpe, 4),
            "max_drawdown": 0.0,
            "type": "buy_and_hold",
        }

    def _compute_simplicity_comparison(
        self,
        full_metrics: dict[str, Any],
        baseline: dict[str, Any],
        n_features: int,
    ) -> dict[str, Any]:
        """Compare strategy complexity vs performance gain.

        Parameters:
            full_metrics: Full-period strategy metrics.
            baseline: Baseline metrics.
            n_features: Number of features used.

        Returns:
            dict: Simplicity comparison results.
        """
        sharpe_gain = full_metrics.get("sharpe", 0) - baseline.get("sharpe", 0)

        return {
            "n_features": n_features,
            "sharpe_gain_over_baseline": round(sharpe_gain, 4),
            "gain_per_feature": round(sharpe_gain / max(n_features, 1), 4),
            "complexity_justified": sharpe_gain > 0.1,
        }

    def _determine_verdict(
        self,
        era_results: list[dict[str, Any]],
        full_metrics: dict[str, Any],
        baseline: dict[str, Any],
    ) -> str:
        """Determine the overall validation verdict.

        Parameters:
            era_results: Per-era metrics.
            full_metrics: Full-period metrics.
            baseline: Baseline metrics.

        Returns:
            str: 'PASS', 'FAIL', or 'CONDITIONAL'.
        """
        valid_eras = [e for e in era_results if e.get("status") == "OK"]

        if not valid_eras:
            return "FAIL"

        # Must beat baseline
        if full_metrics.get("sharpe", 0) <= baseline.get("sharpe", 0):
            return "FAIL"

        # Must have positive return in majority of eras
        positive_eras = sum(1 for e in valid_eras if e.get("return", 0) > 0)
        if positive_eras < len(valid_eras) * 0.6:
            return "FAIL"

        # All eras positive = PASS, otherwise CONDITIONAL
        if positive_eras == len(valid_eras):
            return "PASS"

        return "CONDITIONAL"

    def _store_result(self, result: dict[str, Any]) -> None:
        """Store validation result in the validation_results table.

        Parameters:
            result: Complete validation result dict.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO validation_results
                        (hypothesis_id, vintage_policy, era_results,
                         full_period_metrics, baseline_comparison,
                         simplicity_comparison, walk_forward_splits,
                         cost_assumption_bps, overall_verdict, gate_detail)
                        VALUES
                        (:hid, :vp, :er, :fpm, :bc, :sc, :wfs, :cab, :ov, :gd)
                    """),
                    {
                        "hid": result["hypothesis_id"],
                        "vp": result["vintage_policy"],
                        "er": json.dumps(result["era_results"]),
                        "fpm": json.dumps(result["full_period_metrics"]),
                        "bc": json.dumps(result["baseline_comparison"]),
                        "sc": json.dumps(result["simplicity_comparison"]),
                        "wfs": result["walk_forward_splits"],
                        "cab": result["cost_assumption_bps"],
                        "ov": result["overall_verdict"],
                        "gd": json.dumps(result["gate_detail"]),
                    },
                )
            log.info("Validation result stored for hypothesis {h}", h=result["hypothesis_id"])
        except Exception as exc:
            log.error("Failed to store validation result: {err}", err=str(exc))


# ─────────────────────────────────────────────────────────────────────
# Single-name hold validator (LEVER-PACKAGE.md §7 T2.3)
# ─────────────────────────────────────────────────────────────────────


def _coerce_date(value: date | datetime | pd.Timestamp | str) -> date:
    """Normalise an entry-date input to a plain ``date``."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _feature_name_for_id(engine: Engine, feature_id: int) -> str | None:
    """Human-readable ``feature_registry.name`` for ``feature_id`` (read-only).

    Best effort: any failure returns ``None`` so a naming lookup can never
    block the validation itself.
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT name FROM feature_registry WHERE id = :fid"),
                {"fid": int(feature_id)},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("hold_validation: feature name lookup failed: {e}", e=str(exc))
        return None
    if row is None:
        return None
    return str(row[0])


def _hold_stats(alphas: list[float]) -> dict[str, float | None]:
    """Summary statistics over the scored alphas (numpy, sample stderr)."""
    if not alphas:
        return {
            "hit_rate": None,
            "mean_alpha": None,
            "median_alpha": None,
            "alpha_t_stat": None,
            "min_alpha": None,
            "max_alpha": None,
        }
    arr = np.asarray(alphas, dtype="float64")
    n = int(arr.size)
    t_stat: float | None = None
    if n >= 2:
        sd = float(arr.std(ddof=1))
        if sd > 0.0 and np.isfinite(sd):
            t_stat = float(arr.mean() / (sd / np.sqrt(n)))
    return {
        "hit_rate": float((arr > 0).sum() / n),
        "mean_alpha": float(arr.mean()),
        "median_alpha": float(np.median(arr)),
        "alpha_t_stat": t_stat,
        "min_alpha": float(arr.min()),
        "max_alpha": float(arr.max()),
    }


def _hold_verdict(
    n_scored: int,
    mean_alpha: float | None,
    hit_rate: float | None,
) -> tuple[str, str]:
    """``(verdict, reason)`` per the T2.3 rule set."""
    if n_scored < HOLD_MIN_SCORED_FOR_VERDICT or mean_alpha is None or hit_rate is None:
        return (
            "insufficient",
            f"Only {n_scored} scored entries; need at least "
            f"{HOLD_MIN_SCORED_FOR_VERDICT} closed holds to judge the thesis.",
        )
    if mean_alpha > 0.0 and hit_rate >= HOLD_PASS_HIT_RATE:
        return (
            "pass",
            f"Mean alpha {mean_alpha:+.2%} with hit rate {hit_rate:.0%} "
            f"over {n_scored} scored entries.",
        )
    if mean_alpha <= 0.0 or hit_rate < HOLD_FAIL_HIT_RATE:
        return (
            "fail",
            f"Mean alpha {mean_alpha:+.2%} with hit rate {hit_rate:.0%} "
            f"over {n_scored} scored entries does not beat SPY net of costs.",
        )
    return (
        "insufficient",
        f"Mean alpha {mean_alpha:+.2%} is positive but hit rate {hit_rate:.0%} "
        f"sits in the {HOLD_FAIL_HIT_RATE:.0%}-{HOLD_PASS_HIT_RATE:.0%} grey zone.",
    )


def run_hold_validation(
    engine: Engine,
    ticker: str,
    entry_dates: Sequence[date],
    hold_days: int,
    *,
    cost_bps: float = 5.0,
    direction: str = "LONG",
    as_of: date | None = None,
) -> dict[str, Any]:
    """Empirically test a multi-month single-name thesis against SPY.

    "If I had bought ``ticker`` on each of ``entry_dates`` and held
    ``hold_days`` calendar days, what was my alpha vs SPY net of costs?"

    Every price is read through ``store/pit.py`` (via the realized-alpha
    helpers) with ``as_of`` as the vintage cut-off, so nothing after
    ``as_of`` can leak in. Missing prices are never invented: an entry
    whose entry/exit price cannot be observed is reported as ``skipped``
    with the reason, and the batch continues.

    Parameters:
        engine: SQLAlchemy engine (read-only use).
        ticker: Ticker symbol; resolved to its close feature in ``feature_registry``.
        entry_dates: Hypothetical entry dates (calendar dates).
        hold_days: Holding period in calendar days; exit = entry + hold_days.
        cost_bps: Cost per side in basis points (charged twice).
        direction: ``LONG`` or ``SHORT`` (aliases accepted, see
            ``normalize_direction``).
        as_of: PIT cut-off; defaults to today. Entries exiting after
            ``as_of`` are ``open`` — marked to the last PIT close on or
            before ``as_of`` and excluded from the summary statistics.

    Returns:
        JSON-safe dict with per-entry rows, counts, summary statistics and
        a ``pass`` / ``fail`` / ``insufficient`` verdict.

    Raises:
        ValueError: no price feature for ``ticker``, bad ``hold_days``,
            unknown ``direction``, or SPY benchmark unavailable.
    """
    if hold_days <= 0:
        raise ValueError("hold_days must be a positive number of calendar days")
    if cost_bps < 0:
        raise ValueError("cost_bps must be non-negative")
    side = normalize_direction(direction)
    if side is None:
        raise ValueError(f"unrecognised direction: {direction!r}")

    tk = str(ticker).strip().upper()
    as_of_d = _coerce_date(as_of) if as_of is not None else date.today()
    entries_in: list[date] = sorted(_coerce_date(d) for d in entry_dates)

    feature_id = _resolve_ticker_feature_id(engine, tk)
    if feature_id is None:
        raise ValueError(f"no price feature for ticker {tk!r}")
    feature_name = _feature_name_for_id(engine, feature_id)

    try:
        spy_id, spy_name = resolve_spy_feature(engine)
    except LookupError as exc:
        raise ValueError(f"SPY benchmark unavailable: {exc}") from exc

    base: dict[str, Any] = {
        "ticker": tk,
        "direction": side,
        "hold_days": int(hold_days),
        "cost_bps": float(cost_bps),
        "as_of": as_of_d.isoformat(),
        "feature_name": feature_name,
        "feature_id": int(feature_id),
        "spy_feature": spy_name,
    }

    if not entries_in:
        verdict, reason = _hold_verdict(0, None, None)
        return {
            **base,
            "entries": [],
            "n_scored": 0,
            "n_open": 0,
            "n_skipped": 0,
            "mean_net_return": None,
            "mean_spy_return": None,
            **_hold_stats([]),
            "verdict": verdict,
            "verdict_reason": reason,
        }

    # Load both paths ONCE for the whole batch; only as_of is passed as the
    # vintage cut-off so no read can see past the decision timestamp.
    path_start = entries_in[0] - timedelta(days=_HOLD_PATH_LOOKBACK_DAYS)
    price_path = load_price_path(engine, feature_id, path_start, as_of_d, as_of_d)
    spy_path = load_price_path(engine, spy_id, path_start, as_of_d, as_of_d)
    log.info(
        "hold_validation: {t} x{n} entries, hold={h}d, as_of={a}, "
        "{p} ticker closes, {s} SPY closes (feature={f}, spy={sf})",
        t=tk, n=len(entries_in), h=hold_days, a=as_of_d,
        p=len(price_path), s=len(spy_path), f=feature_name, sf=spy_name,
    )

    mark_date: date | None = None
    if len(price_path) > 0:
        try:
            last_ts = price_path.index[price_path.index <= pd.Timestamp(as_of_d)][-1]
            mark_date = pd.Timestamp(last_ts).date()
        except IndexError:
            mark_date = None

    entries_out: list[dict[str, Any]] = []
    scored_alphas: list[float] = []
    scored_net: list[float] = []
    scored_spy: list[float] = []
    n_open = 0
    n_skipped = 0

    for entry_d in entries_in:
        exit_d = entry_d + timedelta(days=int(hold_days))
        row: dict[str, Any] = {
            "entry_date": entry_d.isoformat(),
            "exit_date": exit_d.isoformat(),
            "entry_price": None,
            "exit_price": None,
            "gross_return": None,
            "net_return": None,
            "spy_return": None,
            "alpha": None,
            "holding_days": None,
            "status": "skipped",
        }

        if entry_d > as_of_d:
            row["reason"] = f"entry_date {entry_d} is after as_of {as_of_d}"
            n_skipped += 1
            entries_out.append(row)
            continue

        is_open = exit_d > as_of_d
        try:
            entry_px = price_at(price_path, entry_d)
            if is_open:
                if mark_date is None or mark_date < entry_d:
                    raise ValueError(
                        f"no PIT close between entry {entry_d} and as_of {as_of_d} to mark open hold"
                    )
                score_exit_d = mark_date
            else:
                score_exit_d = exit_d
            exit_px = price_at(price_path, score_exit_d)
            res = compute_trade_alpha(
                entry_d, score_exit_d, entry_px, exit_px, side, spy_path, cost_bps=cost_bps,
            )
        except ValueError as exc:
            row["reason"] = str(exc)
            n_skipped += 1
            log.debug("hold_validation: {t} entry {d} skipped: {e}", t=tk, d=entry_d, e=str(exc))
            entries_out.append(row)
            continue

        row.update({
            "entry_price": float(entry_px),
            "exit_price": float(exit_px),
            "gross_return": float(res.gross_return),
            "net_return": float(res.net_return),
            "spy_return": float(res.spy_return),
            "alpha": float(res.alpha),
            "holding_days": int(res.holding_days),
        })
        if is_open:
            row["status"] = "open"
            row["mark_date"] = score_exit_d.isoformat()
            n_open += 1
        else:
            row["status"] = "scored"
            scored_alphas.append(float(res.alpha))
            scored_net.append(float(res.net_return))
            scored_spy.append(float(res.spy_return))
        entries_out.append(row)

    n_scored = len(scored_alphas)
    stats = _hold_stats(scored_alphas)
    verdict, reason = _hold_verdict(n_scored, stats["mean_alpha"], stats["hit_rate"])
    log.info(
        "hold_validation: {t} verdict={v} scored={s} open={o} skipped={k} "
        "mean_alpha={m} hit_rate={hr}",
        t=tk, v=verdict, s=n_scored, o=n_open, k=n_skipped,
        m=stats["mean_alpha"], hr=stats["hit_rate"],
    )

    return {
        **base,
        "entries": entries_out,
        "n_scored": n_scored,
        "n_open": n_open,
        "n_skipped": n_skipped,
        "mean_net_return": float(np.mean(scored_net)) if scored_net else None,
        "mean_spy_return": float(np.mean(scored_spy)) if scored_spy else None,
        **stats,
        "verdict": verdict,
        "verdict_reason": reason,
    }


if __name__ == "__main__":
    import argparse

    from db import get_engine

    parser = argparse.ArgumentParser(description="GRID validation engines")
    sub = parser.add_subparsers(dest="cmd")
    hold = sub.add_parser("hold", help="single-name hold validator (T2.3)")
    hold.add_argument("--ticker", required=True)
    hold.add_argument(
        "--entries", required=True,
        help="comma-separated entry dates, YYYY-MM-DD",
    )
    hold.add_argument("--hold-days", type=int, required=True)
    hold.add_argument("--cost-bps", type=float, default=5.0)
    hold.add_argument("--direction", default="LONG")
    hold.add_argument("--as-of", default=None, help="PIT cut-off, YYYY-MM-DD (default today)")
    args = parser.parse_args()

    engine = get_engine()
    if args.cmd == "hold":
        result = run_hold_validation(
            engine,
            args.ticker,
            [date.fromisoformat(s.strip()) for s in args.entries.split(",") if s.strip()],
            args.hold_days,
            cost_bps=args.cost_bps,
            direction=args.direction,
            as_of=date.fromisoformat(args.as_of) if args.as_of else None,
        )
        print(json.dumps(result, indent=2))
    else:
        pit = PITStore(engine)
        bt = WalkForwardBacktest(engine, pit)
        print("WalkForwardBacktest ready")
