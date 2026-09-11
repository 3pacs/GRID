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

HOLD_MIN_SCORED_FOR_VERDICT: int = 5
HOLD_PASS_HIT_RATE: float = 0.5
HOLD_FAIL_HIT_RATE: float = 0.4
_HOLD_PATH_LOOKBACK_DAYS: int = 10


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
                        predictions. If None, uses a simple baseline.

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

        for i in range(n_splits):
            era_start = start_date + timedelta(days=i * split_days)
            era_end = era_start + timedelta(days=split_days - 1)
            if i == n_splits - 1:
                era_end = end_date

            log.info("Era {i}/{n}: {s} to {e}", i=i + 1, n=n_splits, s=era_start, e=era_end)

            # Get PIT-correct feature matrix for this era
            matrix = self.pit_store.get_feature_matrix(
                feature_ids=feature_ids,
                start_date=era_start,
                end_date=era_end,
                as_of_date=era_end,
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
                })
                continue

            # Forward-fill and drop NaN
            matrix = matrix.ffill().dropna()

            # Compute era metrics
            era_metric = self._compute_era_metrics(matrix, predict_fn, cost_bps)
            era_metric["era"] = i + 1
            era_metric["start"] = era_start.isoformat()
            era_metric["end"] = era_end.isoformat()
            era_metric["n_observations"] = len(matrix)
            era_metric["status"] = "OK"
            era_results.append(era_metric)

        # Compute full-period metrics
        full_matrix = self.pit_store.get_feature_matrix(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=end_date,
            as_of_date=end_date,
            vintage_policy=vintage_policy,
        )
        full_matrix = full_matrix.ffill().dropna()
        full_metrics = self._compute_era_metrics(full_matrix, predict_fn, cost_bps)

        # Baseline comparison (buy-and-hold equivalent)
        baseline = self._compute_baseline_metrics(full_matrix)

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

    def _compute_era_metrics(
        self,
        matrix: pd.DataFrame,
        predict_fn: Any,
        cost_bps: float,
    ) -> dict[str, Any]:
        """Compute performance metrics for a single era.

        Parameters:
            matrix: Feature matrix for the era.
            predict_fn: Prediction function (or None for baseline).
            cost_bps: Cost assumption in basis points.

        Returns:
            dict: Era performance metrics.
        """
        if matrix.empty:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        # Use first column as a proxy return series for metric computation
        returns = matrix.iloc[:, 0].pct_change().dropna()
        if returns.empty:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        # Apply cost adjustment
        cost_adjustment = cost_bps / 10000.0
        adjusted_returns = returns - cost_adjustment / 252  # Daily cost

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

    def _compute_baseline_metrics(self, matrix: pd.DataFrame) -> dict[str, Any]:
        """Compute baseline (buy-and-hold) metrics.

        Parameters:
            matrix: Full period feature matrix.

        Returns:
            dict: Baseline performance metrics.
        """
        if matrix.empty or matrix.shape[1] == 0:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        returns = matrix.iloc[:, 0].pct_change().dropna()
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
