"""Walk-forward backtest harness for adaptive_rotation parameter variants.

Lets the compute_coordinator fan out a parameter sweep across the worker fleet.
Each call simulates one parameter set over a historical window, returns metrics,
and persists run + per-rebalance equity points to astrogrid.backtest_run/result.

Tunable params (all optional — fall back to module defaults):
  TREND_WEEKS, VIX_ZSCORE_THRESHOLD, DRAWDOWN_THRESHOLD, DRAWDOWN_WINDOW,
  FAST_RISK_OFF_DURATION, FAST_RISK_OFF_CASH_FLOOR, RANKING_WEEKS,
  ABSOLUTE_STOP, TRAILING_STOP, COOLDOWN_DAYS, MAX_ACTIVE_GROUPS

Returns:
  {
    "config_id": <hash>,
    "window_start": "2023-01-01",
    "window_end": "2026-03-25",
    "rebalance_count": int,
    "total_return": float,
    "sharpe": float,
    "sortino": float,
    "max_drawdown": float,
    "alpha_vs_qqq": float,
    "win_rate_per_rebalance": float,
    "avg_holding_days": float,
    "n_trades": int,
    "config": {...},
    "backtest_run_id": int,
  }
"""
from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import math
import os
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alpha_research.data.panel_builder import VIX_FEATURE_NAME, build_price_panel
from alpha_research.strategies import adaptive_rotation as ar
from store.pit import PITStore, PITWindowCache, PITWindowMiss


# ── Config ─────────────────────────────────────────────────────────────

TUNABLE_FIELDS = (
    "TREND_WEEKS",
    "VIX_ZSCORE_THRESHOLD",
    "DRAWDOWN_THRESHOLD",
    "DRAWDOWN_WINDOW",
    "FAST_RISK_OFF_DURATION",
    "FAST_RISK_OFF_CASH_FLOOR",
    "RANKING_WEEKS",
    "ABSOLUTE_STOP",
    "TRAILING_STOP",
    "COOLDOWN_DAYS",
    "MAX_ACTIVE_GROUPS",
)


@dataclass
class RotationConfig:
    TREND_WEEKS: int = 26
    VIX_ZSCORE_THRESHOLD: float = 3.0
    DRAWDOWN_THRESHOLD: float = -0.03
    DRAWDOWN_WINDOW: int = 3
    FAST_RISK_OFF_DURATION: int = 10
    FAST_RISK_OFF_CASH_FLOOR: float = 0.50
    RANKING_WEEKS: int = 12
    ABSOLUTE_STOP: float = 0.05
    TRAILING_STOP: float = 0.10
    COOLDOWN_DAYS: int = 20
    MAX_ACTIVE_GROUPS: int = 2

    def fingerprint(self) -> str:
        s = json.dumps(asdict(self), sort_keys=True)
        return hashlib.sha256(s.encode()).hexdigest()[:12]


@contextlib.contextmanager
def patched_constants(config: RotationConfig):
    """Temporarily swap adaptive_rotation module globals for the duration of one
    backtest call. Restores originals (and re-raises on error) on exit."""
    saved = {f: getattr(ar, f) for f in TUNABLE_FIELDS}
    try:
        for f in TUNABLE_FIELDS:
            setattr(ar, f, getattr(config, f))
        yield
    finally:
        for f, v in saved.items():
            setattr(ar, f, v)


# ── PIT data window (W2-7, 2026-11-06) ─────────────────────────────────
#
# Each rebalance calls run_rotation, which reads a 1-year price panel and a
# 60-day VIX series. Since build_price_panel moved onto store/pit.py those
# reads were unbounded below (every obs_date since inception, every
# rebalance): 751 s and ~1 GB per job on gridz4 against 157 s before. The
# backtest now reads every vintage of the window it needs once
# (PITWindowCache) and answers all ~117 rebalances from memory, with the
# same PIT filters and vintage pick, or loads that window from a
# hash-receipted snapshot file (GRID_PIT_SNAPSHOT_DIR on the worker).

#: Set to "0" to make every rebalance read the database again.
PIT_CACHE_ENV = "GRID_ROTATION_PIT_CACHE"

#: Price history the backtest reads before window_start (prices_full).
PRICE_LOOKBACK_DAYS = 400


def _local_today() -> date:
    """Today as build_price_panel's ``as_of_date`` default computes it."""
    return date.today()  # noqa: DTZ011 - same clock as panel_builder's default


def rotation_pit_cache_enabled() -> bool:
    return os.environ.get(PIT_CACHE_ENV, "1").strip().lower() not in ("0", "false", "no", "off")


def rotation_universe() -> list[str]:
    """Every ticker run_rotation and the backtest read."""
    tickers: set[str] = set()
    for g in ar.ASSET_GROUPS.values():
        tickers.update(g["tickers"])
    tickers.update(["SPY", "QQQ"])
    tickers.update(ar.FALLBACK_TICKERS)
    return sorted(tickers)


def rotation_feature_names() -> list[str]:
    """feature_registry names behind rotation_universe() plus the VIX feature."""
    return sorted({f"{t.lower()}_full" for t in rotation_universe()} | {VIX_FEATURE_NAME})


def rotation_window_bounds(window_start: date, window_end: date) -> tuple[date, date]:
    """obs_date window a backtest over [window_start, window_end] reads."""
    return window_start - timedelta(days=PRICE_LOOKBACK_DAYS), window_end


def load_rotation_pit_window(
    engine: Engine,
    window_start: date,
    window_end: date,
    data_as_of: date,
    *,
    allow_db_fallback: bool = True,
) -> PITWindowCache:
    """Read the backtest's whole PIT window from the database in two statements."""
    names = rotation_feature_names()
    found = PITStore(engine).feature_ids_by_name(names)
    obs_start, obs_end = rotation_window_bounds(window_start, window_end)
    return PITWindowCache.load(
        engine,
        list(found.values()),
        obs_start,
        obs_end,
        max(data_as_of, window_end),
        feature_names={n: found.get(n) for n in names},
        allow_db_fallback=allow_db_fallback,
    )


def snapshot_covers(cache: PITWindowCache, window_start: date, window_end: date) -> bool:
    """True when *cache* holds every read a backtest over the window makes."""
    obs_start, obs_end = rotation_window_bounds(window_start, window_end)
    if not set(rotation_feature_names()) <= set(cache.feature_names):
        return False
    if cache.start_date is not None and cache.start_date > obs_start:
        return False
    return cache.end_date >= obs_end and cache.max_as_of >= window_end


def build_rotation_snapshot(
    engine: Engine,
    directory: str | Path,
    window_start: date,
    window_end: date,
    data_as_of: date | None = None,
    *,
    built_by: str = "",
) -> Path:
    """Write a snapshot covering backtests inside [window_start, window_end].

    Built once on grid-svr or gridz4 (DB-local), copied to remote workers'
    ``GRID_PIT_SNAPSHOT_DIR``. The file name carries the content sha256.
    """
    as_of = data_as_of or _local_today()
    cache = load_rotation_pit_window(engine, window_start, window_end, as_of, allow_db_fallback=False)
    Path(directory).mkdir(parents=True, exist_ok=True)
    return cache.write_snapshot(directory, built_by=built_by)


def find_rotation_snapshot(
    directory: str | Path,
    window_start: date,
    window_end: date,
    *,
    sha256: str | None = None,
) -> PITWindowCache | None:
    """Newest verified snapshot in *directory* that covers the window, or None.

    With *sha256* only that snapshot is considered. A file whose digest does
    not verify is skipped with a warning, never used.
    """
    root = Path(directory)
    if not root.is_dir():
        log.warning("PIT snapshot dir {d} does not exist", d=root)
        return None
    pattern = f"pit-window-{sha256}.json.gz" if sha256 else "pit-window-*.json.gz"
    best: PITWindowCache | None = None
    for path in sorted(root.glob(pattern)):
        cache = _read_snapshot_cached(path, sha256)
        if cache is None or not snapshot_covers(cache, window_start, window_end):
            continue
        if best is None or cache.max_as_of > best.max_as_of:
            best = cache
    if best is None:
        return None
    # Per-job view: shares the (read-only) arrays, fresh read counters.
    view = copy.copy(best)
    view.reads = 0
    view.db_fallbacks = 0
    return view


#: Parsed snapshots kept for the life of the worker process, keyed by
#: (path, size, mtime): verified once, then reused by every job.
_SNAPSHOT_MEMO: dict[tuple[str, int, int], PITWindowCache | None] = {}


def _read_snapshot_cached(path: Path, sha256: str | None) -> PITWindowCache | None:
    try:
        st = path.stat()
    except OSError:
        return None
    key = (str(path.resolve()), st.st_size, st.st_mtime_ns)
    if key not in _SNAPSHOT_MEMO:
        try:
            cache = PITWindowCache.read_snapshot(path, expected_sha256=sha256)
            if path.name != f"pit-window-{cache.receipt['sha256']}.json.gz":
                raise ValueError("file name does not carry its content sha256")
            _SNAPSHOT_MEMO[key] = cache
        except Exception as exc:  # noqa: BLE001 - a bad file is skipped, not fatal
            log.warning("PIT snapshot {p} rejected: {e}", p=path.name, e=exc)
            _SNAPSHOT_MEMO[key] = None
    return _SNAPSHOT_MEMO[key]


# ── Walk-forward backtest ──────────────────────────────────────────────

def backtest_rotation_variant(
    engine: Engine,
    config: RotationConfig,
    window_start: date,
    window_end: date,
    rebalance_every_days: int = 7,
    benchmark_ticker: str = "QQQ",
    persist: bool = True,
    *,
    pit: PITStore | None = None,
    data_as_of: date | None = None,
) -> dict[str, Any]:
    """Walk through (window_start..window_end), call run_rotation at each rebalance,
    track an equal-weight-by-target paper portfolio, return metrics.

    ``pit``: the PIT reader for every read. ``None`` loads one
    ``PITWindowCache`` for the window from *engine* (unless
    ``GRID_ROTATION_PIT_CACHE=0``); a snapshot-backed cache from
    ``find_rotation_snapshot`` makes the run read no database rows at all.
    ``data_as_of``: the vintage cutoff of the price reads; default today, or
    the snapshot's ``max_as_of`` so a snapshot run equals the DB run made
    the day the snapshot was built.
    """
    config_id = config.fingerprint()
    rebalance_dates = []
    d = window_start
    while d <= window_end:
        # Skip weekends
        if d.weekday() < 5:
            rebalance_dates.append(d)
        d += timedelta(days=rebalance_every_days)

    if len(rebalance_dates) < 4:
        return {"error": f"window too short: only {len(rebalance_dates)} rebalances"}

    data_source: dict[str, Any] = {"mode": "db"}
    if pit is None and rotation_pit_cache_enabled():
        data_as_of = data_as_of or _local_today()
        try:
            pit = load_rotation_pit_window(engine, window_start, window_end, data_as_of)
            data_source = {"mode": "pit_window"}
        except Exception as exc:  # noqa: BLE001 - the uncached path still works
            log.warning(
                "rotation_variant_backtest: PIT window load failed, reading per rebalance: {e}",
                e=exc,
            )
    elif isinstance(pit, PITWindowCache) and pit.receipt:
        data_as_of = data_as_of or pit.max_as_of
        data_source = {"mode": "snapshot", "pit_snapshot_sha256": pit.receipt["sha256"]}
    elif pit is not None:
        data_source = {"mode": "pit_window"}
    if pit is not None:
        data_as_of = data_as_of or _local_today()
    if data_as_of is not None:
        data_source["data_as_of"] = data_as_of.isoformat()

    # Pull the price panel ONCE for the whole window (efficient)
    all_tickers = rotation_universe()

    prices_full = build_price_panel(
        engine,
        tickers=list(all_tickers),
        start_date=window_start - timedelta(days=PRICE_LOOKBACK_DAYS),
        end_date=window_end,
        as_of_date=data_as_of,
        pit=pit,
    ).ffill(limit=5)

    if prices_full.empty or benchmark_ticker not in prices_full.columns:
        return {"error": f"no price panel for window {window_start}..{window_end}"}

    # Run rotation at each rebalance date with the patched constants
    daily_returns: list[float] = []
    bench_returns: list[float] = []
    n_trades = 0
    last_weights: dict[str, float] = {}
    holding_days: list[int] = []
    days_held: dict[str, int] = {}

    with patched_constants(config):
        for i, asof in enumerate(rebalance_dates):
            try:
                if pit is None and data_as_of is None:
                    result = ar.run_rotation(engine, as_of_date=asof, positions={})
                else:
                    result = ar.run_rotation(
                        engine, as_of_date=asof, positions={}, pit=pit, data_as_of=data_as_of,
                    )
            except PITWindowMiss:
                # A snapshot that cannot answer a read must not silently
                # drop rebalances: fail the job instead.
                raise
            except Exception as exc:
                log.warning(
                    "rotation_variant_backtest: run_rotation failed asof={asof}: {err}",
                    asof=asof,
                    err=str(exc),
                )
                continue
            new_weights = dict(result.weights)

            # Compute return between asof and next rebalance using the panel
            if i + 1 < len(rebalance_dates):
                nxt = rebalance_dates[i + 1]
            else:
                nxt = window_end

            seg = prices_full.loc[
                (prices_full.index.date >= asof) & (prices_full.index.date <= nxt)
            ]
            if seg.empty or len(seg) < 2:
                continue

            # Per-ticker segment return; clip absurd values (delisting/data gaps)
            first = seg.iloc[0]
            last = seg.iloc[-1]
            seg_ret = (last / first) - 1.0
            # Drop NaN, clip to sane bounds so one bad data point doesn't blow up cum_return
            seg_ret = seg_ret.dropna().clip(lower=-0.5, upper=0.5)
            port_ret = sum(new_weights.get(t, 0.0) * float(seg_ret.get(t, 0.0)) for t in new_weights)
            if not math.isfinite(port_ret) or abs(port_ret) > 0.5:
                # Catch all — skip this segment entirely if math went sideways
                continue
            cash_ret = 0.0  # cash earns nothing in this sim
            cash_w = max(0.0, 1.0 - sum(new_weights.values()))
            port_ret = port_ret + cash_w * cash_ret
            bench_ret = float(seg_ret.get(benchmark_ticker, 0.0))

            daily_returns.append(port_ret)
            bench_returns.append(bench_ret)

            # Trade accounting
            for t in set(new_weights) | set(last_weights):
                old = last_weights.get(t, 0.0)
                new = new_weights.get(t, 0.0)
                if abs(new - old) > 1e-4:
                    n_trades += 1
                    if old > 0 and new == 0:
                        holding_days.append(days_held.pop(t, 0))
            for t in new_weights:
                if t in days_held:
                    days_held[t] += rebalance_every_days
                elif new_weights[t] > 0:
                    days_held[t] = rebalance_every_days

            last_weights = new_weights

    # Close out any remaining positions
    for t, d_held in days_held.items():
        holding_days.append(d_held)

    if not daily_returns:
        return {"error": "no rebalance segments produced returns"}

    arr = np.array(daily_returns)
    bench_arr = np.array(bench_returns)
    excess = arr - bench_arr

    # Annualization: rebalance_every_days segments per year
    seg_per_year = 365.0 / rebalance_every_days
    cum_return = float(np.prod(1 + arr) - 1)
    bench_cum = float(np.prod(1 + bench_arr) - 1)
    ann_return = (1 + cum_return) ** (seg_per_year / len(arr)) - 1 if len(arr) else 0.0
    sharpe = float(np.mean(arr) / (np.std(arr) + 1e-9) * np.sqrt(seg_per_year)) if len(arr) > 1 else 0.0
    downside = arr[arr < 0]
    sortino = float(np.mean(arr) / (np.std(downside) + 1e-9) * np.sqrt(seg_per_year)) if len(downside) > 1 else 0.0
    equity = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(equity)
    drawdown = (equity - peak) / peak
    max_dd = float(np.min(drawdown)) if len(drawdown) else 0.0
    alpha_vs_bench = cum_return - bench_cum
    win_rate = float((arr > bench_arr).mean()) if len(arr) else 0.0
    avg_hold = float(np.mean(holding_days)) if holding_days else 0.0

    metrics = {
        "config_id": config_id,
        "config": asdict(config),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "rebalance_count": len(daily_returns),
        "total_return": cum_return,
        "annualized_return": ann_return,
        "benchmark_return": bench_cum,
        "alpha_vs_benchmark": alpha_vs_bench,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_dd,
        "win_rate_per_rebalance": win_rate,
        "avg_holding_days": avg_hold,
        "n_trades": n_trades,
    }
    if isinstance(pit, PITWindowCache):
        data_source["pit_reads"] = pit.reads
        data_source["db_fallbacks"] = pit.db_fallbacks
    metrics["data_source"] = data_source

    if persist:
        try:
            metrics["backtest_run_id"] = _persist_run(engine, config, window_start, window_end, metrics)
        except Exception as exc:
            metrics["persist_error"] = f"{type(exc).__name__}: {exc}"

    return metrics


def _persist_run(engine: Engine, config: RotationConfig, ws: date, we: date, m: dict) -> int:
    """Insert a row into astrogrid.backtest_run + a summary row in backtest_result."""
    run_key = f"rotation_variant_{config.fingerprint()}_{ws}_{we}"
    payload = {
        "config": asdict(config),
        "summary": {k: v for k, v in m.items() if k != "config"},
    }
    with engine.begin() as conn:
        rid = conn.execute(
            text(
                """
                INSERT INTO astrogrid.backtest_run
                  (run_key, strategy_variant, horizon_label, target_universe,
                   started_at, completed_at, status, window_start, window_end, params_payload)
                VALUES
                  (:run_key, 'grid_only', 'swing', 'asset_groups',
                   now(), now(), 'completed', :ws, :we, cast(:payload as jsonb))
                ON CONFLICT (run_key) DO UPDATE SET
                  completed_at = now(), status = 'completed',
                  params_payload = EXCLUDED.params_payload
                RETURNING id
                """
            ),
            {
                "run_key": run_key,
                "ws": ws,
                "we": we,
                "payload": json.dumps(payload),
            },
        ).scalar_one()

        conn.execute(
            text(
                """
                INSERT INTO astrogrid.backtest_result
                  (backtest_run_id, result_key, strategy_variant, target_symbol,
                   as_of_date, alpha_vs_benchmark, metrics_payload)
                VALUES
                  (:rid, 'summary', 'grid_only', NULL, :we, :alpha, cast(:metrics as jsonb))
                ON CONFLICT (backtest_run_id, result_key) DO UPDATE SET
                  alpha_vs_benchmark = EXCLUDED.alpha_vs_benchmark,
                  metrics_payload = EXCLUDED.metrics_payload
                """
            ),
            {
                "rid": rid,
                "we": we,
                "alpha": m["alpha_vs_benchmark"],
                "metrics": json.dumps(m),
            },
        )
    return rid


# ── CLI for ad-hoc testing ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default="2026-03-25")
    parser.add_argument("--trend-weeks", type=int, default=26)
    parser.add_argument("--ranking-weeks", type=int, default=12)
    parser.add_argument("--vix-z", type=float, default=3.0)
    parser.add_argument("--cash-floor", type=float, default=0.50)
    parser.add_argument(
        "--build-snapshot", metavar="DIR",
        help="write a PIT snapshot covering --start..--end into DIR and exit",
    )
    parser.add_argument("--data-as-of", default=None, help="vintage cutoff for --build-snapshot (default today)")
    args = parser.parse_args()

    from db import get_engine
    eng = get_engine()
    if args.build_snapshot:
        import socket

        out = build_rotation_snapshot(
            eng, args.build_snapshot,
            date.fromisoformat(args.start), date.fromisoformat(args.end),
            date.fromisoformat(args.data_as_of) if args.data_as_of else None,
            built_by=socket.gethostname(),
        )
        print(out)
        raise SystemExit(0)
    cfg = RotationConfig(
        TREND_WEEKS=args.trend_weeks,
        RANKING_WEEKS=args.ranking_weeks,
        VIX_ZSCORE_THRESHOLD=args.vix_z,
        FAST_RISK_OFF_CASH_FLOOR=args.cash_floor,
    )
    res = backtest_rotation_variant(
        eng, cfg,
        date.fromisoformat(args.start),
        date.fromisoformat(args.end),
    )
    print(json.dumps(res, indent=2, default=str))
