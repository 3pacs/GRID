#!/usr/bin/env python3
"""The one registered look at the high-Sharpe ensemble (paper only, read-only database).

Loads the price panel and VIX for ``trading.high_sharpe_ensemble.UNIVERSE`` through
GRID's point-in-time readers, runs the walk-forward, and writes the report JSON and
Markdown under ``docs/reports/``. It writes nothing to the database: the engine is
created with ``default_transaction_read_only=on``.

Usage (on grid-svr, release tree on PYTHONPATH, service .env loaded):

    python -m scripts.high_sharpe_ensemble_backtest --set v2 --start 2016-01-04 --end 2026-11-06 \\
        --source raw-adj-close --out docs/reports

``--source`` picks the registered price basis: ``resolved`` reads ``{ticker}_full``
from ``resolved_series`` through ``PITStore.get_feature_matrix`` (latest vintage as
of the run date, then the universal split adjuster); ``raw-adj-close`` reads
``YF:{TICKER}:adj_close`` from ``raw_series`` through ``store.observations.read_window``
(SUCCESS rows, latest pull per date, bounded by ``as_of``). The registration names
which one the look uses and why.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

os.environ.setdefault("GRID_PIT_MAX_YEARS", "12")

import pandas as pd  # noqa: E402
from loguru import logger as log  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402

from alpha_research.data.panel_builder import get_vix_series  # noqa: E402
from alpha_research.data.split_adjuster import adjust_panel  # noqa: E402
from config import settings  # noqa: E402
from store import observations as obs  # noqa: E402
from store.pit import PITStore  # noqa: E402
from trading import high_sharpe_ensemble as hse  # noqa: E402


def read_only_engine(timeout_s: int = 120) -> Engine:
    options = f"-c default_transaction_read_only=on -c statement_timeout={timeout_s * 1000}"
    return create_engine(settings.DB_URL, connect_args={"options": options}, pool_pre_ping=True)


def load_resolved_panel(engine: Engine, tickers: list[str], start: date, end: date, as_of: date) -> tuple[pd.DataFrame, dict[str, Any]]:
    pit = PITStore(engine)
    names = [f"{t.lower()}_full" for t in tickers]
    ids = pit.feature_ids_by_name(names)
    id_to_ticker = {fid: name.replace("_full", "").upper() for name, fid in ids.items()}
    matrix = pit.get_feature_matrix(list(ids.values()), start, end, as_of, vintage_policy="LATEST_AS_OF")
    panel = matrix.rename(columns=id_to_ticker).sort_index()
    panel.index = pd.to_datetime(panel.index)
    panel = adjust_panel(panel)
    missing = sorted(set(tickers) - set(panel.columns))
    return panel, {"source": "resolved_series {ticker}_full, LATEST_AS_OF, split-adjusted", "missing": missing}


TIINGO_COVERAGE = 0.95     # use tiingo when it holds >= this share of SPY's observation dates
MIN_COVERAGE = 0.90        # exclude a ticker below this share of SPY's observation dates
MAX_REVERSED_JUMPS = 25    # exclude a ticker with more reversing >10 % jumps than this (junk ≈ 105, TSLA 16)
MAX_STALE_SESSIONS = 10    # exclude a ticker whose last observation is older than this


def reversed_jumps(s: pd.Series) -> int:
    """Day-to-day moves above 10 % followed within five sessions by an opposite move above 8 %."""
    import numpy as np

    r = s.pct_change().to_numpy(dtype=float)
    ok = np.isfinite(r)
    big = ok & (np.abs(r) > 0.10)
    count = 0
    for i in np.flatnonzero(big):
        nxt = r[i + 1 : i + 6]
        if len(nxt) and np.any(np.isfinite(nxt) & (np.sign(nxt) == -np.sign(r[i])) & (np.abs(nxt) > 0.08)):
            count += 1
    return int(count)


def screen_series(s: pd.Series, spy_n: int, end: date) -> list[str]:
    """Registration §3 exclusion rule; returns the reasons a ticker is excluded (empty = keep)."""
    reasons = []
    if len(s) < MIN_COVERAGE * spy_n:
        reasons.append(f"coverage {len(s)}/{spy_n}")
    if any(d.weekday() >= 5 for d in s.index):
        reasons.append("weekend observation dates")
    rj = reversed_jumps(s)
    if rj > MAX_REVERSED_JUMPS:
        reasons.append(f"{rj} reversing jumps")
    if len(s) and (pd.Timestamp(end) - s.index.max()).days > MAX_STALE_SESSIONS * 7 // 5 + 3:
        reasons.append(f"stale since {s.index.max().date()}")
    return reasons


def _read_series(conn: Any, series_id: str, source: str, start: date, as_of: date) -> pd.Series:
    rows = obs.read_window(conn, series_id, source=source, start=start, as_of=as_of)
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series({pd.Timestamp(r.obs_date): float(r.value) for r in rows}).sort_index()


def load_raw_adj_close_panel(
    engine: Engine, tickers: list[str], start: date, end: date, as_of: date
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Registration §3: ``YF:{T}:adj_close``, Tiingo when it covers SPY's dates, else yfinance; then the screen."""
    series: dict[str, pd.Series] = {}
    chosen: dict[str, str] = {}
    excluded: dict[str, list[str]] = {}
    with engine.connect() as conn:
        spy = _read_series(conn, "YF:SPY:adj_close", "tiingo", start, as_of)
        if spy.empty:
            spy = _read_series(conn, "YF:SPY:adj_close", "yfinance", start, as_of)
        spy_n = int(len(spy.loc[: str(end)]))
        for t in tickers:
            tiingo = _read_series(conn, f"YF:{t}:adj_close", "tiingo", start, as_of).loc[: str(end)]
            if len(tiingo) >= TIINGO_COVERAGE * spy_n:
                s, src = tiingo, "tiingo"
            else:
                yf = _read_series(conn, f"YF:{t}:adj_close", "yfinance", start, as_of).loc[: str(end)]
                s, src = (yf, "yfinance") if len(yf) >= len(tiingo) else (tiingo, "tiingo")
            if s.empty:
                excluded[t] = ["no adj_close series"]
                continue
            reasons = screen_series(s, spy_n, end)
            if reasons:
                excluded[t] = reasons + [f"source {src}"]
                continue
            series[t], chosen[t] = s, src
    panel = pd.DataFrame(series).sort_index()
    return panel, {
        "source": "raw_series YF:{TICKER}:adj_close via store.observations.read_window (SUCCESS, latest pull)",
        "source_per_ticker": chosen,
        "excluded": excluded,
        "missing": sorted(excluded),
        "spy_observations": spy_n,
        "screen": {
            "tiingo_coverage": TIINGO_COVERAGE, "min_coverage": MIN_COVERAGE,
            "max_reversed_jumps": MAX_REVERSED_JUMPS, "max_stale_sessions": MAX_STALE_SESSIONS,
        },
    }


def load_vix(engine: Engine, start: date, end: date, as_of: date) -> pd.Series:
    vix = get_vix_series(engine, start_date=start, end_date=end, as_of_date=as_of)
    vix.index = pd.to_datetime(vix.index)
    return vix.sort_index()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    ap.add_argument("--end", type=date.fromisoformat, default=date(2026, 11, 6))
    ap.add_argument("--as-of", type=date.fromisoformat, default=None, help="vintage date (default: today)")
    ap.add_argument("--source", choices=("resolved", "raw-adj-close"), default="raw-adj-close")
    ap.add_argument("--out", default="docs/reports")
    ap.add_argument("--set", dest="set_name", choices=("v1", "v2", "v3", "v4"), default="v4",
                    help="registered candidate set; fixes universe, candidates, trial count and criterion")
    ap.add_argument("--tag", default=None, help="report file prefix (default high-sharpe-ensemble-<set>)")
    ap.add_argument("--lookback-days", type=int, default=400, help="price history loaded before --start")
    ap.add_argument("--secondary-cost-bps", type=float, default=None,
                    help="also run the selector at this cost per side and attach it as a descriptive secondary (v4: 10)")
    args = ap.parse_args(argv)

    as_of = args.as_of or date.today()
    cset = hse.candidate_set(args.set_name)
    tag = args.tag or f"high-sharpe-ensemble-{cset.name}"
    tickers = list(cset.universe)
    engine = read_only_engine()
    load_start = args.start - pd.Timedelta(days=args.lookback_days).to_pytimedelta()
    log.info("loading {n} tickers {s}..{e} as of {a} from {src}", n=len(tickers), s=load_start, e=args.end, a=as_of, src=args.source)
    if args.source == "resolved":
        panel, provenance = load_resolved_panel(engine, tickers, load_start, args.end, as_of)
    else:
        panel, provenance = load_raw_adj_close_panel(engine, tickers, load_start, args.end, as_of)
    vix = load_vix(engine, load_start, args.end, as_of)
    panel = panel.ffill(limit=5)
    panel = panel.dropna(how="all")
    log.info("panel {shape}, vix {n}; excluded {m}", shape=panel.shape, n=len(vix), m=provenance.get("excluded") or provenance["missing"])

    pit = hse.PanelPITStore(panel, vix, data_as_of=as_of)
    config = cset.config()
    run = hse.walk_forward(panel, vix, args.start, args.end, config, cset.candidates(), engine=None, pit=pit)
    report = hse.evaluate(run)
    if args.secondary_cost_bps is not None:
        run2 = hse.walk_forward(panel, vix, args.start, args.end, cset.config(cost_bps_per_side=args.secondary_cost_bps),
                                cset.candidates(), engine=None, pit=pit)
        rep2 = hse.evaluate(run2)
        report["secondary_cost_run"] = {k: rep2[k] for k in ("config", "ensemble", "spy_buy_and_hold", "equal_weight_all_candidates",
                                        "best_single_in_hindsight", "candidates", "excess_vs_spy_bootstrap",
                                        "deflated_sharpe", "pbo_candidates", "watch_criterion", "pass_criterion")}
    report["provenance"] = {
        **provenance,
        "as_of": as_of.isoformat(),
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "panel_shape": list(panel.shape),
        "panel_first": panel.index.min().date().isoformat(),
        "panel_last": panel.index.max().date().isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidate_set": cset.name,
        "registration": hse.REGISTRATIONS[cset.name][0],
        "registration_sha256_pinned": hse.REGISTRATIONS[cset.name][1],
        "registration_sha256_observed": hse.registration_sha256(_GRID_DIR, cset.name),
        "n_trials_cumulative": cset.n_trials_cumulative,
        "pit_reads": pit.reads,
        "host": os.uname().nodename,
    }
    report["series"] = {
        "ensemble_net": hse.frame_to_records(run.ensemble_net.to_frame("ensemble_net")),
        "benchmark_net": hse.frame_to_records(run.benchmark_net.to_frame("spy_net")),
        "equal_weight_net": hse.frame_to_records(run.equal_weight_net.to_frame("ew_net")),
        "candidate_net": hse.frame_to_records(run.candidate_net),
        "ensemble_allocations": hse.frame_to_records(run.ensemble_weights),
    }
    report["selections"] = run.selections

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ")
    json_path = out_dir / f"{tag}-look-{stamp}.json"
    md_path = out_dir / f"{tag}-look-{stamp}.md"
    json_path.write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
    md_path.write_text(
        hse.report_markdown(
            report,
            title=f"High-Sharpe ensemble {cset.name} — registered look {stamp}",
            data_basis=f"Prices: {provenance['source']}; vintages as of {as_of}; missing {provenance['missing'] or 'none'}.",
        ),
        encoding="utf-8",
    )
    log.info("wrote {j} and {m}; verdict {v}", j=json_path, m=md_path, v=report["pass_criterion"]["verdict"])
    print(md_path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
