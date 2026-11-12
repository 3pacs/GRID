#!/usr/bin/env python3
"""The one registered look at the regime-conditioned ensemble (set v6). Paper only, read-only DB.

Two walk-forwards share one external-factor regime labeller
(:mod:`trading.regime_ensemble`): the ETF era (weekly, Tiingo adj_close, the 27 weekly arms of
sets v1–v3) and the long sample (monthly, Kenneth French portfolios 1926–2026 plus a bond index
from Treasury yields and WTI). Both select regime-conditionally. The report classifies today's
regime, names the analog windows, and gives every arm's record (a) in today's cell within the
ETF era, (b) in the analog cells and windows of the long sample, (c) overall.

    python -m scripts.regime_ensemble_look --start 2016-01-04 --end 2026-11-06 --out docs/reports \\
        --cache-dir outputs/long_sample_cache
"""

from __future__ import annotations

import argparse
import json
import math
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

from alpha_research.data import long_sample as ls  # noqa: E402
from scripts.high_sharpe_ensemble_backtest import load_raw_adj_close_panel, load_vix, read_only_engine  # noqa: E402
from store import observations as obs  # noqa: E402
from trading import high_sharpe_ensemble as hse  # noqa: E402
from trading import regime_ensemble as rg  # noqa: E402


def build_regime(cache: ls.DownloadCache) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, pd.DataFrame]:
    fred = ls.load_fred_long(cache)
    gpr = ls.load_gpr(cache)
    inp = rg.RegimeInputs(cpi=fred["CPIAUCNS"], indpro=fred["INDPRO"], baa=fred["BAA"], aaa=fred["AAA"],
                          wti=fred["WTISPLC"], gpr=gpr["GPRH"])
    factors = rg.regime_factors(inp)
    cells = rg.label_cells(factors)
    return fred, factors, cells, rg.cells_known_at(cells), gpr


def long_sample_panel(cache: ls.DownloadCache, fred: pd.DataFrame) -> pd.DataFrame:
    french = ls.load_french(cache)
    panel = ls.french_price_panel(french)
    # bond index: long-term government yield 1925–1953-03, then GS10, spliced by chaining returns
    lt = rg.bond_index_from_yields(fred["LTGOVTBD"], duration=10.0)
    gs = rg.bond_index_from_yields(fred["GS10"], duration=7.0)
    lt_ret = lt.pct_change()
    gs_ret = gs.pct_change()
    bond_ret = pd.concat([lt_ret.loc[: gs_ret.dropna().index[0] - pd.Timedelta(days=1)], gs_ret.dropna()]).sort_index()
    bond = (1.0 + bond_ret.fillna(0.0)).cumprod()
    bond.index = bond.index + pd.offsets.MonthEnd(0)
    oil = fred["WTISPLC"].dropna().copy()
    oil.index = oil.index + pd.offsets.MonthEnd(0)
    panel["BOND"] = bond.reindex(panel.index)
    panel["OIL"] = oil.reindex(panel.index)
    return panel


def db_cross_check(engine: Any, fred: pd.DataFrame, as_of: date) -> dict[str, Any]:
    """Compare the downloaded INDPRO/FEDFUNDS with griddb's FRED rows (1990+) — same vintage family?"""
    out: dict[str, Any] = {}
    with engine.connect() as conn:
        for sid in ("INDPRO", "FEDFUNDS"):
            rows = obs.read_window(conn, sid, source="FRED", start=date(1990, 1, 1), as_of=as_of)
            db = pd.Series({pd.Timestamp(r.obs_date): float(r.value) for r in rows}).sort_index()
            dl = fred[sid].reindex(db.index)
            diff = (dl - db).abs()
            out[sid] = {"n_db": int(len(db)), "n_compared": int(diff.notna().sum()),
                        "max_abs_diff": float(diff.max()) if diff.notna().any() else None,
                        "share_within_0_1pct": float(((diff / db.abs()).fillna(1) <= 0.001).mean()) if len(db) else None}
    return out


def window_slice(series: pd.Series, start: str, end: str) -> pd.Series:
    return series.loc[pd.Timestamp(start): pd.Timestamp(end)].dropna()


def ann(r: pd.Series, ppy: int) -> dict[str, Any]:
    r = r.dropna()
    if len(r) < 2:
        return {"n": int(len(r))}
    sd = float(r.std(ddof=1))
    return {"n": int(len(r)), "annualised_return": float((1 + r).prod() ** (ppy / len(r)) - 1),
            "sharpe": float(r.mean() / sd * math.sqrt(ppy)) if sd > 0 else None,
            "max_drawdown": float(hse.max_drawdown(r))}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    ap.add_argument("--end", type=date.fromisoformat, default=date(2026, 11, 6))
    ap.add_argument("--as-of", type=date.fromisoformat, default=None)
    ap.add_argument("--ls-start", type=date.fromisoformat, default=date(1931, 1, 31), help="long-sample walk-forward start")
    ap.add_argument("--out", default="docs/reports")
    ap.add_argument("--cache-dir", default="outputs/long_sample_cache")
    ap.add_argument("--lookback-days", type=int, default=400)
    ap.add_argument("--long-sample-only", action="store_true",
                    help="skip the ETF era; write a correction record of the long-sample part only")
    ap.add_argument("--correction-of", default=None, help="stamp of the look record this run corrects")
    args = ap.parse_args(argv)

    as_of = args.as_of or date.today()
    cset = hse.candidate_set("v6")
    cache = ls.DownloadCache(args.cache_dir)
    fred, factors, cells, cells_known, gpr = build_regime(cache)
    current = rg.current_regime(factors, cells, as_of)
    log.info("regime labels {n} months; current cell {c}", n=int(cells.notna().sum()), c=current.get("cell"))

    engine = read_only_engine()
    cross = db_cross_check(engine, fred, as_of)
    if args.long_sample_only:
        panel, provenance = pd.DataFrame(), {"source": "ETF era skipped (long-sample correction run)", "excluded": {}}
        rep_rc = rep_uc = None
    else:
        load_start = args.start - pd.Timedelta(days=args.lookback_days).to_pytimedelta()
        panel, provenance = load_raw_adj_close_panel(engine, list(cset.universe), load_start, args.end, as_of)
        vix = load_vix(engine, load_start, args.end, as_of)
        panel = panel.ffill(limit=5).dropna(how="all")
        # ── ETF era: regime-conditional selection over the 27 weekly arms, plus the unconditional control
        pit = hse.PanelPITStore(panel, vix, data_as_of=as_of)
        cfg = cset.config()
        run_rc = hse.walk_forward(panel, vix, args.start, args.end, cfg, cset.candidates(), pit=pit, regime=cells_known)
        rep_rc = hse.evaluate(run_rc)
        pit2 = hse.PanelPITStore(panel, vix, data_as_of=as_of)
        run_uc = hse.walk_forward(panel, vix, args.start, args.end, cfg, cset.candidates(), pit=pit2)
        rep_uc = hse.evaluate(run_uc)

    # ── Long sample: monthly French panel, same cells, regime-conditional selection
    lpanel = long_sample_panel(cache, fred)
    lvix = pd.Series(20.0, index=lpanel.index)  # no arm reads VIX here; the engine wants a series
    lcfg = hse.EnsembleConfig(n_trials_declared=cset.n_trials_cumulative, criterion="watch", **rg.LS_CONFIG)
    lpit = hse.PanelPITStore(lpanel, lvix, data_as_of=as_of)
    run_ls = hse.walk_forward(lpanel, lvix, args.ls_start, lpanel.index.max().date(), lcfg,
                              rg.long_sample_candidates(), pit=lpit, regime=cells_known)
    rep_ls = hse.evaluate(run_ls)

    # analog windows: panel columns, cells, and the long-sample ensemble vs market inside each window
    analogs: dict[str, Any] = {}
    cells_me = cells.copy()
    cells_me.index = cells_me.index + pd.offsets.MonthEnd(0)
    for name, (a, b, role) in rg.ANALOG_WINDOWS.items():
        w = rg.window_report(lpanel, cells_me, a, b)
        w["role"] = role
        ens = window_slice(run_ls.ensemble_net, a, b)
        mkt = window_slice(run_ls.benchmark_net, a, b)
        w["long_sample_ensemble"] = ann(ens, 12)
        w["market_same_months"] = ann(mkt, 12)
        if len(ens) >= 2 and len(mkt) >= 2:
            ex = (ens - mkt).dropna()
            w["ensemble_minus_market_mean_monthly"] = float(ex.mean()) if len(ex) else None
        w["arms_in_window"] = {c: ann(window_slice(run_ls.candidate_net[c], a, b), 12) for c in run_ls.candidate_net.columns}
        analogs[name] = w

    # ── verdict: watch criterion inside today's cell (ETF era) and no contradiction in the long sample
    cell = current.get("cell")
    etf_cell = rep_rc["by_regime_cell"].get(str(cell), {}) if (cell and rep_rc) else {}
    ls_cell = rep_ls["by_regime_cell"].get(str(cell), {}) if cell else {}
    e_c, s_c = etf_cell.get("ensemble", {}), etf_cell.get("spy", {})
    boot_c = etf_cell.get("excess_vs_spy_bootstrap", {})
    parts = {
        "cell": cell,
        "etf_cell_n_segments": etf_cell.get("n_segments"),
        "sharpe_beats_spy_in_cell": bool(e_c.get("sharpe_annualised", -9) > s_c.get("sharpe_annualised", 9)),
        "excess_lb_positive_in_cell": bool((boot_c.get("lower_bound") or -1) > 0),
        "dsr_above_half_overall": bool(rep_rc is not None and (rep_rc["deflated_sharpe"].get("dsr") or 0) > 0.5),
        "drawdown_no_worse_in_cell": bool(e_c.get("max_drawdown", -9) >= s_c.get("max_drawdown", 9)),
        "long_sample_cell_n_months": ls_cell.get("n_segments"),
        "long_sample_cell_excess_mean_nonneg": bool((ls_cell.get("excess_vs_spy_bootstrap", {}).get("mean") or -1) >= 0),
        "analog_1946_48_excess_nonneg": bool((analogs["1946-48 post-war disinflation under heavy debt"].get("ensemble_minus_market_mean_monthly") or -1) >= 0),
    }
    parts["etf_cell_pass"] = all(parts[k] for k in ("sharpe_beats_spy_in_cell", "excess_lb_positive_in_cell", "dsr_above_half_overall", "drawdown_no_worse_in_cell"))
    parts["long_sample_not_contradicting"] = parts["long_sample_cell_excess_mean_nonneg"] and parts["analog_1946_48_excess_nonneg"]
    parts["verdict"] = "PASS" if parts["etf_cell_pass"] and parts["long_sample_not_contradicting"] else "FAIL"
    if args.long_sample_only:
        parts["verdict"] = f"CORRECTION of {args.correction_of}: long-sample part only; the verdict stays with the original record"
    thin = {k: v.get("n_segments") for k, v in rep_rc["by_regime_cell"].items() if isinstance(v, dict) and (v.get("n_segments") or 0) < 52} if rep_rc else {}

    report: dict[str, Any] = {
        "version": hse.ENSEMBLE_VERSION, "candidate_set": "v6", "n_trials_cumulative": cset.n_trials_cumulative,
        "current_regime": current,
        "regime_cell_counts_all_history": {str(k): int(v) for k, v in cells.value_counts().items()},
        "regime_cells_recent": {str(k.date()): v for k, v in cells.loc["2024-01-01":].dropna().items()},
        "verdict": parts,
        "thin_etf_cells_under_52_segments": thin,
        "etf_regime_conditional": {k: rep_rc[k] for k in ("ensemble", "spy_buy_and_hold", "equal_weight_all_candidates", "best_single_in_hindsight", "excess_vs_spy_bootstrap", "deflated_sharpe", "pbo_candidates", "watch_criterion", "by_regime_cell", "candidates", "selection_counts", "ensemble_turnover_annualised", "oos_period", "faults", "config")} if rep_rc else None,
        "etf_unconditional_control": {k: rep_uc[k] for k in ("ensemble", "excess_vs_spy_bootstrap", "watch_criterion", "selection_counts")} if rep_uc else None,
        "correction_of": args.correction_of,
        "long_sample_regime_conditional": {k: rep_ls[k] for k in ("ensemble", "spy_buy_and_hold", "equal_weight_all_candidates", "best_single_in_hindsight", "excess_vs_spy_bootstrap", "deflated_sharpe", "pbo_candidates", "by_regime_cell", "candidates", "selection_counts", "oos_period", "faults", "config")},
        "analog_windows": analogs,
        "selections_etf": run_rc.selections if rep_rc else None,
        "selections_long_sample": run_ls.selections,
        "provenance": {
            **provenance, "as_of": as_of.isoformat(), "start": args.start.isoformat(), "end": args.end.isoformat(),
            "panel_shape": list(panel.shape), "long_sample_panel_shape": list(lpanel.shape),
            "long_sample_benchmark": lcfg.benchmark,
            "long_sample_first": lpanel.index.min().date().isoformat(), "long_sample_last": lpanel.index.max().date().isoformat(),
            "downloads": cache.receipt(), "publication_lag_days": ls.PUBLICATION_LAG_DAYS,
            "db_cross_check_1990_plus": cross,
            "registration": hse.REGISTRATIONS["v6"][0], "registration_sha256_pinned": hse.REGISTRATIONS["v6"][1],
            "registration_sha256_observed": hse.registration_sha256(_GRID_DIR, "v6"),
            "generated_at": datetime.now(timezone.utc).isoformat(), "host": os.uname().nodename,
        },
    }
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ")
    kind = "correction" if args.long_sample_only else "look"
    json_path = out_dir / f"high-sharpe-ensemble-v6-{kind}-{stamp}.json"
    md_path = out_dir / f"high-sharpe-ensemble-v6-{kind}-{stamp}.md"
    json_path.write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
    md_path.write_text(render(report), encoding="utf-8")
    log.info("wrote {j}; verdict {v}", j=json_path, v=parts["verdict"])
    print(md_path.read_text(encoding="utf-8"))
    return 0


def _f(v: Any, nd: int = 3) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    return "—" if not math.isfinite(x) else f"{x:.{nd}f}"


def render(rep: dict[str, Any]) -> str:
    cur, v = rep["current_regime"], rep["verdict"]
    rc, uc, lsr = rep["etf_regime_conditional"], rep["etf_unconditional_control"], rep["long_sample_regime_conditional"]
    if rc is None:
        rc = {"ensemble": {}, "spy_buy_and_hold": {}, "equal_weight_all_candidates": {}, "best_single_in_hindsight": {}, "excess_vs_spy_bootstrap": {},
              "deflated_sharpe": {}, "pbo_candidates": {}, "by_regime_cell": {}, "candidates": {}, "selection_counts": {}}
        uc = {"ensemble": {}, "excess_vs_spy_bootstrap": {}}
    lines = [
        f"# High-Sharpe ensemble v6 — regime-conditional, registered look ({rep['provenance']['generated_at'][:16]}Z)",
        "",
        f"Verdict: **{v['verdict']}** (cumulative trials {rep['n_trials_cumulative']}). Today's cell: **{cur.get('cell')}** "
        f"(observation month {cur.get('observation_month')}, as known {cur.get('as_of')}): CPI y/y {_f(cur.get('cpi_yoy'))} vs "
        f"{_f(cur.get('cpi_yoy_12m_ago'))} a year earlier, INDPRO y/y {_f(cur.get('indpro_yoy'))}, Baa−Aaa {_f(cur.get('baa_aaa'),2)} "
        f"(stress threshold {_f(cur.get('stress_threshold'),2)}), WTI 6-month log change {_f(cur.get('oil_6m_log'))}, "
        f"GPR {_f(cur.get('gpr'),0)} (10-yr 90th pct {_f(cur.get('gpr_threshold'),0)}).",
        "",
        "Verdict parts: " + ", ".join(f"{k}={val}" for k, val in v.items() if k not in ("cell", "verdict")),
        "",
        "## ETF era (weekly, 27 arms, regime-conditional top-3)",
        "",
        "| series | Sharpe | ann. return | max DD | excess LB |",
        "|---|---|---|---|---|",
    ]
    for label, s, b in (("regime-conditional ensemble", rc["ensemble"], rc["excess_vs_spy_bootstrap"]),
                        ("unconditional selector (control)", uc["ensemble"], uc["excess_vs_spy_bootstrap"]),
                        ("SPY buy-and-hold", rc["spy_buy_and_hold"], {}),
                        ("1/N over 27 arms", rc["equal_weight_all_candidates"], {}),
                        (f"best arm in hindsight ({rc['best_single_in_hindsight'].get('name')})", rc["best_single_in_hindsight"], {})):
        lines.append(f"| {label} | {_f(s.get('sharpe_annualised'))} | {_f(s.get('annualised_return'))} | {_f(s.get('max_drawdown'))} | {_f(b.get('lower_bound'),5)} |")
    lines += ["", f"DSR {_f(rc['deflated_sharpe'].get('dsr'))} at n_trials {rc['deflated_sharpe'].get('n_trials')}; PBO {_f(rc['pbo_candidates'].get('pbo'))}; "
              f"selection fallbacks to unconditional: {_f(rc['by_regime_cell'].get('_selection_fallback_share'))} of selections.", "",
              "### ETF era by regime cell (ensemble vs SPY; cells under 52 segments are too thin to read)", "",
              "| cell | n weeks | share | ens Sharpe | SPY Sharpe | ens ann | SPY ann | ens DD | SPY DD | excess LB |", "|---|---|---|---|---|---|---|---|---|---|"]
    for cell, d in rc["by_regime_cell"].items():
        if not isinstance(d, dict):
            continue
        e, s, b = d["ensemble"], d["spy"], d.get("excess_vs_spy_bootstrap", {})
        lines.append(f"| {cell} | {d['n_segments']} | {_f(d.get('share'),2)} | {_f(e.get('sharpe_annualised'))} | {_f(s.get('sharpe_annualised'))} | "
                     f"{_f(e.get('annualised_return'))} | {_f(s.get('annualised_return'))} | {_f(e.get('max_drawdown'))} | {_f(s.get('max_drawdown'))} | {_f(b.get('lower_bound'),5)} |")
    cell = cur.get("cell")
    if cell and str(cell) in rc["by_regime_cell"]:
        lines += ["", f"### Arms inside today's cell `{cell}` (ETF era) vs overall", "", "| arm | Sharpe in cell | ann in cell | Sharpe overall | ann overall | times selected |", "|---|---|---|---|---|---|"]
        incell = rc["by_regime_cell"][str(cell)]["candidates"]
        for arm, s in rc["candidates"].items():
            c = incell.get(arm, {})
            lines.append(f"| {arm} | {_f(c.get('sharpe_annualised'))} | {_f(c.get('annualised_return'))} | {_f(s.get('sharpe_annualised'))} | {_f(s.get('annualised_return'))} | {rc['selection_counts'].get(arm, 0)} |")
    lines += ["", "## Long sample (monthly, French portfolios + bonds + oil, 8 arms, regime-conditional)", "",
              f"OOS {lsr['oos_period']['first_segment']} → {lsr['oos_period']['last_segment']} ({lsr['oos_period']['n_segments']} months). "
              f"Ensemble Sharpe {_f(lsr['ensemble'].get('sharpe_annualised'))} vs market {_f(lsr['spy_buy_and_hold'].get('sharpe_annualised'))}; "
              f"excess LB {_f(lsr['excess_vs_spy_bootstrap'].get('lower_bound'),5)}; DSR {_f(lsr['deflated_sharpe'].get('dsr'))}; PBO {_f(lsr['pbo_candidates'].get('pbo'))}.", "",
              "| cell | n months | ens Sharpe | market Sharpe | ens ann | market ann | excess mean |", "|---|---|---|---|---|---|---|"]
    for cell_, d in lsr["by_regime_cell"].items():
        if not isinstance(d, dict):
            continue
        e, s, b = d["ensemble"], d["spy"], d.get("excess_vs_spy_bootstrap", {})
        lines.append(f"| {cell_} | {d['n_segments']} | {_f(e.get('sharpe_annualised'))} | {_f(s.get('sharpe_annualised'))} | {_f(e.get('annualised_return'))} | {_f(s.get('annualised_return'))} | {_f(b.get('mean'),5)} |")
    lines += ["", "## Analog windows (long sample; annualised returns of the panel columns, cells in force)", ""]
    for name, w in rep["analog_windows"].items():
        ar = w.get("annualised_return", {})
        lines.append(f"**{name}** — {w.get('role')}; {w.get('n_months')} months; cells {w.get('cells_in_force')}. "
                     f"Market {_f(ar.get('MKT'))}, bills {_f(ar.get('RF'))}, bonds {_f(ar.get('BOND'))}, energy {_f(ar.get('Enrgy'))} "
                     f"(energy − market {_f(w.get('energy_minus_market_annualised'))}), momentum factor {_f(ar.get('MOM'))}, value factor {_f(ar.get('HML'))}, "
                     f"oil {_f(ar.get('OIL'))}; regime-conditional ensemble {_f(w.get('long_sample_ensemble', {}).get('annualised_return'))} vs market "
                     f"{_f(w.get('market_same_months', {}).get('annualised_return'))} (mean monthly excess {_f(w.get('ensemble_minus_market_mean_monthly'),5)}).")
        lines.append("")
    lines += [f"Provenance: {json.dumps({k: rep['provenance'].get(k) for k in ('as_of','panel_shape','long_sample_panel_shape','long_sample_benchmark','db_cross_check_1990_plus','excluded','registration_sha256_observed')}, default=str)}", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
