"""Known-effect replication on REAL prices entirely outside VS1 (pre-2011, non-Technology).

The only real-outcome part of E0. Data: split+dividend adjusted TIINGO closes
(source 524) of today's non-Technology sector-map companies, 1993-06 ..
2010-12, sampled every 5 sessions, cached offline in ``data/`` by
:mod:`evals.e0.extract_replication`. Hard guards, checked on every load:

* every date is before ``cutoff_exclusive`` (2011-01-01): no VS1 window
  (v6/v7 discovery 2011-08..2019-12, holdout 2020-01..2026-06) is present;
* no ticker is in the pinned VS1 Technology deny-list (the 782-issuer v2
  universe, every sector-map Technology company and XLK).

Declared trials (config ``replication.trials``) are published pre-period
price effects: weekly and one-month reversal, 12-1 momentum. Each trial runs
through the VS1 statistic (machinery ``measure``: per-date rank IC, block
sign-flip); BH at ``bh_q`` over the declared trials. A *hit* is a BH selection
with the published sign. Survivorship: the universe is today's list, so the
panel over-represents survivors; this is a machinery check, not an estimate
of the effects' size.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping

import numpy as np

from evals.e0 import machinery

PACKAGE = Path(__file__).resolve().parent


class ReplicationGuardError(PermissionError):
    """The replication data touches a VS1 window or the VS1 universe."""


@dataclass(frozen=True)
class PricePanel:
    dates: list[date]
    tickers: list[str]
    closes: np.ndarray  # grid dates x tickers, NaN = no close


def load_denylist(config: Mapping) -> frozenset[str]:
    raw = json.loads((PACKAGE / config["replication"]["denylist"]).read_text(encoding="utf-8"))
    return frozenset(str(t).upper() for t in raw["tickers"])


def check_guards(dates: list[date], tickers: list[str], denylist: frozenset[str], cutoff: date) -> None:
    if not dates:
        raise ReplicationGuardError("empty replication panel")
    late = [d for d in dates if d >= cutoff]
    if late:
        raise ReplicationGuardError(f"replication data reaches {late[0]} (cutoff {cutoff}): VS1 windows are off limits")
    if any(b <= a for a, b in zip(dates, dates[1:])):
        raise ReplicationGuardError("replication dates must be strictly increasing")
    banned = sorted(set(t.upper() for t in tickers) & denylist)
    if banned:
        raise ReplicationGuardError(f"replication data contains VS1 Technology tickers: {banned[:10]}")


def load_prices(config: Mapping, path: Path | None = None) -> PricePanel:
    spec = config["replication"]
    data = np.load(path or (PACKAGE / spec["file"]), allow_pickle=False)
    dates = [date.fromisoformat(str(d)) for d in data["dates"]]
    tickers = [str(t) for t in data["tickers"]]
    closes = np.asarray(data["closes"], dtype=float)
    if closes.shape != (len(dates), len(tickers)):
        raise ValueError("closes must be grid dates x tickers")
    check_guards(dates, tickers, load_denylist(config), date.fromisoformat(spec["cutoff_exclusive"]))
    if np.any(closes[np.isfinite(closes)] <= 0):
        raise ValueError("non-positive adjusted close in the replication panel")
    return PricePanel(dates=dates, tickers=tickers, closes=closes)


def trial_matrices(panel: PricePanel, spec: Mapping, decision_start: date) -> tuple[list[date], np.ndarray, np.ndarray]:
    """Horizon-spaced decisions; feature = past return, label = forward return (grid steps)."""
    look, skip, h = int(spec["lookback_steps"]), int(spec["skip_steps"]), int(spec["horizon_steps"])
    if not 0 <= skip < look or h < 1:
        raise ValueError("need 0 <= skip < lookback and horizon >= 1")
    c = panel.closes
    first = next(i for i, d in enumerate(panel.dates) if d >= decision_start)
    first = max(first, look)
    positions = list(range(first, len(panel.dates) - h, h))
    with np.errstate(divide="ignore", invalid="ignore"):
        feature = np.array([c[g - skip] / c[g - look] - 1.0 for g in positions])
        label = np.array([c[g + h] / c[g] - 1.0 for g in positions])
    feature[~np.isfinite(feature)] = np.nan
    label[~np.isfinite(label)] = np.nan
    return [panel.dates[g] for g in positions], feature, label


def run_replication(config: Mapping, *, perms: int | None = None, path: Path | None = None) -> dict:
    spec = config["replication"]
    perms = int(perms or spec["perms"])
    panel = load_prices(config, path)
    start = date.fromisoformat(spec["decision_start"])
    records, dates_used = {}, {}
    for trial, tspec in spec["trials"].items():
        decided, feature, label = trial_matrices(panel, tspec, start)
        rec = machinery.measure(feature, label, int(tspec["horizon_steps"]) * int(spec["grid_sessions"]), trial,
                                perms=perms)
        records[trial] = rec
        dates_used[trial] = [decided[0].isoformat(), decided[-1].isoformat()] if decided else None
    trials = list(records)
    pvalues = [records[t]["p"] if records[t]["status"] == "tested" else 1.0 for t in trials]
    bh = machinery.bh(pvalues)
    holm = machinery.holm(pvalues)
    out = {}
    hits = 0
    for t, b, hm in zip(trials, bh, holm):
        rec, tspec = records[t], spec["trials"][t]
        sign_ok = rec["mean_ic"] is not None and np.sign(rec["mean_ic"]) == tspec["expected_sign"]
        selected = rec["status"] == "tested" and b <= float(spec["bh_q"])
        hit = bool(selected and sign_ok)
        hits += hit
        out[t] = {
            "prior": tspec["prior"],
            "expected_sign": tspec["expected_sign"],
            "mean_ic": rec["mean_ic"],
            "p": rec["p"],
            "bh_adjusted_p": b,
            "holm_adjusted_p": hm,
            "n_dates": rec["n"],
            "median_entities": rec["median_entities"],
            "block": rec["block"],
            "decision_range": dates_used[t],
            "bh_selected": bool(selected),
            "sign_matches_prior": bool(sign_ok),
            "hit": hit,
        }
    return {
        "universe": {"tickers": len(panel.tickers), "grid_dates": len(panel.dates),
                     "first_date": panel.dates[0].isoformat(), "last_date": panel.dates[-1].isoformat()},
        "guards": {"cutoff_exclusive": spec["cutoff_exclusive"], "denylist_size": len(load_denylist(config)),
                   "vs1_windows_touched": False, "vs1_universe_touched": False},
        "perms": perms,
        "trials": out,
        "hits": hits,
        "declared": len(trials),
        "survivorship": "today's non-Technology sector-map companies with pre-2011 TIINGO history: survivor-biased",
    }
