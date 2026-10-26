"""v1's known-effect replication, restricted to dates before 2007-11-01.

Same cached panel file, deny-list, declared trials, statistic and BH rule as
``evals.e0.replication`` (imported, not copied where unchanged). The only
change is the window: VS1 v8 (discovery from 2008-01-01, probe from
2007-11-02) and the sectors-v6 registration bound to it make non-Technology
2008-2010 a registered window, so v2 reads nothing on or after 2007-11-01.

The panel's later rows are dropped from the DATE vector alone, before any close
is used; the remaining dates and tickers then pass v1's own ``check_guards`` at
the 2007-11-01 cutoff (and the 908-ticker VS1 Technology deny-list).
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import date

import numpy as np

from evals.e0 import machinery, replication
from evals.e0.replication import PricePanel, ReplicationGuardError
from evals.e0.structure import PACKAGE as E0_PACKAGE

#: Hard ceiling, independent of config: E0 v2 never reads a date on or after 2007-11-01.
MAX_CUTOFF = date(2007, 11, 1)


def _sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_prices(config: Mapping) -> PricePanel:
    """The pinned v1 replication panel cut at ``cutoff_exclusive`` (only earlier dates are returned)."""
    spec = config["replication"]
    cutoff = date.fromisoformat(spec["cutoff_exclusive"])
    if cutoff > MAX_CUTOFF:  # refused before the panel file is opened
        raise ReplicationGuardError(f"replication cutoff {cutoff} is after {MAX_CUTOFF}: E0 v2 reads no later date")
    path = E0_PACKAGE / spec["file"]
    sha = _sha256(path)
    if sha != spec["file_sha256"]:
        raise ReplicationGuardError(f"replication panel {spec['file']} changed: sha256 {sha}")
    with np.load(path, allow_pickle=False) as data:
        all_dates = [date.fromisoformat(str(d)) for d in data["dates"]]
        tickers = [str(t) for t in data["tickers"]]
        n = sum(d < cutoff for d in all_dates)
        if n == 0 or any(d >= cutoff for d in all_dates[:n]):
            raise ReplicationGuardError("replication dates before the cutoff are not a sorted prefix")
        dates = all_dates[:n]
        replication.check_guards(dates, tickers, replication.load_denylist(config), cutoff)
        closes = np.asarray(data["closes"], dtype=float)[:n]
    if closes.shape != (len(dates), len(tickers)):
        raise ValueError("closes must be grid dates x tickers")
    if np.any(closes[np.isfinite(closes)] <= 0):
        raise ValueError("non-positive adjusted close in the replication panel")
    return PricePanel(dates=dates, tickers=tickers, closes=closes)


def run_replication(config: Mapping, *, perms: int | None = None) -> dict:
    """v1's ``run_replication`` on the pre-cutoff panel (same trials, statistic and BH rule)."""
    spec = config["replication"]
    perms = int(perms or spec["perms"])
    panel = load_prices(config)
    start = date.fromisoformat(spec["decision_start"])
    records, dates_used = {}, {}
    for trial, tspec in spec["trials"].items():
        decided, feature, label = replication.trial_matrices(panel, tspec, start)
        rec = machinery.measure(feature, label, int(tspec["horizon_steps"]) * int(spec["grid_sessions"]), trial,
                                perms=perms)
        records[trial] = rec
        dates_used[trial] = [decided[0].isoformat(), decided[-1].isoformat()] if decided else None
    trials = list(records)
    pvalues = [records[t]["p"] if records[t]["status"] == "tested" else 1.0 for t in trials]
    bh = machinery.bh(pvalues)
    holm = machinery.holm(pvalues)
    out, hits = {}, 0
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
        "guards": {"cutoff_exclusive": spec["cutoff_exclusive"], "panel_sha256": spec["file_sha256"],
                   "denylist_size": len(replication.load_denylist(config)),
                   "vs1_windows_touched": False, "vs1_universe_touched": False},
        "perms": perms,
        "trials": out,
        "hits": hits,
        "declared": len(trials),
        "survivorship": "today's non-Technology sector-map companies with pre-2007-11 TIINGO history: survivor-biased",
    }
