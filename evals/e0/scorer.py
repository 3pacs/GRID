"""Score raw runner records into the E0 scorecard sections.

* power curve per target IC for the planted (primary) trial, under the VS1
  selection (Holm at the ledger run alpha), BH at ``bh_q`` and the v1 Stage-0
  raw threshold; a detection must also have the planted sign;
* empirical FDR = mean over simulations of V / max(R, 1), where V counts
  selections of exact-null trials plus wrong-sign selections of the planted
  trial; FWER = P(V >= 1); both under the global null (IC = 0) and pooled
  over every simulation;
* null p-value calibration: one-sample Kolmogorov-Smirnov distance of the
  IC = 0 p-values from U(0, 1) (pooled and per trial) and the empirical size at
  0.01 / 0.05 / 0.10.

Permutation p-values live on a grid of 1/(perms + 1); the KS distance of an
exactly calibrated grid p-value is at most about 1/(perms + 1).
"""

from __future__ import annotations

import math
from typing import Mapping, Sequence

import numpy as np


def ks_uniform(pvalues: Sequence[float]) -> dict:
    """One-sample KS distance from U(0,1), its asymptotic p-value and the 5% critical value."""
    x = np.sort(np.asarray(pvalues, dtype=float))
    n = len(x)
    if n == 0:
        return {"n": 0, "ks_stat": None, "ks_p": None, "critical_5pct": None}
    i = np.arange(1, n + 1)
    d = float(max(np.max(i / n - x), np.max(x - (i - 1) / n)))
    return {"n": n, "ks_stat": d, "ks_p": kolmogorov_sf(d * math.sqrt(n)), "critical_5pct": 1.358 / math.sqrt(n)}


def kolmogorov_sf(t: float) -> float:
    """P(sqrt(n) D > t) for large n (Kolmogorov distribution survival function)."""
    if t <= 0:
        return 1.0
    if t < 0.27:
        return 1.0
    total = 0.0
    for k in range(1, 101):
        term = 2.0 * (-1) ** (k - 1) * math.exp(-2.0 * k * k * t * t)
        total += term
        if abs(term) < 1e-16:
            break
    return float(min(1.0, max(0.0, total)))


def _rate(values: Sequence[float]) -> dict:
    """Mean of 0/1 flags (a rate) or of per-simulation proportions, with its standard error."""
    x = np.asarray(values, dtype=float)
    n = len(x)
    if not n:
        return {"rate": None, "se": None, "n": 0}
    se = float(x.std(ddof=1) / math.sqrt(n)) if n > 1 else None
    return {"rate": float(x.mean()), "se": se, "n": n}


def _false_and_total(sim: Mapping, planted: Sequence[str], rule: str, planted_sign: int, is_null: bool) -> tuple[int, int]:
    v = r = 0
    for t, rec in sim["trials"].items():
        if not rec[rule]:
            continue
        r += 1
        if is_null or t not in planted:
            v += 1
        elif rec["mean_ic"] is None or np.sign(rec["mean_ic"]) != planted_sign:
            v += 1  # a wrong-sign selection of the planted trial is a false (directional) discovery
    return v, r


def score_scenario(
    rows: Mapping[str, list],
    *,
    primary: str,
    planted: Sequence[str],
    threshold: float,
    scales: Mapping[str, Mapping],
    direction: int,
) -> dict:
    power = []
    fdp = {"holm": [], "bh": []}
    null_fdp = {"holm": [], "bh": []}
    for key, sims in rows.items():
        ic = float(key)
        is_null = ic == 0
        for rule in ("holm", "bh"):
            for sim in sims:
                v, r = _false_and_total(sim, planted, rule, direction, is_null)
                fdp[rule].append(v / max(r, 1))
                if is_null:
                    null_fdp[rule].append(v / max(r, 1))
        if is_null:
            continue
        prim = [sim["trials"][primary] for sim in sims]
        right_sign = [rec["mean_ic"] is not None and np.sign(rec["mean_ic"]) == direction for rec in prim]
        realized = [rec["mean_ic"] for rec in prim if rec["mean_ic"] is not None]
        power.append({
            "target_ic": ic,
            "planted_scale": scales[primary]["scales"][key],
            "realized_mean_ic": float(np.mean(realized)) if realized else None,
            "realized_ic_sd_across_sims": float(np.std(realized, ddof=1)) if len(realized) > 1 else None,
            "sims": len(prim),
            "power_holm_run_alpha": _rate([rec["holm"] and ok for rec, ok in zip(prim, right_sign)]),
            "power_bh": _rate([rec["bh"] and ok for rec, ok in zip(prim, right_sign)]),
            "power_raw_threshold": _rate([rec["status"] == "tested" and rec["p"] <= threshold and ok
                                          for rec, ok in zip(prim, right_sign)]),
            "median_block": float(np.median([rec["block"] for rec in prim if rec["block"] is not None]))
            if any(rec["block"] is not None for rec in prim) else None,
            "median_usable_dates": float(np.median([rec["n"] for rec in prim])),
        })

    null_rows = rows.get("0", [])
    pooled = [rec["p"] for sim in null_rows for rec in sim["trials"].values() if rec["status"] == "tested"]
    per_trial = {}
    for t in (null_rows[0]["trials"] if null_rows else {}):
        ps = [sim["trials"][t]["p"] for sim in null_rows if sim["trials"][t]["status"] == "tested"]
        ics = [sim["trials"][t]["mean_ic"] for sim in null_rows if sim["trials"][t]["mean_ic"] is not None]
        per_trial[t] = {
            **ks_uniform(ps),
            "size_at_0.01": _rate([p <= 0.01 for p in ps]),
            "size_at_0.05": _rate([p <= 0.05 for p in ps]),
            "size_at_0.10": _rate([p <= 0.10 for p in ps]),
            "null_mean_ic": float(np.mean(ics)) if ics else None,
            "null_ic_sd_across_sims": float(np.std(ics, ddof=1)) if len(ics) > 1 else None,
            "median_block": float(np.median([sim["trials"][t]["block"] for sim in null_rows
                                             if sim["trials"][t]["block"] is not None]))
            if ps else None,
        }
    return {
        "power_curve": power,
        "fdr": {
            "global_null": {
                "sims": len(null_rows),
                "fwer_holm_run_alpha": _rate([x > 0 for x in null_fdp["holm"]]),
                "fwer_bh": _rate([x > 0 for x in null_fdp["bh"]]),
                "fdr_holm_run_alpha": _rate(null_fdp["holm"]),
                "fdr_bh": _rate(null_fdp["bh"]),
            },
            "all_simulations": {
                "sims": len(fdp["bh"]),
                "fdr_holm_run_alpha": _rate(fdp["holm"]),
                "fdr_bh": _rate(fdp["bh"]),
            },
        },
        "null_calibration": {
            "pooled": {
                **ks_uniform(pooled),
                "size_at_0.01": _rate([p <= 0.01 for p in pooled]),
                "size_at_0.05": _rate([p <= 0.05 for p in pooled]),
                "size_at_0.10": _rate([p <= 0.10 for p in pooled]),
            },
            "per_trial": per_trial,
        },
    }


def power_at(scored: Mapping, ic: float, rule: str) -> float | None:
    for row in scored["power_curve"]:
        if abs(row["target_ic"] - ic) < 1e-12:
            return row[rule]["rate"]
    return None
