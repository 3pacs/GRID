"""Replay the vault #101 vein-scan v2 panel through analysis.offline_research_proof.

Run: python -m scripts.replay_vein_scan_v2 CSV [--perms 10000] [--seed 20260924]
     [--sampling horizon_spaced|fixed_step_block_null|both] [--v2-ledger LEDGER_CSV]

Offline and CSV-only: no DB, provider, registry, route or timer. The input is the
39-series panel attached to obsidian-vault commit 532883a68
(grid-vein-scan-signals-20260923.csv, sha256 b8014421...). That CSV is a
latest-vintage hindsight pull with no per-source known-at contract, so the run is
an ``exploratory_replay``: nothing it produces is evidence, registered, scored
forward, or eligible for alerts, research, learning, promotion or weights.

Panel loading and feature engineering are copied from the v2 prototype
(``grid-vein-scan-prototype.py`` v2, functions load_panel/build_features) so the
trial universe is identical. Labels, sampling, the null and BH-FDR are NOT
copied: they come from analysis.offline_research_proof.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from analysis.offline_research_proof import (
    Protocol,
    bh_adjusted,
    build_family_rows,
    discover,
    evaluate_holdout,
)

HORIZONS = (1, 5, 10, 20)
TICKERS = ("SPY", "QQQ", "IWM", "GLD", "TLT", "HYG")
STEP = 5  # weekly base decision spacing, business days (v2 STEP)
SPLIT_START, SPLIT_FRAC = "2025-03-01", 0.6  # v2 split rule
V2_SHA256 = "b801442154280f709ac4c512cc93741f2598fefd1df6c4ed11fee50f5554a07a"


def csv_sha256(path) -> str:
    """sha256 of the CSV with LF line endings (git may check it out as CRLF)."""
    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def load_panel(csv_path) -> pd.DataFrame:
    """v2 load_panel: (series,date,value) rows -> business-day wide panel."""
    df = pd.read_csv(
        csv_path, header=None, names=["series", "date", "value"], dtype={"series": str}
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna()
    wide = df.pivot_table(
        index="date", columns="series", values="value", aggfunc="last"
    ).sort_index()
    return wide.reindex(pd.bdate_range(wide.index.min(), wide.index.max()))


def build_features(wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """v2 build_features: level -> chg5/z60/chg20; count -> sum5/z60 of the sum."""
    count = [
        c for c in wide.columns if c.startswith("DERIVED:") and "etf_flow" not in c
    ]
    level = [c for c in wide.columns if c not in count]
    lvl = wide[level].ffill(limit=10)
    cnt = wide[count].fillna(0)
    feats = {}
    for c in level:
        x = lvl[c]
        pct = c.startswith("YF:") or "liquidity" in c
        feats[f"{c}|chg5"] = x.pct_change(5) if pct else x.diff(5)
        feats[f"{c}|z60"] = (x - x.rolling(60).mean()) / x.rolling(60).std()
        feats[f"{c}|chg20"] = x.pct_change(20) if pct else x.diff(20)
    for c in count:
        x = cnt[c].rolling(5).sum()
        feats[f"{c}|sum5"] = x
        feats[f"{c}|z60"] = (x - x.rolling(60).mean()) / x.rolling(60).std()
    return pd.DataFrame(feats), lvl


def prepare(csv_path):
    """Features, target price columns and the v2 split, on a UTC session index."""
    wide = load_panel(csv_path)
    features, lvl = build_features(wide)
    # inf (e.g. pct_change from 0) is not an observation: explicit abstention.
    features = features.replace([np.inf, -np.inf], np.nan)
    features.index = features.index.tz_localize("UTC")
    lvl.index = features.index
    prices = [
        c
        for c in lvl.columns
        if c.startswith("YF:") and c.endswith(":close") and any(t in c for t in TICKERS)
    ]
    idx = features.index[features.index >= pd.Timestamp(SPLIT_START, tz="UTC")]
    split = idx[int(len(idx) * SPLIT_FRAC)]
    end = features.index[-1] + pd.Timedelta(days=1)
    return features, lvl[prices], split, end


def replay(csv_path, sampling, perms=10000, seed=20260924, run_id=None):
    features, prices, split, end = prepare(csv_path)
    families = {f"{p}|fwd{h}": (p, h) for p in prices.columns for h in HORIZONS}
    protocol = Protocol(
        run_id=run_id or f"vein-scan-v2-replay-{sampling}",
        features=tuple(features.columns),
        split=split.isoformat(),
        end=end.isoformat(),
        min_n=30,
        origin="exploratory_replay",
        families=tuple(families),
        fdr_q=0.10,
        sampling=sampling,
        step=STEP,
        perms=perms,
        seed=seed,
        statistic="spearman",
        start=pd.Timestamp(SPLIT_START, tz="UTC").isoformat(),
    )

    def rows(window):
        return {
            name: build_family_rows(protocol, features, prices[p], h, window)
            for name, (p, h) in families.items()
        }

    discovery = rows("discovery")
    frozen = discover(protocol, discovery)
    payload = frozen["payload"]
    ledger = payload["ledger"]
    tested = [t for t in ledger if t["status"] == "tested"]
    by_horizon = {
        h: sum(
            1
            for t in ledger
            if families[t["family"]][1] == h and t["status"] != "tested"
        )
        for h in HORIZONS
    }
    # Diagnostic only: v2's denominator (testable trials only). Never selects.
    testable_only = bh_adjusted([t["p"] for t in tested])
    holdout = evaluate_holdout(frozen, rows("holdout"))
    return {
        "sampling": sampling,
        "input_sha256": csv_sha256(csv_path),
        "discovery_first_decision": min(
            r["decision_at"] for f in discovery.values() for r in f
        )[:10],
        "holdout_start": split.date().isoformat(),
        "trials_attempted": payload["trial_count"],
        "trials_testable": payload["tested_count"],
        "trials_untestable": payload["untestable_count"],
        "untestable_by_horizon": by_horizon,
        "blocks": sorted(set(payload["blocks"].values())),
        "raw_p_lt_0.05": sum(t["p"] < 0.05 for t in tested),
        "min_p": min((t["p"] for t in tested), default=None),
        # BH rejections at 10% over the whole run. Fixed-step runs never select
        # (diagnostic only, #658 review), so count rejections, not selections.
        "bh10_survivors": sum(
            t["status"] == "tested" and t["adjusted_p"] <= 0.10 for t in ledger
        ),
        "selected": sum(t["selected"] for t in ledger),
        "candidate_eligible": payload["candidate_eligible"],
        "bh05_survivors": sum(
            t["status"] == "tested" and t["adjusted_p"] <= 0.05 for t in ledger
        ),
        "bh10_survivors_testable_only_denominator": sum(
            a <= 0.10 for a in testable_only
        ),
        "holdout_checks": len(holdout["holdout_checks"]),
        "candidates": len(holdout["candidates"]),
        "state": holdout["state"],
        "manifest_sha256": frozen["sha256"],
        "_ledger": ledger,
    }


def compare_with_v2(result, v2_ledger_csv):
    """Per-trial n/status/rho agreement with v2's discovery ledger for one method."""
    method = "spaced" if result["sampling"] == "horizon_spaced" else "blockperm"
    v2 = pd.read_csv(v2_ledger_csv)
    v2 = v2[(v2.window == "discovery") & (v2.method == method)]
    v2 = v2.set_index(["feature", "target"])
    mismatched_n = mismatched_status = 0
    rho_gap = 0.0
    for t in result["_ledger"]:
        ref = v2.loc[(t["feature"], t["family"])]
        mismatched_n += int(ref.n) != t["n"] and t["status"] != "excluded_telemetry"
        mismatched_status += (ref.status == "tested") != (t["status"] == "tested")
        if t["status"] == "tested":
            rho_gap = max(rho_gap, abs(float(ref.rho) - t["r"]))
    return {
        "v2_trials": len(v2),
        "n_mismatches": mismatched_n,
        "testable_mismatches": mismatched_status,
        "max_abs_rho_gap": rho_gap,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv")
    parser.add_argument("--perms", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=20260924)
    parser.add_argument("--sampling", default="both")
    parser.add_argument("--v2-ledger")
    args = parser.parse_args()
    modes = (
        ("horizon_spaced", "fixed_step_block_null")
        if args.sampling == "both"
        else (args.sampling,)
    )
    for mode in modes:
        out = replay(args.csv, mode, args.perms, args.seed)
        if args.v2_ledger:
            out["v2_comparison"] = compare_with_v2(out, args.v2_ledger)
        out.pop("_ledger")
        print(json.dumps(out, indent=2, sort_keys=True))
