#!/usr/bin/env python3
"""Null-calibration farm CLI (S7, 2026-11-06): plan, run and summarize sweeps.

The harness lives in :mod:`alpha_research.validation.null_farm`; this script
only plans shards, runs them on one box (multi-process, resumable) and merges
the per-shard JSONL files. No DB, no network: results are files.

    # 1. plan (writes the cells + shards; the same plan file goes to every box)
    python scripts/null_farm.py plan --grid first_sweep --out plan.json
    # 2. run this box's share (shard i of every k), resumable
    python scripts/null_farm.py run --plan plan.json --out results/ --jobs 20 \\
        --host-index 0 --host-count 3
    # 3. merge every box's results
    python scripts/null_farm.py summarize --plan plan.json --results r1/ r2/ r3/ \\
        --json summary.json --markdown summary.md

Shards are assigned by weighted round-robin (``--weights``) so boxes of
different sizes finish together. A shard file is written atomically
(``.part`` then rename), so a killed run restarts where it stopped.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

for _var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_var, "1")

from alpha_research.validation import null_farm as nf

BASE_SEED = 20261106
# 149 permutations: a calibrated p < 0.05 then rejects with probability 7/150 =
# 4.67% exactly (the gauntlet's 999 default reaches 4.9% at ~6.7x the cost; the
# null distribution itself does not depend on the count).
GAUNTLET_PERMS = 149
GAUNTLET_REPS = 100


# --- grids ---------------------------------------------------------------------------


def ci_cells() -> list[dict]:
    """A seconds-long grid for CI (see tests/test_null_farm.py)."""
    return [
        nf.make_cell("s09_discover", "iid", 60, reps=40, perms=199),
        nf.make_cell("dsr_best_of", "iid", 250, reps=200, n_trials=10),
        nf.make_cell("gauntlet_perm", "iid", 40, reps=2, perms=19, n_assets=10, top_n=2),
    ]


def first_sweep_cells() -> list[dict]:
    """First fleet sweep: size of every test x world x n x block rule, plus power.

    Gauntlet cells are costly (perms + 1 long-short passes per replicate), so
    they get fewer replicates; S09 and DSR replicates are cheap.
    """
    cells: list[dict] = []
    g_reps, g = GAUNTLET_REPS, {"perms": GAUNTLET_PERMS}

    def gp(world: str, n: int = 250, block: int = 0, reps: int = g_reps, **params) -> None:
        cells.append(nf.make_cell("gauntlet_perm", world, n, block=block, reps=reps, **g, **params))

    # Gauntlet permutation test (real defaults: top_n=5, cost_bps=10, 20 assets).
    for world in ("iid", "persistent_feature", "ar1_target", "regime_switch", "jump_diffusion"):
        gp(world)
    # Costs: block-shuffling a persistent signal changes its turnover (and costs).
    gp("persistent_feature", cost_bps=0.0)
    gp("ar1_target", cost_bps=0.0)
    # Declared block lengths.
    gp("persistent_feature", block=20)
    gp("ar1_target", block=5, cost_bps=0.0)
    gp("ar1_target", block=20, cost_bps=0.0)
    # Overlapping 5-day forward returns, overlap declared vs not (costs off to isolate it).
    gp("persistent_feature", horizon=5, cost_bps=0.0)
    gp("persistent_feature", horizon=5, cost_bps=0.0, declare_overlap=False)
    # Full gauntlet verdicts under the null (ROBUST / MARGINAL false positives).
    cells.append(nf.make_cell("gauntlet_full", "persistent_feature", 250, reps=60, **g))
    # Power at small cross-sectional ICs.
    for effect in (0.02, 0.05):
        gp("persistent_feature", effect=effect, cost_bps=0.0)
    gp("persistent_feature", effect=0.05)
    # Phase B additions: does the cost-free persistent excess shrink with n?
    gp("persistent_feature", n=500, cost_bps=0.0)

    # Deflated Sharpe, best of n_trials (cheap).
    d_reps = 4000
    dsr_worlds = {
        "iid": {},
        "jump_diffusion": {},
        "ar1_target": {"target": {"phi": 0.2}},  # autocorrelated strategy returns
        "regime_switch": {"target": {"mu_sd": 0.0}},  # volatility regimes only (true SR 0)
    }
    for world, extra in dsr_worlds.items():
        for n in (250, 1000):
            for trials in (1, 10, 100):
                cells.append(nf.make_cell("dsr_best_of", world, n, reps=d_reps, n_trials=trials, **extra))
    for n in (250, 1000):
        cells.append(nf.make_cell("dsr_best_of", "iid", n, reps=d_reps, n_trials=10, horizon=5))
        cells.append(nf.make_cell("dsr_best_of", "iid", n, reps=d_reps, n_trials=10, trial_rho=0.5))
    for sr in (0.5, 1.0, 2.0):
        for trials in (1, 10, 100):
            cells.append(
                nf.make_cell("dsr_best_of", "iid", 1000, reps=2000, n_trials=trials, effect_sr=sr)
            )

    # S09 / S11 discovery (cheap; 1999 perms, exact nominal 5%).
    s_reps = 4000
    s09_worlds = {
        "iid": {},
        "persistent_feature": {},
        "ar1_target": {},
        "ar1_target_0.7": {"target": {"phi": 0.7}},
        "ar1_target_neg": {"target": {"phi": -0.35}},
        "regime_switch": {},
        "jump_diffusion": {},
    }
    for label, extra in s09_worlds.items():
        world = label.split("_0.")[0].replace("_neg", "")
        for n in (60, 120, 240, 500):
            cells.append(nf.make_cell("s09_discover", world, n, reps=s_reps, **extra))
            if label in ("ar1_target", "persistent_feature", "regime_switch"):
                for block in (1, 5):
                    cells.append(
                        nf.make_cell("s09_discover", world, n, block=block, reps=s_reps, **extra)
                    )
    for n in (60, 240):
        for sampling in ("horizon_spaced", "fixed_step_block_null"):
            cells.append(
                nf.make_cell(
                    "s09_discover", "persistent_feature", n, reps=s_reps, horizon=5, sampling=sampling
                )
            )
        cells.append(
            nf.make_cell("s09_discover", "ar1_target", n, reps=s_reps, selection="ledger_holm")
        )
    for n in (60, 240):
        for effect in (0.1, 0.2, 0.3):
            cells.append(
                nf.make_cell("s09_discover", "persistent_feature", n, reps=2000, effect=effect)
            )
            cells.append(nf.make_cell("s09_discover", "ar1_target", n, reps=2000, effect=effect))
    # Phase B additions: long-memory regimes and declared blocks at larger n, ledger Holm FWER.
    for block in (0, 20):
        cells.append(nf.make_cell("s09_discover", "regime_switch", 1000, block=block, reps=s_reps))
    for block in (0, 5):
        cells.append(nf.make_cell("s09_discover", "ar1_target", 1000, block=block, reps=s_reps))
    for n in (120, 500):
        cells.append(
            nf.make_cell("s09_discover", "ar1_target", n, reps=s_reps, selection="ledger_holm")
        )
    return cells


GRIDS = {"ci": ci_cells, "first_sweep": first_sweep_cells}


def shard_size(cell: dict) -> int:
    """Replicates per shard: roughly a few minutes of work each."""
    if cell["test"] == "gauntlet_full":
        return 5
    if cell["test"] == "gauntlet_perm":
        return 10 if cell["n"] <= 250 else 5
    return 500


def shard_cost(shard: dict) -> float:
    """Relative cost estimate, used to balance boxes."""
    cell, reps = shard["cell"], shard["rep_stop"] - shard["rep_start"]
    params = cell.get("params", {})
    if cell["test"].startswith("gauntlet"):
        per = (int(params.get("perms", 249)) + 1) * cell["n"] * 2.5e-4
        if cell["test"] == "gauntlet_full":
            per += cell["n"] * 0.08
        return reps * per
    if cell["test"] == "dsr_best_of":
        return reps * (1e-4 * int(params.get("n_trials", 1)) + 1e-3)
    return reps * (0.02 + cell["n"] * 1e-4)


# --- commands ------------------------------------------------------------------------


def cmd_plan(args: argparse.Namespace) -> int:
    global GAUNTLET_REPS
    if args.gauntlet_reps:
        GAUNTLET_REPS = args.gauntlet_reps
    cells = GRIDS[args.grid]()
    ids = [c["id"] for c in cells]
    if len(set(ids)) != len(ids):
        raise SystemExit("duplicate cells in grid")
    only = set(args.only or ())
    done = {p.stem for d in args.done or () for p in Path(d).glob("*.jsonl")}
    shards = []
    for cell in cells:
        if only and cell["id"] not in only:
            continue
        shards.extend(
            s for s in nf.make_shards([cell], shard_size(cell), args.seed) if s["shard_id"] not in done
        )
    plan = {
        "version": nf.FARM_VERSION,
        "grid": args.grid,
        "base_seed": args.seed,
        "cells": cells,
        "shards": shards,
    }
    Path(args.out).write_text(json.dumps(plan, indent=1, sort_keys=True))
    total = sum(shard_cost(s) for s in shards)
    print(f"{len(cells)} cells, {len(shards)} shards, est. {total / 3600:.1f} CPU-hours -> {args.out}")
    return 0


def assigned(shards: Sequence[dict], index: int, weights: Sequence[float]) -> list[dict]:
    """Greedy cost balancing: each shard goes to the box with the lowest load/weight."""
    loads = [0.0] * len(weights)
    mine = []
    for shard in sorted(shards, key=shard_cost, reverse=True):
        box = min(range(len(weights)), key=lambda b: loads[b] / weights[b])
        loads[box] += shard_cost(shard)
        if box == index:
            mine.append(shard)
    return mine


def _run_one(shard: dict, out_dir: str) -> tuple[str, int, float]:
    started = time.time()
    records = nf.run_shard(shard)
    path = Path(out_dir) / f"{shard['shard_id']}.jsonl"
    part = path.with_suffix(".part")
    part.write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in records))
    os.replace(part, path)
    return shard["shard_id"], len(records), time.time() - started


def cmd_run(args: argparse.Namespace) -> int:
    plan = json.loads(Path(args.plan).read_text())
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    weights = [float(w) for w in args.weights.split(",")] if args.weights else [1.0] * args.host_count
    if len(weights) != args.host_count:
        raise SystemExit("--weights needs one weight per host")
    mine = assigned(plan["shards"], args.host_index, weights)
    todo = [s for s in mine if not (out / f"{s['shard_id']}.jsonl").exists()]
    print(f"{len(mine)} shards assigned, {len(todo)} to run, {args.jobs} processes", flush=True)
    done = 0
    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        futures = [pool.submit(_run_one, s, str(out)) for s in todo]
        for future in as_completed(futures):
            shard_id, n, seconds = future.result()
            done += 1
            print(f"[{done}/{len(todo)}] {shard_id} {n} reps {seconds:.0f}s", flush=True)
    return 0


def load_records(dirs: Sequence[str]) -> list[dict]:
    records = []
    for d in dirs:
        for path in sorted(Path(d).glob("*.jsonl")):
            records.extend(json.loads(line) for line in path.read_text().splitlines() if line)
    return records


def cmd_summarize(args: argparse.Namespace) -> int:
    plan = json.loads(Path(args.plan).read_text())
    rows = nf.summarize(plan["cells"], load_records(args.results))
    if args.json:
        Path(args.json).write_text(json.dumps(rows, indent=1, sort_keys=True))
    table = nf.markdown_table(rows)
    if args.markdown:
        Path(args.markdown).write_text(table)
    else:
        print(table)
    missing = [r["cell_id"] for r in rows if r["reject"]["n"] < r["reps_planned"]]
    if missing:
        print(f"{len(missing)} cells incomplete", file=sys.stderr)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--grid", choices=sorted(GRIDS), default="first_sweep")
    p.add_argument("--seed", type=int, default=BASE_SEED)
    p.add_argument("--out", required=True)
    p.add_argument("--gauntlet-reps", type=int, default=0, help="top-up: replicates per gauntlet_perm cell")
    p.add_argument("--only", nargs="*", help="top-up: plan shards for these cell ids only")
    p.add_argument("--done", nargs="*", help="top-up: skip shards with a result file in these dirs")
    p.set_defaults(func=cmd_plan)
    r = sub.add_parser("run")
    r.add_argument("--plan", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--jobs", type=int, default=1)
    r.add_argument("--host-index", type=int, default=0)
    r.add_argument("--host-count", type=int, default=1)
    r.add_argument("--weights", default="", help="comma-separated relative box speeds")
    r.set_defaults(func=cmd_run)
    s = sub.add_parser("summarize")
    s.add_argument("--plan", required=True)
    s.add_argument("--results", nargs="+", required=True)
    s.add_argument("--json")
    s.add_argument("--markdown")
    s.set_defaults(func=cmd_summarize)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
