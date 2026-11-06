"""Null-calibration farm (S7): seeded synthetic worlds through the real test code paths."""

from __future__ import annotations

import json

import numpy as np
import pytest

from alpha_research.validation import null_farm as nf
from scripts import null_farm as cli
from scripts import worker


def test_cell_ids_are_stable_and_ignore_reps():
    a = nf.make_cell("s09_discover", "iid", 60, reps=10, perms=199)
    b = nf.make_cell("s09_discover", "iid", 60, reps=999, perms=199)
    c = nf.make_cell("s09_discover", "iid", 60, reps=10, perms=399)
    assert a["id"] == b["id"] == nf.cell_id(a)
    assert a["id"] != c["id"]


@pytest.mark.parametrize(
    "args",
    [("nope", "iid", 60), ("s09_discover", "nope", 60), ("s09_discover", "iid", 10)],
)
def test_make_cell_refuses_bad_specs(args):
    with pytest.raises(ValueError):
        nf.make_cell(*args)


def test_replicates_are_deterministic_and_independent_of_shard_layout():
    cell = nf.make_cell("s09_discover", "ar1_target", 60, reps=8, perms=199)
    whole = nf.run_shard(nf.make_shards([cell], 8, 7)[0])
    halves = [r for s in nf.make_shards([cell], 4, 7) for r in nf.run_shard(s)]
    assert whole == halves
    assert len({r["p"] for r in whole}) > 1  # replicates differ
    other_seed = nf.run_shard(nf.make_shards([cell], 8, 8)[0])
    assert [r["p"] for r in other_seed] != [r["p"] for r in whole]


def test_run_shard_refuses_a_tampered_cell():
    cell = nf.make_cell("s09_discover", "iid", 60, reps=2)
    shard = nf.make_shards([cell], 2, 1)[0]
    shard["cell"]["n"] = 61
    with pytest.raises(ValueError, match="cell id"):
        nf.run_shard(shard)


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"horizon": 5},
        {"horizon": 5, "sampling": "fixed_step_block_null"},
        {"selection": "ledger_holm"},
    ],
)
def test_s09_world_builds_exactly_n_discovery_rows(params):
    cell = nf.make_cell("s09_discover", "regime_switch", 60, reps=1, perms=199, **params)
    rec = nf.replicate(cell, 0, 1)  # raises unless build_family_rows gives n rows
    assert rec["status"] == "tested" and 0 < rec["p"] <= 1
    if params.get("sampling") == "fixed_step_block_null":
        assert rec["block_used"] >= 5  # overlap depth 4 + 1
        assert rec["any_selected"] is False  # fixed-step runs never select


def test_panel_world_plants_the_specified_ic_and_nothing_under_the_null():
    def mean_ic(effect):
        cell = nf.make_cell("gauntlet_perm", "persistent_feature", 400, reps=1, effect=effect)
        signal, fwd, depth = nf.panel_world(cell, np.random.default_rng(3))
        s, f = signal.to_numpy()[:-1], fwd.to_numpy()[1:]
        ics = [np.corrcoef(a, b)[0, 1] for a, b in zip(s, f)]
        assert depth == 0
        return float(np.mean(ics))

    assert abs(mean_ic(0.0)) < 0.03
    assert mean_ic(0.3) > 0.2


def test_overlap_depth_follows_the_declaration():
    rng = np.random.default_rng(0)
    declared = nf.make_cell("gauntlet_perm", "iid", 60, reps=1, horizon=5)
    undeclared = nf.make_cell("gauntlet_perm", "iid", 60, reps=1, horizon=5, declare_overlap=False)
    assert nf.panel_world(declared, rng)[2] == 4
    assert nf.panel_world(undeclared, rng)[2] == 0


def test_gauntlet_replicate_runs_the_real_permutation_test():
    cell = nf.make_cell("gauntlet_perm", "iid", 40, reps=1, perms=19, n_assets=10, top_n=2)
    rec = nf.replicate(cell, 0, 1)
    assert rec["p"] >= 1 / 20 and rec["block_used"] >= 1
    assert set(rec) >= {"reject", "observed_sharpe", "dsr", "dsr_passed"}


def test_ci_sized_calibration_is_near_nominal_and_power_is_high():
    """Seeded, seconds-long: S09 and the one-trial DSR hold size on iid worlds.

    The best-of-10 DSR is conservative (the first sweep measured well under 5%):
    the expected-max threshold is compared with a single trial's standard error.
    """
    s09 = nf.make_cell("s09_discover", "iid", 60, reps=200, perms=199)
    dsr = nf.make_cell("dsr_best_of", "iid", 250, reps=600, n_trials=1)
    best_of = nf.make_cell("dsr_best_of", "iid", 250, reps=300, n_trials=10)
    power = nf.make_cell("s09_discover", "persistent_feature", 120, reps=40, perms=199, effect=0.45)
    cells = (s09, dsr, best_of, power)
    rows = {
        row["cell_id"]: row
        for row in nf.summarize(
            cells,
            [r for c in cells for s in nf.make_shards([c], 1000, 11) for r in nf.run_shard(s)],
        )
    }
    assert rows[best_of["id"]]["reject"]["rate"] <= 0.05
    for cell in (s09, dsr):
        r = rows[cell["id"]]["reject"]
        assert r["n"] == cell["reps"]
        assert 0.01 <= r["rate"] <= 0.10, r  # loose: MC SE ~1.5% / ~0.9%
    assert rows[power["id"]]["kind"] == "power"
    assert rows[power["id"]]["reject"]["rate"] >= 0.8


def test_exact_permutation_levels():
    assert nf.exact_permutation_level(149) == pytest.approx(7 / 150)
    assert nf.exact_permutation_level(999) == pytest.approx(0.049)
    assert nf.exact_permutation_level(1999, strict=False) == pytest.approx(0.05)


def test_rate_and_size_status():
    r = nf.rate([True] * 5 + [False] * 95)
    assert r["rate"] == pytest.approx(0.05) and r["se"] == pytest.approx((0.05 * 0.95 / 100) ** 0.5)
    assert r["ci95"][0] < 0.05 < r["ci95"][1]
    assert nf.size_status(r) == "in_band"
    assert nf.size_status(nf.rate([True] * 300 + [False] * 700)) == "high_sig"
    assert nf.size_status(nf.rate([True] * 7 + [False] * 93)) == "high"
    assert nf.rate([])["rate"] is None


def test_summarize_deduplicates_reruns_and_renders_markdown():
    cell = nf.make_cell("dsr_best_of", "iid", 250, reps=4, n_trials=3)
    recs = nf.run_shard(nf.make_shards([cell], 4, 1)[0])
    rows = nf.summarize([cell], recs + recs[:2])
    assert rows[0]["reject"]["n"] == 4 and rows[0]["kind"] == "size"
    table = nf.markdown_table(rows)
    assert "dsr_best_of" in table and "n_trials=3" in table


def test_worker_runs_a_null_calibration_shard_without_db():
    cell = nf.make_cell("s09_discover", "iid", 60, reps=3, perms=199)
    shard = nf.make_shards([cell], 3, 5)[0]
    out = worker.run_simulation({"kind": "null_calibration", "shard": shard})
    assert out["output"]["records"] == nf.run_shard(shard)
    assert out["metrics"]["reps"] == 3
    assert "error" in worker.run_simulation({"kind": "null_calibration"})


def test_cli_plan_run_summarize_round_trip(tmp_path):
    plan = tmp_path / "plan.json"
    assert cli.main(["plan", "--grid", "ci", "--out", str(plan)]) == 0
    spec = json.loads(plan.read_text())
    assert {c["test"] for c in spec["cells"]} == {"s09_discover", "dsr_best_of", "gauntlet_perm"}
    results = tmp_path / "results"
    for box in (0, 1):
        assert cli.main(
            ["run", "--plan", str(plan), "--out", str(results), "--host-index", str(box),
             "--host-count", "2", "--weights", "2,1"]
        ) == 0
    assert len(list(results.glob("*.jsonl"))) == len(spec["shards"])
    summary, table = tmp_path / "s.json", tmp_path / "s.md"
    assert cli.main(
        ["summarize", "--plan", str(plan), "--results", str(results), "--json", str(summary),
         "--markdown", str(table)]
    ) == 0
    rows = json.loads(summary.read_text())
    assert all(r["reject"]["n"] == r["reps_planned"] for r in rows)
    # resumable: a rerun finds nothing to do; a top-up plan skips finished shards
    topup = tmp_path / "topup.json"
    assert cli.main(["plan", "--grid", "ci", "--out", str(topup), "--done", str(results)]) == 0
    assert json.loads(topup.read_text())["shards"] == []


def test_shard_assignment_partitions_the_plan():
    shards = nf.make_shards(cli.ci_cells(), 7, 1)
    parts = [cli.assigned(shards, i, [3.0, 1.0, 1.0]) for i in range(3)]
    ids = [s["shard_id"] for part in parts for s in part]
    assert sorted(ids) == sorted(s["shard_id"] for s in shards)
