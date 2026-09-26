"""S11: ledger-steered exploration (analysis.ledger_steered_exploration).

Synthetic data and in-memory/tmp ledgers only; no DB.

* the global ledger is append-only and hash-chained;
* allocation is frozen into the ledger and the run protocol before any data;
* SELF_LAG families are never allocated, holdout windows are single-use;
* cross-run error control: alpha_k = q / (k (k + 1)) with Holm inside a run;
  20+ runs of pure noise do not accumulate false discoveries (end to end and
  by Monte Carlo), while per-run BH on the same p-values does;
* done-when: two consecutive dry runs shift the allocation toward the planted
  family and away from the pure-noise families.
"""

import json
import math
import re
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy.stats import norm

import analysis.ledger_steered_exploration as lse
from analysis.offline_research_proof import (
    Protocol,
    bh_adjusted,
    build_family_rows,
    digest,
    discover,
    holm_adjusted,
    protocol_from_payload,
    run_proof,
)

FIXED = "2026-09-26T12:00:00+00:00"
PLANTED = "alpha::T1|change|fwd1"


def window(k, days=400):
    start = pd.Timestamp("2001-01-01", tz="UTC") + pd.Timedelta(days=k * days)
    return {
        "start": start.isoformat(),
        "split": (start + pd.Timedelta(days=days // 2)).isoformat(),
        "end": (start + pd.Timedelta(days=days - 1)).isoformat(),
    }


def new_ledger(tmp_path=None, q=0.10, catalog=None, ledger_id="s11-test"):
    path = None if tmp_path is None else tmp_path / "ledger.jsonl"
    anchor = None if tmp_path is None else tmp_path / "ledger.anchor.jsonl"
    return lse.Ledger.create(
        path,
        catalog=catalog or lse.synthetic_catalog(),
        anchor=anchor,
        ledger_id=ledger_id,
        q=q,
        recorded_at=FIXED,
    )


def open_ledger(directory):
    return lse.Ledger(directory / "ledger.jsonl", anchor=directory / "ledger.anchor.jsonl")


def run_step(ledger, catalog, policy, epoch, output, run_id):
    return lse.run_synthetic_step(
        ledger, catalog, policy, epoch, output, run_id=run_id, perms_cap=2999,
        recorded_at=FIXED,
    )


# --- ledger chain -------------------------------------------------------------------


def test_ledger_is_append_only_and_hash_chained(tmp_path):
    ledger = new_ledger(tmp_path)
    catalog = lse.synthetic_catalog()
    lse.allocate(
        ledger, catalog, lse.Policy(budget=21), run_id="r1", windows=window(1),
        recorded_at=FIXED,
    )
    lse.abandon(ledger, "test", recorded_at=FIXED)
    path = tmp_path / "ledger.jsonl"
    lines = path.read_bytes().split(b"\n")[:-1]
    assert len(lines) == 3
    records = [json.loads(line) for line in lines]
    assert [r["kind"] for r in records] == ["genesis", "allocation", "abandoned"]
    assert records[0]["prev_sha256"] is None
    for previous, record in zip(lines, records[1:]):
        assert record["prev_sha256"] == lse.sha256_bytes(previous)
    assert all(r["promotion_allowed"] is False for r in records)
    anchored = lse.Anchor(tmp_path / "ledger.anchor.jsonl").records()
    assert [a["ledger_head_sha256"] for a in anchored] == [lse.sha256_bytes(x) for x in lines]
    assert open_ledger(tmp_path).head == ledger.head  # reload verifies chain + anchor
    with pytest.raises(ValueError, match="anchor"):
        lse.Ledger(path, anchor=None)

    original = path.read_bytes()
    path.write_bytes(original.replace(b'"run_id":"r1"', b'"run_id":"r9"'))
    with pytest.raises(ValueError, match="chain"):
        open_ledger(tmp_path)
    # an edit of the last record is invisible to the chain, not to the anchor
    path.write_bytes(original.replace(b'"reason":"test"', b'"reason":"tost"'))
    with pytest.raises(ValueError, match="differs from its anchor"):
        open_ledger(tmp_path)
    path.write_bytes(original.replace(b'"reason":"test"', b'"reason": "test"'))
    with pytest.raises(ValueError, match="canonical"):
        open_ledger(tmp_path)
    path.write_bytes(b"\n".join([lines[0], lines[2], lines[1]]) + b"\n")  # reordered
    with pytest.raises(ValueError, match="chain"):
        open_ledger(tmp_path)
    path.write_bytes(original[:-1])  # cut inside a line
    with pytest.raises(ValueError, match="truncated"):
        open_ledger(tmp_path)
    path.write_bytes(original)
    ledger.verify()

    record = dict(json.loads(lines[2]), promotion_allowed=True)
    with pytest.raises(ValueError, match="never allows promotion"):
        ledger._append(record)


def test_truncation_at_a_line_boundary_and_run_counter_reset_are_refused(tmp_path):
    ledger = new_ledger(tmp_path)
    catalog = lse.synthetic_catalog()
    for k in (1, 2):
        lse.allocate(ledger, catalog, lse.Policy(budget=21), run_id=f"r{k}",
                     windows=window(k), recorded_at=FIXED)
        lse.abandon(ledger, "x", recorded_at=FIXED)
    path = tmp_path / "ledger.jsonl"
    original = path.read_bytes()
    lines = original.split(b"\n")[:-1]
    assert len(lines) == 5
    # a valid chain, cut at a line boundary: the chain alone accepts it
    path.write_bytes(b"\n".join(lines[:3]) + b"\n")
    lse.verify_chain(lines[:3])
    with pytest.raises(ValueError, match="truncated, extended or restarted"):
        open_ledger(tmp_path)
    with pytest.raises(ValueError, match="changed outside"):
        ledger.verify()
    path.write_bytes(original)

    # a fresh genesis against the same anchor would restart k at 1: refused
    path.unlink()
    with pytest.raises(ValueError, match="reset its run counter"):
        new_ledger(tmp_path)
    with pytest.raises(ValueError, match="pins ledger 's11-test'"):
        new_ledger(tmp_path, ledger_id="another-ledger")
    assert not path.exists()  # nothing was written
    # a fresh ledger file under the old anchor does not open either
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    other = new_ledger(elsewhere)
    assert other.genesis["ledger_id"] == "s11-test"
    path.write_bytes((elsewhere / "ledger.jsonl").read_bytes())
    with pytest.raises(ValueError, match="truncated, extended or restarted"):
        open_ledger(tmp_path)
    path.write_bytes(original)
    reopened = open_ledger(tmp_path)
    nxt = lse.allocate(reopened, catalog, lse.Policy(budget=21), run_id="r3",
                       windows=window(3), recorded_at=FIXED)
    assert nxt["record"]["run_index"] == 3
    assert nxt["record"]["alpha"] == pytest.approx(0.10 / 12)


def test_genesis_freezes_the_global_level_and_the_catalog(tmp_path):
    catalog = lse.synthetic_catalog()
    with pytest.raises(ValueError):
        lse.Ledger.create(None, catalog=catalog, ledger_id="x", q=0.2)
    with pytest.raises(ValueError):
        lse.Ledger.create(None, catalog=catalog, ledger_id="", q=0.1)
    with pytest.raises(ValueError, match="anchor"):
        lse.Ledger.create(tmp_path / "l.jsonl", catalog=catalog, ledger_id="x")
    ledger = new_ledger(q=0.05)
    assert ledger.genesis["spending"] == lse.SPENDING
    assert ledger.genesis["within_run"] == "holm"
    assert ledger.catalog_sha256 == catalog.sha256()
    assert lse.CANONICAL_LEDGER_ID == lse.Ledger.create(
        None, catalog=catalog).genesis["ledger_id"]


def test_relabelled_classes_cannot_re_test_the_same_trials_on_a_window():
    """Review B2: renaming classes used to re-open identical family/feature trials."""
    catalog = lse.synthetic_catalog()
    ledger = new_ledger(catalog=catalog)
    policy = lse.Policy(budget=100)  # every identity in one run
    first = lse.allocate(ledger, catalog, policy, run_id="r1", windows=window(1),
                         recorded_at=FIXED)
    lse.abandon(ledger, "x", recorded_at=FIXED)
    relabelled = lse.Catalog(
        families=catalog.families,
        features=catalog.features,
        classes=tuple((f, "renamed" + c) for f, c in catalog.classes),
        self_lag=catalog.self_lag,
    )
    with pytest.raises(ValueError, match="frozen at the ledger's genesis"):
        lse.allocate(ledger, relabelled, policy, run_id="r2", windows=window(1),
                     recorded_at=FIXED)
    # and the registry itself is keyed on feature x target x horizon, not on labels
    touched = ledger.touched_windows()
    assert set(touched) == {t["identity"] for t in first["record"]["trials"]}
    assert all("::" not in key for key in touched)
    with pytest.raises(ValueError, match="no eligible family"):
        lse.allocate(ledger, catalog, policy, run_id="r2", windows=window(1),
                     recorded_at=FIXED)


def test_spending_sequence_never_exceeds_q():
    q = 0.10
    alphas = [lse.run_alpha(k, q) for k in range(1, 100001)]
    assert math.isclose(sum(alphas), q * 100000 / 100001, rel_tol=1e-9)
    assert sum(alphas) < q
    ledger = new_ledger(q=q)
    catalog = lse.synthetic_catalog()
    for k in range(1, 6):
        allocation = lse.allocate(
            ledger, catalog, lse.Policy(budget=21), run_id=f"r{k}", windows=window(k),
            recorded_at=FIXED,
        )
        assert allocation["record"]["alpha"] == pytest.approx(q / (k * (k + 1)))
        lse.abandon(ledger, "abandoned runs still spend alpha", recorded_at=FIXED)
    assert ledger.alpha_spent() == pytest.approx(q * 5 / 6)


# --- allocation frozen before data --------------------------------------------------


def test_allocation_is_frozen_into_the_protocol_before_data(tmp_path):
    ledger = new_ledger(tmp_path)
    catalog = lse.synthetic_catalog()
    epoch = lse.synthetic_epoch(catalog, 1)
    allocation = lse.allocate(
        ledger, catalog, lse.Policy(budget=21), run_id="r1", windows=epoch.windows,
        recorded_at=FIXED,
    )
    record = allocation["record"]
    assert ledger.line_sha(record["seq"]) == allocation["sha256"]
    assert record["catalog_sha256"] == catalog.sha256()
    protocol = lse.protocol_for_allocation(
        allocation, origin="synthetic_fixture", perms=999, statistic="pearson"
    )
    assert protocol.selection == "ledger_holm"
    assert protocol.selection_alpha == record["alpha"] == 0.05
    assert protocol.allocation_sha256 == allocation["sha256"]
    assert protocol.trials == tuple((t["family"], t["feature"]) for t in record["trials"])
    assert (protocol.start, protocol.split, protocol.end) == tuple(
        epoch.windows[k] for k in ("start", "split", "end")
    )
    for fixed in ("trials", "selection_alpha", "split", "allocation_sha256"):
        with pytest.raises(ValueError, match="fixed by the allocation"):
            lse.protocol_for_allocation(allocation, **{fixed: None})

    rows = {
        family: build_family_rows(
            protocol, epoch.features, epoch.levels[family[:2]], 1, "discovery", "change"
        )
        for family in protocol.families
    }
    frozen = discover(protocol, rows)
    payload = frozen["payload"]
    assert {(t["family"], t["feature"]) for t in payload["ledger"]} == set(protocol.trials)
    assert payload["trial_count"] == len(protocol.trials) == record["trial_count"]
    assert "Holm at ledger run alpha" in payload["method"]
    adjusted = holm_adjusted([t["p"] for t in payload["ledger"]])
    assert [t["adjusted_p"] for t in payload["ledger"]] == adjusted
    assert [t["selected"] for t in payload["ledger"]] == [
        t["status"] == "tested" and a <= protocol.selection_alpha
        for t, a in zip(payload["ledger"], adjusted)
    ]
    for trial in payload["ledger"]:
        assert trial["trial_id"] == lse.trial_id("r1", trial["family"], trial["feature"])
    # the payload round-trips its declared trials
    assert protocol_from_payload(payload["protocol"]) == protocol


def test_holm_adjusted_matches_the_step_down_definition():
    rng = np.random.default_rng(3)
    for _ in range(200):
        p = rng.uniform(size=7) ** rng.uniform(1, 4)
        alpha = 0.05
        order = np.argsort(p)
        rejected = set()
        for i, j in enumerate(order):
            if p[j] > alpha / (len(p) - i):
                break
            rejected.add(int(j))
        assert {i for i, a in enumerate(holm_adjusted(p)) if a <= alpha} == rejected
    assert holm_adjusted([]) == []
    with pytest.raises(ValueError):
        holm_adjusted([1.5])


def test_contract_refuses_malformed_ledger_protocols():
    base = {
        "run_id": "x",
        "features": ("f1", "f2"),
        "families": ("T|change|fwd1",),
        "split": "2020-01-01T00:00:00+00:00",
        "end": "2021-01-01T00:00:00+00:00",
        "start": "2019-01-01T00:00:00+00:00",
    }
    ledger_run = dict(
        base,
        trials=(("T|change|fwd1", "f1"),),
        selection="ledger_holm",
        selection_alpha=0.05,
        allocation_sha256="a" * 64,
    )
    Protocol(**ledger_run).validate()
    Protocol(**base).validate()  # the default per-run BH contract is unchanged
    for bad in (
        {"selection_alpha": 0.0},
        {"selection_alpha": 0.2},
        {"allocation_sha256": "z" * 64},
        {"trials": ()},
        {"start": ""},
        {"trials": (("T|change|fwd1", "f9"),)},
        {"trials": (("T|change|fwd1", "f1"),) * 2},
        {"selection": "lord"},
        {"self_lag": (("T|change|fwd1", "f1"),)},
    ):
        with pytest.raises(ValueError):
            Protocol(**{**ledger_run, **bad}).validate()
    with pytest.raises(ValueError, match="only ledger_holm"):
        Protocol(**base, selection_alpha=0.05).validate()


# --- SELF_LAG, exclusions, windows ----------------------------------------------------


def test_self_lag_families_are_never_allocated_on_the_real_panel_universe():
    from scripts.run_real_panel_scan import FEATURES, HORIZONS, TARGETS

    catalog = lse.catalog_from_specs(FEATURES, TARGETS, HORIZONS)
    assert len(catalog.features) == 87 and len(catalog.families) == 12
    assert len(catalog.self_lag) == 153  # as the S09b scan declares
    arms = catalog.arms()
    self_lag_arms = [k for k in arms if k.startswith("SELF_LAG::")]
    assert self_lag_arms and all("never" in arms[k] for k in self_lag_arms)
    ledger = new_ledger(catalog=catalog, ledger_id="real-panel-universe")
    allocation = lse.allocate(
        ledger, catalog, lse.Policy(budget=5000), run_id="all",
        windows=window(1), recorded_at=FIXED,
    )
    declared = {(t["family"], t["feature"]) for t in allocation["record"]["trials"]}
    assert not declared & set(catalog.self_lag)
    assert len(declared) == 12 * 87 - 153  # every other trial fits this budget
    for key in self_lag_arms:
        assert allocation["record"]["arms"][key]["eligible"] is False
        assert "count" not in allocation["record"]["arms"][key]
    protocol = lse.protocol_for_allocation(
        allocation, origin="latest_vintage_read", read_receipt="a" * 64
    )
    # the panel's own self_lag check (verify_latest_vintage_rows) would pass
    from analysis.research_real_panel import self_lag_pairs

    assert protocol.self_lag == self_lag_pairs(protocol.families, protocol.features)


def test_excluded_telemetry_never_allocated():
    catalog = lse.Catalog(
        families=("T|change|fwd1",),
        features=("snap:llm_tasks", "x1"),
        classes=(("snap:llm_tasks", "tele"), ("x1", "macro")),
    )
    arms = catalog.arms()
    assert "never" in arms["tele::T|change|fwd1"]
    allocation = lse.allocate(
        new_ledger(catalog=catalog), catalog, lse.Policy(budget=10), run_id="r",
        windows=window(1),
        recorded_at=FIXED,
    )
    assert [t["feature"] for t in allocation["record"]["trials"]] == ["x1"]
    with pytest.raises(ValueError):
        lse.Catalog(
            families=("T|change|fwd1",), features=("x1",), classes=(("x1", "SELF_LAG"),)
        ).validate()


def test_holdout_windows_are_single_use_per_identity():
    ledger = new_ledger()
    catalog = lse.synthetic_catalog()
    policy = lse.Policy(budget=8)  # floor only: about one trial per arm
    first = lse.allocate(ledger, catalog, policy, run_id="r1", windows=window(1),
                         recorded_at=FIXED)
    touched = {t["identity"] for t in first["record"]["trials"]}
    lse.abandon(ledger, "abandoned runs still consume their windows", recorded_at=FIXED)
    overlapping = {
        "start": window(1)["split"],
        "split": window(1)["end"],
        "end": (pd.Timestamp(window(1)["end"]) + pd.Timedelta(days=90)).isoformat(),
    }
    second = lse.allocate(ledger, catalog, policy, run_id="r2", windows=overlapping,
                          recorded_at=FIXED)
    declared = {t["identity"] for t in second["record"]["trials"]}
    assert declared and not declared & touched  # untouched identities only
    arms = second["record"]["arms"]
    assert arms[PLANTED]["stale"] >= 1
    # the own-change arm has a single identity, touched in r1: now ineligible
    assert arms["own::T2|change|fwd1"]["eligible"] is False
    assert "no fresh window" in arms["own::T2|change|fwd1"]["ineligible"]
    lse.abandon(ledger, "x", recorded_at=FIXED)
    fresh = lse.allocate(ledger, catalog, policy, run_id="r3", windows=window(2),
                         recorded_at=FIXED)
    assert arms[PLANTED]["eligible"] and fresh["record"]["arms"]["own::T2|change|fwd1"][
        "eligible"
    ]
    lse.abandon(ledger, "x", recorded_at=FIXED)
    everything = lse.allocate(ledger, catalog, lse.Policy(budget=100), run_id="r4",
                              windows=window(3), recorded_at=FIXED)
    assert everything["record"]["trial_count"] == 37  # 2 x 19 identities minus 1 self_lag
    lse.abandon(ledger, "x", recorded_at=FIXED)
    with pytest.raises(ValueError, match="no eligible family"):
        lse.allocate(ledger, catalog, policy, run_id="r5", windows=window(3),
                     recorded_at=FIXED)


def test_one_open_allocation_at_a_time_and_unique_run_ids():
    ledger = new_ledger()
    catalog = lse.synthetic_catalog()
    policy = lse.Policy(budget=21)
    lse.allocate(ledger, catalog, policy, run_id="r1", windows=window(1), recorded_at=FIXED)
    with pytest.raises(ValueError, match="still open"):
        lse.allocate(ledger, catalog, policy, run_id="r2", windows=window(2),
                     recorded_at=FIXED)
    lse.abandon(ledger, "x", recorded_at=FIXED)
    with pytest.raises(ValueError, match="no open allocation"):
        lse.abandon(ledger, "x", recorded_at=FIXED)
    with pytest.raises(ValueError, match="unique"):
        lse.allocate(ledger, catalog, policy, run_id="r1", windows=window(2),
                     recorded_at=FIXED)
    with pytest.raises(ValueError, match="floor"):
        lse.allocate(ledger, catalog, lse.Policy(budget=3), run_id="r3",
                     windows=window(3), recorded_at=FIXED)


# --- recording results --------------------------------------------------------------


def test_record_run_refuses_anything_but_the_allocated_run(tmp_path):
    ledger = new_ledger(tmp_path)
    catalog = lse.synthetic_catalog()
    epoch = lse.synthetic_epoch(catalog, 1)
    allocation = lse.allocate(
        ledger, catalog, lse.Policy(budget=21), run_id="r1", windows=epoch.windows,
        recorded_at=FIXED,
    )
    protocol = lse.protocol_for_allocation(
        allocation, origin="synthetic_fixture", perms=999, statistic="pearson"
    )

    def rows(p, window_name):
        return {
            f: build_family_rows(p, epoch.features, epoch.levels[f[:2]], 1, window_name,
                                 "change")
            for f in p.families
        }

    result = run_proof(protocol, rows(protocol, "discovery"), rows(protocol, "holdout"),
                       tmp_path / "run")
    frozen = json.loads((tmp_path / "run" / "discovery-frozen.json").read_text())

    tampered = json.loads(json.dumps(frozen))
    tampered["payload"]["ledger"][0]["p"] = 0.5
    with pytest.raises(ValueError, match="changed"):
        lse.record_run(ledger, tampered, result)

    # a re-signed manifest with a different alpha is not the allocated run
    resigned = json.loads(json.dumps(frozen))
    resigned["payload"]["protocol"]["selection_alpha"] = 0.1
    resigned["sha256"] = digest(resigned["payload"])
    with pytest.raises(ValueError, match="holdout result"):
        lse.record_run(ledger, resigned, result)
    with pytest.raises(ValueError, match="open allocation"):
        lse.record_run(ledger, resigned, dict(result, discovery_manifest=resigned["sha256"]))

    # a manifest over a trial subset differs from the allocation
    subset = Protocol(**{**protocol.__dict__, "trials": protocol.trials[:-1]})
    frozen_subset = discover(subset, rows(subset, "discovery"))
    with pytest.raises(ValueError, match="trials differ"):
        lse.record_run(ledger, frozen_subset, dict(result,
                       discovery_manifest=frozen_subset["sha256"]))

    recorded = lse.record_run(ledger, frozen, result, recorded_at=FIXED)["record"]
    assert recorded["allocation_sha256"] == allocation["sha256"]
    assert len(recorded["trials"]) == allocation["record"]["trial_count"]
    assert {t["status"] for t in recorded["trials"]} <= {"tested", "untestable"}
    assert recorded["selected"] == sum(t["selected"] for t in frozen["payload"]["ledger"])
    for trial in recorded["trials"]:
        assert trial["holdout_outcome"] in lse.HOLDOUT_OUTCOMES
        assert (trial["candidate_sha256"] is not None) == (
            trial["holdout_outcome"] == "survived"
        )
    with pytest.raises(ValueError, match="no open allocation"):
        lse.record_run(ledger, frozen, result)
    assert open_ledger(tmp_path).head == ledger.head


# --- done-when: two consecutive dry runs shift the allocation ---------------------------


def test_two_dry_runs_shift_budget_to_the_planted_family(tmp_path):
    summary = lse.run_synthetic_dry_runs(tmp_path / "dry", runs=2)
    first, second = summary["steps"]
    noise = [k for k in first["counts"] if k != PLANTED and not k.startswith("SELF_LAG")]
    # run 1: no ledger outcomes -> identical posteriors -> identical shares
    assert len({round(first["p_best"][k], 12) for k in noise + [PLANTED]}) == 1
    assert first["counts"][PLANTED] == 3
    assert all(first["counts"][k] == (1 if k.startswith("own") else 3) for k in noise)
    # run 1 outcomes: only the planted family yields holdout survivors
    ledger = open_ledger(tmp_path / "dry")
    first_results = ledger.of_kind("run_result")[0]["trials"]
    assert {t["family_key"] for t in first_results if t["selected"]} == {PLANTED}
    assert first["survived"] == 3
    # run 2: the planted family gains budget, every pure-noise family loses or holds
    # at its floor, and the run spends fewer trials (a larger Holm level per trial)
    assert second["counts"][PLANTED] == 6 > first["counts"][PLANTED]
    assert second["p_best"][PLANTED] > 0.8
    assert all(second["counts"][k] <= first["counts"][k] for k in noise)
    assert sum(second["counts"][k] for k in noise) < sum(first["counts"][k] for k in noise)
    assert all(second["p_best"][k] < first["p_best"][k] for k in noise)
    assert second["counts"]["SELF_LAG::T1|change|fwd1"] == 0
    # nothing false was discovered, and every record forbids promotion
    assert summary["ledger"]["discoveries"] == first["selected"] + second["selected"]
    second_results = ledger.of_kind("run_result")[1]["trials"]
    assert all(
        t["family_key"] == PLANTED for t in first_results + second_results if t["selected"]
    )
    assert all(r["promotion_allowed"] is False for r in ledger.records)
    assert (tmp_path / "dry" / "summary.json").exists()
    assert (tmp_path / "dry" / "run-02" / "discovery-frozen.json").exists()


# --- cross-run error control ---------------------------------------------------------


def test_twenty_noise_runs_end_to_end_accumulate_no_false_discoveries(tmp_path):
    """Full loop (allocate -> contract -> record) on 20 fresh epochs of pure noise."""
    runs = 20
    summary = lse.run_synthetic_dry_runs(
        tmp_path / "noise", runs=runs, planted=None, perms_cap=2999
    )
    ledger = open_ledger(tmp_path / "noise")
    results = ledger.of_kind("run_result")
    assert len(results) == runs
    assert summary["ledger"]["discoveries"] == 0  # every discovery would be false
    assert summary["ledger"]["alpha_spent"] == pytest.approx(0.10 * runs / (runs + 1))
    assert summary["ledger"]["alpha_spent"] < 0.10
    # (the leak of per-run BH without cross-run accounting is shown by the
    # Monte Carlo below; one fixed-seed sequence cannot show a rate)
    assert any(r["resolution_limited"] for r in results)  # recorded honestly


def simulate(rng, runs, q, rho, planted_shift=0.0):
    """One ledger sequence with dependent Gaussian p-values; returns counts per run."""
    catalog = lse.synthetic_catalog()
    ledger = lse.Ledger.create(None, catalog=catalog, ledger_id="mc", q=q,
                               recorded_at=FIXED)
    policy = lse.Policy(budget=21, grid=512)
    false, true, naive_false, planted_counts = 0, 0, 0, []
    for k in range(1, runs + 1):
        allocation = lse.allocate(
            ledger, catalog, policy, run_id=f"mc{k}", windows=window(k), recorded_at=FIXED
        )
        record = allocation["record"]
        trials = record["trials"]
        signal = np.array([planted_shift * (t["family_key"] == PLANTED) for t in trials])
        # equicorrelated z-scores: every trial in a run shares one factor
        z = math.sqrt(rho) * rng.normal() + math.sqrt(1 - rho) * rng.normal(size=len(trials))
        p = 2 * norm.sf(np.abs(z + signal))
        selected = np.array(holm_adjusted(p)) <= record["alpha"]
        holdout_p = 2 * norm.sf(np.abs(rng.normal(size=len(trials)) + signal))
        survived = selected & (holdout_p * max(1, selected.sum()) <= 0.05)
        null = signal == 0
        false += int((selected & null).sum())
        true += int((selected & ~null).sum())
        naive_false += int(((np.array(bh_adjusted(p)) <= q) & null).sum())
        planted_counts.append(int((~null).sum()))
        entries = [
            {
                **t,
                "status": "tested",
                "p": float(pv),
                "selected": bool(s),
                "holdout_outcome": "not_selected" if not s else (
                    "survived" if v else "failed"
                ),
            }
            for t, pv, s, v in zip(trials, p, selected, survived)
        ]
        lse._append_run_result(ledger, record, allocation["sha256"], entries, {}, FIXED)
    return {"false": false, "true": true, "naive_false": naive_false,
            "planted_counts": planted_counts}


@pytest.mark.parametrize("rho", [0.0, 0.5])
def test_monte_carlo_cross_run_error_stays_within_q_on_pure_noise(rho):
    """P(any false discovery over 20 runs) <= q; per-run BH on the same p-values leaks."""
    q, runs, replicates = 0.10, 20, 200
    rng = np.random.default_rng([20260926, int(rho * 10)])
    sims = [simulate(rng, runs, q, rho) for _ in range(replicates)]
    fwer = np.mean([s["false"] > 0 for s in sims])
    naive_fwer = np.mean([s["naive_false"] > 0 for s in sims])
    mean_false = np.mean([s["false"] for s in sims])
    assert fwer <= q * runs / (runs + 1)  # the nominal cumulative bound
    assert mean_false <= q
    assert naive_fwer > 0.5  # without cross-run accounting the errors accumulate


def test_monte_carlo_fdr_with_a_planted_family_and_allocation_follows_it():
    q, runs, replicates = 0.10, 20, 150
    rng = np.random.default_rng(11)
    sims = [simulate(rng, runs, q, 0.3, planted_shift=5.0) for _ in range(replicates)]
    fdp = [s["false"] / max(1, s["false"] + s["true"]) for s in sims]
    assert np.mean(fdp) <= q
    assert np.mean([s["true"] for s in sims]) > 3  # the budget still finds the signal
    counts = np.array([s["planted_counts"] for s in sims])
    assert counts[:, 0].mean() == 3
    assert counts[:, 1:].mean() > 5  # the planted family keeps (nearly) its full pool


# --- forward-log input (S10 interface) ----------------------------------------------


def forward_line(trial, **overrides):
    entry = {
        "trial_id": trial["trial_id"],
        "candidate_sha256": trial["candidate_sha256"],
        "outcome": "pass",
        "n": 40,
        "evaluated_through": "2027-03-01T00:00:00+00:00",
        "prereg_sha256": "b" * 64,
    }
    return json.dumps({**entry, **overrides})


def write_lines(target, *lines):
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target


def dry_ledger(tmp_path):
    lse.run_synthetic_dry_runs(tmp_path / "dry", runs=1)
    ledger = open_ledger(tmp_path / "dry")
    survivors = [t for t in ledger.trial_results() if t["holdout_outcome"] == "survived"]
    losers = [t for t in ledger.trial_results() if t["holdout_outcome"] != "survived"]
    run_end = ledger.allocations()[0]["windows"]["end"]
    return ledger, survivors, losers, pd.Timestamp(run_end)


def test_forward_outcomes_feed_the_ledger(tmp_path):
    ledger, survivors, losers, run_end = dry_ledger(tmp_path)
    assert len(survivors) == 3
    through = (run_end + pd.Timedelta(days=120)).isoformat()

    before = ledger.outcomes()["families"][PLANTED]
    for bad in (
        forward_line(losers[0], candidate_sha256=None),
        forward_line(survivors[0], candidate_sha256="c" * 64),
        forward_line(survivors[0], outcome="pending"),
        forward_line(survivors[0], outcome="FORWARD_SUPPORTED_REVIEW_REQUIRED"),
        forward_line(survivors[0], n=0),
        forward_line(survivors[0], prereg_sha256="short"),
        forward_line(survivors[0], evaluated_through=(run_end - pd.Timedelta(days=1))
                     .isoformat()),
        json.dumps({"trial_id": survivors[0]["trial_id"]}),
    ):
        with pytest.raises(ValueError):
            lse.ingest_forward_outcomes(ledger, write_lines(tmp_path / "bad.jsonl", bad))
    with pytest.raises(ValueError, match="already"):
        lse.ingest_forward_outcomes(
            ledger,
            write_lines(tmp_path / "dup.jsonl", forward_line(survivors[0]),
                        forward_line(survivors[0])),
        )
    assert ledger.of_kind("forward_outcome") == []  # all-or-nothing

    source = write_lines(
        tmp_path / "fwd.jsonl",
        forward_line(survivors[0], evaluated_through=through),
        forward_line(survivors[1], outcome="fail", evaluated_through=through),
        forward_line(survivors[2], outcome="inconclusive", n=12, evaluated_through=through),
    )
    assert lse.ingest_forward_outcomes(ledger, source, recorded_at=FIXED) == 3
    after = ledger.outcomes()["families"][PLANTED]
    # pass +1 success, fail +1 failure, inconclusive: no score update
    assert after["successes"] == before["successes"] + 1
    assert after["failures"] == before["failures"] + 1
    recorded = ledger.of_kind("forward_outcome")
    assert {r["source_sha256"] for r in recorded} == {lse.sha256_bytes(source.read_bytes())}
    assert [r["scored"] for r in recorded] == [True, True, False]
    # every verdict, inconclusive included, touches its forward window
    touched = ledger.touched_windows()
    for trial in survivors:
        assert {"start": run_end.isoformat(), "end": through, "closed": True,
                "source": "forward"} in touched[lse.identity(trial["family"], trial["feature"])]
    with pytest.raises(ValueError, match="already"):
        lse.ingest_forward_outcomes(
            ledger, write_lines(tmp_path / "again.jsonl", forward_line(survivors[0]))
        )
    assert open_ledger(tmp_path / "dry").head == ledger.head
    summary = lse.summarize(ledger)
    assert summary["forward_verdicts"] == {"pass": 1, "fail": 1, "inconclusive": 1}


def test_forward_data_is_never_re_used_by_a_later_run(tmp_path):
    """Review B1: a forward pass must not be followed by a run on the same data."""
    ledger, survivors, _, run_end = dry_ledger(tmp_path)
    catalog = lse.synthetic_catalog()
    passed = survivors[0]
    passed_identity = lse.identity(passed["family"], passed["feature"])
    through = run_end + pd.Timedelta(days=120)
    lse.ingest_forward_outcomes(
        ledger,
        write_lines(tmp_path / "fwd.jsonl",
                    forward_line(passed, evaluated_through=through.isoformat())),
        recorded_at=FIXED,
    )
    covering = {  # a later run whose labels cover the forward period
        "start": (run_end + pd.Timedelta(days=10)).isoformat(),
        "split": (run_end + pd.Timedelta(days=200)).isoformat(),
        "end": (run_end + pd.Timedelta(days=300)).isoformat(),
    }
    policy = lse.Policy(budget=100)
    later = lse.allocate(ledger, catalog, policy, run_id="covering", windows=covering,
                         recorded_at=FIXED)
    declared = {t["identity"] for t in later["record"]["trials"]}
    assert passed_identity not in declared
    others = {lse.identity(t["family"], t["feature"]) for t in survivors[1:]}
    assert others <= declared  # survivors without a forward verdict stay testable
    assert later["record"]["arms"][PLANTED]["stale"] == 1
    lse.abandon(ledger, "x", recorded_at=FIXED)
    after = {  # labels strictly after evaluated_through (and after "covering")
        "start": (run_end + pd.Timedelta(days=301)).isoformat(),
        "split": (run_end + pd.Timedelta(days=500)).isoformat(),
        "end": (run_end + pd.Timedelta(days=600)).isoformat(),
    }
    fresh = lse.allocate(ledger, catalog, policy, run_id="after", windows=after,
                         recorded_at=FIXED)
    assert passed_identity in {t["identity"] for t in fresh["record"]["trials"]}


def test_a_verdict_on_data_a_later_run_already_used_does_not_score(tmp_path):
    ledger, survivors, _, run_end = dry_ledger(tmp_path)
    catalog = lse.synthetic_catalog()
    covering = {
        "start": (run_end + pd.Timedelta(days=10)).isoformat(),
        "split": (run_end + pd.Timedelta(days=200)).isoformat(),
        "end": (run_end + pd.Timedelta(days=300)).isoformat(),
    }
    lse.allocate(ledger, catalog, lse.Policy(budget=100), run_id="covering",
                 windows=covering, recorded_at=FIXED)
    lse.abandon(ledger, "x", recorded_at=FIXED)
    before = ledger.outcomes()["families"][PLANTED]
    through = (run_end + pd.Timedelta(days=120)).isoformat()
    lse.ingest_forward_outcomes(
        ledger,
        write_lines(tmp_path / "fwd.jsonl",
                    forward_line(survivors[0], evaluated_through=through)),
        recorded_at=FIXED,
    )
    verdict = ledger.of_kind("forward_outcome")[0]
    assert verdict["scored"] is False
    assert "covering" in verdict["unscored_reason"]
    assert ledger.outcomes()["families"][PLANTED] == before
    assert lse.summarize(ledger)["forward_unscored"] == 1


# --- hard limits --------------------------------------------------------------------


def test_module_is_files_and_ledger_only():
    source = Path(lse.__file__).read_text(encoding="utf-8")
    code = source.split('"""', 2)[2].lower()  # skip the module docstring
    for forbidden in (
        "discovered_hypotheses",
        "hypothesis_registry",
        "scanner_weights",
        "sqlalchemy",
        "insert ",
        "promotion_allowed\": true",
        "requests",
    ):
        assert forbidden not in code
    assert not re.search(r"(?<![\w.])text\(", code)  # no SQL text(); read_text is fine
    assert "promotion_allowed" in code


def test_split_budget_properties():
    shares = {"a": 0.7, "b": 0.2, "c": 0.1}
    capacity = {"a": 3, "b": 10, "c": 10}
    counts = lse.split_budget(shares, capacity, budget=20, floor=1)
    assert counts["a"] == 3  # capped at its pool
    assert counts["b"] > counts["c"] >= 1
    assert sum(counts.values()) <= 20
    equal = lse.split_budget(dict.fromkeys("abc", 1 / 3), dict.fromkeys("abc", 10), 9, 1)
    assert equal == dict.fromkeys("abc", 3)
    for x, y in combinations("abc", 2):
        assert lse.probability_best({x: (1, 1), y: (1, 1)}, 1024) == pytest.approx(
            {x: 0.5, y: 0.5}
        )
    best = lse.probability_best({"a": (10, 1), "b": (1, 10)}, 2048)
    assert best["a"] > 0.99
