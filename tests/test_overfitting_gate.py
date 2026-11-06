"""W2-3 / S11: mandatory CPCV, PBO and deflated-Sharpe gate for research candidates.

* CPCV purge and embargo are exact on synthetic overlapping labels.
* PBO (CSCV) is near 0.5 on pure-noise trial matrices and low on a planted signal.
* The S11 gate takes its trial matrix only from the S11 ledger: missing or
  extra trials, unledgered candidates, open allocations and look-ahead rows
  are refused, and every result (pass, fail, refused) is kept in the ledger.
"""

from __future__ import annotations

import math
from itertools import combinations

import numpy as np
import pandas as pd
import pytest

import analysis.ledger_steered_exploration as lse
from alpha_research.validation import overfitting as ovf
from analysis.offline_research_proof import digest

FIXED = "2026-11-06T00:00:00+00:00"
LATER = "2026-11-07T00:00:00+00:00"


# --- CPCV ---------------------------------------------------------------------------


def test_cpcv_n10_k2_has_45_splits_and_9_paths():
    splits = ovf.cpcv_splits(1000, 10, 2)
    paths = ovf.cpcv_paths(10, 2)
    assert len(splits) == 45 == math.comb(10, 2)
    assert paths.shape == (9, 10)
    for p in range(9):
        for g in range(10):
            assert g in splits[paths[p, g]].test_groups  # the split really tests g
    # every (split, test group) cell is used by exactly one path
    cells = {(int(paths[p, g]), g) for p in range(9) for g in range(10)}
    assert len(cells) == 90 == sum(len(s.test_groups) for s in splits)
    for split in splits:  # train, test, purged and embargoed partition the rows
        parts = np.concatenate([split.train, split.test, split.purged, split.embargoed])
        assert sorted(parts.tolist()) == list(range(1000))


def test_cpcv_purge_and_embargo_by_hand():
    """20 rows, 4 groups of 5, labels span 3 rows (depth 2), embargo 2."""
    splits = ovf.cpcv_splits(20, 4, 1, overlap_depth=2, embargo=2)
    split = next(s for s in splits if s.test_groups == (1,))  # test rows 5..9
    assert split.test.tolist() == [5, 6, 7, 8, 9]
    # 3 and 4 have labels reaching row 5; 10 and 11 start inside the last test label (ends at 11)
    assert split.purged.tolist() == [3, 4, 10, 11]
    assert split.embargoed.tolist() == [12, 13]  # after the test run's last label end only
    assert split.train.tolist() == [0, 1, 2] + list(range(14, 20))


def test_cpcv_embargo_is_only_after_the_test_set():
    split = next(
        s for s in ovf.cpcv_splits(20, 4, 1, overlap_depth=0, embargo=3) if s.test_groups == (1,)
    )
    assert split.purged.tolist() == []
    assert split.embargoed.tolist() == [10, 11, 12]
    assert {3, 4} <= set(split.train.tolist())


def test_cpcv_adjacent_test_groups_form_one_run():
    split = next(
        s for s in ovf.cpcv_splits(20, 4, 2, overlap_depth=2, embargo=2) if s.test_groups == (1, 2)
    )
    assert split.test.tolist() == list(range(5, 15))
    assert split.purged.tolist() == [3, 4, 15, 16]
    assert split.embargoed.tolist() == [17, 18]


def brute_force(n, test_rows, ends, embargo):
    """Independent purge / embargo: per test row, never via contiguous runs."""
    test = set(test_rows)
    purged = {
        j
        for j in range(n)
        if j not in test and any(j <= ends[i] and i <= ends[j] for i in test)
    }
    embargoed = set()
    for i in test:
        if i + 1 not in test:  # last row of a contiguous test run
            start = i
            while start - 1 in test:
                start -= 1
            last_end = max(ends[k] for k in range(start, i + 1))
            embargoed |= {
                j
                for j in range(last_end + 1, last_end + embargo + 1)
                if j < n and j not in test and j not in purged
            }
    return purged, embargoed


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_cpcv_matches_brute_force_on_irregular_overlapping_labels(seed):
    rng = np.random.default_rng(seed)
    n = 120
    ends = np.minimum(np.arange(n) + rng.integers(0, 7, n), n - 1)  # uneven label spans
    splits = ovf.cpcv_splits(n, 6, 2, label_end=ends, embargo=4)
    assert len(splits) == 15
    for split in splits:
        purged, embargoed = brute_force(n, split.test.tolist(), ends, 4)
        assert set(split.purged.tolist()) == purged
        assert set(split.embargoed.tolist()) == embargoed
        # no training label overlaps any test label
        for j in split.train:
            assert not any(j <= ends[i] and i <= ends[j] for i in split.test)


def test_cpcv_refuses_bad_label_ends():
    with pytest.raises(ovf.OverfitInputError):
        ovf.cpcv_splits(10, 5, 1, label_end=np.arange(10) - 1)
    with pytest.raises(ovf.OverfitInputError):
        ovf.cpcv_splits(10, 5, 5)


# --- PBO ----------------------------------------------------------------------------


def noise(seed, rows=640, trials=20):
    rng = np.random.default_rng(seed)
    index = pd.date_range("2001-01-01", periods=rows, freq="D", tz="UTC")
    return pd.DataFrame(
        rng.normal(0.0, 0.01, (rows, trials)),
        index=index,
        columns=[f"t{i:02d}" for i in range(trials)],
    )


def test_pbo_is_near_one_half_on_pure_noise():
    values = [ovf.probability_of_backtest_overfitting(noise(seed)).pbo for seed in range(24)]
    assert 0.4 <= float(np.mean(values)) <= 0.6
    assert ovf.probability_of_backtest_overfitting(noise(0)).n_combinations == math.comb(16, 8)


def test_pbo_is_low_on_a_planted_strong_signal():
    matrix = noise(3)
    matrix["t07"] += 0.002  # per-period Sharpe ~0.2 against pure noise
    result = ovf.probability_of_backtest_overfitting(matrix)
    assert result.pbo < 0.1
    assert result.prob_oos_loss < 0.05


def test_pbo_is_invariant_to_column_order_and_counts_gap():
    matrix = noise(5)
    a = ovf.probability_of_backtest_overfitting(matrix, gap=3)
    b = ovf.probability_of_backtest_overfitting(matrix[matrix.columns[::-1]], gap=3)
    assert a.pbo == pytest.approx(b.pbo)
    assert a.gap == 3 and a.rows_per_partition_min == 640 // 16 - 3


def test_pbo_matches_a_direct_cscv_computation():
    """The moment-sum shortcut equals the textbook loop over every half."""
    matrix = noise(9, rows=96, trials=6)
    values = matrix.to_numpy()
    blocks = np.array_split(np.arange(96), 8)
    logits = []
    for half in combinations(range(8), 4):
        ins = np.concatenate([blocks[b] for b in half])
        outs = np.setdiff1d(np.arange(96), ins)
        is_sr = values[ins].mean(0) / values[ins].std(0, ddof=1)
        oos_sr = values[outs].mean(0) / values[outs].std(0, ddof=1)
        best = int(np.argmax(is_sr))
        rank = (oos_sr < oos_sr[best]).sum() + 1
        omega = rank / 7
        logits.append(math.log(omega / (1 - omega)))
    expected = float(np.mean(np.asarray(logits) <= 0))
    assert ovf.probability_of_backtest_overfitting(matrix, 8).pbo == pytest.approx(expected)


def test_pbo_refuses_unsupported_inputs():
    with pytest.raises(ovf.OverfitInputError):
        ovf.probability_of_backtest_overfitting(noise(0), 15)
    with pytest.raises(ovf.OverfitInputError):
        ovf.probability_of_backtest_overfitting(noise(0, rows=40))
    with pytest.raises(ovf.OverfitInputError):
        ovf.probability_of_backtest_overfitting(noise(0)[["t00"]])
    bad = noise(0)
    bad.iloc[3, 2] = np.nan
    with pytest.raises(ovf.OverfitInputError, match="non-finite"):
        ovf.probability_of_backtest_overfitting(bad)


# --- DSR and config -----------------------------------------------------------------


def test_matrix_dsr_uses_ledger_trial_count_and_deflates_overlap():
    matrix = noise(1)
    matrix["t00"] += 0.002
    base = ovf.matrix_deflated_sharpe(matrix, "t00", n_trials=20)
    more = ovf.matrix_deflated_sharpe(matrix, "t00", n_trials=200)
    overlap = ovf.matrix_deflated_sharpe(matrix, "t00", n_trials=20, overlap_depth=4)
    assert base.n_trials == 20 and more.sr0 > base.sr0 and more.dsr < base.dsr
    assert overlap.n_observations == 640 // 5 and overlap.dsr < base.dsr
    with pytest.raises(ovf.OverfitInputError):
        ovf.matrix_deflated_sharpe(matrix, "t00", n_trials=5)


def test_thresholds_are_preregistered_and_only_stricter():
    config = ovf.OverfitGateConfig()
    config.validate()
    assert (config.pbo_max, config.dsr_min) == (0.5, 0.95)
    ovf.OverfitGateConfig(pbo_max=0.3, dsr_min=0.99).validate()
    for loose in (
        ovf.OverfitGateConfig(pbo_max=0.6),
        ovf.OverfitGateConfig(dsr_min=0.9),
        ovf.OverfitGateConfig(cscv_partitions=8),
        ovf.OverfitGateConfig(cpcv_groups=6),
    ):
        with pytest.raises(ovf.OverfitInputError):
            loose.validate()


def test_prereg_digest_is_pinned(monkeypatch):
    assert lse.overfit_gate_prereg_sha256() == digest(lse.OVERFIT_GATE_PREREG)
    monkeypatch.setitem(lse.OVERFIT_GATE_PREREG, "pbo", "pass iff PBO < 0.9")
    with pytest.raises(ValueError, match="pinned"):
        lse.overfit_gate_prereg_sha256()


# --- the S11 gate -------------------------------------------------------------------


def window(k: int, days: int = 400) -> dict:
    start = pd.Timestamp("2001-01-01", tz="UTC") + pd.Timedelta(days=k * days)
    return {
        "start": start.isoformat(),
        "split": (start + pd.Timedelta(days=days // 2)).isoformat(),
        "end": (start + pd.Timedelta(days=days - 1)).isoformat(),
    }


def run(ledger: lse.Ledger, k: int, *, untestable: int = 0) -> list[dict]:
    """One allocated and recorded synthetic run; returns its result trials."""
    allocation = lse.allocate(
        ledger, lse.synthetic_catalog(), lse.Policy(budget=12, grid=512),
        run_id=f"r{k}", windows=window(k), recorded_at=FIXED,
    )
    trials = allocation["record"]["trials"]
    entries = [
        {
            **t,
            "status": "untestable" if i < untestable else "tested",
            "p": 0.5,
            "selected": False,
            "holdout_outcome": "not_selected",
        }
        for i, t in enumerate(trials)
    ]
    lse._append_run_result(ledger, allocation["record"], allocation["sha256"], entries, {}, FIXED)
    return entries


def new_ledger(tmp_path=None) -> lse.Ledger:
    path = None if tmp_path is None else tmp_path / "ledger.jsonl"
    anchor = None if tmp_path is None else tmp_path / "ledger.anchor.jsonl"
    return lse.Ledger.create(
        path, catalog=lse.synthetic_catalog(), anchor=anchor, ledger_id="s11-w2-3",
        recorded_at=FIXED,
    )


def trial_matrix(ledger: lse.Ledger, planted: str | None = None, seed: int = 0) -> pd.DataFrame:
    ids = sorted(lse.ledgered_tested_trials(ledger))
    rng = np.random.default_rng(seed)
    index = pd.date_range("2003-01-01", periods=800, freq="D", tz="UTC")
    matrix = pd.DataFrame(rng.normal(0.0, 0.01, (800, len(ids))), index=index, columns=ids)
    if planted is not None:
        matrix[planted] += 0.002
    return matrix


@pytest.fixture
def ledger() -> lse.Ledger:
    led = new_ledger()
    run(led, 1, untestable=2)
    run(led, 2)
    return led


def test_planted_candidate_passes_and_is_recorded(ledger):
    candidate = sorted(lse.ledgered_tested_trials(ledger))[3]
    out = lse.record_overfit_gate(
        ledger, [candidate], trial_matrix(ledger, candidate), recorded_at=LATER
    )
    record = out["record"]
    assert record["kind"] == "overfit_gate" and record["result"] == "pass" and record["passed"]
    assert record["promotion_allowed"] is False
    assert record["pbo"]["pbo"] < 0.5 and record["dsr"][candidate]["dsr"] > 0.95
    assert record["cpcv"]["n_splits"] == 45 and record["cpcv"]["n_paths"] == 9
    assert len(record["cpcv"]["path_sharpes"]) == 9
    # DSR deflates by every declared trial, untestable ones included
    declared = sum(a["trial_count"] for a in ledger.allocations())
    assert record["n_trials_declared"] == declared == record["dsr"][candidate]["n_trials"]
    assert record["n_trials_tested"] == declared - 2 == record["matrix"]["columns"]
    assert record["prereg_sha256"] == lse.OVERFIT_GATE_PREREG_SHA256
    assert lse.require_overfit_gate(ledger, [candidate])["result"] == "pass"
    assert lse.summarize(ledger)["overfit_gates"] == {"pass": 1, "fail": 0, "refused": 0}


def test_noise_candidate_fails_with_numbers_and_is_kept(ledger):
    matrix = trial_matrix(ledger, seed=4)
    best = str(matrix.mean().idxmax())  # the in-sample winner of pure noise
    record = lse.record_overfit_gate(ledger, [best], matrix, recorded_at=LATER)["record"]
    assert record["result"] == "fail" and record["passed"] is False
    assert record["dsr"][best]["dsr"] <= 0.95
    assert 0 <= record["pbo"]["pbo"] <= 1 and record["reasons"]
    with pytest.raises(lse.OverfitGateRefused, match="fail"):
        lse.require_overfit_gate(ledger, [best])
    assert lse.summarize(ledger)["overfit_gates"]["fail"] == 1


def test_missing_ledgered_trial_is_refused_and_recorded(ledger):
    candidate = min(lse.ledgered_tested_trials(ledger))
    matrix = trial_matrix(ledger, candidate).drop(columns=sorted(lse.ledgered_tested_trials(ledger))[5])
    record = lse.record_overfit_gate(ledger, [candidate], matrix, recorded_at=LATER)["record"]
    assert record["result"] == "refused" and record["passed"] is False
    assert any("missing 1 of" in r for r in record["reasons"])
    assert "pbo" not in record  # nothing computed on a subset
    with pytest.raises(lse.OverfitGateRefused, match="refused"):
        lse.require_overfit_gate(ledger, [candidate])


def test_trials_outside_the_ledger_are_refused(ledger):
    candidate = min(lse.ledgered_tested_trials(ledger))
    matrix = trial_matrix(ledger, candidate)
    matrix["f" * 64] = 0.0
    verdict = lse.evaluate_overfit_gate(ledger, [candidate], matrix, as_of=LATER)
    assert verdict["result"] == "refused"
    assert any("outside the S11 ledger" in r for r in verdict["reasons"])

    unledgered = "e" * 64
    verdict = lse.evaluate_overfit_gate(ledger, [unledgered], trial_matrix(ledger), as_of=LATER)
    assert verdict["result"] == "refused"
    assert any("not tested trials" in r for r in verdict["reasons"])
    # an untestable (declared, never measured) trial cannot be a candidate either
    untested = next(t["trial_id"] for t in ledger.trial_results() if t["status"] == "untestable")
    verdict = lse.evaluate_overfit_gate(ledger, [untested], trial_matrix(ledger), as_of=LATER)
    assert verdict["result"] == "refused"


def test_open_allocation_and_look_ahead_are_refused(ledger):
    candidate = min(lse.ledgered_tested_trials(ledger))
    matrix = trial_matrix(ledger, candidate)
    early = lse.evaluate_overfit_gate(
        ledger, [candidate], matrix, as_of="2003-02-01T00:00:00+00:00"
    )
    assert any("look-ahead" in r for r in early["reasons"])
    lse.allocate(
        ledger, lse.synthetic_catalog(), lse.Policy(budget=12, grid=512),
        run_id="r3", windows=window(3), recorded_at=FIXED,
    )
    verdict = lse.evaluate_overfit_gate(ledger, [candidate], matrix, as_of=LATER)
    assert any("allocation is open" in r for r in verdict["reasons"])


def test_a_pass_goes_stale_when_the_ledger_grows(ledger):
    old_tested = lse.ledgered_tested_trials(ledger)
    candidate = sorted(old_tested)[2]
    lse.record_overfit_gate(ledger, [candidate], trial_matrix(ledger, candidate), recorded_at=LATER)
    assert lse.require_overfit_gate(ledger, [candidate])
    with pytest.raises(lse.OverfitGateRefused, match="no overfitting gate"):
        lse.require_overfit_gate(ledger, [candidate, "0" * 64])
    run(ledger, 3)
    with pytest.raises(lse.OverfitGateRefused, match="stale"):
        lse.require_overfit_gate(ledger, [candidate])
    # re-running against the grown ledger needs the new run's trials in the matrix
    old_ids = sorted(old_tested)
    old_matrix = trial_matrix(ledger, candidate)[old_ids]
    verdict = lse.evaluate_overfit_gate(ledger, [candidate], old_matrix, as_of=LATER)
    assert verdict["result"] == "refused"
    assert any("missing" in r for r in verdict["reasons"])


def test_gate_records_survive_reopen_of_a_file_ledger(tmp_path):
    led = new_ledger(tmp_path)
    run(led, 1)
    candidate = sorted(lse.ledgered_tested_trials(led))[1]
    lse.record_overfit_gate(led, [candidate], trial_matrix(led, candidate), recorded_at=LATER)
    lse.record_overfit_gate(led, [candidate], trial_matrix(led).iloc[:, :3], recorded_at=LATER)
    reopened = lse.Ledger(tmp_path / "ledger.jsonl", anchor=tmp_path / "ledger.anchor.jsonl")
    reopened.verify(full=True)
    results = [r["result"] for r in reopened.of_kind("overfit_gate")]
    assert results == ["pass", "refused"]
    # the latest record for these trials governs: a later refusal blocks promotion
    with pytest.raises(lse.OverfitGateRefused):
        lse.require_overfit_gate(reopened, [candidate])
    # the gate never blocks the loop itself: the next run can still be allocated
    run(reopened, 2)
