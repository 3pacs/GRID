import copy
from dataclasses import replace

import pytest

from analysis.offline_research_proof import (
    corrected_p,
    discover,
    evaluate_holdout,
    run_proof,
)
from scripts.demo_offline_research_proof import fixture


def test_complete_denominator_and_exclusions():
    protocol, discovery, _ = fixture()
    result = discover(protocol, discovery)["payload"]
    assert result["trial_count"] == 4
    assert len({t["trial_id"] for t in result["ledger"]}) == 4
    assert result["ledger"][2]["status"] == "zero_variance"
    assert result["ledger"][3]["status"] == "excluded_telemetry"
    assert not result["ledger"][3]["selected"]
    assert corrected_p(0.02, 4) == 0.08  # raw significant, family-wise refused


def test_finished_vertical_slice_never_promotes(tmp_path):
    result = run_proof(*fixture(), tmp_path / "proof")
    assert result["candidates"]
    assert result["forward_evidence_count"] == 0
    assert not result["promotion_allowed"]
    # S10 (#658 review): a synthetic candidate never waits for forward evidence
    assert all(
        c["state"] == "SYNTHETIC_PROOF_ONLY" and not c["promotion_allowed"]
        for c in result["candidates"]
    )
    assert (tmp_path / "proof/discovery-frozen.json").exists()
    with pytest.raises(FileExistsError):
        run_proof(*fixture(), tmp_path / "proof")


def test_holdout_flip_does_not_change_discovery_and_rejects_candidate():
    protocol, discovery, holdout = fixture()
    frozen = discover(protocol, discovery)
    before = copy.deepcopy(frozen)
    for row in holdout:
        row["target"] *= -1
    assert not evaluate_holdout(frozen, holdout)["candidates"]
    assert frozen == before == discover(protocol, discovery)


@pytest.mark.parametrize(
    "defect", ["future", "crossing", "overlap", "nan", "late_target"]
)
def test_leakage_negative_controls(defect):
    protocol, rows, _ = fixture()
    if defect == "future":
        rows[0]["features"][protocol.features[0]]["known_at"] = protocol.end
    elif defect == "crossing":
        rows[-1]["label_end"] = protocol.split
    elif defect == "overlap":
        rows[1]["decision_at"] = rows[0]["decision_at"]
    elif defect == "nan":
        rows[0]["target"] = float("nan")
    else:
        rows[0]["target_known_at"] = protocol.end
    with pytest.raises(ValueError):
        discover(protocol, rows)


def test_manifest_tampering_refused():
    protocol, discovery, holdout = fixture()
    frozen = discover(protocol, discovery)
    frozen["payload"]["ledger"][0]["selected"] = False
    with pytest.raises(ValueError, match="changed"):
        evaluate_holdout(frozen, holdout)


def test_real_data_refused_not_relabelled_as_proof():
    protocol, discovery, _ = fixture()
    with pytest.raises(ValueError, match="real PIT"):
        discover(replace(protocol, origin="real_market"), discovery)


def test_holdout_horizon_change_refused():
    protocol, discovery, holdout = fixture()
    for row in holdout:
        row["label_end"] = row["target_known_at"]
    with pytest.raises(ValueError, match="horizon"):
        evaluate_holdout(discover(protocol, discovery), holdout)
