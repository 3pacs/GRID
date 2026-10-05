"""Tests for E3 gates (CI gate and E0 non-degradation gate)."""

from __future__ import annotations

import copy, hashlib, importlib, json
from pathlib import Path
from typing import Any, Mapping
import pytest
from evals.e0 import machinery

try:
    import gates
except ImportError:
    try:
        from evals.e3 import gates
    except ImportError:
        from evals.e3b import gates

REPO = Path(__file__).resolve().parents[1]
VALID_SHA = "0123456789abcdef0123456789abcdef01234567"


def _digest(data: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode("utf-8")).hexdigest()


def _receipt(
    sha: str = VALID_SHA, checks: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    if checks is None:
        checks = {
            k: {"status": "completed", "conclusion": "success"}
            for k in gates.MANDATORY_CI
        }
    return {"commit_sha": sha, "checks": dict(checks)}


@pytest.fixture(scope="module")
def clean_cards():
    rel = json.loads((REPO / "evals" / "RELEASED.json").read_text(encoding="utf-8"))
    entries = [
        e
        for e in rel.get("entries", [])
        if e.get("kind") == "suite" and e.get("suite") == "e0"
    ]
    cards = {}
    for e in entries:
        pkg = e["path"].replace("/", ".").strip(".")
        mod = importlib.import_module(f"{pkg}.benchmark")
        try:
            cards[e["version"]] = mod.run(
                profile="ci", replicate=False, log=lambda _: None
            )
        except TypeError:
            cards[e["version"]] = mod.run(profile="ci", replicate=False)
    return cards


@pytest.fixture(scope="module")
def base_env(clean_cards):
    mach = machinery.machinery_fingerprint(REPO)
    digs = {ver: _digest(card) for ver, card in clean_cards.items()}
    return mach, digs


def test_ci_gate_exact_sha_and_valid():
    rec = _receipt()
    assert gates.ci_gate(VALID_SHA, rec)["passed"] is True
    assert gates.ci_gate("short_sha", rec)["passed"] is False
    assert gates.ci_gate("g" * 40, rec)["passed"] is False
    assert gates.ci_gate("f" * 40, rec)["passed"] is False


@pytest.mark.parametrize("bad_conc", [True, "ok", "neutral", "failure", 1])
def test_ci_gate_failed_and_nonliteral_conclusion(bad_conc):
    rec = _receipt(
        checks={
            k: {"status": "completed", "conclusion": "success"}
            for k in gates.MANDATORY_CI
        }
    )
    rec["checks"]["Lint"] = {"status": "completed", "conclusion": bad_conc}
    assert gates.ci_gate(VALID_SHA, rec)["passed"] is False
    rec["checks"]["Lint"] = {"status": "queued", "conclusion": "success"}
    assert gates.ci_gate(VALID_SHA, rec)["passed"] is False


@pytest.mark.parametrize("req", [[], ["Backend Tests"], ["Backend Tests", "Lint"]])
def test_ci_gate_empty_and_subset_required_cannot_bypass(req):
    assert gates.ci_gate(VALID_SHA, _receipt(), required_checks=req)["passed"] is False


@pytest.mark.parametrize("kind", ["feature", "parameter"])
def test_e0_gate_unchanged_machinery_trivial_pass(kind, base_env, clean_cards):
    mach, digs = base_env

    def raising_runner(*_):
        raise RuntimeError("runner should not be invoked")

    res = gates.e0_gate(
        kind,
        baseline_hashes=mach,
        baseline_cards=clean_cards,
        baseline_digests=digs,
        repo_root=REPO,
        runner=raising_runner,
    )
    assert res["passed"] is True
    assert res["details"].get("trivial_pass") is True


def test_e0_gate_invalid_kind_and_baseline_hashes(base_env, clean_cards):
    mach, digs = base_env
    assert (
        gates.e0_gate(
            "invalid",
            baseline_hashes=mach,
            baseline_cards=clean_cards,
            baseline_digests=digs,
            repo_root=REPO,
        )["passed"]
        is False
    )
    bad_keys = dict(mach)
    bad_keys["evals/unknown.py"] = "0" * 64
    assert (
        gates.e0_gate(
            "machinery-change",
            baseline_hashes=bad_keys,
            baseline_cards=clean_cards,
            baseline_digests=digs,
            repo_root=REPO,
        )["passed"]
        is False
    )
    bad_vals = {k: "not_a_sha" for k in mach}
    assert (
        gates.e0_gate(
            "machinery-change",
            baseline_hashes=bad_vals,
            baseline_cards=clean_cards,
            baseline_digests=digs,
            repo_root=REPO,
        )["passed"]
        is False
    )


def test_e0_gate_machinery_change_fresh_real_cards(base_env, clean_cards):
    mach, digs = base_env
    res = gates.e0_gate(
        "machinery-change",
        baseline_hashes=mach,
        baseline_cards=clean_cards,
        baseline_digests=digs,
        repo_root=REPO,
    )
    assert res["passed"] is True
    fresh = res["details"]["cards"]
    assert set(fresh.keys()) == set(clean_cards.keys())
    for card in fresh.values():
        for chk in card["checks"].values():
            assert all(chk.get(flg) is True for flg in gates.SCENARIO_FLAGS)


def test_e0_gate_adversarial_rejected(base_env, clean_cards):
    mach, digs = base_env
    assert (
        gates.e0_gate(
            "machinery-change",
            baseline_hashes=mach,
            baseline_cards={},
            baseline_digests=digs,
            repo_root=REPO,
        )["passed"]
        is False
    )
    bad_digs = {k: "0" * 64 for k in digs}
    assert (
        gates.e0_gate(
            "machinery-change",
            baseline_hashes=mach,
            baseline_cards=clean_cards,
            baseline_digests=bad_digs,
            repo_root=REPO,
        )["passed"]
        is False
    )

    def fp_runner(entry, _):
        c = copy.deepcopy(clean_cards[entry["version"]])
        c["machinery_sha256"] = {k: "0" * 64 for k in mach}
        return c

    assert (
        gates.e0_gate(
            "machinery-change",
            baseline_hashes=mach,
            baseline_cards=clean_cards,
            baseline_digests=digs,
            repo_root=REPO,
            runner=fp_runner,
        )["passed"]
        is False
    )

    def missing_flag_runner(entry, _):
        c = copy.deepcopy(clean_cards[entry["version"]])
        sc = next(iter(c["checks"]))
        del c["checks"][sc]["fdr_bh_controlled"]
        return c

    assert (
        gates.e0_gate(
            "machinery-change",
            baseline_hashes=mach,
            baseline_cards=clean_cards,
            baseline_digests=digs,
            repo_root=REPO,
            runner=missing_flag_runner,
        )["passed"]
        is False
    )

    def false_flag_runner(entry, _):
        c = copy.deepcopy(clean_cards[entry["version"]])
        sc = next(iter(c["checks"]))
        c["checks"][sc]["fwer_holm_controlled"] = False
        return c

    assert (
        gates.e0_gate(
            "machinery-change",
            baseline_hashes=mach,
            baseline_cards=clean_cards,
            baseline_digests=digs,
            repo_root=REPO,
            runner=false_flag_runner,
        )["passed"]
        is False
    )

    def power_runner(entry, _):
        c = copy.deepcopy(clean_cards[entry["version"]])
        sc = next(iter(c["scenarios"]))
        pc = c["scenarios"][sc]["power_curve"]
        pc[-1]["power_bh"] = pc[-1]["power_bh"] - 0.1
        return c

    assert (
        gates.e0_gate(
            "machinery-change",
            baseline_hashes=mach,
            baseline_cards=clean_cards,
            baseline_digests=digs,
            repo_root=REPO,
            runner=power_runner,
        )["passed"]
        is False
    )


def test_evaluate_gates_conjunction(base_env, clean_cards):
    mach, digs = base_env
    res = gates.evaluate_gates(
        VALID_SHA,
        _receipt(),
        "feature",
        baseline_hashes=mach,
        baseline_cards=clean_cards,
        baseline_digests=digs,
        repo_root=REPO,
    )
    assert (
        res["passed"] is True
        and res["ci"]["passed"] is True
        and res["e0"]["passed"] is True
    )
    assert len(res["digest"]) == 64
