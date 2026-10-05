"""Tests for E3B holdout custodian adapter enforcing stage entry and S11 accounting."""

from __future__ import annotations

from dataclasses import replace
import pytest
from analysis import ledger_steered_exploration as s11
from analysis import offline_research_proof as orp
from evals.e3 import ledger as e3ledger
from evals.e3.holdout import Custodian, _JUDGE_TOKEN


@pytest.fixture
def env(tmp_path):
    root, anchor = tmp_path / "s11", tmp_path / "s11-anchor"
    root.mkdir()
    anchor.mkdir()
    catalog = s11.synthetic_catalog(per_class=1)
    account = s11.Ledger.create(
        root / "l.jsonl",
        anchor=anchor / "a.jsonl",
        catalog=catalog,
        ledger_id="e3b-fix",
    )
    judge = e3ledger.CandidateLedger.genesis(
        tmp_path / "e3", tmp_path / "e3-a", s11_ledger_id="e3b-fix"
    )
    epoch = s11.synthetic_epoch(
        catalog, 0, seed=81291, effect=4.0, sessions=260, discovery=160
    )
    pairs = [
        (fam, feat)
        for fam in catalog.families
        for feat in catalog.features
        if (fam, feat) not in catalog.self_lag
    ]
    client = e3ledger.client_for_proposer(
        "reviewer", tmp_path / "e3", tmp_path / "e3-a"
    )
    cid = client.propose(
        family="rev-family",
        candidate_kind="feature",
        trials=pairs,
        spec={"fixture": "planted"},
        rationale="Synthetic fixture",
        engine_version="reviewer-v1",
    )
    judge.stage_entered(
        cid,
        "screen",
        s11=account,
        window={"start": epoch.windows["start"], "end": epoch.windows["split"]},
    )
    judge.stage_result(
        cid, "screen", "pass", receipt_sha256="a" * 64, p_values=[0.001] * len(pairs)
    )
    judge.stage_entered(cid, "gates")
    judge.stage_result(cid, "gates", "pass", receipt_sha256="b" * 64)
    allocation = s11.allocate(
        account,
        catalog,
        s11.Policy(budget=len(pairs)),
        run_id="reviewer-r1",
        windows=epoch.windows,
    )
    protocol = s11.protocol_for_allocation(
        allocation,
        origin="synthetic_fixture",
        sampling="horizon_spaced",
        step=1,
        perms=999,
        seed=81291,
        statistic="pearson",
        alpha=allocation["record"]["alpha"],
    )
    discovery = {
        f: orp.build_family_rows(
            protocol,
            epoch.features,
            epoch.levels[f.split("|")[0]],
            1,
            "discovery",
            label="change",
        )
        for f in protocol.families
    }
    frozen = orp.discover(protocol, discovery)

    class Reader:
        calls = 0

        def read_holdout(self, p):
            self.calls += 1
            cand = judge.state().candidates[cid]
            assert "holdout" in cand.entered, (
                "Label access preceded HOLDOUT stage entry"
            )
            assert account.open_allocation()[1] == allocation["sha256"]
            summary = s11.summarize(account)
            assert summary["alpha_spent"] == allocation["record"]["alpha"]
            assert 0 < summary["alpha_spent"] < summary["q"]
            return {
                f: orp.build_family_rows(
                    p,
                    epoch.features,
                    epoch.levels[f.split("|")[0]],
                    1,
                    "holdout",
                    label="change",
                )
                for f in p.families
            }

    return judge, account, cid, allocation, frozen, Reader(), client, pairs, epoch


def test_capability_refusal_and_public_passed(env):
    judge, account, cid, allocation, frozen, reader, client, pairs, epoch = env
    cust = Custodian(judge, account, reader)
    assert cust.passed(cid) is False
    assert type(cust.passed(cid)) is bool
    with pytest.raises(PermissionError, match="_JUDGE_TOKEN"):
        cust.check(object(), cid, frozen, allocation)
    assert reader.calls == 0


def test_wrong_token_abandon(env):
    judge, account, cid, allocation, frozen, reader, client, pairs, epoch = env
    cust = Custodian(judge, account, reader)
    with pytest.raises(PermissionError, match="_JUDGE_TOKEN"):
        cust.abandon(object(), cid, "unauthorized attempt")


def test_holdout_check_spend_denominator_duplicate_fresh(env):
    judge, account, cid, allocation, frozen, reader, client, pairs, epoch = env
    cust = Custodian(judge, account, reader)
    trials = allocation["record"]["trials"]
    assert len(trials) == 7 == len(pairs)
    assert {(t["family"], t["feature"]) for t in trials} == set(pairs)
    res = cust.check(_JUDGE_TOKEN, cid, frozen, allocation)
    assert isinstance(res, dict) and "passed" in res
    assert reader.calls == 1
    passed_val = cust.passed(cid)
    assert type(passed_val) is bool and passed_val == res["passed"]
    cand = judge.state().candidates[cid]
    results = (
        cand.get("results") if isinstance(cand, dict) else getattr(cand, "results")
    )
    assert results["holdout"]["result"] in ("pass", "fail")
    assert results["holdout"]["receipt_sha256"] == orp.digest(res["receipt"])
    fresh = Custodian(judge, account, reader)
    assert fresh.passed(cid) == passed_val
    assert type(fresh.passed(cid)) is bool
    with pytest.raises(ValueError, match="already"):
        fresh.check(_JUDGE_TOKEN, cid, frozen, allocation)
    assert reader.calls == 1


def test_crashed_reader_closes_abandon(env):
    judge, account, cid, allocation, frozen, reader, client, pairs, epoch = env

    class CrashingReader:
        def read_holdout(self, p):
            raise RuntimeError("Simulated holdout read failure")

    cust = Custodian(judge, account, CrashingReader())
    with pytest.raises(RuntimeError, match="Simulated holdout read failure"):
        cust.check(_JUDGE_TOKEN, cid, frozen, allocation)
    assert account.open_allocation() is None
    s11_sum = s11.summarize(account)
    assert s11_sum["runs_abandoned"] == 1
    assert s11_sum["alpha_spent"] == allocation["record"]["alpha"]
    alpha = allocation["record"]["alpha"]
    fc = judge.family_counts("rev-family")
    assert fc["abandoned"] == 1
    assert fc["failures"] == 1
    assert fc["alpha_spent"] == alpha
    cand = judge.state().candidates[cid]
    assert cand.terminal == "abandoned"


def test_mismatched_protocol_alpha_refused_before_labels(env):
    judge, account, cid, allocation, frozen, reader, client, pairs, epoch = env
    bad_proto = s11.protocol_for_allocation(
        allocation,
        origin="synthetic_fixture",
        sampling="horizon_spaced",
        step=1,
        perms=999,
        seed=81291,
        statistic="pearson",
        alpha=allocation["record"]["alpha"],
    )
    bad_proto = replace(bad_proto, alpha=allocation["record"]["alpha"] * 0.5)
    bad_disc = {
        f: orp.build_family_rows(
            bad_proto,
            epoch.features,
            epoch.levels[f.split("|")[0]],
            1,
            "discovery",
            label="change",
        )
        for f in bad_proto.families
    }
    bad_frozen = orp.discover(bad_proto, bad_disc)
    cust = Custodian(judge, account, reader)
    with pytest.raises(
        ValueError, match="Protocol.alpha must equal allocation alpha exactly"
    ):
        cust.check(_JUDGE_TOKEN, cid, bad_frozen, allocation)
    assert reader.calls == 0
    assert account.open_allocation() is None
    alpha = allocation["record"]["alpha"]
    s11_sum = s11.summarize(account)
    assert s11_sum["runs_abandoned"] == 1
    assert s11_sum["alpha_spent"] == alpha
    fc = judge.family_counts("rev-family")
    assert fc["abandoned"] == 1
    assert fc["failures"] == 1
    assert fc["alpha_spent"] == alpha
    cand = judge.state().candidates[cid]
    assert cand.terminal == "abandoned"


def test_repeated_identity_segment(env):
    judge, account, cid, allocation, frozen, reader, client, pairs, epoch = env
    cust = Custodian(judge, account, reader)
    cust.check(_JUDGE_TOKEN, cid, frozen, allocation)
    cid2 = client.propose(
        family="rev-family",
        candidate_kind="feature",
        trials=pairs,
        spec={"fixture": "second"},
        rationale="Retest fixture",
        engine_version="reviewer-v2",
        retest_of=cid,
    )
    with pytest.raises(ValueError, match="overlapping"):
        judge.stage_entered(
            cid2,
            "screen",
            s11=account,
            window={"start": epoch.windows["start"], "end": epoch.windows["split"]},
        )
    assert reader.calls == 1
