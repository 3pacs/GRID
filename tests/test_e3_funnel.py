import ast, copy, inspect, json, math
from pathlib import Path
import pytest
from analysis import ledger_steered_exploration as lse
from analysis import offline_research_proof as orp
from analysis.research_forward_log import shift_sessions
from evals.e0 import machinery
from evals.e3.admit import Admission
from evals.e3.funnel import Funnel
from evals.e3.holdout import Custodian
from pandas import Timestamp
from evals.e3.ledger import CandidateLedger, client_for_proposer

REPO = Path(__file__).resolve().parents[1]
CI_RECEIPT = {
    "commit_sha": "a" * 40,
    "checks": {
        "Backend Tests": {"status": "completed", "conclusion": "success"},
        "Frontend Build": {"status": "completed", "conclusion": "success"},
        "Lint": {"status": "completed", "conclusion": "success"},
    },
}


class Reader:
    def __init__(self, judge, s11, epoch, family, cid):
        self.judge, self.s11, self.epoch, self.family, self.cid = (
            judge,
            s11,
            epoch,
            family,
            cid,
        )
        self.discovery_calls = self.holdout_calls = 0

    def read_discovery(self, protocol):
        self.discovery_calls += 1
        cand = self.judge.state().candidates[self.cid]
        assert "screen" in cand.entered and "holdout" not in cand.entered
        return {
            family: orp.build_family_rows(
                protocol,
                self.epoch.features,
                self.epoch.levels[family.split("|")[0]],
                1,
                "discovery",
                label="change",
            )
            for family in protocol.families
        }

    def read_holdout(self, protocol):
        self.holdout_calls += 1
        cand = self.judge.state().candidates[self.cid]
        assert "holdout" in cand.entered
        open_alloc = self.s11.open_allocation()
        assert (
            open_alloc is not None and self.s11.alpha_spent() == open_alloc[0]["alpha"]
        )
        return {
            family: orp.build_family_rows(
                protocol,
                self.epoch.features,
                self.epoch.levels[family.split("|")[0]],
                1,
                "holdout",
                label="change",
            )
            for family in protocol.families
        }


def _make_account(s11_dir, s11_anchor, lid, catalog):
    s11_dir.mkdir(parents=True, exist_ok=True)
    s11_anchor.mkdir(parents=True, exist_ok=True)
    return lse.Ledger.create(
        s11_dir / "ledger.jsonl",
        anchor=s11_anchor / "anchor.jsonl",
        catalog=catalog,
        ledger_id=lid,
    )


def _propose(client, family, pairs, seed):
    sig = inspect.signature(client.propose)
    kw = {
        "trials": pairs,
        "spec": {"seed": seed},
        "rationale": "fixture",
        "engine_version": "1.0",
    }
    kw["candidate_kind" if "candidate_kind" in sig.parameters else "kind"] = "feature"
    return client.propose(family=family, **kw)


def _run_gates(funnel, cid, repo_root=REPO, ci_receipt=CI_RECEIPT):
    return funnel.gates(
        cid,
        commit_sha="a" * 40,
        ci_receipt=ci_receipt,
        baseline_hashes=machinery.machinery_fingerprint(repo_root),
        baseline_cards={},
        baseline_digests={},
        repo_root=str(repo_root),
    )


def make_world(tmp_path, seed=0, planted=None, effect=4):
    catalog = lse.synthetic_catalog(per_class=1)
    lid = f"e3_{seed}"
    s11_dir, s11_anc = tmp_path / f"s11_{seed}", tmp_path / f"s11_a_{seed}"
    e3_dir, e3_anc = tmp_path / f"e3_{seed}", tmp_path / f"e3_a_{seed}"
    e3_dir.mkdir(parents=True, exist_ok=True)
    e3_anc.mkdir(parents=True, exist_ok=True)
    account = _make_account(s11_dir, s11_anc, lid, catalog)
    judge = CandidateLedger.genesis(str(e3_dir), str(e3_anc), s11_ledger_id=lid)
    client = client_for_proposer("proposer", e3_dir, e3_anc)
    pairs = [
        (fam, feat)
        for fam in catalog.families
        for feat in catalog.features
        if (fam, feat) not in catalog.self_lag
    ]
    assert len(pairs) == 7
    fam_name = f"{catalog.families[0]}|{seed}"
    cid = _propose(client, fam_name, pairs, seed)
    epoch = lse.synthetic_epoch(
        catalog,
        0,
        seed=seed,
        planted=("alpha", "T1") if planted else None,
        effect=effect,
        sessions=260,
        discovery=160,
    )
    protocol = orp.Protocol(
        run_id="r1",
        features=catalog.features,
        families=catalog.families,
        trials=tuple(pairs),
        self_lag=catalog.self_lag,
        start=epoch.windows["start"],
        split=epoch.windows["split"],
        end=epoch.windows["end"],
        perms=999,
        seed=seed,
        origin="synthetic_fixture",
        statistic="pearson",
        sampling="horizon_spaced",
    )
    reader = Reader(judge, account, epoch, fam_name, cid)
    custodian = Custodian(judge, account, reader)
    funnel = Funnel(judge, account, reader, custodian)
    return {
        "catalog": catalog,
        "account": account,
        "judge": judge,
        "client": client,
        "cid": cid,
        "epoch": epoch,
        "protocol": protocol,
        "reader": reader,
        "custodian": custodian,
        "funnel": funnel,
        "pairs": pairs,
    }


def make_admitted(tmp_path, name="p"):
    w = make_world(tmp_path / name, seed=42, planted=True)
    sc = w["funnel"].screen(w["cid"], w["protocol"])
    _run_gates(w["funnel"], w["cid"])
    alloc = lse.allocate(
        w["account"],
        w["catalog"],
        lse.Policy(budget=7),
        run_id="r1",
        windows=w["epoch"].windows,
    )
    w["funnel"].holdout(w["cid"], sc, alloc)
    adm = Admission(w["judge"], tmp_path / name / "receipts")
    adm_res = adm.admit(
        w["cid"],
        admitted_at="2026-10-05T12:00:00+00:00",
        forward_check={"check_id": "c1", "method": "m"},
    )
    return (
        adm,
        w["cid"],
        orp.stamp(adm_res["payload"]["first_decision_at"]),
        adm_res["payload"]["forward_check_sha256"],
        w["judge"],
    )


def _eval_dict(decisions, fwd_sha, first_dec, passed=True, retired=False):
    return {
        "check_id": "c1",
        "forward_check_sha256": fwd_sha,
        "decisions_sha256": orp.digest(decisions),
        "passed": passed,
        "retirement_triggered": retired,
        "evaluated_through": str(shift_sessions(first_dec, 160)),
    }


def test_strong_planted_reaches_forward_admitted(tmp_path):
    w = make_world(tmp_path, seed=42, planted=True, effect=4)
    for m in ("screen", "gates", "holdout", "check"):
        assert not hasattr(w["client"], m)
    sc = w["funnel"].screen(w["cid"], w["protocol"])
    assert sc["passed"] is True and _run_gates(w["funnel"], w["cid"])["passed"] is True
    alloc = lse.allocate(
        w["account"],
        w["catalog"],
        lse.Policy(budget=7),
        run_id="r1",
        windows=w["epoch"].windows,
    )
    h_res = w["funnel"].holdout(w["cid"], sc, alloc)
    assert h_res["passed"] is True and w["custodian"].passed(w["cid"]) is True
    adm = Admission(w["judge"], tmp_path / "receipts")
    adm_res = adm.admit(
        w["cid"],
        admitted_at="2026-10-05T12:00:00+00:00",
        forward_check={
            "check_id": "fixture-preregistered",
            "method": "fixture-judge-check",
        },
    )
    assert adm_res["state"] == "FORWARD_ADMITTED"
    assert adm_res["payload"]["first_decision_at"] > adm_res["payload"]["admitted_at"]
    assert (
        w["account"].alpha_spent() == alloc["record"]["alpha"]
        and w["account"].q == 0.10
    )
    assert list(w["judge"].state().candidates[w["cid"]].results.keys()) == [
        "screen",
        "gates",
        "holdout",
        "forward",
    ]


def test_200_null_monte_carlo(tmp_path):
    q, screen_passes, holdout_looks, null_passing_holdout, num_trials, perms = (
        0.10,
        0,
        0,
        0,
        7,
        999,
    )
    min_res = 1.0 / (perms + 1)
    assert min_res <= lse.run_alpha(1, q) / num_trials
    for s in range(200):
        w = make_world(tmp_path, seed=s, planted=None)
        sc = w["funnel"].screen(w["cid"], w["protocol"])
        if sc["passed"]:
            screen_passes += 1
            if _run_gates(w["funnel"], w["cid"])["passed"]:
                holdout_looks += 1
                alloc = lse.allocate(
                    w["account"],
                    w["catalog"],
                    lse.Policy(budget=7),
                    run_id="r1",
                    windows=w["epoch"].windows,
                )
                if w["funnel"].holdout(w["cid"], sc, alloc)["passed"]:
                    null_passing_holdout += 1
    rate = null_passing_holdout / 200.0
    bound = q + 3.0 * math.sqrt(q * (1.0 - q) / 200.0)
    print(
        f"SCREEN passes: {screen_passes}, HOLDOUT looks: {holdout_looks}, null passing HOLDOUT: {null_passing_holdout}, trials: {num_trials}, perms: {perms}, min_res: {min_res}, rate: {rate:.4f}"
    )
    assert holdout_looks > 0 and rate <= bound


def test_holdout_only_plant_invisible_at_screen(tmp_path):
    w1 = make_world(tmp_path / "w1", seed=42, planted=None)
    w2 = make_world(tmp_path / "w2", seed=42, planted=None)
    epoch = w2["epoch"]
    levels = epoch.levels
    features = epoch.features
    split_index = levels.index.get_loc(Timestamp(epoch.windows["split"]))
    levels.iloc[split_index + 1 :, levels.columns.get_loc("T1")] += (
        30 * features["alpha1|x"].iloc[split_index:-1].cumsum().to_numpy()
    )
    sc1 = w1["funnel"].screen(w1["cid"], w1["protocol"])
    sc2 = w2["funnel"].screen(w2["cid"], w2["protocol"])
    assert w1["reader"].holdout_calls == 0 and w2["reader"].holdout_calls == 0
    assert sc1["frozen"]["payload"]["ledger"] == sc2["frozen"]["payload"]["ledger"]


def test_failed_ci_gate_prevents_allocation_and_holdout(tmp_path):
    w = make_world(tmp_path, seed=42, planted=True)
    sc = w["funnel"].screen(w["cid"], w["protocol"])
    assert sc["passed"] is True
    bad_ci = {
        "commit_sha": "a" * 40,
        "checks": {
            "Backend Tests": {"status": "completed", "conclusion": "failure"},
            "Frontend Build": {"status": "completed", "conclusion": "success"},
            "Lint": {"status": "completed", "conclusion": "success"},
        },
    }
    assert _run_gates(w["funnel"], w["cid"], ci_receipt=bad_ci)["passed"] is False
    with pytest.raises(Exception):
        w["funnel"].holdout(w["cid"], sc, {})


def test_forged_screened_refuses_and_closes_s11_spend(tmp_path):
    w = make_world(tmp_path, seed=42, planted=True)
    sc = w["funnel"].screen(w["cid"], w["protocol"])
    assert _run_gates(w["funnel"], w["cid"])["passed"] is True
    alloc = lse.allocate(
        w["account"],
        w["catalog"],
        lse.Policy(budget=7),
        run_id="r1",
        windows=w["epoch"].windows,
    )
    forged = copy.deepcopy(sc)
    forged["frozen"]["payload"]["ledger"][0]["p"] = 0.0001
    forged["frozen"]["sha256"] = orp.digest(forged["frozen"]["payload"])
    with pytest.raises(Exception):
        w["funnel"].holdout(w["cid"], forged, alloc)
    assert w["reader"].holdout_calls == 0 and w["account"].open_allocation() is None


def test_actual_s11_run_synthetic_step(tmp_path):
    cat = lse.synthetic_catalog(per_class=1)
    acc = _make_account(tmp_path / "step_s11", tmp_path / "step_anc", "step_lid", cat)
    ep = lse.synthetic_epoch(
        cat, 0, seed=123, planted=("alpha", "T1"), effect=4, sessions=260, discovery=160
    )
    res = lse.run_synthetic_step(
        acc,
        cat,
        lse.Policy(budget=7),
        ep,
        tmp_path / "step-output",
        run_id="step-r1",
        perms_cap=999,
    )
    alloc_trials = (
        res["allocation"]["record"]["trials"]
        if "record" in res["allocation"]
        else res["allocation"]["trials"]
    )
    run_trials = (
        res["result"]["record"]["trials"]
        if "record" in res["result"]
        else res["result"]["trials"]
    )
    survived = [t for t in run_trials if t.get("holdout_outcome") == "survived"]
    assert len(survived) >= 1
    assert len(run_trials) == len(alloc_trials) == 7
    assert acc.alpha_spent() < acc.q
    assert len(acc.allocations()) >= 1


def test_promotion_parameter_matrix(tmp_path):
    adm, cid, fdec, fsha, _ = make_admitted(tmp_path, "c39")
    decs = [
        {
            "decision_at": str(shift_sessions(fdec, k * 4)),
            "gross_return": 3.0,
            "cost": 1.0,
        }
        for k in range(39)
    ]
    assert (
        adm.promote(cid, decisions=decs, evaluation=_eval_dict(decs, fsha, fdec))[
            "passed"
        ]
        is False
    )
    adm, cid, fdec, fsha, _ = make_admitted(tmp_path, "c_loss")
    decs = [
        {
            "decision_at": str(shift_sessions(fdec, k * 4)),
            "gross_return": 1.0,
            "cost": 1.0,
        }
        for k in range(40)
    ]
    assert (
        adm.promote(cid, decisions=decs, evaluation=_eval_dict(decs, fsha, fdec))[
            "passed"
        ]
        is False
    )
    adm, cid, fdec, fsha, _ = make_admitted(tmp_path, "c_ret")
    decs = [
        {
            "decision_at": str(shift_sessions(fdec, k * 4)),
            "gross_return": 3.0,
            "cost": 1.0,
        }
        for k in range(40)
    ]
    assert (
        adm.promote(
            cid, decisions=decs, evaluation=_eval_dict(decs, fsha, fdec, retired=True)
        )["passed"]
        is False
    )
    adm, cid, fdec, fsha, judge = make_admitted(tmp_path, "c_ok")
    res = adm.promote(cid, decisions=decs, evaluation=_eval_dict(decs, fsha, fdec))
    assert res["passed"] is True and judge.state().candidates[cid].promoted is True
    with pytest.raises(ValueError, match="already promoted"):
        adm.promote(cid, decisions=decs, evaluation=_eval_dict(decs, fsha, fdec))


def test_admission_tamper_and_early_decisions(tmp_path):
    adm, cid, fdec, fsha, _ = make_admitted(tmp_path, "tamper")
    early_decs = [
        {"decision_at": "2026-10-01T00:00:00+00:00", "gross_return": 3.0, "cost": 1.0}
    ] * 40
    res = adm.promote(
        cid, decisions=early_decs, evaluation=_eval_dict(early_decs, fsha, fdec)
    )
    assert res["passed"] is False and res["reason"] == "invalid_decisions"
    rf = adm.receipt_dir / f"{cid}.admission.json"
    data = json.loads(rf.read_text(encoding="utf-8"))
    data["forward_check"]["check_id"] = "tampered"
    rf.write_text(json.dumps(data), encoding="utf-8")
    valid_decs = [
        {
            "decision_at": str(shift_sessions(fdec, k * 4)),
            "gross_return": 3.0,
            "cost": 1.0,
        }
        for k in range(40)
    ]
    with pytest.raises(ValueError):
        adm.promote(
            cid, decisions=valid_decs, evaluation=_eval_dict(valid_decs, fsha, fdec)
        )


def test_no_trading_alert_weights_imports():
    for py_file in (REPO / "evals" / "e3").glob("*.py"):
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not any(
                        f in alias.name for f in ("trading", "alert", "weights")
                    )
            elif isinstance(node, ast.ImportFrom):
                assert not any(
                    f in (node.module or "") for f in ("trading", "alert", "weights")
                )
