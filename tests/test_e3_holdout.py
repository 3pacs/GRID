"""Tests for E3B holdout custodian adapter enforcing stage entry and S11 accounting."""

from __future__ import annotations

import copy
import json
from pathlib import Path

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


# Golden bytes from base87601dcb ledger.py, SHA2569df5e3ac931d9317970e158e35c290a59f7209d0051e09976021d6a63cb56d02.


_compat_V1_LEDGER = (
    '{"created_at":"2026-10-01T00:00:00+00:00","e3_version":"e3-v1","kind":"genesis","ledger_id":"grid-e3-candidates","prev_sha256":null,"promotion_allowed":false,"recorded_at":"2026-10-01T00:00:00+00:00","s11_ledger_id":"fixture-s11","schema":1,"seq":0,"stages":["screen","gates","holdout","forward","promotion"]}\n'
    '{"actor":"proposer:scope-proposer","candidate_id":"96ba7bd718f52b80c956145de2878631a51004dfdaef8ec405461ec9b3f0d020","candidate_kind":"feature","declared_trials":1,"engine_version":"e4-test","expected_sign":0,"family":"scope-family","identity":[{"feature_series":"alpha1","feature_suffix":"x","horizon_sessions":1,"label":"change","target":"T1"}],"identity_sha256":["944b5211ae43d8f30aa19e744125e02a1bd70830298c3068abfa95738d808d02"],"kind":"proposed","prev_sha256":"635d557aff774836a9eb5b06d0e772716d8fd74e4070d02a265d582e316b436c","promotion_allowed":false,"proposed_at":"2026-10-01T00:00:00+00:00","proposer":"scope-proposer","rationale_sha256":"20cc5aaf67dbe0485bfe78856f2324dc2a7129e39786e93039e21fab4e7c6e2c","recorded_at":"2026-10-01T00:00:00+00:00","retest_of":null,"seq":1,"spec_sha256":"752bd69bd3032fdc935090605e0cafa1f14732c9c2aab8a28340b96b149c4852"}\n'
)

_compat_V1_ANCHOR = (
    '{"kind":"anchor","ledger_head_sha256":"635d557aff774836a9eb5b06d0e772716d8fd74e4070d02a265d582e316b436c","ledger_id":"grid-e3-candidates","ledger_seq":0,"prev_sha256":null,"seq":0}\n'
    '{"kind":"anchor","ledger_head_sha256":"67cf482b8f8599b4c18bca72dd0ec0ac1f3888eb70d43f52cc797d13a4a0f787","ledger_id":"grid-e3-candidates","ledger_seq":1,"prev_sha256":"a197f6d8d153733a2ca6c2384ccc1b7aeb46fb325195fdde1a5cc48bdc3a8acb","seq":1}\n'
)

_compat_UNSUPPORTED_LEDGER = '{"created_at":"2026-10-01T00:00:00+00:00","e3_version":"e3-future","kind":"genesis","ledger_id":"grid-e3-candidates","prev_sha256":null,"promotion_allowed":false,"recorded_at":"2026-10-01T00:00:00+00:00","s11_ledger_id":"fixture-s11","schema":1,"seq":0,"stages":["screen","gates","holdout","forward","promotion"]}\n'

_compat_UNSUPPORTED_ANCHOR = '{"kind":"anchor","ledger_head_sha256":"3cbdc14a39d36a9b10a183fb12a520bb5b1e13aff56def10543a2330e931d3c6","ledger_id":"grid-e3-candidates","ledger_seq":0,"prev_sha256":null,"seq":0}\n'

_compat_V1_GOLDEN_CID = (
    "96ba7bd718f52b80c956145de2878631a51004dfdaef8ec405461ec9b3f0d020"
)


def _compat_setup_v1_golden(base_dir: Path) -> tuple[Path, Path]:
    ledger_dir = base_dir / "ledger_v1"
    anchor_dir = base_dir / "anchor_v1"
    ledger_dir.mkdir(parents=True, exist_ok=True)
    anchor_dir.mkdir(parents=True, exist_ok=True)
    (ledger_dir / e3ledger.LEDGER_FILENAME).write_text(
        _compat_V1_LEDGER, encoding="utf-8"
    )
    (anchor_dir / e3ledger.ANCHOR_FILENAME).write_text(
        _compat_V1_ANCHOR, encoding="utf-8"
    )
    return ledger_dir, anchor_dir


def _compat_setup_unsupported_golden(base_dir: Path) -> tuple[Path, Path]:
    ledger_dir = base_dir / "ledger_future"
    anchor_dir = base_dir / "anchor_future"
    ledger_dir.mkdir(parents=True, exist_ok=True)
    anchor_dir.mkdir(parents=True, exist_ok=True)
    (ledger_dir / e3ledger.LEDGER_FILENAME).write_text(
        _compat_UNSUPPORTED_LEDGER, encoding="utf-8"
    )
    (anchor_dir / e3ledger.ANCHOR_FILENAME).write_text(
        _compat_UNSUPPORTED_ANCHOR, encoding="utf-8"
    )
    return ledger_dir, anchor_dir


@pytest.mark.parametrize(
    "case",
    [1, 2, 3, 4, 5, 6],
    ids=[
        "case1_v1_golden_read_and_append_preserves_v1",
        "case2_fresh_v2_hash_differs_from_v1",
        "case3_wrong_namespace_v2_into_v1_rejected",
        "case4_wrong_namespace_v1_into_v2_rejected",
        "case5_unknown_version_golden_refused_on_open",
        "case6_future_version_genesis_refused_before_write",
    ],
)
def test_e3_ledger_compatibility(case, tmp_path, monkeypatch):
    if case == 1:
        # Case 1: actual historical v1 golden bytes read unchanged, ID unchanged;
        # append variant2 remains v1 hashing and preserves both immutable prefixes;
        # independently build canonical core dict by replacing e3_version in candidate_core
        # result to v1, hash matches.
        # (Old original-reader verification done outside CI; unreleased history not loaded into CI).
        ledger_dir, anchor_dir = _compat_setup_v1_golden(tmp_path)
        ledger_file = ledger_dir / e3ledger.LEDGER_FILENAME
        anchor_file = anchor_dir / e3ledger.ANCHOR_FILENAME
        orig_ledger_bytes = ledger_file.read_bytes()
        orig_anchor_bytes = anchor_file.read_bytes()

        ledger = e3ledger.CandidateLedger.open(ledger_dir, anchor_dir)
        state = ledger.state()
        assert state.genesis["e3_version"] == "e3-v1"
        assert _compat_V1_GOLDEN_CID in state.candidates
        assert (
            state.candidates[_compat_V1_GOLDEN_CID].candidate_id
            == _compat_V1_GOLDEN_CID
        )
        recs = ledger.records()
        assert len(recs) == 2
        assert recs[1]["candidate_id"] == _compat_V1_GOLDEN_CID
        assert ledger_file.read_bytes() == orig_ledger_bytes
        assert anchor_file.read_bytes() == orig_anchor_bytes

        client = e3ledger.client_for_proposer("scope-proposer", ledger_dir, anchor_dir)
        cid2 = client.propose(
            family="scope-family",
            candidate_kind="feature",
            trials=[("T1|change|fwd1", "alpha1|x")],
            spec={"fixture": "variant2"},
            rationale="Variant 2 proposal rationale",
            engine_version="e4-test",
        )
        assert cid2 is not None

        new_ledger_bytes = ledger_file.read_bytes()
        new_anchor_bytes = anchor_file.read_bytes()
        assert new_ledger_bytes.startswith(orig_ledger_bytes)
        assert new_anchor_bytes.startswith(orig_anchor_bytes)

        recs2 = ledger.records()
        assert len(recs2) == 3
        rec2 = recs2[2]
        assert rec2["candidate_id"] == cid2

        core_dict = dict(e3ledger.candidate_core(rec2))
        core_dict["e3_version"] = "e3-v1"
        expected_cid = e3ledger.sha256_bytes(e3ledger.canonical(core_dict))
        assert cid2 == expected_cid

        core_v2 = dict(e3ledger.candidate_core(rec2))
        core_v2["e3_version"] = "e3-v2"
        v2_cid = e3ledger.sha256_bytes(e3ledger.canonical(core_v2))
        assert cid2 != v2_cid

    elif case == 2:
        # Case 2: fresh actual v2 genesis/proposal IDs hashv2 != hashv1, unsupported old reader
        # limitation docstring; actual original reader done independent probe outside CI.
        """Unsupported old reader limitation: old v1 reader accepts v2 genesis alone, but fails

        v2 proposal candidate hashing. Actual original-reader verification is performed as an
        independent probe outside CI.
        """
        ledger_dir = tmp_path / "ledger_v2"
        anchor_dir = tmp_path / "anchor_v2"
        ledger_dir.mkdir(parents=True, exist_ok=True)
        anchor_dir.mkdir(parents=True, exist_ok=True)

        ledger = e3ledger.CandidateLedger.genesis(
            ledger_dir, anchor_dir, s11_ledger_id="s11-v2"
        )
        assert ledger.state().genesis["e3_version"] == "e3-v2"

        client = e3ledger.client_for_proposer("scope-proposer", ledger_dir, anchor_dir)
        cid_v2 = client.propose(
            family="scope-family",
            candidate_kind="feature",
            trials=[("T1|change|fwd1", "alpha1|x")],
            spec={"fixture": "fresh-v2"},
            rationale="Fresh v2 rationale",
            engine_version="e4-test",
        )
        assert cid_v2 is not None

        rec = ledger.records()[1]
        assert rec["candidate_id"] == cid_v2

        core_v2 = dict(e3ledger.candidate_core(rec, e3_version="e3-v2"))
        core_v1 = dict(e3ledger.candidate_core(rec, e3_version="e3-v1"))
        hash_v2 = e3ledger.sha256_bytes(e3ledger.canonical(core_v2))
        hash_v1 = e3ledger.sha256_bytes(e3ledger.canonical(core_v1))

        assert cid_v2 == hash_v2
        assert hash_v2 != hash_v1

    elif case == 3:
        # Case 3: wrong namespace candidate ID (v2 ID) rejected before append into target v1.
        ledger_dir, anchor_dir = _compat_setup_v1_golden(tmp_path)
        ledger_file = ledger_dir / e3ledger.LEDGER_FILENAME
        anchor_file = anchor_dir / e3ledger.ANCHOR_FILENAME
        ledger_before = ledger_file.read_bytes()
        anchor_before = anchor_file.read_bytes()

        paths = e3ledger.LedgerPaths.from_dirs(ledger_dir, anchor_dir)

        rec = json.loads(_compat_V1_LEDGER.splitlines()[1])
        prop = copy.deepcopy(rec)
        prop.pop("seq", None)
        prop.pop("prev_sha256", None)
        prop["candidate_id"] = e3ledger.sha256_bytes(
            e3ledger.canonical(e3ledger.candidate_core(prop, e3_version="e3-v2"))
        )

        with pytest.raises(
            ValueError, match="candidate_id is not the sha256 of the canonical proposal"
        ):
            e3ledger._write(
                paths,
                e3ledger._PROPOSER_CAP,
                lambda state: prop,
                recorded_at=prop["proposed_at"],
            )

        assert ledger_file.read_bytes() == ledger_before
        assert anchor_file.read_bytes() == anchor_before

    elif case == 4:
        # Case 4: wrong namespace candidate ID (v1 ID) rejected before append into target v2.
        ledger_dir = tmp_path / "target_v2"
        anchor_dir = tmp_path / "target_anchor_v2"
        ledger_dir.mkdir(parents=True, exist_ok=True)
        anchor_dir.mkdir(parents=True, exist_ok=True)

        ledger = e3ledger.CandidateLedger.genesis(
            ledger_dir, anchor_dir, s11_ledger_id="s11-v2"
        )
        paths = ledger.paths
        ledger_before = paths.ledger.read_bytes()
        anchor_before = paths.anchor.read_bytes()

        rec = json.loads(_compat_V1_LEDGER.splitlines()[1])
        prop = copy.deepcopy(rec)
        prop.pop("seq", None)
        prop.pop("prev_sha256", None)
        prop["candidate_id"] = e3ledger.sha256_bytes(
            e3ledger.canonical(e3ledger.candidate_core(prop, e3_version="e3-v1"))
        )

        with pytest.raises(
            ValueError, match="candidate_id is not the sha256 of the canonical proposal"
        ):
            e3ledger._write(
                paths,
                e3ledger._PROPOSER_CAP,
                lambda state: prop,
                recorded_at=prop["proposed_at"],
            )

        assert paths.ledger.read_bytes() == ledger_before
        assert paths.anchor.read_bytes() == anchor_before

    elif case == 5:
        # Case 5: existing well-formed unknown-version golden genesis refused on open without rewrite
        # (hash chain valid, not artificially tampered).
        ledger_dir, anchor_dir = _compat_setup_unsupported_golden(tmp_path)
        ledger_file = ledger_dir / e3ledger.LEDGER_FILENAME
        anchor_file = anchor_dir / e3ledger.ANCHOR_FILENAME
        ledger_before = ledger_file.read_bytes()
        anchor_before = anchor_file.read_bytes()

        with pytest.raises(ValueError, match="malformed genesis"):
            e3ledger.CandidateLedger.open(ledger_dir, anchor_dir)

        assert ledger_file.read_bytes() == ledger_before
        assert anchor_file.read_bytes() == anchor_before

    elif case == 6:
        # Case 6: monkeypatch actual module VERSION e3-future -> genesis refused before
        # ledger/anchor writes. Check absent or zero bytes as actual mechanics.
        monkeypatch.setattr(e3ledger, "VERSION", "e3-future")
        ledger_dir = tmp_path / "ledger_case6"
        anchor_dir = tmp_path / "anchor_case6"

        paths = e3ledger.LedgerPaths.from_dirs(ledger_dir, anchor_dir)

        with pytest.raises(ValueError, match="malformed genesis"):
            e3ledger.CandidateLedger.genesis(
                ledger_dir, anchor_dir, s11_ledger_id="s11-test"
            )

        if paths.ledger.exists():
            assert paths.ledger.stat().st_size == 0
        else:
            assert not paths.ledger.exists()

        if paths.anchor.exists():
            assert paths.anchor.stat().st_size == 0
        else:
            assert not paths.anchor.exists()
