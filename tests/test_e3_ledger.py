"""EVAL-E3A: the E3 candidate ledger -- every candidate counts, nothing is edited.

Acceptance items 1-6 of the E3A brief, plus the manifest pin. The S11 fixture
ledgers are in memory (``analysis.ledger_steered_exploration``); the E3
ledger is always on disk with its anchor in a separate directory.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import analysis.ledger_steered_exploration as lse
from evals.e3 import VERSION, manifest
from evals.e3 import ledger as e3

REPO = Path(__file__).resolve().parent.parent
FIXED = "2026-10-01T00:00:00+00:00"
S11_ID = "s11-e3-test"
RECEIPT = "a" * 64
FAMILY = "fam-a"
TRIAL = ("T1|change|fwd1", "alpha1|x")
W_2000 = {"start": "2000-01-03T00:00:00+00:00", "end": "2000-06-01T00:00:00+00:00"}


# --- fixtures -----------------------------------------------------------------------


def dirs(tmp_path: Path) -> tuple[Path, Path]:
    return tmp_path / "ledger", tmp_path / "anchor"


def new_e3(tmp_path: Path, s11_ledger_id: str = S11_ID) -> e3.CandidateLedger:
    ledger_dir, anchor_dir = dirs(tmp_path)
    return e3.CandidateLedger.genesis(
        ledger_dir, anchor_dir, s11_ledger_id=s11_ledger_id, recorded_at=FIXED
    )


def client(tmp_path: Path, proposer: str = "agent-a") -> e3.ProposerClient:
    return e3.client_for_proposer(proposer, *dirs(tmp_path))


def propose(c: e3.ProposerClient, n: int, *, family: str = FAMILY, trials=(TRIAL,), **extra) -> str:
    return c.propose(
        family=family,
        candidate_kind="feature",
        trials=list(trials),
        spec={"variant": n, "family": family},
        rationale=f"idea {n}",
        engine_version="e4b-test",
        **extra,
    )


def s11_ledger(ledger_id: str = S11_ID) -> lse.Ledger:
    return lse.Ledger.create(
        None, catalog=lse.synthetic_catalog(), ledger_id=ledger_id, recorded_at=FIXED
    )


def screen_pass(judge: e3.CandidateLedger, cid: str, s11: lse.Ledger, window=W_2000) -> None:
    judge.stage_entered(cid, "screen", s11=s11, window=window)
    judge.stage_result(cid, "screen", "pass", receipt_sha256=RECEIPT, p_values=[0.01])


def lines(path: Path) -> list[bytes]:
    return path.read_bytes()[:-1].split(b"\n")


def write_lines(path: Path, items: list[bytes]) -> None:
    path.write_bytes(b"".join(line + b"\n" for line in items))


def rechain(records: list[dict]) -> list[bytes]:
    out, previous = [], None
    for seq, record in enumerate(records):
        line = lse.canonical({**record, "seq": seq, "prev_sha256": previous})
        out.append(line)
        previous = lse.sha256_bytes(line)
    return out


# --- 1. order and invariants --------------------------------------------------------


def test_stage_before_proposed_and_out_of_order_are_refused(tmp_path):
    judge, s11 = new_e3(tmp_path), s11_ledger()
    with pytest.raises(ValueError, match="never proposed"):
        judge.stage_entered("f" * 64, "screen", s11=s11, window=W_2000)
    cid = propose(client(tmp_path), 0)
    with pytest.raises(ValueError, match="out of order"):
        judge.stage_entered(cid, "gates")
    with pytest.raises(ValueError, match="out of order"):
        judge.stage_entered(cid, "holdout", s11=s11, s11_allocation_sha256=RECEIPT)
    judge.stage_entered(cid, "screen", s11=s11, window=W_2000)
    with pytest.raises(ValueError, match="needs a passed screen"):
        judge.stage_entered(cid, "gates")
    with pytest.raises(ValueError, match="no open gates"):
        judge.stage_result(cid, "gates", "pass", receipt_sha256=RECEIPT)
    judge.stage_result(cid, "screen", "pass", receipt_sha256=RECEIPT)
    with pytest.raises(ValueError, match="re-entered"):
        judge.stage_entered(cid, "screen", s11=s11, window=W_2000)
    with pytest.raises(ValueError, match="no open screen"):
        judge.stage_result(cid, "screen", "fail", receipt_sha256=RECEIPT)
    judge.stage_entered(cid, "gates")
    judge.stage_result(cid, "gates", "fail", receipt_sha256=RECEIPT)
    with pytest.raises(ValueError, match="needs a passed gates"):
        judge.stage_entered(cid, "holdout", s11=s11, s11_allocation_sha256=RECEIPT)


def test_withdrawn_pre_data_only_before_any_stage(tmp_path):
    judge, s11, c = new_e3(tmp_path), s11_ledger(), client(tmp_path)
    early, late = propose(c, 0), propose(c, 1)
    c.withdraw(early, "duplicate of another idea")
    with pytest.raises(ValueError, match="already withdrawn_pre_data"):
        judge.stage_entered(early, "screen", s11=s11, window=W_2000)
    judge.stage_entered(late, "screen", s11=s11, window=W_2000)
    with pytest.raises(ValueError, match="only allowed before any stage_entered"):
        c.withdraw(late, "too late")
    with pytest.raises(ValueError, match="only allowed before any stage_entered"):
        judge.withdrawn_pre_data(late, "too late")


def test_a_retest_is_a_new_candidate_that_pays_again(tmp_path):
    new_e3(tmp_path)
    c = client(tmp_path)
    first = propose(c, 0)
    with pytest.raises(ValueError, match="already proposed"):
        propose(c, 0)
    again = propose(c, 0, retest_of=first)
    assert again != first
    with pytest.raises(ValueError, match="retest_of must name an earlier candidate"):
        propose(c, 1, retest_of="b" * 64)


def test_proposed_record_carries_hashes_and_identities_only(tmp_path):
    judge = new_e3(tmp_path)
    cid = client(tmp_path).propose(
        family=FAMILY,
        candidate_kind="parameter",
        trials=[TRIAL, ("T2|change|fwd1", "beta2|x")],
        spec={"lookback": 20, "data_path": "/secret/holdout.parquet"},
        rationale="the rationale text stays out of the ledger",
        engine_version="e4b-test",
        expected_sign=1,
    )
    record = judge.records()[-1]
    assert record["kind"] == "proposed" and record["candidate_id"] == cid
    assert set(record) == e3.FIELDS["proposed"]
    assert record["declared_trials"] == 2
    assert record["identity"][0] == lse.scientific_identity(*TRIAL)
    assert record["identity_sha256"][0] == lse.identity(*TRIAL)
    assert record["candidate_id"] == lse.sha256_bytes(lse.canonical(e3.candidate_core(record)))
    raw = (dirs(tmp_path)[0] / e3.LEDGER_FILENAME).read_bytes()
    assert b"holdout.parquet" not in raw and b"rationale text" not in raw
    assert record["proposed_at"] == record["recorded_at"]


def test_invalid_proposals_are_refused(tmp_path):
    new_e3(tmp_path)
    c = client(tmp_path)
    with pytest.raises(ValueError, match="candidate_kind"):
        c.propose(family=FAMILY, candidate_kind="vibes", trials=[TRIAL], spec={"a": 1},
                  rationale="r", engine_version="v")
    with pytest.raises(ValueError, match="unknown family shape"):
        c.propose(family=FAMILY, candidate_kind="feature", trials=[("bad", "x")], spec={"a": 1},
                  rationale="r", engine_version="v")
    with pytest.raises(ValueError, match="expected_sign"):
        propose(c, 0, expected_sign=2)
    with pytest.raises(ValueError, match="each identity once"):
        propose(c, 0, trials=(TRIAL, TRIAL))


def test_open_replays_every_invariant_even_for_a_well_chained_record(tmp_path):
    new_e3(tmp_path)
    ledger_dir, anchor_dir = dirs(tmp_path)
    storage = lse.Ledger(ledger_dir / e3.LEDGER_FILENAME, anchor=anchor_dir / e3.ANCHOR_FILENAME)
    storage._append({  # chained and anchored, but for a candidate never proposed
        "kind": "stage_entered", "actor": "judge", "candidate_id": "c" * 64, "stage": "screen",
        "window": W_2000, "s11_ledger_id": S11_ID, "s11_head_sha256": "d" * 64,
        "s11_allocation_sha256": None, "recorded_at": FIXED,
    })
    with pytest.raises(ValueError, match="never proposed"):
        e3.CandidateLedger.open(ledger_dir, anchor_dir)


# --- 2. every candidate counts -------------------------------------------------------


def test_every_candidate_counts_including_abandoned_and_withdrawn(tmp_path):
    judge, s11, c = new_e3(tmp_path), s11_ledger(), client(tmp_path)
    ids = [propose(c, n) for n in range(10)]
    c.withdraw(ids[0], "superseded before any data")
    judge.withdrawn_pre_data(ids[1], "judge: out of scope")
    c.abandon(ids[2], "agent dropped it before screening")  # proposer, pre-data
    judge.stage_entered(ids[3], "screen", s11=s11, window=W_2000)
    judge.abandoned(ids[3], "screen run crashed")  # mid-stage
    screen_pass(judge, ids[4], s11)
    judge.stage_entered(ids[4], "gates")
    c.abandon(ids[4], "agent lost interest during gates")  # proposer, after screen
    judge.stage_entered(ids[5], "screen", s11=s11, window=W_2000)
    judge.stage_result(ids[5], "screen", "fail", receipt_sha256=RECEIPT, p_values=[0.7])
    for cid in ids[6:]:
        screen_pass(judge, cid, s11)
    counts = judge.family_counts(FAMILY)
    assert counts["proposed"] == 10 and counts["declared_trials"] == 10
    assert counts["withdrawn"] == 2
    assert counts["abandoned"] == 3
    assert counts["failed"] == 1
    assert counts["failures"] == 4  # the 3 abandoned count as failures
    assert counts["screened"] == 7
    assert counts["holdout_looks"] == 0 and counts["alpha_spent"] == 0.0
    assert judge.families() == [FAMILY]
    with pytest.raises(ValueError, match="already abandoned"):
        judge.abandoned(ids[2], "twice")


def test_abandon_after_allocation_needs_the_s11_abandon_and_alpha_stays_spent(tmp_path):
    judge, c = new_e3(tmp_path), client(tmp_path)
    catalog = lse.synthetic_catalog()
    s11 = s11_ledger()
    window = {"start": "2001-01-01T00:00:00+00:00", "split": "2001-07-01T00:00:00+00:00",
              "end": "2002-01-01T00:00:00+00:00"}
    allocation = lse.allocate(s11, catalog, lse.Policy(budget=7), run_id="r1", windows=window,
                              recorded_at=FIXED)
    trial = allocation["record"]["trials"][0]
    cid = propose(c, 0, trials=((trial["family"], trial["feature"]),))
    outside = propose(c, 1, trials=(("T1|change|fwd1", lse.SYNTHETIC_OWN),))  # never allocated
    for candidate in (cid, outside):
        screen_pass(judge, candidate, s11)
        judge.stage_entered(candidate, "gates")
        judge.stage_result(candidate, "gates", "pass", receipt_sha256=RECEIPT)
    with pytest.raises(ValueError, match="does not declare this candidate's trials"):
        judge.stage_entered(outside, "holdout", s11=s11, s11_allocation_sha256=allocation["sha256"])
    with pytest.raises(ValueError, match="not an allocation"):
        judge.stage_entered(cid, "holdout", s11=s11, s11_allocation_sha256="e" * 64)
    judge.stage_entered(cid, "holdout", s11=s11, s11_allocation_sha256=allocation["sha256"])

    with pytest.raises(ValueError, match="only the judge may abandon"):
        c.abandon(cid, "proposer cannot close an S11 allocation")
    with pytest.raises(ValueError, match="still open"):
        judge.abandoned(cid, "dropped", s11=s11)
    s11_abandoned = lse.abandon(s11, "e3 candidate abandoned", recorded_at=FIXED)
    with pytest.raises(ValueError, match="not the S11 record"):
        judge.abandoned(cid, "dropped", s11=s11, s11_abandoned_sha256="f" * 64)
    record = judge.abandoned(cid, "dropped", s11=s11,
                             s11_abandoned_sha256=s11_abandoned["sha256"])["record"]
    assert record["s11_close_kind"] == "abandoned"
    assert record["s11_close_sha256"] == s11_abandoned["sha256"]
    assert record["s11_allocation_sha256"] == allocation["sha256"]

    summary = lse.summarize(s11)
    assert summary["runs_abandoned"] == 1
    assert allocation["record"]["alpha"] > 0
    assert summary["alpha_spent"] == pytest.approx(allocation["record"]["alpha"])
    counts = judge.family_counts(FAMILY)
    assert counts["holdout_looks"] == 1 and counts["abandoned"] == 1 and counts["failures"] == 1


def test_s11_binding_is_checked(tmp_path):
    judge, c = new_e3(tmp_path), client(tmp_path)
    cid = propose(c, 0)
    with pytest.raises(ValueError, match="binds S11 ledger"):
        judge.stage_entered(cid, "screen", s11=s11_ledger("another-ledger"), window=W_2000)
    with pytest.raises(ValueError, match="checked against the bound S11 ledger"):
        judge.stage_entered(cid, "screen", window=W_2000)


def test_full_funnel_to_promotion_suspension_and_retirement(tmp_path):
    judge, c = new_e3(tmp_path), client(tmp_path)
    catalog, s11 = lse.synthetic_catalog(), s11_ledger()
    window = {"start": "2001-01-01T00:00:00+00:00", "split": "2001-07-01T00:00:00+00:00",
              "end": "2002-01-01T00:00:00+00:00"}
    allocation = lse.allocate(s11, catalog, lse.Policy(budget=7), run_id="r1", windows=window,
                              recorded_at=FIXED)
    trial = allocation["record"]["trials"][0]
    cid = propose(c, 0, trials=((trial["family"], trial["feature"]),))
    screen_pass(judge, cid, s11)
    judge.stage_entered(cid, "gates")
    judge.stage_result(cid, "gates", "pass", receipt_sha256=RECEIPT)
    judge.stage_entered(cid, "holdout", s11=s11, s11_allocation_sha256=allocation["sha256"])
    result = judge.stage_result(cid, "holdout", "pass", receipt_sha256=RECEIPT,
                                alpha_spent=allocation["record"]["alpha"], p_values=[0.001])
    assert result["record"]["s11_allocation_sha256"] == allocation["sha256"]
    with pytest.raises(ValueError, match="needs a passed promotion"):
        judge.promoted_research(cid, receipt_sha256=RECEIPT)
    for stage in ("forward", "promotion"):
        judge.stage_entered(cid, stage)
        judge.stage_result(cid, stage, "pass", receipt_sha256=RECEIPT)
    judge.promoted_research(cid, receipt_sha256=RECEIPT)
    judge.suspended(cid, "edge decaying")
    judge.retired(cid, "decayed below the retirement rule", receipt_sha256=RECEIPT)
    with pytest.raises(ValueError, match="already retired"):
        judge.suspended(cid, "again")
    counts = judge.family_counts(FAMILY)
    assert counts["holdout_looks"] == 1 and counts["forward_admitted"] == 1
    assert counts["promoted"] == 1 and counts["suspended"] == 1 and counts["retired"] == 1
    assert counts["alpha_spent"] == pytest.approx(allocation["record"]["alpha"])
    assert counts["failures"] == 0


# --- 3. tamper evidence -------------------------------------------------------------


def _populated(tmp_path: Path) -> tuple[Path, Path, Path]:
    judge, s11, c = new_e3(tmp_path), s11_ledger(), client(tmp_path)
    ids = [propose(c, n) for n in range(3)]
    screen_pass(judge, ids[0], s11)
    c.abandon(ids[1], "dropped")
    ledger_dir, anchor_dir = dirs(tmp_path)
    return ledger_dir, anchor_dir, ledger_dir / e3.LEDGER_FILENAME


def test_edit_truncate_reorder_and_recompute_are_refused(tmp_path):
    ledger_dir, anchor_dir, path = _populated(tmp_path)
    e3.CandidateLedger.open(ledger_dir, anchor_dir)
    original = path.read_bytes()

    def refused():
        with pytest.raises(ValueError):
            e3.CandidateLedger.open(ledger_dir, anchor_dir)
        path.write_bytes(original)

    items = lines(path)
    record = json.loads(items[2])
    record["family"] = "fam-z"  # edit a past line, still canonical JSON
    write_lines(path, items[:2] + [lse.canonical(record)] + items[3:])
    refused()
    write_lines(path, items[:-1])  # truncate one line at a line boundary
    refused()
    write_lines(path, items[:2] + [items[3], items[2]] + items[4:])  # reorder
    refused()
    records = [json.loads(line) for line in items]
    records[6]["reason"] = "a different story"
    write_lines(path, rechain(records))  # recompute the whole file: chain valid
    refused()
    e3.CandidateLedger.open(ledger_dir, anchor_dir)  # restored original opens


def test_rewriting_ledger_and_anchor_is_caught_by_the_offhost_witness(tmp_path):
    ledger_dir, anchor_dir, path = _populated(tmp_path)
    anchor_path = anchor_dir / e3.ANCHOR_FILENAME
    witness = tmp_path / "vault" / "ledger.anchors.jsonl"
    witness.parent.mkdir()
    shutil.copyfile(anchor_path, witness)
    assert e3.CandidateLedger.open(ledger_dir, anchor_dir, external_anchors=witness).verify(
        external_anchors=witness)["witnessed_records"] == len(lines(path))

    records = [json.loads(line) for line in lines(path)]
    records[6]["reason"] = "rewritten history"
    new_lines = rechain(records)
    write_lines(path, new_lines)
    anchor_path.unlink()
    anchor = lse.Anchor(anchor_path)
    for seq, line in enumerate(new_lines):
        anchor.append(records[0]["ledger_id"], seq, lse.sha256_bytes(line))
    e3.CandidateLedger.open(ledger_dir, anchor_dir)  # local files agree with each other ...
    with pytest.raises(ValueError, match="differs from its off-host witness"):
        e3.CandidateLedger.open(ledger_dir, anchor_dir, external_anchors=witness)  # ... not off-host
    with pytest.raises(ValueError, match="missing"):
        e3.CandidateLedger.open(ledger_dir, anchor_dir, external_anchors=tmp_path / "nope.jsonl")


def test_a_second_genesis_is_refused(tmp_path):
    ledger_dir, anchor_dir, path = _populated(tmp_path)
    with pytest.raises(ValueError, match="second genesis"):
        e3.CandidateLedger.genesis(ledger_dir, anchor_dir, s11_ledger_id=S11_ID)
    path.unlink()  # a fresh ledger file cannot restart against the existing anchor
    with pytest.raises(ValueError, match="second genesis"):
        e3.CandidateLedger.genesis(ledger_dir, anchor_dir, s11_ledger_id=S11_ID)
    with pytest.raises(ValueError):
        e3.CandidateLedger.open(ledger_dir, anchor_dir)


def test_dirs_come_from_env_and_must_differ(tmp_path, monkeypatch):
    monkeypatch.delenv(e3.ENV_LEDGER_DIR, raising=False)
    monkeypatch.delenv(e3.ENV_ANCHOR_DIR, raising=False)
    with pytest.raises(ValueError, match=e3.ENV_LEDGER_DIR):
        e3.CandidateLedger.genesis(s11_ledger_id=S11_ID)
    with pytest.raises(ValueError, match="separate directories"):
        e3.CandidateLedger.genesis(tmp_path / "same", tmp_path / "same", s11_ledger_id=S11_ID)
    monkeypatch.setenv(e3.ENV_LEDGER_DIR, str(tmp_path / "l"))
    monkeypatch.setenv(e3.ENV_ANCHOR_DIR, str(tmp_path / "a"))
    judge = e3.CandidateLedger.genesis(s11_ledger_id=lse.CANONICAL_LEDGER_ID)
    assert judge.records()[0]["s11_ledger_id"] == lse.CANONICAL_LEDGER_ID
    assert judge.records()[0]["e3_version"] == VERSION
    assert (tmp_path / "l" / e3.LEDGER_FILENAME).is_file()
    assert (tmp_path / "a" / e3.ANCHOR_FILENAME).is_file()
    propose(e3.client_for_proposer("agent-env"), 0)
    assert e3.CandidateLedger.open().family_counts(FAMILY)["proposed"] == 1


# --- 4. proposer client -------------------------------------------------------------


def test_proposer_client_exposes_no_judge_writes(tmp_path):
    new_e3(tmp_path)
    c = client(tmp_path)
    for name in ("stage_entered", "stage_result", "promoted_research", "retired",
                 "suspended", "abandoned", "withdrawn_pre_data", "family_counts", "paths"):
        with pytest.raises(AttributeError):
            getattr(c, name)
    with pytest.raises(AttributeError):
        c.extra = 1
    with pytest.raises(TypeError, match="issued only by client_for_proposer"):
        e3.ProposerClient(object(), e3.LedgerPaths.from_dirs(*dirs(tmp_path)), "agent-x")


def test_proposer_capability_refuses_judge_records(tmp_path):
    new_e3(tmp_path)
    cid = propose(client(tmp_path), 0)
    paths = e3.LedgerPaths.from_dirs(*dirs(tmp_path))
    judge_record = {
        "kind": "stage_entered", "actor": "proposer:agent-a", "candidate_id": cid,
        "stage": "screen", "window": W_2000, "s11_ledger_id": S11_ID,
        "s11_head_sha256": "d" * 64, "s11_allocation_sha256": None,
    }
    with pytest.raises(PermissionError, match="may not write stage_entered"):
        e3._write(paths, e3._PROPOSER_CAP, lambda state: judge_record)
    with pytest.raises(PermissionError, match="judge records carry actor"):
        e3._write(paths, e3._JUDGE_CAP, lambda state: judge_record)
    with pytest.raises(PermissionError, match="capability"):
        e3._write(paths, object(), lambda state: judge_record)


def test_a_proposer_abandons_only_its_own_candidates_and_it_counts(tmp_path):
    judge = new_e3(tmp_path)
    mine, theirs = client(tmp_path, "agent-a"), client(tmp_path, "agent-b")
    cid = propose(mine, 0)
    with pytest.raises(ValueError, match="its own candidates"):
        theirs.abandon(cid, "not mine")
    with pytest.raises(ValueError, match="its own candidates"):
        theirs.withdraw(cid, "not mine")
    record = mine.abandon(cid, "dead end")["record"]
    assert record["actor"] == "proposer:agent-a"
    counts = judge.family_counts(FAMILY)
    assert counts["abandoned"] == 1 and counts["failures"] == 1 and counts["proposed"] == 1


# --- 5. window reuse against S11's registry -----------------------------------------


def test_an_identity_whose_s11_window_was_touched_is_refused_at_screen(tmp_path):
    judge, c = new_e3(tmp_path), client(tmp_path)
    catalog = lse.synthetic_catalog()
    s11 = s11_ledger()
    epoch = lse.synthetic_epoch(catalog, 1)
    step = lse.run_synthetic_step(
        s11, catalog, lse.Policy(budget=7), epoch, tmp_path / "s11-run1", run_id="r1",
        perms_cap=999, recorded_at=FIXED,
    )
    trials = step["allocation"]["record"]["trials"]
    touched_trial = (trials[0]["family"], trials[0]["feature"])
    used = {(t["family"], t["feature"]) for t in trials}
    fresh_trial = next(
        ("T2|change|fwd1", f) for f in catalog.features
        if ("T2|change|fwd1", f) not in used and f != lse.SYNTHETIC_OWN
    )
    inside = {"start": epoch.windows["split"], "end": epoch.windows["end"]}
    after = {"start": "2030-01-01T00:00:00+00:00", "end": "2031-01-01T00:00:00+00:00"}

    reused = propose(c, 0, trials=(touched_trial,))
    with pytest.raises(ValueError, match="already touched an overlapping window"):
        judge.stage_entered(reused, "screen", s11=s11, window=inside)
    judge.stage_entered(reused, "screen", s11=s11, window=after)  # disjoint window: fine
    fresh = propose(c, 1, trials=(fresh_trial,))
    judge.stage_entered(fresh, "screen", s11=s11, window=inside)  # untouched identity: fine
    mixed = propose(c, 2, trials=(fresh_trial, touched_trial))
    with pytest.raises(ValueError, match="already touched"):
        judge.stage_entered(mixed, "screen", s11=s11, window=inside)
    entered = [r for r in judge.records() if r["kind"] == "stage_entered"]
    assert entered[-1]["s11_head_sha256"] == s11.head
    assert entered[-1]["s11_ledger_id"] == S11_ID


# --- 6. concurrency -----------------------------------------------------------------

_WORKER = """
import sys
from evals.e3.ledger import client_for_proposer
proposer, ledger_dir, anchor_dir, n = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
c = client_for_proposer(proposer, ledger_dir, anchor_dir)
for i in range(n):
    c.propose(family="fam-c", candidate_kind="feature", trials=[("T1|change|fwd1", "alpha1|x")],
              spec={"proposer": proposer, "i": i}, rationale="r", engine_version="e4b-test")
"""


def test_two_processes_appending_concurrently_keep_one_valid_chain(tmp_path):
    judge = new_e3(tmp_path)
    ledger_dir, anchor_dir = dirs(tmp_path)
    n = 12
    env = {**os.environ, "PYTHONPATH": str(REPO) + os.pathsep + os.environ.get("PYTHONPATH", "")}
    workers = [
        subprocess.Popen(
            [sys.executable, "-c", _WORKER, name, str(ledger_dir), str(anchor_dir), str(n)],
            cwd=REPO, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        for name in ("agent-p", "agent-q")
    ]
    for worker in workers:
        _, err = worker.communicate(timeout=900)
        assert worker.returncode == 0, err.decode("utf-8", "replace")[-2000:]
    records = judge.records()
    assert len(records) == 1 + 2 * n
    proposed = [r for r in records if r["kind"] == "proposed"]
    assert len({r["candidate_id"] for r in proposed}) == 2 * n
    for name in ("agent-p", "agent-q"):
        mine = [r for r in proposed if r["proposer"] == name]
        assert len(mine) == n
    assert judge.verify()["records"] == 1 + 2 * n
    assert len(lines(ledger_dir / e3.LEDGER_FILENAME)) == len(lines(anchor_dir / e3.ANCHOR_FILENAME))
    assert judge.family_counts("fam-c")["proposed"] == 2 * n
    assert not (ledger_dir / e3.LOCK_FILENAME).exists()


# --- manifest -----------------------------------------------------------------------


def test_every_pinned_file_matches_the_manifest():
    result = manifest.verify()
    assert result["version"] == VERSION
    assert manifest.main(["--check"]) == 0


def test_manifest_detects_a_changed_file(tmp_path):
    root = tmp_path / "e3"
    shutil.copytree(manifest.PACKAGE, root, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    manifest.verify(root)
    (root / "ledger.py").write_text("# edited\n", encoding="utf-8")
    with pytest.raises(manifest.ManifestError, match="changed: ledger.py"):
        manifest.verify(root)
    (root / "extra.py").write_text("x = 1\n", encoding="utf-8")
    with pytest.raises(manifest.ManifestError, match="unpinned: extra.py"):
        manifest.verify(root)
