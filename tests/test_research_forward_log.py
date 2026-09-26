"""S10 forward log: eligibility, hash chain, no-lookahead evaluation, stop rule.

A real ``scripts/run_real_panel_scan.py`` scan runs on an in-memory SQLite
``raw_series`` (through ``store.observations.read_window``) with a planted
cross-series effect, so admission sees genuine manifests. Forward runs read
the same table with simulated clocks. No production DB, no network.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

from analysis import offline_research_proof as orp
from analysis import research_forward_log as fl
from analysis import research_real_panel as rp
from scripts import research_forward_log as cli
from scripts import run_real_panel_scan as scan_script

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

REPO = Path(__file__).resolve().parent.parent
SCAN_SHA = "ab" * 20
DATA_START, DATA_END = date(2019, 6, 3), date(2022, 12, 30)
SCAN_AS_OF = date(2021, 6, 30)
SCAN_AS_OF_TS = datetime(2021, 7, 1, tzinfo=timezone.utc)
FROZEN_AT = datetime(2021, 7, 2, 9, 0, tzinfo=timezone.utc)
FEATURES = (
    rp.SeriesSpec("FEAT_X", "diff", "FRB_H15"),
    rp.SeriesSpec("FEAT_D", "diff", "FRB_H15"),
    rp.SeriesSpec("FEAT_N", "diff", "FRB_H15"),
)
TARGETS = (rp.TargetSpec("TGT", "change", "FRB_H15"),)
HORIZONS = (1, 5)
H15 = rp.PUBLICATIONS["FRB_H15"]


def proxy_patch(mp: pytest.MonkeyPatch) -> None:
    mp.setitem(rp.PROXY_GROUPS, "TGT", frozenset({"TGT", "FEAT_D"}))


@pytest.fixture(autouse=True)
def tgt_proxy_group(monkeypatch):
    proxy_patch(monkeypatch)


def naive(ts: pd.Timestamp) -> datetime:
    return ts.tz_convert("UTC").tz_localize(None).to_pydatetime()


def build_engine(
    revision: tuple[date, float, datetime] | None = None,
    drop: tuple[str, date] | None = None,
):
    """TGT responds to FEAT_X's published 5-session change; FEAT_D copies TGT."""
    engine = create_engine("sqlite://")
    md = MetaData()
    raw = Table(
        "raw_series",
        md,
        Column("series_id", String, nullable=False),
        Column("source_id", String, nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)
    rng = np.random.default_rng(10)
    days = pd.bdate_range(DATA_START, DATA_END)
    n = len(days)
    feat = np.cumsum(rng.normal(0, 1, n))
    inc = np.zeros(n)
    inc[7:] = 0.3 * (feat[5:-2] - feat[:-7]) / 5 + rng.normal(0, 0.05, n - 7)
    tgt = 4 + np.cumsum(inc)
    noise = np.cumsum(rng.normal(0, 1, n))
    pulled = rp.publication_times([d.date() for d in days], H15) + pd.Timedelta(hours=1)
    rows = []
    for i, d in enumerate(days):
        for sid, value in (("TGT", tgt[i]), ("FEAT_X", feat[i]), ("FEAT_D", tgt[i]),
                           ("FEAT_N", noise[i])):
            if drop == (sid, d.date()):
                continue
            rows.append({"series_id": sid, "source_id": "fred", "obs_date": d.date(),
                         "pull_timestamp": naive(pulled[i]), "value": float(value),
                         "raw_payload": "{}", "pull_status": "SUCCESS"})
    if revision is not None:
        obs_date, value, when = revision
        rows.append({"series_id": "FEAT_X", "source_id": "fred", "obs_date": obs_date,
                     "pull_timestamp": when, "value": value, "raw_payload": "{}",
                     "pull_status": "SUCCESS"})
    with engine.begin() as c:
        c.execute(raw.insert(), rows)
    return engine


class FakeRepo:
    """Scan code SCAN_SHA includes #661; its files are this checkout's files."""

    def __init__(self, includes: bool = True, files: dict | None = None) -> None:
        self.includes, self.files = includes, files or {}

    def is_ancestor(self, ancestor: str, commit: str) -> bool:
        return self.includes and commit == SCAN_SHA

    def file_bytes(self, commit: str, path: str) -> bytes | None:
        if path in self.files:
            return self.files[path]
        return (REPO / path).read_bytes() if commit == SCAN_SHA else None


@pytest.fixture(scope="module")
def db():
    return build_engine()


@pytest.fixture(scope="module")
def scan_dir(db, tmp_path_factory):
    out = tmp_path_factory.mktemp("scan") / "scan-test"
    args = argparse.Namespace(
        as_of=SCAN_AS_OF.isoformat(),
        as_of_ts=SCAN_AS_OF_TS.isoformat(),
        read_start=DATA_START.isoformat(),
        discovery_start="2019-10-01",
        split="2020-10-01",
        perms=999,
        seed=7,
        code_sha=SCAN_SHA,
    )
    with pytest.MonkeyPatch.context() as mp:
        proxy_patch(mp)
        mp.setattr(scan_script, "FEATURES", FEATURES)
        mp.setattr(scan_script, "TARGETS", TARGETS)
        mp.setattr(scan_script, "HORIZONS", HORIZONS)
        out.mkdir(parents=True)
        with db.connect() as conn:
            scan_script.scan(conn, out, args)
    return out


def copy_scan(scan_dir: Path, tmp_path: Path) -> Path:
    target = tmp_path / "scan-copy"
    shutil.copytree(scan_dir, target)
    return target


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


def resign_payload(scan: Path, change) -> None:
    """Mutate the manifest payload and re-sign it (an internally consistent forgery)."""
    path = scan / "run" / "discovery-frozen.json"
    frozen = read(path)
    change(frozen["payload"])
    frozen["sha256"] = orp.digest(frozen["payload"])
    write(path, frozen)


def rewrite_candidates(scan: Path, change, resign: bool = True) -> None:
    """Mutate every frozen candidate consistently in both candidate files."""
    candidates = read(scan / "frozen-candidates.json")
    for candidate in candidates:
        change(candidate)
        if resign:
            candidate["sha256"] = orp.digest(candidate["specification"])
    write(scan / "frozen-candidates.json", candidates)
    holdout = read(scan / "run" / "holdout-result.json")
    holdout["candidates"] = candidates
    write(scan / "run" / "holdout-result.json", holdout)


def admit(log_dir: Path, scan: Path, repo=None, now=FROZEN_AT):
    return fl.admit_scan(fl.ForwardLog(log_dir), scan, repo or FakeRepo(), now, "c" * 40)


# --- pre-registration and the carried #658 fix ----------------------------------------


def test_preregistration_hash_is_pinned():
    assert fl.prereg_file_sha256(REPO) == fl.PREREG_SHA256
    text = (REPO / fl.PREREG_PATH).read_text(encoding="utf-8")
    for pinned in (fl.MIN_SCAN_CODE_SHA, *fl.DENIED_DISCOVERY_MANIFESTS, "20260926", "9,999"):
        assert pinned in text
    assert fl.PERMS == 9999 and fl.SEED == 20260926 and fl.GRACE.days == 5


def test_candidate_state_is_tagged_by_origin():
    """#658 review: only a latest_vintage_read candidate waits for forward evidence."""
    assert orp.candidate_state("latest_vintage_read") == "FORWARD_EVIDENCE_PENDING"
    assert orp.candidate_state("synthetic_fixture") == "SYNTHETIC_PROOF_ONLY"
    assert orp.candidate_state("exploratory_replay") == "EXPLORATORY_REPLAY_ONLY"


def test_planted_scan_freezes_cross_series_candidates_only(scan_dir):
    candidates = read(scan_dir / "frozen-candidates.json")
    assert {c["specification"]["feature"] for c in candidates} == {"FEAT_X|chg5", "FEAT_X|z60"}
    assert all(c["state"] == "FORWARD_EVIDENCE_PENDING" for c in candidates)
    ledger = read(scan_dir / "run" / "discovery-frozen.json")["payload"]["ledger"]
    assert all(not t["selected"] for t in ledger if t["feature"].startswith("FEAT_D"))


# --- admission ---------------------------------------------------------------------


def test_admission_writes_header_and_frozen_plans(scan_dir, tmp_path):
    written = admit(tmp_path / "log", scan_dir)
    assert [r["kind"] for r in written] == ["header", "admission", "admission"]
    header = written[0]
    assert header["prereg_sha256"] == fl.PREREG_SHA256 and header["prev_sha256"] is None
    assert header["min_scan_code_sha"] == fl.MIN_SCAN_CODE_SHA
    for record in written[1:]:
        plan = record["plan"]
        assert record["plan_sha256"] == orp.digest(plan)
        assert record["promotion_allowed"] is False and plan["promotion_allowed"] is False
        assert record["scan"]["code_sha"] == SCAN_SHA
        assert plan["alpha"] == pytest.approx(0.05 / 2) and plan["family_size"] == 2
        assert plan["min_n"] == 30 and plan["max_decisions"] == 60
        assert plan["horizon_sessions"] == 1 and plan["spacing_sessions"] == 5
        assert plan["target"]["series_id"] == "TGT" and plan["feature"]["series_id"] == "FEAT_X"
        assert pd.Timestamp(plan["first_decision_at"]) > pd.Timestamp(FROZEN_AT)
        assert plan["first_decision_at"] == "2021-07-05T00:00:00+00:00"
        assert 1 <= plan["block"] <= 30 // orp.MIN_BLOCKS
        assert plan["target"]["publication"] == asdict(H15) == plan["feature"]["publication"]
    assert fl.ForwardLog(tmp_path / "log").verify_chain()["ok"]
    with pytest.raises(fl.Refused, match="already admitted"):
        admit(tmp_path / "log", scan_dir)


def _set_origin(origin):
    def change(payload):
        payload["protocol"]["origin"] = origin
    return change


def _drop_self_lag(payload):
    del payload["protocol"]["self_lag"]


def _fixed_step(payload):
    payload["candidate_eligible"] = False


def _tamper_payload(scan):
    path = scan / "run" / "discovery-frozen.json"
    frozen = read(path)
    frozen["payload"]["fdr_q_tampered"] = True
    write(path, frozen)


def _denied_manifest(scan):
    path = scan / "run" / "discovery-frozen.json"
    frozen = read(path)
    frozen["sha256"] = next(iter(fl.DENIED_DISCOVERY_MANIFESTS))
    write(path, frozen)


def _relabelled(scan):
    candidates = read(scan / "frozen-candidates.json")
    (scan / fl.RELABELLED_FILE).write_text(
        json.dumps(rp.relabel_frozen_candidates(candidates)), encoding="utf-8"
    )


def _summary_origin(scan):
    summary = read(scan / "summary.json")
    summary["origin"] = "exploratory_replay"
    write(scan / "summary.json", summary)


def _short_code_sha(scan):
    summary = read(scan / "summary.json")
    summary["code_sha"] = "ef0d564b"
    write(scan / "summary.json", summary)


def _summary_spec_edit(scan):
    summary = read(scan / "summary.json")
    summary["feature_specs"][0]["stale_sessions"] = 50
    write(scan / "summary.json", summary)


def _drop_one_candidate(scan):
    candidates = read(scan / "frozen-candidates.json")[:-1]
    write(scan / "frozen-candidates.json", candidates)
    holdout = read(scan / "run" / "holdout-result.json")
    holdout["candidates"] = candidates
    write(scan / "run" / "holdout-result.json", holdout)


def _candidate_field(key, value):
    def mutate(scan):
        rewrite_candidates(scan, lambda c: c.__setitem__(key, value), resign=False)
    return mutate


def _spec_field(key, value):
    def mutate(scan):
        rewrite_candidates(scan, lambda c: c["specification"].__setitem__(key, value))
    return mutate


def _unsigned_spec(scan):
    rewrite_candidates(
        scan, lambda c: c["specification"].__setitem__("direction", -c["specification"]["direction"]),
        resign=False,
    )


@pytest.mark.parametrize(
    "mutate, match",
    [
        (lambda s: resign_payload(s, _set_origin("exploratory_replay")), "exploratory_replay"),
        (lambda s: resign_payload(s, _set_origin("synthetic_fixture")), "synthetic_fixture"),
        (lambda s: resign_payload(s, _set_origin("pit_vintage_read")), "pit_vintage_read"),
        (_summary_origin, "scan summary: origin"),
        (_spec_field("origin", "exploratory_replay"), "origin 'exploratory_replay' refused"),
        (lambda s: resign_payload(s, _drop_self_lag), "predates S09b"),
        (lambda s: resign_payload(s, _fixed_step), "not candidate-eligible"),
        (_tamper_payload, "does not match its payload"),
        (_denied_manifest, "ef0d564b"),
        (_relabelled, "relabelled"),
        (_short_code_sha, "not a full commit sha"),
        (_summary_spec_edit, "does not rebuild the signed read receipt"),
        (_drop_one_candidate, "exactly the holdout survivors"),
        (_candidate_field("state", "RESCAN_REQUIRED"), "RESCAN_REQUIRED"),
        (_candidate_field("state", "SELF_LAG_NEVER_A_CANDIDATE"), "SELF_LAG_NEVER"),
        (_candidate_field("state", "SYNTHETIC_PROOF_ONLY"), "SYNTHETIC_PROOF_ONLY"),
        (_candidate_field("promotion_allowed", True), "promotion_allowed"),
        (_spec_field("feature", "FEAT_D|chg5"), "SELF_LAG"),
        (_spec_field("feature", "FEAT_N|chg5"), "not a selected discovery trial"),
        (_spec_field("direction", -1), "not a selected discovery trial"),
        (_spec_field("forward_start_not_before", "2021-06-01T00:00:00+00:00"), "frozen before"),
        (_unsigned_spec, "does not match its specification"),
    ],
)
def test_ineligible_scans_are_refused_whole(scan_dir, tmp_path, mutate, match):
    scan = copy_scan(scan_dir, tmp_path)
    mutate(scan)
    with pytest.raises(fl.Refused, match=match):
        admit(tmp_path / "log", scan)
    assert fl.ForwardLog(tmp_path / "log").read_all() == []  # all or nothing


def test_one_bad_candidate_refuses_the_whole_scan(scan_dir, tmp_path):
    scan = copy_scan(scan_dir, tmp_path)
    candidates = read(scan / "frozen-candidates.json")
    candidates[-1]["specification"]["feature"] = "FEAT_D|z60"
    candidates[-1]["sha256"] = orp.digest(candidates[-1]["specification"])
    write(scan / "frozen-candidates.json", candidates)
    holdout = read(scan / "run" / "holdout-result.json")
    holdout["candidates"] = candidates
    write(scan / "run" / "holdout-result.json", holdout)
    with pytest.raises(fl.Refused, match="SELF_LAG"):
        admit(tmp_path / "log", scan)
    assert not (tmp_path / "log" / fl.LOG_FILENAME).exists()


@pytest.mark.parametrize(
    "repo, match",
    [
        (FakeRepo(includes=False), "does not include #661"),
        (FakeRepo(files={"store/observations.py": b"changed\n"}), "store/observations.py differs"),
        (FakeRepo(files={"analysis/research_real_panel.py": None}), "research_real_panel.py differs"),
    ],
)
def test_scan_code_must_include_661_and_match_its_files(scan_dir, tmp_path, repo, match):
    with pytest.raises(fl.Refused, match=match):
        admit(tmp_path / "log", scan_dir, repo=repo)


def test_admission_before_forward_start_is_refused(scan_dir, tmp_path):
    with pytest.raises(fl.Refused, match="frozen before"):
        admit(tmp_path / "log", scan_dir, now=SCAN_AS_OF_TS - timedelta(hours=1))


def test_ef0d564b_shaped_scan_is_refused(scan_dir, tmp_path):
    """The first real scan: pit_vintage_read origin, pre-S09b code, relabelled copy."""
    scan = copy_scan(scan_dir, tmp_path)
    resign_payload(scan, _set_origin("pit_vintage_read"))
    rewrite_candidates(scan, lambda c: c["specification"].__setitem__("origin", "pit_vintage_read"))
    with pytest.raises(fl.Refused, match="pit_vintage_read"):
        admit(tmp_path / "log", scan)
    _relabelled(scan)
    with pytest.raises(fl.Refused, match="relabelled"):
        admit(tmp_path / "log", scan)


EF0D564B = Path(
    "C:/Users/owner/Documents/Codex/2026-09-14/wha/outputs/hypothesis-loop-20260926/scan-ef0d564b"
)


@pytest.mark.skipif(not EF0D564B.exists(), reason="operator artifact only on the operator laptop")
def test_the_real_ef0d564b_artifact_is_refused(tmp_path):
    with pytest.raises(fl.Refused):
        admit(tmp_path / "log", EF0D564B, repo=fl.GitRepo(REPO))


def test_git_repo_ancestry_and_file_lookup(tmp_path):
    if shutil.which("git") is None:
        pytest.skip("git not installed")
    root = tmp_path / "repo"
    root.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid",
             "-c", "commit.gpgsign=false", "-C", str(root), *args],
            check=True, capture_output=True, text=True,
        ).stdout.strip()

    git("init", "-q")
    (root / "a.py").write_text("one\n", encoding="utf-8")
    git("add", "a.py")
    git("commit", "-q", "-m", "one")
    first = git("rev-parse", "HEAD")
    (root / "a.py").write_text("two\n", encoding="utf-8")
    git("commit", "-q", "-am", "two")
    second = git("rev-parse", "HEAD")
    repo = fl.GitRepo(root)
    assert repo.is_ancestor(first, second) and not repo.is_ancestor(second, first)
    assert not repo.is_ancestor("0" * 40, second)
    assert repo.file_bytes(first, "a.py").replace(b"\r\n", b"\n") == b"one\n"
    assert repo.file_bytes(first, "missing.py") is None


# --- hash chain ------------------------------------------------------------------------


@pytest.fixture()
def chained(scan_dir, tmp_path):
    log_dir = tmp_path / "log"
    admit(log_dir, scan_dir)
    with build_engine().connect() as conn:
        for day in (5, 13):  # the 07-05 prediction on time, then its outcome
            fl.run_forward(fl.ForwardLog(log_dir), conn,
                           datetime(2021, 7, day, 6, tzinfo=timezone.utc), "c" * 40)
    log = fl.ForwardLog(log_dir)
    assert log.verify_chain()["ok"] and len(log.read_all()) >= 6
    return log


def lines_of(log):
    return log.path.read_bytes().split(b"\n")[:-1]


def put_lines(log, lines):
    log.path.write_bytes(b"\n".join(lines) + b"\n")


def _edit_value(lines):
    record = json.loads(lines[2])
    record["plan"]["alpha"] = 0.05
    lines[2] = fl.canonical(record)


def _rechain_with_new_prereg(lines):
    """An attacker rewrites the header and re-chains every later line."""
    previous = None
    out = []
    for i, line in enumerate(lines):
        record = json.loads(line)
        if i == 0:
            record["prereg_sha256"] = "f" * 64
        record["prev_sha256"] = previous
        line = fl.canonical(record)
        previous = hashlib.sha256(line).hexdigest()
        out.append(line)
    lines[:] = out


@pytest.mark.parametrize(
    "tamper, match",
    [
        (_edit_value, "record 3: prev_sha256"),
        (lambda lines: lines.pop(2), "record 2: prev_sha256"),
        (lambda lines: lines.insert(2, lines.pop(3)), "record 2: prev_sha256"),
        (lambda lines: lines.__setitem__(1, lines[1].replace(b'","', b'", "', 1)), "not canonical"),
        (lambda lines: lines.__setitem__(1, b"{not json"), "not JSON"),
        (_rechain_with_new_prereg, "record 0: first record is not the pinned header"),
        (lambda lines: lines.insert(3, lines[0]), "record 3"),
    ],
)
def test_chain_detects_tampering_and_blocks_appends(chained, tamper, match):
    lines = lines_of(chained)
    tamper(lines)
    put_lines(chained, lines)
    check = chained.verify_chain()
    assert not check["ok"] and match in check["detail"]
    with pytest.raises(RuntimeError, match="chain is broken"):
        fl.run_forward(chained, None, datetime(2021, 8, 2, tzinfo=timezone.utc), "c" * 40)
    with pytest.raises(RuntimeError, match="chain is broken"):
        chained.append([{"kind": "note"}])
    text = fl.format_status(fl.status_report(chained))
    assert "BROKEN" in text
    assert cli.main(["verify", "--log-dir", str(chained.log_dir)]) == 1


def test_empty_log_run_writes_the_header_only(tmp_path):
    log = fl.ForwardLog(tmp_path / "log")
    written = fl.run_forward(log, None, datetime(2026, 9, 27, 6, 20, tzinfo=timezone.utc), "d" * 40)
    assert [r["kind"] for r in written] == ["header"]
    assert fl.run_forward(log, None, datetime(2026, 9, 28, 6, 20, tzinfo=timezone.utc), "d" * 40) == []
    assert log.verify_chain() == {
        "ok": True, "records": 1,
        "head_sha256": hashlib.sha256(lines_of(log)[0]).hexdigest(), "detail": None,
    }
    assert cli.main(["verify", "--log-dir", str(log.log_dir)]) == 0
    assert cli.main(["status", "--log-dir", str(log.log_dir)]) == 0


# --- no-lookahead evaluation ---------------------------------------------------------


def test_predictions_precede_outcomes_and_follow_the_freeze(chained):
    records = chained.read_all()
    admissions = {r["candidate_id"]: r for r in records if r["kind"] == "admission"}
    predictions = [r for r in records if r["kind"] == "prediction"]
    outcomes = [r for r in records if r["kind"] == "outcome"]
    assert predictions and outcomes
    for p in predictions:
        decided = pd.Timestamp(p["decision_at"])
        assert decided > pd.Timestamp(admissions[p["candidate_id"]]["run_at"])
        assert pd.Timestamp(p["feature"]["known_at"]) <= decided
        assert decided <= pd.Timestamp(p["run_at"]) < pd.Timestamp(p["label_known_at"])
    for o in outcomes:
        assert pd.Timestamp(o["run_at"]) >= pd.Timestamp(o["label_known_at"])
    # the decision of 07-12 has label end 07-13, published 07-14 21:17Z: no outcome yet
    assert {o["decision_at"][:10] for o in outcomes} == {"2021-07-05"}
    assert {p["decision_at"][:10] for p in predictions} == {"2021-07-05", "2021-07-12"}


def test_prediction_ignores_values_pulled_after_the_decision(scan_dir, tmp_path):
    """A revision pulled after the decision instant cannot reach the prediction."""
    decision = pd.Timestamp("2021-07-12T00:00:00+00:00")
    last_usable = date(2021, 7, 8)  # published 07-09 21:17Z, usable at 07-12 00:00Z
    after = datetime(2021, 7, 12, 3, 0)  # noqa: DTZ001 - naive UTC as stored
    before = datetime(2021, 7, 11, 3, 0)  # noqa: DTZ001
    admit(tmp_path / "log", scan_dir)
    plan = next(r for r in fl.ForwardLog(tmp_path / "log").read_all()
                if r["kind"] == "admission" and r["plan"]["feature"]["name"] == "FEAT_X|chg5")["plan"]

    def predicted(engine):
        with engine.connect() as conn:
            return fl.read_prediction(conn, plan, decision)

    clean = predicted(build_engine())
    late = predicted(build_engine(revision=(last_usable, 999.0, after)))
    early = predicted(build_engine(revision=(last_usable, 999.0, before)))
    assert clean[0] is not None and late[0] == clean[0]
    assert early[0] != clean[0]  # the control: a pre-decision pull does change it
    assert pd.Timestamp(clean[1]) <= decision


def test_late_predictions_are_excluded_not_backfilled(scan_dir, tmp_path):
    log_dir = tmp_path / "log"
    admit(log_dir, scan_dir)
    log = fl.ForwardLog(log_dir)
    with build_engine().connect() as conn:
        fl.run_forward(log, conn, datetime(2021, 7, 21, 6, tzinfo=timezone.utc), "c" * 40)
    predictions = [r for r in log.read_all() if r["kind"] == "prediction"]
    by_day = {}
    for p in predictions:
        by_day.setdefault(p["decision_at"][:10], set()).add(p["exclusion_reason"])
    # labels of 07-05 and 07-12 were already published: late; 07-19's is not
    assert by_day == {"2021-07-05": {"late_prediction"}, "2021-07-12": {"late_prediction"},
                      "2021-07-19": {None}}
    assert all(p["feature"]["value"] is None for p in predictions if p["excluded"])


def test_missing_target_waits_for_the_grace_period_then_is_excluded(scan_dir, tmp_path):
    log_dir = tmp_path / "log"
    admit(log_dir, scan_dir)
    log = fl.ForwardLog(log_dir)
    engine = build_engine(drop=("TGT", date(2021, 7, 6)))  # label end of decision 07-05
    with engine.connect() as conn:
        # label known 07-07 21:17Z; grace ends 07-12 21:17Z
        for day in (5, 8, 12):
            fl.run_forward(log, conn, datetime(2021, 7, day, 6, tzinfo=timezone.utc), "c" * 40)
        assert not [r for r in log.read_all() if r["kind"] == "outcome"]
        fl.run_forward(log, conn, datetime(2021, 7, 13, 6, tzinfo=timezone.utc), "c" * 40)
    outcomes = [r for r in log.read_all() if r["kind"] == "outcome"]
    assert outcomes and {o["exclusion_reason"] for o in outcomes} == {"target_missing"}


def _pair(value=1.0, label=0.5, **overrides):
    admission = {"run_at": "2021-07-02T09:00:00+00:00"}
    prediction = {
        "decision_at": "2021-07-05T00:00:00+00:00",
        "label_known_at": "2021-07-07T21:17:00+00:00",
        "run_at": "2021-07-05T06:00:00+00:00",
        "excluded": False,
        "feature": {"value": value, "known_at": "2021-07-05T00:00:00+00:00"},
    }
    outcome = {
        "decision_at": "2021-07-05T00:00:00+00:00",
        "run_at": "2021-07-08T06:00:00+00:00",
        "excluded": False,
        "label": label,
    }
    for key, value in overrides.items():
        part, field = key.split("__")
        target = {"admission": admission, "prediction": prediction, "outcome": outcome}[part]
        if field == "feature_known_at":
            target["feature"]["known_at"] = value
        else:
            target[field] = value
    return admission, prediction, outcome


@pytest.mark.parametrize(
    "overrides",
    [
        {"admission__run_at": "2021-07-05T00:00:00+00:00"},  # decision not after the freeze
        {"prediction__feature_known_at": "2021-07-05T00:00:01+00:00"},  # feature from the future
        {"prediction__run_at": "2021-07-07T21:17:00+00:00"},  # logged once the label was known
        {"prediction__run_at": "2021-07-04T23:00:00+00:00"},  # logged before the decision
        {"outcome__run_at": "2021-07-07T21:16:59+00:00"},  # outcome read before publication
        {"outcome__decision_at": "2021-07-12T00:00:00+00:00"},  # mismatched pair
        {"prediction__excluded": True},
        {"outcome__excluded": True},
        {"outcome__label": None},
    ],
)
def test_evaluation_drops_any_pair_that_breaks_a_no_lookahead_rule(overrides):
    assert fl.valid_pair(*_pair())
    assert not fl.valid_pair(*_pair(**overrides))


# --- stop rule -----------------------------------------------------------------------


def make_entry(values, labels, direction=1, excluded=(), min_n=30, pending=()):
    plan = {
        "min_n": min_n, "max_decisions": 2 * min_n, "direction": direction,
        "statistic": "spearman", "block": 1, "perms": 999, "seed": 1, "alpha": 0.025,
    }
    admission = {"candidate_id": "x" * 64, "plan": plan, "plan_sha256": orp.digest(plan),
                 "run_at": "2021-07-02T09:00:00+00:00"}
    entry = {"admission": admission, "predictions": {}, "outcomes": {}, "verdict": None}
    for k, (value, label) in enumerate(zip(values, labels)):
        decided = pd.Timestamp("2021-07-05T00:00:00+00:00") + pd.offsets.BDay(5 * k)
        known = decided + pd.Timedelta(days=2, hours=21)
        entry["predictions"][k] = {
            "decision_at": decided.isoformat(), "label_known_at": known.isoformat(),
            "run_at": (decided + pd.Timedelta(hours=6)).isoformat(),
            "excluded": k in excluded,
            "feature": {"value": None if k in excluded else value, "known_at": decided.isoformat()},
        }
        if k not in excluded and k not in pending:
            entry["outcomes"][k] = {"decision_at": decided.isoformat(),
                                    "run_at": (known + pd.Timedelta(hours=9)).isoformat(),
                                    "excluded": False, "label": label}
    return entry


def test_no_verdict_before_min_n_valid_pairs():
    rng = np.random.default_rng(1)
    x = rng.normal(size=29)
    assert fl.verdict(make_entry(x, x)) is None
    # an unresolved earlier decision blocks the look even when 30 later pairs exist
    x = rng.normal(size=40)
    assert fl.verdict(make_entry(x, x, pending=(3,))) is None


def test_single_look_uses_exactly_the_first_min_n_valid_pairs():
    rng = np.random.default_rng(2)
    x = rng.normal(size=45)
    y = x + rng.normal(0, 0.3, 45)
    first = fl.verdict(make_entry(x, y, excluded=(4, 9)))
    assert first["state"] == fl.SUPPORTED and first["n"] == 30
    assert first["decisions_resolved"] == 32 and first["promotion_allowed"] is False
    later = y.copy()
    later[32:] = -later[32:] * 100  # pairs after the look cannot change it
    assert fl.verdict(make_entry(x, later, excluded=(4, 9))) == first


def test_wrong_direction_or_weak_evidence_fails():
    rng = np.random.default_rng(3)
    x = rng.normal(size=30)
    wrong = fl.verdict(make_entry(x, x, direction=-1))
    assert wrong["state"] == fl.FAILED and wrong["rho"] > 0.99
    noise = fl.verdict(make_entry(x, rng.normal(size=30)))
    assert noise["state"] == fl.FAILED and noise["p_one_sided"] > 0.025
    constant = fl.verdict(make_entry(np.ones(30), x))
    assert constant["state"] == fl.FAILED and constant["reason"] == "constant input"


def test_inconclusive_stop_after_max_decisions():
    rng = np.random.default_rng(4)
    x = rng.normal(size=60)
    result = fl.verdict(make_entry(x, x, excluded=(1, *range(0, 60, 2))))
    assert result["state"] == fl.INCONCLUSIVE and result["n"] == 29
    assert result["decisions_resolved"] == 60 and result["rho"] is None


def test_status_never_shows_a_correlation_before_the_verdict(chained):
    report = fl.status_report(chained)
    assert all(row["verdict"] is None for row in report["candidates"])
    text = fl.format_status(report)
    assert "rho" not in text and "Verdict" not in text
    assert "valid pairs 0 / 30" in text or "valid pairs 1 / 30" in text


# --- end to end ----------------------------------------------------------------------


def test_forward_run_end_to_end_reaches_one_verdict_per_candidate(scan_dir, tmp_path):
    log_dir = tmp_path / "log"
    admit(log_dir, scan_dir)
    log = fl.ForwardLog(log_dir)
    engine = build_engine()
    with engine.connect() as conn:
        for day in pd.bdate_range("2021-07-05", "2022-06-30"):
            fl.run_forward(log, conn, day.to_pydatetime().replace(hour=6, tzinfo=timezone.utc),
                           "c" * 40)
    records = log.read_all()
    assert log.verify_chain()["ok"]
    verdicts = {r["candidate_id"]: r for r in records if r["kind"] == "verdict"}
    admissions = {r["candidate_id"]: r for r in records if r["kind"] == "admission"}
    assert set(verdicts) == set(admissions)
    for cid, v in verdicts.items():
        assert v["n"] == 30 and v["promotion_allowed"] is False
        index = records.index(v)
        assert not [r for r in records[index + 1:] if r.get("candidate_id") == cid]
    chg5 = next(v for cid, v in verdicts.items()
                if admissions[cid]["plan"]["feature"]["name"] == "FEAT_X|chg5")
    assert chg5["state"] == fl.SUPPORTED and chg5["rho"] > 0
    assert all(r.get("promotion_allowed") in (None, False) for r in records)
    text = fl.write_status(log)
    assert "Verdict: FORWARD_SUPPORTED_REVIEW_REQUIRED" in text
    assert (log_dir / fl.STATUS_FILENAME).read_text(encoding="utf-8") == text


def test_module_never_names_the_registry_tables_or_builds_sql():
    for path in ("analysis/research_forward_log.py", "scripts/research_forward_log.py"):
        source = (REPO / path).read_text(encoding="utf-8")
        for forbidden in ("hypothesis_registry", "discovered_hypotheses", "scanner_weights",
                          "INSERT", "UPDATE ", "DELETE "):
            assert forbidden not in source, (path, forbidden)
        assert not re.search(r"(?<![\w.])text\(", source), path  # no SQLAlchemy text()




@pytest.mark.parametrize("name", ["bad name", "x;DROP", "a-b"])
def test_read_only_engine_refuses_unsafe_application_names(name):
    with pytest.raises(ValueError, match="application_name"):
        scan_script.read_only_engine(60, name)
