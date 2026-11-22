"""The Desk receipt log's off-host witness: scripts/desk_receipts_witness_sync.sh (2026-11-21).

Modelled on tests/test_e2_witness_sync.py. Exercised against a throwaway origin with a stub sync
script: it exports with the real ``export-anchors`` command, commits ONLY
``05-GRID/Paper-Log/desk-receipts/`` (plus ``*.jsonl -text``) and pushes; the pushed copy is a
byte-exact prefix of the log's anchors and passes ``verify --external-anchors``; it refuses (commits
and pushes nothing) a clone with changes elsewhere, a non-append, an unexpected file, the paper-log
and E2 clones, a clone off main and a held receipt lock; an export refusal is loud; a push that does
not land fails loudly; no empty commits.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from intelligence import desk_source_receipts as dsr
from tests.test_desk_source_receipts import THU_CLOSE, _record, _source

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "desk_receipts_witness_sync.sh"
TEMPLATE = REPO / "deploy" / "systemd-user" / "grid-desk-receipts.service.template"
WITNESS_CLONE = "/home/grid/dev/obsidian-vault-deskwitness"
FOLDER = dsr.WITNESS_DIR
ANCHORS = f"{FOLDER}/{dsr.ANCHOR_NAME}"


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True).stdout


@pytest.mark.unit
def test_unit_runs_the_witness_after_every_receipt_run():
    lines = [ln.strip() for ln in TEMPLATE.read_text(encoding="utf-8").splitlines()]
    assert f"ExecStopPost=/bin/bash @CODE@/scripts/desk_receipts_witness_sync.sh {WITNESS_CLONE}" in lines
    stop = next(ln for ln in lines if ln.startswith("TimeoutStopSec="))
    assert stop.endswith("min") and int(stop.split("=")[1][:-3]) >= 5
    text = SCRIPT.read_text(encoding="utf-8")
    assert f'CLONE="${{1:-{WITNESS_CLONE}}}"' in text and f'FOLDER="{FOLDER}"' in text
    assert f'ANCHOR_NAME="{dsr.ANCHOR_NAME}"' in text
    assert b"\r\n" not in SCRIPT.read_bytes()
    assert "obsidian-vault-paperlog" not in TEMPLATE.read_text(encoding="utf-8")


class Witness:
    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.origin = tmp / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(self.origin)], check=True)
        seed = tmp / "seed"
        subprocess.run(["git", "clone", "-q", str(self.origin), str(seed)], check=True, capture_output=True)
        for k, v in (("user.name", "t"), ("user.email", "t@t")):
            _git(seed, "config", k, v)
        _git(seed, "checkout", "-q", "-b", "main")
        (seed / "README.md").write_text("vault\n", encoding="utf-8")
        _git(seed, "add", "-A"); _git(seed, "commit", "-q", "-m", "seed"); _git(seed, "push", "-q", "origin", "main")
        self.clone = tmp / "obsidian-vault-deskwitness"
        subprocess.run(["git", "clone", "-q", str(self.origin), str(self.clone)], check=True, capture_output=True)
        for k, v in (("user.name", "desk"), ("user.email", "desk@t")):
            _git(self.clone, "config", k, v)
        self.good_sync = self._stub("sync.sh", b"git fetch -q origin main\ngit merge -q --ff-only origin/main\ngit push -q origin main\n")
        self.dead_sync = self._stub("dead-sync.sh", b"exit 0\n")
        self.lock = tmp / "receipts.lock"
        self.log = dsr.ReceiptLog(tmp / "receipts")
        self.paperlog = tmp / "obsidian-vault-paperlog"
        self.e2 = tmp / "obsidian-vault-e2witness"

    def _stub(self, name: str, body: bytes) -> Path:
        stub = self.tmp / name
        stub.write_bytes(b'#!/usr/bin/env bash\nset -e\ncd "$1"\n' + body)
        stub.chmod(0o755)
        return stub

    def run(self, path: Path | None = None, sync: Path | None = None, export: str | None = None) -> subprocess.CompletedProcess:
        env = {**os.environ, "DESK_WITNESS_VAULT_SYNC": str(sync or self.good_sync), "DESK_WITNESS_LOCK_WAIT_S": "2",
               "DESK_WITNESS_LOCK": str(self.lock), "DESK_WITNESS_PAPERLOG_CLONE": str(self.paperlog),
               "DESK_WITNESS_E2CLONE": str(self.e2), "DESK_RECEIPTS_DIR": str(self.log.root),
               "DESK_RECEIPTS_PYTHON": sys.executable, "PYTHONPATH": str(REPO)}
        env.pop("DESK_WITNESS_EXPORT", None)
        if export:
            env["DESK_WITNESS_EXPORT"] = export
        return subprocess.run(["bash", str(SCRIPT), str(path or self.clone)], env=env, capture_output=True, text=True)

    def origin_head(self) -> str:
        return _git(self.origin, "rev-parse", "main")


@pytest.fixture
def w(tmp_path):
    for tool in ("bash", "git", "flock", "cmp", "od", "realpath"):
        if shutil.which(tool) is None:
            pytest.skip(f"{tool} is not available")
    return Witness(tmp_path)


def test_exports_commits_and_pushes_only_the_desk_folder(w):
    _record(w.log, {"s": _source({"v": 1})})
    result = w.run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert sorted(_git(w.origin, "show", "--name-only", "--format=", "main").split()) == [f"{FOLDER}/.gitattributes", ANCHORS]
    assert _git(w.origin, "show", f"main:{FOLDER}/.gitattributes") == "*.jsonl -text\n"
    before = w.origin_head()
    again = w.run()
    assert again.returncode == 0 and "no new anchor lines" in again.stdout
    assert w.origin_head() == before  # no empty commit
    _record(w.log, {"s": _source({"v": 2})}, at=THU_CLOSE + timedelta(minutes=30))
    assert w.run().returncode == 0
    pushed = subprocess.run(["git", "-C", str(w.origin), "show", f"main:{ANCHORS}"], check=True, capture_output=True).stdout
    assert pushed == w.log.anchor_path.read_bytes()
    external = w.tmp / "pushed.anchors.jsonl"
    external.write_bytes(pushed)
    assert w.log.verify(external, full=True)["ok"]


def test_an_export_refusal_is_loud_and_commits_nothing(w):
    _record(w.log, {"s": _source({"v": 1})})
    other = dsr.ReceiptLog(w.tmp / "other")       # the vault file already witnesses another log
    _record(other, {"s": _source({"v": "other"})})
    other.export_anchors(w.clone)
    _git(w.clone, "add", "-A"); _git(w.clone, "commit", "-q", "-m", "foreign"); before = _git(w.clone, "rev-parse", "HEAD")
    result = w.run()
    assert result.returncode == 6 and "export-anchors failed" in result.stdout, result.stdout + result.stderr
    assert _git(w.clone, "rev-parse", "HEAD") == before


@pytest.mark.parametrize("case", ["edited", "unexpected_file"])
def test_refuses_anything_but_a_pure_append(w, case):
    _record(w.log, {"s": _source({"v": 1})})
    assert w.run().returncode == 0
    before = w.origin_head()
    noop = w._stub("noop-export.sh", b"exit 0\n")
    if case == "edited":
        (w.clone / ANCHORS).write_bytes(b'{"junk":1}\n')
    else:
        (w.clone / FOLDER / "notes.md").write_text("x\n", encoding="utf-8")
    result = w.run(export=str(noop))
    assert result.returncode == 4 and "not a pure append" in result.stdout, result.stdout + result.stderr
    assert w.origin_head() == before


def test_refuses_a_clone_with_changes_outside_the_folder_before_exporting(w):
    _record(w.log, {"s": _source({"v": 1})})
    (w.clone / "70-Inbox").mkdir()
    (w.clone / "70-Inbox" / "stray.md").write_text("x\n", encoding="utf-8")
    result = w.run()
    assert result.returncode == 3 and "outside" in result.stdout
    assert not (w.clone / ANCHORS).exists(), "nothing written into a dirty clone"


def test_refuses_the_paperlog_and_e2_clones_and_a_clone_off_main(w):
    for other, word in ((w.paperlog, "paper-log mirror"), (w.e2, "E2 scoreboard")):
        w.clone.rename(other)
        result = w.run(other)
        assert result.returncode == 2 and word in result.stdout, result.stdout
        other.rename(w.clone)
    _git(w.clone, "checkout", "-q", "-b", "side")
    result = w.run()
    assert result.returncode == 2 and "not on main" in result.stdout


def test_waits_for_a_running_receipt_job_and_exports_nothing(w):
    import fcntl
    _record(w.log, {"s": _source({"v": 1})})
    with open(w.lock, "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        result = w.run()
    assert result.returncode == 0 and "holds" in result.stdout
    assert not (w.clone / ANCHORS).exists()


def test_an_empty_log_is_a_no_op_and_a_push_that_never_lands_fails_loudly(w):
    result = w.run()
    assert result.returncode == 0, result.stdout + result.stderr
    _record(w.log, {"s": _source({"v": 1})})
    result = w.run(sync=w.dead_sync)
    assert result.returncode == 5 and "not on origin/main" in result.stdout
