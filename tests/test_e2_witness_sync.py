"""The E2 scoreboard's off-host witness runs from its own vault clone, never the GEX mirror's.

* The E2 unit templates never reference ``obsidian-vault-paperlog`` (the GEX
  paper-log mirror's exclusive clone, whose mirror refuses to run while that
  clone has changes outside its own folder) and point ``--witness-worktree``
  at ``/home/grid/dev/obsidian-vault-e2witness``.
* ``ExecStopPost`` runs ``scripts/e2_witness_sync.sh`` from the release tree on
  that clone (so anchors are pushed after failed runs too).
* The script, exercised against a throwaway origin with a stub sync script:
  commits ONLY ``05-GRID/Paper-Log/e2/`` (plus its ``*.jsonl -text``
  attributes) and pushes; the pushed history passes the real
  ``evals.e2.witness.check_offhost``; it refuses (commits and pushes nothing)
  a clone with changes elsewhere, a partial last line, an edited or deleted
  anchor file, an unexpected file, a rename out of the folder, the paperlog
  clone, a clone off main, and a held scoreboard lock; a push that does not
  land fails loudly; no empty commits.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SYSTEMD = REPO / "deploy" / "systemd"
SCRIPT = REPO / "scripts" / "e2_witness_sync.sh"
WITNESS_CLONE = "/home/grid/dev/obsidian-vault-e2witness"
FOLDER = "05-GRID/Paper-Log/e2"


def _joined(text: str) -> list[str]:
    out, buf = [], ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.endswith("\\"):
            buf += line[:-1] + " "
            continue
        out.append(buf + line)
        buf = ""
    return out


def _value(lines: list[str], key: str) -> str | None:
    for line in lines:
        if line.strip().startswith(f"{key}="):
            return line.strip()[len(key) + 1:]
    return None


@pytest.mark.unit
def test_e2_templates_never_reference_the_gex_paperlog_clone():
    templates = sorted(SYSTEMD.glob("grid-e2-scoreboard.*"))
    assert [t.name for t in templates] == ["grid-e2-scoreboard.service.template",
                                           "grid-e2-scoreboard.timer.template"]
    for path in templates:  # not even in a comment
        assert "obsidian-vault-paperlog" not in path.read_text(encoding="utf-8"), path.name


@pytest.mark.unit
def test_service_witnesses_into_the_dedicated_clone_and_syncs_after_every_run():
    lines = _joined((SYSTEMD / "grid-e2-scoreboard.service.template").read_text(encoding="utf-8"))
    exec_start = _value(lines, "ExecStart")
    assert exec_start is not None and f"--witness-worktree {WITNESS_CLONE}" in exec_start
    stop_post = _value(lines, "ExecStopPost")
    assert stop_post == f"/bin/bash /data/grid_v4/grid_release/scripts/e2_witness_sync.sh {WITNESS_CLONE}"
    assert SCRIPT.is_file()
    assert b"\r\n" not in SCRIPT.read_bytes(), "the script must have LF line endings"
    assert f'CLONE="${{1:-{WITNESS_CLONE}}}"' in SCRIPT.read_text(encoding="utf-8")


def _tools() -> str | None:
    for tool in ("bash", "git", "flock", "cmp", "od", "realpath"):
        if shutil.which(tool) is None:
            return tool
    return None


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True).stdout


ANCHORS = f"{FOLDER}/e2_scoreboard_e2-v1.anchors.jsonl"
LINE1 = b'{"head_sha256":"a","prev_anchor_sha256":null,"records":1,"run_at":"t1"}\n'
LINE2 = b'{"head_sha256":"b","prev_anchor_sha256":"x","records":2,"run_at":"t2"}\n'


class Witness:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.origin = tmp_path / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(self.origin)], check=True)
        seed = tmp_path / "seed"
        subprocess.run(["git", "clone", "-q", str(self.origin), str(seed)], check=True, capture_output=True)
        _git(seed, "config", "user.name", "t")
        _git(seed, "config", "user.email", "t@t")
        _git(seed, "checkout", "-q", "-b", "main")
        (seed / "README.md").write_text("vault\n", encoding="utf-8")
        _git(seed, "add", "-A")
        _git(seed, "commit", "-q", "-m", "seed")
        _git(seed, "push", "-q", "origin", "main")
        self.clone = tmp_path / "obsidian-vault-e2witness"
        subprocess.run(["git", "clone", "-q", str(self.origin), str(self.clone)], check=True, capture_output=True)
        _git(self.clone, "config", "user.name", "e2")
        _git(self.clone, "config", "user.email", "e2@t")
        self.good_sync = self._stub("sync.sh", b"git fetch -q origin main\ngit merge -q --ff-only origin/main\n"
                                              b"git push -q origin main\n")
        self.dead_sync = self._stub("dead-sync.sh", b"exit 0\n")
        self.lock = tmp_path / "scoreboard.lock"
        self.paperlog = tmp_path / "obsidian-vault-paperlog"

    def _stub(self, name: str, body: bytes) -> Path:
        stub = self.tmp / name
        stub.write_bytes(b'#!/usr/bin/env bash\nset -e\ncd "$1"\n' + body)
        stub.chmod(0o755)
        return stub

    def run(self, path: Path | None = None, sync: Path | None = None) -> subprocess.CompletedProcess:
        env = {**os.environ, "E2_VAULT_SYNC": str(sync or self.good_sync), "E2_WITNESS_LOCK_WAIT_S": "2",
               "E2_SCOREBOARD_LOCK": str(self.lock), "E2_PAPERLOG_CLONE": str(self.paperlog)}
        return subprocess.run(["bash", str(SCRIPT), str(path or self.clone)], env=env, capture_output=True,
                              text=True)

    def write(self, data: bytes) -> None:
        (self.clone / FOLDER).mkdir(parents=True, exist_ok=True)
        (self.clone / ANCHORS).write_bytes(data)

    def origin_head(self) -> str:
        return _git(self.origin, "rev-parse", "main")


@pytest.fixture
def w(tmp_path):
    missing = _tools()
    if missing:
        pytest.skip(f"{missing} is not available")
    return Witness(tmp_path)


def test_commits_and_pushes_only_the_e2_folder_and_check_offhost_accepts_it(w):
    from evals.e2 import witness

    w.write(LINE1)
    result = w.run()
    assert result.returncode == 0, result.stdout + result.stderr
    changed = _git(w.origin, "show", "--name-only", "--format=", "main").split()
    assert sorted(changed) == [f"{FOLDER}/.gitattributes", ANCHORS]
    assert _git(w.origin, "show", f"main:{FOLDER}/.gitattributes") == "*.jsonl -text\n"
    assert "pushed" in result.stdout
    before = w.origin_head()
    again = w.run()
    assert again.returncode == 0 and "no new anchor lines" in again.stdout
    assert w.origin_head() == before  # no empty commit
    w.write(LINE1 + LINE2)
    assert w.run().returncode == 0
    offhost = witness.check_offhost(w.clone, "e2-v1", remote_url=str(w.origin))
    assert [v["covered_records"] for v in offhost["versions"]] == [1, 2]
    assert offhost["content"] == LINE1 + LINE2


@pytest.mark.parametrize("case", ["partial_line", "edited", "deleted", "unexpected_file"])
def test_refuses_anything_but_a_pure_append(w, case):
    w.write(LINE1)
    assert w.run().returncode == 0
    before = w.origin_head()
    if case == "partial_line":
        w.write(LINE1 + LINE2[:-10])
    elif case == "edited":
        w.write(LINE2 + LINE1)
    elif case == "deleted":
        (w.clone / ANCHORS).unlink()
    else:
        w.write(LINE1 + LINE2)
        (w.clone / FOLDER / "notes.md").write_text("x\n", encoding="utf-8")
    result = w.run()
    assert result.returncode == 4, result.stdout + result.stderr
    assert "not a pure append" in result.stdout
    assert w.origin_head() == before
    assert len(_git(w.clone, "log", "--format=%H").split()) == 2  # seed + the first anchors commit


def test_refuses_a_clone_with_changes_outside_the_folder(w):
    before = w.origin_head()
    w.write(LINE1)
    (w.clone / "70-Inbox").mkdir()
    (w.clone / "70-Inbox" / "stray.md").write_text("x\n", encoding="utf-8")
    result = w.run()
    assert result.returncode == 3 and "outside" in result.stdout
    assert w.origin_head() == before
    assert _git(w.clone, "log", "-1", "--format=%s") == "seed\n"


def test_refuses_a_rename_out_of_the_folder(w):
    w.write(LINE1)
    assert w.run().returncode == 0
    before = w.origin_head()
    _git(w.clone, "mv", ANCHORS, "moved.jsonl")
    result = w.run()
    assert result.returncode == 3, result.stdout
    assert w.origin_head() == before


def test_refuses_the_gex_paperlog_clone_and_a_clone_off_main(w):
    w.clone.rename(w.paperlog)
    result = w.run(w.paperlog)
    assert result.returncode == 2 and "paper-log mirror" in result.stdout
    w.paperlog.rename(w.clone)
    _git(w.clone, "checkout", "-q", "-b", "side")
    result = w.run()
    assert result.returncode == 2 and "not on main" in result.stdout


def test_waits_for_a_running_scoreboard_and_commits_nothing(w):
    import fcntl

    w.write(LINE1)
    before = w.origin_head()
    with open(w.lock, "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        result = w.run()
    assert result.returncode == 0 and "holds" in result.stdout
    assert w.origin_head() == before
    assert _git(w.clone, "log", "-1", "--format=%s") == "seed\n"


def test_no_folder_yet_is_a_no_op(w):
    result = w.run()
    assert result.returncode == 0 and "no 05-GRID/Paper-Log/e2 yet" in result.stdout


def test_a_push_that_never_lands_fails_loudly(w):
    w.write(LINE1)
    result = w.run(sync=w.dead_sync)
    assert result.returncode == 5 and "not on origin/main" in result.stdout
