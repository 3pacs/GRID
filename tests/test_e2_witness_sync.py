"""The E2 scoreboard's off-host witness runs from its own vault clone, never the GEX mirror's.

* The E2 unit templates never reference ``obsidian-vault-paperlog`` (the GEX
  paper-log mirror's exclusive clone, whose mirror refuses to run while that
  clone has changes outside its own folder) and point ``--witness-worktree``
  at ``/home/grid/dev/obsidian-vault-e2witness``.
* ``ExecStopPost`` runs ``scripts/e2_witness_sync.sh`` from the release tree on
  that clone (so anchors are pushed after failed runs too).
* The script, exercised against a throwaway origin with a stub sync script:
  commits ONLY ``05-GRID/Paper-Log/e2/`` (plus its ``*.jsonl -text``
  attributes), pushes, refuses a clone with changes elsewhere, refuses the
  paperlog clone by name, and makes no empty commits.
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
    for tool in ("bash", "git", "flock"):
        if shutil.which(tool) is None:
            return tool
    return None


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True).stdout


@pytest.fixture
def witness(tmp_path):
    missing = _tools()
    if missing:
        pytest.skip(f"{missing} is not available")
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(origin)], check=True)
    seed = tmp_path / "seed"
    subprocess.run(["git", "clone", "-q", str(origin), str(seed)], check=True, capture_output=True)
    for repo in (seed,):
        _git(repo, "config", "user.name", "t")
        _git(repo, "config", "user.email", "t@t")
        _git(repo, "checkout", "-q", "-b", "main")
    (seed / "README.md").write_text("vault\n", encoding="utf-8")
    _git(seed, "add", "-A")
    _git(seed, "commit", "-q", "-m", "seed")
    _git(seed, "push", "-q", "origin", "main")
    clone = tmp_path / "obsidian-vault-e2witness"
    subprocess.run(["git", "clone", "-q", str(origin), str(clone)], check=True, capture_output=True)
    _git(clone, "config", "user.name", "e2")
    _git(clone, "config", "user.email", "e2@t")
    stub = tmp_path / "sync.sh"
    stub.write_bytes(b'#!/usr/bin/env bash\nset -e\ncd "$1"\ngit fetch -q origin main\n'
                     b'git merge -q --ff-only origin/main\ngit push -q origin main\n')
    stub.chmod(0o755)
    env = {**os.environ, "E2_VAULT_SYNC": str(stub), "E2_WITNESS_LOCK_WAIT_S": "5"}

    def run(path: Path = clone) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", str(SCRIPT), str(path)], env=env, capture_output=True, text=True)

    return origin, clone, run


def test_commits_and_pushes_only_the_e2_folder(witness):
    origin, clone, run = witness
    (clone / FOLDER).mkdir(parents=True)
    (clone / FOLDER / "e2_scoreboard_e2-v1.anchors.jsonl").write_bytes(b'{"records":1}\n')
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    changed = _git(origin, "show", "--name-only", "--format=", "main").split()
    assert sorted(changed) == [f"{FOLDER}/.gitattributes", f"{FOLDER}/e2_scoreboard_e2-v1.anchors.jsonl"]
    assert _git(origin, "show", f"main:{FOLDER}/.gitattributes") == "*.jsonl -text\n"
    assert "pushed" in result.stdout
    before = _git(origin, "rev-parse", "main")
    again = run()
    assert again.returncode == 0 and "no new anchor lines" in again.stdout
    assert _git(origin, "rev-parse", "main") == before  # no empty commit


def test_refuses_a_clone_with_changes_outside_the_folder(witness):
    origin, clone, run = witness
    before = _git(origin, "rev-parse", "main")
    (clone / FOLDER).mkdir(parents=True)
    (clone / FOLDER / "e2_scoreboard_e2-v1.anchors.jsonl").write_bytes(b'{"records":1}\n')
    (clone / "70-Inbox").mkdir()
    (clone / "70-Inbox" / "stray.md").write_text("x\n", encoding="utf-8")
    result = run()
    assert result.returncode == 3 and "outside" in result.stdout
    assert _git(origin, "rev-parse", "main") == before
    assert _git(clone, "log", "-1", "--format=%s") == "seed\n"


def test_refuses_the_gex_paperlog_clone(witness, tmp_path):
    _, clone, run = witness
    paperlog = tmp_path / "obsidian-vault-paperlog"
    clone.rename(paperlog)
    result = run(paperlog)
    assert result.returncode == 2 and "paper-log mirror" in result.stdout
