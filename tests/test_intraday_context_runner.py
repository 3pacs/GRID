"""Concurrency and failure reporting controls for the offline runner."""

import json
from threading import Barrier
from types import SimpleNamespace

import pytest

from scripts.intraday_lab import run_context_tests as runner


def test_lanes_overlap_and_failure_is_not_pass(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    root.mkdir()
    for path in runner.LANES.values():
        target = root / path
        target.parent.mkdir(exist_ok=True)
        target.write_text("# fixture\n")
    monkeypatch.setattr(runner, "ROOT", root)
    barrier = Barrier(3)

    def fake_run(command, **kwargs):
        if command[0] == "git":
            return SimpleNamespace(returncode=0, stdout="fixture-head\n")
        barrier.wait(timeout=10)  # Serial execution fails this control.
        code = 1 if "sector" in command[4] else 0
        return SimpleNamespace(returncode=code, stdout="controls\n", stderr="")

    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    output = tmp_path / "receipt"
    receipt = runner.run_all(output)
    assert receipt["status"] == "FAIL"
    assert [r["status"] for r in receipt["lanes"]] == ["PASS", "FAIL", "PASS"]
    assert json.loads((output / "receipt.json").read_text()) == receipt
    with pytest.raises(FileExistsError):
        runner.run_all(output)


def test_timeout_reports_error(tmp_path, monkeypatch):
    test = tmp_path / "test.py"
    test.write_text("# fixture\n")
    monkeypatch.setattr(runner, "ROOT", tmp_path)

    def timeout(*args, **kwargs):
        raise runner.subprocess.TimeoutExpired("pytest", 180)

    monkeypatch.setattr(runner.subprocess, "run", timeout)
    receipt = runner.run_lane("timeout", "test.py", tmp_path)
    assert receipt["status"] == "ERROR"
    assert receipt["returncode"] is None


def test_missing_test_reports_error(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    receipt = runner.run_lane("missing", "missing.py", tmp_path)
    assert receipt["status"] == "ERROR"
    assert receipt["test_sha256"] is None
