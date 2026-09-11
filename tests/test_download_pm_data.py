"""Regression tests for scripts/download_pm_data.py.

Covers only the CLI-zstd fallback extract path — the branch that historically
shelled out via `shell=True` and interpolated caller-controlled paths. Ensures
the replacement uses an argument-list `Popen`/`run` pair so paths with spaces
or shell metacharacters can never be interpreted by `/bin/sh`.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _import_module():
    import importlib

    return importlib.import_module("scripts.download_pm_data")


class _FakeCompleted:
    def __init__(self, returncode: int = 0, args=None):
        self.returncode = returncode
        self.args = args or []


class _FakeZstdProc:
    def __init__(self, returncode: int = 0):
        self.stdout = mock.MagicMock(name="stdout_pipe")
        self._returncode = returncode

    def wait(self) -> int:
        return self._returncode


def test_extract_fallback_uses_argv_not_shell(tmp_path, monkeypatch):
    """The CLI-zstd fallback must pass argv lists, never `shell=True`."""

    module = _import_module()

    data_dir = tmp_path / "malicious; rm -rf /"
    data_dir.mkdir()
    archive = data_dir / "data.tar.zst"
    archive.write_bytes(b"payload")
    sentinel = data_dir / ".download_complete"
    sentinel.touch()  # short-circuits download; we only care about the extract path

    # Simulate zstandard not being installed so we take the CLI fallback branch.
    monkeypatch.setitem(sys.modules, "zstandard", None)

    popen_calls: list[list[str]] = []
    run_calls: list[dict] = []

    def fake_popen(args, stdout=None, **_kwargs):
        popen_calls.append(list(args))
        return _FakeZstdProc(returncode=0)

    def fake_run(args, **kwargs):
        run_calls.append({"args": list(args), "kwargs": kwargs})
        return _FakeCompleted(returncode=0, args=list(args))

    monkeypatch.setattr(module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(module.subprocess, "run", fake_run)

    # sentinel exists so `download_and_extract` returns before touching subprocess.
    # Drop it so we walk through the extract branch, but skip the actual download by
    # letting curl succeed via the mocked run.
    sentinel.unlink()

    module.download_and_extract(data_dir)

    # The zstd invocation must go through Popen with an argv list (never shell=True).
    assert popen_calls, "expected zstd Popen call"
    assert popen_calls[-1][0] == "zstd", popen_calls
    assert str(archive) in popen_calls[-1]

    # tar must be an argv run() call with stdin wired to zstd's stdout, no shell.
    tar_run = next((c for c in run_calls if c["args"] and c["args"][0] == "tar"), None)
    assert tar_run is not None, run_calls
    assert "shell" not in tar_run["kwargs"] or tar_run["kwargs"]["shell"] is False
    assert str(data_dir) in tar_run["args"]
    assert tar_run["kwargs"].get("stdin") is not None


def test_extract_fallback_raises_on_zstd_failure(tmp_path, monkeypatch):
    module = _import_module()

    data_dir = tmp_path / "pm"
    data_dir.mkdir()
    archive = data_dir / "data.tar.zst"
    archive.write_bytes(b"payload")

    monkeypatch.setitem(sys.modules, "zstandard", None)

    def fake_popen(args, stdout=None, **_kwargs):
        return _FakeZstdProc(returncode=2)

    def fake_run(args, **kwargs):
        # curl succeeds; tar succeeds; only zstd wait() returns non-zero.
        return _FakeCompleted(returncode=0, args=list(args))

    monkeypatch.setattr(module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(module.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        module.download_and_extract(data_dir)


def test_extract_fallback_raises_on_tar_failure(tmp_path, monkeypatch):
    module = _import_module()

    data_dir = tmp_path / "pm"
    data_dir.mkdir()
    archive = data_dir / "data.tar.zst"
    archive.write_bytes(b"payload")

    monkeypatch.setitem(sys.modules, "zstandard", None)

    def fake_popen(args, stdout=None, **_kwargs):
        return _FakeZstdProc(returncode=0)

    def fake_run(args, **kwargs):
        if args and args[0] == "tar":
            return _FakeCompleted(returncode=1, args=list(args))
        return _FakeCompleted(returncode=0, args=list(args))

    monkeypatch.setattr(module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(module.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        module.download_and_extract(data_dir)
