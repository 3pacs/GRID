from __future__ import annotations

import builtins
import subprocess
import sys


def test_detect_ram_gb_uses_macos_sysctl_when_proc_is_missing(monkeypatch):
    from scripts import worker

    def missing_proc(*_args, **_kwargs):
        raise FileNotFoundError("/proc/meminfo")

    def fake_run(args, **_kwargs):
        assert args == ["sysctl", "-n", "hw.memsize"]
        return subprocess.CompletedProcess(args, 0, stdout="17179869184\n", stderr="")

    monkeypatch.setattr(builtins, "open", missing_proc)
    monkeypatch.setitem(sys.modules, "psutil", None)
    monkeypatch.setattr(worker.subprocess, "run", fake_run)

    assert worker.detect_ram_gb() == 17.2
