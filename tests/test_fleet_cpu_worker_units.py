"""Static checks for the CPU compute-worker install kit (scripts/fleet/cpu_worker, slice S4 2026-11-06)."""

from __future__ import annotations

import re
import shlex
import subprocess
from pathlib import Path

import pytest

KIT = Path(__file__).resolve().parents[1] / "scripts" / "fleet" / "cpu_worker"
LLM_TYPES = {"HUMAN_LLM_QUERY", "LLM_INFERENCE", "EMBEDDING_BATCH", "KILL_PREDICTOR_SCORE"}


def _exec_start_argv(unit_text: str) -> list[str]:
    """Return the argv of the last non-empty ExecStart= (continuation lines joined)."""
    joined = re.sub(r"\\\n\s*", " ", unit_text)
    starts = [line.split("=", 1)[1].strip() for line in joined.splitlines() if line.startswith("ExecStart=")]
    starts = [s for s in starts if s]
    assert starts, "no ExecStart"
    return shlex.split(starts[-1])


def _flag(argv: list[str], name: str) -> str:
    return argv[argv.index(name) + 1]


@pytest.mark.parametrize("unit", ["grid-worker@.service", "gridz4-zz-release-tree.conf"])
def test_worker_units_exclude_llm_job_types(unit: str) -> None:
    argv = _exec_start_argv((KIT / unit).read_text())
    assert argv[1] == "scripts/worker.py"
    excluded = set(_flag(argv, "--exclude-types").split(","))
    assert LLM_TYPES <= excluded
    assert "DATA_PULL" in excluded  # workers get DB credentials only, no puller API keys


def test_template_runs_one_job_per_process_with_distinct_hostname() -> None:
    text = (KIT / "grid-worker@.service").read_text()
    argv = _exec_start_argv(text)
    # One interpreter tops out near one core (GIL); throughput comes from instances, not threads.
    assert _flag(argv, "--max-concurrent") == "1"
    # Coordinator registration upserts by hostname, so every instance needs its own name.
    assert "%i" in _flag(argv, "--hostname")
    assert "User=gridworker" in text
    assert "EnvironmentFile=/etc/grid/grid-worker.env" in text
    assert "PrivateDevices=yes" in text  # never registers a GPU it does not own
    assert "Slice=grid-worker.slice" in text


def test_template_never_inlines_credentials() -> None:
    for path in KIT.iterdir():
        text = path.read_text()
        assert not re.search(r"(DB_PASSWORD|PG_PASSWORD)=\S", text.replace("DB_PASSWORD=smoke-not-a-secret", "")), path


def test_install_script_parses() -> None:
    subprocess.run(["bash", "-n", str(KIT / "install_release.sh")], check=True)


def test_install_script_verifies_inputs_and_refuses_shared_staging() -> None:
    text = (KIT / "install_release.sh").read_text()
    for required in ("GRID_TARBALL_SHA256:?", "GRID_CONSTRAINTS_SHA256:?", "sha256sum -c", "get-tar-commit-id"):
        assert required in text, required
    assert "assert_private_path" in text
    code = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
    assert not [ln for ln in code if "/tmp" in ln]  # nothing staged or run from /tmp
    assert any('mktemp -d -p "$STAGING_ROOT"' in ln for ln in code)


def test_install_script_aborts_on_missing_hashes() -> None:
    env = {"PATH": "/usr/bin:/bin", "GRID_SHA": "0" * 40, "GRID_TARBALL": "x", "GRID_CONSTRAINTS": "y", "GRID_USER": "u"}
    r = subprocess.run(["bash", str(KIT / "install_release.sh")], env=env, capture_output=True, text=True, check=False)
    assert r.returncode != 0
    assert "GRID_TARBALL_SHA256" in r.stderr
