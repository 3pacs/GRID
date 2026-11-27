"""GRID Tests: the PostgreSQL contracts run beside the suite shards, and "Backend Tests" gates both.

Until 2026-11-26 the ~60 PostgreSQL contract steps ran inside the required
"Backend Tests" job, after the suite shards had finished, which added about
four minutes to every run. They read nothing the suite produces, so they now
run in their own job, ``backend-tests-contracts``, at the same time as
``backend-tests-suite``. "Backend Tests" keeps its name (evals/e3/gates.py
MANDATORY_CI and the #1352 release check read it). It needs both jobs and
fails unless both passed. It checks the shards' manifests and runs the #1352
witness. These tests pin that wiring, and two rules from reviews:

* the tested checkout is the aggregate's first step (the #1352 release kit's
  workflow guard binds the job to it);
* the shard manifests are downloaded into ``runner.temp``, which the runner
  empties around every job, so on a self-hosted runner a stale manifest
  cannot fill a missing shard (Codex review 6070573984).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "test.yml"
SHARED_SETUP = {"Set up Python", "Install dependencies"}


@pytest.fixture(scope="module")
def jobs() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def _names(job: dict[str, Any]) -> list[str]:
    return [step.get("name") or step.get("uses") or "" for step in job["steps"]]


def test_contracts_run_beside_the_suite_not_after_it(jobs: dict[str, Any]) -> None:
    contracts = jobs["backend-tests-contracts"]
    assert contracts["name"] == "Backend Tests contracts"
    assert "needs" not in contracts and "if" not in contracts
    # routed exactly like the required aggregate (hosted unless TEST_RUNNER is set)
    assert contracts["runs-on"] == jobs["backend-tests"]["runs-on"]
    names = _names(contracts)
    assert names[0].startswith("actions/checkout@")
    assert "Run tests" not in names and "Option B release witness (#1352)" not in names
    contract_steps = [n for n in names if "PostgreSQL contract" in n]
    assert len(contract_steps) >= 50, len(contract_steps)
    # its own database, with the same hosted/self-hosted switch as before
    start = next(s for s in contracts["steps"] if s.get("name") == "Start ephemeral Postgres (hosted runner)")
    reset = next(s for s in contracts["steps"] if s.get("name") == "Reset persistent Postgres (self-hosted)")
    assert start["if"] == "env.ON_SELF_HOSTED != 'true'" and reset["if"] == "env.ON_SELF_HOSTED == 'true'"
    assert names.index("Start ephemeral Postgres (hosted runner)") < names.index(contract_steps[0])


def test_backend_tests_is_the_gate_for_all(jobs: dict[str, Any]) -> None:
    job = jobs["backend-tests"]
    assert job["name"] == "Backend Tests"
    assert "strategy" not in job
    assert job["needs"] == ["backend-tests-suite", "backend-tests-contracts", "backend-tests-pit-postgres"]
    assert job["if"] == "${{ !cancelled() }}"
    steps = job["steps"]
    # the tested checkout first, plain
    assert steps[0]["uses"].startswith("actions/checkout@") and "with" not in steps[0] and "if" not in steps[0]
    gate = steps[1]
    assert gate["env"] == {
        "SUITE_RESULT": "${{ needs.backend-tests-suite.result }}",
        "CONTRACTS_RESULT": "${{ needs.backend-tests-contracts.result }}",
        "PIT_POSTGRES_RESULT": "${{ needs.backend-tests-pit-postgres.result }}",
    }
    for var in ("SUITE_RESULT", "CONTRACTS_RESULT", "PIT_POSTGRES_RESULT"):
        assert f'[ "${var}" != "success" ]' in gate["run"]
    assert gate["run"].count("exit 1") == 3
    # no database here, and nothing that belongs to the other jobs
    names = _names(job)
    assert not [n for n in names if "Postgres" in n or "PostgreSQL contract" in n]
    assert "Run tests" not in names
    assert names[-1] == "Option B release witness (#1352)"


def test_manifests_land_in_a_fresh_per_job_directory(jobs: dict[str, Any]) -> None:
    steps = jobs["backend-tests"]["steps"]
    download = next(s for s in steps if s.get("name") == "Download suite shard manifests")
    verify = next(s for s in steps if s.get("name") == "Suite shards ran every collected test exactly once")
    path = download["with"]["path"]
    assert path.startswith("${{ runner.temp }}/")
    assert verify["run"].endswith(f"{path}/backend-suite-manifest-*")
    assert "/tmp/" not in verify["run"]


def test_no_step_is_in_both_jobs(jobs: dict[str, Any]) -> None:
    shared = (set(_names(jobs["backend-tests"])) & set(_names(jobs["backend-tests-contracts"]))) - SHARED_SETUP
    assert {n for n in shared if not n.startswith("actions/checkout@")} == set()


def test_pit_postgres_uses_a_fresh_hosted_service_without_enabling_the_shards(jobs: dict[str, Any]) -> None:
    pit = jobs["backend-tests-pit-postgres"]
    assert pit["runs-on"] == "ubuntu-latest"
    assert "needs" not in pit and "if" not in pit
    pg = pit["services"]["postgres"]
    assert pg["image"] == "postgres:15"
    assert pg["env"]["POSTGRES_DB"] == "grid_pit_test"
    assert pg["ports"] == ["5432/tcp"]
    suite = jobs["backend-tests-suite"]
    assert all("GRID_TEST_DB_URL" not in s.get("env", {}) for s in suite["steps"])
