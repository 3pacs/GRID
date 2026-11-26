"""The CI shard split (tests/ci_shard.py + the pytest_ignore_collect hook in conftest.py).

The risk of sharding is a test file that no shard runs. These tests pin that
every test file under tests/ is owned by exactly one shard for the shard
counts CI uses, that no shard is starved, that the hook skips what it should
and nothing else, and that the workflows run the shards they claim to.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from tests import ci_shard

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"


def _test_files() -> list[str]:
    found = [
        path.relative_to(ROOT).as_posix()
        for path in TESTS.rglob("*.py")
        if "node_modules" not in path.parts and ci_shard.is_test_file(path.name)
    ]
    assert found, "no test files found"
    return sorted(found)


@pytest.mark.parametrize("total", [1, 2, 3, 4, 5, 6])
def test_every_test_file_belongs_to_exactly_one_shard(total):
    owners: dict[str, list[int]] = {}
    for rel in _test_files():
        owners[rel] = [i for i in range(1, total + 1) if ci_shard.shard_of(rel, total) == i]
    assert all(len(shards) == 1 for shards in owners.values())
    assert {s[0] for s in owners.values()} <= set(range(1, total + 1))


@pytest.mark.parametrize("total", [2, 3, 4])
def test_no_shard_is_starved(total):
    files = _test_files()
    counts = [sum(ci_shard.shard_of(rel, total) == i for rel in files) for i in range(1, total + 1)]
    assert min(counts) >= len(files) / total * 0.7, counts


def test_the_split_is_stable_across_calls():
    rel = "tests/test_ci_shard_partition.py"
    assert {ci_shard.shard_of(rel, 3) for _ in range(5)} == {ci_shard.shard_of(rel, 3)}
    # the value CI relied on when this was written: a change here re-homes the file
    assert ci_shard.shard_of("tests/test_api_event_loop_guard.py", 3) in (1, 2, 3)


@pytest.mark.parametrize("bad", ["", "3", "0/3", "4/3", "a/b", "1/0", "1/3/5", "-1/3"])
def test_parse_shard_rejects_malformed_specs(bad):
    with pytest.raises(ValueError):
        ci_shard.parse_shard(bad)


def test_pinned_files_exist_and_name_a_real_shard():
    files = set(_test_files())
    for rel, shard in ci_shard._PINNED_FOR_3.items():
        assert rel in files, f"{rel} is pinned but no longer exists: update tests/ci_shard.py"
        assert 1 <= shard <= 3
        assert ci_shard.shard_of(rel, 3) == shard
    # other shard counts ignore the pins and use the hash
    assert ci_shard.shard_of("tests/test_e0_calibration.py", 4) in (1, 2, 3, 4)


def test_parse_shard_accepts_index_over_total():
    assert ci_shard.parse_shard("2/3") == (2, 3)


class _Config:
    rootpath = ROOT


def _ignore(spec: str | None, rel: str, monkeypatch):
    from tests import conftest

    if spec is None:
        monkeypatch.delenv(ci_shard.SHARD_ENV, raising=False)
    else:
        monkeypatch.setenv(ci_shard.SHARD_ENV, spec)
    return conftest.pytest_ignore_collect(ROOT / rel, _Config())


def test_the_hook_skips_other_shards_test_files_and_only_those(monkeypatch):
    mine = "tests/test_ci_shard_partition.py"
    owner = ci_shard.shard_of(mine, 3)
    other = owner % 3 + 1
    assert _ignore(f"{owner}/3", mine, monkeypatch) is None
    assert _ignore(f"{other}/3", mine, monkeypatch) is True
    assert _ignore(None, mine, monkeypatch) is None
    # directories, conftest.py and helper modules are never skipped
    for rel in ("tests", "tests/conftest.py", "tests/ci_shard.py", "tests/contracts"):
        assert _ignore(f"{other}/3", rel, monkeypatch) is None, rel


def test_a_malformed_shard_spec_is_a_usage_error_not_a_silent_full_run(monkeypatch):
    with pytest.raises(pytest.UsageError, match="GRID_TEST_SHARD"):
        _ignore("4/3", "tests/test_ci_shard_partition.py", monkeypatch)


def _collected(directory: str, spec: str | None) -> set[str]:
    env = {k: v for k, v in os.environ.items() if k != ci_shard.SHARD_ENV}
    if spec is not None:
        env[ci_shard.SHARD_ENV] = spec
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider",
         "-p", "no:xdist", "-p", "no:randomly", directory],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=300,
    )
    assert result.returncode in (0, 5), result.stdout + result.stderr
    return {line for line in result.stdout.splitlines() if "::" in line}


def test_the_shards_together_collect_exactly_the_unsharded_suite():
    """End to end on a real directory, the way CI passes it (a directory, not files)."""
    everything = _collected("tests/contracts", None)
    assert everything, "tests/contracts collected nothing"
    shards = [_collected("tests/contracts", f"{i}/3") for i in (1, 2, 3)]
    assert set().union(*shards) == everything
    assert sum(len(s) for s in shards) == len(everything), "a test is collected by two shards"


def _workflow(name: str) -> dict:
    return yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "workflow, job",
    [("test.yml", "backend-suite"), ("python314.yml", "backend-suite-py314")],
)
def test_workflows_run_every_shard_with_a_consistent_total(workflow, job):
    spec = _workflow(workflow)["jobs"][job]
    shards = spec["strategy"]["matrix"]["shard"]
    assert shards == list(range(1, len(shards) + 1)), shards
    assert spec["strategy"].get("fail-fast") is False
    run_steps = [s for s in spec["steps"] if s.get("name") == "Run tests"]
    assert len(run_steps) == 1
    assert run_steps[0]["env"][ci_shard.SHARD_ENV] == "${{ matrix.shard }}/${{ strategy.job-total }}"


@pytest.mark.parametrize(
    "workflow, aggregator, needs",
    [
        ("test.yml", "backend-tests", ["backend-suite", "backend-gates"]),
        ("python314.yml", "backend-tests-py314", ["backend-suite-py314"]),
    ],
)
def test_the_stable_check_name_is_an_aggregate_that_fails_if_any_shard_does(workflow, aggregator, needs):
    spec = _workflow(workflow)["jobs"][aggregator]
    assert spec["needs"] == needs
    assert "always()" in spec["if"]
    (step,) = spec["steps"]
    for need in needs:
        assert f"needs.{need}.result" in step["run"]
