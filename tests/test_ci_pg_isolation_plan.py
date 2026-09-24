"""No-database safety checks for per-module CI isolation plans."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.ci_pg_isolation_plan import build_plan


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "scripts" / "ci_pg_isolation_profiles.json"


def test_full_source_manifest_allocates_one_local_test_database_per_module() -> None:
    plan = build_plan(ROOT, MANIFEST, "127.0.0.1", 5432, "grid_ci_local_20260923")
    modules = plan["modules"]
    assert plan["module_count"] == 45
    assert len({item["database_name"] for item in modules}) == 45
    assert plan["preserve_existing_dedicated_ci_steps"] is True
    for item in modules:
        assert item["database_name"].startswith("grid_ci_")
        assert item["bootstrap_validated_on_disposable_database"] is False
        env = item["environment_template"]
        expected_url = env["GRID_TEST_DB_URL"]
        assert all(env[name] == expected_url for name in (
            "GRID_BRIEFING_ROUTE_TEST_DB_URL",
            "GRID_WATCHLIST_EDGE_TEST_DB_URL",
            "DB_URL",
        ))
        assert env["DB_NAME"] == env["PGDATABASE"] == item["database_name"]
        assert env["DB_HOST"] == env["PGHOST"] == "127.0.0.1"
        assert env["DB_PASSWORD"] == "${CI_DB_PASSWORD}"
        assert "testpass" not in json.dumps(item)


@pytest.mark.parametrize("host", ["grid-svr", "10.0.0.4", "example.com"])
def test_nonlocal_hosts_are_refused(host: str) -> None:
    with pytest.raises(ValueError, match="host must be literal"):
        build_plan(ROOT, MANIFEST, host, 5432, "grid_ci_local_20260923")


@pytest.mark.parametrize("prefix", ["grid", "griddb_test", "production", "grid_ci_../../prod"])
def test_nontest_or_pathlike_database_identities_are_refused(prefix: str) -> None:
    with pytest.raises(ValueError, match="grid_ci_ test identity"):
        build_plan(ROOT, MANIFEST, "127.0.0.1", 5432, prefix)


def test_new_pg_module_requires_profile_review(tmp_path: Path) -> None:
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_known_pg.py").write_text("def test_known(): pass\n", encoding="utf-8")
    (tests / "test_new_pg.py").write_text("def test_new(): pass\n", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "version": 1,
        "status": "source_only",
        "profiles": {"module_owned_schema": {
            "bootstrap": "module_owned",
            "destructive_scope": "unique_schema",
            "modules": ["tests/test_known_pg.py"],
        }},
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="new PostgreSQL modules need profile review"):
        build_plan(tmp_path, manifest, "localhost", 5432, "grid_ci_safe")


def test_discovered_fixture_module_requires_profile_review(tmp_path: Path) -> None:
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_known_pg.py").write_text("def test_known(): pass\n", encoding="utf-8")
    (tests / "test_new_fixture.py").write_text(
        "def test_new(pg_engine): pass\n", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "version": 1, "status": "source_only",
        "profiles": {"module_owned_schema": {
            "bootstrap": "module_owned", "destructive_scope": "unique_schema",
            "modules": ["tests/test_known_pg.py"],
        }},
    }), encoding="utf-8")
    discovery = tmp_path / "discovery.json"
    discovery.write_text(json.dumps({
        "version": 1,
        "pg_tests": [{"nodeid": "tests/test_new_fixture.py::test_new",
                      "signals": ["pg_engine_fixture"]}],
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="fixture modules need profile review"):
        build_plan(tmp_path, manifest, "localhost", 5432, "grid_ci_safe", discovery)
