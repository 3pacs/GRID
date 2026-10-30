"""The E1 PostgreSQL fixture never touches a production database.

Scratch schemas are created and dropped only in a database named by
``GRID_TEST_DB_URL``, and only when that database is not a production one.
These tests open no connection.
"""

from __future__ import annotations

import sys

import pytest

from evals.e1.pg_safety import ProductionDatabaseRefused, check_scratch_target


@pytest.mark.parametrize("name", ["grid", "griddb", "GRIDDB", "grid_obsidian", "grid_v4", "gridprod",
                                  "postgres", "griddb_prod", "grid_live"])
def test_production_database_names_are_refused(name):
    with pytest.raises(ProductionDatabaseRefused):
        check_scratch_target(f"postgresql://grid_user:x@localhost:5432/{name}")


@pytest.mark.parametrize("name", ["griddb_test", "grid_test", "e1_scratch_test", "scratch"])
def test_disposable_test_databases_are_allowed(name):
    assert check_scratch_target(f"postgresql://grid:testpass@localhost:5432/{name}") == name.lower()


def test_url_without_a_database_is_refused():
    with pytest.raises(ProductionDatabaseRefused):
        check_scratch_target("postgresql://grid:testpass@localhost:5432")


def _conftest_module():
    found = [m for m in list(sys.modules.values())
             if str(getattr(m, "__file__", "") or "").replace("\\", "/").endswith("evals/e1/conftest.py")]
    assert found, "evals/e1/conftest.py is not loaded"
    return found[0]


def test_fixture_refuses_a_production_url_before_connecting(request, monkeypatch):
    monkeypatch.setenv("GRID_TEST_DB_URL", "postgresql://grid_user:x@localhost:5432/griddb")
    monkeypatch.setattr(_conftest_module(), "create_engine",
                        lambda *a, **k: pytest.fail("opened a connection to production"))
    with pytest.raises(pytest.fail.Exception, match="production database"):
        request.getfixturevalue("pg_scratch")


def test_fixture_has_no_default_url(request, monkeypatch):
    monkeypatch.delenv("GRID_TEST_DB_URL", raising=False)
    monkeypatch.delenv("E1_REQUIRE_PG", raising=False)
    with pytest.raises(pytest.skip.Exception, match="GRID_TEST_DB_URL unset"):
        request.getfixturevalue("pg_scratch")


def test_fixture_unset_url_fails_when_postgres_is_required(request, monkeypatch):
    monkeypatch.delenv("GRID_TEST_DB_URL", raising=False)
    monkeypatch.setenv("E1_REQUIRE_PG", "1")
    with pytest.raises(pytest.fail.Exception, match="GRID_TEST_DB_URL is unset"):
        request.getfixturevalue("pg_scratch")
