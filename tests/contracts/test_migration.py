"""Smoke test: the contracts-infrastructure migration creates both tables."""
from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text


MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "migrations"
    / "20260411_contracts_infrastructure.sql"
)


@pytest.mark.integration
def test_migration_creates_contracts_audit(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(MIGRATION_PATH.read_text()))
        row = conn.execute(
            text(
                "SELECT to_regclass(:name)::text AS name"
            ).bindparams(name="contracts_audit")
        ).fetchone()
        assert row is not None and row[0] == "contracts_audit"


@pytest.mark.integration
def test_migration_creates_contracts_dead_letter(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(MIGRATION_PATH.read_text()))
        row = conn.execute(
            text(
                "SELECT to_regclass(:name)::text AS name"
            ).bindparams(name="contracts_dead_letter")
        ).fetchone()
        assert row is not None and row[0] == "contracts_dead_letter"
