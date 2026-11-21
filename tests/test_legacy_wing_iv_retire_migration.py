"""legacy_wing_iv_retire_20261120 retires only the legacy 25d wing features."""
import importlib

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text

revision = importlib.import_module("migrations.versions.legacy_wing_iv_retire_20261120")


def _registry(conn) -> None:
    conn.exec_driver_sql(
        "CREATE TABLE feature_registry (name TEXT UNIQUE, model_eligible BOOLEAN, deprecated_at TIMESTAMP)"
    )
    rows = [
        ("spy_iv_25d_put", True, None),
        ("spy_iv_25d_call", True, None),
        ("brk-b_iv_25d_call", True, None),
        ("spy_iv_m90_put_v1", False, None),
        ("spy_iv_atm", True, None),
        ("spy_iv_skew", True, None),
        ("spyxiv_25d_put", True, None),  # underscore is literal, not a wildcard
        ("qqq_iv_25d_put", False, "2026-01-01 00:00:00"),
    ]
    for name, eligible, deprecated in rows:
        conn.execute(
            text("INSERT INTO feature_registry VALUES (:n, :e, :d)"),
            {"n": name, "e": eligible, "d": deprecated},
        )


def test_retires_only_legacy_wing_features_and_keeps_prior_deprecations(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        _registry(conn)
        monkeypatch.setattr(revision, "op", Operations(MigrationContext.configure(conn)))
        revision.upgrade()
        revision.upgrade()  # idempotent
        rows = {
            name: (bool(eligible), deprecated)
            for name, eligible, deprecated in conn.exec_driver_sql(
                "SELECT name, model_eligible, deprecated_at FROM feature_registry"
            )
        }
    for name in ("spy_iv_25d_put", "spy_iv_25d_call", "brk-b_iv_25d_call"):
        assert rows[name][0] is False and rows[name][1] is not None, name
    for name in ("spy_iv_atm", "spy_iv_skew", "spyxiv_25d_put"):
        assert rows[name] == (True, None), name
    assert rows["spy_iv_m90_put_v1"] == (False, None)
    assert rows["qqq_iv_25d_put"] == (False, "2026-01-01 00:00:00")


def test_downgrade_refuses() -> None:
    with pytest.raises(RuntimeError, match="refused"):
        revision.downgrade()


def test_chains_after_the_wing_gdelt_merge() -> None:
    assert revision.down_revision == "merge_wing_gdelt_20261120"
    assert len(revision.revision) <= 32
