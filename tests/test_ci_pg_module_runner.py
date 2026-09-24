"""No-database guards for the isolated PostgreSQL module pilot runner."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts import ci_pg_module_runner as runner

TOKEN = "a" * 32
PREFIX = "grid_ci_pilot"
DATABASE = f"{PREFIX}_{hashlib.sha256(runner.PILOT_MODULE.encode()).hexdigest()[:12]}"
URL = f"postgresql://pilot:placeholder@127.0.0.1:25499/{DATABASE}"


def _plan(tmp_path: Path) -> tuple[dict, Path, Path]:
    root = tmp_path / "repo"
    module = root / runner.PILOT_MODULE
    module.parent.mkdir(parents=True)
    module.write_text("def test_pilot(): pass\n", encoding="utf-8")
    plan = {
        "version": 1, "status": "plan_only_no_database_created_or_test_executed",
        "host": "127.0.0.1", "port": 25499, "database_prefix": PREFIX,
        "module_count": 1,
        "modules": [{
            "module": runner.PILOT_MODULE,
            "source_sha256": runner._source_sha(module),
            "database_name": DATABASE,
            "profile": {
                "profile": "module_owned_schema",
                "bootstrap": "module_creates_unique_schema_and_tables",
                "destructive_scope": "module_unique_schema",
            },
        }],
    }
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    allowlist = tmp_path / "allowlist.json"
    allowlist.write_text('{"version":1,"allowed_skips":[]}', encoding="utf-8")
    return plan, plan_path, allowlist


@pytest.mark.parametrize("url", [
    f"postgresql://pilot:placeholder@grid-svr:25499/{DATABASE}",
    "postgresql://pilot:placeholder@127.0.0.1:25499/grid",
    f"postgresql://pilot:placeholder@127.0.0.1:5432/{DATABASE}",
    f"postgresql://pilot:placeholder@127.0.0.1:25499/{DATABASE}?host=grid-svr",
])
def test_nonlocal_or_wrong_database_url_is_rejected_before_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, url: str,
) -> None:
    plan, _, _ = _plan(tmp_path)
    connect = MagicMock(side_effect=AssertionError("must not connect"))
    monkeypatch.setattr(runner, "create_engine", connect)
    with pytest.raises(ValueError, match="database URL"):
        runner._database_identity(url, plan, plan["modules"][0], TOKEN)
    connect.assert_not_called()


@pytest.mark.parametrize("comment,owner", [
    (f"grid_ci_disposable_v1:wrong:{DATABASE}", "pilot"),
    (f"grid_ci_disposable_v1:{TOKEN}:{DATABASE}", "other_role"),
])
def test_owner_marker_and_database_owner_must_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, comment: str, owner: str,
) -> None:
    plan, _, _ = _plan(tmp_path)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.one.return_value = (
        DATABASE, "pilot", 1234, owner, comment, False, True, False, False,
    )
    monkeypatch.setattr(runner, "create_engine", lambda *args, **kwargs: engine)
    with pytest.raises(ValueError, match="ownership marker"):
        runner._database_identity(URL, plan, plan["modules"][0], TOKEN)
    engine.dispose.assert_called_once()


@pytest.mark.parametrize("superuser,createdb", [(True, False), (False, True)])
def test_privileged_database_owner_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, superuser: bool, createdb: bool,
) -> None:
    plan, _, _ = _plan(tmp_path)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.one.return_value = (
        DATABASE, "pilot", 1234, "pilot",
        f"grid_ci_disposable_v1:{TOKEN}:{DATABASE}", False, True, superuser, createdb,
    )
    monkeypatch.setattr(runner, "create_engine", lambda *args, **kwargs: engine)
    with pytest.raises(ValueError, match="ownership marker"):
        runner._database_identity(URL, plan, plan["modules"][0], TOKEN)
    engine.dispose.assert_called_once()


def test_source_and_profile_are_bound_to_selected_pilot(tmp_path: Path) -> None:
    plan, _, _ = _plan(tmp_path)
    root = tmp_path / "repo"
    entry = runner._entry(plan, runner.PILOT_MODULE, root)
    assert entry["database_name"] == DATABASE
    (root / runner.PILOT_MODULE).write_text("def test_changed(): pass\n", encoding="utf-8")
    with pytest.raises(ValueError, match="source differs"):
        runner._entry(plan, runner.PILOT_MODULE, root)
    plan["modules"][0]["profile"]["profile"] = "shared_public_tables"
    with pytest.raises(ValueError, match="module-owned schema"):
        runner._entry(plan, runner.PILOT_MODULE, root)


def test_plan_source_digest_matches_crlf_checkout(tmp_path: Path) -> None:
    plan, _, _ = _plan(tmp_path)
    root = tmp_path / "repo"
    module = root / runner.PILOT_MODULE
    module.write_bytes(b"def test_pilot(): pass\r\n")
    assert runner._entry(plan, runner.PILOT_MODULE, root)["module"] == runner.PILOT_MODULE


def test_missing_allowlist_refuses_before_database_contact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, plan_path, allowlist = _plan(tmp_path)
    allowlist.unlink()
    connect = MagicMock(side_effect=AssertionError("must not connect"))
    monkeypatch.setattr(runner, "_database_identity", connect)
    with pytest.raises(FileNotFoundError):
        runner.run_module(tmp_path / "repo", plan_path, tmp_path / "out",
                          runner.PILOT_MODULE, URL, TOKEN, allowlist)
    connect.assert_not_called()


def test_actual_pytest_process_exit_is_preserved_even_with_other_successes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, plan_path, allowlist = _plan(tmp_path)
    monkeypatch.setattr(runner, "_database_identity", lambda *args: {
        "database": DATABASE, "database_oid": 1234, "database_owner": "pilot",
    })
    exits = iter([7, 0])
    monkeypatch.setattr(runner.subprocess, "run", lambda *args, **kwargs:
                        SimpleNamespace(returncode=next(exits)))
    output = tmp_path / "output"
    status = runner.run_module(tmp_path / "repo", plan_path, output,
                               runner.PILOT_MODULE, URL, TOKEN, allowlist)
    result = json.loads((output / "run.json").read_text(encoding="utf-8"))
    assert status == 7
    assert result["pytest_process_exit"] == 7
    assert result["discovery_process_exit"] == 0
    assert result["audit_process_exit"] is None
    assert result["ok"] is False
    assert result["module_sha256"] == runner._source_sha(tmp_path / "repo" / runner.PILOT_MODULE)
    assert result["plan_sha256"] == runner._sha(plan_path)


def test_success_artifacts_are_hashed_and_all_db_routes_are_isolated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, plan_path, allowlist = _plan(tmp_path)
    monkeypatch.setattr(runner, "_database_identity", lambda *args: {
        "database": DATABASE, "database_oid": 1234, "database_owner": "pilot",
    })
    output = tmp_path / "output"
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        env = kwargs["env"]
        assert all(env[name] == URL for name in runner.URL_VARIABLES)
        assert env["DB_NAME"] == DATABASE
        assert "PGHOSTADDR" not in env
        assert "DATABASE_URL" not in env
        artifact = ("junit.xml", "exact.json", "discovery.json", "audit.json")
        for name in artifact:
            (output / name).write_text(f"{name}\n", encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setenv("PGHOSTADDR", "grid-svr")
    monkeypatch.setenv("DATABASE_URL", "postgresql://production")
    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    status = runner.run_module(tmp_path / "repo", plan_path, output,
                               runner.PILOT_MODULE, URL, TOKEN, allowlist)
    result = json.loads((output / "run.json").read_text(encoding="utf-8"))
    assert status == 0
    assert result["ok"] is True
    assert result["pytest_process_exit"] == result["audit_process_exit"] == 0
    assert len(calls) == 3
    assert result["artifacts"] == {
        name: runner._sha(output / filename)
        for name, filename in (("junit", "junit.xml"), ("report", "exact.json"),
                               ("discovery", "discovery.json"), ("audit", "audit.json"))
    }

