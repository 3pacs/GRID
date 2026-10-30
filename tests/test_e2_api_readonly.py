"""The E2 scoreboard GETs never write: not the ledger, not the board directory, not a lock."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from api.routers import evals_e2
from evals.e2 import board
from tests import e2_support as S


def _tree(root: Path) -> dict:
    return {p.relative_to(root).as_posix(): (p.stat().st_mtime_ns, p.read_bytes())
            for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture
def built_board(tmp_path, monkeypatch):
    log = tmp_path / "stream.jsonl"
    S.write_chain(log, S.stream_records())
    rules = S.rules()
    rules["version"] = "e2-v1"
    board.run(tmp_path / "board", [S.stream_adapter(log, rules, S.prices())], S.utc(2026, 10, 2, 21, 30),
              rules=rules, cost_model=S.cost_model(), manifest_info=S.MANIFEST_INFO, code_sha=S.CODE_SHA)
    monkeypatch.setenv("GRID_E2_BOARD_DIR", str(tmp_path / "board"))
    return tmp_path / "board"


def test_scoreboard_get_serves_the_verified_snapshot_and_writes_nothing(built_board):
    before = _tree(built_board)
    out = evals_e2.get_scoreboard(_token="t", window="all", bucket="official")
    md = evals_e2.get_scoreboard_markdown(_token="t")
    assert out["status"] == "ok" and out["chain"]["ok"] is True
    assert out["snapshot"]["counts"]["scores"] == 6
    assert {r["window"] for r in out["snapshot"]["aggregates"]} == {"all"}
    assert "E2 forward scoreboard" in md
    assert _tree(built_board) == before


def test_missing_board_is_reported_and_not_created(tmp_path, monkeypatch):
    target = tmp_path / "nowhere" / "e2"
    monkeypatch.setenv("GRID_E2_BOARD_DIR", str(target))
    out = evals_e2.get_scoreboard(_token="t", window="all", bucket="official")
    assert out["status"] == "not_initialized" and out["snapshot"] is None
    assert not target.exists() and not target.parent.exists()


def test_broken_ledger_serves_no_scores(built_board):
    path = built_board / "e2_scoreboard_e2-v1.jsonl"
    path.write_bytes(path.read_bytes().replace(b'"side":1', b'"side":-1', 1))
    before = _tree(built_board)
    out = evals_e2.get_scoreboard(_token="t", window="all", bucket="any")
    assert out["status"] == "chain_broken" and out["snapshot"] is None
    assert _tree(built_board) == before


def test_router_is_get_only_and_authenticated():
    from api.auth import require_auth

    for route in evals_e2.router.routes:
        assert route.methods == {"GET"}, route.path
        assert any(dep.call is require_auth for dep in route.dependant.dependencies), route.path


def test_report_cli_is_read_only(built_board, capsys):
    from evals.e2.__main__ import main

    before = _tree(built_board)
    assert main(["report", "--board-dir", str(built_board), "--format", "json"]) == 0
    assert '"status": "ok"' in capsys.readouterr().out
    assert _tree(built_board) == before
    assert not any(name.endswith(".lock") for name in os.listdir(built_board))
