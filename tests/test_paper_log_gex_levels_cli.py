"""Tests for the `python -m paper_log.gex_levels` CLI dispatcher.

`run_preopen`/`run_postclose`/`compute_status`/`run_evaluate` are all
monkeypatched where `__main__.py` imported them, so this file never
touches a database, the network, or the filesystem beyond `tmp_path`.
"""

from __future__ import annotations

import os

# __main__.py imports db.py, which imports GRID's root config.settings —
# see test_paper_log_gex_levels_db.py for why this guard is needed.
os.environ.setdefault("DB_PASSWORD", "test-password")

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import paper_log.gex_levels.__main__ as cli_mod
from paper_log.gex_levels.evaluate import EvaluateReport
from paper_log.gex_levels.status import StatusReport


def test_build_parser_requires_log_dir() -> None:
    parser = cli_mod.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["status"])  # missing --log-dir


def test_build_parser_rejects_unknown_command() -> None:
    parser = cli_mod.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["launch-nukes", "--log-dir", "x"])


def test_interim_flag_rejected_outside_evaluate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_mod, "compute_status", lambda log_dir: StatusReport(0, 0, 0))
    with pytest.raises(SystemExit):
        cli_mod.main(["status", "--log-dir", str(tmp_path), "--interim"])


def test_status_command_dispatches_and_prints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    called = {}

    def fake_compute_status(log_dir):
        called["log_dir"] = log_dir
        return StatusReport(sessions_preopen=3, sessions_postclose=2, valid_sessions=1)

    monkeypatch.setattr(cli_mod, "compute_status", fake_compute_status)

    code = cli_mod.main(["status", "--log-dir", str(tmp_path)])

    assert code == 0
    assert called["log_dir"] == tmp_path
    out = capsys.readouterr().out
    assert "activity and data quality only" in out


def test_evaluate_command_returns_0_when_not_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli_mod, "run_evaluate",
        lambda log_dir, interim: EvaluateReport(interim=interim, valid_sessions=60, refused=False, refusal_reason=None,
                                                 h1=None, h2=None, h3=None),
    )
    # format_evaluate would crash on None h1/h2/h3 — patch it too, since
    # this test only cares about dispatch + exit code, not formatting.
    monkeypatch.setattr(cli_mod, "format_evaluate", lambda report: "ok")
    code = cli_mod.main(["evaluate", "--log-dir", str(tmp_path)])
    assert code == 0


def test_evaluate_command_returns_1_when_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli_mod, "run_evaluate",
        lambda log_dir, interim: EvaluateReport(interim=False, valid_sessions=5, refused=True,
                                                 refusal_reason="only 5 valid sessions", h1=None, h2=None, h3=None),
    )
    code = cli_mod.main(["evaluate", "--log-dir", str(tmp_path)])
    assert code == 1


def test_evaluate_command_passes_interim_flag_through(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_run_evaluate(log_dir, interim):
        captured["interim"] = interim
        return EvaluateReport(interim=interim, valid_sessions=5, refused=False, refusal_reason=None, h1=None, h2=None, h3=None)

    monkeypatch.setattr(cli_mod, "run_evaluate", fake_run_evaluate)
    monkeypatch.setattr(cli_mod, "format_evaluate", lambda report: "ok")
    cli_mod.main(["evaluate", "--log-dir", str(tmp_path), "--interim"])
    assert captured["interim"] is True


def test_preopen_command_builds_readonly_engine_and_disposes_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_engine = MagicMock()
    monkeypatch.setattr(cli_mod, "build_readonly_engine", lambda: fake_engine)
    monkeypatch.setattr(cli_mod, "resolve_code_sha", lambda repo_root, override=None: "codesha123")

    captured = {}

    def fake_run_preopen(*, log_dir, db_engine, code_sha, ticker):
        captured.update(log_dir=log_dir, db_engine=db_engine, code_sha=code_sha, ticker=ticker)
        return {"session_date": "2026-09-24", "excluded": False, "exclusion_reason": None}

    monkeypatch.setattr(cli_mod, "run_preopen", fake_run_preopen)

    code = cli_mod.main(["preopen", "--log-dir", str(tmp_path)])

    assert code == 0
    assert captured["db_engine"] is fake_engine
    assert captured["code_sha"] == "codesha123"
    fake_engine.dispose.assert_called_once()


def test_preopen_disposes_engine_even_if_run_preopen_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_engine = MagicMock()
    monkeypatch.setattr(cli_mod, "build_readonly_engine", lambda: fake_engine)
    monkeypatch.setattr(cli_mod, "resolve_code_sha", lambda repo_root, override=None: "codesha123")

    def raising_run_preopen(**kwargs):
        raise RuntimeError("yfinance is down")

    monkeypatch.setattr(cli_mod, "run_preopen", raising_run_preopen)

    with pytest.raises(RuntimeError):
        cli_mod.main(["preopen", "--log-dir", str(tmp_path)])

    fake_engine.dispose.assert_called_once()


def test_postclose_command_does_not_touch_the_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    build_engine_spy = MagicMock(side_effect=AssertionError("postclose must not build a DB engine"))
    monkeypatch.setattr(cli_mod, "build_readonly_engine", build_engine_spy)
    monkeypatch.setattr(cli_mod, "resolve_code_sha", lambda repo_root, override=None: "codesha123")
    monkeypatch.setattr(
        cli_mod, "run_postclose",
        lambda *, log_dir, code_sha, ticker: {"session_date": "2026-09-24", "excluded": False, "exclusion_reason": None},
    )

    code = cli_mod.main(["postclose", "--log-dir", str(tmp_path)])

    assert code == 0
    build_engine_spy.assert_not_called()


def test_code_sha_override_is_threaded_through(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}
    monkeypatch.setattr(cli_mod, "resolve_code_sha", lambda repo_root, override=None: captured.setdefault("override", override) or "resolved")
    monkeypatch.setattr(
        cli_mod, "run_postclose",
        lambda *, log_dir, code_sha, ticker: {"session_date": "x", "excluded": False, "exclusion_reason": None},
    )
    cli_mod.main(["postclose", "--log-dir", str(tmp_path), "--code-sha", "manual-override-sha"])
    assert captured["override"] == "manual-override-sha"
