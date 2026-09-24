"""Tests for the `python -m paper_log.gex_levels` CLI dispatcher.

Command-specific modules are imported *lazily* inside `main()` (see
`__main__.py`'s module docstring for why), so tests patch each command's
run function at its OWN module (`paper_log.gex_levels.preopen`, `...db`,
`...postclose`, `...status`, `...evaluate`) rather than on `__main__`
itself — patching `__main__`'s namespace wouldn't affect a name that
`main()` re-imports fresh from its source module on every call.
"""

from __future__ import annotations

import os
import subprocess
import sys

# Importing paper_log.gex_levels.db / .preopen (to have something to
# monkeypatch) pulls in GRID's root config.settings, whose startup
# validator rejects an empty DB_PASSWORD — see
# test_paper_log_gex_levels_db.py for the same guard. setdefault leaves a
# real value (CI, .env) untouched. Only needed for the tests that patch
# those two modules; test_status_and_evaluate_need_no_db_password below
# deliberately runs in a *separate* process without this variable, to
# prove status/evaluate really don't need it (a bug caught by the
# 2026-09-24 production smoke test would not have been caught by any test
# that shares this process's now-set DB_PASSWORD).
os.environ.setdefault("DB_PASSWORD", "test-password")

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import paper_log.gex_levels.__main__ as cli_mod
import paper_log.gex_levels.db as db_mod
import paper_log.gex_levels.evaluate as evaluate_mod
import paper_log.gex_levels.postclose as postclose_mod
import paper_log.gex_levels.preopen as preopen_mod
import paper_log.gex_levels.status as status_mod
from paper_log.gex_levels.evaluate import EvaluateReport
from paper_log.gex_levels.status import StatusReport

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_build_parser_requires_log_dir() -> None:
    parser = cli_mod.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["status"])  # missing --log-dir


def test_build_parser_rejects_unknown_command() -> None:
    parser = cli_mod.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["launch-nukes", "--log-dir", "x"])


def test_interim_flag_rejected_outside_evaluate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(status_mod, "compute_status", lambda log_dir: StatusReport(0, 0, 0))
    with pytest.raises(SystemExit):
        cli_mod.main(["status", "--log-dir", str(tmp_path), "--interim"])


def test_status_command_dispatches_and_prints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    called = {}

    def fake_compute_status(log_dir):
        called["log_dir"] = log_dir
        return StatusReport(sessions_preopen=3, sessions_postclose=2, valid_sessions=1)

    monkeypatch.setattr(status_mod, "compute_status", fake_compute_status)

    code = cli_mod.main(["status", "--log-dir", str(tmp_path)])

    assert code == 0
    assert called["log_dir"] == tmp_path
    out = capsys.readouterr().out
    assert "activity and data quality only" in out


def test_evaluate_command_returns_0_when_not_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        evaluate_mod, "run_evaluate",
        lambda log_dir, interim: EvaluateReport(interim=interim, valid_sessions=60, refused=False, refusal_reason=None,
                                                 h1=None, h2=None, h3=None),
    )
    # format_evaluate would crash on None h1/h2/h3 — patch it too, since
    # this test only cares about dispatch + exit code, not formatting.
    monkeypatch.setattr(evaluate_mod, "format_evaluate", lambda report: "ok")
    code = cli_mod.main(["evaluate", "--log-dir", str(tmp_path)])
    assert code == 0


def test_evaluate_command_returns_1_when_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        evaluate_mod, "run_evaluate",
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

    monkeypatch.setattr(evaluate_mod, "run_evaluate", fake_run_evaluate)
    monkeypatch.setattr(evaluate_mod, "format_evaluate", lambda report: "ok")
    cli_mod.main(["evaluate", "--log-dir", str(tmp_path), "--interim"])
    assert captured["interim"] is True


def test_preopen_command_builds_readonly_engine_and_disposes_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_engine = MagicMock()
    monkeypatch.setattr(db_mod, "build_readonly_engine", lambda: fake_engine)
    monkeypatch.setattr(cli_mod, "resolve_code_sha", lambda repo_root, override=None: "codesha123")

    captured = {}

    def fake_run_preopen(*, log_dir, db_engine, code_sha, ticker):
        captured.update(log_dir=log_dir, db_engine=db_engine, code_sha=code_sha, ticker=ticker)
        return {"session_date": "2026-09-24", "excluded": False, "exclusion_reason": None}

    monkeypatch.setattr(preopen_mod, "run_preopen", fake_run_preopen)

    code = cli_mod.main(["preopen", "--log-dir", str(tmp_path)])

    assert code == 0
    assert captured["db_engine"] is fake_engine
    assert captured["code_sha"] == "codesha123"
    fake_engine.dispose.assert_called_once()


def test_preopen_disposes_engine_even_if_run_preopen_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_engine = MagicMock()
    monkeypatch.setattr(db_mod, "build_readonly_engine", lambda: fake_engine)
    monkeypatch.setattr(cli_mod, "resolve_code_sha", lambda repo_root, override=None: "codesha123")

    def raising_run_preopen(**kwargs):
        raise RuntimeError("yfinance is down")

    monkeypatch.setattr(preopen_mod, "run_preopen", raising_run_preopen)

    with pytest.raises(RuntimeError):
        cli_mod.main(["preopen", "--log-dir", str(tmp_path)])

    fake_engine.dispose.assert_called_once()


def test_postclose_command_does_not_import_db_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    build_engine_spy = MagicMock(side_effect=AssertionError("postclose must not build a DB engine"))
    monkeypatch.setattr(db_mod, "build_readonly_engine", build_engine_spy)
    monkeypatch.setattr(cli_mod, "resolve_code_sha", lambda repo_root, override=None: "codesha123")
    monkeypatch.setattr(
        postclose_mod, "run_postclose",
        lambda *, log_dir, code_sha, ticker: {"session_date": "2026-09-24", "excluded": False, "exclusion_reason": None},
    )

    code = cli_mod.main(["postclose", "--log-dir", str(tmp_path)])

    assert code == 0
    build_engine_spy.assert_not_called()


def test_code_sha_override_is_threaded_through(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}
    monkeypatch.setattr(cli_mod, "resolve_code_sha", lambda repo_root, override=None: captured.setdefault("override", override) or "resolved")
    monkeypatch.setattr(
        postclose_mod, "run_postclose",
        lambda *, log_dir, code_sha, ticker: {"session_date": "x", "excluded": False, "exclusion_reason": None},
    )
    cli_mod.main(["postclose", "--log-dir", str(tmp_path), "--code-sha", "manual-override-sha"])
    assert captured["override"] == "manual-override-sha"


# ── Regression: status/evaluate must not require DB_PASSWORD ───────────
#
# Caught by the 2026-09-24 production smoke test: a fresh `ssh grid-svr`
# invocation of `status` (no .env sourced — that command was never
# supposed to need it) crashed with a pydantic DB_PASSWORD validation
# error, because __main__.py used to import preopen.py/db.py (which pull
# in GRID's root config.settings) at module level, unconditionally. Every
# test above shares this file's one process, which sets DB_PASSWORD at
# import time for the tests that DO need to patch preopen/db — so it
# could never have caught this itself. This test spawns a genuinely
# separate interpreter with DB_PASSWORD removed, the same way the smoke
# test's ssh session was genuinely separate from the earlier `preopen`
# invocation that had sourced .env.


def test_status_and_evaluate_need_no_db_password(tmp_path: Path) -> None:
    env = {k: v for k, v in os.environ.items() if k != "DB_PASSWORD"}

    status_result = subprocess.run(
        [sys.executable, "-m", "paper_log.gex_levels", "status", "--log-dir", str(tmp_path)],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=60,
    )
    assert status_result.returncode == 0, f"stderr:\n{status_result.stderr}"
    assert "DB_PASSWORD" not in status_result.stderr
    assert "activity and data quality only" in status_result.stdout

    evaluate_result = subprocess.run(
        [sys.executable, "-m", "paper_log.gex_levels", "evaluate", "--log-dir", str(tmp_path)],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=60,
    )
    assert evaluate_result.returncode == 1, f"stderr:\n{evaluate_result.stderr}"  # refused: 0 valid sessions
    assert "DB_PASSWORD" not in evaluate_result.stderr
    assert "evaluate refused" in evaluate_result.stdout
