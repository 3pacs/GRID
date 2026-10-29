"""Only project the digest branch; never invoke/import the pipeline or command.

All mail behavior is a synthetic double. No application modules are loaded.
"""
from __future__ import annotations

import ast
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _pipeline_function(source=None):
    path = ROOT / "scripts/run_full_pipeline.py"
    tree = ast.parse(path.read_text(encoding="utf8") if source is None else source)
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_pipeline")


def _digest_projection(source=None):
    pipeline = _pipeline_function(source)
    blocks = [n for n in pipeline.body if isinstance(n, ast.If)
              and isinstance(n.test, ast.Compare)
              and isinstance(n.test.left, ast.Name) and n.test.left.id == "send_digest"]
    if blocks:
        assert len(blocks) == 1
        body = blocks
    else:
        # Baseline negative control: the original unconditional digest step.
        body = [n for n in pipeline.body if
                isinstance(n, ast.FunctionDef) and n.name == "_digest"
                or isinstance(n, ast.Assign) and any(
                    isinstance(t, ast.Subscript) and isinstance(t.slice, ast.Constant)
                    and t.slice.value == "digest" for t in n.targets)]
    assert body
    # Deliberately compile no ingestion, resolution, scoring, or pipeline body.
    fn = ast.parse("def projected_digest(send_digest=True):\n    summary = {'steps': {}}\n").body[0]
    fn.body += body + ast.parse("return summary").body
    namespace = {}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])),
                 "<isolated-pipeline-digest-branch>", "exec"), namespace)
    return namespace


@pytest.mark.parametrize("enabled", [False, None, 0, "false", "true"])
def test_disabled_or_nonboolean_digest_never_calls_mail_wrapper(enabled, monkeypatch):
    namespace = _digest_projection()
    safe_run = MagicMock(side_effect=AssertionError("mail wrapper must not be reached"))
    namespace["_safe_run"] = safe_run
    result = namespace["projected_digest"](enabled)
    assert result["steps"]["digest"] == {"skipped": "disabled_by_caller"}
    safe_run.assert_not_called()


def test_standalone_default_still_includes_digest_with_only_synthetic_mail(monkeypatch):
    namespace = _digest_projection()
    fake_mail = ModuleType("alerts.email")
    fake_mail.daily_digest = MagicMock(return_value=None)
    monkeypatch.setitem(sys.modules, "alerts.email", fake_mail)
    namespace["_safe_run"] = MagicMock(side_effect=lambda label, fn: fn())
    result = namespace["projected_digest"]()
    fake_mail.daily_digest.assert_called_once_with()
    namespace["_safe_run"].assert_called_once()
    assert result["steps"]["digest"] is None


def test_digest_flag_is_additive_keyword_only_with_true_default():
    args = _pipeline_function().args
    assert [a.arg for a in args.args] == ["historical"]
    index = [a.arg for a in args.kwonlyargs].index("send_digest")
    assert ast.literal_eval(args.kw_defaults[index]) is True


def test_both_hermes_pipeline_calls_explicitly_disable_research_digest():
    tree = ast.parse((ROOT / "scripts/hermes_fixers.py").read_text(encoding="utf8"))
    calls = [(fn.name, call) for fn in tree.body if isinstance(fn, ast.FunctionDef)
             for call in ast.walk(fn) if isinstance(call, ast.Call)
             and isinstance(call.func, ast.Name) and call.func.id == "run_pipeline"]
    assert {name for name, _ in calls} == {"_execute_hermes_repair_command", "maybe_run_pipeline"}
    assert len(calls) == 2
    for _, call in calls:
        kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in call.keywords}
        assert kwargs == {"historical": False, "send_digest": False}
