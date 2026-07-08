"""Tests for the Hermes Codex backend (intelligence/hermes/codex_provider.py).

The Codex CLI is never actually invoked — ``subprocess.run`` and
``shutil.which`` are mocked — so these run anywhere without a logged-in CLI.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import intelligence.hermes.codex_provider as cp
from intelligence.hermes import (
    CodexProvider,
    HermesAgent,
    HermesConfig,
    HermesProvider,
    TokenUsage,
    build_messages,
)
from intelligence.hermes import config as hconfig
from intelligence.hermes.agent import _make_provider
from intelligence.hermes.provider import HermesResponse

pytestmark = pytest.mark.unit


def _codex_config(**over) -> HermesConfig:
    base = dict(
        enabled=True, api_key="", base_url="", model="gpt-4o", timeout_seconds=30,
        max_completion_tokens=256, temperature=None, reasoning_effort=None,
        daily_spend_cap_usd=0.0, ledger_path="", price_input_per_mtok=2.5,
        price_output_per_mtok=10.0, fallback_tier="local",
        backend="codex", codex_bin="codex", codex_model="", codex_timeout_seconds=30,
        codex_extra_args="",
    )
    base.update(over)
    return HermesConfig(**base)


def _fake_run(captured: dict, *, returncode=0, write="pong", stdout="", stderr=""):
    """Build a subprocess.run stand-in that records argv/input and writes the
    --output-last-message file."""
    def _run(argv, input=None, **kwargs):  # noqa: A002 - mirror subprocess kw
        captured["argv"] = argv
        captured["input"] = input
        if write is not None and "--output-last-message" in argv:
            Path(argv[argv.index("--output-last-message") + 1]).write_text(write)
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)
    return _run


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
def test_config_maps_codex_fields(monkeypatch):
    ns = SimpleNamespace(
        HERMES_BACKEND="Codex", HERMES_CODEX_BIN="/usr/local/bin/codex",
        HERMES_CODEX_MODEL="gpt-5.5", HERMES_CODEX_TIMEOUT_SECONDS=99,
        HERMES_CODEX_EXTRA_ARGS="--reasoning high",
    )
    monkeypatch.setattr(hconfig, "_settings", lambda: ns)
    cfg = hconfig.load_hermes_config()
    assert cfg.backend == "codex"  # normalised to lower-case
    assert cfg.codex_bin == "/usr/local/bin/codex"
    assert cfg.codex_model == "gpt-5.5"
    assert cfg.codex_timeout_seconds == 99
    assert cfg.codex_extra_args == "--reasoning high"


# --------------------------------------------------------------------------- #
# provider — argv + availability
# --------------------------------------------------------------------------- #
def test_build_argv_shape():
    provider = CodexProvider(_codex_config(codex_model="gpt-5.5", codex_extra_args="--reasoning high"))
    argv = provider._build_argv("/tmp/out.txt", None)
    assert argv[0] == "codex" and argv[1] == "exec"
    assert "--sandbox" in argv and argv[argv.index("--sandbox") + 1] == "read-only"
    assert "--skip-git-repo-check" in argv
    assert argv[argv.index("--output-last-message") + 1] == "/tmp/out.txt"
    assert argv[argv.index("--model") + 1] == "gpt-5.5"
    assert "--reasoning" in argv and "high" in argv  # from extra_args
    assert argv[-1] == "-"  # prompt comes from stdin


def test_is_available(monkeypatch):
    monkeypatch.setattr(cp.shutil, "which", lambda b: "/usr/bin/codex")
    assert CodexProvider(_codex_config()).is_available is True
    monkeypatch.setattr(cp.shutil, "which", lambda b: None)
    assert CodexProvider(_codex_config()).is_available is False
    # wrong backend -> not available even if the binary exists
    monkeypatch.setattr(cp.shutil, "which", lambda b: "/usr/bin/codex")
    assert CodexProvider(_codex_config(backend="openai")).is_available is False


# --------------------------------------------------------------------------- #
# provider — complete()
# --------------------------------------------------------------------------- #
def test_complete_success_feeds_prompt_on_stdin(monkeypatch):
    captured: dict = {}
    monkeypatch.setattr(cp.shutil, "which", lambda b: "/usr/bin/codex")
    monkeypatch.setattr(cp.subprocess, "run", _fake_run(captured, write="pong"))

    provider = CodexProvider(_codex_config(codex_model="gpt-5.5"))
    resp = provider.complete(build_messages("ping the model"))

    assert resp is not None
    assert resp.text == "pong"
    assert resp.provider == "codex"
    assert resp.model == "codex:gpt-5.5"
    assert resp.cost_usd == 0.0  # subscription billing — no per-token cost
    assert "ping the model" in captured["input"]  # prompt via stdin, not argv
    assert "ping the model" not in " ".join(captured["argv"])


def test_complete_returns_none_when_binary_missing(monkeypatch):
    captured: dict = {}
    monkeypatch.setattr(cp.shutil, "which", lambda b: None)
    monkeypatch.setattr(cp.subprocess, "run", _fake_run(captured))
    assert CodexProvider(_codex_config()).complete(build_messages("x")) is None
    assert "argv" not in captured  # never spawned a process


def test_complete_returns_none_on_nonzero_exit(monkeypatch):
    monkeypatch.setattr(cp.shutil, "which", lambda b: "/usr/bin/codex")
    monkeypatch.setattr(cp.subprocess, "run",
                        _fake_run({}, returncode=1, write=None, stderr="not logged in"))
    assert CodexProvider(_codex_config()).complete(build_messages("x")) is None


def test_complete_returns_none_on_timeout(monkeypatch):
    monkeypatch.setattr(cp.shutil, "which", lambda b: "/usr/bin/codex")

    def _boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="codex", timeout=30)

    monkeypatch.setattr(cp.subprocess, "run", _boom)
    assert CodexProvider(_codex_config()).complete(build_messages("x")) is None


def test_complete_returns_none_on_empty_output(monkeypatch):
    monkeypatch.setattr(cp.shutil, "which", lambda b: "/usr/bin/codex")
    monkeypatch.setattr(cp.subprocess, "run", _fake_run({}, write=None, stdout=""))
    assert CodexProvider(_codex_config()).complete(build_messages("x")) is None


# --------------------------------------------------------------------------- #
# agent wiring
# --------------------------------------------------------------------------- #
def test_make_provider_selects_backend():
    assert isinstance(_make_provider(_codex_config()), CodexProvider)
    assert isinstance(_make_provider(_codex_config(backend="openai")), HermesProvider)


def test_agent_codex_source_label():
    resp = HermesResponse(text="ANSWER", model="codex:gpt-5.5", usage=TokenUsage(),
                          cost_usd=0.0, latency_ms=5.0, provider="codex")

    class _Stub:
        def complete(self, *a, **k):
            return resp

    result = HermesAgent(_codex_config(), provider=_Stub()).analyze("q")
    assert result.source == "codex"
    assert result.text == "ANSWER"


def test_agent_codex_falls_back_to_local(monkeypatch):
    class _Stub:
        def complete(self, *a, **k):
            return None

    agent = HermesAgent(_codex_config(), provider=_Stub())
    fake_local = SimpleNamespace(is_available=True, model="qwen3-14b",
                                 chat=lambda messages, temperature=0.3: "LOCAL")
    monkeypatch.setattr("llm.router.get_llm", lambda tier: fake_local)
    result = agent.analyze("q")
    assert result.source == "local"
    assert result.text == "LOCAL"
