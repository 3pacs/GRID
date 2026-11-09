"""Tier.RESEARCH: the research desk runs on x99-hex, not the oracle's 3090.

Plan item 0.2 (2026-11-09). The live .env leads ORACLE and REASON with the
disabled ``llamacpp_z4``, so both fell through to grid-svr's RTX 3090 (one
8k slot, 94% busy) while x99-hex's identical Qwen3.8-27B (2 x 32k slots)
sat at 0%. Sleuth, the research agent, postmortems and the backtest scanner
now ask for RESEARCH: x99-hex first, then the gridz4 proxy, the 3090 last
among local nodes, paid providers after every local one.
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

import config
from llm import router


@pytest.fixture(autouse=True)
def _clean_router(monkeypatch):
    monkeypatch.setattr(router, "_client_cache", {})
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_UNTIL", {})
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_REASONS", {})
    monkeypatch.setattr(router, "_UNREACHABLE_WARNED", set())
    monkeypatch.setattr(router, "_FALLBACK_WARNED", set())
    yield


class _Fake:
    def __init__(self, available: bool) -> None:
        self.is_available = available


def _factory(monkeypatch, providers: dict[str, Any]) -> list[str]:
    built: list[str] = []

    def create(name: str) -> Any:
        built.append(name)
        return providers.get(name)

    monkeypatch.setattr(router, "_create_client", create)
    return built


def test_research_defaults_to_x99():
    assert config.Settings.model_fields["LLM_RESEARCH_PROVIDER"].default == "llamacpp_x99"
    assert router.Tier("research") is router.Tier.RESEARCH


def test_research_chain_puts_the_3090_last_among_local_nodes():
    chain = router._fallback_chain(router.Tier.RESEARCH, "none")
    assert chain[:4] == ["llamacpp_x99", "llamacpp", "llamacpp_z4", "llamacpp_oracle"]
    for paid in ("gemini", "openrouter", "openai"):
        assert chain.index("llamacpp_oracle") < chain.index(paid)


def test_research_uses_x99_when_it_answers(monkeypatch):
    monkeypatch.setattr(config.settings, "LLM_RESEARCH_PROVIDER", "llamacpp_x99")
    x99, rtx = _Fake(True), _Fake(True)
    _factory(monkeypatch, {"llamacpp_x99": x99, "llamacpp": _Fake(True), "llamacpp_oracle": rtx})
    assert router.get_llm(router.Tier.RESEARCH) is x99


def test_research_falls_to_gridz4_before_the_3090(monkeypatch):
    monkeypatch.setattr(config.settings, "LLM_RESEARCH_PROVIDER", "llamacpp_x99")
    gz4, rtx = _Fake(True), _Fake(True)
    _factory(monkeypatch, {"llamacpp_x99": _Fake(False), "llamacpp": gz4,
                           "llamacpp_z4": _Fake(False), "llamacpp_oracle": rtx})
    assert router.get_llm(router.Tier.RESEARCH) is gz4


def test_research_reaches_the_3090_only_when_x99_and_gridz4_are_dark(monkeypatch):
    monkeypatch.setattr(config.settings, "LLM_RESEARCH_PROVIDER", "llamacpp_x99")
    rtx = _Fake(True)
    _factory(monkeypatch, {"llamacpp_x99": _Fake(False), "llamacpp": _Fake(False),
                           "llamacpp_z4": None, "llamacpp_oracle": rtx})
    assert router.get_llm(router.Tier.RESEARCH) is rtx


@pytest.mark.parametrize(("module", "attr"), [
    ("intelligence.sleuth", "_get_llm"),
    ("analysis.research_agent", "_get_llm"),
])
def test_research_desk_callers_ask_for_research(module, attr):
    src = inspect.getsource(getattr(__import__(module, fromlist=[attr]), attr))
    assert "Tier.RESEARCH" in src and "Tier.ORACLE" not in src


def test_postmortem_and_backtest_scanner_ask_for_research():
    import analysis.backtest_scanner as bs
    import intelligence.postmortem as pm
    pm_src, bs_src = inspect.getsource(pm), inspect.getsource(bs)
    assert pm_src.count("get_llm(Tier.RESEARCH)") == 2 and "get_llm(Tier.REASON)" not in pm_src
    assert "get_llm(Tier.RESEARCH)" in bs_src and "get_llm(Tier.ORACLE)" not in bs_src
    # x99's client default is 512 tokens; postmortems keep an explicit cap.
    assert pm_src.count("num_predict=POSTMORTEM_NUM_PREDICT") == 2
