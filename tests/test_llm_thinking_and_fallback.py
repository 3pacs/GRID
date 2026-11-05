"""Router fallback-warning dedupe, chat kwarg filtering, task-queue thinking.

2026-11-04 grid-hermes: "LLM provider llamacpp_quick unavailable, falling
back to llamacpp" 76x/day; 203 queue tasks/day ended "LLM returned None"
after thinking through max_tokens on the single REASON slot.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from llm import router


@pytest.fixture(autouse=True)
def _clean_router(monkeypatch):
    monkeypatch.setattr(router, "_client_cache", {})
    monkeypatch.setattr(router, "_FALLBACK_WARNED", set())
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_UNTIL", {})
    yield


class _Client:
    def __init__(self, available: bool) -> None:
        self.is_available = available

    def chat(self, messages, temperature=0.3, num_predict=None, enable_thinking=None):
        return "ok"


def test_fallback_is_warned_once_per_outage(monkeypatch):
    from config import settings

    monkeypatch.setattr(settings, "LLM_LOCAL_PROVIDER", "llamacpp_quick", raising=False)
    primary = {"available": False}
    fallback = _Client(True)

    def create(provider):
        if provider == "llamacpp_quick":
            return _Client(primary["available"])
        if provider == "ollama_koala":
            return fallback
        return None

    monkeypatch.setattr(router, "_create_client", create)
    warnings: list[str] = []
    monkeypatch.setattr(router.log, "warning",
                        lambda template, **k: warnings.append(template.format(**k)))
    for _ in range(5):
        assert router.get_llm(router.Tier.LOCAL) is fallback
    assert len(warnings) == 1 and "falling back to ollama_koala" in warnings[0]

    primary["available"] = True
    assert router.get_llm(router.Tier.LOCAL).is_available
    router._client_cache.pop("llamacpp_quick")
    primary["available"] = False
    router.get_llm(router.Tier.LOCAL)
    assert len(warnings) == 2, "a new outage after a recovery warns again"


def test_supported_chat_kwargs_filters_by_signature():
    assert router.supported_chat_kwargs(_Client(True), enable_thinking=False) == {
        "enable_thinking": False}

    class Plain:
        def chat(self, messages, temperature=0.3):
            return "x"

    assert router.supported_chat_kwargs(Plain(), enable_thinking=False) == {}
    assert router.supported_chat_kwargs(MagicMock(), enable_thinking=False) == {}
    assert router.supported_chat_kwargs(object(), enable_thinking=False) == {}


@pytest.mark.parametrize("task_type,expected", [
    ("earnings_preview", False),
    ("strategy151_signal", False),
    ("deep_forensic", False),
    ("trade_review", True),
])
def test_task_queue_disables_thinking_for_structured_tasks(monkeypatch, task_type, expected):
    from orchestration import llm_taskqueue as tq

    seen: dict[str, Any] = {}

    class Client:
        is_available = True
        timeout = 60

        def chat(self, messages, temperature=0.3, num_predict=None, enable_thinking=None):
            seen["enable_thinking"] = enable_thinking
            seen["num_predict"] = num_predict
            return "RESULT"

    queue = tq.LLMTaskQueue(engine=MagicMock())
    monkeypatch.setattr(queue, "_get_client", lambda task_type=None: Client())
    monkeypatch.setattr(queue, "_record_done", lambda task: None)
    queue.enqueue(task_type, "prompt", priority=3)
    task = queue.process_next()
    assert task.result == "RESULT"
    assert seen == {"enable_thinking": expected, "num_predict": 2000}


def test_task_queue_works_with_clients_without_the_keyword(monkeypatch):
    from orchestration import llm_taskqueue as tq

    class Legacy:
        is_available = True
        timeout = 60

        def chat(self, messages, temperature=0.3, num_predict=None):
            return "LEGACY"

    queue = tq.LLMTaskQueue(engine=MagicMock())
    monkeypatch.setattr(queue, "_get_client", lambda task_type=None: Legacy())
    monkeypatch.setattr(queue, "_record_done", lambda task: None)
    queue.enqueue("earnings_preview", "prompt", priority=3)
    assert queue.process_next().result == "LEGACY"
