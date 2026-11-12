"""Background LLM task-queue work is spread over several nodes.

On 2026-11-12 grid-hermes' background queue (1,988 priority-3 tasks in 24 h)
ran entirely on the RTX 3090's single slot, because Hermes pins the REASON
tier there, while gridz4 and x99-hex served the same model nearly idle. These
tests pin the routing that fixes it:

* ``llm.router.get_provider_client`` returns exactly the named provider or
  ``None`` (never a fallback), and
* ``LLMTaskQueue`` rotates REASON-tier background tasks over
  ``LLM_TASKQUEUE_BACKGROUND_PROVIDERS`` and leaves everything else alone.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from llm import router


class _Client:
    """Minimal stand-in for an LLM client."""

    def __init__(self, name: str, available: bool = True) -> None:
        self.name = name
        self.is_available = available
        self.timeout = 120
        self.calls: list[str] = []

    def chat(self, messages: list[dict[str, str]], temperature: float = 0.3,
             num_predict: int | None = None) -> str:
        self.calls.append(messages[-1]["content"])
        return f"answer from {self.name}"


@pytest.fixture(autouse=True)
def _clean_router(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(router, "_client_cache", {})
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_UNTIL", {})
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_REASONS", {})
    monkeypatch.setattr(router, "_UNREACHABLE_WARNED", set())


# ---------------------------------------------------------------------------
# llm.router.get_provider_client
# ---------------------------------------------------------------------------

def test_get_provider_client_returns_the_named_provider_and_caches_it(monkeypatch):
    built: list[str] = []
    node = _Client("x99")

    def create(provider: str) -> Any:
        built.append(provider)
        return node if provider == "llamacpp_x99" else _Client("other")

    monkeypatch.setattr(router, "_create_client", create)

    assert router.get_provider_client("llamacpp_x99") is node
    assert router.get_provider_client("llamacpp_x99") is node
    assert built == ["llamacpp_x99"]  # second call came from the cache


def test_get_provider_client_never_falls_back(monkeypatch):
    """A disabled provider is ``None`` even when other providers are healthy."""
    asked: list[str] = []

    def create(provider: str) -> Any:
        asked.append(provider)
        return None if provider == "llamacpp_x99" else _Client(provider)

    monkeypatch.setattr(router, "_create_client", create)

    assert router.get_provider_client("llamacpp_x99") is None
    assert asked == ["llamacpp_x99"]  # no other provider was even built


def test_get_provider_client_cools_down_a_node_that_fails_its_probe(monkeypatch):
    from config import settings

    monkeypatch.setattr(settings, "LLM_PROVIDER_COOLDOWN_SECONDS", 120.0, raising=False)
    built: list[str] = []

    def create(provider: str) -> Any:
        built.append(provider)
        return _Client(provider, available=False)

    monkeypatch.setattr(router, "_create_client", create)

    assert router.get_provider_client("llamacpp") is None
    assert router.get_provider_client("llamacpp") is None
    assert built == ["llamacpp"]  # the second call was skipped by the cooldown


def test_get_provider_client_rebuilds_a_cached_client_that_went_unavailable(monkeypatch):
    stale = _Client("gridz4", available=False)
    fresh = _Client("gridz4")
    monkeypatch.setitem(router._client_cache, "llamacpp", stale)
    monkeypatch.setattr(router, "_create_client", lambda provider: fresh)

    assert router.get_provider_client("llamacpp") is fresh


# ---------------------------------------------------------------------------
# LLMTaskQueue background rotation
# ---------------------------------------------------------------------------

@pytest.fixture
def nodes(monkeypatch: pytest.MonkeyPatch) -> dict[str, _Client]:
    """Three fake nodes behind the router: gridz4, x99-hex and the 3090."""
    from config import settings

    clients = {
        "llamacpp": _Client("gridz4"),
        "llamacpp_x99": _Client("x99"),
        "llamacpp_oracle": _Client("3090"),
    }
    monkeypatch.setattr(
        settings, "LLM_TASKQUEUE_BACKGROUND_PROVIDERS", "llamacpp,llamacpp_x99", raising=False,
    )
    monkeypatch.setattr(router, "get_provider_client", lambda provider: (
        clients[provider] if clients.get(provider) and clients[provider].is_available else None
    ))
    # Every tier resolves to the 3090, as it does for grid-hermes.
    monkeypatch.setattr(router, "get_llm", lambda tier=None, provider=None: clients["llamacpp_oracle"])
    return clients


@pytest.fixture
def queue(monkeypatch: pytest.MonkeyPatch):
    from orchestration import llm_taskqueue as tq

    q = tq.LLMTaskQueue(engine=MagicMock())
    monkeypatch.setattr(q, "_record_done", lambda task: None)
    return q


def _run(queue, task_type: str, priority: int) -> str:
    queue.enqueue(task_type, "prompt", priority=priority)
    task = queue.process_next()
    assert task is not None and task.error is None
    return task.result


def test_reason_tier_background_tasks_alternate_between_nodes(queue, nodes):
    answers = [_run(queue, "deep_forensic", 3) for _ in range(4)]

    assert answers == [
        "answer from gridz4", "answer from x99", "answer from gridz4", "answer from x99",
    ]
    assert nodes["llamacpp_oracle"].calls == []  # the 3090 saw none of it
    assert queue.get_status()["background_by_provider"] == {"llamacpp": 2, "llamacpp_x99": 2}


@pytest.mark.parametrize("task_type,priority", [
    ("trade_review", 1),            # realtime, ORACLE tier
    ("thesis_narrative", 2),        # scheduled, REASON tier
    ("panama_papers_research", 3),  # background, but ORACLE tier
    ("feature_interpretation", 3),  # background, but LOCAL tier
])
def test_other_tasks_keep_their_tier_provider(queue, nodes, task_type, priority):
    assert _run(queue, task_type, priority) == "answer from 3090"
    assert nodes["llamacpp"].calls == [] and nodes["llamacpp_x99"].calls == []
    assert queue.get_status()["background_by_provider"] == {}


def test_a_node_that_is_down_is_skipped(queue, nodes):
    nodes["llamacpp"].is_available = False

    answers = [_run(queue, "company_analysis", 3) for _ in range(3)]

    assert answers == ["answer from x99"] * 3
    assert nodes["llamacpp_oracle"].calls == []


def test_falls_back_to_the_task_tier_when_no_listed_node_answers(queue, nodes):
    nodes["llamacpp"].is_available = False
    nodes["llamacpp_x99"].is_available = False

    assert _run(queue, "deep_forensic", 3) == "answer from 3090"


@pytest.mark.parametrize("value", ["", "  ", ","])
def test_blank_setting_restores_the_old_routing(queue, nodes, monkeypatch, value):
    from config import settings

    monkeypatch.setattr(settings, "LLM_TASKQUEUE_BACKGROUND_PROVIDERS", value, raising=False)

    assert _run(queue, "deep_forensic", 3) == "answer from 3090"
    assert nodes["llamacpp"].calls == [] and nodes["llamacpp_x99"].calls == []


def test_one_provider_raising_does_not_stop_the_rotation(queue, nodes, monkeypatch):
    def flaky(provider: str) -> Any:
        if provider == "llamacpp":
            raise RuntimeError("boom")
        return nodes[provider]

    monkeypatch.setattr(router, "get_provider_client", flaky)

    assert _run(queue, "deep_forensic", 3) == "answer from x99"


def test_duplicate_names_in_the_setting_do_not_weight_a_node(queue, nodes, monkeypatch):
    from config import settings

    monkeypatch.setattr(
        settings, "LLM_TASKQUEUE_BACKGROUND_PROVIDERS",
        "llamacpp, llamacpp ,llamacpp_x99", raising=False,
    )

    answers = [_run(queue, "deep_forensic", 3) for _ in range(4)]

    assert answers.count("answer from gridz4") == 2
    assert answers.count("answer from x99") == 2
