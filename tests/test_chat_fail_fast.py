"""Fail-fast + budget tests for the stepdad.finance composer/verdict LLM paths.

2026-09-10 read-only QA (gemini-task 34544181414) found dad waiting
18-20s for compose/ask-stream to fall back to the busy message while the
local card was degraded, with one candidate host not erroring out of its
own read timeout until 120s later. These tests pin the fix: every call site
must (a) fail fast with no connection attempt when every LLM endpoint is
already known-disabled, and (b) never wait past its named budget constant
regardless of how slow (or hung) the underlying host is.

Every host is mocked — nothing here opens a real network connection.
"""

from __future__ import annotations

import os
import time
from unittest.mock import patch

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")

import api.routers.chat as chat  # noqa: E402


def _fast_client(reply: str = "here's the layout"):
    client = type("FakeClient", (), {})()
    client.is_available = True
    client.chat = lambda *a, **k: reply
    return client


def _slow_client(delay_s: float, reply: str = "too slow"):
    client = type("FakeClient", (), {})()
    client.is_available = True

    def _chat(*_a, **_k):
        time.sleep(delay_s)
        return reply

    client.chat = _chat
    return client


# ── _resilient_chat (compose) ───────────────────────────────────────────


class TestResilientChatAllDisabled:
    def test_returns_busy_fallback_in_under_one_second(self):
        with patch.object(chat, "_get_local_oracle", return_value=(None, None)), \
             patch.object(chat, "_paid_clients", return_value=[]), \
             patch.object(chat.log, "warning") as mock_warn:
            start = time.monotonic()
            text, label = chat._resilient_chat([{"role": "user", "content": "hi"}])
            elapsed = time.monotonic() - start

        assert (text, label) == (None, None)
        assert elapsed < 1.0
        assert any("disabled" in str(c.args) for c in mock_warn.call_args_list)


class TestResilientChatSlowEndpoint:
    def test_returns_fallback_at_budget_not_at_host_timeout(self):
        # The fake host would take far longer than the budget to respond;
        # the call must be abandoned at the budget, not at the host's delay.
        slow = _slow_client(delay_s=5.0)
        with patch.object(chat, "_compose_budget_s", return_value=0.3), \
             patch.object(chat, "_get_local_oracle", return_value=(slow, "test-local")), \
             patch.object(chat, "_paid_clients", return_value=[]):
            start = time.monotonic()
            text, label = chat._resilient_chat([{"role": "user", "content": "hi"}])
            elapsed = time.monotonic() - start

        assert (text, label) == (None, None)
        assert elapsed < 2.0  # well under the host's 5s delay

    def test_budget_exceeded_is_logged_as_warning_not_error(self):
        slow = _slow_client(delay_s=2.0)
        with patch.object(chat, "_compose_budget_s", return_value=0.2), \
             patch.object(chat, "_get_local_oracle", return_value=(slow, "test-local")), \
             patch.object(chat, "_paid_clients", return_value=[]), \
             patch.object(chat.log, "warning") as mock_warn, \
             patch.object(chat.log, "error") as mock_error:
            chat._resilient_chat([{"role": "user", "content": "hi"}])

        assert mock_error.call_count == 0
        assert any("budget" in str(c.args).lower() for c in mock_warn.call_args_list)


class TestResilientChatHealthyEndpoint:
    def test_unchanged_behavior_when_local_answers_promptly(self):
        fast = _fast_client(reply="here is your dashboard")
        with patch.object(chat, "_get_local_oracle", return_value=(fast, "qwen3.6-local")), \
             patch.object(chat, "_paid_clients", return_value=[]):
            start = time.monotonic()
            text, label = chat._resilient_chat([{"role": "user", "content": "hi"}])
            elapsed = time.monotonic() - start

        assert text == "here is your dashboard"
        assert label == "qwen3.6-local"
        assert elapsed < 1.0

    def test_logs_elapsed_ms_and_label_so_the_budget_can_be_tuned_from_data(self):
        fast = _fast_client(reply="here is your dashboard")
        with patch.object(chat, "_get_local_oracle", return_value=(fast, "qwen3.6-local")), \
             patch.object(chat, "_paid_clients", return_value=[]), \
             patch.object(chat.log, "info") as mock_info:
            chat._resilient_chat([{"role": "user", "content": "hi"}])

        logged = [str(c.args) + str(c.kwargs) for c in mock_info.call_args_list]
        assert any("qwen3.6-local" in entry and "ms" in entry.lower() for entry in logged)


class TestComposeBudgetSetting:
    def test_reads_the_configured_budget_from_settings(self):
        from config import settings
        with patch.object(settings, "COMPOSE_LLM_BUDGET_S", 42.0):
            assert chat._compose_budget_s() == 42.0

    def test_defaults_to_eighteen_seconds_unchanged_from_pre_fail_fast_behavior(self):
        from config import settings
        with patch.object(settings, "COMPOSE_LLM_BUDGET_S", 18.0):
            assert chat._compose_budget_s() == 18.0


# ── _stream_verdict (ask/stream) ─────────────────────────────────────────


def _drain(gen):
    import json
    events = []
    for frame in gen:
        assert frame.startswith("data: ")
        events.append(json.loads(frame[len("data: "):].strip()))
    return events


class TestStreamVerdictAllDisabled:
    def test_emits_fallback_delta_and_done_in_under_one_second(self):
        with patch.object(chat, "_get_local_oracle", return_value=(None, None)), \
             patch.object(chat, "_paid_clients", return_value=[]), \
             patch.object(chat.log, "warning") as mock_warn:
            start = time.monotonic()
            events = _drain(chat._stream_verdict([{"role": "user", "content": "hi"}]))
            elapsed = time.monotonic() - start

        assert elapsed < 1.0
        assert events[0] == {"delta": chat.CARD_BUSY_MESSAGE}
        assert events[-1] == {"done": True}
        assert any("disabled" in str(c.args) for c in mock_warn.call_args_list)


class TestStreamVerdictBudget:
    def test_paid_failover_never_waits_past_stream_paid_budget(self):
        # Local produces nothing; two paid candidates would each take far
        # longer than the stream paid budget if allowed to run to completion.
        slow_a = _slow_client(delay_s=3.0, reply="a")
        slow_b = _slow_client(delay_s=3.0, reply="b")
        with patch.object(chat, "_STREAM_PAID_BUDGET_S", 0.3), \
             patch.object(chat, "_get_local_oracle", return_value=(None, None)), \
             patch.object(chat, "_paid_clients", return_value=[(slow_a, "openai"), (slow_b, "openrouter")]):
            start = time.monotonic()
            events = _drain(chat._stream_verdict([{"role": "user", "content": "hi"}]))
            elapsed = time.monotonic() - start

        assert elapsed < 3.0  # bounded well under either host's own 3s delay
        assert events[-1] == {"done": True}
        assert events[0] == {"delta": chat.CARD_BUSY_MESSAGE}

    def test_healthy_local_stream_is_unchanged(self):
        def _fake_stream(_messages, _client):
            yield ("delta", "stocks are ")
            yield ("delta", "going up")
            yield ("end", None)

        with patch.object(chat, "_get_local_oracle", return_value=(_fast_client(), "qwen3.6-local")), \
             patch.object(chat, "_stream_local_tokens", side_effect=_fake_stream):
            events = _drain(chat._stream_verdict([{"role": "user", "content": "hi"}]))

        assert events == [{"delta": "stocks are "}, {"delta": "going up"}, {"done": True}]
