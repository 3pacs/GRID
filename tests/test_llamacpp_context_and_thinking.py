"""llama.cpp client: thinking switch, prompt-budget fitting, probe caching.

2026-11-04 grid-hermes evidence: 261x/day "chat returned reasoning without
final content; finish=length" (Qwen3.8-27B, --reasoning-effort xhigh, one
8192-token slot), 9x/day HTTP 400 exceed_context_size_error (9.2-9.9k-token
prompts), 76x/day "llama.cpp server not available at
http://100.126.129.45:8080" for a retired host. All HTTP is faked.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

import llamacpp.client as lc


class _Resp:
    def __init__(self, status: int = 200, payload: Any = None) -> None:
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.headers = {"content-type": "application/json"}

    def json(self) -> Any:
        return self._payload


def _chat_ok(content: str = "OK") -> _Resp:
    return _Resp(200, {"choices": [{"message": {"content": content}, "finish_reason": "stop"}],
                       "usage": {"prompt_tokens": 5, "completion_tokens": 1}})


@pytest.fixture(autouse=True)
def _clean_endpoint_state():
    for table in (lc._ENDPOINT_BACKOFF_UNTIL, lc._PROBE_FAILED_UNTIL, lc._CTX_SIZE_BY_URL):
        table.clear()
    lc._UNAVAILABLE_WARNED.clear()
    yield
    for table in (lc._ENDPOINT_BACKOFF_UNTIL, lc._PROBE_FAILED_UNTIL, lc._CTX_SIZE_BY_URL):
        table.clear()
    lc._UNAVAILABLE_WARNED.clear()


def _healthy(monkeypatch, n_ctx: int = 8192, props: Any = None) -> None:
    def get(url, timeout=None):
        if url.endswith("/props"):
            return _Resp(200, props if props is not None else
                         {"default_generation_settings": {"n_ctx": n_ctx}})
        return _Resp(200, {"status": "ok"})

    monkeypatch.setattr(lc.requests, "get", get)


def _capture_posts(monkeypatch, responder) -> list[tuple[str, dict]]:
    sent: list[tuple[str, dict]] = []

    def post(url, json=None, timeout=None):
        sent.append((url, json))
        return responder(url, json)

    monkeypatch.setattr(lc.requests, "post", post)
    return sent


# ── thinking switch ──────────────────────────────────────────────────


def test_enable_thinking_false_is_sent_and_drops_reasoning_headroom(monkeypatch):
    _healthy(monkeypatch)
    sent = _capture_posts(monkeypatch, lambda url, body: _chat_ok())
    client = lc.LlamaCppClient(base_url="http://reason:18081", reasoning_headroom=4096)
    assert client.chat([{"role": "user", "content": "hi"}], num_predict=2000,
                       enable_thinking=False) == "OK"
    (url, body), = [s for s in sent if s[0].endswith("/v1/chat/completions")]
    assert body["chat_template_kwargs"] == {"enable_thinking": False}
    assert body["max_tokens"] == 2000


def test_default_call_is_unchanged(monkeypatch):
    _healthy(monkeypatch)
    sent = _capture_posts(monkeypatch, lambda url, body: _chat_ok())
    client = lc.LlamaCppClient(base_url="http://reason:18081", reasoning_headroom=4096)
    client.chat([{"role": "user", "content": "hi"}], num_predict=2000)
    body = sent[-1][1]
    assert "chat_template_kwargs" not in body
    assert body["max_tokens"] == 6096


def test_generate_forwards_enable_thinking(monkeypatch):
    _healthy(monkeypatch)
    sent = _capture_posts(monkeypatch, lambda url, body: _chat_ok())
    client = lc.LlamaCppClient(base_url="http://reason:18081")
    client.generate("hi", enable_thinking=False)
    assert sent[-1][1]["chat_template_kwargs"] == {"enable_thinking": False}


# ── context size discovery ───────────────────────────────────────────


@pytest.mark.parametrize("props,expected", [
    ({"default_generation_settings": {"n_ctx": 8192}}, 8192),
    ({"default_generation_settings": {"params": {"n_ctx": 16384}}}, 16384),
    ({"n_ctx": 2048}, 2048),
    ({}, None),
    ({"default_generation_settings": {"n_ctx": "8192"}}, None),
])
def test_ctx_size_from_props(props, expected):
    assert lc._ctx_size_from_props(props) == expected


def test_ctx_size_comes_from_the_server(monkeypatch):
    _healthy(monkeypatch, n_ctx=8192)
    client = lc.LlamaCppClient(base_url="http://reason:18081")
    assert client._ctx_size == 8192


# ── prompt-budget fitting ────────────────────────────────────────────


def test_short_prompt_is_not_tokenized(monkeypatch):
    _healthy(monkeypatch)
    sent = _capture_posts(monkeypatch, lambda url, body: _chat_ok())
    client = lc.LlamaCppClient(base_url="http://reason:18081")
    client.chat([{"role": "user", "content": "short"}], enable_thinking=False)
    assert not any(url.endswith("/tokenize") for url, _ in sent)


def test_over_long_prompt_is_truncated_to_leave_answer_room(monkeypatch):
    _healthy(monkeypatch, n_ctx=8192)

    def responder(url, body):
        if url.endswith("/tokenize"):
            # 2.4 chars/token, like the indented JSON status report.
            return _Resp(200, {"tokens": [0] * int(len(body["content"]) / 2.4)})
        return _chat_ok("ANSWER")

    sent = _capture_posts(monkeypatch, responder)
    client = lc.LlamaCppClient(base_url="http://reason:18081")
    huge = "x" * 30000  # ~12.5k tokens: would have been a 400
    out = client.chat([{"role": "system", "content": "rules"},
                       {"role": "user", "content": huge}],
                      num_predict=2000, enable_thinking=False)
    assert out == "ANSWER"
    body = [b for u, b in sent if u.endswith("/v1/chat/completions")][-1]
    user = body["messages"][1]["content"]
    assert "characters truncated to fit the model context" in user
    assert body["messages"][0]["content"] == "rules"
    prompt_tokens = int(sum(len(m["content"]) for m in body["messages"]) / 2.4)
    assert prompt_tokens + body["max_tokens"] <= 8192
    assert body["max_tokens"] >= 2000, "the answer keeps its requested room"
    # ...without throwing away more than needed (limit = 8192-2000-64).
    assert prompt_tokens >= 0.85 * (8192 - 2000 - 64)


def test_dense_prompt_truncation_keeps_most_of_the_budget(monkeypatch):
    """grid-svr probe 2026-11-04: a 1.4 chars/token prompt was cut to 1029
    tokens against a 6128-token limit by a margin applied to the cut."""
    _healthy(monkeypatch, n_ctx=8192)

    def responder(url, body):
        if url.endswith("/tokenize"):
            return _Resp(200, {"tokens": [0] * int(len(body["content"]) / 1.4)})
        return _chat_ok()

    _capture_posts(monkeypatch, responder)
    client = lc.LlamaCppClient(base_url="http://reason:18081")
    fitted, tokens = client._fit_messages_to_context(
        [{"role": "user", "content": "a" * 56000}], 2000)
    limit = 8192 - 2000 - 64
    assert 0.85 * limit <= tokens <= limit


def test_tokenize_unavailable_falls_back_to_conservative_estimate(monkeypatch):
    _healthy(monkeypatch, n_ctx=4096)

    def responder(url, body):
        if url.endswith("/tokenize"):
            return _Resp(404, {})
        return _chat_ok()

    sent = _capture_posts(monkeypatch, responder)
    client = lc.LlamaCppClient(base_url="http://reason:18081")
    client.chat([{"role": "user", "content": "y" * 20000}], num_predict=512)
    body = [b for u, b in sent if u.endswith("/v1/chat/completions")][-1]
    assert len(body["messages"][0]["content"]) < 20000 / 2


def test_context_overflow_400_is_refit_and_retried_once(monkeypatch):
    _healthy(monkeypatch, n_ctx=16384)  # /props over-reports; server says 8192
    chats = {"n": 0}

    def responder(url, body):
        if url.endswith("/tokenize"):
            return _Resp(200, {"tokens": [0] * int(len(body["content"]) / 2.4)})
        chats["n"] += 1
        if chats["n"] == 1:
            return _Resp(400, {"error": {"code": 400, "type": "exceed_context_size_error",
                                         "message": "request (9318 tokens) exceeds ...",
                                         "n_prompt_tokens": 9318, "n_ctx": 8192}})
        return _chat_ok("SECOND")

    sent = _capture_posts(monkeypatch, responder)
    client = lc.LlamaCppClient(base_url="http://reason:18081")
    out = client.chat([{"role": "user", "content": "z" * 22000}], num_predict=2000,
                      enable_thinking=False)
    assert out == "SECOND"
    assert chats["n"] == 2
    assert client._ctx_size == 8192
    retry_body = [b for u, b in sent if u.endswith("/v1/chat/completions")][-1]
    assert "characters truncated" in retry_body["messages"][0]["content"]


def test_other_400_is_not_retried(monkeypatch):
    _healthy(monkeypatch)
    chats = {"n": 0}

    def responder(url, body):
        chats["n"] += 1
        return _Resp(400, {"error": {"type": "invalid_request_error", "message": "bad"}})

    _capture_posts(monkeypatch, responder)
    client = lc.LlamaCppClient(base_url="http://reason:18081")
    assert client.chat([{"role": "user", "content": "hi"}]) is None
    assert chats["n"] == 1


# ── unavailable probe cache ──────────────────────────────────────────


def test_dead_endpoint_is_probed_and_warned_once_per_ttl(monkeypatch):
    probes = {"n": 0}

    def dead(url, timeout=None):
        probes["n"] += 1
        raise ConnectionError("no route")

    monkeypatch.setattr(lc.requests, "get", dead)
    warnings: list[str] = []
    monkeypatch.setattr(lc.log, "warning", lambda template, **k: warnings.append(template.format(**k)))
    for _ in range(5):
        client = lc.LlamaCppClient(base_url="http://100.126.129.45:8080")
        assert client.is_available is False
        assert client.chat([{"role": "user", "content": "hi"}]) is None
    assert probes["n"] == 1
    assert len(warnings) == 1 and "not available" in warnings[0]

    # TTL expiry: re-probed, still dead -> no second warning.
    lc._PROBE_FAILED_UNTIL.clear()
    lc.LlamaCppClient(base_url="http://100.126.129.45:8080")
    assert probes["n"] == 2 and len(warnings) == 1

    # Recovery re-arms the warning for a later outage.
    lc._PROBE_FAILED_UNTIL.clear()
    _healthy(monkeypatch)
    assert lc.LlamaCppClient(base_url="http://100.126.129.45:8080").is_available is True
    assert "http://100.126.129.45:8080" not in lc._UNAVAILABLE_WARNED
