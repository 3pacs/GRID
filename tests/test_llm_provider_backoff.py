from __future__ import annotations

import time

import requests


def test_openai_compatible_client_marks_auth_and_billing_failures_unavailable(monkeypatch):
    from llm.router import OpenAIClient

    class Resp:
        status_code = 402
        text = '{"error":{"message":"Insufficient credits","code":402}}'

        def json(self):
            return {}

    monkeypatch.setattr("llm.router.requests.post", lambda *_args, **_kwargs: Resp())

    client = OpenAIClient(api_key="key", base_url="https://example.invalid/v1")
    assert client.is_available is True
    assert client.chat([{"role": "user", "content": "hi"}]) is None
    assert client.is_available is False


def test_llamacpp_client_backs_off_endpoint_after_chat_timeout(monkeypatch):
    from llamacpp.client import LlamaCppClient, _ENDPOINT_BACKOFF_UNTIL

    _ENDPOINT_BACKOFF_UNTIL.clear()

    class HealthResp:
        status_code = 200

        def json(self):
            return {"default_generation_settings": {"n_ctx": 4096}}

    monkeypatch.setattr("llamacpp.client.requests.get", lambda *_args, **_kwargs: HealthResp())

    def _timeout(*_args, **_kwargs):
        raise requests.Timeout("timed out")

    monkeypatch.setattr("llamacpp.client.requests.post", _timeout)

    client = LlamaCppClient(base_url="http://slow-llm:8080", model="m")
    assert client.is_available is True
    assert client.chat([{"role": "user", "content": "hi"}]) is None
    assert client.is_available is False

    replacement = LlamaCppClient(base_url="http://slow-llm:8080", model="m")
    assert replacement.is_available is False


def test_llamacpp_backoff_is_shared_across_provider_names_on_same_host(monkeypatch):
    """Two router provider names (e.g. llamacpp_oracle + llamacpp) that
    resolve to the same base_url under different configured model strings
    must share one backoff window — otherwise a chat failure recorded
    against one provider name leaves its same-host sibling free to probe
    the still-broken endpoint fresh on the very next request."""
    from llamacpp.client import LlamaCppClient, _ENDPOINT_BACKOFF_UNTIL

    _ENDPOINT_BACKOFF_UNTIL.clear()

    class HealthResp:
        status_code = 200

        def json(self):
            return {"default_generation_settings": {"n_ctx": 4096}}

    monkeypatch.setattr("llamacpp.client.requests.get", lambda *_args, **_kwargs: HealthResp())

    def _timeout(*_args, **_kwargs):
        raise requests.Timeout("timed out")

    monkeypatch.setattr("llamacpp.client.requests.post", _timeout)

    oracle_client = LlamaCppClient(base_url="http://grid-svr-shim:8081", model="gemma-4-31B-it-Q4_K_M")
    assert oracle_client.is_available is True
    assert oracle_client.chat([{"role": "user", "content": "hi"}]) is None
    assert oracle_client.is_available is False

    # Same host, different configured model string (the "llamacpp" generic
    # provider vs. "llamacpp_oracle" both point at the :8081 shim).
    generic_client = LlamaCppClient(base_url="http://grid-svr-shim:8081", model="some-other-model-name")
    assert generic_client.is_available is False


def test_llamacpp_chat_retry_health_check_honors_backoff(monkeypatch):
    """A client constructed while its host is already in a chat-failure
    backoff window must not spend a network round-trip re-probing /health
    on .chat() — that would defeat the whole point of the backoff."""
    from llamacpp.client import LlamaCppClient, _ENDPOINT_BACKOFF_UNTIL

    _ENDPOINT_BACKOFF_UNTIL.clear()
    _ENDPOINT_BACKOFF_UNTIL["http://slow-llm:8080"] = time.time() + 300.0

    def _fail_if_called(*_args, **_kwargs):
        raise AssertionError("no network call should be attempted during an active backoff window")

    monkeypatch.setattr("llamacpp.client.requests.get", _fail_if_called)
    monkeypatch.setattr("llamacpp.client.requests.post", _fail_if_called)

    client = LlamaCppClient(base_url="http://slow-llm:8080", model="m")
    assert client.is_available is False

    result = client.chat([{"role": "user", "content": "hi"}])
    assert result is None

    _ENDPOINT_BACKOFF_UNTIL.clear()
