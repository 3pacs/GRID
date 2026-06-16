from __future__ import annotations

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
