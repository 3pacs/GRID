"""
Tests for the Gemma 3 27B QAT client.

Tests the GemmaClient class against the LLMClient protocol,
mocking HTTP calls so no live server is required.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from gemma.client import GemmaClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def offline_client() -> GemmaClient:
    """Create a GemmaClient that cannot reach a server."""
    with patch("gemma.client.requests.get", side_effect=ConnectionError("offline")):
        client = GemmaClient(
            base_url="http://localhost:9999",
            model="gemma-3-27b-it",
        )
    assert not client.is_available
    return client


@pytest.fixture
def online_client() -> GemmaClient:
    """Create a GemmaClient that thinks it can reach a server."""
    health_resp = MagicMock()
    health_resp.status_code = 200
    health_resp.headers = {"content-type": "application/json"}
    health_resp.json.return_value = {"status": "ok"}

    props_resp = MagicMock()
    props_resp.json.return_value = {
        "default_generation_settings": {"n_ctx": 131072}
    }

    with patch("gemma.client.requests.get", side_effect=[health_resp, props_resp]):
        client = GemmaClient(
            base_url="http://localhost:8081",
            model="gemma-3-27b-it",
        )
    assert client.is_available
    assert client._ctx_size == 131072
    return client


# ---------------------------------------------------------------------------
# Init / Health
# ---------------------------------------------------------------------------


class TestGemmaInit:
    def test_offline_init(self, offline_client: GemmaClient) -> None:
        assert not offline_client.is_available
        assert offline_client.base_url == "http://localhost:9999"
        assert offline_client.model == "gemma-3-27b-it"

    def test_online_init(self, online_client: GemmaClient) -> None:
        assert online_client.is_available
        assert online_client._ctx_size == 131072

    def test_health_check_offline(self, offline_client: GemmaClient) -> None:
        with patch("gemma.client.requests.get", side_effect=ConnectionError):
            hc = offline_client.health_check()
        assert hc["available"] is False
        assert hc["endpoint"] == "http://localhost:9999"

    def test_health_check_online(self, online_client: GemmaClient) -> None:
        mock_health = MagicMock()
        mock_health.status_code = 200
        mock_health.headers = {"content-type": "application/json"}
        mock_health.json.return_value = {
            "status": "ok",
            "slots_idle": 2,
            "slots_processing": 1,
        }

        mock_models = MagicMock()
        mock_models.status_code = 200
        mock_models.json.return_value = {
            "data": [{"id": "gemma-3-27b-it"}]
        }

        with patch("gemma.client.requests.get", side_effect=[mock_health, mock_models]):
            hc = online_client.health_check()

        assert hc["available"] is True
        assert hc["latency_ms"] is not None
        assert hc["slots_idle"] == 2
        assert hc["slots_processing"] == 1
        assert "gemma-3-27b-it" in hc["models"]


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------


class TestGemmaChat:
    def test_chat_when_offline_returns_none(self, offline_client: GemmaClient) -> None:
        # Re-check also fails
        with patch("gemma.client.requests.get", side_effect=ConnectionError):
            result = offline_client.chat([{"role": "user", "content": "hello"}])
        assert result is None

    def test_chat_success(self, online_client: GemmaClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "Hello from Gemma!"}}],
            "model": "gemma-3-27b-it",
            "usage": {"prompt_tokens": 5, "completion_tokens": 4},
        }

        with patch("gemma.client.requests.post", return_value=mock_resp):
            result = online_client.chat([{"role": "user", "content": "hello"}])

        assert result == "Hello from Gemma!"

    def test_chat_http_error(self, online_client: GemmaClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"

        with patch("gemma.client.requests.post", return_value=mock_resp):
            result = online_client.chat([{"role": "user", "content": "hello"}])

        assert result is None

    def test_chat_exception(self, online_client: GemmaClient) -> None:
        with patch("gemma.client.requests.post", side_effect=ConnectionError("down")):
            result = online_client.chat([{"role": "user", "content": "hello"}])

        assert result is None

    def test_chat_clamps_tokens(self, online_client: GemmaClient) -> None:
        """Verify max_tokens is clamped when prompt is very large."""
        # Create a message that would use most of the context
        big_msg = "x" * (131072 * 3)  # ~131K tokens estimated
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "ok"}}],
            "usage": {},
        }

        with patch("gemma.client.requests.post", return_value=mock_resp) as mock_post:
            online_client.chat(
                [{"role": "user", "content": big_msg}],
                num_predict=10000,
            )

        # The actual max_tokens sent should be clamped
        payload = mock_post.call_args[1]["json"]
        assert payload["max_tokens"] <= 131072


# ---------------------------------------------------------------------------
# Generate
# ---------------------------------------------------------------------------


class TestGemmaGenerate:
    def test_generate_delegates_to_chat(self, online_client: GemmaClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "Generated text"}}],
            "usage": {},
        }

        with patch("gemma.client.requests.post", return_value=mock_resp):
            result = online_client.generate("Analyze BTC", system="You are an analyst")

        assert result == "Generated text"

    def test_generate_offline_returns_none(self, offline_client: GemmaClient) -> None:
        with patch("gemma.client.requests.get", side_effect=ConnectionError):
            result = offline_client.generate("test")
        assert result is None


# ---------------------------------------------------------------------------
# Embed
# ---------------------------------------------------------------------------


class TestGemmaEmbed:
    def test_embed_success(self, online_client: GemmaClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": [
                {"embedding": [0.1, 0.2, 0.3]},
                {"embedding": [0.4, 0.5, 0.6]},
            ]
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("gemma.client.requests.post", return_value=mock_resp):
            result = online_client.embed(["hello", "world"])

        assert result is not None
        assert len(result) == 2
        assert result[0] == [0.1, 0.2, 0.3]

    def test_embed_offline_returns_none(self, offline_client: GemmaClient) -> None:
        result = offline_client.embed(["hello"])
        assert result is None

    def test_embed_exception_returns_none(self, online_client: GemmaClient) -> None:
        with patch("gemma.client.requests.post", side_effect=ConnectionError):
            result = online_client.embed(["hello"])
        assert result is None


# ---------------------------------------------------------------------------
# Model management
# ---------------------------------------------------------------------------


class TestGemmaModels:
    def test_list_models(self, online_client: GemmaClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": [
                {"id": "gemma-3-27b-it", "object": "model"},
            ]
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("gemma.client.requests.get", return_value=mock_resp):
            models = online_client.list_models()

        assert len(models) == 1
        assert models[0]["id"] == "gemma-3-27b-it"

    def test_get_model_names(self, online_client: GemmaClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": [{"id": "gemma-3-27b-it"}]
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("gemma.client.requests.get", return_value=mock_resp):
            names = online_client.get_model_names()

        assert names == ["gemma-3-27b-it"]

    def test_pull_model_returns_false(self, online_client: GemmaClient) -> None:
        assert online_client.pull_model("anything") is False


# ---------------------------------------------------------------------------
# Vision (multimodal)
# ---------------------------------------------------------------------------


class TestGemmaVision:
    def test_chat_with_image_success(self, online_client: GemmaClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "I see a chart showing BTC at 70K"}}],
            "usage": {},
        }

        with patch("gemma.client.requests.post", return_value=mock_resp) as mock_post:
            result = online_client.chat_with_image(
                prompt="What does this chart show?",
                image_base64="iVBORw0KGgo=",
            )

        assert result == "I see a chart showing BTC at 70K"
        # Verify multimodal message format was sent
        payload = mock_post.call_args[1]["json"]
        msg_content = payload["messages"][0]["content"]
        assert isinstance(msg_content, list)
        assert msg_content[0]["type"] == "text"
        assert msg_content[1]["type"] == "image_url"

    def test_chat_with_image_offline(self, offline_client: GemmaClient) -> None:
        result = offline_client.chat_with_image(
            prompt="test",
            image_base64="abc",
        )
        assert result is None

    def test_chat_with_image_error(self, online_client: GemmaClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 400

        with patch("gemma.client.requests.post", return_value=mock_resp):
            result = online_client.chat_with_image(
                prompt="test",
                image_base64="abc",
            )

        assert result is None


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


class TestGemmaProtocol:
    def test_implements_llm_protocol(self, offline_client: GemmaClient) -> None:
        """Verify GemmaClient satisfies the LLMClient protocol."""
        from llm.protocol import LLMClient

        assert isinstance(offline_client, LLMClient)

    def test_has_is_available_attribute(self, offline_client: GemmaClient) -> None:
        assert hasattr(offline_client, "is_available")
        assert isinstance(offline_client.is_available, bool)


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


class TestGemmaSingleton:
    def test_get_client_returns_same_instance(self) -> None:
        import gemma.client as gc

        gc._client_instance = None  # Reset

        mock_settings = MagicMock()
        mock_settings.GEMMA_BASE_URL = "http://localhost:8081"
        mock_settings.GEMMA_CHAT_MODEL = "gemma-3-27b-it"
        mock_settings.GEMMA_EMBED_MODEL = "gemma-3-27b-it"
        mock_settings.GEMMA_TIMEOUT_SECONDS = 180

        with patch("gemma.client.requests.get", side_effect=ConnectionError):
            with patch("config.settings", mock_settings):
                c1 = gc.get_client()
                c2 = gc.get_client()

        assert c1 is c2
        gc._client_instance = None  # Clean up
