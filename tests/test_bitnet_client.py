"""Tests for the BitNet client module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch



class TestBitNetClient:
    """BitNet client unit tests."""

    def test_import(self) -> None:
        """Module imports without errors."""
        from bitnet.client import BitNetClient
        assert BitNetClient is not None

    def test_init_server_offline(self) -> None:
        """Client initialises gracefully when server is down."""
        from bitnet.client import BitNetClient

        client = BitNetClient(base_url="http://localhost:99999")
        assert client.is_available is False

    def test_chat_returns_none_when_unavailable(self) -> None:
        """chat() returns None instead of crashing when server is down."""
        from bitnet.client import BitNetClient

        client = BitNetClient(base_url="http://localhost:99999")
        result = client.chat([{"role": "user", "content": "hello"}])
        assert result is None

    def test_generate_returns_none_when_unavailable(self) -> None:
        """generate() returns None when server is down."""
        from bitnet.client import BitNetClient

        client = BitNetClient(base_url="http://localhost:99999")
        result = client.generate("hello")
        assert result is None

    def test_embed_returns_none_when_unavailable(self) -> None:
        """embed() returns None when server is down."""
        from bitnet.client import BitNetClient

        client = BitNetClient(base_url="http://localhost:99999")
        result = client.embed(["hello"])
        assert result is None

    def test_health_check_offline(self) -> None:
        """health_check() returns structured result when offline."""
        from bitnet.client import BitNetClient

        client = BitNetClient(base_url="http://localhost:99999")
        hc = client.health_check()
        assert hc["available"] is False
        assert hc["provider"] == "bitnet"
        assert hc["endpoint"] == "http://localhost:99999"

    def test_list_models_offline(self) -> None:
        """list_models() returns empty list when offline."""
        from bitnet.client import BitNetClient

        client = BitNetClient(base_url="http://localhost:99999")
        assert client.list_models() == []

    def test_pull_model_not_supported(self) -> None:
        """pull_model() returns False (not supported)."""
        from bitnet.client import BitNetClient

        client = BitNetClient(base_url="http://localhost:99999")
        assert client.pull_model("some-model") is False

    def test_protocol_compliance(self) -> None:
        """BitNetClient satisfies the LLMClient protocol."""
        from bitnet.client import BitNetClient
        from llm.protocol import LLMClient

        assert isinstance(BitNetClient(base_url="http://localhost:99999"), LLMClient)

    def test_router_dispatches_bitnet(self) -> None:
        """Router's _create_client recognises 'bitnet' provider."""
        from llm.router import _create_client

        # BITNET_ENABLED is False by default, so should return None
        result = _create_client("bitnet")
        assert result is None

    @patch("bitnet.client.requests")
    def test_chat_success(self, mock_requests: MagicMock) -> None:
        """chat() returns content on successful response."""
        from bitnet.client import BitNetClient

        # Mock health check
        mock_health = MagicMock()
        mock_health.status_code = 200
        mock_health.json.return_value = {}

        # Mock props
        mock_props = MagicMock()
        mock_props.json.return_value = {"default_generation_settings": {"n_ctx": 2048}}

        mock_requests.get.side_effect = [mock_health, mock_props]

        client = BitNetClient(base_url="http://localhost:8090")
        assert client.is_available is True

        # Mock chat response
        mock_chat_resp = MagicMock()
        mock_chat_resp.status_code = 200
        mock_chat_resp.json.return_value = {
            "choices": [{"message": {"content": "Hello from BitNet!"}}],
            "model": "bitnet-b1.58-2B-4T",
            "usage": {"prompt_tokens": 10, "completion_tokens": 5},
        }
        mock_requests.post.return_value = mock_chat_resp

        result = client.chat([{"role": "user", "content": "hello"}])
        assert result == "Hello from BitNet!"

    def test_config_defaults(self) -> None:
        """Config has BitNet settings with ENABLED=False."""
        from config import settings

        assert settings.BITNET_ENABLED is False
        assert settings.BITNET_BASE_URL == "http://localhost:8090"
        assert settings.BITNET_CHAT_MODEL == "bitnet-b1.58-2B-4T"
