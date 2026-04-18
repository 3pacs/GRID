"""Tests for the llama.cpp QUICK-tier client factory (redbox node)."""

from unittest.mock import patch


class TestLlamacppQuickFactory:
    """`_create_client("llamacpp_quick")` dispatch + env-gated behavior."""

    def test_router_dispatches_llamacpp_quick(self) -> None:
        """Router recognises 'llamacpp_quick' provider and calls the QUICK factory."""
        with patch("llm.router._create_llamacpp_quick_client") as mock_factory:
            mock_factory.return_value = "sentinel"
            from llm.router import _create_client

            assert _create_client("llamacpp_quick") == "sentinel"
            mock_factory.assert_called_once()

    def test_factory_returns_none_when_disabled(self) -> None:
        """Factory returns None when LLAMACPP_QUICK_ENABLED is False."""
        from llm.router import _create_llamacpp_quick_client

        settings = type("S", (), {"LLAMACPP_QUICK_ENABLED": False})()
        assert _create_llamacpp_quick_client(settings) is None

    @patch("llamacpp.client.requests")
    def test_factory_builds_client_when_enabled(self, mock_requests: object) -> None:
        """Factory returns a LlamaCppClient pointed at the configured QUICK URL."""
        # Short-circuit the health probe so construction succeeds offline.
        from unittest.mock import MagicMock

        ok = MagicMock()
        ok.status_code = 200
        ok.json.return_value = {"default_generation_settings": {"n_ctx": 8192}}
        mock_requests.get.return_value = ok  # type: ignore[attr-defined]

        from llm.router import _create_llamacpp_quick_client

        settings = type(
            "S",
            (),
            {
                "LLAMACPP_QUICK_ENABLED": True,
                "LLAMACPP_QUICK_BASE_URL": "http://100.126.129.45:8080",
                "LLAMACPP_QUICK_CHAT_MODEL": "qwen3-14b",
                "LLAMACPP_QUICK_TIMEOUT_SECONDS": 120,
            },
        )()

        client = _create_llamacpp_quick_client(settings)
        assert client is not None
        assert client.base_url == "http://100.126.129.45:8080"
        assert client.model == "qwen3-14b"
        assert client.timeout == 120


class TestLlamacppZ4Factory:
    """`_create_client("llamacpp_z4")` dispatch + env-gated behavior."""

    def test_router_dispatches_llamacpp_z4(self) -> None:
        """Router recognises 'llamacpp_z4' provider and calls the Z4 factory."""
        with patch("llm.router._create_llamacpp_z4_client") as mock_factory:
            mock_factory.return_value = "sentinel"
            from llm.router import _create_client

            assert _create_client("llamacpp_z4") == "sentinel"
            mock_factory.assert_called_once()

    def test_z4_factory_returns_none_when_disabled(self) -> None:
        """Factory returns None when LLAMACPP_Z4_ENABLED is False."""
        from llm.router import _create_llamacpp_z4_client

        settings = type("S", (), {"LLAMACPP_Z4_ENABLED": False})()
        assert _create_llamacpp_z4_client(settings) is None

    @patch("llamacpp.client.requests")
    def test_z4_factory_builds_client_when_enabled(self, mock_requests: object) -> None:
        """Factory returns a LlamaCppClient pointed at the configured gridz4 URL."""
        from unittest.mock import MagicMock

        ok = MagicMock()
        ok.status_code = 200
        ok.json.return_value = {"default_generation_settings": {"n_ctx": 8192}}
        mock_requests.get.return_value = ok  # type: ignore[attr-defined]

        from llm.router import _create_llamacpp_z4_client

        settings = type(
            "S",
            (),
            {
                "LLAMACPP_Z4_ENABLED": True,
                "LLAMACPP_Z4_BASE_URL": "http://gridz4:8080",
                "LLAMACPP_Z4_CHAT_MODEL": "Qwen3.5-9B-Claude-Opus-Reasoning-v2.Q4_K_M.gguf",
                "LLAMACPP_Z4_TIMEOUT_SECONDS": 180,
            },
        )()

        client = _create_llamacpp_z4_client(settings)
        assert client is not None
        assert client.base_url == "http://gridz4:8080"
        assert client.model == "Qwen3.5-9B-Claude-Opus-Reasoning-v2.Q4_K_M.gguf"
        assert client.timeout == 180
