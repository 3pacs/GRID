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


class TestLlamacppOracleFactory:
    """ORACLE client keeps enough generation room for reasoning models."""

    @patch("llamacpp.client.requests")
    def test_oracle_factory_sets_reasoning_budget(self, mock_requests: object) -> None:
        from unittest.mock import MagicMock

        ok = MagicMock()
        ok.status_code = 200
        ok.json.return_value = {"default_generation_settings": {"n_ctx": 16384}}
        mock_requests.get.return_value = ok  # type: ignore[attr-defined]

        from llm.router import _create_llamacpp_oracle_client

        settings = type(
            "S",
            (),
            {
                "LLAMACPP_ORACLE_ENABLED": True,
                "LLAMACPP_ORACLE_BASE_URL": "http://localhost:8081",
                "LLAMACPP_ORACLE_CHAT_MODEL": "Qwen3-32B-Q4_K_M",
                "LLAMACPP_ORACLE_TIMEOUT_SECONDS": 900,
                "LLAMACPP_ORACLE_NUM_PREDICT": 10000,
                "LLAMACPP_ORACLE_MIN_NUM_PREDICT": 10000,
            },
        )()

        client = _create_llamacpp_oracle_client(settings)
        assert client is not None
        assert client.base_url == "http://localhost:8081"
        assert client.model == "Qwen3-32B-Q4_K_M"
        assert client.timeout == 900
        assert client.default_num_predict == 10000
        assert client.min_num_predict == 10000


class TestLlamacppReasoningResponses:
    """Qwen-style reasoning responses must not silently become blank output."""

    @patch("llamacpp.client.requests")
    def test_min_num_predict_is_applied_before_request(self, mock_requests: object) -> None:
        from unittest.mock import MagicMock

        ok = MagicMock()
        ok.status_code = 200
        ok.json.return_value = {"default_generation_settings": {"n_ctx": 16384}}
        mock_requests.get.return_value = ok  # type: ignore[attr-defined]

        done = MagicMock()
        done.status_code = 200
        done.json.return_value = {
            "model": "Qwen3-32B-Q4_K_M",
            "choices": [
                {
                    "finish_reason": "stop",
                    "message": {"content": "final answer"},
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20},
        }
        mock_requests.post.return_value = done  # type: ignore[attr-defined]

        from llamacpp.client import LlamaCppClient

        client = LlamaCppClient(
            base_url="http://localhost:8081",
            model="Qwen3-32B-Q4_K_M",
            timeout=900,
            min_num_predict=10000,
        )

        assert client.chat([{"role": "user", "content": "x"}], num_predict=8) == "final answer"
        payload = mock_requests.post.call_args.kwargs["json"]  # type: ignore[attr-defined]
        assert payload["max_tokens"] == 10000

    @patch("llamacpp.client.requests")
    def test_reasoning_without_final_content_returns_none(self, mock_requests: object) -> None:
        from unittest.mock import MagicMock

        ok = MagicMock()
        ok.status_code = 200
        ok.json.return_value = {"default_generation_settings": {"n_ctx": 8192}}
        mock_requests.get.return_value = ok  # type: ignore[attr-defined]

        unfinished = MagicMock()
        unfinished.status_code = 200
        unfinished.json.return_value = {
            "model": "Qwen3-32B-Q4_K_M",
            "choices": [
                {
                    "finish_reason": "length",
                    "message": {
                        "content": "",
                        "reasoning_content": "still thinking",
                    },
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 8},
        }
        mock_requests.post.return_value = unfinished  # type: ignore[attr-defined]

        from llamacpp.client import LlamaCppClient

        client = LlamaCppClient(base_url="http://localhost:8081", model="Qwen3-32B-Q4_K_M")
        assert client.chat([{"role": "user", "content": "x"}], num_predict=8) is None
