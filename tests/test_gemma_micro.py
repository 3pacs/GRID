"""
Tests for the Gemma 3 270M micro models module.

Tests GemmaMicroClient, GemmaMicroPool, and MicroModelConfig
with mocked HTTP calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from gemma.micro import (
    ANOMALY_NARRATOR,
    EDGAR_EXTRACTOR,
    SIGNAL_CLASSIFIER,
    GemmaMicroClient,
    GemmaMicroPool,
    MicroModelConfig,
)


# ---------------------------------------------------------------------------
# MicroModelConfig
# ---------------------------------------------------------------------------


class TestMicroModelConfig:
    def test_frozen_dataclass(self) -> None:
        cfg = MicroModelConfig(
            name="test",
            base_url="http://localhost:9999",
            model="test-model",
            system_prompt="You are a test.",
        )
        assert cfg.name == "test"
        assert cfg.max_tokens == 256
        assert cfg.temperature == 0.1

        with pytest.raises(AttributeError):
            cfg.name = "changed"  # type: ignore[misc]

    def test_default_configs_exist(self) -> None:
        assert SIGNAL_CLASSIFIER.name == "signal_classifier"
        assert ANOMALY_NARRATOR.name == "anomaly_narrator"
        assert EDGAR_EXTRACTOR.name == "edgar_extractor"

    def test_classifier_config(self) -> None:
        assert SIGNAL_CLASSIFIER.max_tokens == 64
        assert SIGNAL_CLASSIFIER.temperature == 0.0
        assert "classifier" in SIGNAL_CLASSIFIER.system_prompt


# ---------------------------------------------------------------------------
# GemmaMicroClient
# ---------------------------------------------------------------------------


class TestGemmaMicroClient:
    @pytest.fixture
    def offline_client(self) -> GemmaMicroClient:
        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            client = GemmaMicroClient(MicroModelConfig(
                name="test",
                base_url="http://localhost:9999",
                model="test",
                system_prompt="Test",
            ))
        assert not client.is_available
        return client

    @pytest.fixture
    def online_client(self) -> GemmaMicroClient:
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("gemma.micro.requests.get", return_value=mock_resp):
            client = GemmaMicroClient(MicroModelConfig(
                name="test",
                base_url="http://localhost:8082",
                model="test",
                system_prompt="You are a test assistant.",
            ))
        assert client.is_available
        return client

    def test_run_offline_returns_none(self, offline_client: GemmaMicroClient) -> None:
        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            result = offline_client.run("test input")
        assert result is None

    def test_run_success(self, online_client: GemmaMicroClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "CATEGORY: macro\nURGENCY: high"}}],
        }

        with patch("gemma.micro.requests.post", return_value=mock_resp):
            result = online_client.run("Fed raised rates 25bp")

        assert result == "CATEGORY: macro\nURGENCY: high"

    def test_run_http_error(self, online_client: GemmaMicroClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 500

        with patch("gemma.micro.requests.post", return_value=mock_resp):
            result = online_client.run("test")

        assert result is None

    def test_run_exception(self, online_client: GemmaMicroClient) -> None:
        with patch("gemma.micro.requests.post", side_effect=ConnectionError):
            result = online_client.run("test")
        assert result is None

    def test_health_check_online(self, online_client: GemmaMicroClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("gemma.micro.requests.get", return_value=mock_resp):
            hc = online_client.health_check()

        assert hc["available"] is True
        assert hc["name"] == "test"
        assert hc["latency_ms"] is not None

    def test_health_check_offline(self, offline_client: GemmaMicroClient) -> None:
        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            hc = offline_client.health_check()
        assert hc["available"] is False

    def test_sends_system_prompt(self, online_client: GemmaMicroClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "ok"}}],
        }

        with patch("gemma.micro.requests.post", return_value=mock_resp) as mock_post:
            online_client.run("test")

        payload = mock_post.call_args[1]["json"]
        assert payload["messages"][0]["role"] == "system"
        assert payload["messages"][0]["content"] == "You are a test assistant."
        assert payload["messages"][1]["role"] == "user"


# ---------------------------------------------------------------------------
# GemmaMicroPool
# ---------------------------------------------------------------------------


class TestGemmaMicroPool:
    @pytest.fixture
    def pool(self) -> GemmaMicroPool:
        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            pool = GemmaMicroPool()
        return pool

    def test_pool_creates_three_clients(self, pool: GemmaMicroPool) -> None:
        assert pool.get_client("signal_classifier") is not None
        assert pool.get_client("anomaly_narrator") is not None
        assert pool.get_client("edgar_extractor") is not None

    def test_classify_signal_offline(self, pool: GemmaMicroPool) -> None:
        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            result = pool.classify_signal("Fed raised rates")
        assert result is None

    def test_narrate_anomaly_offline(self, pool: GemmaMicroPool) -> None:
        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            result = pool.narrate_anomaly("VIX spiked to 40")
        assert result is None

    def test_extract_edgar_offline(self, pool: GemmaMicroPool) -> None:
        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            result = pool.extract_edgar("Filing text...", "revenue,eps")
        assert result is None

    def test_health_check(self, pool: GemmaMicroPool) -> None:
        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            hc = pool.health_check()
        assert "signal_classifier" in hc
        assert "anomaly_narrator" in hc
        assert "edgar_extractor" in hc

    def test_available_count(self, pool: GemmaMicroPool) -> None:
        assert pool.available_count == 0  # All offline

    def test_get_nonexistent_client(self, pool: GemmaMicroPool) -> None:
        assert pool.get_client("nonexistent") is None

    def test_pool_with_custom_configs(self) -> None:
        cfg = MicroModelConfig(
            name="custom",
            base_url="http://localhost:9999",
            model="custom-model",
            system_prompt="Custom prompt",
        )

        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            pool = GemmaMicroPool(configs=[cfg])

        assert pool.get_client("custom") is not None
        assert pool.get_client("signal_classifier") is None

    def test_classify_signal_success(self) -> None:
        """Test full classify flow with mocked online client."""
        health_resp = MagicMock()
        health_resp.status_code = 200

        chat_resp = MagicMock()
        chat_resp.status_code = 200
        chat_resp.json.return_value = {
            "choices": [{"message": {"content": "CATEGORY: rates\nURGENCY: critical\nREASON: Fed rate hike"}}],
        }

        with patch("gemma.micro.requests.get", return_value=health_resp):
            pool = GemmaMicroPool()

        with patch("gemma.micro.requests.post", return_value=chat_resp):
            result = pool.classify_signal("Fed raised rates 50bp")

        assert result is not None
        assert "CATEGORY: rates" in result
        assert "URGENCY: critical" in result


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


class TestMicroPoolSingleton:
    def test_get_micro_pool_returns_same_instance(self) -> None:
        import gemma.micro as gm

        gm._pool_instance = None

        mock_settings = MagicMock()
        mock_settings.GEMMA_MICRO_CLASSIFIER_URL = ""
        mock_settings.GEMMA_MICRO_NARRATOR_URL = ""
        mock_settings.GEMMA_MICRO_EXTRACTOR_URL = ""

        with patch("gemma.micro.requests.get", side_effect=ConnectionError):
            with patch("config.settings", mock_settings):
                p1 = gm.get_micro_pool()
                p2 = gm.get_micro_pool()

        assert p1 is p2
        gm._pool_instance = None
