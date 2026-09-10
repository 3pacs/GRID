"""Embedding chain + Gemini provider tests (hand-off 04, 2026-09-10).

Background: embeddings used to go to the CPU-only llama.cpp unit on grid-svr
:8080 via ``HYPERSPACE_BASE_URL``. That server was never started with
``--embeddings``, so every call returned HTTP 501. The operator retired
CPU-only inference ("there should be no CPU-only Qwens"), so embeddings now
walk ``EMBED_PROVIDER_CHAIN`` across tailnet GPU Ollama nodes.

Nothing here touches the network: every provider is a fake injected into the
router's client cache, and the Gemini client is exercised against a stubbed
``requests.post``.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

import config
from llm import router


@pytest.fixture(autouse=True)
def _clean_router_state():
    """Each test gets a clean provider cache and no leftover backoffs."""
    router._client_cache.clear()
    router._PROVIDER_BACKOFF_UNTIL.clear()
    router._PROVIDER_BACKOFF_REASONS.clear()
    yield
    router._client_cache.clear()
    router._PROVIDER_BACKOFF_UNTIL.clear()
    router._PROVIDER_BACKOFF_REASONS.clear()


class _FakeEmbedProvider:
    """Stand-in for an Ollama node. Records the calls it received."""

    def __init__(
        self,
        available: bool = True,
        vectors: list[list[float]] | None = None,
        raises: bool = False,
    ) -> None:
        self.is_available = available
        self._vectors = vectors
        self._raises = raises
        self.calls: list[list[str]] = []

    def embed(
        self, texts: list[str], model: str | None = None
    ) -> list[list[float]] | None:
        self.calls.append(list(texts))
        if self._raises:
            raise RuntimeError("node exploded")
        return self._vectors


def _install(monkeypatch, providers: dict[str, Any]) -> None:
    """Make ``_create_client`` hand back our fakes and nothing else."""
    monkeypatch.setattr(router, "_create_client", lambda name: providers.get(name))


# ---------------------------------------------------------------------------
# EMBED_PROVIDER_CHAIN configuration
# ---------------------------------------------------------------------------

class TestEmbedChainConfig:
    def test_setting_is_declared_with_gpu_node_order(self) -> None:
        assert "EMBED_PROVIDER_CHAIN" in config.Settings.model_fields
        default = config.Settings.model_fields["EMBED_PROVIDER_CHAIN"].default
        # gridz4 is the only separate tailnet box verified serving
        # nomic-embed-text, so it leads. grid-svr's Ollama shares the 3090
        # with the REASON/ORACLE llama-server, so it must come last.
        assert default == "ollama_z4,ollama_koala,ollama_z400,ollama"

    def test_grid_svr_ollama_is_last_resort(self) -> None:
        """Embedding batches must not evict the 27B chat model on the 3090."""
        chain = config.Settings.model_fields["EMBED_PROVIDER_CHAIN"].default.split(",")
        assert chain[-1] == "ollama"
        assert chain[0] == "ollama_z4"

    def test_chain_parses_to_provider_names(self, monkeypatch) -> None:
        monkeypatch.setattr(
            config.settings, "EMBED_PROVIDER_CHAIN", "ollama_z4, ollama_koala ,ollama"
        )
        assert router._embed_chain() == ["ollama_z4", "ollama_koala", "ollama"]

    def test_empty_chain_falls_back_to_defaults(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "EMBED_PROVIDER_CHAIN", "")
        assert router._embed_chain() == [
            "ollama_z4", "ollama_koala", "ollama_z400", "ollama"
        ]

    def test_gridz4_ollama_is_distinct_from_gridz4_llamacpp(self) -> None:
        """ollama_z4 (:11434) and llamacpp_z4 (:8080) are different daemons."""
        assert config.Settings.model_fields["OLLAMA_Z4_BASE_URL"].default.endswith(
            ":11434"
        )
        assert config.Settings.model_fields["LLAMACPP_Z4_BASE_URL"].default.endswith(
            ":8080"
        )

    def test_chain_contains_no_retired_cpu_providers(self) -> None:
        """The :8080 providers must never reappear in the embed chain."""
        default = config.Settings.model_fields["EMBED_PROVIDER_CHAIN"].default
        assert "gemma" not in default
        assert "llamacpp" not in default


# ---------------------------------------------------------------------------
# embed() routing behaviour
# ---------------------------------------------------------------------------

class TestEmbed:
    def test_first_healthy_provider_wins(self, monkeypatch) -> None:
        z4 = _FakeEmbedProvider(vectors=[[0.1, 0.2]])
        koala = _FakeEmbedProvider(vectors=[[9.9, 9.9]])
        _install(monkeypatch, {"ollama_z4": z4, "ollama_koala": koala})

        assert router.embed(["hello"]) == [[0.1, 0.2]]
        assert z4.calls == [["hello"]]
        assert koala.calls == []  # never reached

    def test_falls_through_to_z400_when_koala_offline(self, monkeypatch) -> None:
        koala = _FakeEmbedProvider(available=False)
        z400 = _FakeEmbedProvider(vectors=[[0.5]])
        _install(monkeypatch, {"ollama_koala": koala, "ollama_z400": z400})

        assert router.embed(["hi"]) == [[0.5]]
        assert koala.calls == []
        assert z400.calls == [["hi"]]

    def test_falls_through_when_provider_returns_none(self, monkeypatch) -> None:
        koala = _FakeEmbedProvider(vectors=None)
        z400 = _FakeEmbedProvider(vectors=[[1.0]])
        _install(monkeypatch, {"ollama_koala": koala, "ollama_z400": z400})

        assert router.embed(["x"]) == [[1.0]]

    def test_provider_exception_does_not_sink_the_chain(self, monkeypatch) -> None:
        koala = _FakeEmbedProvider(raises=True)
        z400 = _FakeEmbedProvider(vectors=[[2.0]])
        _install(monkeypatch, {"ollama_koala": koala, "ollama_z400": z400})

        assert router.embed(["x"]) == [[2.0]]

    def test_returns_none_when_no_provider_answers(self, monkeypatch) -> None:
        """Graceful degradation — the whole point of the hand-off."""
        _install(monkeypatch, {})
        assert router.embed(["nobody home"]) is None

    def test_returns_none_without_raising_when_all_offline(self, monkeypatch) -> None:
        _install(
            monkeypatch,
            {
                "ollama_koala": _FakeEmbedProvider(available=False),
                "ollama_z400": _FakeEmbedProvider(available=False),
                "ollama": _FakeEmbedProvider(available=False),
            },
        )
        assert router.embed(["a", "b"]) is None

    def test_empty_input_short_circuits(self, monkeypatch) -> None:
        koala = _FakeEmbedProvider(vectors=[[1.0]])
        _install(monkeypatch, {"ollama_koala": koala})

        assert router.embed([]) == []
        assert koala.calls == []

    def test_provider_in_backoff_is_skipped(self, monkeypatch) -> None:
        koala = _FakeEmbedProvider(vectors=[[1.0]])
        z400 = _FakeEmbedProvider(vectors=[[3.0]])
        _install(monkeypatch, {"ollama_koala": koala, "ollama_z400": z400})
        router._mark_provider_unavailable("ollama_koala", "test", 600.0)

        assert router.embed(["x"]) == [[3.0]]
        assert koala.calls == []

    def test_model_override_is_forwarded(self, monkeypatch) -> None:
        koala = MagicMock()
        koala.is_available = True
        koala.embed.return_value = [[1.0]]
        _install(monkeypatch, {"ollama_koala": koala})

        router.embed(["x"], model="custom-embed")
        koala.embed.assert_called_once_with(["x"], model="custom-embed")

    def test_batch_of_texts_returns_batch_of_vectors(self, monkeypatch) -> None:
        koala = _FakeEmbedProvider(vectors=[[0.1], [0.2], [0.3]])
        _install(monkeypatch, {"ollama_koala": koala})

        assert router.embed(["a", "b", "c"]) == [[0.1], [0.2], [0.3]]
        assert koala.calls == [["a", "b", "c"]]


# ---------------------------------------------------------------------------
# Fallback chains — the retired :8080 providers must be gone
# ---------------------------------------------------------------------------

class TestFallbackChains:
    @pytest.mark.parametrize(
        "tier", [router.Tier.LOCAL, router.Tier.REASON, router.Tier.ORACLE, router.Tier.BATCH]
    )
    def test_gemma_is_absent_from_every_chain(self, tier) -> None:
        """`gemma` only ever pointed at GEMMA_BASE_URL=localhost:8080."""
        assert "gemma" not in router._fallback_chain(tier, "none")

    @pytest.mark.parametrize(
        "tier", [router.Tier.LOCAL, router.Tier.REASON, router.Tier.ORACLE, router.Tier.BATCH]
    )
    def test_gemini_precedes_openrouter(self, tier) -> None:
        chain = router._fallback_chain(tier, "none")
        assert "gemini" in chain
        assert chain.index("gemini") < chain.index("openrouter")

    def test_healthy_gpu_providers_are_retained(self) -> None:
        """`llamacpp` is the :8081 shim to the RTX 3090 — not the :8080 unit."""
        reason = router._fallback_chain(router.Tier.REASON, "none")
        for keeper in ("llamacpp_quick", "llamacpp_z4", "llamacpp", "ollama_koala"):
            assert keeper in reason
        oracle = router._fallback_chain(router.Tier.ORACLE, "none")
        assert "llamacpp_oracle" in oracle

    def test_requested_provider_is_excluded_from_its_own_fallbacks(self) -> None:
        chain = router._fallback_chain(router.Tier.REASON, "llamacpp_z4")
        assert "llamacpp_z4" not in chain


# ---------------------------------------------------------------------------
# Gemini provider — paid-gated, mocked, never hits the network
# ---------------------------------------------------------------------------

class TestGeminiGating:
    def test_gemini_is_a_paid_provider(self) -> None:
        assert "gemini" in router._PAID_PROVIDERS

    def test_blocked_when_paid_llm_disabled(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        monkeypatch.setattr(config.settings, "GEMINI_API_KEY", "test-key")
        assert router._create_client("gemini") is None

    def test_created_when_paid_llm_enabled(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
        monkeypatch.setattr(config.settings, "GEMINI_API_KEY", "test-key")
        client = router._create_client("gemini")
        assert isinstance(client, router.GeminiClient)
        assert client.is_available is True

    def test_unavailable_without_api_key(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
        monkeypatch.setattr(config.settings, "GEMINI_API_KEY", "")
        assert router._create_client("gemini") is None


class TestGeminiClient:
    def _client(self) -> router.GeminiClient:
        return router.GeminiClient(api_key="test-key", model="test-model")

    def test_payload_translation_roles_and_system(self) -> None:
        contents, system = router.GeminiClient._to_gemini_payload(
            [
                {"role": "system", "content": "be terse"},
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "hi"},
            ]
        )
        assert system == "be terse"
        assert contents == [
            {"role": "user", "parts": [{"text": "hello"}]},
            # Gemini spells the assistant role "model"
            {"role": "model", "parts": [{"text": "hi"}]},
        ]

    def test_chat_returns_text(self, monkeypatch) -> None:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "candidates": [{"content": {"parts": [{"text": "the answer"}]}}],
            "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 2},
        }
        post = MagicMock(return_value=resp)
        monkeypatch.setattr(router.requests, "post", post)

        out = self._client().chat([{"role": "user", "content": "q"}])
        assert out == "the answer"

        # Key must travel as a header, never in the URL (proxy/access logs).
        _, kwargs = post.call_args
        assert kwargs["headers"]["x-goog-api-key"] == "test-key"
        assert "test-key" not in post.call_args[0][0]

    def test_chat_sends_system_instruction(self, monkeypatch) -> None:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "candidates": [{"content": {"parts": [{"text": "ok"}]}}]
        }
        post = MagicMock(return_value=resp)
        monkeypatch.setattr(router.requests, "post", post)

        self._client().chat(
            [
                {"role": "system", "content": "rules"},
                {"role": "user", "content": "q"},
            ]
        )
        payload = post.call_args[1]["json"]
        assert payload["systemInstruction"] == {"parts": [{"text": "rules"}]}

    def test_http_error_returns_none(self, monkeypatch) -> None:
        resp = MagicMock()
        resp.status_code = 500
        resp.text = "server error"
        monkeypatch.setattr(router.requests, "post", MagicMock(return_value=resp))

        assert self._client().chat([{"role": "user", "content": "q"}]) is None

    def test_auth_error_marks_provider_unavailable(self, monkeypatch) -> None:
        resp = MagicMock()
        resp.status_code = 403
        resp.text = "forbidden"
        monkeypatch.setattr(router.requests, "post", MagicMock(return_value=resp))

        client = self._client()
        assert client.chat([{"role": "user", "content": "q"}]) is None
        assert client.is_available is False
        assert router._provider_in_backoff("gemini") is True

    def test_safety_block_with_no_candidates_returns_none(self, monkeypatch) -> None:
        """A blocked prompt is a 200 with no candidates — a miss, not a crash."""
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"promptFeedback": {"blockReason": "SAFETY"}}
        monkeypatch.setattr(router.requests, "post", MagicMock(return_value=resp))

        assert self._client().chat([{"role": "user", "content": "q"}]) is None

    def test_network_exception_returns_none(self, monkeypatch) -> None:
        monkeypatch.setattr(
            router.requests, "post", MagicMock(side_effect=OSError("no route"))
        )
        assert self._client().chat([{"role": "user", "content": "q"}]) is None

    def test_chat_without_key_short_circuits(self, monkeypatch) -> None:
        post = MagicMock()
        monkeypatch.setattr(router.requests, "post", post)

        client = router.GeminiClient(api_key="")
        assert client.chat([{"role": "user", "content": "q"}]) is None
        post.assert_not_called()

    def test_embed_is_not_wired_to_the_paid_api(self) -> None:
        """Embeddings stay free on tailnet GPUs — never billed per call."""
        assert self._client().embed(["x"]) is None

    def test_generate_wraps_chat(self, monkeypatch) -> None:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "candidates": [{"content": {"parts": [{"text": "gen"}]}}]
        }
        post = MagicMock(return_value=resp)
        monkeypatch.setattr(router.requests, "post", post)

        assert self._client().generate("prompt", system="sys") == "gen"
        payload = post.call_args[1]["json"]
        assert payload["systemInstruction"] == {"parts": [{"text": "sys"}]}
        assert payload["contents"][0]["parts"][0]["text"] == "prompt"

    def test_health_check_shape(self) -> None:
        hc = self._client().health_check()
        assert hc["provider"] == "gemini"
        assert hc["available"] is True
