"""LLM routing hygiene after the 2026-11-05 fleet audit (wave plan slice S2).

Facts behind these tests (read-only probes from grid-svr, 2026-11-05):

* koala, z400, ocr-node, panda and the redbox :8080 default do not answer;
  koala and ocr-node cost 5 s timeouts per call.
* grid-svr's RTX 3090 serves Qwen3.8-27B on 127.0.0.1:18081 (alias "oracle");
  the old :8081 shim is gone. It is the only GPU independent of gridz4.
* i7gen10 (RTX 2060) runs Ollama on 100.98.109.92:11434 and becomes the
  second embedding node.

Nothing here touches the network: providers are fakes, and the two client
tests stub ``requests.get`` / ``requests.post``.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import config
from llm import router

ALL_TIERS = [router.Tier.LOCAL, router.Tier.REASON, router.Tier.ORACLE, router.Tier.BATCH]
DEAD = ("ollama_koala", "ollama_ocr", "ollama_z400", "ollama_panda", "llamacpp_quick")


@pytest.fixture(autouse=True)
def _clean_router(monkeypatch):
    monkeypatch.setattr(router, "_client_cache", {})
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_UNTIL", {})
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_REASONS", {})
    monkeypatch.setattr(router, "_UNREACHABLE_WARNED", set())
    monkeypatch.setattr(router, "_FALLBACK_WARNED", set())
    yield


class _Fake:
    """Stand-in client; counts how often the router built it."""

    def __init__(self, available: bool, vectors: list[list[float]] | None = None) -> None:
        self.is_available = available
        self._vectors = vectors
        self.embed_calls = 0

    def embed(self, texts: list[str], model: str | None = None) -> list[list[float]] | None:
        self.embed_calls += 1
        return self._vectors


def _counting_factory(monkeypatch, providers: dict[str, Any]) -> dict[str, int]:
    """Route ``_create_client`` to ``providers`` and count constructions."""
    built: dict[str, int] = {}

    def create(name: str) -> Any:
        built[name] = built.get(name, 0) + 1
        spec = providers.get(name)
        return spec() if callable(spec) else spec

    monkeypatch.setattr(router, "_create_client", create)
    return built


# ---------------------------------------------------------------------------
# Chain composition
# ---------------------------------------------------------------------------

class TestChainComposition:
    @pytest.mark.parametrize("tier", ALL_TIERS)
    def test_dead_providers_absent_from_every_chain(self, tier) -> None:
        chain = router._fallback_chain(tier, "none")
        for dead in DEAD:
            assert dead not in chain, f"{dead} is back in the {tier} chain"
        assert not set(chain) & router._RETIRED_PROVIDERS

    def test_dead_providers_absent_from_embed_defaults(self, monkeypatch) -> None:
        default = config.Settings.model_fields["EMBED_PROVIDER_CHAIN"].default
        monkeypatch.setattr(config.settings, "EMBED_PROVIDER_CHAIN", "")
        for chain in (default.split(","), router._embed_chain()):
            assert not set(chain) & router._RETIRED_PROVIDERS

    def test_dead_nodes_disabled_by_default(self) -> None:
        fields = config.Settings.model_fields
        for flag in ("OLLAMA_KOALA_ENABLED", "OLLAMA_OCR_ENABLED", "OLLAMA_Z400_ENABLED",
                     "OLLAMA_PANDA_ENABLED", "LLAMACPP_QUICK_ENABLED"):
            assert fields[flag].default is False, flag
        assert fields["LLAMACPP_QUICK_BASE_URL"].default == ""

    def test_no_default_points_at_the_gone_8081_shim(self) -> None:
        fields = config.Settings.model_fields
        for name in ("LLAMACPP_BASE_URL", "LLAMACPP_ORACLE_BASE_URL"):
            assert ":8081" not in fields[name].default, name

    def test_quick_factory_without_host_opens_no_socket(self, monkeypatch) -> None:
        def boom(*_a, **_k):
            raise AssertionError("no network call expected")

        monkeypatch.setattr("llamacpp.client.requests.get", boom)
        settings = type("S", (), {"LLAMACPP_QUICK_ENABLED": True, "LLAMACPP_QUICK_BASE_URL": ""})()
        assert router._create_llamacpp_quick_client(settings) is None

    @pytest.mark.parametrize("field", ["LLM_LOCAL_PROVIDER", "LLM_REASON_PROVIDER",
                                       "LLM_ORACLE_PROVIDER"])
    def test_every_tier_defaults_to_gridz4(self, field) -> None:
        assert config.Settings.model_fields[field].default == "llamacpp_z4"


# ---------------------------------------------------------------------------
# The RTX 3090 (llamacpp_oracle) as independent fallback
# ---------------------------------------------------------------------------

class TestRtx3090Fallback:
    def test_oracle_default_is_the_3090_on_18081(self) -> None:
        fields = config.Settings.model_fields
        assert fields["LLAMACPP_ORACLE_BASE_URL"].default == "http://127.0.0.1:18081"
        assert fields["LLAMACPP_ORACLE_CHAT_MODEL"].default == "oracle"

    def test_oracle_chain_is_gridz4_then_3090(self) -> None:
        chain = router._fallback_chain(router.Tier.ORACLE, "none")
        assert chain[:2] == ["llamacpp_z4", "llamacpp_oracle"]

    def test_reason_chain_has_3090_second(self) -> None:
        chain = router._fallback_chain(router.Tier.REASON, "none")
        assert chain[:2] == ["llamacpp_z4", "llamacpp_oracle"]

    def test_3090_precedes_every_paid_provider(self) -> None:
        for tier in (router.Tier.ORACLE, router.Tier.REASON):
            chain = router._fallback_chain(tier, "none")
            for paid in ("gemini", "openrouter", "openai"):
                assert chain.index("llamacpp_oracle") < chain.index(paid)

    @pytest.mark.parametrize("tier", [router.Tier.ORACLE, router.Tier.REASON])
    def test_gridz4_down_routes_to_the_3090(self, monkeypatch, tier) -> None:
        """gridz4 (z4 + its :8086 proxy) dark -> the 3090 answers."""
        monkeypatch.setattr(config.settings, "LLM_ORACLE_PROVIDER", "llamacpp_z4")
        monkeypatch.setattr(config.settings, "LLM_REASON_PROVIDER", "llamacpp_z4")
        rtx = _Fake(True)
        _counting_factory(monkeypatch, {
            "llamacpp_z4": lambda: _Fake(False),
            "llamacpp": lambda: _Fake(False),
            "llamacpp_oracle": rtx,
        })
        assert router.get_llm(tier) is rtx

    def test_oracle_factory_targets_18081(self, monkeypatch) -> None:
        seen: list[tuple[str, Any]] = []

        class Ok:
            status_code = 200

            def json(self) -> dict:
                return {"default_generation_settings": {"n_ctx": 8192}}

        def get(url, timeout=None, **_k):
            seen.append((url, timeout))
            return Ok()

        monkeypatch.setattr("llamacpp.client.requests.get", get)
        monkeypatch.setattr("llamacpp.client._PROBE_FAILED_UNTIL", {})
        monkeypatch.setattr("llamacpp.client._ENDPOINT_BACKOFF_UNTIL", {})
        settings = type("S", (), {"LLAMACPP_ORACLE_ENABLED": True,
                                  "LLM_CONNECT_TIMEOUT_SECONDS": 2.0})()
        client = router._create_llamacpp_oracle_client(settings)
        assert client.base_url == "http://127.0.0.1:18081"
        assert client.model == "oracle"
        assert seen[0][0] == "http://127.0.0.1:18081/health"
        assert seen[0][1][0] == 2.0  # (connect, read)


# ---------------------------------------------------------------------------
# Embedding chain order
# ---------------------------------------------------------------------------

class TestEmbedChainOrder:
    def test_default_order_is_gridz4_i7gen10_grid_svr(self, monkeypatch) -> None:
        assert config.Settings.model_fields["EMBED_PROVIDER_CHAIN"].default == (
            "ollama_z4,ollama_i7gen10,ollama"
        )
        monkeypatch.setattr(config.settings, "EMBED_PROVIDER_CHAIN", "")
        assert router._embed_chain() == ["ollama_z4", "ollama_i7gen10", "ollama"]

    def test_i7gen10_defaults(self) -> None:
        fields = config.Settings.model_fields
        assert fields["OLLAMA_I7GEN10_BASE_URL"].default == "http://100.98.109.92:11434"
        assert fields["OLLAMA_I7GEN10_ENABLED"].default is True
        assert fields["OLLAMA_I7GEN10_EMBED_MODEL"].default == "nomic-embed-text"

    def test_router_dispatches_i7gen10(self, monkeypatch) -> None:
        monkeypatch.setattr(router, "_create_ollama_i7gen10_client", lambda _s: "sentinel")
        assert router._create_client("ollama_i7gen10") == "sentinel"

    def test_i7gen10_factory_builds_ollama_client(self, monkeypatch) -> None:
        class Ok:
            status_code = 200

        timeouts: list[Any] = []
        monkeypatch.setattr(
            "ollama.client.requests.get",
            lambda url, timeout=None, **_k: timeouts.append(timeout) or Ok(),
        )
        settings = type("S", (), {"OLLAMA_I7GEN10_ENABLED": True,
                                  "OLLAMA_I7GEN10_BASE_URL": "http://100.98.109.92:11434",
                                  "LLM_CONNECT_TIMEOUT_SECONDS": 1.5})()
        client = router._create_ollama_i7gen10_client(settings)
        assert client.base_url == "http://100.98.109.92:11434"
        assert client.embed_model == "nomic-embed-text"
        assert client.connect_timeout == 1.5
        assert timeouts == [(1.5, 5)]

    def test_i7gen10_answers_when_gridz4_is_down(self, monkeypatch) -> None:
        svr = _Fake(True, vectors=[[9.0]])
        i7 = _Fake(True, vectors=[[2.0]])
        _counting_factory(monkeypatch, {"ollama_z4": lambda: _Fake(False),
                                        "ollama_i7gen10": i7, "ollama": svr})
        assert router.embed(["x"]) == [[2.0]]
        assert svr.embed_calls == 0  # grid-svr (shares the 3090) not touched


# ---------------------------------------------------------------------------
# Fail-fast cooldown
# ---------------------------------------------------------------------------

class TestFailFastCooldown:
    def test_refused_provider_is_not_reprobed_inside_cooldown(self, monkeypatch) -> None:
        """A dead node is probed once, then skipped — not 5 s on every call."""
        monkeypatch.setattr(config.settings, "LLM_PROVIDER_COOLDOWN_SECONDS", 120.0)
        monkeypatch.setattr(config.settings, "LLM_REASON_PROVIDER", "llamacpp_z4")
        ok = _Fake(True)
        built = _counting_factory(monkeypatch, {
            "llamacpp_z4": lambda: _Fake(False),
            "llamacpp_oracle": ok,
        })
        for _ in range(5):
            assert router.get_llm(router.Tier.REASON) is ok
        assert built["llamacpp_z4"] == 1
        assert router._provider_in_backoff("llamacpp_z4")

    def test_cooldown_expires_and_provider_is_retried(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "LLM_PROVIDER_COOLDOWN_SECONDS", 60.0)
        monkeypatch.setattr(config.settings, "LLM_REASON_PROVIDER", "llamacpp_z4")
        now = {"t": 1_000.0}
        monkeypatch.setattr(router, "time", SimpleNamespace(time=lambda: now["t"],
                                                            monotonic=lambda: now["t"]))
        state = {"up": False}
        built = _counting_factory(monkeypatch, {
            "llamacpp_z4": lambda: _Fake(state["up"]),
            "llamacpp_oracle": _Fake(True),
        })
        router.get_llm(router.Tier.REASON)
        now["t"] += 30
        router.get_llm(router.Tier.REASON)
        assert built["llamacpp_z4"] == 1  # still cooling down
        state["up"] = True
        now["t"] += 31
        client = router.get_llm(router.Tier.REASON)
        assert built["llamacpp_z4"] == 2 and client.is_available
        assert "llamacpp_z4" not in router._UNREACHABLE_WARNED

    def test_disabled_provider_is_not_cooled_down(self, monkeypatch) -> None:
        """A factory returning None opened no socket — nothing to cool down."""
        _counting_factory(monkeypatch, {})
        router.get_llm(router.Tier.LOCAL)
        assert router._PROVIDER_BACKOFF_UNTIL == {}

    def test_refusal_warns_once_per_outage(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "LLM_PROVIDER_COOLDOWN_SECONDS", 120.0)
        warnings: list[str] = []
        monkeypatch.setattr(router.log, "warning",
                            lambda template, **k: warnings.append(template.format(**k)))
        router._mark_provider_refused("ollama_z4", "health probe failed")
        router._PROVIDER_BACKOFF_UNTIL.clear()
        router._mark_provider_refused("ollama_z4", "health probe failed")
        assert len([w for w in warnings if "ollama_z4" in w]) == 1

    def test_zero_cooldown_disables_the_policy(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "LLM_PROVIDER_COOLDOWN_SECONDS", 0.0)
        router._mark_provider_refused("ollama_z4", "x")
        assert not router._provider_in_backoff("ollama_z4")

    def test_embed_miss_cools_the_node_down(self, monkeypatch) -> None:
        """Up but without the embed model (i7gen10 before the pull) -> skip it."""
        monkeypatch.setattr(config.settings, "LLM_PROVIDER_COOLDOWN_SECONDS", 120.0)
        i7 = _Fake(True, vectors=None)
        svr = _Fake(True, vectors=[[1.0]])
        built = _counting_factory(monkeypatch, {"ollama_i7gen10": lambda: i7, "ollama": svr})
        assert router.embed(["a"]) == [[1.0]]
        assert router.embed(["b"]) == [[1.0]]
        assert built["ollama_i7gen10"] == 1
        assert i7.embed_calls == 1

    @pytest.mark.parametrize("raw,expected", [
        (2.0, 2.0), (0.5, 0.5), (0, 2.0), (-1, 2.0), (float("nan"), 2.0),
        ("junk", 2.0), (999, 30.0),
    ])
    def test_connect_timeout_is_clamped(self, raw, expected) -> None:
        settings = type("S", (), {"LLM_CONNECT_TIMEOUT_SECONDS": raw})()
        assert router._connect_timeout(settings) == expected

    @pytest.mark.parametrize("raw,expected", [
        (120.0, 120.0), (0, 0.0), (-5, 120.0), (float("nan"), 120.0), (99_999, 3600.0),
    ])
    def test_cooldown_is_clamped(self, monkeypatch, raw, expected) -> None:
        monkeypatch.setattr(config.settings, "LLM_PROVIDER_COOLDOWN_SECONDS", raw)
        assert router._provider_cooldown_s() == expected

    def test_ollama_calls_use_a_short_connect_timeout(self, monkeypatch) -> None:
        """Connect fails fast; the long read timeout only applies once connected."""
        from ollama.client import OllamaClient

        class Ok:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"embedding": [0.1]}

        gets: list[Any] = []
        posts: list[Any] = []
        monkeypatch.setattr("ollama.client.requests.get",
                            lambda url, timeout=None, **_k: gets.append(timeout) or Ok())
        monkeypatch.setattr("ollama.client.requests.post",
                            lambda url, timeout=None, **_k: posts.append(timeout) or Ok())
        client = OllamaClient(base_url="http://node:11434", timeout=120, connect_timeout=2.0)
        assert client.embed(["x"]) == [[0.1]]
        assert gets == [(2.0, 5)]
        assert posts == [(2.0, 120)]

    def test_llamacpp_probe_uses_a_short_connect_timeout(self, monkeypatch) -> None:
        from llamacpp import client as lc

        seen: list[Any] = []

        def get(url, timeout=None, **_k):
            seen.append(timeout)
            raise OSError("connection refused")

        monkeypatch.setattr(lc.requests, "get", get)
        monkeypatch.setattr(lc, "_PROBE_FAILED_UNTIL", {})
        monkeypatch.setattr(lc, "_ENDPOINT_BACKOFF_UNTIL", {})
        monkeypatch.setattr(lc, "_UNAVAILABLE_WARNED", set())
        client = lc.LlamaCppClient(base_url="http://dead:18081", connect_timeout=2.0)
        assert client.is_available is False
        assert seen == [(2.0, 5)]
