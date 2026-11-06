"""Tier.QUICK on threadripper and the x99-hex REASON/ORACLE fallback (W2-9).

Topology behind these tests (2026-11-06):

* threadripper (2x RTX A2000 12 GB, 100.78.68.72) serves a small fast model
  on :8088 (alias ``quick``) — ``llamacpp_threadripper``. It is Tier.QUICK's
  primary and LOCAL's first fallback after gridz4.
* x99-hex (site B, 100.99.56.77:8086) serves Qwen3.8-27B with gridz4's flags
  — ``llamacpp_x99``. It is independent of gridz4 and the 3090 but relayed
  from grid-svr, so it sits after the gridz4 :8086 proxy (``llamacpp``) in
  REASON/ORACLE.
* Both are off by default; the ops script turns them on in grid-svr's .env.

Nothing here touches the network: providers are fakes, and the factory tests
stub ``requests.get``.
"""
from __future__ import annotations

from typing import Any

import pytest

import config
from llm import router

PAID = ("gemini", "openrouter", "openai")


@pytest.fixture(autouse=True)
def _clean_router(monkeypatch):
    monkeypatch.setattr(router, "_client_cache", {})
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_UNTIL", {})
    monkeypatch.setattr(router, "_PROVIDER_BACKOFF_REASONS", {})
    monkeypatch.setattr(router, "_UNREACHABLE_WARNED", set())
    monkeypatch.setattr(router, "_FALLBACK_WARNED", set())
    yield


class _Fake:
    def __init__(self, available: bool) -> None:
        self.is_available = available


def _counting_factory(monkeypatch, providers: dict[str, Any]) -> dict[str, int]:
    built: dict[str, int] = {}

    def create(name: str) -> Any:
        built[name] = built.get(name, 0) + 1
        spec = providers.get(name)
        return spec() if callable(spec) else spec

    monkeypatch.setattr(router, "_create_client", create)
    return built


class _HealthOk:
    status_code = 200

    def json(self) -> dict:
        return {"default_generation_settings": {"n_ctx": 8192}}


def _stub_probe(monkeypatch) -> list[tuple[str, Any]]:
    seen: list[tuple[str, Any]] = []

    def get(url, timeout=None, **_k):
        seen.append((url, timeout))
        return _HealthOk()

    monkeypatch.setattr("llamacpp.client.requests.get", get)
    monkeypatch.setattr("llamacpp.client._PROBE_FAILED_UNTIL", {})
    monkeypatch.setattr("llamacpp.client._ENDPOINT_BACKOFF_UNTIL", {})
    return seen


def _settings(**values: Any) -> Any:
    return type("S", (), values)()


# ---------------------------------------------------------------------------
# Chain order
# ---------------------------------------------------------------------------

class TestChainOrder:
    def test_quick_chain_leads_with_threadripper(self) -> None:
        chain = router._fallback_chain(router.Tier.QUICK, "none")
        assert chain == ["llamacpp_threadripper", "llamacpp_z4", "llamacpp", "ollama", *PAID]

    def test_local_chain_has_threadripper_right_after_gridz4(self) -> None:
        chain = router._fallback_chain(router.Tier.LOCAL, "none")
        assert chain[:3] == ["llamacpp_z4", "llamacpp_threadripper", "llamacpp"]

    @pytest.mark.parametrize("tier", [router.Tier.REASON, router.Tier.ORACLE])
    def test_x99_follows_the_gridz4_proxy(self, tier) -> None:
        chain = router._fallback_chain(tier, "none")
        assert chain[:4] == ["llamacpp_z4", "llamacpp_oracle", "llamacpp", "llamacpp_x99"]

    def test_x99_and_threadripper_stay_out_of_other_tiers(self) -> None:
        for tier in (router.Tier.LOCAL, router.Tier.QUICK, router.Tier.BATCH):
            assert "llamacpp_x99" not in router._fallback_chain(tier, "none")
        for tier in (router.Tier.REASON, router.Tier.ORACLE, router.Tier.BATCH):
            assert "llamacpp_threadripper" not in router._fallback_chain(tier, "none")

    @pytest.mark.parametrize("tier", [router.Tier.QUICK, router.Tier.LOCAL,
                                      router.Tier.REASON, router.Tier.ORACLE])
    def test_new_nodes_precede_every_paid_provider(self, tier) -> None:
        chain = router._fallback_chain(tier, "none")
        for node in ("llamacpp_threadripper", "llamacpp_x99"):
            if node in chain:
                assert all(chain.index(node) < chain.index(p) for p in PAID)

    def test_new_nodes_are_not_retired(self) -> None:
        assert not {"llamacpp_threadripper", "llamacpp_x99"} & router._RETIRED_PROVIDERS

    def test_quick_tier_primary_defaults_to_threadripper(self) -> None:
        fields = config.Settings.model_fields
        assert fields["LLM_QUICK_TIER_PROVIDER"].default == "llamacpp_threadripper"
        # The legacy LOCAL fallback key keeps its meaning.
        assert fields["LLM_QUICK_PROVIDER"].default == "llamacpp"


# ---------------------------------------------------------------------------
# Fallback behaviour through get_llm()
# ---------------------------------------------------------------------------

class TestFallback:
    def test_quick_uses_threadripper_when_it_answers(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "LLM_QUICK_TIER_PROVIDER", "llamacpp_threadripper")
        tr = _Fake(True)
        built = _counting_factory(monkeypatch, {"llamacpp_threadripper": tr,
                                                "llamacpp_z4": _Fake(True)})
        assert router.get_llm(router.Tier.QUICK) is tr
        assert "llamacpp_z4" not in built

    def test_quick_falls_back_to_gridz4_when_threadripper_is_dark(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "LLM_QUICK_TIER_PROVIDER", "llamacpp_threadripper")
        monkeypatch.setattr(config.settings, "LLM_PROVIDER_COOLDOWN_SECONDS", 120.0)
        z4 = _Fake(True)
        built = _counting_factory(monkeypatch, {
            "llamacpp_threadripper": lambda: _Fake(False), "llamacpp_z4": z4,
        })
        for _ in range(4):
            assert router.get_llm(router.Tier.QUICK) is z4
        # fail-fast: probed once, then skipped for the cooldown
        assert built["llamacpp_threadripper"] == 1
        assert router._provider_in_backoff("llamacpp_threadripper")

    def test_quick_disabled_threadripper_opens_no_cooldown(self, monkeypatch) -> None:
        """Flag off -> factory returns None -> next node, nothing cooled down."""
        monkeypatch.setattr(config.settings, "LLM_QUICK_TIER_PROVIDER", "llamacpp_threadripper")
        z4 = _Fake(True)
        _counting_factory(monkeypatch, {"llamacpp_threadripper": None, "llamacpp_z4": z4})
        assert router.get_llm(router.Tier.QUICK) is z4
        assert router._PROVIDER_BACKOFF_UNTIL == {}

    def test_local_reaches_threadripper_when_gridz4_is_down(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "LLM_LOCAL_PROVIDER", "llamacpp_quick")
        tr = _Fake(True)
        proxy = _Fake(True)
        _counting_factory(monkeypatch, {
            "llamacpp_quick": lambda: _Fake(False),  # gridz4:8080 in the live .env
            "llamacpp_z4": None,                      # LLAMACPP_Z4_ENABLED=false live
            "llamacpp_threadripper": tr,
            "llamacpp": proxy,
        })
        assert router.get_llm(router.Tier.LOCAL) is tr

    @pytest.mark.parametrize("tier", [router.Tier.REASON, router.Tier.ORACLE])
    def test_x99_answers_when_gridz4_and_3090_are_dark(self, monkeypatch, tier) -> None:
        monkeypatch.setattr(config.settings, "LLM_REASON_PROVIDER", "llamacpp_z4")
        monkeypatch.setattr(config.settings, "LLM_ORACLE_PROVIDER", "llamacpp_z4")
        x99 = _Fake(True)
        _counting_factory(monkeypatch, {
            "llamacpp_z4": lambda: _Fake(False),
            "llamacpp_oracle": lambda: _Fake(False),
            "llamacpp": lambda: _Fake(False),
            "llamacpp_x99": x99,
        })
        assert router.get_llm(tier) is x99

    @pytest.mark.parametrize("tier", [router.Tier.REASON, router.Tier.ORACLE])
    def test_gridz4_proxy_beats_x99(self, monkeypatch, tier) -> None:
        monkeypatch.setattr(config.settings, "LLM_REASON_PROVIDER", "llamacpp_z4")
        monkeypatch.setattr(config.settings, "LLM_ORACLE_PROVIDER", "llamacpp_z4")
        proxy = _Fake(True)
        built = _counting_factory(monkeypatch, {
            "llamacpp_z4": None, "llamacpp_oracle": lambda: _Fake(False),
            "llamacpp": proxy, "llamacpp_x99": _Fake(True),
        })
        assert router.get_llm(tier) is proxy
        assert "llamacpp_x99" not in built

    def test_dark_x99_is_cooled_down_not_reprobed(self, monkeypatch) -> None:
        monkeypatch.setattr(config.settings, "LLM_REASON_PROVIDER", "llamacpp_z4")
        monkeypatch.setattr(config.settings, "LLM_PROVIDER_COOLDOWN_SECONDS", 120.0)
        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        built = _counting_factory(monkeypatch, {
            "llamacpp_z4": None, "llamacpp_oracle": None, "llamacpp": None,
            "llamacpp_x99": lambda: _Fake(False), "ollama": _Fake(True),
        })
        for _ in range(3):
            router.get_llm(router.Tier.REASON)
        assert built["llamacpp_x99"] == 1


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

class TestFactories:
    def test_router_dispatches_new_providers(self, monkeypatch) -> None:
        monkeypatch.setattr(router, "_create_llamacpp_threadripper_client", lambda _s: "tr")
        monkeypatch.setattr(router, "_create_llamacpp_x99_client", lambda _s: "x99")
        assert router._create_client("llamacpp_threadripper") == "tr"
        assert router._create_client("llamacpp_x99") == "x99"

    def test_both_off_by_default_with_tailnet_urls(self) -> None:
        f = config.Settings.model_fields
        assert f["LLAMACPP_THREADRIPPER_ENABLED"].default is False
        assert f["LLAMACPP_X99_ENABLED"].default is False
        assert f["LLAMACPP_THREADRIPPER_BASE_URL"].default == "http://100.78.68.72:8088"
        assert f["LLAMACPP_X99_BASE_URL"].default == "http://100.99.56.77:8086"
        assert f["LLAMACPP_X99_CHAT_MODEL"].default == "Qwen3.8-27B-Q4_K_M"
        # relayed node: longer request timeout than gridz4's 180 s
        assert f["LLAMACPP_X99_TIMEOUT_SECONDS"].default > f["LLAMACPP_Z4_TIMEOUT_SECONDS"].default

    @pytest.mark.parametrize("factory,prefix", [
        ("_create_llamacpp_threadripper_client", "LLAMACPP_THREADRIPPER"),
        ("_create_llamacpp_x99_client", "LLAMACPP_X99"),
    ])
    def test_disabled_or_blank_url_opens_no_socket(self, monkeypatch, factory, prefix) -> None:
        def boom(*_a, **_k):
            raise AssertionError("no network call expected")

        monkeypatch.setattr("llamacpp.client.requests.get", boom)
        make = getattr(router, factory)
        assert make(_settings(**{f"{prefix}_ENABLED": False,
                                 f"{prefix}_BASE_URL": "http://x:1"})) is None
        assert make(_settings(**{f"{prefix}_ENABLED": True,
                                 f"{prefix}_BASE_URL": "  "})) is None

    def test_threadripper_factory_is_bounded_and_uses_its_connect_timeout(self, monkeypatch) -> None:
        seen = _stub_probe(monkeypatch)
        client = router._create_llamacpp_threadripper_client(_settings(
            LLAMACPP_THREADRIPPER_ENABLED=True,
            LLAMACPP_THREADRIPPER_BASE_URL="http://100.78.68.72:8088",
            LLAMACPP_THREADRIPPER_CHAT_MODEL="quick",
            LLAMACPP_THREADRIPPER_CONNECT_TIMEOUT_SECONDS=6.0,
            LLM_CONNECT_TIMEOUT_SECONDS=2.0,
        ))
        assert client.base_url == "http://100.78.68.72:8088"
        assert client.model == "quick"
        assert client.reasoning_headroom == 0
        assert seen[0][0] == "http://100.78.68.72:8088/health"
        assert seen[0][1][0] == 6.0

    def test_x99_factory_targets_8086_with_long_timeouts(self, monkeypatch) -> None:
        seen = _stub_probe(monkeypatch)
        client = router._create_llamacpp_x99_client(_settings(
            LLAMACPP_X99_ENABLED=True,
            LLAMACPP_X99_BASE_URL="http://100.99.56.77:8086",
            LLAMACPP_X99_TIMEOUT_SECONDS=360,
            LLAMACPP_X99_CONNECT_TIMEOUT_SECONDS=6.0,
            LLM_CONNECT_TIMEOUT_SECONDS=2.0,
        ))
        assert client.base_url == "http://100.99.56.77:8086"
        assert client.model == "Qwen3.8-27B-Q4_K_M"
        assert client.timeout == 360
        assert client.reasoning_headroom == 0
        assert seen[0] == ("http://100.99.56.77:8086/health", (6.0, seen[0][1][1]))


class TestPerNodeConnectTimeout:
    @pytest.mark.parametrize("raw,expected", [
        (6.0, 6.0), ("4.5", 4.5), (None, 2.0), ("", 2.0), (0, 2.0), (-3, 2.0),
        (float("nan"), 2.0), ("junk", 2.0), (999, 30.0),
    ])
    def test_override_falls_back_to_global(self, raw, expected) -> None:
        s = _settings(LLM_CONNECT_TIMEOUT_SECONDS=2.0, NODE_CT=raw)
        assert router._connect_timeout(s, "NODE_CT") == expected

    def test_missing_override_uses_global(self) -> None:
        s = _settings(LLM_CONNECT_TIMEOUT_SECONDS=1.5)
        assert router._connect_timeout(s, "LLAMACPP_X99_CONNECT_TIMEOUT_SECONDS") == 1.5
