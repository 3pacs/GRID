"""Tests for the Hermes analyst bridge (intelligence/hermes/).

Covers prompts, the spend ledger + daily cap, config resolution, the OpenAI
provider (reasoning-token accounting, cap enforcement, unsupported-param
retry), the agent's local fallback, and the CLI ping. External calls are
mocked — no network is touched.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from intelligence.hermes import (
    HermesAgent,
    HermesConfig,
    HermesProvider,
    SpendLedger,
    TokenUsage,
    build_messages,
)
from intelligence.hermes import config as hconfig
from intelligence.hermes.prompts import SYSTEM, SYSTEM_VERSION

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _config(**overrides) -> HermesConfig:
    base = dict(
        enabled=True, api_key="sk-test", base_url="https://api.openai.com/v1",
        model="gpt-4o", timeout_seconds=30, max_completion_tokens=256,
        temperature=None, reasoning_effort=None, daily_spend_cap_usd=0.0,
        ledger_path="", price_input_per_mtok=2.5, price_output_per_mtok=10.0,
        fallback_tier="reason",
    )
    base.update(overrides)
    return HermesConfig(**base)


def _fake_openai_response(content="pong", *, prompt=10, completion=50, reasoning=30, total=60, model="gpt-4o"):
    details = SimpleNamespace(reasoning_tokens=reasoning)
    usage = SimpleNamespace(
        prompt_tokens=prompt, completion_tokens=completion,
        total_tokens=total, completion_tokens_details=details,
    )
    message = SimpleNamespace(content=content)
    choice = SimpleNamespace(message=message)
    return SimpleNamespace(choices=[choice], usage=usage, model=model)


class _FakeCompletions:
    def __init__(self, response=None, error_on_temperature=False):
        self._response = response or _fake_openai_response()
        self._error_on_temperature = error_on_temperature
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if self._error_on_temperature and "temperature" in kwargs:
            raise RuntimeError("Unsupported value: 'temperature' is not supported with this model")
        return self._response


class _FakeClient:
    def __init__(self, completions):
        self.chat = SimpleNamespace(completions=completions)


def _provider_with(completions, **cfg_over) -> HermesProvider:
    provider = HermesProvider(_config(**cfg_over), ledger=SpendLedger(None))
    provider._client = lambda: _FakeClient(completions)  # type: ignore[assignment]
    return provider


# --------------------------------------------------------------------------- #
# prompts
# --------------------------------------------------------------------------- #
def test_system_prompt_is_versioned_and_encodes_sop():
    assert SYSTEM.strip()
    assert "grid-analyst-v1" in SYSTEM_VERSION
    assert "LEVER" in SYSTEM and "CONDITION" in SYSTEM


def test_build_messages_orders_system_context_user():
    msgs = build_messages("question?", context="ctx data")
    assert [m["role"] for m in msgs] == ["system", "system", "user"]
    assert msgs[0]["content"] == SYSTEM
    assert "ctx data" in msgs[1]["content"]
    assert msgs[-1]["content"] == "question?"


# --------------------------------------------------------------------------- #
# spend ledger
# --------------------------------------------------------------------------- #
def test_spend_ledger_records_and_caps():
    led = SpendLedger(None)
    assert led.spend_today() == 0.0
    led.record(0.01)
    led.record(0.02)
    assert led.spend_today() == pytest.approx(0.03)
    assert led.would_exceed(0.03) is True
    assert led.would_exceed(0.05) is False
    assert led.would_exceed(0.0) is False  # 0 == no cap


def test_spend_ledger_persists_to_disk(tmp_path):
    path = tmp_path / "nested" / "ledger.json"
    SpendLedger(path).record(0.05)
    assert SpendLedger(path).spend_today() == pytest.approx(0.05)


def test_spend_ledger_survives_corrupt_file(tmp_path):
    path = tmp_path / "ledger.json"
    path.write_text("not json{{")
    led = SpendLedger(path)  # must not raise
    assert led.spend_today() == 0.0


def test_spend_ledger_rolls_over_by_day(tmp_path, monkeypatch):
    path = tmp_path / "ledger.json"
    monkeypatch.setattr("intelligence.hermes.spend._utc_today", lambda: "2026-06-05")
    SpendLedger(path).record(0.10)
    monkeypatch.setattr("intelligence.hermes.spend._utc_today", lambda: "2026-06-06")
    assert SpendLedger(path).spend_today() == 0.0  # new day starts clean


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
def test_load_config_maps_settings(monkeypatch):
    ns = SimpleNamespace(
        HERMES_ENABLED=True, HERMES_API_KEY="", OPENAI_API_KEY="sk-openai",
        HERMES_BASE_URL="", OPENAI_BASE_URL="https://x/v1", HERMES_MODEL="o4-mini",
        HERMES_TIMEOUT_SECONDS=99, HERMES_MAX_COMPLETION_TOKENS=512,
        HERMES_TEMPERATURE="", HERMES_REASONING_EFFORT="high",
        HERMES_DAILY_SPEND_CAP_USD=5.0, HERMES_LEDGER_PATH="x.json",
        HERMES_PRICE_INPUT_PER_MTOK=1.0, HERMES_PRICE_OUTPUT_PER_MTOK=3.0,
        HERMES_FALLBACK_TIER="reason",
    )
    monkeypatch.setattr(hconfig, "_settings", lambda: ns)
    cfg = hconfig.load_hermes_config()
    assert cfg.api_key == "sk-openai"        # falls back to OPENAI_API_KEY
    assert cfg.base_url == "https://x/v1"    # falls back to OPENAI_BASE_URL
    assert cfg.model == "o4-mini"
    assert cfg.temperature is None           # blank -> omitted
    assert cfg.reasoning_effort == "high"
    assert cfg.configured is True


def test_config_temperature_parsed_when_set(monkeypatch):
    ns = SimpleNamespace(HERMES_TEMPERATURE="0.2", HERMES_API_KEY="k")
    monkeypatch.setattr(hconfig, "_settings", lambda: ns)
    assert hconfig.load_hermes_config().temperature == pytest.approx(0.2)


def test_get_coerces_env_when_setting_absent(monkeypatch):
    monkeypatch.setattr(hconfig, "_settings", lambda: None)
    monkeypatch.setenv("HERMES_MADE_UP_INT", "7")
    assert hconfig._get("HERMES_MADE_UP_INT", 0) == 7
    monkeypatch.setenv("HERMES_MADE_UP_BOOL", "yes")
    assert hconfig._get("HERMES_MADE_UP_BOOL", False) is True


# --------------------------------------------------------------------------- #
# provider
# --------------------------------------------------------------------------- #
def test_provider_unavailable_without_key():
    provider = HermesProvider(_config(api_key=""), ledger=SpendLedger(None))
    assert provider.is_available is False
    assert provider.complete(build_messages("hi")) is None


def test_provider_complete_extracts_reasoning_and_records_spend():
    comp = _FakeCompletions(_fake_openai_response(content="pong", reasoning=30))
    provider = _provider_with(comp)
    resp = provider.complete(build_messages("ping"))
    assert resp is not None
    assert resp.text == "pong"
    assert resp.usage.reasoning_tokens == 30
    assert resp.usage.completion_tokens == 50
    # cost = 10/1e6*2.5 + 50/1e6*10 = 0.000525
    assert resp.cost_usd == pytest.approx(0.000525)
    assert provider.ledger.spend_today() == pytest.approx(0.000525)
    assert comp.calls[0]["max_completion_tokens"] == 256
    assert "temperature" not in comp.calls[0]  # omitted by default


def test_provider_enforces_daily_cap_without_calling():
    comp = _FakeCompletions()
    provider = _provider_with(comp, daily_spend_cap_usd=0.01)
    provider.ledger.record(0.02)  # already over cap
    assert provider.is_available is False
    assert provider.complete(build_messages("ping")) is None
    assert comp.calls == []  # never reached the API


def test_provider_retries_without_unsupported_temperature():
    comp = _FakeCompletions(error_on_temperature=True)
    provider = _provider_with(comp, temperature=0.3)
    resp = provider.complete(build_messages("ping"))
    assert resp is not None and resp.text == "pong"
    assert len(comp.calls) == 2
    assert "temperature" in comp.calls[0] and "temperature" not in comp.calls[1]


def test_provider_estimate_cost_uses_output_price():
    provider = HermesProvider(_config(), ledger=SpendLedger(None))
    cost = provider.estimate_cost(TokenUsage(prompt_tokens=1_000_000, completion_tokens=1_000_000))
    assert cost == pytest.approx(2.5 + 10.0)


# --------------------------------------------------------------------------- #
# agent
# --------------------------------------------------------------------------- #
class _StubProvider:
    def __init__(self, response):
        self._response = response

    def complete(self, *a, **k):
        return self._response


def test_agent_uses_hermes_primary():
    from intelligence.hermes.provider import HermesResponse

    resp = HermesResponse(text="ANSWER", model="gpt-4o",
                          usage=TokenUsage(reasoning_tokens=12), cost_usd=0.001, latency_ms=5.0)
    agent = HermesAgent(_config(), provider=_StubProvider(resp))
    result = agent.analyze("q")
    assert result.source == "hermes"
    assert result.text == "ANSWER"
    assert result.reasoning_tokens == 12


def test_agent_falls_back_to_local(monkeypatch):
    agent = HermesAgent(_config(api_key=""), provider=_StubProvider(None))
    fake_local = SimpleNamespace(is_available=True, model="qwen3-14b",
                                 chat=lambda messages, temperature=0.3: "LOCAL ANSWER")
    monkeypatch.setattr("llm.router.get_llm", lambda tier: fake_local)
    result = agent.analyze("q")
    assert result.source == "local"
    assert result.text == "LOCAL ANSWER"
    assert result.model == "qwen3-14b"


def test_agent_unavailable_when_both_down(monkeypatch):
    agent = HermesAgent(_config(api_key=""), provider=_StubProvider(None))
    monkeypatch.setattr("llm.router.get_llm", lambda tier: None)
    result = agent.analyze("q")
    assert result.source == "unavailable"
    assert result.ok is False


def test_agent_no_fallback_when_disallowed():
    agent = HermesAgent(_config(api_key=""), provider=_StubProvider(None))
    result = agent.analyze("q", allow_fallback=False)
    assert result.source == "unavailable"


def test_score_hypothesis_parses_json():
    from intelligence.hermes.provider import HermesResponse

    payload = '{"probability": 0.7, "direction": "up", "conviction": 0.6}'
    resp = HermesResponse(text=payload, model="gpt-4o", usage=TokenUsage(), cost_usd=0.0, latency_ms=1.0)
    agent = HermesAgent(_config(), provider=_StubProvider(resp))
    verdict = agent.score_hypothesis("BTC up because whale X")
    assert verdict["probability"] == 0.7
    assert verdict["direction"] == "up"
    assert verdict["source"] == "hermes"


def test_score_hypothesis_keeps_raw_on_bad_json():
    from intelligence.hermes.provider import HermesResponse

    resp = HermesResponse(text="not json", model="gpt-4o", usage=TokenUsage(), cost_usd=0.0, latency_ms=1.0)
    agent = HermesAgent(_config(), provider=_StubProvider(resp))
    verdict = agent.score_hypothesis("hypo")
    assert verdict["raw"] == "not json"


# --------------------------------------------------------------------------- #
# cli
# --------------------------------------------------------------------------- #
def test_cli_ping_no_call_runs(monkeypatch, capsys):
    from intelligence.hermes import cli

    monkeypatch.setattr(cli, "_fallback_available", lambda tier: True)
    rc = cli.main(["ping", "--no-call"])
    out = capsys.readouterr().out
    assert "Hermes analyst bridge" in out
    assert "status:" in out
    assert rc == 0


def test_cli_ping_reports_unavailable(monkeypatch, capsys):
    from intelligence.hermes import cli

    monkeypatch.setattr(cli, "load_hermes_config", lambda: _config(api_key=""))
    monkeypatch.setattr(cli, "_fallback_available", lambda tier: False)
    rc = cli.main(["ping", "--no-call"])
    assert rc == 1
    assert "UNAVAILABLE" in capsys.readouterr().out
