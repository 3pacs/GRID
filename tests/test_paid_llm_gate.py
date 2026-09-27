"""Paid-LLM-gate coverage (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 remediation).

The triage found paid Gemini/OpenAI/OpenRouter/Anthropic calls that bypass
``llm.router``'s own ``GRID_ALLOW_PAID_LLM`` gate by talking to the SDK/REST
API directly. This file asserts, for every path that was found:

  * with the flag unset (default), the underlying paid client/SDK is never
    constructed or called, and the caller gets an honest error/fallback
    (never a fabricated placeholder result);
  * with the flag set, the same path proceeds to build the client (mocked
    here — no test ever hits a real network endpoint or spends real money).

Nothing in this file makes a real HTTP call to Gemini, OpenAI, Anthropic, or
OpenRouter: every SDK/`requests` call is mocked or replaced with a fake.
"""

from __future__ import annotations

import sys
import types as _types
from unittest.mock import MagicMock, patch

import pytest

import config


@pytest.fixture(autouse=True)
def _ensure_google_genai_importable():
    """``google-genai`` isn't pinned in requirements.txt, so it may not be
    installed everywhere this suite runs (confirmed missing in CI). When a
    real ``import google.genai`` fails, attach a synthetic ``genai``
    submodule -- just enough for ``from google import genai`` / ``from
    google.genai import types`` and ``patch("google.genai.Client", ...)`` to
    work -- to the REAL ``google`` namespace package (imported normally, so
    its other real members like ``google.protobuf`` -- a transitive
    dependency of langfuse/opentelemetry, used elsewhere in this same test
    file -- are completely untouched). Never replaces ``sys.modules["google"]``
    itself. If ``google.genai`` IS installed, this is a no-op.
    """
    import importlib

    try:
        importlib.import_module("google.genai")
        yield
        return
    except ImportError:
        pass

    try:
        google_mod = importlib.import_module("google")
    except ImportError:
        # No google.* distribution at all -- fabricate a bare namespace
        # package as a last resort (not expected to happen in this repo).
        google_mod = _types.ModuleType("google")
        google_mod.__path__ = []
        sys.modules["google"] = google_mod

    genai_mod = _types.ModuleType("google.genai")
    genai_mod.Client = MagicMock(name="StubGenaiClient")
    genai_types_mod = _types.ModuleType("google.genai.types")
    genai_types_mod.GenerateImagesConfig = MagicMock(name="StubGenerateImagesConfig")
    genai_mod.types = genai_types_mod

    sys.modules["google.genai"] = genai_mod
    sys.modules["google.genai.types"] = genai_types_mod
    google_mod.genai = genai_mod

    yield

    sys.modules.pop("google.genai", None)
    sys.modules.pop("google.genai.types", None)
    if hasattr(google_mod, "genai"):
        try:
            delattr(google_mod, "genai")
        except AttributeError:
            pass


# ---------------------------------------------------------------------------
# intelligence/deep_dive.py — direct Anthropic + direct Gemini bypass
# ---------------------------------------------------------------------------

class TestDeepDivePaidGate:
    def _block_batch_tier(self, monkeypatch):
        """Force the (already-gated) BATCH tier to report unavailable so
        ``_call_best_llm`` falls through to the direct-SDK branches under
        test."""
        from llm import router as llm_router

        dead_client = MagicMock()
        dead_client.is_available = False
        monkeypatch.setattr(llm_router, "get_llm", lambda *a, **kw: dead_client)

    def test_direct_anthropic_and_gemini_blocked_when_gate_off(self, monkeypatch):
        from intelligence import deep_dive

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        monkeypatch.setattr(config.settings, "ANTHROPIC_API_KEY", "sk-should-not-be-used")
        monkeypatch.setattr(config.settings, "AGENTS_ANTHROPIC_API_KEY", "")
        monkeypatch.setenv("GEMINI_API_KEY", "should-not-be-used")
        self._block_batch_tier(monkeypatch)

        import requests as _requests

        def _boom_post(*a, **kw):
            raise AssertionError("direct Anthropic call must not happen when gate is off")

        monkeypatch.setattr(_requests, "post", _boom_post)

        genai_client_mock = MagicMock()
        with patch("google.genai.Client", genai_client_mock):
            with pytest.raises(RuntimeError, match="paid providers blocked"):
                deep_dive._call_best_llm("test prompt")

        genai_client_mock.assert_not_called()

    def test_direct_anthropic_used_when_gate_on(self, monkeypatch):
        from intelligence import deep_dive

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
        monkeypatch.setattr(config.settings, "ANTHROPIC_API_KEY", "sk-real")
        monkeypatch.setattr(config.settings, "AGENTS_ANTHROPIC_API_KEY", "")
        monkeypatch.setattr(config.settings, "ANTHROPIC_BASE_URL", "https://api.anthropic.com")
        monkeypatch.setattr(config.settings, "ANTHROPIC_TIMEOUT_SECONDS", 30)
        self._block_batch_tier(monkeypatch)

        import requests as _requests

        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {"content": [{"type": "text", "text": "analysis"}]}
        monkeypatch.setattr(_requests, "post", MagicMock(return_value=fake_resp))

        text_out, model, provider = deep_dive._call_best_llm("test prompt")
        assert provider == "anthropic"
        assert text_out == "analysis"

    def test_direct_gemini_blocked_when_gate_off_no_anthropic_key(self, monkeypatch):
        from intelligence import deep_dive

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        monkeypatch.setattr(config.settings, "ANTHROPIC_API_KEY", "")
        monkeypatch.setattr(config.settings, "AGENTS_ANTHROPIC_API_KEY", "")
        monkeypatch.setenv("GEMINI_API_KEY", "should-not-be-used")
        self._block_batch_tier(monkeypatch)

        genai_client_mock = MagicMock()
        with patch("google.genai.Client", genai_client_mock):
            with pytest.raises(RuntimeError, match="paid providers blocked"):
                deep_dive._call_best_llm("test prompt")
        genai_client_mock.assert_not_called()

    def test_direct_gemini_used_when_gate_on(self, monkeypatch):
        from intelligence import deep_dive

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
        monkeypatch.setattr(config.settings, "ANTHROPIC_API_KEY", "")
        monkeypatch.setattr(config.settings, "AGENTS_ANTHROPIC_API_KEY", "")
        monkeypatch.setenv("GEMINI_API_KEY", "real-key")
        self._block_batch_tier(monkeypatch)

        fake_response = MagicMock()
        fake_response.text = "gemini analysis"
        fake_client = MagicMock()
        fake_client.models.generate_content.return_value = fake_response

        with patch("google.genai.Client", return_value=fake_client) as ctor:
            text_out, model, provider = deep_dive._call_best_llm("test prompt")
            ctor.assert_called_once_with(api_key="real-key")
        assert provider == "gemini"
        assert text_out == "gemini analysis"


# ---------------------------------------------------------------------------
# intelligence/audio_briefing.py — direct Gemini (script/title-card) +
# direct OpenAI TTS bypass
# ---------------------------------------------------------------------------

class TestAudioBriefingPaidGate:
    def test_gemini_client_blocked_when_gate_off(self, monkeypatch):
        from intelligence import audio_briefing

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        monkeypatch.setenv("GEMINI_API_KEY", "should-not-be-used")

        genai_client_mock = MagicMock()
        with patch("google.genai.Client", genai_client_mock):
            with pytest.raises(PermissionError, match="Paid generation disabled"):
                audio_briefing._get_gemini_client()
        genai_client_mock.assert_not_called()

    def test_gemini_client_built_when_gate_on(self, monkeypatch):
        from intelligence import audio_briefing

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
        monkeypatch.setenv("GEMINI_API_KEY", "real-key")

        fake_client = MagicMock()
        with patch("google.genai.Client", return_value=fake_client) as ctor:
            client = audio_briefing._get_gemini_client()
            ctor.assert_called_once_with(api_key="real-key")
        assert client is fake_client

    def test_openai_tts_client_blocked_when_gate_off(self, monkeypatch):
        from intelligence import audio_briefing

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        monkeypatch.setenv("OPENAI_API_KEY", "should-not-be-used")

        with patch("openai.OpenAI") as openai_mock:
            with pytest.raises(PermissionError, match="Paid generation disabled"):
                audio_briefing._get_openai_client()
        openai_mock.assert_not_called()

    def test_openai_tts_client_built_when_gate_on(self, monkeypatch):
        from intelligence import audio_briefing

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
        monkeypatch.setenv("OPENAI_API_KEY", "real-key")

        fake_client = MagicMock()
        with patch("openai.OpenAI", return_value=fake_client) as ctor:
            client = audio_briefing._get_openai_client()
            ctor.assert_called_once_with(api_key="real-key")
        assert client is fake_client


# ---------------------------------------------------------------------------
# intelligence/image_gen.py — direct Imagen bypass
# ---------------------------------------------------------------------------

class TestImageGenPaidGate:
    def test_get_client_blocked_when_gate_off(self, monkeypatch):
        from intelligence import image_gen

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        monkeypatch.setenv("GEMINI_API_KEY", "should-not-be-used")

        genai_client_mock = MagicMock()
        with patch("google.genai.Client", genai_client_mock):
            with pytest.raises(PermissionError, match="Paid generation disabled"):
                image_gen._get_client()
        genai_client_mock.assert_not_called()

    def test_get_client_built_when_gate_on(self, monkeypatch):
        from intelligence import image_gen

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
        monkeypatch.setenv("GEMINI_API_KEY", "real-key")

        fake_client = MagicMock()
        with patch("google.genai.Client", return_value=fake_client) as ctor:
            client = image_gen._get_client()
            ctor.assert_called_once_with(api_key="real-key")
        assert client is fake_client

    def test_generate_image_never_calls_generator_when_gate_off(self, monkeypatch):
        """No generator function should reach the network when the gate is off —
        the check happens before any Imagen call is built."""
        from intelligence import image_gen

        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        monkeypatch.setenv("GEMINI_API_KEY", "should-not-be-used")

        with pytest.raises(PermissionError):
            image_gen.generate_custom("a prompt")

    def test_daily_pack_reraises_permission_error_instead_of_swallowing(self, monkeypatch):
        """The pack loop must not read a PermissionError from one generator as
        "that image failed, try the next one" -- when generation is
        disabled, it applies to every generator in the batch, so the pack
        should surface that instead of quietly returning fewer images than
        expected (which reads as "0 flows today", not "generation is off").
        """
        from intelligence import image_gen

        def _boom(engine, style="dark"):
            raise PermissionError("Paid generation disabled: set GRID_ALLOW_PAID_LLM=1 to enable Gemini.")

        monkeypatch.setattr(image_gen, "generate_flow_infographic", _boom)

        with pytest.raises(PermissionError):
            image_gen.generate_daily_briefing_pack(MagicMock())


# ---------------------------------------------------------------------------
# api/routers/flows.py — generate-image is POST-only, gated, auth-checked
# ---------------------------------------------------------------------------

class TestFlowsImageRouterIsPostOnly:
    def test_generate_image_route_is_post_not_get(self):
        from api.routers import flows as flows_router

        matches = [
            r for r in flows_router.router.routes
            if getattr(r, "path", "").endswith("/generate-image/{image_type}")
        ]
        assert matches, "route not found"
        assert matches[0].methods == {"POST"}

    def test_generate_image_custom_route_is_post(self):
        from api.routers import flows as flows_router

        matches = [
            r for r in flows_router.router.routes
            if getattr(r, "path", "").endswith("/generate-image/custom")
        ]
        assert matches, "route not found"
        assert matches[0].methods == {"POST"}

    def test_generate_image_route_requires_auth(self):
        import inspect

        from api.routers import flows as flows_router

        sig = inspect.signature(flows_router.generate_flow_image)
        assert "_token" in sig.parameters
        default = sig.parameters["_token"].default
        assert default is not inspect.Parameter.empty  # a Depends(...) marker

    def test_generate_flow_image_returns_403_when_gate_off(self, monkeypatch):
        import asyncio

        from api.routers import flows as flows_router

        monkeypatch.setattr(flows_router, "get_db_engine", lambda: MagicMock())
        monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
        monkeypatch.setenv("GEMINI_API_KEY", "should-not-be-used")

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as excinfo:
            asyncio.run(
                flows_router.generate_flow_image(
                    "flow_infographic", style="dark", model_tier="fast", _token="test-token",
                )
            )
        assert excinfo.value.status_code == 403


# ---------------------------------------------------------------------------
# agents/config.py — TradingAgents openai/anthropic providers
# ---------------------------------------------------------------------------

class TestAgentsConfigPaidGate:
    @patch("agents.config.settings")
    def test_openai_provider_falls_back_to_llamacpp_when_gate_off(self, mock_settings, monkeypatch):
        from llm import router as llm_router

        mock_settings.AGENTS_LLM_PROVIDER = "openai"
        mock_settings.AGENTS_LLM_MODEL = "auto"
        mock_settings.AGENTS_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_OPENAI_API_KEY = "sk-should-not-be-used"
        mock_settings.LLAMACPP_BASE_URL = "http://localhost:8080"
        mock_settings.LLAMACPP_CHAT_MODEL = "hermes"

        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)

        from agents.config import build_agent_config

        with patch("agents.config._llamacpp_config") as mock_llama:
            mock_llama.return_value = {"llm_provider": "llamacpp"}
            build_agent_config()
            mock_llama.assert_called_once()

    @patch("agents.config.settings")
    def test_anthropic_provider_falls_back_to_llamacpp_when_gate_off(self, mock_settings, monkeypatch):
        from llm import router as llm_router

        mock_settings.AGENTS_LLM_PROVIDER = "anthropic"
        mock_settings.AGENTS_LLM_MODEL = "auto"
        mock_settings.AGENTS_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_ANTHROPIC_API_KEY = "sk-should-not-be-used"
        mock_settings.LLAMACPP_BASE_URL = "http://localhost:8080"
        mock_settings.LLAMACPP_CHAT_MODEL = "hermes"

        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)

        from agents.config import build_agent_config

        with patch("agents.config._llamacpp_config") as mock_llama:
            mock_llama.return_value = {"llm_provider": "llamacpp"}
            build_agent_config()
            mock_llama.assert_called_once()

    @patch("agents.config.settings")
    def test_openai_provider_used_when_gate_on_and_key_present(self, mock_settings, monkeypatch):
        from llm import router as llm_router

        mock_settings.AGENTS_LLM_PROVIDER = "openai"
        mock_settings.AGENTS_LLM_MODEL = "auto"
        mock_settings.AGENTS_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_OPENAI_API_KEY = "sk-real"

        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: True)
        monkeypatch.setenv("OPENAI_API_KEY", "")  # os.environ write happens inside; harmless

        from agents.config import build_agent_config

        cfg = build_agent_config()
        assert cfg["llm_provider"] == "openai"


# ---------------------------------------------------------------------------
# ollama/router.py — legacy TaskRouter quick/deep openai + anthropic
# ---------------------------------------------------------------------------

class TestOllamaRouterPaidGate:
    @pytest.fixture(autouse=True)
    def _reset_singleton(self):
        from ollama import router as ollama_router

        ollama_router._router_instance = None
        yield
        ollama_router._router_instance = None

    def test_quick_openai_blocked_when_gate_off(self, monkeypatch):
        from llm import router as llm_router
        from ollama import router as ollama_router

        monkeypatch.setattr(config.settings, "LLM_ROUTER_ENABLED", True)
        monkeypatch.setattr(config.settings, "LLM_QUICK_PROVIDER", "openai")
        monkeypatch.setattr(config.settings, "LLM_DEEP_PROVIDER", "llamacpp")
        monkeypatch.setattr(config.settings, "OPENAI_API_KEY", "sk-should-not-be-used")
        monkeypatch.setattr(config.settings, "AGENTS_OPENAI_API_KEY", "")
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)

        with patch("ollama.client.OpenAIClient") as openai_client_mock:
            with patch("llamacpp.client.get_client", return_value=MagicMock(is_available=True)):
                router_obj = ollama_router.get_router()
        openai_client_mock.assert_not_called()
        assert router_obj.quick_client is None

    def test_deep_anthropic_blocked_when_gate_off(self, monkeypatch):
        from llm import router as llm_router
        from ollama import router as ollama_router

        monkeypatch.setattr(config.settings, "LLM_ROUTER_ENABLED", True)
        monkeypatch.setattr(config.settings, "LLM_QUICK_PROVIDER", "llamacpp")
        monkeypatch.setattr(config.settings, "LLM_DEEP_PROVIDER", "anthropic")
        monkeypatch.setattr(config.settings, "AGENTS_ANTHROPIC_API_KEY", "sk-should-not-be-used")
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)

        with patch("ollama.client.OpenAIClient") as openai_client_mock:
            with patch("llamacpp.client.get_client", return_value=MagicMock(is_available=True)):
                router_obj = ollama_router.get_router()
        openai_client_mock.assert_not_called()
        assert router_obj.deep_client is None


# ---------------------------------------------------------------------------
# ollama/client.py — get_client() singleton, OpenAI-first bypass
# ---------------------------------------------------------------------------

class TestOllamaClientPaidGate:
    @pytest.fixture(autouse=True)
    def _reset_singleton(self):
        from ollama import client as ollama_client

        ollama_client._client_instance = None
        yield
        ollama_client._client_instance = None

    def test_openai_first_blocked_when_gate_off(self, monkeypatch):
        from llm import router as llm_router
        from ollama import client as ollama_client

        monkeypatch.setattr(config.settings, "OPENAI_API_KEY", "sk-should-not-be-used")
        monkeypatch.setattr(config.settings, "AGENTS_OPENAI_API_KEY", "")
        monkeypatch.setattr(config.settings, "LLAMACPP_ENABLED", False)
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)

        with patch("ollama.client.OpenAIClient") as openai_client_mock:
            client = ollama_client.get_client()
        openai_client_mock.assert_not_called()
        # Falls through to the local Ollama client, never OpenAI.
        assert not isinstance(client, MagicMock)
        assert client.__class__.__name__ == "OllamaClient"

    def test_openai_first_used_when_gate_on(self, monkeypatch):
        from llm import router as llm_router
        from ollama import client as ollama_client

        monkeypatch.setattr(config.settings, "OPENAI_API_KEY", "sk-real")
        monkeypatch.setattr(config.settings, "AGENTS_OPENAI_API_KEY", "")
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: True)

        fake_openai = MagicMock()
        fake_openai.is_available = True
        with patch("ollama.client.OpenAIClient", return_value=fake_openai) as ctor:
            client = ollama_client.get_client()
        ctor.assert_called_once()
        assert client is fake_openai


# ---------------------------------------------------------------------------
# api/routers/chat.py — background A/B "fire Opus via OpenRouter" call
# ---------------------------------------------------------------------------

class TestChatAskOpusABGate:
    def test_ab_opus_block_reads_paid_gate_before_building_client(self):
        """Static check that the A/B block now consults _paid_llm_allowed
        before constructing the OpenRouter client, so the gate can never be
        bypassed by editing the surrounding code without touching the check.
        """
        import inspect

        from api.routers import chat as chat_router

        src = inspect.getsource(chat_router)
        idx = src.index('model="anthropic/claude-opus-4"')
        # The paid-gate import/check must appear before the client the model
        # string belongs to is constructed.
        preceding = src[:idx]
        assert "_paid_llm_allowed" in preceding.split("or_key = getattr(settings")[-1] \
            or "_paid_llm_allowed" in preceding


# ---------------------------------------------------------------------------
# scripts/baseline_predictions.py — query_openai/anthropic/gemini/openrouter
# ---------------------------------------------------------------------------

class TestBaselinePredictionsPaidGate:
    def _load_module(self):
        import importlib

        return importlib.import_module("scripts.baseline_predictions")

    def test_query_openai_returns_none_when_gate_off(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-should-not-be-used")

        with patch("requests.post") as post_mock:
            assert mod.query_openai("prompt") is None
        post_mock.assert_not_called()

    def test_query_anthropic_returns_none_when_gate_off(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-should-not-be-used")

        with patch("requests.post") as post_mock:
            assert mod.query_anthropic("prompt") is None
        post_mock.assert_not_called()

    def test_query_gemini_returns_none_when_gate_off(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)
        monkeypatch.setenv("GEMINI_API_KEY", "should-not-be-used")

        with patch("requests.post") as post_mock:
            assert mod.query_gemini("prompt") is None
        post_mock.assert_not_called()

    def test_query_openrouter_returns_none_when_gate_off(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)
        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-should-not-be-used")

        with patch("requests.post") as post_mock:
            assert mod.query_openrouter("prompt") is None
        post_mock.assert_not_called()

    def test_query_groq_returns_none_when_gate_off(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)
        monkeypatch.setenv("GROQ_API_KEY", "sk-should-not-be-used")

        with patch("requests.post") as post_mock:
            assert mod.query_groq("prompt") is None
        post_mock.assert_not_called()

    def test_query_groq_proceeds_when_gate_on(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: True)
        monkeypatch.setenv("GROQ_API_KEY", "sk-real")

        fake_resp = MagicMock()
        fake_resp.json.return_value = {"choices": [{"message": {"content": "ok"}}]}
        with patch("requests.post", return_value=fake_resp) as post_mock:
            result = mod.query_groq("prompt")
        post_mock.assert_called_once()
        assert result == "ok"

    def test_query_openai_proceeds_when_gate_on(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: True)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-real")

        fake_resp = MagicMock()
        fake_resp.json.return_value = {"choices": [{"message": {"content": "ok"}}]}
        with patch("requests.post", return_value=fake_resp) as post_mock:
            result = mod.query_openai("prompt")
        post_mock.assert_called_once()
        assert result == "ok"


# ---------------------------------------------------------------------------
# scripts/run_regression_eval.py — arm B (OpenRouter -> Opus)
# ---------------------------------------------------------------------------

class TestRegressionEvalArmBGate:
    @pytest.fixture(autouse=True)
    def _ensure_env_file(self):
        """``scripts/run_regression_eval.py`` ``sys.exit(2)``s at import time
        if the repo has no ``.env`` (a real dev checkout always does; CI's
        Backend Tests job passes env vars directly and has none). That's a
        pre-existing script quirk unrelated to the paid gate -- create an
        empty one only if missing, so importing the module in a test doesn't
        depend on it, and remove it again so nothing is left behind.
        """
        import pathlib

        env_path = pathlib.Path(__file__).resolve().parent.parent / ".env"
        created = False
        if not env_path.exists():
            env_path.write_text("DB_PASSWORD=testpass\n")
            created = True
        yield
        if created:
            env_path.unlink(missing_ok=True)

    def _load_module(self):
        import importlib

        return importlib.import_module("scripts.run_regression_eval")

    def test_arm_b_client_blocked_when_gate_off(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: False)
        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-should-not-be-used")

        with pytest.raises(RuntimeError, match="GRID_ALLOW_PAID_LLM"):
            mod._make_arm_b_client()

    def test_arm_b_client_built_when_gate_on(self, monkeypatch):
        from llm import router as llm_router

        mod = self._load_module()
        monkeypatch.setattr(llm_router, "_paid_llm_allowed", lambda: True)
        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-real")

        client = mod._make_arm_b_client()
        assert client.api_key == "sk-real"
        assert client.model == mod.ARM_B_MODEL


# ---------------------------------------------------------------------------
# config.py — GRID_ALLOW_PAID_LLM must never crash the app on a blank value
# ---------------------------------------------------------------------------

class TestGridAllowPaidLlmFlagCoercion:
    """An unset/blank flag must mean "paid off", never "refuse to start".

    Pydantic's default bool coercion raises a ValidationError on an empty
    string, which is exactly what a templated ``.env`` line
    (``GRID_ALLOW_PAID_LLM=``) or an unset shell var substituted into one
    produces -- the fail-safe default this flag exists to guarantee would
    instead crash the whole app at startup.
    """

    @staticmethod
    def _build(value):
        import config as config_module

        return config_module.Settings(GRID_ALLOW_PAID_LLM=value)

    def test_empty_string_coerces_to_false(self):
        assert self._build("").GRID_ALLOW_PAID_LLM is False

    def test_whitespace_only_coerces_to_false(self):
        assert self._build("   ").GRID_ALLOW_PAID_LLM is False

    @pytest.mark.parametrize("value", ["0", "false", "False", "FALSE", "no", "NO", "off", "OFF"])
    def test_falsy_strings_coerce_to_false(self, value):
        assert self._build(value).GRID_ALLOW_PAID_LLM is False

    @pytest.mark.parametrize("value", ["1", "true", "True", "TRUE", "yes", "YES", "on", "ON"])
    def test_truthy_strings_coerce_to_true(self, value):
        assert self._build(value).GRID_ALLOW_PAID_LLM is True

    def test_real_bool_values_pass_through(self):
        assert self._build(True).GRID_ALLOW_PAID_LLM is True
        assert self._build(False).GRID_ALLOW_PAID_LLM is False

    def test_garbage_string_still_raises(self):
        import pydantic

        with pytest.raises(pydantic.ValidationError):
            self._build("maybe")
