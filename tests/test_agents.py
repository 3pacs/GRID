"""
Tests for the agents module — config, adapter, and runner fallback logic.
"""

from __future__ import annotations

from unittest.mock import patch


from agents.adapter import parse_agent_decision, _extract_action
from agents.config import build_agent_config


# ---------------------------------------------------------------------------
# adapter tests
# ---------------------------------------------------------------------------

class TestExtractAction:
    def test_buy(self):
        assert _extract_action("BUY at 450") == "BUY"

    def test_sell(self):
        assert _extract_action("We should SELL here") == "SELL"

    def test_hold(self):
        assert _extract_action("Neutral outlook") == "HOLD"

    def test_long_maps_to_buy(self):
        assert _extract_action("Go LONG SPY") == "BUY"

    def test_short_maps_to_sell(self):
        assert _extract_action("SHORT the market") == "SELL"

    def test_non_string(self):
        assert _extract_action(42) == "HOLD"


class TestParseAgentDecision:
    def test_none_input(self):
        result = parse_agent_decision(None)
        assert result["final_decision"] == "ERROR"

    def test_string_input(self):
        result = parse_agent_decision("We recommend BUY")
        assert result["final_decision"] == "BUY"
        assert "BUY" in result["decision_reasoning"]

    def test_dict_input(self):
        raw = {
            "action": "SELL",
            "reasoning": "Market overvalued",
            "analyst_reports": {"fundamental": "bearish"},
            "debate": {"summary": "consensus sell"},
            "risk": {"level": "high"},
        }
        result = parse_agent_decision(raw)
        assert result["final_decision"] == "SELL"
        assert result["decision_reasoning"] == "Market overvalued"

    def test_dict_no_action_defaults_hold(self):
        raw = {"summary": "nothing happening"}
        result = parse_agent_decision(raw)
        assert result["final_decision"] == "HOLD"

    def test_unexpected_type(self):
        result = parse_agent_decision(12345)
        assert result["final_decision"] == "HOLD"


# ---------------------------------------------------------------------------
# config tests
# ---------------------------------------------------------------------------

class TestBuildAgentConfig:
    @patch("agents.config.settings")
    def test_llamacpp_default(self, mock_settings):
        mock_settings.AGENTS_LLM_PROVIDER = "llamacpp"
        mock_settings.AGENTS_LLM_MODEL = "auto"
        mock_settings.AGENTS_DEBATE_ROUNDS = 1
        mock_settings.LLAMACPP_BASE_URL = "http://localhost:8080"
        mock_settings.LLAMACPP_CHAT_MODEL = "hermes"

        with patch("agents.config._llamacpp_config") as mock_llama:
            mock_llama.return_value = {"llm_provider": "openai"}
            build_agent_config()
            mock_llama.assert_called_once()

    @patch("agents.config.settings")
    def test_openai_without_key_falls_back(self, mock_settings):
        mock_settings.AGENTS_LLM_PROVIDER = "openai"
        mock_settings.AGENTS_LLM_MODEL = "auto"
        mock_settings.AGENTS_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_OPENAI_API_KEY = ""
        mock_settings.LLAMACPP_BASE_URL = "http://localhost:8080"
        mock_settings.LLAMACPP_CHAT_MODEL = "hermes"

        with patch("agents.config._llamacpp_config") as mock_llama:
            mock_llama.return_value = {"llm_provider": "openai"}
            build_agent_config()
            mock_llama.assert_called_once()


# ---------------------------------------------------------------------------
# runner single-model fallback tests
# ---------------------------------------------------------------------------

class TestRunnerSingleModelParsing:
    """Test the static _parse_single_model_response method."""

    def test_parse_buy_decision(self):
        from agents.runner import AgentRunner

        response = (
            "DECISION: BUY\n"
            "CONFIDENCE: HIGH\n\n"
            "FUNDAMENTAL ANALYSIS:\n"
            "Strong growth indicators across the board.\n\n"
            "SENTIMENT & POSITIONING:\n"
            "Risk-on sentiment prevails.\n\n"
            "RISK ASSESSMENT:\n"
            "Potential for rate hikes remains a concern.\n\n"
            "REASONING:\n"
            "Growth regime with high confidence supports equity exposure."
        )
        result = AgentRunner._parse_single_model_response(response)
        assert result["action"] == "BUY"
        assert "Strong growth" in result["analyst_reports"]["fundamental"]
        assert "rate hikes" in result["risk"]["assessment"]
        assert "Growth regime" in result["reasoning"]

    def test_parse_sell_decision(self):
        from agents.runner import AgentRunner

        response = "DECISION: SELL\nREASONING:\nMarket downturn expected."
        result = AgentRunner._parse_single_model_response(response)
        assert result["action"] == "SELL"

    def test_parse_no_decision_line(self):
        from agents.runner import AgentRunner

        response = "The market looks uncertain and I would hold."
        result = AgentRunner._parse_single_model_response(response)
        assert result["action"] == "HOLD"

    def test_parse_empty_response(self):
        from agents.runner import AgentRunner

        result = AgentRunner._parse_single_model_response("")
        assert result["action"] == "HOLD"


# ---------------------------------------------------------------------------
# runner single-model analysis — Hermes bridge redirect
# ---------------------------------------------------------------------------

class TestRunnerSingleModelAnalysis:
    """The analyst path routes through HermesAgent with a LOCAL-pinned fallback."""

    @staticmethod
    def _runner():
        from agents.runner import AgentRunner

        # Bypass __init__ (and its GRIDContext/DB wiring) — the method under
        # test only needs the static response parser.
        return object.__new__(AgentRunner)

    def test_routes_through_hermes_and_parses_decision(self):
        from datetime import date

        structured = (
            "DECISION: BUY\nCONFIDENCE: HIGH\n\n"
            "FUNDAMENTAL ANALYSIS:\nGrowth indicators strong.\n\n"
            "RISK ASSESSMENT:\nRate path uncertain.\n\n"
            "REASONING:\nGrowth regime supports equity exposure."
        )

        class _Analysis:
            ok = True
            text = structured
            source = "hermes"
            model = "gpt-4o"

        with patch("intelligence.hermes.HermesAgent") as MockAgent:
            MockAgent.return_value.analyze.return_value = _Analysis()
            out = self._runner()._run_single_model_analysis(
                "SPY", date(2026, 6, 5), "regime: GROWTH (HIGH confidence)"
            )

        assert out["action"] == "BUY"
        assert "Growth indicators" in out["analyst_reports"]["fundamental"]

        # Fallback tier pinned to LOCAL so behavior is unchanged until a key is
        # configured; the analyst system prompt + budget are passed through.
        cfg = MockAgent.call_args.args[0]
        assert cfg.fallback_tier == "local"
        _, kwargs = MockAgent.return_value.analyze.call_args
        assert kwargs["system"].startswith("You are a senior macro trading analyst")
        assert kwargs["max_completion_tokens"] == 1500

    def test_returns_hold_when_no_backend(self):
        from datetime import date

        class _Analysis:
            ok = False
            text = None
            source = "unavailable"
            model = None

        with patch("intelligence.hermes.HermesAgent") as MockAgent:
            MockAgent.return_value.analyze.return_value = _Analysis()
            out = self._runner()._run_single_model_analysis(
                "SPY", date(2026, 6, 5), "ctx"
            )

        assert out["action"] == "HOLD"
        assert out["analyst_reports"]["note"] == "LLM unavailable"
