"""
Tests for trading/solana/pipeline.py.

The LLM client and Jupiter client are mocked — the pipeline should be
entirely deterministic given a canned response sequence.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from trading.solana.jupiter_client import SOL_MINT, USDC_MINT
from trading.solana.pipeline import (
    PipelineDecision,
    SolanaPipeline,
    _parse_json_block,
)


# ----------------------------------------------------------------------
# _parse_json_block helper
# ----------------------------------------------------------------------
def test_parse_json_block_extracts_object():
    raw = "some preamble {\"foo\": 1, \"bar\": [1,2]} trailing text"
    assert _parse_json_block(raw, "x") == {"foo": 1, "bar": [1, 2]}


def test_parse_json_block_raises_on_empty():
    with pytest.raises(ValueError, match="empty response"):
        _parse_json_block(None, "director")


def test_parse_json_block_raises_on_missing_json():
    with pytest.raises(ValueError, match="no JSON object"):
        _parse_json_block("no curly braces here", "quant")


def test_parse_json_block_raises_on_malformed():
    with pytest.raises(ValueError, match="malformed JSON"):
        _parse_json_block("{not valid json}", "risk")


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
@pytest.fixture()
def stage_responses() -> list[str]:
    """Canned JSON responses for the 4 pipeline stages."""
    director = {
        "mint": SOL_MINT,
        "symbol": "SOL",
        "thesis": "SOL breaking out on rising volume",
    }
    quant = {
        "technical_score": 0.72,
        "trend_strength": 0.65,
        "volatility": 0.55,
        "probability_score": 0.6,
        "notes": "bullish MA cross",
    }
    risk = {
        "position_size": 0.05,
        "stop_loss_pct": 0.08,
        "risk_score": 0.7,
        "veto": False,
        "reason": "acceptable",
    }
    execution = {
        "action": "BUY",
        "symbol": "SOL",
        "mint": SOL_MINT,
        "quote_mint": USDC_MINT,
        "size_fraction": 0.05,
        "stop_loss_pct": 0.08,
        "take_profit_pct": 0.15,
        "rationale": "momentum long",
    }
    return [json.dumps(x) for x in (director, quant, risk, execution)]


@pytest.fixture()
def mock_llm(stage_responses):
    """LLM client whose .chat() returns the canned responses in order."""
    llm = MagicMock()
    llm.chat.side_effect = stage_responses
    return llm


@pytest.fixture()
def mock_jupiter():
    jup = MagicMock()
    jup.get_token_price.return_value = {
        SOL_MINT: {"usdPrice": 125.0, "decimals": 9}
    }
    return jup


# ----------------------------------------------------------------------
# SolanaPipeline.run
# ----------------------------------------------------------------------
def test_pipeline_run_happy_path(mock_llm, mock_jupiter):
    pipeline = SolanaPipeline(llm=mock_llm, jupiter=mock_jupiter)

    decision = pipeline.run("Analyse SOL momentum today")

    assert isinstance(decision, PipelineDecision)
    assert decision.symbol == "SOL"
    assert decision.mint == SOL_MINT
    assert decision.action == "BUY"
    assert decision.size_fraction == 0.05
    assert decision.stop_loss_pct == 0.08
    assert decision.take_profit_pct == 0.15
    assert decision.risk_score == 0.7
    assert decision.risk_veto is False
    assert decision.actionable is True
    assert decision.price_snapshot == {"usdPrice": 125.0, "decimals": 9}
    assert mock_llm.chat.call_count == 4


def test_pipeline_run_requires_task(mock_llm, mock_jupiter):
    pipeline = SolanaPipeline(llm=mock_llm, jupiter=mock_jupiter)
    with pytest.raises(ValueError, match="task must be non-empty"):
        pipeline.run("")


def test_pipeline_run_director_missing_mint(mock_jupiter):
    llm = MagicMock()
    llm.chat.return_value = json.dumps({"symbol": "SOL", "thesis": "x"})
    pipeline = SolanaPipeline(llm=llm, jupiter=mock_jupiter)
    with pytest.raises(ValueError, match="did not return a mint"):
        pipeline.run("task")


def test_pipeline_run_graceful_price_failure(mock_llm):
    jup = MagicMock()
    jup.get_token_price.side_effect = RuntimeError("jupiter down")
    pipeline = SolanaPipeline(llm=mock_llm, jupiter=jup)

    decision = pipeline.run("Analyse SOL")

    # Empty snapshot, but pipeline still completes using downstream LLM calls.
    assert decision.price_snapshot == {}
    assert decision.action == "BUY"


def test_pipeline_risk_veto_is_not_actionable():
    llm = MagicMock()
    llm.chat.side_effect = [
        json.dumps({"mint": SOL_MINT, "symbol": "SOL", "thesis": "t"}),
        json.dumps({"technical_score": 0.2, "probability_score": 0.1}),
        json.dumps(
            {
                "position_size": 0.0,
                "stop_loss_pct": 0.05,
                "risk_score": 0.1,
                "veto": True,
                "reason": "too risky",
            }
        ),
        json.dumps(
            {
                "action": "BUY",
                "symbol": "SOL",
                "mint": SOL_MINT,
                "quote_mint": USDC_MINT,
                "size_fraction": 0.0,
                "stop_loss_pct": 0.05,
                "take_profit_pct": 0.1,
                "rationale": "ignored",
            }
        ),
    ]
    jup = MagicMock()
    jup.get_token_price.return_value = {SOL_MINT: {"usdPrice": 1.0}}

    pipeline = SolanaPipeline(llm=llm, jupiter=jup)
    decision = pipeline.run("task")

    assert decision.risk_veto is True
    assert decision.actionable is False
