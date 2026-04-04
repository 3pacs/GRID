"""
GRID Signal Hypothesis Agent — AutoAgent Harness

Top section (EDITABLE): system prompt, tools, and task execution.
Bottom section (FIXED): Harbor adapter. Do not modify below the boundary.
"""

from __future__ import annotations

import os
from typing import Any

# ============================================================
# EDITABLE SECTION — Meta-agent may modify everything above
# the FIXED ADAPTER BOUNDARY marker below.
# ============================================================

SYSTEM_PROMPT = """\
You are a quantitative signal engineer AND financial news analyst for GRID,
an intelligence system for financial markets. Your job is to write a Python
script that generates BUY/NO_BUY signals for EOG Resources.

Your edge is NOT just running sklearn models. Your edge is:
1. Reading energy news headlines and reasoning about supply chain implications
2. Mapping those implications to EOG specifically (Permian Basin producer)
3. Combining LLM-reasoned news signals with vol-regime quant context

The strategy has two layers:
- REGIME LAYER: VIX + credit spreads classify the market environment
  (RISK_ON, NEUTRAL, RISK_OFF, CRISIS). In CRISIS = NO_BUY always.
- NEWS REASONING LAYER: Read energy headlines, classify supply chain impact
  (OPEC cuts → supply tight → EOG benefits), set conviction.

You have access to GRID's PostgreSQL database via grid_bridge:
- bridge.get_eog_prices() — EOG daily closes (13k+ rows since 1989)
- bridge.get_features([...]) — macro/sector features (VIX, oil, credit, etc.)
- bridge.get_energy_news_context(days=7) — energy-specific headlines
- bridge.get_news_headlines(ticker="EOG") — EOG-specific news
- bridge.get_supply_chain_data() — freight, manufacturing, trade balance
- bridge.get_gdelt_tone() — global event sentiment

Output predictions.csv with: obs_date, signal, confidence, predicted_return.
Walk-forward validation required — NEVER train on future data.

Key reasoning chains:
- "OPEC production cut" → tighter supply → oil up → EOG benefits
- "Houthi attacks on shipping" → freight spike → energy supply disruption
- "China demand slowing" → crude demand down → bearish energy
- "Permian pipeline expansion" → EOG can ship more → volume upside
- "Russia sanctions" → less supply → US shale fills gap → EOG wins
"""

MODEL = "claude-sonnet-4-20250514"

def create_tools(client: Any) -> list:
    """Define tools available to the agent during task execution."""
    from openai.types.chat import ChatCompletionToolParam

    return [
        ChatCompletionToolParam(
            type="function",
            function={
                "name": "execute_python",
                "description": (
                    "Execute a Python script in the container. "
                    "The script has access to pandas, numpy, scikit-learn, "
                    "lightgbm, xgboost, scipy, statsmodels, and the grid_bridge module. "
                    "Use this to write and run signal_generator.py."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "code": {
                            "type": "string",
                            "description": "Python code to execute",
                        },
                        "filename": {
                            "type": "string",
                            "description": "Save code to this filename before executing (e.g. signal_generator.py)",
                            "default": "",
                        },
                    },
                    "required": ["code"],
                },
            },
        ),
        ChatCompletionToolParam(
            type="function",
            function={
                "name": "read_file",
                "description": "Read a file from the container filesystem",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Absolute path to read",
                        },
                    },
                    "required": ["path"],
                },
            },
        ),
        ChatCompletionToolParam(
            type="function",
            function={
                "name": "list_features",
                "description": (
                    "Query GRID database for available features. "
                    "Returns feature names, families, and row counts."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "family": {
                            "type": "string",
                            "description": "Filter by family (rates, credit, equity, vol, etc.)",
                            "default": "",
                        },
                        "min_rows": {
                            "type": "integer",
                            "description": "Minimum row count to include",
                            "default": 500,
                        },
                    },
                },
            },
        ),
        ChatCompletionToolParam(
            type="function",
            function={
                "name": "get_energy_news",
                "description": (
                    "Pull recent energy-sector news headlines from GRID database. "
                    "Returns headlines about oil, OPEC, pipelines, shale, Permian, "
                    "sanctions, shipping, freight. Use these for supply chain reasoning."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "days": {
                            "type": "integer",
                            "description": "Days of news to pull (default 7)",
                            "default": 7,
                        },
                    },
                },
            },
        ),
        ChatCompletionToolParam(
            type="function",
            function={
                "name": "reason_supply_chain",
                "description": (
                    "Given a list of energy news headlines, reason about their "
                    "supply chain implications for EOG Resources. Classify each as: "
                    "SUPPLY_TIGHTENING, SUPPLY_EXPANDING, DEMAND_GROWING, "
                    "DEMAND_SHRINKING, GEOPOLITICAL_RISK, INFRASTRUCTURE, "
                    "REGULATORY, or NEUTRAL. Return net supply/demand assessment."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "headlines": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of news headline strings to analyze",
                        },
                    },
                    "required": ["headlines"],
                },
            },
        ),
    ]


def create_agent(client: Any) -> Any:
    """Create the OpenAI Agents SDK agent."""
    from agents import Agent

    return Agent(
        name="GRID Signal Engineer",
        instructions=SYSTEM_PROMPT,
        model=MODEL,
        tools=create_tools(client),
    )


async def run_task(agent: Any, instruction: str, context: dict) -> Any:
    """Execute the signal hypothesis task."""
    from agents import Runner

    result = await Runner.run(
        agent,
        input=instruction,
        context=context,
    )
    return result


# ============================================================
# FIXED ADAPTER BOUNDARY — Do not modify below this line
# unless explicitly asked by the operator.
# ============================================================

import asyncio
import json
import time
from dataclasses import dataclass, field


def to_atif(result: Any) -> dict:
    """Convert SDK result to ATIF trajectory format."""
    steps = []
    if hasattr(result, "raw_responses"):
        for resp in result.raw_responses:
            step = {
                "type": "message",
                "role": "assistant",
                "content": str(resp),
                "timestamp": time.time(),
            }
            steps.append(step)
    return {
        "format": "atif",
        "version": "1.0",
        "steps": steps,
        "final_output": str(result.final_output) if hasattr(result, "final_output") else "",
    }


@dataclass
class AutoAgent:
    """Harbor-compatible agent adapter for GRID signal tasks."""

    metadata: dict = field(default_factory=dict)

    async def setup(self, environment: dict) -> None:
        """Initialize agent with environment context."""
        self.env = environment

    async def run(self, instruction: str, environment: dict, context: dict | None = None) -> dict:
        """Execute the task and return ATIF trajectory."""
        try:
            from openai import OpenAI

            client = OpenAI()
            agent = create_agent(client)
            result = await run_task(agent, instruction, context or {})
            return to_atif(result)
        except Exception as e:
            return {
                "format": "atif",
                "version": "1.0",
                "steps": [{"type": "error", "content": str(e)}],
                "final_output": f"Error: {e}",
            }
