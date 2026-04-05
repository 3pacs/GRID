"""
GRID Intelligence — Agent Arena: 10 Competing Trading Analysts.

Spawns 10 LLM agents with distinct viewpoints and biases. Each independently
evaluates the same market data and produces a directional thesis. A meta-judge
aggregates their scores, weighted by historical accuracy. Over time, agents
that are consistently wrong get retrained; agents that are right gain influence.

Key entry points:
  run_arena           — execute one competition round (all 10 agents score)
  score_past_rounds   — evaluate agent predictions against actual outcomes
  get_agent_leaderboard — rank agents by accuracy
  evolve_agents       — retrain bottom performers, mutate top performers

The arena runs every 4 hours via Hermes, producing a consensus thesis that
feeds into the main thesis_scorer as an additional model.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# AGENT DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════

AGENTS: list[dict[str, Any]] = [
    {
        "id": "bull_advocate",
        "name": "Bull Advocate",
        "bias": "bullish",
        "system_prompt": (
            "You are a bullish market analyst. Your job is to find reasons "
            "the market will go UP over the next 5 trading days. Look for: "
            "momentum, accumulation, bullish divergences, positive catalysts, "
            "oversold conditions. Be specific — cite the data points."
        ),
    },
    {
        "id": "bear_advocate",
        "name": "Bear Advocate",
        "bias": "bearish",
        "system_prompt": (
            "You are a bearish market analyst. Your job is to find reasons "
            "the market will go DOWN over the next 5 trading days. Look for: "
            "distribution, overvaluation, bearish divergences, negative catalysts, "
            "overbought conditions. Be specific — cite the data points."
        ),
    },
    {
        "id": "macro_strategist",
        "name": "Macro Strategist",
        "bias": "neutral",
        "system_prompt": (
            "You are a macro strategist focused on Fed liquidity, credit conditions, "
            "yield curves, and global capital flows. Assess the 5-day outlook based on "
            "the macro environment. Ignore noise — focus on the structural forces."
        ),
    },
    {
        "id": "flow_detective",
        "name": "Flow Detective",
        "bias": "neutral",
        "system_prompt": (
            "You are a capital flow specialist. Follow the money. Look at institutional "
            "flows, insider trades, congressional activity, dark pool volume, and ETF "
            "flows. Who is buying? Who is selling? Smart money vs dumb money divergence?"
        ),
    },
    {
        "id": "contrarian",
        "name": "Contrarian",
        "bias": "contrarian",
        "system_prompt": (
            "You are a contrarian analyst. Your job is to argue AGAINST whatever "
            "the consensus appears to be. If the data looks bullish, find the hidden "
            "risk. If bearish, find the overlooked opportunity. The crowd is usually "
            "wrong at extremes."
        ),
    },
    {
        "id": "technical_analyst",
        "name": "Technical Analyst",
        "bias": "neutral",
        "system_prompt": (
            "You are a price action specialist. Analyze support/resistance levels, "
            "moving averages, volume patterns, and momentum indicators. Where is price "
            "going in the next 5 days based purely on the chart structure?"
        ),
    },
    {
        "id": "options_whisperer",
        "name": "Options Whisperer",
        "bias": "neutral",
        "system_prompt": (
            "You are an options market specialist. Analyze put/call ratios, max pain, "
            "gamma exposure, IV skew, and dealer positioning. What is the options market "
            "pricing in? Where will dealer hedging push prices over the next 5 days?"
        ),
    },
    {
        "id": "geopolitical_analyst",
        "name": "Geopolitical Analyst",
        "bias": "neutral",
        "system_prompt": (
            "You are a geopolitical risk analyst. Assess how wars, sanctions, elections, "
            "trade policy, and regulatory changes affect markets over the next 5 days. "
            "Focus on specific events with known dates and probable market impact."
        ),
    },
    {
        "id": "narrative_tracker",
        "name": "Narrative Tracker",
        "bias": "neutral",
        "system_prompt": (
            "You are a market narrative specialist. Track the dominant stories driving "
            "market sentiment. Is the narrative shifting? Are media tone and social "
            "momentum aligned or diverging? Narratives drive short-term flows."
        ),
    },
    {
        "id": "risk_manager",
        "name": "Risk Manager",
        "bias": "cautious",
        "system_prompt": (
            "You are the chief risk officer. Your job is to identify what could go wrong. "
            "Tail risks, correlation breakdowns, liquidity traps, crowded trades. "
            "You don't predict direction — you assess whether the current position "
            "is safe or dangerous. Score negative if risk is elevated, positive if low."
        ),
    },
]

# ── Response format required from each agent ────────────────────────────

_RESPONSE_FORMAT = """
Respond with ONLY a JSON object (no markdown, no explanation outside the JSON):
{
    "direction": "bullish" or "bearish" or "neutral",
    "score": <integer from -100 to 100>,
    "confidence": <integer from 0 to 100>,
    "reasoning": "<2-3 sentences explaining your call, citing specific data>",
    "key_signal": "<the single most important data point driving your call>",
    "risk_to_thesis": "<what would prove you wrong>"
}
"""


# ══════════════════════════════════════════════════════════════════════════
# DATA CONTEXT BUILDER
# ══════════════════════════════════════════════════════════════════════════

def _build_market_context(engine: Engine) -> str:
    """Build a concise market data context string for agents to evaluate.

    Pulls from: thesis_scorer, flow_aggregator, regime, options signals.
    Returns a text summary under ~2000 tokens.
    """
    parts: list[str] = []

    try:
        from analysis.thesis_scorer import score_thesis
        thesis = score_thesis(engine)
        parts.append(f"== THESIS SCORER ==")
        parts.append(f"Direction: {thesis['direction']}, Score: {thesis['score']:+.1f}, Conviction: {thesis['conviction']}%")
        parts.append(f"Regime: {thesis.get('regime', '?')}, Bull: {thesis['bull_pct']}%, Bear: {thesis['bear_pct']}%")
        for m in thesis.get("models", []):
            if m["status"] == "active":
                parts.append(f"  {m['name']}: {m['score']:+.1f} ({m['direction']}) — {m['data_point']}")
    except Exception as exc:
        parts.append(f"Thesis scorer unavailable: {exc}")

    try:
        from analysis.flow_aggregator import aggregate_smart_vs_dumb, compute_sector_conviction
        sd = aggregate_smart_vs_dumb(engine, days=14)
        parts.append(f"\n== CAPITAL FLOWS (14d) ==")
        parts.append(f"Smart money: ${sd.get('smart',{}).get('net_flow',0)/1e6:+,.0f}M ({sd.get('smart',{}).get('direction','?')})")
        parts.append(f"Dumb money: ${sd.get('dumb',{}).get('net_flow',0)/1e6:+,.0f}M ({sd.get('dumb',{}).get('direction','?')})")
        parts.append(f"Divergence: {sd.get('divergence','?')}")

        conv = compute_sector_conviction(engine, days=14)
        top_sectors = sorted(conv.items(), key=lambda x: -x[1].get("conviction", 0))[:5]
        parts.append(f"\n== SECTOR CONVICTION ==")
        for s, c in top_sectors:
            parts.append(f"  {s}: {c['conviction']}% conviction, ${c['net_flow']/1e6:+,.0f}M net")
    except Exception as exc:
        parts.append(f"Flow data unavailable: {exc}")

    try:
        with engine.connect() as conn:
            # Congressional signal
            row = conn.execute(text("""
                SELECT signal_type, COUNT(*) FROM signal_sources
                WHERE source_type = 'congressional' AND signal_date >= CURRENT_DATE - 45
                GROUP BY signal_type
            """)).fetchall()
            if row:
                buys = sum(r[1] for r in row if r[0] in ("BUY", "PURCHASE"))
                sells = sum(r[1] for r in row if r[0] in ("SELL", "SALE", "SALE_FULL", "SALE_PARTIAL"))
                parts.append(f"\n== CONGRESSIONAL TRADES (45d) ==")
                parts.append(f"Buys: {buys}, Sells: {sells}, Ratio: {buys/(buys+sells)*100:.0f}% buy" if buys + sells > 0 else "No trades")

            # SPY options
            orow = conn.execute(text("""
                SELECT put_call_ratio, max_pain, spot_price FROM options_daily_signals
                WHERE ticker = 'SPY' ORDER BY signal_date DESC LIMIT 1
            """)).fetchone()
            if orow:
                parts.append(f"\n== SPY OPTIONS ==")
                parts.append(f"PCR: {orow[0]:.2f}, Max Pain: ${orow[1]:.0f}, Spot: ${orow[2]:.0f}")
                gap = (float(orow[2]) - float(orow[1])) / float(orow[2]) * 100
                parts.append(f"Gap from max pain: {gap:+.1f}%")
    except Exception as exc:
        log.warning("Failed to fetch options context for agent arena: {e}", e=exc)

    return "\n".join(parts)


# ══════════════════════════════════════════════════════════════════════════
# AGENT EXECUTION
# ══════════════════════════════════════════════════════════════════════════

def _get_llm_client():
    """Get the best available LLM client for agent execution."""
    # Use the LLM router (handles fallback chain automatically)
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.ORACLE)
        if client and client.is_available:
            return client, "router"
    except Exception as exc:
        log.warning("LLM router unavailable for agent arena: {e}", e=exc)

    # Direct Ollama fallback
    try:
        from ollama.client import get_client
        client = get_client()
        if client:
            return client, "ollama"
    except Exception as exc:
        log.warning("Ollama client unavailable for agent arena: {e}", e=exc)

    return None, None


def _run_single_agent(
    agent: dict,
    context: str,
    llm_client: Any,
    provider: str,
) -> dict[str, Any]:
    """Run one agent against the market context and parse its response."""
    prompt = (
        f"{agent['system_prompt']}\n\n"
        f"Here is today's market data:\n\n{context}\n\n"
        f"Based on this data, what is your 5-day directional call?\n\n"
        f"{_RESPONSE_FORMAT}"
    )

    try:
        if hasattr(llm_client, 'chat'):
            response = llm_client.chat(
                messages=[
                    {"role": "system", "content": agent["system_prompt"]},
                    {"role": "user", "content": f"Market data:\n\n{context}\n\n5-day call?\n\n{_RESPONSE_FORMAT}"},
                ],
                temperature=0.7,
            )
        elif hasattr(llm_client, 'generate'):
            response = llm_client.generate(prompt)
        else:
            return _agent_error(agent, "LLM client has no chat/generate method")

        # Extract text from response
        if isinstance(response, dict):
            text_resp = response.get("message", {}).get("content", "") or response.get("text", "") or response.get("response", "")
        elif isinstance(response, str):
            text_resp = response
        else:
            text_resp = str(response)

        # Parse JSON from response
        parsed = _parse_agent_response(text_resp)
        if parsed:
            return {
                "agent_id": agent["id"],
                "agent_name": agent["name"],
                "bias": agent["bias"],
                **parsed,
                "raw_response": text_resp[:500],
                "provider": provider,
            }
        else:
            return _agent_error(agent, f"Failed to parse JSON from response: {text_resp[:200]}")

    except Exception as exc:
        return _agent_error(agent, str(exc))


def _parse_agent_response(text: str) -> dict | None:
    """Extract JSON from agent response, handling markdown code blocks."""
    # Try direct JSON parse
    text = text.strip()

    # Remove markdown code blocks
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()

    # Find JSON object
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start:end])
            # Validate required fields
            if "direction" in parsed and "score" in parsed:
                return {
                    "direction": str(parsed.get("direction", "neutral")).lower(),
                    "score": max(-100, min(100, int(parsed.get("score", 0)))),
                    "confidence": max(0, min(100, int(parsed.get("confidence", 50)))),
                    "reasoning": str(parsed.get("reasoning", ""))[:500],
                    "key_signal": str(parsed.get("key_signal", ""))[:200],
                    "risk_to_thesis": str(parsed.get("risk_to_thesis", ""))[:200],
                }
        except (json.JSONDecodeError, ValueError, TypeError):
            pass

    return None


def _agent_error(agent: dict, error: str) -> dict:
    """Return a neutral verdict when an agent fails."""
    return {
        "agent_id": agent["id"],
        "agent_name": agent["name"],
        "bias": agent["bias"],
        "direction": "neutral",
        "score": 0,
        "confidence": 0,
        "reasoning": f"Agent error: {error}",
        "key_signal": "none",
        "risk_to_thesis": "none",
        "error": error,
    }


# ══════════════════════════════════════════════════════════════════════════
# ARENA EXECUTION
# ══════════════════════════════════════════════════════════════════════════

def run_arena(engine: Engine) -> dict[str, Any]:
    """Execute one competition round: all agents score the same data.

    Returns the consensus thesis plus individual agent verdicts.
    """
    started = time.time()

    # Build market context
    context = _build_market_context(engine)
    if not context or len(context) < 50:
        return {"error": "Insufficient market data for arena", "agents": []}

    # Get LLM client
    llm_client, provider = _get_llm_client()
    if not llm_client:
        return {"error": "No LLM client available", "agents": []}

    log.info("Agent arena starting: {n} agents via {p}", n=len(AGENTS), p=provider)

    # Run all agents
    verdicts: list[dict] = []
    for agent in AGENTS:
        verdict = _run_single_agent(agent, context, llm_client, provider)
        verdicts.append(verdict)
        log.debug(
            "Agent {name}: {dir} score={s} conf={c}",
            name=agent["name"], dir=verdict["direction"],
            s=verdict["score"], c=verdict["confidence"],
        )

    # Load historical accuracy for weighting
    accuracies = _load_agent_accuracies(engine)

    # Compute weighted consensus
    weighted_sum = 0.0
    weight_total = 0.0
    bull_weight = 0.0
    bear_weight = 0.0

    for v in verdicts:
        conf = v.get("confidence", 0)
        if conf <= 0 or v.get("error"):
            continue

        # Historical accuracy multiplier (0.5 to 2.0)
        acc = accuracies.get(v["agent_id"], 0.5)
        effective_weight = conf * (0.5 + acc * 1.5)

        weighted_sum += v["score"] * effective_weight
        weight_total += effective_weight

        if v["score"] > 5:
            bull_weight += effective_weight
        elif v["score"] < -5:
            bear_weight += effective_weight

        v["historical_accuracy"] = round(acc, 3)
        v["effective_weight"] = round(effective_weight, 1)

    if weight_total > 0:
        consensus_score = weighted_sum / weight_total
        bull_pct = round(bull_weight / weight_total * 100, 1)
        bear_pct = round(bear_weight / weight_total * 100, 1)
    else:
        consensus_score = 0
        bull_pct = 0
        bear_pct = 0

    consensus_direction = (
        "BULLISH" if consensus_score > 10 else
        "BEARISH" if consensus_score < -10 else
        "NEUTRAL"
    )

    # Agreement metric: how much do agents agree?
    directions = [v["direction"] for v in verdicts if not v.get("error")]
    agreement = max(directions.count("bullish"), directions.count("bearish"), directions.count("neutral")) / len(directions) if directions else 0

    elapsed = round(time.time() - started, 1)

    result = {
        "consensus_direction": consensus_direction,
        "consensus_score": round(consensus_score, 1),
        "conviction": round(min(100, abs(consensus_score))),
        "bull_pct": bull_pct,
        "bear_pct": bear_pct,
        "agreement": round(agreement * 100, 1),
        "agents_responded": sum(1 for v in verdicts if not v.get("error")),
        "agents_total": len(verdicts),
        "agents": verdicts,
        "elapsed_seconds": elapsed,
        "provider": provider,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Persist round for scoring
    _persist_arena_round(engine, result)

    log.info(
        "Agent arena complete: {d} score={s:+.1f} agreement={a}% ({t}s)",
        d=consensus_direction, s=consensus_score, a=result["agreement"], t=elapsed,
    )

    return result


# ══════════════════════════════════════════════════════════════════════════
# PERSISTENCE + ACCURACY
# ══════════════════════════════════════════════════════════════════════════

def _persist_arena_round(engine: Engine, result: dict) -> None:
    """Save arena round to thesis_snapshots for accuracy tracking."""
    try:
        model_states = {}
        for v in result.get("agents", []):
            model_states[v["agent_id"]] = {
                "direction": v["direction"],
                "score": v["score"],
                "confidence": v["confidence"],
                "reasoning": v.get("reasoning", ""),
                "key_signal": v.get("key_signal", ""),
            }

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO thesis_snapshots
                (overall_direction, conviction, model_states, narrative)
                VALUES (:dir, :conv, :ms, :narr)
            """), {
                "dir": result["consensus_direction"].lower(),
                "conv": result["conviction"] / 100,
                "ms": json.dumps(model_states),
                "narr": f"Agent arena: {result['consensus_direction']} "
                        f"score={result['consensus_score']:+.1f} "
                        f"agreement={result['agreement']}% "
                        f"({result['agents_responded']}/{result['agents_total']} agents)",
            })
    except Exception as exc:
        log.warning("Failed to persist arena round: {e}", e=str(exc))


def _load_agent_accuracies(engine: Engine) -> dict[str, float]:
    """Load per-agent win rates from scored thesis_snapshots.

    Returns {agent_id: accuracy} where 0.5 = coin flip.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT model_states, outcome FROM thesis_snapshots
                WHERE outcome IS NOT NULL AND model_states IS NOT NULL
                ORDER BY timestamp DESC LIMIT 200
            """)).fetchall()

        if not rows:
            return {}

        hits: dict[str, int] = {}
        total: dict[str, int] = {}

        for row in rows:
            states = row[0] if isinstance(row[0], dict) else json.loads(row[0]) if row[0] else {}
            outcome = row[1]

            for agent_id, state in states.items():
                direction = state.get("direction", "neutral")
                if direction == "neutral":
                    continue
                total[agent_id] = total.get(agent_id, 0) + 1
                if outcome == "correct":
                    hits[agent_id] = hits.get(agent_id, 0) + 1
                elif outcome == "partial":
                    hits[agent_id] = hits.get(agent_id, 0) + 0.5

        return {
            k: hits.get(k, 0) / total[k]
            for k in total
            if total[k] >= 3
        }

    except Exception:
        return {}


def get_agent_leaderboard(engine: Engine) -> list[dict[str, Any]]:
    """Rank all agents by historical accuracy.

    Returns sorted list of {agent_id, agent_name, accuracy, total_scored, rank}.
    """
    accuracies = _load_agent_accuracies(engine)
    agent_map = {a["id"]: a["name"] for a in AGENTS}

    leaderboard = []
    for agent_id, acc in sorted(accuracies.items(), key=lambda x: -x[1]):
        leaderboard.append({
            "agent_id": agent_id,
            "agent_name": agent_map.get(agent_id, agent_id),
            "accuracy": round(acc, 3),
            "rank": len(leaderboard) + 1,
        })

    return leaderboard
