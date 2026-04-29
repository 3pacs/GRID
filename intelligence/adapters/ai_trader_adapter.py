"""
GRID Intelligence — AI-Trader Signal Adapter.

Polls top-performing AI-Trader agent signals from an AI-Trader instance
(HKUDS/AI-Trader) and converts them into RegisteredSignal objects for
the GRID oracle ensemble.

AI-Trader is a multi-agent trading signal marketplace where AI agents
publish signals, debate strategies, and copy-trade each other. This
adapter consumes the top agent signals filtered by PnL leaderboard.

Signal types produced:
  - DIRECTIONAL: buy/sell/short/cover → bullish/bearish per ticker
  - MAGNITUDE:   consensus strength (agreement among top agents)

Validity window: now → now + 4 hours.  Refresh: every 4 hours.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from config import settings
from intelligence.signal_registry import (
    Direction,
    RegisteredSignal,
    SignalType,
    make_signal_id,
)

_SOURCE_MODULE = "ai_trader"
_VALID_HOURS = 4.0
_REFRESH_HOURS = 4.0
_REQUEST_TIMEOUT = 30

# Map AI-Trader actions → GRID direction
_ACTION_MAP: dict[str, Direction] = {
    "buy":   Direction.BULLISH,
    "cover": Direction.BULLISH,
    "sell":  Direction.BEARISH,
    "short": Direction.BEARISH,
}

# Map AI-Trader actions → numeric value for signal magnitude
_ACTION_VALUE: dict[str, float] = {
    "buy":    1.0,
    "cover":  0.6,
    "sell":  -1.0,
    "short": -0.6,
}


def _sid(*parts: str) -> str:
    """Deterministic short signal ID from parts."""
    return hashlib.sha1(
        ":".join(parts).encode(),
        usedforsecurity=False,
    ).hexdigest()[:16]


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


class AITraderAdapter:
    """Adapter that polls an AI-Trader instance for top agent signals."""

    @property
    def source_module(self) -> str:
        return _SOURCE_MODULE

    @property
    def refresh_interval_hours(self) -> float:
        return _REFRESH_HOURS

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        """Fetch signals from AI-Trader and convert to RegisteredSignal."""
        if not settings.AI_TRADER_ENABLED:
            return []

        base_url = settings.AI_TRADER_BASE_URL.rstrip("/")
        if not base_url:
            log.warning("AITraderAdapter: AI_TRADER_BASE_URL not configured")
            return []

        now = datetime.now(timezone.utc)
        valid_until = now + timedelta(hours=_VALID_HOURS)
        signals: list[RegisteredSignal] = []

        try:
            # Step 1: Get top agents from leaderboard
            top_agents = self._fetch_leaderboard(base_url)
            if not top_agents:
                log.info("AITraderAdapter: no agents on leaderboard")
                return []

            # Step 2: Get recent signals from top agents
            raw_signals = self._fetch_signal_feed(base_url)
            if not raw_signals:
                log.info("AITraderAdapter: no signals in feed")
                return []

            # Step 3: Filter to signals from top agents only
            top_agent_ids = {a["agent_id"] for a in top_agents if "agent_id" in a}
            filtered = [s for s in raw_signals if s.get("agent_id") in top_agent_ids]

            # Step 4: Convert to RegisteredSignals
            # Group by ticker for consensus computation
            ticker_signals: dict[str, list[dict[str, Any]]] = {}
            for sig in filtered:
                ticker = sig.get("symbol", "").upper()
                if not ticker:
                    continue
                ticker_signals.setdefault(ticker, []).append(sig)

            for ticker, sigs in ticker_signals.items():
                # Per-signal directional signals
                for sig in sigs:
                    action = sig.get("action", "").lower()
                    direction = _ACTION_MAP.get(action)
                    if direction is None:
                        continue

                    agent_id = sig.get("agent_id", "unknown")
                    signal_ts = sig.get("executed_at") or sig.get("created_at") or now.isoformat()
                    value = _ACTION_VALUE.get(action, 0.0)

                    # Confidence from agent's leaderboard PnL rank
                    agent_info = next((a for a in top_agents if a.get("agent_id") == agent_id), {})
                    agent_rank = agent_info.get("rank", len(top_agents))
                    # Top agent gets 0.85 confidence, decays to 0.45 by rank 20
                    confidence = _clamp(0.85 - (agent_rank - 1) * 0.02, 0.45, 0.85)

                    signals.append(RegisteredSignal(
                        signal_id=make_signal_id(
                            _SOURCE_MODULE,
                            f"{ticker}:{action}:{agent_id}:{now.date().isoformat()}",
                        ),
                        source_module=_SOURCE_MODULE,
                        signal_type=SignalType.DIRECTIONAL,
                        ticker=ticker,
                        direction=direction,
                        value=value,
                        confidence=confidence,
                        valid_from=now,
                        valid_until=valid_until,
                        freshness_hours=0.0,
                        metadata={
                            "agent_id": agent_id,
                            "agent_name": agent_info.get("name", ""),
                            "action": action,
                            "price": sig.get("price"),
                            "quantity": sig.get("quantity"),
                            "market": sig.get("market", "stocks"),
                            "agent_pnl": agent_info.get("pnl"),
                            "agent_rank": agent_rank,
                        },
                        provenance=f"ai_trader:{agent_id}:{ticker}",
                    ))

                # Consensus signal: agreement among top agents for this ticker
                if len(sigs) >= 2:
                    bullish_count = sum(
                        1 for s in sigs if _ACTION_MAP.get(s.get("action", "").lower()) == Direction.BULLISH
                    )
                    bearish_count = sum(
                        1 for s in sigs if _ACTION_MAP.get(s.get("action", "").lower()) == Direction.BEARISH
                    )
                    total = bullish_count + bearish_count
                    if total >= 2:
                        consensus = (bullish_count - bearish_count) / total
                        direction = Direction.BULLISH if consensus > 0 else Direction.BEARISH if consensus < 0 else Direction.NEUTRAL
                        coherence = abs(consensus)

                        signals.append(RegisteredSignal(
                            signal_id=make_signal_id(
                                _SOURCE_MODULE,
                                f"{ticker}:consensus:{now.date().isoformat()}",
                            ),
                            source_module=_SOURCE_MODULE,
                            signal_type=SignalType.MAGNITUDE,
                            ticker=ticker,
                            direction=direction,
                            value=round(consensus, 4),
                            z_score=round(consensus * 2, 2),
                            confidence=_clamp(coherence * 0.8, 0.3, 0.8),
                            valid_from=now,
                            valid_until=valid_until,
                            freshness_hours=0.0,
                            metadata={
                                "bullish_agents": bullish_count,
                                "bearish_agents": bearish_count,
                                "total_agents": total,
                                "coherence": round(coherence, 4),
                            },
                            provenance=f"ai_trader:consensus:{ticker}",
                        ))

            log.info(
                "AITraderAdapter: produced {n} signals from {t} tickers, {a} agents",
                n=len(signals), t=len(ticker_signals), a=len(top_agent_ids),
            )
        except requests.RequestException as exc:
            log.error("AITraderAdapter: HTTP error - {e}", e=exc)
        except Exception as exc:
            log.error("AITraderAdapter: unexpected error - {e}", e=exc)

        return signals

    # ── Private helpers ──────────────────────────────────────────────

    def _fetch_leaderboard(self, base_url: str) -> list[dict[str, Any]]:
        """Fetch top agents from AI-Trader leaderboard."""
        url = f"{base_url}/api/leaderboard/position-pnl"
        headers = self._auth_headers()
        try:
            resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            agents = data if isinstance(data, list) else data.get("data", data.get("agents", []))
            # Take top N agents by PnL
            top_n = settings.AI_TRADER_TOP_AGENTS
            result = []
            for i, agent in enumerate(agents[:top_n]):
                result.append({
                    "agent_id": str(agent.get("agent_id", agent.get("id", ""))),
                    "name": agent.get("name", agent.get("agent_name", "")),
                    "pnl": agent.get("unrealized_pnl", agent.get("pnl", 0)),
                    "rank": i + 1,
                })
            return result
        except requests.RequestException as exc:
            log.warning("AITraderAdapter: leaderboard fetch failed - {e}", e=exc)
            return []

    def _fetch_signal_feed(self, base_url: str) -> list[dict[str, Any]]:
        """Fetch recent real-time signals from AI-Trader feed."""
        url = f"{base_url}/api/signals/feed"
        headers = self._auth_headers()
        params = {
            "message_type": "realtime",
            "sort": "recent",
        }
        market = settings.AI_TRADER_MARKET_FILTER
        if market:
            params["market"] = market

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            signals = data if isinstance(data, list) else data.get("data", data.get("signals", []))
            return signals[:settings.AI_TRADER_MAX_SIGNALS]
        except requests.RequestException as exc:
            log.warning("AITraderAdapter: signal feed fetch failed - {e}", e=exc)
            return []

    def _auth_headers(self) -> dict[str, str]:
        """Build auth headers if API key is configured."""
        headers: dict[str, str] = {"Accept": "application/json"}
        if settings.AI_TRADER_API_KEY:
            headers["Authorization"] = f"Bearer {settings.AI_TRADER_API_KEY}"
        return headers
