"""
GRID Hourly Market Briefing Engine.

Generates comprehensive market condition reports using Ollama with
GRID's knowledge base. Pulls latest market data, constructs a
structured prompt with real data context, and produces an AI-powered
market briefing every hour.

Can run as a standalone scheduler or be called from the API.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import schedule
from loguru import logger as log

# Output directory for saved briefings
_BRIEFING_DIR = Path(__file__).parent.parent / "outputs" / "market_briefings"


class MarketBriefingEngine:
    """Generates AI-powered market condition briefings using Ollama.

    Pulls latest data from GRID's data stores and feeds it as context
    to the Ollama model along with GRID's knowledge documents for
    deeply informed market analysis.

    Attributes:
        ollama_client: OllamaClient instance.
        db_engine: Optional SQLAlchemy engine for live data access.
    """

    def __init__(
        self,
        ollama_client: Any = None,
        db_engine: Any = None,
    ) -> None:
        self.ollama = ollama_client
        self.engine = db_engine

        if self.ollama is None:
            # Prefer llama.cpp (direct), fall back to Ollama wrapper
            try:
                from llamacpp.client import get_client as get_llamacpp
                client = get_llamacpp()
                if client.is_available:
                    self.ollama = client
                else:
                    from ollama.client import get_client
                    self.ollama = get_client()
            except Exception:
                from ollama.client import get_client
                self.ollama = get_client()

        _BRIEFING_DIR.mkdir(parents=True, exist_ok=True)
        log.info("MarketBriefingEngine initialised")

    # ------------------------------------------------------------------
    # Data gathering
    # ------------------------------------------------------------------
    def _gather_market_snapshot(self) -> dict[str, Any]:
        """Gather latest available market data for the briefing.

        Returns:
            dict: Structured market data snapshot.
        """
        snapshot: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "date": date.today().isoformat(),
            "equities": {},
            "rates": {},
            "credit": {},
            "volatility": {},
            "commodities": {},
            "fx": {},
            "macro": {},
        }

        if self.engine is None:
            return snapshot

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                # Get latest values from raw_series for key tickers
                key_series = {
                    "equities": [
                        "YF:^GSPC:close", "YF:^DJI:close", "YF:^IXIC:close",
                        "YF:^RUT:close", "YF:^GSPC:volume",
                    ],
                    "rates": [
                        "FRED:DFF", "FRED:T10Y2Y", "FRED:T10Y3M",
                        "FRED:DGS10", "FRED:DGS2",
                    ],
                    "credit": [
                        "YF:HYG:close", "YF:LQD:close", "YF:JNK:close",
                        "YF:EMB:close",
                    ],
                    "volatility": [
                        "YF:^VIX:close", "YF:^VIX3M:close", "YF:^VIX9D:close",
                    ],
                    "commodities": [
                        "YF:GC=F:close", "YF:SI=F:close", "YF:CL=F:close",
                        "YF:HG=F:close", "YF:GLD:close",
                    ],
                    "fx": [
                        "YF:UUP:close", "YF:FXE:close", "YF:FXY:close",
                        "YF:EEM:close",
                    ],
                }

                for category, series_ids in key_series.items():
                    for sid in series_ids:
                        row = conn.execute(
                            text(
                                "SELECT value, obs_date FROM raw_series "
                                "WHERE series_id = :sid "
                                "ORDER BY obs_date DESC LIMIT 1"
                            ),
                            {"sid": sid},
                        ).fetchone()
                        if row:
                            # Extract a clean label
                            parts = sid.split(":")
                            label = parts[1] if len(parts) > 1 else sid
                            snapshot[category][label] = {
                                "value": round(row[0], 4),
                                "date": str(row[1]),
                            }

                # Get latest feature values from resolved_series
                feature_rows = conn.execute(
                    text(
                        "SELECT fr.name, rs.value, rs.obs_date "
                        "FROM resolved_series rs "
                        "JOIN feature_registry fr ON fr.id = rs.feature_id "
                        "WHERE fr.model_eligible = TRUE "
                        "AND rs.obs_date = ("
                        "  SELECT MAX(obs_date) FROM resolved_series "
                        "  WHERE feature_id = rs.feature_id"
                        ") "
                        "ORDER BY fr.name"
                    )
                ).fetchall()

                snapshot["features"] = {
                    row[0]: {"value": round(row[1], 4), "date": str(row[2])}
                    for row in feature_rows
                }

                # Get latest regime inference if available
                regime_row = conn.execute(
                    text(
                        "SELECT inferred_state, state_confidence, "
                        "transition_probability, contradiction_flags, "
                        "grid_recommendation, decision_timestamp "
                        "FROM decision_journal "
                        "ORDER BY decision_timestamp DESC LIMIT 1"
                    )
                ).fetchone()

                if regime_row:
                    snapshot["latest_regime"] = {
                        "state": regime_row[0],
                        "confidence": round(regime_row[1], 4),
                        "transition_prob": round(regime_row[2], 4),
                        "contradictions": regime_row[3],
                        "recommendation": regime_row[4],
                        "timestamp": str(regime_row[5]),
                    }

        except Exception as exc:
            log.warning("Could not gather market snapshot: {err}", err=str(exc))

        return snapshot

    def _build_data_context(self, snapshot: dict[str, Any]) -> str:
        """Convert market snapshot to a readable text block for the LLM.

        Parameters:
            snapshot: Market data snapshot dict.

        Returns:
            str: Formatted data context.
        """
        lines: list[str] = []
        lines.append(f"## Market Data Snapshot — {snapshot['timestamp']}")
        lines.append("")

        for category in ["equities", "rates", "credit", "volatility", "commodities", "fx"]:
            data = snapshot.get(category, {})
            if data:
                lines.append(f"### {category.upper()}")
                for label, info in data.items():
                    lines.append(f"- {label}: {info['value']} (as of {info['date']})")
                lines.append("")

        # Feature values — limit to top 30 most recent to stay within context
        features = snapshot.get("features", {})
        if features:
            lines.append(f"### GRID FEATURES ({len(features)} total, showing top 30)")
            shown = 0
            for name, info in features.items():
                if shown >= 30:
                    break
                lines.append(f"- {name}: {info['value']} (as of {info['date']})")
                shown += 1
            lines.append("")

        # Latest regime
        regime = snapshot.get("latest_regime")
        if regime:
            lines.append("### LATEST REGIME INFERENCE")
            lines.append(f"- State: {regime['state']}")
            lines.append(f"- Confidence: {regime['confidence']}")
            lines.append(f"- Transition Probability: {regime['transition_prob']}")
            lines.append(f"- Recommendation: {regime['recommendation']}")
            lines.append(f"- Timestamp: {regime['timestamp']}")
            if regime.get("contradictions"):
                lines.append(f"- Contradictions: {json.dumps(regime['contradictions'])}")
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Briefing generation
    # ------------------------------------------------------------------
    def generate_briefing(
        self,
        briefing_type: str = "hourly",
        save: bool = True,
    ) -> dict[str, Any]:
        """Generate a market conditions briefing.

        Parameters:
            briefing_type: One of 'hourly', 'daily', 'weekly'.
            save: Whether to save the briefing to disk.

        Returns:
            dict: Briefing result with keys 'content', 'snapshot',
                  'timestamp', 'type'.
        """
        log.info("Generating {t} market briefing", t=briefing_type)

        # Gather live data
        snapshot = self._gather_market_snapshot()
        data_context = self._build_data_context(snapshot)

        # Build the prompt
        system_prompt = self._get_system_prompt(briefing_type)
        user_prompt = self._get_user_prompt(briefing_type, data_context)

        # Generate via Ollama with knowledge injection
        knowledge_docs = [
            "06_market_analysis_framework",
            "07_economic_mechanisms",
        ]

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        content = self.ollama.chat(
            messages=messages,
            temperature=0.4,
            num_predict=1000,
            system_knowledge=knowledge_docs,
        )

        if content is None:
            content = self._generate_fallback_briefing(snapshot)
            log.warning("Ollama unavailable — using fallback briefing")

        result = {
            "content": content,
            "snapshot": snapshot,
            "timestamp": datetime.now().isoformat(),
            "type": briefing_type,
        }

        if save:
            self._save_briefing(result)

        log.info(
            "{t} briefing generated — {n} chars",
            t=briefing_type,
            n=len(content),
        )
        return result

    def _get_system_prompt(self, briefing_type: str) -> str:
        """Build the system prompt for the briefing.

        Parameters:
            briefing_type: Type of briefing.

        Returns:
            str: System prompt.
        """
        base = (
            "You are GRID's market analyst AI. You produce precise, actionable "
            "market condition briefings for a systematic trading operation. "
            "You have deep knowledge of macroeconomics, cross-asset dynamics, "
            "regime detection, and quantitative finance. "
            "You think in terms of economic transmission mechanisms, not just "
            "statistical patterns. You are direct, concise, and honest about "
            "uncertainty. You never fabricate data."
        )

        if briefing_type == "hourly":
            return (
                f"{base}\n\n"
                "This is an HOURLY briefing. Focus on:\n"
                "1. Current regime classification and confidence\n"
                "2. Any signal changes in the last hour\n"
                "3. Cross-asset contradiction scan\n"
                "4. Key levels to watch\n"
                "5. One-paragraph actionable summary\n"
                "Keep it tight — 400-600 words max."
            )
        elif briefing_type == "daily":
            return (
                f"{base}\n\n"
                "This is a DAILY briefing. Provide:\n"
                "1. Full regime assessment with confidence\n"
                "2. Complete cross-asset signal check (all families)\n"
                "3. International context (China, OECD, Korea, Euro)\n"
                "4. Contradiction analysis\n"
                "5. Three-scenario framework (base/bull/bear)\n"
                "6. Actionable takeaway with time horizon\n"
                "Target 800-1200 words."
            )
        else:  # weekly
            return (
                f"{base}\n\n"
                "This is a WEEKLY briefing. Deliver:\n"
                "1. Comprehensive regime analysis with historical comparison\n"
                "2. Full feature family walkthrough with trends\n"
                "3. International macro deep dive\n"
                "4. Derived signal analysis (credit impulse, ECI, patent velocity)\n"
                "5. Risk assessment and tail risk scenarios\n"
                "6. Week-ahead playbook\n"
                "Target 1500-2500 words."
            )

    def _get_user_prompt(self, briefing_type: str, data_context: str) -> str:
        """Build the user prompt with embedded data.

        Parameters:
            briefing_type: Type of briefing.
            data_context: Formatted market data context.

        Returns:
            str: User prompt.
        """
        now = datetime.now()
        return (
            f"Generate the {briefing_type} GRID market conditions briefing.\n\n"
            f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            f"Day of week: {now.strftime('%A')}\n"
            f"Market hours context: "
            f"{'US markets are open' if 9 <= now.hour <= 16 and now.weekday() < 5 else 'US markets are closed'}\n\n"
            f"{data_context}\n\n"
            f"Analyze these conditions using the GRID framework. Follow the "
            f"market analysis framework structure. Be specific about feature "
            f"values and what they indicate. Flag any contradictions between "
            f"signal families. Provide a clear regime classification with "
            f"confidence level."
        )

    def _generate_fallback_briefing(self, snapshot: dict[str, Any]) -> str:
        """Generate a basic data-driven briefing when Ollama is unavailable.

        Parameters:
            snapshot: Market data snapshot.

        Returns:
            str: Fallback briefing text.
        """
        lines = [
            f"# GRID Market Briefing — {snapshot['timestamp']}",
            "",
            "**Note: AI analysis unavailable. Data summary only.**",
            "",
        ]

        for category in ["equities", "rates", "credit", "volatility", "commodities", "fx"]:
            data = snapshot.get(category, {})
            if data:
                lines.append(f"## {category.title()}")
                for label, info in data.items():
                    lines.append(f"- **{label}**: {info['value']} ({info['date']})")
                lines.append("")

        regime = snapshot.get("latest_regime")
        if regime:
            lines.append("## Latest Regime")
            lines.append(f"- State: **{regime['state']}**")
            lines.append(f"- Confidence: {regime['confidence']}")
            lines.append(f"- Recommendation: {regime['recommendation']}")
            lines.append("")

        lines.append("---")
        lines.append("*Ollama offline — connect Ollama for AI-powered analysis*")

        return "\n".join(lines)

    def _save_briefing(self, result: dict[str, Any]) -> None:
        """Save a briefing to disk as a markdown file.

        Parameters:
            result: Briefing result dict.
        """
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{result['type']}_{ts}.md"
        filepath = _BRIEFING_DIR / filename

        with filepath.open("w", encoding="utf-8") as f:
            f.write(result["content"])
            f.write(f"\n\n---\n*Generated: {result['timestamp']}*\n")

        log.debug("Briefing saved to {p}", p=filepath)

    # ------------------------------------------------------------------
    # Latest briefing retrieval
    # ------------------------------------------------------------------
    def get_latest_briefing(self, briefing_type: str = "hourly") -> str | None:
        """Read the most recent briefing file of the given type.

        Parameters:
            briefing_type: Type prefix to filter by.

        Returns:
            str: Briefing content, or None if not found.
        """
        pattern = f"{briefing_type}_*.md"
        files = sorted(_BRIEFING_DIR.glob(pattern), reverse=True)
        if not files:
            return None
        return files[0].read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def start_hourly_briefings(db_engine: Any = None) -> None:
    """Start the hourly market briefing scheduler.

    Generates an hourly briefing during US market hours (8 AM - 5 PM ET,
    Monday-Friday) and a daily summary at 5:30 PM ET on weekdays.

    Parameters:
        db_engine: Optional SQLAlchemy engine for live data.
    """
    engine_instance = MarketBriefingEngine(db_engine=db_engine)

    log.info("Starting hourly market briefing scheduler")

    # Hourly briefings every hour on the hour, market hours
    for hour in range(8, 18):
        time_str = f"{hour:02d}:00"
        schedule.every().monday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )
        schedule.every().tuesday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )
        schedule.every().wednesday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )
        schedule.every().thursday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )
        schedule.every().friday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )

    # Daily summary at 5:30 PM ET on weekdays
    schedule.every().monday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )
    schedule.every().tuesday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )
    schedule.every().wednesday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )
    schedule.every().thursday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )
    schedule.every().friday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )

    # Weekly summary Sunday evening
    schedule.every().sunday.at("18:00").do(
        engine_instance.generate_briefing, briefing_type="weekly"
    )

    # Generate an initial briefing immediately
    log.info("Generating initial briefing...")
    engine_instance.generate_briefing(briefing_type="hourly")

    log.info(
        "Briefing scheduler configured — hourly (8AM-5PM M-F), "
        "daily (5:30PM M-F), weekly (6PM Sun)"
    )

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("Briefing scheduler stopped (KeyboardInterrupt)")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        # Generate a single briefing and print it
        briefing_type = sys.argv[2] if len(sys.argv) > 2 else "hourly"
        engine = MarketBriefingEngine()
        result = engine.generate_briefing(briefing_type=briefing_type)
        print(result["content"])
    elif len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        # Run as a background scheduler
        try:
            from db import get_engine
            db_eng = get_engine()
        except Exception:
            db_eng = None
            log.warning("No database — briefings will use limited data")
        start_hourly_briefings(db_engine=db_eng)
    else:
        print("Usage:")
        print("  python -m ollama.market_briefing --once [hourly|daily|weekly]")
        print("  python -m ollama.market_briefing --daemon")
