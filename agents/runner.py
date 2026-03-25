"""
TradingAgents orchestration runner.

Fetches GRID context, injects it into agent prompts, runs the
multi-agent deliberation, and logs results to both the agent_runs
table and the decision journal.
"""

from __future__ import annotations

import json
import time
import concurrent.futures
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from agents.adapter import parse_agent_decision
from agents.config import build_agent_config
from agents.context import GRIDContext
from agents.progress import emit_progress, emit_run_complete
from config import settings
from outputs.llm_logger import log_agent_deliberation


class AgentRunner:
    """Runs TradingAgents with GRID regime context.

    Attributes:
        engine: SQLAlchemy database engine.
        context_builder: GRIDContext for regime intelligence.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.context_builder = GRIDContext(db_engine)
        log.info("AgentRunner initialised")

    def run(
        self,
        ticker: str | None = None,
        as_of_date: date | None = None,
    ) -> dict[str, Any]:
        """Execute a full TradingAgents deliberation run.

        Parameters:
            ticker: Stock ticker to analyse (default from config).
            as_of_date: Decision date (default: today).

        Returns:
            dict: Full run result including agent deliberation and
                  references to journal entries.
        """
        if not settings.AGENTS_ENABLED:
            return {"error": "TradingAgents integration is disabled", "enabled": False}

        ticker = ticker or settings.AGENTS_DEFAULT_TICKER
        if as_of_date is None:
            as_of_date = date.today()

        log.info(
            "Starting agent run — ticker={t}, date={d}",
            t=ticker,
            d=as_of_date,
        )

        start_time = time.time()

        emit_progress(None, "context", ticker, "Building GRID regime context", 0.1)

        # 1. Build GRID context
        grid_context = self.context_builder.build(as_of_date)
        regime_state = grid_context["regime_state"]
        confidence = grid_context["confidence"]

        emit_progress(None, "config", ticker, f"Regime: {regime_state} ({confidence:.0%})", 0.2)

        # 2. Run TradingAgents
        agent_config = build_agent_config()
        llm_provider = agent_config.get("llm_provider", "unknown")
        llm_model = agent_config.get("deep_think_llm", "unknown")

        emit_progress(None, "analysts", ticker, "Running analyst agents (fundamentals, sentiment, news, technical)", 0.3)

        # Run agents with a timeout to prevent indefinite hangs
        agent_timeout = 300  # 5 minutes max
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    self._run_agents,
                    ticker, as_of_date, grid_context["prompt_context"], agent_config,
                )
                decision_raw = future.result(timeout=agent_timeout)
            emit_progress(None, "parsing", ticker, "Parsing agent deliberation", 0.8)
            parsed = parse_agent_decision(decision_raw)
            error = None
        except concurrent.futures.TimeoutError:
            log.error("Agent run timed out after {t}s", t=agent_timeout)
            parsed = parse_agent_decision(None)
            parsed["decision_reasoning"] = f"Agent run timed out after {agent_timeout}s"
            error = f"Timeout after {agent_timeout}s"
        except Exception as exc:
            log.error("Agent run failed: {e}", e=str(exc))
            parsed = parse_agent_decision(None)
            parsed["decision_reasoning"] = f"Agent run failed: {exc}"
            error = str(exc)

        duration = round(time.time() - start_time, 2)

        emit_progress(None, "journal", ticker, "Logging to decision journal", 0.9)

        # 3. Log to decision journal
        journal_id = self._log_to_journal(
            regime_state=regime_state,
            confidence=confidence,
            grid_action=grid_context["suggested_action"],
            agent_decision=parsed["final_decision"],
            agent_reasoning=parsed["decision_reasoning"],
        )

        # 4. Log to agent_runs table
        run_id = self._save_agent_run(
            ticker=ticker,
            as_of_date=as_of_date,
            regime_state=regime_state,
            confidence=confidence,
            parsed=parsed,
            journal_id=journal_id,
            llm_provider=llm_provider,
            llm_model=llm_model,
            duration=duration,
            error=error,
        )

        log.info(
            "Agent run complete — id={id}, decision={d}, duration={dur}s",
            id=run_id,
            d=parsed["final_decision"],
            dur=duration,
        )

        # Log full deliberation to timestamped markdown
        try:
            log_agent_deliberation(
                ticker=ticker,
                regime_state=regime_state,
                confidence=confidence,
                parsed=parsed,
                provider=llm_provider,
                model=llm_model,
                duration=duration,
            )
        except Exception as exc:
            log.warning("Failed to log agent deliberation to file: {e}", e=str(exc))

        # Send agent report newsletter
        try:
            from alerts.email import send_agent_report
            send_agent_report(
                ticker=ticker,
                decision=parsed["final_decision"],
                reasoning=parsed["decision_reasoning"],
                regime_state=regime_state,
                confidence=confidence,
                duration=duration,
            )
        except Exception as exc:
            log.debug("Agent report email skipped: {e}", e=str(exc))

        emit_run_complete(run_id, ticker, parsed["final_decision"], duration, error)

        return {
            "run_id": run_id,
            "ticker": ticker,
            "as_of_date": as_of_date.isoformat(),
            "regime_state": regime_state,
            "regime_confidence": confidence,
            "final_decision": parsed["final_decision"],
            "decision_reasoning": parsed["decision_reasoning"],
            "analyst_reports": parsed["analyst_reports"],
            "bull_bear_debate": parsed["bull_bear_debate"],
            "risk_assessment": parsed["risk_assessment"],
            "decision_journal_id": journal_id,
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "duration_seconds": duration,
            "error": error,
        }

    def _run_agents(
        self,
        ticker: str,
        as_of_date: date,
        prompt_context: str,
        agent_config: dict[str, Any],
    ) -> Any:
        """Import and run TradingAgents, falling back to single-model analysis.

        Attempts TradingAgents multi-agent framework first. If the package
        is not installed, falls back to a direct llama.cpp single-model
        analysis that performs fundamental, sentiment, and risk evaluation
        in one prompt.
        """
        try:
            from tradingagents.graph.trading_graph import TradingAgentsGraph
        except ImportError:
            log.info(
                "tradingagents package not installed — using single-model llama.cpp analysis"
            )
            return self._run_single_model_analysis(ticker, as_of_date, prompt_context)

        # Inject GRID context into the config's system prompt addition
        agent_config["additional_context"] = prompt_context

        ta = TradingAgentsGraph(debug=False, config=agent_config)
        _, decision = ta.propagate(ticker, as_of_date.isoformat())
        return decision

    def _run_single_model_analysis(
        self,
        ticker: str,
        as_of_date: date,
        prompt_context: str,
    ) -> dict[str, Any]:
        """Run a single-model analysis via llama.cpp as a fallback.

        Sends the GRID regime context to the local Hermes model and asks
        for a structured BUY/SELL/HOLD decision with reasoning.  This
        provides useful analysis even without the full multi-agent
        TradingAgents framework.

        Parameters:
            ticker: Stock ticker to analyse.
            as_of_date: Decision date.
            prompt_context: GRID regime context string.

        Returns:
            dict: Decision with reasoning, analyst reports, and risk assessment.
        """
        from llamacpp.client import get_client

        client = get_client()
        if not client.is_available:
            log.warning("llama.cpp server not available — returning default HOLD")
            return {
                "action": "HOLD",
                "reasoning": (
                    "LLM server (llama.cpp) is not available. "
                    "Start llama-server at localhost:8080 to enable agent analysis."
                ),
                "analyst_reports": {"note": "LLM unavailable"},
                "debate": {"note": "LLM unavailable"},
                "risk": {"note": "LLM unavailable"},
            }

        emit_progress(None, "llm", ticker, "Sending analysis prompt to local LLM", 0.4)

        system_prompt = (
            "You are a senior macro trading analyst for the GRID intelligence system. "
            "Analyse the given ticker using the regime context provided. "
            "Give a clear BUY, SELL, or HOLD recommendation with structured reasoning.\n\n"
            "Respond in this exact format:\n"
            "DECISION: [BUY|SELL|HOLD]\n"
            "CONFIDENCE: [LOW|MEDIUM|HIGH]\n\n"
            "FUNDAMENTAL ANALYSIS:\n[Your analysis of macro conditions and regime]\n\n"
            "SENTIMENT & POSITIONING:\n[Your read on market sentiment given the regime]\n\n"
            "RISK ASSESSMENT:\n[Key risks to this position]\n\n"
            "REASONING:\n[2-3 sentence summary of why this decision]"
        )

        user_prompt = (
            f"Analyse {ticker} as of {as_of_date.isoformat()}.\n\n"
            f"{prompt_context}\n\n"
            f"Based on this regime intelligence, what is your recommendation for {ticker}? "
            f"Consider the current regime state, confidence level, and macro features."
        )

        response = client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            num_predict=1500,
        )

        emit_progress(None, "parsing", ticker, "Parsing LLM response", 0.7)

        if response is None:
            log.warning("llama.cpp returned no response — defaulting to HOLD")
            return {
                "action": "HOLD",
                "reasoning": "LLM returned no response",
                "analyst_reports": {"note": "No LLM response"},
                "debate": {"note": "single-model fallback"},
                "risk": {"note": "No LLM response"},
            }

        # Parse structured sections from the response
        result = self._parse_single_model_response(response)
        log.info(
            "Single-model analysis complete — decision={d}",
            d=result.get("action", "HOLD"),
        )
        return result

    @staticmethod
    def _parse_single_model_response(response: str) -> dict[str, Any]:
        """Parse the structured response from single-model analysis.

        Parameters:
            response: Raw LLM text response.

        Returns:
            dict: Parsed decision with analyst reports and risk data.
        """
        lines = response.strip().split("\n")
        result: dict[str, Any] = {
            "action": "HOLD",
            "reasoning": response,
            "analyst_reports": {},
            "debate": {"note": "single-model analysis (no multi-agent debate)"},
            "risk": {},
        }

        # Extract DECISION line
        for line in lines:
            upper = line.upper().strip()
            if upper.startswith("DECISION:"):
                decision_text = upper.replace("DECISION:", "").strip()
                if "BUY" in decision_text:
                    result["action"] = "BUY"
                elif "SELL" in decision_text:
                    result["action"] = "SELL"
                else:
                    result["action"] = "HOLD"
                break

        # Extract sections
        sections: dict[str, list[str]] = {}
        current_section: str | None = None
        for line in lines:
            upper = line.upper().strip()
            if upper.startswith("FUNDAMENTAL ANALYSIS:"):
                current_section = "fundamental"
                sections[current_section] = []
            elif upper.startswith("SENTIMENT") and "POSITIONING" in upper:
                current_section = "sentiment"
                sections[current_section] = []
            elif upper.startswith("RISK ASSESSMENT:"):
                current_section = "risk"
                sections[current_section] = []
            elif upper.startswith("REASONING:"):
                current_section = "reasoning"
                sections[current_section] = []
            elif current_section is not None:
                sections[current_section].append(line)

        if "fundamental" in sections or "sentiment" in sections:
            result["analyst_reports"] = {
                "fundamental": "\n".join(sections.get("fundamental", [])).strip(),
                "sentiment": "\n".join(sections.get("sentiment", [])).strip(),
                "source": "single-model (llama.cpp)",
            }
        if "risk" in sections:
            result["risk"] = {
                "assessment": "\n".join(sections["risk"]).strip(),
                "source": "single-model (llama.cpp)",
            }
        if "reasoning" in sections:
            result["reasoning"] = "\n".join(sections["reasoning"]).strip()

        return result

    def _log_to_journal(
        self,
        regime_state: str,
        confidence: float,
        grid_action: str,
        agent_decision: str,
        agent_reasoning: str,
    ) -> int | None:
        """Log the agent decision to the GRID decision journal.

        Returns the decision_journal.id or None if no production model exists.
        """
        # Find current production model for journal FK
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id FROM model_registry "
                    "WHERE state = 'PRODUCTION' ORDER BY layer LIMIT 1"
                )
            ).fetchone()

        if row is None:
            log.warning("No production model — skipping journal entry")
            return None

        model_id = row[0]

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO decision_journal
                    (model_version_id, inferred_state, state_confidence,
                     transition_probability, contradiction_flags,
                     grid_recommendation, baseline_recommendation,
                     action_taken, counterfactual, operator_confidence,
                     annotation)
                    VALUES
                    (:mvid, :state, :sc, :tp, :cf, :gr, :br, :at, :cft, :oc, :ann)
                    RETURNING id
                """),
                {
                    "mvid": model_id,
                    "state": regime_state,
                    "sc": min(max(confidence, 0.0), 1.0),
                    "tp": 0.0,
                    "cf": json.dumps({}),
                    "gr": agent_decision,
                    "br": grid_action,
                    "at": "AGENT_ADVISORY",
                    "cft": f"GRID suggested {grid_action}; agents decided {agent_decision}",
                    "oc": "MEDIUM",
                    "ann": f"TradingAgents: {agent_reasoning[:500]}",
                },
            )
            journal_id = result.fetchone()[0]

        log.info("Logged agent decision to journal — id={id}", id=journal_id)
        return journal_id

    def _save_agent_run(
        self,
        ticker: str,
        as_of_date: date,
        regime_state: str,
        confidence: float,
        parsed: dict[str, Any],
        journal_id: int | None,
        llm_provider: str,
        llm_model: str,
        duration: float,
        error: str | None,
    ) -> int:
        """Save the full agent run to the agent_runs table."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO agent_runs
                    (ticker, as_of_date, grid_regime_state, grid_confidence,
                     analyst_reports, bull_bear_debate, risk_assessment,
                     final_decision, decision_reasoning,
                     decision_journal_id, llm_provider, llm_model,
                     duration_seconds, error)
                    VALUES
                    (:ticker, :aod, :grs, :gc,
                     :ar, :bbd, :ra,
                     :fd, :dr,
                     :djid, :llmp, :llmm,
                     :dur, :err)
                    RETURNING id
                """),
                {
                    "ticker": ticker,
                    "aod": as_of_date,
                    "grs": regime_state,
                    "gc": confidence,
                    "ar": json.dumps(parsed["analyst_reports"], default=str),
                    "bbd": json.dumps(parsed["bull_bear_debate"], default=str),
                    "ra": json.dumps(parsed["risk_assessment"], default=str),
                    "fd": parsed["final_decision"],
                    "dr": parsed["decision_reasoning"],
                    "djid": journal_id,
                    "llmp": llm_provider,
                    "llmm": llm_model,
                    "dur": duration,
                    "err": error,
                },
            )
            run_id = result.fetchone()[0]

        return run_id

    def get_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """Fetch recent agent runs."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id, run_timestamp, ticker, as_of_date,
                           grid_regime_state, grid_confidence,
                           final_decision, decision_reasoning,
                           llm_provider, llm_model, duration_seconds, error
                    FROM agent_runs
                    ORDER BY run_timestamp DESC
                    LIMIT :lim
                """),
                {"lim": limit},
            ).fetchall()

        return [
            {
                "id": r[0],
                "run_timestamp": r[1].isoformat() if r[1] else None,
                "ticker": r[2],
                "as_of_date": r[3].isoformat() if r[3] else None,
                "grid_regime_state": r[4],
                "grid_confidence": r[5],
                "final_decision": r[6],
                "decision_reasoning": r[7],
                "llm_provider": r[8],
                "llm_model": r[9],
                "duration_seconds": r[10],
                "error": r[11],
            }
            for r in rows
        ]

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        """Fetch a single agent run with full deliberation details."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM agent_runs WHERE id = :id"),
                {"id": run_id},
            ).fetchone()

        if row is None:
            return None

        cols = [
            "id", "run_timestamp", "ticker", "as_of_date",
            "grid_regime_state", "grid_confidence",
            "analyst_reports", "bull_bear_debate", "risk_assessment",
            "final_decision", "decision_reasoning",
            "decision_journal_id", "llm_provider", "llm_model",
            "duration_seconds", "error",
        ]
        result = {}
        for i, col in enumerate(cols):
            val = row[i]
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            result[col] = val

        return result
