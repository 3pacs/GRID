"""
GRID LLM Task Queue — keeps the onboard Qwen 32B working constantly.

Priority queue that feeds the local llama.cpp model a continuous stream
of work.  Real-time requests (trade reviews, user chat) jump the line;
scheduled Hermes cycle tasks run at priority 2; background work
(feature interpretation, anomaly detection, hypothesis generation)
auto-fills when nothing else is pending.

Usage:
    from orchestration.llm_taskqueue import LLMTaskQueue, get_task_queue

    tq = get_task_queue(engine)
    tq.enqueue("trade_review", prompt, priority=1, callback=my_cb)
    tq.run_forever()        # blocking — run in a thread
    tq.get_status()         # introspection for the API

Wire into hermes_operator:
    Start as a daemon thread alongside the operator loop.
    Hermes enqueues its narrative tasks at priority 2.
    Background generators auto-fill at priority 3.

Implementation split across:
  - orchestration.llm_task_models   — LLMTask dataclass, type constants, config
  - orchestration.llm_task_workers  — background generators, expectation handler, API router
"""

from __future__ import annotations

import heapq
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger as log

# Re-export models for backward compatibility
from orchestration.llm_task_models import (  # noqa: F401
    LLMTask,
    REALTIME_TYPES,
    SCHEDULED_TYPES,
    BACKGROUND_TYPES,
    TASK_TIMEOUT_SECONDS,
    BACKGROUND_BATCH_SIZE,
    BACKGROUND_REFILL_COOLDOWN,
    MAX_HISTORY,
)

# Re-export workers/router for backward compatibility
from orchestration.llm_task_workers import (  # noqa: F401
    _generate_background_tasks,
    _handle_expectation_result,
    build_router,
)

# Internal aliases for the queue class
_TASK_TIMEOUT_SECONDS = TASK_TIMEOUT_SECONDS
_BACKGROUND_BATCH_SIZE = BACKGROUND_BATCH_SIZE
_BACKGROUND_REFILL_COOLDOWN = BACKGROUND_REFILL_COOLDOWN
_MAX_HISTORY = MAX_HISTORY


class LLMTaskQueue:
    """Priority queue that keeps the onboard LLM busy around the clock.

    Thread-safe.  Call ``run_forever()`` in a daemon thread; enqueue work
    from any thread.  Real-time tasks (priority 1) pre-empt background
    work the next time the LLM finishes its current generation.
    """

    def __init__(self, engine: Any) -> None:
        self._engine = engine
        self._client = None  # lazy — import heavy

        # Priority queue (min-heap on _sort_key)
        self._queue: list[LLMTask] = []
        self._lock = threading.Lock()

        # Currently executing task
        self._running: LLMTask | None = None

        # Completed task history (bounded ring)
        self._history: list[LLMTask] = []

        # Throughput counters
        self._total_completed: int = 0
        self._total_errors: int = 0
        self._total_timeouts: int = 0
        self._started_at: float | None = None
        self._idle_seconds: float = 0.0
        self._last_idle_start: float | None = None

        # Background refill tracking
        self._last_refill: float = 0.0
        self._last_briefing: datetime | None = None
        self._interpreted_features: set[str] = set()
        self._researched_actors: set[str] = set()
        self._researched_offshore: set[str] = set()

        self._stop = threading.Event()

    # ------------------------------------------------------------------
    # Client init (lazy — avoids import at module load)
    # ------------------------------------------------------------------
    def _get_client(self):
        """Return the LlamaCppClient singleton, creating it on first call."""
        if self._client is None:
            try:
                from llm.router import get_llm, Tier
                self._client = get_llm(Tier.REASON)
            except Exception as exc:
                log.warning("LLM client init failed: {e}", e=str(exc))
        return self._client

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------
    def enqueue(
        self,
        task_type: str,
        prompt: str,
        context: dict | None = None,
        priority: int = 3,
        callback: Callable | None = None,
    ) -> str:
        """Add a task to the queue.  Returns the task ID.

        Parameters:
            task_type: One of the type constants (e.g. ``"trade_review"``).
            prompt: Full prompt text for the LLM.
            context: Arbitrary metadata dict stored with the task.
            priority: 1 = realtime, 2 = scheduled, 3 = background.
            callback: Optional ``fn(task: LLMTask)`` invoked after completion.

        Returns:
            str: UUID of the enqueued task.
        """
        task_id = uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()

        task = LLMTask(
            id=task_id,
            priority=max(1, min(3, priority)),
            task_type=task_type,
            prompt=prompt,
            context=context or {},
            callback=callback,
            created_at=now,
        )

        with self._lock:
            heapq.heappush(self._queue, task)

        log.debug(
            "LLM-TQ enqueue [{id}] p{p} {t} (queue={q})",
            id=task_id, p=priority, t=task_type, q=len(self._queue),
        )
        return task_id

    # ------------------------------------------------------------------
    # Process one task
    # ------------------------------------------------------------------
    def process_next(self) -> LLMTask | None:
        """Pop and process the highest-priority task.

        Returns the completed (or errored) task, or None if the queue
        is empty.
        """
        with self._lock:
            if not self._queue:
                return None
            task = heapq.heappop(self._queue)
            self._running = task

        # Mark idle end
        if self._last_idle_start is not None:
            self._idle_seconds += time.monotonic() - self._last_idle_start
            self._last_idle_start = None

        task.started_at = datetime.now(timezone.utc).isoformat()
        log.info(
            "LLM-TQ start [{id}] p{p} {t}",
            id=task.id, p=task.priority, t=task.task_type,
        )

        client = self._get_client()
        if client is None or not client.is_available:
            task.error = "LLM client unavailable"
            task.completed_at = datetime.now(timezone.utc).isoformat()
            self._record_done(task)
            return task

        # Run with timeout — we cap at _TASK_TIMEOUT_SECONDS via the
        # client's own HTTP timeout.  We override it per-task so long
        # background tasks don't starve real-time requests.
        timeout = _TASK_TIMEOUT_SECONDS if task.priority <= 2 else 90
        old_timeout = client.timeout
        try:
            client.timeout = timeout

            system_msg = self._system_prompt_for(task.task_type)
            messages: list[dict[str, str]] = []
            if system_msg:
                messages.append({"role": "system", "content": system_msg})
            messages.append({"role": "user", "content": task.prompt})

            result = client.chat(
                messages,
                temperature=0.3 if task.priority <= 2 else 0.5,
                num_predict=2000,
            )
            if result is not None:
                task.result = result
            else:
                task.error = "LLM returned None (possible timeout or server error)"

        except Exception as exc:
            task.error = f"{type(exc).__name__}: {str(exc)[:300]}"
            log.warning(
                "LLM-TQ error [{id}] {t}: {e}",
                id=task.id, t=task.task_type, e=task.error,
            )
        finally:
            client.timeout = old_timeout

        task.completed_at = datetime.now(timezone.utc).isoformat()
        self._record_done(task)

        # Fire callback (non-blocking)
        if task.callback and task.result:
            try:
                task.callback(task)
            except Exception as cb_exc:
                log.warning(
                    "LLM-TQ callback error [{id}]: {e}",
                    id=task.id, e=str(cb_exc),
                )

        return task

    # ------------------------------------------------------------------
    # Persist result to DB + LLM logger
    # ------------------------------------------------------------------
    def _record_done(self, task: LLMTask) -> None:
        """Store completed task in history and optionally DB."""
        with self._lock:
            self._running = None
            self._history.append(task)
            if len(self._history) > _MAX_HISTORY:
                self._history = self._history[-_MAX_HISTORY:]
            self._total_completed += 1
            if task.error:
                self._total_errors += 1

        # Log insight for non-trivial results
        if task.result and task.task_type != "user_chat":
            try:
                from outputs.llm_logger import log_insight
                log_insight(
                    category="ad_hoc",
                    title=f"[TQ:{task.task_type}] {task.id}",
                    content=task.result,
                    metadata={
                        "task_id": task.id,
                        "task_type": task.task_type,
                        "priority": task.priority,
                        "context": task.context,
                    },
                    provider="llamacpp",
                )
            except Exception as exc:
                log.debug("LLM-TQ: snapshot store write failed: {e}", e=str(exc))

        # Persist to analytical_snapshots for long-term reference
        if task.result:
            try:
                from store.snapshots import AnalyticalSnapshotStore
                from datetime import date
                snap = AnalyticalSnapshotStore(db_engine=self._engine)
                snap.save_snapshot(
                    category=f"llm_task_{task.task_type}",
                    payload={
                        "task_id": task.id,
                        "priority": task.priority,
                        "prompt_preview": task.prompt[:300],
                        "result_preview": task.result[:500],
                        "context": task.context,
                    },
                    as_of_date=date.today(),
                    metrics={
                        "prompt_len": len(task.prompt),
                        "result_len": len(task.result),
                    },
                )
            except Exception as exc:
                log.debug("LLM-TQ: analytical snapshot save failed: {e}", e=str(exc))

        # Post-processing callbacks for structured task types
        if task.result and task.task_type == "expectation_tracking":
            try:
                _handle_expectation_result(
                    self._engine, task.task_type, task.result, task.context
                )
            except Exception as exc:
                log.debug("LLM-TQ: expectation result handler failed: {e}", e=str(exc))

        # Mark backlog task as done if it came from the backlog table
        backlog_id = task.context.get("backlog_id") if task.context else None
        if backlog_id:
            try:
                from sqlalchemy import text as sa_text
                with self._engine.begin() as conn:
                    conn.execute(sa_text(
                        "UPDATE llm_task_backlog SET status = 'done' WHERE id = :id"
                    ), {"id": backlog_id})
            except Exception as exc:
                log.debug("LLM-TQ: backlog status update failed for id {id}: {e}", id=backlog_id, e=str(exc))

        log.info(
            "LLM-TQ done [{id}] {t} — {s}",
            id=task.id, t=task.task_type,
            s="OK" if task.result else f"ERR: {task.error}",
        )

    # ------------------------------------------------------------------
    # System prompts per task type
    # ------------------------------------------------------------------
    @staticmethod
    def _system_prompt_for(task_type: str) -> str | None:
        """Return a tailored system prompt for the given task type."""
        # Base context injected into every task type so the LLM knows
        # the full GRID architecture, modules, and data sources.
        _base = (
            "You are part of GRID Intelligence, a systematic trading intelligence platform. "
            "GRID has 50+ data sources (FRED, yFinance, ECB, BOJ, BOE, KOSIS, AKShare, BIS, "
            "congressional trades, insider Form 4, dark pool FINRA, whale options, Polymarket, "
            "smart money Reddit, supply chain, Fed liquidity, ETF flows, 13F institutional). "
            "NEW data sources: government contracts (USASpending via gov_contracts.py), "
            "lobbying disclosures (OpenSecrets via lobbying.py), campaign finance (FEC via campaign_finance.py), "
            "and legislation tracking (Congress.gov via legislation.py). "
            "Intelligence modules: trust_scorer (Bayesian), lever_pullers (named actors), "
            "actor_network (100+ players), cross_reference (lie detector), "
            "dollar_flows (HOW MUCH — normalized USD amounts across all signal types), "
            "event_sequence (WHEN — chronological timeline of all events per ticker), "
            "causation + forensics (WHY — links actor actions to contracts, legislation, earnings), "
            "sleuth (investigative lead generation), gov_intel + legislative_intel (government intelligence), "
            "thesis_tracker, trend_tracker, postmortem, source_audit. "
            "Use this knowledge to give specific, data-aware answers."
        )

        prompts = {
            "trade_review": (
                f"{_base}\n\n"
                "You are GRID's trade review agent. Evaluate the proposed trade "
                "for risk/reward, identify potential failure modes, check for "
                "confirmation bias, and give a clear APPROVE / CAUTION / REJECT "
                "recommendation with reasoning. Cross-check against dollar_flows "
                "(how much smart money is moving), event_sequence (what events "
                "preceded this setup), and causation (why are actors positioned "
                "this way — check gov_contracts, legislation, lobbying data). "
                "Be concise and specific."
            ),
            "convergence_alert": (
                f"{_base}\n\n"
                "You are GRID's convergence analyst. Explain the alert in plain "
                "English: what signals are converging, what the historical precedent "
                "is, and what the likely market impact will be. Reference dollar_flows "
                "for the magnitude of money moving and event_sequence for the timeline "
                "of events leading to convergence. 2-3 paragraphs max."
            ),
            "regime_change_explanation": (
                f"{_base}\n\n"
                "You are a financial economist within GRID. Explain this regime transition: "
                "what economic mechanisms are driving it, what historical episodes "
                "are analogous, and what to expect in the next 1-3 months. Reference "
                "cross_reference (official stats vs reality), dollar_flows (capital "
                "movement patterns), and any relevant legislation or government "
                "contract activity that may be influencing the shift."
            ),
            "user_chat": (
                f"{_base}\n\n"
                "You are GRID Intelligence. Answer the user's question using the full "
                "suite of GRID modules: dollar_flows for amounts, event_sequence for "
                "timelines, causation/forensics for explanations, cross_reference for "
                "truth-checking, lever_pullers/actor_network for who is acting, and "
                "gov_contracts/legislation/lobbying/campaign_finance for political "
                "intelligence. Be precise, cite data when available, take positions "
                "when the data supports them."
            ),
            "thesis_narrative": (
                f"{_base}\n\n"
                "You are GRID's thesis writer. Synthesize the provided data into "
                "a coherent investment thesis narrative. State the thesis, the "
                "supporting evidence (including dollar_flows magnitude, event_sequence "
                "timeline, and causation links to contracts/legislation), the key risks, "
                "and the falsification criteria."
            ),
            "cross_reference_narrative": (
                f"{_base}\n\n"
                "You are GRID's cross-reference analyst (lie detector). Compare the official "
                "statistics with the physical/alternative data and explain any "
                "discrepancies. What is the government data saying versus reality? "
                "Cross-check against gov_contracts (are government spending patterns "
                "consistent with reported economic data?), lobbying spend, and "
                "legislative activity for additional context."
            ),
            "postmortem_analysis": (
                f"{_base}\n\n"
                "You are GRID's postmortem analyst. Analyze what went wrong with "
                "this trade/prediction. Use forensics.py to reconstruct what events "
                "preceded the move, causation.py to identify why actors acted as they "
                "did, and dollar_flows to quantify the magnitude of flows that moved "
                "against the position. Identify the root cause, contributing "
                "factors, and specific lessons to incorporate going forward."
            ),
            "hypothesis_review": (
                f"{_base}\n\n"
                "You are a quantitative researcher within GRID. Review the hypothesis and "
                "its test results. Is the evidence convincing? What alternative "
                "explanations exist? Check whether gov_contracts, legislation, or "
                "lobbying data could be confounding variables. Should this hypothesis "
                "be promoted or retired?"
            ),
            "feature_interpretation": (
                f"{_base}\n\n"
                "You are a financial data scientist within GRID. Explain what this feature "
                "measures, why it matters for market prediction, and what economic "
                "mechanism connects it to asset prices. Consider whether this feature "
                "relates to dollar_flows, government activity, or actor behavior. "
                "One clear paragraph."
            ),
            "actor_research": (
                f"{_base}\n\n"
                "You are GRID's financial intelligence analyst. Research this market "
                "actor's current positioning, recent actions, and likely next "
                "moves. Use dollar_flows for transaction sizes, causation for "
                "motivations (check gov_contracts for government awards, legislation "
                "for relevant bills, lobbying for political connections, campaign_finance "
                "for donor relationships). Distinguish between confirmed facts and inference."
            ),
            "hypothesis_generation": (
                f"{_base}\n\n"
                "You are a quantitative researcher within GRID. Look at the provided data "
                "patterns and generate novel, falsifiable hypotheses that could "
                "explain or exploit them. Consider cross-domain hypotheses linking "
                "government data (contracts, legislation, lobbying) to market moves. "
                "Each hypothesis must specify variables, direction, horizon, and a test method."
            ),
            "market_briefing": (
                f"{_base}\n\n"
                "You are GRID's market briefing writer. Produce a concise, "
                "actionable daily market briefing covering: regime state, key "
                "moves, convergence signals, dollar flow magnitudes, notable "
                "government contract awards, legislative developments, and "
                "trade opportunities. No fluff. Name names."
            ),
            "anomaly_detection": (
                f"{_base}\n\n"
                "You are a statistical analyst within GRID. Explain the detected anomaly: "
                "what moved, by how much relative to history, possible causes "
                "(check event_sequence for preceding events, causation for actor "
                "motivations, gov_contracts/legislation for political catalysts), "
                "and whether it signals a regime change or is noise."
            ),
            "narrative_history": (
                f"{_base}\n\n"
                "You are GRID's market diarist. Write a concise daily diary "
                "entry explaining what happened in markets today, what drove "
                "the moves (reference dollar_flows, actor actions, government "
                "activity), and what it means for the thesis."
            ),
            "prediction_refinement": (
                f"{_base}\n\n"
                "You are GRID's forecasting analyst. Review the active prediction "
                "and its current trajectory. Check dollar_flows for confirming/ "
                "disconfirming capital movement, event_sequence for new developments, "
                "and causation for shifts in actor motivation. Should conviction be "
                "raised, lowered, or maintained? What new evidence has emerged?"
            ),
            "correlation_discovery": (
                f"{_base}\n\n"
                "You are a quantitative analyst within GRID. Examine the correlation between "
                "these two features. Is it spurious or economically meaningful? "
                "What mechanism could explain it? Consider government activity "
                "(contracts, legislation, lobbying) as a potential hidden variable. "
                "Is it stable across regimes?"
            ),
            "company_analysis": (
                f"{_base}\n\n"
                "You are GRID's company influence analyst. Analyze the company's full "
                "influence profile: government contracts, lobbying spend, congressional "
                "holders, insider activity, export controls, and influence loops. "
                "Who is positioned around this company? What circular flows of money "
                "exist? What is the regulatory/political risk? Be specific and "
                "data-driven. Name names."
            ),
            "offshore_leak_investigation": (
                f"{_base}\n\n"
                "You are GRID's offshore financial intelligence analyst. You have access "
                "to data from the ICIJ Offshore Leaks database (Panama Papers, Pandora "
                "Papers, Paradise Papers). Investigate the actor's offshore connections: "
                "What was the entity used for? Is this a legitimate tax structure or "
                "suspicious? What other actors in GRID's network are connected to the "
                "same entities? What are the implications for their public trading "
                "positions, political roles, or fiduciary duties? Cross-reference "
                "with dollar_flows, gov_contracts, lobbying, and campaign_finance data "
                "for the full picture. Be specific and forensic."
            ),
            "panama_papers_research": (
                f"{_base}\n\n"
                "You are GRID's offshore leak researcher. Analyze the connection between "
                "a known financial actor and offshore entities found in the ICIJ database. "
                "Determine: (1) the likely purpose of the offshore structure, (2) whether "
                "it represents legitimate tax planning or potential evasion/corruption, "
                "(3) connections to other actors in the network, (4) implications for "
                "any active GRID theses or positions. Use jurisdiction analysis (BVI, "
                "Panama, Cayman, etc.) to assess risk level. Cross-check against "
                "lobbying and campaign finance for political exposure."
            ),
        }
        return prompts.get(task_type)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run_forever(self) -> None:
        """Process queue continuously.  Refill with background tasks when empty.

        Intended to run in a daemon thread.  Call ``stop()`` to exit.
        """
        self._started_at = time.monotonic()
        log.info("LLM Task Queue started — processing continuously")

        while not self._stop.is_set():
            # If queue is empty, generate background work
            with self._lock:
                queue_len = len(self._queue)

            if queue_len == 0:
                self._maybe_refill_background()

            with self._lock:
                queue_len = len(self._queue)

            if queue_len > 0:
                self.process_next()
            else:
                # Truly nothing to do — brief sleep then check again
                if self._last_idle_start is None:
                    self._last_idle_start = time.monotonic()
                self._stop.wait(timeout=2.0)

        log.info("LLM Task Queue stopped")

    def stop(self) -> None:
        """Signal the run_forever loop to exit."""
        self._stop.set()

    # ------------------------------------------------------------------
    # Background task generators
    # ------------------------------------------------------------------
    def _maybe_refill_background(self) -> None:
        """Generate a batch of background tasks if cooldown has elapsed."""
        now = time.monotonic()
        if now - self._last_refill < _BACKGROUND_REFILL_COOLDOWN:
            return
        self._last_refill = now

        tasks = _generate_background_tasks(self._engine, self)
        for t_type, prompt, ctx in tasks[:_BACKGROUND_BATCH_SIZE]:
            self.enqueue(t_type, prompt, context=ctx, priority=3)

        if tasks:
            log.info("LLM-TQ refilled {n} background tasks", n=min(len(tasks), _BACKGROUND_BATCH_SIZE))

    # ------------------------------------------------------------------
    # Status / introspection
    # ------------------------------------------------------------------
    def get_status(self) -> dict[str, Any]:
        """Current queue depth, running task, throughput, idle time %."""
        with self._lock:
            queue_depth = len(self._queue)
            running = None
            if self._running:
                running = {
                    "id": self._running.id,
                    "type": self._running.task_type,
                    "priority": self._running.priority,
                    "started_at": self._running.started_at,
                }
            by_priority = {1: 0, 2: 0, 3: 0}
            for t in self._queue:
                by_priority[t.priority] = by_priority.get(t.priority, 0) + 1

            recent = self._history[-20:] if self._history else []

        uptime = time.monotonic() - self._started_at if self._started_at else 0
        idle_total = self._idle_seconds
        if self._last_idle_start is not None:
            idle_total += time.monotonic() - self._last_idle_start
        idle_pct = round(idle_total / max(uptime, 1) * 100, 1)

        # Throughput: tasks per hour
        hours = max(uptime / 3600, 0.01)
        throughput = round(self._total_completed / hours, 1)

        return {
            "queue_depth": queue_depth,
            "queue_by_priority": by_priority,
            "running_task": running,
            "total_completed": self._total_completed,
            "total_errors": self._total_errors,
            "total_timeouts": self._total_timeouts,
            "throughput_per_hour": throughput,
            "uptime_seconds": round(uptime, 1),
            "idle_pct": idle_pct,
            "recent_tasks": [
                {
                    "id": t.id,
                    "type": t.task_type,
                    "priority": t.priority,
                    "completed_at": t.completed_at,
                    "has_result": t.result is not None,
                    "error": t.error,
                }
                for t in reversed(recent)
            ],
        }

    def get_task_result(self, task_id: str) -> LLMTask | None:
        """Look up a completed task by ID."""
        with self._lock:
            for t in self._history:
                if t.id == task_id:
                    return t
        return None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_tq_instance: LLMTaskQueue | None = None
_tq_lock = threading.Lock()


def get_task_queue(engine: Any | None = None) -> LLMTaskQueue:
    """Return the global LLMTaskQueue singleton.

    Parameters:
        engine: SQLAlchemy engine.  Required on first call; ignored after.

    Returns:
        LLMTaskQueue: The singleton instance.
    """
    global _tq_instance
    with _tq_lock:
        if _tq_instance is None:
            if engine is None:
                from db import get_engine
                engine = get_engine()
            _tq_instance = LLMTaskQueue(engine)
        return _tq_instance


def start_task_queue_thread(engine: Any | None = None) -> threading.Thread:
    """Start the task queue in a daemon thread.

    Returns the thread object (already started).  Safe to call multiple
    times — returns the existing thread if already running.
    """
    tq = get_task_queue(engine)
    t = threading.Thread(target=tq.run_forever, name="llm-taskqueue", daemon=True)
    t.start()
    log.info("LLM Task Queue daemon thread started")
    return t
