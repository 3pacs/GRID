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
"""

from __future__ import annotations

import heapq
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger as log

# ---------------------------------------------------------------------------
# Task dataclass
# ---------------------------------------------------------------------------

# Task type constants — used for routing and display
REALTIME_TYPES = frozenset({
    "trade_review",
    "convergence_alert",
    "regime_change_explanation",
    "user_chat",
})

SCHEDULED_TYPES = frozenset({
    "thesis_narrative",
    "cross_reference_narrative",
    "postmortem_analysis",
    "hypothesis_review",
})

BACKGROUND_TYPES = frozenset({
    "web_scrape_summarize",
    "feature_interpretation",
    "actor_research",
    "hypothesis_generation",
    "market_briefing",
    "anomaly_detection",
    "narrative_history",
    "prediction_refinement",
    "knowledge_building",
    "correlation_discovery",
    "company_analysis",
    "offshore_leak_investigation",
    "panama_papers_research",
})


@dataclass
class LLMTask:
    """A unit of work for the LLM."""

    id: str
    priority: int                   # 1=realtime, 2=scheduled, 3=background
    task_type: str
    prompt: str
    context: dict
    callback: Callable | None       # called with (task) after completion
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    result: str | None = None
    error: str | None = None

    # For heapq ordering: (priority, creation_time, unique_id)
    _sort_key: tuple = field(default=(), repr=False)

    def __post_init__(self) -> None:
        if not self._sort_key:
            object.__setattr__(
                self, "_sort_key", (self.priority, self.created_at, self.id)
            )

    def __lt__(self, other: LLMTask) -> bool:
        return self._sort_key < other._sort_key

    def __le__(self, other: LLMTask) -> bool:
        return self._sort_key <= other._sort_key


# ---------------------------------------------------------------------------
# Task Queue
# ---------------------------------------------------------------------------

# Default timeout for a single LLM call (seconds)
_TASK_TIMEOUT_SECONDS = 60

# How many background tasks to generate per refill
_BACKGROUND_BATCH_SIZE = 25

# Minimum seconds between background refills (avoid spamming)
_BACKGROUND_REFILL_COOLDOWN = 10  # refill more often — never idle

# Max completed tasks kept in memory for status/history
_MAX_HISTORY = 500


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
                from llamacpp.client import get_client
                self._client = get_client()
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
            except Exception:
                pass

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
            except Exception:
                pass  # snapshot store may not exist yet

        # Post-processing callbacks for structured task types
        if task.result and task.task_type == "expectation_tracking":
            try:
                _handle_expectation_result(
                    self._engine, task.task_type, task.result, task.context
                )
            except Exception:
                pass

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
# Background task generators
# ---------------------------------------------------------------------------

def _generate_background_tasks(
    engine: Any,
    tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Generate a batch of background work when the queue runs dry.

    Checks what needs doing and returns a list of
    ``(task_type, prompt, context)`` tuples.
    """
    tasks: list[tuple[str, str, dict]] = []

    # 1. Feature interpretations — any features without an explanation?
    try:
        tasks.extend(_gen_feature_interpretations(engine, tq))
    except Exception as exc:
        log.debug("Feature interpretation gen failed: {e}", e=str(exc))

    # 2. Actor research — actors without recent profiles
    try:
        tasks.extend(_gen_actor_research(engine, tq))
    except Exception as exc:
        log.debug("Actor research gen failed: {e}", e=str(exc))

    # 3. Market briefing — refresh if older than 2 hours
    try:
        tasks.extend(_gen_market_briefing(engine, tq))
    except Exception as exc:
        log.debug("Market briefing gen failed: {e}", e=str(exc))

    # 4. Anomaly detection — scan for >3 sigma moves
    try:
        tasks.extend(_gen_anomaly_detection(engine, tq))
    except Exception as exc:
        log.debug("Anomaly detection gen failed: {e}", e=str(exc))

    # 5. Hypothesis generation — look for new patterns
    try:
        tasks.extend(_gen_hypothesis_tasks(engine, tq))
    except Exception as exc:
        log.debug("Hypothesis gen failed: {e}", e=str(exc))

    # 6. Narrative history — daily market diary
    try:
        tasks.extend(_gen_narrative_history(engine, tq))
    except Exception as exc:
        log.debug("Narrative history gen failed: {e}", e=str(exc))

    # 7. Prediction refinement — review active oracle predictions
    try:
        tasks.extend(_gen_prediction_refinement(engine, tq))
    except Exception as exc:
        log.debug("Prediction refinement gen failed: {e}", e=str(exc))

    # 8. Company analysis — work through the NASDAQ 100 queue
    try:
        tasks.extend(_gen_company_analysis(engine, tq))
    except Exception as exc:
        log.debug("Company analysis gen failed: {e}", e=str(exc))

    # 9. Correlation discovery — test feature pairs
    try:
        tasks.extend(_gen_correlation_discovery(engine, tq))
    except Exception as exc:
        log.debug("Correlation discovery gen failed: {e}", e=str(exc))

    # 10. Panama Papers / ICIJ offshore leak research
    try:
        tasks.extend(_gen_panama_papers_research(engine, tq))
    except Exception as exc:
        log.debug("Panama Papers research gen failed: {e}", e=str(exc))

    # 11. Expectation tracker — Qwen generates market expectations for Mag 7
    try:
        tasks.extend(_gen_expectation_tracking(engine, tq))
    except Exception as exc:
        log.debug("Expectation tracking gen failed: {e}", e=str(exc))

    # 12. Deep forensic analysis — decompose price moves for Mag 7
    try:
        tasks.extend(_gen_deep_forensics(engine, tq))
    except Exception as exc:
        log.debug("Deep forensics gen failed: {e}", e=str(exc))

    # 13. Offshore network analysis — trace connections in ICIJ data
    try:
        tasks.extend(_gen_offshore_analysis(engine, tq))
    except Exception as exc:
        log.debug("Offshore analysis gen failed: {e}", e=str(exc))

    # 14. Sector rotation analysis — what money is moving where
    try:
        tasks.extend(_gen_sector_rotation(engine, tq))
    except Exception as exc:
        log.debug("Sector rotation gen failed: {e}", e=str(exc))

    # 15. Signal cross-validation — check if multiple signals agree
    try:
        tasks.extend(_gen_signal_cross_validation(engine, tq))
    except Exception as exc:
        log.debug("Signal cross-validation gen failed: {e}", e=str(exc))

    # 16. Earnings preview — pre-analyze upcoming earnings
    try:
        tasks.extend(_gen_earnings_preview(engine, tq))
    except Exception as exc:
        log.debug("Earnings preview gen failed: {e}", e=str(exc))

    return tasks


# --- Individual generators ---

def _gen_feature_interpretations(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Generate interpretation tasks for un-interpreted features."""
    from sqlalchemy import text

    tasks: list[tuple[str, str, dict]] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT name, family, description FROM feature_registry "
                "WHERE model_eligible = TRUE "
                "ORDER BY name LIMIT 100"
            )).fetchall()

        for row in rows:
            name, family, desc = row[0], row[1], row[2] or ""
            if name in tq._interpreted_features:
                continue
            tq._interpreted_features.add(name)

            prompt = (
                f"Feature: {name}\n"
                f"Family: {family}\n"
                f"Description: {desc}\n\n"
                f"Write a one-paragraph explanation of what this feature measures, "
                f"why it matters for market prediction, and what economic mechanism "
                f"connects it to asset prices."
            )
            tasks.append(("feature_interpretation", prompt, {"feature": name, "family": family}))
            if len(tasks) >= 3:
                break
    except Exception:
        pass
    return tasks


def _gen_actor_research(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Generate research tasks for actors without recent profiles."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from analysis.sector_map import SECTOR_MAP, get_actor_influence

        for sector_name in SECTOR_MAP:
            actors = get_actor_influence(sector_name)
            for actor in actors[:5]:
                key = f"{sector_name}:{actor['name']}"
                if key in tq._researched_actors:
                    continue
                tq._researched_actors.add(key)

                prompt = (
                    f"Actor: {actor['name']} ({actor['type']})\n"
                    f"Sector: {sector_name}\n"
                    f"Influence weight: {actor['influence']:.0%}\n"
                    f"Description: {actor.get('description', 'N/A')}\n\n"
                    f"Research this actor's current market positioning, recent "
                    f"actions (last 30 days), and likely next moves. What is the "
                    f"single most important catalyst to watch? Distinguish confirmed "
                    f"facts from inference."
                )
                tasks.append(("actor_research", prompt, {
                    "actor": actor["name"],
                    "sector": sector_name,
                    "influence": actor["influence"],
                }))
                if len(tasks) >= 2:
                    return tasks
    except Exception:
        pass
    return tasks


def _gen_market_briefing(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Generate a market briefing if the last one is >2 hours old."""
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    if tq._last_briefing and (now - tq._last_briefing) < timedelta(hours=2):
        return []

    tq._last_briefing = now

    # Gather recent data context
    context_parts: list[str] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            # Latest regime
            row = conn.execute(text(
                "SELECT payload FROM analytical_snapshots "
                "WHERE category = 'regime_labels' "
                "ORDER BY created_at DESC LIMIT 1"
            )).fetchone()
            if row:
                context_parts.append(f"Current regime snapshot: {str(row[0])[:500]}")

            # Recent features
            rows = conn.execute(text(
                "SELECT fr.name, rs.value, rs.obs_date "
                "FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE rs.obs_date >= CURRENT_DATE - 1 "
                "ORDER BY rs.obs_date DESC LIMIT 20"
            )).fetchall()
            if rows:
                lines = [f"  {r[0]}: {r[1]} ({r[2]})" for r in rows]
                context_parts.append("Recent data:\n" + "\n".join(lines))
    except Exception:
        pass

    context_text = "\n\n".join(context_parts) if context_parts else "No recent data available."

    prompt = (
        f"Current date: {now.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"{context_text}\n\n"
        f"Write a concise, actionable market briefing covering:\n"
        f"1. Current regime state and key drivers\n"
        f"2. Notable moves in the last 24 hours\n"
        f"3. Key convergence signals or divergences\n"
        f"4. Actionable opportunities or risks\n\n"
        f"Be specific. No hedging language. State your confidence level."
    )
    return [("market_briefing", prompt, {"generated_at": now.isoformat()})]


def _gen_anomaly_detection(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Scan resolved_series for >3 sigma moves and explain them."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            # Find features with recent values that are >3 std devs from mean
            rows = conn.execute(text("""
                WITH stats AS (
                    SELECT feature_id,
                           AVG(value) AS mean_val,
                           STDDEV(value) AS std_val
                    FROM resolved_series
                    WHERE obs_date >= CURRENT_DATE - 252
                    GROUP BY feature_id
                    HAVING STDDEV(value) > 0
                ),
                latest AS (
                    SELECT DISTINCT ON (feature_id)
                           feature_id, value, obs_date
                    FROM resolved_series
                    WHERE obs_date >= CURRENT_DATE - 2
                    ORDER BY feature_id, obs_date DESC
                )
                SELECT fr.name, l.value, s.mean_val, s.std_val, l.obs_date,
                       ABS(l.value - s.mean_val) / s.std_val AS z_score
                FROM latest l
                JOIN stats s ON s.feature_id = l.feature_id
                JOIN feature_registry fr ON fr.id = l.feature_id
                WHERE ABS(l.value - s.mean_val) / s.std_val > 3
                ORDER BY z_score DESC
                LIMIT 5
            """)).fetchall()

            for row in rows:
                name, value, mean, std, obs_date, z = row
                prompt = (
                    f"Anomaly detected in {name}:\n"
                    f"  Current value: {value:.4f}\n"
                    f"  252-day mean: {mean:.4f}\n"
                    f"  252-day std: {std:.4f}\n"
                    f"  Z-score: {z:.2f}\n"
                    f"  Observation date: {obs_date}\n\n"
                    f"Explain this anomaly: what moved, possible causes, and "
                    f"whether this signals a regime change or is transient noise."
                )
                tasks.append(("anomaly_detection", prompt, {
                    "feature": name,
                    "z_score": float(z),
                    "value": float(value),
                    "obs_date": str(obs_date),
                }))
    except Exception:
        pass
    return tasks


def _gen_hypothesis_tasks(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Generate hypothesis generation tasks from unresearched candidates."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, statement FROM hypothesis_registry "
                "WHERE state = 'CANDIDATE' "
                "ORDER BY id DESC LIMIT 3"
            )).fetchall()

        for row in rows:
            hyp_id, statement = row[0], row[1]
            prompt = (
                f"Hypothesis (ID={hyp_id}):\n{statement}\n\n"
                f"Analyze this hypothesis:\n"
                f"1. Is the economic mechanism plausible?\n"
                f"2. What confounding variables could explain the pattern?\n"
                f"3. What additional data would strengthen or weaken it?\n"
                f"4. Suggest a specific, falsifiable test.\n"
                f"5. Rate confidence: LOW / MEDIUM / HIGH with reasoning."
            )
            tasks.append(("hypothesis_generation", prompt, {"hypothesis_id": hyp_id}))
    except Exception:
        pass
    return tasks


def _gen_narrative_history(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Generate a daily market diary entry."""
    from datetime import date as date_cls

    today = date_cls.today().isoformat()
    # Only one diary entry per day
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            existing = conn.execute(text(
                "SELECT 1 FROM analytical_snapshots "
                "WHERE category = 'llm_task_narrative_history' "
                "AND as_of_date = :d LIMIT 1"
            ), {"d": today}).fetchone()
            if existing:
                return []
    except Exception:
        pass

    # Gather day's data
    context_lines: list[str] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT fr.name, rs.value FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE rs.obs_date = CURRENT_DATE "
                "ORDER BY fr.name LIMIT 30"
            )).fetchall()
            context_lines = [f"  {r[0]}: {r[1]}" for r in rows]
    except Exception:
        pass

    data_block = "\n".join(context_lines) if context_lines else "No data for today yet."

    prompt = (
        f"Date: {today}\n\n"
        f"Today's data:\n{data_block}\n\n"
        f"Write a concise daily market diary entry (3-5 paragraphs) explaining:\n"
        f"1. What happened in markets today\n"
        f"2. What drove the moves (causes, not just descriptions)\n"
        f"3. How this fits the current macro regime\n"
        f"4. What to watch tomorrow\n\n"
        f"Write as a thoughtful journal entry, not a news report."
    )
    return [("narrative_history", prompt, {"date": today})]


def _gen_prediction_refinement(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Review active oracle predictions and suggest conviction updates."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, prediction_text, predicted_value, confidence, "
                "       target_date, created_at "
                "FROM oracle_predictions "
                "WHERE status = 'ACTIVE' "
                "AND target_date > CURRENT_DATE "
                "ORDER BY target_date ASC LIMIT 3"
            )).fetchall()

        for row in rows:
            pid, pred_text, pred_val, conf, target, created = row
            prompt = (
                f"Active prediction (ID={pid}):\n"
                f"  Text: {pred_text}\n"
                f"  Predicted value: {pred_val}\n"
                f"  Confidence: {conf}\n"
                f"  Target date: {target}\n"
                f"  Created: {created}\n\n"
                f"Given current market conditions, should this prediction's "
                f"conviction be RAISED, LOWERED, or MAINTAINED? What new "
                f"evidence has emerged since it was made? Provide a specific "
                f"updated confidence level (0-1)."
            )
            tasks.append(("prediction_refinement", prompt, {
                "prediction_id": pid,
                "current_confidence": float(conf) if conf else None,
            }))
    except Exception:
        pass
    return tasks


def _gen_correlation_discovery(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Test random feature pairs for non-obvious relationships."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        import random
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT name, family FROM feature_registry "
                "WHERE model_eligible = TRUE "
                "ORDER BY random() LIMIT 20"
            )).fetchall()

        features = [(r[0], r[1]) for r in rows]
        if len(features) < 2:
            return []

        # Pick 2 features from different families
        random.shuffle(features)
        for i in range(len(features)):
            for j in range(i + 1, len(features)):
                if features[i][1] != features[j][1]:
                    f1, fam1 = features[i]
                    f2, fam2 = features[j]
                    prompt = (
                        f"Feature A: {f1} (family: {fam1})\n"
                        f"Feature B: {f2} (family: {fam2})\n\n"
                        f"These features are from different families. Could there "
                        f"be a non-obvious economic relationship between them? "
                        f"What mechanism would connect them? Is this worth testing "
                        f"empirically? What lag structure would you expect?"
                    )
                    tasks.append(("correlation_discovery", prompt, {
                        "feature_a": f1,
                        "feature_b": f2,
                    }))
                    return tasks  # just one per refill
    except Exception:
        pass
    return tasks


def _gen_company_analysis(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Generate company analysis tasks — work through the NASDAQ 100 queue.

    Picks the next batch of unanalyzed companies from the ANALYSIS_QUEUE
    and enqueues them as P3 background tasks. Each cycle analyzes ~5
    companies, running 24/7 until the full queue is covered. Companies
    already analyzed in the last 30 days are skipped.
    """
    tasks: list[tuple[str, str, dict]] = []
    try:
        from intelligence.company_analyzer import (
            ANALYSIS_QUEUE,
            run_analysis_queue,
            _TICKER_NAMES,
        )
        from sqlalchemy import text as sa_text
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(days=30)

        with engine.connect() as conn:
            rows = conn.execute(sa_text(
                "SELECT ticker FROM company_profiles "
                "WHERE last_analyzed >= :cutoff"
            ), {"cutoff": cutoff}).fetchall()
        recently_analyzed = {row[0] for row in rows}

        batch_count = 0
        for ticker in ANALYSIS_QUEUE:
            if ticker in recently_analyzed:
                continue
            name = _TICKER_NAMES.get(ticker, ticker)

            prompt = (
                f"GRID Company Analysis Task: {name} ({ticker})\n\n"
                f"Running full influence pipeline — querying government contracts, "
                f"lobbying, congressional holdings, insider activity, export controls, "
                f"and actor network for {ticker}. This task triggers the "
                f"company_analyzer.analyze_company() pipeline and stores results "
                f"in company_profiles.\n\n"
                f"After analysis completes, summarize the key findings for {name}."
            )

            def _make_callback(t: str):
                def _cb(task):
                    try:
                        run_analysis_queue(engine, batch_size=1)
                    except Exception as exc:
                        log.debug("Company analysis callback failed: {e}", e=str(exc))
                return _cb

            tasks.append(("company_analysis", prompt, {
                "ticker": ticker,
                "company": name,
                "action": "analyze_company",
            }))
            batch_count += 1
            if batch_count >= 5:
                break

        if tasks:
            log.info(
                "LLM-TQ company analysis: {n} tickers queued, {s} skipped (recent)",
                n=len(tasks), s=len(recently_analyzed),
            )
    except Exception:
        pass
    return tasks


def _gen_panama_papers_research(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Have the LLM research connections between known actors and offshore entities.

    Scans signal_sources for entries with source_type='offshore_leak',
    then generates research tasks for each actor found in the ICIJ
    Offshore Leaks database (Panama Papers, Pandora Papers, Paradise
    Papers). Each task asks Qwen to investigate the entity, jurisdiction,
    legitimacy, and network implications.

    Also triggers a cross-reference check for any newly discovered actors
    that haven't been screened against the offshore database yet.
    """
    tasks: list[tuple[str, str, dict]] = []

    try:
        from sqlalchemy import text as sa_text

        # ── Phase 1: Research actors with existing offshore_leak signals ──
        with engine.connect() as conn:
            rows = conn.execute(sa_text("""
                SELECT DISTINCT
                    ss.ticker         AS actor_id,
                    ss.metadata->>'actor_name'     AS actor_name,
                    ss.metadata->>'entity_name'    AS entity_name,
                    ss.metadata->>'jurisdiction'   AS jurisdiction,
                    ss.metadata->>'match_type'     AS match_type,
                    ss.metadata->>'entity_status'  AS entity_status,
                    ss.metadata->>'leak_source'    AS leak_source,
                    ss.metadata->>'officer_name'   AS officer_name
                FROM signal_sources ss
                WHERE ss.source_type = 'offshore_leak'
                ORDER BY ss.signal_date DESC
                LIMIT 100
            """)).fetchall()

        for row in rows:
            actor_id = row[0] or ""
            actor_name = row[1] or actor_id
            entity_name = row[2] or "unknown entity"
            jurisdiction = row[3] or "unknown"
            match_type = row[4] or "unknown"
            entity_status = row[5] or ""
            leak_source = row[6] or ""
            officer_name = row[7] or ""

            # Dedup: skip if already researched this actor+entity combo
            research_key = f"{actor_id}:{entity_name}"
            if research_key in tq._researched_offshore:
                continue
            tq._researched_offshore.add(research_key)

            # Determine leak database name from source_id
            leak_db = "ICIJ Offshore Leaks"
            if leak_source:
                source_map = {
                    "Panama Papers": "Panama Papers",
                    "Pandora Papers": "Pandora Papers",
                    "Paradise Papers": "Paradise Papers",
                    "Bahamas Leaks": "Bahamas Leaks",
                    "Offshore Leaks": "Offshore Leaks",
                }
                for key, label in source_map.items():
                    if key.lower() in leak_source.lower():
                        leak_db = label
                        break

            prompt = (
                f"PANAMA PAPERS / ICIJ RESEARCH TASK\n"
                f"{'=' * 50}\n\n"
                f"Actor: {actor_name} (ID: {actor_id})\n"
                f"Match type: {match_type}\n"
                f"Officer name in leak: {officer_name}\n"
                f"Connected offshore entity: {entity_name}\n"
                f"Jurisdiction: {jurisdiction}\n"
                f"Entity status: {entity_status or 'unknown'}\n"
                f"Leak database: {leak_db}\n\n"
                f"RESEARCH QUESTIONS:\n"
                f"1. What was '{entity_name}' (jurisdiction: {jurisdiction}) "
                f"likely used for? Legitimate holding company, tax optimization, "
                f"asset protection, or suspicious shell structure?\n\n"
                f"2. Is this a standard offshore structure for someone in "
                f"{actor_name}'s position, or does it raise red flags? Consider "
                f"the jurisdiction's secrecy score and regulatory reputation.\n\n"
                f"3. What other actors in GRID's network might be connected to "
                f"the same entity or jurisdiction? Look for shared intermediaries "
                f"or service providers.\n\n"
                f"4. What are the implications for {actor_name}'s:\n"
                f"   - Public trading positions (conflict of interest?)\n"
                f"   - Political roles or government positions\n"
                f"   - Fiduciary duties\n"
                f"   - Trust/credibility score in GRID\n\n"
                f"5. Should any active GRID theses involving {actor_name} be "
                f"re-evaluated? What new risk factors does this introduce?\n\n"
                f"Provide a RISK RATING: LOW / MEDIUM / HIGH / CRITICAL "
                f"with justification."
            )

            tasks.append(("panama_papers_research", prompt, {
                "actor_id": actor_id,
                "actor_name": actor_name,
                "entity_name": entity_name,
                "jurisdiction": jurisdiction,
                "match_type": match_type,
                "leak_source": leak_db,
                "action": "offshore_research",
            }))

            # Cap at 3 per refill cycle to avoid starving other background tasks
            if len(tasks) >= 3:
                break

        # ── Phase 2: Cross-reference newly discovered actors ──
        # Check if any recently discovered actors (last 7 days) haven't
        # been screened against the offshore leaks database yet.
        try:
            with engine.connect() as conn:
                new_actors = conn.execute(sa_text("""
                    SELECT a.id, a.name
                    FROM actors a
                    WHERE a.created_at >= NOW() - INTERVAL '7 days'
                      AND NOT EXISTS (
                          SELECT 1 FROM signal_sources ss
                          WHERE ss.source_type = 'offshore_leak'
                            AND ss.ticker = a.id
                      )
                    ORDER BY a.created_at DESC
                    LIMIT 20
                """)).fetchall()

            for actor_row in new_actors:
                aid = actor_row[0]
                aname = actor_row[1]
                screen_key = f"offshore_screen:{aid}"
                if screen_key in tq._researched_offshore:
                    continue
                tq._researched_offshore.add(screen_key)

                # Don't generate LLM task — just trigger the DB/CSV check.
                # If matches are found, the check function queues its own task.
                try:
                    from ingestion.altdata.offshore_leaks import (
                        check_actor_in_offshore_leaks,
                        queue_offshore_investigation,
                    )
                    offshore_hits = check_actor_in_offshore_leaks(
                        engine, aname, actor_id=aid,
                    )
                    if offshore_hits:
                        queue_offshore_investigation(
                            engine, aname, aid, offshore_hits,
                        )
                        log.warning(
                            "New actor {name} found in offshore leaks — "
                            "investigation queued",
                            name=aname,
                        )
                except ImportError:
                    pass
                except Exception as exc:
                    log.debug(
                        "Offshore screen for new actor {n} failed: {e}",
                        n=aname, e=str(exc),
                    )

        except Exception:
            pass  # actors table may not have created_at column

        if tasks:
            log.info(
                "LLM-TQ Panama Papers research: {n} tasks generated",
                n=len(tasks),
            )

    except Exception as exc:
        log.debug("Panama Papers research gen failed: {e}", e=str(exc))

    return tasks


def _gen_deep_forensics(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Deep forensic price move analysis for top tickers."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        tickers = ["AAPL", "NVDA", "MSFT", "TSLA", "GOOGL", "META", "AMZN",
                    "SPY", "QQQ", "BTC-USD", "ETH-USD"]
        for ticker in tickers[:3]:  # 3 per refill
            # Find recent significant moves
            with engine.connect() as conn:
                rows = conn.execute(sa_text(
                    "SELECT signal_date, spot_price, put_call_ratio, iv_atm "
                    "FROM options_daily_signals WHERE ticker = :t "
                    "ORDER BY signal_date DESC LIMIT 5"
                ), {"t": ticker}).fetchall()

            if not rows:
                continue

            prices = [f"{r[0]}: ${r[1]:.2f} PCR={r[2]:.2f} IV={r[3]:.3f}" for r in rows if r[1]]
            prompt = (
                f"DEEP FORENSIC ANALYSIS: {ticker}\n\n"
                f"Recent price data:\n" + "\n".join(prices) + "\n\n"
                f"Analyze:\n"
                f"1. What drove each day's move? (earnings, macro, flow, technical)\n"
                f"2. How much of each move was market-wide vs stock-specific?\n"
                f"3. What's the current implied expectation from options positioning?\n"
                f"4. What's the biggest risk the market is NOT pricing in?\n"
                f"5. If you had to bet, what's the 2-month outlook?\n\n"
                f"Be specific with numbers. No generic statements."
            )
            tasks.append(("deep_forensic", prompt, {"ticker": ticker}))
    except Exception:
        pass
    return tasks


def _gen_offshore_analysis(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Deep analysis of offshore networks from ICIJ data.

    Works through entities tier by tier:
    Tier 1: UBS, Credit Suisse, HSBC (9K+ entities each)
    Tier 2: Mossack Fonseca offices, PwC, KPMG, Deloitte (1-5K each)
    Tier 3: Named individuals with 100+ shells
    Tier 4: Cross-references with public company insiders
    """
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text

        # Pick a random tier to work on each refill
        import random
        tier = random.choice([1, 1, 2, 2, 3, 3, 4])

        if tier == 1:
            # Tier 1: Major bank networks
            banks = [
                ("UBS", "UBS TRUSTEES (BAHAMAS) LTD.", 9731),
                ("Credit Suisse", "CREDIT SUISSE TRUST LIMITED", 8316),
                ("HSBC", "HSBC PRIVATE BANK (SUISSE) S.A.", 730),
            ]
            for bank_name, exact_name, count in banks[:1]:
                # Get sample entities
                with engine.connect() as conn:
                    entities = conn.execute(sa_text(
                        "SELECT e.name, e.metadata->>'jurisdiction' "
                        "FROM actor_connections ac "
                        "JOIN actors e ON e.id = ac.actor_a AND e.category = 'icij_entity' "
                        "JOIN actors i ON i.id = ac.actor_b AND i.name = :n "
                        "WHERE ac.relationship = 'icij_intermediary_of' "
                        "LIMIT 20"
                    ), {"n": exact_name}).fetchall()

                entity_list = "\n".join(f"  - {r[0]} ({r[1]})" for r in entities)
                prompt = (
                    f"TIER 1 OFFSHORE ANALYSIS: {bank_name}\n\n"
                    f"This bank facilitated {count} offshore entities.\n"
                    f"Sample entities:\n{entity_list}\n\n"
                    f"Analyze:\n"
                    f"1. What types of structures are these? (trusts, SPVs, holding cos, funds)\n"
                    f"2. Why these specific jurisdictions?\n"
                    f"3. What legitimate vs suspicious purposes do these serve?\n"
                    f"4. What patterns suggest tax evasion vs legitimate tax planning?\n"
                    f"5. Which entity names suggest they're connected to major deals?\n"
                    f"6. Rate suspicion level 1-10 with reasoning.\n\n"
                    f"Confidence label each finding: confirmed/derived/estimated/rumored."
                )
                tasks.append(("offshore_tier1", prompt, {
                    "bank": bank_name, "entity_count": count, "tier": 1,
                }))

        elif tier == 2:
            # Tier 2: Law firms and formation agents
            with engine.connect() as conn:
                firms = conn.execute(sa_text(
                    "SELECT i.name, COUNT(DISTINCT ac.actor_a) as cnt "
                    "FROM actors i "
                    "JOIN actor_connections ac ON ac.actor_b = i.id "
                    "AND ac.relationship = 'icij_intermediary_of' "
                    "WHERE i.category = 'icij_intermediary' "
                    "GROUP BY i.name "
                    "ORDER BY cnt DESC LIMIT 5"
                )).fetchall()

            for firm_name, count in firms[:2]:
                prompt = (
                    f"TIER 2 OFFSHORE ANALYSIS: {firm_name}\n\n"
                    f"This intermediary created {count} shell entities.\n\n"
                    f"Research this firm:\n"
                    f"1. Where is it based? What's its corporate structure?\n"
                    f"2. Who are the principals/partners?\n"
                    f"3. What's its reputation in the offshore industry?\n"
                    f"4. Has it been sanctioned, fined, or investigated?\n"
                    f"5. What types of clients does it typically serve?\n"
                    f"6. Connection to Mossack Fonseca or other leaked firms?\n\n"
                    f"Rate: legitimate corporate services vs enabler of financial crime (1-10)."
                )
                tasks.append(("offshore_tier2", prompt, {
                    "firm": firm_name, "entity_count": count, "tier": 2,
                }))

        elif tier == 3:
            # Tier 3: Named individuals with most shells
            with engine.connect() as conn:
                people = conn.execute(sa_text(
                    "SELECT o.name, COUNT(DISTINCT ac.actor_a) as shells "
                    "FROM actor_connections ac "
                    "JOIN actors o ON o.id = ac.actor_b AND o.category = 'icij_officer' "
                    "WHERE ac.relationship = 'icij_officer_of' "
                    "AND o.name !~ '.*(Limited|Ltd|Corp|Bearer|Nominees|Services|Trust|Bank|S\\.A\\.).*' "
                    "AND LENGTH(o.name) > 8 "
                    "GROUP BY o.name "
                    "HAVING COUNT(DISTINCT ac.actor_a) >= 50 "
                    "ORDER BY RANDOM() LIMIT 3"
                )).fetchall()

            for person, shells in people:
                prompt = (
                    f"TIER 3 PERSON ANALYSIS: {person}\n\n"
                    f"This individual is linked to {shells} offshore entities in ICIJ data.\n\n"
                    f"Research:\n"
                    f"1. Who is this person? What's their background?\n"
                    f"2. What legitimate business reasons could explain {shells} shells?\n"
                    f"3. What red flags exist in having this many offshore entities?\n"
                    f"4. Are they a nominee/agent, or a beneficial owner?\n"
                    f"5. Any public records, news articles, or sanctions?\n"
                    f"6. Connected to any publicly traded companies?\n"
                    f"7. Net worth estimate if available.\n\n"
                    f"Confidence: confirmed/derived/estimated/rumored for each finding."
                )
                tasks.append(("offshore_tier3", prompt, {
                    "person": person, "shells": shells, "tier": 3,
                }))

        elif tier == 4:
            # Tier 4: Cross-reference offshore with public markets
            with engine.connect() as conn:
                # Companies in signal_sources that might have offshore presence
                tickers = conn.execute(sa_text(
                    "SELECT DISTINCT ticker FROM signal_sources "
                    "WHERE signal_type IN ('CONTRACT_AWARD', 'BUY', 'SELL', 'CLUSTER_BUY') "
                    "ORDER BY RANDOM() LIMIT 3"
                )).fetchall()

            for (ticker,) in tickers:
                prompt = (
                    f"TIER 4 CROSS-REFERENCE: {ticker} offshore exposure\n\n"
                    f"Investigate whether {ticker} or its executives have offshore structures.\n"
                    f"Check:\n"
                    f"1. Does the company use offshore subsidiaries? (10-K filings)\n"
                    f"2. Have any executives appeared in Panama/Paradise/Pandora Papers?\n"
                    f"3. What's the company's effective tax rate vs statutory rate?\n"
                    f"4. Any transfer pricing controversies?\n"
                    f"5. Lobbying spend on tax policy?\n"
                    f"6. Board members with offshore connections?\n\n"
                    f"For each finding, label: confirmed (from filings), derived (from data), "
                    f"estimated (calculated), rumored (media reports), inferred (pattern match)."
                )
                tasks.append(("offshore_tier4", prompt, {
                    "ticker": ticker, "tier": 4,
                }))

    except Exception:
        pass
    return tasks


def _gen_sector_rotation(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Analyze sector rotation patterns from flow data."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        sectors = ["Technology", "Healthcare", "Energy", "Financials",
                   "Industrials", "Consumer Discretionary", "Utilities"]

        with engine.connect() as conn:
            for sector in sectors[:2]:
                # Get recent sector ETF data
                rows = conn.execute(sa_text(
                    "SELECT fr.name, rs.obs_date, rs.value "
                    "FROM feature_registry fr "
                    "JOIN resolved_series rs ON rs.feature_id = fr.id "
                    "WHERE fr.name LIKE :pat "
                    "AND rs.obs_date > CURRENT_DATE - 7 "
                    "ORDER BY rs.obs_date DESC LIMIT 10"
                ), {"pat": f"%{sector[:3].lower()}%"}).fetchall()

                data_str = "\n".join(f"  {r[0]}: {r[1]} = {r[2]:.2f}" for r in rows[:5])
                prompt = (
                    f"SECTOR ROTATION: {sector}\n\n"
                    f"Recent data:\n{data_str or '  Limited data available'}\n\n"
                    f"Analyze:\n"
                    f"1. Is money flowing INTO or OUT OF this sector?\n"
                    f"2. What's driving the rotation? (macro, earnings, policy, technical)\n"
                    f"3. Which subsectors are leading vs lagging?\n"
                    f"4. What's the institutional positioning? (13F trends)\n"
                    f"5. Contrarian signal: is the crowd wrong?\n"
                    f"6. Top 3 names to watch in this sector and why."
                )
                tasks.append(("sector_rotation", prompt, {"sector": sector}))
    except Exception:
        pass
    return tasks


def _gen_signal_cross_validation(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Cross-validate multiple signal types for the same ticker."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        with engine.connect() as conn:
            # Find tickers with multiple signal types in last 7 days
            tickers = conn.execute(sa_text(
                "SELECT ticker, COUNT(DISTINCT signal_type) as sig_types, "
                "array_agg(DISTINCT signal_type) as types "
                "FROM signal_sources "
                "WHERE signal_date > CURRENT_DATE - 7 "
                "GROUP BY ticker "
                "HAVING COUNT(DISTINCT signal_type) >= 2 "
                "ORDER BY sig_types DESC LIMIT 5"
            )).fetchall()

            for ticker, sig_count, sig_types in tickers[:2]:
                # Get the actual signals
                signals = conn.execute(sa_text(
                    "SELECT signal_type, signal_date, "
                    "LEFT(signal_value::text, 100) "
                    "FROM signal_sources "
                    "WHERE ticker = :t AND signal_date > CURRENT_DATE - 7 "
                    "ORDER BY signal_date DESC LIMIT 10"
                ), {"t": ticker}).fetchall()

                sig_str = "\n".join(f"  {r[0]} on {r[1]}: {r[2]}" for r in signals)
                prompt = (
                    f"SIGNAL CROSS-VALIDATION: {ticker}\n\n"
                    f"{sig_count} different signal types in last 7 days:\n{sig_str}\n\n"
                    f"Analyze:\n"
                    f"1. Do these signals agree or contradict?\n"
                    f"2. Which signal is most reliable for this ticker historically?\n"
                    f"3. Is there a clear directional bias? Bull or bear?\n"
                    f"4. What's the conviction level (1-10) based on signal agreement?\n"
                    f"5. What additional signal would confirm or deny the thesis?\n"
                    f"6. Specific trade recommendation if conviction > 7."
                )
                tasks.append(("signal_cross_validation", prompt, {
                    "ticker": ticker, "signal_types": sig_count,
                }))
    except Exception:
        pass
    return tasks


def _gen_earnings_preview(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Pre-analyze upcoming earnings for major tickers."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        # Focus on tickers we have data for
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]
        import random
        random.shuffle(tickers)

        for ticker in tickers[:2]:
            with engine.connect() as conn:
                # Get recent options data for pre-earnings analysis
                opts = conn.execute(sa_text(
                    "SELECT put_call_ratio, iv_atm, iv_skew, max_pain, spot_price "
                    "FROM options_daily_signals "
                    "WHERE ticker = :t ORDER BY signal_date DESC LIMIT 1"
                ), {"t": ticker}).fetchone()

            if not opts:
                continue

            prompt = (
                f"EARNINGS PREVIEW: {ticker}\n\n"
                f"Current options positioning:\n"
                f"  Put/Call Ratio: {opts[0]:.2f}\n"
                f"  IV ATM: {opts[1]:.1%}\n"
                f"  IV Skew: {opts[2]:.3f}\n"
                f"  Max Pain: ${opts[3]:.2f}\n"
                f"  Spot: ${opts[4]:.2f}\n\n"
                f"Analyze for next earnings:\n"
                f"1. What's the implied move from options pricing?\n"
                f"2. Is the skew suggesting more fear of downside or upside?\n"
                f"3. Where is max pain relative to spot? (dealer positioning)\n"
                f"4. Historical earnings surprise pattern for this company?\n"
                f"5. Key metrics to watch (revenue growth, margins, guidance)\n"
                f"6. Pre-earnings trade idea (2+ month expiry):\n"
                f"   - Direction, strike selection, position sizing\n"
                f"   - Entry criteria, profit target, stop loss\n"
                f"   - Why this trade has edge"
            )
            tasks.append(("earnings_preview", prompt, {"ticker": ticker}))
    except Exception:
        pass
    return tasks


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


def _gen_expectation_tracking(
    engine: Any, tq: LLMTaskQueue,
) -> list[tuple[str, str, dict]]:
    """Have Qwen generate market expectations for Mag 7+ tickers.

    For each ticker, Qwen analyzes recent news, signals, and price action
    to produce structured expectations: what the market expects to happen,
    how much is already priced in, and by when.

    Results are parsed and stored in news_impact_expectations table.
    Runs once per 6 hours per ticker.
    """
    tasks: list[tuple[str, str, dict]] = []
    try:
        from intelligence.news_impact import MAG7_TICKERS, ensure_tables
        from sqlalchemy import text as sa_text

        ensure_tables(engine)

        # Check which tickers haven't been analyzed in 6 hours
        with engine.connect() as conn:
            recent = conn.execute(sa_text(
                "SELECT DISTINCT ticker FROM news_impact_expectations "
                "WHERE created_at > NOW() - INTERVAL '6 hours'"
            )).fetchall()
        recent_tickers = {r[0] for r in recent}

        for ticker in MAG7_TICKERS:
            if ticker in recent_tickers:
                continue

            # Gather context for the prompt
            news_context = ""
            signal_context = ""
            try:
                with engine.connect() as conn:
                    news = conn.execute(sa_text(
                        "SELECT title, sentiment, confidence FROM news_articles "
                        "WHERE :t = ANY(tickers) AND published_at > NOW() - INTERVAL '7 days' "
                        "ORDER BY published_at DESC LIMIT 5"
                    ), {"t": ticker}).fetchall()
                    news_context = "\n".join(
                        f"  - [{r[1]}] {r[0]} (conf={r[2]:.1f})" for r in news
                    ) if news else "  No recent news"

                    sigs = conn.execute(sa_text(
                        "SELECT signal_type, signal_value, signal_date "
                        "FROM signal_sources WHERE ticker = :t "
                        "AND signal_date > CURRENT_DATE - 7 "
                        "ORDER BY signal_date DESC LIMIT 5"
                    ), {"t": ticker}).fetchall()
                    signal_context = "\n".join(
                        f"  - {r[0]} on {r[2]}: {str(r[1])[:60]}" for r in sigs
                    ) if sigs else "  No recent signals"
            except Exception:
                pass

            prompt = f"""Analyze {ticker} and generate 3-5 market expectations.

RECENT NEWS:
{news_context}

RECENT SIGNALS:
{signal_context}

For each expectation, provide in this EXACT format (one per line):
EXPECT|<description>|<type>|<horizon>|<direction>|<magnitude_bps>|<baked_in_pct>|<deadline_YYYY-MM-DD or NONE>

Types: earnings, guidance, product_launch, regulation, macro_data, m_and_a, legal, geopolitical
Horizons: short (< 1 week), medium (1-8 weeks), long (> 8 weeks)
Directions: bullish, bearish
Magnitude: estimated basis points impact (e.g. 200 for a 2% move)
Baked_in: 0-100, how much is already in the price

Example:
EXPECT|Q2 earnings beat expected by 8%|earnings|short|bullish|300|65|2026-05-01
EXPECT|EU antitrust fine possible|regulation|long|bearish|150|30|NONE

Be specific and realistic. Base estimates on the news and signals provided."""

            tasks.append(("expectation_tracking", prompt, {
                "ticker": ticker,
                "action": "generate_expectations",
            }))

        if tasks:
            log.info("LLM-TQ expectation tracking: {n} tickers queued", n=len(tasks))
    except Exception:
        pass
    return tasks


def _handle_expectation_result(engine: Any, task_type: str, result: str, context: dict) -> None:
    """Parse Qwen's expectation output and store in DB.

    Called after the LLM task completes. Parses EXPECT| lines from
    the response and creates Expectation records.
    """
    if task_type != "expectation_tracking":
        return

    ticker = context.get("ticker", "")
    if not ticker or not result:
        return

    try:
        from intelligence.news_impact import Expectation, ExpectationTracker, ensure_tables
        import hashlib

        ensure_tables(engine)
        tracker = ExpectationTracker(engine)

        for line in result.split("\n"):
            line = line.strip()
            if not line.startswith("EXPECT|"):
                continue

            parts = line.split("|")
            if len(parts) < 8:
                continue

            _, desc, cat_type, horizon, direction, mag_str, baked_str, deadline_str = parts[:8]

            try:
                magnitude = float(mag_str)
                baked_in = float(baked_str)
            except (ValueError, TypeError):
                continue

            deadline = None
            if deadline_str.strip() != "NONE":
                try:
                    from datetime import date as dt_date
                    deadline = dt_date.fromisoformat(deadline_str.strip())
                except ValueError:
                    pass

            exp_id = hashlib.sha256(
                f"{ticker}:{desc[:50]}:{horizon}".encode()
            ).hexdigest()[:16]

            exp = Expectation(
                id=exp_id,
                ticker=ticker,
                description=desc.strip(),
                catalyst_type=cat_type.strip(),
                horizon=horizon.strip(),
                expected_direction=direction.strip(),
                expected_magnitude_bps=magnitude,
                baked_in_pct=min(100, max(0, baked_in)),
                deadline=deadline,
                status="active",
            )
            tracker.create_expectation(exp)

        log.info("Expectation tracking: parsed results for {t}", t=ticker)
    except Exception as exc:
        log.debug("Expectation result parsing failed: {e}", e=str(exc))


# ---------------------------------------------------------------------------
# FastAPI router (wire into api/main.py or api/routers/system.py)
# ---------------------------------------------------------------------------

def build_router():
    """Build a FastAPI APIRouter with LLM task queue endpoints.

    Returns:
        APIRouter: Router with /api/v1/system/llm-status and llm-task routes.
    """
    from fastapi import APIRouter, Depends, HTTPException
    from pydantic import BaseModel, Field
    from api.auth import require_auth

    router = APIRouter(prefix="/api/v1/system", tags=["system"])

    class EnqueueRequest(BaseModel):
        task_type: str = Field(..., description="Task type (e.g. user_chat, trade_review)")
        prompt: str = Field(..., description="Prompt text for the LLM")
        context: dict = Field(default_factory=dict, description="Arbitrary metadata")
        priority: int = Field(default=3, ge=1, le=3, description="1=realtime, 2=scheduled, 3=background")

    class EnqueueResponse(BaseModel):
        task_id: str
        queue_depth: int
        priority: int

    class TaskResultResponse(BaseModel):
        task_id: str
        task_type: str
        priority: int
        status: str
        result: str | None = None
        error: str | None = None
        created_at: str
        completed_at: str | None = None

    @router.get("/llm-status")
    async def llm_status(_token: str = Depends(require_auth)):
        """Current LLM task queue status: depth, running task, throughput, idle %.

        When the task queue runs in a separate process (Hermes), we read
        recent DB snapshots to reconstruct the status.
        """
        try:
            tq = get_task_queue()
            status = tq.get_status()
            # If local queue is empty (API process, not Hermes), check DB
            if status["total_completed"] == 0:
                from sqlalchemy import text as sa_text
                engine = tq._engine
                with engine.connect() as conn:
                    # Count recent LLM task completions from snapshots
                    row = conn.execute(sa_text(
                        "SELECT COUNT(*) FROM analytical_snapshots "
                        "WHERE category LIKE 'llm_task_%%' "
                        "AND created_at > NOW() - INTERVAL '1 hour'"
                    )).fetchone()
                    completed_1h = row[0] if row else 0

                    # Get recent task list
                    recent_rows = conn.execute(sa_text(
                        "SELECT category, created_at FROM analytical_snapshots "
                        "WHERE category LIKE 'llm_task_%%' "
                        "ORDER BY created_at DESC LIMIT 20"
                    )).fetchall()

                    recent_tasks = [
                        {
                            "type": r[0].replace("llm_task_", ""),
                            "completed_at": r[1].isoformat() if r[1] else None,
                            "has_result": True,
                        }
                        for r in recent_rows
                    ]

                    # Check if queue is actively processing
                    is_active = completed_1h > 0

                    status.update({
                        "total_completed": completed_1h,
                        "throughput_per_hour": completed_1h,
                        "recent_tasks": recent_tasks,
                        "running_task": {"type": "background", "note": "running in Hermes process"} if is_active else None,
                        "source": "db_snapshots",
                    })
            return status
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    @router.post("/llm-task", response_model=EnqueueResponse)
    async def enqueue_task(
        req: EnqueueRequest,
        _token: str = Depends(require_auth),
    ):
        """Enqueue a custom LLM task (for Ask GRID, ad-hoc queries)."""
        try:
            tq = get_task_queue()
            task_id = tq.enqueue(
                task_type=req.task_type,
                prompt=req.prompt,
                context=req.context,
                priority=req.priority,
            )
            status = tq.get_status()
            return EnqueueResponse(
                task_id=task_id,
                queue_depth=status["queue_depth"],
                priority=req.priority,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    @router.get("/llm-task/{task_id}", response_model=TaskResultResponse)
    async def get_task(task_id: str, _token: str = Depends(require_auth)):
        """Get the result of a completed LLM task."""
        tq = get_task_queue()
        task = tq.get_task_result(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found in history")
        return TaskResultResponse(
            task_id=task.id,
            task_type=task.task_type,
            priority=task.priority,
            status="completed" if task.result else ("error" if task.error else "pending"),
            result=task.result,
            error=task.error,
            created_at=task.created_at,
            completed_at=task.completed_at,
        )

    return router
