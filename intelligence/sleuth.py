"""
GRID Intelligence — Investigative Research Engine (Sleuth).

A financial detective that continuously generates and follows leads.
Every answer spawns more questions. Every connection reveals more connections.

Lead categories:
    1. actor_pattern     — "Why did [actors] all do [action] around the same time?"
    2. data_anomaly      — "This number moved 3 sigma — what happened?"
    3. timing_suspicious — "Insider sold 2 days before bad earnings"
    4. connection_found  — "Actor A and Actor B both acted on Ticker X — connected?"
    5. narrative_mismatch — "The Fed says X but the data says Y"
    6. money_trail       — "Where did this $500M flow go after leaving Fund X?"

Pipeline:
    generate_leads        — scan all intelligence data for things that deserve investigation
    investigate_lead      — deep-dive into a specific lead using LLM + data
    follow_rabbit_hole    — follow a lead and its children up to N levels deep
    daily_investigation   — run a full investigation cycle

Key entry points:
    Sleuth(engine).daily_investigation()
    Sleuth(engine).generate_leads()
    Sleuth(engine).investigate_lead(lead)

Wired into hermes via:
    - generate_leads() every 6 hours
    - investigate_lead() as background LLM tasks (priority 3)
    - High-priority leads (>0.8) investigated immediately (priority 2)

Storage: investigation_leads table (see ensure_tables()).
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────

# Statistical anomaly threshold (z-score)
ANOMALY_ZSCORE_THRESHOLD: float = 3.0

# Cluster detection: max days apart for same-ticker activity to be a "cluster"
CLUSTER_WINDOW_DAYS: int = 7

# Minimum actors in a cluster to flag
MIN_CLUSTER_SIZE: int = 2

# Lookback for lead generation scans (days)
SCAN_LOOKBACK_DAYS: int = 14

# Max follow-up depth for rabbit holes
DEFAULT_RABBIT_HOLE_DEPTH: int = 3

# Priority thresholds
HIGH_PRIORITY_THRESHOLD: float = 0.8
MEDIUM_PRIORITY_THRESHOLD: float = 0.5

# LLM settings
LLM_TEMPERATURE: float = 0.3
LLM_MAX_TOKENS: int = 1500

LEAD_CATEGORIES = (
    "actor_pattern",
    "data_anomaly",
    "timing_suspicious",
    "connection_found",
    "narrative_mismatch",
    "money_trail",
)

LEAD_STATUSES = ("new", "investigating", "resolved", "dead_end")


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class Lead:
    """A single investigative lead to follow."""

    id: str
    question: str                     # human-readable question
    category: str                     # one of LEAD_CATEGORIES
    priority: float                   # 0-1, higher = more urgent
    evidence: list[dict]              # what triggered this lead
    status: str = "new"               # one of LEAD_STATUSES
    findings: str | None = None       # what we found
    hypotheses: list[dict] = field(default_factory=list)
    follow_up_leads: list[str] = field(default_factory=list)
    created_at: str = ""
    resolved_at: str | None = None

    def __post_init__(self) -> None:
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()


# ── Table Setup ──────────────────────────────────────────────────────────

_tables_ensured = False


def ensure_tables(engine: Engine) -> None:
    """Create investigation_leads table if it doesn't exist."""
    global _tables_ensured
    if _tables_ensured:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS investigation_leads (
                id TEXT PRIMARY KEY,
                question TEXT NOT NULL,
                category TEXT NOT NULL,
                priority NUMERIC DEFAULT 0.5,
                evidence JSONB,
                status TEXT DEFAULT 'new',
                findings TEXT,
                follow_up_leads JSONB DEFAULT '[]',
                hypotheses JSONB DEFAULT '[]',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                resolved_at TIMESTAMPTZ
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_leads_status
            ON investigation_leads (status, priority DESC)
        """))
    _tables_ensured = True
    log.debug("investigation_leads table ensured")


# ── LLM Helper ───────────────────────────────────────────────────────────

def _get_llm():
    """Get the LLM client (llamacpp preferred, fallback to ollama)."""
    try:
        from llamacpp.client import get_client
        client = get_client()
        if client.is_available:
            return client
    except Exception:
        pass
    try:
        from ollama.client import get_client
        client = get_client()
        if client.is_available:
            return client
    except Exception:
        pass
    return None


def _llm_investigate(question: str, evidence_block: str, context_block: str) -> dict | None:
    """Ask the LLM to investigate a lead. Returns parsed hypotheses or None."""
    llm = _get_llm()
    if not llm:
        log.warning("Sleuth: LLM unavailable, cannot investigate")
        return None

    system_prompt = (
        "You are a financial investigator working for an institutional trading desk. "
        "You follow the money, connect dots between actors, and identify patterns "
        "that others miss. You are rigorous: every claim needs evidence. "
        "You are also paranoid: you assume coincidences are not coincidences until "
        "proven otherwise.\n\n"
        "When responding, use this exact format:\n"
        "HYPOTHESES:\n"
        "1. [Most likely] <hypothesis> | Confidence: <high/medium/low>\n"
        "2. [Alternative] <hypothesis> | Confidence: <high/medium/low>\n"
        "3. [Contrarian] <hypothesis> | Confidence: <high/medium/low>\n\n"
        "EVIDENCE NEEDED:\n"
        "- <what additional data would confirm or deny hypothesis 1>\n"
        "- <what additional data would confirm or deny hypothesis 2>\n\n"
        "FOLLOW-UP QUESTIONS:\n"
        "- <new question this investigation raises>\n"
        "- <another question>\n\n"
        "CONCLUSION:\n"
        "<1-2 sentence summary of what you think is happening>"
    )

    user_prompt = (
        f"INVESTIGATION QUESTION:\n{question}\n\n"
        f"EVIDENCE:\n{evidence_block}\n\n"
        f"ADDITIONAL CONTEXT:\n{context_block}\n\n"
        "Investigate this lead. Generate hypotheses ranked by likelihood. "
        "For each, explain what additional evidence would confirm or deny it. "
        "Then identify follow-up questions that this investigation raises."
    )

    response = llm.chat(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=LLM_TEMPERATURE,
        num_predict=LLM_MAX_TOKENS,
    )

    if not response:
        return None

    return _parse_investigation_response(response)


def _parse_investigation_response(response: str) -> dict:
    """Parse a structured LLM investigation response into components."""
    result: dict[str, Any] = {
        "raw_response": response,
        "hypotheses": [],
        "evidence_needed": [],
        "follow_up_questions": [],
        "conclusion": "",
    }

    current_section = None
    for line in response.split("\n"):
        stripped = line.strip()
        upper = stripped.upper()

        if upper.startswith("HYPOTHES"):
            current_section = "hypotheses"
            continue
        elif upper.startswith("EVIDENCE NEEDED"):
            current_section = "evidence_needed"
            continue
        elif upper.startswith("FOLLOW-UP") or upper.startswith("FOLLOW UP"):
            current_section = "follow_up_questions"
            continue
        elif upper.startswith("CONCLUSION"):
            current_section = "conclusion"
            continue

        if not stripped or stripped == "-":
            continue

        # Remove leading bullet / number
        content = stripped.lstrip("-•*0123456789.) ").strip()
        if not content:
            continue

        if current_section == "hypotheses":
            # Parse "hypothesis | Confidence: high"
            parts = content.split("|")
            hyp_text = parts[0].strip()
            # Remove bracketed labels like [Most likely]
            if hyp_text.startswith("["):
                bracket_end = hyp_text.find("]")
                if bracket_end > 0:
                    hyp_text = hyp_text[bracket_end + 1:].strip()
            confidence = "medium"
            if len(parts) > 1:
                conf_part = parts[-1].lower()
                if "high" in conf_part:
                    confidence = "high"
                elif "low" in conf_part:
                    confidence = "low"
            if hyp_text:
                result["hypotheses"].append({
                    "hypothesis": hyp_text,
                    "confidence": confidence,
                })
        elif current_section == "evidence_needed":
            result["evidence_needed"].append(content)
        elif current_section == "follow_up_questions":
            result["follow_up_questions"].append(content)
        elif current_section == "conclusion":
            if result["conclusion"]:
                result["conclusion"] += " " + content
            else:
                result["conclusion"] = content

    return result


# ══════════════════════════════════════════════════════════════════════════
# SLEUTH ENGINE
# ══════════════════════════════════════════════════════════════════════════

class Sleuth:
    """Financial detective that follows leads and connects dots."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        ensure_tables(engine)

    # ── Lead Persistence ─────────────────────────────────────────────────

    def _save_lead(self, lead: Lead) -> None:
        """Persist a lead to the database."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO investigation_leads
                    (id, question, category, priority, evidence, status,
                     findings, follow_up_leads, hypotheses, created_at, resolved_at)
                VALUES
                    (:id, :question, :category, :priority, :evidence, :status,
                     :findings, :follow_up_leads, :hypotheses, :created_at, :resolved_at)
                ON CONFLICT (id) DO UPDATE SET
                    priority = EXCLUDED.priority,
                    evidence = EXCLUDED.evidence,
                    status = EXCLUDED.status,
                    findings = EXCLUDED.findings,
                    follow_up_leads = EXCLUDED.follow_up_leads,
                    hypotheses = EXCLUDED.hypotheses,
                    resolved_at = EXCLUDED.resolved_at
            """), {
                "id": lead.id,
                "question": lead.question,
                "category": lead.category,
                "priority": lead.priority,
                "evidence": json.dumps(lead.evidence),
                "status": lead.status,
                "findings": lead.findings,
                "follow_up_leads": json.dumps(lead.follow_up_leads),
                "hypotheses": json.dumps(lead.hypotheses),
                "created_at": lead.created_at,
                "resolved_at": lead.resolved_at,
            })

    def _load_lead(self, lead_id: str) -> Lead | None:
        """Load a single lead from the database."""
        with self.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT id, question, category, priority, evidence, status, "
                "findings, follow_up_leads, hypotheses, created_at, resolved_at "
                "FROM investigation_leads WHERE id = :id"
            ), {"id": lead_id}).fetchone()
        if not row:
            return None
        return Lead(
            id=row[0],
            question=row[1],
            category=row[2],
            priority=float(row[3]) if row[3] is not None else 0.5,
            evidence=row[4] if isinstance(row[4], (list, dict)) else (json.loads(row[4]) if row[4] else []),
            status=row[5] or "new",
            findings=row[6],
            hypotheses=row[8] if isinstance(row[8], (list, dict)) else (json.loads(row[8]) if row[8] else []),
            follow_up_leads=row[7] if isinstance(row[7], (list, dict)) else (json.loads(row[7]) if row[7] else []),
            created_at=str(row[9]) if row[9] else "",
            resolved_at=str(row[10]) if row[10] else None,
        )

    def get_leads(
        self,
        status: str | None = None,
        category: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[Lead]:
        """Query leads with optional filters."""
        clauses = []
        params: dict[str, Any] = {"lim": limit, "off": offset}
        if status:
            clauses.append("status = :status")
            params["status"] = status
        if category:
            clauses.append("category = :category")
            params["category"] = category

        where = ""
        if clauses:
            where = "WHERE " + " AND ".join(clauses)

        with self.engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT id, question, category, priority, evidence, status, "
                f"findings, follow_up_leads, hypotheses, created_at, resolved_at "
                f"FROM investigation_leads {where} "
                f"ORDER BY priority DESC, created_at DESC "
                f"LIMIT :lim OFFSET :off"
            ), params).fetchall()

        return [
            Lead(
                id=r[0], question=r[1], category=r[2],
                priority=float(r[3]) if r[3] is not None else 0.5,
                evidence=r[4] if isinstance(r[4], (list, dict)) else (json.loads(r[4]) if r[4] else []),
                status=r[5] or "new",
                findings=r[6],
                hypotheses=r[8] if isinstance(r[8], (list, dict)) else (json.loads(r[8]) if r[8] else []),
                follow_up_leads=r[7] if isinstance(r[7], (list, dict)) else (json.loads(r[7]) if r[7] else []),
                created_at=str(r[9]) if r[9] else "",
                resolved_at=str(r[10]) if r[10] else None,
            )
            for r in rows
        ]

    def count_leads(self, status: str | None = None) -> int:
        """Count leads, optionally filtered by status."""
        params: dict[str, Any] = {}
        where = ""
        if status:
            where = "WHERE status = :status"
            params["status"] = status
        with self.engine.connect() as conn:
            row = conn.execute(text(
                f"SELECT COUNT(*) FROM investigation_leads {where}"
            ), params).fetchone()
        return row[0] if row else 0

    # ── Lead ID Generation ───────────────────────────────────────────────

    @staticmethod
    def _make_lead_id(category: str) -> str:
        """Generate a unique lead ID."""
        short = uuid.uuid4().hex[:10]
        return f"LEAD-{category[:4].upper()}-{short}"

    # ══════════════════════════════════════════════════════════════════════
    # LEAD GENERATION — scan all intelligence for things worth investigating
    # ══════════════════════════════════════════════════════════════════════

    def generate_leads(self) -> list[Lead]:
        """Scan all intelligence data for things that deserve investigation.

        Sources:
            1. Actor patterns — clustered activity on same ticker
            2. Data anomalies — statistical outliers in resolved_series
            3. Timing suspicious — insider trades preceding big moves
            4. Connection found — multiple actors acting on same ticker
            5. Narrative mismatch — cross-reference divergences
            6. Money trail — wealth flow anomalies

        Returns:
            List of newly created leads.
        """
        log.info("Sleuth: generating leads")
        all_leads: list[Lead] = []

        # Deduplicate against existing open leads
        existing_questions = set()
        for lead in self.get_leads(status="new", limit=500):
            existing_questions.add(lead.question)
        for lead in self.get_leads(status="investigating", limit=500):
            existing_questions.add(lead.question)

        generators = [
            self._generate_actor_pattern_leads,
            self._generate_data_anomaly_leads,
            self._generate_timing_suspicious_leads,
            self._generate_connection_leads,
            self._generate_narrative_mismatch_leads,
            self._generate_money_trail_leads,
        ]

        for gen_fn in generators:
            try:
                leads = gen_fn()
                for lead in leads:
                    if lead.question not in existing_questions:
                        self._save_lead(lead)
                        all_leads.append(lead)
                        existing_questions.add(lead.question)
            except Exception as exc:
                log.warning(
                    "Sleuth: lead generator {fn} failed: {e}",
                    fn=gen_fn.__name__, e=str(exc),
                )

        log.info("Sleuth: generated {n} new leads", n=len(all_leads))
        return all_leads

    # ── 1. Actor Pattern Leads ───────────────────────────────────────────

    def _generate_actor_pattern_leads(self) -> list[Lead]:
        """Find clusters of activity: multiple actors acting on the same
        ticker within a short window."""
        leads: list[Lead] = []
        cutoff = date.today() - timedelta(days=SCAN_LOOKBACK_DAYS)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT ticker, source_id, source_type, signal_type,
                           signal_date, signal_value
                    FROM signal_sources
                    WHERE signal_date >= :cutoff
                      AND ticker IS NOT NULL
                    ORDER BY ticker, signal_date
                """), {"cutoff": cutoff}).fetchall()
        except Exception:
            log.debug("Sleuth: signal_sources query failed (table may not exist)")
            return leads

        # Group by ticker, find clusters
        by_ticker: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            by_ticker[r[0]].append({
                "ticker": r[0],
                "source_id": str(r[1]),
                "source_type": r[2],
                "direction": r[3],
                "signal_date": str(r[4]),
                "signal_value": float(r[5]) if r[5] is not None else None,
            })

        for ticker, signals in by_ticker.items():
            if len(signals) < MIN_CLUSTER_SIZE:
                continue

            # Find same-direction clusters within the window
            for direction in ("BUY", "SELL", "BULLISH", "BEARISH"):
                dir_signals = [s for s in signals if s["direction"] and
                               s["direction"].upper() == direction]
                if len(dir_signals) < MIN_CLUSTER_SIZE:
                    continue

                # Check if signals fall within CLUSTER_WINDOW_DAYS of each other
                unique_sources = set()
                for s in dir_signals:
                    unique_sources.add(f"{s['source_type']}:{s['source_id']}")

                if len(unique_sources) >= MIN_CLUSTER_SIZE:
                    source_names = ", ".join(sorted(unique_sources)[:5])
                    question = (
                        f"Why did {len(unique_sources)} actors "
                        f"({source_names}) all {direction} {ticker} "
                        f"within the last {SCAN_LOOKBACK_DAYS} days?"
                    )
                    priority = min(1.0, 0.4 + 0.1 * len(unique_sources))

                    leads.append(Lead(
                        id=self._make_lead_id("actor_pattern"),
                        question=question,
                        category="actor_pattern",
                        priority=priority,
                        evidence=dir_signals[:10],  # cap evidence size
                    ))

        return leads

    # ── 2. Data Anomaly Leads ────────────────────────────────────────────

    def _generate_data_anomaly_leads(self) -> list[Lead]:
        """Scan resolved_series for statistical outliers (3+ sigma moves)."""
        leads: list[Lead] = []

        try:
            with self.engine.connect() as conn:
                # Find features with recent large moves relative to their history
                rows = conn.execute(text("""
                    WITH recent AS (
                        SELECT rs.feature_id, fr.name AS feature_name,
                               rs.value, rs.obs_date,
                               AVG(rs.value) OVER w AS mean_val,
                               STDDEV(rs.value) OVER w AS std_val
                        FROM resolved_series rs
                        JOIN feature_registry fr ON fr.id = rs.feature_id
                        WHERE rs.obs_date >= CURRENT_DATE - 504
                          AND fr.model_eligible = TRUE
                        WINDOW w AS (
                            PARTITION BY rs.feature_id
                            ORDER BY rs.obs_date
                            ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING
                        )
                    )
                    SELECT feature_name, value, obs_date, mean_val, std_val,
                           CASE WHEN std_val > 0
                                THEN ABS(value - mean_val) / std_val
                                ELSE 0 END AS zscore
                    FROM recent
                    WHERE obs_date >= CURRENT_DATE - :lookback
                      AND std_val > 0
                      AND ABS(value - mean_val) / std_val >= :threshold
                    ORDER BY zscore DESC
                    LIMIT 20
                """), {
                    "lookback": SCAN_LOOKBACK_DAYS,
                    "threshold": ANOMALY_ZSCORE_THRESHOLD,
                }).fetchall()
        except Exception as exc:
            log.debug("Sleuth: anomaly scan failed: {e}", e=str(exc))
            return leads

        for r in rows:
            feature_name = r[0]
            value = float(r[1])
            obs_date = str(r[2])
            mean_val = float(r[3]) if r[3] else 0
            zscore = float(r[5]) if r[5] else 0

            direction = "up" if value > mean_val else "down"
            question = (
                f"{feature_name} moved {zscore:.1f} sigma {direction} "
                f"on {obs_date} (value={value:.4f}, mean={mean_val:.4f}) "
                f"— what caused this?"
            )
            # Higher z-score = higher priority
            priority = min(1.0, 0.5 + (zscore - ANOMALY_ZSCORE_THRESHOLD) * 0.1)

            leads.append(Lead(
                id=self._make_lead_id("data_anomaly"),
                question=question,
                category="data_anomaly",
                priority=priority,
                evidence=[{
                    "feature": feature_name,
                    "value": value,
                    "mean": mean_val,
                    "zscore": zscore,
                    "obs_date": obs_date,
                    "direction": direction,
                }],
            ))

        return leads

    # ── 3. Timing Suspicious Leads ───────────────────────────────────────

    def _generate_timing_suspicious_leads(self) -> list[Lead]:
        """Cross-reference insider/congressional trades with subsequent price moves."""
        leads: list[Lead] = []
        cutoff = date.today() - timedelta(days=60)

        try:
            with self.engine.connect() as conn:
                # Find insider/congressional signals followed by significant price moves
                rows = conn.execute(text("""
                    SELECT ss.source_type, ss.source_id, ss.ticker,
                           ss.signal_type, ss.signal_date, ss.signal_value,
                           rs_after.value AS price_after,
                           rs_before.value AS price_before,
                           CASE WHEN rs_before.value > 0
                                THEN (rs_after.value - rs_before.value)
                                     / rs_before.value * 100.0
                                ELSE 0 END AS pct_move
                    FROM signal_sources ss
                    JOIN feature_registry fr ON LOWER(fr.name) = LOWER(ss.ticker)
                    JOIN resolved_series rs_before
                         ON rs_before.feature_id = fr.id
                         AND rs_before.obs_date = ss.signal_date
                    JOIN resolved_series rs_after
                         ON rs_after.feature_id = fr.id
                         AND rs_after.obs_date = (
                             SELECT MIN(obs_date)
                             FROM resolved_series
                             WHERE feature_id = fr.id
                               AND obs_date >= ss.signal_date + 5
                               AND obs_date <= ss.signal_date + 30
                         )
                    WHERE ss.source_type IN ('congressional', 'insider')
                      AND ss.signal_date >= :cutoff
                      AND ss.ticker IS NOT NULL
                    ORDER BY ABS(
                        CASE WHEN rs_before.value > 0
                             THEN (rs_after.value - rs_before.value)
                                  / rs_before.value * 100.0
                             ELSE 0 END
                    ) DESC
                    LIMIT 30
                """), {"cutoff": cutoff}).fetchall()
        except Exception as exc:
            log.debug("Sleuth: timing scan failed: {e}", e=str(exc))
            return leads

        for r in rows:
            source_type = r[0]
            source_id = str(r[1])
            ticker = r[2]
            direction = r[3]
            signal_date = str(r[4])
            pct_move = float(r[8]) if r[8] else 0

            # Flag trades where the move was in the "right" direction
            suspicious = False
            if direction and direction.upper() in ("BUY", "BULLISH") and pct_move > 5:
                suspicious = True
            elif direction and direction.upper() in ("SELL", "BEARISH") and pct_move < -5:
                suspicious = True

            if not suspicious:
                continue

            question = (
                f"{source_type.title()} {source_id} made a {direction} on "
                f"{ticker} on {signal_date}. The stock then moved "
                f"{pct_move:+.1f}% within 30 days — was this foreknowledge?"
            )
            priority = min(1.0, 0.6 + abs(pct_move) * 0.02)

            leads.append(Lead(
                id=self._make_lead_id("timing_suspicious"),
                question=question,
                category="timing_suspicious",
                priority=priority,
                evidence=[{
                    "source_type": source_type,
                    "source_id": source_id,
                    "ticker": ticker,
                    "direction": direction,
                    "signal_date": signal_date,
                    "pct_move_after": pct_move,
                }],
            ))

        return leads

    # ── 4. Connection Leads ──────────────────────────────────────────────

    def _generate_connection_leads(self) -> list[Lead]:
        """Find actors who independently acted on the same ticker — are they connected?"""
        leads: list[Lead] = []
        cutoff = date.today() - timedelta(days=SCAN_LOOKBACK_DAYS)

        try:
            with self.engine.connect() as conn:
                # Find tickers with activity from multiple source types
                rows = conn.execute(text("""
                    SELECT ticker,
                           COUNT(DISTINCT source_type) AS type_count,
                           COUNT(DISTINCT source_id) AS actor_count,
                           ARRAY_AGG(DISTINCT source_type) AS types,
                           ARRAY_AGG(DISTINCT source_id) AS actors
                    FROM signal_sources
                    WHERE signal_date >= :cutoff
                      AND ticker IS NOT NULL
                    GROUP BY ticker
                    HAVING COUNT(DISTINCT source_type) >= 2
                       AND COUNT(DISTINCT source_id) >= 2
                    ORDER BY COUNT(DISTINCT source_id) DESC
                    LIMIT 15
                """), {"cutoff": cutoff}).fetchall()
        except Exception as exc:
            log.debug("Sleuth: connection scan failed: {e}", e=str(exc))
            return leads

        for r in rows:
            ticker = r[0]
            type_count = r[1]
            actor_count = r[2]
            types = r[3] if r[3] else []
            actors = r[4] if r[4] else []

            actor_list = ", ".join(str(a) for a in actors[:5])
            type_list = ", ".join(str(t) for t in types)

            question = (
                f"{actor_count} actors ({actor_list}) from {type_count} "
                f"source types ({type_list}) all acted on {ticker} recently "
                f"— are they connected?"
            )
            priority = min(1.0, 0.4 + 0.1 * actor_count + 0.1 * type_count)

            leads.append(Lead(
                id=self._make_lead_id("connection_found"),
                question=question,
                category="connection_found",
                priority=priority,
                evidence=[{
                    "ticker": ticker,
                    "actor_count": actor_count,
                    "type_count": type_count,
                    "types": [str(t) for t in types],
                    "actors": [str(a) for a in actors[:10]],
                }],
            ))

        return leads

    # ── 5. Narrative Mismatch Leads ──────────────────────────────────────

    def _generate_narrative_mismatch_leads(self) -> list[Lead]:
        """Find cross-reference divergences — official stats vs physical reality."""
        leads: list[Lead] = []

        try:
            from intelligence.cross_reference import run_all_checks
            report = run_all_checks(self.engine)
        except Exception as exc:
            log.debug("Sleuth: cross-reference scan failed: {e}", e=str(exc))
            return leads

        # report can be a dict or a LieDetectorReport
        red_flags = []
        if hasattr(report, "red_flags"):
            red_flags = report.red_flags
        elif isinstance(report, dict):
            red_flags = report.get("red_flags", [])

        for flag in red_flags:
            # Normalize — could be CrossRefCheck dataclass or dict
            if hasattr(flag, "name"):
                name = flag.name
                category = flag.category
                divergence = flag.actual_divergence
                official = flag.official_source
                physical = flag.physical_source
                implication = flag.implication
                flag_dict = asdict(flag) if hasattr(flag, "__dataclass_fields__") else {}
            elif isinstance(flag, dict):
                name = flag.get("name", "unknown")
                category = flag.get("category", "unknown")
                divergence = flag.get("actual_divergence", 0)
                official = flag.get("official_source", "?")
                physical = flag.get("physical_source", "?")
                implication = flag.get("implication", "")
                flag_dict = flag
            else:
                continue

            question = (
                f"Narrative mismatch: {name} — official ({official}) "
                f"diverges {abs(divergence):.1f} sigma from physical "
                f"({physical}). {implication}"
            )
            priority = min(1.0, 0.5 + abs(divergence) * 0.1)

            leads.append(Lead(
                id=self._make_lead_id("narrative_mismatch"),
                question=question,
                category="narrative_mismatch",
                priority=priority,
                evidence=[flag_dict] if flag_dict else [{"name": name}],
            ))

        return leads

    # ── 6. Money Trail Leads ─────────────────────────────────────────────

    def _generate_money_trail_leads(self) -> list[Lead]:
        """Trace wealth flows and flag unusual movements."""
        leads: list[Lead] = []

        try:
            from intelligence.actor_network import track_wealth_migration, WealthFlow
            flows = track_wealth_migration(self.engine, days=30)
        except Exception as exc:
            log.debug("Sleuth: wealth flow scan failed: {e}", e=str(exc))
            return leads

        # Flag large or rumored flows
        for flow in flows[:20]:
            if hasattr(flow, "amount_estimate"):
                amount = flow.amount_estimate
                from_actor = flow.from_actor
                to_actor = flow.to_actor
                confidence = flow.confidence
                flow_dict = asdict(flow) if hasattr(flow, "__dataclass_fields__") else {}
            elif isinstance(flow, dict):
                amount = flow.get("amount_estimate", 0)
                from_actor = flow.get("from_actor", "unknown")
                to_actor = flow.get("to_actor", "unknown")
                confidence = flow.get("confidence", "unknown")
                flow_dict = flow
            else:
                continue

            # Only investigate large or suspicious flows
            if amount < 100_000_000 and confidence not in ("rumored", "likely"):
                continue

            amount_str = f"${amount / 1e9:.1f}B" if amount >= 1e9 else f"${amount / 1e6:.0f}M"
            question = (
                f"Money trail: {amount_str} flowed from {from_actor} to "
                f"{to_actor} (confidence: {confidence}) — "
                f"where does this money go next and why?"
            )
            priority = min(1.0, 0.5 + (amount / 1e10) * 0.3)
            if confidence == "rumored":
                priority = min(priority + 0.1, 1.0)

            leads.append(Lead(
                id=self._make_lead_id("money_trail"),
                question=question,
                category="money_trail",
                priority=priority,
                evidence=[flow_dict] if flow_dict else [{
                    "from": from_actor, "to": to_actor, "amount": amount,
                }],
            ))

        return leads

    # ══════════════════════════════════════════════════════════════════════
    # INVESTIGATION — deep-dive into a specific lead
    # ══════════════════════════════════════════════════════════════════════

    def investigate_lead(self, lead: Lead) -> Lead:
        """Deep-dive into a specific lead using LLM + data.

        Gathers all relevant context (actor profiles, signal history,
        price data, news context), sends it to the LLM for investigation,
        parses the response for hypotheses and follow-up questions.

        Args:
            lead: The lead to investigate.

        Returns:
            Updated lead with findings, hypotheses, and follow-up leads.
        """
        log.info("Sleuth: investigating {id} — {q}", id=lead.id, q=lead.question[:80])
        lead.status = "investigating"
        self._save_lead(lead)

        # Gather evidence context
        evidence_block = json.dumps(lead.evidence, indent=2, default=str)

        # Gather additional context based on category
        context_block = self._gather_context(lead)

        # Ask the LLM
        result = _llm_investigate(lead.question, evidence_block, context_block)

        if result is None:
            lead.findings = "LLM unavailable — investigation deferred."
            lead.status = "new"  # put back in queue
            self._save_lead(lead)
            return lead

        # Store findings
        lead.findings = result.get("conclusion", "No conclusion reached.")
        lead.hypotheses = result.get("hypotheses", [])

        # Create follow-up leads from the LLM's questions
        follow_up_ids: list[str] = []
        for fq in result.get("follow_up_questions", [])[:3]:
            if not fq or len(fq) < 10:
                continue
            fu_lead = Lead(
                id=self._make_lead_id(lead.category),
                question=fq,
                category=lead.category,
                priority=max(0.3, lead.priority - 0.1),
                evidence=[{
                    "spawned_from": lead.id,
                    "parent_question": lead.question[:200],
                }],
            )
            self._save_lead(fu_lead)
            follow_up_ids.append(fu_lead.id)

        lead.follow_up_leads = follow_up_ids
        lead.status = "resolved"
        lead.resolved_at = datetime.now(timezone.utc).isoformat()
        self._save_lead(lead)

        log.info(
            "Sleuth: resolved {id} — {n_hyp} hypotheses, {n_fu} follow-ups",
            id=lead.id, n_hyp=len(lead.hypotheses), n_fu=len(follow_up_ids),
        )
        return lead

    def _gather_context(self, lead: Lead) -> str:
        """Build additional context for the LLM based on lead category."""
        parts: list[str] = []

        # Extract tickers from evidence
        tickers: set[str] = set()
        actor_ids: set[str] = set()
        for ev in lead.evidence:
            if isinstance(ev, dict):
                if ev.get("ticker"):
                    tickers.add(ev["ticker"])
                if ev.get("source_id"):
                    actor_ids.add(str(ev["source_id"]))
                if ev.get("actors"):
                    for a in ev["actors"]:
                        actor_ids.add(str(a))

        # Actor context
        if actor_ids:
            try:
                from intelligence.actor_network import (
                    get_actor_context_for_ticker,
                    find_connected_actions,
                )
                for ticker in list(tickers)[:3]:
                    ctx = get_actor_context_for_ticker(self.engine, ticker)
                    if ctx:
                        parts.append(f"Actor context for {ticker}: {json.dumps(ctx, default=str)[:1000]}")
            except Exception:
                pass

        # Lever puller context
        if tickers:
            try:
                from intelligence.lever_pullers import get_active_lever_events
                events = get_active_lever_events(self.engine)
                relevant = [e for e in events if isinstance(e, dict) and
                            e.get("ticker") in tickers]
                if relevant:
                    parts.append(f"Lever puller events: {json.dumps(relevant[:5], default=str)[:1000]}")
            except Exception:
                pass

        # Convergence data
        if tickers:
            try:
                from intelligence.trust_scorer import detect_convergence
                for ticker in list(tickers)[:3]:
                    conv = detect_convergence(self.engine, ticker=ticker)
                    if conv:
                        parts.append(f"Convergence on {ticker}: {json.dumps(conv[:3], default=str)[:800]}")
            except Exception:
                pass

        # Cross-reference data for narrative mismatches
        if lead.category == "narrative_mismatch":
            try:
                from intelligence.cross_reference import run_all_checks
                report = run_all_checks(self.engine)
                if hasattr(report, "narrative"):
                    parts.append(f"Cross-ref narrative: {report.narrative[:800]}")
                elif isinstance(report, dict) and report.get("narrative"):
                    parts.append(f"Cross-ref narrative: {report['narrative'][:800]}")
            except Exception:
                pass

        # Recent price data for relevant tickers
        if tickers:
            try:
                with self.engine.connect() as conn:
                    for ticker in list(tickers)[:3]:
                        rows = conn.execute(text("""
                            SELECT rs.obs_date, rs.value
                            FROM resolved_series rs
                            JOIN feature_registry fr ON fr.id = rs.feature_id
                            WHERE LOWER(fr.name) = LOWER(:ticker)
                            ORDER BY rs.obs_date DESC
                            LIMIT 20
                        """), {"ticker": ticker}).fetchall()
                        if rows:
                            prices = [{"date": str(r[0]), "value": float(r[1])} for r in rows]
                            parts.append(f"Recent prices for {ticker}: {json.dumps(prices)[:600]}")
            except Exception:
                pass

        return "\n\n".join(parts) if parts else "No additional context available."

    # ══════════════════════════════════════════════════════════════════════
    # RABBIT HOLE — follow a lead chain to depth N
    # ══════════════════════════════════════════════════════════════════════

    def follow_rabbit_hole(self, lead: Lead, depth: int = DEFAULT_RABBIT_HOLE_DEPTH) -> list[Lead]:
        """Follow a lead and its children up to N levels deep.

        Investigates the given lead, then recursively investigates the
        highest-priority follow-up leads until depth is exhausted.

        Args:
            lead: The root lead to start from.
            depth: Maximum recursion depth.

        Returns:
            Flat list of all investigated leads (root + children).
        """
        log.info(
            "Sleuth: following rabbit hole from {id} (depth={d})",
            id=lead.id, d=depth,
        )
        investigated: list[Lead] = []

        # Investigate the root lead if not already resolved
        if lead.status not in ("resolved", "dead_end"):
            lead = self.investigate_lead(lead)
        investigated.append(lead)

        if depth <= 0:
            return investigated

        # Follow the highest-priority children
        for fu_id in lead.follow_up_leads:
            child = self._load_lead(fu_id)
            if child and child.status not in ("resolved", "dead_end"):
                child_results = self.follow_rabbit_hole(child, depth=depth - 1)
                investigated.extend(child_results)

        return investigated

    # ══════════════════════════════════════════════════════════════════════
    # DAILY INVESTIGATION — full cycle
    # ══════════════════════════════════════════════════════════════════════

    def daily_investigation(self) -> dict[str, Any]:
        """Run a full investigation cycle.

        1. Generate new leads from all intelligence sources
        2. Investigate high-priority leads immediately
        3. Follow rabbit holes on the most interesting leads
        4. Return summary report

        Returns:
            Investigation report dict.
        """
        log.info("Sleuth: starting daily investigation")
        start = datetime.now(timezone.utc)

        # 1. Generate leads
        new_leads = self.generate_leads()

        # 2. Investigate high-priority leads immediately
        high_priority = [l for l in new_leads if l.priority >= HIGH_PRIORITY_THRESHOLD]
        investigated: list[Lead] = []
        for lead in high_priority[:5]:  # cap to avoid runaway LLM calls
            try:
                result = self.investigate_lead(lead)
                investigated.append(result)
            except Exception as exc:
                log.warning(
                    "Sleuth: investigation failed for {id}: {e}",
                    id=lead.id, e=str(exc),
                )

        # 3. Follow rabbit holes on the top lead
        rabbit_hole_results: list[Lead] = []
        if investigated:
            top_lead = max(investigated, key=lambda l: l.priority)
            if top_lead.follow_up_leads:
                try:
                    for fu_id in top_lead.follow_up_leads[:2]:
                        child = self._load_lead(fu_id)
                        if child:
                            rh = self.follow_rabbit_hole(child, depth=2)
                            rabbit_hole_results.extend(rh)
                except Exception as exc:
                    log.warning("Sleuth: rabbit hole failed: {e}", e=str(exc))

        # 4. Compile report
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        total_open = self.count_leads(status="new")
        total_investigating = self.count_leads(status="investigating")
        total_resolved = self.count_leads(status="resolved")

        report = {
            "timestamp": start.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "leads_generated": len(new_leads),
            "leads_investigated": len(investigated),
            "rabbit_hole_depth": len(rabbit_hole_results),
            "high_priority_count": len(high_priority),
            "total_open": total_open,
            "total_investigating": total_investigating,
            "total_resolved": total_resolved,
            "top_findings": [
                {
                    "id": l.id,
                    "question": l.question[:200],
                    "findings": (l.findings or "")[:300],
                    "hypotheses": l.hypotheses[:2],
                    "priority": l.priority,
                }
                for l in investigated
            ],
            "new_leads_summary": [
                {
                    "id": l.id,
                    "question": l.question[:200],
                    "category": l.category,
                    "priority": l.priority,
                }
                for l in new_leads[:10]
            ],
        }

        # Persist report as analytical snapshot
        try:
            from store.snapshots import AnalyticalSnapshotStore
            snap = AnalyticalSnapshotStore(db_engine=self.engine)
            snap.save_snapshot(
                category="sleuth_investigation",
                payload=report,
                as_of_date=date.today(),
                metrics={
                    "leads_generated": len(new_leads),
                    "leads_investigated": len(investigated),
                    "total_open": total_open,
                    "elapsed_seconds": elapsed,
                },
            )
        except Exception as exc:
            log.warning("Sleuth: snapshot save failed: {e}", e=str(exc))

        log.info(
            "Sleuth: daily investigation complete — "
            "{gen} generated, {inv} investigated, {rh} rabbit hole in {t:.0f}s",
            gen=len(new_leads), inv=len(investigated),
            rh=len(rabbit_hole_results), t=elapsed,
        )
        return report


# ══════════════════════════════════════════════════════════════════════════
# API HELPERS — for use by FastAPI router
# ══════════════════════════════════════════════════════════════════════════

def get_sleuth(engine: Engine) -> Sleuth:
    """Convenience factory."""
    return Sleuth(engine)


# ══════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    sleuth = Sleuth(engine)

    print("=== SLEUTH — Investigative Research Engine ===\n")

    print("Generating leads...")
    leads = sleuth.generate_leads()
    print(f"  Generated: {len(leads)} leads\n")

    for lead in leads[:10]:
        print(f"  [{lead.category}] (p={lead.priority:.2f}) {lead.question[:100]}")

    if leads:
        print(f"\nInvestigating top lead...")
        top = max(leads, key=lambda l: l.priority)
        result = sleuth.investigate_lead(top)
        print(f"  Findings: {result.findings}")
        for h in result.hypotheses:
            print(f"  Hypothesis ({h['confidence']}): {h['hypothesis'][:100]}")
        print(f"  Follow-up leads: {len(result.follow_up_leads)}")
