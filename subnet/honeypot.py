"""
GRID Subnet Honeypot Calibration System.

Injects known-answer tasks into the LLM task backlog to calibrate miner
quality.  Honeypot tasks are indistinguishable from real tasks — same
format, same length, same task_types — but the validator already knows
the correct answer because it was derived from verified intelligence in
the encrypted_intelligence table.

Flow:
    1. Pull confirmed/derived entries from encrypted_intelligence (43+ records)
    2. Generate research prompts that would naturally produce the known answers
    3. Insert into llm_task_backlog + parallel entry in honeypot_registry
    4. When a miner submits a response, score it against ground truth
    5. Feed calibration scores into the Bayesian reputation system

The honeypot ratio is configurable (default 15-20% of total tasks).
"""

from __future__ import annotations

import hashlib
import json
import random
import re
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Prompt Templates ────────────────────────────────────────────────────
# Each maps an intel category to a prompt generator that looks identical
# to the real backlog tasks queued by scripts/queue_massive_backlog.py.

_PROMPT_TEMPLATES: dict[str, list[str]] = {
    "offshore_structure": [
        "INVESTIGATE: {subject}. Background, nationality, offshore entities, "
        "public companies, sanctions, net worth, red flags. "
        "Label each finding: confirmed/derived/estimated/rumored/inferred.",

        "OFFSHORE XREF {subject}: 10-K subsidiaries, tax rate, transfer pricing, "
        "executives in leaks, tax lobbying, board offshore connections.",

        "DEEP PROFILE {subject}: executives, board, insider trades, PAC, lobbying, "
        "offshore, tax, SEC, shorts. Label each: confirmed/derived/estimated/rumored/inferred.",
    ],
    "wealth_network": [
        "INVESTIGATE: {subject}. Background, nationality, offshore entities, "
        "public companies, sanctions, net worth, red flags. Label each finding.",

        "ENABLER {subject}: offshore entities facilitated, jurisdictions, client types, "
        "fines/sanctions, compliance posture, key personnel, money laundering cases.",
    ],
    "jurisdiction": [
        "TAX HAVEN: {subject}. Legal framework, ownership transparency, FATF rating, "
        "law firms, estimated assets, recent changes, famous cases.",
    ],
    "actor_profile": [
        "INVESTIGATE: {subject}. Background, nationality, offshore entities, "
        "public companies, sanctions, net worth, red flags. Label each finding.",

        "DEEP PROFILE {subject}: executives, board, insider trades, PAC, lobbying, "
        "offshore, tax, SEC, shorts. Label each: confirmed/derived/estimated/rumored/inferred.",
    ],
    "sanctions_exposure": [
        "FORENSIC {subject}: decompose recent activity, regulatory exposure, "
        "sanctions risk, compliance posture, cross-border connections, red flags.",
    ],
    "sector_flow": [
        "SECTOR {subject} — flows: state, trend, drivers, top 5 names, "
        "contrarian signals, historical analogs, trade ideas.",
    ],
    "bank_enabler": [
        "ENABLER {subject}: offshore entities facilitated, jurisdictions, client types, "
        "fines/sanctions, compliance posture, key personnel, money laundering cases.",
    ],
}

# Map intel categories to task_types that match the real backlog
_CATEGORY_TASK_TYPES: dict[str, list[str]] = {
    "offshore_structure": ["icij_officer", "offshore_xref", "sp500_profile"],
    "wealth_network": ["icij_officer", "bank_enabler"],
    "jurisdiction": ["jurisdiction"],
    "actor_profile": ["icij_officer", "sp500_profile", "forensic"],
    "sanctions_exposure": ["forensic"],
    "sector_flow": ["sector"],
    "bank_enabler": ["bank_enabler"],
}

# Fallback for categories not explicitly mapped
_DEFAULT_TEMPLATES = [
    "INVESTIGATE: {subject}. Background, connections, financial exposure, "
    "red flags, public record, estimated scale. Label each finding: "
    "confirmed/derived/estimated/rumored/inferred.",
]
_DEFAULT_TASK_TYPES = ["icij_officer", "forensic"]


# ── Honeypot Ratio ──────────────────────────────────────────────────────

DEFAULT_HONEYPOT_RATIO_MIN = 0.15
DEFAULT_HONEYPOT_RATIO_MAX = 0.20


# ── Keyword Extraction ──────────────────────────────────────────────────

def _extract_keywords(data: dict) -> list[str]:
    """Extract scorable keywords from an intelligence payload.

    Walks the dict recursively, pulling strings that look like entity
    names, amounts, jurisdictions, etc.  These become the expected_keywords
    used to score miner responses.
    """
    keywords: list[str] = []

    def _walk(obj: Any, depth: int = 0) -> None:
        if depth > 6:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                # Keys themselves are often meaningful
                if isinstance(k, str) and len(k) > 2:
                    keywords.append(k.lower())
                _walk(v, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item, depth + 1)
        elif isinstance(obj, str) and 2 < len(obj) < 200:
            # Split on common delimiters, keep meaningful tokens
            for token in re.split(r"[,;|/\n]+", obj):
                token = token.strip()
                if len(token) > 2:
                    keywords.append(token.lower())
        elif isinstance(obj, (int, float)) and obj != 0:
            keywords.append(str(obj))

    _walk(data)
    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for kw in keywords:
        if kw not in seen:
            seen.add(kw)
            unique.append(kw)
    return unique


def _build_key_facts(data: dict, category: str, subject: str) -> dict:
    """Build a structured key_facts object from intel data for scoring."""
    facts: dict[str, Any] = {
        "category": category,
        "subject": subject,
    }

    # Pull top-level keys that are typically factual assertions
    for key in ("entities", "jurisdictions", "amounts", "connections",
                "officers", "companies", "sanctions", "fines",
                "net_worth", "estimated_assets", "banks", "law_firms",
                "subsidiaries", "beneficiaries", "shareholders",
                "relationships", "flags", "red_flags"):
        if key in data:
            facts[key] = data[key]

    # If nothing specific was found, store the whole payload
    if len(facts) <= 2:
        facts["raw"] = data

    return facts


# ── Core Class ──────────────────────────────────────────────────────────

class HoneypotInjector:
    """Generate, inject, and score honeypot calibration tasks.

    Honeypot tasks are derived from verified intelligence records and
    inserted into llm_task_backlog so they are indistinguishable from
    real research tasks.  When miners return results, we score them
    against the known ground truth and feed the calibration into
    the Bayesian reputation system.
    """

    def __init__(
        self,
        engine: Engine,
        *,
        ratio_min: float = DEFAULT_HONEYPOT_RATIO_MIN,
        ratio_max: float = DEFAULT_HONEYPOT_RATIO_MAX,
    ) -> None:
        self.engine = engine
        self.ratio_min = ratio_min
        self.ratio_max = ratio_max
        self._intel_key = self._get_intel_key()

    # ── Table Setup ─────────────────────────────────────────────────

    def ensure_tables(self) -> None:
        """Create honeypot_registry table if it does not exist."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS honeypot_registry (
                        task_id       BIGINT PRIMARY KEY,
                        category      TEXT NOT NULL,
                        subject       TEXT NOT NULL,
                        key_facts     JSONB NOT NULL DEFAULT '{}',
                        expected_keywords TEXT[] NOT NULL DEFAULT '{}',
                        source_intel_id BIGINT,
                        created_at    TIMESTAMPTZ DEFAULT NOW()
                    )
                """))
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_hp_created "
                    "ON honeypot_registry (created_at)"
                ))
            log.info("Honeypot: honeypot_registry table ensured")
        except Exception as exc:
            log.warning("Honeypot: failed to ensure tables: {e}", e=str(exc))

    # ── Batch Generation ────────────────────────────────────────────

    def generate_batch(self, n: int = 10) -> list[int]:
        """Create n honeypot tasks in llm_task_backlog + honeypot_registry.

        Pulls confirmed/derived intelligence records, generates prompts
        that would naturally produce the known answers, and inserts them
        into the backlog indistinguishably from real tasks.

        Returns list of task IDs created.
        """
        intel_records = self._pull_ground_truth(limit=max(n * 3, 50))
        if not intel_records:
            log.warning("Honeypot: no ground truth records available")
            return []

        # Sample n records (with replacement if needed)
        if len(intel_records) < n:
            selected = intel_records * (n // len(intel_records) + 1)
            selected = selected[:n]
        else:
            selected = random.sample(intel_records, n)

        created_ids: list[int] = []

        for record in selected:
            try:
                task_id = self._inject_single(record)
                if task_id:
                    created_ids.append(task_id)
            except Exception as exc:
                log.debug("Honeypot: failed to inject task: {e}", e=str(exc))

        log.info(
            "Honeypot: generated {n}/{requested} tasks",
            n=len(created_ids), requested=n,
        )
        return created_ids

    # ── Honeypot Detection ──────────────────────────────────────────

    def is_honeypot(self, task_id: int) -> bool:
        """Check if a task_id is a honeypot (server-side only).

        This must NEVER be exposed to miners.
        """
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text("SELECT 1 FROM honeypot_registry WHERE task_id = :tid"),
                    {"tid": task_id},
                ).fetchone()
                return row is not None
        except Exception:
            return False

    # ── Scoring ─────────────────────────────────────────────────────

    def score_honeypot(self, task_id: int, response: str) -> float:
        """Compare a miner's response against known ground truth.

        Returns a calibration score between 0.0 (no match) and 1.0
        (perfect coverage of expected keywords / key facts).
        """
        registry = self._get_registry_entry(task_id)
        if not registry:
            log.debug("Honeypot: task {tid} not in registry", tid=task_id)
            return 0.0

        expected_keywords = registry["expected_keywords"] or []
        key_facts = registry["key_facts"] or {}

        if not expected_keywords and not key_facts:
            log.debug("Honeypot: no ground truth for task {tid}", tid=task_id)
            return 0.5  # No ground truth to check — neutral score

        response_lower = response.lower()
        score = 0.0
        total_weight = 0.0

        # --- Keyword coverage (60% weight) ---
        if expected_keywords:
            hits = sum(1 for kw in expected_keywords if kw in response_lower)
            keyword_score = hits / len(expected_keywords) if expected_keywords else 0.0
            score += keyword_score * 0.6
            total_weight += 0.6

        # --- Key fact coverage (30% weight) ---
        if key_facts:
            fact_keywords = _extract_keywords(key_facts)
            if fact_keywords:
                fact_hits = sum(1 for kw in fact_keywords if kw in response_lower)
                fact_score = fact_hits / len(fact_keywords)
                score += fact_score * 0.3
                total_weight += 0.3

        # --- Response quality signals (10% weight) ---
        quality_score = 0.0
        # Has confidence labels?
        for label in ("confirmed", "derived", "estimated", "rumored", "inferred"):
            if label in response_lower:
                quality_score += 0.2
        quality_score = min(quality_score, 1.0)
        score += quality_score * 0.1
        total_weight += 0.1

        # Normalize
        final = score / total_weight if total_weight > 0 else 0.0
        return round(min(max(final, 0.0), 1.0), 4)

    # ── Calibration Divergence ──────────────────────────────────────

    def get_calibration_divergence(self, miner_id: str) -> dict[str, Any]:
        """Measure how far a miner's honeypot scores deviate from regular scores.

        Returns a dict with:
            - honeypot_avg: average honeypot score
            - regular_avg: average regular task score
            - divergence: abs(honeypot_avg - regular_avg)
            - honeypot_count: number of honeypot tasks scored
            - suspicious: True if divergence > 0.3 (gaming detection)
        """
        try:
            with self.engine.connect() as conn:
                # Honeypot scores: tasks in honeypot_registry assigned to this miner
                hp_rows = conn.execute(text(
                    "SELECT AVG(t.score), COUNT(*) "
                    "FROM compute_task_results t "
                    "JOIN honeypot_registry h ON t.task_id = h.task_id "
                    "WHERE t.miner_id = :mid AND t.score IS NOT NULL"
                ), {"mid": miner_id}).fetchone()

                # Regular scores: tasks NOT in honeypot_registry
                reg_rows = conn.execute(text(
                    "SELECT AVG(t.score), COUNT(*) "
                    "FROM compute_task_results t "
                    "LEFT JOIN honeypot_registry h ON t.task_id = h.task_id "
                    "WHERE t.miner_id = :mid "
                    "AND t.score IS NOT NULL "
                    "AND h.task_id IS NULL"
                ), {"mid": miner_id}).fetchone()

            hp_avg = float(hp_rows[0]) if hp_rows and hp_rows[0] is not None else 0.5
            hp_count = int(hp_rows[1]) if hp_rows else 0
            reg_avg = float(reg_rows[0]) if reg_rows and reg_rows[0] is not None else 0.5
            reg_count = int(reg_rows[1]) if reg_rows else 0

            divergence = abs(hp_avg - reg_avg)

            return {
                "miner_id": miner_id,
                "honeypot_avg": round(hp_avg, 4),
                "honeypot_count": hp_count,
                "regular_avg": round(reg_avg, 4),
                "regular_count": reg_count,
                "divergence": round(divergence, 4),
                "suspicious": divergence > 0.3 and hp_count >= 3,
            }
        except Exception as exc:
            log.debug(
                "Honeypot: calibration divergence failed for {m}: {e}",
                m=miner_id, e=str(exc),
            )
            return {
                "miner_id": miner_id,
                "honeypot_avg": 0.5,
                "honeypot_count": 0,
                "regular_avg": 0.5,
                "regular_count": 0,
                "divergence": 0.0,
                "suspicious": False,
            }

    # ── Current Honeypot Ratio ──────────────────────────────────────

    def get_current_ratio(self) -> float:
        """What fraction of pending backlog tasks are honeypots?"""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT "
                    "  COUNT(*) FILTER (WHERE h.task_id IS NOT NULL) AS hp_count, "
                    "  COUNT(*) AS total_count "
                    "FROM llm_task_backlog b "
                    "LEFT JOIN honeypot_registry h ON b.id = h.task_id "
                    "WHERE b.status = 'pending'"
                )).fetchone()

            if row and row[1] > 0:
                return round(row[0] / row[1], 4)
            return 0.0
        except Exception:
            return 0.0

    def needs_injection(self) -> int:
        """How many honeypots need to be injected to maintain target ratio?

        Returns 0 if we are within range, otherwise the count needed.
        """
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT "
                    "  COUNT(*) FILTER (WHERE h.task_id IS NOT NULL) AS hp_count, "
                    "  COUNT(*) AS total_count "
                    "FROM llm_task_backlog b "
                    "LEFT JOIN honeypot_registry h ON b.id = h.task_id "
                    "WHERE b.status = 'pending'"
                )).fetchone()

            if not row or row[1] == 0:
                return 0

            hp_count = row[0]
            total = row[1]
            current_ratio = hp_count / total

            if current_ratio >= self.ratio_min:
                return 0

            # Calculate how many to add to reach target midpoint
            target_ratio = (self.ratio_min + self.ratio_max) / 2.0
            # Solve: (hp_count + x) / (total + x) = target_ratio
            # x = (target_ratio * total - hp_count) / (1 - target_ratio)
            needed = (target_ratio * total - hp_count) / (1.0 - target_ratio)
            return max(0, int(needed) + 1)
        except Exception:
            return 0

    # ── Stats ───────────────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        """Summary statistics for the honeypot system."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT COUNT(*), "
                    "  COUNT(DISTINCT category), "
                    "  MIN(created_at), "
                    "  MAX(created_at) "
                    "FROM honeypot_registry"
                )).fetchone()

            return {
                "total_honeypots": row[0] if row else 0,
                "categories": row[1] if row else 0,
                "oldest": row[2].isoformat() if row and row[2] else None,
                "newest": row[3].isoformat() if row and row[3] else None,
                "current_ratio": self.get_current_ratio(),
                "target_ratio": f"{self.ratio_min:.0%}-{self.ratio_max:.0%}",
                "injection_needed": self.needs_injection(),
            }
        except Exception:
            return {"total_honeypots": 0, "current_ratio": 0.0}

    # ── Private Helpers ─────────────────────────────────────────────

    @staticmethod
    def _get_intel_key() -> str:
        """Mirror opsec.py key derivation."""
        import os
        key = os.getenv("GRID_INTEL_KEY")
        if key:
            return key
        jwt_secret = os.getenv("GRID_JWT_SECRET")
        if jwt_secret:
            return hashlib.sha256(f"grid-intel-{jwt_secret}".encode()).hexdigest()
        return "dev-intel-key-DO-NOT-USE-IN-PRODUCTION"

    def _pull_ground_truth(self, limit: int = 50) -> list[dict]:
        """Pull confirmed/derived intel records for honeypot generation.

        Only uses high-confidence records where we know the real answer.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, category, subject, confidence, "
                        "pgp_sym_decrypt(encrypted_data, :key)::text AS data, "
                        "tags "
                        "FROM encrypted_intelligence "
                        "WHERE confidence IN ('confirmed', 'derived') "
                        "ORDER BY RANDOM() LIMIT :lim"
                    ),
                    {"key": self._intel_key, "lim": limit},
                ).fetchall()

                results = []
                for r in rows:
                    try:
                        payload = json.loads(r[4]) if r[4] else {}
                    except (json.JSONDecodeError, TypeError):
                        payload = {"raw": r[4]}

                    results.append({
                        "id": r[0],
                        "category": r[1],
                        "subject": r[2],
                        "confidence": r[3],
                        "data": payload,
                        "tags": r[5] or [],
                    })

                log.debug(
                    "Honeypot: pulled {n} ground truth records",
                    n=len(results),
                )
                return results
        except Exception as exc:
            log.warning("Honeypot: failed to pull ground truth: {e}", e=str(exc))
            return []

    def _inject_single(self, record: dict) -> int | None:
        """Generate and inject a single honeypot task.

        Returns the task_id on success, None on failure.
        """
        category = record["category"]
        subject = record["subject"]
        data = record["data"]
        intel_id = record["id"]

        # Pick a prompt template that matches the category
        templates = _PROMPT_TEMPLATES.get(category, _DEFAULT_TEMPLATES)
        prompt = random.choice(templates).format(subject=subject)

        # Pick a task_type that matches real backlog tasks
        task_types = _CATEGORY_TASK_TYPES.get(category, _DEFAULT_TASK_TYPES)
        task_type = random.choice(task_types)

        # Build context that looks like real tasks
        context = self._build_context(category, subject, record.get("tags", []))

        # Extract ground truth for scoring
        key_facts = _build_key_facts(data, category, subject)
        expected_keywords = _extract_keywords(data)

        try:
            with self.engine.begin() as conn:
                # Insert into llm_task_backlog (identical to real tasks)
                row = conn.execute(
                    text(
                        "INSERT INTO llm_task_backlog "
                        "(task_type, prompt, context, priority, status) "
                        "VALUES (:t, :p, CAST(:c AS jsonb), :pri, 'pending') "
                        "RETURNING id"
                    ),
                    {
                        "t": task_type,
                        "p": prompt,
                        "c": json.dumps(context),
                        "pri": random.choice([2, 3, 3, 3]),  # mostly background, sometimes scheduled
                    },
                ).fetchone()

                if not row:
                    return None

                task_id = row[0]

                # Register in honeypot_registry (server-side only)
                conn.execute(
                    text(
                        "INSERT INTO honeypot_registry "
                        "(task_id, category, subject, key_facts, "
                        "expected_keywords, source_intel_id) "
                        "VALUES (:tid, :cat, :sub, CAST(:kf AS jsonb), "
                        ":kw, :sid)"
                    ),
                    {
                        "tid": task_id,
                        "cat": category,
                        "sub": subject,
                        "kf": json.dumps(key_facts),
                        "kw": expected_keywords,
                        "sid": intel_id,
                    },
                )

                return task_id
        except Exception as exc:
            log.debug("Honeypot: inject failed: {e}", e=str(exc))
            return None

    @staticmethod
    def _build_context(category: str, subject: str, tags: list[str]) -> dict:
        """Build a context dict that mirrors real backlog task contexts.

        Must be indistinguishable from contexts generated by the queue
        scripts (queue_massive_backlog.py, queue_crypto_backlog.py, etc.).
        """
        # Match the patterns used in real queue scripts
        ctx: dict[str, Any] = {}

        if category in ("offshore_structure", "actor_profile", "wealth_network"):
            ctx["person"] = subject
        elif category == "jurisdiction":
            ctx["haven"] = subject
        elif category == "sector_flow":
            ctx["sector"] = subject
            ctx["angle"] = random.choice(
                ["flows", "positioning", "earnings", "valuation", "technicals"]
            )
        elif category == "bank_enabler":
            ctx["bank"] = subject
        else:
            # Generic — use ticker-like context if it looks like a ticker
            if len(subject) <= 5 and subject.isalpha() and subject.isupper():
                ctx["ticker"] = subject
            else:
                ctx["person"] = subject

        if tags:
            # Don't include tags — real tasks don't have them
            pass

        return ctx

    def _get_registry_entry(self, task_id: int) -> dict | None:
        """Fetch a honeypot registry entry by task_id."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT task_id, category, subject, key_facts, "
                        "expected_keywords, source_intel_id, created_at "
                        "FROM honeypot_registry WHERE task_id = :tid"
                    ),
                    {"tid": task_id},
                ).fetchone()

                if not row:
                    return None

                return {
                    "task_id": row[0],
                    "category": row[1],
                    "subject": row[2],
                    "key_facts": row[3] if isinstance(row[3], dict) else json.loads(row[3] or "{}"),
                    "expected_keywords": row[4] or [],
                    "source_intel_id": row[5],
                    "created_at": row[6],
                }
        except Exception as exc:
            log.debug("Honeypot: registry lookup failed: {e}", e=str(exc))
            return None
