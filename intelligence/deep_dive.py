"""GRID Intelligence — Thesis Deep Dive Engine.

Automatically triggered when a new thesis is generated. Runs a thorough
background analysis using the best available LLM (Claude Opus preferred,
falls back to Sonnet → GPT-4o → Gemini).

Deep dives are NEVER deleted — they accumulate in the thesis_deep_dives table
as a permanent research archive. Each dive is linked to the thesis snapshot
that triggered it, but stands alone as an independent research artifact.

Pipeline:
  1. Thesis snapshot triggers deep_dive_async() in background thread
  2. Collects context: thesis data, recent flow state, CDS, actor signals,
     convergence events, sleuth leads, forensic patterns
  3. Sends to the best available LLM with a structured research prompt
  4. Saves the full analysis to thesis_deep_dives table
  5. Optionally generates follow-up research questions

Public API:
    deep_dive_async(engine, thesis_data, snapshot_id) -> threading.Thread
    run_deep_dive(engine, thesis_data, snapshot_id) -> DeepDiveResult
    get_deep_dives(engine, days=90) -> list[dict]
    get_deep_dive(engine, dive_id) -> dict | None
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# -- Configuration -----------------------------------------------------------

# Model preference order: Claude Opus (Max plan) → Sonnet → GPT-4o → Gemini
OPUS_MODEL = "claude-opus-4-6"
SONNET_MODEL = "claude-sonnet-4-6"
GPT4O_MODEL = "gpt-4o"
GEMINI_MODEL = "gemini-2.5-flash"

MAX_ANALYSIS_TOKENS = 8192


@dataclass(frozen=True)
class DeepDiveResult:
    """Immutable result from a deep dive analysis."""

    snapshot_id: int
    analysis: str
    model_used: str
    provider_used: str
    follow_up_questions: list[str] = field(default_factory=list)
    key_insights: list[str] = field(default_factory=list)
    contrarian_signals: list[str] = field(default_factory=list)
    risk_blind_spots: list[str] = field(default_factory=list)
    duration_ms: int = 0
    generated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "analysis": self.analysis,
            "model_used": self.model_used,
            "provider_used": self.provider_used,
            "follow_up_questions": self.follow_up_questions,
            "key_insights": self.key_insights,
            "contrarian_signals": self.contrarian_signals,
            "risk_blind_spots": self.risk_blind_spots,
            "duration_ms": self.duration_ms,
            "generated_at": self.generated_at,
        }


# -- Database ----------------------------------------------------------------

def _ensure_table(engine: Engine) -> None:
    """Create thesis_deep_dives table if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS thesis_deep_dives (
                id              SERIAL PRIMARY KEY,
                snapshot_id     INTEGER REFERENCES thesis_snapshots(id),
                analysis        TEXT NOT NULL,
                model_used      VARCHAR(100) NOT NULL,
                provider_used   VARCHAR(50) NOT NULL,
                key_insights    JSONB DEFAULT '[]',
                contrarian_signals JSONB DEFAULT '[]',
                risk_blind_spots JSONB DEFAULT '[]',
                follow_up_questions JSONB DEFAULT '[]',
                thesis_direction VARCHAR(20),
                thesis_conviction FLOAT,
                context_summary JSONB DEFAULT '{}',
                duration_ms     INTEGER DEFAULT 0,
                generated_at    TIMESTAMPTZ DEFAULT NOW(),
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_deep_dives_snapshot
                ON thesis_deep_dives (snapshot_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_deep_dives_generated
                ON thesis_deep_dives (generated_at DESC)
        """))


def _save_deep_dive(engine: Engine, result: DeepDiveResult,
                     thesis_data: dict) -> int:
    """Persist deep dive to database. Returns the row ID."""
    _ensure_table(engine)

    with engine.begin() as conn:
        row = conn.execute(text("""
            INSERT INTO thesis_deep_dives
                (snapshot_id, analysis, model_used, provider_used,
                 key_insights, contrarian_signals, risk_blind_spots,
                 follow_up_questions, thesis_direction, thesis_conviction,
                 context_summary, duration_ms, generated_at)
            VALUES
                (:snapshot_id, :analysis, :model_used, :provider_used,
                 :key_insights, :contrarian_signals, :risk_blind_spots,
                 :follow_up_questions, :thesis_direction, :thesis_conviction,
                 :context_summary, :duration_ms, :generated_at)
            RETURNING id
        """), {
            "snapshot_id": result.snapshot_id,
            "analysis": result.analysis,
            "model_used": result.model_used,
            "provider_used": result.provider_used,
            "key_insights": json.dumps(result.key_insights),
            "contrarian_signals": json.dumps(result.contrarian_signals),
            "risk_blind_spots": json.dumps(result.risk_blind_spots),
            "follow_up_questions": json.dumps(result.follow_up_questions),
            "thesis_direction": thesis_data.get("overall_direction", ""),
            "thesis_conviction": thesis_data.get("conviction"),
            "context_summary": json.dumps({
                "key_drivers": thesis_data.get("key_drivers", [])[:5],
                "risk_factors": thesis_data.get("risk_factors", [])[:5],
            }),
            "duration_ms": result.duration_ms,
            "generated_at": result.generated_at,
        }).fetchone()

    dive_id = row[0]
    log.info(
        "Deep dive saved: id={id}, model={m}, {ms}ms",
        id=dive_id, m=result.model_used, ms=result.duration_ms,
    )
    return dive_id


# -- Context Collection -----------------------------------------------------

def _collect_deep_dive_context(engine: Engine) -> dict[str, Any]:
    """Gather rich context for the deep dive analysis."""
    context: dict[str, Any] = {}

    # Flow engine state
    try:
        from analysis.money_flow_engine import build_flow_map
        flow_map = build_flow_map(engine)
        context["flow"] = {
            "global_liquidity": flow_map.global_liquidity_total,
            "liquidity_change_1m": flow_map.global_liquidity_change_1m,
            "narrative": flow_map.narrative or "",
            "layers": [
                {
                    "name": l.label, "regime": l.regime,
                    "stress": l.stress_score, "total_usd": l.total_value_usd,
                    "net_flow_1m": l.net_flow_1m, "confidence": l.confidence,
                }
                for l in flow_map.layers
            ],
        }
    except Exception as exc:
        context["flow"] = {"error": str(exc)}

    # CDS / credit
    try:
        from intelligence.cds_tracker import build_cds_dashboard, cds_to_dict
        context["credit"] = cds_to_dict(build_cds_dashboard(engine))
    except Exception as exc:
        context["credit"] = {"error": str(exc)}

    # Recent convergence events
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, signal_type, source_count, combined_confidence,
                       sources, detected_at
                FROM convergence_alerts
                WHERE detected_at >= NOW() - INTERVAL '48 hours'
                ORDER BY combined_confidence DESC
                LIMIT 10
            """)).fetchall()
            context["convergence"] = [
                {
                    "ticker": r[0], "signal": r[1], "sources": r[2],
                    "confidence": float(r[3]) if r[3] else 0,
                    "detected_at": r[5].isoformat() if r[5] else "",
                }
                for r in rows
            ]
    except Exception:
        context["convergence"] = []

    # Recent investigative leads
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT question, category, priority, status, findings
                FROM investigation_leads
                WHERE created_at >= NOW() - INTERVAL '7 days'
                  AND priority >= 0.5
                ORDER BY priority DESC
                LIMIT 8
            """)).fetchall()
            context["leads"] = [
                {
                    "question": r[0], "category": r[1],
                    "priority": float(r[2]) if r[2] else 0,
                    "status": r[3],
                    "findings": (r[4] or "")[:300],
                }
                for r in rows
            ]
    except Exception:
        context["leads"] = []

    # Recent forensic patterns
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT pattern_type, ticker, description, confidence
                FROM recurring_patterns
                WHERE detected_at >= NOW() - INTERVAL '14 days'
                ORDER BY confidence DESC
                LIMIT 10
            """)).fetchall()
            context["patterns"] = [
                {
                    "type": r[0], "ticker": r[1],
                    "description": (r[2] or "")[:200],
                    "confidence": float(r[3]) if r[3] else 0,
                }
                for r in rows
            ]
    except Exception:
        context["patterns"] = []

    # Recent thesis accuracy
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT outcome, COUNT(*)
                FROM thesis_snapshots
                WHERE outcome IS NOT NULL
                  AND scored_at >= NOW() - INTERVAL '30 days'
                GROUP BY outcome
            """)).fetchall()
            context["recent_accuracy"] = {r[0]: r[1] for r in rows}
    except Exception:
        context["recent_accuracy"] = {}

    # Previous deep dives (last 3) for continuity
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT key_insights, contrarian_signals, thesis_direction,
                       generated_at
                FROM thesis_deep_dives
                ORDER BY generated_at DESC
                LIMIT 3
            """)).fetchall()
            context["prior_dives"] = [
                {
                    "insights": json.loads(r[0]) if r[0] else [],
                    "contrarian": json.loads(r[1]) if r[1] else [],
                    "direction": r[2],
                    "when": r[3].isoformat() if r[3] else "",
                }
                for r in rows
            ]
    except Exception:
        context["prior_dives"] = []

    return context


# -- Prompt Construction -----------------------------------------------------

def _build_deep_dive_prompt(thesis_data: dict, context: dict) -> str:
    """Build a thorough research prompt for the deep dive."""

    direction = thesis_data.get("overall_direction", "NEUTRAL")
    conviction = thesis_data.get("conviction", "unknown")
    narrative = thesis_data.get("narrative", "No narrative available.")
    key_drivers = thesis_data.get("key_drivers", [])
    risk_factors = thesis_data.get("risk_factors", [])
    agreements = thesis_data.get("agreements", [])
    contradictions = thesis_data.get("contradictions", [])

    # Format models
    models_block = ""
    for m in thesis_data.get("models", thesis_data.get("top_theses", []))[:12]:
        if isinstance(m, dict):
            name = m.get("name", m.get("key", "?"))
            score = m.get("score", m.get("direction", "?"))
            conf = m.get("confidence", "?")
            reasoning = m.get("reasoning", m.get("detail", ""))[:150]
            models_block += f"  - {name}: {score} (conf={conf}) — {reasoning}\n"

    # Format context sections
    flow = context.get("flow", {})
    flow_block = flow.get("narrative", "N/A")
    if flow.get("layers"):
        for l in flow["layers"]:
            usd = f"${l.get('total_usd', 0) / 1e12:.1f}T" if l.get("total_usd") else ""
            flow_block += f"\n  - {l['name']}: {l.get('regime', '?')} stress={l.get('stress', '?')} {usd}"

    credit = context.get("credit", {})
    credit_block = f"Regime: {credit.get('regime', 'unknown')}"

    convergence = context.get("convergence", [])
    conv_block = "\n".join(
        f"  - {c['ticker']} {c['signal']} ({c['sources']} sources, {c['confidence']:.0%})"
        for c in convergence
    ) or "  None active"

    leads = context.get("leads", [])
    leads_block = "\n".join(
        f"  - [{l['category']}] {l['question']} (priority={l['priority']:.1f}, {l['status']})"
        for l in leads
    ) or "  None"

    patterns = context.get("patterns", [])
    patterns_block = "\n".join(
        f"  - {p['ticker']} {p['type']}: {p['description']}"
        for p in patterns
    ) or "  None detected"

    accuracy = context.get("recent_accuracy", {})
    acc_block = ", ".join(f"{k}: {v}" for k, v in accuracy.items()) or "No scored theses yet"

    prior_dives = context.get("prior_dives", [])
    prior_block = ""
    for pd in prior_dives[:2]:
        insights = "; ".join(pd.get("insights", [])[:3])
        prior_block += f"\n  [{pd.get('when', '?')}] Dir={pd.get('direction', '?')}: {insights}"
    if not prior_block:
        prior_block = "\n  No prior deep dives"

    # Format contradictions
    contra_block = ""
    for c in contradictions[:5]:
        if isinstance(c, dict):
            contra_block += f"\n  - {c.get('bullish', '?')} vs {c.get('bearish', '?')}"
        else:
            contra_block += f"\n  - {c}"
    if not contra_block:
        contra_block = "\n  None"

    return f"""You are GRID's Deep Research Analyst. You have been triggered because a new market thesis was just generated. Your job is to perform a thorough, independent deep dive that challenges the thesis, identifies blind spots, and surfaces non-obvious connections.

This analysis is permanently archived and may influence future model weighting.

═══ CURRENT THESIS ═══
Direction: {direction}
Conviction: {conviction}
Narrative: {narrative}

Key Drivers:
{chr(10).join(f'  - {d}' if isinstance(d, str) else f'  - {d.get("name", d.get("key", "?"))}: {d.get("detail", d.get("direction", "?"))}' for d in key_drivers[:8])}

Risk Factors:
{chr(10).join(f'  - {r}' if isinstance(r, str) else f'  - {r.get("name", r.get("key", "?"))}: {r.get("detail", r.get("direction", "?"))}' for r in risk_factors[:8])}

Model Signals:
{models_block}

Internal Contradictions:
{contra_block}

═══ LIVE CONTEXT ═══

Capital Flows:
{flow_block}

Credit Regime:
{credit_block}

Convergence Signals (48h):
{conv_block}

Active Investigations:
{leads_block}

Forensic Patterns (14d):
{patterns_block}

Recent Thesis Accuracy (30d):
{acc_block}

Prior Deep Dives:
{prior_block}

═══ YOUR TASK ═══

Produce a structured deep dive with these exact sections:

1. **THESIS STRESS TEST**: Steelman the thesis, then attack it. What assumptions could break? What historical analogs suggest a different outcome? Be specific with examples.

2. **BLIND SPOT ANALYSIS**: What is the thesis NOT seeing? Check for: regime transition signals, cross-asset divergences, seasonal effects, upcoming catalysts (earnings, FOMC, OpEx), geopolitical risks, positioning extremes.

3. **CONTRARIAN SIGNAL CHECK**: What data would a contrarian point to? Are there divergences between what actors say and what they do? Is the crowd too one-sided?

4. **CROSS-REFERENCE TRIANGULATION**: Do independent data sources (credit, flows, positioning, insider activity, congressional trades) agree or disagree? Where are the cracks?

5. **SECOND-ORDER EFFECTS**: If the thesis is right, what are the non-obvious downstream consequences? What trades beyond the obvious would benefit? What breaks?

6. **KEY INSIGHTS** (bullet list, 3-6 items): The most important non-obvious findings from this analysis. Be specific and actionable.

7. **RISK BLIND SPOTS** (bullet list, 2-4 items): Specific risks the thesis is underweighting or ignoring entirely.

8. **CONTRARIAN SIGNALS** (bullet list, 2-4 items): Data points that argue against the thesis direction.

9. **FOLLOW-UP QUESTIONS** (bullet list, 3-5 items): What should GRID investigate next? Be specific — name tickers, data sources, actors, or events.

RULES:
- Be specific. Use numbers, tickers, dates, actor names.
- Don't just agree with the thesis — your value is in finding what it misses.
- Reference the live context data provided above.
- Keep total length to 800-1200 words.
- If the thesis contradicts itself internally, call it out.
- If prior deep dives flagged something that's still relevant, note whether it played out.
"""


# -- LLM Call with Fallback Chain -------------------------------------------

def _call_best_llm(prompt: str) -> tuple[str, str, str]:
    """Call the best available LLM. Returns (response, model, provider).

    Preference: Claude Opus → Claude Sonnet → GPT-4o → Gemini.
    """
    from config import settings

    # 1. Try Claude Opus (Max plan)
    anthropic_key = (
        settings.ANTHROPIC_API_KEY or settings.AGENTS_ANTHROPIC_API_KEY
    )
    if anthropic_key:
        for model in [OPUS_MODEL, SONNET_MODEL]:
            try:
                import requests as req
                resp = req.post(
                    f"{settings.ANTHROPIC_BASE_URL}/v1/messages",
                    headers={
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": model,
                        "max_tokens": MAX_ANALYSIS_TOKENS,
                        "temperature": 0.4,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=settings.ANTHROPIC_TIMEOUT_SECONDS,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    text_out = data["content"][0]["text"]
                    log.info("Deep dive via Anthropic ({m})", m=model)
                    return text_out, model, "anthropic"
                else:
                    log.warning(
                        "Anthropic {m} returned {s}: {b}",
                        m=model, s=resp.status_code,
                        b=resp.text[:200],
                    )
            except Exception as exc:
                log.warning("Anthropic {m} failed: {e}", m=model, e=str(exc))

    # 2. Try OpenAI GPT-4o
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if openai_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=openai_key)
            response = client.chat.completions.create(
                model=GPT4O_MODEL,
                messages=[
                    {"role": "system", "content": "You are a senior quantitative research analyst performing deep dive analysis for an institutional trading intelligence platform."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=MAX_ANALYSIS_TOKENS,
                temperature=0.4,
            )
            text_out = response.choices[0].message.content.strip()
            log.info("Deep dive via OpenAI (gpt-4o)")
            return text_out, GPT4O_MODEL, "openai"
        except Exception as exc:
            log.warning("OpenAI deep dive failed: {e}", e=str(exc))

    # 3. Try Gemini
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if gemini_key:
        try:
            from google import genai
            client = genai.Client(api_key=gemini_key)
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
            )
            if response.text:
                log.info("Deep dive via Gemini")
                return response.text.strip(), GEMINI_MODEL, "gemini"
        except Exception as exc:
            log.warning("Gemini deep dive failed: {e}", e=str(exc))

    raise RuntimeError("All LLM providers failed for deep dive")


# -- Parse Structured Output ------------------------------------------------

def _parse_deep_dive(analysis: str) -> dict[str, list[str]]:
    """Extract structured lists from the deep dive analysis text."""
    sections = {
        "key_insights": [],
        "contrarian_signals": [],
        "risk_blind_spots": [],
        "follow_up_questions": [],
    }

    current_section = None
    for line in analysis.split("\n"):
        line_lower = line.lower().strip()

        if "key insights" in line_lower:
            current_section = "key_insights"
            continue
        elif "contrarian signal" in line_lower:
            current_section = "contrarian_signals"
            continue
        elif "risk blind spot" in line_lower or "blind spots" in line_lower:
            current_section = "risk_blind_spots"
            continue
        elif "follow-up" in line_lower or "follow up" in line_lower:
            current_section = "follow_up_questions"
            continue
        elif line.strip().startswith("**") and line.strip().endswith("**"):
            # New section header — stop collecting for current section
            if current_section and any(
                h in line_lower for h in [
                    "stress test", "blind spot analysis",
                    "cross-reference", "second-order",
                ]
            ):
                current_section = None
            continue

        # Collect bullet items
        stripped = line.strip()
        if current_section and stripped.startswith(("-", "*", "•")):
            item = stripped.lstrip("-*• ").strip()
            if item and len(item) > 5:
                sections[current_section].append(item)

    return sections


# -- Public API --------------------------------------------------------------

def run_deep_dive(
    engine: Engine,
    thesis_data: dict,
    snapshot_id: int,
) -> DeepDiveResult:
    """Run a synchronous deep dive analysis.

    Collects context, calls the best LLM, parses structured output,
    and persists everything to the database. Deep dives are NEVER deleted.
    """
    t0 = time.monotonic()

    context = _collect_deep_dive_context(engine)
    prompt = _build_deep_dive_prompt(thesis_data, context)
    analysis, model_used, provider_used = _call_best_llm(prompt)
    parsed = _parse_deep_dive(analysis)

    duration_ms = int((time.monotonic() - t0) * 1000)

    result = DeepDiveResult(
        snapshot_id=snapshot_id,
        analysis=analysis,
        model_used=model_used,
        provider_used=provider_used,
        key_insights=parsed["key_insights"],
        contrarian_signals=parsed["contrarian_signals"],
        risk_blind_spots=parsed["risk_blind_spots"],
        follow_up_questions=parsed["follow_up_questions"],
        duration_ms=duration_ms,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )

    _save_deep_dive(engine, result, thesis_data)
    return result


def deep_dive_async(
    engine: Engine,
    thesis_data: dict,
    snapshot_id: int,
) -> threading.Thread:
    """Launch a deep dive in a background thread.

    Returns the thread handle (daemon thread — won't block shutdown).
    """
    def _worker():
        try:
            result = run_deep_dive(engine, thesis_data, snapshot_id)
            log.info(
                "Background deep dive complete: model={m}, insights={n}, {ms}ms",
                m=result.model_used,
                n=len(result.key_insights),
                ms=result.duration_ms,
            )
        except Exception as exc:
            log.error("Background deep dive failed: {e}", e=str(exc))

    thread = threading.Thread(
        target=_worker,
        name=f"deep-dive-snap-{snapshot_id}",
        daemon=True,
    )
    thread.start()
    log.info(
        "Deep dive launched in background for snapshot {id}",
        id=snapshot_id,
    )
    return thread


# -- Query API ---------------------------------------------------------------

def get_deep_dives(
    engine: Engine,
    days: int = 90,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Retrieve all deep dives within the lookback window.

    Deep dives are NEVER deleted — this returns the full archive.
    """
    _ensure_table(engine)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, snapshot_id, analysis, model_used, provider_used,
                   key_insights, contrarian_signals, risk_blind_spots,
                   follow_up_questions, thesis_direction, thesis_conviction,
                   context_summary, duration_ms, generated_at
            FROM thesis_deep_dives
            WHERE generated_at >= NOW() - make_interval(days => :days)
            ORDER BY generated_at DESC
            LIMIT :limit
        """), {"days": days, "limit": limit}).fetchall()

    def _parse_jsonb(val, default):
        """Handle JSONB columns — psycopg2 may return str, list, or dict."""
        if val is None:
            return default
        if isinstance(val, (list, dict)):
            return val
        if isinstance(val, str):
            try:
                return json.loads(val)
            except (json.JSONDecodeError, TypeError):
                return default
        return default

    return [
        {
            "id": r[0],
            "snapshot_id": r[1],
            "analysis": r[2],
            "model_used": r[3],
            "provider_used": r[4],
            "key_insights": _parse_jsonb(r[5], []),
            "contrarian_signals": _parse_jsonb(r[6], []),
            "risk_blind_spots": _parse_jsonb(r[7], []),
            "follow_up_questions": _parse_jsonb(r[8], []),
            "thesis_direction": r[9],
            "thesis_conviction": float(r[10]) if r[10] is not None else None,
            "context_summary": _parse_jsonb(r[11], {}),
            "duration_ms": r[12],
            "generated_at": r[13].isoformat() if r[13] else "",
        }
        for r in rows
    ]


def get_deep_dive(engine: Engine, dive_id: int) -> dict[str, Any] | None:
    """Retrieve a single deep dive by ID."""
    _ensure_table(engine)

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, snapshot_id, analysis, model_used, provider_used,
                   key_insights, contrarian_signals, risk_blind_spots,
                   follow_up_questions, thesis_direction, thesis_conviction,
                   context_summary, duration_ms, generated_at
            FROM thesis_deep_dives
            WHERE id = :id
        """), {"id": dive_id}).fetchone()

    if not row:
        return None

    def _parse_jsonb(val, default):
        if val is None:
            return default
        if isinstance(val, (list, dict)):
            return val
        if isinstance(val, str):
            try:
                return json.loads(val)
            except (json.JSONDecodeError, TypeError):
                return default
        return default

    return {
        "id": row[0],
        "snapshot_id": row[1],
        "analysis": row[2],
        "model_used": row[3],
        "provider_used": row[4],
        "key_insights": _parse_jsonb(row[5], []),
        "contrarian_signals": _parse_jsonb(row[6], []),
        "risk_blind_spots": _parse_jsonb(row[7], []),
        "follow_up_questions": _parse_jsonb(row[8], []),
        "thesis_direction": row[9],
        "thesis_conviction": float(row[10]) if row[10] is not None else None,
        "context_summary": _parse_jsonb(row[11], {}),
        "duration_ms": row[12],
        "generated_at": row[13].isoformat() if row[13] else "",
    }
