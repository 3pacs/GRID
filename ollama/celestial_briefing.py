"""
AstroGrid Celestial Narrative Synthesis.

Generates daily celestial briefings by combining:
1. Current planetary positions and aspects
2. Lunar phase and upcoming events
3. Vedic/Chinese calendar context
4. Market-astro correlation data (if available)
5. Historical pattern matching

Uses the local Qwen 32B model (same as market briefing).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── DB table DDL ──────────────────────────────────────────────────────

_ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS celestial_briefings (
    id SERIAL PRIMARY KEY,
    briefing_date DATE NOT NULL UNIQUE,
    content TEXT NOT NULL,
    celestial_state JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def _ensure_table(engine: Engine) -> None:
    """Create the celestial_briefings table if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text(_ENSURE_TABLE_SQL))


# ── LLM client (same pattern as market_briefing.py) ──────────────────

def _get_llm_client() -> Any:
    """Return the best available shared LLM client."""
    try:
        from ollama.client import get_client
        return get_client()
    except Exception:
        return None


# ── Data gathering ────────────────────────────────────────────────────

def _gather_celestial_state(engine: Engine) -> dict[str, Any]:
    """Query latest celestial features from resolved_series.

    Returns a dict with categorized celestial data:
    lunar, planetary, solar, vedic, and raw feature values.
    """
    state: dict[str, Any] = {
        "date": date.today().isoformat(),
        "lunar": {},
        "planetary": {},
        "solar": {},
        "vedic": {},
        "features": {},
    }

    # Category mappings for celestial feature names
    _CATEGORY_MAP: dict[str, str] = {
        "lunar": "lunar",
        "moon": "lunar",
        "mercury": "planetary",
        "jupiter": "planetary",
        "mars": "planetary",
        "planetary": "planetary",
        "venus": "planetary",
        "sunspot": "solar",
        "solar": "solar",
        "geomagnetic": "solar",
        "nakshatra": "vedic",
        "tithi": "vedic",
        "rahu": "vedic",
        "dasha": "vedic",
    }

    try:
        with engine.connect() as conn:
            # Get latest celestial feature values
            rows = conn.execute(
                text(
                    "SELECT fr.name, rs.value, rs.obs_date "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.model_eligible = TRUE "
                    "AND ("
                    "  LOWER(fr.name) LIKE '%lunar%' OR LOWER(fr.name) LIKE '%moon%' "
                    "  OR LOWER(fr.name) LIKE '%mercury%' OR LOWER(fr.name) LIKE '%jupiter%' "
                    "  OR LOWER(fr.name) LIKE '%mars%' OR LOWER(fr.name) LIKE '%planetary%' "
                    "  OR LOWER(fr.name) LIKE '%venus%' OR LOWER(fr.name) LIKE '%sunspot%' "
                    "  OR LOWER(fr.name) LIKE '%solar%' OR LOWER(fr.name) LIKE '%geomagnetic%' "
                    "  OR LOWER(fr.name) LIKE '%nakshatra%' OR LOWER(fr.name) LIKE '%tithi%' "
                    "  OR LOWER(fr.name) LIKE '%rahu%' OR LOWER(fr.name) LIKE '%dasha%' "
                    "  OR LOWER(fr.name) LIKE '%eclipse%' OR LOWER(fr.name) LIKE '%celestial%' "
                    ") "
                    "AND rs.obs_date = ("
                    "  SELECT MAX(obs_date) FROM resolved_series "
                    "  WHERE feature_id = rs.feature_id"
                    ") "
                    "ORDER BY fr.name"
                )
            ).fetchall()

            for name, value, obs_date in rows:
                entry = {
                    "value": round(float(value), 4) if value is not None else None,
                    "obs_date": str(obs_date),
                }
                state["features"][name] = entry

                # Categorize
                name_lower = name.lower()
                categorized = False
                for pattern, category in _CATEGORY_MAP.items():
                    if pattern in name_lower:
                        state[category][name] = entry
                        categorized = True
                        break
                if not categorized:
                    state.setdefault("other", {})[name] = entry

    except Exception as exc:
        log.warning("Could not gather celestial state: {err}", err=str(exc))

    return state


def _gather_correlations(engine: Engine, top_n: int = 10) -> list[dict[str, Any]]:
    """Fetch top statistically significant astro-market correlations."""
    correlations: list[dict[str, Any]] = []

    try:
        with engine.connect() as conn:
            # Check if the table exists first
            exists = conn.execute(
                text(
                    "SELECT EXISTS ("
                    "  SELECT FROM information_schema.tables "
                    "  WHERE table_name = 'astro_correlations'"
                    ")"
                )
            ).scalar()

            if not exists:
                return []

            rows = conn.execute(
                text(
                    "SELECT celestial_feature, market_feature, correlation, "
                    "       optimal_lag, p_value, n_observations, "
                    "       confidence_low, confidence_high "
                    "FROM astro_correlations "
                    "WHERE p_value < 0.05 "
                    "ORDER BY ABS(correlation) DESC "
                    "LIMIT :n"
                ),
                {"n": top_n},
            ).fetchall()

            for row in rows:
                correlations.append({
                    "celestial_feature": row[0],
                    "market_feature": row[1],
                    "correlation": round(row[2], 4) if row[2] is not None else None,
                    "optimal_lag": row[3],
                    "p_value": round(row[4], 6) if row[4] is not None else None,
                    "n_observations": row[5],
                    "confidence_low": round(row[6], 4) if row[6] is not None else None,
                    "confidence_high": round(row[7], 4) if row[7] is not None else None,
                })

    except Exception as exc:
        log.debug("Could not fetch astro correlations: {err}", err=str(exc))

    return correlations


# ── Prompt construction ───────────────────────────────────────────────

def _format_celestial_data(state: dict[str, Any]) -> str:
    """Format celestial state into readable text for the LLM prompt."""
    lines: list[str] = []

    for category, label in [
        ("lunar", "Lunar"),
        ("planetary", "Planetary"),
        ("solar", "Solar"),
        ("vedic", "Vedic"),
    ]:
        data = state.get(category, {})
        if data:
            lines.append(f"### {label}")
            for name, info in data.items():
                val = info.get("value")
                obs = info.get("obs_date", "?")
                lines.append(f"- {name}: {val} (as of {obs})")
            lines.append("")

    return "\n".join(lines)


def _format_correlations(correlations: list[dict[str, Any]]) -> str:
    """Format correlation data for the LLM prompt."""
    if not correlations:
        return "No statistically significant correlations computed yet."

    lines: list[str] = []
    for c in correlations:
        lag_str = f"lag {c['optimal_lag']}d" if c.get("optimal_lag") else "same day"
        lines.append(
            f"- {c['celestial_feature']} -> {c['market_feature']}: "
            f"r={c['correlation']}, p={c['p_value']}, n={c['n_observations']}, "
            f"{lag_str}, 95% CI [{c.get('confidence_low', '?')}, {c.get('confidence_high', '?')}]"
        )
    return "\n".join(lines)


def _detect_active_events(state: dict[str, Any]) -> str:
    """Identify notable active astronomical events from feature values."""
    events: list[str] = []
    features = state.get("features", {})

    # Mercury retrograde
    merc = features.get("mercury_retrograde", {}).get("value")
    if merc is not None:
        if merc == 1.0:
            events.append("- MERCURY RETROGRADE: Currently active")
        else:
            events.append("- Mercury: Direct motion")

    # Planetary stress
    stress = features.get("planetary_stress_index", {}).get("value")
    if stress is not None:
        if stress >= 3:
            events.append(f"- ELEVATED PLANETARY STRESS: {stress:.0f} hard aspects active")
        else:
            events.append(f"- Planetary stress: {stress:.0f} hard aspects (low)")

    # Eclipse proximity
    for eclipse_type, label in [
        ("lunar_eclipse_proximity", "Lunar eclipse"),
        ("solar_eclipse_proximity", "Solar eclipse"),
    ]:
        prox = features.get(eclipse_type, {}).get("value")
        if prox is not None and prox <= 14:
            events.append(f"- {label.upper()} PROXIMITY: {prox:.0f} days away")

    # Moon phase
    phase = features.get("lunar_phase", {}).get("value")
    if phase is not None:
        if phase < 0.05 or phase > 0.95:
            events.append(f"- NEW MOON: lunar_phase={phase:.3f}")
        elif 0.45 < phase < 0.55:
            events.append(f"- FULL MOON: lunar_phase={phase:.3f}")
        else:
            events.append(f"- Lunar phase: {phase:.3f}")

    # Illumination
    illum = features.get("lunar_illumination", {}).get("value")
    if illum is not None:
        events.append(f"- Lunar illumination: {illum:.1f}%")

    # Solar activity
    kp = features.get("geomagnetic_kp_index", {}).get("value")
    if kp is not None:
        if kp >= 5:
            events.append(f"- GEOMAGNETIC STORM: Kp={kp:.1f}")
        elif kp >= 4:
            events.append(f"- Elevated geomagnetic activity: Kp={kp:.1f}")
        else:
            events.append(f"- Geomagnetic Kp index: {kp:.1f}")

    storm_prob = features.get("solar_storm_probability", {}).get("value")
    if storm_prob is not None and storm_prob > 0.3:
        events.append(f"- Solar storm probability: {storm_prob:.1%}")

    # Jupiter-Saturn angle (major conjunctions/oppositions)
    js_angle = features.get("jupiter_saturn_angle", {}).get("value")
    if js_angle is not None:
        if js_angle < 10:
            events.append(f"- JUPITER-SATURN CONJUNCTION: {js_angle:.1f} degrees")
        elif js_angle > 170:
            events.append(f"- JUPITER-SATURN OPPOSITION: {js_angle:.1f} degrees")
        else:
            events.append(f"- Jupiter-Saturn angle: {js_angle:.1f} degrees")

    # Mars volatility
    mars_vol = features.get("mars_volatility_index", {}).get("value")
    if mars_vol is not None and mars_vol > 0.7:
        events.append(f"- Elevated Mars volatility index: {mars_vol:.3f}")

    # Vedic
    nakshatra = features.get("nakshatra_index", {}).get("value")
    if nakshatra is not None:
        events.append(f"- Nakshatra index: {nakshatra:.0f}")

    quality = features.get("nakshatra_quality", {}).get("value")
    if quality is not None:
        labels = {0: "fixed", 1: "movable", 2: "sharp/dreadful", 3: "mixed"}
        events.append(f"- Nakshatra quality: {labels.get(int(quality), quality)}")

    if not events:
        return "No active celestial features detected (data may be pending ingestion)."

    return "\n".join(events)


def _build_prompt(
    state: dict[str, Any],
    correlations: list[dict[str, Any]],
) -> tuple[str, str]:
    """Build system and user prompts for the celestial briefing.

    Returns:
        (system_prompt, user_prompt)
    """
    system_prompt = (
        "You are AstroGrid, a celestial intelligence system that synthesizes "
        "astronomical data with market context. You are analytical and precise "
        "— note which observations have statistical backing and which are "
        "traditional/speculative.\n\n"
        "IMPORTANT RULES:\n"
        "- If a correlation has p < 0.05, cite the actual numbers.\n"
        "- If no statistical backing exists, say 'traditionally associated with...'\n"
        "- Never invent correlations. If data is missing, say so.\n"
        "- Keep the briefing under 600 words.\n"
        "- Be honest about confidence levels."
    )

    celestial_data = _format_celestial_data(state)
    active_events = _detect_active_events(state)
    corr_text = _format_correlations(correlations)

    user_prompt = (
        f"Generate a celestial market briefing for {date.today().isoformat()}.\n\n"
        f"## Current Celestial State\n{celestial_data}\n\n"
        f"## Active Astronomical Events\n{active_events}\n\n"
        f"## Market-Astro Correlations (statistically significant)\n{corr_text}\n\n"
        "Generate a celestial market briefing with these sections:\n\n"
        "### Celestial Overview\n"
        "One paragraph: what's happening in the sky right now. Plain language.\n\n"
        "### Active Influences\n"
        "For each major active event (retrograde, eclipse proximity, major aspect):\n"
        "- What it is\n"
        "- Historical market correlation (if statistically significant, cite the numbers)\n"
        "- If no statistical backing, say 'traditionally associated with...'\n\n"
        "### Upcoming Events (Next 7 Days)\n"
        "List upcoming celestial events with dates and potential market relevance.\n\n"
        "### Pattern Recognition\n"
        "Any historical periods with similar celestial configurations and what "
        "happened in markets.\n\n"
        "### Confidence Assessment\n"
        "Rate your overall confidence: HIGH (multiple significant correlations active), "
        "MEDIUM (some suggestive patterns), LOW (mostly traditional associations). "
        "Be honest."
    )

    return system_prompt, user_prompt


# ── Main entry points ─────────────────────────────────────────────────

def generate_celestial_briefing(engine: Engine) -> dict[str, Any]:
    """Generate and store a daily celestial briefing.

    Parameters:
        engine: SQLAlchemy engine for database access.

    Returns:
        dict with keys: content, celestial_state, briefing_date, created_at.
    """
    _ensure_table(engine)

    # Gather data
    state = _gather_celestial_state(engine)
    correlations = _gather_correlations(engine)

    # Build prompt
    system_prompt, user_prompt = _build_prompt(state, correlations)

    # Call LLM (same pattern as MarketBriefingEngine)
    client = _get_llm_client()
    content: str | None = None

    if client is not None:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        content = client.chat(
            messages=messages,
            temperature=0.4,
            num_predict=800,
        )

    if content is None:
        # Fallback: data-only summary
        content = _generate_fallback(state, correlations)
        log.warning("LLM unavailable — using fallback celestial briefing")

    # Store in DB
    today = date.today()
    state_json = json.dumps(state, default=str)

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO celestial_briefings (briefing_date, content, celestial_state) "
                    "VALUES (:d, :c, :s::jsonb) "
                    "ON CONFLICT (briefing_date) DO UPDATE "
                    "SET content = EXCLUDED.content, "
                    "    celestial_state = EXCLUDED.celestial_state, "
                    "    created_at = NOW()"
                ),
                {"d": today, "c": content, "s": state_json},
            )
        log.info("Celestial briefing stored for {d}", d=today)
    except Exception as exc:
        log.warning("Could not store celestial briefing: {err}", err=str(exc))

    return {
        "content": content,
        "celestial_state": state,
        "briefing_date": today.isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def get_latest_briefing(engine: Engine) -> dict[str, Any]:
    """Return the latest celestial briefing from the database.

    If no briefing exists for today, returns yesterday's with a note.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with keys: content, celestial_state, briefing_date, created_at, stale.
    """
    _ensure_table(engine)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT briefing_date, content, celestial_state, created_at "
                    "FROM celestial_briefings "
                    "ORDER BY briefing_date DESC "
                    "LIMIT 1"
                )
            ).fetchone()

        if row is None:
            return {
                "content": None,
                "celestial_state": None,
                "briefing_date": None,
                "created_at": None,
                "stale": True,
                "note": "No celestial briefings generated yet.",
            }

        briefing_date = row[0]
        is_stale = briefing_date < date.today()

        result: dict[str, Any] = {
            "content": row[1],
            "celestial_state": row[2],
            "briefing_date": str(briefing_date),
            "created_at": str(row[3]) if row[3] else None,
            "stale": is_stale,
        }

        if is_stale:
            result["note"] = (
                f"Briefing is from {briefing_date}, not today. "
                "Today's briefing has not been generated yet."
            )

        return result

    except Exception as exc:
        log.warning("Could not fetch celestial briefing: {err}", err=str(exc))
        return {
            "content": None,
            "celestial_state": None,
            "briefing_date": None,
            "created_at": None,
            "stale": True,
            "error": str(exc),
        }


def _generate_fallback(
    state: dict[str, Any],
    correlations: list[dict[str, Any]],
) -> str:
    """Generate a data-only briefing when LLM is unavailable."""
    lines = [
        f"# Celestial Briefing — {date.today().isoformat()}",
        "",
        "**Note: AI analysis unavailable. Data summary only.**",
        "",
    ]

    for category, label in [
        ("lunar", "Lunar"),
        ("planetary", "Planetary"),
        ("solar", "Solar"),
        ("vedic", "Vedic"),
    ]:
        data = state.get(category, {})
        if data:
            lines.append(f"## {label}")
            for name, info in data.items():
                lines.append(f"- **{name}**: {info.get('value')} ({info.get('obs_date', '?')})")
            lines.append("")

    if correlations:
        lines.append("## Significant Correlations")
        for c in correlations[:5]:
            lines.append(
                f"- {c['celestial_feature']} -> {c['market_feature']}: "
                f"r={c['correlation']}, p={c['p_value']}"
            )
        lines.append("")

    lines.append("---")
    lines.append("*LLM offline — set OPENAI_API_KEY or start a local model for AI-powered celestial analysis*")

    return "\n".join(lines)
