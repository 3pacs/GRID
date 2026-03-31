"""AstroGrid shared helpers — Pydantic models and computation utilities.

Imported by the focused sub-routers:
  astrogrid_core.py       — /overview, /snapshot, /scorecard, /universe, /interpret
  astrogrid_predictions.py — /predictions, /backtest, /weights, /review, /learning-loop
  astrogrid_celestial.py  — /ephemeris, /correlations, /timeline, /briefing, /compare, etc.
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timedelta
from typing import Any
from uuid import uuid4  # noqa: F401 — re-exported for sub-router convenience

from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from analysis.ephemeris import (
    Ephemeris as AstroEphemeris,
    OBLIQUITY_J2000 as EPHEMERIS_OBLIQUITY_J2000,
    ZODIAC_SIGNS as EPHEMERIS_ZODIAC_SIGNS,
    _ecliptic_to_equatorial as _ephemeris_ecliptic_to_equatorial,
    _normalize_angle as _ephemeris_normalize_angle,
    get_ephemeris as build_astrological_ephemeris,
)
from ingestion.celestial.lunar import (
    _lunar_phase,
    _illumination,
    _days_to_phase,
    _nearest_eclipse,
    _LUNAR_ECLIPSES,
    _SOLAR_ECLIPSES,
    SYNODIC_MONTH,
    LunarCyclePuller,
)
from ingestion.celestial.planetary import (
    _is_mercury_retrograde,
    _geo_longitude,
    _angular_separation,
    _hard_aspect_count,
    _venus_synodic_phase,
    _mars_volatility_index,
    _MERCURY_RETROGRADES,
    PlanetaryAspectPuller,
)
from ingestion.celestial.solar import (
    _solar_cycle_phase,
    _kp_to_ap,
    SolarActivityPuller,
)
from ingestion.celestial.vedic import (
    _moon_sidereal_longitude,
    _nakshatra_from_longitude,
    _tithi,
    _rahu_longitude,
    _dasha_cycle_phase,
    _NAKSHATRA_NAMES,
    _NAKSHATRA_QUALITY,
    VedicAstroPuller,
)
from ingestion.celestial.chinese import (
    _zodiac_index,
    _element_index,
    _yin_yang,
    _flying_star,
    _chinese_lunar_month,
    _iching_hexagram,
    _ZODIAC_ANIMALS,
    _ELEMENTS,
    ChineseCalendarPuller,
)
from oracle.astrogrid_universe import (
    enrich_astrogrid_scoreable_universe,
    get_astrogrid_scoreable_universe,
    scoreable_universe_by_symbol,
)
from oracle.scoreboard import build_oracle_ticker_rollup
from oracle.publish import publish_astrogrid_prediction

# Re-export celestial imports so sub-routers can import from here
__all__ = [
    # Pydantic models
    "CompareDatesRequest",
    "AstrogridInterpretRequest",
    "AstrogridPredictionRequest",
    "AstrogridScoreRequest",
    "AstrogridBacktestRequest",
    "AstrogridReviewRequest",
    "AstrogridWeightDecisionRequest",
    "AstrogridLearningLoopRequest",
    # Celestial raw imports
    "AstroEphemeris",
    "build_astrological_ephemeris",
    "_lunar_phase",
    "_illumination",
    "_days_to_phase",
    "_nearest_eclipse",
    "_LUNAR_ECLIPSES",
    "_SOLAR_ECLIPSES",
    "SYNODIC_MONTH",
    "_is_mercury_retrograde",
    "_geo_longitude",
    "_angular_separation",
    "_hard_aspect_count",
    "_venus_synodic_phase",
    "_mars_volatility_index",
    "_MERCURY_RETROGRADES",
    "_solar_cycle_phase",
    "_kp_to_ap",
    "_moon_sidereal_longitude",
    "_nakshatra_from_longitude",
    "_tithi",
    "_rahu_longitude",
    "_dasha_cycle_phase",
    "_NAKSHATRA_NAMES",
    "_NAKSHATRA_QUALITY",
    "_zodiac_index",
    "_element_index",
    "_yin_yang",
    "_flying_star",
    "_chinese_lunar_month",
    "_iching_hexagram",
    "_ZODIAC_ANIMALS",
    "_ELEMENTS",
    "enrich_astrogrid_scoreable_universe",
    "get_astrogrid_scoreable_universe",
    "scoreable_universe_by_symbol",
    "build_oracle_ticker_rollup",
    "publish_astrogrid_prediction",
    # Helper functions
    "_phase_name",
    "_parse_snapshot_date",
    "_public_lens_label",
    "_parse_optional_date",
    "_compact_prediction_snapshot",
    "_infer_prediction_horizon",
    "_infer_target_symbols",
    "_target_groups_for_symbols",
    "_infer_target_group",
    "_infer_question_intent",
    "_grid_driver_summary",
    "_mystical_driver_summary",
    "_build_postmortem_stub",
    "_prediction_confidence",
    "_signed_longitude_delta",
    "_compute_sun_position",
    "_body_longitude",
    "_daily_motion",
    "_build_objects",
    "_dominant_element",
    "_get_market_regime",
    "_build_signal_field",
    "_build_snapshot_events",
    "_build_snapshot_seer",
    "_llm_backend_name",
    "_top_snapshot_threads",
    "_compact_engine_outputs",
    "_classify_prediction_scoreability",
    "_parse_json_response",
    "_fallback_interpretation",
    "_build_interpret_messages",
    "_get_llm_client",
    "_compact_objects_for_prompt",
    "_compact_aspects_for_prompt",
    "_compact_engine_runs_for_prompt",
    "_compact_threads_for_prompt",
    "_compute_full_ephemeris",
    "_interpret_kp",
    "_get_latest_resolved",
    "_signed_pct_change",
    "_find_history_baseline",
    "_momentum_score",
    "_momentum_bias",
    "_momentum_trend",
    "_scorecard_confidence",
    "_resolve_scorecard_feature",
    "_load_scorecard_history",
    "_build_scorecard_item",
    "_group_scorecard_items",
    "_build_scorecard_summary",
    "_build_scorecard_evaluation",
]


# ── Pydantic models ────────────────────────────────────────────────────────

class CompareDatesRequest(BaseModel):
    date1: str
    date2: str


class AstrogridInterpretRequest(BaseModel):
    question: str = "What threads matter now?"
    mode: str = "chorus"
    lens_ids: list[str] = []
    threads: list[dict[str, Any]] = []
    snapshot: dict[str, Any] = {}
    engine_outputs: list[dict[str, Any]] = []
    seer: dict[str, Any] = {}
    persona_id: str = "seer"


class AstrogridPredictionRequest(BaseModel):
    question: str
    call: str
    timing: str
    setup: str
    invalidation: str
    as_of_ts: str | None = None
    note: str = ""
    mode: str = "chorus"
    lens_ids: list[str] = []
    snapshot: dict[str, Any] = {}
    seer: dict[str, Any] = {}
    engine_outputs: list[dict[str, Any]] = []
    market_overlay_snapshot: dict[str, Any] = {}
    target_universe: str = "hybrid"
    target_symbols: list[str] = []
    horizon_label: str | None = None
    weight_version: str = "astrogrid-v1"
    model_version: str = "astrogrid-oracle-v1"
    live_or_local: str = "local"
    scoring_class: str = "liquid_market"
    publish_oracle: bool = True


class AstrogridScoreRequest(BaseModel):
    as_of_date: str | None = None
    limit: int = 100
    prediction_ids: list[str] = []


class AstrogridBacktestRequest(BaseModel):
    strategy_variants: list[str] = []
    horizon_label: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    limit: int = 250


class AstrogridReviewRequest(BaseModel):
    provider_mode: str = "deterministic"
    prediction_limit: int = 200
    backtest_limit: int = 12


class AstrogridWeightDecisionRequest(BaseModel):
    decided_by: str = "system"
    notes: str = ""


class AstrogridLearningLoopRequest(BaseModel):
    as_of_date: str | None = None
    score_limit: int = 200
    backtest_limit: int = 250
    backtest_window_days: int = 180
    provider_mode: str = "deterministic"
    horizon_label: str | None = None


# ── Constants ─────────────────────────────────────────────────────────────

_PHASE_NAMES = [
    (0.000, 0.025, "New Moon"),
    (0.025, 0.225, "Waxing Crescent"),
    (0.225, 0.275, "First Quarter"),
    (0.275, 0.475, "Waxing Gibbous"),
    (0.475, 0.525, "Full Moon"),
    (0.525, 0.725, "Waning Gibbous"),
    (0.725, 0.775, "Last Quarter"),
    (0.775, 0.975, "Waning Crescent"),
    (0.975, 1.001, "New Moon"),
]

_BODY_META = {
    "Sun": {"id": "sun", "class": "luminary", "glyph": "Su", "visual_priority": 100},
    "Moon": {"id": "moon", "class": "luminary", "glyph": "Mo", "visual_priority": 95},
    "Mercury": {"id": "mercury", "class": "planet", "glyph": "Me", "visual_priority": 90},
    "Venus": {"id": "venus", "class": "planet", "glyph": "Ve", "visual_priority": 88},
    "Mars": {"id": "mars", "class": "planet", "glyph": "Ma", "visual_priority": 86},
    "Jupiter": {"id": "jupiter", "class": "planet", "glyph": "Ju", "visual_priority": 84},
    "Saturn": {"id": "saturn", "class": "planet", "glyph": "Sa", "visual_priority": 82},
    "Uranus": {"id": "uranus", "class": "planet", "glyph": "Ur", "visual_priority": 76},
    "Neptune": {"id": "neptune", "class": "planet", "glyph": "Ne", "visual_priority": 74},
    "Pluto": {"id": "pluto", "class": "planet", "glyph": "Pl", "visual_priority": 72},
    "Rahu": {"id": "rahu", "class": "node", "glyph": "Ra", "visual_priority": 68},
    "Ketu": {"id": "ketu", "class": "node", "glyph": "Ke", "visual_priority": 66},
}

_ELEMENT_BY_SIGN = {
    "Aries": "fire",
    "Taurus": "earth",
    "Gemini": "air",
    "Cancer": "water",
    "Leo": "fire",
    "Virgo": "earth",
    "Libra": "air",
    "Scorpio": "water",
    "Sagittarius": "fire",
    "Capricorn": "earth",
    "Aquarius": "air",
    "Pisces": "water",
}

_MARKET_REGIME_BIAS = {
    "risk_on": 0.8,
    "bull": 0.8,
    "bullish": 0.8,
    "momentum": 0.55,
    "trend": 0.4,
    "neutral": 0.0,
    "range": 0.0,
    "uncertain": 0.0,
    "defensive": -0.45,
    "risk_off": -0.75,
    "bear": -0.85,
    "bearish": -0.85,
}

_PUBLIC_LENS_LABELS = {
    "western": "Meridian House",
    "hellenistic": "Bronze Hour",
    "vedic": "Lunar Knot",
    "hermetic": "Mirror Gate",
    "iching": "Turning Lines",
    "kabbalistic": "Ladder Seal",
    "babylonian": "Watchtower",
    "maya": "Count Wheel",
    "arabic": "Star Road",
    "egyptian": "Solar Gate",
    "taoist": "Quiet Current",
    "tantric": "Inner Seal",
}

_HYBRID_SCORECARD_UNIVERSE = get_astrogrid_scoreable_universe()

_GROUP_CUE_MAP = {
    "crypto": {"crypto", "coin", "token", "btc", "eth", "sol", "bitcoin", "ethereum", "solana"},
    "equity": {"stock", "stocks", "equity", "equities", "shares", "tech", "apple", "microsoft",
               "google", "alphabet", "nvidia", "meta"},
    "macro": {"macro", "hedge", "index", "indices", "bond", "bonds", "dollar", "gold", "crude",
              "oil", "spy", "qqq", "tlt", "dxy", "gld", "cl"},
}

_ASSET_ALIASES = {
    "BTC": {"btc", "bitcoin"},
    "ETH": {"eth", "ethereum"},
    "SOL": {"sol", "solana"},
    "AAPL": {"aapl", "apple"},
    "MSFT": {"msft", "microsoft"},
    "GOOGL": {"googl", "goog", "google", "alphabet"},
    "NVDA": {"nvda", "nvidia"},
    "META": {"meta", "facebook"},
    "SPY": {"spy", "s&p 500", "sp500"},
    "QQQ": {"qqq", "nasdaq 100", "nasdaq"},
    "TLT": {"tlt", "long bonds", "treasuries", "treasury"},
    "DXY": {"dxy", "dollar index", "dollar"},
    "GLD": {"gld", "gold"},
    "CL": {"cl", "cl=f", "crude", "crude oil", "oil"},
}


# ── Helper functions ───────────────────────────────────────────────────────

def _phase_name(phase: float) -> str:
    for lo, hi, name in _PHASE_NAMES:
        if lo <= phase < hi:
            return name
    return "Unknown"


def _parse_snapshot_date(value: str | None) -> date:
    if not value:
        return date.today()
    try:
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid date format: {value}. Use YYYY-MM-DD or ISO datetime."
        ) from exc


def _public_lens_label(value: str) -> str:
    key = str(value or "").strip().lower()
    return _PUBLIC_LENS_LABELS.get(key, key or "unknown lens")


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def _compact_prediction_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {}
    grid_state = snapshot.get("grid") if isinstance(snapshot.get("grid"), dict) else {}
    return {
        "lunar": snapshot.get("lunar"),
        "nakshatra": snapshot.get("nakshatra"),
        "aspects": list(snapshot.get("aspects") or [])[:8],
        "signals": snapshot.get("signals") or {},
        "signal_field": list(snapshot.get("signal_field") or [])[:8],
        "void_of_course": snapshot.get("void_of_course") or {},
        "retrograde_planets": list(snapshot.get("retrograde_planets") or [])[:8],
        "events": list(snapshot.get("events") or [])[:6],
        "canonical_ephemeris": snapshot.get("canonical_ephemeris") or {},
        "grid": {
            "market_regime": grid_state.get("market_regime"),
            "market_regime_bias": grid_state.get("market_regime_bias"),
            "solar": grid_state.get("solar") or {},
        },
    }


def _infer_prediction_horizon(req: AstrogridPredictionRequest) -> str:
    explicit = str(req.horizon_label or "").strip().lower()
    if explicit in {"macro", "swing"}:
        return explicit
    seer_horizon = str((req.seer or {}).get("horizon") or "").lower()
    if "week" in seer_horizon or "cycle" in seer_horizon or "macro" in seer_horizon:
        return "macro"
    return "swing"


def _infer_target_symbols(req: AstrogridPredictionRequest) -> list[str]:
    if req.target_symbols:
        return [str(symbol).upper() for symbol in req.target_symbols[:12]]
    text_corpus = " ".join(
        part for part in [
            req.question,
            req.call,
            req.setup,
            req.note,
            req.invalidation,
        ]
        if part
    ).lower()

    matched_symbols: list[str] = []
    for symbol, aliases in _ASSET_ALIASES.items():
        for alias in aliases:
            if re.search(rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])", text_corpus):
                matched_symbols.append(symbol)
                break
    if matched_symbols:
        return list(dict.fromkeys(matched_symbols))[:12]

    group_filter = None
    for group_name, cues in _GROUP_CUE_MAP.items():
        if any(
            re.search(rf"(?<![a-z0-9]){re.escape(cue)}(?![a-z0-9])", text_corpus)
            for cue in cues
        ):
            group_filter = group_name
            break

    overlay = req.market_overlay_snapshot or {}
    scorecard = overlay.get("scorecard") if isinstance(overlay, dict) else {}
    symbols: list[str] = []
    if isinstance(scorecard, dict):
        for bucket in ("leaders", "laggards"):
            for item in list(scorecard.get(bucket) or [])[:3]:
                symbol = item.get("symbol") if isinstance(item, dict) else None
                item_group = str(item.get("group") or "").lower() if isinstance(item, dict) else ""
                if group_filter and item_group and item_group != group_filter:
                    continue
                if symbol:
                    symbols.append(str(symbol).upper())
    if symbols:
        return list(dict.fromkeys(symbols))[:12]

    if group_filter:
        group_symbols = [
            str(asset["symbol"]).upper()
            for asset in _HYBRID_SCORECARD_UNIVERSE
            if str(asset.get("group") or "").lower() == group_filter
        ]
        if group_symbols:
            return group_symbols[:12]
    return []


def _target_groups_for_symbols(target_symbols: list[str]) -> list[str]:
    groups: list[str] = []
    by_symbol = {
        str(asset.get("symbol") or "").upper(): str(asset.get("group") or "").lower()
        for asset in _HYBRID_SCORECARD_UNIVERSE
        if asset.get("symbol")
    }
    for symbol in target_symbols:
        group = by_symbol.get(str(symbol).upper())
        if group:
            groups.append(group)
    return list(dict.fromkeys(groups))


def _infer_target_group(target_symbols: list[str], req: AstrogridPredictionRequest) -> str:
    groups = _target_groups_for_symbols(target_symbols)
    if len(groups) == 1:
        return groups[0]
    if len(groups) > 1:
        return "hybrid"
    text_corpus = " ".join(
        part for part in [
            req.question,
            req.call,
            req.setup,
            req.note,
            req.invalidation,
        ]
        if part
    ).lower()
    for group_name, cues in _GROUP_CUE_MAP.items():
        if any(
            re.search(rf"(?<![a-z0-9]){re.escape(cue)}(?![a-z0-9])", text_corpus)
            for cue in cues
        ):
            return group_name
    return "hybrid"


def _infer_question_intent(
    req: AstrogridPredictionRequest, target_symbols: list[str]
) -> str:
    text_corpus = " ".join(
        part for part in [
            req.question,
            req.call,
            req.setup,
            req.note,
            req.invalidation,
        ]
        if part
    ).lower()
    if "avoid" in text_corpus or "should i avoid" in text_corpus:
        return "avoid_now"
    if "when should i buy" in text_corpus or ("when" in text_corpus and "buy" in text_corpus):
        return "timing_entry"
    if "wait" in text_corpus and "buy" in text_corpus:
        return "buy_or_wait"
    if any(
        phrase in text_corpus
        for phrase in (
            "which is best",
            "best buy",
            "better buy",
            "which stock",
            "which crypto",
            "which should i buy",
        )
    ):
        return "relative_strength_choice"
    if "should i buy" in text_corpus or "what should i buy" in text_corpus:
        return "best_buy_now"
    if (
        "shouldn't move" in text_corpus
        or "should not move" in text_corpus
        or "flat" in text_corpus
    ):
        return "range_bound_view"
    if len(target_symbols) > 1:
        return "relative_strength_choice"
    return "directional_view"


def _grid_driver_summary(
    market_overlay: dict[str, Any],
) -> tuple[list[str], str]:
    drivers: list[str] = []
    regime = market_overlay.get("regime") if isinstance(market_overlay, dict) else {}
    thesis = market_overlay.get("thesis") if isinstance(market_overlay, dict) else {}
    if isinstance(regime, dict) and regime.get("state"):
        drivers.append(f"regime:{regime.get('state')}")
    if isinstance(thesis, dict) and thesis.get("bias"):
        drivers.append(f"thesis:{thesis.get('bias')}")
    scorecard = market_overlay.get("scorecard") if isinstance(market_overlay, dict) else {}
    if isinstance(scorecard, dict):
        leaders = scorecard.get("leaders") or []
        if leaders and isinstance(leaders[0], dict):
            drivers.append(f"leader:{leaders[0].get('symbol')}")
        laggards = scorecard.get("laggards") or []
        if laggards and isinstance(laggards[0], dict):
            drivers.append(f"laggard:{laggards[0].get('symbol')}")
    return drivers[:5], " / ".join(drivers[:3]) or "grid overlay thin"


def _mystical_driver_summary(
    req: AstrogridPredictionRequest,
) -> tuple[list[str], str]:
    snapshot = req.snapshot or {}
    lunar = snapshot.get("lunar") if isinstance(snapshot, dict) else {}
    nakshatra = snapshot.get("nakshatra") if isinstance(snapshot, dict) else {}
    aspects = snapshot.get("aspects") if isinstance(snapshot, dict) else []
    drivers: list[str] = []
    if isinstance(lunar, dict) and lunar.get("phase_name"):
        drivers.append(f"moon:{lunar.get('phase_name')}")
    if isinstance(nakshatra, dict) and nakshatra.get("nakshatra_name"):
        drivers.append(f"nakshatra:{nakshatra.get('nakshatra_name')}")
    if aspects and isinstance(aspects[0], dict):
        drivers.append(
            f"aspect:{aspects[0].get('planet1')} "
            f"{aspects[0].get('aspect_type')} "
            f"{aspects[0].get('planet2')}"
        )
    for engine in req.engine_outputs[:2]:
        if engine.get("engine_id"):
            drivers.append(f"lens:{engine.get('engine_id')}")
    return drivers[:5], " / ".join(drivers[:3]) or "celestial field muted"


def _build_postmortem_stub(req: AstrogridPredictionRequest) -> dict[str, Any]:
    grid_drivers, grid_summary = _grid_driver_summary(req.market_overlay_snapshot or {})
    mystical_drivers, mystical_summary = _mystical_driver_summary(req)
    horizon = _infer_prediction_horizon(req)
    target_symbols = _infer_target_symbols(req)
    summary = (
        f"Pending {horizon} read on "
        f"{', '.join(target_symbols) if target_symbols else req.target_universe}: "
        f"{req.call}. Break if {req.invalidation.lower()}."
    )
    return {
        "summary": summary,
        "dominant_grid_drivers": grid_drivers,
        "dominant_mystical_drivers": mystical_drivers,
        "feature_family_summary": {
            "grid": grid_drivers,
            "mystical": mystical_drivers,
        },
        "grid_summary": grid_summary,
        "mystical_summary": mystical_summary,
    }


def _prediction_confidence(req: AstrogridPredictionRequest) -> float:
    try:
        value = float((req.seer or {}).get("confidence"))
    except (TypeError, ValueError):
        return 0.5
    return min(max(value, 0.0), 1.0)


def _signed_longitude_delta(current: float, future: float) -> float:
    diff = (future - current) % 360.0
    if diff > 180.0:
        diff -= 360.0
    return diff


def _compute_sun_position(
    ephemeris: AstroEphemeris, target: date
) -> dict[str, Any]:
    T = ephemeris.centuries_since_j2000(target)
    earth_lon, earth_lat, earth_dist = ephemeris._heliocentric_position("Earth", T)
    sun_lon = _ephemeris_normalize_angle(earth_lon + 180.0)
    sun_lat = -earth_lat
    obliquity = EPHEMERIS_OBLIQUITY_J2000 - 0.013004 * T
    ra, dec = _ephemeris_ecliptic_to_equatorial(sun_lon, sun_lat, obliquity)
    sign_idx = int(sun_lon / 30.0) % 12
    sign_deg = sun_lon % 30.0
    return {
        "planet": "Sun",
        "ecliptic_longitude": round(sun_lon, 4),
        "ecliptic_latitude": round(sun_lat, 4),
        "heliocentric_longitude": None,
        "distance_au": round(earth_dist, 6),
        "geocentric_longitude": round(sun_lon, 4),
        "zodiac_sign": EPHEMERIS_ZODIAC_SIGNS[sign_idx],
        "zodiac_degree": round(sign_deg, 4),
        "is_retrograde": False,
        "right_ascension": round(ra, 4),
        "declination": round(dec, 4),
    }


def _body_longitude(
    ephemeris: AstroEphemeris, body_name: str, target: date
) -> float | None:
    if body_name == "Sun":
        return float(_compute_sun_position(ephemeris, target)["geocentric_longitude"])
    try:
        return float(ephemeris.compute_position(body_name, target)["geocentric_longitude"])
    except Exception:
        return None


def _daily_motion(
    ephemeris: AstroEphemeris, body_name: str, target: date
) -> float | None:
    current = _body_longitude(ephemeris, body_name, target)
    future = _body_longitude(ephemeris, body_name, target + timedelta(days=1))
    if current is None or future is None:
        return None
    return round(_signed_longitude_delta(current, future), 4)


def _build_objects(
    target: date,
    ephemeris: AstroEphemeris,
    full_ephemeris: dict[str, Any],
) -> list[dict[str, Any]]:
    positions = dict(full_ephemeris["positions"])
    positions["Sun"] = _compute_sun_position(ephemeris, target)

    ordered_bodies = [
        "Sun", "Moon", "Mercury", "Venus", "Mars",
        "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto",
        "Rahu", "Ketu",
    ]

    objects: list[dict[str, Any]] = []
    for body_name in ordered_bodies:
        meta = _BODY_META[body_name]
        position = positions[body_name]
        objects.append({
            "id": meta["id"],
            "name": body_name,
            "class": meta["class"],
            "glyph": meta["glyph"],
            "visual_priority": meta["visual_priority"],
            "track_mode": "reliable",
            "precision": "computed",
            "source": "analysis.ephemeris",
            "longitude": position.get("geocentric_longitude"),
            "latitude": position.get("ecliptic_latitude"),
            "right_ascension": position.get("right_ascension"),
            "declination": position.get("declination"),
            "distance": position.get("distance_au"),
            "speed": _daily_motion(ephemeris, body_name, target),
            "sign": position.get("zodiac_sign"),
            "degree": position.get("zodiac_degree"),
            "retrograde": bool(position.get("is_retrograde", False)),
        })
    return objects


def _dominant_element(objects: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for body in objects:
        sign = body.get("sign")
        element = _ELEMENT_BY_SIGN.get(sign)
        if not element:
            continue
        counts[element] = counts.get(element, 0) + 1
    if not counts:
        return "unknown"
    return max(counts.items(), key=lambda item: item[1])[0]


def _get_market_regime(engine: Any, target: date) -> tuple[str | None, float | None]:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT regime_label "
                    "FROM regime_history "
                    "WHERE obs_date <= :target "
                    "ORDER BY obs_date DESC LIMIT 1"
                ),
                {"target": target},
            ).fetchone()
    except Exception:
        return None, None

    if not row:
        return None, None

    label = str(row[0])
    normalized = label.strip().lower().replace(" ", "_").replace("-", "_")
    return label, _MARKET_REGIME_BIAS.get(normalized)


def _build_signal_field(
    lunar: dict[str, Any],
    nakshatra: dict[str, Any],
    aspects: list[dict[str, Any]],
    objects: list[dict[str, Any]],
    solar_features: dict[str, float | None],
    market_regime: str | None,
    market_bias: float | None,
    void_of_course: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    hard_aspects = [
        aspect for aspect in aspects
        if aspect.get("aspect_type") in {"conjunction", "square", "opposition"}
    ]
    soft_aspects = [
        aspect for aspect in aspects
        if aspect.get("aspect_type") in {"trine", "sextile"}
    ]
    retrogrades = [obj for obj in objects if obj.get("retrograde") and obj["class"] != "node"]
    dominant_element = _dominant_element(objects)
    kp_value = solar_features.get("geomagnetic_kp_index")

    signals = {
        "planetaryStress": len(hard_aspects),
        "softAspectCount": len(soft_aspects),
        "retrogradeCount": len(retrogrades),
        "lunarIllumination": lunar.get("illumination"),
        "lunarPhase": lunar.get("phase_name"),
        "lunarCycleEdge": round(1.0 - abs(float(lunar.get("phase", 0.5)) - 0.5) * 2.0, 4),
        "nakshatra": nakshatra.get("nakshatra_name"),
        "nakshatraQuality": nakshatra.get("quality"),
        "voidOfCourse": bool(void_of_course.get("is_void")),
        "dominantElement": dominant_element,
        "solarGeomagneticKp": kp_value,
        "solarGeomagneticStatus": _interpret_kp(kp_value),
        "solarWindSpeed": solar_features.get("solar_wind_speed"),
        "solarCyclePhase": solar_features.get("solar_cycle_phase"),
        "marketRegime": market_regime,
        "marketRegimeBias": market_bias,
    }

    signal_field = [
        {
            "key": "planetary_stress",
            "name": "Planetary Stress",
            "value": len(hard_aspects),
            "label": f"{len(hard_aspects)} hard aspects",
            "direction": "up" if len(hard_aspects) >= 4 else "flat",
            "description": "Hard aspect count derived from current sky geometry.",
        },
        {
            "key": "retrograde_pressure",
            "name": "Retrograde Pressure",
            "value": -len(retrogrades),
            "label": f"{len(retrogrades)} active retrogrades",
            "direction": "down" if retrogrades else "flat",
            "description": "Retrograde bodies reduce clean forward motion.",
        },
        {
            "key": "lunar_illumination",
            "name": "Lunar Illumination",
            "value": round((float(lunar.get("illumination", 0)) / 100.0) - 0.5, 4),
            "label": f"{float(lunar.get('illumination', 0)):.1f}%",
            "direction": "up" if float(lunar.get("illumination", 0)) >= 50 else "down",
            "description": f"The moon is {str(lunar.get('phase_name', 'unknown')).lower()}.",
        },
    ]

    if kp_value is not None:
        signal_field.append({
            "key": "geomagnetic_kp_index",
            "name": "Geomagnetic Kp",
            "value": kp_value,
            "label": _interpret_kp(kp_value),
            "direction": "down" if kp_value >= 5 else "flat",
            "description": "Latest geomagnetic reading from the shared data layer.",
        })

    if market_regime:
        signal_field.append({
            "key": "market_regime",
            "name": "Market Regime",
            "value": market_bias if market_bias is not None else 0.0,
            "label": market_regime,
            "direction": (
                "up" if market_bias is not None and market_bias > 0
                else "down" if market_bias is not None and market_bias < 0
                else "flat"
            ),
            "description": "Latest regime label carried from GRID.",
        })

    return signals, signal_field


def _build_snapshot_events(
    target: date,
    lunar: dict[str, Any],
    nakshatra: dict[str, Any],
    aspects: list[dict[str, Any]],
    void_of_course: dict[str, Any],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    today_iso = str(target)

    events.append({
        "id": f"phase-{today_iso}",
        "type": "lunar",
        "name": lunar.get("phase_name", "Lunar phase"),
        "date": today_iso,
        "description": f"{float(lunar.get('illumination', 0)):.1f}% illumination.",
        "days_until": 0,
    })

    next_full = target + timedelta(days=int(round(float(lunar.get("days_to_full", 0)))))
    next_new = target + timedelta(days=int(round(float(lunar.get("days_to_new", 0)))))
    events.append({
        "id": f"next-full-{next_full.isoformat()}",
        "type": "full_moon_window",
        "name": "Next Full Moon",
        "date": next_full.isoformat(),
        "description": "Synodic midpoint.",
        "days_until": (next_full - target).days,
    })
    events.append({
        "id": f"next-new-{next_new.isoformat()}",
        "type": "new_moon_window",
        "name": "Next New Moon",
        "date": next_new.isoformat(),
        "description": "Cycle reset.",
        "days_until": (next_new - target).days,
    })

    if void_of_course.get("is_void"):
        events.append({
            "id": f"voc-{today_iso}",
            "type": "void_of_course",
            "name": "Void of Course",
            "date": today_iso,
            "description": (
                f"Moon holds {void_of_course.get('current_sign', 'its sign')} "
                "with no major aspect before exit."
            ),
            "days_until": 0,
        })

    if nakshatra.get("nakshatra_name"):
        events.append({
            "id": f"nak-{today_iso}",
            "type": "nakshatra",
            "name": f"Nakshatra {nakshatra['nakshatra_name']}",
            "date": today_iso,
            "description": (
                f"{nakshatra.get('quality', 'unmarked')} quality. "
                f"Pada {nakshatra.get('pada', '—')}."
            ),
            "days_until": 0,
        })

    for aspect in sorted(aspects, key=lambda item: float(item.get("orb_used", 999)))[:3]:
        events.append({
            "id": f"aspect-{aspect['planet1']}-{aspect['planet2']}-{aspect['aspect_type']}",
            "type": "aspect",
            "name": f"{aspect['planet1']} {aspect['aspect_type']} {aspect['planet2']}",
            "date": today_iso,
            "description": (
                f"Orb {float(aspect.get('orb_used', 0)):.2f}°. "
                f"{'Applying.' if aspect.get('applying') else 'Separating.'}"
            ),
            "days_until": 0,
        })

    for start, end in _MERCURY_RETROGRADES:
        if start <= target <= end:
            events.append({
                "id": f"retro-mercury-{start.isoformat()}",
                "type": "retrograde",
                "name": "Mercury Retrograde",
                "date": str(start),
                "end_date": str(end),
                "description": f"Active until {end.isoformat()}.",
                "days_until": 0,
            })
            break
        if target < start <= target + timedelta(days=120):
            events.append({
                "id": f"retro-mercury-{start.isoformat()}",
                "type": "retrograde_window",
                "name": "Mercury Retrograde Window",
                "date": str(start),
                "end_date": str(end),
                "description": f"Begins in {(start - target).days} days.",
                "days_until": (start - target).days,
            })
            break

    next_lunar = next((ecl for ecl in _LUNAR_ECLIPSES if ecl >= target), None)
    next_solar = next((ecl for ecl in _SOLAR_ECLIPSES if ecl >= target), None)
    if next_lunar:
        events.append({
            "id": f"eclipse-lunar-{next_lunar.isoformat()}",
            "type": "eclipse",
            "name": "Next Lunar Eclipse",
            "date": next_lunar.isoformat(),
            "description": "Lunar discontinuity ahead.",
            "days_until": (next_lunar - target).days,
        })
    if next_solar:
        events.append({
            "id": f"eclipse-solar-{next_solar.isoformat()}",
            "type": "eclipse",
            "name": "Next Solar Eclipse",
            "date": next_solar.isoformat(),
            "description": "Solar discontinuity ahead.",
            "days_until": (next_solar - target).days,
        })

    events.sort(key=lambda item: (int(item.get("days_until", 9999)), item["date"]))
    return events[:8]


def _build_snapshot_seer(
    lunar: dict[str, Any],
    nakshatra: dict[str, Any],
    signals: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    stress = int(signals.get("planetaryStress") or 0)
    softness = int(signals.get("softAspectCount") or 0)
    retrogrades = int(signals.get("retrogradeCount") or 0)
    kp_value = signals.get("solarGeomagneticKp")
    void_of_course = bool(signals.get("voidOfCourse"))
    market_bias = signals.get("marketRegimeBias")

    pressure = (
        stress + retrogrades
        + (1 if void_of_course else 0)
        + (1 if kp_value is not None and kp_value >= 5 else 0)
    )
    release = softness + (1 if float(lunar.get("illumination", 0)) >= 50 else 0)

    if pressure >= 6 and pressure > release:
        reading = "Hard aspects, retrogrades, and timing friction dominate."
        prediction = "Expect failed breaks, sharper reversals, and narrower acceptable risk."
        confidence = 0.72
    elif release >= pressure + 2:
        reading = "Soft aspects and lunar timing currently outweigh pressure."
        prediction = "Continuation has the cleaner edge while the current geometry holds."
        confidence = 0.69
    else:
        reading = "The active lenses are mixed; no side has clear control."
        prediction = "Favor selective timing over broad conviction until the next cleaner cut."
        confidence = 0.6

    if market_bias is not None:
        if market_bias > 0.3 and pressure >= 6:
            conflicts = ["market regime leans risk-on while celestial pressure stays elevated"]
        elif market_bias < -0.3 and release > pressure:
            conflicts = ["market regime leans defensive while the sky opens"]
        else:
            conflicts = []
    else:
        conflicts = []

    key_factors = [
        f"{signals.get('planetaryStress', 0)} hard aspects",
        f"{signals.get('retrogradeCount', 0)} retrogrades",
        nakshatra.get("nakshatra_name", "unmarked nakshatra"),
        lunar.get("phase_name", "lunar state"),
    ]
    if kp_value is not None:
        key_factors.append(_interpret_kp(kp_value))
    if void_of_course:
        key_factors.append("void-of-course moon")

    supporting_lenses = ["western", "hellenistic"]
    if nakshatra.get("nakshatra_name"):
        supporting_lenses.append("vedic")
    if any(event["type"] == "eclipse" for event in events):
        supporting_lenses.append("babylonian")
    if signals.get("dominantElement") in {"air", "water"}:
        supporting_lenses.append("taoist")

    if confidence >= 0.7:
        confidence_band = "high"
    elif confidence >= 0.62:
        confidence_band = "medium"
    else:
        confidence_band = "low"

    return {
        "reading": reading,
        "prediction": prediction,
        "confidence": round(confidence, 3),
        "confidence_band": confidence_band,
        "key_factors": key_factors,
        "supporting_lenses": list(dict.fromkeys(supporting_lenses)),
        "conflicts": conflicts,
    }


def _llm_backend_name(client: Any) -> str:
    if client is None:
        return "none"
    name = type(client).__name__.lower()
    if "openai" in name:
        return "openai"
    if "llama" in name:
        return "llamacpp"
    if "ollama" in name:
        return "ollama"
    return name


def _top_snapshot_threads(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    threads: list[dict[str, Any]] = []
    aspects = list(snapshot.get("aspects") or [])
    for aspect in sorted(aspects, key=lambda item: float(item.get("orb_used", 999)))[:5]:
        threads.append({
            "kind": "aspect",
            "title": (
                f"{aspect.get('planet1')} {aspect.get('aspect_type')} {aspect.get('planet2')}"
            ),
            "detail": (
                f"Orb {float(aspect.get('orb_used', 0)):.2f}°. "
                f"{'Applying' if aspect.get('applying') else 'Separating'}."
            ),
        })

    lunar = snapshot.get("lunar") or {}
    if lunar:
        threads.append({
            "kind": "lunar",
            "title": str(lunar.get("phase_name", "Lunar phase")),
            "detail": f"{float(lunar.get('illumination', 0)):.1f}% illumination.",
        })

    nakshatra = snapshot.get("nakshatra") or {}
    if nakshatra:
        threads.append({
            "kind": "nakshatra",
            "title": str(nakshatra.get("nakshatra_name", "Nakshatra")),
            "detail": (
                f"{nakshatra.get('quality', 'unmarked')} quality. "
                f"Pada {nakshatra.get('pada', '—')}."
            ),
        })

    for body in list(snapshot.get("objects") or snapshot.get("bodies") or []):
        if body.get("retrograde"):
            threads.append({
                "kind": "retrograde",
                "title": f"{body.get('name', body.get('id'))} retrograde",
                "detail": (
                    f"In {body.get('sign', 'current sign')} at {body.get('degree', '—')}°."
                ),
            })

    for signal in list(snapshot.get("signal_field") or [])[:4]:
        threads.append({
            "kind": "signal",
            "title": str(signal.get("name", signal.get("key", "Signal"))),
            "detail": str(signal.get("description", signal.get("label", ""))),
        })

    return threads[:12]


def _compact_engine_outputs(
    engine_outputs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for engine in engine_outputs[:12]:
        compact.append({
            "engine_id": engine.get("engine_id"),
            "engine_name": engine.get("engine_name"),
            "family": engine.get("family"),
            "tradition_frame": engine.get("tradition_frame"),
            "confidence": engine.get("confidence"),
            "horizon": engine.get("horizon"),
            "reading": engine.get("reading"),
            "prediction": engine.get("prediction"),
            "claims": engine.get("claims", [])[:3],
            "rationale": engine.get("rationale", [])[:4],
            "contradictions": engine.get("contradictions", [])[:4],
            "feature_trace": {
                "top_factors": list(
                    (engine.get("feature_trace") or {}).get("top_factors", [])
                )[:5],
                "dominant_sign": (engine.get("feature_trace") or {}).get("dominant_sign"),
                "dominant_element": (engine.get("feature_trace") or {}).get("dominant_element"),
                "retrograde_count": (engine.get("feature_trace") or {}).get("retrograde_count"),
            },
        })
    return compact


def _classify_prediction_scoreability(
    target_symbols: list[str],
) -> tuple[str, list[dict[str, Any]]]:
    from api.dependencies import get_db_engine

    normalized_symbols = [str(symbol).upper() for symbol in target_symbols if str(symbol).strip()]
    if not normalized_symbols:
        return "liquid_market", []

    contract_by_symbol = scoreable_universe_by_symbol()
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            enriched = enrich_astrogrid_scoreable_universe(conn)
        contract_by_symbol = {item["symbol"]: item for item in enriched}
    except Exception as exc:
        log.debug("AstroGrid scoreability contract fallback in effect: {e}", e=str(exc))

    target_statuses = []
    unknown_present = False
    for symbol in normalized_symbols:
        item = contract_by_symbol.get(symbol)
        if not item:
            unknown_present = True
            target_statuses.append({
                "symbol": symbol,
                "status": "unscored",
                "scoreable_now": False,
                "reason_if_not": "symbol is outside the current AstroGrid scoreable universe",
            })
            continue
        status = str(item.get("status") or "unknown")
        scoreable_now = bool(item.get("scoreable_now")) if "scoreable_now" in item else None
        if status == "unknown":
            unknown_present = True
        target_statuses.append({
            "symbol": symbol,
            "status": status,
            "scoreable_now": scoreable_now,
            "reason_if_not": item.get("reason_if_not"),
        })

    if unknown_present:
        return "liquid_market", target_statuses
    if target_statuses and all(item["status"] == "scoreable_now" for item in target_statuses):
        return "liquid_market", target_statuses
    return "unscored_experimental", target_statuses


def _parse_json_response(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    txt = raw.strip()
    if not txt:
        return None

    try:
        return json.loads(txt)
    except Exception:
        pass

    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", txt, flags=re.S)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except Exception:
            pass

    start = txt.find("{")
    end = txt.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(txt[start:end + 1])
        except Exception:
            return None
    return None


def _fallback_interpretation(req: AstrogridInterpretRequest) -> dict[str, Any]:
    snapshot = req.snapshot or {}
    seer = req.seer or {}
    engine_outputs = req.engine_outputs or []
    threads = list(req.threads or _top_snapshot_threads(snapshot))
    support = [
        engine.get("engine_id")
        for engine in engine_outputs
        if float(engine.get("confidence", 0) or 0) >= 0.55
    ][:5]
    warnings = []
    for engine in engine_outputs:
        warnings.extend(list(engine.get("contradictions") or []))
    warnings = list(dict.fromkeys(warnings))[:5]

    return {
        "summary": seer.get("reading") or "Signals are mixed; use the explicit threads.",
        "seer": {
            "reading": seer.get("reading") or "No interpreted reading.",
            "prediction": seer.get("prediction") or "No interpreted prediction.",
            "why": list((seer.get("key_factors") or []))[:5],
            "warnings": warnings,
        },
        "threads": threads,
        "engine_notes": [
            {
                "engine_id": engine.get("engine_id"),
                "rewrite": engine.get("prediction") or engine.get("reading"),
                "basis": list(
                    (engine.get("feature_trace") or {}).get("top_factors", [])
                )[:4],
            }
            for engine in engine_outputs[:6]
        ],
        "tone_notes": [
            "One sentence, one claim, one basis.",
            "Plain action first. Mystic framing second.",
            "No devotional language. No life advice. No identity-targeting content.",
        ],
        "used_llm": False,
        "backend": "fallback",
        "model": None,
    }


def _build_interpret_messages(req: AstrogridInterpretRequest) -> list[dict[str, str]]:
    snapshot = req.snapshot or {}
    engine_outputs = _compact_engine_outputs(req.engine_outputs or [])
    threads = list(req.threads or _top_snapshot_threads(snapshot))
    payload = {
        "question": req.question,
        "mode": req.mode,
        "lens_ids": [_public_lens_label(lens_id) for lens_id in req.lens_ids],
        "persona_id": req.persona_id,
        "seer": req.seer,
        "threads": threads,
        "engine_outputs": engine_outputs,
        "signal_field": list(snapshot.get("signal_field") or [])[:8],
        "events": list(snapshot.get("events") or [])[:8],
        "lunar": snapshot.get("lunar"),
        "nakshatra": snapshot.get("nakshatra"),
        "grid": snapshot.get("grid"),
    }

    system = (
        "You are AstroGrid's interpretation layer. "
        "AstroGrid is a market prediction product, not a spiritual counselor. "
        "Work only from the supplied deterministic sky state, lens outputs, and GRID overlays. "
        "Do not invent occult mechanics that are not present in the data. "
        "Use terse analytical language. One sentence, one claim, one basis. "
        "Lead with plain action, then minimal mystic framing. "
        "Avoid ceremonial filler, generic mystic nouns, and inflated certainty. "
        "Do not write devotional guidance, religious instruction, therapy, lifestyle advice, "
        "or identity-targeting/slur content. "
        "Treat traditions as analytical lenses only. "
        "If evidence is mixed, say it is mixed and name the competing threads. "
        "Return strict JSON with keys: summary, seer, threads, engine_notes, tone_notes. "
        "seer must contain reading, prediction, why, warnings. "
        "threads must be an array of objects with title, detail, lenses, confidence. "
        "engine_notes must be an array of objects with engine_id, rewrite, basis. "
        "tone_notes must be a short array of strings."
    )
    user = (
        "Interpret this AstroGrid state. "
        "Be more granular than the seed engine text. "
        "Find the strongest threads, even if some are speculative; "
        "label speculative leaps clearly. "
        "Keep the atmosphere minimal and let the evidence carry the reading. "
        "Prefer explicit bias, window, trigger, invalidation, trade, and risk framing "
        "whenever the data supports it.\n\n"
        f"{json.dumps(payload, ensure_ascii=True)}"
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _get_llm_client() -> Any:
    try:
        from ollama.client import get_client
        return get_client()
    except Exception:
        return None


def _compact_objects_for_prompt(snapshot: dict[str, Any]) -> str:
    objects = snapshot.get("objects") or snapshot.get("bodies") or []
    ranked = sorted(
        [obj for obj in objects if isinstance(obj, dict)],
        key=lambda obj: obj.get("visual_priority", 0),
        reverse=True,
    )[:10]
    lines: list[str] = []
    for obj in ranked:
        lon = obj.get("longitude")
        sign = obj.get("sign")
        degree = obj.get("degree")
        retro = " retrograde" if obj.get("retrograde") else ""
        lines.append(
            f"- {obj.get('name', obj.get('id', 'body'))}: "
            f"{sign or '?'} {degree if degree is not None else '?'} "
            f"(lon {lon if lon is not None else '?'}){retro}"
        )
    return "\n".join(lines) or "- no body data"


def _compact_aspects_for_prompt(snapshot: dict[str, Any]) -> str:
    aspects = snapshot.get("aspects") or []
    ranked = sorted(
        [aspect for aspect in aspects if isinstance(aspect, dict)],
        key=lambda aspect: float(aspect.get("orb_used") or aspect.get("orb") or 99),
    )[:10]
    lines: list[str] = []
    for aspect in ranked:
        lines.append(
            f"- {aspect.get('planet1', '?')} {aspect.get('aspect_type', '?')} "
            f"{aspect.get('planet2', '?')}; "
            f"orb {aspect.get('orb_used', aspect.get('orb', '?'))}; "
            f"{'applying' if aspect.get('applying') else 'separating'}"
        )
    return "\n".join(lines) or "- no aspect data"


def _compact_engine_runs_for_prompt(engine_runs: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for run in engine_runs[:8]:
        claims = run.get("claims") or []
        claim_bits = "; ".join(
            f"{claim.get('topic')}: {claim.get('statement')}"
            for claim in claims[:3]
            if isinstance(claim, dict)
        )
        lines.append(
            f"- {run.get('engine_name', run.get('engine_id', 'engine'))} "
            f"[{run.get('family', '?')}] {run.get('direction_label', '?')} "
            f"conf={run.get('confidence', '?')} horizon={run.get('horizon', '?')}\n"
            f"  doctrine={run.get('doctrine', '')}\n"
            f"  prediction={run.get('prediction', '')}\n"
            f"  claims={claim_bits or 'none'}"
        )
    return "\n".join(lines) or "- no engine runs"


def _compact_threads_for_prompt(threads: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for thread in threads[:12]:
        lines.append(
            f"- [{thread.get('kind', 'thread')}] {thread.get('title', 'thread')}: "
            f"{thread.get('detail', '')} "
            f"(relevance {thread.get('relevance', '?')})"
        )
    return "\n".join(lines) or "- no extracted threads"


def _compute_full_ephemeris(d: date) -> dict[str, Any]:
    """Compute all 23 celestial features for a single date."""
    phase = _lunar_phase(d)
    illum = _illumination(phase)

    jup_lon = _geo_longitude("jupiter", d)
    sat_lon = _geo_longitude("saturn", d)

    moon_lon = _moon_sidereal_longitude(d)
    nak_idx = _nakshatra_from_longitude(moon_lon)
    rahu_lon = _rahu_longitude(d)

    return {
        "lunar_phase": round(phase, 6),
        "lunar_illumination": round(illum, 4),
        "days_to_new_moon": round(_days_to_phase(d, 0.0), 2),
        "days_to_full_moon": round(_days_to_phase(d, 0.5), 2),
        "lunar_eclipse_proximity": _nearest_eclipse(d, _LUNAR_ECLIPSES),
        "solar_eclipse_proximity": _nearest_eclipse(d, _SOLAR_ECLIPSES),
        "mercury_retrograde": 1.0 if _is_mercury_retrograde(d) else 0.0,
        "jupiter_saturn_angle": round(_angular_separation(jup_lon, sat_lon), 4),
        "mars_volatility_index": round(_mars_volatility_index(d), 6),
        "planetary_stress_index": float(_hard_aspect_count(d)),
        "venus_cycle_phase": round(_venus_synodic_phase(d), 6),
        "solar_cycle_phase": round(_solar_cycle_phase(d), 6),
        "nakshatra_index": float(nak_idx),
        "nakshatra_name": _NAKSHATRA_NAMES[nak_idx],
        "nakshatra_quality": float(_NAKSHATRA_QUALITY[nak_idx]),
        "tithi": float(_tithi(d)),
        "rahu_ketu_axis": round(rahu_lon, 4),
        "dasha_cycle_phase": round(_dasha_cycle_phase(d), 6),
        "chinese_zodiac_year": float(_zodiac_index(d)),
        "chinese_zodiac_animal": _ZODIAC_ANIMALS[_zodiac_index(d)],
        "chinese_element": float(_element_index(d)),
        "chinese_element_name": _ELEMENTS[_element_index(d)],
        "chinese_yin_yang": float(_yin_yang(d)),
        "feng_shui_flying_star": float(_flying_star(d)),
        "chinese_lunar_month": float(_chinese_lunar_month(d)),
        "iching_hexagram_of_day": float(_iching_hexagram(d)),
    }


def _interpret_kp(kp: float | None) -> str:
    if kp is None:
        return "No data"
    if kp < 2:
        return "Quiet"
    if kp < 4:
        return "Unsettled"
    if kp < 5:
        return "Active"
    if kp < 6:
        return "Minor storm"
    if kp < 7:
        return "Moderate storm"
    if kp < 8:
        return "Strong storm"
    return "Severe storm"


def _get_latest_resolved(
    engine: Any, feature_name: str
) -> tuple[float | None, str | None]:
    """Get the latest value and date for a feature from resolved_series."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT rs.value, rs.obs_date "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON rs.feature_id = fr.id "
                    "WHERE fr.name = :name "
                    "ORDER BY rs.obs_date DESC LIMIT 1"
                ),
                {"name": feature_name},
            ).fetchone()
            if row:
                return float(row[0]), str(row[1])
    except Exception:
        pass
    return None, None


def _signed_pct_change(
    current: float | None, baseline: float | None
) -> float | None:
    if current is None or baseline is None or baseline == 0:
        return None
    return round(((current - baseline) / baseline) * 100.0, 2)


def _find_history_baseline(
    history: list[tuple[date, float]],
    reference_date: date,
    lookback_days: int,
) -> float | None:
    cutoff = reference_date - timedelta(days=lookback_days)
    for obs_date, value in reversed(history):
        if obs_date <= cutoff:
            return value
    return history[0][1] if history else None


def _momentum_score(
    change_1d: float | None,
    change_5d: float | None,
    change_20d: float | None,
) -> float:
    weighted = (
        (change_1d or 0.0) * 0.2
        + (change_5d or 0.0) * 0.35
        + (change_20d or 0.0) * 0.45
    )
    return round(math.tanh(weighted / 12.0), 4)


def _momentum_bias(score: float) -> str:
    if score >= 0.18:
        return "press"
    if score <= -0.18:
        return "hedge"
    return "wait"


def _momentum_trend(
    change_5d: float | None,
    change_20d: float | None,
) -> str:
    if change_5d is None and change_20d is None:
        return "unresolved"
    if (change_5d or 0) >= 1.0 and (change_20d or 0) >= 3.0:
        return "uptrend"
    if (change_5d or 0) <= -1.0 and (change_20d or 0) <= -3.0:
        return "downtrend"
    if abs(change_5d or 0) <= 0.75 and abs(change_20d or 0) <= 1.5:
        return "range"
    return "mixed"


def _scorecard_confidence(
    history_points: int,
    latest_date: date | None,
    has_live_price: bool,
) -> float:
    stale_penalty = 0.0
    if latest_date and (date.today() - latest_date).days > 3:
        stale_penalty = 0.18
    live_boost = 0.08 if has_live_price else 0.0
    history_boost = min(history_points / 60.0, 0.35)
    return round(max(0.15, min(0.92, 0.28 + history_boost + live_boost - stale_penalty)), 4)


def _resolve_scorecard_feature(
    conn: Any, asset: dict[str, Any]
) -> tuple[str | None, list[str]]:
    from api.routers.watchlist import _resolve_feature_names

    candidates = [str(asset.get("price_feature") or "").strip()]
    candidates.extend(_resolve_feature_names(asset["lookup_ticker"]))
    candidates = [
        candidate
        for idx, candidate in enumerate(candidates)
        if candidate and candidate not in candidates[:idx]
    ]
    rows = conn.execute(
        text(
            "SELECT fr.name, MAX(rs.obs_date) AS last_date, COUNT(rs.obs_date) AS row_count "
            "FROM feature_registry fr "
            "LEFT JOIN resolved_series rs ON rs.feature_id = fr.id "
            "WHERE fr.name = ANY(:names) "
            "GROUP BY fr.name "
            "ORDER BY COUNT(rs.obs_date) DESC, MAX(rs.obs_date) DESC NULLS LAST, fr.name"
        ),
        {"names": candidates},
    ).fetchall()

    canonical = str(asset.get("price_feature") or "")
    canonical_row = next((row for row in rows if row[0] == canonical), None)
    if canonical_row and (canonical_row[2] or 0) > 0:
        return canonical, candidates

    best = next((row[0] for row in rows if (row[2] or 0) > 0), None)
    return best, candidates


def _load_scorecard_history(
    conn: Any,
    feature_name: str,
    start_date: date,
) -> list[tuple[date, float]]:
    rows = conn.execute(
        text(
            "SELECT rs.obs_date, rs.value "
            "FROM resolved_series rs "
            "JOIN feature_registry fr ON fr.id = rs.feature_id "
            "WHERE fr.name = :name AND rs.obs_date >= :start_date "
            "ORDER BY rs.obs_date"
        ),
        {"name": feature_name, "start_date": start_date},
    ).fetchall()
    return [
        (row[0], float(row[1]))
        for row in rows
        if row[0] is not None and row[1] is not None
    ]


def _build_scorecard_item(
    asset: dict[str, str],
    feature_name: str | None,
    candidate_features: list[str],
    history: list[tuple[date, float]],
    live_quote: dict[str, Any] | None,
) -> dict[str, Any]:
    latest_date = history[-1][0] if history else None
    latest_db_value = history[-1][1] if history else None
    latest_value = (
        float(live_quote["price"])
        if live_quote and live_quote.get("price") is not None
        else latest_db_value
    )
    reference_date = (
        date.today() if live_quote and latest_value is not None else latest_date
    )

    change_1d = None
    if live_quote and live_quote.get("pct_1d") is not None:
        change_1d = round(float(live_quote["pct_1d"]) * 100.0, 2)
    elif reference_date and latest_value is not None:
        change_1d = _signed_pct_change(
            latest_value, _find_history_baseline(history, reference_date, 1)
        )

    change_5d = None
    if live_quote and live_quote.get("pct_1w") is not None:
        change_5d = round(float(live_quote["pct_1w"]) * 100.0, 2)
    elif reference_date and latest_value is not None:
        change_5d = _signed_pct_change(
            latest_value, _find_history_baseline(history, reference_date, 5)
        )

    change_20d = None
    if reference_date and latest_value is not None:
        change_20d = _signed_pct_change(
            latest_value, _find_history_baseline(history, reference_date, 20)
        )

    momentum = (
        _momentum_score(change_1d, change_5d, change_20d)
        if latest_value is not None
        else 0.0
    )
    trend = _momentum_trend(change_5d, change_20d)
    source_parts: list[str] = []
    if feature_name and history:
        source_parts.append("resolved_series")
    if live_quote:
        source_parts.append("watchlist_live")

    return {
        "symbol": asset["symbol"],
        "label": asset["label"],
        "group": asset["group"],
        "asset_class": asset["asset_class"],
        "lookup_ticker": asset["lookup_ticker"],
        "feature_name": feature_name,
        "price_feature": asset.get("price_feature"),
        "benchmark_symbol": asset.get("benchmark_symbol"),
        "candidate_features": candidate_features,
        "latest": round(latest_value, 4) if latest_value is not None else None,
        "latest_date": str(latest_date) if latest_date else None,
        "live_price": (
            round(float(live_quote["price"]), 4)
            if live_quote and live_quote.get("price") is not None
            else None
        ),
        "history_points": len(history),
        "change_1d_pct": change_1d,
        "change_5d_pct": change_5d,
        "change_20d_pct": change_20d,
        "momentum_score": momentum,
        "bias": _momentum_bias(momentum) if latest_value is not None else "wait",
        "trend": trend,
        "confidence": _scorecard_confidence(len(history), latest_date, bool(live_quote)),
        "coverage": {
            "has_feature": bool(feature_name),
            "has_history": bool(history),
            "has_live_price": bool(live_quote),
        },
        "scoreable_now": bool(asset.get("scoreable_now")),
        "status": asset.get("status", "unscored"),
        "reason_if_not": asset.get("reason_if_not"),
        "history_points_contract": asset.get("history_points"),
        "source": "+".join(source_parts) if source_parts else "unresolved",
    }


def _group_scorecard_items(
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        groups.setdefault(item["group"], []).append(item)

    summary: list[dict[str, Any]] = []
    for group_name, group_items in groups.items():
        scored = [item for item in group_items if item.get("latest") is not None]
        scores = [float(item["momentum_score"]) for item in scored]
        composite = round(sum(scores) / len(scores), 4) if scores else 0.0
        strongest = max(scored, key=lambda item: float(item["momentum_score"]), default=None)
        weakest = min(scored, key=lambda item: float(item["momentum_score"]), default=None)
        summary.append({
            "id": group_name,
            "label": group_name.title(),
            "symbols": [item["symbol"] for item in group_items],
            "available": len(scored),
            "total": len(group_items),
            "composite_score": composite,
            "bias": _momentum_bias(composite),
            "strongest": strongest["symbol"] if strongest else None,
            "weakest": weakest["symbol"] if weakest else None,
        })
    return sorted(summary, key=lambda item: item["id"])


def _build_scorecard_summary(
    items: list[dict[str, Any]],
    groups: list[dict[str, Any]],
    evaluation: dict[str, Any],
) -> dict[str, Any]:
    covered = [item for item in items if item.get("latest") is not None]
    leaders = sorted(covered, key=lambda item: float(item["momentum_score"]), reverse=True)
    laggards = sorted(covered, key=lambda item: float(item["momentum_score"]))
    composite_score = (
        round(
            sum(float(item["momentum_score"]) for item in covered) / len(covered),
            4,
        )
        if covered
        else 0.0
    )
    return {
        "total": len(items),
        "available": len(covered),
        "coverage_ratio": round(len(covered) / len(items), 4) if items else 0.0,
        "composite_score": composite_score,
        "bias": _momentum_bias(composite_score),
        "leaders": [item["symbol"] for item in leaders[:3]],
        "laggards": [item["symbol"] for item in laggards[:3]],
        "crypto_score": next(
            (group["composite_score"] for group in groups if group["id"] == "crypto"), 0.0
        ),
        "macro_score": next(
            (group["composite_score"] for group in groups if group["id"] == "macro"), 0.0
        ),
        "oracle_accuracy": evaluation["overall"]["accuracy"],
    }


def _build_scorecard_evaluation(
    engine: Any,
    universe: list[dict[str, str]],
) -> dict[str, Any]:
    lookup_to_symbol = {asset["lookup_ticker"]: asset["symbol"] for asset in universe}
    rollup = build_oracle_ticker_rollup(
        engine,
        tickers=list(lookup_to_symbol),
        ticker_aliases=lookup_to_symbol,
        include_calibration=True,
    )
    return {
        "overall": rollup["overall"],
        "by_symbol": [
            {
                "symbol": item["ticker"],
                "lookup_ticker": item["lookup_ticker"],
                "total": item["total"],
                "scored": item["scored"],
                "hits": item["hits"],
                "misses": item["misses"],
                "partials": item["partials"],
                "pending": item["pending"],
                "accuracy": item["accuracy"],
                "avg_pnl": item["avg_pnl"],
                "total_pnl": item["total_pnl"],
                "calibration": item.get("calibration"),
            }
            for item in rollup["by_ticker"]
        ],
    }
