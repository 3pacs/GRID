"""AstroGrid API — expanded celestial intelligence endpoints.

Provides ephemeris computation, celestial-market correlations, event timelines,
retrograde tracking, eclipse calendars, nakshatra stats, lunar calendars,
solar weather, and comparative date analysis.
"""

from __future__ import annotations

import calendar
import json
import math
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_astrogrid_store, get_db_engine
from analysis.ephemeris import (
    Ephemeris as AstroEphemeris,
    OBLIQUITY_J2000 as EPHEMERIS_OBLIQUITY_J2000,
    ZODIAC_SIGNS as EPHEMERIS_ZODIAC_SIGNS,
    _ecliptic_to_equatorial as _ephemeris_ecliptic_to_equatorial,
    _normalize_angle as _ephemeris_normalize_angle,
    get_ephemeris as build_astrological_ephemeris,
)

# ── Celestial computation imports ──────────────────────────────────────
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

router = APIRouter(prefix="/api/v1/astrogrid", tags=["astrogrid"])


# ── Pydantic models ───────────────────────────────────────────────────

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


# ── Helpers ────────────────────────────────────────────────────────────

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
        raise ValueError(f"Invalid date format: {value}. Use YYYY-MM-DD or ISO datetime.") from exc


def _public_lens_label(value: str) -> str:
    key = str(value or "").strip().lower()
    return _PUBLIC_LENS_LABELS.get(key, key or "unknown lens")


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


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
    overlay = req.market_overlay_snapshot or {}
    scorecard = overlay.get("scorecard") if isinstance(overlay, dict) else {}
    symbols: list[str] = []
    if isinstance(scorecard, dict):
        for bucket in ("leaders", "laggards"):
            for item in list(scorecard.get(bucket) or [])[:3]:
                symbol = item.get("symbol") if isinstance(item, dict) else None
                if symbol:
                    symbols.append(str(symbol).upper())
    return list(dict.fromkeys(symbols))[:12]


def _grid_driver_summary(market_overlay: dict[str, Any]) -> tuple[list[str], str]:
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


def _mystical_driver_summary(req: AstrogridPredictionRequest) -> tuple[list[str], str]:
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
        drivers.append(f"aspect:{aspects[0].get('planet1')} {aspects[0].get('aspect_type')} {aspects[0].get('planet2')}")
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
        f"Pending {horizon} read on {', '.join(target_symbols) if target_symbols else req.target_universe}: "
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


def _compute_sun_position(ephemeris: AstroEphemeris, target: date) -> dict[str, Any]:
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


def _body_longitude(ephemeris: AstroEphemeris, body_name: str, target: date) -> float | None:
    if body_name == "Sun":
        return float(_compute_sun_position(ephemeris, target)["geocentric_longitude"])
    try:
        return float(ephemeris.compute_position(body_name, target)["geocentric_longitude"])
    except Exception:
        return None


def _daily_motion(ephemeris: AstroEphemeris, body_name: str, target: date) -> float | None:
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
        "Sun",
        "Moon",
        "Mercury",
        "Venus",
        "Mars",
        "Jupiter",
        "Saturn",
        "Uranus",
        "Neptune",
        "Pluto",
        "Rahu",
        "Ketu",
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


def _get_market_regime(engine, target: date) -> tuple[str | None, float | None]:
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
            "description": f"Moon holds {void_of_course.get('current_sign', 'its sign')} with no major aspect before exit.",
            "days_until": 0,
        })

    if nakshatra.get("nakshatra_name"):
        events.append({
            "id": f"nak-{today_iso}",
            "type": "nakshatra",
            "name": f"Nakshatra {nakshatra['nakshatra_name']}",
            "date": today_iso,
            "description": f"{nakshatra.get('quality', 'unmarked')} quality. Pada {nakshatra.get('pada', '—')}.",
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

    pressure = stress + retrogrades + (1 if void_of_course else 0) + (1 if kp_value is not None and kp_value >= 5 else 0)
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
    name = type(client).__name__.lower()
    if "openai" in name:
        return "openai"
    if "ollama" in name:
        return "ollama"
    if "llama" in name:
        return "llamacpp"
    return name


def _top_snapshot_threads(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    threads: list[dict[str, Any]] = []
    aspects = list(snapshot.get("aspects") or [])
    for aspect in sorted(aspects, key=lambda item: float(item.get("orb_used", 999)))[:5]:
        threads.append({
            "kind": "aspect",
            "title": f"{aspect.get('planet1')} {aspect.get('aspect_type')} {aspect.get('planet2')}",
            "detail": f"Orb {float(aspect.get('orb_used', 0)):.2f}°. {'Applying' if aspect.get('applying') else 'Separating'}.",
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
            "detail": f"{nakshatra.get('quality', 'unmarked')} quality. Pada {nakshatra.get('pada', '—')}.",
        })

    for body in list(snapshot.get("objects") or snapshot.get("bodies") or []):
        if body.get("retrograde"):
            threads.append({
                "kind": "retrograde",
                "title": f"{body.get('name', body.get('id'))} retrograde",
                "detail": f"In {body.get('sign', 'current sign')} at {body.get('degree', '—')}°.",
            })

    for signal in list(snapshot.get("signal_field") or [])[:4]:
        threads.append({
            "kind": "signal",
            "title": str(signal.get("name", signal.get("key", "Signal"))),
            "detail": str(signal.get("description", signal.get("label", ""))),
        })

    return threads[:12]


def _compact_engine_outputs(engine_outputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
                "top_factors": list((engine.get("feature_trace") or {}).get("top_factors", []))[:5],
                "dominant_sign": (engine.get("feature_trace") or {}).get("dominant_sign"),
                "dominant_element": (engine.get("feature_trace") or {}).get("dominant_element"),
                "retrograde_count": (engine.get("feature_trace") or {}).get("retrograde_count"),
            },
        })
    return compact


def _classify_prediction_scoreability(target_symbols: list[str]) -> tuple[str, list[dict[str, Any]]]:
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
    text = raw.strip()
    if not text:
        return None

    try:
        return json.loads(text)
    except Exception:
        pass

    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.S)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except Exception:
            pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            return None
    return None


def _fallback_interpretation(req: AstrogridInterpretRequest) -> dict[str, Any]:
    snapshot = req.snapshot or {}
    seer = req.seer or {}
    engine_outputs = req.engine_outputs or []
    threads = list(req.threads or _top_snapshot_threads(snapshot))
    support = [engine.get("engine_id") for engine in engine_outputs if float(engine.get("confidence", 0) or 0) >= 0.55][:5]
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
                "basis": list((engine.get("feature_trace") or {}).get("top_factors", []))[:4],
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
        "Do not write devotional guidance, religious instruction, therapy, lifestyle advice, or identity-targeting/slur content. "
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
        "Find the strongest threads, even if some are speculative; label speculative leaps clearly. "
        "Keep the atmosphere minimal and let the evidence carry the reading. "
        "Prefer explicit bias, window, trigger, invalidation, trade, and risk framing whenever the data supports it.\n\n"
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
            f"- {aspect.get('planet1', '?')} {aspect.get('aspect_type', '?')} {aspect.get('planet2', '?')}; "
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


def _fallback_prophecy_text(req: ProphecyInterpretRequest) -> str:
    seer = req.seer or {}
    snapshot = req.snapshot or {}
    lunar = snapshot.get("lunar") or {}
    nakshatra = snapshot.get("nakshatra") or {}
    signals = snapshot.get("signals") or {}
    engines = req.engine_runs or []

    lines = [
        "THESIS",
        seer.get("reading") or "No synthesized reading available.",
        "",
        "THREADS",
        f"- Lunar phase: {lunar.get('phase_name', 'unknown')} at {lunar.get('illumination', '?')}% illumination.",
        f"- Nakshatra: {nakshatra.get('nakshatra_name', 'unknown')} ({nakshatra.get('quality', 'unmarked')}).",
        f"- Hard aspects: {signals.get('planetaryStress', '?')}; retrogrades: {signals.get('retrogradeCount', '?')}.",
        "",
        "FORECAST",
        seer.get("prediction") or "No forecast available.",
        "",
        "INVALIDATION",
        "- This reading fails if the next logged branch contradicts the current engine claims.",
    ]

    if engines:
        lines.extend(["", "ACTIVE LENSES"])
        for engine in engines[:5]:
            lines.append(
                f"- {engine.get('engine_name', engine.get('engine_id', 'engine'))}: "
                f"{engine.get('prediction', engine.get('reading', 'no output'))}"
            )

    if req.threads:
        lines.extend(["", "THREAD MAP"])
        for thread in req.threads[:8]:
            lines.append(f"- {thread.get('title', 'thread')}: {thread.get('detail', '')}")

    return "\n".join(lines)


def _build_prophecy_messages(req: ProphecyInterpretRequest) -> list[dict[str, str]]:
    snapshot = req.snapshot or {}
    seer = req.seer or {}
    signals = snapshot.get("signals") or {}
    lunar = snapshot.get("lunar") or {}
    nakshatra = snapshot.get("nakshatra") or {}
    events = snapshot.get("events") or []
    event_lines = [
        f"- {event.get('name', event.get('type', 'event'))}: {event.get('description', '')}"
        for event in events[:6]
        if isinstance(event, dict)
    ]

    system = (
        "You are AstroGrid's interpretation layer. "
        "You receive computed celestial state plus deterministic engine runs. "
        "Write a granular interpretation that is concrete, auditable, and unsentimental. "
        "Do not write generic mystical filler. "
        "Every claim must anchor to supplied bodies, aspects, lunar state, nakshatra, signals, or engine outputs. "
        "It is acceptable to stretch a thread, but the thread must be legible. "
        "If evidence is weak or contradictory, say so plainly. "
        "Return exactly these labeled sections: THESIS, THREADS, FORECAST, INVALIDATION."
    )

    user = (
        f"Question: {req.question or 'What is the present reading?'}\n"
        f"Lens mode: {req.mode}\n"
        f"Active lenses: {', '.join(req.active_lenses) or 'none'}\n"
        f"Persona: {req.persona_id}\n\n"
        f"SEER\n"
        f"- reading: {seer.get('reading', '')}\n"
        f"- prediction: {seer.get('prediction', '')}\n"
        f"- confidence: {seer.get('confidence', '')} ({seer.get('confidence_band', '')})\n"
        f"- key factors: {', '.join(seer.get('key_factors', []) or [])}\n"
        f"- conflicts: {seer.get('conflicts', []) or []}\n\n"
        f"LUNAR\n"
        f"- phase: {lunar.get('phase_name', '')}\n"
        f"- illumination: {lunar.get('illumination', '')}\n"
        f"- days_to_new: {lunar.get('days_to_new', '')}\n"
        f"- days_to_full: {lunar.get('days_to_full', '')}\n\n"
        f"NAKSHATRA\n"
        f"- name: {nakshatra.get('nakshatra_name', '')}\n"
        f"- quality: {nakshatra.get('quality', '')}\n"
        f"- pada: {nakshatra.get('pada', '')}\n\n"
        f"SIGNALS\n"
        f"- hard_aspects: {signals.get('planetaryStress', '')}\n"
        f"- soft_aspects: {signals.get('softAspectCount', '')}\n"
        f"- retrogrades: {signals.get('retrogradeCount', '')}\n"
        f"- dominant_element: {signals.get('dominantElement', '')}\n"
        f"- market_regime: {signals.get('marketRegime', '')}\n"
        f"- market_bias: {signals.get('marketRegimeBias', '')}\n\n"
        f"OBJECTS\n{_compact_objects_for_prompt(snapshot)}\n\n"
        f"ASPECTS\n{_compact_aspects_for_prompt(snapshot)}\n\n"
        f"EVENTS\n{chr(10).join(event_lines) or '- no events'}\n\n"
        f"THREADS\n{_compact_threads_for_prompt(req.threads)}\n\n"
        f"ENGINE RUNS\n{_compact_engine_runs_for_prompt(req.engine_runs)}\n\n"
        "Constraints:\n"
        "- Name at least six concrete threads.\n"
        "- At least two threads must cite exact bodies or aspects.\n"
        "- Forecast must distinguish what is actionable now from what is only atmospheric.\n"
        "- Invalidation must say what would make this reading wrong.\n"
        "- Tone must be clean, sharp, and not embarrassed by uncertainty.\n"
    )

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _compute_full_ephemeris(d: date) -> dict[str, Any]:
    """Compute all 23 celestial features for a single date."""
    # Lunar
    phase = _lunar_phase(d)
    illum = _illumination(phase)

    # Planetary
    jup_lon = _geo_longitude("jupiter", d)
    sat_lon = _geo_longitude("saturn", d)

    # Vedic
    moon_lon = _moon_sidereal_longitude(d)
    nak_idx = _nakshatra_from_longitude(moon_lon)
    rahu_lon = _rahu_longitude(d)

    return {
        # Lunar
        "lunar_phase": round(phase, 6),
        "lunar_illumination": round(illum, 4),
        "days_to_new_moon": round(_days_to_phase(d, 0.0), 2),
        "days_to_full_moon": round(_days_to_phase(d, 0.5), 2),
        "lunar_eclipse_proximity": _nearest_eclipse(d, _LUNAR_ECLIPSES),
        "solar_eclipse_proximity": _nearest_eclipse(d, _SOLAR_ECLIPSES),
        # Planetary
        "mercury_retrograde": 1.0 if _is_mercury_retrograde(d) else 0.0,
        "jupiter_saturn_angle": round(_angular_separation(jup_lon, sat_lon), 4),
        "mars_volatility_index": round(_mars_volatility_index(d), 6),
        "planetary_stress_index": float(_hard_aspect_count(d)),
        "venus_cycle_phase": round(_venus_synodic_phase(d), 6),
        # Solar (deterministic components only)
        "solar_cycle_phase": round(_solar_cycle_phase(d), 6),
        # Vedic
        "nakshatra_index": float(nak_idx),
        "nakshatra_name": _NAKSHATRA_NAMES[nak_idx],
        "nakshatra_quality": float(_NAKSHATRA_QUALITY[nak_idx]),
        "tithi": float(_tithi(d)),
        "rahu_ketu_axis": round(rahu_lon, 4),
        "dasha_cycle_phase": round(_dasha_cycle_phase(d), 6),
        # Chinese
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


def _get_latest_resolved(engine, feature_name: str) -> tuple[float | None, str | None]:
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


def _signed_pct_change(current: float | None, baseline: float | None) -> float | None:
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


def _resolve_scorecard_feature(conn, asset: dict[str, Any]) -> tuple[str | None, list[str]]:
    from api.routers.watchlist import _resolve_feature_names

    candidates = [str(asset.get("price_feature") or "").strip()]
    candidates.extend(_resolve_feature_names(asset["lookup_ticker"]))
    candidates = [candidate for idx, candidate in enumerate(candidates) if candidate and candidate not in candidates[:idx]]
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
    conn,
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
    latest_value = float(live_quote["price"]) if live_quote and live_quote.get("price") is not None else latest_db_value
    reference_date = date.today() if live_quote and latest_value is not None else latest_date

    change_1d = None
    if live_quote and live_quote.get("pct_1d") is not None:
        change_1d = round(float(live_quote["pct_1d"]) * 100.0, 2)
    elif reference_date and latest_value is not None:
        change_1d = _signed_pct_change(latest_value, _find_history_baseline(history, reference_date, 1))

    change_5d = None
    if live_quote and live_quote.get("pct_1w") is not None:
        change_5d = round(float(live_quote["pct_1w"]) * 100.0, 2)
    elif reference_date and latest_value is not None:
        change_5d = _signed_pct_change(latest_value, _find_history_baseline(history, reference_date, 5))

    change_20d = None
    if reference_date and latest_value is not None:
        change_20d = _signed_pct_change(latest_value, _find_history_baseline(history, reference_date, 20))

    momentum = _momentum_score(change_1d, change_5d, change_20d) if latest_value is not None else 0.0
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
        "live_price": round(float(live_quote["price"]), 4) if live_quote and live_quote.get("price") is not None else None,
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


def _group_scorecard_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    composite_score = round(
        sum(float(item["momentum_score"]) for item in covered) / len(covered),
        4,
    ) if covered else 0.0
    return {
        "total": len(items),
        "available": len(covered),
        "coverage_ratio": round(len(covered) / len(items), 4) if items else 0.0,
        "composite_score": composite_score,
        "bias": _momentum_bias(composite_score),
        "leaders": [item["symbol"] for item in leaders[:3]],
        "laggards": [item["symbol"] for item in laggards[:3]],
        "crypto_score": next((group["composite_score"] for group in groups if group["id"] == "crypto"), 0.0),
        "macro_score": next((group["composite_score"] for group in groups if group["id"] == "macro"), 0.0),
        "oracle_accuracy": evaluation["overall"]["accuracy"],
    }


def _build_scorecard_evaluation(
    engine,
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


# ── Endpoints ──────────────────────────────────────────────────────────

@router.get("/overview")
async def get_overview(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current state of all celestial systems with interpretations."""
    today = date.today()

    try:
        engine = get_db_engine()
        ephemeris = _compute_full_ephemeris(today)
        phase = ephemeris["lunar_phase"]
        illum = ephemeris["lunar_illumination"]
        phase_name = _phase_name(phase)

        # Active retrogrades
        active_retros: list[dict] = []
        for start, end in _MERCURY_RETROGRADES:
            if start <= today <= end:
                active_retros.append({
                    "planet": "Mercury",
                    "start": str(start),
                    "end": str(end),
                })

        # Solar data from resolved_series (API-sourced)
        kp_val, kp_date = _get_latest_resolved(engine, "geomagnetic_kp_index")
        sunspot_val, _ = _get_latest_resolved(engine, "sunspot_number")
        wind_val, _ = _get_latest_resolved(engine, "solar_wind_speed")

        # Group by category
        categories = {
            "lunar": {
                "moon_phase": phase_name,
                "illumination_pct": round(illum, 1),
                "phase_fraction": round(phase, 4),
                "days_to_new_moon": ephemeris["days_to_new_moon"],
                "days_to_full_moon": ephemeris["days_to_full_moon"],
                "interpretation": f"{phase_name}, {illum:.0f}% illuminated",
            },
            "planetary": {
                "mercury_retrograde": bool(ephemeris["mercury_retrograde"]),
                "jupiter_saturn_angle": ephemeris["jupiter_saturn_angle"],
                "mars_volatility_index": ephemeris["mars_volatility_index"],
                "planetary_stress_index": int(ephemeris["planetary_stress_index"]),
                "venus_cycle_phase": ephemeris["venus_cycle_phase"],
                "active_retrogrades": active_retros,
                "interpretation": (
                    f"Mercury retrograde active since {active_retros[0]['start']}"
                    if active_retros
                    else f"No retrogrades active. {int(ephemeris['planetary_stress_index'])} hard aspects."
                ),
            },
            "solar": {
                "kp_index": kp_val,
                "sunspot_number": sunspot_val,
                "solar_wind_speed_kms": wind_val,
                "solar_cycle_phase": ephemeris["solar_cycle_phase"],
                "geomagnetic_status": _interpret_kp(kp_val),
                "interpretation": (
                    f"Geomagnetic: {_interpret_kp(kp_val)}. "
                    f"Sunspots: {int(sunspot_val) if sunspot_val else 'N/A'}. "
                    f"Solar wind: {wind_val:.0f} km/s" if wind_val else "Solar data limited"
                ),
            },
            "vedic": {
                "nakshatra_index": int(ephemeris["nakshatra_index"]),
                "nakshatra_name": ephemeris["nakshatra_name"],
                "nakshatra_quality": int(ephemeris["nakshatra_quality"]),
                "nakshatra_quality_name": ["Fixed", "Movable", "Dual"][int(ephemeris["nakshatra_quality"])],
                "tithi": int(ephemeris["tithi"]),
                "rahu_ketu_axis": ephemeris["rahu_ketu_axis"],
                "dasha_cycle_phase": ephemeris["dasha_cycle_phase"],
                "interpretation": (
                    f"Nakshatra: {ephemeris['nakshatra_name']} "
                    f"({'Fixed' if ephemeris['nakshatra_quality'] == 0 else 'Movable' if ephemeris['nakshatra_quality'] == 1 else 'Dual'}). "
                    f"Tithi: {int(ephemeris['tithi'])}."
                ),
            },
            "chinese": {
                "zodiac_animal": ephemeris["chinese_zodiac_animal"],
                "element": ephemeris["chinese_element_name"],
                "yin_yang": "Yang" if ephemeris["chinese_yin_yang"] == 0 else "Yin",
                "flying_star": int(ephemeris["feng_shui_flying_star"]),
                "lunar_month": int(ephemeris["chinese_lunar_month"]),
                "iching_hexagram": int(ephemeris["iching_hexagram_of_day"]),
                "interpretation": (
                    f"Year of the {ephemeris['chinese_element_name']} "
                    f"{ephemeris['chinese_zodiac_animal']} "
                    f"({'Yang' if ephemeris['chinese_yin_yang'] == 0 else 'Yin'}). "
                    f"Flying Star Period {int(ephemeris['feng_shui_flying_star'])}."
                ),
            },
        }

        return {
            "as_of": str(today),
            "categories": categories,
            "active_retrogrades": active_retros,
            "current_moon_phase": phase_name,
            "solar_activity_level": _interpret_kp(kp_val),
            "current_nakshatra": ephemeris["nakshatra_name"],
            "chinese_year": (
                f"{ephemeris['chinese_element_name']} "
                f"{ephemeris['chinese_zodiac_animal']}"
            ),
        }

    except Exception as exc:
        log.warning("AstroGrid overview failed: {e}", e=str(exc))
        return {"error": str(exc), "as_of": str(today)}


@router.get("/snapshot")
async def get_snapshot(
    date_str: str | None = Query(alias="date", default=None, description="YYYY-MM-DD or ISO datetime"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Unified AstroGrid state payload for the standalone UI."""
    try:
        target = _parse_snapshot_date(date_str)
    except ValueError as exc:
        return {"error": str(exc)}

    engine = get_db_engine()
    ephemeris = AstroEphemeris()
    full_ephemeris = build_astrological_ephemeris(target)
    objects = _build_objects(target, ephemeris, full_ephemeris)

    solar_features = {
        "geomagnetic_kp_index": None,
        "sunspot_number": None,
        "solar_wind_speed": None,
        "solar_cycle_phase": None,
    }
    for feature_name in solar_features:
        solar_features[feature_name], _ = _get_latest_resolved(engine, feature_name)

    if solar_features["solar_cycle_phase"] is None:
        solar_features["solar_cycle_phase"] = round(_solar_cycle_phase(target), 6)

    market_regime, market_bias = _get_market_regime(engine, target)
    signals, signal_field = _build_signal_field(
        full_ephemeris["lunar_phase"],
        full_ephemeris["nakshatra"],
        full_ephemeris["aspects"],
        objects,
        solar_features,
        market_regime,
        market_bias,
        full_ephemeris["void_of_course"],
    )
    events = _build_snapshot_events(
        target,
        full_ephemeris["lunar_phase"],
        full_ephemeris["nakshatra"],
        full_ephemeris["aspects"],
        full_ephemeris["void_of_course"],
    )
    seer = _build_snapshot_seer(
        full_ephemeris["lunar_phase"],
        full_ephemeris["nakshatra"],
        signals,
        events,
    )

    source_parts = ["analysis.ephemeris"]
    if any(value is not None for value in solar_features.values()):
        source_parts.append("resolved_series")
    if market_regime is not None:
        source_parts.append("regime_history")

    snapshot = {
        "date": str(target),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "+".join(source_parts),
        "objects": objects,
        "bodies": objects,
        "positions": full_ephemeris["positions"],
        "aspects": full_ephemeris["aspects"],
        "lunar": full_ephemeris["lunar_phase"],
        "nakshatra": full_ephemeris["nakshatra"],
        "void_of_course": full_ephemeris["void_of_course"],
        "retrograde_planets": full_ephemeris["retrograde_planets"],
        "summary": full_ephemeris["summary"],
        "signals": signals,
        "signal_field": signal_field,
        "events": events,
        "seer": seer,
        "grid": {
            "market_regime": market_regime,
            "market_regime_bias": market_bias,
            "solar": solar_features,
        },
    }
    try:
        get_astrogrid_store().save_snapshot(snapshot)
    except Exception as exc:
        log.warning("AstroGrid snapshot store unavailable: {e}", e=str(exc))
    return snapshot


@router.get("/scorecard")
async def get_scorecard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Hybrid AstroGrid market scorecard using existing GRID read paths."""
    engine = get_db_engine()
    history_start = date.today() - timedelta(days=120)

    from api.routers.watchlist import _batch_fetch_prices, _cache_price_to_db

    scoreable_universe = get_astrogrid_scoreable_universe()
    lookup_tickers = [asset["lookup_ticker"] for asset in scoreable_universe]
    try:
        live_quotes = _batch_fetch_prices(lookup_tickers)
    except Exception as exc:
        log.debug("AstroGrid scorecard live-price fallback unavailable: {e}", e=str(exc))
        live_quotes = {}

    items: list[dict[str, Any]] = []
    source_parts: set[str] = set()

    with engine.connect() as conn:
        scoreable_universe = enrich_astrogrid_scoreable_universe(conn)
        for asset in scoreable_universe:
            feature_name, candidate_features = _resolve_scorecard_feature(conn, asset)
            history = _load_scorecard_history(conn, feature_name, history_start) if feature_name else []
            live_quote = live_quotes.get(asset["lookup_ticker"])
            if live_quote and live_quote.get("price") is not None:
                _cache_price_to_db(engine, asset["lookup_ticker"], float(live_quote["price"]), date.today())
            item = _build_scorecard_item(asset, feature_name, candidate_features, history, live_quote)
            items.append(item)
            if feature_name and history:
                source_parts.add("resolved_series")
            if live_quote:
                source_parts.add("watchlist_live")

    groups = _group_scorecard_items(items)
    leaders = sorted(
        [item for item in items if item.get("latest") is not None],
        key=lambda item: float(item["momentum_score"]),
        reverse=True,
    )[:3]
    laggards = sorted(
        [item for item in items if item.get("latest") is not None],
        key=lambda item: float(item["momentum_score"]),
    )[:3]
    evaluation = _build_scorecard_evaluation(engine, scoreable_universe)
    if evaluation["overall"]["total_predictions"] > 0:
        source_parts.add("oracle_predictions")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "+".join(sorted(source_parts)) if source_parts else "none",
        "universe": {
            "id": "hybrid_v1",
            "ranked_horizons": ["macro", "swing", "intraday"],
            "groups": ["crypto", "macro"],
            "assets": [
                {
                    "symbol": asset["symbol"],
                    "asset_class": asset["asset_class"],
                    "price_feature": asset["price_feature"],
                    "benchmark_symbol": asset["benchmark_symbol"],
                    "status": asset.get("status", "unscored"),
                    "scoreable_now": bool(asset.get("scoreable_now")),
                    "reason_if_not": asset.get("reason_if_not"),
                }
                for asset in scoreable_universe
            ],
        },
        "items": items,
        "groups": groups,
        "leaders": leaders,
        "laggards": laggards,
        "summary": _build_scorecard_summary(items, groups, evaluation),
        "evaluation": evaluation,
    }


@router.get("/universe")
async def get_scoreable_universe(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the canonical AstroGrid scoreable-universe contract."""
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            assets = enrich_astrogrid_scoreable_universe(conn)
    except Exception as exc:
        log.warning("AstroGrid universe contract fallback used: {e}", e=str(exc))
        assets = get_astrogrid_scoreable_universe()
        for asset in assets:
            asset["status"] = "unknown"
            asset["scoreable_now"] = False
            asset["reason_if_not"] = "coverage check unavailable"
            asset["history_points"] = None
            asset["latest_obs_date"] = None
    counts = Counter(str(asset.get("status") or "unknown") for asset in assets)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_id": "hybrid_v1",
        "assets": assets,
        "counts": {
            "scoreable_now": int(counts.get("scoreable_now", 0)),
            "degraded": int(counts.get("degraded", 0)),
            "unscored": int(counts.get("unscored", 0)),
            "unknown": int(counts.get("unknown", 0)),
            "total": len(assets),
        },
    }


@router.post("/interpret")
async def interpret_snapshot(
    req: AstrogridInterpretRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run a model-backed interpretation over deterministic AstroGrid state."""
    fallback = _fallback_interpretation(req)

    try:
        from ollama.client import get_client

        client = get_client()
        backend = _llm_backend_name(client)
        model = getattr(client, "model", None)
        if not getattr(client, "is_available", False):
            fallback["backend"] = backend
            fallback["model"] = model
            return fallback

        messages = _build_interpret_messages(req)
        raw = client.chat(
            messages=messages,
            temperature=0.2,
            num_predict=1200,
        )
        parsed = _parse_json_response(raw)
        if not parsed:
            fallback["backend"] = backend
            fallback["model"] = model
            return fallback

        summary = str(parsed.get("summary") or fallback["summary"])
        seer = parsed.get("seer") if isinstance(parsed.get("seer"), dict) else fallback["seer"]
        threads = parsed.get("threads") if isinstance(parsed.get("threads"), list) else fallback["threads"]
        engine_notes = parsed.get("engine_notes") if isinstance(parsed.get("engine_notes"), list) else fallback["engine_notes"]
        tone_notes = parsed.get("tone_notes") if isinstance(parsed.get("tone_notes"), list) else fallback["tone_notes"]

        result = {
            "summary": summary,
            "seer": {
                "reading": str(seer.get("reading") or fallback["seer"]["reading"]),
                "prediction": str(seer.get("prediction") or fallback["seer"]["prediction"]),
                "why": list(seer.get("why") or fallback["seer"]["why"])[:6],
                "warnings": list(seer.get("warnings") or fallback["seer"]["warnings"])[:6],
            },
            "threads": threads[:12],
            "engine_notes": engine_notes[:8],
            "tone_notes": tone_notes[:6],
            "used_llm": True,
            "backend": backend,
            "model": model,
            "raw_length": len(raw or ""),
        }
        try:
            get_astrogrid_store().save_interpretation(req.model_dump(), result)
        except Exception as persist_exc:
            log.warning("AstroGrid interpret store unavailable: {e}", e=str(persist_exc))
        return result
    except Exception as exc:
        log.warning("AstroGrid interpretation failed: {e}", e=str(exc))
        fallback["error"] = str(exc)
        try:
            get_astrogrid_store().save_interpretation(req.model_dump(), fallback)
        except Exception as persist_exc:
            log.warning("AstroGrid fallback store unavailable: {e}", e=str(persist_exc))
        return fallback


@router.post("/predictions")
async def create_prediction(
    req: AstrogridPredictionRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Persist an AstroGrid prediction and immediate postmortem stub."""
    store = get_astrogrid_store()
    stub = _build_postmortem_stub(req)
    horizon = _infer_prediction_horizon(req)
    target_symbols = _infer_target_symbols(req)
    scoring_class, target_statuses = _classify_prediction_scoreability(target_symbols)
    confidence = _prediction_confidence(req)
    market_overlay_snapshot = dict(req.market_overlay_snapshot or {})
    scorecard_overlay = dict(market_overlay_snapshot.get("scorecard") or {})
    scorecard_overlay["target_statuses"] = target_statuses
    market_overlay_snapshot["scorecard"] = scorecard_overlay

    oracle_publish_result = {"status": "not_attempted"}
    publish_payload = {
        "prediction_id": None,
        "question": req.question,
        "target_universe": req.target_universe,
        "scoring_class": scoring_class,
        "target_symbols": target_symbols,
        "horizon_label": horizon,
        "call": req.call,
        "timing": req.timing,
        "invalidation": req.invalidation,
        "confidence": confidence,
        "weight_version": req.weight_version,
        "model_version": req.model_version,
        "grid_summary": stub["grid_summary"],
        "mystical_summary": stub["mystical_summary"],
    }

    prediction_payload = {
        "as_of_ts": datetime.now(timezone.utc).isoformat(),
        "question": req.question,
        "call": req.call,
        "timing": req.timing,
        "setup": req.setup,
        "invalidation": req.invalidation,
        "note": req.note,
        "mode": req.mode,
        "lens_ids": req.lens_ids,
        "snapshot": req.snapshot,
        "seer_summary": (req.seer or {}).get("prediction") or (req.seer or {}).get("reading"),
        "market_overlay_snapshot": market_overlay_snapshot,
        "mystical_feature_payload": {
            "seer": req.seer,
            "engine_outputs": req.engine_outputs,
            "snapshot": {
                "lunar": (req.snapshot or {}).get("lunar"),
                "nakshatra": (req.snapshot or {}).get("nakshatra"),
                "aspects": list((req.snapshot or {}).get("aspects") or [])[:8],
            },
        },
        "grid_feature_payload": market_overlay_snapshot,
        "weight_version": req.weight_version,
        "model_version": req.model_version,
        "live_or_local": req.live_or_local,
        "status": "pending",
        "target_universe": req.target_universe,
        "scoring_class": scoring_class,
        "target_symbols": target_symbols,
        "horizon_label": horizon,
        "postmortem_summary": stub["summary"],
        "dominant_grid_drivers": stub["dominant_grid_drivers"],
        "dominant_mystical_drivers": stub["dominant_mystical_drivers"],
        "feature_family_summary": stub["feature_family_summary"],
        "postmortem_raw_payload": {
            "question": req.question,
            "call": req.call,
            "timing": req.timing,
            "setup": req.setup,
            "invalidation": req.invalidation,
            "note": req.note,
            "seer": req.seer,
            "engine_outputs": req.engine_outputs,
            "market_overlay": market_overlay_snapshot,
        },
    }

    prediction_payload["prediction_id"] = str(uuid4())

    if req.publish_oracle:
        try:
            publish_payload["prediction_id"] = prediction_payload["prediction_id"]
            oracle_publish_result = publish_astrogrid_prediction(get_db_engine(), publish_payload)
        except Exception as exc:
            oracle_publish_result = {
                "status": "failed",
                "error": str(exc),
                "contract": "oracle.publish.v1",
            }
            log.warning("AstroGrid Oracle publish failed: {e}", e=str(exc))

    prediction_payload["oracle_publish"] = oracle_publish_result
    record = store.save_prediction(prediction_payload)
    if not record:
        return {"error": "Prediction persistence failed."}
    return record


@router.get("/predictions/latest")
async def get_latest_predictions(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return {
        "predictions": get_astrogrid_store().list_predictions(limit=limit, offset=offset),
        "limit": limit,
        "offset": offset,
    }


@router.get("/postmortems")
async def get_postmortems(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return {
        "postmortems": get_astrogrid_store().list_postmortems(limit=limit, offset=offset),
        "limit": limit,
        "offset": offset,
    }


@router.post("/predictions/score")
async def score_predictions(
    req: AstrogridScoreRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    as_of_date = None
    if req.as_of_date:
        try:
            as_of_date = _parse_optional_date(req.as_of_date)
        except ValueError:
            return {"error": f"Invalid date format: {req.as_of_date}. Use YYYY-MM-DD."}
    return get_astrogrid_store().score_predictions(
        as_of_date=as_of_date,
        limit=max(1, min(req.limit, 500)),
        prediction_ids=req.prediction_ids or None,
    )


@router.get("/predictions/scoreboard")
async def get_prediction_scoreboard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    store = get_astrogrid_store()
    return {
        "scoreboard": store.build_prediction_scoreboard(),
        "weights": store.ensure_active_weight_version(),
    }


@router.post("/backtest/run")
async def run_backtest(
    req: AstrogridBacktestRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    try:
        window_start = _parse_optional_date(req.window_start)
        window_end = _parse_optional_date(req.window_end)
    except ValueError as exc:
        return {"error": str(exc)}
    return get_astrogrid_store().run_backtests(
        strategy_variants=req.strategy_variants,
        horizon_label=req.horizon_label,
        window_start=window_start,
        window_end=window_end,
        limit=max(1, min(req.limit, 1000)),
    )


@router.get("/backtest/summary")
async def get_backtest_summary(
    limit: int = Query(default=12, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return get_astrogrid_store().get_backtest_summary(limit=limit)


@router.get("/backtest/results")
async def get_backtest_results(
    strategy_variant: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return {
        "results": get_astrogrid_store().list_backtest_results(
            strategy_variant=strategy_variant,
            limit=limit,
        ),
        "strategy_variant": strategy_variant,
    }


@router.get("/weights/current")
async def get_current_weights(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return get_astrogrid_store().ensure_active_weight_version()


@router.post("/review/generate")
async def generate_review_run(
    req: AstrogridReviewRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return get_astrogrid_store().generate_review_run(
        provider_mode=req.provider_mode,
        prediction_limit=max(1, min(req.prediction_limit, 1000)),
        backtest_limit=max(1, min(req.backtest_limit, 100)),
    )


@router.post("/learning-loop/run")
async def run_learning_loop(
    req: AstrogridLearningLoopRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    try:
        as_of_date = _parse_optional_date(req.as_of_date)
    except ValueError as exc:
        return {"error": str(exc)}
    return get_astrogrid_store().run_learning_loop(
        as_of_date=as_of_date,
        score_limit=max(1, min(req.score_limit, 1000)),
        backtest_limit=max(1, min(req.backtest_limit, 2000)),
        backtest_window_days=max(7, min(req.backtest_window_days, 3650)),
        provider_mode=req.provider_mode,
        horizon_label=req.horizon_label,
    )


@router.get("/review/latest")
async def get_latest_review(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    review = get_astrogrid_store().get_latest_review()
    if not review:
        return {"error": "No review run available yet."}
    return review


@router.get("/weights/proposals")
async def get_weight_proposals(
    status: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return {
        "proposals": get_astrogrid_store().list_weight_proposals(status=status, limit=limit),
        "status": status,
    }


@router.post("/weights/proposals/{weight_proposal_id}/approve")
async def approve_weight_proposal(
    weight_proposal_id: str,
    req: AstrogridWeightDecisionRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    proposal = get_astrogrid_store().approve_weight_proposal(
        weight_proposal_id,
        decided_by=req.decided_by,
        notes=req.notes,
    )
    if not proposal:
        return {"error": f"Weight proposal not found: {weight_proposal_id}"}
    return proposal


@router.post("/weights/proposals/{weight_proposal_id}/reject")
async def reject_weight_proposal(
    weight_proposal_id: str,
    req: AstrogridWeightDecisionRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    proposal = get_astrogrid_store().reject_weight_proposal(
        weight_proposal_id,
        decided_by=req.decided_by,
        notes=req.notes,
    )
    if not proposal:
        return {"error": f"Weight proposal not found: {weight_proposal_id}"}
    return proposal


@router.get("/predictions/{prediction_id}")
async def get_prediction_detail(
    prediction_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    record = get_astrogrid_store().get_prediction(prediction_id)
    if not record:
        return {"error": f"Prediction not found: {prediction_id}"}
    return record


@router.get("/ephemeris")
async def get_ephemeris(
    date_str: str = Query(alias="date", default=None, description="YYYY-MM-DD"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Full ephemeris for any date (computed on the fly if not in DB)."""
    target = date.today()
    if date_str:
        try:
            target = date.fromisoformat(date_str)
        except ValueError:
            return {"error": f"Invalid date format: {date_str}. Use YYYY-MM-DD."}

    engine = get_db_engine()

    # Try resolved_series first
    db_data: dict[str, float] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT fr.name, rs.value "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON rs.feature_id = fr.id "
                    "WHERE rs.obs_date = :d "
                    "AND (fr.family = 'celestial' OR fr.name LIKE 'lunar%%' "
                    "     OR fr.name LIKE 'solar%%' OR fr.name LIKE 'planetary%%' "
                    "     OR fr.name LIKE 'mercury%%' OR fr.name LIKE 'nakshatra%%' "
                    "     OR fr.name LIKE 'chinese%%' OR fr.name LIKE 'venus%%' "
                    "     OR fr.name LIKE 'mars%%' OR fr.name LIKE 'jupiter%%' "
                    "     OR fr.name LIKE 'tithi%%' OR fr.name LIKE 'rahu%%' "
                    "     OR fr.name LIKE 'dasha%%' OR fr.name LIKE 'sunspot%%' "
                    "     OR fr.name LIKE 'geomagnetic%%' OR fr.name LIKE 'feng%%' "
                    "     OR fr.name LIKE 'iching%%' OR fr.name LIKE 'days_to%%')"
                ),
                {"d": target},
            ).fetchall()
            for r in rows:
                db_data[r[0]] = float(r[1])
    except Exception:
        pass

    # Always compute full ephemeris (deterministic features)
    computed = _compute_full_ephemeris(target)

    # Merge: DB values override computed where available (they may include API-sourced solar data)
    for key, val in db_data.items():
        if key in computed:
            computed[key] = val
        else:
            computed[key] = val

    # Add solar API features if available from DB but not computable
    for solar_feat in ["sunspot_number", "solar_flux_10_7cm", "geomagnetic_kp_index",
                       "geomagnetic_ap_index", "solar_wind_speed", "solar_storm_probability"]:
        if solar_feat not in computed and solar_feat in db_data:
            computed[solar_feat] = db_data[solar_feat]

    return {
        "date": str(target),
        "ephemeris": computed,
        "source": "computed" if not db_data else "db+computed",
        "feature_count": len(computed),
    }


@router.get("/correlations")
async def get_correlations(
    market_feature: str | None = Query(default=None),
    celestial_category: str | None = Query(default=None),
    lookback_days: int = Query(default=504),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return ranked list of significant celestial-market correlations."""
    from analysis.astro_correlations import AstroCorrelationEngine, ALL_CELESTIAL_FEATURES

    engine = get_db_engine()
    ace = AstroCorrelationEngine(engine)

    # Filter celestial features by category if requested
    cel_features: list[str] | None = None
    if celestial_category:
        category_map = {
            "lunar": ["lunar_phase", "lunar_illumination", "days_to_new_moon",
                       "days_to_full_moon", "lunar_eclipse_proximity", "solar_eclipse_proximity"],
            "planetary": ["mercury_retrograde", "jupiter_saturn_angle", "mars_volatility_index",
                          "planetary_stress_index", "venus_cycle_phase"],
            "solar": ["sunspot_number", "solar_flux_10_7cm", "geomagnetic_kp_index",
                       "geomagnetic_ap_index", "solar_wind_speed", "solar_storm_probability",
                       "solar_cycle_phase"],
            "vedic": ["nakshatra_index", "nakshatra_quality", "tithi",
                       "rahu_ketu_axis", "dasha_cycle_phase"],
            "chinese": ["chinese_zodiac_year", "chinese_element", "chinese_yin_yang",
                         "feng_shui_flying_star", "chinese_lunar_month", "iching_hexagram_of_day"],
        }
        cel_features = category_map.get(celestial_category.lower())
        if cel_features is None:
            return {"error": f"Unknown category: {celestial_category}. "
                    f"Valid: {', '.join(category_map.keys())}"}

    mkt_features: list[str] | None = None
    if market_feature:
        mkt_features = [market_feature]

    try:
        results = ace.get_cached_or_compute()

        # Filter if needed
        if cel_features:
            results = [r for r in results if r["celestial_feature"] in cel_features]
        if market_feature:
            results = [r for r in results if r["market_feature"] == market_feature]

        return {
            "correlations": results,
            "count": len(results),
            "lookback_days": lookback_days,
            "filters": {
                "market_feature": market_feature,
                "celestial_category": celestial_category,
            },
        }
    except Exception as exc:
        log.warning("Correlation query failed: {e}", e=str(exc))
        return {"error": str(exc), "correlations": [], "count": 0}


@router.get("/timeline")
async def get_timeline(
    start: str = Query(default=None, description="YYYY-MM-DD"),
    end: str = Query(default=None, description="YYYY-MM-DD"),
    types: str = Query(default=None, description="Comma-separated: retrograde,eclipse,full_moon,new_moon"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Event timeline merging celestial and market regime events."""
    today = date.today()
    start_date = date.fromisoformat(start) if start else today - timedelta(days=365)
    end_date = date.fromisoformat(end) if end else today + timedelta(days=90)

    type_filter = set()
    if types:
        type_filter = {t.strip().lower() for t in types.split(",")}

    events: list[dict] = []

    # Mercury retrogrades
    if not type_filter or "retrograde" in type_filter:
        for s, e in _MERCURY_RETROGRADES:
            if s <= end_date and e >= start_date:
                events.append({
                    "type": "retrograde",
                    "subtype": "mercury",
                    "date": str(s),
                    "end_date": str(e),
                    "description": f"Mercury retrograde: {s} to {e}",
                })

    # Lunar eclipses
    if not type_filter or "eclipse" in type_filter:
        for ecl in _LUNAR_ECLIPSES:
            if start_date <= ecl <= end_date:
                events.append({
                    "type": "eclipse",
                    "subtype": "lunar",
                    "date": str(ecl),
                    "description": f"Lunar eclipse",
                })

    # Solar eclipses
    if not type_filter or "eclipse" in type_filter:
        for ecl in _SOLAR_ECLIPSES:
            if start_date <= ecl <= end_date:
                events.append({
                    "type": "eclipse",
                    "subtype": "solar",
                    "date": str(ecl),
                    "description": f"Solar eclipse",
                })

    # Full moons and new moons
    if not type_filter or "full_moon" in type_filter or "new_moon" in type_filter:
        d = start_date
        while d <= end_date:
            phase = _lunar_phase(d)
            if (not type_filter or "full_moon" in type_filter) and abs(phase - 0.5) < 0.017:
                events.append({
                    "type": "full_moon",
                    "date": str(d),
                    "description": f"Full Moon ({_illumination(phase):.0f}% illuminated)",
                })
                d += timedelta(days=25)
                continue
            if (not type_filter or "new_moon" in type_filter) and (phase < 0.017 or phase > 0.983):
                events.append({
                    "type": "new_moon",
                    "date": str(d),
                    "description": "New Moon",
                })
                d += timedelta(days=25)
                continue
            d += timedelta(days=1)

    # Market regime changes from regime_history (if available)
    if not type_filter or "regime" in type_filter:
        engine = get_db_engine()
        try:
            with engine.connect() as conn:
                regime_rows = conn.execute(
                    text(
                        "SELECT obs_date, regime_label "
                        "FROM regime_history "
                        "WHERE obs_date >= :start AND obs_date <= :end "
                        "ORDER BY obs_date"
                    ),
                    {"start": start_date, "end": end_date},
                ).fetchall()

            prev_regime = None
            for r in regime_rows:
                if r[1] != prev_regime:
                    events.append({
                        "type": "regime",
                        "date": str(r[0]),
                        "description": f"Market regime: {r[1]}",
                    })
                    prev_regime = r[1]
        except Exception:
            pass

    # Sort chronologically
    events.sort(key=lambda e: e["date"])

    return {
        "start": str(start_date),
        "end": str(end_date),
        "events": events,
        "count": len(events),
    }


@router.get("/briefing")
async def get_briefing(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Latest celestial narrative briefing."""
    engine = get_db_engine()
    today = date.today()

    # Try cached briefing
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT content, generated_at FROM briefings "
                    "WHERE type = 'celestial' "
                    "ORDER BY generated_at DESC LIMIT 1"
                ),
            ).fetchone()
            if row:
                return {
                    "briefing": row[0],
                    "generated_at": str(row[1]),
                    "source": "cached",
                }
    except Exception:
        pass

    # Generate on demand from current state
    eph = _compute_full_ephemeris(today)
    phase_name = _phase_name(eph["lunar_phase"])
    illum = eph["lunar_illumination"]

    active_retros = []
    for s, e in _MERCURY_RETROGRADES:
        if s <= today <= e:
            active_retros.append(f"Mercury ({s} to {e})")

    briefing_lines = [
        f"Celestial Briefing for {today.isoformat()}",
        "",
        f"LUNAR: {phase_name}, {illum:.0f}% illuminated. "
        f"Next new moon in {eph['days_to_new_moon']:.0f} days, "
        f"next full moon in {eph['days_to_full_moon']:.0f} days.",
        "",
        f"PLANETARY: {'Retrogrades active: ' + ', '.join(active_retros) if active_retros else 'No retrogrades active.'}. "
        f"{int(eph['planetary_stress_index'])} hard aspects. "
        f"Mars volatility index: {eph['mars_volatility_index']:.3f}.",
        "",
        f"VEDIC: Nakshatra {eph['nakshatra_name']} "
        f"({'Fixed' if eph['nakshatra_quality'] == 0 else 'Movable' if eph['nakshatra_quality'] == 1 else 'Dual'}). "
        f"Tithi {int(eph['tithi'])}.",
        "",
        f"CHINESE: Year of the {_ELEMENTS[_element_index(today)]} "
        f"{_ZODIAC_ANIMALS[_zodiac_index(today)]}. "
        f"I Ching hexagram #{int(eph['iching_hexagram_of_day'])}.",
    ]

    return {
        "briefing": "\n".join(briefing_lines),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "computed",
    }


@router.post("/compare")
async def compare_dates(
    body: CompareDatesRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Side-by-side ephemeris comparison for two dates."""
    try:
        d1 = date.fromisoformat(body.date1)
        d2 = date.fromisoformat(body.date2)
    except ValueError as exc:
        return {"error": f"Invalid date format: {exc}. Use YYYY-MM-DD."}

    eph1 = _compute_full_ephemeris(d1)
    eph2 = _compute_full_ephemeris(d2)

    # Identify differences
    differences: list[dict] = []

    # Moon phase
    pn1, pn2 = _phase_name(eph1["lunar_phase"]), _phase_name(eph2["lunar_phase"])
    if pn1 != pn2:
        differences.append({
            "feature": "moon_phase",
            "date1": pn1,
            "date2": pn2,
        })

    # Mercury retrograde
    mr1, mr2 = bool(eph1["mercury_retrograde"]), bool(eph2["mercury_retrograde"])
    if mr1 != mr2:
        differences.append({
            "feature": "mercury_retrograde",
            "date1": mr1,
            "date2": mr2,
        })

    # Nakshatra
    if eph1["nakshatra_name"] != eph2["nakshatra_name"]:
        differences.append({
            "feature": "nakshatra",
            "date1": eph1["nakshatra_name"],
            "date2": eph2["nakshatra_name"],
        })

    # Chinese zodiac
    if eph1["chinese_zodiac_animal"] != eph2["chinese_zodiac_animal"]:
        differences.append({
            "feature": "chinese_zodiac",
            "date1": f"{eph1['chinese_element_name']} {eph1['chinese_zodiac_animal']}",
            "date2": f"{eph2['chinese_element_name']} {eph2['chinese_zodiac_animal']}",
        })

    # Planetary stress
    if int(eph1["planetary_stress_index"]) != int(eph2["planetary_stress_index"]):
        differences.append({
            "feature": "planetary_stress_index",
            "date1": int(eph1["planetary_stress_index"]),
            "date2": int(eph2["planetary_stress_index"]),
        })

    # Tithi
    if int(eph1["tithi"]) != int(eph2["tithi"]):
        differences.append({
            "feature": "tithi",
            "date1": int(eph1["tithi"]),
            "date2": int(eph2["tithi"]),
        })

    return {
        "date1": str(d1),
        "date2": str(d2),
        "ephemeris1": eph1,
        "ephemeris2": eph2,
        "differences": differences,
        "difference_count": len(differences),
    }


@router.get("/retrograde")
async def get_retrogrades(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Active and upcoming retrograde periods."""
    today = date.today()
    horizon = today + timedelta(days=30)

    currently_active: list[dict] = []
    upcoming_30: list[dict] = []

    for start, end in _MERCURY_RETROGRADES:
        if start <= today <= end:
            currently_active.append({
                "planet": "Mercury",
                "start": str(start),
                "end": str(end),
                "days_remaining": (end - today).days,
            })
        elif today < start <= horizon:
            upcoming_30.append({
                "planet": "Mercury",
                "start": str(start),
                "end": str(end),
                "days_until": (start - today).days,
            })

    return {
        "as_of": str(today),
        "currently_active": currently_active,
        "upcoming_30_days": upcoming_30,
        "mercury_retrogrades_total": len(_MERCURY_RETROGRADES),
    }


@router.get("/eclipses")
async def get_eclipses(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Eclipse calendar with optional market impact data."""
    engine = get_db_engine()
    today = date.today()
    one_year = today + timedelta(days=365)

    # Next eclipses
    next_lunar = None
    for ecl in _LUNAR_ECLIPSES:
        if ecl >= today:
            next_lunar = {"date": str(ecl), "type": "lunar", "days_until": (ecl - today).days}
            break

    next_solar = None
    for ecl in _SOLAR_ECLIPSES:
        if ecl >= today:
            next_solar = {"date": str(ecl), "type": "solar", "days_until": (ecl - today).days}
            break

    # Upcoming year
    upcoming: list[dict] = []
    for ecl in _LUNAR_ECLIPSES:
        if today <= ecl <= one_year:
            upcoming.append({"date": str(ecl), "type": "lunar"})
    for ecl in _SOLAR_ECLIPSES:
        if today <= ecl <= one_year:
            upcoming.append({"date": str(ecl), "type": "solar"})
    upcoming.sort(key=lambda e: e["date"])

    # Past eclipses with market return data (+-5 days)
    past_with_returns: list[dict] = []
    try:
        with engine.connect() as conn:
            # Check if we have SPY data
            fid_row = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = 'spy_full' AND model_eligible = TRUE")
            ).fetchone()

            if fid_row:
                fid = fid_row[0]
                for ecl_list, ecl_type in [(_LUNAR_ECLIPSES, "lunar"), (_SOLAR_ECLIPSES, "solar")]:
                    for ecl in ecl_list:
                        if ecl >= today:
                            continue
                        pre = ecl - timedelta(days=5)
                        post = ecl + timedelta(days=5)
                        vals = conn.execute(
                            text(
                                "SELECT obs_date, value FROM resolved_series "
                                "WHERE feature_id = :fid AND obs_date >= :pre AND obs_date <= :post "
                                "ORDER BY obs_date"
                            ),
                            {"fid": fid, "pre": pre, "post": post},
                        ).fetchall()
                        if len(vals) >= 2:
                            first_val = float(vals[0][1])
                            last_val = float(vals[-1][1])
                            if first_val > 0:
                                ret = (last_val - first_val) / first_val * 100.0
                                past_with_returns.append({
                                    "date": str(ecl),
                                    "type": ecl_type,
                                    "spy_return_pct": round(ret, 2),
                                    "window": "+-5 days",
                                })
    except Exception:
        pass

    return {
        "as_of": str(today),
        "next_lunar": next_lunar,
        "next_solar": next_solar,
        "upcoming_year": upcoming,
        "past_with_market_data": past_with_returns[-20:],  # last 20
    }


@router.get("/nakshatra")
async def get_nakshatra(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current nakshatra with market stats if available."""
    engine = get_db_engine()
    today = date.today()

    moon_lon = _moon_sidereal_longitude(today)
    nak_idx = _nakshatra_from_longitude(moon_lon)
    nak_name = _NAKSHATRA_NAMES[nak_idx]
    quality = _NAKSHATRA_QUALITY[nak_idx]
    quality_name = ["Fixed", "Movable", "Dual"][quality]

    result: dict[str, Any] = {
        "as_of": str(today),
        "nakshatra_index": nak_idx,
        "nakshatra_name": nak_name,
        "quality": quality_name,
        "moon_sidereal_longitude": round(moon_lon, 2),
        "tithi": _tithi(today),
    }

    # Try to get average market return during this nakshatra
    try:
        with engine.connect() as conn:
            # Get nakshatra_index feature_id and spy_full feature_id
            nak_fid = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = 'nakshatra_index'")
            ).fetchone()
            spy_fid = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = 'spy_full' AND model_eligible = TRUE")
            ).fetchone()

            if nak_fid and spy_fid:
                # Get dates when this nakshatra was active
                rows = conn.execute(
                    text(
                        "SELECT n.obs_date, s.value "
                        "FROM resolved_series n "
                        "JOIN resolved_series s ON n.obs_date = s.obs_date "
                        "WHERE n.feature_id = :nid AND n.value = :nval "
                        "AND s.feature_id = :sid "
                        "ORDER BY n.obs_date"
                    ),
                    {"nid": nak_fid[0], "nval": float(nak_idx), "sid": spy_fid[0]},
                ).fetchall()

                if len(rows) >= 2:
                    # Compute daily returns during this nakshatra
                    vals = [float(r[1]) for r in rows]
                    returns = [(vals[i] - vals[i - 1]) / vals[i - 1] * 100.0
                               for i in range(1, len(vals)) if vals[i - 1] > 0]
                    if returns:
                        import numpy as np
                        arr = np.array(returns)
                        result["market_stats"] = {
                            "avg_daily_return_pct": round(float(np.mean(arr)), 4),
                            "median_daily_return_pct": round(float(np.median(arr)), 4),
                            "n_observations": len(returns),
                            "positive_pct": round(float(np.mean(arr > 0) * 100), 1),
                        }
    except Exception as exc:
        log.debug("Nakshatra market stats failed: {e}", e=str(exc))

    return result


@router.get("/lunar/calendar")
async def get_lunar_calendar(
    month: int = Query(default=None, ge=1, le=12),
    year: int = Query(default=None, ge=2000, le=2040),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Monthly moon phase calendar."""
    today = date.today()
    y = year or today.year
    m = month or today.month

    _, days_in_month = calendar.monthrange(y, m)

    days: list[dict] = []
    for day in range(1, days_in_month + 1):
        d = date(y, m, day)
        phase = _lunar_phase(d)
        illum = _illumination(phase)
        days.append({
            "date": str(d),
            "phase": round(phase, 4),
            "illumination": round(illum, 1),
            "phase_name": _phase_name(phase),
        })

    return {
        "year": y,
        "month": m,
        "days": days,
    }


@router.get("/solar/activity")
async def get_solar_activity(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current solar weather from resolved_series."""
    engine = get_db_engine()
    today = date.today()

    solar_features = {
        "geomagnetic_kp_index": None,
        "geomagnetic_ap_index": None,
        "sunspot_number": None,
        "solar_flux_10_7cm": None,
        "solar_wind_speed": None,
        "solar_storm_probability": None,
        "solar_cycle_phase": None,
    }

    dates: dict[str, str | None] = {}

    for feat in solar_features:
        val, obs_date = _get_latest_resolved(engine, feat)
        solar_features[feat] = val
        dates[feat] = obs_date

    # Compute solar_cycle_phase if not in DB
    if solar_features["solar_cycle_phase"] is None:
        solar_features["solar_cycle_phase"] = round(_solar_cycle_phase(today), 6)

    kp = solar_features["geomagnetic_kp_index"]

    return {
        "as_of": str(today),
        "features": solar_features,
        "obs_dates": dates,
        "interpretation": {
            "geomagnetic_status": _interpret_kp(kp),
            "kp_level": (
                f"Kp = {kp:.1f}" if kp is not None else "Kp unavailable"
            ),
            "sunspot_count": (
                f"{int(solar_features['sunspot_number'])} sunspots"
                if solar_features["sunspot_number"] is not None
                else "Sunspot data unavailable"
            ),
            "solar_wind": (
                f"{solar_features['solar_wind_speed']:.0f} km/s"
                if solar_features["solar_wind_speed"] is not None
                else "Wind speed unavailable"
            ),
            "storm_probability": (
                f"{solar_features['solar_storm_probability']:.0f}%"
                if solar_features["solar_storm_probability"] is not None
                else "N/A"
            ),
        },
    }
