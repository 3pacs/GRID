"""AstroGrid API — expanded celestial intelligence endpoints.

Provides ephemeris computation, celestial-market correlations, event timelines,
retrograde tracking, eclipse calendars, nakshatra stats, lunar calendars,
solar weather, and comparative date analysis.
"""

from __future__ import annotations

import calendar
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

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

router = APIRouter(prefix="/api/v1/astrogrid", tags=["astrogrid"])


# ── Pydantic models ───────────────────────────────────────────────────

class CompareDatesRequest(BaseModel):
    date1: str
    date2: str


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


def _phase_name(phase: float) -> str:
    for lo, hi, name in _PHASE_NAMES:
        if lo <= phase < hi:
            return name
    return "Unknown"


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


# ── Endpoints ──────────────────────────────────────────────────────────

@router.get("/overview")
async def get_overview(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current state of all celestial systems with interpretations."""
    engine = get_db_engine()
    today = date.today()

    try:
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
