"""AstroGrid sub-router: ephemeris, correlations, timeline, briefing, compare,
retrograde, eclipses, nakshatra, lunar calendar, solar activity."""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.astrogrid_helpers import (
    CompareDatesRequest,
    _compute_full_ephemeris,
    _element_index,
    _ELEMENTS,
    _get_latest_resolved,
    _illumination,
    _interpret_kp,
    _lunar_phase,
    _LUNAR_ECLIPSES,
    _MERCURY_RETROGRADES,
    _moon_sidereal_longitude,
    _nakshatra_from_longitude,
    _NAKSHATRA_NAMES,
    _NAKSHATRA_QUALITY,
    _phase_name,
    _solar_cycle_phase,
    _SOLAR_ECLIPSES,
    _tithi,
    _zodiac_index,
    _ZODIAC_ANIMALS,
)

router = APIRouter(tags=["astrogrid"])


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

    computed = _compute_full_ephemeris(target)

    for key, val in db_data.items():
        computed[key] = val

    for solar_feat in [
        "sunspot_number", "solar_flux_10_7cm", "geomagnetic_kp_index",
        "geomagnetic_ap_index", "solar_wind_speed", "solar_storm_probability",
    ]:
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
    from analysis.astro_correlations import AstroCorrelationEngine

    engine = get_db_engine()
    ace = AstroCorrelationEngine(engine)

    cel_features: list[str] | None = None
    if celestial_category:
        category_map = {
            "lunar": [
                "lunar_phase", "lunar_illumination", "days_to_new_moon",
                "days_to_full_moon", "lunar_eclipse_proximity", "solar_eclipse_proximity",
            ],
            "planetary": [
                "mercury_retrograde", "jupiter_saturn_angle", "mars_volatility_index",
                "planetary_stress_index", "venus_cycle_phase",
            ],
            "solar": [
                "sunspot_number", "solar_flux_10_7cm", "geomagnetic_kp_index",
                "geomagnetic_ap_index", "solar_wind_speed", "solar_storm_probability",
                "solar_cycle_phase",
            ],
            "vedic": [
                "nakshatra_index", "nakshatra_quality", "tithi",
                "rahu_ketu_axis", "dasha_cycle_phase",
            ],
            "chinese": [
                "chinese_zodiac_year", "chinese_element", "chinese_yin_yang",
                "feng_shui_flying_star", "chinese_lunar_month", "iching_hexagram_of_day",
            ],
        }
        cel_features = category_map.get(celestial_category.lower())
        if cel_features is None:
            return {
                "error": (
                    f"Unknown category: {celestial_category}. "
                    f"Valid: {', '.join(category_map.keys())}"
                )
            }

    try:
        results = ace.get_cached_or_compute()

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
    types: str = Query(
        default=None,
        description="Comma-separated: retrograde,eclipse,full_moon,new_moon",
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Event timeline merging celestial and market regime events."""
    today = date.today()
    start_date = date.fromisoformat(start) if start else today - timedelta(days=365)
    end_date = date.fromisoformat(end) if end else today + timedelta(days=90)

    type_filter: set[str] = set()
    if types:
        type_filter = {t.strip().lower() for t in types.split(",")}

    events: list[dict[str, Any]] = []

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

    if not type_filter or "eclipse" in type_filter:
        for ecl in _LUNAR_ECLIPSES:
            if start_date <= ecl <= end_date:
                events.append({
                    "type": "eclipse",
                    "subtype": "lunar",
                    "date": str(ecl),
                    "description": "Lunar eclipse",
                })

    if not type_filter or "eclipse" in type_filter:
        for ecl in _SOLAR_ECLIPSES:
            if start_date <= ecl <= end_date:
                events.append({
                    "type": "eclipse",
                    "subtype": "solar",
                    "date": str(ecl),
                    "description": "Solar eclipse",
                })

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
            if (not type_filter or "new_moon" in type_filter) and (
                phase < 0.017 or phase > 0.983
            ):
                events.append({
                    "type": "new_moon",
                    "date": str(d),
                    "description": "New Moon",
                })
                d += timedelta(days=25)
                continue
            d += timedelta(days=1)

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
        (
            f"PLANETARY: "
            f"{'Retrogrades active: ' + ', '.join(active_retros) if active_retros else 'No retrogrades active.'}. "
            f"{int(eph['planetary_stress_index'])} hard aspects. "
            f"Mars volatility index: {eph['mars_volatility_index']:.3f}."
        ),
        "",
        (
            f"VEDIC: Nakshatra {eph['nakshatra_name']} "
            f"({'Fixed' if eph['nakshatra_quality'] == 0 else 'Movable' if eph['nakshatra_quality'] == 1 else 'Dual'}). "
            f"Tithi {int(eph['tithi'])}."
        ),
        "",
        (
            f"CHINESE: Year of the {_ELEMENTS[_element_index(today)]} "
            f"{_ZODIAC_ANIMALS[_zodiac_index(today)]}. "
            f"I Ching hexagram #{int(eph['iching_hexagram_of_day'])}."
        ),
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

    differences: list[dict[str, Any]] = []

    pn1, pn2 = _phase_name(eph1["lunar_phase"]), _phase_name(eph2["lunar_phase"])
    if pn1 != pn2:
        differences.append({"feature": "moon_phase", "date1": pn1, "date2": pn2})

    mr1, mr2 = bool(eph1["mercury_retrograde"]), bool(eph2["mercury_retrograde"])
    if mr1 != mr2:
        differences.append({"feature": "mercury_retrograde", "date1": mr1, "date2": mr2})

    if eph1["nakshatra_name"] != eph2["nakshatra_name"]:
        differences.append({
            "feature": "nakshatra",
            "date1": eph1["nakshatra_name"],
            "date2": eph2["nakshatra_name"],
        })

    if eph1["chinese_zodiac_animal"] != eph2["chinese_zodiac_animal"]:
        differences.append({
            "feature": "chinese_zodiac",
            "date1": f"{eph1['chinese_element_name']} {eph1['chinese_zodiac_animal']}",
            "date2": f"{eph2['chinese_element_name']} {eph2['chinese_zodiac_animal']}",
        })

    if int(eph1["planetary_stress_index"]) != int(eph2["planetary_stress_index"]):
        differences.append({
            "feature": "planetary_stress_index",
            "date1": int(eph1["planetary_stress_index"]),
            "date2": int(eph2["planetary_stress_index"]),
        })

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

    currently_active: list[dict[str, Any]] = []
    upcoming_30: list[dict[str, Any]] = []

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

    upcoming: list[dict[str, Any]] = []
    for ecl in _LUNAR_ECLIPSES:
        if today <= ecl <= one_year:
            upcoming.append({"date": str(ecl), "type": "lunar"})
    for ecl in _SOLAR_ECLIPSES:
        if today <= ecl <= one_year:
            upcoming.append({"date": str(ecl), "type": "solar"})
    upcoming.sort(key=lambda e: e["date"])

    past_with_returns: list[dict[str, Any]] = []
    try:
        with engine.connect() as conn:
            fid_row = conn.execute(
                text(
                    "SELECT id FROM feature_registry "
                    "WHERE name = 'spy_full' AND model_eligible = TRUE"
                )
            ).fetchone()

            if fid_row:
                fid = fid_row[0]
                for ecl_list, ecl_type in [
                    (_LUNAR_ECLIPSES, "lunar"),
                    (_SOLAR_ECLIPSES, "solar"),
                ]:
                    for ecl in ecl_list:
                        if ecl >= today:
                            continue
                        pre = ecl - timedelta(days=5)
                        post = ecl + timedelta(days=5)
                        vals = conn.execute(
                            text(
                                "SELECT obs_date, value FROM resolved_series "
                                "WHERE feature_id = :fid "
                                "AND obs_date >= :pre AND obs_date <= :post "
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
        "past_with_market_data": past_with_returns[-20:],
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

    try:
        with engine.connect() as conn:
            nak_fid = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = 'nakshatra_index'")
            ).fetchone()
            spy_fid = conn.execute(
                text(
                    "SELECT id FROM feature_registry "
                    "WHERE name = 'spy_full' AND model_eligible = TRUE"
                )
            ).fetchone()

            if nak_fid and spy_fid:
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
                    vals = [float(r[1]) for r in rows]
                    returns = [
                        (vals[i] - vals[i - 1]) / vals[i - 1] * 100.0
                        for i in range(1, len(vals))
                        if vals[i - 1] > 0
                    ]
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

    days: list[dict[str, Any]] = []
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

    return {"year": y, "month": m, "days": days}


@router.get("/solar/activity")
async def get_solar_activity(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current solar weather from resolved_series."""
    engine = get_db_engine()
    today = date.today()

    solar_features: dict[str, Any] = {
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
