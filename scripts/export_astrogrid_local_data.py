#!/usr/bin/env python3
"""Export AstroGrid data to portable local files.

This script does not require PostgreSQL. It computes a deterministic
AstroGrid archive locally and fetches NOAA solar files with the Python
standard library so the dataset can be shipped to a server later.

Outputs:
    outputs/astrogrid_data/
      manifest.json
      latest_snapshot.json
      years/daily_YYYY.jsonl
      raw/noaa/*.json
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.ephemeris import (  # noqa: E402
    ASPECTS,
    AYANAMSHA_J2000,
    J2000_DATE,
    OBLIQUITY_J2000,
    PRECESSION_RATE,
    ZODIAC_SIGNS,
    _ecliptic_to_equatorial,
    Ephemeris,
    get_ephemeris,
)

DEFAULT_START_DATE = date(2000, 1, 1)
OUTPUT_ROOT = Path("outputs/astrogrid_data")

NOAA_URLS = {
    "noaa_planetary_k_index.json": "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json",
    "solar_wind_plasma_7day.json": "https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json",
    "observed_solar_cycle_indices.json": "https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json",
}

ELEMENT_BY_SIGN = {
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

BODY_META = {
    "Sun": {"id": "sun", "class": "luminary", "visual_priority": 100},
    "Moon": {"id": "moon", "class": "luminary", "visual_priority": 95},
    "Mercury": {"id": "mercury", "class": "planet", "visual_priority": 90},
    "Venus": {"id": "venus", "class": "planet", "visual_priority": 88},
    "Mars": {"id": "mars", "class": "planet", "visual_priority": 86},
    "Jupiter": {"id": "jupiter", "class": "planet", "visual_priority": 84},
    "Saturn": {"id": "saturn", "class": "planet", "visual_priority": 82},
    "Uranus": {"id": "uranus", "class": "planet", "visual_priority": 76},
    "Neptune": {"id": "neptune", "class": "planet", "visual_priority": 74},
    "Pluto": {"id": "pluto", "class": "planet", "visual_priority": 72},
    "Rahu": {"id": "rahu", "class": "node", "visual_priority": 68},
    "Ketu": {"id": "ketu", "class": "node", "visual_priority": 66},
}

ORDERED_BODIES = [
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

ASPECT_BODIES = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"]
NAKSHATRA_QUALITY_INDEX = {"fixed": 0, "movable": 1, "dual": 2}

_CNY_DATES = {
    2015: date(2015, 2, 19),
    2016: date(2016, 2, 8),
    2017: date(2017, 1, 28),
    2018: date(2018, 2, 16),
    2019: date(2019, 2, 5),
    2020: date(2020, 1, 25),
    2021: date(2021, 2, 12),
    2022: date(2022, 2, 1),
    2023: date(2023, 1, 22),
    2024: date(2024, 2, 10),
    2025: date(2025, 1, 29),
    2026: date(2026, 2, 17),
    2027: date(2027, 2, 6),
    2028: date(2028, 1, 26),
    2029: date(2029, 2, 13),
    2030: date(2030, 2, 3),
    2031: date(2031, 1, 23),
    2032: date(2032, 2, 11),
    2033: date(2033, 1, 31),
    2034: date(2034, 2, 19),
    2035: date(2035, 2, 8),
}

_ELEMENTS = ["Wood", "Fire", "Earth", "Metal", "Water"]
_ZODIAC_ANIMALS = [
    "Rat", "Ox", "Tiger", "Rabbit", "Dragon", "Snake",
    "Horse", "Goat", "Monkey", "Rooster", "Dog", "Pig",
]
_FLYING_STAR_PERIODS = [
    (1864, 1883, 1), (1884, 1903, 2), (1904, 1923, 3), (1924, 1943, 4),
    (1944, 1963, 5), (1964, 1983, 6), (1984, 2003, 7), (2004, 2023, 8),
    (2024, 2043, 9), (2044, 2063, 1), (2064, 2083, 2),
]

LUNAR_ECLIPSES = [
    date(2020, 1, 10), date(2020, 6, 5), date(2020, 7, 5), date(2020, 11, 30),
    date(2021, 5, 26), date(2021, 11, 19), date(2022, 5, 16), date(2022, 11, 8),
    date(2023, 5, 5), date(2023, 10, 28), date(2024, 3, 25), date(2024, 9, 18),
    date(2025, 3, 14), date(2025, 9, 7), date(2026, 3, 3), date(2026, 8, 28),
    date(2027, 2, 20), date(2027, 7, 18), date(2027, 8, 17), date(2028, 1, 12),
    date(2028, 7, 6), date(2028, 12, 31), date(2029, 6, 26), date(2029, 12, 20),
    date(2030, 6, 15), date(2030, 12, 9),
]

SOLAR_ECLIPSES = [
    date(2020, 6, 21), date(2020, 12, 14), date(2021, 6, 10), date(2021, 12, 4),
    date(2022, 4, 30), date(2022, 10, 25), date(2023, 4, 20), date(2023, 10, 14),
    date(2024, 4, 8), date(2024, 10, 2), date(2025, 3, 29), date(2025, 9, 21),
    date(2026, 2, 17), date(2026, 8, 12), date(2027, 2, 6), date(2027, 8, 2),
    date(2028, 1, 26), date(2028, 7, 22), date(2029, 1, 14), date(2029, 6, 12),
    date(2029, 7, 11), date(2029, 12, 5), date(2030, 6, 1), date(2030, 11, 25),
]

_SYNODIC_MONTH = 29.53059
_REF_NEW_MOON = datetime(2000, 1, 6, 18, 14, 0, tzinfo=timezone.utc)
_MOON_L0 = 218.3165
_MOON_RATE = 13.17640
_SUN_L0 = 280.4665
_SUN_RATE = 0.98565
_RAHU_L0 = 125.0445
_RAHU_RATE = -0.05295
_CYCLE_25_START = date(2019, 12, 1)
_CYCLE_LENGTH_DAYS = 11.0 * 365.25
_VENUS_SYNODIC_REF = date(2022, 1, 9)
_VENUS_SYNODIC_PERIOD = 583.9
_DASHA_YEARS = [7, 20, 6, 10, 7, 18, 16, 19, 17]
_DASHA_TOTAL = 120.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export AstroGrid local data")
    parser.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--outdir", default=str(OUTPUT_ROOT))
    return parser.parse_args()


def ensure_dirs(outdir: Path) -> None:
    (outdir / "raw" / "noaa").mkdir(parents=True, exist_ok=True)
    (outdir / "years").mkdir(parents=True, exist_ok=True)


def normalize_angle(value: float) -> float:
    return value % 360.0


def signed_delta(current: float, future: float) -> float:
    diff = (future - current) % 360.0
    if diff > 180.0:
        diff -= 360.0
    return diff


def angular_separation(lon1: float, lon2: float) -> float:
    diff = abs(lon1 - lon2) % 360.0
    return diff if diff <= 180.0 else 360.0 - diff


def compute_sun_position(ephemeris: Ephemeris, target: date) -> dict[str, Any]:
    t_centuries = ephemeris.centuries_since_j2000(target)
    earth_lon, earth_lat, earth_dist = ephemeris._heliocentric_position("Earth", t_centuries)
    sun_lon = normalize_angle(earth_lon + 180.0)
    sun_lat = -earth_lat
    obliquity = OBLIQUITY_J2000 - 0.013004 * t_centuries
    ra, dec = _ecliptic_to_equatorial(sun_lon, sun_lat, obliquity)
    sign_idx = int(sun_lon / 30.0) % 12
    return {
        "planet": "Sun",
        "ecliptic_longitude": round(sun_lon, 4),
        "ecliptic_latitude": round(sun_lat, 4),
        "heliocentric_longitude": None,
        "distance_au": round(earth_dist, 6),
        "geocentric_longitude": round(sun_lon, 4),
        "zodiac_sign": ZODIAC_SIGNS[sign_idx],
        "zodiac_degree": round(sun_lon % 30.0, 4),
        "is_retrograde": False,
        "right_ascension": round(ra, 4),
        "declination": round(dec, 4),
        "source": "computed",
        "precision": "approximate_daily",
    }


def enrich_position(position: dict[str, Any], *, source: str = "analysis.ephemeris", precision: str = "approximate_daily") -> dict[str, Any]:
    enriched = dict(position)
    enriched["source"] = source
    enriched["precision"] = precision
    return enriched


def moon_sidereal_longitude(target: date) -> float:
    days = (target - J2000_DATE).days
    return normalize_angle(_MOON_L0 + _MOON_RATE * days)


def sun_sidereal_longitude(target: date) -> float:
    days = (target - J2000_DATE).days
    tropical = normalize_angle(_SUN_L0 + _SUN_RATE * days)
    ayanamsha = AYANAMSHA_J2000 + PRECESSION_RATE * (days / 365.25)
    return normalize_angle(tropical - ayanamsha)


def tithi(target: date) -> int:
    diff = (moon_sidereal_longitude(target) - sun_sidereal_longitude(target)) % 360.0
    return min(int(diff / 12.0) + 1, 30)


def rahu_longitude(target: date) -> float:
    days = (target - J2000_DATE).days
    return normalize_angle(_RAHU_L0 + _RAHU_RATE * days)


def dasha_cycle_phase(target: date) -> float:
    moon_lon = moon_sidereal_longitude(target)
    nak_span = 360.0 / 27.0
    nak_index = int(moon_lon / nak_span) % 27
    pos_in_nak = (moon_lon % nak_span) / nak_span
    planet_idx = nak_index % 9
    cumulative = sum(_DASHA_YEARS[:planet_idx]) + pos_in_nak * _DASHA_YEARS[planet_idx]
    return cumulative / _DASHA_TOTAL


def chinese_year(target: date) -> int:
    cny = _CNY_DATES.get(target.year, date(target.year, 2, 5))
    return target.year - 1 if target < cny else target.year


def chinese_features(target: date) -> dict[str, Any]:
    cy = chinese_year(target)
    exact_support = target.year in _CNY_DATES
    zodiac_index = (cy - 4) % 12
    element_index = ((cy - 4) % 10) // 2
    yin_yang = (cy - 4) % 2
    flying_star = next(
        (
            star for start, end, star in _FLYING_STAR_PERIODS
            if start <= target.year <= end
        ),
        ((target.year - 1864) % 180) // 20 % 9 + 1,
    )
    dt = datetime(target.year, target.month, target.day, 12, 0, 0, tzinfo=timezone.utc)
    cny = _CNY_DATES.get(target.year, date(target.year, 2, 5))
    cny_dt = datetime(cny.year, cny.month, cny.day, 12, 0, 0, tzinfo=timezone.utc)
    lunar_month = ((int((dt - cny_dt).total_seconds() / 86400.0 / _SYNODIC_MONTH)) % 12) + 1
    upper = ((target.year % 100) + target.month + target.day) % 8
    lower = ((target.year % 100) + target.month * target.day) % 8
    hexagram = min(max(upper * 8 + lower + 1, 1), 64)
    return {
        "year_number": cy,
        "zodiac_index": zodiac_index,
        "zodiac_animal": _ZODIAC_ANIMALS[zodiac_index],
        "element_index": element_index,
        "element_name": _ELEMENTS[element_index],
        "yin_yang": "Yang" if yin_yang == 0 else "Yin",
        "flying_star": flying_star,
        "lunar_month": lunar_month,
        "iching_hexagram": hexagram,
        "source": "computed",
        "precision": "exact_table" if exact_support else "approximate_feb5_fallback",
        "exact_support": exact_support,
    }


def solar_cycle_phase(target: date) -> float:
    days_into_cycle = (target - _CYCLE_25_START).days
    return (days_into_cycle % _CYCLE_LENGTH_DAYS) / _CYCLE_LENGTH_DAYS


def venus_cycle_phase(target: date) -> float:
    days = (target - _VENUS_SYNODIC_REF).days
    return (days % _VENUS_SYNODIC_PERIOD) / _VENUS_SYNODIC_PERIOD


def dominant_element(positions: dict[str, dict[str, Any]]) -> str:
    counts = Counter()
    for body_name in ORDERED_BODIES:
        sign = positions[body_name].get("zodiac_sign")
        element = ELEMENT_BY_SIGN.get(sign)
        if element:
            counts[element] += 1
    return counts.most_common(1)[0][0] if counts else "unknown"


def aspect_counts(aspects: list[dict[str, Any]]) -> dict[str, int]:
    hard = sum(1 for aspect in aspects if aspect.get("aspect_type") in {"conjunction", "square", "opposition"})
    soft = sum(1 for aspect in aspects if aspect.get("aspect_type") in {"trine", "sextile"})
    return {"hard": hard, "soft": soft}


def nearest_eclipse(target: date, eclipse_dates: list[date]) -> int | None:
    if target.year < 2020 or target.year > 2030:
        return None
    return min(abs((eclipse - target).days) for eclipse in eclipse_dates)


def compute_daily_motions(current_positions: dict[str, dict[str, Any]], next_positions: dict[str, dict[str, Any]]) -> dict[str, float | None]:
    motions: dict[str, float | None] = {}
    for body_name in ORDERED_BODIES:
        current = current_positions[body_name].get("geocentric_longitude")
        future = next_positions[body_name].get("geocentric_longitude")
        if current is None or future is None:
            motions[body_name] = None
            continue
        motions[body_name] = round(signed_delta(float(current), float(future)), 4)
    return motions


def compute_aspects_from_positions(
    current_positions: dict[str, dict[str, Any]],
    next_positions: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for index, planet1 in enumerate(ASPECT_BODIES):
        for planet2 in ASPECT_BODIES[index + 1:]:
            lon1 = float(current_positions[planet1]["geocentric_longitude"])
            lon2 = float(current_positions[planet2]["geocentric_longitude"])
            separation = angular_separation(lon1, lon2)
            next_separation = angular_separation(
                float(next_positions[planet1]["geocentric_longitude"]),
                float(next_positions[planet2]["geocentric_longitude"]),
            )
            for aspect_name, aspect_meta in ASPECTS.items():
                diff = abs(separation - float(aspect_meta["angle"]))
                if diff > float(aspect_meta["orb"]):
                    continue
                found.append({
                    "planet1": planet1,
                    "planet2": planet2,
                    "aspect_type": aspect_name,
                    "exact_angle": aspect_meta["angle"],
                    "angle_between": round(separation, 4),
                    "orb_used": round(diff, 4),
                    "nature": aspect_meta["nature"],
                    "applying": abs(next_separation - float(aspect_meta["angle"])) < diff,
                })
    return found


def build_summary(positions: dict[str, dict[str, Any]]) -> list[str]:
    summary: list[str] = []
    for body_name in ORDERED_BODIES:
        pos = positions[body_name]
        retrograde = " (R)" if pos.get("is_retrograde") and body_name not in {"Rahu", "Ketu"} else ""
        summary.append(f"{body_name}: {pos['zodiac_sign']} {float(pos['zodiac_degree']):.1f}{retrograde}")
    return summary


def build_objects(positions: dict[str, dict[str, Any]], motions: dict[str, float | None]) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for body_name in ORDERED_BODIES:
        meta = BODY_META[body_name]
        pos = positions[body_name]
        objects.append({
            "id": meta["id"],
            "name": body_name,
            "class": meta["class"],
            "visual_priority": meta["visual_priority"],
            "track_mode": "reliable",
            "source": pos.get("source", "analysis.ephemeris"),
            "precision": pos.get("precision", "approximate_daily"),
            "longitude": pos.get("geocentric_longitude"),
            "latitude": pos.get("ecliptic_latitude"),
            "right_ascension": pos.get("right_ascension"),
            "declination": pos.get("declination"),
            "distance": pos.get("distance_au"),
            "speed": motions.get(body_name),
            "sign": pos.get("zodiac_sign"),
            "degree": pos.get("zodiac_degree"),
            "retrograde": bool(pos.get("is_retrograde")),
        })
    return objects


def build_events(
    target: date,
    lunar: dict[str, Any],
    nakshatra: dict[str, Any],
    aspects: list[dict[str, Any]],
    void_of_course: dict[str, Any],
    retrogrades: list[str],
) -> list[dict[str, Any]]:
    next_full = target + timedelta(days=int(round(float(lunar.get("days_to_full", 0.0)))))
    next_new = target + timedelta(days=int(round(float(lunar.get("days_to_new", 0.0)))))
    events: list[dict[str, Any]] = [
        {
            "id": f"phase-{target.isoformat()}",
            "type": "lunar",
            "name": lunar.get("phase_name", "Lunar phase"),
            "date": target.isoformat(),
            "description": f"{float(lunar.get('illumination', 0.0)):.1f}% illumination.",
        },
        {
            "id": f"next-full-{next_full.isoformat()}",
            "type": "full_moon_window",
            "name": "Next Full Moon",
            "date": next_full.isoformat(),
            "description": f"In {(next_full - target).days} days.",
        },
        {
            "id": f"next-new-{next_new.isoformat()}",
            "type": "new_moon_window",
            "name": "Next New Moon",
            "date": next_new.isoformat(),
            "description": f"In {(next_new - target).days} days.",
        },
        {
            "id": f"nakshatra-{target.isoformat()}",
            "type": "nakshatra",
            "name": str(nakshatra.get("nakshatra_name", "Nakshatra")),
            "date": target.isoformat(),
            "description": f"{nakshatra.get('quality', 'unknown')} quality, pada {nakshatra.get('pada', '—')}.",
        },
    ]
    if void_of_course.get("is_void"):
        events.append({
            "id": f"void-{target.isoformat()}",
            "type": "void_of_course",
            "name": "Void of Course",
            "date": target.isoformat(),
            "description": f"Moon remains in {void_of_course.get('current_sign', 'sign')} before next ingress.",
        })
    for aspect in sorted(aspects, key=lambda item: float(item.get("orb_used", 999.0)))[:4]:
        events.append({
            "id": f"aspect-{aspect['planet1']}-{aspect['planet2']}-{aspect['aspect_type']}",
            "type": "aspect",
            "name": f"{aspect['planet1']} {aspect['aspect_type']} {aspect['planet2']}",
            "date": target.isoformat(),
            "description": f"Orb {float(aspect['orb_used']):.2f}°. {'Applying' if aspect.get('applying') else 'Separating'}.",
        })
    for planet in retrogrades:
        events.append({
            "id": f"retrograde-{planet.lower()}-{target.isoformat()}",
            "type": "retrograde",
            "name": f"{planet} retrograde",
            "date": target.isoformat(),
            "description": "Backward apparent motion is active.",
        })
    next_lunar = next((eclipse for eclipse in LUNAR_ECLIPSES if eclipse >= target), None)
    next_solar = next((eclipse for eclipse in SOLAR_ECLIPSES if eclipse >= target), None)
    if next_lunar:
        events.append({
            "id": f"lunar-eclipse-{next_lunar.isoformat()}",
            "type": "eclipse",
            "name": "Next Lunar Eclipse",
            "date": next_lunar.isoformat(),
            "description": f"In {(next_lunar - target).days} days.",
        })
    if next_solar:
        events.append({
            "id": f"solar-eclipse-{next_solar.isoformat()}",
            "type": "eclipse",
            "name": "Next Solar Eclipse",
            "date": next_solar.isoformat(),
            "description": f"In {(next_solar - target).days} days.",
        })
    return events


def build_local_features(
    target: date,
    positions: dict[str, dict[str, Any]],
    lunar: dict[str, Any],
    nakshatra: dict[str, Any],
    aspects: list[dict[str, Any]],
    chinese: dict[str, Any],
    monthlies: dict[str, dict[str, float]],
    recent_daily: dict[str, dict[str, float]],
) -> dict[str, float | int | None]:
    month_key = target.strftime("%Y-%m")
    day_key = target.isoformat()
    jupiter_lon = float(positions["Jupiter"]["geocentric_longitude"])
    saturn_lon = float(positions["Saturn"]["geocentric_longitude"])
    mars_lon = float(positions["Mars"]["geocentric_longitude"])
    hard_count = aspect_counts(aspects)["hard"]
    mars_volatility = 0.0
    for comparison in (jupiter_lon, saturn_lon):
        separation = angular_separation(mars_lon, comparison)
        for target_angle in (0.0, 90.0, 180.0):
            closeness = abs(separation - target_angle)
            if closeness < 15.0:
                mars_volatility += (15.0 - closeness) / 15.0

    chinese_exact = bool(chinese["exact_support"])
    return {
        "lunar_phase": float(lunar.get("phase", 0.0)),
        "lunar_illumination": float(lunar.get("illumination", 0.0)),
        "days_to_new_moon": float(lunar.get("days_to_new", 0.0)),
        "days_to_full_moon": float(lunar.get("days_to_full", 0.0)),
        "lunar_eclipse_proximity": nearest_eclipse(target, LUNAR_ECLIPSES),
        "solar_eclipse_proximity": nearest_eclipse(target, SOLAR_ECLIPSES),
        "mercury_retrograde": 1 if positions["Mercury"].get("is_retrograde") else 0,
        "jupiter_saturn_angle": round(angular_separation(jupiter_lon, saturn_lon), 4),
        "mars_volatility_index": round(min(mars_volatility / 2.0, 1.0), 6),
        "planetary_stress_index": hard_count,
        "venus_cycle_phase": round(venus_cycle_phase(target), 6),
        "nakshatra_index": int(nakshatra.get("nakshatra_index", 0)),
        "nakshatra_quality": NAKSHATRA_QUALITY_INDEX.get(str(nakshatra.get("quality", "")).lower()),
        "tithi": tithi(target),
        "rahu_ketu_axis": round(rahu_longitude(target), 4),
        "dasha_cycle_phase": round(dasha_cycle_phase(target), 6),
        "chinese_zodiac_year": int(chinese["zodiac_index"]) if chinese_exact else None,
        "chinese_element": int(chinese["element_index"]) if chinese_exact else None,
        "chinese_yin_yang": (0 if chinese["yin_yang"] == "Yang" else 1) if chinese_exact else None,
        "feng_shui_flying_star": int(chinese["flying_star"]) if chinese_exact else None,
        "chinese_lunar_month": int(chinese["lunar_month"]) if chinese_exact else None,
        "iching_hexagram_of_day": int(chinese["iching_hexagram"]) if chinese_exact else None,
        "solar_cycle_phase": round(solar_cycle_phase(target), 6),
        "sunspot_number_monthly": monthlies["sunspot"].get(month_key),
        "solar_flux_10_7cm_monthly": monthlies["flux"].get(month_key),
        "geomagnetic_kp_index_recent": recent_daily["kp"].get(day_key),
        "solar_wind_speed_recent": recent_daily["wind"].get(day_key),
    }


def fetch_json(url: str) -> Any:
    with urlopen(url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_noaa_cache(outdir: Path) -> dict[str, Any]:
    noaa_dir = outdir / "raw" / "noaa"
    cache: dict[str, Any] = {"sources": {}, "errors": {}}
    for filename, url in NOAA_URLS.items():
        try:
            payload = fetch_json(url)
            (noaa_dir / filename).write_text(json.dumps(payload, indent=2), encoding="utf-8")
            cache["sources"][filename] = {
                "url": url,
                "records": len(payload) if isinstance(payload, list) else None,
            }
        except URLError as exc:
            cache["errors"][filename] = str(exc)
        except Exception as exc:  # noqa: BLE001
            cache["errors"][filename] = str(exc)
    return cache


def parse_noaa_solar_monthlies(raw_dir: Path) -> dict[str, dict[str, float]]:
    path = raw_dir / "observed_solar_cycle_indices.json"
    if not path.exists():
        return {"sunspot": {}, "flux": {}}
    data = json.loads(path.read_text(encoding="utf-8"))
    sunspot: dict[str, float] = {}
    flux: dict[str, float] = {}
    for row in data:
        tag = row.get("time-tag", "")
        if not tag:
            continue
        month_key = tag[:7]
        try:
            ssn = row.get("ssn")
            f107 = row.get("f10.7")
            if ssn is not None and float(ssn) >= 0:
                sunspot[month_key] = float(ssn)
            if f107 is not None and float(f107) >= 0:
                flux[month_key] = float(f107)
        except Exception:  # noqa: BLE001
            continue
    return {"sunspot": sunspot, "flux": flux}


def parse_noaa_recent_daily(raw_dir: Path) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {"kp": {}, "wind": {}}

    kp_path = raw_dir / "noaa_planetary_k_index.json"
    if kp_path.exists():
        data = json.loads(kp_path.read_text(encoding="utf-8"))
        per_day: dict[str, list[float]] = {}
        for row in data[1:]:
            try:
                day = row[0][:10]
                per_day.setdefault(day, []).append(float(row[1]))
            except Exception:  # noqa: BLE001
                continue
        result["kp"] = {day: sum(vals) / len(vals) for day, vals in per_day.items() if vals}

    wind_path = raw_dir / "solar_wind_plasma_7day.json"
    if wind_path.exists():
        data = json.loads(wind_path.read_text(encoding="utf-8"))
        per_day: dict[str, list[float]] = {}
        for row in data[1:]:
            try:
                if row[2] in (None, ""):
                    continue
                day = row[0][:10]
                per_day.setdefault(day, []).append(float(row[2]))
            except Exception:  # noqa: BLE001
                continue
        result["wind"] = {day: sum(vals) / len(vals) for day, vals in per_day.items() if vals}

    return result


def export_range(start_date: date, end_date: date, outdir: Path) -> dict[str, Any]:
    ephemeris = Ephemeris()
    ensure_dirs(outdir)
    noaa_fetch = fetch_noaa_cache(outdir)
    monthlies = parse_noaa_solar_monthlies(outdir / "raw" / "noaa")
    recent_daily = parse_noaa_recent_daily(outdir / "raw" / "noaa")

    handles: dict[int, Any] = {}
    counts_by_year: dict[int, int] = {}
    latest_snapshot: dict[str, Any] | None = None

    current = start_date
    snapshot = get_ephemeris(current)
    next_snapshot = get_ephemeris(current + timedelta(days=1))

    try:
        while current <= end_date:
            day_key = current.isoformat()
            positions = {name: enrich_position(pos) for name, pos in snapshot["positions"].items()}
            positions["Sun"] = compute_sun_position(ephemeris, current)

            next_positions = {name: enrich_position(pos) for name, pos in next_snapshot["positions"].items()}
            next_positions["Sun"] = compute_sun_position(ephemeris, current + timedelta(days=1))

            motions = compute_daily_motions(positions, next_positions)
            aspects = compute_aspects_from_positions(positions, next_positions)
            counts = aspect_counts(aspects)
            retrogrades = [
                name for name in ORDERED_BODIES
                if positions[name].get("is_retrograde") and name not in {"Rahu", "Ketu"}
            ]
            chinese = chinese_features(current)
            local_features = build_local_features(
                current,
                positions,
                snapshot["lunar_phase"],
                snapshot["nakshatra"],
                aspects,
                chinese,
                monthlies,
                recent_daily,
            )
            objects = build_objects(positions, motions)
            events = build_events(
                current,
                snapshot["lunar_phase"],
                snapshot["nakshatra"],
                aspects,
                snapshot["void_of_course"],
                retrogrades,
            )

            record = {
                "as_of": datetime(current.year, current.month, current.day, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
                "date": day_key,
                "source": "analysis.ephemeris",
                "precision": "approximate_daily",
                "summary": build_summary(positions),
                "lunar": snapshot["lunar_phase"],
                "nakshatra": snapshot["nakshatra"],
                "void_of_course": snapshot["void_of_course"],
                "retrograde_planets": retrogrades,
                "positions": positions,
                "motions": motions,
                "objects": objects,
                "aspects": aspects,
                "events": events,
                "derived": {
                    "hard_aspect_count": counts["hard"],
                    "soft_aspect_count": counts["soft"],
                    "dominant_element": dominant_element(positions),
                    "tithi": local_features["tithi"],
                    "rahu_longitude": local_features["rahu_ketu_axis"],
                    "ketu_longitude": round(normalize_angle(float(local_features["rahu_ketu_axis"]) + 180.0), 4),
                    "solar_cycle_phase": local_features["solar_cycle_phase"],
                    "sunspot_number_monthly": local_features["sunspot_number_monthly"],
                    "solar_flux_10_7cm_monthly": local_features["solar_flux_10_7cm_monthly"],
                    "geomagnetic_kp_index_recent": local_features["geomagnetic_kp_index_recent"],
                    "solar_wind_speed_recent": local_features["solar_wind_speed_recent"],
                },
                "local_features": local_features,
                "chinese": chinese,
                "provenance": {
                    "positions": {"source": "analysis.ephemeris", "precision": "approximate_daily"},
                    "sun": {"source": "computed_from_earth_heliocentric", "precision": "approximate_daily"},
                    "noaa_overlays": {
                        "kp": "recent_intraday",
                        "solar_wind": "recent_intraday",
                        "sunspot": "monthly",
                        "f10_7": "monthly",
                    },
                    "chinese_calendar": {"precision": chinese["precision"], "exact_support": chinese["exact_support"]},
                },
            }

            year = current.year
            if year not in handles:
                handles[year] = (outdir / "years" / f"daily_{year}.jsonl").open("w", encoding="utf-8")
                counts_by_year[year] = 0
            handles[year].write(json.dumps(record, ensure_ascii=True) + "\n")
            counts_by_year[year] += 1
            latest_snapshot = record

            if counts_by_year[year] % 200 == 0:
                print(f"[astrogrid-export] {current.isoformat()} {counts_by_year[year]} rows in {year}", flush=True)

            current += timedelta(days=1)
            if current <= end_date:
                snapshot = next_snapshot
                next_snapshot = get_ephemeris(current + timedelta(days=1))
    finally:
        for handle in handles.values():
            handle.close()

    if latest_snapshot is not None:
        (outdir / "latest_snapshot.json").write_text(json.dumps(latest_snapshot, indent=2), encoding="utf-8")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "days_exported": sum(counts_by_year.values()),
        "year_files": {str(year): count for year, count in sorted(counts_by_year.items())},
        "noaa_fetch": noaa_fetch,
        "notes": [
            "Deterministic positions, Sun-inclusive aspects, lunar state, and nakshatra exported for every day.",
            "Per-body daily motion and event summaries are included for downstream AstroGrid predictions.",
            "NOAA Kp and solar wind are recent-only overlays; NOAA sunspot and F10.7 are monthly.",
            "Chinese calendar exact support is limited to years with a hardcoded CNY table; older years are flagged approximate.",
            "Export is portable and DB-free.",
        ],
    }
    (outdir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    args = parse_args()
    start_date: date = args.start_date
    end_date: date = args.end_date
    if end_date < start_date:
        raise SystemExit("--end-date must be on or after --start-date")

    outdir = Path(args.outdir)
    ensure_dirs(outdir)
    manifest = export_range(start_date, end_date, outdir)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
