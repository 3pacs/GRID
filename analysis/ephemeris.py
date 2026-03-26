"""
GRID Ephemeris Engine -- Copernicus Module.

Pure-math planetary position calculator using VSOP87-simplified orbital elements.
Computes heliocentric and geocentric coordinates for all classical planets
plus Rahu/Ketu (lunar nodes) for Vedic calculations.

Accuracy: ~1 degree for inner planets, ~0.5 degree for outer planets.
Sufficient for astrological aspect computation and market correlation research.

No external API dependencies. All orbital elements embedded from
JPL simplified Keplerian elements (Standish 1992) / Meeus "Astronomical Algorithms".
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

# ============================================================================
# CONSTANTS
# ============================================================================

# J2000.0 epoch: 2000-01-01 12:00 TT
J2000_JD = 2451545.0
J2000_DATE = date(2000, 1, 1)

# Obliquity of the ecliptic at J2000.0 (degrees)
OBLIQUITY_J2000 = 23.439291

# Synodic month (days)
SYNODIC_MONTH = 29.53059

# Reference new moon: 2000-01-06 18:14 UTC
REF_NEW_MOON = datetime(2000, 1, 6, 18, 14, 0, tzinfo=timezone.utc)

# Moon's mean orbital elements at J2000
MOON_L0 = 218.3165  # mean longitude (degrees)
MOON_RATE = 13.17640  # degrees per day (mean sidereal motion)

# Rahu (North Node) at J2000 and regression rate
RAHU_L0 = 125.0445  # degrees
RAHU_RATE = -0.05295  # degrees per day (retrograde, 18.61-year cycle)

# Ayanamsha at J2000 (Lahiri) and annual precession rate
AYANAMSHA_J2000 = 23.85  # degrees
PRECESSION_RATE = 50.3 / 3600.0  # degrees per year

# ── Zodiac ─────────────────────────────────────────────────────────────
ZODIAC_SIGNS = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces",
]

# ── Nakshatras (27 lunar mansions) ─────────────────────────────────────
# Each spans 13.3333... degrees. Start is degrees from 0 Aries.
NAKSHATRAS = [
    {"name": "Ashwini",            "start":   0.000, "ruler": "Ketu",    "deity": "Ashwini Kumaras", "quality": "movable"},
    {"name": "Bharani",            "start":  13.333, "ruler": "Venus",   "deity": "Yama",            "quality": "fixed"},
    {"name": "Krittika",           "start":  26.667, "ruler": "Sun",     "deity": "Agni",            "quality": "dual"},
    {"name": "Rohini",             "start":  40.000, "ruler": "Moon",    "deity": "Brahma",          "quality": "fixed"},
    {"name": "Mrigashira",         "start":  53.333, "ruler": "Mars",    "deity": "Soma",            "quality": "dual"},
    {"name": "Ardra",              "start":  66.667, "ruler": "Rahu",    "deity": "Rudra",           "quality": "fixed"},
    {"name": "Punarvasu",          "start":  80.000, "ruler": "Jupiter", "deity": "Aditi",           "quality": "dual"},
    {"name": "Pushya",             "start":  93.333, "ruler": "Saturn",  "deity": "Brihaspati",      "quality": "fixed"},
    {"name": "Ashlesha",           "start": 106.667, "ruler": "Mercury", "deity": "Nagas",           "quality": "movable"},
    {"name": "Magha",              "start": 120.000, "ruler": "Ketu",    "deity": "Pitris",          "quality": "movable"},
    {"name": "Purva Phalguni",     "start": 133.333, "ruler": "Venus",   "deity": "Bhaga",           "quality": "fixed"},
    {"name": "Uttara Phalguni",    "start": 146.667, "ruler": "Sun",     "deity": "Aryaman",         "quality": "fixed"},
    {"name": "Hasta",              "start": 160.000, "ruler": "Moon",    "deity": "Savitar",         "quality": "movable"},
    {"name": "Chitra",             "start": 173.333, "ruler": "Mars",    "deity": "Vishvakarma",     "quality": "dual"},
    {"name": "Svati",              "start": 186.667, "ruler": "Rahu",    "deity": "Vayu",            "quality": "movable"},
    {"name": "Vishakha",           "start": 200.000, "ruler": "Jupiter", "deity": "Indra-Agni",      "quality": "dual"},
    {"name": "Anuradha",           "start": 213.333, "ruler": "Saturn",  "deity": "Mitra",           "quality": "fixed"},
    {"name": "Jyeshtha",           "start": 226.667, "ruler": "Mercury", "deity": "Indra",           "quality": "fixed"},
    {"name": "Mula",               "start": 240.000, "ruler": "Ketu",    "deity": "Nirriti",         "quality": "fixed"},
    {"name": "Purva Ashadha",      "start": 253.333, "ruler": "Venus",   "deity": "Apas",            "quality": "movable"},
    {"name": "Uttara Ashadha",     "start": 266.667, "ruler": "Sun",     "deity": "Vishvedevas",     "quality": "fixed"},
    {"name": "Shravana",           "start": 280.000, "ruler": "Moon",    "deity": "Vishnu",          "quality": "movable"},
    {"name": "Dhanishta",          "start": 293.333, "ruler": "Mars",    "deity": "Vasus",           "quality": "movable"},
    {"name": "Shatabhisha",        "start": 306.667, "ruler": "Rahu",    "deity": "Varuna",          "quality": "movable"},
    {"name": "Purva Bhadrapada",   "start": 320.000, "ruler": "Jupiter", "deity": "Aja Ekapada",     "quality": "dual"},
    {"name": "Uttara Bhadrapada",  "start": 333.333, "ruler": "Saturn",  "deity": "Ahir Budhnya",    "quality": "fixed"},
    {"name": "Revati",             "start": 346.667, "ruler": "Mercury", "deity": "Pushan",          "quality": "movable"},
]

# ── Aspects ────────────────────────────────────────────────────────────
ASPECTS = {
    "conjunction": {"angle": 0, "orb": 8, "nature": "variable"},
    "sextile":     {"angle": 60, "orb": 6, "nature": "harmonious"},
    "square":      {"angle": 90, "orb": 7, "nature": "challenging"},
    "trine":       {"angle": 120, "orb": 8, "nature": "harmonious"},
    "opposition":  {"angle": 180, "orb": 8, "nature": "challenging"},
}

# ── Planetary hours (Chaldean order) ───────────────────────────────────
CHALDEAN_ORDER = ["Saturn", "Jupiter", "Mars", "Sun", "Venus", "Mercury", "Moon"]

# Day rulers: Sunday=Sun, Monday=Moon, ...
DAY_RULERS = {
    0: "Sun",       # Monday in Python is 0 but we remap below
    1: "Moon",
    2: "Mars",
    3: "Mercury",
    4: "Jupiter",
    5: "Venus",
    6: "Saturn",
}

# ── J2000.0 Keplerian orbital elements + rates per Julian century ──────
# Source: JPL simplified Keplerian elements (Standish 1992)
# Format: [value_at_J2000, rate_per_century]
# a = semi-major axis (AU), e = eccentricity, i = inclination (deg),
# L = mean longitude (deg), omega_bar = longitude of perihelion (deg),
# Omega = longitude of ascending node (deg)
PLANETS = {
    "Mercury": {
        "a": [0.38709927, 0.00000037],
        "e": [0.20563593, 0.00001906],
        "i": [7.00497902, -0.00594749],
        "L": [252.25032350, 149472.67411175],
        "omega_bar": [77.45779628, 0.16047689],
        "Omega": [48.33076593, -0.12534081],
    },
    "Venus": {
        "a": [0.72333566, 0.00000390],
        "e": [0.00677672, -0.00004107],
        "i": [3.39467605, -0.00078890],
        "L": [181.97909950, 58517.81538729],
        "omega_bar": [131.60246718, 0.00268329],
        "Omega": [76.67984255, -0.27769418],
    },
    "Earth": {
        "a": [1.00000261, 0.00000562],
        "e": [0.01671123, -0.00004392],
        "i": [-0.00001531, -0.01294668],
        "L": [100.46457166, 35999.37244981],
        "omega_bar": [102.93768193, 0.32327364],
        "Omega": [0.0, 0.0],
    },
    "Mars": {
        "a": [1.52371034, 0.00001847],
        "e": [0.09339410, 0.00007882],
        "i": [1.84969142, -0.00813131],
        "L": [-4.55343205, 19140.30268499],
        "omega_bar": [-23.94362959, 0.44441088],
        "Omega": [49.55953891, -0.29257343],
    },
    "Jupiter": {
        "a": [5.20288700, -0.00011607],
        "e": [0.04838624, -0.00013253],
        "i": [1.30439695, -0.00183714],
        "L": [34.39644051, 3034.74612775],
        "omega_bar": [14.72847983, 0.21252668],
        "Omega": [100.47390909, 0.20469106],
    },
    "Saturn": {
        "a": [9.53667594, -0.00125060],
        "e": [0.05386179, -0.00050991],
        "i": [2.48599187, 0.00193609],
        "L": [49.95424423, 1222.49362201],
        "omega_bar": [92.59887831, -0.41897216],
        "Omega": [113.66242448, -0.28867794],
    },
    "Uranus": {
        "a": [19.18916464, -0.00196176],
        "e": [0.04725744, -0.00004397],
        "i": [0.77263783, -0.00242939],
        "L": [313.23810451, 428.48202785],
        "omega_bar": [170.95427630, 0.40805281],
        "Omega": [74.01692503, 0.04240589],
    },
    "Neptune": {
        "a": [30.06992276, 0.00026291],
        "e": [0.00859048, 0.00005105],
        "i": [1.77004347, 0.00035372],
        "L": [-55.12002969, 218.45945325],
        "omega_bar": [44.96476227, -0.32241464],
        "Omega": [131.78422574, -0.00508664],
    },
    "Pluto": {
        "a": [39.48211675, -0.00031596],
        "e": [0.24882730, 0.00005170],
        "i": [17.14001206, 0.00004818],
        "L": [238.92903833, 145.20780515],
        "omega_bar": [224.06891629, -0.04062942],
        "Omega": [110.30393684, -0.01183482],
    },
}

# Planets to compute (excluding Earth which is the observer)
COMPUTE_PLANETS = [
    "Mercury", "Venus", "Mars", "Jupiter", "Saturn",
    "Uranus", "Neptune", "Pluto",
]

# Lunar phase names
PHASE_NAMES = [
    "New Moon", "Waxing Crescent", "First Quarter", "Waxing Gibbous",
    "Full Moon", "Waning Gibbous", "Last Quarter", "Waning Crescent",
]


# ============================================================================
# HELPER MATH
# ============================================================================

def _normalize_angle(deg: float) -> float:
    """Normalize angle to 0-360 degrees."""
    return deg % 360.0


def _deg_to_rad(deg: float) -> float:
    return deg * math.pi / 180.0


def _rad_to_deg(rad: float) -> float:
    return rad * 180.0 / math.pi


def _solve_kepler(M_deg: float, e: float, tol: float = 1e-6) -> float:
    """Solve Kepler's equation M = E - e*sin(E) for eccentric anomaly E.

    Uses Newton-Raphson iteration.

    Parameters:
        M_deg: Mean anomaly in degrees.
        e: Orbital eccentricity.
        tol: Convergence tolerance in radians.

    Returns:
        Eccentric anomaly in degrees.
    """
    M = _deg_to_rad(_normalize_angle(M_deg))
    # Initial guess
    E = M + e * math.sin(M) if e < 0.8 else math.pi
    for _ in range(50):
        dE = (E - e * math.sin(E) - M) / (1.0 - e * math.cos(E))
        E -= dE
        if abs(dE) < tol:
            break
    return _rad_to_deg(E)


def _ecliptic_to_equatorial(
    lon_deg: float, lat_deg: float, obliquity_deg: float
) -> tuple[float, float]:
    """Convert ecliptic coordinates to equatorial (RA, Dec).

    Parameters:
        lon_deg: Ecliptic longitude in degrees.
        lat_deg: Ecliptic latitude in degrees.
        obliquity_deg: Obliquity of the ecliptic in degrees.

    Returns:
        (right_ascension_deg, declination_deg)
    """
    lon = _deg_to_rad(lon_deg)
    lat = _deg_to_rad(lat_deg)
    eps = _deg_to_rad(obliquity_deg)

    sin_ra_cos_dec = math.sin(lon) * math.cos(eps) - math.tan(lat) * math.sin(eps)
    cos_ra_cos_dec = math.cos(lon)
    ra = _rad_to_deg(math.atan2(sin_ra_cos_dec, cos_ra_cos_dec)) % 360.0

    sin_dec = (
        math.sin(lat) * math.cos(eps)
        + math.cos(lat) * math.sin(eps) * math.sin(lon)
    )
    dec = _rad_to_deg(math.asin(max(-1.0, min(1.0, sin_dec))))

    return ra, dec


def _angular_separation(lon1: float, lon2: float) -> float:
    """Return angular separation in degrees (0-180)."""
    diff = abs(lon1 - lon2) % 360.0
    return diff if diff <= 180.0 else 360.0 - diff


def _signed_angular_diff(lon1: float, lon2: float) -> float:
    """Return signed angular difference lon1 - lon2, range -180 to +180."""
    diff = (lon1 - lon2) % 360.0
    if diff > 180.0:
        diff -= 360.0
    return diff


# ============================================================================
# EPHEMERIS ENGINE
# ============================================================================

class Ephemeris:
    """Pure-math ephemeris calculator for planetary positions and aspects.

    All orbital elements are embedded. No external API calls.
    Accuracy: ~1 degree for inner planets, ~0.5 degree for outer planets.
    """

    # ── Time conversions ───────────────────────────────────────────────

    @staticmethod
    def julian_date(dt: date) -> float:
        """Convert a date to Julian Date (JD).

        Uses the standard algorithm from Meeus, "Astronomical Algorithms".
        """
        y = dt.year
        m = dt.month
        d = dt.day
        if m <= 2:
            y -= 1
            m += 12
        A = int(y / 100)
        B = 2 - A + int(A / 4)
        return int(365.25 * (y + 4716)) + int(30.6001 * (m + 1)) + d + B - 1524.5

    @staticmethod
    def centuries_since_j2000(dt: date) -> float:
        """Return T in Julian centuries since J2000.0 epoch."""
        jd = Ephemeris.julian_date(dt)
        return (jd - J2000_JD) / 36525.0

    # ── Core position computation ──────────────────────────────────────

    def _heliocentric_position(
        self, planet: str, T: float
    ) -> tuple[float, float, float]:
        """Compute heliocentric ecliptic coordinates for a planet.

        Parameters:
            planet: Planet name (key in PLANETS dict).
            T: Julian centuries since J2000.0.

        Returns:
            (ecliptic_longitude_deg, ecliptic_latitude_deg, distance_au)
        """
        elem = PLANETS[planet]

        # Compute current orbital elements
        a = elem["a"][0] + elem["a"][1] * T
        e = elem["e"][0] + elem["e"][1] * T
        i = elem["i"][0] + elem["i"][1] * T
        L = elem["L"][0] + elem["L"][1] * T
        omega_bar = elem["omega_bar"][0] + elem["omega_bar"][1] * T
        Omega = elem["Omega"][0] + elem["Omega"][1] * T

        # Argument of perihelion
        omega = omega_bar - Omega

        # Mean anomaly
        M = _normalize_angle(L - omega_bar)

        # Solve Kepler's equation for eccentric anomaly
        E = _solve_kepler(M, e)
        E_rad = _deg_to_rad(E)

        # True anomaly
        nu = _rad_to_deg(
            math.atan2(
                math.sqrt(1.0 - e * e) * math.sin(E_rad),
                math.cos(E_rad) - e,
            )
        )

        # Distance from Sun
        r = a * (1.0 - e * math.cos(E_rad))

        # Heliocentric ecliptic coordinates in the orbital plane
        # Argument of latitude
        u = _deg_to_rad(nu + omega)
        Omega_rad = _deg_to_rad(Omega)
        i_rad = _deg_to_rad(i)

        # Heliocentric ecliptic longitude and latitude
        cos_u = math.cos(u)
        sin_u = math.sin(u)
        cos_Omega = math.cos(Omega_rad)
        sin_Omega = math.sin(Omega_rad)
        cos_i = math.cos(i_rad)
        sin_i = math.sin(i_rad)

        x_ecl = r * (cos_Omega * cos_u - sin_Omega * sin_u * cos_i)
        y_ecl = r * (sin_Omega * cos_u + cos_Omega * sin_u * cos_i)
        z_ecl = r * sin_u * sin_i

        lon = _normalize_angle(_rad_to_deg(math.atan2(y_ecl, x_ecl)))
        lat = _rad_to_deg(math.asin(max(-1.0, min(1.0, z_ecl / r)))) if r > 0 else 0.0

        return lon, lat, r

    def compute_position(self, planet: str, dt: date) -> dict[str, Any]:
        """Compute full position data for a planet on a given date.

        Parameters:
            planet: Planet name (Mercury through Pluto, or "Rahu", "Ketu").
            dt: Date to compute.

        Returns:
            dict with keys: ecliptic_longitude, ecliptic_latitude, distance_au,
            geocentric_longitude, zodiac_sign, zodiac_degree, is_retrograde,
            right_ascension, declination
        """
        # Handle lunar nodes
        if planet in ("Rahu", "Ketu"):
            return self._compute_lunar_node(planet, dt)

        # Handle Moon
        if planet == "Moon":
            return self._compute_moon_position(dt)

        T = self.centuries_since_j2000(dt)

        # Heliocentric position of the planet
        h_lon, h_lat, h_dist = self._heliocentric_position(planet, T)

        # Heliocentric position of Earth (for geocentric conversion)
        e_lon, e_lat, e_dist = self._heliocentric_position("Earth", T)

        # Convert to geocentric ecliptic longitude
        # Using rectangular coordinates for proper geocentric conversion
        h_lon_rad = _deg_to_rad(h_lon)
        h_lat_rad = _deg_to_rad(h_lat)
        e_lon_rad = _deg_to_rad(e_lon)
        e_lat_rad = _deg_to_rad(e_lat)

        # Heliocentric rectangular
        xp = h_dist * math.cos(h_lat_rad) * math.cos(h_lon_rad)
        yp = h_dist * math.cos(h_lat_rad) * math.sin(h_lon_rad)
        zp = h_dist * math.sin(h_lat_rad)

        xe = e_dist * math.cos(e_lat_rad) * math.cos(e_lon_rad)
        ye = e_dist * math.cos(e_lat_rad) * math.sin(e_lon_rad)
        ze = e_dist * math.sin(e_lat_rad)

        # Geocentric rectangular
        xg = xp - xe
        yg = yp - ye
        zg = zp - ze

        geo_dist = math.sqrt(xg * xg + yg * yg + zg * zg)
        geo_lon = _normalize_angle(_rad_to_deg(math.atan2(yg, xg)))
        geo_lat = _rad_to_deg(
            math.asin(max(-1.0, min(1.0, zg / geo_dist)))
        ) if geo_dist > 0 else 0.0

        # Zodiac position
        sign_idx = int(geo_lon / 30.0) % 12
        sign_deg = geo_lon % 30.0

        # Retrograde detection: compare position 1 day before and after
        is_retro = self._check_retrograde(planet, dt)

        # Equatorial coordinates
        obliquity = OBLIQUITY_J2000 - 0.013004 * T
        ra, dec = _ecliptic_to_equatorial(geo_lon, geo_lat, obliquity)

        return {
            "planet": planet,
            "ecliptic_longitude": round(geo_lon, 4),
            "ecliptic_latitude": round(geo_lat, 4),
            "heliocentric_longitude": round(h_lon, 4),
            "distance_au": round(h_dist, 6),
            "geocentric_longitude": round(geo_lon, 4),
            "zodiac_sign": ZODIAC_SIGNS[sign_idx],
            "zodiac_degree": round(sign_deg, 4),
            "is_retrograde": is_retro,
            "right_ascension": round(ra, 4),
            "declination": round(dec, 4),
        }

    def _check_retrograde(self, planet: str, dt: date) -> bool:
        """Determine if a planet is retrograde by comparing daily positions."""
        T_prev = self.centuries_since_j2000(dt - timedelta(days=1))
        T_next = self.centuries_since_j2000(dt + timedelta(days=1))

        h_prev, _, _ = self._heliocentric_position(planet, T_prev)
        e_prev, _, _ = self._heliocentric_position("Earth", T_prev)
        h_next, _, _ = self._heliocentric_position(planet, T_next)
        e_next, _, _ = self._heliocentric_position("Earth", T_next)

        geo_prev = _normalize_angle(h_prev - e_prev)
        geo_next = _normalize_angle(h_next - e_next)

        # For proper geocentric, we should use full computation but the
        # simplified helio-minus-earth gives a good retrograde indicator
        # for outer planets. For inner planets we need full geocentric.
        T_curr = self.centuries_since_j2000(dt)

        # Use full geocentric for accuracy
        def _quick_geo_lon(T: float) -> float:
            hl, hla, hd = self._heliocentric_position(planet, T)
            el, ela, ed = self._heliocentric_position("Earth", T)
            hr = _deg_to_rad(hl)
            hlar = _deg_to_rad(hla)
            er = _deg_to_rad(el)
            elar = _deg_to_rad(ela)
            xp = hd * math.cos(hlar) * math.cos(hr)
            yp = hd * math.cos(hlar) * math.sin(hr)
            xe = ed * math.cos(elar) * math.cos(er)
            ye = ed * math.cos(elar) * math.sin(er)
            return _normalize_angle(_rad_to_deg(math.atan2(yp - ye, xp - xe)))

        lon_prev = _quick_geo_lon(T_prev)
        lon_next = _quick_geo_lon(T_next)

        # Signed difference: if next < prev (accounting for wrap), retrograde
        diff = _signed_angular_diff(lon_next, lon_prev)
        return diff < 0.0

    def _compute_moon_position(self, dt: date) -> dict[str, Any]:
        """Compute approximate Moon position using mean elements.

        This is a simplified calculation. For higher accuracy, perturbation
        terms from Meeus Chapter 47 would be needed.
        """
        days = (dt - J2000_DATE).days + 0.5  # noon

        # Mean longitude
        L = _normalize_angle(MOON_L0 + MOON_RATE * days)

        # Mean anomaly of Moon
        M_moon = _normalize_angle(134.9634 + 13.06499 * days)
        # Mean anomaly of Sun
        M_sun = _normalize_angle(357.5291 + 0.98560 * days)
        # Mean elongation
        D = _normalize_angle(297.8502 + 12.19075 * days)
        # Argument of latitude
        F = _normalize_angle(93.2720 + 13.22935 * days)

        # Principal perturbation terms (degrees)
        dL = (
            6.289 * math.sin(_deg_to_rad(M_moon))
            - 1.274 * math.sin(_deg_to_rad(2.0 * D - M_moon))
            + 0.658 * math.sin(_deg_to_rad(2.0 * D))
            - 0.214 * math.sin(_deg_to_rad(2.0 * M_moon))
            - 0.186 * math.sin(_deg_to_rad(M_sun))
            - 0.114 * math.sin(_deg_to_rad(2.0 * F))
        )

        ecl_lon = _normalize_angle(L + dL)

        # Latitude (simplified)
        ecl_lat = (
            5.128 * math.sin(_deg_to_rad(F))
            + 0.281 * math.sin(_deg_to_rad(M_moon + F))
            - 0.278 * math.sin(_deg_to_rad(F - M_moon))
        )

        # Distance (km, simplified)
        dist_km = 385001.0 - 20905.0 * math.cos(_deg_to_rad(M_moon))
        dist_au = dist_km / 149597870.7

        sign_idx = int(ecl_lon / 30.0) % 12
        sign_deg = ecl_lon % 30.0

        # Moon retrograde check: Moon never goes retrograde in geocentric view
        # But we track if it's "slow" (near standstill)

        # Equatorial
        T = self.centuries_since_j2000(dt)
        obliquity = OBLIQUITY_J2000 - 0.013004 * T
        ra, dec = _ecliptic_to_equatorial(ecl_lon, ecl_lat, obliquity)

        return {
            "planet": "Moon",
            "ecliptic_longitude": round(ecl_lon, 4),
            "ecliptic_latitude": round(ecl_lat, 4),
            "heliocentric_longitude": None,
            "distance_au": round(dist_au, 6),
            "geocentric_longitude": round(ecl_lon, 4),
            "zodiac_sign": ZODIAC_SIGNS[sign_idx],
            "zodiac_degree": round(sign_deg, 4),
            "is_retrograde": False,
            "right_ascension": round(ra, 4),
            "declination": round(dec, 4),
        }

    def _compute_lunar_node(self, node: str, dt: date) -> dict[str, Any]:
        """Compute Rahu or Ketu position."""
        days = (dt - J2000_DATE).days + 0.5
        rahu_lon = _normalize_angle(RAHU_L0 + RAHU_RATE * days)

        if node == "Ketu":
            lon = _normalize_angle(rahu_lon + 180.0)
        else:
            lon = rahu_lon

        sign_idx = int(lon / 30.0) % 12
        sign_deg = lon % 30.0

        return {
            "planet": node,
            "ecliptic_longitude": round(lon, 4),
            "ecliptic_latitude": 0.0,
            "heliocentric_longitude": None,
            "distance_au": None,
            "geocentric_longitude": round(lon, 4),
            "zodiac_sign": ZODIAC_SIGNS[sign_idx],
            "zodiac_degree": round(sign_deg, 4),
            "is_retrograde": True,  # Nodes are always retrograde
            "right_ascension": None,
            "declination": None,
        }

    # ── Batch computation ──────────────────────────────────────────────

    def compute_all_positions(self, dt: date) -> dict[str, dict]:
        """Compute positions for all planets, Moon, and lunar nodes.

        Parameters:
            dt: Date to compute.

        Returns:
            dict mapping planet name to position dict.
        """
        result = {}
        for planet in COMPUTE_PLANETS:
            result[planet] = self.compute_position(planet, dt)
        result["Moon"] = self.compute_position("Moon", dt)
        result["Rahu"] = self.compute_position("Rahu", dt)
        result["Ketu"] = self.compute_position("Ketu", dt)
        return result

    # ── Aspect computation ─────────────────────────────────────────────

    def compute_aspects(
        self, dt: date, orb: float | None = None
    ) -> list[dict[str, Any]]:
        """Find all aspects between planet pairs on a given date.

        Parameters:
            dt: Date to compute.
            orb: Global orb override (degrees). If None, uses per-aspect defaults.

        Returns:
            List of aspect dicts with keys: planet1, planet2, aspect_type,
            exact_angle, angle_between, orb_used, nature, applying
        """
        positions = self.compute_all_positions(dt)

        # Planets to check aspects between (exclude nodes for traditional aspects)
        aspect_bodies = COMPUTE_PLANETS + ["Moon"]

        found = []
        for i, p1 in enumerate(aspect_bodies):
            for p2 in aspect_bodies[i + 1:]:
                lon1 = positions[p1]["geocentric_longitude"]
                lon2 = positions[p2]["geocentric_longitude"]
                sep = _angular_separation(lon1, lon2)

                for asp_name, asp_def in ASPECTS.items():
                    asp_orb = orb if orb is not None else asp_def["orb"]
                    diff = abs(sep - asp_def["angle"])
                    if diff <= asp_orb:
                        # Determine if applying or separating
                        applying = self._is_aspect_applying(
                            p1, p2, dt, asp_def["angle"]
                        )
                        found.append({
                            "planet1": p1,
                            "planet2": p2,
                            "aspect_type": asp_name,
                            "exact_angle": asp_def["angle"],
                            "angle_between": round(sep, 4),
                            "orb_used": round(diff, 4),
                            "nature": asp_def["nature"],
                            "applying": applying,
                        })
        return found

    def _is_aspect_applying(
        self, p1: str, p2: str, dt: date, exact_angle: float
    ) -> bool:
        """Determine if an aspect is applying (getting closer to exact)."""
        try:
            pos_today = self.compute_all_positions(dt)
            pos_tomorrow = self.compute_all_positions(dt + timedelta(days=1))

            sep_today = _angular_separation(
                pos_today[p1]["geocentric_longitude"],
                pos_today[p2]["geocentric_longitude"],
            )
            sep_tomorrow = _angular_separation(
                pos_tomorrow[p1]["geocentric_longitude"],
                pos_tomorrow[p2]["geocentric_longitude"],
            )

            # Applying if getting closer to exact angle
            diff_today = abs(sep_today - exact_angle)
            diff_tomorrow = abs(sep_tomorrow - exact_angle)
            return diff_tomorrow < diff_today
        except Exception:
            return False

    # ── Lunar phase ────────────────────────────────────────────────────

    def compute_lunar_phase(self, dt: date) -> dict[str, Any]:
        """Compute lunar phase information for a date.

        Returns:
            dict with keys: phase (0-1), illumination (0-100), phase_name,
            days_to_new, days_to_full, phase_angle
        """
        dt_utc = datetime(dt.year, dt.month, dt.day, 12, 0, 0, tzinfo=timezone.utc)
        days_since_ref = (dt_utc - REF_NEW_MOON).total_seconds() / 86400.0
        phase = (days_since_ref % SYNODIC_MONTH) / SYNODIC_MONTH

        # Illumination from phase angle
        illumination = (1.0 - math.cos(phase * 2.0 * math.pi)) / 2.0 * 100.0

        # Phase name (8 divisions)
        phase_idx = int(phase * 8.0) % 8
        phase_name = PHASE_NAMES[phase_idx]

        # Days to new moon
        days_to_new = (1.0 - phase) * SYNODIC_MONTH if phase > 0.001 else 0.0
        # Days to full moon
        phase_to_full = 0.5 - phase
        if phase_to_full < 0:
            phase_to_full += 1.0
        days_to_full = phase_to_full * SYNODIC_MONTH

        return {
            "phase": round(phase, 6),
            "illumination": round(illumination, 4),
            "phase_name": phase_name,
            "days_to_new": round(days_to_new, 2),
            "days_to_full": round(days_to_full, 2),
            "phase_angle": round(phase * 360.0, 4),
        }

    # ── Nakshatra ──────────────────────────────────────────────────────

    def compute_nakshatra(self, dt: date) -> dict[str, Any]:
        """Compute the Moon's nakshatra (Vedic lunar mansion) for a date.

        Returns:
            dict with keys: nakshatra_index, nakshatra_name, pada (1-4),
            ruling_planet, deity, quality, moon_longitude,
            degree_in_nakshatra
        """
        moon = self._compute_moon_position(dt)
        moon_lon = moon["ecliptic_longitude"]

        # Apply ayanamsha for sidereal longitude
        days = (dt - J2000_DATE).days
        ayanamsha = AYANAMSHA_J2000 + PRECESSION_RATE * (days / 365.25)
        sidereal_lon = _normalize_angle(moon_lon - ayanamsha)

        # Each nakshatra spans 360/27 = 13.3333... degrees
        nak_span = 360.0 / 27.0
        nak_idx = int(sidereal_lon / nak_span) % 27
        nak = NAKSHATRAS[nak_idx]

        # Degree within nakshatra
        deg_in_nak = sidereal_lon - nak_idx * nak_span

        # Pada (quarter): each nakshatra has 4 padas of 3.3333 degrees
        pada = int(deg_in_nak / (nak_span / 4.0)) + 1
        pada = min(pada, 4)

        return {
            "nakshatra_index": nak_idx,
            "nakshatra_name": nak["name"],
            "pada": pada,
            "ruling_planet": nak["ruler"],
            "deity": nak["deity"],
            "quality": nak["quality"],
            "moon_longitude": round(moon_lon, 4),
            "sidereal_longitude": round(sidereal_lon, 4),
            "degree_in_nakshatra": round(deg_in_nak, 4),
        }

    # ── Planetary hours ────────────────────────────────────────────────

    def compute_planetary_hours(
        self, dt: date, latitude: float, longitude: float
    ) -> list[dict[str, Any]]:
        """Compute traditional planetary hours for a date and location.

        Planetary hours divide daylight and nighttime into 12 segments each,
        with each hour ruled by a planet in the Chaldean order.

        Parameters:
            dt: Date to compute.
            latitude: Observer latitude in degrees.
            longitude: Observer longitude in degrees.

        Returns:
            List of 24 dicts, each with: hour_number (1-24), planet,
            start_time, end_time, is_day
        """
        # Approximate sunrise/sunset using solar declination
        T = self.centuries_since_j2000(dt)
        earth_elem = PLANETS["Earth"]
        L_sun = _normalize_angle(
            earth_elem["L"][0] + earth_elem["L"][1] * T + 180.0
        )
        # Solar declination (simplified)
        obliquity = OBLIQUITY_J2000 - 0.013004 * T
        decl = _rad_to_deg(
            math.asin(math.sin(_deg_to_rad(obliquity)) * math.sin(_deg_to_rad(L_sun)))
        )

        lat_rad = _deg_to_rad(latitude)
        decl_rad = _deg_to_rad(decl)

        # Hour angle at sunrise/sunset
        cos_ha = -math.tan(lat_rad) * math.tan(decl_rad)
        cos_ha = max(-1.0, min(1.0, cos_ha))
        ha = _rad_to_deg(math.acos(cos_ha))

        # Day length in hours
        day_length = 2.0 * ha / 15.0
        night_length = 24.0 - day_length

        # Solar noon (approximate)
        noon_offset = -longitude / 15.0  # hours from UTC noon
        sunrise_utc = 12.0 + noon_offset - day_length / 2.0
        sunset_utc = 12.0 + noon_offset + day_length / 2.0

        # Planetary hour duration
        day_hour = day_length / 12.0
        night_hour = night_length / 12.0

        # Day ruler determines the starting planet
        # Python weekday: Monday=0, Sunday=6
        # Traditional: Sunday=0, Monday=1, ...
        py_weekday = dt.weekday()
        # Convert: Monday(0)->1, Tuesday(1)->2, ..., Sunday(6)->0
        trad_weekday = (py_weekday + 1) % 7
        day_ruler_names = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
        ruler = day_ruler_names[trad_weekday]

        # Find starting index in Chaldean order
        chaldean_idx = CHALDEAN_ORDER.index(ruler)

        hours = []
        for h in range(24):
            planet_idx = (chaldean_idx + h) % 7
            planet = CHALDEAN_ORDER[planet_idx]
            is_day = h < 12

            if is_day:
                start = sunrise_utc + h * day_hour
                end = start + day_hour
            else:
                start = sunset_utc + (h - 12) * night_hour
                end = start + night_hour

            # Convert decimal hours to HH:MM
            def fmt(hrs: float) -> str:
                hrs = hrs % 24.0
                hh = int(hrs)
                mm = int((hrs - hh) * 60)
                return f"{hh:02d}:{mm:02d}"

            hours.append({
                "hour_number": h + 1,
                "planet": planet,
                "start_time": fmt(start),
                "end_time": fmt(end),
                "is_day": is_day,
            })

        return hours

    # ── Void-of-course Moon ────────────────────────────────────────────

    def is_void_of_course(self, dt: date) -> dict[str, Any]:
        """Determine if the Moon is void-of-course on the given date.

        The Moon is void-of-course when it makes no major aspects to any
        planet before leaving its current sign.

        Returns:
            dict with keys: is_void, current_sign, moon_longitude,
            next_sign_entry (date), last_aspect (dict or None)
        """
        moon_pos = self.compute_position("Moon", dt)
        moon_lon = moon_pos["geocentric_longitude"]
        current_sign = moon_pos["zodiac_sign"]
        sign_idx = ZODIAC_SIGNS.index(current_sign)

        # Degrees remaining in current sign
        deg_remaining = 30.0 - moon_pos["zodiac_degree"]

        # Moon moves ~13.2 degrees/day, so time to leave sign
        days_to_exit = deg_remaining / 13.2

        # Check if Moon makes any major aspect before leaving sign
        # Step through in 0.25-day increments
        last_aspect = None
        makes_aspect = False
        step = 0.25  # quarter-day steps
        check_dt = dt

        steps = int(days_to_exit / step) + 1
        for s in range(steps + 1):
            check_date = dt + timedelta(days=s * step)
            # We can only use whole dates for our computation
            check_date_rounded = date(
                check_date.year, check_date.month, check_date.day
            )
            if check_date_rounded == dt:
                continue

            moon_check = self.compute_position("Moon", check_date_rounded)
            # If Moon has changed sign, stop
            if moon_check["zodiac_sign"] != current_sign:
                break

            # Check aspects to all planets
            moon_check_lon = moon_check["geocentric_longitude"]
            all_pos = self.compute_all_positions(check_date_rounded)

            for p_name in COMPUTE_PLANETS:
                p_lon = all_pos[p_name]["geocentric_longitude"]
                sep = _angular_separation(moon_check_lon, p_lon)
                for asp_name, asp_def in ASPECTS.items():
                    if abs(sep - asp_def["angle"]) <= 1.0:  # tight orb
                        makes_aspect = True
                        last_aspect = {
                            "planet": p_name,
                            "aspect": asp_name,
                            "date": check_date_rounded.isoformat(),
                        }

        # Estimate next sign entry date
        next_sign_date = dt + timedelta(days=days_to_exit)

        return {
            "is_void": not makes_aspect,
            "current_sign": current_sign,
            "moon_longitude": round(moon_lon, 4),
            "next_sign_entry": next_sign_date.isoformat(),
            "last_aspect": last_aspect,
            "degrees_remaining_in_sign": round(deg_remaining, 4),
        }

    # ── Solar return ───────────────────────────────────────────────────

    def compute_solar_return(self, birth_date: date, year: int) -> date:
        """Compute the solar return date for a given year.

        The solar return is when the Sun returns to its exact natal longitude.

        Parameters:
            birth_date: Birth date (used to find natal Sun longitude).
            year: Year for which to compute the return.

        Returns:
            Approximate date of the solar return.
        """
        # Get natal Sun longitude (Sun = Earth's helio longitude + 180)
        T_birth = self.centuries_since_j2000(birth_date)
        e_lon, _, _ = self._heliocentric_position("Earth", T_birth)
        natal_sun_lon = _normalize_angle(e_lon + 180.0)

        # Start searching from birth_date's month/day in the target year
        search_start = date(year, birth_date.month, max(birth_date.day - 2, 1))

        best_date = search_start
        best_diff = 999.0

        # Search over a 5-day window with fine granularity
        for day_offset in range(5):
            check = search_start + timedelta(days=day_offset)
            T_check = self.centuries_since_j2000(check)
            e_check, _, _ = self._heliocentric_position("Earth", T_check)
            sun_check = _normalize_angle(e_check + 180.0)
            diff = _angular_separation(sun_check, natal_sun_lon)
            if diff < best_diff:
                best_diff = diff
                best_date = check

        return best_date

    # ── Full ephemeris ─────────────────────────────────────────────────

    def get_full_ephemeris(self, dt: date) -> dict[str, Any]:
        """Compute everything: positions, aspects, lunar, nakshatra, void-of-course.

        This is the master method that returns the complete ephemeris snapshot.

        Parameters:
            dt: Date to compute.

        Returns:
            dict with keys: date, positions, aspects, lunar_phase, nakshatra,
            void_of_course, retrograde_planets, summary
        """
        positions = self.compute_all_positions(dt)
        aspects = self.compute_aspects(dt)
        lunar = self.compute_lunar_phase(dt)
        nakshatra = self.compute_nakshatra(dt)
        voc = self.is_void_of_course(dt)

        # Collect retrograde planets
        retrogrades = [
            name for name, pos in positions.items()
            if pos.get("is_retrograde") and name not in ("Rahu", "Ketu")
        ]

        # Summary line
        summary_parts = []
        for name, pos in positions.items():
            r = " (R)" if pos.get("is_retrograde") and name not in ("Rahu", "Ketu") else ""
            summary_parts.append(
                f"{name}: {pos['zodiac_sign']} {pos['zodiac_degree']:.1f}{r}"
            )

        return {
            "date": dt.isoformat(),
            "positions": positions,
            "aspects": aspects,
            "lunar_phase": lunar,
            "nakshatra": nakshatra,
            "void_of_course": voc,
            "retrograde_planets": retrogrades,
            "summary": summary_parts,
        }


# ============================================================================
# MODULE-LEVEL CONVENIENCE
# ============================================================================

def get_ephemeris(dt: date | None = None) -> dict[str, Any]:
    """Convenience function: get full ephemeris for a date (default: today)."""
    if dt is None:
        dt = date.today()
    return Ephemeris().get_full_ephemeris(dt)
