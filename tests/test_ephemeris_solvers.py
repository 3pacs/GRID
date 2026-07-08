"""Reference-value tests for analysis.ephemeris core solver math.

Covers the four pure-math helpers called out in
``docs/PUNCH-LIST-2026-05-13.md`` (auditor 2026-06-21 analysis/, [P1]):
``_solve_kepler``, ``_ecliptic_to_equatorial``, ``_normalize_angle``,
``_angular_separation``. Plus the closely-coupled ``_signed_angular_diff``.

These functions feed every planetary position computation (``compute_position``
→ ``_heliocentric_position`` calls all four), so a closed-form regression
guard here protects every downstream consumer of ``analysis/ephemeris.py``.
"""

from __future__ import annotations

import math

import pytest

from analysis.ephemeris import (
    OBLIQUITY_J2000,
    _angular_separation,
    _ecliptic_to_equatorial,
    _normalize_angle,
    _signed_angular_diff,
    _solve_kepler,
)


# ─────────────────────────────────────────────────────────────────────────
# _normalize_angle — modulo 360, never negative
# ─────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw, expected",
    [
        (0.0, 0.0),
        (90.0, 90.0),
        (360.0, 0.0),
        (720.0, 0.0),
        (361.5, 1.5),
        (-1.0, 359.0),
        (-720.5, 359.5),
    ],
)
def test_normalize_angle_wraps_into_0_360(raw: float, expected: float) -> None:
    assert _normalize_angle(raw) == pytest.approx(expected, abs=1e-9)


# ─────────────────────────────────────────────────────────────────────────
# _solve_kepler — M = E - e * sin(E)
# ─────────────────────────────────────────────────────────────────────────


def _kepler_residual(M_deg: float, e: float, E_deg: float) -> float:
    """Kepler's equation residual in radians (should be ~0 at a valid root)."""
    M_rad = math.radians(M_deg % 360.0)
    E_rad = math.radians(E_deg)
    return E_rad - e * math.sin(E_rad) - M_rad


@pytest.mark.parametrize("M_deg", [0.0, 30.0, 90.0, 180.0, 270.0, 359.0])
def test_solve_kepler_circular_orbit_returns_mean_anomaly(M_deg: float) -> None:
    """When e=0 the Kepler equation collapses to E == M."""
    E = _solve_kepler(M_deg, e=0.0)
    assert E == pytest.approx(M_deg % 360.0, abs=1e-6)


@pytest.mark.parametrize(
    "M_deg, e",
    [
        (0.0, 0.1),
        (45.0, 0.2),
        (90.0, 0.5),
        (135.0, 0.3),
        (200.0, 0.6),
        (270.0, 0.7),
        (350.0, 0.1),
    ],
)
def test_solve_kepler_satisfies_keplers_equation(M_deg: float, e: float) -> None:
    """The returned E must satisfy M = E - e*sin(E) to within solver tol."""
    E = _solve_kepler(M_deg, e)
    assert abs(_kepler_residual(M_deg, e, E)) < 1e-5


def test_solve_kepler_high_eccentricity_branch_converges() -> None:
    """e >= 0.8 takes the ``E = pi`` initial-guess branch; must still converge."""
    M_deg = 10.0
    e = 0.9
    E = _solve_kepler(M_deg, e)
    assert abs(_kepler_residual(M_deg, e, E)) < 1e-5


def test_solve_kepler_accepts_unnormalized_mean_anomaly() -> None:
    """M_deg outside 0-360 is internally normalized before iteration."""
    E_in_range = _solve_kepler(30.0, e=0.2)
    E_wrapped = _solve_kepler(30.0 + 720.0, e=0.2)
    assert E_in_range == pytest.approx(E_wrapped, abs=1e-6)


# ─────────────────────────────────────────────────────────────────────────
# _ecliptic_to_equatorial — rotation about the x-axis by obliquity
# ─────────────────────────────────────────────────────────────────────────


def test_ecliptic_to_equatorial_vernal_equinox_is_origin() -> None:
    """lon=0, lat=0 lies on both the ecliptic and the equator: RA=0, Dec=0."""
    ra, dec = _ecliptic_to_equatorial(0.0, 0.0, OBLIQUITY_J2000)
    assert ra == pytest.approx(0.0, abs=1e-9)
    assert dec == pytest.approx(0.0, abs=1e-9)


def test_ecliptic_to_equatorial_summer_solstice_declination() -> None:
    """Ecliptic (lon=90, lat=0) → equatorial (RA=90, Dec=obliquity)."""
    ra, dec = _ecliptic_to_equatorial(90.0, 0.0, OBLIQUITY_J2000)
    assert ra == pytest.approx(90.0, abs=1e-6)
    assert dec == pytest.approx(OBLIQUITY_J2000, abs=1e-6)


def test_ecliptic_to_equatorial_winter_solstice_declination() -> None:
    """Ecliptic (lon=270, lat=0) → equatorial (RA=270, Dec=-obliquity)."""
    ra, dec = _ecliptic_to_equatorial(270.0, 0.0, OBLIQUITY_J2000)
    assert ra == pytest.approx(270.0, abs=1e-6)
    assert dec == pytest.approx(-OBLIQUITY_J2000, abs=1e-6)


def test_ecliptic_to_equatorial_zero_obliquity_is_identity() -> None:
    """With obliquity=0 the ecliptic and equator coincide."""
    for lon, lat in [(15.0, 5.0), (135.0, -10.0), (300.0, 0.0)]:
        ra, dec = _ecliptic_to_equatorial(lon, lat, 0.0)
        assert ra == pytest.approx(lon, abs=1e-9)
        assert dec == pytest.approx(lat, abs=1e-9)


def test_ecliptic_to_equatorial_ra_is_wrapped_to_0_360() -> None:
    """RA must be normalized to the 0-360 range even when atan2 yields negative."""
    ra, _ = _ecliptic_to_equatorial(200.0, 0.0, OBLIQUITY_J2000)
    assert 0.0 <= ra < 360.0


# ─────────────────────────────────────────────────────────────────────────
# _angular_separation — unsigned 0-180 distance on the great circle
# ─────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "a, b, expected",
    [
        (0.0, 0.0, 0.0),
        (10.0, 10.0, 0.0),
        (0.0, 90.0, 90.0),
        (0.0, 180.0, 180.0),
        (0.0, 270.0, 90.0),      # short way around
        (350.0, 10.0, 20.0),     # wraps across 0
        (45.0, 405.0, 0.0),      # 405 == 45 (mod 360)
        (200.0, 20.0, 180.0),    # exactly half a circle
    ],
)
def test_angular_separation_short_arc(a: float, b: float, expected: float) -> None:
    assert _angular_separation(a, b) == pytest.approx(expected, abs=1e-6)


def test_angular_separation_is_bounded_by_180() -> None:
    """No matter the inputs, the result must lie in [0, 180]."""
    for a, b in [(0.0, 359.999), (123.4, -540.0), (1000.0, -1000.0)]:
        sep = _angular_separation(a, b)
        assert 0.0 <= sep <= 180.0


# ─────────────────────────────────────────────────────────────────────────
# _signed_angular_diff — signed shortest path lon1 - lon2 in [-180, 180]
# ─────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "lon1, lon2, expected",
    [
        (10.0, 350.0, 20.0),    # forward across 0
        (350.0, 10.0, -20.0),   # backward across 0
        (90.0, 0.0, 90.0),
        (0.0, 90.0, -90.0),
        (180.0, 0.0, 180.0),    # boundary: half-circle returns +180
        (0.0, 0.0, 0.0),
    ],
)
def test_signed_angular_diff(lon1: float, lon2: float, expected: float) -> None:
    assert _signed_angular_diff(lon1, lon2) == pytest.approx(expected, abs=1e-6)


def test_signed_angular_diff_is_bounded_by_180() -> None:
    for a, b in [(0.0, 359.999), (10.0, 200.0), (-720.5, 359.2)]:
        diff = _signed_angular_diff(a, b)
        assert -180.0 <= diff <= 180.0
