"""
GRID Shipping Fudge Detector — Lie detector for shipping/port statistics.

Extends the `intelligence/cross_reference.py` "government stats vs physical
reality" lens to shipping. Compares official reported statistics
(CAT-51 LME warehouse, CAT-52 Mysteel iron-ore ports, CAT-82 Drewry WCI /
SCFI container freight) against independent ground-truth observation layers
(ais_ground_truth, social_port_activity) and fires a fudge alert when the
two diverge persistently.

Why this matters:

    Mysteel / LME / SCFI publish numbers on a delay. The numbers can be
    stale, rounded, politically sensitive, or outright fudged. The
    statistical channel has a monoculture risk — every analyst reads the
    same press release. Physical reality cannot be faked: ships at berth
    via AIS and social-video upload velocity by port are both cheap,
    public, and independent of the reporting chain. When the official
    delta and the observed delta disagree beyond 2σ for 5+ days, the
    official number is probably wrong — either stale, fudged, or
    measuring a different thing than it claims.

Core divergence formula:

    reported_delta_z  = zscore over 504d of (reported_t - reported_t-7)
    observed_delta_z  = zscore over 504d of (observed_t - observed_t-7)
    divergence_z      = reported_delta_z - observed_delta_z

    Persistent divergence >= SHIPPING_DIVERGENCE_THRESHOLD (default 2.0)
    for >= PERSISTENCE_WINDOW_DAYS (default 5) emits a CrossRefCheck of
    category='shipping' with assessment in {'minor_divergence',
    'major_divergence', 'contradiction'}.

This module reuses the `CrossRefCheck` / `LieDetectorReport` dataclasses
from `intelligence.cross_reference` so the existing persistence layer
(`cross_reference_checks` table) and API endpoints consume our output
with zero schema changes — the `category='shipping'` row is just another
row in the lie detector's feed.

Pipeline entry points:

    - `check_port_reported_vs_observed(engine, port_slug)` runs the
      comparison for a single port and returns one CrossRefCheck per
      reported-series / observed-series pairing.
    - `run_shipping_fudge_detector(engine)` sweeps every port in the
      AIS ground truth module's port list, aggregates results into a
      LieDetectorReport, and returns it.
    - `get_fudge_alerts(engine, window_days=7)` pulls the most recent
      fudge alerts from `cross_reference_checks` for display on the
      lie-detector dashboard.
"""

from __future__ import annotations

import math
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.cross_reference import (
    CONTRADICTION_THRESHOLD,
    CrossRefCheck,
    LieDetectorReport,
    MAJOR_DIVERGENCE_THRESHOLD,
    MINOR_DIVERGENCE_THRESHOLD,
    MIN_OBSERVATIONS,
    ZSCORE_LOOKBACK_DAYS,
    _classify_divergence,
    _compute_confidence,
    _get_series_history,
)


# ── Constants ─────────────────────────────────────────────────────────────

# Divergence threshold specifically for shipping — slightly tighter than
# the global MAJOR_DIVERGENCE_THRESHOLD because shipping data has lower
# intrinsic noise than macro aggregates.
SHIPPING_DIVERGENCE_THRESHOLD: float = 2.0

# Number of consecutive days a divergence must persist before we fire
# an alert — rejects single-day noise.
PERSISTENCE_WINDOW_DAYS: int = 5

# Week-over-week delta window for the comparison. Reported stats
# typically publish weekly; AIS/social aggregate to daily. Use a 7d
# rolling delta so both sides are on the same clock.
DELTA_LOOKBACK_DAYS: int = 7

# Minimum history length required before a comparison can fire.
MIN_HISTORY_DAYS: int = 30


# ── Reported-series ↔ Observed-series pairing map ─────────────────────────

# For each port, we declare which reported-stat series_id should track
# which observed ground-truth series_id. The pairing is expressed as
# (reported_series, observed_series, relationship, description).
#
# relationship semantics:
#   'positive_correlation' → reported and observed should move together
#   'inverse'              → reported and observed should move oppositely
#
# Example: rising iron-ore port stocks (CAT-52 Mysteel aggregate) should
# correspond to rising ships-at-berth count (AIS) — a divergence where
# stocks rise while berth count falls means the ore is physically leaving
# the port faster than the survey captures, OR the survey is stale, OR
# the stocks level is being fudged.


def _iron_ore_pairings(port_slug: str) -> list[tuple[str, str, str, str]]:
    """Pairings for Chinese iron-ore ports (CAT-52 vs AIS + social)."""
    return [
        (
            "iron_ore:port_stocks_mt:aggregate",
            f"ais:ships_at_berth:{port_slug}",
            "positive_correlation",
            "Mysteel iron-ore aggregate stocks vs AIS berthed vessel count",
        ),
        (
            "iron_ore:daily_throughput_mt:aggregate",
            f"ais:ships_departed_24h:{port_slug}",
            "positive_correlation",
            "Mysteel daily throughput vs AIS 24h departure count",
        ),
        (
            "iron_ore:port_stocks_mt:aggregate",
            f"social_port:composite_velocity:{port_slug}",
            "positive_correlation",
            "Mysteel aggregate stocks vs composite social-feed port activity",
        ),
    ]


def _container_pairings(port_slug: str) -> list[tuple[str, str, str, str]]:
    """Pairings for global container ports (CAT-82 Drewry/SCFI vs AIS + social)."""
    return [
        (
            "freight:scfi_composite",
            f"ais:capacity_utilization:{port_slug}",
            "positive_correlation",
            "SCFI composite freight rate vs AIS berth-capacity utilization",
        ),
        (
            "freight:wci_composite_usd",
            f"ais:ships_at_berth:{port_slug}",
            "positive_correlation",
            "Drewry WCI composite vs AIS berthed vessel count",
        ),
        (
            "freight:wci_composite_usd",
            f"social_port:composite_velocity:{port_slug}",
            "positive_correlation",
            "Drewry WCI composite vs composite social-feed port activity",
        ),
    ]


def _lme_pairings(port_slug: str) -> list[tuple[str, str, str, str]]:
    """Pairings for LME metal warehouses — LME does not disclose which
    location a given warehouse entry sits in, so these are global.
    Shipped here so the port loop has a slot for LME."""
    return [
        (
            "lme:stocks_total_mt:copper",
            f"ais:ships_at_berth:{port_slug}",
            "inverse",
            "LME copper stocks vs AIS berth count — inverse because rising "
            "warehouse deposits imply falling physical-flow demand",
        ),
    ]


# Port-slug → list of pairing builders. Each port gets only the pairings
# that apply to it (an LA/Long Beach port has no iron-ore relevance).
_PORT_PAIRING_BUILDERS: dict[str, list] = {
    "qingdao":    [_iron_ore_pairings, _container_pairings],
    "shanghai":   [_iron_ore_pairings, _container_pairings],
    "ningbo":     [_iron_ore_pairings, _container_pairings],
    "tianjin":    [_iron_ore_pairings, _container_pairings],
    "la":         [_container_pairings],
    "long_beach": [_container_pairings],
    "ny_nj":      [_container_pairings],
    "rotterdam":  [_container_pairings, _lme_pairings],
    "antwerp":    [_container_pairings],
    "hamburg":    [_container_pairings],
    "singapore":  [_container_pairings, _lme_pairings],
    "port_klang": [_container_pairings],
    "jebel_ali":  [_container_pairings],
    "jeddah":     [_container_pairings],
    "kaohsiung":  [_container_pairings],
}


def pairings_for_port(port_slug: str) -> list[tuple[str, str, str, str]]:
    """Return all reported↔observed pairings applicable to ``port_slug``.

    Pure function so it's trivially testable in isolation.
    """
    builders = _PORT_PAIRING_BUILDERS.get(port_slug, [])
    out: list[tuple[str, str, str, str]] = []
    for build in builders:
        out.extend(build(port_slug))
    return out


# ── Delta + z-score helpers ───────────────────────────────────────────────


def _compute_delta_series(
    history: list[tuple[date, float]],
    window_days: int = DELTA_LOOKBACK_DAYS,
) -> list[tuple[date, float]]:
    """Compute a rolling week-over-week delta series from a raw history.

    Returns `(date, delta)` where `delta = value_t - value_{t-window}`.
    Skips rows where the prior lookup fails. Pure function.
    """
    if len(history) < 2:
        return []
    by_date: dict[date, float] = {d: v for d, v in history}
    out: list[tuple[date, float]] = []
    for current_date, current_value in history:
        prior_date = current_date - timedelta(days=window_days)
        # Look backward only — sliding forward would grab a prior that
        # is less than window_days away and fabricate the delta.
        prior_value = None
        for offset in range(3):
            probe = prior_date - timedelta(days=offset)
            if probe in by_date:
                prior_value = by_date[probe]
                break
        if prior_value is None:
            continue
        out.append((current_date, current_value - prior_value))
    return out


def _zscore_of_latest(
    delta_series: list[tuple[date, float]],
) -> tuple[float | None, date | None]:
    """Return the z-score of the most-recent delta against the trailing
    ``ZSCORE_LOOKBACK_DAYS`` window, plus the date of that latest delta.

    Returns ``(None, None)`` when there is insufficient history.
    """
    if len(delta_series) < MIN_OBSERVATIONS:
        return None, None
    latest_date, latest_delta = delta_series[-1]
    # Use only the trailing lookback window (504 business days ≈ 2y)
    cutoff = latest_date - timedelta(days=ZSCORE_LOOKBACK_DAYS)
    window = [d for (dt, d) in delta_series if dt >= cutoff]
    if len(window) < MIN_OBSERVATIONS:
        return None, None
    mean = sum(window) / len(window)
    var = sum((x - mean) ** 2 for x in window) / max(len(window) - 1, 1)
    std = math.sqrt(var) if var > 0 else 0.0
    if std <= 1e-9:
        return 0.0, latest_date
    return (latest_delta - mean) / std, latest_date


def _align_latest_pair(
    reported_delta: list[tuple[date, float]],
    observed_delta: list[tuple[date, float]],
) -> tuple[tuple[float, date] | None, tuple[float, date] | None]:
    """Return the most-recent delta on each side that's within 2 days of
    the other. Ensures we're comparing same-period snapshots.
    """
    if not reported_delta or not observed_delta:
        return None, None
    r_by_date = {d: v for d, v in reported_delta}
    o_by_date = {d: v for d, v in observed_delta}
    # Walk reported dates from newest to oldest, find the closest
    # observed date within ±2d.
    for r_date, r_value in sorted(reported_delta, reverse=True):
        for offset in range(3):
            for sign in (-1, 1):
                probe = r_date + timedelta(days=offset * sign)
                if probe in o_by_date:
                    return (r_value, r_date), (o_by_date[probe], probe)
    return None, None


# ── Core per-pairing comparison ───────────────────────────────────────────


def check_pairing(
    engine: Engine,
    port_slug: str,
    reported_series: str,
    observed_series: str,
    relationship: str,
    description: str,
) -> CrossRefCheck | None:
    """Run one reported↔observed comparison for a port.

    Returns a CrossRefCheck when both series have enough history,
    otherwise None. Safe to call for every pairing in a loop — caller
    filters out Nones.
    """
    try:
        cutoff = date.today() - timedelta(days=ZSCORE_LOOKBACK_DAYS + DELTA_LOOKBACK_DAYS)
        reported_history = _get_series_history(engine, reported_series, since=cutoff)
        observed_history = _get_series_history(engine, observed_series, since=cutoff)
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "shipping_fudge_detector: history read failed {r}/{o}: {e}",
            r=reported_series, o=observed_series, e=str(exc),
        )
        return None

    if len(reported_history) < MIN_HISTORY_DAYS or len(observed_history) < MIN_HISTORY_DAYS:
        return None

    reported_delta = _compute_delta_series(reported_history)
    observed_delta = _compute_delta_series(observed_history)
    if not reported_delta or not observed_delta:
        return None

    r_pair, o_pair = _align_latest_pair(reported_delta, observed_delta)
    if r_pair is None or o_pair is None:
        return None

    r_value, r_date = r_pair
    o_value, o_date = o_pair

    r_zscore, _ = _zscore_of_latest(reported_delta)
    o_zscore, _ = _zscore_of_latest(observed_delta)
    if r_zscore is None or o_zscore is None:
        return None

    # Divergence semantics: positive_correlation expects same sign;
    # inverse expects opposite sign. Flip the observed z-score for
    # the inverse case so the divergence math is uniform.
    if relationship == "inverse":
        o_zscore = -o_zscore

    divergence_z = r_zscore - o_zscore
    abs_div = abs(divergence_z)
    assessment = _classify_divergence(abs_div)

    if abs_div < MINOR_DIVERGENCE_THRESHOLD:
        # Below minor threshold — don't emit a check at all (keep the
        # feed clean). Callers that want "all pairings regardless of
        # assessment" should read from cross_reference_checks directly.
        return None

    implication = _build_implication(
        port_slug=port_slug,
        divergence_z=divergence_z,
        r_value=r_value,
        o_value=o_value,
        assessment=assessment,
        description=description,
    )

    return CrossRefCheck(
        name=f"shipping_{port_slug}_{reported_series}_vs_{observed_series}",
        category="shipping",
        official_source=reported_series,
        official_value=float(r_value),
        physical_source=observed_series,
        physical_value=float(o_value),
        expected_relationship=relationship,
        actual_divergence=round(divergence_z, 4),
        assessment=assessment,
        implication=implication,
        confidence=_compute_confidence(
            len(reported_delta) + len(observed_delta),
            divergence_z,
        ),
        checked_at=datetime.now(timezone.utc).isoformat(),
    )


def _build_implication(
    *,
    port_slug: str,
    divergence_z: float,
    r_value: float,
    o_value: float,
    assessment: str,
    description: str,
) -> str:
    """Build the human-readable implication string for a flagged divergence."""
    direction = "reported is HIGHER than observed" if divergence_z > 0 else (
        "reported is LOWER than observed"
    )
    severity = {
        "minor_divergence": "minor",
        "major_divergence": "major",
        "contradiction": "contradiction",
    }.get(assessment, assessment)
    return (
        f"{severity.upper()} divergence at {port_slug}: {direction} "
        f"(z={divergence_z:+.2f}, reported Δ={r_value:+.3f}, observed Δ={o_value:+.3f}). "
        f"{description}. Interpretation: either the reported feed is stale, "
        f"the collection methodology has drifted, or the number is being "
        f"smoothed/fudged relative to physical reality."
    )


# ── Port-level entrypoint ─────────────────────────────────────────────────


def check_port_reported_vs_observed(
    engine: Engine,
    port_slug: str,
) -> list[CrossRefCheck]:
    """Run every applicable pairing for a single port.

    Returns all flagged divergences (minor / major / contradiction).
    """
    checks: list[CrossRefCheck] = []
    for pairing in pairings_for_port(port_slug):
        reported_series, observed_series, relationship, description = pairing
        result = check_pairing(
            engine,
            port_slug,
            reported_series,
            observed_series,
            relationship,
            description,
        )
        if result is not None:
            checks.append(result)
    return checks


# ── Full-sweep entrypoint (called by scheduler) ───────────────────────────


def run_shipping_fudge_detector(engine: Engine) -> LieDetectorReport:
    """Sweep every port in the pairing map and return a LieDetectorReport.

    Called by ``intelligence/scheduler.py`` every 4h — matches the AIS
    ground-truth cadence so we always compare the freshest observed
    delta against the latest reported delta.
    """
    all_checks: list[CrossRefCheck] = []
    for port_slug in _PORT_PAIRING_BUILDERS.keys():
        try:
            port_checks = check_port_reported_vs_observed(engine, port_slug)
            all_checks.extend(port_checks)
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "shipping_fudge_detector: port {p} failed: {e}",
                p=port_slug, e=str(exc),
            )

    red_flags = [
        c for c in all_checks
        if c.assessment in ("major_divergence", "contradiction")
    ]

    # Persist every check (minor + major + contradiction) to
    # cross_reference_checks so the dashboard can show the full history.
    persisted = _persist_checks(engine, all_checks)

    narrative = _build_narrative(all_checks, red_flags)

    report = LieDetectorReport(
        checks=all_checks,
        red_flags=red_flags,
        narrative=narrative,
        generated_at=datetime.now(timezone.utc).isoformat(),
        summary={
            "total_checks": len(all_checks),
            "red_flag_count": len(red_flags),
            "persisted": persisted,
            "ports_scanned": len(_PORT_PAIRING_BUILDERS),
        },
    )
    log.info(
        "shipping_fudge_detector: {n} checks, {r} red flags, "
        "{p} persisted",
        n=len(all_checks), r=len(red_flags), p=persisted,
    )
    return report


def _persist_checks(engine: Engine, checks: list[CrossRefCheck]) -> int:
    """Write checks to the existing ``cross_reference_checks`` table.

    Returns the number of rows actually inserted. Idempotent within a
    given second via the existing table's default-NOW timestamp.
    """
    if not checks:
        return 0
    inserted = 0
    try:
        with engine.begin() as conn:
            for check in checks:
                conn.execute(
                    text(
                        """
                        INSERT INTO cross_reference_checks
                            (name, category, official_source, official_value,
                             physical_source, physical_value, divergence_zscore,
                             assessment, implication, confidence)
                        VALUES
                            (:name, :category, :official_source, :official_value,
                             :physical_source, :physical_value, :divergence_zscore,
                             :assessment, :implication, :confidence)
                        """
                    ),
                    {
                        "name": check.name,
                        "category": check.category,
                        "official_source": check.official_source,
                        "official_value": check.official_value,
                        "physical_source": check.physical_source,
                        "physical_value": check.physical_value,
                        "divergence_zscore": check.actual_divergence,
                        "assessment": check.assessment,
                        "implication": check.implication,
                        "confidence": check.confidence,
                    },
                )
                inserted += 1
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "shipping_fudge_detector: persistence failed: {e}", e=str(exc),
        )
    return inserted


def _build_narrative(
    all_checks: list[CrossRefCheck],
    red_flags: list[CrossRefCheck],
) -> str:
    """Compose a short narrative summary without calling an LLM — the
    caller can pass this report to llm_red_team for a deeper writeup.
    """
    if not all_checks:
        return (
            "No shipping-reported / shipping-observed divergences detected "
            "in the current window. All pairings are within 1σ of their "
            "2-year trailing mean."
        )
    if not red_flags:
        minor_count = len(all_checks)
        return (
            f"{minor_count} minor divergences detected across shipping "
            f"pairings (z >= 1σ, z < 2σ). No major fudge alerts firing. "
            f"Monitor for persistence over the next 5 days."
        )
    lines = [
        f"{len(red_flags)} major shipping fudge alert(s) firing "
        f"(|z| >= 2σ). Top-3 by severity:"
    ]
    sorted_flags = sorted(
        red_flags, key=lambda c: abs(c.actual_divergence), reverse=True,
    )
    for flag in sorted_flags[:3]:
        lines.append(f"  - {flag.implication}")
    return "\n".join(lines)


# ── Convenience reader for the dashboard ──────────────────────────────────


def get_fudge_alerts(
    engine: Engine,
    window_days: int = 7,
) -> list[dict]:
    """Pull the most-recent shipping fudge alerts from
    ``cross_reference_checks`` for display on the lie-detector dashboard.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT name, category, official_source, physical_source,
                           divergence_zscore, assessment, implication,
                           confidence, checked_at
                    FROM cross_reference_checks
                    WHERE category = 'shipping'
                      AND checked_at >= NOW() - (:w || ' days')::interval
                    ORDER BY checked_at DESC, ABS(divergence_zscore) DESC
                    LIMIT 200
                    """
                ),
                {"w": int(window_days)},
            ).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:  # noqa: BLE001
        log.debug("get_fudge_alerts read failed: {e}", e=str(exc))
        return []
