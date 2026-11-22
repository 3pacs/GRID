"""Deterministic, data-driven ``feature_registry.model_eligible`` audit: DRY-RUN ONLY.

Reports which ``model_eligible`` flags an explicit rule would change. It never writes:
it reads in a read-only transaction, and it has no apply path. The flag is a global
switch read by the Oracle, inference, discovery, feature selection and the auto_regime
BUY gate (3pacs/GRID#1301). A change ships as its own reviewed migration that records
the pre-image of every row it touches, in the order agreed for #1301: the Oracle
polarity fix, then auto_regime's own bindings and hysteresis (#1307), then eligibility.

The rule replaces hand-set flags (and the retired ``fix_model_eligible.py``
ID-range script, which disabled vix_spot and yld_curve_2s10s).

Two reports, kept apart on purpose:

``audit_feature_eligibility`` -- POINT-IN-TIME HEALTH at ``as_of`` (the decision).
    Every statistic is computed from the cells ``store/pit.py``'s canonical as-of read
    would give for ``as_of``: a ``resolved_series`` row counts only when its
    ``obs_date``, ``release_date`` and ``vintage_date`` are all ``<= as_of`` and its key
    ``(feature_id, obs_date, vintage_date)`` was not retracted by the end of ``as_of``'s
    UTC day (``store.pit.retraction_cutoff``). Retractions are removed *before* the
    vintage pick, so a retracted revision falls back to the earlier vintage and a cell
    whose every vintage is retracted has no value. The latest remaining vintage per
    ``(feature_id, obs_date)`` is the as-of value. A revision, a late row or a
    retraction recorded after ``as_of`` therefore does not change the result.

    A feature is eligible for quantitative models iff ALL of the following hold:
      1. Not deprecated (``deprecated_at IS NULL``) and not in the excluded
         non-economic/astrological names (``ephemeris*``, celestial/calendar).
         ``feature_registry`` is read as stored now: it keeps no as-of history of
         ``model_eligible``, and a feature deprecated now must never be proposed for
         enabling by a migration applied now.
      2. Sufficient history: ``n_obs >= min_obs_for_cadence(cadence)``.
      3. Freshness: ``(as_of - last_obs).days <= 3 * cadence_days``.
      4. Finite, positive variance: more than one distinct value, no NaN/+-inf value
         (or one beyond ``MAX_ABS_VALUE``), and ``value_std`` finite and ``> 0``
         (quarantines constant placeholder feeds such as ``ais_ground_truth``
         reporting identical 1.0/2.0/0.0 values).

``audit_future_dated_rows`` -- CURRENT-STORE CONTAMINATION DIAGNOSTICS (discovery).
    Every ``resolved_series`` row as stored now: all vintages, superseded and retracted
    rows included, no release/vintage fence. It lists rows observed after ``as_of`` and
    rows observed after their own ``vintage_date`` (future-dated when written: year-3018
    FinBERT rows, weather forecasts, 2100 ephemeris, calendars, expiries). It is never
    an input to the health decision; a point-in-time read never serves a row with
    ``obs_date > as_of`` anyway.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine, Row

from store.pit import retraction_cutoff, retractions_table_exists

CADENCE_LIMITS: dict[str, tuple[int, int]] = {
    # cadence -> (min_obs, nominal_period_days)
    "daily": (252, 5),      # 3 * 5 = 15 calendar days tolerance (covers holidays)
    "weekly": (52, 10),     # 3 * 10 = 30 calendar days tolerance
    "monthly": (36, 45),    # 3 * 45 = 135 calendar days tolerance (macro lag)
    "quarterly": (12, 105), # 3 * 105 = 315 calendar days tolerance
}

EXCLUDED_PREFIXES: tuple[str, ...] = (
    "ephemeris",
)

EXCLUDED_NAMES: frozenset[str] = frozenset({
    "days_to_full_moon",
    "days_to_new_moon",
    "jupiter_saturn_angle",
    "lunar_eclipse_proximity",
    "lunar_illumination",
    "lunar_phase",
    "mars_volatility_index",
    "mercury_retrograde",
    "planetary_stress_index",
    "solar_eclipse_proximity",
    "venus_cycle_phase",
    "chinese_element",
    "chinese_lunar_month",
    "chinese_yin_yang",
    "chinese_zodiac_year",
    "dasha_cycle_phase",
    "feng_shui_flying_star",
    "iching_hexagram_of_day",
    "nakshatra_index",
    "nakshatra_quality",
    "rahu_ketu_axis",
    "tithi",
})

# Families whose default cadence is monthly unless overridden
_MONTHLY_FAMILIES: frozenset[str] = frozenset({"macro", "trade"})

#: A value beyond this magnitude counts as non-finite. PostgreSQL's STDDEV_SAMP raises
#: "value out of range: overflow" once squared deviations pass ~1.8e308 (measured on
#: postgres:15: STDDEV_SAMP over (1, 1e200) errors), and one such feed would abort the
#: whole all-feature scan. No economic series is legitimately this large.
MAX_ABS_VALUE = 1e100

#: The canonical as-of read, aggregated per feature. The WHERE clause is store/pit.py's
#: ``get_source_snapshot`` predicate (obs, release and vintage dates fenced to as_of;
#: retractions by ``retraction_cutoff(as_of)`` removed before the pick) and the pick is
#: ``get_pit``'s LATEST_AS_OF ``DISTINCT ON ... ORDER BY vintage_date DESC``.
_HEALTH_SQL = text("""
    WITH as_of_cells AS (
        SELECT DISTINCT ON (rs.feature_id, rs.obs_date)
               rs.feature_id, rs.obs_date, rs.value
        FROM resolved_series rs
        WHERE rs.obs_date <= :as_of_date
          AND rs.release_date <= :as_of_date
          AND rs.vintage_date <= :as_of_date
          AND NOT EXISTS (
              SELECT 1 FROM resolved_series_retractions rr
              WHERE rr.feature_id = rs.feature_id
                AND rr.obs_date = rs.obs_date
                AND rr.vintage_date = rs.vintage_date
                AND rr.retracted_at <= :retraction_cutoff
          )
        ORDER BY rs.feature_id, rs.obs_date, rs.vintage_date DESC
    ),
    agg AS (
        SELECT feature_id,
               COUNT(*) AS n_obs,
               MAX(obs_date) AS last_obs,
               COUNT(DISTINCT value) AS n_distinct,
               COUNT(*) FILTER (WHERE NOT (ABS(value) <= :max_abs)) AS n_non_finite,
               STDDEV_SAMP(value) FILTER (WHERE ABS(value) <= :max_abs) AS value_std
        FROM as_of_cells
        GROUP BY feature_id
    )
    SELECT fr.id, fr.name, fr.family, fr.model_eligible,
           (fr.deprecated_at IS NOT NULL) AS deprecated,
           COALESCE(a.n_obs, 0) AS n_obs,
           a.last_obs,
           COALESCE(a.n_distinct, 0) AS n_distinct,
           COALESCE(a.n_non_finite, 0) AS n_non_finite,
           a.value_std
    FROM feature_registry fr
    LEFT JOIN agg a ON a.feature_id = fr.id
    ORDER BY fr.id
""")

#: Raw rows as stored now: no release/vintage fence, retracted rows included (and
#: counted). Rows observed after as_of, or after their own vintage_date.
_CONTAMINATION_SQL = text("""
    SELECT rs.feature_id, fr.name, fr.model_eligible,
           COUNT(*) FILTER (WHERE rs.obs_date > :as_of_date) AS rows_after_as_of,
           COUNT(DISTINCT rs.obs_date) FILTER (WHERE rs.obs_date > :as_of_date)
               AS obs_dates_after_as_of,
           COUNT(*) FILTER (
               WHERE rs.obs_date > :as_of_date
                 AND rs.release_date <= :as_of_date
                 AND rs.vintage_date <= :as_of_date
           ) AS after_as_of_stored_by_as_of,
           COUNT(*) FILTER (WHERE rs.obs_date > rs.vintage_date) AS observed_after_vintage,
           COUNT(rr.feature_id) AS retracted_rows,
           MIN(rs.obs_date) FILTER (WHERE rs.obs_date > :as_of_date) AS first_obs_after_as_of,
           MAX(rs.obs_date) AS max_obs_date
    FROM resolved_series rs
    JOIN feature_registry fr ON fr.id = rs.feature_id
    LEFT JOIN resolved_series_retractions rr
      ON rr.feature_id = rs.feature_id
     AND rr.obs_date = rs.obs_date
     AND rr.vintage_date = rs.vintage_date
    WHERE rs.obs_date > :as_of_date OR rs.obs_date > rs.vintage_date
    GROUP BY rs.feature_id, fr.name, fr.model_eligible
    ORDER BY rs.feature_id
""")

HEALTH_READ_RULE = (
    "store/pit.py canonical as-of read: obs_date, release_date and vintage_date <= as_of; "
    "rows retracted by the end of as_of's UTC day removed before the vintage pick; "
    "latest remaining vintage per (feature_id, obs_date)"
)
CONTAMINATION_READ_RULE = (
    "current store, every row: all vintages, superseded and retracted rows included, "
    "no release/vintage fence; never an input to the health decision"
)


@dataclass(frozen=True)
class FeatureHealthStats:
    """Point-in-time series statistics for one ``feature_registry`` row at ``as_of``."""

    feature_id: int
    name: str
    family: str
    current_eligible: bool
    deprecated: bool
    n_obs: int
    last_obs: date | None
    n_distinct: int
    value_std: float | None
    #: As-of values that are NaN, +-inf, or beyond ``MAX_ABS_VALUE``.
    n_non_finite: int = 0
    cadence: str = "daily"


@dataclass(frozen=True)
class EligibilityDecision:
    """Result of applying the deterministic eligibility rule to one feature."""

    feature_id: int
    name: str
    eligible: bool
    reasons: tuple[str, ...]
    changed: bool


def infer_cadence(family: str, name: str) -> str:
    """Infer nominal cadence from feature metadata when not explicitly stored."""
    lname = name.lower()
    if any(tok in lname for tok in ("quarterly", "_qoq", "gdp", "tdsp", "delinq")):
        return "quarterly"
    if any(tok in lname for tok in ("weekly", "claims", "walcl", "tga", "cftc", "h8", "eia_")):
        return "weekly"
    if family in _MONTHLY_FAMILIES or any(tok in lname for tok in ("cpi", "pce", "unrate", "payems", "indpro", "pmi")):
        return "monthly"
    return "daily"


def has_finite_positive_variance(stats: FeatureHealthStats) -> bool:
    """True iff every as-of value is finite, >1 is distinct, and ``value_std`` is finite and > 0."""
    std = stats.value_std
    return (
        stats.n_non_finite == 0
        and stats.n_distinct > 1
        and std is not None
        and math.isfinite(std)
        and std > 0.0
    )


def evaluate_feature_eligibility(
    stats: FeatureHealthStats,
    *,
    as_of: date,
) -> EligibilityDecision:
    """Pure eligibility check on point-in-time health — no DB I/O, no contamination input."""
    reasons: list[str] = []
    lname = stats.name.lower()

    if stats.deprecated:
        reasons.append("deprecated")
    if any(lname.startswith(p) for p in EXCLUDED_PREFIXES) or lname in EXCLUDED_NAMES:
        reasons.append("excluded_non_economic_feature")

    min_obs, period_days = CADENCE_LIMITS.get(stats.cadence, CADENCE_LIMITS["daily"])
    if stats.n_obs < min_obs:
        reasons.append(f"insufficient_history({stats.n_obs}<{min_obs})")

    if stats.last_obs is None:
        reasons.append("no_observations")
    else:
        max_age_days = 3 * period_days
        age_days = (as_of - stats.last_obs).days
        if age_days > max_age_days:
            reasons.append(f"stale({age_days}d>{max_age_days}d)")

    if stats.n_obs > 0 and not has_finite_positive_variance(stats):
        std = stats.value_std
        if stats.n_non_finite == 0 and (
            stats.n_distinct <= 1 or (std is not None and math.isfinite(std) and std <= 0.0)
        ):
            reasons.append("zero_variance_placeholder")
        else:
            reasons.append(f"non_finite_variance(std={std!r},non_finite_values={stats.n_non_finite})")

    eligible = len(reasons) == 0
    return EligibilityDecision(
        feature_id=stats.feature_id,
        name=stats.name,
        eligible=eligible,
        reasons=tuple(reasons),
        changed=(eligible != stats.current_eligible),
    )


def _utc_today() -> date:
    """Today's UTC date: the day convention ``retraction_cutoff`` uses."""
    return datetime.now(timezone.utc).date()


def _read_only_rows(engine: Engine, sql: Any, params: dict[str, Any]) -> list[Row]:
    """Run one statement in a read-only transaction; refuse without retraction knowledge."""
    if not retractions_table_exists(engine):
        raise RuntimeError(
            "resolved_series_retractions is missing or unreadable; refusing to audit "
            "without retraction knowledge (store.pit.retractions_table_exists)"
        )
    with engine.connect() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text("SET TRANSACTION READ ONLY"))
        return list(conn.execute(sql, params).fetchall())


def read_feature_health(engine: Engine, *, as_of: date) -> list[FeatureHealthStats]:
    """Point-in-time health statistics for every ``feature_registry`` row at ``as_of``.

    One statement over all of ``resolved_series`` (the same full scan the earlier
    current-store version made, now fenced to ``as_of``), so run it off-hours.
    """
    rows = _read_only_rows(engine, _HEALTH_SQL, {
        "as_of_date": as_of,
        "retraction_cutoff": retraction_cutoff(as_of),
        "max_abs": MAX_ABS_VALUE,
    })
    stats: list[FeatureHealthStats] = []
    for r in rows:
        stats.append(FeatureHealthStats(
            feature_id=int(r[0]),
            name=str(r[1]),
            family=str(r[2] or ""),
            current_eligible=bool(r[3]),
            deprecated=bool(r[4]),
            n_obs=int(r[5]),
            last_obs=r[6],
            n_distinct=int(r[7]),
            n_non_finite=int(r[8]),
            value_std=float(r[9]) if r[9] is not None else None,
            cadence=infer_cadence(str(r[2] or ""), str(r[1])),
        ))
    return stats


def audit_feature_eligibility(
    engine: Engine,
    *,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Point-in-time eligibility health at ``as_of``; never writes.

    Returns the flags the rule would turn on and off; applying them is a migration's
    job. Future-dated rows are not part of this report: see ``audit_future_dated_rows``.
    """
    ref_date = as_of or _utc_today()
    decisions = [
        evaluate_feature_eligibility(st, as_of=ref_date)
        for st in read_feature_health(engine, as_of=ref_date)
    ]
    to_enable = [d for d in decisions if d.changed and d.eligible]
    to_disable = [d for d in decisions if d.changed and not d.eligible]

    return {
        "section": "point_in_time_health",
        "as_of": ref_date.isoformat(),
        "applied": False,
        "read_rule": HEALTH_READ_RULE,
        "registry_read": "current (feature_registry keeps no as-of history of model_eligible)",
        "total": len(decisions),
        "eligible_count": sum(1 for d in decisions if d.eligible),
        "to_enable": [{"id": d.feature_id, "name": d.name} for d in to_enable],
        "to_disable": [
            {"id": d.feature_id, "name": d.name, "reasons": list(d.reasons)}
            for d in to_disable
        ],
    }


def audit_future_dated_rows(
    engine: Engine,
    *,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Current-store contamination diagnostics; never writes, never feeds the health decision.

    Per feature with any hit, over every stored row (all vintages, retracted included):

    * ``rows_after_as_of`` / ``obs_dates_after_as_of``: rows observed after ``as_of``;
      with a past ``as_of`` this includes ordinary later data;
    * ``after_as_of_stored_by_as_of``: of those, rows whose release and vintage dates
      are ``<= as_of`` (the store already held a later observation at ``as_of``);
    * ``observed_after_vintage``: rows whose ``obs_date`` is after their own
      ``vintage_date`` (future-dated when written), independent of ``as_of``;
    * ``retracted_rows``: listed rows that carry a retraction (any ``retracted_at``).
    """
    ref_date = as_of or _utc_today()
    rows = _read_only_rows(engine, _CONTAMINATION_SQL, {"as_of_date": ref_date})
    features = [
        {
            "id": int(r[0]),
            "name": str(r[1]),
            "current_eligible": bool(r[2]),
            "rows_after_as_of": int(r[3]),
            "obs_dates_after_as_of": int(r[4]),
            "after_as_of_stored_by_as_of": int(r[5]),
            "observed_after_vintage": int(r[6]),
            "retracted_rows": int(r[7]),
            "first_obs_after_as_of": r[8].isoformat() if r[8] is not None else None,
            "max_obs_date": r[9].isoformat() if r[9] is not None else None,
        }
        for r in rows
    ]
    return {
        "section": "current_store_contamination",
        "as_of": ref_date.isoformat(),
        "applied": False,
        "read_rule": CONTAMINATION_READ_RULE,
        "total_features": len(features),
        "features": features,
    }


if __name__ == "__main__":
    import json

    from db import get_engine

    parser = argparse.ArgumentParser(description="Dry-run audit of feature_registry.model_eligible (never writes)")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None, help="YYYY-MM-DD (default: today, UTC)")
    parser.add_argument("--json", action="store_true", help="print both reports as JSON")
    args = parser.parse_args()
    engine = get_engine()
    health = audit_feature_eligibility(engine, as_of=args.as_of)
    contamination = audit_future_dated_rows(engine, as_of=args.as_of)
    if args.json:
        print(json.dumps({"health": health, "contamination": contamination}, indent=2, sort_keys=True))
    else:
        log.info("Point-in-time health (dry run): {s}",
                 s={k: v for k, v in health.items() if k not in ("to_enable", "to_disable")})
        log.info("Would enable: {n}, would disable: {m}", n=len(health["to_enable"]), m=len(health["to_disable"]))
        log.info("Current-store contamination (separate, not part of the decision): {n} features "
                 "with future-dated rows", n=contamination["total_features"])
