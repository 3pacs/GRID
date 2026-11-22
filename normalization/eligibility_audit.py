"""Deterministic, data-driven ``feature_registry.model_eligible`` audit: DRY-RUN ONLY.

Reports which ``model_eligible`` flags an explicit rule would change. It never writes:
it reads in a read-only transaction, and it has no apply path. The flag is a global
switch read by the Oracle, inference, discovery, feature selection and the auto_regime
BUY gate (3pacs/GRID#1301). A change ships as its own reviewed migration that records
the pre-image of every row it touches, in the order agreed for #1301: the Oracle
polarity fix, then auto_regime's own bindings and hysteresis (#1307), then eligibility.

The rule replaces hand-set flags (and the retired ``fix_model_eligible.py``
ID-range script, which disabled vix_spot and yld_curve_2s10s):

A feature is eligible for quantitative models iff ALL of the following hold:
  1. Not deprecated (``deprecated_at IS NULL``) and not in the excluded
     non-economic/astrological prefixes (``ephemeris``, celestial/calendar).
  2. Sufficient non-null history: ``n_obs >= min_obs_for_cadence(cadence)``.
  3. Freshness: ``(as_of - last_obs).days <= 3 * cadence_days``.
  4. No future-dated contamination: ``max_obs <= as_of`` (prevents year-3018
     FinBERT rows, future weather forecasts, or 2100 ephemeris from entering).
  5. Non-degenerate variance: ``distinct_values > 1`` and ``value_std > 0``
     (automatically quarantines constant placeholder feeds such as
     ``ais_ground_truth`` reporting identical 1.0/2.0/0.0 values).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

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


@dataclass(frozen=True)
class FeatureHealthStats:
    """Observed series statistics for one ``feature_registry`` row."""

    feature_id: int
    name: str
    family: str
    current_eligible: bool
    deprecated: bool
    n_obs: int
    last_obs: date | None
    max_obs: date | None
    n_distinct: int
    value_std: float | None
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


def evaluate_feature_eligibility(
    stats: FeatureHealthStats,
    *,
    as_of: date,
) -> EligibilityDecision:
    """Pure eligibility check — no DB I/O."""
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

    if stats.max_obs is not None and stats.max_obs > as_of:
        reasons.append(f"future_dated({stats.max_obs.isoformat()}>{as_of.isoformat()})")

    if stats.n_obs > 0 and (stats.n_distinct <= 1 or (stats.value_std is not None and stats.value_std <= 0.0)):
        reasons.append("zero_variance_placeholder")

    eligible = len(reasons) == 0
    return EligibilityDecision(
        feature_id=stats.feature_id,
        name=stats.name,
        eligible=eligible,
        reasons=tuple(reasons),
        changed=(eligible != stats.current_eligible),
    )


def audit_feature_eligibility(
    engine: Engine,
    *,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Audit every ``feature_registry`` row against ``resolved_series`` health; never writes.

    Scans all of ``resolved_series`` (latest vintage per obs_date), so run it off-hours.
    Returns the flags the rule would turn on and off; applying them is a migration's job.
    """
    ref_date = as_of or date.today()
    sql = text("""
        WITH dedup AS (
            SELECT DISTINCT ON (feature_id, obs_date)
                   feature_id, obs_date, value
            FROM resolved_series
            WHERE value IS NOT NULL
            ORDER BY feature_id, obs_date, vintage_date DESC
        ),
        agg AS (
            SELECT feature_id,
                   COUNT(*) AS n_obs,
                   MAX(obs_date) FILTER (WHERE obs_date <= :as_of) AS last_obs,
                   MAX(obs_date) AS max_obs,
                   COUNT(DISTINCT value) AS n_distinct,
                   STDDEV_SAMP(value) AS value_std
            FROM dedup
            GROUP BY feature_id
        )
        SELECT fr.id, fr.name, fr.family, fr.model_eligible,
               (fr.deprecated_at IS NOT NULL) AS deprecated,
               COALESCE(a.n_obs, 0) AS n_obs,
               a.last_obs, a.max_obs,
               COALESCE(a.n_distinct, 0) AS n_distinct,
               a.value_std
        FROM feature_registry fr
        LEFT JOIN agg a ON a.feature_id = fr.id
        ORDER BY fr.id
    """)

    with engine.connect() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text("SET TRANSACTION READ ONLY"))
        rows = conn.execute(sql, {"as_of": ref_date}).fetchall()

    decisions: list[EligibilityDecision] = []
    for r in rows:
        cadence = infer_cadence(str(r[2] or ""), str(r[1]))
        st = FeatureHealthStats(
            feature_id=int(r[0]),
            name=str(r[1]),
            family=str(r[2] or ""),
            current_eligible=bool(r[3]),
            deprecated=bool(r[4]),
            n_obs=int(r[5]),
            last_obs=r[6],
            max_obs=r[7],
            n_distinct=int(r[8]),
            value_std=float(r[9]) if r[9] is not None else None,
            cadence=cadence,
        )
        decisions.append(evaluate_feature_eligibility(st, as_of=ref_date))

    to_enable = [d for d in decisions if d.changed and d.eligible]
    to_disable = [d for d in decisions if d.changed and not d.eligible]

    return {
        "as_of": ref_date.isoformat(),
        "applied": False,
        "total": len(decisions),
        "eligible_count": sum(1 for d in decisions if d.eligible),
        "to_enable": [{"id": d.feature_id, "name": d.name} for d in to_enable],
        "to_disable": [
            {"id": d.feature_id, "name": d.name, "reasons": list(d.reasons)}
            for d in to_disable
        ],
    }


if __name__ == "__main__":
    import json

    from db import get_engine

    parser = argparse.ArgumentParser(description="Dry-run audit of feature_registry.model_eligible (never writes)")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None, help="YYYY-MM-DD (default: today)")
    parser.add_argument("--json", action="store_true", help="print the full report as JSON")
    args = parser.parse_args()
    summary = audit_feature_eligibility(get_engine(), as_of=args.as_of)
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        log.info("Eligibility audit (dry run): {s}",
                 s={k: v for k, v in summary.items() if k not in ("to_enable", "to_disable")})
        log.info("Would enable: {n}, would disable: {m}", n=len(summary["to_enable"]), m=len(summary["to_disable"]))
