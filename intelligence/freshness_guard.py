"""
GRID — Feature Freshness Guard.

Reusable utility to check whether features have recent data before
running analysis.  Returns a status per feature: FRESH, STALE, or MISSING.

Usage:
    from intelligence.freshness_guard import check_freshness, FreshnessStatus

    statuses = check_freshness(engine, ["vix_spot", "sp500_close"])
    for name, status in statuses.items():
        if status.status != "FRESH":
            log.warning("Feature %s is %s", name, status.status)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.post_query_scanner import FRESHNESS_SLA, DEFAULT_SLA_DAYS


@dataclass(frozen=True)
class FreshnessStatus:
    """Immutable status for a single feature's data freshness."""

    feature_name: str
    status: str  # "FRESH", "STALE", or "MISSING"
    last_date: date | None
    days_old: int | None
    sla_days: int


def check_freshness(
    engine: Engine,
    feature_names: list[str],
) -> dict[str, FreshnessStatus]:
    """Check freshness of multiple features in a single DB round-trip.

    Parameters:
        engine: SQLAlchemy engine.
        feature_names: List of feature_registry.name values to check.

    Returns:
        Dict mapping feature_name -> FreshnessStatus.
    """
    if not feature_names:
        return {}

    today = date.today()
    results: dict[str, FreshnessStatus] = {}

    try:
        with engine.connect() as conn:
            # Batch query: get latest obs_date per feature
            placeholders = ", ".join(
                [f":f{i}" for i in range(len(feature_names))]
            )
            params = {f"f{i}": n for i, n in enumerate(feature_names)}

            rows = conn.execute(text(f"""
                SELECT fr.name, fr.family, MAX(rs.obs_date) as latest
                FROM feature_registry fr
                LEFT JOIN resolved_series rs
                    ON rs.feature_id = fr.id AND rs.value IS NOT NULL
                WHERE fr.name IN ({placeholders})
                GROUP BY fr.name, fr.family
            """), params).fetchall()

            found_names = set()
            for row in rows:
                name = row[0]
                family = row[1] or ""
                latest = row[2]
                found_names.add(name)

                sla = FRESHNESS_SLA.get(family, DEFAULT_SLA_DAYS)

                if latest is None:
                    results[name] = FreshnessStatus(
                        feature_name=name,
                        status="MISSING",
                        last_date=None,
                        days_old=None,
                        sla_days=sla,
                    )
                else:
                    days_old = (today - latest).days
                    status = "STALE" if days_old > sla else "FRESH"
                    results[name] = FreshnessStatus(
                        feature_name=name,
                        status=status,
                        last_date=latest,
                        days_old=days_old,
                        sla_days=sla,
                    )

            # Features not found in registry at all
            for name in feature_names:
                if name not in found_names:
                    results[name] = FreshnessStatus(
                        feature_name=name,
                        status="MISSING",
                        last_date=None,
                        days_old=None,
                        sla_days=DEFAULT_SLA_DAYS,
                    )

    except Exception as exc:
        log.error("freshness_guard check failed: {e}", e=str(exc))
        # Return MISSING for all on DB error — never block
        for name in feature_names:
            if name not in results:
                results[name] = FreshnessStatus(
                    feature_name=name,
                    status="MISSING",
                    last_date=None,
                    days_old=None,
                    sla_days=DEFAULT_SLA_DAYS,
                )

    return results


def log_stale_features(
    statuses: dict[str, FreshnessStatus],
    caller: str = "",
) -> None:
    """Log warnings for any non-FRESH features.  Never raises.

    Parameters:
        statuses: Output from check_freshness().
        caller: Name of the calling module for log context.
    """
    prefix = f"[{caller}] " if caller else ""
    for name, fs in statuses.items():
        if fs.status == "STALE":
            log.warning(
                "{pfx}SANITY freshness: {n} is STALE "
                "(last data {d}, {age}d old, SLA={sla}d)",
                pfx=prefix, n=name, d=fs.last_date,
                age=fs.days_old, sla=fs.sla_days,
            )
        elif fs.status == "MISSING":
            log.warning(
                "{pfx}SANITY freshness: {n} is MISSING (no data found)",
                pfx=prefix, n=name,
            )
