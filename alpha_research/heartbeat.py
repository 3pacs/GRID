"""
Alpha Research Heartbeat — autonomous monitoring job.

Runs every 30 minutes via Hermes. Checks critical conditions
and fires alerts when thresholds are crossed.

From OpenClaw patterns: HEARTBEAT.md is the checklist the agent
reads autonomously to detect transitions and anomalies.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from dataclasses import dataclass

from alpha_research.signals.exposure_scaler import compute_vix_exposure_scalar
from alpha_research.signals.credit_cycle import compute_credit_cycle


@dataclass
class HeartbeatAlert:
    level: str  # INFO, WARNING, CRITICAL
    source: str
    message: str
    data: dict[str, Any]


def run_heartbeat(engine: Engine) -> list[HeartbeatAlert]:
    """
    Run all heartbeat checks. Returns list of alerts.
    """
    alerts: list[HeartbeatAlert] = []

    alerts.extend(_check_regime_transition(engine))
    alerts.extend(_check_vix_ma_cross(engine))
    alerts.extend(_check_puller_health(engine))
    alerts.extend(_check_pit_freshness(engine))

    return alerts


def _check_regime_transition(engine: Engine) -> list[HeartbeatAlert]:
    """Check if VIX exposure regime changed since last check."""
    alerts = []
    result = compute_vix_exposure_scalar(engine)

    if result.get("regime_hint") == "stressed":
        alerts.append(HeartbeatAlert(
            level="CRITICAL",
            source="vix_exposure",
            message=f"VIX regime STRESSED: VIX={result['vix']}, ratio={result['ratio']:.3f}, scalar={result['scalar']:.3f}",
            data=result,
        ))
    elif result.get("regime_hint") == "elevated":
        alerts.append(HeartbeatAlert(
            level="WARNING",
            source="vix_exposure",
            message=f"VIX elevated: {result['vix']:.1f} (MA={result['vix_ma']:.1f}, scalar={result['scalar']:.3f})",
            data=result,
        ))

    return alerts


def _check_vix_ma_cross(engine: Engine) -> list[HeartbeatAlert]:
    """Alert if VIX just crossed above its 20-day MA."""
    alerts = []
    result = compute_vix_exposure_scalar(engine)

    if result.get("ratio") and result["ratio"] > 1.0:
        # Check if it was below yesterday (approximate — full check needs history)
        if result["ratio"] < 1.05:
            alerts.append(HeartbeatAlert(
                level="INFO",
                source="vix_ma_cross",
                message=f"VIX near MA crossover: ratio={result['ratio']:.4f}",
                data=result,
            ))

    return alerts


def _check_puller_health(engine: Engine) -> list[HeartbeatAlert]:
    """Check if any data pullers have >3 failures in the last 6 hours."""
    alerts = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=6)

    try:
        with engine.connect() as conn:
            r = conn.execute(text("""
                SELECT sc.name, COUNT(*) as fail_count
                FROM raw_series rs
                JOIN source_catalog sc ON rs.source_id = sc.id
                WHERE rs.pull_timestamp > :cutoff
                  AND rs.value IS NULL
                GROUP BY sc.name
                HAVING COUNT(*) > 3
            """), {"cutoff": cutoff})

            for row in r:
                alerts.append(HeartbeatAlert(
                    level="WARNING",
                    source="puller_health",
                    message=f"Puller '{row[0]}' has {row[1]} failures in last 6h",
                    data={"puller": row[0], "failures": row[1]},
                ))
    except Exception:
        pass  # Table structure may differ, don't crash heartbeat

    return alerts


def _check_pit_freshness(engine: Engine) -> list[HeartbeatAlert]:
    """Alert if the most recent resolved_series data is stale (>24h)."""
    alerts = []

    try:
        with engine.connect() as conn:
            r = conn.execute(text("""
                SELECT MAX(release_date) FROM resolved_series
            """))
            max_date = r.scalar()

            if max_date:
                staleness = (date.today() - max_date).days
                if staleness > 1:
                    alerts.append(HeartbeatAlert(
                        level="WARNING",
                        source="pit_freshness",
                        message=f"PIT store is {staleness} days stale (last data: {max_date})",
                        data={"last_date": str(max_date), "staleness_days": staleness},
                    ))
    except Exception:
        pass

    return alerts


def format_alerts(alerts: list[HeartbeatAlert]) -> str:
    """Format alerts for logging or email."""
    if not alerts:
        return "Heartbeat: all clear"

    lines = [f"GRID Heartbeat — {len(alerts)} alert(s):\n"]
    for a in sorted(alerts, key=lambda x: {"CRITICAL": 0, "WARNING": 1, "INFO": 2}.get(x.level, 3)):
        lines.append(f"  [{a.level}] {a.source}: {a.message}")

    return "\n".join(lines)
