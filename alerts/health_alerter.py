"""Health-derived alerting (audit item #31).

Inspects the dict returned by `scripts.hermes_health.check_system_health`
and emits an email alert when a condition crosses a threshold. Each
condition is throttled by a configurable cooldown so a sustained problem
doesn't spam the inbox every cycle.

State (per-condition last-fired timestamp) is persisted as JSON to
`.server-logs/alert_state.json` so cooldowns survive Hermes restarts.

Wire into the Hermes cycle:

    from alerts.health_alerter import check_and_alert
    check_and_alert(health_dict)

The function is best-effort — it never raises. All conditions that fire
return a tuple of (key, subject, body). Callers don't need to inspect.

Conditions covered today:
    - db.unhealthy:        DB connectivity check failed
    - db.failed_pulls:     >50 failed pulls in last 24h
    - db.stale_sources:    >20 sources past freshness threshold
    - hermes.unhealthy:    Hermes process not responsive
    - pool.exhausted:      Active connections > 80% of (pool_size + max_overflow)

New conditions slot in by adding a row to the CHECKS table and a
threshold constant — no code restructuring needed.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log


# ── Tunables ─────────────────────────────────────────────────────────

DEFAULT_COOLDOWN_HOURS = 6
"""How long after firing an alert before the same condition can fire again.
Prevents inbox flooding when a condition stays bad for hours."""

FAILED_PULLS_24H_THRESHOLD = 50
"""Failed-pull count over the last 24h that triggers an alert. Below
this is treated as routine flakiness — many feeds have transient errors."""

STALE_SOURCES_THRESHOLD = 20
"""Number of sources past their freshness window before alerting. Below
this is normal (some sources are weekly/monthly and look stale within
their cycle)."""

POOL_EXHAUSTION_THRESHOLD = 0.8
"""Fraction of (pool_size + max_overflow) at which pool is considered
near-exhaustion."""


# ── Persistence ──────────────────────────────────────────────────────

_STATE_PATH = Path(
    os.getenv(
        "GRID_ALERT_STATE_PATH",
        str(Path(__file__).resolve().parent.parent / ".server-logs" / "alert_state.json"),
    )
)


def _load_state() -> dict[str, str]:
    """Read the per-condition last-fired map. Returns empty on first use
    or any read error — alerter degrades to "fire fresh" rather than
    locking up on a corrupt state file."""
    if not _STATE_PATH.exists():
        return {}
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.warning("alert_state load failed, starting fresh: {e}", e=str(exc))
        return {}


def _save_state(state: dict[str, str]) -> None:
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(_STATE_PATH)
    except OSError as exc:
        log.warning("alert_state save failed: {e}", e=str(exc))


# ── Condition definitions ────────────────────────────────────────────

@dataclass(frozen=True)
class _Check:
    key: str
    severity: str
    predicate: Callable[[dict[str, Any]], bool]
    render: Callable[[dict[str, Any]], tuple[str, str]]


def _db_unhealthy(h: dict) -> bool:
    return not h.get("db", {}).get("healthy", False)


def _db_unhealthy_render(h: dict) -> tuple[str, str]:
    err = h.get("db", {}).get("error", "(no error message)")
    return (
        "GRID DB connectivity FAILED",
        f"Database health check failed.\n\nError: {err}\n\n"
        f"Timestamp: {h.get('timestamp')}",
    )


def _failed_pulls(h: dict) -> bool:
    n = h.get("db", {}).get("failed_pulls_24h", 0)
    return bool(n and n > FAILED_PULLS_24H_THRESHOLD)


def _failed_pulls_render(h: dict) -> tuple[str, str]:
    n = h.get("db", {}).get("failed_pulls_24h", 0)
    return (
        f"GRID: {n} failed pulls in last 24h",
        f"{n} pulls failed in the last 24 hours (threshold "
        f"{FAILED_PULLS_24H_THRESHOLD}). Check raw_series.pull_status "
        f"FAILED rows + scheduler logs.",
    )


def _stale_sources(h: dict) -> bool:
    sources = h.get("db", {}).get("stale_sources") or []
    return len(sources) > STALE_SOURCES_THRESHOLD


def _stale_sources_render(h: dict) -> tuple[str, str]:
    sources = h.get("db", {}).get("stale_sources") or []
    n = len(sources)
    sample = "\n".join(
        f"  - {s['source']}: last_pull={s['last_pull']}" for s in sources[:15]
    )
    if n > 15:
        sample += f"\n  ... and {n - 15} more"
    return (
        f"GRID: {n} sources past freshness window",
        f"{n} active sources have not been updated within the freshness "
        f"window (threshold {STALE_SOURCES_THRESHOLD}). Sample:\n\n{sample}",
    )


def _hermes_unhealthy(h: dict) -> bool:
    if "hermes" not in h:
        return False
    # Hermes block can be present-but-not-meaningful for daemons not
    # running this check; only alert when explicitly unhealthy with a
    # reason.
    hermes = h["hermes"]
    return not hermes.get("healthy", True) and bool(hermes.get("error"))


def _hermes_unhealthy_render(h: dict) -> tuple[str, str]:
    err = h.get("hermes", {}).get("error", "(no error)")
    return (
        "GRID Hermes operator unhealthy",
        f"Hermes daemon is reporting unhealthy.\n\nError: {err}",
    )


def _pool_exhausted(h: dict) -> bool:
    pool = h.get("pool") or h.get("db", {}).get("pool")
    if not pool:
        return False
    size = pool.get("size", 0)
    overflow = pool.get("overflow", 0)
    used = pool.get("checked_out", 0)
    cap = size + overflow
    if cap <= 0:
        return False
    return (used / cap) >= POOL_EXHAUSTION_THRESHOLD


def _pool_exhausted_render(h: dict) -> tuple[str, str]:
    pool = h.get("pool") or h.get("db", {}).get("pool") or {}
    return (
        "GRID DB pool near exhaustion",
        f"Connection pool at "
        f"{pool.get('checked_out', '?')}/"
        f"{pool.get('size', '?') + pool.get('overflow', 0)} "
        f"({POOL_EXHAUSTION_THRESHOLD:.0%} threshold). Investigate stuck "
        f"queries: SELECT pid,state,query FROM pg_stat_activity WHERE "
        f"state='active' ORDER BY query_start;",
    )


CHECKS: tuple[_Check, ...] = (
    _Check("db.unhealthy", "critical", _db_unhealthy, _db_unhealthy_render),
    _Check("db.failed_pulls", "warning", _failed_pulls, _failed_pulls_render),
    _Check("db.stale_sources", "warning", _stale_sources, _stale_sources_render),
    _Check("hermes.unhealthy", "warning", _hermes_unhealthy, _hermes_unhealthy_render),
    _Check("pool.exhausted", "critical", _pool_exhausted, _pool_exhausted_render),
)


# ── Public entry point ───────────────────────────────────────────────

def check_and_alert(
    health: dict[str, Any],
    cooldown_hours: float = DEFAULT_COOLDOWN_HOURS,
    now: datetime | None = None,
) -> list[str]:
    """Run all checks, fire alerts for any that crossed.

    Returns the list of condition keys that fired this call (useful for
    tests + log lines). Cooldown is enforced per-condition.
    """
    now = now or datetime.now(timezone.utc)
    state = _load_state()
    fired: list[str] = []

    for check in CHECKS:
        try:
            if not check.predicate(health):
                # Healthy now — clear the last-fired entry so the alert
                # fires again on the next bad transition rather than
                # waiting out the cooldown.
                if check.key in state:
                    state.pop(check.key)
                continue
        except Exception as exc:
            log.warning("alerter predicate {k} raised: {e}", k=check.key, e=str(exc))
            continue

        last_fired_iso = state.get(check.key)
        if last_fired_iso:
            try:
                last_fired = datetime.fromisoformat(last_fired_iso)
                if now - last_fired < timedelta(hours=cooldown_hours):
                    continue  # still in cooldown
            except ValueError:
                pass  # corrupt entry → fire fresh

        try:
            subject, body = check.render(health)
        except Exception as exc:
            log.warning("alerter render {k} raised: {e}", k=check.key, e=str(exc))
            continue

        if _send(subject, body, check.severity):
            state[check.key] = now.isoformat()
            fired.append(check.key)

    _save_state(state)
    return fired


def _send(subject: str, body: str, severity: str) -> bool:
    """Best-effort send via alerts.email. Returns True on success."""
    try:
        from alerts.email import send_alert
        return bool(send_alert(subject, body, severity=severity))
    except Exception as exc:
        log.warning("send_alert failed for '{s}': {e}", s=subject, e=str(exc))
        return False
