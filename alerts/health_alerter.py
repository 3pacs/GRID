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

FAILED_PULLS_1H_THRESHOLD = 20
"""Failed-pull count over the last hour. Sliding-window so a brief
outage auto-resolves instead of paging for 24 hours after it ends.
20 in 1 hour is a real ongoing problem; lower would be noise from
transient timeouts."""

STALE_SOURCES_THRESHOLD = 20
"""Number of sources past their freshness window before alerting. The
freshness window itself is now cadence-aware per source (see
alerts.cadence / scripts.hermes_health.check_db_health) — a weekly/monthly
source within its real cadence no longer counts toward this at all,
rather than relying on this threshold to absorb it."""

STALE_SOURCES_MAX_REMINDER_HOURS = 24.0
"""GRID-STALE-SOURCES-AUDIT-20260929.md: the old behavior resent this
email every DEFAULT_COOLDOWN_HOURS (6h) with no check that the list had
changed. Now it only re-fires when a source newly goes stale (a source
recovering is not news), with this as a "daily reminder, at most" ceiling
for when nothing new has broken."""

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


def _load_state() -> dict[str, Any]:
    """Read the per-condition last-fired map. Values are either an ISO
    timestamp string (``<key>``) or a dedupe-content payload, currently
    always a sorted list of strings (``<key>.content``). Returns empty on
    first use or any read error — alerter degrades to "fire fresh" rather
    than locking up on a corrupt state file."""
    if not _STATE_PATH.exists():
        return {}
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.warning("alert_state load failed, starting fresh: {e}", e=str(exc))
        return {}


def _save_state(state: dict[str, Any]) -> None:
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
    # dedupe_key: when set, check_and_alert only re-fires while this
    # check stays bad if the value it returns has changed since the last
    # fire (must be JSON-round-trippable — a sorted list, not a set/tuple).
    # None (the default) keeps the old cooldown-only behavior.
    dedupe_key: Callable[[dict[str, Any]], Any] | None = None
    # max_reminder_hours: the longest this check goes without re-firing
    # even when dedupe_key's value hasn't changed (a "daily reminder, at
    # most" cap). Defaults to the cooldown_hours passed to check_and_alert
    # when unset, i.e. the old single-cooldown behavior.
    max_reminder_hours: float | None = None
    # refire_on_additions_only: with dedupe_key, re-fire only when the new
    # value has members the last fired value lacked. Removals (recoveries)
    # update the stored value silently, so a source that recovers and
    # breaks again is new again.
    refire_on_additions_only: bool = False


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
    # Use the 1-hour sliding window so brief outages auto-resolve.
    # Falls back to 24h if 1h isn't populated (older Hermes builds).
    db = h.get("db", {})
    n = db.get("failed_pulls_1h")
    if n is None:
        n = db.get("failed_pulls_24h", 0)
        # Without 1h data, fall back to a higher threshold so we don't
        # flood-alert on the same data we used to use cumulatively.
        return bool(n and n > 200)
    return bool(n and n > FAILED_PULLS_1H_THRESHOLD)


def _failed_pulls_render(h: dict) -> tuple[str, str]:
    db = h.get("db", {})
    n_1h = db.get("failed_pulls_1h", 0)
    n_24h = db.get("failed_pulls_24h", 0)
    return (
        f"GRID: {n_1h} failed pulls in last hour ({n_24h} in 24h)",
        f"{n_1h} pulls failed in the last hour (threshold "
        f"{FAILED_PULLS_1H_THRESHOLD}); 24h cumulative is {n_24h}. "
        f"Check raw_series.pull_status FAILED rows, pull_log FAILED/PARTIAL "
        f"rows + scheduler logs.",
    )


def _actionable_stale(h: dict) -> list[dict[str, Any]]:
    """Stale sources that count toward the alert: everything except sources
    whose last attempt says they are switched off on this host (no key,
    token or SDK). Those are listed in the email but not counted."""
    sources = h.get("db", {}).get("stale_sources") or []
    return [s for s in sources if not s.get("not_configured")]


def _stale_sources(h: dict) -> bool:
    return len(_actionable_stale(h)) > STALE_SOURCES_THRESHOLD


def _short_reason(reason: str | None) -> str:
    text = (reason or "").strip()
    for prefix in ("PullerReportedFailure: ", "PullerReportedFailure:"):
        if text.startswith(prefix):
            text = text[len(prefix):]
    return text[:160] or "(no reason recorded)"


def _fmt_ts(iso: str | None) -> str:
    if not iso or iso == "never":
        return "never"
    try:
        return datetime.fromisoformat(iso).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")
    except ValueError:
        return iso


def _format_stale_source(s: dict[str, Any]) -> str:
    age = s.get("age_hours")
    age_txt = f"{age / 24:.1f}d ago" if age is not None else "never succeeded"
    line = (
        f"  - {s['source']}: last success {_fmt_ts(s.get('last_pull'))} "
        f"({age_txt}, cadence {s.get('cadence') or '?'})"
    )
    if s.get("last_attempt_status"):
        line += f"; last attempt {s['last_attempt_status']} {_fmt_ts(s.get('last_attempt_at'))}"
    return line


def _stale_bucket(s: dict[str, Any]) -> str:
    if s.get("not_configured"):
        return "not_configured"
    status = (s.get("last_attempt_status") or "").upper()
    reason = (s.get("last_attempt_reason") or "")
    if not status:
        return "never_attempted"
    if status == "SUCCESS" and reason.startswith("NO_NEW_DATA"):
        return "quiet"
    if status == "SUCCESS":
        return "succeeded_not_fresh"
    return "failing"


def _stale_sources_render(h: dict) -> tuple[str, str]:
    sources = h.get("db", {}).get("stale_sources") or []
    previous = set(h.get("_alert_previous_content") or [])
    actionable = _actionable_stale(h)
    n = len(actionable)
    new = sorted(s["source"] for s in actionable if previous and s["source"] not in previous)

    buckets: dict[str, list[dict[str, Any]]] = {}
    for s in sources:
        buckets.setdefault(_stale_bucket(s), []).append(s)

    if new:
        subject = f"GRID: {n} sources stale — {len(new)} newly stale: {', '.join(new[:4])}"
        if len(new) > 4:
            subject += f" +{len(new) - 4}"
    elif previous:
        subject = f"GRID: {n} sources stale (daily reminder — nothing new broke)"
    else:
        subject = f"GRID: {n} sources past freshness window"

    lines = [
        f"{n} sources are past their own freshness window (alert threshold "
        f"{STALE_SOURCES_THRESHOLD}; daily/weekly/monthly/quarterly grace per "
        f"source, see alerts/cadence.py).",
    ]
    off = buckets.get("not_configured", [])
    if off:
        lines.append(f"{len(off)} more are switched off on this host and not counted.")
    if new:
        lines += ["", f"NEWLY STALE SINCE THE LAST ALERT ({len(new)})"]
        lines += [f"  - {name}" for name in new]

    failing = buckets.get("failing", [])
    if failing:
        lines += ["", f"FAILING — the last attempt failed ({len(failing)}), grouped by reason"]
        by_reason: dict[str, list[dict[str, Any]]] = {}
        for s in failing:
            by_reason.setdefault(_short_reason(s.get("last_attempt_reason")), []).append(s)
        for reason, group in sorted(by_reason.items(), key=lambda kv: -len(kv[1])):
            lines.append(f"  [{len(group)}] {reason}")
            lines += ["  " + _format_stale_source(s) for s in group]

    sections = (
        ("quiet", "QUIET — last check ran fine, upstream has nothing newer"),
        ("succeeded_not_fresh", "RAN BUT NOT CREDITED — last attempt SUCCESS without a fresh write"),
        ("never_attempted", "NO ATTEMPT ON RECORD — not scheduled, or never logged to pull_log"),
    )
    for key, title in sections:
        group = buckets.get(key, [])
        if group:
            lines += ["", f"{title} ({len(group)})"]
            for s in group:
                line = _format_stale_source(s)
                if key != "never_attempted" and s.get("last_attempt_reason"):
                    line += f" — {_short_reason(s['last_attempt_reason'])}"
                lines.append(line)

    if off:
        lines += ["", f"SWITCHED OFF — not counted ({len(off)}); needs a key/token/SDK on grid-svr"]
        lines += [f"{_format_stale_source(s)} — {_short_reason(s.get('last_attempt_reason'))}" for s in off]

    lines += [
        "",
        "Detail: python3 scripts/audit_error_log.py --hours 24 on grid-svr, "
        "and pull_log for the per-run reasons above.",
    ]
    return subject, "\n".join(lines)


def _stale_sources_dedupe_key(h: dict) -> list[str]:
    """The set of counted stale source names, as a sorted list (JSON-stable).
    check_and_alert re-fires only when this gains a member, or once per
    STALE_SOURCES_MAX_REMINDER_HOURS if it hasn't."""
    return sorted({s["source"] for s in _actionable_stale(h)})


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
    # New health snapshots carry configured overflow separately. Retain
    # compatibility with older producers that only supplied live overflow.
    max_overflow = pool.get("max_overflow", overflow)
    if "max_overflow" in pool and max_overflow < 0:
        return False  # QueuePool max_overflow=-1 means an unbounded pool.
    cap = size + max_overflow
    if cap <= 0:
        return False
    return (used / cap) >= POOL_EXHAUSTION_THRESHOLD


def _pool_exhausted_render(h: dict) -> tuple[str, str]:
    pool = h.get("pool") or h.get("db", {}).get("pool") or {}
    cap = pool.get("size", 0) + pool.get("max_overflow", pool.get("overflow", 0))
    return (
        "GRID DB pool near exhaustion",
        f"Connection pool at "
        f"{pool.get('checked_out', '?')}/"
        f"{cap} "
        f"({POOL_EXHAUSTION_THRESHOLD:.0%} threshold). Investigate stuck "
        f"queries: SELECT pid,state,query FROM pg_stat_activity WHERE "
        f"state='active' ORDER BY query_start;",
    )


CHECKS: tuple[_Check, ...] = (
    _Check("db.unhealthy", "critical", _db_unhealthy, _db_unhealthy_render),
    _Check("db.failed_pulls", "warning", _failed_pulls, _failed_pulls_render),
    _Check(
        "db.stale_sources", "warning", _stale_sources, _stale_sources_render,
        dedupe_key=_stale_sources_dedupe_key,
        max_reminder_hours=STALE_SOURCES_MAX_REMINDER_HOURS,
        refire_on_additions_only=True,
    ),
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
        content_key = check.key + ".content"
        try:
            if not check.predicate(health):
                # Healthy now — clear the last-fired entry (and any dedupe
                # content) so the alert fires again on the next bad
                # transition rather than waiting out the cooldown, and a
                # later recurrence starts its change-detection from scratch.
                state.pop(check.key, None)
                state.pop(content_key, None)
                continue
        except Exception as exc:
            log.warning("alerter predicate {k} raised: {e}", k=check.key, e=str(exc))
            continue

        current_content: Any = None
        content_changed = False
        if check.dedupe_key is not None:
            try:
                current_content = check.dedupe_key(health)
            except Exception as exc:
                log.warning("alerter dedupe_key {k} raised: {e}", k=check.key, e=str(exc))
                current_content = None
            previous_content = state.get(content_key)
            if check.refire_on_additions_only and check.key in state and isinstance(previous_content, list):
                added = set(current_content or []) - set(previous_content)
                content_changed = bool(added)
                if not added and current_content != previous_content:
                    # Recoveries only: remember the smaller set without
                    # sending, so a source that breaks again counts as new.
                    state[content_key] = current_content
            else:
                content_changed = check.key not in state or current_content != previous_content

        last_fired_iso = state.get(check.key)
        if last_fired_iso and not content_changed:
            # Nothing new to report — only re-fire after max_reminder_hours
            # (a "daily reminder, at most" cap for stale_sources-style
            # checks; plain cooldown_hours for everything else, same as
            # before this change).
            reminder_hours = (
                check.max_reminder_hours if check.max_reminder_hours is not None else cooldown_hours
            )
            try:
                last_fired = datetime.fromisoformat(last_fired_iso)
                if now - last_fired < timedelta(hours=reminder_hours):
                    continue  # still within the reminder/cooldown window
            except ValueError:
                pass  # corrupt entry → fire fresh

        render_input = health
        if check.dedupe_key is not None and check.key in state:
            render_input = {**health, "_alert_previous_content": previous_content}
        try:
            subject, body = check.render(render_input)
        except Exception as exc:
            log.warning("alerter render {k} raised: {e}", k=check.key, e=str(exc))
            continue

        if _send(subject, body, check.severity):
            state[check.key] = now.isoformat()
            if check.dedupe_key is not None:
                state[content_key] = current_content
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
