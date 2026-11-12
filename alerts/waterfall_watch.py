"""
GRID Alerts — Waterfall Watch.

Renders versioned grouped rule counts separately from overlapping condition
diagnostics. Historical briefings without that method retain their original
raw-check presentation. Neither scale is a calibrated probability, and a
sourced schedule is not evidence that dealer transactions occurred.

Alert activation remains the monitor's responsibility. Its built-in calendar
estimates cannot supply the second evidence group required by the new method.
"""

from __future__ import annotations

import html
import math
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from loguru import logger as log

if TYPE_CHECKING:
    from intelligence.forced_flow_monitor import MorningBriefing


SUBJECT_PREFIX = "GRID Waterfall Watch"


# ── Formatting Helpers ──────────────────────────────────────────────────


def _esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _threshold_state(th: dict[str, Any], regime: str | None = None) -> str:
    """A missing measurement is not a passed check; calendar absence is distinct."""
    if regime == "UNKNOWN" and th.get("name") in {"short_gamma_regime", "compound_regime_catalyst"}:
        return "MISSING"
    if _number(th.get("threshold_value")) is None:
        return "MISSING"
    if _number(th.get("current_value")) is None:
        if th.get("name") == "high_impact_catalyst_within_5d" and th.get("tripped") is False:
            return "NO LISTED EVENT"
        return "MISSING"
    if th.get("tripped") is True:
        return "TRIPPED"
    return "NOT TRIPPED" if th.get("tripped") is False else "MISSING"


def _check_counts(briefing: "MorningBriefing") -> tuple[int, int, int]:
    states = [_threshold_state(th.to_dict(), briefing.regime.regime) for th in briefing.thresholds]
    missing = states.count("MISSING")
    return states.count("TRIPPED"), len(states) - missing, missing


def _uses_grouped_score(briefing: "MorningBriefing") -> bool:
    """Only the explicitly versioned method uses the 0-2 presentation."""
    return getattr(briefing, "score_method", None) == "grouped_rule_count_v1"


def _grouped_count(briefing: "MorningBriefing") -> int | None:
    """Validate the reported count without reconstructing or rescoring history."""
    count = _number(briefing.waterfall_risk_score)
    return int(count) if count in (0, 1, 2) else None


def _alert_accent(briefing: "MorningBriefing") -> str:
    if _uses_grouped_score(briefing):
        return "red" if _grouped_count(briefing) == 2 else "amber"
    return "red" if briefing.waterfall_risk_score >= 3 else "amber"


def _format_event_line(event: dict[str, Any]) -> str:
    event_date = _date(event.get("event_date"))
    today = datetime.now(timezone.utc).date()
    status = "date unavailable; occurrence unconfirmed"
    if event_date:
        status = ("past date; outcome unconfirmed" if event_date < today
                  else "scheduled; occurrence unconfirmed")
    days = event.get("trading_days_out")
    relative = f"; T+{_esc(days)} trading days from briefing" if days is not None else ""
    return (f"<strong>{_esc(event.get('label') or event.get('kind') or 'Unnamed event')}</strong>: "
            f"{_esc(event_date or 'n/a')} ({status}{relative})")


def _format_threshold_line(
    th: dict[str, Any], regime: str | None = None, *, grouped: bool = False,
) -> str:
    cur, threshold = _number(th.get("current_value")), _number(th.get("threshold_value"))
    cur_str = f"{cur:.2f}" if cur is not None else "n/a"
    thr_str = f"{threshold:.2f}" if threshold is not None else "n/a"
    status = _threshold_state(th, regime)
    if grouped and status == "NO LISTED EVENT":
        status = "NO QUALIFIED SCHEDULE"
    if status == "MISSING" and regime == "UNKNOWN":
        cur_str = "n/a (regime inputs unavailable)"
    return (f"{status} <strong>{_esc(th.get('name'))}</strong>: {_esc(th.get('description'))} "
            f"(current={cur_str}, threshold={thr_str})")


def _build_html_body(briefing: "MorningBriefing") -> str:
    """Summary-first briefing with supplied-check coverage and source dates."""
    regime, posture = briefing.regime, briefing.posture
    tripped, checked, missing = _check_counts(briefing)
    grouped = _uses_grouped_score(briefing)
    count = _grouped_count(briefing) if grouped else None
    headline = f"{tripped}/{checked} checked conditions tripped"
    headline_color = "#EF4444"
    if grouped:
        headline = f"{count}/2 evidence groups tripped" if count is not None else "Evidence-group count unavailable"
        headline_color = "#EF4444" if count == 2 else "#F59E0B"
    today = datetime.now(timezone.utc).date()
    snapshot_date = _date(regime.snapshot_date)
    snapshot_age = (today - snapshot_date).days if snapshot_date else None
    snapshot_note = "Snapshot age: unavailable; source freshness unverified."
    if snapshot_age is not None:
        snapshot_note = f"Snapshot age: {snapshot_age} day(s) as of {today}; conditions were not rechecked by this email."
        if snapshot_age < 0:
            snapshot_note = "Snapshot date is in the future; source freshness unverified."
    generated_date = _date(briefing.generated_at)
    briefing_note = ""
    if generated_date and generated_date < today:
        briefing_note = f" Briefing is {(today - generated_date).days} day(s) old; this is a historical snapshot."
    if grouped:
        summary_lines = [
            "Method: grouped_rule_count_v1. The two evidence groups are dealer positioning "
            "and calendar catalyst; this is not independent voting or a calibrated probability.",
            f"Raw diagnostics: {tripped}/{checked} checked conditions tripped, "
            f"{missing} missing-input conditions; {len(briefing.thresholds)} conditions supplied. "
            "Shared positioning checks count once; the compound check adds no group.",
            "Built-in calendar estimates cannot activate a two-group alert. "
            "A sourced schedule is not observed flow; calendar coverage remains incomplete.",
            "Historical raw-condition scores use a different scale and are not directly comparable.",
        ]
        if count is None:
            summary_lines.append("The reported grouped count is invalid or missing; expected an integer from 0 to 2.")
    else:
        summary_lines = [
            f"{len(briefing.thresholds)} supplied conditions: {checked} checked, {missing} missing-input conditions.",
            f"Monitor-reported risk score: {briefing.waterfall_risk_score}. "
            "The displayed count uses the supplied checks; it is not a probability. "
            "Compound conditions overlap with their component checks.",
        ]
    names = [th.name.replace("_", " ") for th in briefing.thresholds
             if _threshold_state(th.to_dict(), regime.regime) == "TRIPPED"]
    summary_lines.append("Tripped: " + (", ".join(names) if names else "none in the available checks"))
    if missing:
        summary_lines.append("Missing measurements are not all-clear conditions. Review data coverage before relying on the posture.")
    if not grouped and briefing.waterfall_risk_score != tripped:
        summary_lines.append("Reported score differs from the available tripped checks; treat the headline as supplied-check coverage.")
    dated_events = sorted(((_date(e.event_date), e) for e in briefing.upcoming_events if _date(e.event_date)),
                          key=lambda pair: pair[0])
    upcoming = [(day, event) for day, event in dated_events if day >= today]
    past = sum(day < today for day, _ in dated_events)
    if upcoming:
        day, event = upcoming[0]
        summary_lines.append(f"Next listed date: {event.label} on {day}, scheduled and unconfirmed. "
                             f"{len(briefing.upcoming_events)} supplied calendar events shown; {past} past dates.")
    else:
        summary_lines.append(f"{len(briefing.upcoming_events)} supplied calendar events shown; {past} past dates. "
                             "No future dated event in this payload; calendar completeness is unverified.")
    summary_lines.append(f"Regime snapshot date: {regime.snapshot_date}. {snapshot_note}{briefing_note}")
    summary = "".join(f"<p>{_esc(line)}</p>" for line in summary_lines)

    def numeric(value: Any, spec: str = '.2f') -> str:
        number = _number(value)
        return format(number, spec) if number is not None else "n/a"

    spot = _number(regime.spot)
    spot_str = numeric(spot) if spot is not None and spot > 0 else "n/a"
    gex_str = numeric(regime.aggregate_gex, ',.0f') if regime.regime != "UNKNOWN" else "n/a"
    event_lines = "".join(f"<li>{_format_event_line(e.to_dict())}</li>" for e in briefing.upcoming_events)
    event_lines = event_lines or "<li>No calendar events supplied; schedule coverage unconfirmed.</li>"
    threshold_lines = "".join(f"<li>{_format_threshold_line(t.to_dict(), regime.regime, grouped=grouped)}</li>"
                              for t in briefing.thresholds) or "<li>No threshold checks supplied.</li>"
    threshold_heading = "Raw Condition Diagnostics" if grouped else "Threshold Checks"
    return f"""
    <div style="font-size:32px;font-weight:800;color:{headline_color};">
        {headline}
    </div>
    {summary}
    <p><strong>Regime:</strong> {_esc(regime.regime)}<br>
    <strong>SPY spot (est):</strong> {spot_str}<br>
    <strong>Gamma flip:</strong> {numeric(regime.gamma_flip)}<br>
    <strong>Aggregate GEX:</strong> {gex_str}</p>
    <h3 style="color:#1A6EBF;">Supplied Forced-Flow Dates</h3>
    <ul>{event_lines}</ul>
    <h3 style="color:#1A6EBF;">{threshold_heading}</h3>
    <ul>{threshold_lines}</ul>
    <h3 style="color:#F59E0B;">Standing Posture</h3>
    <p><strong>LEVER:</strong> {_esc(posture.get('lever', 'n/a'))}<br>
    <strong>CONDITION:</strong> {_esc(posture.get('condition', 'n/a'))}<br>
    <strong>THESIS:</strong> {_esc(posture.get('thesis', 'n/a'))}<br>
    <strong>INVALIDATION:</strong> {_esc(posture.get('invalidation', 'n/a'))}</p>
    <p style="color:#8AA0B5;font-size:12px;">
        Briefing generated at {_esc(briefing.generated_at)}. Generation time does not establish source freshness.
        Discipline: <code>docs/playbooks/opex_waterfall.md</code>
    </p>
    """


# ── Public API ──────────────────────────────────────────────────────────


def send_waterfall_alert(briefing: "MorningBriefing") -> bool:
    """Send a waterfall-watch email alert for the given briefing.

    Returns True on best-effort send success, False if alert emails are
    disabled, the alert layer is unavailable, or send fails. Never raises.
    """
    try:
        from alerts.email import _get_settings, _send
    except Exception as exc:
        log.warning("waterfall_watch: alerts.email import failed: {e}", e=str(exc))
        return False

    try:
        cfg = _get_settings()
        if not getattr(cfg, "ALERT_EMAIL_ENABLED", False):
            log.debug("waterfall_watch: alert email disabled, skipping send")
            return False
    except Exception:
        # If settings can't be read, fall through and still attempt the send
        pass

    score = briefing.waterfall_risk_score
    subject = build_alert_subject(briefing)
    body_html = _build_html_body(briefing)

    try:
        # Use the newsletter renderer for consistent brand styling
        sections = [{
            "title": "Waterfall Watch: at a glance",
            "body": body_html,
            "accent": _alert_accent(briefing),
        }]
        _send(subject, sections)
        log.info("waterfall_watch: alert queued - monitor score {s}", s=score)
        return True
    except Exception as exc:
        log.warning("waterfall_watch: send failed: {e}", e=str(exc))
        return False


def send_waterfall_alert_if_triggered(
    briefing: "MorningBriefing",
    threshold: int = 2,
) -> bool:
    """Convenience wrapper: only send if briefing meets the threshold."""
    if briefing.waterfall_risk_score >= threshold:
        return send_waterfall_alert(briefing)
    return False


def build_alert_subject(briefing: "MorningBriefing") -> str:
    """Expose subject-line construction for testability."""
    tripped, checked, _ = _check_counts(briefing)
    regime = " ".join(str(briefing.regime.regime).split())
    if _uses_grouped_score(briefing):
        count = _grouped_count(briefing)
        label = f"{count}/2 evidence groups" if count is not None else "group count unavailable"
        return (f"{SUBJECT_PREFIX} - {label} "
                f"({regime}; {tripped}/{checked} raw checks tripped)")
    return (f"{SUBJECT_PREFIX} - {tripped}/{checked} checked tripped "
            f"({regime}; {briefing.waterfall_risk_score} reported)")
