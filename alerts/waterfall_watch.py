"""
GRID Alerts — Waterfall Watch.

Fires a premium newsletter-style email when the forced_flow_monitor detects
two or more independent forced-flow conditions simultaneously tripped. This
is the practical expression of the discipline from
docs/playbooks/opex_waterfall.md: stop trading from a directional view and
start trading from "who is about to be forced to transact, and in which
direction".

The alert is intentionally loud because by the time the waterfall starts,
the dealer is already selling into the spot — discretionary traders only
have edge if they move before the forced seller does.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loguru import logger as log

if TYPE_CHECKING:
    from intelligence.forced_flow_monitor import MorningBriefing


SUBJECT_PREFIX = "GRID Waterfall Watch"


# ── Formatting Helpers ──────────────────────────────────────────────────


def _format_event_line(event: dict[str, Any]) -> str:
    days = event.get("trading_days_out", 0)
    when = "today" if days == 0 else f"T+{days}"
    return f"<strong>{event.get('label', event.get('kind'))}</strong> — {when}"


def _format_threshold_line(th: dict[str, Any]) -> str:
    status_icon = "🔴" if th.get("tripped") else "🟢"
    cur = th.get("current_value")
    thr = th.get("threshold_value")
    cur_str = f"{cur:.2f}" if isinstance(cur, (int, float)) else "n/a"
    thr_str = f"{thr:.2f}" if isinstance(thr, (int, float)) else "n/a"
    return (
        f"{status_icon} <strong>{th.get('name')}</strong>: "
        f"{th.get('description')} (current={cur_str}, threshold={thr_str})"
    )


def _build_html_body(briefing: "MorningBriefing") -> str:
    """Render the briefing as an HTML email body fragment."""
    regime = briefing.regime
    posture = briefing.posture

    flip_str = (
        f"{regime.gamma_flip:.2f}" if regime.gamma_flip is not None else "n/a"
    )
    spot_str = f"{regime.spot:.2f}" if regime.spot else "n/a"

    event_lines = "".join(
        f"<li>{_format_event_line(e.to_dict())}</li>"
        for e in briefing.upcoming_events[:6]
    ) or "<li>No high-impact catalysts in 10-day window.</li>"

    threshold_lines = "".join(
        f"<li>{_format_threshold_line(t.to_dict())}</li>"
        for t in briefing.thresholds
    )

    return f"""
    <h2 style="color:#EF4444; margin-top:0;">
        WATERFALL WATCH — {briefing.waterfall_risk_score}/5 conditions tripped
    </h2>

    <p><strong>Regime:</strong> {regime.regime}<br>
    <strong>SPY spot (est):</strong> {spot_str}<br>
    <strong>Gamma flip:</strong> {flip_str}<br>
    <strong>Aggregate GEX:</strong> {regime.aggregate_gex:,.0f}</p>

    <h3 style="color:#1A6EBF;">Upcoming Forced-Flow Dates</h3>
    <ul>{event_lines}</ul>

    <h3 style="color:#1A6EBF;">Threshold Checks</h3>
    <ul>{threshold_lines}</ul>

    <h3 style="color:#F59E0B;">Standing Posture</h3>
    <p><strong>LEVER:</strong> {posture.get('lever', 'n/a')}<br>
    <strong>CONDITION:</strong> {posture.get('condition', 'n/a')}<br>
    <strong>THESIS:</strong> {posture.get('thesis', 'n/a')}<br>
    <strong>INVALIDATION:</strong> {posture.get('invalidation', 'n/a')}</p>

    <p style="color:#5A7A96; font-size:12px;">
        Generated at {briefing.generated_at}. Discipline:
        <code>docs/playbooks/opex_waterfall.md</code>
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
    regime = briefing.regime.regime
    subject = f"{SUBJECT_PREFIX} — {score}/5 tripped ({regime})"
    body_html = _build_html_body(briefing)

    try:
        # Use the newsletter renderer for consistent brand styling
        sections = [{
            "title": f"Waterfall Watch — {score}/5 tripped",
            "body": body_html,
            "accent": "red" if score >= 3 else "amber",
        }]
        _send(subject, sections)
        log.info("waterfall_watch: alert sent — {s}/5 tripped", s=score)
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
    return f"{SUBJECT_PREFIX} — {briefing.waterfall_risk_score}/5 tripped ({briefing.regime.regime})"
