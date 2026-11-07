"""
GRID email notification module.

Sends email summaries when interesting insights are discovered during
autoresearch runs, backtest validations, or daily digest cycles.

Environment Variables
---------------------
GRID_SMTP_HOST      SMTP server hostname (default: localhost)
GRID_SMTP_PORT      SMTP server port (default: 25)
GRID_SMTP_USER      SMTP username for authentication (default: empty, no auth)
GRID_SMTP_PASSWORD  SMTP password for authentication (default: empty)
GRID_NOTIFY_EMAIL   Recipient email address for notifications (required for
                    sending; if unset, emails are logged but not sent)

All variables can also be set via the Settings class in config.py or a .env
file at the project root.

Usage:
    from scripts.notify import notify_on_pass, send_insight_email

    # After an autoresearch run:
    notify_on_pass(result)

    # Manual email:
    send_insight_email("Subject", "Body text", to_email="user@example.com")
"""

from __future__ import annotations

import math
import os
import re
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log

from governance.hypothesis_labels import LEGACY_CUTOFF


# ---------------------------------------------------------------------------
# SMTP helpers
# ---------------------------------------------------------------------------

def _get_smtp_config() -> dict[str, Any]:
    """Read SMTP configuration from environment variables.

    Falls back to config.Settings if available, then to defaults.
    """
    try:
        from config import settings
        return {
            "host": os.getenv("ALERT_SMTP_HOST", settings.ALERT_SMTP_HOST),
            "port": int(os.getenv("ALERT_SMTP_PORT", str(settings.ALERT_SMTP_PORT))),
            "user": os.getenv("ALERT_SMTP_USER", settings.ALERT_SMTP_USER),
            "password": os.getenv("ALERT_SMTP_PASSWORD", settings.ALERT_SMTP_PASSWORD),
            "to_email": os.getenv("ALERT_EMAIL_TO", settings.ALERT_EMAIL_TO),
        }
    except Exception:
        return {
            "host": os.getenv("ALERT_SMTP_HOST", "localhost"),
            "port": int(os.getenv("ALERT_SMTP_PORT", "25")),
            "user": os.getenv("ALERT_SMTP_USER", ""),
            "password": os.getenv("ALERT_SMTP_PASSWORD", ""),
            "to_email": os.getenv("ALERT_EMAIL_TO", ""),
        }


def send_insight_email(
    subject: str,
    body: str,
    to_email: str | None = None,
    html: bool = False,
    *,
    summary_ready: bool = False,
) -> bool:
    """Send an email notification.  Logs but never crashes on failure.

    Parameters:
        subject:  Email subject line.
        body:     Email body (plain text or HTML).
        to_email: Recipient address.  Falls back to GRID_NOTIFY_EMAIL.
        html:     Whether the supplied body is HTML.
        summary_ready: Internal formatters already include their substantive recap.

    Returns:
        True if the email was sent successfully, False otherwise.
    """
    cfg = _get_smtp_config()
    recipient = to_email or cfg["to_email"]

    if not recipient:
        log.warning("No recipient email configured — skipping notification")
        return False

    from_addr = cfg["user"] or f"grid@{cfg['host']}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = recipient

    from alerts.email import _html_to_plain, _render_html, _section_summary, _summary_text

    if summary_ready:
        plain = _html_to_plain(body) if html else body
        rendered = body if html else _render_html(subject, [{
            "title": "Notification", "body": f'<div style="white-space:pre-wrap">{escape(body)}</div>',
        }])
    else:
        source = _html_to_plain(body) if html else body
        summary = _section_summary(
            subject, "Notification", _summary_text(source)
            + " Source time was not supplied; report time is not source freshness.",
        )
        if html:
            # Preserve a caller's complete HTML document while inserting the
            # recap inside its body, rather than nesting one document in another.
            card = f"<section><h2>At a glance</h2>{summary['body']}</section><h2>Full detail</h2>"
            rendered, found = re.subn(r"(<body\b[^>]*>)", lambda match: match[0] + card, body, count=1, flags=re.I)
            if not found:
                rendered = _render_html(subject, [summary, {"title": "Full detail", "body": body}])
        else:
            rendered = _render_html(subject, [summary, {
                "title": "Full detail", "body": f'<div style="white-space:pre-wrap">{escape(body)}</div>',
            }])
        plain = "[At a glance]\n" + summary["plain"] + "\n\n[Full detail]\n" + source
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(rendered, "html", "utf-8"))

    try:
        if cfg["port"] == 465:
            server = smtplib.SMTP_SSL(cfg["host"], cfg["port"], timeout=30)
        else:
            server = smtplib.SMTP(cfg["host"], cfg["port"], timeout=30)
            if cfg["port"] == 587:
                server.starttls()

        if cfg["user"] and cfg["password"]:
            server.login(cfg["user"], cfg["password"])

        server.sendmail(from_addr, [recipient], msg.as_string())
        server.quit()
        log.info("Email sent — to={to}, subject={subj}", to=recipient, subj=subject)
        return True

    except Exception as exc:
        log.error(
            "Failed to send email — {err} (host={h}:{p})",
            err=str(exc),
            h=cfg["host"],
            p=cfg["port"],
        )
        return False


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def _metric(value: Any, fmt: str = ".2f") -> str:
    if value is None or isinstance(value, bool):
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return "n/a"
    return format(number, fmt) if math.isfinite(number) else "n/a"


def _timestamp(value: Any) -> str:
    try:
        at = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return "unknown"
    if at.tzinfo is None or at.utcoffset() is None:
        return f"{at:%Y-%m-%d %H:%M:%S} (timezone unknown)"
    return f"{at.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S} UTC"


def format_backtest_summary(result: dict[str, Any]) -> str:
    """Summarize supplied simulation outputs without implying a live track record.

    Supports the full run result and its raw attempt for preview. Notification
    eligibility remains controlled separately by notify_on_pass's existing gate.
    Return/drawdown inputs from validation.backtest are fractional values.
    """
    from alerts.email import _render_html, _section_summary, _summary_text

    best = result.get("best_result") if "best_result" in result else result
    best = best if isinstance(best, dict) else {}
    statement = str(best.get("statement") or "No hypothesis supplied.")
    verdict = str(best.get("verdict") or "unknown")
    iterations = result.get("iterations_run", best.get("iteration"))
    eras = [era for era in best.get("era_results") or [] if isinstance(era, dict)]
    ok_eras = sum(era.get("status") == "OK" for era in eras)
    feature_ids = best.get("feature_ids") or []
    sharpe = _metric(best.get("sharpe"))
    summary = (
        f"Recorded verdict: {verdict}; reported iteration/run count: {_metric(iterations, ',.0f')}. "
        f"Baseline Sharpe: {_metric(best.get('baseline_sharpe'))}. "
        f"{ok_eras} of {len(eras)} supplied eras have status OK. "
        + _summary_text(statement, max_chars=200)
        + " These are backtest outputs, not live investment returns."
    )
    sections = [_section_summary(f"Simulated Sharpe: {sharpe}", "Autoresearch", summary)]
    sections.append({"title": "Hypothesis", "body": f'<div style="white-space:pre-wrap">{escape(statement)}</div>'})
    rows = [
        ("Recorded verdict", escape(verdict)),
        ("Simulated Sharpe", sharpe),
        ("Baseline Sharpe", _metric(best.get("baseline_sharpe"))),
        ("Simulated total return", _metric(best.get("return"), "+.1%")),
        ("Simulated max drawdown", _metric(best.get("max_drawdown"), ".1%")),
    ]
    sections.append({"title": "Reported simulation metrics", "body":
        '<table class="data-table">' + ''.join(f"<tr><td>{label}</td><td>{value}</td></tr>" for label, value in rows)
        + '</table><p>Totals are not annualized. Return and drawdown inputs are fractions rendered as percentages. '
          'Costs, test coverage and acceptance are not independently verified by this email.</p>'})
    era_rows = "".join(
        f"<tr><td>{escape(str(era.get('era', 'unknown')))}</td>"
        f"<td>{escape(str(era.get('status', 'unknown')))}</td>"
        f"<td>{escape(str(era.get('start') or era.get('test_start') or 'unknown'))} to "
        f"{escape(str(era.get('end') or era.get('test_end') or 'unknown'))}</td>"
        f"<td>{_metric(era.get('sharpe'))}</td><td>{_metric(era.get('return'), '+.1%')}</td></tr>"
        for era in eras
    )
    sections.append({"title": "Era breakdown", "body": (
        '<table class="data-table"><tr><th>Era</th><th>Recorded status</th><th>Test window</th><th>Sharpe</th><th>Total return</th></tr>'
        + era_rows + '</table>' if eras else '<p>No era rows supplied; test coverage unknown.</p>'
    ) + f"<p>{escape(str(best.get('era_summary') or ''))}</p>"})
    sections.append({"title": "Features used", "body": escape(', '.join(str(f) for f in feature_ids)) or 'None supplied'})
    sections.append({"title": "Source timing", "body":
        f"Result timestamp: {escape(_timestamp(result.get('timestamp') or best.get('timestamp')))}<br>"
        "Missing dates stay unknown. The generation time below does not establish when a backtest ran."})
    return _render_html("GRID Autoresearch Report", sections,
                        footer_note="Reported simulation outputs; not a verified investment track record.")


def format_daily_digest(engine: Any, *, now: datetime | None = None) -> str:
    """Build a daily digest summary from the decision journal and hypothesis registry.

    Queries:
      - New journal entries in the last 24 hours
      - Any hypothesis that PASSED recently
      - Feature importance drift alerts

    Parameters:
        engine: SQLAlchemy engine connected to the GRID database.

    Returns:
        A plain-text summary string suitable for email.
    """
    from sqlalchemy import text as sa_text

    now = now or datetime.now(timezone.utc)
    since = now - timedelta(hours=24)
    lines: list[str] = []
    counts: dict[str, int | None] = {"journal entries": None, "recorded PASSED hypotheses": None, "drift records": None}

    lines.append(f"GRID Daily Digest — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("=" * 60)

    # --- Recent journal decisions ---
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sa_text(
                    "SELECT id, inferred_state, state_confidence, "
                    "grid_recommendation, verdict, decision_timestamp "
                    "FROM decision_journal "
                    "WHERE decision_timestamp >= :since AND decision_timestamp <= :now "
                    "ORDER BY decision_timestamp DESC"
                ),
                {"since": since, "now": now},
            ).fetchall()

        counts["journal entries"] = len(rows)
        lines.append(f"\nDecision Journal  ({len(rows)} entries in last 24h)")
        lines.append("-" * 40)
        if rows:
            for r in rows:
                ts = _timestamp(r[5])
                lines.append(
                    f"  [{ts}] id={r[0]}  state={r[1]}  "
                    f"confidence score={_metric(r[2])}  rec={r[3]}  verdict={r[4] or 'pending'}"
                )
        else:
            lines.append("  No new decisions.")
    except Exception as exc:
        lines.append(f"\nDecision Journal: query failed ({exc})")

    # --- Passed hypotheses ---
    try:
        with engine.connect() as conn:
            passed = conn.execute(
                sa_text(
                    "SELECT h.id, h.statement, h.updated_at "
                    "FROM hypothesis_registry h "
                    "WHERE h.state = 'PASSED' AND h.updated_at >= :since AND h.updated_at <= :now "
                    "AND h.created_at >= :legacy_cutoff "
                    "AND NOT EXISTS (SELECT 1 FROM hypothesis_registry_labels l "
                    "WHERE l.registry_id = h.id) "
                    "ORDER BY h.updated_at DESC"
                ),
                {"since": since, "now": now, "legacy_cutoff": LEGACY_CUTOFF},
            ).fetchall()

        counts["recorded PASSED hypotheses"] = len(passed)
        lines.append(f"\nPassed Hypotheses  ({len(passed)} recorded statuses)")
        lines.append("-" * 40)
        if passed:
            for r in passed:
                lines.append(f"  id={r[0]} [{_timestamp(r[2])}]: {r[1]}")
        else:
            lines.append("  None in last 24h.")
    except Exception as exc:
        lines.append(f"\nPassed Hypotheses: query failed ({exc})")

    # --- Feature importance drift ---
    try:
        with engine.connect() as conn:
            drift = conn.execute(
                sa_text(
                    "SELECT f.name, d.drift_score, d.checked_at "
                    "FROM feature_drift_log d "
                    "JOIN feature_registry f ON f.id = d.feature_id "
                    "WHERE d.checked_at >= :since AND d.checked_at <= :now "
                    "AND d.drift_score > 0.15 "
                    "ORDER BY d.drift_score DESC"
                ),
                {"since": since, "now": now},
            ).fetchall()

        counts["drift records"] = len(drift)
        lines.append(f"\nFeature Drift Alerts  ({len(drift)} records above 0.15)")
        lines.append("-" * 40)
        if drift:
            for r in drift:
                lines.append(f"  {r[0]}: drift={_metric(r[1], '.3f')} [{_timestamp(r[2])}]")
        else:
            lines.append("  No significant drift detected.")
    except Exception:
        # feature_drift_log may not exist yet — that's fine
        lines.append("\nFeature Drift Alerts: not available")

    lines.append("\n" + "=" * 60)
    lines.append("End of digest.")
    summary = "[At a glance]\n" + "; ".join(
        f"{value:,} {label}" if value is not None else f"{label}: unknown"
        for label, value in counts.items()
    )
    summary += (f"\nWindow: {_timestamp(since)} through {_timestamp(now)}, inclusive. "
                "Historical records, not current open alerts. This is not a verified investment track record. "
                "PASSED counts exclude pre-cutoff and labeled legacy rows under the registry policy. Query failures remain unknown.\n\n")
    return summary + "\n".join(lines)


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------

def notify_on_pass(result: dict[str, Any], to_email: str | None = None) -> bool:
    """Send an email only if the autoresearch result contains a PASS.

    Parameters:
        result: Dict returned by ``autoresearch.run_autoresearch()``.
        to_email: Optional override recipient.

    Returns:
        True if a PASS was found and email was sent, False otherwise.
    """
    if not result.get("passed"):
        log.debug("Hypothesis did not pass — no notification sent")
        return False

    best = result.get("best_result") or {}
    subject = (
        f"[GRID] Autoresearch result — "
        f"Reported verdict {best.get('verdict') or 'unknown'}; simulated Sharpe {_metric(best.get('sharpe'))} "
        f"(iter {result.get('iterations_run', '?')})"
    )
    body = format_backtest_summary(result)
    return send_insight_email(subject, body, to_email=to_email, html=True, summary_ready=True)


# ---------------------------------------------------------------------------
# CLI — manual test / daily digest sender
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GRID email notifications")
    parser.add_argument(
        "--digest", action="store_true", help="Send a daily digest email"
    )
    parser.add_argument(
        "--test", action="store_true", help="Send a test email to verify config"
    )
    parser.add_argument("--to", type=str, default=None, help="Override recipient")
    args = parser.parse_args()

    if args.test:
        ok = send_insight_email(
            subject="[GRID] Test notification",
            body="This is a test email from the GRID notification system.",
            to_email=args.to,
        )
        log.info("Test email sent." if ok else "Test email failed — check logs.")

    elif args.digest:
        from db import get_engine

        engine = get_engine()
        body = format_daily_digest(engine)
        print(body)
        ok = send_insight_email(
            subject=f"[GRID] Daily Digest — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
            body=body,
            to_email=args.to,
            summary_ready=True,
        )
        log.info("Digest sent." if ok else "Digest send failed — check logs.")

    else:
        parser.print_help()
