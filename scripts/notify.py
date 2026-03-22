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

import os
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log


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
            "host": os.getenv("GRID_SMTP_HOST", settings.GRID_SMTP_HOST),
            "port": int(os.getenv("GRID_SMTP_PORT", str(settings.GRID_SMTP_PORT))),
            "user": os.getenv("GRID_SMTP_USER", settings.GRID_SMTP_USER),
            "password": os.getenv("GRID_SMTP_PASSWORD", settings.GRID_SMTP_PASSWORD),
            "to_email": os.getenv("GRID_NOTIFY_EMAIL", settings.GRID_NOTIFY_EMAIL),
        }
    except Exception:
        return {
            "host": os.getenv("GRID_SMTP_HOST", "localhost"),
            "port": int(os.getenv("GRID_SMTP_PORT", "25")),
            "user": os.getenv("GRID_SMTP_USER", ""),
            "password": os.getenv("GRID_SMTP_PASSWORD", ""),
            "to_email": os.getenv("GRID_NOTIFY_EMAIL", ""),
        }


def send_insight_email(
    subject: str,
    body: str,
    to_email: str | None = None,
    html: bool = False,
) -> bool:
    """Send an email notification.  Logs but never crashes on failure.

    Parameters:
        subject:  Email subject line.
        body:     Email body (plain text or HTML).
        to_email: Recipient address.  Falls back to GRID_NOTIFY_EMAIL.
        html:     If True, send as text/html; otherwise text/plain.

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

    if html:
        # Attach both plain-text fallback and HTML
        plain = body.replace("<br>", "\n")
        plain = plain.replace("</tr>", "\n")
        plain = plain.replace("</td>", "  ")
        # Strip remaining tags for the plain-text part
        import re
        plain = re.sub(r"<[^>]+>", "", plain)
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(body, "html"))
    else:
        msg.attach(MIMEText(body, "plain"))

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

def format_backtest_summary(result: dict[str, Any]) -> str:
    """Format an autoresearch result dict into an HTML email body.

    Expects the dict shape returned by ``autoresearch.run_autoresearch()``:
        - best_result.statement, .sharpe, .baseline_sharpe, .verdict,
          .feature_ids, .era_results / .era_summary
        - all_attempts, passed, iterations_run

    Parameters:
        result: Autoresearch result dictionary.

    Returns:
        HTML string suitable for email body.
    """
    best = result.get("best_result") or {}
    passed = result.get("passed", False)
    iterations = result.get("iterations_run", 0)

    statement = best.get("statement", "(no hypothesis)")
    sharpe = best.get("sharpe", "N/A")
    baseline_sharpe = best.get("baseline_sharpe", "N/A")
    verdict = best.get("verdict", "N/A")
    feature_ids = best.get("feature_ids", [])
    era_summary = best.get("era_summary", "N/A")
    era_results = best.get("era_results", [])
    total_return = best.get("return", "N/A")
    max_dd = best.get("max_drawdown", "N/A")

    verdict_color = "#27ae60" if verdict == "PASS" else (
        "#e67e22" if verdict == "CONDITIONAL" else "#e74c3c"
    )

    # Build era breakdown rows
    era_rows = ""
    if era_results:
        for e in era_results:
            status = e.get("status", "?")
            era_sharpe = e.get("sharpe", "N/A")
            era_ret = e.get("return", "N/A")
            era_rows += (
                f"<tr><td>Era {e.get('era', '?')}</td>"
                f"<td>{status}</td>"
                f"<td>{era_sharpe}</td>"
                f"<td>{era_ret}</td></tr>"
            )
    elif era_summary and era_summary != "N/A":
        era_rows = f"<tr><td colspan='4'>{era_summary}</td></tr>"

    html = f"""\
<html>
<body style="font-family: Arial, sans-serif; color: #333; max-width: 600px;">
<h2 style="color: #2c3e50;">GRID Autoresearch Report</h2>

<table style="border-collapse: collapse; width: 100%; margin-bottom: 16px;">
  <tr>
    <td style="padding: 6px 12px; font-weight: bold;">Verdict</td>
    <td style="padding: 6px 12px;">
      <span style="color: {verdict_color}; font-weight: bold; font-size: 1.1em;">
        {verdict}
      </span>
    </td>
  </tr>
  <tr>
    <td style="padding: 6px 12px; font-weight: bold;">Iterations</td>
    <td style="padding: 6px 12px;">{iterations}</td>
  </tr>
</table>

<h3>Hypothesis</h3>
<p style="background: #f8f9fa; padding: 12px; border-left: 4px solid #3498db;">
  {statement}
</p>

<h3>Performance</h3>
<table style="border-collapse: collapse; width: 100%;">
  <tr>
    <td style="padding: 4px 12px; font-weight: bold;">Sharpe Ratio</td>
    <td style="padding: 4px 12px;">{sharpe}</td>
  </tr>
  <tr>
    <td style="padding: 4px 12px; font-weight: bold;">Baseline Sharpe</td>
    <td style="padding: 4px 12px;">{baseline_sharpe}</td>
  </tr>
  <tr>
    <td style="padding: 4px 12px; font-weight: bold;">Return</td>
    <td style="padding: 4px 12px;">{total_return}</td>
  </tr>
  <tr>
    <td style="padding: 4px 12px; font-weight: bold;">Max Drawdown</td>
    <td style="padding: 4px 12px;">{max_dd}</td>
  </tr>
</table>

<h3>Era Breakdown</h3>
<table style="border-collapse: collapse; width: 100%; border: 1px solid #ddd;">
  <tr style="background: #ecf0f1;">
    <th style="padding: 6px 12px; text-align: left;">Era</th>
    <th style="padding: 6px 12px; text-align: left;">Status</th>
    <th style="padding: 6px 12px; text-align: left;">Sharpe</th>
    <th style="padding: 6px 12px; text-align: left;">Return</th>
  </tr>
  {era_rows}
</table>

<h3>Features Used</h3>
<p>{', '.join(str(f) for f in feature_ids) if feature_ids else 'None'}</p>

<hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">
<p style="font-size: 0.85em; color: #999;">
  Generated by GRID Autoresearch at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
</p>
</body>
</html>"""

    return html


def format_daily_digest(engine: Any) -> str:
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

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=24)
    lines: list[str] = []

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
                    "WHERE decision_timestamp >= :since "
                    "ORDER BY decision_timestamp DESC"
                ),
                {"since": since},
            ).fetchall()

        lines.append(f"\nDecision Journal  ({len(rows)} entries in last 24h)")
        lines.append("-" * 40)
        if rows:
            for r in rows:
                ts = r[5].strftime("%H:%M") if r[5] else "?"
                lines.append(
                    f"  [{ts}] id={r[0]}  state={r[1]}  "
                    f"conf={r[2]:.2f}  rec={r[3]}  verdict={r[4] or 'pending'}"
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
                    "SELECT id, statement, updated_at "
                    "FROM hypothesis_registry "
                    "WHERE state = 'PASSED' AND updated_at >= :since "
                    "ORDER BY updated_at DESC"
                ),
                {"since": since},
            ).fetchall()

        lines.append(f"\nPassed Hypotheses  ({len(passed)})")
        lines.append("-" * 40)
        if passed:
            for r in passed:
                lines.append(f"  id={r[0]}: {r[1]}")
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
                    "WHERE d.checked_at >= :since "
                    "AND d.drift_score > 0.15 "
                    "ORDER BY d.drift_score DESC"
                ),
                {"since": since},
            ).fetchall()

        lines.append(f"\nFeature Drift Alerts  ({len(drift)})")
        lines.append("-" * 40)
        if drift:
            for r in drift:
                lines.append(f"  {r[0]}: drift={r[1]:.3f}")
        else:
            lines.append("  No significant drift detected.")
    except Exception as exc:
        # feature_drift_log may not exist yet — that's fine
        lines.append("\nFeature Drift Alerts: not available")

    lines.append("\n" + "=" * 60)
    lines.append("End of digest.")
    return "\n".join(lines)


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

    best = result.get("best_result", {})
    subject = (
        f"[GRID] Hypothesis PASSED — "
        f"Sharpe {best.get('sharpe', '?')} "
        f"(iter {result.get('iterations_run', '?')})"
    )
    body = format_backtest_summary(result)
    return send_insight_email(subject, body, to_email=to_email, html=True)


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
        print("Test email sent." if ok else "Test email failed — check logs.")

    elif args.digest:
        from db import get_engine

        engine = get_engine()
        body = format_daily_digest(engine)
        print(body)
        ok = send_insight_email(
            subject=f"[GRID] Daily Digest — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
            body=body,
            to_email=args.to,
        )
        print("Digest sent." if ok else "Digest send failed — check logs.")

    else:
        parser.print_help()
