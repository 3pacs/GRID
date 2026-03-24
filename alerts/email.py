"""
GRID email alerting module.

Sends email alerts for data-pull failures, regime changes, 100x options
opportunities, and a daily digest summary.  All sending is non-blocking
(fire-and-forget in a daemon thread) and never crashes the caller.
"""

from __future__ import annotations

import smtplib
import threading
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from loguru import logger as log


def _get_settings() -> Any:
    """Lazy-import settings to avoid circular imports at module level."""
    from config import settings
    return settings


def _send_in_thread(subject: str, body: str, severity: str) -> None:
    """Send email in a daemon thread (fire-and-forget)."""
    t = threading.Thread(
        target=_do_send,
        args=(subject, body, severity),
        daemon=True,
        name="alert-email",
    )
    t.start()


def _do_send(subject: str, body: str, severity: str) -> bool:
    """Actual SMTP send logic.  Returns True on success, False on failure."""
    try:
        cfg = _get_settings()

        if not cfg.ALERT_EMAIL_ENABLED:
            log.debug("Email alerts disabled — skipping: {s}", s=subject)
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[GRID {severity.upper()}] {subject}"
        msg["From"] = cfg.ALERT_EMAIL_FROM
        msg["To"] = cfg.ALERT_EMAIL_TO

        # Plain text part
        msg.attach(MIMEText(body, "plain"))

        # HTML part (simple wrapper)
        html = (
            "<html><body>"
            f"<h2>GRID Alert — {severity.upper()}</h2>"
            f"<pre>{body}</pre>"
            f"<hr><small>Sent at {datetime.now(timezone.utc).isoformat()} UTC</small>"
            "</body></html>"
        )
        msg.attach(MIMEText(html, "html"))

        host = cfg.ALERT_SMTP_HOST
        port = cfg.ALERT_SMTP_PORT
        user = cfg.ALERT_SMTP_USER
        password = cfg.ALERT_SMTP_PASSWORD
        use_tls = cfg.ALERT_SMTP_USE_TLS

        # Require TLS when sending to external SMTP (non-localhost)
        is_external = host not in ("localhost", "127.0.0.1", "::1")
        if is_external and not use_tls:
            log.warning(
                "ALERT_SMTP_USE_TLS is off but host is external ({h}) — "
                "forcing TLS to protect signal data in transit",
                h=host,
            )
            use_tls = True

        if use_tls:
            smtp = smtplib.SMTP(host, port, timeout=30)
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
        else:
            smtp = smtplib.SMTP(host, port, timeout=30)

        if user and password:
            smtp.login(user, password)

        smtp.sendmail(cfg.ALERT_EMAIL_FROM, [cfg.ALERT_EMAIL_TO], msg.as_string())
        smtp.quit()

        log.info("Alert email sent — subject={s}", s=subject)
        return True

    except Exception as exc:
        log.warning("Alert email failed — subject={s}, error={e}", s=subject, e=str(exc))
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_alert(subject: str, body: str, severity: str = "info") -> bool:
    """Send an alert email (non-blocking).

    Parameters:
        subject: Email subject line (will be prefixed with severity).
        body: Plain-text email body.
        severity: One of "info", "warning", "critical".

    Returns:
        True if the send was dispatched (not necessarily delivered).
        False if alerts are disabled or dispatch failed.
    """
    try:
        cfg = _get_settings()
        if not cfg.ALERT_EMAIL_ENABLED:
            return False
        _send_in_thread(subject, body, severity)
        return True
    except Exception as exc:
        log.warning("send_alert dispatch failed: {e}", e=str(exc))
        return False


def alert_on_failure(source: str, error: str) -> None:
    """Send an alert for a data-pull or system failure.

    Parameters:
        source: Name of the failing subsystem (e.g. "FRED", "yfinance").
        error: Error message or traceback excerpt.
    """
    subject = f"Data pull failure — {source}"
    body = (
        f"Source:    {source}\n"
        f"Time:     {datetime.now(timezone.utc).isoformat()} UTC\n"
        f"Error:    {error}\n"
        "\n"
        "Action required: check the ingestion logs and retry if needed."
    )
    send_alert(subject, body, severity="warning")


def alert_on_regime_change(from_regime: str, to_regime: str, confidence: float) -> None:
    """Send an alert when the detected market regime changes.

    Parameters:
        from_regime: Previous regime label.
        to_regime: New regime label.
        confidence: Confidence score for the transition (0.0-1.0).
    """
    subject = f"Regime change — {from_regime} -> {to_regime}"
    body = (
        f"Regime Transition Detected\n"
        f"{'=' * 40}\n"
        f"From:       {from_regime}\n"
        f"To:         {to_regime}\n"
        f"Confidence: {confidence:.2%}\n"
        f"Time:       {datetime.now(timezone.utc).isoformat()} UTC\n"
        "\n"
        "Review the regime dashboard for full context and recommended actions."
    )
    sev = "critical" if confidence >= 0.8 else "warning"
    send_alert(subject, body, severity=sev)


def alert_on_100x_opportunity(
    ticker: str,
    score: float,
    direction: str,
    thesis: str,
) -> None:
    """Send an alert for a flagged 100x+ options opportunity.

    Parameters:
        ticker: Underlying symbol.
        score: Composite mispricing score (0-10).
        direction: "CALL" or "PUT".
        thesis: Human-readable thesis for the opportunity.
    """
    subject = f"100x Opportunity — {ticker} {direction}"
    body = (
        f"100x+ Options Opportunity Detected\n"
        f"{'=' * 40}\n"
        f"Ticker:    {ticker}\n"
        f"Direction: {direction}\n"
        f"Score:     {score:.1f}/10\n"
        f"Time:      {datetime.now(timezone.utc).isoformat()} UTC\n"
        f"\n"
        f"Thesis:\n{thesis}\n"
        "\n"
        "Review the options scanner dashboard for strikes, expiry, and full analysis."
    )
    send_alert(subject, body, severity="critical")


def daily_digest() -> None:
    """Compile and send a daily summary email.

    Queries the database for:
    - Recent journal entries (last 24h)
    - Current regime state
    - Data freshness per source
    - Active mispricing alerts
    """
    try:
        from sqlalchemy import text as sa_text
        from db import get_engine

        engine = get_engine()
        now_utc = datetime.now(timezone.utc).isoformat()
        sections: list[str] = [
            f"GRID Daily Digest — {now_utc[:10]}",
            "=" * 50,
            "",
        ]

        with engine.connect() as conn:
            # --- Recent journal entries (last 24h) ---
            try:
                rows = conn.execute(sa_text(
                    "SELECT model_version_id, direction, confidence, created_at "
                    "FROM decision_journal "
                    "WHERE created_at >= NOW() - INTERVAL '24 hours' "
                    "ORDER BY created_at DESC "
                    "LIMIT 20"
                )).fetchall()
                sections.append(f"Journal Entries (last 24h): {len(rows)}")
                for r in rows:
                    sections.append(
                        f"  {r[3]} | model={r[0]} | {r[1]} | conf={r[2]}"
                    )
            except Exception as exc:
                sections.append(f"Journal: unavailable ({exc})")

            sections.append("")

            # --- Current regime state ---
            try:
                row = conn.execute(sa_text(
                    "SELECT regime_label, confidence, detected_at "
                    "FROM regime_states "
                    "ORDER BY detected_at DESC "
                    "LIMIT 1"
                )).fetchone()
                if row:
                    sections.append(
                        f"Current Regime: {row[0]} (confidence={row[1]:.2%}, "
                        f"detected={row[2]})"
                    )
                else:
                    sections.append("Current Regime: no data")
            except Exception as exc:
                sections.append(f"Regime: unavailable ({exc})")

            sections.append("")

            # --- Data freshness ---
            try:
                rows = conn.execute(sa_text(
                    "SELECT source_name, MAX(release_date) AS latest "
                    "FROM source_catalog sc "
                    "JOIN raw_economic_data red ON red.source_id = sc.source_id "
                    "GROUP BY source_name "
                    "ORDER BY latest DESC "
                    "LIMIT 15"
                )).fetchall()
                sections.append("Data Freshness (top 15 sources):")
                for r in rows:
                    sections.append(f"  {r[0]:30s} latest={r[1]}")
            except Exception as exc:
                sections.append(f"Data freshness: unavailable ({exc})")

            sections.append("")

            # --- Active 100x alerts ---
            try:
                rows = conn.execute(sa_text(
                    "SELECT ticker, direction, score, payoff_multiple, scan_date "
                    "FROM options_mispricing_scans "
                    "WHERE is_100x = TRUE "
                    "AND scan_date >= CURRENT_DATE - INTERVAL '3 days' "
                    "ORDER BY score DESC "
                    "LIMIT 10"
                )).fetchall()
                sections.append(f"Active 100x Opportunities (3d): {len(rows)}")
                for r in rows:
                    sections.append(
                        f"  {r[0]} {r[1]} | score={r[2]:.1f} | "
                        f"payoff={r[3]:.0f}x | date={r[4]}"
                    )
            except Exception as exc:
                sections.append(f"100x alerts: unavailable ({exc})")

        body = "\n".join(sections)
        send_alert("Daily Digest", body, severity="info")
        log.info("Daily digest email sent")

    except Exception as exc:
        log.warning("Daily digest failed: {e}", e=str(exc))
