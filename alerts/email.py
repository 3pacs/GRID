"""
GRID Intelligence — Premium newsletter email system.

Every email GRID sends is designed to be worth reading. Dark-theme HTML
templates matching the PWA, card-based layouts, actionable insights.
This email stream is the foundation of the monetizable product.

All sending is non-blocking (daemon thread) and never crashes the caller.
TLS is forced for external SMTP to protect signal data in transit.
"""

from __future__ import annotations

import smtplib
import threading
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from loguru import logger as log


# ---------------------------------------------------------------------------
# Settings helper
# ---------------------------------------------------------------------------

def _get_settings() -> Any:
    from config import settings
    return settings


# ---------------------------------------------------------------------------
# HTML Template Engine
# ---------------------------------------------------------------------------

_CSS = """
body { margin:0; padding:0; background:#080C10; font-family:'Helvetica Neue',Arial,sans-serif; }
.wrapper { max-width:640px; margin:0 auto; padding:20px; }
.header { text-align:center; padding:32px 0 24px; }
.logo { font-size:32px; font-weight:800; color:#1A6EBF; letter-spacing:6px; }
.tagline { font-size:13px; color:#5A7A96; margin-top:4px; letter-spacing:2px; }
.card { background:#111820; border-radius:12px; padding:24px; margin-bottom:16px; border-left:3px solid #1A6EBF; }
.card-accent-green { border-left-color:#22C55E; }
.card-accent-red { border-left-color:#EF4444; }
.card-accent-amber { border-left-color:#F59E0B; }
.card-accent-purple { border-left-color:#A855F7; }
.card-title { font-size:14px; font-weight:700; color:#1A6EBF; text-transform:uppercase; letter-spacing:1px; margin-bottom:12px; }
.card-body { font-size:15px; color:#C8D8E8; line-height:1.6; }
.card-body strong { color:#E8F0F8; }
.badge { display:inline-block; padding:4px 12px; border-radius:20px; font-size:12px; font-weight:700; letter-spacing:1px; }
.badge-buy { background:#22C55E22; color:#22C55E; }
.badge-sell { background:#EF444422; color:#EF4444; }
.badge-hold { background:#F59E0B22; color:#F59E0B; }
.badge-regime { background:#1A6EBF22; color:#1A6EBF; }
.kpi-row { display:flex; justify-content:space-between; padding:8px 0; border-bottom:1px solid #1A2A3A; }
.kpi-label { color:#5A7A96; font-size:13px; }
.kpi-value { color:#E8F0F8; font-size:15px; font-weight:600; }
.kpi-change-up { color:#22C55E; font-size:12px; }
.kpi-change-down { color:#EF4444; font-size:12px; }
.divider { border:0; border-top:1px solid #1A2A3A; margin:24px 0; }
.footer { text-align:center; padding:24px 0; color:#3A5A76; font-size:11px; line-height:1.6; }
.footer a { color:#1A6EBF; text-decoration:none; }
.cta-button { display:inline-block; padding:12px 32px; background:#1A6EBF; color:#fff; border-radius:8px; font-weight:700; font-size:14px; text-decoration:none; letter-spacing:1px; }
table.data-table { width:100%; border-collapse:collapse; }
table.data-table th { text-align:left; padding:8px 4px; font-size:12px; color:#5A7A96; border-bottom:1px solid #1A2A3A; }
table.data-table td { padding:8px 4px; font-size:14px; color:#C8D8E8; border-bottom:1px solid #0D1520; }
"""


def _render_html(subject: str, sections: list[dict], footer_note: str = "") -> str:
    """Build a complete HTML newsletter email."""
    now = datetime.now(timezone.utc)
    cards_html = ""
    for s in sections:
        accent = s.get("accent", "")
        accent_class = f" card-accent-{accent}" if accent else ""
        cards_html += f"""
        <div class="card{accent_class}">
            <div class="card-title">{s['title']}</div>
            <div class="card-body">{s['body']}</div>
        </div>
        """

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{subject}</title><style>{_CSS}</style></head>
<body><div class="wrapper">
    <div class="header">
        <div class="logo">GRID</div>
        <div class="tagline">INTELLIGENCE</div>
    </div>
    {cards_html}
    <hr class="divider">
    <div class="footer">
        {footer_note + '<br>' if footer_note else ''}
        {now.strftime('%B %d, %Y %H:%M UTC')}<br>
        Powered by <a href="#">GRID Intelligence</a><br>
        <a href="#">Manage preferences</a> &middot; <a href="#">Unsubscribe</a>
    </div>
</div></body></html>"""


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_regime(state: str, confidence: float, action: str) -> dict:
    accent = "green" if "ON" in state.upper() or "BUY" in action.upper() else "red" if "OFF" in state.upper() or "SELL" in action.upper() else "amber"
    badge_cls = "badge-buy" if "BUY" in action.upper() else "badge-sell" if "SELL" in action.upper() else "badge-hold"
    return {
        "title": "Regime State",
        "body": (
            f'<span class="badge badge-regime">{state}</span> '
            f'&nbsp; Confidence: <strong>{confidence:.0%}</strong><br><br>'
            f'Suggested Action: <span class="badge {badge_cls}">{action}</span>'
        ),
        "accent": accent,
    }


def _section_100x(ticker: str, direction: str, score: float, thesis: str, payoff: float = 0) -> dict:
    return {
        "title": f"100x Opportunity — {ticker}",
        "body": (
            f'<span class="badge {"badge-buy" if direction == "CALL" else "badge-sell"}">'
            f'{ticker} {direction}S</span> &nbsp; '
            f'Score: <strong>{score:.1f}/10</strong>'
            f'{f" &nbsp; Est. Payoff: <strong>{payoff:.0f}x</strong>" if payoff else ""}'
            f'<br><br>{thesis}'
        ),
        "accent": "purple",
    }


def _section_feature_movers(movers: list[dict]) -> dict:
    rows = ""
    for m in movers[:8]:
        chg = m.get("change", 0)
        cls = "kpi-change-up" if chg >= 0 else "kpi-change-down"
        arrow = "&#9650;" if chg >= 0 else "&#9660;"
        rows += f"""
        <tr>
            <td>{m.get('name', '')}</td>
            <td>{m.get('value', 'N/A')}</td>
            <td class="{cls}">{arrow} {abs(chg):.2f}</td>
        </tr>"""
    return {
        "title": "Top Feature Movers",
        "body": f"""<table class="data-table">
            <thead><tr><th>Feature</th><th>Value</th><th>Change</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>""",
    }


def _section_agent_decision(ticker: str, decision: str, reasoning: str) -> dict:
    badge_cls = "badge-buy" if decision == "BUY" else "badge-sell" if decision == "SELL" else "badge-hold"
    return {
        "title": f"Agent Decision — {ticker}",
        "body": (
            f'<span class="badge {badge_cls}">{decision}</span><br><br>'
            f'{reasoning[:500]}{"..." if len(reasoning) > 500 else ""}'
        ),
        "accent": "green" if decision == "BUY" else "red" if decision == "SELL" else "amber",
    }


def _section_hypothesis(statement: str, status: str) -> dict:
    return {
        "title": f"Hypothesis — {status.upper()}",
        "body": statement,
        "accent": "green" if status == "PASSED" else "red" if status == "FAILED" else "",
    }


def _section_briefing(briefing_type: str, excerpt: str) -> dict:
    return {
        "title": f"{briefing_type.title()} Market Briefing",
        "body": excerpt[:800] + ("..." if len(excerpt) > 800 else ""),
    }


def _section_insight(title: str, content_preview: str) -> dict:
    return {
        "title": title,
        "body": content_preview[:600] + ("..." if len(content_preview) > 600 else ""),
        "accent": "purple",
    }


def _section_kpi(label: str, value: str, change: str = "") -> dict:
    return {
        "title": label,
        "body": f'<span style="font-size:28px;font-weight:800;color:#E8F0F8;">{value}</span>'
                + (f' <span style="font-size:14px;color:#22C55E;">{change}</span>' if change else ""),
    }


def _section_text(title: str, body: str, accent: str = "") -> dict:
    return {"title": title, "body": body, "accent": accent}


# ---------------------------------------------------------------------------
# SMTP sending
# ---------------------------------------------------------------------------

def _send_in_thread(subject: str, html: str, plain: str) -> None:
    t = threading.Thread(target=_do_send, args=(subject, html, plain), daemon=True, name="grid-email")
    t.start()


def _do_send(subject: str, html: str, plain: str) -> bool:
    try:
        cfg = _get_settings()
        if not cfg.ALERT_EMAIL_ENABLED:
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = cfg.ALERT_EMAIL_FROM
        msg["To"] = cfg.ALERT_EMAIL_TO
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(html, "html"))

        host = cfg.ALERT_SMTP_HOST
        port = cfg.ALERT_SMTP_PORT
        use_tls = cfg.ALERT_SMTP_USE_TLS
        is_external = host not in ("localhost", "127.0.0.1", "::1")
        if is_external and not use_tls:
            use_tls = True

        if use_tls:
            smtp = smtplib.SMTP(host, port, timeout=30)
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
        else:
            smtp = smtplib.SMTP(host, port, timeout=30)

        if cfg.ALERT_SMTP_USER and cfg.ALERT_SMTP_PASSWORD:
            smtp.login(cfg.ALERT_SMTP_USER, cfg.ALERT_SMTP_PASSWORD)

        smtp.sendmail(cfg.ALERT_EMAIL_FROM, [cfg.ALERT_EMAIL_TO], msg.as_string())
        smtp.quit()
        log.info("Newsletter sent — {s}", s=subject)
        return True
    except Exception as exc:
        log.warning("Newsletter send failed — {s}: {e}", s=subject, e=str(exc))
        return False


def _send(subject: str, sections: list[dict], footer_note: str = "") -> None:
    """Render and send a newsletter email (non-blocking)."""
    html = _render_html(subject, sections, footer_note)
    plain = "\n\n".join(f"[{s['title']}]\n{s.get('body', '')}" for s in sections)
    _send_in_thread(subject, html, plain)


# ---------------------------------------------------------------------------
# Public API — Email types
# ---------------------------------------------------------------------------

def send_alert(subject: str, body: str, severity: str = "info") -> bool:
    """Send a simple alert email (backward compatible)."""
    try:
        cfg = _get_settings()
        if not cfg.ALERT_EMAIL_ENABLED:
            return False
        _send(f"[GRID {severity.upper()}] {subject}", [_section_text("Alert", body)])
        return True
    except Exception:
        return False


def alert_on_failure(source: str, error: str) -> None:
    _send(
        f"[GRID WARNING] Data pull failure — {source}",
        [_section_text("Ingestion Failure", f"<strong>{source}</strong> failed at "
         f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}<br><br>"
         f"<code>{error[:500]}</code>", accent="red")],
    )


def alert_on_regime_change(from_regime: str, to_regime: str, confidence: float) -> None:
    _send(
        f"GRID Intelligence — Regime Shift: {from_regime} → {to_regime}",
        [
            _section_regime(to_regime, confidence, "REVIEW"),
            _section_text("Transition", f"Market regime changed from <strong>{from_regime}</strong> "
                          f"to <strong>{to_regime}</strong> with {confidence:.0%} confidence.<br><br>"
                          "Review the dashboard for feature drivers and recommended positioning."),
        ],
    )


def alert_on_100x_opportunity(ticker: str, score: float, direction: str, thesis: str) -> None:
    _send(
        f"GRID Intelligence — 100x Alert: {ticker} {direction}S",
        [_section_100x(ticker, direction, score, thesis)],
    )


def send_insight(category: str, title: str, content: str, metadata: dict | None = None) -> None:
    """Send a newsletter for a noteworthy LLM insight."""
    if content is None:
        return
    accent_map = {"regime_analysis": "amber", "hypothesis": "purple", "critique": "red", "explanation": "", "100x_opportunity": "purple"}
    _send(
        f"GRID Intelligence — {title}",
        [_section_insight(title, content)],
        footer_note=f"Category: {category}",
    )


def send_agent_report(ticker: str, decision: str, reasoning: str,
                       regime_state: str, confidence: float, duration: float) -> None:
    """Send agent deliberation results as a newsletter."""
    _send(
        f"GRID Intelligence — {ticker}: Agent says {decision}",
        [
            _section_regime(regime_state, confidence, decision),
            _section_agent_decision(ticker, decision, reasoning),
            _section_kpi("Analysis Time", f"{duration:.1f}s"),
        ],
    )


def send_weekly_review(review_content: str) -> None:
    """Send the weekly insight review as a newsletter."""
    _send(
        "GRID Intelligence — Weekly Review",
        [_section_briefing("Weekly", review_content)],
    )


def daily_digest() -> None:
    """Compile and send a daily digest newsletter with live data."""
    try:
        from sqlalchemy import text as sa_text
        from db import get_engine

        engine = get_engine()
        sections: list[dict] = []

        with engine.connect() as conn:
            # Regime
            try:
                row = conn.execute(sa_text(
                    "SELECT inferred_state, state_confidence, grid_recommendation "
                    "FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1"
                )).fetchone()
                if row:
                    sections.append(_section_regime(row[0], row[1], row[2]))
            except Exception:
                pass

            # Journal count
            try:
                row = conn.execute(sa_text(
                    "SELECT COUNT(*) FROM decision_journal "
                    "WHERE decision_timestamp >= NOW() - INTERVAL '24 hours'"
                )).fetchone()
                sections.append(_section_kpi("Decisions (24h)", str(row[0]) if row else "0"))
            except Exception:
                pass

            # 100x opportunities
            try:
                rows = conn.execute(sa_text(
                    "SELECT ticker, direction, score, payoff_multiple, thesis "
                    "FROM options_mispricing_scans "
                    "WHERE is_100x = TRUE AND scan_date >= CURRENT_DATE - 3 "
                    "ORDER BY score DESC LIMIT 5"
                )).fetchall()
                for r in rows:
                    sections.append(_section_100x(r[0], r[1], r[2], r[4], r[3]))
            except Exception:
                pass

            # Data freshness
            try:
                row = conn.execute(sa_text(
                    "SELECT COUNT(DISTINCT source_id), MAX(pull_timestamp) FROM raw_series "
                    "WHERE pull_timestamp >= NOW() - INTERVAL '24 hours'"
                )).fetchone()
                if row and row[0]:
                    sections.append(_section_kpi("Active Sources (24h)", str(row[0]),
                                                 f"latest: {str(row[1])[:16]}"))
            except Exception:
                pass

        if not sections:
            sections.append(_section_text("Status", "All systems operational. No notable events in the last 24 hours."))

        _send("GRID Intelligence — Daily Digest", sections)
        log.info("Daily digest sent")
    except Exception as exc:
        log.warning("Daily digest failed: {e}", e=str(exc))


def _section_code_block(title: str, code: str) -> dict:
    """Render a code block section with copy-paste styling."""
    # Escape HTML in code content
    safe_code = code.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return {
        "title": title,
        "body": (
            '<div style="background:#080C10;border:1px solid #1A2A3A;border-radius:8px;'
            'padding:16px;font-family:monospace;font-size:13px;color:#22C55E;'
            f'white-space:pre-wrap;word-break:break-all;margin-top:8px;">'
            f'{safe_code}</div>'
        ),
    }


def alert_on_failure_with_fix(source: str, error: str, fix_commands: dict | None = None) -> None:
    """Send failure alert with copy-paste fix commands for Claude Code.

    Parameters:
        source: Name of the failing data source.
        error: Error message text.
        fix_commands: Optional dict with keys: diagnose, fix, retry, file.
    """
    sections = [
        _section_text(
            "Ingestion Failure",
            f"<strong>{source}</strong> failed at "
            f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}<br><br>"
            f"<code>{error[:500]}</code>",
            accent="red",
        ),
    ]

    if fix_commands:
        claude_prompt = f"The {source} data pull failed with this error:\n\n"
        claude_prompt += f"```\n{error[:300]}\n```\n\n"
        claude_prompt += "Please diagnose and fix this. Here are the relevant details:\n\n"
        if fix_commands.get("diagnose"):
            claude_prompt += f"Diagnose: `{fix_commands['diagnose']}`\n"
        if fix_commands.get("fix"):
            claude_prompt += f"Fix: `{fix_commands['fix']}`\n"
        if fix_commands.get("retry"):
            claude_prompt += f"Retry: `{fix_commands['retry']}`\n"
        if fix_commands.get("file"):
            claude_prompt += f"Relevant file: `{fix_commands['file']}`\n"

        sections.append(_section_code_block("Paste into Claude Code", claude_prompt))

    _send(f"[GRID WARNING] {source} failed — fix commands included", sections)


def alert_on_transition_leaders(leaders: list[dict], cluster_result: dict) -> None:
    """Alert when transition leader features are identified."""
    if not leaders:
        return
    rows = ""
    for leader in leaders[:5]:
        rows += (
            f"<tr><td>{leader.get('feature', '?')}</td>"
            f"<td>{leader.get('lead_weeks', '?')}w</td>"
            f"<td>{leader.get('direction', '?')}</td>"
            f"<td>{leader.get('t_stat', 0):.2f}</td></tr>"
        )

    _send(
        "GRID Intelligence — Transition Leaders Detected",
        [
            _section_text(
                "Cluster Transition Signals",
                f"Best k={cluster_result.get('best_k', '?')} clusters detected. "
                "These features predict regime transitions:",
                accent="purple",
            ),
            _section_text(
                "Leading Indicators",
                f'<table class="data-table"><thead><tr>'
                f'<th>Feature</th><th>Lead</th><th>Direction</th><th>t-stat</th>'
                f'</tr></thead><tbody>{rows}</tbody></table>',
            ),
        ],
    )


def alert_on_discovery_insight(title: str, description: str, data: dict | None = None) -> None:
    """Alert for noteworthy discovery findings.

    Used for dimensionality shifts, redundancy warnings, and other
    structural changes in the feature space.
    """
    sections = [_section_text("Discovery Insight", description, accent="amber")]
    if data and data.get("by_family"):
        family_text = "<br>".join(
            f"&bull; <strong>{k}</strong>: {len(v)} features"
            for k, v in data["by_family"].items()
        )
        sections.append(_section_text("Feature Taxonomy", family_text))
    _send(f"GRID Intelligence — {title}", sections)


def send_test_email() -> bool:
    """Send a test newsletter showcasing the template and expansion plan."""
    sections = [
        _section_regime("RISK_ON", 0.87, "BUY"),
        _section_100x("SPY", "CALL", 8.5, "IV at 2-year lows + extreme put/call ratio + max pain divergence. Gamma squeeze setup detected.", 185),
        _section_feature_movers([
            {"name": "Yield Curve 2s10s", "value": "+0.42", "change": 0.15},
            {"name": "VIX Spot", "value": "14.2", "change": -3.8},
            {"name": "Copper/Gold Ratio", "value": "0.0048", "change": 0.0003},
            {"name": "DXY Index", "value": "103.7", "change": -1.2},
            {"name": "HY Spread", "value": "3.21%", "change": -0.18},
        ]),
        _section_agent_decision("SPY", "BUY",
            "Multi-agent consensus: regime transition to RISK_ON confirmed by yield curve steepening, "
            "VIX compression, and credit spread tightening. Bull case supported by improving breadth "
            "and positive momentum across 4 of 5 sectors. Risk assessment: limited downside with "
            "strong support at 200-day MA. Conviction: HIGH."),
        _section_hypothesis(
            "Copper/Gold ratio slope > 0 for 63 consecutive days predicts equity returns "
            "> 2% over the following 21 trading days with 73% accuracy (p=0.004).",
            "PASSED"),
        _section_text("GRID Intelligence — Expansion Roadmap",
            "<strong>Horizontals:</strong><br>"
            "&#8226; <strong>GRID Daily Brief</strong> — Free newsletter tier. Macro regime updates, "
            "feature snapshots, market context. Build audience.<br>"
            "&#8226; <strong>GRID Pro</strong> ($49/mo) — 100x options alerts, agent decisions, "
            "feature importance rankings, regime change notifications.<br>"
            "&#8226; <strong>GRID API</strong> ($499/mo) — B2B data feed. Regime labels, feature "
            "z-scores, transition probabilities. For quant funds and RIAs.<br>"
            "&#8226; <strong>GRID Enterprise</strong> ($2,499/mo) — Full platform access, custom "
            "feature engineering, dedicated support, SLA.<br><br>"
            "<strong>Verticals:</strong><br>"
            "&#8226; <strong>Options Intelligence</strong> — 100x scanner as standalone product. "
            "IV skew, gamma squeeze, max pain divergence alerts.<br>"
            "&#8226; <strong>Crypto Regime Signals</strong> — Solana/DeFi-specific alerts, "
            "on-chain regime detection, memecoin momentum.<br>"
            "&#8226; <strong>International Macro</strong> — China credit impulse, Korea exports, "
            "ECB policy, EM FX stress. Leading indicators newsletter.<br>"
            "&#8226; <strong>Celestial Correlation Research</strong> — Lunar cycles, planetary "
            "aspects, geomagnetic activity vs market regimes. Novel research product.<br><br>"
            "<strong>Revenue Model:</strong><br>"
            "Free newsletter &#8594; Pro subscription ($49/mo) &#8594; API access ($499/mo) "
            "&#8594; Enterprise ($2,499/mo)<br>"
            "Target: 10K free &#8594; 500 Pro &#8594; 20 API &#8594; 5 Enterprise = "
            "<strong>$47K MRR</strong>",
            accent="purple"),
    ]

    _send("GRID Intelligence — Platform Preview", sections,
          footer_note="This is a test email showcasing the GRID newsletter template.")
    return True


if __name__ == "__main__":
    send_test_email()
    print("Test email dispatched — check stepdadfinance@gmail.com")
