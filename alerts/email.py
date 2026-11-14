"""
GRID Intelligence — Premium newsletter email system.

Every email GRID sends is designed to be worth reading. Dark-theme HTML
templates matching the PWA, card-based layouts, actionable insights.
This email stream is the foundation of the monetizable product.

All sending is non-blocking (daemon thread) and never crashes the caller.
TLS is forced for external SMTP to protect signal data in transit.
"""

from __future__ import annotations

import atexit
import html
import math
import smtplib
import re
import threading
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html.parser import HTMLParser
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
<title>{html.escape(subject)}</title><style>{_CSS}</style></head>
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

class _PlainHTMLParser(HTMLParser):
    """Readable text with preformatted regions preserved for repair commands."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[tuple[str, bool]] = []
        self.hidden = 0
        self.format_stack: list[tuple[str, bool]] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in {"script", "style", "head"}:
            self.hidden += 1
        if self.hidden:
            return
        inherited = self.format_stack[-1][1] if self.format_stack else False
        style = dict(attrs).get("style") or ""
        preformatted = tag == "pre" or bool(re.search(
            r"white-space\s*:\s*(?:pre-wrap|pre|break-spaces)(?:\s*[;!]|\s*$)", style, re.IGNORECASE,
        ))
        if tag not in {"br", "hr", "img", "input", "meta", "link", "wbr"}:
            self.format_stack.append((tag, inherited or preformatted))
        if tag in {"br", "p", "div", "tr", "li", "h1", "h2", "h3", "hr", "pre"}:
            self.parts.append(("\n", False))
        if tag == "li":
            self.parts.append(("- ", False))

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "head"}:
            self.hidden = max(0, self.hidden - 1)
        elif not self.hidden:
            if tag in {"td", "th"}:
                self.parts.append((" | ", False))
            elif tag in {"p", "div", "tr", "li", "h1", "h2", "h3", "pre"}:
                self.parts.append(("\n", False))
            for index in range(len(self.format_stack) - 1, -1, -1):
                if self.format_stack[index][0] == tag:
                    del self.format_stack[index:]
                    break

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            preserve = self.format_stack[-1][1] if self.format_stack else False
            self.parts.append((data, preserve))


def _html_to_plain(markup: str) -> str:
    """Strip HTML with readable boundaries, retaining pre/pre-wrap whitespace."""
    parser = _PlainHTMLParser()
    parser.feed(str(markup))
    parser.close()
    lines: list[str] = []
    line_parts: list[str] = []
    preserved = False
    for text, preformatted in [*parser.parts, ("\n", False)]:
        for fragment in text.splitlines(keepends=True):
            line_parts.append(fragment.rstrip("\r\n"))
            preserved = preserved or preformatted
            if fragment.endswith(("\n", "\r")):
                line = "".join(line_parts)
                if not preserved:
                    line = re.sub(r"[ \t\xa0]+", " ", line).strip().removesuffix(" |").rstrip()
                if line or preserved:
                    lines.append(line)
                line_parts, preserved = [], False
    return "\n".join(lines).strip("\n")


def _summary_text(text: str, max_chars: int = 320) -> str:
    """Extract at most two opening sentences/lines; never infer or invent facts.

    Input is plain source text, not HTML. Long prose stays in its detail section.
    """
    text = str(text or "").strip()
    if not text:
        return "No narrative provided."
    sentences = re.split(r"(?<=[.!?])\s+|\n+", text)
    excerpt = re.sub(r"\s+", " ", " ".join(sentences[:2])).strip()
    if len(excerpt) <= max_chars:
        return excerpt
    clipped = excerpt[:max(0, max_chars - 3)]
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0]
    return clipped.rstrip() + "..."


def _text_html(text: Any) -> str:
    """Keep complete plain-source text, escaped and with line breaks intact."""
    return f'<div style="white-space:pre-wrap;">{html.escape(str(text or ""))}</div>'


def _section_summary(headline: str, label: str, summary: str, accent: str = "") -> dict:
    """Opt-in headline/summary card. All arguments are plain text, never HTML."""
    return {
        "title": "At a glance",
        "body": (f'<div style="font-size:32px;font-weight:800;color:#E8F0F8;">'
                 f'{html.escape(str(headline))}</div>'
                 f'<div style="font-size:13px;color:#9AB2C8;">{html.escape(label)}</div>'
                 f'<p>{html.escape(summary)}</p>'),
        "plain": f"{headline} — {label}\n{summary}",
        "accent": accent,
    }


def _confidence_text(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and 0 <= value <= 1:
        return f"{value:.0%}"
    return "unavailable"


def _score_text(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and 0 <= value <= 10:
        return f"{value:.1f}/10"
    return "unavailable"


def _render_plain(sections: list[dict]) -> str:
    return "\n\n".join(
        f"[{_html_to_plain(s['title'])}]\n"
        + (s["plain"] if "plain" in s else _html_to_plain(s.get("body", "")))
        for s in sections
    )


def _section_regime(state: str, confidence: float, action: str) -> dict:
    accent = "green" if "ON" in state.upper() or "BUY" in action.upper() else "red" if "OFF" in state.upper() or "SELL" in action.upper() else "amber"
    badge_cls = "badge-buy" if "BUY" in action.upper() else "badge-sell" if "SELL" in action.upper() else "badge-hold"
    return {
        "title": "Regime State",
        "body": (
            f'<span class="badge badge-regime">{html.escape(state)}</span> '
            f'&nbsp; Reported state confidence: <strong>{_confidence_text(confidence)}</strong><br><br>'
            f'Suggested Action: <span class="badge {badge_cls}">{html.escape(action)}</span>'
        ),
        "accent": accent,
    }


def _section_100x(
    ticker: str, direction: str, score: float, thesis: str, payoff: float = 0,
    confidence: str | None = None,
) -> dict:
    # options_mispricing_scans.confidence (LOW/MEDIUM/HIGH) must survive
    # into the rendered alert — without it, a LOW-confidence flagged row is
    # visually identical to a HIGH-confidence one.
    #
    # What this label actually is, made visible in the email itself (not
    # only here) per docs/reference/CONFIDENCE_POLICY.md (#544, adopted
    # 2026-09-17): discovery/options_scanner.py sets it from a fixed
    # threshold on the composite score (>=8.0/>=6.0), not from a scored
    # track record (hit rate, calibration, verified-vs-flagged count) —
    # the bar that policy sets for a field named "confidence". It doesn't
    # clear that bar, so every rendering below says "heuristic confidence"
    # and never the bare word, in every branch including MEDIUM/HIGH — a
    # HIGH bucket is still just a threshold on the same composite score,
    # not a calibrated high-confidence signal, and saying so only for LOW
    # would misleadingly imply the other two are measured. Renaming the
    # underlying field belongs to whoever next revises
    # discovery/options_scanner.py (#539's lane), not this fix.
    #
    # The column is `confidence TEXT NOT NULL` — verified against
    # schema.sql, which puts no CHECK constraint on this column (contrast
    # decision_journal.operator_confidence, which does constrain its values
    # to LOW/MEDIUM/HIGH). NOT NULL rules out None; it does NOT rule out an
    # unexpected non-LOW/MEDIUM/HIGH string reaching here — nothing in the
    # schema stops one being written, so the "unknown" branch below is a
    # real, reachable case (a typo, a future code path, a direct edit),
    # not purely defensive for a hypothetical legacy row.
    label = (confidence or "").strip().upper()
    if label == "LOW":
        conf_note = (
            ' &nbsp; <span class="badge badge-hold">HEURISTIC CONFIDENCE: LOW</span>'
        )
        accent = "amber"
    elif label in ("MEDIUM", "HIGH"):
        conf_note = (
            f' &nbsp; <span class="badge badge-regime">heuristic confidence: {label}</span>'
        )
        accent = "purple"
    else:
        conf_note = (
            ' &nbsp; <span class="badge badge-hold">HEURISTIC CONFIDENCE: UNKNOWN</span>'
        )
        accent = "amber"
    payoff_text = (
        f" &nbsp; Estimated payoff (model, not return): <strong>{payoff:.0f}x</strong>"
        if isinstance(payoff, (int, float)) and not isinstance(payoff, bool) and math.isfinite(payoff) and payoff > 0 else ""
    )
    return {
        "title": f"100x Opportunity — {html.escape(ticker)}",
        "body": (
            f'<span class="badge {"badge-buy" if direction == "CALL" else "badge-sell"}">'
            f'{html.escape(ticker)} {html.escape(direction)}S</span> &nbsp; '
            f'Composite score: <strong>{_score_text(score)}</strong>'
            f'{payoff_text}'
            f'{conf_note}'
            f'<br><br>{_text_html(thesis)}'
            f'<br><br><span style="font-size:11px;color:#5A7A96;">Confidence is a '
            f'scanner heuristic (a threshold on the composite score) — not a '
            f'calibrated or track-record-scored confidence.</span>'
        ),
        "accent": accent,
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
        "title": f"Agent Decision — {html.escape(ticker)}",
        "body": (
            f'<span class="badge {badge_cls}">{html.escape(decision)}</span><br><br>'
            f'{_text_html(reasoning)}'
        ),
        "accent": "green" if decision == "BUY" else "red" if decision == "SELL" else "amber",
    }


def _section_hypothesis(statement: str, status: str) -> dict:
    return {
        "title": f"Hypothesis — {status.upper()}",
        "body": _text_html(statement),
        "accent": "green" if status == "PASSED" else "red" if status == "FAILED" else "",
    }


def _section_briefing(briefing_type: str, excerpt: str) -> dict:
    return {
        "title": f"{briefing_type.title()} Market Briefing",
        "body": _text_html(excerpt),
    }


def _section_insight(title: str, content_preview: str) -> dict:
    return {
        "title": html.escape(title),
        "body": _text_html(content_preview),
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

# Sends still in flight. A one-shot process (a systemd oneshot, a cron script) used to exit while its
# daemon mail thread was still connecting, so the message was never sent and nothing was logged: the
# fleet heartbeat raised 1,322 alerts between 2026-05-25 and 2026-11-14 and delivered none.
_PENDING_SENDS: list[threading.Thread] = []
_PENDING_LOCK = threading.Lock()
EXIT_DRAIN_SECONDS = 45.0   # one SMTP attempt is capped at 30 s


def _send_in_thread(subject: str, html: str, plain: str) -> None:
    t = threading.Thread(target=_do_send, args=(subject, html, plain), daemon=True, name="grid-email")
    with _PENDING_LOCK:
        _PENDING_SENDS[:] = [p for p in _PENDING_SENDS if p.is_alive()]
        _PENDING_SENDS.append(t)
    t.start()


def _drain_pending_sends(timeout: float = EXIT_DRAIN_SECONDS) -> int:
    """Wait up to ``timeout`` seconds for in-flight sends; return how many were still running.

    Registered with ``atexit`` so a short-lived process delivers what it queued. The threads stay
    daemonic, so a hung SMTP server delays exit by at most ``timeout`` and never blocks it.
    """
    deadline = time.monotonic() + timeout
    with _PENDING_LOCK:
        pending = [t for t in _PENDING_SENDS if t.is_alive()]
    for t in pending:
        t.join(max(0.0, deadline - time.monotonic()))
    left = sum(1 for t in pending if t.is_alive())
    if left:
        log.warning("E-mail: {n} send(s) still in flight at exit after {s:.0f}s", n=left, s=timeout)
    return left


atexit.register(_drain_pending_sends)


def _validated_research_recipients(value: tuple[str, ...]) -> tuple[str, str]:
    """Two unique ASCII mailbox addresses, never a comma-joined envelope.

    This deliberately accepts simple addr-specs rather than display names,
    groups or quoted local parts. Normalize case/outer spaces and deduplicate
    before requiring exactly two. Never fall back to the global audience.
    """
    if not isinstance(value, (tuple, list)):
        raise ValueError("research audience must be an address sequence")
    unique: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or any(ord(c) < 32 or ord(c) >= 127 for c in raw):
            raise ValueError("research audience contains invalid characters")
        address = raw.strip().lower()
        if len(address) > 254 or not re.fullmatch(
            r"[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+", address,
        ):
            raise ValueError("research audience contains an invalid mailbox")
        local, domain = address.rsplit("@", 1)
        if len(local) > 64 or local.startswith(".") or local.endswith(".") or ".." in local:
            raise ValueError("research audience contains an invalid local part")
        if any(len(label) > 63 or label.startswith("-") or label.endswith("-") for label in domain.split(".")):
            raise ValueError("research audience contains an invalid domain")
        if address not in unique:
            unique.append(address)
    if len(unique) != 2:
        raise ValueError("research audience must contain exactly two unique mailboxes")
    return unique[0], unique[1]


def _do_send(
    subject: str, html: str, plain: str, *,
    research_recipients: tuple[str, ...] | None = None,
    delivery: dict[str, Any] | None = None,
) -> str:
    """Send over SMTP, returning one of three outcomes — not a bool.

    "sent": the relay accepted the message (for research, every envelope
        recipient). A partial research refusal is instead "uncertain";
        callers must not retry the whole audience. A later failure in
        ``quit()`` does not change a confirmed acceptance:
        QUIT is connection cleanup, not part of the accept decision, and
        treating its failure as "the send failed" was the exact bug this
        fixes — an accepted message could be reported as a failure and
        retried, risking a duplicate.
    "failed": a clean, pre-acceptance failure — mail disabled, or the
        connection/auth/HELO never completed, or the relay explicitly
        rejected the sender/recipients before any message data went
        out. Nothing was transmitted; safe to retry.
    "uncertain": the connection was lost, timed out, or otherwise
        interrupted during or after the DATA phase, so whether the relay
        actually received the message cannot be determined from here.
        Callers must not treat this as either success or a safe-to-retry
        failure.
    """
    audience: tuple[str, ...] = ()
    def outcome(label: str, accepted: Iterable[str] = (), refused: Iterable[str] = ()) -> None:
        if research_recipients is not None and delivery is not None:
            delivery.update({"outcome": label, "audience": list(audience),
                             "relay_accepted": list(accepted), "refused": list(refused)})
    try:
        if research_recipients is not None:
            if delivery is not None:
                delivery.clear()
            audience = _validated_research_recipients(research_recipients)
        cfg = _get_settings()
        if not cfg.ALERT_EMAIL_ENABLED:
            outcome("disabled")
            return "failed"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = cfg.ALERT_EMAIL_FROM
        msg["To"] = ", ".join(audience) if research_recipients is not None else cfg.ALERT_EMAIL_TO
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(html, "html"))

        host = cfg.ALERT_SMTP_HOST
        port = cfg.ALERT_SMTP_PORT
        use_tls = cfg.ALERT_SMTP_USE_TLS
        is_external = host not in ("localhost", "127.0.0.1", "::1")
        if is_external and not use_tls:
            use_tls = True

        smtp = smtplib.SMTP(host, port, timeout=30)
        if use_tls:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
        if cfg.ALERT_SMTP_USER and cfg.ALERT_SMTP_PASSWORD:
            smtp.login(cfg.ALERT_SMTP_USER, cfg.ALERT_SMTP_PASSWORD)
    except Exception as exc:
        outcome("failed")
        log.warning("Newsletter send failed before relay acceptance — {s}: {e}", s=subject, e=str(exc))
        return "failed"

    try:
        refused = smtp.sendmail(
            cfg.ALERT_EMAIL_FROM,
            list(audience) if research_recipients is not None else [cfg.ALERT_EMAIL_TO],
            msg.as_string(),
        )
    except (smtplib.SMTPRecipientsRefused, smtplib.SMTPSenderRefused, smtplib.SMTPResponseException) as exc:
        outcome("failed", refused=audience if isinstance(exc, smtplib.SMTPRecipientsRefused) else ())
        # An explicit rejection response — the relay told us no before
        # accepting the body. Clean, safe to retry.
        log.warning("Newsletter send rejected by relay — {s}: {e}", s=subject, e=str(exc))
        try:
            smtp.quit()
        except Exception:
            pass
        return "failed"
    except Exception as exc:
        outcome("uncertain")
        # Connection lost, timed out, or otherwise interrupted while
        # transmitting — genuinely unknown whether the relay got it.
        log.warning("Newsletter send outcome unknown — {s}: {e}", s=subject, e=str(exc))
        return "uncertain"

    status = "sent"
    if research_recipients is not None:
        # sendmail may accept one recipient while returning refusals for the
        # other. That is neither all-sent nor safely retryable for the pair.
        # Unknown result shapes/recipient keys are conservatively uncertain.
        if not isinstance(refused, dict) or any(
            not isinstance(key, str) or key not in audience
            or not isinstance(value, tuple) or len(value) != 2
            or not isinstance(value[0], int) or not 400 <= value[0] <= 599
            for key, value in refused.items()
        ):
            status = "uncertain"
            outcome("uncertain")
        elif refused:
            accepted = [address for address in audience if address not in refused]
            status = "uncertain" if accepted else "failed"
            outcome("partial" if accepted else "failed", accepted, refused.keys())
        else:
            outcome("accepted", audience)

    try:
        smtp.quit()
    except Exception as exc:
        log.debug(
            "SMTP quit() failed after a successful sendmail — message was already "
            "accepted, this does not change the outcome: {e}", e=str(exc),
        )

    if research_recipients is None:
        log.info("Newsletter sent — {s}", s=subject)
    else:
        log.info("Research newsletter relay outcome={status} — {s}", status=status, s=subject)
    return status


def _send(subject: str, sections: list[dict], footer_note: str = "") -> None:
    """Render and send a newsletter email (non-blocking)."""
    html = _render_html(subject, sections, footer_note)
    plain = _render_plain(sections)
    _send_in_thread(subject, html, plain)


def _send_sync(
    subject: str, sections: list[dict], footer_note: str = "", *,
    research_recipients: tuple[str, ...] | None = None,
    delivery: dict[str, Any] | None = None,
) -> str:
    """Render and send synchronously, returning "sent"/"failed"/"uncertain".

    ``_send`` exists to keep request handlers non-blocking; it fires a
    background thread and returns before ``_do_send`` even runs, so a
    caller can never learn whether the message was actually accepted —
    "sent" there means "handed to a thread," not "delivered." A caller
    running on its own background thread already (not a request path —
    e.g. the daily digest scheduler) should call this instead, so it can
    record a successful send only once one has actually happened, and
    treat an "uncertain" outcome as neither success nor a safe retry.
    """
    html = _render_html(subject, sections, footer_note)
    plain = _render_plain(sections)
    if research_recipients is None:
        return _do_send(subject, html, plain)
    return _do_send(subject, html, plain, research_recipients=research_recipients, delivery=delivery)


# ---------------------------------------------------------------------------
# Public API — Email types
# ---------------------------------------------------------------------------

def _alert_summary(subject: str, body: str, severity: str) -> dict:
    """Recognize existing health/fleet metrics; unknown layouts use severity."""
    headline, label, summary = severity.upper(), subject, _summary_text(body)
    source_count = re.match(r"(\d+) sources are past their own freshness window\b", body)
    failed_pulls = re.match(r"(\d+) pulls failed in the last hour\b", body)
    fleet = re.search(r"^Currently not-up \((\d+) of (\d+)\),[^\n]*", body, re.MULTILINE)
    if source_count:
        headline, label = source_count[1], f"Sources past freshness window · {severity.upper()}"
    elif failed_pulls:
        headline, label = failed_pulls[1], f"Failed pulls (last hour) · {severity.upper()}"
    elif fleet:
        headline, label = f"{fleet[1]}/{fleet[2]}", f"Fleet entities currently not up · {severity.upper()}"
        summary = fleet[0] + " " + summary
    return _section_summary(headline, label, summary,
                            "red" if severity.lower() in {"error", "critical"} else "amber")


def send_alert(subject: str, body: str, severity: str = "info") -> bool:
    """Send a simple alert email (backward compatible)."""
    try:
        cfg = _get_settings()
        if not cfg.ALERT_EMAIL_ENABLED:
            return False
        # Callers pass plain text. Escape it for the HTML part (reasons like
        # "RetryError[<Future ...>]" were parsed as tags and vanished) and
        # keep its line structure; the plain part carries it verbatim.
        section = _section_text(
            "Alert",
            '<div style="white-space:pre-wrap;font-family:ui-monospace,Menlo,Consolas,monospace;'
            f'font-size:12px;line-height:1.45;">{html.escape(body)}</div>',
        )
        section["plain"] = body
        _send(f"[GRID {severity.upper()}] {subject}", [
            _alert_summary(subject, body, severity),
            section,
        ])
        return True
    except Exception:
        return False


def _failure_sections(source: str, error: str, fix_note: str = "") -> list[dict]:
    return [
        _section_summary("FAILED", f"Data pull: {source}",
                         _summary_text(error) + (f" {fix_note}" if fix_note else ""), "red"),
        _section_text("Ingestion Failure", f"<strong>{html.escape(source)}</strong><br>Failure reported at "
                      f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}<br><br>"
                      f"{_text_html(error)}", accent="red"),
    ]


def alert_on_failure(source: str, error: str) -> None:
    _send(f"[GRID WARNING] Data pull failure — {source}", _failure_sections(source, error))


def alert_on_regime_change(from_regime: str, to_regime: str, confidence: float) -> None:
    _send(
        f"GRID Intelligence — Regime Shift: {from_regime} → {to_regime}",
        [
            _section_summary(to_regime, "New reported regime",
                             f"{from_regime} → {to_regime}. Reported state confidence: "
                             f"{_confidence_text(confidence)}. Review feature drivers before changing positioning.", "amber"),
            _section_regime(to_regime, confidence, "REVIEW"),
            _section_text("Transition", f"Market regime changed from <strong>{html.escape(from_regime)}</strong> "
                          f"to <strong>{html.escape(to_regime)}</strong> with {_confidence_text(confidence)} reported state confidence.<br><br>"
                          "Review the dashboard for feature drivers and recommended positioning."),
        ],
    )


def alert_on_100x_opportunity(
    ticker: str, score: float, direction: str, thesis: str, confidence: str | None = None,
) -> None:
    detail = _section_100x(ticker, direction, score, thesis, confidence=confidence)
    label = (confidence or "").strip().upper()
    heuristic = (f"heuristic confidence: {label}" if label in {"MEDIUM", "HIGH"}
                 else f"HEURISTIC CONFIDENCE: {label if label == 'LOW' else 'UNKNOWN'}")
    _send(
        f"GRID Intelligence — 100x Alert: {ticker} {direction}S",
        [_section_summary(_score_text(score), f"{ticker} {direction} composite score, not probability",
                          f"{heuristic}. {_summary_text(thesis)}", detail["accent"]), detail],
    )


def send_insight(category: str, title: str, content: str, metadata: dict | None = None) -> None:
    """Send a newsletter for a noteworthy LLM insight."""
    if content is None:
        return
    _send(
        f"GRID Intelligence — {title}",
        [_section_summary(_summary_text(title, 100), f"{category} insight", _summary_text(content), "purple"),
         _section_insight(title, content)],
        footer_note=f"Category: {html.escape(category)}",
    )


def send_agent_report(ticker: str, decision: str, reasoning: str,
                       regime_state: str, confidence: float, duration: float) -> None:
    """Send agent deliberation results as a newsletter."""
    _send(
        f"GRID Intelligence — {ticker}: Agent says {decision}",
        [
            _section_summary(decision, f"{ticker} agent verdict",
                             f"Regime: {regime_state}; reported state confidence: {_confidence_text(confidence)}. "
                             + _summary_text(reasoning), "amber" if decision == "HOLD" else ""),
            _section_regime(regime_state, confidence, decision),
            _section_agent_decision(ticker, decision, reasoning),
            _section_kpi("Analysis Time", f"{duration:.1f}s"),
        ],
    )


def _weekly_review_summary(content: str) -> dict:
    """Extract known review fields without turning text into invented metrics."""
    total = re.search(r"^\*\*Total insights:\*\*\s*(\d+)\s*$", content, re.MULTILINE)
    if not total:
        return _section_summary("Weekly review", "Research summary", _summary_text(content))
    period = re.search(r"^\*\*Period:\*\*\s*(.+)$", content, re.MULTILINE)
    highlights = [period[1].strip()] if period else ["Review period not supplied."]
    groups: dict[str, list[str]] = {}
    current = ""
    for raw in content.splitlines():
        line = raw.strip()
        if line.startswith("## "):
            current = line[3:]
            groups[current] = []
        elif current and line and not line.startswith("#"):
            groups[current].append(line.replace("**", "").lstrip("- "))
    for title in ("Longer-Term Patterns", "Regime Transitions", "Agent Decision Distribution", "Dominant Themes"):
        findings = [line for line in groups.get(title, [])
                    if not line.startswith(("Sufficient data for pattern analysis:", "---", "*Generated"))]
        if findings:
            highlights.append(f"{title}: {_summary_text(findings[0], 140)}")
    return _section_summary(total[1], "Insights in review period", " ".join(highlights))


def send_weekly_review(review_content: str) -> None:
    """Send the weekly insight review as a newsletter."""
    _send(
        "GRID Intelligence — Weekly Review",
        [_weekly_review_summary(review_content),
         _section_briefing("Weekly", review_content)],
    )


_STALE_REGIME_HOURS = 24


def _saved_board_note(board: dict, now: datetime) -> str:
    """Keep saved research provenance visible without claiming live eligibility."""
    raw = board.get("generated_at")
    generated = "generation time unknown; age unverified"
    if raw:
        try:
            ts = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                generated = f"generated {raw}; timezone unknown, age unverified"
            else:
                hours = (now - ts).total_seconds() / 3600
                generated = (f"generated {ts.astimezone(timezone.utc).isoformat()}; "
                             + ("future timestamp, age unverified" if hours < 0 else f"{hours:.1f}h ago"))
        except (TypeError, ValueError):
            generated = "generation time invalid; age unverified"
    data_date = board.get("as_of") or "unknown"
    return f"Saved board, not live eligibility. Data as of {data_date}; {generated}."


def _catalyst_date_label(raw: Any, now: datetime) -> str:
    if not raw:
        return "date unknown"
    try:
        day = datetime.fromisoformat(str(raw)).date()
        return f"{raw} (past date; outcome unconfirmed)" if day < now.date() else str(raw)
    except (TypeError, ValueError):
        return f"{raw} (date unverified)"


def daily_digest(
    dry_run: bool = False, *, research_recipients: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Compile and send a daily digest newsletter with live data.

    Parameters:
        dry_run: If True, build the digest content but never call ``_send``.
            Use this to preview a section count/titles/degraded-collector
            list against real data before relying on a live send — e.g.
            after a period where this function wasn't being invoked at all.
        research_recipients: Optional explicit research-only audience for
            controlled callers. Otherwise RESEARCH_DIGEST_EMAIL_TO is used.
            Exactly two validated unique mailboxes are required; empty or
            invalid values fail closed, never falling back to ALERT_EMAIL_TO.

    Returns:
        dict summarizing what was built: ``sections``, ``section_titles``,
        ``degraded`` (collector names whose query raised), and either
        ``dry_run`` or ``sent``/``error``. A truthy return does not mean
        every section built successfully — check ``degraded``.
    """
    # One UTC cutoff for this build, including its existing 24-hour and
    # scan-date window (D-3 through D inclusive). This excludes future-dated records;
    # it does not reconstruct historical vintages or prove PIT completeness.
    as_of = datetime.now(timezone.utc)
    result: dict[str, Any] = {"timestamp": as_of.isoformat()}
    try:
        audience = _validated_research_recipients(
            getattr(_get_settings(), "RESEARCH_DIGEST_EMAIL_TO", ())
            if research_recipients is None else research_recipients
        )
        result["research_recipients"] = list(audience)
        from sqlalchemy import text as sa_text
        from db import get_engine

        engine = get_engine()
        sections: list[dict] = []
        degraded: list[str] = []
        decision_count: int | None = None
        active_sources: int | None = None
        option_rows: list | None = None
        long_play_summary: str | None = None

        with engine.connect() as conn:
            # Regime — a missing, stale, or future-dated decision_journal
            # timestamp must never render through the normal "current
            # regime" branch. Age is unverifiable in the first two cases
            # (worse than merely stale) and untrustworthy in the third
            # (clock skew or bad data) — none of them are "current."
            try:
                row = conn.execute(sa_text(
                    "SELECT inferred_state, state_confidence, grid_recommendation, "
                    "decision_timestamp FROM decision_journal "
                    "WHERE decision_timestamp <= :as_of OR decision_timestamp IS NULL "
                    "ORDER BY decision_timestamp DESC LIMIT 1"
                ), {"as_of": as_of}).fetchone()
                if row:
                    ts = row[3]
                    ts = ts if (ts is None or ts.tzinfo) else ts.replace(tzinfo=timezone.utc)
                    age_hours = (
                        (as_of - ts).total_seconds() / 3600
                        if ts is not None else None
                    )
                    if ts is None:
                        sections.append(_section_text(
                            "Regime State — unverified",
                            f"decision_journal's latest row has no timestamp; age "
                            f"cannot be verified. Last known state: {html.escape(str(row[0]))}, "
                            f"suggested action was: {html.escape(str(row[2]))}.",
                            accent="amber",
                        ))
                    elif age_hours < 0:
                        sections.append(_section_text(
                            "Regime State — unverified",
                            f"decision_journal's latest timestamp ({ts}) is in the "
                            f"future — rejecting it rather than treating it as "
                            f"current. Last known state: {html.escape(str(row[0]))}, suggested "
                            f"action was: {html.escape(str(row[2]))}.",
                            accent="red",
                        ))
                    elif age_hours >= _STALE_REGIME_HOURS:
                        sections.append(_section_text(
                            "Regime State — stale",
                            f"last decision_journal entry {age_hours:.0f}h ago "
                            f"({ts}). Last known state: {html.escape(str(row[0]))}, "
                            f"suggested action was: {html.escape(str(row[2]))}.",
                            accent="amber",
                        ))
                    else:
                        sections.append(_section_regime(row[0], row[1], row[2]))
            except Exception as exc:
                degraded.append("regime")
                log.debug("Daily digest regime section failed: {e}", e=str(exc))

            # Journal count
            try:
                row = conn.execute(sa_text(
                    "SELECT COUNT(*) FROM decision_journal "
                    "WHERE decision_timestamp >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours' "
                    "AND decision_timestamp <= :as_of"
                ), {"as_of": as_of}).fetchone()
                decision_count = row[0] if row else None
                sections.append(_section_kpi("Decisions (24h)", str(decision_count) if decision_count is not None else "Unavailable"))
            except Exception as exc:
                degraded.append("journal_count")
                log.debug("Daily digest journal-count section failed: {e}", e=str(exc))

            # 100x opportunities
            try:
                rows = conn.execute(sa_text(
                    "SELECT ticker, direction, score, payoff_multiple, thesis, confidence "
                    "FROM options_mispricing_scans "
                    "WHERE is_100x = TRUE AND scan_date >= :as_of_date - 3 "
                    "AND scan_date <= :as_of_date "
                    "ORDER BY score DESC LIMIT 5"
                ), {"as_of_date": as_of.date()}).fetchall()
                option_rows = rows
                for r in rows:
                    sections.append(_section_100x(r[0], r[1], r[2], r[4], r[3], confidence=r[5]))
            except Exception as exc:
                degraded.append("100x_opportunities")
                log.debug("Daily digest 100x section failed: {e}", e=str(exc))

            # Data freshness — pull_status = 'SUCCESS' only. raw_series also
            # gets a row on a FAILED pull (value=0, a real pull_timestamp),
            # so counting every row regardless of status reports a source as
            # "active" off pulls that never actually produced an
            # observation. analysis/flow_thesis_data.py's raw_series reads
            # already filter to SUCCESS — this KPI didn't. That is NOT true
            # of most raw_series readers: the 2026-09-17 fake-data audit's
            # root cause #1 is that 22 analytical modules read raw_series by
            # obs_date alone, and #535 has migrated 7 of them so far,
            # leaving 18 legacy unfiltered readers as documented, open
            # follow-up work (see grid-fake-data-LEDGER.md). This fix covers
            # only this one KPI in this file, not that broader gap.
            try:
                row = conn.execute(sa_text(
                    "SELECT COUNT(DISTINCT source_id), MAX(pull_timestamp) FROM raw_series "
                    "WHERE pull_timestamp >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours' "
                    "AND pull_timestamp <= :as_of "
                    "AND pull_status = 'SUCCESS'"
                ), {"as_of": as_of}).fetchone()
                active_sources = row[0] if row else None
                if row and row[0]:
                    sections.append(_section_kpi("Active Sources (24h)", str(row[0]),
                                                 f"latest: {str(row[1])[:16]}"))
            except Exception as exc:
                degraded.append("data_freshness")
                log.debug("Daily digest freshness section failed: {e}", e=str(exc))

        # Long plays (multi-year board; every multiple is a labelled proxy)
        try:
            from intelligence.long_plays import entry_first, load_latest_board

            board = load_latest_board(engine)
            if board:
                board_note = _saved_board_note(board, as_of)
                lines: list[str] = []
                # Preserve the saved ordering: entry candidates first. These
                # recorded stances do not establish current entry eligibility.
                for cand in entry_first(board.get("candidates") or []):
                    if cand.get("stance") not in ("entry_candidate", "watch"):
                        continue
                    proj = ((cand.get("projection") or {}).get("3y") or {})
                    p50 = proj.get("p50_multiple")
                    p50_txt = f"{p50:.2f}x" if isinstance(p50, (int, float)) and math.isfinite(p50) else "n/a"
                    gate = cand.get("gate") or {}
                    cats = cand.get("catalysts") or []
                    first_cat = (
                        f"{cats[0].get('event_type')} {_catalyst_date_label(cats[0].get('expected_date'), as_of)}"
                        if cats
                        else (
                            f"catalyst {_catalyst_date_label(gate['next_catalyst_date'], as_of)}"
                            if gate.get("next_catalyst_date") else "no catalyst"
                        )
                    )
                    route = cand.get("coverage_route")
                    route_txt = f" — via {route}" if route else ""
                    lines.append(
                        f"{cand.get('ticker')} — saved stance: {cand.get('stance')}{route_txt} — "
                        f"p50 3y {p50_txt} (proxy) — {first_cat}"
                    )
                    if len(lines) >= 5:
                        break
                if lines:
                    long_play_summary = board_note + " First saved row: " + lines[0]
                    sections.append(_section_text(
                        "Long plays",
                        _text_html(board_note + "\nShowing up to 5 saved candidates, entry candidates first.\n"
                                   + "\n".join(lines) + "\n(proxy multiples from historical CAGR/vol; not forecasts)"),
                        accent="#7c5cff",
                    ))
                elif board.get("stand_down_reason"):
                    long_play_summary = board_note + " " + _summary_text(str(board["stand_down_reason"]), 160)
                    sections.append(_section_text("Long plays", _text_html(board_note + "\n" + str(board["stand_down_reason"]))))
        except Exception as exc:
            degraded.append("long_plays")
            log.debug("Daily digest long-plays section failed: {e}", e=str(exc))

        # A collector failure must never be silently indistinguishable from
        # "nothing to report" — that's how a digest ends up claiming
        # "All systems operational" while its own queries were failing.
        if degraded:
            sections.append(_section_text(
                "Data collection issues",
                f"{len(degraded)} section(s) failed to build and are omitted "
                f"above: {', '.join(degraded)}. This digest is incomplete.",
                accent="red",
            ))
        elif not sections:
            sections.append(_section_text(
                "Status",
                "No reportable records returned. This does not establish system health.",
            ))

        # Summarize the already-gated evidence, never bypass freshness checks
        # or invent totals from capped query rows. No extra database queries.
        regime_section = next((s for s in sections if s["title"].startswith("Regime State")), None)
        if regime_section:
            regime_summary = f"{regime_section['title']}: {_summary_text(_html_to_plain(regime_section['body']), 250)}"
        else:
            regime_summary = "Regime unavailable; no current state can be established."
        summary = [regime_summary]
        summary.append(f"Decisions (24h): {decision_count if decision_count is not None else 'unavailable'}.")
        summary.append(f"{active_sources} sources with successful pulls (24h)." if active_sources is not None
                       else "Sources with successful pulls (24h): unavailable.")
        if option_rows is not None:
            summary.append(f"{len(option_rows)} sampled option rows (up to 5, not a total; scan dates D-3 through D inclusive).")
            if option_rows:
                top = option_rows[0]
                confidence = str(top[5] or "").upper()
                confidence = confidence if confidence in {"LOW", "MEDIUM", "HIGH"} else "UNKNOWN"
                summary.append(f"Top sampled row: {top[0]} {top[1]}, {_score_text(top[2])} composite score (not probability); heuristic confidence: {confidence}.")
        else:
            summary.append("Option scan sample unavailable.")
        if long_play_summary:
            summary.append("Long plays: " + long_play_summary)
        if degraded:
            summary.insert(0, f"Incomplete collection: {', '.join(degraded)}.")
        sections.insert(0, _section_summary(
            "INCOMPLETE" if degraded else str(decision_count) if decision_count is not None else "RESEARCH UPDATE",
            "Research digest; incomplete data" if degraded else "Decisions recorded (24h)" if decision_count is not None else "Research status",
            " ".join(summary), "red" if degraded else "amber" if not regime_section or regime_section["title"] != "Regime State" else "",
        ))

        subject = "GRID Intelligence — Daily Digest"
        result.update({
            "sections": len(sections),
            "section_titles": [s["title"] for s in sections],
            "degraded": degraded,
        })

        if dry_run:
            result["dry_run"] = True
            # Full content, not just titles — this is what a live check
            # verifying timestamps/staleness/missing-data behavior needs
            # to actually inspect; titles alone can't show a regime's
            # rendered age or a degraded section's message text.
            result["section_bodies"] = [
                {"title": s["title"], "body": s.get("body", ""), "accent": s.get("accent", "")}
                for s in sections
            ]
            log.info("Daily digest built (dry run) — {n} sections, {d} degraded",
                      n=len(sections), d=len(degraded))
            return result

        # Synchronous send: daily_digest() already runs on the scheduler's
        # own background thread (alerts/scheduler.py), never a request
        # handler, so blocking here on real SMTP I/O is safe — and it's
        # what lets the caller record a claim only after an actual
        # confirmed outcome instead of after merely dispatching one.
        delivery: dict[str, Any] = {}
        send_status = _send_sync(subject, sections, research_recipients=audience, delivery=delivery)
        result["delivery"] = delivery
        result["send_status"] = send_status
        result["sent"] = send_status == "sent"
        if send_status != "sent":
            result["error"] = f"send status: {send_status}"
        log.info("Daily digest send status={status} — {n} sections, {d} degraded",
                  status=send_status, n=len(sections), d=len(degraded))
    except Exception as exc:
        result["send_status"] = "failed"
        result["sent"] = False
        result["error"] = str(exc)
        log.warning("Daily digest failed: {e}", e=str(exc))
    return result


def _section_code_block(title: str, code: str) -> dict:
    """Render a code block section with copy-paste styling."""
    # Escape HTML in code content
    safe_code = code.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return {
        "title": title,
        "plain": code,
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
    has_commands = bool(fix_commands and any(fix_commands.get(k) for k in ("diagnose", "fix", "retry", "file")))
    sections = _failure_sections(
        source, error,
        "Suggested repair commands included below; not executed." if has_commands else "No fix commands supplied.",
    )

    if fix_commands:
        claude_prompt = f"The {source} data pull failed with this error:\n\n"
        claude_prompt += f"```\n{error}\n```\n\n"
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

    _send(f"[GRID WARNING] {source} failed" + (" — fix commands included" if has_commands else ""), sections)


def alert_on_transition_leaders(leaders: list[dict], cluster_result: dict) -> None:
    """Alert when transition leader features are identified."""
    if not leaders:
        return
    rows = ""
    for leader in leaders:
        t_stat = leader.get("t_stat")
        t_stat_text = f"{t_stat:.2f}" if isinstance(t_stat, (int, float)) and not isinstance(t_stat, bool) and math.isfinite(t_stat) else "unavailable"
        rows += (
            f"<tr><td>{html.escape(str(leader.get('feature', '?')))}</td>"
            f"<td>{html.escape(str(leader.get('lead_weeks', '?')))}w</td>"
            f"<td>{html.escape(str(leader.get('direction', '?')))}</td>"
            f"<td>{t_stat_text}</td></tr>"
        )

    _send(
        "GRID Intelligence — Transition Leaders Detected",
        [
            _section_summary(str(len(leaders)), "Supplied transition leaders",
                             f"{len(leaders)} supplied transition leaders. First listed: {leaders[0].get('feature', '?')}, "
                             f"{leaders[0].get('lead_weeks', '?')} weeks lead, direction {leaders[0].get('direction', '?')}. "
                             "Historical associations, not guaranteed forecasts.", "purple"),
            _section_text(
                "Cluster Transition Signals",
                f"Best k={html.escape(str(cluster_result.get('best_k', '?')))} clusters detected. "
                "These features were associated with regime transitions:",
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
    sections = [
        _section_summary(_summary_text(title, 100), "Discovery finding", _summary_text(description), "amber"),
        _section_text("Discovery Insight", _text_html(description), accent="amber"),
    ]
    if data and data.get("by_family"):
        family_text = "<br>".join(
            f"&bull; <strong>{html.escape(str(k))}</strong>: {len(v)} features"
            for k, v in data["by_family"].items()
        )
        sections.append(_section_text("Feature Taxonomy", family_text))
    _send(f"GRID Intelligence — {title}", sections)


# ---------------------------------------------------------------------------
# Anomaly report (intelligence/anomaly_report.py -> one e-mail)
# ---------------------------------------------------------------------------

_VERDICT_TITLE = {"sharp_move": "Sharp move", "new_extreme": "New extreme", "stretched": "Stretched"}
_ANOMALY_MAX_MOVE_CARDS = 6
_ANOMALY_MAX_TABLE_ROWS = 12
_CELL = "padding:6px 8px 6px 0;font-size:13px;color:#C8D8E8;border-bottom:1px solid #1A2A3A;"
_HEAD = "padding:6px 8px 6px 0;font-size:11px;color:#5A7A96;text-align:left;border-bottom:1px solid #1A2A3A;"
_NOTE = "font-size:13px;color:#9AB2C8;line-height:1.5;"


def _an_num(value: Any) -> str:
    """A level, sized to its magnitude: 5,273 / 162.85 / 0.0675."""
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        return "n/a"
    mag = abs(value)
    if mag >= 1000:
        return f"{value:,.0f}"
    if mag >= 10:
        return f"{value:,.2f}"
    if mag >= 1:
        return f"{value:.3f}"
    return f"{value:.4f}"


def _an_pct(value: Any, signed: bool = True) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        return "n/a"
    return f"{value * 100:+.1f}%" if signed else f"{value * 100:.0f}%"


def _an_day(iso: Any) -> str:
    """'2026-11-13' -> 'Nov 13'; anything unparseable comes back as it was."""
    try:
        d = datetime.strptime(str(iso)[:10], "%Y-%m-%d")
    except ValueError:
        return str(iso) if iso else "n/a"
    return f"{d.strftime('%b')} {d.day}"


def _an_day_year(iso: Any) -> str:
    try:
        d = datetime.strptime(str(iso)[:10], "%Y-%m-%d")
    except ValueError:
        return str(iso) if iso else "n/a"
    return f"{d.strftime('%b')} {d.day}, {d.year}"


def _an_change(change: dict | None) -> str:
    if not change or change.get("value") is None:
        return "n/a"
    if change.get("unit") == "pct":
        return _an_pct(change["value"])
    value = change["value"]
    return f"{value:+.3f}" if abs(value) < 10 else f"{value:+,.2f}"


def _an_change_note(change: dict | None) -> str:
    """How unusual a change is against the feature's own changes."""
    z = (change or {}).get("robust_z")
    if z is None:
        return ""
    if abs(z) >= 2.5:
        return f"rare ({abs(z):.1f}σ)"
    if abs(z) >= 1.5:
        return f"large ({abs(z):.1f}σ)"
    return "ordinary"


def _an_target(name: Any) -> str:
    text = str(name or "")
    for suffix in ("_usd_full", "_full", "_close"):
        if text.lower().endswith(suffix):
            return text[: -len(suffix)].replace("_", "-").upper()
    return text.upper()


def _an_situation(item: dict) -> str:
    z = item.get("zscore") or 0.0
    side = "above" if z >= 0 else "below"
    return (f"{_an_num(item.get('value'))} on {_an_day(item.get('obs_date'))}: {abs(z):.1f}σ {side} "
            f"its one-year average of {_an_num(item.get('mean'))}.")


def _an_excursion(item: dict, threshold: Any) -> str:
    ex = item.get("excursion") or {}
    n = ex.get("sessions")
    if not n:
        return ""
    plural = "session" if n == 1 else "sessions"
    text = f"Beyond {threshold}σ for {n} {plural}, since {_an_day(ex.get('since'))}."
    if item.get("verdict") == "stretched":
        text += " A one-year trend, not a new event."
    return text


def _an_range(item: dict) -> str:
    r = item.get("range") or {}
    if not r or r.get("high") is None:
        return ""
    return (f"One-year range {_an_num(r.get('low'))} ({_an_day(r.get('low_date'))}) to "
            f"{_an_num(r.get('high'))} ({_an_day(r.get('high_date'))}): now {_an_pct(r.get('off_high'))} "
            f"from the high, {_an_pct(r.get('off_low'))} from the low.")


def _an_history(h: dict | None) -> str:
    """One precedent read in a sentence or two; every number carries its n."""
    h = h or {}
    if not h:
        return ""
    basis = str(h.get("basis") or "level")
    side = h.get("direction", "above")
    if basis.startswith("change_"):
        cut = h.get("cut")
        size = _an_pct(cut, signed=False) if h.get("unit") == "pct" else _an_num(cut)
        if h.get("unit") == "pct" and isinstance(cut, (int, float)):
            size = f"{cut * 100:.1f}%"
        event = f"it {'rose' if side == 'above' else 'fell'} {size} or more in {basis.split('_')[1]} sessions"
    else:
        what = f"this far {side} its trailing year"
        event = f"it was {what}"
    if not h.get("available"):
        return f"No precedent read: {h.get('reason', 'unavailable')}."
    target = _an_target(h.get("target"))
    since = _an_day_year(h.get("since"))
    n = h.get("n", 0)
    if n == 0:
        return f"No precedent since {since}: the first time {event}."
    long = (h.get("horizons") or {}).get("fwd_20") or {}
    short = (h.get("horizons") or {}).get("fwd_5") or {}
    if not h.get("enough"):
        cases = ", ".join(
            f"{_an_day_year(e.get('date'))} {_an_pct(e.get('fwd_20'))}"
            for e in h.get("episodes") or [] if e.get("fwd_20") is not None)
        tail = f" {target} 20 sessions after: {cases}." if cases else ""
        return (f"{n} prior time{'s' if n != 1 else ''} {event} since {since}: "
                f"too few to read.{tail}")
    parts = []
    for sessions, row in ((20, long), (5, short)):
        if not row.get("n"):
            continue
        ups = round((row.get("share_up") or 0.0) * row["n"])
        parts.append(
            f"{sessions} sessions later {target} was higher in {ups} of {row['n']} "
            f"(median {_an_pct(row.get('median'))}, worst {_an_pct(row.get('worst'))}, "
            f"best {_an_pct(row.get('best'))}); any {sessions}-session window: "
            f"higher {_an_pct(row.get('baseline_share_up'), signed=False)}, "
            f"median {_an_pct(row.get('baseline_median'))}")
    return (f"The {n} prior times {event} (since {since}): " + ". ".join(parts)
            + ". A base rate, not a tested rule.")


def _an_move_section(item: dict, threshold: Any) -> dict:
    changes = item.get("changes") or {}
    rows = [(f"{h} session{'s' if h != 1 else ''}", changes.get(f"d{h}")) for h in (1, 5, 20)]
    cells = "".join(
        f'<td style="{_CELL}"><span style="font-size:11px;color:#5A7A96;">{label}</span><br>'
        f'<strong style="color:#E8F0F8;font-size:15px;">{html.escape(_an_change(ch))}</strong><br>'
        f'<span style="font-size:11px;color:#9AB2C8;">{html.escape(_an_change_note(ch))}</span></td>'
        for label, ch in rows)
    situation, excursion = _an_situation(item), _an_excursion(item, threshold)
    span = _an_range(item)
    reads = [r for r in (_an_history(item.get("move_history")), _an_history(item.get("history"))) if r]
    context = " ".join(x for x in (excursion, span) if x)
    body = (f'<p style="margin:0 0 10px;">{html.escape(situation)}</p>'
            f'<table style="width:100%;border-collapse:collapse;margin-bottom:10px;"><tr>{cells}</tr></table>'
            + (f'<p style="margin:0 0 10px;">{html.escape(context)}</p>' if context else "")
            + (f'<p style="margin:0 0 6px;{_NOTE}"><strong>What followed before.</strong></p>' if reads else "")
            + "".join(f'<p style="margin:0 0 6px;{_NOTE}">{html.escape(r)}</p>' for r in reads))
    plain_changes = "; ".join(
        f"{label} {_an_change(ch)}" + (f" ({_an_change_note(ch)})" if _an_change_note(ch) else "")
        for label, ch in rows)
    plain = "\n".join(x for x in (situation, plain_changes, context,
                                  "What followed before." if reads else "", *(f"- {r}" for r in reads)) if x)
    title = f"{_VERDICT_TITLE.get(item.get('verdict'), 'Flagged')} — {item.get('label', item.get('feature'))}"
    return {"title": html.escape(title), "body": body, "plain": plain,
            "accent": "amber" if item.get("verdict") == "sharp_move" else "purple"}


def _an_table_section(title: str, intro: str, items: list[dict], accent: str = "") -> dict:
    shown, extra = items[:_ANOMALY_MAX_TABLE_ROWS], len(items) - _ANOMALY_MAX_TABLE_ROWS
    rows, plain_rows = "", []
    for it in shown:
        z = it.get("zscore") or 0.0
        sessions = (it.get("excursion") or {}).get("sessions")
        d20 = _an_change((it.get("changes") or {}).get("d20"))
        rows += (f'<tr><td style="{_CELL}">{html.escape(str(it.get("label", it.get("feature"))))}</td>'
                 f'<td style="{_CELL}">{html.escape(_an_num(it.get("value")))}</td>'
                 f'<td style="{_CELL}">{z:+.1f}σ</td>'
                 f'<td style="{_CELL}">{html.escape(d20)}</td>'
                 f'<td style="{_CELL}">{sessions if sessions is not None else "n/a"}</td></tr>')
        plain_rows.append(f"- {it.get('label', it.get('feature'))}: {_an_num(it.get('value'))}, {z:+.1f}σ, "
                          f"20 sessions {d20}, beyond for {sessions if sessions is not None else 'n/a'} sessions")
    more = f"{extra} more not shown." if extra > 0 else ""
    body = (f'<p style="margin:0 0 10px;{_NOTE}">{html.escape(intro)}</p>'
            f'<table style="width:100%;border-collapse:collapse;"><thead><tr>'
            f'<th style="{_HEAD}">Feature</th><th style="{_HEAD}">Now</th><th style="{_HEAD}">vs 1-yr</th>'
            f'<th style="{_HEAD}">20 sessions</th><th style="{_HEAD}">Sessions beyond</th>'
            f'</tr></thead><tbody>{rows}</tbody></table>'
            + (f'<p style="margin:8px 0 0;{_NOTE}">{more}</p>' if more else ""))
    return {"title": html.escape(title), "body": body, "accent": accent,
            "plain": "\n".join([intro, *plain_rows] + ([more] if more else []))}


def _an_names(names: list[str], limit: int = 6) -> str:
    shown = ", ".join(names[:limit])
    return shown + (f" (+{len(names) - limit} more)" if len(names) > limit else "")


def _an_fault_text(fault: dict, items_by_feature: dict[str, dict]) -> str:
    label = fault.get("label", fault.get("id", "series"))
    if fault.get("kind") in ("split", "reverse_split"):
        ratio = fault.get("ratio")
        z = items_by_feature.get(fault.get("feature", ""), {}).get("zscore")
        if fault.get("shape") == "basis_break":
            reading = f"The {abs(z):.1f}σ reading is that break, not a market move. " if z is not None else ""
            return (f"{label}: the level changed by a factor of about {ratio} on {_an_day(fault.get('date'))} "
                    f"and stayed there ({_an_num(fault.get('before'))} to {_an_num(fault.get('after'))}). "
                    f"That is the series changing source or unit. {reading}"
                    f"Every level reading on this series is wrong until it is put on one basis.")
        shape = f"{ratio}-for-1 split" if fault["kind"] == "split" else f"1-for-{ratio} reverse split"
        reading = f"The {abs(z):.1f}σ reading is the split, not a market move. " if z is not None else ""
        return (f"{label}: looks like a {shape} on {_an_day(fault.get('date'))} that the series was not "
                f"adjusted for ({_an_num(fault.get('before'))} to {_an_num(fault.get('after'))}). {reading}"
                f"Every level reading on this series is wrong until it is re-based.")
    text = (f"{label}: {fault.get('collapsed')} of {fault.get('members')} series have been under a fifth of "
            f"their own normal since {_an_day(fault.get('onset'))} "
            f"(median {_an_pct(fault.get('median_ratio'), signed=False)} of normal). "
            f"That is the feed, not the market.")
    flagged = [items_by_feature.get(n, {}).get("label", n) for n in fault.get("flagged") or []]
    suspect = [items_by_feature.get(n, {}).get("label", n) for n in fault.get("suspect") or []]
    if flagged:
        text += f" Flagged because of it: {_an_names(flagged)}."
    if suspect:
        text += f" Computed from the same chains, treat as unverified: {_an_names(suspect)}."
    return text


def anomaly_report_headline(report: dict) -> tuple[str, str]:
    """(counts line, one-paragraph summary) for the top card and the subject."""
    items = report.get("items") or []
    counts = report.get("counts") or {}
    moves = [i for i in items if i.get("verdict") in ("sharp_move", "new_extreme")]
    n_fault = counts.get("data_fault", 0) + counts.get("suspect", 0) + counts.get("stale", 0)
    n_stretched = counts.get("stretched", 0)
    bits = [f"{len(moves)} new move{'s' if len(moves) != 1 else ''}", f"{n_stretched} stretched"]
    if n_fault:
        bits.append(f"{n_fault} bad-data reading{'s' if n_fault != 1 else ''}")
    sentences: list[str] = []
    if moves:
        top = moves[0]
        z = top.get("zscore") or 0.0
        d20 = (top.get("changes") or {}).get("d20")
        move = f" after {_an_change(d20)} in 20 sessions" if d20 and d20.get("value") is not None else ""
        sentences.append(f"{top.get('label')} is {abs(z):.1f}σ {'above' if z >= 0 else 'below'} "
                         f"its one-year average{move}.")
        if len(moves) > 1:
            sentences.append("Also new: " + _an_names([str(m.get("label")) for m in moves[1:]], 4) + ".")
    else:
        sentences.append("No new market move in this scan.")
    if n_fault:
        sentences.append(f"{n_fault} of the {report.get('flagged', len(items))} flagged readings come from "
                         f"bad data (a split, a broken feed or a stale series), not from the market.")
    if n_stretched:
        sentences.append(f"{n_stretched} have sat beyond the threshold for more than a week: nothing new.")
    return " · ".join(bits), " ".join(sentences)


def anomaly_report_subject(report: dict) -> str:
    items = report.get("items") or []
    moves = [i for i in items if i.get("verdict") in ("sharp_move", "new_extreme")]
    n_fault = len(report.get("faults") or []) + sum(1 for i in items if i.get("verdict") == "stale")
    if moves:
        top = moves[0]
        d20 = (top.get("changes") or {}).get("d20")
        if d20 and d20.get("value") is not None and d20.get("unit") == "pct":
            lead = f"{top.get('label')} {_an_change(d20)} in 20 sessions"
        else:
            lead = f"{top.get('label')} {(top.get('zscore') or 0.0):+.1f}σ"
        if len(moves) > 1:
            lead += f", +{len(moves) - 1} more"
    else:
        lead = "no new market move"
    if n_fault:
        lead += f"; {n_fault} data fault{'s' if n_fault != 1 else ''}"
    return f"GRID anomalies {_an_day(report.get('latest_obs') or report.get('as_of'))}: {lead}"


def anomaly_report_sections(report: dict) -> list[dict]:
    """The report as newsletter sections (situation first, faults and method last)."""
    items = report.get("items") or []
    threshold = report.get("threshold", 2.5)
    by_feature = {str(i.get("feature")): i for i in items}
    moves = [i for i in items if i.get("verdict") in ("sharp_move", "new_extreme")]
    stretched = [i for i in items if i.get("verdict") == "stretched"]
    counts_line, summary = anomaly_report_headline(report)
    scanned = report.get("features_scanned")
    label = (f"Anomaly scan, data through {_an_day_year(report.get('latest_obs') or report.get('as_of'))} · "
             f"{report.get('flagged', len(items))} flagged"
             + (f" of {scanned:,} features" if isinstance(scanned, int) else ""))
    sections = [_section_summary(counts_line, label, summary, "amber" if moves else "")]

    sections += [_an_move_section(i, threshold) for i in moves[:_ANOMALY_MAX_MOVE_CARDS]]
    if len(moves) > _ANOMALY_MAX_MOVE_CARDS:
        sections.append(_an_table_section(
            "More new moves", "Also newly beyond the threshold.", moves[_ANOMALY_MAX_MOVE_CARDS:], "purple"))
    if stretched:
        sections.append(_an_table_section(
            "Stretched, nothing new",
            f"Beyond {threshold}σ of the one-year average for more than a week. For a price that has "
            "trended all year this is the trend showing up in the average, not an event.", stretched))

    faded = [i for i in items if i.get("verdict") == "faded"]
    if faded:
        text = (f"Flagged on the scan's shared data date ({_an_day(report.get('as_of'))}) and back inside "
                f"{threshold}σ on their own latest observation: "
                + ", ".join(f"{i.get('label')} ({(i.get('zscore') or 0.0):+.1f}σ)" for i in faded) + ".")
        sections.append({"title": "Back inside the threshold", "plain": text,
                         "body": f'<p style="margin:0;{_NOTE}">{html.escape(text)}</p>'})

    faults = report.get("faults") or []
    stale = [i for i in items if i.get("verdict") == "stale"]
    if faults or stale:
        texts = [_an_fault_text(f, by_feature) for f in faults]
        texts += [
            f"{i.get('label')}: no observation since {_an_day((i.get('fault') or {}).get('last_obs'))} "
            f"({(i.get('fault') or {}).get('days_behind')} days). The scan is reading a carried-forward "
            f"value, not a new one."
            for i in stale]
        sections.append({
            "title": "Data faults: not market signals",
            "body": "".join(f'<p style="margin:0 0 10px;">{html.escape(t)}</p>' for t in texts),
            "plain": "\n".join(f"- {t}" for t in texts),
            "accent": "red",
        })

    method = (f"A feature is flagged when its latest value is more than {threshold}σ from its one-year "
              "average. Each change is rated against the feature's own past changes. \"What followed "
              "before\" counts earlier crossings of the same threshold and is a description of the past "
              "with its sample size, not a trading rule that has been tested.")
    sections.append({"title": "How to read this", "plain": method,
                     "body": f'<p style="margin:0;{_NOTE}">{html.escape(method)}</p>'})
    return sections


def send_anomaly_report(report: dict) -> None:
    """E-mail one anomaly report (``intelligence.anomaly_report.build_report``)."""
    if not report or not report.get("items"):
        return
    _send(anomaly_report_subject(report), anomaly_report_sections(report))


def send_test_email() -> bool:
    """Send a test newsletter showcasing the template and expansion plan."""
    sections = [
        _section_summary("PREVIEW", "Synthetic demonstration, not live market data",
                         "All market signals, scores, returns and pricing below are illustrative test content. "
                         "This preview is not a trading recommendation or a live performance report.", "amber"),
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
