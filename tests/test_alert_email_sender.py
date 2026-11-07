"""GRID's outbound mail identity is the Hermes mailbox (2026-09-10).

Digests used to be sent as ``grid-alerts@grid-svr`` through local Postfix, a
bare sender Gmail drops silently. The default From is now
``hermes@stepdad.finance``; the credentials stay in the server ``.env`` and
``alerts/email.py`` forces STARTTLS for any non-localhost host.
"""
from __future__ import annotations

import html
from pathlib import Path
from types import SimpleNamespace

import pytest

import alerts.email as email_mod

ROOT = Path(__file__).resolve().parents[1]


def test_default_sender_is_the_hermes_mailbox() -> None:
    from config import Settings

    assert Settings.model_fields["ALERT_EMAIL_FROM"].default == "hermes@stepdad.finance"
    # Transport defaults stay local; the authenticated relay is configured in .env.
    assert Settings.model_fields["ALERT_SMTP_PASSWORD"].default == ""


def test_env_example_documents_the_relay_without_a_secret() -> None:
    env = (ROOT / ".env.example").read_text(encoding="utf-8")
    for key in (
        "ALERT_EMAIL_FROM=hermes@stepdad.finance",
        "ALERT_SMTP_USER=hermes@stepdad.finance",
        "ALERT_SMTP_PORT=587",
        "ALERT_SMTP_USE_TLS=true",
        "ALERT_SMTP_PASSWORD=",
    ):
        assert key in env, key
    line = [item for item in env.splitlines() if item.startswith("ALERT_SMTP_PASSWORD=")][0]
    assert line.strip() == "ALERT_SMTP_PASSWORD=", "the template must not carry a password"


def test_external_smtp_forces_tls() -> None:
    src = (ROOT / "alerts/email.py").read_text(encoding="utf-8")
    assert 'is_external = host not in ("localhost", "127.0.0.1", "::1")' in src
    assert "if is_external and not use_tls:\n            use_tls = True" in src

# Core template regression tests. All payloads/transports are synthetic.


@pytest.fixture
def captured_email(monkeypatch):
    captured = {}
    monkeypatch.setattr(email_mod, '_get_settings', lambda: SimpleNamespace(ALERT_EMAIL_ENABLED=True))
    monkeypatch.setattr(email_mod, '_send', lambda subject, sections, **kwargs: captured.update(subject=subject, sections=sections))
    return captured


@pytest.mark.parametrize('sender,args,headline', [
    ('send_alert', ('Health', '3 failed checks. Details follow.\n' + 'trace ' * 250 + '<Future final>', 'warning'), 'WARNING'),
    ('alert_on_failure', ('SYN', 'Timeout. ' + 'trace ' * 250 + '<Future final>'), 'FAILED'),
    ('alert_on_failure_with_fix', ('SYN', 'Timeout. ' + 'trace ' * 250 + '<Future final>', {'retry': 'retry <source>'}), 'FAILED'),
    ('alert_on_regime_change', ('RISK_OFF', 'RISK_ON', .81), 'RISK_ON'),
    ('alert_on_100x_opportunity', ('SYN', 8.5, 'CALL', 'Mispricing detected. ' + 'detail ' * 250 + '<Future final>'), '8.5/10'),
    ('send_insight', ('macro', 'Credit spreads', 'Spreads widened. ' + 'detail ' * 250 + '<Future final>'), 'Credit spreads'),
    ('send_agent_report', ('SYN', 'HOLD', 'Wait for earnings. ' + 'detail ' * 250 + '<Future final>', 'MIXED', .61, 12.5), 'HOLD'),
    ('send_weekly_review', ('Credit tightened this week. ' + 'detail ' * 250 + '<Future final>',), 'Weekly review'),
    ('alert_on_transition_leaders', ([{'feature': 'Credit', 'lead_weeks': 3, 'direction': 'up', 't_stat': 2.1}], {'best_k': 4}), '1'),
    ('alert_on_discovery_insight', ('Redundancy', 'Rates overlap. ' + 'detail ' * 250 + '<Future final>'), 'Redundancy'),
    ('send_test_email', (), 'PREVIEW'),
])
def test_every_core_template_leads_with_summary_and_retains_full_source(captured_email, sender, args, headline):
    getattr(email_mod, sender)(*args)
    sections = captured_email['sections']
    assert sections[0]['title'] == 'At a glance'
    assert headline in sections[0]['plain']
    assert len(sections[0]['plain']) < 900
    rendered = email_mod._render_html(captured_email['subject'], sections)
    assert rendered.index('At a glance') < rendered.index(f"class=\"card-title\">{sections[1]['title']}")
    for arg in args:
        if isinstance(arg, str) and '<Future final>' in arg:
            assert html.escape(arg) in rendered
            assert '<Future final>' not in rendered
    if sender == 'send_test_email':
        assert 'synthetic' in sections[0]['plain'].lower()
        assert 'not live' in sections[0]['plain'].lower()


def test_extract_summary_is_short_without_altering_source_numbers():
    source = '7/19 nodes failed. Retry the unhealthy nodes. ' + 'Long detail. ' * 100
    summary = email_mod._summary_text(source)
    assert summary.startswith('7/19 nodes failed.')
    assert len(summary) <= 320
    assert 'Long detail' not in summary
    assert email_mod._summary_text('') == 'No narrative provided.'
    assert '<Future pending>' in email_mod._summary_text('Failure <Future pending>')


@pytest.mark.parametrize('synchronous', [False, True])
def test_transport_plain_fallback_is_readable_and_explicit_plain_is_unchanged(monkeypatch, synchronous):
    captured = {}
    def capture(subject, rendered, plain, **kwargs):
        captured.update(rendered=rendered, plain=plain)
        return 'sent'
    monkeypatch.setattr(email_mod, '_do_send' if synchronous else '_send_in_thread', capture)
    sections = [
        {'title': 'Details', 'body': '<style>hide me</style><p>First &amp; second</p><table><tr><td>SYN</td><td>8.5</td></tr></table><ul><li>Next</li></ul>'},
        {'title': 'Raw', 'body': '<p>ignored</p>', 'plain': 'KEEP <Future pending>\n  exact spacing'},
    ]
    (email_mod._send_sync if synchronous else email_mod._send)('Subject', sections)
    assert 'First & second' in captured['plain']
    assert 'SYN | 8.5' in captured['plain']
    assert '<td>' not in captured['plain']
    assert 'hide me' not in captured['plain']
    assert 'KEEP <Future pending>\n  exact spacing' in captured['plain']
    assert 'At a glance' not in captured['rendered']


def test_transition_keeps_all_provided_leaders(captured_email):
    leaders = [{'feature': f'Feature {i}', 'lead_weeks': i, 'direction': '<up>', 't_stat': 2.1} for i in range(7)]
    email_mod.alert_on_transition_leaders(leaders, {'best_k': 3})
    rendered = email_mod._render_html('Transition', captured_email['sections'])
    assert 'Feature 6' in rendered
    assert '&lt;up&gt;' in rendered
    assert '7 supplied transition leaders' in captured_email['sections'][0]['plain']


def test_failure_without_fix_does_not_promise_commands(captured_email):
    email_mod.alert_on_failure_with_fix('SYN', 'Error')
    assert 'fix commands included' not in captured_email['subject']
    assert 'No fix commands supplied' in captured_email['sections'][0]['plain']


def test_disabled_generic_alert_and_none_insight_keep_no_send_gate(captured_email, monkeypatch):
    monkeypatch.setattr(email_mod, '_get_settings', lambda: SimpleNamespace(ALERT_EMAIL_ENABLED=False))
    assert email_mod.send_alert('Health', 'failure') is False
    email_mod.send_insight('test', 'No content', None)
    assert not captured_email


@pytest.mark.parametrize("body,headline,label", [
    ("5 sources are past their own freshness window (alert threshold 3).\n2 more are switched off and not counted.", "5", "Sources past freshness window"),
    ("201 pulls failed in the last hour (threshold 50); 24h cumulative is 400.", "201", "Failed pulls (last hour)"),
    ("Fleet heartbeat - state changes:\n\n[RECOVERED] a\n\nCurrently not-up (3 of 20), worst first:\n  - b\nSwept 20 entities", "3/20", "Fleet entities currently not up"),
])
def test_health_and_fleet_use_scoped_source_numbers(captured_email, body, headline, label):
    email_mod.send_alert("Health", body, "critical")
    lead = captured_email["sections"][0]
    assert lead["plain"].startswith(headline)
    assert label in lead["plain"]
    assert "CRITICAL" in lead["plain"]


def test_generated_weekly_review_extracts_actual_activity_and_findings(captured_email):
    source = """# GRID Insight Review - 2026-11-07

**Period:** Last 7 days (since 2026-10-31)
**Total insights:** 23
**Generated:** 2026-11-07T01:02:03

## Summary by Category
| Category | Count |
|----------|-------|
| hypothesis | 23 |

## Dominant Themes
- **credit** - mentioned in 8 insight(s)

## Regime Transitions
No regime transitions detected in this period.

## Agent Decision Distribution
- **HOLD**: 4 run(s)

## Longer-Term Patterns
Sufficient data for pattern analysis:
- **Strong consensus**: 80% of agent runs recommend HOLD
"""
    email_mod.send_weekly_review(source)
    lead = captured_email["sections"][0]["plain"]
    assert lead.startswith("23") and "Insights in review period" in lead
    assert "Last 7 days" in lead
    assert "80% of agent runs recommend HOLD" in lead
    assert "No regime transitions" in lead
    assert html.escape(source) in captured_email["sections"][1]["body"]


@pytest.mark.parametrize("score", [None, float("nan"), float("inf"), -1, 11, True])
def test_options_missing_invalid_score_is_unavailable_not_a_number(captured_email, score):
    email_mod.alert_on_100x_opportunity("SYN", score, "CALL", "Thesis")
    lead = captured_email["sections"][0]["plain"]
    assert lead.startswith("unavailable")
    assert "0.0/10" not in lead


def test_valid_zero_score_is_retained(captured_email):
    email_mod.alert_on_100x_opportunity("SYN", 0, "CALL", "Thesis")
    assert captured_email["sections"][0]["plain"].startswith("0.0/10")


@pytest.mark.parametrize("confidence", [None, float("nan"), float("inf"), -1, 2, True])
def test_invalid_regime_confidence_does_not_make_a_probability(captured_email, confidence):
    email_mod.alert_on_regime_change("RISK_OFF", "MIXED", confidence)
    assert "confidence: unavailable" in captured_email["sections"][0]["plain"]


def test_code_plain_preserves_indentation_and_all_error_text(captured_email):
    error = "Failure. " + "detail " * 100 + "LAST LINE"
    retry = "if failed:\n    retry()"
    email_mod.alert_on_failure_with_fix("SYN", error, {"retry": retry})
    code = next(s for s in captured_email["sections"] if s["title"] == "Paste into Claude Code")
    assert retry in code["plain"] and error in code["plain"]
    assert code["plain"] in email_mod._render_plain([code])


def test_missing_transition_stat_is_unknown(captured_email):
    email_mod.alert_on_transition_leaders([{"feature": "SYN"}], {})
    table = captured_email["sections"][-1]["body"]
    assert "unavailable" in table and "0.00" not in table


@pytest.mark.parametrize("wrapper", ["<pre>{}</pre>", '<div style="white-space:pre-wrap;">{}</div>', '<div style="white-space: pre;"><span>{}</span></div>'])
def test_html_plain_fallback_preserves_preformatted_commands(wrapper):
    source = "python - <<'PY'\nif True:\n    print('a  b')\n\n    print('done')\nPY"
    markup = "<html><body><h2>Repair</h2>" + wrapper.format(html.escape(source)) + "<p>End</p></body></html>"
    converted = email_mod._html_to_plain(markup)
    assert source in converted
    assert converted.startswith("Repair\n") and converted.endswith("\nEnd")


@pytest.mark.parametrize("sender", ["alert_on_failure", "alert_on_failure_with_fix"])
def test_failure_timestamp_labels_report_generation_not_event_time(captured_email, monkeypatch, sender):
    from datetime import datetime

    monkeypatch.setattr(email_mod, "datetime", SimpleNamespace(
        now=lambda tz: datetime(2026, 11, 7, 12, 34, tzinfo=tz)))
    error = "Source recorded failure at 2026-11-06 01:02 UTC."
    getattr(email_mod, sender)("SYN", error)
    details = next(s for s in captured_email["sections"] if s["title"] == "Ingestion Failure")
    plain = email_mod._html_to_plain(details["body"])
    assert "Failure reported at 2026-11-07 12:34 UTC" in plain
    assert "SYN failed at 2026-11-07" not in plain
    assert error in plain
