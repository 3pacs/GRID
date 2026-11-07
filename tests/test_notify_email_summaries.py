"""Offline contracts for the legacy notification transport and its templates."""
from datetime import datetime, timezone
from email import message_from_string
from unittest.mock import Mock

import pytest

from scripts import notify


def backtest(**updates):
    result = {"passed": True, "iterations_run": 3, "best_result": {
        "statement": "A < B. " + "Full hypothesis detail. " * 100,
        "verdict": "PASS", "sharpe": 1.5, "baseline_sharpe": .8,
        "return": .12, "max_drawdown": -.07, "feature_ids": [1, 2],
        "era_results": [{"era": 1, "status": "OK", "sharpe": 1.4, "return": .1,
                         "start": "2020-01-01", "end": "2021-01-01"},
                        {"era": 2, "status": "INSUFFICIENT_DATA"}],
    }}
    result.update(updates)
    return result


def test_backtest_leads_with_simulated_metric_scope_and_keeps_hypothesis():
    body = notify.format_backtest_summary(backtest())
    assert body.index("At a glance") < body.index("Hypothesis</div>")
    assert "1.50" in body and "Simulated Sharpe" in body
    assert "1 of 2 supplied eras" in body
    assert "+12.0%" in body and "-7.0%" in body
    assert "not live investment returns" in body
    assert "A &lt; B." in body and "Full hypothesis detail. " * 99 in body
    assert "2020-01-01" in body and "INSUFFICIENT_DATA" in body


@pytest.mark.parametrize("value", [None, float('nan'), float('inf'), 'bad'])
def test_invalid_backtest_metrics_stay_unknown(value):
    data = backtest()
    data['best_result']['sharpe'] = value
    body = notify.format_backtest_summary(data)
    assert "Simulated Sharpe: n/a" in body
    assert "nan" not in body.lower() and "+inf" not in body.lower()


def test_raw_attempt_formatting_does_not_enable_the_dormant_notification_gate(monkeypatch):
    data = backtest()['best_result']
    data['iteration'] = 4
    assert "Simulated Sharpe: 1.50" in notify.format_backtest_summary(data)
    sender = Mock()
    monkeypatch.setattr(notify, 'send_insight_email', sender)
    assert notify.notify_on_pass(data) is False
    sender.assert_not_called()


def test_generic_notification_adds_short_summary_and_full_readable_detail(monkeypatch):
    smtp = Mock()
    monkeypatch.setattr(notify, '_get_smtp_config', lambda: {
        'host':'localhost','port':25,'user':'','password':'','to_email':'synthetic@example.invalid'})
    monkeypatch.setattr(notify.smtplib, 'SMTP', lambda *a, **k: smtp)
    source = 'Build request #42 needs a new export.\n\n' + 'Detail <source> & evidence. ' * 150
    assert notify.send_insight_email('Build request #42', source)
    msg = message_from_string(smtp.sendmail.call_args.args[2])
    parts = {p.get_content_type():p.get_payload(decode=True).decode(p.get_content_charset()) for p in msg.get_payload()}
    plain = parts['text/plain']
    assert plain.index('At a glance') < plain.index('Full detail')
    assert source in plain
    assert 'Build request #42 needs a new export.' in plain
    assert '&lt;source&gt;' in parts['text/html']
    assert '<source>' not in parts['text/html']


def test_preformatted_summary_not_duplicated(monkeypatch):
    smtp = Mock()
    monkeypatch.setattr(notify, '_get_smtp_config', lambda: {
        'host':'localhost','port':25,'user':'','password':'','to_email':'synthetic@example.invalid'})
    monkeypatch.setattr(notify.smtplib, 'SMTP', lambda *a, **k: smtp)
    notify.send_insight_email('Autoresearch', notify.format_backtest_summary(backtest()), html=True, summary_ready=True)
    msg = message_from_string(smtp.sendmail.call_args.args[2])
    rendered = msg.get_payload()[1].get_payload(decode=True).decode('utf-8')
    assert rendered.count('At a glance') == 1


def test_legacy_digest_uses_bounded_window_and_counts_before_detail():
    from sqlalchemy import create_engine, text
    engine = create_engine('sqlite:///:memory:')
    now = datetime(2026,11,7,8,tzinfo=timezone.utc)
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE decision_journal(id INTEGER, inferred_state TEXT, state_confidence REAL, grid_recommendation TEXT, verdict TEXT, decision_timestamp TIMESTAMP)'))
        for i, at in enumerate(['2026-11-07 07:00:00+00:00', '2026-11-08 07:00:00+00:00']):
            conn.execute(text("INSERT INTO decision_journal VALUES(:i,'MIXED',NULL,'HOLD',NULL,:at)"), {'i':i,'at':at})
        conn.execute(text('CREATE TABLE hypothesis_registry(id INTEGER, statement TEXT, updated_at TIMESTAMP, state TEXT)'))
        conn.execute(text("INSERT INTO hypothesis_registry VALUES(1,'Full hypothesis','2026-11-07 07:00:00+00:00','PASSED')"))
        conn.execute(text('CREATE TABLE feature_registry(id INTEGER, name TEXT)'))
        conn.execute(text('CREATE TABLE feature_drift_log(feature_id INTEGER, drift_score REAL, checked_at TIMESTAMP)'))
    body = notify.format_daily_digest(engine, now=now)
    assert body.index('[At a glance]') < body.index('Decision Journal  (')
    assert '1 journal entries' in body and '1 recorded PASSED hypotheses' in body
    assert '0 drift records' in body
    assert 'n/a' in body and '2026-11-07 07:00:00 UTC' in body
    assert '2026-11-08' not in body
    assert 'not a verified investment track record' in body


def test_unavailable_legacy_digest_sources_never_become_zero():
    engine = Mock()
    engine.connect.side_effect = RuntimeError('synthetic unavailable')
    body = notify.format_daily_digest(engine)
    first = body.split('Decision Journal:')[0]
    assert 'journal entries: unknown' in first
    assert 'PASSED hypotheses: unknown' in first
    assert 'drift records: unknown' in first
    assert '0 journal' not in body


def test_html_notification_preserves_preformatted_code_in_plain_part(monkeypatch):
    smtp = Mock()
    monkeypatch.setattr(notify, '_get_smtp_config', lambda: {
        'host':'localhost','port':25,'user':'','password':'','to_email':'synthetic@example.invalid'})
    monkeypatch.setattr(notify.smtplib, 'SMTP', lambda *a, **k: smtp)
    code = "if True:\n    print('a  b')"
    source = f'<html><body><pre>{code}</pre></body></html>'
    assert notify.send_insight_email('Repair instructions', source, html=True)
    msg = message_from_string(smtp.sendmail.call_args.args[2])
    plain = msg.get_payload()[0].get_payload(decode=True).decode('utf-8')
    assert code in plain
    assert plain.index('At a glance') < plain.index('Full detail')
