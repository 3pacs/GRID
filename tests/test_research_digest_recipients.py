"""Research-only routing with synthetic mailboxes and inert SMTP/state fakes."""
from __future__ import annotations

import json
import smtplib
from datetime import datetime, timedelta, timezone
from email import message_from_string
from types import SimpleNamespace

import pytest

import alerts.email as email_mod
from alerts import scheduler

PAIR = ("research.one@example.com", "research.two@example.com")
GLOBAL = "legacy-alert@example.com"


def _settings(**overrides):
    values = dict(ALERT_EMAIL_ENABLED=True, ALERT_EMAIL_TO=GLOBAL,
                  RESEARCH_DIGEST_EMAIL_TO=PAIR, ALERT_EMAIL_FROM="sender@example.com",
                  ALERT_SMTP_HOST="localhost", ALERT_SMTP_PORT=25,
                  ALERT_SMTP_USER="", ALERT_SMTP_PASSWORD="", ALERT_SMTP_USE_TLS=False)
    values.update(overrides)
    return SimpleNamespace(**values)


def _smtp(monkeypatch, *, refused=None, error=None, quit_error=None):
    instances = []
    class SMTP:
        def __init__(self, *a, **kw):
            self.envelopes, self.messages, self.quit_calls = [], [], 0
            instances.append(self)
        def ehlo(self): pass
        def starttls(self): pass
        def login(self, *a): pass
        def sendmail(self, sender, recipients, message):
            self.envelopes.append((sender, recipients))
            self.messages.append(message_from_string(message))
            if error: raise error
            return {} if refused is None else refused
        def quit(self):
            self.quit_calls += 1
            if quit_error: raise quit_error
    monkeypatch.setattr(email_mod, "_get_settings", _settings)
    monkeypatch.setattr(email_mod.smtplib, "SMTP", SMTP)
    monkeypatch.setattr(email_mod, "_render_html", lambda *a, **kw: "<p>synthetic</p>")
    return instances


def test_research_exact_two_envelopes_match_header_and_ignore_global(monkeypatch):
    instances = _smtp(monkeypatch)
    delivery = {}
    assert email_mod._send_sync("synthetic", [{"title": "T", "body": "B"}],
                               research_recipients=PAIR, delivery=delivery) == "sent"
    assert instances[0].envelopes == [("sender@example.com", list(PAIR))]
    assert instances[0].messages[0]["To"] == ", ".join(PAIR)
    assert GLOBAL not in instances[0].messages[0]["To"]
    assert delivery == {"outcome": "accepted", "audience": list(PAIR),
                        "relay_accepted": list(PAIR), "refused": []}


def test_legacy_transport_and_sync_callers_keep_global_route(monkeypatch):
    instances = _smtp(monkeypatch)
    assert email_mod._do_send("synthetic", "html", "plain") == "sent"
    assert instances[0].envelopes[0][1] == [GLOBAL]
    assert instances[0].messages[0]["To"] == GLOBAL
    calls = []
    monkeypatch.setattr(email_mod, "_do_send", lambda *args: calls.append(args) or "sent")
    assert email_mod._send_sync("synthetic", [{"title": "T", "body": "B"}]) == "sent"
    assert len(calls[0]) == 3


def test_normalization_deduplicates_then_requires_two_unique_addresses():
    assert email_mod._validated_research_recipients(
        (" RESEARCH.ONE@EXAMPLE.COM ", PAIR[0], PAIR[1])) == PAIR


@pytest.mark.parametrize("bad", [(), (PAIR[0],), (PAIR[0], PAIR[0]),
    PAIR[0] + "," + PAIR[1], ("", PAIR[1]), ("broken", PAIR[1]),
    ("Name <research.one@example.com>", PAIR[1]),
    ("research.one@example.com\r\nBcc: extra@example.com", PAIR[1]),
    ("research.one@example.com\t", PAIR[1]), ("a..b@example.com", PAIR[1]),
    ("a@-example.com", PAIR[1]), ("é@example.com", PAIR[1]),
    (PAIR[0], PAIR[1], "third@example.com"), (None, PAIR[1])])
def test_invalid_research_audience_is_rejected(bad):
    with pytest.raises(ValueError): email_mod._validated_research_recipients(bad)


def test_invalid_transport_audience_never_constructs_smtp_or_falls_back(monkeypatch):
    instances = _smtp(monkeypatch)
    delivery = {}
    assert email_mod._do_send("synthetic", "html", "plain",
                             research_recipients=(PAIR[0],), delivery=delivery) == "failed"
    assert instances == [] and delivery["audience"] == []


class _Result:
    def __init__(self, one=None, many=()): self.one, self.many = one, many
    def fetchone(self): return self.one
    def fetchall(self): return self.many


def _quiet_engine(monkeypatch):
    class Conn:
        def execute(self, clause, *a, **kw):
            sql = str(clause)
            return _Result(one=(0, None) if "raw_series" in sql else (0,) if "COUNT(*)" in sql else None)
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr("db.get_engine", lambda: SimpleNamespace(connect=Conn))
    monkeypatch.setattr("intelligence.long_plays.load_latest_board", lambda engine: None)


def test_empty_research_setting_fails_before_db_and_no_global_fallback(monkeypatch):
    calls = []
    monkeypatch.setattr(email_mod, "_get_settings", lambda: _settings(RESEARCH_DIGEST_EMAIL_TO=()))
    monkeypatch.setattr("db.get_engine", lambda: calls.append("DB"))
    monkeypatch.setattr(email_mod, "_send_sync", lambda *a, **kw: calls.append("send"))
    result = email_mod.daily_digest(dry_run=True)
    assert result["send_status"] == "failed" and result["sent"] is False
    assert "exactly two" in result["error"] and calls == []


def test_dry_run_validates_audience_without_smtp(monkeypatch):
    instances = _smtp(monkeypatch)
    _quiet_engine(monkeypatch)
    result = email_mod.daily_digest(dry_run=True)
    assert result["dry_run"] is True and result["research_recipients"] == list(PAIR)
    assert instances == []


def test_typed_input_uses_research_path_without_global_reroute(monkeypatch):
    instances = _smtp(monkeypatch)
    _quiet_engine(monkeypatch)
    monkeypatch.setattr(email_mod, "_get_settings", lambda: _settings(RESEARCH_DIGEST_EMAIL_TO=()))
    result = email_mod.daily_digest(research_recipients=PAIR)
    assert result["send_status"] == "sent" and result["sent"] is True
    assert instances[0].envelopes[0][1] == list(PAIR)
    assert email_mod._get_settings().ALERT_EMAIL_TO == GLOBAL


@pytest.mark.parametrize("refused_index", [0, 1])
def test_partial_refusal_is_uncertain_not_all_sent_or_retryable(monkeypatch, refused_index):
    refused = PAIR[refused_index]
    instances = _smtp(monkeypatch, refused={refused: (550, b"synthetic refusal")})
    _quiet_engine(monkeypatch)
    result = email_mod.daily_digest()
    assert result["send_status"] == "uncertain" and result["sent"] is False
    assert result["delivery"] == {"outcome": "partial", "audience": list(PAIR),
        "relay_accepted": [PAIR[1-refused_index]], "refused": [refused]}
    assert instances[0].quit_calls == 1


def test_all_recipient_refusal_is_clean_failure(monkeypatch):
    error = smtplib.SMTPRecipientsRefused({p: (550, b"synthetic") for p in PAIR})
    _smtp(monkeypatch, error=error)
    delivery = {}
    assert email_mod._do_send("synthetic", "h", "p", research_recipients=PAIR, delivery=delivery) == "failed"
    assert delivery["relay_accepted"] == [] and delivery["refused"] == list(PAIR)


def test_sender_rejection_does_not_fabricate_recipient_refusals(monkeypatch):
    _smtp(monkeypatch, error=smtplib.SMTPSenderRefused(550, b"synthetic", "sender@example.com"))
    delivery = {}
    assert email_mod._do_send("synthetic", "h", "p", research_recipients=PAIR, delivery=delivery) == "failed"
    assert delivery["outcome"] == "failed" and delivery["relay_accepted"] == [] and delivery["refused"] == []


def test_full_refusal_map_is_failed_not_partial(monkeypatch):
    _smtp(monkeypatch, refused={p: (550, b"synthetic") for p in PAIR})
    delivery = {}
    assert email_mod._do_send("synthetic", "h", "p", research_recipients=PAIR, delivery=delivery) == "failed"
    assert delivery["outcome"] == "failed" and delivery["relay_accepted"] == []


def test_data_disconnect_remains_uncertain(monkeypatch):
    _smtp(monkeypatch, error=smtplib.SMTPServerDisconnected("synthetic disconnect"))
    delivery = {}
    assert email_mod._do_send("synthetic", "h", "p", research_recipients=PAIR, delivery=delivery) == "uncertain"
    assert delivery["outcome"] == "uncertain" and delivery["relay_accepted"] == []


@pytest.mark.parametrize("partial", [False, True])
def test_quit_failure_never_reclassifies_confirmed_acceptance(monkeypatch, partial):
    _smtp(monkeypatch, refused={PAIR[0]: (550, b"synthetic")} if partial else {},
          quit_error=ConnectionResetError("synthetic quit"))
    delivery = {}
    status = email_mod._do_send("synthetic", "h", "p", research_recipients=PAIR, delivery=delivery)
    assert status == ("uncertain" if partial else "sent")
    assert delivery["outcome"] == ("partial" if partial else "accepted")


def test_unknown_refusal_map_is_conservatively_uncertain(monkeypatch):
    _smtp(monkeypatch, refused={"unrequested@example.com": (550, b"synthetic")})
    delivery = {}
    assert email_mod._do_send("synthetic", "h", "p", research_recipients=PAIR, delivery=delivery) == "uncertain"
    assert delivery["outcome"] == "uncertain" and delivery["relay_accepted"] == []


def test_malformed_full_refusal_map_cannot_become_safely_retryable(monkeypatch):
    _smtp(monkeypatch, refused={p: None for p in PAIR})
    delivery = {}
    assert email_mod._do_send("synthetic", "h", "p", research_recipients=PAIR, delivery=delivery) == "uncertain"
    assert delivery["outcome"] == "uncertain" and delivery["relay_accepted"] == []


class _StateEngine:
    """Execute only the actual lock/read/write SQL against an in-memory row."""
    def __init__(self, state=None, *, fail_read=False, fail_write=None, locked=True):
        self.state, self.fail_read, self.fail_write, self.locked = state, fail_read, fail_write, locked
        self.writes, self.unlocks = [], 0
    def connect(self): return _StateConn(self)
    def begin(self): return _StateConn(self)


class _StateConn:
    def __init__(self, engine): self.engine = engine
    def execute(self, clause, params=None):
        sql, engine = str(clause), self.engine
        if "pg_try_advisory_lock" in sql: return SimpleNamespace(scalar=lambda: engine.locked)
        if "pg_advisory_unlock" in sql:
            engine.unlocks += 1
            return SimpleNamespace(scalar=lambda: True)
        assert params["t"] == "research_daily_digest" and params["e"] == "daily"
        if "SELECT seen_at, payload" in sql:
            if engine.fail_read: raise RuntimeError("synthetic state read failure")
            return _Result(one=engine.state)
        assert "INSERT INTO alert_state" in sql
        payload = json.loads(params["payload"])
        engine.writes.append(payload)
        if engine.fail_write == len(engine.writes): raise RuntimeError("synthetic persistence failure")
        engine.state = (params["seen_at"], payload)
        return _Result()
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _partial():
    return {"send_status": "uncertain", "sent": False, "delivery": {
        "outcome": "partial", "audience": list(PAIR), "relay_accepted": [PAIR[0]], "refused": [PAIR[1]]}}


def _window(): return datetime(2026, 10, 27, 7, 20, tzinfo=timezone.utc)


def test_partial_outcome_is_durable_and_blocks_pair_retry_after_restart(monkeypatch):
    calls = []
    monkeypatch.setattr(email_mod, "daily_digest", lambda: calls.append(1) or _partial())
    engine = _StateEngine()
    scheduler._send_digest_once_per_window(engine, _window())
    assert [p["status"] for p in engine.writes] == ["uncertain", "uncertain"]
    assert "delivery" in engine.state[1]
    assert engine.state[1]["delivery"] == _partial()["delivery"]
    # A new engine/connection reads the persisted row rather than a process flag.
    restarted = _StateEngine(state=engine.state)
    scheduler._send_digest_once_per_window(restarted, _window()+timedelta(minutes=10))
    assert calls == [1] and restarted.writes == [] and restarted.unlocks == 1


@pytest.mark.parametrize("status", ["sent", "uncertain", "legacy-unknown"])
def test_old_state_is_not_rekeyed_or_cleared_for_two_address_expansion(monkeypatch, status):
    calls = []
    monkeypatch.setattr(email_mod, "daily_digest", lambda: calls.append(1))
    engine = _StateEngine(state=(_window()-timedelta(hours=1), {"status": status}))
    scheduler._send_digest_once_per_window(engine, _window())
    assert calls == [] and engine.writes == []


def test_partial_terminal_write_failure_leaves_pre_send_uncertainty(monkeypatch):
    calls = []
    monkeypatch.setattr(email_mod, "daily_digest", lambda: calls.append(1) or _partial())
    engine = _StateEngine(fail_write=2)
    scheduler._send_digest_once_per_window(engine, _window())
    assert engine.state[1] == {"status": "uncertain"}
    scheduler._send_digest_once_per_window(engine, _window()+timedelta(minutes=1))
    assert calls == [1]


@pytest.mark.parametrize("kwargs", [{"fail_read": True}, {"fail_write": 1}, {"locked": False}])
def test_state_or_lock_failure_never_attempts_delivery(monkeypatch, kwargs):
    calls = []
    monkeypatch.setattr(email_mod, "daily_digest", lambda: calls.append(1) or _partial())
    scheduler._send_digest_once_per_window(_StateEngine(**kwargs), _window())
    assert calls == []


def test_partial_metadata_cannot_be_made_retryable_by_wrong_status(monkeypatch):
    result = _partial(); result["send_status"] = "failed"
    monkeypatch.setattr(email_mod, "daily_digest", lambda: result)
    engine = _StateEngine()
    scheduler._send_digest_once_per_window(engine, _window())
    assert engine.state[1]["status"] == "uncertain"
    assert engine.state[1]["delivery"]["outcome"] == "partial"


def test_public_research_setting_defaults_empty_without_importing_config():
    import ast
    from pathlib import Path

    tree = ast.parse((Path(__file__).resolve().parents[1]/"config.py").read_text(encoding="utf-8"))
    fields = [n for n in ast.walk(tree) if isinstance(n, ast.AnnAssign)
              and isinstance(n.target, ast.Name) and n.target.id == "RESEARCH_DIGEST_EMAIL_TO"]
    assert len(fields) == 1 and ast.literal_eval(fields[0].value) == ()
