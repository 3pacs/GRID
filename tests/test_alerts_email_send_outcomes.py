"""Coverage for alerts/email.py::_do_send()'s three-way outcome.

Before this fix, _do_send wrapped sendmail() and quit() in one try
block and returned a plain bool: an accepted message whose quit() call
then failed was reported as a send failure, and the digest scheduler
retried it — risking a duplicate of a message that had already gone
out. Connection loss during transmission was collapsed into the same
"failed" bucket as a send that never started at all, even though the
two need different handling (one is safe to retry, the other isn't).
"""
from __future__ import annotations

import smtplib
from types import SimpleNamespace
from typing import ClassVar

import alerts.email as email_mod


def _fake_settings(**overrides):
    defaults = {
        "ALERT_EMAIL_ENABLED": True,
        "ALERT_EMAIL_TO": "ops@example.com",
        "ALERT_EMAIL_FROM": "grid@example.com",
        "ALERT_SMTP_HOST": "localhost",
        "ALERT_SMTP_PORT": 25,
        "ALERT_SMTP_USER": "",
        "ALERT_SMTP_PASSWORD": "",
        "ALERT_SMTP_USE_TLS": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class _FakeSMTP:
    """Stands in for smtplib.SMTP. Class-level hooks let each test choose
    what sendmail()/quit() do without touching the real network."""

    sendmail_raises: Exception | None = None
    quit_raises: Exception | None = None
    instances: ClassVar[list] = []

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.sendmail_called = False
        self.quit_called = False
        type(self).instances.append(self)

    def ehlo(self):
        pass

    def starttls(self):
        pass

    def login(self, user, password):
        pass

    def sendmail(self, from_addr, to_addrs, msg):
        self.sendmail_called = True
        if self.sendmail_raises:
            raise self.sendmail_raises

    def quit(self):
        self.quit_called = True
        if self.quit_raises:
            raise self.quit_raises


def _install_fake_smtp(monkeypatch, sendmail_raises=None, quit_raises=None):
    _FakeSMTP.sendmail_raises = sendmail_raises
    _FakeSMTP.quit_raises = quit_raises
    _FakeSMTP.instances = []
    monkeypatch.setattr(email_mod.smtplib, "SMTP", _FakeSMTP)
    return _FakeSMTP.instances


def test_successful_sendmail_is_sent_even_if_quit_fails(monkeypatch) -> None:
    """The exact bug this fixes: sendmail() succeeding means the relay
    already accepted the message. A subsequent quit() failure is cleanup
    noise, not a reason to report the send as failed.
    """
    monkeypatch.setattr(email_mod, "_get_settings", lambda: _fake_settings())
    instances = _install_fake_smtp(monkeypatch, quit_raises=ConnectionResetError("bye already"))

    status = email_mod._do_send("subject", "<p>html</p>", "plain")

    assert status == "sent"
    assert instances[0].sendmail_called is True
    assert instances[0].quit_called is True


def test_disabled_mail_is_a_clean_failure(monkeypatch) -> None:
    monkeypatch.setattr(email_mod, "_get_settings", lambda: _fake_settings(ALERT_EMAIL_ENABLED=False))

    status = email_mod._do_send("subject", "<p>html</p>", "plain")

    assert status == "failed"


def test_connect_failure_before_any_data_is_a_clean_failure(monkeypatch) -> None:
    monkeypatch.setattr(email_mod, "_get_settings", lambda: _fake_settings())

    def _raise(*_a, **_k):
        raise ConnectionRefusedError("nobody home")

    monkeypatch.setattr(email_mod.smtplib, "SMTP", _raise)

    status = email_mod._do_send("subject", "<p>html</p>", "plain")

    assert status == "failed"


def test_explicit_relay_rejection_is_a_clean_failure(monkeypatch) -> None:
    monkeypatch.setattr(email_mod, "_get_settings", lambda: _fake_settings())
    _install_fake_smtp(
        monkeypatch,
        sendmail_raises=smtplib.SMTPRecipientsRefused({"ops@example.com": (550, b"no such user")}),
    )

    status = email_mod._do_send("subject", "<p>html</p>", "plain")

    assert status == "failed"


def test_connection_lost_during_sendmail_is_uncertain_not_failed(monkeypatch) -> None:
    """This is the case the previous bool return couldn't represent at
    all: we don't know if the relay received the message before the
    connection dropped. Reporting this the same as a clean failure would
    let the scheduler retry it as if nothing had been sent.
    """
    monkeypatch.setattr(email_mod, "_get_settings", lambda: _fake_settings())
    _install_fake_smtp(monkeypatch, sendmail_raises=smtplib.SMTPServerDisconnected("connection lost"))

    status = email_mod._do_send("subject", "<p>html</p>", "plain")

    assert status == "uncertain"
