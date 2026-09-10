"""GRID's outbound mail identity is the Hermes mailbox (2026-09-10).

Digests used to be sent as ``grid-alerts@grid-svr`` through local Postfix, a
bare sender Gmail drops silently. The default From is now
``hermes@stepdad.finance``; the credentials stay in the server ``.env`` and
``alerts/email.py`` forces STARTTLS for any non-localhost host.
"""
from __future__ import annotations

from pathlib import Path

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
    line = [l for l in env.splitlines() if l.startswith("ALERT_SMTP_PASSWORD=")][0]
    assert line.strip() == "ALERT_SMTP_PASSWORD=", "the template must not carry a password"


def test_external_smtp_forces_tls() -> None:
    src = (ROOT / "alerts/email.py").read_text(encoding="utf-8")
    assert 'is_external = host not in ("localhost", "127.0.0.1", "::1")' in src
    assert "if is_external and not use_tls:\n            use_tls = True" in src
