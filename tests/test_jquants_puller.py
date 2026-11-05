"""Regression tests for the J-Quants puller credential wiring.

Production audit 2026-11-05: JQuants failed every run since 2026-09-30 with
``RetryError[... raised ValueError]``. ``ingestion/scheduler.py`` builds
``JQuantsPuller(db_engine)`` with no credentials, the puller defaulted
email/password to ``""`` without reading ``settings``, and the resulting
deterministic ``ValueError`` was retried three times by tenacity and logged
as a FAILED pull.

These tests pin: credentials default from settings, missing credentials
return the scheduler-contract SKIPPED result with no network call (and the
shared classifier reads it as SKIPPED, not FAILED), and a config error is
never retried. All HTTP is mocked.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ingestion.international import jquants
from ingestion.international.jquants import JQuantsNotConfigured, JQuantsPuller
from ingestion.smart_scheduler import OUTCOME_SKIPPED, _classify_outcome
from ingestion.source_freshness import error_class


def _engine() -> MagicMock:
    """Mock engine whose source_catalog lookup returns id 213."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = (213,)
    return engine


def _settings(email: str = "", password: str = "") -> SimpleNamespace:
    return SimpleNamespace(JQUANTS_EMAIL=email, JQUANTS_PASSWORD=password)


def _resp(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def test_credentials_default_from_settings() -> None:
    with patch("config.settings", _settings("ops@example.test", "pw-from-settings")):
        puller = JQuantsPuller(_engine())
    assert puller.email == "ops@example.test"
    assert puller.password == "pw-from-settings"
    assert puller.is_configured


def test_explicit_credentials_override_settings() -> None:
    with patch("config.settings", _settings("ops@example.test", "pw-from-settings")):
        puller = JQuantsPuller(_engine(), email="arg@example.test", password="pw-arg")
    assert (puller.email, puller.password) == ("arg@example.test", "pw-arg")


def test_settings_credentials_reach_auth_endpoint() -> None:
    """The scheduler's no-arg construction authenticates with settings values."""
    with patch("config.settings", _settings("ops@example.test", "pw-from-settings")):
        puller = JQuantsPuller(_engine())
    with patch.object(jquants.requests, "post") as post, \
            patch.object(jquants.requests, "get") as get:
        post.side_effect = [_resp({"refreshToken": "rt"}), _resp({"idToken": "it"})]
        get.return_value = _resp({"topix": []})
        result = puller.pull_all()

    auth_user = post.call_args_list[0]
    assert auth_user.kwargs["json"] == {
        "mailaddress": "ops@example.test", "password": "pw-from-settings",
    }
    assert get.call_args.kwargs["headers"] == {"Authorization": "Bearer it"}
    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 0


@pytest.mark.parametrize("email,password", [("", ""), ("ops@example.test", ""), ("", "pw")])
def test_missing_credentials_skip_without_network(email: str, password: str) -> None:
    with patch("config.settings", _settings(email, password)):
        puller = JQuantsPuller(_engine())
    with patch.object(jquants.requests, "post") as post, \
            patch.object(jquants.requests, "get") as get:
        result = puller.pull_all()

    post.assert_not_called()
    get.assert_not_called()
    assert result["status"] == "SKIPPED"
    assert result["rows_inserted"] == 0
    assert "not configured" in result["skipped_reason"]


def test_missing_credentials_classify_as_skipped_config() -> None:
    with patch("config.settings", _settings()):
        result = JQuantsPuller(_engine()).pull_all()
    outcome, rows, note = _classify_outcome(result)
    assert outcome == OUTCOME_SKIPPED
    assert rows == 0
    assert note is not None and "not configured" in note
    assert error_class(note) == "CONFIG"


def test_config_error_is_not_retried() -> None:
    """A deterministic config error raises once, unwrapped (no RetryError)."""
    with patch("config.settings", _settings()):
        puller = JQuantsPuller(_engine())
    with patch.object(puller, "_authenticate", wraps=puller._authenticate) as auth, \
            patch.object(jquants.requests, "get") as get, \
            pytest.raises(JQuantsNotConfigured):
        puller._fetch_data("indices/topix")
    assert auth.call_count == 1
    get.assert_not_called()
