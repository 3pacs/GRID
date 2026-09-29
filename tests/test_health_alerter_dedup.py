"""Tests for alerts/health_alerter.py's change-detection + daily-reminder
cap (GRID-STALE-SOURCES-AUDIT-20260929.md, item 2 of the fix).

Before this change, db.stale_sources re-sent every DEFAULT_COOLDOWN_HOURS
(6h) with no check that the stale-source list had actually changed — the
audit found emails firing on record at 03:10, 06:11, 12:58 and 19:07Z on
back-to-back days while the underlying set barely moved. Now it should
fire on a genuine change to the stale set, and otherwise at most once
every STALE_SOURCES_MAX_REMINDER_HOURS.

All tests stub ``_send`` (never touches real SMTP) and point
``_STATE_PATH`` at a tmp_path file so runs don't share state.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from alerts import health_alerter as ha


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(ha, "_STATE_PATH", tmp_path / "alert_state.json")
    sent: list[tuple[str, str, str]] = []

    def _fake_send(subject: str, body: str, severity: str) -> bool:
        sent.append((subject, body, severity))
        return True

    monkeypatch.setattr(ha, "_send", _fake_send)
    yield sent


def _health(stale_names: list[str]) -> dict:
    return {
        "timestamp": "2026-09-28T00:00:00+00:00",
        "db": {
            "healthy": True,
            "stale_sources": [
                {"source": n, "last_pull": "never", "cadence": "DAILY", "age_hours": 100.0}
                for n in stale_names
            ],
        },
    }


# 21 distinct names so len(stale_sources) > STALE_SOURCES_THRESHOLD (20)
_STALE_21 = [f"source_{i}" for i in range(21)]
_STALE_21_REORDERED = list(reversed(_STALE_21))
_STALE_21_CHANGED = _STALE_21[:-1] + ["source_new"]


class TestChangeDetection:
    def test_first_fire_always_sends(self, _isolated_state):
        now = datetime(2026, 9, 28, 0, 0, tzinfo=timezone.utc)
        fired = ha.check_and_alert(_health(_STALE_21), now=now)

        assert "db.stale_sources" in fired
        assert len(_isolated_state) == 1

    def test_unchanged_set_does_not_refire_within_the_reminder_window(self, _isolated_state):
        t0 = datetime(2026, 9, 28, 0, 0, tzinfo=timezone.utc)
        ha.check_and_alert(_health(_STALE_21), now=t0)
        assert len(_isolated_state) == 1

        t1 = t0 + timedelta(hours=6)  # old cooldown would have re-fired by now
        fired = ha.check_and_alert(_health(_STALE_21), now=t1)

        assert "db.stale_sources" not in fired
        assert len(_isolated_state) == 1  # still just the one email

    def test_same_set_in_a_different_order_does_not_refire(self, _isolated_state):
        """The dedupe key is the *set* of names — list order (e.g. from a
        resort by age) must not look like a change."""
        t0 = datetime(2026, 9, 28, 0, 0, tzinfo=timezone.utc)
        ha.check_and_alert(_health(_STALE_21), now=t0)

        t1 = t0 + timedelta(hours=1)
        fired = ha.check_and_alert(_health(_STALE_21_REORDERED), now=t1)

        assert "db.stale_sources" not in fired
        assert len(_isolated_state) == 1

    def test_changed_set_refires_immediately_even_within_cooldown(self, _isolated_state):
        t0 = datetime(2026, 9, 28, 0, 0, tzinfo=timezone.utc)
        ha.check_and_alert(_health(_STALE_21), now=t0)

        t1 = t0 + timedelta(minutes=15)  # well inside the old 6h cooldown
        fired = ha.check_and_alert(_health(_STALE_21_CHANGED), now=t1)

        assert "db.stale_sources" in fired
        assert len(_isolated_state) == 2

    def test_daily_reminder_fires_even_when_the_set_never_changes(self, _isolated_state):
        t0 = datetime(2026, 9, 28, 0, 0, tzinfo=timezone.utc)
        ha.check_and_alert(_health(_STALE_21), now=t0)

        t1 = t0 + timedelta(hours=25)  # past STALE_SOURCES_MAX_REMINDER_HOURS (24)
        fired = ha.check_and_alert(_health(_STALE_21), now=t1)

        assert "db.stale_sources" in fired
        assert len(_isolated_state) == 2

    def test_reminder_cap_is_shorter_than_the_default_cooldown_would_have_allowed(self, _isolated_state):
        # DEFAULT_COOLDOWN_HOURS is 6h; the stale_sources reminder cap must
        # be its own, longer, constant -- not silently inherit the 6h value.
        assert ha.STALE_SOURCES_MAX_REMINDER_HOURS > ha.DEFAULT_COOLDOWN_HOURS

    def test_dropping_below_threshold_then_recurring_refires(self, _isolated_state):
        t0 = datetime(2026, 9, 28, 0, 0, tzinfo=timezone.utc)
        ha.check_and_alert(_health(_STALE_21), now=t0)

        t1 = t0 + timedelta(minutes=5)
        healthy = ha.check_and_alert(_health([]), now=t1)
        assert "db.stale_sources" not in healthy

        # Recurs minutes later with the *same* set as the first incident --
        # must fire again since the condition cleared in between.
        t2 = t1 + timedelta(minutes=5)
        fired = ha.check_and_alert(_health(_STALE_21), now=t2)

        assert "db.stale_sources" in fired
        assert len(_isolated_state) == 2

    def test_other_checks_are_unaffected_by_dedupe_logic(self, _isolated_state):
        """db.failed_pulls has no dedupe_key -- it must keep the plain
        cooldown_hours behavior this change didn't touch."""
        t0 = datetime(2026, 9, 28, 0, 0, tzinfo=timezone.utc)
        health = _health([])
        health["db"]["failed_pulls_1h"] = 999

        ha.check_and_alert(health, now=t0)
        assert any("failed pulls" in s for s, _, _ in _isolated_state)

        t1 = t0 + timedelta(hours=1)  # inside the 6h default cooldown
        fired = ha.check_and_alert(health, now=t1)
        assert "db.failed_pulls" not in fired

        t2 = t0 + timedelta(hours=7)  # past the 6h default cooldown
        fired = ha.check_and_alert(health, now=t2)
        assert "db.failed_pulls" in fired
