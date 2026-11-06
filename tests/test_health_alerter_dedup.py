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


class TestAdditionsOnlyAndExplanations:
    """2026-11-04: emails re-sent at 33 -> 34 -> 35 stale sources and said
    only "N sources past freshness window". Now: re-fire only when a source
    newly goes stale, exclude switched-off sources from the count, and say
    why each source is stale."""

    def test_recovery_alone_does_not_refire_but_rebreak_does(self, _isolated_state):
        t0 = datetime(2026, 11, 4, 0, 0, tzinfo=timezone.utc)
        ha.check_and_alert(_health(_STALE_21 + ["flappy"]), now=t0)
        assert len(_isolated_state) == 1

        t1 = t0 + timedelta(hours=1)  # flappy recovers: no email
        assert "db.stale_sources" not in ha.check_and_alert(_health(_STALE_21), now=t1)
        assert len(_isolated_state) == 1

        t2 = t1 + timedelta(hours=1)  # flappy breaks again: newly stale, email
        assert "db.stale_sources" in ha.check_and_alert(_health(_STALE_21 + ["flappy"]), now=t2)
        subject = _isolated_state[-1][0]
        assert "newly stale: flappy" in subject

    def test_not_configured_sources_are_listed_but_not_counted(self, _isolated_state):
        health = _health(_STALE_21[:20])
        health["db"]["stale_sources"].append({
            "source": "Cloudflare_Radar", "last_pull": "never", "cadence": "DAILY",
            "age_hours": None, "not_configured": True, "last_attempt_status": "SUCCESS",
            "last_attempt_reason": "SKIPPED: ['no CF Radar token configured']",
        })
        # 20 counted (threshold is "> 20"), so no alert despite 21 entries.
        assert ha.check_and_alert(health, now=datetime(2026, 11, 4, tzinfo=timezone.utc)) == []
        assert ha._stale_sources_dedupe_key(health) == sorted(_STALE_21[:20])

    def test_body_groups_failures_by_reason_and_escapes_nothing_away(self):
        health = _health([])
        rows = health["db"]["stale_sources"]
        for name in ("polygon", "fmp"):
            rows.append({"source": name, "last_pull": "2026-09-30T21:05:00+00:00", "cadence": "DAILY",
                         "age_hours": 826.0, "last_attempt_status": "FAILED",
                         "last_attempt_at": "2026-11-03T21:05:33+00:00", "not_configured": False,
                         "last_attempt_reason": "PullerReportedFailure: puller did not report committed write count"})
        rows.append({"source": "BIS", "last_pull": "2026-09-27T00:00:00+00:00", "cadence": "WEEKLY",
                     "age_hours": 914.0, "last_attempt_status": "FAILED", "not_configured": False,
                     "last_attempt_at": "2026-11-04T01:33:10+00:00",
                     "last_attempt_reason": "RetryError[<Future at 0x7c raised HTTPError>]"})
        rows.append({"source": "NYFED_GSCPI", "last_pull": "2026-10-06T00:00:00+00:00", "cadence": "DAILY",
                     "age_hours": 686.0, "last_attempt_status": "SUCCESS", "not_configured": False,
                     "last_attempt_at": "2026-11-03T21:07:57+00:00",
                     "last_attempt_reason": "NO_NEW_DATA: run completed, 0 rows written"})
        rows.append({"source": "jodi_oil", "last_pull": "never", "cadence": "MONTHLY", "age_hours": None,
                     "last_attempt_status": None, "not_configured": False})
        subject, body = ha._stale_sources_render(health)

        assert subject == "GRID: 5 sources past freshness window"
        assert "[2] puller did not report committed write count" in body
        assert "polygon: last success 2026-09-30 21:05Z (34.4d ago, cadence DAILY); last attempt FAILED 2026-11-03 21:05Z" in body
        assert "<Future at 0x7c raised HTTPError>" in body
        assert "QUIET" in body and "NYFED_GSCPI" in body
        assert "NO ATTEMPT ON RECORD" in body and "jodi_oil: last success never (never succeeded" in body


def test_send_alert_html_escapes_and_keeps_plain_text(monkeypatch):
    from alerts import email as em

    captured = {}

    class _Cfg:
        ALERT_EMAIL_ENABLED = True

    monkeypatch.setattr(em, "_get_settings", lambda: _Cfg())
    monkeypatch.setattr(em, "_send", lambda subject, sections: captured.update(subject=subject, sections=sections))
    assert em.send_alert("subj", "line1\nRetryError[<Future x>]", severity="warning") is True
    section = captured["sections"][0]
    assert "&lt;Future x&gt;" in section["body"] and "pre-wrap" in section["body"]
    assert section["plain"] == "line1\nRetryError[<Future x>]"


# ── astrogrid.sky_snapshot_missing (issue #500, forward-only) ────────────────


def _sky_health(*, checked=True, due=True, present=False, today="2026-11-06") -> dict:
    return {
        "timestamp": f"{today}T03:00:00+00:00",
        "db": {"healthy": True, "stale_sources": []},
        "astrogrid": {
            "sky_snapshot": {
                "table": "astrogrid.sky_snapshot",
                "checked": checked,
                "today": today,
                "due": due,
                "due_hour_utc": 2,
                "present_today": present if checked else None,
                "latest_snapshot_date": "2026-11-05",
                "latest_created_at": "2026-11-05T00:04:00+00:00",
            }
        },
    }


class TestSkySnapshotMissing:
    KEY = "astrogrid.sky_snapshot_missing"

    def test_fires_when_due_and_missing(self, _isolated_state):
        now = datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
        assert ha.check_and_alert(_sky_health(), now=now) == [self.KEY]
        subject, body, severity = _isolated_state[-1]
        assert "2026-11-06" in subject and "latest 2026-11-05" in subject
        assert "forward-only" in body and severity == "warning"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"present": True},           # today's row exists
            {"due": False},              # before 02:00Z
            {"checked": False},          # relation missing / query error
        ],
    )
    def test_silent_when_present_not_due_or_unchecked(self, _isolated_state, kwargs):
        now = datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
        assert self.KEY not in ha.check_and_alert(_sky_health(**kwargs), now=now)

    def test_silent_for_older_producer_without_block(self, _isolated_state):
        now = datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
        assert self.KEY not in ha.check_and_alert(_health([]), now=now)

    def test_once_per_missing_day_then_again_next_day(self, _isolated_state):
        t0 = datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
        assert ha.check_and_alert(_sky_health(), now=t0) == [self.KEY]
        # Later Hermes cycles the same day: no repeat.
        assert ha.check_and_alert(_sky_health(), now=t0 + timedelta(hours=8)) == []
        # Next day still missing: new content, fires again.
        t1 = datetime(2026, 11, 7, 2, 5, tzinfo=timezone.utc)
        assert ha.check_and_alert(_sky_health(today="2026-11-07"), now=t1) == [self.KEY]

    def test_recovery_clears_state(self, _isolated_state):
        t0 = datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
        ha.check_and_alert(_sky_health(), now=t0)
        ha.check_and_alert(_sky_health(present=True), now=t0 + timedelta(hours=1))
        assert ha.check_and_alert(_sky_health(), now=t0 + timedelta(hours=2)) == [self.KEY]
