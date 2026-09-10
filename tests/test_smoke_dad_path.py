"""Unit tests for scripts/smoke_dad_path.py — pure logic only, HTTP mocked.

Covers: widget catalog validation, compose branch classification, as_of age
math, SSE frame parsing, secret redaction, and report rendering.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.smoke_dad_path import (
    StepResult,
    as_of_age_days,
    compose_branch,
    find_index_asset,
    find_mascot_asset,
    iter_sse_events,
    redact_secrets,
    render_report,
    validate_widget_types,
    worst_status,
)

# ── validate_widget_types ────────────────────────────────────────────────


class TestValidateWidgetTypes:
    def test_all_valid(self):
        widgets = [{"type": "verdict"}, {"type": "ticker_pulse"}, {"type": "money_flow"}]
        assert validate_widget_types(widgets) == []

    def test_flags_unknown_type(self):
        widgets = [{"type": "verdict"}, {"type": "not_a_widget"}]
        assert validate_widget_types(widgets) == ["not_a_widget"]

    def test_empty_list(self):
        assert validate_widget_types([]) == []

    def test_none_input(self):
        assert validate_widget_types(None) == []

    def test_missing_type_key(self):
        assert validate_widget_types([{"title": "no type here"}]) == [None]


# ── compose_branch ────────────────────────────────────────────────────────


class TestComposeBranch:
    def test_normal_layout(self):
        payload = {"spoken_reply": "here you go", "widgets": [{"type": "verdict"}]}
        assert compose_branch(payload) == "normal"

    def test_alert_created(self):
        payload = {"spoken_reply": "done", "alert_created": True, "alert": {"ticker": "NVDA"}}
        assert compose_branch(payload) == "alert_created"

    def test_cannot_fulfill(self):
        payload = {"spoken_reply": "can't do that yet", "cannot_fulfill": True}
        assert compose_branch(payload) == "cannot_fulfill"

    def test_error_on_non_dict(self):
        assert compose_branch(None) == "error"
        assert compose_branch([]) == "error"

    def test_error_when_no_spoken_reply(self):
        assert compose_branch({}) == "error"

    def test_alert_created_takes_priority_over_cannot_fulfill(self):
        payload = {"spoken_reply": "x", "alert_created": True, "cannot_fulfill": True}
        assert compose_branch(payload) == "alert_created"


# ── as_of_age_days ───────────────────────────────────────────────────────


class TestAsOfAgeDays:
    def test_none_input(self):
        assert as_of_age_days(None) is None

    def test_empty_string(self):
        assert as_of_age_days("") is None

    def test_iso_date_string(self):
        now = datetime(2026, 9, 10, tzinfo=timezone.utc)
        age = as_of_age_days("2026-09-03", now=now)
        assert age == pytest.approx(7.0, abs=0.01)

    def test_iso_datetime_string(self):
        now = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
        age = as_of_age_days("2026-09-10T00:00:00+00:00", now=now)
        assert age == pytest.approx(0.5, abs=0.01)

    def test_datetime_object(self):
        now = datetime(2026, 9, 10, tzinfo=timezone.utc)
        dt = now - timedelta(days=2)
        assert as_of_age_days(dt, now=now) == pytest.approx(2.0, abs=0.01)

    def test_date_object(self):
        from datetime import date

        now = datetime(2026, 9, 10, tzinfo=timezone.utc)
        assert as_of_age_days(date(2026, 9, 8), now=now) == pytest.approx(2.0, abs=0.01)

    def test_naive_datetime_treated_as_utc(self):
        now = datetime(2026, 9, 10, tzinfo=timezone.utc)
        naive = datetime(2026, 9, 9)  # noqa: DTZ001 — intentionally naive, that's what's under test
        assert as_of_age_days(naive, now=now) == pytest.approx(1.0, abs=0.01)

    def test_garbage_string_returns_none(self):
        assert as_of_age_days("not-a-date") is None

    def test_never_negative_for_future_date(self):
        now = datetime(2026, 9, 10, tzinfo=timezone.utc)
        future = now + timedelta(days=5)
        assert as_of_age_days(future, now=now) == 0.0


# ── iter_sse_events ───────────────────────────────────────────────────────


class TestIterSseEvents:
    def test_single_delta_event(self):
        buf = 'data: {"delta": "hello"}\n\n'
        events = iter_sse_events(buf)
        assert events == [{"delta": "hello"}]

    def test_multiple_events(self):
        buf = 'data: {"delta": "a"}\n\ndata: {"delta": "b"}\n\ndata: {"done": true}\n\n'
        events = iter_sse_events(buf)
        assert events == [{"delta": "a"}, {"delta": "b"}, {"done": True}]

    def test_ignores_non_data_lines(self):
        buf = 'event: ping\n\ndata: {"delta": "x"}\n\n'
        events = iter_sse_events(buf)
        assert events == [{"delta": "x"}]

    def test_skips_malformed_json(self):
        buf = 'data: {not json}\n\ndata: {"delta": "ok"}\n\n'
        events = iter_sse_events(buf)
        assert events == [{"delta": "ok"}]

    def test_error_event(self):
        buf = 'data: {"error": true, "message": "card busy"}\n\n'
        events = iter_sse_events(buf)
        assert events[0]["error"] is True

    def test_empty_buffer(self):
        assert iter_sse_events("") == []


# ── redact_secrets ───────────────────────────────────────────────────────


class TestRedactSecrets:
    def test_redacts_jwt(self):
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYWQifQ.abc123def456ghi789"
        out = redact_secrets(f"token was {jwt} in the log")
        assert jwt not in out
        assert "[REDACTED]" in out

    def test_redacts_bearer_header(self):
        out = redact_secrets("Authorization: Bearer abcdefgh12345678")
        assert "abcdefgh12345678" not in out
        assert "Bearer [REDACTED]" in out

    def test_redacts_env_style_secret(self):
        out = redact_secrets("GRID_JWT_SECRET=supersecretvalue123")
        assert "supersecretvalue123" not in out
        assert "[REDACTED]" in out

    def test_redacts_password_kv(self):
        out = redact_secrets("DB_PASSWORD=hunter22ok")
        assert "hunter22ok" not in out

    def test_redacts_json_style_secret(self):
        out = redact_secrets('{"password": "hunter22ok", "role": "contributor"}')
        assert "hunter22ok" not in out
        assert '"role": "contributor"' in out

    def test_leaves_normal_text_untouched(self):
        text = "grid-api restarted cleanly at 12:00 UTC"
        assert redact_secrets(text) == text

    def test_empty_string(self):
        assert redact_secrets("") == ""

    def test_none_passthrough(self):
        assert redact_secrets(None) is None


# ── find_index_asset / find_mascot_asset ─────────────────────────────────


class TestFindAssets:
    def test_find_index_asset(self):
        html = '<script src="/assets/index-abc123.js"></script>'
        assert find_index_asset(html) == "/assets/index-abc123.js"

    def test_find_index_asset_missing(self):
        assert find_index_asset("<html></html>") is None

    def test_find_mascot_asset(self):
        js = 'const img = "/assets/stepdad-mascot-9f8e.png";'
        assert find_mascot_asset(js) == "/assets/stepdad-mascot-9f8e.png"

    def test_find_mascot_asset_none_found(self):
        assert find_mascot_asset("<html>no images here</html>") is None

    def test_find_mascot_asset_searches_multiple_texts(self):
        assert find_mascot_asset("nothing", '"/assets/mascot.svg"') == "/assets/mascot.svg"


# ── worst_status ─────────────────────────────────────────────────────────


class TestWorstStatus:
    def test_broken_wins(self):
        assert worst_status(["ok", "degraded", "broken"]) == "broken"

    def test_blocked_beats_degraded(self):
        assert worst_status(["ok", "blocked", "degraded"]) == "blocked"

    def test_all_ok(self):
        assert worst_status(["ok", "ok"]) == "ok"

    def test_empty(self):
        assert worst_status([]) == "ok"


# ── render_report ────────────────────────────────────────────────────────


class TestRenderReport:
    def _steps(self):
        return [
            StepResult("health", "ok", 12.3, "status=ok"),
            StepResult("static", "degraded", 45.0, "mascot not found"),
            StepResult("composer", "broken", 100.0, "HTTP 500"),
            StepResult("auth", "blocked", None, "release dir not found"),
        ]

    def test_contains_summary_counts(self):
        report = render_report(self._steps(), {"base_url": "http://x", "release_dir": "/tmp", "generated_at": "now"})
        assert "**Works** (1)" in report
        assert "**Degraded** (1)" in report
        assert "**Broken** (1)" in report
        assert "**Blocked** (1)" in report

    def test_contains_evidence_table_rows(self):
        report = render_report(self._steps(), {"base_url": "http://x", "release_dir": "/tmp", "generated_at": "now"})
        assert "| health | ok | 12 | status=ok |" in report

    def test_redacts_secrets_in_notes(self):
        steps = [StepResult("auth", "blocked", None, "token mint failed: GRID_JWT_SECRET=abcdef123456")]
        report = render_report(steps, {"base_url": "http://x", "release_dir": "/tmp", "generated_at": "now"})
        assert "abcdef123456" not in report

    def test_top_problems_lists_broken_and_degraded_only(self):
        report = render_report(self._steps(), {"base_url": "http://x", "release_dir": "/tmp", "generated_at": "now"})
        assert "[broken] composer" in report
        assert "[degraded] static" in report
        assert "[ok] health" not in report

    def test_no_problems_message(self):
        steps = [StepResult("health", "ok", 1.0, "fine")]
        report = render_report(steps, {"base_url": "http://x", "release_dir": "/tmp", "generated_at": "now"})
        assert "None — every step reported ok." in report

    def test_blocked_section_present_when_blocked_items_exist(self):
        report = render_report(self._steps(), {"base_url": "http://x", "release_dir": "/tmp", "generated_at": "now"})
        assert "## Blocked items" in report
        assert "release dir not found" in report
