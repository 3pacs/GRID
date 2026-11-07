from __future__ import annotations

from datetime import datetime, timezone

import json
from unittest.mock import MagicMock

import pytest

from scripts import daily_digest as digest


def test_issue_and_failed_pull_times_preserve_date_and_timezone():
    at = datetime(2026, 11, 4, 23, 45, tzinfo=timezone.utc)
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [
        (1, "pull_failure", "ERROR", "synthetic", "test", "", "", "", "PENDING", at, 1, 0, 1, 0),
    ]
    issue = digest._collect_issues(engine)["items"][0]
    assert issue["time"] == at.isoformat()
    conn.execute.return_value.fetchone.return_value = None
    conn.execute.return_value.fetchall.return_value = [("synthetic", "test", "", "PENDING", at)]
    assert digest._collect_system_stats(engine)["failed_pulls"][0]["time"] == at.isoformat()


@pytest.mark.parametrize("stored_as_json", [False, True])
def test_digest_recovers_legacy_skips_from_full_report(stored_as_json):
    engine = MagicMock()
    report = {
        "endpoints": [{"ok": True}, {"ok": False, "error": "NO_AUTH", "data_quality": "SKIPPED"}],
        "journeys": [{"grade": "FAIL", "steps": 1, "steps_ok": 0, "blockers": ["/private: not tested"]}],
    }
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
        None, 2, 1, None, 0, 1, None, [], [], datetime(2026, 11, 4, tzinfo=timezone.utc),
        json.dumps(report) if stored_as_json else report,
    )
    audit = digest._collect_ux_audit(engine)
    assert audit["coverage"]["endpoints_failed"] == 0
    assert audit["coverage"]["endpoints_skipped"] == 1
    # Old journey FAIL grades alone cannot establish a measured journey failure.
    assert audit["coverage"]["journeys_failed"] is None
    body = next(s["body"] for s in digest._build_digest_sections([], audit, {}) if s["title"] == "UX Audit")
    assert "1 skipped" in body and "0 failed" in body
    assert "NO_AUTH: 1" in body
    assert "Score unavailable" in body and "Latency unavailable" in body
    assert "2026-11-04" in body


def test_digest_missing_coverage_stays_unknown():
    audit = {"score": None, "avg_latency_ms": None, "endpoints_ok": 1, "total_endpoints": 20}
    section = next(s for s in digest._build_digest_sections([], audit, {}) if s["title"] == "UX Audit")
    assert "Coverage unknown" in section["body"]
    assert section["accent"] == "amber"


def test_timestamp_display_handles_midnight_offsets_and_unknown():
    now = datetime(2026, 11, 5, 0, 15, tzinfo=timezone.utc)
    assert digest._format_event_time("2026-11-04T23:45:00+00:00", now) == "2026-11-04 23:45:00 UTC (30m ago)"
    assert digest._format_event_time("2026-11-05T01:45:00+02:00", now) == "2026-11-04 23:45:00 UTC (30m ago)"
    assert "timezone unknown" in digest._format_event_time("2026-11-04T23:45:00", now)
    assert "future" in digest._format_event_time("2026-11-05T00:16:00Z", now)
    assert digest._format_event_time(None, now) == "Time unknown"
    assert digest._format_event_time("23:45", now) == "Time unknown"


@pytest.mark.parametrize("payload", [None, "bad json", {}, {"endpoints": []}])
def test_absent_or_inconsistent_full_report_keeps_coverage_unknown(payload):
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
        8, 20, 1, 0, 0, 4, None, "[]", "[]", None, payload,
    )
    audit = digest._collect_ux_audit(engine)
    assert audit is not None and audit["coverage"] is None
    section = next(s for s in digest._build_digest_sections([], audit, {}) if s["title"] == "UX Audit")
    assert "Coverage unknown" in section["body"]
    assert "coverage incomplete or unknown" in section["body"]
    assert section["accent"] == "amber"


def test_new_audit_round_trips_into_digest_without_live_services(monkeypatch):
    from scripts import ux_auditor as audit
    monkeypatch.setattr(audit, "_get_auth_token", lambda _: None)
    monkeypatch.setattr(audit, "_test_endpoint", lambda ep, *_: {"path": ep["path"], "ok": True, "latency_ms": 0})
    monkeypatch.setattr(audit, "_run_llm_analysis", lambda *_: {"score": 8})
    engine = MagicMock()
    report = audit.run_ux_audit(engine)
    insert = engine.begin.return_value.__enter__.return_value.execute.call_args.args[0].compile().params
    at = datetime(2026, 11, 4, 23, 45, tzinfo=timezone.utc)
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
        insert["score"], insert["total_endpoints"], insert["endpoints_ok"], insert["avg_latency_ms"],
        insert["journey_pass"], insert["journey_total"], insert["priority_fix"],
        insert["friction_points"], insert["improvements"], at, insert["full_report"],
    )
    collected = digest._collect_ux_audit(engine)
    assert collected["coverage"]["endpoints_skipped"] == report["summary"]["endpoints_skipped"]
    issues = [{"severity": "ERROR", "time": at.isoformat(), "title": "Synthetic", "source": "test"}]
    sections = {s["title"]: s for s in digest._build_digest_sections(
        issues, collected, {"failed_pulls": [{"source": "test", "time": at.isoformat()}]},
    )}
    assert "0 failed" in sections["UX Audit"]["body"]
    assert "4 incomplete" in sections["UX Audit"]["body"]
    assert sections["UX Audit"]["accent"] == "amber"
    for title in ("Error Details", "System Health (24h)", "UX Audit"):
        assert "2026-11-04 23:45:00 UTC" in sections[title]["body"]
        assert "ago" in sections[title]["body"]


def test_collection_failure_does_not_claim_no_audit_ran():
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("synthetic unavailable database")
    assert digest._collect_ux_audit(engine) is None
    section = next(s for s in digest._build_digest_sections(None, None, {}) if s["title"] == "UX Audit")
    assert "status unknown" in section["body"]


class _State:
    last_daily_digest = None


def test_daily_digest_first_run_waits_for_digest_window(monkeypatch):
    from scripts import daily_digest

    sent: list[str] = []
    monkeypatch.setattr(
        daily_digest,
        "send_daily_digest",
        lambda *_args, **_kwargs: sent.append("sent") or {"sent": True},
    )

    result = daily_digest.maybe_send_daily_digest(
        _State(),
        engine=None,
        dry_run=False,
        now=datetime(2026, 5, 20, 3, 15, tzinfo=timezone.utc),
    )

    assert result is None
    assert sent == []


def test_daily_digest_uses_persisted_db_timestamp_after_restart(monkeypatch):
    from scripts import daily_digest

    class Engine:
        pass

    persisted = datetime(2026, 5, 20, 8, 10, tzinfo=timezone.utc)
    monkeypatch.setattr(daily_digest, "_latest_digest_sent_at", lambda _engine: persisted)

    sent: list[str] = []
    monkeypatch.setattr(
        daily_digest,
        "send_daily_digest",
        lambda *_args, **_kwargs: sent.append("sent") or {"sent": True},
    )

    result = daily_digest.maybe_send_daily_digest(
        _State(),
        Engine(),
        dry_run=False,
        now=datetime(2026, 5, 20, 8, 30, tzinfo=timezone.utc),
    )

    assert result is None
    assert sent == []


@pytest.mark.parametrize("count", [0, 3, 100, 137])
def test_issue_total_is_uncapped_and_excludes_future_records(count):
    from datetime import timedelta
    from sqlalchemy import create_engine, text

    engine = create_engine("sqlite:///:memory:")
    now = datetime(2026, 11, 7, 8, tzinfo=timezone.utc)
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE operator_issues (
            id INTEGER, category TEXT, severity TEXT, source TEXT, title TEXT,
            detail TEXT, hermes_diagnosis TEXT, fix_applied TEXT, fix_result TEXT,
            created_at TIMESTAMP
        )"""))
        for i in range(count + 2):
            at = now if i < count else now + timedelta(seconds=1)
            if count > 1 and i == count - 1:
                at = now - timedelta(hours=24)
            if i == count + 1:
                at = now - timedelta(hours=24, seconds=1)
            conn.execute(text("""INSERT INTO operator_issues VALUES
                (:id, 'test', :severity, 'synthetic', 'Synthetic issue', '', '', '', 'SUCCESS', :at)
            """), {"id": i, "severity": "CRITICAL" if i == 0 else ("WARNING" if i % 3 == 0 else "ERROR"), "at": at})
    report = digest._collect_issues(engine, now=now)
    assert report["counts"]["total"] == count
    assert report["counts"]["critical"] == min(count, 1)
    warnings = max(count - 1, 0) // 3
    assert report["counts"]["warning"] == warnings
    assert report["counts"]["error"] == max(count - 1, 0) - warnings
    assert len(report["items"]) == min(count, 100)
    if count:
        assert report["items"][0]["severity"] == "CRITICAL"
    engine.dispose()


def test_headline_summarizes_before_details_and_labels_record_counts():
    issues = [{"severity": "ERROR", "title": "Synthetic issue", "source": "test"}] * 100
    sections = digest._build_digest_sections(
        issues, {"score": 8}, {"sources_pulled": 0, "total_rows_ingested": 0},
        {"as_of": "2026-01-01", "generated_at": "2026-01-01T08:00:00Z", "candidates": []},
        issue_counts={"total": 137, "critical": 0, "error": 120, "warning": 17},
    )
    first = sections[0]["body"]
    assert "137" in first and "operator issue records" in first
    assert "120" in first and "critical/error" in first
    assert "not a current open-issue count" in first
    assert "UX" in first and "coverage unknown" in first
    assert "0 sources" in first
    assert "2026-01-01" in first
    detail = next(s["body"] for s in sections if s["title"] == "Error Details")
    assert "Showing 15 of 137" in detail
    assert "122 additional records" in detail


def test_issue_details_preserve_diagnosis_fix_status_and_escape_html():
    issue = {
        "severity": "ERROR", "title": "A < B", "source": "<source>",
        "detail": "Long detail <tag>\nSecond line", "diagnosis": "Cause & evidence",
        "fix_applied": "Retry <step>", "fix_result": "SUCCESS",
        "time": "2026-11-07T07:00:00Z",
    }
    sections = digest._build_digest_sections([issue], None, {})
    body = next(s["body"] for s in sections if s["title"] == "Error Details")
    for text in ("A &lt; B", "&lt;source&gt;", "Long detail &lt;tag&gt;", "Second line",
                 "Cause &amp; evidence", "Retry &lt;step&gt;", "SUCCESS"):
        assert text in body
    assert "<tag>" not in body


@pytest.mark.parametrize("board,expected", [
    ({"as_of": "2026-11-01", "generated_at": "2026-11-01T08:00:00Z"}, "6 calendar days old"),
    ({"as_of": "2026-11-08", "generated_at": "2026-11-08T08:00:00Z"}, "future"),
    ({}, "age unknown"),
    ({"as_of": "bad-date", "generated_at": "bad-date"}, "age unknown"),
])
def test_long_plays_snapshot_is_dated_and_never_claims_live_eligibility(board, expected):
    board["candidates"] = [{"ticker": "SYN", "stance": "entry_candidate", "gate": {}}]
    section = digest._build_long_plays_section(board, now=datetime(2026, 11, 7, 8, tzinfo=timezone.utc))
    assert expected in section["body"]
    assert "Recorded entry candidates" in section["body"]
    assert "not revalidated" in section["body"]
    assert section.get("accent") != "green"


def test_past_catalyst_date_is_not_presented_as_upcoming():
    board = {"as_of": "2026-11-01", "generated_at": "2026-11-01T08:00:00Z", "candidates": [
        {"ticker": "<SYN>", "stance": "entry_candidate", "price_as_of": "2026-10-30",
         "gate": {"next_catalyst_date": "2026-11-02"}},
    ]}
    body = digest._build_long_plays_section(board, now=datetime(2026, 11, 7, 8, tzinfo=timezone.utc))["body"]
    assert "2026-11-02 (past date; outcome unverified)" in body
    assert "2026-10-30" in body
    assert "&lt;SYN&gt;" in body


def test_send_preview_uses_full_total_and_never_sends(monkeypatch):
    import smtplib
    import alerts.email as email
    calls = []
    monkeypatch.setattr(smtplib, "SMTP", lambda *a, **k: calls.append("SMTP"))
    monkeypatch.setattr(email, "_send", lambda *a, **k: calls.append("send"))
    monkeypatch.setattr(digest, "_collect_issues", lambda *a, **k: {
        "items": [{"severity": "ERROR", "title": "Synthetic", "source": "test"}] * 100,
        "counts": {"total": 137, "critical": 2, "error": 120, "warning": 15},
    })
    monkeypatch.setattr(digest, "_collect_ux_audit", lambda *a: {"score": 8})
    monkeypatch.setattr(digest, "_collect_system_stats", lambda *a: {})
    monkeypatch.setattr(digest, "_collect_long_plays", lambda *a: None)
    result = digest.send_daily_digest(None, dry_run=True)
    assert result["issues_count"] == 137
    assert "137 issue records (24h)" in result["subject"]
    assert "2 critical" in result["subject"]
    assert "UX: 8/10" not in result["subject"]
    assert result["section_bodies"][0]["title"] == "Error Summary"
    assert calls == []


def test_unavailable_issue_history_never_becomes_zero(monkeypatch):
    monkeypatch.setattr(digest, "_collect_issues", lambda *a, **k: None)
    monkeypatch.setattr(digest, "_collect_ux_audit", lambda *a: None)
    monkeypatch.setattr(digest, "_collect_system_stats", lambda *a: {})
    monkeypatch.setattr(digest, "_collect_long_plays", lambda *a: None)
    result = digest.send_daily_digest(None, dry_run=True)
    assert result["issues_count"] is None
    assert "issue history unavailable" in result["subject"]
    assert "0 operator issues" not in result["subject"]
    assert "health status is unknown" in result["section_bodies"][0]["body"]



def test_issue_collector_failure_stays_unknown():
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("synthetic unavailable database")
    assert digest._collect_issues(engine) is None


@pytest.mark.parametrize("value,expected", [
    ("2026-11-08", "2026-11-08 (scheduled; unverified)"),
    ("2026-11-07", "2026-11-07 (today; outcome unverified)"),
    (None, "Date unknown"),
    ("<bad>", "&lt;bad&gt; (date unverified)"),
])
def test_catalyst_dates_do_not_invent_confirmations(value, expected):
    assert digest._catalyst_date(value, datetime(2026, 11, 7, tzinfo=timezone.utc)) == expected


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), None])
def test_invalid_projection_values_are_not_published(value):
    assert digest._fmt_num(value) == "&#8212;"



def test_summary_and_board_use_one_cutoff_and_readable_plain_text():
    now = datetime(2026, 11, 7, 0, 0, tzinfo=timezone.utc)
    board = {"as_of": "2026-11-06", "generated_at": "2026-11-06T08:00:00Z", "candidates": [
        {"ticker": "SYN", "stance": "entry_candidate", "gate": {"next_catalyst_date": "2026-11-06"}},
    ]}
    issue = {"severity": "ERROR", "title": "A < B", "source": "test", "detail": "Cause & fix"}
    sections = digest._build_digest_sections([issue], None, {}, board, now=now)
    summary = sections[0]["plain"]
    assert "Issue window (UTC): 2026-11-06 00:00:00 through 2026-11-07 00:00:00, inclusive" in summary
    assert "A < B" in summary and "1 calendar days old" in summary
    detail = next(s["plain"] for s in sections if s["title"] == "Error Details")
    assert "Cause & fix" in detail
    assert "SEV | RECORDED AT / AGE | ISSUE | SOURCE | FIX RESULT" in detail
    long_plays = next(s["plain"] for s in sections if s["title"] == "Long Plays")
    assert "1 calendar days old" in long_plays
    assert "past date; outcome unverified" in long_plays
    for section in sections:
        assert "<strong" not in section["plain"] and "<td" not in section["plain"]
        assert "&amp;" not in section["plain"]


def test_send_passes_collection_cutoff_to_renderer(monkeypatch):
    cutoffs = []
    monkeypatch.setattr(digest, "_collect_issues", lambda *a, **k: cutoffs.append(k["now"]) or None)
    monkeypatch.setattr(digest, "_collect_ux_audit", lambda *a: None)
    monkeypatch.setattr(digest, "_collect_system_stats", lambda *a: {})
    monkeypatch.setattr(digest, "_collect_long_plays", lambda *a: None)
    monkeypatch.setattr(digest, "_build_digest_sections", lambda *a, **k: cutoffs.append(k["now"]) or [])
    result = digest.send_daily_digest(None, dry_run=True)
    assert cutoffs[0] is cutoffs[1]
    assert result["timestamp"] == cutoffs[0].isoformat()


def test_digest_recap_tolerates_malformed_saved_candidate_entries():
    board = {"as_of": "2026-11-07", "candidates": [None, "bad", {
        "ticker": "SYN", "stance": "entry_candidate", "gate": {},
    }]}
    sections = digest._build_digest_sections([], None, {}, board)
    assert "1 recorded entry candidates" in sections[0]["body"]
    assert "SYN" in next(s["body"] for s in sections if s["title"] == "Long Plays")
