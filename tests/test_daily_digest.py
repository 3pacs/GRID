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
        (1, "pull_failure", "ERROR", "synthetic", "test", "", "", "", "PENDING", at),
    ]
    issue = digest._collect_issues(engine)[0]
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
