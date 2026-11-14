"""Synthetic policy tests only; no DB, providers, schedule or writer."""
from dataclasses import replace
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from intelligence.regime.capture_policy import SourceEvidence, SourceRequirement, assess_source, plan_sessions

D = date.fromisoformat
T = datetime.fromisoformat
HASH = "a" * 64
TARGET = D("2026-11-13")
NOW = T("2026-11-14T00:50:00+00:00")
REQ = SourceRequirement("dealer:SPY", TARGET, "dealer-exact-session/v1")
EVIDENCE = SourceEvidence("dealer:SPY", TARGET, T("2026-11-13T21:00:00+00:00"),
                          T("2026-11-13T22:00:00+00:00"), HASH, True, True)


def plan(days=(TARGET,), **kw):
    args = dict(now=NOW, start=days[0], end=days[-1], calendar_sessions=days, calendar_sha256=HASH,
                calendar_verified=True, calendar_start=days[0], calendar_end=days[-1], existing=set(),
                requirements={d: [replace(REQ, expected_observation=d)] for d in days},
                evidence={d: {REQ.source: replace(EVIDENCE, observation=d,
                          published_at=datetime.combine(d, datetime.min.time(), timezone.utc),
                          captured_at=datetime.combine(d, datetime.min.time(), timezone.utc))} for d in days})
    args.update(kw)
    return plan_sessions(**args)


def test_source_specific_lag_not_blanket_same_day():
    req = SourceRequirement("fred:WALCL", D("2026-11-11"), "h41-vintage/v1")
    evidence = replace(EVIDENCE, source=req.source, observation=req.expected_observation)
    result = assess_source(req, evidence, TARGET, NOW)
    assert result["ready"] and result["captured_before_cutoff"]
    assert not assess_source(REQ, replace(EVIDENCE, observation=D("2026-11-12")), TARGET, NOW)["ready"]


@pytest.mark.parametrize("change,reason", [
    ({"source": "other"}, "source_mismatch"),
    ({"observation": D("2026-11-12")}, "wrong_observation"),
    ({"receipt_verified": False}, "unverified_receipt"),
    ({"receipt_sha256": "not-a-hash"}, "unverified_receipt"),
    ({"vintage_verified": False}, "unverified_vintage"),
    ({"published_at": T("2026-11-14T00:00:00+00:00")}, "published_after_cutoff"),
    ({"captured_at": T("2026-11-13T20:00:00+00:00")}, "inconsistent_capture_time"),
    ({"captured_at": T("2026-11-14T02:00:00+00:00")}, "inconsistent_capture_time"),
])
def test_bad_evidence_holds(change, reason):
    assert assess_source(REQ, replace(EVIDENCE, **change), TARGET, NOW)["reason"] == reason


def test_late_capture_is_replay_not_forward():
    e = replace(EVIDENCE, captured_at=T("2026-11-14T00:30:00+00:00"))
    row = plan(evidence={TARGET: {REQ.source: e}})[0]
    assert row["outcome"] == "eligible_insert"
    assert row["capture_class"] == "historical_replay"
    assert row["forward_qualified"] is False


def test_existing_preserved_even_if_sources_missing():
    row = plan(existing={TARGET}, evidence={}, requirements={})[0]
    assert row["outcome"] == "already_present" and row["forward_qualified"] is False
    assert "sources" not in row


def test_bounded_catchup_keeps_hole_visible_and_older_first():
    days = tuple(map(D, ["2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"]))
    rows = plan(days, evidence={}, limit=1)
    assert all(r["outcome"] == "held_source_readiness" for r in rows)
    rows = plan(days, existing={days[0]}, limit=1)
    assert [r["outcome"] for r in rows] == ["already_present", "eligible_insert", "deferred_batch_limit", "deferred_batch_limit"]
    assert all(not r["forward_qualified"] for r in rows)


@pytest.mark.parametrize("days,now", [
    (["2026-11-13", "2026-11-16"], "2026-11-14T00:50:00+00:00"),  # Friday collected Saturday
    (["2026-11-25", "2026-11-27"], "2026-11-28T00:50:00+00:00"),  # Thanksgiving omitted, half day included
    (["2026-03-06", "2026-03-09"], "2026-03-10T00:50:00+00:00"),  # spring DST
    (["2026-10-30", "2026-11-02"], "2026-11-03T00:50:00+00:00"),  # autumn DST
])
def test_calendar_sessions_and_utc_cutoff(days, now):
    dates = tuple(map(D, days))
    rows = plan(dates, now=T(now))
    local = plan(dates, now=T(now).astimezone(ZoneInfo("America/Los_Angeles")))
    assert rows == local
    assert len(rows) == len(dates)
    assert rows[0]["outcome"] == "eligible_insert"
    assert rows[-1]["outcome"] == ("not_due" if dates[-1] >= T(now).date() else "eligible_insert")


def test_exclusive_cutoff_not_early_close_or_wallclock_session_end():
    assert plan(now=T("2026-11-13T23:59:59+00:00"))[0]["outcome"] == "not_due"
    assert plan(now=T("2026-11-14T00:00:00+00:00"))[0]["outcome"] == "eligible_insert"


@pytest.mark.parametrize("kw", [
    {"calendar_verified": False}, {"calendar_sha256": "x"},
    {"calendar_start": D("2026-11-14")}, {"calendar_end": D("2026-11-12")},
    {"calendar_sessions": [TARGET, TARGET]}, {"limit": 0}, {"limit": 21},
    {"now": datetime(2026, 11, 14)},
])
def test_invalid_planning_inputs_refused(kw):
    with pytest.raises(ValueError):
        plan(**kw)


@pytest.mark.parametrize("reqs", [[], [REQ, REQ]])
def test_missing_or_ambiguous_policy_is_not_ready(reqs):
    assert plan(requirements={TARGET: reqs})[0]["outcome"] == "held_missing_or_duplicate_policy"
