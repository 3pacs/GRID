"""Leads and thesis-history reads for the Research Desk (3pacs/GRID#1164).

* A failed read is HTTP 503 with ``available: false`` and ``null`` fields. It
  must not look like "no leads" or "no thesis change".
* Each lead carries its stored tests (the reviewed test its verdict declared
  and the baseline), and says so when a stored link does not resolve.
* A GET runs no DDL.
* The thesis history says its scoring is held and that it is not the Desk's
  research-decision log.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import StaticPool

from api.routers import intelligence_thesis as router
from intelligence import sleuth_intersections as si

LEAD_A = "LEAD-INTX-aaaaaaaaaaaa"
LEAD_B = "LEAD-INTX-bbbbbbbbbbbb"
LEAD_C = "LEAD-INTX-cccccccccccc"
LEAD_D = "LEAD-INTX-dddddddddddd"

_LEADS_DDL = (
    "CREATE TABLE investigation_leads (id TEXT PRIMARY KEY, question TEXT, category TEXT, "
    "priority NUMERIC, evidence TEXT, status TEXT, findings TEXT, follow_up_leads TEXT, "
    "hypotheses TEXT, created_at TEXT, resolved_at TEXT, origin TEXT, outcome TEXT, "
    "outcome_reason TEXT, declared_hypothesis_id TEXT)"
)
_TESTS_DDL = (
    "CREATE TABLE discovered_hypotheses (id TEXT PRIMARY KEY, test_criteria TEXT, status TEXT, "
    "created_at TEXT, last_tested TEXT, times_tested INTEGER, times_correct INTEGER, "
    "kill_reason TEXT, killed_at TEXT, scoring_version TEXT, trial_p_value REAL, "
    "trial_q_value REAL, trial_ledger_sha256 TEXT)"
)


def _engine(*ddl: str):
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        for statement in ddl:
            conn.exec_driver_sql(statement)
    return engine


def _add_lead(engine, lead_id, *, category="connection_found", status="resolved",
              declared=None, priority=0.6):
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "INSERT INTO investigation_leads VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (lead_id, f"question for {lead_id}", category, priority, "[]", status, None,
             "[]", "[]", "2026-11-08T02:37:25+00:00", None, "intersection",
             "hypothesis_declared" if declared else None, None, declared))


def _add_test(engine, test_id, lead_id, source, *, status="active"):
    criteria = {"ticker": "BAH", "window_days": 14, "min_move_pct": 2.0,
                "expected_direction": "up", "source": source, "lead_id": lead_id}
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "INSERT INTO discovered_hypotheses VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (test_id, json.dumps(criteria), status, "2026-11-08T03:10:26+00:00", None,
             0, 0, None, None, None, None, None, None))


@pytest.fixture
def client(monkeypatch):
    def make(engine):
        app = FastAPI()
        app.include_router(router.router)
        app.dependency_overrides[router.require_auth] = lambda: "synthetic-test-identity"
        monkeypatch.setattr(router, "get_db_engine", lambda: engine)
        return TestClient(app)
    return make


# ── the lead-to-test link ───────────────────────────────────────────────────


def test_a_lead_carries_its_declared_test_and_its_baseline(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A))
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth")
    _add_test(engine, si.baseline_id(LEAD_A), LEAD_A, si.BASELINE_PATTERN)

    body = client(engine).get("/leads").json()

    lead = body["leads"][0]
    assert lead["declared_hypothesis_id"] == si.sleuth_test_id(LEAD_A)
    assert lead["test_link_problems"] == []
    by_role = {t["role"]: t for t in lead["declared_tests"]}
    assert set(by_role) == {si.TEST_ROLE_DECLARED, si.TEST_ROLE_BASELINE}
    declared = by_role[si.TEST_ROLE_DECLARED]
    assert declared["id"] == si.sleuth_test_id(LEAD_A)
    assert declared["linked_from_lead"] is True
    assert declared["test_criteria"] == {
        "ticker": "BAH", "window_days": 14, "min_move_pct": 2.0,
        "expected_direction": "up", "source": "sleuth", "lead_id": LEAD_A}
    assert declared["status"] == "active" and declared["times_tested"] == 0
    assert declared["trial_p_value"] is None  # untested: null, not 0
    assert by_role[si.TEST_ROLE_BASELINE]["linked_from_lead"] is False


def test_a_lead_with_only_a_baseline_has_no_declared_test_and_no_problem(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_B, status="dead_end")
    _add_test(engine, si.baseline_id(LEAD_B), LEAD_B, si.BASELINE_PATTERN)

    lead = client(engine).get("/leads").json()["leads"][0]

    assert [t["role"] for t in lead["declared_tests"]] == [si.TEST_ROLE_BASELINE]
    assert lead["test_link_problems"] == []


def test_a_stored_link_to_a_missing_row_is_reported_not_dropped(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_C, declared=si.sleuth_test_id(LEAD_C))

    lead = client(engine).get("/leads").json()["leads"][0]

    assert lead["declared_tests"] == []
    assert lead["test_link_problems"] == [
        {"problem": si.TEST_LINK_MISSING, "id": si.sleuth_test_id(LEAD_C)}]


def test_a_test_row_that_names_another_lead_is_not_joined(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_D, declared=si.sleuth_test_id(LEAD_D))
    _add_test(engine, si.sleuth_test_id(LEAD_D), LEAD_A, "sleuth")

    lead = client(engine).get("/leads").json()["leads"][0]

    assert lead["declared_tests"] == []
    assert lead["test_link_problems"] == [
        {"problem": si.TEST_LINK_MISMATCH, "id": si.sleuth_test_id(LEAD_D)}]


def test_single_lead_read_carries_the_same_link(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A))
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth")

    body = client(engine).get(f"/leads/{LEAD_A}").json()

    assert body["available"] is True
    assert [t["id"] for t in body["lead"]["declared_tests"]] == [si.sleuth_test_id(LEAD_A)]


# ── unreadable is not empty ─────────────────────────────────────────────────


def test_no_leads_is_an_empty_list_with_available_true(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)

    response = client(engine).get("/leads")

    assert response.status_code == 200
    assert response.json() == {"available": True, "leads": [], "total": 0,
                               "limit": 20, "offset": 0, "has_more": False}


def test_an_unreadable_leads_table_is_503_with_null_fields(client):
    engine = _engine()  # no tables: the read itself fails

    response = client(engine).get("/leads")
    body = response.json()

    assert response.status_code == 503
    assert body["available"] is False and body["status"] == "unavailable"
    assert body["leads"] is None and body["total"] is None
    assert body["source"] == "investigation_leads" and body["as_of"] is None
    assert "error" not in body


def test_an_unreadable_tests_table_fails_the_whole_read(client):
    """Leads without their tests would read as "no declared test"."""
    engine = _engine(_LEADS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A))

    response = client(engine).get("/leads")

    assert response.status_code == 503 and response.json()["leads"] is None


def test_the_response_does_not_carry_the_exception_text(client):
    engine = _engine()

    body = client(engine).get("/leads").json()

    assert "investigation_leads could not be read" in body["reason"]
    assert "no such table" not in json.dumps(body)


def test_reading_leads_creates_no_table(client):
    engine = _engine()

    client(engine).get("/leads")
    client(engine).get(f"/leads/{LEAD_A}")

    assert inspect(engine).get_table_names() == []


def test_missing_lead_is_404_and_unreadable_lead_is_503(client):
    readable = _engine(_LEADS_DDL, _TESTS_DDL)

    assert client(readable).get(f"/leads/{LEAD_A}").status_code == 404
    unreadable = client(_engine()).get(f"/leads/{LEAD_A}")
    assert unreadable.status_code == 503 and unreadable.json()["lead"] is None


def test_total_follows_the_category_filter(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, category="connection_found")
    _add_lead(engine, LEAD_B, category="anomaly")
    _add_lead(engine, LEAD_C, category="anomaly")

    body = client(engine).get("/leads?category=anomaly&limit=1").json()

    assert body["total"] == 2 and len(body["leads"]) == 1 and body["has_more"] is True


# ── thesis history ──────────────────────────────────────────────────────────


def test_thesis_history_says_scoring_is_held_and_what_it_is_not(client, monkeypatch):
    from intelligence import thesis_tracker

    rows = [SimpleNamespace(timestamp="2026-11-14T17:58:17+00:00",
                            to_dict=lambda: {"id": 5493, "outcome": None})]
    monkeypatch.setattr(thesis_tracker, "get_thesis_history", lambda engine, days: rows)

    body = client(object()).get("/thesis/history?days=30").json()

    assert body["available"] is True and body["count"] == 1
    assert body["as_of"] == "2026-11-14T17:58:17+00:00"  # the data's clock, not now
    assert body["scoring_held"] is thesis_tracker.THESIS_SCORING_HELD is True
    assert "not a log of research decisions" in body["note"]


def test_empty_thesis_history_has_no_as_of(client, monkeypatch):
    from intelligence import thesis_tracker

    monkeypatch.setattr(thesis_tracker, "get_thesis_history", lambda engine, days: [])

    body = client(object()).get("/thesis/history").json()

    assert body["snapshots"] == [] and body["as_of"] is None


def test_unreadable_thesis_history_is_503_with_null_fields(client, monkeypatch):
    from intelligence import thesis_tracker

    def boom(engine, days):
        raise RuntimeError("connection refused: secret-host:5432")

    monkeypatch.setattr(thesis_tracker, "get_thesis_history", boom)

    response = client(object()).get("/thesis/history")
    body = response.json()

    assert response.status_code == 503
    assert body["available"] is False and body["snapshots"] is None and body["count"] is None
    assert "secret-host" not in json.dumps(body)
