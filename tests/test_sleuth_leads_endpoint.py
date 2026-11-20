"""Leads and thesis-history reads for the Research Desk (3pacs/GRID#1164).

* A failed read is HTTP 503 with ``available: false`` and ``null`` fields. It
  must not look like "no leads" or "no thesis change".
* Each lead carries its stored tests (the reviewed test its verdict declared
  and the baseline), and says so when a stored link does not resolve.
* A row is a declared test only when the lead's pointer, the test's identity,
  its role and source, and its criteria all agree. Staged, orphaned and
  malformed rows are shown as unverified, never counted.
* Page, total and tests come from one snapshot, in a total order.
* A GET runs no DDL.
* The thesis history says its scoring is held and that it is not the Desk's
  research-decision log.
"""

from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, inspect
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
    "CREATE TABLE discovered_hypotheses (id TEXT PRIMARY KEY, pattern_type TEXT, "
    "test_criteria TEXT, status TEXT, "
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


_TERMS = {"ticker": "BAH", "direction": "up", "window_days": 14, "min_move_pct": 2.0}


def _lead_row(lead_id, *, category="connection_found", status="resolved", declared=None,
              priority=0.6, terms=_TERMS, created_at="2026-11-08T02:37:25+00:00"):
    """A lead as ``investigate_intersection_lead`` leaves it: when it declared
    a test, the pointer and the lead's own record of the terms are both set."""
    hypotheses = [{"id": declared, **terms}] if declared and terms else []
    return (lead_id, f"question for {lead_id}", category, priority, "[]", status, None,
            "[]", json.dumps(hypotheses), created_at, None, "intersection",
            "hypothesis_declared" if declared else None, None, declared)


def _add_lead(engine, lead_id, **kwargs):
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "INSERT INTO investigation_leads VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            _lead_row(lead_id, **kwargs))


def _test_row(test_id, lead_id, source, *, status="active", pattern=None, **criteria_changes):
    criteria = {"ticker": "BAH", "window_days": 14, "min_move_pct": 2.0,
                "expected_direction": "up", "source": source, "lead_id": lead_id}
    criteria.update(criteria_changes)
    criteria = {k: v for k, v in criteria.items() if v is not ...}
    if pattern is None:
        pattern = si.BASELINE_PATTERN if source == si.BASELINE_PATTERN else si.HYPOTHESIS_PATTERN
    return (test_id, pattern, json.dumps(criteria), status, "2026-11-08T03:10:26+00:00",
            None, 0, 0, None, None, None, None, None, None)


def _add_test(engine, test_id, lead_id, source, **kwargs):
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "INSERT INTO discovered_hypotheses VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            _test_row(test_id, lead_id, source, **kwargs))


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
    assert [(t["id"], t["verified"], t["state"]) for t in lead["unverified_tests"]] == [
        (si.sleuth_test_id(LEAD_D), False, si.TEST_LINK_MISMATCH)]


def test_single_lead_read_carries_the_same_link(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A))
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth")

    body = client(engine).get(f"/leads/{LEAD_A}").json()

    assert body["available"] is True
    assert [t["id"] for t in body["lead"]["declared_tests"]] == [si.sleuth_test_id(LEAD_A)]


# ── a row is declared only when every stored fact agrees ────────────────────


def _only_lead(client, engine):
    return client(engine).get("/leads").json()["leads"][0]


def _unverified(lead):
    return [(t["id"], t["state"], t["faults"]) for t in lead["unverified_tests"]]


def test_a_test_row_committed_before_the_lead_pointer_is_staged_not_declared(client):
    """``investigate_intersection_lead`` commits the test row, then saves the
    lead. Between the two the row exists and the lead is still open."""
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, status="new")
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth")

    lead = _only_lead(client, engine)

    assert lead["declared_tests"] == []
    assert _unverified(lead) == [(si.sleuth_test_id(LEAD_A), si.TEST_ROW_STAGED, [])]
    assert lead["unverified_tests"][0]["role"] is None
    assert lead["test_link_problems"] == [
        {"problem": si.TEST_ROW_STAGED, "id": si.sleuth_test_id(LEAD_A)}]


def test_a_test_row_a_closed_lead_does_not_point_at_is_orphaned(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, status="dead_end")
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth")
    _add_test(engine, si.baseline_id(LEAD_A), LEAD_A, si.BASELINE_PATTERN)

    lead = _only_lead(client, engine)

    assert [t["role"] for t in lead["declared_tests"]] == [si.TEST_ROLE_BASELINE]
    assert _unverified(lead) == [(si.sleuth_test_id(LEAD_A), si.TEST_ROW_ORPHANED, [])]


@pytest.mark.parametrize(("row_changes", "faults"), [
    ({"pattern": si.BASELINE_PATTERN}, ["pattern_type"]),
    ({"source": si.BASELINE_PATTERN, "pattern": si.HYPOTHESIS_PATTERN}, ["source"]),
    ({"source": ...}, ["source"]),
    ({"expected_direction": ...}, ["expected_direction"]),
    ({"expected_direction": "sideways"}, ["expected_direction"]),
    ({"ticker": "BTC-USD"}, ["ticker"]),
    ({"window_days": 0}, ["window_days"]),
    ({"min_move_pct": ...}, ["min_move_pct"]),
])
def test_a_pointed_row_with_the_wrong_role_or_criteria_is_malformed(client, row_changes, faults):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A))
    changes = dict(row_changes)
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, changes.pop("source", "sleuth"), **changes)

    lead = _only_lead(client, engine)

    assert lead["declared_tests"] == []
    assert _unverified(lead) == [(si.sleuth_test_id(LEAD_A), si.TEST_ROW_MALFORMED, faults)]
    assert lead["test_link_problems"] == [
        {"problem": si.TEST_ROW_MALFORMED, "id": si.sleuth_test_id(LEAD_A), "faults": faults}]


def test_criteria_that_are_not_json_are_malformed_not_a_failed_read(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "INSERT INTO discovered_hypotheses (id, pattern_type, test_criteria, status) "
            "VALUES (?,?,?,?)", (si.baseline_id(LEAD_A), si.BASELINE_PATTERN, "{not json", "active"))

    response = client(engine).get("/leads")

    lead = response.json()["leads"][0]
    assert response.status_code == 200 and lead["declared_tests"] == []
    assert _unverified(lead) == [(si.baseline_id(LEAD_A), si.TEST_LINK_MISMATCH, [])]


def test_the_baseline_is_checked_on_its_own_terms(client):
    """A baseline id holding a reviewed-looking row is not a baseline, and it
    is not promoted to a declared test either."""
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A))
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth")
    _add_test(engine, si.baseline_id(LEAD_A), LEAD_A, "sleuth")

    lead = _only_lead(client, engine)

    assert [(t["id"], t["role"]) for t in lead["declared_tests"]] == [
        (si.sleuth_test_id(LEAD_A), si.TEST_ROLE_DECLARED)]
    assert _unverified(lead) == [
        (si.baseline_id(LEAD_A), si.TEST_ROW_MALFORMED, ["pattern_type", "source"])]


def test_a_bad_declared_row_does_not_cost_the_lead_its_baseline(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A))
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth", ticker="BTC-USD")
    _add_test(engine, si.baseline_id(LEAD_A), LEAD_A, si.BASELINE_PATTERN)

    lead = _only_lead(client, engine)

    assert [t["role"] for t in lead["declared_tests"]] == [si.TEST_ROLE_BASELINE]
    assert all(t["verified"] is True for t in lead["declared_tests"])


def test_a_pointer_at_another_leads_test_is_not_a_declared_test(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_B))
    _add_test(engine, si.sleuth_test_id(LEAD_B), LEAD_A, "sleuth")

    lead = _only_lead(client, engine)

    assert lead["declared_tests"] == []
    assert _unverified(lead) == [(si.sleuth_test_id(LEAD_B), si.TEST_LINK_FOREIGN, [])]


@pytest.mark.parametrize(("lead_changes", "faults"), [
    ({"terms": None}, ["declared_terms_missing"]),
    ({"terms": {**_TERMS, "direction": "down"}}, ["direction"]),
    ({"terms": {**_TERMS, "ticker": "LDOS", "window_days": 30}}, ["ticker", "window_days"]),
])
def test_the_leads_own_record_must_match_the_test_row(client, lead_changes, faults):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A), **lead_changes)
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth")

    lead = _only_lead(client, engine)

    assert lead["declared_tests"] == []
    assert _unverified(lead) == [(si.sleuth_test_id(LEAD_A), si.TEST_LEAD_DISAGREES, faults)]


def test_what_declare_hypothesis_and_declare_baseline_write_verifies():
    """The checks are held to the writers' own output, not to a test fixture."""
    written: dict[str, dict] = {}

    class Conn:
        def execute(self, _stmt, params):
            written[params["id"]] = params
            return SimpleNamespace(rowcount=1)

    evidence = [{"role": "agrees", "ticker": "BAH", "direction": "up"}]
    lead = SimpleNamespace(id=LEAD_A, question="q", evidence=evidence)
    si.declare_hypothesis(Conn(), lead, "BAH", "up", {"invalidation": "x"})
    si.declare_baseline(Conn(), LEAD_A, "q", evidence)

    for test_id, pattern, source in (
            (si.sleuth_test_id(LEAD_A), si.HYPOTHESIS_PATTERN, si.SLEUTH_SOURCE),
            (si.baseline_id(LEAD_A), si.BASELINE_PATTERN, si.BASELINE_PATTERN)):
        row = {"pattern_type": written[test_id]["ptype"]}
        criteria = json.loads(written[test_id]["criteria"])
        assert criteria["lead_id"] == LEAD_A
        assert si._row_faults(row, criteria, pattern=pattern, source=source) == []


# ── one snapshot, one order ─────────────────────────────────────────────────


def _file_engine(tmp_path):
    """A file database with real transactions: a reader that has begun keeps
    its snapshot while another connection commits (SQLite WAL)."""
    path = tmp_path / "leads.sqlite"
    engine = create_engine(f"sqlite:///{path}")

    @event.listens_for(engine, "connect")
    def _connect(dbapi_connection, _record):
        dbapi_connection.isolation_level = None  # the driver must not manage BEGIN
        dbapi_connection.execute("PRAGMA journal_mode=WAL")

    @event.listens_for(engine, "begin")
    def _begin(conn):
        conn.exec_driver_sql("BEGIN")

    with engine.begin() as conn:
        conn.exec_driver_sql(_LEADS_DDL)
        conn.exec_driver_sql(_TESTS_DDL)
    return engine, path


def test_a_write_between_the_reads_cannot_split_one_response(client, tmp_path):
    engine, path = _file_engine(tmp_path)
    _add_lead(engine, LEAD_A, declared=si.sleuth_test_id(LEAD_A))
    _add_test(engine, si.sleuth_test_id(LEAD_A), LEAD_A, "sleuth")
    fired = []

    @event.listens_for(engine, "before_cursor_execute")
    def _interleave(_conn, _cursor, statement, *_rest):
        # Once the page is read and before the total and the tests are:
        # another writer adds a lead and removes the first lead's test.
        if "COUNT(*)" in statement and not fired:
            fired.append(statement)
            other = sqlite3.connect(path)
            other.execute("INSERT INTO investigation_leads VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                          _lead_row(LEAD_B))
            other.execute("DELETE FROM discovered_hypotheses")
            other.commit()
            other.close()

    api = client(engine)
    during = api.get("/leads").json()
    after = api.get("/leads").json()

    assert fired, "the interleaved write never ran"
    # The first response is the database as it was when its read began.
    assert during["total"] == 1 and during["has_more"] is False
    assert [lead["id"] for lead in during["leads"]] == [LEAD_A]
    assert [t["role"] for t in during["leads"][0]["declared_tests"]] == [si.TEST_ROLE_DECLARED]
    assert during["leads"][0]["test_link_problems"] == []
    # The next one is the database after the write, also whole.
    assert after["total"] == 2 and len(after["leads"]) == 2
    assert {lead["id"]: [p["problem"] for p in lead["test_link_problems"]]
            for lead in after["leads"]} == {LEAD_A: [si.TEST_LINK_MISSING], LEAD_B: []}


def test_the_page_total_and_tests_are_read_on_one_connection_in_one_transaction(client):
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    _add_lead(engine, LEAD_A)
    seen = []

    @event.listens_for(engine, "before_cursor_execute")
    def _record(conn, _cursor, statement, *_rest):
        if statement.lstrip().upper().startswith("SELECT"):
            seen.append((id(conn.get_transaction()), conn.in_transaction()))

    client(engine).get("/leads")
    client(engine).get(f"/leads/{LEAD_A}")

    list_read, single_read = seen[:3], seen[3:]
    assert len(list_read) == 3 and len(single_read) == 2
    for read in (list_read, single_read):
        assert len({transaction for transaction, _ in read}) == 1
        assert all(in_transaction for _, in_transaction in read)


def test_postgres_snapshot_is_repeatable_read_and_read_only():
    asked = {}

    class Conn:
        def execution_options(self, **options):
            asked.update(options)
            return self

        def begin(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"), connect=Conn)

    with si.read_snapshot(engine):
        pass

    assert asked == {"isolation_level": "REPEATABLE READ", "postgresql_readonly": True}


def test_tied_leads_page_without_repeats_or_gaps(client):
    """Same priority, same created_at: only the id can order them."""
    engine = _engine(_LEADS_DDL, _TESTS_DDL)
    ids = [f"LEAD-INTX-{n:012d}" for n in (7, 3, 9, 1, 5, 8, 2, 6, 4)]
    for lead_id in ids:
        _add_lead(engine, lead_id)
    api = client(engine)

    pages = [api.get(f"/leads?limit=2&offset={offset}").json() for offset in range(0, 10, 2)]

    paged = [lead["id"] for page in pages for lead in page["leads"]]
    assert paged == sorted(ids)
    assert [page["has_more"] for page in pages] == [True, True, True, True, False]
    assert {page["total"] for page in pages} == {9}


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
