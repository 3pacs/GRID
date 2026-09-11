"""Regression tests for catalyst_calendar ticker safety.

The ingestor now resolves sponsors through ``grid.signals.sponsor_resolver``
(class + name hard-reject, SEC, GRID name maps, cache, local LLM). These tests
inject a resolver so no network / DB is touched.
"""

from __future__ import annotations

from grid.ingestors.trial_ingestor import (
    TICKER_SHAPE_RE,
    deactivate_name_tickers,
    upsert_catalyst_calendar,
)
from grid.signals.sponsor_resolver import ResolvedSponsor


class _Cursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 3

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def close(self):
        pass


class _Conn:
    def __init__(self):
        self.cursor_obj = _Cursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


_EVENTS = [
    {
        "nct_id": "NCT1",
        "sponsor": "University of Somewhere",
        "sponsor_class": "OTHER",
        "expected_date": "2026-06-01",
        "event_type": "READOUT",
        "confidence_window": 30,
        "source": "clinicaltrials.gov",
        "notes": "Academic trial",
    },
    {
        "nct_id": "NCT2",
        "sponsor": "Moderna, Inc.",
        "sponsor_class": "INDUSTRY",
        "expected_date": "2026-07-01",
        "event_type": "READOUT",
        "confidence_window": 30,
        "source": "clinicaltrials.gov",
        "notes": "Industry trial",
    },
    {
        "nct_id": "NCT3",
        "sponsor": "Moderna, Inc.",  # same sponsor again -> resolver called once
        "sponsor_class": "INDUSTRY",
        "expected_date": "2026-08-01",
        "event_type": "READOUT",
        "confidence_window": 30,
        "source": "clinicaltrials.gov",
        "notes": "Second industry trial",
    },
]


def test_upsert_catalyst_calendar_writes_only_resolved_industry_sponsors():
    conn = _Conn()
    calls: list[tuple[str, str | None]] = []

    def resolver(engine, name, cls):
        calls.append((name, cls))
        if name == "Moderna, Inc.":
            return ResolvedSponsor("MRNA", "sec_exact", 0.95)
        return ResolvedSponsor(None, "non_industry", 1.0, "non_industry")

    resolved, rows = upsert_catalyst_calendar(conn, _EVENTS, engine=None, resolver=resolver)

    assert (resolved, rows) == (2, 2)
    assert conn.commits == 1 and conn.rollbacks == 0
    # one resolver call per distinct sponsor
    assert calls == [("University of Somewhere", "OTHER"), ("Moderna, Inc.", "INDUSTRY")]
    assert len(conn.cursor_obj.executed) == 2
    for sql, params in conn.cursor_obj.executed:
        assert params["ticker"] == "MRNA"
        assert "%(ticker)s" in sql and "%(nct_id)s" in sql  # parameterised, no interpolation
        assert "UPDATE catalyst_calendar" in sql and "INSERT INTO catalyst_calendar" in sql
        assert "NOT EXISTS" in sql  # idempotent on (nct_id, ticker, event_type)
    assert [p["nct_id"] for _, p in conn.cursor_obj.executed] == ["NCT2", "NCT3"]


def test_upsert_catalyst_calendar_survives_resolver_error():
    conn = _Conn()

    def resolver(engine, name, cls):
        raise RuntimeError("sec.gov down")

    resolved, rows = upsert_catalyst_calendar(conn, _EVENTS[:2], resolver=resolver)
    assert (resolved, rows) == (0, 0)
    assert conn.cursor_obj.executed == []
    assert conn.commits == 1


def test_deactivate_name_tickers_is_parameterised_and_idempotent():
    conn = _Conn()
    n = deactivate_name_tickers(conn)
    assert n == 3
    sql, params = conn.cursor_obj.executed[0]
    assert "SET is_active = FALSE" in sql and "!~ %s" in sql
    assert params == (TICKER_SHAPE_RE,)
    assert TICKER_SHAPE_RE == r"^[A-Z.\-]{1,6}$"
    assert conn.commits == 1
