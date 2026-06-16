"""Regression tests for catalyst_calendar ticker safety."""

from __future__ import annotations

from unittest.mock import patch

from grid.ingestors.trial_ingestor import upsert_catalyst_calendar


class _Cursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params):
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


def test_upsert_catalyst_calendar_skips_unresolved_sponsor_fragments():
    conn = _Conn()
    events = [
        {
            "nct_id": "NCT1",
            "sponsor": "University of Somewhere",
            "expected_date": "2026-06-01",
            "event_type": "READOUT",
            "confidence_window": 30,
            "source": "clinicaltrials.gov",
            "notes": "Academic trial",
        },
        {
            "nct_id": "NCT2",
            "sponsor": "Moderna, Inc.",
            "expected_date": "2026-07-01",
            "event_type": "READOUT",
            "confidence_window": 30,
            "source": "clinicaltrials.gov",
            "notes": "Industry trial",
        },
    ]

    def resolver(name):
        return {"Moderna, Inc.": "MRNA"}.get(name)

    with patch("grid.signals.trial_signal._resolve_ticker_sec", side_effect=resolver):
        count = upsert_catalyst_calendar(conn, events)

    assert count == 1
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert len(conn.cursor_obj.executed) == 1
    params = conn.cursor_obj.executed[0][1]
    assert params[0] == "MRNA"
    assert params[1] == "NCT2"

