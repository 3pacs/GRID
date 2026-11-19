"""Eurostat: live dataset codes, and no re-insert of unchanged vintages.

On 2026-11-07 eurozone_hicp_yoy held 85,608 raw_series rows for 348 dates
(the 1-hour _row_exists window re-inserted the full history on every run)
and stopped at 2025-12 because prc_hicp_manr was frozen; the unemployment
and industrial-output queries matched no rows (EA19, indic_bt=PROD).
No network, no database.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.international import eurostat


def _payload(periods: dict[str, float]) -> dict:
    keys = list(periods)
    return {
        "dimension": {"time": {"category": {"index": {p: i for i, p in enumerate(keys)}}}},
        "value": {str(i): periods[p] for i, p in enumerate(keys)},
    }


def _puller(stored_rows):
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn

    def execute(stmt, params=None):
        res = MagicMock()
        res.fetchall.return_value = stored_rows if "DISTINCT ON (obs_date)" in str(stmt) else []
        return res

    conn.execute.side_effect = execute
    with patch.object(eurostat.EurostatPuller, "_resolve_source_id", return_value=219):
        p = eurostat.EurostatPuller(engine)
    return p, conn


def _inserts(conn):
    return [c.args[1] for c in conn.execute.call_args_list
            if "INSERT INTO raw_series" in str(c.args[0])]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(eurostat.time, "sleep", lambda s: None)


def test_unchanged_history_is_not_reinserted():
    p, conn = _puller([(date(2026, 8, 1), 3.2), (date(2026, 9, 1), 3.8)])
    with patch.object(p, "_fetch_data", return_value=_payload({"2026-08": 3.2, "2026-09": 3.8})):
        res = p.pull_series("q", "eurozone_hicp_yoy")
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 0
    assert _inserts(conn) == []


def test_new_and_revised_periods_are_inserted():
    p, conn = _puller([(date(2026, 8, 1), 3.2), (date(2026, 9, 1), 3.8)])
    with patch.object(p, "_fetch_data",
                      return_value=_payload({"2026-08": 3.2, "2026-09": 3.7, "2026-10": 4.0})):
        res = p.pull_series("q", "eurozone_hicp_yoy")
    assert res["rows_inserted"] == 2
    assert [(r["od"], r["val"]) for r in _inserts(conn)] == [
        (date(2026, 9, 1), 3.7), (date(2026, 10, 1), 4.0)]


def test_queries_use_the_live_codes():
    q = dict((v, k) for k, v in eurostat.EUROSTAT_SERIES.items())
    assert q["eurozone_hicp_yoy"].startswith("prc_hicp_minr?")
    assert "unit=RCH_A" in q["eurozone_hicp_yoy"] and "coicop18=TOTAL" in q["eurozone_hicp_yoy"]
    assert "geo=EA21" in q["eurozone_unemployment"] and "unit=PC_ACT" in q["eurozone_unemployment"]
    assert "indic_bt=PRD" in q["eu_industrial_output"] and "unit=I21" in q["eu_industrial_output"]
    assert "EA19" not in "".join(q.values()) and "indic_bt=PROD" not in "".join(q.values())


def test_provider_failure_is_a_warning_and_a_failed_result():
    p, _conn = _puller([])
    with patch.object(p, "_fetch_data", side_effect=RuntimeError("HTTP 503")), \
         patch.object(eurostat.log, "error") as err, patch.object(eurostat.log, "warning") as warn:
        res = p.pull_series("q", "eurozone_hicp_yoy")
    assert res["status"] == "FAILED"
    err.assert_not_called()
    warn.assert_called_once()
