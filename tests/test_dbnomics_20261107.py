"""DBnomics: live series ids, no Eurostat overlap, no re-insert of unchanged vintages.

On 2026-11-07 DBnomics held 166,650 raw_series rows, ~165k of them exact
repeats (the 1-hour _row_exists window re-inserted the full history on every
run). It also wrote eurozone_hicp_yoy from the retired prc_hicp_manr mirror
(frozen at 2025-12) next to the Eurostat puller, which made
store.observations raise MixedSourceError, and five of its ids 404'd.
No network, no database.
"""
from __future__ import annotations

import sys
import types
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.international import eurostat
from ingestion.physical import dbnomics


def _puller(stored_rows):
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn

    def execute(stmt, params=None):
        res = MagicMock()
        res.fetchall.return_value = stored_rows if "DISTINCT ON (obs_date)" in str(stmt) else []
        return res

    conn.execute.side_effect = execute
    with patch.object(dbnomics.DBnomicsPuller, "_resolve_source_id", return_value=965):
        p = dbnomics.DBnomicsPuller(engine)
    return p, conn


def _inserts(conn):
    return [c.args[1] for c in conn.execute.call_args_list
            if "INSERT INTO raw_series" in str(c.args[0])]


@pytest.fixture
def fake_dbnomics(monkeypatch):
    mod = types.ModuleType("dbnomics")
    mod.fetch_series = MagicMock()
    monkeypatch.setitem(sys.modules, "dbnomics", mod)
    return mod


def _frame(periods: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame({"period": [pd.Timestamp(p) for p in periods], "value": list(periods.values())})


def test_unchanged_history_is_not_reinserted(fake_dbnomics):
    p, conn = _puller([(date(2026, 8, 1), 4.3), (date(2026, 9, 1), 4.362)])
    fake_dbnomics.fetch_series.return_value = _frame({"2026-08-01": 4.3, "2026-09-01": 4.362})
    res = p.pull_series("ECB/IRS/M.IT.L.L40.CI.0000.EUR.N.Z", "italy_10y_ecb_dbnomics")
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 0
    assert _inserts(conn) == []


def test_new_and_revised_periods_are_inserted(fake_dbnomics):
    p, conn = _puller([(date(2026, 8, 1), 4.3), (date(2026, 9, 1), 4.362)])
    fake_dbnomics.fetch_series.return_value = _frame(
        {"2026-08-01": 4.3, "2026-09-01": 4.4, "2026-10-01": 4.5})
    res = p.pull_series("ECB/IRS/M.IT.L.L40.CI.0000.EUR.N.Z", "italy_10y_ecb_dbnomics")
    assert res["rows_inserted"] == 2
    assert [(r["od"], r["val"]) for r in _inserts(conn)] == [
        (date(2026, 9, 1), 4.4), (date(2026, 10, 1), 4.5)]


def test_no_feature_is_written_by_both_dbnomics_and_eurostat():
    assert not set(dbnomics.DBNOMICS_SERIES.values()) & set(eurostat.EUROSTAT_SERIES.values())
    assert not any(k.startswith("Eurostat/") for k in dbnomics.DBNOMICS_SERIES)


def test_series_ids_are_the_live_ones():
    ids = {v: k for k, v in dbnomics.DBNOMICS_SERIES.items()}
    assert ids["france_10y_yield"] == "ECB/IRS/M.FR.L.L40.CI.0000.EUR.N.Z"
    # 2026-11-09: us_credit_gdp_bis moved to the BIS puller (BIS direct is
    # current, the DBnomics mirror stops at 2024-Q4); see
    # tests/test_intl_pullers_20261109.py.
    assert "us_credit_gdp_bis" not in ids
    dead = ("BDF/FM/", "OECD/KEI/", "BIS/total_credit/", "BDF/BP/", "Bundesbank/BBDP1/")
    assert not any(k.startswith(dead) for k in dbnomics.DBNOMICS_SERIES)


def test_provider_failure_is_a_warning_and_a_failed_result(fake_dbnomics):
    p, _conn = _puller([])
    fake_dbnomics.fetch_series.side_effect = RuntimeError("HTTP 404")
    with patch.object(dbnomics.log, "error") as err, patch.object(dbnomics.log, "warning") as warn:
        res = p.pull_series("ECB/IRS/M.IT.L.L40.CI.0000.EUR.N.Z", "italy_10y_ecb_dbnomics")
    assert res["status"] == "FAILED"
    err.assert_not_called()
    warn.assert_called_once()
