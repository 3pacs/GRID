"""Supply-chain FRED pull: an empty window is no new data, not a failure.

On 2026-11-07 every Hermes repair of Supply_Chain logged 8 "ISM <id>: 'date'"
warnings (172/24h): fedfred 3.0's to_pd_df raises KeyError('date') when FRED
returns no observations, which is the normal answer for monthly series asked
for the last few weeks. The pull also reported PARTIAL whenever it wrote no
rows, even with no errors. Also: a Tiingo news network timeout is a warning.
No network, no database.
"""
from __future__ import annotations

import sys
import types
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from ingestion.altdata import supply_chain as sc


@pytest.fixture
def fred(monkeypatch):
    client = MagicMock()
    mod = types.ModuleType("fedfred")
    mod.FredAPI = MagicMock(return_value=client)
    monkeypatch.setitem(sys.modules, "fedfred", mod)
    monkeypatch.setattr(sc.time, "sleep", lambda s: None)
    return client


def _puller(existing=frozenset()):
    with patch.object(sc.SupplyChainPuller, "_resolve_source_id", return_value=1):
        p = sc.SupplyChainPuller(MagicMock(), fred_api_key="k")
    p._get_existing_dates = MagicMock(return_value=set(existing))
    p._insert_raw = MagicMock()
    return p


def _frame(periods: dict[str, float]) -> pd.DataFrame:
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in periods], name="date")
    return pd.DataFrame({"value": list(periods.values())}, index=idx)


def test_empty_window_is_success_with_no_warnings(fred):
    fred.get_series_observations.side_effect = KeyError("date")
    p = _puller()
    with patch.object(sc.log, "warning") as warn:
        res = p._pull_ism_fred(date(2026, 10, 17))
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 0 and res["errors"] == []
    warn.assert_not_called()
    p._insert_raw.assert_not_called()


def test_no_new_rows_is_success(fred):
    fred.get_series_observations.return_value = _frame({"2026-09-01": 103.9})
    p = _puller(existing={date(2026, 9, 1)})
    res = p._pull_ism_fred(date(2026, 8, 1))
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 0


def test_new_rows_are_written(fred):
    fred.get_series_observations.return_value = _frame({"2026-09-01": 103.9})
    p = _puller()
    res = p._pull_ism_fred(date(2026, 8, 1))
    assert res["status"] == "SUCCESS"
    assert res["rows_inserted"] == len(sc.ISM_FRED_SERIES)


def test_other_key_errors_still_fail(fred):
    fred.get_series_observations.side_effect = KeyError("observations")
    p = _puller()
    res = p._pull_ism_fred(date(2026, 10, 17))
    assert res["status"] == "PARTIAL"
    assert len(res["errors"]) == len(sc.ISM_FRED_SERIES)


def test_tiingo_news_network_timeout_is_a_warning():
    from ingestion import tiingo_news_pull as tn

    with patch.object(tn, "_TIINGO_API_KEY", "test-key"), \
         patch.object(tn.TiingoNewsPuller, "_resolve_source_id", return_value=1):
        p = tn.TiingoNewsPuller(MagicMock())
    with patch.object(tn, "_TIINGO_API_KEY", "test-key"), \
         patch("requests.get", side_effect=requests.ReadTimeout("Read timed out. (read timeout=30)")), \
         patch.object(tn.log, "error") as err, patch.object(tn.log, "warning") as warn:
        res = p.pull_ticker_news("XLB")
    assert res["status"] == "FAILED"
    err.assert_not_called()
    warn.assert_called()
