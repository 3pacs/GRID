"""Regression tests for the 2026-11-09 international/macro puller pass.

Pins:
* BasePuller._latest_stored_values: insert only new or revised values.
  BIS, RBI, IMF, BCB and OFR used the 1-hour ``_row_exists`` window and
  re-inserted their whole fetch window every run (ofr_fsi: 1,766,133 rows
  for 6,795 dates on grid-svr, 2026-11-09).
* BIS dataflow version ``~`` (WS_TC is at 2.0; ``WS_TC/1.0`` is a 404) and
  us_credit_gdp_bis moved from the stale DBnomics mirror to BIS direct.
* DBnomics: IMF/IFS (retired, frozen at 2021-08) dropped.
* BCB: SGS 7454 (monthly percentages, stopped 2025-07, mislabeled as a trade
  balance) replaced by SGS 22707 under a new raw id.
* OFR: FSM retired (404) -> FSI category sub-indices; STFM rebuilt on the
  keyless data.financialresearch.gov timeseries API.

No network and no database: every HTTP call and connection is mocked.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests


class FakeConn:
    """Answers source_catalog lookups and DISTINCT ON reads; records INSERTs."""

    def __init__(self, stored: dict[str, dict[date, float]] | None = None, source_id: int = 7) -> None:
        self.stored = stored or {}
        self.source_id = source_id
        self.inserts: list[dict[str, Any]] = []
        self.reads: list[tuple[str, dict[str, Any]]] = []

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> MagicMock:
        sql = str(stmt)
        params = params or {}
        res = MagicMock()
        if "INSERT INTO raw_series" in sql:
            self.inserts.append(params)
            res.rowcount = 1
        elif "DISTINCT ON (obs_date)" in sql:
            self.reads.append((sql, params))
            rows = sorted(self.stored.get(params.get("sid"), {}).items())
            start = params.get("start_date")
            res.fetchall.return_value = [r for r in rows if start is None or r[0] >= start]
        elif "MAX(obs_date)" in sql:
            rows = self.stored.get(params.get("sid"), {})
            res.fetchone.return_value = (max(rows) if rows else None,)
        else:
            res.fetchone.return_value = (self.source_id,)
            res.fetchall.return_value = []
        return res


def _engine(conn: FakeConn) -> MagicMock:
    engine = MagicMock()
    for attr in ("connect", "begin"):
        getattr(engine, attr).return_value.__enter__ = MagicMock(return_value=conn)
        getattr(engine, attr).return_value.__exit__ = MagicMock(return_value=False)
    return engine


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    import time

    monkeypatch.setattr(time, "sleep", lambda s: None)


# ---------------------------------------------------------------------------
# BasePuller._latest_stored_values
# ---------------------------------------------------------------------------


def test_latest_stored_values_is_bounded_and_vintage_aware() -> None:
    from ingestion.international.bis import BISPuller

    conn = FakeConn({"x": {date(2020, 1, 1): 1.0, date(2026, 1, 1): 2.0}})
    puller = BISPuller(_engine(conn))
    out = puller._latest_stored_values("x", conn, start_date=date(2025, 1, 1))
    assert out == {date(2026, 1, 1): 2.0}
    sql, params = conn.reads[-1]
    assert "pull_status = 'SUCCESS'" in sql and "ORDER BY obs_date, pull_timestamp DESC" in sql
    assert params == {"sid": "x", "src": 7, "start_date": date(2025, 1, 1)}
    assert puller._latest_stored_values("x", conn) == {date(2020, 1, 1): 1.0, date(2026, 1, 1): 2.0}


# ---------------------------------------------------------------------------
# BIS
# ---------------------------------------------------------------------------


def test_bis_writes_only_new_or_revised_values(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.international import bis

    conn = FakeConn({"bis_credit_gdp_gap_us": {date(2025, 10, 1): -11.0, date(2026, 1, 1): -11.3}})
    puller = bis.BISPuller(_engine(conn))
    monkeypatch.setattr(
        bis.sdmx_json, "fetch_bis_series",
        lambda flow, key, start: [(date(2025, 10, 1), -11.0), (date(2026, 1, 1), -11.32), (date(2026, 4, 1), -11.5)],
    )
    res = puller.pull_series("WS_CREDIT_GAP/Q.US.P.A.C")
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 2
    assert [(r["od"], r["val"]) for r in conn.inserts] == [(date(2026, 1, 1), -11.32), (date(2026, 4, 1), -11.5)]

    conn.inserts.clear()
    conn.stored["bis_credit_gdp_gap_us"].update({date(2026, 1, 1): -11.32, date(2026, 4, 1): -11.5})
    res = puller.pull_series("WS_CREDIT_GAP/Q.US.P.A.C")
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 0 and conn.inserts == []


def test_bis_credit_to_gdp_comes_from_bis_direct() -> None:
    from ingestion.international.bis import BIS_SERIES_LIST
    from ingestion.physical.dbnomics import DBNOMICS_SERIES

    assert BIS_SERIES_LIST["WS_TC/Q.US.P.A.M.770.A"] == "us_credit_gdp_bis"
    assert not set(BIS_SERIES_LIST.values()) & set(DBNOMICS_SERIES.values())


def test_bis_url_uses_latest_dataflow_version() -> None:
    from ingestion.international import sdmx_json

    assert sdmx_json.bis_series_url("WS_TC", "Q.US.P.A.M.770.A") == (
        "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_TC/~/Q.US.P.A.M.770.A"
    )
    assert "/2.0/" in sdmx_json.bis_series_url("WS_TC", "K", version="2.0")


# ---------------------------------------------------------------------------
# RBI / IMF
# ---------------------------------------------------------------------------


def test_rbi_skips_unchanged_observations(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.international import rbi

    conn = FakeConn({"india_repo_rate": {date(2026, 8, 1): 5.5, date(2026, 9, 1): 5.25}})
    puller = rbi.RBIPuller(_engine(conn))
    assert puller._insert_observations("india_repo_rate", [(date(2026, 8, 1), 5.5), (date(2026, 9, 1), 5.25)]) == 0
    assert puller._insert_observations("india_repo_rate", [(date(2026, 9, 1), 5.0)]) == 1
    assert puller._insert_observations("india_repo_rate", []) == 0
    assert [(r["od"], r["val"]) for r in conn.inserts] == [(date(2026, 9, 1), 5.0)]


def test_imf_ifs_skips_unchanged_observations(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.international import imf

    conn = FakeConn({"us_gdp_real_imf": {date(2026, 1, 1): 1.0}})
    puller = imf.IMFPuller(_engine(conn))
    monkeypatch.setattr(
        imf.sdmx_json, "fetch_imf_series",
        lambda flow, key, start=None: [(date(2026, 1, 1), 1.0), (date(2026, 4, 1), 2.0)],
    )
    res = puller.pull_ifs("us_gdp_real_imf")
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 1
    assert [(r["od"], r["val"]) for r in conn.inserts] == [(date(2026, 4, 1), 2.0)]


# ---------------------------------------------------------------------------
# DBnomics
# ---------------------------------------------------------------------------


def test_dbnomics_drops_retired_imf_ifs_mirror() -> None:
    from ingestion.physical.dbnomics import DBNOMICS_SERIES

    assert not any(k.startswith("IMF/IFS/") for k in DBNOMICS_SERIES)
    assert "us_mmkt_rate_imf" not in DBNOMICS_SERIES.values()
    assert set(DBNOMICS_SERIES.values()) == {"italy_10y_ecb_dbnomics", "france_10y_yield"}


# ---------------------------------------------------------------------------
# BCB
# ---------------------------------------------------------------------------


def test_bcb_replaces_mislabeled_7454_with_bop_trade_balance() -> None:
    from ingestion.international.bcb import BCB_SERIES

    assert 7454 not in BCB_SERIES
    assert "brazil_trade_balance" not in BCB_SERIES.values()  # old mislabeled rows stay isolated
    assert BCB_SERIES[22707] == "brazil_trade_balance_bop"


def test_bcb_writes_only_new_or_revised_values(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.international import bcb

    conn = FakeConn({"brazil_ipca_yoy": {date(2026, 8, 1): 4.6, date(2026, 9, 1): 4.58}})
    puller = bcb.BCBPuller(_engine(conn))
    monkeypatch.setattr(
        puller, "_fetch_series_data",
        lambda code, start, end: [
            {"data": "01/08/2026", "valor": "4.6"},
            {"data": "01/09/2026", "valor": "4.58"},
            {"data": "01/10/2026", "valor": "4.4"},
        ],
    )
    res = puller.pull_series(13522, "2025-09-01")
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 1
    assert [(r["od"], r["val"]) for r in conn.inserts] == [(date(2026, 10, 1), 4.4)]
    assert conn.reads[-1][1]["start_date"] == date(2025, 9, 1)


# ---------------------------------------------------------------------------
# OFR
# ---------------------------------------------------------------------------

FSI_CSV = (
    "Date,OFR FSI,Credit,Equity valuation,Safe assets,Funding,Volatility,United States,"
    "Other advanced economies,Emerging markets\n"
    "2026-01-02,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8\n"
    "2026-11-03,-1.484,-0.842,-0.458,-0.292,-0.129,0.237,-0.912,-0.093,-0.479\n"
    "2026-11-04,-2.162,-0.861,-0.479,-0.298,-0.123,,-1.109,-0.572,-0.481\n"
)


def _resp(text: str = "", payload: Any = None, status: int = 200) -> MagicMock:
    resp = MagicMock(status_code=status, text=text)
    resp.json.return_value = payload
    if status >= 400:
        err = requests.Response()
        err.status_code = status
        resp.raise_for_status.side_effect = requests.HTTPError(f"{status}", response=err)
    return resp


def test_ofr_fsi_writes_index_and_components_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.physical import ofr

    conn = FakeConn()
    puller = ofr.OFRPuller(_engine(conn))
    monkeypatch.setattr(ofr.requests, "get", MagicMock(return_value=_resp(FSI_CSV)))
    res = puller.pull_fsi()
    assert res["status"] == "SUCCESS"
    by_sid: dict[str, list[tuple[date, float]]] = {}
    for r in conn.inserts:
        by_sid.setdefault(r["sid"], []).append((r["od"], r["val"]))
    assert set(by_sid) == set(ofr.FSI_COLUMNS.values())
    assert by_sid["ofr_fsi"] == [(date(2026, 1, 2), 1.0), (date(2026, 11, 3), -1.484), (date(2026, 11, 4), -2.162)]
    assert len(by_sid["ofr_fsi_volatility"]) == 2  # blank cell dropped, never 0
    assert res["rows_inserted"] == sum(len(v) for v in by_sid.values())

    # Second run with everything stored: nothing re-inserted, and the
    # incremental window keeps the read near the latest stored date.
    conn.stored = {sid: dict(rows) for sid, rows in by_sid.items()}
    conn.inserts.clear()
    conn.reads.clear()
    res = puller.pull_fsi()
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 0 and conn.inserts == []
    assert all(p["start_date"] >= date(2026, 8, 1) for _sql, p in conn.reads)


def test_ofr_fsi_column_change_is_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.physical import ofr

    puller = ofr.OFRPuller(_engine(FakeConn()))
    monkeypatch.setattr(ofr.requests, "get", MagicMock(return_value=_resp("day,stress\n2026-11-04,1\n")))
    assert puller.pull_fsi()["status"] == "FAILED"


def test_ofr_stfm_uses_timeseries_api_and_drops_nulls(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.physical import ofr

    conn = FakeConn({"ofr_repo_rate_1d": {date(2026, 11, 3): 3.98}})
    puller = ofr.OFRPuller(_engine(conn))
    calls: list[dict[str, str]] = []

    def fake_get(url: str, params: dict[str, str] | None = None, timeout: int = 0) -> MagicMock:
        assert url == ofr._STFM_API_URL
        calls.append(dict(params or {}))
        return _resp(payload=[["2026-11-03", 3.98], ["2026-11-04", None], ["2026-11-05", 3.94]])

    monkeypatch.setattr(ofr.requests, "get", fake_get)
    res = puller.pull_stfm()
    assert res["status"] == "SUCCESS"
    assert {c["mnemonic"] for c in calls} == set(ofr.STFM_SERIES)
    rate_call = next(c for c in calls if c["mnemonic"] == "REPO-DVP_AR_OO-P")
    assert rate_call["start_date"] == "2026-10-04"  # latest stored minus lookback
    vol_call = next(c for c in calls if c["mnemonic"] == "REPO-DVP_TV_OO-P")
    assert "start_date" not in vol_call  # first load: full history
    got = {(r["sid"], r["od"]) for r in conn.inserts}
    assert ("ofr_repo_rate_1d", date(2026, 11, 3)) not in got
    assert ("ofr_repo_rate_1d", date(2026, 11, 5)) in got
    assert not any(r["od"] == date(2026, 11, 4) for r in conn.inserts)


def test_ofr_stfm_partial_and_failed_are_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.physical import ofr

    puller = ofr.OFRPuller(_engine(FakeConn()))
    errors: list[Any] = []
    monkeypatch.setattr(ofr.log, "error", lambda *a, **k: errors.append(a))

    def half(url: str, params: dict[str, str] | None = None, timeout: int = 0) -> MagicMock:
        if params and params["mnemonic"] == "REPO-DVP_TV_OO-P":
            return _resp(status=503)
        return _resp(payload=[["2026-11-05", 3.94]])

    monkeypatch.setattr(ofr.requests, "get", half)
    assert puller.pull_stfm()["status"] == "PARTIAL"
    monkeypatch.setattr(ofr.requests, "get", lambda *a, **k: _resp(status=503))
    assert puller.pull_stfm()["status"] == "FAILED"
    assert errors == []


def test_ofr_pull_all_no_longer_pulls_retired_fsm(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.physical import ofr

    puller = ofr.OFRPuller(_engine(FakeConn()))
    assert not hasattr(puller, "pull_fsm")
    monkeypatch.setattr(puller, "pull_fsi", lambda: {"status": "SUCCESS", "rows_inserted": 3})
    monkeypatch.setattr(puller, "pull_stfm", lambda: {"status": "SUCCESS", "rows_inserted": 2})
    out = puller.pull_all()
    assert out == {"source": "OFR", "total_rows": 5, "succeeded": 2, "total": 2}


def test_fsi_frame_parse_matches_live_header() -> None:
    from io import StringIO

    from ingestion.physical import ofr

    df = pd.read_csv(StringIO(FSI_CSV))
    assert all(c in df.columns for c in ofr.FSI_COLUMNS)
