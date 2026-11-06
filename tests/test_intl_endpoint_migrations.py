"""Endpoint-migration tests for international pullers (2026-11-06).

* OECD: stats.oecd.org/SDMX-JSON retired -> sdmx.oecd.org csvfile.

All HTTP is mocked; fixture rows are trimmed from live responses probed
2026-11-06.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
import requests


def _mock_engine(source_id: int = 7) -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = (source_id,)
    conn.execute.return_value.fetchall.return_value = []
    return engine, conn


def _http_error(status: int) -> requests.HTTPError:
    resp = requests.Response()
    resp.status_code = status
    return requests.HTTPError(f"{status} Client Error", response=resp)


def _inserts(conn: MagicMock) -> list[dict]:
    return [
        c.args[1] for c in conn.execute.call_args_list
        if c.args and "INSERT INTO raw_series" in str(c.args[0])
    ]


def _text_resp(body: str, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = body
    if status >= 400:
        resp.raise_for_status.side_effect = _http_error(status)
    else:
        resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# OECD
# ---------------------------------------------------------------------------

OECD_CLI_CSV = (
    "DATAFLOW,REF_AREA,FREQ,MEASURE,UNIT_MEASURE,ACTIVITY,ADJUSTMENT,TRANSFORMATION,"
    "TIME_HORIZ,METHODOLOGY,TIME_PERIOD,OBS_VALUE,OBS_STATUS,UNIT_MULT,DECIMALS,BASE_PER\n"
    "OECD.SDD.STES:DSD_STES@DF_CLI(4.1),USA,M,LI,IX,_Z,AA,IX,_Z,H,2026-07,100.10,A,0,2,\n"
    "OECD.SDD.STES:DSD_STES@DF_CLI(4.1),USA,M,LI,IX,_Z,AA,IX,_Z,H,2026-08,100.30,A,0,2,\n"
    "OECD.SDD.STES:DSD_STES@DF_CLI(4.1),USA,M,LI,IX,_Z,AA,IX,_Z,H,2026-09,100.70,A,0,2,\n"
    "OECD.SDD.STES:DSD_STES@DF_CLI(4.1),G7,M,LI,IX,_Z,AA,IX,_Z,H,2026-09,100.20,A,0,2,\n"
    "OECD.SDD.STES:DSD_STES@DF_CLI(4.1),G7,M,LI,IX,_Z,AA,IX,_Z,H,2026-10,,M,0,2,\n"
)
OECD_KEI_CSV = (
    "DATAFLOW,REF_AREA,FREQ,MEASURE,UNIT_MEASURE,ACTIVITY,ADJUSTMENT,TRANSFORMATION,"
    "TIME_PERIOD,OBS_VALUE,OBS_STATUS,UNIT_MULT,DECIMALS,BASE_PER\n"
    "OECD.SDD.STES:DSD_KEI@DF_KEI(4.0),USA,M,PRVM,IX,BTE,Y,_Z,2026-09,102.95,A,0,1,2015\n"
    "OECD.SDD.STES:DSD_KEI@DF_KEI(4.0),USA,M,PRVM,IX,C,Y,_Z,2026-09,97.8,A,0,1,2015\n"
    "OECD.SDD.STES:DSD_KEI@DF_KEI(4.0),USA,M,CP,GR,_Z,_Z,G1,2026-09,0.585,A,0,1,\n"
    "OECD.SDD.STES:DSD_KEI@DF_KEI(4.0),USA,M,CP,GR,_Z,_Z,GY,2026-09,3.4,A,0,1,\n"
    "OECD.SDD.STES:DSD_KEI@DF_KEI(4.0),USA,M,UNEMP,PT_LF,_T,Y,_Z,2026-09,4.2,A,0,1,\n"
    "OECD.SDD.STES:DSD_KEI@DF_KEI(4.0),USA,M,H_EARN,IX,C,Y,_Z,2026-09,151.8,A,0,1,2015\n"
    "OECD.SDD.STES:DSD_KEI@DF_KEI(4.0),DEU,M,PRVM,IX,BTE,Y,_Z,2026-08,87.66,A,0,1,2015\n"
)


class TestOECD:
    def _puller(self, monkeypatch):
        from ingestion.international import oecd

        engine, conn = _mock_engine()
        puller = oecd.OECDPuller(engine)
        puller._row_exists = lambda *a, **k: False
        monkeypatch.setattr(oecd.time, "sleep", lambda s: None)
        return oecd, puller, conn

    def test_no_retired_host_and_one_request_per_dataflow(self):
        from ingestion.international import oecd

        assert "stats.oecd.org" not in oecd._OECD_BASE_URL
        assert oecd.cli_url() == (
            "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,/"
            "USA+DEU+JPN+GBR+FRA+ITA+CAN+CHN+KOR+G7.M.LI...AA...H"
        )
        assert oecd.kei_url() == (
            "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI,/"
            "DEU+USA.M.CP+H_EARN+PRVM+UNEMP...."
        )
        # every pulled MEI series has a KEI key; retired ones are documented
        assert set(oecd.OECD_MEI_SERIES) == set(oecd.OECD_KEI_KEYS)
        assert "MEI.PRINTO01.CHN.ST.M" in oecd.OECD_MEI_RETIRED
        assert "OECD" in oecd.OECD_CLI_RETIRED and "OECD" not in oecd.OECD_CLI_COUNTRIES

    def test_parse_csv_drops_blank_values_and_builds_keys(self):
        from ingestion.international.oecd import _CLI_KEY_DIMS, parse_oecd_csv

        rows = parse_oecd_csv(OECD_CLI_CSV, _CLI_KEY_DIMS)
        assert len(rows) == 4  # the blank G7 2026-10 value is dropped, not 0
        assert rows[0].key == "USA.M.LI.IX._Z.AA.IX._Z.H"
        assert rows[0].obs_date == date(2026, 7, 1)

    def test_parse_csv_rejects_non_data_payload(self):
        from ingestion.international.oecd import _KEI_KEY_DIMS, parse_oecd_csv

        with pytest.raises(ValueError):
            parse_oecd_csv("<html>error</html>\n", _KEI_KEY_DIMS)

    def test_pull_all_keeps_legacy_series_ids(self, monkeypatch):
        oecd, puller, conn = self._puller(monkeypatch)
        calls = []

        def fake_get(url, params=None, **kw):
            calls.append((url, dict(params or {})))
            return _text_resp(OECD_CLI_CSV if "DF_CLI" in url else OECD_KEI_CSV)

        monkeypatch.setattr(oecd.requests, "get", fake_get)
        out = puller.pull_all("2026-08-15")

        assert len(calls) == 2  # rate-limit friendly: one call per dataflow
        assert calls[0][1] == {"format": "csvfile", "startPeriod": "2026-05"}  # slope lookback
        assert calls[1][1] == {"format": "csvfile", "startPeriod": "2026-08"}
        by_sid: dict[str, list] = {}
        for p in _inserts(conn):
            by_sid.setdefault(p["sid"], []).append((p["od"], p["val"]))
        assert by_sid["oecd_cli_usa"] == [
            (date(2026, 7, 1), 100.10), (date(2026, 8, 1), 100.30), (date(2026, 9, 1), 100.70),
        ]
        assert by_sid["oecd_cli_usa_slope"] == [(date(2026, 9, 1), pytest.approx(0.30))]
        assert by_sid["oecd_cli_g7"] == [(date(2026, 9, 1), 100.20)]
        assert by_sid["us_indpro_oecd"] == [(date(2026, 9, 1), 102.95)]  # BTE, not C
        assert by_sid["us_cpi_oecd"] == [(date(2026, 9, 1), 0.585)]       # G1, not GY
        assert by_sid["us_unemployment_oecd"] == [(date(2026, 9, 1), 4.2)]
        assert by_sid["us_unit_labor_cost"] == [(date(2026, 9, 1), 151.8)]
        assert by_sid["germany_indpro_oecd"] == [(date(2026, 8, 1), 87.66)]
        statuses = {r["series_id"]: r["status"] for r in out["results"]}
        assert statuses["oecd_cli_usa"] == "SUCCESS"
        assert statuses["oecd_cli_deu"] == "PARTIAL"  # absent from the fixture
        assert out["total_rows"] == sum(len(v) for v in by_sid.values())

    def test_no_records_found_is_empty_not_failure(self, monkeypatch):
        oecd, puller, conn = self._puller(monkeypatch)
        monkeypatch.setattr(
            oecd.requests, "get", lambda *a, **k: _text_resp("NoRecordsFound", status=404)
        )
        out = puller.pull_all("2030-01-01")
        assert all(r["status"] == "PARTIAL" for r in out["results"])
        assert _inserts(conn) == []

    def test_4xx_not_retried(self, monkeypatch):
        oecd, puller, _conn = self._puller(monkeypatch)
        get = MagicMock(return_value=_text_resp("Not enough key values", status=403))
        monkeypatch.setattr(oecd.requests, "get", get)
        out = puller.pull_all()
        assert get.call_count == 2  # one per dataflow, no retries
        assert all(r["status"] == "FAILED" for r in out["results"])

    def test_retired_series_are_skipped_not_fetched(self, monkeypatch):
        oecd, puller, _conn = self._puller(monkeypatch)
        get = MagicMock()
        monkeypatch.setattr(oecd.requests, "get", get)
        assert puller.pull_cli("OECD")["status"] == "SKIPPED"
        assert puller.pull_mei("MEI.PRINTO01.CHN.ST.M")["status"] == "SKIPPED"
        get.assert_not_called()


# ---------------------------------------------------------------------------
# ECB — empty-window 200 responses were filed as "upstream unavailable"
# ---------------------------------------------------------------------------

ECB_MONTHLY_JSON = {
    "dataSets": [{"series": {"0:0:0:0:0:0:0:0:0:0:0:0": {"observations": {
        "0": [3.1], "1": [3.4], "2": [None],
    }}}}],
    "structure": {"dimensions": {"observation": [{"id": "TIME_PERIOD", "values": [
        {"id": "2026-07"}, {"id": "2026-08"}, {"id": "2026-09"},
    ]}]}},
}


def _ecb_resp(body: bytes | dict, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    if isinstance(body, dict):
        resp.content = b"{...}"
        resp.json.return_value = body
    else:
        resp.content = body
        resp.json.side_effect = requests.exceptions.JSONDecodeError("Expecting value", "", 0)
    if status >= 400:
        resp.raise_for_status.side_effect = _http_error(status)
    else:
        resp.raise_for_status = MagicMock()
    return resp


class TestECB:
    def _puller(self, monkeypatch):
        from ingestion.international import ecb

        engine, conn = _mock_engine()
        puller = ecb.ECBPuller(engine)
        puller._row_exists = lambda *a, **k: False
        monkeypatch.setattr(ecb.time, "sleep", lambda s: None)
        return ecb, puller, conn

    def test_monthly_series_window_backs_off_past_publication_lag(self):
        from ingestion.international.ecb import _series_start_period

        assert _series_start_period("BSI.M.U2.Y.V.M30.X.I.U2.2300.Z01.A", "2026-10-30") == "2026-04"
        assert _series_start_period("EXR.D.USD.EUR.SP00.A", "2026-10-30") == "2026-10"
        assert _series_start_period("XYZ.Q.U2", "2026-02-01") == "2025-02"
        assert _series_start_period("EXR.D.USD.EUR.SP00.A", "1999-01-01") == "1999-01"

    def test_empty_200_body_is_unchanged_not_skipped(self, monkeypatch):
        ecb, puller, conn = self._puller(monkeypatch)
        get = MagicMock(return_value=_ecb_resp(b""))
        monkeypatch.setattr(ecb.requests, "get", get)
        res = puller.pull_series("BSI.M.U2.Y.V.M30.X.I.U2.2300.Z01.A", "2026-10-30")
        assert res["status"] == "UNCHANGED"
        assert get.call_count == 1  # no retry storm on an empty window
        assert get.call_args.kwargs["params"]["startPeriod"] == "2026-04"
        assert _inserts(conn) == []

    def test_monthly_series_inserts_after_backoff(self, monkeypatch):
        ecb, puller, conn = self._puller(monkeypatch)
        monkeypatch.setattr(ecb.requests, "get", MagicMock(return_value=_ecb_resp(ECB_MONTHLY_JSON)))
        res = puller.pull_series("BSI.M.U2.Y.V.M30.X.I.U2.2300.Z01.A", "2026-10-30")
        assert res["status"] == "SUCCESS" and res["rows_inserted"] == 2
        assert [(p["sid"], p["od"], p["val"]) for p in _inserts(conn)] == [
            ("ecb_m3_yoy", date(2026, 7, 1), 3.1), ("ecb_m3_yoy", date(2026, 8, 1), 3.4),
        ]

    def test_404_no_results_is_unchanged(self, monkeypatch):
        ecb, puller, _conn = self._puller(monkeypatch)
        monkeypatch.setattr(ecb.requests, "get", MagicMock(return_value=_ecb_resp(b"No results found", 404)))
        assert puller.pull_series("EXR.D.USD.EUR.SP00.A", "2026-11-01")["status"] == "UNCHANGED"

    def test_4xx_is_not_retried(self, monkeypatch):
        ecb, puller, _conn = self._puller(monkeypatch)
        get = MagicMock(return_value=_ecb_resp(b"bad key", 400))
        monkeypatch.setattr(ecb.requests, "get", get)
        res = puller.pull_series("EXR.D.USD.EUR.SP00.A", "2026-11-01")
        assert get.call_count == 1
        assert res["status"] == "SKIPPED"  # existing soft-skip policy for HTTP errors

    def test_pull_all_counts_unchanged_as_checked(self, monkeypatch):
        from ingestion.smart_scheduler import _classify_outcome

        ecb, puller, _conn = self._puller(monkeypatch)
        monkeypatch.setattr(ecb.requests, "get", MagicMock(return_value=_ecb_resp(b"")))
        puller._compute_btp_bund_spread = lambda: {"rows_inserted": 0, "status": "SUCCESS", "errors": []}
        out = puller.pull_all("2026-10-30")
        assert out["succeeded"] == out["total"]
        outcome, rows, _note = _classify_outcome(out)
        assert outcome == "NO_NEW_DATA" and rows == 0
