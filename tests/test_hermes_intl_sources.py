"""Regression tests for the 2026-11-04 Hermes log-hygiene wave, workstream D.

Every test pins one root cause seen in the grid-hermes 24h journal (or one
source in the "35 sources past freshness window" alert). No network: every
HTTP call and DB connection is mocked.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import httpx
import pandas as pd
import pytest
import requests

from ingestion.international import sdmx_json


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


# Shapes copied (trimmed) from live responses probed on grid-svr 2026-11-04.
BIS_PAYLOAD = {
    "meta": {"id": "x"},
    "data": {
        "dataSets": [{"series": {"0:0:0:0:0": {"observations": {
            "0": ["-11.0108", 0, 0, None], "1": ["-11.2587", 0, 0, None], "2": [None, 0, 0, None],
        }}}}],
        "structure": {"dimensions": {
            "series": [
                {"id": "FREQ", "values": [{"id": "Q"}]},
                {"id": "BORROWERS_CTY", "values": [{"id": "US"}]},
                {"id": "TC_BORROWERS", "values": [{"id": "P"}]},
                {"id": "TC_LENDERS", "values": [{"id": "A"}]},
                {"id": "CG_DTYPE", "values": [{"id": "C"}]},
            ],
            "observation": [{"id": "TIME_PERIOD", "role": "time", "values": [
                {"id": "2024-Q1"}, {"id": "2024-Q2"}, {"id": "2024-Q3"},
            ]}],
        }},
    },
}

IMF_PAYLOAD = {
    "meta": {},
    "data": {
        "dataSets": [{"structure": 0, "series": {"0:0:0:0": {"observations": {
            "0": ["5.871212121212102", None, 0, None], "1": ["6.219122348154576", None, 0, None],
        }}}}],
        "structures": [{"dimensions": {
            "series": [
                {"id": "COUNTRY", "values": [{"id": "IND"}]},
                {"id": "PRODUCTION_INDEX", "values": [{"id": "IND"}]},
                {"id": "TYPE_OF_TRANSFORMATION", "values": [{"id": "YOY_PCH_PT"}]},
                {"id": "FREQUENCY", "values": [{"id": "Q"}]},
            ],
            "observation": [{"id": "TIME_PERIOD", "values": [{"value": "2026-Q1"}, {"value": "2026-Q2"}]}],
        }}],
    },
}


# ---------------------------------------------------------------------------
# Shared SDMX-JSON helper (BIS + IMF)
# ---------------------------------------------------------------------------


class TestSdmxJson:
    def test_parses_bis_sdmx_json_1(self):
        out = sdmx_json.parse_sdmx_json(BIS_PAYLOAD)
        assert out == {"Q.US.P.A.C": [(date(2024, 1, 1), -11.0108), (date(2024, 4, 1), -11.2587)]}

    def test_parses_imf_sdmx_json_2(self):
        out = sdmx_json.parse_sdmx_json(IMF_PAYLOAD)
        assert out["IND.IND.YOY_PCH_PT.Q"][-1] == (date(2026, 4, 1), 6.219122348154576)

    def test_error_payload_raises_instead_of_silent_empty(self):
        with pytest.raises(ValueError):
            sdmx_json.parse_sdmx_json({"status": 400, "message": "bad key"})
        with pytest.raises(ValueError):
            sdmx_json.parse_sdmx_json("<html>maintenance</html>")  # type: ignore[arg-type]

    @pytest.mark.parametrize("code,expected", [
        ("2026", date(2026, 1, 1)), ("2026-Q3", date(2026, 7, 1)), ("2026Q4", date(2026, 10, 1)),
        ("2026-M08", date(2026, 8, 1)), ("2026-08", date(2026, 8, 1)), ("2026-08-15", date(2026, 8, 15)),
        ("2026-S2", date(2026, 7, 1)), ("garbage", None), ("2026-Q9", None),
    ])
    def test_parse_period(self, code, expected):
        assert sdmx_json.parse_period(code) == expected

    def test_lookback_start_year(self):
        assert sdmx_json.lookback_start_year("2026-10-27") == "2024"
        assert sdmx_json.lookback_start_year(date(2000, 1, 1)) == "1998"
        assert sdmx_json.lookback_start_year(None) is None

    @pytest.mark.parametrize("start,key,expected", [
        ("2024", "USA.B1GQ.Q.SA.XDC.Q", "2024-Q1"),
        ("2024", "SGP.TRGMV_REVS.USD.M", "2024-M01"),
        ("2024", "USA.NGDP_RPCH.A", "2024-A1"),
        ("2024-Q3", "USA.B1GQ.Q.SA.XDC.Q", "2024-Q3"),
        ("2024", "USA.X.Q+M", "2024"),
        (None, "USA.B1GQ.Q.SA.XDC.Q", None),
    ])
    def test_imf_period_bound_matches_series_frequency(self, start, key, expected):
        """IMF ignores ge:YYYY and returns full history (730 dup rows per run)."""
        assert sdmx_json.imf_period_bound(start, key) == expected

    def test_imf_fetch_sends_frequency_correct_time_filter(self):
        resp = MagicMock()
        resp.json.return_value = IMF_PAYLOAD
        with patch.object(sdmx_json.requests, "get", return_value=resp) as get:
            obs = sdmx_json.fetch_imf_series("QNEA", "USA.B1GQ.Q.SA.XDC.Q", "2024")
        assert get.call_args.kwargs["params"] == {"c[TIME_PERIOD]": "ge:2024-Q1"}
        assert get.call_args.args[0].endswith("/IMF.STA/QNEA/+/USA.B1GQ.Q.SA.XDC.Q")
        assert len(obs) == 2

    def test_bis_fetch_sends_sdmx_accept_header(self):
        resp = MagicMock()
        resp.json.return_value = BIS_PAYLOAD
        with patch.object(sdmx_json.requests, "get", return_value=resp) as get:
            obs = sdmx_json.fetch_bis_series("WS_CREDIT_GAP", "Q.US.P.A.C", "2024")
        assert obs[0] == (date(2024, 1, 1), -11.0108)
        url = get.call_args.args[0]
        assert url == "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CREDIT_GAP/~/Q.US.P.A.C"  # latest version (2026-11-09)
        assert get.call_args.kwargs["headers"]["Accept"] == sdmx_json.BIS_ACCEPT
        assert get.call_args.kwargs["params"] == {"startPeriod": "2024"}


# ---------------------------------------------------------------------------
# BIS — invalid query paths answered 400, retried, logged as ERROR (40/day)
# ---------------------------------------------------------------------------


class TestBIS:
    def _puller(self):
        from ingestion.international.bis import BISPuller

        engine, conn = _mock_engine()
        puller = BISPuller(engine)
        puller._row_exists = lambda *a, **k: False
        return puller, conn

    def test_series_are_real_bis_dataflow_keys(self):
        from ingestion.international.bis import BIS_SERIES_LIST, _split_ref

        for ref in BIS_SERIES_LIST:
            flow, key = _split_ref(ref)
            assert flow.startswith("WS_") and "/" not in key

    def test_pull_series_inserts_parsed_observations(self, monkeypatch):
        from ingestion.international import bis

        puller, conn = self._puller()
        monkeypatch.setattr(bis.time, "sleep", lambda s: None)
        calls = []

        def fake(flow, key, start):
            calls.append((flow, key, start))
            return [(date(2024, 1, 1), -11.0), (date(2024, 4, 1), -11.2)]

        monkeypatch.setattr(bis.sdmx_json, "fetch_bis_series", fake)
        res = puller.pull_series("WS_CREDIT_GAP/Q.US.P.A.C", "1970-01-01")
        assert res["status"] == "SUCCESS" and res["rows_inserted"] == 2
        assert calls == [("WS_CREDIT_GAP", "Q.US.P.A.C", "1968")]
        assert {p["sid"] for p in _inserts(conn)} == {"bis_credit_gdp_gap_us"}

    def test_4xx_is_not_retried_and_logs_warning_not_error(self, monkeypatch):
        from ingestion.international import bis

        puller, _conn = self._puller()
        monkeypatch.setattr(bis.time, "sleep", lambda s: None)
        fetch = MagicMock(side_effect=_http_error(400))
        monkeypatch.setattr(bis.sdmx_json, "fetch_bis_series", fetch)
        errors = []
        monkeypatch.setattr(bis.log, "error", lambda *a, **k: errors.append(a))
        res = puller.pull_series("WS_SPP/Q.XW.R.628")
        assert res["status"] == "FAILED"
        assert fetch.call_count == 1
        assert errors == []


# ---------------------------------------------------------------------------
# RBI — dbie.rbi.org.in serves a cert for data.rbi.org.in only (SSLError, 24/day)
# ---------------------------------------------------------------------------


class TestRBI:
    def test_no_series_targets_the_mismatched_dbie_host(self):
        import inspect

        from ingestion.international import rbi

        assert "dbie.rbi.org.in/DBIE" not in inspect.getsource(rbi.RBIPuller)
        assert "verify=False" not in inspect.getsource(rbi)
        assert {c["provider"] for c in rbi.RBI_SERIES.values()} <= {"bis", "imf"}

    def test_pull_all_routes_each_series_to_its_provider(self, monkeypatch):
        from ingestion.international import rbi

        engine, conn = _mock_engine()
        puller = rbi.RBIPuller(engine)
        puller._row_exists = lambda *a, **k: False
        monkeypatch.setattr(rbi.time, "sleep", lambda s: None)
        seen = []

        def fake(provider):
            def _f(flow, key, start):
                seen.append((provider, flow, key))
                return [(date(2026, 7, 1), 1.0)]
            return _f

        monkeypatch.setitem(rbi._FETCHERS, "bis", fake("bis"))
        monkeypatch.setitem(rbi._FETCHERS, "imf", fake("imf"))
        out = puller.pull_all(start_date="2026-10-27")
        assert out["succeeded"] == out["total"] == 3
        assert ("bis", "WS_CBPOL", "M.IN") in seen
        assert ("imf", "IL", "IND.TRGMV_REVS.USD.M") in seen
        assert {p["sid"] for p in _inserts(conn)} == {"india_repo_rate", "india_iip_yoy", "india_fx_reserves"}

    def test_ssl_failure_is_a_warning_and_not_retried_forever(self, monkeypatch):
        from ingestion.international import rbi

        engine, _conn = _mock_engine()
        puller = rbi.RBIPuller(engine)
        monkeypatch.setattr(rbi.time, "sleep", lambda s: None)
        monkeypatch.setattr(rbi.RBIPuller._fetch_series.retry, "sleep", lambda s: None)
        boom = MagicMock(side_effect=requests.exceptions.SSLError("hostname mismatch"))
        monkeypatch.setitem(rbi._FETCHERS, "bis", boom)
        monkeypatch.setitem(rbi._FETCHERS, "imf", boom)
        errors = []
        monkeypatch.setattr(rbi.log, "error", lambda *a, **k: errors.append(a))
        out = puller.pull_all()
        assert out["succeeded"] == 0
        assert errors == []


# ---------------------------------------------------------------------------
# IMF — imfdatapy targeted the retired dataservices.imf.org host (10/day)
# ---------------------------------------------------------------------------


class TestIMF:
    def test_pull_ifs_uses_sdmx_3_keys(self, monkeypatch):
        from ingestion.international import imf

        engine, conn = _mock_engine()
        puller = imf.IMFPuller(engine)
        puller._row_exists = lambda *a, **k: False
        monkeypatch.setattr(imf.time, "sleep", lambda s: None)
        calls = []

        def fake(flow, key, start=None):
            calls.append((flow, key))
            return [(date(2026, 1, 1), 1.0), (date(2026, 4, 1), 2.0)]

        monkeypatch.setattr(imf.sdmx_json, "fetch_imf_series", fake)
        res = puller.pull_ifs("us_gdp_real_imf")
        assert res["status"] == "SUCCESS" and res["rows_inserted"] == 2
        assert calls == [("QNEA", "USA.B1GQ.Q.SA.XDC.Q")]

    def test_every_former_ifs_target_has_a_flow_and_key(self):
        from ingestion.international import imf

        assert set(imf.IMF_IFS_SERIES) == {
            "us_gdp_real_imf", "china_gdp_real_imf", "germany_gdp_real_imf",
            "us_current_account_imf", "china_current_account_imf",
        }
        assert all(flow in {"QNEA", "BOP"} for flow, _ in imf.IMF_IFS_SERIES.values())


# ---------------------------------------------------------------------------
# MAS — legacy datastore API retired (HTML maintenance page -> JSONDecodeError)
# ---------------------------------------------------------------------------


class TestMAS:
    def test_pull_all_pulls_reserves_from_imf_and_reports_rest_once(self, monkeypatch):
        from ingestion.international import mas

        engine, conn = _mock_engine()
        puller = mas.MASPuller(engine)
        puller._row_exists = lambda *a, **k: False
        monkeypatch.setattr(mas.time, "sleep", lambda s: None)
        monkeypatch.setattr(mas, "_UNAVAILABLE_LOGGED", False)
        monkeypatch.setattr(mas.sdmx_json, "fetch_imf_series", lambda f, k, s=None: [(date(2026, 8, 1), 4.5e11)])
        infos = []
        monkeypatch.setattr(mas.log, "info", lambda msg, **kw: infos.append(msg))
        out1 = puller.pull_all()
        out2 = puller.pull_all()
        assert out1["succeeded"] == out1["total"] == 1
        assert out2["total"] == 1
        assert sum("not pulling" in m for m in infos) == 1
        assert {p["sid"] for p in _inserts(conn)} == {"singapore_fx_reserves"}


# ---------------------------------------------------------------------------
# BCB — SGS caps daily-series queries at 10 years (HTTP 406 for codes 1, 11)
# ---------------------------------------------------------------------------


class TestBCB:
    def test_date_windows_never_exceed_ten_years(self):
        from ingestion.international.bcb import _date_windows

        wins = _date_windows(date(1994, 1, 1), date(2026, 11, 4))
        assert wins[0][0] == date(1994, 1, 1) and wins[-1][1] == date(2026, 11, 4)
        for (s, e), (s2, _e2) in zip(wins, wins[1:]):
            assert s2 == e + timedelta(days=1)
        assert all(e < s.replace(year=s.year + 10) for s, e in wins)

    def test_daily_series_is_fetched_in_windows(self, monkeypatch):
        from ingestion.international import bcb

        engine, _conn = _mock_engine()
        puller = bcb.BCBPuller(engine)
        monkeypatch.setattr(bcb.time, "sleep", lambda s: None)
        calls = []

        def fake(code, start, end):
            calls.append((code, start, end))
            return [{"data": "03/01/1994", "valor": "1.0"}]

        monkeypatch.setattr(puller, "_fetch_series_data", fake)
        res = puller.pull_series(11, "1994-01-01", "2026-11-04")
        assert res["status"] == "SUCCESS"
        assert len(calls) == 4 and all(c[0] == 11 for c in calls)
        monthly_calls = []
        monkeypatch.setattr(puller, "_fetch_series_data", lambda c, s, e: monthly_calls.append(c) or [])
        puller.pull_series(13522, "1994-01-01")
        assert monthly_calls == [13522]  # monthly series: one request

    def test_406_is_not_retried(self, monkeypatch):
        from ingestion.international import bcb

        engine, _conn = _mock_engine()
        puller = bcb.BCBPuller(engine)
        get = MagicMock()
        get.return_value.raise_for_status.side_effect = _http_error(406)
        monkeypatch.setattr(bcb.requests, "get", get)
        with pytest.raises(requests.HTTPError):
            puller._fetch_series_data(11, "1994-01-01", None)
        assert get.call_count == 1

    def test_empty_window_404_is_no_new_data(self, monkeypatch):
        # Hermes' repair pulls from a recent start; a monthly series has no
        # observation in that window and SGS answers 404 "Value(s) not found".
        from ingestion.international import bcb

        engine, _conn = _mock_engine()
        puller = bcb.BCBPuller(engine)
        monkeypatch.setattr(bcb.time, "sleep", lambda s: None)
        resp = MagicMock(status_code=404, text='{"erro":{"statusCode":404,"detail":'
                         '"br.gov.bcb.pec.sgs.comum.excecoes.SGSNegocioException: Value(s) not found"}}')
        get = MagicMock(return_value=resp)
        monkeypatch.setattr(bcb.requests, "get", get)
        res = puller.pull_series(13522, "2026-10-17")
        assert res["status"] == "SUCCESS" and res["rows_inserted"] == 0 and res["errors"] == []
        assert get.call_count == 1
        resp.raise_for_status.assert_not_called()

    def test_other_404_still_fails(self, monkeypatch):
        from ingestion.international import bcb

        engine, _conn = _mock_engine()
        puller = bcb.BCBPuller(engine)
        monkeypatch.setattr(bcb.time, "sleep", lambda s: None)
        resp = MagicMock(status_code=404, text="<html>Not Found</html>")
        resp.raise_for_status.side_effect = _http_error(404)
        monkeypatch.setattr(bcb.requests, "get", MagicMock(return_value=resp))
        res = puller.pull_series(99999, "2026-10-17")
        assert res["status"] == "FAILED"

    def test_incremental_start_uses_latest_obs_minus_lookback(self, monkeypatch):
        from ingestion.international import bcb

        engine, _conn = _mock_engine()
        puller = bcb.BCBPuller(engine)
        monkeypatch.setattr(puller, "_get_latest_date", lambda sid: date(2026, 10, 30))
        assert puller._incremental_start(11, "1994-01-01") == (
            date(2026, 10, 30) - timedelta(days=bcb._REVISION_LOOKBACK_DAYS)
        ).isoformat()
        monkeypatch.setattr(puller, "_get_latest_date", lambda sid: None)
        assert puller._incremental_start(11, "1994-01-01") == "1994-01-01"


# ---------------------------------------------------------------------------
# AKShare — wrong value column, unparsed month labels, dead jin10 tables
# ---------------------------------------------------------------------------


class TestAKShare:
    @pytest.mark.parametrize("raw,expected", [
        ("2026年09月份", date(2026, 9, 1)),
        ("2026年第1-3季度", date(2026, 7, 1)),
        ("2026年第2季度", date(2026, 4, 1)),
        ("201501", date(2015, 1, 1)),
        (date(2025, 12, 1), date(2025, 12, 1)),
        (datetime(2025, 12, 1, 9, 30), date(2025, 12, 1)),
        ("--", None),
    ])
    def test_parse_akshare_period(self, raw, expected):
        from ingestion.international.akshare_macro import parse_akshare_period

        assert parse_akshare_period(raw) == expected

    def test_m2_uses_growth_column_not_level(self, monkeypatch):
        from ingestion.international import akshare_macro as akm

        monkeypatch.setattr(akm, "_AKSHARE_AVAILABLE", True)
        engine, conn = _mock_engine()
        puller = akm.AKShareMacroPuller(engine)
        puller._row_exists = lambda *a, **k: False
        df = pd.DataFrame({
            "月份": ["2026年09月份", "2026年08月份"],
            "货币和准货币(M2)-数量(亿元)": [3596289.7, 3570000.0],
            "货币和准货币(M2)-同比增长": [7.2, 7.1],
        })
        warnings = []
        monkeypatch.setattr(akm.log, "warning", lambda *a, **k: warnings.append(a))
        with patch.dict("sys.modules", {"akshare": MagicMock()}):
            res = puller.pull_series(
                "macro_china_money_supply", "china_m2_yoy",
                value_column="货币和准货币(M2)-同比增长", frame=df,
            )
        assert res["status"] == "SUCCESS" and res["rows_inserted"] == 2
        assert sorted(p["val"] for p in _inserts(conn)) == [7.1, 7.2]
        assert {p["od"] for p in _inserts(conn)} == {date(2026, 9, 1), date(2026, 8, 1)}
        assert warnings == []

    def test_missing_named_column_fails_instead_of_guessing(self, monkeypatch):
        from ingestion.international import akshare_macro as akm

        monkeypatch.setattr(akm, "_AKSHARE_AVAILABLE", True)
        engine, conn = _mock_engine()
        puller = akm.AKShareMacroPuller(engine)
        df = pd.DataFrame({"月份": ["2026年09月份"], "其它": [1.0]})
        with patch.dict("sys.modules", {"akshare": MagicMock()}):
            res = puller.pull_series("macro_china_pmi", "china_pmi_mfg", value_column="制造业-指数", frame=df)
        assert res["status"] == "FAILED"
        assert "制造业-指数" in res["errors"][0]
        assert _inserts(conn) == []

    def test_pull_plan_has_no_dead_functions_and_shares_hgjck(self):
        from ingestion.international.akshare_macro import AKSHARE_PULLS, AKSHARE_SERIES

        funcs = [f for f, _, _ in AKSHARE_PULLS]
        assert "macro_china_fx_reserves" not in funcs  # removed from akshare
        assert not any(f.endswith("_yearly") for f in funcs)  # jin10, frozen 2025-09
        assert funcs.count("macro_china_hgjck") == 2
        assert len({feat for _, feat, _ in AKSHARE_PULLS}) == len(AKSHARE_PULLS)
        assert "macro_china_money_supply" in AKSHARE_SERIES


# ---------------------------------------------------------------------------
# EDGAR — "grid@localhost" UA drew 403 on www.sec.gov/Archives
# ---------------------------------------------------------------------------


class TestEdgarIdentity:
    def test_localhost_identity_is_replaced(self):
        from ingestion import edgar

        resolved = edgar.resolve_sec_identity("GRID Trading System grid@localhost")
        assert "@localhost" not in resolved and "@" in resolved

    def test_explicit_valid_identity_wins(self):
        from ingestion import edgar

        assert edgar.resolve_sec_identity("GRID Research ops@example.org") == "GRID Research ops@example.org"

    def test_config_identity_used_when_no_explicit(self, monkeypatch):
        from config import settings
        from ingestion import edgar

        monkeypatch.setattr(settings, "SEC_USER_AGENT", "GRID research someone@example.com", raising=False)
        assert edgar.resolve_sec_identity(None) == "GRID research someone@example.com"
        monkeypatch.setattr(settings, "SEC_USER_AGENT", "", raising=False)
        assert edgar.resolve_sec_identity(None) == edgar.DEFAULT_SEC_IDENTITY

    def test_puller_sets_resolved_identity(self, monkeypatch):
        from ingestion import edgar

        seen = []
        monkeypatch.setattr(edgar, "set_identity", seen.append)
        engine, _conn = _mock_engine()
        edgar.EDGARPuller(engine)
        assert seen and "@localhost" not in seen[0] and "@" in seen[0]


# ---------------------------------------------------------------------------
# Solana — token.jup.ag retired (NXDOMAIN): universe always empty
# ---------------------------------------------------------------------------


class TestSolanaJupiter:
    def _resp(self, data):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.json.return_value = data
        resp.raise_for_status.return_value = None
        return resp

    def test_retired_url_from_config_is_remapped(self):
        from ingestion.solana.top_volume import _JUPITER_VERIFIED_URL, JupiterDexScreenerProvider

        p = JupiterDexScreenerProvider(jupiter_tokens_url="https://token.jup.ag/strict",
                                       client=MagicMock(spec=httpx.Client))
        assert p._jupiter_url == _JUPITER_VERIFIED_URL
        assert "lite-api.jup.ag/tokens/v2" in _JUPITER_VERIFIED_URL

    def test_v2_id_field_and_volume_cap(self, monkeypatch):
        from ingestion.solana import top_volume as tv

        monkeypatch.setattr(tv, "_MAX_CANDIDATES", 2)
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = self._resp([
            {"id": "LOW", "symbol": "L", "stats24h": {"buyVolume": 1.0, "sellVolume": 1.0}},
            {"id": "HIGH", "symbol": "H", "stats24h": {"buyVolume": 900.0, "sellVolume": 100.0}},
            {"id": "MID", "symbol": "M", "stats24h": {"buyVolume": 50.0, "sellVolume": 50.0}},
            {"symbol": "NOMINT"},
        ])
        p = tv.JupiterDexScreenerProvider(client=client, request_delay=0)
        universe = p._fetch_jupiter_universe()
        assert [t["address"] for t in universe] == ["HIGH", "MID"]


# ---------------------------------------------------------------------------
# GDELT — 10 s read timeout on a host that takes 10-13 s to say 429;
# hermes_fixers retried a pull_all() that did not exist
# ---------------------------------------------------------------------------


class TestGDELT:
    def _puller(self):
        from ingestion.altdata.gdelt import GDELTPuller

        engine, conn = _mock_engine()
        with patch("ingestion.altdata.gdelt.os.makedirs"):
            return GDELTPuller(engine), conn

    def test_pull_all_alias_uses_bounded_defaults(self):
        p, _ = self._puller()
        with patch.object(p, "pull_recent", return_value={"total_rows": 0}) as pr:
            p.pull_all()
        pr.assert_called_once_with(
            days_back=1, max_theme_queries=4, include_actor_tones=False,
            include_tensions=False, include_signals=False,
        )

    def test_timeout_stops_remaining_theme_queries(self, monkeypatch):
        from ingestion.altdata import gdelt

        p, _ = self._puller()
        monkeypatch.setattr(gdelt.time, "sleep", lambda s: None)
        fetch = MagicMock(side_effect=requests.ReadTimeout("Read timed out. (read timeout=30)"))
        monkeypatch.setattr(p, "_fetch_gdelt_api", fetch)
        warnings = []
        monkeypatch.setattr(gdelt.log, "warning", lambda *a, **k: warnings.append(a))
        res = p.pull_recent(days_back=1, max_theme_queries=4, include_actor_tones=False,
                            include_tensions=False, include_signals=False)
        assert fetch.call_count == 1
        assert len(warnings) == 1
        assert res["total_rows"] == 0 and len(res["errors"]) == 1

    def test_429_stops_remaining_theme_queries_and_is_recorded(self, monkeypatch):
        from ingestion.altdata import gdelt

        p, _ = self._puller()
        monkeypatch.setattr(gdelt.time, "sleep", lambda s: None)
        fetch = MagicMock(return_value={"timeline": [], "status": "SKIPPED", "http_status": 429})
        monkeypatch.setattr(p, "_fetch_gdelt_api", fetch)
        res = p.pull_recent(days_back=1, max_theme_queries=4, include_actor_tones=False,
                            include_tensions=False, include_signals=False)
        assert fetch.call_count == 1
        assert "429" in res["errors"][0]

    def test_doc_api_read_timeout_is_longer_than_gdelts_429_latency(self, monkeypatch):
        from ingestion.altdata import gdelt

        p, _ = self._puller()
        monkeypatch.setattr(p, "_acquire_doc_token", lambda: 0.0)
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"timeline": []}
        with patch.object(gdelt.requests, "get", return_value=resp) as get:
            p._fetch_gdelt_api("q", "timelinetone", "1d")
        connect, read = get.call_args.kwargs["timeout"]
        assert read >= 25


# ---------------------------------------------------------------------------
# JODI — old CSV + SDMX URLs 404; current per-year files use new codes
# ---------------------------------------------------------------------------


class TestJODI:
    CSV = (
        "REF_AREA,TIME_PERIOD,ENERGY_PRODUCT,FLOW_BREAKDOWN,UNIT_MEASURE,OBS_VALUE,ASSESSMENT_CODE\n"
        "SA,2026-07,CRUDEOIL,INDPROD,CONVBBL,7323.0000,3\n"
        "SA,2026-07,CRUDEOIL,INDPROD,KBBL,252188.0000,1\n"
        "SA,2026-07,CRUDEOIL,INDPROD,KBD,8135.0968,1\n"
        "SA,2026-07,CRUDEOIL,CLOSTLV,KBBL,175632.0000,3\n"
        "SA,2026-07,CRUDEOIL,CLOSTLV,KBD,x,3\n"
        "AE,2026-07,CRUDEOIL,INDPROD,KBD,-,3\n"
        "SA,2026-07,GASDIES,REFGROUT,KBD,1200.0,1\n"
        "SA,2026-07,GASDIES,TOTEXPSB,KBD,300.0,1\n"
        "ZA,2026-07,CRUDEOIL,INDPROD,KBD,1.0,1\n"
    )

    def test_current_release_codes_are_normalised(self):
        from ingestion.altdata.jodi_oil import _parse_jodi_csv

        obs = {(o.country, o.product, o.flow, o.unit): o.value for o in _parse_jodi_csv(self.CSV)}
        assert obs == {
            ("SAU", "CRUDEOIL", "PRODUCTION", "KBD"): 8135.0968,
            ("SAU", "CRUDEOIL", "CLOSSTLV", "KBBL"): 175632.0,
            ("SAU", "GASDIESEL", "PRODUCTION", "KBD"): 1200.0,
            ("SAU", "GASDIESEL", "EXPORTS", "KBD"): 300.0,
        }

    def test_csv_urls_are_per_year_files(self):
        from ingestion.altdata.jodi_oil import jodi_csv_urls

        nov = jodi_csv_urls(date(2026, 11, 4))
        assert nov == [
            "https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/primary/primaryyear2026.csv",
            "https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/secondary/secondaryyear2026.csv",
        ]
        assert len(jodi_csv_urls(date(2027, 2, 1))) == 4  # Q1 also reads the closed year

    def test_save_bumps_last_pull_at(self):
        from ingestion.altdata.jodi_oil import JODIObservation, JODIOilPuller

        engine, conn = _mock_engine(source_id=3473)
        puller = JODIOilPuller(engine)
        puller.save_to_db([JODIObservation(date(2026, 7, 1), "SAU", "CRUDEOIL", "PRODUCTION", 1.0, "KBD", "1")])
        sql = [str(c.args[0]) for c in conn.execute.call_args_list if c.args]
        assert any("UPDATE source_catalog SET last_pull_at" in s for s in sql)


# ---------------------------------------------------------------------------
# ecb_ilm_ltro — weekly rows landed but last_pull_at stayed NULL (stale alert)
# ---------------------------------------------------------------------------


class TestECBLtroFreshness:
    def test_successful_pull_bumps_last_pull_at(self, monkeypatch):
        from ingestion.altdata import ecb_ltro

        engine, conn = _mock_engine(source_id=4908)
        puller = ecb_ltro.ECBLtroPuller(engine, session=MagicMock())
        monkeypatch.setattr(puller, "_get_latest_date", lambda sid: date(2026, 10, 18))
        monkeypatch.setattr(puller, "fetch", lambda incremental: [(date(2026, 10, 25), 12.3, "2026-W43")])
        res = puller.pull()
        assert res["status"] == "SUCCESS"
        updates = [c for c in conn.execute.call_args_list
                   if c.args and "UPDATE source_catalog SET last_pull_at" in str(c.args[0])]
        assert len(updates) == 1 and updates[0].args[1] == {"sid": 4908}


class TestFreshnessBumpOnlyOnNewRows:
    def test_ecb_ltro_zero_new_rows_does_not_bump(self, monkeypatch):
        from ingestion.altdata import ecb_ltro

        engine, conn = _mock_engine(source_id=4908)
        conn.execute.return_value.fetchall.return_value = [(date(2026, 10, 25),)]
        puller = ecb_ltro.ECBLtroPuller(engine, session=MagicMock())
        monkeypatch.setattr(puller, "_get_latest_date", lambda sid: date(2026, 10, 25))
        monkeypatch.setattr(puller, "fetch", lambda incremental: [(date(2026, 10, 25), 12.3, "2026-W43")])
        res = puller.pull()
        assert res["status"] == "SUCCESS" and res["rows_inserted"] == 0
        assert not any("UPDATE source_catalog" in str(c.args[0]) for c in conn.execute.call_args_list if c.args)


# ---------------------------------------------------------------------------
# CEPII BACI — never provisioned: 30 "not found locally" warnings + FAILED per run
# ---------------------------------------------------------------------------


class TestCEPII:
    def test_unprovisioned_dataset_is_one_skip(self, monkeypatch):
        from ingestion.trade import cepii

        monkeypatch.setattr(cepii.os, "makedirs", lambda *a, **k: None)
        monkeypatch.setattr(cepii.os.path, "exists", lambda p: False)
        engine, _conn = _mock_engine(source_id=967)
        puller = cepii.CEPIIPuller(engine)
        warnings = []
        monkeypatch.setattr(cepii.log, "warning", lambda *a, **k: warnings.append(a))
        out = puller.pull_all()
        assert out["status"] == "SKIPPED"
        assert "BACI" in out["skipped_reason"]
        assert len(warnings) == 1

    def test_only_provisioned_years_are_pulled(self, monkeypatch):
        from ingestion.trade import cepii

        monkeypatch.setattr(cepii.os, "makedirs", lambda *a, **k: None)
        monkeypatch.setattr(cepii.os.path, "exists", lambda p: p.endswith("BACI_HS17_Y2022_V202401.csv"))
        engine, _conn = _mock_engine(source_id=967)
        puller = cepii.CEPIIPuller(engine)
        pulled = []
        monkeypatch.setattr(puller, "pull_year", lambda y: pulled.append(y) or {"status": "SUCCESS", "rows_inserted": 5})
        out = puller.pull_all()
        assert pulled == [2022]
        assert out["succeeded"] == out["total"] == 1 and out["total_rows"] == 5


def test_sec_velocity_default_identity_is_resolved(monkeypatch):
    from ingestion import sec_velocity

    seen: list[str] = []
    monkeypatch.setattr(sec_velocity, "set_identity", lambda ident: seen.append(ident))
    monkeypatch.setattr(sec_velocity.BasePuller, "__init__", lambda self, engine: setattr(self, "source_id", 1))
    sec_velocity.SECVelocityPuller(db_engine=None)
    assert seen and "localhost" not in seen[0] and "@" in seen[0]


def test_gdelt_cadence_and_jupiter_default():
    from config import Settings
    from ingestion.smart_scheduler import PULLER_REGISTRY

    gdelt = next(p for p in PULLER_REGISTRY if p["name"] == "gdelt")
    assert gdelt["freq_h"] >= 1
    assert "token.jup.ag" not in Settings.model_fields["SOLANA_UNIVERSE_JUPITER_URL"].default
