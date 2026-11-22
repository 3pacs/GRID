"""Regression tests for ingestion integrity defects (2026-11-22 slice).

Covers:
1. Drewry container_index (ingestion/altdata/supply_chain.py):
   - Uses the page's own assessment date (never date.today() / today - weeks).
   - Fails closed (writes nothing, logs warning) when no assessment date is parseable.
   - Never writes an obs_date later than the pull date.
2. AIS ground truth (ingestion/altdata/ais_ground_truth.py):
   - Removes raw unstripped HTML regex fallback.
   - Fails closed (returns None / writes nothing, logs warning) on malformed HTML,
     missing structured sections, or BeautifulSoup parse failure.
3. EIA puller & entity map (ingestion/altdata/eia_puller.py, normalization/entity_map.py):
   - Puller's emitted series IDs resolve in SEED_MAPPINGS / NEW_MAPPINGS_V2.
   - Weekly Cushing crude stocks (fetch_weekly_stocks) is wired into EIAPuller.pull().

Pure unit tests: synthetic fixtures and mocks only, no database, no network.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from loguru import logger as log

from ingestion.altdata import ais_ground_truth as ais
from ingestion.altdata import eia_puller as eia
from ingestion.altdata import supply_chain as sc
from normalization.entity_map import EntityMap, NEW_MAPPINGS_V2, SEED_MAPPINGS


# ---------------------------------------------------------------------------
# Shared test helpers (pure Python, no DB / no network)
# ---------------------------------------------------------------------------

_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "sources"


def _load_fake_store():
    spec = importlib.util.spec_from_file_location(
        "fake_store_for_fp_ingest_integrity",
        _FIXTURES_DIR / "fake_store.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


_fake_store_mod = _load_fake_store()
FakeEngine = _fake_store_mod.FakeEngine


def _http_resp(text: str = "", status: int = 200, json_body: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.raise_for_status = MagicMock()
    if json_body is not None:
        resp.json = MagicMock(return_value=json_body)
    return resp


@pytest.fixture
def captured_warnings():
    messages: list[str] = []
    sink_id = log.add(lambda m: messages.append(str(m)), level="WARNING")
    yield messages
    log.remove(sink_id)


def _make_supply_chain_puller(existing: set[date] | None = None):
    with patch.object(sc.SupplyChainPuller, "_resolve_source_id", return_value=1):
        puller = sc.SupplyChainPuller(MagicMock())
    inserted_rows: list[dict] = []

    def _capture_insert(conn, series_id, obs_date, value, raw_payload=None, **kwargs):
        inserted_rows.append(
            {
                "series_id": series_id,
                "obs_date": obs_date,
                "value": value,
                "raw_payload": raw_payload,
            }
        )

    puller._get_existing_dates = MagicMock(return_value=set(existing or ()))
    puller._insert_raw = MagicMock(side_effect=_capture_insert)
    return puller, inserted_rows


def _make_ais_mock_engine() -> MagicMock:
    engine = MagicMock()
    begin_conn = MagicMock()
    begin_result = MagicMock()
    begin_result.fetchone.return_value = None
    begin_result.fetchall.return_value = []
    begin_conn.execute.return_value = begin_result
    engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    connect_conn = MagicMock()
    source_result = MagicMock()
    source_result.fetchone.return_value = (1,)
    connect_conn.execute.return_value = source_result
    engine.connect.return_value.__enter__ = MagicMock(return_value=connect_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


# ===========================================================================
# Defect 1: Drewry container_index assessment date & future-date guard
# ===========================================================================


class TestDrewryContainerIndexIntegrity:
    def test_uses_page_assessment_date_from_table(self):
        html = """
        <html><body>
          <h1>World Container Index</h1>
          <table>
            <thead>
              <tr><th>Date</th><th>Composite</th><th>Shanghai - Rotterdam</th></tr>
            </thead>
            <tbody>
              <tr><td>19 Nov 2026</td><td>$2,145</td><td>$2,680</td></tr>
              <tr><td>12 Nov 2026</td><td>$2,100</td><td>$2,610</td></tr>
            </tbody>
          </table>
        </body></html>
        """
        puller, rows = _make_supply_chain_puller()
        with patch("ingestion.altdata.supply_chain.requests.get", return_value=_http_resp(html)):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["status"] == "SUCCESS"
        assert res["rows_inserted"] == 2
        by_date = {r["obs_date"]: r["value"] for r in rows}
        assert by_date == {
            date(2026, 11, 19): pytest.approx(2145.0),
            date(2026, 11, 12): pytest.approx(2100.0),
        }
        assert date.today() not in by_date
        assert all(r["series_id"] == "container_index" for r in rows)

    def test_uses_page_assessment_date_from_prose(self):
        html = """
        <html><body>
          <article>
            <h1>World Container Index - 19 Nov 2026</h1>
            <p>The Drewry World Container Index increased 2% to $2,145 per 40ft container
               this week (assessed on 19 Nov 2026).</p>
          </article>
        </body></html>
        """
        puller, rows = _make_supply_chain_puller()
        with patch("ingestion.altdata.supply_chain.requests.get", return_value=_http_resp(html)):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["status"] == "SUCCESS"
        assert res["rows_inserted"] == 1
        assert len(rows) == 1
        assert rows[0]["obs_date"] == date(2026, 11, 19)
        assert rows[0]["value"] == pytest.approx(2145.0)

    def test_uses_transposed_table_header_dates(self):
        html = """
        <html><body>
          <h2>World Container Index, 19 Nov 2026</h2>
          <table>
            <thead>
              <tr><th>Route</th><th>12 Nov 26</th><th>19 Nov 26</th><th>Weekly change</th></tr>
            </thead>
            <tbody>
              <tr><td>Composite Index</td><td>$2,100</td><td>$2,145</td><td>+2%</td></tr>
              <tr><td>Shanghai - Rotterdam</td><td>$2,610</td><td>$2,680</td><td>+3%</td></tr>
            </tbody>
          </table>
        </body></html>
        """
        puller, rows = _make_supply_chain_puller()
        with patch("ingestion.altdata.supply_chain.requests.get", return_value=_http_resp(html)):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["status"] == "SUCCESS"
        by_date = {r["obs_date"]: r["value"] for r in rows}
        assert by_date == {
            date(2026, 11, 12): pytest.approx(2100.0),
            date(2026, 11, 19): pytest.approx(2145.0),
        }

    def test_refuses_to_write_without_assessment_date(self, captured_warnings):
        # Has BOTH an unlabeled script array and a WCI composite dollar figure,
        # but NO assessment date anywhere on the page. Must fail closed.
        html = """
        <html><body>
          <script>
            var chart = { series: [2010, 2050, 2090, 2120, 2145, 2180] };
          </script>
          <p>The Drewry World Container Index composite is $2,180 per 40ft container.</p>
        </body></html>
        """
        puller, rows = _make_supply_chain_puller()
        with patch("ingestion.altdata.supply_chain.requests.get", return_value=_http_resp(html)):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["rows_inserted"] == 0
        assert res["status"] == "PARTIAL"
        assert rows == []
        assert any("Drewry" in w or "assessment date" in w.lower() for w in captured_warnings)

    def test_never_writes_future_obs_date(self, captured_warnings):
        future_date = date.today() + timedelta(days=3)
        valid_past = date(2026, 11, 19)
        html = f"""
        <html><body>
          <h1>Drewry World Container Index</h1>
          <table>
            <thead><tr><th>Date</th><th>Composite</th></tr></thead>
            <tbody>
              <tr><td>{future_date.isoformat()}</td><td>$2,500</td></tr>
              <tr><td>{valid_past.isoformat()}</td><td>$2,145</td></tr>
            </tbody>
          </table>
        </body></html>
        """
        puller, rows = _make_supply_chain_puller()
        with patch("ingestion.altdata.supply_chain.requests.get", return_value=_http_resp(html)):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["rows_inserted"] == 1
        assert [r["obs_date"] for r in rows] == [valid_past]
        assert all(r["obs_date"] <= date.today() for r in rows)
        assert any("future" in w.lower() for w in captured_warnings)

    def test_pull_drewry_rejects_future_date_even_from_fetch_helper(self, captured_warnings):
        future_date = date.today() + timedelta(days=2)
        puller, rows = _make_supply_chain_puller()
        with patch.object(
            puller,
            "_fetch_drewry_wci",
            return_value=[{"date": future_date.isoformat(), "value": 2400.0}],
        ):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["rows_inserted"] == 0
        assert rows == []
        assert any("future" in w.lower() for w in captured_warnings)


# ===========================================================================
# Defect 2: AIS ground truth raw-HTML fallback removed (fail closed)
# ===========================================================================


class TestAISGroundTruthFailClosed:
    @pytest.mark.parametrize(
        "malformed_html",
        [
            # Port code digits right after 'arrivals' in raw HTML attribute
            '<a href="/ports/CNQIN001/arrivals" data-port="001">Arrivals</a>',
            # Attribute integer right after 'departed'
            '<a class="departed" data-id="42" href="/ports/USLAX001">Departures</a>',
            # Script content + canonical URL with port code and query digit
            (
                '<html><head><link rel="canonical" '
                'href="https://www.vesselfinder.com/ports/CNQIN001?tab=arrivals&v=7">'
                '</head><body><script>var s = "in port: 999";</script></body></html>'
            ),
            # Unstructured raw text with no section/div container tags
            "Currently in port: 12 vessels; Expected arrivals: 8; Departed: 3",
            # Error banner inside a div containing HTTP status code after label
            "<html><body><div class='err'>Expected arrivals unavailable, error 503</div></body></html>",
            # Port code inside a nav div
            "<html><body><div class='nav'>Arrivals CNQIN001</div></body></html>",
        ],
    )
    def test_malformed_or_unstructured_html_returns_none_and_logs(
        self, malformed_html: str, captured_warnings
    ):
        snap = ais._parse_vesselfinder_html(malformed_html, ais.AIS_PORTS[0])
        assert snap is None
        assert len(captured_warnings) >= 1

    def test_missing_structured_section_does_not_leak_raw_html_attributes(self):
        # Only "In Port" is properly structured; "arrivals" and "departures"
        # only appear in raw anchor attributes / port codes and must stay None (UNKNOWN).
        html = """
        <html><body>
          <nav>
            <a class="arrivals" href="/ports/CNQIN001/arrivals" data-tab="88">Arrivals</a>
            <a class="departures" href="/ports/CNQIN001/departures?page=9">Departures</a>
          </nav>
          <section class="in-port">
            <h2>In Port</h2>
            <span class="count">12 vessels in port</span>
          </section>
        </body></html>
        """
        snap = ais._parse_vesselfinder_html(html, ais.AIS_PORTS[0])
        assert snap is not None
        assert snap.ships_at_berth == 12
        assert snap.ships_at_anchor is None
        assert snap.ships_expected is None
        assert snap.ships_departed_24h is None
        assert snap.capacity_utilization is None

    def test_bs4_parse_error_fails_closed_no_raw_fallback(self, captured_warnings):
        html = "<section><h2>In Port</h2><span>12 vessels in port</span></section>"
        with patch("bs4.BeautifulSoup", side_effect=RuntimeError("parser exploded")):
            snap = ais._parse_vesselfinder_html(html, ais.AIS_PORTS[0])

        assert snap is None
        assert any("failed" in w.lower() for w in captured_warnings)

    def test_puller_writes_zero_rows_on_malformed_html(self, monkeypatch):
        monkeypatch.delenv("AISHUB_API_KEY", raising=False)
        engine = _make_ais_mock_engine()
        bad_html = '<a href="/ports/CNQIN001/arrivals" data-port="001">Arrivals</a>'
        with patch(
            "ingestion.altdata.ais_ground_truth.requests.get",
            return_value=_http_resp(bad_html, status=200),
        ), patch("ingestion.altdata.ais_ground_truth.time.sleep", lambda *a, **k: None):
            result = ais.run_ais_ground_truth_puller(engine)

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert len(result["ports_failed"]) == len(ais.AIS_PORTS)


# ===========================================================================
# Defect 3: EIA series-id alignment & weekly stocks wiring
# ===========================================================================


class TestEIASeriesAlignmentAndWeeklyStocks:
    def test_all_emitted_eia_series_ids_resolve_in_entity_map(self, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)

        all_mappings = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}

        # Check static puller id table (one source of truth)
        declared_ids = list(getattr(eia, "EMITTED_SERIES_IDS", ()))
        if not declared_ids:
            # Pre-fix fallback so this test fails on the actual mapping mismatch
            # rather than AttributeError before the fix is applied.
            declared_ids = [
                f"{eia._SERIES_PREFIX}.{suffix}" for suffix in eia._SERIES_MAP.values()
            ]
        assert len(declared_ids) >= 3, (
            f"Expected Brent, WTI, and weekly Cushing stocks ids, got {declared_ids!r}"
        )

        for sid in declared_ids:
            assert sid in all_mappings, (
                f"EIA puller series_id {sid!r} is missing from SEED_MAPPINGS/NEW_MAPPINGS_V2"
            )
            assert all_mappings[sid]

        # WTI (RWTC) must align to existing registered eia_crude_price mapping
        assert all_mappings["EIA:eia_crude_price"] == "eia_crude_price"
        assert "EIA:eia_crude_price" in declared_ids
        # Weekly Cushing stocks must NOT alias onto US-total eia_crude_stocks
        cushing_id = getattr(eia, "WEEKLY_STOCKS_SERIES_ID", None)
        assert cushing_id is not None and cushing_id in declared_ids
        assert all_mappings[cushing_id] != "eia_crude_stocks"

        # Verify EntityMap.get_feature_id resolves every declared id against a registry
        registry_rows = [
            (100 + idx, all_mappings[sid]) for idx, sid in enumerate(declared_ids)
        ]
        reg_engine = MagicMock()
        reg_conn = MagicMock()
        reg_engine.connect.return_value.__enter__.return_value = reg_conn
        reg_conn.execute.return_value.fetchall.return_value = registry_rows
        mapper = EntityMap(db_engine=reg_engine)
        for sid in declared_ids:
            assert mapper.get_feature_id(sid) is not None, f"EntityMap failed to resolve {sid!r}"

    def test_pull_wires_weekly_stocks_and_writes_mapped_ids(self, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)

        brent_body = {
            "response": {
                "data": [{"period": "2026-11-18", "series": "RBRTE", "value": "72.50", "units": "$/BBL"}]
            }
        }
        wti_body = {
            "response": {
                "data": [{"period": "2026-11-18", "series": "RWTC", "value": "68.90", "units": "$/BBL"}]
            }
        }
        stocks_body = {
            "response": {
                "data": [
                    {
                        "period": "2026-11-13",
                        "series": "W_EPC0_SAX_YCUOK_MBBL",
                        "value": "24531",
                        "units": "MBBL",
                    },
                    {
                        "period": "2026-11-06",
                        "series": "W_EPC0_SAX_YCUOK_MBBL",
                        "value": "24102",
                        "units": "MBBL",
                    },
                ]
            }
        }

        engine = FakeEngine()
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[
                _http_resp(json_body=brent_body),
                _http_resp(json_body=wti_body),
                _http_resp(json_body=stocks_body),
            ],
        ) as mock_get:
            result = eia.EIAPuller(db_engine=engine).pull(days_back=30)

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 4
        assert mock_get.call_count == 3

        # Confirm the 3rd request targeted the weekly stocks route and Cushing series
        stocks_call = mock_get.call_args_list[2]
        assert "petroleum/stoc/wstk/data/" in stocks_call.args[0]
        assert stocks_call.kwargs["params"]["facets[series][]"] == eia.DEFAULT_WEEKLY_STOCKS_SERIES

        # Every emitted series_id in raw_series must resolve via entity_map
        all_mappings = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
        written_sids = {r["series_id"] for r in engine.store.rows}
        assert written_sids == {
            "EIA:eia_brent_spot",
            "EIA:eia_crude_price",
            "EIA:eia_cushing_crude_stocks",
        }
        for sid in written_sids:
            assert sid in all_mappings

        # Check Cushing values & payload
        stock_rows = engine.store.rows_for("EIA:eia_cushing_crude_stocks")
        assert {r["obs_date"]: r["value"] for r in stock_rows} == {
            date(2026, 11, 13): pytest.approx(24531.0),
            date(2026, 11, 6): pytest.approx(24102.0),
        }
        payload = json.loads(stock_rows[0]["raw_payload"])
        assert payload["units"] == "MBBL"
        assert payload["series"] == "W_EPC0_SAX_YCUOK_MBBL"

        # Idempotent re-run writes 0 additional rows
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[
                _http_resp(json_body=brent_body),
                _http_resp(json_body=wti_body),
                _http_resp(json_body=stocks_body),
            ],
        ):
            rerun = eia.EIAPuller(db_engine=engine).pull(days_back=30)
        assert rerun["status"] == "SUCCESS"
        assert rerun["rows_inserted"] == 0

    def test_weekly_stocks_failure_marks_partial_and_redacts_key(
        self, monkeypatch
    ):
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)

        brent_body = {
            "response": {
                "data": [{"period": "2026-11-18", "series": "RBRTE", "value": "72.50", "units": "$/BBL"}]
            }
        }
        wti_body = {
            "response": {
                "data": [{"period": "2026-11-18", "series": "RWTC", "value": "68.90", "units": "$/BBL"}]
            }
        }
        leaking_err = ValueError(
            "500 Server Error for url: https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
            "?api_key=REDACTED-FIXTURE&frequency=weekly"
        )

        engine = FakeEngine()
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[
                _http_resp(json_body=brent_body),
                _http_resp(json_body=wti_body),
                leaking_err,
            ],
        ), patch("ingestion.altdata.eia_puller.log") as mock_log:
            result = eia.EIAPuller(db_engine=engine).pull(days_back=30)

        assert result["status"] == "PARTIAL"
        assert result["rows_inserted"] == 2
        assert mock_log.error.call_count == 0
        assert mock_log.warning.call_count == 1
        logged_text = " ".join(str(a) for a in mock_log.warning.call_args.args) + " " + " ".join(
            f"{k}={v}" for k, v in mock_log.warning.call_args.kwargs.items()
        )
        assert "api_key=***" in logged_text
        assert "REDACTED-FIXTURE" not in logged_text
