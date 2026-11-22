"""Regression tests closing Codex review HOLDs on PR #1370 (ab73c34b).

Covers:
1. EIA validation before writes (weekly Cushing stocks + daily Brent/WTI spot):
   - returned record series/facet matches requested series/facet
   - returned unit matches expected unit (MBBL/thousand barrels for Cushing;
     dollars per barrel for spot)
   - finite numeric value (rejects NaN, inf, -inf, blank, non-numeric, bool)
   - observation date within bounds (<= pull UTC clock, >= documented floor)
   - mixed valid/invalid writes only valid records and warns per rejection
   - wholly wrong-source response writes nothing and reports PARTIAL / FAILED
2. Drewry WCI composite proof:
   - rejects sibling-lane charts/tables/prose (e.g. Shanghai-Rotterdam)
   - rejects unlabeled generic charts and generic chart/page dates
   - accepts proven WCI composite evidence (chart, table, prose) with date
3. AIS UNKNOWN for absent fields:
   - unparsed fields stay None (UNKNOWN), never 0, never written as 0
   - capacity_utilization stays None when berth or anchor is UNKNOWN
   - explicit parsed 0 remains 0; no parsed sections still fails closed (None)

Pure unit tests: synthetic fixtures and mocks only, no DB, no network.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from loguru import logger as log

from ingestion.altdata import ais_ground_truth as ais
from ingestion.altdata import eia_puller as eia
from ingestion.altdata import supply_chain as sc
from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS
from oracle.feature_polarity import FEATURE_POLARITY


# ---------------------------------------------------------------------------
# Shared test helpers (pure Python, no DB / no network)
# ---------------------------------------------------------------------------

_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "sources"


def _load_fake_store():
    spec = importlib.util.spec_from_file_location(
        "fake_store_for_fp_ingest_validation",
        _FIXTURES_DIR / "fake_store.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


_fake_store_mod = _load_fake_store()
FakeEngine = _fake_store_mod.FakeEngine


def _http_resp(
    text: str = "",
    status: int = 200,
    json_body: dict | list | None = None,
) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.raise_for_status = MagicMock()
    if json_body is not None:
        resp.json = MagicMock(return_value=json_body)
    return resp


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


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


def _make_ais_puller_capture():
    engine = MagicMock()
    conn = MagicMock()
    res = MagicMock()
    res.fetchone.return_value = None
    conn.execute.return_value = res
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(ais.AISGroundTruthPuller, "_resolve_source_id", return_value=1):
        puller = ais.AISGroundTruthPuller(engine, aishub_api_key="test-key")

    written: list[dict] = []

    def _capture_insert(conn, series_id, obs_date, value, raw_payload=None, **kwargs):
        written.append(
            {
                "series_id": series_id,
                "obs_date": obs_date,
                "value": value,
                "raw_payload": raw_payload,
            }
        )

    puller._bucket_row_exists = MagicMock(return_value=False)
    puller._insert_raw = MagicMock(side_effect=_capture_insert)
    return puller, written


# ===========================================================================
# HOLD 1: EIA validation before any write (weekly Cushing + daily spot)
# ===========================================================================


class TestEIAWeeklyStocksValidation:
    def test_rejects_wrong_series_id(self, captured_warnings):
        payload = {
            "response": {
                "data": [
                    {
                        "period": "2026-11-13",
                        "series": "WCRSTUS1",  # US total, NOT Cushing
                        "value": "430000",
                        "units": "MBBL",
                    },
                    {
                        "period": "2026-11-06",
                        # Missing series field altogether
                        "value": "24100",
                        "units": "MBBL",
                    },
                ]
            }
        }
        rows = eia.parse_weekly_stocks_response(payload)
        assert rows == []
        assert len(captured_warnings) >= 2

    def test_rejects_wrong_or_missing_unit(self, captured_warnings):
        payload = {
            "response": {
                "data": [
                    {
                        "period": "2026-11-13",
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": "24531",
                        "units": "$/BBL",
                    },
                    {
                        "period": "2026-11-06",
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": "24531",
                        "units": "BBL",  # barrels, not thousand barrels (MBBL)
                    },
                    {
                        "period": "2026-10-30",
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": "24531",
                        "units": "MMBBL",  # million barrels
                    },
                ]
            }
        }
        rows = eia.parse_weekly_stocks_response(payload)
        assert rows == []
        assert len(captured_warnings) >= 3

    def test_missing_unit_field_accepted_only_on_exact_series(self, captured_warnings):
        # No recorded live EIA response pins the unit field's spelling, so a
        # record with NO unit field passes only on an exact series match (one
        # summary warning); a wrong series without units is still rejected.
        payload = {
            "response": {
                "data": [
                    {
                        "period": "2026-10-23",
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": "24531",
                    },
                    {
                        "period": "2026-10-16",
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": "24400",
                        "units": "   ",
                    },
                    {
                        "period": "2026-10-09",
                        "series": "WCRSTUS1",
                        "value": "430000",
                    },
                ]
            }
        }
        rows = eia.parse_weekly_stocks_response(payload)
        # Only the record with no unit alias at all passes, flagged unverified;
        # a present blank unit is rejected (Codex HOLD on 42c3df0e).
        assert [r["value"] for r in rows] == [24531.0]
        assert all("units" not in r and r["units_verified"] is False for r in rows)
        joined = " ".join(str(w) for w in captured_warnings)
        assert "carried no unit field" in joined
        assert "series mismatch" in joined
        assert "blank, malformed or unexpected units" in joined

    def test_rejects_future_and_absurdly_old_dates(self, captured_warnings):
        tomorrow = (_utc_today() + timedelta(days=1)).isoformat()
        payload = {
            "response": {
                "data": [
                    {
                        "period": tomorrow,
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": "24531",
                        "units": "MBBL",
                    },
                    {
                        "period": "1970-01-01",
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": "24531",
                        "units": "MBBL",
                    },
                    {
                        "period": "1900-01-01",
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": "24531",
                        "units": "MBBL",
                    },
                ]
            }
        }
        rows = eia.parse_weekly_stocks_response(payload)
        assert rows == []
        assert len(captured_warnings) >= 3

    @pytest.mark.parametrize(
        "bad_val",
        ["NaN", "nan", float("nan"), "inf", "-inf", "Infinity", float("inf"), "", "   ", "bad", None, True],
    )
    def test_rejects_nonfinite_blank_and_nonnumeric_values(self, bad_val, captured_warnings):
        payload = {
            "response": {
                "data": [
                    {
                        "period": "2026-11-13",
                        "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES,
                        "value": bad_val,
                        "units": "MBBL",
                    }
                ]
            }
        }
        rows = eia.parse_weekly_stocks_response(payload)
        assert rows == []
        assert len(captured_warnings) >= 1

    def test_mixed_valid_and_invalid_writes_only_valid_in_pull(
        self, monkeypatch, captured_warnings
    ):
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)
        tomorrow = (_utc_today() + timedelta(days=1)).isoformat()

        empty = {"response": {"data": []}}
        mixed_stocks = {
            "response": {
                "data": [
                    # Wrong series
                    {"period": "2026-11-13", "series": "WRONG_SERIES", "value": "99999", "units": "MBBL"},
                    # Wrong unit
                    {"period": "2026-11-13", "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES, "value": "24500", "units": "$/BBL"},
                    # Future-dated NaN (exact reviewer scenario)
                    {"period": tomorrow, "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES, "value": "NaN", "units": "MBBL"},
                    # Inf value
                    {"period": "2026-11-06", "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES, "value": "inf", "units": "MBBL"},
                    # Blank value
                    {"period": "2026-10-30", "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES, "value": "", "units": "MBBL"},
                    # Absurdly old
                    {"period": "1965-05-01", "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES, "value": "24000", "units": "MBBL"},
                    # Valid record
                    {"period": "2026-11-13", "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES, "value": "24531", "units": "MBBL"},
                ]
            }
        }

        engine = FakeEngine()
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[_http_resp(json_body=empty), _http_resp(json_body=empty), _http_resp(json_body=mixed_stocks)],
        ):
            result = eia.EIAPuller(db_engine=engine).pull(days_back=30)

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 1
        rows = engine.store.rows_for("EIA:eia_cushing_crude_stocks")
        assert len(rows) == 1
        assert rows[0]["obs_date"] == date(2026, 11, 13)
        assert rows[0]["value"] == pytest.approx(24531.0)
        assert len(captured_warnings) >= 6

    def test_wholly_wrong_source_cushing_writes_nothing_and_marks_partial(
        self, monkeypatch, captured_warnings
    ):
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)
        tomorrow = (_utc_today() + timedelta(days=1)).isoformat()

        brent_ok = {
            "response": {
                "data": [{"period": "2026-11-18", "series": "RBRTE", "value": "72.50", "units": "$/BBL"}]
            }
        }
        wti_ok = {
            "response": {
                "data": [{"period": "2026-11-18", "series": "RWTC", "value": "68.90", "units": "$/BBL"}]
            }
        }
        # Exact Codex HOLD scenario: wrong series/units future-dated NaN under Cushing
        bad_cushing = {
            "response": {
                "data": [
                    {"period": tomorrow, "series": "WCRSTUS1", "value": "NaN", "units": "$/BBL"},
                ]
            }
        }

        engine = FakeEngine()
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[
                _http_resp(json_body=brent_ok),
                _http_resp(json_body=wti_ok),
                _http_resp(json_body=bad_cushing),
            ],
        ):
            result = eia.EIAPuller(db_engine=engine).pull(days_back=30)

        assert result["status"] == "PARTIAL"
        assert result["rows_inserted"] == 2
        assert engine.store.rows_for("EIA:eia_cushing_crude_stocks") == []
        assert len(engine.store.rows_for("EIA:eia_brent_spot")) == 1
        assert len(engine.store.rows_for("EIA:eia_crude_price")) == 1
        assert len(captured_warnings) >= 1


class TestEIADailySpotValidation:
    def test_rejects_wrong_series_wrong_unit_future_old_and_nonfinite_spot(
        self, monkeypatch, captured_warnings
    ):
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)
        tomorrow = (_utc_today() + timedelta(days=1)).isoformat()

        mixed_brent = {
            "response": {
                "data": [
                    # Wrong series (WTI returned inside Brent pull)
                    {"period": "2026-11-18", "series": "RWTC", "value": "68.90", "units": "$/BBL"},
                    # Missing series
                    {"period": "2026-11-17", "value": "71.00", "units": "$/BBL"},
                    # Wrong unit (MBBL instead of $/BBL)
                    {"period": "2026-11-18", "series": "RBRTE", "value": "72.50", "units": "MBBL"},
                    # Missing unit field on an exact series match: validated as
                    # units_verified=false, then quarantined before the raw write
                    {"period": "2026-11-16", "series": "RBRTE", "value": "71.50"},
                    # Future date
                    {"period": tomorrow, "series": "RBRTE", "value": "73.00", "units": "$/BBL"},
                    # Absurdly old date
                    {"period": "1970-01-01", "series": "RBRTE", "value": "3.00", "units": "$/BBL"},
                    # Non-finite & blank values
                    {"period": "2026-11-15", "series": "RBRTE", "value": "NaN", "units": "$/BBL"},
                    {"period": "2026-11-14", "series": "RBRTE", "value": "inf", "units": "$/BBL"},
                    {"period": "2026-11-13", "series": "RBRTE", "value": "-inf", "units": "$/BBL"},
                    {"period": "2026-11-12", "series": "RBRTE", "value": "", "units": "$/BBL"},
                    # Valid record
                    {"period": "2026-11-18", "series": "RBRTE", "value": "72.50", "units": "$/BBL"},
                ]
            }
        }
        # WTI also accepts "Dollars per Barrel" and historical negative WTI (-36.98 on 2020-04-20)
        valid_wti = {
            "response": {
                "data": [
                    {"period": "2026-11-18", "series": "RWTC", "value": "68.90", "units": "Dollars per Barrel"},
                    {"period": "2020-04-20", "series": "RWTC", "value": "-36.98", "units": "$/BBL"},
                ]
            }
        }
        empty = {"response": {"data": []}}

        engine = FakeEngine()
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[
                _http_resp(json_body=mixed_brent),
                _http_resp(json_body=valid_wti),
                _http_resp(json_body=empty),
            ],
        ):
            result = eia.EIAPuller(db_engine=engine).pull(days_back=30)

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 3
        brent_rows = engine.store.rows_for("EIA:eia_brent_spot")
        assert {r["obs_date"]: r["value"] for r in brent_rows} == {
            date(2026, 11, 18): pytest.approx(72.50),
        }
        assert any("carried no unit field" in w for w in captured_warnings)
        assert any("quarantined 1 record(s) with no unit field" in w for w in captured_warnings)

        wti_rows = engine.store.rows_for("EIA:eia_crude_price")
        assert {r["obs_date"]: r["value"] for r in wti_rows} == {
            date(2026, 11, 18): pytest.approx(68.90),
            date(2020, 4, 20): pytest.approx(-36.98),
        }
        assert len(captured_warnings) >= 10

    def test_wholly_wrong_source_all_facets_writes_nothing_and_returns_failed(
        self, monkeypatch, captured_warnings
    ):
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)

        wrong_brent = {
            "response": {
                "data": [{"period": "2026-11-18", "series": "NOT_BRENT", "value": "70.00", "units": "$/BBL"}]
            }
        }
        wrong_wti = {
            "response": {
                "data": [{"period": "2026-11-18", "series": "RWTC", "value": "68.00", "units": "$/GAL"}]
            }
        }
        wrong_cushing = {
            "response": {
                "data": [{"period": "2026-11-13", "series": "WCRSTUS1", "value": "430000", "units": "MBBL"}]
            }
        }

        engine = FakeEngine()
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[
                _http_resp(json_body=wrong_brent),
                _http_resp(json_body=wrong_wti),
                _http_resp(json_body=wrong_cushing),
            ],
        ):
            result = eia.EIAPuller(db_engine=engine).pull(days_back=30)

        assert result["status"] == "FAILED"
        assert result["rows_inserted"] == 0
        assert engine.store.rows == []
        assert len(captured_warnings) >= 3

    def test_emitted_series_ids_consistent_with_entity_map_and_feature_polarity(self):
        all_mappings = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
        assert eia.EMITTED_SERIES_IDS == (
            "EIA:eia_brent_spot",
            "EIA:eia_crude_price",
            "EIA:eia_cushing_crude_stocks",
        )
        for sid in eia.EMITTED_SERIES_IDS:
            feature_name = all_mappings[sid]
            assert feature_name in FEATURE_POLARITY


# ===========================================================================
# HOLD 2: Drewry WCI composite proof (sibling chart, generic date, composite)
# ===========================================================================


class TestDrewryWCICompositeProof:
    @pytest.mark.parametrize(
        "sibling_html",
        [
            # Sibling single-lane chart script (Shanghai - Rotterdam)
            """
            <html><body>
              <h1>World Container Index</h1>
              <script>
                var laneChart = {
                  title: "Shanghai - Rotterdam",
                  labels: ["12 Nov 2026", "19 Nov 2026"],
                  data: [2610, 2680]
                };
              </script>
            </body></html>
            """,
            # Lane chart whose title also mentions World Container Index
            """
            <html><body>
              <script>
                var chart = {
                  title: "World Container Index - Shanghai to Rotterdam",
                  labels: ["12 Nov 2026", "19 Nov 2026"],
                  data: [2610, 2680]
                };
              </script>
            </body></html>
            """,
            # Generic unlabeled chart script on a WCI page (proves nothing)
            """
            <html><body>
              <h1>World Container Index</h1>
              <script>
                var chart = {
                  labels: ["12 Nov 2026", "19 Nov 2026"],
                  data: [2610, 2680]
                };
              </script>
            </body></html>
            """,
            # Multi-series chart with ONLY lane datasets, no composite series
            """
            <html><body>
              <script>
                var chart = {
                  title: "World Container Index",
                  labels: ["12 Nov 2026", "19 Nov 2026"],
                  datasets: [
                    { label: "Shanghai - Rotterdam", data: [2610, 2680] },
                    { label: "Shanghai - Los Angeles", data: [3100, 3250] }
                  ]
                };
              </script>
            </body></html>
            """,
            # Sibling lane HTML table (no composite column)
            """
            <html><body>
              <h1>World Container Index</h1>
              <table>
                <thead><tr><th>Date</th><th>WCI Shanghai - Rotterdam</th></tr></thead>
                <tbody><tr><td>19 Nov 2026</td><td>$2,680</td></tr></tbody>
              </table>
            </body></html>
            """,
            # Sibling lane HTML table carrying "World Container Index" in header
            """
            <html><body>
              <table>
                <thead><tr><th>Date</th><th>World Container Index - Shanghai - Rotterdam</th></tr></thead>
                <tbody><tr><td>19 Nov 2026</td><td>$2,680</td></tr></tbody>
              </table>
            </body></html>
            """,
            # Sibling lane prose appearing right after WCI heading
            """
            <html><body>
              <article>
                <h1>World Container Index - 19 Nov 2026</h1>
                <p>Freight rates on Shanghai - Rotterdam increased 3% to $2,680 per 40ft
                   container (assessed on 19 Nov 2026).</p>
              </article>
            </body></html>
            """,
        ],
    )
    def test_rejects_sibling_chart_or_unproven_lane_page(
        self, sibling_html: str, captured_warnings
    ):
        puller, rows = _make_supply_chain_puller()
        with patch(
            "ingestion.altdata.supply_chain.requests.get",
            return_value=_http_resp(sibling_html),
        ):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["rows_inserted"] == 0
        assert res["status"] == "PARTIAL"
        assert rows == []
        assert len(captured_warnings) >= 1

    @pytest.mark.parametrize(
        "generic_date_html",
        [
            # Generic <time> tag in page header/meta decoupled from composite value
            """
            <html><body>
              <div class="meta">Chart updated <time datetime="2026-11-19">19 Nov 2026</time></div>
              <p>The Drewry World Container Index composite is $2,145 per 40ft container.</p>
            </body></html>
            """,
            # Generic "Updated on" banner decoupled from composite sentence
            """
            <html><body>
              <header>Page updated on 19 Nov 2026</header>
              <section>
                <p>The Drewry World Container Index composite stands at $2,145 per 40ft container.</p>
              </section>
            </body></html>
            """,
            # Generic chart date next to an unlabeled freight figure
            """
            <html><body>
              <div class="chart-card">
                <span class="chart-date">As of 19 Nov 2026</span>
                <p>Benchmark freight rate: $2,145 per 40ft container.</p>
              </div>
            </body></html>
            """,
        ],
    )
    def test_rejects_generic_chart_or_page_date(
        self, generic_date_html: str, captured_warnings
    ):
        puller, rows = _make_supply_chain_puller()
        with patch(
            "ingestion.altdata.supply_chain.requests.get",
            return_value=_http_resp(generic_date_html),
        ):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["rows_inserted"] == 0
        assert res["status"] == "PARTIAL"
        assert rows == []
        assert len(captured_warnings) >= 1

    def test_accepts_composite_dataset_from_multiseries_chart_ignoring_sibling_lane(self):
        html = """
        <html><body>
          <h1>Drewry World Container Index</h1>
          <script>
            var wciChart = {
              title: "World Container Index",
              labels: ["12 Nov 2026", "19 Nov 2026"],
              datasets: [
                { label: "Shanghai - Rotterdam", data: [2610, 2680] },
                { label: "WCI Composite", data: [2100, 2145] }
              ]
            };
          </script>
        </body></html>
        """
        puller, rows = _make_supply_chain_puller()
        with patch(
            "ingestion.altdata.supply_chain.requests.get",
            return_value=_http_resp(html),
        ):
            res = puller._pull_drewry(date(2026, 1, 1))

        assert res["status"] == "SUCCESS"
        assert res["rows_inserted"] == 2
        by_date = {r["obs_date"]: r["value"] for r in rows}
        assert by_date == {
            date(2026, 11, 12): pytest.approx(2100.0),
            date(2026, 11, 19): pytest.approx(2145.0),
        }

    def test_accepts_single_series_composite_chart_and_composite_prose(self):
        chart_html = """
        <html><body>
          <script>
            var chart = {
              label: "WCI Composite Index",
              dates: ["2026-11-12", "2026-11-19"],
              values: [2100, 2145]
            };
          </script>
        </body></html>
        """
        points = sc._parse_drewry_wci_html(chart_html, pull_date=date(2026, 11, 22))
        assert points == [
            {"date": "2026-11-12", "value": 2100.0},
            {"date": "2026-11-19", "value": 2145.0},
        ]

        prose_html = """
        <html><body>
          <article>
            <p>The Drewry World Container Index composite assessed on 19 Nov 2026
               stood at $2,145 per 40ft container.</p>
          </article>
        </body></html>
        """
        prose_points = sc._parse_drewry_wci_html(prose_html, pull_date=date(2026, 11, 22))
        assert prose_points == [{"date": "2026-11-19", "value": 2145.0}]

        highcharts_html = """
        <html><body>
          <script>
            Highcharts.chart('container', {
              title: { text: 'World Container Index' },
              xAxis: { categories: ['12 Nov 2026', '19 Nov 2026'] },
              series: [
                { name: 'Shanghai - Rotterdam', data: [2610, 2680] },
                { name: 'Composite Index', data: [2100, 2145] }
              ]
            });
          </script>
        </body></html>
        """
        hc_points = sc._parse_drewry_wci_html(highcharts_html, pull_date=date(2026, 11, 22))
        assert hc_points == [
            {"date": "2026-11-12", "value": 2100.0},
            {"date": "2026-11-19", "value": 2145.0},
        ]


# ===========================================================================
# HOLD 3: AIS UNKNOWN for absent fields (never 0, never written as 0)
# ===========================================================================


class TestAISAbsentFieldsStayUnknown:
    def test_partial_page_leaves_unparsed_fields_none_and_writes_only_parsed(self):
        # Only "In Port" section is present; at_anchor, expected, departed are absent.
        partial_html = """
        <html><body>
          <section class="in-port">
            <h2>In Port</h2>
            <span class="count">12 vessels in port</span>
          </section>
        </body></html>
        """
        port = ais.AIS_PORTS[0]  # qingdao
        snap = ais._parse_vesselfinder_html(partial_html, port)

        assert snap is not None
        assert snap.ships_at_berth == 12
        assert snap.ships_at_anchor is None
        assert snap.ships_expected is None
        assert snap.ships_departed_24h is None
        assert snap.capacity_utilization is None

        puller, written = _make_ais_puller_capture()
        inserted = puller.save_to_db([snap])

        assert inserted == 1
        assert len(written) == 1
        assert written[0]["series_id"] == "ais:ships_at_berth:qingdao"
        assert written[0]["value"] == pytest.approx(12.0)
        assert written[0]["raw_payload"]["ships_at_anchor"] is None
        assert written[0]["raw_payload"]["ships_expected"] is None
        assert written[0]["raw_payload"]["ships_departed_24h"] is None
        assert written[0]["raw_payload"]["capacity_utilization"] is None

    def test_partial_page_only_expected_arrivals_leaves_others_unknown(self):
        partial_html = """
        <html><body>
          <section class="expected">
            <h2>Expected Arrivals</h2>
            <span class="count">8 vessels expected</span>
          </section>
        </body></html>
        """
        snap = ais._parse_vesselfinder_html(partial_html, ais.AIS_PORTS[1])

        assert snap is not None
        assert snap.ships_at_berth is None
        assert snap.ships_at_anchor is None
        assert snap.ships_expected == 8
        assert snap.ships_departed_24h is None
        assert snap.capacity_utilization is None

        puller, written = _make_ais_puller_capture()
        inserted = puller.save_to_db([snap])
        assert inserted == 1
        assert [r["series_id"] for r in written] == ["ais:ships_expected:shanghai"]

    def test_explicit_zero_section_remains_zero_while_absent_sections_stay_none(self):
        html = """
        <html><body>
          <section class="in-port">
            <h2>In Port</h2>
            <span class="count">12 vessels in port</span>
          </section>
          <section class="at-anchor">
            <h2>At Anchor</h2>
            <span class="count">0 vessels at anchor</span>
          </section>
        </body></html>
        """
        snap = ais._parse_vesselfinder_html(html, ais.AIS_PORTS[0])

        assert snap is not None
        assert snap.ships_at_berth == 12
        assert snap.ships_at_anchor == 0
        assert snap.ships_expected is None
        assert snap.ships_departed_24h is None
        assert snap.capacity_utilization == pytest.approx(1.0)

        puller, written = _make_ais_puller_capture()
        inserted = puller.save_to_db([snap])
        assert inserted == 3
        written_map = {r["series_id"]: r["value"] for r in written}
        assert written_map == {
            "ais:ships_at_berth:qingdao": pytest.approx(12.0),
            "ais:ships_at_anchor:qingdao": pytest.approx(0.0),
            "ais:capacity_utilization:qingdao": pytest.approx(1.0),
        }

    def test_compute_capacity_utilization_returns_none_when_either_input_unknown(self):
        assert ais.compute_capacity_utilization(12, None) is None
        assert ais.compute_capacity_utilization(None, 5) is None
        assert ais.compute_capacity_utilization(None, None) is None

    def test_no_section_parsed_still_fails_closed_none(self, captured_warnings):
        html = "<html><body><section><h2>In Port</h2><span>unavailable</span></section></body></html>"
        assert ais._parse_vesselfinder_html(html, ais.AIS_PORTS[0]) is None
        assert len(captured_warnings) >= 1

    def test_aishub_leaves_unobserved_expected_and_departed_none(self):
        puller, written = _make_ais_puller_capture()
        body = [
            {"ERROR": False},
            [
                {"NAVSTAT": 5, "SOG": 0.0},
                {"NAVSTAT": 1, "SOG": 0.1},
                {"NAVSTAT": 0, "SOG": 0.2},
            ],
        ]
        with patch(
            "ingestion.altdata.ais_ground_truth.requests.get",
            return_value=_http_resp(json_body=body, status=200),
        ):
            snap = puller._pull_aishub(ais.AIS_PORTS[0])

        assert snap is not None
        assert snap.ships_expected is None
        assert snap.ships_departed_24h is None
        inserted = puller.save_to_db([snap])
        written_sids = {r["series_id"] for r in written}
        assert inserted == 3
        assert f"ais:ships_expected:{ais.AIS_PORTS[0].slug}" not in written_sids
        assert f"ais:ships_departed_24h:{ais.AIS_PORTS[0].slug}" not in written_sids


def test_drewry_js_object_scan_is_depth_capped_and_still_finds_shallow_charts():
    # A pathologically deep page must not make the scan quadratic; real chart
    # configs (a few levels deep) are still extracted.
    import time as _time

    deep = "{" * 5000 + "}" * 5000
    shallow = '{"title": "World Container Index composite", "data": [1, 2]}'
    started = _time.monotonic()
    objs = sc._iter_js_objects(deep + shallow)
    assert _time.monotonic() - started < 5.0
    assert shallow in objs
    # Only the outermost _MAX_JS_OBJECT_DEPTH levels of the deep block are kept.
    assert len(objs) == sc._MAX_JS_OBJECT_DEPTH + 1



# ===========================================================================
# Codex HOLD on #1384 head 42c3df0e
# (1) a generic "composite" is not positive Drewry WCI attribution;
# (2) one EIA unit alias must not mask another's blank/malformed/contradicting value.
# ===========================================================================


class TestDrewryWCIPositiveAttribution:
    PULL = date(2026, 11, 22)

    def test_unrelated_composite_chart_on_a_wci_page_is_refused(self, captured_warnings):
        html = """
        <html><body>
          <h1>Drewry World Container Index</h1>
          <script>
            new Chart(ctx, {
              labels: ["12 Nov 2026", "19 Nov 2026"],
              data: [2100, 2145],
              label: "Composite Warehousing Cost"
            });
          </script>
        </body></html>
        """
        assert sc._parse_drewry_wci_html(html, pull_date=self.PULL) == []

    def test_unrelated_composite_table_column_on_a_wci_page_is_refused(self):
        html = """
        <html><body>
          <h1>Drewry World Container Index</h1>
          <table>
            <thead><tr><th>Date</th><th>Composite Warehousing Cost</th></tr></thead>
            <tbody><tr><td>2026-11-19</td><td>$2,145</td></tr></tbody>
          </table>
        </body></html>
        """
        assert sc._parse_drewry_wci_html(html, pull_date=self.PULL) == []

    def test_bare_composite_needs_its_own_wci_context(self):
        no_context = """
        <html><body>
          <table>
            <thead><tr><th>Date</th><th>Composite</th></tr></thead>
            <tbody><tr><td>2026-11-19</td><td>$2,145</td></tr></tbody>
          </table>
        </body></html>
        """
        assert sc._parse_drewry_wci_html(no_context, pull_date=self.PULL) == []

        chart_other_title = """
        <html><body>
          <h1>Drewry World Container Index</h1>
          <script>
            Highcharts.chart('c', {
              title: { text: 'Warehousing cost index' },
              xAxis: { categories: ['12 Nov 2026', '19 Nov 2026'] },
              series: [ { name: 'Composite', data: [2100, 2145] } ]
            });
          </script>
        </body></html>
        """
        assert sc._parse_drewry_wci_html(chart_other_title, pull_date=self.PULL) == []

    def test_unrelated_composite_table_is_skipped_and_the_wci_table_is_read(self):
        html = """
        <html><body>
          <h1>Drewry World Container Index</h1>
          <h2>Warehousing costs</h2>
          <table>
            <thead><tr><th>Date</th><th>Composite</th></tr></thead>
            <tbody><tr><td>2026-11-19</td><td>$9,999</td></tr></tbody>
          </table>
          <h2>World Container Index, 19 Nov 2026</h2>
          <table>
            <thead><tr><th>Date</th><th>Composite</th><th>Shanghai - Rotterdam</th></tr></thead>
            <tbody>
              <tr><td>2026-11-12</td><td>$2,100</td><td>$2,610</td></tr>
              <tr><td>2026-11-19</td><td>$2,145</td><td>$2,680</td></tr>
            </tbody>
          </table>
        </body></html>
        """
        assert sc._parse_drewry_wci_html(html, pull_date=self.PULL) == [
            {"date": "2026-11-12", "value": 2100.0},
            {"date": "2026-11-19", "value": 2145.0},
        ]

    def test_bare_composite_in_a_heading_less_section_after_a_wci_section_is_refused(self):
        """Codex HOLD on 4a1738df: the nearest-heading fallback must not lend a
        WCI heading from another section to a heading-less one."""
        html = """
        <html><body>
          <section>
            <h2>World Container Index, 19 Nov 2026</h2>
            <table>
              <thead><tr><th>Date</th><th>Shanghai - Rotterdam</th></tr></thead>
              <tbody><tr><td>2026-11-19</td><td>$2,680</td></tr></tbody>
            </table>
          </section>
          <section>
            <table>
              <thead><tr><th>Date</th><th>Composite</th></tr></thead>
              <tbody><tr><td>2026-11-19</td><td>$9,999</td></tr></tbody>
            </table>
          </section>
          <table>
            <thead><tr><th>Date</th><th>Composite</th></tr></thead>
            <tbody><tr><td>2026-11-12</td><td>$8,888</td></tr></tbody>
          </table>
        </body></html>
        """
        assert sc._parse_drewry_wci_html(html, pull_date=self.PULL) == []

        same_section = """
        <html><body>
          <section>
            <h2>World Container Index, 19 Nov 2026</h2>
            <div class="wrap"><table>
              <thead><tr><th>Date</th><th>Composite</th></tr></thead>
              <tbody><tr><td>2026-11-19</td><td>$2,145</td></tr></tbody>
            </table></div>
          </section>
        </body></html>
        """
        assert sc._parse_drewry_wci_html(same_section, pull_date=self.PULL) == [{"date": "2026-11-19", "value": 2145.0}]

    @pytest.mark.parametrize("label,ctx,ok", [
        ("WCI Composite", False, True),
        ("Drewry World Container Index composite", False, True),
        ("Composite WCI (USD/40ft)", False, True),
        ("Composite", True, True),
        ("Composite Index", True, True),
        ("Composite", False, False),
        ("Composite Warehousing Cost", True, False),
        ("Composite index of warehousing", True, False),
        ("WCI Composite Shanghai - Rotterdam", True, False),
        ("SCFI Composite", True, False),
    ])
    def test_label_attribution_rule(self, label, ctx, ok):
        assert sc._is_drewry_composite_chart_label(label, context_names_wci=ctx) is ok


def _eia_rec(period: str, value: str, **units):
    rec = {"period": period, "series": eia.DEFAULT_WEEKLY_STOCKS_SERIES, "value": value}
    rec.update(units)
    return rec


class TestEIAUnitAliases:
    @pytest.mark.parametrize("units", [
        {"units": "", "value-units": "USD"},
        {"units": "MBBL", "value-units": "USD"},
        {"units": "   ", "value-units": "MBBL"},
        {"units": None},
        {"value-units": None},
        {"units": 1000},
        {"units": ["MBBL"]},
        {"value-units": "MMBBL"},
    ], ids=["blank_vs_usd", "mbbl_vs_usd", "blank_vs_mbbl", "null_units", "null_value_units",
            "number", "list", "wrong_value_units"])
    def test_every_supplied_alias_must_be_an_expected_unit(self, units, captured_warnings):
        rows = eia.parse_weekly_stocks_response({"response": {"data": [_eia_rec("2026-11-13", "24531", **units)]}})
        assert rows == []
        assert any("blank, malformed or unexpected units" in str(w) for w in captured_warnings)

    def test_agreeing_aliases_are_verified(self):
        rows = eia.parse_weekly_stocks_response({"response": {"data": [
            _eia_rec("2026-11-13", "24531", units="MBBL", **{"value-units": "thousand barrels"}),
        ]}})
        assert [(r["value"], r["units"], r["units_verified"]) for r in rows] == [(24531.0, "MBBL", True)]

    def test_writer_quarantines_omitted_units_before_the_raw_write_and_refuses_alias_conflicts(
        self, monkeypatch, captured_warnings
    ):
        """Codex HOLD on 4a1738df: a record with no unit field must not become a
        canonical raw_series row (the entity map and resolver would normalize it)."""
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)
        empty = {"response": {"data": []}}
        stocks = {"response": {"data": [
            _eia_rec("2026-11-13", "24531", units="MBBL"),                       # verified
            _eia_rec("2026-11-06", "24400"),                                     # truly omitted
            _eia_rec("2026-10-30", "24300", units="", **{"value-units": "USD"}),  # blank masks USD
            _eia_rec("2026-10-23", "24200", units="MBBL", **{"value-units": "USD"}),  # MBBL masks USD
        ]}}
        engine = FakeEngine()
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[_http_resp(json_body=empty), _http_resp(json_body=empty), _http_resp(json_body=stocks)],
        ):
            result = eia.EIAPuller(db_engine=engine).pull(days_back=60)

        assert result["status"] == "SUCCESS" and result["rows_inserted"] == 1
        rows = engine.store.rows_for("EIA:eia_cushing_crude_stocks")
        assert [r["obs_date"] for r in rows] == [date(2026, 11, 13)]
        raw = rows[0]["raw_payload"]
        payload = json.loads(raw) if isinstance(raw, str) else raw
        assert payload["units"] == "MBBL" and payload["units_verified"] is True
        assert any("quarantined 1 record(s) with no unit field" in str(w) for w in captured_warnings)

    def test_a_facet_with_only_unit_less_records_writes_nothing_and_reports_failure(
        self, monkeypatch, captured_warnings
    ):
        monkeypatch.setenv("EIA_API_KEY", "REDACTED-FIXTURE")
        monkeypatch.setattr(eia.time, "sleep", lambda _s: None)
        brent = {"response": {"data": [{"period": "2026-11-18", "series": "RBRTE", "value": "72.50"}]}}
        wti = {"response": {"data": [{"period": "2026-11-18", "series": "RWTC", "value": "68.90", "units": "$/BBL"}]}}
        stocks = {"response": {"data": [_eia_rec("2026-11-13", "24531")]}}
        engine = FakeEngine()
        with patch(
            "ingestion.altdata.eia_puller.requests.get",
            side_effect=[_http_resp(json_body=brent), _http_resp(json_body=wti), _http_resp(json_body=stocks)],
        ):
            result = eia.EIAPuller(db_engine=engine).pull(days_back=30)

        assert result["status"] == "PARTIAL" and result["rows_inserted"] == 1
        assert engine.store.rows_for("EIA:eia_brent_spot") == []
        assert engine.store.rows_for("EIA:eia_cushing_crude_stocks") == []
        assert len(engine.store.rows_for("EIA:eia_crude_price")) == 1
