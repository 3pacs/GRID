"""Unit tests for ingestion/altdata/container_freight.py (CAT-82)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.container_freight import (
    ContainerFreightPuller,
    ContainerFreightSnapshot,
    DREWRY_ROUTES,
    DREWRY_WCI_URL,
    FRED_CANDIDATE_SERIES,
    SCFI_URL,
    SERIES_SCFI_COMPOSITE,
    SERIES_WCI_COMPOSITE,
    SERIES_WCI_ROUTE_PREFIX,
    _parse_drewry_wci_html,
    _parse_scfi_html,
    run_container_freight_puller,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_engine(source_id: int = 7) -> tuple[MagicMock, MagicMock]:
    """Build a minimal mock SQLAlchemy engine with two context managers.

    The engine returns ``source_id`` from source_catalog and an empty
    ``fetchall`` (no existing dates) for every subsequent query.
    """
    engine = MagicMock()
    conn = MagicMock()

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # Default: source_catalog lookup returns (source_id,), fetchall empty.
    conn.execute.return_value.fetchone.return_value = (source_id,)
    conn.execute.return_value.fetchall.return_value = []
    return engine, conn


@pytest.fixture
def mock_engine() -> tuple[MagicMock, MagicMock]:
    return _mock_engine(source_id=7)


# ---------------------------------------------------------------------------
# Canned HTML fixtures
# ---------------------------------------------------------------------------


DREWRY_HTML = """
<html><body>
<h1>World Container Index</h1>
<table>
  <thead>
    <tr>
      <th>Date</th>
      <th>Composite</th>
      <th>Shanghai - Rotterdam</th>
      <th>Shanghai - Los Angeles</th>
      <th>Shanghai - Genoa</th>
      <th>Shanghai - New York</th>
      <th>Rotterdam - Shanghai</th>
      <th>Rotterdam - New York</th>
      <th>New York - Rotterdam</th>
      <th>Los Angeles - Shanghai</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2026-04-03</td>
      <td>$2,410</td>
      <td>$2,800</td>
      <td>$3,100</td>
      <td>$3,450</td>
      <td>$3,900</td>
      <td>$540</td>
      <td>$1,650</td>
      <td>$820</td>
      <td>$680</td>
    </tr>
    <tr>
      <td>2026-03-27</td>
      <td>$2,380</td>
      <td>$2,770</td>
      <td>$3,080</td>
      <td>$3,420</td>
      <td>$3,880</td>
      <td>$535</td>
      <td>$1,640</td>
      <td>$815</td>
      <td>$675</td>
    </tr>
  </tbody>
</table>
</body></html>
"""


SCFI_HTML = """
<html><body>
<h2>SCFI Weekly Update</h2>
<table>
  <tr><th>Date</th><th>SCFI Comprehensive Index</th></tr>
  <tr><td>2026-04-03</td><td>1,842.57</td></tr>
  <tr><td>2026-03-27</td><td>1,820.11</td></tr>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# Dataclass tests
# ---------------------------------------------------------------------------


class TestContainerFreightSnapshot:
    def test_roundtrip_empty_routes(self) -> None:
        snap = ContainerFreightSnapshot(
            week_end=date(2026, 4, 3),
            wci_composite_usd=2410.0,
            scfi_composite=1842.57,
        )
        assert snap.week_end == date(2026, 4, 3)
        assert snap.wci_composite_usd == 2410.0
        assert snap.scfi_composite == 1842.57
        assert snap.wci_routes == {}

    def test_roundtrip_populated_routes(self) -> None:
        routes = {"shanghai_rotterdam": 2800.0, "shanghai_la": 3100.0}
        snap = ContainerFreightSnapshot(
            week_end=date(2026, 4, 3),
            wci_composite_usd=2410.0,
            scfi_composite=None,
            wci_routes=routes,
        )
        assert snap.wci_routes["shanghai_rotterdam"] == 2800.0
        assert snap.scfi_composite is None
        # frozen dataclass — direct attribute assignment must raise
        with pytest.raises(Exception):
            snap.week_end = date(2026, 1, 1)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Pure HTML parser tests
# ---------------------------------------------------------------------------


class TestDrewryHtmlParser:
    def test_parses_composite_and_routes(self) -> None:
        snaps = _parse_drewry_wci_html(DREWRY_HTML)
        assert len(snaps) == 2
        first = snaps[0]
        assert first.week_end == date(2026, 4, 3)
        assert first.wci_composite_usd == 2410.0
        # All 8 routes should be captured and keyed by lowercase+underscore slug.
        assert set(first.wci_routes.keys()) == {r.slug for r in DREWRY_ROUTES}
        assert first.wci_routes["shanghai_rotterdam"] == 2800.0
        assert first.wci_routes["la_shanghai"] == 680.0
        assert all(k == k.lower() and " " not in k for k in first.wci_routes)

    def test_empty_html_returns_empty_list(self) -> None:
        assert _parse_drewry_wci_html("") == []
        assert _parse_drewry_wci_html("<html><body>no tables</body></html>") == []


class TestScfiHtmlParser:
    def test_parses_scfi_table(self) -> None:
        snaps = _parse_scfi_html(SCFI_HTML)
        assert len(snaps) == 2
        first = snaps[0]
        assert first.week_end == date(2026, 4, 3)
        assert first.scfi_composite == pytest.approx(1842.57)
        assert first.wci_composite_usd is None
        assert first.wci_routes == {}

    def test_scfi_empty_html(self) -> None:
        assert _parse_scfi_html("") == []
        assert _parse_scfi_html("<html></html>") == []


# ---------------------------------------------------------------------------
# Fallback walk tests (FRED → akshare → HTML)
# ---------------------------------------------------------------------------


class TestFallbackWalk:
    def test_fred_primary_path(self, mock_engine) -> None:
        engine, _ = mock_engine
        fred_snaps = [
            ContainerFreightSnapshot(
                week_end=date(2026, 3, 27),
                wci_composite_usd=2380.0,
                scfi_composite=None,
            ),
            ContainerFreightSnapshot(
                week_end=date(2026, 4, 3),
                wci_composite_usd=2410.0,
                scfi_composite=None,
            ),
        ]

        with patch(
            "ingestion.altdata.container_freight._try_fred_sources",
            return_value=fred_snaps,
        ), patch(
            "ingestion.altdata.container_freight._try_akshare_sources"
        ) as ak_mock, patch(
            "ingestion.altdata.container_freight._try_html_sources"
        ) as html_mock:
            result = run_container_freight_puller(engine)

        assert result["source"] == "fred"
        assert result["fetched"] == 2
        assert result["inserted"] == 2
        # FRED won — fallbacks must NOT be called.
        ak_mock.assert_not_called()
        html_mock.assert_not_called()

    def test_akshare_fallback_when_fred_empty(self, mock_engine) -> None:
        engine, _ = mock_engine
        ak_snaps = [
            ContainerFreightSnapshot(
                week_end=date(2026, 4, 3),
                wci_composite_usd=2410.0,
                scfi_composite=1842.57,
            ),
        ]
        with patch(
            "ingestion.altdata.container_freight._try_fred_sources",
            return_value=[],
        ), patch(
            "ingestion.altdata.container_freight._try_akshare_sources",
            return_value=ak_snaps,
        ), patch(
            "ingestion.altdata.container_freight._try_html_sources"
        ) as html_mock:
            result = run_container_freight_puller(engine)

        assert result["source"] == "akshare"
        assert result["fetched"] == 1
        assert result["inserted"] == 2  # WCI + SCFI both written
        html_mock.assert_not_called()

    def test_html_fallback_when_fred_and_akshare_empty(self, mock_engine) -> None:
        engine, _ = mock_engine

        def _fake_http_get(url: str) -> str | None:
            if "drewry" in url:
                return DREWRY_HTML
            if "sse.net.cn" in url:
                return SCFI_HTML
            return None

        with patch(
            "ingestion.altdata.container_freight._try_fred_sources",
            return_value=[],
        ), patch(
            "ingestion.altdata.container_freight._try_akshare_sources",
            return_value=[],
        ), patch(
            "ingestion.altdata.container_freight._http_get",
            side_effect=_fake_http_get,
        ):
            result = run_container_freight_puller(engine)

        assert result["source"] == "html"
        # 2 weeks x (1 composite + 8 routes) + 2 SCFI writes = 20
        assert result["fetched"] == 2
        assert result["inserted"] == 20

    def test_all_sources_fail_no_crash(self, mock_engine) -> None:
        engine, _ = mock_engine
        with patch(
            "ingestion.altdata.container_freight._try_fred_sources",
            return_value=[],
        ), patch(
            "ingestion.altdata.container_freight._try_akshare_sources",
            return_value=[],
        ), patch(
            "ingestion.altdata.container_freight._http_get",
            return_value=None,
        ):
            result = run_container_freight_puller(engine)

        assert result == {"fetched": 0, "inserted": 0, "source": "none"}


# ---------------------------------------------------------------------------
# Idempotency + partial-data insert tests
# ---------------------------------------------------------------------------


class TestSaveToDb:
    def test_idempotent_rerun_skips_existing(self, mock_engine) -> None:
        engine, conn = mock_engine
        snaps = [
            ContainerFreightSnapshot(
                week_end=date(2026, 4, 3),
                wci_composite_usd=2410.0,
                scfi_composite=1842.57,
                wci_routes={},
            ),
        ]

        # First call: _get_existing_dates returns empty set → both inserted.
        conn.execute.return_value.fetchall.return_value = []
        puller = ContainerFreightPuller(db_engine=engine)
        puller.last_source = "html"
        first = puller.save_to_db(snaps)
        assert first == 2

        # Second call: pretend raw_series now already has these rows.
        conn.execute.return_value.fetchall.return_value = [(date(2026, 4, 3),)]
        second = puller.save_to_db(snaps)
        assert second == 0  # idempotent — zero duplicates written

    def test_partial_data_wci_only(self, mock_engine) -> None:
        engine, conn = mock_engine
        conn.execute.return_value.fetchall.return_value = []

        snaps = [
            ContainerFreightSnapshot(
                week_end=date(2026, 4, 3),
                wci_composite_usd=2410.0,
                scfi_composite=None,  # SCFI missing
                wci_routes={},
            ),
        ]
        puller = ContainerFreightPuller(db_engine=engine)
        puller.last_source = "html"
        inserted = puller.save_to_db(snaps)
        assert inserted == 1  # only WCI written, SCFI skipped because None

    def test_route_breakdown_slugs_lowercase_underscore(self, mock_engine) -> None:
        engine, conn = mock_engine
        conn.execute.return_value.fetchall.return_value = []

        snaps = _parse_drewry_wci_html(DREWRY_HTML)
        assert snaps, "parser should return snapshots"
        # Every route slug must be lowercase + underscores only
        for s in snaps:
            for slug in s.wci_routes.keys():
                assert slug == slug.lower()
                assert " " not in slug
                assert "-" not in slug

        puller = ContainerFreightPuller(db_engine=engine)
        puller.last_source = "html"
        # 2 weeks * (1 composite + 8 routes) = 18 writes
        assert puller.save_to_db(snaps) == 18


# ---------------------------------------------------------------------------
# Constants / sanity
# ---------------------------------------------------------------------------


class TestConstants:
    def test_drewry_routes_are_exactly_eight(self) -> None:
        assert len(DREWRY_ROUTES) == 8
        slugs = {r.slug for r in DREWRY_ROUTES}
        assert len(slugs) == 8  # all unique

    def test_urls_are_public(self) -> None:
        assert DREWRY_WCI_URL.startswith("https://www.drewry.co.uk/")
        assert SCFI_URL.startswith("https://en.sse.net.cn/")

    def test_fred_candidate_series_nonempty(self) -> None:
        assert "IR14270" in FRED_CANDIDATE_SERIES

    def test_series_namespaces(self) -> None:
        assert SERIES_WCI_COMPOSITE == "freight:wci_composite_usd"
        assert SERIES_SCFI_COMPOSITE == "freight:scfi_composite"
        assert SERIES_WCI_ROUTE_PREFIX == "freight:wci_route:"
