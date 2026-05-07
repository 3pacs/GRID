"""Unit tests for ingestion/altdata/semi_book_to_bill.py (CAT-89)."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.semi_book_to_bill import (
    FRED_SERIES_IDS,
    SERIES_LABELS,
    SemiBookToBill,
    SemiBookToBillPuller,
    _find_semi_table,
    _parse_month_label,
    _parse_number,
    run_semi_book_to_bill_puller,
)


# ─────────────────────────────────────────────────────────────────────
# Fake engine: tracks every raw_series INSERT so tests can assert on
# inserted rows and detect duplicates (idempotency).
# ─────────────────────────────────────────────────────────────────────


class _FakeConn:
    def __init__(self, store: dict[tuple[str, date], dict[str, Any]]) -> None:
        self._store = store

    def execute(self, statement: Any, params: dict[str, Any] | None = None):  # noqa: ANN401
        sql = str(statement).lower()
        result = MagicMock()

        if "select id from source_catalog" in sql:
            row = MagicMock()
            row.__getitem__ = lambda _self, _idx: 42
            result.fetchone.return_value = row
            return result

        if "select distinct obs_date from raw_series" in sql:
            series_id = (params or {}).get("sid")
            rows = [
                (od,) for (sid, od) in self._store.keys() if sid == series_id
            ]
            result.fetchall.return_value = rows
            return result

        if "insert into raw_series" in sql:
            assert params is not None
            key = (params["sid"], params["od"])
            # Mirror the DB uniqueness contract: reject exact duplicates.
            if key in self._store:
                raise AssertionError(
                    f"duplicate insert for {key} (test harness)"
                )
            self._store[key] = {
                "value": params["val"],
                "payload": params["payload"],
            }
            return result

        # Unknown statements: return a harmless mock.
        result.fetchone.return_value = None
        result.fetchall.return_value = []
        return result


class _FakeEngine:
    def __init__(self) -> None:
        self.store: dict[tuple[str, date], dict[str, Any]] = {}

    @contextmanager
    def connect(self):
        yield _FakeConn(self.store)

    @contextmanager
    def begin(self):
        yield _FakeConn(self.store)


@pytest.fixture
def engine() -> _FakeEngine:
    return _FakeEngine()


@pytest.fixture
def puller(engine: _FakeEngine) -> SemiBookToBillPuller:
    return SemiBookToBillPuller(engine, fred_api_key="fake-key")  # type: ignore[arg-type]


# ─────────────────────────────────────────────────────────────────────
# Dataclass + ratio computation
# ─────────────────────────────────────────────────────────────────────


class TestSemiBookToBillDataclass:
    def test_construction_and_ratio_greater_than_one(self) -> None:
        ob = SemiBookToBill.from_inputs(
            month_end=date(2026, 3, 31),
            bookings=2500.0,
            billings=2000.0,
        )
        assert ob.month_end == date(2026, 3, 31)
        assert ob.bookings_usd_m == 2500.0
        assert ob.billings_usd_m == 2000.0
        assert ob.ratio is not None
        assert ob.ratio == pytest.approx(1.25)

    def test_ratio_less_than_one(self) -> None:
        ob = SemiBookToBill.from_inputs(
            month_end=date(2026, 1, 31), bookings=900.0, billings=1000.0
        )
        assert ob.ratio == pytest.approx(0.9)

    def test_ratio_exactly_one(self) -> None:
        ob = SemiBookToBill.from_inputs(
            month_end=date(2026, 2, 28), bookings=1500.0, billings=1500.0
        )
        assert ob.ratio == pytest.approx(1.0)

    def test_ratio_none_when_billings_zero(self) -> None:
        ob = SemiBookToBill.from_inputs(
            month_end=date(2026, 2, 28), bookings=500.0, billings=0.0
        )
        assert ob.ratio is None

    def test_ratio_none_when_either_missing(self) -> None:
        ob = SemiBookToBill.from_inputs(
            month_end=date(2026, 2, 28), bookings=None, billings=1500.0
        )
        assert ob.ratio is None
        assert ob.bookings_usd_m is None
        assert ob.billings_usd_m == 1500.0

    def test_frozen_immutable(self) -> None:
        ob = SemiBookToBill.from_inputs(
            month_end=date(2026, 2, 28), bookings=1.0, billings=1.0
        )
        with pytest.raises(Exception):
            ob.ratio = 99.0  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────────────
# Module-level parser helpers
# ─────────────────────────────────────────────────────────────────────


class TestParserHelpers:
    def test_parse_number_dollar_comma(self) -> None:
        assert _parse_number("$2,817.4") == pytest.approx(2817.4)

    def test_parse_number_plain(self) -> None:
        assert _parse_number("1.08") == pytest.approx(1.08)

    def test_parse_number_dash(self) -> None:
        assert _parse_number("—") is None
        assert _parse_number("-") is None
        assert _parse_number("") is None

    def test_parse_number_garbage(self) -> None:
        assert _parse_number("oops") is None

    def test_parse_month_label_short(self) -> None:
        assert _parse_month_label("Jan 2026") == date(2026, 1, 31)

    def test_parse_month_label_long(self) -> None:
        assert _parse_month_label("February 2025") == date(2025, 2, 28)

    def test_parse_month_label_december(self) -> None:
        assert _parse_month_label("Dec 2024") == date(2024, 12, 31)

    def test_parse_month_label_invalid(self) -> None:
        assert _parse_month_label("Header") is None
        assert _parse_month_label("") is None
        assert _parse_month_label("Foo 2026") is None


# ─────────────────────────────────────────────────────────────────────
# HTML scrape path (canned snippet, no network)
# ─────────────────────────────────────────────────────────────────────


CANNED_SEMI_HTML = """
<html><body>
<h1>North American Billings Report</h1>
<table>
  <thead>
    <tr><th>Month</th><th>Bookings (3-mo avg, $M)</th><th>Billings (3-mo avg, $M)</th><th>Book-to-Bill</th></tr>
  </thead>
  <tbody>
    <tr><td>Jan 2026</td><td>$2,900.1</td><td>$2,700.0</td><td>1.07</td></tr>
    <tr><td>Feb 2026</td><td>$3,100.5</td><td>$2,800.0</td><td>1.11</td></tr>
    <tr><td>Mar 2026</td><td>$2,500.0</td><td>$2,600.0</td><td>0.96</td></tr>
  </tbody>
</table>
<table>
  <tr><th>Unrelated</th><th>Table</th></tr>
  <tr><td>Foo</td><td>Bar</td></tr>
</table>
</body></html>
"""


class TestHtmlScraper:
    def test_scrape_extracts_three_months(self, puller: SemiBookToBillPuller) -> None:
        rows = puller._scrape_semi_html(html=CANNED_SEMI_HTML)
        assert len(rows) == 3
        assert rows[0].month_end == date(2026, 1, 31)
        assert rows[0].bookings_usd_m == pytest.approx(2900.1)
        assert rows[0].billings_usd_m == pytest.approx(2700.0)
        # Ratio is recomputed from raw numbers, not the printed column.
        assert rows[0].ratio == pytest.approx(2900.1 / 2700.0)

    def test_scrape_boundary_ratios(self, puller: SemiBookToBillPuller) -> None:
        rows = puller._scrape_semi_html(html=CANNED_SEMI_HTML)
        assert rows[1].ratio is not None and rows[1].ratio > 1.0
        assert rows[2].ratio is not None and rows[2].ratio < 1.0

    def test_scrape_handles_empty_html(self, puller: SemiBookToBillPuller) -> None:
        assert puller._scrape_semi_html(html="<html><body></body></html>") == []

    def test_find_semi_table_picks_right_one(self) -> None:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(CANNED_SEMI_HTML, "html.parser")
        tbl = _find_semi_table(soup)
        assert tbl is not None
        header = tbl.find("th").get_text(strip=True).lower()
        assert "month" in header


# ─────────────────────────────────────────────────────────────────────
# FRED path — mocked requests.get
# ─────────────────────────────────────────────────────────────────────


def _fred_response(pairs: list[tuple[str, str]]) -> MagicMock:
    """Build a fake FRED JSON response with the given date/value pairs."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "observations": [{"date": d, "value": v} for d, v in pairs]
    }
    return resp


def _fred_side_effect(
    bookings_rows: list[tuple[str, str]],
    billings_rows: list[tuple[str, str]],
) -> Any:
    """Return a requests.get side-effect that routes by series_id param."""

    def _side_effect(url: str, params: dict[str, Any] | None = None, **_: Any):
        series_id = (params or {}).get("series_id")
        if series_id == FRED_SERIES_IDS["bookings"]:
            return _fred_response(bookings_rows)
        if series_id == FRED_SERIES_IDS["billings"]:
            return _fred_response(billings_rows)
        raise AssertionError(f"unexpected series_id {series_id}")

    return _side_effect


class TestFredPath:
    def test_run_puller_with_mocked_fred(
        self, engine: _FakeEngine
    ) -> None:
        bookings = [("2026-01-01", "2500"), ("2026-02-01", "2600")]
        billings = [("2026-01-01", "2000"), ("2026-02-01", "2500")]

        with patch(
            "ingestion.altdata.semi_book_to_bill.requests.get",
            side_effect=_fred_side_effect(bookings, billings),
        ):
            result = run_semi_book_to_bill_puller(engine, fred_api_key="k")  # type: ignore[arg-type]

        assert result["source"] == "fred"
        assert result["fetched"] == 2
        # 2 months x 3 labels (bookings/billings/ratio) = 6 rows.
        assert result["inserted"] == 6

        # Verify rows landed in the expected series_ids.
        series_ids = {sid for (sid, _od) in engine.store.keys()}
        for label in SERIES_LABELS:
            assert f"semi:{label}" in series_ids

    def test_fred_path_partial_data_inserts_what_exists(
        self, engine: _FakeEngine
    ) -> None:
        """Bookings present, billings missing → ratio None, still insert bookings."""
        bookings = [("2026-03-01", "3000")]
        billings: list[tuple[str, str]] = []  # FRED returns nothing

        with patch(
            "ingestion.altdata.semi_book_to_bill.requests.get",
            side_effect=_fred_side_effect(bookings, billings),
        ):
            result = run_semi_book_to_bill_puller(engine, fred_api_key="k")  # type: ignore[arg-type]

        assert result["source"] == "fred"
        assert result["fetched"] == 1
        # Only bookings inserted — billings and ratio are None.
        assert result["inserted"] == 1
        keys = list(engine.store.keys())
        assert ("semi:bookings", date(2026, 3, 1)) in keys
        assert ("semi:billings", date(2026, 3, 1)) not in keys
        assert ("semi:ratio", date(2026, 3, 1)) not in keys

    def test_idempotent_rerun_no_duplicates(self, engine: _FakeEngine) -> None:
        bookings = [("2026-01-01", "2500")]
        billings = [("2026-01-01", "2000")]

        with patch(
            "ingestion.altdata.semi_book_to_bill.requests.get",
            side_effect=_fred_side_effect(bookings, billings),
        ):
            first = run_semi_book_to_bill_puller(engine, fred_api_key="k")  # type: ignore[arg-type]
            second = run_semi_book_to_bill_puller(engine, fred_api_key="k")  # type: ignore[arg-type]

        assert first["inserted"] == 3
        assert second["inserted"] == 0  # Dedup caught everything
        # Still exactly 3 rows in the store.
        assert len(engine.store) == 3


# ─────────────────────────────────────────────────────────────────────
# Fallback path: FRED missing → HTML scrape
# ─────────────────────────────────────────────────────────────────────


class TestFallbackPath:
    def test_no_fred_key_uses_html(
        self, engine: _FakeEngine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FRED_API_KEY", raising=False)

        def _fake_get(url: str, **_: Any) -> MagicMock:
            # Only the SEMI HTML page should be fetched in this path.
            assert "semi.org" in url
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.text = CANNED_SEMI_HTML
            return resp

        with patch(
            "ingestion.altdata.semi_book_to_bill.requests.get",
            side_effect=_fake_get,
        ):
            result = run_semi_book_to_bill_puller(engine, fred_api_key="")  # type: ignore[arg-type]

        assert result["source"] == "html"
        assert result["fetched"] == 3
        # 3 months x 3 labels = 9 rows, all present in the canned snippet.
        assert result["inserted"] == 9

    def test_both_paths_failing_returns_zero(
        self, engine: _FakeEngine
    ) -> None:
        """FRED raises, HTML raises → zero-row graceful result, no crash."""

        def _always_fail(*_args: Any, **_kwargs: Any) -> MagicMock:
            raise ConnectionError("network down")

        with patch(
            "ingestion.altdata.semi_book_to_bill.requests.get",
            side_effect=_always_fail,
        ):
            result = run_semi_book_to_bill_puller(engine, fred_api_key="k")  # type: ignore[arg-type]

        assert result == {"fetched": 0, "inserted": 0, "source": "none"}
        assert engine.store == {}

    def test_fred_empty_falls_back_to_html(
        self, engine: _FakeEngine
    ) -> None:
        """FRED returns 0 rows → puller transparently uses HTML path."""

        def _side_effect(url: str, params: dict[str, Any] | None = None, **_: Any) -> MagicMock:
            if "stlouisfed" in url:
                return _fred_response([])
            if "semi.org" in url:
                resp = MagicMock()
                resp.raise_for_status = MagicMock()
                resp.text = CANNED_SEMI_HTML
                return resp
            raise AssertionError(f"unexpected URL {url}")

        with patch(
            "ingestion.altdata.semi_book_to_bill.requests.get",
            side_effect=_side_effect,
        ):
            result = run_semi_book_to_bill_puller(engine, fred_api_key="k")  # type: ignore[arg-type]

        assert result["source"] == "html"
        assert result["fetched"] == 3
        assert result["inserted"] == 9
