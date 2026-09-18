"""Neither Finviz writer may turn a failed parse into a SUCCESS numeric observation (B-M17).

`raw_series.value` is `DOUBLE PRECISION NOT NULL` and `pull_status` is
`CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED'))`, so the table can only
hold numeric observations. Two writers used to coerce anything `_parse_finviz_value`
could not parse - `Sector`, `Industry`, an `"N/A"` cell - to `0.0` and write it with
`pull_status='SUCCESS'`:

  * `api/routers/dad.py::_store_finviz_snapshot`  (user-triggered, `refresh_finviz=true`)
  * `ingestion/altdata/finviz_scraper.py::FinvizScraperPuller.pull_ticker`  (daily cron)

Both now write no row at all for a non-numeric field and count it as
`skipped_text_fields`. A real `0` still gets a row: zero is a measurement.

Everything here runs against recording fakes. No database, no network, no browser.
"""

from __future__ import annotations

import inspect
import json
import pathlib
import sys
import types
from datetime import date, datetime, timezone
from typing import Any

import pytest

from api.routers.dad import (
    _grid_decision_stack,
    _num_field,
    _read_finviz_rows,
    _store_finviz_snapshot,
)

# `ingestion/altdata/finviz_scraper.py` imports Playwright at module scope and the
# browser is never launched by these tests, so a stub keeps the module importable
# on a machine without Playwright installed.
if "playwright" not in sys.modules:  # pragma: no cover - environment dependent
    _pw = types.ModuleType("playwright")
    _pw_sync = types.ModuleType("playwright.sync_api")
    _pw_sync.sync_playwright = lambda: None
    _pw_sync.Browser = object
    _pw_sync.Page = object
    _pw.sync_api = _pw_sync
    sys.modules["playwright"] = _pw
    sys.modules["playwright.sync_api"] = _pw_sync

from ingestion.altdata.finviz_scraper import (  # noqa: E402
    FIELDS_OF_INTEREST,
    TEXT_FIELDS,
    FinvizScraperPuller,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
DAD_PATH = REPO_ROOT / "api" / "routers" / "dad.py"
SCRAPER_PATH = REPO_ROOT / "ingestion" / "altdata" / "finviz_scraper.py"

# One text field, one always-text field, one numeric field whose cell is unparsable,
# one ordinary numeric field, and one genuine zero.
PAIRS: dict[str, str] = {
    "Sector": "Technology",
    "Industry": "Software - Infrastructure",
    "Debt/Eq": "N/A",
    "P/E": "28.40",
    "ROE": "0.00",
    "Beta": "-",  # Finviz's own "missing" marker; already filtered before this change
}


# ── recording fakes ───────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, row: Any = None, rowcount: int = 1) -> None:
        self._row = row
        self.rowcount = rowcount

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list[Any]:
        return []


class _RecordingConn:
    """Records every statement instead of talking to Postgres."""

    def __init__(self, source_id: int = 7) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._source_id = source_id

    def execute(self, statement: Any, params: Any = None) -> _FakeResult:
        sql = str(statement)
        self.calls.append((sql, dict(params or {})))
        if "source_catalog" in sql and "SELECT" in sql.upper():
            return _FakeResult(row=(self._source_id,))
        return _FakeResult(row=None, rowcount=1)

    def __enter__(self) -> _RecordingConn:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


class _RecordingEngine:
    def __init__(self) -> None:
        self.conn = _RecordingConn()

    def connect(self) -> _RecordingConn:
        return self.conn

    def begin(self) -> _RecordingConn:
        return self.conn


def _raw_series_inserts(conn: _RecordingConn) -> list[tuple[str, dict[str, Any]]]:
    return [(sql, params) for sql, params in conn.calls if "INSERT INTO raw_series" in sql]


# ── writer A: api/routers/dad.py ──────────────────────────────────────────────


@pytest.fixture()
def dad_write() -> tuple[dict[str, int], dict[str, dict[str, Any]], list[str]]:
    engine = _RecordingEngine()
    summary = _store_finviz_snapshot(engine, "RXT", PAIRS)
    inserts = _raw_series_inserts(engine.conn)
    by_series = {params["series_id"]: params for _sql, params in inserts}
    statements = [sql for sql, _params in inserts]
    return summary, by_series, statements


def test_dad_writer_skips_text_fields_entirely(dad_write) -> None:
    _summary, by_series, _sql = dad_write
    assert "finviz.RXT.sector" not in by_series
    assert "finviz.RXT.industry" not in by_series


def test_dad_writer_skips_an_unparsable_numeric_field(dad_write) -> None:
    _summary, by_series, _sql = dad_write
    # "N/A" is not "-", so it used to survive the None filter and land as 0.0.
    assert "finviz.RXT.debt_equity" not in by_series


def test_dad_writer_still_writes_a_real_number_as_success(dad_write) -> None:
    _summary, by_series, statements = dad_write
    assert by_series["finviz.RXT.pe_ratio"]["value"] == pytest.approx(28.40)
    # The writer's INSERT is the only place the literal status is set.
    assert all("'SUCCESS'" in sql for sql in statements)


def test_dad_writer_writes_a_genuine_zero_as_zero(dad_write) -> None:
    _summary, by_series, _sql = dad_write
    # 0 is a measurement. ROE of 0.00% must survive as 0.0 with SUCCESS.
    roe = by_series["finviz.RXT.roe"]
    assert roe["value"] == 0.0
    assert json.loads(roe["payload"])["parsed"] == 0.0


def test_dad_writer_reports_what_it_skipped(dad_write) -> None:
    summary, by_series, _sql = dad_write
    assert summary["skipped_text_fields"] == 3  # Sector, Industry, Debt/Eq
    assert set(by_series) == {"finviz.RXT.pe_ratio", "finviz.RXT.roe"}


def test_dad_writer_never_writes_a_zero_it_did_not_measure(dad_write) -> None:
    _summary, by_series, _sql = dad_write
    zero_rows = {sid for sid, params in by_series.items() if params["value"] == 0.0}
    assert zero_rows == {"finviz.RXT.roe"}


# ── writer B: ingestion/altdata/finviz_scraper.py (daily scheduled puller) ────


def _puller(engine: _RecordingEngine, pairs: dict[str, str]) -> FinvizScraperPuller:
    puller = FinvizScraperPuller.__new__(FinvizScraperPuller)
    puller.engine = engine
    puller.source_id = 7
    puller._browser = None
    puller._playwright_ctx = None
    puller._fetch_page = lambda ticker: "<html></html>"  # type: ignore[method-assign]
    puller._parse_snapshot_table = lambda html: dict(pairs)  # type: ignore[method-assign]
    puller._get_existing_dates = lambda series_id, conn: set()  # type: ignore[method-assign]
    return puller


@pytest.fixture()
def scraper_write() -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    engine = _RecordingEngine()
    result = _puller(engine, PAIRS).pull_ticker("RXT")
    by_series = {params["sid"]: params for _sql, params in _raw_series_inserts(engine.conn)}
    return result, by_series


def test_scraper_text_fields_are_in_fields_of_interest_but_never_written(scraper_write) -> None:
    _result, by_series = scraper_write
    # The puller still reads them - it simply refuses to call them observations.
    assert {"Sector", "Industry"} <= set(FIELDS_OF_INTEREST)
    assert {"Sector", "Industry"} <= TEXT_FIELDS
    assert "finviz.RXT.sector" not in by_series
    assert "finviz.RXT.industry" not in by_series


def test_scraper_skips_an_unparsable_numeric_field(scraper_write) -> None:
    _result, by_series = scraper_write
    assert "finviz.RXT.debt_equity" not in by_series


def test_scraper_still_writes_a_real_number_as_success(scraper_write) -> None:
    _result, by_series = scraper_write
    row = by_series["finviz.RXT.pe_ratio"]
    assert row["val"] == pytest.approx(28.40)
    assert row["status"] == "SUCCESS"


def test_scraper_writes_a_genuine_zero_as_zero(scraper_write) -> None:
    _result, by_series = scraper_write
    row = by_series["finviz.RXT.roe"]
    assert row["val"] == 0.0
    assert row["status"] == "SUCCESS"
    assert json.loads(row["payload"])["parsed"] == 0.0


def test_scraper_pull_summary_counts_skipped_text_fields(scraper_write) -> None:
    result, by_series = scraper_write
    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 2
    assert result["skipped_text_fields"] == 3  # Sector, Industry, Debt/Eq
    assert set(by_series) == {"finviz.RXT.pe_ratio", "finviz.RXT.roe"}


def test_scraper_never_writes_a_partial_or_failed_row_for_text(scraper_write) -> None:
    _result, by_series = scraper_write
    # The fix is "no row", not "a PARTIAL row with a 0 placeholder".
    assert all(params["status"] == "SUCCESS" for params in by_series.values())


# ── read side: api/routers/dad.py ─────────────────────────────────────────────


class _RowReadingConn:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def execute(self, statement: Any, params: Any = None) -> _FakeResult:
        result = _FakeResult()
        result.fetchall = lambda: self._rows  # type: ignore[method-assign]
        return result

    def __enter__(self) -> _RowReadingConn:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


class _RowReadingEngine:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def connect(self) -> _RowReadingConn:
        return _RowReadingConn(self._rows)


def _legacy_rows() -> list[tuple[Any, ...]]:
    """Rows already in raw_series from before the fix. They are never rewritten."""
    pulled = datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)
    obs = date(2026, 9, 16)
    return [
        # dad.py writer: text coerced to 0.0, stamped SUCCESS
        ("finviz.RXT.sector", obs, pulled, 0.0,
         json.dumps({"label": "Sector", "group": "company", "raw_value": "Technology", "parsed": "Technology"})),
        # dad.py writer: a real number
        ("finviz.RXT.pe_ratio", obs, pulled, 28.4,
         json.dumps({"label": "P/E", "group": "valuation", "raw_value": "28.40", "parsed": 28.4})),
        # scraper writer: payload["parsed"] used to be str(parsed)
        ("finviz.RXT.roe", obs, pulled, 0.0,
         json.dumps({"field": "ROE", "raw_value": "0.00", "parsed": "0.0"})),
        # dad.py writer: an unparsable numeric cell coerced to 0.0
        ("finviz.RXT.debt_equity", obs, pulled, 0.0,
         json.dumps({"label": "Debt/Eq", "group": "risk", "raw_value": "N/A", "parsed": "N/A"})),
    ]


def test_legacy_text_rows_are_read_as_text_not_as_zero() -> None:
    fields = _read_finviz_rows(_RowReadingEngine(_legacy_rows()), "RXT")["fields"]
    sector = fields["sector"]
    assert sector["numeric_value"] is None
    assert sector["parsed"] is None
    assert sector["value_kind"] == "text"
    assert sector["raw_value"] == "Technology"
    assert sector["text_value"] == "Technology"


def test_legacy_unparsable_numeric_row_is_read_as_text_not_as_zero() -> None:
    fields = _read_finviz_rows(_RowReadingEngine(_legacy_rows()), "RXT")["fields"]
    debt = fields["debt_equity"]
    assert debt["numeric_value"] is None
    assert debt["parsed"] is None
    assert debt["value_kind"] == "text"


def test_legacy_numeric_rows_survive_including_a_stringified_zero() -> None:
    fields = _read_finviz_rows(_RowReadingEngine(_legacy_rows()), "RXT")["fields"]
    assert fields["pe_ratio"]["numeric_value"] == pytest.approx(28.4)
    assert fields["pe_ratio"]["value_kind"] == "numeric"
    # The scraper's str(parsed) form must still read as the number it is.
    assert fields["roe"]["numeric_value"] == 0.0
    assert fields["roe"]["parsed"] == 0.0
    assert fields["roe"]["value_kind"] == "numeric"


@pytest.mark.parametrize(
    "item",
    [
        {"parsed": "N/A", "numeric_value": 0.0},
        {"parsed": "Technology", "numeric_value": 0.0},
        {"parsed": None, "numeric_value": 0.0, "value_kind": "text"},
    ],
)
def test_num_field_returns_none_for_an_unparsable_field(item: dict[str, Any]) -> None:
    assert _num_field({"fields": {"debt_equity": item}}, "debt_equity") is None


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        ({"parsed": 0.4}, 0.4),
        ({"parsed": 0.0}, 0.0),          # a measured zero is a number
        ({"parsed": "0.0"}, 0.0),        # legacy scraper payload
        ({"parsed": None, "numeric_value": 0.4}, 0.4),
    ],
)
def test_num_field_keeps_real_numbers(item: dict[str, Any], expected: float) -> None:
    assert _num_field({"fields": {"debt_equity": item}}, "debt_equity") == pytest.approx(expected)


# ── read side: the gold / decision stack must not score an unparsable field ──

_GRID = {"metrics": {"return_1y_pct": 32.0, "pct_from_52w_high": -4.0}, "source_freshness": []}
_SIGNALS = {"signal_sources": [], "tradingview_signals": [], "regime": None}
_GOLD = {"heuristic_score": None}


def _finviz(debt_field: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ready",
        "field_count": 1,
        "freshness": {"state": "fresh", "label": "fresh"},
        "fields": {"debt_equity": debt_field},
    }


def test_unparsable_debt_equity_scores_nothing() -> None:
    measured = _grid_decision_stack(None, _GOLD, _GRID, _finviz({"parsed": 0.4}), None, _SIGNALS)
    unparsable = _grid_decision_stack(
        None, _GOLD, _GRID, _finviz({"parsed": "N/A", "numeric_value": 0.0}), None, _SIGNALS
    )

    def _finviz_points(decision: dict[str, Any]) -> float:
        return next(c["points"] for c in decision["cards"] if c["source"] == "Finviz fundamentals")

    # The +4 finviz_debt_equity_0_to_1 award must not fire on text.
    assert _finviz_points(measured) - _finviz_points(unparsable) == 4
    assert measured["heuristic_score"] > unparsable["heuristic_score"]


def test_a_measured_zero_debt_equity_still_scores() -> None:
    zero = _grid_decision_stack(None, _GOLD, _GRID, _finviz({"parsed": 0.0}), None, _SIGNALS)
    card = next(c for c in zero["cards"] if c["source"] == "Finviz fundamentals")
    assert card["inputs"]["debt_equity"] == 0.0
    assert "debt_equity" not in card["skipped_fields"]


def test_decision_stack_publishes_the_terms_it_skipped() -> None:
    unparsable = _grid_decision_stack(
        None, _GOLD, _GRID, _finviz({"parsed": "N/A", "numeric_value": 0.0}), None, _SIGNALS
    )
    card = next(c for c in unparsable["cards"] if c["source"] == "Finviz fundamentals")
    assert card["inputs"]["debt_equity"] is None
    assert "debt_equity" in card["skipped_fields"]
    assert "debt_equity" in unparsable["skipped_terms"]["finviz"]


# ── source guard ──────────────────────────────────────────────────────────────


def test_neither_writer_coerces_a_failed_parse_to_zero() -> None:
    """Scoped guard: no `else 0.0` fallback in either raw_series writer."""
    writers = {
        "api/routers/dad.py::_store_finviz_snapshot": inspect.getsource(_store_finviz_snapshot),
        "ingestion/altdata/finviz_scraper.py::pull_ticker": inspect.getsource(
            FinvizScraperPuller.pull_ticker
        ),
    }
    for where, body in writers.items():
        assert "INSERT INTO raw_series" in body or "_insert_raw" in body, where
        assert "else 0.0" not in body, f"{where} still coerces a failed parse to 0.0"
        assert "else 0" not in body, f"{where} still coerces a failed parse to a zero"


def test_neither_finviz_file_contains_a_zero_coercion_at_all() -> None:
    for path in (DAD_PATH, SCRAPER_PATH):
        source = path.read_text(encoding="utf-8")
        assert "else 0.0" not in source, f"{path.name} reintroduced an `else 0.0` coercion"
