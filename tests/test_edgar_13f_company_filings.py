"""ingestion.edgar: 13F pull must use ``Company(cik).get_filings`` and the
8-K full-index 403 must be classified honestly.

Evidence (grid-hermes journal, grid-svr, 2026-10-20):

* 04:20:55Z ``ingestion.edgar:pull_13f_holdings:190 — 13F pull failed for CIK
  0001067983: get_filings() got an unexpected keyword argument 'cik'`` -- the
  same line for all 75 verified filers, then ``13F pull complete — 0 rows``.
  edgartools' module-level ``get_filings`` is the quarterly full-index
  downloader; it has never accepted ``cik`` (checked on 5.28.4, the version
  grid-hermes runs, and 5.61.1 locally).
* 04:21:55Z ``pull_8k_counts:445 — EDGAR full-index returned 403 — index
  likely not yet generated for current quarter ... full-index/2026/QTR1/form.gz``
  -- a 403 on Q1's index in October is an access denial, not a missing index.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

edgar_lib = pytest.importorskip("edgar")  # ingestion.edgar imports it at module scope
import ingestion.edgar as edgar_mod  # noqa: E402
from ingestion.edgar import EDGARPuller  # noqa: E402


def _puller(mock_engine) -> EDGARPuller:
    """An EDGARPuller without the DB source_catalog lookup in __init__."""
    p = EDGARPuller.__new__(EDGARPuller)
    p.engine = mock_engine
    p.source_id = 61
    p._row_exists = lambda *_a, **_k: False  # type: ignore[method-assign]
    return p


@pytest.fixture(autouse=True)
def _no_sleep_and_clean_breaker(monkeypatch):
    monkeypatch.setattr(edgar_mod.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(EDGARPuller, "_PULL_8K_BREAKER", False)
    monkeypatch.setattr(EDGARPuller, "_PULL_8K_BREAKER_TS", 0.0)
    yield


# ---------------------------------------------------------------------------
# 13F
# ---------------------------------------------------------------------------


def test_module_get_filings_rejects_cik_kwarg():
    """The library contract the old call site violated (no network: the
    TypeError is raised at argument binding, before any request)."""
    with pytest.raises(TypeError, match=r"unexpected keyword argument 'cik'"):
        edgar_lib.get_filings(form="13F-HR", cik="0001067983")


class _FakeFiling:
    def __init__(self, filing_date: str, values: list[float]) -> None:
        self.filing_date = filing_date  # submissions API gives ISO strings
        self._values = values

    def obj(self):
        return MagicMock(infotable=pd.DataFrame({"value": self._values}))


def test_pull_13f_uses_company_filings_and_writes_metrics(mock_engine, monkeypatch):
    """Fails on main: main calls ``get_filings(form=..., cik=...)`` which the
    real edgartools rejects with TypeError, so errors is non-empty and 0 rows
    are written. With the fix the per-entity ``Company(cik).get_filings``
    path is used and the three metrics per filing are inserted."""
    seen: dict[str, object] = {}

    class FakeCompany:
        def __init__(self, cik):
            seen["cik"] = cik

        def get_filings(self, **kw):
            seen["kw"] = kw
            return [
                _FakeFiling("2026-08-14", [50.0, 30.0, 10.0, 5.0, 3.0, 2.0]),
                _FakeFiling("2026-05-15", [10.0, 10.0]),
            ]

    monkeypatch.setattr(edgar_mod, "Company", FakeCompany)
    # Guard: the module-level downloader must not be touched by the 13F path.
    monkeypatch.setattr(
        edgar_mod,
        "get_filings",
        lambda *a, **k: pytest.fail(f"module-level get_filings called: {a} {k}"),
    )

    out = _puller(mock_engine).pull_13f_holdings(cik_list=["0001067983"])

    assert out["errors"] == []
    assert out["status"] == "SUCCESS"
    assert out["rows_inserted"] == 6  # 2 filings x 3 metrics
    assert seen["cik"] == "0001067983"
    assert seen["kw"] == {"form": "13F-HR"}

    conn = mock_engine.begin.return_value.__enter__.return_value
    params = [c.args[1] for c in conn.execute.call_args_list]
    by_sid = {(p["sid"], str(p["od"])): p["val"] for p in params}
    assert by_sid[("13F:0001067983:TOTAL_VALUE", "2026-08-14")] == 100.0
    assert by_sid[("13F:0001067983:NUM_POSITIONS", "2026-08-14")] == 6.0
    assert by_sid[("13F:0001067983:TOP5_CONCENTRATION", "2026-08-14")] == 0.98
    assert by_sid[("13F:0001067983:TOTAL_VALUE", "2026-05-15")] == 20.0
    assert all(p["src"] == 61 for p in params)


def test_pull_13f_respects_max_filings_per_fund(mock_engine, monkeypatch):
    class FakeCompany:
        def __init__(self, cik):
            pass

        def get_filings(self, **kw):
            return [_FakeFiling(f"2026-0{m}-01", [1.0]) for m in (9, 6, 3, 1)]

    monkeypatch.setattr(edgar_mod, "Company", FakeCompany)
    out = _puller(mock_engine).pull_13f_holdings(
        cik_list=["0001067983"], max_filings_per_fund=1
    )
    assert out["rows_inserted"] == 3
    assert out["errors"] == []


def test_pull_13f_company_error_is_reported_per_cik(mock_engine, monkeypatch):
    class FakeCompany:
        def __init__(self, cik):
            raise RuntimeError("submissions 503")

    monkeypatch.setattr(edgar_mod, "Company", FakeCompany)
    out = _puller(mock_engine).pull_13f_holdings(cik_list=["0001067983", "0001350694"])
    assert out["status"] == "PARTIAL"
    assert out["rows_inserted"] == 0
    assert len(out["errors"]) == 2
    assert out["errors"][0].startswith("13F pull failed for CIK 0001067983: ")


# ---------------------------------------------------------------------------
# 8-K full-index 403 classification
# ---------------------------------------------------------------------------


def _raise_403(url: str):
    def _get_filings(**_kw):
        raise RuntimeError(f"Client error '403 Forbidden' for url '{url}'")

    return _get_filings


def _previous_quarter(y: int, q: int) -> tuple[int, int]:
    return (y - 1, 4) if q == 1 else (y, q - 1)


def test_8k_403_on_current_quarter_index_is_skipped(mock_engine, monkeypatch):
    y, q = EDGARPuller._current_year_quarter()
    monkeypatch.setattr(
        edgar_mod,
        "get_filings",
        _raise_403(f"https://www.sec.gov/Archives/edgar/full-index/{y}/QTR{q}/form.gz"),
    )
    out = _puller(mock_engine).pull_8k_counts(days_back=7)
    assert out["status"] == "SKIPPED"
    assert out["rows_inserted"] == 0
    assert EDGARPuller._PULL_8K_BREAKER is True


def test_8k_403_on_past_quarter_index_is_a_real_failure(mock_engine, monkeypatch):
    """The 2026-10-20 case: 403 for .../full-index/2026/QTR1/form.gz in Q4."""
    y, q = _previous_quarter(*EDGARPuller._current_year_quarter())
    monkeypatch.setattr(
        edgar_mod,
        "get_filings",
        _raise_403(f"https://www.sec.gov/Archives/edgar/full-index/{y}/QTR{q}/form.gz"),
    )
    out = _puller(mock_engine).pull_8k_counts(days_back=7)
    assert out["status"] == "PARTIAL"
    assert out["rows_inserted"] == 0
    assert len(out["errors"]) == 1
    assert f"past quarter {y}Q{q}" in out["errors"][0]
    assert "403" in out["errors"][0]
    assert EDGARPuller._PULL_8K_BREAKER is False  # no 6h silence for a denial


def test_8k_403_on_non_index_endpoint_is_not_masked(mock_engine, monkeypatch):
    monkeypatch.setattr(
        edgar_mod,
        "get_filings",
        _raise_403("https://data.sec.gov/submissions/CIK0001067983.json"),
    )
    out = _puller(mock_engine).pull_8k_counts(days_back=7)
    assert out["status"] == "PARTIAL"
    assert out["errors"] == [
        "8-K count pull failed: Client error '403 Forbidden' for url "
        "'https://data.sec.gov/submissions/CIK0001067983.json'"
    ]
    assert EDGARPuller._PULL_8K_BREAKER is False


def test_full_index_quarter_parser():
    f = EDGARPuller._full_index_403_quarter
    assert f(
        "403 Forbidden for url 'https://www.sec.gov/Archives/edgar/full-index/2026/QTR1/form.gz'"
    ) == (2026, 1)
    assert f(
        "403 Forbidden for url 'https://www.sec.gov/Archives/edgar/full-index/2026/QTR4/'"
    ) == (2026, 4)
    assert f("500 Server Error for url '.../full-index/2026/QTR1/form.gz'") is None
    assert (
        f("403 Forbidden for url 'https://data.sec.gov/submissions/CIK1.json'") is None
    )
    assert f("403 Forbidden full-index with no quarter in the url") is None
    assert EDGARPuller._current_year_quarter(date(2026, 10, 20)) == (2026, 4)
    assert EDGARPuller._current_year_quarter(date(2026, 1, 1)) == (2026, 1)
