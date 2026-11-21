"""ingestion.edgar Form 4 pull: read transactions where edgartools keeps them.

``pull_form4_transactions`` checked ``hasattr(f4, "transactions")``. edgartools'
``Form4`` has no such attribute (4.6.3, 5.62.1 and 6.3.0 alike). The rows live
in ``f4.non_derivative_table.transactions.data``. So every filing was skipped
and the FORM4:* series were never written.

All offline: the real-shape tests run edgartools' own table parser on a
synthetic XML fixture (``Form4.parse_xml`` itself would look the reporting
owner up on SEC), and the pull tests stub ``Company``.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

edgar_lib = pytest.importorskip("edgar")
edgar_mod = pytest.importorskip("ingestion.edgar")
EDGARPuller = edgar_mod.EDGARPuller

from edgar.ownership import Form4  # noqa: E402
from edgar.ownership.forms import parse_document  # noqa: E402
from edgar.ownership.table_containers import NonDerivativeTable  # noqa: E402
from edgar.ownership.tables import (  # noqa: E402
    NonDerivativeHoldings,
    NonDerivativeTransactions,
)
from edgar.xmltools import find_element  # noqa: E402

from intelligence import signal_extractor  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "form4_synthetic.xml"

# The fixture: P 100 @ 10 (A), S 40 @ 12 (D), A 50 @ 0 (A), F 10 @ 11 (D),
# plus one derivative option grant that must not be counted.
EXPECTED = {
    "net_shares": 100.0 - 40.0 + 50.0 - 10.0,
    "net_value": 1000.0 - 480.0 + 0.0 - 110.0,
    "txn_count": 4.0,
    "open_mkt_net_shares": 60.0,
    "open_mkt_net_value": 520.0,
    "open_mkt_txn_count": 2.0,
}


def _real_table() -> NonDerivativeTable:
    root = parse_document(FIXTURE.read_text())
    return NonDerivativeTable.extract(find_element(root, "nonDerivativeTable"), form="4")


def _empty_table() -> NonDerivativeTable:
    # Exactly what Form4.parse_xml builds when a filing has no
    # nonDerivativeTable (derivative-only or holdings-only filings).
    return NonDerivativeTable(
        holdings=NonDerivativeHoldings(), transactions=NonDerivativeTransactions(), form="4"
    )


def _f4(table: NonDerivativeTable | None) -> SimpleNamespace:
    return SimpleNamespace(non_derivative_table=table)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(edgar_mod.time, "sleep", lambda *_a, **_k: None)


# ── the library contract ────────────────────────────────────────────────────


def test_form4_has_no_transactions_attribute():
    """Why the old check skipped every filing."""
    assert not hasattr(Form4, "transactions")
    assert "transactions" not in vars(Form4) and not any(
        "transactions" in vars(base) for base in Form4.__mro__
    )


def test_real_table_has_the_columns_we_read():
    data = edgar_mod.form4_nonderivative_transactions(_f4(_real_table()))
    assert data is not None
    assert {"Code", "AcquiredDisposed", "Shares", "Price"} <= set(data.columns)


# ── extraction: None and empty ──────────────────────────────────────────────


@pytest.mark.parametrize(
    "f4",
    [
        None,  # filing.obj() answered None (unmodelled form)
        SimpleNamespace(),  # no non_derivative_table attribute
        _f4(None),
        _f4(SimpleNamespace(transactions=None)),
        _f4(SimpleNamespace(transactions=SimpleNamespace(data=None))),
        _f4(SimpleNamespace(transactions=SimpleNamespace(data=pd.DataFrame()))),
    ],
    ids=["obj-none", "no-table-attr", "table-none", "transactions-none", "data-none", "data-empty"],
)
def test_missing_or_none_transactions_return_none(f4):
    assert edgar_mod.form4_nonderivative_transactions(f4) is None


def test_real_empty_table_returns_none():
    assert edgar_mod.form4_nonderivative_transactions(_f4(_empty_table())) is None


# ── aggregation ─────────────────────────────────────────────────────────────


def test_aggregate_real_table_signs_and_open_market_split():
    data = edgar_mod.form4_nonderivative_transactions(_f4(_real_table()))
    assert edgar_mod.aggregate_form4_transactions(data) == EXPECTED


def test_aggregate_non_numeric_shares_count_as_zero_with_warning(monkeypatch):
    warnings: list[str] = []
    monkeypatch.setattr(edgar_mod.log, "warning", lambda msg, **kw: warnings.append(msg))
    data = pd.DataFrame(
        {
            "Code": ["P", "S"],
            "AcquiredDisposed": ["A", "D"],
            "Shares": ["100 [F1]", 30],
            "Price": [None, 5.0],
        }
    )
    agg = edgar_mod.aggregate_form4_transactions(data)
    assert agg["net_shares"] == -30.0
    assert agg["net_value"] == -150.0
    assert agg["txn_count"] == 2.0
    assert warnings, "a coerced share value must be logged"


# ── the pull ────────────────────────────────────────────────────────────────


def _filing(filed: date, f4) -> SimpleNamespace:
    calls = {"n": 0}

    def obj():
        calls["n"] += 1
        return f4

    return SimpleNamespace(filing_date=filed, obj=obj, calls=calls)


def _run_pull(monkeypatch, filings, days_back=7):
    monkeypatch.setattr(
        edgar_mod, "Company",
        lambda ticker: SimpleNamespace(get_filings=lambda form: filings),
    )
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    puller = EDGARPuller.__new__(EDGARPuller)
    puller.engine = engine
    puller.source_id = 61
    puller._row_exists = lambda *_a, **_k: False  # type: ignore[method-assign]
    out = puller.pull_form4_transactions(tickers=["EXMP"], days_back=days_back)
    inserts = [
        (str(c.args[0]), c.args[1])
        for c in conn.execute.call_args_list
        if "INSERT INTO raw_series" in str(c.args[0])
    ]
    return out, inserts


def test_pull_writes_six_series_per_filing_date(monkeypatch):
    today = date.today()
    yesterday = today - timedelta(days=1)
    filings = [
        _filing(today, _f4(_real_table())),
        _filing(today, _f4(_real_table())),  # same day: summed
        _filing(yesterday, _f4(_real_table())),
    ]
    out, inserts = _run_pull(monkeypatch, filings)

    assert out["errors"] == [] and out["status"] == "SUCCESS"
    assert out["rows_inserted"] == len(inserts) == 12
    for sql, _params in inserts:
        assert "'SUCCESS'" in sql and ":od" in sql and ":val" in sql  # bound, not formatted
    by_key = {(p["sid"], p["od"]): p["val"] for _sql, p in inserts}
    assert {p["src"] for _s, p in inserts} == {61}
    assert by_key[("FORM4:EXMP:NET_SHARES", today)] == 2 * EXPECTED["net_shares"]
    assert by_key[("FORM4:EXMP:NET_VALUE", today)] == 2 * EXPECTED["net_value"]
    assert by_key[("FORM4:EXMP:TXN_COUNT", today)] == 8.0
    assert by_key[("FORM4:EXMP:OPEN_MKT_NET_SHARES", today)] == 120.0
    assert by_key[("FORM4:EXMP:OPEN_MKT_NET_VALUE", today)] == 1040.0
    assert by_key[("FORM4:EXMP:OPEN_MKT_TXN_COUNT", today)] == 4.0
    assert by_key[("FORM4:EXMP:NET_SHARES", yesterday)] == EXPECTED["net_shares"]
    # obs_date is the filing date (when it became public), never later.
    assert {od for _sid, od in by_key} == {today, yesterday}


def test_pull_stops_at_days_back_cutoff(monkeypatch):
    today = date.today()
    old = _filing(today - timedelta(days=30), _f4(_real_table()))
    after_old = _filing(today, _f4(_real_table()))
    filings = [_filing(today, _f4(_real_table())), old, after_old]
    out, inserts = _run_pull(monkeypatch, filings, days_back=7)

    assert old.calls["n"] == 0 and after_old.calls["n"] == 0  # break, not continue
    assert {p["od"] for _s, p in inserts} == {today}
    assert out["rows_inserted"] == 6


def test_pull_reads_at_most_20_filings(monkeypatch):
    today = date.today()
    filings = [_filing(today, _f4(_real_table())) for _ in range(25)]
    _out, inserts = _run_pull(monkeypatch, filings)

    assert sum(f.calls["n"] for f in filings) == 20
    by_sid = {p["sid"]: p["val"] for _s, p in inserts}
    assert by_sid["FORM4:EXMP:TXN_COUNT"] == 20 * EXPECTED["txn_count"]


@pytest.mark.parametrize(
    "f4",
    [None, _f4(_empty_table()), _f4(None)],
    ids=["obj-none", "empty-table", "table-none"],
)
def test_pull_writes_nothing_for_filings_without_transactions(monkeypatch, f4):
    out, inserts = _run_pull(monkeypatch, [_filing(date.today(), f4)])
    assert inserts == [] and out["rows_inserted"] == 0 and out["errors"] == []


# ── downstream: the aggregates are not insider signals ──────────────────────


@pytest.mark.parametrize(
    "metric",
    ["NET_SHARES", "NET_VALUE", "TXN_COUNT",
     "OPEN_MKT_NET_SHARES", "OPEN_MKT_NET_VALUE", "OPEN_MKT_TXN_COUNT"],
)
def test_signal_extractor_skips_form4_aggregates(metric):
    entry = next(e for e in signal_extractor.EXTRACTORS if e["pattern"] == "FORM4:%")
    parsed = entry["parse"](f"FORM4:AAPL:{metric}")
    assert parsed["ticker"] is None  # extract_from_raw_series skips ticker-less rows
    assert parsed["actor"] is None


def test_signal_extractor_other_form4_shapes_unchanged():
    entry = next(e for e in signal_extractor.EXTRACTORS if e["pattern"] == "FORM4:%")
    parsed = entry["parse"]("FORM4:AAPL:john_smith:x:purchase")
    assert parsed["ticker"] == "AAPL"
    assert parsed["actor"] == "John Smith"
    assert parsed["direction"] == "BULL"
