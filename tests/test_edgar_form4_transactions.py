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

from datetime import date, datetime, timezone
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
# plus one derivative option grant that must not be counted. P and S are
# "open market or private" (SEC): the PS_* sums do not claim a venue.
EXPECTED = {
    "net_shares": 100.0 - 40.0 + 50.0 - 10.0,
    "net_value": 1000.0 - 480.0 + 0.0 - 110.0,
    "txn_count": 4.0,
    "ps_net_shares": 60.0,
    "ps_net_value": 520.0,
    "ps_txn_count": 2.0,
    "ps_shares_valid_count": 2.0,
    "ps_priced_count": 2.0,
}

# Series written per covered date (FILINGS_* come from the pull).
SERIES = (
    "NET_SHARES", "NET_VALUE", "TXN_COUNT",
    "PS_NET_SHARES", "PS_NET_VALUE", "PS_TXN_COUNT",
    "PS_SHARES_VALID_COUNT", "PS_PRICED_COUNT", "FILINGS_LISTED", "FILINGS_UNREAD",
)


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


def test_aggregate_real_table_signs_and_ps_split():
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
    # The P/S sums impute nothing: the unreadable P row is not share-valid.
    assert agg["ps_txn_count"] == 2.0
    assert agg["ps_shares_valid_count"] == 1.0
    assert agg["ps_priced_count"] == 1.0
    assert agg["ps_net_shares"] == -30.0
    assert agg["ps_net_value"] == -150.0


def test_aggregate_ps_counts_impute_no_price_or_direction():
    data = pd.DataFrame(
        {
            "Code": ["P", "S", "P", "S", "P", "A", "M"],
            "AcquiredDisposed": ["A", "D", "A", "D", "", "A", "A"],
            "Shares": [100, 10, 5, -3, 7, 1_000, 500],
            "Price": [float("nan"), 10.0, 0.0, 9.0, 4.0, 0.0, 2.0],
        }
    )
    agg = edgar_mod.aggregate_form4_transactions(data)
    assert agg["ps_txn_count"] == 5.0
    # Valid: P 100 (unpriced), S 10 @ 10, P 5 @ $0. Not: negative shares, blank A/D.
    assert agg["ps_shares_valid_count"] == 3.0
    assert agg["ps_priced_count"] == 1.0  # only S 10 @ 10; a $0 price is not a price
    assert agg["ps_net_shares"] == 100.0 - 10.0 + 5.0
    assert agg["ps_net_value"] == -100.0
    # The all-code legacy sums still count the award and exercise.
    assert agg["txn_count"] == 7.0


# ── table classification: verified empty vs unreadable ─────────────────────


def test_read_classifies_real_rows_empty_and_unreadable_tables():
    status, data = edgar_mod.read_form4_nonderivative(_f4(_real_table()))
    assert status == edgar_mod.FORM4_TABLE_ROWS and data is not None and len(data) == 4
    # What Form4.parse_xml builds for a derivative- or holdings-only filing.
    assert edgar_mod.read_form4_nonderivative(_f4(_empty_table())) == (edgar_mod.FORM4_TABLE_EMPTY, None)


@pytest.mark.parametrize(
    "f4",
    [
        None,
        SimpleNamespace(),
        _f4(None),
        _f4(SimpleNamespace(transactions=None)),
        _f4(SimpleNamespace(transactions=SimpleNamespace(data=pd.DataFrame()))),
    ],
    ids=["obj-none", "no-table-attr", "table-none", "transactions-none", "foreign-empty-table"],
)
def test_read_marks_unknown_table_shapes_unreadable(f4):
    assert edgar_mod.read_form4_nonderivative(f4) == (edgar_mod.FORM4_TABLE_UNREADABLE, None)


def test_read_marks_rows_without_a_read_column_unreadable():
    table = _real_table()
    table.transactions.data = table.transactions.data.drop(columns=["Code"])
    assert edgar_mod.read_form4_nonderivative(_f4(table)) == (edgar_mod.FORM4_TABLE_UNREADABLE, None)


# ── the pull: verified coverage interval ───────────────────────────────────
#
# Clock: 2026-11-20 10:00 ET. The last closed ET filing date is 11-19 (a date
# closes 6 h after its ET midnight), and days_back=7 puts the cutoff at 11-13.

NOW = datetime(2026, 11, 20, 15, 0, tzinfo=timezone.utc)
D = {n: date(2026, 11, n) for n in range(1, 31)}
COVERED_7 = [D[n] for n in range(13, 20)]  # 11-13 .. 11-19
OLD = date(2026, 10, 1)  # older than the cutoff: proves the listing reached it


def _filing(filed: date, f4) -> SimpleNamespace:
    calls = {"n": 0}

    def obj():
        calls["n"] += 1
        return f4

    return SimpleNamespace(filing_date=filed, obj=obj, calls=calls)


def _run_pull(monkeypatch, filings, days_back=7, now=NOW):
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
    out = puller.pull_form4_transactions(tickers=["EXMP"], days_back=days_back, now=now)
    inserts = [
        (str(c.args[0]), c.args[1])
        for c in conn.execute.call_args_list
        if "INSERT INTO raw_series" in str(c.args[0])
    ]
    return out, inserts


def _by_key(inserts):
    return {(p["sid"], p["od"]): p["val"] for _sql, p in inserts}


def test_pull_writes_every_covered_date_with_all_series(monkeypatch):
    filings = [
        _filing(D[20], _f4(_real_table())),  # today ET: the date is still open
        _filing(D[19], _f4(_real_table())),
        _filing(D[19], _f4(_real_table())),  # same day: summed
        _filing(D[17], _f4(_real_table())),
        _filing(OLD, _f4(_real_table())),
    ]
    out, inserts = _run_pull(monkeypatch, filings)

    assert out["errors"] == [] and out["status"] == "SUCCESS"
    assert out["rows_inserted"] == len(inserts) == len(COVERED_7) * len(SERIES)
    for sql, _params in inserts:
        assert "'SUCCESS'" in sql and ":od" in sql and ":val" in sql  # bound, not formatted
    by_key = _by_key(inserts)
    assert {p["src"] for _s, p in inserts} == {61}
    assert {od for _sid, od in by_key} == set(COVERED_7)  # today (open) never written
    assert by_key[("FORM4:EXMP:NET_SHARES", D[19])] == 2 * EXPECTED["net_shares"]
    assert by_key[("FORM4:EXMP:PS_NET_SHARES", D[19])] == 120.0
    assert by_key[("FORM4:EXMP:PS_NET_VALUE", D[19])] == 1040.0
    assert by_key[("FORM4:EXMP:PS_TXN_COUNT", D[19])] == 4.0
    assert by_key[("FORM4:EXMP:PS_PRICED_COUNT", D[19])] == 4.0
    assert by_key[("FORM4:EXMP:FILINGS_LISTED", D[19])] == 2.0
    assert by_key[("FORM4:EXMP:FILINGS_UNREAD", D[19])] == 0.0
    assert by_key[("FORM4:EXMP:PS_TXN_COUNT", D[17])] == EXPECTED["ps_txn_count"]
    # A covered date with no filing is written as a verified zero.
    assert by_key[("FORM4:EXMP:FILINGS_LISTED", D[15])] == 0.0
    assert by_key[("FORM4:EXMP:PS_TXN_COUNT", D[15])] == 0.0
    assert not any("OPEN_MKT" in sid for sid, _od in by_key)


def test_pull_stops_at_days_back_cutoff(monkeypatch):
    old = _filing(OLD, _f4(_real_table()))
    after_old = _filing(D[19], _f4(_real_table()))
    filings = [_filing(D[19], _f4(_real_table())), old, after_old]
    out, inserts = _run_pull(monkeypatch, filings, days_back=7)

    assert old.calls["n"] == 0 and after_old.calls["n"] == 0  # break, not continue
    assert {p["od"] for _s, p in inserts} == set(COVERED_7)
    assert out["rows_inserted"] == len(COVERED_7) * len(SERIES)


def test_a_date_is_not_closed_until_six_hours_past_et_midnight(monkeypatch):
    filings = [_filing(D[19], _f4(_real_table())), _filing(OLD, _f4(_real_table()))]
    early = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)  # 05:00 ET on 11-20
    _out, inserts = _run_pull(monkeypatch, filings, now=early)

    assert {p["od"] for _s, p in inserts} == {D[n] for n in range(13, 19)}  # 11-19 still open


def test_pull_reads_at_most_20_filings_and_writes_no_partly_read_date(monkeypatch):
    filings = [_filing(D[19], _f4(_real_table())) for _ in range(25)]
    _out, inserts = _run_pull(monkeypatch, filings)

    assert sum(f.calls["n"] for f in filings) == 20
    assert sum(f.calls["n"] for f in filings[20:]) == 0  # inspected by date only
    # Five 11-19 filings were never read and nothing older was listed: no
    # date is proven, so nothing is written.
    assert inserts == []


def test_pull_cap_proves_only_dates_after_the_next_filing(monkeypatch):
    filings = [_filing(D[19], _f4(_real_table())) for _ in range(20)]
    filings += [_filing(D[16], _f4(_real_table())) for _ in range(3)]
    _out, inserts = _run_pull(monkeypatch, filings)

    by_key = _by_key(inserts)
    assert {od for _sid, od in by_key} == {D[17], D[18], D[19]}
    assert by_key[("FORM4:EXMP:FILINGS_LISTED", D[19])] == 20.0
    assert by_key[("FORM4:EXMP:FILINGS_LISTED", D[17])] == 0.0


def test_pull_cap_with_the_next_filing_older_than_the_window_proves_the_window(monkeypatch):
    filings = [_filing(D[19], _f4(_real_table())) for _ in range(20)]
    filings.append(_filing(OLD, _f4(_real_table())))
    _out, inserts = _run_pull(monkeypatch, filings, days_back=7)

    assert {p["od"] for _s, p in inserts} == set(COVERED_7)


def test_a_listing_that_ends_early_proves_only_the_dates_after_its_oldest_filing(monkeypatch):
    filings = [_filing(D[19], _f4(_real_table())), _filing(D[16], _f4(_real_table()))]
    _out, inserts = _run_pull(monkeypatch, filings)

    assert {p["od"] for _s, p in inserts} == {D[17], D[18], D[19]}


def test_pull_marks_a_filing_that_failed_to_parse(monkeypatch):
    def broken():
        raise ValueError("malformed ownership XML")

    filings = [
        _filing(D[19], _f4(_real_table())),
        SimpleNamespace(filing_date=D[19], obj=broken, calls={"n": 0}),
        _filing(OLD, _f4(_real_table())),
    ]
    out, inserts = _run_pull(monkeypatch, filings)

    by_key = _by_key(inserts)
    assert out["errors"] == []
    assert by_key[("FORM4:EXMP:FILINGS_UNREAD", D[19])] == 1.0
    assert by_key[("FORM4:EXMP:FILINGS_LISTED", D[19])] == 2.0
    assert by_key[("FORM4:EXMP:PS_TXN_COUNT", D[19])] == EXPECTED["ps_txn_count"]


@pytest.mark.parametrize(
    ("f4", "unread"),
    [(None, 1.0), (_f4(None), 1.0), (_f4(_empty_table()), 0.0)],
    ids=["obj-none-unread", "table-none-unread", "verified-derivative-only"],
)
def test_pull_tells_unreadable_from_verified_empty_filings(monkeypatch, f4, unread):
    out, inserts = _run_pull(monkeypatch, [_filing(D[19], f4), _filing(OLD, _f4(_real_table()))])

    by_key = _by_key(inserts)
    assert out["errors"] == []
    assert by_key[("FORM4:EXMP:FILINGS_LISTED", D[19])] == 1.0
    assert by_key[("FORM4:EXMP:FILINGS_UNREAD", D[19])] == unread
    assert by_key[("FORM4:EXMP:PS_TXN_COUNT", D[19])] == 0.0


def test_an_undated_filing_breaks_the_listing_proof(monkeypatch):
    class _Undated:
        calls = {"n": 0}

        @property
        def filing_date(self):
            raise ValueError("no filing date")

    filings = [_filing(D[19], _f4(_real_table())), _Undated(), _filing(OLD, _f4(_real_table()))]
    out, inserts = _run_pull(monkeypatch, filings)

    assert inserts == [] and out["errors"] == []


# ── downstream: the aggregates are not insider signals ──────────────────────


@pytest.mark.parametrize(
    "metric",
    ["NET_SHARES", "NET_VALUE", "TXN_COUNT",
     "OPEN_MKT_NET_SHARES", "OPEN_MKT_NET_VALUE", "OPEN_MKT_TXN_COUNT",
     "PS_NET_SHARES", "PS_NET_VALUE", "PS_TXN_COUNT",
     "PS_SHARES_VALID_COUNT", "PS_PRICED_COUNT", "FILINGS_LISTED", "FILINGS_UNREAD"],
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
