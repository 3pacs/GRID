"""Form 4 writer to SMART_MONEY reader, end to end (Codex HOLD on #1353 67ce6bdc).

Each case builds a Form 4 ownership XML and parses it with edgartools' own table
parser. ``EDGARPuller.pull_form4_transactions`` then writes it into an in-memory
SQLite ``raw_series`` (``Company`` is stubbed), and
``conviction_scorer.score_smart_money`` reads it back through
``store.observations``.

The cases:

* a private P purchase: P/S is "open market or private" (SEC Form 4 codes), so
  the label never claims a venue;
* all P/S rows unpriced;
* opposite-signed rows with uneven price coverage (an unpriced buy and a priced
  sale);
* an explicitly fully priced offset;
* an award/exercise-only filing;
* a filing the pull could not read;
* legacy and partial rows.

The table's ``pull_timestamp`` default is one fixed instant. That mirrors
PostgreSQL, where ``NOW()`` is constant for the writer's one transaction.
SQLite's ``CURRENT_TIMESTAMP`` can change between statements.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

pytest.importorskip("edgar")
edgar_mod = pytest.importorskip("ingestion.edgar")

from edgar.ownership.forms import parse_document  # noqa: E402
from edgar.ownership.table_containers import NonDerivativeTable  # noqa: E402
from edgar.xmltools import find_element  # noqa: E402

from alpha_research import conviction_scorer as cs  # noqa: E402

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

FIXTURE = Path(__file__).parent / "fixtures" / "form4_synthetic.xml"
EDGAR_SRC = 1
TODAY = date.today()
PULLED = datetime.combine(TODAY, time(6, 0))
LABEL = "Insider reported P/S (non-derivative, venue unknown)"
PRIVATE_FOOTNOTE = "The reporting person purchased these shares in a private transaction."


def _row(code: str, shares: str, price: str | None, acq_disp: str, footnote: bool = False) -> str:
    fn = '<footnoteId id="F1"/>' if footnote else ""
    if price is None:
        price_xml = '<transactionPricePerShare><footnoteId id="F2"/></transactionPricePerShare>'
    else:
        price_xml = f"<transactionPricePerShare><value>{price}</value></transactionPricePerShare>"
    return (
        "<nonDerivativeTransaction>"
        "<securityTitle><value>Common Stock</value></securityTitle>"
        "<transactionDate><value>2026-11-02</value></transactionDate>"
        "<transactionCoding><transactionFormType>4</transactionFormType>"
        f"<transactionCode>{code}</transactionCode><equitySwapInvolved>0</equitySwapInvolved>{fn}"
        "</transactionCoding>"
        f"<transactionAmounts><transactionShares><value>{shares}</value></transactionShares>{price_xml}"
        f"<transactionAcquiredDisposedCode><value>{acq_disp}</value></transactionAcquiredDisposedCode>"
        "</transactionAmounts>"
        "<postTransactionAmounts><sharesOwnedFollowingTransaction><value>1000</value>"
        "</sharesOwnedFollowingTransaction></postTransactionAmounts>"
        "<ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>"
        "</ownershipNature></nonDerivativeTransaction>"
    )


def _table(*rows: str) -> NonDerivativeTable:
    """A real edgartools NonDerivativeTable parsed from the synthetic header plus ``rows``."""
    xml = FIXTURE.read_text()
    body = (
        "<nonDerivativeTable>" + "".join(rows) + "</nonDerivativeTable>"
        f'<footnotes><footnote id="F1">{PRIVATE_FOOTNOTE}</footnote>'
        '<footnote id="F2">Price not reported.</footnote></footnotes>'
    )
    xml = re.sub(r"<nonDerivativeTable>.*</derivativeTable>", lambda _m: body, xml, flags=re.S)
    root = parse_document(xml)
    return NonDerivativeTable.extract(find_element(root, "nonDerivativeTable"), form="4")


def _filing(filed: date, table: NonDerivativeTable | None = None, broken: bool = False) -> SimpleNamespace:
    def obj():
        if broken:
            raise ValueError("malformed ownership XML")
        return SimpleNamespace(non_derivative_table=table)

    return SimpleNamespace(filing_date=filed, obj=obj)


class _Store:
    def __init__(self) -> None:
        self.engine = create_engine("sqlite://")
        md = MetaData()
        source_catalog = Table(
            "source_catalog", md,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
        )
        self.raw = Table(
            "raw_series", md,
            Column("series_id", String, nullable=False),
            Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
            Column("obs_date", Date, nullable=False),
            Column("pull_timestamp", DateTime, nullable=False,
                   server_default=PULLED.isoformat(sep=" ")),
            Column("value", Float, nullable=False),
            Column("raw_payload", Text),
            Column("pull_status", String, nullable=False),
        )
        md.create_all(self.engine)
        with self.engine.begin() as c:
            c.execute(source_catalog.insert(), [{"id": EDGAR_SRC, "name": "SEC_EDGAR"}])

    def pull(self, monkeypatch: pytest.MonkeyPatch, filings: list[SimpleNamespace]) -> dict:
        monkeypatch.setattr(
            edgar_mod, "Company",
            lambda _ticker: SimpleNamespace(get_filings=lambda form: filings),
        )
        monkeypatch.setattr(edgar_mod.time, "sleep", lambda *_a, **_k: None)
        puller = edgar_mod.EDGARPuller.__new__(edgar_mod.EDGARPuller)
        puller.engine = self.engine
        puller.source_id = EDGAR_SRC
        puller._row_exists = lambda *_a, **_k: False  # type: ignore[method-assign]
        return puller.pull_form4_transactions(tickers=["EXMP"], days_back=7)

    def score(self) -> cs.LayerResult:
        with self.engine.connect() as c:
            return cs.score_smart_money(c, "EXMP", as_of_date=TODAY)


def _venue_free(result: cs.LayerResult) -> bool:
    return not any("open-market" in s.lower() or "open market" in s.lower() for s in result.signals)


def test_private_p_purchase_is_scored_without_a_venue_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    # Shape of the cited Digital Ally Form 4/A: a P purchase of 40,000 shares
    # at $1.1225 that a footnote says was private.
    out = store.pull(monkeypatch, [_filing(TODAY, _table(_row("P", "40000", "1.1225", "A", footnote=True)))])
    assert out["errors"] == []

    result = store.score()

    assert result.data_available is True
    assert result.score == 2
    assert f"{LABEL} net buy $44,900" in result.signals
    assert _venue_free(result)


def test_all_unpriced_ps_rows_score_share_direction_only(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    store.pull(monkeypatch, [_filing(TODAY, _table(
        _row("P", "100", None, "A"),
        _row("S", "40", None, "D"),
    ))])

    result = store.score()

    assert result.data_available is True
    assert result.score == 2
    assert f"{LABEL} net share buy +60 sh (dollars unverified: 2 of 2 P/S rows unpriced)" in result.signals


def test_unpriced_buy_and_priced_sale_do_not_read_as_a_dollar_sale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store()
    # Codex's trace: P +100 at a missing price, S 10 at $10. The imputed-$0
    # dollar sum is -$100, but +90 shares were reported and the total is unknown.
    store.pull(monkeypatch, [_filing(TODAY, _table(
        _row("P", "100", None, "A"),
        _row("S", "10", "10.00", "D"),
    ))])

    result = store.score()

    assert result.score == 2
    assert f"{LABEL} net share buy +90 sh (dollars unverified: 1 of 2 P/S rows unpriced)" in result.signals
    assert not any("net sell" in s for s in result.signals)


def test_explicitly_fully_priced_offset_is_a_measured_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    store.pull(monkeypatch, [
        _filing(TODAY, _table(_row("P", "100", "10.00", "A"))),
        _filing(TODAY - timedelta(days=1), _table(_row("S", "100", "10.00", "D"))),
    ])

    result = store.score()

    assert result.data_available is True
    assert result.score == 0
    assert f"{LABEL} buys and sells offset (fully priced, net $0)" in result.signals


def test_award_and_exercise_only_filing_scores_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    store.pull(monkeypatch, [_filing(TODAY, _table(
        _row("A", "5000", "0", "A"),
        _row("M", "2000", "5.00", "A"),
    ))])

    result = store.score()

    assert result.data_available is True
    assert result.score == 0
    assert (
        "No reported non-derivative P/S insider transactions in the observed Form 4 data (90d)"
        in result.signals
    )


def test_a_filing_the_pull_could_not_read_withholds_the_window(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    store.pull(monkeypatch, [
        _filing(TODAY, _table(_row("P", "100", "10.00", "A"))),
        _filing(TODAY, broken=True),  # its trades are unknown: the date is incomplete
    ])

    result = store.score()

    assert result.data_available is False
    assert result.score == 0
    assert any("P/S data incomplete on 1 filing date(s)" in s for s in result.signals)


def test_legacy_and_partial_rows_are_not_scored(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    legacy_day = TODAY - timedelta(days=10)
    with store.engine.begin() as c:
        # What the writer stored before 2026-11-23: imputed-$0 OPEN_MKT_* rows.
        c.execute(store.raw.insert(), [
            {"series_id": f"FORM4:EXMP:{field}", "source_id": EDGAR_SRC, "obs_date": legacy_day,
             "value": value, "pull_status": "SUCCESS"}
            for field, value in (
                ("OPEN_MKT_NET_SHARES", 90.0), ("OPEN_MKT_NET_VALUE", -100.0),
                ("OPEN_MKT_TXN_COUNT", 2.0), ("NET_SHARES", 5_090.0),
            )
        ])
    assert store.score().data_available is False

    # A real pull adds a complete date. A partial date (PS rows without the
    # validity counts) still withholds the window.
    store.pull(monkeypatch, [_filing(TODAY, _table(_row("P", "100", "10.00", "A")))])
    assert store.score().score == 2
    with store.engine.begin() as c:
        c.execute(store.raw.insert(), [
            {"series_id": f"FORM4:EXMP:{field}", "source_id": EDGAR_SRC, "obs_date": legacy_day,
             "value": value, "pull_status": "SUCCESS"}
            for field, value in (("PS_NET_SHARES", -5_000.0), ("PS_NET_VALUE", -50_000.0),
                                 ("PS_TXN_COUNT", 1.0))
        ])

    result = store.score()

    assert result.data_available is False
    assert any("P/S data incomplete on 1 filing date(s)" in s for s in result.signals)
