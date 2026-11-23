"""Form 4 writer to SMART_MONEY reader, end to end (Codex HOLDs on #1353 67ce6bdc, 4f55dab9).

Each case builds Form 4 ownership XML and parses it with edgartools' own table
parser. ``EDGARPuller.pull_form4_transactions`` then writes it into an in-memory
SQLite ``raw_series`` (``Company`` is stubbed), and
``conviction_scorer.score_smart_money`` reads it back through
``store.observations``.

Each pull runs at its own clock (10:00 ET) and sees only the filings EDGAR
would have listed by then. Its rows get that clock as their ``pull_timestamp``,
one instant per pull. That mirrors PostgreSQL, where ``NOW()`` is constant for
the writer's one transaction. A full history is 13 weekly 7-day pulls, which
covers 91 closed days.

Cases:

* a private P purchase: P/S is "open market or private" (SEC Form 4 codes), so
  the label never claims a venue;
* all P/S rows unpriced;
* opposite-signed rows with uneven price coverage;
* a fully priced offset;
* an award/exercise-only filing;
* a 7-day cold start and the completed interval;
* a partial listing (the filing cap);
* malformed vs verified derivative-only filings;
* a recovery pull, with the earlier as-of left unchanged;
* legacy and partial rows.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
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
    text,
)

pytest.importorskip("edgar")
edgar_mod = pytest.importorskip("ingestion.edgar")

from edgar.ownership.forms import parse_document  # noqa: E402
from edgar.ownership.table_containers import NonDerivativeTable  # noqa: E402
from edgar.ownership.tables import (  # noqa: E402
    NonDerivativeHoldings,
    NonDerivativeTransactions,
)
from edgar.xmltools import find_element  # noqa: E402

from alpha_research import conviction_scorer as cs  # noqa: E402

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

FIXTURE = Path(__file__).parent / "fixtures" / "form4_synthetic.xml"
EDGAR_SRC = 1
LAST = date(2026, 11, 20)  # the last routine pull, and the as-of date scored
ANCHOR = date(2025, 1, 2)  # an old filing: every pull's listing reaches its cutoff
SENTINEL = datetime(1999, 1, 1)
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


def _derivative_only() -> NonDerivativeTable:
    # What Form4.parse_xml builds when a filing has no nonDerivativeTable.
    return NonDerivativeTable(
        holdings=NonDerivativeHoldings(), transactions=NonDerivativeTransactions(), form="4"
    )


def _filing(filed: date, table: object = None, *, malformed: bool = False) -> SimpleNamespace:
    def obj():
        if malformed:
            return SimpleNamespace(non_derivative_table=SimpleNamespace(transactions=None))
        return SimpleNamespace(non_derivative_table=table)

    return SimpleNamespace(filing_date=filed, obj=obj)


def _listing(*filings: SimpleNamespace) -> list[SimpleNamespace]:
    out = sorted(filings, key=lambda f: f.filing_date, reverse=True)
    return out + [_filing(ANCHOR, _table(_row("A", "1", "0", "A")))]


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
                   server_default=SENTINEL.isoformat(sep=" ")),
            Column("value", Float, nullable=False),
            Column("raw_payload", Text),
            Column("pull_status", String, nullable=False),
        )
        md.create_all(self.engine)
        with self.engine.begin() as c:
            c.execute(source_catalog.insert(), [{"id": EDGAR_SRC, "name": "SEC_EDGAR"}])

    def pull(
        self, monkeypatch: pytest.MonkeyPatch, filings: list[SimpleNamespace], on: date,
        days_back: int = 7,
    ) -> dict:
        now = datetime.combine(on, time(15, 0), tzinfo=timezone.utc)  # 10:00 ET
        listed = [f for f in filings if f.filing_date <= on]  # what EDGAR listed then
        monkeypatch.setattr(
            edgar_mod, "Company",
            lambda _ticker: SimpleNamespace(get_filings=lambda form: listed),
        )
        monkeypatch.setattr(edgar_mod.time, "sleep", lambda *_a, **_k: None)
        puller = edgar_mod.EDGARPuller.__new__(edgar_mod.EDGARPuller)
        puller.engine = self.engine
        puller.source_id = EDGAR_SRC
        puller._row_exists = lambda *_a, **_k: False  # type: ignore[method-assign]
        out = puller.pull_form4_transactions(tickers=["EXMP"], days_back=days_back, now=now)
        with self.engine.begin() as c:
            c.execute(
                text("UPDATE raw_series SET pull_timestamp = :ts WHERE pull_timestamp = :sentinel"),
                {"ts": now.replace(tzinfo=None), "sentinel": SENTINEL},
            )
        assert out["errors"] == []
        return out

    def history(
        self, monkeypatch: pytest.MonkeyPatch, filings: list[SimpleNamespace],
        last: date = LAST, skip: tuple[int, ...] = (),
    ) -> None:
        """Thirteen weekly 7-day pulls ending at ``last`` (weeks back listed in ``skip`` are missed)."""
        for weeks_back in range(12, -1, -1):
            if weeks_back not in skip:
                self.pull(monkeypatch, filings, last - timedelta(days=7 * weeks_back))

    def score(self, as_of: date = LAST) -> cs.LayerResult:
        with self.engine.connect() as c:
            return cs.score_smart_money(c, "EXMP", as_of_date=as_of)


FULL_SPAN = f"verified {LAST - timedelta(days=90):%Y-%m-%d}..{LAST - timedelta(days=1):%Y-%m-%d}, 90d of 90d"


def _venue_free(result: cs.LayerResult) -> bool:
    return not any("open-market" in s.lower() or "open market" in s.lower() for s in result.signals)


def test_private_p_purchase_is_scored_without_a_venue_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    # Shape of the cited Digital Ally Form 4/A: a P purchase of 40,000 shares
    # at $1.1225 that a footnote says was private.
    store.history(monkeypatch, _listing(
        _filing(LAST - timedelta(days=3), _table(_row("P", "40000", "1.1225", "A", footnote=True))),
    ))

    result = store.score()

    assert result.data_available is True
    assert result.score == 2
    assert result.trust == 0.85
    assert f"{LABEL} net buy $44,900 ({FULL_SPAN})" in result.signals
    assert _venue_free(result)


def test_all_unpriced_ps_rows_score_share_direction_only(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    store.history(monkeypatch, _listing(_filing(LAST - timedelta(days=3), _table(
        _row("P", "100", None, "A"),
        _row("S", "40", None, "D"),
    ))))

    result = store.score()

    assert result.score == 2
    assert (
        f"{LABEL} net share buy +60 sh (dollars unverified: 2 of 2 P/S rows unpriced; {FULL_SPAN})"
        in result.signals
    )


def test_unpriced_buy_and_priced_sale_do_not_read_as_a_dollar_sale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store()
    # Codex's trace: P +100 at a missing price, S 10 at $10. The imputed-$0
    # dollar sum is -$100, but +90 shares were reported and the total is unknown.
    store.history(monkeypatch, _listing(_filing(LAST - timedelta(days=3), _table(
        _row("P", "100", None, "A"),
        _row("S", "10", "10.00", "D"),
    ))))

    result = store.score()

    assert result.score == 2
    assert (
        f"{LABEL} net share buy +90 sh (dollars unverified: 1 of 2 P/S rows unpriced; {FULL_SPAN})"
        in result.signals
    )
    assert not any("net sell" in s for s in result.signals)


def test_explicitly_fully_priced_offset_is_a_measured_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    store.history(monkeypatch, _listing(
        _filing(LAST - timedelta(days=3), _table(_row("P", "100", "10.00", "A"))),
        _filing(LAST - timedelta(days=40), _table(_row("S", "100", "10.00", "D"))),
    ))

    result = store.score()

    assert result.data_available is True
    assert result.score == 0
    assert f"{LABEL} buys and sells offset (fully priced, net $0; {FULL_SPAN})" in result.signals


def test_award_and_exercise_only_filing_scores_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    store.history(monkeypatch, _listing(_filing(LAST - timedelta(days=3), _table(
        _row("A", "5000", "0", "A"),
        _row("M", "2000", "5.00", "A"),
    ))))

    result = store.score()

    assert result.data_available is True
    assert result.score == 0
    assert (
        f"No reported non-derivative P/S insider transactions in the observed Form 4 data ({FULL_SPAN})"
        in result.signals
    )


def test_seven_day_cold_start_then_the_completed_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    filings = _listing(_filing(LAST - timedelta(days=3), _table(_row("P", "100000", "10.00", "A"))))
    cold = _Store()
    cold.pull(monkeypatch, filings, LAST)

    result = cold.score()

    # One routine pull proves 7 closed days: direction only, never +5.
    assert result.data_available is True
    assert result.score == 2
    assert result.trust == 0.5
    assert any("7d of 90d; short window, direction only" in s for s in result.signals)

    full = _Store()
    full.history(monkeypatch, filings)
    completed = full.score()
    assert completed.score == 5
    assert completed.trust == 0.85
    assert f"{LABEL} net buy $1,000,000 ({FULL_SPAN})" in completed.signals


def test_partial_listing_from_the_filing_cap_shortens_the_window(monkeypatch: pytest.MonkeyPatch) -> None:
    busy = LAST - timedelta(days=4)
    filings = _listing(
        *[_filing(busy, _table(_row("S", "10", "10.00", "D"))) for _ in range(25)],
        _filing(LAST - timedelta(days=60), _table(_row("P", "100000", "10.00", "A"))),
    )
    store = _Store()
    store.history(monkeypatch, filings)

    result = store.score()

    # The last pull read only 20 of busy's 25 filings, so it proves only the
    # dates after busy. The run is the 3 days busy+1 .. LAST-1: too short to score.
    assert result.data_available is False
    assert any("coverage too short" in s and "3d of 90d" in s for s in result.signals)


@pytest.mark.parametrize(
    ("filing_kind", "expect_available"),
    [("malformed", False), ("derivative_only", True)],
)
def test_malformed_table_vs_verified_derivative_only_filing(
    monkeypatch: pytest.MonkeyPatch, filing_kind: str, expect_available: bool,
) -> None:
    day = LAST - timedelta(days=3)
    odd = (
        _filing(day, malformed=True) if filing_kind == "malformed"
        else _filing(day, _derivative_only())
    )
    store = _Store()
    store.history(monkeypatch, _listing(odd))

    result = store.score()

    assert result.data_available is expect_available
    if expect_available:
        assert result.score == 0
        assert any(s.startswith("No reported non-derivative P/S") for s in result.signals)
    else:
        # The unread filing ends the run: only day+1 .. LAST-1 is verified.
        assert any("coverage too short" in s and "2d of 90d" in s for s in result.signals)


def test_recovery_pull_completes_the_window_without_rewriting_the_earlier_as_of(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filings = _listing(_filing(LAST - timedelta(days=3), _table(_row("P", "100000", "10.00", "A"))))
    store = _Store()
    store.history(monkeypatch, filings, skip=(2, 3))  # two weekly pulls missed

    before = store.score()
    # The missed pulls left LAST-28 .. LAST-15 unobserved: the run is the 14 days after.
    assert before.score == 2 and before.trust == 0.5
    assert any("14d of 90d" in s for s in before.signals)

    recovered = LAST + timedelta(days=1)
    store.pull(monkeypatch, filings, recovered, days_back=30)

    after = store.score(as_of=recovered)
    assert after.score == 5 and after.trust == 0.85
    assert store.score().signals == before.signals  # the earlier as-of read is unchanged


def test_legacy_and_partial_rows_are_not_scored(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    legacy_day = LAST - timedelta(days=10)
    with store.engine.begin() as c:
        # What the writer stored before 2026-11-23: imputed-$0 OPEN_MKT_* rows.
        c.execute(store.raw.insert(), [
            {"series_id": f"FORM4:EXMP:{field}", "source_id": EDGAR_SRC, "obs_date": legacy_day,
             "pull_timestamp": datetime.combine(legacy_day, time(22, 0)),
             "value": value, "pull_status": "SUCCESS"}
            for field, value in (
                ("OPEN_MKT_NET_SHARES", 90.0), ("OPEN_MKT_NET_VALUE", -100.0),
                ("OPEN_MKT_TXN_COUNT", 2.0), ("NET_SHARES", 5_090.0),
            )
        ])
    assert store.score().data_available is False

    store.history(monkeypatch, _listing(_filing(LAST - timedelta(days=3), _table(_row("P", "100", "10.00", "A")))))
    assert store.score().score == 2

    # A partial newer vintage of the last closed date (PS rows without the
    # counts) leaves that date with mixed vintages: nothing is verified.
    last_closed = LAST - timedelta(days=1)
    with store.engine.begin() as c:
        c.execute(store.raw.insert(), [
            {"series_id": f"FORM4:EXMP:{field}", "source_id": EDGAR_SRC, "obs_date": last_closed,
             "pull_timestamp": datetime.combine(LAST, time(16, 0)), "value": value,
             "pull_status": "SUCCESS"}
            for field, value in (("PS_NET_SHARES", -5_000.0), ("PS_NET_VALUE", -50_000.0),
                                 ("PS_TXN_COUNT", 1.0))
        ])

    result = store.score()

    assert result.data_available is False
    assert any("coverage not verified through the as-of date" in s for s in result.signals)
