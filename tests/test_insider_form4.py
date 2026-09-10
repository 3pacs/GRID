"""Form 4 transaction codes reach the insider motivation model (hand-off 05).

Covers the three legs of the fix:
  1. ``ingestion/altdata/insider_filings.py`` parses the transaction code, the
     Rule 10b5-1 flag (``<aff10b5One>`` element or footnote text) and the
     direct/indirect ownership nature, and carries them into ``signal_value``.
  2. ``intelligence/lever_pullers.assess_motivation`` ranks an insider trade
     from those fields across both insider feeds, and
     ``_motivation_narrative`` prints the code and the plan flag.
  3. ``scripts/backfill_form4_codes.py`` patches historical rows with a
     bounded, parameterized jsonb merge.

No DB, no network: the puller is exercised on fixture XML with a stub
connection, and the backfill against a fake engine.
"""
from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence import lever_pullers as lp

ROOT = Path(__file__).resolve().parents[1]


# ── Fixture Form 4 documents ─────────────────────────────────────────────

# A planned sale: the filing-level <aff10b5One> attestation is set.
FORM4_PLANNED_SALE = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <aff10b5One>1</aff10b5One>
  <issuer><issuerTradingSymbol>ACME</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Doe Jane</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><officerTitle>Chief Executive Officer</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-09-01</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>12000</value></transactionShares>
        <transactionPricePerShare><value>50.00</value></transactionPricePerShare>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

# A pre-X0508 filing: no <aff10b5One>, the plan is only named in a footnote.
FORM4_FOOTNOTE_PLAN = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <issuer><issuerTradingSymbol>ACME</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Roe Richard</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><officerTitle>Director</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-09-02</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>4000</value></transactionShares>
        <transactionPricePerShare><value>100.00</value></transactionPricePerShare>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>I</value></directOrIndirectOwnership>
      </ownershipNature>
      <footnoteId id="F1"/>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <footnotes>
    <footnote id="F1">This sale was effected pursuant to a Rule 10b5-1 trading plan adopted on March 3, 2026.</footnote>
  </footnotes>
</ownershipDocument>
"""

# A discretionary open-market purchase: no plan anywhere.
FORM4_DISCRETIONARY_BUY = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <issuer><issuerTradingSymbol>ACME</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Poe Pat</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><officerTitle>Chief Financial Officer</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-09-03</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>2000</value></transactionShares>
        <transactionPricePerShare><value>75.00</value></transactionPricePerShare>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
      <footnoteId id="F1"/>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <footnotes>
    <footnote id="F1">Shares held in a family trust.</footnote>
  </footnotes>
</ownershipDocument>
"""


@pytest.fixture()
def puller() -> Any:
    """An InsiderFilingsPuller with the DB side of BasePuller stubbed out."""
    from ingestion.altdata import insider_filings

    obj = object.__new__(insider_filings.InsiderFilingsPuller)
    obj.engine = MagicMock()
    obj.source_id = 1
    return obj


# ── 1. Puller parsing ────────────────────────────────────────────────────


def test_parses_document_level_10b5_1_attestation(puller: Any) -> None:
    trades = puller._parse_form4_xml(FORM4_PLANNED_SALE)
    assert len(trades) == 1
    trade = trades[0]
    assert trade["transaction_code"] == "S"
    assert trade["is_10b5_1"] is True
    assert trade["direct_or_indirect"] == "D"
    # price stays the per-share trade price (trust_scorer._extract_price)
    assert trade["price"] == 50.0
    assert trade["value"] == 600_000.0


def test_parses_footnote_10b5_1_plan(puller: Any) -> None:
    trade = puller._parse_form4_xml(FORM4_FOOTNOTE_PLAN)[0]
    assert trade["is_10b5_1"] is True
    assert trade["direct_or_indirect"] == "I"
    assert trade["insider_title"] == "Director"


def test_discretionary_trade_has_no_plan_flag(puller: Any) -> None:
    trade = puller._parse_form4_xml(FORM4_DISCRETIONARY_BUY)[0]
    assert trade["transaction_code"] == "P"
    assert trade["is_10b5_1"] is False
    assert trade["transaction_type"] == "BUY"


@pytest.mark.parametrize(
    "raw,expected",
    [("1", True), ("true", True), ("Y", True), ("0", False), ("", False), (None, False)],
)
def test_xml_flag_parsing(raw: str | None, expected: bool) -> None:
    from ingestion.altdata.insider_filings import _xml_flag_is_true

    assert _xml_flag_is_true(raw) is expected


@pytest.mark.parametrize(
    "note",
    [
        "Sold under a Rule 10b5-1 plan.",
        "Pursuant to a 10b5-1(c) trading plan",
        "effected under the reporting person's 10B5-1 plan",
    ],
)
def test_footnote_plan_wording_variants(note: str) -> None:
    from ingestion.altdata.insider_filings import _RULE_10B5_1_RE

    assert _RULE_10B5_1_RE.search(note)


def test_unrelated_footnote_is_not_a_plan() -> None:
    from ingestion.altdata.insider_filings import _RULE_10B5_1_RE

    assert _RULE_10B5_1_RE.search("Shares held in a family trust.") is None


def test_emit_signal_carries_code_plan_and_ownership(puller: Any) -> None:
    trade = puller._parse_form4_xml(FORM4_PLANNED_SALE)[0]
    conn = MagicMock()
    puller._emit_signal(conn, trade)

    params = conn.execute.call_args[0][1]
    payload = json.loads(params["sval"])
    assert payload["transaction_code"] == "S"
    assert payload["is_10b5_1"] is True
    assert payload["direct_or_indirect"] == "D"
    # Unchanged contract: the trade price per share and the existing keys.
    assert payload["price"] == 50.0
    assert payload["value"] == 600_000.0
    assert payload["is_unusual_size"] is True
    # signal_type semantics and the conflict key are untouched.
    assert params["stype2"] == "UNUSUAL_SELL"
    assert params["stype"] == "insider"
    assert params["sid"] == "Doe Jane"


# ── 2. Motivation rules ──────────────────────────────────────────────────


def _insider(recent: list[dict] | None = None) -> lp.LeverPuller:
    return lp.LeverPuller(
        id="insider:x", name="x", category="insider", influence_rank=0.6,
        trust_score=0.7, position="p", motivation_model="unknown",
        recent_actions=recent or [],
    )


@pytest.mark.parametrize(
    "signal_type,details,expected",
    [
        # A plan flag beats everything, however big the sale.
        ("UNUSUAL_SELL", {"transaction_code": "S", "is_10b5_1": True, "value": 9_000_000}, "routine"),
        # Mechanical codes are routine regardless of size or seniority.
        ("BUY", {"transaction_code": "M", "value": 5_000_000, "insider_title": "CEO"}, "routine"),
        ("BUY", {"transaction_code": "A", "value": 5_000_000}, "routine"),
        ("SELL", {"transaction_code": "F", "value": 5_000_000}, "routine"),
        ("SELL", {"transaction_code": "G", "value": 5_000_000}, "routine"),
        ("BUY", {"transaction_code": "X", "value": 5_000_000}, "routine"),
        # P: informed on size ...
        ("BUY", {"transaction_code": "P", "value": 150_000, "insider_title": ""}, "likely_informed"),
        # ... or on seniority, even when small.
        ("BUY", {"transaction_code": "P", "value": 5_000, "insider_title": "Chief Executive Officer"}, "likely_informed"),
        ("BUY", {"transaction_code": "P", "value": 5_000, "insider_title": "Director"}, "likely_informed"),
        # A small purchase by a rank-and-file filer is not a signal, and a
        # Vice President is not C-suite however the word "president" reads.
        ("BUY", {"transaction_code": "P", "value": 5_000, "insider_title": ""}, "routine"),
        ("BUY", {"transaction_code": "P", "value": 5_000, "insider_title": "Vice President, Sales"}, "routine"),
        # S: discretionary and large is informed; small is housekeeping.
        ("SELL", {"transaction_code": "S", "value": 300_000}, "likely_informed"),
        ("SELL", {"transaction_code": "S", "value": 60_000}, "routine"),
        # No code at all: the puller's own direction still ranks the row.
        ("UNUSUAL_SELL", {"value": 900_000}, "likely_informed"),
        ("BUY", {"value": 1_000}, "routine"),
        # Cluster buys carry total_value rather than value.
        ("CLUSTER_BUY", {"total_value": 2_000_000, "insider_count": 3}, "likely_informed"),
        # An unranked code with no direction is the only remaining unknown.
        ("insider_activity", {"transaction_code": "J"}, "unknown"),
    ],
)
def test_insider_motivation_rules(
    signal_type: str, details: dict, expected: str,
) -> None:
    action = {"ticker": "ACME", "signal_type": signal_type, "details": details}
    assert lp.assess_motivation(_insider(), action, engine=object()) == expected


@pytest.mark.parametrize(
    "details,expected",
    [
        # QuiverQuant labels every row insider_sell; the acquired/disposed
        # code is the real direction.
        ({"TransactionCode": "P", "AcquiredDisposedCode": "A",
          "Shares": 5000.0, "PricePerShare": 40.0, "officerTitle": ""}, "likely_informed"),
        ({"TransactionCode": "S", "AcquiredDisposedCode": "D",
          "Shares": 100.0, "PricePerShare": 10.0}, "routine"),
        ({"TransactionCode": "S", "AcquiredDisposedCode": "D",
          "Shares": 50_000.0, "PricePerShare": 25.0}, "likely_informed"),
        # Option exercise is mechanical even though it acquires shares.
        ({"TransactionCode": "X", "AcquiredDisposedCode": "A",
          "Shares": 50_000.0, "PricePerShare": 25.0}, "routine"),
        # isDirector stands in for a title the feed leaves blank.
        ({"TransactionCode": "P", "AcquiredDisposedCode": "A", "Shares": 10.0,
          "PricePerShare": 5.0, "officerTitle": "", "isDirector": True}, "likely_informed"),
    ],
)
def test_quiverquant_insider_rows_are_ranked(details: dict, expected: str) -> None:
    action = {"ticker": "ACME", "signal_type": "insider_sell", "details": details}
    assert lp.assess_motivation(_insider(), action, engine=object()) == expected


def test_quiverquant_sell_label_no_longer_forces_unknown() -> None:
    """The regression this hand-off exists to fix: 'insider_sell' + QQ payload."""
    action = {
        "ticker": "ACME",
        "signal_type": "insider_sell",
        "details": {
            "Name": "Offer Or", "Shares": 86300.0, "PricePerShare": 12.0,
            "TransactionCode": "S", "AcquiredDisposedCode": "D",
            "directOrIndirectOwnership": "D", "isOfficer": True,
        },
    }
    assert lp.assess_motivation(_insider(), action, engine=object()) != "unknown"


# ── 3. Narrative ─────────────────────────────────────────────────────────


def test_narrative_prints_code_and_plan_flag() -> None:
    text = lp._motivation_narrative(
        "routine", _insider(), "ACME",
        {"transaction_code": "S", "is_10b5_1": True},
    )
    assert "code S" in text and "open-market sale" in text
    assert "under a Rule 10b5-1 plan" in text


def test_narrative_says_when_there_is_no_plan_flag() -> None:
    text = lp._motivation_narrative(
        "likely_informed", _insider(), "ACME",
        {"transaction_code": "P", "is_10b5_1": False},
    )
    assert "code P" in text and "open-market purchase" in text
    assert "no Rule 10b5-1 plan flag" in text


def test_narrative_reads_the_quiverquant_key() -> None:
    text = lp._motivation_narrative(
        "routine", _insider(), "ACME", {"TransactionCode": "M"},
    )
    assert "code M" in text and "option exercise" in text


def test_narrative_admits_a_missing_code() -> None:
    text = lp._motivation_narrative("routine", _insider(), "ACME", {"value": 10})
    assert "Form 4 code not recorded" in text
    # The old text claimed a 10b5-1 plan for every routine sell; it must not.
    assert "under a Rule 10b5-1 plan" not in text


def test_unknown_insider_narrative_still_names_the_gap() -> None:
    text = lp._motivation_narrative("unknown", _insider(), "ACME", {"transaction_code": "J"})
    assert text.startswith("Unknown motivation")
    assert "code J" in text


# ── 4. QuiverQuant direction at the source ───────────────────────────────


@pytest.mark.parametrize(
    "rec,expected",
    [
        ({"AcquiredDisposedCode": "A", "TransactionCode": "P"}, "insider_buy"),
        ({"AcquiredDisposedCode": "D", "TransactionCode": "S"}, "insider_sell"),
        # No acquired/disposed code: fall back to the transaction code.
        ({"TransactionCode": "P"}, "insider_buy"),
        ({"TransactionCode": "S"}, "insider_sell"),
        # Neither: the legacy TransactionType path, still supported.
        ({"TransactionType": "Purchase"}, "insider_buy"),
        ({}, "insider_sell"),
    ],
)
def test_quiverquant_insider_signal_type(rec: dict, expected: str) -> None:
    from ingestion.altdata.quiverquant import _insider_signal_type

    assert _insider_signal_type(rec) == expected


# ── 5. Backfill ──────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows: list[Any], rowcount: int = 1) -> None:
        self._rows = rows
        self.rowcount = rowcount

    def mappings(self) -> "_FakeResult":
        return self

    def all(self) -> list[Any]:
        return self._rows

    def __iter__(self) -> Any:
        return iter(self._rows)


class _FakeConn:
    """Records every (sql, params) pair and replays canned results in order."""

    def __init__(self, results: list[_FakeResult]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: Any, params: dict | None = None) -> _FakeResult:
        self.calls.append((str(sql), params or {}))
        return self._results.pop(0) if self._results else _FakeResult([], rowcount=1)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


class _FakeEngine:
    def __init__(self, read: _FakeConn, write: _FakeConn) -> None:
        self._read = read
        self._write = write

    def connect(self) -> _FakeConn:
        return self._read

    def begin(self) -> _FakeConn:
        return self._write


def _backfill_engine(
    candidates: list[dict], payload_rows: list[tuple[date, dict]],
) -> tuple[Any, _FakeConn, _FakeConn]:
    read = _FakeConn([_FakeResult(candidates), _FakeResult(payload_rows)])
    write = _FakeConn([])
    return _FakeEngine(read, write), read, write


def test_backfill_patches_only_the_recoverable_fields() -> None:
    from scripts import backfill_form4_codes as bf

    day = date.today() - timedelta(days=3)
    candidates = [{
        "source_id": "Doe Jane", "ticker": "ACME",
        "signal_date": day, "signal_type": "UNUSUAL_SELL",
    }]
    payloads = [(day, {
        "ticker": "ACME", "insider_name": "Doe Jane",
        "transaction_code": "s", "is_10b5_1": True, "direct_or_indirect": "d",
    })]
    engine, _read, write = _backfill_engine(candidates, payloads)

    stats = bf.backfill(engine, days=120, dry_run=False)

    assert stats == {
        "examined": 1, "patched": 1,
        "skipped_no_payload": 0, "skipped_no_fields": 0,
    }
    _sql, params = write.calls[0]
    patch = json.loads(params["patch"])
    assert patch == {
        "transaction_code": "S", "is_10b5_1": True, "direct_or_indirect": "D",
    }
    assert params["signal_date"] == day
    assert params["source_type"] == "insider"


def test_backfill_dry_run_writes_nothing() -> None:
    from scripts import backfill_form4_codes as bf

    day = date.today() - timedelta(days=3)
    candidates = [{
        "source_id": "Doe Jane", "ticker": "ACME",
        "signal_date": day, "signal_type": "SELL",
    }]
    payloads = [(day, {
        "ticker": "ACME", "insider_name": "Doe Jane", "transaction_code": "S",
    })]
    engine, _read, write = _backfill_engine(candidates, payloads)

    stats = bf.backfill(engine, days=120, dry_run=True)

    assert stats["patched"] == 1
    assert write.calls == []


def test_backfill_skips_rows_with_no_matching_filing() -> None:
    from scripts import backfill_form4_codes as bf

    day = date.today() - timedelta(days=3)
    candidates = [{
        "source_id": "Nobody", "ticker": "ZZZZ",
        "signal_date": day, "signal_type": "SELL",
    }]
    engine, _read, write = _backfill_engine(candidates, [])

    stats = bf.backfill(engine, days=120, dry_run=False)

    assert stats["skipped_no_payload"] == 1 and stats["patched"] == 0
    assert write.calls == []


def test_backfill_skips_a_payload_with_nothing_to_add() -> None:
    from scripts import backfill_form4_codes as bf

    day = date.today() - timedelta(days=3)
    candidates = [{
        "source_id": "Doe Jane", "ticker": "ACME",
        "signal_date": day, "signal_type": "SELL",
    }]
    # An old payload with no code and no plan flag: nothing is recoverable, so
    # nothing is stamped and a later run can still fill it.
    payloads = [(day, {"ticker": "ACME", "insider_name": "Doe Jane"})]
    engine, _read, write = _backfill_engine(candidates, payloads)

    stats = bf.backfill(engine, days=120, dry_run=False)

    assert stats["skipped_no_fields"] == 1 and write.calls == []


def test_backfill_sql_shape_is_bounded_parameterized_and_idempotent() -> None:
    from scripts import backfill_form4_codes as bf

    select_sql = str(bf._SELECT_CANDIDATES)
    payload_sql = str(bf._SELECT_PAYLOADS)
    update_sql = str(bf._UPDATE_ROW)

    # Bounded on both sides of the date column (raw_series is a hypertable).
    for sql, column in ((select_sql, "signal_date"), (payload_sql, "obs_date")):
        assert f"{column} >= :start_date" in sql
        assert f"{column} <= :end_date" in sql

    # Idempotent: only rows still missing the code are selected or updated.
    assert "NOT (signal_value ? 'transaction_code')" in select_sql
    assert "NOT (signal_value ? 'transaction_code')" in update_sql

    # jsonb merge, not a replacement, and never a literal patch.
    assert "signal_value || CAST(:patch AS jsonb)" in update_sql

    # The whole natural key is bound, so one row is touched per statement.
    for key in ("source_type", "source_id", "ticker", "signal_date", "signal_type"):
        assert f":{key}" in update_sql

    # decision_journal is never written.
    for sql in (select_sql, payload_sql, update_sql):
        assert "decision_journal" not in sql


def test_backfill_module_uses_no_sql_string_interpolation() -> None:
    source = (ROOT / "scripts" / "backfill_form4_codes.py").read_text()
    for line in source.splitlines():
        stripped = line.strip()
        if not re.search(r"\b(SELECT|UPDATE|INSERT|DELETE)\b", stripped):
            continue
        assert not stripped.startswith(("f'", 'f"')), stripped
        assert ".format(" not in stripped, stripped
