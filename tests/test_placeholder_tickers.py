"""Placeholder tickers ("NONE", "N/A", "NAN") never become signal_sources rows.

Unlisted issuers file Form 4s with the symbol "NONE", and the insider puller
stored them: 21 CLUSTER_BUY rows on "NONE" from 2026-03-25 to 2026-11-05
pooled unrelated private-fund filers (Blackstone, Qatar Investment
Authority, Neuberger Berman ...) into one fake cluster. QuiverQuant wrote
"N/A" (insider funds) and "NAN" (64 off-exchange rows). The writers now skip
them and Sleuth ignores the historical rows.

No DB, no network: fixture XML, a fake upsert engine and a stub connection.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from ingestion.altdata import insider_filings
from ingestion.altdata import quiverquant as qq
from intelligence import sleuth_intersections as si

FORM4 = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <issuer><issuerTradingSymbol>{symbol}</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Loeb Daniel S</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-11-03</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>160000</value></transactionShares>
        <transactionPricePerShare><value>25.00</value></transactionPricePerShare>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>I</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


@pytest.fixture()
def puller():
    obj = object.__new__(insider_filings.InsiderFilingsPuller)
    obj.engine = MagicMock()
    obj.source_id = 1
    return obj


@pytest.mark.parametrize("symbol", ["NONE", "none", "N/A", "NA", "NULL", "-"])
def test_form4_for_an_unlisted_issuer_yields_no_trade(puller, symbol):
    assert puller._parse_form4_xml(FORM4.format(symbol=symbol)) == []


def test_form4_for_a_listed_issuer_still_parses(puller):
    (trade,) = puller._parse_form4_xml(FORM4.format(symbol="acme"))
    assert trade["ticker"] == "ACME"


class _Engine:
    def __init__(self):
        self.table: dict = {}

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        pass

    def execute(self, statement, params):
        self.table[(params["source_type"], params["ticker"], params["signal_date"])] = (
            json.loads(params["signal_value"]))


@pytest.mark.parametrize("bad", ["N/A", "NAN", "nan", float("nan"), "None", None, ""])
def test_quiverquant_skips_placeholder_tickers(bad):
    engine = _Engine()
    records = [{"Ticker": bad, "Date": "2026-11-06", "OTC_Short_Volume": 5},
               {"Ticker": "acme", "Date": "2026-11-06", "OTC_Short_Volume": 7}]
    stored = qq._store_signals(engine, records, qq.ENDPOINTS["off_exchange"]["source_type"],
                               "off_exchange")
    assert stored == 1
    assert [k[1] for k in engine.table] == ["ACME"]


def test_sleuth_ignores_historical_placeholder_rows():
    end = date(2026, 11, 7)
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        (1, "insider", "cluster_none", "NONE", end, "CLUSTER_BUY", {}),
        (2, "quiverquant:insider", "qq", "N/A", end, "insider_buy", {}),
        (3, "insider", "CEO", "nvda", end, "BUY", {}),
    ]
    obs = si.collect_signal_observations(conn, date(2026, 10, 24), end,
                                         datetime(2026, 11, 8, tzinfo=timezone.utc))
    assert [(o.ticker, o.citation["id"]) for o in obs] == [("NVDA", 3)]
