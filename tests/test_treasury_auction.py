"""CAT-25 — Treasury auction puller tests."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch


from ingestion.altdata.treasury_auction import (
    TreasuryAuctionPuller,
    AuctionRow,
)


def _build_puller():
    puller = TreasuryAuctionPuller.__new__(TreasuryAuctionPuller)
    puller.engine = MagicMock()
    puller.source_id = 42
    return puller


def _mock_resp(payload):
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = payload
    return r


TODAY = date.today()
RECENT_ISO = TODAY.isoformat()
OLD_ISO = (TODAY - timedelta(days=120)).isoformat()


class TestFetchAuctions:
    def test_http_failure(self):
        puller = _build_puller()
        with patch(
            "ingestion.altdata.treasury_auction.requests.get",
            side_effect=RuntimeError("down"),
        ):
            assert puller._fetch_auctions() == []

    def test_non_list_payload(self):
        puller = _build_puller()
        with patch(
            "ingestion.altdata.treasury_auction.requests.get",
            return_value=_mock_resp({"error": "bad"}),
        ):
            assert puller._fetch_auctions() == []

    def test_filters_by_type(self):
        puller = _build_puller()
        payload = [
            {"type": "Bill", "auctionDate": RECENT_ISO, "cusip": "A"},
            {"type": "Bond", "auctionDate": RECENT_ISO, "cusip": "B"},
            {"type": "StrangeType", "auctionDate": RECENT_ISO, "cusip": "C"},
        ]
        with patch(
            "ingestion.altdata.treasury_auction.requests.get",
            return_value=_mock_resp(payload),
        ):
            result = puller._fetch_auctions()
        assert len(result) == 2
        assert {r["cusip"] for r in result} == {"A", "B"}

    def test_filters_by_date(self):
        puller = _build_puller()
        payload = [
            {"type": "Bill", "auctionDate": RECENT_ISO, "cusip": "A"},
            {"type": "Bill", "auctionDate": OLD_ISO, "cusip": "B"},
        ]
        with patch(
            "ingestion.altdata.treasury_auction.requests.get",
            return_value=_mock_resp(payload),
        ):
            result = puller._fetch_auctions(lookback_days=60)
        assert len(result) == 1
        assert result[0]["cusip"] == "A"


class TestParseAuction:
    def test_extracts_all_metrics(self):
        puller = _build_puller()
        row = {
            "cusip": "91282CAL5",
            "auctionDate": RECENT_ISO,
            "type": "Note",
            "term": "10-Year",
            "bidToCoverRatio": "2.45",
            "highYield": "4.325",
            "indirectBidderAcceptedPct": "68.5",
            "directBidderAcceptedPct": "15.2",
            "primaryDealerAcceptedPct": "16.3",
        }
        rows = puller._parse_auction(row)
        metrics = {r.metric for r in rows}
        assert "bid_to_cover" in metrics
        assert "stop_yield" in metrics
        assert "indirect_pct" in metrics
        assert "direct_pct" in metrics
        assert "primary_dealer_pct" in metrics

    def test_missing_cusip_empty(self):
        puller = _build_puller()
        assert puller._parse_auction({"auctionDate": RECENT_ISO}) == []

    def test_bad_date_empty(self):
        puller = _build_puller()
        assert puller._parse_auction({
            "cusip": "A", "auctionDate": "not-a-date",
        }) == []

    def test_non_numeric_metric_skipped(self):
        puller = _build_puller()
        row = {
            "cusip": "A",
            "auctionDate": RECENT_ISO,
            "bidToCoverRatio": "not a number",
            "highYield": "4.5",
        }
        rows = puller._parse_auction(row)
        metrics = {r.metric for r in rows}
        assert "stop_yield" in metrics
        assert "bid_to_cover" not in metrics


class TestUpsertRows:
    def test_empty(self):
        puller = _build_puller()
        assert puller._upsert_rows([]) == 0

    def test_skips_existing(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn
        puller._get_existing_dates = MagicMock(
            return_value={date(2026, 4, 1)},
        )
        inserts = []

        def capture(q, p=None):
            if "INSERT INTO raw_series" in str(q):
                inserts.append(p)
            return MagicMock()

        conn.execute = capture
        rows = [
            AuctionRow("A", "bid_to_cover", date(2026, 4, 1), 2.45, "10-Year"),
            AuctionRow("A", "bid_to_cover", date(2026, 4, 8), 2.50, "10-Year"),
        ]
        inserted = puller._upsert_rows(rows)
        assert inserted == 1


class TestPullAll:
    def test_orchestrator(self):
        puller = _build_puller()
        fake_auction = {
            "cusip": "A", "auctionDate": RECENT_ISO,
            "type": "Note", "term": "10-Year",
            "bidToCoverRatio": "2.45", "highYield": "4.3",
        }
        with patch.object(puller, "_fetch_auctions", return_value=[fake_auction]), \
             patch.object(puller, "_upsert_rows", return_value=2):
            result = puller.pull_all()
        assert result["auctions"] == 1
        assert result["inserted"] == 2
