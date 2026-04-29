"""CAT-25 — Treasury auction tail + bid-to-cover puller.

Pulls US Treasury auction results from Treasury Direct's public API
(no key required). Each auction emits:

  bid_to_cover     Total bids / accepted bids — demand measure
  stop_yield       The yield at which bidding ceased
  when_issued_bp   Difference between stop yield and the when-issued
                   (pre-auction) yield, in basis points. Positive =
                   tail (weak demand); negative = stop-through (strong).
  indirect_pct     Fraction awarded to indirect bidders (foreign CBs,
                   pension funds)
  direct_pct       Fraction to direct bidders (banks, domestic institutions)

Why this matters (Tier A catalog #25): auction tails signal foreign
demand weakness BEFORE it shows up in TIC flows (which lag by ~45
days). A +3bp tail on a 10Y auction is historically correlated with
a 5-8bp widening in the 10Y yield over the next 5 trading days.

Public API
----------
  GET https://www.treasurydirect.gov/TA_WS/securities/announced

No auth. Returns JSON list of announced + recently-auctioned
securities. We filter to ``type`` in (Bill, Note, Bond, TIPS, FRN)
and ``auctionDate`` within the lookback window.

Storage: raw_series 'treasury_auction:<cusip>:<metric>' — one row
per (auction, metric) pair.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text

from ingestion.base import BasePuller


TD_BASE = "https://www.treasurydirect.gov/TA_WS/securities/auctioned"
_LOOKBACK_DAYS = 60
_AUCTION_TYPES = {"Bill", "Note", "Bond", "TIPS", "FRN"}


@dataclass
class AuctionRow:
    cusip: str
    metric: str
    obs_date: date
    value: float
    term: str  # e.g. "10-Year"


class TreasuryAuctionPuller(BasePuller):
    """Pulls Treasury auction results from Treasury Direct public API."""

    SOURCE_NAME = "treasury_auction"
    SOURCE_CONFIG = {
        "base_url": TD_BASE,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 22,
    }

    def __init__(self, db_engine) -> None:
        super().__init__(db_engine)

    # ── Fetch ─────────────────────────────────────────────────────────

    def _fetch_auctions(
        self, *, lookback_days: int = _LOOKBACK_DAYS, timeout: float = 15.0,
    ) -> list[dict[str, Any]]:
        """Pull the full auctioned-securities feed and filter by date."""
        try:
            resp = requests.get(TD_BASE, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            log.warning("treasury_auction fetch failed: {e}", e=str(exc))
            return []

        if not isinstance(payload, list):
            log.warning("treasury_auction: unexpected payload shape")
            return []

        cutoff = date.today() - timedelta(days=lookback_days)
        filtered: list[dict[str, Any]] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            if row.get("type") not in _AUCTION_TYPES:
                continue
            auction_date_str = row.get("auctionDate") or ""
            try:
                auction_date = date.fromisoformat(auction_date_str[:10])
            except ValueError:
                continue
            if auction_date < cutoff:
                continue
            filtered.append(row)
        return filtered

    # ── Parse ─────────────────────────────────────────────────────────

    def _parse_auction(self, row: dict[str, Any]) -> list[AuctionRow]:
        """Extract metric rows from one Treasury Direct auction record."""
        cusip = str(row.get("cusip") or "").strip()
        if not cusip:
            return []
        try:
            auction_date = date.fromisoformat(row.get("auctionDate", "")[:10])
        except ValueError:
            return []
        term = str(row.get("term") or "")

        results: list[AuctionRow] = []

        def _add(metric: str, raw: Any) -> None:
            if raw is None or raw == "":
                return
            try:
                val = float(raw)
            except (TypeError, ValueError):
                return
            results.append(AuctionRow(
                cusip=cusip,
                metric=metric,
                obs_date=auction_date,
                value=val,
                term=term,
            ))

        _add("bid_to_cover", row.get("bidToCoverRatio"))
        _add("stop_yield", row.get("highYield") or row.get("highDiscountRate"))
        _add("indirect_pct", row.get("indirectBidderAcceptedPct"))
        _add("direct_pct", row.get("directBidderAcceptedPct"))
        _add("primary_dealer_pct", row.get("primaryDealerAcceptedPct"))

        # Tail computation: stop_yield - when_issued (not directly in feed)
        # so we leave it as None here; it's computed downstream by joining
        # with yield curve series.
        return results

    # ── Upsert ────────────────────────────────────────────────────────

    def _upsert_rows(self, rows: list[AuctionRow]) -> int:
        if not rows:
            return 0
        inserted = 0
        with self.engine.begin() as conn:
            # Group by series_id for dedup
            by_series: dict[str, list[AuctionRow]] = {}
            for r in rows:
                series_id = f"treasury_auction:{r.cusip}:{r.metric}"
                by_series.setdefault(series_id, []).append(r)

            for series_id, batch in by_series.items():
                existing = self._get_existing_dates(series_id, conn)
                for r in batch:
                    if r.obs_date in existing:
                        continue
                    try:
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, "
                                " pull_status, pull_timestamp) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS', :ts)"
                            ),
                            {
                                "sid": series_id,
                                "src": self.source_id,
                                "od": r.obs_date,
                                "val": r.value,
                                "ts": datetime.now(timezone.utc),
                            },
                        )
                        inserted += 1
                    except Exception as exc:  # noqa: BLE001
                        log.debug(
                            "treasury_auction insert failed {s} {d}: {e}",
                            s=series_id, d=r.obs_date, e=str(exc),
                        )
        return inserted

    # ── Orchestrator ──────────────────────────────────────────────────

    def pull_all(
        self, *, lookback_days: int = _LOOKBACK_DAYS,
    ) -> dict[str, Any]:
        raw_auctions = self._fetch_auctions(lookback_days=lookback_days)
        all_rows: list[AuctionRow] = []
        for auction in raw_auctions:
            all_rows.extend(self._parse_auction(auction))
        inserted = self._upsert_rows(all_rows)
        log.info(
            "treasury_auction: {a} auctions, {r} metric rows, {i} new",
            a=len(raw_auctions), r=len(all_rows), i=inserted,
        )
        return {
            "auctions": len(raw_auctions),
            "rows": len(all_rows),
            "inserted": inserted,
        }


def run_treasury_auction_puller(engine) -> dict[str, Any]:
    puller = TreasuryAuctionPuller(db_engine=engine)
    return puller.pull_all()
