"""
GRID SEC EDGAR data ingestion module.

Pulls institutional holdings (13F), insider transactions (Form 4), and
corporate event filings (8-K) from SEC EDGAR using the ``edgartools`` library.
Stores results in ``raw_series`` with source 'SEC_EDGAR'.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
from edgar import Company, get_filings, set_identity
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.altdata.verified_13f_filers import VERIFIED_FILERS
from ingestion.base import BasePuller

# 13F filer CIK numbers for institutional-holdings tracking.
#
# GD0 §1.3 / §6 item 4 (owner decision, adopted 2026-09-28; corrected
# 2026-09-28 -- see ``ingestion/altdata/verified_13f_filers.py``'s module
# docstring): this module used to carry its own hardcoded 50-fund CIK list
# that disagreed with both ``ingestion/altdata/institutional_flows.py`` and
# ``ingestion/altdata/sec_13f_live.py`` on the same CIK for different funds
# (e.g. CIK 1167483 was "Eton Park Capital" here, "Elliott Management" in
# institutional_flows.py, and "Tiger Global Management" -- SEC's verified
# registrant name). This is now a derived, zero-padded view over the single
# verified 13F filer-CIK map (the union of all three old lists, each CIK
# checked against SEC's own registrant data), not a second copy that can
# drift out of sync. 72 verified filers, up from this module's old 50 (2
# managers -- Balyasny Asset Management, GIC Private Limited -- could not be
# matched to any SEC-registered filer after multiple search attempts and are
# documented as dropped in verified_13f_filers.py, not silently omitted).
TOP_HEDGE_FUND_CIKS: list[str] = [f.cik.zfill(10) for f in VERIFIED_FILERS]

# Rate limit between EDGAR requests (be polite to SEC servers)
_RATE_LIMIT_DELAY: float = 0.12  # SEC asks for <=10 req/sec


class EDGARPuller(BasePuller):
    """Pulls SEC filing data from EDGAR into ``raw_series``.

    Supports three filing types:
    - 13F-HR: Quarterly institutional holdings
    - Form 4: Daily insider transactions
    - 8-K: Corporate event filings (used for sector velocity)

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for SEC_EDGAR.
    """

    SOURCE_NAME = "SEC_EDGAR"
    SOURCE_CONFIG = {"base_url": "https://www.sec.gov/cgi-bin/browse-edgar", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 12}

    def __init__(
        self,
        db_engine: Engine,
        identity: str = "GRID Trading System grid@localhost",
    ) -> None:
        """Initialise the EDGAR puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            identity: User-agent identity string for SEC EDGAR compliance.
        """
        set_identity(identity)
        super().__init__(db_engine)
        log.info("EDGARPuller initialised — source_id={sid}", sid=self.source_id)

    # ------------------------------------------------------------------
    # 13F Institutional Holdings
    # ------------------------------------------------------------------

    @staticmethod
    def _pick_value_column(holdings: pd.DataFrame) -> str | None:
        """Return the infotable's market-value column, or ``None``.

        Case-insensitive, preferring ``Value`` (edgartools 5.x) then
        ``value`` (4.x / raw XML), mirroring
        ``ingestion.altdata.sec_13f_live._infotable_df_to_positions``.
        """
        cols = {str(c).lower(): c for c in holdings.columns}
        for name in ("Value", "value"):
            hit = cols.get(name.lower())
            if hit is not None:
                return str(hit)
        return None

    def pull_13f_holdings(
        self,
        cik_list: list[str] | None = None,
        max_filings_per_fund: int = 4,
    ) -> dict[str, Any]:
        """Pull 13F-HR institutional holdings for top hedge funds.

        Stores aggregated portfolio metrics as series:
        - 13F:{CIK}:TOTAL_VALUE — total portfolio market value
        - 13F:{CIK}:NUM_POSITIONS — count of distinct holdings
        - 13F:{CIK}:TOP5_CONCENTRATION — top 5 holdings as % of total

        Parameters:
            cik_list: CIK numbers to pull. Defaults to TOP_HEDGE_FUND_CIKS.
            max_filings_per_fund: How many recent 13F filings to process per fund.

        Returns:
            dict: Result summary with rows_inserted and any errors.
        """
        if cik_list is None:
            cik_list = TOP_HEDGE_FUND_CIKS

        log.info("Pulling 13F holdings for {n} funds", n=len(cik_list))
        total_inserted = 0
        errors: list[str] = []

        for cik in cik_list:
            try:
                # Per-entity lookup via the submissions API, exactly like the
                # Form 4 path below. edgartools' module-level ``get_filings``
                # is the quarterly full-index downloader and has never taken a
                # ``cik`` kwarg (signature: year/quarter/form/amendments/
                # filing_date/index/priority_sorted_forms), so the original
                # ``get_filings(form="13F-HR", cik=cik)`` raised TypeError for
                # every filer -- grid-hermes 2026-10-20 04:20:55Z: "13F pull
                # failed for CIK 0001067983: get_filings() got an unexpected
                # keyword argument 'cik'" x75, 0 rows.
                filings = Company(cik).get_filings(form="13F-HR")
                if filings is None:
                    continue

                processed = 0
                for filing in filings:
                    if processed >= max_filings_per_fund:
                        break

                    try:
                        thirteenf = filing.obj()
                        holdings = thirteenf.infotable

                        if holdings is None or holdings.empty:
                            processed += 1
                            continue

                        filing_date = (
                            filing.filing_date
                            if isinstance(filing.filing_date, date)
                            else pd.Timestamp(filing.filing_date).date()
                        )

                        # Resolve the value column case-insensitively:
                        # edgartools 5.x (5.28.4 on grid-hermes, 5.61.1
                        # locally) emits ``Value``; 4.x / the raw parser emit
                        # ``value`` (same rule as sec_13f_live's
                        # _infotable_df_to_positions). Testing for lowercase
                        # ``"value"`` only would write TOTAL_VALUE=0.0 and
                        # TOP5_CONCENTRATION=0.0 as SUCCESS for every filing.
                        value_col = self._pick_value_column(holdings)
                        if value_col is None:
                            log.debug(
                                "13F infotable for CIK {cik} ({d}) has no value "
                                "column (columns={cols}); skipping filing",
                                cik=cik, d=filing_date, cols=list(holdings.columns),
                            )
                            processed += 1
                            continue

                        # Compute portfolio metrics
                        total_value = float(holdings[value_col].sum())
                        num_positions = len(holdings)

                        # Top 5 concentration
                        if total_value > 0:
                            top5_value = float(
                                holdings.nlargest(5, value_col)[value_col].sum()
                            )
                            top5_pct = top5_value / total_value
                        else:
                            top5_pct = 0.0

                        metrics = {
                            f"13F:{cik}:TOTAL_VALUE": total_value,
                            f"13F:{cik}:NUM_POSITIONS": float(num_positions),
                            f"13F:{cik}:TOP5_CONCENTRATION": round(top5_pct, 6),
                        }

                        with self.engine.begin() as conn:
                            for sid, val in metrics.items():
                                if not self._row_exists(sid, filing_date, conn):
                                    conn.execute(
                                        text(
                                            "INSERT INTO raw_series "
                                            "(series_id, source_id, obs_date, value, "
                                            "raw_payload, pull_status) "
                                            "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                                        ),
                                        {
                                            "sid": sid,
                                            "src": self.source_id,
                                            "od": filing_date,
                                            "val": val,
                                            "payload": json.dumps({
                                                "cik": cik,
                                                "filing_date": str(filing_date),
                                                "num_positions": num_positions,
                                            }),
                                        },
                                    )
                                    total_inserted += 1

                        processed += 1
                    except Exception as exc:
                        log.debug(
                            "Could not parse 13F filing for CIK {cik}: {e}",
                            cik=cik, e=str(exc),
                        )
                        processed += 1

                    time.sleep(_RATE_LIMIT_DELAY)

            except Exception as exc:
                msg = f"13F pull failed for CIK {cik}: {exc}"
                log.warning(msg)
                errors.append(msg)

            time.sleep(_RATE_LIMIT_DELAY)

        log.info("13F pull complete — {n} rows inserted", n=total_inserted)
        return {
            "type": "13F",
            "rows_inserted": total_inserted,
            "funds_attempted": len(cik_list),
            "errors": errors,
            "status": "SUCCESS" if not errors else "PARTIAL",
        }

    # ------------------------------------------------------------------
    # Form 4 Insider Transactions
    # ------------------------------------------------------------------

    def pull_form4_transactions(
        self,
        tickers: list[str] | None = None,
        days_back: int = 7,
    ) -> dict[str, Any]:
        """Pull Form 4 insider transactions for tracked companies.

        Stores daily aggregated insider activity:
        - FORM4:{ticker}:NET_SHARES — net shares bought/sold
        - FORM4:{ticker}:NET_VALUE — net dollar value of transactions
        - FORM4:{ticker}:TXN_COUNT — number of transactions

        Parameters:
            tickers: Company tickers to track. Defaults to major indices components.
            days_back: How many days of recent filings to process.

        Returns:
            dict: Result summary.
        """
        if tickers is None:
            tickers = [
                "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
                "BRK-B", "JPM", "V", "JNJ", "UNH", "HD", "PG", "MA",
                "XOM", "BAC", "ABBV", "KO", "PFE", "COST", "MRK", "TMO",
                "AVGO", "PEP",
            ]

        log.info("Pulling Form 4 for {n} tickers", n=len(tickers))
        total_inserted = 0
        errors: list[str] = []

        for ticker in tickers:
            try:
                company = Company(ticker)
                form4_filings = company.get_filings(form="4")

                if form4_filings is None:
                    continue

                # Aggregate transactions by date
                daily_agg: dict[date, dict[str, float]] = {}

                for filing in form4_filings[:20]:  # Limit to recent filings
                    try:
                        filing_date = (
                            filing.filing_date
                            if isinstance(filing.filing_date, date)
                            else pd.Timestamp(filing.filing_date).date()
                        )

                        cutoff = date.today() - timedelta(days=days_back)
                        if filing_date < cutoff:
                            break

                        f4 = filing.obj()
                        if not hasattr(f4, "transactions") or f4.transactions is None:
                            continue

                        txns = f4.transactions
                        if hasattr(txns, "empty") and txns.empty:
                            continue

                        if filing_date not in daily_agg:
                            daily_agg[filing_date] = {
                                "net_shares": 0.0,
                                "net_value": 0.0,
                                "txn_count": 0.0,
                            }

                        for _, txn in txns.iterrows():
                            shares = float(txn.get("shares", txn.get("transactionShares", 0)) or 0)
                            price = float(txn.get("pricePerShare", txn.get("price", 0)) or 0)
                            acq_disp = str(txn.get("acquiredDisposedCode", txn.get("acquired_disposed", "A")))

                            if acq_disp == "D":
                                shares = -shares

                            daily_agg[filing_date]["net_shares"] += shares
                            daily_agg[filing_date]["net_value"] += shares * price
                            daily_agg[filing_date]["txn_count"] += 1

                    except Exception as exc:
                        log.debug(
                            "Could not parse Form 4 for {t}: {e}",
                            t=ticker, e=str(exc),
                        )

                    time.sleep(_RATE_LIMIT_DELAY)

                # Insert aggregated daily data
                with self.engine.begin() as conn:
                    for fdate, agg in daily_agg.items():
                        metrics = {
                            f"FORM4:{ticker}:NET_SHARES": agg["net_shares"],
                            f"FORM4:{ticker}:NET_VALUE": agg["net_value"],
                            f"FORM4:{ticker}:TXN_COUNT": agg["txn_count"],
                        }
                        for sid, val in metrics.items():
                            if not self._row_exists(sid, fdate, conn):
                                conn.execute(
                                    text(
                                        "INSERT INTO raw_series "
                                        "(series_id, source_id, obs_date, value, "
                                        "pull_status) "
                                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                    ),
                                    {
                                        "sid": sid,
                                        "src": self.source_id,
                                        "od": fdate,
                                        "val": val,
                                    },
                                )
                                total_inserted += 1

            except Exception as exc:
                msg = f"Form 4 pull failed for {ticker}: {exc}"
                log.warning(msg)
                errors.append(msg)

        log.info("Form 4 pull complete — {n} rows inserted", n=total_inserted)
        return {
            "type": "FORM4",
            "rows_inserted": total_inserted,
            "tickers_attempted": len(tickers),
            "errors": errors,
            "status": "SUCCESS" if not errors else "PARTIAL",
        }

    # ------------------------------------------------------------------
    # 8-K Filing Counts by Sector
    # ------------------------------------------------------------------

    # Process-scoped breaker for the daily 8-K full-index endpoint. SEC
    # answers 403 (not 404) for a full-index path that does not exist yet,
    # which happens for the first day or two of a new calendar quarter
    # before ``full-index/<year>/QTR<n>/`` is generated. That case is "no
    # data yet", so trip the breaker once and stay quiet until the index
    # appears. A 403 on a *past* quarter's index -- whose files certainly
    # exist -- or on any other endpoint is an access denial and must stay a
    # visible failure (grid-hermes 2026-10-20 04:21:55Z returned 403 for
    # .../full-index/2026/QTR1/form.gz in October and was mislabelled
    # "index likely not yet generated for current quarter").
    _PULL_8K_BREAKER: bool = False
    _PULL_8K_BREAKER_TS: float = 0.0
    _PULL_8K_BREAKER_TTL_S: int = 6 * 3600  # 6 hours
    _FULL_INDEX_QTR_RE = re.compile(r"/full-index/(\d{4})/QTR([1-4])/")

    @classmethod
    def _full_index_403_quarter(cls, err_str: str) -> tuple[int, int] | None:
        """Return ``(year, quarter)`` named by a full-index URL in ``err_str``.

        ``None`` when the message is not a full-index 403 or names no
        parseable ``/full-index/<year>/QTR<n>/`` path.
        """
        if "403" not in err_str or "full-index" not in err_str:
            return None
        m = cls._FULL_INDEX_QTR_RE.search(err_str)
        if m is None:
            return None
        return int(m.group(1)), int(m.group(2))

    @staticmethod
    def _current_year_quarter(today: date | None = None) -> tuple[int, int]:
        """Calendar ``(year, quarter)`` for ``today`` (default ``date.today()``)."""
        today = today or date.today()
        return today.year, (today.month - 1) // 3 + 1

    def pull_8k_counts(
        self,
        days_back: int = 7,
    ) -> dict[str, Any]:
        """Pull 8-K filing counts as a velocity metric.

        Counts total 8-K filings in the recent window and stores as
        series 8K:TOTAL:COUNT. Sector-level breakdown is handled by
        sec_velocity.py for deeper historical analysis.

        Parameters:
            days_back: How many days of recent filings to count.

        Returns:
            dict: Result summary.
        """
        # Honour the 403-cooldown breaker — return SKIPPED with no log noise.
        import time as _t
        now = _t.time()
        if (
            EDGARPuller._PULL_8K_BREAKER
            and now - EDGARPuller._PULL_8K_BREAKER_TS
            < EDGARPuller._PULL_8K_BREAKER_TTL_S
        ):
            return {
                "type": "8K",
                "rows_inserted": 0,
                "errors": [],
                "status": "SKIPPED",
            }
        # Cooldown expired — clear the breaker and try again.
        EDGARPuller._PULL_8K_BREAKER = False

        log.info("Pulling 8-K filing counts (last {d} days)", d=days_back)
        total_inserted = 0
        errors: list[str] = []

        try:
            filings = get_filings(form="8-K")
            if filings is None:
                return {
                    "type": "8K",
                    "rows_inserted": 0,
                    "errors": ["No filings returned"],
                    "status": "PARTIAL",
                }

            # Count filings by date
            daily_counts: dict[date, int] = {}
            cutoff = date.today() - timedelta(days=days_back)

            for filing in filings:
                try:
                    filing_date = (
                        filing.filing_date
                        if isinstance(filing.filing_date, date)
                        else pd.Timestamp(filing.filing_date).date()
                    )

                    if filing_date < cutoff:
                        break

                    daily_counts[filing_date] = daily_counts.get(filing_date, 0) + 1
                except Exception as exc:
                    log.warning("EDGAR filing parse failed: {e}", e=exc)
                    continue

            with self.engine.begin() as conn:
                for fdate, count in daily_counts.items():
                    sid = "8K:TOTAL:COUNT"
                    if not self._row_exists(sid, fdate, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, "
                                "pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {
                                "sid": sid,
                                "src": self.source_id,
                                "od": fdate,
                                "val": float(count),
                            },
                        )
                        total_inserted += 1

        except Exception as exc:
            err_str = str(exc)
            # SEC full-index 403 on the CURRENT calendar quarter
            # (e.g. .../full-index/2026/QTR4/form.gz on 2026-10-01): the
            # quarter's index isn't generated yet. That is "no data yet",
            # not a failure: trip the breaker, log ONE warning, retry after
            # the cooldown. Any other 403 -- a past quarter's index (which
            # exists) or a non-index endpoint -- is a real access denial and
            # falls through to the error path below, unmasked.
            idx_quarter = self._full_index_403_quarter(err_str)
            if idx_quarter is not None and idx_quarter == self._current_year_quarter():
                EDGARPuller._PULL_8K_BREAKER = True
                EDGARPuller._PULL_8K_BREAKER_TS = _t.time()
                log.warning(
                    "EDGAR full-index returned 403 for the current quarter "
                    "{y}Q{q} — index not yet generated. Skipping 8-K count "
                    "pull for {h}h. Endpoint: {e}",
                    y=idx_quarter[0],
                    q=idx_quarter[1],
                    h=EDGARPuller._PULL_8K_BREAKER_TTL_S // 3600,
                    e=err_str[:200],
                )
                return {
                    "type": "8K",
                    "rows_inserted": 0,
                    "errors": [err_str],
                    "status": "SKIPPED",
                }
            if idx_quarter is not None:
                msg = (
                    f"8-K count pull failed: EDGAR full-index returned 403 for "
                    f"past quarter {idx_quarter[0]}Q{idx_quarter[1]} (index exists; "
                    f"access denied, not a missing index): {exc}"
                )
            else:
                msg = f"8-K count pull failed: {exc}"
            log.error(msg)
            errors.append(msg)

        log.info("8-K count pull complete — {n} rows inserted", n=total_inserted)
        return {
            "type": "8K",
            "rows_inserted": total_inserted,
            "errors": errors,
            "status": "SUCCESS" if not errors else "PARTIAL",
        }

    def pull_all(self) -> list[dict[str, Any]]:
        """Run all EDGAR pulls sequentially.

        Returns:
            list[dict]: Results from 13F, Form 4, and 8-K pulls.
        """
        log.info("Starting full EDGAR pull")
        results = []

        results.append(self.pull_13f_holdings())
        results.append(self.pull_form4_transactions())
        results.append(self.pull_8k_counts())

        total_rows = sum(r["rows_inserted"] for r in results)
        log.info("Full EDGAR pull complete — {n} total rows", n=total_rows)
        return results


if __name__ == "__main__":
    from db import get_engine

    puller = EDGARPuller(db_engine=get_engine())

    # Quick test: just pull 8-K counts
    result = puller.pull_8k_counts(days_back=3)
    print(f"8-K counts: {result}")
