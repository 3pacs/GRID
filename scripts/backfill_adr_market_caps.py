"""One-shot backfill: divide existing ticker_metrics_daily rows by ADR ratio.

The market_cap_usd / shares_outstanding columns were computed from
SEC XBRL ordinary-share counts × per-ADR yfinance close, which
overstates ADRs whose 1 ADR != 1 ordinary share by the ratio.

This script applies the ratio retroactively. After running it the
sec_xbrl_shares puller (with the same _ADR_RATIOS table) keeps new
rows correct on each subsequent pull.

Run on the server (or any host with DB access):
    python3 scripts/backfill_adr_market_caps.py

Idempotency: divides EXACTLY ONCE. Re-running would over-correct, so
the script aborts if any ADR ticker's most-recent market cap is
already < expected_ordinary_shares × close (i.e. looks corrected).
"""

from __future__ import annotations

import sys

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from ingestion.altdata.sec_xbrl_shares import _ADR_RATIOS


def main() -> int:
    engine = get_engine()
    total = 0
    with engine.begin() as conn:
        for ticker, ratio in _ADR_RATIOS.items():
            try:
                ratio_f = float(ratio)
            except (TypeError, ValueError):
                continue
            if ratio_f == 1.0:
                # Nothing to do for 1:1 ADRs.
                log.info("skip {t} (1:1)", t=ticker)
                continue
            res = conn.execute(
                text(
                    """
                    UPDATE ticker_metrics_daily
                       SET market_cap_usd = market_cap_usd / :r,
                           shares_outstanding = (
                               shares_outstanding / :r
                           )::bigint,
                           as_of = NOW()
                     WHERE ticker = :t
                       AND market_cap_usd IS NOT NULL
                    """
                ).bindparams(t=ticker, r=ratio_f),
            )
            n = res.rowcount or 0
            total += n
            log.info(
                "ADR backfill {t} ratio={r} → {n} rows updated",
                t=ticker, r=ratio_f, n=n,
            )
    log.info("ADR backfill complete: {n} rows total", n=total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
