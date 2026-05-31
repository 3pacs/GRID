"""SEC XBRL shares-outstanding ingestor for daily market_cap computation.

Sibling of ``sec_xbrl_financials.py``. Pulls ``CommonStockSharesOutstanding``
facts from the SEC XBRL Company Facts API, forward-fills quarterly XBRL
entries into daily shares, joins the daily ``raw_series`` close prices
(``YF:{TICKER}:close``), computes ``market_cap_usd = shares * close``, and
upserts into ``ticker_metrics_daily`` (migration 0025).

Data source:
    - https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
    - Ticker → CIK map via sec_xbrl_financials._fetch_ticker_to_cik_map()
    - Rate limit: 10 req/sec — we sleep 120ms between requests.

Forward-fill policy:
    XBRL shares are reported quarterly at filing time. For every trading
    day in the backfill window we use the most-recent XBRL entry whose
    ``filed`` date is <= the trading day. If no entry is available yet
    (e.g. a newly-IPO'd company), that day is skipped.

Idempotency:
    Upserts on UNIQUE(ticker, obs_date). Re-running refreshes
    ``close_price`` / ``market_cap_usd`` / ``as_of`` without creating
    duplicates. The ``source`` string is always ``"sec_xbrl + yfinance"``.

Graceful degradation:
    - Tickers with no CIK or no XBRL facts → logged, skipped, not fatal.
    - Tickers with no raw_series close prices → logged, skipped.
    - Network / HTTP errors on individual tickers → logged, skipped.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata.sec_xbrl_financials import (
    _RATE_LIMIT_DELAY_SECS,
    _fetch_company_facts,
    _fetch_ticker_to_cik_map,
    _sector_map_tickers,
)


# ── Constants ───────────────────────────────────────────────────────────

# XBRL tags we try, in priority order. us-gaap comes first because it's
# the canonical financial taxonomy; dei is the entity-info taxonomy which
# is used by many smaller filers that report shares under a DEI fact
# rather than a full us-gaap capital-stock fact.
# IFRS taxonomy tags for foreign issuers (20-F / 6-K filers).
#
# Foreign private issuers (TSM, NVO, BP, etc.) file on Form 20-F (annual)
# and 6-K (interim) and report share counts under the ``ifrs-full`` taxonomy,
# NOT us-gaap — so the us-gaap/dei specs above find nothing and the puller
# returned 0 rows for them. Verified live against SEC Company Facts:
#   * NVO  → ifrs-full:NumberOfSharesOutstanding
#   * TSM  → ifrs-full:NumberOfSharesIssuedAndFullyPaid (6-K/20-F, ~25.9B)
# These all report under the same "shares" unit as us-gaap, so the existing
# extraction path handles them once the tags are queried. Listed AFTER the
# us-gaap/dei specs so a dual-filer (e.g. BHP/RIO/AZN, which also expose
# dei:EntityCommonStockSharesOutstanding) still prefers the canonical tag.
_IFRS_SHARES_TAG_SPECS: list[tuple[str, str]] = [
    ("ifrs-full", "NumberOfSharesOutstanding"),
    ("ifrs-full", "NumberOfSharesIssuedAndFullyPaid"),
    ("ifrs-full", "NumberOfSharesIssued"),
    # Weighted-average fallback for IFRS filers that omit a point-in-time
    # count (mirrors the us-gaap weighted-average fallback below).
    ("ifrs-full", "WeightedAverageShares"),
    ("ifrs-full", "AdjustedWeightedAverageShares"),
]

# XBRL tags we try, in priority order. us-gaap comes first because it's the
# canonical financial taxonomy; dei is the entity-info taxonomy used by many
# smaller filers; ifrs-full covers foreign 20-F/6-K issuers.
_SHARES_TAG_SPECS: list[tuple[str, str]] = [
    # Highest-priority exact point-in-time tags.
    ("us-gaap", "CommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesIssued"),
    ("dei", "EntityCommonStockSharesOutstanding"),
    # IFRS point-in-time tags for foreign issuers.
    ("ifrs-full", "NumberOfSharesOutstanding"),
    ("ifrs-full", "NumberOfSharesIssuedAndFullyPaid"),
    ("ifrs-full", "NumberOfSharesIssued"),
    # Fallback: weighted-average basic/diluted shares. Modern tech
    # filers (META, etc.) only report these in Company Facts, not a
    # point-in-time outstanding count. They understate slightly
    # (weighted-average over the period) but beat having no value.
    ("us-gaap", "WeightedAverageNumberOfSharesOutstandingBasic"),
    ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"),
    # IFRS weighted-average fallbacks (e.g. TSM AdjustedWeightedAverageShares).
    ("ifrs-full", "WeightedAverageShares"),
    ("ifrs-full", "AdjustedWeightedAverageShares"),
]

# Units. XBRL reports shares as "shares" (a count unit, not USD).
_SHARES_UNIT: str = "shares"

# Foreign-issuer forms that report shares under IFRS. Exposed for the runner
# and for callers that want to confirm 20-F/6-K coverage.
_FOREIGN_ISSUER_FORMS: frozenset[str] = frozenset({"20-F", "6-K", "40-F"})

# Known foreign private issuers (ADRs) that file 20-F/6-K. Many are absent
# from the domestic sector map, so they're never attempted by default and
# show 0 rows. The runner's --foreign-issuers flag seeds this list so their
# IFRS (or dei) share counts get ingested.
FOREIGN_ISSUER_TICKERS: tuple[str, ...] = (
    "TSM", "ASML", "BHP", "RIO", "NVO", "AZN", "BP", "BABA", "JD",
    "SAP", "SHEL", "UL", "DEO", "SAN", "TD", "RY", "SNY", "GSK",
    "NVS", "TM", "SONY", "BTI", "FMX", "NSRGY", "BUD",
)


def ifrs_shares_tag_map() -> dict[str, tuple[str, ...]]:
    """Return the IFRS share-count tag map (taxonomy → tags), for inspection.

    Pure accessor so the foreign-issuer tag coverage can be unit-tested.
    """
    tags = tuple(tag for _tax, tag in _IFRS_SHARES_TAG_SPECS)
    return {"ifrs-full": tags}

_DEFAULT_BACKFILL_DAYS: int = 90
_MAX_RUNTIME_SECS: int = 3600


# ── ADR ratios ──────────────────────────────────────────────────────────
#
# SEC XBRL reports the foreign issuer's full *common-share* count
# (e.g. TSM = 25.9B Taiwan ordinary shares). However, the US ADR price
# in raw_series (YF:{TICKER}:close) is denominated *per ADR*. For ADRs
# where 1 ADR != 1 ordinary share, multiplying common shares by the
# ADR price overstates market cap by the ratio.
#
# Example: TSM = 25.9B common × $370 ADR = $9.59T (WRONG)
#          TSM ADR ratio = 5 → 25.9B / 5 × $370 = $1.92T (CORRECT)
#
# Lookup table of known ADR → ordinary-share ratios. Tickers not in
# this table default to 1 (1:1 listing or true US-domiciled common).
# When updating, the rule is: ratio = (number of ordinary shares
# represented by 1 ADR).
_ADR_RATIOS: dict[str, float] = {
    "TSM": 5,      # 1 ADR = 5 common shares
    "NVO": 1,      # 1 ADR = 1 share
    "AZN": 0.5,    # 1 ADR = 0.5 share
    "BABA": 8,     # 1 ADR = 8 ordinary shares
    "JD": 2,       # 1 ADR = 2 ordinary shares
    "BP": 6,       # 1 ADR = 6 ordinary shares
    "UL": 1,       # 1:1
    "DEO": 4,      # 1 ADR = 4 shares
    "SAN": 1,
    "TD": 1,
    "RY": 1,
    "SHOP": 1,
    "SE": 1,
    "BUD": 1,
    "HEINY": 2,
    "NSRGY": 1,
    "BTI": 1,
    "FMX": 10,     # 1 ADR = 10 shares
    "KOF": 10,
    "CCEP": 1,
    "TM": 10,
    "SNY": 2,
    "ASML": 1,
    "RIO": 1,
    "BHP": 2,
}


def _adr_ratio_for(ticker: str) -> float:
    """Return ordinary-shares-per-ADR for ``ticker``. Defaults to 1."""
    if not ticker:
        return 1.0
    try:
        return float(_ADR_RATIOS.get(ticker.strip().upper(), 1))
    except (TypeError, ValueError):
        return 1.0


# ── XBRL fact extraction ────────────────────────────────────────────────


def _extract_shares_entries(
    facts: dict[str, Any],
) -> list[tuple[date, int]]:
    """Return a sorted list of (filed_date, shares_outstanding) pairs.

    Merges facts from all candidate tag specs in ``_SHARES_TAG_SPECS``,
    deduplicates by ``filed`` date (keeping the highest-priority tag for
    each date), and returns newest-last so callers can walk in
    chronological order for forward-fill.
    """
    by_filed: dict[date, tuple[int, int]] = {}
    for priority, (taxonomy, tag) in enumerate(_SHARES_TAG_SPECS):
        try:
            entries = (
                facts.get("facts", {})
                .get(taxonomy, {})
                .get(tag, {})
                .get("units", {})
                .get(_SHARES_UNIT, [])
            )
        except AttributeError:
            continue
        if not isinstance(entries, list):
            continue
        for entry in entries:
            val = entry.get("val")
            filed_str = str(entry.get("filed") or "")
            if val is None or not filed_str:
                continue
            try:
                shares = int(float(val))
            except (TypeError, ValueError):
                continue
            if shares <= 0:
                continue
            try:
                filed = date.fromisoformat(filed_str)
            except ValueError:
                continue
            existing = by_filed.get(filed)
            # Lower priority number wins (us-gaap over dei).
            if existing is None or priority < existing[1]:
                by_filed[filed] = (shares, priority)

    pairs = [(filed, shares) for filed, (shares, _p) in by_filed.items()]
    pairs.sort(key=lambda p: p[0])
    return pairs


def _shares_for_date(
    shares_timeline: list[tuple[date, int]],
    obs: date,
) -> int | None:
    """Return the latest XBRL shares value known on or before ``obs``.

    ``shares_timeline`` must be sorted ascending by filed date. Returns
    None if no filing exists yet at ``obs``.
    """
    latest: int | None = None
    for filed, shares in shares_timeline:
        if filed > obs:
            break
        latest = shares
    return latest


# ── Raw series join ─────────────────────────────────────────────────────


_CLOSE_SQL = text(
    """
    SELECT obs_date, value
    FROM raw_series
    WHERE series_id = :sid
      AND pull_status = 'SUCCESS'
      AND value IS NOT NULL
      AND value > 0
      AND obs_date >= :start
      AND obs_date <= :end
    ORDER BY obs_date ASC
    """
)


def _fetch_close_prices(
    engine: Engine,
    ticker: str,
    start: date,
    end: date,
) -> list[tuple[date, float]]:
    """Return (obs_date, close_price) tuples from raw_series."""
    sid = f"YF:{ticker}:close"
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                _CLOSE_SQL.bindparams(sid=sid, start=start, end=end),
            ).fetchall()
    except Exception as exc:
        log.warning(
            "sec_xbrl_shares: close lookup failed {t}: {e}",
            t=ticker, e=str(exc),
        )
        return []
    out: list[tuple[date, float]] = []
    # raw_series can have multiple rows per obs_date (different pulls).
    # Keep the last one per date as the canonical close.
    by_date: dict[date, float] = {}
    for r in rows:
        try:
            by_date[r[0]] = float(r[1])
        except (TypeError, ValueError):
            continue
    for d in sorted(by_date.keys()):
        out.append((d, by_date[d]))
    return out


# ── DB writer ───────────────────────────────────────────────────────────
#
# Source-precedence policy (task #171, decided 2026-05-17):
# ---------------------------------------------------------
# Two writers target ticker_metrics_daily:
#   1. sec_xbrl_shares (this module) — writes all 5 columns
#      (shares_outstanding, close_price, market_cap_usd, source, as_of)
#      for a ~90-day rolling window where XBRL facts + raw_series prices
#      are both available. Source label: ``sec_xbrl + yfinance``.
#   2. scripts/td_backfill_universe.py — writes close_price ONLY for the
#      full freshness-audit ticker universe (365d window for DEAD bucket,
#      down to 60d for STALE_7_30). Source label: ``twelvedata_universe_backfill``.
#
# Decision: (c) Coalesce with SEC XBRL preference for the columns it owns.
#   - SEC XBRL is the canonical writer for shares_outstanding + market_cap_usd
#     (they are computed from regulator-filed facts, immune to data-provider
#     ticker mapping drift).
#   - Twelve Data is the canonical writer for close_price IN THE GAP
#     (older than XBRL's window OR more recent than XBRL's last raw_series
#     observation by 1-3 days).
#   - When the windows overlap on (ticker, obs_date), this SEC XBRL writer
#     wins on every column because XBRL's close_price comes from the same
#     raw_series row the rest of the pipeline reads, so the resulting
#     market_cap = shares × close stays internally consistent. TD's
#     close_price floating ~0.5% different would silently shift market_cap.
#   - TD must never overwrite shares_outstanding or market_cap_usd. Its
#     UPSERT clause has only ``close_price, source, as_of`` in the DO UPDATE
#     SET list, so those columns stay XBRL-set even after a TD overwrite.
#
# Trade-ticket extractor (#118) reads market_cap_usd from this table.
# Keeping XBRL as the canonical writer for that column means the trade
# ticket sizing logic always sees a regulator-grounded market cap, not
# a TD-API value that could lag a corporate action by a day.
#
# Tested by live PG smoke 2026-05-17 (task #164 sibling): with current
# writer ordering there are zero rows where TD has overwritten a
# market_cap-bearing XBRL row, confirming the policy holds in practice.
_UPSERT_SQL = text(
    """
    INSERT INTO ticker_metrics_daily (
        ticker, obs_date, shares_outstanding, close_price,
        market_cap_usd, source, as_of
    ) VALUES (
        :ticker, :obs_date, :shares, :close, :mcap, :source, NOW()
    )
    ON CONFLICT (ticker, obs_date) DO UPDATE SET
        shares_outstanding = EXCLUDED.shares_outstanding,
        close_price        = EXCLUDED.close_price,
        market_cap_usd     = EXCLUDED.market_cap_usd,
        source             = EXCLUDED.source,
        as_of              = NOW()
    """
)

_SOURCE_LABEL: str = "sec_xbrl + yfinance"


def _write_rows(
    engine: Engine,
    ticker: str,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert per-day market-cap rows. Returns count written."""
    if not rows:
        return 0
    written = 0
    try:
        with engine.begin() as conn:
            for r in rows:
                try:
                    conn.execute(
                        _UPSERT_SQL,
                        {
                            "ticker": ticker,
                            "obs_date": r["obs_date"],
                            "shares": r["shares"],
                            "close": r["close"],
                            "mcap": r["mcap"],
                            "source": _SOURCE_LABEL,
                        },
                    )
                    written += 1
                except Exception as exc:
                    log.warning(
                        "sec_xbrl_shares upsert failed {t} {d}: {e}",
                        t=ticker, d=r.get("obs_date"), e=str(exc),
                    )
    except Exception as exc:
        log.warning("sec_xbrl_shares txn failed {t}: {e}",
                    t=ticker, e=str(exc))
    return written


# ── Main runner ─────────────────────────────────────────────────────────


class SECXBRLSharesPuller:
    """Pulls SEC XBRL shares outstanding and writes ticker_metrics_daily rows.

    Attributes:
        engine: SQLAlchemy engine connected to griddb.
    """

    SOURCE_NAME: str = "SEC_XBRL_SHARES"

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine

    def pull_all(
        self,
        limit: int | None = None,
        backfill_days: int = _DEFAULT_BACKFILL_DAYS,
        tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Pull XBRL shares + close prices → ticker_metrics_daily.

        Parameters:
            limit: Maximum tickers to process (None = all).
            backfill_days: How many days back from today to compute rows.
            tickers: Optional explicit ticker list (overrides sector map).

        Returns:
            Per-ticker result dicts plus a final SUMMARY entry.
        """
        start_ts = time.monotonic()

        universe = (
            [t.strip().upper() for t in tickers if t and t.strip()]
            if tickers
            else _sector_map_tickers()
        )
        if not universe:
            log.error("sec_xbrl_shares: empty universe — aborting")
            return [{"status": "FAILED", "error": "empty universe"}]

        cik_map = _fetch_ticker_to_cik_map()
        if not cik_map:
            log.error("sec_xbrl_shares: CIK map empty — aborting")
            return [{"status": "FAILED", "error": "cik map empty"}]
        time.sleep(_RATE_LIMIT_DELAY_SECS)

        today = date.today()
        window_start = today - timedelta(days=backfill_days)

        results: list[dict[str, Any]] = []
        processed = 0
        rows_total = 0
        no_shares: list[str] = []

        for ticker in universe:
            if limit is not None and processed >= limit:
                log.info("sec_xbrl_shares: limit {n} reached", n=limit)
                break
            if time.monotonic() - start_ts > _MAX_RUNTIME_SECS:
                log.warning("sec_xbrl_shares: runtime cap reached")
                break

            cik_padded = cik_map.get(ticker)
            if not cik_padded:
                results.append({
                    "ticker": ticker, "status": "NO_CIK", "rows": 0,
                })
                processed += 1
                continue

            facts = _fetch_company_facts(cik_padded)
            time.sleep(_RATE_LIMIT_DELAY_SECS)
            if facts is None:
                results.append({
                    "ticker": ticker, "status": "NO_FACTS", "rows": 0,
                })
                processed += 1
                continue

            timeline = _extract_shares_entries(facts)
            if not timeline:
                no_shares.append(ticker)
                results.append({
                    "ticker": ticker, "status": "NO_SHARES", "rows": 0,
                })
                processed += 1
                continue

            closes = _fetch_close_prices(
                self.engine, ticker, window_start, today,
            )
            if not closes:
                results.append({
                    "ticker": ticker, "status": "NO_PRICES", "rows": 0,
                })
                processed += 1
                continue

            adr_ratio = _adr_ratio_for(ticker)
            day_rows: list[dict[str, Any]] = []
            for obs, close in closes:
                raw_shares = _shares_for_date(timeline, obs)
                if raw_shares is None:
                    continue
                # ADR adjustment: divide ordinary-share count by the
                # ADR ratio so (shares * ADR price) yields the correct
                # market cap. For non-ADRs and 1:1 listings the ratio
                # is 1.0 and this is a no-op.
                try:
                    adj_shares = int(round(float(raw_shares) / adr_ratio))
                except (TypeError, ValueError, ZeroDivisionError):
                    adj_shares = int(raw_shares)
                try:
                    mcap = float(adj_shares) * float(close)
                except (TypeError, ValueError):
                    continue
                day_rows.append({
                    "obs_date": obs,
                    "shares": adj_shares,
                    "close": close,
                    "mcap": mcap,
                })

            written = _write_rows(self.engine, ticker, day_rows)
            rows_total += written

            log.info(
                "sec_xbrl_shares: {t} → {w} daily rows "
                "(latest_shares={s:,})",
                t=ticker, w=written,
                s=(timeline[-1][1] if timeline else 0),
            )
            results.append({
                "ticker": ticker,
                "cik": cik_padded,
                "status": "SUCCESS" if written > 0 else "NO_ROWS",
                "rows": written,
                "latest_shares": timeline[-1][1] if timeline else None,
            })
            processed += 1

        elapsed = time.monotonic() - start_ts
        log.info(
            "sec_xbrl_shares complete — {p} tickers, {r} rows, "
            "{n} without shares tag, {e:.1f}s",
            p=processed, r=rows_total, n=len(no_shares), e=elapsed,
        )
        results.append({
            "status": "SUMMARY",
            "tickers_processed": processed,
            "rows_written": rows_total,
            "no_shares_count": len(no_shares),
            "no_shares_sample": no_shares[:10],
            "elapsed_seconds": round(elapsed, 1),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        })
        return results


if __name__ == "__main__":
    from db import get_engine

    puller = SECXBRLSharesPuller(db_engine=get_engine())
    out = puller.pull_all(limit=5, backfill_days=30)
    for r in out:
        print(r)
