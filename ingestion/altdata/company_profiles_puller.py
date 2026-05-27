"""Company-profile / market-cap enrichment puller for GRID.

The ``company_profiles`` table (schema owned by ``intelligence.company_analyzer``)
is read by the context provider, business-news parser, news→signals resolver,
and social-attention enrichment, but nothing keeps **market cap** fresh for the
small-cap biotech tickers surfaced by the Trial Gem Hunter. The trial signal's
regime gate and position sizing depend on ``mcap < $2B`` — stale or missing
market caps silently break that gate.

This puller resolves the universe of trial tickers (from ``catalyst_calendar``
and ``trial_signals``) and upserts name / sector / market cap into
``company_profiles`` (market cap stored inside the existing JSONB ``profile``
column so no schema change is needed). It reuses the existing :class:`FMPPuller`
profile/quote endpoints (FMP free tier) and degrades gracefully when no key is
configured.

Series/table written:
    company_profiles(ticker, name, sector, profile{market_cap,...}, last_analyzed)
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata.fmp_puller import FMPPuller
from intelligence.company_analyzer import ensure_table

# Be polite to the FMP free tier (250 req/day, ~no per-second cap documented,
# but we space calls so a large universe doesn't burst).
_RATE_LIMIT_DELAY: float = 0.3


def _to_float(value: Any) -> float | None:
    """Coerce an API numeric field to float, or None if missing/invalid."""
    if value is None or value == "":
        return None
    try:
        f = float(value)
    except (ValueError, TypeError):
        return None
    if f != f:  # NaN
        return None
    return f


def shape_profile_row(
    ticker: str,
    quote: dict[str, Any] | None,
    profile: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Shape an FMP quote + profile into a ``company_profiles`` upsert row.

    Pure function (no I/O) so the row shaping is unit-testable offline.

    Market cap is taken from the quote's ``marketCap`` first (real-time),
    falling back to the profile's ``mktCap``. Name/sector come from the
    profile, falling back to the quote's ``name``.

    Parameters:
        ticker: Stock ticker (will be upper-cased and stripped).
        quote: FMP ``quote/{ticker}`` dict (has price, marketCap), or None.
        profile: FMP ``profile/{ticker}`` dict (has companyName, sector), or None.

    Returns:
        Dict with keys ``ticker, name, sector, profile, last_analyzed`` ready
        to bind into the upsert, or ``None`` if the ticker is blank or no
        usable market cap / identity could be resolved.
    """
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return None

    quote = quote or {}
    profile = profile or {}

    market_cap = _to_float(quote.get("marketCap"))
    if market_cap is None:
        market_cap = _to_float(profile.get("mktCap"))

    price = _to_float(quote.get("price"))

    name = (
        profile.get("companyName")
        or quote.get("name")
        or profile.get("name")
        or None
    )
    sector = profile.get("sector") or None
    industry = profile.get("industry") or None
    exchange = profile.get("exchangeShortName") or quote.get("exchange") or None

    # Require at least an identity OR a market cap — otherwise there's nothing
    # worth persisting (avoids inserting empty shells for delisted tickers).
    if market_cap is None and name is None:
        return None

    profile_json = {
        "ticker": ticker,
        "market_cap": market_cap,
        "price": price,
        "name": name,
        "sector": sector,
        "industry": industry,
        "exchange": exchange,
        "source": "fmp",
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "ticker": ticker,
        "name": name,
        "sector": sector,
        "profile": profile_json,
        "last_analyzed": datetime.now(timezone.utc),
    }


_UPSERT_SQL = (
    "INSERT INTO company_profiles "
    "(ticker, name, sector, profile, last_analyzed) "
    "VALUES (:ticker, :name, :sector, :profile, :last_analyzed) "
    "ON CONFLICT (ticker) DO UPDATE SET "
    "name = COALESCE(EXCLUDED.name, company_profiles.name), "
    "sector = COALESCE(EXCLUDED.sector, company_profiles.sector), "
    # Merge JSONB so existing analyzer-written keys (narrative, suspicion
    # inputs) survive while market-cap fields are refreshed.
    "profile = COALESCE(company_profiles.profile, '{}'::jsonb) || EXCLUDED.profile, "
    "last_analyzed = EXCLUDED.last_analyzed"
)


class CompanyProfilesPuller:
    """Enriches ``company_profiles`` with market cap for trial tickers.

    Attributes:
        engine: SQLAlchemy engine connected to the GRID database.
        fmp: Underlying FMP puller used for quote/profile fetches.
    """

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        self.engine = db_engine
        self.fmp = FMPPuller(db_engine, api_key=api_key)

    # ------------------------------------------------------------------
    # Universe resolution
    # ------------------------------------------------------------------

    def _trial_tickers(self) -> list[str]:
        """Collect distinct tickers from the trial catalyst tables.

        Reads ``catalyst_calendar`` and ``trial_signals`` defensively — either
        table may be absent in a partial deployment.

        Returns:
            Sorted list of upper-cased, deduplicated tickers.
        """
        tickers: set[str] = set()
        queries = (
            "SELECT DISTINCT ticker FROM catalyst_calendar WHERE ticker IS NOT NULL",
            "SELECT DISTINCT ticker FROM trial_signals WHERE ticker IS NOT NULL",
        )
        with self.engine.connect() as conn:
            for q in queries:
                try:
                    rows = conn.execute(text(q)).fetchall()
                except Exception as exc:  # noqa: BLE001
                    log.debug("company_profiles: source query skipped: {e}", e=str(exc))
                    continue
                for (tk,) in rows:
                    if tk:
                        tickers.add(str(tk).strip().upper())
        return sorted(tickers)

    # ------------------------------------------------------------------
    # Enrichment
    # ------------------------------------------------------------------

    def enrich_ticker(self, ticker: str) -> dict[str, Any] | None:
        """Fetch + shape a single ticker's profile row (no DB write)."""
        quote = self.fmp.pull_quote(ticker)
        profile = self.fmp.pull_profile(ticker)
        return shape_profile_row(ticker, quote, profile)

    def pull(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Enrich market caps for the given tickers (or all trial tickers).

        Parameters:
            tickers: Explicit ticker list; defaults to the trial-ticker union.

        Returns:
            dict with status, rows_upserted, tickers_attempted, errors.
        """
        if not self.fmp.api_key:
            log.warning("company_profiles: FMP_API_KEY not set — puller disabled")
            return {"status": "DISABLED", "rows_upserted": 0, "reason": "no_api_key"}

        ensure_table(self.engine)

        if tickers is None:
            tickers = self._trial_tickers()
        if not tickers:
            log.info("company_profiles: no trial tickers to enrich")
            return {"status": "SUCCESS", "rows_upserted": 0, "tickers_attempted": 0}

        upserted = 0
        errors: list[str] = []

        for ticker in tickers:
            try:
                row = self.enrich_ticker(ticker)
                if row is None:
                    continue
                row_to_bind = dict(row)
                row_to_bind["profile"] = json.dumps(row["profile"])
                with self.engine.begin() as conn:
                    conn.execute(text(_UPSERT_SQL), row_to_bind)
                upserted += 1
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "company_profiles: enrichment failed for {t}: {e}",
                    t=ticker, e=str(exc),
                )
                errors.append(f"{ticker}: {exc}")
            time.sleep(_RATE_LIMIT_DELAY)

        log.info(
            "company_profiles: {n} rows upserted from {t} trial tickers",
            n=upserted, t=len(tickers),
        )
        return {
            "status": "SUCCESS",
            "rows_upserted": upserted,
            "tickers_attempted": len(tickers),
            "errors": errors,
        }
