"""Which TIINGO-source price rows are this instrument's prices.

``ingestion.tiingo_pull.pull_ticker`` used to strip ``=F``/``=X`` before
calling Tiingo, and Tiingo answered for whatever equity held the remainder.
Verified on grid-svr 2026-11-09: TIINGO's ``YF:CL=F:close`` is
Colgate-Palmolive (history from 1990), ``HG=F`` Hamilton Insurance, ``ZS=F``
Zscaler, ``BRL=X`` Barr Pharmaceuticals. Because yfinance outranks TIINGO in
the resolver but has gaps, ``cl_close`` took Colgate's price on 2,722 of 9,304
dates (all of 1990-01-02..2000-08-22) and ``copper`` took Hamilton's on 7
dates in 2026. Tiingo also wrote rows for acquired symbols after their last
real session (K, CYBR, X, VRE, ARCH: 2026-06-04..06-10), which belong to some
other listing.

#1078 stopped new writes. These rules keep the rows already stored out of
every reader: the resolver, the completed-close reader and the
``store.observations`` window/latest/vintage readers.
"""

from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache

TIINGO_SOURCE = "TIINGO"

# Oracle tickers Tiingo's daily equity endpoint cannot serve (crypto, the
# dollar index); see TiingoPuller.oracle_tickers.
_ORACLE_NON_TIINGO: frozenset[str] = frozenset({"BTC", "ETH", "SOL", "DXY"})


def is_tiingo_equity(ticker: str) -> bool:
    """False for symbols Tiingo's daily equity endpoint cannot serve.

    Not just a missed fetch: the puller used to strip ``=F``/``=X`` and Tiingo
    answered for whatever equity holds the remainder (see the module
    docstring). Six-letter FX pairs (``EURUSD=X``) are real FX on this
    endpoint (match yfinance to 1e-3), so they stay. A dotted exchange suffix
    (``ATD.TO``, ``DX-Y.NYB``) is a non-US or index listing; a one-letter
    share class (``MOG.A``) is left to analysis/sector_map_ticker_status.yaml,
    since Tiingo's spelling varies (``MOG-A``, ``KELYA``).
    """
    if ticker in _ORACLE_NON_TIINGO or ticker.endswith("-USD") or ticker.startswith("^"):
        return False
    if ticker.endswith("=X"):
        base = ticker[:-2]
        return len(base) == 6 and base.isalpha()
    if "=" in ticker:
        return False
    _, dot, suffix = ticker.rpartition(".")
    return not (dot and len(suffix) >= 2)


@lru_cache(maxsize=1)
def _last_sessions() -> dict[str, date]:
    """``ticker -> last real session`` from the sector map's status sidecar."""
    try:
        from analysis.sector_map import TICKER_STATUS
    except Exception:  # noqa: BLE001 -- no sidecar: only the symbol rule applies
        return {}
    out: dict[str, date] = {}
    for ticker, entry in TICKER_STATUS.items():
        last = entry.get("last_session") if isinstance(entry, dict) else None
        if isinstance(last, datetime):
            last = last.date()
        elif isinstance(last, str):
            last = date.fromisoformat(last[:10])
        if isinstance(last, date):
            out[ticker] = last
    return out


def _ticker(series_id: str) -> str | None:
    parts = series_id.split(":")
    return parts[1] if len(parts) >= 3 and parts[0] == "YF" else None


def tiingo_cutoff(series_id: str) -> date | None:
    """Latest obs_date a TIINGO row of ``series_id`` can be trusted for, or
    None when there is no cutoff (``date.min`` when no row can be trusted)."""
    ticker = _ticker(series_id)
    if ticker is None:
        return None
    if not is_tiingo_equity(ticker):
        return date.min
    return _last_sessions().get(ticker)


def is_foreign_tiingo_row(series_id: str, source_name: str | None, obs_date: date | datetime) -> bool:
    """True when a raw row is a TIINGO price that is not ``series_id``'s
    instrument: a symbol Tiingo cannot serve, or a date after the ticker's
    last real session."""
    if (source_name or "").upper() != TIINGO_SOURCE:
        return False
    cutoff = tiingo_cutoff(series_id)
    if cutoff is None:
        return False
    d = obs_date.date() if isinstance(obs_date, datetime) else obs_date
    return d > cutoff
