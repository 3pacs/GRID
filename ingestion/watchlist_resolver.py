"""
Shared ticker-universe resolver for ingestion pullers (Tasks #185 + #182).

Used by:
- ingestion.earnings_events_puller   (SEC EDGAR + AV — 21 → ~580 tickers)
- ingestion.altdata.social_attention (Wikipedia attention — 16 → ~580 tickers)

Three sources, all returning a deduped uppercase list:

    "watchlist"        — the legacy ``watchlist`` table  (~21 entries)
    "signal_registry"  — tickers active in ``signal_registry`` recently,
                         unioned with the watchlist + gem_alerts backtest set
                         so the gem-hunter universe always opts in
    "cli"              — explicit comma-separated list (programmatic only)

Each puller's CLI now accepts ``--ticker-source signal_registry|watchlist|cli``
and ``--tickers AAPL,NVDA,...`` (still supported as an override).

Intentionally lightweight: no extra DB columns, no schema migrations. Works
against the existing griddb tables; falls back to ``watchlist`` if the
broader sources are missing.
"""
from __future__ import annotations

from typing import Iterable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# How aggressively we expand the universe via signal_registry. A ticker must
# have at least N rows in the last `lookback_days` window AND only ASCII A-Z
# characters to be considered (filters out crypto pairs, futures expiries,
# share-class suffixes like 'BRK.B' which the SEC submissions endpoint can
# choke on).
DEFAULT_LOOKBACK_DAYS = 120
DEFAULT_MIN_SIGNALS = 3
DEFAULT_MAX_TICKERS = 1000  # safety cap so an exploding signal_registry can't
                            # blow past SEC rate limits unbounded.


def _normalize(tickers: Iterable[str]) -> list[str]:
    """Uppercase, strip, drop blanks, dedupe; preserve sort order."""
    seen: set[str] = set()
    out: list[str] = []
    for t in tickers:
        if not t:
            continue
        u = t.strip().upper()
        if not u or u in seen:
            continue
        # Skip anything with non-A-Z chars (filters crypto BTC-USD, BRK.B,
        # futures expiries like ESH25, etc.). The earnings + wiki pullers
        # both target equity-style tickers only.
        if not u.isalpha():
            continue
        if len(u) > 6:
            continue
        seen.add(u)
        out.append(u)
    return sorted(out)


def _from_watchlist(engine: Engine) -> list[str]:
    """Pull the legacy ``watchlist`` (stocks + ETFs only)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT ticker FROM watchlist "
                "WHERE asset_type IN ('stock','etf') "
                "ORDER BY ticker"
            )
        ).fetchall()
    return [r[0] for r in rows if r[0]]


def _from_signal_registry(
    engine: Engine,
    *,
    lookback_days: int,
    min_signals: int,
) -> list[str]:
    """Pull tickers from signal_registry with recent activity.

    A ticker counts as 'active' when it has at least ``min_signals`` rows
    in the last ``lookback_days`` window. We also union with the watchlist
    (so the operator's hand-curated list is never dropped) and with the
    gem_alerts backtest_ticker set (so anything the gem-hunter has flagged
    keeps getting earnings/attention pulls even if its signal_registry
    activity dips).
    """
    sql = """
        WITH sig AS (
            SELECT ticker
            FROM signal_registry
            WHERE ticker IS NOT NULL
              AND created_at > now() - make_interval(days => :lookback)
            GROUP BY ticker
            HAVING COUNT(*) >= :min_n
        ),
        wl AS (
            SELECT ticker FROM watchlist
            WHERE asset_type IN ('stock','etf')
        ),
        gem AS (
            -- gem_alerts is the gem-hunter backtest universe; we always
            -- include it so the catalyst-timeline / earnings_events table
            -- stays populated for actively-tracked names.
            SELECT backtest_ticker AS ticker
            FROM gem_alerts
            WHERE backtest_ticker IS NOT NULL
        )
        SELECT DISTINCT ticker FROM (
            SELECT ticker FROM sig
            UNION SELECT ticker FROM wl
            UNION SELECT ticker FROM gem
        ) u
        ORDER BY ticker
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(sql),
                {"lookback": lookback_days, "min_n": min_signals},
            ).fetchall()
        return [r[0] for r in rows if r[0]]
    except Exception as exc:
        # signal_registry / gem_alerts may not exist on every deploy
        # (e.g. local dev DB). Fall back to watchlist so the puller still
        # runs against something.
        log.warning(
            "signal_registry resolve failed ({e}) — falling back to watchlist",
            e=str(exc)[:200],
        )
        return _from_watchlist(engine)


def resolve_universe(
    engine: Engine,
    *,
    source: str = "signal_registry",
    cli_tickers: list[str] | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_signals: int = DEFAULT_MIN_SIGNALS,
    max_tickers: int = DEFAULT_MAX_TICKERS,
) -> list[str]:
    """Resolve the ticker universe for a puller run.

    Args:
        engine: SQLAlchemy engine pointed at griddb.
        source: One of ``"signal_registry"``, ``"watchlist"``, or ``"cli"``.
            ``"signal_registry"`` is the new default for the expanded
            universe (Tasks #185 + #182). ``"watchlist"`` preserves the
            old 21-ticker behaviour. ``"cli"`` ignores the DB entirely
            and uses ``cli_tickers``.
        cli_tickers: When ``source == "cli"`` (or as an override), the
            explicit list to use. Normalized + deduped.
        lookback_days: Recent-activity window for signal_registry.
        min_signals: Minimum rows in window for a ticker to qualify.
        max_tickers: Hard upper bound so runaway expansion can't DOS
            SEC EDGAR / Wikipedia.

    Returns:
        Deduped, uppercase, A-Z-only ticker list (length <= max_tickers).
    """
    if cli_tickers:
        resolved = _normalize(cli_tickers)
        log.info(
            "watchlist_resolver: cli override — {n} tickers",
            n=len(resolved),
        )
        return resolved[:max_tickers]

    src = (source or "signal_registry").lower()
    if src == "cli":
        # Caller asked for CLI but didn't supply tickers — fall back
        # to watchlist rather than running on zero.
        log.warning(
            "watchlist_resolver: source='cli' with no tickers — "
            "falling back to watchlist"
        )
        raw = _from_watchlist(engine)
    elif src == "watchlist":
        raw = _from_watchlist(engine)
    elif src in ("signal_registry", "signals", "registry", "auto"):
        raw = _from_signal_registry(
            engine,
            lookback_days=lookback_days,
            min_signals=min_signals,
        )
    else:
        log.warning(
            "watchlist_resolver: unknown source '{s}' — "
            "defaulting to signal_registry",
            s=source,
        )
        raw = _from_signal_registry(
            engine,
            lookback_days=lookback_days,
            min_signals=min_signals,
        )

    resolved = _normalize(raw)
    if len(resolved) > max_tickers:
        log.warning(
            "watchlist_resolver: {n} tickers > max_tickers={m}, truncating",
            n=len(resolved), m=max_tickers,
        )
        resolved = resolved[:max_tickers]

    log.info(
        "watchlist_resolver: source={s} -> {n} tickers "
        "(lookback={d}d, min_signals={k})",
        s=src, n=len(resolved), d=lookback_days, k=min_signals,
    )
    return resolved


__all__ = ["resolve_universe", "DEFAULT_LOOKBACK_DAYS",
           "DEFAULT_MIN_SIGNALS", "DEFAULT_MAX_TICKERS"]
