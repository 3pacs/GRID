"""godview.finra_short_volume_pillar — FINRA daily short-volume God View pillar.

Consumes (read-only) ``raw_series`` rows under the ``finra:short_volume:<symbol>:<market>``
namespace written by ``ingestion/altdata/finra_short_volume.py`` (that module
lives on ``origin/fable/sources-finra-ftd-20260918``, commit ``40a9f1ae``,
not yet merged into this branch -- read via ``git show``, per this lane's
read-only-cross-branch-reference pattern; never checked out).

**This is emphatically NOT short interest, and this pillar never computes a
squeeze score.** Per FINRA's own catalog page
(https://www.finra.org/finra-data/browse-catalog/short-sale-volume,
quoted verbatim in that branch's contracts doc,
``docs/handoffs/2026-09-18/fable-w5b-source-contracts.md``):

    "Short Sale Files do not -- and are not intended to -- equate to
    bi-monthly reported short interest position information. The short
    interest data reflects short positions held by market participants at
    a specific moment in time on two discrete days each month, while the
    Daily File reflects the aggregate volume of short trades effected on
    each trade date..."

So every value here is aggregate SHARE VOLUME of short-sale trades
*executed* on a trade date -- not a position, not a "days to cover," and
not evidence of anything resembling a squeeze. ``short_ratio`` (this
pillar's one derived field) is simply ``short_volume / total_volume`` for
that trade date -- a description of that day's trading mix, nothing more.

Release schedule, quoted 2026-09-18 via WebFetch against
https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files:

    "FINRA posts the Daily Short Sale Volume Files to this no later than
    6:00:00pm ET of the same day on the relevant trade date."

So ``release_date = trade_date`` (same day) always -- unlike the CFTC/Fed
pillars, there is no weekly single-weekday gate here; FINRA publishes for
every trade date. ``availability_basis`` still applies the shared
tolerance (``godview/availability_basis.py``): a pull recorded within 1
day of the trade date is ``observed_acquisition``; a much later pull (a
historical backfill, which is the only way this pillar will see any data
at all until the puller is scheduled -- see below) is ``inferred_schedule``.

**Missing input, named exactly** (operator direction, 2026-09-18):
``FINRAShortVolumePuller`` is deliberately NOT registered in
``ingestion/scheduler.py`` (unauthorised live pulls -- see that module's
own docstring: "CONTRACT-FIRST / NOT ACTIVATED"). This pillar's
materializer therefore has nothing to read from in a real deployment
until that authorization decision is made and the puller is scheduled;
until then, ``read_finra_short_volume_pillar`` returns
``unavailable(never_configured)`` -- correctly, not as a bug. It
materializes from whatever rows happen to exist in ``raw_series`` (e.g.
from a manual one-off pull or a test fixture), same as every other
pillar in this package.

``symbols`` here means "distinct symbols this run found under the
``finra:short_volume:*`` prefix" -- there is no curated watchlist to draw
from, because no scheduled pull has ever populated one. Coverage is
therefore "of the symbols raw_series currently offers, how many produced
a valid ratio row" -- not "of some target universe."

Everything above the "DB wrappers" marker is pure Python.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.availability_basis import classify_availability_basis
from godview.generations import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    latest_attempt as _latest_attempt,
    latest_complete_generation as _latest_complete_generation,
    new_generation_id,
    record_generation,
)
from store.availability import measured_or_none

PILLAR_NAME = "finra_short_volume"
SERIES_PREFIX = "finra:short_volume"
UNIT_RATIO = "ratio_0_1"
UNIT_SHARES = "shares"

#: Same-day publication -> tight tolerance (allow 1 day for a puller that
#: runs just after midnight ET).
RELEASE_LAG_DAYS = 0
AVAILABILITY_TOLERANCE_DAYS = 1
RELEASE_RULE_ID = "finra_short_volume_same_day_v1"
_RELEASE_RULE_URL = (
    "https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/"
    "daily-short-sale-volume-files"
)
NOT_SHORT_INTEREST_NOTE = (
    "this is daily short-sale VOLUME executed on the trade date, NOT short INTEREST "
    "(a bi-monthly position snapshot) and never a squeeze score"
)

#: Documented heuristic only (not a calibrated threshold) -- flagged the same
#: way godview/commodity_warehouse_pillar.py flags its tightness threshold.
SPIKE_RATIO_THRESHOLD = 0.60
MOVING_AVERAGE_WINDOW_DAYS = 20


def compute_release_date(trade_date: date) -> tuple[date, str]:
    """FINRA publishes every trade date same-day; release_date == trade_date always."""
    source_ref = (
        f"{RELEASE_RULE_ID}: release_date = trade_date (same day, by 18:00 ET, "
        f"{_RELEASE_RULE_URL})"
    )
    return trade_date, source_ref


def compute_short_ratio(short_volume: float, total_volume: float) -> float | None:
    """short_volume / total_volume. None (never 0) when total_volume <= 0."""
    if total_volume <= 0:
        return None
    return short_volume / total_volume


def compute_moving_average(history: list[float], window: int = MOVING_AVERAGE_WINDOW_DAYS) -> float | None:
    """Trailing mean of ``history`` (ascending, most recent last), None if empty."""
    finite = [v for v in history if v is not None]
    if not finite:
        return None
    windowed = finite[-window:] if len(finite) >= window else finite
    return sum(windowed) / len(windowed)


def classify_spike(short_ratio: float | None, moving_average: float | None) -> bool:
    """Documented heuristic: today's ratio at/above SPIKE_RATIO_THRESHOLD. Never None."""
    if short_ratio is None:
        return False
    return short_ratio >= SPIKE_RATIO_THRESHOLD


@dataclass(frozen=True)
class MaterializationResult:
    status: str  # "SUCCESS" | "SUCCESS_NOOP" | "EMPTY" | "FAILED"
    generation_id: str
    rows_written: int = 0
    symbols_with_data: int = 0
    symbols_discovered: int = 0
    message: str = ""


# ---------------------------------------------------------------------------
# DB wrappers
# ---------------------------------------------------------------------------


def _discover_symbols(conn: Connection, as_of: date) -> list[str]:
    """Distinct symbols under finra:short_volume:* with any row up to as_of.

    series_id shape is finra:short_volume:<symbol>:<market> -- split on ':'
    to recover the symbol (position 2, 0-indexed).
    """
    rows = conn.execute(
        text(
            "SELECT DISTINCT series_id FROM raw_series "
            "WHERE series_id LIKE :prefix AND obs_date <= :as_of AND pull_status = 'SUCCESS'"
        ),
        {"prefix": f"{SERIES_PREFIX}:%", "as_of": as_of},
    ).fetchall()
    symbols: set[str] = set()
    for (sid,) in rows:
        parts = sid.split(":")
        if len(parts) >= 3:
            symbols.add(parts[2])
    return sorted(symbols)


def _read_symbol_history(conn: Connection, symbol: str, as_of: date) -> dict[date, dict[str, Any]]:
    """PIT-style (LATEST_AS_OF) read of ALL market-suffixed rows for one symbol.

    A given trade date can have more than one finra:short_volume:<symbol>:<market>
    series (different reporting facilities); this sums short_volume and
    total_volume across whatever market rows exist for that (symbol, date)
    -- the tracked finra_short_volume_daily table has no market dimension
    (UNIQUE(trade_date, ticker) only), so a ticker-level row must already be
    the combined figure. The most recently pulled row per (series_id, obs_date)
    wins per series; markets are then summed.
    """
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (series_id, obs_date)
                series_id, obs_date, value, raw_payload, pull_timestamp
            FROM raw_series
            WHERE series_id LIKE :prefix
              AND obs_date <= :as_of
              AND pull_status = 'SUCCESS'
            ORDER BY series_id, obs_date, pull_timestamp DESC
            """
        ),
        {"prefix": f"{SERIES_PREFIX}:{symbol}:%", "as_of": as_of},
    ).mappings().all()

    out: dict[date, dict[str, Any]] = {}
    for row in rows:
        obs_date = row["obs_date"]
        payload = row["raw_payload"] or {}
        total_volume = measured_or_none(payload.get("total_volume"))
        short_exempt = measured_or_none(payload.get("short_exempt_volume"))
        short_volume = measured_or_none(row["value"])
        market = payload.get("market")

        bucket = out.setdefault(
            obs_date,
            {"short_volume": 0.0, "total_volume": 0.0, "short_exempt_volume": 0.0,
             "markets": [], "pull_timestamps": []},
        )
        if short_volume is not None:
            bucket["short_volume"] += short_volume
        if total_volume is not None:
            bucket["total_volume"] += total_volume
        if short_exempt is not None:
            bucket["short_exempt_volume"] += short_exempt
        if market:
            bucket["markets"].append(market)
        bucket["pull_timestamps"].append(row["pull_timestamp"])
    return out


def _existing_trade_dates(conn: Connection, ticker: str) -> set[date]:
    rows = conn.execute(
        text("SELECT trade_date FROM finra_short_volume_daily WHERE ticker = :t"),
        {"t": ticker},
    ).fetchall()
    return {r[0] for r in rows}


def _prior_short_ratios(conn: Connection, ticker: str, before: date) -> list[float]:
    rows = conn.execute(
        text(
            "SELECT short_ratio FROM finra_short_volume_daily "
            "WHERE ticker = :t AND trade_date < :before AND short_ratio IS NOT NULL "
            "ORDER BY trade_date ASC"
        ),
        {"t": ticker, "before": before},
    ).fetchall()
    return [float(r[0]) for r in rows]


def _distinct_pull_count(conn: Connection, symbol: str, obs_date: date) -> int:
    n = conn.execute(
        text(
            "SELECT COUNT(DISTINCT pull_timestamp) FROM raw_series "
            "WHERE series_id LIKE :prefix AND obs_date = :od AND pull_status = 'SUCCESS'"
        ),
        {"prefix": f"{SERIES_PREFIX}:{symbol}:%", "od": obs_date},
    ).scalar()
    return int(n or 0)


def materialize_finra_short_volume_pillar(engine: Engine, *, as_of: date | None = None) -> MaterializationResult:
    """Materialize new FINRA short-volume rows as one atomic generation.

    Same transactional/idempotent/no-fallback shape as the other pillars.
    """
    as_of = as_of or date.today()
    generation_id = new_generation_id()

    try:
        with engine.begin() as conn:
            symbols = _discover_symbols(conn, as_of)
            if not symbols:
                raise _EmptyUpstream()

            rows_to_insert: list[dict[str, Any]] = []
            symbols_with_data: set[str] = set()

            for symbol in symbols:
                history = _read_symbol_history(conn, symbol, as_of)
                if not history:
                    continue
                existing = _existing_trade_dates(conn, symbol)

                for trade_date, bucket in sorted(history.items()):
                    if trade_date in existing:
                        continue
                    short_volume = bucket["short_volume"]
                    total_volume = bucket["total_volume"]
                    if total_volume <= 0:
                        continue  # no fallback -- an unratioable day stays unmaterialized

                    short_ratio = compute_short_ratio(short_volume, total_volume)
                    prior_ratios = _prior_short_ratios(conn, symbol, trade_date)
                    ma = compute_moving_average(prior_ratios + [short_ratio])
                    spike = classify_spike(short_ratio, ma)

                    release_date, source_ref = compute_release_date(trade_date)
                    available_at = min(bucket["pull_timestamps"]) if bucket["pull_timestamps"] else None
                    distinct_pulls = _distinct_pull_count(conn, symbol, trade_date)
                    basis, basis_note = classify_availability_basis(
                        release_date, available_at,
                        distinct_pull_count=distinct_pulls,
                        tolerance_days=AVAILABILITY_TOLERANCE_DAYS,
                    )
                    full_source_ref = f"{source_ref}; {NOT_SHORT_INTEREST_NOTE}"
                    if basis_note:
                        full_source_ref = f"{full_source_ref}; {basis_note}"

                    rows_to_insert.append(
                        {
                            "trade_date": trade_date,
                            "ticker": symbol,
                            "market": ",".join(sorted(set(bucket["markets"]))) or None,
                            "short_volume": short_volume,
                            "short_exempt_volume": bucket["short_exempt_volume"],
                            "total_volume": total_volume,
                            "short_ratio": short_ratio,
                            "short_ratio_20d_ma": ma,
                            "is_spike": spike,
                            "release_date": release_date,
                            "available_at": available_at,
                            "provenance": "measured",
                            "availability_basis": basis,
                            "generation_id": generation_id,
                            "coverage_fraction": None,
                            "source_ref": full_source_ref,
                        }
                    )
                    symbols_with_data.add(symbol)

            for row in rows_to_insert:
                conn.execute(
                    text(
                        """
                        INSERT INTO finra_short_volume_daily (
                            trade_date, ticker, market, short_volume, short_exempt_volume,
                            total_volume, short_ratio, short_ratio_20d_ma, is_spike,
                            release_date, available_at, provenance, availability_basis,
                            generation_id, coverage_fraction, source_ref
                        ) VALUES (
                            :trade_date, :ticker, :market, :short_volume, :short_exempt_volume,
                            :total_volume, :short_ratio, :short_ratio_20d_ma, :is_spike,
                            :release_date, :available_at, :provenance, :availability_basis,
                            :generation_id, :coverage_fraction, :source_ref
                        )
                        ON CONFLICT (trade_date, ticker) DO NOTHING
                        """
                    ),
                    row,
                )

            coverage_fraction = len(symbols_with_data) / len(symbols) if symbols else None
            record_generation(
                conn, pillar=PILLAR_NAME, generation_id=generation_id,
                status=STATUS_COMPLETE, row_count=len(rows_to_insert),
                coverage_fraction=coverage_fraction,
            )

        status = "SUCCESS" if rows_to_insert else "SUCCESS_NOOP"
        return MaterializationResult(
            status=status, generation_id=generation_id, rows_written=len(rows_to_insert),
            symbols_with_data=len(symbols_with_data), symbols_discovered=len(symbols),
            message=f"{len(rows_to_insert)} new row(s) across {len(symbols_with_data)} symbol(s)",
        )
    except _EmptyUpstream:
        _record_failure(engine, generation_id, "empty_upstream")
        return MaterializationResult(status="EMPTY", generation_id=generation_id, message="no finra:short_volume:* rows in raw_series")
    except Exception as exc:  # noqa: BLE001
        _record_failure(engine, generation_id, str(exc))
        return MaterializationResult(status="FAILED", generation_id=generation_id, message=str(exc))


class _EmptyUpstream(Exception):
    pass


def _record_failure(engine: Engine, generation_id: str, reason: str) -> None:
    try:
        with engine.begin() as conn:
            record_generation(conn, pillar=PILLAR_NAME, generation_id=generation_id, status=STATUS_FAILED, failure_reason=reason)
    except Exception:  # noqa: BLE001
        pass


@dataclass(frozen=True)
class PillarReadResult:
    state: str  # "never_configured" | "materializer_failed" | "ok"
    rows: list[dict[str, Any]] = field(default_factory=list)
    symbols_with_data: int = 0
    generation_id: str | None = None
    generation_published_at: Any = None


def read_finra_short_volume_pillar(
    conn: Connection, as_of: date, *, include_inferred: bool = False
) -> PillarReadResult:
    """Latest qualifying row per ticker as of ``as_of``. See module docstring."""
    generation = _latest_complete_generation(conn, PILLAR_NAME)
    attempt = _latest_attempt(conn, PILLAR_NAME)

    if generation is None:
        if attempt is not None and attempt["status"] == STATUS_FAILED:
            return PillarReadResult(state="materializer_failed")
        return PillarReadResult(state="never_configured")

    basis_filter = "" if include_inferred else "AND availability_basis = 'observed_acquisition'"
    rows = conn.execute(
        text(
            f"""
            SELECT DISTINCT ON (ticker)
                ticker, market, trade_date, short_volume, short_exempt_volume, total_volume,
                short_ratio, short_ratio_20d_ma, is_spike, release_date, available_at,
                provenance, availability_basis, generation_id, coverage_fraction, source_ref
            FROM finra_short_volume_daily
            WHERE release_date IS NOT NULL AND release_date <= :as_of
              {basis_filter}
            ORDER BY ticker, trade_date DESC
            """
        ),
        {"as_of": as_of},
    ).mappings().all()

    rows_out = [dict(r) for r in rows]
    return PillarReadResult(
        state="ok", rows=rows_out, symbols_with_data=len(rows_out),
        generation_id=generation["generation_id"], generation_published_at=generation["published_at"],
    )
