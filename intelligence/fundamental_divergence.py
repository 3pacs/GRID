"""Fundamental-vs-price divergence detector.

For every ticker with enough capital_flows history + raw_series price
history, build a sector-relative fundamental score and a sector-relative
price score, then flag the ones where the two disagree.

Pure-function, deterministic computation. No LLM, no external APIs —
every input is a parameterized SQL query against ``griddb``.

Scoring
-------
Fundamental score (0..100), weighted average of three sector-relative
signals:

    0.40  revenue_3y_cagr percentile      — growth
    0.30  gross_margin_trend              — expansion vs contraction
          (+100 expanding, 50 stable, 0 contracting)
    0.30  shareholder_yield percentile    — capital return discipline

Price score (0..100): 3y stock CAGR percentile within sector. Uses the
``YF:{TICKER}:close`` series id convention in ``raw_series``.

Divergence
----------
    divergence = fundamental_score − price_score

        > +30   → long_candidate   (fundamentals strong, price lagging)
        < −30   → short_candidate  (fundamentals weak, price ripping)
        else    → aligned

Each row is upserted into ``fundamental_divergence`` keyed on
``(ticker, as_of)``. The runner script reschedules daily from
``scripts/hermes_operator.py``.

Thresholds
----------
All numeric thresholds live in module-level constants so tests can
assert against them and future tuning stays explicit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Tunable constants ──────────────────────────────────────────────

MIN_PERIODS: int = 4               # capital_flows annual rows required
MIN_PRICE_OBS: int = 500           # raw_series YF close rows required
PRICE_LOOKBACK_DAYS: int = 365 * 3 # 3y window for price CAGR
LONG_THRESHOLD: float = 30.0
SHORT_THRESHOLD: float = -30.0

# Fundamental score weights (must sum to 1.0)
W_REVENUE_CAGR: float = 0.40
W_MARGIN_TREND: float = 0.30
W_SHAREHOLDER_YIELD: float = 0.30

# Gross-margin trend points (on 0..100 scale, mapped from trichotomy)
MARGIN_POINTS = {"expanding": 100.0, "flat": 50.0, "stable": 50.0, "contracting": 0.0}

# Minimum sector population required for a meaningful percentile rank.
# Below this we fall back to a neutral 50 for the percentile dimensions.
MIN_SECTOR_POPULATION: int = 3


# ── Helpers ────────────────────────────────────────────────────────

def _safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den is None:
        return None
    try:
        d = float(den)
        return None if d == 0.0 else float(num) / d
    except (TypeError, ValueError):
        return None


def _table_exists(conn: Any, name: str) -> bool:
    try:
        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(n=name)
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _percentile_rank(value: float, population: list[float]) -> float:
    """Return value's percentile within population on a 0..100 scale.

    Uses the standard ``rank / (n - 1)`` definition — minimum maps to
    0, maximum to 100, single-element populations return 50 (neutral).
    """
    if value is None:
        return 50.0
    pop = [float(x) for x in population if x is not None]
    if len(pop) < MIN_SECTOR_POPULATION:
        return 50.0
    pop_sorted = sorted(pop)
    # Count strictly-below + half of ties (tie-broken-midrank) for stability.
    below = sum(1 for v in pop_sorted if v < value)
    ties = sum(1 for v in pop_sorted if v == value)
    rank = below + 0.5 * ties
    n = len(pop_sorted)
    if n <= 1:
        return 50.0
    return round(100.0 * rank / n, 2)


# ── Ticker universe from SECTOR_MAP ────────────────────────────────

@dataclass(frozen=True)
class SectorTicker:
    ticker: str
    sector: str


def _load_universe() -> list[SectorTicker]:
    """Flatten ``SECTOR_MAP`` into ``[(ticker, sector)]`` pairs."""
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception as exc:
        log.warning("fundamental_divergence: sector_map import failed: {e}", e=str(exc))
        return []

    out: list[SectorTicker] = []
    seen: set[str] = set()
    for sector_name, sector in SECTOR_MAP.items():
        if not isinstance(sector, dict):
            continue
        for sub in (sector.get("subsectors") or {}).values():
            if not isinstance(sub, dict):
                continue
            for actor in sub.get("actors") or []:
                tk = (actor.get("ticker") or "").strip().upper()
                if not tk or tk in seen:
                    continue
                seen.add(tk)
                out.append(SectorTicker(ticker=tk, sector=sector_name))
    return out


# ── Fundamental metric extraction ──────────────────────────────────

def _load_ticker_fundamentals(conn: Any, ticker: str) -> dict[str, Any] | None:
    """Return ``{revenue_cagr, margin_trend, shareholder_yield, periods}``
    or ``None`` if the ticker has fewer than ``MIN_PERIODS`` annual rows.

    Queries ``capital_flows`` applying the same SEC-over-seed dedup the
    API read path uses (``api/routers/capital_flow.py::_DEDUP_SQL``).

    The base table carries multiple ``source_filing`` variants for the
    same logical (actor, fiscal_period, flow_type, counterparty) — a SEC
    10-K row AND a seed row, where seed used *total* revenue and SEC uses
    *net sales*. Naively ``SUM``-ing across them double-counts and is
    exactly what made WMT show a -16.6% and JPM a -10.8% 3y revenue CAGR.
    So we first pick ONE row per natural key (SEC 10-* > 20-* > 8-* >
    other > seed, then confidence, then most-recent ``as_of``), THEN sum
    over counterparties within each (fiscal_period, flow_type).
    """
    if not _table_exists(conn, "public.capital_flows"):
        return None

    variants = list({ticker, ticker.upper(), ticker.lower()})
    rows = conn.execute(
        text(
            """
            WITH ranked AS (
                SELECT
                    fiscal_period,
                    flow_type,
                    amount_usd,
                    ROW_NUMBER() OVER (
                        PARTITION BY actor_id, fiscal_period, period_type,
                                     flow_type, direction,
                                     COALESCE(NULLIF(counterparty_id, ''), '__none__')
                        ORDER BY
                            -- Prefer SEC filings, then other regulatory,
                            -- then seed/other. Mirrors capital_flow._DEDUP_SQL.
                            CASE
                                WHEN source_filing LIKE '10-%' THEN 1
                                WHEN source_filing LIKE '20-%' THEN 2
                                WHEN source_filing LIKE '8-%'  THEN 3
                                WHEN source_filing LIKE 'seed%' THEN 5
                                ELSE 4
                            END,
                            CASE confidence
                                WHEN 'confirmed' THEN 1
                                WHEN 'derived'   THEN 2
                                WHEN 'estimated' THEN 3
                                WHEN 'rumored'   THEN 4
                                WHEN 'inferred'  THEN 5
                                ELSE 6
                            END,
                            as_of DESC NULLS LAST
                    ) AS rk
                FROM capital_flows
                WHERE actor_id = ANY(:ids)
                  AND period_type = 'annual'
                  AND flow_type IN (
                    'revenue', 'cogs', 'dividends', 'buybacks'
                  )
            )
            SELECT fiscal_period, flow_type, SUM(amount_usd) AS amt
            FROM ranked
            WHERE rk = 1
            GROUP BY fiscal_period, flow_type
            ORDER BY fiscal_period DESC
            """
        ).bindparams(ids=variants)
    ).fetchall()
    if not rows:
        return None

    by_period: dict[Any, dict[str, float]] = {}
    for fp, ft, amt in rows:
        by_period.setdefault(fp, {})[ft] = float(amt or 0.0)

    periods = sorted(by_period.keys(), reverse=True)
    if len(periods) < MIN_PERIODS:
        return None

    # Revenue 3y CAGR: latest vs period 3 years back.
    rev_latest = by_period[periods[0]].get("revenue")
    rev_3y = by_period[periods[3]].get("revenue")
    cagr: float | None = None
    if rev_latest is not None and rev_3y is not None and rev_3y > 0:
        try:
            cagr = (rev_latest / rev_3y) ** (1.0 / 3.0) - 1.0
        except (ValueError, ZeroDivisionError):
            cagr = None

    # Gross margin trichotomy over the 4-period window.
    margins: list[float] = []
    for p in periods[:4]:
        rev = by_period[p].get("revenue")
        cogs = by_period[p].get("cogs")
        gm = _safe_div((rev or 0.0) - (cogs or 0.0), rev)
        if gm is not None:
            margins.append(gm)
    margin_trend: str
    if len(margins) >= 2:
        delta = margins[0] - margins[-1]
        if delta > 0.005:
            margin_trend = "expanding"
        elif delta < -0.005:
            margin_trend = "contracting"
        else:
            margin_trend = "flat"
    else:
        margin_trend = "flat"

    # Shareholder yield: (dividends + buybacks) / revenue averaged over window.
    sy_window: list[float] = []
    for p in periods[:4]:
        rev = by_period[p].get("revenue")
        if rev is None or rev <= 0:
            continue
        div = by_period[p].get("dividends", 0.0) or 0.0
        buy = by_period[p].get("buybacks", 0.0) or 0.0
        sy = _safe_div(div + buy, rev)
        if sy is not None:
            sy_window.append(sy)
    shareholder_yield = (
        sum(sy_window) / len(sy_window) if sy_window else None
    )

    return {
        "revenue_cagr": cagr,
        "margin_trend": margin_trend,
        "shareholder_yield": shareholder_yield,
        "periods": len(periods),
    }


# ── Price metric extraction ────────────────────────────────────────

def _load_ticker_price_cagr(
    conn: Any, ticker: str, as_of: date
) -> float | None:
    """Return 3y price CAGR for ``ticker`` or ``None`` if insufficient
    history. Requires at least ``MIN_PRICE_OBS`` rows to qualify.

    The series id convention is ``YF:{TICKER}:close`` (upper case),
    mirroring ``intelligence.contagion_backtest``.
    """
    if not _table_exists(conn, "public.raw_series"):
        return None

    series_id = f"YF:{ticker.upper()}:close"
    count_row = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM raw_series
            WHERE series_id = :sid AND value IS NOT NULL
            """
        ).bindparams(sid=series_id)
    ).fetchone()
    if count_row is None or int(count_row[0] or 0) < MIN_PRICE_OBS:
        return None

    # Latest close at/before as_of
    latest_row = conn.execute(
        text(
            """
            SELECT value, obs_date
            FROM raw_series
            WHERE series_id = :sid
              AND obs_date <= :d
              AND value IS NOT NULL
            ORDER BY obs_date DESC
            LIMIT 1
            """
        ).bindparams(sid=series_id, d=as_of)
    ).fetchone()
    if latest_row is None or latest_row[0] is None:
        return None

    target_prior = as_of - timedelta(days=PRICE_LOOKBACK_DAYS)
    prior_row = conn.execute(
        text(
            """
            SELECT value, obs_date
            FROM raw_series
            WHERE series_id = :sid
              AND obs_date <= :d
              AND value IS NOT NULL
            ORDER BY obs_date DESC
            LIMIT 1
            """
        ).bindparams(sid=series_id, d=target_prior)
    ).fetchone()
    if prior_row is None or prior_row[0] is None:
        return None

    try:
        latest = float(latest_row[0])
        prior = float(prior_row[0])
    except (TypeError, ValueError):
        return None
    if prior <= 0:
        return None
    try:
        return (latest / prior) ** (1.0 / 3.0) - 1.0
    except (ValueError, ZeroDivisionError):
        return None


# ── Core scoring ──────────────────────────────────────────────────

def _build_fundamental_score(
    ticker_fund: dict[str, Any],
    sector_cagrs: list[float],
    sector_yields: list[float],
) -> float:
    """Compose the 0..100 fundamental score from per-ticker metrics
    and the sector-wide distributions used for percentile ranks.
    """
    cagr = ticker_fund.get("revenue_cagr")
    cagr_pct = _percentile_rank(cagr, sector_cagrs) if cagr is not None else 50.0

    trend = ticker_fund.get("margin_trend") or "flat"
    margin_pts = MARGIN_POINTS.get(trend, 50.0)

    sy = ticker_fund.get("shareholder_yield")
    sy_pct = _percentile_rank(sy, sector_yields) if sy is not None else 50.0

    score = (
        W_REVENUE_CAGR * cagr_pct
        + W_MARGIN_TREND * margin_pts
        + W_SHAREHOLDER_YIELD * sy_pct
    )
    return round(max(0.0, min(100.0, score)), 2)


def _build_price_score(
    ticker_cagr: float | None, sector_cagrs: list[float]
) -> float:
    """Return the 3y price CAGR percentile (0..100) within the sector.

    Missing price history maps to neutral 50.
    """
    if ticker_cagr is None:
        return 50.0
    return _percentile_rank(ticker_cagr, sector_cagrs)


def _classify(divergence: float) -> str:
    if divergence > LONG_THRESHOLD:
        return "long_candidate"
    if divergence < SHORT_THRESHOLD:
        return "short_candidate"
    return "aligned"


def _narrative(
    ticker: str,
    sector: str,
    fundamental_score: float,
    price_score: float,
    divergence: float,
    classification: str,
    fund: dict[str, Any],
) -> str:
    """Deterministic template — no LLM."""
    cagr = fund.get("revenue_cagr")
    trend = fund.get("margin_trend") or "flat"
    sy = fund.get("shareholder_yield")
    cagr_str = f"{cagr * 100:.1f}% 3y CAGR" if cagr is not None else "n/a CAGR"
    sy_str = f"{sy * 100:.1f}% yield" if sy is not None else "n/a yield"

    verdict = {
        "long_candidate": (
            "LONG candidate — fundamentals outrun price. Possibly mispriced cheap."
        ),
        "short_candidate": (
            "SHORT candidate — price outruns fundamentals. Possibly mispriced expensive."
        ),
        "aligned": "Aligned — price tracks fundamentals.",
    }[classification]
    return (
        f"{ticker} ({sector}) fundamental={fundamental_score:.0f} "
        f"vs price={price_score:.0f} → div={divergence:+.0f}. "
        f"{cagr_str}, margins {trend}, {sy_str}. {verdict}"
    )


# ── Public API ────────────────────────────────────────────────────

def compute_divergence(engine: Engine, as_of: date | None = None) -> list[dict[str, Any]]:
    """Compute one divergence row per eligible ticker.

    Returns the in-memory list of result dicts WITHOUT writing to the
    database. Use ``snapshot_all`` to also upsert into
    ``fundamental_divergence``.
    """
    as_of = as_of or date.today()
    universe = _load_universe()
    if not universe:
        log.warning("fundamental_divergence: empty universe")
        return []

    # Group by sector for population-based percentile ranks.
    by_sector: dict[str, list[SectorTicker]] = {}
    for s in universe:
        by_sector.setdefault(s.sector, []).append(s)

    out: list[dict[str, Any]] = []

    with engine.connect() as conn:
        # Preload per-ticker fundamentals + price cagr. Two-pass keeps
        # the percentile ranks sector-relative without redoing SQL.
        fund_cache: dict[str, dict[str, Any] | None] = {}
        price_cache: dict[str, float | None] = {}
        for st in universe:
            try:
                fund_cache[st.ticker] = _load_ticker_fundamentals(conn, st.ticker)
            except Exception as exc:
                log.debug(
                    "fundamental_divergence: fundamentals failed for {t}: {e}",
                    t=st.ticker, e=str(exc),
                )
                fund_cache[st.ticker] = None
            try:
                price_cache[st.ticker] = _load_ticker_price_cagr(conn, st.ticker, as_of)
            except Exception as exc:
                log.debug(
                    "fundamental_divergence: price failed for {t}: {e}",
                    t=st.ticker, e=str(exc),
                )
                price_cache[st.ticker] = None

        for sector_name, members in by_sector.items():
            # Sector-relative distributions (drop None for cleaner percentiles).
            sector_cagrs: list[float] = []
            sector_yields: list[float] = []
            sector_price_cagrs: list[float] = []
            for st in members:
                fund = fund_cache.get(st.ticker)
                if fund:
                    if fund.get("revenue_cagr") is not None:
                        sector_cagrs.append(float(fund["revenue_cagr"]))
                    if fund.get("shareholder_yield") is not None:
                        sector_yields.append(float(fund["shareholder_yield"]))
                pc = price_cache.get(st.ticker)
                if pc is not None:
                    sector_price_cagrs.append(float(pc))

            for st in members:
                fund = fund_cache.get(st.ticker)
                if fund is None:
                    continue  # skip tickers without enough fundamentals
                # Require *some* price signal — if missing, fall through
                # with neutral 50 for the price score so the row still gets
                # written (most benign outcome = aligned).
                fundamental_score = _build_fundamental_score(
                    fund, sector_cagrs, sector_yields
                )
                price_score = _build_price_score(
                    price_cache.get(st.ticker), sector_price_cagrs
                )
                divergence = round(fundamental_score - price_score, 2)
                classification = _classify(divergence)
                narrative = _narrative(
                    st.ticker, sector_name,
                    fundamental_score, price_score, divergence,
                    classification, fund,
                )
                out.append({
                    "ticker": st.ticker,
                    "as_of": as_of,
                    "sector": sector_name,
                    "fundamental_score": fundamental_score,
                    "price_score": price_score,
                    "divergence": divergence,
                    "classification": classification,
                    "narrative": narrative,
                })

    log.info(
        "fundamental_divergence: scored {n} tickers across {s} sectors",
        n=len(out), s=len(by_sector),
    )
    return out


def snapshot_all(engine: Engine, as_of: date | None = None) -> dict[str, Any]:
    """Compute + upsert divergence rows into ``fundamental_divergence``.

    Returns a summary dict with per-classification counts. Safe to run
    daily from the hermes scheduler.
    """
    as_of = as_of or date.today()
    rows = compute_divergence(engine, as_of=as_of)
    counts = {"long_candidate": 0, "short_candidate": 0, "aligned": 0}
    written = 0
    if not rows:
        return {
            "as_of": as_of.isoformat(),
            "rows": 0,
            "written": 0,
            "counts": counts,
        }

    with engine.begin() as conn:
        if not _table_exists(conn, "public.fundamental_divergence"):
            log.warning(
                "fundamental_divergence: table missing, run migration 0033"
            )
            return {
                "as_of": as_of.isoformat(),
                "rows": len(rows),
                "written": 0,
                "counts": counts,
                "error": "table_missing",
            }
        for r in rows:
            counts[r["classification"]] = counts.get(r["classification"], 0) + 1
            try:
                conn.execute(
                    text(
                        """
                        INSERT INTO fundamental_divergence
                            (ticker, as_of, sector, fundamental_score,
                             price_score, divergence, classification,
                             narrative)
                        VALUES
                            (:ticker, :as_of, :sector, :fundamental_score,
                             :price_score, :divergence, :classification,
                             :narrative)
                        ON CONFLICT (ticker, as_of) DO UPDATE
                        SET sector = EXCLUDED.sector,
                            fundamental_score = EXCLUDED.fundamental_score,
                            price_score = EXCLUDED.price_score,
                            divergence = EXCLUDED.divergence,
                            classification = EXCLUDED.classification,
                            narrative = EXCLUDED.narrative
                        """
                    ).bindparams(**r)
                )
                written += 1
            except Exception as exc:
                log.warning(
                    "fundamental_divergence: upsert failed for {t}: {e}",
                    t=r["ticker"], e=str(exc),
                )

            # SYNTH-26: non-fatal SignalFired fanout. Only non-aligned
            # classifications carry information — aligned rows are the
            # "everything in line" null hypothesis.
            if r["classification"] in ("long_candidate", "short_candidate"):
                try:
                    _emit_divergence_signal(r)
                except Exception as exc:  # pragma: no cover - defensive
                    log.debug(
                        "fundamental_divergence emit skipped for {t}: {e}",
                        t=r["ticker"], e=str(exc),
                    )

    summary = {
        "as_of": as_of.isoformat(),
        "rows": len(rows),
        "written": written,
        "counts": counts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    log.info(
        "fundamental_divergence: wrote {w}/{n} rows; long={l} short={s} aligned={a}",
        w=written, n=len(rows),
        l=counts.get("long_candidate", 0),
        s=counts.get("short_candidate", 0),
        a=counts.get("aligned", 0),
    )
    return summary


# ── SYNTH-26 emit helper ───────────────────────────────────────────

def _emit_divergence_signal(r: dict[str, Any]) -> None:
    """Emit a ``SignalFired`` contract for one divergence row.

    Strength encoding:

        long_candidate   →  +(|divergence| / 100) clamped ≤ 1.0
        short_candidate  →  −(|divergence| / 100) clamped ≥ -1.0

    The oracle handler's sign-to-BUY/SELL mapping converts this into a
    directional entry in ``signal_sources``.
    """
    from uuid import uuid4

    from contracts.correlation import (
        get_current_correlation_id,
        new_correlation_id,
    )
    from contracts.emit import emit as _emit
    from contracts.schemas import SignalFired

    divergence = float(r.get("divergence") or 0.0)
    classification = r.get("classification", "")
    if classification == "long_candidate":
        strength = min(1.0, abs(divergence) / 100.0)
    elif classification == "short_candidate":
        strength = -min(1.0, abs(divergence) / 100.0)
    else:
        return

    corr_id = get_current_correlation_id() or new_correlation_id()
    _emit(
        SignalFired(
            producer_module="intelligence.fundamental_divergence",
            correlation_id=corr_id,
            signal_id=uuid4(),
            source=f"fundamental_divergence:{r.get('sector') or 'unknown'}",
            signal_type="fundamental_divergence",
            strength=strength,
            ticker=r.get("ticker"),
            actor_hint=None,
            raw_row_ids=[],
        )
    )
