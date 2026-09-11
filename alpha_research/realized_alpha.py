"""Realized-alpha truth gate (GRID-4 pivot §8.1, LEVER-PACKAGE §7 T0.1).

The one instrument the 2026-05-16 pivot named as the thing that "blocks every
claim of positive alpha" and that was never built. It answers a single
question, every day, from the trades GRID has *actually* made:

    Net of 5 bp/side costs, did GRID beat SPY over the last N trading days?

Scope
-----
* ``paper_trades``       — every closed trade uses ``exit_date``/``exit_price``;
                           every OPEN trade is marked to the latest PIT close
                           and flagged ``is_open=True`` so it can be told apart.
* ``oracle_predictions`` — every scored (``verdict <> 'pending'``) prediction
                           uses ``created_at → expiry`` and
                           ``entry_price → actual_price``. AstroGrid rows
                           (ids prefixed ``astrogrid:`` or
                           ``flow_context->>'source' = 'astrogrid'``) are
                           excluded — they are a separate product and must not
                           contaminate the trading-layer number.

``decision_journal`` is deliberately **out of scope**: it has no ticker, entry
price or exit price columns, so a per-trade alpha cannot be computed from it
without inventing data. ``journal/log.py`` is not touched. When the journal
grows price fields this module is the place to add a third source.

Benchmark
---------
SPY total return over the identical entry→exit dates, read through
``store.pit.PITStore.get_feature_matrix`` (the only lookahead-safe read path).
The SPY feature is resolved at runtime from ``feature_registry`` in this
order: ``spy_full`` → the ``entity_map`` mapping for ``YF:SPY:adj_close`` →
``sp500_full`` (documented fallback — the index, not the ETF; no dividends).
The name that was used is logged and stored in the run summary.

Costs
-----
``cost_bps`` per side, charged twice (entry + exit), so the default 5 bp/side
is a 10 bp round-trip drag on gross return.

Outputs
-------
Two tables (``migrations/0057_realized_alpha.sql``):

* ``realized_alpha_daily``  — one row per (as_of, source, horizon_days):
  equal-weight mean alpha, annualized, n_trades, hit rate, mean SPY return.
* ``realized_alpha_trades`` — one row per (as_of, source, source_id): the
  per-trade decomposition so every headline number is auditable.

Kill criteria from the pivot (§8.1) are evaluated by whoever reads the table,
not here: "if 60d alpha after 90 days is < +1% annualized, pause and
root-cause. If 180d alpha < 0, kill the layer."

Entry points
------------
* ``compute_trade_alpha`` — pure function, fully unit-testable.
* ``compute_realized_alpha(engine, as_of)`` — windows only.
* ``run_daily(engine)`` — compute + persist; scheduled 06:30 UTC in
  ``intelligence/scheduler.py``.
* ``fetch_realized_alpha(...)`` — paginated read used by
  ``api/routers/realized_alpha.py``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

HORIZONS: tuple[int, ...] = (5, 10, 20, 60, 90, 180)
SOURCES: tuple[str, ...] = ("paper_trades", "oracle_predictions")
DEFAULT_COST_BPS: float = 5.0
TRADING_DAYS_PER_YEAR: int = 252

# SPY feature resolution order. The middle entry is looked up from the
# entity map at call time (it may be absent — that is fine).
_SPY_PRIMARY = "spy_full"
_SPY_ENTITY_KEY = "YF:SPY:adj_close"
_SPY_FALLBACK = "sp500_full"

_LONG_ALIASES = frozenset({"LONG", "BUY", "UP", "BULLISH", "CALL"})
_SHORT_ALIASES = frozenset({"SHORT", "SELL", "DOWN", "BEARISH", "PUT"})


# ─────────────────────────────────────────────────────────────────────
# Dataclasses
# ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TradeAlpha:
    """Per-trade decomposition: gross → net of costs → vs SPY."""

    gross_return: float
    cost_drag: float
    net_return: float
    spy_return: float
    alpha: float
    holding_days: int


@dataclass(frozen=True)
class TradeRow:
    """A trade or prediction as collected from its ledger."""

    source: str
    source_id: str
    ticker: str
    direction: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    is_open: bool = False


@dataclass(frozen=True)
class ScoredTrade:
    """A ``TradeRow`` joined to its ``TradeAlpha``."""

    trade: TradeRow
    result: TradeAlpha


@dataclass(frozen=True)
class AlphaWindow:
    """One row of ``realized_alpha_daily``."""

    as_of: date
    source: str
    horizon_days: int
    n_trades: int
    mean_alpha: float | None
    mean_alpha_annualized: float | None
    mean_gross: float | None
    mean_spy: float | None
    hit_rate: float | None
    cost_bps: float


# ─────────────────────────────────────────────────────────────────────
# Pure helpers
# ─────────────────────────────────────────────────────────────────────


def normalize_direction(direction: str | None) -> str | None:
    """Map the assorted direction vocabularies to ``LONG``/``SHORT``.

    ``paper_trades`` uses LONG/SHORT; ``oracle_predictions`` uses
    bullish/bearish (and CALL/PUT for options-style rows). Anything else
    (``neutral``, ``None``) returns ``None`` and the row is skipped.
    """
    if not direction:
        return None
    key = str(direction).strip().upper()
    if key in _LONG_ALIASES:
        return "LONG"
    if key in _SHORT_ALIASES:
        return "SHORT"
    return None


def _to_timestamp(d: date | datetime | pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(d).normalize()


def price_at(path: pd.Series, d: date | datetime | pd.Timestamp) -> float:
    """Last observed price on or before ``d`` (as-of lookup, no lookahead).

    Raises ``ValueError`` when the path has no observation at or before
    ``d`` — the caller must never treat a missing benchmark as 0 %.
    """
    if path is None or len(path) == 0:
        raise ValueError("price path is empty")
    ts = _to_timestamp(d)
    idx = path.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.DatetimeIndex(pd.to_datetime(idx))
        path = pd.Series(path.values, index=idx)
    path = path.sort_index()
    pos = path.index.searchsorted(ts, side="right") - 1
    if pos < 0:
        raise ValueError(f"price path has no observation on or before {ts.date()}")
    value = float(path.iloc[pos])
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"non-positive or non-finite price at {ts.date()}: {value}")
    return value


def trading_days_between(
    path_index: pd.DatetimeIndex | None,
    start: date | datetime,
    end: date | datetime,
) -> int:
    """Count trading days in ``(start, end]`` using the benchmark calendar.

    Falls back to a calendar-day approximation (252/365) when no calendar
    is available. Always returns at least 1 so annualization never divides
    by zero.
    """
    s, e = _to_timestamp(start), _to_timestamp(end)
    if e <= s:
        return 1
    if path_index is not None and len(path_index) > 0:
        idx = pd.DatetimeIndex(path_index).sort_values()
        n = int(((idx > s) & (idx <= e)).sum())
        if n > 0:
            return n
    cal = (e - s).days
    return max(1, int(round(cal * TRADING_DAYS_PER_YEAR / 365.0)))


def compute_trade_alpha(
    entry_date: date | datetime,
    exit_date: date | datetime,
    entry_price: float,
    exit_price: float,
    direction: str,
    spy_path: pd.Series,
    cost_bps: float = DEFAULT_COST_BPS,
) -> TradeAlpha:
    """Alpha of one trade vs SPY, net of ``cost_bps`` per side.

    * gross  = (exit/entry − 1) for LONG, (entry − exit)/entry for SHORT
    * net    = gross − 2 × cost_bps / 10 000
    * spy    = SPY(exit)/SPY(entry) − 1   (as-of lookups on ``spy_path``)
    * alpha  = net − spy

    ``spy_path`` is a ``pd.Series`` of SPY closes indexed by date. Raises
    ``ValueError`` on bad prices, an unknown direction, or a benchmark gap.
    """
    side = normalize_direction(direction)
    if side is None:
        raise ValueError(f"unrecognised direction: {direction!r}")
    if entry_price is None or exit_price is None:
        raise ValueError("entry_price and exit_price are required")
    entry_price = float(entry_price)
    exit_price = float(exit_price)
    if not (math.isfinite(entry_price) and math.isfinite(exit_price)):
        raise ValueError("entry_price/exit_price must be finite")
    if entry_price <= 0 or exit_price <= 0:
        raise ValueError("entry_price/exit_price must be positive")
    if cost_bps < 0:
        raise ValueError("cost_bps must be non-negative")

    raw = exit_price / entry_price - 1.0
    gross = raw if side == "LONG" else -raw
    cost_drag = 2.0 * float(cost_bps) / 10_000.0
    net = gross - cost_drag

    spy_entry = price_at(spy_path, entry_date)
    spy_exit = price_at(spy_path, exit_date)
    spy_ret = spy_exit / spy_entry - 1.0

    holding = trading_days_between(
        spy_path.index if isinstance(spy_path.index, pd.DatetimeIndex) else None,
        entry_date, exit_date,
    )
    return TradeAlpha(
        gross_return=gross,
        cost_drag=cost_drag,
        net_return=net,
        spy_return=spy_ret,
        alpha=net - spy_ret,
        holding_days=holding,
    )


def window_start(
    as_of: date,
    horizon_days: int,
    trading_calendar: pd.DatetimeIndex | None = None,
) -> date:
    """First date of the ``horizon_days``-trading-day window ending ``as_of``.

    Uses the benchmark calendar when available (the last ``horizon_days``
    sessions on or before ``as_of``); otherwise a 252/365 calendar
    approximation.
    """
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    ts = _to_timestamp(as_of)
    if trading_calendar is not None and len(trading_calendar) > 0:
        idx = pd.DatetimeIndex(trading_calendar).sort_values()
        idx = idx[idx <= ts]
        if len(idx) >= horizon_days:
            return idx[-horizon_days].date()
    cal_days = int(math.ceil(horizon_days * 365.0 / TRADING_DAYS_PER_YEAR))
    return (ts - timedelta(days=cal_days)).date()


def score_trades(
    trades: Iterable[TradeRow],
    spy_path: pd.Series,
    cost_bps: float = DEFAULT_COST_BPS,
) -> tuple[list[ScoredTrade], int]:
    """Apply ``compute_trade_alpha`` to every row; skip (and count) failures."""
    scored: list[ScoredTrade] = []
    skipped = 0
    for t in trades:
        try:
            res = compute_trade_alpha(
                t.entry_date, t.exit_date, t.entry_price, t.exit_price,
                t.direction, spy_path, cost_bps=cost_bps,
            )
        except ValueError as exc:
            skipped += 1
            log.debug(
                "realized_alpha: skipped {s}/{i} ({tk}): {e}",
                s=t.source, i=t.source_id, tk=t.ticker, e=str(exc),
            )
            continue
        scored.append(ScoredTrade(trade=t, result=res))
    return scored, skipped


def bucket_windows(
    scored: Iterable[ScoredTrade],
    as_of: date,
    source: str,
    cost_bps: float = DEFAULT_COST_BPS,
    horizons: tuple[int, ...] = HORIZONS,
    trading_calendar: pd.DatetimeIndex | None = None,
) -> list[AlphaWindow]:
    """Equal-weight mean alpha per horizon for trades whose exit/mark date
    falls inside ``(window_start, as_of]``.

    Annualization is linear: ``mean_alpha × 252 / mean_holding_days``.
    Linear (not compounded) so a losing short (alpha ≤ −100 %) cannot blow
    up the exponent; at the magnitudes a truth gate cares about the two
    agree to a few bp.
    """
    rows = [s for s in scored if s.trade.source == source]
    out: list[AlphaWindow] = []
    for h in horizons:
        start = window_start(as_of, h, trading_calendar)
        in_win = [
            s for s in rows
            if start < s.trade.exit_date <= as_of
        ]
        n = len(in_win)
        if n == 0:
            out.append(AlphaWindow(
                as_of=as_of, source=source, horizon_days=h, n_trades=0,
                mean_alpha=None, mean_alpha_annualized=None, mean_gross=None,
                mean_spy=None, hit_rate=None, cost_bps=cost_bps,
            ))
            continue
        mean_alpha = sum(s.result.alpha for s in in_win) / n
        mean_gross = sum(s.result.gross_return for s in in_win) / n
        mean_spy = sum(s.result.spy_return for s in in_win) / n
        mean_hold = sum(s.result.holding_days for s in in_win) / n
        hit_rate = sum(1 for s in in_win if s.result.alpha > 0) / n
        annualized = mean_alpha * TRADING_DAYS_PER_YEAR / max(mean_hold, 1.0)
        out.append(AlphaWindow(
            as_of=as_of, source=source, horizon_days=h, n_trades=n,
            mean_alpha=mean_alpha, mean_alpha_annualized=annualized,
            mean_gross=mean_gross, mean_spy=mean_spy, hit_rate=hit_rate,
            cost_bps=cost_bps,
        ))
    return out


# ─────────────────────────────────────────────────────────────────────
# Data access — SPY via PITStore only
# ─────────────────────────────────────────────────────────────────────


def _entity_mapped_spy_name() -> str | None:
    try:
        from normalization.entity_map import SEED_MAPPINGS
        return SEED_MAPPINGS.get(_SPY_ENTITY_KEY)
    except Exception as exc:  # noqa: BLE001
        log.debug("realized_alpha: entity_map unavailable: {e}", e=str(exc))
        return None


def resolve_spy_feature(engine: Engine) -> tuple[int, str]:
    """Return ``(feature_id, name)`` for the SPY benchmark.

    Tries ``spy_full``, then the entity-map name for ``YF:SPY:adj_close``,
    then ``sp500_full``. Logs which one won. Raises ``LookupError`` when
    none of them exist in ``feature_registry``.
    """
    candidates: list[str] = [_SPY_PRIMARY]
    mapped = _entity_mapped_spy_name()
    if mapped and mapped not in candidates:
        candidates.append(mapped)
    if _SPY_FALLBACK not in candidates:
        candidates.append(_SPY_FALLBACK)

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name FROM feature_registry WHERE name = ANY(:names)"),
            {"names": candidates},
        ).fetchall()
    by_name = {str(r[1]): int(r[0]) for r in rows}
    for name in candidates:
        if name in by_name:
            if name == _SPY_FALLBACK:
                log.warning(
                    "realized_alpha: SPY benchmark falling back to {n} "
                    "(index level, no dividends) — spy_full not in registry",
                    n=name,
                )
            else:
                log.info("realized_alpha: SPY benchmark feature = {n} (id={i})",
                         n=name, i=by_name[name])
            return by_name[name], name
    raise LookupError(
        f"no SPY benchmark feature in feature_registry (tried {candidates})"
    )


def load_price_path(
    engine: Engine,
    feature_id: int,
    start: date,
    end: date,
    as_of: date,
) -> pd.Series:
    """PIT-safe close series for one feature over ``[start, end]``.

    Goes through ``PITStore.get_feature_matrix`` (``release_date <= as_of``).
    ``LATEST_AS_OF`` so a corrected/adjusted price supersedes its first print.
    """
    from store.pit import PITStore

    matrix = PITStore(engine).get_feature_matrix(
        [feature_id], start, end, as_of, vintage_policy="LATEST_AS_OF",
    )
    if matrix is None or matrix.empty or feature_id not in matrix.columns:
        return pd.Series(dtype="float64", index=pd.DatetimeIndex([], name="obs_date"))
    series = pd.to_numeric(matrix[feature_id], errors="coerce").dropna()
    series.index = pd.DatetimeIndex(series.index).normalize()
    return series.astype("float64").sort_index()


def load_spy_path(engine: Engine, start: date, end: date, as_of: date) -> pd.Series:
    """SPY close path via the PIT store (see ``resolve_spy_feature``)."""
    fid, _name = resolve_spy_feature(engine)
    return load_price_path(engine, fid, start, end, as_of)


def _resolve_ticker_feature_id(engine: Engine, ticker: str) -> int | None:
    """Best-effort ``feature_registry`` id for a ticker's close series."""
    tk = str(ticker).strip()
    if not tk:
        return None
    candidates = [f"{tk.lower()}_full", f"{tk.lower()}_close"]
    try:
        from normalization.entity_map import SEED_MAPPINGS
        for key in (f"YF:{tk}:close", f"YF:{tk}:adj_close"):
            mapped = SEED_MAPPINGS.get(key)
            if mapped and mapped not in candidates:
                candidates.append(mapped)
    except Exception:  # noqa: BLE001
        pass
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name FROM feature_registry WHERE name = ANY(:names)"),
            {"names": candidates},
        ).fetchall()
    by_name = {str(r[1]): int(r[0]) for r in rows}
    for name in candidates:
        if name in by_name:
            return by_name[name]
    return None


def mark_open_trade(engine: Engine, ticker: str, as_of: date) -> tuple[date, float] | None:
    """Latest PIT close on or before ``as_of`` for ``ticker`` → (date, price)."""
    fid = _resolve_ticker_feature_id(engine, ticker)
    if fid is None:
        return None
    path = load_price_path(engine, fid, as_of - timedelta(days=14), as_of, as_of)
    if path.empty:
        return None
    last_ts = path.index[-1]
    return last_ts.date(), float(path.iloc[-1])


# ─────────────────────────────────────────────────────────────────────
# Trade collection
# ─────────────────────────────────────────────────────────────────────


def _is_astrogrid(source_id: str, fc_source: str | None) -> bool:
    return str(source_id).startswith("astrogrid:") or (
        fc_source is not None and str(fc_source).lower() == "astrogrid"
    )


def _as_date(v: Any) -> date | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return pd.Timestamp(v).date()
    except Exception:  # noqa: BLE001
        return None


def _collect_paper_trades(engine: Engine, as_of: date) -> list[TradeRow]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, ticker, direction, entry_price, exit_price,
                       entry_date, exit_date, status
                FROM paper_trades
                WHERE entry_date <= :as_of
                  AND entry_price IS NOT NULL
                  AND entry_price > 0
                ORDER BY entry_date, id
            """),
            {"as_of": as_of},
        ).fetchall()

    out: list[TradeRow] = []
    mark_cache: dict[str, tuple[date, float] | None] = {}
    for r in rows:
        pid, ticker, direction, entry_px, exit_px, entry_dt, exit_dt, status = r[:8]
        entry_d = _as_date(entry_dt)
        if entry_d is None or not ticker:
            continue
        status_u = str(status or "").upper()
        closed = status_u in ("CLOSED", "STOPPED") and exit_px is not None and exit_dt is not None
        if closed:
            exit_d = _as_date(exit_dt)
            if exit_d is None or exit_d > as_of:
                continue
            out.append(TradeRow(
                source="paper_trades", source_id=str(pid), ticker=str(ticker),
                direction=str(direction), entry_date=entry_d, exit_date=exit_d,
                entry_price=float(entry_px), exit_price=float(exit_px), is_open=False,
            ))
            continue
        # OPEN (or closed-without-exit): mark to the latest PIT close.
        key = str(ticker).upper()
        if key not in mark_cache:
            try:
                mark_cache[key] = mark_open_trade(engine, key, as_of)
            except Exception as exc:  # noqa: BLE001
                log.debug("realized_alpha: mark failed for {t}: {e}", t=key, e=str(exc))
                mark_cache[key] = None
        mark = mark_cache[key]
        if mark is None or mark[0] < entry_d:
            continue
        out.append(TradeRow(
            source="paper_trades", source_id=str(pid), ticker=str(ticker),
            direction=str(direction), entry_date=entry_d, exit_date=mark[0],
            entry_price=float(entry_px), exit_price=float(mark[1]), is_open=True,
        ))
    return out


def _collect_oracle_predictions(engine: Engine, as_of: date) -> list[TradeRow]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, ticker, direction, entry_price, actual_price,
                       (created_at AT TIME ZONE 'UTC')::date AS entry_date,
                       expiry, verdict, flow_context->>'source' AS fc_source
                FROM oracle_predictions
                WHERE verdict IS NOT NULL AND verdict <> 'pending'
                  AND actual_price IS NOT NULL
                  AND entry_price IS NOT NULL AND entry_price > 0
                  AND expiry <= :as_of
                  AND dedup_keep = TRUE
                  AND left(id, 10) <> 'astrogrid:'
                  AND COALESCE(flow_context->>'source', '') <> 'astrogrid'
                ORDER BY expiry, id
            """),
            {"as_of": as_of},
        ).fetchall()

    out: list[TradeRow] = []
    for r in rows:
        pid, ticker, direction, entry_px, actual_px, entry_dt, expiry, _verdict, fc_source = r[:9]
        if _is_astrogrid(str(pid), fc_source):
            continue  # belt-and-braces: never trust the SQL filter alone
        entry_d, exit_d = _as_date(entry_dt), _as_date(expiry)
        if entry_d is None or exit_d is None or not ticker:
            continue
        if actual_px is None or entry_px is None:
            continue
        if normalize_direction(direction) is None:
            continue
        out.append(TradeRow(
            source="oracle_predictions", source_id=str(pid), ticker=str(ticker),
            direction=str(direction), entry_date=entry_d, exit_date=exit_d,
            entry_price=float(entry_px), exit_price=float(actual_px), is_open=False,
        ))
    return out


def collect_trades(engine: Engine, source: str, as_of: date | None = None) -> list[TradeRow]:
    """Collect every scorable trade/prediction from ``source``.

    ``source`` ∈ {"paper_trades", "oracle_predictions"}. Rows with an
    exit/expiry after ``as_of`` are excluded (they have not happened yet
    from the gate's point of view).
    """
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}; expected one of {SOURCES}")
    as_of = as_of or date.today()
    if source == "paper_trades":
        return _collect_paper_trades(engine, as_of)
    return _collect_oracle_predictions(engine, as_of)


# ─────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────


def _compute(
    engine: Engine,
    as_of: date,
    cost_bps: float,
) -> tuple[list[AlphaWindow], list[ScoredTrade], dict[str, Any]]:
    trades: list[TradeRow] = []
    per_source_counts: dict[str, int] = {}
    for src in SOURCES:
        try:
            rows = collect_trades(engine, src, as_of)
        except Exception as exc:  # noqa: BLE001
            log.warning("realized_alpha: collect {s} failed: {e}", s=src, e=str(exc))
            rows = []
        per_source_counts[src] = len(rows)
        trades.extend(rows)

    spy_fid, spy_name = resolve_spy_feature(engine)
    if trades:
        earliest = min(t.entry_date for t in trades)
    else:
        earliest = as_of
    # Widen the window enough for the 180-day bucket plus lookback slack.
    start = min(earliest, as_of - timedelta(days=400)) - timedelta(days=10)
    spy_path = load_price_path(engine, spy_fid, start, as_of, as_of)
    if spy_path.empty:
        raise LookupError(f"SPY path ({spy_name}) is empty for {start}..{as_of}")

    scored, skipped = score_trades(trades, spy_path, cost_bps)
    windows: list[AlphaWindow] = []
    for src in SOURCES:
        windows.extend(bucket_windows(
            scored, as_of, src, cost_bps=cost_bps,
            trading_calendar=spy_path.index,
        ))
    summary = {
        "as_of": as_of.isoformat(),
        "spy_feature": spy_name,
        "collected": per_source_counts,
        "scored": len(scored),
        "skipped": skipped,
        "open_marked": sum(1 for s in scored if s.trade.is_open),
    }
    return windows, scored, summary


def compute_realized_alpha(
    engine: Engine,
    as_of: date | None = None,
    cost_bps: float = DEFAULT_COST_BPS,
) -> list[AlphaWindow]:
    """Rolling realized alpha per (source, horizon) as of ``as_of``.

    Does not persist anything — see ``run_daily`` for that.
    """
    as_of = as_of or date.today()
    windows, _scored, _summary = _compute(engine, as_of, cost_bps)
    return windows


_INSERT_DAILY = text("""
    INSERT INTO realized_alpha_daily (
        as_of, source, horizon_days, n_trades, mean_alpha,
        mean_alpha_annualized, mean_gross, mean_spy, hit_rate, cost_bps,
        computed_at
    ) VALUES (
        :as_of, :source, :horizon_days, :n_trades, :mean_alpha,
        :mean_alpha_annualized, :mean_gross, :mean_spy, :hit_rate, :cost_bps,
        NOW()
    )
    ON CONFLICT (as_of, source, horizon_days) DO UPDATE SET
        n_trades = EXCLUDED.n_trades,
        mean_alpha = EXCLUDED.mean_alpha,
        mean_alpha_annualized = EXCLUDED.mean_alpha_annualized,
        mean_gross = EXCLUDED.mean_gross,
        mean_spy = EXCLUDED.mean_spy,
        hit_rate = EXCLUDED.hit_rate,
        cost_bps = EXCLUDED.cost_bps,
        computed_at = NOW()
""")

_INSERT_TRADE = text("""
    INSERT INTO realized_alpha_trades (
        as_of, source, source_id, ticker, entry_date, exit_date, is_open,
        gross_return, spy_return, alpha, cost_bps
    ) VALUES (
        :as_of, :source, :source_id, :ticker, :entry_date, :exit_date, :is_open,
        :gross_return, :spy_return, :alpha, :cost_bps
    )
    ON CONFLICT (as_of, source, source_id) DO UPDATE SET
        ticker = EXCLUDED.ticker,
        entry_date = EXCLUDED.entry_date,
        exit_date = EXCLUDED.exit_date,
        is_open = EXCLUDED.is_open,
        gross_return = EXCLUDED.gross_return,
        spy_return = EXCLUDED.spy_return,
        alpha = EXCLUDED.alpha,
        cost_bps = EXCLUDED.cost_bps
""")


def persist(
    engine: Engine,
    windows: Iterable[AlphaWindow],
    scored: Iterable[ScoredTrade],
    as_of: date,
) -> dict[str, int]:
    """Upsert window rows and per-trade rows for ``as_of``."""
    daily_params = [
        {
            "as_of": w.as_of, "source": w.source, "horizon_days": w.horizon_days,
            "n_trades": w.n_trades, "mean_alpha": w.mean_alpha,
            "mean_alpha_annualized": w.mean_alpha_annualized,
            "mean_gross": w.mean_gross, "mean_spy": w.mean_spy,
            "hit_rate": w.hit_rate, "cost_bps": w.cost_bps,
        }
        for w in windows
    ]
    trade_params = [
        {
            "as_of": as_of, "source": s.trade.source, "source_id": s.trade.source_id,
            "ticker": s.trade.ticker, "entry_date": s.trade.entry_date,
            "exit_date": s.trade.exit_date, "is_open": s.trade.is_open,
            "gross_return": s.result.gross_return, "spy_return": s.result.spy_return,
            "alpha": s.result.alpha, "cost_bps": s.result.cost_drag * 10_000.0 / 2.0,
        }
        for s in scored
    ]
    with engine.begin() as conn:
        if daily_params:
            conn.execute(_INSERT_DAILY, daily_params)
        if trade_params:
            conn.execute(_INSERT_TRADE, trade_params)
    return {"daily_rows": len(daily_params), "trade_rows": len(trade_params)}


def run_daily(
    engine: Engine,
    as_of: date | None = None,
    cost_bps: float = DEFAULT_COST_BPS,
) -> dict[str, Any]:
    """Compute and persist realized alpha for ``as_of`` (default today).

    Returns a summary dict; never raises on an empty ledger (writes the
    zero-trade rows so the gate is visibly *measured*, not merely absent).
    """
    as_of = as_of or date.today()
    windows, scored, summary = _compute(engine, as_of, cost_bps)
    counts = persist(engine, windows, scored, as_of)
    summary.update(counts)
    headline = {
        f"{w.source}/{w.horizon_days}d": (
            None if w.mean_alpha_annualized is None
            else round(w.mean_alpha_annualized, 4)
        )
        for w in windows
        if w.horizon_days in (60, 180)
    }
    summary["headline_annualized"] = headline
    log.info(
        "realized_alpha {d}: {s} scored ({k} skipped), spy={spy}, "
        "60d/180d annualized={h}",
        d=as_of, s=summary["scored"], k=summary["skipped"],
        spy=summary["spy_feature"], h=headline,
    )
    return summary


# ─────────────────────────────────────────────────────────────────────
# Read path (for the router)
# ─────────────────────────────────────────────────────────────────────


def fetch_realized_alpha(
    engine: Engine,
    source: str | None = None,
    horizon_days: int | None = None,
    days: int = 90,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Paginated read of ``realized_alpha_daily`` for the last ``days`` days."""
    where = ["as_of >= CURRENT_DATE - make_interval(days => :days)"]
    params: dict[str, Any] = {"days": int(days)}
    if source:
        if source not in SOURCES:
            raise ValueError(f"unknown source {source!r}")
        where.append("source = :source")
        params["source"] = source
    if horizon_days is not None:
        where.append("horizon_days = :horizon_days")
        params["horizon_days"] = int(horizon_days)
    where_sql = " AND ".join(where)

    select_sql = text(
        "SELECT as_of, source, horizon_days, n_trades, mean_alpha, "
        "mean_alpha_annualized, mean_gross, mean_spy, hit_rate, cost_bps, "
        "computed_at FROM realized_alpha_daily WHERE " + where_sql +
        " ORDER BY as_of DESC, source, horizon_days LIMIT :limit OFFSET :offset"
    )
    count_sql = text(
        "SELECT COUNT(*) FROM realized_alpha_daily WHERE " + where_sql
    )
    with engine.connect() as conn:
        rows = conn.execute(
            select_sql, {**params, "limit": int(limit), "offset": int(offset)},
        ).fetchall()
        total_row = conn.execute(count_sql, params).fetchone()
    total = int(total_row[0]) if total_row else 0

    entries = [
        {
            "as_of": _iso(r[0]),
            "source": r[1],
            "horizon_days": int(r[2]),
            "n_trades": int(r[3] or 0),
            "mean_alpha": _f(r[4]),
            "mean_alpha_annualized": _f(r[5]),
            "mean_gross": _f(r[6]),
            "mean_spy": _f(r[7]),
            "hit_rate": _f(r[8]),
            "cost_bps": _f(r[9]),
            "computed_at": _iso(r[10]),
        }
        for r in rows
    ]
    return {
        "entries": entries,
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "has_more": (int(offset) + int(limit)) < total,
    }


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None
