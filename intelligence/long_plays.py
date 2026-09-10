"""Long Plays board — multi-year candidates with explicit multiple math.

Purpose
-------
GRID already had every ingredient for a sit-for-years 10x/100x hunt —
frontier themes (``strategy.ten_year_portfolio``), edge-scanner playbooks
(``intelligence.market_edge_scanner``), trial catalysts (``upcoming_catalysts``
view), options asymmetry (``options_mispricing_scans``), the coverage-gated
weekly sweep (``universe_ranking_history``) and the engine's own realized
alpha track record — but nothing composed them at a multi-year horizon.
This module does exactly that and nothing else: it *reads* those surfaces,
computes the multiple math explicitly, and persists a JSON board.

Honesty rules (read before touching any number here)
---------------------------------------------------
* Every projection is a **proxy**: a seeded normal Monte Carlo driven by
  the historical CAGR / volatility of the adjusted-close chart
  (``strategy.ten_year_portfolio.build_monte_carlo_projection``). It is
  labelled ``method = "seeded_normal_proxy_from_historical_cagr_and_volatility"``
  on every candidate and must never be presented as a forecast.
* ``multiple_math`` is arithmetic on that proxy and on the latest market
  cap — "what must be true" — not a prediction that it will happen.
* Every external read degrades to ``None`` plus a line in
  ``method_notes``. A missing table or a DB outage produces a thinner
  board, never an exception.
* ``as_of`` is the point-in-time cut-off for every price read
  (``resolved_series`` via ``store.pit.PITStore``; ``raw_series`` bounded
  by ``pull_timestamp <= as_of``).

Asymmetry score (``asymmetry_score``)
-------------------------------------
``clamp( 0.5 * tanh(ln(p90_3y_multiple))
       + 0.2 * min(catalyst_strength, 1)
       + 0.2 * min(payoff_multiple / 100, 1)
       + 0.1 * playbook_edge / 25 , 0, 1)``

* ``p90_3y_multiple`` — the p90 outcome of the 3-year proxy projection
  expressed as a multiple of 1.0 invested. ``tanh(ln(x))`` is 0 at 1x,
  ~0.76 at 4x, ~0.96 at 10x and negative below 1x, so the term rewards
  right-tail width and penalises charts whose right tail is still a loss.
* ``catalyst_strength`` — best ``trial_strength_score`` of an upcoming
  catalyst (0..1).
* ``payoff_multiple`` — best options payoff multiple in the last 30 days.
* ``playbook_edge`` — ``base_edge`` (percent) of the strongest playbook
  that lists the ticker; 25 is the scale ceiling.

Missing components contribute 0; the sum is clamped to [0, 1].

Stance (``classify_stance``)
----------------------------
* ``entry_candidate`` only if ALL of:
    1. p50 3y multiple > 1.5, OR a trial catalyst inside 12 months with
       ``trial_strength_score >= 0.6``;
    2. max drawdown over the chart window shallower than -75 %
       (drawdown is stored as a signed negative fraction: -0.62 = -62 %);
    3. the latest sweep verdict for the ticker is ``high`` or ``moderate``
       (the coverage-gated stack agrees), OR the ticker has no sweep
       coverage AND ``options_asymmetry.max_payoff_multiple >= 20``.
* ``avoid`` if p10 3y multiple < 0.25 and there is no catalyst at all.
* otherwise ``watch``.

Public API
----------
``build_long_plays_board(engine, *, as_of=None, horizons_years=(1, 3, 5), top_k=25)``
``classify_stance(...)``, ``asymmetry_score(...)``, ``multiple_math(...)``
``ensure_long_plays_table(engine)``, ``persist_board(engine, board)``,
``load_latest_board(engine)``
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Sequence, TypeVar

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from strategy.ten_year_portfolio import (
    FRONTIER_THEME_CANDIDATES,
    FRONTIER_THEMATIC_UNIVERSE,
    build_monte_carlo_projection,
    compute_chart_metrics,
    is_common_stock_candidate,
)

# ── Constants ─────────────────────────────────────────────────────────────

DEFAULT_HORIZONS_YEARS: tuple[int, ...] = (1, 3, 5)
DEFAULT_TOP_K: int = 25
CHART_YEARS: int = 10
STANCE_HORIZON_YEARS: int = 3
PROJECTION_SIMULATIONS: int = 2000
PROJECTION_METHOD: str = "seeded_normal_proxy_from_historical_cagr_and_volatility"
MULTIPLE_MATH_NOTE: str = "proxy from historical CAGR/vol; not a forecast"

CATALYST_WINDOW_DAYS: int = 5 * 365
CATALYST_OVERRIDE_DAYS: int = 365
CATALYST_OVERRIDE_STRENGTH: float = 0.6
OPTIONS_LOOKBACK_DAYS: int = 30
TRIAL_LOOKBACK_DAYS: int = 180
SWEEP_HORIZON_DAYS: int = 90
REALIZED_ALPHA_HORIZON_DAYS: int = 60

ENTRY_P50_3Y_MULTIPLE: float = 1.5
ENTRY_MAX_DRAWDOWN: float = -0.75
ENTRY_OPTIONS_PAYOFF_MULTIPLE: float = 20.0
AVOID_P10_3Y_MULTIPLE: float = 0.25
ENTRY_SWEEP_VERDICTS: frozenset[str] = frozenset({"high", "moderate"})

STANCE_ENTRY: str = "entry_candidate"
STANCE_WATCH: str = "watch"
STANCE_AVOID: str = "avoid"

# (upper bound exclusive, bucket) — USD.
MARKET_CAP_BUCKETS: tuple[tuple[float, str], ...] = (
    (300e6, "micro"),
    (2e9, "small"),
    (10e9, "mid"),
    (200e9, "large"),
    (math.inf, "mega"),
)

T = TypeVar("T")


# ── Pure helpers (no I/O) ─────────────────────────────────────────────────


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    """Clamp ``value`` into ``[lo, hi]``."""
    return max(lo, min(hi, value))


def _finite(value: Any) -> float | None:
    """Return ``value`` as a finite float, else ``None``."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def market_cap_bucket(market_cap_usd: float | None) -> str | None:
    """Bucket a USD market cap into micro/small/mid/large/mega."""
    mcap = _finite(market_cap_usd)
    if mcap is None or mcap <= 0:
        return None
    for upper, label in MARKET_CAP_BUCKETS:
        if mcap < upper:
            return label
    return "mega"


def asymmetry_score(
    *,
    p90_3y_multiple: float | None,
    catalyst_strength: float | None = None,
    payoff_multiple: float | None = None,
    playbook_edge: float | None = None,
) -> float:
    """Composite right-tail score in ``[0, 1]`` (formula in module docstring).

    Missing or non-positive components contribute 0.
    """
    score = 0.0
    p90 = _finite(p90_3y_multiple)
    if p90 is not None and p90 > 0:
        score += 0.5 * math.tanh(math.log(p90))
    strength = _finite(catalyst_strength)
    if strength is not None and strength > 0:
        score += 0.2 * min(strength, 1.0)
    payoff = _finite(payoff_multiple)
    if payoff is not None and payoff > 0:
        score += 0.2 * min(payoff / 100.0, 1.0)
    edge = _finite(playbook_edge)
    if edge is not None and edge > 0:
        score += 0.1 * (edge / 25.0)
    return round(clamp(score), 4)


def classify_stance(
    *,
    p50_3y_multiple: float | None,
    p10_3y_multiple: float | None,
    max_drawdown: float | None,
    sweep_verdict: str | None,
    has_sweep_coverage: bool,
    catalyst_strength_12m: float | None = None,
    options_payoff_multiple: float | None = None,
    has_catalyst: bool = False,
) -> str:
    """Pure stance rule (documented in the module docstring).

    ``max_drawdown`` is the signed drawdown of the chart window
    (``-0.62`` means a 62 % peak-to-trough loss). A missing chart
    (``None``) fails the drawdown gate — a name with no price history
    cannot be an entry candidate on this board.
    """
    p50 = _finite(p50_3y_multiple)
    p10 = _finite(p10_3y_multiple)
    drawdown = _finite(max_drawdown)
    strength_12m = _finite(catalyst_strength_12m)
    payoff = _finite(options_payoff_multiple)
    verdict = (sweep_verdict or "").strip().lower() or None

    catalyst_override = strength_12m is not None and strength_12m >= CATALYST_OVERRIDE_STRENGTH
    return_gate = (p50 is not None and p50 > ENTRY_P50_3Y_MULTIPLE) or catalyst_override
    drawdown_gate = drawdown is not None and drawdown > ENTRY_MAX_DRAWDOWN
    if has_sweep_coverage:
        coverage_gate = verdict in ENTRY_SWEEP_VERDICTS
    else:
        coverage_gate = payoff is not None and payoff >= ENTRY_OPTIONS_PAYOFF_MULTIPLE

    if return_gate and drawdown_gate and coverage_gate:
        return STANCE_ENTRY
    if p10 is not None and p10 < AVOID_P10_3Y_MULTIPLE and not has_catalyst:
        return STANCE_AVOID
    return STANCE_WATCH


def _years_to_multiple(cagr: float | None, target_multiple: float = 10.0) -> float | None:
    """Years for ``(1 + cagr) ** years == target_multiple``; ``None`` if cagr <= 0."""
    rate = _finite(cagr)
    if rate is None or rate <= 0:
        return None
    return round(math.log(target_multiple) / math.log1p(rate), 1)


def _cagr_from_multiple(multiple: float | None, years: int) -> float | None:
    """Annualise a terminal multiple over ``years``."""
    mult = _finite(multiple)
    if mult is None or mult <= 0 or years <= 0:
        return None
    return mult ** (1.0 / years) - 1.0


def multiple_math(
    *,
    p50_3y_multiple: float | None,
    p90_3y_multiple: float | None,
    market_cap_usd: float | None,
    horizon_years: int = STANCE_HORIZON_YEARS,
) -> dict[str, Any]:
    """Explicit 10x/100x arithmetic from the proxy projection and market cap.

    The p50/p90 multiples are annualised into CAGR proxies, then
    ``ln(10) / ln(1 + cagr)`` gives the years to 10x *if* that CAGR
    held. ``None`` when the proxy CAGR is non-positive (no finite path).
    """
    cagr_p50 = _cagr_from_multiple(p50_3y_multiple, horizon_years)
    cagr_p90 = _cagr_from_multiple(p90_3y_multiple, horizon_years)
    mcap = _finite(market_cap_usd)
    return {
        "p50_cagr_proxy": round(cagr_p50, 4) if cagr_p50 is not None else None,
        "p90_cagr_proxy": round(cagr_p90, 4) if cagr_p90 is not None else None,
        "years_to_10x_at_p50_cagr": _years_to_multiple(cagr_p50, 10.0),
        "years_to_10x_at_p90": _years_to_multiple(cagr_p90, 10.0),
        "mcap_at_10x_usd": round(mcap * 10.0, 2) if mcap is not None and mcap > 0 else None,
        "mcap_at_100x_usd": round(mcap * 100.0, 2) if mcap is not None and mcap > 0 else None,
        "note": MULTIPLE_MATH_NOTE,
    }


def _fmt_usd(value: float | None) -> str:
    """Human-readable USD (``$1.2 B``)."""
    v = _finite(value)
    if v is None:
        return "unknown"
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
        if abs(v) >= divisor:
            return f"${v / divisor:.1f} {suffix}"
    return f"${v:,.0f}"


def _what_must_be_true(math_block: dict[str, Any]) -> str:
    """One sentence of market-cap arithmetic for a 10x."""
    mcap_10x = math_block.get("mcap_at_10x_usd")
    years = math_block.get("years_to_10x_at_p50_cagr")
    cagr = math_block.get("p50_cagr_proxy")
    if mcap_10x is None:
        head = "Market cap unknown, so the 10x target cap cannot be stated"
    else:
        head = f"Market cap must reach {_fmt_usd(mcap_10x)}"
    if years is None or cagr is None:
        tail = "the historical CAGR proxy is non-positive, so there is no finite path to 10x on trend alone"
    else:
        tail = f"at today's p50 CAGR proxy ({cagr:.0%}) that is {years:.0f} years"
    return f"{head}; {tail}."


def _why(
    *,
    themes: Sequence[str],
    thesis_sources: Sequence[dict[str, Any]],
    projection_3y: dict[str, Any] | None,
    sweep: dict[str, Any] | None,
    options: dict[str, Any] | None,
    catalysts: Sequence[dict[str, Any]],
) -> str:
    """Two-sentence rationale: tailwind, multiple math, coverage gate."""
    if themes:
        tailwind = ", ".join(themes[:3])
    elif thesis_sources:
        tailwind = str(thesis_sources[0].get("text") or thesis_sources[0].get("kind"))[:120]
    else:
        tailwind = "no named tailwind"
    if projection_3y:
        p50 = projection_3y.get("p50_multiple")
        p90 = projection_3y.get("p90_multiple")
        math_part = f"3y proxy multiples p50 {p50}x / p90 {p90}x (not a forecast)"
    else:
        math_part = "no chart history, so no multiple proxy"
    gate_bits: list[str] = []
    if sweep:
        gate_bits.append(f"sweep verdict {sweep.get('verdict')}")
    else:
        gate_bits.append("no sweep coverage")
    if options and options.get("max_payoff_multiple") is not None:
        gate_bits.append(f"options payoff {options['max_payoff_multiple']:.0f}x")
    if catalysts:
        first = catalysts[0]
        gate_bits.append(
            f"next catalyst {first.get('event_type')} in {first.get('days_out')} d"
        )
    return f"Tailwind: {tailwind}. Multiple math: {math_part}; coverage gate: {'; '.join(gate_bits)}."


# ── Degrade-to-None wrapper ───────────────────────────────────────────────


def _safe(
    name: str,
    fn: Callable[[], T],
    notes: list[str],
    default: T,
) -> T:
    """Run ``fn``; on any exception log, append a method note and return ``default``."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001
        log.warning("long_plays: {n} unavailable: {e}", n=name, e=str(exc))
        notes.append(f"{name}: unavailable ({type(exc).__name__}: {str(exc)[:120]}); field degraded to None")
        return default


# ── Loaders (all parameterised; all take the PIT cut-off where prices are read) ──

_FEATURE_IDS_SQL = text(
    """
    SELECT id, name
    FROM feature_registry
    WHERE name = ANY(:names)
    """
)

_RAW_ADJ_CLOSE_SQL = text(
    """
    SELECT DISTINCT ON (series_id, obs_date)
           series_id, obs_date, value
    FROM raw_series
    WHERE series_id = ANY(:series_ids)
      AND pull_status = 'SUCCESS'
      AND value > 0
      AND obs_date >= :start
      AND obs_date <= :as_of
      AND pull_timestamp <= :as_of_ts
    ORDER BY series_id, obs_date ASC, pull_timestamp DESC
    """
)

_MARKET_CAP_SQL = text(
    """
    SELECT DISTINCT ON (ticker) ticker, market_cap_usd, obs_date
    FROM ticker_metrics_daily
    WHERE ticker = ANY(:tickers)
      AND market_cap_usd IS NOT NULL
      AND obs_date <= :as_of
    ORDER BY ticker, obs_date DESC
    """
)

_CATALYSTS_SQL = text(
    """
    SELECT ticker, event_type, expected_date, days_out, confidence_window_days,
           trial_strength_score, signal_type, regime_at_signal, market_cap_mm,
           primary_indication
    FROM upcoming_catalysts
    WHERE expected_date >= :as_of
      AND expected_date <= :max_date
    ORDER BY expected_date ASC
    """
)

_TRIAL_TICKERS_SQL = text(
    """
    SELECT DISTINCT ON (ticker) ticker, company_name, primary_indication, market_cap_mm
    FROM trial_signals
    WHERE created_at >= :start
      AND created_at <= :as_of_ts
    ORDER BY ticker, created_at DESC
    """
)

_OPTIONS_SQL = text(
    """
    SELECT DISTINCT ON (ticker) ticker, scan_date, score, payoff_multiple, direction, thesis, is_100x
    FROM options_mispricing_scans
    WHERE scan_date >= :start
      AND scan_date <= :as_of
    ORDER BY ticker, payoff_multiple DESC, scan_date DESC
    """
)

_REALIZED_ALPHA_SQL = text(
    """
    SELECT source, horizon_days, mean_alpha, n_trades
    FROM realized_alpha_daily
    WHERE as_of = (SELECT MAX(as_of) FROM realized_alpha_daily)
    """
)


def _as_of_timestamp(as_of: date) -> datetime:
    """End of the ``as_of`` day in UTC — the PIT cut-off for timestamp columns."""
    return datetime.combine(as_of, datetime.max.time()).replace(tzinfo=timezone.utc)


def _normalize_ticker(value: Any) -> str | None:
    """Uppercase/strip a ticker; ``None`` when empty."""
    if value is None:
        return None
    ticker = str(value).strip().upper()
    return ticker or None


def _load_adj_close(
    engine: Engine,
    tickers: Sequence[str],
    years: int,
    as_of: date,
) -> dict[str, list[tuple[date, float]]]:
    """Adjusted-close history per ticker, PIT-bounded by ``as_of``.

    Tickers with a ``{ticker.lower()}_full`` feature are read from
    ``resolved_series`` through ``PITStore.get_feature_matrix``
    (``LATEST_AS_OF`` — adjusted closes are revised by later splits and
    dividends, and the latest vintage known at ``as_of`` is the one a
    chart reader would have seen). The rest fall back to ``raw_series``
    ``YF:{T}:adj_close`` with ``DISTINCT ON (obs_date)`` ordered by
    ``pull_timestamp DESC`` (``raw_series`` carries no ``vintage_date``;
    ``pull_timestamp`` is its vintage), bounded by ``obs_date >= start``
    and ``pull_timestamp <= as_of``.
    """
    wanted = sorted({t for t in (_normalize_ticker(x) for x in tickers) if t})
    if not wanted:
        return {}
    start = as_of - timedelta(days=int(years * 365.25) + 45)
    history: dict[str, list[tuple[date, float]]] = {}

    # 1. resolved_series via PIT store for tickers that have a *_full feature.
    feature_to_ticker: dict[str, str] = {f"{t.lower()}_full": t for t in wanted}
    id_to_ticker: dict[int, str] = {}
    with engine.connect() as conn:
        rows = conn.execute(_FEATURE_IDS_SQL, {"names": list(feature_to_ticker)}).fetchall()
    for row in rows:
        ticker = feature_to_ticker.get(str(row[1]))
        if ticker:
            id_to_ticker[int(row[0])] = ticker

    if id_to_ticker:
        from store.pit import PITStore

        matrix = PITStore(engine).get_feature_matrix(
            list(id_to_ticker),
            start_date=start,
            end_date=as_of,
            as_of_date=as_of,
            vintage_policy="LATEST_AS_OF",
        )
        for feature_id, ticker in id_to_ticker.items():
            if feature_id not in matrix.columns:
                continue
            series = matrix[feature_id].dropna()
            points = [
                (idx.date() if hasattr(idx, "date") else idx, float(val))
                for idx, val in series.items()
                if val is not None and float(val) > 0
            ]
            if points:
                history[ticker] = points

    # 2. raw_series fallback for the rest.
    remaining = [t for t in wanted if t not in history]
    if remaining:
        series_to_ticker = {f"YF:{t}:adj_close": t for t in remaining}
        with engine.connect() as conn:
            raw_rows = conn.execute(
                _RAW_ADJ_CLOSE_SQL,
                {
                    "series_ids": list(series_to_ticker),
                    "start": start,
                    "as_of": as_of,
                    "as_of_ts": _as_of_timestamp(as_of),
                },
            ).fetchall()
        for row in raw_rows:
            ticker = series_to_ticker.get(str(row[0]))
            if ticker is None:
                continue
            obs = row[1]
            obs_date = obs.date() if isinstance(obs, datetime) else obs
            history.setdefault(ticker, []).append((obs_date, float(row[2])))
        for ticker in remaining:
            if ticker in history:
                history[ticker].sort(key=lambda p: p[0])
    return history


def _load_market_caps(
    engine: Engine, tickers: Sequence[str], as_of: date
) -> dict[str, dict[str, Any]]:
    """Latest ``ticker_metrics_daily.market_cap_usd`` per ticker at or before ``as_of``."""
    wanted = sorted({t for t in (_normalize_ticker(x) for x in tickers) if t})
    if not wanted:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(_MARKET_CAP_SQL, {"tickers": wanted, "as_of": as_of}).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = _normalize_ticker(row[0])
        mcap = _finite(row[1])
        if ticker and mcap is not None and mcap > 0:
            obs = row[2]
            out[ticker] = {
                "market_cap_usd": mcap,
                "as_of": obs.isoformat() if hasattr(obs, "isoformat") else obs,
            }
    return out


def _load_catalysts(engine: Engine, as_of: date) -> dict[str, list[dict[str, Any]]]:
    """``upcoming_catalysts`` rows inside the 5-year window, keyed by ticker."""
    max_date = as_of + timedelta(days=CATALYST_WINDOW_DAYS)
    with engine.connect() as conn:
        rows = conn.execute(_CATALYSTS_SQL, {"as_of": as_of, "max_date": max_date}).fetchall()
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        ticker = _normalize_ticker(row[0])
        if not ticker:
            continue
        expected = row[2]
        expected_date = expected.date() if isinstance(expected, datetime) else expected
        days_out = (expected_date - as_of).days if isinstance(expected_date, date) else _finite(row[3])
        mcap_mm = _finite(row[8])
        out.setdefault(ticker, []).append(
            {
                "event_type": row[1],
                "expected_date": expected_date.isoformat() if hasattr(expected_date, "isoformat") else expected_date,
                "days_out": int(days_out) if days_out is not None else None,
                "confidence_window_days": int(row[4]) if row[4] is not None else None,
                "trial_strength_score": _finite(row[5]),
                "signal_type": row[6],
                "regime_at_signal": row[7],
                "market_cap_mm": mcap_mm,
                "primary_indication": row[9],
            }
        )
    for events in out.values():
        events.sort(key=lambda e: (e["days_out"] if e["days_out"] is not None else 10**6))
    return out


def _load_trial_tickers(engine: Engine, as_of: date) -> dict[str, dict[str, Any]]:
    """Tickers with a ``trial_signals`` row in the last 180 days (latest row each)."""
    start = _as_of_timestamp(as_of) - timedelta(days=TRIAL_LOOKBACK_DAYS)
    with engine.connect() as conn:
        rows = conn.execute(
            _TRIAL_TICKERS_SQL, {"start": start, "as_of_ts": _as_of_timestamp(as_of)}
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = _normalize_ticker(row[0])
        if ticker:
            out[ticker] = {
                "company_name": row[1],
                "primary_indication": row[2],
                "market_cap_mm": _finite(row[3]),
            }
    return out


def _load_options_asymmetry(engine: Engine, as_of: date) -> dict[str, dict[str, Any]]:
    """Best payoff-multiple scan per ticker in the last 30 days."""
    start = as_of - timedelta(days=OPTIONS_LOOKBACK_DAYS)
    with engine.connect() as conn:
        rows = conn.execute(_OPTIONS_SQL, {"start": start, "as_of": as_of}).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = _normalize_ticker(row[0])
        if not ticker:
            continue
        scan = row[1]
        out[ticker] = {
            "max_payoff_multiple": _finite(row[3]),
            "score": _finite(row[2]),
            "direction": row[4],
            "thesis": row[5],
            "is_100x": bool(row[6]) if row[6] is not None else None,
            "scan_date": scan.isoformat() if hasattr(scan, "isoformat") else scan,
        }
    return out


def _load_sweep(engine: Engine) -> dict[str, dict[str, Any]]:
    """Latest persisted 90 d sweep, keyed by ticker (``top_k`` entries only)."""
    from intelligence.universe_ranker import load_latest_ranking

    ranking = load_latest_ranking(engine, horizon_days=SWEEP_HORIZON_DAYS)
    if not ranking:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for entry in ranking.get("top_k") or []:
        ticker = _normalize_ticker(entry.get("ticker"))
        if not ticker:
            continue
        out[ticker] = {
            "verdict": entry.get("verdict"),
            "composite_score": _finite(entry.get("composite_score")),
            "horizon_days": ranking.get("horizon_days"),
            "generated_at": ranking.get("generated_at"),
        }
    return out


def _load_realized_alpha(engine: Engine) -> dict[str, Any]:
    """Engine track record from the latest ``realized_alpha_daily`` rows."""
    with engine.connect() as conn:
        rows = conn.execute(_REALIZED_ALPHA_SQL).fetchall()
    out: dict[str, Any] = {
        "oracle_predictions_60d_mean_alpha": None,
        "paper_trades_60d_mean_alpha": None,
        "as_of": None,
    }
    for row in rows:
        source = str(row[0])
        horizon = int(row[1]) if row[1] is not None else None
        if horizon != REALIZED_ALPHA_HORIZON_DAYS:
            continue
        key = f"{source}_60d_mean_alpha"
        if key in out:
            out[key] = _finite(row[2])
            out[f"{source}_60d_n_trades"] = int(row[3] or 0)
    return out


def _company_name(ticker: str) -> str | None:
    """Name from ``analysis.market_universe`` (exact ticker match)."""
    from analysis.market_universe import search_company

    for item in search_company(ticker):
        if _normalize_ticker(item.get("ticker")) == ticker:
            return item.get("name")
    return None


def _playbook_index() -> dict[str, list[dict[str, Any]]]:
    """Playbooks keyed by ticker (id, title, thesis_stub, horizon, category, base_edge)."""
    from intelligence.market_edge_scanner import PLAYBOOKS

    out: dict[str, list[dict[str, Any]]] = {}
    for playbook in PLAYBOOKS:
        for raw in playbook.target_pool:
            ticker = _normalize_ticker(raw)
            if not ticker:
                continue
            out.setdefault(ticker, []).append(
                {
                    "id": playbook.id,
                    "title": playbook.title,
                    "thesis_stub": playbook.thesis_stub,
                    "horizon": playbook.horizon,
                    "category": playbook.category,
                    "sector_focus": playbook.sector_focus,
                    "base_edge": float(playbook.base_edge),
                }
            )
    return out


# ── Board assembly ────────────────────────────────────────────────────────


def _projection_block(
    metrics: dict[str, Any], ticker: str, horizons_years: Sequence[int]
) -> dict[str, Any]:
    """Per-horizon p10/p50/p90 multiples from the seeded Monte Carlo proxy."""
    allocation = [
        {
            "target_weight": 1.0,
            "cagr": float(metrics.get("cagr", 0.0)),
            "annual_volatility": float(metrics.get("annual_volatility", 0.0)),
        }
    ]
    block: dict[str, Any] = {}
    for years in horizons_years:
        mc = build_monte_carlo_projection(
            allocation,
            capital=1.0,
            years=int(years),
            simulations=PROJECTION_SIMULATIONS,
            seed=f"long_plays:{ticker}",
        )
        block[f"{int(years)}y"] = {
            "p10_multiple": _finite(mc.get("p10")),
            "p50_multiple": _finite(mc.get("p50")),
            "p90_multiple": _finite(mc.get("p90")),
            "probability_above_start": _finite(mc.get("probability_above_start")),
        }
    block["method"] = PROJECTION_METHOD
    return block


def _chart_block(metrics: dict[str, Any]) -> dict[str, Any]:
    """Chart summary with the drawdown stored as a signed negative fraction."""
    drawdown = _finite(metrics.get("max_drawdown"))
    return {
        "years": _finite(metrics.get("years")),
        "cagr": _finite(metrics.get("cagr")),
        "annual_volatility": _finite(metrics.get("annual_volatility")),
        "max_drawdown": -abs(drawdown) if drawdown is not None else None,
        "r2": _finite(metrics.get("trend_r2")),
        "first_date": metrics.get("first_date"),
        "last_date": metrics.get("last_date"),
        "observations": metrics.get("observations"),
    }


def build_universe(
    engine: Engine,
    *,
    as_of: date,
    notes: list[str],
) -> tuple[list[str], dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    """Union of the thematic, playbook, trial and options universes (stocks only).

    Returns ``(tickers, trial_meta, catalysts, options)`` so the callers do
    not re-read the trial/catalyst/options tables.
    """
    playbooks = _safe("playbooks", _playbook_index, notes, {})
    trial_meta = _safe("trial_signals", lambda: _load_trial_tickers(engine, as_of), notes, {})
    catalysts = _safe("upcoming_catalysts", lambda: _load_catalysts(engine, as_of), notes, {})
    options = _safe("options_mispricing_scans", lambda: _load_options_asymmetry(engine, as_of), notes, {})

    pool: set[str] = set()
    pool.update(_normalize_ticker(t) or "" for t in FRONTIER_THEMATIC_UNIVERSE)
    pool.update(playbooks)
    pool.update(trial_meta)
    pool.update(catalysts)
    pool.update(options)
    pool.discard("")
    tickers = sorted(t for t in pool if is_common_stock_candidate(t))
    skipped = len(pool) - len(tickers)
    if skipped:
        notes.append(f"universe: skipped {skipped} ETF/macro/non-stock symbols (is_common_stock_candidate)")
    return tickers, trial_meta, catalysts, options


def _candidate(
    ticker: str,
    *,
    as_of: date,
    horizons_years: Sequence[int],
    points: list[tuple[date, float]] | None,
    market_cap: dict[str, Any] | None,
    trial_meta: dict[str, Any] | None,
    catalysts: list[dict[str, Any]],
    options: dict[str, Any] | None,
    sweep: dict[str, Any] | None,
    sweep_available: bool,
    playbooks: list[dict[str, Any]],
    realized_alpha: dict[str, Any] | None,
    notes: list[str],
) -> dict[str, Any]:
    """Assemble one JSON-safe candidate row."""
    themes: list[str] = list(FRONTIER_THEME_CANDIDATES.get(ticker, ()))
    thesis_sources: list[dict[str, Any]] = []
    if themes:
        thesis_sources.append({"kind": "frontier_theme", "id": ticker, "text": ", ".join(themes)})
    playbook_edge: float | None = None
    for pb in playbooks:
        thesis_sources.append(
            {
                "kind": "playbook",
                "id": pb["id"],
                "text": f"{pb['title']} — {pb['thesis_stub']} (horizon {pb['horizon']})",
            }
        )
        for theme in (pb.get("category"), pb.get("sector_focus")):
            if theme and theme not in themes:
                themes.append(str(theme))
        edge = _finite(pb.get("base_edge"))
        if edge is not None and (playbook_edge is None or edge > playbook_edge):
            playbook_edge = edge
    indication = (trial_meta or {}).get("primary_indication") or next(
        (c.get("primary_indication") for c in catalysts if c.get("primary_indication")), None
    )
    if indication and indication not in themes:
        themes.append(str(indication))
    for event in catalysts[:3]:
        thesis_sources.append(
            {
                "kind": "trial_catalyst",
                "id": f"{event.get('event_type')}@{event.get('expected_date')}",
                "text": (
                    f"{event.get('event_type')} expected {event.get('expected_date')}"
                    f" ({event.get('primary_indication') or 'indication unknown'};"
                    f" strength {event.get('trial_strength_score')})"
                ),
            }
        )
    if options:
        thesis_sources.append(
            {"kind": "options_asymmetry", "id": options.get("scan_date"), "text": options.get("thesis")}
        )

    name = _safe(f"name:{ticker}", lambda: _company_name(ticker), notes, None) or (
        (trial_meta or {}).get("company_name")
    )

    # Market cap: ticker_metrics_daily, then trial_signals.market_cap_mm.
    mcap_usd: float | None = None
    mcap_source: str | None = None
    if market_cap and market_cap.get("market_cap_usd"):
        mcap_usd = market_cap["market_cap_usd"]
        mcap_source = "ticker_metrics_daily"
    else:
        mm = (trial_meta or {}).get("market_cap_mm") or next(
            (c.get("market_cap_mm") for c in catalysts if c.get("market_cap_mm")), None
        )
        if mm:
            mcap_usd = float(mm) * 1e6
            mcap_source = "trial_signals.market_cap_mm"

    # Chart + projection (proxy).
    chart: dict[str, Any] | None = None
    projection: dict[str, Any] | None = None
    price: float | None = None
    price_as_of: str | None = None
    if points:
        metrics = compute_chart_metrics(ticker, points, None)
        if metrics is not None:
            chart = _chart_block(metrics)
            projection = _projection_block(metrics, ticker, horizons_years)
            price = _finite(metrics.get("latest_price"))
            price_as_of = metrics.get("last_date")
        else:
            last_date, last_value = points[-1]
            price = _finite(last_value)
            price_as_of = last_date.isoformat() if hasattr(last_date, "isoformat") else str(last_date)
            notes.append(f"{ticker}: fewer than 52 weekly points; chart/projection degraded to None")

    key_3y = f"{STANCE_HORIZON_YEARS}y"
    proj_3y = (projection or {}).get(key_3y) if projection else None
    p10_3y = (proj_3y or {}).get("p10_multiple")
    p50_3y = (proj_3y or {}).get("p50_multiple")
    p90_3y = (proj_3y or {}).get("p90_multiple")

    strength_12m: float | None = None
    strength_any: float | None = None
    for event in catalysts:
        strength = _finite(event.get("trial_strength_score"))
        if strength is None:
            continue
        strength_any = strength if strength_any is None else max(strength_any, strength)
        days_out = event.get("days_out")
        if days_out is not None and days_out <= CATALYST_OVERRIDE_DAYS:
            strength_12m = strength if strength_12m is None else max(strength_12m, strength)

    payoff = (options or {}).get("max_payoff_multiple") if options else None
    stance = classify_stance(
        p50_3y_multiple=p50_3y,
        p10_3y_multiple=p10_3y,
        max_drawdown=(chart or {}).get("max_drawdown") if chart else None,
        sweep_verdict=(sweep or {}).get("verdict") if sweep else None,
        has_sweep_coverage=sweep is not None,
        catalyst_strength_12m=strength_12m,
        options_payoff_multiple=payoff,
        has_catalyst=bool(catalysts),
    )
    score = asymmetry_score(
        p90_3y_multiple=p90_3y,
        catalyst_strength=strength_any,
        payoff_multiple=payoff,
        playbook_edge=playbook_edge,
    )
    math_block = multiple_math(
        p50_3y_multiple=p50_3y, p90_3y_multiple=p90_3y, market_cap_usd=mcap_usd
    )
    sweep_note = None if sweep_available else "no persisted 90 d sweep; coverage gate falls back to options payoff"
    return {
        "ticker": ticker,
        "name": name,
        "themes": themes,
        "thesis_sources": thesis_sources,
        "market_cap_usd": mcap_usd,
        "market_cap_bucket": market_cap_bucket(mcap_usd),
        "market_cap_source": mcap_source,
        "price": price,
        "price_as_of": price_as_of,
        "chart": chart,
        "projection": projection,
        "multiple_math": math_block,
        "catalysts": catalysts,
        "options_asymmetry": options,
        "sweep": sweep,
        "sweep_note": sweep_note,
        "playbook_edge": playbook_edge,
        "realized_alpha_context": realized_alpha,
        "asymmetry_score": score,
        "stance": stance,
        "why": _why(
            themes=themes,
            thesis_sources=thesis_sources,
            projection_3y=proj_3y,
            sweep=sweep,
            options=options,
            catalysts=catalysts,
        ),
        "what_must_be_true_for_10x": _what_must_be_true(math_block),
    }


def build_long_plays_board(
    engine: Engine,
    *,
    as_of: date | None = None,
    horizons_years: Sequence[int] = DEFAULT_HORIZONS_YEARS,
    top_k: int = DEFAULT_TOP_K,
) -> dict[str, Any]:
    """Compose the multi-year board (shape documented in the module docstring).

    Sorted by ``asymmetry_score`` descending, truncated to ``top_k``.
    ``stand_down_reason`` is set when no candidate reaches
    ``entry_candidate``. Every source read is wrapped so a failing table
    degrades its field to ``None`` and adds a ``method_notes`` line.
    """
    as_of = as_of or date.today()
    horizons = tuple(int(h) for h in horizons_years if int(h) > 0) or DEFAULT_HORIZONS_YEARS
    if STANCE_HORIZON_YEARS not in horizons:
        horizons = tuple(sorted({*horizons, STANCE_HORIZON_YEARS}))
    top_k = max(1, int(top_k))
    notes: list[str] = [
        f"projection: {PROJECTION_METHOD}; {PROJECTION_SIMULATIONS} paths per horizon; multiples of 1.0 invested",
        "multiple_math: arithmetic on the proxy projection and latest market cap; not a forecast",
        f"prices: PIT cut-off as_of={as_of.isoformat()} on every read (resolved_series via PITStore, raw_series via pull_timestamp)",
        "sweep coverage: only the persisted top_k of the latest 90 d sweep is visible; names outside it count as uncovered",
    ]

    tickers, trial_meta, catalysts, options = build_universe(engine, as_of=as_of, notes=notes)
    playbooks = _safe("playbooks", _playbook_index, notes, {})
    prices = _safe(
        "price_history",
        lambda: _load_adj_close(engine, tickers, CHART_YEARS, as_of),
        notes,
        {},
    )
    market_caps = _safe("ticker_metrics_daily", lambda: _load_market_caps(engine, tickers, as_of), notes, {})
    sweep_by_ticker = _safe("universe_ranking_history", lambda: _load_sweep(engine), notes, None)
    sweep_available = bool(sweep_by_ticker)
    if sweep_by_ticker is None:
        sweep_by_ticker = {}
    elif not sweep_by_ticker:
        notes.append("universe_ranking_history: no persisted 90 d sweep; coverage gate falls back to options payoff")
    realized_alpha = _safe("realized_alpha_daily", lambda: _load_realized_alpha(engine), notes, None)

    candidates: list[dict[str, Any]] = []
    for ticker in tickers:
        try:
            candidates.append(
                _candidate(
                    ticker,
                    as_of=as_of,
                    horizons_years=horizons,
                    points=prices.get(ticker),
                    market_cap=market_caps.get(ticker),
                    trial_meta=trial_meta.get(ticker),
                    catalysts=list(catalysts.get(ticker, [])),
                    options=options.get(ticker),
                    sweep=sweep_by_ticker.get(ticker),
                    sweep_available=sweep_available,
                    playbooks=list(playbooks.get(ticker, [])),
                    realized_alpha=realized_alpha,
                    notes=notes,
                )
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("long_plays: candidate {t} skipped: {e}", t=ticker, e=str(exc))
            notes.append(f"{ticker}: candidate assembly failed ({type(exc).__name__}); skipped")

    candidates.sort(key=lambda c: (-float(c.get("asymmetry_score") or 0.0), c["ticker"]))
    board_candidates = candidates[:top_k]
    n_entry = sum(1 for c in board_candidates if c["stance"] == STANCE_ENTRY)
    stand_down_reason: str | None = None
    if n_entry == 0:
        if not board_candidates:
            stand_down_reason = "no candidates: universe empty or every source unavailable"
        else:
            stand_down_reason = (
                f"no entry candidates among {len(board_candidates)}: none clears p50 3y multiple > "
                f"{ENTRY_P50_3Y_MULTIPLE} (or a >= {CATALYST_OVERRIDE_STRENGTH} catalyst inside 12 months), "
                f"drawdown shallower than {ENTRY_MAX_DRAWDOWN:.0%}, and sweep high/moderate "
                f"(or uncovered with options payoff >= {ENTRY_OPTIONS_PAYOFF_MULTIPLE:.0f}x) together"
            )

    return {
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "horizons_years": list(horizons),
        "universe_size": len(tickers),
        "candidates": board_candidates,
        "entry_candidates": n_entry,
        "stand_down_reason": stand_down_reason,
        "method_notes": notes,
    }


# ── Persistence ───────────────────────────────────────────────────────────

_ENSURE_TABLE_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS long_plays_board (
        id                BIGSERIAL PRIMARY KEY,
        as_of             DATE NOT NULL,
        generated_at      TIMESTAMPTZ NOT NULL,
        universe_size     INTEGER NOT NULL DEFAULT 0,
        candidates        JSONB NOT NULL,
        stand_down_reason TEXT,
        method_notes      JSONB NOT NULL DEFAULT '[]'::jsonb
    )
    """
)

_ENSURE_INDEX_SQL = text(
    """
    CREATE INDEX IF NOT EXISTS idx_long_plays_board_generated_at
        ON long_plays_board (generated_at DESC)
    """
)

_INSERT_BOARD_SQL = text(
    """
    INSERT INTO long_plays_board (
        as_of, generated_at, universe_size, candidates, stand_down_reason, method_notes
    ) VALUES (
        :as_of, :generated_at, :universe_size, :candidates, :stand_down_reason, :method_notes
    )
    RETURNING id
    """
)

_LATEST_BOARD_SQL = text(
    """
    SELECT id, as_of, generated_at, universe_size, candidates, stand_down_reason, method_notes
    FROM long_plays_board
    ORDER BY generated_at DESC, id DESC
    LIMIT 1
    """
)


def ensure_long_plays_table(engine: Engine) -> None:
    """Create ``long_plays_board`` if missing. Idempotent; never raises."""
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_TABLE_SQL)
            conn.execute(_ENSURE_INDEX_SQL)
    except Exception as exc:  # noqa: BLE001
        log.debug("long_plays: ensure_long_plays_table failed: {e}", e=exc)


def _parse_iso_datetime(value: Any) -> datetime:
    """ISO string → aware datetime (UTC when naive); ``datetime`` passes through."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def persist_board(engine: Engine, board: dict[str, Any]) -> int:
    """Insert one board row. Returns the new id, or ``-1`` on any failure."""
    ensure_long_plays_table(engine)
    try:
        as_of_raw = board.get("as_of") or date.today().isoformat()
        as_of = as_of_raw if isinstance(as_of_raw, date) else date.fromisoformat(str(as_of_raw))
        params = {
            "as_of": as_of,
            "generated_at": _parse_iso_datetime(board.get("generated_at") or datetime.now(timezone.utc)),
            "universe_size": int(board.get("universe_size") or 0),
            "candidates": json.dumps(board.get("candidates") or [], default=str),
            "stand_down_reason": board.get("stand_down_reason"),
            "method_notes": json.dumps(board.get("method_notes") or [], default=str),
        }
        with engine.begin() as conn:
            row = conn.execute(_INSERT_BOARD_SQL, params).first()
        return int(row[0]) if row is not None else -1
    except Exception as exc:  # noqa: BLE001
        log.warning("long_plays: persist_board failed: {e}", e=str(exc))
        return -1


def _board_row_to_dict(row: Any) -> dict[str, Any]:
    """Row tuple → JSON-safe board dict (JSONB accepted parsed or as text)."""

    def _j(value: Any, default: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, (list, dict)):
            return value
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return default

    as_of = row[1]
    generated_at = row[2]
    candidates = _j(row[4], [])
    return {
        "id": int(row[0]),
        "as_of": as_of.isoformat() if hasattr(as_of, "isoformat") else as_of,
        "generated_at": generated_at.isoformat() if hasattr(generated_at, "isoformat") else generated_at,
        "universe_size": int(row[3] or 0),
        "candidates": candidates,
        "entry_candidates": sum(1 for c in candidates if isinstance(c, dict) and c.get("stance") == STANCE_ENTRY),
        "stand_down_reason": row[5],
        "method_notes": _j(row[6], []),
    }


def load_latest_board(engine: Engine) -> dict[str, Any] | None:
    """Most recent persisted board, or ``None`` when empty / table missing."""
    try:
        with engine.connect() as conn:
            row = conn.execute(_LATEST_BOARD_SQL).first()
    except Exception as exc:  # noqa: BLE001
        log.debug("long_plays: load_latest_board failed: {e}", e=exc)
        return None
    return _board_row_to_dict(row) if row is not None else None
