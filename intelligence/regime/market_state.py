"""Daily market-state record: named state variables, each read against its own trailing baseline.

Anik, 2026-11-13: the market is conditional ("we need to know the regime and state of the market, if there was news
or no news ... its fluid, not black and white"), and every state variable "needs a baseline to go up or down from".
This record is the shared, machine-readable answer: one row per day, every variable reported as a value, its
trailing median, a robust z, a percentile and a label (``intelligence/regime/baseline.py``, rule trailing-robust/1).

It sits beside ``intelligence/regime/state_vector.py`` rather than changing it: the analog engine and the regime
classifier depend on that module's 24 dimensions and its mean/std normalisation. The SPY price read is shared
(``state_vector._fetch_spy_prices``); every other input is read here with its own declared publication lag.

Availability contract (``as_of`` = end of that UTC day), same as state_vector:

* Every series is read through ``store.observations.read_window_known_at``: an observation counts only if it was
  pulled by ``as_of`` or, for backfilled history, its :data:`LAGS` entry puts its release on or before ``as_of``.
  Lags are deliberately late. Backfilled history is revised vintage, not first release.
* A variable's baseline is its own previous observations (strictly before the one read), so a value never
  standardizes against itself or against anything later.
* A variable without enough known history is reported ``unavailable`` with the reason, never guessed.

Scheduled economic events come from ``config/econ_release_dates.json`` (official release dates: BLS / BEA / Fed;
regenerate with ``scripts/build_econ_release_dates.py``). A release date is public long before the release, so the
calendar has no availability lag. Unscheduled news and release surprises are not in record v1 (their history sources
are listed in :data:`PENDING`), and the record says so instead of filling them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.regime.baseline import BASELINE_RULE, standardize_last, window_for
from store.observations import MixedSourceError, PublicationLag, read_window_known_at

RECORD_RULE = "market-state/1"

# ── Publication lags (modeled availability for backfilled history; late on purpose) ──
_NEXT_BD = PublicationLag(1, "business", "posted the next business day (FRED daily / Cboe close / FINRA evening file)")
_EMPSIT = PublicationLag(
    42, "calendar",
    "BLS Employment Situation: conservative proxy; documented shutdown releases override it",
    release_overrides=(
        (date(2013, 9, 1), date(2013, 10, 22)),
        (date(2013, 10, 1), date(2013, 11, 8)),
        (date(2025, 9, 1), date(2025, 11, 20)),
        (date(2025, 10, 1), date(2025, 12, 16)),
        (date(2025, 11, 1), date(2025, 12, 16)),
    ),
)
_UNRATE = PublicationLag(
    _EMPSIT.days, _EMPSIT.unit, _EMPSIT.basis + "; October 2025 household survey not collected",
    release_overrides=tuple(o for o in _EMPSIT.release_overrides if o[0] != date(2025, 10, 1)),
    unpublished_dates=frozenset({date(2025, 10, 1)}),
)
LAGS: dict[str, PublicationLag | None] = {
    "CBOE:VIX": _NEXT_BD,
    "CBOE:VIX3M": _NEXT_BD,
    "BAMLH0A0HYM2": _NEXT_BD,
    "BAMLC0A0CM": _NEXT_BD,
    "T10Y2Y": _NEXT_BD,
    "DFF": _NEXT_BD,
    "T5YIE": _NEXT_BD,
    "RRPONTSYD": _NEXT_BD,
    "WALCL": PublicationLag(3, "calendar", "Fed H.4.1: Wednesday level released Thursday 16:30 ET"),
    "WTREGEN": PublicationLag(3, "calendar", "Fed H.4.1: Wednesday level released Thursday 16:30 ET"),
    "ICSA": PublicationLag(6, "calendar", "DOL weekly claims, the following Thursday (Friday after a holiday)"),
    "UNRATE": _UNRATE,
    "PAYEMS": _EMPSIT,
    "CPIAUCSL": PublicationLag(
        50, "calendar", "BLS CPI, mid next month; shutdown releases override it",
        release_overrides=((date(2013, 9, 1), date(2013, 10, 30)), (date(2025, 9, 1), date(2025, 10, 24))),
        unpublished_dates=frozenset({date(2025, 10, 1)}),
    ),
    "INDPRO": PublicationLag(50, "calendar", "Fed G.17, ~15th-18th of the next month"),
    "UMCSENT": PublicationLag(60, "calendar", "UMich sentiment as posted to FRED"),
    "finra:short_volume_agg:market_ratio": _NEXT_BD,
}
SOURCES: dict[str, str] = {"CBOE:VIX": "CBOE", "CBOE:VIX3M": "CBOE"}

# Inputs a later record version adds; listed in every record so a reader sees what is not there yet.
PENDING: dict[str, str] = {
    "news_intensity": "needs a news-volume source with history before 2026-03 (GDELT DOC timeline volume)",
    "unscheduled_news_shock": "news_intensity on a day with no scheduled event",
    "release_surprise": "actual minus consensus per scheduled release; GRID has no consensus source yet",
    "earnings_surprise": "mega-cap EPS actual vs estimate (FMP), read point in time",
    "dix": "dollar-weighted S&P 500 dark-pool short ratio (FINRA files back to 2018-08; edge search 11 rebuild)",
    "dealer_gamma": "end-of-day net GEX and distance to the gamma flip; dealer_gex_daily has no writer",
}


@dataclass(frozen=True)
class VarSpec:
    name: str
    group: str
    cadence: str                       # baseline window class (intelligence.regime.baseline.WINDOWS)
    build: Callable[["_Reads"], pd.Series]
    unit: str
    note: str = ""


class _Reads:
    """Known-at reads for one ``as_of``, memoized per series."""

    def __init__(self, engine: Engine, as_of: date, lookback_days: int) -> None:
        self.engine, self.as_of, self.lookback_days = engine, as_of, lookback_days
        self._memo: dict[str, pd.Series] = {}
        self.spy_basis: str | None = None

    def series(self, series_id: str) -> pd.Series:
        if series_id not in self._memo:
            start = self.as_of - timedelta(days=self.lookback_days)
            try:
                with self.engine.connect() as conn:
                    obs = read_window_known_at(conn, series_id, as_of=self.as_of, lag=LAGS.get(series_id),
                                               source=SOURCES.get(series_id), start=start)
            except MixedSourceError as exc:
                log.warning("market_state: {s} is mixed-source, skipped: {e}", s=series_id, e=str(exc))
                obs = []
            s = pd.Series({o.obs_date: o.value for o in obs if o.value is not None}, dtype=float).sort_index()
            self._memo[series_id] = s[np.isfinite(s.to_numpy())] if len(s) else s
        return self._memo[series_id]

    def spy(self) -> pd.Series:
        """SPY closes known at ``as_of``, one basis per record.

        The resolved ``spy_full`` feature (state_vector's preferred basis) when it covers the trend window
        (:data:`SPY_NEED_OBS` closes); otherwise whichever of it and the raw ``YF:SPY:close`` (state_vector's
        known-at fallback, ``SPY_CLOSE_LAG``) has more history. state_vector takes ``spy_full`` whenever it is
        non-empty, which left the 200-day distance short of history in mid-2022 (107 closes).
        """
        if "__spy__" not in self._memo:
            from intelligence.regime.state_vector import SPY_CLOSE_LAG, _fetch_resolved_spy_full

            start = self.as_of - timedelta(days=self.lookback_days)
            resolved = _fetch_resolved_spy_full(self.engine, self.as_of, start)
            resolved = resolved if resolved is not None else pd.Series(dtype=float)
            prices, basis = resolved, ("spy_full" if len(resolved) else None)
            if len(resolved) < SPY_NEED_OBS:
                try:
                    with self.engine.connect() as conn:
                        obs = read_window_known_at(conn, "YF:SPY:close", as_of=self.as_of, lag=SPY_CLOSE_LAG,
                                                   source="yfinance", start=start)
                    raw = pd.Series({o.obs_date: o.value for o in obs if o.value is not None}, dtype=float).sort_index()
                except Exception as exc:
                    log.debug("market_state: raw SPY read failed: {e}", e=str(exc))
                    raw = pd.Series(dtype=float)
                if len(raw) > len(resolved):
                    prices, basis = raw, "YF:SPY:close"
            self._memo["__spy__"], self.spy_basis = prices, basis
        return self._memo["__spy__"]


# ── Transforms (each returns the variable's whole known history, so the baseline reads from it) ──

def _level(series_id: str) -> Callable[[_Reads], pd.Series]:
    return lambda r: r.series(series_id)


def _ratio(a: str, b: str) -> Callable[[_Reads], pd.Series]:
    def f(r: _Reads) -> pd.Series:
        x, y = r.series(a), r.series(b)
        both = pd.concat([x, y], axis=1, join="inner").dropna()
        both = both[both.iloc[:, 1] > 0]
        return (both.iloc[:, 0] / both.iloc[:, 1]).astype(float)
    return f


def _yoy(series_id: str, periods: int = 12) -> Callable[[_Reads], pd.Series]:
    def f(r: _Reads) -> pd.Series:
        s = r.series(series_id)
        return (s / s.shift(periods) - 1.0).dropna()
    return f


def _avg_change(series_id: str, periods: int) -> Callable[[_Reads], pd.Series]:
    def f(r: _Reads) -> pd.Series:
        s = r.series(series_id)
        return (s.diff().rolling(periods).mean()).dropna()
    return f


def _spread(a: str, b: str) -> Callable[[_Reads], pd.Series]:
    def f(r: _Reads) -> pd.Series:
        both = pd.concat([r.series(a), r.series(b)], axis=1, join="inner").dropna()
        return (both.iloc[:, 0] - both.iloc[:, 1]).astype(float)
    return f


def _spy_ma_distance(n: int) -> Callable[[_Reads], pd.Series]:
    def f(r: _Reads) -> pd.Series:
        p = r.spy()
        return (p / p.rolling(n).mean() - 1.0).dropna()
    return f


def net_liquidity(walcl: pd.Series, tga: pd.Series, rrp: pd.Series) -> pd.Series:
    """WALCL - WTREGEN - RRPONTSYD*1000 ($M), on WALCL dates, each part as of that date (latest at or before it)."""
    if walcl.empty or tga.empty or rrp.empty:
        return pd.Series(dtype=float)
    idx = pd.DatetimeIndex(pd.to_datetime(list(walcl.index)))
    t = pd.Series(tga.to_numpy(), index=pd.to_datetime(list(tga.index))).sort_index()
    p = pd.Series(rrp.to_numpy(), index=pd.to_datetime(list(rrp.index))).sort_index()
    t_at = t.reindex(t.index.union(idx)).ffill().reindex(idx)
    p_at = p.reindex(p.index.union(idx)).ffill().reindex(idx)
    out = pd.Series(walcl.to_numpy(), index=idx) - t_at - p_at * 1000.0
    out.index = [d.date() for d in out.index]
    return out.dropna()


def _net_liq(r: _Reads) -> pd.Series:
    return net_liquidity(r.series("WALCL"), r.series("WTREGEN"), r.series("RRPONTSYD"))


def _net_liq_change_4w(r: _Reads) -> pd.Series:
    return _net_liq(r).diff(4).dropna()


VARIABLES: list[VarSpec] = [
    VarSpec("vix", "volatility", "daily_fast", _level("CBOE:VIX"), "index points"),
    VarSpec("vix_term", "volatility", "daily_fast", _ratio("CBOE:VIX", "CBOE:VIX3M"), "VIX / VIX3M",
            "above 1 = near-term fear priced above 3-month"),
    VarSpec("hy_spread", "credit", "daily_fast", _level("BAMLH0A0HYM2"), "% (ICE BofA HY OAS)"),
    VarSpec("ig_spread", "credit", "daily_fast", _level("BAMLC0A0CM"), "% (ICE BofA IG OAS)"),
    VarSpec("yield_curve", "rates", "daily_slow", _level("T10Y2Y"), "% (10y - 2y)"),
    VarSpec("fed_funds", "rates", "daily_slow", _level("DFF"), "%"),
    VarSpec("real_fed_funds", "rates", "daily_slow", _spread("DFF", "T5YIE"), "% (DFF - 5y breakeven)"),
    VarSpec("net_liquidity", "liquidity", "weekly", _net_liq, "$M (Fed assets - TGA - RRP)"),
    VarSpec("net_liquidity_4w", "liquidity", "weekly", _net_liq_change_4w, "$M change over 4 weeks"),
    VarSpec("initial_claims", "labor", "weekly", _level("ICSA"), "claims"),
    VarSpec("unemployment", "labor", "monthly", _level("UNRATE"), "%"),
    VarSpec("payrolls_3m", "labor", "monthly", _avg_change("PAYEMS", 3), "thousand jobs/month, 3-month average"),
    VarSpec("cpi_yoy", "inflation", "monthly", _yoy("CPIAUCSL"), "year-over-year"),
    VarSpec("industrial_production_yoy", "growth", "monthly", _yoy("INDPRO"), "year-over-year"),
    VarSpec("consumer_sentiment", "growth", "monthly", _level("UMCSENT"), "index"),
    VarSpec("spy_vs_50d", "trend", "daily_fast", _spy_ma_distance(50), "SPY / 50-day average - 1"),
    VarSpec("spy_vs_200d", "trend", "daily_fast", _spy_ma_distance(200), "SPY / 200-day average - 1"),
    VarSpec("short_volume_ratio", "flow", "daily_fast", _level("finra:short_volume_agg:market_ratio"),
            "short / total off-exchange volume, all symbols", "FINRA aggregate; GRID history from 2025-11"),
]
VAR_NAMES = [v.name for v in VARIABLES]

# Five years of the slowest cadence plus transform warm-up (12-month change, 200-day average).
LOOKBACK_DAYS = 365 * 7
SPY_NEED_OBS = 252 + 200 + 1   # one trading year of 200-day distances


# ── Scheduled events ──────────────────────────────────────────────────────

CALENDAR_PATH = Path(__file__).resolve().parents[2] / "config" / "econ_release_dates.json"


def load_calendar(path: Path = CALENDAR_PATH) -> dict[str, list[date]]:
    """{event: sorted release dates} from the generated calendar file; {} when the file is missing."""
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    return {k: sorted(date.fromisoformat(x) for x in v) for k, v in (raw.get("events") or {}).items()}


def events_state(as_of: date, calendar: dict[str, list[date]], horizon_days: int = 14) -> dict[str, Any]:
    """Scheduled releases on ``as_of``, the next one after it, and every release within ``horizon_days``."""
    if not calendar:
        return {"status": "unavailable", "reason": "config/econ_release_dates.json missing"}
    today = sorted(k for k, ds in calendar.items() if as_of in ds)
    upcoming: list[tuple[date, str]] = []
    for k, ds in calendar.items():
        upcoming.extend((d, k) for d in ds if as_of < d <= as_of + timedelta(days=horizon_days))
    upcoming.sort()
    nxt = None
    for k, ds in calendar.items():
        later = [d for d in ds if d > as_of]
        if later and (nxt is None or later[0] < nxt[0]):
            nxt = (later[0], k)
    covered_to = max((ds[-1] for ds in calendar.values() if ds), default=None)
    return {
        "status": "ok" if covered_to and covered_to >= as_of else "stale_calendar",
        "today": today,
        "scheduled_day": bool(today),
        "next": {"event": nxt[1], "date": nxt[0].isoformat(), "days_away": (nxt[0] - as_of).days} if nxt else None,
        "within_14d": [{"event": k, "date": d.isoformat()} for d, k in upcoming],
        "calendar_covers_to": covered_to.isoformat() if covered_to else None,
    }


# ── The record ────────────────────────────────────────────────────────────

@dataclass
class MarketState:
    as_of: date
    variables: dict[str, dict[str, Any]] = field(default_factory=dict)
    events: dict[str, Any] = field(default_factory=dict)
    spy_basis: str | None = None

    def to_dict(self) -> dict[str, Any]:
        ok = [k for k, v in self.variables.items() if v.get("status") == "ok"]
        return {
            "rule": RECORD_RULE,
            "baseline_rule": BASELINE_RULE,
            "as_of": self.as_of.isoformat(),
            "availability": "known by the end of as_of (UTC); backfilled history is revised vintage",
            "variables": self.variables,
            "events": self.events,
            "pending": PENDING,
            "coverage": {"ok": len(ok), "total": len(self.variables)},
            "spy_basis": self.spy_basis,
        }


def compute_market_state(engine: Engine, as_of: date, *, calendar: dict[str, list[date]] | None = None) -> MarketState:
    """Every variable in :data:`VARIABLES` at ``as_of``, standardized against its own trailing baseline."""
    reads = _Reads(engine, as_of, LOOKBACK_DAYS)
    state = MarketState(as_of=as_of)
    for spec in VARIABLES:
        window, min_obs = window_for(spec.cadence)
        base = {"group": spec.group, "unit": spec.unit, "cadence": spec.cadence}
        if spec.note:
            base["note"] = spec.note
        try:
            hist = spec.build(reads)
        except Exception as exc:  # one bad input never sinks the record
            log.warning("market_state: {v} failed at {d}: {e}", v=spec.name, d=as_of, e=str(exc))
            state.variables[spec.name] = {**base, "status": "unavailable", "reason": f"error: {type(exc).__name__}"}
            continue
        if hist.empty:
            state.variables[spec.name] = {**base, "status": "unavailable", "reason": "no observation known at as_of"}
            continue
        reading = standardize_last(hist.to_numpy(dtype=float), window=window, min_obs=min_obs)
        last_obs = hist.index[-1]
        if reading is None:
            state.variables[spec.name] = {**base, "status": "unavailable", "obs_date": str(last_obs),
                                          "reason": f"{len(hist) - 1} prior observations known; baseline needs {min_obs}"}
            continue
        state.variables[spec.name] = {**base, "status": "ok", "obs_date": str(last_obs),
                                      "age_days": (as_of - last_obs).days if isinstance(last_obs, date) else None,
                                      **reading.to_dict()}
    state.spy_basis = reads.spy_basis
    state.events = events_state(as_of, load_calendar() if calendar is None else calendar)
    return state


# ── Storage (insert-only per as_of; a recompute needs --replace) ──────────

_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS market_state_daily ("
    " as_of_date DATE PRIMARY KEY,"
    " rule TEXT NOT NULL,"
    " record JSONB NOT NULL,"
    " computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
)


def ensure_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_TABLE_SQL))


# Insert-only by default. CAST(... AS JSONB) only on Postgres: SQLite (tests) would coerce the JSON text to a number.
_INSERT_SQL = {
    ("postgresql", False): text("INSERT INTO market_state_daily (as_of_date, rule, record) "
                                "VALUES (:d, :r, CAST(:rec AS JSONB)) ON CONFLICT (as_of_date) DO NOTHING"),
    ("postgresql", True): text("INSERT INTO market_state_daily (as_of_date, rule, record) "
                               "VALUES (:d, :r, CAST(:rec AS JSONB)) ON CONFLICT (as_of_date) DO UPDATE SET "
                               "rule = EXCLUDED.rule, record = EXCLUDED.record, computed_at = CURRENT_TIMESTAMP"),
    ("other", False): text("INSERT INTO market_state_daily (as_of_date, rule, record) "
                           "VALUES (:d, :r, :rec) ON CONFLICT (as_of_date) DO NOTHING"),
    ("other", True): text("INSERT INTO market_state_daily (as_of_date, rule, record) "
                          "VALUES (:d, :r, :rec) ON CONFLICT (as_of_date) DO UPDATE SET "
                          "rule = EXCLUDED.rule, record = EXCLUDED.record, computed_at = CURRENT_TIMESTAMP"),
}


def store_market_state(engine: Engine, state: MarketState, *, replace: bool = False) -> bool:
    """Write one record. Returns False when a row for that date exists and ``replace`` is False."""
    ensure_table(engine)
    with engine.begin() as conn:
        dialect = "postgresql" if conn.dialect.name == "postgresql" else "other"
        res = conn.execute(_INSERT_SQL[(dialect, bool(replace))],
                           {"d": state.as_of, "r": RECORD_RULE, "rec": json.dumps(state.to_dict())})
    return bool(res.rowcount)
