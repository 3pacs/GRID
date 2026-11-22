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
calendar has no availability lag.

News comes from the daily newspaper-based uncertainty indexes (USEPUINDXD, WLEMUINDXD), read from ALFRED vintages
(``store/fred_vintages.py``) as FRED showed them on each day, because they are revised for weeks after first print.
``news_shock`` reports elevated uncertainty and calendar co-occurrence without attributing a shock.

DIX is GRID's ``finra:dix_sp500`` series (``ingestion/altdata/finra_dix.py``). Dealer gamma comes from
``dealer_gex_daily`` (``physics/dealer_gex_daily.py``), whose SPY history starts with registered option captures on
2026-09-26; until it has a baseline's worth of sessions it is reported ``no_baseline`` with its value, and
``gamma_state`` gives the regime and flip. Release and earnings surprises are listed in :data:`PENDING`.

The policy path (market-state/3) is the expected upper bound of the fed funds target after the next FOMC meeting,
priced from every strike of the Kalshi KXFED ladder (``intelligence/regime/policy_path.py``; known by pull clock only),
with its 5-session change inside one meeting's contract and the move it prices against FRED's DFEDTARU.
``policy_response`` sets VIX stress against how far the front end (2-year yield, that path) eased over the same five
sessions: Neon's PRFD "is the Fed put alive?" composite with only the legs Codex's triage admitted. It is a research
reading tracked for #1311, ``unknown`` whenever a leg is not ``ok``, and nothing reads it to decide.

A provisional record computed at the session's close (#1312) is display only and has its own table and store
functions at the end of this module; :func:`store_market_state` refuses one. It is computed with ``known_by`` (the
session's close): every read is cut at that instant instead of the end of ``as_of``. Without ``known_by`` nothing
changes.
"""

from __future__ import annotations

import hashlib
import json
from bisect import bisect_right
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.regime import policy_path as PP
from intelligence.regime.baseline import BASELINE_RULE, standardize_last, window_for
from store.observations import MixedSourceError, PublicationLag, read_window_known_at

# market-state/2 = market-state/1 plus freshness (#1303): an input older than its maximum age reads ``stale``, not ok.
# market-state/3 = market-state/2 plus the policy-path variables (2-year yield change, the Kalshi KXFED path at the
# next FOMC meeting and its same-contract change, the priced move against DFEDTARU) and the ``policy_response`` block
# (Neon's PRFD "is the Fed put alive?" composite, VIX + front-end legs only, research tracking, #1311).
RECORD_RULE = "market-state/3"
FRESHNESS_RULE = "max-age/1"

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
    "finra:dix_sp500": _NEXT_BD,
    "DGS2": _NEXT_BD,
    "DFEDTARU": _NEXT_BD,
}
SOURCES: dict[str, str] = {"CBOE:VIX": "CBOE", "CBOE:VIX3M": "CBOE"}

# Inputs a later record version adds; listed in every record so a reader sees what is not there yet.
PENDING: dict[str, str] = {
    "release_surprise": "actual minus consensus per scheduled release; GRID has no consensus source yet",
    "earnings_surprise": "mega-cap EPS actual vs estimate (FMP), read point in time",
}


@dataclass(frozen=True)
class VarSpec:
    name: str
    group: str
    cadence: str                       # baseline window class (intelligence.regime.baseline.WINDOWS)
    build: Callable[["_Reads"], pd.Series]
    unit: str
    note: str = ""


def raw_selection_receipt(observations: list, selected: pd.Series, *, series_id: str,
                          as_of: date, start: date, status: str = "read",
                          as_of_ts: datetime | None = None) -> dict[str, Any]:
    """Hash exactly the finite, date-collapsed raw inputs consumed, including their clocks.

    This is a selection fingerprint, not immutable byte custody or PIT qualification.
    No arbitrary raw_payload is copied. Modeled publication dates remain modeled.
    ``as_of_ts`` (the provisional record's cutoff, #1312) is recorded only when the read was cut at an instant.
    """
    by_date = {o.obs_date: o for o in observations if o.value is not None}
    rows = []
    for d in selected.index:
        o = by_date[d]
        rows.append({"series_id": o.series_id, "obs_date": o.obs_date.isoformat(),
                     "value": float(selected.loc[d]), "source": o.source,
                     "pull_timestamp": o.pull_timestamp.isoformat() if o.pull_timestamp else None,
                     "known_at": o.known_at.isoformat() if o.known_at else None,
                     "known_at_basis": o.known_at_basis})
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    bases = Counter(row["known_at_basis"] or "unknown" for row in rows)
    cut = {"as_of_ts": as_of_ts.isoformat()} if as_of_ts is not None else {}
    return {"schema": "market-state-raw-selection/1", "status": status,
            "series_id": series_id, "as_of": as_of.isoformat(), **cut, "start": start.isoformat(),
            "source_filter": SOURCES.get(series_id),
            "publication_policy": {**asdict(LAGS[series_id]),
                "release_overrides": [[a.isoformat(), b.isoformat()] for a, b in LAGS[series_id].release_overrides],
                "unpublished_dates": sorted(d.isoformat() for d in LAGS[series_id].unpublished_dates)}
                if series_id in LAGS else None,
            "rows": len(rows), "selected_rows_sha256": hashlib.sha256(encoded).hexdigest(),
            "row_hash_fields": ["series_id", "obs_date", "value", "source", "pull_timestamp",
                                "known_at", "known_at_basis"],
            "first_obs_date": rows[0]["obs_date"] if rows else None,
            "latest_observation": rows[-1] if rows else None,
            "known_at_basis_counts": dict(sorted(bases.items())),
            "missing_pull_timestamps": sum(row["pull_timestamp"] is None for row in rows),
            "source_names": sorted({row["source"] for row in rows if row["source"]}),
            "pit_qualified": False,
            "limitation": "fingerprint only; modeled lag cannot reconstruct revised vintages"}


class _Reads:
    """Known-at reads for one ``as_of``, memoized per series.

    ``known_by`` (the provisional record's availability cutoff, #1312) cuts every read at that instant, each on the
    clock its store keeps: raw_series ``pull_timestamp`` (no modeled path), fred_vintages ``fetched_at``
    (intervals rebuilt from the versions known then, since ``realtime_end`` is extended in place) and
    dealer_gex_daily ``created_at``. Resolved ``spy_full`` is not read, because ``resolved_series`` has no write
    clock; SPY comes from ``YF:SPY:close``. Without ``known_by`` every read is the decision record's own
    end-of-``as_of`` read, unchanged.
    """

    def __init__(self, engine: Engine, as_of: date, lookback_days: int, known_by: datetime | None = None) -> None:
        self.engine, self.as_of, self.lookback_days = engine, as_of, lookback_days
        self.known_by = known_by.astimezone(timezone.utc) if known_by is not None else None
        self._memo: dict[str, Any] = {}
        self.kxfed_lineage: dict[str, Any] | None = None
        self.spy_basis: str | None = None
        self.first_print_obs: dict[str, date | None] = {}
        self.raw_lineage: dict[str, dict[str, Any]] = {}

    def series(self, series_id: str) -> pd.Series:
        if series_id not in self._memo:
            start = self.as_of - timedelta(days=self.lookback_days)
            status = "read"
            try:
                with self.engine.connect() as conn:
                    obs = read_window_known_at(conn, series_id, as_of=self.as_of, lag=LAGS.get(series_id),
                                               source=SOURCES.get(series_id), start=start, **self._cut_kw())
            except MixedSourceError as exc:
                log.warning("market_state: {s} is mixed-source, skipped: {e}", s=series_id, e=str(exc))
                obs = []
                status = "refused_mixed_source"
            s = pd.Series({o.obs_date: o.value for o in obs if o.value is not None}, dtype=float).sort_index()
            self._memo[series_id] = s[np.isfinite(s.to_numpy())] if len(s) else s
            self.raw_lineage[series_id] = raw_selection_receipt(
                obs, self._memo[series_id], series_id=series_id, as_of=self.as_of, start=start, status=status,
                **self._cut_kw())
        return self._memo[series_id]

    def _cut_kw(self) -> dict[str, datetime]:
        """``as_of_ts`` only when the reads are cut, so the decision record's calls stay exactly as they were."""
        return {"as_of_ts": self.known_by} if self.known_by is not None else {}

    def kxfed(self) -> list[PP.Snapshot]:
        """Every Kalshi KXFED row pulled before the end of ``as_of`` (or by ``known_by``), priced from its ladder.

        Known-at is the row's ``pull_timestamp`` only (intelligence/regime/policy_path.py); the row's ``value`` column
        is never read. The pull clock is compared here rather than in SQL so SQLite tests and Postgres agree.
        """
        if "__kxfed__" not in self._memo:
            cutoff = self.known_by or datetime.combine(self.as_of + timedelta(days=1), time(0), tzinfo=timezone.utc)
            start = self.as_of - timedelta(days=KXFED_LOOKBACK_DAYS)
            snaps: list[PP.Snapshot] = []
            try:
                with self.engine.connect() as conn:
                    rows = conn.execute(_KXFED_SQL, {"src": PP.KALSHI_SOURCE, "pat": PP.KXFED_PREFIX + "%",
                                                     "a": start, "b": self.as_of}).fetchall()
                for sid, od, ts, payload in rows:
                    pulled = _instant(ts)
                    if pulled is None or (pulled > cutoff if self.known_by is not None else pulled >= cutoff):
                        continue
                    obs = od if isinstance(od, date) else date.fromisoformat(str(od)[:10])
                    snaps.append(PP.snapshot_from_row(str(sid), obs, pulled, payload))
            except Exception as exc:
                log.warning("market_state: KXFED rows unreadable: {e}", e=str(exc))
                snaps = []
            self._memo["__kxfed__"] = snaps
            self.kxfed_lineage = PP.selection_receipt(snaps, as_of=self.as_of, cutoff=cutoff,
                                                      inclusive=self.known_by is not None)
        return self._memo["__kxfed__"]

    def policy_path(self) -> tuple[pd.Series, pd.Series, dict[date, PP.DayReading]]:
        if "__path__" not in self._memo:
            _, _, days, _ = xnys_sessions()
            start = self.as_of - timedelta(days=KXFED_LOOKBACK_DAYS)
            window = [d for d in days if start <= d <= self.as_of]
            self._memo["__path__"] = PP.path_series(self.kxfed(), window)
        return self._memo["__path__"]

    def vintages(self, series_id: str) -> pd.DataFrame:
        """ALFRED vintage rows (store/fred_vintages.py) for observations in the lookback, published by as_of."""
        key = "__vint__" + series_id
        if key not in self._memo:
            from store import fred_vintages as FV

            cut = {"fetched_by": self.known_by} if self.known_by is not None else {}
            try:
                df = FV.load(self.engine, series_id, self.as_of - timedelta(days=NEWS_LOOKBACK_DAYS), self.as_of,
                             self.as_of, **cut)
            except Exception as exc:  # table absent before the first fetch
                log.warning("market_state: no vintages for {s}: {e}", s=series_id, e=str(exc))
                df = pd.DataFrame(columns=["obs_date", "realtime_start", "realtime_end", "value"])
            self._memo[key] = df
        return self._memo[key]

    def gex_rows(self) -> pd.DataFrame:
        """``dealer_gex_daily`` SPY rows up to as_of. A row for day d comes from a capture completed by the end of d
        (``physics/dealer_gex_daily.py``), so it is known at the end of d."""
        if "__gex__" not in self._memo:
            params = {"t": "SPY", "a": self.as_of - timedelta(days=self.lookback_days), "b": self.as_of}
            try:
                with self.engine.connect() as conn:
                    if self.known_by is None:
                        rows = conn.execute(_GEX_SQL, params).fetchall()
                    else:
                        # A row without a creation clock is not known at the cutoff.
                        rows = [r[:6] for r in conn.execute(_GEX_CREATED_SQL, params).fetchall()
                                if (t := _instant(r[6])) is not None and t <= self.known_by]
            except Exception as exc:
                log.warning("market_state: dealer_gex_daily unreadable: {e}", e=str(exc))
                rows = []
            df = pd.DataFrame(rows, columns=["obs_date", "net_gex_usd_m", "spot_to_flip_pct", "gamma_flip_strike",
                                             "gex_regime", "spot_price"])
            df["obs_date"] = [x if isinstance(x, date) else date.fromisoformat(str(x)[:10]) for x in df["obs_date"]]
            self._memo["__gex__"] = df.set_index("obs_date").sort_index() if len(df) else df
        return self._memo["__gex__"]

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
            # Under a cutoff the resolved feature is not read: resolved_series has no write clock, and the resolver
            # stamps release_date with the raw pull date, so a row it wrote after the cutoff looks the same as one
            # written before. YF:SPY:close below has pull timestamps.
            resolved = None if self.known_by is not None else _fetch_resolved_spy_full(self.engine, self.as_of, start)
            resolved = resolved if resolved is not None else pd.Series(dtype=float)
            prices, basis = resolved, ("spy_full" if len(resolved) else None)
            if len(resolved) < SPY_NEED_OBS:
                try:
                    with self.engine.connect() as conn:
                        obs = read_window_known_at(conn, "YF:SPY:close", as_of=self.as_of, lag=SPY_CLOSE_LAG,
                                                   source="yfinance", start=start, **self._cut_kw())
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


# ── News (daily newspaper-based uncertainty indexes, read as FRED showed them on each day) ──
#
# USEPUINDXD / WLEMUINDXD are revised for weeks: the first print of 2022-06-02 was 60.23, the settled value 144.13.
# Each past day's reading is recomputed from the vintages live on that day, so today's reading is compared with past
# readings made the same way (a first print against past first prints), never with revised history.
NEWS_SETTLE_DAYS = 3          # skip the newest observations, which move most after first print
NEWS_WINDOW_DAYS = 7          # a full week, so weekday/weekend levels never tilt the mean
NEWS_MIN_KNOWN = 5
NEWS_LOOKBACK_DAYS = 420      # 252 trading days of readings plus the 7+3 day window and holidays
SHOCK_Z = 2.0


def _reading_days(as_of: date) -> list[date]:
    from ingestion.market_calendar import trading_days_between

    days = trading_days_between(as_of - timedelta(days=NEWS_LOOKBACK_DAYS - 15), as_of)
    return days if days and days[-1] == as_of else days + [as_of]


def settled_news(vint: pd.DataFrame, days: list[date]) -> pd.Series:
    """Per day t: mean of the observations t-9..t-3 as FRED showed them at the end of t (>= 5 known)."""
    from store.fred_vintages import values_as_of

    out = {}
    for t in days:
        obs = [t - timedelta(days=NEWS_SETTLE_DAYS + k) for k in range(NEWS_WINDOW_DAYS)]
        vals = [v for v in values_as_of(vint, obs, t).values() if v is not None]
        if len(vals) >= NEWS_MIN_KNOWN:
            out[t] = float(np.mean(vals))
    return pd.Series(out, dtype=float).sort_index()


def first_print_news(vint: pd.DataFrame, days: list[date]) -> tuple[pd.Series, dict[date, date]]:
    """Per day t: the newest weekday observation FRED had published by the end of t, as first shown then.

    Returns (readings by t, the observation date each reading is for). Weekday observations only, so the reading never
    flips between weekday and weekend newspaper levels.
    """
    from store.fred_vintages import values_as_of

    out, obs_for = {}, {}
    for t in days:
        cands = [t - timedelta(days=k) for k in range(1, 6)]
        cands = [o for o in cands if o.weekday() < 5]
        known = values_as_of(vint, cands, t)
        for o in cands:                      # newest first
            if known.get(o) is not None:
                out[t], obs_for[t] = known[o], o
                break
    return pd.Series(out, dtype=float).sort_index(), obs_for


def _settled(series_id: str) -> Callable[["_Reads"], pd.Series]:
    return lambda r: settled_news(r.vintages(series_id), _reading_days(r.as_of))


def _first_print(series_id: str) -> Callable[["_Reads"], pd.Series]:
    def f(r: "_Reads") -> pd.Series:
        s, obs_for = first_print_news(r.vintages(series_id), _reading_days(r.as_of))
        r.first_print_obs[series_id] = obs_for.get(r.as_of)
        return s
    return f


def news_shock(reading: dict[str, Any], obs_date: date | None, calendar: dict[str, list[date]]) -> dict[str, Any]:
    """Describe a newspaper uncertainty proxy and calendar co-occurrence, not news causality.

    A large index reading is not proof of a market-moving shock. Sharing a date with a
    listed release cannot attribute the reading to that release. Conversely, no listed
    release or a low proxy reading cannot establish an unscheduled shock or flow-only day.
    The enclosing key remains ``news_shock`` for record compatibility; kind is descriptive.
    """
    if reading.get("status") != "ok" or obs_date is None:
        return {"status": "unavailable", "reason": reading.get("reason", "no first print")}
    z = reading.get("robust_z")
    if isinstance(z, (bool, np.bool_)) or not isinstance(z, (int, float, np.number)) or not np.isfinite(z):
        return {"status": "unavailable", "reason": "first-print robust_z is not a finite number"}
    events = sorted(k for k, ds in calendar.items() if obs_date in ds) if calendar else []
    context = "listed_release" if events else ("no_listed_release" if calendar else "unknown")
    return {
        "status": "ok", "kind": "elevated_uncertainty" if z >= SHOCK_Z else "not_elevated",
        "obs_date": obs_date.isoformat(), "robust_z": float(z), "threshold_z": SHOCK_Z,
        "events_on_obs_day": events, "calendar_known": bool(calendar), "calendar_context": context,
        "news_attribution": "unverified", "supports_flow_only_classification": False,
        "basis": "newspaper uncertainty index first print; calendar co-occurrence only",
    }



# ── DIX and dealer gamma ──

_GEX_SQL = text(
    "SELECT obs_date, net_gex_usd_m, spot_to_flip_pct, gamma_flip_strike, gex_regime, spot_price "
    "FROM dealer_gex_daily WHERE ticker = :t AND obs_date >= :a AND obs_date <= :b ORDER BY obs_date"
)
# The same read with each row's creation clock, for a read cut at an instant (#1312).
_GEX_CREATED_SQL = text(
    "SELECT obs_date, net_gex_usd_m, spot_to_flip_pct, gamma_flip_strike, gex_regime, spot_price, created_at "
    "FROM dealer_gex_daily WHERE ticker = :t AND obs_date >= :a AND obs_date <= :b ORDER BY obs_date"
)


def _instant(raw: Any) -> datetime | None:
    """A stored clock as an aware UTC instant (naive taken as UTC, as griddb runs Etc/UTC); None when absent."""
    if raw is None:
        return None
    ts = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def _gex_col(col: str) -> Callable[["_Reads"], pd.Series]:
    def f(r: "_Reads") -> pd.Series:
        df = r.gex_rows()
        return pd.Series(dtype=float) if df.empty else df[col].astype(float).dropna()
    return f


def _change(series_id: str, periods: int) -> Callable[["_Reads"], pd.Series]:
    return lambda r: r.series(series_id).diff(periods).dropna()


# ── Policy path (Kalshi KXFED ladder, intelligence/regime/policy_path.py) ──

KXFED_LOOKBACK_DAYS = 560      # 252 sessions of baseline + the 5-session change + meeting rolls, with room
_KXFED_SQL = text(
    "SELECT r.series_id, r.obs_date, r.pull_timestamp, r.raw_payload FROM raw_series r "
    "JOIN source_catalog sc ON sc.id = r.source_id "
    "WHERE sc.name = :src AND r.series_id LIKE :pat AND r.pull_status = 'SUCCESS' "
    "AND r.obs_date >= :a AND r.obs_date <= :b"
)


def _fed_path_level(r: "_Reads") -> pd.Series:
    return r.policy_path()[0]


def _fed_path_change(r: "_Reads") -> pd.Series:
    return r.policy_path()[1]


def _fed_priced_move(r: "_Reads") -> pd.Series:
    readings = r.policy_path()[2]
    return PP.priced_move_series(readings, r.series("DFEDTARU"), PP.meetings_seen(r.kxfed()))


def next_meeting_detail(r: "_Reads") -> dict[str, Any]:
    """The newest next-meeting reading at or before ``as_of``: contract, clock, path, and odds against the target."""
    readings = r.policy_path()[2]
    if not readings:
        return {"status": "unavailable", "reason": "no usable KXFED next-meeting ladder known at as_of",
                "rule": PP.POLICY_PATH_RULE}
    d = max(readings)
    rd = readings[d]
    out: dict[str, Any] = {
        "status": "ok", "rule": PP.POLICY_PATH_RULE, "obs_date": d.isoformat(), "event": rd.event,
        "meeting_at": rd.meeting_at.isoformat(), "snapshot_pulled_at": rd.pulled_at.isoformat(),
        "expected_upper": round(rd.ladder.expected_upper, 4), "monotone_adjusted": rd.ladder.monotone_adjusted,
        "max_spread": round(rd.ladder.max_spread, 4),
        "tails": {"below": round(rd.ladder.tail_below, 4), "above": round(rd.ladder.tail_above, 4)},
    }
    pair = PP.target_pairing(rd, r.series("DFEDTARU"), PP.meetings_seen(r.kxfed()))
    if pair is None:
        out["target"] = {"status": "unavailable",
                         "reason": "no DFEDTARU observation known after the last meeting's decision took effect"}
    else:
        t_obs, t_val = pair
        out["target"] = {"status": "ok", "series": "DFEDTARU", "obs_date": t_obs.isoformat(), "upper": t_val}
        out["priced_move"] = round(rd.ladder.expected_upper - t_val, 4)
        out["odds"] = PP.outcome_odds(rd.ladder, t_val)
    return out


# ── Policy response (PRFD, "is the Fed put alive?") ──
#
# Neon's PRFD composite (2026-11-22) reduced to the legs Codex's triage allowed first (GRID-COMPOSITE-TRIAGE-20261122:
# "Test VIX-only and VIX+front-end controls before layering EMPS or text"): equity stress (VIX) against how much the
# front end eased over the same five sessions (2-year yield, Kalshi path at the next meeting). Equal weights and
# round thresholds, declared here and never fitted. Research tracking only (#1311): nothing reads it to decide.
POLICY_RESPONSE_RULE = "policy-response/1"
POLICY_LEGS = ("vix", "ust_2y_change_5d", "fed_path_change_5d")
POLICY_STRESS_Z = 1.5          # VIX robust z at or above which the market is under stress
POLICY_PUT_Z = 1.0             # front-end easing z at or above which a rescue is priced


def policy_response(variables: dict[str, dict[str, Any]], next_meeting: dict[str, Any]) -> dict[str, Any]:
    """Stress vs. front-end easing from the record's own readings; ``unknown`` unless every leg reads ``ok``."""
    base: dict[str, Any] = {
        "rule": POLICY_RESPONSE_RULE, "decision_use": "none",
        "validation": "unvalidated research composite tracked for #1311; no registered test",
        "thresholds": {"stress_z": POLICY_STRESS_Z, "put_priced_z": POLICY_PUT_Z},
        "next_meeting": next_meeting,
    }
    legs, bad = {}, {}
    for k in POLICY_LEGS:
        v = variables.get(k) or {}
        z = v.get("robust_z")
        legs[k] = {"status": v.get("status", "missing"), "obs_date": v.get("obs_date"), "robust_z": z}
        finite = isinstance(z, (int, float, np.number)) and not isinstance(z, (bool, np.bool_)) and np.isfinite(z)
        if v.get("status") != "ok" or not finite:
            bad[k] = v.get("reason") or ("no finite robust z" if v.get("status") == "ok" else v.get("status", "missing"))
    base["legs"] = legs
    if bad:
        return {**base, "status": "unknown", "label": "unknown",
                "reason": "; ".join(f"{k}: {why}" for k, why in bad.items())}
    stress = float(legs["vix"]["robust_z"])
    easing_2y = -float(legs["ust_2y_change_5d"]["robust_z"])
    easing_path = -float(legs["fed_path_change_5d"]["robust_z"])
    easing = (easing_2y + easing_path) / 2.0
    if stress < POLICY_STRESS_Z:
        label = "no_stress"
    elif easing >= POLICY_PUT_Z:
        label = "put_priced"
    elif easing <= 0.0:
        label = "put_absent"
    else:
        label = "partial_response"
    return {**base, "status": "ok", "label": label, "score": round(stress - easing, 4),
            "controls": {"vix_only_z": round(stress, 4), "front_end_easing_z": round(easing, 4),
                         "ust_2y_easing_z": round(easing_2y, 4), "fed_path_easing_z": round(easing_path, 4)}}


def gamma_state(df: pd.DataFrame, as_of: date) -> dict[str, Any]:
    """The latest dealer-gamma row's categorical read (regime, flip), or unavailable.

    ``stale`` (with the read kept) when that row is not from the ``as_of`` session itself (:data:`MAX_AGE`).
    """
    if df is None or df.empty:
        return {"status": "unavailable", "reason": "no dealer_gex_daily row at or before as_of"}
    d = df.index[-1]
    row = df.iloc[-1]
    fresh = freshness(d, as_of, MAX_AGE["dealer_gamma"])
    out = {"status": "ok" if fresh["status"] == "fresh" else "stale", "obs_date": str(d),
           "age_days": (as_of - d).days, "regime": row["gex_regime"],
           "gamma_flip": float(row["gamma_flip_strike"]), "spot": float(row["spot_price"]),
           "spot_vs_flip": "above" if float(row["spot_to_flip_pct"]) >= 0 else "below", "freshness": fresh}
    if out["status"] == "stale":
        out["reason"] = _stale_reason(d, fresh)
    return out


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
    VarSpec("dix", "flow", "daily_fast", _level("finra:dix_sp500"),
            "dark-pool short share of S&P 500 dollar volume", "FINRA Reg SHO file, same-day close weights; from 2018-08"),
    VarSpec("dix_change_5d", "flow", "daily_fast", _change("finra:dix_sp500", 5), "DIX change over 5 sessions"),
    VarSpec("dealer_gamma", "positioning", "daily_fast", _gex_col("net_gex_usd_m"),
            "$M per 1% SPY move (assumed long calls / short puts)", "dealer_gex_daily; registered captures from 2026-09-26"),
    VarSpec("spot_vs_gamma_flip", "positioning", "daily_fast", _gex_col("spot_to_flip_pct"),
            "% SPY above (+) or below (-) its gamma flip"),
    VarSpec("news_uncertainty", "news", "daily_fast", _settled("USEPUINDXD"),
            "EPU index, 7-day mean of days t-9..t-3", "Baker-Bloom-Davis daily newspaper index, as FRED showed it on t"),
    VarSpec("equity_news_uncertainty", "news", "daily_fast", _settled("WLEMUINDXD"),
            "equity-market uncertainty index, 7-day mean of days t-9..t-3", "as FRED showed it on t"),
    VarSpec("news_first_print", "news", "daily_fast", _first_print("USEPUINDXD"),
            "EPU index, newest weekday first print", "timely but noisy: compared only with past first prints"),
    VarSpec("ust_2y_change_5d", "rates", "daily_fast", _change("DGS2", 5), "% points over 5 sessions",
            "2-year Treasury constant maturity (FRED DGS2)"),
    VarSpec("fed_path_next_meeting", "policy", "daily_fast", _fed_path_level,
            "% (expected upper bound after the next FOMC meeting)",
            "Kalshi KXFED strike ladder, bid/ask mids (kalshi-fed-path/1); snapshot pulled overnight before the session"),
    VarSpec("fed_path_change_5d", "policy", "daily_fast", _fed_path_change,
            "% points over 5 sessions, same meeting contract", "never differenced across two meetings' contracts"),
    VarSpec("fed_priced_move_next_meeting", "policy", "daily_fast", _fed_priced_move,
            "% points (expected upper bound after the next meeting minus the current upper bound)",
            "current upper bound = FRED DFEDTARU, only when published after the last meeting's decision took effect"),
]
VAR_NAMES = [v.name for v in VARIABLES]


# ── Freshness (max-age/1) ─────────────────────────────────────────────────
#
# A reading is ``stale`` when its newest observation is older than that variable normally is. Measured over every
# stored record (2019-01-02..2026-11-19): next-business-day series are 1 session old (2 when a federal holiday falls
# on a trading day), same-session reads are 0, and each weekly or monthly series peaks at exactly one period plus its
# declared publication lag, minus a day. Older readings were real gaps: news vintages up to 23 sessions old
# (2022-03), SPY 5 sessions (2026-04), claims 18 days (2026-08), dealer gamma 4 sessions (2026-10), the 2025
# shutdown releases. Before this rule every one of them read ``ok``.

@dataclass(frozen=True)
class MaxAge:
    """How old a variable's newest observation may be before its reading is ``stale``."""

    limit: int
    unit: str          # 'sessions': NYSE sessions after the observation, up to and including as_of | 'days'
    basis: str


def _after_next_release(series_id: str, period_days: int) -> MaxAge:
    """Calendar-day limit for a weekly or monthly input: one period plus its declared publication lag, minus a day.

    Under the declared (deliberately late) lag the next observation is public ``period + lag`` days after the current
    one, so the current one is normally at most ``period + lag - 1`` days old. Older means the next observation is
    overdue or was never published (documented shutdown releases read stale, which is what they are).
    """
    lag = LAGS[series_id]
    if lag is None or lag.unit != "calendar":
        raise ValueError(f"{series_id}: a calendar-day publication lag is required for a day limit")
    return MaxAge(period_days + lag.days - 1, "days",
                  f"{series_id}: {period_days}-day period + {lag.days}-day publication lag - 1")


_NEXT_SESSION = MaxAge(2, "sessions", "posted the next business day: 1 session old, 2 when a federal holiday "
                                      "falls on a trading day")
_SAME_SESSION = MaxAge(1, "sessions", "read for the session itself: one missed session allowed")
_EXACT_SESSION = MaxAge(0, "sessions", "dealer positioning changes every session with open interest and expiries: "
                                       "only the session's own capture describes it")
_WEEK, _LONGEST_MONTH = 7, 31

MAX_AGE: dict[str, MaxAge] = {
    "vix": _NEXT_SESSION,
    "vix_term": _NEXT_SESSION,
    "hy_spread": _NEXT_SESSION,
    "ig_spread": _NEXT_SESSION,
    "yield_curve": _NEXT_SESSION,
    "fed_funds": _NEXT_SESSION,
    "real_fed_funds": _NEXT_SESSION,
    "net_liquidity": _after_next_release("WALCL", _WEEK),
    "net_liquidity_4w": _after_next_release("WALCL", _WEEK),
    "initial_claims": _after_next_release("ICSA", _WEEK),
    "unemployment": _after_next_release("UNRATE", _LONGEST_MONTH),
    "payrolls_3m": _after_next_release("PAYEMS", _LONGEST_MONTH),
    "cpi_yoy": _after_next_release("CPIAUCSL", _LONGEST_MONTH),
    "industrial_production_yoy": _after_next_release("INDPRO", _LONGEST_MONTH),
    "consumer_sentiment": _after_next_release("UMCSENT", _LONGEST_MONTH),
    "spy_vs_50d": _SAME_SESSION,
    "spy_vs_200d": _SAME_SESSION,
    "short_volume_ratio": _NEXT_SESSION,
    "dix": _NEXT_SESSION,
    "dix_change_5d": _NEXT_SESSION,
    "dealer_gamma": _EXACT_SESSION,
    "spot_vs_gamma_flip": _EXACT_SESSION,
    "news_uncertainty": _SAME_SESSION,
    "equity_news_uncertainty": _SAME_SESSION,
    "news_first_print": _SAME_SESSION,
    "ust_2y_change_5d": _NEXT_SESSION,
    # The KXFED snapshot dated a session is pulled overnight before it opens, so it is read for the session itself.
    "fed_path_next_meeting": _SAME_SESSION,
    "fed_path_change_5d": _SAME_SESSION,
    "fed_priced_move_next_meeting": _SAME_SESSION,
}
if set(MAX_AGE) != set(VAR_NAMES):
    raise RuntimeError(f"MAX_AGE must cover exactly VARIABLES: {sorted(set(MAX_AGE) ^ set(VAR_NAMES))}")


# Session ages count real NYSE sessions from the hash-pinned XNYS manifests in docs/reference/calendar-manifests,
# never the approximate scheduler helpers in ingestion.market_calendar. Those close Juneteenth before it existed
# (2020, 2021) and miss one-off closures such as 2025-01-09, so they misjudge ages both ways. The parts are
# analysis/factory_session_calendar.PRESCREEN_PINS then PINS: the same lists the deploy's
# scripts/prepare_factory_calendars.py normalizes, so a deployed tree can always open them. Prescreen ends
# 2022-11-11 and the factory parts start 2022-11-01: the overlap must agree.
_MANIFEST_ROOT = Path(__file__).resolve().parents[2] / "docs/reference/calendar-manifests"
_xnys_cache: tuple[date, date, tuple[date, ...], str] | None = None


def xnys_sessions() -> tuple[date, date, tuple[date, ...], str]:
    """(first covered day, last covered day, every session in order, version) from the pinned manifests.

    Raises ValueError when a manifest fails its pin or permission check, the parts leave a gap, or two parts
    disagree on a day both cover: an age is never counted on a calendar that is not the pinned one.
    """
    global _xnys_cache
    if _xnys_cache is None:
        from analysis.factory_session_calendar import PINS, PRESCREEN_PINS
        from ingestion.market_calendar import ManifestCalendar

        pins = tuple(PRESCREEN_PINS) + tuple(PINS)
        days: set[date] = set()
        start = end = None
        for name, sha in pins:
            part = ManifestCalendar(_MANIFEST_ROOT / name, sha)
            own = {date.fromisoformat(s.key.split(":")[1]) for s in part.sessions}
            if end is not None:
                if part.start > end + timedelta(days=1):
                    raise ValueError(f"XNYS calendar gap between {end} and {part.start}")
                if {d for d in days if d >= part.start} != {d for d in own if d <= end}:
                    raise ValueError(f"XNYS calendar parts disagree between {part.start} and {end}")
            days |= own
            start = part.start if start is None else start
            end = part.end if end is None else max(end, part.end)
        _xnys_cache = (start, end, tuple(sorted(days)), "pinned XNYS: " + ",".join(sha for _, sha in pins))
    return _xnys_cache


def sessions_after(obs_date: date, as_of: date) -> tuple[int, bool]:
    """NYSE sessions strictly after ``obs_date`` up to and including ``as_of``, and whether that is a lower bound.

    An ``obs_date`` before the calendar's first day counts from that day, so the count is a lower bound. An ``as_of``
    past its last day raises: extend the manifests rather than guess.
    """
    start, end, days, _ = xnys_sessions()
    if not start <= as_of <= end:
        raise ValueError(f"as_of {as_of} is outside the pinned XNYS calendar ({start}..{end}); extend the manifests")
    lower_bound = obs_date < start
    first = bisect_right(days, max(obs_date, start - timedelta(days=1)))
    return bisect_right(days, as_of) - first, lower_bound


def freshness(obs_date: date, as_of: date, max_age: MaxAge) -> dict[str, Any]:
    """Age of ``obs_date`` at ``as_of`` in ``max_age``'s unit, and whether it is past the limit.

    A limit of 0 sessions is the same-date rule: fresh only when ``obs_date`` is ``as_of`` itself.
    """
    out: dict[str, Any] = {"unit": max_age.unit, "limit": max_age.limit, "basis": max_age.basis}
    if max_age.unit == "sessions":
        age, lower_bound = sessions_after(obs_date, as_of)
        if lower_bound:
            out["age_is_lower_bound"] = True
        if max_age.limit == 0:
            stale = obs_date != as_of
        elif lower_bound and age <= max_age.limit:
            raise ValueError(f"cannot establish the age of {obs_date}: it is before the pinned XNYS calendar")
        else:
            stale = age > max_age.limit
    else:
        age = (as_of - obs_date).days
        stale = age > max_age.limit
    return {"status": "stale" if stale else "fresh", "age": age, **out}


def _stale_reason(obs_date: date, fresh: dict[str, Any]) -> str:
    return (f"newest observation {obs_date.isoformat()} is {fresh['age']} {fresh['unit']} old; "
            f"limit {fresh['limit']} ({fresh['basis']})")


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
    news_shock: dict[str, Any] = field(default_factory=dict)
    gamma_state: dict[str, Any] = field(default_factory=dict)
    policy_response: dict[str, Any] = field(default_factory=dict)
    spy_basis: str | None = None
    writer_provenance: dict[str, Any] | None = None
    source_lineage: dict[str, Any] | None = None
    freshness_calendar: str | None = None   # the pinned XNYS manifests session ages were counted on

    def to_dict(self) -> dict[str, Any]:
        ok = [k for k, v in self.variables.items() if v.get("status") == "ok"]
        return {
            "rule": RECORD_RULE,
            "baseline_rule": BASELINE_RULE,
            "freshness_rule": FRESHNESS_RULE,
            "freshness_calendar": self.freshness_calendar,
            "as_of": self.as_of.isoformat(),
            "availability": "date-bounded with source-specific availability rules; modeled-lag backfills may use revised vintages",
            "variables": self.variables,
            "events": self.events,
            "news_shock": self.news_shock,
            "gamma_state": self.gamma_state,
            "policy_response": self.policy_response,
            "pending": PENDING,
            "coverage": {"ok": len(ok), "no_baseline": sum(1 for v in self.variables.values()
                                                           if v.get("status") == "no_baseline"),
                         "stale": sum(1 for v in self.variables.values() if v.get("status") == "stale"),
                         "total": len(self.variables)},
            "spy_basis": self.spy_basis,
            "writer_provenance": self.writer_provenance,
            "source_lineage": self.source_lineage,
        }


def compute_market_state(engine: Engine, as_of: date, *, calendar: dict[str, list[date]] | None = None,
                         known_by: datetime | None = None) -> MarketState:
    """Every variable in :data:`VARIABLES` at ``as_of``, standardized against its own trailing baseline.

    ``known_by`` (timezone-aware, before the end of ``as_of``'s UTC day) is an exact availability cutoff: only inputs
    GRID held by that instant count (:class:`_Reads`). The provisional record passes the session's close (#1312);
    the decision record passes nothing and reads to the end of ``as_of``, as before.
    """
    if known_by is not None:
        if known_by.utcoffset() is None:
            raise ValueError("known_by must be timezone-aware")
        if known_by.astimezone(timezone.utc) >= datetime.combine(as_of + timedelta(days=1), time(0), tzinfo=timezone.utc):
            raise ValueError(f"known_by {known_by.isoformat()} must be before the end of as_of {as_of}")
    sessions_after(as_of, as_of)  # fail before any read when as_of is outside the pinned XNYS calendar
    reads = _Reads(engine, as_of, LOOKBACK_DAYS, known_by=known_by)
    state = MarketState(as_of=as_of, freshness_calendar=xnys_sessions()[3])
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
            # Known value, too little history to say what normal is: report the value, no z or label.
            entry = {**base, "status": "no_baseline", "obs_date": str(last_obs),
                     "value": round(float(hist.iloc[-1]), 6), "n": int(len(hist) - 1),
                     "reason": f"{len(hist) - 1} prior observations known; baseline needs {min_obs}"}
        else:
            entry = {**base, "status": "ok", "obs_date": str(last_obs),
                     "age_days": (as_of - last_obs).days if isinstance(last_obs, date) else None,
                     **reading.to_dict()}
        if isinstance(last_obs, date):
            # Too old outranks both ok and no_baseline; the reading is kept so a reader can see what was stale.
            fresh = freshness(last_obs, as_of, MAX_AGE[spec.name])
            entry["freshness"] = fresh
            if fresh["status"] == "stale":
                stale = _stale_reason(last_obs, fresh)
                entry["reason"] = stale if entry["status"] == "ok" else f"{stale}; {entry['reason']}"
                entry["status"] = "stale"
        state.variables[spec.name] = entry
    state.spy_basis = reads.spy_basis
    cal = load_calendar() if calendar is None else calendar
    state.events = events_state(as_of, cal)
    state.news_shock = news_shock(state.variables.get("news_first_print", {}),
                                  reads.first_print_obs.get("USEPUINDXD"), cal)
    state.gamma_state = gamma_state(reads.gex_rows(), as_of)
    try:
        detail = next_meeting_detail(reads)
    except Exception as exc:  # the block reports unknown; it never sinks the record
        log.warning("market_state: next-meeting detail failed at {d}: {e}", d=as_of, e=str(exc))
        detail = {"status": "unavailable", "reason": f"error: {type(exc).__name__}", "rule": PP.POLICY_PATH_RULE}
    state.policy_response = policy_response(state.variables, detail)
    state.source_lineage = {
        "schema": "market-state-source-lineage/1", "status": "partial", "pit_qualified": False,
        "raw_series": reads.raw_lineage,
        "kxfed": reads.kxfed_lineage,
        "unqualified_read_paths": ["ALFRED vintage selection/capture", "dealer-gamma capture",
                                   "SPY resolved/raw selected window", "calendar publication/capture"],
        "limitation": "selected raw-input fingerprints only; not full immutable custody or scheduled-run evidence",
    }
    if reads.known_by is not None:
        state.source_lineage["availability_cutoff"] = {"at": reads.known_by.isoformat(), "reads": dict(CUTOFF_READS)}
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
    """Write one record. Returns False when a row for that date exists and ``replace`` is False.

    A provisional record (writer mode ``provisional``, #1312) is refused before the table is touched: it goes to
    its own table through :func:`store_provisional_market_state`, never here.
    """
    if is_provisional(state):
        raise ValueError("market_state_daily takes decision records only; a provisional record goes to "
                         "market_state_provisional (#1312)")
    ensure_table(engine)
    with engine.begin() as conn:
        dialect = "postgresql" if conn.dialect.name == "postgresql" else "other"
        res = conn.execute(_INSERT_SQL[(dialect, bool(replace))],
                           {"d": state.as_of, "r": RECORD_RULE, "rec": json.dumps(state.to_dict())})
    return bool(res.rowcount)


# ── Provisional record at the close (#1312): display only ─────────────────
#
# Owner's decision (bring-up plan rev 100): #1191's previous-session rule stays for anything that decides. A record
# computed at the session's close is for display only, labelled provisional, and kept in its own table, so no reader
# of market_state_daily can pick one up by accident. Same record rule, earlier clock: the writer
# (scripts/run_market_state.py --provisional) runs only at or after the session's close in the pinned XNYS calendar
# and computes with compute_market_state(known_by=that close), an exact availability cutoff (CUTOFF_READS). A rerun
# hours later reads the same inputs, so nothing that reached GRID after the close can appear in it, and it never
# refreshes the FRED vintages. The only reader is the post-close desk brief (analysis/desk_brief_sources.market_state).
# tests/test_market_state_provisional_guard.py keeps it that way.

PROVISIONAL_TABLE = "market_state_provisional"
PROVISIONAL_MODE = "provisional"            # writer_provenance["mode"] of every provisional record
PROVISIONAL_DECISION_USE = "display_only"
PROVISIONAL_LABEL = ("Provisional, display only: computed from the inputs GRID held at the close of session {day} "
                     "({cutoff}); nothing that arrived later is in it. The session's decision record is written to "
                     "market_state_daily the next evening (#1191); nothing that decides reads this one.")
# How each read is cut at the provisional record's availability cutoff (compute_market_state(known_by=...)).
CUTOFF_READS = {
    "raw_series": "pull_timestamp at or before the cutoff only: a modeled known-at date never admits a row first "
                  "pulled later; a release override still delays one until its UTC day has ended",
    "fred_vintages": "fetched_at at or before the cutoff; each version's interval rebuilt from the versions known "
                     "then (the stored realtime_end is extended in place by later refreshes and is not read)",
    "dealer_gex_daily": "created_at at or before the cutoff; a row without one is not known",
    "kxfed": "raw_series KALSHI:KXFED-* rows (source Kalshi) by pull_timestamp at or before the cutoff; no modeled "
             "path; priced from raw_payload all_strikes, never the value column (intelligence/regime/policy_path.py)",
    "resolved_series": "not read: it keeps no write clock (the resolver stamps release_date with the raw pull "
                       "date), so SPY comes from YF:SPY:close by pull_timestamp, the record's own fallback basis",
}
# The table is created by the reviewed migration migrations/versions/market_state_prov_20261122.py; production
# never runs CREATE TABLE for it. This copy exists for SQLite tests only and keeps the same columns
# (tests/test_market_state_provisional_migration.py). record is TEXT here because a JSONB column would get numeric
# affinity on SQLite.
_PROVISIONAL_TABLE_SQL = text(
    "CREATE TABLE IF NOT EXISTS market_state_provisional ("
    " as_of_date DATE PRIMARY KEY,"
    " rule TEXT NOT NULL,"
    " record TEXT NOT NULL,"
    " computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    " provisional BOOLEAN NOT NULL DEFAULT TRUE CHECK (provisional))"
)
PROVISIONAL_MIGRATION = "market_state_prov_20261122"

_PROVISIONAL_INSERT_SQL = {
    ("postgresql", False): text("INSERT INTO market_state_provisional (as_of_date, rule, record, provisional) "
                                "VALUES (:d, :r, CAST(:rec AS JSONB), TRUE) ON CONFLICT (as_of_date) DO NOTHING"),
    ("postgresql", True): text("INSERT INTO market_state_provisional (as_of_date, rule, record, provisional) "
                               "VALUES (:d, :r, CAST(:rec AS JSONB), TRUE) ON CONFLICT (as_of_date) DO UPDATE SET "
                               "rule = EXCLUDED.rule, record = EXCLUDED.record, computed_at = CURRENT_TIMESTAMP"),
    ("other", False): text("INSERT INTO market_state_provisional (as_of_date, rule, record, provisional) "
                           "VALUES (:d, :r, :rec, TRUE) ON CONFLICT (as_of_date) DO NOTHING"),
    ("other", True): text("INSERT INTO market_state_provisional (as_of_date, rule, record, provisional) "
                          "VALUES (:d, :r, :rec, TRUE) ON CONFLICT (as_of_date) DO UPDATE SET "
                          "rule = EXCLUDED.rule, record = EXCLUDED.record, computed_at = CURRENT_TIMESTAMP"),
}


def is_provisional(state: MarketState) -> bool:
    """True for a record the provisional writer made (its provenance says mode ``provisional``)."""
    return (state.writer_provenance or {}).get("mode") == PROVISIONAL_MODE


def provisional_record(state: MarketState) -> dict[str, Any]:
    """The stored form of a provisional record: the record itself plus its display-only label and clocks.

    Raises ValueError unless the provisional writer made it (provenance mode ``provisional``) and it was computed
    with the availability cutoff its provenance names, so neither the label nor a cutoff can be claimed by a record
    that does not have them.
    """
    prov = state.writer_provenance or {}
    if prov.get("mode") != PROVISIONAL_MODE:
        raise ValueError("a provisional record needs writer provenance with mode 'provisional' "
                         "(scripts/run_market_state.py --provisional)")
    cutoff = ((state.source_lineage or {}).get("availability_cutoff") or {}).get("at")
    if cutoff is None or cutoff != prov.get("availability_cutoff"):
        raise ValueError("a provisional record must be computed with the availability cutoff its provenance names "
                         "(compute_market_state(known_by=the session's close))")
    computed = prov.get("started_at")
    after = (datetime.fromisoformat(computed) - datetime.fromisoformat(cutoff)).total_seconds() / 60 if computed else None
    return {**state.to_dict(), "provisional": True, "decision_use": PROVISIONAL_DECISION_USE,
            "label": PROVISIONAL_LABEL.format(day=state.as_of.isoformat(), cutoff=cutoff),
            "availability": f"inputs GRID held at {cutoff}, each read cut on its own stored clock (CUTOFF_READS)",
            "availability_cutoff": cutoff, "computed_at": computed,
            "minutes_after_close": round(after, 1) if after is not None else None}


def ensure_provisional_table(engine: Engine) -> None:
    """SQLite (tests) only. Elsewhere the table comes from migration ``market_state_prov_20261122``."""
    if engine.dialect.name != "sqlite":
        raise RuntimeError(f"{PROVISIONAL_TABLE} is created by alembic migration {PROVISIONAL_MIGRATION}; "
                           "production never creates it at runtime")
    with engine.begin() as conn:
        conn.execute(_PROVISIONAL_TABLE_SQL)


def store_provisional_market_state(engine: Engine, state: MarketState, *, replace: bool = False) -> bool:
    """Write one provisional record (insert-only per date; ``replace`` rewrites it). False when one exists.

    Never creates the table outside SQLite: on Postgres a missing table is an error naming the migration.
    """
    rec = provisional_record(state)
    if engine.dialect.name == "sqlite":
        ensure_provisional_table(engine)
    else:
        from sqlalchemy import inspect

        if not inspect(engine).has_table(PROVISIONAL_TABLE):
            raise RuntimeError(f"{PROVISIONAL_TABLE} is missing: apply alembic migration {PROVISIONAL_MIGRATION}")
    with engine.begin() as conn:
        dialect = "postgresql" if conn.dialect.name == "postgresql" else "other"
        res = conn.execute(_PROVISIONAL_INSERT_SQL[(dialect, bool(replace))],
                           {"d": state.as_of, "r": RECORD_RULE, "rec": json.dumps(rec)})
    return bool(res.rowcount)
