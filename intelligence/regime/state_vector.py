"""
State vector construction for the regime-matched analog engine.

Each state vector captures the macro environment at a point in time across
24 dimensions — VIX, rates, spreads, employment, liquidity, momentum, and
cross-reference divergence scores.

Availability contract (``as_of`` = end of that UTC day)
--------------------------------------------------------------------
* **Macro inputs** are read with ``store.observations.read_window_known_at``:
  an observation is visible at ``as_of`` only if it was pulled by then, or —
  for history backfilled long after the fact (every FRED row on griddb was
  pulled on or after 2026-03-24) — if its declared, conservative
  :data:`PUBLICATION_LAGS` entry says it was published by then. The
  observation date alone never makes a value visible. Modeled dates delay
  eligibility but values remain revised vintage as of backfill (2026-03-24),
  not first-release/ALFRED values. Documented UNRATE shutdown dates override
  the ordinary lag; other exceptional release delays remain unmodeled.
  ``store.pit.PITStore`` cannot serve these reads: ``resolved_series``'s
  ``release_date`` for them is (almost always) the backfill pull date, so
  it returns little or no history before 2026-03.
* **Normalisation** (z-scores of the ``raw`` dims) uses mean/std over the
  same PIT-visible series in a rolling :data:`NORM_LOOKBACK_DAYS` window
  ending at ``as_of`` — the 10,000-day window the original design used,
  anchored at ``as_of`` instead of the run date. No process-level cache: a
  vector no longer uses run-date normalization. Later insertion of previously
  absent historical observations can still change modeled historical vectors.
* **SPY** momentum/RSI keep their basis order (``spy_full`` via ``store.pit``
  where PIT-visible, i.e. ``release_date <= as_of``, else raw
  ``YF:SPY:close``). The raw fallback is read with the same
  ``read_window_known_at`` rule as the macro inputs (E1-V1): a close is
  visible if it was pulled by ``as_of`` (latest such vintage) or, for
  backfilled history, by :data:`SPY_CLOSE_LAG` — the close of session ``d``
  is public once ``d``'s session ends, i.e. by the end of UTC day ``d``
  (earliest pulled vintage). A re-pull or restatement after ``as_of`` never
  changes a past vector.
* **Insider sentiment** counts a Form 4 row only if it was known by the end
  of ``as_of`` (E1-V2): pulled by then, or filed by then under VS1's
  convention (``analysis.panel_insider_density.filing_known_at``: filing date
  22:00 America/New_York). ``INSIDER:*`` rows are dated by *transaction*
  date, so ``obs_date <= as_of`` alone admits filings made after ``as_of``.
* **VIX** (``vix_level`` / ``vix_percentile``) reads FRED ``VIXCLS``
  whenever its known-at window can produce those dimensions. Only when it
  cannot (absent, too short, or mixed-source at ``as_of``) does the whole
  series switch to the Cboe-published close ``CBOE:VIX`` (source ``CBOE``,
  the originator of VIXCLS), read with the same known-at rule and the same
  next-business-day lag. A usable VIXCLS that is only *late* keeps every
  close it has; just its trailing gap, up to the date VIXCLS itself would be
  modeled as published by ``as_of``, is filled from CBOE:VIX closes known
  at ``as_of``. The choice is recorded in ``StateVector.vix_basis``.

State vectors are cached in the `regime_state_vectors` table.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.observations import (
    MixedSourceError,
    PublicationLag,
    read_window_known_at,
)


# ── Dimension specification ──────────────────────────────────────────────

@dataclass(frozen=True)
class DimensionSpec:
    """Declarative config for one state vector dimension."""
    name: str
    series_id: str          # raw_series.series_id or 'DERIVED:xxx'
    transform: str          # 'raw', 'percentile_rank', 'slope', 'diff', 'pct_change', 'rsi', 'ma_ratio', 'spread'
    weight: float           # importance weight for similarity matching
    transform_param: int | float = 0   # window/period for transform
    transform_param2: int = 0          # second param (e.g., slow MA for ma_ratio)
    min_history: int = 100             # minimum obs needed


# The 24 dimensions of the macro state
STATE_DIMENSIONS: list[DimensionSpec] = [
    # ── Volatility & Risk ──
    DimensionSpec('vix_level',           'VIXCLS',                             'raw',             1.2),
    DimensionSpec('vix_percentile',      'VIXCLS',                             'percentile_rank', 1.5, transform_param=504),
    # ── Rates & Curve ──
    DimensionSpec('yield_curve_level',   'T10Y2Y',                             'raw',             1.3),
    DimensionSpec('yield_curve_dir',     'T10Y2Y',                             'slope',           1.0, transform_param=63),
    DimensionSpec('fed_funds_level',     'DFF',                                'raw',             1.0),
    DimensionSpec('fed_funds_dir',       'DFF',                                'diff',            1.2, transform_param=63),
    # ── Credit ──
    DimensionSpec('hy_spread_level',     'BAMLH0A0HYM2',                       'raw',             1.4),
    DimensionSpec('hy_spread_dir',       'BAMLH0A0HYM2',                       'diff',            1.1, transform_param=63),
    DimensionSpec('ig_spread_level',     'BAMLC0A0CM',                         'raw',             0.9),
    # ── Employment & Economy ──
    DimensionSpec('unemployment_level',  'UNRATE',                             'raw',             0.8, min_history=30),
    DimensionSpec('unemployment_dir',    'UNRATE',                             'diff',            0.9, transform_param=3, min_history=30),
    DimensionSpec('industrial_prod_yoy', 'INDPRO',                             'pct_change',      0.8, transform_param=12, min_history=30),
    DimensionSpec('capacity_util',       'TCU',                                'raw',             0.6, min_history=30),
    # ── Money & Inflation ──
    DimensionSpec('m2_growth',           'M2SL',                               'pct_change',      0.7, transform_param=12, min_history=30),
    DimensionSpec('breakeven_5y',        'DERIVED:T5YIE',                      'raw',             0.8, min_history=50),
    DimensionSpec('consumer_sentiment',  'UMCSENT',                            'raw',             0.6, min_history=30),
    # ── Labor Market ──
    DimensionSpec('initial_claims',      'ICSA',                               'raw',             0.7),
    # ── Liquidity ──
    DimensionSpec('fed_net_liq_level',   'COMPUTED:fed_net_liquidity',          'raw',             1.3, min_history=20),
    DimensionSpec('fed_net_liq_chg',     'COMPUTED:fed_net_liquidity_change_1m','raw',             1.1, min_history=20),
    # ── Equity Momentum ──
    DimensionSpec('spy_momentum',        'DERIVED:SPY_MA_RATIO',               'raw',             1.0),
    DimensionSpec('spy_rsi',             'DERIVED:SPY_RSI',                     'raw',             0.8),
    # ── Real Rates ──
    DimensionSpec('real_fed_funds',      'DERIVED:REAL_FF',                     'raw',             1.0),
    # ── Cross-reference divergence ──
    DimensionSpec('crossref_divergence', 'DERIVED:CROSSREF_SCORE',             'raw',             1.2, min_history=5),
    # ── Insider sentiment ──
    DimensionSpec('insider_sentiment',   'DERIVED:INSIDER_NET',                 'raw',             0.9, min_history=5),
]

DIM_NAMES = [d.name for d in STATE_DIMENSIONS]
DIM_WEIGHTS = np.array([d.weight for d in STATE_DIMENSIONS], dtype=np.float64)


# ── Publication lags (modeled availability) ──────────────────────────────
#
# Used only where GRID has no pull evidence for an observation (backfilled
# history); a value pulled by ``as_of`` is visible from its pull date. Each
# lag is deliberately late — at or beyond the latest normal release date —
# because an early lag is look-ahead and a late one only costs freshness.
# "First pull" figures are the observed obs->first-pull lags for the dates
# GRID ingested live (griddb, read-only, 2026-09-29).
#
# UNRATE shutdown exceptions below use official BLS release dates. Other
# series/delays still use modeled lags; this is not a complete release calendar.
# Revised history uses earliest GRID vintage (backfilled >=2026-03-24), not
# the historical first published value. Only actual vintage data can fix that.
_NEXT_BUSINESS_DAY = PublicationLag(
    1, "business",
    "next business day; same lag as analysis.research_real_panel.PUBLICATIONS "
    "(FRB_H15 / FRED_H15_SPREAD / ICE_BOFA / CBOE_VIX, reviewer-verified)",
)

PUBLICATION_LAGS: dict[str, PublicationLag | None] = {
    # Daily market/rates series, posted to FRED the next business day
    'VIXCLS': _NEXT_BUSINESS_DAY,
    'T10Y2Y': _NEXT_BUSINESS_DAY,
    'T5YIE': _NEXT_BUSINESS_DAY,
    'DFF': _NEXT_BUSINESS_DAY,
    'BAMLH0A0HYM2': _NEXT_BUSINESS_DAY,
    'BAMLC0A0CM': _NEXT_BUSINESS_DAY,
    # Monthly, dated the 1st of the reference month
    'UNRATE': PublicationLag(
        42, "calendar",
        "BLS Employment Situation: conservative ordinary release-date proxy; "
        "documented shutdown dates override it; revised vintage as of backfill 2026-03-24",
        release_overrides=(
            # https://www.bls.gov/schedule/2013/home.htm
            (date(2013, 9, 1), date(2013, 10, 22)),
            (date(2013, 10, 1), date(2013, 11, 8)),
            # https://www.bls.gov/news.release/archives/empsit_11202025.htm
            (date(2025, 9, 1), date(2025, 11, 20)),
            # https://www.bls.gov/news.release/archives/empsit_12162025.htm
            (date(2025, 11, 1), date(2025, 12, 16)),
        ),
        # CPS October data were not collected and will not be reconstructed.
        # https://www.bls.gov/cps/methods/2025-federal-government-shutdown-impact-cps.htm
        unpublished_dates=frozenset({date(2025, 10, 1)}),
    ),
    'INDPRO': PublicationLag(
        50, "calendar",
        "Fed G.17 industrial production, ~15th-18th of the next month; first pull 44-46d",
    ),
    'TCU': PublicationLag(
        50, "calendar",
        "Fed G.17 capacity utilisation, ~15th-18th of the next month; first pull 44-46d",
    ),
    'M2SL': PublicationLag(
        60, "calendar",
        "Fed H.6 money stock, ~4th Tuesday of the next month; first pull 51-55d",
    ),
    'UMCSENT': PublicationLag(
        60, "calendar",
        "UMich sentiment as posted to FRED (delayed vs the survey's own release); "
        "first pull 52-57d",
    ),
    # Weekly, dated the Saturday week-end
    'ICSA': PublicationLag(
        6, "calendar",
        "DOL weekly claims, following Thursday (Friday after a holiday); first pull 5d",
    ),
    # Computed by GRID itself: known when GRID computed it, never earlier
    'COMPUTED:fed_net_liquidity': None,
    'COMPUTED:fed_net_liquidity_change_1m': None,
    # VIX fallback (see VIX_FALLBACK_SERIES below): the Cboe close FRED
    # republishes as VIXCLS, so the same reviewer-verified lag applies.
    'CBOE:VIX': _NEXT_BUSINESS_DAY,
}

# ── VIX basis (R4) ───────────────────────────────────────────────────────
#
# ``vix_level`` / ``vix_percentile`` prefer FRED ``VIXCLS``. ``CBOE:VIX`` is
# the Cboe-published close FRED republishes (``ingestion/altdata/
# cboe_indices.py``, source ``CBOE``; history from 1990 via
# ``scripts/bulk_historical_pull.py``). It is used only when VIXCLS cannot
# produce the VIX dimensions at ``as_of``, and then as a whole series (values,
# percentile window and z-score stats all from CBOE:VIX). The read pins
# ``source`` because both writers of CBOE:VIX use the CBOE catalog entry and
# any other writer would be a different provenance.
#
# A usable VIXCLS that is merely behind (FRED posts late: 2-6 days on ~8% of
# dates since 2026-04, e.g. the 2026-09-23..25 closes landed on 09-29) gets
# its *trailing* gap filled from CBOE:VIX: only dates after VIXCLS's last
# known close, only Cboe closes known at ``as_of``, and only dates VIXCLS
# itself would be modeled as published by ``as_of`` (its own lag), so the
# fill never gives the VIX dims a fresher close than an on-time VIXCLS
# would. A date VIXCLS has is never replaced. On griddb the two series agree
# to the cent on all 9,150 overlapping dates (1990-01-02..2026-03-25).
VIX_SERIES = 'VIXCLS'
VIX_FALLBACK_SERIES = 'CBOE:VIX'
VIX_GAP_FILLED_BASIS = 'VIXCLS+CBOE:VIX'
SERIES_SOURCES: dict[str, str] = {VIX_FALLBACK_SERIES: 'CBOE'}
# A backup series is read for numeric use only: a SUCCESS row whose value is
# not a finite number (NaN, +/-inf) is not an observation, the same way the
# reader already treats NULL, so it can neither fill a date, nor count toward
# ``min_history``, nor enter the z-score history. VIXCLS and every other
# primary series keep the reader's unchanged behaviour.
FINITE_ONLY_SERIES: frozenset[str] = frozenset({VIX_FALLBACK_SERIES})

# Raw ``YF:SPY:close`` fallback (E1-V1). A daily close is public when its
# session ends (16:00 America/New_York = 20:00/21:00 UTC), so the close dated
# ``d`` is known by the end of UTC day ``d``: lag 0 business days. Used only
# where GRID has no pull by ``as_of`` (every close before 2026-04-09 on griddb
# was backfilled then); the value is the earliest pulled vintage, as for the
# macro inputs above. A wrong early vintage (e.g. a mis-adjusted close, #642)
# is therefore not repaired by re-pulling: mark it QUARANTINED.
SPY_CLOSE_LAG = PublicationLag(
    0, "business",
    "US equity close: public at the end of its own session (16:00 ET), i.e. by "
    "the end of that UTC day; earliest pulled vintage for backfilled history",
)

# Rolling window for the z-score mean/std, ending at as_of (see module doc).
NORM_LOOKBACK_DAYS = 10000
# Window the dimension transforms read (unchanged).
VALUE_LOOKBACK_DAYS = 2520

# ── Cadence-aware staleness (GRID-STALE-SOURCES-AUDIT-20260929.md §4) ────
#
# The old rule flagged a dimension stale when its latest obs was >30 days
# old, for every dimension regardless of publication cadence. FRED's
# monthly macro series carry obs_date = the 1st of the reference month but
# are published 4-8 weeks later, so a monthly dimension is *always* >30
# days old, even the same day GRID picks up its newest release (the audit
# measured a 4-minute-to-~51-hour GRID pickup lag across all five monthly
# series it checked -- the flag was 100% a false positive, never a real
# gap). These are the only monthly series among STATE_DIMENSIONS today;
# QUARTERLY_FRED_SERIES starts empty and is ready for one, same pattern.
MONTHLY_FRED_SERIES: frozenset[str] = frozenset({
    "UNRATE", "INDPRO", "TCU", "M2SL", "UMCSENT",
})
QUARTERLY_FRED_SERIES: frozenset[str] = frozenset()

DEFAULT_STALE_DAYS = 30
# ~70 days safely covers a monthly series' worst-case release lag (4-8
# weeks) plus GRID's own pickup delay, while still catching a release
# that's genuinely been missed (the next one is always <45 days away) --
# this is the audit's own suggested fix (§4: "monthly = stale only if obs
# is more than ~70 days old").
MONTHLY_STALE_DAYS = 70
# Same logic one tier out: a quarterly series can be published 1-3 months
# after quarter-end, so give it a proportionally larger window.
QUARTERLY_STALE_DAYS = 160


def _stale_threshold_days(series_id: str) -> int:
    """Cadence-aware staleness threshold, in days, for one series_id."""
    if series_id in MONTHLY_FRED_SERIES:
        return MONTHLY_STALE_DAYS
    if series_id in QUARTERLY_FRED_SERIES:
        return QUARTERLY_STALE_DAYS
    return DEFAULT_STALE_DAYS


# ── State Vector ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StateVector:
    """Immutable macro state at a point in time."""
    as_of_date: date
    values: tuple[float | None, ...]    # one per dimension, None = missing
    completeness: float                 # fraction of non-null dims
    stale_dimensions: tuple[str, ...]   # usable non-derived dims beyond their cadence threshold
    # Which SPY price series fed the momentum/RSI dimensions: "spy_full"
    # (the resolved, post re-resolve feature) or the raw "YF:SPY:close"
    # fallback, or None when neither was available (see _fetch_spy_prices).
    price_basis: str | None = None
    # True when this vector was served from regime_state_vectors rather
    # than freshly computed. GET routes (persist=False) return cached=False
    # for an in-memory computation that was never written.
    cached: bool = False
    # Which VIX close series fed vix_level/vix_percentile: VIX_SERIES
    # ("VIXCLS"), VIX_GAP_FILLED_BASIS ("VIXCLS+CBOE:VIX", VIXCLS with its
    # trailing gap filled), the VIX_FALLBACK_SERIES ("CBOE:VIX"), or None
    # when neither had enough known history (see _resolve_vix_series and
    # _fill_vix_trailing_gap). Cached rows written before this field existed
    # read back as None.
    vix_basis: str | None = None

    @property
    def array(self) -> np.ndarray:
        """Values as numpy array with NaN for missing."""
        return np.array([v if v is not None else np.nan for v in self.values], dtype=np.float64)

    @property
    def mask(self) -> np.ndarray:
        """Boolean mask — True where data exists."""
        return np.array([v is not None for v in self.values])

    def to_dict(self) -> dict[str, Any]:
        return {
            'as_of_date': self.as_of_date.isoformat(),
            'dimensions': {DIM_NAMES[i]: self.values[i] for i in range(len(self.values))},
            'completeness': self.completeness,
            'stale_dimensions': list(self.stale_dimensions),
            'price_basis': self.price_basis,
            'vix_basis': self.vix_basis,
            'cached': self.cached,
        }


# ── Helpers ──────────────────────────────────────────────────────────────

def _fetch_series(
    engine: Engine, series_id: str, as_of: date, lookback_days: int = VALUE_LOOKBACK_DAYS,
) -> pd.Series:
    """Macro series values known by the end of ``as_of`` (point-in-time).

    Goes through ``store.observations.read_window_known_at``: SUCCESS-only,
    one value per ``obs_date``, and an observation is included only if it was
    pulled by ``as_of`` (latest such vintage) or, for backfilled history, its
    :data:`PUBLICATION_LAGS` entry puts its release on or before ``as_of``
    (earliest pulled vintage). A series not in :data:`PUBLICATION_LAGS` gets
    no modeled path (pull evidence only). A series in :data:`SERIES_SOURCES`
    is read from that source only; one in :data:`FINITE_ONLY_SERIES` keeps
    only finite values. A series_id whose selected rows
    span more than one source (``store.observations.MixedSourceError``)
    degrades to an empty series rather than silently mixing sources; the
    caller already treats a short or empty series as "dimension unavailable".
    """
    cutoff = as_of - timedelta(days=lookback_days)
    try:
        with engine.connect() as conn:
            obs = read_window_known_at(
                conn, series_id, as_of=as_of, lag=PUBLICATION_LAGS.get(series_id),
                source=SERIES_SOURCES.get(series_id), start=cutoff,
            )
    except MixedSourceError as exc:
        log.warning("state_vector: {sid} is mixed-source, skipping: {e}", sid=series_id, e=str(exc))
        return pd.Series(dtype=float)
    if not obs:
        return pd.Series(dtype=float)
    series = pd.Series(
        {o.obs_date: o.value for o in obs},
        dtype=float,
    ).sort_index()
    if series_id in FINITE_ONLY_SERIES:
        series = series[np.isfinite(series.to_numpy())]
    return series


class _AsOfReader:
    """Per-as_of memo with separate normalization and accepted compute reads.

    Mixed-source validation is bounded by each read. A different source outside
    the 2520-day compute window must not make an otherwise accepted value vanish.
    Values and stale age share one cached compute input; normalization uses its
    distinct 10000-day window. No memo is shared across dates or calls.
    """

    def __init__(self, engine: Engine, as_of: date) -> None:
        self.engine = engine
        self.as_of = as_of
        self._full: dict[str, pd.Series] = {}
        self._windows: dict[str, pd.Series] = {}

    def full(self, series_id: str) -> pd.Series:
        if series_id not in self._full:
            self._full[series_id] = _fetch_series(
                self.engine, series_id, self.as_of, lookback_days=NORM_LOOKBACK_DAYS,
            )
        return self._full[series_id]

    def window(self, series_id: str) -> pd.Series:
        if series_id not in self._windows:
            self._windows[series_id] = _fetch_series(
                self.engine, series_id, self.as_of, lookback_days=VALUE_LOOKBACK_DAYS,
            )
        return self._windows[series_id]

    def append(self, series_id: str, extra: pd.Series) -> None:
        """Append later-dated observations to both memoized reads of ``series_id``.

        Both expanded series are built before either memo is assigned, so a
        failure while building the second one leaves both reads exactly as
        they were: the value/percentile window and the normalization history
        can never disagree about which observations they contain.
        """
        full = pd.concat([self.full(series_id), extra]).sort_index()
        window = pd.concat([self.window(series_id), extra]).sort_index()
        self._full[series_id] = full
        self._windows[series_id] = window


def _resolve_vix_series(reader: _AsOfReader) -> str | None:
    """VIX close series for ``vix_level``/``vix_percentile`` at ``reader.as_of``.

    :data:`VIX_SERIES` whenever its known-at compute window has the
    ``min_history`` those dimensions need, else :data:`VIX_FALLBACK_SERIES`
    under the same test (its window holds only finite closes, see
    :data:`FINITE_ONLY_SERIES`, so the count is of numerically usable
    history), else ``None``. The fallback is therefore read only
    when the VIXCLS dimensions would otherwise be unavailable; a vector whose
    VIXCLS is usable is computed exactly as before and never touches
    CBOE:VIX.
    """
    vix_dims = [d for d in STATE_DIMENSIONS if d.series_id == VIX_SERIES]
    if not vix_dims:
        return None
    need = max(d.min_history for d in vix_dims)
    for series_id in (VIX_SERIES, VIX_FALLBACK_SERIES):
        if len(reader.window(series_id)) >= need:
            return series_id
    return None


def _vix_publication_horizon(as_of: date) -> date:
    """Latest weekday close VIXCLS is modeled as published by the end of ``as_of``."""
    lag = PUBLICATION_LAGS[VIX_SERIES]
    horizon = as_of
    while horizon.weekday() >= 5 or lag.known_dates([horizon])[0] > as_of:
        horizon -= timedelta(days=1)
    return horizon


def _fill_vix_trailing_gap(reader: _AsOfReader) -> int:
    """Fill VIXCLS's trailing gap from CBOE:VIX; return the number of dates filled.

    Applies only when VIXCLS's last known close is older than
    :func:`_vix_publication_horizon`, so an on-time VIXCLS is left exactly as
    read (CBOE:VIX is not read at all, except the day after a federal holiday
    the horizon still counts, where nothing can be filled). Fills dates
    strictly after that close and no later than the horizon, from CBOE:VIX
    closes known at ``as_of``. Dates VIXCLS has are never touched. Only
    finite Cboe closes are used: a NaN or infinite backup value fills nothing
    for its date, and when nothing finite remains the series and its label
    stay plain VIXCLS.
    """
    vixcls = reader.window(VIX_SERIES)
    if vixcls.empty:
        return 0
    last = vixcls.index[-1]
    horizon = _vix_publication_horizon(reader.as_of)
    if last >= horizon:
        return 0
    cboe = reader.window(VIX_FALLBACK_SERIES)
    fill = cboe[(cboe.index > last) & (cboe.index <= horizon)]
    fill = fill[np.isfinite(fill.to_numpy())]
    if fill.empty:
        return 0
    reader.append(VIX_SERIES, fill)
    return len(fill)


def _fetch_resolved_spy_full(engine: Engine, as_of: date, cutoff: date) -> pd.Series | None:
    """Best-effort PIT read of the resolved ``spy_full`` feature.

    Mirrors ``alpha_research.realized_alpha.resolve_spy_feature`` +
    ``load_price_path`` (the re-resolve's own PIT reader, through
    ``store.pit.PITStore`` — ``LATEST_AS_OF``, retraction-aware). Returns
    ``None`` — never raises — when ``feature_registry``/``resolved_series``
    aren't there yet (the re-resolve hasn't landed) or the query fails for
    any other reason, so the caller can degrade to the raw YF series
    instead of crashing or assuming infra state this module can't verify.
    """
    try:
        from alpha_research.realized_alpha import load_price_path, resolve_spy_feature

        feature_id, _name = resolve_spy_feature(engine)
        series = load_price_path(engine, feature_id, cutoff, as_of, as_of)
    except Exception as exc:
        log.debug("state_vector: resolved spy_full unavailable ({e}); falling back to raw YF series", e=str(exc))
        return None
    if series is None or series.empty:
        return None
    idx = [d.date() if hasattr(d, "date") else d for d in series.index]
    return pd.Series(series.to_numpy(dtype=float), index=idx, dtype=float).sort_index()


def _fetch_spy_prices(engine: Engine, as_of: date, lookback_days: int = 504) -> tuple[pd.Series, str | None]:
    """SPY close series for momentum/RSI computation, PIT-correct.

    Prefers the resolved ``spy_full`` feature (the post re-resolve price
    basis, with retractions honoured, ``release_date <= as_of``) so
    momentum/RSI agree with the rest of the platform. Falls back to the raw
    ``YF:SPY:close`` observation series when the resolved feature isn't
    available at ``as_of``: SUCCESS-only, ``source="yfinance"`` per
    ``store.observations``'s mixed-source rule, and point-in-time through
    ``read_window_known_at`` with :data:`SPY_CLOSE_LAG` — a close pulled by
    ``as_of`` (latest such vintage), or a backfilled close whose session
    ended by ``as_of`` (earliest pulled vintage). A vintage pulled after
    ``as_of`` (a re-pull, a basis change, a repaired contamination) never
    replaces what a past read saw (E1-V1). Returns ``(empty series, None)``
    — an honest "unavailable", not a crash or a silent stale read — when
    neither path has data.

    Returns ``(prices, price_basis)`` where ``price_basis`` is
    ``"spy_full"``, ``"YF:SPY:close"``, or ``None``.
    """
    cutoff = as_of - timedelta(days=lookback_days)

    resolved = _fetch_resolved_spy_full(engine, as_of, cutoff)
    if resolved is not None and not resolved.empty:
        return resolved, "spy_full"

    try:
        with engine.connect() as conn:
            obs = read_window_known_at(
                conn, "YF:SPY:close", as_of=as_of, lag=SPY_CLOSE_LAG, source="yfinance", start=cutoff,
            )
    except Exception as exc:
        log.debug("state_vector: raw SPY:close fallback failed: {e}", e=str(exc))
        return pd.Series(dtype=float), None
    if not obs:
        return pd.Series(dtype=float), None
    series = pd.Series({o.obs_date: o.value for o in obs}, dtype=float).sort_index()
    return series, "YF:SPY:close"


def _percentile_rank(series: pd.Series, window: int) -> float | None:
    """Percentile rank of latest value within rolling window."""
    if len(series) < 20:
        return None
    tail = series.iloc[-min(window, len(series)):]
    current = tail.iloc[-1]
    return float((tail < current).sum() / len(tail))


def _rolling_slope(series: pd.Series, window: int) -> float | None:
    """OLS slope of last `window` observations, normalized by mean."""
    tail = series.dropna().iloc[-min(window, len(series)):]
    if len(tail) < 10:
        return None
    x = np.arange(len(tail), dtype=float)
    y = tail.values
    mean_y = np.mean(y)
    if abs(mean_y) < 1e-8:
        mean_y = 1.0
    slope = np.polyfit(x, y, 1)[0]
    return float(slope / abs(mean_y))


def _diff(series: pd.Series, periods: int) -> float | None:
    """Difference between latest and value `periods` obs ago."""
    if len(series) < periods + 1:
        return None
    return float(series.iloc[-1] - series.iloc[-periods - 1])


def _pct_change(series: pd.Series, periods: int) -> float | None:
    """Percent change over `periods` observations."""
    if len(series) < periods + 1:
        return None
    prev = series.iloc[-periods - 1]
    if abs(prev) < 1e-8:
        return None
    return float((series.iloc[-1] - prev) / abs(prev))


def _rsi(prices: pd.Series, period: int = 14) -> float | None:
    """RSI-14 from price series."""
    if len(prices) < period + 1:
        return None
    delta = prices.diff().dropna()
    if len(delta) < period:
        return None
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    last_gain = gain.iloc[-1]
    last_loss = loss.iloc[-1]
    if last_loss == 0:
        return 100.0
    rs = last_gain / last_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _ma_ratio(prices: pd.Series, fast: int = 50, slow: int = 200) -> float | None:
    """Ratio of fast MA to slow MA."""
    if len(prices) < slow:
        return None
    fast_ma = prices.iloc[-fast:].mean()
    slow_ma = prices.iloc[-slow:].mean()
    if abs(slow_ma) < 1e-8:
        return None
    return float(fast_ma / slow_ma)


def _get_crossref_score(engine: Engine, as_of: date) -> float | None:
    """Mean absolute divergence z-score from cross-reference checks."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT AVG(ABS(divergence_zscore)), COUNT(*) "
                "FROM cross_reference_checks "
                "WHERE checked_at::date <= :as_of "
                "AND checked_at::date >= :as_of - INTERVAL '7 days'"
            ),
            {"as_of": as_of},
        ).fetchone()
    if row is None or row[1] == 0:
        return None
    return float(row[0]) if row[0] is not None else None


# ── Insider sentiment (Form 4), point-in-time ────────────────────────────
#
# ``INSIDER:{ticker}:{insider_name}:{BUY|SELL}`` rows are dated by the
# *transaction* date (ingestion/altdata/insider_filings.py); the filing date
# rides in ``raw_payload.filing_date``. A Form 4 is due two business days
# after the trade and late filers take weeks or years, so ``obs_date <=
# as_of`` alone counts filings nobody could see at ``as_of`` (E1-V2; on
# griddb every pre-2026 INSIDER row was filed and pulled in 2026).
#
# A row is known by the end of ``as_of`` (UTC) if it was pulled by then, or
# if it was filed by then under VS1's convention
# (``analysis.panel_insider_density.filing_known_at``: filing date 22:00
# America/New_York, the Reg S-T 13(a)(4) dissemination cutoff). The
# convention is restated here rather than imported so the API path does not
# import the research harness; ``tests/test_regime_insider_known_at.py``
# pins the two to each other.
_FORM4_KNOWN_AT_LOCAL = time(22, 0)
_NEW_YORK = ZoneInfo("America/New_York")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_SEC_DATE = re.compile(r"^\d{2}-[A-Za-z]{3}-\d{4}$")

# One round trip per call (PR #713 review: ~1,548 distinct INSIDER:* series
# are active in a 30-day window; the per-series loop this replaced made one
# query each). SUCCESS-only and bounded to [cutoff, as_of] by transaction
# date; availability, vintage choice and the mixed-source rule are applied
# in Python by ``_insider_known_rows`` so they stay portable and testable.
# The filing date is read from the JSON payload with each dialect's own
# accessor (``raw_payload`` is JSONB on Postgres, JSON text on SQLite).
_INSIDER_ROWS_SQL_PG = text(
    "SELECT series_id, obs_date, value, pull_timestamp, source_id,"
    "       raw_payload->>'filing_date' AS filing_date"
    "  FROM raw_series"
    " WHERE series_id LIKE :insider_pat AND pull_status = 'SUCCESS'"
    "   AND obs_date >= :cutoff AND obs_date <= :as_of"
)
_INSIDER_ROWS_SQL_SQLITE = text(
    "SELECT series_id, obs_date, value, pull_timestamp, source_id,"
    "       json_extract(raw_payload, '$.filing_date') AS filing_date"
    "  FROM raw_series"
    " WHERE series_id LIKE :insider_pat AND pull_status = 'SUCCESS'"
    "   AND obs_date >= :cutoff AND obs_date <= :as_of"
)


def _form4_known_at(filing_date: Any) -> datetime | None:
    """UTC instant a Form 4 filed on ``filing_date`` is public (VS1 convention).

    Accepts a ``date``/``datetime`` or the ISO (``YYYY-MM-DD[...]``) and SEC
    data-set (``DD-MON-YYYY``) strings VS1's ``parse_dates`` accepts.
    ``None`` when there is no usable filing date: such a row is visible only
    through its pull.
    """
    if filing_date is None:
        return None
    if isinstance(filing_date, datetime):
        d = filing_date.date()
    elif isinstance(filing_date, date):
        d = filing_date
    else:
        s = str(filing_date).strip()
        try:
            if _ISO_DATE.match(s):
                d = date.fromisoformat(s[:10])
            elif _SEC_DATE.match(s):
                d = datetime.strptime(s.upper(), "%d-%b-%Y").date()
            else:
                return None
        except ValueError:
            return None
    local = datetime.combine(d, _FORM4_KNOWN_AT_LOCAL, tzinfo=_NEW_YORK)
    return local.astimezone(timezone.utc)


def _pulled_at_utc(pull_ts: Any) -> datetime | None:
    """``pull_timestamp`` as an aware UTC datetime (naive = UTC, as griddb runs Etc/UTC)."""
    if pull_ts is None:
        return None
    if not isinstance(pull_ts, datetime):
        try:
            pull_ts = datetime.fromisoformat(str(pull_ts))
        except ValueError:
            return None
    if pull_ts.tzinfo is None:
        return pull_ts.replace(tzinfo=timezone.utc)
    return pull_ts.astimezone(timezone.utc)


def _insider_known_rows(rows: list[tuple], as_of: date) -> list[tuple[str, float]]:
    """``(series_id, value)`` per ``(series_id, obs_date)`` known by the end of ``as_of``.

    ``rows`` are ``(series_id, obs_date, value, pull_timestamp, source_id,
    filing_date)``. A vintage is visible if it was pulled, or filed
    (:func:`_form4_known_at`), before 00:00 UTC of ``as_of + 1``. Per
    ``(series_id, obs_date)``: the latest vintage pulled by then; otherwise
    the earliest-pulled vintage visible through its filing date (the same
    pulled-else-earliest rule as ``store.observations.read_window_known_at``).
    A series_id whose known vintages (every vintage pulled by ``as_of``, or
    the earliest-pulled ones of a filing-visible date) span more than one
    source is dropped entirely (fail closed, as ``MixedSourceError``); a
    second source re-pulling a filing after ``as_of`` cannot drop a series
    from a past read.
    """
    end = datetime.combine(as_of + timedelta(days=1), time(0), tzinfo=timezone.utc)
    never = datetime.max.replace(tzinfo=timezone.utc)
    # (series_id, obs_date) -> [(pulled_by_end, pulled_at, value, source_id)]
    groups: dict[tuple[str, str], list[tuple[bool, datetime, float, Any]]] = {}
    sources: dict[str, set] = {}
    filed: dict[Any, datetime | None] = {}  # a few hundred distinct filing dates per window
    for series_id, obs_date, value, pull_ts, source_id, filing_date in rows:
        if value is None:
            continue
        pulled_at = _pulled_at_utc(pull_ts)
        pulled = pulled_at is not None and pulled_at < end
        if filing_date not in filed:
            filed[filing_date] = _form4_known_at(filing_date)
        filed_at = filed[filing_date]
        if not pulled and not (filed_at is not None and filed_at < end):
            continue  # not public at as_of
        key = (str(series_id), str(obs_date)[:10])
        groups.setdefault(key, []).append((pulled, pulled_at or never, float(value), source_id))

    chosen: dict[tuple[str, str], float] = {}
    for key, vintages in groups.items():
        proven = [v for v in vintages if v[0]]
        if proven:
            pick = max(proven, key=lambda v: (v[1], v[2]))  # latest pulled by as_of
            known = proven  # every vintage pulled by as_of
        else:
            pick = min(vintages, key=lambda v: (v[1], v[2]))  # earliest vintage filed by as_of
            known = [v for v in vintages if v[1] == pick[1]]  # sources tied at that pull
        chosen[key] = pick[2]
        # As in read_window_known_at: only the vintages a past read actually
        # rests on take part in the mixed-source check, so a second source
        # re-pulling an already-public filing after as_of cannot drop the series.
        sources.setdefault(key[0], set()).update(v[3] for v in known)

    return [(sid, value) for (sid, _obs), value in sorted(chosen.items()) if len(sources[sid]) <= 1]


def _get_insider_sentiment(engine: Engine, as_of: date) -> float | None:
    """Net insider sentiment from SEC Form 4 filings (30d window), PIT.

    One query reads every ``INSIDER:{ticker}:{insider_name}:{BUY|SELL}``
    row with a transaction date in ``[as_of - 30d, as_of]`` (SUCCESS-only);
    :func:`_insider_known_rows` keeps only filings known by the end of
    ``as_of`` (pulled or filed by then, see the section comment), collapses
    vintages and drops mixed-source series; the result is summed by the
    ``:BUY``/``:SELL`` suffix. A filing made or pulled after ``as_of`` never
    changes a past value (E1-V2).
    """
    cutoff = as_of - timedelta(days=30)
    try:
        with engine.connect() as conn:
            sql = _INSIDER_ROWS_SQL_PG if conn.dialect.name == "postgresql" else _INSIDER_ROWS_SQL_SQLITE
            rows = conn.execute(
                sql, {"insider_pat": "INSIDER:%", "cutoff": cutoff, "as_of": as_of},
            ).fetchall()
    except Exception as exc:
        log.debug("insider sentiment: batched read failed: {e}", e=str(exc))
        return None

    buy_vol = 0.0
    sell_vol = 0.0
    for series_id, value in _insider_known_rows([tuple(r) for r in rows], as_of):
        if series_id.endswith(":BUY"):
            buy_vol += value
        elif series_id.endswith(":SELL"):
            sell_vol += value
    total = buy_vol + sell_vol
    if total == 0:
        return None
    return float((buy_vol - sell_vol) / total)  # -1 to +1


# ── Normalization stats ──────────────────────────────────────────────────

def _get_normalization_stats(
    engine: Engine, as_of: date, reader: _AsOfReader | None = None,
) -> dict[str, tuple[float, float]]:
    """Mean and std for each series z-scored in the state vector, as of ``as_of``.

    Point-in-time: computed only from observations known by the end of
    ``as_of`` (the same known-at read as the dimension values), over a
    rolling :data:`NORM_LOOKBACK_DAYS` window ending at ``as_of``. This keeps
    the original design's 10,000-day window but anchors it at ``as_of``
    instead of the run date (``date.today()``), so a historical row's
    z-scores no longer depend on data after ``as_of`` or on when the job
    ran. Rolling rather than expanding because that is what the original
    lookback was; for any as_of before ~2017 it spans all GRID history
    (from 1990) anyway.
    """
    reader = reader or _AsOfReader(engine, as_of)
    stats: dict[str, tuple[float, float]] = {}
    for dim in STATE_DIMENSIONS:
        if dim.series_id.startswith('DERIVED:') or dim.series_id in stats:
            continue
        series_stats = _series_norm_stats(reader, dim.series_id)
        if series_stats is not None:
            stats[dim.series_id] = series_stats
    return stats


def _series_norm_stats(reader: _AsOfReader, series_id: str) -> tuple[float, float] | None:
    """PIT mean/std of one series over the normalization window, or None if too short."""
    series = reader.full(series_id)
    if len(series) < 20:
        return None
    return (float(series.mean()), float(series.std()))


def _zscore_normalize(value: float | None, mean: float, std: float) -> float | None:
    """Z-score normalize a single value."""
    if value is None or std == 0:
        return value
    return (value - mean) / std


# ── Main computation ─────────────────────────────────────────────────────

def compute_state_vector(engine: Engine, as_of: date | None = None) -> StateVector:
    """Compute the macro state vector at a specific date with availability bounds.

    Each macro dimension is read point-in-time (only observations known by
    the end of ``as_of``, see :func:`_fetch_series`), transformed, and the
    ``raw`` ones z-scored against PIT stats as of ``as_of``
    (:func:`_get_normalization_stats`). Backfilled values remain revised vintages,
    not historically known first releases. The nightly job
    (``get_or_compute_state_vector``) and ``compute_state_vector_series``
    both call this and get the same vector for the same ``as_of``.
    """
    if as_of is None:
        as_of = date.today()

    reader = _AsOfReader(engine, as_of)
    try:
        vix_basis = _resolve_vix_series(reader)
    except Exception as exc:
        # A failed VIXCLS read is not evidence VIXCLS is absent: keep it as
        # the series, so the VIX dims fail below exactly as they did before.
        log.debug("state_vector: VIX basis unresolved for {dt}: {e}", dt=as_of, e=str(exc))
        vix_basis = None
    vix_series = vix_basis or VIX_SERIES
    if vix_basis == VIX_SERIES:
        try:
            if _fill_vix_trailing_gap(reader):
                vix_basis = VIX_GAP_FILLED_BASIS
        except Exception as exc:
            # No fill is the pre-R4 behaviour: VIXCLS as known, however late.
            log.debug("state_vector: VIX gap fill skipped for {dt}: {e}", dt=as_of, e=str(exc))
    norm_stats = _get_normalization_stats(engine, as_of, reader)
    if vix_series != VIX_SERIES:
        # The fallback is z-scored against its own history, never VIXCLS's.
        fallback_stats = _series_norm_stats(reader, vix_series)
        if fallback_stats is not None:
            norm_stats = {**norm_stats, vix_series: fallback_stats}
    spy_prices, price_basis = _fetch_spy_prices(engine, as_of)
    values: list[float | None] = []
    stale: list[str] = []

    for dim in STATE_DIMENSIONS:
        if dim.series_id == VIX_SERIES:
            dim = replace(dim, series_id=vix_series)
        try:
            # Same accepted 2520-day series drives value and stale age.
            series = None
            if not dim.series_id.startswith('DERIVED:'):
                series = reader.window(dim.series_id)
            val = _compute_dimension(reader, dim, as_of, norm_stats, spy_prices, series=series)
            if val is not None and series is not None and not series.empty:
                days_stale = (as_of - series.index[-1]).days
                if days_stale > _stale_threshold_days(dim.series_id):
                    stale.append(dim.name)
            values.append(val)
        except Exception as exc:
            log.debug("Dim {d} failed for {dt}: {e}", d=dim.name, dt=as_of, e=str(exc))
            values.append(None)

    non_null = sum(1 for v in values if v is not None)
    completeness = non_null / len(values) if values else 0.0

    return StateVector(
        as_of_date=as_of,
        values=tuple(values),
        completeness=completeness,
        stale_dimensions=tuple(stale),
        price_basis=price_basis,
        cached=False,
        vix_basis=vix_basis,
    )


def _compute_dimension(
    reader: _AsOfReader,
    dim: DimensionSpec,
    as_of: date,
    norm_stats: dict[str, tuple[float, float]],
    spy_prices: pd.Series,
    *,
    series: pd.Series | None = None,
) -> float | None:
    """Compute a dimension, reusing its accepted input when supplied."""
    if not isinstance(reader, _AsOfReader):
        reader = _AsOfReader(reader, as_of)
    engine = reader.engine

    # ── Derived dimensions (computed from other series) ──
    if dim.series_id == 'DERIVED:T5YIE':
        series = reader.window('T5YIE')
        if series.empty:
            return None
        val = float(series.iloc[-1])
        stats = norm_stats.get('T5YIE')
        return _zscore_normalize(val, stats[0], stats[1]) if stats else val

    if dim.series_id == 'DERIVED:SPY_MA_RATIO':
        return _ma_ratio(spy_prices, 50, 200)

    if dim.series_id == 'DERIVED:SPY_RSI':
        rsi_val = _rsi(spy_prices)
        return (rsi_val - 50.0) / 25.0 if rsi_val is not None else None  # normalize to ~[-2, 2]

    if dim.series_id == 'DERIVED:REAL_FF':
        dff = reader.window('DFF')
        t5yie = reader.window('T5YIE')
        if dff.empty or t5yie.empty:
            return None
        return float(dff.iloc[-1] - t5yie.iloc[-1])

    if dim.series_id == 'DERIVED:CROSSREF_SCORE':
        return _get_crossref_score(engine, as_of)

    if dim.series_id == 'DERIVED:INSIDER_NET':
        return _get_insider_sentiment(engine, as_of)

    # ── Standard series dimensions ──
    if series is None:
        series = reader.window(dim.series_id)
    if series.empty or len(series) < dim.min_history:
        return None

    # Apply transform
    if dim.transform == 'raw':
        val = float(series.iloc[-1])
    elif dim.transform == 'percentile_rank':
        val = _percentile_rank(series, int(dim.transform_param))
        return val  # already 0-1, no z-score normalization needed
    elif dim.transform == 'slope':
        val = _rolling_slope(series, int(dim.transform_param))
        return val  # already normalized by mean
    elif dim.transform == 'diff':
        val = _diff(series, int(dim.transform_param))
    elif dim.transform == 'pct_change':
        val = _pct_change(series, int(dim.transform_param))
        return val  # already a ratio
    else:
        val = float(series.iloc[-1])

    if val is None:
        return None

    # Z-score normalize raw values
    stats = norm_stats.get(dim.series_id)
    if stats and dim.transform == 'raw':
        return _zscore_normalize(val, stats[0], stats[1])

    return val


# ── Batch computation ────────────────────────────────────────────────────

def compute_state_vector_series(
    engine: Engine,
    start: date,
    end: date,
    freq_days: int = 5,
) -> list[StateVector]:
    """Compute state vectors at regular intervals over a date range.

    Used for building the historical library.
    """
    vectors: list[StateVector] = []
    current = start
    total = (end - start).days // freq_days
    computed = 0

    while current <= end:
        try:
            sv = compute_state_vector(engine, current)
            if sv.completeness >= MIN_CACHE_COMPLETENESS:
                vectors.append(sv)
            computed += 1
            if computed % 100 == 0:
                log.info("State vectors: {n}/{t} computed", n=computed, t=total)
        except Exception as exc:
            log.debug("State vector failed for {dt}: {e}", dt=current, e=str(exc))
        current += timedelta(days=freq_days)

    log.info("Computed {n} state vectors from {s} to {e}", n=len(vectors), s=start, e=end)
    return vectors


# ── Cache ────────────────────────────────────────────────────────────────

_CACHE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS regime_state_vectors (
    id          BIGSERIAL PRIMARY KEY,
    as_of_date  DATE NOT NULL UNIQUE,
    vector      JSONB NOT NULL,
    completeness DOUBLE PRECISION NOT NULL,
    stale_dims  TEXT[],
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_regime_sv_date
    ON regime_state_vectors (as_of_date DESC);
"""


def _ensure_cache_table(engine: Engine) -> None:
    with engine.begin() as conn:
        for stmt in _CACHE_TABLE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))


def cache_state_vector(engine: Engine, sv: StateVector) -> None:
    """Store a state vector in the cache table.

    The only callers of this function are the nightly job
    (``scripts/run_regime_state_vectors.py``) and, indirectly,
    ``get_or_compute_state_vector(..., persist=True)`` — never a GET route.
    ``price_basis`` rides along inside the existing ``vector`` JSONB column
    under a reserved key rather than a new column, so no migration is
    needed and the 1,927 pre-existing rows (computed before this field
    existed) are read back with ``price_basis=None``, unchanged.
    ``vix_basis`` rides along the same way under ``__vix_basis__``.
    """
    _ensure_cache_table(engine)
    dim_dict = {DIM_NAMES[i]: sv.values[i] for i in range(len(sv.values))}
    if sv.price_basis is not None:
        dim_dict["__price_basis__"] = sv.price_basis
    if sv.vix_basis is not None:
        dim_dict["__vix_basis__"] = sv.vix_basis
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO regime_state_vectors (as_of_date, vector, completeness, stale_dims) "
                "VALUES (:dt, :vec, :comp, :stale) "
                "ON CONFLICT (as_of_date) DO UPDATE SET "
                "vector = EXCLUDED.vector, completeness = EXCLUDED.completeness, "
                "stale_dims = EXCLUDED.stale_dims, computed_at = NOW()"
            ),
            {
                "dt": sv.as_of_date,
                "vec": json.dumps(dim_dict),
                "comp": sv.completeness,
                "stale": list(sv.stale_dimensions),
            },
        )


# Minimum completeness for a vector to be cached, and for a cached row to be
# served instead of recomputed. Same floor load_cached_vectors() and
# compute_state_vector_series() already apply when building the library.
MIN_CACHE_COMPLETENESS = 0.4


def _row_to_state_vector(row: tuple, *, cached: bool) -> StateVector:
    dt, vec_json, comp, stale = row
    vec_dict = vec_json if isinstance(vec_json, dict) else json.loads(vec_json)
    values = tuple(vec_dict.get(name) for name in DIM_NAMES)
    return StateVector(
        as_of_date=dt,
        values=values,
        completeness=comp,
        stale_dimensions=tuple(stale or []),
        price_basis=vec_dict.get("__price_basis__"),
        cached=cached,
        vix_basis=vec_dict.get("__vix_basis__"),
    )


def load_cached_vectors(engine: Engine, min_completeness: float = MIN_CACHE_COMPLETENESS) -> list[StateVector]:
    """Load all cached state vectors from the database. Read-only.

    Deliberately does not call ``_ensure_cache_table`` — this is a GET-path
    reader (``/regime/history`` and the analog library build). A missing
    table means "no vectors yet", not "create it now"; it degrades to an
    empty list rather than issuing DDL. Excludes any future-dated row
    defensively, even if one exists (it never should — see
    ``get_or_compute_state_vector``).
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT as_of_date, vector, completeness, stale_dims "
                    "FROM regime_state_vectors "
                    "WHERE completeness >= :mc AND as_of_date <= :today "
                    "ORDER BY as_of_date"
                ),
                {"mc": min_completeness, "today": date.today()},
            ).fetchall()
    except Exception as exc:
        log.debug("load_cached_vectors: cache table unavailable: {e}", e=str(exc))
        return []

    return [_row_to_state_vector(row, cached=True) for row in rows]


def _read_cached_row(engine: Engine, as_of: date, today: date) -> tuple | None:
    """Best-effort read of one cached row for ``as_of``, or ``None``.

    Never issues DDL: a missing table (or any other read failure) is
    treated as "no cached row", not created. ``as_of_date <= :today`` is a
    defensive belt-and-suspenders filter — the caller already refuses to
    cache or serve a future ``as_of`` — so a future-dated row can never be
    served even if one somehow ended up in the table.
    """
    try:
        with engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT as_of_date, vector, completeness, stale_dims "
                    "FROM regime_state_vectors "
                    "WHERE as_of_date = :dt AND as_of_date <= :today"
                ),
                {"dt": as_of, "today": today},
            ).fetchone()
    except Exception as exc:
        log.debug("state_vector: cache read unavailable: {e}", e=str(exc))
        return None


def get_or_compute_state_vector(
    engine: Engine,
    as_of: date | None = None,
    force_recompute: bool = False,
    persist: bool = True,
) -> StateVector:
    """Get from cache, or compute fresh.

    persist: True (default) is the nightly job's contract
        (``scripts/run_regime_state_vectors.py``) — it may ensure the cache
        table exists and, if the computed vector clears
        ``MIN_CACHE_COMPLETENESS``, write it. GET-triggered callers (the
        ``/regime`` and ``/regime/analogs`` routes) MUST pass
        ``persist=False``: with that, this function issues no DDL and calls
        ``cache_state_vector`` under no circumstance. A cache hit is still
        served (``cached=True``); a miss is computed in memory and returned
        with ``cached=False`` — never written.

    A future ``as_of`` (no observations exist yet) is always computed in
    memory only, regardless of ``persist`` — it is never read from or
    written to the cache.
    """
    today = date.today()
    if as_of is None:
        as_of = today

    if as_of > today:
        log.warning(
            "state_vector: as_of {d} is in the future — computing in memory only, never cached",
            d=as_of,
        )
        return compute_state_vector(engine, as_of)

    if not force_recompute:
        if persist:
            _ensure_cache_table(engine)
        row = _read_cached_row(engine, as_of, today)
        if row is not None and row[2] is not None and row[2] >= MIN_CACHE_COMPLETENESS:
            return _row_to_state_vector(row, cached=True)

    sv = compute_state_vector(engine, as_of)
    # Never pin a mostly-empty vector (e.g. every dimension query timed out)
    # as the day's cached state: it would be served for the rest of the day
    # instead of being recomputed, and load_cached_vectors() filters it out
    # of the analog library anyway.
    if sv.completeness < MIN_CACHE_COMPLETENESS:
        log.warning(
            "State vector for {d} only {c:.0%} complete — not cached",
            d=as_of, c=sv.completeness,
        )
        return sv
    if not persist:
        return sv
    try:
        cache_state_vector(engine, sv)
    except Exception as exc:
        log.warning("Failed to cache state vector: {e}", e=str(exc))
    return sv
