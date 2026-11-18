"""Anomaly report: what each flagged feature is, whether it is real, how big the move is, what followed before.

The associations anomaly scan (``api/routers/associations.py::_compute_anomalies``) flags a feature when its
latest value sits more than 2.5 standard deviations from its one-year mean. On its own that reading cannot
tell an unadjusted stock split, a broken feed and a real market move apart, and the e-mail built from it
named up to five features and their z-scores and nothing else. Measured on 2026-11-13: the three "extreme"
features were ITA (a 3-for-1 split on 2026-11-05 the series was not re-based for) and two readings from an
options feed returning about 2 % of normal volume since 2026-11-09; NVDA, down 25 % in 13 sessions, ranked
sixteenth.

This module turns a scan into a report. Per flagged feature:

* **identity**: readable label, ticker, kind (price / options volume / options-derived / other);
* **verdict**: ``data_fault`` (split-shaped jump, feed collapse), ``stale`` (no observation for
  several of its own reporting intervals; the scan is reading a carried-forward value), ``suspect``
  (computed from a feed that is in fault), ``sharp_move`` (the 1- or 5-session change is itself unusual), ``new_extreme`` (crossed
  the threshold within the last :data:`NEW_EXTREME_SESSIONS` sessions), ``stretched`` (has sat beyond
  the threshold for longer; a one-year trend, not a new event) or ``faded`` (back inside the threshold
  on its own latest observation);
* **level**: read at the feature's own latest observation against its own trailing year. The scan reads
  every feature on the last date all of them share, which can trail a feature's newest value by days;
* **move**: 1 / 5 / 20-session change and how unusual each is against the feature's own trailing changes
  (``intelligence.regime.baseline``: robust z, the current change never in its own baseline);
* **excursion**: sessions beyond the threshold and the date it started;
* **range** (prices): the one-year high and low and how far the latest value is from each;
* **history**: the prior times the feature crossed the same threshold in the same direction, and what the
  linked tradable price did 5 and 20 sessions later, next to the same statistic for every window. A
  sharp move also gets ``move_history``: the prior times it moved at least 80 % as far over the same span.
  Descriptive base rates with their n shown; not tested rules.

Pure functions over DataFrames; the caller does the PIT reads. The output is a JSON-serialisable dict
(``schema`` :data:`SCHEMA`) stored with the scan; ``alerts.email.send_anomaly_report`` renders it.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from alpha_research.data.split_adjuster import detect_splits
from intelligence.regime.baseline import standardize

SCHEMA = "anomaly-report/1"

#: The scan's own threshold; an excursion is "beyond" this many standard deviations.
SIGMA_THRESHOLD = 2.5
#: Level z above which a stretched (not new) reading still makes the headline set.
HEADLINE_Z = 3.5
#: A 1- or 5-session change this unusual (robust z against the feature's own changes) is a sharp move.
SHARP_CHANGE_Z = 2.5
#: An excursion this many sessions old or younger is "new"; older is "stretched".
NEW_EXTREME_SESSIONS = 5

#: The scan's window (calendar days) and the fewest observations a level reading needs.
WINDOW_DAYS = 252
LEVEL_MIN_OBS = 30

CHANGE_HORIZONS = (1, 5, 20)
CHANGE_WINDOW, CHANGE_MIN_OBS = 252, 60

# Feed collapse: a count series (options volume, open interest) at under a fifth of its own normal, for at
# least half of a group of at least five siblings, is the feed and not the market.
COLLAPSE_RATIO = 0.2
COLLAPSE_GROUP_SHARE = 0.5
COLLAPSE_MIN_MEMBERS = 5
COLLAPSE_BASELINE_OBS = 120
COLLAPSE_RECENT_OBS = 15
COLLAPSE_MIN_HISTORY = 40

#: Stale: the latest observation is older than the newest in the report by more than this many of the
#: feature's own typical gaps (and at least this many calendar days).
STALE_GAPS = 3
STALE_MIN_DAYS = 4

#: Ratios companies and funds actually split by. A persisting jump by any other integer factor is read
#: as a change of source or unit, not a split.
COMMON_SPLIT_RATIOS = frozenset({2, 3, 4, 5, 6, 7, 8, 10, 15, 20, 25, 30, 40, 50})

#: A split-shaped jump must land within this fraction of an integer ratio.
SPLIT_RATIO_TOLERANCE = 0.08

# History read.
EPISODE_WINDOW, EPISODE_MIN_OBS = 252, 120
EPISODE_MIN_GAP = 20
FORWARD_HORIZONS = (5, 20)
MIN_EPISODES_TO_READ = 5
#: A prior move counts as a precedent when it was at least this fraction of the current one.
MOVE_MATCH = 0.8
MAX_EPISODES_LISTED = 8

COUNT_SUFFIXES = ("_opt_vol", "_total_oi")
OPTIONS_DERIVED_SUFFIXES = (
    "_max_pain", "_oi_conc", "_pcr", "_iv_atm", "_term_slope", "_iv_25d_put", "_iv_25d_call", "_iv_skew",
)
PRICE_SUFFIXES = ("_usd_full", "_full", "_close")
PRICE_FAMILIES = frozenset({"equity", "breadth", "commodity", "fx", "crypto", "credit"})
DEFAULT_TARGET = "spy_full"

_SUFFIX_LABEL = {
    "_opt_vol": "options volume", "_total_oi": "open interest", "_max_pain": "max-pain strike",
    "_oi_conc": "open-interest concentration", "_pcr": "put/call ratio", "_iv_atm": "at-the-money implied vol",
    "_term_slope": "implied-vol term slope", "_iv_25d_put": "25-delta put implied vol",
    "_iv_25d_call": "25-delta call implied vol", "_iv_skew": "implied-vol skew",
}
_GROUP_LABEL = {"_opt_vol": "Options volume feed", "_total_oi": "Open interest feed"}


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

def _suffix(name: str, suffixes: tuple[str, ...]) -> str | None:
    low = name.lower()
    for s in suffixes:
        if low.endswith(s) and len(low) > len(s):
            return s
    return None


def feature_kind(name: str, family: str | None) -> str:
    """``price`` | ``count`` | ``options_derived`` | ``other`` from the feature's name and family."""
    if _suffix(name, COUNT_SUFFIXES):
        return "count"
    if _suffix(name, OPTIONS_DERIVED_SUFFIXES):
        return "options_derived"
    if _suffix(name, PRICE_SUFFIXES):
        return "price"
    if (family or "").lower() in PRICE_FAMILIES and "_" not in name:
        return "price"
    return "other"


def feature_ticker(name: str) -> str | None:
    """The ticker a per-symbol feature belongs to (``brk_b_opt_vol`` -> ``BRK-B``), else ``None``."""
    s = _suffix(name, COUNT_SUFFIXES + OPTIONS_DERIVED_SUFFIXES + PRICE_SUFFIXES)
    if s is None:
        return None
    return name[: -len(s)].replace("_", "-").upper()


def feature_label(name: str, description: str | None, kind: str) -> str:
    """A label a person can read: ``NVDA price``, ``BLK options volume``, else the registry description."""
    ticker = feature_ticker(name)
    s = _suffix(name, tuple(_SUFFIX_LABEL))
    if ticker and s:
        return f"{ticker} {_SUFFIX_LABEL[s]}"
    desc = (description or "").strip()
    if kind == "price":
        if ticker:
            return f"{ticker} price"
        return f"{desc} ({name.upper()})" if desc else name.upper()
    return desc or name


def _norm(name: str) -> str:
    return name.lower().replace("-", "_")


def linked_price_feature(name: str, kind: str, columns: Any) -> str | None:
    """The tradable price series the history read is measured on.

    A price feature is its own target; a per-ticker feature maps to ``{ticker}_full``; anything else is
    read against :data:`DEFAULT_TARGET`. ``None`` when that series is not among ``columns``.
    """
    by_norm = {_norm(str(c)): str(c) for c in columns}
    if kind == "price":
        return by_norm.get(_norm(name))
    ticker = feature_ticker(name)
    if ticker:
        hit = by_norm.get(_norm(ticker) + "_full")
        if hit:
            return hit
    return by_norm.get(DEFAULT_TARGET)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _clean(series: pd.Series | None) -> pd.Series:
    """Own observations only: numeric, finite, date-indexed, sorted, no fill."""
    if series is None:
        return pd.Series(dtype="float64")
    s = pd.to_numeric(series, errors="coerce").astype("float64")
    s = s[np.isfinite(s)]
    if s.empty:
        return s
    s.index = pd.to_datetime(s.index)
    return s[~s.index.duplicated(keep="last")].sort_index()


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _num(value: Any, digits: int = 4) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return round(f, digits) if math.isfinite(f) else None


def _column(frame: pd.DataFrame | None, name: str) -> pd.Series | None:
    if frame is None or name not in frame.columns:
        return None
    col = frame[name]
    return col.iloc[:, -1] if isinstance(col, pd.DataFrame) else col


# ---------------------------------------------------------------------------
# Data-fault checks
# ---------------------------------------------------------------------------

def split_like_jump(series: pd.Series) -> dict[str, Any] | None:
    """The most recent split-shaped jump in a price series, or ``None``.

    Uses ``alpha_research.data.split_adjuster.detect_splits`` (a one-session drop past 40 % to an integer
    ratio whose new level persists) on the series for forward splits and on its reciprocal for reverse
    splits. A hit says the series changed basis mid-window, so every level reading on it is wrong.
    ``shape`` is ``split`` for a ratio in :data:`COMMON_SPLIT_RATIOS` and ``basis_break`` for any other.
    """
    s = _clean(series)
    s = s[s > 0]
    if len(s) < 3:
        return None
    hits: list[dict[str, Any]] = []
    for kind, probe in (("split", s), ("reverse_split", 1.0 / s)):
        for hit in detect_splits(probe.rename(str(series.name or "series"))):
            idx = s.index.get_loc(hit["date"])
            before, after = float(s.iloc[idx - 1]), float(s.iloc[idx])
            observed = before / after if kind == "split" else after / before
            # A real split lands on its ratio give or take the session's own move; a 45 % crash does not.
            if abs(observed / int(hit["ratio"]) - 1.0) > SPLIT_RATIO_TOLERANCE:
                continue
            hits.append({
                "kind": kind, "date": _iso(hit["date"]), "ratio": int(hit["ratio"]),
                # 3-for-1 is a corporate action; 38x or 2274x is the series changing what it measures
                # (an ETF price replaced by its index level, a pair replaced by another pair).
                "shape": "split" if int(hit["ratio"]) in COMMON_SPLIT_RATIOS else "basis_break",
                "before": _num(before), "after": _num(after),
            })
    if not hits:
        return None
    return max(hits, key=lambda h: h["date"])


def feed_collapses(recent: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Count-series groups whose members mostly collapsed together: ``{suffix: fault}``.

    For each suffix in :data:`COUNT_SUFFIXES`, every member's normal is the median of its own
    :data:`COLLAPSE_BASELINE_OBS` observations before the last :data:`COLLAPSE_RECENT_OBS`. A member is
    collapsed on a date when its value is under :data:`COLLAPSE_RATIO` of that normal. The group is in
    fault when at least :data:`COLLAPSE_GROUP_SHARE` of at least :data:`COLLAPSE_MIN_MEMBERS` members are
    collapsed on the latest date; the onset is the first date of that unbroken run.
    """
    out: dict[str, dict[str, Any]] = {}
    if recent is None or recent.empty:
        return out
    frame = recent.copy()
    frame.index = pd.to_datetime(frame.index)
    frame = frame.sort_index()
    for suffix in COUNT_SUFFIXES:
        cols = [c for c in frame.columns if _suffix(str(c), (suffix,))]
        if len(cols) < COLLAPSE_MIN_MEMBERS:
            continue
        block = frame[cols].apply(pd.to_numeric, errors="coerce")
        block = block.dropna(how="all")
        if len(block) < COLLAPSE_MIN_HISTORY + COLLAPSE_RECENT_OBS:
            continue
        base = block.iloc[:-COLLAPSE_RECENT_OBS].tail(COLLAPSE_BASELINE_OBS)
        normal = base.median()
        enough = base.notna().sum() >= COLLAPSE_MIN_HISTORY
        usable = [c for c in cols if bool(enough[c]) and float(normal[c]) > 0]
        if len(usable) < COLLAPSE_MIN_MEMBERS:
            continue
        # Carry a member's last reading forward inside the tail: a symbol that has not reported yet today
        # is still whatever it last was (the scan itself forward-fills the same way).
        tail = block[usable].tail(COLLAPSE_RECENT_OBS).ffill()
        ratio = tail.divide(normal[usable], axis=1)
        collapsed = ratio < COLLAPSE_RATIO
        share = collapsed.sum(axis=1) / tail.notna().sum(axis=1).replace(0, np.nan)
        if not len(share) or not (share.iloc[-1] >= COLLAPSE_GROUP_SHARE):
            continue
        onset = share.index[-1]
        for dt, value in zip(reversed(share.index), reversed(share.to_numpy())):
            if not (value >= COLLAPSE_GROUP_SHARE):
                break
            onset = dt
        latest_ratio = ratio.iloc[-1].dropna()
        out[suffix] = {
            "id": f"feed:{suffix.strip('_')}",
            "kind": "feed_collapse",
            "suffix": suffix,
            "label": _GROUP_LABEL.get(suffix, f"{suffix.strip('_')} feed"),
            "onset": _iso(onset),
            "latest_date": _iso(share.index[-1]),
            "members": len(usable),
            "collapsed": int(collapsed.iloc[-1].sum()),
            "share": _num(share.iloc[-1], 3),
            "median_ratio": _num(latest_ratio.median(), 4) if len(latest_ratio) else None,
            "collapsed_members": [str(c) for c in collapsed.columns[collapsed.iloc[-1].to_numpy()]],
        }
    return out


def staleness(series: pd.Series, newest: pd.Timestamp | None) -> dict[str, Any] | None:
    """How far the feature's latest observation trails ``newest``, judged against its own cadence.

    Returns the fault dict when the gap exceeds :data:`STALE_GAPS` typical gaps (median of the last 20
    gaps between its observations) and :data:`STALE_MIN_DAYS` calendar days; a monthly series a few weeks
    old is not stale, a daily one a week old is.
    """
    s = _clean(series)
    if newest is None or len(s) < 3:
        return None
    gaps = s.index.to_series().diff().dt.days.dropna().tail(20)
    typical = float(gaps.median()) if len(gaps) else 1.0
    behind = int((pd.Timestamp(newest) - s.index[-1]).days)
    if behind <= max(STALE_MIN_DAYS, STALE_GAPS * typical):
        return None
    return {"kind": "stale", "last_obs": _iso(s.index[-1]), "days_behind": behind,
            "typical_gap_days": _num(typical, 1)}


def year_range(series: pd.Series, window_days: int = WINDOW_DAYS) -> dict[str, Any] | None:
    """One-year high and low of a positive series and the latest value's distance from each."""
    s = _clean(series)
    if s.empty:
        return None
    w = s[s.index >= s.index[-1] - pd.Timedelta(days=window_days)]
    w = w[w > 0]
    if len(w) < LEVEL_MIN_OBS:
        return None
    last, high, low = float(w.iloc[-1]), float(w.max()), float(w.min())
    return {"high": _num(high), "high_date": _iso(w.idxmax()), "low": _num(low), "low_date": _iso(w.idxmin()),
            "off_high": _num(last / high - 1.0, 4), "off_low": _num(last / low - 1.0, 4)}


# ---------------------------------------------------------------------------
# Move, excursion, history
# ---------------------------------------------------------------------------

def changes(series: pd.Series, *, as_pct: bool) -> dict[str, dict[str, Any]]:
    """Latest 1 / 5 / 20-session change and its robust z against the feature's own trailing changes."""
    s = _clean(series)
    out: dict[str, dict[str, Any]] = {}
    for h in CHANGE_HORIZONS:
        if len(s) <= h:
            continue
        delta = (s / s.shift(h) - 1.0) if as_pct else (s - s.shift(h))
        delta = delta.dropna()
        delta = delta[np.isfinite(delta)]
        if delta.empty:
            continue
        # The baseline is the changes that ended before this one started, so overlapping windows never
        # share observations with the change being read.
        reading = standardize(float(delta.iloc[-1]), delta.iloc[:-h].tolist(),
                              window=CHANGE_WINDOW, min_obs=CHANGE_MIN_OBS)
        out[f"d{h}"] = {
            "sessions": h,
            "value": _num(delta.iloc[-1], 6),
            "unit": "pct" if as_pct else "abs",
            "from_date": _iso(s.index[-1 - h]),
            "robust_z": _num(reading.robust_z, 2) if reading and reading.robust_z is not None else None,
        }
    return out


def level_reading(series: pd.Series, window_days: int = WINDOW_DAYS) -> dict[str, Any] | None:
    """The feature's latest own observation against its own trailing ``window_days`` calendar days.

    The same definition the scan uses (mean and sample standard deviation over the window, latest value
    included), taken on the feature's own observations instead of the forward-filled shared matrix.
    """
    s = _clean(series)
    if s.empty:
        return None
    window = s[s.index >= s.index[-1] - pd.Timedelta(days=window_days)]
    if len(window) < LEVEL_MIN_OBS:
        return None
    mean, std = float(window.mean()), float(window.std())
    if not math.isfinite(std) or std <= 0:
        return None
    return {"value": float(window.iloc[-1]), "obs_date": _iso(window.index[-1]), "mean": mean, "std": std,
            "zscore": (float(window.iloc[-1]) - mean) / std, "n": int(len(window))}


def excursion(series: pd.Series, mean: float, std: float, z_now: float,
              threshold: float = SIGMA_THRESHOLD) -> dict[str, Any]:
    """How many consecutive latest sessions sit beyond the threshold (scan mean/std), and since when."""
    s = _clean(series)
    if s.empty or not std or not math.isfinite(std) or std <= 0:
        return {"sessions": None, "since": None}
    sign = 1.0 if z_now >= 0 else -1.0
    beyond = (sign * (s - mean) / std) >= threshold
    n, since = 0, None
    for dt, flag in zip(reversed(s.index), reversed(beyond.to_numpy())):
        if not flag:
            break
        n, since = n + 1, dt
    return {"sessions": n, "since": _iso(since)}


def _forward(target: pd.Series, start: pd.Timestamp, horizon: int) -> float | None:
    pos = int(target.index.searchsorted(start, side="left"))
    if pos >= len(target) or pos + horizon >= len(target):
        return None
    base = float(target.iloc[pos])
    if base <= 0:
        return None
    return float(target.iloc[pos + horizon]) / base - 1.0


def _precedents(signal: pd.Series, cut: float, sign: float, target: pd.Series, *,
                current_since: str | None, out: dict[str, Any]) -> dict[str, Any]:
    """Count the prior times ``sign * signal >= cut`` began, and the target's forward returns after each.

    An episode is the first session of a crossing, at least :data:`EPISODE_MIN_GAP` sessions after the
    previous one. The episode being reported is not one of its own precedents: crossings on or after
    ``current_since`` (and any in the unbroken run that ends at the latest session) are left out.
    ``baseline`` is the same statistic over every window of the target from the signal's first date.
    """
    beyond = (sign * signal) >= cut
    starts = beyond & ~beyond.shift(1, fill_value=False)
    flags = beyond.to_numpy()
    current_start = len(flags)          # first position of the unbroken run that ends at the latest session
    while current_start > 0 and flags[current_start - 1]:
        current_start -= 1
    if current_since:
        current_start = min(current_start,
                            int(signal.index.searchsorted(pd.Timestamp(current_since), side="left")))

    episodes: list[dict[str, Any]] = []
    last_pos = -10 ** 9
    positions = {dt: i for i, dt in enumerate(signal.index)}
    for dt in signal.index[starts.to_numpy()]:
        pos = positions[dt]
        if pos - last_pos < EPISODE_MIN_GAP:
            continue
        last_pos = pos
        if pos >= current_start:
            continue
        fwd = {h: _forward(target, dt, h) for h in FORWARD_HORIZONS}
        if all(v is None for v in fwd.values()):
            continue  # too recent to have a forward return
        episodes.append({"date": _iso(dt), "signal": _num(signal.loc[dt], 4),
                         **{f"fwd_{h}": _num(v, 4) for h, v in fwd.items()}})

    horizons: dict[str, dict[str, Any]] = {}
    scope = target[target.index >= signal.index[0]]
    for h in FORWARD_HORIZONS:
        vals = [e[f"fwd_{h}"] for e in episodes if e.get(f"fwd_{h}") is not None]
        base = (scope.shift(-h) / scope - 1.0).dropna()
        horizons[f"fwd_{h}"] = {
            "sessions": h,
            "n": len(vals),
            "median": _num(np.median(vals), 4) if vals else None,
            "share_up": _num(float(np.mean([v > 0 for v in vals])), 3) if vals else None,
            "worst": _num(min(vals), 4) if vals else None,
            "best": _num(max(vals), 4) if vals else None,
            "baseline_n": int(len(base)),
            "baseline_median": _num(base.median(), 4) if len(base) else None,
            "baseline_share_up": _num(float((base > 0).mean()), 3) if len(base) else None,
        }
    return {**out, "available": True, "n": len(episodes), "since": _iso(signal.index[0]),
            "enough": len(episodes) >= MIN_EPISODES_TO_READ,
            "episodes": episodes[-MAX_EPISODES_LISTED:], "horizons": horizons}


def _usable_target(target: pd.Series, target_name: str) -> tuple[pd.Series, str | None]:
    t = _clean(target)
    t = t[t > 0]
    if len(t) < EPISODE_MIN_OBS:
        return t, f"no price history for {target_name}"
    if split_like_jump(t) is not None:
        return t, f"{target_name} has an unadjusted split in its history"
    return t, None


def history_read(feature: pd.Series, target: pd.Series, z_now: float, *, target_name: str,
                 threshold: float = SIGMA_THRESHOLD, current_since: str | None = None) -> dict[str, Any]:
    """Prior times the feature sat this far from its trailing year, and what the target did next.

    The trailing z uses the previous :data:`EPISODE_WINDOW` observations only (the value read is never in
    its own mean or standard deviation). See :func:`_precedents` for how episodes are counted.
    """
    f = _clean(feature)
    out: dict[str, Any] = {"basis": "level", "target": target_name, "cut": threshold,
                           "direction": "above" if z_now >= 0 else "below"}
    if len(f) < EPISODE_MIN_OBS + EPISODE_MIN_GAP:
        return {**out, "available": False,
                "reason": f"history too short ({len(f)} observations since {_iso(f.index[0]) if len(f) else 'n/a'})"}
    t, problem = _usable_target(target, target_name)
    if problem:
        return {**out, "available": False, "reason": problem}
    prior = f.shift(1)
    mean = prior.rolling(EPISODE_WINDOW, min_periods=EPISODE_MIN_OBS).mean()
    std = prior.rolling(EPISODE_WINDOW, min_periods=EPISODE_MIN_OBS).std()
    z = ((f - mean) / std.where(std > 0)).dropna()
    if z.empty:
        return {**out, "available": False, "reason": "history too short for a trailing one-year baseline"}
    return _precedents(z, threshold, 1.0 if z_now >= 0 else -1.0, t, current_since=current_since, out=out)


def move_read(feature: pd.Series, target: pd.Series, change: dict[str, Any], *, as_pct: bool,
              target_name: str) -> dict[str, Any]:
    """Prior times the feature moved at least :data:`MOVE_MATCH` as far over the same number of sessions.

    Magnitude, not z: "fell 19 % or more in 20 sessions" is the same event in a calm year and a wild one,
    and it is what a person means by "the last time this happened".
    """
    f = _clean(feature)
    h = int(change["sessions"])
    now = float(change["value"])
    cut = abs(now) * MOVE_MATCH
    out: dict[str, Any] = {"basis": f"change_{h}", "target": target_name, "cut": _num(cut, 6),
                           "unit": "pct" if as_pct else "abs", "direction": "above" if now >= 0 else "below"}
    if len(f) < EPISODE_MIN_OBS + h or cut <= 0:
        return {**out, "available": False, "reason": f"history too short ({len(f)} observations)"}
    t, problem = _usable_target(target, target_name)
    if problem:
        return {**out, "available": False, "reason": problem}
    delta = ((f / f.shift(h) - 1.0) if as_pct else (f - f.shift(h))).dropna()
    delta = delta[np.isfinite(delta)]
    return _precedents(delta, cut, 1.0 if now >= 0 else -1.0, t,
                       current_since=_iso(f.index[-1 - h]), out=out)


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------

def _registry_lookup(registry: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if registry is None or registry.empty or "name" not in registry.columns:
        return {}
    cols = [c for c in ("family", "description") if c in registry.columns]
    return {str(r["name"]): {c: r[c] for c in cols} for _, r in registry.iterrows()}


def _classify_market(item: dict[str, Any]) -> str:
    for key in ("d1", "d5"):
        z = (item["changes"].get(key) or {}).get("robust_z")
        if z is not None and abs(z) >= SHARP_CHANGE_Z:
            return "sharp_move"
    sessions = item["excursion"].get("sessions")
    if sessions is not None and sessions <= NEW_EXTREME_SESSIONS:
        return "new_extreme"
    return "stretched"


def _sharpest_change(item_changes: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    """The 5- or 20-session change with the largest robust z (the move the precedent read is about)."""
    rated = [c for k, c in item_changes.items() if k in ("d5", "d20") and c.get("robust_z") is not None]
    return max(rated, key=lambda c: abs(c["robust_z"])) if rated else None


def _on_or_after(day: str | None, onset: str | None, slack_days: int = 3) -> bool:
    if not day or not onset:
        return False
    return pd.Timestamp(day) >= pd.Timestamp(onset) - pd.Timedelta(days=slack_days)


def build_report(
    anomalies: list[dict[str, Any]],
    recent: pd.DataFrame,
    *,
    history: pd.DataFrame | None = None,
    registry: pd.DataFrame | None = None,
    as_of: date | datetime | None = None,
    generated_at: datetime | None = None,
    features_scanned: int | None = None,
    threshold: float = SIGMA_THRESHOLD,
    window_days: int = WINDOW_DAYS,
) -> dict[str, Any]:
    """Build the report for one scan.

    Parameters:
        anomalies: the scan's ``anomalies`` list (feature, zscore, current_value, historical_mean/std).
        recent: every scanned feature's own observations (columns = feature names, no fill), at least a
            year; read for the feed-collapse sweep and as the fallback history.
        history: a longer read of the flagged features and their linked price series (same shape). The
            history read needs more than a year; without it most features report "history too short".
        registry: ``feature_registry`` rows (name, family, description).
        as_of: the scan's latest shared observation date (each item carries its own ``obs_date``).
    """
    reg = _registry_lookup(registry)
    recent = recent if recent is not None else pd.DataFrame()
    long = history if history is not None and not history.empty else recent
    faults = feed_collapses(recent)
    fault_list: list[dict[str, Any]] = list(faults.values())
    options_fault = faults.get("_opt_vol") or faults.get("_total_oi")

    newest_obs = None
    if not recent.empty:
        filled = recent.dropna(how="all")
        newest_obs = pd.to_datetime(filled.index).max() if len(filled) else None

    items: list[dict[str, Any]] = []
    for a in anomalies:
        name = str(a["feature"])
        meta = reg.get(name, {})
        kind = feature_kind(name, meta.get("family"))
        own = _clean(_column(long, name))
        if own.empty:
            own = _clean(_column(recent, name))
        level = level_reading(own, window_days)
        if level is None:   # not in the frames handed over: fall back to the scan's own numbers
            level = {"value": a.get("current_value"), "obs_date": None, "mean": a.get("historical_mean"),
                     "std": a.get("historical_std"), "zscore": float(a["zscore"]), "n": None}
        z_now = float(level["zscore"])
        mean, std = level["mean"], level["std"]
        as_pct = kind in ("price", "count") and bool(len(own)) and bool((own.tail(CHANGE_WINDOW) > 0).all())
        item: dict[str, Any] = {
            "feature": name,
            "label": feature_label(name, meta.get("description"), kind),
            "ticker": feature_ticker(name),
            "family": meta.get("family"),
            "kind": kind,
            "value": _num(level["value"]),
            "obs_date": level["obs_date"],
            "zscore": _num(z_now, 3),
            "mean": _num(mean),
            "std": _num(std),
            "window_obs": level["n"],
            "scan_zscore": _num(a.get("zscore"), 3),
            "scan_value": _num(a.get("current_value")),
            "changes": changes(own, as_pct=as_pct),
            "excursion": excursion(own.tail(CHANGE_WINDOW * 2), float(mean), float(std), z_now, threshold)
            if mean is not None and std is not None else {"sessions": None, "since": None},
            "range": year_range(own, window_days) if kind == "price" else None,
            "fault": None,
            "history": None,
            "move_history": None,
        }

        split = split_like_jump(own.tail(CHANGE_WINDOW + 30)) if kind == "price" else None
        suffix = _suffix(name, COUNT_SUFFIXES)
        if split is not None:
            fault = {"id": f"split:{name}", "kind": "split", "label": item["label"], **split}
            item["fault"], item["verdict"] = fault, "data_fault"
            fault_list.append({**fault, "feature": name})
        elif suffix in faults and name in faults[suffix]["collapsed_members"]:
            item["fault"] = {"id": faults[suffix]["id"], "kind": "feed_collapse",
                             "onset": faults[suffix]["onset"]}
            item["verdict"] = "data_fault"
        elif (kind == "options_derived" and options_fault is not None
              and _on_or_after(item["obs_date"] or _iso(as_of), options_fault["onset"], slack_days=0)):
            # Its latest reading was computed from chains the feed fault covers, whatever it was before.
            item["fault"] = {"id": options_fault["id"], "kind": "feed_collapse",
                             "onset": options_fault["onset"]}
            item["verdict"] = "suspect"
        elif (stale := staleness(own, newest_obs)) is not None:
            item["fault"] = {"id": f"stale:{name}", **stale}
            item["verdict"] = "stale"
        elif abs(z_now) < threshold:
            item["verdict"] = "faded"
        else:
            item["verdict"] = _classify_market(item)
            target_name = linked_price_feature(name, kind, long.columns)
            if target_name is None:
                item["history"] = {"available": False, "reason": "no linked price series"}
            else:
                target = _clean(_column(long, target_name))
                item["history"] = history_read(own, target, z_now, target_name=target_name,
                                               threshold=threshold,
                                               current_since=item["excursion"].get("since"))
                sharpest = _sharpest_change(item["changes"]) if item["verdict"] == "sharp_move" else None
                if sharpest is not None:
                    item["move_history"] = move_read(own, target, sharpest, as_pct=as_pct,
                                                     target_name=target_name)
        items.append(item)

    order = {"sharp_move": 0, "new_extreme": 1, "stretched": 2, "faded": 3, "suspect": 4, "stale": 5,
             "data_fault": 6}
    items.sort(key=lambda i: (order[i["verdict"]], -abs(i["zscore"] or 0.0)))

    for fault in fault_list:
        fault["flagged"] = [i["feature"] for i in items
                            if i["fault"] and i["fault"]["id"] == fault["id"] and i["verdict"] == "data_fault"]
        fault["suspect"] = [i["feature"] for i in items
                            if i["fault"] and i["fault"]["id"] == fault["id"] and i["verdict"] == "suspect"]
        fault.pop("collapsed_members", None)

    counts = {v: sum(1 for i in items if i["verdict"] == v) for v in order}
    headline = sorted(
        [i["feature"] for i in items
         if i["verdict"] in ("sharp_move", "new_extreme")
         or (i["verdict"] == "stretched" and abs(i["zscore"] or 0.0) > HEADLINE_Z)]
        + [f["id"] for f in fault_list]
    )
    return {
        "schema": SCHEMA,
        "as_of": _iso(as_of),
        "latest_obs": max((i["obs_date"] for i in items if i["obs_date"]), default=_iso(as_of)),
        "generated_at": generated_at.isoformat() if generated_at else None,
        "threshold": threshold,
        "features_scanned": features_scanned,
        "flagged": len(items),
        "counts": counts,
        "headline_key": headline,
        "items": items,
        "faults": fault_list,
    }


def new_headlines(report: dict[str, Any], previous_key: list[str] | None) -> list[str]:
    """Headline entries in ``report`` that the previous scan's report did not have."""
    seen = set(previous_key or [])
    return [k for k in report.get("headline_key") or [] if k not in seen]
