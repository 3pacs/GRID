"""
Regime changepoints over GRID's declared regime features (#1301).

Detector version ``changepoint_detector/2``. What it does, in order:

1. **Declared feature set.** ``REGIME_CHANGEPOINT_FEATURES`` names the
   features by concept (volatility, credit, rates/curve, equity, liquidity,
   macro). Each is bound by ``feature_registry`` name, never by
   ``model_eligible``. Where ``scripts/auto_regime.py`` already declares a
   concept, its candidate list (``REGIME_FEATURE_SOURCES``) and its stress
   sign (the sign of ``DEFAULT_FEATURE_WEIGHTS``) are reused as they are.
   Ephemeris and astrological features cannot be in the set: a declared name
   with such a token fails at import, and a registry row in the
   ``alternative`` family is refused at run time.
2. **Point-in-time read.** One ``store.pit.PITStore.get_pit`` call
   (LATEST_AS_OF): one value per obs_date, the latest vintage released by
   ``as_of``, obs_date <= as_of, retracted vintages excluded. A feature with
   too little history or a stale newest observation is skipped and reported.
   Each value is known from the later of its release_date and vintage_date
   (a value fetched or revised after its release date is not known before
   the fetch), and the retractions visible at as_of are read alongside.
3. **Causal detection.** ``AutoBNNDecomposer.detect_causal_breaks`` (the
   drift-break test in ``timeseries/autobnn.py``). A break's decision reads no
   observation after its confirming observation, so a later scan finds the
   same break on the same date.
4. **Direction per feature.** The sign of the drift change times the
   feature's stress sign. VIX or a credit spread drifting up is risk-off
   (``bearish``); the S&P 500 drifting up is risk-on (``bullish``).
5. **Identity and availability.**
   * The break's support is every observation its decision read: index
     i - 2 * pre_window through the confirming observation i + peak + post.
   * ``known_at`` = the latest of release_date, vintage_date and visible
     retraction date over that whole support. Its UTC day-end (the day-level
     rule of ``store.pit.retraction_cutoff``) is the break's *effective
     known-at* (``metadata.effective_known_at``): when it became knowable
     from its inputs. A break whose ``known_at`` is after ``as_of`` (a
     vintage the read should not see yet) waits.
   * The detector's own clock is kept apart: ``metadata.first_published_at``
     is the scan that first wrote the break (carried onto its later rows),
     ``metadata.published_at`` the scan that wrote the row. The freshness
     window (here and in thesis_scorer) is measured on the effective
     known-at only; publication times never extend it.
   * ``signal_id`` is the logical break: ``"<detector version>:<feature>:
     <break obs_date>"``. Availability is the row's ``valid_from``; the
     table's key is ``(signal_id, valid_from)``. ``metadata.support_sha256``
     fingerprints the support's values.
   * Only fresh breaks are written: the confirming observation is one of the
     feature's newest ``RECENCY_OBSERVATIONS``, and the break became known
     within ``FRESH_BREAK_DAYS`` of ``as_of``.
   * ``valid_from`` is the row's availability: the effective known-at for a
     break's first row (never ``now()``).
   * A re-scan on unchanged support writes nothing. If a later vintage or a
     retraction changes the support of a break already written, the
     re-evaluation is a new availability row of the same ``signal_id``
     (``metadata.status`` ``live``), and if the break is no longer detected,
     a ``withdrawn`` row (direction ``neutral``, confidence 0). Either is
     valid from the later of its own effective known-at and the scan that
     saw it, so a late or back-dated vintage can never move a break to an
     earlier ``valid_from``. Rows are only ever inserted, never updated.
     Readers fence every row on both ``valid_from`` and ``published_at``
     at their as-of (a late first scan's row has valid_from = effective
     known-at, before it was written), reduce each ``signal_id`` to its
     latest visible row, drop withdrawn breaks, then pick per feature.
   * ``provenance`` = ``CHANGEPOINT_DETECTOR_VERSION`` on every row. Rows
     from the earlier detector have no provenance, and readers ignore them by
     version. Nothing is deleted.
"""

from __future__ import annotations

import hashlib
import json
import time as _time
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from scripts.auto_regime import DEFAULT_FEATURE_WEIGHTS, REGIME_FEATURE_SOURCES
from store.pit import PITStore, retraction_cutoff
from timeseries.autobnn import (
    CAUSAL_PEAK_WINDOW,
    CAUSAL_POST_WINDOW,
    CAUSAL_PRE_WINDOW,
    AutoBNNDecomposer,
    RegimeChangeSignal,
    causal_context_start,
)

# ── Version and windows ─────────────────────────────────────────────────
#
# analysis/thesis_scorer.py counts only rows whose provenance equals this
# version, over the last FRESH_BREAK_DAYS (its CHANGEPOINT_* constants; a
# test pins them to these).
CHANGEPOINT_DETECTOR_VERSION = "changepoint_detector/2"
SOURCE_MODULE = "discovery.changepoint_detector"
SIGNAL_TYPE = "regime_change"

# Calendar days of history read per feature: three years, so the weekly
# series clear MIN_OBSERVATIONS too.
LOOKBACK_DAYS = 3 * 365
# The causal test needs 2 * pre-window + peak + post observations around a
# break; this leaves room for the recency window and some slack.
MIN_OBSERVATIONS = 2 * CAUSAL_PRE_WINDOW + CAUSAL_PEAK_WINDOW + CAUSAL_POST_WINDOW + 20
# A break is fresh when its confirming observation is one of the feature's
# newest RECENCY_OBSERVATIONS as of as_of, and it became known within
# FRESH_BREAK_DAYS of as_of.
RECENCY_OBSERVATIONS = 3
FRESH_BREAK_DAYS = 5
# metadata.status of a written row.
STATUS_LIVE = "live"
STATUS_WITHDRAWN = "withdrawn"
# Stored statistics are clipped so a near-noiseless series cannot write an
# absurd number (the old detector's headline magnitude was 13,629,417.65).
_STAT_CLIP = 99.0


# ── Declared feature set ────────────────────────────────────────────────

@dataclass(frozen=True)
class RegimeFeature:
    """One declared regime feature.

    Attributes:
        key: Stable declared key (the auto_regime concept name where one
            exists).
        concept: vol | credit | rates | equity | liquidity | macro.
        candidates: feature_registry names, best first. The first one that is
            registered, long enough and fresh is scanned.
        stress_sign: +1 when a rise is risk-off (VIX, spreads), -1 when a rise
            is risk-on (S&P 500).
        max_age_days: Calendar days the newest observation may be old at
            as_of before the feature is skipped as stale.
        basis: Why this feature and this sign.
    """

    key: str
    concept: str
    candidates: tuple[str, ...]
    stress_sign: int
    max_age_days: int
    basis: str


# Daily series: a weekend plus a holiday plus a one-day publication lag
# (auto_regime's MAX_FRESH_DATA_AGE_DAYS). Weekly series: one period plus the
# publication lag minus a day, as in intelligence/regime/market_state.py
# MAX_AGE (ICSA 7 + 6 - 1, H.4.1 7 + 3 - 1).
_DAILY_MAX_AGE = 5
_ICSA_MAX_AGE = 12
_H41_MAX_AGE = 9


def _from_auto_regime(
    key: str, concept: str, basis: str, *, max_age_days: int = _DAILY_MAX_AGE,
    extra: tuple[str, ...] = (),
) -> RegimeFeature:
    """A feature whose candidates and stress sign come from scripts/auto_regime.py."""
    weight = DEFAULT_FEATURE_WEIGHTS[key]
    candidates = tuple(REGIME_FEATURE_SOURCES.get(key, [key]))
    candidates += tuple(c for c in extra if c not in candidates)
    return RegimeFeature(key, concept, candidates, 1 if weight > 0 else -1, max_age_days,
                         f"auto_regime weight {weight:+.2f}; {basis}")


REGIME_CHANGEPOINT_FEATURES: tuple[RegimeFeature, ...] = (
    _from_auto_regime("vix", "vol", "a rise in implied volatility is stress"),
    _from_auto_regime("hy_spread", "credit", "wider high-yield spreads are stress"),
    RegimeFeature("ig_spread", "credit", ("ig_oas_spread",), +1, _DAILY_MAX_AGE,
                  "BAMLC0A0CM; wider investment-grade spreads are stress"),
    _from_auto_regime("yield_curve_10y2y", "rates", "a flattening or inverting curve is stress"),
    # entity_map: DGS10 has fed treasury_10y since 2026-11-04. yc_10y, auto_regime's
    # only candidate, was never registered, so treasury_10y is added after it.
    _from_auto_regime("treasury_10y", "rates", "higher long rates tighten conditions",
                      extra=("treasury_10y",)),
    _from_auto_regime("sp500", "equity", "a falling S&P 500 is risk-off"),
    RegimeFeature("fed_net_liquidity", "liquidity", ("fed_net_liquidity",), -1, _H41_MAX_AGE,
                  "WALCL - TGA - RRP; draining liquidity is risk-off (thesis_scorer's "
                  "fed_liquidity model reads a rise as bullish)"),
    _from_auto_regime("dollar_index", "liquidity",
                      "a stronger dollar tightens global dollar funding (safe-haven bid)"),
    RegimeFeature("initial_claims", "macro", ("initial_claims",), +1, _ICSA_MAX_AGE,
                  "ICSA; rising jobless claims are labour-market stress"),
    _from_auto_regime("breakeven_10y", "macro", "falling inflation breakevens are deflation fear"),
)

REQUIRED_CONCEPTS = frozenset({"vol", "credit", "rates", "equity", "liquidity", "macro"})

# Never scanned: ephemeris, astrological and celestial features. The old
# detector's first six inputs were ephemeris rows dated out to 2100-12-31.
FORBIDDEN_FAMILIES = frozenset({"alternative"})
FORBIDDEN_NAME_TOKENS = (
    "ephemeris", "astro", "lunar", "moon", "planet", "solar", "celestial", "vedic",
    "nakshatra", "zodiac", "retrograde", "eclipse", "tithi", "mercury", "venus",
    "jupiter", "saturn",
)


def _forbidden_name(name: str) -> bool:
    lowered = name.lower()
    return any(token in lowered for token in FORBIDDEN_NAME_TOKENS)


def check_declared_features(features: tuple[RegimeFeature, ...]) -> None:
    """Raise if the set misses a required concept or names a forbidden feature."""
    concepts = {f.concept for f in features}
    if not REQUIRED_CONCEPTS <= concepts:
        raise RuntimeError(f"changepoint features miss concepts: {sorted(REQUIRED_CONCEPTS - concepts)}")
    keys = [f.key for f in features]
    if len(keys) != len(set(keys)):
        raise RuntimeError("changepoint feature keys must be unique")
    for f in features:
        if f.stress_sign not in (-1, 1) or not f.candidates:
            raise RuntimeError(f"changepoint feature {f.key}: needs a stress sign and candidates")
        bad = [c for c in f.candidates if _forbidden_name(c)]
        if bad:
            raise RuntimeError(f"changepoint feature {f.key}: ephemeris/astrological names {bad}")


check_declared_features(REGIME_CHANGEPOINT_FEATURES)


# ── Results ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FeatureSeries:
    """The point-in-time series one feature was scanned on.

    Attributes:
        spec: The declared feature.
        feature: The bound feature_registry name (the signals' ticker).
        feature_id: Its feature_registry id.
        obs: obs_date per observation, in order.
        values: The selected vintage's value per observation.
        known: Per observation, the later of its release_date and
            vintage_date: the day that value became readable point-in-time.
        retracted: (obs_date, retraction UTC date) for every retraction of
            this feature visible at as_of inside the read window.
    """

    spec: RegimeFeature
    feature: str
    feature_id: int
    obs: tuple[date, ...]
    values: tuple[float, ...]
    known: tuple[date, ...]
    retracted: tuple[tuple[date, date], ...] = ()

    def support(self, change_index: int) -> tuple[int, int]:
        """First and last observation index a break decision at ``change_index`` read."""
        last = change_index + CAUSAL_PEAK_WINDOW + CAUSAL_POST_WINDOW
        return causal_context_start(change_index), min(len(self.obs) - 1, last)

    def decidable(self, change_index: int) -> bool:
        """Whether this series holds the full support of a break at ``change_index``."""
        return (change_index - 2 * CAUSAL_PRE_WINDOW >= 0
                and change_index + CAUSAL_PEAK_WINDOW + CAUSAL_POST_WINDOW <= len(self.obs) - 1)

    def support_known_at(self, start: int, end: int) -> date:
        """Latest release, vintage or visible retraction date over obs ``start..end``."""
        known = max(self.known[start:end + 1])
        lo, hi = self.obs[start], self.obs[end]
        for obs_date, retracted_on in self.retracted:
            if lo <= obs_date <= hi and retracted_on > known:
                known = retracted_on
        return known

    def support_sha256(self, start: int, end: int) -> str:
        """Fingerprint of the values a decision over obs ``start..end`` read."""
        payload = [
            self.feature_id, [CAUSAL_PRE_WINDOW, CAUSAL_POST_WINDOW, CAUSAL_PEAK_WINDOW],
            [[self.obs[i].isoformat(), repr(float(self.values[i]))] for i in range(start, end + 1)],
        ]
        return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class FeatureBreak:
    """A detected break on one bound feature.

    Attributes:
        spec: The declared feature.
        feature: The bound feature_registry name (the signal's ticker).
        feature_id: Its feature_registry id.
        signal: The detector output.
        break_date: obs_date of the break observation.
        confirm_date: obs_date of the last observation the decision read.
        support_start_date: obs_date of the first observation it read.
        known_at_date: Latest release, vintage or visible retraction date
            over the whole support.
        valid_from: End of known_at_date, UTC: the effective known-at, which
            is the availability of the break's first row.
        direction: bullish (risk-on) | bearish (risk-off) | neutral.
        fresh: Whether the break is inside the recency window of as_of and
            known by as_of.
        support_sha256: Fingerprint of the support's values.
    """

    spec: RegimeFeature
    feature: str
    feature_id: int
    signal: RegimeChangeSignal
    break_date: date
    confirm_date: date
    known_at_date: date
    valid_from: datetime
    direction: str
    fresh: bool
    support_start_date: date | None = None
    support_sha256: str = ""

    @property
    def signal_id(self) -> str:
        """The logical break id (version, feature, break date); not its availability."""
        return changepoint_signal_id(self.feature, self.break_date)

    @property
    def effective_known_at(self) -> datetime:
        """When the break became knowable from its inputs (``known_by(known_at_date)``)."""
        return known_by(self.known_at_date)


@dataclass
class ChangeReport:
    """Summary of changepoint detection across the declared features.

    Attributes:
        timestamp: When the detection was run.
        features_scanned: Number of features analysed.
        changepoints_found: Fresh breaks (the ones a publish may write).
        regime_changes: The fresh breaks' detector outputs.
        elapsed_seconds: Time taken for the analysis.
        breaks: Every break in the lookback, fresh or not, with its feature,
            dates and direction.
        skipped: Declared feature key -> why it was not scanned.
        as_of: The point-in-time date of the read.
        detector_version: CHANGEPOINT_DETECTOR_VERSION.
        series: Feature name -> the series it was scanned on.
        min_confidence: The confidence floor of this scan.
        waiting: Breaks detected on a value not known by as_of.
        publish_counts: Filled by publish_regime_signals.
    """

    timestamp: datetime
    features_scanned: int
    changepoints_found: int
    regime_changes: list[RegimeChangeSignal]
    elapsed_seconds: float
    breaks: list[FeatureBreak] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    as_of: date | None = None
    detector_version: str = CHANGEPOINT_DETECTOR_VERSION
    series: dict[str, FeatureSeries] = field(default_factory=dict)
    min_confidence: float = 0.5
    waiting: int = 0
    publish_counts: dict[str, int] = field(default_factory=dict)

    @property
    def fresh_breaks(self) -> list[FeatureBreak]:
        return [b for b in self.breaks if b.fresh]


# ── Helpers ─────────────────────────────────────────────────────────────

def changepoint_signal_id(feature: str, break_date: date) -> str:
    """Logical break id: detector version, feature, break obs_date."""
    return f"{CHANGEPOINT_DETECTOR_VERSION}:{feature}:{break_date.isoformat()}"


def known_by(release_date: date) -> datetime:
    """The instant a day-level release is known: the end of that UTC day.

    Same convention as ``store.pit.retraction_cutoff`` for a date: a row
    released on day D is visible to ``as_of = D``.
    """
    return datetime.combine(release_date, time.max, tzinfo=timezone.utc)


def break_direction(signal: RegimeChangeSignal, stress_sign: int) -> str:
    """bullish (risk-on) / bearish (risk-off) / neutral for a break.

    The drift change's sign times the feature's stress sign: a positive
    product means the break moved the feature toward stress.
    """
    statistic = signal.statistic
    if statistic is None or not np.isfinite(statistic) or statistic == 0:
        return "neutral"
    stress_change = np.sign(statistic) * stress_sign
    return "bearish" if stress_change > 0 else "bullish"


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _registry_rows(engine: Engine, names: list[str]) -> dict[str, tuple[int, str, Any]]:
    """name -> (id, family, deprecated_at) for the registered candidate names."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, name, family, deprecated_at FROM feature_registry "
                "WHERE name = ANY(:names)"
            ),
            {"names": names},
        ).fetchall()
    return {str(r[1]): (int(r[0]), str(r[2] or ""), r[3]) for r in rows}


def _series_for(pit: pd.DataFrame, feature_id: int) -> pd.DataFrame:
    """One feature's PIT rows, finite values only, in obs_date order."""
    rows = pit[pit["feature_id"] == feature_id].copy()
    if rows.empty:
        return rows
    rows["value"] = pd.to_numeric(rows["value"], errors="coerce")
    finite = np.isfinite(rows["value"].to_numpy(dtype=np.float64))
    if not finite.all():
        log.warning("Changepoint scan: feature {f} has {n} non-finite PIT values (dropped, not filled)",
                    f=feature_id, n=int((~finite).sum()))
    return rows[finite].sort_values("obs_date").reset_index(drop=True)


def _bind(
    spec: RegimeFeature,
    registry: dict[str, tuple[int, str, Any]],
    pit: pd.DataFrame,
    as_of: date,
    used: set[int],
) -> tuple[str, int, pd.DataFrame] | str:
    """The first usable candidate as (name, id, rows), or why none is usable."""
    reasons: list[str] = []
    for name in spec.candidates:
        row = registry.get(name)
        if row is None:
            reasons.append(f"{name}: not registered")
            continue
        fid, family, deprecated_at = row
        if family in FORBIDDEN_FAMILIES or _forbidden_name(name):
            reasons.append(f"{name}: family {family!r} is never scanned")
            continue
        if deprecated_at is not None:
            reasons.append(f"{name}: deprecated")
            continue
        if fid in used:
            reasons.append(f"{name}: already bound to another key")
            continue
        series = _series_for(pit, fid)
        if len(series) < MIN_OBSERVATIONS:
            reasons.append(f"{name}: {len(series)} observations < {MIN_OBSERVATIONS}")
            continue
        newest = _as_date(series["obs_date"].iloc[-1])
        age = (as_of - newest).days
        if age > spec.max_age_days:
            reasons.append(f"{name}: stale, newest obs {newest.isoformat()} is {age} days old "
                           f"(limit {spec.max_age_days})")
            continue
        return name, fid, series
    return "; ".join(reasons) or "no candidates"


def _retractions(
    engine: Engine, feature_ids: list[int], start: date, as_of: date,
) -> dict[int, list[tuple[date, date]]]:
    """feature_id -> [(obs_date, retraction UTC date)] visible at as_of in the read window."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT feature_id, obs_date, retracted_at FROM resolved_series_retractions "
                "WHERE feature_id = ANY(:fids) AND obs_date BETWEEN :start AND :end "
                "AND retracted_at <= :cutoff"
            ),
            {"fids": feature_ids, "start": start, "end": as_of, "cutoff": retraction_cutoff(as_of)},
        ).fetchall()
    out: dict[int, list[tuple[date, date]]] = {}
    for fid, obs_date, retracted_at in rows:
        out.setdefault(int(fid), []).append((_as_date(obs_date), _as_date(retracted_at)))
    return out


def _feature_series(
    spec: RegimeFeature, feature: str, feature_id: int, rows: pd.DataFrame,
    retracted: list[tuple[date, date]],
) -> FeatureSeries:
    obs = tuple(_as_date(v) for v in rows["obs_date"])
    known = tuple(
        max(_as_date(r), _as_date(v)) for r, v in zip(rows["release_date"], rows["vintage_date"])
    )
    return FeatureSeries(
        spec=spec, feature=feature, feature_id=feature_id, obs=obs,
        values=tuple(float(v) for v in rows["value"]), known=known,
        retracted=tuple(sorted(retracted)),
    )


def _feature_breaks(
    series: FeatureSeries,
    signals: list[RegimeChangeSignal],
    as_of: date,
) -> list[FeatureBreak]:
    n = len(series.obs)
    out: list[FeatureBreak] = []
    for sig in signals:
        if sig.confirm_index is None or sig.confirm_index >= n:
            continue
        start, end = series.support(sig.change_index)
        known_at = series.support_known_at(start, end)
        fresh = (
            sig.confirm_index >= n - RECENCY_OBSERVATIONS
            and known_at <= as_of
            and (as_of - known_at).days <= FRESH_BREAK_DAYS
        )
        out.append(FeatureBreak(
            spec=series.spec, feature=series.feature, feature_id=series.feature_id, signal=sig,
            break_date=series.obs[sig.change_index], confirm_date=series.obs[sig.confirm_index],
            known_at_date=known_at, valid_from=known_by(known_at),
            direction=break_direction(sig, series.spec.stress_sign), fresh=fresh,
            support_start_date=series.obs[start], support_sha256=series.support_sha256(start, end),
        ))
    return out


# ── Scan ────────────────────────────────────────────────────────────────

def scan_for_changepoints(
    engine: Engine,
    min_confidence: float = 0.5,
    lookback_days: int = LOOKBACK_DAYS,
    max_features: int | None = None,
    *,
    as_of: date | None = None,
    features: tuple[RegimeFeature, ...] = REGIME_CHANGEPOINT_FEATURES,
    pit: PITStore | None = None,
) -> ChangeReport:
    """Scan the declared regime features for causal drift breaks.

    Parameters:
        engine: SQLAlchemy database engine.
        min_confidence: Minimum break confidence (0-1).
        lookback_days: Calendar days of history read per feature.
        max_features: Optional cap on how many declared features are scanned
            (declared order).
        as_of: Point-in-time date of the read (default: today, UTC).
        features: The declared feature set (tests pass their own).
        pit: The point-in-time reader (default: ``PITStore(engine)``).

    Returns:
        ChangeReport with every break in the lookback, which of them are
        fresh, the series they were found on, and the features skipped with
        their reasons.
    """
    start = _time.monotonic()
    as_of = as_of or datetime.now(timezone.utc).date()
    specs = tuple(features[:max_features] if max_features else features)
    report = ChangeReport(
        timestamp=datetime.now(timezone.utc), features_scanned=0, changepoints_found=0,
        regime_changes=[], elapsed_seconds=0.0, as_of=as_of, min_confidence=min_confidence,
    )
    if not specs:
        log.info("Changepoint scan: no declared features")
        return report

    names = sorted({c for s in specs for c in s.candidates})
    registry = _registry_rows(engine, names)
    fids = sorted({registry[n][0] for n in names if n in registry})
    window_start = as_of - timedelta(days=lookback_days)
    retractions: dict[int, list[tuple[date, date]]] = {}
    if fids:
        rows = (pit or PITStore(engine)).get_pit(
            fids, as_of, "LATEST_AS_OF", start_date=window_start, end_date=as_of,
        )
        retractions = _retractions(engine, fids, window_start, as_of)
    else:
        rows = pd.DataFrame(columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"])
    if not rows.empty:
        late = sum(_as_date(v) > as_of for v in rows["vintage_date"])
        if late:
            log.warning(
                "Changepoint scan: {n} PIT rows have vintage_date after as_of {d}; "
                "breaks that read them wait until that vintage is known",
                n=late, d=as_of.isoformat(),
            )

    decomposer = AutoBNNDecomposer(num_samples=100, num_chains=1)
    used: set[int] = set()
    for spec in specs:
        bound = _bind(spec, registry, rows, as_of, used)
        if isinstance(bound, str):
            report.skipped[spec.key] = bound
            continue
        feature, fid, frame = bound
        used.add(fid)
        report.features_scanned += 1
        series = _feature_series(spec, feature, fid, frame, retractions.get(fid, []))
        report.series[feature] = series
        signals = decomposer.detect_causal_breaks(
            np.asarray(series.values, dtype=np.float64),
            dates=list(series.obs),
            series_id=feature,
            min_confidence=min_confidence,
            pre_window=CAUSAL_PRE_WINDOW,
            post_window=CAUSAL_POST_WINDOW,
            peak_window=CAUSAL_PEAK_WINDOW,
        )
        breaks = _feature_breaks(series, signals, as_of)
        report.waiting += sum(1 for b in breaks if b.known_at_date > as_of)
        report.breaks.extend(breaks)

    fresh = report.fresh_breaks
    report.regime_changes = [b.signal for b in fresh]
    report.changepoints_found = len(fresh)
    report.elapsed_seconds = _time.monotonic() - start
    if report.skipped:
        log.warning("Changepoint scan skipped {n} declared features: {s}",
                    n=len(report.skipped), s=report.skipped)
    log.info(
        "Changepoint scan as_of {d}: {n} features scanned, {c} breaks in lookback, {f} fresh, "
        "{w} waiting on a later vintage, {t:.1f}s",
        d=as_of.isoformat(), n=report.features_scanned, c=len(report.breaks),
        f=len(fresh), w=report.waiting, t=report.elapsed_seconds,
    )
    return report


# ── Publish ─────────────────────────────────────────────────────────────

# Rows already written for these logical breaks, and every row of the scanned
# features inside the readers' window (to find breaks that were withdrawn).
_EXISTING_SQL = text("""
    SELECT signal_id, ticker, valid_from, confidence,
           metadata->>'support_sha256', metadata->>'status', metadata->>'break_date',
           metadata->>'effective_known_at', metadata->>'first_published_at'
    FROM signal_registry
    WHERE source_module = :src AND signal_type = :stype AND provenance = :version
      AND (signal_id = ANY(:ids) OR (ticker = ANY(:tickers) AND valid_from > :since))
""")

# Insert-only. A row is never written at or before an existing availability
# of the same logical break, so nothing can be back-dated.
_INSERT_SQL = text("""
    INSERT INTO signal_registry
        (signal_id, source_module, signal_type, ticker, value,
         z_score, confidence, direction, valid_from, metadata, provenance)
    SELECT
        CAST(:sid AS text), CAST(:src AS text), CAST(:stype AS text), CAST(:ticker AS text),
        CAST(:val AS double precision), CAST(:zscore AS double precision),
        CAST(:conf AS double precision), CAST(:direction AS text),
        CAST(:vfrom AS timestamptz), CAST(:meta AS jsonb), CAST(:version AS text)
    WHERE NOT EXISTS (
        SELECT 1 FROM signal_registry WHERE signal_id = :sid AND valid_from >= CAST(:vfrom AS timestamptz)
    )
    ON CONFLICT (signal_id, valid_from) DO NOTHING
""")


@dataclass(frozen=True)
class _Written:
    """The latest row already written for one logical break."""

    signal_id: str
    ticker: str
    valid_from: datetime
    confidence: float
    support_sha256: str | None
    status: str
    break_date: date | None
    effective_known_at: datetime | None = None
    first_published_at: str | None = None


def _latest_written(rows: list) -> dict[str, _Written]:
    latest: dict[str, _Written] = {}
    for sid, ticker, vfrom, conf, sha, status, bdate, effective, first_published in rows:
        vfrom = vfrom if vfrom.tzinfo else vfrom.replace(tzinfo=timezone.utc)
        row = _Written(str(sid), str(ticker or ""), vfrom, float(conf or 0.0), sha,
                       status or STATUS_LIVE, date.fromisoformat(bdate) if bdate else None,
                       datetime.fromisoformat(effective) if effective else None, first_published)
        if sid not in latest or vfrom > latest[sid].valid_from:
            latest[sid] = row
    return latest


def _base_meta(series_spec: RegimeFeature, feature: str, feature_id: int, signal_id: str,
               as_of: date | None) -> dict[str, Any]:
    return {
        "detector_version": CHANGEPOINT_DETECTOR_VERSION,
        "break_id": signal_id,
        "feature": feature,
        "feature_id": feature_id,
        "regime_key": series_spec.key,
        "concept": series_spec.concept,
        "stress_sign": series_spec.stress_sign,
        "as_of": as_of.isoformat() if as_of else None,
        "windows": {"pre": CAUSAL_PRE_WINDOW, "post": CAUSAL_POST_WINDOW, "peak": CAUSAL_PEAK_WINDOW},
    }


def _signal_params(
    b: FeatureBreak, as_of: date | None, *, published_at: datetime,
    valid_from: datetime | None = None, supersedes: datetime | None = None,
    first_published_at: str | None = None,
) -> dict[str, Any]:
    """Bind parameters for one live row of a break.

    ``effective_known_at`` (inputs) and ``first_published_at`` /
    ``published_at`` (the detector's scans) are separate fields. Every row
    carries ``published_at``: readers fence on it as well as on valid_from.
    """
    sig = b.signal
    statistic = float(np.clip(sig.statistic or 0.0, -_STAT_CLIP, _STAT_CLIP))
    risk = {"bearish": "risk_off", "bullish": "risk_on"}.get(b.direction, "none")
    meta = _base_meta(b.spec, b.feature, b.feature_id, b.signal_id, as_of)
    meta.update({
        "status": STATUS_LIVE,
        "risk": risk,
        "break_date": b.break_date.isoformat(),
        "confirm_date": b.confirm_date.isoformat(),
        "support_start_date": b.support_start_date.isoformat() if b.support_start_date else None,
        "known_at_date": b.known_at_date.isoformat(),
        "effective_known_at": b.effective_known_at.isoformat(),
        "first_published_at": first_published_at,
        "published_at": published_at.isoformat(),
        "support_sha256": b.support_sha256,
        "supersedes_valid_from": supersedes.isoformat() if supersedes else None,
        "pre_regime": sig.pre_regime,
        "post_regime": sig.post_regime,
        "drift_change": sig.magnitude if statistic >= 0 else -sig.magnitude,
        "statistic": statistic,
    })
    return {
        "sid": b.signal_id,
        "src": SOURCE_MODULE,
        "stype": SIGNAL_TYPE,
        "ticker": b.feature,
        "val": abs(statistic),
        "zscore": statistic,
        "conf": float(sig.confidence),
        "direction": b.direction,
        "vfrom": valid_from or b.valid_from,
        "meta": json.dumps(meta, sort_keys=True),
        "version": CHANGEPOINT_DETECTOR_VERSION,
    }


def _withdrawal(
    prev: _Written, series: FeatureSeries, as_of: date | None, now: datetime,
) -> dict[str, Any] | None:
    """Bind parameters for a withdrawal of ``prev``, or None if the scan cannot tell."""
    bd = prev.break_date
    if bd is None or not series.obs or not series.obs[0] <= bd <= series.obs[-1]:
        return None
    idx = bisect_left(series.obs, bd)
    present = idx < len(series.obs) and series.obs[idx] == bd
    if present and not series.decidable(idx):
        return None
    start, end = series.support(idx if present else max(0, idx - 1))
    end = max(end, start)
    sha = series.support_sha256(start, end)
    if present and sha == prev.support_sha256:
        return None   # same inputs give the same decision; nothing was withdrawn
    known_at = series.support_known_at(start, end)
    meta = _base_meta(series.spec, series.feature, series.feature_id, prev.signal_id, as_of)
    meta.update({
        "status": STATUS_WITHDRAWN,
        "break_date": bd.isoformat(),
        "known_at_date": known_at.isoformat(),
        "effective_known_at": known_by(known_at).isoformat(),
        "first_published_at": prev.first_published_at,
        "published_at": now.isoformat(),
        "support_sha256": sha,
        "supersedes_valid_from": prev.valid_from.isoformat(),
        "reason": ("the break is no longer detected on the current vintages"
                   if present else "the break observation is no longer in the series"),
    })
    return {
        "sid": prev.signal_id, "src": SOURCE_MODULE, "stype": SIGNAL_TYPE, "ticker": series.feature,
        "val": 0.0, "zscore": 0.0, "conf": 0.0, "direction": "neutral",
        "vfrom": max(known_by(known_at), now),
        "meta": json.dumps(meta, sort_keys=True), "version": CHANGEPOINT_DETECTOR_VERSION,
    }


def _plan_writes(
    report: ChangeReport, written: dict[str, _Written], now: datetime,
) -> tuple[list[tuple[str, dict[str, Any]]], dict[str, int]]:
    """The rows a publish inserts, as (kind, params), and counts by outcome."""
    counts = {"first": 0, "revision": 0, "withdrawal": 0, "unchanged": 0, "waiting": 0}
    plan: list[tuple[str, dict[str, Any]]] = []
    for b in report.fresh_breaks:
        prev = written.get(b.signal_id)
        if prev is not None and prev.status == STATUS_LIVE and prev.support_sha256 == b.support_sha256:
            counts["unchanged"] += 1
            continue
        if prev is None:
            kind, vfrom = "first", b.valid_from
        else:
            # Seen only now: never earlier than this scan, so a late or
            # back-dated vintage cannot move the break before its last row.
            kind, vfrom = "revision", max(b.valid_from, now)
        if vfrom > now:
            counts["waiting"] += 1
            continue
        plan.append((kind, _signal_params(
            b, report.as_of, valid_from=vfrom, supersedes=prev.valid_from if prev else None,
            published_at=now, first_published_at=prev.first_published_at if prev else now.isoformat(),
        )))

    detected = {b.signal_id for b in report.breaks}
    since = now - timedelta(days=FRESH_BREAK_DAYS)
    for sid, prev in written.items():
        series = report.series.get(prev.ticker)
        # Only a break a reader can still count (fresh by effective known-at).
        if (prev.status != STATUS_LIVE or (prev.effective_known_at or prev.valid_from) <= since
                or sid in detected or series is None or prev.confidence < report.min_confidence):
            continue
        params = _withdrawal(prev, series, report.as_of, now)
        if params is None:
            continue
        if params["vfrom"] > now:
            counts["waiting"] += 1
            continue
        plan.append(("withdrawal", params))
    return plan, counts


def publish_regime_signals(
    engine: Engine,
    report: ChangeReport,
    *,
    now: datetime | None = None,
) -> int:
    """Write the report's fresh breaks, revisions and withdrawals to signal_registry.

    * A logical break not written before gets a row valid from the end of
      its known_at day; one still in the future waits for a later scan.
    * A break whose support values are unchanged since its latest row is not
      written again.
    * A break whose support changed (a later vintage, a retraction) gets a
      new row of the same signal_id, valid from the later of its known_at
      day-end and ``now``; a break no longer detected gets a ``withdrawn``
      row the same way. Rows are never updated or deleted.

    Parameters:
        engine: SQLAlchemy database engine.
        report: ChangeReport from scan_for_changepoints.
        now: Current time (default: now, UTC).

    Returns:
        int: Number of rows inserted. ``report.publish_counts`` has them by
        kind, plus the unchanged and waiting breaks.
    """
    now = now or datetime.now(timezone.utc)
    fresh = report.fresh_breaks
    tickers = sorted(report.series)
    if not fresh and not tickers:
        report.publish_counts = {}
        return 0

    with engine.begin() as conn:
        rows = conn.execute(_EXISTING_SQL, {
            "src": SOURCE_MODULE, "stype": SIGNAL_TYPE, "version": CHANGEPOINT_DETECTOR_VERSION,
            "ids": sorted({b.signal_id for b in fresh}), "tickers": tickers,
            "since": now - timedelta(days=FRESH_BREAK_DAYS),
        }).fetchall()
        plan, counts = _plan_writes(report, _latest_written(rows), now)
        published = 0
        for kind, params in plan:
            result = conn.execute(_INSERT_SQL, params)
            inserted = max(0, int(result.rowcount or 0))
            published += inserted
            counts[kind] += inserted

    report.publish_counts = counts
    if counts["waiting"]:
        log.info("Changepoint publish: {n} rows not known yet (valid_from > now), deferred",
                 n=counts["waiting"])
    if published:
        log.info("Published {n} regime change rows ({v}): {c}",
                 n=published, v=CHANGEPOINT_DETECTOR_VERSION, c=counts)
    return published


def run_changepoint_cycle(
    engine: Engine,
    min_confidence: float = 0.5,
    *,
    as_of: date | None = None,
    now: datetime | None = None,
    pit: PITStore | None = None,
) -> dict[str, Any]:
    """Full changepoint detection cycle: scan + publish.

    Called by the Hermes operator every 12 hours.

    Parameters:
        engine: SQLAlchemy database engine.
        min_confidence: Minimum confidence threshold.
        as_of: Point-in-time date of the read (default: today, UTC).
        now: Current time for the publish deferral (default: now, UTC).
        pit: The point-in-time reader (default: ``PITStore(engine)``).

    Returns:
        dict: Cycle summary (JSON-safe).
    """
    report = scan_for_changepoints(engine, min_confidence=min_confidence, as_of=as_of, pit=pit)
    published = publish_regime_signals(engine, report, now=now)
    fresh = report.fresh_breaks

    return {
        "detector_version": report.detector_version,
        "as_of": report.as_of.isoformat() if report.as_of else None,
        "features_scanned": report.features_scanned,
        "features_skipped": dict(report.skipped),
        "changepoints_found": report.changepoints_found,
        "breaks_in_lookback": len(report.breaks),
        "breaks_waiting_on_a_vintage": report.waiting,
        "signals_published": published,
        "publish": dict(report.publish_counts),
        "elapsed_seconds": round(report.elapsed_seconds, 2),
        "top_changes": [
            {
                "series": b.feature,
                "concept": b.spec.concept,
                "break_date": b.break_date.isoformat(),
                "direction": b.direction,
                "pre": b.signal.pre_regime,
                "post": b.signal.post_regime,
                "confidence": round(b.signal.confidence, 3),
            }
            for b in sorted(fresh, key=lambda b: b.signal.confidence, reverse=True)[:5]
        ],
    }
