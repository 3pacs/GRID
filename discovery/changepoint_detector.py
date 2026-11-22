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
3. **Causal detection.** ``AutoBNNDecomposer.detect_causal_breaks`` (the
   drift-break test in ``timeseries/autobnn.py``). A break's decision reads no
   observation after its confirming observation, so a later scan finds the
   same break on the same date.
4. **Direction per feature.** The sign of the drift change times the
   feature's stress sign. VIX or a credit spread drifting up is risk-off
   (``bearish``); the S&P 500 drifting up is risk-on (``bullish``).
5. **Identity and dates.**
   * ``signal_id`` = ``"<detector version>:<feature>:<break obs_date>"``.
   * ``valid_from`` = the end of the UTC day on which the last observation
     the decision read was released. That is the day-level "known by the end
     of the release day" rule of ``store.pit.retraction_cutoff``; it is never
     ``now()``.
   * Only fresh breaks are written: the confirming observation is one of the
     feature's newest ``RECENCY_OBSERVATIONS``, and the break became known
     within ``FRESH_BREAK_DAYS`` of ``as_of``. A break whose ``valid_from``
     is still in the future waits for the next scan. An id already in the
     registry is never written again, so a re-scan writes nothing new.
   * ``provenance`` = ``CHANGEPOINT_DETECTOR_VERSION`` on every row. Rows
     from the earlier detector have no provenance, and readers ignore them by
     version. Nothing is deleted.
"""

from __future__ import annotations

import json
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from scripts.auto_regime import DEFAULT_FEATURE_WEIGHTS, REGIME_FEATURE_SOURCES
from store.pit import PITStore
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
class FeatureBreak:
    """A detected break on one bound feature.

    Attributes:
        spec: The declared feature.
        feature: The bound feature_registry name (the signal's ticker).
        feature_id: Its feature_registry id.
        signal: The detector output.
        break_date: obs_date of the break observation.
        confirm_date: obs_date of the last observation the decision read.
        known_at_date: Latest release_date among the observations the
            decision read.
        valid_from: End of known_at_date, UTC.
        direction: bullish (risk-on) | bearish (risk-off) | neutral.
        fresh: Whether the break is inside the recency window of as_of.
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

    @property
    def signal_id(self) -> str:
        return changepoint_signal_id(self.feature, self.break_date)


@dataclass
class ChangeReport:
    """Summary of changepoint detection across the declared features.

    Attributes:
        timestamp: When the detection was run.
        features_scanned: Number of features analysed.
        changepoints_found: Fresh breaks (the ones a publish writes).
        regime_changes: The fresh breaks' detector outputs.
        elapsed_seconds: Time taken for the analysis.
        breaks: Every break in the lookback, fresh or not, with its feature,
            dates and direction.
        skipped: Declared feature key -> why it was not scanned.
        as_of: The point-in-time date of the read.
        detector_version: CHANGEPOINT_DETECTOR_VERSION.
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

    @property
    def fresh_breaks(self) -> list[FeatureBreak]:
        return [b for b in self.breaks if b.fresh]


# ── Helpers ─────────────────────────────────────────────────────────────

def changepoint_signal_id(feature: str, break_date: date) -> str:
    """Deterministic id: detector version, feature, break obs_date."""
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
        return value.date()
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


def _feature_breaks(
    spec: RegimeFeature,
    feature: str,
    feature_id: int,
    series: pd.DataFrame,
    signals: list[RegimeChangeSignal],
    as_of: date,
) -> list[FeatureBreak]:
    obs = [_as_date(v) for v in series["obs_date"]]
    released = [_as_date(v) for v in series["release_date"]]
    n = len(obs)
    out: list[FeatureBreak] = []
    for sig in signals:
        if sig.confirm_index is None or sig.confirm_index >= n:
            continue
        start = causal_context_start(sig.change_index)
        known_at = max(released[start:sig.confirm_index + 1])
        fresh = (
            sig.confirm_index >= n - RECENCY_OBSERVATIONS
            and (as_of - known_at).days <= FRESH_BREAK_DAYS
        )
        out.append(FeatureBreak(
            spec=spec, feature=feature, feature_id=feature_id, signal=sig,
            break_date=obs[sig.change_index], confirm_date=obs[sig.confirm_index],
            known_at_date=known_at, valid_from=known_by(known_at),
            direction=break_direction(sig, spec.stress_sign), fresh=fresh,
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
        fresh, and the features skipped with their reasons.
    """
    start = _time.monotonic()
    as_of = as_of or datetime.now(timezone.utc).date()
    specs = tuple(features[:max_features] if max_features else features)
    report = ChangeReport(
        timestamp=datetime.now(timezone.utc), features_scanned=0, changepoints_found=0,
        regime_changes=[], elapsed_seconds=0.0, as_of=as_of,
    )
    if not specs:
        log.info("Changepoint scan: no declared features")
        return report

    names = sorted({c for s in specs for c in s.candidates})
    registry = _registry_rows(engine, names)
    fids = sorted({registry[n][0] for n in names if n in registry})
    if fids:
        rows = (pit or PITStore(engine)).get_pit(
            fids, as_of, "LATEST_AS_OF",
            start_date=as_of - timedelta(days=lookback_days), end_date=as_of,
        )
    else:
        rows = pd.DataFrame(columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"])

    decomposer = AutoBNNDecomposer(num_samples=100, num_chains=1)
    used: set[int] = set()
    for spec in specs:
        bound = _bind(spec, registry, rows, as_of, used)
        if isinstance(bound, str):
            report.skipped[spec.key] = bound
            continue
        feature, fid, series = bound
        used.add(fid)
        report.features_scanned += 1
        signals = decomposer.detect_causal_breaks(
            series["value"].to_numpy(dtype=np.float64),
            dates=[_as_date(v) for v in series["obs_date"]],
            series_id=feature,
            min_confidence=min_confidence,
            pre_window=CAUSAL_PRE_WINDOW,
            post_window=CAUSAL_POST_WINDOW,
            peak_window=CAUSAL_PEAK_WINDOW,
        )
        report.breaks.extend(_feature_breaks(spec, feature, fid, series, signals, as_of))

    fresh = report.fresh_breaks
    report.regime_changes = [b.signal for b in fresh]
    report.changepoints_found = len(fresh)
    report.elapsed_seconds = _time.monotonic() - start
    if report.skipped:
        log.warning("Changepoint scan skipped {n} declared features: {s}",
                    n=len(report.skipped), s=report.skipped)
    log.info(
        "Changepoint scan as_of {d}: {n} features scanned, {c} breaks in lookback, {f} fresh, {t:.1f}s",
        d=as_of.isoformat(), n=report.features_scanned, c=len(report.breaks),
        f=len(fresh), t=report.elapsed_seconds,
    )
    return report


# ── Publish ─────────────────────────────────────────────────────────────

_INSERT_SQL = text("""
    INSERT INTO signal_registry
        (signal_id, source_module, signal_type, ticker, value,
         z_score, confidence, direction, valid_from, metadata, provenance)
    SELECT
        CAST(:sid AS text), CAST(:src AS text), CAST(:stype AS text), CAST(:ticker AS text),
        CAST(:val AS double precision), CAST(:zscore AS double precision),
        CAST(:conf AS double precision), CAST(:direction AS text),
        CAST(:vfrom AS timestamptz), CAST(:meta AS jsonb), CAST(:version AS text)
    WHERE NOT EXISTS (SELECT 1 FROM signal_registry WHERE signal_id = :sid)
    ON CONFLICT (signal_id, valid_from) DO NOTHING
""")


def _signal_params(b: FeatureBreak, as_of: date | None) -> dict[str, Any]:
    """Bind parameters for one break's row."""
    sig = b.signal
    statistic = float(np.clip(sig.statistic or 0.0, -_STAT_CLIP, _STAT_CLIP))
    risk = {"bearish": "risk_off", "bullish": "risk_on"}.get(b.direction, "none")
    meta = {
        "detector_version": CHANGEPOINT_DETECTOR_VERSION,
        "feature": b.feature,
        "feature_id": b.feature_id,
        "regime_key": b.spec.key,
        "concept": b.spec.concept,
        "stress_sign": b.spec.stress_sign,
        "risk": risk,
        "break_date": b.break_date.isoformat(),
        "confirm_date": b.confirm_date.isoformat(),
        "known_at_date": b.known_at_date.isoformat(),
        "as_of": as_of.isoformat() if as_of else None,
        "pre_regime": sig.pre_regime,
        "post_regime": sig.post_regime,
        "drift_change": sig.magnitude if statistic >= 0 else -sig.magnitude,
        "statistic": statistic,
        "windows": {"pre": CAUSAL_PRE_WINDOW, "post": CAUSAL_POST_WINDOW, "peak": CAUSAL_PEAK_WINDOW},
    }
    return {
        "sid": b.signal_id,
        "src": SOURCE_MODULE,
        "stype": SIGNAL_TYPE,
        "ticker": b.feature,
        "val": abs(statistic),
        "zscore": statistic,
        "conf": float(sig.confidence),
        "direction": b.direction,
        "vfrom": b.valid_from,
        "meta": json.dumps(meta, sort_keys=True),
        "version": CHANGEPOINT_DETECTOR_VERSION,
    }


def publish_regime_signals(
    engine: Engine,
    report: ChangeReport,
    *,
    now: datetime | None = None,
) -> int:
    """Write the report's fresh breaks to signal_registry.

    A break whose valid_from is after ``now`` is deferred to a later scan, so
    no row is ever valid from the future. A signal_id already present is not
    written again.

    Parameters:
        engine: SQLAlchemy database engine.
        report: ChangeReport from scan_for_changepoints.
        now: Current time (default: now, UTC).

    Returns:
        int: Number of rows inserted.
    """
    fresh = report.fresh_breaks
    if not fresh:
        return 0
    now = now or datetime.now(timezone.utc)
    due = [b for b in fresh if b.valid_from <= now]
    if len(due) < len(fresh):
        log.info("Changepoint publish: {n} fresh breaks not known yet (valid_from > now), deferred",
                 n=len(fresh) - len(due))

    published = 0
    with engine.begin() as conn:
        for b in due:
            result = conn.execute(_INSERT_SQL, _signal_params(b, report.as_of))
            published += max(0, int(result.rowcount or 0))

    if published:
        log.info("Published {n} regime change signals ({v})", n=published, v=CHANGEPOINT_DETECTOR_VERSION)
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
        "signals_published": published,
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
