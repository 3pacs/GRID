"""
Contra-indicator ensemble — "who is on the wrong side?" detector.

CAT-184 (#286). Complements ``intelligence/consensus_crowdedness.py`` (CAT-182).

Some actors are historically wrong more often than right:
  - Retail options flow at tops
  - AAII bulls at bottoms
  - Sell-side consensus around recessions
  - Momentum-chasing funds into mean-reverting regimes

When a contrarian indicator flips extreme, it is evidence to LEAN INTO the
opposite direction of what they are doing. Where ``consensus_crowdedness``
merely flags "is everyone on the same trade", this module goes further: it
names the direction of the contrarian trade and scores how much to weight it.

The public entry points are :func:`build_contra_report` (full analytical
report) and :func:`contra_conviction_multiplier` (live path convenience used
by ``signal_provenance.build_provenance_report``). Both functions are
PIT-correct via :class:`store.pit.PITStore` and never raise — any failure
degrades gracefully to a neutral report with ``ensemble_score = 0``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Callable, Mapping

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore

# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

#: Rolling window length (in calendar days) used for z-score normalization.
#: Strictly historical — only observations with obs_date < as_of_date feed the
#: mean/std (PIT safety).
NORMALIZATION_WINDOW_DAYS: int = 252

#: Minimum number of historical observations required to compute a stable
#: z-score. Below this, the indicator is treated as missing.
MIN_HISTORY_OBS: int = 30

#: Clamp bound for z-scores. Prevents a handful of outliers from dominating
#: the ensemble.
Z_CLAMP: float = 4.0

#: Ensemble score above which the ensemble is read as "contrarian bullish".
BULLISH_THRESHOLD: float = 0.25

#: Ensemble score below which the ensemble is read as "contrarian bearish".
BEARISH_THRESHOLD: float = -0.25

#: Sentinel used for missing / indeterminate z-scores.
NEUTRAL_Z: float = 0.0

#: Sentinel used for missing contributions.
NEUTRAL_CONTRIBUTION: float = 0.0

#: Direction constants for ``contrarian_direction``.
CONTRA_BULLISH: int = 1   # high z => contrarian bullish trade
CONTRA_BEARISH: int = -1  # high z => contrarian bearish trade
SAME_DIRECTION: int = 0   # not contrarian — follow the sign of z directly


@dataclass(frozen=True)
class IndicatorSpec:
    """Static configuration for one contrarian indicator.

    Attributes:
        name: Canonical short name used on report objects and in dicts.
        feature_registry_name: Exact ``feature_registry.name`` the puller
            writes into ``resolved_series``.
        threshold: Absolute z-score gate. Below this, the indicator emits a
            zero contribution and ``active = False``.
        contrarian_direction: +1 if high z means contrarian bullish, -1 if
            high z means contrarian bearish, 0 for same-direction indicators
            (only ``smart_money_flow_index``).
        description: Human-readable rationale for the sign convention.
    """

    name: str
    feature_registry_name: str
    threshold: float
    contrarian_direction: int
    description: str


#: The six indicators used by the ensemble. Thresholds and directions are
#: hard-coded here (per spec) and individually defensible:
#:
#:   * aaii_bull_bear_spread: retail survey. Bullish extreme -> fade.
#:   * bofa_sellside_indicator: sell-side allocation survey. High = bearish
#:     analyst consensus -> contrarian bullish.
#:   * put_call_ratio_10d: 10-day smoothed p/c. Extreme put buying -> fade.
#:   * retail_options_net_call_volume: retail piling into calls -> fade.
#:   * smart_money_flow_index: SAME direction — this is the institutional
#:     companion that goes with its own z-sign.
#:   * cftc_noncomm_net_long: speculator positioning. Max long -> fade.
INDICATOR_SPECS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec(
        name="aaii_bull_bear_spread",
        feature_registry_name="aaii_bull_bear_spread",
        threshold=2.0,
        contrarian_direction=CONTRA_BEARISH,
        description=(
            "AAII retail bull-bear spread. Extreme retail bullishness is a "
            "contrarian sell signal; extreme retail bearishness a contrarian "
            "buy signal."
        ),
    ),
    IndicatorSpec(
        name="bofa_sellside_indicator",
        feature_registry_name="bofa_sellside_indicator",
        threshold=1.5,
        contrarian_direction=CONTRA_BULLISH,
        description=(
            "BofA Sell-Side Indicator. High = bearish analyst positioning "
            "(wall-of-worry) = contrarian bullish."
        ),
    ),
    IndicatorSpec(
        name="put_call_ratio_10d",
        feature_registry_name="put_call_ratio_10d",
        threshold=1.5,
        contrarian_direction=CONTRA_BULLISH,
        description=(
            "10-day smoothed put/call ratio. Extreme put buying is a "
            "contrarian bullish signal; extreme call buying is a contrarian "
            "bearish signal."
        ),
    ),
    IndicatorSpec(
        name="retail_options_net_call_volume",
        feature_registry_name="retail_options_net_call_volume",
        threshold=2.0,
        contrarian_direction=CONTRA_BEARISH,
        description=(
            "Retail options flow net call volume. Retail piling into calls "
            "is a contrarian bearish signal."
        ),
    ),
    IndicatorSpec(
        name="smart_money_flow_index",
        feature_registry_name="smart_money_flow_index",
        threshold=1.0,
        contrarian_direction=SAME_DIRECTION,
        description=(
            "Smart money flow index — the institutional companion to retail. "
            "Same-direction indicator: a positive z means lean bullish."
        ),
    ),
    IndicatorSpec(
        name="cftc_noncomm_net_long",
        feature_registry_name="cftc_noncomm_net_long",
        threshold=2.0,
        contrarian_direction=CONTRA_BEARISH,
        description=(
            "CFTC COT non-commercial net long. Extreme speculator long "
            "positioning is a contrarian bearish signal."
        ),
    ),
)


# ---------------------------------------------------------------------------
# Conviction multiplier lookup
# ---------------------------------------------------------------------------

#: Ceiling multiplier applied when the trade is ALIGNED with a strong
#: contrarian bias (``n_active >= 4`` AND ``|ensemble_score| >= 0.5``).
MULT_STRONG_ALIGNED: float = 1.15

#: Medium-strength aligned multiplier.
MULT_MEDIUM_ALIGNED: float = 1.08

#: Neutral multiplier — no contrarian conviction either way.
MULT_NEUTRAL: float = 1.00

#: Penalty applied when the trade is OPPOSITE a strong contrarian bias.
MULT_STRONG_OPPOSED: float = 0.85

#: Smaller penalty for medium opposition.
MULT_MEDIUM_OPPOSED: float = 0.92

#: Threshold on ``|ensemble_score|`` below which we treat the ensemble as
#: effectively neutral regardless of ``n_active``.
NEUTRAL_SCORE_EPSILON: float = 0.10

#: Strong-conviction score threshold.
STRONG_SCORE_ABS: float = 0.5

#: Medium-conviction score threshold.
MEDIUM_SCORE_ABS: float = 0.25

#: Minimum ``n_active`` required for strong aligned multiplier.
STRONG_N_ACTIVE: int = 4

#: Minimum ``n_active`` required for medium aligned multiplier.
MEDIUM_N_ACTIVE: int = 3


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ContraIndicator:
    """Per-indicator slice of the ensemble report.

    Attributes:
        name: Indicator short name.
        feature_registry_name: Source feature in ``feature_registry``.
        z_score: Clamped to ``[-Z_CLAMP, +Z_CLAMP]``. ``0.0`` on missing data.
        threshold: Absolute z gate (same value as the spec).
        contrarian_direction: +1, -1 or 0.
        contribution: Signed contribution in ``[-1.0, +1.0]`` where positive
            means "lean contrarian bullish" and negative "lean contrarian
            bearish".
        active: ``True`` iff ``|z_score| > threshold``.
    """

    name: str
    feature_registry_name: str
    z_score: float
    threshold: float
    contrarian_direction: int
    contribution: float
    active: bool

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict for JSON encoding."""
        return {
            "name": self.name,
            "feature_registry_name": self.feature_registry_name,
            "z_score": self.z_score,
            "threshold": self.threshold,
            "contrarian_direction": self.contrarian_direction,
            "contribution": self.contribution,
            "active": self.active,
        }


@dataclass(frozen=True)
class ContraEnsembleReport:
    """Aggregate report across the six contrarian indicators.

    Attributes:
        as_of: ISO date string for the decision timestamp.
        indicators: Per-indicator records, one per spec (always 6).
        ensemble_score: Mean of ``contribution_i`` across indicators, in
            ``[-1.0, +1.0]``.
        n_active: Count of indicators whose ``|z|`` exceeded their threshold.
        contrarian_bias: ``"bullish"``, ``"bearish"`` or ``"neutral"``.
        advisory: Human-readable one-liner.
        missing_indicator_count: Number of indicators whose PIT history was
            unavailable at ``as_of``.
    """

    as_of: str
    indicators: list[ContraIndicator]
    ensemble_score: float
    n_active: int
    contrarian_bias: str
    advisory: str
    missing_indicator_count: int

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict for JSON encoding."""
        return {
            "as_of": self.as_of,
            "indicators": [i.to_dict() for i in self.indicators],
            "ensemble_score": self.ensemble_score,
            "n_active": self.n_active,
            "contrarian_bias": self.contrarian_bias,
            "advisory": self.advisory,
            "missing_indicator_count": self.missing_indicator_count,
        }


# ---------------------------------------------------------------------------
# Math helpers (pure — no DB)
# ---------------------------------------------------------------------------


def _clamp(value: float, lo: float, hi: float) -> float:
    """Clamp ``value`` into the closed interval ``[lo, hi]``."""
    if value != value:  # NaN guard without importing math
        return 0.0
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


def _compute_zscore(
    history: pd.Series,
    latest_value: float,
) -> float:
    """Return the clamped z-score of ``latest_value`` against ``history``.

    ``history`` is expected to contain only observations strictly BEFORE the
    as-of date — the caller enforces this. Returns :data:`NEUTRAL_Z` if there
    is insufficient history or std is zero/NaN.
    """
    if history is None or len(history) < MIN_HISTORY_OBS:
        return NEUTRAL_Z

    mean = float(history.mean())
    std = float(history.std(ddof=0))

    if std != std or std <= 0.0:  # NaN or zero std
        return NEUTRAL_Z
    if latest_value != latest_value:  # NaN
        return NEUTRAL_Z

    raw_z = (latest_value - mean) / std
    return _clamp(raw_z, -Z_CLAMP, Z_CLAMP)


def _contribution_from_z(
    z_score: float,
    threshold: float,
    contrarian_direction: int,
) -> float:
    """Compute the signed contribution for one indicator.

    The formula (per spec):

    .. code-block:: text

        contribution = clamp(
            contrarian_direction * (|z| - threshold) / (Z_CLAMP - threshold),
            -1, +1,
        )

    If ``|z|`` is at or below the threshold the indicator does NOT contribute
    (returns ``0.0``). ``SAME_DIRECTION`` indicators track the sign of z
    directly instead of inverting it.
    """
    abs_z = abs(z_score)
    if abs_z <= threshold:
        return NEUTRAL_CONTRIBUTION

    denom = Z_CLAMP - threshold
    if denom <= 0.0:
        return NEUTRAL_CONTRIBUTION

    magnitude = (abs_z - threshold) / denom

    if contrarian_direction == SAME_DIRECTION:
        # Smart money: follow the sign of z, not its inverse.
        sign = 1.0 if z_score >= 0 else -1.0
        return _clamp(sign * magnitude, -1.0, 1.0)

    # Contrarian: positive z AND contrarian_direction +1 => lean bullish (+);
    # positive z AND contrarian_direction -1 => lean bearish (-);
    # negative z symmetrically flips the sign.
    sign = 1.0 if z_score >= 0 else -1.0
    raw = contrarian_direction * sign * magnitude
    return _clamp(raw, -1.0, 1.0)


def _classify_bias(ensemble_score: float) -> str:
    """Map an ensemble score to a bias label."""
    if ensemble_score > BULLISH_THRESHOLD:
        return "bullish"
    if ensemble_score < BEARISH_THRESHOLD:
        return "bearish"
    return "neutral"


def _build_advisory(
    bias: str,
    ensemble_score: float,
    n_active: int,
    missing: int,
) -> str:
    """Compose a short human-readable advisory for the report."""
    if missing == len(INDICATOR_SPECS):
        return (
            "Contra-indicator ensemble: no data available — neutral stance."
        )
    if bias == "neutral":
        return (
            f"Contra-indicator ensemble neutral "
            f"(score={ensemble_score:+.2f}, active={n_active})."
        )
    direction_word = "bullish" if bias == "bullish" else "bearish"
    return (
        f"Contra-indicator ensemble leans {direction_word} "
        f"(score={ensemble_score:+.2f}, active={n_active}, "
        f"missing={missing}). Contrarian read suggests "
        f"fading the crowd toward {direction_word}."
    )


# ---------------------------------------------------------------------------
# Multiplier lookup (pure)
# ---------------------------------------------------------------------------


def _multiplier_from_report(
    report: ContraEnsembleReport,
    trade_direction: str,
) -> float:
    """Translate a report + trade direction into a conviction multiplier.

    Pure function. Unknown / ``"flat"`` / empty trade directions always map to
    :data:`MULT_NEUTRAL`.
    """
    direction = (trade_direction or "").strip().lower()
    if direction not in ("bullish", "bearish"):
        return MULT_NEUTRAL

    score = report.ensemble_score
    abs_score = abs(score)

    # Neutral-epsilon short-circuit: the ensemble has nothing to say.
    if abs_score < NEUTRAL_SCORE_EPSILON:
        return MULT_NEUTRAL

    bias = report.contrarian_bias
    if bias == "neutral":
        return MULT_NEUTRAL

    aligned = bias == direction

    if aligned:
        if report.n_active >= STRONG_N_ACTIVE and abs_score >= STRONG_SCORE_ABS:
            return MULT_STRONG_ALIGNED
        if report.n_active >= MEDIUM_N_ACTIVE and abs_score >= MEDIUM_SCORE_ABS:
            return MULT_MEDIUM_ALIGNED
        return MULT_NEUTRAL

    # Opposed.
    if abs_score >= STRONG_SCORE_ABS:
        return MULT_STRONG_OPPOSED
    if abs_score >= MEDIUM_SCORE_ABS:
        return MULT_MEDIUM_OPPOSED
    return MULT_NEUTRAL


# ---------------------------------------------------------------------------
# PIT data access helpers
# ---------------------------------------------------------------------------


def _resolve_feature_ids(
    engine: Engine,
    feature_names: list[str],
) -> dict[str, int]:
    """Look up ``feature_registry.id`` for each requested name.

    Returns a ``{name: id}`` dict containing only the names that resolved.
    Parameterized SQL — never interpolates strings. Any DB error is logged
    and returns an empty dict so the caller falls back to neutral.
    """
    resolved: dict[str, int] = {}
    if not feature_names:
        return resolved

    query = text(
        "SELECT id, name FROM feature_registry "
        "WHERE name = ANY(:names)"
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"names": list(feature_names)}).fetchall()
    except Exception as exc:  # pragma: no cover - exercised via mocks
        log.warning(
            "contra_indicator_ensemble: feature_registry lookup failed: {e}",
            e=exc,
        )
        return resolved

    for row in rows:
        # row may be a Row object or a plain tuple (test fakes).
        try:
            fid = int(row[0])
            fname = str(row[1])
        except (IndexError, TypeError, ValueError):
            continue
        resolved[fname] = fid
    return resolved


def _latest_and_history(
    pit_df: pd.DataFrame,
    as_of: date,
) -> tuple[float | None, pd.Series | None]:
    """Split a PIT-correct dataframe into (latest_value, prior_history).

    ``latest_value`` is the observation with the greatest ``obs_date`` that
    is still strictly less than ``as_of``. ``history`` contains every prior
    observation within the :data:`NORMALIZATION_WINDOW_DAYS` window — again
    strictly BEFORE ``as_of`` (no same-day leakage).
    """
    if pit_df is None or pit_df.empty:
        return None, None

    df = pit_df.copy()
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    # Strict PIT: drop any row on or after as_of. The PIT store already
    # filters release_date; we additionally guard obs_date here.
    df = df[df["obs_date"] < as_of]
    if df.empty:
        return None, None

    df = df.sort_values("obs_date")
    window_start = as_of - timedelta(days=NORMALIZATION_WINDOW_DAYS)
    window = df[df["obs_date"] >= window_start]
    if window.empty:
        return None, None

    try:
        latest_value = float(window.iloc[-1]["value"])
    except (TypeError, ValueError):
        return None, None

    # History feeding the z-score is everything EXCEPT the latest observation,
    # so the numerator value does not appear in the denominator statistics.
    history = window.iloc[:-1]["value"].astype(float)
    history = history.dropna()
    return latest_value, history


def _evaluate_indicator(
    spec: IndicatorSpec,
    pit_df: pd.DataFrame | None,
    as_of: date,
) -> tuple[ContraIndicator, bool]:
    """Score one indicator.

    Returns ``(ContraIndicator, missing)`` where ``missing`` is True iff the
    PIT lookup returned nothing usable. A missing indicator contributes 0 to
    the ensemble score but is still returned as a record.
    """
    if pit_df is None or pit_df.empty:
        neutral = ContraIndicator(
            name=spec.name,
            feature_registry_name=spec.feature_registry_name,
            z_score=NEUTRAL_Z,
            threshold=spec.threshold,
            contrarian_direction=spec.contrarian_direction,
            contribution=NEUTRAL_CONTRIBUTION,
            active=False,
        )
        return neutral, True

    latest, history = _latest_and_history(pit_df, as_of)
    if latest is None or history is None:
        return (
            ContraIndicator(
                name=spec.name,
                feature_registry_name=spec.feature_registry_name,
                z_score=NEUTRAL_Z,
                threshold=spec.threshold,
                contrarian_direction=spec.contrarian_direction,
                contribution=NEUTRAL_CONTRIBUTION,
                active=False,
            ),
            True,
        )

    z_score = _compute_zscore(history, latest)
    if z_score == NEUTRAL_Z and len(history) < MIN_HISTORY_OBS:
        return (
            ContraIndicator(
                name=spec.name,
                feature_registry_name=spec.feature_registry_name,
                z_score=NEUTRAL_Z,
                threshold=spec.threshold,
                contrarian_direction=spec.contrarian_direction,
                contribution=NEUTRAL_CONTRIBUTION,
                active=False,
            ),
            True,
        )

    contribution = _contribution_from_z(
        z_score=z_score,
        threshold=spec.threshold,
        contrarian_direction=spec.contrarian_direction,
    )
    active = abs(z_score) > spec.threshold
    return (
        ContraIndicator(
            name=spec.name,
            feature_registry_name=spec.feature_registry_name,
            z_score=z_score,
            threshold=spec.threshold,
            contrarian_direction=spec.contrarian_direction,
            contribution=contribution,
            active=active,
        ),
        False,
    )


def _neutral_report(as_of: date, reason: str) -> ContraEnsembleReport:
    """Build a fully-neutral report (used for any failure path)."""
    indicators = [
        ContraIndicator(
            name=spec.name,
            feature_registry_name=spec.feature_registry_name,
            z_score=NEUTRAL_Z,
            threshold=spec.threshold,
            contrarian_direction=spec.contrarian_direction,
            contribution=NEUTRAL_CONTRIBUTION,
            active=False,
        )
        for spec in INDICATOR_SPECS
    ]
    return ContraEnsembleReport(
        as_of=as_of.isoformat() if isinstance(as_of, date) else str(as_of),
        indicators=indicators,
        ensemble_score=0.0,
        n_active=0,
        contrarian_bias="neutral",
        advisory=f"Contra-indicator ensemble neutral — {reason}.",
        missing_indicator_count=len(INDICATOR_SPECS),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_contra_report(
    engine: Engine,
    *,
    as_of: date,
    pit_store: PITStore | None = None,
) -> ContraEnsembleReport:
    """PIT-correct ensemble across the six contrarian indicators.

    Parameters:
        engine: SQLAlchemy engine against the GRID database.
        as_of: Decision date. No data with ``release_date > as_of`` or
            ``obs_date >= as_of`` is considered.
        pit_store: Optional pre-built :class:`PITStore` — mainly for tests.
            If omitted, one is constructed from ``engine``.

    Returns:
        :class:`ContraEnsembleReport`. Never raises. On any internal error
        returns a neutral report with ``ensemble_score = 0.0``.
    """
    try:
        store = pit_store if pit_store is not None else PITStore(engine)
    except Exception as exc:  # pragma: no cover
        log.warning(
            "contra_indicator_ensemble: PITStore init failed: {e}", e=exc
        )
        return _neutral_report(as_of, "PITStore unavailable")

    feature_names = [spec.feature_registry_name for spec in INDICATOR_SPECS]
    try:
        name_to_id = _resolve_feature_ids(engine, feature_names)
    except Exception as exc:  # pragma: no cover
        log.warning(
            "contra_indicator_ensemble: feature id resolution failed: {e}",
            e=exc,
        )
        return _neutral_report(as_of, "feature_registry unavailable")

    # Cache PIT lookups by feature_id — if the same feature were ever
    # referenced twice in INDICATOR_SPECS (not today, but guard anyway) we
    # hit PIT once per feature.
    pit_cache: dict[int, pd.DataFrame] = {}

    indicators: list[ContraIndicator] = []
    missing_count = 0
    active_count = 0
    contributions_sum = 0.0

    for spec in INDICATOR_SPECS:
        fid = name_to_id.get(spec.feature_registry_name)
        if fid is None:
            indicator, missing = _evaluate_indicator(spec, None, as_of)
        else:
            if fid not in pit_cache:
                try:
                    pit_cache[fid] = store.get_pit(
                        [fid], as_of, vintage_policy="LATEST_AS_OF"
                    )
                except Exception as exc:
                    log.warning(
                        "contra_indicator_ensemble: PIT lookup failed for "
                        "feature '{n}' (id={i}): {e}",
                        n=spec.feature_registry_name,
                        i=fid,
                        e=exc,
                    )
                    pit_cache[fid] = pd.DataFrame()
            indicator, missing = _evaluate_indicator(
                spec, pit_cache[fid], as_of
            )

        indicators.append(indicator)
        contributions_sum += indicator.contribution
        if missing:
            missing_count += 1
        if indicator.active:
            active_count += 1

    n_specs = len(INDICATOR_SPECS)
    ensemble_score = contributions_sum / n_specs if n_specs > 0 else 0.0
    bias = _classify_bias(ensemble_score)
    advisory = _build_advisory(bias, ensemble_score, active_count, missing_count)

    return ContraEnsembleReport(
        as_of=as_of.isoformat(),
        indicators=indicators,
        ensemble_score=ensemble_score,
        n_active=active_count,
        contrarian_bias=bias,
        advisory=advisory,
        missing_indicator_count=missing_count,
    )


def contra_conviction_multiplier(
    engine: Engine,
    *,
    as_of: date,
    trade_direction: str,
) -> float:
    """Live-path convenience. Returns a multiplier to apply to conviction.

    Callers (e.g. ``signal_provenance.build_provenance_report``) can do::

        mult = contra_conviction_multiplier(
            engine, as_of=today, trade_direction='bullish',
        )
        conviction *= mult

    Never raises. Returns :data:`MULT_NEUTRAL` (``1.0``) on any failure or
    unknown ``trade_direction``.
    """
    try:
        report = build_contra_report(engine, as_of=as_of)
    except Exception as exc:  # pragma: no cover - build_contra_report never raises
        log.warning(
            "contra_indicator_ensemble: build_contra_report raised: {e}",
            e=exc,
        )
        return MULT_NEUTRAL

    try:
        return _multiplier_from_report(report, trade_direction)
    except Exception as exc:  # pragma: no cover
        log.warning(
            "contra_indicator_ensemble: multiplier lookup failed: {e}",
            e=exc,
        )
        return MULT_NEUTRAL


__all__ = [
    "NORMALIZATION_WINDOW_DAYS",
    "MIN_HISTORY_OBS",
    "Z_CLAMP",
    "BULLISH_THRESHOLD",
    "BEARISH_THRESHOLD",
    "CONTRA_BULLISH",
    "CONTRA_BEARISH",
    "SAME_DIRECTION",
    "MULT_STRONG_ALIGNED",
    "MULT_MEDIUM_ALIGNED",
    "MULT_NEUTRAL",
    "MULT_STRONG_OPPOSED",
    "MULT_MEDIUM_OPPOSED",
    "NEUTRAL_SCORE_EPSILON",
    "STRONG_SCORE_ABS",
    "MEDIUM_SCORE_ABS",
    "STRONG_N_ACTIVE",
    "MEDIUM_N_ACTIVE",
    "IndicatorSpec",
    "INDICATOR_SPECS",
    "ContraIndicator",
    "ContraEnsembleReport",
    "build_contra_report",
    "contra_conviction_multiplier",
]
