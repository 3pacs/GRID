"""
Oracle prediction context enrichment.

Every oracle_predictions row needs the following 4 keys in its ``signals`` JSONB
payload so the 11-layer conviction stack can do regime-conditional Brier,
meta-learning, and per-signal bootstraps against it:

    * ``regime``               — 5-state liquidity regime label
                                 (CRISIS / TIGHTENING / NEUTRAL / EXPANSION / EXPANSION_STRONG)
    * ``fci_regime``           — Financial Conditions Index coarse regime
                                 (CRISIS / TIGHTENING / NEUTRAL / EXPANSION / EXPANSION_STRONG)
    * ``vix_level``            — latest PIT-correct VIX close (float | None)
    * ``signal_contributions`` — per-model shapley-style weight dict
                                 {model_name: weight}; populated from ensemble
                                 model weights (or per-prediction votes) when
                                 shapley attribution is unavailable

This module exposes two public helpers:

    build_prediction_context(engine, as_of=None, model_weights=None)
        Compute the 4-key context dict from the current DB state. Used by the
        live oracle writers (``oracle/engine.py``, ``oracle/publish.py``).

    enrich_signals_payload(signals_payload, context)
        Merge the context into an existing signals payload without overwriting
        any pre-existing keys. Returns a dict of shape
        ``{"items": [...], regime, fci_regime, vix_level, signal_contributions}``.

Every lookup is wrapped in try/except — a missing upstream table or an unexpected
DB shape must never break the prediction write path. Defaults are:
    regime="NEUTRAL", fci_regime="NEUTRAL", vix_level=None, signal_contributions={}
"""

from __future__ import annotations

from dataclasses import is_dataclass, asdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


DEFAULT_REGIME = "NEUTRAL"
DEFAULT_FCI_REGIME = "NEUTRAL"
VALID_REGIMES = (
    "CRISIS",
    "TIGHTENING",
    "NEUTRAL",
    "EXPANSION",
    "EXPANSION_STRONG",
)


# ── Canonical regime normalization ───────────────────────────────────────────

def canonical_regime(raw: Any) -> str:
    """Normalize an arbitrary regime label to one of the 5 canonical buckets.

    Mirrors ``features/regime_conditional_brier._canonical_regime`` semantics
    (that module is referenced but may not yet exist in every branch — we keep
    a local copy so the oracle write path never breaks on import).
    """
    if raw is None:
        return DEFAULT_REGIME
    try:
        token = str(raw).strip().upper()
    except Exception:
        return DEFAULT_REGIME
    if not token:
        return DEFAULT_REGIME

    # Direct match
    if token in VALID_REGIMES:
        return token

    # Common synonyms → canonical bucket
    if "CRISIS" in token or "STRESS" in token or "PANIC" in token:
        return "CRISIS"
    if "TIGHT" in token or "HIKING" in token or "RESTRICT" in token or "FRAGILE" in token or "RISK_OFF" in token:
        return "TIGHTENING"
    if "EXPANSION_STRONG" in token or "STRONG_EXPANSION" in token or "EASING_STRONG" in token:
        return "EXPANSION_STRONG"
    if "EXPANSION" in token or "EASING" in token or "GROWTH" in token or "RISK_ON" in token:
        return "EXPANSION"
    if "NEUTRAL" in token or "HOLDING" in token or "NORMAL" in token:
        return "NEUTRAL"

    return DEFAULT_REGIME


# ── Building block lookups (each wrapped for safety) ─────────────────────────

def _latest_feature_value(
    engine: Engine,
    feature_names: Iterable[str],
    as_of: date,
    lookback_days: int = 30,
) -> float | None:
    """PIT-correct latest close for the first matching feature name.

    Tries ``feature_registry`` first (V5 schema) then ``feature_catalog``
    (legacy). Returns ``None`` on any failure so callers can fall back to the
    safe default without raising.
    """
    cutoff = as_of - timedelta(days=lookback_days)
    for fname in feature_names:
        for registry_table in ("feature_registry", "feature_catalog"):
            try:
                query = text(
                    f"""
                    SELECT value
                    FROM resolved_series
                    WHERE feature_id IN (
                        SELECT id FROM {registry_table} WHERE name = :fname
                    )
                      AND obs_date <= :aod
                      AND obs_date >= :cut
                      AND release_date <= :aod
                    ORDER BY obs_date DESC, release_date DESC
                    LIMIT 1
                    """
                )
                with engine.connect() as conn:
                    row = conn.execute(
                        query, {"fname": fname, "aod": as_of, "cut": cutoff}
                    ).fetchone()
                if row is not None and row[0] is not None:
                    return float(row[0])
            except Exception as exc:  # pragma: no cover - defensive
                log.debug(
                    "latest_feature_value failed for {f} on {tbl}: {e}",
                    f=fname,
                    tbl=registry_table,
                    e=str(exc),
                )
                continue
    return None


def fetch_vix_level(engine: Engine, as_of: date) -> float | None:
    """Return the most recent PIT-correct VIX close, or None on failure."""
    try:
        return _latest_feature_value(
            engine,
            feature_names=("vix_close", "vix_level", "vix_full", "vix_spot", "vix"),
            as_of=as_of,
        )
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("fetch_vix_level failed: {e}", e=str(exc))
        return None


def fetch_liquidity_regime(engine: Engine, as_of: date) -> str:
    """Return a canonical 5-state liquidity regime.

    Resolution order:
      1. ``regime_history`` table (most recent label at/ before as_of).
      2. Rule-based classification from VIX + HY spread + net liquidity.
      3. ``DEFAULT_REGIME`` on any failure.
    """
    # 1. regime_history overlay — real schema is (obs_date, regime, confidence,
    # source) not (detected_at, regime_label).
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT regime
                    FROM regime_history
                    WHERE obs_date <= :aod
                    ORDER BY obs_date DESC
                    LIMIT 1
                    """
                ),
                {"aod": as_of},
            ).fetchone()
        if row is not None and row[0]:
            canon = canonical_regime(row[0])
            # Only trust the overlay when it carries an informative bucket.
            # A NEUTRAL overlay is indistinguishable from "no signal", so
            # we fall through to the VIX rule for dated dispersion.
            if canon != DEFAULT_REGIME:
                return canon
    except Exception as exc:
        log.debug("regime_history lookup skipped: {e}", e=str(exc))

    # 2. Rule-based from VIX
    try:
        vix = fetch_vix_level(engine, as_of)
        if vix is None:
            return DEFAULT_REGIME
        if vix >= 40:
            return "CRISIS"
        if vix >= 25:
            return "TIGHTENING"
        if vix >= 18:
            return "NEUTRAL"
        if vix >= 13:
            return "EXPANSION"
        return "EXPANSION_STRONG"
    except Exception as exc:  # pragma: no cover - defensive
        log.debug("rule-based regime classification failed: {e}", e=str(exc))

    return DEFAULT_REGIME


def fetch_fci_regime(engine: Engine, as_of: date) -> str:
    """Return a canonical FCI regime label.

    Tries a named FCI composite feature first; if unavailable, derives a coarse
    label from VIX + HY OAS. Falls back to ``DEFAULT_FCI_REGIME`` on failure.
    """
    try:
        fci_value = _latest_feature_value(
            engine,
            feature_names=(
                "fci_composite",
                "fci_nfci",
                "chicago_fed_nfci",
                "goldman_fci",
            ),
            as_of=as_of,
        )
        if fci_value is not None:
            # Chicago Fed NFCI semantics: positive = tightening, negative = expansion
            if fci_value >= 1.0:
                return "CRISIS"
            if fci_value >= 0.25:
                return "TIGHTENING"
            if fci_value >= -0.25:
                return "NEUTRAL"
            if fci_value >= -0.75:
                return "EXPANSION"
            return "EXPANSION_STRONG"
    except Exception as exc:
        log.debug("fci feature lookup failed: {e}", e=str(exc))

    # Derivation fallback: VIX + HY OAS
    try:
        vix = fetch_vix_level(engine, as_of)
        hy = _latest_feature_value(
            engine,
            feature_names=("hy_oas", "bamlh0a0hym2", "hy_spread", "hy_oas_level"),
            as_of=as_of,
        )
        if vix is None and hy is None:
            return DEFAULT_FCI_REGIME
        score = 0.0
        if vix is not None:
            score += (vix - 18.0) / 10.0
        if hy is not None:
            score += (hy - 4.0) / 2.0
        if score >= 2.0:
            return "CRISIS"
        if score >= 0.5:
            return "TIGHTENING"
        if score >= -0.5:
            return "NEUTRAL"
        if score >= -1.5:
            return "EXPANSION"
        return "EXPANSION_STRONG"
    except Exception as exc:  # pragma: no cover - defensive
        log.debug("fci derivation failed: {e}", e=str(exc))

    return DEFAULT_FCI_REGIME


# ── Signal contribution extraction ───────────────────────────────────────────

def extract_signal_contributions(
    *,
    shapley_contributions: Mapping[str, float] | None = None,
    model_weights: Mapping[str, float] | None = None,
    model_votes: Mapping[str, float] | None = None,
) -> dict[str, float]:
    """Return per-model weight dict suitable for per-signal Brier bootstrap.

    Priority:
      1. Shapley contributions (if the ensemble computed them).
      2. Per-prediction model votes.
      3. Global model weights (fallback — shape-preserving).
      4. Empty dict (safe default).

    The dict is normalized: every value is coerced to float, NaN/inf are
    dropped, and keys are non-empty strings.
    """
    for source in (shapley_contributions, model_votes, model_weights):
        if not source:
            continue
        try:
            cleaned: dict[str, float] = {}
            for k, v in dict(source).items():
                if not k:
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if fv != fv or fv in (float("inf"), float("-inf")):  # NaN/inf
                    continue
                cleaned[str(k)] = fv
            if cleaned:
                return cleaned
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("signal_contribution source coerce failed: {e}", e=str(exc))
            continue
    return {}


# ── Top-level context builder ────────────────────────────────────────────────

def build_prediction_context(
    engine: Engine | None,
    *,
    as_of: date | None = None,
    model_weights: Mapping[str, float] | None = None,
    model_votes: Mapping[str, float] | None = None,
    shapley_contributions: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Build the 4-key context dict merged into every oracle_predictions row.

    Never raises. On any lookup failure, defaults are used.
    """
    when = as_of or date.today()

    regime = DEFAULT_REGIME
    fci_regime = DEFAULT_FCI_REGIME
    vix_level: float | None = None

    if engine is not None:
        try:
            regime = canonical_regime(fetch_liquidity_regime(engine, when))
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("regime lookup failed, defaulting to NEUTRAL: {e}", e=str(exc))
            regime = DEFAULT_REGIME
        try:
            fci_regime = canonical_regime(fetch_fci_regime(engine, when))
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("fci lookup failed, defaulting to NEUTRAL: {e}", e=str(exc))
            fci_regime = DEFAULT_FCI_REGIME
        try:
            vix_level = fetch_vix_level(engine, when)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("vix lookup failed, defaulting to None: {e}", e=str(exc))
            vix_level = None

    contributions = extract_signal_contributions(
        shapley_contributions=shapley_contributions,
        model_weights=model_weights,
        model_votes=model_votes,
    )

    return {
        "regime": regime,
        "fci_regime": fci_regime,
        "vix_level": vix_level,
        "signal_contributions": contributions,
    }


# ── Signals payload merger ───────────────────────────────────────────────────

def _as_list_of_dicts(value: Any) -> list[dict[str, Any]]:
    """Coerce a signals list to plain JSON-safe dicts."""
    out: list[dict[str, Any]] = []
    if not value:
        return out
    try:
        for item in value:
            if isinstance(item, dict):
                out.append(item)
            elif is_dataclass(item):
                out.append(asdict(item))
            else:  # pragma: no cover - defensive
                try:
                    out.append(dict(item))
                except Exception:
                    continue
    except TypeError:  # pragma: no cover - defensive
        pass
    return out


def enrich_signals_payload(
    signals_payload: Any,
    context: Mapping[str, Any],
) -> dict[str, Any]:
    """Merge ``context`` into ``signals_payload`` without overwriting existing keys.

    ``signals_payload`` may be:
      * a list of signal dicts/dataclasses (legacy shape) — wrapped under ``items``
      * an existing dict — keys are preserved, context only fills gaps
      * ``None`` / empty — an empty dict is used

    Returns a new dict with shape::

        {
            "items": [...signals...],
            "regime": "NEUTRAL",
            "fci_regime": "NEUTRAL",
            "vix_level": 18.5 | None,
            "signal_contributions": {...},
            # plus any pre-existing keys from signals_payload
        }

    Pre-existing keys in ``signals_payload`` are never overwritten. Immutability:
    a new dict is returned, the input is not mutated.
    """
    if isinstance(signals_payload, Mapping):
        merged: dict[str, Any] = dict(signals_payload)
        if "items" not in merged:
            # dict without items → preserve as-is, don't invent a fake list
            merged.setdefault("items", [])
    elif signals_payload is None:
        merged = {"items": []}
    else:
        merged = {"items": _as_list_of_dicts(signals_payload)}

    for key, value in context.items():
        merged.setdefault(key, value)

    return merged
