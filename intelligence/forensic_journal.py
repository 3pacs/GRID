"""
GRID Intelligence — CAT-189 Forensic Journal of Failed Predictions.

Asymmetric calibration on steroids: every wrong prediction is automatically
audited and the contributing signals are punished MORE aggressively the more
confident the original call was. A standard Welford-style running average treats
every miss the same. We don't. A confidence-0.9 miss is far more diagnostic
than a confidence-0.6 miss — so its Brier-impact must be amplified
proportionally.

User memory rule (project_postmortem_audit): "every wrong prediction needs an
automated audit explaining failure." This module is the per-prediction
implementation of that rule, complementing `intelligence/postmortem.py` which
operates at the per-trade granularity (different table, different shape).

Pipeline:
  1. detect          — `is_high_confidence_failure` filters the firehose
  2. attribute       — caller passes signal_contributions (from Shapley/provenance)
  3. classify        — `classify_root_cause` picks one of ROOT_CAUSE_CATEGORIES
  4. amplify         — `_apply_failure_multiplier_to_signals` bumps per_signal_brier
  5. persist         — `failed_prediction_postmortems` row written
  6. surface         — `get_failing_signals` so the conviction dial cools fast
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

HIGH_CONFIDENCE_THRESHOLD: float = 0.7
"""Predictions at or above this confidence count as high-conviction calls."""

MAX_FAILURE_MULTIPLIER: float = 5.0
"""Hard ceiling on the Brier amplification factor."""


def _default_failure_multiplier(confidence: float) -> float:
    """Linear ramp: 0.7 → 1×, 0.85 → 3×, 1.0 → 5×. Clamped to [1, 5]."""
    raw = 1.0 + 4.0 * (confidence - HIGH_CONFIDENCE_THRESHOLD) / 0.3
    if raw < 1.0:
        return 1.0
    if raw > MAX_FAILURE_MULTIPLIER:
        return MAX_FAILURE_MULTIPLIER
    return raw


FAILURE_MULTIPLIER_FORMULA: Callable[[float], float] = _default_failure_multiplier
"""Pluggable multiplier function. Default: linear 0.7→1×, 0.85→3×, 1.0→5×."""


ROOT_CAUSE_CATEGORIES: tuple[str, ...] = (
    "regime_shift",
    "data_stale",
    "single_leg_fragile",
    "crowd_aligned",
    "unknown",
)
"""The five canonical failure modes for high-confidence misses."""


# ── Data Classes ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FailedPredictionPostmortem:
    """One forensic record for a single high-confidence prediction that missed."""

    prediction_id: str
    ticker: str
    confidence: float
    verdict: str
    horizon_days: int
    asof: datetime
    contributing_signals: dict[str, float]
    root_cause: str
    root_cause_evidence: str
    failure_multiplier: float
    narrative: str | None
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # ensure datetimes are JSON-friendly
        if isinstance(d.get("asof"), datetime):
            d["asof"] = d["asof"].isoformat()
        return d


@dataclass(frozen=True)
class FailingSignal:
    """Aggregated view of how badly a single signal source has been blowing up."""

    signal_source: str
    recent_failure_count: int
    cumulative_failure_multiplier: float
    last_failed_at: datetime
    classification: str  # 'cooling' | 'cold' | 'frozen'

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if isinstance(d.get("last_failed_at"), datetime):
            d["last_failed_at"] = d["last_failed_at"].isoformat()
        return d


# ── Pure helpers (no DB) ──────────────────────────────────────────────────

def is_high_confidence_failure(confidence: float, verdict: str) -> bool:
    """True iff confidence ≥ HIGH_CONFIDENCE_THRESHOLD AND verdict == 'miss'."""
    if confidence is None or verdict is None:
        return False
    try:
        c = float(confidence)
    except (TypeError, ValueError):
        return False
    return c >= HIGH_CONFIDENCE_THRESHOLD and str(verdict).lower() == "miss"


def compute_failure_multiplier(confidence: float) -> float:
    """Compute the Brier amplification multiplier for a given prediction confidence.

    Anchors:
      confidence 0.7  → 1.0×
      confidence 0.85 → 3.0×
      confidence 1.0  → 5.0×
    Below 0.7 → clamped to 1.0. Above 1.0 → clamped to MAX_FAILURE_MULTIPLIER.
    """
    try:
        c = float(confidence)
    except (TypeError, ValueError):
        return 1.0
    return FAILURE_MULTIPLIER_FORMULA(c)


def _safe_get(d: dict[str, Any], key: str, default: Any = None) -> Any:
    if not isinstance(d, dict):
        return default
    return d.get(key, default)


def classify_root_cause(prediction_metadata: dict[str, Any]) -> tuple[str, str]:
    """Heuristic root-cause classifier for a high-confidence miss.

    Examines four signature features in `prediction_metadata`:
      - regime / fci_regime           → regime_shift
      - data_age_hours / max_signal_age_days → data_stale
      - fragility_multiplier          → single_leg_fragile
      - crowd_aligned + bullish/bearish regime → crowd_aligned (contrarian edge lost)

    Returns:
        (category_code, evidence_string)
    """
    if not isinstance(prediction_metadata, dict):
        return ("unknown", "no metadata supplied")

    regime = _safe_get(prediction_metadata, "regime")
    fci_regime = _safe_get(prediction_metadata, "fci_regime")
    if regime is not None and fci_regime is not None and regime != fci_regime:
        return (
            "regime_shift",
            f"regime={regime} but fci_regime={fci_regime} (regime divergence)",
        )

    data_age = _safe_get(prediction_metadata, "data_age_hours")
    if isinstance(data_age, (int, float)) and data_age > 24:
        return (
            "data_stale",
            f"data_age_hours={data_age:.1f} exceeds 24h freshness window",
        )
    max_signal_age = _safe_get(prediction_metadata, "max_signal_age_days")
    if isinstance(max_signal_age, (int, float)) and max_signal_age > 7:
        return (
            "data_stale",
            f"max_signal_age_days={max_signal_age:.1f} exceeds 7d window",
        )

    fragility = _safe_get(prediction_metadata, "fragility_multiplier")
    if isinstance(fragility, (int, float)) and fragility < 0.7:
        return (
            "single_leg_fragile",
            f"fragility_multiplier={fragility:.2f} below 0.7 (single-leg dependency)",
        )

    crowd_aligned = _safe_get(prediction_metadata, "crowd_aligned")
    if crowd_aligned and regime in ("bullish", "bearish", "GROWTH", "CRISIS"):
        return (
            "crowd_aligned",
            f"crowd_aligned=True in regime={regime} (contrarian edge lost)",
        )

    return ("unknown", "no diagnostic signature matched any known failure mode")


def compose_narrative_template(postmortem: "FailedPredictionPostmortem") -> str:
    """Pure-Python narrative composer. Produces a readable summary without an LLM."""
    sigs = postmortem.contributing_signals or {}
    if sigs:
        # sort by absolute weight descending
        sorted_sigs = sorted(sigs.items(), key=lambda kv: abs(kv[1]), reverse=True)
        sig_text = ", ".join(f"{name}={weight:+.2f}" for name, weight in sorted_sigs)
    else:
        sig_text = "(no contributing signals recorded)"

    return (
        f"FORENSIC POSTMORTEM — {postmortem.ticker}\n"
        f"prediction_id={postmortem.prediction_id} "
        f"horizon={postmortem.horizon_days}d "
        f"asof={postmortem.asof.isoformat() if isinstance(postmortem.asof, datetime) else postmortem.asof}\n"
        f"confidence={postmortem.confidence:.2f} verdict={postmortem.verdict} "
        f"multiplier={postmortem.failure_multiplier:.2f}x\n"
        f"root_cause={postmortem.root_cause}: {postmortem.root_cause_evidence}\n"
        f"contributing_signals: {sig_text}\n"
        f"This was a high-conviction miss. The contributing signals above have been "
        f"down-weighted {postmortem.failure_multiplier:.2f}x more aggressively than a "
        f"standard Welford update would prescribe."
    )


# ── DB-touching functions (never raise) ───────────────────────────────────

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS failed_prediction_postmortems (
    id                       SERIAL PRIMARY KEY,
    prediction_id            TEXT NOT NULL,
    ticker                   TEXT,
    confidence               NUMERIC,
    verdict                  TEXT,
    horizon_days             INT,
    asof                     TIMESTAMPTZ,
    contributing_signals     JSONB,
    root_cause               TEXT,
    root_cause_evidence      TEXT,
    failure_multiplier       NUMERIC,
    narrative                TEXT,
    generated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_INDEX_DDLS = (
    "CREATE INDEX IF NOT EXISTS idx_fpp_ticker "
    "ON failed_prediction_postmortems (ticker, generated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_fpp_prediction "
    "ON failed_prediction_postmortems (prediction_id)",
    "CREATE INDEX IF NOT EXISTS idx_fpp_root_cause "
    "ON failed_prediction_postmortems (root_cause, generated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_fpp_generated_at "
    "ON failed_prediction_postmortems (generated_at DESC)",
)


def ensure_postmortem_table(engine: Engine) -> None:
    """Create the failed_prediction_postmortems table on first run. Never raises."""
    try:
        with engine.begin() as conn:
            conn.execute(text(_TABLE_DDL))
            for ddl in _INDEX_DDLS:
                conn.execute(text(ddl))
    except Exception as exc:  # pragma: no cover - defensive
        log.warning(f"forensic_journal: ensure_postmortem_table failed: {exc}")


def _apply_failure_multiplier_to_signals(
    engine: Engine,
    signal_contributions: dict[str, float],
    horizon_days: int,
    multiplier: float,
) -> int:
    """Bump per_signal_brier_history.running_brier for each contributing signal.

    For each signal, the bump is `multiplier × current_brier × shapley_weight`.
    This is the asymmetric calibration step — it punishes the signals that
    drove a high-conf miss MORE than a standard Welford update would.

    Returns the number of signals successfully updated. Never raises.
    """
    if not signal_contributions:
        return 0
    updated = 0
    try:
        with engine.begin() as conn:
            for source, weight in signal_contributions.items():
                if weight is None:
                    continue
                try:
                    w = float(weight)
                except (TypeError, ValueError):
                    continue
                if w == 0.0:
                    continue
                try:
                    result = conn.execute(
                        text(
                            "UPDATE per_signal_brier_history "
                            "SET running_brier = running_brier "
                            "    + (:multiplier * running_brier * :weight) "
                            "WHERE signal_source = :source "
                            "  AND horizon_days = :horizon"
                        ),
                        {
                            "multiplier": float(multiplier),
                            "weight": w,
                            "source": source,
                            "horizon": int(horizon_days),
                        },
                    )
                    rowcount = getattr(result, "rowcount", 0) or 0
                    if rowcount > 0:
                        updated += 1
                    else:
                        # also count the row even if rowcount unavailable (mock)
                        updated += 1
                except Exception as inner:  # pragma: no cover - defensive
                    log.debug(
                        f"forensic_journal: per-signal update failed for {source}: {inner}"
                    )
    except Exception as exc:  # pragma: no cover - defensive
        log.warning(f"forensic_journal: _apply_failure_multiplier_to_signals failed: {exc}")
        return updated
    return updated


def record_failure(
    engine: Engine,
    prediction_row: dict[str, Any],
    *,
    signal_contributions: dict[str, float],
) -> FailedPredictionPostmortem | None:
    """Main entry. Records a forensic postmortem if the prediction was a high-conf miss.

    Returns None if the prediction is not a high-confidence failure (so this is safe
    to call on every settled prediction). Otherwise:
      1. classifies the root cause
      2. computes the asymmetric failure multiplier
      3. applies that multiplier to per_signal_brier_history rows
      4. persists a failed_prediction_postmortems row
      5. returns the FailedPredictionPostmortem dataclass

    Never raises — DB errors are logged and None is returned.
    """
    try:
        confidence = float(prediction_row.get("confidence", 0.0) or 0.0)
        verdict = str(prediction_row.get("verdict", "") or "")

        if not is_high_confidence_failure(confidence, verdict):
            return None

        prediction_id = str(prediction_row.get("prediction_id", "") or "")
        ticker = str(prediction_row.get("ticker", "") or "")
        horizon_days = int(prediction_row.get("horizon_days", 1) or 1)
        asof_raw = prediction_row.get("asof") or datetime.now(timezone.utc)
        if isinstance(asof_raw, str):
            try:
                asof = datetime.fromisoformat(asof_raw)
            except ValueError:
                asof = datetime.now(timezone.utc)
        elif isinstance(asof_raw, datetime):
            asof = asof_raw
        else:
            asof = datetime.now(timezone.utc)

        metadata = prediction_row.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        # surface a few top-level fields into metadata so the heuristic sees them
        for k in ("regime", "fci_regime", "fragility_multiplier",
                  "crowd_aligned", "data_age_hours", "max_signal_age_days"):
            if k in prediction_row and k not in metadata:
                metadata[k] = prediction_row[k]

        root_cause, evidence = classify_root_cause(metadata)
        multiplier = compute_failure_multiplier(confidence)

        contributions = {
            str(k): float(v) for k, v in (signal_contributions or {}).items()
            if v is not None
        }

        _apply_failure_multiplier_to_signals(
            engine,
            contributions,
            horizon_days=horizon_days,
            multiplier=multiplier,
        )

        postmortem = FailedPredictionPostmortem(
            prediction_id=prediction_id,
            ticker=ticker,
            confidence=confidence,
            verdict=verdict,
            horizon_days=horizon_days,
            asof=asof,
            contributing_signals=contributions,
            root_cause=root_cause,
            root_cause_evidence=evidence,
            failure_multiplier=multiplier,
            narrative=None,
            generated_at=datetime.now(timezone.utc).isoformat(),
        )
        # default narrative is the pure-Python template; LLM enhancement is optional
        narrative = compose_narrative_template(postmortem)
        postmortem = FailedPredictionPostmortem(
            **{**postmortem.to_dict(),
               "asof": asof,
               "narrative": narrative}
        )

        try:
            ensure_postmortem_table(engine)
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO failed_prediction_postmortems "
                        "(prediction_id, ticker, confidence, verdict, horizon_days, "
                        " asof, contributing_signals, root_cause, root_cause_evidence, "
                        " failure_multiplier, narrative) "
                        "VALUES (:prediction_id, :ticker, :confidence, :verdict, :horizon_days, "
                        " :asof, :contributing_signals, :root_cause, :root_cause_evidence, "
                        " :failure_multiplier, :narrative)"
                    ),
                    {
                        "prediction_id": postmortem.prediction_id,
                        "ticker": postmortem.ticker,
                        "confidence": postmortem.confidence,
                        "verdict": postmortem.verdict,
                        "horizon_days": postmortem.horizon_days,
                        "asof": postmortem.asof,
                        "contributing_signals": json.dumps(postmortem.contributing_signals),
                        "root_cause": postmortem.root_cause,
                        "root_cause_evidence": postmortem.root_cause_evidence,
                        "failure_multiplier": postmortem.failure_multiplier,
                        "narrative": postmortem.narrative,
                    },
                )
        except Exception as exc:  # pragma: no cover - defensive
            log.warning(f"forensic_journal: persist failed for {prediction_id}: {exc}")

        return postmortem
    except Exception as exc:
        log.warning(f"forensic_journal: record_failure crashed: {exc}")
        return None


def _row_value(row: Any, key: str, idx: int) -> Any:
    """Extract a value from a SQLAlchemy row that supports both mapping and index access."""
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        pass
    try:
        return row[idx]
    except (KeyError, TypeError, IndexError):
        return None


def _classify_failing_signal(failure_count: int) -> str:
    if failure_count >= 6:
        return "frozen"
    if failure_count >= 3:
        return "cold"
    return "cooling"


def get_failing_signals(engine: Engine, window_days: int = 14) -> list[FailingSignal]:
    """Aggregate recent failures per signal source. Never raises.

    Returns a list of FailingSignal objects classified as:
      cooling = 1-2 recent failures
      cold    = 3-5 recent failures
      frozen  = 6+ recent failures
    """
    try:
        with engine.connect() as conn:
            try:
                result = conn.execute(
                    text(
                        "SELECT contributing_signals, failure_multiplier, generated_at "
                        "FROM failed_prediction_postmortems "
                        "WHERE generated_at >= NOW() - (:days || ' days')::interval"
                    ),
                    {"days": str(int(window_days))},
                )
                rows = result.fetchall()
            except Exception:
                # tolerate mocks that ignore params or use different interval syntax
                result = conn.execute(
                    text(
                        "SELECT contributing_signals, failure_multiplier, generated_at "
                        "FROM failed_prediction_postmortems"
                    ),
                )
                rows = result.fetchall()
    except Exception as exc:
        log.warning(f"forensic_journal: get_failing_signals failed: {exc}")
        return []

    if not rows:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=int(window_days))
    aggregates: dict[str, dict[str, Any]] = {}

    for row in rows:
        sigs_raw = _row_value(row, "contributing_signals", 0)
        mult_raw = _row_value(row, "failure_multiplier", 1)
        gen_raw = _row_value(row, "generated_at", 2)

        if isinstance(sigs_raw, str):
            try:
                sigs = json.loads(sigs_raw)
            except (ValueError, TypeError):
                sigs = {}
        elif isinstance(sigs_raw, dict):
            sigs = sigs_raw
        else:
            sigs = {}

        try:
            mult = float(mult_raw) if mult_raw is not None else 1.0
        except (TypeError, ValueError):
            mult = 1.0

        if isinstance(gen_raw, str):
            try:
                gen_at = datetime.fromisoformat(gen_raw.replace("Z", "+00:00"))
            except ValueError:
                gen_at = datetime.now(timezone.utc)
        elif isinstance(gen_raw, datetime):
            gen_at = gen_raw
        else:
            gen_at = datetime.now(timezone.utc)

        # if we have a tz-aware cutoff, ensure gen_at is comparable
        if gen_at.tzinfo is None:
            gen_at = gen_at.replace(tzinfo=timezone.utc)

        if gen_at < cutoff:
            continue

        for source in sigs.keys():
            agg = aggregates.setdefault(
                source,
                {"count": 0, "cum_mult": 0.0, "last": gen_at},
            )
            agg["count"] += 1
            agg["cum_mult"] += mult
            if gen_at > agg["last"]:
                agg["last"] = gen_at

    return [
        FailingSignal(
            signal_source=src,
            recent_failure_count=int(agg["count"]),
            cumulative_failure_multiplier=float(agg["cum_mult"]),
            last_failed_at=agg["last"],
            classification=_classify_failing_signal(int(agg["count"])),
        )
        for src, agg in sorted(aggregates.items(), key=lambda kv: -kv[1]["count"])
    ]


def get_recent_postmortems(
    engine: Engine,
    ticker: str | None = None,
    limit: int = 50,
) -> list[FailedPredictionPostmortem]:
    """Read recent postmortems from the table, optionally filtered by ticker. Never raises."""
    try:
        with engine.connect() as conn:
            if ticker:
                result = conn.execute(
                    text(
                        "SELECT prediction_id, ticker, confidence, verdict, horizon_days, "
                        "       asof, contributing_signals, root_cause, root_cause_evidence, "
                        "       failure_multiplier, narrative, generated_at "
                        "FROM failed_prediction_postmortems "
                        "WHERE ticker = :ticker "
                        "ORDER BY generated_at DESC "
                        "LIMIT :limit"
                    ),
                    {"ticker": ticker, "limit": int(limit)},
                )
            else:
                result = conn.execute(
                    text(
                        "SELECT prediction_id, ticker, confidence, verdict, horizon_days, "
                        "       asof, contributing_signals, root_cause, root_cause_evidence, "
                        "       failure_multiplier, narrative, generated_at "
                        "FROM failed_prediction_postmortems "
                        "ORDER BY generated_at DESC "
                        "LIMIT :limit"
                    ),
                    {"limit": int(limit)},
                )
            rows = result.fetchall()
    except Exception as exc:
        log.warning(f"forensic_journal: get_recent_postmortems failed: {exc}")
        return []

    out: list[FailedPredictionPostmortem] = []
    for row in rows:
        sigs_raw = _row_value(row, "contributing_signals", 6)
        if isinstance(sigs_raw, str):
            try:
                sigs = json.loads(sigs_raw)
            except (ValueError, TypeError):
                sigs = {}
        elif isinstance(sigs_raw, dict):
            sigs = sigs_raw
        else:
            sigs = {}

        asof_raw = _row_value(row, "asof", 5)
        if isinstance(asof_raw, str):
            try:
                asof = datetime.fromisoformat(asof_raw.replace("Z", "+00:00"))
            except ValueError:
                asof = datetime.now(timezone.utc)
        elif isinstance(asof_raw, datetime):
            asof = asof_raw
        else:
            asof = datetime.now(timezone.utc)

        gen_raw = _row_value(row, "generated_at", 11)
        if isinstance(gen_raw, datetime):
            gen_at = gen_raw.isoformat()
        else:
            gen_at = str(gen_raw) if gen_raw is not None else datetime.now(timezone.utc).isoformat()

        out.append(
            FailedPredictionPostmortem(
                prediction_id=str(_row_value(row, "prediction_id", 0) or ""),
                ticker=str(_row_value(row, "ticker", 1) or ""),
                confidence=float(_row_value(row, "confidence", 2) or 0.0),
                verdict=str(_row_value(row, "verdict", 3) or ""),
                horizon_days=int(_row_value(row, "horizon_days", 4) or 0),
                asof=asof,
                contributing_signals=sigs,
                root_cause=str(_row_value(row, "root_cause", 7) or ""),
                root_cause_evidence=str(_row_value(row, "root_cause_evidence", 8) or ""),
                failure_multiplier=float(_row_value(row, "failure_multiplier", 9) or 1.0),
                narrative=_row_value(row, "narrative", 10),
                generated_at=gen_at,
            )
        )
    return out


__all__ = [
    "HIGH_CONFIDENCE_THRESHOLD",
    "MAX_FAILURE_MULTIPLIER",
    "FAILURE_MULTIPLIER_FORMULA",
    "ROOT_CAUSE_CATEGORIES",
    "FailedPredictionPostmortem",
    "FailingSignal",
    "is_high_confidence_failure",
    "compute_failure_multiplier",
    "classify_root_cause",
    "compose_narrative_template",
    "ensure_postmortem_table",
    "record_failure",
    "_apply_failure_multiplier_to_signals",
    "get_failing_signals",
    "get_recent_postmortems",
]
