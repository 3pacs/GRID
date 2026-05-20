"""Bounded source quality ablation for paid-vs-free data decisions.

The report is deliberately conservative: it only compares prediction outcomes
when an oracle_prediction can be attributed back to a source-like signal name.
Operational health is still reported, but never treated as causal model lift.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


DEFAULT_DAYS = 30
DEFAULT_PREDICTION_DAYS = 180
DEFAULT_SOURCE_LIMIT = 250
DEFAULT_PREDICTION_LIMIT = 5000
DEFAULT_MIN_PREDICTION_SAMPLE = 20


@dataclass
class SourceOperationalStats:
    source_id: int
    name: str
    cost_tier: str
    active: bool
    trust_score: Optional[str]
    priority_rank: Optional[int]
    last_pull_at: Optional[datetime]
    latest_success_at: Optional[datetime]
    rows_total: int
    success_rows: int
    failed_rows: int
    partial_rows: int
    last_error: Optional[str] = None
    observation_mode: str = "exact_counts"

    @property
    def failure_rate(self) -> float:
        if self.rows_total <= 0:
            return 0.0
        return float(self.failed_rows) / float(self.rows_total)


@dataclass
class SourcePredictionStats:
    source_name: str
    prediction_count: int = 0
    hits: int = 0
    partials: int = 0
    misses: int = 0
    brier_sum: float = 0.0

    def record(self, verdict: str, confidence: Any) -> None:
        verdict_norm = str(verdict or "").strip().lower()
        if verdict_norm == "hit":
            self.hits += 1
        elif verdict_norm == "partial":
            self.partials += 1
        elif verdict_norm == "miss":
            self.misses += 1
        else:
            return
        self.prediction_count += 1
        self.brier_sum += _brier_score(confidence, _outcome_value(verdict_norm))

    @property
    def adjusted_hits(self) -> float:
        return float(self.hits) + 0.5 * float(self.partials)

    @property
    def hit_rate(self) -> Optional[float]:
        if self.prediction_count <= 0:
            return None
        return self.adjusted_hits / float(self.prediction_count)

    @property
    def brier(self) -> Optional[float]:
        if self.prediction_count <= 0:
            return None
        return self.brier_sum / float(self.prediction_count)


@dataclass
class SourceRedundancyStats:
    source_name: str
    checks: int = 0
    wins: int = 0
    losses: int = 0
    mean_correlation: Optional[float] = None
    mean_deviation: Optional[float] = None


@dataclass
class SourceQualityAssessment:
    source_name: str
    cost_tier: str
    cost_bucket: str
    active: bool
    rows_total: int
    success_rows: int
    failed_rows: int
    failure_rate: float
    prediction_count: int
    hit_rate: Optional[float]
    brier: Optional[float]
    redundancy_checks: int
    redundancy_wins: int
    redundancy_losses: int
    recommendation: str
    evidence_grade: str
    reasons: list[str] = field(default_factory=list)
    free_fallback_candidates: list[dict[str, Any]] = field(default_factory=list)
    last_error: Optional[str] = None
    latest_success_at: Optional[str] = None
    operational_observation: str = "exact_counts"


@dataclass
class SourceQualityReport:
    generated_at: str
    days: int
    prediction_days: int
    min_prediction_sample: int
    sources: list[SourceQualityAssessment]
    paid_vs_free_summary: dict[str, Any]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "days": self.days,
            "prediction_days": self.prediction_days,
            "min_prediction_sample": self.min_prediction_sample,
            "paid_vs_free_summary": dict(self.paid_vs_free_summary),
            "summary": dict(self.summary),
            "sources": [asdict(source) for source in self.sources],
        }


def cost_bucket(cost_tier: str) -> str:
    tier = str(cost_tier or "").strip().upper()
    if tier == "PAID":
        return "paid"
    if tier == "LOW":
        return "low_cost"
    if tier == "FREE":
        return "free"
    return "unknown"


def _outcome_value(verdict: str) -> float:
    verdict_norm = str(verdict or "").strip().lower()
    if verdict_norm == "hit":
        return 1.0
    if verdict_norm == "partial":
        return 0.5
    return 0.0


def _brier_score(confidence: Any, outcome: float) -> float:
    try:
        p = float(confidence)
    except (TypeError, ValueError):
        p = 0.5
    if not math.isfinite(p):
        p = 0.5
    p = max(0.0, min(1.0, p))
    return float((p - float(outcome)) ** 2)


def _normalize_for_match(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _source_aliases(source_name: str) -> set[str]:
    source_norm = _normalize_for_match(source_name)
    aliases = {source_norm} if source_norm else set()
    if source_norm in {"yfinance", "yf"}:
        aliases.add("yf")
    if "tiingo" in source_norm:
        aliases.add("tiingo")
    if "polygon" in source_norm:
        aliases.add("polygon")
    if "twelvedata" in source_norm:
        aliases.add("twelvedata")
    if "quiver" in source_norm:
        aliases.add("quiver")
        aliases.add("quiverquant")
    return {alias for alias in aliases if len(alias) >= 2}


def _harvest_signal_fragments(signals: Any, model_weights: Any) -> list[str]:
    fragments: list[str] = []

    def add(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            fragments.append(value)

    if isinstance(signals, dict):
        for key in ("name", "source", "provider", "series_id"):
            add(signals.get(key))
        items = signals.get("items")
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                for key in ("name", "source", "provider", "series_id"):
                    add(item.get(key))
        contributions = signals.get("signal_contributions")
        if isinstance(contributions, dict):
            fragments.extend(str(key) for key in contributions.keys())
    elif isinstance(signals, list):
        for item in signals:
            if isinstance(item, dict):
                for key in ("name", "source", "provider", "series_id"):
                    add(item.get(key))
            else:
                add(item)
    else:
        add(signals)

    if isinstance(model_weights, dict):
        fragments.extend(str(key) for key in model_weights.keys())
    return fragments


def extract_prediction_source_names(
    signals: Any,
    model_weights: Any,
    catalog_source_names: list[str],
) -> set[str]:
    fragments = _harvest_signal_fragments(signals, model_weights)
    haystack = " ".join(_normalize_for_match(fragment) for fragment in fragments)
    if not haystack:
        return set()

    matches: set[str] = set()
    for source_name in catalog_source_names:
        aliases = _source_aliases(source_name)
        if any(alias and alias in haystack for alias in aliases):
            matches.add(source_name)
    return matches


def aggregate_prediction_metrics(
    prediction_rows: list[dict[str, Any]],
    catalog_source_names: list[str],
) -> dict[str, SourcePredictionStats]:
    metrics: dict[str, SourcePredictionStats] = {}
    for row in prediction_rows:
        matched_sources = extract_prediction_source_names(
            row.get("signals"),
            row.get("model_weights"),
            catalog_source_names,
        )
        for source_name in matched_sources:
            stats = metrics.setdefault(
                source_name,
                SourcePredictionStats(source_name=source_name),
            )
            stats.record(row.get("verdict"), row.get("confidence"))
    return metrics


def _group_prediction_summary(
    source_names: list[str],
    prediction_metrics: dict[str, SourcePredictionStats],
) -> dict[str, Any]:
    count = 0
    adjusted_hits = 0.0
    brier_sum = 0.0
    attributed_sources = 0
    for source_name in source_names:
        stats = prediction_metrics.get(source_name)
        if not stats or stats.prediction_count <= 0:
            continue
        attributed_sources += 1
        count += stats.prediction_count
        adjusted_hits += stats.adjusted_hits
        brier_sum += stats.brier_sum
    return {
        "source_count": len(source_names),
        "attributed_sources": attributed_sources,
        "prediction_count": count,
        "hit_rate": adjusted_hits / count if count else None,
        "brier": brier_sum / count if count else None,
    }


def build_source_quality_report(
    source_stats: list[SourceOperationalStats],
    prediction_metrics: dict[str, SourcePredictionStats],
    redundancy_metrics: dict[str, SourceRedundancyStats],
    fallback_lookup: Callable[[str], list[dict[str, Any]]],
    min_prediction_sample: int = DEFAULT_MIN_PREDICTION_SAMPLE,
    days: int = DEFAULT_DAYS,
    prediction_days: int = DEFAULT_PREDICTION_DAYS,
    total_prediction_rows: int = 0,
    attributed_prediction_rows: Optional[int] = None,
) -> SourceQualityReport:
    paid_names = [source.name for source in source_stats if cost_bucket(source.cost_tier) == "paid"]
    free_names = [
        source.name
        for source in source_stats
        if cost_bucket(source.cost_tier) in {"free", "low_cost"}
    ]
    paid_summary = _group_prediction_summary(paid_names, prediction_metrics)
    free_summary = _group_prediction_summary(free_names, prediction_metrics)
    total_attributed_predictions = (
        int(paid_summary["prediction_count"]) + int(free_summary["prediction_count"])
    )
    if attributed_prediction_rows is None:
        attributed_prediction_rows = total_attributed_predictions
    enough_group_samples = (
        paid_summary["prediction_count"] >= min_prediction_sample
        and free_summary["prediction_count"] >= min_prediction_sample
    )
    lineage_gap = (
        int(total_prediction_rows) >= min_prediction_sample
        and int(attributed_prediction_rows) < min_prediction_sample
    )
    paid_lineage_gap = (
        int(total_prediction_rows) >= min_prediction_sample
        and int(paid_summary["prediction_count"]) < min_prediction_sample
        and bool(paid_names)
    )

    paid_vs_free_summary = {
        "status": (
            "ok"
            if enough_group_samples
            else "paid_source_lineage_gap"
            if paid_lineage_gap
            else "source_lineage_gap"
            if lineage_gap
            else "not_enough_attributed_predictions"
        ),
        "total_prediction_rows": int(total_prediction_rows),
        "attributed_prediction_rows": int(attributed_prediction_rows),
        "paid": paid_summary,
        "free_or_low_cost": free_summary,
    }
    lineage_base_count = max(int(total_prediction_rows), total_attributed_predictions)

    free_hit_rate = free_summary["hit_rate"]
    free_brier = free_summary["brier"]
    assessments: list[SourceQualityAssessment] = []

    for source in source_stats:
        bucket = cost_bucket(source.cost_tier)
        pred = prediction_metrics.get(source.name) or SourcePredictionStats(source_name=source.name)
        redundancy = redundancy_metrics.get(source.name) or SourceRedundancyStats(source_name=source.name)
        reasons: list[str] = []
        evidence_grade = "insufficient"

        if source.rows_total <= 0:
            reasons.append("no_recent_pulls")
        if source.failure_rate >= 0.25:
            reasons.append("high_pull_failure_rate")
            evidence_grade = "operational_only"
        if not source.active:
            reasons.append("source_inactive")
        if pred.prediction_count < min_prediction_sample:
            reasons.append("prediction_sample_below_threshold")
        else:
            evidence_grade = "outcome_attributed"

        recommendation = "watch"
        if pred.prediction_count >= min_prediction_sample:
            hit_rate = pred.hit_rate or 0.0
            brier = pred.brier
            free_peer_better = (
                bucket == "paid"
                and enough_group_samples
                and free_hit_rate is not None
                and hit_rate + 0.10 < float(free_hit_rate)
            )
            if (
                free_peer_better
                or (
                    bucket == "paid"
                    and enough_group_samples
                    and free_brier is not None
                    and brier is not None
                    and brier > float(free_brier) + 0.03
                )
            ):
                recommendation = "replace_candidate"
                reasons.append("free_peer_outperformed")
            elif hit_rate >= 0.52 and source.failure_rate < 0.10:
                recommendation = "keep"
            elif hit_rate < 0.45:
                recommendation = "replace_candidate" if bucket == "paid" else "watch"
                reasons.append("weak_attributed_hit_rate")
        elif (
            bucket == "paid"
            and pred.prediction_count == 0
            and lineage_base_count >= min_prediction_sample
        ):
            recommendation = "instrument_or_disable"
            reasons.append("paid_source_not_attributed_to_predictions")
        elif bucket == "paid" and source.failure_rate >= 0.50 and source.success_rows == 0:
            recommendation = "disable_candidate"
            reasons.append("paid_source_failing_without_successes")
        elif bucket == "paid" and source.failure_rate >= 0.25:
            recommendation = "replace_candidate"
            reasons.append("paid_operational_drag")
        elif pred.prediction_count < min_prediction_sample:
            recommendation = "insufficient_evidence"

        fallback_candidates: list[dict[str, Any]] = []
        if bucket == "paid" and recommendation in {
            "replace_candidate",
            "disable_candidate",
            "instrument_or_disable",
            "watch",
        }:
            fallback_candidates = fallback_lookup(source.name)

        assessments.append(
            SourceQualityAssessment(
                source_name=source.name,
                cost_tier=str(source.cost_tier or ""),
                cost_bucket=bucket,
                active=bool(source.active),
                rows_total=int(source.rows_total),
                success_rows=int(source.success_rows),
                failed_rows=int(source.failed_rows),
                failure_rate=round(source.failure_rate, 4),
                prediction_count=int(pred.prediction_count),
                hit_rate=round(pred.hit_rate, 4) if pred.hit_rate is not None else None,
                brier=round(pred.brier, 4) if pred.brier is not None else None,
                redundancy_checks=int(redundancy.checks),
                redundancy_wins=int(redundancy.wins),
                redundancy_losses=int(redundancy.losses),
                recommendation=recommendation,
                evidence_grade=evidence_grade,
                reasons=sorted(set(reasons)),
                free_fallback_candidates=fallback_candidates,
                last_error=source.last_error,
                latest_success_at=(
                    source.latest_success_at.isoformat()
                    if source.latest_success_at is not None
                    else None
                ),
                operational_observation=source.observation_mode,
            )
        )

    priority = {
        "disable_candidate": 0,
        "replace_candidate": 1,
        "instrument_or_disable": 2,
        "watch": 3,
        "insufficient_evidence": 4,
        "keep": 5,
    }
    assessments.sort(
        key=lambda row: (
            priority.get(row.recommendation, 9),
            0 if row.cost_bucket == "paid" else 1,
            -row.failure_rate,
            row.source_name.lower(),
        )
    )

    summary = {
        "source_count": len(assessments),
        "paid_sources": len(paid_names),
        "free_or_low_cost_sources": len(free_names),
        "replace_candidates": sum(1 for row in assessments if row.recommendation == "replace_candidate"),
        "disable_candidates": sum(1 for row in assessments if row.recommendation == "disable_candidate"),
        "instrument_or_disable": sum(1 for row in assessments if row.recommendation == "instrument_or_disable"),
        "insufficient_evidence": sum(1 for row in assessments if row.recommendation == "insufficient_evidence"),
        "attributed_prediction_sources": sum(1 for row in assessments if row.prediction_count > 0),
    }

    return SourceQualityReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        days=days,
        prediction_days=prediction_days,
        min_prediction_sample=min_prediction_sample,
        sources=assessments,
        paid_vs_free_summary=paid_vs_free_summary,
        summary=summary,
    )


def load_source_operational_stats(
    engine: Engine,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_SOURCE_LIMIT,
) -> list[SourceOperationalStats]:
    query = text(
        """
        WITH selected AS (
            SELECT
                id,
                name,
                cost_tier,
                active,
                trust_score,
                priority_rank,
                last_pull_at
            FROM source_catalog
            ORDER BY
                CASE WHEN cost_tier = 'PAID' THEN 0 ELSE 1 END,
                active DESC,
                priority_rank ASC NULLS LAST,
                name ASC
            LIMIT :limit
        ),
        failed_counts AS (
            SELECT source_id, COUNT(*) AS failed_rows
            FROM raw_series
            WHERE pull_status = 'FAILED'
              AND pull_timestamp >= NOW() - (:days || ' days')::interval
            GROUP BY source_id
        ),
        partial_counts AS (
            SELECT source_id, COUNT(*) AS partial_rows
            FROM raw_series
            WHERE pull_status = 'PARTIAL'
              AND pull_timestamp >= NOW() - (:days || ' days')::interval
            GROUP BY source_id
        )
        SELECT
            selected.id,
            selected.name,
            selected.cost_tier,
            selected.active,
            selected.trust_score,
            selected.priority_rank,
            selected.last_pull_at,
            latest_success.latest_success_at,
            (
                COALESCE(failed_counts.failed_rows, 0)
                + COALESCE(partial_counts.partial_rows, 0)
                + CASE
                    WHEN latest_success.latest_success_at >= NOW() - (:days || ' days')::interval
                    THEN 1
                    ELSE 0
                  END
            ) AS rows_total,
            CASE
                WHEN latest_success.latest_success_at >= NOW() - (:days || ' days')::interval
                THEN 1
                ELSE 0
            END AS success_rows,
            COALESCE(failed_counts.failed_rows, 0) AS failed_rows,
            COALESCE(partial_counts.partial_rows, 0) AS partial_rows,
            latest_failed.last_error
        FROM selected
        LEFT JOIN failed_counts ON failed_counts.source_id = selected.id
        LEFT JOIN partial_counts ON partial_counts.source_id = selected.id
        LEFT JOIN LATERAL (
            SELECT rs.pull_timestamp AS latest_success_at
            FROM raw_series rs
            WHERE rs.source_id = selected.id
              AND rs.pull_status = 'SUCCESS'
            ORDER BY rs.pull_timestamp DESC
            LIMIT 1
        ) latest_success ON TRUE
        LEFT JOIN LATERAL (
            SELECT rs.raw_payload::text AS last_error
            FROM raw_series rs
            WHERE rs.source_id = selected.id
              AND rs.pull_status = 'FAILED'
              AND rs.pull_timestamp >= NOW() - (:days || ' days')::interval
            ORDER BY rs.pull_timestamp DESC
            LIMIT 1
        ) latest_failed ON TRUE
        ORDER BY
            CASE WHEN selected.cost_tier = 'PAID' THEN 0 ELSE 1 END,
            COALESCE(failed_counts.failed_rows, 0) DESC,
            selected.priority_rank ASC NULLS LAST,
            selected.name ASC
        """
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SET LOCAL statement_timeout = '15s'"))
            rows = conn.execute(query, {"days": int(days), "limit": int(limit)}).mappings().all()
    except Exception as exc:
        log.warning(
            "source_quality_ablation: operational source stats timed out or failed, "
            "falling back to catalog-only stats: {e}",
            e=str(exc),
        )
        rows = _load_catalog_only_source_stats(engine, limit=limit)

    stats: list[SourceOperationalStats] = []
    for row in rows:
        stats.append(
            SourceOperationalStats(
                source_id=int(row["id"]),
                name=str(row["name"]),
                cost_tier=str(row["cost_tier"] or ""),
                active=bool(row["active"]),
                trust_score=row.get("trust_score"),
                priority_rank=row.get("priority_rank"),
                last_pull_at=row.get("last_pull_at"),
                latest_success_at=row.get("latest_success_at"),
                rows_total=int(row["rows_total"] or 0),
                success_rows=int(row["success_rows"] or 0),
                failed_rows=int(row["failed_rows"] or 0),
                partial_rows=int(row["partial_rows"] or 0),
                last_error=_truncate(row.get("last_error"), 500),
                observation_mode=str(
                    row.get("observation_mode") or "failure_counts_plus_latest_success"
                ),
            )
        )
    return stats


def _load_catalog_only_source_stats(engine: Engine, limit: int) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT
            id,
            name,
            cost_tier,
            active,
            trust_score,
            priority_rank,
            last_pull_at,
            NULL::timestamptz AS latest_success_at,
            0 AS rows_total,
            0 AS success_rows,
            0 AS failed_rows,
            0 AS partial_rows,
            NULL::text AS last_error,
            'catalog_only' AS observation_mode
        FROM source_catalog
        ORDER BY
            CASE WHEN cost_tier = 'PAID' THEN 0 ELSE 1 END,
            active DESC,
            priority_rank ASC NULLS LAST,
            name ASC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        return list(conn.execute(query, {"limit": int(limit)}).mappings().all())


def load_prediction_metrics(
    engine: Engine,
    catalog_source_names: list[str],
    days: int = DEFAULT_PREDICTION_DAYS,
    limit: int = DEFAULT_PREDICTION_LIMIT,
) -> dict[str, SourcePredictionStats]:
    prediction_rows = load_prediction_rows(engine, days=days, limit=limit)
    return aggregate_prediction_metrics(prediction_rows, catalog_source_names)


def load_prediction_rows(
    engine: Engine,
    days: int = DEFAULT_PREDICTION_DAYS,
    limit: int = DEFAULT_PREDICTION_LIMIT,
) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT id, verdict, confidence, signals, model_weights
        FROM oracle_predictions
        WHERE verdict IN ('hit', 'miss', 'partial')
          AND created_at >= NOW() - (:days || ' days')::interval
          AND COALESCE(dedup_keep, TRUE) = TRUE
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"days": int(days), "limit": int(limit)}).mappings().all()
    return [
        {
            "id": row.get("id"),
            "verdict": row.get("verdict"),
            "confidence": row.get("confidence"),
            "signals": row.get("signals"),
            "model_weights": row.get("model_weights"),
        }
        for row in rows
    ]


def count_attributed_prediction_rows(
    prediction_rows: list[dict[str, Any]],
    catalog_source_names: list[str],
) -> int:
    count = 0
    for row in prediction_rows:
        if extract_prediction_source_names(
            row.get("signals"),
            row.get("model_weights"),
            catalog_source_names,
        ):
            count += 1
    return count


def load_redundancy_metrics(
    engine: Engine,
    catalog_source_names: list[str],
    days: int = DEFAULT_DAYS,
    limit: int = 5000,
) -> dict[str, SourceRedundancyStats]:
    query = text(
        """
        SELECT source_a, source_b, correlation, mean_deviation, overall_winner
        FROM source_accuracy
        WHERE checked_at >= NOW() - (:days || ' days')::interval
        ORDER BY checked_at DESC
        LIMIT :limit
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"days": int(days), "limit": int(limit)}).mappings().all()
    except Exception as exc:
        log.warning("source_quality_ablation: redundancy metric load failed: {e}", e=str(exc))
        return {}

    by_source: dict[str, dict[str, Any]] = {}
    for source_name in catalog_source_names:
        by_source[source_name] = {
            "checks": 0,
            "wins": 0,
            "losses": 0,
            "correlations": [],
            "deviations": [],
        }

    for row in rows:
        row_text = f"{row.get('source_a') or ''} {row.get('source_b') or ''}"
        winner = str(row.get("overall_winner") or "")
        for source_name in catalog_source_names:
            if not _text_matches_source(row_text, source_name):
                continue
            bucket = by_source[source_name]
            bucket["checks"] += 1
            if _text_matches_source(winner, source_name):
                bucket["wins"] += 1
            elif winner:
                bucket["losses"] += 1
            corr = _safe_float(row.get("correlation"))
            if corr is not None:
                bucket["correlations"].append(corr)
            dev = _safe_float(row.get("mean_deviation"))
            if dev is not None:
                bucket["deviations"].append(dev)

    metrics: dict[str, SourceRedundancyStats] = {}
    for source_name, bucket in by_source.items():
        if bucket["checks"] <= 0:
            continue
        correlations = bucket["correlations"]
        deviations = bucket["deviations"]
        metrics[source_name] = SourceRedundancyStats(
            source_name=source_name,
            checks=int(bucket["checks"]),
            wins=int(bucket["wins"]),
            losses=int(bucket["losses"]),
            mean_correlation=sum(correlations) / len(correlations) if correlations else None,
            mean_deviation=sum(deviations) / len(deviations) if deviations else None,
        )
    return metrics


def run_source_quality_ablation(
    engine: Engine,
    days: int = DEFAULT_DAYS,
    prediction_days: int = DEFAULT_PREDICTION_DAYS,
    source_limit: int = DEFAULT_SOURCE_LIMIT,
    prediction_limit: int = DEFAULT_PREDICTION_LIMIT,
    min_prediction_sample: int = DEFAULT_MIN_PREDICTION_SAMPLE,
    output_dir: Path = Path("outputs/source_quality"),
    write: bool = True,
) -> dict[str, Any]:
    source_stats = load_source_operational_stats(engine, days=days, limit=source_limit)
    catalog_names = [source.name for source in source_stats]
    prediction_rows = load_prediction_rows(engine, days=prediction_days, limit=prediction_limit)
    prediction_metrics = aggregate_prediction_metrics(prediction_rows, catalog_names)
    attributed_prediction_rows = count_attributed_prediction_rows(prediction_rows, catalog_names)
    redundancy_metrics = load_redundancy_metrics(engine, catalog_names, days=days)
    report = build_source_quality_report(
        source_stats,
        prediction_metrics=prediction_metrics,
        redundancy_metrics=redundancy_metrics,
        fallback_lookup=_default_fallback_lookup,
        min_prediction_sample=min_prediction_sample,
        days=days,
        prediction_days=prediction_days,
        total_prediction_rows=len(prediction_rows),
        attributed_prediction_rows=attributed_prediction_rows,
    )

    paths: dict[str, str] = {}
    if write:
        paths = write_source_quality_report(report, output_dir)

    return {
        "status": "ok",
        "summary": report.summary,
        "paid_vs_free_summary": report.paid_vs_free_summary,
        "top_recommendations": [asdict(row) for row in report.sources[:10]],
        "json_path": paths.get("json"),
        "markdown_path": paths.get("markdown"),
    }


def write_source_quality_report(
    report: SourceQualityReport,
    output_dir: Path,
) -> dict[str, str]:
    try:
        from outputs.path_utils import ensure_output_dir

        outdir = ensure_output_dir(output_dir)
    except Exception:
        outdir = output_dir
        outdir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = outdir / f"source_quality_ablation_{stamp}.json"
    markdown_path = outdir / f"source_quality_ablation_{stamp}.md"
    latest_json = outdir / "source_quality_ablation_latest.json"
    latest_markdown = outdir / "source_quality_ablation_latest.md"

    payload = json.dumps(report.to_dict(), indent=2, default=_json_default)
    markdown = markdown_report(report)
    json_path.write_text(payload + "\n", encoding="utf-8")
    markdown_path.write_text(markdown, encoding="utf-8")
    latest_json.write_text(payload + "\n", encoding="utf-8")
    latest_markdown.write_text(markdown, encoding="utf-8")
    return {
        "json": str(json_path),
        "markdown": str(markdown_path),
        "latest_json": str(latest_json),
        "latest_markdown": str(latest_markdown),
    }


def markdown_report(report: SourceQualityReport) -> str:
    lines = [
        "# GRID Source Quality Ablation",
        "",
        f"- Generated: {report.generated_at}",
        f"- Pull window: {report.days} days",
        f"- Prediction attribution window: {report.prediction_days} days",
        f"- Minimum attributed prediction sample: {report.min_prediction_sample}",
        f"- Paid-vs-free status: {report.paid_vs_free_summary.get('status')}",
        "",
        "This is conservative: outcome lift is counted only when prediction signals name a catalog source.",
        (
            "Operational failures are listed separately and do not prove causal alpha decay. "
            "`failure_counts_plus_latest_success` means failures/partials are exact, while "
            "success is only a latest-success marker so giant feeds do not require counting "
            "hundreds of millions of successful rows."
        ),
        "",
        "## Summary",
    ]
    for key, value in sorted(report.summary.items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Paid vs Free"])
    lines.append(f"- Paid: {report.paid_vs_free_summary.get('paid')}")
    lines.append(f"- Free or low-cost: {report.paid_vs_free_summary.get('free_or_low_cost')}")
    lines.extend(["", "## Top Source Actions"])
    for row in report.sources[:25]:
        hit = "n/a" if row.hit_rate is None else f"{row.hit_rate:.3f}"
        brier = "n/a" if row.brier is None else f"{row.brier:.3f}"
        failure_label = (
            "failure_pressure"
            if row.operational_observation == "failure_counts_plus_latest_success"
            else "fail_rate"
        )
        lines.append(
            "- "
            f"{row.source_name} ({row.cost_tier}) -> {row.recommendation}; "
            f"evidence={row.evidence_grade}; observed_status={row.success_rows}/{row.rows_total}; "
            f"{failure_label}={row.failure_rate:.1%}; predictions={row.prediction_count}; "
            f"hit_rate={hit}; brier={brier}; reasons={', '.join(row.reasons) or 'none'}"
        )
    return "\n".join(lines) + "\n"


def _default_fallback_lookup(source_name: str) -> list[dict[str, Any]]:
    try:
        from scripts.hermes_fixers import _recommend_free_data_fallbacks

        return _recommend_free_data_fallbacks(source_name)
    except Exception:
        return []


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _truncate(value: Any, max_chars: int) -> Optional[str]:
    if value is None:
        return None
    text_value = scrub_secret_text(str(value).strip())
    if len(text_value) <= max_chars:
        return text_value
    return text_value[: max_chars - 3] + "..."


def scrub_secret_text(value: str) -> str:
    """Redact common token-bearing query params before writing reports."""
    text_value = str(value or "")
    patterns = (
        r"(?i)(api[_-]?key=)[^&\s\"']+",
        r"(?i)(token=)[^&\s\"']+",
        r"(?i)(access[_-]?token=)[^&\s\"']+",
        r"(?i)(secret=)[^&\s\"']+",
        r"(?i)(password=)[^&\s\"']+",
    )
    for pattern in patterns:
        text_value = re.sub(pattern, r"\1[REDACTED]", text_value)
    return text_value


def _safe_float(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _text_matches_source(text_value: str, source_name: str) -> bool:
    text_norm = _normalize_for_match(text_value)
    return any(alias in text_norm for alias in _source_aliases(source_name))
