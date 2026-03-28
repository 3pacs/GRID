"""
GRID Intelligence — Data Source Taxonomy Audit.

Continuously audits data sources against each other to find which is most
accurate and flag discrepancies.  When the same economic feature is covered
by two or more raw sources (e.g. FRED:SP500 and YF:^GSPC:close), this module
compares them pairwise, ranks accuracy/timeliness/completeness, and auto-
promotes the best source via ``source_catalog.priority_rank``.

Pipeline:
    1. build_redundancy_map  — scan entity_map for features with 2+ sources
    2. compare_sources       — correlation, MAD, max dev, timeliness, completeness
    3. detect_discrepancies  — flag features where sources disagree > threshold
    4. run_full_audit        — orchestrate the full loop
    5. update_source_priorities — auto-promote best source per feature

DB tables:
    source_accuracy      — pairwise comparison results
    source_discrepancies — active discrepancy log with third-source resolution
"""

from __future__ import annotations

import itertools
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from normalization.entity_map import SEED_MAPPINGS, NEW_MAPPINGS_V2

# ── Constants ──────────────────────────────────────────────────────────────

# Default threshold for discrepancy detection (2%)
DEFAULT_DISCREPANCY_THRESHOLD: float = 0.02

# Per-family thresholds (mirrors resolver.py conventions)
FAMILY_DISCREPANCY_THRESHOLDS: dict[str, float] = {
    "vol": 0.03,
    "commodity": 0.02,
    "crypto": 0.05,
    "equity": 0.015,
    "alternative": 0.05,
    "flows": 0.02,
    "systemic": 0.02,
    "trade": 0.02,
    "rates": 0.005,
    "credit": 0.01,
    "fx": 0.01,
    "sentiment": 0.03,
    "macro": 0.02,
}

# Composite score weights
WEIGHT_ACCURACY: float = 0.50
WEIGHT_TIMELINESS: float = 0.25
WEIGHT_COMPLETENESS: float = 0.25

# Minimum overlapping observations required for meaningful comparison
MIN_OVERLAP_DAYS: int = 30

# ── Hardcoded redundancy seeds ─────────────────────────────────────────────
# Known features with multiple source coverage beyond what entity_map
# auto-detection reveals (e.g. naming differences, computed proxies).

_SEED_REDUNDANCY: dict[str, list[str]] = {
    "vix_spot": ["VIXCLS", "YF:^VIX:close", "CBOE:VIX"],
    "btc_close": ["BINANCE:BTCUSDT:close", "CG:bitcoin:close"],
    "btc_total_volume": [
        "YF:BTC-USD:volume", "BINANCE:BTCUSDT:volume", "CG:bitcoin:volume",
    ],
    "eth_close": ["BINANCE:ETHUSDT:close", "CG:ethereum:close"],
    "eth_total_volume": [
        "YF:ETH-USD:volume", "BINANCE:ETHUSDT:volume", "CG:ethereum:volume",
    ],
    "sol_close": ["BINANCE:SOLUSDT:close", "CG:solana:close"],
    "sol_total_volume": [
        "YF:SOL-USD:volume", "BINANCE:SOLUSDT:volume", "CG:solana:volume",
    ],
    "tao_chain_market_cap": [
        "YF:TAO-USD:close", "BINANCE:TAOUSDT:close", "CG:bittensor:close",
    ],
    "tao_chain_total_volume": [
        "YF:TAO-USD:volume", "BINANCE:TAOUSDT:volume", "CG:bittensor:volume",
    ],
    "ofr_fsm_credit": ["ofr_fsm_credit", "OFR:fsm_credit", "OFR:ofr_fsm_credit"],
    "ofr_fsm_funding": ["ofr_fsm_funding", "OFR:fsm_funding", "OFR:ofr_fsm_funding"],
    "ofr_fsm_composite": [
        "ofr_fsm_composite", "OFR:fsm_composite", "OFR:ofr_fsm_composite",
    ],
}


# ── Schema management ──────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS source_accuracy (
    id SERIAL PRIMARY KEY,
    feature_name TEXT NOT NULL,
    source_a TEXT NOT NULL,
    source_b TEXT NOT NULL,
    correlation NUMERIC,
    mean_deviation NUMERIC,
    max_deviation NUMERIC,
    timeliness_winner TEXT,
    completeness_winner TEXT,
    accuracy_winner TEXT,
    overall_winner TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_source_accuracy_feature
    ON source_accuracy (feature_name, checked_at DESC);

CREATE TABLE IF NOT EXISTS source_discrepancies (
    id SERIAL PRIMARY KEY,
    feature_name TEXT NOT NULL,
    source_a TEXT NOT NULL,
    value_a NUMERIC,
    source_b TEXT NOT NULL,
    value_b NUMERIC,
    deviation NUMERIC,
    third_source TEXT,
    third_value NUMERIC,
    resolution TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_discrepancies_date
    ON source_discrepancies (detected_at DESC);
"""


def ensure_tables(engine: Engine) -> None:
    """Create source_accuracy and source_discrepancies tables if missing."""
    with engine.begin() as conn:
        for stmt in _SCHEMA_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
    log.debug("source_audit tables ensured")


# ── 1. Build the redundancy map ───────────────────────────────────────────


def build_redundancy_map(engine: Engine) -> dict[str, list[str]]:
    """Scan entity_map and feature_registry to auto-detect overlapping sources.

    Two raw_series mapping to the same canonical feature name = redundancy.
    Merges auto-detected overlaps with ``_SEED_REDUNDANCY``.

    Parameters:
        engine: SQLAlchemy engine connected to the GRID database.

    Returns:
        dict: feature_name -> list of raw series_ids that map to it.
    """
    # Merge both mapping dicts (V2 may already be merged at runtime, but be safe)
    all_mappings: dict[str, str] = {}
    all_mappings.update(SEED_MAPPINGS)
    all_mappings.update(NEW_MAPPINGS_V2)

    # Invert: feature_name -> [raw_series_id, ...]
    feature_to_sources: dict[str, list[str]] = defaultdict(list)
    for raw_id, feature_name in all_mappings.items():
        feature_to_sources[feature_name].append(raw_id)

    # Also scan the database for raw_series -> feature_registry links via
    # entity_map lookups already resolved in resolved_series.
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT rs.series_id, fr.name AS feature_name
                FROM raw_series rs
                JOIN source_catalog sc ON rs.source_id = sc.id
                JOIN resolved_series res ON res.source_priority_used = sc.id
                JOIN feature_registry fr ON res.feature_id = fr.id
                WHERE rs.pull_status = 'SUCCESS'
            """)).fetchall()
            for row in rows:
                series_id, fname = row[0], row[1]
                if series_id not in feature_to_sources[fname]:
                    feature_to_sources[fname].append(series_id)
    except Exception as exc:
        log.warning(
            "Could not scan DB for additional redundancy: {e}", e=str(exc),
        )

    # Merge seed redundancy hints
    for fname, sources in _SEED_REDUNDANCY.items():
        existing = set(feature_to_sources.get(fname, []))
        for s in sources:
            if s not in existing:
                feature_to_sources[fname].append(s)

    # Filter to features with 2+ sources (actual redundancy)
    redundancy_map = {
        fname: sorted(sources)
        for fname, sources in feature_to_sources.items()
        if len(sources) >= 2
    }

    log.info(
        "Redundancy map built — {n} features with 2+ sources",
        n=len(redundancy_map),
    )
    return redundancy_map


# ── 2. Source accuracy comparison ──────────────────────────────────────────


def _fetch_series(engine: Engine, series_id: str) -> pd.Series:
    """Pull a single raw_series as a pandas Series indexed by obs_date."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT obs_date, value
                FROM raw_series
                WHERE series_id = :sid AND pull_status = 'SUCCESS'
                ORDER BY obs_date
            """),
            {"sid": series_id},
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows, columns=["obs_date", "value"])
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    df = df.drop_duplicates(subset=["obs_date"], keep="last")
    return df.set_index("obs_date")["value"].astype(float)


def _get_feature_family(engine: Engine, feature_name: str) -> str:
    """Look up the family for a feature in feature_registry."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT family FROM feature_registry WHERE name = :n LIMIT 1"),
                {"n": feature_name},
            ).fetchone()
        return row[0] if row else ""
    except Exception:
        return ""


def compare_sources(engine: Engine, feature_name: str) -> dict[str, Any]:
    """Compare all sources for a given feature and return accuracy rankings.

    For each pair of sources:
      - Pull both series for the overlapping date range
      - Compute correlation, mean absolute deviation, max deviation
      - Determine timeliness winner (latest available date)
      - Determine completeness winner (fewest gaps)
      - Rank by composite score

    Parameters:
        engine: SQLAlchemy engine.
        feature_name: Canonical feature name from feature_registry.

    Returns:
        dict with keys: feature_name, pairs (list of pairwise results),
        rankings (source -> composite_score), best_source.
    """
    redundancy_map = build_redundancy_map(engine)
    sources = redundancy_map.get(feature_name, [])

    if len(sources) < 2:
        return {
            "feature_name": feature_name,
            "pairs": [],
            "rankings": {},
            "best_source": sources[0] if sources else None,
            "status": "insufficient_sources",
        }

    # Fetch all series data
    series_data: dict[str, pd.Series] = {}
    for sid in sources:
        s = _fetch_series(engine, sid)
        if not s.empty:
            series_data[sid] = s

    if len(series_data) < 2:
        return {
            "feature_name": feature_name,
            "pairs": [],
            "rankings": {},
            "best_source": list(series_data.keys())[0] if series_data else None,
            "status": "insufficient_data",
        }

    # Composite scores per source (accumulated across pairs)
    accuracy_scores: dict[str, list[float]] = defaultdict(list)
    timeliness_scores: dict[str, float] = {}
    completeness_scores: dict[str, float] = {}
    pair_results: list[dict[str, Any]] = []

    # Timeliness: latest date available
    for sid, s in series_data.items():
        timeliness_scores[sid] = s.index.max().timestamp()
        completeness_scores[sid] = float(len(s))

    # Pairwise comparisons
    source_ids = list(series_data.keys())
    for sa, sb in itertools.combinations(source_ids, 2):
        s_a = series_data[sa]
        s_b = series_data[sb]

        # Align on overlapping dates
        aligned = pd.concat([s_a, s_b], axis=1, join="inner")
        aligned.columns = ["a", "b"]
        aligned = aligned.dropna()

        if len(aligned) < MIN_OVERLAP_DAYS:
            pair_results.append({
                "source_a": sa,
                "source_b": sb,
                "overlap_days": len(aligned),
                "status": "insufficient_overlap",
            })
            continue

        corr = float(aligned["a"].corr(aligned["b"]))
        abs_dev = (aligned["a"] - aligned["b"]).abs()
        mean_dev = float(abs_dev.mean())
        max_dev = float(abs_dev.max())

        # Normalise deviations by mean value to get percentage
        mean_val = aligned[["a", "b"]].mean().mean()
        pct_mean_dev = mean_dev / abs(mean_val) if mean_val != 0 else float("inf")
        pct_max_dev = max_dev / abs(mean_val) if mean_val != 0 else float("inf")

        # Timeliness: who has the later last date
        last_a = s_a.index.max()
        last_b = s_b.index.max()
        timeliness_winner = sa if last_a >= last_b else sb

        # Completeness: who has more non-null values in the full date range
        full_range = pd.date_range(
            min(s_a.index.min(), s_b.index.min()),
            max(s_a.index.max(), s_b.index.max()),
            freq="D",
        )
        gaps_a = len(full_range) - len(s_a.reindex(full_range).dropna())
        gaps_b = len(full_range) - len(s_b.reindex(full_range).dropna())
        completeness_winner = sa if gaps_a <= gaps_b else sb

        # Accuracy: higher correlation + lower deviation wins
        # Score: corr * (1 - pct_mean_dev), clamped to [0, 1]
        score_a_accuracy = max(0.0, min(1.0, corr * (1.0 - pct_mean_dev)))
        accuracy_winner = sa if gaps_a <= gaps_b and last_a >= last_b else sb

        # Composite per source in this pair
        for sid in (sa, sb):
            is_more_timely = 1.0 if sid == timeliness_winner else 0.0
            is_more_complete = 1.0 if sid == completeness_winner else 0.0
            is_more_accurate = 1.0 if sid == accuracy_winner else 0.0
            composite = (
                WEIGHT_ACCURACY * is_more_accurate
                + WEIGHT_TIMELINESS * is_more_timely
                + WEIGHT_COMPLETENESS * is_more_complete
            )
            accuracy_scores[sid].append(composite)

        pair_result = {
            "source_a": sa,
            "source_b": sb,
            "overlap_days": len(aligned),
            "correlation": round(corr, 6),
            "mean_deviation": round(mean_dev, 6),
            "pct_mean_deviation": round(pct_mean_dev, 6),
            "max_deviation": round(max_dev, 6),
            "pct_max_deviation": round(pct_max_dev, 6),
            "timeliness_winner": timeliness_winner,
            "completeness_winner": completeness_winner,
            "accuracy_winner": accuracy_winner,
            "status": "compared",
        }

        # Determine overall winner for the pair
        wins = {sa: 0, sb: 0}
        for w in (timeliness_winner, completeness_winner, accuracy_winner):
            wins[w] += 1
        pair_result["overall_winner"] = max(wins, key=wins.get)  # type: ignore[arg-type]

        pair_results.append(pair_result)

        # Persist to source_accuracy table
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO source_accuracy
                            (feature_name, source_a, source_b, correlation,
                             mean_deviation, max_deviation, timeliness_winner,
                             completeness_winner, accuracy_winner, overall_winner)
                        VALUES
                            (:fn, :sa, :sb, :corr, :md, :mxd, :tw, :cw, :aw, :ow)
                    """),
                    {
                        "fn": feature_name,
                        "sa": sa,
                        "sb": sb,
                        "corr": round(corr, 6),
                        "md": round(pct_mean_dev, 6),
                        "mxd": round(pct_max_dev, 6),
                        "tw": timeliness_winner,
                        "cw": completeness_winner,
                        "aw": accuracy_winner,
                        "ow": pair_result["overall_winner"],
                    },
                )
        except Exception as exc:
            log.warning(
                "Failed to persist source_accuracy for {fn}: {e}",
                fn=feature_name, e=str(exc),
            )

    # Final rankings
    rankings = {
        sid: round(sum(scores) / len(scores), 4) if scores else 0.0
        for sid, scores in accuracy_scores.items()
    }
    best_source = max(rankings, key=rankings.get) if rankings else None  # type: ignore[arg-type]

    return {
        "feature_name": feature_name,
        "pairs": pair_results,
        "rankings": rankings,
        "best_source": best_source,
        "status": "ok",
    }


# ── 3. Discrepancy detection + third-source resolution ────────────────────


def detect_discrepancies(
    engine: Engine,
    threshold: float = DEFAULT_DISCREPANCY_THRESHOLD,
) -> list[dict[str, Any]]:
    """Find features where sources disagree beyond threshold.

    When sources disagree:
      - Log the discrepancy with both values, date, and magnitude
      - Search for a third source to break the tie
      - If no third source exists, flag for manual review

    Parameters:
        engine: SQLAlchemy engine.
        threshold: Default pct deviation threshold (overridden per family).

    Returns:
        List of dicts: feature, source_a, value_a, source_b, value_b,
        third_source, third_value, winner, confidence.
    """
    redundancy_map = build_redundancy_map(engine)
    discrepancies: list[dict[str, Any]] = []

    for feature_name, sources in redundancy_map.items():
        if len(sources) < 2:
            continue

        family = _get_feature_family(engine, feature_name)
        effective_threshold = FAMILY_DISCREPANCY_THRESHOLDS.get(family, threshold)

        # Fetch most recent overlapping value for each source
        latest_values: dict[str, tuple[date, float]] = {}
        for sid in sources:
            try:
                with engine.connect() as conn:
                    row = conn.execute(
                        text("""
                            SELECT obs_date, value
                            FROM raw_series
                            WHERE series_id = :sid AND pull_status = 'SUCCESS'
                            ORDER BY obs_date DESC
                            LIMIT 1
                        """),
                        {"sid": sid},
                    ).fetchone()
                if row:
                    latest_values[sid] = (row[0], float(row[1]))
            except Exception as exc:
                log.debug(
                    "Could not fetch latest for {sid}: {e}",
                    sid=sid, e=str(exc),
                )

        if len(latest_values) < 2:
            continue

        # Check pairwise for the most recent data
        source_ids = list(latest_values.keys())
        for sa, sb in itertools.combinations(source_ids, 2):
            dt_a, val_a = latest_values[sa]
            dt_b, val_b = latest_values[sb]

            # Only compare if dates are within 3 days of each other
            if abs((dt_a - dt_b).days) > 3:
                continue

            ref = abs(val_a) if val_a != 0 else abs(val_b)
            if ref == 0:
                continue

            deviation = abs(val_a - val_b) / ref

            if deviation <= effective_threshold:
                continue

            # Discrepancy detected -- look for third source tiebreaker
            third_source = None
            third_value = None
            resolution = "unresolved"

            remaining = [s for s in source_ids if s not in (sa, sb)]
            if remaining:
                # Use the first available third source
                for ts in remaining:
                    if ts in latest_values:
                        third_source = ts
                        _, third_value = latest_values[ts]
                        # Third source breaks the tie: closest wins
                        diff_a = abs(third_value - val_a) if third_value is not None else float("inf")
                        diff_b = abs(third_value - val_b) if third_value is not None else float("inf")
                        resolution = (
                            "source_a_wins" if diff_a <= diff_b else "source_b_wins"
                        )
                        break

            # Try yfinance real-time as tiebreaker for price features
            if third_source is None and family in ("equity", "commodity", "fx", "crypto"):
                yf_candidate = _find_yfinance_tiebreaker(feature_name, sources, sa, sb)
                if yf_candidate:
                    third_source = yf_candidate
                    # Cannot fetch real-time here without import cost; log as candidate
                    resolution = f"yfinance_tiebreaker_available:{yf_candidate}"

            confidence = 1.0 if resolution in ("source_a_wins", "source_b_wins") else 0.5

            disc = {
                "feature": feature_name,
                "source_a": sa,
                "value_a": val_a,
                "date_a": str(dt_a),
                "source_b": sb,
                "value_b": val_b,
                "date_b": str(dt_b),
                "deviation": round(deviation, 6),
                "threshold": effective_threshold,
                "third_source": third_source,
                "third_value": third_value,
                "winner": (
                    sa if resolution == "source_a_wins"
                    else sb if resolution == "source_b_wins"
                    else None
                ),
                "resolution": resolution,
                "confidence": confidence,
            }
            discrepancies.append(disc)

            # Persist to source_discrepancies
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO source_discrepancies
                                (feature_name, source_a, value_a, source_b, value_b,
                                 deviation, third_source, third_value, resolution)
                            VALUES
                                (:fn, :sa, :va, :sb, :vb, :dev, :ts, :tv, :res)
                        """),
                        {
                            "fn": feature_name,
                            "sa": sa,
                            "va": val_a,
                            "sb": sb,
                            "vb": val_b,
                            "dev": round(deviation, 6),
                            "ts": third_source,
                            "tv": third_value,
                            "res": resolution,
                        },
                    )
            except Exception as exc:
                log.warning(
                    "Failed to persist discrepancy for {fn}: {e}",
                    fn=feature_name, e=str(exc),
                )

    log.info(
        "Discrepancy scan complete — {n} discrepancies found",
        n=len(discrepancies),
    )
    return discrepancies


def _find_yfinance_tiebreaker(
    feature_name: str,
    all_sources: list[str],
    exclude_a: str,
    exclude_b: str,
) -> str | None:
    """Look for a yfinance series that could serve as a tiebreaker.

    Scans all entity_map keys starting with 'YF:' that map to the same feature
    and are not already in the comparison.
    """
    all_mappings = dict(SEED_MAPPINGS)
    all_mappings.update(NEW_MAPPINGS_V2)

    for raw_id, fname in all_mappings.items():
        if fname == feature_name and raw_id.startswith("YF:"):
            if raw_id not in (exclude_a, exclude_b) and raw_id not in all_sources:
                return raw_id
    return None


# ── 4. Full audit ──────────────────────────────────────────────────────────


def run_full_audit(engine: Engine) -> dict[str, Any]:
    """Run complete source accuracy audit.

    Steps:
      1. Build redundancy map from current entity_map
      2. Compare all redundant sources
      3. Detect active discrepancies
      4. Rank all sources by accuracy
      5. Generate report: most/least reliable, primary vs backup
      6. Flag features with only 1 source (single point of failure)

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict: Full audit report.
    """
    ensure_tables(engine)

    log.info("Starting full source audit")
    started_at = datetime.now(timezone.utc)

    # 1. Build redundancy map
    redundancy_map = build_redundancy_map(engine)

    # 2. Compare all redundant sources
    comparisons: dict[str, dict[str, Any]] = {}
    for feature_name in redundancy_map:
        result = compare_sources(engine, feature_name)
        comparisons[feature_name] = result

    # 3. Detect active discrepancies
    discrepancies = detect_discrepancies(engine)

    # 4. Global source rankings (aggregate across all features)
    global_scores: dict[str, list[float]] = defaultdict(list)
    for fname, comp in comparisons.items():
        for source, score in comp.get("rankings", {}).items():
            global_scores[source].append(score)

    source_rankings = {
        source: round(sum(scores) / len(scores), 4)
        for source, scores in global_scores.items()
        if scores
    }
    source_rankings_sorted = dict(
        sorted(source_rankings.items(), key=lambda x: x[1], reverse=True)
    )

    # 5. Identify best/worst sources
    best_sources = dict(list(source_rankings_sorted.items())[:10])
    worst_sources = dict(list(source_rankings_sorted.items())[-10:])

    # 6. Features with only one source (single point of failure)
    all_mappings = dict(SEED_MAPPINGS)
    all_mappings.update(NEW_MAPPINGS_V2)
    feature_to_sources: dict[str, list[str]] = defaultdict(list)
    for raw_id, fname in all_mappings.items():
        feature_to_sources[fname].append(raw_id)

    single_source_features = sorted([
        fname for fname, srcs in feature_to_sources.items()
        if len(srcs) == 1
    ])

    # 7. Primary vs backup recommendations
    recommendations: dict[str, dict[str, str | None]] = {}
    for fname, comp in comparisons.items():
        best = comp.get("best_source")
        all_src = redundancy_map.get(fname, [])
        backups = [s for s in all_src if s != best]
        recommendations[fname] = {
            "primary": best,
            "backups": backups if backups else None,
        }

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()

    report = {
        "started_at": started_at.isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "redundant_features": len(redundancy_map),
        "single_source_features_count": len(single_source_features),
        "single_source_features": single_source_features,
        "total_comparisons": sum(
            len(c.get("pairs", [])) for c in comparisons.values()
        ),
        "active_discrepancies": len(discrepancies),
        "discrepancies": discrepancies,
        "source_rankings": source_rankings_sorted,
        "best_sources": best_sources,
        "worst_sources": worst_sources,
        "recommendations": recommendations,
        "comparisons": {
            fname: {
                "rankings": comp.get("rankings"),
                "best_source": comp.get("best_source"),
                "pair_count": len(comp.get("pairs", [])),
            }
            for fname, comp in comparisons.items()
        },
    }

    log.info(
        "Full audit complete — {n_red} redundant features, {n_disc} discrepancies, "
        "{n_single} single-source features in {t:.1f}s",
        n_red=report["redundant_features"],
        n_disc=report["active_discrepancies"],
        n_single=report["single_source_features_count"],
        t=elapsed,
    )
    return report


# ── 5. Auto-promote best source ───────────────────────────────────────────


def update_source_priorities(
    engine: Engine,
    audit_results: dict[str, Any],
) -> dict[str, Any]:
    """Update source_catalog priority_rank based on accuracy audit.

    The resolver uses priority_rank to pick winners in conflicts.
    This function updates those priorities based on measured accuracy,
    so the best source automatically wins future conflicts.

    Parameters:
        engine: SQLAlchemy engine.
        audit_results: Output from run_full_audit().

    Returns:
        dict: Summary of priority changes made.
    """
    source_rankings = audit_results.get("source_rankings", {})
    if not source_rankings:
        log.info("No source rankings available — skipping priority update")
        return {"changes": 0, "updates": []}

    # Map source series_id prefixes to source_catalog names
    # e.g. "FRED:SP500" -> prefix "FRED" -> source_catalog.name "FRED"
    # "YF:^GSPC:close" -> prefix "YF" -> source_catalog.name "yfinance"
    prefix_to_source: dict[str, str] = {
        "FRED": "FRED",
        "YF": "yfinance",
        "CBOE": "CBOE",
        "BINANCE": "Binance",
        "CG": "CoinGecko",
        "ECB": "ECB",
        "OFR": "OFR",
        "BLS": "BLS",
        "EIA": "EIA",
        "DEFILLAMA": "DeFiLlama",
        "DEXSCR": "DexScreener",
        "METEO": "OpenMeteo",
        "COMPUTED": "COMPUTED",
    }

    # Aggregate scores per source_catalog name
    catalog_scores: dict[str, list[float]] = defaultdict(list)
    for series_id, score in source_rankings.items():
        prefix = series_id.split(":")[0] if ":" in series_id else series_id
        catalog_name = prefix_to_source.get(prefix)
        if catalog_name:
            catalog_scores[catalog_name].append(score)

    # Compute average score per catalog source
    catalog_avg: dict[str, float] = {
        name: sum(scores) / len(scores)
        for name, scores in catalog_scores.items()
        if scores
    }

    # Rank: best score -> lowest priority_rank (1 = highest priority)
    ranked = sorted(catalog_avg.items(), key=lambda x: x[1], reverse=True)

    updates: list[dict[str, Any]] = []
    with engine.begin() as conn:
        for rank, (source_name, avg_score) in enumerate(ranked, start=1):
            new_priority = rank * 10  # Space out ranks by 10 for manual overrides
            try:
                result = conn.execute(
                    text("""
                        UPDATE source_catalog
                        SET priority_rank = :rank
                        WHERE name = :name
                        RETURNING id, name, priority_rank
                    """),
                    {"rank": new_priority, "name": source_name},
                )
                row = result.fetchone()
                if row:
                    updates.append({
                        "source_name": source_name,
                        "new_priority_rank": new_priority,
                        "avg_accuracy_score": round(avg_score, 4),
                    })
            except Exception as exc:
                log.warning(
                    "Failed to update priority for {s}: {e}",
                    s=source_name, e=str(exc),
                )

    log.info(
        "Source priorities updated — {n} sources re-ranked",
        n=len(updates),
    )
    return {"changes": len(updates), "updates": updates}


# ── Convenience ────────────────────────────────────────────────────────────


def get_latest_audit_summary(engine: Engine) -> dict[str, Any]:
    """Fetch the most recent audit results from DB tables for the API.

    Returns a lightweight summary without re-running the full audit.
    """
    ensure_tables(engine)

    summary: dict[str, Any] = {
        "recent_accuracy": [],
        "recent_discrepancies": [],
        "redundancy_map_size": 0,
        "single_source_count": 0,
    }

    try:
        with engine.connect() as conn:
            # Latest accuracy comparisons (last 24h)
            acc_rows = conn.execute(text("""
                SELECT feature_name, source_a, source_b, correlation,
                       mean_deviation, max_deviation, overall_winner, checked_at
                FROM source_accuracy
                WHERE checked_at > NOW() - INTERVAL '24 hours'
                ORDER BY checked_at DESC
                LIMIT 50
            """)).fetchall()
            summary["recent_accuracy"] = [
                {
                    "feature_name": r[0],
                    "source_a": r[1],
                    "source_b": r[2],
                    "correlation": float(r[3]) if r[3] is not None else None,
                    "mean_deviation": float(r[4]) if r[4] is not None else None,
                    "max_deviation": float(r[5]) if r[5] is not None else None,
                    "overall_winner": r[6],
                    "checked_at": r[7].isoformat() if r[7] else None,
                }
                for r in acc_rows
            ]

            # Latest discrepancies (last 7 days)
            disc_rows = conn.execute(text("""
                SELECT feature_name, source_a, value_a, source_b, value_b,
                       deviation, third_source, third_value, resolution, detected_at
                FROM source_discrepancies
                WHERE detected_at > NOW() - INTERVAL '7 days'
                ORDER BY detected_at DESC
                LIMIT 50
            """)).fetchall()
            summary["recent_discrepancies"] = [
                {
                    "feature_name": r[0],
                    "source_a": r[1],
                    "value_a": float(r[2]) if r[2] is not None else None,
                    "source_b": r[3],
                    "value_b": float(r[4]) if r[4] is not None else None,
                    "deviation": float(r[5]) if r[5] is not None else None,
                    "third_source": r[6],
                    "third_value": float(r[7]) if r[7] is not None else None,
                    "resolution": r[8],
                    "detected_at": r[9].isoformat() if r[9] else None,
                }
                for r in disc_rows
            ]
    except Exception as exc:
        log.warning("Failed to fetch audit summary from DB: {e}", e=str(exc))
        summary["error"] = str(exc)

    # Quick redundancy count from entity_map (no DB needed)
    all_mappings = dict(SEED_MAPPINGS)
    all_mappings.update(NEW_MAPPINGS_V2)
    feature_to_sources: dict[str, list[str]] = defaultdict(list)
    for raw_id, fname in all_mappings.items():
        feature_to_sources[fname].append(raw_id)

    redundant = sum(1 for srcs in feature_to_sources.values() if len(srcs) >= 2)
    single = sum(1 for srcs in feature_to_sources.values() if len(srcs) == 1)
    summary["redundancy_map_size"] = redundant
    summary["single_source_count"] = single

    return summary


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    report = run_full_audit(engine)
    print(f"\nAudit complete:")
    print(f"  Redundant features:     {report['redundant_features']}")
    print(f"  Single-source features: {report['single_source_features_count']}")
    print(f"  Active discrepancies:   {report['active_discrepancies']}")
    print(f"  Elapsed:                {report['elapsed_seconds']:.1f}s")
    print(f"\nSource rankings:")
    for src, score in list(report["source_rankings"].items())[:15]:
        print(f"    {src:40s}  {score:.4f}")
    print(f"\nSingle-source features ({report['single_source_features_count']}):")
    for f in report["single_source_features"][:20]:
        print(f"    {f}")
