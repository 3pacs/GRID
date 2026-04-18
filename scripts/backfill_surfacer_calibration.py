#!/usr/bin/env python3
"""Materialize granular Surfacer calibration for high-volume tickers.

The Surfacer front page should not depend on vague aggregate history when
settled oracle rows are available. This script builds three compact tables:

* ``surfacer_top_volume_universe`` — top tickers by current dollar volume.
* ``surfacer_ticker_calibration`` — ticker/direction/horizon/regime/model hit-rate.
* ``surfacer_signal_calibration`` — same, split by contributing signal source.
* ``surfacer_options_coverage`` — latest options expectation coverage.

Usage:
    python3 scripts/backfill_surfacer_calibration.py --limit 1000
    python3 scripts/backfill_surfacer_calibration.py --limit 1000 --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import get_engine


UNIVERSE_CSV = ROOT / "data" / "tiingo_universe_tiered.csv"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill granular Surfacer calibration tables.")
    parser.add_argument("--limit", type=int, default=1000, help="Top-volume ticker count. Use 0 for the whole CSV.")
    parser.add_argument("--min-dollar-volume", type=float, default=0.0, help="Drop tickers below this dollar volume.")
    parser.add_argument("--dry-run", action="store_true", help="Read universe and report counts without writes.")
    parser.add_argument(
        "--queue-requirements",
        type=int,
        default=1000,
        help="Queue this many highest-priority unresolved requirements into llm_task_backlog. Use 0 to skip.",
    )
    return parser.parse_args(argv)


def _load_top_volume(limit: int, min_dollar_volume: float) -> list[dict[str, Any]]:
    if not UNIVERSE_CSV.exists():
        raise FileNotFoundError(f"missing universe CSV: {UNIVERSE_CSV}")
    rows: list[dict[str, Any]] = []
    with UNIVERSE_CSV.open(newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ticker = str(row.get("ticker") or "").strip().upper()
            if not ticker or "." in ticker or "/" in ticker:
                continue
            try:
                dollar_volume = float(row.get("dollar_volume") or 0)
                volume = float(row.get("volume") or 0)
                last_price = float(row.get("last_price") or 0)
            except ValueError:
                continue
            if dollar_volume < min_dollar_volume:
                continue
            rows.append({
                "ticker": ticker,
                "volume_rank": 0,
                "last_price": last_price,
                "volume": volume,
                "dollar_volume": dollar_volume,
                "tier": row.get("tier") or "",
                "source_timestamp": row.get("timestamp") or None,
            })
    rows.sort(key=lambda item: item["dollar_volume"], reverse=True)
    top = rows if limit <= 0 else rows[:limit]
    for idx, row in enumerate(top, start=1):
        row["volume_rank"] = idx
    return top


def _ensure_tables(conn: Any) -> None:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS surfacer_top_volume_universe (
            ticker TEXT PRIMARY KEY,
            volume_rank INTEGER NOT NULL,
            last_price DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            dollar_volume DOUBLE PRECISION,
            tier TEXT,
            source_timestamp TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS surfacer_ticker_calibration (
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            regime TEXT NOT NULL,
            model_name TEXT NOT NULL,
            prediction_type TEXT NOT NULL,
            samples INTEGER NOT NULL,
            hits INTEGER NOT NULL,
            partials INTEGER NOT NULL,
            misses INTEGER NOT NULL,
            hit_rate DOUBLE PRECISION,
            avg_pnl_pct DOUBLE PRECISION,
            avg_confidence DOUBLE PRECISION,
            avg_expected_move_pct DOUBLE PRECISION,
            avg_actual_move_pct DOUBLE PRECISION,
            brier DOUBLE PRECISION,
            ece DOUBLE PRECISION,
            first_seen TIMESTAMPTZ,
            last_seen TIMESTAMPTZ,
            last_scored_at TIMESTAMPTZ,
            volume_rank INTEGER,
            dollar_volume DOUBLE PRECISION,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (ticker, direction, horizon_days, regime, model_name, prediction_type)
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS surfacer_signal_calibration (
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            regime TEXT NOT NULL,
            signal_source TEXT NOT NULL,
            model_name TEXT NOT NULL,
            samples INTEGER NOT NULL,
            hits INTEGER NOT NULL,
            partials INTEGER NOT NULL,
            misses INTEGER NOT NULL,
            hit_rate DOUBLE PRECISION,
            avg_contribution_weight DOUBLE PRECISION,
            avg_confidence DOUBLE PRECISION,
            brier DOUBLE PRECISION,
            ece DOUBLE PRECISION,
            first_seen TIMESTAMPTZ,
            last_seen TIMESTAMPTZ,
            last_scored_at TIMESTAMPTZ,
            volume_rank INTEGER,
            dollar_volume DOUBLE PRECISION,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (ticker, direction, horizon_days, regime, signal_source, model_name)
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS surfacer_options_coverage (
            ticker TEXT PRIMARY KEY,
            latest_signal_date DATE,
            rows_available INTEGER NOT NULL,
            avg_total_volume DOUBLE PRECISION,
            avg_total_oi DOUBLE PRECISION,
            latest_total_volume DOUBLE PRECISION,
            latest_total_oi DOUBLE PRECISION,
            latest_iv_atm DOUBLE PRECISION,
            latest_spot_price DOUBLE PRECISION,
            latest_near_expiry DATE,
            volume_rank INTEGER,
            dollar_volume DOUBLE PRECISION,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS surfacer_data_requirements (
            id BIGSERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            requirement_type TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 3,
            status TEXT NOT NULL DEFAULT 'pending',
            reason TEXT NOT NULL,
            payload JSONB DEFAULT '{}',
            volume_rank INTEGER,
            dollar_volume DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (ticker, requirement_type)
        )
    """))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stc_ticker_dir_horizon ON surfacer_ticker_calibration (ticker, direction, horizon_days)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ssc_ticker_signal_horizon ON surfacer_signal_calibration (ticker, signal_source, horizon_days)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stvu_rank ON surfacer_top_volume_universe (volume_rank)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sdr_status_priority ON surfacer_data_requirements (status, priority, volume_rank)"))


def _replace_universe(conn: Any, universe: list[dict[str, Any]]) -> None:
    conn.execute(text("TRUNCATE TABLE surfacer_top_volume_universe"))
    for row in universe:
        conn.execute(
            text("""
                INSERT INTO surfacer_top_volume_universe
                    (ticker, volume_rank, last_price, volume, dollar_volume, tier, source_timestamp, updated_at)
                VALUES
                    (:ticker, :volume_rank, :last_price, :volume, :dollar_volume, :tier, :source_timestamp, NOW())
            """),
            row,
        )


def _materialize_ticker_calibration(conn: Any) -> int:
    conn.execute(text("TRUNCATE TABLE surfacer_ticker_calibration"))
    result = conn.execute(text("""
        INSERT INTO surfacer_ticker_calibration (
            ticker, direction, horizon_days, regime, model_name, prediction_type,
            samples, hits, partials, misses, hit_rate, avg_pnl_pct,
            avg_confidence, avg_expected_move_pct, avg_actual_move_pct,
            brier, ece, first_seen, last_seen, last_scored_at,
            volume_rank, dollar_volume, updated_at
        )
        WITH scored AS (
            SELECT
                UPPER(op.ticker) AS ticker,
                CASE
                    WHEN LOWER(COALESCE(op.direction, op.prediction_type, '')) ~ '(put|short|bear|sell|down)' THEN 'bearish'
                    WHEN LOWER(COALESCE(op.direction, op.prediction_type, '')) ~ '(call|long|bull|buy|up)' THEN 'bullish'
                    ELSE 'watch'
                END AS direction,
                CASE
                    WHEN op.expiry IS NULL OR op.created_at IS NULL THEN 7
                    WHEN (op.expiry::date - op.created_at::date) <= 3 THEN 1
                    WHEN (op.expiry::date - op.created_at::date) <= 14 THEN 7
                    WHEN (op.expiry::date - op.created_at::date) <= 60 THEN 30
                    ELSE 90
                END AS horizon_days,
                COALESCE(op.signals->>'regime', op.signals->>'fci_regime', 'NEUTRAL') AS regime,
                COALESCE(NULLIF(op.model_name, ''), 'unknown') AS model_name,
                COALESCE(NULLIF(op.prediction_type, ''), 'unknown') AS prediction_type,
                op.confidence,
                op.expected_move_pct,
                op.actual_move_pct,
                op.pnl_pct,
                op.created_at,
                op.scored_at,
                op.verdict,
                CASE op.verdict WHEN 'hit' THEN 1.0 WHEN 'partial' THEN 0.5 ELSE 0.0 END AS outcome,
                tv.volume_rank,
                tv.dollar_volume
            FROM oracle_predictions op
            JOIN surfacer_top_volume_universe tv ON tv.ticker = UPPER(op.ticker)
            WHERE op.verdict IN ('hit', 'miss', 'partial')
        )
        SELECT
            ticker, direction, horizon_days, regime, model_name, prediction_type,
            COUNT(*)::int AS samples,
            COUNT(*) FILTER (WHERE verdict = 'hit')::int AS hits,
            COUNT(*) FILTER (WHERE verdict = 'partial')::int AS partials,
            COUNT(*) FILTER (WHERE verdict = 'miss')::int AS misses,
            AVG(outcome) AS hit_rate,
            AVG(pnl_pct) AS avg_pnl_pct,
            AVG(confidence) AS avg_confidence,
            AVG(expected_move_pct) AS avg_expected_move_pct,
            AVG(actual_move_pct) AS avg_actual_move_pct,
            AVG(POWER(COALESCE(confidence, 0.5) - outcome, 2)) AS brier,
            AVG(ABS(COALESCE(confidence, 0.5) - outcome)) AS ece,
            MIN(created_at) AS first_seen,
            MAX(created_at) AS last_seen,
            MAX(scored_at) AS last_scored_at,
            MIN(volume_rank) AS volume_rank,
            MAX(dollar_volume) AS dollar_volume,
            NOW() AS updated_at
        FROM scored
        GROUP BY ticker, direction, horizon_days, regime, model_name, prediction_type
    """))
    return int(result.rowcount or 0)


def _materialize_signal_calibration(conn: Any) -> int:
    conn.execute(text("TRUNCATE TABLE surfacer_signal_calibration"))
    result = conn.execute(text("""
        INSERT INTO surfacer_signal_calibration (
            ticker, direction, horizon_days, regime, signal_source, model_name,
            samples, hits, partials, misses, hit_rate, avg_contribution_weight,
            avg_confidence, brier, ece, first_seen, last_seen, last_scored_at,
            volume_rank, dollar_volume, updated_at
        )
        WITH scored AS (
            SELECT
                UPPER(op.ticker) AS ticker,
                CASE
                    WHEN LOWER(COALESCE(op.direction, op.prediction_type, '')) ~ '(put|short|bear|sell|down)' THEN 'bearish'
                    WHEN LOWER(COALESCE(op.direction, op.prediction_type, '')) ~ '(call|long|bull|buy|up)' THEN 'bullish'
                    ELSE 'watch'
                END AS direction,
                CASE
                    WHEN op.expiry IS NULL OR op.created_at IS NULL THEN 7
                    WHEN (op.expiry::date - op.created_at::date) <= 3 THEN 1
                    WHEN (op.expiry::date - op.created_at::date) <= 14 THEN 7
                    WHEN (op.expiry::date - op.created_at::date) <= 60 THEN 30
                    ELSE 90
                END AS horizon_days,
                COALESCE(op.signals->>'regime', op.signals->>'fci_regime', 'NEUTRAL') AS regime,
                COALESCE(NULLIF(op.model_name, ''), 'unknown') AS model_name,
                sig.key AS signal_source,
                NULLIF(sig.value, '')::double precision AS contribution_weight,
                op.confidence,
                op.created_at,
                op.scored_at,
                op.verdict,
                CASE op.verdict WHEN 'hit' THEN 1.0 WHEN 'partial' THEN 0.5 ELSE 0.0 END AS outcome,
                tv.volume_rank,
                tv.dollar_volume
            FROM oracle_predictions op
            JOIN surfacer_top_volume_universe tv ON tv.ticker = UPPER(op.ticker)
            JOIN LATERAL jsonb_each_text(
                CASE
                    WHEN jsonb_typeof(op.signals) = 'object'
                         AND jsonb_typeof(op.signals->'signal_contributions') = 'object'
                    THEN op.signals->'signal_contributions'
                    ELSE '{}'::jsonb
                END
            ) sig ON TRUE
            WHERE op.verdict IN ('hit', 'miss', 'partial')
        )
        SELECT
            ticker, direction, horizon_days, regime, signal_source, model_name,
            COUNT(*)::int AS samples,
            COUNT(*) FILTER (WHERE verdict = 'hit')::int AS hits,
            COUNT(*) FILTER (WHERE verdict = 'partial')::int AS partials,
            COUNT(*) FILTER (WHERE verdict = 'miss')::int AS misses,
            AVG(outcome) AS hit_rate,
            AVG(ABS(contribution_weight)) AS avg_contribution_weight,
            AVG(confidence) AS avg_confidence,
            AVG(POWER(COALESCE(confidence, 0.5) - outcome, 2)) AS brier,
            AVG(ABS(COALESCE(confidence, 0.5) - outcome)) AS ece,
            MIN(created_at) AS first_seen,
            MAX(created_at) AS last_seen,
            MAX(scored_at) AS last_scored_at,
            MIN(volume_rank) AS volume_rank,
            MAX(dollar_volume) AS dollar_volume,
            NOW() AS updated_at
        FROM scored
        GROUP BY ticker, direction, horizon_days, regime, signal_source, model_name
    """))
    return int(result.rowcount or 0)


def _materialize_options_coverage(conn: Any) -> int:
    conn.execute(text("TRUNCATE TABLE surfacer_options_coverage"))
    result = conn.execute(text("""
        INSERT INTO surfacer_options_coverage (
            ticker, latest_signal_date, rows_available, avg_total_volume, avg_total_oi,
            latest_total_volume, latest_total_oi, latest_iv_atm, latest_spot_price,
            latest_near_expiry, volume_rank, dollar_volume, updated_at
        )
        WITH ranked AS (
            SELECT
                UPPER(o.ticker) AS ticker,
                o.signal_date,
                o.total_volume,
                o.total_oi,
                o.iv_atm,
                o.spot_price,
                o.near_expiry,
                tv.volume_rank,
                tv.dollar_volume,
                ROW_NUMBER() OVER (PARTITION BY UPPER(o.ticker) ORDER BY o.signal_date DESC NULLS LAST) AS rn
            FROM options_daily_signals o
            JOIN surfacer_top_volume_universe tv ON tv.ticker = UPPER(o.ticker)
        ),
        agg AS (
            SELECT
                ticker,
                COUNT(*)::int AS rows_available,
                AVG(total_volume) AS avg_total_volume,
                AVG(total_oi) AS avg_total_oi
            FROM ranked
            GROUP BY ticker
        )
        SELECT
            r.ticker,
            r.signal_date AS latest_signal_date,
            a.rows_available,
            a.avg_total_volume,
            a.avg_total_oi,
            r.total_volume AS latest_total_volume,
            r.total_oi AS latest_total_oi,
            r.iv_atm AS latest_iv_atm,
            r.spot_price AS latest_spot_price,
            r.near_expiry AS latest_near_expiry,
            r.volume_rank,
            r.dollar_volume,
            NOW() AS updated_at
        FROM ranked r
        JOIN agg a ON a.ticker = r.ticker
        WHERE r.rn = 1
    """))
    return int(result.rowcount or 0)


def _materialize_requirements(conn: Any) -> int:
    conn.execute(text("DROP TABLE IF EXISTS tmp_surfacer_data_requirements"))
    conn.execute(text("""
        CREATE TEMP TABLE tmp_surfacer_data_requirements ON COMMIT DROP AS
        WITH base AS (
            SELECT
                tv.ticker,
                tv.volume_rank,
                tv.dollar_volume,
                CASE WHEN tv.volume_rank <= 1000 THEN 1
                     WHEN tv.volume_rank <= 5000 THEN 2
                     ELSE 3
                END AS priority
            FROM surfacer_top_volume_universe tv
        ),
        ticker_missing AS (
            SELECT
                b.ticker,
                'ticker_direction_calibration' AS requirement_type,
                b.priority,
                'No settled ticker/direction/horizon calibration rows exist yet.' AS reason,
                jsonb_build_object(
                    'source_tables', jsonb_build_array('oracle_predictions', 'raw_series', 'resolved_series'),
                    'target_tables', jsonb_build_array('surfacer_ticker_calibration'),
                    'acceptance_criteria', jsonb_build_array(
                        'settled hit/miss/partial samples by ticker',
                        'direction, horizon, regime, and model buckets',
                        'point-in-time safe scoring only'
                    )
                ) AS payload,
                b.volume_rank,
                b.dollar_volume
            FROM base b
            WHERE NOT EXISTS (
                SELECT 1 FROM surfacer_ticker_calibration stc WHERE stc.ticker = b.ticker
            )
        ),
        signal_missing AS (
            SELECT
                b.ticker,
                'signal_deaggregation' AS requirement_type,
                b.priority,
                'Ticker has oracle calibration but no per-signal calibration rows.' AS reason,
                jsonb_build_object(
                    'source_tables', jsonb_build_array('oracle_predictions.signals', 'signal_data'),
                    'target_tables', jsonb_build_array('surfacer_signal_calibration', 'per_signal_brier_history', 'regime_conditional_brier_history'),
                    'acceptance_criteria', jsonb_build_array(
                        'extract signal_contributions',
                        'score every contributing signal source separately',
                        'preserve horizon and regime buckets'
                    )
                ) AS payload,
                b.volume_rank,
                b.dollar_volume
            FROM base b
            WHERE EXISTS (
                SELECT 1 FROM surfacer_ticker_calibration stc WHERE stc.ticker = b.ticker
            )
            AND NOT EXISTS (
                SELECT 1 FROM surfacer_signal_calibration ssc WHERE ssc.ticker = b.ticker
            )
        ),
        options_missing AS (
            SELECT
                b.ticker,
                'options_expectation' AS requirement_type,
                b.priority,
                'No options expectation row exists for this liquid ticker.' AS reason,
                jsonb_build_object(
                    'source_tables', jsonb_build_array('options_daily_signals', 'option_contracts', 'yfinance options', 'polygon options'),
                    'target_tables', jsonb_build_array('options_daily_signals', 'surfacer_options_coverage'),
                    'acceptance_criteria', jsonb_build_array(
                        'latest option chain summarized by ticker',
                        'ATM IV, total open interest, total volume, nearest expiry',
                        'mark true no-options tickers separately instead of failing silently'
                    )
                ) AS payload,
                b.volume_rank,
                b.dollar_volume
            FROM base b
            WHERE NOT EXISTS (
                SELECT 1 FROM surfacer_options_coverage soc WHERE soc.ticker = b.ticker
            )
        )
        SELECT ticker, requirement_type, priority, reason, payload, volume_rank, dollar_volume
        FROM ticker_missing
        UNION ALL
        SELECT ticker, requirement_type, priority, reason, payload, volume_rank, dollar_volume
        FROM signal_missing
        UNION ALL
        SELECT ticker, requirement_type, priority, reason, payload, volume_rank, dollar_volume
        FROM options_missing
    """))
    desired_count = conn.execute(text("SELECT COUNT(*) FROM tmp_surfacer_data_requirements")).scalar()
    conn.execute(text("""
        INSERT INTO surfacer_data_requirements (
            ticker, requirement_type, priority, status, reason, payload,
            volume_rank, dollar_volume, created_at, updated_at
        )
        SELECT ticker, requirement_type, priority, 'pending', reason, payload,
               volume_rank, dollar_volume, NOW(), NOW()
        FROM tmp_surfacer_data_requirements
        ON CONFLICT (ticker, requirement_type) DO UPDATE SET
            priority = EXCLUDED.priority,
            reason = EXCLUDED.reason,
            payload = COALESCE(surfacer_data_requirements.payload, '{}'::jsonb) || EXCLUDED.payload,
            volume_rank = EXCLUDED.volume_rank,
            dollar_volume = EXCLUDED.dollar_volume,
            status = CASE
                WHEN surfacer_data_requirements.status IN ('processing', 'done', 'no_data', 'error')
                    THEN surfacer_data_requirements.status
                ELSE 'pending'
            END,
            updated_at = NOW()
    """))
    conn.execute(text("""
        UPDATE surfacer_data_requirements s
        SET status = 'done',
            reason = 'Requirement satisfied by current Surfacer coverage materialization.',
            payload = COALESCE(s.payload, '{}'::jsonb)
                || jsonb_build_object('satisfied_at', NOW(), 'satisfied_by', 'backfill_surfacer_calibration'),
            updated_at = NOW()
        WHERE s.status <> 'processing'
          AND NOT EXISTS (
              SELECT 1
              FROM tmp_surfacer_data_requirements t
              WHERE t.ticker = s.ticker
                AND t.requirement_type = s.requirement_type
          )
    """))
    return int(desired_count or 0)


def _ensure_llm_backlog(conn: Any) -> None:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS llm_task_backlog (
            id BIGSERIAL PRIMARY KEY,
            task_type TEXT NOT NULL,
            prompt TEXT NOT NULL,
            context JSONB DEFAULT '{}',
            priority INTEGER DEFAULT 3,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ltb_status ON llm_task_backlog (status, created_at)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ltb_context_dedupe ON llm_task_backlog ((context->>'dedupe_key'))"))


def _queue_requirements(conn: Any, limit: int) -> int:
    if limit <= 0:
        return 0
    _ensure_llm_backlog(conn)
    rows = conn.execute(
        text("""
            SELECT ticker, requirement_type, priority, reason, payload,
                   volume_rank, dollar_volume
            FROM surfacer_data_requirements
            WHERE status = 'pending'
            ORDER BY priority ASC, volume_rank ASC, requirement_type ASC
            LIMIT :limit
        """),
        {"limit": limit},
    ).fetchall()
    queued = 0
    for row in rows:
        data = dict(row._mapping)
        dedupe_key = f"surfacer:universe:{data['requirement_type']}:{data['ticker']}"
        exists = conn.execute(
            text("""
                SELECT 1
                FROM llm_task_backlog
                WHERE task_type = 'surfacer_data_backfill'
                  AND context->>'dedupe_key' = :dedupe_key
                  AND status IN ('pending', 'processing', 'distributed', 'done')
                LIMIT 1
            """),
            {"dedupe_key": dedupe_key},
        ).fetchone()
        if exists:
            continue
        context = {
            "dedupe_key": dedupe_key,
            "created_by": "surfacer_universe_backfill",
            **data,
        }
        prompt = (
            "SURFACER UNIVERSE DATA REQUIREMENT\n\n"
            "Close this specific missing-data requirement. Prefer deterministic GRID "
            "pullers and exact SQL writes over narrative. Return strict JSON with "
            "ticker, requirement_type, source_queries, puller_or_script, target_tables, "
            "write_plan, blockers, and confidence.\n\n"
            f"{json.dumps(context, default=str, indent=2)}"
        )
        conn.execute(
            text("""
                INSERT INTO llm_task_backlog (task_type, prompt, context, priority, status)
                VALUES ('surfacer_data_backfill', :prompt, CAST(:context AS jsonb), :priority, 'pending')
            """),
            {
                "prompt": prompt,
                "context": json.dumps(context, default=str),
                "priority": int(data.get("priority") or 3),
            },
        )
        queued += 1
    return queued


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    universe = _load_top_volume(args.limit, args.min_dollar_volume)
    if args.dry_run:
        print(json.dumps({
            "dry_run": True,
            "universe_count": len(universe),
            "top": universe[:10],
        }, indent=2, default=str))
        return 0

    engine = get_engine()
    with engine.begin() as conn:
        _ensure_tables(conn)
        _replace_universe(conn, universe)
        ticker_rows = _materialize_ticker_calibration(conn)
        signal_rows = _materialize_signal_calibration(conn)
        options_rows = _materialize_options_coverage(conn)
        requirement_rows = _materialize_requirements(conn)
        queued_requirements = _queue_requirements(conn, args.queue_requirements)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_count": len(universe),
        "ticker_calibration_rows": ticker_rows,
        "signal_calibration_rows": signal_rows,
        "options_coverage_rows": options_rows,
        "data_requirement_rows": requirement_rows,
        "queued_requirement_tasks": queued_requirements,
    }
    log.info("surfacer calibration backfill complete: {s}", s=summary)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
