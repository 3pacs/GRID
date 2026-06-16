#!/usr/bin/env python3
"""Fill Surfacer options-expectation requirements from yfinance chains.

This script is a deterministic worker for ``surfacer_data_requirements``.
It claims pending ``options_expectation`` rows by volume rank, writes
``options_snapshots`` and ``options_daily_signals``, then marks the
requirement done/no_data/error or defers transiently slow tickers.
"""

from __future__ import annotations

import argparse
import math
import json
import multiprocessing as mp
import sys
import time
from datetime import date
from pathlib import Path
from queue import Empty
from typing import Any

import numpy as np
import psycopg2
from loguru import logger as log

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings
from scripts.pull_options import compute_iv_skew, compute_max_pain, create_tables

try:
    import yfinance as yf
except ImportError as exc:  # pragma: no cover
    yf = None
    _YFINANCE_IMPORT_ERROR = exc
else:
    _YFINANCE_IMPORT_ERROR = None


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull options data for Surfacer requirements.")
    parser.add_argument("--limit", type=int, default=250, help="Number of requirement rows to claim.")
    parser.add_argument("--priority-max", type=int, default=2, help="Only process requirements at or above this priority.")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep between tickers for rate limiting.")
    parser.add_argument("--ticker-timeout", type=int, default=60, help="Maximum seconds to spend on one ticker.")
    parser.add_argument("--defer-minutes", type=int, default=30, help="Cooldown for tickers that hit the timeout.")
    parser.add_argument(
        "--max-expirations",
        type=int,
        default=3,
        help="Maximum option expirations to pull per ticker. Use 1 for a fast Surfacer expectation pass.",
    )
    parser.add_argument(
        "--reset-stale-minutes",
        type=int,
        default=90,
        help="Return stuck processing requirements to pending after this many minutes.",
    )
    return parser.parse_args(argv)


def _connect():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def _ensure_columns(cur: Any) -> int:
    create_tables(cur)
    cur.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS spot_price DOUBLE PRECISION")
    cur.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS iv_atm DOUBLE PRECISION")
    cur.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS iv_25d_put DOUBLE PRECISION")
    cur.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS iv_25d_call DOUBLE PRECISION")
    cur.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS term_structure_slope DOUBLE PRECISION")
    cur.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS oi_concentration DOUBLE PRECISION")
    cur.execute(
        "INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,"
        "revision_behavior,trust_score,priority_rank) "
        "VALUES ('YFINANCE_OPTIONS','https://finance.yahoo.com','FREE','EOD',FALSE,"
        "'FREQUENT','MED',7) ON CONFLICT (name) DO NOTHING"
    )
    cur.execute("SELECT id FROM source_catalog WHERE name='YFINANCE_OPTIONS'")
    return cur.fetchone()[0]


def _claim(cur: Any, limit: int, priority_max: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        UPDATE surfacer_data_requirements
        SET status = 'processing', updated_at = NOW()
        WHERE id IN (
            SELECT id
            FROM surfacer_data_requirements
            WHERE requirement_type = 'options_expectation'
              AND status = 'pending'
              AND priority <= %s
              AND COALESCE((payload->>'deferred_until')::TIMESTAMPTZ, '-infinity'::TIMESTAMPTZ) <= NOW()
            ORDER BY priority ASC, volume_rank ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, ticker, priority, volume_rank, dollar_volume
        """,
        (priority_max, limit),
    )
    return [
        {
            "id": row[0],
            "ticker": row[1],
            "priority": row[2],
            "volume_rank": row[3],
            "dollar_volume": row[4],
        }
        for row in cur.fetchall()
    ]


def _reset_stale(cur: Any, stale_minutes: int) -> int:
    if stale_minutes <= 0:
        return 0
    cur.execute(
        """
        UPDATE surfacer_data_requirements
        SET status = 'pending',
            payload = COALESCE(payload, '{}'::jsonb)
                || jsonb_build_object('requeued_at', NOW(), 'requeue_reason', 'stale_processing'),
            updated_at = NOW()
        WHERE requirement_type = 'options_expectation'
          AND status = 'processing'
          AND updated_at < NOW() - (%s || ' minutes')::INTERVAL
        """,
        (stale_minutes,),
    )
    return cur.rowcount


def _safe_int(value: Any) -> int:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return 0
        return int(value)
    except Exception:
        return 0


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        return float(value)
    except Exception:
        return None


def _spot_price(stock: Any) -> float | None:
    try:
        fast_info = stock.fast_info
        spot = fast_info.get("last_price") or fast_info.get("previous_close")
        if spot:
            return float(spot)
    except Exception:
        pass

    try:
        history = stock.history(period="5d", interval="1d", auto_adjust=False)
        if history is not None and not history.empty and "Close" in history.columns:
            close = history["Close"].dropna()
            if not close.empty:
                return float(close.iloc[-1])
    except Exception:
        pass

    try:
        info = stock.info
        spot = info.get("regularMarketPrice") or info.get("previousClose")
        if spot:
            return float(spot)
    except Exception:
        pass

    return None


def _require_yfinance() -> Any:
    if yf is None:
        raise RuntimeError(
            "yfinance is unavailable; install/update yfinance and its dependencies "
            f"before running the Surfacer options puller ({_YFINANCE_IMPORT_ERROR})"
        )
    return yf


def _atm_iv(calls: Any, puts: Any, spot: float) -> float | None:
    ivs = []
    for df in (calls, puts):
        if df.empty or "strike" not in df.columns or "impliedVolatility" not in df.columns:
            continue
        near = df[(df["strike"] >= spot * 0.97) & (df["strike"] <= spot * 1.03)]
        if not near.empty:
            ivs.extend([float(v) for v in near["impliedVolatility"].dropna().tolist()])
    return sum(ivs) / len(ivs) if ivs else None


def _pull_one(
    cur: Any,
    ticker: str,
    src_id: int,
    today: str,
    max_expirations: int = 3,
) -> dict[str, Any]:
    yfinance = _require_yfinance()
    stock = yfinance.Ticker(ticker)
    spot = _spot_price(stock)
    if not spot:
        return {"status": "deferred", "reason": "no spot price from provider"}

    expirations = list(stock.options or [])
    if not expirations:
        return {"status": "no_data", "reason": "no options expirations", "spot_price": spot}

    total_call_oi = total_put_oi = total_call_vol = total_put_vol = 0
    snap_count = 0
    iv_by_expiry: list[float] = []
    near_expiry = expirations[0]
    nearest_chain = None

    expiry_limit = max(1, int(max_expirations or 1))
    for exp in expirations[:expiry_limit]:
        chain = stock.option_chain(exp)
        if nearest_chain is None:
            nearest_chain = chain
        exp_iv = _atm_iv(chain.calls, chain.puts, float(spot))
        if exp_iv is not None:
            iv_by_expiry.append(exp_iv)
        for opt_type, df in (("call", chain.calls), ("put", chain.puts)):
            if df.empty:
                continue
            oi_sum = _safe_int(df["openInterest"].fillna(0).sum()) if "openInterest" in df else 0
            vol_sum = _safe_int(df["volume"].fillna(0).sum()) if "volume" in df else 0
            if opt_type == "call":
                total_call_oi += oi_sum
                total_call_vol += vol_sum
            else:
                total_put_oi += oi_sum
                total_put_vol += vol_sum
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO options_snapshots (
                        ticker, snap_date, expiry, opt_type, strike, last_price,
                        bid, ask, volume, open_interest, implied_vol, in_the_money
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        ticker, today, exp, opt_type, row.get("strike"),
                        _safe_float(row.get("lastPrice")),
                        _safe_float(row.get("bid")),
                        _safe_float(row.get("ask")),
                        _safe_int(row.get("volume")),
                        _safe_int(row.get("openInterest")),
                        _safe_float(row.get("impliedVolatility")),
                        bool(row.get("inTheMoney")) if row.get("inTheMoney") is not None else None,
                    ),
                )
                snap_count += 1

    if nearest_chain is None:
        return {"status": "no_data", "reason": "empty option chains", "spot_price": spot}

    total_oi = total_call_oi + total_put_oi
    total_volume = total_call_vol + total_put_vol
    put_call_ratio = total_put_oi / total_call_oi if total_call_oi > 0 else None
    iv_atm = iv_by_expiry[0] if iv_by_expiry else None
    term_slope = (iv_by_expiry[-1] - iv_by_expiry[0]) if len(iv_by_expiry) > 1 else None
    max_pain = compute_max_pain(nearest_chain.calls, nearest_chain.puts, float(spot))
    iv_skew = compute_iv_skew(nearest_chain.puts, float(spot))

    cur.execute(
        """
        INSERT INTO options_daily_signals (
            ticker, signal_date, put_call_ratio, max_pain, iv_skew,
            total_oi, total_volume, near_expiry, spot_price, iv_atm,
            term_structure_slope
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ticker, signal_date) DO UPDATE SET
            put_call_ratio=EXCLUDED.put_call_ratio,
            max_pain=EXCLUDED.max_pain,
            iv_skew=EXCLUDED.iv_skew,
            total_oi=EXCLUDED.total_oi,
            total_volume=EXCLUDED.total_volume,
            near_expiry=EXCLUDED.near_expiry,
            spot_price=EXCLUDED.spot_price,
            iv_atm=EXCLUDED.iv_atm,
            term_structure_slope=EXCLUDED.term_structure_slope,
            created_at=NOW()
        """,
        (
            ticker, today, put_call_ratio, max_pain, iv_skew,
            total_oi, total_volume, near_expiry, spot, iv_atm, term_slope,
        ),
    )

    return {
        "status": "done",
        "snapshots": snap_count,
        "spot_price": spot,
        "iv_atm": iv_atm,
        "total_oi": total_oi,
        "total_volume": total_volume,
        "near_expiry": near_expiry,
        "source_id": src_id,
    }


def _finish(cur: Any, req_id: int, status: str, result: dict[str, Any]) -> None:
    cur.execute(
        """
        UPDATE surfacer_data_requirements
        SET status = %s,
            payload = COALESCE(payload, '{}'::jsonb) || jsonb_build_object('last_result', CAST(%s AS jsonb)),
            updated_at = NOW()
        WHERE id = %s
        """,
        (status, json.dumps(result, default=str), req_id),
    )


def _defer(cur: Any, req_id: int, result: dict[str, Any], defer_minutes: int) -> None:
    cur.execute(
        """
        UPDATE surfacer_data_requirements
        SET status = 'pending',
            payload = COALESCE(payload, '{}'::jsonb)
                || jsonb_build_object(
                    'last_defer', CAST(%s AS jsonb),
                    'deferred_until', NOW() + (%s || ' minutes')::INTERVAL
                ),
            updated_at = NOW()
        WHERE id = %s
        """,
        (json.dumps(result, default=str), defer_minutes, req_id),
    )


def _is_transient_fetch_error(message: str) -> bool:
    lower = message.lower()
    return any(
        token in lower
        for token in (
            "curl",
            "requestexception",
            "timeout",
            "timed out",
            "connection",
            "temporarily unavailable",
            "too many requests",
        )
    )


def _pull_one_worker(
    ticker: str,
    src_id: int,
    today: str,
    max_expirations: int,
    result_queue: Any,
) -> None:
    conn = None
    try:
        conn = _connect()
        conn.autocommit = False
        cur = conn.cursor()
        result = _pull_one(cur, ticker, src_id, today, max_expirations)
        conn.commit()
        result_queue.put({"ok": True, "result": result})
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        result_queue.put(
            {
                "ok": False,
                "error_type": type(exc).__name__,
                "error": f"{type(exc).__name__}: {str(exc)[:300]}",
            }
        )
    finally:
        if conn is not None:
            conn.close()


def _run_one_with_timeout(
    cur: Any,
    item: dict[str, Any],
    src_id: int,
    today: str,
    timeout_seconds: int,
    defer_minutes: int,
    max_expirations: int,
) -> tuple[str, dict[str, Any]]:
    ticker = item["ticker"]
    if timeout_seconds <= 0:
        result = _pull_one(cur, ticker, src_id, today, max_expirations)
        status = result.get("status") or "error"
        _finish(cur, item["id"], status, result)
        return status, result

    ctx = mp.get_context("fork" if "fork" in mp.get_all_start_methods() else "spawn")
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(target=_pull_one_worker, args=(ticker, src_id, today, max_expirations, result_queue))
    process.start()
    process.join(timeout_seconds)
    if process.is_alive():
        process.terminate()
        process.join(3)
        if process.is_alive():
            process.kill()
            process.join(3)
        result = {"status": "deferred", "reason": "ticker fetch timed out", "timeout_seconds": timeout_seconds}
        _defer(cur, item["id"], result, defer_minutes)
        return "deferred", result

    try:
        message = result_queue.get_nowait()
    except Empty:
        result = {"status": "error", "error": f"worker exited without result: exitcode={process.exitcode}"}
        _finish(cur, item["id"], "error", result)
        return "error", result

    if message.get("ok"):
        result = message["result"]
        status = result.get("status") or "error"
        if status == "deferred":
            _defer(cur, item["id"], result, defer_minutes)
            return "deferred", result
        _finish(cur, item["id"], status, result)
        return status, result

    result = {"status": "deferred", "reason": message.get("error", "fetch failed")}
    if _is_transient_fetch_error(result["reason"]):
        _defer(cur, item["id"], result, defer_minutes)
        return "deferred", result

    result = {"status": "error", "error": message.get("error", "worker failed")}
    _finish(cur, item["id"], "error", result)
    return "error", result


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    conn = _connect()
    conn.autocommit = True
    cur = conn.cursor()
    src_id = _ensure_columns(cur)
    today = date.today().isoformat()
    effective_stale_minutes = max(args.reset_stale_minutes, math.ceil(args.ticker_timeout / 60) + 5)
    if effective_stale_minutes != args.reset_stale_minutes:
        log.info(
            "Raised stale reset from {requested}m to {effective}m to avoid racing live ticker workers",
            requested=args.reset_stale_minutes,
            effective=effective_stale_minutes,
        )
    stale = _reset_stale(cur, effective_stale_minutes)
    if stale:
        log.info("Requeued {n} stale Surfacer options requirements", n=stale)
    tasks = _claim(cur, args.limit, args.priority_max)
    log.info("Claimed {n} Surfacer options requirements", n=len(tasks))
    done = no_data = deferred = errors = 0
    for item in tasks:
        ticker = item["ticker"]
        try:
            status, result = _run_one_with_timeout(
                cur,
                item,
                src_id,
                today,
                args.ticker_timeout,
                args.defer_minutes,
                args.max_expirations,
            )
            if status == "done":
                done += 1
            elif status == "no_data":
                no_data += 1
            elif status == "deferred":
                deferred += 1
            else:
                errors += 1
            log.info("{ticker}: {status} {result}", ticker=ticker, status=status, result=result)
        except Exception as exc:
            errors += 1
            result = {"status": "error", "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
            _finish(cur, item["id"], "error", result)
            log.warning("{ticker}: error {err}", ticker=ticker, err=result["error"])
        time.sleep(args.sleep)
    log.info(
        "Surfacer options pull complete: done={d} no_data={n} deferred={df} errors={e}",
        d=done,
        n=no_data,
        df=deferred,
        e=errors,
    )
    conn.close()
    return 0 if errors == 0 else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
