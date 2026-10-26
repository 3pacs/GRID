#!/usr/bin/env python3
"""GRID — gem_outcomes evaluation loop (task #119).

For each row in ``gem_alerts``, attempt to:
  1. Parse a ticker + predicted direction (BULL/BEAR/NEUTRAL) from the gem.
  2. Look up the close price at ``detected_at::date`` (or nearest prior session).
  3. Look up the close price at ``detected_at + window`` for each of
     {1d, 3d, 7d, 14d, 30d}.
  4. Classify HIT / MISS / WRONG_DIRECTION / INCONCLUSIVE.
  5. UPSERT into ``gem_outcomes`` (idempotent on (gem_id, ticker, window)).

Price source: ``raw_series`` (series_id pattern ``YF:<TICKER>:close``), with
Tiingo REST as a one-shot fallback for tickers that aren't pulled locally.

Run idempotently — re-running on the same gem_id only inserts windows that
haven't been evaluated yet.

CLI:
    python3 evaluate_gem_outcomes.py [--limit N] [--gem-id ID]
                                     [--windows 1d,3d,7d,14d,30d]
                                     [--hit-threshold 0.02]
                                     [--rebuild-view]
                                     [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed in this interpreter", file=sys.stderr)
    raise

try:
    import requests
except ImportError:
    requests = None  # Tiingo fallback disabled


def _connect_params_from_env() -> dict[str, str | int]:
    """Read DB connection params from the process env as keyword args.

    This script's systemd unit already loads
    /home/grid/grid_v4/grid_repo/.env via EnvironmentFile=, the same file
    every other GRID service reads its DB credentials from — no credential
    belongs hardcoded (or defaulted) in a script. Passed to
    psycopg2.connect() as kwargs rather than interpolated into a conninfo
    string — manual interpolation breaks (or worse, silently misparses) on
    a password containing a space, quote, or backslash. Mirrors the fix
    already applied to scripts/td_backfill_universe.py (commit b978ee91).
    """
    password = os.getenv("DB_PASSWORD", "")
    if not password:
        sys.exit("DB_PASSWORD missing from environment")
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME", "griddb"),
        "user": os.getenv("DB_USER", "grid"),
        "password": password,
    }


DB_CONNECT_PARAMS = _connect_params_from_env()

TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")
TIINGO_BASE = "https://api.tiingo.com"
TIINGO_RATE_LIMIT = 0.25  # seconds between calls

DEFAULT_WINDOWS = ("1d", "3d", "7d", "14d", "30d")
DEFAULT_HIT_THRESHOLD = 0.02  # 2%

# Mirrors `extract_trade_ticket.parse_ticker_direction`
_TICKER_RE = re.compile(r"\b([A-Z]{1,5})\b")
_SUBJECT_RE = re.compile(r"\|\|([A-Z]{1,5})\b|__([A-Z]{1,5})__|__([A-Z]{1,5})$")
_HEADING_RE = re.compile(r"\b([A-Z]{1,5})\s+is\s+heading\s+(CALL|PUT)", re.IGNORECASE)
_NON_TICKERS = {
    "MACRO", "LONG", "SHORT", "CALL", "PUT", "USD", "EUR", "GBP",
    "FX", "USA", "CEO", "CFO", "CTO", "SEC", "FED", "GDP", "CPI",
    "AI", "ML", "API", "IT", "VC", "PE", "PR", "HR", "IPO", "ETF",
}

DIR_BULL = "BULL"
DIR_BEAR = "BEAR"
DIR_NEUTRAL = "NEUTRAL"


@dataclass
class TickerHint:
    ticker: str
    direction: str   # BULL | BEAR | NEUTRAL
    source: str


# ── Ticker / direction parsing ─────────────────────────────────────────────

def _candidate_tickers(text: str) -> list[str]:
    out: list[str] = []
    for m in _TICKER_RE.finditer(text or ""):
        sym = m.group(1)
        if 1 <= len(sym) <= 5 and sym not in _NON_TICKERS:
            out.append(sym)
    return out


def _direction_from_evidence(evidence: dict, score: float | None) -> str:
    kind = (evidence.get("kind") or "").lower() if isinstance(evidence, dict) else ""
    if kind == "new_high":
        return DIR_BULL
    if kind == "new_low":
        return DIR_BEAR
    # For bootstrap_ci_break / correlation_break we have no direction. Use score sign.
    if score is None:
        return DIR_NEUTRAL
    return DIR_BULL if score >= 0 else DIR_BEAR


def parse_ticker_direction(gem: dict) -> TickerHint | None:
    source = gem.get("source", "") or ""
    subject_id = gem.get("subject_id", "") or ""
    related_ids = gem.get("related_ids") or []
    evidence = gem.get("evidence") or {}
    if isinstance(evidence, str):
        try:
            evidence = json.loads(evidence)
        except Exception:
            evidence = {}
    if isinstance(related_ids, str):
        try:
            related_ids = json.loads(related_ids)
        except Exception:
            related_ids = []

    score = gem.get("score")

    # 1. ``X is heading CALL/PUT`` anywhere in evidence text
    blob = json.dumps(evidence) if isinstance(evidence, dict) else str(evidence)
    m = _HEADING_RE.search(blob)
    if m:
        return TickerHint(
            ticker=m.group(1).upper(),
            direction=DIR_BULL if m.group(2).upper() == "CALL" else DIR_BEAR,
            source="heading_re",
        )

    # 2. subject_id `||TICKER` or `__TICKER` or `__TICKER__`
    for m in _SUBJECT_RE.finditer(subject_id):
        for grp in m.groups():
            if grp and grp not in _NON_TICKERS:
                return TickerHint(
                    ticker=grp.upper(),
                    direction=_direction_from_evidence(evidence, score),
                    source="subject_id",
                )

    # 3. related_ids — try the rightmost looks-like-a-ticker entry
    for rid in reversed(list(related_ids) if isinstance(related_ids, list) else []):
        if not isinstance(rid, str):
            continue
        rid_stripped = rid.split(":")[-1].split("/")[-1].upper()
        if 1 <= len(rid_stripped) <= 5 and rid_stripped.isalpha() and rid_stripped not in _NON_TICKERS:
            return TickerHint(
                ticker=rid_stripped,
                direction=_direction_from_evidence(evidence, score),
                source="related_ids",
            )

    # 4. Evidence-embedded ticker/symbol
    if isinstance(evidence, dict):
        for key in ("ticker", "symbol", "target_ticker"):
            v = evidence.get(key)
            if isinstance(v, str) and v.upper() not in _NON_TICKERS:
                return TickerHint(
                    ticker=v.upper(),
                    direction=_direction_from_evidence(evidence, score),
                    source=f"evidence.{key}",
                )

    return None


# ── Price lookup ───────────────────────────────────────────────────────────

def _fetch_local_close(cur, ticker: str, target_date: date,
                      back_days: int = 7) -> tuple[float | None, date | None]:
    """Return (close, obs_date) for the closest session at-or-before target_date.

    Looks up to ``back_days`` calendar days backwards to skip weekends/holidays.
    """
    series_id = f"YF:{ticker}:close"
    cur.execute(
        """
        SELECT value, obs_date
        FROM raw_series
        WHERE series_id = %s
          AND obs_date BETWEEN %s AND %s
          AND pull_status = 'SUCCESS'
        ORDER BY obs_date DESC, id DESC
        LIMIT 1
        """,
        (series_id, target_date - timedelta(days=back_days), target_date),
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        return float(row[0]), row[1]
    return None, None


def _fetch_tiingo_close(ticker: str, target_date: date,
                       back_days: int = 7) -> tuple[float | None, date | None]:
    """One-shot Tiingo fallback for tickers we don't pull locally."""
    if not TIINGO_API_KEY or requests is None:
        return None, None
    clean = ticker.replace("^", "").replace("=F", "").replace("=X", "")
    url = f"{TIINGO_BASE}/tiingo/daily/{clean}/prices"
    params = {
        "startDate": str(target_date - timedelta(days=back_days)),
        "endDate": str(target_date),
        "format": "json",
    }
    headers = {"Authorization": f"Token {TIINGO_API_KEY}"}
    try:
        time.sleep(TIINGO_RATE_LIMIT)
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        if resp.status_code != 200:
            return None, None
        data = resp.json()
        if not isinstance(data, list) or not data:
            return None, None
        last = data[-1]
        d_str = last.get("date") or ""
        try:
            d = datetime.fromisoformat(d_str.replace("Z", "+00:00")).date()
        except Exception:
            d = target_date
        return float(last.get("close")), d
    except Exception:
        return None, None


def fetch_close(cur, ticker: str, target_date: date,
                tiingo_cache: dict, use_tiingo: bool) -> tuple[float | None, date | None]:
    val, obs = _fetch_local_close(cur, ticker, target_date)
    if val is not None:
        return val, obs
    if not use_tiingo:
        return None, None
    cache_key = (ticker, target_date.isoformat())
    if cache_key in tiingo_cache:
        return tiingo_cache[cache_key]
    val, obs = _fetch_tiingo_close(ticker, target_date)
    tiingo_cache[cache_key] = (val, obs)
    return val, obs


# ── Window evaluation ──────────────────────────────────────────────────────

def _window_days(w: str) -> int:
    m = re.match(r"^(\d+)([dwmy])$", w)
    if not m:
        raise ValueError(f"bad window: {w}")
    n = int(m.group(1))
    unit = m.group(2)
    return {"d": n, "w": n * 7, "m": n * 30, "y": n * 365}[unit]


def classify(direction: str, pct_move: float | None,
             threshold: float) -> str:
    if pct_move is None:
        return "INCONCLUSIVE"
    if direction == DIR_NEUTRAL:
        # Neutral predictions: HIT if move stays under threshold.
        return "HIT" if abs(pct_move) < threshold else "MISS"
    if abs(pct_move) < threshold:
        return "MISS"
    moved_up = pct_move > 0
    predicted_up = (direction == DIR_BULL)
    return "HIT" if moved_up == predicted_up else "WRONG_DIRECTION"


# ── Schema management ─────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS gem_outcomes (
  id BIGSERIAL PRIMARY KEY,
  gem_id BIGINT REFERENCES gem_alerts(id) ON DELETE CASCADE,
  ticker TEXT NOT NULL,
  predicted_direction TEXT NOT NULL,
  evaluation_window TEXT NOT NULL,
  price_at_detection REAL,
  price_at_window_end REAL,
  pct_move REAL,
  hit_or_miss TEXT,
  detection_date DATE,
  window_end_date DATE,
  evaluated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(gem_id, ticker, evaluation_window)
);
CREATE INDEX IF NOT EXISTS idx_gem_outcomes_gem ON gem_outcomes(gem_id);
CREATE INDEX IF NOT EXISTS idx_gem_outcomes_hit ON gem_outcomes(hit_or_miss);
CREATE INDEX IF NOT EXISTS idx_gem_outcomes_ticker ON gem_outcomes(ticker, evaluation_window);
"""

VIEW_SQL = """
CREATE OR REPLACE VIEW v_rule_win_rate AS
SELECT
  a.source                        AS rule_source,
  a.subject_kind                  AS rule_subject_kind,
  o.evaluation_window,
  COUNT(*)                        AS n,
  SUM(CASE WHEN o.hit_or_miss = 'HIT' THEN 1 ELSE 0 END)                     AS n_hit,
  SUM(CASE WHEN o.hit_or_miss = 'MISS' THEN 1 ELSE 0 END)                    AS n_miss,
  SUM(CASE WHEN o.hit_or_miss = 'WRONG_DIRECTION' THEN 1 ELSE 0 END)         AS n_wrong,
  SUM(CASE WHEN o.hit_or_miss = 'INCONCLUSIVE' THEN 1 ELSE 0 END)            AS n_inconclusive,
  ROUND(
    SUM(CASE WHEN o.hit_or_miss = 'HIT' THEN 1 ELSE 0 END)::numeric
      / NULLIF(SUM(CASE WHEN o.hit_or_miss IN ('HIT','MISS','WRONG_DIRECTION') THEN 1 ELSE 0 END), 0),
    4
  )                               AS win_rate,
  AVG(o.pct_move) FILTER (WHERE o.hit_or_miss = 'HIT')                       AS avg_hit_pct,
  AVG(o.pct_move) FILTER (WHERE o.hit_or_miss = 'WRONG_DIRECTION')           AS avg_wrong_pct
FROM gem_alerts a
JOIN gem_outcomes o ON o.gem_id = a.id
GROUP BY a.source, a.subject_kind, o.evaluation_window;
"""


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute(VIEW_SQL)
    conn.commit()


# ── Main loop ──────────────────────────────────────────────────────────────

def fetch_gems(cur, limit: int | None, gem_id: int | None) -> list[dict]:
    where = []
    args: list = []
    if gem_id is not None:
        where.append("id = %s")
        args.append(gem_id)
    sql = """
        SELECT id, detected_at, source, subject_kind, subject_id,
               related_ids, evidence, score
        FROM gem_alerts
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id ASC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql, args)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def already_evaluated(cur, gem_id: int, ticker: str, window: str) -> bool:
    cur.execute(
        "SELECT 1 FROM gem_outcomes WHERE gem_id = %s AND ticker = %s AND evaluation_window = %s",
        (gem_id, ticker, window),
    )
    return cur.fetchone() is not None


def upsert_outcome(cur, *, gem_id: int, ticker: str, direction: str, window: str,
                   p0: float | None, p1: float | None, pct_move: float | None,
                   verdict: str, det_date: date | None, end_date: date | None) -> None:
    cur.execute(
        """
        INSERT INTO gem_outcomes (
          gem_id, ticker, predicted_direction, evaluation_window,
          price_at_detection, price_at_window_end, pct_move, hit_or_miss,
          detection_date, window_end_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (gem_id, ticker, evaluation_window) DO NOTHING
        """,
        (gem_id, ticker, direction, window, p0, p1, pct_move, verdict, det_date, end_date),
    )


def evaluate(
    *,
    limit: int | None,
    gem_id: int | None,
    windows: Iterable[str],
    hit_threshold: float,
    use_tiingo: bool,
    dry_run: bool,
) -> dict:
    today = date.today()
    stats = {
        "gems_total": 0,
        "gems_skipped_no_ticker": 0,
        "windows_attempted": 0,
        "windows_skipped_existing": 0,
        "windows_skipped_too_soon": 0,
        "windows_written": 0,
        "by_verdict": {"HIT": 0, "MISS": 0, "WRONG_DIRECTION": 0, "INCONCLUSIVE": 0},
        "by_window": {},
    }
    tiingo_cache: dict = {}

    conn = psycopg2.connect(**DB_CONNECT_PARAMS)
    conn.autocommit = False
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            gems = fetch_gems(cur, limit, gem_id)
            stats["gems_total"] = len(gems)

            for gem in gems:
                hint = parse_ticker_direction(gem)
                if hint is None:
                    stats["gems_skipped_no_ticker"] += 1
                    continue

                ticker = hint.ticker
                direction = hint.direction
                detected_at = gem["detected_at"]
                det_day = detected_at.date() if hasattr(detected_at, "date") else detected_at

                p0, p0_d = fetch_close(cur, ticker, det_day, tiingo_cache, use_tiingo)

                for w in windows:
                    stats["windows_attempted"] += 1
                    stats["by_window"].setdefault(w, {"HIT": 0, "MISS": 0,
                                                      "WRONG_DIRECTION": 0,
                                                      "INCONCLUSIVE": 0})
                    if already_evaluated(cur, gem["id"], ticker, w):
                        stats["windows_skipped_existing"] += 1
                        continue

                    target = det_day + timedelta(days=_window_days(w))
                    if target > today:
                        stats["windows_skipped_too_soon"] += 1
                        continue

                    p1, p1_d = fetch_close(cur, ticker, target, tiingo_cache, use_tiingo)
                    # If the window-end price falls back to the same session
                    # as the detection price, the window hasn't actually
                    # completed yet — mark inconclusive instead of fake MISS.
                    if p0 is None or p1 is None or (p0_d and p1_d and p1_d <= p0_d):
                        verdict = "INCONCLUSIVE"
                        pct = None
                    else:
                        pct = (p1 - p0) / p0
                        verdict = classify(direction, pct, hit_threshold)

                    stats["by_verdict"][verdict] += 1
                    stats["by_window"][w][verdict] += 1

                    if not dry_run:
                        upsert_outcome(
                            cur,
                            gem_id=gem["id"], ticker=ticker, direction=direction,
                            window=w, p0=p0, p1=p1, pct_move=pct, verdict=verdict,
                            det_date=p0_d if p0_d else det_day,
                            end_date=p1_d if p1_d else target,
                        )
                        stats["windows_written"] += 1
                conn.commit()
    finally:
        conn.close()

    return stats


# ── Reporting ──────────────────────────────────────────────────────────────

def print_win_rate_table() -> None:
    conn = psycopg2.connect(**DB_CONNECT_PARAMS)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT rule_source, rule_subject_kind, evaluation_window,
                       n, n_hit, n_miss, n_wrong, n_inconclusive,
                       win_rate, avg_hit_pct
                FROM v_rule_win_rate
                ORDER BY rule_source, rule_subject_kind, evaluation_window
            """)
            rows = cur.fetchall()
    finally:
        conn.close()
    print("\n=== Per-rule × window win rate ===")
    print(f"{'rule_source':<22} {'subject':<14} {'win':<5} {'n':>4} "
          f"{'hit':>4} {'miss':>4} {'wrong':>5} {'inc':>4} {'win%':>7} {'avg_hit':>8}")
    for r in rows:
        rs, sk, w, n, h, m, wr, inc, wrate, avg_h = r
        wrate_s = f"{(wrate or 0)*100:>6.1f}%"
        avg_s = f"{(avg_h or 0)*100:>+7.2f}%" if avg_h is not None else "    n/a"
        print(f"{rs:<22} {sk:<14} {w:<5} {n:>4} {h:>4} {m:>4} {wr:>5} {inc:>4} {wrate_s:>7} {avg_s:>8}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--gem-id", type=int, default=None)
    ap.add_argument("--windows", type=str, default=",".join(DEFAULT_WINDOWS))
    ap.add_argument("--hit-threshold", type=float, default=DEFAULT_HIT_THRESHOLD)
    ap.add_argument("--no-tiingo", action="store_true",
                    help="Skip Tiingo fallback; raw_series only")
    ap.add_argument("--rebuild-view", action="store_true",
                    help="Recreate v_rule_win_rate then exit")
    ap.add_argument("--print-rates", action="store_true",
                    help="Print win-rate table after run")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    windows = tuple(w.strip() for w in args.windows.split(",") if w.strip())

    if args.rebuild_view:
        conn = psycopg2.connect(**DB_CONNECT_PARAMS)
        try:
            ensure_schema(conn)
            print("schema + view rebuilt.")
        finally:
            conn.close()
        return 0

    stats = evaluate(
        limit=args.limit,
        gem_id=args.gem_id,
        windows=windows,
        hit_threshold=args.hit_threshold,
        use_tiingo=not args.no_tiingo,
        dry_run=args.dry_run,
    )

    print("=== gem_outcomes evaluation summary ===")
    print(json.dumps(stats, indent=2, default=str))

    if args.print_rates:
        print_win_rate_table()

    return 0


if __name__ == "__main__":
    sys.exit(main())
