"""Daily auto-improvement: mine the postmortem corpus and emit advisories.

Hermes accumulates trade_postmortems / thesis_postmortems /
hypothesis_postmortems silently. This script reads them on a schedule,
extracts the patterns operators would otherwise have to dig for, and
emits a daily advisory.

What it surfaces
----------------

1. **Anti-signal override clusters** — predictions where ≥2 high-severity
   anti-signals fired but were ignored, grouped by (ticker, direction).
   Already addressed by the production veto, but the report shows
   whether the veto is biting.
2. **Star signals being ignored** — signals_right ratios. A signal that
   appears 5× more often in signals_right than signals_wrong is a
   strong oracle that the conviction stack is under-weighting.
3. **Net-misleading signals** — same metric, inverse. Signals that
   appear far more in signals_wrong should be downweighted or removed.
4. **Wrong-direction ticker clusters** — tickers where the system
   consistently predicts wrong (often PUT on bull-trend tickers).
5. **Hypothesis kill-reason distribution** — PATTERN_BROKEN, NO_MOVE,
   etc. — informs whether the issue is signal accuracy vs follow-through.

Output: one JSON to ``/data/grid_obsidian/Sessions/auto-improve-YYYY-MM-DD.json``
plus a markdown summary appended to the day's hermes session log via
``intelligence.obsidian_log``.

Scheduling: see ``server_setup/crontab.d/auto_improve.cron``.

Read-only by default — no production weights touched. Surface findings,
let the operator act. Future: an ``--apply`` flag could tune signal
weights in a config file, but that needs a separate review.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text

from db import get_engine


_VAULT_SESSIONS = Path("/data/grid_obsidian/Sessions")
_MIN_OCCURRENCES_FOR_SIGNAL = 50  # ignore tail noise


def _is_per_ticker_feature(signal_name: str) -> bool:
    """Per-ticker Shapley features (e.g. ``aapl_full``, ``brazil_selic_rate``)
    appear in signals_wrong only as a TAUTOLOGY of the postmortem corpus:
    postmortems run on failures, and a ticker's own feature is always
    aligned with the prediction on that ticker. So a 0r/358w ratio just
    means "AAPL predictions failed sometimes" — not that the AAPL feature
    is bad.

    Excluding these from the star/bad lists is the right move; the
    cross-cutting signals (``feature:equity``, ``alpha_research:vix_exposure``,
    ``news_intel``, ...) are the ones whose ratios actually reflect signal
    quality.
    """
    if not signal_name:
        return False
    s = signal_name.lower()
    # Pattern: lowercase ticker followed by _full / _signal / etc.
    if s.endswith("_full") or s.endswith("_signal"):
        return True
    # Patterns like brazil_selic_rate are country/regional features, not
    # cross-cutting — same selection bias.
    if any(s.startswith(p) for p in (
        "brazil_", "china_", "japan_", "korea_", "germany_",
        "uk_", "france_", "india_", "russia_", "mexico_",
    )):
        return True
    return False


def _signal_ratios(conn) -> list[dict[str, Any]]:
    """For each distinct signal name, count appearances in signals_right
    vs signals_wrong. Returns rows sorted by absolute imbalance.
    """
    rows = conn.execute(text("""
        WITH s AS (
            SELECT jsonb_array_elements_text(CASE
                WHEN jsonb_typeof(signals_right::jsonb) = 'array'
                THEN signals_right::jsonb ELSE '[]'::jsonb END) AS sig,
                'right' AS side
            FROM trade_postmortems WHERE signals_right IS NOT NULL
            UNION ALL
            SELECT jsonb_array_elements_text(CASE
                WHEN jsonb_typeof(signals_wrong::jsonb) = 'array'
                THEN signals_wrong::jsonb ELSE '[]'::jsonb END) AS sig,
                'wrong' AS side
            FROM trade_postmortems WHERE signals_wrong IS NOT NULL
        )
        SELECT sig,
               SUM(CASE WHEN side='right' THEN 1 ELSE 0 END) AS right_ct,
               SUM(CASE WHEN side='wrong' THEN 1 ELSE 0 END) AS wrong_ct
        FROM s GROUP BY sig
        HAVING SUM(CASE WHEN side='right' THEN 1 ELSE 0 END)
             + SUM(CASE WHEN side='wrong' THEN 1 ELSE 0 END) >= :min_n
        ORDER BY ABS(SUM(CASE WHEN side='right' THEN 1 ELSE 0 END)
                  - SUM(CASE WHEN side='wrong' THEN 1 ELSE 0 END)) DESC
    """), {"min_n": _MIN_OCCURRENCES_FOR_SIGNAL}).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        right_ct, wrong_ct = int(r[1] or 0), int(r[2] or 0)
        denom = wrong_ct if wrong_ct > 0 else 1
        out.append({
            "signal": r[0],
            "right_count": right_ct,
            "wrong_count": wrong_ct,
            "right_minus_wrong": right_ct - wrong_ct,
            "right_to_wrong_ratio": round(right_ct / denom, 3),
        })
    return out


def _wrong_direction_tickers(conn) -> list[dict[str, Any]]:
    """Tickers where the system consistently predicts the wrong direction
    (top wrong-direction postmortem clusters)."""
    rows = conn.execute(text("""
        SELECT ticker,
               COUNT(*) FILTER (WHERE outcome IN ('miss','wrong')) AS wrong_n,
               COUNT(*) AS total_n,
               STRING_AGG(DISTINCT failure_category, ', ' ORDER BY failure_category) AS categories
        FROM trade_postmortems
        WHERE ticker IS NOT NULL
          AND generated_at >= NOW() - INTERVAL '30 days'
        GROUP BY ticker
        HAVING COUNT(*) >= 100
        ORDER BY COUNT(*) FILTER (WHERE outcome IN ('miss','wrong'))::float / NULLIF(COUNT(*), 0) DESC
        LIMIT 15
    """)).fetchall()
    return [
        {"ticker": r[0], "wrong_n": int(r[1] or 0), "total_n": int(r[2] or 0),
         "fail_rate": round((r[1] or 0) / max(r[2] or 1, 1), 3),
         "categories": r[3]}
        for r in rows
    ]


def _anti_signal_override_clusters(conn) -> dict[str, Any]:
    """How many ``Anti-signals overridden`` postmortems exist? Has the
    cluster shrunk since the veto landed?"""
    last_24h = int(conn.execute(text("""
        SELECT COUNT(*) FROM trade_postmortems
        WHERE root_cause LIKE 'Anti-signals warned%'
          AND generated_at >= NOW() - INTERVAL '24 hours'
    """)).scalar() or 0)
    last_7d = int(conn.execute(text("""
        SELECT COUNT(*) FROM trade_postmortems
        WHERE root_cause LIKE 'Anti-signals warned%'
          AND generated_at >= NOW() - INTERVAL '7 days'
    """)).scalar() or 0)
    total = int(conn.execute(text("""
        SELECT COUNT(*) FROM trade_postmortems
        WHERE root_cause LIKE 'Anti-signals warned%'
    """)).scalar() or 0)
    return {"last_24h": last_24h, "last_7d": last_7d, "total_all_time": total}


def _hypothesis_kill_reasons(conn) -> dict[str, int]:
    rows = conn.execute(text("""
        SELECT kill_reason, COUNT(*) FROM hypothesis_postmortems
        WHERE created_at IS NULL OR created_at >= NOW() - INTERVAL '30 days'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)).fetchall()
    out: dict[str, int] = {}
    for r in rows:
        out[str(r[0] or "NULL")] = int(r[1] or 0)
    return out


def _failure_categories(conn) -> dict[str, int]:
    rows = conn.execute(text("""
        SELECT failure_category, COUNT(*) FROM trade_postmortems
        WHERE generated_at >= NOW() - INTERVAL '30 days'
        GROUP BY 1 ORDER BY 2 DESC
    """)).fetchall()
    return {str(r[0] or "NULL"): int(r[1] or 0) for r in rows}


def build_advisory(engine) -> dict[str, Any]:
    """Run all the queries and assemble the advisory dict."""
    with engine.connect() as conn:
        signal_ratios = _signal_ratios(conn)
        # Filter out per-ticker / regional features — their right/wrong
        # ratios are tautologies of postmortems-only-run-on-failures.
        # Only cross-cutting signals get meaningful ratios.
        cross_cutting = [
            s for s in signal_ratios
            if not _is_per_ticker_feature(s["signal"])
        ]
        per_ticker_excluded = [
            s for s in signal_ratios
            if _is_per_ticker_feature(s["signal"])
        ]
        # split into stars (right/wrong >= 2) and underperformers (<= 0.5)
        stars = [s for s in cross_cutting if s["right_to_wrong_ratio"] >= 2.0]
        bad = [s for s in cross_cutting if s["right_to_wrong_ratio"] <= 0.5]
        wrong_tickers = _wrong_direction_tickers(conn)
        anti = _anti_signal_override_clusters(conn)
        hypo = _hypothesis_kill_reasons(conn)
        fail_cats = _failure_categories(conn)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "version": "1.0",
        "summary": {
            "trade_failure_categories_30d": fail_cats,
            "hypothesis_kill_reasons_30d": hypo,
            "anti_signal_overrides": anti,
        },
        "star_signals_to_uplift": stars[:15],
        "bad_signals_to_downweight": bad[:15],
        "wrong_direction_ticker_clusters_30d": wrong_tickers,
        "per_ticker_features_excluded": [
            {"signal": s["signal"], "right_count": s["right_count"], "wrong_count": s["wrong_count"]}
            for s in per_ticker_excluded[:20]
        ],
    }


def write_to_obsidian(advisory: dict[str, Any]) -> Path | None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = _VAULT_SESSIONS / f"auto-improve-{today}.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(advisory, indent=2, default=str), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        log.warning("auto_improve: failed to write {p}: {e}", p=path, e=str(exc))
        return None
    return path


def write_markdown_summary(advisory: dict[str, Any]) -> bool:
    """Append a markdown summary to today's hermes session file."""
    try:
        from intelligence.obsidian_log import append_cycle_entry
    except Exception:  # noqa: BLE001
        return False

    stars = advisory["star_signals_to_uplift"][:5]
    bad = advisory["bad_signals_to_downweight"][:5]
    fail = advisory["summary"]["trade_failure_categories_30d"]
    anti = advisory["summary"]["anti_signal_overrides"]

    star_line = ", ".join(
        f"{s['signal']} ({s['right_count']}r/{s['wrong_count']}w)" for s in stars
    ) or "(none above 2.0 ratio)"
    bad_line = ", ".join(
        f"{s['signal']} ({s['right_count']}r/{s['wrong_count']}w)" for s in bad
    ) or "(none below 0.5 ratio)"
    fail_line = ", ".join(f"{k}={v:,}" for k, v in list(fail.items())[:5])

    return append_cycle_entry(
        cycle_name="auto_improve",
        summary=(
            f"30d failure_categories: {fail_line}. "
            f"Anti-signal overrides last 24h={anti['last_24h']}, "
            f"7d={anti['last_7d']}, total={anti['total_all_time']:,}."
        ),
        details={
            "STAR signals (under-weighted)": star_line,
            "NET-MISLEADING signals (downweight)": bad_line,
            "Top wrong-direction tickers (30d)": ", ".join(
                f"{t['ticker']}({t['fail_rate']:.0%})"
                for t in advisory["wrong_direction_ticker_clusters_30d"][:8]
            ) or "(none)",
        },
        tbd=[
            "Operator review: should STAR signals get conviction multipliers?",
            "Operator review: should NET-MISLEADING signals be dropped from the universe?",
        ],
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--print", action="store_true",
                    help="Echo advisory to stdout in addition to writing files")
    ap.add_argument("--no-obsidian", action="store_true",
                    help="Don't write to /data/grid_obsidian/")
    args = ap.parse_args()

    engine = get_engine()
    advisory = build_advisory(engine)

    written_path = None
    if not args.no_obsidian:
        written_path = write_to_obsidian(advisory)
        write_markdown_summary(advisory)

    if args.print or written_path is None:
        print(json.dumps(advisory, indent=2, default=str))

    if written_path:
        log.info("auto_improve: advisory written to {p}", p=written_path)
        # Also log to stdout for cron visibility
        print(f"auto_improve: advisory written to {written_path}")
        a = advisory["summary"]["anti_signal_overrides"]
        print(f"  anti-signal overrides: 24h={a['last_24h']}, 7d={a['last_7d']}, total={a['total_all_time']:,}")
        print(f"  star signals: {len(advisory['star_signals_to_uplift'])}")
        print(f"  bad signals:  {len(advisory['bad_signals_to_downweight'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
