#!/usr/bin/env python3
"""Backfill ``news_articles.tickers`` with real symbols.

The current ``tickers`` column contains 12-char hash suffixes left over
from the tiingo raw_series migration. This script runs
``intelligence.news_ticker_resolver.resolve_tickers`` across every row
and updates the column in-place. Rows whose resolved list is empty are
left with ``ARRAY[]::text[]`` rather than keeping the broken hashes.

Usage:
    python3 scripts/backfill_news_tickers.py              # run
    python3 scripts/backfill_news_tickers.py --dry-run    # no writes
    python3 scripts/backfill_news_tickers.py --limit 100  # sample test

Strategy:
    1. Select all ``news_articles`` rows in batches of ``_BATCH``.
    2. For each row, try to join to ``raw_series.raw_payload`` via
       ``tiingo_news.<hash>`` to pull the Tiingo-supplied ticker list
       (some Tiingo articles have real tickers — they just got
       clobbered by the hash-id bug).
    3. Call ``resolve_tickers(title, summary, fallback)`` to produce
       the final uppercase list.
    4. ``UPDATE news_articles SET tickers = :new WHERE id = :id`` in
       one transaction per batch.
    5. Print running totals every batch.

Safe to re-run — the resolver is deterministic so running twice
produces the same output.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure we can import grid modules even when run as a script
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loguru import logger as log  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402

from intelligence.news_ticker_resolver import resolve_tickers  # noqa: E402

_BATCH: int = 2000


def _get_engine():
    """Return a DB engine using the project config."""
    try:
        from config import settings
        return create_engine(settings.DB_URL)
    except Exception as exc:
        log.error("Failed to load config: {e}", e=str(exc))
        raise


def _fetch_batch(conn, last_id: int, limit: int) -> list[tuple]:
    """Fetch the next batch of news_articles rows.

    Returns: list of (id, title, summary, current_tickers, payload_tickers).

    Strategy: first fetch the plain rows (fast), then for rows where
    the tickers column looks like a 12-char hash, do a cheap
    LATERAL lookup against ``raw_series`` by the exact
    ``tiingo_news.<hash>`` series_id. The two-step split is ~50x
    faster than a blanket JOIN against the 11M-row raw_series table.
    """
    q = text(
        "SELECT na.id, na.title, na.summary, na.tickers "
        "FROM news_articles na "
        "WHERE na.id > :last_id "
        "ORDER BY na.id "
        "LIMIT :lim"
    )
    base_rows = conn.execute(q, {"last_id": last_id, "lim": limit}).fetchall()
    if not base_rows:
        return []

    # Collect hash suffixes that need a raw_payload lookup
    hash_by_id: dict[int, str] = {}
    for rid, _t, _s, tks in base_rows:
        if tks and len(tks) == 1 and isinstance(tks[0], str):
            val = tks[0]
            if len(val) == 12 and all(c in "0123456789abcdef" for c in val):
                hash_by_id[rid] = val

    # Batch fetch raw_payloads for the hash suffixes
    payloads_by_id: dict[int, object] = {}
    if hash_by_id:
        series_ids = [f"tiingo_news.{h}" for h in hash_by_id.values()]
        payload_q = text(
            "SELECT series_id, raw_payload "
            "FROM raw_series "
            "WHERE series_id = ANY(:sids) "
            "LIMIT :lim"
        )
        payload_rows = conn.execute(
            payload_q,
            {"sids": series_ids, "lim": len(series_ids) * 5},
        ).fetchall()
        series_to_payload: dict[str, object] = {}
        for sid, payload in payload_rows:
            # Keep first seen; duplicates from multiple pulls are fine
            series_to_payload.setdefault(sid, payload)
        for rid, h in hash_by_id.items():
            p = series_to_payload.get(f"tiingo_news.{h}")
            if p is not None:
                payloads_by_id[rid] = p

    # Zip back into the (id, title, summary, current_tickers, payload) tuple
    out: list[tuple] = []
    for rid, title, summary, tks in base_rows:
        out.append((rid, title, summary, tks, payloads_by_id.get(rid)))
    return out


def _extract_payload_tickers(payload) -> list[str]:
    """Pull the tickers array out of a raw_payload JSONB value."""
    if payload is None:
        return []
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return []
    if not isinstance(payload, dict):
        return []
    raw = payload.get("tickers")
    if not isinstance(raw, list):
        return []
    return [str(t) for t in raw if t and isinstance(t, (str, int))]


def backfill(dry_run: bool = False, limit: int | None = None) -> dict:
    """Run the backfill.

    Parameters:
        dry_run: If True, compute updates but do not write.
        limit: Stop after processing this many rows (None = all).

    Returns:
        Summary dict with counts and samples.
    """
    engine = _get_engine()
    totals = {
        "processed": 0,
        "updated": 0,
        "cleared": 0,
        "unchanged": 0,
        "resolved_tickers_total": 0,
        "samples": [],
    }
    sample_cap = 20

    last_id = 0
    while True:
        batch_limit = _BATCH
        if limit is not None:
            remaining = limit - totals["processed"]
            if remaining <= 0:
                break
            batch_limit = min(_BATCH, remaining)

        with engine.connect() as conn:
            rows = _fetch_batch(conn, last_id, batch_limit)
        if not rows:
            break

        updates: list[dict] = []
        for rid, title, summary, current_tickers, payload in rows:
            totals["processed"] += 1
            last_id = rid
            payload_tickers = _extract_payload_tickers(payload)
            resolved = resolve_tickers(title, summary, payload_tickers)

            # Decide whether to write
            cur = list(current_tickers or [])
            if resolved == cur:
                totals["unchanged"] += 1
                continue

            updates.append({"id": rid, "new": resolved})
            if resolved:
                totals["updated"] += 1
                totals["resolved_tickers_total"] += len(resolved)
            else:
                totals["cleared"] += 1

            if len(totals["samples"]) < sample_cap and resolved:
                totals["samples"].append({
                    "id": rid,
                    "title": (title or "")[:80],
                    "old": cur,
                    "new": resolved,
                })

        # Write updates
        if updates and not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE news_articles SET tickers = :new WHERE id = :id"),
                    updates,
                )

        log.info(
            "batch done — processed={p:,d} updated={u:,d} cleared={c:,d} unchanged={n:,d}",
            p=totals["processed"],
            u=totals["updated"],
            c=totals["cleared"],
            n=totals["unchanged"],
        )

        if len(rows) < batch_limit:
            break  # final partial batch

    return totals


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill news_articles tickers")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute but do not write")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N rows (for testing)")
    args = parser.parse_args()

    log.info("Starting news_articles ticker backfill (dry_run={d}, limit={l})",
             d=args.dry_run, l=args.limit)
    result = backfill(dry_run=args.dry_run, limit=args.limit)

    log.info("=" * 60)
    log.info("BACKFILL COMPLETE")
    log.info("  processed           : {n:,d}", n=result["processed"])
    log.info("  updated (with tks)  : {n:,d}", n=result["updated"])
    log.info("  cleared (no match)  : {n:,d}", n=result["cleared"])
    log.info("  unchanged           : {n:,d}", n=result["unchanged"])
    log.info("  total resolved tks  : {n:,d}", n=result["resolved_tickers_total"])
    log.info("=" * 60)
    log.info("Sample resolutions:")
    for s in result["samples"][:10]:
        log.info("  [{i}] {t}", i=s["id"], t=s["title"])
        log.info("       old={o} → new={n}", o=s["old"], n=s["new"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
