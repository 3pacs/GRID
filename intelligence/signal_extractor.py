"""Signal Extractor — bridges raw_series → signal_data with actor attribution.

The pullers dump data into raw_series with series_id patterns like:
    CONGRESS:senate:nancy_pelosi:NVDA:BUY
    FORM4:insider:john_smith:AAPL:purchase
    FARA:saudi_arabia:glover_park_group

But the intelligence layer reads from signal_data, which expects:
    signal_type, ticker, actor, direction, magnitude, data

This module scans raw_series for structured entries and creates proper
signal_data records with real actor names, enabling the backlinker to
wire them into the actor graph.

Also scans signal_sources table (used by congressional puller).

Runs as a daemon alongside the backlinker.
"""

from __future__ import annotations

import json
import sys
import time

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Series ID patterns → actor extraction ──

EXTRACTORS = [
    {
        "pattern": "CONGRESS:%",
        "signal_type": "congressional",
        "parse": lambda sid: {
            # CONGRESS:SENATE:shelley_moore_capito:FDS:SALE (FULL)
            "parts": sid.split(":"),
            "actor": sid.split(":")[2].replace("_", " ").title() if len(sid.split(":")) > 2 else None,
            "ticker": sid.split(":")[3] if len(sid.split(":")) > 3 else None,
            "direction": "BEAR" if "SALE" in sid.upper() else "BULL",
        },
    },
    {
        # WHALE:AAPL:110:2026-04-17:CALL → anonymous whale, but link ticker+direction
        "pattern": "WHALE:%",
        "signal_type": "whale_options",
        "parse": lambda sid: {
            "parts": sid.split(":"),
            "actor": None,  # Anonymous whale
            "ticker": sid.split(":")[1] if len(sid.split(":")) > 1 else None,
            "direction": "BULL" if "CALL" in sid.upper() else "BEAR",
        },
    },
    {
        # gdelt_actor_powell_tone → actor=Powell
        "pattern": "gdelt_actor_%",
        "signal_type": "geopolitical_tone",
        "parse": lambda sid: {
            "parts": sid.split("_"),
            # gdelt_actor_powell_tone → "Powell"
            "actor": " ".join(sid.replace("gdelt_actor_", "").replace("_tone", "").split("_")).title(),
            "ticker": "MACRO",
            "direction": "NEUTRAL",  # default; downstream may rewrite from tone value
            "signal_subtype": "geopolitical_tone",
        },
    },
    {
        # gdelt_tension_russia_ukraine → geopolitical tension pair
        "pattern": "gdelt_tension_%",
        "signal_type": "geopolitical_tension",
        "parse": lambda sid: {
            "parts": sid.split("_"),
            # gdelt_tension_us_russia → actor="Us Russia" (country pair)
            "actor": " ".join(sid.replace("gdelt_tension_", "").split("_")).upper(),
            "ticker": "MACRO",
            "direction": None,
            "signal_subtype": "geopolitical_tension",
        },
    },
    {
        "pattern": "FORM4:%",
        "signal_type": "insider",
        "parse": lambda sid: {
            "parts": sid.split(":"),
            "actor": sid.split(":")[2].replace("_", " ").title() if len(sid.split(":")) > 2 else None,
            "ticker": sid.split(":")[1] if len(sid.split(":")) > 1 else None,
            "direction": "BULL" if "purchase" in sid.lower() or "buy" in sid.lower() else "BEAR",
        },
    },
    {
        # INSIDER:ACAD:kihara_james:SELL → actor=Kihara James, ticker=ACAD
        "pattern": "INSIDER:%",
        "signal_type": "insider",
        "parse": lambda sid: {
            "parts": sid.split(":"),
            "actor": sid.split(":")[2].replace("_", " ").title() if len(sid.split(":")) > 2 else None,
            "ticker": sid.split(":")[1] if len(sid.split(":")) > 1 else None,
            "direction": "BULL" if any(w in sid.upper() for w in ("BUY", "PURCHASE", "GRANT")) else "BEAR",
        },
    },
    {
        "pattern": "FARA:%",
        "signal_type": "foreign_lobbying",
        "parse": lambda sid: {
            "parts": sid.split(":"),
            "actor": sid.split(":")[2].replace("_", " ").title() if len(sid.split(":")) > 2 else None,
            "ticker": sid.split(":")[1] if len(sid.split(":")) > 1 else None,
            "direction": None,
            "signal_subtype": "foreign_lobbying",
        },
    },
    {
        "pattern": "DARKPOOL:%",
        "signal_type": "darkpool",
        "parse": lambda sid: {
            "parts": sid.split(":"),
            "actor": None,  # Dark pool = anonymous
            "ticker": sid.split(":")[1] if len(sid.split(":")) > 1 else None,
            "direction": "BULL" if "buy" in sid.lower() else "BEAR",
        },
    },
    {
        "pattern": "LOBBYING:%",
        "signal_type": "lobbying",
        "parse": lambda sid: {
            "parts": sid.split(":"),
            "actor": sid.split(":")[1].replace("_", " ").title() if len(sid.split(":")) > 1 else None,
            "ticker": sid.split(":")[2] if len(sid.split(":")) > 2 else None,
            "direction": None,
            "signal_subtype": "lobbying",
        },
    },
    {
        "pattern": "CAMPAIGN:%",
        "signal_type": "campaign_finance",
        "parse": lambda sid: {
            "parts": sid.split(":"),
            "actor": sid.split(":")[1].replace("_", " ").title() if len(sid.split(":")) > 1 else None,
            "ticker": sid.split(":")[2] if len(sid.split(":")) > 2 else None,
            "direction": None,
            "signal_subtype": "campaign_donation",
        },
    },
]


def extract_from_raw_series(engine: Engine, batch_size: int = 5000) -> dict[str, int]:
    """Scan raw_series for structured entries and create signal_data records."""
    stats = {"scanned": 0, "extracted": 0, "skipped": 0, "errors": 0}

    with engine.connect() as conn:
        for extractor in EXTRACTORS:
            pattern = extractor["pattern"]
            sig_type = extractor["signal_type"]
            parse_fn = extractor["parse"]

            # Find raw_series entries not yet in signal_data
            rows = conn.execute(text("""
                SELECT rs.series_id, rs.obs_date, rs.value
                FROM raw_series rs
                WHERE rs.series_id LIKE :pattern
                  AND NOT EXISTS (
                      SELECT 1 FROM signal_data sd
                      WHERE sd.source_id = rs.series_id
                        AND sd.signal_date = rs.obs_date
                  )
                ORDER BY rs.obs_date DESC
                LIMIT :lim
            """), {"pattern": pattern, "lim": batch_size}).fetchall()

            stats["scanned"] += len(rows)

            for series_id, obs_date, value in rows:
                try:
                    parsed = parse_fn(series_id)
                    actor = parsed.get("actor")
                    ticker = parsed.get("ticker")
                    direction = parsed.get("direction")  # None means non-directional
                    subtype = parsed.get("signal_subtype")
                    # Direction must be canonical {BULL,BEAR,NEUTRAL,None}; anything
                    # else is dropped to None to avoid polluting signal_data.direction.
                    if direction not in ("BULL", "BEAR", "NEUTRAL", None):
                        direction = None

                    if not ticker:
                        stats["skipped"] += 1
                        continue

                    conn.execute(text("""
                        INSERT INTO signal_data
                            (signal_type, signal_date, ticker, actor, direction,
                             signal_subtype, magnitude, confidence, source_id, data, created_at)
                        VALUES (:stype, :sdate, :ticker, :actor, :dir,
                                :sub, :mag, :conf, :src, :data, NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "stype": sig_type,
                        "sdate": obs_date,
                        "ticker": ticker,
                        "actor": actor,
                        "dir": direction,
                        "sub": subtype,
                        "mag": float(value) if value else 0.0,
                        "conf": "confirmed",
                        "src": series_id,
                        "data": json.dumps({"series_id": series_id, "raw_value": float(value) if value else 0}),
                    })
                    stats["extracted"] += 1

                except Exception as exc:
                    stats["errors"] += 1
                    log.debug("Extract error for {s}: {e}", s=series_id, e=str(exc))

        conn.commit()

    log.info(
        "Extractor (raw_series): scanned={s} extracted={e} skipped={sk} errors={er}",
        s=stats["scanned"], e=stats["extracted"], sk=stats["skipped"], er=stats["errors"],
    )
    return stats


def extract_from_signal_sources(engine: Engine, batch_size: int = 5000) -> dict[str, int]:
    """Scan signal_sources table and create signal_data records with real actor names."""
    stats = {"scanned": 0, "extracted": 0, "errors": 0}

    with engine.connect() as conn:
        # Check if signal_sources exists
        try:
            conn.execute(text("SELECT 1 FROM signal_sources LIMIT 1"))
        except Exception:
            log.debug("signal_sources table does not exist, skipping")
            return stats

        rows = conn.execute(text("""
            SELECT ss.source_type, ss.source_id, ss.ticker, ss.signal_date,
                   ss.signal_type, ss.signal_value
            FROM signal_sources ss
            WHERE NOT EXISTS (
                SELECT 1 FROM signal_data sd
                WHERE sd.source_id = CONCAT(ss.source_type, ':', ss.source_id, ':', ss.ticker)
                  AND sd.signal_date = ss.signal_date
            )
            ORDER BY ss.signal_date DESC
            LIMIT :lim
        """), {"lim": batch_size}).fetchall()

        stats["scanned"] = len(rows)

        for source_type, source_id, ticker, signal_date, signal_type, signal_value in rows:
            try:
                # source_id in signal_sources IS the actor name (e.g., "Nancy Pelosi")
                actor = source_id
                # signal_type.lower() is a categorical leak, not a direction.
                # Stash it as subtype; leave direction NULL since we cannot
                # derive bull/bear from the source_type alone.
                subtype = signal_type.lower() if signal_type else None
                direction = None

                # Parse signal_value JSON for extra context
                extra = {}
                if signal_value:
                    try:
                        extra = json.loads(signal_value) if isinstance(signal_value, str) else signal_value
                    except (json.JSONDecodeError, TypeError):
                        extra = {"raw": str(signal_value)}

                magnitude = extra.get("amount_midpoint", 0.0) if isinstance(extra, dict) else 0.0

                composite_src = f"{source_type}:{source_id}:{ticker}"

                conn.execute(text("""
                    INSERT INTO signal_data
                        (signal_type, signal_date, ticker, actor, direction,
                         signal_subtype, magnitude, confidence, source_id, data, created_at)
                    VALUES (:stype, :sdate, :ticker, :actor, :dir,
                            :sub, :mag, :conf, :src, :data, NOW())
                    ON CONFLICT DO NOTHING
                """), {
                    "stype": source_type,
                    "sdate": signal_date,
                    "ticker": ticker,
                    "actor": actor,
                    "dir": direction,
                    "sub": subtype,
                    "mag": float(magnitude) if magnitude else 0.0,
                    "conf": "confirmed",
                    "src": composite_src,
                    "data": json.dumps(extra) if isinstance(extra, dict) else "{}",
                })
                stats["extracted"] += 1

            except Exception as exc:
                stats["errors"] += 1
                log.debug("Extract error for {t}/{a}: {e}", t=ticker, a=source_id, e=str(exc))

        conn.commit()

    log.info(
        "Extractor (signal_sources): scanned={s} extracted={e} errors={er}",
        s=stats["scanned"], e=stats["extracted"], er=stats["errors"],
    )
    return stats


def run_extractor(interval: int = 300) -> None:
    """Main loop — extract signals every `interval` seconds."""
    sys.path.insert(0, ".")
    from db import get_engine

    engine = get_engine()
    log.info("Signal extractor starting — interval={i}s", i=interval)

    # Initial full sweep
    log.info("Extractor: initial full sweep")
    extract_from_raw_series(engine, batch_size=50000)
    extract_from_signal_sources(engine, batch_size=50000)

    while True:
        try:
            extract_from_raw_series(engine)
            extract_from_signal_sources(engine)
        except Exception as exc:
            log.error("Extractor cycle failed: {e}", e=str(exc))

        time.sleep(interval)


if __name__ == "__main__":
    log.remove()
    log.add(sys.stderr, level="INFO")
    run_extractor()
