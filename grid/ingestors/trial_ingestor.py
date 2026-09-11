#!/usr/bin/env python3
"""
grid/ingestors/trial_ingestor.py

GRID Ingestor: ClinicalTrials.gov → griddb

Scheduled two ways (either is sufficient; both are idempotent):
  * cron on grid-svr:  0 6 * * * cd ~/grid_v4/grid_repo && python -m grid.ingestors.trial_ingestor
  * Hermes registry entry ``trial_ingestor`` (scripts/hermes_operator.py, 24 h).

Logging never depends on the cron redirect (the April–September 2026 outage was
``>> /var/log/grid/trial_ingestor.log`` failing because the directory did not
exist, so Python never started). We log to stdout and, when ``GRID_LOG_DIR``
(default ``/data/grid/logs``) exists, to ``trial_ingestor.log`` inside it.

Populates:
  - trial_cache          (raw CT.gov v2 study JSON, 24h TTL)
  - catalyst_calendar    (upcoming READOUT events for RESOLVED INDUSTRY sponsors
                          only — resolution via grid.signals.sponsor_resolver)

Window cached: primary completion in [0, 400] days so the Long Plays board can
see catalysts a year out. The signal's own 30–180 d gate is unchanged.

Mirrors pattern of existing GRID ingestors (FRED, EIA, etc.)
"""

from __future__ import annotations

import os
import sys
import json
import logging
import datetime
from typing import Any, Callable, Optional

import requests
import psycopg2
import psycopg2.extras

from grid.signals.sponsor_resolver import ResolvedSponsor, resolve_sponsor

log = logging.getLogger("grid.ingestors.trial_ingestor")

CT_GOV_BASE = "https://clinicaltrials.gov/api/v2/studies"
DEFAULT_LOG_DIR = "/data/grid/logs"
LOG_FILE_NAME = "trial_ingestor.log"

DB_CONFIG = {
    "host":     os.getenv("GRID_DB_HOST", os.getenv("DB_HOST", "localhost")),
    "port":     int(os.getenv("GRID_DB_PORT", os.getenv("DB_PORT", 5432))),
    "dbname":   os.getenv("DB_NAME", "griddb"),
    "user":     os.getenv("DB_USER", "grid"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Window (days from today) for primary completion dates written to catalyst_calendar.
#
# LOOKAHEAD (2026-09-10): was 400, which silently truncated the Long Plays
# 18-month catalyst horizon (548 d) at ingestion — the board could not see a
# readout it was built to gate on. Measured that day, catalyst_calendar held
# nothing beyond 397 days out. 560 clears 18 months with a fortnight of slack.
#
# LOOKBACK (2026-09-10): was 0, so a readout was DELETED from GRID's memory the
# day after it happened. That is why trial_signals had 0 of 135 rows scored and
# why P(success) could only ever be a borrowed industry average: the event dates
# needed to measure our own hit rate were being discarded. Two years of history
# is enough to fit a phase base rate (see intelligence/catalyst_ev.py
# ``empirical_phase_outcomes`` and intelligence/trial_outcomes.py).
#
# Past events stay out of the operator's way: the ``upcoming_catalysts`` view and
# ``long_plays._load_catalysts`` both filter ``expected_date >= CURRENT_DATE``.
DAYS_LOOKAHEAD = 560
DAYS_LOOKBACK  = -730

# Anything that is not a plain ticker shape is a sponsor name written into the
# ticker column by the pre-resolver ingestor (489 of 561 rows on 2026-09-10).
TICKER_SHAPE_RE = r"^[A-Z.\-]{1,6}$"


# ── Logging ───────────────────────────────────────────────────────────────────

def configure_logging(log_dir: Optional[str] = None) -> Optional[str]:
    """Log to stdout AND to ``$GRID_LOG_DIR/trial_ingestor.log`` when that dir exists.

    Idempotent. Returns the log-file path when a file handler was attached.
    Never raises — a bad log dir must not stop the ingest.
    """
    root = logging.getLogger()
    if not any(getattr(h, "_grid_trial_stdout", False) for h in root.handlers):
        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
        stream._grid_trial_stdout = True  # type: ignore[attr-defined]
        root.addHandler(stream)
    if root.level == logging.NOTSET or root.level > logging.INFO:
        root.setLevel(logging.INFO)

    log_dir = log_dir or os.getenv("GRID_LOG_DIR", DEFAULT_LOG_DIR)
    if not log_dir or not os.path.isdir(log_dir):
        return None
    path = os.path.join(log_dir, LOG_FILE_NAME)
    if any(getattr(h, "baseFilename", None) == path for h in root.handlers):
        return path
    try:
        fh = logging.FileHandler(path)
        fh.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
        root.addHandler(fh)
        return path
    except OSError as e:
        log.warning("Could not open log file %s: %s (stdout only)", path, e)
        return None


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def get_engine() -> Any:
    """SQLAlchemy engine for the sponsor resolver cache (None when unavailable)."""
    try:
        from db import get_engine as _grid_engine

        return _grid_engine()
    except Exception as e:  # noqa: BLE001
        log.debug("db.get_engine unavailable (%s); building from DB_CONFIG", e)
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL

        url = URL.create(
            "postgresql+psycopg2",
            username=DB_CONFIG["user"], password=DB_CONFIG["password"],
            host=DB_CONFIG["host"], port=DB_CONFIG["port"], database=DB_CONFIG["dbname"],
        )
        return create_engine(url, pool_pre_ping=True)
    except Exception as e:  # noqa: BLE001
        log.warning("No SQLAlchemy engine for sponsor cache (%s); resolving without cache", e)
        return None


# ── CT.gov ────────────────────────────────────────────────────────────────────

def fetch_active_trials(page_size=1000) -> list[dict]:
    """Pull all active-not-recruiting Phase 2/3 interventional trials from ClinicalTrials.gov API v2."""
    params = {
        "filter.overallStatus": "ACTIVE_NOT_RECRUITING",
        "filter.advanced": "AREA[Phase](PHASE2 OR PHASE3) AND AREA[StudyType]INTERVENTIONAL",
        "pageSize": page_size,
    }

    all_studies = []
    next_token = None

    while True:
        if next_token:
            params["pageToken"] = next_token
        try:
            resp = requests.get(CT_GOV_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            studies = data.get("studies", [])
            all_studies.extend(studies)
            next_token = data.get("nextPageToken")
            log.info(f"Fetched {len(studies)} studies (total: {len(all_studies)})")
            if not next_token or len(studies) < page_size:
                break
        except Exception as e:
            log.warning(f"CT.gov API error: {e}")
            break

    return all_studies


def parse_date(date_str: str) -> Optional[datetime.date]:
    for fmt in ("%Y-%m-%d", "%B %Y", "%Y-%m", "%Y"):
        try:
            d = datetime.datetime.strptime(date_str, fmt).date()
            if fmt in ("%B %Y", "%Y-%m", "%Y"):
                d = d.replace(day=1)
            return d
        except (ValueError, TypeError):
            continue
    return None


def upsert_trial_cache(conn, studies: list[dict]) -> int:
    """Cache raw trial JSON with 24h TTL."""
    cur = conn.cursor()
    count = 0
    for s in studies:
        nct_id = (
            s.get("protocolSection", {})
             .get("identificationModule", {})
             .get("nctId")
        )
        if not nct_id:
            continue
        try:
            cur.execute("""
                INSERT INTO trial_cache (nct_id, raw_json, parsed_at, expires_at)
                VALUES (%s, %s, NOW(), NOW() + INTERVAL '24 hours')
                ON CONFLICT (nct_id) DO UPDATE
                SET raw_json   = EXCLUDED.raw_json,
                    parsed_at  = NOW(),
                    expires_at = NOW() + INTERVAL '24 hours'
            """, (nct_id, json.dumps(s)))
            count += 1
        except Exception as e:
            log.warning(f"Cache insert failed for {nct_id}: {e}")
            conn.rollback()
    conn.commit()
    cur.close()
    log.info(f"Upserted {count} trials into trial_cache")
    return count


def extract_catalyst_events(studies: list[dict]) -> list[dict]:
    """Extract upcoming readout events (primary completion in [DAYS_LOOKBACK, DAYS_LOOKAHEAD])."""
    events = []
    today = datetime.date.today()

    for s in studies:
        proto = s.get("protocolSection", {})
        ident = proto.get("identificationModule", {})
        status = proto.get("statusModule", {})
        sponsor = proto.get("sponsorCollaboratorsModule", {})
        lead = sponsor.get("leadSponsor", {}) or {}

        nct_id = ident.get("nctId")
        if not nct_id:
            continue

        pc_str = status.get("primaryCompletionDateStruct", {}).get("date")
        pc_date = parse_date(pc_str) if pc_str else None

        if not pc_date:
            continue

        days_out = (pc_date - today).days
        if not (DAYS_LOOKBACK <= days_out <= DAYS_LOOKAHEAD):
            continue

        events.append({
            "nct_id":               nct_id,
            "sponsor":              lead.get("name", ""),
            "sponsor_class":        lead.get("class"),
            "expected_date":        pc_date,
            "event_type":           "READOUT",
            "confidence_window":    30,
            "source":               "clinicaltrials.gov",
            "notes":                ident.get("briefTitle", "")[:200],
        })

    return events


def count_industry(events: list[dict]) -> int:
    """Events whose lead sponsor class is INDUSTRY."""
    return sum(1 for ev in events if str(ev.get("sponsor_class") or "").upper() == "INDUSTRY")


# One statement per event: refresh an existing (nct_id, ticker, event_type)
# row's date/notes, else insert. catalyst_calendar has no unique constraint,
# so the old ``ON CONFLICT DO NOTHING`` never fired and re-runs stacked
# duplicates.
_UPSERT_CALENDAR_SQL = """
    WITH upd AS (
        UPDATE catalyst_calendar
           SET expected_date = %(expected_date)s,
               confidence_window_days = %(confidence_window)s,
               notes = %(notes)s,
               is_active = TRUE
         WHERE nct_id = %(nct_id)s AND ticker = %(ticker)s AND event_type = %(event_type)s
        RETURNING id
    )
    INSERT INTO catalyst_calendar
        (ticker, nct_id, event_type, expected_date,
         confidence_window_days, source, notes, is_active)
    SELECT %(ticker)s, %(nct_id)s, %(event_type)s, %(expected_date)s,
           %(confidence_window)s, %(source)s, %(notes)s, TRUE
    WHERE NOT EXISTS (SELECT 1 FROM upd)
"""


def upsert_catalyst_calendar(
    conn,
    events: list[dict],
    engine: Any = None,
    resolver: Optional[Callable[[Any, str, Optional[str]], ResolvedSponsor]] = None,
) -> tuple[int, int]:
    """Populate catalyst_calendar with readout events for RESOLVED INDUSTRY sponsors.

    Resolution goes through ``grid.signals.sponsor_resolver.resolve_sponsor``
    (class + name hard-reject, SEC, GRID name maps, cache, local LLM). Rows
    for unresolved or non-industry sponsors are never written.

    Returns ``(resolved_events, calendar_rows_written)``.
    """
    resolve = resolver or resolve_sponsor
    cur = conn.cursor()
    resolved_n = 0
    rows = 0
    memo: dict[str, ResolvedSponsor] = {}
    for ev in events:
        sponsor = ev.get("sponsor") or ""
        if sponsor not in memo:
            try:
                memo[sponsor] = resolve(engine, sponsor, ev.get("sponsor_class"))
            except Exception as e:  # noqa: BLE001
                log.warning("Sponsor resolution failed for %r: %s", sponsor, e)
                memo[sponsor] = ResolvedSponsor(None, "error", 0.0, "resolver_error")
        res = memo[sponsor]
        ticker = res.ticker
        if not ticker:
            log.debug(
                "Skipping catalyst_calendar row for %r (%s): %s",
                sponsor, ev["nct_id"], res.reason,
            )
            continue
        resolved_n += 1
        try:
            cur.execute(_UPSERT_CALENDAR_SQL, {
                "ticker": ticker,
                "nct_id": ev["nct_id"],
                "event_type": ev["event_type"],
                "expected_date": ev["expected_date"],
                "confidence_window": ev["confidence_window"],
                "source": ev["source"],
                "notes": ev["notes"],
            })
            rows += 1
        except Exception as e:
            log.warning(f"Catalyst insert failed: {e}")
            conn.rollback()
    conn.commit()
    cur.close()
    log.info(f"Upserted {rows} events into catalyst_calendar ({resolved_n} resolved sponsors)")
    return resolved_n, rows


def deactivate_name_tickers(conn) -> int:
    """Idempotent cleanup: mark legacy rows whose ticker is a sponsor name inactive.

    ``UPDATE catalyst_calendar SET is_active = FALSE WHERE ticker !~ '^[A-Z.\\-]{1,6}$'``
    """
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE catalyst_calendar SET is_active = FALSE "
            "WHERE is_active = TRUE AND ticker !~ %s",
            (TICKER_SHAPE_RE,),
        )
        n = cur.rowcount if isinstance(getattr(cur, "rowcount", None), int) and cur.rowcount >= 0 else 0
        conn.commit()
        cur.close()
        if n:
            log.info(f"Deactivated {n} catalyst_calendar rows with sponsor names in the ticker column")
        return n
    except Exception as e:
        log.warning(f"catalyst_calendar cleanup failed: {e}")
        conn.rollback()
        return 0


def purge_expired_cache(conn) -> int:
    """Remove stale cache entries."""
    cur = conn.cursor()
    cur.execute("DELETE FROM trial_cache WHERE expires_at < NOW()")
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    log.info(f"Purged {deleted} expired cache entries")
    return deleted


def log_ingestor_run(conn, studies_fetched: int, cached: int, catalysts: int):
    """Write ingestor run stats to GRID's standard ingestion_log if it exists."""
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ingestion_log
                (source, records_fetched, records_stored, run_at)
            VALUES ('clinicaltrials.gov', %s, %s, NOW())
        """, (studies_fetched, cached))
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()  # ingestion_log may not exist in all GRID versions


def sync_to_actor_network(conn):
    """Bridge trial sponsors into the GRID actor network."""
    try:
        from intelligence.actors.trial_bridge import sync_trial_sponsors_to_actors
        result = sync_trial_sponsors_to_actors(conn)
        log.info(
            f"Actor bridge: {result['actors_upserted']} actors, "
            f"{result['connections']} connections, "
            f"{result['wealth_flows']} wealth flows"
        )
    except ImportError:
        log.debug("trial_bridge not available, skipping actor sync")
    except Exception as e:
        log.warning(f"Actor bridge failed (non-fatal): {e}")


def run(engine: Any = None) -> dict:
    """Full ingest. Returns the summary counts (also logged as one line)."""
    log.info("GRID Trial Ingestor starting")
    conn = get_conn()
    if engine is None:
        engine = get_engine()

    # 1. Fetch from CT.gov
    studies = fetch_active_trials()
    log.info(f"Total trials fetched: {len(studies)}")

    # 2. Cache raw JSON
    cached = upsert_trial_cache(conn, studies) if studies else 0

    # 3. Legacy cleanup (idempotent) — sponsor names in the ticker column
    deactivated = deactivate_name_tickers(conn)

    # 4. Extract and store catalyst events for resolved industry sponsors
    events = extract_catalyst_events(studies)
    industry = count_industry(events)
    resolved, calendar_rows = upsert_catalyst_calendar(conn, events, engine=engine)

    # 5. Purge stale cache
    purge_expired_cache(conn)

    # 6. Sync sponsors → actor network
    sync_to_actor_network(conn)

    # 7. Log run
    log_ingestor_run(conn, len(studies), cached, calendar_rows)

    conn.close()
    summary = {
        "status": "SUCCESS" if studies else "EMPTY",
        "fetched": len(studies),
        "cached": cached,
        "events_in_window": len(events),
        "industry": industry,
        "resolved": resolved,
        "calendar_rows": calendar_rows,
        "deactivated_name_rows": deactivated,
    }
    log.info(
        "trial_ingestor: cached=%d industry=%d resolved=%d calendar_rows=%d",
        cached, industry, resolved, calendar_rows,
    )
    return summary


def main(engine: Any = None) -> dict:
    """CLI / Hermes entry point: configure logging, then :func:`run`."""
    configure_logging()
    return run(engine)


if __name__ == "__main__":
    main()
