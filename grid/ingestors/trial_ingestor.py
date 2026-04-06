#!/usr/bin/env python3
"""
grid/ingestors/trial_ingestor.py

GRID Ingestor: ClinicalTrials.gov → griddb

Scheduled via cron (add to grid-svr crontab):
  0 6 * * * cd ~/grid_v4/grid_repo && python -m grid.ingestors.trial_ingestor

Populates:
  - trial_cache          (raw CT.gov data, 24h TTL)
  - catalyst_calendar    (upcoming readout events)

Mirrors pattern of existing GRID ingestors (FRED, EIA, etc.)
"""

import os
import json
import logging
import datetime
import requests
import psycopg2
import psycopg2.extras

log = logging.getLogger("grid.ingestors.trial_ingestor")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)

CT_GOV_BASE = "https://clinicaltrials.gov/api/v2/studies"

DB_CONFIG = {
    "host":     os.getenv("GRID_DB_HOST", os.getenv("DB_HOST", "localhost")),
    "port":     int(os.getenv("GRID_DB_PORT", os.getenv("DB_PORT", 5432))),
    "dbname":   os.getenv("DB_NAME", "griddb"),
    "user":     os.getenv("DB_USER", "grid"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Lookback window for primary completion dates
DAYS_LOOKAHEAD = 180
DAYS_LOOKBACK  = 30


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


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
            log.error(f"CT.gov API error: {e}")
            break

    return all_studies


def parse_date(date_str: str) -> datetime.date:
    for fmt in ("%Y-%m-%d", "%B %Y", "%Y"):
        try:
            d = datetime.datetime.strptime(date_str, fmt).date()
            if fmt in ("%B %Y", "%Y"):
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
    """Extract upcoming readout events for catalyst_calendar."""
    events = []
    today = datetime.date.today()

    for s in studies:
        proto = s.get("protocolSection", {})
        ident = proto.get("identificationModule", {})
        status = proto.get("statusModule", {})
        sponsor = proto.get("sponsorCollaboratorsModule", {})

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
            "sponsor":              sponsor.get("leadSponsor", {}).get("name", ""),
            "expected_date":        pc_date,
            "event_type":           "READOUT",
            "confidence_window":    30,
            "source":               "clinicaltrials.gov",
            "notes":                ident.get("briefTitle", "")[:200],
        })

    return events


def upsert_catalyst_calendar(conn, events: list[dict]) -> int:
    """Populate catalyst_calendar with upcoming readout events."""
    cur = conn.cursor()
    count = 0
    for ev in events:
        try:
            cur.execute("""
                INSERT INTO catalyst_calendar
                    (ticker, nct_id, event_type, expected_date,
                     confidence_window_days, source, notes, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT DO NOTHING
            """, (
                ev["sponsor"][:10],  # placeholder until ticker resolution
                ev["nct_id"],
                ev["event_type"],
                ev["expected_date"],
                ev["confidence_window"],
                ev["source"],
                ev["notes"],
            ))
            count += 1
        except Exception as e:
            log.warning(f"Catalyst insert failed: {e}")
            conn.rollback()
    conn.commit()
    cur.close()
    log.info(f"Upserted {count} events into catalyst_calendar")
    return count


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
        pass  # ingestion_log may not exist in all GRID versions


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


def main():
    log.info("GRID Trial Ingestor starting")
    conn = get_conn()

    # 1. Fetch from CT.gov
    studies = fetch_active_trials()
    log.info(f"Total trials fetched: {len(studies)}")

    # 2. Cache raw JSON
    cached = upsert_trial_cache(conn, studies)

    # 3. Extract and store catalyst events
    events = extract_catalyst_events(studies)
    catalysts = upsert_catalyst_calendar(conn, events)

    # 4. Purge stale cache
    purge_expired_cache(conn)

    # 5. Sync sponsors → actor network
    sync_to_actor_network(conn)

    # 6. Log run
    log_ingestor_run(conn, len(studies), cached, catalysts)

    conn.close()
    log.info(
        f"Trial ingestor complete: "
        f"{len(studies)} fetched, {cached} cached, {catalysts} catalysts"
    )


if __name__ == "__main__":
    main()
