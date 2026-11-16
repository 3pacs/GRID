"""GDELT 2.0 bulk export files -> gdelt_events, gdelt_daily_summary and the 12 daily event features.

Why
---
The DOC API refuses GRID (``ingestion/altdata/gdelt_doc.py``: 429s from every
IP tried, zero successes in 96 h of logs on 2026-11-16), while GDELT's bulk
files answer: ``gdeltv2/lastupdate.txt`` names a new export every 15 minutes.
``gdelt_events`` and ``gdelt_daily_summary`` stopped on 2026-03-19 because
``scripts/parse_gdelt.py`` was a one-off backfill of GDELT 1.0 daily files.

What a day is
-------------
GDELT stamps each export file with the end of its 15-minute window
(``YYYYMMDDHHMMSS``, UTC); every event in it carries that stamp as DATEADDED.
Day D is the 96 files stamped D 00:00 .. D 23:45. Measured 2026-11-16 for
2026-11-15: those 96 files hold exactly the 61,540 events of GDELT 1.0's daily
file for D (same GLOBALEVENTIDs, none twice), so this continues the event set
the legacy rows were loaded from. The legacy summary grouped by SQLDATE
instead, which leaves out the ~2 % of a day's events that report an earlier
date and adds them to old days; ``day_basis = 'dateadded_v2'`` marks the new
grouping.

When a day is known (PIT)
-------------------------
A file's publication instant is its HTTP ``Last-Modified`` header (measured:
a median of 11 minutes *before* the stamp, but one file of 2026-11-15 went up
98 minutes after it), else the instant GRID fetched it, which is never
earlier. A day is known when its last file is published. ``store.pit`` reads
a date as "known by the end of that UTC day", and GRID's rule for this source
(Anik, 2026-11-16) is that ``release_date`` never equals ``obs_date``: a day's
summary and features carry ``release_date = vintage_date =`` the later of D+1
and the UTC date of the latest publication instant among its files. The 23:45
file usually goes up near 23:35 UTC on D, so that is D+1; a file published two
days late moves the day to D+2. GDELT 1.0's daily file for D, the history
source, is published about 07:00 UTC on D+1, so both are dated alike.

Idempotency and resume
----------------------
``gdelt_export_files`` records every file as loaded (with its aggregates) or
missing; a recorded file is never fetched again, events insert ``ON CONFLICT
DO NOTHING``, and a file's events, aggregates and ledger row commit together.
A 404 counts as missing only once the slot is ``MISSING_GRACE`` behind the
newest stamp in ``lastupdate.txt``; until then the day waits. A day is
finalised (summary row plus resolved_series points, one transaction) once all
96 slots are recorded. Runs resume from the earliest unfinalised day after the
legacy table's last day, under a Postgres advisory lock, within a time budget.
Days that already have a summary row (the legacy ones) are never touched.

Run ``python -m ingestion.altdata.gdelt_bulk`` for a dry run and ``--write``
to load; Hermes' daily-intel task ``gdelt_bulk_events`` calls ``run_daily``.
"""

from __future__ import annotations

import csv
import email.utils
import hashlib
import io
import json
import time
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine

BASE_URL = "https://data.gdeltproject.org/gdeltv2/"
LASTUPDATE_URL = BASE_URL + "lastupdate.txt"
USER_AGENT = "GRID-gdelt-bulk/1.0"
HTTP_TIMEOUT_S = 60
SLOT = timedelta(minutes=15)
SLOTS_PER_DAY = 96
#: A 404 this far behind lastupdate's newest stamp is final; nearer, the day waits.
MISSING_GRACE = timedelta(hours=24)
#: Hermes gives the task DAILY_INTEL_GDELT_BULK_BUDGET_S (120 s); stop between files.
DAILY_BUDGET_SECONDS = 100
DAY_BASIS = "dateadded_v2"
SOURCE_NAME = "GDELT_BULK"
#: pg advisory lock key: first 8 bytes of sha256("grid:gdelt_bulk"), signed.
LOCK_KEY = int.from_bytes(hashlib.sha256(b"grid:gdelt_bulk").digest()[:8], "big", signed=True)

# GDELT 2.0 export: 61 tab-separated columns, no header.
EXPORT_COLUMNS = 61
COL_GLOBALEVENTID = 0
COL_SQLDATE = 1
COL_ACTOR1COUNTRYCODE = 7
COL_ACTOR2COUNTRYCODE = 17
COL_EVENTROOTCODE = 28
COL_QUADCLASS = 29
COL_GOLDSTEINSCALE = 30
COL_NUMMENTIONS = 31
COL_NUMSOURCES = 32
COL_NUMARTICLES = 33
COL_AVGTONE = 34
COL_DATEADDED = 59
#: GDELT 1.0 daily export (history): 58 columns, DATEADDED (YYYYMMDD) in col 56.
V1_EXPORT_COLUMNS = 58
V1_COL_DATEADDED = 56

#: Feature suffix -> CAMEO actor country code. Actor*CountryCode holds CAMEO
#: three-letter codes ("USA"); scripts/parse_gdelt.py compared them with
#: "US", "CN", ... and so wrote 0 for every gdelt_events_* point it stored.
TRACKED_COUNTRIES: dict[str, str] = {
    "us": "USA", "cn": "CHN", "ru": "RUS", "ir": "IRN", "tw": "TWN", "ua": "UKR",
}

#: The 12 daily features parse_gdelt.py registered, same names and families.
FEATURES: dict[str, tuple[str, str]] = {
    "gdelt_total_events": ("sentiment", "GDELT Total Daily Events"),
    "gdelt_conflict_count": ("sentiment", "GDELT Daily Conflict Events (QuadClass 3+4)"),
    "gdelt_cooperation_count": ("sentiment", "GDELT Daily Cooperation Events (QuadClass 1+2)"),
    "gdelt_avg_goldstein": ("sentiment", "GDELT Daily Average Goldstein Scale"),
    "gdelt_avg_tone": ("sentiment", "GDELT Daily Average Tone"),
    "gdelt_num_mentions": ("sentiment", "GDELT Daily Total Mentions"),
}
for _suffix in TRACKED_COUNTRIES:
    FEATURES[f"gdelt_events_{_suffix}"] = ("sentiment", f"GDELT Daily Events Involving {_suffix.upper()}")

_META = MetaData()
GDELT_EVENTS = Table(
    "gdelt_events", _META,
    Column("global_event_id", BigInteger, primary_key=True),
    Column("event_date", Date), Column("actor1_country", Text), Column("actor2_country", Text),
    Column("event_root_code", Text), Column("quad_class", Integer), Column("goldstein_scale", Float),
    Column("num_mentions", Integer), Column("num_sources", Integer), Column("num_articles", Integer),
    Column("avg_tone", Float), Column("date_added", DateTime(timezone=True)),
)


class GdeltBulkError(RuntimeError):
    """A file GDELT served but that cannot be read as an export; the run stops."""


# --- stamps ---------------------------------------------------------------------------


def parse_stamp(raw: str) -> datetime:
    """``YYYYMMDDHHMMSS`` on a 15-minute boundary -> aware UTC datetime."""
    if len(raw) != 14 or not raw.isdigit():
        raise ValueError(f"not a GDELT stamp: {raw!r}")
    stamp = datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    if stamp.minute % 15 or stamp.second:
        raise ValueError(f"GDELT stamp off the 15-minute grid: {raw!r}")
    return stamp


def format_stamp(stamp: datetime) -> str:
    return stamp.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def export_url(stamp: datetime) -> str:
    return f"{BASE_URL}{format_stamp(stamp)}.export.CSV.zip"


def day_stamps(day: date) -> list[datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return [start + i * SLOT for i in range(SLOTS_PER_DAY)]


def parse_lastupdate(body: str) -> datetime:
    """Newest export stamp named in ``lastupdate.txt`` (``size md5 url`` lines)."""
    for line in body.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[2].endswith(".export.CSV.zip"):
            return parse_stamp(parts[2].rsplit("/", 1)[-1][:14])
    raise GdeltBulkError("lastupdate.txt names no export file")


def published_at(last_modified: str | None, fetched_at: datetime) -> tuple[datetime, str]:
    """Publication instant of a file: Last-Modified when readable, else the fetch instant."""
    if last_modified:
        try:
            parsed = email.utils.parsedate_to_datetime(last_modified)
        except (TypeError, ValueError):
            parsed = None
        if parsed is not None and parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc), "last_modified"
    return fetched_at.astimezone(timezone.utc), "fetched_at"


# --- parsing --------------------------------------------------------------------------


def release_day(obs_day: date, published: datetime) -> date:
    """The date a day's points are known: the later of D+1 and the publication date, never D."""
    return max(obs_day + timedelta(days=1), published.astimezone(timezone.utc).date())


@dataclass
class FileAggregate:
    """Sums over one export file's events; days add these up."""

    events: int = 0
    conflict: int = 0
    cooperation: int = 0
    goldstein_sum: float = 0.0
    goldstein_n: int = 0
    tone_sum: float = 0.0
    tone_n: int = 0
    mentions: int = 0
    countries: dict[str, int] = field(default_factory=lambda: {k: 0 for k in TRACKED_COUNTRIES})

    def add(self, other: FileAggregate) -> None:
        self.events += other.events
        self.conflict += other.conflict
        self.cooperation += other.cooperation
        self.goldstein_sum += other.goldstein_sum
        self.goldstein_n += other.goldstein_n
        self.tone_sum += other.tone_sum
        self.tone_n += other.tone_n
        self.mentions += other.mentions
        for key in TRACKED_COUNTRIES:
            self.countries[key] += other.countries.get(key, 0)

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> FileAggregate:
        agg = cls(**{k: v for k, v in data.items() if k != "countries"})
        agg.countries = {k: int(data.get("countries", {}).get(k, 0)) for k in TRACKED_COUNTRIES}
        return agg


@dataclass
class ParsedFile:
    rows: list[dict[str, Any]]
    aggregate: FileAggregate
    rejected: int
    dateadded_mismatch: int


def _int(raw: str) -> int | None:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _float(raw: str) -> float | None:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _zip_member(data: bytes, label: str) -> bytes:
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
        members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise GdeltBulkError(f"{label}: expected one CSV member, found {members}")
        return archive.read(members[0])
    except zipfile.BadZipFile as exc:
        raise GdeltBulkError(f"{label}: not a zip archive ({exc})") from exc


def parse_export(data: bytes, stamp: datetime) -> ParsedFile:
    """Rows for ``gdelt_events`` and the file's aggregate.

    Rows with the wrong column count, or an unreadable event id or SQLDATE,
    are rejected and counted. Duplicated event ids keep their first row.
    """
    payload = _zip_member(data, format_stamp(stamp))
    return _parse_events(payload, columns=EXPORT_COLUMNS, dateadded_col=COL_DATEADDED,
                         dateadded_text=format_stamp(stamp), date_added=stamp, keep_rows=True)


def parse_daily_v1(data: bytes, day: date) -> ParsedFile:
    """Aggregate of a GDELT 1.0 daily export (``YYYYMMDD.export.CSV.zip``), no rows kept.

    GDELT 1.0 rows have 58 columns: the 2.0 layout up to AvgTone (col 34),
    one ADM2 code fewer in each of the three geography blocks, DATEADDED
    (``YYYYMMDD``) in col 56. Measured 2026-11-16 on 2016-03-15, 2025-06-10
    and 2026-11-15: the daily file for D holds exactly the events of the 96
    GDELT 2.0 files stamped D (239,103 / 140,233 / 61,540 ids, none extra on
    either side), so a day built from it continues the loader's day without a
    seam.
    """
    payload = _zip_member(data, day.strftime("%Y%m%d"))
    return _parse_events(payload, columns=V1_EXPORT_COLUMNS, dateadded_col=V1_COL_DATEADDED,
                         dateadded_text=day.strftime("%Y%m%d"), date_added=None, keep_rows=False)


def _parse_events(payload: bytes, *, columns: int, dateadded_col: int, dateadded_text: str,
                  date_added: datetime | None, keep_rows: bool) -> ParsedFile:
    stamp_text = dateadded_text
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    agg = FileAggregate()
    rejected = mismatch = 0
    reader = csv.reader(io.TextIOWrapper(io.BytesIO(payload), encoding="utf-8", errors="replace"),
                        delimiter="\t", quoting=csv.QUOTE_NONE)
    for rec in reader:
        if len(rec) != columns:
            rejected += 1
            continue
        event_id = _int(rec[COL_GLOBALEVENTID])
        try:
            event_date = datetime.strptime(rec[COL_SQLDATE], "%Y%m%d").date()  # noqa: DTZ007 -- a date, no zone
        except ValueError:
            event_date = None
        if event_id is None or event_date is None:
            rejected += 1
            continue
        if event_id in seen:
            continue
        seen.add(event_id)
        if rec[dateadded_col] != stamp_text:
            mismatch += 1
        a1 = rec[COL_ACTOR1COUNTRYCODE][:3] or None
        a2 = rec[COL_ACTOR2COUNTRYCODE][:3] or None
        quad = _int(rec[COL_QUADCLASS])
        goldstein = _float(rec[COL_GOLDSTEINSCALE])
        mentions = _int(rec[COL_NUMMENTIONS])
        tone = _float(rec[COL_AVGTONE])
        if keep_rows:
            rows.append({
                "global_event_id": event_id, "event_date": event_date,
                "actor1_country": a1, "actor2_country": a2,
                "event_root_code": rec[COL_EVENTROOTCODE][:4] or None,
                "quad_class": quad, "goldstein_scale": goldstein, "num_mentions": mentions,
                "num_sources": _int(rec[COL_NUMSOURCES]), "num_articles": _int(rec[COL_NUMARTICLES]),
                "avg_tone": tone, "date_added": date_added,
            })
        agg.events += 1
        if quad in (3, 4):
            agg.conflict += 1
        elif quad in (1, 2):
            agg.cooperation += 1
        if goldstein is not None:
            agg.goldstein_sum += goldstein
            agg.goldstein_n += 1
        if tone is not None:
            agg.tone_sum += tone
            agg.tone_n += 1
        agg.mentions += mentions or 0
        for key, code in TRACKED_COUNTRIES.items():
            if a1 == code or a2 == code:
                agg.countries[key] += 1
    return ParsedFile(rows=rows, aggregate=agg, rejected=rejected, dateadded_mismatch=mismatch)


def feature_values(agg: FileAggregate) -> dict[str, float]:
    """The 12 daily feature values of a day's combined aggregate (an average needs one value)."""
    values: dict[str, float] = {
        "gdelt_total_events": float(agg.events),
        "gdelt_conflict_count": float(agg.conflict),
        "gdelt_cooperation_count": float(agg.cooperation),
        "gdelt_num_mentions": float(agg.mentions),
    }
    if agg.goldstein_n:
        values["gdelt_avg_goldstein"] = agg.goldstein_sum / agg.goldstein_n
    if agg.tone_n:
        values["gdelt_avg_tone"] = agg.tone_sum / agg.tone_n
    for key in TRACKED_COUNTRIES:
        values[f"gdelt_events_{key}"] = float(agg.countries[key])
    return values


# --- HTTP -----------------------------------------------------------------------------


@dataclass
class Fetched:
    status: int
    body: bytes
    last_modified: str | None
    fetched_at: datetime


def _default_fetch() -> Callable[[str], Fetched]:
    import requests

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    def fetch(url: str) -> Fetched:
        resp = session.get(url, timeout=HTTP_TIMEOUT_S, allow_redirects=True)
        return Fetched(resp.status_code, resp.content, resp.headers.get("Last-Modified"),
                       datetime.now(timezone.utc))

    return fetch


# --- loader ---------------------------------------------------------------------------


@dataclass
class RunSummary:
    write: bool
    status: str = "ok"            # ok | budget | waiting | locked | error
    detail: str = ""
    newest_stamp: str | None = None
    first_day: str | None = None
    files_fetched: int = 0
    files_loaded: int = 0
    files_missing: int = 0
    events_parsed: int = 0
    events_inserted: int = 0
    rows_rejected: int = 0
    dateadded_mismatch: int = 0
    days_finalised: list[str] = field(default_factory=list)
    features_written: int = 0
    features_skipped_existing: int = 0
    late_days: list[str] = field(default_factory=list)


class GdeltBulkLoader:
    """Incremental, resumable loader for GDELT 2.0 export files."""

    def __init__(self, engine: Engine, fetch: Callable[[str], Fetched] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.engine = engine
        self.fetch = fetch or _default_fetch()
        self.clock = clock

    # registry --------------------------------------------------------------------

    def _registry(self, conn: Connection) -> tuple[int, dict[str, int]]:
        conn.execute(text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available, "
            "revision_behavior, trust_score, priority_rank) "
            "VALUES (:name, 'https://data.gdeltproject.org', 'FREE', 'EOD', FALSE, 'NEVER', 'MED', 8) "
            "ON CONFLICT (name) DO NOTHING"), {"name": SOURCE_NAME})
        src = conn.execute(text("SELECT id FROM source_catalog WHERE name = :name"),
                           {"name": SOURCE_NAME}).scalar_one()
        ids: dict[str, int] = {}
        for name, (family, desc) in FEATURES.items():
            conn.execute(text(
                "INSERT INTO feature_registry (name, family, description, transformation, "
                "transformation_version, lag_days, normalization, missing_data_policy, "
                "eligible_from_date, model_eligible) "
                "VALUES (:name, :family, :desc, 'RAW', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-01', TRUE) "
                "ON CONFLICT (name) DO NOTHING"), {"name": name, "family": family, "desc": desc})
            ids[name] = conn.execute(text("SELECT id FROM feature_registry WHERE name = :name"),
                                     {"name": name}).scalar_one()
        return src, ids

    # resume ----------------------------------------------------------------------

    def _first_day(self, conn: Connection, since: date | None) -> date:
        """Earliest day to consider: --since, else the ledger's first day, else after the legacy table."""
        if since is not None:
            return since
        first = conn.execute(text("SELECT min(file_stamp) FROM gdelt_export_files")).scalar()
        if first is not None:
            return first.astimezone(timezone.utc).date()
        legacy = conn.execute(text("SELECT max(summary_date) FROM gdelt_daily_summary")).scalar()
        if legacy is not None:
            return legacy + timedelta(days=1)
        raise GdeltBulkError("empty gdelt_daily_summary and ledger: pass since= for the first run")

    def _pending_days(self, conn: Connection, first: date, newest: datetime) -> list[date]:
        """Days from ``first`` whose last slot is published, without a summary row yet."""
        # Day D's last slot is D 23:45, so D is listed once newest >= D 23:45.
        last_full = (newest - (SLOTS_PER_DAY - 1) * SLOT).date()
        if last_full < first:
            return []
        done = {r[0] for r in conn.execute(text(
            "SELECT summary_date FROM gdelt_daily_summary WHERE summary_date BETWEEN :a AND :b"),
            {"a": first, "b": last_full})}
        out, day = [], first
        while day <= last_full:
            if day not in done:
                out.append(day)
            day += timedelta(days=1)
        return out

    def _recorded(self, conn: Connection, day: date) -> dict[datetime, dict[str, Any]]:
        start = day_stamps(day)[0]
        rows = conn.execute(text(
            "SELECT file_stamp, status, published_at, aggregates FROM gdelt_export_files "
            "WHERE file_stamp >= :a AND file_stamp < :b"),
            {"a": start, "b": start + timedelta(days=1)}).mappings().all()
        return {r["file_stamp"].astimezone(timezone.utc): dict(r) for r in rows}

    # one file --------------------------------------------------------------------

    def _load_file(self, stamp: datetime, got: Fetched, summary: RunSummary, write: bool) -> None:
        parsed = parse_export(got.body, stamp)
        when, basis = published_at(got.last_modified, got.fetched_at)
        summary.files_loaded += 1
        summary.events_parsed += len(parsed.rows)
        summary.rows_rejected += parsed.rejected
        summary.dateadded_mismatch += parsed.dateadded_mismatch
        if not write:
            return
        with self.engine.begin() as conn:
            inserted = 0
            if parsed.rows:
                stmt = (pg_insert(GDELT_EVENTS).on_conflict_do_nothing(index_elements=["global_event_id"])
                        .returning(GDELT_EVENTS.c.global_event_id))
                inserted = len(conn.execute(stmt, parsed.rows).all())
            conn.execute(text(
                "INSERT INTO gdelt_export_files (file_stamp, url, status, published_at, published_basis, "
                "fetched_at, md5, size_bytes, rows_parsed, rows_rejected, rows_inserted, aggregates) "
                "VALUES (:stamp, :url, 'loaded', :pub, :basis, :fetched, :md5, :size, :parsed, :rejected, "
                ":inserted, CAST(:agg AS JSONB)) ON CONFLICT (file_stamp) DO NOTHING"),
                {"stamp": stamp, "url": export_url(stamp), "pub": when, "basis": basis,
                 "fetched": got.fetched_at, "md5": hashlib.md5(got.body).hexdigest(),
                 "size": len(got.body), "parsed": len(parsed.rows), "rejected": parsed.rejected,
                 "inserted": inserted, "agg": json.dumps(asdict(parsed.aggregate))})
        summary.events_inserted += inserted

    def _record_missing(self, stamp: datetime, got: Fetched, write: bool) -> None:
        if not write:
            return
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO gdelt_export_files (file_stamp, url, status, fetched_at) "
                "VALUES (:stamp, :url, 'missing', :fetched) ON CONFLICT (file_stamp) DO NOTHING"),
                {"stamp": stamp, "url": export_url(stamp), "fetched": got.fetched_at})

    # one day ---------------------------------------------------------------------

    def _finalise(self, day: date, src_id: int, feat_ids: dict[str, int], summary: RunSummary) -> None:
        with self.engine.begin() as conn:
            recorded = self._recorded(conn, day)
            if len(recorded) != SLOTS_PER_DAY:
                return
            loaded = [r for r in recorded.values() if r["status"] == "loaded"]
            agg = FileAggregate()
            for r in loaded:
                agg.add(FileAggregate.from_json(r["aggregates"]))
            published = max((r["published_at"] for r in loaded), default=None)
            values = feature_values(agg) if loaded else {}
            done = conn.execute(text(
                "INSERT INTO gdelt_daily_summary (summary_date, total_events, conflict_count, "
                "cooperation_count, avg_goldstein, avg_tone, total_mentions, country_events, "
                "published_at, day_basis, files_loaded, files_missing) "
                "VALUES (:d, :total, :conflict, :coop, :gold, :tone, :mentions, CAST(:countries AS JSONB), "
                ":pub, :basis, :loaded, :missing) ON CONFLICT (summary_date) DO NOTHING RETURNING summary_date"),
                {"d": day, "total": agg.events if loaded else None,
                 "conflict": agg.conflict if loaded else None, "coop": agg.cooperation if loaded else None,
                 "gold": values.get("gdelt_avg_goldstein"), "tone": values.get("gdelt_avg_tone"),
                 "mentions": agg.mentions if loaded else None,
                 "countries": json.dumps({code: agg.countries[k] for k, code in TRACKED_COUNTRIES.items()})
                 if loaded else None,
                 "pub": published, "basis": DAY_BASIS, "loaded": len(loaded),
                 "missing": SLOTS_PER_DAY - len(loaded)}).scalar()
            if done is None:
                return
            summary.days_finalised.append(day.isoformat())
            if published is None:
                return
            published_day = published.astimezone(timezone.utc).date()
            if published_day > day:
                summary.late_days.append(day.isoformat())
            known = release_day(day, published)
            for name, value in values.items():
                hit = conn.execute(text(
                    "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                    "source_priority_used) VALUES (:fid, :od, :known, :known, :val, :src) "
                    "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING RETURNING id"),
                    {"fid": feat_ids[name], "od": day, "known": known, "val": value, "src": src_id}).scalar()
                if hit is None:
                    summary.features_skipped_existing += 1
                else:
                    summary.features_written += 1

    # run -------------------------------------------------------------------------

    def run(self, *, write: bool = False, since: date | None = None, until: date | None = None,
            budget_seconds: float | None = DAILY_BUDGET_SECONDS) -> RunSummary:
        summary = RunSummary(write=write)
        deadline = None if budget_seconds is None else self.clock() + budget_seconds
        with self.engine.connect() as lock_conn:
            if not lock_conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}).scalar():
                summary.status, summary.detail = "locked", "another gdelt_bulk run holds the lock"
                return summary
            try:
                self._run_locked(summary, write, since, until, deadline)
            finally:
                lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_KEY})
                lock_conn.commit()
        return summary

    def _run_locked(self, summary: RunSummary, write: bool, since: date | None, until: date | None,
                    deadline: float | None) -> None:
        got = self.fetch(LASTUPDATE_URL)
        if got.status != 200:
            summary.status, summary.detail = "error", f"lastupdate.txt answered {got.status}"
            return
        newest = parse_lastupdate(got.body.decode("utf-8", errors="replace"))
        summary.newest_stamp = format_stamp(newest)
        with self.engine.begin() as conn:
            src_id, feat_ids = self._registry(conn) if write else (0, {})
            first = self._first_day(conn, since)
            days = self._pending_days(conn, first, newest)
        if until is not None:
            days = [d for d in days if d <= until]
        summary.first_day = days[0].isoformat() if days else None
        for day in days:
            with self.engine.connect() as conn:
                recorded = self._recorded(conn, day)
            waiting = False
            for stamp in day_stamps(day):
                if stamp in recorded:
                    continue
                if deadline is not None and self.clock() >= deadline:
                    summary.status = "budget"
                    return
                got = self.fetch(export_url(stamp))
                summary.files_fetched += 1
                if got.status == 200:
                    try:
                        self._load_file(stamp, got, summary, write)
                    except GdeltBulkError as exc:
                        summary.status, summary.detail = "error", str(exc)
                        return
                elif got.status == 404 and newest - stamp >= MISSING_GRACE:
                    summary.files_missing += 1
                    self._record_missing(stamp, got, write)
                elif got.status == 404:
                    waiting = True
                else:
                    summary.status, summary.detail = "error", f"{format_stamp(stamp)} answered {got.status}"
                    return
            if waiting:
                summary.status, summary.detail = "waiting", f"{day} has a 404 younger than {MISSING_GRACE}"
                return
            if write:
                self._finalise(day, src_id, feat_ids, summary)


def run_daily(db_engine: Engine, write: bool = True) -> dict[str, Any]:
    """Hermes entry point: load and finalise the days since the last one, within the daily budget."""
    summary = GdeltBulkLoader(db_engine).run(write=write, budget_seconds=DAILY_BUDGET_SECONDS)
    log.info("gdelt_bulk: {s} {f} files ({m} missing), {e} new events, days {d}, {w} feature points{x}",
             s=summary.status, f=summary.files_loaded, m=summary.files_missing, e=summary.events_inserted,
             d=",".join(summary.days_finalised) or "-", w=summary.features_written,
             x=f" ({summary.detail})" if summary.detail else "")
    return asdict(summary)


def main(argv: Iterable[str] | None = None) -> int:
    import argparse

    from db import get_engine

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--write", action="store_true",
                    help="insert events, ledger rows, summaries and features (default: dry run, nothing written)")
    ap.add_argument("--since", type=date.fromisoformat, help="first UTC day (default: resume point)")
    ap.add_argument("--until", type=date.fromisoformat, help="last UTC day (default: newest complete day)")
    ap.add_argument("--budget", type=float, default=DAILY_BUDGET_SECONDS,
                    help="seconds before stopping between files; 0 = no limit")
    args = ap.parse_args(list(argv) if argv is not None else None)
    summary = GdeltBulkLoader(get_engine()).run(write=args.write, since=args.since, until=args.until,
                                                budget_seconds=args.budget or None)
    print(json.dumps(asdict(summary), indent=1, default=str))
    return 0 if summary.status in ("ok", "budget", "waiting") else 1


if __name__ == "__main__":
    raise SystemExit(main())
