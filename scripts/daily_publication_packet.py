#!/usr/bin/env python3
"""Daily publication packet producer (the capture producer side of
docs/runbooks/DAILY-PUBLICATION-PACKET.md).

For one *completed* UTC publication day ``D`` (window ``[D 00:00Z, D+1 00:00Z)``)
this job:

1. reads the day's GRID publications in ONE read-only ``REPEATABLE READ``
   transaction through the repo engine: ``market_briefings`` (``created_at``),
   ``thesis_snapshots`` (``timestamp``) and ``market_diary`` (``generated_at``)
   rows published inside the window, plus ``market_diary`` rows *about* the days
   ``[D-7, D]`` as retrospective context, plus audio briefing metadata files;
2. records the diary writer's own execution evidence for the window (the
   ``market_diary`` rows it wrote, and ``systemctl show`` of its unit when the
   last run falls inside the window) — or an explicitly verified non-session
   result on days the diary timer does not fire;
3. screens every publication text for credentials, connection strings, health
   identifiers and hidden characters. A hit quarantines the WHOLE record (never
   truncated, never edited) and is listed in the receipts by hash;
4. writes ``<root>/<D>/``: ``capture.json`` (raw rows, mode 0600),
   ``packet.json`` (screened publications, ``publication-review-v1`` shape the
   M0 checker and the daily-review consumer already read), three chained stage
   receipts under ``receipts/`` (``diary_run``, ``candidate``, ``capture``),
   ``receipt.json`` (summary) and ``observation.json`` in the
   ``daily-publication-packet-observation-v1`` schema with the three
   producer-owned stages declared. ``scripts/daily_publication_packet_status.py``
   reads that observation as-is; the daily-review owner appends its own stages.

A day that already has ``receipt.json`` is never rewritten (safe resume, no
replay). The job never writes to the database, never calls a provider or model,
and never authorizes anything downstream.

Usage:
    python3 scripts/daily_publication_packet.py                 # previous UTC day
    python3 scripts/daily_publication_packet.py --day 2026-11-06
    python3 scripts/daily_publication_packet.py --dry-run --json  # compute only

Timer template (install on grid-svr): deploy/systemd/grid-publication-packet.timer.template
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

JOB = "daily_publication_packet_handoff"
CAPTURE_SCHEMA = "publication-capture-v1"
PACKET_SCHEMA = "publication-review-v1"
RECEIPT_SCHEMA = "daily-publication-packet-stage-receipt-v1"
SUMMARY_SCHEMA = "daily-publication-packet-receipt-v1"
OBSERVATION_SCHEMA = "daily-publication-packet-observation-v1"
PRODUCER = "scripts/daily_publication_packet.py"

DEFAULT_ROOT = Path(os.environ.get("GRID_PUBLICATION_PACKET_ROOT", "~/research/publication-packets"))
DEFAULT_DUE_HOURS = float(os.environ.get("GRID_PUBLICATION_PACKET_DUE_HOURS", "8"))
DIARY_UNIT = os.environ.get("GRID_MARKET_DIARY_UNIT", "grid-market-diary.service")
# The installed grid-market-diary.timer fires Tue..Sat 05:30Z for the previous US
# session; on Sun/Mon UTC no diary generation is scheduled, so a missing in-window
# diary row on those days is an explicitly verified non-session result, not a gap.
DIARY_TIMER_WEEKDAYS = frozenset({1, 2, 3, 4, 5})  # Monday == 0

ROW_LIMIT_MAX = 500
STATEMENT_TIMEOUT_MS = "8000"
TEXT_FIELD_CAP_BYTES = 1_048_576          # per publication text; over = excluded whole, never truncated
PUBLICATIONS_AGGREGATE_CAP_BYTES = 8_000_000
ARTIFACT_READ_CAP_BYTES = 2_000_000
AUDIO_DIR = Path("/data/grid_v4/briefings")
GROUNDING_DIR = Path("/data/grid/outputs/market_briefings")

CHANNEL_TEXT = {"market_briefings": "content", "thesis_snapshots": "narrative",
                "market_diary": "content", "audio": "script_text"}
CHANNEL_CLOCK = {"market_briefings": "created_at", "thesis_snapshots": "timestamp",
                 "market_diary": "generated_at", "audio": "generated_at"}

QUERIES = {
    "market_briefings": (
        "SELECT id, briefing_type, briefing_date, content, sentiment_score, sentiment_label, "
        "created_at, octet_length(snapshot_data::text) AS snapshot_bytes, "
        "CASE WHEN octet_length(snapshot_data::text) <= 250000 THEN snapshot_data ELSE NULL END "
        "AS snapshot_data FROM market_briefings "
        "WHERE created_at >= :start AND created_at < :end AND created_at <= :asof "
        "ORDER BY created_at, id LIMIT :limit"
    ),
    "thesis_snapshots": (
        "SELECT id, timestamp, overall_direction, conviction, conviction_unit, narrative "
        "FROM thesis_snapshots "
        "WHERE timestamp >= :start AND timestamp < :end AND timestamp <= :asof "
        "ORDER BY timestamp, id LIMIT :limit"
    ),
    "market_diary": (
        "SELECT id, date, content, market_moves, thesis_accuracy, narrative_model, "
        "narrative_fallback, generated_at FROM market_diary "
        "WHERE generated_at >= :start AND generated_at < :end AND generated_at <= :asof "
        "ORDER BY generated_at, id LIMIT :limit"
    ),
}
DIARY_CONTEXT_QUERY = (
    "SELECT id, date, content, market_moves, thesis_accuracy, narrative_model, "
    "narrative_fallback, generated_at FROM market_diary "
    "WHERE date >= :since AND date <= :until AND generated_at <= :asof "
    "ORDER BY date DESC LIMIT 15"
)

# Screens (whole-record quarantine on hit). Patterns follow the retained daily
# screened-projection lineage; only size caps were loosened (operator decision).
SCREEN = {name: re.compile(pattern, re.MULTILINE) for name, pattern in {
    "credential_assignment": r'(?i)(?:api[_ -]?key|password|passwd|secret|access[_ -]?token|bearer)["\']?\s*[:=]\s*["\']?[^\s,}\]]{8,}',
    "credential_token": r'(?:\bBearer\s+[A-Za-z0-9._~+/=-]{16,}|\bsk-[A-Za-z0-9_-]{20,}|\bAKIA[0-9A-Z]{16}\b|\bgh[pousr]_[A-Za-z0-9]{30,})',
    "connection_string": r'(?i)\b(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp)://[^\s]+|[?&](?:token|key|sig|signature|password|secret|access_token)=',
    "private_key_block": r'-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----',
    "health_identifier": r'(?:(?i:\b(?:mrn|date of birth|hipaa|patient id|medical record number)\b)|\bPHI\b)',
}.items()}
HIDDEN = re.compile("[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f\u00ad\u200b-\u200f\u2028\u2029\u202a-\u202e\u2060-\u2064\ufeff]")


class Refused(Exception):
    """The job must not run (open window, bad arguments)."""


def canonical(value: Any) -> bytes:
    """Identical to the retained capture adapter and the M0 checker's row hashing."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":"), default=str).encode()


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pretty(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, indent=1, allow_nan=False, default=str).encode() + b"\n"


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def clock(value: Any) -> datetime:
    if isinstance(value, datetime):
        stamp = value
    else:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


def day_window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time(), timezone.utc)
    return start, start + timedelta(days=1)


def previous_utc_day(now: datetime | None = None) -> date:
    now = now or datetime.now(timezone.utc)
    return (now.astimezone(timezone.utc) - timedelta(days=1)).date()


def diary_expected(day: date) -> bool:
    return day.weekday() in DIARY_TIMER_WEEKDAYS


def screen_text(text: str) -> list[str]:
    reasons = [name for name, pattern in SCREEN.items() if pattern.search(text)]
    if HIDDEN.search(text):
        reasons.append("hidden_or_control_characters")
    return reasons


def _strings(value: Any):
    """Every raw string in a nested row: keys and values, so JSON escaping cannot hide a hit."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield str(key)
            if isinstance(item, (str, int, float)) and not isinstance(item, bool):
                yield f"{key}={item}"  # the assignment screens see the pair, not only the halves
            yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)


def screen_row(row: Any) -> list[str]:
    """Screen a whole record: each raw string (control characters intact) and the canonical encoding."""
    reasons: list[str] = []
    for text in [*_strings(row), canonical(row).decode("utf-8")]:
        reasons.extend(r for r in screen_text(text) if r not in reasons)
    return reasons


def bounded_read(path: Path) -> bytes:
    """No symlink following and a hard byte bound, even if the file grows."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        raw = stream.read(ARTIFACT_READ_CAP_BYTES + 1)
    if len(raw) > ARTIFACT_READ_CAP_BYTES:
        raise ValueError("artifact exceeds byte cap")
    return raw


# ---------------------------------------------------------------------------
# Database capture (one read-only REPEATABLE READ transaction)
# ---------------------------------------------------------------------------

def read_publications(engine: Any, day: date, limit: int) -> dict[str, Any]:
    from sqlalchemy import text

    start, end = day_window(day)
    bundle: dict[str, Any] = {
        "version": CAPTURE_SCHEMA, "producer": PRODUCER, "publication_day": day.isoformat(),
        "session_start": iso(start), "session_end": iso(end),
        "records": [], "channels": {}, "failures": [], "existing_diary_context": [],
    }
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": STATEMENT_TIMEOUT_MS})
        conn.execute(text("SELECT set_config('lock_timeout', :ms, true)"), {"ms": "2000"})
        asof = clock(conn.execute(text("SELECT transaction_timestamp()")).scalar_one())
        bundle["captured_at"] = iso(asof)
        bundle["database_receipt"] = {
            "transaction_read_only": conn.exec_driver_sql("SHOW transaction_read_only").scalar_one(),
            "transaction_isolation": conn.exec_driver_sql("SHOW transaction_isolation").scalar_one(),
            "statement_timeout": conn.exec_driver_sql("SHOW statement_timeout").scalar_one(),
            "source": "GRID database through the repo engine (db.get_engine); read-only snapshot",
        }
        for channel, query in QUERIES.items():
            try:
                rows = conn.execute(text(query), {"start": start, "end": end, "asof": asof,
                                                  "limit": limit + 1}).mappings().all()
            except Exception as exc:  # noqa: BLE001 - recorded, never echoed with details
                bundle["failures"].append(f"{channel}: {type(exc).__name__}; details suppressed")
                bundle["channels"][channel] = {"count": 0, "truncated": False, "complete": False,
                                               "state": "QUERY_FAILED"}
                continue
            truncated = len(rows) > limit
            records = [dict(row) for row in rows[:limit]]
            bundle["channels"][channel] = {
                "count": len(records), "truncated": truncated, "complete": not truncated,
                "state": "ROWS_RETURNED" if records else "ZERO_ROWS_RETURNED",
                "scope": "all rows whose stored publication clock lies in the half-open window, "
                         "bounded by the snapshot transaction timestamp",
            }
            for record in records:
                bundle["records"].append({"channel": channel, "record": record})
        try:
            rows = conn.execute(text(DIARY_CONTEXT_QUERY), {
                "since": day - timedelta(days=7), "until": day, "asof": asof}).mappings().all()
            bundle["existing_diary_context"] = [dict(row) for row in rows]
        except Exception as exc:  # noqa: BLE001
            bundle["failures"].append(f"market_diary context: {type(exc).__name__}; details suppressed")
        conn.rollback()
    return bundle


def read_audio_artifacts(bundle: dict[str, Any], limit: int, audio_dir: Path = AUDIO_DIR) -> None:
    """Audio briefing metadata (existing publication artifacts), bounded, not atomic with the DB."""
    start, end = clock(bundle["session_start"]), clock(bundle["session_end"])
    count, truncated = 0, False
    candidates = sorted(audio_dir.glob("briefing_*.json"), reverse=True) if audio_dir.is_dir() else []
    if len(candidates) > 5000:
        truncated = True
        bundle["failures"].append("audio: newest 5000 filename scan cap; older artifacts omitted")
    for path in candidates[:5000]:
        if count >= limit:
            truncated = True
            bundle["failures"].append("audio: cap reached; inventory incomplete")
            break
        try:
            raw = bounded_read(path)
            record = json.loads(raw)
            generated = record.get("generated_at")
            if not generated or not start <= clock(generated) < end:
                continue
            kept = {key: record[key] for key in ("generated_at", "script_text", "briefing_date") if key in record}
            kept.update(id=path.name, artifact=str(path), original_file_sha256=sha256(raw))
            bundle["records"].append({"channel": "audio", "record": kept})
            count += 1
        except Exception as exc:  # noqa: BLE001
            bundle["failures"].append(f"audio:{path.name}: {type(exc).__name__}; details suppressed")
    bundle["channels"]["audio"] = {
        "count": count, "truncated": truncated, "complete": False,
        "state": "ROWS_RETURNED" if count else "ZERO_ROWS_RETURNED_NOT_PROOF_OF_ZERO_PUBLICATIONS",
        "scope": f"existing JSON metadata in {audio_dir} only; not atomic with the database snapshot",
    }


def attach_grounding(bundle: dict[str, Any], grounding_dir: Path = GROUNDING_DIR) -> None:
    """Attach the briefing writer's grounding receipt when its text association is exact."""
    if not grounding_dir.is_dir():
        return
    for item in bundle["records"]:
        if item["channel"] != "market_briefings":
            continue
        row = item["record"]
        kind = row.get("briefing_type")
        if kind not in {"hourly", "daily", "weekly"}:
            continue
        stamp = clock(row["created_at"]).strftime("%Y%m%d_%H%M%S")
        path = grounding_dir / f"{kind}_{stamp}.grounding.json"
        try:
            markdown = bounded_read(path.with_suffix("").with_suffix(".md")).decode("utf-8")
            if not markdown.startswith(str(row.get("content")) + "\n\n---\n*Generated: "):
                raise ValueError("grounding text association mismatch")
            raw = bounded_read(path)
            receipt = json.loads(raw)
            row["grounding_receipt"] = {key: receipt[key] for key in (
                "checked_at", "total_numbers", "grounded_count", "ungrounded_count",
                "ungrounded_ratio", "banner_triggered", "threshold") if key in receipt}
            row["grounding_file_sha256"] = sha256(raw)
        except FileNotFoundError:
            bundle["failures"].append(f"market_briefings:{row['id']}: grounding receipt absent")
        except Exception as exc:  # noqa: BLE001
            bundle["failures"].append(f"market_briefings:{row['id']}: grounding {type(exc).__name__}")


# ---------------------------------------------------------------------------
# Diary execution evidence
# ---------------------------------------------------------------------------

def systemd_unit_evidence(unit: str, start: datetime, end: datetime, runner=subprocess.run) -> dict[str, Any]:
    """``systemctl show`` of the diary unit, kept only if its last run started inside the window."""
    props = ("ExecMainStartTimestamp", "ExecMainExitTimestamp", "ExecMainStatus", "Result")
    try:
        proc = runner(["systemctl", "show", unit] + [f"-p{p}" for p in props],
                      capture_output=True, text=True, timeout=10, check=False)
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "unit": unit, "reason": type(exc).__name__}
    if proc.returncode != 0:
        return {"available": False, "unit": unit, "reason": f"systemctl exit {proc.returncode}"}
    values = dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)
    try:
        started = datetime.strptime(values.get("ExecMainStartTimestamp", ""), "%a %Y-%m-%d %H:%M:%S %Z")
        started = started.replace(tzinfo=timezone.utc)
    except ValueError:
        return {"available": False, "unit": unit, "reason": "no parsable last start timestamp"}
    evidence = {"available": True, "unit": unit, "exec_main_start": iso(started),
                "exec_main_exit": values.get("ExecMainExitTimestamp"),
                "exec_main_status": values.get("ExecMainStatus"), "result": values.get("Result")}
    if not start <= started < end:
        evidence.update(available=False, reason="last run started outside the publication window")
    return evidence


def diary_run_evidence(bundle: dict[str, Any], day: date, unit_evidence: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    rows = [item["record"] for item in bundle["records"] if item["channel"] == "market_diary"]
    channel = bundle["channels"].get("market_diary", {})
    body: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA, "stage": "diary_run", "job": JOB, "publication_day": day.isoformat(),
        "window": {"start": bundle["session_start"], "end": bundle["session_end"]},
        "diary_timer_fires_on_this_weekday": diary_expected(day),
        "rows_generated_in_window": [{
            "id": r["id"], "session_date": str(r["date"]), "generated_at": iso(clock(r["generated_at"])),
            "narrative_model": r.get("narrative_model"), "narrative_fallback": r.get("narrative_fallback"),
            "row_sha256": sha256(canonical(r)),
        } for r in rows],
        "unit_evidence": unit_evidence,
        "clocks": "session_date is the US session the diary describes; generated_at is the publication clock "
                  "that places the row in this UTC window. Neither is inferred from the other.",
    }
    if channel.get("state") == "QUERY_FAILED":
        state, outcome = "unknown", "market_diary query failed; execution not observed"
    elif rows:
        state, outcome = "passed", "diary_rows_written_in_window"
    elif not diary_expected(day):
        state, outcome = "passed", "non_session_verified: no diary generation is scheduled on this UTC weekday"
    else:
        state, outcome = "failed", "diary generation was scheduled but no market_diary row was written in the window"
    body.update(state=state, outcome=outcome)
    return state, body


# ---------------------------------------------------------------------------
# Packet (screened) and receipts
# ---------------------------------------------------------------------------

def build_packet(bundle: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    start, end = clock(bundle["session_start"]), clock(bundle["session_end"])
    captured = clock(bundle["captured_at"])
    packet: dict[str, Any] = {
        "version": PACKET_SCHEMA, "review_mode": "claim_quality_unscorable", "promotion_allowed": False,
        "review_as_of": bundle.get("filesystem_capture_ended_at", bundle["captured_at"]),
        "publication_day": bundle["publication_day"],
        "session_start": bundle["session_start"], "session_end": bundle["session_end"],
        "publication_set_complete": False, "publications": [], "evidence": [], "claims": [],
        "hypotheses": [], "blockers": [
            "Capture-only packet: exact claim extraction and independent review remain required.",
            "Matched outcomes, frozen resolution rules, input vintages and price basis unavailable; no performance scoring.",
            "All-channel publication inventory not proven; channels outside market_briefings, thesis_snapshots, "
            "market_diary and audio metadata are not read.",
        ],
        "capture_receipt": {
            "export_sha256": sha256(canonical(bundle)), "channels": bundle["channels"],
            "database": bundle.get("database_receipt"), "missing_outcomes": True,
            "missing_decision_vintages": True, "omitted_publication_ids": [], "quarantined_publication_ids": [],
        },
    }
    if captured < end:
        packet["blockers"].append("Publication window remained open at capture; provisional review only.")
    packet["blockers"].extend(bundle.get("failures", []))
    for channel, receipt in bundle["channels"].items():
        if receipt.get("truncated"):
            packet["blockers"].append(f"{channel}: record cap reached; requested window truncated")
    exclusions: list[dict[str, Any]] = []
    packet["existing_diary_context"] = []
    packet["capture_receipt"]["quarantined_context_ids"] = []
    for row in bundle.get("existing_diary_context", []):
        identity, row_sha = f"market_diary:{row['id']}:context", sha256(canonical(row))
        # The whole row is screened, not only its text: a context row enters packet.json verbatim.
        reasons = screen_row(row)
        if reasons:
            exclusions.append({"publication_id": identity, "channel": "market_diary", "disposition": "QUARANTINED_SCREEN_HIT",
                               "reasons": reasons, "text_sha256": None, "text_utf8_bytes": None, "source_row_sha256": row_sha})
            packet["capture_receipt"]["quarantined_context_ids"].append(identity)
            continue
        packet["existing_diary_context"].append(row)
        packet["evidence"].append({
            "id": identity, "sha256": row_sha, "role": "hindsight",
            "source": "market_diary existing retrospective output about the day; not a new forecast or validated outcome",
            "available_at": None, "captured_at": bundle["captured_at"]})
    aggregate = 0
    for item in bundle["records"]:
        channel, row = item["channel"], item["record"]
        if channel not in CHANNEL_TEXT:
            raise ValueError("unknown channel")
        published = clock(row[CHANNEL_CLOCK[channel]])
        if not start <= published < end:
            raise ValueError("record outside window")
        identity = f"{channel}:{row['id']}"
        content = row.get(CHANNEL_TEXT[channel])
        if channel == "market_briefings" and (row.get("snapshot_bytes") or 0) > 250000:
            packet["blockers"].append(f"{identity}: stored generator context exceeds byte cap; omitted")
        if not isinstance(content, str) or not content.strip():
            packet["blockers"].append(f"{identity}: original text missing; no invented replacement")
            packet["capture_receipt"]["omitted_publication_ids"].append(identity)
            continue
        text_bytes = content.encode("utf-8")
        text_sha = sha256(text_bytes)
        # Screen the WHOLE canonical row: source_record (snapshot_data, grounding, etc.) enters packet.json too.
        reasons = screen_row(row)
        disposition = None
        if reasons:
            disposition = "QUARANTINED_SCREEN_HIT"
        elif len(text_bytes) > TEXT_FIELD_CAP_BYTES:
            disposition = "EXCLUDED_TEXT_OVER_FIELD_CAP"
        elif aggregate + len(text_bytes) > PUBLICATIONS_AGGREGATE_CAP_BYTES:
            disposition = "EXCLUDED_AGGREGATE_CAP_REACHED"
        if disposition:
            exclusions.append({"publication_id": identity, "channel": channel, "disposition": disposition,
                               "reasons": reasons, "text_sha256": text_sha, "text_utf8_bytes": len(text_bytes),
                               "source_row_sha256": sha256(canonical(row))})
            packet["capture_receipt"]["quarantined_publication_ids"].append(identity)
            continue
        aggregate += len(text_bytes)
        packet["publications"].append({
            "id": identity, "source": channel, "published_at": iso(published), "text": content,
            "sha256": text_sha, "claims_complete": False, "source_record": row})
        packet["evidence"].append({"id": identity + ":receipt", "source": identity, "sha256": sha256(canonical(row)),
                                   "role": "hindsight", "available_at": None, "captured_at": bundle["captured_at"]})
        packet["claims"].append({
            "id": identity + ":unclassified-text", "publication_id": identity, "quote": content,
            "disposition": "unmeasurable",
            "reason": "Whole original text retained for review; this is not an extracted forecast or performance label.",
            "decision_evidence": [], "outcome_evidence": [], "horizon_end": None, "frozen_rule": None})
    return packet, exclusions


def stage_receipt(stage: str, day: date, body: dict[str, Any], upstream: str | None) -> tuple[dict[str, Any], bytes, str]:
    receipt = dict(body, schema=RECEIPT_SCHEMA, stage=stage, job=JOB, publication_day=day.isoformat(),
                   upstream_receipt_sha256=upstream)
    raw = pretty(receipt)
    return receipt, raw, sha256(raw)


def observation(day: date, observed: datetime, due: datetime, stages: dict[str, dict[str, Any]],
                capture_attempt: str) -> dict[str, Any]:
    return {"schema": OBSERVATION_SCHEMA, "publication_day": day.isoformat(), "observed_at": iso(observed),
            "due_at": iso(due), "capture_attempt": capture_attempt, "stages": stages}


def stage_declaration(day: date, state: str, receipt_sha: str, upstream: str | None) -> dict[str, Any]:
    if state != "passed":
        return {"state": state}
    return {"state": "passed", "publication_day": day.isoformat(), "receipt_sha256": receipt_sha,
            "upstream_receipt_sha256": upstream}


# ---------------------------------------------------------------------------
# Filesystem
# ---------------------------------------------------------------------------

def write_file(path: Path, data: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def ensure_root(root: Path) -> Path:
    root = root.expanduser()
    if not root.exists():
        root.mkdir(parents=True, mode=0o700)
    return root


def existing_summary(day_dir: Path) -> dict[str, Any] | None:
    receipt = day_dir / "receipt.json"
    if not receipt.is_file():
        return None
    with receipt.open("rb") as stream:
        raw = stream.read(1_000_000)
    return dict(json.loads(raw), receipt_sha256=sha256(raw))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def produce(engine: Any, day: date, root: Path, *, limit: int, due_hours: float, dry_run: bool,
            now: datetime | None = None, unit_runner=subprocess.run,
            audio_dir: Path = AUDIO_DIR, grounding_dir: Path = GROUNDING_DIR) -> dict[str, Any]:
    now = clock(now or datetime.now(timezone.utc))
    start, end = day_window(day)
    if now < end:
        raise Refused("publication window has not closed yet")
    if not 1 <= limit <= ROW_LIMIT_MAX:
        raise Refused("limit must be 1..500")
    due = end + timedelta(hours=due_hours)
    day_dir = root / day.isoformat()
    existing = existing_summary(day_dir)
    if existing is not None:
        log.info("publication-packet {}: already produced (receipt sha256 {}); nothing rewritten",
                 day, existing.get("receipt_sha256", "?")[:12])
        return dict(existing, already_present=True)

    stages: dict[str, dict[str, Any]] = {}
    files: dict[str, tuple[bytes, int]] = {}
    unit_evidence = systemd_unit_evidence(DIARY_UNIT, start, end, runner=unit_runner)
    try:
        bundle = read_publications(engine, day, limit)
    except Exception as exc:  # noqa: BLE001 - the error type is recorded, never its text
        log.warning("publication-packet {}: database read failed: {}", day, type(exc).__name__)
        diary_state, diary_body = "unknown", {"state": "unknown", "outcome": "database read failed",
                                              "unit_evidence": unit_evidence}
        _, diary_raw, diary_sha = stage_receipt("diary_run", day, diary_body, None)
        files["receipts/diary_run.json"] = (diary_raw, 0o644)
        stages["diary_run"] = stage_declaration(day, diary_state, diary_sha, None)
        stages["candidate"] = {"state": "failed"}
        stages["capture"] = {"state": "failed"}
        failure = {"schema": SUMMARY_SCHEMA, "job": JOB, "publication_day": day.isoformat(), "state": "FAILED",
                   "stage": "candidate", "error_type": type(exc).__name__, "observed_at": iso(now)}
        obs = observation(day, now, due, stages, "not_started")
        if not dry_run:
            for name, (data, mode) in files.items():
                write_file(day_dir / name, data, mode)
            write_file(day_dir / f"FAILED-{now.strftime('%Y%m%dT%H%M%SZ')}.json", pretty(failure), 0o644)
            write_file(day_dir / "observation.json", pretty(obs), 0o644)
        return dict(failure, observation=obs)

    read_audio_artifacts(bundle, limit, audio_dir=audio_dir)
    attach_grounding(bundle, grounding_dir=grounding_dir)
    bundle["filesystem_capture_ended_at"] = iso(datetime.now(timezone.utc))
    bundle = json.loads(canonical(bundle))  # freeze to JSON-native types exactly as hashed
    capture_bytes = canonical(bundle)
    export_sha = sha256(capture_bytes)

    diary_state, diary_body = diary_run_evidence(bundle, day, unit_evidence)
    _, diary_raw, diary_sha = stage_receipt("diary_run", day, diary_body, None)

    packet, exclusions = build_packet(bundle)
    packet_bytes = canonical(packet)
    packet_sha = sha256(packet_bytes)

    candidate_body = {
        "state": "passed", "window": {"start": bundle["session_start"], "end": bundle["session_end"]},
        "sources": {ch: {"clock_field": CHANNEL_CLOCK[ch], **bundle["channels"].get(ch, {})} for ch in CHANNEL_TEXT},
        "captured_at": bundle["captured_at"], "database_receipt": bundle["database_receipt"],
        "record_manifest": [{"publication_id": f"{item['channel']}:{item['record']['id']}",
                             "channel": item["channel"],
                             "published_at": iso(clock(item["record"][CHANNEL_CLOCK[item["channel"]]])),
                             "source_row_sha256": sha256(canonical(item["record"]))} for item in bundle["records"]],
        "diary_context_rows": len(bundle["existing_diary_context"]),
        "failures": bundle["failures"], "capture_sha256": export_sha,
        "coverage": "bounded: at most the row limit per channel; truncation is flagged per channel",
    }
    _, candidate_raw, candidate_sha = stage_receipt("candidate", day, candidate_body, diary_sha)

    capture_body = {
        "state": "passed", "capture_sha256": export_sha, "capture_bytes": len(capture_bytes),
        "packet_sha256": packet_sha, "packet_bytes": len(packet_bytes),
        "publications_projected": len(packet["publications"]),
        "publications_by_channel": {ch: sum(1 for p in packet["publications"] if p["source"] == ch) for ch in CHANNEL_TEXT},
        "exclusions": exclusions, "omitted_publication_ids": packet["capture_receipt"]["omitted_publication_ids"],
        "screens_applied": sorted(SCREEN) + ["hidden_or_control_characters"],
        "caps": {"text_field_bytes": TEXT_FIELD_CAP_BYTES, "publications_aggregate_bytes": PUBLICATIONS_AGGREGATE_CAP_BYTES,
                 "rule": "whole-record exclusion only; nothing is truncated or edited"},
        "late_capture": clock(bundle["captured_at"]) >= end,
        "attempt": "spent", "outcome": "COMPLETED",
    }
    _, capture_raw, capture_sha = stage_receipt("capture", day, capture_body, candidate_sha)

    stages["diary_run"] = stage_declaration(day, diary_state, diary_sha, None)
    stages["candidate"] = stage_declaration(day, "passed", candidate_sha, diary_sha)
    stages["capture"] = stage_declaration(day, "passed", capture_sha, candidate_sha)
    obs = observation(day, now, due, stages, "spent")

    summary = {
        "schema": SUMMARY_SCHEMA, "job": JOB, "job_key": f"{JOB}:{day.isoformat()}",
        "publication_day": day.isoformat(), "state": "PRODUCED", "produced_at": iso(now),
        "window": {"start": bundle["session_start"], "end": bundle["session_end"]}, "due_at": iso(due),
        "diary_run": {"state": diary_state, "outcome": diary_body["outcome"],
                      "rows_generated_in_window": len(diary_body["rows_generated_in_window"])},
        "channels": bundle["channels"], "publications_projected": len(packet["publications"]),
        "quarantined": len(exclusions), "omitted_missing_text": len(packet["capture_receipt"]["omitted_publication_ids"]),
        "capture_sha256": export_sha, "packet_sha256": packet_sha,
        "receipts": {"diary_run": diary_sha, "candidate": candidate_sha, "capture": capture_sha},
        "files": {"capture.json": "raw rows, private (mode 0600)", "packet.json": "screened publications (publication-review-v1)",
                  "observation.json": OBSERVATION_SCHEMA, "receipts/": RECEIPT_SCHEMA},
        "next_owner": "codex_daily_review", "next_stage": "admission_review", "authorizes_execution": False,
        "dry_run": dry_run,
    }
    summary_raw = pretty(summary)
    summary["receipt_sha256"] = sha256(summary_raw)

    files.update({
        "capture.json": (capture_bytes, 0o600), "packet.json": (packet_bytes, 0o644),
        "receipts/diary_run.json": (diary_raw, 0o644), "receipts/candidate.json": (candidate_raw, 0o644),
        "receipts/capture.json": (capture_raw, 0o644), "observation.json": (pretty(obs), 0o644),
    })
    if not dry_run:
        day_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        for name, (data, mode) in files.items():
            write_file(day_dir / name, data, mode)
        write_file(day_dir / "receipt.json", summary_raw, 0o644)
    log.info("publication-packet {}: {} publications projected, {} quarantined, diary_run={}, packet {}{}",
             day, summary["publications_projected"], summary["quarantined"], diary_state, packet_sha[:12],
             " [dry-run]" if dry_run else "")
    return dict(summary, observation=obs, day_dir=str(day_dir))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--day", default=None, help="completed UTC publication day (YYYY-MM-DD); default: yesterday UTC")
    p.add_argument("--out-root", type=Path, default=DEFAULT_ROOT,
                   help="packet root (default $GRID_PUBLICATION_PACKET_ROOT or ~/research/publication-packets)")
    p.add_argument("--limit", type=int, default=ROW_LIMIT_MAX, help="rows per channel, 1..500")
    p.add_argument("--due-hours", type=float, default=DEFAULT_DUE_HOURS,
                   help="hours after the window closes by which the day should be closed (observation due_at)")
    p.add_argument("--dry-run", action="store_true", help="compute and report without writing")
    p.add_argument("--json", action="store_true", help="print the full result as JSON")
    return p


def main(argv: list[str] | None = None, engine: Any = None, now: datetime | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        day = date.fromisoformat(args.day) if args.day else previous_utc_day(now)
    except ValueError:
        print(json.dumps({"state": "REFUSED", "reason": "invalid --day"}))
        return 2
    if engine is None:
        from db import get_engine

        engine = get_engine()
    try:
        result = produce(engine, day, ensure_root(args.out_root), limit=args.limit, due_hours=args.due_hours,
                         dry_run=args.dry_run, now=now)
    except Refused as exc:
        print(json.dumps({"state": "REFUSED", "publication_day": day.isoformat(), "reason": str(exc)}))
        return 2
    if args.json:
        print(json.dumps(result, sort_keys=True, default=str))
    else:
        print(f"publication-packet {day}: state={result.get('state')} "
              f"publications={result.get('publications_projected')} quarantined={result.get('quarantined')} "
              f"diary_run={(result.get('diary_run') or {}).get('state')} "
              f"packet={str(result.get('packet_sha256', ''))[:12]}"
              + (" [already-present]" if result.get("already_present") else "")
              + (" [dry-run]" if result.get("dry_run") else ""))
    return 0 if result.get("state") in {"PRODUCED"} else 1


if __name__ == "__main__":
    sys.exit(main())
