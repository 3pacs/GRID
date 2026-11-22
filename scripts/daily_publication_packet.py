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
import math
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
DEFAULT_DUE_HOURS = float(os.environ.get("GRID_PUBLICATION_PACKET_DUE_HOURS", "12"))
# World-readable copy of the SCREENED files (never capture.json) for any agent on the fleet.
DEFAULT_PUBLIC_ROOT = Path(os.environ.get("GRID_PUBLICATION_PACKET_PUBLIC_ROOT",
                                          "~/research/publication-packets-public")).expanduser()
PUBLIC_FILES = ("packet.json", "receipt.json", "observation.json")
PUBLIC_DIRS = ("receipts",)
PUBLIC_GLOBS = ("FAILED-*.json",)
NEVER_PUBLIC = ("capture.json",)
DIARY_UNIT = os.environ.get("GRID_MARKET_DIARY_UNIT", "grid-market-diary.service")
# The installed grid-market-diary.timer fires Tue..Sat 05:30Z for the previous US
# session; on Sun/Mon UTC no diary generation is scheduled, so a missing in-window
# diary row on those days is an explicitly verified non-session result, not a gap.
DIARY_TIMER_WEEKDAYS = frozenset({1, 2, 3, 4, 5})  # Monday == 0

ROW_LIMIT_MAX = 500
STATEMENT_TIMEOUT_MS = "8000"
# Per publication text; over = excluded whole, never truncated. Reviewed 2026-11-07
# after market_briefings id 5007 (hourly, 1,527,952 chars) was excluded: that row
# was a 1.6 KB narrative plus a 1.5 MB "Feature evidence declarations" appendix
# (961 zero-valued features equal-value-matched a "0%" mention). The writer now
# bounds that appendix (ollama/market_briefing.py: match_list_cap=8 per mention,
# 256 KiB appendix cap, whole-appendix withhold with a declared note), so a
# legitimate briefing is a <=15 KB narrative plus <=256 KiB appendix. The cap
# stays at 1 MiB: nothing legitimate reaches it, and raising it would admit the
# runaway shape instead of the fixed one. Historical rows written before the
# fix that exceed it stay EXCLUDED_TEXT_OVER_FIELD_CAP by id/hash, by design.
TEXT_FIELD_CAP_BYTES = 1_048_576
PUBLICATIONS_AGGREGATE_CAP_BYTES = 8_000_000
ARTIFACT_READ_CAP_BYTES = 2_000_000
AUDIO_DIR = Path("/data/grid_v4/briefings")
GROUNDING_DIR = Path("/data/grid/outputs/market_briefings")

CHANNEL_TEXT = {"market_briefings": "content", "thesis_snapshots": "narrative",
                "market_diary": "content", "audio": "script_text"}
CHANNEL_CLOCK = {"market_briefings": "created_at", "thesis_snapshots": "timestamp",
                 "market_diary": "generated_at", "audio": "generated_at"}

# The briefing claim guard's verdict travels with every briefing, even when the
# whole generator snapshot is over its 250 KB cap (every withheld snapshot from
# 2026-11-05 on was 0.49-8.6 MB, so no withheld record carried its reasons).
# Its rejected draft is bounded to CLAIM_GUARD_DRAFT_CAP_BYTES with a declared
# truncation record (full size + sha256), never silently dropped. This field is
# not part of the publication text, so TEXT_FIELD_CAP_BYTES and
# PUBLICATIONS_AGGREGATE_CAP_BYTES (which count publication text only) do not
# govern it; this cap does.
CLAIM_GUARD_DRAFT_CAP_BYTES = 65_536
CLAIM_GUARD_META_CAP_BYTES = 65_536
# Static SQL text only (no interpolation); 65536 below equals CLAIM_GUARD_DRAFT_CAP_BYTES
# (left() counts characters, so the prefix is re-bounded to bytes in Python).
CLAIM_GUARD_COLUMNS = (
    "CASE WHEN jsonb_typeof(snapshot_data -> 'publication_claim_guard') = 'object' "
    "THEN (snapshot_data -> 'publication_claim_guard') - 'rejected_candidate' END AS claim_guard_meta, "
    "CASE WHEN jsonb_typeof(snapshot_data -> 'publication_claim_guard') = 'object' "
    "THEN left(snapshot_data -> 'publication_claim_guard' ->> 'rejected_candidate', 65536) END "
    "AS claim_guard_draft_prefix, "
    "CASE WHEN jsonb_typeof(snapshot_data -> 'publication_claim_guard') = 'object' "
    "THEN octet_length(snapshot_data -> 'publication_claim_guard' ->> 'rejected_candidate') END "
    "AS claim_guard_draft_bytes, "
    "CASE WHEN jsonb_typeof(snapshot_data -> 'publication_claim_guard') = 'object' "
    "THEN encode(sha256(convert_to(snapshot_data -> 'publication_claim_guard' ->> 'rejected_candidate', "
    "'UTF8')), 'hex') END AS claim_guard_draft_sha256, "
)
_CLAIM_GUARD_KEYS = ("claim_guard_meta", "claim_guard_draft_prefix", "claim_guard_draft_bytes",
                     "claim_guard_draft_sha256")

QUERIES = {
    "market_briefings": (
        "SELECT id, briefing_type, briefing_date, content, sentiment_score, sentiment_label, "
        "created_at, octet_length(snapshot_data::text) AS snapshot_bytes, "
        + CLAIM_GUARD_COLUMNS +
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


def _utf8_prefix(text: str, cap: int) -> str:
    """Longest prefix of ``text`` within ``cap`` UTF-8 bytes, never splitting a character."""
    return text.encode("utf-8")[:cap].decode("utf-8", errors="ignore")


def bound_claim_guard(record: dict[str, Any]) -> dict[str, Any]:
    """Fold the claim-guard columns into one bounded ``publication_claim_guard`` field.

    Rows without the columns (older callers) are returned unchanged. A missing
    guard is an explicit ``None``. The draft keeps at most
    CLAIM_GUARD_DRAFT_CAP_BYTES and declares its full size, sha256 and whether
    it was truncated.
    """
    if not any(key in record for key in _CLAIM_GUARD_KEYS):
        return record
    meta, prefix, draft_bytes, draft_sha = (record.pop(key, None) for key in _CLAIM_GUARD_KEYS)
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except ValueError:
            meta = None
    if not isinstance(meta, dict):
        record["publication_claim_guard"] = None
        return record
    guard: dict[str, Any] = dict(meta)
    meta_bytes = len(canonical(guard))
    if meta_bytes > CLAIM_GUARD_META_CAP_BYTES:
        guard = {key: guard.get(key) for key in ("passed", "reasons", "reason_counts", "scope")}
        guard["metadata_bound"] = {"utf8_bytes": meta_bytes, "cap_bytes": CLAIM_GUARD_META_CAP_BYTES,
                                   "truncated": True,
                                   "note": "guard metadata over cap; only verdict, reasons and counts kept"}
    if isinstance(prefix, str):
        kept = _utf8_prefix(prefix, CLAIM_GUARD_DRAFT_CAP_BYTES)
        kept_bytes = len(kept.encode("utf-8"))
        full_bytes = draft_bytes if isinstance(draft_bytes, int) else len(prefix.encode("utf-8"))
        truncated = full_bytes > kept_bytes
        guard["rejected_candidate"] = kept
        guard["rejected_candidate_bound"] = {
            "utf8_bytes": full_bytes, "retained_utf8_bytes": kept_bytes, "sha256": draft_sha,
            "cap_bytes": CLAIM_GUARD_DRAFT_CAP_BYTES, "truncated": truncated,
            "note": (f"rejected draft truncated to its first {kept_bytes} of {full_bytes} UTF-8 bytes; "
                     "sha256 is of the full stored draft") if truncated else None,
        }
    record["publication_claim_guard"] = guard
    return record


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
                if channel == "market_briefings":
                    bound_claim_guard(record)
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


# Packet guards: regime bound-input minimum and cadence-aware staleness limits.
MIN_REGIME_BOUND_INPUTS = 8
REGIME_BOUND_INPUT_UNAVAILABLE_NOTICE = "regime bound-input count unavailable"
_UNASSERTED_REGIME_LABELS = frozenset({"", "UNKNOWN", "UNCALIBRATED", "NONE", "N/A"})
_INVALID_FEATURE_ID_STRINGS = frozenset({
    "", "null", "none", "n/a", "na", "unknown", "missing", "unavailable", "-",
})
_INELIGIBLE_FEATURE_STATUSES = frozenset({
    "ineligible", "stale", "missing", "unavailable", "rejected",
    "invalid", "unverified", "future", "withheld", "excluded",
})
_SNAPSHOT_ID_KEYS = ("snapshot_id", "producer_snapshot_id", "analytical_snapshot_id")
_SNAPSHOT_HASH_KEYS = ("snapshot_hash", "producer_snapshot_hash", "snapshot_sha256", "payload_sha256")
_OBSERVED_TIMESTAMP_KEYS = (
    "data_as_of", "obs_date", "date", "as_of_date", "as_of",
    "available_at", "snapshot_available_at", "created_at",
    "trained_at", "model_updated_at", "updated_at", "timestamp",
    "collected_at", "checked_at",
)
_NUMERIC_AGE_KEYS = (
    ("age_seconds", "seconds"),
    ("age_hours", "hours"),
    ("age_days", "days"),
    ("data_staleness_days", "days"),
)
# Only actual bound-input fields; n_features / feature_count / n_features_used
# reflect registry or matrix sizes rather than bound inputs and are ignored.
_BOUND_INPUT_KEYS = (
    "features_used",
    "eligible_feature_ids",
    "bound_inputs",
)
# Cadence-aware max-age defaults (days), matching auto_regime.py (5d) and
# intelligence/regime/state_vector.py (monthly 70d, general features 30d).
DEFAULT_MAX_AGE_DAYS: dict[str, int] = {
    "equities": 5,
    "equity_volume": 5,
    "rates": 5,
    "credit": 5,
    "volatility": 5,
    "commodities": 5,
    "fx": 5,
    "equity_etf_proxies": 5,
    "options": 5,
    "latest_regime": 5,
    "regime": 5,
    "regime_detection": 5,
    "market_moves": 5,
    "features": 30,
    "macro": 70,
    "models": 30,
    "model_states": 30,
}
DAILY_MACRO_SERIES = frozenset({"T5YIE", "T10YIE"})
MONTHLY_MACRO_SERIES = frozenset({
    "CPIAUCSL", "PCEPILFE", "UNRATE", "RSAFS", "UMCSENT",
    "INDPRO", "TCU", "M2SL", "PAYEMS",
})


def _normalize_typed_feature_id(val: Any) -> tuple[tuple[str, Any], str | int] | None:
    """Normalize a typed feature identifier (non-bool int >= 0 or non-blank str)."""
    if val is None or isinstance(val, bool):
        return None
    if type(val) is int:
        if val < 0:
            return None
        return ("int", val), val
    if type(val) is str:
        cleaned = val.strip()
        if cleaned.lower() in _INVALID_FEATURE_ID_STRINGS:
            return None
        return ("str", cleaned.casefold()), cleaned
    return None


def _extract_distinct_eligible_feature_ids(
    raw_features: Any,
    *,
    eligible_ids: set[tuple[str, Any]] | None = None,
    excluded_ids: set[tuple[str, Any]] | None = None,
    as_of: datetime | None = None,
) -> list[str | int] | None:
    """Return sorted distinct, non-null, typed, eligible feature IDs from *raw_features*."""
    if not isinstance(raw_features, (dict, list, tuple, set)):
        return None

    def _entry_eligible(entry: dict[str, Any]) -> bool:
        if "eligible" in entry and entry["eligible"] is not True:
            return False
        if "model_eligible" in entry and entry["model_eligible"] is not True:
            return False
        if entry.get("stale") is True:
            return False
        if "status" in entry:
            st = entry["status"]
            if type(st) is not str or st.strip().lower() in _INELIGIBLE_FEATURE_STATUSES:
                return False
        if as_of is not None and _check_entry_clock_sanity(entry, "feature", "input", as_of):
            return False
        return True

    def _dict_entry_id(entry: dict[str, Any], fallback_key: Any = None) -> tuple[tuple[str, Any], str | int] | None:
        for id_key in ("feature_id", "id", "feature", "series_id", "name"):
            if id_key in entry:
                return _normalize_typed_feature_id(entry[id_key])
        if fallback_key is not None:
            return _normalize_typed_feature_id(fallback_key)
        return None

    seen: dict[tuple[str, Any], str | int] = {}

    def _consider(pair: tuple[tuple[str, Any], str | int] | None, alias_key: tuple[str, Any] | None = None) -> None:
        if pair is None:
            return
        norm_key, display_val = pair
        if excluded_ids and (norm_key in excluded_ids or (alias_key is not None and alias_key in excluded_ids)):
            return
        if eligible_ids is not None and norm_key not in eligible_ids and (alias_key is None or alias_key not in eligible_ids):
            return
        seen.setdefault(norm_key, display_val)

    if isinstance(raw_features, dict):
        for k, v in raw_features.items():
            key_pair = _normalize_typed_feature_id(k)
            if key_pair is None:
                continue
            if isinstance(v, dict):
                if not _entry_eligible(v):
                    continue
                _consider(_dict_entry_id(v, fallback_key=k), alias_key=key_pair[0])
            else:
                _consider(_normalize_typed_feature_id(v), alias_key=key_pair[0])
    else:
        for item in raw_features:
            if isinstance(item, dict):
                if not _entry_eligible(item):
                    continue
                _consider(_dict_entry_id(item))
            else:
                _consider(_normalize_typed_feature_id(item))

    return [val for _, val in sorted(seen.items(), key=lambda kv: (kv[0][0], str(kv[0][1])))]


def _snapshot_hash_matches(payload: dict[str, Any], expected_hash: Any) -> bool:
    """Verify *expected_hash* against *payload* canonical sha256 or declared hash."""
    if type(expected_hash) is not str or not expected_hash.strip():
        return False
    exp = expected_hash.strip().lower()
    if exp.startswith("sha256:"):
        exp = exp[7:]
    if not exp:
        return False

    declared: list[str] = []
    for hkey in _SNAPSHOT_HASH_KEYS:
        val = payload.get(hkey)
        if type(val) is str and val.strip():
            norm = val.strip().lower()
            if norm.startswith("sha256:"):
                norm = norm[7:]
            if norm != exp:
                return False
            declared.append(norm)

    is_sha256_hex = len(exp) == 64 and all(c in "0123456789abcdef" for c in exp)
    if is_sha256_hex:
        if sha256(canonical(payload)) == exp:
            return True
        stripped = {k: v for k, v in payload.items() if k not in _SNAPSHOT_HASH_KEYS}
        if len(stripped) != len(payload) and sha256(canonical(stripped)) == exp:
            return True
        if declared and all(h == exp for h in declared) and set(payload.keys()) <= {
            "snapshot_id", "snapshot_hash", "available_at", "regime", "state",
            "producer_snapshot_id", "producer_snapshot_hash", "snapshot_sha256",
        }:
            return True
        return False
    return bool(declared) and all(h == exp for h in declared)


def _extract_snapshot_id(mapping: dict[str, Any], *, allow_bare_id: bool = False) -> int | str | None:
    keys = ("id", *_SNAPSHOT_ID_KEYS) if allow_bare_id else _SNAPSHOT_ID_KEYS
    for key in keys:
        if key in mapping and mapping[key] is not None:
            val = mapping[key]
            if type(val) is int and val >= 0:
                return val
            if type(val) is str and val.strip():
                return val.strip()
    return None


def _extract_snapshot_hash(mapping: dict[str, Any]) -> str | None:
    for key in _SNAPSHOT_HASH_KEYS:
        val = mapping.get(key)
        if type(val) is str and val.strip():
            return val.strip()
    return None


def _extract_bound_input_count(mapping: dict[str, Any], *, as_of: datetime | None = None) -> int | None:
    """Return an integer distinct eligible bound-input count from *mapping*, else None.

    Reads ``features_used`` (distinct non-null typed eligible feature IDs) before
    ``eligible_feature_ids`` and ``bound_inputs`` (collection distinct count or
    non-negative int). Registry-size fields (``n_features``, ``feature_count``,
    ``n_features_used``) are never treated as bound-input counts.
    """
    excl_raw = mapping.get("ineligible_features") or mapping.get("excluded_features")
    excluded_set: set[tuple[str, Any]] | None = None
    if isinstance(excl_raw, (list, tuple, set, dict)):
        excl_items = excl_raw.keys() if isinstance(excl_raw, dict) else excl_raw
        excluded_set = {
            pair[0]
            for item in excl_items
            if (pair := _normalize_typed_feature_id(item)) is not None
        }
    elig_raw = mapping.get("eligible_features")
    eligible_set: set[tuple[str, Any]] | None = None
    if isinstance(elig_raw, (list, tuple, set)):
        eligible_set = {
            pair[0]
            for item in elig_raw
            if (pair := _normalize_typed_feature_id(item)) is not None
        }

    for key in _BOUND_INPUT_KEYS:
        if key not in mapping:
            continue
        raw = mapping[key]
        if key in ("features_used", "eligible_feature_ids"):
            distinct = _extract_distinct_eligible_feature_ids(
                raw,
                eligible_ids=eligible_set if key == "features_used" else None,
                excluded_ids=excluded_set,
                as_of=as_of,
            )
            if distinct is not None:
                return len(distinct)
            continue
        if isinstance(raw, (list, tuple, dict, set)):
            distinct = _extract_distinct_eligible_feature_ids(
                raw, eligible_ids=eligible_set, excluded_ids=excluded_set, as_of=as_of,
            )
            if distinct is not None:
                return len(distinct)
            continue
        if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0:
            return raw
    return None


def _extract_regime_label(mapping: dict[str, Any]) -> str | None:
    """Return an asserted regime label string from *mapping*, or None."""
    for key in ("state", "regime", "inferred_state", "regime_label"):
        val = mapping.get(key)
        if isinstance(val, str) and val.strip().upper() not in _UNASSERTED_REGIME_LABELS:
            return val.strip()
    return None


def _resolve_row_as_of(row: dict[str, Any], as_of: datetime | None) -> datetime | None:
    if as_of is not None:
        return clock(as_of)
    for key in ("created_at", "generated_at", "timestamp"):
        val = row.get(key)
        if val is not None:
            _, dt, _, _ = _parse_observed_clock(val)
            if dt is not None:
                return dt
    return None


def _verify_producer_snapshot_binding(
    regime_mapping: dict[str, Any],
    regime_label: str,
    producer_snap: Any,
    as_of: datetime | None,
) -> tuple[bool, str | None, int | None]:
    """Verify that *producer_snap* matches *regime_mapping* by id/hash, label, and clock."""
    if not isinstance(producer_snap, dict):
        return False, "invalid_producer_snapshot", None

    if as_of is not None:
        clock_errs = _check_entry_clock_sanity(producer_snap, "producer_snapshot", "model", as_of)
        if clock_errs:
            return False, f"producer_snapshot_{clock_errs[0]['reason']}", None

    snap_label = _extract_regime_label(producer_snap)
    if snap_label is None:
        return False, "producer_snapshot_missing_regime_label", None
    if snap_label.upper() != regime_label.upper():
        return False, "producer_snapshot_regime_mismatch", None

    expected_id = _extract_snapshot_id(regime_mapping)
    actual_id = _extract_snapshot_id(producer_snap, allow_bare_id=True)
    if expected_id is not None:
        if actual_id is None or actual_id != expected_id:
            return False, "producer_snapshot_id_mismatch", None

    expected_hash = _extract_snapshot_hash(regime_mapping)
    if expected_hash is not None:
        if not _snapshot_hash_matches(producer_snap, expected_hash):
            return False, "producer_snapshot_hash_mismatch", None

    snap_count = _extract_bound_input_count(producer_snap, as_of=as_of)
    return True, None, snap_count


def inspect_regime_binding(row: dict[str, Any], as_of: datetime | None = None) -> dict[str, Any]:
    """Read-only inspection of regime label binding, snapshot match, clock sanity,
    and distinct eligible bound-input count."""
    if not isinstance(row, dict):
        return {"status": "ok", "label": None, "count": None, "reason": None}

    resolved_as_of = _resolve_row_as_of(row, as_of)
    roots: list[dict[str, Any]] = [row]
    snap = row.get("snapshot_data")
    if isinstance(snap, dict):
        roots.append(snap)

    # Explicit regime sub-containers carry regime declarations and must have a valid label.
    explicit_containers: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for root in roots:
        for key in ("latest_regime", "regime", "regime_detection"):
            sub = root.get(key)
            if isinstance(sub, dict) and sub:
                explicit_containers.append((key, sub, root))

    # Cross-check sibling regime containers within the same root (e.g. latest_regime vs regime_detection)
    for root in roots:
        siblings = [
            (k, root[k])
            for k in ("latest_regime", "regime", "regime_detection")
            if isinstance(root.get(k), dict) and root[k]
        ]
        if len(siblings) >= 2:
            labels = {_extract_regime_label(m).upper() for _, m in siblings if _extract_regime_label(m)}
            if len(labels) > 1:
                return {
                    "status": "mismatch",
                    "label": _extract_regime_label(siblings[0][1]),
                    "count": None,
                    "reason": "conflicting_regime_snapshots",
                }
            sids = {_extract_snapshot_id(m) for _, m in siblings if _extract_snapshot_id(m) is not None}
            if len(sids) > 1:
                return {
                    "status": "mismatch",
                    "label": _extract_regime_label(siblings[0][1]),
                    "count": None,
                    "reason": "conflicting_regime_snapshot_ids",
                }

    unavailable_label: str | None = None

    def _inspect_one(
        mapping: dict[str, Any],
        parent: dict[str, Any] | None,
        *,
        explicit_regime_container: bool,
    ) -> dict[str, Any] | None:
        nonlocal unavailable_label
        label = _extract_regime_label(mapping)
        if label is None:
            if explicit_regime_container:
                return {
                    "status": "mismatch",
                    "label": None,
                    "count": None,
                    "reason": "missing_regime_label",
                }
            return None

        if resolved_as_of is not None:
            clock_errs = _check_entry_clock_sanity(mapping, "regime", "model", resolved_as_of)
            if clock_errs:
                return {
                    "status": "mismatch",
                    "label": label,
                    "count": None,
                    "reason": f"regime_{clock_errs[0]['reason']}",
                }

        raw_bi = mapping.get("bound_inputs")
        if "bound_inputs" in mapping and raw_bi is not None and (
            isinstance(raw_bi, bool)
            or (isinstance(raw_bi, int) and raw_bi < 0)
        ):
            return {
                "status": "mismatch",
                "label": label,
                "count": None,
                "reason": "invalid_bound_inputs",
            }

        snap_count_from_producer: int | None = None
        for snap_key in ("producer_snapshot", "analytical_snapshot"):
            if snap_key in mapping and mapping[snap_key] is not None:
                ok, reason, scount = _verify_producer_snapshot_binding(
                    mapping, label, mapping[snap_key], resolved_as_of,
                )
                if not ok:
                    return {"status": "mismatch", "label": label, "count": None, "reason": reason}
                if scount is not None:
                    snap_count_from_producer = scount

        has_own_collection = any(
            k in mapping and isinstance(mapping[k], (dict, list, tuple, set))
            for k in ("features_used", "eligible_feature_ids", "bound_inputs")
        )
        count = _extract_bound_input_count(mapping, as_of=resolved_as_of)
        if snap_count_from_producer is not None:
            if count is None or (not has_own_collection and snap_count_from_producer < count):
                count = snap_count_from_producer

        if count is None and isinstance(parent, dict):
            parent_count = _extract_bound_input_count(parent, as_of=resolved_as_of)
            if parent_count is not None:
                # Never borrow a feature count from an unrelated/unbound parent snapshot.
                map_sid = _extract_snapshot_id(mapping)
                parent_sid = _extract_snapshot_id(parent, allow_bare_id=False)
                map_hash = _extract_snapshot_hash(mapping)
                parent_label = _extract_regime_label(parent)
                if (
                    map_sid is not None
                    and parent_sid is not None
                    and map_sid == parent_sid
                    and (map_hash is None or _snapshot_hash_matches(parent, map_hash))
                    and parent_label is not None
                    and parent_label.upper() == label.upper()
                    and (resolved_as_of is None or not _check_entry_clock_sanity(parent, "parent_snapshot", "model", resolved_as_of))
                ):
                    count = parent_count
                else:
                    return {
                        "status": "mismatch",
                        "label": label,
                        "count": None,
                        "reason": "unrelated_parent_snapshot",
                    }

        if count is not None and count < MIN_REGIME_BOUND_INPUTS:
            return {
                "status": "insufficient",
                "label": label,
                "count": count,
                "reason": f"regime_bound_inputs_below_minimum:{count}<{MIN_REGIME_BOUND_INPUTS}",
            }
        if count is None and unavailable_label is None:
            unavailable_label = label
        return None

    for _, mapping, parent in explicit_containers:
        verdict = _inspect_one(mapping, parent, explicit_regime_container=True)
        if verdict is not None:
            return verdict

    for root in roots:
        verdict = _inspect_one(root, None, explicit_regime_container=False)
        if verdict is not None:
            return verdict

    if unavailable_label is not None:
        return {"status": "unavailable", "label": unavailable_label, "count": None, "reason": None}
    return {"status": "ok", "label": None, "count": None, "reason": None}


def inspect_regime_bound_inputs(row: dict[str, Any], as_of: datetime | None = None) -> tuple[str, str | None, int | None]:
    """Read-only inspection of any carried regime label's bound-input count.

    Returns:
        (``"insufficient"``, label, count) when a carried regime label has a
            distinct eligible bound-input count < MIN_REGIME_BOUND_INPUTS;
        (``"mismatch"``, label, None) when a carried regime container has a
            missing label, unrelated/mismatched producer snapshot, or invalid clock;
        (``"unavailable"``, label, None) when a carried regime label has no
            available bound-input count;
        (``"ok"``, None, None) when no regime label is asserted or every
            asserted regime label meets MIN_REGIME_BOUND_INPUTS.
    """
    info = inspect_regime_binding(row, as_of=as_of)
    return info["status"], info["label"], info["count"]


def check_regime_bound_inputs(row: dict[str, Any], as_of: datetime | None = None) -> tuple[str, int] | None:
    """Read-only check: return (label, bound_count) if a carried regime label was
    computed from fewer than MIN_REGIME_BOUND_INPUTS bound inputs and that count
    is available in *row*; otherwise None."""
    status, label, count = inspect_regime_bound_inputs(row, as_of=as_of)
    if status == "insufficient" and label is not None and count is not None:
        return label, count
    return None


def _parse_observed_clock(value: Any) -> tuple[date | None, datetime | None, str | None, bool]:
    """Parse an ISO date or timestamp without mutating input.

    Returns ``(date, utc_dt, raw_str, is_date_only)``.
    """
    if isinstance(value, datetime):
        dt = clock(value)
        return dt.date(), dt, iso(dt), False
    if isinstance(value, date):
        return value, datetime.combine(value, time(), timezone.utc), value.isoformat(), True
    if not isinstance(value, str) or not value.strip():
        return None, None, None, False
    raw = value.strip()
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        try:
            d = date.fromisoformat(raw)
            return d, datetime.combine(d, time(), timezone.utc), d.isoformat(), True
        except ValueError:
            return None, None, None, False
    try:
        dt = clock(raw)
        return dt.date(), dt, iso(dt), False
    except ValueError:
        return None, None, None, False


def _resolve_item_max_age(item: dict[str, Any], category: str, name: str | None) -> tuple[float, str] | None:
    """Resolve (limit, unit) from explicit per-item max-age fields or category defaults."""
    for key, unit in (("max_age_seconds", "seconds"), ("max_age_hours", "hours"), ("max_age_days", "days")):
        val = item.get(key)
        if isinstance(val, (int, float)) and not isinstance(val, bool) and math.isfinite(val) and val >= 0:
            return float(val), unit
    nested = item.get("max_age")
    if isinstance(nested, dict):
        unit = nested.get("unit")
        limit = nested.get("limit")
        if unit in ("days", "hours", "seconds") and isinstance(limit, (int, float)) and not isinstance(limit, bool) and math.isfinite(limit) and limit >= 0:
            return float(limit), unit
        for unit in ("seconds", "hours", "days"):
            val = nested.get(unit)
            if isinstance(val, (int, float)) and not isinstance(val, bool) and math.isfinite(val) and val >= 0:
                return float(val), unit
    if category == "macro" and isinstance(name, str):
        upper = name.upper()
        if upper in DAILY_MACRO_SERIES:
            return 5.0, "days"
        if upper in MONTHLY_MACRO_SERIES:
            return 70.0, "days"
    default_days = DEFAULT_MAX_AGE_DAYS.get(category)
    if default_days is not None:
        return float(default_days), "days"
    return None


def _check_entry_clock_sanity(
    item: dict[str, Any],
    path: str,
    kind: str,
    as_of: datetime,
) -> list[dict[str, Any]]:
    """Reject future timestamps (later than *as_of*) or negative/non-finite ages.

    Never trusts a claimed ``age_days`` or ``freshness.status == 'fresh'`` over
    an actual timestamp.
    """
    violations: list[dict[str, Any]] = []
    as_of_utc = clock(as_of)
    as_of_iso = iso(as_of_utc)
    as_of_date = as_of_utc.date()

    # 1. Check freshness block age
    fresh = item.get("freshness")
    if isinstance(fresh, dict) and "age" in fresh and fresh["age"] is not None:
        f_age = fresh["age"]
        if (
            isinstance(f_age, bool)
            or not isinstance(f_age, (int, float))
            or not math.isfinite(f_age)
            or f_age < 0
        ):
            violations.append({
                "path": path, "kind": kind, "status": "rejected", "reason": "negative_age",
                "field": "freshness.age", "observed_at": str(item.get("obs_date") or item.get("date") or "unknown"),
                "as_of": as_of_iso, "age": f_age, "unit": fresh.get("unit") or "days",
            })

    # 2. Check explicit numeric age fields
    for age_key, age_unit in _NUMERIC_AGE_KEYS:
        if age_key in item and item[age_key] is not None:
            raw_age = item[age_key]
            if (
                isinstance(raw_age, bool)
                or not isinstance(raw_age, (int, float))
                or not math.isfinite(raw_age)
                or raw_age < 0
            ):
                violations.append({
                    "path": path, "kind": kind, "status": "rejected", "reason": "negative_age",
                    "field": age_key, "observed_at": str(item.get("obs_date") or item.get("date") or "unknown"),
                    "as_of": as_of_iso, "age": raw_age, "unit": age_unit,
                })

    # 3. Check stale_features map for negative ages
    stale_feat_map = item.get("stale_features")
    if isinstance(stale_feat_map, dict):
        for fname, age_d in sorted(stale_feat_map.items()):
            if age_d is not None and (
                isinstance(age_d, bool)
                or not isinstance(age_d, (int, float))
                or not math.isfinite(age_d)
                or age_d < 0
            ):
                violations.append({
                    "path": f"{path}.stale_features.{fname}", "kind": "input",
                    "status": "rejected", "reason": "negative_age",
                    "field": f"stale_features.{fname}", "observed_at": "unknown",
                    "as_of": as_of_iso, "age": age_d, "unit": "days",
                })

    # 4. Check ALL observed/availability timestamps against the decision clock
    for ts_key in _OBSERVED_TIMESTAMP_KEYS:
        if ts_key not in item or item[ts_key] is None:
            continue
        raw_ts = item[ts_key]
        if isinstance(raw_ts, str) and raw_ts.strip().lower() in _INVALID_FEATURE_ID_STRINGS:
            continue
        obs_date, obs_dt, obs_str, is_date_only = _parse_observed_clock(raw_ts)
        if obs_date is None or obs_dt is None:
            continue
        if is_date_only:
            if obs_date > as_of_date:
                violations.append({
                    "path": path, "kind": kind, "status": "rejected", "reason": "future_timestamp",
                    "field": ts_key, "observed_at": obs_str, "as_of": as_of_iso,
                    "age": (as_of_date - obs_date).days, "unit": "days",
                })
        elif obs_dt > as_of_utc:
            violations.append({
                "path": path, "kind": kind, "status": "rejected", "reason": "future_timestamp",
                "field": ts_key, "observed_at": obs_str, "as_of": as_of_iso,
                "age": round((as_of_utc - obs_dt).total_seconds(), 3), "unit": "seconds",
            })

    return violations


def find_invalid_clock_items(row: dict[str, Any], as_of: datetime) -> list[dict[str, Any]]:
    """Read-only scan for any carried input, model, or regime with a future timestamp or negative age."""
    if not isinstance(row, dict):
        return []
    violations: list[dict[str, Any]] = []
    input_categories = (
        "equities", "equity_volume", "rates", "credit", "volatility",
        "commodities", "fx", "equity_etf_proxies", "macro", "features",
        "options", "inputs",
    )
    model_categories = ("models", "model_states")
    regime_categories = ("latest_regime", "regime", "regime_detection")

    def _scan_container(container: dict[str, Any], prefix: str) -> None:
        for cat in input_categories:
            group = container.get(cat)
            if isinstance(group, dict):
                for name, entry in group.items():
                    if isinstance(entry, dict):
                        violations.extend(_check_entry_clock_sanity(
                            entry, f"{prefix}{cat}.{name}", "input", as_of,
                        ))
        for cat in model_categories:
            group = container.get(cat)
            if isinstance(group, dict):
                for name, entry in group.items():
                    if isinstance(entry, dict):
                        violations.extend(_check_entry_clock_sanity(
                            entry, f"{prefix}{cat}.{name}", "model", as_of,
                        ))
        for cat in regime_categories:
            entry = container.get(cat)
            if isinstance(entry, dict):
                violations.extend(_check_entry_clock_sanity(
                    entry, f"{prefix}{cat}", "model", as_of,
                ))
                for snap_key in ("producer_snapshot", "analytical_snapshot"):
                    psnap = entry.get(snap_key)
                    if isinstance(psnap, dict):
                        violations.extend(_check_entry_clock_sanity(
                            psnap, f"{prefix}{cat}.{snap_key}", "model", as_of,
                        ))

    snap = row.get("snapshot_data")
    if isinstance(snap, dict):
        _scan_container(snap, "snapshot_data.")
    _scan_container(row, "")

    moves = row.get("market_moves")
    if isinstance(moves, dict):
        indices = moves.get("indices")
        if isinstance(indices, dict):
            for name, entry in indices.items():
                if isinstance(entry, dict):
                    violations.extend(_check_entry_clock_sanity(
                        entry, f"market_moves.indices.{name}", "input", as_of,
                    ))
        for list_key in ("notable", "sector_leaders", "sector_laggards"):
            items = moves.get(list_key)
            if isinstance(items, list):
                for idx, entry in enumerate(items):
                    if isinstance(entry, dict):
                        label = str(entry.get("asset") or entry.get("etf") or entry.get("sector") or idx)
                        violations.extend(_check_entry_clock_sanity(
                            entry, f"market_moves.{list_key}.{label}", "input", as_of,
                        ))
    return violations


def _check_entry_staleness(
    item: dict[str, Any],
    path: str,
    category: str,
    kind: str,
    as_of: datetime,
    name: str | None = None,
) -> list[dict[str, Any]]:
    """Read-only staleness check for one carried input or model dict.

    Always verifies observed timestamps against *as_of* rather than trusting a
    claimed ``age_days`` or ``freshness.status == 'fresh'`` to hide stale data.
    """
    findings: list[dict[str, Any]] = []
    as_of_utc = clock(as_of)
    as_of_iso = iso(as_of_utc)
    as_of_date = as_of_utc.date()

    # 1. Explicit freshness block (e.g. market_state.py shape) — can mark stale,
    #    never short-circuits past timestamp verification when claiming fresh.
    fresh = item.get("freshness")
    if isinstance(fresh, dict):
        f_status = fresh.get("status")
        f_age = fresh.get("age")
        f_limit = fresh.get("limit")
        f_unit = fresh.get("unit") or "days"
        over_limit = (
            isinstance(f_age, (int, float)) and not isinstance(f_age, bool) and math.isfinite(f_age)
            and isinstance(f_limit, (int, float)) and not isinstance(f_limit, bool) and math.isfinite(f_limit)
            and f_age > f_limit
        )
        if f_status == "stale" or over_limit:
            findings.append({
                "path": path, "kind": kind, "status": "stale",
                "observed_at": str(item.get("obs_date") or item.get("date") or "unknown"),
                "as_of": as_of_iso, "age": f_age, "max_age": f_limit, "unit": f_unit,
            })
            return findings

    # 2. Explicit stale_features / stale_dimensions lists (auto_regime / state_vector)
    stale_feat_map = item.get("stale_features")
    if isinstance(stale_feat_map, dict):
        limit_days = item.get("max_age_days", DEFAULT_MAX_AGE_DAYS.get(category, 5))
        for fname, age_d in sorted(stale_feat_map.items()):
            findings.append({
                "path": f"{path}.stale_features.{fname}", "kind": "input", "status": "stale",
                "observed_at": "unknown", "as_of": as_of_iso, "age": age_d,
                "max_age": limit_days, "unit": "days",
            })
    for dims_key in ("stale_dimensions", "stale_dims"):
        dims = item.get(dims_key)
        if isinstance(dims, list):
            for dname in dims:
                findings.append({
                    "path": f"{path}.{dims_key}.{dname}", "kind": "input", "status": "stale",
                    "observed_at": "unknown", "as_of": as_of_iso, "age": None,
                    "max_age": None, "unit": "days",
                })

    max_spec = _resolve_item_max_age(item, category, name)
    if max_spec is None:
        return findings
    limit, unit = max_spec

    # 3. Direct non-negative numeric age fields (never trusted over an older timestamp)
    explicit_age: float | None = None
    for age_key, age_unit in _NUMERIC_AGE_KEYS:
        raw_age = item.get(age_key)
        if isinstance(raw_age, (int, float)) and not isinstance(raw_age, bool) and math.isfinite(raw_age) and raw_age >= 0:
            if age_unit == unit:
                explicit_age = float(raw_age)
                break
            if age_unit == "days" and unit == "hours":
                explicit_age = float(raw_age) * 24.0
                break
            if age_unit == "days" and unit == "seconds":
                explicit_age = float(raw_age) * 86400.0
                break
            if age_unit == "hours" and unit == "days":
                explicit_age = float(raw_age) / 24.0
                break
            if age_unit == "seconds" and unit == "days":
                explicit_age = float(raw_age) / 86400.0
                break

    obs_date: date | None = None
    obs_dt: datetime | None = None
    obs_str: str | None = None
    for ts_key in _OBSERVED_TIMESTAMP_KEYS:
        if ts_key in item and item[ts_key] is not None:
            obs_date, obs_dt, obs_str, _ = _parse_observed_clock(item[ts_key])
            if obs_date is not None:
                break

    timestamp_age: float | None = None
    if obs_date is not None and obs_dt is not None:
        if unit == "days":
            timestamp_age = float((as_of_date - obs_date).days)
        elif unit == "hours":
            timestamp_age = round((as_of_utc - obs_dt).total_seconds() / 3600.0, 3)
        elif unit == "seconds":
            timestamp_age = round((as_of_utc - obs_dt).total_seconds(), 3)

    if timestamp_age is not None and explicit_age is not None:
        computed_age: float | None = max(timestamp_age, explicit_age)
    elif timestamp_age is not None:
        computed_age = timestamp_age
    else:
        computed_age = explicit_age

    if computed_age is not None and computed_age > limit:
        age_out: int | float = int(computed_age) if unit == "days" and computed_age.is_integer() else computed_age
        limit_out: int | float = int(limit) if limit.is_integer() else limit
        findings.append({
            "path": path, "kind": kind, "status": "stale",
            "observed_at": obs_str or "unknown", "as_of": as_of_iso,
            "age": age_out, "max_age": limit_out, "unit": unit,
        })
    return findings


def find_stale_carried_items(row: dict[str, Any], as_of: datetime) -> list[dict[str, Any]]:
    """Read-only scan of carried inputs and models in *row* older than their max age."""
    if not isinstance(row, dict):
        return []
    stale: list[dict[str, Any]] = []
    input_categories = (
        "equities", "equity_volume", "rates", "credit", "volatility",
        "commodities", "fx", "equity_etf_proxies", "macro", "features",
        "options", "inputs",
    )
    model_categories = ("models", "model_states")
    regime_categories = ("latest_regime", "regime", "regime_detection")

    def _scan_container(container: dict[str, Any], prefix: str) -> None:
        for cat in input_categories:
            group = container.get(cat)
            if isinstance(group, dict):
                for name, entry in group.items():
                    if isinstance(entry, dict):
                        stale.extend(_check_entry_staleness(
                            entry, f"{prefix}{cat}.{name}", cat, "input", as_of, name=str(name),
                        ))
        for cat in model_categories:
            group = container.get(cat)
            if isinstance(group, dict):
                for name, entry in group.items():
                    if isinstance(entry, dict):
                        stale.extend(_check_entry_staleness(
                            entry, f"{prefix}{cat}.{name}", cat, "model", as_of, name=str(name),
                        ))
        for cat in regime_categories:
            entry = container.get(cat)
            if isinstance(entry, dict):
                stale.extend(_check_entry_staleness(
                    entry, f"{prefix}{cat}", cat, "model", as_of,
                ))

    snap = row.get("snapshot_data")
    if isinstance(snap, dict):
        _scan_container(snap, "snapshot_data.")
    _scan_container(row, "")

    moves = row.get("market_moves")
    if isinstance(moves, dict):
        indices = moves.get("indices")
        if isinstance(indices, dict):
            for name, entry in indices.items():
                if isinstance(entry, dict):
                    stale.extend(_check_entry_staleness(
                        entry, f"market_moves.indices.{name}", "market_moves", "input", as_of, name=str(name),
                    ))
        for list_key in ("notable", "sector_leaders", "sector_laggards"):
            items = moves.get(list_key)
            if isinstance(items, list):
                for idx, entry in enumerate(items):
                    if isinstance(entry, dict):
                        label = str(entry.get("asset") or entry.get("etf") or entry.get("sector") or idx)
                        stale.extend(_check_entry_staleness(
                            entry, f"market_moves.{list_key}.{label}", "market_moves", "input", as_of, name=label,
                        ))
    return stale


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
        "hypotheses": [], "stale_items": [], "notices": [], "blockers": [
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
            packet["blockers"].append(f"{identity}: stored generator context exceeds byte cap; omitted "
                                      "(claim-guard verdict carried separately in publication_claim_guard)")
        if not isinstance(content, str) or not content.strip():
            packet["blockers"].append(f"{identity}: original text missing; no invented replacement")
            packet["capture_receipt"]["omitted_publication_ids"].append(identity)
            continue
        text_bytes = content.encode("utf-8")
        text_sha = sha256(text_bytes)
        # Screen the WHOLE canonical row: source_record (snapshot_data, grounding, etc.) enters packet.json too.
        reasons = screen_row(row)
        disposition = None
        regime_info = inspect_regime_binding(row, as_of=published)
        regime_status = regime_info["status"]
        regime_label = regime_info["label"]
        bound_count = regime_info["count"]
        clock_violations = find_invalid_clock_items(row, published)
        if reasons:
            disposition = "QUARANTINED_SCREEN_HIT"
        elif len(text_bytes) > TEXT_FIELD_CAP_BYTES:
            disposition = "EXCLUDED_TEXT_OVER_FIELD_CAP"
        elif aggregate + len(text_bytes) > PUBLICATIONS_AGGREGATE_CAP_BYTES:
            disposition = "EXCLUDED_AGGREGATE_CAP_REACHED"
        elif regime_status == "insufficient" and regime_label is not None and bound_count is not None:
            disposition = "REFUSED_INSUFFICIENT_REGIME_INPUTS"
            reasons = [f"regime_bound_inputs_below_minimum:{bound_count}<{MIN_REGIME_BOUND_INPUTS}"]
            packet["blockers"].append(
                f"{identity}: refused to carry regime label {regime_label!r} computed from "
                f"{bound_count} bound inputs (minimum {MIN_REGIME_BOUND_INPUTS})"
            )
        elif regime_status == "mismatch":
            disposition = "REFUSED_REGIME_BINDING_MISMATCH"
            mismatch_reason = regime_info["reason"] or "regime_binding_mismatch"
            reasons = [mismatch_reason]
            log.warning("publication-packet: refused {} due to regime binding mismatch: {}", identity, mismatch_reason)
            packet["blockers"].append(
                f"{identity}: refused carried regime ({mismatch_reason})"
            )
        elif clock_violations:
            disposition = "REFUSED_INVALID_SOURCE_CLOCK"
            reasons = [f"{v['reason']}:{v['path']}" for v in clock_violations]
            log.warning("publication-packet: refused {} due to invalid source clock/age: {}", identity, reasons)
            packet["blockers"].append(
                f"{identity}: refused due to invalid source clock/age ({', '.join(reasons)})"
            )
        if disposition:
            exclusions.append({"publication_id": identity, "channel": channel, "disposition": disposition,
                               "reasons": reasons, "text_sha256": text_sha, "text_utf8_bytes": len(text_bytes),
                               "source_row_sha256": sha256(canonical(row))})
            packet["capture_receipt"]["quarantined_publication_ids"].append(identity)
            continue
        aggregate += len(text_bytes)
        pub_entry: dict[str, Any] = {
            "id": identity, "source": channel, "published_at": iso(published), "text": content,
            "sha256": text_sha, "claims_complete": False, "source_record": row,
        }
        if regime_status == "unavailable":
            pub_entry["notices"] = [REGIME_BOUND_INPUT_UNAVAILABLE_NOTICE]
            packet["notices"].append(f"{identity}: {REGIME_BOUND_INPUT_UNAVAILABLE_NOTICE}")
        stale_items = find_stale_carried_items(row, published)
        if stale_items:
            pub_entry["stale"] = True
            pub_entry["stale_items"] = stale_items
            packet["stale_items"].extend(
                {"publication_id": identity, **entry} for entry in stale_items
            )
        packet["publications"].append(pub_entry)
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


def publish_screened(day_dir: Path, public_dir: Path) -> dict[str, Any]:
    """Copy the screened files of one day to a world-readable tree; never capture.json.

    Allow-list, not deny-list: only PUBLIC_FILES, PUBLIC_DIRS/*.json and PUBLIC_GLOBS are
    copied, so a new private file in day_dir can never leak by omission. Bytes are copied
    unchanged (receipt hashes stay verifiable in both trees), dirs 0755, files 0644, and a
    file already identical in the public tree is left alone. Idempotent; safe on re-run.
    """
    copied, unchanged = [], []
    candidates: list[Path] = []
    for name in PUBLIC_FILES:
        if (day_dir / name).is_file():
            candidates.append(day_dir / name)
    for sub in PUBLIC_DIRS:
        if (day_dir / sub).is_dir():
            candidates.extend(sorted(path for path in (day_dir / sub).glob("*.json") if path.is_file()))
    for pattern in PUBLIC_GLOBS:
        candidates.extend(sorted(path for path in day_dir.glob(pattern) if path.is_file()))
    for src in candidates:
        rel = src.relative_to(day_dir)
        if rel.parts in {(name,) for name in NEVER_PUBLIC}:  # top-level capture.json only; receipts/capture.json is a stage receipt
            raise RuntimeError(f"refusing to publish private file {rel}")
        data = src.read_bytes()
        dst = public_dir / rel
        if dst.is_file() and dst.read_bytes() == data:
            unchanged.append(str(rel))
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(dst.parent, 0o755)
        write_file(dst, data, 0o644)
        copied.append(str(rel))
    if public_dir.exists():
        os.chmod(public_dir, 0o755)
    return {"public_dir": str(public_dir), "copied": copied, "unchanged": unchanged}


def publish_or_warn(day_dir: Path, public_root: Path | None, day: date) -> dict[str, Any] | None:
    """Publishing never blocks production: a failure is logged and reported, not raised."""
    if public_root is None:
        return None
    public_dir = public_root.expanduser() / day.isoformat()
    try:
        public_root.expanduser().mkdir(parents=True, exist_ok=True)
        os.chmod(public_root.expanduser(), 0o755)
        published = publish_screened(day_dir, public_dir)
    except OSError as exc:
        log.warning("publication-packet {}: public copy to {} failed: {}", day, public_dir, type(exc).__name__)
        return {"public_dir": str(public_dir), "error": type(exc).__name__}
    if published["copied"]:
        log.info("publication-packet {}: published {} screened file(s) to {}", day, len(published["copied"]), public_dir)
    return published


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
            audio_dir: Path = AUDIO_DIR, grounding_dir: Path = GROUNDING_DIR,
            public_root: Path | None = None) -> dict[str, Any]:
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
        published = None if dry_run else publish_or_warn(day_dir, public_root, day)
        return dict(existing, already_present=True, published=published)

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
        published = None if dry_run else publish_or_warn(day_dir, public_root, day)
        return dict(failure, observation=obs, published=published)

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
    published = None if dry_run else publish_or_warn(day_dir, public_root, day)
    log.info("publication-packet {}: {} publications projected, {} quarantined, diary_run={}, packet {}{}",
             day, summary["publications_projected"], summary["quarantined"], diary_state, packet_sha[:12],
             " [dry-run]" if dry_run else "")
    return dict(summary, observation=obs, day_dir=str(day_dir), published=published)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--day", default=None, help="completed UTC publication day (YYYY-MM-DD); default: yesterday UTC")
    p.add_argument("--out-root", type=Path, default=DEFAULT_ROOT,
                   help="packet root (default $GRID_PUBLICATION_PACKET_ROOT or ~/research/publication-packets)")
    p.add_argument("--limit", type=int, default=ROW_LIMIT_MAX, help="rows per channel, 1..500")
    p.add_argument("--due-hours", type=float, default=DEFAULT_DUE_HOURS,
                   help="hours after the window closes by which the day should be closed (observation due_at)")
    p.add_argument("--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT,
                   help="world-readable tree for the screened files, never capture.json "
                        "(default $GRID_PUBLICATION_PACKET_PUBLIC_ROOT or ~/research/publication-packets-public)")
    p.add_argument("--no-publish", action="store_true", help="skip the public screened copy")
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
                         dry_run=args.dry_run, now=now, public_root=None if args.no_publish else args.public_root)
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
