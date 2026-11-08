#!/usr/bin/env python3
"""Ingest the machine-readable daily review (review.jsonl) into griddb.

The daily review of GRID's own publications is a file, not a document:
``publication-review-verdicts-v1`` -- one header line, then one ``claim_verdict``
per graded claim. Every row names a publication from that day's packet by id and
text hash, quotes a verbatim span of the publication, and states a verdict with
evidence. This job is the consumer side of docs/runbooks/DAILY-PUBLICATION-PACKET.md:

1. read ``<review_root>/<D>/review.jsonl`` and ``<packet_root>/<D>/{receipt.json,packet.json}``;
2. refuse the whole file unless header.packet_sha256 equals the producer's
   receipt.json.packet_sha256 and packet.json hashes to it (the grader read the
   same bytes the producer wrote);
3. accept each row only if its publication is in the packet, its publication_sha256
   matches, its claim_span is a verbatim substring of that publication's text and
   hashes to claim_sha256, its enums are legal, its evidence is concrete and dated
   no later than grading, and its horizon is between the claim and the grading;
   everything else is rejected with a reason and the rest of the file still ingests;
4. one transaction: a ``publication_review_runs`` row (UNIQUE review_sha256 -> a
   re-run is a no-op; ``supersedes_run_id`` links a corrected file to the day's
   previous run) and one ``publication_claim_verdicts`` row per accepted claim;
5. write ``<review_root>/<D>/ingest-receipt.json``.

The file comes from another agent and is treated as hostile input: regular-file
and size checks before reading, per-row isolation (a row that cannot be processed
is rejected, never fatal), no NUL / NaN / lone-surrogate content reaches the
driver, receipts are written through O_EXCL temp files (no symlink following).
It never reads raw_series, never updates a row (both tables are append-only by
trigger), never writes thesis tables (``thesis_postmortems`` is reserved for
``intelligence/thesis_tracker.py``'s wrong-thesis analysis and is under its
scoring hold), and never calls a provider.

Exit 0 = ingested or already present, 1 = refused/failed (nothing written),
3 = ingested with rejected rows or declared-count mismatch (see the receipt).
Without ``--day`` the job scans the last ``--lookback`` days for a review.jsonl
that has no ingest-receipt.json yet, so a late-delivered file is picked up.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

JOB = "ingest_publication_review"
REVIEW_SCHEMA = "publication-review-verdicts-v1"
RECEIPT_SCHEMA = "publication-review-ingest-receipt-v1"
DEFAULT_REVIEW_ROOT = Path(os.environ.get("GRID_PUBLICATION_REVIEW_ROOT",
                                          "~/research/publication-reviews")).expanduser()
DEFAULT_PACKET_ROOT = Path(os.environ.get("GRID_PUBLICATION_PACKET_ROOT",
                                          "~/research/publication-packets")).expanduser()
DEFAULT_LOOKBACK_DAYS = 3
MAX_FILE_BYTES = 8_000_000
MIN_SPAN_CHARS, MAX_SPAN_CHARS = 8, 300
MAX_EVIDENCE_ITEMS = 20
MAX_EVIDENCE_BYTES = 20_000
MAX_TEXT_CHARS = 2_000          # root_cause, lesson
MAX_SHORT_CHARS = 200           # evidence.source, grader.model
MAX_OBSERVED_CHARS = 500
MAX_URL_CHARS = 500
MAX_INSTRUMENT_CHARS = 32
MAX_HEADER_JSON_BYTES = 4_000   # grader, adjudicator
MAX_JSON_DEPTH = 4
MAX_REJECTED_RECORDED = 200
INT4_MAX = 2**31 - 1
GRADED_AT_FUTURE_SLACK = timedelta(minutes=5)
CLAIM_TYPES = frozenset({"direction", "level", "event", "causal", "data_quality"})
VERDICTS = frozenset({"confirmed", "refuted", "partially_confirmed", "unresolved", "unscorable"})
RESOLVED = frozenset({"confirmed", "refuted", "partially_confirmed"})
DIRECTIONS = frozenset({"bullish", "bearish", "neutral"})
HEX64 = re.compile(r"[0-9a-f]{64}")
PUBLICATION_ID = re.compile(r"(market_briefings|thesis_snapshots|market_diary):([0-9]{1,9})")

INSERT_RUN = text("""
    INSERT INTO publication_review_runs
        (publication_day, review_sha256, packet_sha256, schema_version, grader, adjudicator,
         graded_at, declared_total, declared_graded, declared_unscorable,
         accepted_rows, rejected_rows, supersedes_run_id, source_path)
    VALUES
        (:publication_day, :review_sha256, :packet_sha256, :schema_version,
         CAST(:grader AS jsonb), CAST(:adjudicator AS jsonb),
         :graded_at, :declared_total, :declared_graded, :declared_unscorable,
         :accepted_rows, :rejected_rows, :supersedes_run_id, :source_path)
    ON CONFLICT (review_sha256) DO NOTHING
    RETURNING id
""")
INSERT_VERDICT = text("""
    INSERT INTO publication_claim_verdicts
        (run_id, publication_day, channel, publication_id, source_row_id, publication_sha256,
         stated_at, claim_span, claim_sha256, claim_type, instrument, horizon_end, verdict,
         evidence, knowable_at_stated_at, thesis_direction, actual_direction,
         root_cause, lesson, grader_confidence)
    VALUES
        (:run_id, :publication_day, :channel, :publication_id, :source_row_id, :publication_sha256,
         :stated_at, :claim_span, :claim_sha256, :claim_type, :instrument, :horizon_end, :verdict,
         CAST(:evidence AS jsonb), :knowable_at_stated_at, :thesis_direction, :actual_direction,
         :root_cause, :lesson, :grader_confidence)
""")
EXISTING_RUN = text("SELECT id FROM publication_review_runs WHERE review_sha256 = :sha")
LATEST_RUN_FOR_DAY = text("""
    SELECT id FROM publication_review_runs
    WHERE publication_day = :publication_day
    ORDER BY ingested_at DESC, id DESC
    LIMIT 1
""")


# ── primitives ──────────────────────────────────────────────────────────────

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_ts(value: Any) -> datetime | None:
    """Aware ISO-8601 -> UTC datetime; None for anything else (never raises)."""
    if not isinstance(value, str) or not value or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError, TypeError):
        return None


class Refused(Exception):
    """The whole file is unusable; nothing is written."""


class BadValue(ValueError):
    """One field of one row is unusable; the row is rejected."""


def safe_text(value: Any, limit: int, *, what: str, required: bool = False) -> str | None:
    """A str the driver and jsonb accept: no NUL, encodable UTF-8, within ``limit`` chars."""
    if value is None or (isinstance(value, str) and not value.strip()):
        if required:
            raise BadValue(f"{what} missing or blank")
        return None
    if not isinstance(value, str):
        raise BadValue(f"{what} must be a string")
    if len(value) > limit:
        raise BadValue(f"{what} longer than {limit} chars")
    if "\x00" in value:
        raise BadValue(f"{what} contains NUL")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise BadValue(f"{what} is not encodable UTF-8") from exc
    return value


def _check_json_tree(value: Any, depth: int, what: str) -> None:
    if depth > MAX_JSON_DEPTH:
        raise BadValue(f"{what} nested deeper than {MAX_JSON_DEPTH}")
    if isinstance(value, dict):
        for key, item in value.items():
            safe_text(key, MAX_SHORT_CHARS, what=f"{what} key")
            _check_json_tree(item, depth + 1, what)
    elif isinstance(value, list):
        for item in value:
            _check_json_tree(item, depth + 1, what)
    elif isinstance(value, str):
        safe_text(value, MAX_FILE_BYTES, what=what)
    elif isinstance(value, float) and not math.isfinite(value):
        raise BadValue(f"{what} contains NaN or infinity")
    elif value is not None and not isinstance(value, (bool, int, float)):
        raise BadValue(f"{what} has an unsupported JSON type")


def safe_json(value: Any, limit_bytes: int, *, what: str) -> str:
    """Canonical JSON text that Postgres jsonb accepts, bounded in size and depth."""
    _check_json_tree(value, 0, what)
    try:
        encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise BadValue(f"{what} is not serialisable JSON") from exc
    if len(encoded.encode("utf-8")) > limit_bytes:
        raise BadValue(f"{what} larger than {limit_bytes} bytes")
    return encoded


def enum(value: Any, allowed: frozenset[str], *, what: str, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or value not in allowed:
        raise BadValue(f"{what} not in {'|'.join(sorted(allowed))}")
    return value


def int4_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 <= value <= INT4_MAX else None


def _short(value: Any, limit: int = 80) -> str:
    rendered = value if isinstance(value, str) else repr(value)
    return rendered if len(rendered) <= limit else rendered[: limit - 1] + "…"


# ── inputs ──────────────────────────────────────────────────────────────────

def read_regular_file(path: Path, limit: int) -> bytes:
    """Open without following a symlink, require a regular file within ``limit`` bytes."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
    except FileNotFoundError as exc:
        raise Refused(f"{path} not present") from exc
    except OSError as exc:
        raise Refused(f"{path} not readable as a regular file ({type(exc).__name__})") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise Refused(f"{path} is not a regular file")
        if info.st_size > limit:
            raise Refused(f"{path} is {info.st_size} bytes > {limit}")
        chunks, remaining = [], limit + 1
        while remaining > 0:
            chunk = os.read(fd, min(remaining, 1 << 20))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    finally:
        os.close(fd)
    raw = b"".join(chunks)
    if len(raw) > limit:
        raise Refused(f"{path} grew past {limit} bytes while reading")
    return raw


@dataclass
class Packet:
    sha256: str
    publications: dict[str, dict[str, Any]]
    unusable_records: list[dict[str, str]]


def load_packet(packet_root: Path, day: date) -> Packet:
    folder = packet_root / day.isoformat()
    receipt_path, packet_path = folder / "receipt.json", folder / "packet.json"
    if not receipt_path.exists() or not packet_path.exists():
        raise Refused(f"packet for {day} not present under {folder} (receipt.json/packet.json)")
    try:
        receipt = json.loads(read_regular_file(receipt_path, MAX_FILE_BYTES).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Refused(f"receipt.json unreadable: {type(exc).__name__}") from exc
    expected = receipt.get("packet_sha256") if isinstance(receipt, dict) else None
    if not isinstance(expected, str) or not HEX64.fullmatch(expected):
        raise Refused("receipt.json has no packet_sha256")
    packet_bytes = read_regular_file(packet_path, MAX_FILE_BYTES)
    actual = sha256(packet_bytes)
    if actual != expected:
        raise Refused(f"packet.json sha256 {actual[:12]} != receipt packet_sha256 {expected[:12]}")
    try:
        packet = json.loads(packet_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Refused(f"packet.json unreadable: {type(exc).__name__}") from exc
    if not isinstance(packet, dict) or packet.get("publication_day") != day.isoformat():
        raise Refused(f"packet publication_day != {day}")
    publications: dict[str, dict[str, Any]] = {}
    unusable: list[dict[str, str]] = []
    for pub in packet.get("publications") or []:
        pid = pub.get("id") if isinstance(pub, dict) else None
        if not isinstance(pid, str) or not PUBLICATION_ID.fullmatch(pid):
            unusable.append({"id": _short(pid), "reason": "id malformed"})
            continue
        text_value, declared = pub.get("text"), pub.get("sha256")
        if not isinstance(text_value, str) or not isinstance(declared, str) or not HEX64.fullmatch(declared) \
                or sha256(text_value.encode("utf-8", "surrogatepass")) != declared:
            unusable.append({"id": pid, "reason": "text/sha256 inconsistent in packet"})
            continue
        if parse_ts(pub.get("published_at")) is None:
            unusable.append({"id": pid, "reason": "published_at unparseable in packet"})
            continue
        publications[pid] = pub
    return Packet(sha256=expected, publications=publications, unusable_records=unusable)


def read_lines(path: Path) -> tuple[bytes, list[Any]]:
    raw = read_regular_file(path, MAX_FILE_BYTES)
    lines: list[Any] = []
    for number, line in enumerate(raw.split(b"\n"), start=1):
        if number == 1 and line.startswith(b"\xef\xbb\xbf"):
            line = line[3:]
        line = line.rstrip(b"\r")
        if not line.strip():
            continue
        try:
            lines.append(json.loads(line.decode("utf-8")))
        except UnicodeDecodeError as exc:
            raise Refused(f"line {number} is not UTF-8") from exc
        except json.JSONDecodeError as exc:
            raise Refused(f"line {number} is not JSON: {exc.msg}") from exc
    if not lines:
        raise Refused("review.jsonl is empty")
    return raw, lines


@dataclass
class Header:
    raw: dict[str, Any]
    graded_at: datetime
    grader_json: str
    adjudicator_json: str | None
    declared_total: int | None
    declared_graded: int | None
    declared_unscorable: int | None
    notes: list[str] = field(default_factory=list)


def validate_header(header: Any, day: date, packet_sha: str, now: datetime) -> Header:
    if not isinstance(header, dict) or header.get("kind") != "review_header":
        raise Refused("line 1 is not a review_header")
    if header.get("schema") != REVIEW_SCHEMA:
        raise Refused(f"schema {_short(header.get('schema'))!r} != {REVIEW_SCHEMA}")
    if header.get("publication_day") != day.isoformat():
        raise Refused(f"header publication_day {_short(header.get('publication_day'))!r} != {day}")
    if header.get("packet_sha256") != packet_sha:
        raise Refused("header packet_sha256 does not match the producer's receipt: grader read other bytes")
    graded_at = parse_ts(header.get("graded_at"))
    if graded_at is None:
        raise Refused("header graded_at missing or not an aware ISO timestamp")
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    if not day_start < graded_at <= now + GRADED_AT_FUTURE_SLACK:
        raise Refused("header graded_at is before the publication day or in the future")
    try:
        grader = header.get("grader")
        if not isinstance(grader, dict):
            raise BadValue("grader must be an object")
        safe_text(grader.get("model"), MAX_SHORT_CHARS, what="grader.model", required=True)
        grader_json = safe_json(grader, MAX_HEADER_JSON_BYTES, what="grader")
        adjudicator = header.get("adjudicator")
        adjudicator_json = None if adjudicator is None else \
            safe_json(adjudicator, MAX_HEADER_JSON_BYTES, what="adjudicator")
    except BadValue as exc:
        raise Refused(f"header {exc}") from exc
    notes: list[str] = []
    counts = {}
    for key in ("claims_total", "claims_graded", "claims_unscorable"):
        counts[key] = int4_or_none(header.get(key))
        if header.get(key) is not None and counts[key] is None:
            notes.append(f"header {key} not an integer in [0, {INT4_MAX}]; stored as null")
    return Header(raw=header, graded_at=graded_at, grader_json=grader_json, adjudicator_json=adjudicator_json,
                  declared_total=counts["claims_total"], declared_graded=counts["claims_graded"],
                  declared_unscorable=counts["claims_unscorable"], notes=notes)


# ── rows ────────────────────────────────────────────────────────────────────

@dataclass
class Accepted:
    channel: str
    source_row_id: int
    params: dict[str, Any]


@dataclass
class Validation:
    header: Header
    accepted: list[Accepted] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)
    rows_seen: int = 0
    unscorable_seen: int = 0


def _evidence(value: Any, graded_at: datetime) -> str:
    if not isinstance(value, list):
        raise BadValue("evidence must be a list of objects")
    if len(value) > MAX_EVIDENCE_ITEMS:
        raise BadValue(f"evidence has more than {MAX_EVIDENCE_ITEMS} items")
    for item in value:
        if not isinstance(item, dict):
            raise BadValue("evidence must be a list of objects")
        safe_text(item.get("source"), MAX_SHORT_CHARS, what="evidence.source", required=True)
        observed = item.get("observed")
        if observed is None or isinstance(observed, (list, dict)):
            raise BadValue("evidence.observed must be a scalar value")
        if isinstance(observed, str):
            safe_text(observed, MAX_OBSERVED_CHARS, what="evidence.observed", required=True)
        elif isinstance(observed, float) and not math.isfinite(observed):
            raise BadValue("evidence.observed is NaN or infinity")
        as_of = parse_ts(item.get("as_of"))
        if as_of is None:
            raise BadValue("evidence.as_of missing or not an aware ISO timestamp")
        if as_of > graded_at:
            raise BadValue("evidence.as_of is after graded_at (evidence from the future)")
        safe_text(item.get("url"), MAX_URL_CHARS, what="evidence.url")
    return safe_json(value, MAX_EVIDENCE_BYTES, what="evidence")


def _directions(row: dict[str, Any], pub: dict[str, Any], channel: str, claim_type: str,
                verdict: str) -> tuple[str | None, str | None]:
    thesis_direction = enum(row.get("thesis_direction"), DIRECTIONS, what="thesis_direction", optional=True)
    actual_direction = enum(row.get("actual_direction"), DIRECTIONS, what="actual_direction", optional=True)
    if channel == "thesis_snapshots" and thesis_direction is not None:
        record = pub.get("source_record")
        stored = record.get("overall_direction") if isinstance(record, dict) else None
        if isinstance(stored, str) and stored.strip().lower() in DIRECTIONS \
                and stored.strip().lower() != thesis_direction:
            raise BadValue("thesis_direction differs from the snapshot's stored overall_direction")
    if claim_type == "direction" and thesis_direction and actual_direction:
        if verdict == "confirmed" and thesis_direction != actual_direction:
            raise BadValue("confirmed direction claim with thesis_direction != actual_direction")
        if verdict == "refuted" and thesis_direction == actual_direction:
            raise BadValue("refuted direction claim with thesis_direction == actual_direction")
    return thesis_direction, actual_direction


def validate_row(row: Any, day: date, packet: Packet, header: Header) -> Accepted:
    """One claim_verdict -> Accepted, or BadValue with the reason."""
    if not isinstance(row, dict) or row.get("kind") != "claim_verdict":
        raise BadValue("not a claim_verdict object")
    pid = row.get("publication_id")
    match = PUBLICATION_ID.fullmatch(pid) if isinstance(pid, str) else None
    if not match:
        raise BadValue("publication_id malformed")
    channel, source_row_id = match.group(1), int(match.group(2))
    pub = packet.publications.get(pid)
    if pub is None:
        raise BadValue("publication not in packet (excluded, quarantined, inconsistent or other day)")
    declared = row.get("publication_sha256")
    if not isinstance(declared, str) or not HEX64.fullmatch(declared) or declared != pub["sha256"]:
        raise BadValue("publication_sha256 != packet text sha256")
    span = safe_text(row.get("claim_span"), MAX_SPAN_CHARS, what="claim_span", required=True)
    if len(span) < MIN_SPAN_CHARS:
        raise BadValue(f"claim_span shorter than {MIN_SPAN_CHARS} chars")
    if span not in pub["text"]:
        raise BadValue("claim_span is not a verbatim substring of the publication text")
    claim_sha = row.get("claim_sha256")
    if claim_sha != sha256(span.encode("utf-8")):
        raise BadValue("claim_sha256 != sha256(claim_span)")
    stated = parse_ts(row.get("stated_at"))
    published = parse_ts(pub.get("published_at"))
    if stated is None or stated != published:
        raise BadValue("stated_at != packet published_at")
    claim_type = enum(row.get("claim_type"), CLAIM_TYPES, what="claim_type")
    verdict = enum(row.get("verdict"), VERDICTS, what="verdict")
    if claim_type == "causal" and verdict in RESOLVED - {"refuted"}:
        raise BadValue("causal claims cannot be confirmed from co-movement (v1 rule)")
    evidence_raw = row.get("evidence")
    root_cause = safe_text(row.get("root_cause"), MAX_TEXT_CHARS, what="root_cause")
    lesson = safe_text(row.get("lesson"), MAX_TEXT_CHARS, what="lesson")
    if verdict == "unscorable":
        if evidence_raw not in ([], None) or root_cause is None:
            raise BadValue("unscorable requires evidence=[] and a one-line root_cause")
        evidence_json = "[]"
    else:
        if verdict in RESOLVED and not evidence_raw:
            raise BadValue(f"{verdict} requires at least one evidence item")
        evidence_json = _evidence(evidence_raw if evidence_raw is not None else [], header.graded_at)
    horizon_raw = row.get("horizon_end")
    horizon = parse_ts(horizon_raw) if horizon_raw is not None else None
    if horizon_raw is not None and horizon is None:
        raise BadValue("horizon_end is not an aware ISO timestamp")
    if verdict in RESOLVED:
        if horizon is None:
            raise BadValue(f"{verdict} requires horizon_end")
        if not stated < horizon <= header.graded_at:
            raise BadValue("horizon_end must be after stated_at and no later than graded_at")
    elif horizon is not None and horizon <= stated:
        raise BadValue("horizon_end must be after stated_at")
    confidence = row.get("grader_confidence")
    if confidence is not None:
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) \
                or not math.isfinite(confidence) or not 0 <= confidence <= 1:
            raise BadValue("grader_confidence outside [0, 1]")
        confidence = float(confidence)
    thesis_direction, actual_direction = _directions(row, pub, channel, claim_type, verdict)
    knowable = row.get("knowable_at_stated_at")
    if knowable is not None and not isinstance(knowable, bool):
        raise BadValue("knowable_at_stated_at must be true, false or null")
    instrument = safe_text(row.get("instrument"), MAX_INSTRUMENT_CHARS, what="instrument")
    return Accepted(channel=channel, source_row_id=source_row_id, params={
        "publication_day": day, "channel": channel, "publication_id": pid, "source_row_id": source_row_id,
        "publication_sha256": pub["sha256"], "stated_at": stated, "claim_span": span, "claim_sha256": claim_sha,
        "claim_type": claim_type, "instrument": instrument, "horizon_end": horizon, "verdict": verdict,
        "evidence": evidence_json, "knowable_at_stated_at": knowable,
        "thesis_direction": thesis_direction, "actual_direction": actual_direction,
        "root_cause": root_cause, "lesson": lesson, "grader_confidence": confidence,
    })


def validate_rows(lines: list[Any], day: date, packet: Packet, header: Header) -> Validation:
    validation = Validation(header=header)
    seen: set[tuple[str, str]] = set()
    for number, row in enumerate(lines[1:], start=2):
        validation.rows_seen += 1
        if isinstance(row, dict) and row.get("verdict") == "unscorable":
            validation.unscorable_seen += 1
        try:
            accepted = validate_row(row, day, packet, header)
            key = (accepted.params["publication_id"], accepted.params["claim_sha256"])
            if key in seen:
                raise BadValue("duplicate (publication_id, claim_sha256) in this file")
            seen.add(key)
        except BadValue as exc:
            reason = str(exc)
        except (TypeError, ValueError, OverflowError, RecursionError, AttributeError, KeyError) as exc:
            reason = f"unprocessable row: {type(exc).__name__}"
        else:
            validation.accepted.append(accepted)
            continue
        if len(validation.rejected) < MAX_REJECTED_RECORDED:
            pid = row.get("publication_id") if isinstance(row, dict) else None
            validation.rejected.append({"line": number, "publication_id": _short(pid) if pid is not None else None,
                                        "reason": reason})
        else:
            validation.rejected_overflow = getattr(validation, "rejected_overflow", 0) + 1  # type: ignore[attr-defined]
    return validation


def declared_mismatch(validation: Validation) -> list[str]:
    header = validation.header
    out = []
    if header.declared_total is not None and header.declared_total != validation.rows_seen:
        out.append(f"claims_total declared {header.declared_total}, file has {validation.rows_seen} rows")
    if header.declared_unscorable is not None and header.declared_unscorable != validation.unscorable_seen:
        out.append(f"claims_unscorable declared {header.declared_unscorable}, file has {validation.unscorable_seen}")
    if header.declared_graded is not None and header.declared_graded != validation.rows_seen - validation.unscorable_seen:
        out.append(f"claims_graded declared {header.declared_graded}, "
                   f"file has {validation.rows_seen - validation.unscorable_seen} scorable rows")
    return out


# ── database ────────────────────────────────────────────────────────────────

def write_rows(engine, day: date, raw: bytes, path: Path, packet_sha: str, validation: Validation,
               *, dry_run: bool) -> dict[str, Any]:
    review_sha = sha256(raw)
    header = validation.header
    result: dict[str, Any] = {"review_sha256": review_sha, "packet_sha256": packet_sha,
                              "accepted_rows": len(validation.accepted),
                              "rejected_rows": len(validation.rejected) + getattr(validation, "rejected_overflow", 0)}
    if dry_run:
        result.update(state="DRY_RUN", run_id=None, supersedes_run_id=None)
        return result
    with engine.begin() as conn:
        existing = conn.execute(EXISTING_RUN, {"sha": review_sha}).scalar()
        if existing is not None:
            result.update(state="PRESENT", run_id=int(existing))
            return result
        previous = conn.execute(LATEST_RUN_FOR_DAY, {"publication_day": day}).scalar()
        run_id = conn.execute(INSERT_RUN, {
            "publication_day": day, "review_sha256": review_sha, "packet_sha256": packet_sha,
            "schema_version": REVIEW_SCHEMA, "grader": header.grader_json, "adjudicator": header.adjudicator_json,
            "graded_at": header.graded_at, "declared_total": header.declared_total,
            "declared_graded": header.declared_graded, "declared_unscorable": header.declared_unscorable,
            "accepted_rows": result["accepted_rows"], "rejected_rows": result["rejected_rows"],
            "supersedes_run_id": int(previous) if previous is not None else None, "source_path": str(path),
        }).scalar()
        if run_id is None:  # lost a race with a concurrent ingest of the same bytes
            existing = conn.execute(EXISTING_RUN, {"sha": review_sha}).scalar()
            result.update(state="PRESENT", run_id=int(existing) if existing is not None else None)
            return result
        for item in validation.accepted:
            conn.execute(INSERT_VERDICT, dict(item.params, run_id=run_id))
        result.update(state="INGESTED", run_id=int(run_id),
                      supersedes_run_id=int(previous) if previous is not None else None)
    return result


# ── orchestration ───────────────────────────────────────────────────────────

def verdict_counts(validation: Validation) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in validation.accepted:
        counts[item.params["verdict"]] = counts.get(item.params["verdict"], 0) + 1
    return dict(sorted(counts.items()))


def ingest(engine, day: date, *, review_root: Path = DEFAULT_REVIEW_ROOT, packet_root: Path = DEFAULT_PACKET_ROOT,
           dry_run: bool = False, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    folder = review_root / day.isoformat()
    path = folder / "review.jsonl"
    receipt: dict[str, Any] = {"schema": RECEIPT_SCHEMA, "job": JOB, "publication_day": day.isoformat(),
                               "review_path": str(path), "ingested_at": iso(now), "dry_run": dry_run}
    try:
        packet = load_packet(packet_root, day)
        raw, lines = read_lines(path)
        header = validate_header(lines[0], day, packet.sha256, now)
        validation = validate_rows(lines, day, packet, header)
        if not validation.accepted:
            raise Refused(f"no row accepted ({validation.rows_seen} rows, all rejected)")
        result = write_rows(engine, day, raw, path, packet.sha256, validation, dry_run=dry_run)
    except Refused as exc:
        receipt.update(state="REFUSED", reason=str(exc))
        _write_receipt(folder, receipt, now, dry_run)
        return receipt
    except Exception as exc:  # noqa: BLE001 - a DB/IO failure must leave a receipt; the type is the public reason
        traceback.print_exc(file=sys.stderr)
        receipt.update(state="FAILED", reason=type(exc).__name__)
        _write_receipt(folder, receipt, now, dry_run)
        return receipt
    receipt.update(result)
    receipt["grader"] = json.loads(header.grader_json)
    receipt["declared"] = {"claims_total": header.declared_total, "claims_graded": header.declared_graded,
                           "claims_unscorable": header.declared_unscorable}
    receipt["declared_mismatch"] = declared_mismatch(validation)
    receipt["notes"] = header.notes
    receipt["verdicts"] = verdict_counts(validation)
    receipt["channels"] = {ch: sum(1 for a in validation.accepted if a.channel == ch)
                           for ch in sorted({a.channel for a in validation.accepted})}
    receipt["rejected"] = validation.rejected
    receipt["packet_records_unusable"] = packet.unusable_records
    _write_receipt(folder, receipt, now, dry_run)
    return receipt


def _write_receipt(folder: Path, receipt: dict[str, Any], now: datetime, dry_run: bool) -> None:
    """Write the receipt next to review.jsonl without following symlinks; never raise."""
    if dry_run:
        return
    try:
        if not folder.is_dir() or folder.is_symlink():
            return
        state = receipt.get("state")
        final = folder / "ingest-receipt.json"
        if state == "PRESENT":
            if final.exists():
                receipt["receipt_file"] = None  # the original INGESTED evidence stays
                return
            name = f"ingest-PRESENT-{now.strftime('%Y%m%dT%H%M%SZ')}.json"
        elif state == "INGESTED" and not final.exists():
            name = "ingest-receipt.json"
        else:
            name = f"ingest-{state}-{now.strftime('%Y%m%dT%H%M%SZ')}.json"
        payload = json.dumps(receipt, indent=2, sort_keys=True, default=str).encode("utf-8")
        receipt["receipt_sha256"] = sha256(payload)
        payload = json.dumps(receipt, indent=2, sort_keys=True, default=str).encode("utf-8")
        tmp = folder / f".{name}.{os.getpid()}.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
        try:
            os.write(fd, payload)
        finally:
            os.close(fd)
        target = folder / name
        if target.is_symlink():
            os.unlink(tmp)
            raise OSError("receipt target is a symlink")
        os.replace(tmp, target)
        receipt["receipt_file"] = name
    except OSError as exc:
        print(f"{JOB}: could not write receipt in {folder}: {type(exc).__name__}", file=sys.stderr)
        receipt["receipt_write_error"] = type(exc).__name__


def exit_code(receipt: dict[str, Any]) -> int:
    state = receipt.get("state")
    if state in {"REFUSED", "FAILED"}:
        return 1
    if state in {"INGESTED", "DRY_RUN"} and (receipt.get("rejected_rows") or receipt.get("declared_mismatch")):
        return 3
    return 0


def pending_days(review_root: Path, now: datetime, lookback: int) -> list[date]:
    """Days in the lookback window with a review.jsonl and no ingest-receipt.json yet."""
    yesterday = (now - timedelta(days=1)).date()
    days = []
    for offset in range(lookback - 1, -1, -1):
        day = yesterday - timedelta(days=offset)
        folder = review_root / day.isoformat()
        if (folder / "review.jsonl").exists() and not (folder / "ingest-receipt.json").exists():
            days.append(day)
    return days


def combined_exit(codes: list[int]) -> int:
    if 1 in codes:
        return 1
    return 3 if 3 in codes else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--day", help="publication day YYYY-MM-DD (default: scan the lookback window)")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help="days before today to scan for an un-ingested review.jsonl (default 3)")
    parser.add_argument("--review-root", default=str(DEFAULT_REVIEW_ROOT))
    parser.add_argument("--packet-root", default=str(DEFAULT_PACKET_ROOT))
    parser.add_argument("--dry-run", action="store_true", help="validate only; no database writes, no receipt")
    parser.add_argument("--json", action="store_true", help="print the receipt(s) as JSON")
    args = parser.parse_args(argv)
    review_root, packet_root = Path(args.review_root).expanduser(), Path(args.packet_root).expanduser()
    now = datetime.now(timezone.utc)
    if args.day:
        try:
            days = [date.fromisoformat(args.day)]
        except ValueError:
            print("--day must be YYYY-MM-DD", file=sys.stderr)
            return 2
    else:
        if not 1 <= args.lookback <= 31:
            print("--lookback must be between 1 and 31", file=sys.stderr)
            return 2
        days = pending_days(review_root, now, args.lookback)
        if not days:
            print(f"{JOB}: nothing pending in the last {args.lookback} day(s) under {review_root}")
            return 0
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from db import get_engine  # resolved from the release tree at runtime

    engine = get_engine()
    receipts = [ingest(engine, day, review_root=review_root, packet_root=packet_root, dry_run=args.dry_run, now=now)
                for day in days]
    if args.json:
        print(json.dumps(receipts if len(receipts) > 1 else receipts[0], indent=2, sort_keys=True, default=str))
    else:
        for receipt in receipts:
            print(f"{JOB} {receipt['publication_day']}: {receipt.get('state')} "
                  f"accepted={receipt.get('accepted_rows', 0)} rejected={receipt.get('rejected_rows', 0)} "
                  f"{receipt.get('reason', '')}".rstrip())
    return combined_exit([exit_code(r) for r in receipts])


if __name__ == "__main__":
    sys.exit(main())
