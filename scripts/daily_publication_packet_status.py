#!/usr/bin/env python3
"""Report a daily packet job's declared state from a private observation file.

Read-only, stdlib-only. Does not discover receipts, authenticate declarations,
authorize or perform capture, contact Gemini, schedule work, or write a ledger.
See docs/runbooks/DAILY-PUBLICATION-PACKET.md for the existing owners' contract.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re


# Producer-owned stages come first and are declared by scripts/daily_publication_packet.py
# (one scheduled job per UTC day, under the standing daily-capture approval); the
# daily-review owner appends its five stages to that observation.
STAGES = {
    "diary_run": "claude_capture_producer",
    "candidate": "claude_capture_producer",
    "capture": "claude_capture_producer",
    "admission_review": "codex_daily_review",
    "custody": "codex_daily_review",
    "privacy_screen": "codex_daily_review",
    "local_review": "codex_daily_review",
    "gemini_review": "codex_daily_review",
}
_STATES = {"missing", "unknown", "blocked", "failed", "passed"}
_FIELDS = {"schema", "publication_day", "observed_at", "due_at", "capture_attempt", "stages"}
_STAGE_FIELDS = {"state", "receipt_sha256", "publication_day", "upstream_receipt_sha256"}
_SHA = re.compile(r"[0-9a-f]{64}")
_MAX_INPUT_BYTES = 65536


def _stamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("invalid timestamp")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("timestamp requires timezone")
    return stamp.astimezone(timezone.utc)


def evaluate(observation: dict) -> dict:
    """Classify declarations; a passed declaration is not verified evidence."""
    if not isinstance(observation, dict) or set(observation) != _FIELDS:
        raise ValueError("invalid observation fields")
    if observation["schema"] != "daily-publication-packet-observation-v1":
        raise ValueError("invalid observation schema")
    day = observation["publication_day"]
    if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
        raise ValueError("invalid publication day")
    window_start = datetime.combine(date.fromisoformat(day), time(), timezone.utc)
    window_end = window_start + timedelta(days=1)
    observed, due = _stamp(observation["observed_at"]), _stamp(observation["due_at"])
    if min(observed, due) < window_end:
        raise ValueError("observation and deadline require a completed UTC day")
    attempt = observation["capture_attempt"]
    if not isinstance(attempt, str) or attempt not in {"not_started", "started", "spent", "unknown"}:
        raise ValueError("invalid capture attempt")
    stages = observation["stages"]
    if not isinstance(stages, dict) or set(stages) - STAGES.keys():
        raise ValueError("invalid stages")
    states = {}
    previous = None
    for name in STAGES:
        stage = stages.get(name, {"state": "missing"})
        if not isinstance(stage, dict) or set(stage) - _STAGE_FIELDS:
            raise ValueError("invalid stage fields")
        state = stage.get("state")
        if not isinstance(state, str) or state not in _STATES:
            raise ValueError("invalid stage state")
        if state == "passed":
            digest = stage.get("receipt_sha256")
            if (set(stage) != _STAGE_FIELDS or stage["publication_day"] != day
                    or not isinstance(digest, str) or _SHA.fullmatch(digest) is None):
                raise ValueError("invalid receipt binding")
            upstream = stage["upstream_receipt_sha256"]
            if ((name == "diary_run" and upstream is not None)
                    or (name != "diary_run" and (
                        not isinstance(upstream, str) or _SHA.fullmatch(upstream) is None))
                    or (previous is not None and upstream != previous)):
                raise ValueError("invalid receipt chain")
            previous = digest
        else:
            # Non-success states carry no payload or invented evidence binding.
            if set(stage) != {"state"}:
                raise ValueError("non-success stage must contain state only")
            previous = None
        states[name] = state
    if states["capture"] == "passed" and attempt != "spent":
        raise ValueError("capture receipt requires reconciled spent attempt")
    first = next((name for name in STAGES if states[name] != "passed"), None)
    if first is None:
        state, action, owner = "COMPLETE_REPORTED", "retain_existing_completion_receipts", STAGES["local_review"]
    else:
        explicit_block = any(value in {"failed", "blocked", "unknown"} for value in states.values())
        state = "OVERDUE" if observed >= due else ("BLOCKED" if explicit_block else "PENDING")
        action, owner = f"reconcile_{first}_receipt", STAGES[first]
        if first == "capture":
            action = ("producer_run_daily_publication_packet" if attempt == "not_started"
                      else "reconcile_existing_capture_attempt")
    return {
        "schema": "daily-publication-packet-status-v1",
        "job": "daily_publication_packet_handoff",
        "job_key": f"daily_publication_packet_handoff:{day}",
        "publication_day": day,
        "window_start": window_start.isoformat(), "window_end": window_end.isoformat(),
        "observed_at": observed.isoformat(), "due_at": due.isoformat(),
        "state": state, "stage_states": states, "first_blocked_stage": first,
        "accountable_owner": "codex_daily_review", "action_owner": owner,
        "next_action": action, "capture_attempt": attempt,
        "authorizes_execution": False,
    }


def _unique_object(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("observation", type=Path, help="private, normalized JSON observation")
    parser.add_argument("--day", required=True, help="expected completed UTC publication day")
    args = parser.parse_args(argv)
    try:
        with args.observation.open("rb") as source:
            raw = source.read(_MAX_INPUT_BYTES + 1)
        if len(raw) > _MAX_INPUT_BYTES:
            raise ValueError("observation too large")
        result = evaluate(json.loads(raw, object_pairs_hook=_unique_object))
        if result["publication_day"] != args.day:
            raise ValueError("unexpected publication day")
        result["observation_sha256"] = hashlib.sha256(raw).hexdigest()
    except (OSError, ValueError, TypeError, OverflowError, RecursionError):
        # Never echo raw input, paths, exception messages or private receipt text.
        print(json.dumps({"schema": "daily-publication-packet-status-v1",
                          "state": "INVALID_OBSERVATION", "authorizes_execution": False}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result["state"] == "COMPLETE_REPORTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
