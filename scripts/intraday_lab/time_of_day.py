"""Offline activity normalization; supplied exchange calendars define sessions.

No fitted direction or trading rule. Each observation is a complete bucket total
for one metric (volume, trade_count or realized_volatility), with trusted receipt.
Calendar session IDs and UTC opens/closes must come from an upstream calendar;
this module neither guesses holidays nor treats a callback clock as receipt.
"""
from __future__ import annotations

import math
from statistics import median


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def normalize_activity(packet, metric, *, bucket_seconds=300, min_sessions=20, max_sessions=60):
    """Normalize one completed current bucket against prior sessions only.

    packet: decision_at, session, sessions [{session, open_at, close_at}],
    observations [{session, source_id, instrument, metric, value, event_at,
    available_at, clock, status, bucket_start_at, bucket_end_at}]. event_at is
    bucket_end_at. Daily sample count is at most one; duplicates fail closed.
    """
    def unavailable(reason):
        return {"status": "unavailable", "value": None, "reason": reason}

    if (not isinstance(packet, dict) or not isinstance(packet.get("sessions"), list)
            or not isinstance(packet.get("observations"), list)
            or any(not isinstance(row, dict) for row in packet["sessions"] + packet["observations"])):
        return unavailable("invalid packet shape")
    if (not _number(packet.get("decision_at")) or not packet.get("session")
            or isinstance(bucket_seconds, bool) or not isinstance(bucket_seconds, int)
            or bucket_seconds <= 0 or not isinstance(metric, str) or not metric
            or any(isinstance(n, bool) or not isinstance(n, int) for n in (min_sessions, max_sessions))
            or not 1 <= min_sessions <= max_sessions):
        return unavailable("invalid configuration or decision clock")
    decision = packet["decision_at"]
    calendars = {}
    for row in packet.get("sessions", []):
        key, opening, closing = row.get("session"), row.get("open_at"), row.get("close_at")
        if not isinstance(key, str) or not key or key in calendars or not _number(opening) or not _number(closing) or closing <= opening:
            return unavailable("invalid or duplicate calendar session")
        calendars[key] = (opening, closing)
    if not isinstance(packet["session"], str) or packet["session"] not in calendars:
        return unavailable("missing current exchange session")
    opening, closing = calendars[packet["session"]]
    if not opening + bucket_seconds <= decision <= closing:
        return unavailable("no completed bucket in current regular session")
    bucket = min(int((decision - opening) // bucket_seconds) - 1,
                 int((closing - opening) // bucket_seconds) - 1)
    accepted = []
    for row in packet.get("observations", []):
        if row.get("metric") != metric or not isinstance(row.get("session"), str) or row.get("session") not in calendars:
            continue
        row_open, row_close = calendars[row["session"]]
        start = row_open + bucket * bucket_seconds
        end = start + bucket_seconds
        if row.get("bucket_start_at") != start or row.get("bucket_end_at") != end or end > row_close:
            continue
        event, receipt, value = row.get("event_at"), row.get("available_at"), row.get("value")
        if (row.get("status") == "available" and row.get("clock") == "trusted_receipt"
                and isinstance(row.get("source_id"), str) and row["source_id"]
                and isinstance(row.get("instrument"), str) and row["instrument"] and _number(value) and value >= 0
                and _number(event) and _number(receipt) and event == end <= receipt <= decision
                and (row["session"] == packet["session"] or row_close <= opening)):
            accepted.append(row)
    current = [r for r in accepted if r["session"] == packet["session"]]
    if len(current) != 1:
        return unavailable("missing or duplicate completed current bucket")
    target = current[0]
    history = [r for r in accepted if r["session"] != packet["session"]
               and r["instrument"] == target["instrument"] and r["source_id"] == target["source_id"]]
    if len({r["session"] for r in history}) != len(history):
        return unavailable("duplicate prior-session bucket")
    history.sort(key=lambda r: calendars[r["session"]][0])
    history = history[-max_sessions:]
    if len(history) < min_sessions:
        return unavailable("insufficient prior-session history")
    baseline = median(r["value"] for r in history)
    if not math.isfinite(baseline) or baseline <= 0:
        return unavailable("nonpositive baseline")
    ratio = target["value"] / baseline
    if not math.isfinite(ratio):
        return unavailable("nonfinite normalization")
    return {"status": "available", "value": ratio, "metric": metric,
            "baseline_median": baseline, "prior_sessions": len(history),
            "bucket_index": bucket, "bucket_seconds": bucket_seconds,
            "input_sources": [target["source_id"]],
            "latest_available_at": max(r["available_at"] for r in history + current),
            "baseline_sessions": [r["session"] for r in history],
            "interpretation": "relative activity only; directional edge untested"}
