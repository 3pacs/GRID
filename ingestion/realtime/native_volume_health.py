"""Read-only native-volume readiness policy. No alerts are sent or gates changed.

The caller must explicitly enable monitoring after cadence activation. A staged
one-shot is not an unattended producer. This evaluates known local contracts;
healthy is not provider reconciliation, signal validity or delivery proof.
"""

from datetime import timedelta
import re

from ingestion.realtime.native_volume_cache import MAX_BYTES
from ingestion.realtime.native_volume_snapshot import (
    validate_envelope,
    _stamp,
)
from ingestion.realtime.native_volume_worker import SCHEMA as CHECKPOINT_SCHEMA
from ingestion.realtime.research_intervals import _utc


def evaluate(
    *,
    enabled,
    now,
    session=None,
    calendar_version=None,
    baseline_sessions=None,
    checkpoint=None,
    snapshot=None,
    last_exit_code=None,
    disk_bytes=None,
    disk_budget_bytes=128_000_000,
    read_errors=(),
):
    """Return only operational codes. Never return values, paths or raw errors.

    session=None means a VERIFIED non-session, not a failed calendar lookup.
    read_errors carries fixed codes from the bounded reader. A caller must use
    calendar_unavailable on lookup failure instead of claiming a closed market.
    """
    now = _utc(now)
    out = dict(
        schema="grid.native-volume.health.v1",
        checked_at=now.isoformat(),
        status="disabled",
        alert=False,
        ready=False,
        reasons=[],
        requested=False,
        live_acceptance=False,
        signal_confirmation=False,
    )
    if enabled is not True:
        return out
    errors = set(read_errors)
    if not errors <= {
        "calendar_unavailable",
        "checkpoint_unreadable",
        "snapshot_unreadable",
        "disk_unreadable",
        "observation_raced",
    }:
        raise ValueError("unrecognized read error code")
    if "calendar_unavailable" in errors:
        return dict(out, status="blocked", alert=True, reasons=["calendar_unavailable"])
    if session is None:
        return dict(out, status="idle", reasons=["non_session"])
    opens, closes = _utc(session.opens), _utc(session.closes)
    if now < opens + timedelta(seconds=65) or now > closes + timedelta(seconds=180):
        return dict(out, status="idle", reasons=["outside_completed_bar_window"])
    reasons = errors - {"observation_raced"}
    if last_exit_code is not None:
        if type(last_exit_code) is not int or last_exit_code < 0:
            reasons.add("invalid_exit_status")
        elif last_exit_code != 0:
            reasons.add("invocation_failed")
    if type(disk_bytes) is not int or disk_bytes < 0:
        reasons.add("disk_usage_unknown")
    elif type(disk_budget_bytes) is not int or disk_budget_bytes < 2 * MAX_BYTES:
        reasons.add("invalid_disk_budget")
    elif disk_bytes + 2 * MAX_BYTES > disk_budget_bytes:
        reasons.add("disk_budget_exhausted")

    if "observation_raced" in errors:
        if reasons:
            return dict(out, status="blocked", alert=True, reasons=sorted(reasons))
        return dict(out, status="unknown", reasons=["observation_raced"])

    active = False
    publishing = False
    try:
        if checkpoint is None:
            reasons.add("checkpoint_missing")
        else:
            if (
                not isinstance(checkpoint, dict)
                or checkpoint.get("schema") != CHECKPOINT_SCHEMA
                or checkpoint.get("symbol") != "SPY"
                or checkpoint.get("session") != session.key
                or checkpoint.get("mode") not in ("bootstrap", "incremental")
            ):
                raise ValueError("checkpoint identity")
            started = _stamp(checkpoint.get("started_at"))
            boundary = _stamp(checkpoint.get("window_end"))
            if (
                started > now
                or boundary.second
                or boundary.microsecond
                or not opens < boundary <= closes
                or boundary > started - timedelta(seconds=5)
            ):
                raise ValueError("checkpoint clock")
            status = checkpoint.get("status")
            if status == "in_progress" and now - started <= timedelta(seconds=45):
                active = True
            elif status != "complete":
                reasons.add("checkpoint_incomplete")
            else:
                received = _stamp(checkpoint.get("received_at"))
                if not started <= received <= now:
                    raise ValueError("checkpoint receipt clock")
                if not re.fullmatch(
                    r"[0-9a-f]{64}", checkpoint.get("receipt_sha256", "")
                ):
                    raise ValueError("checkpoint receipt identity")
                publishing = now - started <= timedelta(seconds=45)
                if now - received > timedelta(seconds=180):
                    reasons.add("checkpoint_stale")
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
        reasons.add("checkpoint_invalid")

    try:
        if snapshot is None:
            if not active:
                reasons.add("snapshot_missing")
        else:
            validate_envelope(snapshot)
            payload = snapshot["payload"]
            generated = _stamp(snapshot["generated_at"])
            prior_publication = (
                (active or publishing)
                and payload.get("symbol") == "SPY"
                and isinstance(baseline_sessions, (tuple, list))
                and payload.get("session") in baseline_sessions
                and payload.get("calendar_version") == calendar_version
                and generated < opens
            )
            if prior_publication:
                reasons.add("snapshot_previous_session")
            else:
                if (
                    payload.get("symbol") != "SPY"
                    or payload.get("session") != session.key
                    or payload.get("calendar_version") != calendar_version
                    or not isinstance(baseline_sessions, (tuple, list))
                    or len(baseline_sessions) != 10
                    or len(set(baseline_sessions)) != 10
                    or payload.get("baseline_sessions") != list(baseline_sessions)
                ):
                    raise ValueError("snapshot session/calendar identity")
                if generated > now:
                    raise ValueError("snapshot future")
                if now - generated > timedelta(seconds=180):
                    reasons.add("snapshot_stale")
                if payload["status"] == "ready" and (
                    payload.get("reason") is not None
                    or payload.get("baseline_errors") != {}
                ):
                    raise ValueError("contradictory ready status")
                if payload["status"] != "ready":
                    reasons.add("metrics_unavailable")
                else:
                    for key in ("latest_receipt_at", "end_of_completed_window"):
                        if (
                            not timedelta(0)
                            <= now - _stamp(payload[key])
                            <= timedelta(seconds=180)
                        ):
                            reasons.add("source_stale")
                    end = _stamp(payload["end_of_completed_window"])
                    if not opens < end <= closes or end.second or end.microsecond:
                        raise ValueError("snapshot boundary")
                    if payload["completed_minutes"] != int(
                        (end - opens).total_seconds() // 60
                    ):
                        raise ValueError("snapshot coverage count")
                    # A complete checkpoint plus a different snapshot is not an atomic
                    # successful publish. One monitor retry can handle a read race.
                    if (
                        checkpoint
                        and checkpoint.get("status") == "complete"
                        and (
                            checkpoint.get("window_end")
                            != payload["end_of_completed_window"]
                            or _stamp(checkpoint["received_at"]) > generated
                        )
                    ):
                        reasons.add("snapshot_checkpoint_mismatch")
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
        reasons.add("snapshot_invalid")
    # The producer commits a complete checkpoint before building the snapshot.
    # Allow that bounded publish interval without accepting missing/older output.
    # A failed publish still alerts after the 45-second invocation deadline.
    if (
        (publishing or active)
        and reasons
        & {
            "snapshot_missing",
            "snapshot_checkpoint_mismatch",
            "snapshot_previous_session",
        }
        and reasons
        <= {
            "snapshot_missing",
            "snapshot_checkpoint_mismatch",
            "snapshot_previous_session",
            "snapshot_stale",
            "source_stale",
            "metrics_unavailable",
        }
    ):
        return dict(out, status="pending", reasons=["snapshot_publish_pending"])
    if reasons:
        return dict(out, status="blocked", alert=True, reasons=sorted(reasons))
    if active:
        return dict(out, status="pending", reasons=["invocation_in_progress"])
    return dict(out, status="healthy", ready=True)
