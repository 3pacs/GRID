"""Read-only native-volume readiness policy. No alerts are sent or gates changed.

The caller must explicitly enable monitoring after cadence activation. A staged
one-shot is not an unattended producer. This evaluates known local contracts;
healthy is not provider reconciliation, signal validity or delivery proof.
"""

from datetime import timedelta
import re

from ingestion.realtime.native_volume_cache import MAX_BYTES, MIN_FREE_BYTES
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
    free_bytes=None,
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
        "inventory_busy",
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
    if type(disk_budget_bytes) is not int or disk_budget_bytes < 2 * MAX_BYTES:
        reasons.add("invalid_disk_budget")
    elif (
        type(disk_bytes) is int
        and disk_bytes >= 0
        and disk_bytes + 2 * MAX_BYTES > disk_budget_bytes
    ):
        reasons.add("disk_budget_exhausted")

    if type(free_bytes) is not int or free_bytes < 0:
        reasons.add("disk_free_unknown")
    elif free_bytes < MIN_FREE_BYTES + 2 * MAX_BYTES:
        reasons.add("disk_free_low")

    if "observation_raced" in errors:
        # A checkpoint transition plus an occupied writer lock is an observation
        # race, not a failed disk read. Never promote it to ready or recovery.
        if "inventory_busy" in reasons:
            reasons -= {"inventory_busy", "disk_usage_unknown"}
        if snapshot is not None:
            try:
                validate_envelope(snapshot)
            except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
                reasons.add("snapshot_invalid")
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
    if (
        "inventory_busy" in reasons
        and (active or publishing)
        and reasons
        <= {
            "inventory_busy",
            "disk_usage_unknown",
            "snapshot_missing",
            "snapshot_checkpoint_mismatch",
            "snapshot_previous_session",
            "snapshot_stale",
            "source_stale",
            "metrics_unavailable",
        }
    ):
        return dict(out, status="pending", reasons=["disk_observation_pending"])
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


_OPERATIONAL_REASONS = frozenset(
    {
        "calendar_unavailable",
        "checkpoint_unreadable",
        "snapshot_unreadable",
        "disk_unreadable",
        "observation_raced",
        "non_session",
        "outside_completed_bar_window",
        "invalid_exit_status",
        "invocation_failed",
        "disk_usage_unknown",
        "invalid_disk_budget",
        "disk_budget_exhausted",
        "disk_free_unknown",
        "disk_free_low",
        "checkpoint_missing",
        "checkpoint_incomplete",
        "checkpoint_stale",
        "checkpoint_invalid",
        "snapshot_missing",
        "snapshot_previous_session",
        "snapshot_stale",
        "metrics_unavailable",
        "source_stale",
        "snapshot_checkpoint_mismatch",
        "snapshot_invalid",
        "snapshot_publish_pending",
        "invocation_in_progress",
        "inventory_busy",
        "disk_observation_pending",
    }
)


def notification_condition(report, *, now):
    """Fixed-text adapter for the EXISTING operations notifier; never sends.

    bad=None means no transition: pending/idle/disabled must not clear an active
    incident or masquerade as recovery. The existing notifier owns confirmation,
    deduplication, retries and delivery. A transport success is not device receipt.
    """
    out = {
        "key": "native_volume",
        "bad": True,
        "message": "Native volume health report is invalid or stale.",
        "ready": False,
        "delivery_verified": False,
    }
    try:
        now = _utc(now)
        if (
            not isinstance(report, dict)
            or report.get("schema") != "grid.native-volume.health.v1"
        ):
            return out
        if any(
            report.get(k) is not False
            for k in ("requested", "live_acceptance", "signal_confirmation")
        ):
            return out
        age = now - _stamp(report.get("checked_at"))
        if not timedelta(0) <= age <= timedelta(seconds=65):
            return out
        reasons = report.get("reasons")
        if (
            not isinstance(reasons, list)
            or len(reasons) > len(_OPERATIONAL_REASONS)
            or any(type(x) is not str or x not in _OPERATIONAL_REASONS for x in reasons)
            or len(set(reasons)) != len(reasons)
        ):
            return out
        status = report.get("status")
        if (
            status == "healthy"
            and report.get("alert") is False
            and report.get("ready") is True
            and not reasons
        ):
            return dict(
                out,
                bad=False,
                ready=True,
                message="Native volume local producer checks recovered; live coverage and device delivery remain separate.",
            )
        if (
            status == "blocked"
            and report.get("alert") is True
            and report.get("ready") is False
            and reasons
        ):
            return dict(
                out,
                message="Native volume blocked: "
                + ", ".join(sorted(reasons))
                + ". No automatic acquisition retry.",
            )
        nonready_reasons = {
            "disabled": {()},
            "idle": {("non_session",), ("outside_completed_bar_window",)},
            "pending": {
                ("invocation_in_progress",),
                ("snapshot_publish_pending",),
                ("disk_observation_pending",),
            },
            "unknown": {("observation_raced",)},
        }
        if (
            status in nonready_reasons
            and tuple(reasons) in nonready_reasons[status]
            and report.get("alert") is False
            and report.get("ready") is False
        ):
            return dict(
                out,
                bad=None,
                message="Native volume health observation has no recovery transition.",
            )
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
        pass
    return out
