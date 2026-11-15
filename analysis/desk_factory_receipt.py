"""Offline receipt-bound caller for desk factory evidence; no file or ledger IO.

Trust boundary: expected_result_sha256 must come from the existing independently
verified ledger/anchor, not from hashing the supplied receipt. This function
verifies the pinned record only, not the ledger chain or the frozen discovery.
"""
import hashlib
import json
import math
import re
from datetime import datetime, timedelta

from analysis.desk_factory_report import MAX_COUNT, MAX_REPORT_BYTES, factory_report_cards

MAX_RESULT_BYTES = 8_000_000


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("lowercase sha256 required")
    return value


def _pairs(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError("duplicate JSON key")
        out[key] = value
    return out


def _nonfinite(value):
    raise ValueError("non-finite JSON number")


def _parse(raw, limit):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= limit:
        raise ValueError("bounded nonempty bytes required")
    try:
        value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_nonfinite)
    except (UnicodeError, RecursionError, OverflowError, json.JSONDecodeError) as exc:
        raise ValueError("invalid receipt/report JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON object required")
    return value


def _encoded(value, *, compact=False):
    # Matches ledger canonical() when compact, offline_research_proof.digest()
    # otherwise. Neither matches the producer's pretty-printed report bytes.
    kwargs = {"separators": (",", ":")} if compact else {}
    try:
        return json.dumps(value, sort_keys=True, allow_nan=False, **kwargs).encode()
    except (ValueError, RecursionError, OverflowError, TypeError) as exc:
        raise ValueError("invalid canonical JSON") from exc


def _record_clock(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("recorded_at must be an aware timestamp or absent")
    try:
        clock = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("invalid recorded_at") from exc
    if clock.utcoffset() is None:
        raise ValueError("recorded_at must be timezone-aware")
    return clock


def receipt_bound_factory_cards(
    report_raw: bytes, result_raw: bytes, *, expected_result_sha256: str,
    receipt_available_at: datetime | None, as_of: datetime, max_age: timedelta,
) -> dict:
    """Join an existing pinned result receipt to its report without executing it.

    result_raw is the exact canonical ledger line excluding its newline. The
    reported recorded_at is the result-record observation clock, NOT report-file
    creation time. Receipt availability must be supplied from existing custody;
    missing availability remains withheld. Never substitute current time/mtime.
    """
    _sha(expected_result_sha256)
    if not isinstance(result_raw, bytes) or not 0 < len(result_raw) <= MAX_RESULT_BYTES:
        raise ValueError("bounded result receipt required")
    if hashlib.sha256(result_raw).hexdigest() != expected_result_sha256:
        raise ValueError("result receipt hash mismatch")
    receipt = _parse(result_raw, MAX_RESULT_BYTES)
    if _encoded(receipt, compact=True) != result_raw:
        raise ValueError("result receipt is not canonical ledger-line JSON")
    if receipt.get("kind") != "factory_run_result" or receipt.get("promotion_allowed") is not False:
        raise ValueError("non-promoting factory result required")
    report = _parse(report_raw, MAX_REPORT_BYTES)
    if hashlib.sha256(_encoded(report)).hexdigest() != _sha(receipt.get("report_sha256")):
        raise ValueError("report does not match receipt canonical-object digest")
    if report.get("run_id") != receipt.get("run_id"):
        raise ValueError("run identity mismatch")
    if report.get("frozen_sha256") != _sha(receipt.get("frozen_sha256")):
        raise ValueError("frozen identity mismatch")

    # Byte hash is derived only AFTER binding the parsed report to the pinned
    # external receipt. This is not self-authentication of an arbitrary report.
    result = factory_report_cards(
        report_raw, expected_sha256=hashlib.sha256(report_raw).hexdigest(),
        receipt_id=expected_result_sha256,
        observed_at=_record_clock(receipt.get("recorded_at")),
        available_at=receipt_available_at, as_of=as_of, max_age=max_age,
    )
    trials = receipt.get("trials")
    if not isinstance(trials, list) or len(trials) != result["declared_trials"]:
        raise ValueError("receipt must enumerate every declared trial")
    if type(receipt.get("trial_count")) is not int or receipt["trial_count"] != len(trials):
        raise ValueError("receipt trial count mismatch")
    indexed = {}
    tested = selected = 0
    selected_ids = set()
    for trial in trials:
        if not isinstance(trial, dict):
            raise ValueError("trial object required")
        identity = trial.get("trial_id")
        if not isinstance(identity, str) or not identity.strip() or len(identity) > 512 or identity in indexed:
            raise ValueError("unique bounded trial identity required")
        status = trial.get("status")
        if not isinstance(status, str) or not status.strip():
            raise ValueError("trial status required")
        if type(trial.get("selected")) is not bool:
            raise ValueError("explicit trial selection required")
        if trial["selected"] and status != "tested":
            raise ValueError("untested trial cannot be selected")
        if status == "tested":
            if type(trial.get("n")) is not int or not 0 <= trial["n"] <= MAX_COUNT:
                raise ValueError("tested receipt trial needs a JSON-safe fire count")
            for key in ("r", "net_dollars", "t", "p", "adjusted_p"):
                value = trial.get(key)
                try:
                    finite = type(value) in (int, float) and math.isfinite(value)
                except OverflowError as exc:
                    raise ValueError("invalid tested receipt metric") from exc
                if not finite or (key in ("p", "adjusted_p") and not 0 <= value <= 1):
                    raise ValueError("invalid tested receipt metric")
        tested += status == "tested"
        selected += trial["selected"]
        if trial["selected"]:
            selected_ids.add(identity)
        indexed[identity] = trial
    for key, expected in (("tested_count", tested), ("selected_count", selected)):
        if type(receipt.get(key)) is not int or receipt[key] != expected:
            raise ValueError("receipt aggregate count mismatch")
    if tested != result["tested_trials"] or selected_ids != set(result["selected_trial_ids"]):
        raise ValueError("report selection/tested totals disagree with receipt")
    for row in report["ranking"]:
        trial = indexed.get(row["trial_id"])
        if trial is None or trial["status"] != "tested":
            raise ValueError("ranked trial absent or untested in receipt")
        for key in ("family", "feature", "n", "r", "net_dollars", "t", "p", "adjusted_p", "selected"):
            if key not in trial or trial[key] != row[key]:
                raise ValueError("ranked trial disagrees with receipt")
    result["clock_scope"] = "result_record_observation_and_receipt_availability"
    result["result_receipt_sha256"] = expected_result_sha256
    result["report_canonical_sha256"] = receipt["report_sha256"]
    result["selected_ids_verified_against_result_receipt"] = True
    result["selected_not_in_ranking_status"] = "matched_to_pinned_result_receipt"
    result["ledger_chain_verified"] = False
    # frozen_and_ledger_verified and selected_ids_verified_against_discovery
    # intentionally remain False: this receipt join verifies neither artifact.
    return result
